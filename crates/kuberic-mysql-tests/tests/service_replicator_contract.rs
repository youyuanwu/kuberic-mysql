#[path = "service_common/mod.rs"]
mod service_common;

use std::collections::VecDeque;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroU64;
use std::os::unix::net::UnixListener;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use kuberic_mysql::adapter::UnixSocketPath;
use kuberic_mysql::core::{
    AttemptId, AuthorityGeneration, ConfigurationId, ConfiguredGtidLineage, CredentialGeneration,
    EndpointBinding, GroupName, GtidSet, MemberAddress, MemberId, MemberRole, MemberState,
    NativeMember, NativeView, ProcessSessionId, ProgressProjection, ReplicaId, ReplicaIncarnation,
    ServerUuid, StorageBinding, ViewId, project_progress,
};
use kuberic_mysql::service::{
    AccountProvisioningEvidence, ApplicationAccountReceipt, ApplicationCredential,
    AttemptCleanupState, AttemptContext, BackendConnectionId, BackendConnectionReceipt,
    BackendSessionIdentity, ControlCredential, ControlCredentialRole, FreshBootstrapInput,
    FreshJoinInput, LifecycleOperation, MemberConfigurationIntent, MemberControlBinding,
    MemberEffectKind, MemberEffectReceipt, MemberEffectRequest, MemberMutationAuthority,
    MemberNativeEvidence, MemberOperationIdentity, MemberPeer, MemberSourceBoundary,
    MemberTransitionError, MemberViewError, MysqlAccessIntent, MysqlConfigurationOutcome,
    MysqlHandoffBoundary, MysqlHandoffOutcome, MysqlHandoffPublicationProof, MysqlInstanceConfig,
    MysqlInstanceError, MysqlMemberEffectBoundary, MysqlMemberIndex, MysqlOpenReceipt,
    MysqlOperationIdentity, MysqlPeerAuthorityFacts, MysqlPeerOperationIntent,
    MysqlPeerOperationKind, MysqlPeerOperationOutcome, MysqlPeerUnsupportedReason,
    MysqlProxyControlBoundary, MysqlProxyError, MysqlReplicaConfiguration, MysqlReplicaEpoch,
    MysqlReplicaIdentity, MysqlReplicaRole, MysqlReplicaTarget, MysqlReplicator,
    MysqlReplicatorError, MysqlSourceTerminalReceipt, MysqlTerminalState, NativeIdentityEnrollment,
    PrimaryTransferInput,
};
use service_common::{GROUP_UUID, TestRoot};

const LOCAL_UUID: &str = "11111111-1111-1111-1111-111111111111";
const PEER_UUID: &str = "22222222-2222-2222-2222-222222222222";
const THIRD_UUID: &str = "33333333-3333-3333-3333-333333333333";
const MEMBERS: [MysqlMemberIndex; 3] = [
    MysqlMemberIndex::First,
    MysqlMemberIndex::Second,
    MysqlMemberIndex::Third,
];

#[derive(Clone)]
enum ReadPlan {
    Exact {
        role: MysqlReplicaRole,
        executed: &'static str,
    },
    CancelThenExact {
        role: MysqlReplicaRole,
    },
    Evidence(Box<MemberNativeEvidence>),
    Error,
}

#[derive(Clone, Copy)]
enum MutationPlan {
    Exact,
    Error,
    Cancel,
    CancelThenExact,
    DelayThenExact,
}

#[derive(Clone)]
enum ReceiptPlan {
    Exact,
    CrossOperation,
    CrossEpoch,
    CrossRole,
    CrossConfiguration,
    SubstituteAuthority(Box<MemberMutationAuthority>),
    SubstituteSource(Box<MemberSourceBoundary>),
}

#[derive(Clone, Copy)]
enum CleanupPlan {
    Completed,
    NotRequired,
    Failed,
    StopFailed,
    ReapFailed,
}

struct FormationFailurePlan {
    mutation: MutationPlan,
    receipts: Vec<ReceiptPlan>,
    cleanup: CleanupPlan,
    attempt: Option<AttemptContext>,
    cancel_before_invoke: bool,
}

impl FormationFailurePlan {
    fn new(mutation: MutationPlan) -> Self {
        Self {
            mutation,
            receipts: Vec::new(),
            cleanup: CleanupPlan::Completed,
            attempt: None,
            cancel_before_invoke: false,
        }
    }

    fn with_receipts(mut self, receipts: Vec<ReceiptPlan>) -> Self {
        self.receipts = receipts;
        self
    }

    fn with_cleanup(mut self, cleanup: CleanupPlan) -> Self {
        self.cleanup = cleanup;
        self
    }

    fn with_attempt(mut self, attempt: AttemptContext) -> Self {
        self.attempt = Some(attempt);
        self
    }

    fn cancel_before_invoke(mut self) -> Self {
        self.cancel_before_invoke = true;
        self
    }
}

struct ScriptedEffects {
    config: MysqlInstanceConfig,
    _proxy_listener: UnixListener,
    proxy_socket: UnixSocketPath,
    reads: VecDeque<ReadPlan>,
    receipts: VecDeque<ReceiptPlan>,
    bootstrap: MutationPlan,
    join: MutationPlan,
    cleanup: CleanupPlan,
    containments: Arc<AtomicUsize>,
}

impl ScriptedEffects {
    fn new(
        config: MysqlInstanceConfig,
        reads: Vec<ReadPlan>,
        receipts: Vec<ReceiptPlan>,
        cleanup: CleanupPlan,
    ) -> Self {
        let socket_path = config
            .data_root()
            .parent()
            .expect("test data root has a parent")
            .join("proxy-backend.sock");
        let proxy_listener = UnixListener::bind(&socket_path).unwrap();
        let proxy_socket = UnixSocketPath::new(&socket_path).unwrap();
        Self {
            config,
            _proxy_listener: proxy_listener,
            proxy_socket,
            reads: reads.into(),
            receipts: receipts.into(),
            bootstrap: MutationPlan::Exact,
            join: MutationPlan::Exact,
            cleanup,
            containments: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn for_formation(
        config: MysqlInstanceConfig,
        reads: Vec<ReadPlan>,
        receipts: Vec<ReceiptPlan>,
        bootstrap: MutationPlan,
        join: MutationPlan,
        cleanup: CleanupPlan,
        containments: Arc<AtomicUsize>,
    ) -> Self {
        let mut effects = Self::new(config, reads, receipts, cleanup);
        effects.bootstrap = bootstrap;
        effects.join = join;
        effects.containments = containments;
        effects
    }

    fn receipt_request(&mut self, request: MemberEffectRequest) -> MemberEffectRequest {
        let plan = self.receipts.pop_front().unwrap_or(ReceiptPlan::Exact);
        let operation = match &plan {
            ReceiptPlan::CrossOperation => MemberOperationIdentity::new(
                AuthorityGeneration::new("substituted-operation").unwrap(),
                request.operation().generation(),
            ),
            _ => request.operation().clone(),
        };
        let epoch = if matches!(&plan, ReceiptPlan::CrossEpoch) {
            request.epoch() + 1
        } else {
            request.epoch()
        };
        let role = if matches!(&plan, ReceiptPlan::CrossRole) {
            match request.role() {
                MemberRole::Primary => MemberRole::Secondary,
                MemberRole::Secondary => MemberRole::Primary,
            }
        } else {
            request.role()
        };
        let configuration = if matches!(&plan, ReceiptPlan::CrossConfiguration) {
            request.configuration().map(|configuration| {
                MemberConfigurationIntent::new(configuration.id().clone(), Vec::new())
            })
        } else {
            request.configuration().cloned()
        };
        let authority = if let ReceiptPlan::SubstituteAuthority(authority) = &plan {
            authority.as_ref().clone()
        } else {
            request.authority().clone()
        };
        let source = if let ReceiptPlan::SubstituteSource(source) = &plan {
            Some(source.as_ref().clone())
        } else {
            request.source().cloned()
        };
        MemberEffectRequest::new(operation, epoch, role, configuration, authority, source).unwrap()
    }
}

impl MysqlMemberEffectBoundary for ScriptedEffects {
    fn config(&self) -> &MysqlInstanceConfig {
        &self.config
    }

    async fn bootstrap_member(
        &mut self,
        input: FreshBootstrapInput,
        _recovery: &ControlCredential,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError> {
        let receipt_attempt = if matches!(
            self.bootstrap,
            MutationPlan::CancelThenExact | MutationPlan::DelayThenExact
        ) {
            Some(
                AttemptContext::new(
                    attempt.attempt().clone(),
                    Instant::now() + Duration::from_secs(60),
                    Duration::from_secs(10),
                )
                .unwrap(),
            )
        } else {
            None
        };
        match self.bootstrap {
            MutationPlan::Exact => {}
            MutationPlan::Error => {
                return Err(MemberTransitionError::View(
                    MemberViewError::RequiredMemberMissing,
                ));
            }
            MutationPlan::Cancel => {
                attempt.cancel();
                return Err(MemberTransitionError::Cancelled);
            }
            MutationPlan::CancelThenExact => attempt.cancel(),
            MutationPlan::DelayThenExact => {
                tokio::time::sleep(Duration::from_millis(260)).await;
            }
        }
        let request = input
            .request()
            .cloned()
            .expect("replicator bootstrap binds its request");
        let request = self.receipt_request(request);
        let authority = request.authority().clone();
        let successor = single_member_evidence(authority.clone(), "17:1");
        MemberEffectReceipt::for_request(
            MemberEffectKind::Bootstrap,
            request,
            None,
            successor,
            authority.local().binding(),
            receipt_attempt.as_ref().unwrap_or(attempt),
            AttemptCleanupState::Completed,
        )
    }

    async fn join_member(
        &mut self,
        input: FreshJoinInput,
        _recovery: &ControlCredential,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError> {
        let receipt_attempt = if matches!(
            self.join,
            MutationPlan::CancelThenExact | MutationPlan::DelayThenExact
        ) {
            Some(
                AttemptContext::new(
                    attempt.attempt().clone(),
                    Instant::now() + Duration::from_secs(60),
                    Duration::from_secs(10),
                )
                .unwrap(),
            )
        } else {
            None
        };
        match self.join {
            MutationPlan::Exact => {}
            MutationPlan::Error => {
                return Err(MemberTransitionError::View(
                    MemberViewError::RequiredMemberMissing,
                ));
            }
            MutationPlan::Cancel => {
                attempt.cancel();
                return Err(MemberTransitionError::Cancelled);
            }
            MutationPlan::CancelThenExact => attempt.cancel(),
            MutationPlan::DelayThenExact => {
                tokio::time::sleep(Duration::from_millis(260)).await;
            }
        }
        let request = input
            .request()
            .cloned()
            .expect("replicator join binds its request");
        let request = self.receipt_request(request);
        let authority = request.authority().clone();
        let predecessor = request
            .source()
            .expect("join receipt request retains a source")
            .evidence()
            .clone();
        let successor = joined_evidence(&authority, &predecessor);
        MemberEffectReceipt::for_request(
            MemberEffectKind::Join,
            request,
            Some(predecessor),
            successor,
            authority.local().binding(),
            receipt_attempt.as_ref().unwrap_or(attempt),
            AttemptCleanupState::NotRequired,
        )
    }

    async fn read_member_view(
        &mut self,
        request: MemberEffectRequest,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError> {
        let request = self.receipt_request(request);
        let authority = request.authority().clone();
        let (evidence, receipt_attempt) = match self.reads.pop_front().expect("scripted read") {
            ReadPlan::Exact { role, executed } => {
                (evidence(authority.clone(), "23:1", role, executed), None)
            }
            ReadPlan::CancelThenExact { role } => {
                attempt.cancel();
                (
                    evidence(authority.clone(), "23:1", role, ""),
                    Some(
                        AttemptContext::new(
                            attempt.attempt().clone(),
                            Instant::now() + Duration::from_secs(60),
                            Duration::from_secs(10),
                        )
                        .unwrap(),
                    ),
                )
            }
            ReadPlan::Evidence(evidence) => (*evidence, None),
            ReadPlan::Error => return Err(MemberTransitionError::Cancelled),
        };
        MemberEffectReceipt::for_request(
            MemberEffectKind::ViewReadback,
            request,
            None,
            evidence,
            authority.local().binding(),
            receipt_attempt.as_ref().unwrap_or(attempt),
            AttemptCleanupState::NotRequired,
        )
    }

    async fn transfer_primary(
        &mut self,
        input: PrimaryTransferInput,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError> {
        let request = input
            .request()
            .cloned()
            .expect("replicator transfer binds its request");
        let request = self.receipt_request(request);
        let authority = request.authority().clone();
        let predecessor = request
            .source()
            .expect("transfer receipt request retains a source")
            .evidence()
            .clone();
        let successor = transferred_evidence(&authority, &predecessor, input.target());
        MemberEffectReceipt::for_request(
            MemberEffectKind::PrimaryTransfer {
                target: input.target().member_id().clone(),
            },
            request,
            Some(predecessor),
            successor,
            authority.local().binding(),
            attempt,
            AttemptCleanupState::NotRequired,
        )
    }

    async fn terminate_backend_connection(
        &mut self,
        _authority: &MemberMutationAuthority,
        _connection: BackendConnectionId,
        _attempt: &AttemptContext,
    ) -> Result<BackendConnectionReceipt, MemberTransitionError> {
        unreachable!("Phase 3 does not publish a proxy")
    }

    fn contain_member(
        &mut self,
        _attempt: &AttemptContext,
    ) -> Result<AttemptCleanupState, MysqlInstanceError> {
        self.containments.fetch_add(1, Ordering::Relaxed);
        match self.cleanup {
            CleanupPlan::Completed => Ok(AttemptCleanupState::Completed),
            CleanupPlan::NotRequired => Ok(AttemptCleanupState::NotRequired),
            CleanupPlan::StopFailed => Err(MysqlInstanceError::ChildFailure {
                operation: LifecycleOperation::Shutdown,
                code: Some(1),
            }),
            CleanupPlan::ReapFailed => {
                Err(MysqlInstanceError::Timeout(LifecycleOperation::Shutdown))
            }
            CleanupPlan::Failed => Err(MysqlInstanceError::Io {
                operation: LifecycleOperation::ScratchCleanup,
                kind: std::io::ErrorKind::PermissionDenied,
            }),
        }
    }
}

impl MysqlProxyControlBoundary for ScriptedEffects {
    fn proxy_backend_socket(
        &mut self,
        _authority: &MemberMutationAuthority,
    ) -> Result<UnixSocketPath, MysqlProxyError> {
        Ok(self.proxy_socket.clone())
    }

    async fn prepare_application_account(
        &mut self,
        _authority: &MemberMutationAuthority,
        credential: &ApplicationCredential,
        _attempt: &AttemptContext,
    ) -> Result<ApplicationAccountReceipt, MysqlProxyError> {
        Ok(ApplicationAccountReceipt::new(credential))
    }

    async fn remove_application_account(
        &mut self,
        _authority: &MemberMutationAuthority,
        _receipt: &ApplicationAccountReceipt,
        _attempt: &AttemptContext,
    ) -> Result<(), MysqlProxyError> {
        Ok(())
    }

    async fn terminate_proxy_backend(
        &mut self,
        _authority: &MemberMutationAuthority,
        _session: &BackendSessionIdentity,
        _attempt: &AttemptContext,
        _deadline: Instant,
    ) -> Result<(), MysqlProxyError> {
        Ok(())
    }
}

struct Harness {
    _root: TestRoot,
    engine: MysqlReplicator<ScriptedEffects>,
    authority: MemberMutationAuthority,
    identity: MysqlReplicaIdentity,
    local: MysqlReplicaTarget,
    peer: MysqlReplicaTarget,
    attempt: AttemptContext,
}

#[test]
fn open_is_exact_idempotent_and_closed_by_default() {
    let mut harness = harness(Vec::new());
    let receipt = open_receipt(&harness);
    let outcome = harness
        .engine
        .open(operation("open", 1), receipt.clone())
        .unwrap();
    assert_eq!(outcome.identity(), &harness.identity);
    assert!(!harness.engine.access_intent().unwrap().read());
    assert!(!harness.engine.access_intent().unwrap().write());
    assert_eq!(
        harness
            .engine
            .open(operation("open", 1), receipt.clone())
            .unwrap(),
        outcome
    );

    let changed = MysqlOpenReceipt::new(
        harness.identity.clone(),
        harness.authority.clone(),
        nonzero(2),
    );
    assert!(matches!(
        harness.engine.open(operation("open", 1), changed),
        Err(MysqlReplicatorError::OpenConflict)
    ));
    assert!(matches!(
        harness.engine.open(operation("other-open", 2), receipt),
        Err(MysqlReplicatorError::OpenConflict)
    ));
}

#[tokio::test]
async fn same_role_and_changed_role_each_require_a_new_epoch_barrier() {
    let reads = vec![
        exact(MysqlReplicaRole::Secondary),
        exact(MysqlReplicaRole::Secondary),
        exact(MysqlReplicaRole::Secondary),
        exact(MysqlReplicaRole::Secondary),
        exact(MysqlReplicaRole::Primary),
        exact(MysqlReplicaRole::Primary),
    ];
    let mut harness = harness(reads);
    open(&mut harness);
    harness
        .engine
        .advance_epoch(operation("epoch-1", 2), MysqlReplicaEpoch::new(1))
        .unwrap();
    assert!(matches!(
        harness
            .engine
            .observe_configuration(operation("early-config", 3), configuration(&harness, 1))
            .await,
        Err(MysqlReplicatorError::RoleRequired)
    ));
    harness
        .engine
        .observe_role(
            operation("role-1", 3),
            MysqlReplicaEpoch::new(1),
            MysqlReplicaRole::Secondary,
        )
        .await
        .unwrap();
    harness
        .engine
        .observe_configuration(operation("config-1", 4), configuration(&harness, 1))
        .await
        .unwrap();

    assert!(matches!(
        harness
            .engine
            .observe_role(
                operation("same-without-epoch", 5),
                MysqlReplicaEpoch::new(1),
                MysqlReplicaRole::Secondary,
            )
            .await,
        Err(MysqlReplicatorError::EpochBarrierRequired)
    ));
    harness
        .engine
        .advance_epoch(operation("epoch-2", 5), MysqlReplicaEpoch::new(2))
        .unwrap();
    assert!(harness.engine.configuration().is_none());
    harness
        .engine
        .observe_role(
            operation("same-role", 6),
            MysqlReplicaEpoch::new(2),
            MysqlReplicaRole::Secondary,
        )
        .await
        .unwrap();
    harness
        .engine
        .observe_configuration(operation("config-2", 7), configuration(&harness, 2))
        .await
        .unwrap();

    harness
        .engine
        .advance_epoch(operation("epoch-3", 8), MysqlReplicaEpoch::new(3))
        .unwrap();
    harness
        .engine
        .observe_role(
            operation("promote", 9),
            MysqlReplicaEpoch::new(3),
            MysqlReplicaRole::Primary,
        )
        .await
        .unwrap();
    harness
        .engine
        .observe_configuration(operation("config-3", 10), configuration(&harness, 3))
        .await
        .unwrap();
    assert_eq!(harness.engine.role(), Some(MysqlReplicaRole::Primary));
    assert!(matches!(
        harness
            .engine
            .advance_epoch(operation("stale-epoch", 11), MysqlReplicaEpoch::new(3)),
        Err(MysqlReplicatorError::StaleEpoch)
    ));
}

#[tokio::test]
async fn role_and_configuration_retries_are_exact_and_conflicts_are_rejected() {
    let mut harness = harness(vec![
        exact(MysqlReplicaRole::Secondary),
        exact(MysqlReplicaRole::Secondary),
    ]);
    open(&mut harness);
    harness
        .engine
        .advance_epoch(operation("epoch", 2), MysqlReplicaEpoch::new(1))
        .unwrap();
    let role = harness
        .engine
        .observe_role(
            operation("role", 3),
            MysqlReplicaEpoch::new(1),
            MysqlReplicaRole::Secondary,
        )
        .await
        .unwrap();
    assert_eq!(
        harness
            .engine
            .observe_role(
                operation("role", 3),
                MysqlReplicaEpoch::new(1),
                MysqlReplicaRole::Secondary,
            )
            .await
            .unwrap(),
        role
    );
    assert!(matches!(
        harness
            .engine
            .observe_role(
                operation("role", 3),
                MysqlReplicaEpoch::new(1),
                MysqlReplicaRole::Primary,
            )
            .await,
        Err(MysqlReplicatorError::OperationConflict)
    ));

    let accepted = configuration(&harness, 1);
    let configured = harness
        .engine
        .observe_configuration(operation("config", 4), accepted.clone())
        .await
        .unwrap();
    assert_eq!(
        harness
            .engine
            .observe_configuration(operation("config", 4), accepted)
            .await
            .unwrap(),
        configured
    );
    harness
        .engine
        .advance_epoch(operation("next-epoch", 5), MysqlReplicaEpoch::new(2))
        .unwrap();
    assert_eq!(
        harness
            .engine
            .observe_configuration(operation("config", 4), configuration(&harness, 1))
            .await
            .unwrap(),
        configured
    );
    assert!(matches!(
        harness
            .engine
            .observe_configuration(operation("config-stale", 4), configuration(&harness, 1))
            .await,
        Err(MysqlReplicatorError::StaleOperationGeneration)
    ));
}

#[tokio::test]
async fn bootstrap_and_join_credit_only_exact_typed_receipts() {
    let mut bootstrap = harness(vec![exact(MysqlReplicaRole::Primary)]);
    open(&mut bootstrap);
    bootstrap
        .engine
        .advance_epoch(operation("bootstrap-epoch", 2), MysqlReplicaEpoch::new(1))
        .unwrap();
    bootstrap
        .engine
        .observe_role(
            operation("bootstrap-role", 3),
            MysqlReplicaEpoch::new(1),
            MysqlReplicaRole::Primary,
        )
        .await
        .unwrap();
    bootstrap
        .engine
        .bootstrap_configuration(
            operation("bootstrap-config", 4),
            local_only_configuration(&bootstrap, 1),
        )
        .await
        .unwrap();
    assert!(bootstrap.engine.credentials_cleared());

    let mut join = harness(vec![exact(MysqlReplicaRole::Secondary)]);
    open(&mut join);
    join.engine
        .advance_epoch(operation("join-epoch", 2), MysqlReplicaEpoch::new(1))
        .unwrap();
    join.engine
        .observe_role(
            operation("join-role", 3),
            MysqlReplicaEpoch::new(1),
            MysqlReplicaRole::Secondary,
        )
        .await
        .unwrap();
    let source_authority = authority_with_attempt(&join.attempt, MysqlMemberIndex::Second);
    let predecessor = single_member_evidence(source_authority, "31:1");
    let source = MemberSourceBoundary::new(join.peer.member().clone(), predecessor).unwrap();
    join.engine
        .join_configuration(
            operation("join-config", 4),
            configuration(&join, 1),
            source,
            vec![join.peer.member().clone()],
        )
        .await
        .unwrap();
    assert_eq!(
        join.engine.configuration().unwrap().epoch(),
        MysqlReplicaEpoch::new(1)
    );
}

#[tokio::test]
async fn three_independent_owners_form_bootstrap_then_two_sequential_joins() {
    let roots = [
        TestRoot::new("form-one"),
        TestRoot::new("form-two"),
        TestRoot::new("form-three"),
    ];
    let attempt = context("three-owner-formation");
    let authorities = MEMBERS.map(|member| authority_with_attempt(&attempt, member));
    let identities = [
        replica_identity("formation-one"),
        replica_identity("formation-two"),
        replica_identity("formation-three"),
    ];
    let targets: [MysqlReplicaTarget; 3] = std::array::from_fn(|index| {
        target_from_authority(identities[index].clone(), &authorities[index])
    });
    let executed = format!("{GROUP_UUID}:1");
    let first_view = evidence_from_members(
        authorities[0].clone(),
        "17:1",
        &[(MysqlMemberIndex::First, MemberRole::Primary)],
        &executed,
    );
    let second_views = [
        evidence_from_members(
            authorities[0].clone(),
            "17:2",
            &[
                (MysqlMemberIndex::First, MemberRole::Primary),
                (MysqlMemberIndex::Second, MemberRole::Secondary),
            ],
            &executed,
        ),
        evidence_from_members(
            authorities[1].clone(),
            "17:2",
            &[
                (MysqlMemberIndex::First, MemberRole::Primary),
                (MysqlMemberIndex::Second, MemberRole::Secondary),
            ],
            &executed,
        ),
    ];
    let final_views = MEMBERS.map(|member| {
        evidence_from_members(
            authorities[member.as_usize()].clone(),
            "17:3",
            &[
                (MysqlMemberIndex::First, MemberRole::Primary),
                (MysqlMemberIndex::Second, MemberRole::Secondary),
                (MysqlMemberIndex::Third, MemberRole::Secondary),
            ],
            &executed,
        )
    });
    let counters: [Arc<AtomicUsize>; 3] = std::array::from_fn(|_| Arc::new(AtomicUsize::new(0)));
    let reads: [Vec<ReadPlan>; 3] = [
        vec![
            ReadPlan::Evidence(Box::new(second_views[0].clone())),
            ReadPlan::Evidence(Box::new(final_views[0].clone())),
            ReadPlan::Evidence(Box::new(final_views[0].clone())),
        ],
        vec![
            ReadPlan::Evidence(Box::new(final_views[1].clone())),
            ReadPlan::Evidence(Box::new(final_views[1].clone())),
        ],
        vec![ReadPlan::Evidence(Box::new(final_views[2].clone()))],
    ];
    let mut engines: [MysqlReplicator<ScriptedEffects>; 3] = std::array::from_fn(|index| {
        MysqlReplicator::new(
            ScriptedEffects::for_formation(
                roots[index].config_for_member(MEMBERS[index]),
                reads[index].clone(),
                Vec::new(),
                MutationPlan::Exact,
                MutationPlan::Exact,
                CleanupPlan::Completed,
                counters[index].clone(),
            ),
            identities[index].clone(),
            kuberic_mysql::core::ConfiguredGtidLineage::new(GROUP_UUID).unwrap(),
            credential(&attempt, ControlCredentialRole::Observer),
            credential(&attempt, ControlCredentialRole::Recovery),
            attempt.clone(),
        )
        .unwrap()
    });
    for index in 0..3 {
        engines[index]
            .open(
                operation(&format!("formation-open-{index}"), 1),
                MysqlOpenReceipt::new(
                    identities[index].clone(),
                    authorities[index].clone(),
                    nonzero(1),
                ),
            )
            .unwrap();
        engines[index]
            .advance_epoch(
                operation(&format!("formation-epoch-{index}"), 2),
                MysqlReplicaEpoch::new(1),
            )
            .unwrap();
        let role = if index == 0 {
            MysqlReplicaRole::Primary
        } else {
            MysqlReplicaRole::Secondary
        };
        engines[index]
            .intend_formation_role(
                operation(&format!("formation-role-{index}"), 3),
                MysqlReplicaEpoch::new(1),
                role,
            )
            .unwrap();
        assert!(engines[index].configuration().is_none());
        assert!(engines[index].native_evidence().is_none());
    }

    let first_configuration = formation_configuration("formation-one", 1, vec![targets[0].clone()]);
    engines[0]
        .bootstrap_configuration(
            operation("formation-bootstrap", 4),
            first_configuration.clone(),
        )
        .await
        .unwrap();
    assert_eq!(
        engines[0].native_evidence().unwrap().view(),
        first_view.view()
    );

    let second_configuration = formation_configuration(
        "formation-two",
        1,
        vec![targets[0].clone(), targets[1].clone()],
    );
    engines[1]
        .join_configuration(
            operation("formation-join-second", 4),
            second_configuration.clone(),
            engines[0].source_boundary().unwrap(),
            vec![targets[0].member().clone()],
        )
        .await
        .unwrap();
    engines[0]
        .observe_configuration(
            operation("formation-observe-second", 5),
            second_configuration,
        )
        .await
        .unwrap();

    let final_configuration = formation_configuration("formation-three", 1, targets.to_vec());
    engines[2]
        .join_configuration(
            operation("formation-join-third", 4),
            final_configuration.clone(),
            engines[1].source_boundary().unwrap(),
            vec![targets[0].member().clone(), targets[1].member().clone()],
        )
        .await
        .unwrap();
    engines[0]
        .observe_configuration(
            operation("formation-observe-third-first", 6),
            final_configuration.clone(),
        )
        .await
        .unwrap();
    engines[1]
        .observe_configuration(
            operation("formation-observe-third-second", 5),
            final_configuration.clone(),
        )
        .await
        .unwrap();

    let mut progress = Vec::new();
    for index in 0..3 {
        assert_eq!(engines[index].configuration(), Some(&final_configuration));
        assert_eq!(
            engines[index].native_evidence().unwrap(),
            &final_views[index]
        );
        assert!(engines[index].proxy_snapshot().is_none());
        assert!(engines[index].credentials_cleared());
        progress.push(
            engines[index]
                .read_progress(operation(
                    &format!("formation-progress-{index}"),
                    if index == 0 {
                        7
                    } else if index == 1 {
                        6
                    } else {
                        5
                    },
                ))
                .await
                .unwrap()
                .progress(),
        );
        assert!(matches!(
            engines[index]
                .observe_role(
                    operation(&format!("formation-same-role-{index}"), 20),
                    MysqlReplicaEpoch::new(1),
                    if index == 0 {
                        MysqlReplicaRole::Primary
                    } else {
                        MysqlReplicaRole::Secondary
                    },
                )
                .await,
            Err(MysqlReplicatorError::EpochBarrierRequired)
        ));
    }
    assert_eq!(progress, vec![progress[0]; 3]);
    assert!(matches!(
        progress[0],
        kuberic_mysql::core::ProgressProjection::Projectable(value) if value.value() == 1
    ));
    kuberic_mysql::service::CanonicalViewId::parse(first_view.view().id())
        .unwrap()
        .require_successor(
            kuberic_mysql::service::CanonicalViewId::parse(second_views[0].view().id()).unwrap(),
        )
        .unwrap();
    kuberic_mysql::service::CanonicalViewId::parse(second_views[0].view().id())
        .unwrap()
        .require_successor(
            kuberic_mysql::service::CanonicalViewId::parse(final_views[0].view().id()).unwrap(),
        )
        .unwrap();

    for (index, engine) in engines.iter_mut().enumerate() {
        engine
            .close(operation(&format!("formation-close-{index}"), 30))
            .unwrap();
        assert_eq!(counters[index].load(Ordering::Relaxed), 1);
    }
}

#[tokio::test]
async fn every_formation_effect_contains_only_its_owner_on_failure_or_cancellation() {
    for (label, member, peer_count) in [
        ("bootstrap", MysqlMemberIndex::First, 0),
        ("second-join", MysqlMemberIndex::Second, 1),
        ("third-join", MysqlMemberIndex::Third, 2),
    ] {
        for plan in [MutationPlan::Error, MutationPlan::Cancel] {
            let counters = [
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
            ];
            let (result, mut engines) = run_failed_formation_effect(
                label,
                member,
                peer_count,
                FormationFailurePlan::new(plan),
                counters.clone(),
            )
            .await;
            assert!(matches!(
                result,
                Err(MysqlReplicatorError::MemberTransition(_))
            ));
            let failed = member_number(member) - 1;
            for (index, counter) in counters.iter().enumerate() {
                assert_eq!(
                    counter.load(Ordering::Relaxed),
                    usize::from(index == failed)
                );
            }
            for (index, engine) in engines.iter_mut().enumerate() {
                if index == failed {
                    assert_eq!(engine.terminal_state(), Some(MysqlTerminalState::Aborted));
                } else {
                    engine
                        .abort(operation(&format!("{label}-policy-abort-{index}"), 10))
                        .unwrap();
                }
            }
            for counter in &counters {
                assert_eq!(counter.load(Ordering::Relaxed), 1);
            }
        }
    }
}

#[tokio::test]
async fn uncertain_formation_completion_aborts_only_the_exact_local_owner() {
    for (label, member, peer_count) in [
        ("bootstrap-late", MysqlMemberIndex::First, 0),
        ("second-join-late", MysqlMemberIndex::Second, 1),
        ("third-join-late", MysqlMemberIndex::Third, 2),
    ] {
        let counters = std::array::from_fn(|_| Arc::new(AtomicUsize::new(0)));
        let (result, engines) = run_failed_formation_effect(
            label,
            member,
            peer_count,
            FormationFailurePlan::new(MutationPlan::CancelThenExact),
            counters.clone(),
        )
        .await;
        assert!(matches!(
            result,
            Err(MysqlReplicatorError::LateReceipt { .. })
        ));
        assert_only_local_owner_aborted(member, &engines, &counters);
    }

    for (label, member, peer_count) in [
        ("bootstrap-mismatch", MysqlMemberIndex::First, 0),
        ("second-join-mismatch", MysqlMemberIndex::Second, 1),
        ("third-join-mismatch", MysqlMemberIndex::Third, 2),
    ] {
        let counters = std::array::from_fn(|_| Arc::new(AtomicUsize::new(0)));
        let (result, engines) = run_failed_formation_effect(
            label,
            member,
            peer_count,
            FormationFailurePlan::new(MutationPlan::Exact)
                .with_receipts(vec![ReceiptPlan::CrossOperation]),
            counters.clone(),
        )
        .await;
        assert!(matches!(result, Err(MysqlReplicatorError::ReceiptMismatch)));
        assert_only_local_owner_aborted(member, &engines, &counters);
    }
}

#[tokio::test]
async fn cancelled_formation_admission_immediately_contains_only_the_local_owner() {
    for (label, member, peer_count) in [
        ("bootstrap-cancelled-admission", MysqlMemberIndex::First, 0),
        (
            "second-join-cancelled-admission",
            MysqlMemberIndex::Second,
            1,
        ),
        ("third-join-cancelled-admission", MysqlMemberIndex::Third, 2),
    ] {
        let counters = std::array::from_fn(|_| Arc::new(AtomicUsize::new(0)));
        let (result, engines) = run_failed_formation_effect(
            label,
            member,
            peer_count,
            FormationFailurePlan::new(MutationPlan::Exact).cancel_before_invoke(),
            counters.clone(),
        )
        .await;
        assert!(matches!(result, Err(MysqlReplicatorError::Cancelled)));
        assert_only_local_owner_aborted(member, &engines, &counters);
    }
}

#[tokio::test]
async fn formation_receipt_after_work_deadline_is_contained_before_return() {
    for (label, member, peer_count) in [
        ("bootstrap-late-deadline", MysqlMemberIndex::First, 0),
        ("second-join-late-deadline", MysqlMemberIndex::Second, 1),
        ("third-join-late-deadline", MysqlMemberIndex::Third, 2),
    ] {
        let now = Instant::now();
        let attempt = AttemptContext::new_at(
            AttemptId::new(format!("{label}-attempt")).unwrap(),
            now,
            now + Duration::from_millis(350),
            Duration::from_millis(120),
        )
        .unwrap();
        let counters = std::array::from_fn(|_| Arc::new(AtomicUsize::new(0)));
        let (result, engines) = run_failed_formation_effect(
            label,
            member,
            peer_count,
            FormationFailurePlan::new(MutationPlan::DelayThenExact).with_attempt(attempt),
            counters.clone(),
        )
        .await;
        assert!(matches!(
            result,
            Err(MysqlReplicatorError::LateReceipt {
                deadline: kuberic_mysql::service::AttemptDeadlineState::CleanupReserve,
                cancellation: kuberic_mysql::service::AttemptCancellationState::Active,
            })
        ));
        assert_only_local_owner_aborted(member, &engines, &counters);
    }
}

#[tokio::test]
async fn formation_failure_preserves_primary_and_local_cleanup_failure() {
    let counters = std::array::from_fn(|_| Arc::new(AtomicUsize::new(0)));
    let (result, engines) = run_failed_formation_effect(
        "bootstrap-cleanup-failure",
        MysqlMemberIndex::First,
        0,
        FormationFailurePlan::new(MutationPlan::Error).with_cleanup(CleanupPlan::Failed),
        counters.clone(),
    )
    .await;
    assert!(matches!(
        result,
        Err(MysqlReplicatorError::ConfigurationCleanup {
            primary,
            cleanup: _
        }) if matches!(*primary, MysqlReplicatorError::MemberTransition(_))
    ));
    assert_only_local_owner_aborted(MysqlMemberIndex::First, &engines, &counters);
}

#[tokio::test]
async fn every_formation_effect_rejects_after_the_shared_work_deadline() {
    for (label, member, peer_count) in [
        ("bootstrap-deadline", MysqlMemberIndex::First, 0),
        ("second-join-deadline", MysqlMemberIndex::Second, 1),
        ("third-join-deadline", MysqlMemberIndex::Third, 2),
    ] {
        let root = TestRoot::new(label);
        let now = Instant::now();
        let attempt = AttemptContext::new_at(
            AttemptId::new(label).unwrap(),
            now,
            now + Duration::from_millis(80),
            Duration::from_millis(30),
        )
        .unwrap();
        let authority = authority_with_attempt(&attempt, member);
        let identity = replica_identity(label);
        let counter = Arc::new(AtomicUsize::new(0));
        let mut engine = MysqlReplicator::new(
            ScriptedEffects::for_formation(
                root.config_for_member(member),
                Vec::new(),
                Vec::new(),
                MutationPlan::Exact,
                MutationPlan::Exact,
                CleanupPlan::Completed,
                counter.clone(),
            ),
            identity.clone(),
            kuberic_mysql::core::ConfiguredGtidLineage::new(GROUP_UUID).unwrap(),
            credential(&attempt, ControlCredentialRole::Observer),
            credential(&attempt, ControlCredentialRole::Recovery),
            attempt.clone(),
        )
        .unwrap();
        engine
            .open(
                operation(&format!("{label}-open"), 1),
                MysqlOpenReceipt::new(identity.clone(), authority.clone(), nonzero(1)),
            )
            .unwrap();
        engine
            .advance_epoch(
                operation(&format!("{label}-epoch"), 2),
                MysqlReplicaEpoch::new(1),
            )
            .unwrap();
        engine
            .intend_formation_role(
                operation(&format!("{label}-role"), 3),
                MysqlReplicaEpoch::new(1),
                if member == MysqlMemberIndex::First {
                    MysqlReplicaRole::Primary
                } else {
                    MysqlReplicaRole::Secondary
                },
            )
            .unwrap();
        std::thread::sleep(Duration::from_millis(55));
        let result =
            invoke_formation_effect(&mut engine, &attempt, &identity, &authority, peer_count).await;
        assert!(matches!(
            result,
            Err(MysqlReplicatorError::Deadline(
                kuberic_mysql::service::AttemptDeadlineState::CleanupReserve
            ))
        ));
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        assert_eq!(engine.terminal_state(), Some(MysqlTerminalState::Aborted));
        assert!(engine.credentials_cleared());
        assert!(!engine.access_intent().unwrap().read());
        assert!(!engine.access_intent().unwrap().write());
    }
}

#[tokio::test]
async fn receipts_are_bound_to_operation_epoch_configuration_and_predecessor() {
    for (label, plan) in [
        ("cross-operation", ReceiptPlan::CrossOperation),
        ("cross-epoch", ReceiptPlan::CrossEpoch),
        ("cross-role", ReceiptPlan::CrossRole),
    ] {
        let mut harness = harness_with_receipts(
            vec![exact(MysqlReplicaRole::Secondary)],
            vec![plan],
            CleanupPlan::Completed,
        );
        open(&mut harness);
        harness
            .engine
            .advance_epoch(
                operation(&format!("{label}-epoch"), 2),
                MysqlReplicaEpoch::new(1),
            )
            .unwrap();
        assert!(matches!(
            harness
                .engine
                .observe_role(
                    operation(&format!("{label}-role"), 3),
                    MysqlReplicaEpoch::new(1),
                    MysqlReplicaRole::Secondary,
                )
                .await,
            Err(MysqlReplicatorError::ReceiptMismatch)
        ));
        assert_eq!(harness.engine.role(), None);
    }

    let mut cross_configuration = harness_with_receipts(
        vec![
            exact(MysqlReplicaRole::Secondary),
            exact(MysqlReplicaRole::Secondary),
        ],
        vec![ReceiptPlan::Exact, ReceiptPlan::CrossConfiguration],
        CleanupPlan::Completed,
    );
    open(&mut cross_configuration);
    cross_configuration
        .engine
        .advance_epoch(
            operation("cross-config-epoch", 2),
            MysqlReplicaEpoch::new(1),
        )
        .unwrap();
    cross_configuration
        .engine
        .observe_role(
            operation("cross-config-role", 3),
            MysqlReplicaEpoch::new(1),
            MysqlReplicaRole::Secondary,
        )
        .await
        .unwrap();
    assert!(matches!(
        cross_configuration
            .engine
            .observe_configuration(
                operation("cross-config", 4),
                configuration(&cross_configuration, 1),
            )
            .await,
        Err(MysqlReplicatorError::ReceiptMismatch)
    ));
    assert!(cross_configuration.engine.configuration().is_none());

    let substitute_authority = authority("replicator", MysqlMemberIndex::Third);
    let substitute_predecessor = single_member_evidence(substitute_authority.clone(), "31:1");
    let substitute_source = MemberSourceBoundary::new(
        MemberPeer::from_enrollment(substitute_authority.local()),
        substitute_predecessor,
    )
    .unwrap();
    let mut predecessor_substitution = harness_with_receipts(
        vec![exact(MysqlReplicaRole::Secondary)],
        vec![
            ReceiptPlan::Exact,
            ReceiptPlan::SubstituteSource(Box::new(substitute_source)),
        ],
        CleanupPlan::Completed,
    );
    open(&mut predecessor_substitution);
    predecessor_substitution
        .engine
        .advance_epoch(operation("predecessor-epoch", 2), MysqlReplicaEpoch::new(1))
        .unwrap();
    predecessor_substitution
        .engine
        .observe_role(
            operation("predecessor-role", 3),
            MysqlReplicaEpoch::new(1),
            MysqlReplicaRole::Secondary,
        )
        .await
        .unwrap();
    let source_authority =
        authority_with_attempt(&predecessor_substitution.attempt, MysqlMemberIndex::Second);
    let predecessor = single_member_evidence(source_authority, "31:1");
    let source =
        MemberSourceBoundary::new(predecessor_substitution.peer.member().clone(), predecessor)
            .unwrap();
    assert!(matches!(
        predecessor_substitution
            .engine
            .join_configuration(
                operation("predecessor-config", 4),
                configuration(&predecessor_substitution, 1),
                source,
                vec![predecessor_substitution.peer.member().clone()],
            )
            .await,
        Err(MysqlReplicatorError::ReceiptMismatch)
    ));
}

#[tokio::test]
async fn primary_transfer_receipt_credits_only_the_exact_local_demotion() {
    let mut harness = configured(
        MysqlReplicaRole::Primary,
        vec![
            exact(MysqlReplicaRole::Primary),
            exact(MysqlReplicaRole::Primary),
            exact(MysqlReplicaRole::Secondary),
        ],
    )
    .await;
    harness
        .engine
        .advance_epoch(operation("transfer-epoch", 5), MysqlReplicaEpoch::new(2))
        .unwrap();
    let predecessor = evidence(
        harness.authority.clone(),
        "41:1",
        MysqlReplicaRole::Primary,
        "",
    );
    let source = MemberSourceBoundary::new(harness.local.member().clone(), predecessor).unwrap();
    let outcome = harness
        .engine
        .transfer_role(
            operation("transfer-role", 6),
            MysqlReplicaEpoch::new(2),
            MysqlReplicaRole::Secondary,
            source,
            harness.peer.member().clone(),
        )
        .await
        .unwrap();
    assert_eq!(outcome.role(), MysqlReplicaRole::Secondary);
    harness
        .engine
        .observe_configuration(operation("transfer-config", 7), configuration(&harness, 2))
        .await
        .unwrap();
}

#[tokio::test]
async fn mismatched_late_and_uncertain_effects_never_credit_or_open_access() {
    let wrong = authority("replicator", MysqlMemberIndex::Second);
    let mut mismatch = harness_with_receipts(
        vec![exact(MysqlReplicaRole::Secondary)],
        vec![ReceiptPlan::SubstituteAuthority(Box::new(wrong))],
        CleanupPlan::Completed,
    );
    open(&mut mismatch);
    mismatch
        .engine
        .advance_epoch(operation("mismatch-epoch", 2), MysqlReplicaEpoch::new(1))
        .unwrap();
    assert!(matches!(
        mismatch
            .engine
            .observe_role(
                operation("mismatch-role", 3),
                MysqlReplicaEpoch::new(1),
                MysqlReplicaRole::Secondary,
            )
            .await,
        Err(MysqlReplicatorError::ReceiptMismatch)
    ));
    assert_eq!(mismatch.engine.role(), None);
    assert!(!mismatch.engine.access_intent().unwrap().write());
    assert!(matches!(
        mismatch
            .engine
            .observe_role(
                operation("mismatch-role", 3),
                MysqlReplicaEpoch::new(1),
                MysqlReplicaRole::Secondary,
            )
            .await,
        Err(MysqlReplicatorError::OperationUncertain)
    ));

    let mut late = harness(vec![ReadPlan::CancelThenExact {
        role: MysqlReplicaRole::Secondary,
    }]);
    open(&mut late);
    late.engine
        .advance_epoch(operation("late-epoch", 2), MysqlReplicaEpoch::new(1))
        .unwrap();
    assert!(matches!(
        late.engine
            .observe_role(
                operation("late-role", 3),
                MysqlReplicaEpoch::new(1),
                MysqlReplicaRole::Secondary,
            )
            .await,
        Err(MysqlReplicatorError::LateReceipt { .. })
    ));
    assert_eq!(late.engine.role(), None);

    let mut uncertain = harness(vec![ReadPlan::Error]);
    open(&mut uncertain);
    uncertain
        .engine
        .advance_epoch(operation("uncertain-epoch", 2), MysqlReplicaEpoch::new(1))
        .unwrap();
    assert!(matches!(
        uncertain
            .engine
            .observe_role(
                operation("uncertain-role", 3),
                MysqlReplicaEpoch::new(1),
                MysqlReplicaRole::Secondary,
            )
            .await,
        Err(MysqlReplicatorError::MemberTransition(_))
    ));
    assert_eq!(uncertain.engine.role(), None);
}

#[tokio::test]
async fn build_and_remove_validate_prerequisites_targets_and_exact_retries() {
    let mut secondary = configured(
        MysqlReplicaRole::Secondary,
        vec![
            exact(MysqlReplicaRole::Secondary),
            exact(MysqlReplicaRole::Secondary),
        ],
    )
    .await;
    assert!(matches!(
        secondary
            .engine
            .build_replica(peer_intent(&secondary, "secondary-build", 5, false)),
        Err(MysqlReplicatorError::PrimaryRequired)
    ));
    assert!(matches!(
        secondary
            .engine
            .build_replica(peer_intent(&secondary, "secondary-build", 5, false)),
        Err(MysqlReplicatorError::PrimaryRequired)
    ));

    let mut primary = configured(
        MysqlReplicaRole::Primary,
        vec![
            exact(MysqlReplicaRole::Primary),
            exact(MysqlReplicaRole::Primary),
        ],
    )
    .await;
    let build_intent = peer_intent(&primary, "build", 5, false);
    let expected = MysqlPeerOperationOutcome::Unsupported {
        kind: MysqlPeerOperationKind::Build,
        reason: MysqlPeerUnsupportedReason::NativeExecutionUnavailable,
        target: primary.peer.clone(),
    };
    assert_eq!(
        primary.engine.build_replica(build_intent.clone()).unwrap(),
        expected
    );
    assert_eq!(
        primary.engine.build_replica(build_intent).unwrap(),
        expected
    );
    assert!(matches!(
        primary
            .engine
            .build_replica(peer_intent(&primary, "build", 5, true)),
        Err(MysqlReplicatorError::OperationConflict)
    ));

    let remove = peer_intent(&primary, "remove", 6, false);
    assert!(matches!(
        primary.engine.remove_replica(remove.clone()).unwrap(),
        MysqlPeerOperationOutcome::Unsupported {
            kind: MysqlPeerOperationKind::Remove,
            ..
        }
    ));
    assert_eq!(
        primary.engine.remove_replica(remove.clone()).unwrap(),
        primary.engine.remove_replica(remove).unwrap()
    );

    let self_operation = operation("self", 7);
    let self_intent = MysqlPeerOperationIntent::new(
        self_operation.clone(),
        MysqlReplicaEpoch::new(1),
        ConfigurationId::new("configuration-1").unwrap(),
        peer_facts(primary.local.clone()),
    );
    assert!(matches!(
        primary.engine.build_replica(self_intent),
        Err(MysqlReplicatorError::SelfTarget)
    ));
    let self_retry = MysqlPeerOperationIntent::new(
        self_operation.clone(),
        MysqlReplicaEpoch::new(1),
        ConfigurationId::new("configuration-1").unwrap(),
        peer_facts(primary.local.clone()),
    );
    assert!(matches!(
        primary.engine.build_replica(self_retry),
        Err(MysqlReplicatorError::SelfTarget)
    ));
    let changed_retry = MysqlPeerOperationIntent::new(
        self_operation,
        MysqlReplicaEpoch::new(1),
        ConfigurationId::new("configuration-1").unwrap(),
        peer_facts(primary.peer.clone()),
    );
    assert!(matches!(
        primary.engine.build_replica(changed_retry),
        Err(MysqlReplicatorError::OperationConflict)
    ));

    let stale_epoch = MysqlPeerOperationIntent::new(
        operation("stale-peer", 8),
        MysqlReplicaEpoch::new(0),
        ConfigurationId::new("configuration-1").unwrap(),
        peer_facts(primary.peer.clone()),
    );
    assert!(matches!(
        primary.engine.build_replica(stale_epoch),
        Err(MysqlReplicatorError::ConfigurationRequired)
    ));

    let idle = replica_target("idle", "replicator", MysqlMemberIndex::Third);
    let idle_build = MysqlPeerOperationIntent::new(
        operation("idle-build", 9),
        MysqlReplicaEpoch::new(1),
        ConfigurationId::new("configuration-1").unwrap(),
        peer_facts(idle.clone()),
    );
    assert!(matches!(
        primary.engine.build_replica(idle_build).unwrap(),
        MysqlPeerOperationOutcome::Unsupported {
            kind: MysqlPeerOperationKind::Build,
            target,
            ..
        } if target == idle
    ));

    primary
        .engine
        .advance_epoch(operation("peer-next-epoch", 10), MysqlReplicaEpoch::new(2))
        .unwrap();
    assert_eq!(
        primary
            .engine
            .build_replica(peer_intent(&primary, "build", 5, false))
            .unwrap(),
        expected
    );
}

#[tokio::test]
async fn peer_operations_validate_every_supplied_identity_and_binding_fact() {
    let mut primary = configured(
        MysqlReplicaRole::Primary,
        vec![
            exact(MysqlReplicaRole::Primary),
            exact(MysqlReplicaRole::Primary),
        ],
    )
    .await;
    let target = primary.peer.clone();

    let process_mismatch = MysqlPeerOperationIntent::new(
        operation("peer-process-mismatch", 5),
        MysqlReplicaEpoch::new(1),
        ConfigurationId::new("configuration-1").unwrap(),
        MysqlPeerAuthorityFacts::new(
            target.clone(),
            ProcessSessionId::new("substituted-process").unwrap(),
            target.endpoint().clone(),
            target.storage().clone(),
        ),
    );
    assert!(matches!(
        primary.engine.build_replica(process_mismatch),
        Err(MysqlReplicatorError::IdentityMismatch)
    ));

    let endpoint_mismatch = MysqlPeerOperationIntent::new(
        operation("peer-endpoint-mismatch", 6),
        MysqlReplicaEpoch::new(1),
        ConfigurationId::new("configuration-1").unwrap(),
        MysqlPeerAuthorityFacts::new(
            target.clone(),
            target.process_session().clone(),
            EndpointBinding::new("substituted-endpoint").unwrap(),
            target.storage().clone(),
        ),
    );
    assert!(matches!(
        primary.engine.build_replica(endpoint_mismatch),
        Err(MysqlReplicatorError::IdentityMismatch)
    ));

    let storage_mismatch = MysqlPeerOperationIntent::new(
        operation("peer-storage-mismatch", 7),
        MysqlReplicaEpoch::new(1),
        ConfigurationId::new("configuration-1").unwrap(),
        MysqlPeerAuthorityFacts::new(
            target.clone(),
            target.process_session().clone(),
            target.endpoint().clone(),
            StorageBinding::new("substituted-storage").unwrap(),
        ),
    );
    assert!(matches!(
        primary.engine.build_replica(storage_mismatch),
        Err(MysqlReplicatorError::IdentityMismatch)
    ));

    let changed_identity = MysqlReplicaTarget::new(
        MysqlReplicaIdentity::new(
            target.identity().replica_id().clone(),
            ReplicaIncarnation::new("substituted-incarnation").unwrap(),
        ),
        target.member().clone(),
        target.process_session().clone(),
        target.endpoint().clone(),
        target.storage().clone(),
    );
    let changed_identity_operation = operation("peer-incarnation-mismatch", 8);
    let changed_identity_intent = MysqlPeerOperationIntent::new(
        changed_identity_operation.clone(),
        MysqlReplicaEpoch::new(1),
        ConfigurationId::new("configuration-1").unwrap(),
        peer_facts(changed_identity),
    );
    assert!(matches!(
        primary
            .engine
            .remove_replica(changed_identity_intent.clone()),
        Err(MysqlReplicatorError::InvalidConfiguration)
    ));
    assert!(matches!(
        primary.engine.remove_replica(changed_identity_intent),
        Err(MysqlReplicatorError::InvalidConfiguration)
    ));
    let changed_retry = MysqlPeerOperationIntent::new(
        changed_identity_operation,
        MysqlReplicaEpoch::new(1),
        ConfigurationId::new("configuration-1").unwrap(),
        peer_facts(target),
    );
    assert!(matches!(
        primary.engine.remove_replica(changed_retry),
        Err(MysqlReplicatorError::OperationConflict)
    ));
}

#[tokio::test]
async fn progress_projection_and_catch_up_remain_independent() {
    let gtid = "cccccccc-cccc-cccc-cccc-cccccccccccc:1-42";
    let mut harness = harness(vec![
        exact(MysqlReplicaRole::Secondary),
        exact(MysqlReplicaRole::Secondary),
        ReadPlan::Exact {
            role: MysqlReplicaRole::Secondary,
            executed: gtid,
        },
    ]);
    establish(&mut harness, MysqlReplicaRole::Secondary).await;
    let outcome = harness
        .engine
        .read_progress(operation("progress", 5))
        .await
        .unwrap();
    assert_eq!(
        match outcome.progress() {
            kuberic_mysql::core::ProgressProjection::Projectable(progress) => progress.value(),
            other => panic!("unexpected projection: {other:?}"),
        },
        42
    );
    assert_eq!(
        outcome.catch_up(),
        kuberic_mysql::core::CatchUpCapabilityResult::retained_evidence_unavailable()
    );
    assert_eq!(harness.engine.progress(), Some(outcome));
    assert_eq!(
        harness.engine.catch_up_result().unwrap(),
        kuberic_mysql::core::CatchUpCapabilityResult::retained_evidence_unavailable()
    );
}

#[tokio::test]
async fn progress_requires_current_role_configuration_authority_and_binding() {
    let mut unopened = harness(Vec::new());
    assert!(matches!(
        unopened
            .engine
            .read_progress(operation("unopened-progress", 1))
            .await,
        Err(MysqlReplicatorError::NotOpen)
    ));

    let mut pre_configuration = harness(vec![exact(MysqlReplicaRole::Secondary)]);
    open(&mut pre_configuration);
    pre_configuration
        .engine
        .advance_epoch(operation("pre-config-epoch", 2), MysqlReplicaEpoch::new(1))
        .unwrap();
    pre_configuration
        .engine
        .observe_role(
            operation("pre-config-role", 3),
            MysqlReplicaEpoch::new(1),
            MysqlReplicaRole::Secondary,
        )
        .await
        .unwrap();
    assert!(matches!(
        pre_configuration
            .engine
            .read_progress(operation("pre-config-progress", 4))
            .await,
        Err(MysqlReplicatorError::ConfigurationRequired)
    ));

    let mut stale_epoch = configured(
        MysqlReplicaRole::Secondary,
        vec![
            exact(MysqlReplicaRole::Secondary),
            exact(MysqlReplicaRole::Secondary),
        ],
    )
    .await;
    stale_epoch
        .engine
        .advance_epoch(
            operation("stale-progress-epoch", 5),
            MysqlReplicaEpoch::new(2),
        )
        .unwrap();
    assert!(matches!(
        stale_epoch
            .engine
            .read_progress(operation("stale-progress", 6))
            .await,
        Err(MysqlReplicatorError::ConfigurationRequired)
    ));

    let mut wrong_role = harness(vec![
        exact(MysqlReplicaRole::Secondary),
        exact(MysqlReplicaRole::Secondary),
        exact(MysqlReplicaRole::Primary),
    ]);
    establish(&mut wrong_role, MysqlReplicaRole::Secondary).await;
    assert!(matches!(
        wrong_role
            .engine
            .read_progress(operation("wrong-role-progress", 5))
            .await,
        Err(MysqlReplicatorError::NativeRoleMismatch)
    ));
    assert_eq!(wrong_role.engine.progress(), None);

    let mut stale_configuration = harness_with_receipts(
        vec![
            exact(MysqlReplicaRole::Secondary),
            exact(MysqlReplicaRole::Secondary),
            exact(MysqlReplicaRole::Secondary),
        ],
        vec![
            ReceiptPlan::Exact,
            ReceiptPlan::Exact,
            ReceiptPlan::CrossConfiguration,
        ],
        CleanupPlan::Completed,
    );
    establish(&mut stale_configuration, MysqlReplicaRole::Secondary).await;
    assert!(matches!(
        stale_configuration
            .engine
            .read_progress(operation("stale-config-progress", 5))
            .await,
        Err(MysqlReplicatorError::ReceiptMismatch)
    ));
    assert_eq!(stale_configuration.engine.progress(), None);

    let mut aborted = harness(Vec::new());
    open(&mut aborted);
    aborted
        .engine
        .abort(operation("progress-abort", 2))
        .unwrap();
    assert!(matches!(
        aborted
            .engine
            .read_progress(operation("aborted-progress", 3))
            .await,
        Err(MysqlReplicatorError::Terminal)
    ));
}

#[tokio::test]
async fn access_stays_closed_credentials_clear_and_close_requires_settlement() {
    let mut active = harness(vec![
        exact(MysqlReplicaRole::Primary),
        exact(MysqlReplicaRole::Primary),
    ]);
    open(&mut active);
    assert!(!active.engine.credentials_cleared());
    establish_after_open(&mut active, MysqlReplicaRole::Primary).await;
    assert!(active.engine.credentials_cleared());
    let access = active
        .engine
        .set_access_intent(
            operation("access", 5),
            MysqlAccessIntent::new(nonzero(2), true, true),
        )
        .unwrap();
    assert!(access.intent().read());
    assert!(access.intent().write());
    assert!(!access.is_open());
    assert!(matches!(
        active.engine.set_access_intent(
            operation("stale-access", 6),
            MysqlAccessIntent::new(nonzero(2), false, false),
        ),
        Err(MysqlReplicatorError::StaleAccessGeneration)
    ));
    let terminal = active.engine.close(operation("close", 6)).unwrap();
    assert_eq!(terminal.state(), MysqlTerminalState::Closed);
    assert_eq!(terminal.cleanup(), AttemptCleanupState::Completed);
    assert!(!active.engine.access_intent().unwrap().read());
    assert!(!active.engine.access_intent().unwrap().write());

    let mut unsettled = harness_with_cleanup(Vec::new(), CleanupPlan::NotRequired);
    open(&mut unsettled);
    assert!(matches!(
        unsettled.engine.close(operation("unsettled-close", 2)),
        Err(MysqlReplicatorError::CloseUnsettled)
    ));
    assert_eq!(unsettled.engine.terminal_state(), None);
}

#[tokio::test(flavor = "current_thread")]
async fn proxy_publication_requires_explicit_native_backed_revocation_before_lifecycle_progress() {
    let mut harness = harness(vec![
        exact(MysqlReplicaRole::Primary),
        exact(MysqlReplicaRole::Primary),
    ]);
    establish(&mut harness, MysqlReplicaRole::Primary).await;
    let credential = ApplicationCredential::new(
        harness.attempt.attempt().clone(),
        CredentialGeneration::new("application-generation").unwrap(),
        "application_local",
        "application-secret",
    )
    .unwrap();
    let address = harness
        .engine
        .prepare_client_proxy(
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            credential,
        )
        .await
        .unwrap();
    assert!(address.ip().is_loopback());
    let published = harness
        .engine
        .publish_access(
            operation("publish", 5),
            MysqlAccessIntent::new(nonzero(2), true, true),
        )
        .unwrap();
    assert_eq!(published.endpoint(), Some(address));
    assert!(published.is_open());
    let third = replica_target("third", "replicator", MysqlMemberIndex::Third);
    assert!(matches!(
        harness
            .engine
            .freeze_handoff_boundary(harness.peer.clone(), third),
        Err(MysqlReplicatorError::HandoffProofRequired)
    ));
    assert!(matches!(
        harness
            .engine
            .advance_epoch(operation("blocked-epoch", 6), MysqlReplicaEpoch::new(2)),
        Err(MysqlReplicatorError::AccessRevocationRequired)
    ));
    let closed = harness
        .engine
        .revoke_access(
            operation("revoke", 6),
            MysqlAccessIntent::new(nonzero(3), false, false),
        )
        .await
        .unwrap();
    assert!(!closed.is_open());
    assert!(closed.containment().unwrap().handoff_eligible());
    harness
        .engine
        .retire_contained_client_proxy()
        .await
        .unwrap();
    let replacement = ApplicationCredential::new(
        harness.attempt.attempt().clone(),
        CredentialGeneration::new("replacement-application-generation").unwrap(),
        "application_local",
        "application-secret",
    )
    .unwrap();
    harness
        .engine
        .prepare_client_proxy(
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            replacement,
        )
        .await
        .unwrap();
    harness
        .engine
        .publish_access(
            operation("replacement-publish", 7),
            MysqlAccessIntent::new(nonzero(4), true, true),
        )
        .unwrap();
    harness
        .engine
        .revoke_access(
            operation("replacement-revoke", 8),
            MysqlAccessIntent::new(nonzero(5), false, false),
        )
        .await
        .unwrap();
    let terminal = harness
        .engine
        .close_with_proxy(operation("proxy-close", 9))
        .await
        .unwrap();
    assert_eq!(terminal.state(), MysqlTerminalState::Closed);
}

#[tokio::test(flavor = "current_thread")]
async fn handoff_target_publication_requires_successor_acceptance() {
    let mut target = configured(
        MysqlReplicaRole::Secondary,
        vec![
            exact(MysqlReplicaRole::Secondary),
            exact(MysqlReplicaRole::Secondary),
            exact(MysqlReplicaRole::Primary),
            exact(MysqlReplicaRole::Primary),
        ],
    )
    .await;
    target
        .engine
        .advance_epoch(
            operation("handoff-target-epoch", 5),
            MysqlReplicaEpoch::new(2),
        )
        .unwrap();
    target
        .engine
        .observe_role(
            operation("handoff-target-role", 6),
            MysqlReplicaEpoch::new(2),
            MysqlReplicaRole::Primary,
        )
        .await
        .unwrap();
    target
        .engine
        .observe_configuration(
            operation("handoff-target-config", 7),
            configuration(&target, 2),
        )
        .await
        .unwrap();
    let credential = ApplicationCredential::new(
        target.attempt.attempt().clone(),
        CredentialGeneration::new("handoff-target-application").unwrap(),
        "application_local",
        "application-secret",
    )
    .unwrap();
    target
        .engine
        .prepare_client_proxy(
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            credential,
        )
        .await
        .unwrap();
    assert!(matches!(
        target.engine.publish_access(
            operation("handoff-target-publish", 8),
            MysqlAccessIntent::new(nonzero(2), true, true),
        ),
        Err(MysqlReplicatorError::HandoffProofRequired)
    ));
    assert!(!target.engine.access_intent().unwrap().write());
    target
        .engine
        .abort_with_proxy(operation("handoff-target-abort", 9))
        .await
        .unwrap();
}

struct PublicationScenario {
    _roots: [TestRoot; 3],
    source: MysqlReplicator<ScriptedEffects>,
    target: MysqlReplicator<ScriptedEffects>,
    remaining: MysqlReplicator<ScriptedEffects>,
    boundary: MysqlHandoffBoundary,
    target_outcome: MysqlHandoffOutcome,
    remaining_outcome: MysqlHandoffOutcome,
    source_receipt: MysqlSourceTerminalReceipt,
}

#[tokio::test(flavor = "current_thread")]
async fn combined_publication_proof_is_required_exact_and_single_use() {
    let mut scenario = publication_scenario(CleanupPlan::Completed).await;
    let credential = ApplicationCredential::new(
        scenario.boundary.source_authority().attempt().clone(),
        CredentialGeneration::new("target-publication-credential").unwrap(),
        "application_target",
        "application-secret",
    )
    .unwrap();
    scenario
        .target
        .prepare_client_proxy(
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            credential,
        )
        .await
        .unwrap();
    assert_publish_blocked(&mut scenario.target, "before-proof", 10);

    for (label, generation, proof) in [
        (
            "missing-target",
            10,
            publication_proof(&scenario, false, true, true),
        ),
        (
            "missing-remaining",
            11,
            publication_proof(&scenario, true, false, true),
        ),
        (
            "missing-source",
            12,
            publication_proof(&scenario, true, true, false),
        ),
    ] {
        assert!(matches!(
            scenario
                .target
                .accept_handoff_publication_proof(operation(label, generation), proof,),
            Err(MysqlReplicatorError::HandoffProofRequired)
        ));
        assert!(!scenario.target.access_intent().unwrap().write());
    }

    let substituted_remaining = MysqlHandoffPublicationProof::new(
        scenario.boundary.clone(),
        Some(scenario.target_outcome.clone()),
        Some(scenario.target_outcome.clone()),
        Some(scenario.source_receipt.clone()),
    );
    assert!(matches!(
        scenario.target.accept_handoff_publication_proof(
            operation("substituted-remaining", 13),
            substituted_remaining,
        ),
        Err(MysqlReplicatorError::HandoffProofRequired)
    ));
    assert_publish_blocked(&mut scenario.target, "after-substitution", 14);

    let accepted = scenario
        .target
        .accept_handoff_publication_proof(
            operation("combined-proof", 14),
            publication_proof(&scenario, true, true, true),
        )
        .unwrap();
    assert_eq!(accepted.target(), scenario.boundary.target().identity());
    assert_eq!(accepted.boundary(), &scenario.boundary);
    assert!(matches!(
        scenario.target.accept_handoff_publication_proof(
            operation("duplicate-proof", 15),
            publication_proof(&scenario, true, true, true),
        ),
        Err(MysqlReplicatorError::HandoffProofDuplicate)
    ));
    assert!(matches!(
        scenario.target.accept_handoff_publication_proof(
            operation("conflicting-proof", 15),
            publication_proof(&scenario, true, true, false),
        ),
        Err(MysqlReplicatorError::HandoffProofConflict)
    ));
    let published = scenario
        .target
        .publish_access(
            operation("publish-after-proof", 15),
            MysqlAccessIntent::new(nonzero(2), true, true),
        )
        .unwrap();
    assert!(published.is_open());
    scenario
        .target
        .revoke_access(
            operation("revoke-after-proof", 16),
            MysqlAccessIntent::new(nonzero(3), false, false),
        )
        .await
        .unwrap();
    scenario
        .target
        .close_with_proxy(operation("target-close", 17))
        .await
        .unwrap();
    scenario
        .remaining
        .close(operation("remaining-close", 10))
        .unwrap();
    assert_eq!(
        scenario.source.terminal_state(),
        Some(MysqlTerminalState::Closed)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn publication_rejects_substituted_target_remaining_and_source_receipts() {
    let mut scenario = publication_scenario(CleanupPlan::Completed).await;
    let substituted_source = scenario
        .remaining
        .close(operation("substituted-source-close", 9))
        .unwrap()
        .receipt()
        .clone();
    for (generation, proof) in [
        (
            10,
            MysqlHandoffPublicationProof::new(
                scenario.boundary.clone(),
                Some(scenario.remaining_outcome.clone()),
                Some(scenario.remaining_outcome.clone()),
                Some(scenario.source_receipt.clone()),
            ),
        ),
        (
            11,
            MysqlHandoffPublicationProof::new(
                scenario.boundary.clone(),
                Some(scenario.target_outcome.clone()),
                Some(scenario.target_outcome.clone()),
                Some(scenario.source_receipt.clone()),
            ),
        ),
        (
            12,
            MysqlHandoffPublicationProof::new(
                scenario.boundary.clone(),
                Some(scenario.target_outcome.clone()),
                Some(scenario.remaining_outcome.clone()),
                Some(substituted_source),
            ),
        ),
    ] {
        assert!(matches!(
            scenario.target.accept_handoff_publication_proof(
                operation(&format!("invalid-publication-{generation}"), generation),
                proof,
            ),
            Err(MysqlReplicatorError::HandoffProofRequired)
        ));
        assert!(!scenario.target.access_intent().unwrap().write());
    }
    scenario
        .target
        .abort(operation("invalid-target-abort", 13))
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn successor_faults_use_real_receipt_validation_and_never_enable_publication() {
    let expected_roles = [
        (MysqlMemberIndex::First, MemberRole::Primary),
        (MysqlMemberIndex::Third, MemberRole::Secondary),
    ];
    let wrong_primary = [
        (MysqlMemberIndex::First, MemberRole::Secondary),
        (MysqlMemberIndex::Third, MemberRole::Primary),
    ];
    let missing_remaining = [(MysqlMemberIndex::First, MemberRole::Primary)];
    let unexpected_source = [
        (MysqlMemberIndex::First, MemberRole::Primary),
        (MysqlMemberIndex::Second, MemberRole::Secondary),
        (MysqlMemberIndex::Third, MemberRole::Secondary),
    ];
    for (label, view, members, executed, substitute_authority) in [
        (
            "wrong-successor-view",
            "91:3",
            expected_roles.as_slice(),
            format!("{GROUP_UUID}:1-5"),
            false,
        ),
        (
            "wrong-primary",
            "91:2",
            wrong_primary.as_slice(),
            format!("{GROUP_UUID}:1-5"),
            false,
        ),
        (
            "unexpected-additional-loss",
            "91:2",
            missing_remaining.as_slice(),
            format!("{GROUP_UUID}:1-5"),
            false,
        ),
        (
            "wrong-successor-members",
            "91:2",
            unexpected_source.as_slice(),
            format!("{GROUP_UUID}:1-5"),
            false,
        ),
        (
            "missing-history",
            "91:2",
            expected_roles.as_slice(),
            format!("{GROUP_UUID}:1-4"),
            false,
        ),
        (
            "changed-target-authority",
            "91:2",
            expected_roles.as_slice(),
            format!("{GROUP_UUID}:1-5"),
            true,
        ),
    ] {
        let (_root, mut target, boundary) =
            target_ready_for_successor(view, members, &executed, substitute_authority).await;
        assert!(
            target
                .accept_handoff_successor(operation(label, 8), boundary)
                .await
                .is_err()
        );
        assert_publish_blocked(&mut target, &format!("{label}-publish"), 9);
        assert!(!target.access_intent().unwrap().write());
        target
            .abort(operation(&format!("{label}-abort"), 9))
            .unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn source_stop_reap_and_cleanup_fail_before_terminal_receipt() {
    for (label, plan, expected) in [
        (
            "stop",
            CleanupPlan::StopFailed,
            LifecycleOperation::Shutdown,
        ),
        (
            "reap",
            CleanupPlan::ReapFailed,
            LifecycleOperation::Shutdown,
        ),
        (
            "cleanup",
            CleanupPlan::Failed,
            LifecycleOperation::ScratchCleanup,
        ),
    ] {
        let mut harness = harness_with_cleanup(
            vec![
                exact(MysqlReplicaRole::Primary),
                exact(MysqlReplicaRole::Primary),
            ],
            plan,
        );
        open(&mut harness);
        establish_after_open(&mut harness, MysqlReplicaRole::Primary).await;
        let result = harness
            .engine
            .close(operation(&format!("{label}-close"), 5));
        assert!(matches!(
            result,
            Err(MysqlReplicatorError::Instance(
                MysqlInstanceError::Timeout(operation)
                    | MysqlInstanceError::ChildFailure { operation, .. }
                    | MysqlInstanceError::Io { operation, .. }
            )) if operation == expected
        ));
        assert!(!harness.engine.access_intent().unwrap().write());
    }
}

#[test]
fn abort_is_immediate_under_cancellation_and_terminal_even_when_cleanup_fails() {
    let mut cancelled = harness(Vec::new());
    open(&mut cancelled);
    cancelled.attempt.cancel();
    let outcome = cancelled.engine.abort(operation("abort", 2)).unwrap();
    assert_eq!(outcome.state(), MysqlTerminalState::Aborted);
    assert_eq!(
        cancelled.engine.terminal_state(),
        Some(MysqlTerminalState::Aborted)
    );
    assert!(cancelled.engine.credentials_cleared());

    let mut failed = harness_with_cleanup(Vec::new(), CleanupPlan::Failed);
    open(&mut failed);
    assert!(matches!(
        failed.engine.abort(operation("abort-failed", 2)),
        Err(MysqlReplicatorError::Instance(_))
    ));
    assert_eq!(
        failed.engine.terminal_state(),
        Some(MysqlTerminalState::Aborted)
    );
}

#[tokio::test]
async fn cancellation_and_deadline_reject_new_work() {
    let mut cancelled = harness(Vec::new());
    open(&mut cancelled);
    cancelled.attempt.cancel();
    assert!(matches!(
        cancelled
            .engine
            .advance_epoch(operation("cancelled", 2), MysqlReplicaEpoch::new(1)),
        Err(MysqlReplicatorError::Cancelled)
    ));

    let root = TestRoot::new("deadline");
    let config = root.config_for_member(MysqlMemberIndex::First);
    let now = Instant::now();
    let attempt = AttemptContext::new_at(
        AttemptId::new("deadline").unwrap(),
        now,
        now + Duration::from_millis(20),
        Duration::from_millis(10),
    )
    .unwrap();
    let authority = authority_with_attempt(&attempt, MysqlMemberIndex::First);
    let identity = replica_identity("deadline");
    let mut engine = engine(
        config,
        identity.clone(),
        attempt,
        Vec::new(),
        Vec::new(),
        CleanupPlan::Completed,
    );
    std::thread::sleep(Duration::from_millis(12));
    assert!(matches!(
        engine.open(
            operation("deadline-open", 1),
            MysqlOpenReceipt::new(identity, authority, nonzero(1)),
        ),
        Err(MysqlReplicatorError::Deadline(_))
    ));
}

#[tokio::test]
async fn every_operation_rejects_after_terminal_boundary() {
    let mut harness = harness(Vec::new());
    open(&mut harness);
    harness
        .engine
        .abort(operation("terminal-abort", 2))
        .unwrap();
    let terminal = |result: Result<(), MysqlReplicatorError>| {
        assert!(matches!(result, Err(MysqlReplicatorError::Terminal)));
    };

    terminal(
        harness
            .engine
            .open(operation("after-open", 3), open_receipt(&harness))
            .map(|_| ()),
    );
    terminal(
        harness
            .engine
            .advance_epoch(operation("after-epoch", 3), MysqlReplicaEpoch::new(1))
            .map(|_| ()),
    );
    terminal(
        harness
            .engine
            .observe_role(
                operation("after-role", 3),
                MysqlReplicaEpoch::new(1),
                MysqlReplicaRole::Secondary,
            )
            .await
            .map(|_| ()),
    );
    terminal(
        harness
            .engine
            .observe_configuration(operation("after-config", 3), configuration(&harness, 1))
            .await
            .map(|_| ()),
    );
    terminal(
        harness
            .engine
            .read_progress(operation("after-progress", 3))
            .await
            .map(|_| ()),
    );
    terminal(harness.engine.catch_up_result().map(|_| ()));
    terminal(
        harness
            .engine
            .set_access_intent(
                operation("after-access", 3),
                MysqlAccessIntent::new(nonzero(2), true, true),
            )
            .map(|_| ()),
    );
    terminal(
        harness
            .engine
            .build_replica(peer_intent(&harness, "after-build", 3, false))
            .map(|_| ()),
    );
    terminal(
        harness
            .engine
            .remove_replica(peer_intent(&harness, "after-remove", 3, false))
            .map(|_| ()),
    );
    terminal(
        harness
            .engine
            .close(operation("after-close", 3))
            .map(|_| ()),
    );
    terminal(
        harness
            .engine
            .abort(operation("after-abort", 3))
            .map(|_| ()),
    );
}

fn harness(reads: Vec<ReadPlan>) -> Harness {
    harness_with_cleanup(reads, CleanupPlan::Completed)
}

async fn publication_scenario(cleanup: CleanupPlan) -> PublicationScenario {
    let roots = [
        TestRoot::new("proof-source"),
        TestRoot::new("proof-target"),
        TestRoot::new("proof-remaining"),
    ];
    let attempts = [
        context("publication-proof"),
        context("publication-proof"),
        context("publication-proof"),
    ];
    let authorities = [
        authority_with_attempt(&attempts[0], MysqlMemberIndex::First),
        authority_with_attempt(&attempts[1], MysqlMemberIndex::Second),
        authority_with_attempt(&attempts[2], MysqlMemberIndex::Third),
    ];
    let identities = [
        replica_identity("proof-source"),
        replica_identity("proof-target"),
        replica_identity("proof-remaining"),
    ];
    let targets = [
        target_from_authority(identities[0].clone(), &authorities[0]),
        target_from_authority(identities[1].clone(), &authorities[1]),
        target_from_authority(identities[2].clone(), &authorities[2]),
    ];
    let predecessor_roles = [
        (MysqlMemberIndex::First, MemberRole::Primary),
        (MysqlMemberIndex::Second, MemberRole::Secondary),
        (MysqlMemberIndex::Third, MemberRole::Secondary),
    ];
    let transferred_roles = [
        (MysqlMemberIndex::First, MemberRole::Secondary),
        (MysqlMemberIndex::Second, MemberRole::Primary),
        (MysqlMemberIndex::Third, MemberRole::Secondary),
    ];
    let source_reads = vec![
        ReadPlan::Evidence(Box::new(evidence_from_members(
            authorities[0].clone(),
            "71:1",
            &predecessor_roles,
            &format!("{GROUP_UUID}:1-5"),
        ))),
        ReadPlan::Evidence(Box::new(evidence_from_members(
            authorities[0].clone(),
            "71:1",
            &predecessor_roles,
            &format!("{GROUP_UUID}:1-5"),
        ))),
        ReadPlan::Evidence(Box::new(evidence_from_members(
            authorities[0].clone(),
            "71:1",
            &predecessor_roles,
            &format!("{GROUP_UUID}:1-5"),
        ))),
        ReadPlan::Evidence(Box::new(evidence_from_members(
            authorities[0].clone(),
            "71:1",
            &transferred_roles,
            &format!("{GROUP_UUID}:1-5"),
        ))),
    ];
    let target_reads = vec![
        ReadPlan::Evidence(Box::new(evidence_from_members(
            authorities[1].clone(),
            "71:1",
            &predecessor_roles,
            &format!("{GROUP_UUID}:1-5"),
        ))),
        ReadPlan::Evidence(Box::new(evidence_from_members(
            authorities[1].clone(),
            "71:1",
            &predecessor_roles,
            &format!("{GROUP_UUID}:1-5"),
        ))),
        ReadPlan::Evidence(Box::new(evidence_from_members(
            authorities[1].clone(),
            "71:1",
            &transferred_roles,
            &format!("{GROUP_UUID}:1-5"),
        ))),
        ReadPlan::Evidence(Box::new(evidence_from_members(
            authorities[1].clone(),
            "71:1",
            &transferred_roles,
            &format!("{GROUP_UUID}:1-5"),
        ))),
        ReadPlan::Evidence(Box::new(evidence_from_members(
            authorities[1].clone(),
            "71:2",
            &transferred_roles[1..],
            &format!("{GROUP_UUID}:1-5"),
        ))),
    ];
    let remaining_reads = vec![
        ReadPlan::Evidence(Box::new(evidence_from_members(
            authorities[2].clone(),
            "71:1",
            &predecessor_roles,
            &format!("{GROUP_UUID}:1-5"),
        ))),
        ReadPlan::Evidence(Box::new(evidence_from_members(
            authorities[2].clone(),
            "71:1",
            &predecessor_roles,
            &format!("{GROUP_UUID}:1-5"),
        ))),
        ReadPlan::Evidence(Box::new(evidence_from_members(
            authorities[2].clone(),
            "71:1",
            &transferred_roles,
            &format!("{GROUP_UUID}:1-5"),
        ))),
        ReadPlan::Evidence(Box::new(evidence_from_members(
            authorities[2].clone(),
            "71:1",
            &transferred_roles,
            &format!("{GROUP_UUID}:1-5"),
        ))),
        ReadPlan::Evidence(Box::new(evidence_from_members(
            authorities[2].clone(),
            "71:2",
            &transferred_roles[1..],
            &format!("{GROUP_UUID}:1-5"),
        ))),
    ];
    let mut source = engine(
        roots[0].config_for_member(MysqlMemberIndex::First),
        identities[0].clone(),
        attempts[0].clone(),
        source_reads,
        Vec::new(),
        cleanup,
    );
    let mut target = engine(
        roots[1].config_for_member(MysqlMemberIndex::Second),
        identities[1].clone(),
        attempts[1].clone(),
        target_reads,
        Vec::new(),
        CleanupPlan::Completed,
    );
    let mut remaining = engine(
        roots[2].config_for_member(MysqlMemberIndex::Third),
        identities[2].clone(),
        attempts[2].clone(),
        remaining_reads,
        Vec::new(),
        CleanupPlan::Completed,
    );
    for (engine, identity, authority) in [
        (&mut source, &identities[0], &authorities[0]),
        (&mut target, &identities[1], &authorities[1]),
        (&mut remaining, &identities[2], &authorities[2]),
    ] {
        engine
            .open(
                operation(&format!("open-{}", identity.replica_id().as_str()), 1),
                MysqlOpenReceipt::new(identity.clone(), authority.clone(), nonzero(1)),
            )
            .unwrap();
    }
    let predecessor_configuration = MysqlReplicaConfiguration::new(
        ConfigurationId::new("proof-configuration-1").unwrap(),
        MysqlReplicaEpoch::new(1),
        targets.to_vec(),
    );
    for (engine, role, label) in [
        (&mut source, MysqlReplicaRole::Primary, "source"),
        (&mut target, MysqlReplicaRole::Secondary, "target"),
        (&mut remaining, MysqlReplicaRole::Secondary, "remaining"),
    ] {
        engine
            .advance_epoch(
                operation(&format!("{label}-epoch-1"), 2),
                MysqlReplicaEpoch::new(1),
            )
            .unwrap();
        engine
            .observe_role(
                operation(&format!("{label}-role-1"), 3),
                MysqlReplicaEpoch::new(1),
                role,
            )
            .await
            .unwrap();
        engine
            .observe_configuration(
                operation(&format!("{label}-configuration-1"), 4),
                predecessor_configuration.clone(),
            )
            .await
            .unwrap();
    }

    let credential = ApplicationCredential::new(
        attempts[0].attempt().clone(),
        CredentialGeneration::new("source-publication-credential").unwrap(),
        "application_source",
        "application-secret",
    )
    .unwrap();
    source
        .prepare_client_proxy(
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            credential,
        )
        .await
        .unwrap();
    source
        .publish_access(
            operation("source-publish", 5),
            MysqlAccessIntent::new(nonzero(2), true, true),
        )
        .unwrap();
    source
        .revoke_access(
            operation("source-revoke", 6),
            MysqlAccessIntent::new(nonzero(3), false, false),
        )
        .await
        .unwrap();
    source
        .read_progress(operation("source-progress", 7))
        .await
        .unwrap();
    let transfer_source = source.source_boundary().unwrap();
    let boundary = source
        .freeze_handoff_boundary(targets[1].clone(), targets[2].clone())
        .unwrap();
    source.retire_contained_client_proxy().await.unwrap();

    for (engine, label, generation) in [
        (&mut source, "source", 8),
        (&mut target, "target", 5),
        (&mut remaining, "remaining", 5),
    ] {
        engine
            .advance_epoch(
                operation(&format!("{label}-epoch-2"), generation),
                MysqlReplicaEpoch::new(2),
            )
            .unwrap();
    }
    source
        .transfer_role(
            operation("source-transfer", 9),
            MysqlReplicaEpoch::new(2),
            MysqlReplicaRole::Secondary,
            transfer_source,
            targets[1].member().clone(),
        )
        .await
        .unwrap();
    target
        .observe_role(
            operation("target-role-2", 6),
            MysqlReplicaEpoch::new(2),
            MysqlReplicaRole::Primary,
        )
        .await
        .unwrap();
    remaining
        .observe_role(
            operation("remaining-role-2", 6),
            MysqlReplicaEpoch::new(2),
            MysqlReplicaRole::Secondary,
        )
        .await
        .unwrap();
    let successor_configuration = MysqlReplicaConfiguration::new(
        ConfigurationId::new("proof-configuration-2").unwrap(),
        MysqlReplicaEpoch::new(2),
        targets.to_vec(),
    );
    source
        .observe_configuration(
            operation("source-configuration-2", 10),
            successor_configuration.clone(),
        )
        .await
        .unwrap();
    target
        .observe_configuration(
            operation("target-configuration-2", 7),
            successor_configuration.clone(),
        )
        .await
        .unwrap();
    remaining
        .observe_configuration(
            operation("remaining-configuration-2", 7),
            successor_configuration,
        )
        .await
        .unwrap();
    let source_terminal = source.close(operation("source-close", 11)).unwrap();
    let remaining_outcome = remaining
        .accept_handoff_successor(operation("remaining-successor", 8), boundary.clone())
        .await
        .unwrap();
    let target_outcome = target
        .accept_handoff_successor(operation("target-successor", 8), boundary.clone())
        .await
        .unwrap();
    PublicationScenario {
        _roots: roots,
        source,
        target,
        remaining,
        boundary,
        target_outcome,
        remaining_outcome,
        source_receipt: source_terminal.receipt().clone(),
    }
}

fn publication_proof(
    scenario: &PublicationScenario,
    target: bool,
    remaining: bool,
    source: bool,
) -> MysqlHandoffPublicationProof {
    MysqlHandoffPublicationProof::new(
        scenario.boundary.clone(),
        target.then(|| scenario.target_outcome.clone()),
        remaining.then(|| scenario.remaining_outcome.clone()),
        source.then(|| scenario.source_receipt.clone()),
    )
}

fn assert_publish_blocked(
    target: &mut MysqlReplicator<ScriptedEffects>,
    label: &str,
    generation: u64,
) {
    assert!(matches!(
        target.publish_access(
            operation(label, generation),
            MysqlAccessIntent::new(nonzero(2), true, true),
        ),
        Err(MysqlReplicatorError::HandoffProofRequired)
    ));
    assert!(!target.access_intent().unwrap().write());
}

async fn target_ready_for_successor(
    successor_view: &str,
    successor_members: &[(MysqlMemberIndex, MemberRole)],
    successor_executed: &str,
    substitute_authority: bool,
) -> (
    TestRoot,
    MysqlReplicator<ScriptedEffects>,
    MysqlHandoffBoundary,
) {
    let root = TestRoot::new("successor-fault");
    let attempt = context("successor-fault");
    let target_authority = authority_with_attempt(&attempt, MysqlMemberIndex::First);
    let source_authority = authority_with_attempt(&attempt, MysqlMemberIndex::Second);
    let remaining_authority = authority_with_attempt(&attempt, MysqlMemberIndex::Third);
    let target_identity = replica_identity("successor-target");
    let source_identity = replica_identity("successor-source");
    let remaining_identity = replica_identity("successor-remaining");
    let targets = [
        target_from_authority(target_identity.clone(), &target_authority),
        target_from_authority(source_identity, &source_authority),
        target_from_authority(remaining_identity, &remaining_authority),
    ];
    let predecessor_roles = [
        (MysqlMemberIndex::First, MemberRole::Secondary),
        (MysqlMemberIndex::Second, MemberRole::Primary),
        (MysqlMemberIndex::Third, MemberRole::Secondary),
    ];
    let promoted_roles = [
        (MysqlMemberIndex::First, MemberRole::Primary),
        (MysqlMemberIndex::Second, MemberRole::Secondary),
        (MysqlMemberIndex::Third, MemberRole::Secondary),
    ];
    let successor_authority = if substitute_authority {
        authority("substituted-successor", MysqlMemberIndex::First)
    } else {
        target_authority.clone()
    };
    let reads = vec![
        ReadPlan::Evidence(Box::new(evidence_from_members(
            target_authority.clone(),
            "91:1",
            &predecessor_roles,
            &format!("{GROUP_UUID}:1-5"),
        ))),
        ReadPlan::Evidence(Box::new(evidence_from_members(
            target_authority.clone(),
            "91:1",
            &predecessor_roles,
            &format!("{GROUP_UUID}:1-5"),
        ))),
        ReadPlan::Evidence(Box::new(evidence_from_members(
            target_authority.clone(),
            "91:1",
            &promoted_roles,
            &format!("{GROUP_UUID}:1-5"),
        ))),
        ReadPlan::Evidence(Box::new(evidence_from_members(
            target_authority.clone(),
            "91:1",
            &promoted_roles,
            &format!("{GROUP_UUID}:1-5"),
        ))),
        ReadPlan::Evidence(Box::new(evidence_from_members(
            successor_authority,
            successor_view,
            successor_members,
            successor_executed,
        ))),
    ];
    let mut target = engine(
        root.config_for_member(MysqlMemberIndex::First),
        target_identity.clone(),
        attempt,
        reads,
        Vec::new(),
        CleanupPlan::Completed,
    );
    target
        .open(
            operation("successor-open", 1),
            MysqlOpenReceipt::new(target_identity, target_authority.clone(), nonzero(1)),
        )
        .unwrap();
    let predecessor_configuration = MysqlReplicaConfiguration::new(
        ConfigurationId::new("successor-configuration-1").unwrap(),
        MysqlReplicaEpoch::new(1),
        targets.to_vec(),
    );
    target
        .advance_epoch(operation("successor-epoch-1", 2), MysqlReplicaEpoch::new(1))
        .unwrap();
    target
        .observe_role(
            operation("successor-role-1", 3),
            MysqlReplicaEpoch::new(1),
            MysqlReplicaRole::Secondary,
        )
        .await
        .unwrap();
    target
        .observe_configuration(
            operation("successor-configuration-1", 4),
            predecessor_configuration.clone(),
        )
        .await
        .unwrap();
    let executed = GtidSet::from_str(&format!("{GROUP_UUID}:1-5")).unwrap();
    let progress =
        match project_progress(&ConfiguredGtidLineage::new(GROUP_UUID).unwrap(), &executed) {
            ProgressProjection::Projectable(progress) => progress,
            ProgressProjection::NonProjectable(reason) => {
                panic!("test boundary must project: {reason:?}")
            }
        };
    let boundary = MysqlHandoffBoundary::new(
        predecessor_configuration,
        MysqlReplicaEpoch::new(1),
        source_authority,
        targets[1].clone(),
        targets[0].clone(),
        targets[2].clone(),
        ViewId::new("91:1").unwrap(),
        executed,
        progress,
    );
    target
        .advance_epoch(operation("successor-epoch-2", 5), MysqlReplicaEpoch::new(2))
        .unwrap();
    target
        .observe_role(
            operation("successor-role-2", 6),
            MysqlReplicaEpoch::new(2),
            MysqlReplicaRole::Primary,
        )
        .await
        .unwrap();
    target
        .observe_configuration(
            operation("successor-configuration-2", 7),
            MysqlReplicaConfiguration::new(
                ConfigurationId::new("successor-configuration-2").unwrap(),
                MysqlReplicaEpoch::new(2),
                targets.to_vec(),
            ),
        )
        .await
        .unwrap();
    (root, target, boundary)
}

fn harness_with_cleanup(reads: Vec<ReadPlan>, cleanup: CleanupPlan) -> Harness {
    harness_with_receipts(reads, Vec::new(), cleanup)
}

fn harness_with_receipts(
    reads: Vec<ReadPlan>,
    receipts: Vec<ReceiptPlan>,
    cleanup: CleanupPlan,
) -> Harness {
    let root = TestRoot::new("replicat");
    let config = root.config_for_member(MysqlMemberIndex::First);
    let attempt = context("replicator");
    let authority = authority_with_attempt(&attempt, MysqlMemberIndex::First);
    let identity = replica_identity("local");
    let local_binding = authority.local().binding();
    let local = MysqlReplicaTarget::new(
        identity.clone(),
        MemberPeer::from_enrollment(authority.local()),
        local_binding.process_session().clone(),
        local_binding.endpoint().clone(),
        local_binding.storage().clone(),
    );
    let peer_identity = replica_identity("peer");
    let peer_enrollment = enrollment("replicator", MysqlMemberIndex::Second);
    let peer_binding = peer_enrollment.binding();
    let peer = MysqlReplicaTarget::new(
        peer_identity,
        MemberPeer::from_enrollment(&peer_enrollment),
        peer_binding.process_session().clone(),
        peer_binding.endpoint().clone(),
        peer_binding.storage().clone(),
    );
    let engine = engine(
        config,
        identity.clone(),
        attempt.clone(),
        reads,
        receipts,
        cleanup,
    );
    Harness {
        _root: root,
        engine,
        authority,
        identity,
        local,
        peer,
        attempt,
    }
}

fn engine(
    config: MysqlInstanceConfig,
    identity: MysqlReplicaIdentity,
    attempt: AttemptContext,
    reads: Vec<ReadPlan>,
    receipts: Vec<ReceiptPlan>,
    cleanup: CleanupPlan,
) -> MysqlReplicator<ScriptedEffects> {
    let observer = credential(&attempt, ControlCredentialRole::Observer);
    let recovery = credential(&attempt, ControlCredentialRole::Recovery);
    MysqlReplicator::new(
        ScriptedEffects::new(config, reads, receipts, cleanup),
        identity,
        kuberic_mysql::core::ConfiguredGtidLineage::new(GROUP_UUID).unwrap(),
        observer,
        recovery,
        attempt,
    )
    .unwrap()
}

async fn run_failed_formation_effect(
    label: &str,
    member: MysqlMemberIndex,
    peer_count: usize,
    plan: FormationFailurePlan,
    counters: [Arc<AtomicUsize>; 3],
) -> (
    Result<MysqlConfigurationOutcome, MysqlReplicatorError>,
    [MysqlReplicator<ScriptedEffects>; 3],
) {
    let FormationFailurePlan {
        mutation,
        receipts,
        cleanup,
        attempt: attempt_override,
        cancel_before_invoke,
    } = plan;
    let roots = [
        TestRoot::new(&format!("{label}-one")),
        TestRoot::new(&format!("{label}-two")),
        TestRoot::new(&format!("{label}-three")),
    ];
    let attempt = attempt_override.unwrap_or_else(|| context(&format!("{label}-attempt")));
    let authorities = MEMBERS.map(|member| authority_with_attempt(&attempt, member));
    let identities =
        MEMBERS.map(|candidate| replica_identity(&format!("{label}-{}", member_number(candidate))));
    let failed = member_number(member) - 1;
    let mut engines: [MysqlReplicator<ScriptedEffects>; 3] = std::array::from_fn(|index| {
        MysqlReplicator::new(
            ScriptedEffects::for_formation(
                roots[index].config_for_member(MEMBERS[index]),
                Vec::new(),
                if index == failed {
                    receipts.clone()
                } else {
                    Vec::new()
                },
                if index == failed && member == MysqlMemberIndex::First {
                    mutation
                } else {
                    MutationPlan::Exact
                },
                if index == failed && member != MysqlMemberIndex::First {
                    mutation
                } else {
                    MutationPlan::Exact
                },
                if index == failed {
                    cleanup
                } else {
                    CleanupPlan::Completed
                },
                counters[index].clone(),
            ),
            identities[index].clone(),
            kuberic_mysql::core::ConfiguredGtidLineage::new(GROUP_UUID).unwrap(),
            credential(&attempt, ControlCredentialRole::Observer),
            credential(&attempt, ControlCredentialRole::Recovery),
            attempt.clone(),
        )
        .unwrap()
    });
    for index in 0..3 {
        engines[index]
            .open(
                operation(&format!("{label}-open-{index}"), 1),
                MysqlOpenReceipt::new(
                    identities[index].clone(),
                    authorities[index].clone(),
                    nonzero(1),
                ),
            )
            .unwrap();
        engines[index]
            .advance_epoch(
                operation(&format!("{label}-epoch-{index}"), 2),
                MysqlReplicaEpoch::new(1),
            )
            .unwrap();
        engines[index]
            .intend_formation_role(
                operation(&format!("{label}-role-{index}"), 3),
                MysqlReplicaEpoch::new(1),
                if index == 0 {
                    MysqlReplicaRole::Primary
                } else {
                    MysqlReplicaRole::Secondary
                },
            )
            .unwrap();
    }
    if cancel_before_invoke {
        attempt.cancel();
    }
    let result = invoke_formation_effect(
        &mut engines[failed],
        &attempt,
        &identities[failed],
        &authorities[failed],
        peer_count,
    )
    .await;
    (result, engines)
}

fn assert_only_local_owner_aborted(
    failed_member: MysqlMemberIndex,
    engines: &[MysqlReplicator<ScriptedEffects>; 3],
    counters: &[Arc<AtomicUsize>; 3],
) {
    let failed = member_number(failed_member) - 1;
    for (index, engine) in engines.iter().enumerate() {
        assert_eq!(
            engine.terminal_state(),
            (index == failed).then_some(MysqlTerminalState::Aborted)
        );
        assert_eq!(
            counters[index].load(Ordering::Relaxed),
            usize::from(index == failed)
        );
    }
}

async fn invoke_formation_effect(
    engine: &mut MysqlReplicator<ScriptedEffects>,
    attempt: &AttemptContext,
    identity: &MysqlReplicaIdentity,
    authority: &MemberMutationAuthority,
    peer_count: usize,
) -> Result<MysqlConfigurationOutcome, MysqlReplicatorError> {
    if peer_count == 0 {
        return engine
            .bootstrap_configuration(
                operation("invoke-bootstrap", 4),
                formation_configuration(
                    "invoke-bootstrap",
                    1,
                    vec![target_from_authority(identity.clone(), authority)],
                ),
            )
            .await;
    }
    let peer_members = &MEMBERS[..peer_count];
    let peer_authorities = peer_members
        .iter()
        .map(|member| authority_with_attempt(attempt, *member))
        .collect::<Vec<_>>();
    let mut targets = peer_authorities
        .iter()
        .enumerate()
        .map(|(index, authority)| {
            target_from_authority(replica_identity(&format!("invoke-peer-{index}")), authority)
        })
        .collect::<Vec<_>>();
    targets.push(target_from_authority(identity.clone(), authority));
    let source_authority = peer_authorities
        .last()
        .expect("join formation has at least one predecessor")
        .clone();
    let source_evidence = evidence_from_members(
        source_authority.clone(),
        "81:1",
        &peer_members
            .iter()
            .enumerate()
            .map(|(index, member)| {
                (
                    *member,
                    if index == 0 {
                        MemberRole::Primary
                    } else {
                        MemberRole::Secondary
                    },
                )
            })
            .collect::<Vec<_>>(),
        &format!("{GROUP_UUID}:1"),
    );
    let source = MemberSourceBoundary::new(
        MemberPeer::from_enrollment(source_authority.local()),
        source_evidence,
    )
    .unwrap();
    engine
        .join_configuration(
            operation("invoke-join", 4),
            formation_configuration("invoke-join", 1, targets),
            source,
            peer_authorities
                .iter()
                .map(|authority| MemberPeer::from_enrollment(authority.local()))
                .collect(),
        )
        .await
}

fn open(harness: &mut Harness) {
    harness
        .engine
        .open(operation("open", 1), open_receipt(harness))
        .unwrap();
}

async fn establish(harness: &mut Harness, role: MysqlReplicaRole) {
    open(harness);
    establish_after_open(harness, role).await;
}

async fn establish_after_open(harness: &mut Harness, role: MysqlReplicaRole) {
    harness
        .engine
        .advance_epoch(operation("epoch", 2), MysqlReplicaEpoch::new(1))
        .unwrap();
    harness
        .engine
        .observe_role(operation("role", 3), MysqlReplicaEpoch::new(1), role)
        .await
        .unwrap();
    harness
        .engine
        .observe_configuration(operation("config", 4), configuration(harness, 1))
        .await
        .unwrap();
}

async fn configured(role: MysqlReplicaRole, reads: Vec<ReadPlan>) -> Harness {
    let mut harness = harness(reads);
    establish(&mut harness, role).await;
    harness
}

fn open_receipt(harness: &Harness) -> MysqlOpenReceipt {
    MysqlOpenReceipt::new(
        harness.identity.clone(),
        harness.authority.clone(),
        nonzero(1),
    )
}

fn configuration(harness: &Harness, epoch: u64) -> MysqlReplicaConfiguration {
    MysqlReplicaConfiguration::new(
        ConfigurationId::new(format!("configuration-{epoch}")).unwrap(),
        MysqlReplicaEpoch::new(epoch),
        vec![harness.local.clone(), harness.peer.clone()],
    )
}

fn local_only_configuration(harness: &Harness, epoch: u64) -> MysqlReplicaConfiguration {
    MysqlReplicaConfiguration::new(
        ConfigurationId::new(format!("configuration-{epoch}")).unwrap(),
        MysqlReplicaEpoch::new(epoch),
        vec![harness.local.clone()],
    )
}

fn peer_intent(
    harness: &Harness,
    label: &str,
    generation: u64,
    changed: bool,
) -> MysqlPeerOperationIntent {
    let target = harness.peer.clone();
    MysqlPeerOperationIntent::new(
        operation(label, generation),
        MysqlReplicaEpoch::new(1),
        ConfigurationId::new("configuration-1").unwrap(),
        if changed {
            MysqlPeerAuthorityFacts::new(
                target,
                ProcessSessionId::new("changed-peer-process").unwrap(),
                harness.peer.endpoint().clone(),
                harness.peer.storage().clone(),
            )
        } else {
            peer_facts(target)
        },
    )
}

fn peer_facts(target: MysqlReplicaTarget) -> MysqlPeerAuthorityFacts {
    let process_session = target.process_session().clone();
    let endpoint = target.endpoint().clone();
    let storage = target.storage().clone();
    MysqlPeerAuthorityFacts::new(target, process_session, endpoint, storage)
}

fn operation(label: &str, generation: u64) -> MysqlOperationIdentity {
    MysqlOperationIdentity::new(
        AuthorityGeneration::new(label).unwrap(),
        nonzero(generation),
    )
}

fn nonzero(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}

fn replica_identity(label: &str) -> MysqlReplicaIdentity {
    MysqlReplicaIdentity::new(
        ReplicaId::new(format!("replica-{label}")).unwrap(),
        ReplicaIncarnation::new(format!("incarnation-{label}")).unwrap(),
    )
}

fn target_from_authority(
    identity: MysqlReplicaIdentity,
    authority: &MemberMutationAuthority,
) -> MysqlReplicaTarget {
    let binding = authority.local().binding();
    MysqlReplicaTarget::new(
        identity,
        MemberPeer::from_enrollment(authority.local()),
        binding.process_session().clone(),
        binding.endpoint().clone(),
        binding.storage().clone(),
    )
}

fn formation_configuration(
    label: &str,
    epoch: u64,
    members: Vec<MysqlReplicaTarget>,
) -> MysqlReplicaConfiguration {
    MysqlReplicaConfiguration::new(
        ConfigurationId::new(format!("{label}-configuration")).unwrap(),
        MysqlReplicaEpoch::new(epoch),
        members,
    )
}

fn replica_target(
    label: &str,
    attempt_label: &str,
    member: MysqlMemberIndex,
) -> MysqlReplicaTarget {
    let enrollment = enrollment(attempt_label, member);
    let binding = enrollment.binding();
    MysqlReplicaTarget::new(
        replica_identity(label),
        MemberPeer::from_enrollment(&enrollment),
        binding.process_session().clone(),
        binding.endpoint().clone(),
        binding.storage().clone(),
    )
}

fn exact(role: MysqlReplicaRole) -> ReadPlan {
    ReadPlan::Exact { role, executed: "" }
}

fn context(label: &str) -> AttemptContext {
    AttemptContext::new(
        AttemptId::new(label).unwrap(),
        Instant::now() + Duration::from_secs(60),
        Duration::from_secs(10),
    )
    .unwrap()
}

fn credential(attempt: &AttemptContext, role: ControlCredentialRole) -> ControlCredential {
    let (generation, username) = match role {
        ControlCredentialRole::Observer => ("observer-generation", "observer_local"),
        ControlCredentialRole::Recovery => ("recovery-generation", "recovery_local"),
    };
    ControlCredential::new(
        attempt.attempt().clone(),
        CredentialGeneration::new(generation).unwrap(),
        role,
        username,
        "secret-value",
    )
    .unwrap()
}

fn authority(label: &str, member: MysqlMemberIndex) -> MemberMutationAuthority {
    let attempt = context(label);
    authority_with_attempt(&attempt, member)
}

fn authority_with_attempt(
    attempt: &AttemptContext,
    member: MysqlMemberIndex,
) -> MemberMutationAuthority {
    MemberMutationAuthority::new(
        attempt.attempt().clone(),
        GroupName::new(GROUP_UUID).unwrap(),
        enrollment(attempt.attempt().as_str(), member),
    )
    .unwrap()
}

fn enrollment(label: &str, member: MysqlMemberIndex) -> NativeIdentityEnrollment {
    let attempt = AttemptId::new(label).unwrap();
    let binding = binding(label, member);
    let observer = ControlCredential::new(
        attempt.clone(),
        CredentialGeneration::new("observer-generation").unwrap(),
        ControlCredentialRole::Observer,
        "observer_local",
        "observer-secret",
    )
    .unwrap();
    let recovery = ControlCredential::new(
        attempt,
        CredentialGeneration::new("recovery-generation").unwrap(),
        ControlCredentialRole::Recovery,
        "recovery_local",
        "recovery-secret",
    )
    .unwrap();
    let accounts = AccountProvisioningEvidence::new(
        binding.clone(),
        &observer,
        &recovery,
        AccountProvisioningEvidence::required_steps().to_vec(),
    )
    .unwrap();
    NativeIdentityEnrollment::new(
        binding,
        ServerUuid::new(member_uuid(member)).unwrap(),
        &accounts,
        true,
        GtidSet::empty(),
        NativeIdentityEnrollment::required_steps().to_vec(),
    )
    .unwrap()
}

fn binding(label: &str, member: MysqlMemberIndex) -> MemberControlBinding {
    let number = member_number(member);
    MemberControlBinding::new(
        AttemptId::new(label).unwrap(),
        member,
        ProcessSessionId::new(format!("process-{label}-{number}")).unwrap(),
        EndpointBinding::new(format!("endpoint-{label}-{number}")).unwrap(),
        StorageBinding::new(format!("storage-{label}-{number}")).unwrap(),
        MemberAddress::new(format!("127.0.0.1:3306{number}")).unwrap(),
        CredentialGeneration::new("observer-generation").unwrap(),
        CredentialGeneration::new("recovery-generation").unwrap(),
    )
}

fn evidence(
    authority: MemberMutationAuthority,
    view_id: &str,
    role: MysqlReplicaRole,
    executed: &str,
) -> MemberNativeEvidence {
    let local = authority.local().binding().member();
    let peer = if local == MysqlMemberIndex::First {
        MysqlMemberIndex::Second
    } else {
        MysqlMemberIndex::First
    };
    let peer_role = match role {
        MysqlReplicaRole::Primary => MemberRole::Secondary,
        MysqlReplicaRole::Secondary => MemberRole::Primary,
    };
    evidence_from_members(
        authority,
        view_id,
        &[(local, role.native_for_test()), (peer, peer_role)],
        executed,
    )
}

fn single_member_evidence(
    authority: MemberMutationAuthority,
    view_id: &str,
) -> MemberNativeEvidence {
    let local = authority.local().binding().member();
    evidence_from_members(
        authority,
        view_id,
        &[(local, MemberRole::Primary)],
        &format!("{GROUP_UUID}:1"),
    )
}

fn joined_evidence(
    authority: &MemberMutationAuthority,
    predecessor: &MemberNativeEvidence,
) -> MemberNativeEvidence {
    let mut members = predecessor.view().members().to_vec();
    members.push(native_member(
        authority.local().binding().member(),
        MemberRole::Secondary,
    ));
    MemberNativeEvidence::new(
        authority.clone(),
        NativeView::new(
            authority.group_name().clone(),
            ViewId::new(next_view(predecessor.view().id().as_str())).unwrap(),
            members,
        )
        .unwrap(),
        predecessor.executed().clone(),
    )
    .unwrap()
}

fn transferred_evidence(
    authority: &MemberMutationAuthority,
    predecessor: &MemberNativeEvidence,
    target: &MemberPeer,
) -> MemberNativeEvidence {
    let members = predecessor
        .view()
        .members()
        .iter()
        .map(|member| {
            let role = if member.id() == target.member_id() {
                MemberRole::Primary
            } else if member.id() == authority.local().member_id() {
                MemberRole::Secondary
            } else {
                member.role()
            };
            NativeMember::new(
                member.id().clone(),
                member.address().clone(),
                role,
                MemberState::Online,
            )
        })
        .collect();
    MemberNativeEvidence::new(
        authority.clone(),
        NativeView::new(
            authority.group_name().clone(),
            predecessor.view().id().clone(),
            members,
        )
        .unwrap(),
        predecessor.executed().clone(),
    )
    .unwrap()
}

fn evidence_from_members(
    authority: MemberMutationAuthority,
    view_id: &str,
    members: &[(MysqlMemberIndex, MemberRole)],
    executed: &str,
) -> MemberNativeEvidence {
    MemberNativeEvidence::new(
        authority.clone(),
        NativeView::new(
            authority.group_name().clone(),
            ViewId::new(view_id).unwrap(),
            members
                .iter()
                .map(|(member, role)| native_member(*member, *role))
                .collect(),
        )
        .unwrap(),
        GtidSet::from_str(executed).unwrap(),
    )
    .unwrap()
}

fn native_member(member: MysqlMemberIndex, role: MemberRole) -> NativeMember {
    NativeMember::new(
        MemberId::new(member_uuid(member)).unwrap(),
        MemberAddress::new(format!("127.0.0.1:3306{}", member_number(member))).unwrap(),
        role,
        MemberState::Online,
    )
}

fn next_view(view: &str) -> String {
    let (fixed, sequence) = view.split_once(':').unwrap();
    format!("{fixed}:{}", sequence.parse::<u32>().unwrap() + 1)
}

fn member_uuid(member: MysqlMemberIndex) -> &'static str {
    match member {
        MysqlMemberIndex::First => LOCAL_UUID,
        MysqlMemberIndex::Second => PEER_UUID,
        MysqlMemberIndex::Third => THIRD_UUID,
    }
}

fn member_number(member: MysqlMemberIndex) -> usize {
    match member {
        MysqlMemberIndex::First => 1,
        MysqlMemberIndex::Second => 2,
        MysqlMemberIndex::Third => 3,
    }
}

trait RoleTestExt {
    fn native_for_test(self) -> MemberRole;
}

impl RoleTestExt for MysqlReplicaRole {
    fn native_for_test(self) -> MemberRole {
        match self {
            Self::Secondary => MemberRole::Secondary,
            Self::Primary => MemberRole::Primary,
        }
    }
}
