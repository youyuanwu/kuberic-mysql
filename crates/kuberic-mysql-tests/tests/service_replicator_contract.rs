#[path = "service_common/mod.rs"]
mod service_common;

use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::str::FromStr;
use std::time::{Duration, Instant};

use kuberic_mysql::core::{
    AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, EndpointBinding,
    GroupName, GtidSet, MemberAddress, MemberId, MemberRole, MemberState, NativeMember, NativeView,
    ProcessSessionId, ReplicaId, ReplicaIncarnation, ServerUuid, StorageBinding, ViewId,
};
use kuberic_mysql::service::{
    AccountProvisioningEvidence, AttemptCleanupState, AttemptContext, BackendConnectionId,
    BackendConnectionReceipt, ControlCredential, ControlCredentialRole, FreshBootstrapInput,
    FreshJoinInput, LifecycleOperation, MemberControlBinding, MemberEffectKind,
    MemberEffectReceipt, MemberMutationAuthority, MemberNativeEvidence, MemberPeer,
    MemberSourceBoundary, MemberTransitionError, MysqlAccessIntent, MysqlInstanceConfig,
    MysqlInstanceError, MysqlMemberEffectBoundary, MysqlMemberIndex, MysqlOpenReceipt,
    MysqlOperationIdentity, MysqlPeerAuthorityFacts, MysqlPeerOperationIntent,
    MysqlPeerOperationKind, MysqlPeerOperationOutcome, MysqlPeerUnsupportedReason,
    MysqlReplicaConfiguration, MysqlReplicaEpoch, MysqlReplicaIdentity, MysqlReplicaRole,
    MysqlReplicaTarget, MysqlReplicator, MysqlReplicatorError, MysqlTerminalState,
    NativeIdentityEnrollment, PrimaryTransferInput,
};
use service_common::{GROUP_UUID, TestRoot};

const LOCAL_UUID: &str = "11111111-1111-1111-1111-111111111111";
const PEER_UUID: &str = "22222222-2222-2222-2222-222222222222";
const THIRD_UUID: &str = "33333333-3333-3333-3333-333333333333";

#[derive(Clone)]
enum ReadPlan {
    Exact {
        role: MysqlReplicaRole,
        executed: &'static str,
    },
    WrongAuthority(Box<MemberMutationAuthority>),
    CancelThenExact {
        role: MysqlReplicaRole,
    },
    Error,
}

#[derive(Clone, Copy)]
enum CleanupPlan {
    Completed,
    NotRequired,
    Failed,
}

struct ScriptedEffects {
    config: MysqlInstanceConfig,
    reads: VecDeque<ReadPlan>,
    cleanup: CleanupPlan,
    containments: usize,
}

impl ScriptedEffects {
    fn new(config: MysqlInstanceConfig, reads: Vec<ReadPlan>, cleanup: CleanupPlan) -> Self {
        Self {
            config,
            reads: reads.into(),
            cleanup,
            containments: 0,
        }
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
        let authority = input.authority().clone();
        let successor = single_member_evidence(authority.clone(), "17:1");
        MemberEffectReceipt::new(
            MemberEffectKind::Bootstrap,
            authority.clone(),
            None,
            successor,
            authority.local().binding(),
            attempt,
            AttemptCleanupState::Completed,
        )
    }

    async fn join_member(
        &mut self,
        input: FreshJoinInput,
        _recovery: &ControlCredential,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError> {
        let authority = input.authority().clone();
        let predecessor = input.source().evidence().clone();
        let successor = joined_evidence(&authority, &predecessor);
        MemberEffectReceipt::new(
            MemberEffectKind::Join,
            authority.clone(),
            Some(predecessor),
            successor,
            authority.local().binding(),
            attempt,
            AttemptCleanupState::NotRequired,
        )
    }

    async fn read_member_view(
        &mut self,
        authority: &MemberMutationAuthority,
        attempt: &AttemptContext,
    ) -> Result<MemberNativeEvidence, MemberTransitionError> {
        match self.reads.pop_front().expect("scripted read") {
            ReadPlan::Exact { role, executed } => {
                Ok(evidence(authority.clone(), "23:1", role, executed))
            }
            ReadPlan::WrongAuthority(wrong) => {
                Ok(evidence(*wrong, "23:1", MysqlReplicaRole::Secondary, ""))
            }
            ReadPlan::CancelThenExact { role } => {
                attempt.cancel();
                Ok(evidence(authority.clone(), "23:1", role, ""))
            }
            ReadPlan::Error => Err(MemberTransitionError::Cancelled),
        }
    }

    async fn transfer_primary(
        &mut self,
        input: PrimaryTransferInput,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError> {
        let authority = input.authority().clone();
        let predecessor = input.source().evidence().clone();
        let successor = transferred_evidence(&authority, &predecessor, input.target());
        MemberEffectReceipt::new(
            MemberEffectKind::PrimaryTransfer {
                target: input.target().member_id().clone(),
            },
            authority.clone(),
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
        self.containments += 1;
        match self.cleanup {
            CleanupPlan::Completed => Ok(AttemptCleanupState::Completed),
            CleanupPlan::NotRequired => Ok(AttemptCleanupState::NotRequired),
            CleanupPlan::Failed => Err(MysqlInstanceError::Timeout(LifecycleOperation::Shutdown)),
        }
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
    let wrong = authority("wrong", MysqlMemberIndex::First);
    let mut mismatch = harness(vec![ReadPlan::WrongAuthority(Box::new(wrong))]);
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

    let self_intent = MysqlPeerOperationIntent::new(
        operation("self", 7),
        MysqlReplicaEpoch::new(1),
        ConfigurationId::new("configuration-1").unwrap(),
        peer_facts(primary.local.clone()),
    );
    assert!(matches!(
        primary.engine.build_replica(self_intent),
        Err(MysqlReplicatorError::SelfTarget)
    ));
    let stale_epoch = MysqlPeerOperationIntent::new(
        operation("stale-peer", 7),
        MysqlReplicaEpoch::new(0),
        ConfigurationId::new("configuration-1").unwrap(),
        peer_facts(primary.peer.clone()),
    );
    assert!(matches!(
        primary.engine.build_replica(stale_epoch),
        Err(MysqlReplicatorError::ConfigurationRequired)
    ));
    primary
        .engine
        .advance_epoch(operation("peer-next-epoch", 7), MysqlReplicaEpoch::new(2))
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

fn harness_with_cleanup(reads: Vec<ReadPlan>, cleanup: CleanupPlan) -> Harness {
    let root = TestRoot::new("replicat");
    let config = root.config_for_member(MysqlMemberIndex::First);
    let attempt = context("replicator");
    let authority = authority_with_attempt(&attempt, MysqlMemberIndex::First);
    let identity = replica_identity("local");
    let local = MysqlReplicaTarget::new(
        identity.clone(),
        MemberPeer::from_enrollment(authority.local()),
    );
    let peer_identity = replica_identity("peer");
    let peer = MysqlReplicaTarget::new(
        peer_identity,
        MemberPeer::from_enrollment(&enrollment("replicator", MysqlMemberIndex::Second)),
    );
    let engine = engine(config, identity.clone(), attempt.clone(), reads, cleanup);
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
    cleanup: CleanupPlan,
) -> MysqlReplicator<ScriptedEffects> {
    let observer = credential(&attempt, ControlCredentialRole::Observer);
    let recovery = credential(&attempt, ControlCredentialRole::Recovery);
    MysqlReplicator::new(
        ScriptedEffects::new(config, reads, cleanup),
        identity,
        kuberic_mysql::core::ConfiguredGtidLineage::new(GROUP_UUID).unwrap(),
        observer,
        recovery,
        attempt,
    )
    .unwrap()
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
                EndpointBinding::new("peer-endpoint").unwrap(),
                StorageBinding::new("peer-storage").unwrap(),
            )
        } else {
            peer_facts(target)
        },
    )
}

fn peer_facts(target: MysqlReplicaTarget) -> MysqlPeerAuthorityFacts {
    MysqlPeerAuthorityFacts::new(
        target,
        ProcessSessionId::new("peer-process").unwrap(),
        EndpointBinding::new("peer-endpoint").unwrap(),
        StorageBinding::new("peer-storage").unwrap(),
    )
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
    evidence_from_members(authority, view_id, &[(local, MemberRole::Primary)], "")
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
