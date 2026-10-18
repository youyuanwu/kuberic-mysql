#![allow(dead_code)]

#[path = "../service_live_support/mod.rs"]
mod live_support;

use std::collections::HashSet;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use kuberic_mysql::core::{
    AttemptId, AuthorityGeneration, ConfigurationId, ConfiguredGtidLineage, CredentialGeneration,
    ProgressProjection, ReplicaId, ReplicaIncarnation,
};
use kuberic_mysql::service::{
    ApplicationCredential, AttemptContext, CanonicalViewId, ControlCredential,
    ControlCredentialRole, MemberNativeEvidence, MysqlAccessIntent, MysqlFreshMemberCredentials,
    MysqlFreshMemberInput, MysqlHandoffBoundary, MysqlHandoffPublicationProof, MysqlInstanceConfig,
    MysqlMemberConfig, MysqlMemberIndex, MysqlOperationIdentity, MysqlOperationTimeouts,
    MysqlReplicaConfiguration, MysqlReplicaEpoch, MysqlReplicaIdentity, MysqlReplicaRole,
    MysqlReplicaTarget, MysqlReplicator, MysqlTerminalState, MysqlTopologyConfig,
};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder};
use tokio::time::sleep;

#[allow(unused_imports)]
pub use live_support::preflight_oracle_mysql_8_4_11;
use live_support::{
    APPARMOR_EXEC, FixtureRoot, MYSQLD, ReservedPorts, assert_no_application_store,
    assert_process_absent, assert_tcp_refuses,
};

pub const GROUP_UUID: &str = "49a138d2-ceee-49eb-993e-c6506c495481";
const MEMBERS: [MysqlMemberIndex; 3] = [
    MysqlMemberIndex::First,
    MysqlMemberIndex::Second,
    MysqlMemberIndex::Third,
];
const ATTEMPT_BUDGET: Duration = Duration::from_secs(420);
const CLEANUP_RESERVE: Duration = Duration::from_secs(60);

pub struct FormationEvidence {
    targets: [MysqlReplicaTarget; 3],
    first_view: String,
    second_view: String,
    final_view: String,
    progress: ProgressProjection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandoffStage {
    InitialPublicationHistoryAccepted,
    CancellationGenerationPublished,
    CancellationContained,
    SourceGenerationPublished,
    SourceAutocommitCompleted,
    SourceTransactionCompleted,
    SourcePingQuitCompleted,
    SourceRevokedEligible,
    BoundaryFrozen,
    EpochBarriersAccepted,
    SourceTransferCompleted,
    TargetPromotionCompleted,
    RemainingSameRoleCompleted,
    ConfigurationAccepted,
    SourceStoppedAndReaped,
    RemainingSuccessorAccepted,
    TargetSuccessorAccepted,
    PublicationProofAccepted,
    TargetPublished,
    TargetMutationCompleted,
    SourceUnavailableConfirmed,
    CleanupCompleted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandoffTraceEntry {
    pub stage: HandoffStage,
    pub elapsed: Duration,
    pub evidence: String,
}

pub struct ControlledHandoffEvidence {
    trace: Vec<HandoffTraceEntry>,
    boundary: MysqlHandoffBoundary,
    source_endpoint: SocketAddr,
    target_endpoint: SocketAddr,
}

impl ControlledHandoffEvidence {
    pub fn trace(&self) -> &[HandoffTraceEntry] {
        &self.trace
    }

    pub const fn boundary(&self) -> &MysqlHandoffBoundary {
        &self.boundary
    }

    pub const fn source_endpoint(&self) -> SocketAddr {
        self.source_endpoint
    }

    pub const fn target_endpoint(&self) -> SocketAddr {
        self.target_endpoint
    }
}

impl FormationEvidence {
    pub fn targets(&self) -> &[MysqlReplicaTarget; 3] {
        &self.targets
    }

    pub fn first_view(&self) -> &str {
        &self.first_view
    }

    pub fn second_view(&self) -> &str {
        &self.second_view
    }

    pub fn final_view(&self) -> &str {
        &self.final_view
    }

    pub const fn progress(&self) -> ProgressProjection {
        self.progress
    }
}

pub struct ReplicatorLiveFixture {
    root: FixtureRoot,
    root_path: PathBuf,
    attempt: AttemptContext,
    members: [MemberOwner; 3],
    workload: WorkloadCredentialOwner,
    sql_addresses: [SocketAddr; 3],
    group_addresses: [SocketAddr; 3],
    application_schema_created: bool,
    closed: bool,
}

impl ReplicatorLiveFixture {
    pub async fn prepare() -> Result<Self, String> {
        let root = FixtureRoot::new("replicator-live")?;
        let root_path = root.path().to_path_buf();
        let mut native_ports = ReservedPorts::allocate()?;
        let sql_addresses = native_ports.sql_addresses();
        let group_addresses = native_ports.group_replication_addresses();
        let mut proxy_ports = ProxyPortReservations::allocate(
            sql_addresses.into_iter().chain(group_addresses).collect(),
        )?;
        let proxy_addresses = proxy_ports.addresses;
        let all_addresses = sql_addresses
            .into_iter()
            .chain(group_addresses)
            .chain(proxy_addresses)
            .collect::<HashSet<_>>();
        if all_addresses.len() != 9 {
            return Err("formation ports were not distinct".to_owned());
        }

        let attempt = AttemptContext::new(
            AttemptId::new(format!("replicator-live-{}", std::process::id()))
                .map_err(|error| error.to_string())?,
            Instant::now() + ATTEMPT_BUDGET,
            CLEANUP_RESERVE,
        )
        .map_err(|error| format!("{error:?}"))?;
        let topology = topology(sql_addresses, group_addresses)?;
        let timeouts = MysqlOperationTimeouts::new(
            Duration::from_secs(180),
            Duration::from_secs(60),
            Duration::from_secs(30),
            Duration::from_secs(10),
        )
        .map_err(|error| format!("{error:?}"))?;
        let configs =
            MEMBERS.map(|member| instance_config(&root_path, topology.clone(), member, timeouts));
        let identities = [
            replica_identity("one"),
            replica_identity("two"),
            replica_identity("three"),
        ];

        let mut recovery_secret =
            SecretProducer::new(format!("ReplicatorRecovery{}B9", std::process::id()));
        let mut recovery_credentials = MEMBERS.map(|_| {
            Some(
                ControlCredential::new(
                    attempt.attempt().clone(),
                    CredentialGeneration::new("replicator-recovery-generation").unwrap(),
                    ControlCredentialRole::Recovery,
                    format!("kmr_rec_{}", std::process::id()),
                    recovery_secret.copy(),
                )
                .unwrap(),
            )
        });
        recovery_secret.clear();
        if !recovery_secret.is_cleared() {
            return Err("shared recovery producer was not cleared".to_owned());
        }

        let mut application_secret =
            SecretProducer::new(format!("ReplicatorApplication{}C11", std::process::id()));
        let application_username = format!("kmr_app_{}", std::process::id());
        let mut application_credentials = MEMBERS.map(|_| {
            Some(
                ApplicationCredential::new(
                    attempt.attempt().clone(),
                    CredentialGeneration::new("replicator-application-generation").unwrap(),
                    application_username.clone(),
                    application_secret.copy(),
                )
                .unwrap(),
            )
        });
        let workload = WorkloadCredentialOwner::new(
            application_username,
            application_secret.copy().into_bytes(),
        );
        application_secret.clear();
        if !application_secret.is_cleared() {
            return Err("application credential producer was not cleared".to_owned());
        }

        native_ports.release();
        proxy_ports.release();
        let mut prepared = Vec::with_capacity(3);
        for index in 0..3 {
            let mut observer_secret = SecretProducer::new(format!(
                "ReplicatorObserver{}{}A7",
                std::process::id(),
                index + 1
            ));
            let observer = ControlCredential::new(
                attempt.attempt().clone(),
                CredentialGeneration::new(format!("replicator-observer-generation-{}", index + 1))
                    .unwrap(),
                ControlCredentialRole::Observer,
                format!("kmr_obs_{}_{}", std::process::id(), index + 1),
                observer_secret.copy(),
            )
            .unwrap();
            observer_secret.clear();
            if !observer_secret.is_cleared() {
                return Err(format!(
                    "member {} observer producer was not cleared",
                    index + 1
                ));
            }
            let data_root = configs[index].data_root().to_owned();
            let scratch_root = configs[index].runtime().scratch_root().to_owned();
            let socket = configs[index].runtime().socket().to_owned();
            let pid_file = configs[index].runtime().pid().to_owned();
            let credentials = MysqlFreshMemberCredentials::new(
                observer,
                recovery_credentials[index]
                    .take()
                    .expect("member recovery credential transfers once"),
                application_credentials[index]
                    .take()
                    .expect("member application credential transfers once"),
            );
            let input = MysqlFreshMemberInput::new(
                configs[index].clone(),
                identities[index].clone(),
                ConfiguredGtidLineage::new(GROUP_UUID).map_err(|error| error.to_string())?,
                credentials,
                attempt.clone(),
                NonZeroU64::new(1).unwrap(),
            );
            let (mut engine, receipt) = MysqlReplicator::prepare_fresh_member(input)
                .await
                .map_err(|error| format!("{error:?}"))?;
            engine
                .open(operation(&format!("open-member-{}", index + 1), 1), receipt)
                .map_err(|error| format!("{error:?}"))?;
            let pid = fs::read_to_string(&pid_file)
                .map_err(|error| format!("read {}: {error}", pid_file.display()))?
                .trim()
                .parse::<u32>()
                .map_err(|error| format!("parse {}: {error}", pid_file.display()))?;
            prepared.push(MemberOwner {
                engine: Some(engine),
                data_root,
                scratch_root,
                socket,
                pid_file,
                pid,
                proxy_address: proxy_addresses[index],
                next_generation: 2,
                armed: true,
            });
        }
        let members: [MemberOwner; 3] = prepared
            .try_into()
            .map_err(|_| "expected three prepared member owners".to_owned())?;
        Ok(Self {
            root,
            root_path,
            attempt,
            members,
            workload,
            sql_addresses,
            group_addresses,
            application_schema_created: false,
            closed: false,
        })
    }

    pub async fn form(&mut self) -> Result<FormationEvidence, String> {
        let targets = std::array::from_fn(|index| {
            self.members[index]
                .engine()
                .local_target()
                .expect("prepared engine is open")
        });
        if targets
            .iter()
            .map(|target| target.member().member_id())
            .collect::<HashSet<_>>()
            .len()
            != 3
        {
            return Err("native member identities were not distinct".to_owned());
        }
        for index in 0..3 {
            let role = if index == 0 {
                MysqlReplicaRole::Primary
            } else {
                MysqlReplicaRole::Secondary
            };
            let epoch_operation = self.members[index].operation("epoch");
            self.members[index]
                .engine_mut()
                .advance_epoch(epoch_operation, MysqlReplicaEpoch::new(1))
                .map_err(|error| format!("{error:?}"))?;
            let role_operation = self.members[index].operation("formation-role");
            self.members[index]
                .engine_mut()
                .intend_formation_role(role_operation, MysqlReplicaEpoch::new(1), role)
                .map_err(|error| format!("{error:?}"))?;
        }

        let first_configuration = configuration("formation-one", vec![targets[0].clone()]);
        let bootstrap_operation = self.members[0].operation("bootstrap");
        self.members[0]
            .engine_mut()
            .bootstrap_configuration(bootstrap_operation, first_configuration)
            .await
            .map_err(|error| format!("{error:?}"))?;
        let first_evidence = self.members[0]
            .engine()
            .native_evidence()
            .cloned()
            .ok_or_else(|| "bootstrap returned no exact native evidence".to_owned())?;

        let second_configuration = configuration(
            "formation-two",
            vec![targets[0].clone(), targets[1].clone()],
        );
        let second_source = self.members[0]
            .engine()
            .source_boundary()
            .map_err(|error| format!("{error:?}"))?;
        let second_operation = self.members[1].operation("join-second");
        self.members[1]
            .engine_mut()
            .join_configuration(
                second_operation,
                second_configuration.clone(),
                second_source,
                vec![targets[0].member().clone()],
            )
            .await
            .map_err(|error| error.to_string())?;
        let observe_second_operation = self.members[0].operation("observe-second");
        self.members[0]
            .engine_mut()
            .observe_configuration(observe_second_operation, second_configuration)
            .await
            .map_err(|error| error.to_string())?;
        let second_evidence = self.members[1]
            .engine()
            .native_evidence()
            .cloned()
            .ok_or_else(|| "second join returned no exact native evidence".to_owned())?;

        let final_configuration = configuration("formation-three", targets.to_vec());
        let third_source = self.members[1]
            .engine()
            .source_boundary()
            .map_err(|error| error.to_string())?;
        let third_operation = self.members[2].operation("join-third");
        self.members[2]
            .engine_mut()
            .join_configuration(
                third_operation,
                final_configuration.clone(),
                third_source,
                vec![targets[0].member().clone(), targets[1].member().clone()],
            )
            .await
            .map_err(|error| error.to_string())?;
        for index in 0..2 {
            let observe_operation = self.members[index].operation("observe-final");
            self.members[index]
                .engine_mut()
                .observe_configuration(observe_operation, final_configuration.clone())
                .await
                .map_err(|error| error.to_string())?;
        }

        let final_evidence = self.members[2]
            .engine()
            .native_evidence()
            .cloned()
            .ok_or_else(|| "third join returned no exact native evidence".to_owned())?;
        CanonicalViewId::parse(first_evidence.view().id())
            .and_then(|first| {
                CanonicalViewId::parse(second_evidence.view().id())
                    .and_then(|second| first.require_successor(second))
            })
            .map_err(|error| format!("{error:?}"))?;
        CanonicalViewId::parse(second_evidence.view().id())
            .and_then(|second| {
                CanonicalViewId::parse(final_evidence.view().id())
                    .and_then(|final_view| second.require_successor(final_view))
            })
            .map_err(|error| format!("{error:?}"))?;

        let mut common_progress = None;
        for index in 0..3 {
            let progress_operation = self.members[index].operation("progress");
            let progress = self.members[index]
                .engine_mut()
                .read_progress(progress_operation)
                .await
                .map_err(|error| error.to_string())?
                .progress();
            if let Some(expected) = common_progress {
                if progress != expected {
                    return Err("formation members did not report common progress".to_owned());
                }
            } else {
                common_progress = Some(progress);
            }
            let engine = self.members[index].engine();
            if engine.configuration() != Some(&final_configuration)
                || engine.epoch() != Some(MysqlReplicaEpoch::new(1))
                || engine.role()
                    != Some(if index == 0 {
                        MysqlReplicaRole::Primary
                    } else {
                        MysqlReplicaRole::Secondary
                    })
                || engine.proxy_snapshot().is_some()
                || !engine.credentials_cleared()
                || !engine.application_credential_staged()
            {
                return Err(format!(
                    "member {} formation state was incomplete",
                    index + 1
                ));
            }
            let evidence = engine
                .native_evidence()
                .ok_or_else(|| format!("member {} final evidence missing", index + 1))?;
            if evidence.view() != final_evidence.view()
                || evidence.executed() != final_evidence.executed()
                || evidence
                    .executed()
                    .entries()
                    .iter()
                    .any(|entry| entry.source().server_uuid().as_str() != GROUP_UUID)
            {
                return Err(format!("member {} final native state differed", index + 1));
            }
            assert_tcp_refuses(self.members[index].proxy_address)?;
            assert_tcp_refuses(self.sql_addresses[index])?;
        }
        Ok(FormationEvidence {
            targets,
            first_view: first_evidence.view().id().as_str().to_owned(),
            second_view: second_evidence.view().id().as_str().to_owned(),
            final_view: final_evidence.view().id().as_str().to_owned(),
            progress: common_progress.expect("three members produced progress"),
        })
    }

    pub async fn form_and_finalize(mut self) -> Result<FormationEvidence, String> {
        let formation = self.form().await;
        let cleanup_failures = self.cleanup_failures().await;
        finalize_attempt(formation, cleanup_failures)
    }

    pub async fn controlled_handoff_and_finalize(
        mut self,
    ) -> Result<ControlledHandoffEvidence, String> {
        let started = Instant::now();
        let primary = async {
            let formation = self.form().await?;
            self.controlled_handoff(formation, started).await
        }
        .await;
        let cleanup_failures = self.cleanup_failures().await;
        match finalize_attempt(primary, cleanup_failures) {
            Ok(mut evidence) => {
                evidence.trace.push(HandoffTraceEntry {
                    stage: HandoffStage::CleanupCompleted,
                    elapsed: started.elapsed(),
                    evidence: "all member owners, accounts, listeners, tasks, ports, and roots were cleaned"
                        .to_owned(),
                });
                Ok(evidence)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn inject_proxy_failure_and_finalize(mut self, publish: bool) -> Result<(), String> {
        self.form().await?;
        let address = self.members[0].proxy_address;
        self.members[0]
            .engine_mut()
            .prepare_staged_client_proxy(address)
            .await
            .map_err(|error| error.to_string())?;
        if publish {
            let operation = self.members[0].operation("injected-publication");
            self.members[0]
                .engine_mut()
                .publish_access(operation, MysqlAccessIntent::new(nonzero(2), true, true))
                .await
                .map_err(|error| error.to_string())?;
        }
        let marker = if publish {
            "injected failure after publication"
        } else {
            "injected failure after preparation"
        };
        let failures = self.cleanup_failures().await;
        match finalize_attempt::<()>(Err(marker.to_owned()), failures) {
            Err(error) if error == format!("formation failed: {marker}") => Ok(()),
            Err(error) => Err(error),
            Ok(()) => Err("injected fixture failure unexpectedly succeeded".to_owned()),
        }
    }

    async fn controlled_handoff(
        &mut self,
        formation: FormationEvidence,
        started: Instant,
    ) -> Result<ControlledHandoffEvidence, String> {
        let mut trace = Vec::new();
        create_qualification_schema(&self.members[0].socket).await?;
        self.application_schema_created = true;
        let publication_deadline =
            (Instant::now() + Duration::from_secs(10)).min(self.attempt.work_deadline());
        let mut previous = Vec::new();
        loop {
            let mut observations = Vec::new();
            for index in 0..3 {
                let operation = self.members[index].operation("initial-publication-history");
                let progress = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(publication_deadline),
                    self.members[index].engine_mut().read_progress(operation),
                )
                .await
                .map_err(|_| "schema publication observation deadline expired".to_owned())?
                .map_err(|error| error.to_string())?;
                if !matches!(progress.progress(), ProgressProjection::Projectable(_)) {
                    return Err("schema publication history was non-projectable".to_owned());
                }
                let evidence = self.members[index]
                    .engine()
                    .native_evidence()
                    .cloned()
                    .ok_or_else(|| "initial publication evidence was missing".to_owned())?;
                observations.push(evidence);
            }
            if Instant::now() >= publication_deadline {
                return Err("schema publication observation deadline expired".to_owned());
            }
            if observations
                .iter()
                .any(|e| e.view().id().as_str() != formation.final_view())
                || observations
                    .iter()
                    .any(|e| e.view() != observations[0].view())
            {
                return Err("schema publication left the exact accepted formation view".to_owned());
            }
            if previous.iter().zip(&observations).any(
                |(prior, next): (&MemberNativeEvidence, _)| {
                    prior.authority() != next.authority()
                        || !prior.executed().is_subset_of(next.executed())
                },
            ) {
                return Err("schema publication history regressed or owner changed".to_owned());
            }
            if observations
                .iter()
                .all(|e| e.executed() == observations[0].executed())
            {
                break;
            }
            if observations.iter().any(|e| {
                !e.executed().is_subset_of(observations[0].executed())
                    && !observations[0].executed().is_subset_of(e.executed())
            }) {
                return Err("schema publication histories diverged".to_owned());
            }
            if Instant::now() >= publication_deadline {
                return Err("schema DDL did not converge to one exact common publication history before deadline".to_owned());
            }
            previous = observations;
            sleep(Duration::from_millis(20)).await;
        }
        push_trace(
            &mut trace,
            started,
            HandoffStage::InitialPublicationHistoryAccepted,
            "all three members accepted one exact structured history after schema DDL".to_owned(),
        );

        let cancellation_proxy_address = self.members[0].proxy_address;
        let cancellation_address = self.members[0]
            .engine_mut()
            .prepare_staged_client_proxy(cancellation_proxy_address)
            .await
            .map_err(|error| error.to_string())?;
        let publish_cancellation = self.members[0].operation("publish-cancellation");
        self.members[0]
            .engine_mut()
            .publish_access(
                publish_cancellation,
                MysqlAccessIntent::new(nonzero(2), true, true),
            )
            .await
            .map_err(|error| error.to_string())?;
        push_trace(
            &mut trace,
            started,
            HandoffStage::CancellationGenerationPublished,
            format!("source cancellation generation 2 published at {cancellation_address}"),
        );
        let cancellation_options = tcp_options(
            cancellation_address,
            &self.workload.username,
            self.workload.password()?,
        );
        let mut blocking = Conn::new(cancellation_options)
            .await
            .map_err(|error| error.to_string())?;
        let blocking_work = tokio::spawn(async move {
            blocking
                .query_drop("SELECT SLEEP(60)")
                .await
                .map_err(|error| error.to_string())
        });
        await_active(self.members[0].engine()).await?;
        let revoke_cancellation = self.members[0].operation("revoke-cancellation");
        let cancelled = self.members[0]
            .engine_mut()
            .revoke_access(
                revoke_cancellation,
                MysqlAccessIntent::new(nonzero(3), false, false),
            )
            .await
            .map_err(|error| error.to_string())?;
        let cancellation_receipt = cancelled
            .containment()
            .ok_or_else(|| "cancellation generation returned no containment receipt".to_owned())?;
        if !cancellation_receipt.sessions_zero()
            || !cancellation_receipt.commands_zero()
            || !cancellation_receipt.native_absence()
            || !cancellation_receipt.cleanup_complete()
            || !cancellation_receipt.cancelled_commands()
            || !cancellation_receipt.ambiguous_effect()
            || cancellation_receipt.handoff_eligible()
        {
            return Err("blocking cancellation did not remain contained and ineligible".to_owned());
        }
        if blocking_work
            .await
            .map_err(|error| error.to_string())?
            .is_ok()
        {
            return Err("blocking source query unexpectedly completed normally".to_owned());
        }
        assert_tcp_refuses(cancellation_address)?;
        push_trace(
            &mut trace,
            started,
            HandoffStage::CancellationContained,
            "generation 2 reached proxy-zero and native absence with cancellation+ambiguity"
                .to_owned(),
        );
        let clean_address = self.members[0]
            .engine_mut()
            .rearm_contained_client_proxy(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .map_err(|error| error.to_string())?;
        let publish_source = self.members[0].operation("publish-source");
        self.members[0]
            .engine_mut()
            .publish_access(
                publish_source,
                MysqlAccessIntent::new(nonzero(4), true, true),
            )
            .await
            .map_err(|error| error.to_string())?;
        push_trace(
            &mut trace,
            started,
            HandoffStage::SourceGenerationPublished,
            format!("clean source generation 4 published at {clean_address}"),
        );

        let clean_options = tcp_options(
            clean_address,
            &self.workload.username,
            self.workload.password()?,
        );
        let mut source = Conn::new(clean_options)
            .await
            .map_err(|error| error.to_string())?;
        source
            .query_drop(
                "INSERT INTO kuberic_proxy_qualification.tokens(token_name, token_value) \
                 VALUES ('handoff', 1)",
            )
            .await
            .map_err(|error| error.to_string())?;
        push_trace(
            &mut trace,
            started,
            HandoffStage::SourceAutocommitCompleted,
            "autocommit mutation returned a normal OK result".to_owned(),
        );
        source
            .query_drop("START TRANSACTION")
            .await
            .map_err(|error| error.to_string())?;
        source
            .query_drop(
                "UPDATE kuberic_proxy_qualification.tokens SET token_value = 2 \
                 WHERE token_name = 'handoff'",
            )
            .await
            .map_err(|error| error.to_string())?;
        source
            .query_drop("COMMIT")
            .await
            .map_err(|error| error.to_string())?;
        push_trace(
            &mut trace,
            started,
            HandoffStage::SourceTransactionCompleted,
            "explicit transaction committed with a normal OK result".to_owned(),
        );
        source.ping().await.map_err(|error| error.to_string())?;
        source
            .disconnect()
            .await
            .map_err(|error| error.to_string())?;
        await_sessions(self.members[0].engine(), 0).await?;
        push_trace(
            &mut trace,
            started,
            HandoffStage::SourcePingQuitCompleted,
            "ping completed normally and quit removed the exact backend session".to_owned(),
        );

        let revoke_source = self.members[0].operation("revoke-source");
        let source_closed = self.members[0]
            .engine_mut()
            .revoke_access(
                revoke_source,
                MysqlAccessIntent::new(nonzero(5), false, false),
            )
            .await
            .map_err(|error| error.to_string())?;
        let closure = source_closed
            .containment()
            .ok_or_else(|| "source revocation returned no containment receipt".to_owned())?;
        if !closure.handoff_eligible() || closure.cancelled_commands() || closure.ambiguous_effect()
        {
            return Err("clean source generation was not handoff eligible".to_owned());
        }
        push_trace(
            &mut trace,
            started,
            HandoffStage::SourceRevokedEligible,
            "source listener/dispatch/session/task/native-session counts reached zero with only normal commands"
                .to_owned(),
        );

        let source_progress_operation = self.members[0].operation("freeze-progress");
        let source_progress = self.members[0]
            .engine_mut()
            .read_progress(source_progress_operation)
            .await
            .map_err(|error| error.to_string())?
            .progress();
        for index in 1..3 {
            let operation = self.members[index].operation("prove-frozen-progress");
            let progress = self.members[index]
                .engine_mut()
                .read_progress(operation)
                .await
                .map_err(|error| error.to_string())?
                .progress();
            if progress != source_progress
                || self.members[index]
                    .engine()
                    .native_evidence()
                    .is_none_or(|evidence| {
                        self.members[0]
                            .engine()
                            .native_evidence()
                            .is_none_or(|source| evidence.executed() != source.executed())
                    })
            {
                return Err("survivor did not contain the exact frozen source boundary".to_owned());
            }
        }
        let transfer_source = self.members[0]
            .engine()
            .source_boundary()
            .map_err(|error| error.to_string())?;
        let freeze_operation = self.members[0].operation("freeze-handoff");
        let boundary = self.members[0]
            .engine_mut()
            .freeze_handoff_boundary(
                freeze_operation,
                formation.targets[1].clone(),
                formation.targets[2].clone(),
            )
            .await
            .map_err(|error| format!("freeze handoff boundary: {error}"))?;
        push_trace(
            &mut trace,
            started,
            HandoffStage::BoundaryFrozen,
            format!(
                "structured GTID boundary frozen with scalar {}",
                boundary.progress()
            ),
        );
        self.members[0]
            .engine_mut()
            .retire_contained_client_proxy()
            .await
            .map_err(|error| format!("retire frozen source proxy/account: {error}"))?;

        for index in 0..3 {
            let operation = self.members[index].operation("handoff-epoch");
            self.members[index]
                .engine_mut()
                .advance_epoch(operation, MysqlReplicaEpoch::new(2))
                .map_err(|error| error.to_string())?;
        }
        push_trace(
            &mut trace,
            started,
            HandoffStage::EpochBarriersAccepted,
            "newer epoch 2 accepted by source, target, and same-role remaining secondary"
                .to_owned(),
        );

        let source_transfer = self.members[0].operation("source-transfer");
        self.members[0]
            .engine_mut()
            .transfer_role(
                source_transfer,
                MysqlReplicaEpoch::new(2),
                MysqlReplicaRole::Secondary,
                transfer_source,
                formation.targets[1].member().clone(),
            )
            .await
            .map_err(|error| error.to_string())?;
        push_trace(
            &mut trace,
            started,
            HandoffStage::SourceTransferCompleted,
            "source demotion completed only from the exact target transfer receipt".to_owned(),
        );
        let target_promotion = self.members[1].operation("target-promotion");
        self.members[1]
            .engine_mut()
            .observe_role(
                target_promotion,
                MysqlReplicaEpoch::new(2),
                MysqlReplicaRole::Primary,
            )
            .await
            .map_err(|error| error.to_string())?;
        push_trace(
            &mut trace,
            started,
            HandoffStage::TargetPromotionCompleted,
            "target promotion completed from exact target-primary native evidence".to_owned(),
        );
        let remaining_role = self.members[2].operation("remaining-same-role");
        self.members[2]
            .engine_mut()
            .observe_role(
                remaining_role,
                MysqlReplicaEpoch::new(2),
                MysqlReplicaRole::Secondary,
            )
            .await
            .map_err(|error| error.to_string())?;
        push_trace(
            &mut trace,
            started,
            HandoffStage::RemainingSameRoleCompleted,
            "remaining secondary completed the newer same-role transition from fresh evidence"
                .to_owned(),
        );

        let handoff_configuration = configuration_at(
            "handoff-three",
            MysqlReplicaEpoch::new(2),
            formation.targets.to_vec(),
        );
        for index in 0..3 {
            let operation = self.members[index].operation("handoff-configuration");
            self.members[index]
                .engine_mut()
                .observe_configuration(operation, handoff_configuration.clone())
                .await
                .map_err(|error| error.to_string())?;
        }
        push_trace(
            &mut trace,
            started,
            HandoffStage::ConfigurationAccepted,
            "all members accepted the unchanged exact three-member configuration for epoch 2"
                .to_owned(),
        );

        let source_close = self.members[0].operation("source-close");
        let terminal = self.members[0]
            .engine_mut()
            .close(source_close)
            .map_err(|error| format!("close frozen source lifecycle: {error}"))?;
        if terminal.state() != MysqlTerminalState::Closed {
            return Err("source did not cross the closed lifecycle boundary".to_owned());
        }
        let source_terminal = terminal.receipt().unwrap().clone();
        assert_process_absent(self.members[0].pid)?;
        if self.members[0].socket.exists() || self.members[0].pid_file.exists() {
            return Err("source socket or PID file survived exact stop/reap".to_owned());
        }
        assert_tcp_refuses(clean_address)?;
        push_trace(
            &mut trace,
            started,
            HandoffStage::SourceStoppedAndReaped,
            format!(
                "source PID {} reaped and private socket disappeared",
                self.members[0].pid
            ),
        );

        let remaining_successor = self.members[2].operation("remaining-successor");
        let remaining_outcome = self.members[2]
            .engine_mut()
            .accept_handoff_successor(remaining_successor, boundary.clone())
            .await
            .map_err(|error| format!("accept remaining successor: {error}"))?;
        push_trace(
            &mut trace,
            started,
            HandoffStage::RemainingSuccessorAccepted,
            "remaining secondary accepted the exact canonical two-survivor successor".to_owned(),
        );
        let target_successor = self.members[1].operation("target-successor");
        let target_outcome = self.members[1]
            .engine_mut()
            .accept_handoff_successor(target_successor, boundary.clone())
            .await
            .map_err(|error| format!("accept target successor: {error}"))?;
        push_trace(
            &mut trace,
            started,
            HandoffStage::TargetSuccessorAccepted,
            "target revalidated exact identity, authority, PRIMARY/ONLINE state, quorum, and frozen history"
                .to_owned(),
        );
        let publication_proof = MysqlHandoffPublicationProof::new(
            boundary.clone(),
            Some(target_outcome),
            Some(remaining_outcome),
            Some(source_terminal),
        );
        let publication_operation = self.members[1].operation("target-publication-proof");
        self.members[1]
            .engine_mut()
            .accept_handoff_publication_proof(publication_operation, publication_proof)
            .map_err(|error| format!("accept combined publication proof: {error}"))?;
        push_trace(
            &mut trace,
            started,
            HandoffStage::PublicationProofAccepted,
            "target consumed exact target+remaining successor receipts and source terminal stop/reap receipt"
                .to_owned(),
        );

        let target_proxy_address = self.members[1].proxy_address;
        let target_address = self.members[1]
            .engine_mut()
            .prepare_staged_client_proxy(target_proxy_address)
            .await
            .map_err(|error| error.to_string())?;
        let target_publish = self.members[1].operation("target-publish");
        self.members[1]
            .engine_mut()
            .publish_access(
                target_publish,
                MysqlAccessIntent::new(nonzero(2), true, true),
            )
            .await
            .map_err(|error| format!("publish target after successor: {error}"))?;
        push_trace(
            &mut trace,
            started,
            HandoffStage::TargetPublished,
            format!(
                "target generation 2 published only after both successor acceptances at {target_address}"
            ),
        );
        let mut target = Conn::new(tcp_options(
            target_address,
            &self.workload.username,
            self.workload.password()?,
        ))
        .await
        .map_err(|error| error.to_string())?;
        target
            .query_drop(
                "UPDATE kuberic_proxy_qualification.tokens SET token_value = 3 \
                 WHERE token_name = 'handoff'",
            )
            .await
            .map_err(|error| error.to_string())?;
        let target_value: u64 = target
            .query_first(
                "SELECT token_value FROM kuberic_proxy_qualification.tokens \
                 WHERE token_name = 'handoff'",
            )
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "target mutation row was absent".to_owned())?;
        if target_value != 3 {
            return Err(format!("target mutation returned value {target_value}"));
        }
        target
            .disconnect()
            .await
            .map_err(|error| error.to_string())?;
        await_sessions(self.members[1].engine(), 0).await?;
        push_trace(
            &mut trace,
            started,
            HandoffStage::TargetMutationCompleted,
            "target gateway mutation completed normally with value 3".to_owned(),
        );
        assert_tcp_refuses(clean_address)?;
        assert_tcp_refuses(cancellation_address)?;
        assert_tcp_refuses(self.sql_addresses[0])?;
        push_trace(
            &mut trace,
            started,
            HandoffStage::SourceUnavailableConfirmed,
            "both source generations and the native SQL endpoint remained unavailable".to_owned(),
        );

        let target_revoke = self.members[1].operation("target-cleanup-revoke");
        self.members[1]
            .engine_mut()
            .revoke_access(
                target_revoke,
                MysqlAccessIntent::new(nonzero(3), false, false),
            )
            .await
            .map_err(|error| error.to_string())?;
        let target_close = self.members[1].operation("target-cleanup-close");
        self.members[1]
            .engine_mut()
            .close_with_proxy(target_close)
            .await
            .map_err(|error| error.to_string())?;
        let remaining_close = self.members[2].operation("remaining-cleanup-close");
        self.members[2]
            .engine_mut()
            .close(remaining_close)
            .map_err(|error| error.to_string())?;

        Ok(ControlledHandoffEvidence {
            trace,
            boundary,
            source_endpoint: clean_address,
            target_endpoint: target_address,
        })
    }

    async fn cleanup_failures(&mut self) -> Vec<String> {
        if self.closed {
            return Vec::new();
        }
        self.attempt.cancel();
        let mut failures = Vec::new();
        for (index, owner) in self.members.iter_mut().enumerate() {
            if let Err(error) = owner.cleanup().await {
                failures.push(format!("member {} cleanup: {error}", index + 1));
            }
        }
        self.workload.clear();
        for owner in &self.members {
            if let Err(error) = assert_process_absent(owner.pid) {
                failures.push(error);
            }
            if owner.socket.exists() {
                failures.push(format!("member socket survived {}", owner.socket.display()));
            }
            if owner.pid_file.exists() {
                failures.push(format!(
                    "member PID file survived {}",
                    owner.pid_file.display()
                ));
            }
            if let Err(error) = assert_tcp_refuses(owner.proxy_address) {
                failures.push(error);
            }
            if !self.application_schema_created
                && let Err(error) = assert_no_application_store(&owner.data_root)
            {
                failures.push(error);
            }
            if owner.scratch_root.exists() {
                failures.push(format!(
                    "member scratch root survived {}",
                    owner.scratch_root.display()
                ));
            }
        }
        for address in self.sql_addresses.into_iter().chain(self.group_addresses) {
            if let Err(error) = assert_tcp_refuses(address) {
                failures.push(error);
            }
        }
        if failures.is_empty() {
            if let Err(error) = self.root.remove() {
                failures.push(error);
            }
            if self.root_path.exists() {
                failures.push(format!(
                    "fixture root survived {}",
                    self.root_path.display()
                ));
            }
        } else {
            self.root.preserve();
        }
        self.closed = failures.is_empty();
        failures
    }
}

impl Drop for ReplicatorLiveFixture {
    fn drop(&mut self) {
        if !self.closed {
            self.attempt.cancel();
            for owner in &mut self.members {
                owner.emergency_drop_engine();
            }
            if self.members.iter().any(|owner| {
                assert_process_absent(owner.pid).is_err()
                    || owner.socket.exists()
                    || owner.pid_file.exists()
            }) {
                self.root.preserve();
                eprintln!(
                    "emergency replicator fixture cleanup preserved {} after incomplete containment",
                    self.root_path.display()
                );
            }
        }
    }
}

pub(crate) fn finalize_attempt<T>(
    primary: Result<T, String>,
    cleanup_failures: Vec<String>,
) -> Result<T, String> {
    match (primary, cleanup_failures.is_empty()) {
        (Ok(value), true) => Ok(value),
        (Err(primary), true) => Err(format!("formation failed: {primary}")),
        (Ok(_), false) => Err(format!(
            "formation cleanup failed: {}",
            cleanup_failures.join("; ")
        )),
        (Err(primary), false) => Err(format!(
            "formation failed: {primary}; cleanup failures: {}",
            cleanup_failures.join("; ")
        )),
    }
}

struct MemberOwner {
    engine: Option<MysqlReplicator>,
    data_root: PathBuf,
    scratch_root: PathBuf,
    socket: PathBuf,
    pid_file: PathBuf,
    pid: u32,
    proxy_address: SocketAddr,
    next_generation: u64,
    armed: bool,
}

impl MemberOwner {
    fn engine(&self) -> &MysqlReplicator {
        self.engine.as_ref().expect("armed member retains engine")
    }

    fn engine_mut(&mut self) -> &mut MysqlReplicator {
        self.engine.as_mut().expect("armed member retains engine")
    }

    fn operation(&mut self, label: &str) -> MysqlOperationIdentity {
        let generation = self.next_generation;
        self.next_generation += 1;
        operation(
            &format!("{label}-{}", self.engine().identity().replica_id().as_str()),
            generation,
        )
    }

    async fn cleanup(&mut self) -> Result<(), String> {
        if !self.armed {
            return Ok(());
        }
        let operation = operation(
            &format!("cleanup-{}", self.engine().identity().replica_id().as_str()),
            u64::MAX,
        );
        let result = if self.engine().terminal_outcome().is_some() {
            Ok(())
        } else {
            self.engine_mut()
                .abort_with_proxy(operation)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        };
        if result.is_ok() {
            self.armed = false;
        }
        result
    }

    fn emergency_drop_engine(&mut self) {
        if self.armed {
            self.engine.take();
            self.armed = false;
        }
    }
}

impl Drop for MemberOwner {
    fn drop(&mut self) {
        self.emergency_drop_engine();
    }
}

struct WorkloadCredentialOwner {
    username: String,
    password: Box<[u8]>,
}

impl WorkloadCredentialOwner {
    fn new(username: String, password: Vec<u8>) -> Self {
        Self {
            username,
            password: password.into_boxed_slice(),
        }
    }

    fn clear(&mut self) {
        self.password.fill(0);
    }

    fn password(&self) -> Result<&str, String> {
        std::str::from_utf8(&self.password)
            .map_err(|_| "application password was not valid UTF-8".to_owned())
    }
}

impl Drop for WorkloadCredentialOwner {
    fn drop(&mut self) {
        self.clear();
    }
}

struct SecretProducer {
    value: Vec<u8>,
}

impl SecretProducer {
    fn new(value: String) -> Self {
        Self {
            value: value.into_bytes(),
        }
    }

    fn copy(&self) -> String {
        String::from_utf8(self.value.clone()).expect("producer secret is valid UTF-8")
    }

    fn clear(&mut self) {
        self.value.fill(0);
    }

    fn is_cleared(&self) -> bool {
        self.value.iter().all(|byte| *byte == 0)
    }
}

impl Drop for SecretProducer {
    fn drop(&mut self) {
        self.clear();
    }
}

struct ProxyPortReservations {
    listeners: Vec<TcpListener>,
    addresses: [SocketAddr; 3],
}

impl ProxyPortReservations {
    fn allocate(mut used: HashSet<SocketAddr>) -> Result<Self, String> {
        let mut listeners = Vec::with_capacity(3);
        let mut addresses = Vec::with_capacity(3);
        while listeners.len() < 3 {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
                .map_err(|error| format!("reserve proxy loopback port: {error}"))?;
            let address = listener
                .local_addr()
                .map_err(|error| format!("inspect proxy loopback port: {error}"))?;
            if !used.insert(address) {
                continue;
            }
            listeners.push(listener);
            addresses.push(address);
        }
        Ok(Self {
            listeners,
            addresses: addresses
                .try_into()
                .map_err(|_| "expected exactly three proxy ports".to_owned())?,
        })
    }

    fn release(&mut self) {
        self.listeners.clear();
    }
}

fn topology(
    sql_addresses: [SocketAddr; 3],
    group_addresses: [SocketAddr; 3],
) -> Result<MysqlTopologyConfig, String> {
    MysqlTopologyConfig::new([
        MysqlMemberConfig::new(
            101,
            sql_addresses[0],
            group_addresses[0],
            GROUP_UUID,
            group_addresses,
        )
        .map_err(|error| format!("{error:?}"))?,
        MysqlMemberConfig::new(
            102,
            sql_addresses[1],
            group_addresses[1],
            GROUP_UUID,
            group_addresses,
        )
        .map_err(|error| format!("{error:?}"))?,
        MysqlMemberConfig::new(
            103,
            sql_addresses[2],
            group_addresses[2],
            GROUP_UUID,
            group_addresses,
        )
        .map_err(|error| format!("{error:?}"))?,
    ])
    .map_err(|error| format!("{error:?}"))
}

fn instance_config(
    root: &Path,
    topology: MysqlTopologyConfig,
    member: MysqlMemberIndex,
    timeouts: MysqlOperationTimeouts,
) -> MysqlInstanceConfig {
    let number = member_number(member);
    MysqlInstanceConfig::new_topology_member(
        MYSQLD,
        APPARMOR_EXEC,
        root.join(format!("member-{number}-data")),
        root.join(format!("member-{number}-scratch")),
        topology,
        member,
        timeouts,
    )
    .unwrap()
}

fn replica_identity(label: &str) -> MysqlReplicaIdentity {
    MysqlReplicaIdentity::new(
        ReplicaId::new(format!("replicator-live-{label}")).unwrap(),
        ReplicaIncarnation::new(format!("replicator-live-{label}-{}", std::process::id())).unwrap(),
    )
}

fn configuration(label: &str, members: Vec<MysqlReplicaTarget>) -> MysqlReplicaConfiguration {
    configuration_at(label, MysqlReplicaEpoch::new(1), members)
}

fn configuration_at(
    label: &str,
    epoch: MysqlReplicaEpoch,
    members: Vec<MysqlReplicaTarget>,
) -> MysqlReplicaConfiguration {
    MysqlReplicaConfiguration::new(ConfigurationId::new(label).unwrap(), epoch, members)
}

fn operation(label: &str, generation: u64) -> MysqlOperationIdentity {
    MysqlOperationIdentity::new(
        AuthorityGeneration::new(label).unwrap(),
        NonZeroU64::new(generation).unwrap(),
    )
}

fn member_number(member: MysqlMemberIndex) -> usize {
    match member {
        MysqlMemberIndex::First => 1,
        MysqlMemberIndex::Second => 2,
        MysqlMemberIndex::Third => 3,
    }
}

fn nonzero(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("access generation is nonzero")
}

fn tcp_options(address: SocketAddr, username: &str, password: &str) -> OptsBuilder {
    OptsBuilder::default()
        .ip_or_hostname(address.ip().to_string())
        .tcp_port(address.port())
        .prefer_socket(false)
        .socket(None::<String>)
        .user(Some(username.to_owned()))
        .pass(Some(password.to_owned()))
        .db_name(Some("kuberic_proxy_qualification".to_owned()))
}

async fn root_connection(socket: &Path) -> Result<Conn, String> {
    Conn::new(
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(1)
            .socket(Some(
                socket
                    .to_str()
                    .ok_or_else(|| "private socket path was not UTF-8".to_owned())?
                    .to_owned(),
            ))
            .user(Some("root".to_owned())),
    )
    .await
    .map_err(|error| error.to_string())
}

async fn create_qualification_schema(socket: &Path) -> Result<(), String> {
    let mut root = root_connection(socket).await?;
    root.query_drop("CREATE DATABASE kuberic_proxy_qualification")
        .await
        .map_err(|error| error.to_string())?;
    root.query_drop(
        "CREATE TABLE kuberic_proxy_qualification.tokens (\
         token_name VARCHAR(64) PRIMARY KEY, token_value BIGINT UNSIGNED NOT NULL)",
    )
    .await
    .map_err(|error| error.to_string())?;
    root.disconnect().await.map_err(|error| error.to_string())
}

async fn await_active(engine: &MysqlReplicator) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if engine
            .proxy_snapshot()
            .is_some_and(|snapshot| snapshot.active_commands() == 1)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("proxy command did not become active".to_owned());
        }
        sleep(Duration::from_millis(20)).await;
    }
}

async fn await_sessions(engine: &MysqlReplicator, expected: usize) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if engine
            .proxy_snapshot()
            .is_some_and(|snapshot| snapshot.sessions() == expected)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "proxy sessions did not reach expected count {expected}"
            ));
        }
        sleep(Duration::from_millis(20)).await;
    }
}

fn push_trace(
    trace: &mut Vec<HandoffTraceEntry>,
    started: Instant,
    stage: HandoffStage,
    evidence: String,
) {
    trace.push(HandoffTraceEntry {
        stage,
        elapsed: started.elapsed(),
        evidence,
    });
}
