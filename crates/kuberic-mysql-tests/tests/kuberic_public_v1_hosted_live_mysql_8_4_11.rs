//! Real production service/ReplicaHost qualification at the pinned public API.
//!
//! Only generic store/history initialization and operation dispatch use the
//! runtime's testing facade. This is not bearer-wire or controller/recovery
//! acceptance coverage. No replacement service, direct driver, or fake provider
//! participates in the hosted lifecycle.

#[path = "service_live_support/mod.rs"]
mod live_support;

use std::collections::BTreeMap;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::num::NonZeroU64;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kuberic_mysql::core::{AttemptId, ConfiguredGtidLineage, CredentialGeneration, GtidSet};
use kuberic_mysql::service::{
    ApplicationCredential, AttemptContext, ControlCredential, ControlCredentialRole,
    MysqlFreshMemberCredentials, MysqlInstanceConfig, MysqlMemberConfig, MysqlMemberIndex,
    MysqlOperationTimeouts, MysqlPublicV1Service, MysqlPublicV1ServiceConfig, MysqlTopologyConfig,
};
use kuberic_runtime::application::StatefulServiceReplica;
use kuberic_runtime::host::{
    ApplicationStorageState, ReplicaEndpointResolver, ReplicaHandle, ReplicaHost,
    ReplicaProcessConfig, RunningReplica,
};
use kuberic_runtime::protocol::public_operations::{
    PossibleDataLossIntent, PublicConfiguration, PublicLifecycleInput, PublicLifecycleRecipe,
    PublicOperationClass, PublicOperationIntent, PublicOperationProgram,
    PublicOperationReplayDisposition, PublicV1AgentReport, PublicV1CallKind, PublicV1Identity,
    PublicV1OperationResult,
};
use kuberic_runtime::protocol::types::{
    AccessStatus, ConfigurationDescriptor, ConfigurationMember, EffectivePolicy, Epoch,
    OperationId, PodUid, PublicProtocolGeneration, PvcUid, ReplicaId, ReplicaIdentity,
    ReplicaInstanceId, ReplicaRole, ReplicaStorageIdentity, ReplicaStorageIncarnationContext,
    ResourceUid, StorageGeneration, derive_agent_generation, derive_bootstrap_history,
    derive_history_initialization_id, derive_initialization_id,
};
use kuberic_runtime::testing::sqlite_store::SqliteStore;
use kuberic_runtime::testing::state::{SCHEMA_VERSION, StorageIdentity};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder};

use live_support::{
    APPARMOR_EXEC, FixtureRoot, MYSQLD, ReservedPorts, assert_process_absent, assert_tcp_refuses,
    preflight_oracle_mysql_8_4_11,
};

const GROUP_UUID: &str = "b5a47b3f-bf9f-4c7c-8420-495d5a52cb09";
const TERMINAL_BUDGET: Duration = Duration::from_secs(45);
type TestResult<T = ()> = Result<T, String>;

#[test]
fn hosted_new_factory_initial_primary_progress_and_native_access() {
    run_native(async {
        let mut fixture = HostedFixture::new("hosted-new").await?;
        let result = fixture.fresh_primary().await;
        let cleanup = fixture.shutdown().await;
        result.and(cleanup)
    });
}

fn run_native(future: impl Future<Output = TestResult> + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(future)
        })
        .unwrap()
        .join()
        .unwrap()
        .unwrap_or_else(|error| panic!("{error}"));
}

fn require(value: bool, message: &str) -> TestResult {
    if value { Ok(()) } else { Err(message.into()) }
}

fn failure(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

struct SingleResolver {
    address: SocketAddr,
}

impl ReplicaEndpointResolver for SingleResolver {
    fn control_endpoint(&self, _: &ReplicaIdentity) -> String {
        format!("http://{}", self.address)
    }

    fn replication_endpoint(&self, _: &ReplicaIdentity) -> String {
        format!("http://{}", self.address)
    }
}

struct HostedFixture {
    // Retained even on failure. Generic host state is separate from native roots.
    root: FixtureRoot,
    service: Arc<MysqlPublicV1Service>,
    running: RunningReplica,
    handle: ReplicaHandle,
    identity: ReplicaIdentity,
    configuration: ConfigurationDescriptor,
    attempt: AttemptContext,
    sql: SocketAddr,
    proxy: SocketAddr,
    pid: Option<u32>,
    revision: u64,
    application_user: String,
    application_password: String,
}

impl HostedFixture {
    async fn new(label: &str) -> TestResult<Self> {
        preflight_oracle_mysql_8_4_11()?;
        let mut root = FixtureRoot::new(label)?;
        root.preserve();
        eprintln!("retained hosted fixture: {}", root.path().display());
        let mut ports = ReservedPorts::allocate()?;
        let sql = ports.sql_addresses();
        let group = ports.group_replication_addresses();
        let topology = MysqlTopologyConfig::new([
            MysqlMemberConfig::new(201, sql[0], group[0], GROUP_UUID, group).map_err(failure)?,
            MysqlMemberConfig::new(202, sql[1], group[1], GROUP_UUID, group).map_err(failure)?,
            MysqlMemberConfig::new(203, sql[2], group[2], GROUP_UUID, group).map_err(failure)?,
        ])
        .map_err(failure)?;
        let proxy_reservation = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).map_err(failure)?;
        let proxy = proxy_reservation.local_addr().map_err(failure)?;
        let resource = ResourceUid::new(format!("{label}-{}", std::process::id()));
        let pod = PodUid::new(format!("{label}-pod"));
        let pvc = PvcUid::new(format!("{label}-pvc"));
        let initialization_id = derive_initialization_id(&resource, ReplicaId::new(1), &pod, &pvc);
        let identity = ReplicaIdentity {
            replica_id: ReplicaId::new(1),
            instance_id: ReplicaInstanceId::new(pod.as_str()),
            agent_generation: derive_agent_generation(&initialization_id),
        };
        let configuration = ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            identity.replica_id,
            vec![ConfigurationMember {
                identity: identity.clone(),
                role: ReplicaRole::Primary,
            }],
            1,
        );
        let attempt = AttemptContext::new(
            AttemptId::new(format!("{label}-attempt")).map_err(failure)?,
            Instant::now() + Duration::from_secs(300),
            Duration::from_secs(20),
        )
        .map_err(failure)?;
        let application_user = "kms_hosted_app".to_owned();
        // Per-fixture disposable credentials, never a deployed credential.
        let application_password = format!("HostedFixture{}App7", std::process::id());
        let credentials = MysqlFreshMemberCredentials::new(
            ControlCredential::new(
                attempt.attempt().clone(),
                CredentialGeneration::new("hosted-observer").map_err(failure)?,
                ControlCredentialRole::Observer,
                "kms_hosted_obs",
                format!("HostedFixture{}Obs7", std::process::id()),
            )
            .map_err(failure)?,
            ControlCredential::new(
                attempt.attempt().clone(),
                CredentialGeneration::new("hosted-recovery").map_err(failure)?,
                ControlCredentialRole::Recovery,
                "kms_hosted_rec",
                format!("HostedFixture{}Rec7", std::process::id()),
            )
            .map_err(failure)?,
            ApplicationCredential::new(
                attempt.attempt().clone(),
                CredentialGeneration::new("hosted-application").map_err(failure)?,
                application_user.clone(),
                application_password.clone(),
            )
            .map_err(failure)?,
        );
        let instance = MysqlInstanceConfig::new_topology_member(
            MYSQLD,
            APPARMOR_EXEC,
            root.path().join("d"),
            root.path().join("s"),
            topology,
            MysqlMemberIndex::First,
            MysqlOperationTimeouts::new(
                Duration::from_secs(90),
                Duration::from_secs(60),
                Duration::from_secs(15),
                Duration::from_secs(5),
            )
            .map_err(failure)?,
        )
        .map_err(failure)?;
        let config = MysqlPublicV1ServiceConfig::new(
            resource.as_str(),
            instance,
            ConfiguredGtidLineage::new(GROUP_UUID).map_err(failure)?,
            credentials,
            attempt.clone(),
            NonZeroU64::MIN,
            proxy,
        )
        .map_err(failure)?;
        let paths = BTreeMap::from([
            ("mysql-data".to_owned(), root.path().join("d")),
            ("mysql-runtime".to_owned(), root.path().join("s")),
        ]);
        let agent_root = root.path().join("k");
        // This creates generic runtime state only. Never inspect its private store.
        drop(
            SqliteStore::create_public_v1_with_application_paths(
                SqliteStore::metadata_database_path(&agent_root),
                PublicV1Identity::new(1),
                StorageIdentity {
                    schema_version: SCHEMA_VERSION,
                    resource_uid: resource.clone(),
                    pod_uid: pod.clone(),
                    pvc_uid: pvc.clone(),
                    initialization_id,
                    local_identity: identity.clone(),
                    effective_policy: EffectivePolicy::fixed(1, 30)
                        .ok_or("invalid fixture policy")?,
                },
                paths.clone(),
            )
            .map_err(failure)?,
        );
        let service = Arc::new(MysqlPublicV1Service::new(config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(failure)?;
        let address = listener.local_addr().map_err(failure)?;
        ports.release();
        drop(proxy_reservation);
        let running = ReplicaHost::new(
            ReplicaProcessConfig {
                resource_uid: resource.clone(),
                replica_id: identity.replica_id,
                pod_uid: pod.clone(),
                pvc_uid: pvc.clone(),
                data_root: agent_root,
                replication_address: address,
                bearer_token: "hosted-fixture-not-wire-authenticated".into(),
                rpc_deadline: Duration::from_secs(2),
                transport_window_capacity: 8,
            },
            service.clone(),
            ApplicationStorageState::FreshEmpty,
            Arc::new(SingleResolver { address }),
        )
        .with_application_storage_paths(paths)
        .start(listener)
        .await
        .map_err(failure)?;
        let handle = running.handle();
        let mut fixture = Self {
            root,
            service,
            running,
            handle,
            identity: identity.clone(),
            configuration,
            attempt,
            sql: sql[0],
            proxy,
            pid: None,
            revision: 0,
            application_user,
            application_password,
        };
        let generation = PublicProtocolGeneration::new(1);
        let history_id = derive_history_initialization_id(&resource, generation);
        let history = derive_bootstrap_history(&resource, generation, &history_id);
        let storage = ReplicaStorageIncarnationContext {
            replica: identity,
            storage: ReplicaStorageIdentity::Persistent {
                pod_uid: pod,
                pvc_uid: pvc,
            },
            storage_generation: StorageGeneration::new(format!("{label}-storage")),
        };
        if let Err(error) = fixture
            .handle
            .initialize_public_v1_history(&history_id, &history, &storage)
            .await
        {
            let cleanup = fixture.shutdown().await;
            return Err(format!(
                "initialize host history: {error}; cleanup: {cleanup:?}"
            ));
        }
        Ok(fixture)
    }

    fn intent(&mut self, program: PublicOperationProgram) -> PublicOperationIntent {
        self.revision += 1;
        let class = match program {
            PublicOperationProgram::Close => PublicOperationClass::Close,
            PublicOperationProgram::Abort => PublicOperationClass::Abort,
            _ => PublicOperationClass::Authority,
        };
        PublicOperationIntent {
            identity: PublicV1Identity::new(1),
            operation_id: OperationId::new(format!("hosted-op-{}", self.revision)),
            revision: self.revision,
            process_session_id: self.handle.process_session_id(),
            class,
            input_digest: program.digest(),
            replay_disposition: program.replay_disposition(),
            lifecycle: None,
            program: Some(program),
        }
    }

    async fn execute(&mut self, program: PublicOperationProgram) -> TestResult {
        let intent = self.intent(program);
        self.handle.execute_public_v1(intent).await.map_err(failure)
    }

    async fn open(&mut self) -> TestResult<PublicV1AgentReport> {
        self.execute(PublicOperationProgram::Open {
            replica: self.identity.clone(),
            existing: false,
        })
        .await
        .map_err(|e| format!("real host New/Open: {e}"))?;
        let pid = std::fs::read_to_string(self.root.path().join("s/mysqld.pid"))
            .map_err(failure)?
            .trim()
            .parse::<u32>()
            .map_err(failure)?;
        require(
            std::fs::read_link(format!("/proc/{pid}/exe")).map_err(failure)? == Path::new(MYSQLD),
            "native PID is not the selected mysqld",
        )?;
        self.pid = Some(pid);
        let report = self.report().await?;
        require(report.replica == self.identity, "host identity differs")?;
        require(
            !report.replicator_creation_id.as_str().contains("pending"),
            "factory was not registered",
        )?;
        require(
            report
                .replication_address
                .as_ref()
                .is_some_and(|a| a.contains("mysql")),
            "missing custom native endpoint",
        )?;
        require(
            self.handle.public_v1_provider_writer_count() == 0,
            "progress-only provider acquired writer",
        )?;
        require(
            report.progress.first == 0 && report.progress.last == 0,
            "fresh-empty zero proof differs",
        )?;
        require(
            report.read_status != AccessStatus::Granted
                && report.write_status != AccessStatus::Granted,
            "fresh Open granted access",
        )?;
        assert_tcp_refuses(self.proxy)?;
        let gtids = self.gtids().await?;
        require(
            gtids == GtidSet::empty(),
            "New progress zero did not describe an empty native history",
        )?;
        self.reject_direct_login().await?;
        Ok(report)
    }

    async fn initial_primary(&mut self) -> TestResult {
        self.revision += 1;
        let lifecycle = PublicLifecycleInput {
            recipe: PublicLifecycleRecipe::InitialPrimary,
            replica: self.identity.clone(),
            epoch: self.configuration.epoch,
            possible_data_loss: PossibleDataLossIntent::NotPossible,
            current: self.configuration.clone(),
            previous: None,
            replicas: Vec::new(),
            data_loss_recovery: None,
        };
        let intent = PublicOperationIntent {
            identity: PublicV1Identity::new(1),
            operation_id: OperationId::new(format!("hosted-primary-{}", self.revision)),
            revision: self.revision,
            process_session_id: self.handle.process_session_id(),
            class: PublicOperationClass::Authority,
            input_digest: lifecycle.digest(),
            replay_disposition: PublicOperationReplayDisposition::Convergent,
            lifecycle: Some(lifecycle),
            program: None,
        };
        self.handle
            .execute_public_v1(intent)
            .await
            .map_err(|e| format!("real host InitialPrimary: {e}"))
    }

    async fn fresh_primary(&mut self) -> TestResult {
        let opened = self.open().await?;
        self.initial_primary().await?;
        let formed = self.report().await?;
        require(
            formed.replicator_creation_id == opened.replicator_creation_id,
            "factory creation changed",
        )?;
        require(
            formed.process_session_id == opened.process_session_id
                && formed.report_sequence > opened.report_sequence,
            "host report session/sequence differs",
        )?;
        require(
            formed.role == ReplicaRole::Primary,
            "host did not form Primary",
        )?;
        require(
            formed.read_status == AccessStatus::Granted
                && formed.write_status == AccessStatus::Granted,
            "InitialPrimary did not genuinely admit client access",
        )?;
        require(
            formed.completed_operations.iter().any(|op| {
                op.call_kind == PublicV1CallKind::InitialPrimary
                    && op.result == PublicV1OperationResult::Succeeded
            }),
            "missing successful public lifecycle completion",
        )?;
        let mut root = self.root_connection().await?;
        root.query_drop("CREATE DATABASE kuberic_proxy_qualification")
            .await
            .map_err(failure)?;
        root.query_drop(
            "CREATE TABLE kuberic_proxy_qualification.hosted (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)"
        ).await.map_err(failure)?;
        root.disconnect().await.map_err(failure)?;
        let mut client = self
            .application_connection(self.proxy)
            .await
            .map_err(failure)?;
        let value: Option<u64> = client.query_first("SELECT 41 + 1").await.map_err(failure)?;
        require(value == Some(42), "admitted native proxy command failed")?;
        client
            .query_drop("INSERT INTO kuberic_proxy_qualification.hosted VALUES (1, 42)")
            .await
            .map_err(failure)?;
        let stored: Option<u64> = client
            .query_first("SELECT value FROM kuberic_proxy_qualification.hosted WHERE id = 1")
            .await
            .map_err(failure)?;
        require(
            stored == Some(42),
            "admitted proxy did not commit/read real native DML",
        )?;
        client.disconnect().await.map_err(failure)?;
        self.reject_direct_login().await?;
        self.execute(PublicOperationProgram::Epoch {
            epoch: self.configuration.epoch,
        })
        .await?;
        self.execute(PublicOperationProgram::Configuration(PublicConfiguration {
            current: self.configuration.clone(),
            previous: None,
            replicas: Vec::new(),
        }))
        .await?;
        self.execute(PublicOperationProgram::Progress { capability: false })
            .await?;
        let repeated = self.report().await?;
        require(
            repeated.progress.first >= formed.progress.first
                && repeated.progress.last >= formed.progress.last,
            "reported durability regressed",
        )?;
        require(
            repeated.progress.first == repeated.progress.last,
            "registered applied/committed pair is incoherent",
        )?;
        require(
            repeated.progress.last > formed.progress.last,
            "real native transactions did not advance hosted progress",
        )?;
        require(
            self.gtids().await? != GtidSet::empty(),
            "formed native member has no GTID evidence",
        )?;
        Ok(())
    }

    async fn report(&self) -> TestResult<PublicV1AgentReport> {
        let report = self.handle.public_v1_report().await.map_err(failure)?;
        report.validate().map_err(failure)?;
        eprintln!(
            "host report: session={} creation={} sequence={} role={:?} read={:?} write={:?} progress={:?} completed={}",
            report.process_session_id.as_str(),
            report.replicator_creation_id.as_str(),
            report.report_sequence,
            report.role,
            report.read_status,
            report.write_status,
            report.progress,
            report.completed_operations.len()
        );
        Ok(report)
    }

    async fn gtids(&self) -> TestResult<GtidSet> {
        let mut root = self.root_connection().await?;
        let value: String = root
            .query_first("SELECT @@GLOBAL.gtid_executed")
            .await
            .map_err(failure)?
            .ok_or("missing full GTID result")?;
        root.disconnect().await.map_err(failure)?;
        GtidSet::from_str(&value).map_err(failure)
    }

    async fn root_connection(&self) -> TestResult<Conn> {
        let options = OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(1)
            .socket(Some(
                self.root
                    .path()
                    .join("s/mysql.sock")
                    .to_string_lossy()
                    .into_owned(),
            ))
            .user(Some("root"));
        Conn::new(options).await.map_err(failure)
    }

    async fn application_connection(&self, address: SocketAddr) -> mysql_async::Result<Conn> {
        Conn::new(
            OptsBuilder::default()
                .ip_or_hostname(address.ip().to_string())
                .tcp_port(address.port())
                .user(Some(self.application_user.clone()))
                .pass(Some(self.application_password.clone())),
        )
        .await
    }

    async fn reject_direct_login(&self) -> TestResult {
        match self.application_connection(self.sql).await {
            Ok(connection) => {
                let _ = connection.disconnect().await;
                Err("application login bypassed the hosted proxy".into())
            }
            Err(_) => Ok(()),
        }
    }

    async fn shutdown(&mut self) -> TestResult {
        self.service.abort();
        self.running.shutdown();
        tokio::time::timeout(TERMINAL_BUDGET, self.running.wait())
            .await
            .map_err(failure)?
            .map_err(failure)?;
        if let Some(pid) = self.pid {
            assert_process_absent(pid)?;
        }
        assert_tcp_refuses(self.proxy)?;
        Ok(())
    }
}

impl Drop for HostedFixture {
    fn drop(&mut self) {
        self.attempt.cancel();
        self.service.abort();
        self.running.shutdown();
    }
}
