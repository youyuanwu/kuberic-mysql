//! Real production service/ReplicaHost qualification at the pinned public API.
//!
//! Existing `hosted_*` cases use the testing facade for generic initialization
//! and dispatch. `authenticated::*` cases instead use ordinary bearer RPCs,
//! including initialization, and assert terminal receipts with failed live queries.
//! Neither route claims controller/recovery acceptance.

#[path = "hosted_authenticated/mod.rs"]
mod authenticated;
#[path = "service_live_support/mod.rs"]
mod live_support;

use std::collections::BTreeMap;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::num::NonZeroU64;
use std::path::Path;
use std::process::{Child, Command, Stdio};
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
    INVALID_LSN, PossibleDataLossIntent, ProgressQueryResult, PublicConfiguration,
    PublicLifecycleInput, PublicLifecycleRecipe, PublicOperationClass, PublicOperationIntent,
    PublicOperationProgram, PublicOperationReplayDisposition, PublicV1AgentReport,
    PublicV1CallKind, PublicV1Identity, PublicV1OperationResult,
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
    APPARMOR_EXEC, FixtureRoot, MYSQLD, ReservedPorts, assert_no_application_store,
    assert_no_application_store_with_native_schemas, assert_process_absent, assert_tcp_refuses,
    preflight_oracle_mysql_8_4,
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
        finish_scenario(result, cleanup, Ok(()))
    });
}

#[test]
fn hosted_existing_open_rejects_without_creating_native_roots() {
    run_native(async {
        let mut fixture = HostedFixture::new("no-existing").await?;
        let result = async {
            let error = fixture.execute(PublicOperationProgram::Open {
                replica: fixture.identity.clone(),
                existing: true,
            }).await.expect_err("Existing Open cannot adopt native storage");
            require(
                error == r#"CommandRejected("public operation did not complete successfully: Some(Failed(\"agent store identity mismatch: open context changed\"))")"#,
                &format!("wrong Existing rejection: {error}"),
            )?;
            require(
                !fixture.root.path().join("d").exists()
                    && !fixture.root.path().join("s").exists(),
                "unsupported Existing created native roots",
            )?;
            Ok(())
        }.await;
        let cleanup = fixture.shutdown().await;
        finish_scenario(result, cleanup, Ok(()))
    });
}

#[test]
fn native_schema_allowlist_still_rejects_nested_or_sibling_private_stores() {
    let root = FixtureRoot::new("hosted-scan").unwrap();
    let schema = root.path().join("kuberic_proxy_qualification");
    std::fs::create_dir(&schema).unwrap();
    std::fs::write(schema.join("hosted.ibd"), b"fixture-native-data").unwrap();
    assert_no_application_store_with_native_schemas(root.path(), &["kuberic_proxy_qualification"])
        .unwrap();
    std::fs::write(schema.join("receipt.json"), b"forbidden").unwrap();
    assert!(
        assert_no_application_store_with_native_schemas(
            root.path(),
            &["kuberic_proxy_qualification"]
        )
        .is_err()
    );
    std::fs::remove_file(schema.join("receipt.json")).unwrap();
    std::fs::write(root.path().join("kuberic-metadata.json"), b"forbidden").unwrap();
    assert!(
        assert_no_application_store_with_native_schemas(
            root.path(),
            &["kuberic_proxy_qualification"]
        )
        .is_err()
    );
}

#[test]
fn hosted_graceful_close_contains_exact_child_and_retains_native_evidence() {
    run_native(terminal_scenario(false));
}

#[test]
fn hosted_abort_contains_exact_child_and_retains_native_evidence() {
    run_native(terminal_scenario(true));
}

#[test]
fn hosted_interrupted_open_after_real_server_spawn_is_contained() {
    run_native(async {
        let mut fixture = HostedFixture::new("hosted-cut").await?;
        let mut foreign = ForeignChild::new()?;
        let result = fixture.interrupt_open().await;
        let cleanup = fixture.shutdown().await;
        let unaffected = foreign.assert_live();
        finish_scenario(result, cleanup, unaffected)
    });
}

#[test]
fn service_close_failure_remains_an_exact_obligation_on_retry_and_abort() {
    run_native(async {
        let mut fixture = HostedFixture::new("close-fail").await?;
        let result = async {
            fixture.open().await?;
            // Our own new empty destination creates a real retention failure,
            // not a simulated success or a substituted native owner.
            let collision = fixture.root.path().join("s.retained");
            std::fs::create_dir(&collision).map_err(failure)?;
            require(
                fixture.service.close().await.is_err(),
                "Close ignored collision",
            )?;
            require(
                fixture.service.close().await.is_err(),
                "retry returned vacuous success",
            )?;
            std::fs::remove_dir(&collision).map_err(failure)?;
            fixture.service.abort();
            fixture.assert_contained_evidence()?;
            require(
                fixture.service.close().await.is_err(),
                "settled containment erased an earlier terminal failure",
            )?;
            Ok(())
        }
        .await;
        let cleanup = fixture.shutdown().await;
        finish_scenario(result, cleanup, Ok(()))
    });
}

#[test]
fn dropped_service_close_still_forwards_abort_to_the_exact_live_driver() {
    run_native(async {
        let mut fixture = HostedFixture::new("close-drop").await?;
        let result = async {
            fixture.fresh_primary().await?;
            let mut close = Box::pin(fixture.service.close());
            require(
                std::future::poll_fn(|cx| std::task::Poll::Ready(close.as_mut().poll(cx)))
                    .await
                    .is_pending(),
                "Close did not reach asynchronous native/proxy settlement",
            )?;
            drop(close);
            fixture.service.abort();
            fixture.assert_contained_evidence()?;
            Ok(())
        }
        .await;
        let cleanup = fixture.shutdown().await;
        finish_scenario(result, cleanup, Ok(()))
    });
}

#[test]
fn concurrent_service_close_callers_wait_for_the_same_native_settlement() {
    run_native(async {
        let mut fixture = HostedFixture::new("close-both").await?;
        let result = async {
            fixture.fresh_primary().await?;
            let mut first = Box::pin(fixture.service.close());
            require(
                std::future::poll_fn(|cx| std::task::Poll::Ready(first.as_mut().poll(cx)))
                    .await
                    .is_pending(),
                "first Close never entered asynchronous settlement",
            )?;
            let mut second = Box::pin(fixture.service.close());
            require(
                std::future::poll_fn(|cx| std::task::Poll::Ready(second.as_mut().poll(cx)))
                    .await
                    .is_pending(),
                "second Close acknowledged success before first settled",
            )?;
            let (first, second) = tokio::join!(first, second);
            first.map_err(failure)?;
            second.map_err(failure)?;
            fixture.assert_contained_evidence()?;
            Ok(())
        }
        .await;
        let cleanup = fixture.shutdown().await;
        finish_scenario(result, cleanup, Ok(()))
    });
}

async fn terminal_scenario(abort: bool) -> TestResult {
    let mut fixture = HostedFixture::new(if abort {
        "hosted-abort"
    } else {
        "hosted-close"
    })
    .await?;
    let mut foreign = ForeignChild::new()?;
    let result = async {
        fixture.fresh_primary().await?;
        // Terminal assertions start from a genuinely admitted, live proxy.
        let before = std::fs::read(fixture.root.path().join("d/auto.cnf")).map_err(failure)?;
        let started = Instant::now();
        tokio::time::timeout(
            TERMINAL_BUDGET,
            fixture.execute(if abort {
                PublicOperationProgram::Abort
            } else {
                PublicOperationProgram::Close
            }),
        )
        .await
        .map_err(failure)??;
        require(
            started.elapsed() < TERMINAL_BUDGET,
            "terminal callback exceeded fixture budget",
        )?;
        fixture.assert_contained_evidence()?;
        require(
            std::fs::read(fixture.root.path().join("d/auto.cnf")).map_err(failure)? == before,
            "native data identity changed on termination",
        )?;
        require(
            fixture
                .root
                .path()
                .join("d/kuberic_proxy_qualification/hosted.ibd")
                .is_file(),
            "committed native table was not retained",
        )?;
        assert_terminal_queries(&fixture.report().await?)?;
        eprintln!(
            "terminal {} completed in {:?}; report preserves failed closed-driver queries",
            if abort { "Abort" } else { "Close" },
            started.elapsed()
        );
        foreign.assert_live()?;
        Ok(())
    }
    .await;
    let cleanup = fixture.shutdown().await;
    finish_scenario(result, cleanup, foreign.assert_live())
}

/// A retained unrelated child, signalled only by its own fixture owner.
struct ForeignChild(Child);

impl ForeignChild {
    fn new() -> TestResult<Self> {
        Command::new("/usr/bin/sleep")
            .arg("3600")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map(Self)
            .map_err(failure)
    }

    fn assert_live(&mut self) -> TestResult {
        require(
            self.0.try_wait().map_err(failure)?.is_none(),
            "unrelated child was terminated",
        )
    }
}

impl Drop for ForeignChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
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
    wire: Option<authenticated::WireControl>,
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
        Self::new_with_transport(label, false).await
    }

    async fn new_with_transport(label: &str, ordinary: bool) -> TestResult<Self> {
        preflight_oracle_mysql_8_4()?;
        let root = FixtureRoot::new_retained(label)?;
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
            Instant::now() + Duration::from_secs(600),
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
                Duration::from_secs(180),
                Duration::from_secs(120),
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
        if !ordinary {
            drop(
                SqliteStore::create_public_v1_with_application_paths(
                    SqliteStore::metadata_database_path(&agent_root),
                    PublicV1Identity::new(1),
                    StorageIdentity {
                        schema_version: SCHEMA_VERSION,
                        resource_uid: resource.clone(),
                        pod_uid: pod.clone(),
                        pvc_uid: pvc.clone(),
                        initialization_id: initialization_id.clone(),
                        local_identity: identity.clone(),
                        effective_policy: EffectivePolicy::fixed(1, 30)
                            .ok_or("invalid fixture policy")?,
                    },
                    paths.clone(),
                )
                .map_err(failure)?,
            );
        }
        let service = Arc::new(MysqlPublicV1Service::new(config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(failure)?;
        let address = listener.local_addr().map_err(failure)?;
        ports.release();
        drop(proxy_reservation);
        let process_config = ReplicaProcessConfig {
            resource_uid: resource.clone(),
            replica_id: identity.replica_id,
            pod_uid: pod.clone(),
            pvc_uid: pvc.clone(),
            data_root: agent_root,
            replication_address: address,
            bearer_token: format!("disposable-hosted-control-{}", std::process::id()),
            rpc_deadline: Duration::from_secs(2),
            transport_window_capacity: 8,
        };
        let host = ReplicaHost::new(
            process_config.clone(),
            service.clone(),
            ApplicationStorageState::FreshEmpty,
            Arc::new(SingleResolver { address }),
        )
        .with_application_storage_paths(paths);
        let (running, wire) = if ordinary {
            let (running, wire) = authenticated::WireControl::start(
                host,
                listener,
                process_config,
                &identity,
                &configuration,
                initialization_id.to_string(),
                root.path(),
            )
            .await?;
            (running, Some(wire))
        } else {
            (host.start(listener).await.map_err(failure)?, None)
        };
        let handle = running.handle();
        let mut fixture = Self {
            root,
            service,
            running,
            handle,
            wire,
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
        if fixture.wire.is_some() {
            return Ok(fixture);
        }
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
            process_session_id: self.process_session_id(),
            class,
            input_digest: program.digest(),
            replay_disposition: program.replay_disposition(),
            lifecycle: None,
            program: Some(program),
        }
    }

    async fn execute(&mut self, program: PublicOperationProgram) -> TestResult {
        let intent = self.intent(program);
        self.execute_intent(intent).await
    }

    fn process_session_id(&self) -> kuberic_runtime::protocol::types::ProcessSessionId {
        match &self.wire {
            Some(wire) => wire.session.clone(),
            None => self.handle.process_session_id(),
        }
    }

    async fn execute_intent(&self, intent: PublicOperationIntent) -> TestResult {
        match &self.wire {
            Some(wire) => wire.execute(intent).await.map(|_| ()).map_err(failure),
            None => self.handle.execute_public_v1(intent).await.map_err(failure),
        }
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
        if self.wire.is_none() {
            require(
                self.handle.public_v1_provider_writer_count() == 0,
                "progress-only provider acquired writer",
            )?;
        }
        require(
            report.progress.current() == Some(0)
                && report.progress.capability() == Some(INVALID_LSN),
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
            process_session_id: self.process_session_id(),
            class: PublicOperationClass::Authority,
            input_digest: lifecycle.digest(),
            replay_disposition: PublicOperationReplayDisposition::Convergent,
            lifecycle: Some(lifecycle),
            program: None,
        };
        self.execute_intent(intent)
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
        self.reject_unauthorized_proxy_logins().await?;
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
        let formed_progress = formed
            .progress
            .current()
            .ok_or("formed current progress is unavailable")?;
        let repeated_progress = repeated
            .progress
            .current()
            .ok_or("repeated current progress is unavailable")?;
        require(
            repeated.progress.capability() == Some(INVALID_LSN),
            "unknown native catch-up capability became a commitment claim",
        )?;
        require(
            repeated_progress > formed_progress,
            "real native transactions did not advance hosted progress",
        )?;
        require(
            self.gtids().await? != GtidSet::empty(),
            "formed native member has no GTID evidence",
        )?;
        Ok(())
    }

    async fn report(&self) -> TestResult<PublicV1AgentReport> {
        let report = match &self.wire {
            Some(wire) => wire.report().await.map_err(failure)?,
            None => self.handle.public_v1_report().await.map_err(failure)?,
        };
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

    async fn reject_unauthorized_proxy_logins(&self) -> TestResult {
        for (user, password) in [
            (
                self.application_user.as_str(),
                "incorrect-disposable-password",
            ),
            ("root", ""),
        ] {
            let options = OptsBuilder::default()
                .ip_or_hostname(self.proxy.ip().to_string())
                .tcp_port(self.proxy.port())
                .user(Some(user))
                .pass(Some(password));
            match Conn::new(options).await {
                // The production proxy can close a rejected authentication
                // exchange rather than emit a MySQL ERR packet. A valid
                // command immediately afterwards rules out a dead listener.
                Err(_) => {}
                Ok(connection) => {
                    let _ = connection.disconnect().await;
                    return Err("unauthorized login was admitted by the production proxy".into());
                }
            }
            let mut valid = self
                .application_connection(self.proxy)
                .await
                .map_err(failure)?;
            let value: Option<u64> = valid.query_first("SELECT 1").await.map_err(failure)?;
            require(
                value == Some(1),
                "proxy unavailable after rejected authentication",
            )?;
            valid.disconnect().await.map_err(failure)?;
        }
        Ok(())
    }

    fn assert_contained_evidence(&self) -> TestResult {
        let pid = self.pid.ok_or("no observed actual server PID")?;
        assert_process_absent(pid)?;
        assert_tcp_refuses(self.sql)?;
        assert_tcp_refuses(self.proxy)?;
        require(
            !self.root.path().join("s").exists(),
            "active runtime path was not retired",
        )?;
        require(
            self.root.path().join("d/auto.cnf").is_file(),
            "initialized native data was lost",
        )?;
        for log in [
            "mysqld.err",
            "mysqld.out",
            "initialize.out",
            "initialize.err",
            "version.out",
        ] {
            require(
                self.root.path().join("s.retained").join(log).is_file(),
                "native log evidence was lost",
            )?;
        }
        assert_no_application_store_with_native_schemas(
            &self.root.path().join("d"),
            &["kuberic_proxy_qualification"],
        )?;
        assert_no_application_store(&self.root.path().join("s.retained"))?;
        Ok(())
    }

    async fn interrupt_open(&mut self) -> TestResult {
        let intent = self.intent(PublicOperationProgram::Open {
            replica: self.identity.clone(),
            existing: false,
        });
        let handle = self.handle.clone();
        let mut opening = tokio::spawn(async move { handle.execute_public_v1(intent).await });
        let defaults = format!(
            "--defaults-file={}",
            self.root.path().join("s/my.cnf").display()
        );
        // Initialization precedes the cut; allow the existing native fixture's
        // initialization budget on a constrained host. Settlement remains 45s.
        let observed = tokio::time::timeout(Duration::from_secs(180), async {
            loop {
                if opening.is_finished() {
                    return Err(
                        "Open completed before a proven startup interruption cut".to_owned()
                    );
                }
                if let Some(pid) = actual_starting_server(&defaults)? {
                    require(
                        self.root.path().join("d/auto.cnf").is_file(),
                        "server spawn preceded native initialization",
                    )?;
                    require(!opening.is_finished(), "Open won the interruption race")?;
                    self.pid = Some(pid);
                    // Same production AttemptContext, not RPC-client cancellation
                    // or a synthetic generic containment witness.
                    self.attempt.cancel();
                    eprintln!(
                        "cancelled real ApplicationOpen after actual mysqld spawn: pid={pid}"
                    );
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .map_err(failure)
        .and_then(|r| r);
        if observed.is_err() {
            self.attempt.cancel();
        }
        let settled = match tokio::time::timeout(TERMINAL_BUDGET, &mut opening).await {
            Ok(result) => result.map_err(failure),
            Err(error) => {
                opening.abort();
                let joined = opening.await;
                return Err(format!(
                    "interrupted Open did not settle: {error}; aborted task acknowledgment: {joined:?}"
                ));
            }
        }?;
        observed?;
        require(
            settled.is_err(),
            "cancelled real startup was falsely reported successful",
        )?;
        eprintln!("interrupted public Open outcome: {settled:?}");
        self.assert_contained_evidence()?;
        require(
            !self.root.path().join("s.retained/mysql.sock").exists(),
            "interrupted startup retained a live socket name",
        )?;
        Ok(())
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

fn actual_starting_server(defaults: &str) -> TestResult<Option<u32>> {
    for entry in std::fs::read_dir("/proc").map_err(failure)? {
        let entry = entry.map_err(failure)?;
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(executable) = std::fs::read_link(entry.path().join("exe")) else {
            continue;
        };
        if executable != Path::new(MYSQLD) {
            continue;
        }
        let Ok(command) = std::fs::read(entry.path().join("cmdline")) else {
            continue;
        };
        let arguments = command.split(|byte| *byte == 0).collect::<Vec<_>>();
        if arguments.contains(&defaults.as_bytes())
            && !arguments
                .iter()
                .any(|arg| arg.starts_with(b"--initialize") || *arg == b"--version")
        {
            return Ok(Some(pid));
        }
    }
    Ok(None)
}

impl Drop for HostedFixture {
    fn drop(&mut self) {
        self.attempt.cancel();
        self.service.abort();
        self.running.shutdown();
    }
}

fn finish_scenario(scenario: TestResult, cleanup: TestResult, foreign: TestResult) -> TestResult {
    let errors = [
        ("scenario", scenario),
        ("shutdown/containment", cleanup),
        ("foreign child", foreign),
    ]
    .into_iter()
    .filter_map(|(label, result)| result.err().map(|error| format!("{label}: {error}")))
    .collect::<Vec<_>>();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

fn assert_terminal_queries(report: &PublicV1AgentReport) -> TestResult {
    report.validate().map_err(failure)?;
    require(
        report.role == ReplicaRole::None
            && report.read_status != AccessStatus::Granted
            && report.write_status != AccessStatus::Granted
            && report.progress.current_progress
                == ProgressQueryResult::Failed("runtime is closed".into())
            && report.progress.catch_up_capability
                == ProgressQueryResult::Failed("runtime is closed".into()),
        &format!("terminal report fabricated live progress or access: {report:?}"),
    )
}

#[test]
fn failed_scenario_keeps_all_cleanup_and_noninterference_errors() {
    let result = finish_scenario(
        Err("primary failure".into()),
        Err("child still live".into()),
        Err("sentinel exited".into()),
    );
    assert_eq!(
        result.unwrap_err(),
        "scenario: primary failure; shutdown/containment: child still live; foreign child: sentinel exited"
    );
    assert!(finish_scenario(Ok(()), Ok(()), Ok(())).is_ok());
    assert!(finish_scenario(Ok(()), Err("cleanup failed".into()), Ok(())).is_err());
}
