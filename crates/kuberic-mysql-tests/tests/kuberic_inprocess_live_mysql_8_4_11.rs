//! Native custom-replicator capability probe, not settled-cluster qualification.

#[path = "service_live_support/mod.rs"]
mod live_support;

use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::num::NonZeroU64;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use kuberic_controller::test_util::{
    ApplicationFactory, CreatedApplication, ErrorKind, ReplicaContext, TestCluster, TestError,
};
use kuberic_mysql::core::{AttemptId, ConfiguredGtidLineage, CredentialGeneration};
use kuberic_mysql::service::{
    ApplicationCredential, AttemptContext, ControlCredential, ControlCredentialRole,
    MysqlFreshMemberCredentials, MysqlInstanceConfig, MysqlMemberConfig, MysqlMemberIndex,
    MysqlOperationTimeouts, MysqlPublicV1Service, MysqlPublicV1ServiceConfig, MysqlTopologyConfig,
};
use kuberic_runtime::StatefulServiceReplica;
use kuberic_runtime::host::ApplicationStorageState;
use kuberic_runtime::protocol::public_operations::{PublicOperationProgram, PublicV1CallKind};
use kuberic_runtime::protocol::types::{AccessStatus, ReplicaRole};
use serde_json::Value;

use live_support::{
    APPARMOR_EXEC, MYSQL_X_PORT, MYSQLD, ReservedPorts, assert_no_application_store,
    assert_tcp_refuses, preflight_oracle_mysql_8_4_11,
};

const GROUP_UUID: &str = "03e84887-4d65-42d0-b941-0f691cb0aa16";
const CHILD_MODE: &str = "KUBERIC_MYSQL_INPROCESS_PROBE_CHILD";
const NATIVE_TEST: &str = "controller_fresh_open_boundary_keeps_native_state_absent";
const EXPECTED_BOUNDARY: &str = "current-pin fresh MySQL controller Open boundary";
const OPEN_MISMATCH: &str = "agent store identity mismatch: open context changed";

#[test]
fn controller_fresh_open_boundary_keeps_native_state_absent() -> Result<()> {
    if std::env::var_os(CHILD_MODE).is_some() {
        preflight_oracle_mysql_8_4_11().map_err(anyhow::Error::msg)?;
        return tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_stack_size(8 * 1024 * 1024)
            .enable_all()
            .build()?
            .block_on(controller_probe());
    }

    // Only the child harness storage uses this short path, not Cargo compilation.
    // Three random characters keep its UUID/PVC/application socket below 108 bytes.
    let root = tempfile::Builder::new()
        .prefix("")
        .rand_bytes(3)
        .tempdir_in("/tmp")?
        .keep();
    eprintln!("retained MySQL in-process probe root: {}", root.display());
    let output = Command::new(std::env::current_exe()?)
        .args(["--exact", NATIVE_TEST, "--nocapture"])
        .env(CHILD_MODE, "1")
        .env("CARGO_TARGET_DIR", &root)
        .env_remove("KUBERIC_WORKSPACE_TESTS")
        .output()?;
    eprintln!("{}", String::from_utf8_lossy(&output.stdout));
    eprintln!("{}", String::from_utf8_lossy(&output.stderr));
    ensure!(output.status.success(), "native probe child failed");
    Ok(())
}

#[derive(Clone)]
struct MysqlFactory {
    topology: MysqlTopologyConfig,
    proxies: [SocketAddr; 3],
    state: Arc<Mutex<FactoryState>>,
}

struct FactoryState {
    ports: ReservedPorts,
    proxy_reservations: [Option<TcpListener>; 3],
    members: Vec<NativeMember>,
}

#[derive(Clone)]
struct NativeMember {
    service: Arc<MysqlPublicV1Service>,
    root: PathBuf,
    sql: SocketAddr,
    group: SocketAddr,
    proxy: SocketAddr,
    drained: bool,
}

impl MysqlFactory {
    fn new() -> Result<Self> {
        let ports = ReservedPorts::allocate().map_err(anyhow::Error::msg)?;
        let sql = ports.sql_addresses();
        let group = ports.group_replication_addresses();
        let topology = MysqlTopologyConfig::new([
            MysqlMemberConfig::new(301, sql[0], group[0], GROUP_UUID, group)
                .map_err(native_error)?,
            MysqlMemberConfig::new(302, sql[1], group[1], GROUP_UUID, group)
                .map_err(native_error)?,
            MysqlMemberConfig::new(303, sql[2], group[2], GROUP_UUID, group)
                .map_err(native_error)?,
        ])
        .map_err(native_error)?;
        let first = reserve_proxy()?;
        let second = reserve_proxy()?;
        let third = reserve_proxy()?;
        let proxies = [
            first.local_addr()?,
            second.local_addr()?,
            third.local_addr()?,
        ];
        Ok(Self {
            topology,
            proxies,
            state: Arc::new(Mutex::new(FactoryState {
                ports,
                proxy_reservations: [Some(first), Some(second), Some(third)],
                members: Vec::new(),
            })),
        })
    }

    fn prepare(&self, context: ReplicaContext) -> Result<CreatedApplication<MysqlPublicV1Service>> {
        let identity = context.identity();
        let (slot, index) = match identity.replica_id.value() {
            1 => (0, MysqlMemberIndex::First),
            2 => (1, MysqlMemberIndex::Second),
            3 => (2, MysqlMemberIndex::Third),
            _ => bail!("probe supports only the harness's initial three replicas"),
        };
        let environment = context.environment();
        let partition = environment
            .get("KUBERIC_RESOURCE_UID")
            .context("factory lacks resource identity")?;
        ensure!(
            environment.get("KUBERIC_POD_UID").map(String::as_str)
                == Some(identity.instance_id.as_str()),
            "factory Pod identity differs from replica context"
        );
        let attempt = AttemptContext::new(
            AttemptId::new(format!("inprocess-{}", identity.instance_id))?,
            Instant::now() + Duration::from_secs(600),
            Duration::from_secs(20),
        )
        .map_err(native_error)?;
        let credentials = MysqlFreshMemberCredentials::new(
            ControlCredential::new(
                attempt.attempt().clone(),
                CredentialGeneration::new("inprocess-observer")?,
                ControlCredentialRole::Observer,
                "kms_probe_obs",
                format!("InprocessFixture{}Obs7", std::process::id()),
            )?,
            ControlCredential::new(
                attempt.attempt().clone(),
                CredentialGeneration::new("inprocess-recovery")?,
                ControlCredentialRole::Recovery,
                "kms_probe_rec",
                format!("InprocessFixture{}Rec7", std::process::id()),
            )?,
            ApplicationCredential::new(
                attempt.attempt().clone(),
                CredentialGeneration::new("inprocess-application")?,
                "kms_probe_app",
                format!("InprocessFixture{}App7", std::process::id()),
            )?,
        );
        let root = context.application_root();
        let metadata = std::fs::symlink_metadata(root)?;
        ensure!(metadata.is_dir() && !metadata.file_type().is_symlink());
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700))?;
        let data = root.join("d");
        let runtime = root.join("s");
        let instance = MysqlInstanceConfig::new_topology_member(
            MYSQLD,
            APPARMOR_EXEC,
            &data,
            &runtime,
            self.topology.clone(),
            index,
            MysqlOperationTimeouts::new(
                Duration::from_secs(180),
                Duration::from_secs(120),
                Duration::from_secs(15),
                Duration::from_secs(5),
            )
            .map_err(native_error)?,
        )
        .map_err(native_error)?;
        let config = MysqlPublicV1ServiceConfig::new(
            partition,
            instance,
            ConfiguredGtidLineage::new(GROUP_UUID)?,
            credentials,
            attempt,
            NonZeroU64::MIN,
            self.proxies[slot],
        )?;
        let storage = MysqlPublicV1ServiceConfig::storage_state(&config)?;
        ensure!(storage == ApplicationStorageState::FreshEmpty);
        let service = Arc::new(MysqlPublicV1Service::new(config));
        ensure!(
            !data.exists() && !runtime.exists(),
            "factory opened native state"
        );
        let mut state = self.state.lock().unwrap();
        ensure!(
            !state
                .members
                .iter()
                .any(|member| member.proxy == self.proxies[slot]),
            "probe does not support restart or replacement"
        );
        state.members.push(NativeMember {
            service: service.clone(),
            root: root.to_owned(),
            sql: self.topology.members()[slot].sql_address(),
            group: self.topology.members()[slot].group_replication_address(),
            proxy: self.proxies[slot],
            drained: false,
        });
        state.ports.release();
        state.proxy_reservations[slot].take();
        Ok(CreatedApplication::new(service, storage).with_storage_paths([data, runtime]))
    }

    fn members(&self) -> Result<Vec<NativeMember>> {
        let state = self.state.lock().unwrap();
        ensure!(
            state.members.len() == 3,
            "not all MySQL services were constructed"
        );
        Ok(state.members.clone())
    }
}

#[async_trait]
impl ApplicationFactory for MysqlFactory {
    type Service = MysqlPublicV1Service;

    async fn create(
        &self,
        context: ReplicaContext,
    ) -> kuberic_controller::test_util::Result<CreatedApplication<Self::Service>> {
        self.prepare(context).map_err(|source| {
            TestError::with_source(
                ErrorKind::Application,
                "MySQL factory preparation failed",
                source,
            )
        })
    }

    async fn shutdown_and_drain(
        &self,
        service: Arc<Self::Service>,
    ) -> kuberic_controller::test_util::Result<()> {
        service.close().await.map_err(|source| {
            TestError::with_source(
                ErrorKind::Cleanup,
                "MySQL owned service drain failed",
                source,
            )
        })?;
        let mut state = self.state.lock().unwrap();
        let member = state
            .members
            .iter_mut()
            .find(|member| Arc::ptr_eq(&member.service, &service))
            .ok_or_else(|| {
                TestError::new(ErrorKind::Cleanup, "drain service is not factory-owned")
            })?;
        member.drained = true;
        Ok(())
    }
}

fn reserve_proxy() -> Result<TcpListener> {
    loop {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        if listener.local_addr()?.port() != MYSQL_X_PORT {
            return Ok(listener);
        }
    }
}

fn native_error(error: impl std::fmt::Debug) -> anyhow::Error {
    anyhow::anyhow!("{error:?}")
}

async fn controller_probe() -> Result<()> {
    let factory = MysqlFactory::new()?;
    let mut cluster = TestCluster::builder(factory.clone())
        .name("mysql-probe")
        .image("in-process/mysql-native")
        .scenario_timeout(Duration::from_secs(60))
        .teardown_timeout(Duration::from_secs(60))
        .start()
        .await?;
    let result = drive_until_open_rejection(&mut cluster, &factory).await;
    let evidence = match result {
        Ok(evidence) => evidence,
        Err(error) => {
            let failure = cluster.finish(Err::<(), _>(error)).await;
            if let Err(error) = &failure {
                print_diagnostics(error);
            }
            failure?;
            bail!("failed probe unexpectedly finalized successfully");
        }
    };
    let error = cluster
        .finish(Err::<(), _>(TestError::new(
            ErrorKind::Application,
            EXPECTED_BOUNDARY,
        )))
        .await
        .err()
        .context("unsupported Open unexpectedly finalized successfully")?;
    print_diagnostics(&error);
    ensure!(
        error.kind() == ErrorKind::Application,
        "scenario had an unexpected or unacknowledged cleanup failure: {error}"
    );
    let diagnostics = error
        .diagnostics()
        .context("missing retained diagnostics")?;
    assert_open_boundary(diagnostics)?;
    let retained = diagnostics["retainedStorage"]
        .as_str()
        .context("missing retained storage path")?;
    ensure!(
        Path::new(retained).is_dir(),
        "diagnostics were not retained"
    );
    serde_json::to_writer_pretty(
        std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(Path::new(retained).join("probe-diagnostics.json"))?,
        diagnostics,
    )?;
    ensure!(
        factory.members()?.iter().all(|member| member.drained),
        "factory cleanup was not acknowledged for every service"
    );
    for member in evidence {
        for address in [member.sql, member.group, member.proxy] {
            assert_tcp_refuses(address).map_err(anyhow::Error::msg)?;
        }
        ensure!(
            std::fs::read_dir(&member.root)?
                .next()
                .transpose()?
                .is_none(),
            "rejected Open created native state"
        );
        assert_no_application_store(&member.root).map_err(anyhow::Error::msg)?;
        factory.shutdown_and_drain(member.service).await?;
    }
    Ok(())
}

async fn drive_until_open_rejection(
    cluster: &mut TestCluster<MysqlFactory>,
    factory: &MysqlFactory,
) -> Result<Vec<NativeMember>> {
    loop {
        let before = cluster.observe().await?;
        let pending = before
            .set()
            .status
            .as_ref()
            .and_then(|status| status.public_v1_transition.as_ref())
            .map(|transition| transition.transition().map_err(anyhow::Error::msg))
            .transpose()?;
        cluster.step().await?;
        let after = cluster.observe().await?;
        if let Some(pending) = pending
            && pending.intent.call_kind == PublicV1CallKind::Open
            && after.reconcile_errors() > before.reconcile_errors()
        {
            let status = after
                .set()
                .status
                .as_ref()
                .context("missing controller status")?;
            let current = status
                .public_v1_transition
                .as_ref()
                .context("failed Open lost its accepted transition")?;
            ensure!(current.operation_id == pending.intent.operation_id);
            ensure!(pending.evidence.completed_operations.is_empty());
            let command = status
                .public_v1_operation
                .as_ref()
                .context("missing controller Open command")?
                .command()
                .map_err(anyhow::Error::msg)?;
            ensure!(command.intent.operation_id == pending.intent.operation_id);
            ensure!(
                matches!(
                    &command.intent.program,
                    Some(PublicOperationProgram::Open { replica, existing: true })
                        if replica == &command.target
                ),
                "controller did not request Existing for the fresh member"
            );
            ensure!(
                after.failures().is_empty(),
                "host observations were unavailable"
            );
            ensure!(after.reports().count() == 3, "missing exact host reports");
            for (_, report) in after.reports() {
                report.validate().map_err(anyhow::Error::msg)?;
                ensure!(
                    !report.completed_operations.iter().any(|completion| {
                        completion.operation_id == pending.intent.operation_id
                    }),
                    "rejected Open received completion credit"
                );
                ensure!(
                    report.replication_address.is_none(),
                    "a member unexpectedly opened"
                );
                ensure!(
                    report.role == ReplicaRole::None
                        && report.read_status != AccessStatus::Granted
                        && report.write_status != AccessStatus::Granted,
                    "failed Open published role or application access"
                );
            }
            eprintln!(
                "controller Open {} rejected before native startup",
                current.operation_id
            );
            let members = factory.members()?;
            for member in &members {
                ensure!(
                    std::fs::read_dir(&member.root)?
                        .next()
                        .transpose()?
                        .is_none(),
                    "failed Open had native effects"
                );
            }
            return Ok(members);
        }
    }
}

fn print_diagnostics(error: &TestError) {
    eprintln!("MySQL harness result: {error}");
    if let Some(diagnostics) = error.diagnostics() {
        eprintln!("redacted harness diagnostics: {diagnostics}");
    }
}

fn assert_open_boundary(diagnostics: &Value) -> Result<()> {
    ensure!(
        diagnostics["lastReconcileError"]
            .as_str()
            .is_some_and(|error| error.contains(OPEN_MISMATCH)),
        "controller failed for a different reason"
    );
    ensure!(
        diagnostics["pendingTaskOwners"].as_u64() == Some(0),
        "native/host cleanup still has pending owners"
    );
    Ok(())
}

#[test]
fn unrelated_or_missing_diagnostics_do_not_satisfy_open_boundary() {
    for diagnostics in [
        serde_json::json!({}),
        serde_json::json!({"lastReconcileError":"deadline elapsed","pendingTaskOwners":0}),
        serde_json::json!({"lastReconcileError":"application factory failed","pendingTaskOwners":0}),
        serde_json::json!({"lastReconcileError":"runtime is not open","pendingTaskOwners":0}),
        serde_json::json!({"lastReconcileError":OPEN_MISMATCH,"pendingTaskOwners":1}),
    ] {
        assert!(assert_open_boundary(&diagnostics).is_err());
    }
    assert!(
        assert_open_boundary(&serde_json::json!({
            "lastReconcileError":OPEN_MISMATCH,
            "pendingTaskOwners":0
        }))
        .is_ok()
    );
}
