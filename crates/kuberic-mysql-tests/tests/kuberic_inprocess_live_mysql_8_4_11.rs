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
use kuberic_controller::crd::{KubericSetStatus, PublicV1OperationStatus};
use kuberic_controller::test_util::{
    ApplicationFactory, CreatedApplication, ErrorKind, ObservationFailureKind, ReplicaContext,
    TestCluster, TestError,
};
use kuberic_mysql::core::{AttemptId, ConfiguredGtidLineage, CredentialGeneration};
use kuberic_mysql::service::{
    ApplicationCredential, AttemptContext, ControlCredential, ControlCredentialRole,
    MysqlFreshMemberCredentials, MysqlInstanceConfig, MysqlMemberConfig, MysqlMemberIndex,
    MysqlOperationTimeouts, MysqlPublicV1Service, MysqlPublicV1ServiceConfig, MysqlTopologyConfig,
};
use kuberic_runtime::StatefulServiceReplica;
use kuberic_runtime::application::OpenMode;
use kuberic_runtime::host::ApplicationStorageState;
use kuberic_runtime::protocol::command::PublicV1OperationCommand;
use kuberic_runtime::protocol::public_operations::{
    PublicOperationClass, PublicOperationProgram, PublicV1AcceptedOperationCompletion,
    PublicV1AgentReport, PublicV1CallKind, PublicV1OperationResult,
};
use kuberic_runtime::protocol::types::{AccessStatus, Epoch, ReplicaRole};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder};

use live_support::{
    APPARMOR_EXEC, MYSQL_X_PORT, MYSQLD, ReservedPorts, assert_no_application_store,
    assert_process_absent, assert_tcp_refuses, preflight_oracle_mysql_8_4_11,
};

const GROUP_UUID: &str = "03e84887-4d65-42d0-b941-0f691cb0aa16";
const CHILD_MODE: &str = "KUBERIC_MYSQL_INPROCESS_PROBE_CHILD";
const ROLE_PROGRESS_ERROR: &str =
    "Primary committed progress requires an applicable configuration proof";

#[test]
fn controller_fresh_new_open_starts_and_contains_native_members() -> Result<()> {
    run_native_probe(
        "controller_fresh_new_open_starts_and_contains_native_members",
        false,
    )
}

#[test]
fn controller_fresh_primary_role_requires_configuration_progress_proof() -> Result<()> {
    run_native_probe(
        "controller_fresh_primary_role_requires_configuration_progress_proof",
        true,
    )
}

#[test]
fn formation_boundary_rejects_unrelated_errors() {
    for message in [
        "agent report: Unavailable(\"Execute timed out\")",
        "agent report: Unavailable(\"runtime is closed\")",
        r#"agent report: Unavailable("code: 'The system is not in a state required for the operation's execution', message: \"invalid replication evidence: Primary committed progress requires an applicable configuration proof\"")"#,
        ROLE_PROGRESS_ERROR,
        "Primary committed progress requires a different proof",
    ] {
        let error = TestError::with_source(
            ErrorKind::Fatal,
            "harness operation failed",
            anyhow::anyhow!("{message}"),
        );
        assert!(assert_role_progress_rejection(&error).is_err());
    }
    for kind in [
        ErrorKind::Deadline,
        ErrorKind::Cleanup,
        ErrorKind::Application,
    ] {
        assert!(
            assert_role_progress_rejection(&TestError::new(kind, ROLE_PROGRESS_ERROR)).is_err()
        );
    }
}

fn run_native_probe(native_test: &str, formation: bool) -> Result<()> {
    if std::env::var_os(CHILD_MODE).is_some() {
        preflight_oracle_mysql_8_4_11().map_err(anyhow::Error::msg)?;
        return tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_stack_size(8 * 1024 * 1024)
            .enable_all()
            .build()?
            .block_on(controller_probe(formation));
    }

    // Only the child harness storage uses this short path, not Cargo compilation.
    // Three random characters keep its UUID/PVC/application socket below 108 bytes.
    let root = tempfile::Builder::new()
        .prefix("")
        .rand_bytes(3)
        .tempdir_in("/tmp")?;
    eprintln!("MySQL in-process probe storage: {}", root.path().display());
    let output = Command::new(std::env::current_exe()?)
        .args(["--exact", native_test, "--nocapture"])
        .env(CHILD_MODE, "1")
        .env("CARGO_TARGET_DIR", root.path())
        .env_remove("KUBERIC_WORKSPACE_TESTS")
        .output();
    let output = match output {
        Ok(output) => output,
        Err(error) => {
            let retained = root.keep();
            return Err(anyhow::Error::new(error).context(format!(
                "probe child I/O failed; retained storage: {}",
                retained.display()
            )));
        }
    };
    eprintln!("{}", String::from_utf8_lossy(&output.stdout));
    eprintln!("{}", String::from_utf8_lossy(&output.stderr));
    if !output.status.success() {
        bail!(
            "native probe child failed; retained storage: {}",
            root.keep().display()
        );
    }
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
    pid_path: PathBuf,
    sql: SocketAddr,
    group: SocketAddr,
    proxy: SocketAddr,
    drained: bool,
}

struct NativeEvidence {
    member: NativeMember,
    pid: u32,
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
        let pid_path = instance.runtime().pid().to_owned();
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
            pid_path,
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

    fn native_evidence(&self) -> Result<Vec<NativeEvidence>> {
        self.members()?
            .into_iter()
            .map(|member| {
                let pid = std::fs::read_to_string(&member.pid_path)?.trim().parse()?;
                ensure!(
                    Path::new(&format!("/proc/{pid}")).exists(),
                    "native child is absent"
                );
                ensure!(
                    member.root.join("d/auto.cnf").is_file(),
                    "native identity is absent"
                );
                assert_no_application_store(&member.root.join("d")).map_err(anyhow::Error::msg)?;
                Ok(NativeEvidence { member, pid })
            })
            .collect()
    }

    async fn assert_native_unformed(&self) -> Result<()> {
        for member in self.members()? {
            assert_tcp_refuses(member.proxy).map_err(anyhow::Error::msg)?;
            let mut connection = Conn::new(
                OptsBuilder::default()
                    .ip_or_hostname("127.0.0.1")
                    .tcp_port(1)
                    .socket(Some(
                        member
                            .root
                            .join("s/mysql.sock")
                            .to_string_lossy()
                            .into_owned(),
                    ))
                    .user(Some("root")),
            )
            .await?;
            let evidence: (u8, String, u64) = connection
                .query_first(
                    "SELECT @@GLOBAL.group_replication_bootstrap_group, \
                     @@GLOBAL.gtid_executed, \
                     (SELECT COUNT(*) FROM performance_schema.replication_group_members \
                      WHERE MEMBER_STATE <> 'OFFLINE')",
                )
                .await?
                .context("missing native formation evidence")?;
            connection.disconnect().await?;
            ensure!(
                evidence == (0, String::new(), 0),
                "role-boundary member bootstrapped, joined a group, or had native transactions: {evidence:?}"
            );
        }
        Ok(())
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

async fn controller_probe(formation: bool) -> Result<()> {
    let factory = MysqlFactory::new()?;
    let mut cluster = TestCluster::builder(factory.clone())
        .name("mysql-probe")
        .image("in-process/mysql-native")
        .scenario_timeout(Duration::from_secs(480))
        .teardown_timeout(Duration::from_secs(60))
        .start()
        .await?;
    let (result, evidence) = match drive_until_fresh_open(&mut cluster, &factory).await {
        Ok(evidence) => {
            let result = if formation {
                drive_until_formation_boundary(&mut cluster, &factory).await
            } else {
                Ok(())
            };
            (result, Some(evidence))
        }
        Err(error) => (Err(error), None),
    };
    if let Err(error) = cluster.finish(result).await {
        print_diagnostics(&error);
        if let Err(retention) = retain_diagnostics(&error) {
            return Err(anyhow::Error::new(error).context(format!(
                "additionally failed to retain redacted diagnostics: {retention:#}"
            )));
        }
        return Err(error.into());
    }
    ensure!(
        factory.members()?.iter().all(|member| member.drained),
        "factory cleanup was not acknowledged for every service"
    );
    for native in evidence.context("successful finish lost native evidence")? {
        assert_process_absent(native.pid).map_err(anyhow::Error::msg)?;
        let member = native.member;
        for address in [member.sql, member.group, member.proxy] {
            assert_tcp_refuses(address).map_err(anyhow::Error::msg)?;
        }
        ensure!(
            !member.root.exists(),
            "successful harness finish retained application storage"
        );
        factory.shutdown_and_drain(member.service).await?;
    }
    Ok(())
}

async fn drive_until_formation_boundary(
    cluster: &mut TestCluster<MysqlFactory>,
    factory: &MysqlFactory,
) -> Result<()> {
    let initial = cluster.observe().await?;
    ensure!(
        initial.failures().is_empty(),
        "fresh reports became unavailable"
    );
    let initial_errors = initial.reconcile_errors();
    let fresh = initial
        .reports()
        .map(|(_, report)| report.clone())
        .collect::<Vec<_>>();
    loop {
        cluster.step().await?;
        let observation = cluster.observe().await?;
        if observation.reconcile_errors() == initial_errors && observation.failures().is_empty() {
            continue;
        }
        ensure!(
            observation.reconcile_errors() > initial_errors,
            "report failed without an observed controller rejection"
        );
        let status = observation
            .set()
            .status
            .as_ref()
            .context("missing controller status")?;
        let command = assert_pending_primary_role(status, &fresh)?;
        assert_rejects_changed_role_evidence(status, &fresh)?;
        ensure!(
            observation.credentials_consistent(),
            "controller/host credentials changed"
        );
        let [failure] = observation.failures() else {
            bail!("expected only the intended Primary report to be unavailable");
        };
        ensure!(
            failure.kind() == ObservationFailureKind::Unavailable
                && failure.instance_id() == command.target.instance_id.as_str(),
            "an unrelated replica or observation failed"
        );
        assert_role_progress_rejection(failure.error().context("missing report rejection")?)?;
        ensure!(
            observation.reports().count() == 2,
            "healthy peer reports are missing"
        );
        for (_, report) in observation.reports() {
            assert_fresh_open_report(report)?;
            ensure!(
                fresh.iter().any(|opened| {
                    opened.replica == report.replica
                        && opened.process_session_id == report.process_session_id
                        && opened.storage_incarnation == report.storage_incarnation
                        && opened.replicator_creation_id == report.replicator_creation_id
                }) && report.role == ReplicaRole::None
                    && report.read_status != AccessStatus::Granted
                    && report.write_status != AccessStatus::Granted
                    && report
                        .completed_operations
                        .iter()
                        .all(|completion| completion.call_kind == PublicV1CallKind::Open),
                "peer changed identity or received formation/admission credit"
            );
        }
        cluster.probe(factory.assert_native_unformed()).await?;
        eprintln!(
            "controller Primary role reached the exact unconfigured-progress guard; \
             only three New Opens were accepted, all native members remain unformed and closed"
        );
        return Ok(());
    }
}

fn assert_pending_primary_role(
    status: &KubericSetStatus,
    fresh: &[PublicV1AgentReport],
) -> Result<PublicV1OperationCommand> {
    ensure!(
        fresh.len() == 3
            && status.public_v1_topology.is_none()
            && status.public_v1_session_renewal.is_none()
            && status.public_v1_replacement.is_none(),
        "formation boundary has unexpected topology or recovery authority"
    );
    let authority = status
        .public_v1_authority
        .as_ref()
        .context("missing accepted authority")?
        .authority()
        .map_err(anyhow::Error::msg)?;
    let transition = status
        .public_v1_transition
        .as_ref()
        .context("missing frozen transition")?
        .transition()
        .map_err(anyhow::Error::msg)?;
    let command = status
        .public_v1_operation
        .as_ref()
        .context("missing pending operation")?
        .command()
        .map_err(anyhow::Error::msg)?;
    let program = PublicOperationProgram::Role {
        epoch: Epoch::new(0, 1),
        role: ReplicaRole::Primary,
    };
    ensure!(
        command.target.replica_id.value() == 1
            && command.intent.program.as_ref() == Some(&program)
            && command.intent.lifecycle.is_none()
            && command.intent.class == PublicOperationClass::Authority
            && command.intent.input_digest == program.digest(),
        "first rejected operation is not the exact standalone fresh Primary role"
    );
    let target = fresh
        .iter()
        .find(|report| report.replica == command.target)
        .context("pending Primary is not a fresh-open member")?;
    ensure!(
        command.intent.identity == target.identity
            && command.intent.process_session_id == target.process_session_id
            && transition.evidence == authority
            && transition.intent.operation_id == command.intent.operation_id
            && transition.intent.revision == command.intent.revision
            && transition.intent.call_kind == PublicV1CallKind::Role
            && transition.intent.source.storage_incarnation == target.storage_incarnation
            && transition.intent.process_session_id == target.process_session_id
            && transition.intent.history == target.history
            && transition.intent.input_digest == command.intent.input_digest,
        "pending Role differs from exact accepted session/storage/history authority"
    );
    ensure!(
        authority.completed_operations.len() == 3,
        "controller credited an operation beyond the three New Opens"
    );
    for report in fresh {
        assert_fresh_open_report(report)?;
        ensure!(
            report.history == authority.history
                && report.completed_operations.iter().any(|completion| {
                    completion.call_kind == PublicV1CallKind::Open
                        && completion.result == PublicV1OperationResult::Succeeded
                        && command.intent.revision > completion.revision
                        && authority.completed_operations.contains(
                            &PublicV1AcceptedOperationCompletion::from_completion(completion),
                        )
                }),
            "controller lacks exact accepted New Open evidence"
        );
    }
    Ok(command)
}

fn assert_rejects_changed_role_evidence(
    status: &KubericSetStatus,
    fresh: &[PublicV1AgentReport],
) -> Result<()> {
    let mut missing = status.clone();
    missing.public_v1_operation = None;
    ensure!(
        assert_pending_primary_role(&missing, fresh).is_err(),
        "missing Role passed"
    );
    let mut changed = status.clone();
    let mut command = assert_pending_primary_role(status, fresh)?;
    command.intent.program = Some(PublicOperationProgram::Epoch {
        epoch: Epoch::new(0, 1),
    });
    command.intent.input_digest = command.intent.program.as_ref().unwrap().digest();
    changed.public_v1_operation =
        Some(PublicV1OperationStatus::from_command(&command).map_err(anyhow::Error::msg)?);
    ensure!(
        assert_pending_primary_role(&changed, fresh).is_err(),
        "Epoch substituted for Role passed"
    );
    let mut changed = status.clone();
    changed
        .public_v1_authority
        .as_mut()
        .context("missing authority")?
        .completed_operation_count += 1;
    ensure!(
        assert_pending_primary_role(&changed, fresh).is_err(),
        "changed completion credit passed"
    );
    let mut changed = fresh.to_vec();
    changed[0].process_session_id = changed[1].process_session_id.clone();
    ensure!(
        assert_pending_primary_role(status, &changed).is_err(),
        "substituted session passed"
    );
    Ok(())
}

fn assert_role_progress_rejection(error: &TestError) -> Result<()> {
    let expected = format!(
        "agent report: Invalid({:?})",
        format!(
            "code: 'The system is not in a state required for the operation's execution', \
             message: \"invalid replication evidence: {ROLE_PROGRESS_ERROR}\""
        )
    );
    let sources = error.sources().map(ToString::to_string).collect::<Vec<_>>();
    ensure!(
        error.kind() == ErrorKind::Fatal && sources == [expected],
        "unexpected report rejection, not the exact Primary progress guard: {error}; {sources:?}"
    );
    Ok(())
}

async fn drive_until_fresh_open(
    cluster: &mut TestCluster<MysqlFactory>,
    factory: &MysqlFactory,
) -> Result<Vec<NativeEvidence>> {
    loop {
        cluster.step().await?;
        let observation = cluster.observe().await?;
        if observation.reports().count() == 3
            && observation
                .reports()
                .all(|(_, report)| report.has_current_open_endpoint())
        {
            ensure!(
                observation.failures().is_empty(),
                "host observations were unavailable"
            );
            for (_, report) in observation.reports() {
                assert_fresh_open_report(report)?;
                assert_rejects_changed_open_evidence(report)?;
                ensure!(
                    report.role == ReplicaRole::None
                        && report.read_status != AccessStatus::Granted
                        && report.write_status != AccessStatus::Granted,
                    "fresh-Open probe unexpectedly entered role or application admission"
                );
            }
            eprintln!("three controller-driven New Opens completed with exact native evidence");
            return factory.native_evidence();
        }
    }
}

fn assert_fresh_open_report(report: &PublicV1AgentReport) -> Result<()> {
    report.validate().map_err(anyhow::Error::msg)?;
    ensure!(
        report.open_mode == OpenMode::Existing,
        "initialized storage still reports New"
    );
    ensure!(
        report.has_current_open_endpoint(),
        "missing current-process Open endpoint"
    );
    let digest = PublicOperationProgram::Open {
        replica: report.replica.clone(),
        existing: false,
    }
    .digest();
    ensure!(
        report.completed_operations.iter().any(|completion| {
            completion.call_kind == PublicV1CallKind::Open
                && completion.process_session_id == report.process_session_id
                && completion.storage_incarnation == report.storage_incarnation
                && completion.history == report.history
                && completion.input_digest == digest
                && completion.result == PublicV1OperationResult::Succeeded
        }),
        "missing exact successful New Open receipt"
    );
    Ok(())
}

fn assert_rejects_changed_open_evidence(report: &PublicV1AgentReport) -> Result<()> {
    let mut missing = report.clone();
    missing.completed_operations.clear();
    ensure!(
        assert_fresh_open_report(&missing).is_err(),
        "missing Open receipt passed"
    );
    let mut uninitialized = report.clone();
    uninitialized.open_mode = OpenMode::New;
    ensure!(
        assert_fresh_open_report(&uninitialized).is_err(),
        "uninitialized mode passed"
    );
    let mut changed = report.clone();
    for completion in &mut changed.completed_operations {
        if completion.call_kind == PublicV1CallKind::Open {
            completion.input_digest = PublicOperationProgram::Open {
                replica: report.replica.clone(),
                existing: true,
            }
            .digest();
        }
    }
    ensure!(
        assert_fresh_open_report(&changed).is_err(),
        "Existing Open receipt passed as New"
    );
    Ok(())
}

fn print_diagnostics(error: &TestError) {
    eprintln!("MySQL harness result: {error}");
    if let Some(diagnostics) = error.diagnostics() {
        eprintln!("redacted harness diagnostics: {diagnostics}");
    }
}

fn retain_diagnostics(error: &TestError) -> Result<()> {
    let diagnostics = error
        .diagnostics()
        .context("missing retained diagnostics")?;
    let root = diagnostics["retainedStorage"]
        .as_str()
        .context("missing retained storage path")?;
    serde_json::to_writer_pretty(
        std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(Path::new(root).join("probe-diagnostics.json"))?,
        diagnostics,
    )?;
    Ok(())
}
