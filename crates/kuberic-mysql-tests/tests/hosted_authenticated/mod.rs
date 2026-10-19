//! Ordinary authenticated coverage sharing only native fixture infrastructure.
//! No testing-facade initialization, dispatch, report or provider inspection.

use super::*;
use std::os::unix::fs::MetadataExt;

use kuberic_mysql::core::{ProgressProjection, project_progress};
use kuberic_runtime::control::{
    normalize_public_v1_agent_status_report, normalize_uninitialized_agent_status_report, proto,
    public_v1_operation_command_to_proto,
};
use kuberic_runtime::protocol::command::PublicV1OperationCommand;
use kuberic_runtime::protocol::types::ProcessSessionId;

type Client = proto::agent_control_client::AgentControlClient<tonic::transport::Channel>;

pub(super) struct WireControl {
    client: Client,
    config: ReplicaProcessConfig,
    target: ReplicaIdentity,
    pub(super) session: ProcessSessionId,
    initialization_session: ProcessSessionId,
    status: proto::GetAgentStatusRequest,
}

fn authenticated<T>(body: T, token: &str) -> tonic::Request<T> {
    let mut request = tonic::Request::new(body);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}")
            .parse()
            .expect("disposable bearer"),
    );
    request.set_timeout(Duration::from_secs(240));
    request
}

impl WireControl {
    pub(super) async fn start(
        host: ReplicaHost<MysqlPublicV1Service, SingleResolver>,
        listener: tokio::net::TcpListener,
        config: ReplicaProcessConfig,
        target: &ReplicaIdentity,
        configuration: &ConfigurationDescriptor,
        initialization_id: String,
        root: &Path,
    ) -> TestResult<(RunningReplica, Self)> {
        let endpoint = format!("http://{}", config.replication_address);
        let status = proto::GetAgentStatusRequest {
            protocol_version: 11,
            resource_uid: config.resource_uid.to_string(),
            replica_id: target.replica_id.value(),
            expected_instance_id: target.instance_id.to_string(),
        };
        let (shutdown, receiver) = tokio::sync::watch::channel(false);
        let mut startup = tokio::spawn(host.start_with_shutdown(listener, receiver));
        let setup = tokio::time::timeout(Duration::from_secs(15), async {
            let mut client = Client::connect(endpoint.clone()).await.map_err(failure)?;
            let uninitialized = client
                .get_status(authenticated(status.clone(), &config.bearer_token))
                .await
                .map_err(failure)?
                .into_inner();
            require(
                uninitialized.storage_state == proto::AgentStorageState::Uninitialized as i32,
                "ordinary host was not genuinely uninitialized",
            )?;
            native_absent(root)?;
            let response = client
                .execute(authenticated(
                    proto::ExecuteCommandRequest {
                        protocol_version: 11,
                        resource_uid: config.resource_uid.to_string(),
                        target: Some(target.clone().into()),
                        expected_process_session_id: uninitialized.process_session_id.clone(),
                        command: Some(
                            proto::execute_command_request::Command::InitializeAgentStore(
                                proto::InitializeAgentStoreCommand {
                                    initialization_id,
                                    resource_uid: config.resource_uid.to_string(),
                                    local_replica_id: target.replica_id.value(),
                                    expected_instance_id: target.instance_id.to_string(),
                                    expected_pod_uid: config.pod_uid.to_string(),
                                    expected_pvc_uid: config.pvc_uid.to_string(),
                                    assigned_agent_generation: target.agent_generation.to_string(),
                                    effective_policy: Some(proto::EffectivePolicy {
                                        replica_set_size: 1,
                                        write_quorum: 1,
                                        read_quorum: 1,
                                        failover_delay_seconds: 30,
                                    }),
                                    bootstrap_configuration: Some(configuration.clone().into()),
                                    provisioning: None,
                                },
                            ),
                        ),
                    },
                    &config.bearer_token,
                ))
                .await
                .map_err(failure)?
                .into_inner();
            normalize_uninitialized_agent_status_report(
                response
                    .observation
                    .ok_or("missing initialization observation")?,
            )
            .map_err(failure)?;
            native_absent(root)?;
            Ok::<_, String>(ProcessSessionId::new(uninitialized.process_session_id))
        })
        .await
        .map_err(failure)
        .and_then(|result| result);
        let old_session = match setup {
            Ok(session) => session,
            Err(primary) => {
                shutdown.send_replace(true);
                let cleanup = settle_startup(&mut startup).await;
                return Err(format!(
                    "ordinary initialization: {primary}; startup cleanup: {cleanup:?}"
                ));
            }
        };
        let readiness = match tokio::time::timeout(Duration::from_secs(15), &mut startup).await {
            Ok(result) => result
                .map_err(failure)
                .and_then(|result| result.map_err(failure)),
            Err(primary) => {
                shutdown.send_replace(true);
                // A timed-out future remains alive until cooperative settlement.
                let cleanup = settle_startup(&mut startup).await;
                return Err(format!(
                    "ordinary readiness: {primary}; startup cleanup: {cleanup:?}"
                ));
            }
        };
        let mut running = readiness?.ok_or("ordinary startup cancelled before readiness")?;
        let connect = async {
            let client = Client::connect(endpoint).await.map_err(failure)?;
            let wire = Self {
                client,
                config,
                target: target.clone(),
                // Replaced only by the genuinely initialized report below.
                session: old_session.clone(),
                initialization_session: old_session,
                status,
            };
            let report = wire.report().await.map_err(failure)?;
            require(
                report.replica == *target && report.identity == PublicV1Identity::new(1),
                "initialized ordinary identity differs",
            )?;
            require(
                report.process_session_id != wire.initialization_session,
                "ordinary initialization reused its old process session",
            )?;
            native_absent(root)?;
            eprintln!(
                "AUTHENTICATED_INITIALIZE=ok old_session={} current_session={} history={:?} storage={:?}",
                wire.initialization_session,
                report.process_session_id,
                report.history,
                report.storage_incarnation,
            );
            Ok::<_, String>(Self {
                session: report.process_session_id,
                ..wire
            })
        };
        match tokio::time::timeout(Duration::from_secs(15), connect)
            .await
            .map_err(failure)
            .and_then(|result| result)
        {
            Ok(wire) => Ok((running, wire)),
            Err(primary) => {
                running.shutdown();
                let cleanup = tokio::time::timeout(TERMINAL_BUDGET, running.wait()).await;
                Err(format!(
                    "ordinary reconnect: {primary}; host cleanup: {cleanup:?}"
                ))
            }
        }
    }

    fn request(
        &self,
        target: ReplicaIdentity,
        intent: PublicOperationIntent,
    ) -> TestResult<proto::ExecuteCommandRequest> {
        let session = intent.process_session_id.to_string();
        let command = PublicV1OperationCommand {
            target: target.clone(),
            intent,
        };
        Ok(proto::ExecuteCommandRequest {
            protocol_version: 11,
            resource_uid: self.config.resource_uid.to_string(),
            target: Some(target.into()),
            expected_process_session_id: session,
            command: Some(proto::execute_command_request::Command::PublicV1Operation(
                public_v1_operation_command_to_proto(&command).map_err(failure)?,
            )),
        })
    }

    async fn send(
        &self,
        request: proto::ExecuteCommandRequest,
    ) -> Result<PublicV1AgentReport, tonic::Status> {
        let response = self
            .client
            .clone()
            .execute(authenticated(request, &self.config.bearer_token))
            .await?
            .into_inner();
        normalize_public_v1_agent_status_report(
            response
                .observation
                .ok_or_else(|| tonic::Status::internal("missing observation"))?,
        )
        .map_err(|error| tonic::Status::internal(error.to_string()))
    }

    pub(super) async fn execute(
        &self,
        intent: PublicOperationIntent,
    ) -> Result<PublicV1AgentReport, tonic::Status> {
        let request = self
            .request(self.target.clone(), intent)
            .map_err(tonic::Status::invalid_argument)?;
        self.send(request).await
    }

    pub(super) async fn report(&self) -> Result<PublicV1AgentReport, tonic::Status> {
        let response = self
            .client
            .clone()
            .get_status(authenticated(
                self.status.clone(),
                &self.config.bearer_token,
            ))
            .await?
            .into_inner();
        normalize_public_v1_agent_status_report(response)
            .map_err(|error| tonic::Status::internal(error.to_string()))
    }
}

async fn settle_startup(
    startup: &mut tokio::task::JoinHandle<kuberic_runtime::host::Result<Option<RunningReplica>>>,
) -> TestResult {
    match tokio::time::timeout(TERMINAL_BUDGET, &mut *startup).await {
        Ok(Ok(Ok(Some(mut running)))) => {
            running.shutdown();
            tokio::time::timeout(TERMINAL_BUDGET, running.wait())
                .await
                .map_err(failure)?
                .map_err(failure)
        }
        Ok(Ok(Ok(None))) => Ok(()),
        Ok(Ok(Err(error))) => Err(failure(error)),
        Ok(Err(error)) => Err(failure(error)),
        Err(error) => {
            // Last-resort failure containment, never accepted as settled cleanup.
            startup.abort();
            let joined = startup
                .await
                .map(|result| result.map(|running| running.is_some()));
            Err(format!(
                "startup failed to settle: {error}; emergency join: {joined:?}"
            ))
        }
    }
}

fn native_absent(root: &Path) -> TestResult {
    require(
        !root.join("d").exists() && !root.join("s").exists(),
        "generic initialization or unauthorized Open created native roots",
    )
}

#[test]
fn ordinary_authenticated_close_after_granted_proxy_dml() {
    run_native(ordinary_terminal(false));
}

#[test]
fn ordinary_authenticated_abort_after_granted_proxy_dml() {
    run_native(ordinary_terminal(true));
}

async fn ordinary_terminal(abort: bool) -> TestResult {
    let mut fixture =
        HostedFixture::new_with_transport(if abort { "auth-abort" } else { "auth-close" }, true)
            .await?;
    let mut foreign = ForeignChild::new()?;
    let scenario = async {
        fixture.reject_unauthorized_open().await?;
        fixture.fresh_primary().await?;
        fixture.verify_independent_progress().await?;
        fixture.reject_stale_requests().await?;
        let identity = std::fs::read(fixture.root.path().join("d/auto.cnf")).map_err(failure)?;
        let terminal = fixture.intent(if abort {
            PublicOperationProgram::Abort
        } else {
            PublicOperationProgram::Close
        });
        let started = Instant::now();
        let error = rpc_error(
            tokio::time::timeout(
                TERMINAL_BUDGET,
                fixture.wire.as_ref().unwrap().execute(terminal),
            )
            .await
            .map_err(failure)?,
            "ordinary terminal RPC unexpectedly acknowledged success",
        )?;
        assert_closed(&error)?;
        let status_error = rpc_error(
            fixture.wire.as_ref().unwrap().report().await,
            "closed member supplied live ordinary status",
        )?;
        assert_closed(&status_error)?;
        // All proof precedes emergency service abort, host shutdown and Drop.
        fixture.assert_contained_evidence()?;
        require(
            std::fs::read(fixture.root.path().join("d/auto.cnf")).map_err(failure)? == identity,
            "terminal native identity changed",
        )?;
        require(
            fixture
                .root
                .path()
                .join("d/kuberic_proxy_qualification/hosted.ibd")
                .is_file(),
            "terminal lost committed native table",
        )?;
        foreign.assert_live()?;
        eprintln!(
            "AUTHENTICATED_TERMINAL={} error={:?}:{} status={:?}:{} elapsed={:?}; \
             PRE_FALLBACK_CONTAINMENT=ok pid={:?} absent, listeners closed, \
             identity/data/logs retained, foreign_pid={} live; root={}",
            if abort { "Abort" } else { "Close" },
            error.code(),
            error.message(),
            status_error.code(),
            status_error.message(),
            started.elapsed(),
            fixture.pid,
            foreign.0.id(),
            fixture.root.path().display(),
        );
        Ok(())
    }
    .await;
    let cleanup = fixture.shutdown().await;
    let unaffected = foreign.assert_live();
    eprintln!("ORDINARY_RESULTS scenario={scenario:?} cleanup={cleanup:?} foreign={unaffected:?}");
    finish_scenario(scenario, cleanup, unaffected)
}

fn assert_closed(error: &tonic::Status) -> TestResult {
    require(
        error.code() == tonic::Code::Unavailable && error.message() == "runtime is closed",
        &format!("unexpected terminal/status failure: {error}"),
    )
}

fn rpc_error<T>(result: Result<T, tonic::Status>, context: &str) -> TestResult<tonic::Status> {
    match result {
        Ok(_) => Err(context.to_owned()),
        Err(error) => Ok(error),
    }
}

#[derive(Debug, PartialEq, Eq)]
struct NativeSnapshot {
    pid: u32,
    process_start: String,
    paths: Vec<(u64, u64)>,
    auto_cnf: Vec<u8>,
    uuid: String,
    executed: GtidSet,
    purged: GtidSet,
    group: String,
    group_address: String,
    read_only: u8,
    super_read_only: u8,
    members: Vec<(String, String, u16, String, String)>,
    view: Vec<(String, String)>,
    rows: Vec<(u64, u64)>,
}

impl HostedFixture {
    async fn reject_unauthorized_open(&mut self) -> TestResult {
        let intent = self.intent(PublicOperationProgram::Open {
            replica: self.identity.clone(),
            existing: false,
        });
        let wire = self.wire.as_ref().ok_or("ordinary transport missing")?;
        let request = wire.request(self.identity.clone(), intent)?;
        for token in [None, Some("incorrect-disposable-bearer")] {
            let request = match token {
                None => tonic::Request::new(request.clone()),
                Some(token) => authenticated(request.clone(), token),
            };
            let error = rpc_error(
                wire.client.clone().execute(request).await,
                "unauthorized ordinary Open admitted",
            )?;
            require(
                error.code() == tonic::Code::Unauthenticated,
                "unexpected unauthorized ordinary Open result",
            )?;
            native_absent(self.root.path())?;
            eprintln!("UNAUTHORIZED_OPEN={:?}; native roots absent", error.code());
        }
        Ok(())
    }

    async fn native_snapshot(&self) -> TestResult<NativeSnapshot> {
        let pid = self.pid.ok_or("no observed PID")?;
        require(
            std::fs::read_link(format!("/proc/{pid}/exe")).map_err(failure)? == Path::new(MYSQLD),
            "native process executable changed",
        )?;
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).map_err(failure)?;
        let process_start = stat
            .rsplit_once(") ")
            .ok_or("invalid process stat")?
            .1
            .split_whitespace()
            .nth(19)
            .ok_or("missing process start time")?
            .to_owned();
        let mut paths = Vec::new();
        for path in ["d", "d/auto.cnf", "s", "s/mysqld.pid", "s/mysql.sock"] {
            let metadata = std::fs::metadata(self.root.path().join(path)).map_err(failure)?;
            paths.push((metadata.dev(), metadata.ino()));
        }
        let mut connection = self.root_connection().await?;
        let (uuid, executed, purged, group, group_address, read_only, super_read_only): (
            String,
            String,
            String,
            String,
            String,
            u8,
            u8,
        ) = connection
            .query_first(
                "SELECT @@GLOBAL.server_uuid, @@GLOBAL.gtid_executed, @@GLOBAL.gtid_purged, \
                 @@GLOBAL.group_replication_group_name, @@GLOBAL.group_replication_local_address, \
                 @@GLOBAL.read_only, @@GLOBAL.super_read_only",
            )
            .await
            .map_err(failure)?
            .ok_or("missing native state")?;
        let members = connection
            .query(
                "SELECT MEMBER_ID, MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE \
             FROM performance_schema.replication_group_members ORDER BY MEMBER_ID",
            )
            .await
            .map_err(failure)?;
        let view = connection
            .query(
                "SELECT MEMBER_ID, VIEW_ID FROM performance_schema.replication_group_member_stats \
             WHERE MEMBER_ID = @@GLOBAL.server_uuid",
            )
            .await
            .map_err(failure)?;
        let rows = connection
            .query("SELECT id, value FROM kuberic_proxy_qualification.hosted ORDER BY id")
            .await
            .map_err(failure)?;
        connection.disconnect().await.map_err(failure)?;
        Ok(NativeSnapshot {
            pid,
            process_start,
            paths,
            auto_cnf: std::fs::read(self.root.path().join("d/auto.cnf")).map_err(failure)?,
            uuid,
            executed: executed.parse().map_err(failure)?,
            purged: purged.parse().map_err(failure)?,
            group,
            group_address,
            read_only,
            super_read_only,
            members,
            view,
            rows,
        })
    }

    async fn verify_independent_progress(&mut self) -> TestResult {
        let before = self.native_snapshot().await?;
        self.execute(PublicOperationProgram::Progress { capability: false })
            .await?;
        let report = self.report().await?;
        let after = self.native_snapshot().await?;
        require(
            before == after,
            "native boundary changed while sampling independent progress",
        )?;
        let ProgressProjection::Projectable(projected) = project_progress(
            &ConfiguredGtidLineage::new(GROUP_UUID).map_err(failure)?,
            &before.executed,
        ) else {
            return Err("independent complete GTIDs are not projectable".into());
        };
        require(
            report.progress.first == projected.value()
                && report.progress.last == projected.value()
                && projected.value() > 0,
            "applied/committed report differs from independent complete native GTIDs",
        )?;
        require(before.rows == vec![(1, 42)], "committed native row differs")?;
        eprintln!(
            "INDEPENDENT_PROGRESS=ok uuid={} full_executed={:?} full_purged={:?} \
             applied={} committed={} projection={} rows={:?}",
            before.uuid,
            before.executed,
            before.purged,
            report.progress.last,
            report.progress.first,
            projected.value(),
            before.rows,
        );
        Ok(())
    }

    async fn reject_stale_requests(&mut self) -> TestResult {
        for kind in ["session", "target", "revision"] {
            let before = self.native_snapshot().await?;
            let report_before = self.report().await?;
            let mut intent = self.intent(PublicOperationProgram::Progress { capability: false });
            let rejected_id = intent.operation_id.clone();
            let wire = self.wire.as_ref().ok_or("ordinary transport missing")?;
            let mut target = self.identity.clone();
            let expected = match kind {
                "session" => {
                    intent.process_session_id = wire.initialization_session.clone();
                    "command targets a stale agent process session"
                }
                "target" => {
                    target.instance_id = ReplicaInstanceId::new("noncurrent-native-instance");
                    "command targets another durable replica identity"
                }
                _ => {
                    // New ID, valid digest/class/session, but older positive authority.
                    intent.revision = 1;
                    "authority revision does not exceed retained authority"
                }
            };
            let request = wire.request(target, intent)?;
            let error = rpc_error(wire.send(request).await, "stale ordinary request admitted")?;
            require(
                error.code() == tonic::Code::FailedPrecondition
                    && error.message().contains(expected),
                &format!("unexpected stale {kind} rejection: {error}"),
            )?;
            let report_after = self.report().await?;
            let after = self.native_snapshot().await?;
            require(
                before == after,
                &format!("stale {kind} request mutated native state"),
            )?;
            same_public_boundary(&report_before, &report_after)?;
            require(
                !report_after
                    .completed_operations
                    .iter()
                    .any(|op| op.operation_id == rejected_id),
                "stale request became a completed public operation",
            )?;
            let mut proxy = self
                .application_connection(self.proxy)
                .await
                .map_err(failure)?;
            let value: Option<u64> = proxy
                .query_first("SELECT value FROM kuberic_proxy_qualification.hosted WHERE id = 1")
                .await
                .map_err(failure)?;
            proxy.disconnect().await.map_err(failure)?;
            require(
                value == Some(42),
                "proxy unavailable or row changed after stale rejection",
            )?;
            self.execute(PublicOperationProgram::Progress { capability: false })
                .await?;
            eprintln!(
                "STALE_{kind}=FailedPrecondition; full native/public boundary unchanged; \
                       current Progress and proxy SELECT succeed"
            );
        }
        self.verify_independent_progress().await
    }
}

fn same_public_boundary(before: &PublicV1AgentReport, after: &PublicV1AgentReport) -> TestResult {
    require(
        before.identity == after.identity
            && before.replica == after.replica
            && before.process_session_id == after.process_session_id
            && before.replicator_creation_id == after.replicator_creation_id
            && before.history == after.history
            && before.storage_incarnation == after.storage_incarnation
            && before.role == after.role
            && before.progress == after.progress
            && before.read_status == AccessStatus::Granted
            && after.read_status == AccessStatus::Granted
            && before.write_status == AccessStatus::Granted
            && after.write_status == AccessStatus::Granted,
        "rejected request changed public identity/ownership/progress/access boundary",
    )
}

#[test]
fn retained_root_collision_never_deletes_existing_evidence() {
    let root = FixtureRoot::new_retained("auth-collide").unwrap();
    let evidence = root.path().join("evidence");
    std::fs::write(&evidence, b"retained").unwrap();
    assert!(FixtureRoot::new_retained("auth-collide").is_err());
    assert_eq!(std::fs::read(evidence).unwrap(), b"retained");
}

#[test]
fn unexpected_rpc_success_preserves_cleanup_and_noninterference_failures() {
    let primary = rpc_error(Ok(()), "unexpected RPC success").map(|_| ());
    let result = finish_scenario(
        primary,
        Err("cleanup failure".into()),
        Err("foreign child failure".into()),
    );
    assert_eq!(
        result.unwrap_err(),
        "scenario: unexpected RPC success; shutdown/containment: cleanup failure; foreign child: foreign child failure"
    );
    let error = rpc_error::<()>(
        Err(tonic::Status::unavailable("runtime is closed")),
        "success",
    )
    .unwrap();
    assert_closed(&error).unwrap();
}
