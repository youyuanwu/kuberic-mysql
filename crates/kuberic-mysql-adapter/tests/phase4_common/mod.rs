#![allow(dead_code)]

use core::future::Future;
use core::pin::Pin;
use std::collections::VecDeque;
use std::fs;
use std::io::ErrorKind;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kuberic_mysql_core::{
    AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, EndpointBinding, Epoch,
    ExactBinding, ExactBindingParts, GroupName, MemberAddress, MemberId, ObservationInstant,
    ObservationProvenance, ObservationSessionId, PartitionId, ProcessSessionId, ReplicaId,
    ReplicaIncarnation, ResourceId, ServerUuid, StorageBinding, ViewId,
};

use crate::observer::{SessionConnector, SessionFuture};
use crate::query::{QueryId, RawResult, RawValue};
use crate::request::{ObservationRequest, ObserverCredentials, SocketPathError, UnixSocketPath};
use crate::session::{NativeSession, SessionError};
use crate::time::{ClockContext, ClockError, ObservationClock};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct ScriptClock {
    tick: Arc<AtomicU64>,
    runtime_origin: Instant,
}

impl ScriptClock {
    pub fn new(tick: u64) -> Self {
        Self {
            tick: Arc::new(AtomicU64::new(tick)),
            runtime_origin: Instant::now(),
        }
    }

    pub fn set(&self, tick: u64) {
        self.tick.store(tick, Ordering::Release);
    }
}

impl ObservationClock for ScriptClock {
    fn now(&self) -> ObservationInstant {
        ObservationInstant::new(self.tick.load(Ordering::Acquire))
    }

    fn to_std_instant(&self, instant: ObservationInstant) -> Result<Instant, ClockError> {
        self.runtime_origin
            .checked_add(Duration::from_millis(instant.tick()))
            .ok_or(ClockError::UnrepresentableInstant)
    }
}

pub struct TestSocket {
    path: PathBuf,
    listener: Option<UnixListener>,
    directory: PathBuf,
}

impl TestSocket {
    pub fn new(label: &str) -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let short_label = &label[..label.len().min(18)];
        let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "p4-{short_label}-{}-{sequence}",
                std::process::id()
            ));
        fs::create_dir_all(&directory).expect("create Phase 4 test directory");
        let path = directory.join("mysql.sock");
        let listener = UnixListener::bind(&path).expect("bind Phase 4 request socket");
        Self {
            path,
            listener: Some(listener),
            directory,
        }
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    pub fn remove_socket(&mut self) {
        self.listener.take();
        fs::remove_file(&self.path).expect("remove Phase 4 request socket");
    }

    pub fn replace_socket(&mut self) {
        self.remove_socket();
        self.listener = Some(UnixListener::bind(&self.path).expect("replace Phase 4 socket"));
    }
}

impl Drop for TestSocket {
    fn drop(&mut self) {
        self.listener.take();
        let _ = fs::remove_file(&self.path);
        fs::remove_dir(&self.directory).expect("remove Phase 4 test directory");
    }
}

#[derive(Clone)]
pub enum ScriptAction {
    Result(RawResult),
    DelayedResult {
        result: RawResult,
        delay: Duration,
        advance_to: u64,
    },
    LateResult {
        result: RawResult,
        delay: Duration,
        advance_to: u64,
    },
    Error(SessionErrorSpec),
    ErrorAt {
        error: SessionErrorSpec,
        advance_to: u64,
    },
    Pending {
        advance_to: u64,
    },
}

#[derive(Clone, Copy)]
pub enum SessionErrorSpec {
    Transport,
    Authentication,
    Permission,
    Unsupported,
}

pub struct ScriptStep {
    pub query: QueryId,
    pub action: ScriptAction,
}

#[derive(Clone, Default)]
pub struct Tracking {
    pub connected: Arc<AtomicBool>,
    pub disconnect_started: Arc<AtomicBool>,
    pub disconnected: Arc<AtomicBool>,
    pub late_completion: Arc<AtomicBool>,
    pub query_count: Arc<AtomicU64>,
}

pub struct ScriptedConnector {
    revalidation: Mutex<Result<(), SocketPathError>>,
    connect: Mutex<Option<ConnectAction>>,
    session: Mutex<Option<ScriptedSession>>,
}

pub enum ConnectAction {
    Success,
    Error(SessionErrorSpec),
    Pending { advance_to: u64 },
}

impl ScriptedConnector {
    pub fn new(clock: ScriptClock, steps: Vec<ScriptStep>, tracking: Tracking) -> Self {
        Self {
            revalidation: Mutex::new(Ok(())),
            connect: Mutex::new(Some(ConnectAction::Success)),
            session: Mutex::new(Some(ScriptedSession {
                clock,
                steps: VecDeque::from(steps),
                tracking,
                disconnect_error: None,
                disconnect_advance_to: None,
                disconnect_pending: false,
            })),
        }
    }

    pub fn with_revalidation(self, result: Result<(), SocketPathError>) -> Self {
        *self.revalidation.lock().expect("revalidation lock") = result;
        self
    }

    pub fn with_connect(self, action: ConnectAction) -> Self {
        *self.connect.lock().expect("connect lock") = Some(action);
        self
    }

    pub fn with_disconnect_error(self, error: SessionErrorSpec) -> Self {
        self.session
            .lock()
            .expect("session lock")
            .as_mut()
            .expect("scripted session")
            .disconnect_error = Some(error);
        self
    }

    pub fn with_disconnect_advance(self, tick: u64) -> Self {
        self.session
            .lock()
            .expect("session lock")
            .as_mut()
            .expect("scripted session")
            .disconnect_advance_to = Some(tick);
        self
    }

    pub fn with_pending_disconnect(self, tick: u64) -> Self {
        {
            let mut session = self.session.lock().expect("session lock");
            let session = session.as_mut().expect("scripted session");
            session.disconnect_advance_to = Some(tick);
            session.disconnect_pending = true;
        }
        self
    }
}

impl SessionConnector<ScriptClock> for ScriptedConnector {
    fn revalidate(&self, _socket: &UnixSocketPath) -> Result<(), SocketPathError> {
        *self.revalidation.lock().expect("revalidation lock")
    }

    fn connect<'a>(&'a self, _request: &'a ObservationRequest<ScriptClock>) -> SessionFuture<'a> {
        let action = self
            .connect
            .lock()
            .expect("connect lock")
            .take()
            .expect("connect exactly once");
        match action {
            ConnectAction::Success => {
                let session = self
                    .session
                    .lock()
                    .expect("session lock")
                    .take()
                    .expect("scripted session");
                session.tracking.connected.store(true, Ordering::Release);
                Box::pin(async move { Ok(Box::new(session) as Box<dyn NativeSession + Send>) })
            }
            ConnectAction::Error(error) => Box::pin(async move { Err(session_error(error, None)) }),
            ConnectAction::Pending { advance_to } => {
                let clock = self
                    .session
                    .lock()
                    .expect("session lock")
                    .as_ref()
                    .expect("scripted session")
                    .clock
                    .clone();
                Box::pin(async move {
                    clock.set(advance_to);
                    std::future::pending().await
                })
            }
        }
    }
}

pub struct ScriptedSession {
    clock: ScriptClock,
    steps: VecDeque<ScriptStep>,
    tracking: Tracking,
    disconnect_error: Option<SessionErrorSpec>,
    disconnect_advance_to: Option<u64>,
    disconnect_pending: bool,
}

impl NativeSession for ScriptedSession {
    fn query<'a>(
        &'a mut self,
        query: QueryId,
    ) -> Pin<Box<dyn Future<Output = Result<RawResult, SessionError>> + Send + 'a>> {
        let step = self.steps.pop_front().expect("scripted query step");
        assert_eq!(step.query, query, "query order");
        self.tracking.query_count.fetch_add(1, Ordering::AcqRel);
        let clock = self.clock.clone();
        let late_completion = Arc::clone(&self.tracking.late_completion);
        Box::pin(async move {
            match step.action {
                ScriptAction::Result(result) => Ok(result),
                ScriptAction::DelayedResult {
                    result,
                    delay,
                    advance_to,
                } => {
                    tokio::time::sleep(delay).await;
                    clock.set(advance_to);
                    Ok(result)
                }
                ScriptAction::LateResult {
                    result,
                    delay,
                    advance_to,
                } => {
                    tokio::time::sleep(delay).await;
                    clock.set(advance_to);
                    late_completion.store(true, Ordering::Release);
                    Ok(result)
                }
                ScriptAction::Error(error) => Err(session_error(error, Some(query))),
                ScriptAction::ErrorAt { error, advance_to } => {
                    clock.set(advance_to);
                    Err(session_error(error, Some(query)))
                }
                ScriptAction::Pending { advance_to } => {
                    clock.set(advance_to);
                    std::future::pending().await
                }
            }
        })
    }

    fn disconnect(
        self: Box<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SessionError>> + Send + 'static>> {
        Box::pin(async move {
            self.tracking
                .disconnect_started
                .store(true, Ordering::Release);
            if let Some(tick) = self.disconnect_advance_to {
                self.clock.set(tick);
            }
            if self.disconnect_pending {
                std::future::pending::<()>().await;
            }
            self.tracking.disconnected.store(true, Ordering::Release);
            match self.disconnect_error {
                Some(error) => Err(session_error(error, None)),
                None => Ok(()),
            }
        })
    }
}

pub fn request(socket: &TestSocket, clock: ScriptClock) -> ObservationRequest<ScriptClock> {
    let binding = binding();
    ObservationRequest::new(
        binding.clone(),
        UnixSocketPath::new(socket.path()).expect("validated test socket"),
        ObserverCredentials::new("observer", "scripted-secret").expect("credentials"),
        ObservationProvenance::new("phase4-script", binding.parts().attempt.clone())
            .expect("provenance"),
        ClockContext::new(clock, ObservationInstant::new(100)).expect("clock context"),
    )
    .expect("observation request")
}

pub fn binding() -> ExactBinding {
    ExactBinding::new(ExactBindingParts {
        resource: ResourceId::new("resource").unwrap(),
        partition: PartitionId::new("partition").unwrap(),
        replica: ReplicaId::new("replica").unwrap(),
        incarnation: ReplicaIncarnation::new("incarnation").unwrap(),
        process_session: ProcessSessionId::new("process").unwrap(),
        observation_session: ObservationSessionId::new("observation").unwrap(),
        attempt: AttemptId::new("attempt").unwrap(),
        endpoint: EndpointBinding::new("endpoint").unwrap(),
        storage: StorageBinding::new("storage").unwrap(),
        server_uuid: ServerUuid::new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
        group_name: GroupName::new("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
        member_id: MemberId::new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
        member_address: MemberAddress::new("mysql-a:3306").unwrap(),
        configuration: ConfigurationId::new("configuration").unwrap(),
        epoch: Epoch::new("epoch").unwrap(),
        authority_generation: AuthorityGeneration::new("authority").unwrap(),
        view_id: ViewId::new("view-0001").unwrap(),
        credential_generation: CredentialGeneration::new("credential").unwrap(),
    })
}

pub fn success_steps() -> Vec<ScriptStep> {
    vec![
        result_step(
            QueryId::Mysql8411ProductIdentityV1,
            "oracle-community-8.4.11-product",
        ),
        result_step(QueryId::Mysql8411LocalStateV1, "online-local-state"),
        result_step(QueryId::Mysql8411GroupMembersV1, "online-members"),
        result_step(QueryId::Mysql8411LocalMemberStatsV1, "online-local-view"),
        result_step(
            QueryId::Mysql8411ExecutedGtidsV1,
            "native-tagged-and-newline-gtids",
        ),
        result_step(QueryId::Mysql8411LocalStateV1, "online-local-state"),
        result_step(QueryId::Mysql8411GroupMembersV1, "online-members"),
        result_step(QueryId::Mysql8411LocalMemberStatsV1, "online-local-view"),
    ]
}

pub fn result_step(query: QueryId, fixture: &str) -> ScriptStep {
    ScriptStep {
        query,
        action: ScriptAction::Result(crate::common::fixture(fixture).raw()),
    }
}

pub fn mutate_step(steps: &mut [ScriptStep], index: usize, mutate: impl FnOnce(&mut RawResult)) {
    let ScriptAction::Result(result) = &mut steps[index].action else {
        panic!("mutation requires result step");
    };
    mutate(result);
}

pub fn failure_step(query: QueryId, error: SessionErrorSpec) -> ScriptStep {
    ScriptStep {
        query,
        action: ScriptAction::Error(error),
    }
}

pub fn pending_step(query: QueryId) -> ScriptStep {
    ScriptStep {
        query,
        action: ScriptAction::Pending { advance_to: 101 },
    }
}

pub fn session_error(spec: SessionErrorSpec, query: Option<QueryId>) -> SessionError {
    let stage = if query.is_some() {
        crate::ObservationStage::Query
    } else {
        crate::ObservationStage::Disconnect
    };
    match spec {
        SessionErrorSpec::Transport => SessionError::Transport { stage },
        SessionErrorSpec::Authentication => SessionError::Server {
            stage: crate::ObservationStage::Authenticate,
            surface: query.map(QueryId::surface),
            code: 1045,
            sql_state: crate::SqlState::new("28000").unwrap(),
        },
        SessionErrorSpec::Permission => SessionError::Server {
            stage,
            surface: query.map(QueryId::surface),
            code: 1142,
            sql_state: crate::SqlState::new("42000").unwrap(),
        },
        SessionErrorSpec::Unsupported => SessionError::Server {
            stage,
            surface: query.map(QueryId::surface),
            code: 1193,
            sql_state: crate::SqlState::new("HY000").unwrap(),
        },
    }
}

pub fn replace_query_step(steps: &mut [ScriptStep], index: usize, action: ScriptAction) {
    steps[index].action = action;
}

pub fn metadata_error(kind: ErrorKind) -> SocketPathError {
    SocketPathError::Metadata(kind)
}

pub fn bytes(value: &str) -> RawValue {
    RawValue::Bytes(value.as_bytes().to_vec())
}
