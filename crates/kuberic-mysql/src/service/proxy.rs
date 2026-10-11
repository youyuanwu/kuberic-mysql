//! Bounded generation-fenced MySQL Classic Protocol client gateway.

use core::fmt;
use std::collections::{BTreeMap, BTreeSet};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::adapter::UnixSocketPath;
use crate::core::{AttemptId, CredentialGeneration};
use crate::service::{
    AttemptContext, BackendConnectionId, MemberMutationAuthority, MysqlProxyError, MysqlProxyPhase,
    MysqlProxyProtocolError,
};

/// Exact schema receiving the Stage 2 application's minimum DML grants.
pub const APPLICATION_QUALIFICATION_SCHEMA: &str = "kuberic_proxy_qualification";

const MAX_PACKET_PAYLOAD: usize = 1024 * 1024;
const MYSQL_CONTINUATION_PAYLOAD: usize = 0x00ff_ffff;
const GREETING_AUTH_TIMEOUT: Duration = Duration::from_secs(10);
const CLIENT_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
const SERVER_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const REVOCATION_TIMEOUT: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
const CLIENT_CONNECT_ATTRS: u32 = 0x0010_0000;
const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x0020_0000;
const ALLOWED_CAPABILITIES: u32 = CLIENT_LONG_PASSWORD
    | CLIENT_LONG_FLAG
    | CLIENT_CONNECT_WITH_DB
    | CLIENT_PROTOCOL_41
    | CLIENT_TRANSACTIONS
    | CLIENT_SECURE_CONNECTION
    | CLIENT_PLUGIN_AUTH
    | CLIENT_CONNECT_ATTRS
    | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
const REQUIRED_CAPABILITIES: u32 = CLIENT_LONG_PASSWORD
    | CLIENT_PROTOCOL_41
    | CLIENT_TRANSACTIONS
    | CLIENT_SECURE_CONNECTION
    | CLIENT_PLUGIN_AUTH;
const SERVER_MORE_RESULTS_EXISTS: u16 = 0x0008;
const COLUMN_DEFINITION_FIXED_FIELDS: u64 = 0x0c;

/// Dedicated application credential whose secret is always redacted and
/// explicitly zeroed.
pub struct ApplicationCredential {
    attempt: AttemptId,
    generation: CredentialGeneration,
    username: String,
    password: Box<[u8]>,
}

impl ApplicationCredential {
    /// Creates one nonempty restricted application account credential.
    pub fn new(
        attempt: AttemptId,
        generation: CredentialGeneration,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, MysqlProxyError> {
        let username = username.into();
        let password = password.into();
        if !valid_account_component(&username, 32)
            || !valid_marker_component(attempt.as_str())
            || !valid_marker_component(generation.as_str())
            || attempt.as_str().len() + generation.as_str().len() + username.len() > 245
            || password.is_empty()
            || password.len() > 128
            || password
                .bytes()
                .any(|byte| !byte.is_ascii_graphic() || matches!(byte, b'\'' | b'"' | b'\\' | b'`'))
        {
            return Err(MysqlProxyError::InvalidConfiguration);
        }
        Ok(Self {
            attempt,
            generation,
            username,
            password: password.into_bytes().into_boxed_slice(),
        })
    }

    /// Attempt identity.
    #[must_use]
    pub const fn attempt(&self) -> &AttemptId {
        &self.attempt
    }

    /// Credential generation.
    #[must_use]
    pub const fn generation(&self) -> &CredentialGeneration {
        &self.generation
    }

    /// Non-secret exact application username.
    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Returns whether the password buffer was cleared.
    #[must_use]
    pub fn is_cleared(&self) -> bool {
        self.password.iter().all(|byte| *byte == 0)
    }

    /// Clears the retained password buffer.
    pub fn clear(&mut self) {
        self.password.fill(0);
    }

    pub(crate) fn password(&self) -> &str {
        std::str::from_utf8(&self.password).expect("application password originated as valid UTF-8")
    }
}

impl fmt::Debug for ApplicationCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationCredential")
            .field("attempt", &self.attempt)
            .field("generation", &self.generation)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

impl Drop for ApplicationCredential {
    fn drop(&mut self) {
        self.clear();
    }
}

fn valid_account_component(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn valid_marker_component(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

/// Secret-free proof that the exact application account exists, has only the
/// fixed schema DML grants, and completed one private-UDS cache-warming login.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationAccountReceipt {
    attempt: AttemptId,
    generation: CredentialGeneration,
    username: String,
    schema: &'static str,
    marker: String,
}

impl ApplicationAccountReceipt {
    /// Creates secret-free evidence from the exact prepared credential.
    #[must_use]
    pub fn new(credential: &ApplicationCredential) -> Self {
        Self {
            attempt: credential.attempt.clone(),
            generation: credential.generation.clone(),
            username: credential.username.clone(),
            schema: APPLICATION_QUALIFICATION_SCHEMA,
            marker: application_account_marker(
                credential.attempt.as_str(),
                credential.generation.as_str(),
                &credential.username,
            ),
        }
    }

    /// Attempt identity.
    #[must_use]
    pub const fn attempt(&self) -> &AttemptId {
        &self.attempt
    }

    /// Credential generation.
    #[must_use]
    pub const fn generation(&self) -> &CredentialGeneration {
        &self.generation
    }

    /// Exact non-secret application username.
    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Exact granted schema.
    #[must_use]
    pub const fn schema(&self) -> &'static str {
        self.schema
    }

    pub(crate) fn marker(&self) -> &str {
        &self.marker
    }

    pub(crate) fn matches_credential(&self, credential: &ApplicationCredential) -> bool {
        self.attempt == credential.attempt
            && self.generation == credential.generation
            && self.username == credential.username
    }
}

fn application_account_marker(attempt: &str, generation: &str, username: &str) -> String {
    format!("kuberic:{attempt}:{generation}:{username}")
}

/// Secret-free exact native session identity captured before any application
/// command can be dispatched.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendSessionIdentity {
    connection: BackendConnectionId,
    account: ApplicationAccountReceipt,
    authority: MemberMutationAuthority,
    socket: UnixSocketPath,
}

impl BackendSessionIdentity {
    pub(crate) fn new(
        connection: BackendConnectionId,
        account: ApplicationAccountReceipt,
        authority: MemberMutationAuthority,
        socket: UnixSocketPath,
    ) -> Self {
        Self {
            connection,
            account,
            authority,
            socket,
        }
    }

    /// Numeric native connection identifier.
    #[must_use]
    pub const fn connection(&self) -> BackendConnectionId {
        self.connection
    }

    /// Exact dedicated application-account generation.
    #[must_use]
    pub const fn account(&self) -> &ApplicationAccountReceipt {
        &self.account
    }

    /// Exact admitted member, process, storage, and endpoint authority.
    #[must_use]
    pub const fn authority(&self) -> &MemberMutationAuthority {
        &self.authority
    }

    /// Exact private socket object used for this backend session.
    #[must_use]
    pub const fn socket(&self) -> &UnixSocketPath {
        &self.socket
    }
}

/// Member-local private control required by proxy account preparation and
/// generation revocation.
#[allow(async_fn_in_trait)]
pub trait MysqlProxyControlBoundary {
    /// Synchronously contains the exact member if account preparation ownership
    /// is dropped before the proxy or account cleanup takes ownership.
    fn contain_proxy_preparation(
        &mut self,
        authority: &MemberMutationAuthority,
        attempt: &AttemptContext,
    ) -> Result<(), MysqlProxyError>;

    /// Returns the currently attested exact private backend UDS.
    fn proxy_backend_socket(
        &mut self,
        authority: &MemberMutationAuthority,
    ) -> Result<UnixSocketPath, MysqlProxyError>;

    /// Creates the account, grants exact DML, restores binary logging, and
    /// warms `caching_sha2_password` through one private-UDS login.
    async fn prepare_application_account(
        &mut self,
        authority: &MemberMutationAuthority,
        credential: &ApplicationCredential,
        attempt: &AttemptContext,
    ) -> Result<ApplicationAccountReceipt, MysqlProxyError>;

    /// Removes the exact account with binary logging disabled and restored.
    async fn remove_application_account(
        &mut self,
        authority: &MemberMutationAuthority,
        receipt: &ApplicationAccountReceipt,
        attempt: &AttemptContext,
    ) -> Result<(), MysqlProxyError>;

    /// Terminates and proves absence of one exact native backend connection.
    async fn terminate_proxy_backend(
        &mut self,
        authority: &MemberMutationAuthority,
        session: &BackendSessionIdentity,
        attempt: &AttemptContext,
        deadline: Instant,
    ) -> Result<(), MysqlProxyError>;
}

/// Current observable proxy counts and exact backend identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlProxySnapshot {
    accepting: bool,
    generation: Option<NonZeroU64>,
    sessions: usize,
    active_commands: usize,
    backend_connections: Vec<BackendConnectionId>,
    unidentified_backends: usize,
    diagnostics: MysqlProxyDiagnostics,
}

impl MysqlProxySnapshot {
    /// Whether the listener may admit the current generation.
    #[must_use]
    pub const fn accepting(&self) -> bool {
        self.accepting
    }

    /// Published access generation.
    #[must_use]
    pub const fn generation(&self) -> Option<NonZeroU64> {
        self.generation
    }

    /// Accepted client session count.
    #[must_use]
    pub const fn sessions(&self) -> usize {
        self.sessions
    }

    /// Currently dispatched command count.
    #[must_use]
    pub const fn active_commands(&self) -> usize {
        self.active_commands
    }

    /// Exact registered native backend identities.
    #[must_use]
    pub fn backend_connections(&self) -> &[BackendConnectionId] {
        &self.backend_connections
    }

    /// Initiated backend connections whose exact native identity was never
    /// obtained.
    #[must_use]
    pub const fn unidentified_backends(&self) -> usize {
        self.unidentified_backends
    }

    /// Bounded secret-free outcome summary for the current generation.
    #[must_use]
    pub const fn diagnostics(&self) -> MysqlProxyDiagnostics {
        self.diagnostics
    }
}

/// Bounded secret-free per-generation session and command outcome summary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MysqlProxyDiagnostics {
    auth_rejections: u64,
    pre_dispatch_rejections: u64,
    normal_completions: u64,
    cancellations: u64,
    ambiguities: u64,
    deadlines: u64,
    cleanup_failures: u64,
}

impl MysqlProxyDiagnostics {
    /// Authentication or greeting rejections before command dispatch.
    #[must_use]
    pub const fn auth_rejections(self) -> u64 {
        self.auth_rejections
    }

    /// Other rejections before command dispatch.
    #[must_use]
    pub const fn pre_dispatch_rejections(self) -> u64 {
        self.pre_dispatch_rejections
    }

    /// Commands that reached one documented normal completion.
    #[must_use]
    pub const fn normal_completions(self) -> u64 {
        self.normal_completions
    }

    /// Sessions cancelled by generation or attempt settlement.
    #[must_use]
    pub const fn cancellations(self) -> u64 {
        self.cancellations
    }

    /// Dispatched commands whose effect could not be classified normally.
    #[must_use]
    pub const fn ambiguities(self) -> u64 {
        self.ambiguities
    }

    /// Sessions that reached a bounded phase deadline.
    #[must_use]
    pub const fn deadlines(self) -> u64 {
        self.deadlines
    }

    /// Native/account cleanup failures.
    #[must_use]
    pub const fn cleanup_failures(self) -> u64 {
        self.cleanup_failures
    }
}

/// Completed generation-containment proof.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlProxyContainmentReceipt {
    generation: NonZeroU64,
    sessions_zero: bool,
    commands_zero: bool,
    native_absence: bool,
    cancelled_commands: bool,
    ambiguous_effect: bool,
    cleanup_complete: bool,
    unidentified_backends: usize,
    diagnostics: MysqlProxyDiagnostics,
}

impl MysqlProxyContainmentReceipt {
    /// Revoked generation.
    #[must_use]
    pub const fn generation(&self) -> NonZeroU64 {
        self.generation
    }

    /// Proxy accepted-session count reached zero.
    #[must_use]
    pub const fn sessions_zero(&self) -> bool {
        self.sessions_zero
    }

    /// Active command count reached zero.
    #[must_use]
    pub const fn commands_zero(&self) -> bool {
        self.commands_zero
    }

    /// Every registered native backend identity was proved absent.
    #[must_use]
    pub const fn native_absence(&self) -> bool {
        self.native_absence
    }

    /// At least one dispatched command was interrupted by revocation.
    #[must_use]
    pub const fn cancelled_commands(&self) -> bool {
        self.cancelled_commands
    }

    /// At least one dispatched command ended without a normal protocol result.
    #[must_use]
    pub const fn ambiguous_effect(&self) -> bool {
        self.ambiguous_effect
    }

    /// Listener, proxy sessions, commands, and native sessions were contained.
    #[must_use]
    pub const fn cleanup_complete(&self) -> bool {
        self.cleanup_complete
    }

    /// Initiated backend connections lacking exact native identity.
    #[must_use]
    pub const fn unidentified_backends(&self) -> usize {
        self.unidentified_backends
    }

    /// Secret-free outcome summary retained at containment.
    #[must_use]
    pub const fn diagnostics(&self) -> MysqlProxyDiagnostics {
        self.diagnostics
    }

    /// Only normal command completion plus complete containment permits handoff.
    #[must_use]
    pub const fn handoff_eligible(&self) -> bool {
        self.cleanup_complete
            && !self.cancelled_commands
            && !self.ambiguous_effect
            && self.sessions_zero
            && self.commands_zero
            && self.native_absence
            && self.unidentified_backends == 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionCommandState {
    Idle,
    Active,
}

#[derive(Clone, Debug)]
struct SessionRecord {
    generation: NonZeroU64,
    backend: Option<BackendSessionIdentity>,
    command: SessionCommandState,
    backend_initiated: bool,
}

#[derive(Debug)]
struct Registry {
    accepting: bool,
    generation: Option<NonZeroU64>,
    next_session: u64,
    sessions: BTreeMap<u64, SessionRecord>,
    retired_backends: BTreeMap<u64, BackendSessionIdentity>,
    active_commands: usize,
    cancelled_commands: bool,
    ambiguous_effect: bool,
    unidentified_backends: usize,
    diagnostics: MysqlProxyDiagnostics,
}

impl Registry {
    fn snapshot(&self) -> MysqlProxySnapshot {
        MysqlProxySnapshot {
            accepting: self.accepting,
            generation: self.generation,
            sessions: self.sessions.len(),
            active_commands: self.active_commands,
            backend_connections: self
                .sessions
                .values()
                .filter_map(|session| {
                    session
                        .backend
                        .as_ref()
                        .map(BackendSessionIdentity::connection)
                })
                .collect(),
            unidentified_backends: self.unidentified_backends
                + self
                    .sessions
                    .values()
                    .filter(|session| session.backend_initiated && session.backend.is_none())
                    .count(),
            diagnostics: self.diagnostics,
        }
    }
}

struct Shared {
    registry: Mutex<Registry>,
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, Registry> {
        self.registry.lock().expect("proxy registry mutex poisoned")
    }
}

struct AccountPreparationContainment<'a, C: MysqlProxyControlBoundary> {
    control: &'a mut C,
    authority: &'a MemberMutationAuthority,
    attempt: &'a AttemptContext,
    armed: bool,
}

impl<'a, C: MysqlProxyControlBoundary> AccountPreparationContainment<'a, C> {
    fn new(
        control: &'a mut C,
        authority: &'a MemberMutationAuthority,
        attempt: &'a AttemptContext,
    ) -> Self {
        Self {
            control,
            authority,
            attempt,
            armed: true,
        }
    }

    fn control(&mut self) -> &mut C {
        self.control
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<C: MysqlProxyControlBoundary> Drop for AccountPreparationContainment<'_, C> {
    fn drop(&mut self) {
        if self.armed {
            let _ = self
                .control
                .contain_proxy_preparation(self.authority, self.attempt);
        }
    }
}

/// Prepared loopback-only endpoint for one exact private UDS and account.
#[must_use = "the proxy requires explicit async revocation and account cleanup"]
pub struct MysqlClientProxy {
    listener: Option<TcpListener>,
    address: SocketAddr,
    backend: UnixSocketPath,
    account: ApplicationAccountReceipt,
    authority: MemberMutationAuthority,
    credential: ApplicationCredential,
    shared: Arc<Shared>,
    cancel: watch::Sender<bool>,
    accept_task: Option<JoinHandle<()>>,
    published_generation: Option<NonZeroU64>,
    contained_generation: Option<NonZeroU64>,
    tasks_joined: bool,
    native_absence_proved: bool,
    account_active: bool,
}

impl MysqlClientProxy {
    /// Prepares the exact account and binds one unpublished loopback listener.
    pub async fn prepare<C: MysqlProxyControlBoundary>(
        control: &mut C,
        authority: &MemberMutationAuthority,
        listen_address: SocketAddr,
        credential: ApplicationCredential,
        attempt: &AttemptContext,
    ) -> Result<Self, MysqlProxyError> {
        let mut credential = Some(credential);
        let mut account_obligation = None;
        Self::prepare_staged(
            control,
            authority,
            listen_address,
            &mut credential,
            &mut account_obligation,
            attempt,
        )
        .await
    }

    pub(crate) async fn prepare_staged<C: MysqlProxyControlBoundary>(
        control: &mut C,
        authority: &MemberMutationAuthority,
        listen_address: SocketAddr,
        credential: &mut Option<ApplicationCredential>,
        account_obligation: &mut Option<ApplicationAccountReceipt>,
        attempt: &AttemptContext,
    ) -> Result<Self, MysqlProxyError> {
        let credential_ref = credential
            .as_ref()
            .ok_or(MysqlProxyError::InvalidConfiguration)?;
        if !listen_address.ip().is_loopback()
            || credential_ref.attempt() != attempt.attempt()
            || credential_ref.attempt() != authority.attempt()
        {
            return Err(MysqlProxyError::InvalidConfiguration);
        }
        attempt.admit_work().map_err(map_attempt_admission)?;
        let account = control
            .prepare_application_account(authority, credential_ref, attempt)
            .await?;
        *account_obligation = Some(account.clone());
        let mut containment = AccountPreparationContainment::new(control, authority, attempt);
        let backend = match containment.control().proxy_backend_socket(authority) {
            Ok(backend) => backend,
            Err(primary) => {
                let cleanup = containment
                    .control()
                    .remove_application_account(authority, &account, attempt)
                    .await;
                if cleanup.is_ok() {
                    account_obligation.take();
                    containment.disarm();
                }
                return Err(pair_cleanup(primary, cleanup));
            }
        };
        let listener = match TcpListener::bind(listen_address).await {
            Ok(listener) => listener,
            Err(error) => {
                let primary = MysqlProxyError::Io {
                    phase: MysqlProxyPhase::Listener,
                    kind: error.kind(),
                };
                let cleanup = containment
                    .control()
                    .remove_application_account(authority, &account, attempt)
                    .await;
                if cleanup.is_ok() {
                    account_obligation.take();
                    containment.disarm();
                }
                return Err(pair_cleanup(primary, cleanup));
            }
        };
        let address = match listener.local_addr() {
            Ok(address) => address,
            Err(error) => {
                let primary = MysqlProxyError::Io {
                    phase: MysqlProxyPhase::Listener,
                    kind: error.kind(),
                };
                let cleanup = containment
                    .control()
                    .remove_application_account(authority, &account, attempt)
                    .await;
                if cleanup.is_ok() {
                    account_obligation.take();
                    containment.disarm();
                }
                return Err(pair_cleanup(primary, cleanup));
            }
        };
        let (cancel, _) = watch::channel(false);
        let credential = credential
            .take()
            .expect("staged credential remained owned until proxy construction");
        account_obligation.take();
        containment.disarm();
        Ok(Self {
            listener: Some(listener),
            address,
            backend,
            account,
            authority: authority.clone(),
            credential,
            shared: Arc::new(Shared {
                registry: Mutex::new(Registry {
                    accepting: false,
                    generation: None,
                    next_session: 1,
                    sessions: BTreeMap::new(),
                    retired_backends: BTreeMap::new(),
                    active_commands: 0,
                    cancelled_commands: false,
                    ambiguous_effect: false,
                    unidentified_backends: 0,
                    diagnostics: MysqlProxyDiagnostics::default(),
                }),
            }),
            cancel,
            accept_task: None,
            published_generation: None,
            contained_generation: None,
            tasks_joined: true,
            native_absence_proved: true,
            account_active: true,
        })
    }

    /// Bound unpublished or published loopback address.
    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Secret-free prepared account evidence.
    #[must_use]
    pub const fn account(&self) -> &ApplicationAccountReceipt {
        &self.account
    }

    /// Current exact registry snapshot.
    #[must_use]
    pub fn snapshot(&self) -> MysqlProxySnapshot {
        self.shared.lock().snapshot()
    }

    /// Publishes exactly one generation. Both permissions are mandatory.
    pub fn publish(
        &mut self,
        generation: NonZeroU64,
        read: bool,
        write: bool,
        attempt: &AttemptContext,
    ) -> Result<SocketAddr, MysqlProxyError> {
        attempt.admit_work().map_err(map_attempt_admission)?;
        if !read || !write {
            return Err(MysqlProxyError::BothPermissionsRequired);
        }
        if self.accept_task.is_some() || !self.account_active {
            return Err(MysqlProxyError::InvalidState);
        }
        let listener = self.listener.take().ok_or(MysqlProxyError::InvalidState)?;
        {
            let mut registry = self.shared.lock();
            if registry.generation.is_some() || !registry.sessions.is_empty() {
                return Err(MysqlProxyError::InvalidState);
            }
            registry.accepting = true;
            registry.generation = Some(generation);
            registry.cancelled_commands = false;
            registry.ambiguous_effect = false;
        }
        let shared = Arc::clone(&self.shared);
        let backend = self.backend.clone();
        let account = self.account.clone();
        let authority = self.authority.clone();
        let cancel = self.cancel.subscribe();
        let cancel_sender = self.cancel.clone();
        let attempt = attempt.clone();
        self.accept_task = Some(tokio::spawn(async move {
            accept_loop(
                listener,
                AcceptContext {
                    shared,
                    backend,
                    account,
                    authority,
                    generation,
                    attempt,
                },
                cancel_sender,
                cancel,
            )
            .await;
        }));
        self.published_generation = Some(generation);
        self.contained_generation = None;
        self.tasks_joined = false;
        self.native_absence_proved = false;
        Ok(self.address)
    }

    /// Atomically stops acceptance and all later command dispatch. Native
    /// absence proof remains required through [`Self::revoke`].
    pub fn seal_now(&self) {
        let mut registry = self.shared.lock();
        registry.accepting = false;
        drop(registry);
        let _ = self.cancel.send(true);
    }

    /// Revokes one generation, terminates every exact registered native
    /// backend, and returns only after proxy-zero and native absence.
    pub async fn revoke<C: MysqlProxyControlBoundary>(
        &mut self,
        control: &mut C,
        authority: &MemberMutationAuthority,
        generation: NonZeroU64,
        attempt: &AttemptContext,
    ) -> Result<MysqlProxyContainmentReceipt, MysqlProxyError> {
        if self.published_generation != Some(generation)
            || self.shared.lock().generation != Some(generation)
        {
            return Err(MysqlProxyError::GenerationMismatch);
        }
        self.seal_now();
        self.listener.take();
        let deadline = bounded_deadline(attempt.deadline(), REVOCATION_TIMEOUT)
            .ok_or(MysqlProxyError::Deadline(MysqlProxyPhase::Revocation))?;
        if let Some(task) = self.accept_task.as_mut() {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .ok_or(MysqlProxyError::Deadline(MysqlProxyPhase::Revocation))?;
            match tokio::time::timeout(remaining, &mut *task).await {
                Ok(joined) => {
                    if joined.is_err() {
                        self.accept_task.take();
                        self.record_cleanup_failure();
                        return Err(MysqlProxyError::InvalidState);
                    }
                    self.accept_task.take();
                }
                Err(_) => {
                    let task = self
                        .accept_task
                        .take()
                        .expect("timed out task remained owned by proxy");
                    task.abort();
                    let _ = task.await;
                    self.record_cleanup_failure();
                    return Err(MysqlProxyError::Deadline(MysqlProxyPhase::Revocation));
                }
            }
        }
        self.tasks_joined = true;
        let mut proved = BTreeSet::new();
        loop {
            let pending = {
                let registry = self.shared.lock();
                registry
                    .retired_backends
                    .iter()
                    .filter(|(connection, _)| !proved.contains(*connection))
                    .map(|(_, session)| session.clone())
                    .collect::<Vec<_>>()
            };
            for session in pending {
                if let Err(error) = control
                    .terminate_proxy_backend(authority, &session, attempt, deadline)
                    .await
                {
                    self.record_cleanup_failure();
                    return Err(error);
                }
                proved.insert(session.connection().get());
            }
            let snapshot = self.snapshot();
            if snapshot.unidentified_backends != 0 {
                self.record_cleanup_failure();
                return Err(MysqlProxyError::UnidentifiedBackend);
            }
            let all_known_proved = {
                let registry = self.shared.lock();
                registry
                    .retired_backends
                    .keys()
                    .all(|connection| proved.contains(connection))
            };
            if snapshot.sessions == 0 && snapshot.active_commands == 0 && all_known_proved {
                break;
            }
            if Instant::now() >= deadline {
                self.record_cleanup_failure();
                return Err(MysqlProxyError::Deadline(MysqlProxyPhase::Revocation));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        let registry = self.shared.lock();
        let receipt = MysqlProxyContainmentReceipt {
            generation,
            sessions_zero: registry.sessions.is_empty(),
            commands_zero: registry.active_commands == 0,
            native_absence: registry
                .retired_backends
                .keys()
                .all(|connection| proved.contains(connection)),
            cancelled_commands: registry.cancelled_commands,
            ambiguous_effect: registry.ambiguous_effect,
            cleanup_complete: true,
            unidentified_backends: registry.unidentified_backends,
            diagnostics: registry.diagnostics,
        };
        self.native_absence_proved = receipt.native_absence;
        self.contained_generation = Some(generation);
        Ok(receipt)
    }

    /// Binds a fresh unpublished listener after complete containment while
    /// retaining the exact prepared account and credential.
    pub async fn rearm(
        &mut self,
        listen_address: SocketAddr,
        attempt: &AttemptContext,
    ) -> Result<SocketAddr, MysqlProxyError> {
        attempt.admit_work().map_err(map_attempt_admission)?;
        let snapshot = self.snapshot();
        if !listen_address.ip().is_loopback()
            || !self.account_active
            || !self.is_fully_contained()
            || snapshot.accepting()
            || snapshot.sessions() != 0
            || snapshot.active_commands() != 0
            || self.accept_task.is_some()
        {
            return Err(MysqlProxyError::InvalidState);
        }
        let listener =
            TcpListener::bind(listen_address)
                .await
                .map_err(|error| MysqlProxyError::Io {
                    phase: MysqlProxyPhase::Listener,
                    kind: error.kind(),
                })?;
        let address = listener.local_addr().map_err(|error| MysqlProxyError::Io {
            phase: MysqlProxyPhase::Listener,
            kind: error.kind(),
        })?;
        {
            let mut registry = self.shared.lock();
            registry.accepting = false;
            registry.generation = None;
            registry.sessions.clear();
            registry.retired_backends.clear();
            registry.active_commands = 0;
            registry.cancelled_commands = false;
            registry.ambiguous_effect = false;
            registry.unidentified_backends = 0;
            registry.diagnostics = MysqlProxyDiagnostics::default();
        }
        let (cancel, _) = watch::channel(false);
        self.cancel = cancel;
        self.listener = Some(listener);
        self.address = address;
        self.published_generation = None;
        self.contained_generation = None;
        self.tasks_joined = true;
        self.native_absence_proved = true;
        Ok(address)
    }

    /// Removes the prepared account and clears the retained password.
    pub async fn cleanup_account<C: MysqlProxyControlBoundary>(
        &mut self,
        control: &mut C,
        authority: &MemberMutationAuthority,
        attempt: &AttemptContext,
    ) -> Result<(), MysqlProxyError> {
        let cleanup_ready = {
            let registry = self.shared.lock();
            !registry.accepting
                && registry.sessions.is_empty()
                && registry.active_commands == 0
                && registry.unidentified_backends == 0
                && self.tasks_joined
                && self.native_absence_proved
                && self.is_fully_contained()
        };
        if !cleanup_ready {
            return Err(MysqlProxyError::InvalidState);
        }
        self.listener.take();
        let result = control
            .remove_application_account(authority, &self.account, attempt)
            .await;
        self.credential.clear();
        if result.is_ok() {
            self.account_active = false;
        } else {
            self.record_cleanup_failure();
        }
        result
    }

    /// Whether the retained application secret was cleared.
    #[must_use]
    pub fn credential_cleared(&self) -> bool {
        self.credential.is_cleared()
    }

    /// Whether the exact prepared application account remains active.
    #[must_use]
    pub const fn account_active(&self) -> bool {
        self.account_active
    }

    /// Whether the endpoint was never published or its published generation
    /// has a completed containment barrier.
    #[must_use]
    pub fn is_fully_contained(&self) -> bool {
        self.published_generation.is_none()
            || (self.published_generation == self.contained_generation
                && self.tasks_joined
                && self.native_absence_proved)
    }

    /// Generation that was published through this prepared listener.
    #[must_use]
    pub const fn published_generation(&self) -> Option<NonZeroU64> {
        self.published_generation
    }

    fn record_cleanup_failure(&self) {
        let mut registry = self.shared.lock();
        registry.diagnostics.cleanup_failures =
            registry.diagnostics.cleanup_failures.saturating_add(1);
    }
}

impl Drop for MysqlClientProxy {
    fn drop(&mut self) {
        self.seal_now();
        self.listener.take();
        if let Some(task) = self.accept_task.take() {
            task.abort();
        }
        self.contained_generation = None;
        self.tasks_joined = false;
        self.native_absence_proved = false;
        self.credential.clear();
    }
}

fn pair_cleanup(primary: MysqlProxyError, cleanup: Result<(), MysqlProxyError>) -> MysqlProxyError {
    match cleanup {
        Ok(()) => primary,
        Err(cleanup) => MysqlProxyError::Cleanup {
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        },
    }
}

fn map_attempt_admission(error: crate::service::AttemptAdmissionError) -> MysqlProxyError {
    match error {
        crate::service::AttemptAdmissionError::Cancelled => MysqlProxyError::Cancelled,
        crate::service::AttemptAdmissionError::CleanupReserve
        | crate::service::AttemptAdmissionError::Expired => {
            MysqlProxyError::Deadline(MysqlProxyPhase::Revocation)
        }
    }
}

fn bounded_deadline(attempt_deadline: Instant, duration: Duration) -> Option<Instant> {
    let now = Instant::now();
    let phase = now.checked_add(duration)?;
    let deadline = phase.min(attempt_deadline);
    (deadline > now).then_some(deadline)
}

struct AcceptContext {
    shared: Arc<Shared>,
    backend: UnixSocketPath,
    account: ApplicationAccountReceipt,
    authority: MemberMutationAuthority,
    generation: NonZeroU64,
    attempt: AttemptContext,
}

async fn accept_loop(
    listener: TcpListener,
    context: AcceptContext,
    cancel_sender: watch::Sender<bool>,
    mut cancel: watch::Receiver<bool>,
) {
    let mut attempt_cancel = context.attempt.cancellation_receiver();
    let mut sessions = tokio::task::JoinSet::new();
    loop {
        if *cancel.borrow() || context.attempt.admit_work().is_err() {
            seal_registry(&context.shared);
            let _ = cancel_sender.send(true);
            break;
        }
        tokio::select! {
            changed = cancel.changed() => {
                if changed.is_err() || *cancel.borrow() {
                    break;
                }
            }
            changed = attempt_cancel.changed() => {
                let _ = changed;
                seal_registry(&context.shared);
                let _ = cancel_sender.send(true);
                break;
            }
            () = tokio::time::sleep_until(context.attempt.work_deadline().into()) => {
                seal_registry(&context.shared);
                let _ = cancel_sender.send(true);
                break;
            }
            completed = sessions.join_next(), if !sessions.is_empty() => {
                if completed.is_some_and(|result| result.is_err()) {
                    let mut registry = context.shared.lock();
                    registry.ambiguous_effect = true;
                    registry.diagnostics.ambiguities =
                        registry.diagnostics.ambiguities.saturating_add(1);
                }
            }
            accepted = listener.accept() => {
                let Ok((client, _)) = accepted else {
                    break;
                };
                let session_id = {
                    let mut registry = context.shared.lock();
                    if !registry.accepting
                        || registry.generation != Some(context.generation)
                        || context.attempt.admit_work().is_err()
                    {
                        drop(client);
                        continue;
                    }
                    let id = registry.next_session;
                    registry.next_session = registry.next_session.saturating_add(1);
                    registry.sessions.insert(id, SessionRecord {
                        generation: context.generation,
                        backend: None,
                        command: SessionCommandState::Idle,
                        backend_initiated: false,
                    });
                    id
                };
                sessions.spawn(run_session(
                    client,
                    SessionContext {
                        shared: Arc::clone(&context.shared),
                        backend: context.backend.clone(),
                        account: context.account.clone(),
                        authority: context.authority.clone(),
                        generation: context.generation,
                        session_id,
                        attempt: context.attempt.clone(),
                    },
                    cancel.clone(),
                ));
            }
        }
    }
    while let Some(result) = sessions.join_next().await {
        if result.is_err() {
            let mut registry = context.shared.lock();
            registry.ambiguous_effect = true;
            registry.diagnostics.ambiguities = registry.diagnostics.ambiguities.saturating_add(1);
        }
    }
}

fn seal_registry(shared: &Shared) {
    shared.lock().accepting = false;
}

struct SessionGuard {
    shared: Arc<Shared>,
    id: u64,
}

impl SessionGuard {
    fn backend_initiated(&self) {
        if let Some(session) = self.shared.lock().sessions.get_mut(&self.id) {
            session.backend_initiated = true;
        }
    }

    fn backend_not_created(&self) {
        if let Some(session) = self.shared.lock().sessions.get_mut(&self.id) {
            session.backend_initiated = false;
        }
    }

    fn register_backend(&self, session_identity: BackendSessionIdentity) {
        let mut registry = self.shared.lock();
        if let Some(session) = registry.sessions.get_mut(&self.id) {
            session.backend = Some(session_identity);
            session.backend_initiated = false;
        }
    }

    fn begin_command(
        &self,
        generation: NonZeroU64,
        attempt: &AttemptContext,
    ) -> Result<(), MysqlProxyError> {
        let mut registry = self.shared.lock();
        if attempt.admit_work().is_err()
            || !registry.accepting
            || registry.generation != Some(generation)
        {
            return Err(MysqlProxyError::GenerationMismatch);
        }
        let session = registry
            .sessions
            .get_mut(&self.id)
            .ok_or(MysqlProxyError::InvalidState)?;
        if session.generation != generation || session.command != SessionCommandState::Idle {
            return Err(MysqlProxyError::GenerationMismatch);
        }
        session.command = SessionCommandState::Active;
        registry.active_commands += 1;
        Ok(())
    }

    fn complete_command(&self) {
        let mut registry = self.shared.lock();
        if let Some(session) = registry.sessions.get_mut(&self.id)
            && session.command == SessionCommandState::Active
        {
            session.command = SessionCommandState::Idle;
            registry.active_commands = registry.active_commands.saturating_sub(1);
            registry.diagnostics.normal_completions =
                registry.diagnostics.normal_completions.saturating_add(1);
        }
    }

    fn record_result(&self, result: &Result<(), MysqlProxyError>) {
        let mut registry = self.shared.lock();
        let active = registry
            .sessions
            .get(&self.id)
            .is_some_and(|session| session.command == SessionCommandState::Active);
        match result {
            Ok(()) => {}
            Err(MysqlProxyError::Cancelled) => {
                registry.diagnostics.cancellations =
                    registry.diagnostics.cancellations.saturating_add(1);
            }
            Err(MysqlProxyError::Deadline(_)) => {
                registry.diagnostics.deadlines = registry.diagnostics.deadlines.saturating_add(1);
            }
            Err(MysqlProxyError::Protocol {
                phase: MysqlProxyPhase::Greeting | MysqlProxyPhase::Authentication,
                ..
            }) => {
                registry.diagnostics.auth_rejections =
                    registry.diagnostics.auth_rejections.saturating_add(1);
            }
            Err(_) if !active => {
                registry.diagnostics.pre_dispatch_rejections = registry
                    .diagnostics
                    .pre_dispatch_rejections
                    .saturating_add(1);
            }
            Err(_) => {}
        }
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        let mut registry = self.shared.lock();
        if let Some(session) = registry.sessions.remove(&self.id) {
            let unidentified = session.backend_initiated && session.backend.is_none();
            if let Some(connection) = session.backend {
                registry
                    .retired_backends
                    .insert(connection.connection().get(), connection);
            }
            if unidentified {
                registry.unidentified_backends = registry.unidentified_backends.saturating_add(1);
            }
            if session.command == SessionCommandState::Active {
                registry.active_commands = registry.active_commands.saturating_sub(1);
                registry.ambiguous_effect = true;
                registry.diagnostics.ambiguities =
                    registry.diagnostics.ambiguities.saturating_add(1);
                if !registry.accepting {
                    registry.cancelled_commands = true;
                }
            }
        }
    }
}

struct SessionContext {
    shared: Arc<Shared>,
    backend: UnixSocketPath,
    account: ApplicationAccountReceipt,
    authority: MemberMutationAuthority,
    generation: NonZeroU64,
    session_id: u64,
    attempt: AttemptContext,
}

async fn run_session(
    mut client: TcpStream,
    context: SessionContext,
    mut cancel: watch::Receiver<bool>,
) {
    let guard = SessionGuard {
        shared: Arc::clone(&context.shared),
        id: context.session_id,
    };
    let result = run_session_inner(&mut client, &guard, &context, &mut cancel).await;
    guard.record_result(&result);
}

async fn run_session_inner(
    client: &mut TcpStream,
    guard: &SessionGuard,
    context: &SessionContext,
    cancel: &mut watch::Receiver<bool>,
) -> Result<(), MysqlProxyError> {
    context
        .backend
        .revalidate()
        .map_err(|_| MysqlProxyError::Io {
            phase: MysqlProxyPhase::Greeting,
            kind: ErrorKind::NotFound,
        })?;
    let authentication_deadline = phase_timeout(
        context.attempt.work_deadline(),
        GREETING_AUTH_TIMEOUT,
        MysqlProxyPhase::Greeting,
    )?;
    guard.backend_initiated();
    let connected = select_cancel(
        cancel,
        authentication_deadline,
        MysqlProxyPhase::Greeting,
        UnixStream::connect(context.backend.as_path()),
    )
    .await?;
    let mut backend = match connected {
        Ok(backend) => backend,
        Err(error) => {
            if definitive_connect_non_creation(error.kind()) {
                guard.backend_not_created();
            }
            return Err(MysqlProxyError::Io {
                phase: MysqlProxyPhase::Greeting,
                kind: error.kind(),
            });
        }
    };
    let greeting = read_packet_cancel(
        &mut backend,
        cancel,
        authentication_deadline,
        MysqlProxyPhase::Greeting,
    )
    .await?;
    if greeting.sequence != 0 {
        return Err(protocol(
            MysqlProxyPhase::Greeting,
            MysqlProxyProtocolError::SequenceMismatch,
        ));
    }
    let (connection, filtered) = filter_greeting(&greeting.payload)?;
    guard.register_backend(BackendSessionIdentity::new(
        connection,
        context.account.clone(),
        context.authority.clone(),
        context.backend.clone(),
    ));
    write_packet(
        client,
        greeting.sequence,
        &filtered,
        authentication_deadline,
        MysqlProxyPhase::Greeting,
    )
    .await?;

    let response = read_packet_cancel(
        client,
        cancel,
        authentication_deadline,
        MysqlProxyPhase::Authentication,
    )
    .await?;
    if response.sequence != 1 {
        return Err(protocol(
            MysqlProxyPhase::Authentication,
            MysqlProxyProtocolError::SequenceMismatch,
        ));
    }
    validate_handshake_response(&response.payload, context.account.username())?;
    write_packet(
        &mut backend,
        response.sequence,
        &response.payload,
        authentication_deadline,
        MysqlProxyPhase::Authentication,
    )
    .await?;
    let fast = read_packet_cancel(
        &mut backend,
        cancel,
        authentication_deadline,
        MysqlProxyPhase::Authentication,
    )
    .await?;
    if fast.sequence != 2 || fast.payload.as_slice() != [0x01, 0x03] {
        return Err(protocol(
            MysqlProxyPhase::Authentication,
            MysqlProxyProtocolError::UnsupportedAuthentication,
        ));
    }
    write_packet(
        client,
        fast.sequence,
        &fast.payload,
        authentication_deadline,
        MysqlProxyPhase::Authentication,
    )
    .await?;
    let ok = read_packet_cancel(
        &mut backend,
        cancel,
        authentication_deadline,
        MysqlProxyPhase::Authentication,
    )
    .await?;
    if ok.sequence != 3 || ok.payload.first() != Some(&0x00) {
        return Err(protocol(
            MysqlProxyPhase::Authentication,
            MysqlProxyProtocolError::UnsupportedAuthentication,
        ));
    }
    validate_ok_packet(&ok.payload)?;
    write_packet(
        client,
        ok.sequence,
        &ok.payload,
        authentication_deadline,
        MysqlProxyPhase::Authentication,
    )
    .await?;

    loop {
        let command_deadline = phase_timeout(
            context.attempt.work_deadline(),
            CLIENT_COMMAND_TIMEOUT,
            MysqlProxyPhase::ClientCommand,
        )?;
        let command = read_packet_cancel(
            client,
            cancel,
            command_deadline,
            MysqlProxyPhase::ClientCommand,
        )
        .await?;
        if command.sequence != 0 {
            return Err(protocol(
                MysqlProxyPhase::ClientCommand,
                MysqlProxyProtocolError::SequenceMismatch,
            ));
        }
        let command_byte = *command.payload.first().ok_or_else(|| {
            protocol(
                MysqlProxyPhase::ClientCommand,
                MysqlProxyProtocolError::IncompletePacket,
            )
        })?;
        match command_byte {
            0x03 | 0x0e | 0x01 => {}
            _ => {
                return Err(protocol(
                    MysqlProxyPhase::ClientCommand,
                    MysqlProxyProtocolError::UnsupportedCommand,
                ));
            }
        }
        guard.begin_command(context.generation, &context.attempt)?;
        if context.attempt.admit_work().is_err() || *cancel.borrow() {
            return Err(MysqlProxyError::Cancelled);
        }
        write_packet(
            &mut backend,
            command.sequence,
            &command.payload,
            command_deadline,
            MysqlProxyPhase::ClientCommand,
        )
        .await?;
        if command_byte == 0x01 {
            let response_deadline = phase_timeout(
                context.attempt.work_deadline(),
                SERVER_RESPONSE_TIMEOUT,
                MysqlProxyPhase::ServerResponse,
            )?;
            let mut byte = [0_u8; 1];
            let read = select_cancel(
                cancel,
                response_deadline,
                MysqlProxyPhase::ServerResponse,
                backend.read(&mut byte),
            )
            .await?
            .map_err(|error| MysqlProxyError::Io {
                phase: MysqlProxyPhase::ServerResponse,
                kind: error.kind(),
            })?;
            if read != 0 {
                return Err(protocol(
                    MysqlProxyPhase::ServerResponse,
                    MysqlProxyProtocolError::UnsupportedResult,
                ));
            }
            guard.complete_command();
            return Ok(());
        }
        let response_deadline = phase_timeout(
            context.attempt.work_deadline(),
            SERVER_RESPONSE_TIMEOUT,
            MysqlProxyPhase::ServerResponse,
        )?;
        if command_byte == 0x0e {
            forward_ping_response(&mut backend, client, cancel, response_deadline).await?;
        } else {
            forward_query_response(&mut backend, client, cancel, response_deadline).await?;
        }
        guard.complete_command();
    }
}

async fn forward_ping_response(
    backend: &mut UnixStream,
    client: &mut TcpStream,
    cancel: &mut watch::Receiver<bool>,
    deadline: Instant,
) -> Result<(), MysqlProxyError> {
    let packet =
        read_packet_cancel(backend, cancel, deadline, MysqlProxyPhase::ServerResponse).await?;
    if packet.sequence != 1 {
        return Err(protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::SequenceMismatch,
        ));
    }
    match packet.payload.first() {
        Some(0x00) => validate_ok_packet(&packet.payload)?,
        Some(0xff) => validate_err_packet(&packet.payload)?,
        Some(0xfb) => {
            return Err(protocol(
                MysqlProxyPhase::ServerResponse,
                MysqlProxyProtocolError::LocalInfile,
            ));
        }
        _ => {
            return Err(protocol(
                MysqlProxyPhase::ServerResponse,
                MysqlProxyProtocolError::UnsupportedResult,
            ));
        }
    }
    write_packet(
        client,
        packet.sequence,
        &packet.payload,
        deadline,
        MysqlProxyPhase::ServerResponse,
    )
    .await
}

async fn forward_query_response(
    backend: &mut UnixStream,
    client: &mut TcpStream,
    cancel: &mut watch::Receiver<bool>,
    deadline: Instant,
) -> Result<(), MysqlProxyError> {
    let first =
        read_packet_cancel(backend, cancel, deadline, MysqlProxyPhase::ServerResponse).await?;
    if first.sequence != 1 {
        return Err(protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::SequenceMismatch,
        ));
    }
    match first.payload.first() {
        Some(0x00) => {
            validate_ok_packet(&first.payload)?;
            return write_packet(
                client,
                first.sequence,
                &first.payload,
                deadline,
                MysqlProxyPhase::ServerResponse,
            )
            .await;
        }
        Some(0xff) => {
            validate_err_packet(&first.payload)?;
            return write_packet(
                client,
                first.sequence,
                &first.payload,
                deadline,
                MysqlProxyPhase::ServerResponse,
            )
            .await;
        }
        Some(0xfb) => {
            return Err(protocol(
                MysqlProxyPhase::ServerResponse,
                MysqlProxyProtocolError::LocalInfile,
            ));
        }
        _ => {}
    }
    let (columns, consumed) = decode_lenenc(&first.payload, 0)?;
    if columns == 0 || consumed != first.payload.len() {
        return Err(protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::UnsupportedResult,
        ));
    }
    write_packet(
        client,
        first.sequence,
        &first.payload,
        deadline,
        MysqlProxyPhase::ServerResponse,
    )
    .await?;
    let mut sequence = 2_u8;
    for _ in 0..columns {
        let column =
            read_packet_cancel(backend, cancel, deadline, MysqlProxyPhase::ServerResponse).await?;
        require_sequence(&column, sequence)?;
        validate_column_definition(&column.payload)?;
        write_packet(
            client,
            column.sequence,
            &column.payload,
            deadline,
            MysqlProxyPhase::ServerResponse,
        )
        .await?;
        sequence = sequence.wrapping_add(1);
    }
    let metadata_end =
        read_packet_cancel(backend, cancel, deadline, MysqlProxyPhase::ServerResponse).await?;
    require_sequence(&metadata_end, sequence)?;
    validate_eof_packet(&metadata_end.payload)?;
    write_packet(
        client,
        metadata_end.sequence,
        &metadata_end.payload,
        deadline,
        MysqlProxyPhase::ServerResponse,
    )
    .await?;
    sequence = sequence.wrapping_add(1);
    loop {
        let row =
            read_packet_cancel(backend, cancel, deadline, MysqlProxyPhase::ServerResponse).await?;
        require_sequence(&row, sequence)?;
        let complete = row.payload.first() == Some(&0xfe) && row.payload.len() < 9;
        if complete {
            validate_eof_packet(&row.payload)?;
        } else {
            validate_text_row(&row.payload, columns)?;
        }
        write_packet(
            client,
            row.sequence,
            &row.payload,
            deadline,
            MysqlProxyPhase::ServerResponse,
        )
        .await?;
        if complete {
            return Ok(());
        }
        sequence = sequence.wrapping_add(1);
    }
}

fn require_sequence(packet: &Packet, expected: u8) -> Result<(), MysqlProxyError> {
    if packet.sequence == expected {
        Ok(())
    } else {
        Err(protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::SequenceMismatch,
        ))
    }
}

fn validate_ok_packet(payload: &[u8]) -> Result<(), MysqlProxyError> {
    if payload.first() != Some(&0x00) {
        return Err(protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::UnsupportedResult,
        ));
    }
    let (_, next) = decode_lenenc(payload, 1)?;
    let (_, next) = decode_lenenc(payload, next)?;
    let status = read_u16(payload, next)?;
    let warnings_offset = next.checked_add(2).ok_or_else(|| {
        protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::IncompletePacket,
        )
    })?;
    let _warnings = read_u16(payload, warnings_offset)?;
    if status & SERVER_MORE_RESULTS_EXISTS != 0 {
        Err(protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::MoreResults,
        ))
    } else {
        Ok(())
    }
}

fn validate_err_packet(payload: &[u8]) -> Result<(), MysqlProxyError> {
    if payload.first() != Some(&0xff) {
        return Err(protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::UnsupportedResult,
        ));
    }
    let _error_code = read_u16(payload, 1)?;
    if payload.get(3) != Some(&b'#') {
        return Err(protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::UnsupportedResult,
        ));
    }
    let sql_state = payload.get(4..9).ok_or_else(|| {
        protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::IncompletePacket,
        )
    })?;
    if !sql_state
        .iter()
        .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        return Err(protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::UnsupportedResult,
        ));
    }
    Ok(())
}

fn validate_eof_packet(payload: &[u8]) -> Result<(), MysqlProxyError> {
    if payload.len() != 5 || payload.first() != Some(&0xfe) {
        return Err(protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::UnsupportedResult,
        ));
    }
    let status = read_u16(payload, 3)?;
    if status & SERVER_MORE_RESULTS_EXISTS != 0 {
        Err(protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::MoreResults,
        ))
    } else {
        Ok(())
    }
}

fn validate_column_definition(payload: &[u8]) -> Result<(), MysqlProxyError> {
    let mut offset = 0;
    for _ in 0..6 {
        offset = skip_lenenc_bytes(payload, offset)?;
    }
    let (fixed_length, next) = decode_lenenc(payload, offset)?;
    if fixed_length != COLUMN_DEFINITION_FIXED_FIELDS {
        return Err(protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::UnsupportedResult,
        ));
    }
    let end = next
        .checked_add(COLUMN_DEFINITION_FIXED_FIELDS as usize)
        .ok_or_else(|| {
            protocol(
                MysqlProxyPhase::ServerResponse,
                MysqlProxyProtocolError::IncompletePacket,
            )
        })?;
    if end != payload.len() {
        return Err(protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::UnsupportedResult,
        ));
    }
    let _character_set = read_u16(payload, next)?;
    let _column_length = read_u32(payload, next + 2)?;
    let _column_type = payload.get(next + 6).ok_or_else(|| {
        protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::IncompletePacket,
        )
    })?;
    let _flags = read_u16(payload, next + 7)?;
    let _decimals = payload.get(next + 9).ok_or_else(|| {
        protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::IncompletePacket,
        )
    })?;
    let _filler = payload.get(next + 10..next + 12).ok_or_else(|| {
        protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::IncompletePacket,
        )
    })?;
    Ok(())
}

fn validate_text_row(payload: &[u8], columns: u64) -> Result<(), MysqlProxyError> {
    let mut offset = 0;
    for _ in 0..columns {
        if payload.get(offset) == Some(&0xfb) {
            offset += 1;
        } else {
            offset = skip_lenenc_bytes(payload, offset)?;
        }
    }
    if offset == payload.len() {
        Ok(())
    } else {
        Err(protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::UnsupportedResult,
        ))
    }
}

fn skip_lenenc_bytes(payload: &[u8], offset: usize) -> Result<usize, MysqlProxyError> {
    let (length, next) = decode_lenenc(payload, offset)?;
    let length = usize::try_from(length).map_err(|_| {
        protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::IncompletePacket,
        )
    })?;
    let end = next.checked_add(length).ok_or_else(|| {
        protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::IncompletePacket,
        )
    })?;
    if end > payload.len() {
        Err(protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::IncompletePacket,
        ))
    } else {
        Ok(end)
    }
}

fn filter_greeting(payload: &[u8]) -> Result<(BackendConnectionId, Vec<u8>), MysqlProxyError> {
    if payload.first() != Some(&10) {
        return Err(protocol(
            MysqlProxyPhase::Greeting,
            MysqlProxyProtocolError::InvalidGreeting,
        ));
    }
    let server_end = payload[1..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|position| position + 1)
        .ok_or_else(|| {
            protocol(
                MysqlProxyPhase::Greeting,
                MysqlProxyProtocolError::InvalidGreeting,
            )
        })?;
    let connection_offset = server_end + 1;
    let capability_offset = connection_offset + 4 + 8 + 1;
    if payload.len() < capability_offset + 2 + 1 + 2 + 2 + 1 + 10 {
        return Err(protocol(
            MysqlProxyPhase::Greeting,
            MysqlProxyProtocolError::InvalidGreeting,
        ));
    }
    let connection = u32::from_le_bytes(
        payload[connection_offset..connection_offset + 4]
            .try_into()
            .expect("validated four-byte connection ID"),
    );
    let connection = NonZeroU64::new(u64::from(connection)).ok_or_else(|| {
        protocol(
            MysqlProxyPhase::Greeting,
            MysqlProxyProtocolError::InvalidGreeting,
        )
    })?;
    let upper_offset = capability_offset + 2 + 1 + 2;
    let capabilities = u32::from(read_u16(payload, capability_offset)?)
        | (u32::from(read_u16(payload, upper_offset)?) << 16);
    if capabilities & REQUIRED_CAPABILITIES != REQUIRED_CAPABILITIES
        || !payload.ends_with(b"caching_sha2_password\0")
    {
        return Err(protocol(
            MysqlProxyPhase::Greeting,
            MysqlProxyProtocolError::UnsupportedAuthentication,
        ));
    }
    let filtered = capabilities & ALLOWED_CAPABILITIES;
    let mut output = payload.to_vec();
    output[capability_offset..capability_offset + 2]
        .copy_from_slice(&(filtered as u16).to_le_bytes());
    output[upper_offset..upper_offset + 2]
        .copy_from_slice(&((filtered >> 16) as u16).to_le_bytes());
    Ok((BackendConnectionId::new(connection), output))
}

fn validate_handshake_response(payload: &[u8], username: &str) -> Result<(), MysqlProxyError> {
    if payload.len() < 32 {
        return Err(protocol(
            MysqlProxyPhase::Authentication,
            MysqlProxyProtocolError::IncompletePacket,
        ));
    }
    let capabilities = u32::from_le_bytes(
        payload[..4]
            .try_into()
            .expect("validated four-byte capability flags"),
    );
    if capabilities & !ALLOWED_CAPABILITIES != 0
        || capabilities & REQUIRED_CAPABILITIES != REQUIRED_CAPABILITIES
    {
        return Err(protocol(
            MysqlProxyPhase::Authentication,
            MysqlProxyProtocolError::UnsupportedCapability,
        ));
    }
    let mut offset = 32;
    let username_end = payload[offset..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|position| offset + position)
        .ok_or_else(|| {
            protocol(
                MysqlProxyPhase::Authentication,
                MysqlProxyProtocolError::IncompletePacket,
            )
        })?;
    if payload[offset..username_end] != *username.as_bytes() {
        return Err(protocol(
            MysqlProxyPhase::Authentication,
            MysqlProxyProtocolError::UsernameMismatch,
        ));
    }
    offset = username_end + 1;
    let (auth_length, next) = decode_lenenc(payload, offset)?;
    offset = next
        .checked_add(usize::try_from(auth_length).map_err(|_| {
            protocol(
                MysqlProxyPhase::Authentication,
                MysqlProxyProtocolError::IncompletePacket,
            )
        })?)
        .ok_or_else(|| {
            protocol(
                MysqlProxyPhase::Authentication,
                MysqlProxyProtocolError::IncompletePacket,
            )
        })?;
    if offset > payload.len() {
        return Err(protocol(
            MysqlProxyPhase::Authentication,
            MysqlProxyProtocolError::IncompletePacket,
        ));
    }
    if capabilities & CLIENT_CONNECT_WITH_DB != 0 {
        offset = skip_nul(payload, offset)?;
    }
    let plugin_end = payload[offset..]
        .iter()
        .position(|byte| *byte == 0)
        .map(|position| offset + position)
        .ok_or_else(|| {
            protocol(
                MysqlProxyPhase::Authentication,
                MysqlProxyProtocolError::IncompletePacket,
            )
        })?;
    if &payload[offset..plugin_end] != b"caching_sha2_password" {
        return Err(protocol(
            MysqlProxyPhase::Authentication,
            MysqlProxyProtocolError::UnsupportedAuthentication,
        ));
    }
    offset = plugin_end + 1;
    if capabilities & CLIENT_CONNECT_ATTRS != 0 {
        let (attributes, next) = decode_lenenc(payload, offset)?;
        offset = next
            .checked_add(usize::try_from(attributes).map_err(|_| {
                protocol(
                    MysqlProxyPhase::Authentication,
                    MysqlProxyProtocolError::IncompletePacket,
                )
            })?)
            .ok_or_else(|| {
                protocol(
                    MysqlProxyPhase::Authentication,
                    MysqlProxyProtocolError::IncompletePacket,
                )
            })?;
    }
    if offset != payload.len() {
        return Err(protocol(
            MysqlProxyPhase::Authentication,
            MysqlProxyProtocolError::UnsupportedAuthentication,
        ));
    }
    Ok(())
}

fn skip_nul(payload: &[u8], offset: usize) -> Result<usize, MysqlProxyError> {
    payload
        .get(offset..)
        .and_then(|rest| rest.iter().position(|byte| *byte == 0))
        .map(|position| offset + position + 1)
        .ok_or_else(|| {
            protocol(
                MysqlProxyPhase::Authentication,
                MysqlProxyProtocolError::IncompletePacket,
            )
        })
}

fn decode_lenenc(payload: &[u8], offset: usize) -> Result<(u64, usize), MysqlProxyError> {
    let first = *payload.get(offset).ok_or_else(|| {
        protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::IncompletePacket,
        )
    })?;
    match first {
        0x00..=0xfa => Ok((u64::from(first), offset + 1)),
        0xfc => Ok((u64::from(read_u16(payload, offset + 1)?), offset + 3)),
        0xfd => {
            let bytes = payload.get(offset + 1..offset + 4).ok_or_else(|| {
                protocol(
                    MysqlProxyPhase::ServerResponse,
                    MysqlProxyProtocolError::IncompletePacket,
                )
            })?;
            Ok((
                u64::from(bytes[0]) | (u64::from(bytes[1]) << 8) | (u64::from(bytes[2]) << 16),
                offset + 4,
            ))
        }
        0xfe => {
            let bytes = payload.get(offset + 1..offset + 9).ok_or_else(|| {
                protocol(
                    MysqlProxyPhase::ServerResponse,
                    MysqlProxyProtocolError::IncompletePacket,
                )
            })?;
            Ok((
                u64::from_le_bytes(
                    bytes
                        .try_into()
                        .expect("validated eight-byte length-encoded integer"),
                ),
                offset + 9,
            ))
        }
        0xfb | 0xff => Err(protocol(
            MysqlProxyPhase::ServerResponse,
            MysqlProxyProtocolError::UnsupportedResult,
        )),
    }
}

fn read_u16(payload: &[u8], offset: usize) -> Result<u16, MysqlProxyError> {
    payload
        .get(offset..offset + 2)
        .map(|bytes| {
            u16::from_le_bytes(
                bytes
                    .try_into()
                    .expect("validated two-byte little-endian value"),
            )
        })
        .ok_or_else(|| {
            protocol(
                MysqlProxyPhase::ServerResponse,
                MysqlProxyProtocolError::IncompletePacket,
            )
        })
}

fn read_u32(payload: &[u8], offset: usize) -> Result<u32, MysqlProxyError> {
    payload
        .get(offset..offset + 4)
        .map(|bytes| {
            u32::from_le_bytes(
                bytes
                    .try_into()
                    .expect("validated four-byte little-endian value"),
            )
        })
        .ok_or_else(|| {
            protocol(
                MysqlProxyPhase::ServerResponse,
                MysqlProxyProtocolError::IncompletePacket,
            )
        })
}

fn protocol(phase: MysqlProxyPhase, reason: MysqlProxyProtocolError) -> MysqlProxyError {
    MysqlProxyError::Protocol { phase, reason }
}

struct Packet {
    sequence: u8,
    payload: Vec<u8>,
}

async fn read_packet_cancel<R: AsyncRead + Unpin>(
    reader: &mut R,
    cancel: &mut watch::Receiver<bool>,
    deadline: Instant,
    phase: MysqlProxyPhase,
) -> Result<Packet, MysqlProxyError> {
    select_cancel(cancel, deadline, phase, read_packet(reader, phase)).await?
}

async fn read_packet<R: AsyncRead + Unpin>(
    reader: &mut R,
    phase: MysqlProxyPhase,
) -> Result<Packet, MysqlProxyError> {
    let mut header = [0_u8; 4];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|error| map_read_error(error, phase))?;
    let length =
        usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    if length == MYSQL_CONTINUATION_PAYLOAD {
        return Err(protocol(phase, MysqlProxyProtocolError::ContinuationPacket));
    }
    if length > MAX_PACKET_PAYLOAD {
        return Err(protocol(phase, MysqlProxyProtocolError::PacketTooLarge));
    }
    let mut payload = vec![0_u8; length];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|error| map_read_error(error, phase))?;
    Ok(Packet {
        sequence: header[3],
        payload,
    })
}

fn map_read_error(error: std::io::Error, phase: MysqlProxyPhase) -> MysqlProxyError {
    if error.kind() == ErrorKind::UnexpectedEof {
        protocol(phase, MysqlProxyProtocolError::IncompletePacket)
    } else {
        MysqlProxyError::Io {
            phase,
            kind: error.kind(),
        }
    }
}

async fn write_packet<W: AsyncWrite + Unpin>(
    writer: &mut W,
    sequence: u8,
    payload: &[u8],
    deadline: Instant,
    phase: MysqlProxyPhase,
) -> Result<(), MysqlProxyError> {
    if payload.len() > MAX_PACKET_PAYLOAD {
        return Err(protocol(phase, MysqlProxyProtocolError::PacketTooLarge));
    }
    let length = u32::try_from(payload.len())
        .map_err(|_| protocol(phase, MysqlProxyProtocolError::PacketTooLarge))?;
    let header = [
        length as u8,
        (length >> 8) as u8,
        (length >> 16) as u8,
        sequence,
    ];
    timeout_io(deadline, phase, writer.write_all(&header)).await?;
    timeout_io(deadline, phase, writer.write_all(payload)).await?;
    timeout_io(deadline, phase, writer.flush()).await
}

async fn timeout_io<F>(
    deadline: Instant,
    phase: MysqlProxyPhase,
    future: F,
) -> Result<(), MysqlProxyError>
where
    F: core::future::Future<Output = Result<(), std::io::Error>>,
{
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(MysqlProxyError::Deadline(phase))?;
    tokio::time::timeout(remaining, future)
        .await
        .map_err(|_| MysqlProxyError::Deadline(phase))?
        .map_err(|error| MysqlProxyError::Io {
            phase,
            kind: error.kind(),
        })
}

async fn select_cancel<F, T>(
    cancel: &mut watch::Receiver<bool>,
    deadline: Instant,
    phase: MysqlProxyPhase,
    future: F,
) -> Result<T, MysqlProxyError>
where
    F: core::future::Future<Output = T>,
{
    if *cancel.borrow() {
        return Err(MysqlProxyError::Cancelled);
    }
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(MysqlProxyError::Deadline(phase))?;
    tokio::select! {
        biased;
        changed = cancel.changed() => {
            let _ = changed;
            Err(MysqlProxyError::Cancelled)
        }
        result = tokio::time::timeout(remaining, future) => {
            result.map_err(|_| MysqlProxyError::Deadline(phase))
        }
    }
}

fn definitive_connect_non_creation(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::NotFound
            | ErrorKind::ConnectionRefused
            | ErrorKind::PermissionDenied
            | ErrorKind::InvalidInput
            | ErrorKind::AddrNotAvailable
    )
}

fn phase_timeout(
    attempt_deadline: Instant,
    duration: Duration,
    phase: MysqlProxyPhase,
) -> Result<Instant, MysqlProxyError> {
    bounded_deadline(attempt_deadline, duration).ok_or(MysqlProxyError::Deadline(phase))
}
