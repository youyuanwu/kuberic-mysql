//! Pure capabilities and evidence validation for the bounded three-member flow.

use core::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::adapter::{
    AdapterDiagnostic, ClockContext, ClockError, ObservationClock, ObservationReport,
    ObservationRequest, ObserverCredentials, RequestError, UnixSocketPath,
};
use crate::core::{
    AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, EndpointBinding, Epoch,
    ExactBinding, ExactBindingParts, GroupName, GtidSet, MemberAddress, MemberId, MemberRole,
    MemberState, NativeField, NativeMember, ObservationInstant, ObservationOutcome,
    ObservationProvenance, ObservationSessionId, PartitionId, ProcessSessionId, ReplicaId,
    ReplicaIncarnation, ResourceId, ServerUuid, StorageBinding, UnsupportedReason, ViewId,
};
use crate::service::{
    ControlStage, MemberCleanupFailure, MysqlInstanceConfig, MysqlInstanceError,
    MysqlInstanceManager, MysqlMemberIndex, MysqlTopologyError, MysqlTopologyManagerError,
    TopologyAuthorityError, TopologyEvidenceError, TopologyGtidError, TopologyNativeStateError,
    TopologyStateError,
};

/// One monotonic tick owned by a topology clock.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TopologyInstant(u64);

impl TopologyInstant {
    /// Creates an opaque monotonic instant.
    #[must_use]
    pub const fn new(tick: u64) -> Self {
        Self(tick)
    }
}

/// Monotonic clock used by one topology manager and all of its observations.
pub trait TopologyClock: Send + Sync {
    /// Current manager-clock instant.
    fn now(&self) -> ObservationInstant;

    /// Maps one manager-clock instant into the process monotonic domain.
    fn to_std_instant(&self, instant: ObservationInstant) -> Result<Instant, ClockError>;

    /// Computes one absolute manager-clock deadline from a duration.
    fn deadline_after(
        &self,
        now: ObservationInstant,
        timeout: Duration,
    ) -> Result<ObservationInstant, ClockError>;
}

/// Real process-monotonic clock used by default topology construction.
#[derive(Clone, Debug)]
pub struct SystemTopologyClock {
    origin: Instant,
}

impl SystemTopologyClock {
    /// Starts one process-monotonic clock domain.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemTopologyClock {
    fn default() -> Self {
        Self::new()
    }
}

impl TopologyClock for SystemTopologyClock {
    fn now(&self) -> ObservationInstant {
        let nanos = self.origin.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        ObservationInstant::new(nanos)
    }

    fn to_std_instant(&self, instant: ObservationInstant) -> Result<Instant, ClockError> {
        self.origin
            .checked_add(Duration::from_nanos(instant.tick()))
            .ok_or(ClockError::UnrepresentableInstant)
    }

    fn deadline_after(
        &self,
        now: ObservationInstant,
        timeout: Duration,
    ) -> Result<ObservationInstant, ClockError> {
        let timeout =
            u64::try_from(timeout.as_nanos()).map_err(|_| ClockError::UnrepresentableInstant)?;
        now.tick()
            .checked_add(timeout)
            .map(ObservationInstant::new)
            .ok_or(ClockError::UnrepresentableInstant)
    }
}

#[derive(Clone)]
struct ManagerObservationClock(Arc<dyn TopologyClock>);

impl ObservationClock for ManagerObservationClock {
    fn now(&self) -> ObservationInstant {
        self.0.now()
    }

    fn to_std_instant(&self, instant: ObservationInstant) -> Result<Instant, ClockError> {
        self.0.to_std_instant(instant)
    }
}

impl TopologyInstant {
    /// Returns the opaque tick.
    #[must_use]
    pub const fn tick(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QualifiedXcomView {
    fixed: u64,
    monotonic: u32,
}

impl QualifiedXcomView {
    fn parse(view_id: &ViewId) -> Result<Self, MysqlTopologyError> {
        let Some((fixed, monotonic)) = view_id.as_str().split_once(':') else {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::MalformedViewId,
            ));
        };
        if fixed.is_empty()
            || monotonic.is_empty()
            || monotonic.contains(':')
            || !canonical_decimal(fixed)
            || !canonical_decimal(monotonic)
        {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::MalformedViewId,
            ));
        }
        Ok(Self {
            fixed: fixed.parse().map_err(|_| {
                MysqlTopologyError::TopologyState(TopologyStateError::MalformedViewId)
            })?,
            monotonic: monotonic.parse().map_err(|_| {
                MysqlTopologyError::TopologyState(TopologyStateError::MalformedViewId)
            })?,
        })
    }

    fn require_successor(self, successor: Self) -> Result<(), MysqlTopologyError> {
        if successor.fixed != self.fixed {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::ViewFixedPartChanged,
            ));
        }
        let expected = self
            .monotonic
            .checked_add(1)
            .ok_or(MysqlTopologyError::TopologyState(
                TopologyStateError::ViewSequenceOverflow,
            ))?;
        if successor.monotonic != expected {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::ViewSequenceNotSuccessor,
            ));
        }
        Ok(())
    }
}

fn canonical_decimal(value: &str) -> bool {
    value.bytes().all(|byte| byte.is_ascii_digit()) && (value == "0" || !value.starts_with('0'))
}

/// One positive process-monotonic native-control deadline bound to an attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeControlDeadline {
    attempt: AttemptId,
    deadline: Instant,
}

impl NativeControlDeadline {
    /// Creates a future absolute deadline for one attempt's native operation.
    pub(crate) fn new(attempt: AttemptId, deadline: Instant) -> Result<Self, MysqlTopologyError> {
        if deadline <= Instant::now() {
            return Err(MysqlTopologyError::Deadline(
                ControlStage::ProductValidation,
            ));
        }
        Ok(Self { attempt, deadline })
    }

    /// Bound topology attempt.
    #[must_use]
    pub const fn attempt(&self) -> &AttemptId {
        &self.attempt
    }

    pub(crate) const fn instant(&self) -> Instant {
        self.deadline
    }
}

/// One exact bounded three-member topology attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopologyAttempt {
    id: AttemptId,
    group_name: GroupName,
    designated_bootstrap: MysqlMemberIndex,
    timeout: Duration,
    start: TopologyInstant,
    deadline: TopologyInstant,
}

impl TopologyAttempt {
    /// Creates an attempt with one positive finite duration. The manager binds
    /// the absolute monotonic start and deadline when it takes ownership.
    pub fn new(
        id: AttemptId,
        group_name: GroupName,
        designated_bootstrap: MysqlMemberIndex,
        timeout: Duration,
    ) -> Result<Self, MysqlTopologyError> {
        let timeout_ticks = u64::try_from(timeout.as_nanos())
            .map_err(|_| MysqlTopologyError::Deadline(ControlStage::ProductValidation))?;
        if timeout_ticks == 0 {
            return Err(MysqlTopologyError::Deadline(
                ControlStage::ProductValidation,
            ));
        }
        Ok(Self {
            id,
            group_name,
            designated_bootstrap,
            timeout,
            start: TopologyInstant::new(0),
            deadline: TopologyInstant::new(timeout_ticks),
        })
    }

    /// Attempt identity.
    #[must_use]
    pub const fn id(&self) -> &AttemptId {
        &self.id
    }

    /// Configured Group Replication UUID.
    #[must_use]
    pub const fn group_name(&self) -> &GroupName {
        &self.group_name
    }

    /// Sole member authorized to bootstrap.
    #[must_use]
    pub const fn designated_bootstrap(&self) -> MysqlMemberIndex {
        self.designated_bootstrap
    }

    /// Attempt deadline.
    #[must_use]
    pub const fn deadline(&self) -> TopologyInstant {
        self.deadline
    }

    fn bind_deadline(&mut self, start: ObservationInstant, deadline: ObservationInstant) {
        self.start = TopologyInstant::new(start.tick());
        self.deadline = TopologyInstant::new(deadline.tick());
    }

    fn check_deadline(
        &self,
        now: TopologyInstant,
        stage: ControlStage,
    ) -> Result<(), MysqlTopologyError> {
        if now >= self.deadline {
            Err(MysqlTopologyError::Deadline(stage))
        } else {
            Ok(())
        }
    }
}

/// Credential purpose.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlCredentialRole {
    /// Read-only observation principal.
    Observer,
    /// Distributed-recovery principal.
    Recovery,
}

/// Attempt-bound credential whose secret is redacted and zeroed.
pub struct ControlCredential {
    attempt: AttemptId,
    generation: CredentialGeneration,
    role: ControlCredentialRole,
    username: String,
    password: Box<[u8]>,
}

impl ControlCredential {
    /// Validates an attempt-bound local account credential.
    pub fn new(
        attempt: AttemptId,
        generation: CredentialGeneration,
        role: ControlCredentialRole,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, CredentialError> {
        let username = username.into();
        let password = password.into();
        if username.is_empty()
            || username.len() > 32
            || !username
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(CredentialError::InvalidUsername);
        }
        if password.is_empty()
            || password.len() > 255
            || !password
                .bytes()
                .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'\'' | b'"' | b'\\' | b'`'))
        {
            return Err(CredentialError::InvalidPassword);
        }
        Ok(Self {
            attempt,
            generation,
            role,
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

    /// Credential purpose.
    #[must_use]
    pub const fn role(&self) -> ControlCredentialRole {
        self.role
    }

    /// Non-secret account name.
    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Explicitly clears the retained password before normal drop.
    pub fn clear(&mut self) {
        self.password.fill(0);
    }

    /// Returns whether the retained password buffer has been cleared.
    #[must_use]
    pub fn is_cleared(&self) -> bool {
        self.password.iter().all(|byte| *byte == 0)
    }

    pub(crate) fn password(&self) -> &str {
        std::str::from_utf8(&self.password).expect("credential password originated as UTF-8")
    }
}

impl fmt::Debug for ControlCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlCredential")
            .field("attempt", &self.attempt)
            .field("generation", &self.generation)
            .field("role", &self.role)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

impl Drop for ControlCredential {
    fn drop(&mut self) {
        self.clear();
    }
}

/// Credential construction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialError {
    /// Account name was empty, too long, or outside the restricted local
    /// identifier alphabet.
    InvalidUsername,
    /// Password was empty, too long, or could not be embedded without SQL
    /// quoting ambiguity.
    InvalidPassword,
}

impl fmt::Display for CredentialError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid topology credential: {self:?}")
    }
}

impl std::error::Error for CredentialError {}

/// Secret-free control steps retained as deterministic evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlStep {
    /// Validate exact product identity.
    ValidateProduct,
    /// Read the original session binary-log switch.
    ReadBinaryLogging,
    /// Disable session binary logging.
    DisableBinaryLogging,
    /// Create the observer account.
    CreateObserver,
    /// Grant membership-table read access.
    GrantObserverMembers,
    /// Grant member-statistics read access.
    GrantObserverStats,
    /// Create the recovery account.
    CreateRecovery,
    /// Grant exactly `REPLICATION SLAVE` and `CONNECTION_ADMIN`.
    GrantRecovery,
    /// Restore the original session binary-log switch.
    RestoreBinaryLogging,
    /// Prove the original binary-log switch was restored.
    ProveBinaryLoggingRestored,
    /// Prove no existing Group Replication identity is active or retained.
    ProveFreshGroupState,
    /// Prove no executed transaction history exists.
    ProveFreshExecutedHistory,
    /// Read and freeze `@@server_uuid`.
    EnrollServerIdentity,
    /// Set bootstrap mode on.
    EnableBootstrap,
    /// Start Group Replication with in-memory recovery credentials.
    StartGroupReplication,
    /// Set bootstrap mode off.
    DisableBootstrap,
    /// Prove bootstrap mode is off.
    ProveBootstrapDisabled,
    /// Revalidate the enrolled server/member identity.
    VerifyEnrolledIdentity,
    /// Discover only the proposed current view identity.
    DiscoverView,
}

impl ControlStep {
    /// Secret-free exact SQL contract where the step executes a statement.
    #[must_use]
    pub const fn sql_contract(self) -> Option<&'static str> {
        match self {
            Self::ValidateProduct => Some(
                "SELECT @@version, @@version_comment, @@version_compile_machine, \
                 @@version_compile_os, @@GLOBAL.server_uuid",
            ),
            Self::ReadBinaryLogging => Some("SELECT @@SESSION.sql_log_bin"),
            Self::DisableBinaryLogging => Some("SET SESSION sql_log_bin = OFF"),
            Self::CreateObserver => {
                Some("CREATE USER '<observer>'@'localhost' IDENTIFIED BY '<redacted>'")
            }
            Self::GrantObserverMembers => Some(
                "GRANT SELECT ON performance_schema.replication_group_members \
                 TO '<observer>'@'localhost'",
            ),
            Self::GrantObserverStats => Some(
                "GRANT SELECT ON performance_schema.replication_group_member_stats \
                 TO '<observer>'@'localhost'",
            ),
            Self::CreateRecovery => {
                Some("CREATE USER '<recovery>'@'localhost' IDENTIFIED BY '<redacted>'")
            }
            Self::GrantRecovery => Some(
                "GRANT REPLICATION SLAVE, CONNECTION_ADMIN ON *.* \
                 TO '<recovery>'@'localhost'",
            ),
            Self::RestoreBinaryLogging => {
                Some("SET SESSION sql_log_bin = <original-session-value>")
            }
            Self::ProveBinaryLoggingRestored => Some("SELECT @@SESSION.sql_log_bin"),
            Self::ProveFreshGroupState => Some(
                "SELECT COUNT(*) FROM performance_schema.replication_group_members \
                 WHERE MEMBER_ID IS NOT NULL AND MEMBER_ID <> ''",
            ),
            Self::ProveFreshExecutedHistory => Some("SELECT @@GLOBAL.gtid_executed"),
            Self::EnrollServerIdentity | Self::VerifyEnrolledIdentity => {
                Some("SELECT @@GLOBAL.server_uuid")
            }
            Self::EnableBootstrap => Some("SET GLOBAL group_replication_bootstrap_group = ON"),
            Self::StartGroupReplication => Some(
                "START GROUP_REPLICATION USER='<recovery>', PASSWORD='<redacted>', \
                 DEFAULT_AUTH='caching_sha2_password'",
            ),
            Self::DisableBootstrap => Some("SET GLOBAL group_replication_bootstrap_group = OFF"),
            Self::ProveBootstrapDisabled => {
                Some("SELECT @@GLOBAL.group_replication_bootstrap_group")
            }
            Self::DiscoverView => {
                Some("SELECT the enrolled local member and proposed current VIEW_ID")
            }
        }
    }
}

const ACCOUNT_STEPS: [ControlStep; 10] = [
    ControlStep::ValidateProduct,
    ControlStep::ReadBinaryLogging,
    ControlStep::DisableBinaryLogging,
    ControlStep::CreateObserver,
    ControlStep::GrantObserverMembers,
    ControlStep::GrantObserverStats,
    ControlStep::CreateRecovery,
    ControlStep::GrantRecovery,
    ControlStep::RestoreBinaryLogging,
    ControlStep::ProveBinaryLoggingRestored,
];
const ENROLLMENT_STEPS: [ControlStep; 4] = [
    ControlStep::ValidateProduct,
    ControlStep::ProveFreshGroupState,
    ControlStep::ProveFreshExecutedHistory,
    ControlStep::EnrollServerIdentity,
];
const BOOTSTRAP_STEPS: [ControlStep; 5] = [
    ControlStep::ValidateProduct,
    ControlStep::EnableBootstrap,
    ControlStep::StartGroupReplication,
    ControlStep::DisableBootstrap,
    ControlStep::ProveBootstrapDisabled,
];
const JOIN_STEPS: [ControlStep; 3] = [
    ControlStep::ValidateProduct,
    ControlStep::ProveBootstrapDisabled,
    ControlStep::StartGroupReplication,
];
const DISCOVERY_STEPS: [ControlStep; 3] = [
    ControlStep::ValidateProduct,
    ControlStep::VerifyEnrolledIdentity,
    ControlStep::DiscoverView,
];

/// Exact process/storage/endpoint identity frozen before native mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberControlBinding {
    attempt: AttemptId,
    member: MysqlMemberIndex,
    process_session: ProcessSessionId,
    endpoint: EndpointBinding,
    storage: StorageBinding,
    member_address: MemberAddress,
    observer_generation: CredentialGeneration,
    recovery_generation: CredentialGeneration,
}

impl MemberControlBinding {
    /// Creates one attempt-local member binding.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        attempt: AttemptId,
        member: MysqlMemberIndex,
        process_session: ProcessSessionId,
        endpoint: EndpointBinding,
        storage: StorageBinding,
        member_address: MemberAddress,
        observer_generation: CredentialGeneration,
        recovery_generation: CredentialGeneration,
    ) -> Self {
        Self {
            attempt,
            member,
            process_session,
            endpoint,
            storage,
            member_address,
            observer_generation,
            recovery_generation,
        }
    }

    /// Attempt identity.
    #[must_use]
    pub const fn attempt(&self) -> &AttemptId {
        &self.attempt
    }

    /// Fixed member position.
    #[must_use]
    pub const fn member(&self) -> MysqlMemberIndex {
        self.member
    }

    /// Process-session identity.
    #[must_use]
    pub const fn process_session(&self) -> &ProcessSessionId {
        &self.process_session
    }

    /// Private endpoint binding.
    #[must_use]
    pub const fn endpoint(&self) -> &EndpointBinding {
        &self.endpoint
    }

    /// Persistent storage binding.
    #[must_use]
    pub const fn storage(&self) -> &StorageBinding {
        &self.storage
    }

    /// Expected SQL member address.
    #[must_use]
    pub const fn member_address(&self) -> &MemberAddress {
        &self.member_address
    }

    /// Observer credential generation.
    #[must_use]
    pub const fn observer_generation(&self) -> &CredentialGeneration {
        &self.observer_generation
    }

    /// Recovery credential generation.
    #[must_use]
    pub const fn recovery_generation(&self) -> &CredentialGeneration {
        &self.recovery_generation
    }
}

/// Proof that exact minimum local accounts were created with binary logging
/// restored.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountProvisioningEvidence {
    binding: MemberControlBinding,
    observer_username: String,
    recovery_username: String,
    steps: Vec<ControlStep>,
}

impl AccountProvisioningEvidence {
    /// Required statement/evidence order.
    #[must_use]
    pub const fn required_steps() -> &'static [ControlStep] {
        &ACCOUNT_STEPS
    }

    /// Validates scripted or native provisioning evidence.
    pub fn new(
        binding: MemberControlBinding,
        observer: &ControlCredential,
        recovery: &ControlCredential,
        steps: Vec<ControlStep>,
    ) -> Result<Self, MysqlTopologyError> {
        validate_credential(&binding, observer, ControlCredentialRole::Observer)?;
        validate_credential(&binding, recovery, ControlCredentialRole::Recovery)?;
        if steps != ACCOUNT_STEPS {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::InvalidTransition,
            ));
        }
        Ok(Self {
            binding,
            observer_username: observer.username.clone(),
            recovery_username: recovery.username.clone(),
            steps,
        })
    }

    /// Exact member binding.
    #[must_use]
    pub const fn binding(&self) -> &MemberControlBinding {
        &self.binding
    }

    /// Secret-free completed steps.
    #[must_use]
    pub fn steps(&self) -> &[ControlStep] {
        &self.steps
    }
}

/// Pre-effect native identity enrollment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeIdentityEnrollment {
    binding: MemberControlBinding,
    server_uuid: ServerUuid,
    member_id: MemberId,
    observer_username: String,
    recovery_username: String,
    steps: Vec<ControlStep>,
}

impl NativeIdentityEnrollment {
    /// Required identity-enrollment order.
    #[must_use]
    pub const fn required_steps() -> &'static [ControlStep] {
        &ENROLLMENT_STEPS
    }

    /// Freezes one fresh server UUID after account setup and before mutation.
    pub fn new(
        binding: MemberControlBinding,
        server_uuid: ServerUuid,
        accounts: &AccountProvisioningEvidence,
        fresh_group_state: bool,
        executed: GtidSet,
        steps: Vec<ControlStep>,
    ) -> Result<Self, MysqlTopologyError> {
        if accounts.binding != binding {
            return Err(MysqlTopologyError::Evidence(
                TopologyEvidenceError::BindingMismatch,
            ));
        }
        if !fresh_group_state {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::ExistingGroupState,
            ));
        }
        if !executed.entries().is_empty() {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::ExistingTransactionHistory,
            ));
        }
        if steps != ENROLLMENT_STEPS {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::InvalidTransition,
            ));
        }
        let member_id = MemberId::new(server_uuid.as_str())
            .map_err(|_| MysqlTopologyError::Evidence(TopologyEvidenceError::IdentityDrift))?;
        Ok(Self {
            binding,
            server_uuid,
            member_id,
            observer_username: accounts.observer_username.clone(),
            recovery_username: accounts.recovery_username.clone(),
            steps,
        })
    }

    /// Exact frozen binding.
    #[must_use]
    pub const fn binding(&self) -> &MemberControlBinding {
        &self.binding
    }

    /// Enrolled native server UUID.
    #[must_use]
    pub const fn server_uuid(&self) -> &ServerUuid {
        &self.server_uuid
    }

    /// Enrolled Group Replication member UUID.
    #[must_use]
    pub const fn member_id(&self) -> &MemberId {
        &self.member_id
    }

    pub(crate) fn recovery_username(&self) -> &str {
        &self.recovery_username
    }
}

/// Single issued bootstrap capability.
#[derive(Debug, Eq, PartialEq)]
pub struct BootstrapCapability {
    attempt: TopologyAttempt,
    target: NativeIdentityEnrollment,
    nonce: u64,
}

impl BootstrapCapability {
    /// Records the exact effect trace. Bootstrap-off proof is mandatory.
    pub fn record_effect(
        self,
        steps: Vec<ControlStep>,
    ) -> Result<BootstrapEffect, MysqlTopologyError> {
        if steps != BOOTSTRAP_STEPS {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::InvalidTransition,
            ));
        }
        Ok(BootstrapEffect {
            capability: self,
            steps,
            observation_attempt: None,
        })
    }

    pub(crate) const fn attempt(&self) -> &TopologyAttempt {
        &self.attempt
    }

    pub(crate) const fn target(&self) -> &NativeIdentityEnrollment {
        &self.target
    }
}

/// Bootstrap effect that proves bootstrap mode was disabled afterward.
#[derive(Debug, Eq, PartialEq)]
pub struct BootstrapEffect {
    capability: BootstrapCapability,
    steps: Vec<ControlStep>,
    observation_attempt: Option<AttemptId>,
}

impl BootstrapEffect {
    /// Required native control order.
    #[must_use]
    pub const fn required_steps() -> &'static [ControlStep] {
        &BOOTSTRAP_STEPS
    }

    /// Secret-free completed steps.
    #[must_use]
    pub fn steps(&self) -> &[ControlStep] {
        &self.steps
    }

    /// Binds the one fresh post-effect observation attempt.
    #[must_use]
    pub fn bind_observation_attempt(mut self, attempt: AttemptId) -> Self {
        self.observation_attempt = Some(attempt);
        self
    }

    /// Rebinds the same authorized bootstrap operation to a new observation
    /// attempt. This is required after a prior observation reported pending.
    pub fn rebind_observation_attempt(&mut self, attempt: AttemptId) {
        self.observation_attempt = Some(attempt);
    }
}

/// Source transaction boundary captured from the accepted predecessor view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceGtidBoundary {
    attempt: AttemptId,
    source: NativeIdentityEnrollment,
    predecessor_observation_attempt: AttemptId,
    observation_attempt: AttemptId,
    view_id: ViewId,
    executed: GtidSet,
}

impl SourceGtidBoundary {
    /// Captured executed set.
    #[must_use]
    pub const fn executed(&self) -> &GtidSet {
        &self.executed
    }

    /// Fresh observer attempt that captured this source boundary.
    #[must_use]
    pub const fn observation_attempt(&self) -> &AttemptId {
        &self.observation_attempt
    }
}

/// Sequential non-bootstrap join capability.
#[derive(Debug, Eq, PartialEq)]
pub struct JoinCapability {
    attempt: TopologyAttempt,
    target: NativeIdentityEnrollment,
    predecessor: TransitionCredit,
    boundary: SourceGtidBoundary,
    nonce: u64,
}

impl JoinCapability {
    /// Fresh source boundary authorized for this exact join.
    #[must_use]
    pub const fn boundary(&self) -> &SourceGtidBoundary {
        &self.boundary
    }

    /// Records the exact non-bootstrap join trace.
    pub fn record_effect(self, steps: Vec<ControlStep>) -> Result<JoinEffect, MysqlTopologyError> {
        if steps != JOIN_STEPS {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::InvalidTransition,
            ));
        }
        Ok(JoinEffect {
            capability: self,
            steps,
            observation_attempt: None,
        })
    }

    pub(crate) const fn attempt(&self) -> &TopologyAttempt {
        &self.attempt
    }

    pub(crate) const fn target(&self) -> &NativeIdentityEnrollment {
        &self.target
    }
}

/// One entered non-bootstrap join effect.
#[derive(Debug, Eq, PartialEq)]
pub struct JoinEffect {
    capability: JoinCapability,
    steps: Vec<ControlStep>,
    observation_attempt: Option<AttemptId>,
}

impl JoinEffect {
    /// Required native control order.
    #[must_use]
    pub const fn required_steps() -> &'static [ControlStep] {
        &JOIN_STEPS
    }

    /// Binds the one fresh post-effect observation attempt.
    #[must_use]
    pub fn bind_observation_attempt(mut self, attempt: AttemptId) -> Self {
        self.observation_attempt = Some(attempt);
        self
    }

    /// Rebinds the same authorized join operation to a new observation
    /// attempt. This is required after a prior observation reported pending.
    pub fn rebind_observation_attempt(&mut self, attempt: AttemptId) {
        self.observation_attempt = Some(attempt);
    }
}

/// Exact local identity and ownership values reported with post-effect evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedLocalBinding {
    attempt: AttemptId,
    member: MysqlMemberIndex,
    process_session: ProcessSessionId,
    endpoint: EndpointBinding,
    storage: StorageBinding,
    server_uuid: ServerUuid,
    member_id: MemberId,
    member_address: MemberAddress,
    observer_generation: CredentialGeneration,
}

impl ObservedLocalBinding {
    /// Creates explicit observed local binding evidence.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        attempt: AttemptId,
        member: MysqlMemberIndex,
        process_session: ProcessSessionId,
        endpoint: EndpointBinding,
        storage: StorageBinding,
        server_uuid: ServerUuid,
        member_id: MemberId,
        member_address: MemberAddress,
        observer_generation: CredentialGeneration,
    ) -> Self {
        Self {
            attempt,
            member,
            process_session,
            endpoint,
            storage,
            server_uuid,
            member_id,
            member_address,
            observer_generation,
        }
    }

    /// Creates matching evidence from an enrollment.
    #[must_use]
    pub fn from_enrollment(enrollment: &NativeIdentityEnrollment) -> Self {
        Self {
            attempt: enrollment.binding.attempt.clone(),
            member: enrollment.binding.member,
            process_session: enrollment.binding.process_session.clone(),
            endpoint: enrollment.binding.endpoint.clone(),
            storage: enrollment.binding.storage.clone(),
            server_uuid: enrollment.server_uuid.clone(),
            member_id: enrollment.member_id.clone(),
            member_address: enrollment.binding.member_address.clone(),
            observer_generation: enrollment.binding.observer_generation.clone(),
        }
    }
}

/// Collection status before topology-specific transition validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopologyObservationStatus {
    /// Complete product-compatible observation.
    Complete,
    /// A required observation was absent or partial.
    Missing,
    /// Product identity was outside the qualified profile.
    ProductMismatch,
    /// Ownership context was unavailable.
    OwnershipContextLoss,
    /// Native role text was unsupported.
    UnsupportedRole,
    /// Native state text was unsupported.
    UnsupportedState,
}

/// Fresh proposed post-effect topology evidence. Construction grants no credit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopologyObservation {
    status: TopologyObservationStatus,
    observation_attempt: AttemptId,
    local: ObservedLocalBinding,
    group_name: GroupName,
    binding_view_id: ViewId,
    view_id: ViewId,
    members: Vec<NativeMember>,
    executed: GtidSet,
    decision: TopologyInstant,
}

impl TopologyObservation {
    /// Creates explicit proposed evidence for pure validation.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        status: TopologyObservationStatus,
        observation_attempt: AttemptId,
        local: ObservedLocalBinding,
        group_name: GroupName,
        binding_view_id: ViewId,
        view_id: ViewId,
        mut members: Vec<NativeMember>,
        executed: GtidSet,
        decision: TopologyInstant,
    ) -> Self {
        members.sort_by(|left, right| left.id().as_str().cmp(right.id().as_str()));
        Self {
            status,
            observation_attempt,
            local,
            group_name,
            binding_view_id,
            view_id,
            members,
            executed,
            decision,
        }
    }
}

/// Discovery result that can propose only a new view identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ViewDiscovery {
    enrollment: NativeIdentityEnrollment,
    group_name: GroupName,
    view_id: ViewId,
    steps: Vec<ControlStep>,
}

impl ViewDiscovery {
    /// Required discovery order.
    #[must_use]
    pub const fn required_steps() -> &'static [ControlStep] {
        &DISCOVERY_STEPS
    }

    /// Creates discovery evidence without granting lifecycle credit.
    pub fn new(
        enrollment: NativeIdentityEnrollment,
        group_name: GroupName,
        view_id: ViewId,
        steps: Vec<ControlStep>,
    ) -> Result<Self, MysqlTopologyError> {
        if steps != DISCOVERY_STEPS {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::InvalidTransition,
            ));
        }
        Ok(Self {
            enrollment,
            group_name,
            view_id,
            steps,
        })
    }

    /// Proposed native view identity.
    #[must_use]
    pub const fn view_id(&self) -> &ViewId {
        &self.view_id
    }

    /// Immutable identity enrollment retained by discovery.
    #[must_use]
    pub const fn enrollment(&self) -> &NativeIdentityEnrollment {
        &self.enrollment
    }

    /// Configured group identity retained by discovery.
    #[must_use]
    pub const fn group_name(&self) -> &GroupName {
        &self.group_name
    }
}

/// Accepted lifecycle evidence. It never opens client access.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionCredit {
    attempt: AttemptId,
    observation_attempt: AttemptId,
    view_id: ViewId,
    members: Vec<NativeIdentityEnrollment>,
    executed: GtidSet,
}

impl TransitionCredit {
    /// Accepted view.
    #[must_use]
    pub const fn view_id(&self) -> &ViewId {
        &self.view_id
    }

    /// Observation attempt that supplied accepted predecessor evidence.
    #[must_use]
    pub const fn observation_attempt(&self) -> &AttemptId {
        &self.observation_attempt
    }

    /// Accepted exact member enrollments.
    #[must_use]
    pub fn members(&self) -> &[NativeIdentityEnrollment] {
        &self.members
    }

    /// Executed history accepted for this view.
    #[must_use]
    pub const fn executed(&self) -> &GtidSet {
        &self.executed
    }

    /// Constructs a join boundary only from a distinct, fresh observation of
    /// one exact member in this accepted predecessor view.
    pub fn source_gtid_boundary(
        &self,
        topology_attempt: &TopologyAttempt,
        source: &NativeIdentityEnrollment,
        evidence: &TopologyObservation,
        now: TopologyInstant,
    ) -> Result<SourceGtidBoundary, MysqlTopologyError> {
        topology_attempt.check_deadline(now, ControlStage::DiscoverView)?;
        if self.attempt != *topology_attempt.id()
            || !self.members.contains(source)
            || evidence.observation_attempt == self.observation_attempt
        {
            return Err(MysqlTopologyError::Gtid(
                TopologyGtidError::SourceBoundaryBindingMismatch,
            ));
        }
        validate_source_observation(topology_attempt, self, source, evidence)?;
        Ok(SourceGtidBoundary {
            attempt: self.attempt.clone(),
            source: source.clone(),
            predecessor_observation_attempt: self.observation_attempt.clone(),
            observation_attempt: evidence.observation_attempt.clone(),
            view_id: self.view_id.clone(),
            executed: evidence.executed.clone(),
        })
    }
}

/// Transition evaluation without implied retry or fallback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionEvaluation {
    /// `RECOVERING` is valid progress but grants no lifecycle credit.
    Pending,
    /// Exact fresh `ONLINE` evidence grants one lifecycle credit.
    Accepted(TransitionCredit),
}

/// Pure fixed-topology authority and sequential-transition state.
///
/// This grants bootstrap/join lifecycle credit only. Its read and write access
/// projections are permanently closed.
#[derive(Debug)]
pub struct TopologyAuthority {
    attempt: TopologyAttempt,
    enrollments: [Option<NativeIdentityEnrollment>; 3],
    state: AuthorityState,
    next_nonce: u64,
    lifecycle_credits: u8,
}

#[derive(Debug)]
enum AuthorityState {
    Enrolling,
    BootstrapIssued(u64),
    Accepted(TransitionCredit),
    JoinIssued(u64, TransitionCredit),
    Complete(TransitionCredit),
    Invalidated,
}

impl TopologyAuthority {
    /// Creates a closed-access authority session.
    #[must_use]
    pub fn new(attempt: TopologyAttempt) -> Self {
        Self {
            attempt,
            enrollments: std::array::from_fn(|_| None),
            state: AuthorityState::Enrolling,
            next_nonce: 1,
            lifecycle_credits: 0,
        }
    }

    /// Registers one pre-effect exact enrollment.
    pub fn register_enrollment(
        &mut self,
        enrollment: NativeIdentityEnrollment,
    ) -> Result<(), MysqlTopologyError> {
        if enrollment.binding.attempt != self.attempt.id {
            return Err(MysqlTopologyError::Authority(
                TopologyAuthorityError::AttemptMismatch,
            ));
        }
        if self.enrollments.iter().flatten().any(|existing| {
            existing.binding.process_session == enrollment.binding.process_session
                || existing.binding.endpoint == enrollment.binding.endpoint
                || existing.binding.storage == enrollment.binding.storage
                || existing.binding.member_address == enrollment.binding.member_address
                || existing.server_uuid == enrollment.server_uuid
        }) {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::DuplicateEnrollment,
            ));
        }
        if self.enrollments.iter().flatten().any(|existing| {
            existing.observer_username != enrollment.observer_username
                || existing.recovery_username != enrollment.recovery_username
                || existing.binding.observer_generation != enrollment.binding.observer_generation
                || existing.binding.recovery_generation != enrollment.binding.recovery_generation
        }) {
            return Err(MysqlTopologyError::Authority(
                TopologyAuthorityError::PrincipalMismatch,
            ));
        }
        let slot = &mut self.enrollments[enrollment.binding.member.as_usize()];
        if slot.is_some() {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::DuplicateEnrollment,
            ));
        }
        *slot = Some(enrollment);
        Ok(())
    }

    /// Issues bootstrap authority once, only to the designated enrolled member.
    pub fn authorize_bootstrap(
        &mut self,
        member: MysqlMemberIndex,
        now: TopologyInstant,
    ) -> Result<BootstrapCapability, MysqlTopologyError> {
        self.attempt
            .check_deadline(now, ControlStage::EnableBootstrap)?;
        if member != self.attempt.designated_bootstrap {
            return Err(MysqlTopologyError::Authority(
                TopologyAuthorityError::WrongBootstrapMember,
            ));
        }
        if !matches!(self.state, AuthorityState::Enrolling) {
            return Err(MysqlTopologyError::Authority(
                TopologyAuthorityError::BootstrapAlreadyUsed,
            ));
        }
        let target =
            self.enrollments[member.as_usize()]
                .clone()
                .ok_or(MysqlTopologyError::Authority(
                    TopologyAuthorityError::MissingEnrollment,
                ))?;
        let nonce = self.take_nonce();
        self.state = AuthorityState::BootstrapIssued(nonce);
        Ok(BootstrapCapability {
            attempt: self.attempt.clone(),
            target,
            nonce,
        })
    }

    /// Validates fresh bootstrap evidence. Pending evidence grants no credit.
    pub fn accept_bootstrap(
        &mut self,
        effect: &BootstrapEffect,
        evidence: &TopologyObservation,
        now: TopologyInstant,
    ) -> Result<TransitionEvaluation, MysqlTopologyError> {
        let AuthorityState::BootstrapIssued(nonce) = &self.state else {
            return Err(MysqlTopologyError::Authority(
                TopologyAuthorityError::StaleCapability,
            ));
        };
        if *nonce != effect.capability.nonce {
            return Err(MysqlTopologyError::Authority(
                TopologyAuthorityError::StaleCapability,
            ));
        }
        if effect.observation_attempt.as_ref() != Some(&evidence.observation_attempt) {
            return Err(MysqlTopologyError::Evidence(
                TopologyEvidenceError::BindingMismatch,
            ));
        }
        let evaluation = validate_transition(
            &self.attempt,
            &effect.capability.target,
            None,
            None,
            evidence,
            now,
        )?;
        if let TransitionEvaluation::Accepted(credit) = &evaluation {
            self.lifecycle_credits = self.lifecycle_credits.saturating_add(1);
            self.state = AuthorityState::Accepted(credit.clone());
        }
        Ok(evaluation)
    }

    /// Issues only the next sequential join after accepted predecessor credit.
    pub fn authorize_join(
        &mut self,
        target: MysqlMemberIndex,
        boundary: SourceGtidBoundary,
        now: TopologyInstant,
    ) -> Result<JoinCapability, MysqlTopologyError> {
        self.attempt
            .check_deadline(now, ControlStage::StartGroupReplication)?;
        let predecessor = match &self.state {
            AuthorityState::Accepted(credit) => credit.clone(),
            AuthorityState::JoinIssued(_, _) => {
                return Err(MysqlTopologyError::Authority(
                    TopologyAuthorityError::TransitionInFlight,
                ));
            }
            AuthorityState::Complete(_) => {
                return Err(MysqlTopologyError::TopologyState(
                    TopologyStateError::InvalidTransition,
                ));
            }
            AuthorityState::Enrolling
            | AuthorityState::BootstrapIssued(_)
            | AuthorityState::Invalidated => {
                return Err(MysqlTopologyError::TopologyState(
                    TopologyStateError::InvalidTransition,
                ));
            }
        };
        let expected = next_target(&self.attempt, predecessor.members.len()).ok_or(
            MysqlTopologyError::TopologyState(TopologyStateError::InvalidTransition),
        )?;
        if target != expected {
            return Err(MysqlTopologyError::Authority(
                TopologyAuthorityError::WrongJoinTarget,
            ));
        }
        let target =
            self.enrollments[target.as_usize()]
                .clone()
                .ok_or(MysqlTopologyError::Authority(
                    TopologyAuthorityError::MissingEnrollment,
                ))?;
        validate_boundary(&self.attempt, &predecessor, &boundary)?;
        let nonce = self.take_nonce();
        self.state = AuthorityState::JoinIssued(nonce, predecessor.clone());
        Ok(JoinCapability {
            attempt: self.attempt.clone(),
            target,
            predecessor,
            boundary,
            nonce,
        })
    }

    /// Validates fresh join evidence and advances only after `ONLINE` credit.
    pub fn accept_join(
        &mut self,
        effect: &JoinEffect,
        evidence: &TopologyObservation,
        now: TopologyInstant,
    ) -> Result<TransitionEvaluation, MysqlTopologyError> {
        let AuthorityState::JoinIssued(nonce, predecessor) = &self.state else {
            return Err(MysqlTopologyError::Authority(
                TopologyAuthorityError::StaleCapability,
            ));
        };
        if *nonce != effect.capability.nonce || predecessor != &effect.capability.predecessor {
            return Err(MysqlTopologyError::Authority(
                TopologyAuthorityError::StaleCapability,
            ));
        }
        if effect.observation_attempt.as_ref() != Some(&evidence.observation_attempt) {
            return Err(MysqlTopologyError::Evidence(
                TopologyEvidenceError::BindingMismatch,
            ));
        }
        let evaluation = validate_transition(
            &self.attempt,
            &effect.capability.target,
            Some(&effect.capability.predecessor),
            Some(&effect.capability.boundary),
            evidence,
            now,
        )?;
        if let TransitionEvaluation::Accepted(credit) = &evaluation {
            self.lifecycle_credits = self.lifecycle_credits.saturating_add(1);
            self.state = if credit.members.len() == 3 {
                AuthorityState::Complete(credit.clone())
            } else {
                AuthorityState::Accepted(credit.clone())
            };
        }
        Ok(evaluation)
    }

    /// Number of accepted bootstrap/join transitions.
    #[must_use]
    pub const fn lifecycle_credits(&self) -> u8 {
        self.lifecycle_credits
    }

    /// Client read access remains closed for this complete phase.
    #[must_use]
    pub const fn read_access_open(&self) -> bool {
        false
    }

    /// Client write access remains closed for this complete phase.
    #[must_use]
    pub const fn write_access_open(&self) -> bool {
        false
    }

    /// Final accepted credit, available only after three members are accepted.
    #[must_use]
    pub fn final_credit(&self) -> Option<&TransitionCredit> {
        if let AuthorityState::Complete(credit) = &self.state {
            Some(credit)
        } else {
            None
        }
    }

    fn current_credit(&self) -> Option<&TransitionCredit> {
        match &self.state {
            AuthorityState::Accepted(credit) | AuthorityState::Complete(credit) => Some(credit),
            AuthorityState::Enrolling
            | AuthorityState::BootstrapIssued(_)
            | AuthorityState::JoinIssued(_, _)
            | AuthorityState::Invalidated => None,
        }
    }

    pub(crate) fn invalidate(&mut self) {
        self.state = AuthorityState::Invalidated;
        self.enrollments.fill(None);
    }

    fn take_nonce(&mut self) -> u64 {
        let nonce = self.next_nonce;
        self.next_nonce = self.next_nonce.saturating_add(1);
        nonce
    }
}

/// Explicit restart-stateless lifecycle of one fixed three-member manager.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MysqlTopologyState {
    /// Three configured instance managers are owned but no roots exist.
    Configured,
    /// All three fresh layouts were initialized.
    Initialized,
    /// The designated member was started and enrolled before bootstrap.
    BootstrapMemberEnrolled,
    /// Bootstrap ran and one exact fresh observation is required.
    BootstrapObservationPending,
    /// Bootstrap received accepted one-member lifecycle credit.
    BootstrapAccepted,
    /// The second member was started and enrolled before joining.
    SecondMemberEnrolled,
    /// The second join ran and one exact fresh observation is required.
    SecondObservationPending,
    /// The second join received accepted two-member lifecycle credit.
    SecondAccepted,
    /// The third member was started and enrolled before joining.
    ThirdMemberEnrolled,
    /// The third join ran and one exact fresh observation is required.
    ThirdObservationPending,
    /// Exact fresh three-member evidence was accepted.
    Complete,
    /// A failure invalidated capabilities and topology progress.
    Failed,
    /// Every exactly owned member was contained and disposable scratch removed.
    Stopped,
}

/// Caller-owned Kuberic identity context for one manager-created observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopologyObservationContext {
    resource: ResourceId,
    partition: PartitionId,
    replica: ReplicaId,
    incarnation: ReplicaIncarnation,
    observation_session: ObservationSessionId,
    configuration: ConfigurationId,
    epoch: Epoch,
    authority_generation: AuthorityGeneration,
    origin: String,
}

impl TopologyObservationContext {
    /// Creates the non-native identity context for one exact observation.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        resource: ResourceId,
        partition: PartitionId,
        replica: ReplicaId,
        incarnation: ReplicaIncarnation,
        observation_session: ObservationSessionId,
        configuration: ConfigurationId,
        epoch: Epoch,
        authority_generation: AuthorityGeneration,
        origin: impl Into<String>,
    ) -> Self {
        Self {
            resource,
            partition,
            replica,
            incarnation,
            observation_session,
            configuration,
            epoch,
            authority_generation,
            origin: origin.into(),
        }
    }
}

/// Final accepted exact three-member evidence. Client access remains closed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedMysqlTopology {
    credit: TransitionCredit,
}

impl AcceptedMysqlTopology {
    /// Exact accepted transition credit.
    #[must_use]
    pub const fn credit(&self) -> &TransitionCredit {
        &self.credit
    }

    /// Ordinary client reads are not published by this manager.
    #[must_use]
    pub const fn read_access_open(&self) -> bool {
        false
    }

    /// Ordinary client writes are not published by this manager.
    #[must_use]
    pub const fn write_access_open(&self) -> bool {
        false
    }
}

#[derive(Debug)]
enum PendingEffect {
    Bootstrap {
        effect: Box<BootstrapEffect>,
        last_observation: Option<(AttemptId, ObservationInstant)>,
    },
    Join {
        effect: Box<JoinEffect>,
        last_observation: Option<(AttemptId, ObservationInstant)>,
    },
}

impl PendingEffect {
    fn bind_fresh_observation(
        &mut self,
        attempt: AttemptId,
        deadline: ObservationInstant,
    ) -> Result<(), MysqlTopologyManagerError> {
        let last_observation = match self {
            Self::Bootstrap {
                effect,
                last_observation,
            } => {
                effect.rebind_observation_attempt(attempt.clone());
                last_observation
            }
            Self::Join {
                effect,
                last_observation,
            } => {
                effect.rebind_observation_attempt(attempt.clone());
                last_observation
            }
        };
        if last_observation
            .as_ref()
            .is_some_and(|(prior_attempt, prior_deadline)| {
                prior_attempt == &attempt || deadline <= *prior_deadline
            })
        {
            return Err(MysqlTopologyManagerError::Topology(
                MysqlTopologyError::Evidence(TopologyEvidenceError::BindingMismatch),
            ));
        }
        *last_observation = Some((attempt, deadline));
        Ok(())
    }
}

/// One member dependency used by [`MysqlTopologyManager`].
///
/// Production uses [`MysqlInstanceManager`]. Deterministic tests may supply a
/// scripted implementation while exercising the same public manager methods.
#[allow(async_fn_in_trait)]
pub trait MysqlTopologyMemberRuntime {
    /// Immutable validated member configuration.
    fn config(&self) -> &MysqlInstanceConfig;

    /// Initializes one fresh member generation.
    fn initialize(&mut self) -> Result<(), MysqlInstanceError>;

    /// Starts and attests one exact member process.
    fn start(&mut self) -> Result<(), MysqlInstanceError>;

    /// Freezes the exact process, endpoint, storage, and address binding.
    fn topology_binding(
        &mut self,
        attempt: AttemptId,
        observer_generation: CredentialGeneration,
        recovery_generation: CredentialGeneration,
    ) -> Result<MemberControlBinding, MysqlTopologyError>;

    /// Provisions the exact topology accounts.
    async fn provision_topology_accounts(
        &mut self,
        binding: MemberControlBinding,
        observer: &ControlCredential,
        recovery: &ControlCredential,
        deadline: &NativeControlDeadline,
    ) -> Result<AccountProvisioningEvidence, MysqlTopologyError>;

    /// Enrolls the exact pre-effect native identity.
    async fn enroll_topology_identity(
        &mut self,
        binding: MemberControlBinding,
        accounts: &AccountProvisioningEvidence,
        deadline: &NativeControlDeadline,
    ) -> Result<NativeIdentityEnrollment, MysqlTopologyError>;

    /// Executes the designated bootstrap effect.
    ///
    /// Implementations must synchronously contain their exact owned generation
    /// if this future is dropped before bootstrap-off proof completes.
    async fn bootstrap_group_replication(
        &mut self,
        capability: BootstrapCapability,
        recovery: &ControlCredential,
        deadline: &NativeControlDeadline,
    ) -> Result<BootstrapEffect, MysqlTopologyError>;

    /// Executes one non-bootstrap join effect.
    async fn join_group_replication(
        &mut self,
        capability: JoinCapability,
        recovery: &ControlCredential,
        deadline: &NativeControlDeadline,
    ) -> Result<JoinEffect, MysqlTopologyError>;

    /// Discovers the proposed post-effect view.
    async fn discover_topology_view(
        &mut self,
        enrollment: NativeIdentityEnrollment,
        group_name: GroupName,
        deadline: &NativeControlDeadline,
    ) -> Result<ViewDiscovery, MysqlTopologyError>;

    /// Performs one exact read-only observer attempt.
    async fn observe<C: ObservationClock>(
        &mut self,
        request: ObservationRequest<C>,
    ) -> Result<ObservationReport, MysqlInstanceError>;

    /// Contains the exact owned process and disposable resources.
    fn contain(&mut self) -> Result<(), MysqlInstanceError>;
}

impl MysqlTopologyMemberRuntime for MysqlInstanceManager {
    fn config(&self) -> &MysqlInstanceConfig {
        MysqlInstanceManager::config(self)
    }

    fn initialize(&mut self) -> Result<(), MysqlInstanceError> {
        MysqlInstanceManager::initialize(self)
    }

    fn start(&mut self) -> Result<(), MysqlInstanceError> {
        MysqlInstanceManager::start(self)
    }

    fn topology_binding(
        &mut self,
        attempt: AttemptId,
        observer_generation: CredentialGeneration,
        recovery_generation: CredentialGeneration,
    ) -> Result<MemberControlBinding, MysqlTopologyError> {
        MysqlInstanceManager::topology_binding(
            self,
            attempt,
            observer_generation,
            recovery_generation,
        )
    }

    async fn provision_topology_accounts(
        &mut self,
        binding: MemberControlBinding,
        observer: &ControlCredential,
        recovery: &ControlCredential,
        deadline: &NativeControlDeadline,
    ) -> Result<AccountProvisioningEvidence, MysqlTopologyError> {
        MysqlInstanceManager::provision_topology_accounts(
            self, binding, observer, recovery, deadline,
        )
        .await
    }

    async fn enroll_topology_identity(
        &mut self,
        binding: MemberControlBinding,
        accounts: &AccountProvisioningEvidence,
        deadline: &NativeControlDeadline,
    ) -> Result<NativeIdentityEnrollment, MysqlTopologyError> {
        MysqlInstanceManager::enroll_topology_identity(self, binding, accounts, deadline).await
    }

    async fn bootstrap_group_replication(
        &mut self,
        capability: BootstrapCapability,
        recovery: &ControlCredential,
        deadline: &NativeControlDeadline,
    ) -> Result<BootstrapEffect, MysqlTopologyError> {
        MysqlInstanceManager::bootstrap_group_replication(self, capability, recovery, deadline)
            .await
    }

    async fn join_group_replication(
        &mut self,
        capability: JoinCapability,
        recovery: &ControlCredential,
        deadline: &NativeControlDeadline,
    ) -> Result<JoinEffect, MysqlTopologyError> {
        MysqlInstanceManager::join_group_replication(self, capability, recovery, deadline).await
    }

    async fn discover_topology_view(
        &mut self,
        enrollment: NativeIdentityEnrollment,
        group_name: GroupName,
        deadline: &NativeControlDeadline,
    ) -> Result<ViewDiscovery, MysqlTopologyError> {
        MysqlInstanceManager::discover_topology_view(self, enrollment, group_name, deadline).await
    }

    async fn observe<C: ObservationClock>(
        &mut self,
        request: ObservationRequest<C>,
    ) -> Result<ObservationReport, MysqlInstanceError> {
        MysqlInstanceManager::observe(self, request).await
    }

    fn contain(&mut self) -> Result<(), MysqlInstanceError> {
        MysqlInstanceManager::contain(self)
    }
}

fn contain_members<M: MysqlTopologyMemberRuntime>(
    instances: &mut [M; 3],
) -> Vec<MemberCleanupFailure> {
    let mut failures = Vec::new();
    for member in MysqlMemberIndex::all() {
        if let Err(error) = instances[member.as_usize()].contain() {
            failures.push(MemberCleanupFailure::new(member, error));
        }
    }
    failures
}

struct TopologyCancellationContainment<'a, M: MysqlTopologyMemberRuntime> {
    instances: &'a mut [M; 3],
    state: &'a mut MysqlTopologyState,
    armed: bool,
}

impl<'a, M: MysqlTopologyMemberRuntime> TopologyCancellationContainment<'a, M> {
    fn new(instances: &'a mut [M; 3], state: &'a mut MysqlTopologyState) -> Self {
        *state = MysqlTopologyState::Failed;
        Self {
            instances,
            state,
            armed: true,
        }
    }

    fn member(&mut self, member: MysqlMemberIndex) -> &mut M {
        &mut self.instances[member.as_usize()]
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<M: MysqlTopologyMemberRuntime> Drop for TopologyCancellationContainment<'_, M> {
    fn drop(&mut self) {
        if self.armed {
            let _ = contain_members(self.instances);
            *self.state = MysqlTopologyState::Failed;
        }
    }
}

fn validate_member_paths<M: MysqlTopologyMemberRuntime>(
    instances: &[M; 3],
) -> Result<(), MysqlTopologyManagerError> {
    for left_member in MysqlMemberIndex::all() {
        for right_member in MysqlMemberIndex::all()
            .into_iter()
            .skip(left_member.as_usize() + 1)
        {
            for (left_kind, left_path) in instances[left_member.as_usize()].config().owned_paths() {
                for (right_kind, right_path) in
                    instances[right_member.as_usize()].config().owned_paths()
                {
                    if left_path == right_path
                        || left_path.starts_with(right_path)
                        || right_path.starts_with(left_path)
                    {
                        return Err(MysqlTopologyManagerError::PathConflict {
                            left_member,
                            left_kind,
                            right_member,
                            right_kind,
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

/// Owns exactly three fresh instance generations and their in-memory topology
/// attempt.
///
/// The only successful order is initialize all members, start/enroll the
/// designated bootstrap member, accept bootstrap observation, then
/// start/enroll/join and accept each remaining member sequentially. Any
/// operation or evidence failure invalidates the attempt and contains every
/// exactly owned member. The manager has no callback, adoption, resume,
/// publication, switchover, repair, or durable-store surface.
/// Dropping the bootstrap operation at either native-effect or discovery await
/// point synchronously contains all exact members and leaves the manager
/// failed. All operation times and deadlines come from the manager clock.
pub struct MysqlTopologyManager<M: MysqlTopologyMemberRuntime = MysqlInstanceManager> {
    instances: [M; 3],
    authority: TopologyAuthority,
    state: MysqlTopologyState,
    observer: ControlCredential,
    recovery: ControlCredential,
    enrollments: [Option<NativeIdentityEnrollment>; 3],
    pending_effect: Option<PendingEffect>,
    pending_discovery: Option<ViewDiscovery>,
    accepted: Option<AcceptedMysqlTopology>,
    observation_serial: u64,
    clock: Arc<dyn TopologyClock>,
    observation_deadline: ObservationInstant,
    runtime_deadline: Instant,
    last_now: TopologyInstant,
}

#[cfg(test)]
pub(super) struct QualificationManagerParts<'a, M: MysqlTopologyMemberRuntime> {
    pub(super) instances: &'a mut [M; 3],
    pub(super) attempt: &'a TopologyAttempt,
    pub(super) observer: &'a ControlCredential,
    pub(super) recovery: &'a ControlCredential,
    pub(super) runtime_deadline: Instant,
}

impl<M: MysqlTopologyMemberRuntime> MysqlTopologyManager<M> {
    /// Creates one fixed manager from exactly three topology-specific instance
    /// managers using a real process-monotonic clock.
    pub fn new(
        instances: [M; 3],
        attempt: TopologyAttempt,
        observer: ControlCredential,
        recovery: ControlCredential,
    ) -> Result<Self, MysqlTopologyManagerError> {
        Self::new_with_clock(
            instances,
            attempt,
            observer,
            recovery,
            Arc::new(SystemTopologyClock::new()),
        )
    }

    /// Creates one manager with an injectable monotonic clock for deterministic
    /// contracts. The manager derives one absolute attempt deadline and rejects
    /// regression, future-dated mappings, and unrepresentable clock values.
    pub fn new_with_clock(
        mut instances: [M; 3],
        mut attempt: TopologyAttempt,
        observer: ControlCredential,
        recovery: ControlCredential,
        clock: Arc<dyn TopologyClock>,
    ) -> Result<Self, MysqlTopologyManagerError> {
        validate_member_paths(&instances)?;
        let start = clock.now();
        let mapped_start = clock.to_std_instant(start).map_err(|_| {
            MysqlTopologyManagerError::Topology(MysqlTopologyError::Evidence(
                TopologyEvidenceError::UnrepresentableClock,
            ))
        })?;
        if mapped_start > Instant::now() {
            return Err(MysqlTopologyManagerError::Topology(
                MysqlTopologyError::Evidence(TopologyEvidenceError::FutureDatedClock),
            ));
        }
        let observation_deadline = clock.deadline_after(start, attempt.timeout).map_err(|_| {
            MysqlTopologyManagerError::Topology(MysqlTopologyError::Evidence(
                TopologyEvidenceError::UnrepresentableClock,
            ))
        })?;
        if observation_deadline <= start {
            return Err(MysqlTopologyManagerError::Topology(
                MysqlTopologyError::Deadline(ControlStage::ProductValidation),
            ));
        }
        let runtime_deadline = clock.to_std_instant(observation_deadline).map_err(|_| {
            MysqlTopologyManagerError::Topology(MysqlTopologyError::Evidence(
                TopologyEvidenceError::UnrepresentableClock,
            ))
        })?;
        if runtime_deadline <= Instant::now() {
            return Err(MysqlTopologyManagerError::Topology(
                MysqlTopologyError::Deadline(ControlStage::ProductValidation),
            ));
        }
        attempt.bind_deadline(start, observation_deadline);
        let validation_error = if observer.attempt() != attempt.id()
            || recovery.attempt() != attempt.id()
            || observer.role() != ControlCredentialRole::Observer
            || recovery.role() != ControlCredentialRole::Recovery
        {
            Some(MysqlTopologyManagerError::Topology(
                MysqlTopologyError::Authority(TopologyAuthorityError::AttemptMismatch),
            ))
        } else {
            let topology = instances[0].config().topology();
            if topology.is_none()
                || instances.iter().enumerate().any(|(index, instance)| {
                    instance.config().member_index().as_usize() != index
                        || instance.config().topology() != topology
                        || instance.config().member().group_uuid() != attempt.group_name().as_str()
                })
            {
                Some(MysqlTopologyManagerError::Topology(
                    MysqlTopologyError::TopologyState(
                        TopologyStateError::TopologyConfigurationRequired,
                    ),
                ))
            } else {
                None
            }
        };
        if let Some(primary) = validation_error {
            let failures = contain_members(&mut instances);
            return if failures.is_empty() {
                Err(primary)
            } else {
                Err(MysqlTopologyManagerError::Cleanup {
                    primary: Some(Box::new(primary)),
                    failures,
                })
            };
        }
        Ok(Self {
            instances,
            authority: TopologyAuthority::new(attempt),
            state: MysqlTopologyState::Configured,
            observer,
            recovery,
            enrollments: std::array::from_fn(|_| None),
            pending_effect: None,
            pending_discovery: None,
            accepted: None,
            observation_serial: 0,
            clock,
            observation_deadline,
            runtime_deadline,
            last_now: TopologyInstant::new(start.tick()),
        })
    }

    /// Current restart-stateless manager state.
    #[must_use]
    pub const fn state(&self) -> MysqlTopologyState {
        self.state
    }

    /// Exact owned member managers in fixed topology order.
    #[must_use]
    pub const fn members(&self) -> &[M; 3] {
        &self.instances
    }

    /// Accepted final topology, available only in `Complete`.
    #[must_use]
    pub const fn accepted_topology(&self) -> Option<&AcceptedMysqlTopology> {
        self.accepted.as_ref()
    }

    /// Ordinary client reads remain closed in every state.
    #[must_use]
    pub const fn read_access_open(&self) -> bool {
        false
    }

    /// Ordinary client writes remain closed in every state.
    #[must_use]
    pub const fn write_access_open(&self) -> bool {
        false
    }

    #[cfg(test)]
    pub(super) fn enter_native_qualification(
        &mut self,
    ) -> Result<QualificationManagerParts<'_, M>, MysqlTopologyManagerError> {
        if self.state != MysqlTopologyState::Complete {
            return Err(MysqlTopologyManagerError::InvalidState {
                expected: MysqlTopologyState::Complete,
                actual: self.state,
            });
        }
        self.pending_effect = None;
        self.pending_discovery = None;
        self.accepted = None;
        self.enrollments.fill(None);
        self.authority.invalidate();
        self.state = MysqlTopologyState::Failed;
        Ok(QualificationManagerParts {
            instances: &mut self.instances,
            attempt: &self.authority.attempt,
            observer: &self.observer,
            recovery: &self.recovery,
            runtime_deadline: self.runtime_deadline,
        })
    }

    #[cfg(test)]
    pub(super) fn mark_complete_for_qualification_contract(&mut self) {
        self.state = MysqlTopologyState::Complete;
    }

    #[cfg(test)]
    pub(super) fn qualification_authority_is_invalidated(&self) -> bool {
        matches!(self.authority.state, AuthorityState::Invalidated)
    }

    /// Initializes all three fresh members before any member is started.
    pub fn initialize(&mut self) -> Result<(), MysqlTopologyManagerError> {
        self.require_state_or_fail(MysqlTopologyState::Configured)?;
        for member in MysqlMemberIndex::all() {
            if let Err(error) = self.instances[member.as_usize()].initialize() {
                return Err(self.fail(MysqlTopologyManagerError::Instance { member, error }));
            }
        }
        self.state = MysqlTopologyState::Initialized;
        Ok(())
    }

    /// Starts, provisions, and enrolls the designated member before bootstrap.
    pub async fn start_designated_member(&mut self) -> Result<(), MysqlTopologyManagerError> {
        self.require_state_or_fail(MysqlTopologyState::Initialized)?;
        let deadline = match self.control_deadline(ControlStage::ProductValidation) {
            Ok(deadline) => deadline,
            Err(error) => return Err(self.fail(error)),
        };
        let member = self.authority.attempt.designated_bootstrap();
        self.start_and_enroll(member, &deadline).await?;
        self.state = MysqlTopologyState::BootstrapMemberEnrolled;
        Ok(())
    }

    /// Enters the sole designated bootstrap and discovers its proposed view.
    pub async fn bootstrap(&mut self) -> Result<(), MysqlTopologyManagerError> {
        self.require_state_or_fail(MysqlTopologyState::BootstrapMemberEnrolled)?;
        let now = match self.manager_now(ControlStage::EnableBootstrap) {
            Ok(now) => now,
            Err(error) => return Err(self.fail(error)),
        };
        let deadline = match self.control_deadline(ControlStage::EnableBootstrap) {
            Ok(deadline) => deadline,
            Err(error) => return Err(self.fail(error)),
        };
        let member = self.authority.attempt.designated_bootstrap();
        let capability = match self.authority.authorize_bootstrap(member, now) {
            Ok(capability) => capability,
            Err(error) => {
                return Err(self.fail(MysqlTopologyManagerError::Topology(error)));
            }
        };
        let enrollment = match self.enrollment(member) {
            Ok(enrollment) => enrollment.clone(),
            Err(error) => return Err(self.fail(error)),
        };
        let group_name = self.authority.attempt.group_name().clone();
        let mut containment =
            TopologyCancellationContainment::new(&mut self.instances, &mut self.state);
        let result = async {
            let effect = containment
                .member(member)
                .bootstrap_group_replication(capability, &self.recovery, &deadline)
                .await
                .map_err(MysqlTopologyManagerError::Topology)?;
            let discovery = containment
                .member(member)
                .discover_topology_view(enrollment, group_name, &deadline)
                .await
                .map_err(MysqlTopologyManagerError::Topology)?;
            Ok((effect, discovery))
        }
        .await;
        containment.disarm();
        drop(containment);
        match result {
            Ok((effect, discovery)) => {
                self.install_pending(
                    PendingEffect::Bootstrap {
                        effect: Box::new(effect),
                        last_observation: None,
                    },
                    discovery,
                    MysqlTopologyState::BootstrapObservationPending,
                );
                Ok(())
            }
            Err(error) => Err(self.fail(error)),
        }
    }

    /// Starts, provisions, and enrolls the fixed second join target.
    pub async fn start_second_member(&mut self) -> Result<(), MysqlTopologyManagerError> {
        self.require_state_or_fail(MysqlTopologyState::BootstrapAccepted)?;
        let deadline = match self.control_deadline(ControlStage::ProductValidation) {
            Ok(deadline) => deadline,
            Err(error) => return Err(self.fail(error)),
        };
        let member = self.join_order()[0];
        self.start_and_enroll(member, &deadline).await?;
        self.state = MysqlTopologyState::SecondMemberEnrolled;
        Ok(())
    }

    /// Enters the second member's join from the exact accepted bootstrap credit.
    pub async fn join_second_member(
        &mut self,
        source_context: TopologyObservationContext,
    ) -> Result<(), MysqlTopologyManagerError> {
        self.require_state_or_fail(MysqlTopologyState::SecondMemberEnrolled)?;
        self.join_member(self.join_order()[0], source_context)
            .await?;
        Ok(())
    }

    /// Starts, provisions, and enrolls the fixed third join target.
    pub async fn start_third_member(&mut self) -> Result<(), MysqlTopologyManagerError> {
        self.require_state_or_fail(MysqlTopologyState::SecondAccepted)?;
        let deadline = match self.control_deadline(ControlStage::ProductValidation) {
            Ok(deadline) => deadline,
            Err(error) => return Err(self.fail(error)),
        };
        let member = self.join_order()[1];
        self.start_and_enroll(member, &deadline).await?;
        self.state = MysqlTopologyState::ThirdMemberEnrolled;
        Ok(())
    }

    /// Enters the third member's join from the exact accepted second credit.
    pub async fn join_third_member(
        &mut self,
        source_context: TopologyObservationContext,
    ) -> Result<(), MysqlTopologyManagerError> {
        self.require_state_or_fail(MysqlTopologyState::ThirdMemberEnrolled)?;
        self.join_member(self.join_order()[1], source_context)
            .await?;
        Ok(())
    }

    /// Collects and evaluates the one exact fresh observer attempt for the
    /// currently pending bootstrap or join.
    pub async fn observe_pending(
        &mut self,
        context: TopologyObservationContext,
    ) -> Result<TransitionEvaluation, MysqlTopologyManagerError> {
        if !matches!(
            self.state,
            MysqlTopologyState::BootstrapObservationPending
                | MysqlTopologyState::SecondObservationPending
                | MysqlTopologyState::ThirdObservationPending
        ) {
            let error = MysqlTopologyManagerError::InvalidState {
                expected: MysqlTopologyState::BootstrapObservationPending,
                actual: self.state,
            };
            return Err(self.fail(error));
        }
        let observation_attempt = match self.next_observation_attempt() {
            Ok(attempt) => attempt,
            Err(error) => return Err(self.fail(error)),
        };
        let observation_started = match self.manager_now(ControlStage::DiscoverView) {
            Ok(now) => now,
            Err(error) => return Err(self.fail(error)),
        };
        if let Err(error) = self
            .pending_effect
            .as_mut()
            .expect("pending state retains one effect")
            .bind_fresh_observation(
                observation_attempt.clone(),
                ObservationInstant::new(observation_started.tick()),
            )
        {
            return Err(self.fail(error));
        }
        let result = self
            .observe_pending_inner(context, observation_attempt)
            .await;
        let evidence = match result {
            Ok(evidence) => evidence,
            Err(error) => return Err(self.fail(error)),
        };
        self.accept_pending_evidence(&evidence, evidence.decision)
    }

    /// Contains every exact member and reports every cleanup failure.
    pub fn stop(&mut self) -> Result<(), MysqlTopologyManagerError> {
        let failures = self.contain_all();
        self.invalidate_context();
        if failures.is_empty() {
            self.state = MysqlTopologyState::Stopped;
            Ok(())
        } else {
            self.state = MysqlTopologyState::Failed;
            Err(MysqlTopologyManagerError::Cleanup {
                primary: None,
                failures,
            })
        }
    }

    async fn start_and_enroll(
        &mut self,
        member: MysqlMemberIndex,
        deadline: &NativeControlDeadline,
    ) -> Result<(), MysqlTopologyManagerError> {
        let result = async {
            self.instances[member.as_usize()]
                .start()
                .map_err(|error| MysqlTopologyManagerError::Instance { member, error })?;
            let binding = self.instances[member.as_usize()]
                .topology_binding(
                    self.authority.attempt.id().clone(),
                    self.observer.generation().clone(),
                    self.recovery.generation().clone(),
                )
                .map_err(MysqlTopologyManagerError::Topology)?;
            let accounts = self.instances[member.as_usize()]
                .provision_topology_accounts(
                    binding.clone(),
                    &self.observer,
                    &self.recovery,
                    deadline,
                )
                .await
                .map_err(MysqlTopologyManagerError::Topology)?;
            let enrollment = self.instances[member.as_usize()]
                .enroll_topology_identity(binding, &accounts, deadline)
                .await
                .map_err(MysqlTopologyManagerError::Topology)?;
            self.record_enrollment(member, enrollment)?;
            Ok(())
        }
        .await;
        result.map_err(|error| self.fail(error))
    }

    async fn join_member(
        &mut self,
        member: MysqlMemberIndex,
        source_context: TopologyObservationContext,
    ) -> Result<(), MysqlTopologyManagerError> {
        let result = async {
            let predecessor = self
                .authority
                .current_credit()
                .ok_or(MysqlTopologyManagerError::Topology(
                    MysqlTopologyError::TopologyState(TopologyStateError::InvalidTransition),
                ))?
                .clone();
            let source = predecessor.members().last().cloned().ok_or(
                MysqlTopologyManagerError::Topology(MysqlTopologyError::Evidence(
                    TopologyEvidenceError::Missing,
                )),
            )?;
            let source_attempt = self.next_observation_attempt()?;
            let source_evidence = self
                .observe_enrollment(
                    source_context,
                    source_attempt,
                    &source,
                    predecessor.view_id(),
                )
                .await?;
            let now = source_evidence.decision;
            let boundary = predecessor
                .source_gtid_boundary(&self.authority.attempt, &source, &source_evidence, now)
                .map_err(MysqlTopologyManagerError::Topology)?;
            let capability = self
                .authority
                .authorize_join(member, boundary, now)
                .map_err(MysqlTopologyManagerError::Topology)?;
            let deadline = self.control_deadline(ControlStage::StartGroupReplication)?;
            let effect = self.instances[member.as_usize()]
                .join_group_replication(capability, &self.recovery, &deadline)
                .await
                .map_err(MysqlTopologyManagerError::Topology)?;
            let enrollment = self.enrollment(member)?.clone();
            let discovery = self.instances[member.as_usize()]
                .discover_topology_view(
                    enrollment,
                    self.authority.attempt.group_name().clone(),
                    &deadline,
                )
                .await
                .map_err(MysqlTopologyManagerError::Topology)?;
            let pending_state = if member == self.join_order()[0] {
                MysqlTopologyState::SecondObservationPending
            } else {
                MysqlTopologyState::ThirdObservationPending
            };
            self.install_pending(
                PendingEffect::Join {
                    effect: Box::new(effect),
                    last_observation: None,
                },
                discovery,
                pending_state,
            );
            Ok(())
        }
        .await;
        result.map_err(|error| self.fail(error))
    }

    async fn observe_pending_inner(
        &mut self,
        context: TopologyObservationContext,
        observation_attempt: AttemptId,
    ) -> Result<TopologyObservation, MysqlTopologyManagerError> {
        let discovery =
            self.pending_discovery
                .as_ref()
                .ok_or(MysqlTopologyManagerError::Topology(
                    MysqlTopologyError::Evidence(TopologyEvidenceError::Missing),
                ))?;
        let enrollment = discovery.enrollment().clone();
        let group_name = discovery.group_name().clone();
        let view_id = discovery.view_id().clone();
        self.observe_enrollment(context, observation_attempt, &enrollment, &view_id)
            .await
            .and_then(|observation| {
                if observation.group_name != group_name {
                    Err(MysqlTopologyManagerError::Topology(
                        MysqlTopologyError::Evidence(TopologyEvidenceError::GroupMismatch),
                    ))
                } else {
                    Ok(observation)
                }
            })
    }

    async fn observe_enrollment(
        &mut self,
        context: TopologyObservationContext,
        observation_attempt: AttemptId,
        enrollment: &NativeIdentityEnrollment,
        binding_view_id: &ViewId,
    ) -> Result<TopologyObservation, MysqlTopologyManagerError> {
        let binding = ExactBinding::new(ExactBindingParts {
            resource: context.resource,
            partition: context.partition,
            replica: context.replica,
            incarnation: context.incarnation,
            process_session: enrollment.binding().process_session().clone(),
            observation_session: context.observation_session,
            attempt: observation_attempt.clone(),
            endpoint: enrollment.binding().endpoint().clone(),
            storage: enrollment.binding().storage().clone(),
            server_uuid: enrollment.server_uuid().clone(),
            group_name: self.authority.attempt.group_name().clone(),
            member_id: enrollment.member_id().clone(),
            member_address: enrollment.binding().member_address().clone(),
            configuration: context.configuration,
            epoch: context.epoch,
            authority_generation: context.authority_generation,
            view_id: binding_view_id.clone(),
            credential_generation: enrollment.binding().observer_generation().clone(),
        });
        let expected_binding = binding.clone();
        let provenance = ObservationProvenance::new(context.origin, observation_attempt.clone())
            .map_err(|_| {
                MysqlTopologyManagerError::ObservationRequest(RequestError::AttemptMismatch)
            })?;
        let socket = UnixSocketPath::new(
            self.instances[enrollment.binding().member().as_usize()]
                .config()
                .runtime()
                .socket(),
        )
        .map_err(|_| {
            MysqlTopologyManagerError::Topology(MysqlTopologyError::OwnershipContextLoss)
        })?;
        let credentials =
            ObserverCredentials::new(self.observer.username(), self.observer.password())
                .map_err(MysqlTopologyManagerError::ObservationRequest)?;
        let clock = ClockContext::new(
            ManagerObservationClock(Arc::clone(&self.clock)),
            self.observation_deadline,
        )
        .map_err(|_| {
            MysqlTopologyManagerError::Topology(MysqlTopologyError::Evidence(
                TopologyEvidenceError::UnrepresentableClock,
            ))
        })?;
        let request = ObservationRequest::new(binding, socket, credentials, provenance, clock)
            .map_err(MysqlTopologyManagerError::ObservationRequest)?;
        let report = self.instances[enrollment.binding().member().as_usize()]
            .observe(request)
            .await
            .map_err(|error| MysqlTopologyManagerError::Instance {
                member: enrollment.binding().member(),
                error,
            })?;
        let now = self.manager_now(ControlStage::DiscoverView)?;
        topology_observation(
            report.outcome(),
            report.diagnostic(),
            &expected_binding,
            observation_attempt,
            enrollment,
            self.authority.attempt.group_name(),
            binding_view_id,
            now,
        )
        .map_err(MysqlTopologyManagerError::Topology)
    }

    fn accept_pending_evidence(
        &mut self,
        evidence: &TopologyObservation,
        now: TopologyInstant,
    ) -> Result<TransitionEvaluation, MysqlTopologyManagerError> {
        let evaluation = match self
            .pending_effect
            .as_ref()
            .expect("pending effect retained until evaluation")
        {
            PendingEffect::Bootstrap { effect, .. } => self
                .authority
                .accept_bootstrap(effect, evidence, now)
                .map_err(MysqlTopologyManagerError::Topology),
            PendingEffect::Join { effect, .. } => self
                .authority
                .accept_join(effect, evidence, now)
                .map_err(MysqlTopologyManagerError::Topology),
        };
        let evaluation = match evaluation {
            Ok(evaluation) => evaluation,
            Err(error) => return Err(self.fail(error)),
        };
        if let TransitionEvaluation::Accepted(credit) = &evaluation {
            self.pending_effect = None;
            self.pending_discovery = None;
            self.state = match credit.members().len() {
                1 => MysqlTopologyState::BootstrapAccepted,
                2 => MysqlTopologyState::SecondAccepted,
                3 => {
                    self.accepted = Some(AcceptedMysqlTopology {
                        credit: credit.clone(),
                    });
                    MysqlTopologyState::Complete
                }
                _ => {
                    return Err(self.fail(MysqlTopologyManagerError::Topology(
                        MysqlTopologyError::TopologyState(TopologyStateError::InvalidTransition),
                    )));
                }
            };
        }
        Ok(evaluation)
    }

    fn record_enrollment(
        &mut self,
        member: MysqlMemberIndex,
        enrollment: NativeIdentityEnrollment,
    ) -> Result<(), MysqlTopologyManagerError> {
        if enrollment.binding().member() != member {
            return Err(MysqlTopologyManagerError::Topology(
                MysqlTopologyError::Evidence(TopologyEvidenceError::BindingMismatch),
            ));
        }
        self.authority
            .register_enrollment(enrollment.clone())
            .map_err(MysqlTopologyManagerError::Topology)?;
        self.enrollments[member.as_usize()] = Some(enrollment);
        Ok(())
    }

    fn install_pending(
        &mut self,
        effect: PendingEffect,
        discovery: ViewDiscovery,
        state: MysqlTopologyState,
    ) {
        self.pending_effect = Some(effect);
        self.pending_discovery = Some(discovery);
        self.state = state;
    }

    fn enrollment(
        &self,
        member: MysqlMemberIndex,
    ) -> Result<&NativeIdentityEnrollment, MysqlTopologyManagerError> {
        self.enrollments[member.as_usize()]
            .as_ref()
            .ok_or(MysqlTopologyManagerError::Topology(
                MysqlTopologyError::Authority(TopologyAuthorityError::MissingEnrollment),
            ))
    }

    fn next_observation_attempt(&mut self) -> Result<AttemptId, MysqlTopologyManagerError> {
        self.observation_serial =
            self.observation_serial
                .checked_add(1)
                .ok_or(MysqlTopologyManagerError::Topology(
                    MysqlTopologyError::Evidence(TopologyEvidenceError::BindingMismatch),
                ))?;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.authority.attempt.id().hash(&mut hasher);
        AttemptId::new(format!(
            "topology-observation-{:016x}-{}",
            hasher.finish(),
            self.observation_serial
        ))
        .map_err(|_| {
            MysqlTopologyManagerError::Topology(MysqlTopologyError::Evidence(
                TopologyEvidenceError::BindingMismatch,
            ))
        })
    }

    fn manager_now(
        &mut self,
        stage: ControlStage,
    ) -> Result<TopologyInstant, MysqlTopologyManagerError> {
        let observed = self.clock.now();
        let mapped = self.clock.to_std_instant(observed).map_err(|_| {
            MysqlTopologyManagerError::Topology(MysqlTopologyError::Evidence(
                TopologyEvidenceError::UnrepresentableClock,
            ))
        })?;
        if mapped > Instant::now() {
            return Err(MysqlTopologyManagerError::Topology(
                MysqlTopologyError::Evidence(TopologyEvidenceError::FutureDatedClock),
            ));
        }
        let now = TopologyInstant::new(observed.tick());
        if now < self.last_now {
            return Err(MysqlTopologyManagerError::Topology(
                MysqlTopologyError::Evidence(TopologyEvidenceError::ClockRegression),
            ));
        }
        self.authority
            .attempt
            .check_deadline(now, stage)
            .map_err(MysqlTopologyManagerError::Topology)?;
        self.last_now = now;
        Ok(now)
    }

    fn control_deadline(
        &mut self,
        stage: ControlStage,
    ) -> Result<NativeControlDeadline, MysqlTopologyManagerError> {
        self.manager_now(stage)?;
        if self.runtime_deadline <= Instant::now() {
            return Err(MysqlTopologyManagerError::Topology(
                MysqlTopologyError::Deadline(stage),
            ));
        }
        NativeControlDeadline::new(self.authority.attempt.id().clone(), self.runtime_deadline)
            .map_err(MysqlTopologyManagerError::Topology)
    }

    fn join_order(&self) -> [MysqlMemberIndex; 2] {
        let mut remaining = MysqlMemberIndex::all()
            .into_iter()
            .filter(|member| *member != self.authority.attempt.designated_bootstrap());
        [
            remaining.next().expect("fixed topology has second member"),
            remaining.next().expect("fixed topology has third member"),
        ]
    }

    fn require_state(&self, expected: MysqlTopologyState) -> Result<(), MysqlTopologyManagerError> {
        if self.state == expected {
            Ok(())
        } else {
            Err(MysqlTopologyManagerError::InvalidState {
                expected,
                actual: self.state,
            })
        }
    }

    fn require_state_or_fail(
        &mut self,
        expected: MysqlTopologyState,
    ) -> Result<(), MysqlTopologyManagerError> {
        match self.require_state(expected) {
            Ok(()) => Ok(()),
            Err(error) => Err(self.fail(error)),
        }
    }

    fn fail(&mut self, primary: MysqlTopologyManagerError) -> MysqlTopologyManagerError {
        self.invalidate_context();
        self.state = MysqlTopologyState::Failed;
        let failures = self.contain_all();
        if failures.is_empty() {
            primary
        } else {
            MysqlTopologyManagerError::Cleanup {
                primary: Some(Box::new(primary)),
                failures,
            }
        }
    }

    fn contain_all(&mut self) -> Vec<MemberCleanupFailure> {
        contain_members(&mut self.instances)
    }

    fn invalidate_context(&mut self) {
        self.authority.invalidate();
        self.pending_effect = None;
        self.pending_discovery = None;
        self.accepted = None;
        self.enrollments.fill(None);
        self.observer.clear();
        self.recovery.clear();
    }
}

impl<M: MysqlTopologyMemberRuntime> Drop for MysqlTopologyManager<M> {
    fn drop(&mut self) {
        self.invalidate_context();
        let _ = self.contain_all();
    }
}

#[allow(clippy::too_many_arguments)]
fn topology_observation(
    outcome: &ObservationOutcome,
    diagnostic: &AdapterDiagnostic,
    expected_binding: &ExactBinding,
    observation_attempt: AttemptId,
    enrollment: &NativeIdentityEnrollment,
    group_name: &GroupName,
    binding_view_id: &ViewId,
    decision: TopologyInstant,
) -> Result<TopologyObservation, MysqlTopologyError> {
    let actual_binding = outcome.metadata().binding();
    if actual_binding != expected_binding
        || outcome.metadata().provenance().attempt() != &observation_attempt
    {
        let actual = actual_binding.parts();
        let expected = expected_binding.parts();
        let error = if actual.server_uuid != expected.server_uuid
            || actual.member_id != expected.member_id
            || actual.member_address != expected.member_address
        {
            TopologyEvidenceError::IdentityDrift
        } else if actual.process_session != expected.process_session
            || actual.endpoint != expected.endpoint
            || actual.storage != expected.storage
        {
            TopologyEvidenceError::OwnershipDrift
        } else if actual.group_name != expected.group_name {
            TopologyEvidenceError::GroupMismatch
        } else {
            TopologyEvidenceError::BindingMismatch
        };
        return Err(MysqlTopologyError::Evidence(error));
    }
    if let AdapterDiagnostic::UnexpectedIdentity { field } = diagnostic {
        let error = match field {
            crate::adapter::IdentityField::GroupName => TopologyEvidenceError::GroupMismatch,
            crate::adapter::IdentityField::ViewId => TopologyEvidenceError::BindingMismatch,
            crate::adapter::IdentityField::ServerUuid
            | crate::adapter::IdentityField::LocalMemberId => TopologyEvidenceError::IdentityDrift,
        };
        return Err(MysqlTopologyError::Evidence(error));
    }
    let status = match outcome {
        ObservationOutcome::Valid(_) => TopologyObservationStatus::Complete,
        ObservationOutcome::Unsupported {
            reason: UnsupportedReason::NativeValue(NativeField::Role),
            ..
        } => TopologyObservationStatus::UnsupportedRole,
        ObservationOutcome::Unsupported {
            reason: UnsupportedReason::NativeValue(NativeField::State),
            ..
        } => TopologyObservationStatus::UnsupportedState,
        _ if matches!(diagnostic, AdapterDiagnostic::Product(_)) => {
            TopologyObservationStatus::ProductMismatch
        }
        _ => TopologyObservationStatus::Missing,
    };
    let (view_id, members, executed) = outcome.valid().map_or_else(
        || (binding_view_id.clone(), Vec::new(), GtidSet::empty()),
        |valid| {
            (
                valid.view().id().clone(),
                valid.view().members().to_vec(),
                valid.executed().clone(),
            )
        },
    );
    Ok(TopologyObservation::new(
        status,
        observation_attempt,
        ObservedLocalBinding::from_enrollment(enrollment),
        group_name.clone(),
        binding_view_id.clone(),
        view_id,
        members,
        executed,
        decision,
    ))
}

fn validate_credential(
    binding: &MemberControlBinding,
    credential: &ControlCredential,
    role: ControlCredentialRole,
) -> Result<(), MysqlTopologyError> {
    if credential.attempt != binding.attempt {
        return Err(MysqlTopologyError::Authority(
            TopologyAuthorityError::AttemptMismatch,
        ));
    }
    let expected = match role {
        ControlCredentialRole::Observer => &binding.observer_generation,
        ControlCredentialRole::Recovery => &binding.recovery_generation,
    };
    if credential.role != role || &credential.generation != expected || credential.is_cleared() {
        return Err(MysqlTopologyError::Authority(
            TopologyAuthorityError::CredentialGenerationMismatch,
        ));
    }
    Ok(())
}

fn validate_source_observation(
    attempt: &TopologyAttempt,
    predecessor: &TransitionCredit,
    source: &NativeIdentityEnrollment,
    evidence: &TopologyObservation,
) -> Result<(), MysqlTopologyError> {
    if evidence.decision >= attempt.deadline {
        return Err(MysqlTopologyError::Deadline(ControlStage::DiscoverView));
    }
    if evidence.status != TopologyObservationStatus::Complete {
        return Err(match evidence.status {
            TopologyObservationStatus::Missing => {
                MysqlTopologyError::Evidence(TopologyEvidenceError::Missing)
            }
            TopologyObservationStatus::ProductMismatch => {
                MysqlTopologyError::Evidence(TopologyEvidenceError::ProductMismatch)
            }
            TopologyObservationStatus::OwnershipContextLoss => {
                MysqlTopologyError::Evidence(TopologyEvidenceError::OwnershipContextLoss)
            }
            TopologyObservationStatus::UnsupportedRole => {
                MysqlTopologyError::NativeState(TopologyNativeStateError::UnsupportedRole)
            }
            TopologyObservationStatus::UnsupportedState => {
                MysqlTopologyError::NativeState(TopologyNativeStateError::UnsupportedState)
            }
            TopologyObservationStatus::Complete => unreachable!(),
        });
    }
    if evidence.local.attempt != attempt.id
        || evidence.group_name != attempt.group_name
        || evidence.binding_view_id != predecessor.view_id
        || evidence.view_id != predecessor.view_id
    {
        return Err(MysqlTopologyError::Gtid(
            TopologyGtidError::SourceBoundaryBindingMismatch,
        ));
    }
    validate_local_binding(source, &evidence.local)?;
    validate_accepted_members(attempt, &predecessor.members, &evidence.members)?;
    validate_group_sources(attempt, &evidence.executed)?;
    if !predecessor.executed.is_subset_of(&evidence.executed) {
        return Err(MysqlTopologyError::Gtid(
            TopologyGtidError::SourceBoundaryNotContained,
        ));
    }
    Ok(())
}

fn validate_boundary(
    attempt: &TopologyAttempt,
    predecessor: &TransitionCredit,
    boundary: &SourceGtidBoundary,
) -> Result<(), MysqlTopologyError> {
    if boundary.attempt != attempt.id
        || predecessor.attempt != attempt.id
        || boundary.predecessor_observation_attempt != predecessor.observation_attempt
        || boundary.observation_attempt == predecessor.observation_attempt
        || boundary.view_id != predecessor.view_id
        || !predecessor.members.contains(&boundary.source)
    {
        return Err(MysqlTopologyError::Gtid(
            TopologyGtidError::SourceBoundaryBindingMismatch,
        ));
    }
    validate_group_sources(attempt, &boundary.executed)
}

fn validate_accepted_members(
    attempt: &TopologyAttempt,
    expected: &[NativeIdentityEnrollment],
    actual: &[NativeMember],
) -> Result<(), MysqlTopologyError> {
    for enrollment in expected {
        let member = actual
            .iter()
            .find(|member| member.id() == enrollment.member_id())
            .ok_or(MysqlTopologyError::TopologyState(
                TopologyStateError::RequiredMemberMissing,
            ))?;
        if member.address() != enrollment.binding().member_address() {
            return Err(MysqlTopologyError::Evidence(
                TopologyEvidenceError::IdentityDrift,
            ));
        }
        let expected_role = if enrollment.binding().member == attempt.designated_bootstrap {
            MemberRole::Primary
        } else {
            MemberRole::Secondary
        };
        if member.role() != expected_role {
            return Err(MysqlTopologyError::NativeState(
                TopologyNativeStateError::PredecessorRoleChanged,
            ));
        }
        if member.state() != MemberState::Online {
            return Err(MysqlTopologyError::NativeState(
                TopologyNativeStateError::PredecessorNotOnline,
            ));
        }
    }
    if actual.len() != expected.len() {
        return Err(MysqlTopologyError::TopologyState(
            TopologyStateError::UnexpectedMember,
        ));
    }
    Ok(())
}

fn validate_transition(
    attempt: &TopologyAttempt,
    target: &NativeIdentityEnrollment,
    predecessor: Option<&TransitionCredit>,
    boundary: Option<&SourceGtidBoundary>,
    evidence: &TopologyObservation,
    now: TopologyInstant,
) -> Result<TransitionEvaluation, MysqlTopologyError> {
    attempt.check_deadline(now, ControlStage::DiscoverView)?;
    if evidence.decision >= attempt.deadline {
        return Err(MysqlTopologyError::Deadline(ControlStage::DiscoverView));
    }
    match evidence.status {
        TopologyObservationStatus::Complete => {}
        TopologyObservationStatus::Missing => {
            return Err(MysqlTopologyError::Evidence(TopologyEvidenceError::Missing));
        }
        TopologyObservationStatus::ProductMismatch => {
            return Err(MysqlTopologyError::Evidence(
                TopologyEvidenceError::ProductMismatch,
            ));
        }
        TopologyObservationStatus::OwnershipContextLoss => {
            return Err(MysqlTopologyError::Evidence(
                TopologyEvidenceError::OwnershipContextLoss,
            ));
        }
        TopologyObservationStatus::UnsupportedRole => {
            return Err(MysqlTopologyError::NativeState(
                TopologyNativeStateError::UnsupportedRole,
            ));
        }
        TopologyObservationStatus::UnsupportedState => {
            return Err(MysqlTopologyError::NativeState(
                TopologyNativeStateError::UnsupportedState,
            ));
        }
    }
    if evidence.local.attempt != attempt.id || evidence.binding_view_id != evidence.view_id {
        return Err(MysqlTopologyError::Evidence(
            TopologyEvidenceError::BindingMismatch,
        ));
    }
    if evidence.group_name != attempt.group_name {
        return Err(MysqlTopologyError::Evidence(
            TopologyEvidenceError::GroupMismatch,
        ));
    }
    validate_local_binding(target, &evidence.local)?;
    let post_view = QualifiedXcomView::parse(&evidence.view_id)?;
    if let Some(predecessor) = predecessor {
        let predecessor_view = QualifiedXcomView::parse(&predecessor.view_id)?;
        predecessor_view.require_successor(post_view)?;
    }

    let mut expected = predecessor.map_or_else(Vec::new, |credit| credit.members.clone());
    expected.push(target.clone());
    validate_members(attempt, &expected, target, &evidence.members)?;

    let local = evidence
        .members
        .iter()
        .find(|member| member.id() == target.member_id())
        .ok_or(MysqlTopologyError::TopologyState(
            TopologyStateError::TargetMemberMissing,
        ))?;
    if predecessor.is_none() {
        if local.role() != MemberRole::Primary {
            return Err(MysqlTopologyError::NativeState(
                TopologyNativeStateError::BootstrapMemberNotPrimary,
            ));
        }
    } else if local.role() != MemberRole::Secondary {
        return Err(MysqlTopologyError::NativeState(
            TopologyNativeStateError::JoiningMemberNotSecondary,
        ));
    }
    match local.state() {
        MemberState::Recovering => return Ok(TransitionEvaluation::Pending),
        MemberState::Offline => {
            return Err(MysqlTopologyError::NativeState(
                TopologyNativeStateError::Offline,
            ));
        }
        MemberState::Error => {
            return Err(MysqlTopologyError::NativeState(
                TopologyNativeStateError::Error,
            ));
        }
        MemberState::Unreachable => {
            return Err(MysqlTopologyError::NativeState(
                TopologyNativeStateError::Unreachable,
            ));
        }
        MemberState::Online => {}
    }
    validate_group_sources(attempt, &evidence.executed)?;
    if let Some(boundary) = boundary
        && !boundary.executed.is_subset_of(&evidence.executed)
    {
        return Err(MysqlTopologyError::Gtid(
            TopologyGtidError::SourceBoundaryNotContained,
        ));
    }
    Ok(TransitionEvaluation::Accepted(TransitionCredit {
        attempt: attempt.id.clone(),
        observation_attempt: evidence.observation_attempt.clone(),
        view_id: evidence.view_id.clone(),
        members: expected,
        executed: evidence.executed.clone(),
    }))
}

fn validate_local_binding(
    enrollment: &NativeIdentityEnrollment,
    observed: &ObservedLocalBinding,
) -> Result<(), MysqlTopologyError> {
    if observed.member != enrollment.binding.member
        || observed.server_uuid != enrollment.server_uuid
        || observed.member_id != enrollment.member_id
        || observed.member_address != enrollment.binding.member_address
    {
        return Err(MysqlTopologyError::Evidence(
            TopologyEvidenceError::IdentityDrift,
        ));
    }
    if observed.process_session != enrollment.binding.process_session
        || observed.endpoint != enrollment.binding.endpoint
        || observed.storage != enrollment.binding.storage
        || observed.observer_generation != enrollment.binding.observer_generation
    {
        return Err(MysqlTopologyError::Evidence(
            TopologyEvidenceError::OwnershipDrift,
        ));
    }
    Ok(())
}

fn validate_members(
    attempt: &TopologyAttempt,
    expected: &[NativeIdentityEnrollment],
    target: &NativeIdentityEnrollment,
    actual: &[NativeMember],
) -> Result<(), MysqlTopologyError> {
    for enrollment in expected {
        let Some(member) = actual
            .iter()
            .find(|member| member.id() == enrollment.member_id())
        else {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::RequiredMemberMissing,
            ));
        };
        if member.address() != enrollment.binding.member_address() {
            return Err(MysqlTopologyError::Evidence(
                TopologyEvidenceError::IdentityDrift,
            ));
        }
        if enrollment != target {
            let expected_role = if enrollment.binding.member == attempt.designated_bootstrap {
                MemberRole::Primary
            } else {
                MemberRole::Secondary
            };
            if member.role() != expected_role {
                return Err(MysqlTopologyError::NativeState(
                    TopologyNativeStateError::PredecessorRoleChanged,
                ));
            }
            if member.state() != MemberState::Online {
                return Err(MysqlTopologyError::NativeState(
                    TopologyNativeStateError::PredecessorNotOnline,
                ));
            }
        }
    }
    if actual.len() != expected.len() {
        return Err(MysqlTopologyError::TopologyState(
            TopologyStateError::UnexpectedMember,
        ));
    }
    Ok(())
}

fn validate_group_sources(
    attempt: &TopologyAttempt,
    executed: &GtidSet,
) -> Result<(), MysqlTopologyError> {
    if executed
        .entries()
        .iter()
        .any(|entry| entry.source().server_uuid().as_str() != attempt.group_name.as_str())
    {
        Err(MysqlTopologyError::Gtid(TopologyGtidError::NonGroupSource))
    } else {
        Ok(())
    }
}

fn next_target(attempt: &TopologyAttempt, accepted_members: usize) -> Option<MysqlMemberIndex> {
    let remaining = MysqlMemberIndex::all()
        .into_iter()
        .filter(|member| *member != attempt.designated_bootstrap)
        .collect::<Vec<_>>();
    accepted_members
        .checked_sub(1)
        .and_then(|index| remaining.get(index).copied())
}

#[cfg(test)]
mod manager_tests {
    use std::fs;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::time::Duration;

    use super::*;
    use crate::service::{MysqlMemberConfig, MysqlOperationTimeouts, MysqlTopologyConfig};

    #[tokio::test]
    async fn invalid_public_transition_invalidates_manager_context() {
        let group = "cccccccc-cccc-cccc-cccc-cccccccccccc";
        let sql = [33161, 33162, 33163].map(address);
        let replication = [43161, 43162, 43163].map(address);
        let topology = MysqlTopologyConfig::new([
            MysqlMemberConfig::new(1, sql[0], replication[0], group, replication).unwrap(),
            MysqlMemberConfig::new(2, sql[1], replication[1], group, replication).unwrap(),
            MysqlMemberConfig::new(3, sql[2], replication[2], group, replication).unwrap(),
        ])
        .unwrap();
        let timeouts = MysqlOperationTimeouts::new(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap();
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap();
        let root = workspace
            .join("target")
            .join(format!("manager-unit-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let instances = std::array::from_fn(|index| {
            MysqlInstanceManager::new(
                MysqlInstanceConfig::new_topology_member(
                    "/usr/bin/sleep",
                    "/usr/bin/sleep",
                    root.join(format!("data-{index}")),
                    root.join(format!("scratch-{index}")),
                    topology.clone(),
                    MysqlMemberIndex::all()[index],
                    timeouts,
                )
                .unwrap(),
            )
        });
        let attempt = TopologyAttempt::new(
            AttemptId::new("public-invalid-state").unwrap(),
            GroupName::new(group).unwrap(),
            MysqlMemberIndex::First,
            Duration::from_secs(1),
        )
        .unwrap();
        let observer = ControlCredential::new(
            attempt.id().clone(),
            CredentialGeneration::new("observer-generation").unwrap(),
            ControlCredentialRole::Observer,
            "observer_local",
            "observer-secret",
        )
        .unwrap();
        let recovery = ControlCredential::new(
            attempt.id().clone(),
            CredentialGeneration::new("recovery-generation").unwrap(),
            ControlCredentialRole::Recovery,
            "recovery_local",
            "recovery-secret",
        )
        .unwrap();
        let mut manager =
            MysqlTopologyManager::new(instances, attempt, observer, recovery).unwrap();

        assert!(matches!(
            manager.start_designated_member().await,
            Err(MysqlTopologyManagerError::InvalidState {
                expected: MysqlTopologyState::Initialized,
                actual: MysqlTopologyState::Configured,
            })
        ));
        assert_eq!(manager.state(), MysqlTopologyState::Failed);
        assert!(manager.initialize().is_err());
        assert_eq!(manager.state(), MysqlTopologyState::Failed);
        drop(manager);
        fs::remove_dir(root).unwrap();
    }

    fn address(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }
}
