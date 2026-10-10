//! Pure capabilities and evidence validation for the bounded three-member flow.

use core::fmt;
use std::hash::{Hash, Hasher};
use std::time::Instant;

use crate::adapter::{
    AdapterDiagnostic, ClockContext, ObservationClock, ObservationRequest, ObserverCredentials,
    RequestError, UnixSocketPath,
};
use crate::core::{
    AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, EndpointBinding, Epoch,
    ExactBinding, ExactBindingParts, GroupName, GtidSet, MemberAddress, MemberId, MemberRole,
    MemberState, NativeField, NativeMember, ObservationOutcome, ObservationProvenance,
    ObservationSessionId, PartitionId, ProcessSessionId, ReplicaId, ReplicaIncarnation, ResourceId,
    ServerUuid, StorageBinding, UnsupportedReason, ViewId,
};
use crate::service::{
    ControlStage, MemberCleanupFailure, MysqlInstanceManager, MysqlMemberIndex, MysqlTopologyError,
    MysqlTopologyManagerError, TopologyAuthorityError, TopologyEvidenceError, TopologyGtidError,
    TopologyNativeStateError, TopologyStateError,
};

/// One caller-controlled monotonic tick used by topology contracts.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TopologyInstant(u64);

impl TopologyInstant {
    /// Creates an opaque monotonic instant.
    #[must_use]
    pub const fn new(tick: u64) -> Self {
        Self(tick)
    }

    /// Returns the opaque tick.
    #[must_use]
    pub const fn tick(self) -> u64 {
        self.0
    }
}

/// One positive process-monotonic native-control deadline bound to an attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeControlDeadline {
    attempt: AttemptId,
    deadline: Instant,
}

impl NativeControlDeadline {
    /// Creates a future absolute deadline for one attempt's native operation.
    pub fn new(attempt: AttemptId, deadline: Instant) -> Result<Self, MysqlTopologyError> {
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
    start: TopologyInstant,
    deadline: TopologyInstant,
}

impl TopologyAttempt {
    /// Creates an attempt with one positive finite monotonic interval.
    pub fn new(
        id: AttemptId,
        group_name: GroupName,
        designated_bootstrap: MysqlMemberIndex,
        start: TopologyInstant,
        deadline: TopologyInstant,
    ) -> Result<Self, MysqlTopologyError> {
        if deadline <= start {
            return Err(MysqlTopologyError::Deadline(
                ControlStage::ProductValidation,
            ));
        }
        Ok(Self {
            id,
            group_name,
            designated_bootstrap,
            start,
            deadline,
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
}

/// Source transaction boundary captured from the accepted predecessor view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceGtidBoundary {
    attempt: AttemptId,
    source: NativeIdentityEnrollment,
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

    /// Constructs the join boundary from this exact accepted observation.
    pub fn source_gtid_boundary(
        &self,
        source: &NativeIdentityEnrollment,
    ) -> Result<SourceGtidBoundary, MysqlTopologyError> {
        if !self.members.contains(source) {
            return Err(MysqlTopologyError::Gtid(
                TopologyGtidError::SourceBoundaryBindingMismatch,
            ));
        }
        Ok(SourceGtidBoundary {
            attempt: self.attempt.clone(),
            source: source.clone(),
            observation_attempt: self.observation_attempt.clone(),
            view_id: self.view_id.clone(),
            executed: self.executed.clone(),
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
    Bootstrap(Box<BootstrapEffect>),
    Join(Box<JoinEffect>),
}

/// Owns exactly three fresh instance generations and their in-memory topology
/// attempt. It has no adoption, resume, publication, or durable store surface.
pub struct MysqlTopologyManager {
    instances: [MysqlInstanceManager; 3],
    authority: TopologyAuthority,
    state: MysqlTopologyState,
    observer: ControlCredential,
    recovery: ControlCredential,
    enrollments: [Option<NativeIdentityEnrollment>; 3],
    pending_effect: Option<PendingEffect>,
    pending_discovery: Option<ViewDiscovery>,
    accepted: Option<AcceptedMysqlTopology>,
    observation_serial: u64,
}

impl MysqlTopologyManager {
    /// Creates one fixed manager from exactly three existing instance managers.
    pub fn new(
        instances: [MysqlInstanceManager; 3],
        attempt: TopologyAttempt,
        observer: ControlCredential,
        recovery: ControlCredential,
    ) -> Result<Self, MysqlTopologyManagerError> {
        if observer.attempt() != attempt.id()
            || recovery.attempt() != attempt.id()
            || observer.role() != ControlCredentialRole::Observer
            || recovery.role() != ControlCredentialRole::Recovery
        {
            return Err(MysqlTopologyManagerError::Topology(
                MysqlTopologyError::Authority(TopologyAuthorityError::AttemptMismatch),
            ));
        }
        let topology = instances[0].config().topology();
        if instances.iter().enumerate().any(|(index, instance)| {
            instance.config().member_index().as_usize() != index
                || instance.config().topology() != topology
                || instance.config().member().group_uuid() != attempt.group_name().as_str()
        }) {
            return Err(MysqlTopologyManagerError::Topology(
                MysqlTopologyError::TopologyState(TopologyStateError::InvalidTransition),
            ));
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
        })
    }

    /// Current restart-stateless manager state.
    #[must_use]
    pub const fn state(&self) -> MysqlTopologyState {
        self.state
    }

    /// Exact owned member managers in fixed topology order.
    #[must_use]
    pub const fn members(&self) -> &[MysqlInstanceManager; 3] {
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

    /// Initializes all three fresh members before any member is started.
    pub fn initialize(&mut self) -> Result<(), MysqlTopologyManagerError> {
        self.require_state(MysqlTopologyState::Configured)?;
        for member in MysqlMemberIndex::all() {
            if let Err(error) = self.instances[member.as_usize()].initialize() {
                return Err(self.fail(MysqlTopologyManagerError::Instance { member, error }));
            }
        }
        self.state = MysqlTopologyState::Initialized;
        Ok(())
    }

    /// Starts, provisions, and enrolls the designated member before bootstrap.
    pub async fn start_designated_member(
        &mut self,
        deadline: &NativeControlDeadline,
    ) -> Result<(), MysqlTopologyManagerError> {
        self.require_state(MysqlTopologyState::Initialized)?;
        let member = self.authority.attempt.designated_bootstrap();
        self.start_and_enroll(member, deadline).await?;
        self.state = MysqlTopologyState::BootstrapMemberEnrolled;
        Ok(())
    }

    /// Enters the sole designated bootstrap and discovers its proposed view.
    pub async fn bootstrap(
        &mut self,
        now: TopologyInstant,
        deadline: &NativeControlDeadline,
    ) -> Result<(), MysqlTopologyManagerError> {
        self.require_state(MysqlTopologyState::BootstrapMemberEnrolled)?;
        let member = self.authority.attempt.designated_bootstrap();
        let result = async {
            let capability = self
                .authority
                .authorize_bootstrap(member, now)
                .map_err(MysqlTopologyManagerError::Topology)?;
            let effect = self.instances[member.as_usize()]
                .bootstrap_group_replication(capability, &self.recovery, deadline)
                .await
                .map_err(MysqlTopologyManagerError::Topology)?;
            let enrollment = self.enrollment(member)?.clone();
            let discovery = self.instances[member.as_usize()]
                .discover_topology_view(
                    enrollment,
                    self.authority.attempt.group_name().clone(),
                    deadline,
                )
                .await
                .map_err(MysqlTopologyManagerError::Topology)?;
            let effect = effect.bind_observation_attempt(self.next_observation_attempt()?);
            self.install_pending(
                PendingEffect::Bootstrap(Box::new(effect)),
                discovery,
                MysqlTopologyState::BootstrapObservationPending,
            );
            Ok(())
        }
        .await;
        if let Err(error) = result {
            return Err(self.fail(error));
        }
        Ok(())
    }

    /// Starts, provisions, and enrolls the fixed second join target.
    pub async fn start_second_member(
        &mut self,
        deadline: &NativeControlDeadline,
    ) -> Result<(), MysqlTopologyManagerError> {
        self.require_state(MysqlTopologyState::BootstrapAccepted)?;
        let member = self.join_order()[0];
        self.start_and_enroll(member, deadline).await?;
        self.state = MysqlTopologyState::SecondMemberEnrolled;
        Ok(())
    }

    /// Enters the second member's join from the exact accepted bootstrap credit.
    pub async fn join_second_member(
        &mut self,
        now: TopologyInstant,
        deadline: &NativeControlDeadline,
    ) -> Result<(), MysqlTopologyManagerError> {
        self.require_state(MysqlTopologyState::SecondMemberEnrolled)?;
        self.join_member(self.join_order()[0], now, deadline)
            .await?;
        Ok(())
    }

    /// Starts, provisions, and enrolls the fixed third join target.
    pub async fn start_third_member(
        &mut self,
        deadline: &NativeControlDeadline,
    ) -> Result<(), MysqlTopologyManagerError> {
        self.require_state(MysqlTopologyState::SecondAccepted)?;
        let member = self.join_order()[1];
        self.start_and_enroll(member, deadline).await?;
        self.state = MysqlTopologyState::ThirdMemberEnrolled;
        Ok(())
    }

    /// Enters the third member's join from the exact accepted second credit.
    pub async fn join_third_member(
        &mut self,
        now: TopologyInstant,
        deadline: &NativeControlDeadline,
    ) -> Result<(), MysqlTopologyManagerError> {
        self.require_state(MysqlTopologyState::ThirdMemberEnrolled)?;
        self.join_member(self.join_order()[1], now, deadline)
            .await?;
        Ok(())
    }

    /// Collects and evaluates the one exact fresh observer attempt for the
    /// currently pending bootstrap or join.
    pub async fn observe_pending<C: ObservationClock>(
        &mut self,
        context: TopologyObservationContext,
        clock: ClockContext<C>,
        now: TopologyInstant,
    ) -> Result<TransitionEvaluation, MysqlTopologyManagerError> {
        if !matches!(
            self.state,
            MysqlTopologyState::BootstrapObservationPending
                | MysqlTopologyState::SecondObservationPending
                | MysqlTopologyState::ThirdObservationPending
        ) {
            return Err(MysqlTopologyManagerError::InvalidState {
                expected: MysqlTopologyState::BootstrapObservationPending,
                actual: self.state,
            });
        }
        let result = self.observe_pending_inner(context, clock, now).await;
        let evidence = match result {
            Ok(evidence) => evidence,
            Err(error) => return Err(self.fail(error)),
        };
        self.accept_pending_evidence(&evidence, now)
    }

    /// Contains every exact member and reports every cleanup failure.
    pub fn stop(&mut self) -> Result<(), MysqlTopologyManagerError> {
        let failures = self.contain_all();
        self.authority.invalidate();
        self.pending_effect = None;
        self.pending_discovery = None;
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
        now: TopologyInstant,
        deadline: &NativeControlDeadline,
    ) -> Result<(), MysqlTopologyManagerError> {
        let result = async {
            let predecessor = self
                .authority
                .current_credit()
                .ok_or(MysqlTopologyManagerError::Topology(
                    MysqlTopologyError::TopologyState(TopologyStateError::InvalidTransition),
                ))?
                .clone();
            let source =
                predecessor
                    .members()
                    .last()
                    .ok_or(MysqlTopologyManagerError::Topology(
                        MysqlTopologyError::Evidence(TopologyEvidenceError::Missing),
                    ))?;
            let boundary = predecessor
                .source_gtid_boundary(source)
                .map_err(MysqlTopologyManagerError::Topology)?;
            let capability = self
                .authority
                .authorize_join(member, boundary, now)
                .map_err(MysqlTopologyManagerError::Topology)?;
            let effect = self.instances[member.as_usize()]
                .join_group_replication(capability, &self.recovery, deadline)
                .await
                .map_err(MysqlTopologyManagerError::Topology)?;
            let enrollment = self.enrollment(member)?.clone();
            let discovery = self.instances[member.as_usize()]
                .discover_topology_view(
                    enrollment,
                    self.authority.attempt.group_name().clone(),
                    deadline,
                )
                .await
                .map_err(MysqlTopologyManagerError::Topology)?;
            let effect = effect.bind_observation_attempt(self.next_observation_attempt()?);
            let pending_state = if member == self.join_order()[0] {
                MysqlTopologyState::SecondObservationPending
            } else {
                MysqlTopologyState::ThirdObservationPending
            };
            self.install_pending(
                PendingEffect::Join(Box::new(effect)),
                discovery,
                pending_state,
            );
            Ok(())
        }
        .await;
        result.map_err(|error| self.fail(error))
    }

    async fn observe_pending_inner<C: ObservationClock>(
        &mut self,
        context: TopologyObservationContext,
        clock: ClockContext<C>,
        now: TopologyInstant,
    ) -> Result<TopologyObservation, MysqlTopologyManagerError> {
        let discovery =
            self.pending_discovery
                .as_ref()
                .ok_or(MysqlTopologyManagerError::Topology(
                    MysqlTopologyError::Evidence(TopologyEvidenceError::Missing),
                ))?;
        let observation_attempt = match self.pending_effect.as_ref() {
            Some(PendingEffect::Bootstrap(effect)) => effect
                .observation_attempt
                .as_ref()
                .expect("manager binds bootstrap observation before pending"),
            Some(PendingEffect::Join(effect)) => effect
                .observation_attempt
                .as_ref()
                .expect("manager binds join observation before pending"),
            None => {
                return Err(MysqlTopologyManagerError::Topology(
                    MysqlTopologyError::Evidence(TopologyEvidenceError::Missing),
                ));
            }
        };
        let enrollment = discovery.enrollment();
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
            group_name: discovery.group_name().clone(),
            member_id: enrollment.member_id().clone(),
            member_address: enrollment.binding().member_address().clone(),
            configuration: context.configuration,
            epoch: context.epoch,
            authority_generation: context.authority_generation,
            view_id: discovery.view_id().clone(),
            credential_generation: enrollment.binding().observer_generation().clone(),
        });
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
        let request = ObservationRequest::new(binding, socket, credentials, provenance, clock)
            .map_err(MysqlTopologyManagerError::ObservationRequest)?;
        let report = self.instances[enrollment.binding().member().as_usize()]
            .observe(request)
            .await
            .map_err(|error| MysqlTopologyManagerError::Instance {
                member: enrollment.binding().member(),
                error,
            })?;
        topology_observation(
            report.outcome(),
            report.diagnostic(),
            observation_attempt.clone(),
            enrollment,
            discovery,
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
            PendingEffect::Bootstrap(effect) => self
                .authority
                .accept_bootstrap(effect, evidence, now)
                .map_err(MysqlTopologyManagerError::Topology),
            PendingEffect::Join(effect) => self
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
        self.observation_serial = self.observation_serial.saturating_add(1);
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

    fn fail(&mut self, primary: MysqlTopologyManagerError) -> MysqlTopologyManagerError {
        self.authority.invalidate();
        self.pending_effect = None;
        self.pending_discovery = None;
        self.accepted = None;
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
        let mut failures = Vec::new();
        for member in MysqlMemberIndex::all() {
            if let Err(error) = self.instances[member.as_usize()].contain() {
                failures.push(MemberCleanupFailure::new(member, error));
            }
        }
        failures
    }
}

impl Drop for MysqlTopologyManager {
    fn drop(&mut self) {
        self.authority.invalidate();
        self.pending_effect = None;
        self.pending_discovery = None;
        let _ = self.contain_all();
    }
}

fn topology_observation(
    outcome: &ObservationOutcome,
    diagnostic: &AdapterDiagnostic,
    observation_attempt: AttemptId,
    enrollment: &NativeIdentityEnrollment,
    discovery: &ViewDiscovery,
    decision: TopologyInstant,
) -> Result<TopologyObservation, MysqlTopologyError> {
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
        || (discovery.view_id().clone(), Vec::new(), GtidSet::empty()),
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
        discovery.group_name().clone(),
        discovery.view_id().clone(),
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

fn validate_boundary(
    attempt: &TopologyAttempt,
    predecessor: &TransitionCredit,
    boundary: &SourceGtidBoundary,
) -> Result<(), MysqlTopologyError> {
    if boundary.attempt != attempt.id
        || predecessor.attempt != attempt.id
        || boundary.observation_attempt != predecessor.observation_attempt
        || boundary.view_id != predecessor.view_id
        || boundary.executed != predecessor.executed
        || !predecessor.members.contains(&boundary.source)
    {
        return Err(MysqlTopologyError::Gtid(
            TopologyGtidError::SourceBoundaryBindingMismatch,
        ));
    }
    validate_group_sources(attempt, &boundary.executed)
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
    if let Some(predecessor) = predecessor
        && evidence.view_id == predecessor.view_id
    {
        return Err(MysqlTopologyError::TopologyState(
            TopologyStateError::UnchangedView,
        ));
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
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixListener;
    use std::path::Path;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::service::{
        MysqlInstanceConfig, MysqlMemberConfig, MysqlOperationTimeouts, MysqlTopologyConfig,
    };

    static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);
    const GROUP: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
    const UUIDS: [&str; 3] = [
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        "dddddddd-dddd-dddd-dddd-dddddddddddd",
    ];

    #[test]
    fn manager_scripted_happy_path_requires_each_accepted_transition() {
        let (mut manager, root) = manager("happy");
        let attempt = manager.authority.attempt.clone();
        let enrollments = std::array::from_fn(|index| {
            enrollment(
                &attempt,
                MysqlMemberIndex::all()[index],
                &manager.observer,
                &manager.recovery,
            )
        });
        manager.state = MysqlTopologyState::Initialized;
        manager
            .record_enrollment(MysqlMemberIndex::First, enrollments[0].clone())
            .unwrap();
        manager.state = MysqlTopologyState::BootstrapMemberEnrolled;

        let capability = manager
            .authority
            .authorize_bootstrap(MysqlMemberIndex::First, TopologyInstant::new(10))
            .unwrap();
        let observation_attempt = manager.next_observation_attempt().unwrap();
        let effect = capability
            .record_effect(BootstrapEffect::required_steps().to_vec())
            .unwrap()
            .bind_observation_attempt(observation_attempt.clone());
        let discovery = discovery(&attempt, &enrollments[0], "view-1");
        manager.install_pending(
            PendingEffect::Bootstrap(Box::new(effect)),
            discovery,
            MysqlTopologyState::BootstrapObservationPending,
        );
        let first = observation(
            &attempt,
            &enrollments[0],
            observation_attempt,
            "view-1",
            vec![native(
                &enrollments[0],
                MemberRole::Primary,
                MemberState::Online,
            )],
            gtids("1"),
            TopologyInstant::new(20),
        );
        assert!(matches!(
            manager
                .accept_pending_evidence(&first, TopologyInstant::new(21))
                .unwrap(),
            TransitionEvaluation::Accepted(_)
        ));
        assert_eq!(manager.state(), MysqlTopologyState::BootstrapAccepted);

        manager
            .record_enrollment(MysqlMemberIndex::Second, enrollments[1].clone())
            .unwrap();
        manager.state = MysqlTopologyState::SecondMemberEnrolled;
        scripted_join(
            &mut manager,
            &attempt,
            &enrollments,
            MysqlMemberIndex::Second,
            "view-2",
            "1-2",
            TopologyInstant::new(30),
        );
        assert_eq!(manager.state(), MysqlTopologyState::SecondAccepted);

        manager
            .record_enrollment(MysqlMemberIndex::Third, enrollments[2].clone())
            .unwrap();
        manager.state = MysqlTopologyState::ThirdMemberEnrolled;
        scripted_join(
            &mut manager,
            &attempt,
            &enrollments,
            MysqlMemberIndex::Third,
            "view-3",
            "1-3",
            TopologyInstant::new(50),
        );
        assert_eq!(manager.state(), MysqlTopologyState::Complete);
        let accepted = manager.accepted_topology().unwrap();
        assert_eq!(accepted.credit().members().len(), 3);
        assert!(!accepted.read_access_open());
        assert!(!accepted.write_access_open());
        assert_eq!(manager.authority.lifecycle_credits(), 3);
        drop(manager);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn manager_rejects_changed_native_identity_and_invalidates_progress() {
        let (mut manager, root) = manager("identity");
        let attempt = manager.authority.attempt.clone();
        let enrollment = enrollment(
            &attempt,
            MysqlMemberIndex::First,
            &manager.observer,
            &manager.recovery,
        );
        manager
            .record_enrollment(MysqlMemberIndex::First, enrollment.clone())
            .unwrap();
        let capability = manager
            .authority
            .authorize_bootstrap(MysqlMemberIndex::First, TopologyInstant::new(10))
            .unwrap();
        let observation_attempt = manager.next_observation_attempt().unwrap();
        let effect = capability
            .record_effect(BootstrapEffect::required_steps().to_vec())
            .unwrap()
            .bind_observation_attempt(observation_attempt.clone());
        manager.install_pending(
            PendingEffect::Bootstrap(Box::new(effect)),
            discovery(&attempt, &enrollment, "view-1"),
            MysqlTopologyState::BootstrapObservationPending,
        );
        let wrong_local = ObservedLocalBinding::new(
            attempt.id().clone(),
            MysqlMemberIndex::First,
            enrollment.binding().process_session().clone(),
            enrollment.binding().endpoint().clone(),
            enrollment.binding().storage().clone(),
            ServerUuid::new(UUIDS[1]).unwrap(),
            MemberId::new(UUIDS[1]).unwrap(),
            enrollment.binding().member_address().clone(),
            enrollment.binding().observer_generation().clone(),
        );
        let evidence = TopologyObservation::new(
            TopologyObservationStatus::Complete,
            observation_attempt,
            wrong_local,
            attempt.group_name().clone(),
            ViewId::new("view-1").unwrap(),
            ViewId::new("view-1").unwrap(),
            vec![native(
                &enrollment,
                MemberRole::Primary,
                MemberState::Online,
            )],
            gtids("1"),
            TopologyInstant::new(20),
        );
        assert!(matches!(
            manager.accept_pending_evidence(&evidence, TopologyInstant::new(21)),
            Err(MysqlTopologyManagerError::Topology(
                MysqlTopologyError::Evidence(TopologyEvidenceError::IdentityDrift)
            ))
        ));
        assert_eq!(manager.state(), MysqlTopologyState::Failed);
        assert!(manager.pending_effect.is_none());
        assert!(manager.accepted_topology().is_none());
        assert!(!manager.read_access_open());
        assert!(!manager.write_access_open());
        drop(manager);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn boundary_rejects_wrong_predecessor_attempt_process_storage_view_and_observation() {
        for case in [
            "predecessor",
            "attempt",
            "process",
            "storage",
            "view",
            "observation",
        ] {
            let (mut manager, root, enrollments) = accepted_bootstrap_manager(case);
            let predecessor = manager.authority.current_credit().unwrap().clone();
            let mut boundary = predecessor.source_gtid_boundary(&enrollments[0]).unwrap();
            match case {
                "predecessor" => {
                    boundary.source.server_uuid = ServerUuid::new(UUIDS[1]).unwrap();
                    boundary.source.member_id = MemberId::new(UUIDS[1]).unwrap();
                }
                "attempt" => {
                    boundary.attempt = AttemptId::new("wrong-attempt").unwrap();
                }
                "process" => {
                    boundary.source.binding.process_session =
                        ProcessSessionId::new("wrong-process").unwrap();
                }
                "storage" => {
                    boundary.source.binding.storage = StorageBinding::new("wrong-storage").unwrap();
                }
                "view" => {
                    boundary.view_id = ViewId::new("wrong-view").unwrap();
                }
                "observation" => {
                    boundary.observation_attempt =
                        AttemptId::new("wrong-observation-attempt").unwrap();
                }
                _ => unreachable!(),
            }
            assert_eq!(
                manager.authority.authorize_join(
                    MysqlMemberIndex::Second,
                    boundary,
                    TopologyInstant::new(30),
                ),
                Err(MysqlTopologyError::Gtid(
                    TopologyGtidError::SourceBoundaryBindingMismatch
                )),
                "{case}"
            );
            assert_eq!(manager.authority.lifecycle_credits(), 1, "{case}");
            assert!(!manager.read_access_open());
            assert!(!manager.write_access_open());
            drop(manager);
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn failed_runs_after_each_effect_stage_contain_every_owned_member() {
        for (label, started, state) in [
            ("after-bootstrap", 1, MysqlTopologyState::BootstrapAccepted),
            ("after-second", 2, MysqlTopologyState::SecondAccepted),
            ("after-third", 3, MysqlTopologyState::Complete),
        ] {
            let (mut manager, root) = manager(label);
            manager.initialize().unwrap();
            let foreign = root.join("foreign-resource");
            fs::write(&foreign, "untouched").unwrap();
            let mut sockets = Vec::new();
            let mut pids = Vec::new();
            for index in 0..started {
                let instance = &mut manager.instances[index];
                let socket = provide_socket(
                    instance.config().runtime().pid().to_owned(),
                    instance.config().runtime().socket().to_owned(),
                );
                instance.start().unwrap();
                pids.push(
                    fs::read_to_string(instance.config().runtime().pid())
                        .unwrap()
                        .trim()
                        .parse::<u32>()
                        .unwrap(),
                );
                sockets.push(socket);
            }
            manager.state = state;
            let error = manager.fail(MysqlTopologyManagerError::Topology(
                MysqlTopologyError::Evidence(TopologyEvidenceError::Missing),
            ));
            assert!(matches!(
                error,
                MysqlTopologyManagerError::Topology(MysqlTopologyError::Evidence(
                    TopologyEvidenceError::Missing
                ))
            ));
            for socket in sockets {
                socket.join().unwrap();
            }
            assert_eq!(manager.state(), MysqlTopologyState::Failed);
            for pid in pids {
                assert!(!Path::new(&format!("/proc/{pid}")).exists());
            }
            for instance in manager.members() {
                assert!(instance.config().data_root().is_dir());
                assert!(!instance.config().runtime().scratch_root().exists());
                assert!(!instance.config().runtime().socket().exists());
                assert!(
                    TcpStream::connect_timeout(
                        &instance.config().member().group_replication_address(),
                        Duration::from_millis(50),
                    )
                    .is_err()
                );
            }
            assert_eq!(fs::read_to_string(&foreign).unwrap(), "untouched");
            drop(manager);
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn primary_failure_retains_every_member_cleanup_failure() {
        let (mut manager, root) = manager("aggregate");
        manager.initialize().unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o500)).unwrap();
        let error = manager.fail(MysqlTopologyManagerError::Topology(
            MysqlTopologyError::Evidence(TopologyEvidenceError::Missing),
        ));
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let MysqlTopologyManagerError::Cleanup { primary, failures } = error else {
            panic!("expected primary plus cleanup aggregate");
        };
        assert!(matches!(
            primary.as_deref(),
            Some(MysqlTopologyManagerError::Topology(
                MysqlTopologyError::Evidence(TopologyEvidenceError::Missing)
            ))
        ));
        assert_eq!(failures.len(), 3);
        assert_eq!(
            failures
                .iter()
                .map(MemberCleanupFailure::member)
                .collect::<Vec<_>>(),
            MysqlMemberIndex::all()
        );
        drop(manager);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn manager_drop_contains_context_without_adoption_or_resume() {
        let (mut manager, root) = manager("drop");
        manager.initialize().unwrap();
        let socket = provide_socket(
            manager.instances[0].config().runtime().pid().to_owned(),
            manager.instances[0].config().runtime().socket().to_owned(),
        );
        manager.instances[0].start().unwrap();
        let pid = fs::read_to_string(manager.instances[0].config().runtime().pid())
            .unwrap()
            .trim()
            .parse::<u32>()
            .unwrap();
        let layouts = manager.instances.each_ref().map(|instance| {
            (
                instance.config().data_root().to_owned(),
                instance.config().runtime().scratch_root().to_owned(),
                instance.config().member().group_replication_address(),
            )
        });

        drop(manager);
        socket.join().unwrap();
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
        for (data, scratch, endpoint) in layouts {
            assert!(data.is_dir());
            assert!(!scratch.exists());
            assert!(TcpStream::connect_timeout(&endpoint, Duration::from_millis(50)).is_err());
        }
        let inventory = filesystem_inventory(&root);
        for forbidden in [
            "journal", "receipt", "cursor", "adoption", "metadata", "store",
        ] {
            assert!(
                inventory.iter().all(|entry| !entry.contains(forbidden)),
                "{inventory:?}"
            );
        }
        fs::remove_dir_all(root).unwrap();
    }

    fn scripted_join(
        manager: &mut MysqlTopologyManager,
        attempt: &TopologyAttempt,
        enrollments: &[NativeIdentityEnrollment; 3],
        target: MysqlMemberIndex,
        view: &str,
        history: &str,
        now: TopologyInstant,
    ) {
        let predecessor = manager.authority.current_credit().unwrap().clone();
        let source = predecessor.members().last().unwrap();
        let boundary = predecessor.source_gtid_boundary(source).unwrap();
        let capability = manager
            .authority
            .authorize_join(target, boundary, now)
            .unwrap();
        let observation_attempt = manager.next_observation_attempt().unwrap();
        let effect = capability
            .record_effect(JoinEffect::required_steps().to_vec())
            .unwrap()
            .bind_observation_attempt(observation_attempt.clone());
        manager.install_pending(
            PendingEffect::Join(Box::new(effect)),
            discovery(attempt, &enrollments[target.as_usize()], view),
            if target == MysqlMemberIndex::Second {
                MysqlTopologyState::SecondObservationPending
            } else {
                MysqlTopologyState::ThirdObservationPending
            },
        );
        let members = enrollments[..=target.as_usize()]
            .iter()
            .enumerate()
            .map(|(index, enrollment)| {
                native(
                    enrollment,
                    if index == 0 {
                        MemberRole::Primary
                    } else {
                        MemberRole::Secondary
                    },
                    MemberState::Online,
                )
            })
            .collect();
        let evidence = observation(
            attempt,
            &enrollments[target.as_usize()],
            observation_attempt,
            view,
            members,
            gtids(history),
            TopologyInstant::new(now.tick() + 5),
        );
        assert!(matches!(
            manager
                .accept_pending_evidence(&evidence, TopologyInstant::new(now.tick() + 6))
                .unwrap(),
            TransitionEvaluation::Accepted(_)
        ));
    }

    fn accepted_bootstrap_manager(
        label: &str,
    ) -> (MysqlTopologyManager, PathBuf, [NativeIdentityEnrollment; 3]) {
        let (mut manager, root) = manager(label);
        let attempt = manager.authority.attempt.clone();
        let enrollments = std::array::from_fn(|index| {
            enrollment(
                &attempt,
                MysqlMemberIndex::all()[index],
                &manager.observer,
                &manager.recovery,
            )
        });
        manager
            .record_enrollment(MysqlMemberIndex::First, enrollments[0].clone())
            .unwrap();
        let capability = manager
            .authority
            .authorize_bootstrap(MysqlMemberIndex::First, TopologyInstant::new(10))
            .unwrap();
        let observation_attempt = manager.next_observation_attempt().unwrap();
        let effect = capability
            .record_effect(BootstrapEffect::required_steps().to_vec())
            .unwrap()
            .bind_observation_attempt(observation_attempt.clone());
        manager.install_pending(
            PendingEffect::Bootstrap(Box::new(effect)),
            discovery(&attempt, &enrollments[0], "view-1"),
            MysqlTopologyState::BootstrapObservationPending,
        );
        let evidence = observation(
            &attempt,
            &enrollments[0],
            observation_attempt,
            "view-1",
            vec![native(
                &enrollments[0],
                MemberRole::Primary,
                MemberState::Online,
            )],
            gtids("1"),
            TopologyInstant::new(20),
        );
        manager
            .accept_pending_evidence(&evidence, TopologyInstant::new(21))
            .unwrap();
        manager
            .record_enrollment(MysqlMemberIndex::Second, enrollments[1].clone())
            .unwrap();
        (manager, root, enrollments)
    }

    fn manager(label: &str) -> (MysqlTopologyManager, PathBuf) {
        let serial = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap();
        let root = workspace
            .join("target")
            .join(format!("p3m-{}-{serial}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let launcher = root.join("launcher");
        fs::write(
            &launcher,
            r#"#!/bin/sh
while [ "$1" != "--" ]; do shift; done
shift
mysqld="$1"
shift
if [ "$1" = "--version" ]; then
  echo "$mysqld  Ver 8.4.11 for Linux on x86_64 (MySQL Community Server - GPL)"
  exit 0
fi
case "$*" in
  *--initialize-insecure*) exit 0 ;;
esac
config=${1#--defaults-file=}
pid_file=$(sed -n 's/^pid-file=//p' "$config")
echo $$ > "$pid_file"
exec "$mysqld" 60
"#,
        )
        .unwrap();
        fs::set_permissions(&launcher, fs::Permissions::from_mode(0o700)).unwrap();
        let topology = MysqlTopologyConfig::new(member_configs()).unwrap();
        let timeouts = MysqlOperationTimeouts::new(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap();
        let instances = std::array::from_fn(|index| {
            let member = MysqlMemberIndex::all()[index];
            MysqlInstanceManager::new(
                MysqlInstanceConfig::new(
                    "/usr/bin/sleep",
                    &launcher,
                    root.join(format!("data-{index}")),
                    root.join(format!("scratch-{index}")),
                    topology.clone(),
                    member,
                    timeouts,
                )
                .unwrap(),
            )
        });
        let attempt = TopologyAttempt::new(
            AttemptId::new(format!("manager-{label}")).unwrap(),
            GroupName::new(GROUP).unwrap(),
            MysqlMemberIndex::First,
            TopologyInstant::new(0),
            TopologyInstant::new(100),
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
        (
            MysqlTopologyManager::new(instances, attempt, observer, recovery).unwrap(),
            root,
        )
    }

    fn member_configs() -> [MysqlMemberConfig; 3] {
        let sql = [33061, 33062, 33063].map(address);
        let group = [43061, 43062, 43063].map(address);
        [
            MysqlMemberConfig::new(1, sql[0], group[0], GROUP, group).unwrap(),
            MysqlMemberConfig::new(2, sql[1], group[1], GROUP, group).unwrap(),
            MysqlMemberConfig::new(3, sql[2], group[2], GROUP, group).unwrap(),
        ]
    }

    fn address(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    fn enrollment(
        attempt: &TopologyAttempt,
        member: MysqlMemberIndex,
        observer: &ControlCredential,
        recovery: &ControlCredential,
    ) -> NativeIdentityEnrollment {
        let binding = MemberControlBinding::new(
            attempt.id().clone(),
            member,
            ProcessSessionId::new(format!("process-{}", member.as_usize())).unwrap(),
            EndpointBinding::new(format!("endpoint-{}", member.as_usize())).unwrap(),
            StorageBinding::new(format!("storage-{}", member.as_usize())).unwrap(),
            MemberAddress::new(format!("127.0.0.1:{}", 33061 + member.as_usize())).unwrap(),
            observer.generation().clone(),
            recovery.generation().clone(),
        );
        let accounts = AccountProvisioningEvidence::new(
            binding.clone(),
            observer,
            recovery,
            AccountProvisioningEvidence::required_steps().to_vec(),
        )
        .unwrap();
        NativeIdentityEnrollment::new(
            binding,
            ServerUuid::new(UUIDS[member.as_usize()]).unwrap(),
            &accounts,
            true,
            GtidSet::empty(),
            NativeIdentityEnrollment::required_steps().to_vec(),
        )
        .unwrap()
    }

    fn discovery(
        attempt: &TopologyAttempt,
        enrollment: &NativeIdentityEnrollment,
        view: &str,
    ) -> ViewDiscovery {
        ViewDiscovery::new(
            enrollment.clone(),
            attempt.group_name().clone(),
            ViewId::new(view).unwrap(),
            ViewDiscovery::required_steps().to_vec(),
        )
        .unwrap()
    }

    fn observation(
        attempt: &TopologyAttempt,
        enrollment: &NativeIdentityEnrollment,
        observation_attempt: AttemptId,
        view: &str,
        members: Vec<NativeMember>,
        executed: GtidSet,
        decision: TopologyInstant,
    ) -> TopologyObservation {
        TopologyObservation::new(
            TopologyObservationStatus::Complete,
            observation_attempt,
            ObservedLocalBinding::from_enrollment(enrollment),
            attempt.group_name().clone(),
            ViewId::new(view).unwrap(),
            ViewId::new(view).unwrap(),
            members,
            executed,
            decision,
        )
    }

    fn native(
        enrollment: &NativeIdentityEnrollment,
        role: MemberRole,
        state: MemberState,
    ) -> NativeMember {
        NativeMember::new(
            enrollment.member_id().clone(),
            enrollment.binding().member_address().clone(),
            role,
            state,
        )
    }

    fn gtids(interval: &str) -> GtidSet {
        GtidSet::from_str(&format!("{GROUP}:{interval}")).unwrap()
    }

    fn provide_socket(pid_path: PathBuf, socket_path: PathBuf) -> JoinHandle<()> {
        thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(3);
            let pid = loop {
                if let Ok(value) = fs::read_to_string(&pid_path)
                    && let Ok(pid) = value.trim().parse::<u32>()
                {
                    break pid;
                }
                assert!(Instant::now() < deadline, "PID file did not appear");
                thread::sleep(Duration::from_millis(10));
            };
            let listener = UnixListener::bind(&socket_path).unwrap();
            while Path::new(&format!("/proc/{pid}")).exists() {
                thread::sleep(Duration::from_millis(10));
            }
            drop(listener);
            let _ = fs::remove_file(socket_path);
        })
    }

    fn filesystem_inventory(root: &Path) -> Vec<String> {
        let mut inventory = Vec::new();
        let mut pending = vec![root.to_owned()];
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(directory).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    pending.push(entry.path());
                }
                inventory.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        inventory
    }
}
