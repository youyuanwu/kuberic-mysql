//! Typed, non-secret lifecycle failures.

use core::fmt;
use std::io::ErrorKind;

use crate::adapter::RequestError;
use crate::service::{
    AttemptAdmissionError, AttemptCancellationState, AttemptDeadlineState, MysqlInstanceState,
    MysqlMemberIndex, MysqlOwnedPathKind, MysqlTopologyState,
};

/// Bounded client-gateway protocol phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MysqlProxyPhase {
    /// Loopback listener preparation or acceptance.
    Listener,
    /// Backend private-UDS connection and greeting.
    Greeting,
    /// Client handshake response and server authentication exchange.
    Authentication,
    /// One complete client command packet.
    ClientCommand,
    /// One bounded server response.
    ServerResponse,
    /// Generation revocation and native absence proof.
    Revocation,
    /// Application-account creation, cache preparation, or removal.
    AccountLifecycle,
}

/// Stable reason a Classic Protocol exchange was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MysqlProxyProtocolError {
    /// Packet header or payload ended before the declared boundary.
    IncompletePacket,
    /// A packet exceeded the one-mebibyte payload limit.
    PacketTooLarge,
    /// A continuation-sized packet was observed.
    ContinuationPacket,
    /// Packet sequence did not match the exact bounded state.
    SequenceMismatch,
    /// Server greeting was not protocol version 10 or was malformed.
    InvalidGreeting,
    /// An advertised or requested capability was outside the fixed profile.
    UnsupportedCapability,
    /// The client username was not the dedicated application identity.
    UsernameMismatch,
    /// The authentication plugin or exchange was outside warmed fast auth.
    UnsupportedAuthentication,
    /// The command was not query, ping, or quit.
    UnsupportedCommand,
    /// A local-infile request was observed.
    LocalInfile,
    /// A result used a nonlegacy or otherwise unsupported termination mode.
    UnsupportedResult,
    /// A response advertised additional results.
    MoreResults,
}

/// Secret-free application gateway failure.
#[derive(Debug)]
pub enum MysqlProxyError {
    /// Listener input was not an exact loopback address or the account input
    /// was invalid.
    InvalidConfiguration,
    /// The requested transition was incompatible with current proxy state.
    InvalidState,
    /// Stage 2 requires both read and write permission.
    BothPermissionsRequired,
    /// The supplied generation was stale or did not own the active endpoint.
    GenerationMismatch,
    /// A listener or transport operation failed.
    Io {
        /// Protocol phase being performed.
        phase: MysqlProxyPhase,
        /// Stable operating-system error category.
        kind: ErrorKind,
    },
    /// Bounded protocol validation failed.
    Protocol {
        /// Protocol phase being performed.
        phase: MysqlProxyPhase,
        /// Stable rejection reason.
        reason: MysqlProxyProtocolError,
    },
    /// A fixed phase deadline expired.
    Deadline(MysqlProxyPhase),
    /// The shared attempt was cancelled.
    Cancelled,
    /// Private-UDS account lifecycle failed.
    Account(MysqlTopologyError),
    /// Exact native backend termination or absence proof failed.
    Native(MemberTransitionError),
    /// A backend connection was initiated but no exact native connection ID
    /// was obtained, so native absence cannot be proved.
    UnidentifiedBackend,
    /// Cleanup failed after a primary proxy failure.
    Cleanup {
        /// Original proxy failure.
        primary: Box<Self>,
        /// Mandatory cleanup failure.
        cleanup: Box<Self>,
    },
}

impl fmt::Display for MysqlProxyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "MySQL client proxy failure: {self:?}")
    }
}

impl std::error::Error for MysqlProxyError {}

/// Configuration validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    /// A Group Replication member used server ID zero.
    ZeroServerId,
    /// Two Group Replication members used the same server ID.
    DuplicateServerId,
    /// A reported SQL address used port zero.
    ZeroSqlPort,
    /// A reported SQL address was not loopback-only.
    NonLoopbackSqlAddress,
    /// A reported IPv6 SQL address used nonzero flow or scope metadata.
    NonCanonicalSqlAddress,
    /// Two members used the same reported SQL address.
    DuplicateSqlAddress,
    /// A Group Replication address used port zero.
    ZeroGroupReplicationPort,
    /// A Group Replication address was not loopback-only.
    NonLoopbackGroupReplicationAddress,
    /// An IPv6 Group Replication address used nonzero flow or scope metadata.
    NonCanonicalGroupReplicationAddress,
    /// Two members used the same Group Replication address.
    DuplicateGroupReplicationAddress,
    /// A Group Replication group identity was not a canonicalizable non-nil
    /// UUID.
    InvalidGroupUuid,
    /// Members did not use one shared Group Replication group UUID.
    MismatchedGroupUuid,
    /// A Group Replication seed used port zero.
    ZeroGroupReplicationSeedPort,
    /// A Group Replication seed was not loopback-only.
    NonLoopbackGroupReplicationSeed,
    /// An IPv6 Group Replication seed used nonzero flow or scope metadata.
    NonCanonicalGroupReplicationSeed,
    /// A member's Group Replication seed set contained a duplicate.
    DuplicateGroupReplicationSeed,
    /// A member's seed set was not the exact configured three-member Group
    /// Replication address set.
    GroupReplicationSeedSetMismatch,
    /// A path was not absolute.
    PathNotAbsolute,
    /// A path was not lexically normalized.
    PathNotNormalized,
    /// A path could not be represented in generated MySQL configuration.
    PathNotRepresentable,
    /// A path or one of its existing ancestors was a symbolic link.
    Symlink,
    /// An executable path was not a regular executable file.
    NotExecutableFile,
    /// A validated executable or launcher file was replaced before use.
    ExecutableChanged,
    /// A fresh root already existed.
    RootAlreadyExists,
    /// A fresh root's parent did not exist as an owner-controlled directory.
    InvalidRootParent,
    /// Data and scratch roots were equal, aliased, or overlapped.
    RootOverlap,
    /// An operation timeout was zero.
    ZeroTimeout,
    /// An operation timeout cannot be represented by the monotonic clock.
    UnrepresentableTimeout,
    /// The host platform is not the qualified Linux x86-64 target.
    UnsupportedPlatform,
    /// Filesystem inspection failed.
    Filesystem(ErrorKind),
}

/// The bounded operation associated with a timeout or process failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleOperation {
    /// Exact product identity verification.
    ProductValidation,
    /// Fresh data-directory initialization.
    Initialization,
    /// Server startup and private-socket readiness.
    Startup,
    /// Graceful or forced server shutdown.
    Shutdown,
    /// Private-socket disappearance proof.
    SocketDisappearance,
    /// Disposable scratch cleanup.
    ScratchCleanup,
    /// Cleanup of fresh roots claimed by a failed initialization.
    OwnedRootCleanup,
}

/// Exact product validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProductError {
    /// Version inspection did not complete successfully.
    InspectionFailed,
    /// The inspected executable was not exact Oracle MySQL 8.4.11 for Linux
    /// x86-64.
    UnsupportedIdentity,
}

/// Retained child ownership validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnershipError {
    /// The retained child had already exited.
    ChildExited,
    /// The PID file was absent or malformed.
    InvalidPidFile,
    /// The PID file did not name the retained child.
    PidMismatch,
    /// The live process executable was not the configured executable.
    ExecutableMismatch,
    /// An owned data or scratch root was replaced or changed.
    RootMismatch,
    /// Process identity could not be inspected.
    InspectionFailed,
}

/// One exact native-control step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlStage {
    /// Final owned-socket validation.
    SocketValidation,
    /// Root connection over the owned Unix socket.
    Connect,
    /// Root authentication.
    Authenticate,
    /// Exact Oracle MySQL product validation.
    ProductValidation,
    /// Reading the current session binary-log switch.
    ReadBinaryLogging,
    /// Disabling session binary logging.
    DisableBinaryLogging,
    /// Creating the local observer account.
    CreateObserver,
    /// Granting observer access to Group Replication membership.
    GrantObserverMembers,
    /// Granting observer access to local Group Replication statistics.
    GrantObserverStats,
    /// Creating the local distributed-recovery account.
    CreateRecovery,
    /// Granting the exact distributed-recovery privileges.
    GrantRecovery,
    /// Creating the dedicated local application account.
    CreateApplication,
    /// Granting exact qualification-schema DML privileges.
    GrantApplicationDml,
    /// Removing the dedicated local application account.
    DropApplication,
    /// Warming the dedicated account's `caching_sha2_password` cache.
    WarmApplicationAuthentication,
    /// Reading back the exact application-account generation and grants.
    InspectApplicationAccount,
    /// Proving the exact application account is absent.
    ProveApplicationAccountAbsent,
    /// Restoring the original session binary-log switch.
    RestoreBinaryLogging,
    /// Proving the session binary-log switch was restored.
    ProveBinaryLoggingRestored,
    /// Reading the pre-effect native server identity.
    EnrollIdentity,
    /// Revalidating the enrolled server identity after a topology effect.
    VerifyEnrolledIdentity,
    /// Proving that no existing group state is present.
    InspectExistingGroup,
    /// Proving that no executed transaction history is present.
    InspectExistingHistory,
    /// Enabling designated bootstrap mode.
    EnableBootstrap,
    /// Starting Group Replication with in-memory recovery credentials.
    StartGroupReplication,
    /// Disabling designated bootstrap mode.
    DisableBootstrap,
    /// Proving that bootstrap mode is disabled.
    ProveBootstrapDisabled,
    /// Discovering the current native view identity.
    DiscoverView,
    /// Reading one bracketed coherent native membership view.
    ReadMembership,
    /// Reading structured executed GTID history for a member view.
    ReadExecutedHistory,
    /// Transferring the native primary role to one exact member UUID.
    TransferPrimary,
    /// Terminating one exact native backend connection.
    TerminateConnection,
    /// Proving one exact native backend connection is absent.
    ProveConnectionAbsent,
    /// Reading back the exact native backend account identity.
    InspectConnectionIdentity,
    /// Explicit connected-session teardown.
    Disconnect,
}

/// Why member-scoped mutation authority was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemberAuthorityError {
    /// The authority belongs to another attempt.
    AttemptMismatch,
    /// The authority names another local member binding.
    LocalBindingMismatch,
    /// A peer evidence value was substituted for local mutation authority.
    AuthoritySubstitution,
    /// The selected source was not the exact locally owned source.
    SourceMismatch,
    /// The transfer target was absent, local, or otherwise not exact.
    WrongTransferTarget,
}

/// Why coherent member/view evidence was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemberViewError {
    /// A bracketed native read changed before it closed.
    IncoherentReadback,
    /// The supplied predecessor evidence was stale or changed.
    StaleView,
    /// A view identity was not canonical `<u64>:<u32>`.
    MalformedViewId,
    /// A successor changed the fixed XCom component.
    ViewFixedPartChanged,
    /// A successor did not advance by exactly one.
    ViewSequenceNotSuccessor,
    /// The predecessor monotonic component could not advance.
    ViewSequenceOverflow,
    /// A required member was absent.
    RequiredMemberMissing,
    /// An unapproved member was present.
    UnexpectedMember,
    /// A member address differed from its immutable peer input.
    MemberAddressMismatch,
    /// Native membership did not contain exactly one primary.
    PrimaryCardinality,
    /// A required member was not online.
    MemberNotOnline,
    /// A required member had the wrong role.
    MemberRoleMismatch,
    /// Structured history contained a non-group source.
    NonGroupSource,
    /// Successor history did not contain the predecessor boundary.
    SourceBoundaryNotContained,
}

/// Why a typed member effect receipt was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemberReceiptError {
    /// Receipt identity did not match the admitted authority.
    BindingMismatch,
    /// Receipt predecessor evidence did not match the exact source boundary.
    PredecessorMismatch,
    /// Receipt successor evidence did not prove the requested effect.
    SuccessorMismatch,
    /// The attempt was cancelled before receipt credit.
    Cancelled,
    /// The receipt was created outside the primary-work budget.
    Deadline(AttemptDeadlineState),
    /// Mandatory cleanup did not complete.
    CleanupIncomplete,
}

/// One secret-free member-local native effect failure.
#[derive(Debug)]
pub enum MemberTransitionError {
    /// Member-scoped mutation authority was invalid.
    Authority(MemberAuthorityError),
    /// Native membership or view evidence was invalid.
    View(MemberViewError),
    /// The shared attempt was cancelled.
    Cancelled,
    /// The attempt was outside its primary-work budget.
    Deadline(AttemptDeadlineState),
    /// A typed effect receipt was inconsistent.
    Receipt(MemberReceiptError),
    /// Native control failed after local ownership validation.
    Native(MysqlTopologyError),
    /// Primary failure plus local-only containment failure.
    Cleanup {
        /// Original member effect failure.
        primary: Box<Self>,
        /// Exact local cleanup failure.
        cleanup: Box<MysqlInstanceError>,
    },
}

impl fmt::Display for MemberTransitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "MySQL member transition failure: {self:?}")
    }
}

impl std::error::Error for MemberTransitionError {}

impl From<MysqlTopologyError> for MemberTransitionError {
    fn from(error: MysqlTopologyError) -> Self {
        Self::Native(error)
    }
}

/// One secret-free rejection from the future-adapter lifecycle engine.
#[derive(Debug)]
pub enum MysqlReplicatorError {
    /// The engine has not accepted its exact open receipt.
    NotOpen,
    /// Open was repeated with different identity or generation inputs.
    OpenConflict,
    /// A supplied replica, incarnation, member, or credential fact was not exact.
    IdentityMismatch,
    /// The member configuration cannot support Group Replication lifecycle work.
    InvalidMemberConfiguration,
    /// A fresh member did not retain its dedicated application credential.
    ApplicationCredentialRequired,
    /// The operation generation was not newer than the last admitted operation.
    StaleOperationGeneration,
    /// An operation identity was retried with changed inputs.
    OperationConflict,
    /// A prior attempt with this exact operation identity completed uncertainly.
    OperationUncertain,
    /// The epoch was not newer than the current accepted epoch.
    StaleEpoch,
    /// A newer epoch barrier is required before this role operation.
    EpochBarrierRequired,
    /// A role must be accepted for the current epoch before configuration.
    RoleRequired,
    /// A current configuration must be accepted before this operation.
    ConfigurationRequired,
    /// The local replica must be primary under the current configuration.
    PrimaryRequired,
    /// A peer operation selected the exact local replica or native member.
    SelfTarget,
    /// A configuration omitted, duplicated, or changed an exact member identity.
    InvalidConfiguration,
    /// An access generation was stale or conflicting.
    StaleAccessGeneration,
    /// Published gateway authority must be revoked and contained first.
    AccessRevocationRequired,
    /// A typed member receipt or observation did not match the admitted operation.
    ReceiptMismatch,
    /// A typed receipt arrived after cancellation or outside primary-work time.
    LateReceipt {
        /// Deadline position when the receipt arrived.
        deadline: AttemptDeadlineState,
        /// Cancellation position when the receipt arrived.
        cancellation: AttemptCancellationState,
    },
    /// A receipt did not prove the requested local native role.
    NativeRoleMismatch,
    /// Controlled-handoff publication or successor evidence was incomplete.
    HandoffProofRequired,
    /// A handoff publication proof was already accepted.
    HandoffProofDuplicate,
    /// A later handoff publication proof conflicted with the accepted proof.
    HandoffProofConflict,
    /// Mandatory close settlement did not produce completed cleanup evidence.
    CloseUnsettled,
    /// Abort crossed the terminal boundary but exact complete containment was
    /// not proved.
    AbortUnsettled,
    /// The shared attempt was cancelled before operation admission.
    Cancelled,
    /// The shared attempt was outside its primary-work budget.
    Deadline(AttemptDeadlineState),
    /// The engine has crossed a terminal close or abort boundary.
    Terminal,
    /// The member-local effect failed or completed uncertainly.
    MemberTransition(MemberTransitionError),
    /// Fresh member preparation failed and exact local containment also failed.
    FreshPreparationCleanup {
        /// Original member preparation failure.
        prior: Box<MysqlReplicatorError>,
        /// Exact local containment failure.
        cleanup: Box<MysqlInstanceError>,
    },
    /// Configuration completion failed or became uncertain and exact local
    /// containment also failed.
    ConfigurationCleanup {
        /// Original configuration admission, effect, or receipt failure.
        primary: Box<MysqlReplicatorError>,
        /// Exact local containment failure.
        cleanup: Box<MysqlInstanceError>,
    },
    /// Account, listener, protocol, generation, or gateway containment failed.
    Proxy(MysqlProxyError),
    /// Gateway cleanup failed and local process containment also failed.
    ProxyCleanup {
        /// Original gateway cleanup failure.
        proxy: Box<MysqlProxyError>,
        /// Exact local containment failure.
        instance: Box<MysqlInstanceError>,
    },
    /// Exact local containment failed.
    Instance(MysqlInstanceError),
}

impl fmt::Display for MysqlReplicatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "MySQL replicator lifecycle failure: {self:?}")
    }
}

impl std::error::Error for MysqlReplicatorError {}

/// Why a topology capability was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopologyAuthorityError {
    /// The capability or evidence belongs to another topology attempt.
    AttemptMismatch,
    /// The supplied credential generation is not enrolled for this member.
    CredentialGenerationMismatch,
    /// Member-local setup did not use the same attempt principal.
    PrincipalMismatch,
    /// The selected bootstrap member is not the designated member.
    WrongBootstrapMember,
    /// Bootstrap authority was already issued.
    BootstrapAlreadyUsed,
    /// Another topology transition must complete before this operation.
    TransitionInFlight,
    /// The requested join target is not the next sequential member.
    WrongJoinTarget,
    /// The capability was consumed, revoked, or superseded.
    StaleCapability,
    /// No exact enrollment exists for the requested member.
    MissingEnrollment,
}

/// Why the topology state cannot admit or accept an operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopologyStateError {
    /// A topology operation received a single-instance configuration.
    TopologyConfigurationRequired,
    /// A supposedly fresh member already exposes native group state.
    ExistingGroupState,
    /// A supposedly fresh member already exposes executed transaction history.
    ExistingTransactionHistory,
    /// An enrollment duplicates another member's exact identity or binding.
    DuplicateEnrollment,
    /// A transition was requested from the wrong pure topology state.
    InvalidTransition,
    /// A post-effect view did not change from its accepted predecessor view.
    UnchangedView,
    /// A qualified XCom view was not exactly `<u64>:<u32>`.
    MalformedViewId,
    /// A predecessor view's monotonic component had no successor.
    ViewSequenceOverflow,
    /// A post-view changed the fixed XCom view component.
    ViewFixedPartChanged,
    /// A post-view was not the predecessor monotonic component plus one.
    ViewSequenceNotSuccessor,
    /// The post-effect view omitted a required predecessor member.
    RequiredMemberMissing,
    /// The post-effect view contained an unapproved member.
    UnexpectedMember,
    /// The post-effect view did not contain the exact target member.
    TargetMemberMissing,
}

/// Why typed native role or state evidence cannot complete a transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopologyNativeStateError {
    /// Bootstrap did not report the designated member as primary.
    BootstrapMemberNotPrimary,
    /// A joining member did not report the secondary role.
    JoiningMemberNotSecondary,
    /// A previously accepted member changed role.
    PredecessorRoleChanged,
    /// A previously accepted member was no longer online.
    PredecessorNotOnline,
    /// The target reported `OFFLINE`.
    Offline,
    /// The target reported `ERROR`.
    Error,
    /// The target reported `UNREACHABLE`.
    Unreachable,
    /// The observation contained an unsupported native role.
    UnsupportedRole,
    /// The observation contained an unsupported native state.
    UnsupportedState,
}

/// Why structured transaction history cannot complete a join.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopologyGtidError {
    /// A GTID source differed from the configured Group Replication UUID.
    NonGroupSource,
    /// The target did not contain the accepted pre-join source boundary.
    SourceBoundaryNotContained,
    /// The source boundary was not bound to the accepted predecessor evidence.
    SourceBoundaryBindingMismatch,
}

/// Why proposed post-effect evidence cannot receive lifecycle credit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopologyEvidenceError {
    /// The manager-owned monotonic clock moved backwards.
    ClockRegression,
    /// The manager-owned clock mapped its current tick into the future.
    FutureDatedClock,
    /// The manager-owned clock could not represent an instant or deadline.
    UnrepresentableClock,
    /// Required post-effect evidence was not collected.
    Missing,
    /// Product identity differed from the qualified Oracle MySQL 8.4.11
    /// profile.
    ProductMismatch,
    /// Exact retained ownership context was lost.
    OwnershipContextLoss,
    /// Evidence used another observation attempt or native view.
    BindingMismatch,
    /// The observed server/member identity drifted from pre-effect enrollment.
    IdentityDrift,
    /// The process, endpoint, or storage binding drifted from enrollment.
    OwnershipDrift,
    /// The configured Group Replication identity changed.
    GroupMismatch,
}

/// Secret-free topology control or validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MysqlTopologyError {
    /// The owned private UDS could not be reached.
    ControlTransport(ControlStage),
    /// Root authentication over the owned UDS failed.
    ControlAuthentication(ControlStage),
    /// The authenticated account lacked a required control permission.
    ControlPermission(ControlStage),
    /// The native client or server rejected the selected control operation.
    ControlProtocol(ControlStage),
    /// The connected server was not the qualified product.
    ControlProductCompatibility,
    /// Exact retained process/root/socket ownership was unavailable.
    OwnershipContextLoss,
    /// Capability or operation authority was invalid.
    Authority(TopologyAuthorityError),
    /// Pure topology state rejected the operation or evidence.
    TopologyState(TopologyStateError),
    /// Native role/state evidence rejected completion.
    NativeState(TopologyNativeStateError),
    /// The pre-established monotonic deadline was reached or exceeded.
    Deadline(ControlStage),
    /// Structured GTID evidence rejected completion.
    Gtid(TopologyGtidError),
    /// Post-effect evidence was missing, incompatible, or inconsistently bound.
    Evidence(TopologyEvidenceError),
    /// A primary control failure and its mandatory cleanup/proof both failed.
    PairedControl {
        /// Original control failure.
        prior: Box<Self>,
        /// Cleanup or restoration failure.
        cleanup: Box<Self>,
    },
}

impl fmt::Display for MysqlTopologyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "MySQL topology failure: {self:?}")
    }
}

impl std::error::Error for MysqlTopologyError {}

/// One member cleanup failure retained by topology-wide containment.
#[derive(Debug)]
pub struct MemberCleanupFailure {
    member: MysqlMemberIndex,
    error: MysqlInstanceError,
}

impl MemberCleanupFailure {
    pub(crate) const fn new(member: MysqlMemberIndex, error: MysqlInstanceError) -> Self {
        Self { member, error }
    }

    /// Member whose exact containment failed.
    #[must_use]
    pub const fn member(&self) -> MysqlMemberIndex {
        self.member
    }

    /// Exact lifecycle cleanup failure.
    #[must_use]
    pub const fn error(&self) -> &MysqlInstanceError {
        &self.error
    }
}

/// One public three-member manager failure.
#[derive(Debug)]
pub enum MysqlTopologyManagerError {
    /// Two members configured equal or ancestor/descendant owned paths.
    PathConflict {
        /// First conflicting member.
        left_member: MysqlMemberIndex,
        /// First conflicting path category.
        left_kind: MysqlOwnedPathKind,
        /// Second conflicting member.
        right_member: MysqlMemberIndex,
        /// Second conflicting path category.
        right_kind: MysqlOwnedPathKind,
    },
    /// The requested manager transition was out of order.
    InvalidState {
        /// Required manager state.
        expected: MysqlTopologyState,
        /// Current manager state.
        actual: MysqlTopologyState,
    },
    /// One exact member lifecycle operation failed.
    Instance {
        /// Member whose operation failed.
        member: MysqlMemberIndex,
        /// Exact member lifecycle failure.
        error: MysqlInstanceError,
    },
    /// One member-scoped native effect failed with its local cleanup evidence.
    MemberTransition {
        /// Exact member whose effect failed.
        member: MysqlMemberIndex,
        /// Member-local effect and paired containment failure, when any.
        error: MemberTransitionError,
    },
    /// Native control or topology evidence validation failed.
    Topology(MysqlTopologyError),
    /// The manager could not construct an exact observer request.
    ObservationRequest(RequestError),
    /// Primary failure plus every topology-wide containment failure.
    Cleanup {
        /// Original operation failure, absent for an explicit stop failure.
        primary: Option<Box<Self>>,
        /// Every per-member cleanup failure in fixed member order.
        failures: Vec<MemberCleanupFailure>,
    },
}

impl fmt::Display for MysqlTopologyManagerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "MySQL topology manager failure: {self:?}")
    }
}

impl std::error::Error for MysqlTopologyManagerError {}

/// One bounded lifecycle failure.
#[derive(Debug)]
pub enum MysqlInstanceError {
    /// Input or filesystem configuration was invalid.
    Config(ConfigError),
    /// The requested transition was invalid.
    InvalidState {
        /// State required by the operation.
        expected: MysqlInstanceState,
        /// State observed by the operation.
        actual: MysqlInstanceState,
    },
    /// Exact product validation failed.
    Product(ProductError),
    /// A filesystem or process operation failed.
    Io {
        /// Operation being performed.
        operation: LifecycleOperation,
        /// Stable operating-system error category.
        kind: ErrorKind,
    },
    /// A bounded operation exceeded its deadline.
    Timeout(LifecycleOperation),
    /// Shared attempt cancellation or work-cutoff admission stopped a process
    /// lifecycle poll.
    Attempt(AttemptAdmissionError),
    /// A child exited unsuccessfully.
    ChildFailure {
        /// Operation whose child failed.
        operation: LifecycleOperation,
        /// Optional process exit code.
        code: Option<i32>,
    },
    /// Retained child ownership could not be proven.
    Ownership(OwnershipError),
    /// An observation request selected a socket other than the owned socket.
    ObservationSocketMismatch,
    /// The owned socket still existed after the child was reaped.
    SocketStillPresent,
    /// Scratch cleanup failed after another lifecycle failure. Both failures
    /// are retained.
    CleanupAfter {
        /// Original lifecycle failure.
        prior: Box<Self>,
        /// Cleanup failure.
        cleanup: Box<Self>,
    },
}

impl fmt::Display for MysqlInstanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "MySQL instance lifecycle failure: {self:?}")
    }
}

impl std::error::Error for MysqlInstanceError {}

impl From<ConfigError> for MysqlInstanceError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}
