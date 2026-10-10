//! Typed, non-secret lifecycle failures.

use core::fmt;
use std::io::ErrorKind;

use crate::service::MysqlInstanceState;

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
    /// Explicit connected-session teardown.
    Disconnect,
}

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
