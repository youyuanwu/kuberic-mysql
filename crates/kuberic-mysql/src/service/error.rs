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
    /// Two members used the same reported SQL address.
    DuplicateSqlAddress,
    /// A Group Replication address used port zero.
    ZeroGroupReplicationPort,
    /// A Group Replication address was not loopback-only.
    NonLoopbackGroupReplicationAddress,
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
