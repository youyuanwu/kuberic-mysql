//! Bounded, restart-stateless ownership of local Oracle MySQL processes.
//!
//! [`MysqlInstanceManager`] owns one fresh Oracle MySQL Community Server
//! 8.4.11 generation. [`MysqlTopologyManager`] composes exactly three existing
//! instance managers for one designated bootstrap and two sequential joins.
//! Process identity and lifecycle state exist only in memory. Losing a manager
//! means losing ownership context: callers must reset the fixture rather than
//! discover, adopt, or continue surviving processes.
//!
//! Persistent MySQL files live beneath the caller-selected data root. Generated
//! configuration, logs, temporary files, PID state, and the private Unix socket
//! live beneath a separate disposable scratch root. No application metadata
//! file, journal, receipt database, state JSON, or adoption record is created.
//!
//! The managers do not publish client access or implement Kuberic callbacks.
//! Member-local setup and bounded bootstrap/join control use a separate root
//! session over the currently owned private socket. Observation remains
//! delegated unchanged to
//! [`crate::adapter::MysqlObserver`].

mod config;
mod control;
mod error;
mod instance;
mod process;
mod topology;

pub use config::{
    MysqlInstanceConfig, MysqlMemberConfig, MysqlMemberIndex, MysqlOperationTimeouts,
    MysqlRuntimePaths, MysqlTopologyConfig,
};
pub use error::{
    ConfigError, ControlStage, LifecycleOperation, MemberCleanupFailure, MysqlInstanceError,
    MysqlTopologyError, MysqlTopologyManagerError, OwnershipError, ProductError,
    TopologyAuthorityError, TopologyEvidenceError, TopologyGtidError, TopologyNativeStateError,
    TopologyStateError,
};
pub use instance::{MysqlInstanceManager, MysqlInstanceState};
pub use topology::{
    AcceptedMysqlTopology, AccountProvisioningEvidence, BootstrapCapability, BootstrapEffect,
    ControlCredential, ControlCredentialRole, ControlStep, CredentialError, JoinCapability,
    JoinEffect, MemberControlBinding, MysqlTopologyManager, MysqlTopologyState,
    NativeControlDeadline, NativeIdentityEnrollment, ObservedLocalBinding, SourceGtidBoundary,
    TopologyAttempt, TopologyAuthority, TopologyInstant, TopologyObservation,
    TopologyObservationContext, TopologyObservationStatus, TransitionCredit, TransitionEvaluation,
    ViewDiscovery,
};
