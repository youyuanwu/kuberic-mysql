//! Bounded, restart-stateless ownership of local Oracle MySQL processes.
//!
//! [`MysqlInstanceManager`] owns one fresh Oracle MySQL Community Server
//! 8.4.11 generation. [`MysqlTopologyManager`] composes exactly three existing
//! instance managers for one designated bootstrap and two sequential fresh
//! joins. Each transition is admitted from exact enrolled process, storage,
//! endpoint, credential-generation, native-identity, view, and structured GTID
//! evidence. `RECOVERING` remains pending; only exact `ONLINE` evidence grants
//! lifecycle credit. Join credit additionally requires Oracle's qualified XCom
//! view identity to be the exact `<fixed u64>:<predecessor u32 + 1>` successor.
//! One manager-owned monotonic clock and absolute attempt deadline bound every
//! control and observation operation.
//!
//! Process identity, capabilities, credentials, pending effects, and lifecycle
//! state exist only in memory. Losing a manager means losing ownership context:
//! callers must keep access closed and reset the fixture rather than discover,
//! adopt, or continue surviving processes.
//! Cancelling bootstrap synchronously contains the exact owned topology unless
//! bootstrap-off plus its proof already completed; query futures are never
//! detached.
//!
//! Persistent MySQL files live beneath the caller-selected data root. Generated
//! configuration, logs, temporary files, PID state, and the private Unix socket
//! live beneath a separate disposable scratch root. No application metadata
//! file, journal, receipt database, state JSON, or adoption record is created.
//!
//! The managers do not publish client access or implement Kuberic callbacks,
//! access reconciliation, switchover, failover, repair, or restart
//! continuation. Member-local setup and bounded bootstrap/join control use a
//! separate root session over the currently owned private socket. Observation
//! remains delegated unchanged to [`crate::adapter::MysqlObserver`].
//! [`MysqlInstanceConfig::new`] retains the original single-instance profile;
//! topology members require [`MysqlInstanceConfig::new_topology_member`].

mod config;
mod control;
mod error;
mod instance;
mod process;
mod topology;

pub use config::{
    MysqlInstanceConfig, MysqlMemberConfig, MysqlMemberIndex, MysqlOperationTimeouts,
    MysqlOwnedPathKind, MysqlRuntimePaths, MysqlTopologyConfig,
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
    JoinEffect, MemberControlBinding, MysqlTopologyManager, MysqlTopologyMemberRuntime,
    MysqlTopologyState, NativeControlDeadline, NativeIdentityEnrollment, ObservedLocalBinding,
    SourceGtidBoundary, SystemTopologyClock, TopologyAttempt, TopologyAuthority, TopologyClock,
    TopologyInstant, TopologyObservation, TopologyObservationContext, TopologyObservationStatus,
    TransitionCredit, TransitionEvaluation, ViewDiscovery,
};
