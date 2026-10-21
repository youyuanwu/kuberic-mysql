//! Bounded, restart-stateless ownership of local Oracle MySQL processes.
//!
//! [`MysqlInstanceManager`] owns one fresh Oracle MySQL Community Server
//! 8.4 generation. [`MysqlTopologyManager`] composes exactly three existing
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
//! [`MysqlStoppedReopen`] separately supports one exactly owned quiet graceful
//! stop and closed native reopen while its linear in-memory capability survives.
//! It preserves native logs and requires explicit generic caller authority;
//! it never resumes serving or restores role/configuration/progress authority.
//! Cancelling bootstrap synchronously contains the exact owned topology unless
//! bootstrap-off plus its proof already completed; query futures are never
//! detached.
//!
//! Persistent MySQL files live beneath the caller-selected data root. Generated
//! configuration, logs, temporary files, PID state, and the private Unix socket
//! live beneath a separate disposable scratch root. No application metadata
//! file, journal, receipt database, state JSON, or adoption record is created.
//!
//! [`MysqlReplicator`] may publish one bounded loopback Classic Protocol
//! gateway for an explicit all-or-nothing access generation and proves proxy
//! zero plus exact native-session absence before revocation completes.
//! [`kuberic`] translates the pinned Kuberic Public V1 callback bundle into
//! this member-local engine for fresh bootstrap and
//! already-formed planned transitions. Build/reseed, removal, data loss,
//! automated failover, repair, and restart continuation remain unsupported.
//! Member-local setup and bounded native control use a separate root session
//! over the currently owned private socket. Observation remains delegated
//! unchanged to [`crate::adapter::MysqlObserver`].
//! [`MysqlInstanceConfig::new`] retains the original single-instance profile;
//! topology members require [`MysqlInstanceConfig::new_topology_member`].

mod attempt;
mod config;
mod control;
mod error;
mod instance;
pub mod kuberic;
mod member;
mod process;
mod proxy;
#[cfg(test)]
mod qualification;
mod reopen;
mod replicator;
mod topology;

pub use attempt::{
    AttemptAdmissionError, AttemptCancellationState, AttemptCleanupState, AttemptContext,
    AttemptDeadlineState, BuildAttemptContext, BuildAttemptGroup, PairedAttemptOutcome,
};
pub use config::{
    MysqlInstanceConfig, MysqlMemberConfig, MysqlMemberIndex, MysqlOperationTimeouts,
    MysqlOwnedPathKind, MysqlRuntimePaths, MysqlTopologyConfig,
};
pub use error::{
    ConfigError, ControlStage, LifecycleOperation, MemberAuthorityError, MemberCleanupFailure,
    MemberReceiptError, MemberTransitionError, MemberViewError, MysqlInstanceError,
    MysqlProxyError, MysqlProxyPhase, MysqlProxyProtocolError, MysqlProxySettlementReason,
    MysqlReplicatorError, MysqlTopologyError, MysqlTopologyManagerError, OwnershipError,
    ProductError, TopologyAuthorityError, TopologyEvidenceError, TopologyGtidError,
    TopologyNativeStateError, TopologyStateError,
};
pub(crate) use instance::MysqlBuildAbortGeneration;
pub use instance::{MysqlInstanceManager, MysqlInstanceState};
pub use kuberic::{
    CallbackOperationLedger, EpochTranslator, KubericTranslationError, MysqlPublicV1Replicator,
    MysqlPublicV1ReplicatorFactory, MysqlPublicV1Service, MysqlPublicV1ServiceConfig,
    ReplicationEndpoint, TranslationResult, translate_build_replica_information,
    translate_process_session_id, translate_replica_identity, translate_replica_information,
    translate_replica_set_configuration, translate_role, translate_storage_binding,
};
pub use member::{
    BackendConnectionId, BackendConnectionReceipt, CanonicalViewId, FreshBootstrapInput,
    FreshJoinInput, MemberConfigurationIntent, MemberConfigurationTarget, MemberContainmentReceipt,
    MemberEffectKind, MemberEffectReceipt, MemberEffectRequest, MemberMutationAuthority,
    MemberNativeEvidence, MemberOperationIdentity, MemberPeer, MemberSourceBoundary,
    MysqlMemberEffectBoundary, PrimaryTransferInput,
};
pub use proxy::{
    APPLICATION_QUALIFICATION_SCHEMA, ApplicationAccountReceipt, ApplicationCredential,
    BackendSessionIdentity, MysqlClientProxy, MysqlProxyAccessGuard, MysqlProxyContainmentReceipt,
    MysqlProxyControlBoundary, MysqlProxyDiagnostics, MysqlProxySnapshot,
};
pub use reopen::{
    MysqlClosedReopened, MysqlProxyReopenClosure, MysqlReopenAuthority, MysqlReopenBoundary,
    MysqlReopenError, MysqlReopenEvidence, MysqlReopenRenewal, MysqlStoppedReopen,
};
pub use replicator::{
    MysqlAccessIntent, MysqlAccessOutcome, MysqlConfigurationOutcome, MysqlEpochOutcome,
    MysqlFormationRoleOutcome, MysqlFreshMemberCredentials, MysqlFreshMemberInput,
    MysqlHandoffBoundary, MysqlHandoffBoundaryId, MysqlHandoffOutcome,
    MysqlHandoffPublicationOutcome, MysqlHandoffPublicationProof, MysqlOpenOutcome,
    MysqlOpenReceipt, MysqlOperationIdentity, MysqlPeerAuthorityFacts, MysqlPeerOperationIntent,
    MysqlPeerOperationKind, MysqlPeerOperationOutcome, MysqlPeerUnsupportedReason,
    MysqlProgressOutcome, MysqlReplicaConfiguration, MysqlReplicaEpoch, MysqlReplicaIdentity,
    MysqlReplicaRole, MysqlReplicaTarget, MysqlReplicator, MysqlRoleOutcome,
    MysqlRuntimeAccessAuthority, MysqlSourceTerminalReceipt, MysqlTerminalOutcome,
    MysqlTerminalState,
};
pub use topology::{
    AcceptedMysqlTopology, AccountProvisioningEvidence, BootstrapCapability, BootstrapEffect,
    ControlCredential, ControlCredentialRole, ControlStep, CredentialError, JoinCapability,
    JoinEffect, MemberControlBinding, MysqlTopologyManager, MysqlTopologyMemberRuntime,
    MysqlTopologyState, NativeControlDeadline, NativeIdentityEnrollment, ObservedLocalBinding,
    SourceGtidBoundary, SystemTopologyClock, TopologyAttempt, TopologyAuthority, TopologyClock,
    TopologyInstant, TopologyObservation, TopologyObservationContext, TopologyObservationStatus,
    TransitionCredit, TransitionEvaluation, ViewDiscovery,
};
