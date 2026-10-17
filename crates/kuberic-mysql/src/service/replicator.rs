//! One-member lifecycle ordering for a future public runtime adapter.
//!
//! The engine owns one injected member-effect boundary. Production uses
//! [`MysqlInstanceManager`]; deterministic contracts may inject scripted
//! effects. It stores no peer process owner; client publication is available
//! only through the generation-fenced bounded proxy.

use std::net::SocketAddr;
use std::num::NonZeroU64;

use crate::core::{
    AttemptId, AuthorityGeneration, CatchUpCapabilityResult, ConfigurationId,
    ConfiguredGtidLineage, GtidSet, MemberRole, MemberState, ProgressProjection, QualifiedProgress,
    ReplicaId, ReplicaIncarnation, ViewId, project_progress,
};
use crate::service::{
    ApplicationAccountReceipt, ApplicationCredential, AttemptAdmissionError,
    AttemptCancellationState, AttemptCleanupState, AttemptContext, AttemptDeadlineState,
    CanonicalViewId, ControlCredential, ControlCredentialRole, FreshBootstrapInput, FreshJoinInput,
    MemberConfigurationIntent, MemberConfigurationTarget, MemberContainmentReceipt,
    MemberEffectKind, MemberEffectReceipt, MemberEffectRequest, MemberMutationAuthority,
    MemberNativeEvidence, MemberOperationIdentity, MemberPeer, MemberSourceBoundary,
    MysqlBuildAbortGeneration, MysqlClientProxy, MysqlInstanceManager, MysqlMemberEffectBoundary,
    MysqlProxyAccessGuard, MysqlProxyContainmentReceipt, MysqlProxyControlBoundary,
    MysqlReplicatorError, PrimaryTransferInput,
};

/// Exact logical identity of one member-local replica incarnation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlReplicaIdentity {
    replica_id: ReplicaId,
    incarnation: ReplicaIncarnation,
}

fn map_cleanup_admission(error: AttemptAdmissionError) -> MysqlReplicatorError {
    match error {
        AttemptAdmissionError::Expired => {
            MysqlReplicatorError::Deadline(AttemptDeadlineState::Expired)
        }
        AttemptAdmissionError::Cancelled | AttemptAdmissionError::CleanupReserve => {
            unreachable!("cleanup admission ignores cancellation and cleanup reserve")
        }
    }
}

impl MysqlReplicaIdentity {
    /// Creates one explicit logical replica incarnation.
    #[must_use]
    pub const fn new(replica_id: ReplicaId, incarnation: ReplicaIncarnation) -> Self {
        Self {
            replica_id,
            incarnation,
        }
    }

    /// Logical replica identity.
    #[must_use]
    pub const fn replica_id(&self) -> &ReplicaId {
        &self.replica_id
    }

    /// Exact incarnation identity.
    #[must_use]
    pub const fn incarnation(&self) -> &ReplicaIncarnation {
        &self.incarnation
    }
}

/// One explicit adapter operation identity and monotonic generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlOperationIdentity {
    id: AuthorityGeneration,
    generation: NonZeroU64,
}

impl MysqlOperationIdentity {
    /// Creates one nonzero operation generation.
    #[must_use]
    pub const fn new(id: AuthorityGeneration, generation: NonZeroU64) -> Self {
        Self { id, generation }
    }

    /// Opaque operation identity.
    #[must_use]
    pub const fn id(&self) -> &AuthorityGeneration {
        &self.id
    }

    /// Monotonic operation generation.
    #[must_use]
    pub const fn generation(&self) -> NonZeroU64 {
        self.generation
    }

    fn member_identity(&self) -> MemberOperationIdentity {
        MemberOperationIdentity::new(self.id.clone(), self.generation)
    }
}

/// Monotonic lifecycle epoch used only by the MySQL-owned adapter seam.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MysqlReplicaEpoch(u64);

impl MysqlReplicaEpoch {
    /// Creates an epoch value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Numeric epoch.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Public role intent understood by the MySQL lifecycle engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MysqlReplicaRole {
    /// Online non-primary member.
    Secondary,
    /// Online single-primary member.
    Primary,
}

impl MysqlReplicaRole {
    fn native(self) -> MemberRole {
        match self {
            Self::Secondary => MemberRole::Secondary,
            Self::Primary => MemberRole::Primary,
        }
    }
}

/// Explicit non-owning identity of one configured replica.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlReplicaTarget {
    identity: MysqlReplicaIdentity,
    member: MemberPeer,
    process_session: crate::core::ProcessSessionId,
    endpoint: crate::core::EndpointBinding,
    storage: crate::core::StorageBinding,
}

impl MysqlReplicaTarget {
    /// Creates one configured target without transferring process ownership.
    #[must_use]
    pub const fn new(
        identity: MysqlReplicaIdentity,
        member: MemberPeer,
        process_session: crate::core::ProcessSessionId,
        endpoint: crate::core::EndpointBinding,
        storage: crate::core::StorageBinding,
    ) -> Self {
        Self {
            identity,
            member,
            process_session,
            endpoint,
            storage,
        }
    }

    /// Logical target identity.
    #[must_use]
    pub const fn identity(&self) -> &MysqlReplicaIdentity {
        &self.identity
    }

    /// Native peer identity.
    #[must_use]
    pub const fn member(&self) -> &MemberPeer {
        &self.member
    }

    /// Exact process-session identity.
    #[must_use]
    pub const fn process_session(&self) -> &crate::core::ProcessSessionId {
        &self.process_session
    }

    /// Exact endpoint binding.
    #[must_use]
    pub const fn endpoint(&self) -> &crate::core::EndpointBinding {
        &self.endpoint
    }

    /// Exact storage binding.
    #[must_use]
    pub const fn storage(&self) -> &crate::core::StorageBinding {
        &self.storage
    }

    fn member_configuration_target(&self) -> MemberConfigurationTarget {
        MemberConfigurationTarget::new(
            self.identity.replica_id.clone(),
            self.identity.incarnation.clone(),
            self.member.clone(),
            self.process_session.clone(),
            self.endpoint.clone(),
            self.storage.clone(),
        )
    }
}

/// MySQL-owned configuration value accepted after role for the same epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlReplicaConfiguration {
    id: ConfigurationId,
    epoch: MysqlReplicaEpoch,
    members: Vec<MysqlReplicaTarget>,
}

impl MysqlReplicaConfiguration {
    /// Creates one explicit configuration value.
    #[must_use]
    pub fn new(
        id: ConfigurationId,
        epoch: MysqlReplicaEpoch,
        members: Vec<MysqlReplicaTarget>,
    ) -> Self {
        Self { id, epoch, members }
    }

    /// Configuration identity.
    #[must_use]
    pub const fn id(&self) -> &ConfigurationId {
        &self.id
    }

    /// Configuration epoch.
    #[must_use]
    pub const fn epoch(&self) -> MysqlReplicaEpoch {
        self.epoch
    }

    /// Explicit configured members.
    #[must_use]
    pub fn members(&self) -> &[MysqlReplicaTarget] {
        &self.members
    }

    fn member_intent(&self) -> MemberConfigurationIntent {
        MemberConfigurationIntent::new(
            self.id.clone(),
            self.members
                .iter()
                .map(MysqlReplicaTarget::member_configuration_target)
                .collect(),
        )
    }
}

/// Typed exact proof admitted by [`MysqlReplicator::open`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlOpenReceipt {
    identity: MysqlReplicaIdentity,
    authority: MemberMutationAuthority,
    access_generation: NonZeroU64,
}

impl MysqlOpenReceipt {
    /// Creates an open receipt from exact enrolled member authority.
    #[must_use]
    pub const fn new(
        identity: MysqlReplicaIdentity,
        authority: MemberMutationAuthority,
        access_generation: NonZeroU64,
    ) -> Self {
        Self {
            identity,
            authority,
            access_generation,
        }
    }

    /// Exact replica incarnation.
    #[must_use]
    pub const fn identity(&self) -> &MysqlReplicaIdentity {
        &self.identity
    }

    /// Exact local member authority.
    #[must_use]
    pub const fn authority(&self) -> &MemberMutationAuthority {
        &self.authority
    }

    /// Initial closed access generation.
    #[must_use]
    pub const fn access_generation(&self) -> NonZeroU64 {
        self.access_generation
    }
}

/// Credential buffers transferred into one fresh member owner.
pub struct MysqlFreshMemberCredentials {
    observer: ControlCredential,
    recovery: ControlCredential,
    application: ApplicationCredential,
}

impl MysqlFreshMemberCredentials {
    /// Groups the three independently owned credential roles for one member.
    #[must_use]
    pub const fn new(
        observer: ControlCredential,
        recovery: ControlCredential,
        application: ApplicationCredential,
    ) -> Self {
        Self {
            observer,
            recovery,
            application,
        }
    }
}

/// Complete one-member input for fresh process preparation and enrollment.
pub struct MysqlFreshMemberInput {
    config: crate::service::MysqlInstanceConfig,
    identity: MysqlReplicaIdentity,
    lineage: ConfiguredGtidLineage,
    credentials: MysqlFreshMemberCredentials,
    attempt: AttemptContext,
    access_generation: NonZeroU64,
}

impl MysqlFreshMemberInput {
    /// Creates one explicit fresh-member ownership transfer.
    #[must_use]
    pub const fn new(
        config: crate::service::MysqlInstanceConfig,
        identity: MysqlReplicaIdentity,
        lineage: ConfiguredGtidLineage,
        credentials: MysqlFreshMemberCredentials,
        attempt: AttemptContext,
        access_generation: NonZeroU64,
    ) -> Self {
        Self {
            config,
            identity,
            lineage,
            credentials,
            attempt,
            access_generation,
        }
    }
}

/// Accepted lifecycle-open outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlOpenOutcome {
    identity: MysqlReplicaIdentity,
    access_generation: NonZeroU64,
}

impl MysqlOpenOutcome {
    /// Opened replica identity.
    #[must_use]
    pub const fn identity(&self) -> &MysqlReplicaIdentity {
        &self.identity
    }

    /// Initial access generation, which remains closed.
    #[must_use]
    pub const fn access_generation(&self) -> NonZeroU64 {
        self.access_generation
    }
}

/// Accepted newer-epoch barrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MysqlEpochOutcome {
    epoch: MysqlReplicaEpoch,
}

impl MysqlEpochOutcome {
    /// Accepted epoch.
    #[must_use]
    pub const fn epoch(self) -> MysqlReplicaEpoch {
        self.epoch
    }
}

/// Role completion credited from exact native evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MysqlRoleOutcome {
    epoch: MysqlReplicaEpoch,
    role: MysqlReplicaRole,
}

impl MysqlRoleOutcome {
    /// Epoch whose role transition completed.
    #[must_use]
    pub const fn epoch(self) -> MysqlReplicaEpoch {
        self.epoch
    }

    /// Accepted role.
    #[must_use]
    pub const fn role(self) -> MysqlReplicaRole {
        self.role
    }
}

/// Accepted fresh-formation role intent pending exact configuration evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MysqlFormationRoleOutcome {
    epoch: MysqlReplicaEpoch,
    role: MysqlReplicaRole,
}

impl MysqlFormationRoleOutcome {
    /// Epoch whose fresh role intent was accepted.
    #[must_use]
    pub const fn epoch(self) -> MysqlReplicaEpoch {
        self.epoch
    }

    /// Intended role that the bootstrap or join receipt must prove.
    #[must_use]
    pub const fn role(self) -> MysqlReplicaRole {
        self.role
    }
}

/// Configuration completion credited from exact native evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlConfigurationOutcome {
    configuration: MysqlReplicaConfiguration,
}

impl MysqlConfigurationOutcome {
    /// Accepted current configuration.
    #[must_use]
    pub const fn configuration(&self) -> &MysqlReplicaConfiguration {
        &self.configuration
    }
}

/// Currently verifiable peer facts. Future signed authorization stays external.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlPeerAuthorityFacts {
    target: MysqlReplicaTarget,
    process_session: crate::core::ProcessSessionId,
    endpoint: crate::core::EndpointBinding,
    storage: crate::core::StorageBinding,
}

impl MysqlPeerAuthorityFacts {
    /// Creates explicit non-secret target authority facts.
    #[must_use]
    pub const fn new(
        target: MysqlReplicaTarget,
        process_session: crate::core::ProcessSessionId,
        endpoint: crate::core::EndpointBinding,
        storage: crate::core::StorageBinding,
    ) -> Self {
        Self {
            target,
            process_session,
            endpoint,
            storage,
        }
    }

    /// Exact target.
    #[must_use]
    pub const fn target(&self) -> &MysqlReplicaTarget {
        &self.target
    }

    /// Target process-session fact.
    #[must_use]
    pub const fn process_session(&self) -> &crate::core::ProcessSessionId {
        &self.process_session
    }

    /// Target endpoint fact.
    #[must_use]
    pub const fn endpoint(&self) -> &crate::core::EndpointBinding {
        &self.endpoint
    }

    /// Target storage fact.
    #[must_use]
    pub const fn storage(&self) -> &crate::core::StorageBinding {
        &self.storage
    }
}

/// Ordered peer operation intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlPeerOperationIntent {
    operation: MysqlOperationIdentity,
    epoch: MysqlReplicaEpoch,
    configuration: ConfigurationId,
    authority: MysqlPeerAuthorityFacts,
}

impl MysqlPeerOperationIntent {
    /// Creates explicit build/removal admission input.
    #[must_use]
    pub const fn new(
        operation: MysqlOperationIdentity,
        epoch: MysqlReplicaEpoch,
        configuration: ConfigurationId,
        authority: MysqlPeerAuthorityFacts,
    ) -> Self {
        Self {
            operation,
            epoch,
            configuration,
            authority,
        }
    }

    /// Operation identity.
    #[must_use]
    pub const fn operation(&self) -> &MysqlOperationIdentity {
        &self.operation
    }

    /// Current epoch asserted by the caller.
    #[must_use]
    pub const fn epoch(&self) -> MysqlReplicaEpoch {
        self.epoch
    }

    /// Current configuration asserted by the caller.
    #[must_use]
    pub const fn configuration(&self) -> &ConfigurationId {
        &self.configuration
    }

    /// Explicit target authority facts.
    #[must_use]
    pub const fn authority(&self) -> &MysqlPeerAuthorityFacts {
        &self.authority
    }
}

/// Peer operation that is ordered but not executed in Stage 2.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MysqlPeerOperationKind {
    /// Generic peer build.
    Build,
    /// Generic configured-peer removal.
    Remove,
}

/// Why an admitted peer operation has no native completion claim.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MysqlPeerUnsupportedReason {
    /// Signed authorization and native build/removal execution are not in Stage 2.
    NativeExecutionUnavailable,
}

/// Typed result for an admitted build or removal entry point.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MysqlPeerOperationOutcome {
    /// Ordering and identity were valid, but no native completion was attempted.
    Unsupported {
        /// Ordered operation kind.
        kind: MysqlPeerOperationKind,
        /// Stable Stage 2 reason.
        reason: MysqlPeerUnsupportedReason,
        /// Exact non-local target.
        target: MysqlReplicaTarget,
    },
}

/// Explicit all-or-nothing access request. No endpoint is published in Phase 3.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MysqlAccessIntent {
    generation: NonZeroU64,
    read: bool,
    write: bool,
}

impl MysqlAccessIntent {
    /// Creates one explicit generation-bound access request.
    #[must_use]
    pub const fn new(generation: NonZeroU64, read: bool, write: bool) -> Self {
        Self {
            generation,
            read,
            write,
        }
    }

    /// Access generation.
    #[must_use]
    pub const fn generation(self) -> NonZeroU64 {
        self.generation
    }

    /// Requested read authority.
    #[must_use]
    pub const fn read(self) -> bool {
        self.read
    }

    /// Requested write authority.
    #[must_use]
    pub const fn write(self) -> bool {
        self.write
    }
}

/// Exact runtime-owned authority for publishing one primary access generation.
///
/// This is the neutral adapter seam for runtimes that durably own cross-member
/// lifecycle ordering. It does not replace local native role, configuration,
/// view, or progress validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlRuntimeAccessAuthority {
    epoch: MysqlReplicaEpoch,
    role: MysqlReplicaRole,
    configuration: ConfigurationId,
}

impl MysqlRuntimeAccessAuthority {
    /// Binds runtime access authority to the exact accepted primary state.
    #[must_use]
    pub const fn new(
        epoch: MysqlReplicaEpoch,
        role: MysqlReplicaRole,
        configuration: ConfigurationId,
    ) -> Self {
        Self {
            epoch,
            role,
            configuration,
        }
    }

    /// Exact lifecycle epoch.
    #[must_use]
    pub const fn epoch(&self) -> MysqlReplicaEpoch {
        self.epoch
    }

    /// Exact lifecycle role.
    #[must_use]
    pub const fn role(&self) -> MysqlReplicaRole {
        self.role
    }

    /// Exact current configuration identity.
    #[must_use]
    pub const fn configuration(&self) -> &ConfigurationId {
        &self.configuration
    }
}

/// Generation-bound publication or containment outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlAccessOutcome {
    intent: MysqlAccessIntent,
    endpoint: Option<SocketAddr>,
    containment: Option<MysqlProxyContainmentReceipt>,
}

impl MysqlAccessOutcome {
    /// Recorded access intent.
    #[must_use]
    pub const fn intent(&self) -> MysqlAccessIntent {
        self.intent
    }

    /// Published loopback application endpoint, when this outcome opened one.
    #[must_use]
    pub const fn is_open(&self) -> bool {
        self.endpoint.is_some()
    }

    /// Published loopback endpoint.
    #[must_use]
    pub const fn endpoint(&self) -> Option<SocketAddr> {
        self.endpoint
    }

    /// Generation-containment receipt for a closure outcome.
    #[must_use]
    pub const fn containment(&self) -> Option<&MysqlProxyContainmentReceipt> {
        self.containment.as_ref()
    }
}

/// Independently evaluated progress and catch-up results.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MysqlProgressOutcome {
    progress: ProgressProjection,
    catch_up: CatchUpCapabilityResult,
}

/// Frozen source boundary required by one controlled handoff.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlHandoffBoundaryId {
    attempt: AttemptId,
    freeze_operation: MysqlOperationIdentity,
    revoked_generation: NonZeroU64,
}

impl MysqlHandoffBoundaryId {
    /// Attempt that issued this boundary.
    #[must_use]
    pub const fn attempt(&self) -> &AttemptId {
        &self.attempt
    }

    /// Exact source observation operation that issued this boundary.
    #[must_use]
    pub const fn freeze_operation(&self) -> &MysqlOperationIdentity {
        &self.freeze_operation
    }

    /// Exact serving generation whose eligible closure preceded observation.
    #[must_use]
    pub const fn revoked_generation(&self) -> NonZeroU64 {
        self.revoked_generation
    }
}

/// Frozen source boundary required by one controlled handoff.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlHandoffBoundary {
    id: MysqlHandoffBoundaryId,
    closure: MysqlProxyContainmentReceipt,
    configuration: MysqlReplicaConfiguration,
    source_epoch: MysqlReplicaEpoch,
    source_authority: MemberMutationAuthority,
    source: MysqlReplicaTarget,
    target: MysqlReplicaTarget,
    remaining: MysqlReplicaTarget,
    predecessor_view: ViewId,
    executed: GtidSet,
    progress: QualifiedProgress,
}

impl MysqlHandoffBoundary {
    /// Stable source-issued boundary identity.
    #[must_use]
    pub const fn id(&self) -> &MysqlHandoffBoundaryId {
        &self.id
    }

    /// Exact handoff-eligible generation closure that preceded the fresh
    /// source observation.
    #[must_use]
    pub const fn closure(&self) -> &MysqlProxyContainmentReceipt {
        &self.closure
    }

    /// Current three-member configuration retained across the planned stop.
    #[must_use]
    pub const fn configuration(&self) -> &MysqlReplicaConfiguration {
        &self.configuration
    }

    /// Epoch current on the source when the boundary was frozen.
    #[must_use]
    pub const fn source_epoch(&self) -> MysqlReplicaEpoch {
        self.source_epoch
    }

    /// Exact current source authority admitted when the boundary was frozen.
    #[must_use]
    pub const fn source_authority(&self) -> &MemberMutationAuthority {
        &self.source_authority
    }

    /// Source that must be the only absent successor member.
    #[must_use]
    pub const fn source(&self) -> &MysqlReplicaTarget {
        &self.source
    }

    /// Exact transfer target that must remain primary and online.
    #[must_use]
    pub const fn target(&self) -> &MysqlReplicaTarget {
        &self.target
    }

    /// Exact surviving secondary.
    #[must_use]
    pub const fn remaining(&self) -> &MysqlReplicaTarget {
        &self.remaining
    }

    /// Exact predecessor view frozen before the source stopped.
    #[must_use]
    pub const fn predecessor_view(&self) -> &ViewId {
        &self.predecessor_view
    }

    /// Complete structured source history frozen after eligible revocation.
    #[must_use]
    pub const fn executed(&self) -> &GtidSet {
        &self.executed
    }

    /// Qualified scalar projection of the frozen structured history.
    #[must_use]
    pub const fn progress(&self) -> QualifiedProgress {
        self.progress
    }
}

/// Accepted two-survivor controlled-handoff evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlHandoffOutcome {
    boundary: MysqlHandoffBoundary,
    identity: MysqlReplicaIdentity,
    authority: MemberMutationAuthority,
    configuration: MysqlReplicaConfiguration,
    successor: MemberNativeEvidence,
    epoch: MysqlReplicaEpoch,
    role: MysqlReplicaRole,
    progress: QualifiedProgress,
}

impl MysqlHandoffOutcome {
    /// Creates explicit successor evidence for transport to the target. The
    /// target validates every field before granting publication credit.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        boundary: MysqlHandoffBoundary,
        identity: MysqlReplicaIdentity,
        authority: MemberMutationAuthority,
        configuration: MysqlReplicaConfiguration,
        successor: MemberNativeEvidence,
        epoch: MysqlReplicaEpoch,
        role: MysqlReplicaRole,
        progress: QualifiedProgress,
    ) -> Self {
        Self {
            boundary,
            identity,
            authority,
            configuration,
            successor,
            epoch,
            role,
            progress,
        }
    }

    /// Exact frozen boundary accepted by this successor.
    #[must_use]
    pub const fn boundary(&self) -> &MysqlHandoffBoundary {
        &self.boundary
    }

    /// Exact accepting replica incarnation.
    #[must_use]
    pub const fn identity(&self) -> &MysqlReplicaIdentity {
        &self.identity
    }

    /// Exact accepting member authority.
    #[must_use]
    pub const fn authority(&self) -> &MemberMutationAuthority {
        &self.authority
    }

    /// Exact current configuration accepted by this successor.
    #[must_use]
    pub const fn configuration(&self) -> &MysqlReplicaConfiguration {
        &self.configuration
    }

    /// Exact canonical successor view and structured history accepted locally.
    #[must_use]
    pub const fn successor(&self) -> &MemberNativeEvidence {
        &self.successor
    }

    /// Current epoch whose unchanged configuration remains accepted.
    #[must_use]
    pub const fn epoch(&self) -> MysqlReplicaEpoch {
        self.epoch
    }

    /// Exact local role in the successor view.
    #[must_use]
    pub const fn role(&self) -> MysqlReplicaRole {
        self.role
    }

    /// Current qualified scalar containing the frozen boundary.
    #[must_use]
    pub const fn progress(&self) -> QualifiedProgress {
        self.progress
    }
}

/// Exact source close/stop/reap evidence bound to the admitted source
/// incarnation, process session, storage, endpoint, and native identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlSourceTerminalReceipt {
    operation: MysqlOperationIdentity,
    identity: MysqlReplicaIdentity,
    source: MysqlReplicaTarget,
    authority: MemberMutationAuthority,
    state: MysqlTerminalState,
    containment: MemberContainmentReceipt,
    boundary: Option<MysqlHandoffBoundaryId>,
}

impl MysqlSourceTerminalReceipt {
    /// Creates explicit terminal evidence. Publication accepts only exact,
    /// complete `Closed` evidence matching the frozen source boundary.
    #[must_use]
    pub(crate) const fn new(
        operation: MysqlOperationIdentity,
        identity: MysqlReplicaIdentity,
        source: MysqlReplicaTarget,
        authority: MemberMutationAuthority,
        state: MysqlTerminalState,
        containment: MemberContainmentReceipt,
        boundary: Option<MysqlHandoffBoundaryId>,
    ) -> Self {
        Self {
            operation,
            identity,
            source,
            authority,
            state,
            containment,
            boundary,
        }
    }

    /// Exact close or abort operation identity.
    #[must_use]
    pub const fn operation(&self) -> &MysqlOperationIdentity {
        &self.operation
    }

    /// Exact logical source incarnation.
    #[must_use]
    pub const fn identity(&self) -> &MysqlReplicaIdentity {
        &self.identity
    }

    /// Exact configured source identity and binding.
    #[must_use]
    pub const fn source(&self) -> &MysqlReplicaTarget {
        &self.source
    }

    /// Exact source member authority.
    #[must_use]
    pub const fn authority(&self) -> &MemberMutationAuthority {
        &self.authority
    }

    /// Terminal lifecycle state.
    #[must_use]
    pub const fn state(&self) -> MysqlTerminalState {
        self.state
    }

    /// Production effect-boundary stop/reap/absence receipt.
    #[must_use]
    pub const fn containment(&self) -> &MemberContainmentReceipt {
        &self.containment
    }

    /// Source-issued frozen boundary completed by this terminal receipt.
    #[must_use]
    pub const fn boundary(&self) -> Option<&MysqlHandoffBoundaryId> {
        self.boundary.as_ref()
    }
}

/// Combined handoff evidence submitted to the target before publication.
///
/// Optional components make incomplete transport or durable reconstruction
/// fail closed at the target operation rather than being inferred.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlHandoffPublicationProof {
    boundary: MysqlHandoffBoundary,
    target: Option<MysqlHandoffOutcome>,
    remaining: Option<MysqlHandoffOutcome>,
    source: Option<MysqlSourceTerminalReceipt>,
}

impl MysqlHandoffPublicationProof {
    /// Creates one proof submission, including intentionally incomplete input
    /// that the target must reject.
    #[must_use]
    pub const fn new(
        boundary: MysqlHandoffBoundary,
        target: Option<MysqlHandoffOutcome>,
        remaining: Option<MysqlHandoffOutcome>,
        source: Option<MysqlSourceTerminalReceipt>,
    ) -> Self {
        Self {
            boundary,
            target,
            remaining,
            source,
        }
    }

    /// Exact frozen source boundary.
    #[must_use]
    pub const fn boundary(&self) -> &MysqlHandoffBoundary {
        &self.boundary
    }

    /// Exact target successor acceptance.
    #[must_use]
    pub const fn target(&self) -> Option<&MysqlHandoffOutcome> {
        self.target.as_ref()
    }

    /// Exact remaining successor acceptance.
    #[must_use]
    pub const fn remaining(&self) -> Option<&MysqlHandoffOutcome> {
        self.remaining.as_ref()
    }

    /// Exact source terminal close receipt.
    #[must_use]
    pub const fn source(&self) -> Option<&MysqlSourceTerminalReceipt> {
        self.source.as_ref()
    }
}

/// Accepted publication-proof result. Access is still closed until a separate
/// newer generation is explicitly published.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlHandoffPublicationOutcome {
    boundary: MysqlHandoffBoundary,
    epoch: MysqlReplicaEpoch,
    configuration: MysqlReplicaConfiguration,
    target: MysqlReplicaIdentity,
}

impl MysqlHandoffPublicationOutcome {
    /// Exact frozen source boundary.
    #[must_use]
    pub const fn boundary(&self) -> &MysqlHandoffBoundary {
        &self.boundary
    }

    /// Current target epoch.
    #[must_use]
    pub const fn epoch(&self) -> MysqlReplicaEpoch {
        self.epoch
    }

    /// Current unchanged three-member configuration.
    #[must_use]
    pub const fn configuration(&self) -> &MysqlReplicaConfiguration {
        &self.configuration
    }

    /// Exact target incarnation granted publication readiness.
    #[must_use]
    pub const fn target(&self) -> &MysqlReplicaIdentity {
        &self.target
    }
}

impl MysqlProgressOutcome {
    /// Current qualified scalar projection.
    #[must_use]
    pub const fn progress(self) -> ProgressProjection {
        self.progress
    }

    /// Independent catch-up result.
    #[must_use]
    pub const fn catch_up(self) -> CatchUpCapabilityResult {
        self.catch_up
    }
}

/// Terminal lifecycle boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MysqlTerminalState {
    /// Settled close completed.
    Closed,
    /// Immediate abort was requested.
    Aborted,
}

/// Accepted terminal result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlTerminalOutcome {
    state: MysqlTerminalState,
    cleanup: AttemptCleanupState,
    receipt: Option<MysqlSourceTerminalReceipt>,
}

impl MysqlTerminalOutcome {
    /// Terminal state.
    #[must_use]
    pub const fn state(&self) -> MysqlTerminalState {
        self.state
    }

    /// Local cleanup evidence.
    #[must_use]
    pub const fn cleanup(&self) -> AttemptCleanupState {
        self.cleanup
    }

    /// Exact identity-bound terminal receipt.
    #[must_use]
    pub const fn receipt(&self) -> Option<&MysqlSourceTerminalReceipt> {
        self.receipt.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RoleEffect {
    Observe,
    Transfer {
        source: Box<MemberSourceBoundary>,
        target: MemberPeer,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ConfigurationEffect {
    Observe,
    Bootstrap,
    Join {
        source: Box<MemberSourceBoundary>,
        peers: Vec<MemberPeer>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AccessPublicationAuthority {
    IntentOnly,
    HandoffProof,
    Runtime(MysqlRuntimeAccessAuthority),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OperationFingerprint {
    Open(MysqlReplicaIdentity, NonZeroU64),
    Epoch(MysqlReplicaEpoch),
    FormationRole(MysqlReplicaEpoch, MysqlReplicaRole),
    Role(MysqlReplicaEpoch, MysqlReplicaRole, RoleEffect),
    Configuration(MysqlReplicaConfiguration, ConfigurationEffect),
    Progress {
        require_recorded_native_role: bool,
    },
    HandoffFreeze(
        Box<(
            MysqlReplicaTarget,
            MysqlReplicaTarget,
            MysqlProxyContainmentReceipt,
        )>,
    ),
    HandoffSuccessor(Box<MysqlHandoffBoundary>),
    HandoffPublication(Box<MysqlHandoffPublicationProof>),
    Access(MysqlAccessIntent, AccessPublicationAuthority),
    Peer(MysqlPeerOperationKind, Box<MysqlPeerOperationIntent>),
    Close,
    Abort,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RecordedOutcome {
    Open(MysqlOpenOutcome),
    Epoch(MysqlEpochOutcome),
    FormationRole(MysqlFormationRoleOutcome),
    Role(MysqlRoleOutcome),
    Configuration(MysqlConfigurationOutcome),
    Progress(MysqlProgressOutcome),
    HandoffBoundary(Box<MysqlHandoffBoundary>),
    Handoff(Box<MysqlHandoffOutcome>),
    HandoffPublication(Box<MysqlHandoffPublicationOutcome>),
    Access(MysqlAccessOutcome),
    Peer(MysqlPeerOperationOutcome),
    Terminal(Box<MysqlTerminalOutcome>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OperationRejection {
    ConfigurationRequired,
    PrimaryRequired,
    SelfTarget,
    InvalidConfiguration,
    IdentityMismatch,
}

impl OperationRejection {
    const fn error(self) -> MysqlReplicatorError {
        match self {
            Self::ConfigurationRequired => MysqlReplicatorError::ConfigurationRequired,
            Self::PrimaryRequired => MysqlReplicatorError::PrimaryRequired,
            Self::SelfTarget => MysqlReplicatorError::SelfTarget,
            Self::InvalidConfiguration => MysqlReplicatorError::InvalidConfiguration,
            Self::IdentityMismatch => MysqlReplicatorError::IdentityMismatch,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OperationStatus {
    Pending,
    Uncertain,
    Completed(RecordedOutcome),
    Rejected(OperationRejection),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OperationRecord {
    operation: MysqlOperationIdentity,
    fingerprint: OperationFingerprint,
    status: OperationStatus,
}

enum Admission {
    New(usize),
    Retry(RecordedOutcome),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AcceptedHandoffPublicationProof {
    proof: MysqlHandoffPublicationProof,
    epoch: MysqlReplicaEpoch,
    role: MysqlReplicaRole,
    configuration: MysqlReplicaConfiguration,
    authority: MemberMutationAuthority,
    operation_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SourceHandoffState {
    boundary: MysqlHandoffBoundary,
    newer_epoch: Option<MysqlReplicaEpoch>,
    demoted: bool,
    configuration_accepted: bool,
}

/// Public one-member MySQL lifecycle engine.
///
/// `E = MysqlInstanceManager` is the production instantiation. Other
/// implementations exist only to provide deterministic effect scripts.
pub struct MysqlReplicator<E = MysqlInstanceManager>
where
    E: MysqlMemberEffectBoundary,
{
    effects: E,
    identity: MysqlReplicaIdentity,
    lineage: ConfiguredGtidLineage,
    observer: ControlCredential,
    recovery: ControlCredential,
    attempt: AttemptContext,
    authority: Option<MemberMutationAuthority>,
    owned_authority: Option<MemberMutationAuthority>,
    epoch: Option<MysqlReplicaEpoch>,
    role: Option<MysqlReplicaRole>,
    role_epoch: Option<MysqlReplicaEpoch>,
    configuration: Option<MysqlReplicaConfiguration>,
    native_evidence: Option<MemberNativeEvidence>,
    progress: Option<MysqlProgressOutcome>,
    access: Option<MysqlAccessIntent>,
    application_credential: Option<ApplicationCredential>,
    pending_application_account: Option<ApplicationAccountReceipt>,
    proxy: Option<MysqlClientProxy>,
    proxy_access_guard: std::sync::Arc<dyn MysqlProxyAccessGuard>,
    proxy_containment: Option<MysqlProxyContainmentReceipt>,
    source_handoff: Option<Box<SourceHandoffState>>,
    source_handoff_invalidated: bool,
    handoff_publication_required: bool,
    handoff_publication_proof: Option<AcceptedHandoffPublicationProof>,
    terminal: Option<MysqlTerminalState>,
    terminal_outcome: Option<Box<MysqlTerminalOutcome>>,
    fresh_prepared_owner: bool,
    last_operation_generation: u64,
    operations: Vec<OperationRecord>,
}

impl<E> Drop for MysqlReplicator<E>
where
    E: MysqlMemberEffectBoundary,
{
    fn drop(&mut self) {
        if self.terminal_outcome.is_none() {
            if let Some(proxy) = self.proxy.as_ref() {
                proxy.seal_now();
            }
            self.attempt.cancel();
            let _ = self.effects.contain_member(&self.attempt);
            self.terminal = Some(MysqlTerminalState::Aborted);
        }
        self.clear_all_credentials();
    }
}

impl MysqlReplicator<MysqlInstanceManager> {
    /// Arms exact ownership for one application Build interruption guard.
    #[allow(dead_code)]
    pub(crate) fn arm_build_abort_generation(
        &mut self,
    ) -> Result<MysqlBuildAbortGeneration, MysqlReplicatorError> {
        let authority = self
            .authority
            .clone()
            .or_else(|| self.owned_authority.clone())
            .ok_or(MysqlReplicatorError::NotOpen)?;
        self.effects
            .arm_build_abort_generation(&authority)
            .map_err(MysqlReplicatorError::Instance)
    }

    /// Synchronously retires the exact owned MySQL generation even after an
    /// operation deadline. This is reserved for an armed application-owned
    /// Build cleanup guard; it grants no lifecycle completion credit.
    #[allow(dead_code)]
    pub(crate) fn force_retire_owned_generation(
        &mut self,
        generation: &MysqlBuildAbortGeneration,
    ) -> Result<crate::service::MemberContainmentReceipt, MysqlReplicatorError> {
        let receipt = self
            .effects
            .force_retire_owned_generation(generation)
            .map_err(MysqlReplicatorError::Instance)?;
        self.close_access();
        self.clear_all_credentials();
        self.terminal = Some(MysqlTerminalState::Aborted);
        Ok(receipt)
    }
}

impl<E> MysqlReplicator<E>
where
    E: MysqlMemberEffectBoundary,
{
    /// Creates a closed, unopened engine around one member-effect owner.
    pub fn new(
        effects: E,
        identity: MysqlReplicaIdentity,
        lineage: ConfiguredGtidLineage,
        observer: ControlCredential,
        recovery: ControlCredential,
        attempt: AttemptContext,
    ) -> Result<Self, MysqlReplicatorError> {
        if effects.config().topology().is_none() {
            return Err(MysqlReplicatorError::InvalidMemberConfiguration);
        }
        if observer.attempt() != attempt.attempt()
            || recovery.attempt() != attempt.attempt()
            || observer.role() != ControlCredentialRole::Observer
            || recovery.role() != ControlCredentialRole::Recovery
        {
            return Err(MysqlReplicatorError::IdentityMismatch);
        }
        Ok(Self {
            effects,
            identity,
            lineage,
            observer,
            recovery,
            attempt,
            authority: None,
            owned_authority: None,
            epoch: None,
            role: None,
            role_epoch: None,
            configuration: None,
            native_evidence: None,
            progress: None,
            access: None,
            application_credential: None,
            pending_application_account: None,
            proxy: None,
            proxy_access_guard: super::proxy::granted_proxy_access(),
            proxy_containment: None,
            source_handoff: None,
            source_handoff_invalidated: false,
            handoff_publication_required: false,
            handoff_publication_proof: None,
            terminal: None,
            terminal_outcome: None,
            fresh_prepared_owner: false,
            last_operation_generation: 0,
            operations: Vec::new(),
        })
    }

    /// Exact local replica incarnation.
    #[must_use]
    pub const fn identity(&self) -> &MysqlReplicaIdentity {
        &self.identity
    }

    /// Exact shared attempt context governing this engine.
    #[must_use]
    pub const fn attempt(&self) -> &AttemptContext {
        &self.attempt
    }

    /// Current accepted epoch.
    #[must_use]
    pub const fn epoch(&self) -> Option<MysqlReplicaEpoch> {
        self.epoch
    }

    /// Current intended role. Fresh formation proves it only when configuration completes.
    #[must_use]
    pub const fn role(&self) -> Option<MysqlReplicaRole> {
        self.role
    }

    /// Current accepted configuration.
    #[must_use]
    pub const fn configuration(&self) -> Option<&MysqlReplicaConfiguration> {
        self.configuration.as_ref()
    }

    /// Last exact qualified progress and independent catch-up result.
    #[must_use]
    pub const fn progress(&self) -> Option<MysqlProgressOutcome> {
        self.progress
    }

    /// Current closed access intent.
    #[must_use]
    pub const fn access_intent(&self) -> Option<MysqlAccessIntent> {
        self.access
    }

    /// Terminal state after close or abort.
    #[must_use]
    pub const fn terminal_state(&self) -> Option<MysqlTerminalState> {
        self.terminal
    }

    /// Retained successful terminal outcome. A terminal state with no outcome
    /// indicates unsettled containment.
    #[must_use]
    pub fn terminal_outcome(&self) -> Option<&MysqlTerminalOutcome> {
        self.terminal_outcome.as_deref()
    }

    /// Returns whether both owned credential buffers were explicitly cleared.
    #[must_use]
    pub fn credentials_cleared(&self) -> bool {
        self.observer.is_cleared() && self.recovery.is_cleared()
    }

    /// Returns whether a dedicated application credential is retained locally.
    #[must_use]
    pub fn application_credential_staged(&self) -> bool {
        self.application_credential
            .as_ref()
            .is_some_and(|credential| !credential.is_cleared())
    }

    /// Exact current native evidence, when a receipt-bound operation completed.
    #[must_use]
    pub const fn native_evidence(&self) -> Option<&MemberNativeEvidence> {
        self.native_evidence.as_ref()
    }

    /// Exact non-owning local target facts after open.
    pub fn local_target(&self) -> Result<MysqlReplicaTarget, MysqlReplicatorError> {
        self.require_open_active()?;
        Ok(self.target_from_authority(self.authority()))
    }

    fn target_from_authority(&self, authority: &MemberMutationAuthority) -> MysqlReplicaTarget {
        let enrollment = authority.local();
        let binding = enrollment.binding();
        MysqlReplicaTarget::new(
            self.identity.clone(),
            MemberPeer::from_enrollment(enrollment),
            binding.process_session().clone(),
            binding.endpoint().clone(),
            binding.storage().clone(),
        )
    }

    /// Current immutable source boundary for a later member join.
    pub fn source_boundary(&self) -> Result<MemberSourceBoundary, MysqlReplicatorError> {
        self.require_current_configuration()?;
        let evidence = self
            .native_evidence
            .clone()
            .ok_or(MysqlReplicatorError::ConfigurationRequired)?;
        MemberSourceBoundary::new(
            MemberPeer::from_enrollment(self.authority().local()),
            evidence,
        )
        .map_err(MysqlReplicatorError::MemberTransition)
    }

    /// Credits open only from an exact identity, member, attempt, and credential receipt.
    pub fn open(
        &mut self,
        operation: MysqlOperationIdentity,
        receipt: MysqlOpenReceipt,
    ) -> Result<MysqlOpenOutcome, MysqlReplicatorError> {
        self.ensure_not_terminal()?;
        if let Some(authority) = self.authority.as_ref() {
            let fingerprint =
                OperationFingerprint::Open(receipt.identity.clone(), receipt.access_generation);
            let result = self
                .retry_outcome(&operation, &fingerprint)
                .and_then(|outcome| {
                    if authority == receipt.authority() {
                        expect_open(outcome)
                    } else {
                        Err(MysqlReplicatorError::OpenConflict)
                    }
                });
            if matches!(result, Err(MysqlReplicatorError::OpenConflict)) {
                self.invalidate_handoff_publication_proof();
            }
            return result;
        }
        self.admit_work()?;
        self.validate_open_receipt(&receipt)?;
        let fingerprint =
            OperationFingerprint::Open(receipt.identity.clone(), receipt.access_generation);
        let admission = self.begin_operation(operation, fingerprint)?;
        if let Admission::Retry(outcome) = admission {
            return expect_open(outcome);
        }
        let outcome = MysqlOpenOutcome {
            identity: receipt.identity.clone(),
            access_generation: receipt.access_generation,
        };
        self.authority = Some(receipt.authority);
        self.owned_authority = self.authority.clone();
        self.access = Some(MysqlAccessIntent::new(
            receipt.access_generation,
            false,
            false,
        ));
        self.complete_last(RecordedOutcome::Open(outcome.clone()));
        Ok(outcome)
    }

    /// Returns the qualified empty-history progress proved by fresh member
    /// preparation after exact open and before any lifecycle mutation.
    pub(crate) fn fresh_open_progress(&self) -> Result<QualifiedProgress, MysqlReplicatorError> {
        self.require_open_active()?;
        let access_closed = self
            .access
            .is_some_and(|intent| !intent.read() && !intent.write());
        if !self.fresh_prepared_owner
            || self.owned_authority.as_ref() != self.authority.as_ref()
            || !access_closed
            || self.epoch.is_some()
            || self.role.is_some()
            || self.configuration.is_some()
            || self.native_evidence.is_some()
            || self.progress.is_some()
        {
            return Err(MysqlReplicatorError::InvalidConfiguration);
        }
        match project_progress(&self.lineage, &GtidSet::empty()) {
            ProgressProjection::Projectable(progress) => Ok(progress),
            ProgressProjection::NonProjectable(_) => {
                unreachable!("empty structured history is always projectable")
            }
        }
    }

    /// Accepts one strictly newer epoch barrier.
    pub fn advance_epoch(
        &mut self,
        operation: MysqlOperationIdentity,
        epoch: MysqlReplicaEpoch,
    ) -> Result<MysqlEpochOutcome, MysqlReplicatorError> {
        self.require_open_active()?;
        self.ensure_proxy_not_published()?;
        self.admit_work()?;
        let fingerprint = OperationFingerprint::Epoch(epoch);
        match self.begin_operation(operation, fingerprint)? {
            Admission::Retry(outcome) => return expect_epoch(outcome),
            Admission::New(_) => {}
        }
        if self.epoch.is_some_and(|current| epoch <= current) {
            self.discard_last();
            return Err(MysqlReplicatorError::StaleEpoch);
        }
        self.epoch = Some(epoch);
        if let Some(handoff) = self.source_handoff.as_mut() {
            if epoch.value() <= handoff.boundary.source_epoch.value() {
                self.discard_last();
                return Err(MysqlReplicatorError::HandoffProofRequired);
            }
            handoff.newer_epoch = Some(epoch);
        }
        self.role_epoch = None;
        self.configuration = None;
        self.progress = None;
        self.invalidate_handoff_publication_proof();
        self.close_access();
        let outcome = MysqlEpochOutcome { epoch };
        self.complete_last(RecordedOutcome::Epoch(outcome));
        Ok(outcome)
    }

    /// Accepts fresh bootstrap/join role intent before configuration supplies
    /// the peer and predecessor inputs required for the exact native effect.
    pub fn intend_formation_role(
        &mut self,
        operation: MysqlOperationIdentity,
        epoch: MysqlReplicaEpoch,
        role: MysqlReplicaRole,
    ) -> Result<MysqlFormationRoleOutcome, MysqlReplicatorError> {
        self.require_open_active()?;
        self.ensure_proxy_not_published()?;
        self.admit_work()?;
        let fingerprint = OperationFingerprint::FormationRole(epoch, role);
        match self.begin_operation(operation, fingerprint)? {
            Admission::Retry(outcome) => return expect_formation_role(outcome),
            Admission::New(_) => {}
        }
        if self.epoch != Some(epoch)
            || self.role_epoch == Some(epoch)
            || self.native_evidence.is_some()
            || self.configuration.is_some()
        {
            self.discard_last();
            return Err(MysqlReplicatorError::EpochBarrierRequired);
        }
        self.role = Some(role);
        self.role_epoch = Some(epoch);
        self.progress = None;
        self.invalidate_handoff_publication_proof();
        self.close_access();
        let outcome = MysqlFormationRoleOutcome { epoch, role };
        self.complete_last(RecordedOutcome::FormationRole(outcome));
        Ok(outcome)
    }

    /// Completes a role transition from one exact coherent local observation.
    pub async fn observe_role(
        &mut self,
        operation: MysqlOperationIdentity,
        epoch: MysqlReplicaEpoch,
        role: MysqlReplicaRole,
    ) -> Result<MysqlRoleOutcome, MysqlReplicatorError> {
        self.apply_role(operation, epoch, role, RoleEffect::Observe, false)
            .await
            .map(|outcome| outcome.expect("strict role observation cannot remain pending"))
    }

    /// Observes a role only when the current native view already reports it.
    ///
    /// A coherent view with another role returns `None` and releases the
    /// operation identity for a later retry. No lifecycle credit or terminal
    /// state is recorded for that pending external transfer.
    pub async fn observe_role_if_ready(
        &mut self,
        operation: MysqlOperationIdentity,
        epoch: MysqlReplicaEpoch,
        role: MysqlReplicaRole,
    ) -> Result<Option<MysqlRoleOutcome>, MysqlReplicatorError> {
        self.apply_role(operation, epoch, role, RoleEffect::Observe, true)
            .await
    }

    /// Completes local demotion only from an exact primary-transfer receipt.
    pub async fn transfer_role(
        &mut self,
        operation: MysqlOperationIdentity,
        epoch: MysqlReplicaEpoch,
        role: MysqlReplicaRole,
        source: MemberSourceBoundary,
        target: MemberPeer,
    ) -> Result<MysqlRoleOutcome, MysqlReplicatorError> {
        self.apply_role(
            operation,
            epoch,
            role,
            RoleEffect::Transfer {
                source: Box::new(source),
                target,
            },
            false,
        )
        .await
        .map(|outcome| outcome.expect("native transfer cannot remain pending"))
    }

    /// Accepts configuration from an exact coherent observation.
    pub async fn observe_configuration(
        &mut self,
        operation: MysqlOperationIdentity,
        configuration: MysqlReplicaConfiguration,
    ) -> Result<MysqlConfigurationOutcome, MysqlReplicatorError> {
        self.apply_configuration(operation, configuration, ConfigurationEffect::Observe)
            .await
    }

    /// Accepts fresh one-member bootstrap only from its exact typed receipt.
    pub async fn bootstrap_configuration(
        &mut self,
        operation: MysqlOperationIdentity,
        configuration: MysqlReplicaConfiguration,
    ) -> Result<MysqlConfigurationOutcome, MysqlReplicatorError> {
        self.apply_configuration(operation, configuration, ConfigurationEffect::Bootstrap)
            .await
    }

    /// Accepts fresh join only from its exact typed receipt.
    pub async fn join_configuration(
        &mut self,
        operation: MysqlOperationIdentity,
        configuration: MysqlReplicaConfiguration,
        source: MemberSourceBoundary,
        peers: Vec<MemberPeer>,
    ) -> Result<MysqlConfigurationOutcome, MysqlReplicatorError> {
        self.apply_configuration(
            operation,
            configuration,
            ConfigurationEffect::Join {
                source: Box::new(source),
                peers,
            },
        )
        .await
    }

    /// Reads exact native evidence and projects current progress independently of catch-up.
    pub async fn read_progress(
        &mut self,
        operation: MysqlOperationIdentity,
    ) -> Result<MysqlProgressOutcome, MysqlReplicatorError> {
        self.read_progress_observation(operation, true).await
    }

    /// Reads a request-bound pre-epoch progress/view observation while allowing
    /// the native role to have changed before the runtime role callback.
    ///
    /// Exact authority, current configuration membership, receipt, deadline,
    /// cancellation, and structured history projection remain mandatory.
    pub async fn observe_transition_progress(
        &mut self,
        operation: MysqlOperationIdentity,
    ) -> Result<MysqlProgressOutcome, MysqlReplicatorError> {
        self.read_progress_observation(operation, false).await
    }

    async fn read_progress_observation(
        &mut self,
        operation: MysqlOperationIdentity,
        require_recorded_native_role: bool,
    ) -> Result<MysqlProgressOutcome, MysqlReplicatorError> {
        self.require_current_configuration()?;
        self.admit_work()?;
        let request = self.current_effect_request(&operation, None)?;
        let fingerprint = OperationFingerprint::Progress {
            require_recorded_native_role,
        };
        let index = match self.begin_operation(operation.clone(), fingerprint)? {
            Admission::Retry(outcome) => return expect_progress(outcome),
            Admission::New(index) => index,
        };
        let receipt = match self
            .effects
            .read_member_view(request.clone(), &self.attempt)
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => return self.uncertain_member(index, error),
        };
        self.validate_after_effect(index)?;
        let validation = self
            .validate_receipt(&receipt, &MemberEffectKind::ViewReadback, &request)
            .and_then(|()| {
                if require_recorded_native_role {
                    self.require_current_evidence(receipt.successor())
                } else {
                    self.require_transition_evidence(receipt.successor())
                }
            });
        if let Err(error) = validation {
            return self.uncertain_rejection(index, error);
        }
        let evidence = receipt.successor().clone();
        let outcome = MysqlProgressOutcome {
            progress: project_progress(&self.lineage, evidence.executed()),
            catch_up: CatchUpCapabilityResult::retained_evidence_unavailable(),
        };
        self.native_evidence = Some(evidence);
        self.progress = Some(outcome);
        self.complete(index, RecordedOutcome::Progress(outcome));
        Ok(outcome)
    }

    /// Freezes the exact structured and scalar source boundary only after a
    /// native-backed, normally completed proxy revocation.
    pub async fn freeze_handoff_boundary(
        &mut self,
        operation: MysqlOperationIdentity,
        target: MysqlReplicaTarget,
        remaining: MysqlReplicaTarget,
    ) -> Result<MysqlHandoffBoundary, MysqlReplicatorError> {
        self.require_current_configuration()?;
        self.admit_work()?;
        if self.role != Some(MysqlReplicaRole::Primary)
            || self
                .access
                .is_none_or(|access| access.read() || access.write())
            || self
                .proxy_containment
                .as_ref()
                .is_none_or(|receipt| !receipt.handoff_eligible())
        {
            return Err(MysqlReplicatorError::HandoffProofRequired);
        }
        let proxy = self
            .proxy
            .as_ref()
            .ok_or(MysqlReplicatorError::HandoffProofRequired)?;
        let closure = self
            .proxy_containment
            .as_ref()
            .filter(|receipt| receipt.handoff_eligible())
            .cloned()
            .ok_or(MysqlReplicatorError::HandoffProofRequired)?;
        let snapshot = proxy.snapshot();
        if snapshot.accepting()
            || snapshot.sessions() != 0
            || snapshot.active_commands() != 0
            || !snapshot.backend_connections().is_empty()
            || !proxy.is_fully_contained()
            || !proxy.account_active()
            || proxy.credential_cleared()
            || proxy.published_generation() != Some(closure.generation())
        {
            return Err(MysqlReplicatorError::HandoffProofRequired);
        }
        let configuration = self
            .configuration
            .as_ref()
            .expect("current configuration was required")
            .clone();
        let source = self.local_target()?;
        if configuration.members.len() != 3
            || source == target
            || source == remaining
            || target == remaining
            || ![&source, &target, &remaining]
                .into_iter()
                .all(|member| configuration.members.contains(member))
        {
            return Err(MysqlReplicatorError::InvalidConfiguration);
        }
        let fingerprint = OperationFingerprint::HandoffFreeze(Box::new((
            target.clone(),
            remaining.clone(),
            closure.clone(),
        )));
        let index = match self.begin_operation(operation.clone(), fingerprint)? {
            Admission::Retry(outcome) => return expect_handoff_boundary(outcome),
            Admission::New(index) => index,
        };
        let request = self.current_effect_request(&operation, None)?;
        let receipt = match self
            .effects
            .read_member_view(request.clone(), &self.attempt)
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => return self.uncertain_member(index, error),
        };
        self.validate_after_effect(index)?;
        if let Err(error) = self
            .validate_receipt(&receipt, &MemberEffectKind::ViewReadback, &request)
            .and_then(|()| self.require_current_evidence(receipt.successor()))
        {
            return self.uncertain_rejection(index, error);
        }
        let evidence = receipt.successor().clone();
        let projected = project_progress(&self.lineage, evidence.executed());
        let progress = match projected {
            ProgressProjection::Projectable(progress) => progress,
            ProgressProjection::NonProjectable(_) => {
                return self.uncertain_rejection(index, MysqlReplicatorError::HandoffProofRequired);
            }
        };
        let boundary = MysqlHandoffBoundary {
            id: MysqlHandoffBoundaryId {
                attempt: self.attempt.attempt().clone(),
                freeze_operation: operation,
                revoked_generation: closure.generation(),
            },
            closure,
            configuration,
            source_epoch: self
                .epoch
                .expect("current configuration establishes source epoch"),
            source_authority: self.authority().clone(),
            source,
            target,
            remaining,
            predecessor_view: evidence.view().id().clone(),
            executed: evidence.executed().clone(),
            progress,
        };
        self.native_evidence = Some(evidence);
        self.progress = Some(MysqlProgressOutcome {
            progress: projected,
            catch_up: CatchUpCapabilityResult::retained_evidence_unavailable(),
        });
        self.source_handoff = Some(Box::new(SourceHandoffState {
            boundary: boundary.clone(),
            newer_epoch: None,
            demoted: false,
            configuration_accepted: false,
        }));
        self.source_handoff_invalidated = false;
        self.complete(
            index,
            RecordedOutcome::HandoffBoundary(Box::new(boundary.clone())),
        );
        Ok(boundary)
    }

    /// Accepts the exact canonical two-survivor successor without changing the
    /// current public three-member configuration.
    pub async fn accept_handoff_successor(
        &mut self,
        operation: MysqlOperationIdentity,
        boundary: MysqlHandoffBoundary,
    ) -> Result<MysqlHandoffOutcome, MysqlReplicatorError> {
        self.require_current_configuration()?;
        self.ensure_proxy_not_published()?;
        self.admit_work()?;
        let request = self.current_effect_request(&operation, None)?;
        let fingerprint = OperationFingerprint::HandoffSuccessor(Box::new(boundary.clone()));
        let index = match self.begin_operation(operation, fingerprint)? {
            Admission::Retry(outcome) => return expect_handoff(outcome),
            Admission::New(index) => index,
        };
        let Some(current_epoch) = self
            .configuration
            .as_ref()
            .filter(|configuration| configuration.members == boundary.configuration.members)
            .map(|configuration| configuration.epoch)
        else {
            self.discard_last();
            return Err(MysqlReplicatorError::HandoffProofRequired);
        };
        if current_epoch.value() <= boundary.configuration.epoch.value()
            || self.epoch != Some(current_epoch)
        {
            self.discard_last();
            return Err(MysqlReplicatorError::HandoffProofRequired);
        }
        let expected_role = if self.identity == boundary.target.identity {
            MysqlReplicaRole::Primary
        } else if self.identity == boundary.remaining.identity {
            MysqlReplicaRole::Secondary
        } else {
            self.discard_last();
            return Err(MysqlReplicatorError::HandoffProofRequired);
        };
        if self.role != Some(expected_role) {
            self.discard_last();
            return Err(MysqlReplicatorError::NativeRoleMismatch);
        }
        let receipt = match self
            .effects
            .read_member_view(request.clone(), &self.attempt)
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => return self.uncertain_member(index, error),
        };
        self.validate_after_effect(index)?;
        if let Err(error) =
            self.validate_receipt(&receipt, &MemberEffectKind::ViewReadback, &request)
        {
            return self.uncertain_rejection(index, error);
        }
        let evidence = receipt.successor();
        let valid_successor = CanonicalViewId::parse(&boundary.predecessor_view)
            .and_then(|predecessor| {
                CanonicalViewId::parse(evidence.view().id())
                    .and_then(|successor| predecessor.require_successor(successor))
            })
            .is_ok();
        let exact_member = |target: &MysqlReplicaTarget, role: MemberRole| -> bool {
            evidence
                .view()
                .member(target.member.member_id())
                .is_some_and(|member| {
                    member.address() == target.member.address()
                        && member.role() == role
                        && member.state() == MemberState::Online
                })
        };
        let projected = project_progress(&self.lineage, evidence.executed());
        let current_progress = match projected {
            ProgressProjection::Projectable(progress)
                if progress.value() >= boundary.progress.value() =>
            {
                progress
            }
            _ => {
                return self.uncertain_rejection(index, MysqlReplicatorError::HandoffProofRequired);
            }
        };
        if !valid_successor
            || evidence.view().members().len() != 2
            || evidence
                .view()
                .member(boundary.source.member.member_id())
                .is_some()
            || !exact_member(&boundary.target, MemberRole::Primary)
            || !exact_member(&boundary.remaining, MemberRole::Secondary)
            || !boundary.executed.is_subset_of(evidence.executed())
        {
            return self.uncertain_rejection(index, MysqlReplicatorError::HandoffProofRequired);
        }
        self.native_evidence = Some(evidence.clone());
        self.progress = Some(MysqlProgressOutcome {
            progress: projected,
            catch_up: CatchUpCapabilityResult::retained_evidence_unavailable(),
        });
        let outcome = MysqlHandoffOutcome::new(
            boundary,
            self.identity.clone(),
            self.authority().clone(),
            self.configuration
                .as_ref()
                .expect("current configuration was required")
                .clone(),
            evidence.clone(),
            current_epoch,
            expected_role,
            current_progress,
        );
        self.complete(index, RecordedOutcome::Handoff(Box::new(outcome.clone())));
        Ok(outcome)
    }

    /// Validates and consumes the combined target, remaining, and exact source
    /// terminal evidence before making target publication eligible.
    pub fn accept_handoff_publication_proof(
        &mut self,
        operation: MysqlOperationIdentity,
        proof: MysqlHandoffPublicationProof,
    ) -> Result<MysqlHandoffPublicationOutcome, MysqlReplicatorError> {
        self.require_current_configuration()?;
        self.ensure_proxy_not_published()?;
        self.admit_work()?;
        if let Some(accepted) = self.handoff_publication_proof.as_ref() {
            let exact_retry = accepted.proof == proof;
            if !exact_retry {
                self.invalidate_handoff_publication_proof();
            }
            return Err(if exact_retry {
                MysqlReplicatorError::HandoffProofDuplicate
            } else {
                MysqlReplicatorError::HandoffProofConflict
            });
        }
        let fingerprint = OperationFingerprint::HandoffPublication(Box::new(proof.clone()));
        let index = match self.begin_operation(operation, fingerprint)? {
            Admission::Retry(_) => return Err(MysqlReplicatorError::HandoffProofDuplicate),
            Admission::New(index) => index,
        };
        let validation = self.validate_handoff_publication_proof(&proof);
        if let Err(error) = validation {
            self.operations[index].status = OperationStatus::Uncertain;
            self.close_access();
            return Err(error);
        }
        let configuration = self
            .configuration
            .as_ref()
            .expect("current configuration was required")
            .clone();
        let epoch = self
            .epoch
            .expect("current configuration establishes current epoch");
        self.handoff_publication_proof = Some(AcceptedHandoffPublicationProof {
            proof: proof.clone(),
            epoch,
            role: self
                .role
                .expect("current configuration establishes current role"),
            configuration: configuration.clone(),
            authority: self.authority().clone(),
            operation_generation: self.last_operation_generation,
        });
        let outcome = MysqlHandoffPublicationOutcome {
            boundary: proof.boundary,
            epoch,
            configuration,
            target: self.identity.clone(),
        };
        self.complete(
            index,
            RecordedOutcome::HandoffPublication(Box::new(outcome.clone())),
        );
        Ok(outcome)
    }

    /// Returns the independently typed catch-up result without inferring from progress.
    pub fn catch_up_result(&self) -> Result<CatchUpCapabilityResult, MysqlReplicatorError> {
        self.require_current_configuration()?;
        Ok(CatchUpCapabilityResult::retained_evidence_unavailable())
    }

    /// Records a newer explicit access intent while keeping publication closed.
    pub fn set_access_intent(
        &mut self,
        operation: MysqlOperationIdentity,
        intent: MysqlAccessIntent,
    ) -> Result<MysqlAccessOutcome, MysqlReplicatorError> {
        self.require_current_configuration()?;
        self.ensure_proxy_not_published()?;
        self.admit_work()?;
        let fingerprint =
            OperationFingerprint::Access(intent, AccessPublicationAuthority::IntentOnly);
        match self.begin_operation(operation, fingerprint)? {
            Admission::Retry(outcome) => return expect_access(outcome),
            Admission::New(_) => {}
        }
        if self
            .access
            .is_some_and(|current| intent.generation <= current.generation)
        {
            self.discard_last();
            return Err(MysqlReplicatorError::StaleAccessGeneration);
        }
        self.access = Some(intent);
        let outcome = MysqlAccessOutcome {
            intent,
            endpoint: None,
            containment: None,
        };
        self.complete_last(RecordedOutcome::Access(outcome.clone()));
        Ok(outcome)
    }

    /// Validates ordered build admission and returns typed unsupported.
    pub fn build_replica(
        &mut self,
        intent: MysqlPeerOperationIntent,
    ) -> Result<MysqlPeerOperationOutcome, MysqlReplicatorError> {
        self.peer_operation(MysqlPeerOperationKind::Build, intent)
    }

    /// Validates ordered removal admission and returns typed unsupported.
    pub fn remove_replica(
        &mut self,
        intent: MysqlPeerOperationIntent,
    ) -> Result<MysqlPeerOperationOutcome, MysqlReplicatorError> {
        self.peer_operation(MysqlPeerOperationKind::Remove, intent)
    }

    /// Settles local cleanup before crossing the terminal close boundary.
    pub fn close(
        &mut self,
        operation: MysqlOperationIdentity,
    ) -> Result<MysqlTerminalOutcome, MysqlReplicatorError> {
        let fingerprint = OperationFingerprint::Close;
        let index = match self.begin_terminal_operation(operation.clone(), fingerprint)? {
            Admission::Retry(outcome) => return expect_terminal(outcome),
            Admission::New(index) => index,
        };
        self.require_open_active()?;
        if self.proxy.is_some() || self.pending_application_account.is_some() {
            self.operations[index].status = OperationStatus::Uncertain;
            return Err(MysqlReplicatorError::AccessRevocationRequired);
        }
        self.attempt
            .admit_cleanup()
            .map_err(map_cleanup_admission)?;
        let operation_receipt = operation.clone();
        let identity = self.identity.clone();
        let source = self.local_target()?;
        let authority = self.authority().clone();
        let boundary = match self.source_handoff.as_ref() {
            Some(handoff)
                if handoff.demoted
                    && handoff.configuration_accepted
                    && handoff.newer_epoch == self.epoch
                    && self.configuration.as_ref().is_some_and(|configuration| {
                        configuration.members == handoff.boundary.configuration.members
                    }) =>
            {
                Some(handoff.boundary.id.clone())
            }
            Some(_) => {
                self.operations[index].status = OperationStatus::Uncertain;
                return Err(MysqlReplicatorError::HandoffProofRequired);
            }
            None => None,
        };
        let containment = match self.effects.contain_member_exact(&authority, &self.attempt) {
            Ok(containment) => containment,
            Err(error) => {
                self.operations[index].status = OperationStatus::Uncertain;
                self.close_access();
                return Err(MysqlReplicatorError::Instance(error));
            }
        };
        let cleanup = containment.cleanup();
        if cleanup != AttemptCleanupState::Completed {
            self.operations[index].status = OperationStatus::Uncertain;
            self.close_access();
            return Err(MysqlReplicatorError::CloseUnsettled);
        }
        if containment.authority() != &authority
            || containment.binding() != authority.local().binding()
            || !containment.is_complete()
        {
            self.operations[index].status = OperationStatus::Uncertain;
            self.close_access();
            return Err(MysqlReplicatorError::CloseUnsettled);
        }
        self.clear_all_credentials();
        self.close_access();
        self.invalidate_handoff_publication_proof();
        self.terminal = Some(MysqlTerminalState::Closed);
        let receipt = MysqlSourceTerminalReceipt::new(
            operation_receipt,
            identity,
            source,
            authority,
            MysqlTerminalState::Closed,
            containment,
            boundary,
        );
        let outcome = MysqlTerminalOutcome {
            state: MysqlTerminalState::Closed,
            cleanup,
            receipt: Some(receipt),
        };
        self.terminal_outcome = Some(Box::new(outcome.clone()));
        self.complete(index, RecordedOutcome::Terminal(Box::new(outcome.clone())));
        Ok(outcome)
    }

    /// Immediately crosses the abort boundary and attempts local containment.
    pub fn abort(
        &mut self,
        operation: MysqlOperationIdentity,
    ) -> Result<MysqlTerminalOutcome, MysqlReplicatorError> {
        self.abort_inner(operation, false)
    }

    /// Synchronously closes any client listener, aborts the owned proxy task
    /// tree, clears secret-bearing obligations, and then completes exact local
    /// member containment.
    ///
    /// This is the terminal fallback for callers whose async callback future
    /// may be dropped while it owns the engine. It starts no detached work and
    /// does not depend on a later `close` callback. It proves synchronous
    /// listener closure and native containment, not synchronous Tokio task
    /// join completion after abortion.
    pub fn abort_with_proxy_sync(
        &mut self,
        operation: MysqlOperationIdentity,
    ) -> Result<MysqlTerminalOutcome, MysqlReplicatorError> {
        self.abort_inner(operation, true)
    }

    fn abort_inner(
        &mut self,
        operation: MysqlOperationIdentity,
        discard_proxy: bool,
    ) -> Result<MysqlTerminalOutcome, MysqlReplicatorError> {
        let fingerprint = OperationFingerprint::Abort;
        let index = match self.begin_terminal_operation(operation.clone(), fingerprint)? {
            Admission::Retry(outcome) => return expect_terminal(outcome),
            Admission::New(index) => index,
        };
        if !discard_proxy && (self.proxy.is_some() || self.pending_application_account.is_some()) {
            self.operations[index].status = OperationStatus::Uncertain;
            return Err(MysqlReplicatorError::AccessRevocationRequired);
        }
        if discard_proxy {
            if let Some(proxy) = self.proxy.take() {
                proxy.seal_now();
                drop(proxy);
            }
            self.proxy_containment = None;
        }
        let identity = self.identity.clone();
        let opened = self.authority.is_some();
        let authority = self
            .authority
            .clone()
            .or_else(|| self.owned_authority.clone());
        self.close_access();
        self.clear_all_credentials();
        self.invalidate_handoff_publication_proof();
        self.terminal = Some(MysqlTerminalState::Aborted);
        let (cleanup, receipt) = if let Some(authority) = authority {
            let source = self.target_from_authority(&authority);
            let containment = match self.effects.contain_member_exact(&authority, &self.attempt) {
                Ok(containment) => containment,
                Err(error) => {
                    self.operations[index].status = OperationStatus::Uncertain;
                    return Err(MysqlReplicatorError::Instance(error));
                }
            };
            let cleanup = containment.cleanup();
            if cleanup != AttemptCleanupState::Completed
                || containment.authority() != &authority
                || containment.binding() != authority.local().binding()
                || !containment.is_complete()
            {
                self.operations[index].status = OperationStatus::Uncertain;
                return Err(MysqlReplicatorError::AbortUnsettled);
            }
            let receipt = opened.then(|| {
                MysqlSourceTerminalReceipt::new(
                    operation,
                    identity,
                    source,
                    authority,
                    MysqlTerminalState::Aborted,
                    containment,
                    None,
                )
            });
            (cleanup, receipt)
        } else {
            let cleanup = self
                .effects
                .contain_member(&self.attempt)
                .map_err(MysqlReplicatorError::Instance)?;
            if cleanup != AttemptCleanupState::Completed {
                self.operations[index].status = OperationStatus::Uncertain;
                return Err(MysqlReplicatorError::AbortUnsettled);
            }
            (cleanup, None)
        };
        let outcome = MysqlTerminalOutcome {
            state: MysqlTerminalState::Aborted,
            cleanup,
            receipt,
        };
        self.pending_application_account = None;
        self.terminal_outcome = Some(Box::new(outcome.clone()));
        self.complete(index, RecordedOutcome::Terminal(Box::new(outcome.clone())));
        Ok(outcome)
    }

    fn validate_open_receipt(
        &self,
        receipt: &MysqlOpenReceipt,
    ) -> Result<(), MysqlReplicatorError> {
        let authority = receipt.authority();
        let binding = authority.local().binding();
        if receipt.identity() != &self.identity
            || authority.attempt() != self.attempt.attempt()
            || binding.member() != self.effects.config().member_index()
            || binding.member_address().as_str()
                != self.effects.config().member().sql_address().to_string()
            || authority.group_name().as_str() != self.lineage.group_uuid().as_str()
            || binding.observer_generation() != self.observer.generation()
            || binding.recovery_generation() != self.recovery.generation()
        {
            return Err(MysqlReplicatorError::IdentityMismatch);
        }
        Ok(())
    }

    fn require_configuration_evidence(
        &self,
        configuration: &MysqlReplicaConfiguration,
        evidence: &MemberNativeEvidence,
    ) -> Result<(), MysqlReplicatorError> {
        if evidence.view().members().len() != configuration.members.len()
            || configuration.members.iter().any(|configured| {
                evidence
                    .view()
                    .member(configured.member.member_id())
                    .is_none_or(|observed| observed.address() != configured.member.address())
            })
        {
            return Err(MysqlReplicatorError::ReceiptMismatch);
        }
        Ok(())
    }

    fn validate_handoff_publication_proof(
        &self,
        proof: &MysqlHandoffPublicationProof,
    ) -> Result<(), MysqlReplicatorError> {
        if !self.handoff_publication_required
            || !proof.boundary.closure.handoff_eligible()
            || proof.boundary.closure.generation() != proof.boundary.id.revoked_generation
            || proof.boundary.id.attempt != *proof.boundary.source_authority.attempt()
            || self.identity != proof.boundary.target.identity
            || self
                .epoch
                .is_none_or(|epoch| epoch.value() <= proof.boundary.source_epoch.value())
            || self.configuration.as_ref().is_none_or(|configuration| {
                configuration.members != proof.boundary.configuration.members
                    || configuration.epoch != self.epoch.expect("epoch checked above")
            })
        {
            return Err(MysqlReplicatorError::HandoffProofRequired);
        }
        let target = proof
            .target
            .as_ref()
            .ok_or(MysqlReplicatorError::HandoffProofRequired)?;
        let remaining = proof
            .remaining
            .as_ref()
            .ok_or(MysqlReplicatorError::HandoffProofRequired)?;
        let source = proof
            .source
            .as_ref()
            .ok_or(MysqlReplicatorError::HandoffProofRequired)?;
        let configuration = self
            .configuration
            .as_ref()
            .expect("current configuration was required");
        let epoch = self.epoch.expect("current configuration was required");
        let current_evidence = self
            .native_evidence
            .as_ref()
            .ok_or(MysqlReplicatorError::HandoffProofRequired)?;
        let current_progress = match self.progress.map(MysqlProgressOutcome::progress) {
            Some(ProgressProjection::Projectable(progress)) => progress,
            _ => return Err(MysqlReplicatorError::HandoffProofRequired),
        };
        let exact_acceptance = |acceptance: &MysqlHandoffOutcome,
                                expected: &MysqlReplicaTarget,
                                role: MysqlReplicaRole|
         -> bool {
            acceptance.boundary == proof.boundary
                && acceptance.identity == expected.identity
                && acceptance.configuration == *configuration
                && acceptance.successor.authority() == &acceptance.authority
                && acceptance.epoch == epoch
                && acceptance.role == role
                && acceptance.progress.value() >= proof.boundary.progress.value()
                && project_progress(&self.lineage, acceptance.successor.executed())
                    == ProgressProjection::Projectable(acceptance.progress)
                && handoff_successor_matches(&proof.boundary, &acceptance.successor)
                && target_matches_authority(expected, &acceptance.authority)
        };
        if !exact_acceptance(target, &proof.boundary.target, MysqlReplicaRole::Primary)
            || !exact_acceptance(
                remaining,
                &proof.boundary.remaining,
                MysqlReplicaRole::Secondary,
            )
            || target.authority != *self.authority()
            || self.role != Some(MysqlReplicaRole::Primary)
            || target.successor != *current_evidence
            || target.progress != current_progress
            || source.identity != proof.boundary.source.identity
            || source.source != proof.boundary.source
            || source.authority != proof.boundary.source_authority
            || source.state != MysqlTerminalState::Closed
            || source.boundary.as_ref() != Some(&proof.boundary.id)
            || source.containment.authority() != &source.authority
            || source.containment.binding() != source.authority.local().binding()
            || !source.containment.is_complete()
        {
            return Err(MysqlReplicatorError::HandoffProofRequired);
        }
        Ok(())
    }

    fn validate_accepted_handoff_publication_proof(
        &self,
        accepted: &AcceptedHandoffPublicationProof,
    ) -> Result<(), MysqlReplicatorError> {
        if self.last_operation_generation != accepted.operation_generation
            || self.epoch != Some(accepted.epoch)
            || self.role != Some(accepted.role)
            || self.configuration.as_ref() != Some(&accepted.configuration)
            || self.authority.as_ref() != Some(&accepted.authority)
            || accepted.role != MysqlReplicaRole::Primary
        {
            return Err(MysqlReplicatorError::HandoffProofRequired);
        }
        self.validate_handoff_publication_proof(&accepted.proof)
    }

    async fn apply_role(
        &mut self,
        operation: MysqlOperationIdentity,
        epoch: MysqlReplicaEpoch,
        role: MysqlReplicaRole,
        effect: RoleEffect,
        pending_on_native_mismatch: bool,
    ) -> Result<Option<MysqlRoleOutcome>, MysqlReplicatorError> {
        self.require_open_active()?;
        self.ensure_proxy_not_published()?;
        self.admit_work()?;
        let request_operation = operation.clone();
        let fingerprint = OperationFingerprint::Role(epoch, role, effect.clone());
        let index = match self.begin_operation(operation, fingerprint)? {
            Admission::Retry(outcome) => return expect_role(outcome).map(Some),
            Admission::New(index) => index,
        };
        if self.epoch != Some(epoch) || self.role_epoch == Some(epoch) {
            self.discard_last();
            return Err(MysqlReplicatorError::EpochBarrierRequired);
        }
        let source = match &effect {
            RoleEffect::Observe => None,
            RoleEffect::Transfer { source, .. } => Some(source.as_ref().clone()),
        };
        if self.source_handoff_invalidated && matches!(&effect, RoleEffect::Transfer { .. }) {
            return self.uncertain_rejection(index, MysqlReplicatorError::HandoffProofRequired);
        }
        if let Some(handoff) = self.source_handoff.as_ref() {
            let exact_transfer = matches!(
                &effect,
                RoleEffect::Transfer { source, target }
                    if role == MysqlReplicaRole::Secondary
                        && handoff.newer_epoch == Some(epoch)
                        && target.member_id() == handoff.boundary.target.member().member_id()
                        && source.evidence().view().id() == &handoff.boundary.predecessor_view
                        && source.evidence().executed() == &handoff.boundary.executed
            );
            if !exact_transfer {
                return self.uncertain_rejection(index, MysqlReplicatorError::HandoffProofRequired);
            }
        }
        let request = self.effect_request(&request_operation, epoch, role, None, source)?;
        let prior_role = self.role;
        let evidence = match effect {
            RoleEffect::Observe => {
                let receipt = match self
                    .effects
                    .read_member_view(request.clone(), &self.attempt)
                    .await
                {
                    Ok(receipt) => receipt,
                    Err(error) => return self.uncertain_member(index, error),
                };
                if let Err(error) =
                    self.validate_receipt(&receipt, &MemberEffectKind::ViewReadback, &request)
                {
                    return self.uncertain_rejection(index, error);
                }
                receipt.successor().clone()
            }
            RoleEffect::Transfer { source, target } => {
                let expected = MemberEffectKind::PrimaryTransfer {
                    target: target.member_id().clone(),
                };
                let input = PrimaryTransferInput::for_request(request.clone(), *source, target)
                    .map_err(MysqlReplicatorError::MemberTransition)?;
                let receipt = match self.effects.transfer_primary(input, &self.attempt).await {
                    Ok(receipt) => receipt,
                    Err(error) => return self.uncertain_member(index, error),
                };
                if let Err(error) = self.validate_receipt(&receipt, &expected, &request) {
                    return self.uncertain_rejection(index, error);
                }
                receipt.successor().clone()
            }
        };
        self.validate_after_effect(index)?;
        if let Err(error) = self.validate_evidence(&evidence) {
            return self.uncertain_rejection(index, error);
        }
        if let Err(error) = self.require_native_role(&evidence, role) {
            if pending_on_native_mismatch
                && matches!(error, MysqlReplicatorError::NativeRoleMismatch)
            {
                self.discard_last();
                return Ok(None);
            }
            return self.uncertain_rejection(index, error);
        }
        self.role = Some(role);
        self.role_epoch = Some(epoch);
        if let Some(handoff) = self.source_handoff.as_mut() {
            handoff.demoted = true;
        }
        if prior_role == Some(MysqlReplicaRole::Secondary) && role == MysqlReplicaRole::Primary {
            self.handoff_publication_required = true;
        }
        self.invalidate_handoff_publication_proof();
        self.configuration = None;
        self.native_evidence = Some(evidence);
        self.progress = None;
        self.close_access();
        let outcome = MysqlRoleOutcome { epoch, role };
        self.complete(index, RecordedOutcome::Role(outcome));
        Ok(Some(outcome))
    }

    async fn apply_configuration(
        &mut self,
        operation: MysqlOperationIdentity,
        configuration: MysqlReplicaConfiguration,
        effect: ConfigurationEffect,
    ) -> Result<MysqlConfigurationOutcome, MysqlReplicatorError> {
        self.require_open_active()?;
        self.ensure_proxy_not_published()?;
        if let Err(primary) = self.admit_work() {
            return self.contain_configuration_failure(None, primary);
        }
        let request_operation = operation.clone();
        let fingerprint =
            OperationFingerprint::Configuration(configuration.clone(), effect.clone());
        let index = match self.begin_operation(operation, fingerprint)? {
            Admission::Retry(outcome) => return expect_configuration(outcome),
            Admission::New(index) => index,
        };
        let Some(epoch) = self.epoch else {
            self.discard_last();
            return Err(MysqlReplicatorError::EpochBarrierRequired);
        };
        if configuration.epoch != epoch || self.role_epoch != Some(epoch) {
            self.discard_last();
            return Err(MysqlReplicatorError::RoleRequired);
        }
        if let Err(error) = self.validate_configuration(&configuration) {
            self.discard_last();
            return Err(error);
        }
        if let Some(handoff) = self.source_handoff.as_ref()
            && (!handoff.demoted
                || handoff.newer_epoch != Some(configuration.epoch)
                || handoff.boundary.configuration.members != configuration.members)
        {
            self.discard_last();
            return Err(MysqlReplicatorError::HandoffProofRequired);
        }
        let role = self
            .role
            .expect("current role checked before configuration");
        let source = match &effect {
            ConfigurationEffect::Join { source, .. } => Some(source.as_ref().clone()),
            ConfigurationEffect::Observe | ConfigurationEffect::Bootstrap => None,
        };
        if matches!(&effect, ConfigurationEffect::Observe) && self.native_evidence.is_none() {
            self.discard_last();
            return Err(MysqlReplicatorError::RoleRequired);
        }
        let request = self.effect_request(
            &request_operation,
            epoch,
            role,
            Some(configuration.member_intent()),
            source,
        )?;
        let evidence = match effect {
            ConfigurationEffect::Observe => {
                let receipt = match self
                    .effects
                    .read_member_view(request.clone(), &self.attempt)
                    .await
                {
                    Ok(receipt) => receipt,
                    Err(error) => {
                        return self.contain_configuration_failure(
                            Some(index),
                            MysqlReplicatorError::MemberTransition(error),
                        );
                    }
                };
                if let Err(error) =
                    self.validate_receipt(&receipt, &MemberEffectKind::ViewReadback, &request)
                {
                    return self.contain_configuration_failure(Some(index), error);
                }
                receipt.successor().clone()
            }
            ConfigurationEffect::Bootstrap => {
                let input = FreshBootstrapInput::for_request(request.clone())
                    .map_err(MysqlReplicatorError::MemberTransition)?;
                let receipt = match self
                    .effects
                    .bootstrap_member(input, &self.recovery, &self.attempt)
                    .await
                {
                    Ok(receipt) => receipt,
                    Err(error) => {
                        return self.contain_configuration_failure(
                            Some(index),
                            MysqlReplicatorError::MemberTransition(error),
                        );
                    }
                };
                if let Err(error) =
                    self.validate_receipt(&receipt, &MemberEffectKind::Bootstrap, &request)
                {
                    return self.contain_configuration_failure(Some(index), error);
                }
                receipt.successor().clone()
            }
            ConfigurationEffect::Join { source, peers } => {
                let input = FreshJoinInput::for_request(request.clone(), *source, peers)
                    .map_err(MysqlReplicatorError::MemberTransition)?;
                let receipt = match self
                    .effects
                    .join_member(input, &self.recovery, &self.attempt)
                    .await
                {
                    Ok(receipt) => receipt,
                    Err(error) => {
                        return self.contain_configuration_failure(
                            Some(index),
                            MysqlReplicatorError::MemberTransition(error),
                        );
                    }
                };
                if let Err(error) =
                    self.validate_receipt(&receipt, &MemberEffectKind::Join, &request)
                {
                    return self.contain_configuration_failure(Some(index), error);
                }
                receipt.successor().clone()
            }
        };
        if let Err(error) = self.validate_after_effect(index) {
            return self.contain_configuration_failure(Some(index), error);
        }
        if let Err(error) = self.validate_evidence(&evidence).and_then(|()| {
            self.require_native_role(
                &evidence,
                self.role
                    .expect("current role checked before configuration"),
            )
            .and_then(|()| self.require_configuration_evidence(&configuration, &evidence))
        }) {
            return self.contain_configuration_failure(Some(index), error);
        }
        self.configuration = Some(configuration.clone());
        if let Some(handoff) = self.source_handoff.as_mut() {
            handoff.configuration_accepted = true;
        }
        self.native_evidence = Some(evidence);
        self.progress = None;
        self.invalidate_handoff_publication_proof();
        self.clear_control_credentials();
        self.close_access();
        let outcome = MysqlConfigurationOutcome { configuration };
        self.complete(index, RecordedOutcome::Configuration(outcome.clone()));
        Ok(outcome)
    }

    fn peer_operation(
        &mut self,
        kind: MysqlPeerOperationKind,
        intent: MysqlPeerOperationIntent,
    ) -> Result<MysqlPeerOperationOutcome, MysqlReplicatorError> {
        self.require_open_active()?;
        self.admit_work()?;
        let operation = intent.operation.clone();
        let fingerprint = OperationFingerprint::Peer(kind, Box::new(intent.clone()));
        let index = match self.begin_operation(operation, fingerprint)? {
            Admission::Retry(outcome) => return expect_peer(outcome),
            Admission::New(index) => index,
        };
        if self.require_current_configuration().is_err() {
            return self.reject_operation(index, OperationRejection::ConfigurationRequired);
        }
        if self.role != Some(MysqlReplicaRole::Primary) {
            return self.reject_operation(index, OperationRejection::PrimaryRequired);
        }
        let configuration = self
            .configuration
            .as_ref()
            .expect("current configuration was required")
            .clone();
        if intent.epoch != self.epoch.expect("current configuration has an epoch")
            || intent.configuration != configuration.id
        {
            return self.reject_operation(index, OperationRejection::ConfigurationRequired);
        }
        let target = intent.authority.target().clone();
        if intent.authority.process_session() != target.process_session()
            || intent.authority.endpoint() != target.endpoint()
            || intent.authority.storage() != target.storage()
        {
            return self.reject_operation(index, OperationRejection::IdentityMismatch);
        }
        let local_member = self.authority().local().member_id();
        if target.identity == self.identity || target.member.member_id() == local_member {
            return self.reject_operation(index, OperationRejection::SelfTarget);
        }
        let configured_target = configuration.members.iter().find(|member| {
            member.identity == target.identity
                || member.member.member_id() == target.member.member_id()
        });
        match kind {
            MysqlPeerOperationKind::Build => {
                if configured_target.is_some_and(|configured| configured != &target) {
                    return self.reject_operation(index, OperationRejection::InvalidConfiguration);
                }
            }
            MysqlPeerOperationKind::Remove => {
                if configured_target != Some(&target) {
                    return self.reject_operation(index, OperationRejection::InvalidConfiguration);
                }
            }
        }
        let outcome = MysqlPeerOperationOutcome::Unsupported {
            kind,
            reason: MysqlPeerUnsupportedReason::NativeExecutionUnavailable,
            target,
        };
        self.complete(index, RecordedOutcome::Peer(outcome.clone()));
        Ok(outcome)
    }

    fn validate_configuration(
        &self,
        configuration: &MysqlReplicaConfiguration,
    ) -> Result<(), MysqlReplicatorError> {
        let authority = self.authority();
        let local_member = authority.local().member_id();
        let local_binding = authority.local().binding();
        let mut local_count = 0;
        for (index, member) in configuration.members.iter().enumerate() {
            if member.identity == self.identity && member.member.member_id() == local_member {
                if member.process_session != *local_binding.process_session()
                    || member.endpoint != *local_binding.endpoint()
                    || member.storage != *local_binding.storage()
                {
                    return Err(MysqlReplicatorError::InvalidConfiguration);
                }
                local_count += 1;
            }
            if configuration.members[index + 1..].iter().any(|other| {
                other.identity == member.identity
                    || other.member.member_id() == member.member.member_id()
                    || other.process_session == member.process_session
                    || other.endpoint == member.endpoint
                    || other.storage == member.storage
            }) {
                return Err(MysqlReplicatorError::InvalidConfiguration);
            }
        }
        if local_count != 1 {
            return Err(MysqlReplicatorError::InvalidConfiguration);
        }
        Ok(())
    }

    fn validate_receipt(
        &self,
        receipt: &MemberEffectReceipt,
        expected: &MemberEffectKind,
        request: &MemberEffectRequest,
    ) -> Result<(), MysqlReplicatorError> {
        if receipt.kind() != expected
            || receipt.request() != Some(request)
            || receipt.authority() != self.authority()
            || receipt.cancellation_state() != AttemptCancellationState::Active
            || receipt.deadline_state() != AttemptDeadlineState::Work
            || receipt.cleanup_state() == AttemptCleanupState::Failed
        {
            return Err(MysqlReplicatorError::ReceiptMismatch);
        }
        self.validate_evidence(receipt.successor())
    }

    fn effect_request(
        &self,
        operation: &MysqlOperationIdentity,
        epoch: MysqlReplicaEpoch,
        role: MysqlReplicaRole,
        configuration: Option<MemberConfigurationIntent>,
        source: Option<MemberSourceBoundary>,
    ) -> Result<MemberEffectRequest, MysqlReplicatorError> {
        MemberEffectRequest::new(
            operation.member_identity(),
            epoch.value(),
            role.native(),
            configuration,
            self.authority().clone(),
            source,
        )
        .map_err(MysqlReplicatorError::MemberTransition)
    }

    fn current_effect_request(
        &self,
        operation: &MysqlOperationIdentity,
        source: Option<MemberSourceBoundary>,
    ) -> Result<MemberEffectRequest, MysqlReplicatorError> {
        let epoch = self
            .epoch
            .ok_or(MysqlReplicatorError::ConfigurationRequired)?;
        let role = self
            .role
            .ok_or(MysqlReplicatorError::ConfigurationRequired)?;
        let configuration = self
            .configuration
            .as_ref()
            .ok_or(MysqlReplicatorError::ConfigurationRequired)?;
        self.effect_request(
            operation,
            epoch,
            role,
            Some(configuration.member_intent()),
            source,
        )
    }

    fn validate_evidence(
        &self,
        evidence: &MemberNativeEvidence,
    ) -> Result<(), MysqlReplicatorError> {
        if evidence.authority() != self.authority() {
            return Err(MysqlReplicatorError::ReceiptMismatch);
        }
        Ok(())
    }

    fn require_native_role(
        &self,
        evidence: &MemberNativeEvidence,
        role: MysqlReplicaRole,
    ) -> Result<(), MysqlReplicatorError> {
        if evidence
            .view()
            .member(self.authority().local().member_id())
            .is_none_or(|member| member.role() != role.native())
        {
            return Err(MysqlReplicatorError::NativeRoleMismatch);
        }
        Ok(())
    }

    fn require_current_evidence(
        &self,
        evidence: &MemberNativeEvidence,
    ) -> Result<(), MysqlReplicatorError> {
        let role = self
            .role
            .ok_or(MysqlReplicatorError::ConfigurationRequired)?;
        let configuration = self
            .configuration
            .as_ref()
            .ok_or(MysqlReplicatorError::ConfigurationRequired)?;
        self.validate_evidence(evidence)
            .and_then(|()| self.require_native_role(evidence, role))
            .and_then(|()| self.require_configuration_evidence(configuration, evidence))
    }

    fn require_transition_evidence(
        &self,
        evidence: &MemberNativeEvidence,
    ) -> Result<(), MysqlReplicatorError> {
        let configuration = self
            .configuration
            .as_ref()
            .ok_or(MysqlReplicatorError::ConfigurationRequired)?;
        self.validate_evidence(evidence)
            .and_then(|()| self.require_configuration_evidence(configuration, evidence))
    }

    fn require_current_configuration(&self) -> Result<(), MysqlReplicatorError> {
        self.require_open_active()?;
        let epoch = self
            .epoch
            .ok_or(MysqlReplicatorError::ConfigurationRequired)?;
        if self.role_epoch != Some(epoch)
            || self
                .configuration
                .as_ref()
                .is_none_or(|configuration| configuration.epoch != epoch)
        {
            return Err(MysqlReplicatorError::ConfigurationRequired);
        }
        Ok(())
    }

    fn authority(&self) -> &MemberMutationAuthority {
        self.authority
            .as_ref()
            .expect("open requirement establishes authority")
    }

    fn require_open_active(&self) -> Result<(), MysqlReplicatorError> {
        self.ensure_not_terminal()?;
        if self.authority.is_none() {
            Err(MysqlReplicatorError::NotOpen)
        } else {
            Ok(())
        }
    }

    fn ensure_not_terminal(&self) -> Result<(), MysqlReplicatorError> {
        if self.terminal.is_some() {
            Err(MysqlReplicatorError::Terminal)
        } else {
            Ok(())
        }
    }

    fn ensure_proxy_not_published(&self) -> Result<(), MysqlReplicatorError> {
        if let Some(proxy) = self.proxy.as_ref() {
            let snapshot = proxy.snapshot();
            if snapshot.accepting() || snapshot.sessions() != 0 || !proxy.is_fully_contained() {
                return Err(MysqlReplicatorError::AccessRevocationRequired);
            }
        }
        Ok(())
    }

    fn admit_work(&self) -> Result<(), MysqlReplicatorError> {
        self.attempt.admit_work().map_err(map_admission)
    }

    fn validate_after_effect(&mut self, index: usize) -> Result<(), MysqlReplicatorError> {
        match self.attempt.admit_work() {
            Ok(()) => Ok(()),
            Err(AttemptAdmissionError::Cancelled) => {
                self.operations[index].status = OperationStatus::Uncertain;
                self.close_access();
                Err(MysqlReplicatorError::LateReceipt {
                    deadline: self.attempt.deadline_state(),
                    cancellation: self.attempt.cancellation_state(),
                })
            }
            Err(AttemptAdmissionError::CleanupReserve) => {
                self.operations[index].status = OperationStatus::Uncertain;
                self.close_access();
                Err(MysqlReplicatorError::LateReceipt {
                    deadline: AttemptDeadlineState::CleanupReserve,
                    cancellation: self.attempt.cancellation_state(),
                })
            }
            Err(AttemptAdmissionError::Expired) => {
                self.operations[index].status = OperationStatus::Uncertain;
                self.close_access();
                Err(MysqlReplicatorError::LateReceipt {
                    deadline: AttemptDeadlineState::Expired,
                    cancellation: self.attempt.cancellation_state(),
                })
            }
        }
    }

    fn begin_operation(
        &mut self,
        operation: MysqlOperationIdentity,
        fingerprint: OperationFingerprint,
    ) -> Result<Admission, MysqlReplicatorError> {
        if let Some(index) = self
            .operations
            .iter()
            .position(|record| record.operation.id == operation.id)
        {
            let record = &self.operations[index];
            if record.operation != operation || record.fingerprint != fingerprint {
                self.invalidate_handoff_publication_proof();
                return Err(MysqlReplicatorError::OperationConflict);
            }
            return match &record.status {
                OperationStatus::Completed(outcome) => Ok(Admission::Retry(outcome.clone())),
                OperationStatus::Pending | OperationStatus::Uncertain => {
                    Err(MysqlReplicatorError::OperationUncertain)
                }
                OperationStatus::Rejected(rejection) => Err(rejection.error()),
            };
        }
        let generation = operation.generation.get();
        if generation <= self.last_operation_generation {
            return Err(MysqlReplicatorError::StaleOperationGeneration);
        }
        self.last_operation_generation = generation;
        self.operations.push(OperationRecord {
            operation,
            fingerprint,
            status: OperationStatus::Pending,
        });
        Ok(Admission::New(self.operations.len() - 1))
    }

    fn begin_terminal_operation(
        &mut self,
        operation: MysqlOperationIdentity,
        fingerprint: OperationFingerprint,
    ) -> Result<Admission, MysqlReplicatorError> {
        let retry = self
            .operations
            .iter()
            .any(|record| record.operation.id == operation.id);
        if !retry && self.terminal.is_some() {
            return Err(MysqlReplicatorError::Terminal);
        }
        self.begin_operation(operation, fingerprint)
    }

    fn retry_outcome(
        &self,
        operation: &MysqlOperationIdentity,
        fingerprint: &OperationFingerprint,
    ) -> Result<RecordedOutcome, MysqlReplicatorError> {
        let record = self
            .operations
            .iter()
            .find(|record| record.operation.id == operation.id)
            .ok_or(MysqlReplicatorError::OpenConflict)?;
        if &record.operation != operation || &record.fingerprint != fingerprint {
            return Err(MysqlReplicatorError::OpenConflict);
        }
        match &record.status {
            OperationStatus::Completed(outcome) => Ok(outcome.clone()),
            OperationStatus::Pending | OperationStatus::Uncertain => {
                Err(MysqlReplicatorError::OperationUncertain)
            }
            OperationStatus::Rejected(rejection) => Err(rejection.error()),
        }
    }

    fn complete(&mut self, index: usize, outcome: RecordedOutcome) {
        self.operations[index].status = OperationStatus::Completed(outcome);
    }

    fn complete_last(&mut self, outcome: RecordedOutcome) {
        let index = self.operations.len() - 1;
        self.complete(index, outcome);
    }

    fn reject_operation<T>(
        &mut self,
        index: usize,
        rejection: OperationRejection,
    ) -> Result<T, MysqlReplicatorError> {
        self.operations[index].status = OperationStatus::Rejected(rejection);
        Err(rejection.error())
    }

    fn discard_last(&mut self) {
        let record = self
            .operations
            .pop()
            .expect("discard follows new operation admission");
        self.last_operation_generation = self
            .operations
            .iter()
            .map(|operation| operation.operation.generation.get())
            .max()
            .unwrap_or(0);
        debug_assert!(matches!(record.status, OperationStatus::Pending));
    }

    fn uncertain_member<T>(
        &mut self,
        index: usize,
        error: crate::service::MemberTransitionError,
    ) -> Result<T, MysqlReplicatorError> {
        self.operations[index].status = OperationStatus::Uncertain;
        self.close_access();
        Err(MysqlReplicatorError::MemberTransition(error))
    }

    fn uncertain_rejection<T>(
        &mut self,
        index: usize,
        error: MysqlReplicatorError,
    ) -> Result<T, MysqlReplicatorError> {
        self.operations[index].status = OperationStatus::Uncertain;
        self.close_access();
        Err(error)
    }

    fn contain_configuration_failure<T>(
        &mut self,
        index: Option<usize>,
        primary: MysqlReplicatorError,
    ) -> Result<T, MysqlReplicatorError> {
        if let Some(index) = index {
            self.operations[index].status = OperationStatus::Uncertain;
        }
        self.close_access();
        self.clear_all_credentials();
        self.invalidate_handoff_publication_proof();
        self.terminal = Some(MysqlTerminalState::Aborted);
        match self.effects.contain_member(&self.attempt) {
            Ok(_) => Err(primary),
            Err(cleanup) => Err(MysqlReplicatorError::ConfigurationCleanup {
                primary: Box::new(primary),
                cleanup: Box::new(cleanup),
            }),
        }
    }

    fn close_access(&mut self) {
        if let Some(proxy) = self.proxy.as_ref() {
            let snapshot = proxy.snapshot();
            if snapshot.accepting() || snapshot.sessions() != 0 || !proxy.is_fully_contained() {
                proxy.seal_now();
            }
        }
        if let Some(access) = self.access.as_mut() {
            access.read = false;
            access.write = false;
        }
    }

    fn invalidate_handoff_publication_proof(&mut self) {
        self.handoff_publication_proof = None;
    }

    fn invalidate_source_handoff(&mut self) {
        if self.source_handoff.take().is_some() {
            self.source_handoff_invalidated = true;
        }
    }

    fn clear_control_credentials(&mut self) {
        self.observer.clear();
        self.recovery.clear();
    }

    fn clear_all_credentials(&mut self) {
        self.clear_control_credentials();
        if let Some(credential) = self.application_credential.as_mut() {
            credential.clear();
        }
    }
}

type FreshProcessResult = (MysqlInstanceManager, Result<(), MysqlReplicatorError>);

struct FreshProcessWorker {
    attempt: AttemptContext,
    receiver: tokio::sync::oneshot::Receiver<FreshProcessResult>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl FreshProcessWorker {
    fn spawn(mut effects: MysqlInstanceManager, attempt: &AttemptContext) -> Self {
        let worker_attempt = attempt.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let thread = std::thread::spawn(move || {
            let result = prepare_fresh_process(&mut effects, &worker_attempt);
            let _ = sender.send((effects, result));
        });
        Self {
            attempt: attempt.clone(),
            receiver,
            thread: Some(thread),
        }
    }

    async fn finish(mut self) -> Result<FreshProcessResult, MysqlReplicatorError> {
        let received = (&mut self.receiver).await;
        let joined = self
            .thread
            .take()
            .expect("fresh process worker thread remained owned")
            .join();
        match (received, joined) {
            (Ok(result), Ok(())) => Ok(result),
            _ => Err(fresh_worker_failure()),
        }
    }
}

impl Drop for FreshProcessWorker {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            self.attempt.cancel();
            let _ = thread.join();
        }
    }
}

fn prepare_fresh_process(
    effects: &mut MysqlInstanceManager,
    attempt: &AttemptContext,
) -> Result<(), MysqlReplicatorError> {
    if let Err(error) = effects.initialize_with_attempt(attempt) {
        return Err(contain_fresh_process_failure(
            effects,
            attempt,
            MysqlReplicatorError::Instance(error),
        ));
    }
    if let Err(error) = attempt.admit_work() {
        return Err(contain_fresh_process_failure(
            effects,
            attempt,
            map_admission(error),
        ));
    }
    if let Err(error) = effects.start_with_attempt(attempt) {
        return Err(contain_fresh_process_failure(
            effects,
            attempt,
            MysqlReplicatorError::Instance(error),
        ));
    }
    if let Err(error) = attempt.admit_work() {
        return Err(contain_fresh_process_failure(
            effects,
            attempt,
            map_admission(error),
        ));
    }
    Ok(())
}

fn contain_fresh_process_failure(
    effects: &mut MysqlInstanceManager,
    attempt: &AttemptContext,
    prior: MysqlReplicatorError,
) -> MysqlReplicatorError {
    match effects.contain_member(attempt) {
        Ok(_) => prior,
        Err(cleanup) => MysqlReplicatorError::FreshPreparationCleanup {
            prior: Box::new(prior),
            cleanup: Box::new(cleanup),
        },
    }
}

fn fresh_worker_failure() -> MysqlReplicatorError {
    MysqlReplicatorError::Instance(crate::service::MysqlInstanceError::Ownership(
        crate::service::OwnershipError::InspectionFailed,
    ))
}

impl MysqlReplicator<MysqlInstanceManager> {
    /// Initializes, starts, enrolls, and transfers one exact fresh member owner
    /// into an unopened replicator. No peer process authority is accepted.
    pub async fn prepare_fresh_member(
        input: MysqlFreshMemberInput,
    ) -> Result<(Self, MysqlOpenReceipt), MysqlReplicatorError> {
        let MysqlFreshMemberInput {
            config,
            identity,
            lineage,
            credentials:
                MysqlFreshMemberCredentials {
                    mut observer,
                    mut recovery,
                    mut application,
                },
            attempt,
            access_generation,
        } = input;
        let invalid_configuration = config.topology().is_none();
        let invalid_credentials = observer.attempt() != attempt.attempt()
            || recovery.attempt() != attempt.attempt()
            || application.attempt() != attempt.attempt()
            || observer.role() != ControlCredentialRole::Observer
            || recovery.role() != ControlCredentialRole::Recovery;
        if invalid_configuration || invalid_credentials {
            observer.clear();
            recovery.clear();
            application.clear();
            return Err(if invalid_configuration {
                MysqlReplicatorError::InvalidMemberConfiguration
            } else {
                MysqlReplicatorError::IdentityMismatch
            });
        }
        if let Err(error) = attempt.admit_work() {
            observer.clear();
            recovery.clear();
            application.clear();
            return Err(map_admission(error));
        }
        let worker = FreshProcessWorker::spawn(MysqlInstanceManager::new(config), &attempt);
        let (mut effects, process_result) = worker.finish().await?;
        if let Err(error) = process_result {
            observer.clear();
            recovery.clear();
            application.clear();
            return Err(error);
        }
        let authority = match effects
            .enroll_local_control_authority(&attempt, &observer, &recovery)
            .await
        {
            Ok(authority) => authority,
            Err(error) => {
                observer.clear();
                recovery.clear();
                application.clear();
                return contain_fresh_failure(
                    &mut effects,
                    &attempt,
                    MysqlReplicatorError::MemberTransition(error),
                );
            }
        };
        let receipt = MysqlOpenReceipt::new(identity.clone(), authority.clone(), access_generation);
        let mut engine = Self::new(effects, identity, lineage, observer, recovery, attempt)
            .expect("fresh-member inputs were validated before process preparation");
        engine.owned_authority = Some(authority);
        engine.application_credential = Some(application);
        engine.fresh_prepared_owner = true;
        Ok((engine, receipt))
    }
}

impl<E> MysqlReplicator<E>
where
    E: MysqlMemberEffectBoundary + MysqlProxyControlBoundary,
{
    /// Creates the dedicated account, warms its native authentication cache,
    /// and binds an unpublished loopback gateway.
    pub async fn prepare_client_proxy(
        &mut self,
        listen_address: SocketAddr,
        credential: ApplicationCredential,
    ) -> Result<SocketAddr, MysqlReplicatorError> {
        if self.application_credential.is_some() {
            return Err(MysqlReplicatorError::ApplicationCredentialRequired);
        }
        self.application_credential = Some(credential);
        let result = self.prepare_client_proxy_from_staged(listen_address).await;
        if result.is_err()
            && self.pending_application_account.is_none()
            && let Some(mut credential) = self.application_credential.take()
        {
            credential.clear();
        }
        result
    }

    async fn prepare_client_proxy_from_staged(
        &mut self,
        listen_address: SocketAddr,
    ) -> Result<SocketAddr, MysqlReplicatorError> {
        self.require_current_configuration()?;
        self.ensure_proxy_not_published()?;
        self.admit_work()?;
        if self.proxy.is_some() {
            return Err(MysqlReplicatorError::AccessRevocationRequired);
        }
        let authority = self.authority().clone();
        let proxy = {
            let effects = &mut self.effects;
            let credential = &mut self.application_credential;
            let account_obligation = &mut self.pending_application_account;
            MysqlClientProxy::prepare_staged(
                effects,
                &authority,
                listen_address,
                credential,
                account_obligation,
                std::sync::Arc::clone(&self.proxy_access_guard),
                &self.attempt,
            )
            .await
            .map_err(MysqlReplicatorError::Proxy)?
        };
        let address = proxy.address();
        self.proxy = Some(proxy);
        self.proxy_containment = None;
        Ok(address)
    }

    /// Consumes the dedicated credential transferred during fresh-member
    /// preparation and binds an unpublished loopback gateway.
    pub async fn prepare_staged_client_proxy(
        &mut self,
        listen_address: SocketAddr,
    ) -> Result<SocketAddr, MysqlReplicatorError> {
        if self.application_credential.is_none() {
            return Err(MysqlReplicatorError::ApplicationCredentialRequired);
        }
        let result = self.prepare_client_proxy_from_staged(listen_address).await;
        if result.is_err()
            && self.pending_application_account.is_none()
            && let Some(mut credential) = self.application_credential.take()
        {
            credential.clear();
        }
        result
    }

    /// Binds a runtime access guard before a proxy can be prepared or published.
    pub fn bind_proxy_access_guard(
        &mut self,
        access_guard: std::sync::Arc<dyn MysqlProxyAccessGuard>,
    ) -> Result<(), MysqlReplicatorError> {
        if self.proxy.is_some() {
            return Err(MysqlReplicatorError::Proxy(
                crate::service::MysqlProxyError::InvalidState,
            ));
        }
        self.proxy_access_guard = access_guard;
        Ok(())
    }

    /// Explicitly publishes the prepared endpoint for one newer all-or-nothing
    /// access generation.
    pub async fn publish_access(
        &mut self,
        operation: MysqlOperationIdentity,
        intent: MysqlAccessIntent,
    ) -> Result<MysqlAccessOutcome, MysqlReplicatorError> {
        self.publish_access_with_authority(
            operation,
            intent,
            AccessPublicationAuthority::HandoffProof,
        )
        .await
    }

    /// Publishes one primary generation from exact runtime-owned lifecycle
    /// authority plus fresh local native evidence.
    ///
    /// Unlike [`Self::publish_access`], this path does not consume the
    /// repository fixture's combined cross-member handoff proof. It is intended
    /// for a public runtime whose durable operation ordering and access grant
    /// are external to this restart-stateless engine.
    pub async fn publish_access_with_runtime_authority(
        &mut self,
        operation: MysqlOperationIdentity,
        intent: MysqlAccessIntent,
        authority: MysqlRuntimeAccessAuthority,
    ) -> Result<MysqlAccessOutcome, MysqlReplicatorError> {
        self.publish_access_with_authority(
            operation,
            intent,
            AccessPublicationAuthority::Runtime(authority),
        )
        .await
    }

    async fn publish_access_with_authority(
        &mut self,
        operation: MysqlOperationIdentity,
        intent: MysqlAccessIntent,
        publication: AccessPublicationAuthority,
    ) -> Result<MysqlAccessOutcome, MysqlReplicatorError> {
        self.require_current_configuration()?;
        self.admit_work()?;
        if !intent.read || !intent.write {
            return Err(MysqlReplicatorError::Proxy(
                crate::service::MysqlProxyError::BothPermissionsRequired,
            ));
        }
        if let AccessPublicationAuthority::Runtime(authority) = &publication {
            let configuration = self
                .configuration
                .as_ref()
                .expect("current configuration was required");
            if authority.epoch != self.epoch.expect("current configuration has an epoch")
                || authority.role != MysqlReplicaRole::Primary
                || self.role != Some(authority.role)
                || authority.configuration != configuration.id
            {
                return Err(MysqlReplicatorError::HandoffProofRequired);
            }
        }
        let fingerprint = OperationFingerprint::Access(intent, publication.clone());
        if self
            .operations
            .iter()
            .any(|record| record.operation.id == operation.id)
        {
            return match self.begin_operation(operation, fingerprint)? {
                Admission::Retry(outcome) => expect_access(outcome),
                Admission::New(_) => unreachable!("known operation identity cannot be new"),
            };
        }
        if self.handoff_publication_required
            && matches!(publication, AccessPublicationAuthority::HandoffProof)
        {
            let valid = self
                .handoff_publication_proof
                .as_ref()
                .ok_or(MysqlReplicatorError::HandoffProofRequired)
                .and_then(|accepted| self.validate_accepted_handoff_publication_proof(accepted));
            if let Err(error) = valid {
                self.invalidate_handoff_publication_proof();
                return Err(error);
            }
        }
        let index = match self.begin_operation(operation.clone(), fingerprint)? {
            Admission::Retry(outcome) => return expect_access(outcome),
            Admission::New(index) => index,
        };
        self.invalidate_source_handoff();
        if self
            .access
            .is_some_and(|current| intent.generation <= current.generation)
        {
            self.discard_last();
            return Err(MysqlReplicatorError::StaleAccessGeneration);
        }
        if self.handoff_publication_required
            && matches!(publication, AccessPublicationAuthority::HandoffProof)
        {
            let request = self.current_effect_request(&operation, None)?;
            let receipt = match self
                .effects
                .read_member_view(request.clone(), &self.attempt)
                .await
            {
                Ok(receipt) => receipt,
                Err(error) => return self.uncertain_member(index, error),
            };
            self.validate_after_effect(index)?;
            let accepted_target = self
                .handoff_publication_proof
                .as_ref()
                .and_then(|accepted| accepted.proof.target.as_ref())
                .ok_or(MysqlReplicatorError::HandoffProofRequired)?;
            if let Err(error) =
                self.validate_receipt(&receipt, &MemberEffectKind::ViewReadback, &request)
            {
                return self.uncertain_rejection(index, error);
            }
            if receipt.successor() != &accepted_target.successor {
                return self.uncertain_rejection(index, MysqlReplicatorError::HandoffProofRequired);
            }
            self.native_evidence = Some(receipt.successor().clone());
            self.progress = Some(MysqlProgressOutcome {
                progress: project_progress(&self.lineage, receipt.successor().executed()),
                catch_up: CatchUpCapabilityResult::retained_evidence_unavailable(),
            });
        } else if matches!(publication, AccessPublicationAuthority::Runtime(_)) {
            let request = self.current_effect_request(&operation, None)?;
            let receipt = match self
                .effects
                .read_member_view(request.clone(), &self.attempt)
                .await
            {
                Ok(receipt) => receipt,
                Err(error) => return self.uncertain_member(index, error),
            };
            self.validate_after_effect(index)?;
            if let Err(error) =
                self.validate_receipt(&receipt, &MemberEffectKind::ViewReadback, &request)
            {
                return self.uncertain_rejection(index, error);
            }
            if let Err(error) = self.require_current_evidence(receipt.successor()) {
                return self.uncertain_rejection(index, error);
            }
            let progress = project_progress(&self.lineage, receipt.successor().executed());
            if !matches!(progress, ProgressProjection::Projectable(_)) {
                return self.uncertain_rejection(index, MysqlReplicatorError::HandoffProofRequired);
            }
            self.native_evidence = Some(receipt.successor().clone());
            self.progress = Some(MysqlProgressOutcome {
                progress,
                catch_up: CatchUpCapabilityResult::retained_evidence_unavailable(),
            });
        }
        let proxy = self.proxy.as_mut().ok_or(MysqlReplicatorError::Proxy(
            crate::service::MysqlProxyError::InvalidState,
        ))?;
        let endpoint =
            match proxy.publish(intent.generation, intent.read, intent.write, &self.attempt) {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    self.operations[index].status = OperationStatus::Uncertain;
                    return Err(MysqlReplicatorError::Proxy(error));
                }
            };
        self.access = Some(intent);
        self.proxy_containment = None;
        let outcome = MysqlAccessOutcome {
            intent,
            endpoint: Some(endpoint),
            containment: None,
        };
        self.complete(index, RecordedOutcome::Access(outcome.clone()));
        Ok(outcome)
    }

    /// Revokes the current generation and returns only after listener,
    /// command, proxy-session, and exact native-session containment.
    pub async fn revoke_access(
        &mut self,
        operation: MysqlOperationIdentity,
        intent: MysqlAccessIntent,
    ) -> Result<MysqlAccessOutcome, MysqlReplicatorError> {
        self.require_open_active()?;
        let fingerprint =
            OperationFingerprint::Access(intent, AccessPublicationAuthority::HandoffProof);
        let index = match self.begin_operation(operation, fingerprint)? {
            Admission::Retry(outcome) => return expect_access(outcome),
            Admission::New(index) => index,
        };
        let current = self.access.ok_or(MysqlReplicatorError::NotOpen)?;
        if intent.generation <= current.generation {
            self.discard_last();
            return Err(MysqlReplicatorError::StaleAccessGeneration);
        }
        if intent.read && intent.write {
            self.discard_last();
            return Err(MysqlReplicatorError::AccessRevocationRequired);
        }
        let authority = self.authority().clone();
        let proxy = self.proxy.as_mut().ok_or(MysqlReplicatorError::Proxy(
            crate::service::MysqlProxyError::InvalidState,
        ))?;
        let containment = match if proxy.published_generation().is_some() {
            proxy
                .revoke(
                    &mut self.effects,
                    &authority,
                    current.generation,
                    &self.attempt,
                )
                .await
        } else {
            proxy.contain_prepared(intent.generation, &self.attempt)
        } {
            Ok(containment) => containment,
            Err(error) => {
                self.operations[index].status = OperationStatus::Uncertain;
                self.close_access();
                return Err(MysqlReplicatorError::Proxy(error));
            }
        };
        self.access = Some(intent);
        self.proxy_containment = Some(containment.clone());
        let outcome = MysqlAccessOutcome {
            intent,
            endpoint: None,
            containment: Some(containment),
        };
        self.complete(index, RecordedOutcome::Access(outcome.clone()));
        Ok(outcome)
    }

    /// Removes one fully contained proxy/account so a later clean generation
    /// can be prepared on the same member.
    pub async fn retire_contained_client_proxy(&mut self) -> Result<(), MysqlReplicatorError> {
        self.require_current_configuration()?;
        let authority = self.authority().clone();
        let proxy = self.proxy.as_mut().ok_or(MysqlReplicatorError::Proxy(
            crate::service::MysqlProxyError::InvalidState,
        ))?;
        if !proxy.is_fully_contained()
            || self
                .proxy_containment
                .as_ref()
                .is_none_or(|receipt| !receipt.cleanup_complete())
        {
            return Err(MysqlReplicatorError::HandoffProofRequired);
        }
        if let Err(error) = proxy
            .cleanup_account(&mut self.effects, &authority, &self.attempt)
            .await
        {
            return Err(MysqlReplicatorError::Proxy(error));
        }
        self.proxy.take();
        self.proxy_containment = None;
        self.close_access();
        Ok(())
    }

    /// Rearms one fully contained account for a later clean generation.
    pub async fn rearm_contained_client_proxy(
        &mut self,
        listen_address: SocketAddr,
    ) -> Result<SocketAddr, MysqlReplicatorError> {
        self.require_current_configuration()?;
        let proxy = self.proxy.as_mut().ok_or(MysqlReplicatorError::Proxy(
            crate::service::MysqlProxyError::InvalidState,
        ))?;
        let address = proxy
            .rearm(listen_address, &self.attempt)
            .await
            .map_err(MysqlReplicatorError::Proxy)?;
        self.proxy_containment = None;
        self.invalidate_source_handoff();
        self.close_access();
        Ok(address)
    }

    /// Last completed proxy containment receipt.
    #[must_use]
    pub const fn proxy_containment(&self) -> Option<&MysqlProxyContainmentReceipt> {
        self.proxy_containment.as_ref()
    }

    /// Current proxy registry snapshot.
    #[must_use]
    pub fn proxy_snapshot(&self) -> Option<crate::service::MysqlProxySnapshot> {
        self.proxy.as_ref().map(MysqlClientProxy::snapshot)
    }

    /// Removes the account and contains the local member before terminal close.
    pub async fn close_with_proxy(
        &mut self,
        operation: MysqlOperationIdentity,
    ) -> Result<MysqlTerminalOutcome, MysqlReplicatorError> {
        self.terminal_proxy_cleanup().await?;
        self.close(operation)
    }

    /// Immediately seals the endpoint, removes the account where possible,
    /// and contains the local member before returning terminal abort.
    pub async fn abort_with_proxy(
        &mut self,
        operation: MysqlOperationIdentity,
    ) -> Result<MysqlTerminalOutcome, MysqlReplicatorError> {
        self.terminal_proxy_cleanup().await?;
        self.abort(operation)
    }

    async fn terminal_proxy_cleanup(&mut self) -> Result<(), MysqlReplicatorError> {
        let authority = self
            .authority
            .clone()
            .or_else(|| self.owned_authority.clone())
            .ok_or(MysqlReplicatorError::NotOpen)?;
        if let Some(receipt) = self.pending_application_account.clone() {
            let cleanup = self
                .effects
                .remove_application_account(&authority, &receipt, &self.attempt)
                .await;
            match cleanup {
                Ok(()) => {
                    self.pending_application_account = None;
                    if let Some(mut credential) = self.application_credential.take() {
                        credential.clear();
                    }
                }
                Err(proxy_error) => {
                    self.close_access();
                    self.clear_all_credentials();
                    let containment = self.effects.contain_member(&self.attempt);
                    self.invalidate_handoff_publication_proof();
                    self.terminal = Some(MysqlTerminalState::Aborted);
                    return match containment {
                        Ok(_) => Err(MysqlReplicatorError::Proxy(proxy_error)),
                        Err(instance) => Err(MysqlReplicatorError::ProxyCleanup {
                            proxy: Box::new(proxy_error),
                            instance: Box::new(instance),
                        }),
                    };
                }
            }
        }
        let Some(proxy) = self.proxy.as_mut() else {
            return Ok(());
        };
        let revocation = if proxy.is_fully_contained() {
            Ok(self.proxy_containment.clone())
        } else {
            let generation = proxy
                .published_generation()
                .ok_or(MysqlReplicatorError::NotOpen)?;
            proxy
                .revoke(&mut self.effects, &authority, generation, &self.attempt)
                .await
                .map(Some)
        };
        let account = proxy
            .cleanup_account(&mut self.effects, &authority, &self.attempt)
            .await;
        let proxy_result = match (revocation, account) {
            (Ok(containment), Ok(())) => {
                if let Some(containment) = containment {
                    self.proxy_containment = Some(containment);
                }
                Ok(())
            }
            (Err(primary), Ok(())) => Err(primary),
            (Ok(_), Err(cleanup)) => Err(cleanup),
            (Err(primary), Err(cleanup)) => Err(crate::service::MysqlProxyError::Cleanup {
                primary: Box::new(primary),
                cleanup: Box::new(cleanup),
            }),
        };
        if let Err(proxy_error) = proxy_result {
            self.close_access();
            self.clear_all_credentials();
            let containment = self.effects.contain_member(&self.attempt);
            self.invalidate_handoff_publication_proof();
            self.terminal = Some(MysqlTerminalState::Aborted);
            return match containment {
                Ok(_) => Err(MysqlReplicatorError::Proxy(proxy_error)),
                Err(instance) => Err(MysqlReplicatorError::ProxyCleanup {
                    proxy: Box::new(proxy_error),
                    instance: Box::new(instance),
                }),
            };
        }
        self.proxy.take();
        self.close_access();
        Ok(())
    }
}

fn map_admission(error: AttemptAdmissionError) -> MysqlReplicatorError {
    match error {
        AttemptAdmissionError::Cancelled => MysqlReplicatorError::Cancelled,
        AttemptAdmissionError::CleanupReserve => {
            MysqlReplicatorError::Deadline(AttemptDeadlineState::CleanupReserve)
        }
        AttemptAdmissionError::Expired => {
            MysqlReplicatorError::Deadline(AttemptDeadlineState::Expired)
        }
    }
}

fn target_matches_authority(
    target: &MysqlReplicaTarget,
    authority: &MemberMutationAuthority,
) -> bool {
    let binding = authority.local().binding();
    target.member.member_id() == authority.local().member_id()
        && target.member.address() == binding.member_address()
        && target.process_session == *binding.process_session()
        && target.endpoint == *binding.endpoint()
        && target.storage == *binding.storage()
}

fn handoff_successor_matches(
    boundary: &MysqlHandoffBoundary,
    evidence: &MemberNativeEvidence,
) -> bool {
    let exact_member = |target: &MysqlReplicaTarget, role: MemberRole| {
        evidence
            .view()
            .member(target.member.member_id())
            .is_some_and(|member| {
                member.address() == target.member.address()
                    && member.role() == role
                    && member.state() == MemberState::Online
            })
    };
    CanonicalViewId::parse(&boundary.predecessor_view)
        .and_then(|predecessor| {
            CanonicalViewId::parse(evidence.view().id())
                .and_then(|successor| predecessor.require_successor(successor))
        })
        .is_ok()
        && evidence.view().members().len() == 2
        && evidence
            .view()
            .member(boundary.source.member.member_id())
            .is_none()
        && exact_member(&boundary.target, MemberRole::Primary)
        && exact_member(&boundary.remaining, MemberRole::Secondary)
        && boundary.executed.is_subset_of(evidence.executed())
}

fn expect_open(outcome: RecordedOutcome) -> Result<MysqlOpenOutcome, MysqlReplicatorError> {
    if let RecordedOutcome::Open(outcome) = outcome {
        Ok(outcome)
    } else {
        Err(MysqlReplicatorError::OperationConflict)
    }
}

fn expect_epoch(outcome: RecordedOutcome) -> Result<MysqlEpochOutcome, MysqlReplicatorError> {
    if let RecordedOutcome::Epoch(outcome) = outcome {
        Ok(outcome)
    } else {
        Err(MysqlReplicatorError::OperationConflict)
    }
}

fn expect_formation_role(
    outcome: RecordedOutcome,
) -> Result<MysqlFormationRoleOutcome, MysqlReplicatorError> {
    if let RecordedOutcome::FormationRole(outcome) = outcome {
        Ok(outcome)
    } else {
        Err(MysqlReplicatorError::OperationConflict)
    }
}

fn contain_fresh_failure(
    effects: &mut MysqlInstanceManager,
    attempt: &AttemptContext,
    prior: MysqlReplicatorError,
) -> Result<(MysqlReplicator<MysqlInstanceManager>, MysqlOpenReceipt), MysqlReplicatorError> {
    match effects.contain_member(attempt) {
        Ok(_) => Err(prior),
        Err(cleanup) => Err(MysqlReplicatorError::FreshPreparationCleanup {
            prior: Box::new(prior),
            cleanup: Box::new(cleanup),
        }),
    }
}

fn expect_role(outcome: RecordedOutcome) -> Result<MysqlRoleOutcome, MysqlReplicatorError> {
    if let RecordedOutcome::Role(outcome) = outcome {
        Ok(outcome)
    } else {
        Err(MysqlReplicatorError::OperationConflict)
    }
}

fn expect_configuration(
    outcome: RecordedOutcome,
) -> Result<MysqlConfigurationOutcome, MysqlReplicatorError> {
    if let RecordedOutcome::Configuration(outcome) = outcome {
        Ok(outcome)
    } else {
        Err(MysqlReplicatorError::OperationConflict)
    }
}

fn expect_progress(outcome: RecordedOutcome) -> Result<MysqlProgressOutcome, MysqlReplicatorError> {
    if let RecordedOutcome::Progress(outcome) = outcome {
        Ok(outcome)
    } else {
        Err(MysqlReplicatorError::OperationConflict)
    }
}

fn expect_handoff_boundary(
    outcome: RecordedOutcome,
) -> Result<MysqlHandoffBoundary, MysqlReplicatorError> {
    match outcome {
        RecordedOutcome::HandoffBoundary(outcome) => Ok(*outcome),
        _ => Err(MysqlReplicatorError::OperationConflict),
    }
}

fn expect_handoff(outcome: RecordedOutcome) -> Result<MysqlHandoffOutcome, MysqlReplicatorError> {
    match outcome {
        RecordedOutcome::Handoff(outcome) => Ok(*outcome),
        _ => Err(MysqlReplicatorError::OperationConflict),
    }
}

fn expect_access(outcome: RecordedOutcome) -> Result<MysqlAccessOutcome, MysqlReplicatorError> {
    if let RecordedOutcome::Access(outcome) = outcome {
        Ok(outcome)
    } else {
        Err(MysqlReplicatorError::OperationConflict)
    }
}

fn expect_peer(
    outcome: RecordedOutcome,
) -> Result<MysqlPeerOperationOutcome, MysqlReplicatorError> {
    if let RecordedOutcome::Peer(outcome) = outcome {
        Ok(outcome)
    } else {
        Err(MysqlReplicatorError::OperationConflict)
    }
}

fn expect_terminal(outcome: RecordedOutcome) -> Result<MysqlTerminalOutcome, MysqlReplicatorError> {
    match outcome {
        RecordedOutcome::Terminal(outcome) => Ok(*outcome),
        _ => Err(MysqlReplicatorError::OperationConflict),
    }
}
