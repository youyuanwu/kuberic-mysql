//! One-member lifecycle ordering for a future public runtime adapter.
//!
//! The engine owns one injected member-effect boundary. Production uses
//! [`MysqlInstanceManager`]; deterministic contracts may inject scripted
//! effects. It stores no peer process owner and grants no client endpoint.

use std::num::NonZeroU64;

use crate::core::{
    AuthorityGeneration, CatchUpCapabilityResult, ConfigurationId, ConfiguredGtidLineage,
    MemberRole, ProgressProjection, ReplicaId, ReplicaIncarnation, project_progress,
};
use crate::service::{
    AttemptAdmissionError, AttemptCancellationState, AttemptCleanupState, AttemptContext,
    AttemptDeadlineState, ControlCredential, ControlCredentialRole, FreshBootstrapInput,
    FreshJoinInput, MemberEffectKind, MemberEffectReceipt, MemberMutationAuthority,
    MemberNativeEvidence, MemberPeer, MemberSourceBoundary, MysqlInstanceManager,
    MysqlMemberEffectBoundary, MysqlReplicatorError, PrimaryTransferInput,
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
}

impl MysqlReplicaTarget {
    /// Creates one configured target without transferring process ownership.
    #[must_use]
    pub const fn new(identity: MysqlReplicaIdentity, member: MemberPeer) -> Self {
        Self { identity, member }
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

/// Closed Phase 3 access state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MysqlAccessOutcome {
    intent: MysqlAccessIntent,
}

impl MysqlAccessOutcome {
    /// Recorded access intent.
    #[must_use]
    pub const fn intent(self) -> MysqlAccessIntent {
        self.intent
    }

    /// Phase 3 never publishes an application endpoint.
    #[must_use]
    pub const fn is_open(self) -> bool {
        false
    }
}

/// Independently evaluated progress and catch-up results.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MysqlProgressOutcome {
    progress: ProgressProjection,
    catch_up: CatchUpCapabilityResult,
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MysqlTerminalOutcome {
    state: MysqlTerminalState,
    cleanup: AttemptCleanupState,
}

impl MysqlTerminalOutcome {
    /// Terminal state.
    #[must_use]
    pub const fn state(self) -> MysqlTerminalState {
        self.state
    }

    /// Local cleanup evidence.
    #[must_use]
    pub const fn cleanup(self) -> AttemptCleanupState {
        self.cleanup
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
enum OperationFingerprint {
    Open(MysqlReplicaIdentity, NonZeroU64),
    Epoch(MysqlReplicaEpoch),
    Role(MysqlReplicaEpoch, MysqlReplicaRole, RoleEffect),
    Configuration(MysqlReplicaConfiguration, ConfigurationEffect),
    Progress,
    Access(MysqlAccessIntent),
    Peer(MysqlPeerOperationKind, MysqlPeerOperationIntent),
    Close,
    Abort,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RecordedOutcome {
    Open(MysqlOpenOutcome),
    Epoch(MysqlEpochOutcome),
    Role(MysqlRoleOutcome),
    Configuration(MysqlConfigurationOutcome),
    Progress(MysqlProgressOutcome),
    Access(MysqlAccessOutcome),
    Peer(MysqlPeerOperationOutcome),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OperationStatus {
    Pending,
    Uncertain,
    Completed(RecordedOutcome),
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
    epoch: Option<MysqlReplicaEpoch>,
    role: Option<MysqlReplicaRole>,
    role_epoch: Option<MysqlReplicaEpoch>,
    configuration: Option<MysqlReplicaConfiguration>,
    native_evidence: Option<MemberNativeEvidence>,
    progress: Option<MysqlProgressOutcome>,
    access: Option<MysqlAccessIntent>,
    terminal: Option<MysqlTerminalState>,
    last_operation_generation: u64,
    operations: Vec<OperationRecord>,
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
            epoch: None,
            role: None,
            role_epoch: None,
            configuration: None,
            native_evidence: None,
            progress: None,
            access: None,
            terminal: None,
            last_operation_generation: 0,
            operations: Vec::new(),
        })
    }

    /// Exact local replica incarnation.
    #[must_use]
    pub const fn identity(&self) -> &MysqlReplicaIdentity {
        &self.identity
    }

    /// Current accepted epoch.
    #[must_use]
    pub const fn epoch(&self) -> Option<MysqlReplicaEpoch> {
        self.epoch
    }

    /// Current role, which is current only when `role_epoch == epoch`.
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

    /// Returns whether both owned credential buffers were explicitly cleared.
    #[must_use]
    pub fn credentials_cleared(&self) -> bool {
        self.observer.is_cleared() && self.recovery.is_cleared()
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
            return self
                .retry_outcome(&operation, &fingerprint)
                .and_then(|outcome| {
                    if authority == receipt.authority() {
                        expect_open(outcome)
                    } else {
                        Err(MysqlReplicatorError::OpenConflict)
                    }
                });
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
        self.access = Some(MysqlAccessIntent::new(
            receipt.access_generation,
            false,
            false,
        ));
        self.complete_last(RecordedOutcome::Open(outcome.clone()));
        Ok(outcome)
    }

    /// Accepts one strictly newer epoch barrier.
    pub fn advance_epoch(
        &mut self,
        operation: MysqlOperationIdentity,
        epoch: MysqlReplicaEpoch,
    ) -> Result<MysqlEpochOutcome, MysqlReplicatorError> {
        self.require_open_active()?;
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
        self.role_epoch = None;
        self.configuration = None;
        self.progress = None;
        self.close_access();
        let outcome = MysqlEpochOutcome { epoch };
        self.complete_last(RecordedOutcome::Epoch(outcome));
        Ok(outcome)
    }

    /// Completes a role transition from one exact coherent local observation.
    pub async fn observe_role(
        &mut self,
        operation: MysqlOperationIdentity,
        epoch: MysqlReplicaEpoch,
        role: MysqlReplicaRole,
    ) -> Result<MysqlRoleOutcome, MysqlReplicatorError> {
        self.apply_role(operation, epoch, role, RoleEffect::Observe)
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
        )
        .await
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
        self.require_current_configuration()?;
        self.admit_work()?;
        let fingerprint = OperationFingerprint::Progress;
        let index = match self.begin_operation(operation, fingerprint)? {
            Admission::Retry(outcome) => return expect_progress(outcome),
            Admission::New(index) => index,
        };
        let authority = self.authority().clone();
        let evidence = match self
            .effects
            .read_member_view(&authority, &self.attempt)
            .await
        {
            Ok(evidence) => evidence,
            Err(error) => return self.uncertain_member(index, error),
        };
        self.validate_after_effect(index)?;
        if let Err(error) = self.validate_evidence(&evidence) {
            return self.uncertain_rejection(index, error);
        }
        let outcome = MysqlProgressOutcome {
            progress: project_progress(&self.lineage, evidence.executed()),
            catch_up: CatchUpCapabilityResult::retained_evidence_unavailable(),
        };
        self.native_evidence = Some(evidence);
        self.progress = Some(outcome);
        self.complete(index, RecordedOutcome::Progress(outcome));
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
        self.admit_work()?;
        let fingerprint = OperationFingerprint::Access(intent);
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
        let outcome = MysqlAccessOutcome { intent };
        self.complete_last(RecordedOutcome::Access(outcome));
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
        self.require_open_active()?;
        self.attempt
            .admit_cleanup()
            .map_err(map_cleanup_admission)?;
        let fingerprint = OperationFingerprint::Close;
        let index = match self.begin_operation(operation, fingerprint)? {
            Admission::Retry(_) => return Err(MysqlReplicatorError::OperationConflict),
            Admission::New(index) => index,
        };
        let cleanup = match self.effects.contain_member(&self.attempt) {
            Ok(cleanup) => cleanup,
            Err(error) => {
                self.operations[index].status = OperationStatus::Uncertain;
                self.close_access();
                return Err(MysqlReplicatorError::Instance(error));
            }
        };
        if cleanup != AttemptCleanupState::Completed {
            self.operations[index].status = OperationStatus::Uncertain;
            self.close_access();
            return Err(MysqlReplicatorError::CloseUnsettled);
        }
        self.clear_credentials();
        self.close_access();
        self.terminal = Some(MysqlTerminalState::Closed);
        Ok(MysqlTerminalOutcome {
            state: MysqlTerminalState::Closed,
            cleanup,
        })
    }

    /// Immediately crosses the abort boundary and attempts local containment.
    pub fn abort(
        &mut self,
        operation: MysqlOperationIdentity,
    ) -> Result<MysqlTerminalOutcome, MysqlReplicatorError> {
        self.ensure_not_terminal()?;
        let fingerprint = OperationFingerprint::Abort;
        let _ = self.begin_operation(operation, fingerprint)?;
        self.close_access();
        self.clear_credentials();
        self.terminal = Some(MysqlTerminalState::Aborted);
        let cleanup = self
            .effects
            .contain_member(&self.attempt)
            .map_err(MysqlReplicatorError::Instance)?;
        Ok(MysqlTerminalOutcome {
            state: MysqlTerminalState::Aborted,
            cleanup,
        })
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

    async fn apply_role(
        &mut self,
        operation: MysqlOperationIdentity,
        epoch: MysqlReplicaEpoch,
        role: MysqlReplicaRole,
        effect: RoleEffect,
    ) -> Result<MysqlRoleOutcome, MysqlReplicatorError> {
        self.require_open_active()?;
        self.admit_work()?;
        let fingerprint = OperationFingerprint::Role(epoch, role, effect.clone());
        let index = match self.begin_operation(operation, fingerprint)? {
            Admission::Retry(outcome) => return expect_role(outcome),
            Admission::New(index) => index,
        };
        if self.epoch != Some(epoch) || self.role_epoch == Some(epoch) {
            self.discard_last();
            return Err(MysqlReplicatorError::EpochBarrierRequired);
        }
        let authority = self.authority().clone();
        let evidence = match effect {
            RoleEffect::Observe => {
                match self
                    .effects
                    .read_member_view(&authority, &self.attempt)
                    .await
                {
                    Ok(evidence) => evidence,
                    Err(error) => return self.uncertain_member(index, error),
                }
            }
            RoleEffect::Transfer { source, target } => {
                let expected = MemberEffectKind::PrimaryTransfer {
                    target: target.member_id().clone(),
                };
                let input = PrimaryTransferInput::new(authority.clone(), *source, target)
                    .map_err(MysqlReplicatorError::MemberTransition)?;
                let receipt = match self.effects.transfer_primary(input, &self.attempt).await {
                    Ok(receipt) => receipt,
                    Err(error) => return self.uncertain_member(index, error),
                };
                if let Err(error) = self.validate_receipt(&receipt, &expected) {
                    return self.uncertain_rejection(index, error);
                }
                receipt.successor().clone()
            }
        };
        self.validate_after_effect(index)?;
        if let Err(error) = self
            .validate_evidence(&evidence)
            .and_then(|()| self.require_native_role(&evidence, role))
        {
            return self.uncertain_rejection(index, error);
        }
        self.role = Some(role);
        self.role_epoch = Some(epoch);
        self.configuration = None;
        self.native_evidence = Some(evidence);
        self.progress = None;
        self.close_access();
        let outcome = MysqlRoleOutcome { epoch, role };
        self.complete(index, RecordedOutcome::Role(outcome));
        Ok(outcome)
    }

    async fn apply_configuration(
        &mut self,
        operation: MysqlOperationIdentity,
        configuration: MysqlReplicaConfiguration,
        effect: ConfigurationEffect,
    ) -> Result<MysqlConfigurationOutcome, MysqlReplicatorError> {
        self.require_open_active()?;
        self.admit_work()?;
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
        let authority = self.authority().clone();
        let evidence = match effect {
            ConfigurationEffect::Observe => {
                match self
                    .effects
                    .read_member_view(&authority, &self.attempt)
                    .await
                {
                    Ok(evidence) => evidence,
                    Err(error) => return self.uncertain_member(index, error),
                }
            }
            ConfigurationEffect::Bootstrap => {
                let receipt = match self
                    .effects
                    .bootstrap_member(
                        FreshBootstrapInput::new(authority),
                        &self.recovery,
                        &self.attempt,
                    )
                    .await
                {
                    Ok(receipt) => receipt,
                    Err(error) => return self.uncertain_member(index, error),
                };
                if let Err(error) = self.validate_receipt(&receipt, &MemberEffectKind::Bootstrap) {
                    return self.uncertain_rejection(index, error);
                }
                receipt.successor().clone()
            }
            ConfigurationEffect::Join { source, peers } => {
                let input = FreshJoinInput::new(authority, *source, peers)
                    .map_err(MysqlReplicatorError::MemberTransition)?;
                let receipt = match self
                    .effects
                    .join_member(input, &self.recovery, &self.attempt)
                    .await
                {
                    Ok(receipt) => receipt,
                    Err(error) => return self.uncertain_member(index, error),
                };
                if let Err(error) = self.validate_receipt(&receipt, &MemberEffectKind::Join) {
                    return self.uncertain_rejection(index, error);
                }
                receipt.successor().clone()
            }
        };
        self.validate_after_effect(index)?;
        if let Err(error) = self.validate_evidence(&evidence).and_then(|()| {
            self.require_native_role(
                &evidence,
                self.role
                    .expect("current role checked before configuration"),
            )
            .and_then(|()| self.require_configuration_evidence(&configuration, &evidence))
        }) {
            return self.uncertain_rejection(index, error);
        }
        self.configuration = Some(configuration.clone());
        self.native_evidence = Some(evidence);
        self.progress = None;
        self.clear_credentials();
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
        let fingerprint = OperationFingerprint::Peer(kind, intent.clone());
        match self.begin_operation(operation, fingerprint)? {
            Admission::Retry(outcome) => return expect_peer(outcome),
            Admission::New(_) => {}
        }
        if let Err(error) = self.require_current_configuration() {
            self.discard_last();
            return Err(error);
        }
        if self.role != Some(MysqlReplicaRole::Primary) {
            self.discard_last();
            return Err(MysqlReplicatorError::PrimaryRequired);
        }
        let configuration = self
            .configuration
            .as_ref()
            .expect("current configuration was required")
            .clone();
        if intent.epoch != self.epoch.expect("current configuration has an epoch")
            || intent.configuration != configuration.id
        {
            self.discard_last();
            return Err(MysqlReplicatorError::ConfigurationRequired);
        }
        let target = intent.authority.target().clone();
        let local_member = self.authority().local().member_id();
        if target.identity == self.identity || target.member.member_id() == local_member {
            self.discard_last();
            return Err(MysqlReplicatorError::SelfTarget);
        }
        if !configuration.members.iter().any(|member| member == &target) {
            self.discard_last();
            return Err(MysqlReplicatorError::InvalidConfiguration);
        }
        let outcome = MysqlPeerOperationOutcome::Unsupported {
            kind,
            reason: MysqlPeerUnsupportedReason::NativeExecutionUnavailable,
            target,
        };
        self.complete_last(RecordedOutcome::Peer(outcome.clone()));
        Ok(outcome)
    }

    fn validate_configuration(
        &self,
        configuration: &MysqlReplicaConfiguration,
    ) -> Result<(), MysqlReplicatorError> {
        let authority = self.authority();
        let local_member = authority.local().member_id();
        let mut local_count = 0;
        for (index, member) in configuration.members.iter().enumerate() {
            if member.identity == self.identity && member.member.member_id() == local_member {
                local_count += 1;
            }
            if configuration.members[index + 1..].iter().any(|other| {
                other.identity == member.identity
                    || other.member.member_id() == member.member.member_id()
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
    ) -> Result<(), MysqlReplicatorError> {
        if receipt.kind() != expected
            || receipt.authority() != self.authority()
            || receipt.cancellation_state() != AttemptCancellationState::Active
            || receipt.deadline_state() != AttemptDeadlineState::Work
            || receipt.cleanup_state() == AttemptCleanupState::Failed
        {
            return Err(MysqlReplicatorError::ReceiptMismatch);
        }
        self.validate_evidence(receipt.successor())
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
        if let Some(record) = self
            .operations
            .iter()
            .find(|record| record.operation.id == operation.id)
        {
            if record.operation != operation || record.fingerprint != fingerprint {
                return Err(MysqlReplicatorError::OperationConflict);
            }
            return match &record.status {
                OperationStatus::Completed(outcome) => Ok(Admission::Retry(outcome.clone())),
                OperationStatus::Pending | OperationStatus::Uncertain => {
                    Err(MysqlReplicatorError::OperationUncertain)
                }
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
        }
    }

    fn complete(&mut self, index: usize, outcome: RecordedOutcome) {
        self.operations[index].status = OperationStatus::Completed(outcome);
    }

    fn complete_last(&mut self, outcome: RecordedOutcome) {
        let index = self.operations.len() - 1;
        self.complete(index, outcome);
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

    fn close_access(&mut self) {
        if let Some(access) = self.access.as_mut() {
            access.read = false;
            access.write = false;
        }
    }

    fn clear_credentials(&mut self) {
        self.observer.clear();
        self.recovery.clear();
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
