//! Member-scoped native mutation inputs, evidence, receipts, and boundary.

use std::num::NonZeroU64;

use crate::core::{
    AttemptId, AuthorityGeneration, ConfigurationId, EndpointBinding, GroupName, GtidSet,
    MemberAddress, MemberId, MemberRole, MemberState, NativeMember, NativeView, ProcessSessionId,
    ReplicaId, ReplicaIncarnation, StorageBinding, ViewId,
};
use crate::service::{
    AttemptCancellationState, AttemptCleanupState, AttemptContext, AttemptDeadlineState,
    ControlCredential, MemberAuthorityError, MemberReceiptError, MemberTransitionError,
    MemberViewError, MysqlInstanceConfig, MysqlInstanceError, MysqlMemberIndex,
    NativeIdentityEnrollment,
};

/// Canonically decoded Oracle XCom view identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalViewId {
    fixed: u64,
    monotonic: u32,
}

impl CanonicalViewId {
    /// Parses exact canonical `<u64>:<u32>` notation.
    pub fn parse(view_id: &ViewId) -> Result<Self, MemberViewError> {
        let Some((fixed, monotonic)) = view_id.as_str().split_once(':') else {
            return Err(MemberViewError::MalformedViewId);
        };
        if fixed.is_empty()
            || monotonic.is_empty()
            || monotonic.contains(':')
            || !canonical_decimal(fixed)
            || !canonical_decimal(monotonic)
        {
            return Err(MemberViewError::MalformedViewId);
        }
        Ok(Self {
            fixed: fixed
                .parse()
                .map_err(|_| MemberViewError::MalformedViewId)?,
            monotonic: monotonic
                .parse()
                .map_err(|_| MemberViewError::MalformedViewId)?,
        })
    }

    /// Requires the exact next canonical XCom view.
    pub fn require_successor(self, successor: Self) -> Result<(), MemberViewError> {
        if successor.fixed != self.fixed {
            return Err(MemberViewError::ViewFixedPartChanged);
        }
        let expected = self
            .monotonic
            .checked_add(1)
            .ok_or(MemberViewError::ViewSequenceOverflow)?;
        if successor.monotonic != expected {
            return Err(MemberViewError::ViewSequenceNotSuccessor);
        }
        Ok(())
    }
}

fn canonical_decimal(value: &str) -> bool {
    value.bytes().all(|byte| byte.is_ascii_digit()) && (value == "0" || !value.starts_with('0'))
}

/// Immutable peer identity that grants no process or storage ownership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberPeer {
    member_id: MemberId,
    address: MemberAddress,
}

impl MemberPeer {
    /// Creates one immutable native peer input.
    #[must_use]
    pub const fn new(member_id: MemberId, address: MemberAddress) -> Self {
        Self { member_id, address }
    }

    /// Derives peer identity without transferring process ownership.
    #[must_use]
    pub fn from_enrollment(enrollment: &NativeIdentityEnrollment) -> Self {
        Self::new(
            enrollment.member_id().clone(),
            enrollment.binding().member_address().clone(),
        )
    }

    /// Native member UUID.
    #[must_use]
    pub const fn member_id(&self) -> &MemberId {
        &self.member_id
    }

    /// Reported native SQL address.
    #[must_use]
    pub const fn address(&self) -> &MemberAddress {
        &self.address
    }
}

/// Exact local mutation authority for one owned process generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberMutationAuthority {
    attempt: AttemptId,
    group_name: GroupName,
    local: NativeIdentityEnrollment,
}

impl MemberMutationAuthority {
    /// Binds mutation authority to one enrolled local process generation.
    pub fn new(
        attempt: AttemptId,
        group_name: GroupName,
        local: NativeIdentityEnrollment,
    ) -> Result<Self, MemberTransitionError> {
        if local.binding().attempt() != &attempt {
            return Err(MemberTransitionError::Authority(
                MemberAuthorityError::AttemptMismatch,
            ));
        }
        Ok(Self {
            attempt,
            group_name,
            local,
        })
    }

    /// Attempt identity.
    #[must_use]
    pub const fn attempt(&self) -> &AttemptId {
        &self.attempt
    }

    /// Configured Group Replication identity.
    #[must_use]
    pub const fn group_name(&self) -> &GroupName {
        &self.group_name
    }

    /// Exact local enrollment.
    #[must_use]
    pub const fn local(&self) -> &NativeIdentityEnrollment {
        &self.local
    }

    /// Fixed local member slot.
    #[must_use]
    pub const fn member(&self) -> MysqlMemberIndex {
        self.local.binding().member()
    }
}

/// One bracketed coherent native membership and history readback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberNativeEvidence {
    authority: MemberMutationAuthority,
    view: NativeView,
    executed: GtidSet,
    purged: GtidSet,
}

impl MemberNativeEvidence {
    /// Validates local identity, membership uniqueness, primary cardinality,
    /// and group-only history.
    pub fn new(
        authority: MemberMutationAuthority,
        view: NativeView,
        executed: GtidSet,
    ) -> Result<Self, MemberTransitionError> {
        if view.group_name() != authority.group_name() {
            return Err(MemberTransitionError::Authority(
                MemberAuthorityError::AuthoritySubstitution,
            ));
        }
        let local =
            view.member(authority.local().member_id())
                .ok_or(MemberTransitionError::View(
                    MemberViewError::RequiredMemberMissing,
                ))?;
        if local.address() != authority.local().binding().member_address() {
            return Err(MemberTransitionError::View(
                MemberViewError::MemberAddressMismatch,
            ));
        }
        if view
            .members()
            .iter()
            .filter(|member| member.role() == MemberRole::Primary)
            .count()
            != 1
        {
            return Err(MemberTransitionError::View(
                MemberViewError::PrimaryCardinality,
            ));
        }
        validate_group_sources(authority.group_name(), &executed)?;
        CanonicalViewId::parse(view.id()).map_err(MemberTransitionError::View)?;
        Ok(Self {
            authority,
            view,
            executed,
            purged: GtidSet::empty(),
        })
    }

    pub(crate) fn new_with_purged(
        authority: MemberMutationAuthority,
        view: NativeView,
        executed: GtidSet,
        purged: GtidSet,
    ) -> Result<Self, MemberTransitionError> {
        let mut evidence = Self::new(authority, view, executed)?;
        validate_group_sources(evidence.authority.group_name(), &purged)?;
        evidence.purged = purged;
        Ok(evidence)
    }

    /// Exact local authority used for this readback.
    #[must_use]
    pub const fn authority(&self) -> &MemberMutationAuthority {
        &self.authority
    }

    /// Exact native view.
    #[must_use]
    pub const fn view(&self) -> &NativeView {
        &self.view
    }

    /// Complete structured executed history.
    #[must_use]
    pub const fn executed(&self) -> &GtidSet {
        &self.executed
    }

    /// Complete structured purged history sampled in the same bracket.
    #[must_use]
    pub const fn purged(&self) -> &GtidSet {
        &self.purged
    }

    /// Returns the exact current primary.
    #[must_use]
    pub fn primary(&self) -> &NativeMember {
        self.view
            .members()
            .iter()
            .find(|member| member.role() == MemberRole::Primary)
            .expect("construction requires exactly one primary")
    }

    /// Tests coherence with another independently owned member readback.
    #[must_use]
    pub fn coherent_with(&self, other: &Self) -> bool {
        self.authority.attempt == other.authority.attempt
            && self.view == other.view
            && self.executed == other.executed
    }
}

/// Exact source evidence frozen before a member-scoped mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberSourceBoundary {
    source: MemberPeer,
    evidence: MemberNativeEvidence,
}

impl MemberSourceBoundary {
    /// Freezes one exact online source from a coherent view.
    pub fn new(
        source: MemberPeer,
        evidence: MemberNativeEvidence,
    ) -> Result<Self, MemberTransitionError> {
        if source.member_id() != evidence.authority().local().member_id()
            || source.address() != evidence.authority().local().binding().member_address()
        {
            return Err(MemberTransitionError::Authority(
                MemberAuthorityError::SourceMismatch,
            ));
        }
        let member =
            evidence
                .view()
                .member(source.member_id())
                .ok_or(MemberTransitionError::Authority(
                    MemberAuthorityError::SourceMismatch,
                ))?;
        if member.address() != source.address() {
            return Err(MemberTransitionError::Authority(
                MemberAuthorityError::SourceMismatch,
            ));
        }
        if member.state() != MemberState::Online {
            return Err(MemberTransitionError::View(
                MemberViewError::MemberNotOnline,
            ));
        }
        Ok(Self { source, evidence })
    }

    /// Exact source identity.
    #[must_use]
    pub const fn source(&self) -> &MemberPeer {
        &self.source
    }

    /// Pre-effect coherent evidence.
    #[must_use]
    pub const fn evidence(&self) -> &MemberNativeEvidence {
        &self.evidence
    }
}

/// Exact neutral lifecycle operation identity attached to a member effect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberOperationIdentity {
    id: AuthorityGeneration,
    generation: NonZeroU64,
}

impl MemberOperationIdentity {
    /// Creates one exact operation identity.
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

/// Exact non-owning configured-member facts attached to a lifecycle request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberConfigurationTarget {
    replica_id: ReplicaId,
    incarnation: ReplicaIncarnation,
    member: MemberPeer,
    process_session: ProcessSessionId,
    endpoint: EndpointBinding,
    storage: StorageBinding,
}

impl MemberConfigurationTarget {
    /// Creates one exact configured-member binding.
    #[must_use]
    pub const fn new(
        replica_id: ReplicaId,
        incarnation: ReplicaIncarnation,
        member: MemberPeer,
        process_session: ProcessSessionId,
        endpoint: EndpointBinding,
        storage: StorageBinding,
    ) -> Self {
        Self {
            replica_id,
            incarnation,
            member,
            process_session,
            endpoint,
            storage,
        }
    }
}

/// Complete requested configuration attached to a member effect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberConfigurationIntent {
    id: ConfigurationId,
    members: Vec<MemberConfigurationTarget>,
}

impl MemberConfigurationIntent {
    /// Creates one exact configuration request.
    #[must_use]
    pub fn new(id: ConfigurationId, members: Vec<MemberConfigurationTarget>) -> Self {
        Self { id, members }
    }

    /// Configuration identity.
    #[must_use]
    pub const fn id(&self) -> &ConfigurationId {
        &self.id
    }

    /// Exact configured-member facts.
    #[must_use]
    pub fn members(&self) -> &[MemberConfigurationTarget] {
        &self.members
    }
}

/// Exact lifecycle request whose native evidence may receive credit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberEffectRequest {
    operation: MemberOperationIdentity,
    epoch: u64,
    role: MemberRole,
    configuration: Option<MemberConfigurationIntent>,
    authority: MemberMutationAuthority,
    source: Option<MemberSourceBoundary>,
}

impl MemberEffectRequest {
    /// Creates one fully explicit member-effect request.
    pub fn new(
        operation: MemberOperationIdentity,
        epoch: u64,
        role: MemberRole,
        configuration: Option<MemberConfigurationIntent>,
        authority: MemberMutationAuthority,
        source: Option<MemberSourceBoundary>,
    ) -> Result<Self, MemberTransitionError> {
        if let Some(source) = source.as_ref() {
            validate_attempt(&authority, source.evidence())?;
        }
        Ok(Self {
            operation,
            epoch,
            role,
            configuration,
            authority,
            source,
        })
    }

    /// Creates a Build-scoped request whose predecessor belongs to the
    /// authorized source lifetime while the effect authority belongs to the
    /// fresh target lifetime. All non-attempt authority remains exact.
    pub(crate) fn new_for_build(
        operation: MemberOperationIdentity,
        epoch: u64,
        role: MemberRole,
        configuration: Option<MemberConfigurationIntent>,
        authority: MemberMutationAuthority,
        source: MemberSourceBoundary,
    ) -> Result<Self, MemberTransitionError> {
        if authority.group_name() != source.evidence().view().group_name() {
            return Err(MemberTransitionError::Authority(
                MemberAuthorityError::AttemptMismatch,
            ));
        }
        Ok(Self {
            operation,
            epoch,
            role,
            configuration,
            authority,
            source: Some(source),
        })
    }

    /// Exact lifecycle operation identity.
    #[must_use]
    pub const fn operation(&self) -> &MemberOperationIdentity {
        &self.operation
    }

    /// Requested lifecycle epoch.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Requested native role.
    #[must_use]
    pub const fn role(&self) -> MemberRole {
        self.role
    }

    /// Requested configuration identity, when configuration is part of the operation.
    #[must_use]
    pub const fn configuration(&self) -> Option<&MemberConfigurationIntent> {
        self.configuration.as_ref()
    }

    /// Exact current local authority.
    #[must_use]
    pub const fn authority(&self) -> &MemberMutationAuthority {
        &self.authority
    }

    /// Exact predecessor/source boundary, when the effect requires one.
    #[must_use]
    pub const fn source(&self) -> Option<&MemberSourceBoundary> {
        self.source.as_ref()
    }
}

/// Fresh one-member bootstrap input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FreshBootstrapInput {
    authority: MemberMutationAuthority,
    request: Option<MemberEffectRequest>,
}

impl FreshBootstrapInput {
    /// Creates member-scoped bootstrap input.
    #[must_use]
    pub const fn new(authority: MemberMutationAuthority) -> Self {
        Self {
            authority,
            request: None,
        }
    }

    /// Creates lifecycle-bound member-scoped bootstrap input.
    pub fn for_request(request: MemberEffectRequest) -> Result<Self, MemberTransitionError> {
        if request.source().is_some() {
            return Err(MemberTransitionError::Receipt(
                MemberReceiptError::PredecessorMismatch,
            ));
        }
        Ok(Self {
            authority: request.authority().clone(),
            request: Some(request),
        })
    }

    /// Exact lifecycle request, when invoked through the adapter engine.
    #[must_use]
    pub const fn request(&self) -> Option<&MemberEffectRequest> {
        self.request.as_ref()
    }

    /// Exact local authority.
    #[must_use]
    pub const fn authority(&self) -> &MemberMutationAuthority {
        &self.authority
    }
}

/// Fresh non-bootstrap join input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FreshJoinInput {
    authority: MemberMutationAuthority,
    request: Option<MemberEffectRequest>,
    source: MemberSourceBoundary,
    peers: Vec<MemberPeer>,
}

impl FreshJoinInput {
    /// Creates join input from immutable peer evidence without peer mutation
    /// authority.
    pub fn new(
        authority: MemberMutationAuthority,
        source: MemberSourceBoundary,
        peers: Vec<MemberPeer>,
    ) -> Result<Self, MemberTransitionError> {
        Self::new_internal(authority, None, source, peers)
    }

    /// Creates a lifecycle-bound join request.
    pub fn for_request(
        request: MemberEffectRequest,
        source: MemberSourceBoundary,
        peers: Vec<MemberPeer>,
    ) -> Result<Self, MemberTransitionError> {
        if request.source() != Some(&source) {
            return Err(MemberTransitionError::Receipt(
                MemberReceiptError::PredecessorMismatch,
            ));
        }
        Self::new_internal(request.authority().clone(), Some(request), source, peers)
    }

    /// Creates the exact cross-lifetime join used only by an application-owned
    /// Build. The source and target may have distinct parent attempts, while
    /// group, target authority, predecessor, and peer evidence remain exact.
    pub(crate) fn for_build_request(
        request: MemberEffectRequest,
        source: MemberSourceBoundary,
        peers: Vec<MemberPeer>,
    ) -> Result<Self, MemberTransitionError> {
        if request.source() != Some(&source)
            || request.authority().group_name() != source.evidence().view().group_name()
        {
            return Err(MemberTransitionError::Receipt(
                MemberReceiptError::PredecessorMismatch,
            ));
        }
        if source
            .evidence()
            .view()
            .member(request.authority().local().member_id())
            .is_some()
        {
            return Err(MemberTransitionError::Authority(
                MemberAuthorityError::AuthoritySubstitution,
            ));
        }
        validate_peers(source.evidence().view(), &peers)?;
        Ok(Self {
            authority: request.authority().clone(),
            request: Some(request),
            source,
            peers,
        })
    }

    fn new_internal(
        authority: MemberMutationAuthority,
        request: Option<MemberEffectRequest>,
        source: MemberSourceBoundary,
        peers: Vec<MemberPeer>,
    ) -> Result<Self, MemberTransitionError> {
        validate_attempt(&authority, source.evidence())?;
        if source
            .evidence()
            .view()
            .member(authority.local().member_id())
            .is_some()
        {
            return Err(MemberTransitionError::Authority(
                MemberAuthorityError::AuthoritySubstitution,
            ));
        }
        validate_peers(source.evidence().view(), &peers)?;
        Ok(Self {
            authority,
            request,
            source,
            peers,
        })
    }

    /// Exact lifecycle request, when invoked through the adapter engine.
    #[must_use]
    pub const fn request(&self) -> Option<&MemberEffectRequest> {
        self.request.as_ref()
    }

    /// Exact joining-member authority.
    #[must_use]
    pub const fn authority(&self) -> &MemberMutationAuthority {
        &self.authority
    }

    /// Exact predecessor boundary.
    #[must_use]
    pub const fn source(&self) -> &MemberSourceBoundary {
        &self.source
    }

    /// Immutable predecessor peer identities.
    #[must_use]
    pub fn peers(&self) -> &[MemberPeer] {
        &self.peers
    }
}

/// Exact single-primary transfer input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrimaryTransferInput {
    authority: MemberMutationAuthority,
    request: Option<MemberEffectRequest>,
    source: MemberSourceBoundary,
    target: MemberPeer,
}

impl PrimaryTransferInput {
    /// Creates a transfer only from the exact locally owned current primary to
    /// one exact online secondary.
    pub fn new(
        authority: MemberMutationAuthority,
        source: MemberSourceBoundary,
        target: MemberPeer,
    ) -> Result<Self, MemberTransitionError> {
        Self::new_internal(authority, None, source, target)
    }

    /// Creates a lifecycle-bound primary-transfer request.
    pub fn for_request(
        request: MemberEffectRequest,
        source: MemberSourceBoundary,
        target: MemberPeer,
    ) -> Result<Self, MemberTransitionError> {
        if request.source() != Some(&source) {
            return Err(MemberTransitionError::Receipt(
                MemberReceiptError::PredecessorMismatch,
            ));
        }
        Self::new_internal(request.authority().clone(), Some(request), source, target)
    }

    fn new_internal(
        authority: MemberMutationAuthority,
        request: Option<MemberEffectRequest>,
        source: MemberSourceBoundary,
        target: MemberPeer,
    ) -> Result<Self, MemberTransitionError> {
        validate_attempt(&authority, source.evidence())?;
        if source.source().member_id() != authority.local().member_id()
            || source.source().address() != authority.local().binding().member_address()
            || source.evidence().authority() != &authority
        {
            return Err(MemberTransitionError::Authority(
                MemberAuthorityError::SourceMismatch,
            ));
        }
        let source_member = source
            .evidence()
            .view()
            .member(source.source().member_id())
            .expect("source boundary construction requires source membership");
        let target_member = source.evidence().view().member(target.member_id()).ok_or(
            MemberTransitionError::Authority(MemberAuthorityError::WrongTransferTarget),
        )?;
        if target.member_id() == source.source().member_id()
            || target_member.address() != target.address()
            || source_member.role() != MemberRole::Primary
            || target_member.role() != MemberRole::Secondary
            || target_member.state() != MemberState::Online
        {
            return Err(MemberTransitionError::Authority(
                MemberAuthorityError::WrongTransferTarget,
            ));
        }
        Ok(Self {
            authority,
            request,
            source,
            target,
        })
    }

    /// Exact lifecycle request, when invoked through the adapter engine.
    #[must_use]
    pub const fn request(&self) -> Option<&MemberEffectRequest> {
        self.request.as_ref()
    }

    /// Exact local source authority.
    #[must_use]
    pub const fn authority(&self) -> &MemberMutationAuthority {
        &self.authority
    }

    /// Frozen source boundary.
    #[must_use]
    pub const fn source(&self) -> &MemberSourceBoundary {
        &self.source
    }

    /// Exact transfer target.
    #[must_use]
    pub const fn target(&self) -> &MemberPeer {
        &self.target
    }
}

/// Native member effect represented by a receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MemberEffectKind {
    /// Fresh designated bootstrap.
    Bootstrap,
    /// Fresh non-bootstrap join.
    Join,
    /// Exact single-primary transfer.
    PrimaryTransfer {
        /// Exact target native UUID.
        target: MemberId,
    },
    /// Coherent read-only membership/view collection.
    ViewReadback,
}

/// Typed effect receipt required before lifecycle credit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberEffectReceipt {
    kind: MemberEffectKind,
    request: Option<MemberEffectRequest>,
    authority: MemberMutationAuthority,
    predecessor: Option<MemberNativeEvidence>,
    successor: MemberNativeEvidence,
    deadline: AttemptDeadlineState,
    cancellation: AttemptCancellationState,
    cleanup: AttemptCleanupState,
}

/// Exact local terminal containment proof returned by the member effect
/// boundary after the admitted process is stopped, reaped, and absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberContainmentReceipt {
    authority: MemberMutationAuthority,
    binding: crate::service::MemberControlBinding,
    cleanup: AttemptCleanupState,
    process_absent: bool,
    pid_absent: bool,
    socket_absent: bool,
    endpoint_absent: bool,
    scratch_removed: bool,
    retired_owner_generation: Option<u64>,
    retired_process_generation: Option<u64>,
    retired_process_id: Option<u32>,
}

impl MemberContainmentReceipt {
    /// Creates one exact terminal receipt from explicit effect-boundary
    /// observations. Generic cleanup completion is not terminal provenance.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        authority: MemberMutationAuthority,
        cleanup: AttemptCleanupState,
        process_absent: bool,
        pid_absent: bool,
        socket_absent: bool,
        endpoint_absent: bool,
        scratch_removed: bool,
    ) -> Self {
        let binding = authority.local().binding().clone();
        Self {
            authority,
            binding,
            cleanup,
            process_absent,
            pid_absent,
            socket_absent,
            endpoint_absent,
            scratch_removed,
            retired_owner_generation: None,
            retired_process_generation: None,
            retired_process_id: None,
        }
    }

    /// Attaches the exact in-process generation retired by a Build Drop guard.
    #[must_use]
    pub(crate) fn with_retired_generation(
        mut self,
        owner_generation: u64,
        generation: u64,
        process_id: Option<u32>,
    ) -> Self {
        self.retired_owner_generation = Some(owner_generation);
        self.retired_process_generation = Some(generation);
        self.retired_process_id = process_id;
        self
    }

    /// Exact admitted member authority.
    #[must_use]
    pub const fn authority(&self) -> &MemberMutationAuthority {
        &self.authority
    }

    /// Exact admitted process, endpoint, storage, and native binding.
    #[must_use]
    pub const fn binding(&self) -> &crate::service::MemberControlBinding {
        &self.binding
    }

    /// Cleanup settlement.
    #[must_use]
    pub const fn cleanup(&self) -> AttemptCleanupState {
        self.cleanup
    }

    /// Whether the exact retained process is absent after reaping.
    #[must_use]
    pub const fn process_absent(&self) -> bool {
        self.process_absent
    }

    /// Whether the exact process PID file is absent.
    #[must_use]
    pub const fn pid_absent(&self) -> bool {
        self.pid_absent
    }

    /// Whether the exact private socket path is absent.
    #[must_use]
    pub const fn socket_absent(&self) -> bool {
        self.socket_absent
    }

    /// Whether the exact private endpoint is absent.
    #[must_use]
    pub const fn endpoint_absent(&self) -> bool {
        self.endpoint_absent
    }

    /// Whether disposable scratch ownership was removed.
    #[must_use]
    pub const fn scratch_removed(&self) -> bool {
        self.scratch_removed
    }

    /// Exact manager owner generation that issued the retired child.
    #[must_use]
    pub const fn retired_owner_generation(&self) -> Option<u64> {
        self.retired_owner_generation
    }

    /// Exact manager-local generation retired by Build interruption.
    #[must_use]
    pub const fn retired_process_generation(&self) -> Option<u64> {
        self.retired_process_generation
    }

    /// Exact retained child PID captured when the Build guard was armed.
    #[must_use]
    pub const fn retired_process_id(&self) -> Option<u32> {
        self.retired_process_id
    }

    /// Whether this receipt proves complete terminal containment.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        matches!(self.cleanup, AttemptCleanupState::Completed)
            && self.process_absent
            && self.pid_absent
            && self.socket_absent
            && self.endpoint_absent
            && self.scratch_removed
    }
}

impl MemberEffectReceipt {
    /// Validates and creates an exact member effect receipt.
    pub fn new(
        kind: MemberEffectKind,
        authority: MemberMutationAuthority,
        predecessor: Option<MemberNativeEvidence>,
        successor: MemberNativeEvidence,
        final_binding: &crate::service::MemberControlBinding,
        attempt: &AttemptContext,
        cleanup: AttemptCleanupState,
    ) -> Result<Self, MemberTransitionError> {
        Self::new_internal(
            kind,
            authority,
            None,
            predecessor,
            successor,
            final_binding,
            attempt,
            cleanup,
            false,
        )
    }

    /// Validates a Build receipt with distinct source/target parent attempts.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn for_build_request(
        kind: MemberEffectKind,
        request: MemberEffectRequest,
        predecessor: MemberNativeEvidence,
        successor: MemberNativeEvidence,
        final_binding: &crate::service::MemberControlBinding,
        attempt: &AttemptContext,
        cleanup: AttemptCleanupState,
    ) -> Result<Self, MemberTransitionError> {
        let authority = request.authority().clone();
        Self::new_internal(
            kind,
            authority,
            Some(request),
            Some(predecessor),
            successor,
            final_binding,
            attempt,
            cleanup,
            true,
        )
    }

    /// Validates and creates a lifecycle-request-bound effect receipt.
    #[allow(clippy::too_many_arguments)]
    pub fn for_request(
        kind: MemberEffectKind,
        request: MemberEffectRequest,
        predecessor: Option<MemberNativeEvidence>,
        successor: MemberNativeEvidence,
        final_binding: &crate::service::MemberControlBinding,
        attempt: &AttemptContext,
        cleanup: AttemptCleanupState,
    ) -> Result<Self, MemberTransitionError> {
        let authority = request.authority().clone();
        Self::new_internal(
            kind,
            authority,
            Some(request),
            predecessor,
            successor,
            final_binding,
            attempt,
            cleanup,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_internal(
        kind: MemberEffectKind,
        authority: MemberMutationAuthority,
        request: Option<MemberEffectRequest>,
        predecessor: Option<MemberNativeEvidence>,
        successor: MemberNativeEvidence,
        final_binding: &crate::service::MemberControlBinding,
        attempt: &AttemptContext,
        cleanup: AttemptCleanupState,
        allow_cross_attempt_predecessor: bool,
    ) -> Result<Self, MemberTransitionError> {
        let deadline = attempt.deadline_state();
        let cancellation = attempt.cancellation_state();
        if attempt.attempt() != authority.attempt()
            || final_binding != authority.local().binding()
            || successor.authority() != &authority
            || predecessor.as_ref().is_some_and(|evidence| {
                (!allow_cross_attempt_predecessor
                    && evidence.authority().attempt() != authority.attempt())
                    || evidence.view().group_name() != authority.group_name()
            })
        {
            return Err(MemberTransitionError::Receipt(
                MemberReceiptError::BindingMismatch,
            ));
        }
        if cancellation == AttemptCancellationState::Cancelled {
            return Err(MemberTransitionError::Receipt(
                MemberReceiptError::Cancelled,
            ));
        }
        if deadline != AttemptDeadlineState::Work {
            return Err(MemberTransitionError::Receipt(
                MemberReceiptError::Deadline(deadline),
            ));
        }
        if cleanup == AttemptCleanupState::Failed {
            return Err(MemberTransitionError::Receipt(
                MemberReceiptError::CleanupIncomplete,
            ));
        }
        if let Some(request) = request.as_ref() {
            match (&kind, request.source(), predecessor.as_ref()) {
                (
                    MemberEffectKind::Join | MemberEffectKind::PrimaryTransfer { .. },
                    Some(source),
                    Some(predecessor),
                ) if source.evidence() == predecessor => {}
                (MemberEffectKind::Bootstrap | MemberEffectKind::ViewReadback, None, None) => {}
                _ => {
                    return Err(MemberTransitionError::Receipt(
                        MemberReceiptError::PredecessorMismatch,
                    ));
                }
            }
        }
        validate_receipt(&kind, &authority, predecessor.as_ref(), &successor)?;
        Ok(Self {
            kind,
            request,
            authority,
            predecessor,
            successor,
            deadline,
            cancellation,
            cleanup,
        })
    }

    /// Effect kind and exact transfer target when applicable.
    #[must_use]
    pub const fn kind(&self) -> &MemberEffectKind {
        &self.kind
    }

    /// Exact lifecycle request that produced this receipt.
    #[must_use]
    pub const fn request(&self) -> Option<&MemberEffectRequest> {
        self.request.as_ref()
    }

    /// Exact local process/storage/endpoint/native authority.
    #[must_use]
    pub const fn authority(&self) -> &MemberMutationAuthority {
        &self.authority
    }

    /// Optional predecessor native evidence.
    #[must_use]
    pub const fn predecessor(&self) -> Option<&MemberNativeEvidence> {
        self.predecessor.as_ref()
    }

    /// Accepted successor native evidence.
    #[must_use]
    pub const fn successor(&self) -> &MemberNativeEvidence {
        &self.successor
    }

    /// Deadline state at receipt creation.
    #[must_use]
    pub const fn deadline_state(&self) -> AttemptDeadlineState {
        self.deadline
    }

    /// Cancellation state at receipt creation.
    #[must_use]
    pub const fn cancellation_state(&self) -> AttemptCancellationState {
        self.cancellation
    }

    /// Cleanup state paired with the effect.
    #[must_use]
    pub const fn cleanup_state(&self) -> AttemptCleanupState {
        self.cleanup
    }
}

/// Exact nonzero native backend connection identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BackendConnectionId(NonZeroU64);

impl BackendConnectionId {
    /// Creates a nonzero native connection identifier.
    #[must_use]
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    /// Numeric native connection identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Proof that one exact local backend connection was terminated and absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendConnectionReceipt {
    authority: MemberMutationAuthority,
    connection: BackendConnectionId,
    deadline: AttemptDeadlineState,
    cancellation: AttemptCancellationState,
    cleanup: AttemptCleanupState,
}

impl BackendConnectionReceipt {
    /// Creates exact native absence evidence.
    pub fn new(
        authority: MemberMutationAuthority,
        connection: BackendConnectionId,
        final_binding: &crate::service::MemberControlBinding,
        attempt: &AttemptContext,
        cleanup: AttemptCleanupState,
    ) -> Result<Self, MemberTransitionError> {
        let deadline = attempt.deadline_state();
        let cancellation = attempt.cancellation_state();
        if attempt.attempt() != authority.attempt() || final_binding != authority.local().binding()
        {
            return Err(MemberTransitionError::Receipt(
                MemberReceiptError::BindingMismatch,
            ));
        }
        if cancellation == AttemptCancellationState::Cancelled {
            return Err(MemberTransitionError::Receipt(
                MemberReceiptError::Cancelled,
            ));
        }
        if deadline != AttemptDeadlineState::Work {
            return Err(MemberTransitionError::Receipt(
                MemberReceiptError::Deadline(deadline),
            ));
        }
        if cleanup == AttemptCleanupState::Failed {
            return Err(MemberTransitionError::Receipt(
                MemberReceiptError::CleanupIncomplete,
            ));
        }
        Ok(Self {
            authority,
            connection,
            deadline,
            cancellation,
            cleanup,
        })
    }

    /// Exact local authority.
    #[must_use]
    pub const fn authority(&self) -> &MemberMutationAuthority {
        &self.authority
    }

    /// Exact terminated connection.
    #[must_use]
    pub const fn connection(&self) -> BackendConnectionId {
        self.connection
    }

    /// Deadline state at native absence proof.
    #[must_use]
    pub const fn deadline_state(&self) -> AttemptDeadlineState {
        self.deadline
    }

    /// Cleanup state at native absence proof.
    #[must_use]
    pub const fn cleanup_state(&self) -> AttemptCleanupState {
        self.cleanup
    }
}

/// Injected member-local effect boundary used by the lifecycle engine.
#[allow(async_fn_in_trait)]
pub trait MysqlMemberEffectBoundary {
    /// Immutable validated local configuration.
    fn config(&self) -> &MysqlInstanceConfig;

    /// Executes fresh designated bootstrap.
    async fn bootstrap_member(
        &mut self,
        input: FreshBootstrapInput,
        recovery: &ControlCredential,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError>;

    /// Executes one fresh non-bootstrap join.
    async fn join_member(
        &mut self,
        input: FreshJoinInput,
        recovery: &ControlCredential,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError>;

    /// Reads one bracketed coherent member view.
    async fn read_member_view(
        &mut self,
        request: MemberEffectRequest,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError>;

    /// Transfers primary to one exact native target.
    async fn transfer_primary(
        &mut self,
        input: PrimaryTransferInput,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError>;

    /// Terminates one exact backend connection and proves native absence.
    async fn terminate_backend_connection(
        &mut self,
        authority: &MemberMutationAuthority,
        connection: BackendConnectionId,
        attempt: &AttemptContext,
    ) -> Result<BackendConnectionReceipt, MemberTransitionError>;

    /// Contains only this exact locally owned member within the attempt's
    /// cleanup deadline.
    fn contain_member(
        &mut self,
        attempt: &AttemptContext,
    ) -> Result<AttemptCleanupState, MysqlInstanceError>;

    /// Contains the exact admitted local process and returns identity-bound
    /// terminal evidence rather than an unbound cleanup state.
    fn contain_member_exact(
        &mut self,
        authority: &MemberMutationAuthority,
        attempt: &AttemptContext,
    ) -> Result<MemberContainmentReceipt, MysqlInstanceError>;
}

fn validate_attempt(
    authority: &MemberMutationAuthority,
    evidence: &MemberNativeEvidence,
) -> Result<(), MemberTransitionError> {
    if authority.attempt() != evidence.authority().attempt()
        || authority.group_name() != evidence.view().group_name()
    {
        return Err(MemberTransitionError::Authority(
            MemberAuthorityError::AttemptMismatch,
        ));
    }
    Ok(())
}

fn validate_peers(view: &NativeView, peers: &[MemberPeer]) -> Result<(), MemberTransitionError> {
    for member in view.members() {
        let mut matching = peers.iter().filter(|peer| peer.member_id() == member.id());
        let peer = matching.next().ok_or(MemberTransitionError::View(
            MemberViewError::RequiredMemberMissing,
        ))?;
        if matching.next().is_some() {
            return Err(MemberTransitionError::View(
                MemberViewError::UnexpectedMember,
            ));
        }
        if member.address() != peer.address() {
            return Err(MemberTransitionError::View(
                MemberViewError::MemberAddressMismatch,
            ));
        }
    }
    if peers.len() != view.members().len()
        || peers
            .iter()
            .any(|peer| view.member(peer.member_id()).is_none())
    {
        return Err(MemberTransitionError::View(
            MemberViewError::UnexpectedMember,
        ));
    }
    Ok(())
}

fn validate_group_sources(
    group: &GroupName,
    executed: &GtidSet,
) -> Result<(), MemberTransitionError> {
    if executed
        .entries()
        .iter()
        .any(|entry| entry.source().server_uuid().as_str() != group.as_str())
    {
        Err(MemberTransitionError::View(MemberViewError::NonGroupSource))
    } else {
        Ok(())
    }
}

fn validate_receipt(
    kind: &MemberEffectKind,
    authority: &MemberMutationAuthority,
    predecessor: Option<&MemberNativeEvidence>,
    successor: &MemberNativeEvidence,
) -> Result<(), MemberTransitionError> {
    match kind {
        MemberEffectKind::Bootstrap => {
            if predecessor.is_some()
                || successor.view().members().len() != 1
                || successor.primary().id() != authority.local().member_id()
                || successor.primary().state() != MemberState::Online
            {
                return Err(MemberTransitionError::Receipt(
                    MemberReceiptError::SuccessorMismatch,
                ));
            }
        }
        MemberEffectKind::Join => {
            let predecessor = predecessor.ok_or(MemberTransitionError::Receipt(
                MemberReceiptError::PredecessorMismatch,
            ))?;
            CanonicalViewId::parse(predecessor.view().id())
                .and_then(|before| {
                    CanonicalViewId::parse(successor.view().id())
                        .and_then(|after| before.require_successor(after))
                })
                .map_err(MemberTransitionError::View)?;
            if successor.view().members().len() != predecessor.view().members().len() + 1
                || successor
                    .view()
                    .member(authority.local().member_id())
                    .is_none_or(|member| {
                        member.address() != authority.local().binding().member_address()
                            || member.role() != MemberRole::Secondary
                            || member.state() != MemberState::Online
                    })
            {
                return Err(MemberTransitionError::Receipt(
                    MemberReceiptError::SuccessorMismatch,
                ));
            }
            validate_predecessors(predecessor, successor, None)?;
        }
        MemberEffectKind::PrimaryTransfer { target } => {
            let predecessor = predecessor.ok_or(MemberTransitionError::Receipt(
                MemberReceiptError::PredecessorMismatch,
            ))?;
            if predecessor.view().id() != successor.view().id()
                || predecessor.view().members().len() != successor.view().members().len()
                || successor.view().member(target).is_none_or(|member| {
                    member.role() != MemberRole::Primary || member.state() != MemberState::Online
                })
                || successor
                    .view()
                    .member(authority.local().member_id())
                    .is_none_or(|member| {
                        member.role() != MemberRole::Secondary
                            || member.state() != MemberState::Online
                    })
            {
                return Err(MemberTransitionError::Receipt(
                    MemberReceiptError::SuccessorMismatch,
                ));
            }
            validate_predecessors(predecessor, successor, Some(target))?;
        }
        MemberEffectKind::ViewReadback => {
            if predecessor.is_some() {
                return Err(MemberTransitionError::Receipt(
                    MemberReceiptError::PredecessorMismatch,
                ));
            }
        }
    }
    if let Some(predecessor) = predecessor
        && !predecessor.executed().is_subset_of(successor.executed())
    {
        return Err(MemberTransitionError::View(
            MemberViewError::SourceBoundaryNotContained,
        ));
    }
    Ok(())
}

fn validate_predecessors(
    predecessor: &MemberNativeEvidence,
    successor: &MemberNativeEvidence,
    transferred_target: Option<&MemberId>,
) -> Result<(), MemberTransitionError> {
    for before in predecessor.view().members() {
        let after = successor
            .view()
            .member(before.id())
            .ok_or(MemberTransitionError::View(
                MemberViewError::RequiredMemberMissing,
            ))?;
        if after.address() != before.address() {
            return Err(MemberTransitionError::View(
                MemberViewError::MemberAddressMismatch,
            ));
        }
        if after.state() != MemberState::Online {
            return Err(MemberTransitionError::View(
                MemberViewError::MemberNotOnline,
            ));
        }
        let role_may_change = transferred_target.is_some_and(|target| {
            before.id() == target || before.id() == predecessor.primary().id()
        });
        if !role_may_change && after.role() != before.role() {
            return Err(MemberTransitionError::View(
                MemberViewError::MemberRoleMismatch,
            ));
        }
    }
    Ok(())
}
