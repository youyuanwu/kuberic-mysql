//! Member-scoped native mutation inputs, evidence, receipts, and boundary.

use std::num::NonZeroU64;

use crate::core::{
    AttemptId, GroupName, GtidSet, MemberAddress, MemberId, MemberRole, MemberState, NativeMember,
    NativeView, ViewId,
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

/// Fresh one-member bootstrap input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FreshBootstrapInput {
    authority: MemberMutationAuthority,
}

impl FreshBootstrapInput {
    /// Creates member-scoped bootstrap input.
    #[must_use]
    pub const fn new(authority: MemberMutationAuthority) -> Self {
        Self { authority }
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
            source,
            peers,
        })
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
            source,
            target,
        })
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
    authority: MemberMutationAuthority,
    predecessor: Option<MemberNativeEvidence>,
    successor: MemberNativeEvidence,
    deadline: AttemptDeadlineState,
    cancellation: AttemptCancellationState,
    cleanup: AttemptCleanupState,
}

impl MemberEffectReceipt {
    /// Validates and creates an exact member effect receipt.
    pub fn new(
        kind: MemberEffectKind,
        authority: MemberMutationAuthority,
        predecessor: Option<MemberNativeEvidence>,
        successor: MemberNativeEvidence,
        attempt: &AttemptContext,
        cleanup: AttemptCleanupState,
    ) -> Result<Self, MemberTransitionError> {
        let deadline = attempt.deadline_state();
        let cancellation = attempt.cancellation_state();
        if attempt.attempt() != authority.attempt()
            || successor.authority() != &authority
            || predecessor
                .as_ref()
                .is_some_and(|evidence| evidence.authority().attempt() != authority.attempt())
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
        validate_receipt(&kind, &authority, predecessor.as_ref(), &successor)?;
        Ok(Self {
            kind,
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
        attempt: &AttemptContext,
        cleanup: AttemptCleanupState,
    ) -> Result<Self, MemberTransitionError> {
        let deadline = attempt.deadline_state();
        let cancellation = attempt.cancellation_state();
        if attempt.attempt() != authority.attempt() {
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
        authority: &MemberMutationAuthority,
        attempt: &AttemptContext,
    ) -> Result<MemberNativeEvidence, MemberTransitionError>;

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

    /// Contains only this exact locally owned member.
    fn contain_member(&mut self) -> Result<(), MysqlInstanceError>;
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
    if peers.len() != view.members().len() {
        return Err(MemberTransitionError::View(
            MemberViewError::UnexpectedMember,
        ));
    }
    for peer in peers {
        let member = view
            .member(peer.member_id())
            .ok_or(MemberTransitionError::View(
                MemberViewError::RequiredMemberMissing,
            ))?;
        if member.address() != peer.address() {
            return Err(MemberTransitionError::View(
                MemberViewError::MemberAddressMismatch,
            ));
        }
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
