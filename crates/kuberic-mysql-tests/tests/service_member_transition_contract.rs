#[path = "service_common/mod.rs"]
mod common;

use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::str::FromStr;
use std::time::{Duration, Instant};

use common::{GROUP_UUID, TestRoot, provide_socket};
use kuberic_mysql::core::{
    AttemptId, CredentialGeneration, EndpointBinding, GroupName, GtidSet, MemberAddress, MemberId,
    MemberRole, MemberState, NativeMember, NativeView, ProcessSessionId, ServerUuid,
    StorageBinding, ViewId,
};
use kuberic_mysql::service::{
    AccountProvisioningEvidence, AttemptAdmissionError, AttemptCancellationState,
    AttemptCleanupState, AttemptContext, AttemptDeadlineState, BackendConnectionId,
    BackendConnectionReceipt, BootstrapEffect, CanonicalViewId, ControlCredential,
    ControlCredentialRole, FreshBootstrapInput, FreshJoinInput, MemberAuthorityError,
    MemberControlBinding, MemberEffectKind, MemberEffectReceipt, MemberMutationAuthority,
    MemberNativeEvidence, MemberPeer, MemberReceiptError, MemberSourceBoundary,
    MemberTransitionError, MemberViewError, MysqlInstanceConfig, MysqlInstanceError,
    MysqlInstanceManager, MysqlInstanceState, MysqlMemberEffectBoundary, MysqlMemberIndex,
    ObservedLocalBinding, PairedAttemptOutcome, PrimaryTransferInput, TopologyAttempt,
    TopologyAuthority, TopologyInstant, TopologyObservation, TopologyObservationStatus,
    TransitionEvaluation,
};

const MEMBER_UUIDS: [&str; 3] = [
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    "dddddddd-dddd-dddd-dddd-dddddddddddd",
];
const MEMBERS: [MysqlMemberIndex; 3] = [
    MysqlMemberIndex::First,
    MysqlMemberIndex::Second,
    MysqlMemberIndex::Third,
];

#[test]
fn canonical_views_reject_malformed_stale_and_wrong_successors() {
    let first = CanonicalViewId::parse(&ViewId::new("700:1").unwrap()).unwrap();
    let second = CanonicalViewId::parse(&ViewId::new("700:2").unwrap()).unwrap();
    first.require_successor(second).unwrap();

    assert_eq!(
        CanonicalViewId::parse(&ViewId::new("0700:1").unwrap()),
        Err(MemberViewError::MalformedViewId)
    );
    assert_eq!(
        first.require_successor(CanonicalViewId::parse(&ViewId::new("701:2").unwrap()).unwrap()),
        Err(MemberViewError::ViewFixedPartChanged)
    );
    assert_eq!(
        first.require_successor(CanonicalViewId::parse(&ViewId::new("700:3").unwrap()).unwrap()),
        Err(MemberViewError::ViewSequenceNotSuccessor)
    );
    let maximum =
        CanonicalViewId::parse(&ViewId::new(format!("700:{}", u32::MAX)).unwrap()).unwrap();
    assert_eq!(
        maximum.require_successor(maximum),
        Err(MemberViewError::ViewSequenceOverflow)
    );
}

#[test]
fn mutation_authority_rejects_identity_and_member_substitution() {
    let enrollment = enrollment("attempt-a", MysqlMemberIndex::First);
    let error = MemberMutationAuthority::new(
        AttemptId::new("attempt-b").unwrap(),
        GroupName::new(GROUP_UUID).unwrap(),
        enrollment,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        MemberTransitionError::Authority(MemberAuthorityError::AttemptMismatch)
    ));

    let predecessor = evidence(
        authority("join-substitution", MysqlMemberIndex::First),
        "900:1",
        &[
            (MysqlMemberIndex::First, MemberRole::Primary),
            (MysqlMemberIndex::Second, MemberRole::Secondary),
        ],
    );
    let source = MemberSourceBoundary::new(
        peer("join-substitution", MysqlMemberIndex::First),
        predecessor,
    )
    .unwrap();
    let target = authority("join-substitution", MysqlMemberIndex::Second);
    let error = FreshJoinInput::new(
        target,
        source,
        vec![
            peer("join-substitution", MysqlMemberIndex::First),
            peer("join-substitution", MysqlMemberIndex::Second),
        ],
    )
    .unwrap_err();
    assert!(matches!(
        error,
        MemberTransitionError::Authority(MemberAuthorityError::AuthoritySubstitution)
    ));
}

#[test]
fn source_boundary_requires_evidence_from_the_exact_source_binding() {
    let source_authority = authority("source-binding", MysqlMemberIndex::First);
    let predecessor = evidence(
        source_authority,
        "905:1",
        &[
            (MysqlMemberIndex::First, MemberRole::Primary),
            (MysqlMemberIndex::Second, MemberRole::Secondary),
        ],
    );
    let error = MemberSourceBoundary::new(
        peer("source-binding", MysqlMemberIndex::Second),
        predecessor,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        MemberTransitionError::Authority(MemberAuthorityError::SourceMismatch)
    ));
}

#[test]
fn join_inputs_require_exact_unique_peer_set_equality() {
    let predecessor = evidence(
        authority("peer-set", MysqlMemberIndex::First),
        "906:1",
        &[
            (MysqlMemberIndex::First, MemberRole::Primary),
            (MysqlMemberIndex::Second, MemberRole::Secondary),
        ],
    );
    let source =
        MemberSourceBoundary::new(peer("peer-set", MysqlMemberIndex::First), predecessor).unwrap();
    let target = authority("peer-set", MysqlMemberIndex::Third);

    let duplicate = FreshJoinInput::new(
        target.clone(),
        source.clone(),
        vec![
            peer("peer-set", MysqlMemberIndex::First),
            peer("peer-set", MysqlMemberIndex::First),
        ],
    )
    .unwrap_err();
    assert!(matches!(
        duplicate,
        MemberTransitionError::View(MemberViewError::UnexpectedMember)
    ));

    let omitted = FreshJoinInput::new(
        target.clone(),
        source.clone(),
        vec![peer("peer-set", MysqlMemberIndex::First)],
    )
    .unwrap_err();
    assert!(matches!(
        omitted,
        MemberTransitionError::View(MemberViewError::RequiredMemberMissing)
    ));

    let mixed_binding = FreshJoinInput::new(
        target,
        source,
        vec![
            MemberPeer::new(
                member_id(MysqlMemberIndex::First),
                peer("peer-set", MysqlMemberIndex::Second).address().clone(),
            ),
            peer("peer-set", MysqlMemberIndex::Second),
        ],
    )
    .unwrap_err();
    assert!(matches!(
        mixed_binding,
        MemberTransitionError::View(MemberViewError::MemberAddressMismatch)
    ));
}

#[test]
fn transfer_input_preserves_exact_target_and_source_errors() {
    let source_authority = authority("transfer-input", MysqlMemberIndex::First);
    let predecessor = evidence(
        source_authority.clone(),
        "1000:4",
        &[
            (MysqlMemberIndex::First, MemberRole::Primary),
            (MysqlMemberIndex::Second, MemberRole::Secondary),
            (MysqlMemberIndex::Third, MemberRole::Secondary),
        ],
    );
    let boundary =
        MemberSourceBoundary::new(peer("transfer-input", MysqlMemberIndex::First), predecessor)
            .unwrap();

    let local_target = PrimaryTransferInput::new(
        source_authority.clone(),
        boundary.clone(),
        peer("transfer-input", MysqlMemberIndex::First),
    )
    .unwrap_err();
    assert!(matches!(
        local_target,
        MemberTransitionError::Authority(MemberAuthorityError::WrongTransferTarget)
    ));

    let absent_target = PrimaryTransferInput::new(
        source_authority,
        boundary,
        MemberPeer::new(
            MemberId::new("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap(),
            MemberAddress::new("127.0.0.1:33999").unwrap(),
        ),
    )
    .unwrap_err();
    assert!(matches!(
        absent_target,
        MemberTransitionError::Authority(MemberAuthorityError::WrongTransferTarget)
    ));
}

#[test]
fn topology_capabilities_narrow_to_member_inputs_without_peer_ownership() {
    let topology_attempt = TopologyAttempt::new(
        AttemptId::new("narrow").unwrap(),
        GroupName::new(GROUP_UUID).unwrap(),
        MysqlMemberIndex::First,
        Duration::from_secs(60),
    )
    .unwrap();
    let enrollments = [
        enrollment("narrow", MysqlMemberIndex::First),
        enrollment("narrow", MysqlMemberIndex::Second),
        enrollment("narrow", MysqlMemberIndex::Third),
    ];
    let mut topology = TopologyAuthority::new(topology_attempt.clone());
    for enrollment in &enrollments {
        topology.register_enrollment(enrollment.clone()).unwrap();
    }
    let bootstrap = topology
        .authorize_bootstrap(MysqlMemberIndex::First, TopologyInstant::new(1))
        .unwrap();
    let member_bootstrap = bootstrap.member_input().unwrap();
    assert_eq!(
        member_bootstrap.authority().local(),
        &enrollments[MysqlMemberIndex::First.as_usize_for_test()]
    );
    let bootstrap_effect = bootstrap
        .record_effect(BootstrapEffect::required_steps().to_vec())
        .unwrap()
        .bind_observation_attempt(AttemptId::new("narrow-bootstrap-observation").unwrap());
    let bootstrap_evidence = TopologyObservation::new(
        TopologyObservationStatus::Complete,
        AttemptId::new("narrow-bootstrap-observation").unwrap(),
        ObservedLocalBinding::from_enrollment(&enrollments[0]),
        topology_attempt.group_name().clone(),
        ViewId::new("1400:1").unwrap(),
        ViewId::new("1400:1").unwrap(),
        vec![NativeMember::new(
            enrollments[0].member_id().clone(),
            enrollments[0].binding().member_address().clone(),
            MemberRole::Primary,
            MemberState::Online,
        )],
        GtidSet::from_str(&format!("{GROUP_UUID}:1")).unwrap(),
        TopologyInstant::new(2),
    );
    let credit = match topology
        .accept_bootstrap(
            &bootstrap_effect,
            &bootstrap_evidence,
            TopologyInstant::new(3),
        )
        .unwrap()
    {
        TransitionEvaluation::Accepted(credit) => credit,
        TransitionEvaluation::Pending => panic!("bootstrap evidence must be accepted"),
    };
    let source_evidence = TopologyObservation::new(
        TopologyObservationStatus::Complete,
        AttemptId::new("narrow-source-observation").unwrap(),
        ObservedLocalBinding::from_enrollment(&enrollments[0]),
        topology_attempt.group_name().clone(),
        ViewId::new("1400:1").unwrap(),
        ViewId::new("1400:1").unwrap(),
        vec![NativeMember::new(
            enrollments[0].member_id().clone(),
            enrollments[0].binding().member_address().clone(),
            MemberRole::Primary,
            MemberState::Online,
        )],
        GtidSet::from_str(&format!("{GROUP_UUID}:1-2")).unwrap(),
        TopologyInstant::new(4),
    );
    let source = credit
        .source_gtid_boundary(
            &topology_attempt,
            &enrollments[0],
            &source_evidence,
            TopologyInstant::new(5),
        )
        .unwrap();
    let join = topology
        .authorize_join(MysqlMemberIndex::Second, source, TopologyInstant::new(6))
        .unwrap();
    let member_join = join.member_input().unwrap();
    assert_eq!(
        member_join.authority().local(),
        &enrollments[MysqlMemberIndex::Second.as_usize_for_test()]
    );
    assert_eq!(member_join.peers().len(), 1);
    assert_eq!(
        member_join.source().source().member_id(),
        enrollments[0].member_id()
    );
}

#[test]
fn receipts_bind_predecessor_successor_deadline_and_cleanup() {
    let attempt = context("receipt");
    let source_authority = authority("receipt", MysqlMemberIndex::First);
    let predecessor = evidence(
        source_authority.clone(),
        "1100:8",
        &[
            (MysqlMemberIndex::First, MemberRole::Primary),
            (MysqlMemberIndex::Second, MemberRole::Secondary),
            (MysqlMemberIndex::Third, MemberRole::Secondary),
        ],
    );
    let successor = evidence(
        source_authority.clone(),
        "1100:8",
        &[
            (MysqlMemberIndex::First, MemberRole::Secondary),
            (MysqlMemberIndex::Second, MemberRole::Primary),
            (MysqlMemberIndex::Third, MemberRole::Secondary),
        ],
    );
    let receipt = MemberEffectReceipt::new(
        MemberEffectKind::PrimaryTransfer {
            target: member_id(MysqlMemberIndex::Second),
        },
        source_authority.clone(),
        Some(predecessor.clone()),
        successor,
        source_authority.local().binding(),
        &attempt,
        AttemptCleanupState::NotRequired,
    )
    .unwrap();
    assert_eq!(receipt.authority(), &source_authority);
    assert_eq!(receipt.predecessor(), Some(&predecessor));
    assert_eq!(receipt.deadline_state(), AttemptDeadlineState::Work);
    assert_eq!(
        receipt.cancellation_state(),
        AttemptCancellationState::Active
    );
    assert_eq!(receipt.cleanup_state(), AttemptCleanupState::NotRequired);

    let changed_view = evidence(
        source_authority.clone(),
        "1100:9",
        &[
            (MysqlMemberIndex::First, MemberRole::Secondary),
            (MysqlMemberIndex::Second, MemberRole::Primary),
            (MysqlMemberIndex::Third, MemberRole::Secondary),
        ],
    );
    let error = MemberEffectReceipt::new(
        MemberEffectKind::PrimaryTransfer {
            target: member_id(MysqlMemberIndex::Second),
        },
        source_authority.clone(),
        Some(predecessor.clone()),
        changed_view,
        source_authority.local().binding(),
        &attempt,
        AttemptCleanupState::NotRequired,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        MemberTransitionError::Receipt(MemberReceiptError::SuccessorMismatch)
    ));

    let final_binding = source_authority.local().binding().clone();
    let cleanup_error = MemberEffectReceipt::new(
        MemberEffectKind::ViewReadback,
        source_authority,
        None,
        predecessor,
        &final_binding,
        &attempt,
        AttemptCleanupState::Failed,
    )
    .unwrap_err();
    assert!(matches!(
        cleanup_error,
        MemberTransitionError::Receipt(MemberReceiptError::CleanupIncomplete)
    ));
}

#[test]
fn receipts_reject_changed_or_replaced_final_owner_bindings() {
    let attempt = context("final-owner");
    let authority = authority("final-owner", MysqlMemberIndex::First);
    let successor = evidence(
        authority.clone(),
        "1150:1",
        &[(MysqlMemberIndex::First, MemberRole::Primary)],
    );
    let replaced_endpoint = MemberControlBinding::new(
        authority.attempt().clone(),
        MysqlMemberIndex::First,
        authority.local().binding().process_session().clone(),
        EndpointBinding::new("replacement-socket-object").unwrap(),
        authority.local().binding().storage().clone(),
        authority.local().binding().member_address().clone(),
        authority.local().binding().observer_generation().clone(),
        authority.local().binding().recovery_generation().clone(),
    );
    let effect_error = MemberEffectReceipt::new(
        MemberEffectKind::Bootstrap,
        authority.clone(),
        None,
        successor.clone(),
        &replaced_endpoint,
        &attempt,
        AttemptCleanupState::Completed,
    )
    .unwrap_err();
    assert!(matches!(
        effect_error,
        MemberTransitionError::Receipt(MemberReceiptError::BindingMismatch)
    ));

    let replaced_storage = MemberControlBinding::new(
        authority.attempt().clone(),
        MysqlMemberIndex::First,
        authority.local().binding().process_session().clone(),
        authority.local().binding().endpoint().clone(),
        StorageBinding::new("replacement-storage-root").unwrap(),
        authority.local().binding().member_address().clone(),
        authority.local().binding().observer_generation().clone(),
        authority.local().binding().recovery_generation().clone(),
    );
    let storage_error = MemberEffectReceipt::new(
        MemberEffectKind::Bootstrap,
        authority.clone(),
        None,
        successor,
        &replaced_storage,
        &attempt,
        AttemptCleanupState::Completed,
    )
    .unwrap_err();
    assert!(matches!(
        storage_error,
        MemberTransitionError::Receipt(MemberReceiptError::BindingMismatch)
    ));

    let changed_process = MemberControlBinding::new(
        authority.attempt().clone(),
        MysqlMemberIndex::First,
        ProcessSessionId::new("replacement-process-session").unwrap(),
        authority.local().binding().endpoint().clone(),
        authority.local().binding().storage().clone(),
        authority.local().binding().member_address().clone(),
        authority.local().binding().observer_generation().clone(),
        authority.local().binding().recovery_generation().clone(),
    );
    let connection_error = BackendConnectionReceipt::new(
        authority,
        BackendConnectionId::new(std::num::NonZeroU64::new(44).unwrap()),
        &changed_process,
        &attempt,
        AttemptCleanupState::Completed,
    )
    .unwrap_err();
    assert!(matches!(
        connection_error,
        MemberTransitionError::Receipt(MemberReceiptError::BindingMismatch)
    ));
}

#[test]
fn join_receipts_require_exact_canonical_successor_and_history() {
    let attempt = context("join-receipt");
    let source_authority = authority("join-receipt", MysqlMemberIndex::First);
    let target_authority = authority("join-receipt", MysqlMemberIndex::Second);
    let predecessor = evidence_with_gtid(
        source_authority,
        "1200:1",
        &[(MysqlMemberIndex::First, MemberRole::Primary)],
        "cccccccc-cccc-cccc-cccc-cccccccccccc:1-4",
    );
    let same_view = evidence_with_gtid(
        target_authority.clone(),
        "1200:1",
        &[
            (MysqlMemberIndex::First, MemberRole::Primary),
            (MysqlMemberIndex::Second, MemberRole::Secondary),
        ],
        "cccccccc-cccc-cccc-cccc-cccccccccccc:1-4",
    );
    let error = MemberEffectReceipt::new(
        MemberEffectKind::Join,
        target_authority.clone(),
        Some(predecessor.clone()),
        same_view,
        target_authority.local().binding(),
        &attempt,
        AttemptCleanupState::NotRequired,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        MemberTransitionError::View(MemberViewError::ViewSequenceNotSuccessor)
    ));

    let missing_history = evidence_with_gtid(
        target_authority.clone(),
        "1200:2",
        &[
            (MysqlMemberIndex::First, MemberRole::Primary),
            (MysqlMemberIndex::Second, MemberRole::Secondary),
        ],
        "cccccccc-cccc-cccc-cccc-cccccccccccc:1-3",
    );
    let error = MemberEffectReceipt::new(
        MemberEffectKind::Join,
        target_authority.clone(),
        Some(predecessor.clone()),
        missing_history,
        target_authority.local().binding(),
        &attempt,
        AttemptCleanupState::NotRequired,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        MemberTransitionError::View(MemberViewError::SourceBoundaryNotContained)
    ));

    let accepted = evidence_with_gtid(
        target_authority.clone(),
        "1200:2",
        &[
            (MysqlMemberIndex::First, MemberRole::Primary),
            (MysqlMemberIndex::Second, MemberRole::Secondary),
        ],
        "cccccccc-cccc-cccc-cccc-cccccccccccc:1-5",
    );
    MemberEffectReceipt::new(
        MemberEffectKind::Join,
        target_authority,
        Some(predecessor),
        accepted,
        &binding("join-receipt", MysqlMemberIndex::Second),
        &attempt,
        AttemptCleanupState::NotRequired,
    )
    .unwrap();
}

#[test]
fn attempt_context_separates_work_cleanup_expiry_and_cancellation() {
    let now = Instant::now();
    let context = AttemptContext::new_at(
        AttemptId::new("deadline").unwrap(),
        now,
        now + Duration::from_secs(30),
        Duration::from_secs(10),
    )
    .unwrap();
    assert_eq!(
        context.deadline_state_at(now + Duration::from_secs(19)),
        AttemptDeadlineState::Work
    );
    assert_eq!(
        context.deadline_state_at(now + Duration::from_secs(20)),
        AttemptDeadlineState::CleanupReserve
    );
    assert_eq!(
        context.deadline_state_at(now + Duration::from_secs(30)),
        AttemptDeadlineState::Expired
    );
    assert_eq!(context.cleanup_reserve(), Duration::from_secs(10));
    assert_eq!(context.admit_work(), Ok(()));
    let clone = context.clone();
    clone.cancel();
    assert_eq!(context.admit_work(), Err(AttemptAdmissionError::Cancelled));
    assert_eq!(context.admit_cleanup(), Ok(()));
    assert_eq!(
        context.cancellation_state(),
        AttemptCancellationState::Cancelled
    );

    assert_eq!(
        AttemptContext::new_at(AttemptId::new("expired").unwrap(), now, now, Duration::ZERO,)
            .unwrap_err(),
        AttemptAdmissionError::Expired
    );
    assert_eq!(
        AttemptContext::new_at(
            AttemptId::new("no-work").unwrap(),
            now,
            now + Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .unwrap_err(),
        AttemptAdmissionError::CleanupReserve
    );
    assert_eq!(
        AttemptContext::new_at(
            AttemptId::new("no-cleanup").unwrap(),
            now,
            now + Duration::from_secs(5),
            Duration::ZERO,
        )
        .unwrap_err(),
        AttemptAdmissionError::CleanupReserve
    );
}

#[test]
fn paired_outcome_and_connection_receipt_retain_cleanup_evidence() {
    let paired = PairedAttemptOutcome::<(), &'static str>::new(Err("primary"), Err("cleanup"));
    assert_eq!(paired.primary(), &Err("primary"));
    assert_eq!(paired.cleanup(), &Err("cleanup"));
    assert_eq!(paired.into_parts(), (Err("primary"), Err("cleanup")));

    let attempt = context("connection");
    let authority = authority("connection", MysqlMemberIndex::Third);
    let receipt = BackendConnectionReceipt::new(
        authority.clone(),
        BackendConnectionId::new(std::num::NonZeroU64::new(42).unwrap()),
        authority.local().binding(),
        &attempt,
        AttemptCleanupState::Completed,
    )
    .unwrap();
    assert_eq!(receipt.authority(), &authority);
    assert_eq!(receipt.connection().get(), 42);
    assert_eq!(receipt.deadline_state(), AttemptDeadlineState::Work);
    assert_eq!(receipt.cleanup_state(), AttemptCleanupState::Completed);

    attempt.cancel();
    let final_binding = authority.local().binding().clone();
    let error = BackendConnectionReceipt::new(
        authority,
        BackendConnectionId::new(std::num::NonZeroU64::new(43).unwrap()),
        &final_binding,
        &attempt,
        AttemptCleanupState::Completed,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        MemberTransitionError::Receipt(MemberReceiptError::Cancelled)
    ));
}

#[tokio::test]
async fn cancellation_containment_touches_exactly_one_member_owner() {
    let roots = [
        TestRoot::new("member-a"),
        TestRoot::new("member-b"),
        TestRoot::new("member-c"),
    ];
    let mut members = [
        ScriptMember::new(roots[0].config_for_member(MysqlMemberIndex::First)),
        ScriptMember::new(roots[1].config_for_member(MysqlMemberIndex::Second)),
        ScriptMember::new(roots[2].config_for_member(MysqlMemberIndex::Third)),
    ];
    let attempt = context("contain-one");
    let source_authority = authority("contain-one", MysqlMemberIndex::First);
    let predecessor = evidence(
        source_authority.clone(),
        "1300:7",
        &[
            (MysqlMemberIndex::First, MemberRole::Primary),
            (MysqlMemberIndex::Second, MemberRole::Secondary),
            (MysqlMemberIndex::Third, MemberRole::Secondary),
        ],
    );
    let input = PrimaryTransferInput::new(
        source_authority,
        MemberSourceBoundary::new(peer("contain-one", MysqlMemberIndex::First), predecessor)
            .unwrap(),
        peer("contain-one", MysqlMemberIndex::Second),
    )
    .unwrap();
    attempt.cancel();
    let error = members[0]
        .transfer_primary(input, &attempt)
        .await
        .unwrap_err();
    assert!(matches!(error, MemberTransitionError::Cancelled));
    assert_eq!(members[0].containments, 1);
    assert_eq!(members[1].containments, 0);
    assert_eq!(members[2].containments, 0);
}

#[tokio::test]
async fn real_manager_already_cancelled_admission_contains_only_exact_local_owner() {
    let roots = [
        TestRoot::new("cancel-real-a"),
        TestRoot::new("cancel-real-b"),
        TestRoot::new("cancel-real-c"),
    ];
    let mut members = std::array::from_fn(|index| {
        MysqlInstanceManager::new(roots[index].config_for_member(MEMBERS[index]))
    });
    let sockets = start_real_members(&mut members);
    let attempt = context("cancel-real");
    let authorities = members
        .each_ref()
        .map(|member| running_authority("cancel-real", member));
    attempt.cancel();

    let invalid = members[0]
        .terminate_backend_connection(
            &authorities[1],
            BackendConnectionId::new(std::num::NonZeroU64::new(1).unwrap()),
            &attempt,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        invalid,
        MemberTransitionError::Authority(MemberAuthorityError::LocalBindingMismatch)
    ));
    assert!(
        members
            .iter()
            .all(|member| member.state() == MysqlInstanceState::Running)
    );

    let cancelled = members[0]
        .terminate_backend_connection(
            &authorities[0],
            BackendConnectionId::new(std::num::NonZeroU64::new(1).unwrap()),
            &attempt,
        )
        .await
        .unwrap_err();
    assert!(matches!(cancelled, MemberTransitionError::Cancelled));
    assert_eq!(members[0].state(), MysqlInstanceState::Stopped);
    assert!(!roots[0].scratch.exists());
    assert!(!members[0].config().runtime().socket().exists());
    for peer in &members[1..] {
        assert_eq!(peer.state(), MysqlInstanceState::Running);
        assert!(peer.config().runtime().socket().exists());
    }

    members[1].stop().unwrap();
    members[2].stop().unwrap();
    for socket in sockets {
        socket.join().unwrap();
    }
}

#[tokio::test]
async fn real_manager_future_cancellation_contains_only_the_inflight_owner() {
    let roots = [
        TestRoot::new("drop-real-a"),
        TestRoot::new("drop-real-b"),
        TestRoot::new("drop-real-c"),
    ];
    let mut members = std::array::from_fn(|index| {
        MysqlInstanceManager::new(roots[index].config_for_member(MEMBERS[index]))
    });
    let sockets = start_real_members(&mut members);
    let attempt = context("drop-real");
    let authority = running_authority("drop-real", &members[0]);

    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            members[0].terminate_backend_connection(
                &authority,
                BackendConnectionId::new(std::num::NonZeroU64::new(1).unwrap()),
                &attempt,
            ),
        )
        .await
        .is_err()
    );
    assert_eq!(members[0].state(), MysqlInstanceState::Stopped);
    assert!(!roots[0].scratch.exists());
    assert!(!members[0].config().runtime().socket().exists());
    for peer in &members[1..] {
        assert_eq!(peer.state(), MysqlInstanceState::Running);
        assert!(peer.config().runtime().socket().exists());
    }

    members[1].stop().unwrap();
    members[2].stop().unwrap();
    for socket in sockets {
        socket.join().unwrap();
    }
}

#[tokio::test]
async fn real_manager_cancelled_admission_pairs_primary_and_cleanup_failure() {
    let roots = [
        TestRoot::new("pair-real-a"),
        TestRoot::new("pair-real-b"),
        TestRoot::new("pair-real-c"),
    ];
    let mut members = std::array::from_fn(|index| {
        MysqlInstanceManager::new(roots[index].config_for_member(MEMBERS[index]))
    });
    let sockets = start_real_members(&mut members);
    let attempt = context("pair-real");
    let authority = running_authority("pair-real", &members[0]);
    attempt.cancel();
    fs::set_permissions(&roots[0].root, fs::Permissions::from_mode(0o500)).unwrap();

    let error = members[0]
        .terminate_backend_connection(
            &authority,
            BackendConnectionId::new(std::num::NonZeroU64::new(1).unwrap()),
            &attempt,
        )
        .await
        .unwrap_err();
    fs::set_permissions(&roots[0].root, fs::Permissions::from_mode(0o700)).unwrap();
    let MemberTransitionError::Cleanup { primary, cleanup } = error else {
        panic!("cancelled admission must retain its paired cleanup failure");
    };
    assert!(matches!(*primary, MemberTransitionError::Cancelled));
    assert!(matches!(
        *cleanup,
        MysqlInstanceError::Io {
            operation: kuberic_mysql::service::LifecycleOperation::ScratchCleanup,
            ..
        }
    ));
    assert!(!members[0].config().runtime().socket().exists());
    for peer in &members[1..] {
        assert_eq!(peer.state(), MysqlInstanceState::Running);
        assert!(peer.config().runtime().socket().exists());
    }

    let cleanup_attempt = context("pair-real-cleanup");
    members[0].contain_member(&cleanup_attempt).unwrap();
    members[1].stop().unwrap();
    members[2].stop().unwrap();
    for socket in sockets {
        socket.join().unwrap();
    }
}

struct ScriptMember {
    config: MysqlInstanceConfig,
    containments: usize,
}

impl ScriptMember {
    fn new(config: MysqlInstanceConfig) -> Self {
        Self {
            config,
            containments: 0,
        }
    }
}

impl MysqlMemberEffectBoundary for ScriptMember {
    fn config(&self) -> &MysqlInstanceConfig {
        &self.config
    }

    async fn bootstrap_member(
        &mut self,
        _input: FreshBootstrapInput,
        _recovery: &ControlCredential,
        _attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError> {
        unreachable!("not used by this contract")
    }

    async fn join_member(
        &mut self,
        _input: FreshJoinInput,
        _recovery: &ControlCredential,
        _attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError> {
        unreachable!("not used by this contract")
    }

    async fn read_member_view(
        &mut self,
        _authority: &MemberMutationAuthority,
        _attempt: &AttemptContext,
    ) -> Result<MemberNativeEvidence, MemberTransitionError> {
        unreachable!("not used by this contract")
    }

    async fn transfer_primary(
        &mut self,
        _input: PrimaryTransferInput,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError> {
        if attempt.cancellation_state() == AttemptCancellationState::Cancelled {
            self.contain_member(attempt).unwrap();
            Err(MemberTransitionError::Cancelled)
        } else {
            unreachable!("this contract always cancels")
        }
    }

    async fn terminate_backend_connection(
        &mut self,
        _authority: &MemberMutationAuthority,
        _connection: BackendConnectionId,
        _attempt: &AttemptContext,
    ) -> Result<BackendConnectionReceipt, MemberTransitionError> {
        unreachable!("not used by this contract")
    }

    fn contain_member(
        &mut self,
        _attempt: &AttemptContext,
    ) -> Result<AttemptCleanupState, MysqlInstanceError> {
        self.containments += 1;
        Ok(AttemptCleanupState::Completed)
    }
}

fn start_real_members(members: &mut [MysqlInstanceManager; 3]) -> [std::thread::JoinHandle<()>; 3] {
    std::array::from_fn(|index| {
        members[index].initialize().unwrap();
        let socket = provide_socket(
            members[index].config().runtime().pid().to_owned(),
            members[index].config().runtime().socket().to_owned(),
        );
        members[index].start().unwrap();
        socket
    })
}

fn running_authority(label: &str, manager: &MysqlInstanceManager) -> MemberMutationAuthority {
    let config = manager.config();
    let member = config.member_index();
    let attempt = AttemptId::new(label).unwrap();
    let scratch = fs::metadata(config.runtime().scratch_root()).unwrap();
    let data = fs::metadata(config.data_root()).unwrap();
    let pid = fs::read_to_string(config.runtime().pid())
        .unwrap()
        .trim()
        .parse::<u32>()
        .unwrap();
    let binding = MemberControlBinding::new(
        attempt.clone(),
        member,
        ProcessSessionId::new(format!(
            "member-{}-generation-1-pid-{pid}-scratch-{}-{}",
            member.as_usize_for_test() + 1,
            scratch.dev(),
            scratch.ino()
        ))
        .unwrap(),
        EndpointBinding::new(format!(
            "uds:{}:{}:{}",
            scratch.dev(),
            scratch.ino(),
            config.runtime().socket().display()
        ))
        .unwrap(),
        StorageBinding::new(format!("data:{}:{}", data.dev(), data.ino())).unwrap(),
        MemberAddress::new(config.member().sql_address().to_string()).unwrap(),
        CredentialGeneration::new("observer-generation").unwrap(),
        CredentialGeneration::new("recovery-generation").unwrap(),
    );
    let observer = ControlCredential::new(
        attempt.clone(),
        CredentialGeneration::new("observer-generation").unwrap(),
        ControlCredentialRole::Observer,
        "observer_local",
        "observer-secret",
    )
    .unwrap();
    let recovery = ControlCredential::new(
        attempt.clone(),
        CredentialGeneration::new("recovery-generation").unwrap(),
        ControlCredentialRole::Recovery,
        "recovery_local",
        "recovery-secret",
    )
    .unwrap();
    let accounts = AccountProvisioningEvidence::new(
        binding.clone(),
        &observer,
        &recovery,
        AccountProvisioningEvidence::required_steps().to_vec(),
    )
    .unwrap();
    let enrollment = kuberic_mysql::service::NativeIdentityEnrollment::new(
        binding,
        ServerUuid::new(MEMBER_UUIDS[member.as_usize_for_test()]).unwrap(),
        &accounts,
        true,
        GtidSet::empty(),
        kuberic_mysql::service::NativeIdentityEnrollment::required_steps().to_vec(),
    )
    .unwrap();
    MemberMutationAuthority::new(attempt, GroupName::new(GROUP_UUID).unwrap(), enrollment).unwrap()
}

fn context(label: &str) -> AttemptContext {
    AttemptContext::new(
        AttemptId::new(label).unwrap(),
        Instant::now() + Duration::from_secs(60),
        Duration::from_secs(10),
    )
    .unwrap()
}

fn authority(label: &str, member: MysqlMemberIndex) -> MemberMutationAuthority {
    MemberMutationAuthority::new(
        AttemptId::new(label).unwrap(),
        GroupName::new(GROUP_UUID).unwrap(),
        enrollment(label, member),
    )
    .unwrap()
}

fn enrollment(
    label: &str,
    member: MysqlMemberIndex,
) -> kuberic_mysql::service::NativeIdentityEnrollment {
    let attempt = AttemptId::new(label).unwrap();
    let binding = binding(label, member);
    let observer = ControlCredential::new(
        attempt.clone(),
        CredentialGeneration::new("observer-generation").unwrap(),
        ControlCredentialRole::Observer,
        "observer_local",
        "observer-secret",
    )
    .unwrap();
    let recovery = ControlCredential::new(
        attempt,
        CredentialGeneration::new("recovery-generation").unwrap(),
        ControlCredentialRole::Recovery,
        "recovery_local",
        "recovery-secret",
    )
    .unwrap();
    let accounts = AccountProvisioningEvidence::new(
        binding.clone(),
        &observer,
        &recovery,
        AccountProvisioningEvidence::required_steps().to_vec(),
    )
    .unwrap();
    kuberic_mysql::service::NativeIdentityEnrollment::new(
        binding,
        ServerUuid::new(MEMBER_UUIDS[member.as_usize_for_test()]).unwrap(),
        &accounts,
        true,
        GtidSet::empty(),
        kuberic_mysql::service::NativeIdentityEnrollment::required_steps().to_vec(),
    )
    .unwrap()
}

fn binding(label: &str, member: MysqlMemberIndex) -> MemberControlBinding {
    let number = member.as_usize_for_test() + 1;
    MemberControlBinding::new(
        AttemptId::new(label).unwrap(),
        member,
        ProcessSessionId::new(format!("process-{label}-{number}")).unwrap(),
        EndpointBinding::new(format!("endpoint-{label}-{number}")).unwrap(),
        StorageBinding::new(format!("storage-{label}-{number}")).unwrap(),
        MemberAddress::new(format!("127.0.0.1:3306{number}")).unwrap(),
        CredentialGeneration::new("observer-generation").unwrap(),
        CredentialGeneration::new("recovery-generation").unwrap(),
    )
}

fn member_id(member: MysqlMemberIndex) -> MemberId {
    MemberId::new(MEMBER_UUIDS[member.as_usize_for_test()]).unwrap()
}

fn peer(label: &str, member: MysqlMemberIndex) -> MemberPeer {
    MemberPeer::from_enrollment(&enrollment(label, member))
}

fn evidence(
    authority: MemberMutationAuthority,
    view_id: &str,
    members: &[(MysqlMemberIndex, MemberRole)],
) -> MemberNativeEvidence {
    evidence_with_gtid(authority, view_id, members, "")
}

fn evidence_with_gtid(
    authority: MemberMutationAuthority,
    view_id: &str,
    members: &[(MysqlMemberIndex, MemberRole)],
    executed: &str,
) -> MemberNativeEvidence {
    let native_members = members
        .iter()
        .map(|(member, role)| {
            NativeMember::new(
                member_id(*member),
                MemberAddress::new(format!("127.0.0.1:3306{}", member.as_usize_for_test() + 1))
                    .unwrap(),
                *role,
                MemberState::Online,
            )
        })
        .collect();
    MemberNativeEvidence::new(
        authority.clone(),
        NativeView::new(
            authority.group_name().clone(),
            ViewId::new(view_id).unwrap(),
            native_members,
        )
        .unwrap(),
        GtidSet::from_str(executed).unwrap(),
    )
    .unwrap()
}

trait MemberIndexTestExt {
    fn as_usize_for_test(self) -> usize;
}

impl MemberIndexTestExt for MysqlMemberIndex {
    fn as_usize_for_test(self) -> usize {
        match self {
            MysqlMemberIndex::First => 0,
            MysqlMemberIndex::Second => 1,
            MysqlMemberIndex::Third => 2,
        }
    }
}
