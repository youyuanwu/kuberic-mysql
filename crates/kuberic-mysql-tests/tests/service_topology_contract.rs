use std::str::FromStr;

use kuberic_mysql::core::{
    AttemptId, CredentialGeneration, EndpointBinding, GroupName, GtidSet, MemberAddress, MemberId,
    MemberRole, MemberState, NativeMember, ProcessSessionId, ServerUuid, StorageBinding, ViewId,
};
use kuberic_mysql::service::{
    AccountProvisioningEvidence, BootstrapEffect, ControlCredential, ControlCredentialRole,
    ControlStage, ControlStep, JoinEffect, MemberControlBinding, MysqlMemberIndex,
    MysqlTopologyError, NativeIdentityEnrollment, ObservedLocalBinding, SourceGtidBoundary,
    TopologyAttempt, TopologyAuthority, TopologyAuthorityError, TopologyEvidenceError,
    TopologyGtidError, TopologyInstant, TopologyNativeStateError, TopologyObservation,
    TopologyObservationStatus, TopologyStateError, TransitionCredit, TransitionEvaluation,
    ViewDiscovery,
};

const GROUP_UUID: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
const MEMBER_UUIDS: [&str; 3] = [
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    "dddddddd-dddd-dddd-dddd-dddddddddddd",
];

#[test]
fn control_contract_uses_exact_minimum_accounts_and_restores_binary_logging() {
    assert_eq!(
        AccountProvisioningEvidence::required_steps(),
        [
            ControlStep::ValidateProduct,
            ControlStep::ReadBinaryLogging,
            ControlStep::DisableBinaryLogging,
            ControlStep::CreateObserver,
            ControlStep::GrantObserverMembers,
            ControlStep::GrantObserverStats,
            ControlStep::CreateRecovery,
            ControlStep::GrantRecovery,
            ControlStep::RestoreBinaryLogging,
            ControlStep::ProveBinaryLoggingRestored,
        ]
    );
    assert_eq!(
        ControlStep::GrantObserverMembers.sql_contract(),
        Some(
            "GRANT SELECT ON performance_schema.replication_group_members \
             TO '<observer>'@'localhost'"
        )
    );
    assert_eq!(
        ControlStep::GrantObserverStats.sql_contract(),
        Some(
            "GRANT SELECT ON performance_schema.replication_group_member_stats \
             TO '<observer>'@'localhost'"
        )
    );
    assert_eq!(
        ControlStep::GrantRecovery.sql_contract(),
        Some(
            "GRANT REPLICATION SLAVE, CONNECTION_ADMIN ON *.* \
             TO '<recovery>'@'localhost'"
        )
    );
    assert!(
        ControlStep::StartGroupReplication
            .sql_contract()
            .unwrap()
            .contains("PASSWORD='<redacted>'")
    );
    for forbidden in [
        "ALL PRIVILEGES",
        "GRANT OPTION",
        "CLONE_ADMIN",
        "BACKUP_ADMIN",
    ] {
        assert!(
            AccountProvisioningEvidence::required_steps()
                .iter()
                .filter_map(|step| step.sql_contract())
                .all(|sql| !sql.contains(forbidden))
        );
    }
    assert_eq!(
        BootstrapEffect::required_steps(),
        [
            ControlStep::ValidateProduct,
            ControlStep::EnableBootstrap,
            ControlStep::StartGroupReplication,
            ControlStep::DisableBootstrap,
            ControlStep::ProveBootstrapDisabled,
        ]
    );
    assert_eq!(
        JoinEffect::required_steps(),
        [
            ControlStep::ValidateProduct,
            ControlStep::ProveBootstrapDisabled,
            ControlStep::StartGroupReplication,
        ]
    );
}

#[test]
fn account_provisioning_is_attempt_and_generation_bound_on_every_member() {
    let attempt = attempt("topology-attempt", 100);
    let mut evidence = Vec::new();
    for member in MysqlMemberIndex::all_for_test() {
        let binding = binding(&attempt, member);
        let (observer, recovery) = credentials(&attempt);
        evidence.push(
            AccountProvisioningEvidence::new(
                binding,
                &observer,
                &recovery,
                AccountProvisioningEvidence::required_steps().to_vec(),
            )
            .unwrap(),
        );
        assert_eq!(observer.username(), "observer_local");
        assert_eq!(recovery.username(), "recovery_local");
    }
    assert_eq!(evidence.len(), 3);

    let attempt_binding = binding(&attempt, MysqlMemberIndex::First);
    let wrong = ControlCredential::new(
        AttemptId::new("another-attempt").unwrap(),
        attempt_binding.observer_generation().clone(),
        ControlCredentialRole::Observer,
        "observer_local",
        "observer-secret",
    )
    .unwrap();
    let (_, recovery) = credentials(&attempt);
    assert_eq!(
        AccountProvisioningEvidence::new(
            attempt_binding,
            &wrong,
            &recovery,
            AccountProvisioningEvidence::required_steps().to_vec(),
        ),
        Err(MysqlTopologyError::Authority(
            TopologyAuthorityError::AttemptMismatch
        ))
    );
    let wrong_generation = ControlCredential::new(
        attempt.id().clone(),
        CredentialGeneration::new("stale-observer-generation").unwrap(),
        ControlCredentialRole::Observer,
        "observer_local",
        "observer-secret",
    )
    .unwrap();
    let stale_binding = binding(&attempt, MysqlMemberIndex::First);
    let (_, recovery) = credentials(&attempt);
    assert_eq!(
        AccountProvisioningEvidence::new(
            stale_binding,
            &wrong_generation,
            &recovery,
            AccountProvisioningEvidence::required_steps().to_vec(),
        ),
        Err(MysqlTopologyError::Authority(
            TopologyAuthorityError::CredentialGenerationMismatch
        ))
    );

    let mut authority = TopologyAuthority::new(attempt.clone());
    authority
        .register_enrollment(enrollment(&attempt, MysqlMemberIndex::First))
        .unwrap();
    let second_binding = binding(&attempt, MysqlMemberIndex::Second);
    let observer = ControlCredential::new(
        attempt.id().clone(),
        second_binding.observer_generation().clone(),
        ControlCredentialRole::Observer,
        "different_observer",
        "observer-secret",
    )
    .unwrap();
    let recovery = ControlCredential::new(
        attempt.id().clone(),
        second_binding.recovery_generation().clone(),
        ControlCredentialRole::Recovery,
        "recovery_local",
        "recovery-secret",
    )
    .unwrap();
    let second_accounts = AccountProvisioningEvidence::new(
        second_binding.clone(),
        &observer,
        &recovery,
        AccountProvisioningEvidence::required_steps().to_vec(),
    )
    .unwrap();
    let second = NativeIdentityEnrollment::new(
        second_binding,
        ServerUuid::new(MEMBER_UUIDS[1]).unwrap(),
        &second_accounts,
        true,
        NativeIdentityEnrollment::required_steps().to_vec(),
    )
    .unwrap();
    assert_eq!(
        authority.register_enrollment(second),
        Err(MysqlTopologyError::Authority(
            TopologyAuthorityError::PrincipalMismatch
        ))
    );
}

#[test]
fn credentials_redact_and_support_early_zeroing() {
    let attempt = attempt("credential-attempt", 100);
    let mut credential = ControlCredential::new(
        attempt.id().clone(),
        CredentialGeneration::new("observer-generation").unwrap(),
        ControlCredentialRole::Observer,
        "observer_local",
        "do-not-print-this",
    )
    .unwrap();
    let debug = format!("{credential:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("do-not-print-this"));
    credential.clear();
    assert!(credential.is_cleared());
}

#[test]
fn enrollment_rejects_existing_group_state_and_requires_complete_setup_trace() {
    let attempt = attempt("enrollment-attempt", 100);
    let binding = binding(&attempt, MysqlMemberIndex::First);
    let accounts = accounts(&attempt, MysqlMemberIndex::First);
    assert_eq!(
        NativeIdentityEnrollment::new(
            binding.clone(),
            ServerUuid::new(MEMBER_UUIDS[0]).unwrap(),
            &accounts,
            false,
            NativeIdentityEnrollment::required_steps().to_vec(),
        ),
        Err(MysqlTopologyError::TopologyState(
            TopologyStateError::ExistingGroupState
        ))
    );
    assert_eq!(
        NativeIdentityEnrollment::new(
            binding,
            ServerUuid::new(MEMBER_UUIDS[0]).unwrap(),
            &accounts,
            true,
            vec![ControlStep::EnrollServerIdentity],
        ),
        Err(MysqlTopologyError::TopologyState(
            TopologyStateError::InvalidTransition
        ))
    );
}

#[test]
fn designated_bootstrap_is_single_use_and_bootstrap_off_is_required() {
    let (mut authority, _) = authority();
    let capability = authority
        .authorize_bootstrap(MysqlMemberIndex::First, TopologyInstant::new(10))
        .unwrap();
    assert_eq!(
        authority.authorize_bootstrap(MysqlMemberIndex::First, TopologyInstant::new(11)),
        Err(MysqlTopologyError::Authority(
            TopologyAuthorityError::BootstrapAlreadyUsed
        ))
    );
    assert_eq!(
        capability.record_effect(vec![
            ControlStep::ValidateProduct,
            ControlStep::EnableBootstrap,
            ControlStep::StartGroupReplication,
        ]),
        Err(MysqlTopologyError::TopologyState(
            TopologyStateError::InvalidTransition
        ))
    );
    assert_closed_no_credit(&authority, 0);

    let (mut authority, enrollments, effect) = bootstrap_in_flight();
    let evidence = observation(
        &attempt("topology-attempt", 100),
        &enrollments[0],
        "bootstrap-observation",
        "view-1",
        "view-1",
        vec![native(
            &enrollments[0],
            MemberRole::Primary,
            MemberState::Online,
        )],
        group_gtids("1"),
        TopologyObservationStatus::Complete,
        TopologyInstant::new(20),
    );
    assert!(matches!(
        authority
            .accept_bootstrap(&effect, &evidence, TopologyInstant::new(21))
            .unwrap(),
        TransitionEvaluation::Accepted(_)
    ));
    assert_closed_no_credit(&authority, 1);
    assert_eq!(
        authority.accept_bootstrap(&effect, &evidence, TopologyInstant::new(22)),
        Err(MysqlTopologyError::Authority(
            TopologyAuthorityError::StaleCapability
        ))
    );
}

#[test]
fn non_designated_bootstrap_and_unenrolled_members_execute_no_effect() {
    let attempt = attempt("topology-attempt", 100);
    let mut session = TopologyAuthority::new(attempt.clone());
    session
        .register_enrollment(enrollment(&attempt, MysqlMemberIndex::First))
        .unwrap();
    let mut effects = 0;
    if session
        .authorize_bootstrap(MysqlMemberIndex::Second, TopologyInstant::new(10))
        .is_ok()
    {
        effects += 1;
    }
    assert_eq!(effects, 0);
    assert_eq!(
        session.authorize_bootstrap(MysqlMemberIndex::Second, TopologyInstant::new(10)),
        Err(MysqlTopologyError::Authority(
            TopologyAuthorityError::WrongBootstrapMember
        ))
    );
    assert_closed_no_credit(&session, 0);

    let (mut authority, _) = authority();
    let mut effects = 0;
    if authority
        .authorize_bootstrap(MysqlMemberIndex::First, TopologyInstant::new(100))
        .is_ok()
    {
        effects += 1;
    }
    assert_eq!(effects, 0);
    assert_closed_no_credit(&authority, 0);
}

#[test]
fn sequential_joins_treat_recovering_as_pending_and_online_as_credit() {
    let (mut authority, enrollments, bootstrap_credit) = accepted_bootstrap();
    let source_boundary = boundary(&bootstrap_credit, &enrollments[0], group_gtids("1-5"));
    let join = authority
        .authorize_join(
            MysqlMemberIndex::Second,
            source_boundary,
            TopologyInstant::new(30),
        )
        .unwrap();
    let effect = join
        .record_effect(JoinEffect::required_steps().to_vec())
        .unwrap()
        .bind_observation_attempt(AttemptId::new("join-two-observation").unwrap());
    let recovering = observation(
        &attempt("topology-attempt", 100),
        &enrollments[1],
        "join-two-observation",
        "view-2",
        "view-2",
        vec![
            native(&enrollments[0], MemberRole::Primary, MemberState::Online),
            native(
                &enrollments[1],
                MemberRole::Secondary,
                MemberState::Recovering,
            ),
        ],
        group_gtids("1-5"),
        TopologyObservationStatus::Complete,
        TopologyInstant::new(40),
    );
    assert_eq!(
        authority
            .accept_join(&effect, &recovering, TopologyInstant::new(41))
            .unwrap(),
        TransitionEvaluation::Pending
    );
    assert_closed_no_credit(&authority, 1);
    assert!(matches!(
        authority.authorize_join(
            MysqlMemberIndex::Third,
            boundary(&bootstrap_credit, &enrollments[0], group_gtids("1-5")),
            TopologyInstant::new(42),
        ),
        Err(MysqlTopologyError::Authority(
            TopologyAuthorityError::TransitionInFlight
        ))
    ));

    let online = observation(
        &attempt("topology-attempt", 100),
        &enrollments[1],
        "join-two-observation",
        "view-2",
        "view-2",
        vec![
            native(&enrollments[0], MemberRole::Primary, MemberState::Online),
            native(&enrollments[1], MemberRole::Secondary, MemberState::Online),
        ],
        group_gtids("1-6"),
        TopologyObservationStatus::Complete,
        TopologyInstant::new(43),
    );
    let second_credit = accepted(
        authority
            .accept_join(&effect, &online, TopologyInstant::new(44))
            .unwrap(),
    );
    assert_closed_no_credit(&authority, 2);

    let third = authority
        .authorize_join(
            MysqlMemberIndex::Third,
            boundary(&second_credit, &enrollments[0], group_gtids("1-6")),
            TopologyInstant::new(50),
        )
        .unwrap();
    let third_effect = third
        .record_effect(JoinEffect::required_steps().to_vec())
        .unwrap()
        .bind_observation_attempt(AttemptId::new("join-three-observation").unwrap());
    let final_evidence = observation(
        &attempt("topology-attempt", 100),
        &enrollments[2],
        "join-three-observation",
        "view-3",
        "view-3",
        vec![
            native(&enrollments[0], MemberRole::Primary, MemberState::Online),
            native(&enrollments[1], MemberRole::Secondary, MemberState::Online),
            native(&enrollments[2], MemberRole::Secondary, MemberState::Online),
        ],
        group_gtids("1-8"),
        TopologyObservationStatus::Complete,
        TopologyInstant::new(60),
    );
    let final_credit = accepted(
        authority
            .accept_join(&third_effect, &final_evidence, TopologyInstant::new(61))
            .unwrap(),
    );
    assert_eq!(final_credit.members().len(), 3);
    assert_eq!(authority.final_credit(), Some(&final_credit));
    assert_closed_no_credit(&authority, 3);
}

#[test]
fn join_requires_equal_or_superset_group_only_history() {
    for target_history in ["1-5", "1-6"] {
        let (mut authority, enrollments, predecessor) = accepted_bootstrap();
        let join = authority
            .authorize_join(
                MysqlMemberIndex::Second,
                boundary(&predecessor, &enrollments[0], group_gtids("1-5")),
                TopologyInstant::new(30),
            )
            .unwrap();
        let effect = join
            .record_effect(JoinEffect::required_steps().to_vec())
            .unwrap()
            .bind_observation_attempt(AttemptId::new("join-two-observation").unwrap());
        let evidence = join_two_observation(&enrollments, target_history);
        assert!(matches!(
            authority
                .accept_join(&effect, &evidence, TopologyInstant::new(45))
                .unwrap(),
            TransitionEvaluation::Accepted(_)
        ));
        assert_closed_no_credit(&authority, 2);
    }

    let (mut authority, enrollments, predecessor) = accepted_bootstrap();
    let join = authority
        .authorize_join(
            MysqlMemberIndex::Second,
            boundary(&predecessor, &enrollments[0], group_gtids("1-5")),
            TopologyInstant::new(30),
        )
        .unwrap();
    let effect = join
        .record_effect(JoinEffect::required_steps().to_vec())
        .unwrap()
        .bind_observation_attempt(AttemptId::new("join-two-observation").unwrap());
    let too_old = join_two_observation(&enrollments, "1-4");
    assert_eq!(
        authority.accept_join(&effect, &too_old, TopologyInstant::new(45)),
        Err(MysqlTopologyError::Gtid(
            TopologyGtidError::SourceBoundaryNotContained
        ))
    );
    assert_closed_no_credit(&authority, 1);
}

#[test]
fn non_group_gtid_sources_and_wrong_source_bindings_fail_before_join_effect() {
    let (mut authority, enrollments, predecessor) = accepted_bootstrap();
    let foreign = GtidSet::from_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1").unwrap();
    let mut effects = 0;
    if authority
        .authorize_join(
            MysqlMemberIndex::Second,
            boundary(&predecessor, &enrollments[0], foreign),
            TopologyInstant::new(30),
        )
        .is_ok()
    {
        effects += 1;
    }
    assert_eq!(effects, 0);
    assert_closed_no_credit(&authority, 1);

    let (mut authority, enrollments, predecessor) = accepted_bootstrap();
    let wrong_view = SourceGtidBoundary::new(
        predecessor.members()[0].binding().attempt().clone(),
        enrollments[0].clone(),
        predecessor.observation_attempt().clone(),
        ViewId::new("foreign-view").unwrap(),
        group_gtids("1"),
    );
    assert_eq!(
        authority.authorize_join(
            MysqlMemberIndex::Second,
            wrong_view,
            TopologyInstant::new(30),
        ),
        Err(MysqlTopologyError::Gtid(
            TopologyGtidError::SourceBoundaryBindingMismatch
        ))
    );
    assert_closed_no_credit(&authority, 1);

    let (mut authority, enrollments, predecessor) = accepted_bootstrap();
    let wrong_source = SourceGtidBoundary::new(
        predecessor.members()[0].binding().attempt().clone(),
        enrollments[2].clone(),
        predecessor.observation_attempt().clone(),
        predecessor.view_id().clone(),
        group_gtids("1"),
    );
    assert_eq!(
        authority.authorize_join(
            MysqlMemberIndex::Second,
            wrong_source,
            TopologyInstant::new(30),
        ),
        Err(MysqlTopologyError::Gtid(
            TopologyGtidError::SourceBoundaryBindingMismatch
        ))
    );
    assert_closed_no_credit(&authority, 1);
}

#[test]
fn wrong_join_order_is_rejected_without_dependent_effect() {
    let (mut authority, enrollments, predecessor) = accepted_bootstrap();
    let mut effects = 0;
    if authority
        .authorize_join(
            MysqlMemberIndex::Third,
            boundary(&predecessor, &enrollments[0], group_gtids("1")),
            TopologyInstant::new(30),
        )
        .is_ok()
    {
        effects += 1;
    }
    assert_eq!(effects, 0);
    assert_closed_no_credit(&authority, 1);
}

#[test]
fn fail_closed_identity_ownership_membership_and_binding_matrix() {
    let cases = [
        NegativeCase::WrongUuid,
        NegativeCase::OwnershipDrift,
        NegativeCase::UnexpectedMember,
        NegativeCase::RequiredMemberLoss,
        NegativeCase::CrossAttempt,
        NegativeCase::CrossView,
    ];
    for case in cases {
        let (mut authority, enrollments, effect) = bootstrap_in_flight();
        let mut evidence = bootstrap_observation(&enrollments);
        evidence = negative_evidence(case, &enrollments, evidence);
        assert!(
            authority
                .accept_bootstrap(&effect, &evidence, TopologyInstant::new(21))
                .is_err(),
            "{case:?}"
        );
        assert_closed_no_credit(&authority, 0);
        assert!(matches!(
            authority.authorize_join(
                MysqlMemberIndex::Second,
                SourceGtidBoundary::new(
                    AttemptId::new("topology-attempt").unwrap(),
                    enrollments[0].clone(),
                    AttemptId::new("bootstrap-observation").unwrap(),
                    ViewId::new("view-1").unwrap(),
                    group_gtids("1"),
                ),
                TopologyInstant::new(22),
            ),
            Err(MysqlTopologyError::TopologyState(
                TopologyStateError::InvalidTransition
            ))
        ));
    }
}

#[test]
fn fail_closed_collection_product_native_state_and_deadline_matrix() {
    let statuses = [
        (
            TopologyObservationStatus::Missing,
            MysqlTopologyError::Evidence(TopologyEvidenceError::Missing),
        ),
        (
            TopologyObservationStatus::ProductMismatch,
            MysqlTopologyError::Evidence(TopologyEvidenceError::ProductMismatch),
        ),
        (
            TopologyObservationStatus::OwnershipContextLoss,
            MysqlTopologyError::Evidence(TopologyEvidenceError::OwnershipContextLoss),
        ),
        (
            TopologyObservationStatus::UnsupportedRole,
            MysqlTopologyError::NativeState(TopologyNativeStateError::UnsupportedRole),
        ),
        (
            TopologyObservationStatus::UnsupportedState,
            MysqlTopologyError::NativeState(TopologyNativeStateError::UnsupportedState),
        ),
    ];
    for (status, expected) in statuses {
        let (mut authority, enrollments, effect) = bootstrap_in_flight();
        let evidence = observation(
            &attempt("topology-attempt", 100),
            &enrollments[0],
            "bootstrap-observation",
            "view-1",
            "view-1",
            vec![native(
                &enrollments[0],
                MemberRole::Primary,
                MemberState::Online,
            )],
            group_gtids("1"),
            status,
            TopologyInstant::new(20),
        );
        assert_eq!(
            authority.accept_bootstrap(&effect, &evidence, TopologyInstant::new(21)),
            Err(expected)
        );
        assert_closed_no_credit(&authority, 0);
    }

    for state in [
        MemberState::Offline,
        MemberState::Error,
        MemberState::Unreachable,
    ] {
        let (mut authority, enrollments, effect) = bootstrap_in_flight();
        let evidence = observation(
            &attempt("topology-attempt", 100),
            &enrollments[0],
            "bootstrap-observation",
            "view-1",
            "view-1",
            vec![native(&enrollments[0], MemberRole::Primary, state)],
            group_gtids("1"),
            TopologyObservationStatus::Complete,
            TopologyInstant::new(20),
        );
        assert!(
            matches!(
                authority.accept_bootstrap(&effect, &evidence, TopologyInstant::new(21)),
                Err(MysqlTopologyError::NativeState(
                    TopologyNativeStateError::Offline
                        | TopologyNativeStateError::Error
                        | TopologyNativeStateError::Unreachable
                ))
            ),
            "{state:?}"
        );
        assert_closed_no_credit(&authority, 0);
    }

    let (mut authority, enrollments, effect) = bootstrap_in_flight();
    let expired = observation(
        &attempt("topology-attempt", 100),
        &enrollments[0],
        "bootstrap-observation",
        "view-1",
        "view-1",
        vec![native(
            &enrollments[0],
            MemberRole::Primary,
            MemberState::Online,
        )],
        group_gtids("1"),
        TopologyObservationStatus::Complete,
        TopologyInstant::new(100),
    );
    assert_eq!(
        authority.accept_bootstrap(&effect, &expired, TopologyInstant::new(99)),
        Err(MysqlTopologyError::Deadline(ControlStage::DiscoverView))
    );
    assert_closed_no_credit(&authority, 0);
}

#[test]
fn local_role_semantics_are_exact_and_recovery_never_grants_early_credit() {
    let (mut authority, enrollments, effect) = bootstrap_in_flight();
    let secondary_bootstrap = observation(
        &attempt("topology-attempt", 100),
        &enrollments[0],
        "bootstrap-observation",
        "view-1",
        "view-1",
        vec![native(
            &enrollments[0],
            MemberRole::Secondary,
            MemberState::Online,
        )],
        group_gtids("1"),
        TopologyObservationStatus::Complete,
        TopologyInstant::new(20),
    );
    assert_eq!(
        authority.accept_bootstrap(&effect, &secondary_bootstrap, TopologyInstant::new(21)),
        Err(MysqlTopologyError::NativeState(
            TopologyNativeStateError::BootstrapMemberNotPrimary
        ))
    );
    assert_closed_no_credit(&authority, 0);

    let (mut authority, enrollments, predecessor) = accepted_bootstrap();
    let join = authority
        .authorize_join(
            MysqlMemberIndex::Second,
            boundary(&predecessor, &enrollments[0], group_gtids("1")),
            TopologyInstant::new(30),
        )
        .unwrap();
    let effect = join
        .record_effect(JoinEffect::required_steps().to_vec())
        .unwrap()
        .bind_observation_attempt(AttemptId::new("join-two-observation").unwrap());
    let primary_join = observation(
        &attempt("topology-attempt", 100),
        &enrollments[1],
        "join-two-observation",
        "view-2",
        "view-2",
        vec![
            native(&enrollments[0], MemberRole::Primary, MemberState::Online),
            native(&enrollments[1], MemberRole::Primary, MemberState::Online),
        ],
        group_gtids("1"),
        TopologyObservationStatus::Complete,
        TopologyInstant::new(40),
    );
    assert_eq!(
        authority.accept_join(&effect, &primary_join, TopologyInstant::new(41)),
        Err(MysqlTopologyError::NativeState(
            TopologyNativeStateError::JoiningMemberNotSecondary
        ))
    );
    assert_closed_no_credit(&authority, 1);

    let (mut authority, enrollments, predecessor) = accepted_bootstrap();
    let join = authority
        .authorize_join(
            MysqlMemberIndex::Second,
            boundary(&predecessor, &enrollments[0], group_gtids("1")),
            TopologyInstant::new(30),
        )
        .unwrap();
    let effect = join
        .record_effect(JoinEffect::required_steps().to_vec())
        .unwrap()
        .bind_observation_attempt(AttemptId::new("join-two-observation").unwrap());
    let predecessor_offline = observation(
        &attempt("topology-attempt", 100),
        &enrollments[1],
        "join-two-observation",
        "view-2",
        "view-2",
        vec![
            native(&enrollments[0], MemberRole::Primary, MemberState::Offline),
            native(&enrollments[1], MemberRole::Secondary, MemberState::Online),
        ],
        group_gtids("1"),
        TopologyObservationStatus::Complete,
        TopologyInstant::new(40),
    );
    assert_eq!(
        authority.accept_join(&effect, &predecessor_offline, TopologyInstant::new(41)),
        Err(MysqlTopologyError::NativeState(
            TopologyNativeStateError::PredecessorNotOnline
        ))
    );
    assert_closed_no_credit(&authority, 1);
}

#[test]
fn discovery_retains_enrollment_and_cannot_replace_owned_bindings() {
    let attempt = attempt("topology-attempt", 100);
    let enrollment = enrollment(&attempt, MysqlMemberIndex::First);
    let discovery = ViewDiscovery::new(
        enrollment.clone(),
        attempt.group_name().clone(),
        ViewId::new("view-proposal").unwrap(),
        ViewDiscovery::required_steps().to_vec(),
    )
    .unwrap();
    assert_eq!(discovery.enrollment(), &enrollment);
    assert_eq!(discovery.group_name(), attempt.group_name());
    assert_eq!(discovery.view_id().as_str(), "view-proposal");
}

#[test]
fn control_failures_are_distinct_paired_and_secret_free() {
    let errors = [
        MysqlTopologyError::ControlTransport(ControlStage::Connect),
        MysqlTopologyError::ControlAuthentication(ControlStage::Authenticate),
        MysqlTopologyError::ControlPermission(ControlStage::GrantRecovery),
        MysqlTopologyError::ControlProductCompatibility,
        MysqlTopologyError::OwnershipContextLoss,
    ];
    for (left_index, left) in errors.iter().enumerate() {
        assert!(!format!("{left:?}").contains("fixture-secret"));
        for right in errors.iter().skip(left_index + 1) {
            assert_ne!(left, right);
        }
        let (authority, _) = authority();
        let mut dependent_effects = 0;
        let failed_control: Result<(), MysqlTopologyError> = Err(left.clone());
        if failed_control.is_ok() {
            dependent_effects += 1;
        }
        assert_eq!(dependent_effects, 0);
        assert_closed_no_credit(&authority, 0);
    }
    let paired = MysqlTopologyError::PairedControl {
        prior: Box::new(MysqlTopologyError::ControlPermission(
            ControlStage::StartGroupReplication,
        )),
        cleanup: Box::new(MysqlTopologyError::ControlTransport(
            ControlStage::DisableBootstrap,
        )),
    };
    assert!(matches!(paired, MysqlTopologyError::PairedControl { .. }));
    assert!(!format!("{paired}").contains("fixture-secret"));
}

#[derive(Clone, Copy, Debug)]
enum NegativeCase {
    WrongUuid,
    OwnershipDrift,
    UnexpectedMember,
    RequiredMemberLoss,
    CrossAttempt,
    CrossView,
}

fn negative_evidence(
    case: NegativeCase,
    enrollments: &[NativeIdentityEnrollment; 3],
    _baseline: TopologyObservation,
) -> TopologyObservation {
    let attempt = attempt("topology-attempt", 100);
    let target = &enrollments[0];
    let mut local = ObservedLocalBinding::from_enrollment(target);
    let mut members = vec![native(target, MemberRole::Primary, MemberState::Online)];
    let mut observation_attempt = AttemptId::new("bootstrap-observation").unwrap();
    let view_id = ViewId::new("view-1").unwrap();
    let mut binding_view_id = view_id.clone();
    match case {
        NegativeCase::WrongUuid => {
            local = ObservedLocalBinding::new(
                target.binding().attempt().clone(),
                target.binding().member(),
                target.binding().process_session().clone(),
                target.binding().endpoint().clone(),
                target.binding().storage().clone(),
                ServerUuid::new(MEMBER_UUIDS[1]).unwrap(),
                MemberId::new(MEMBER_UUIDS[1]).unwrap(),
                target.binding().member_address().clone(),
                target.binding().observer_generation().clone(),
            );
        }
        NegativeCase::OwnershipDrift => {
            local = ObservedLocalBinding::new(
                target.binding().attempt().clone(),
                target.binding().member(),
                target.binding().process_session().clone(),
                EndpointBinding::new("foreign-endpoint").unwrap(),
                target.binding().storage().clone(),
                target.server_uuid().clone(),
                target.member_id().clone(),
                target.binding().member_address().clone(),
                target.binding().observer_generation().clone(),
            );
        }
        NegativeCase::UnexpectedMember => {
            members.push(NativeMember::new(
                MemberId::new("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap(),
                MemberAddress::new("127.0.0.1:33999").unwrap(),
                MemberRole::Secondary,
                MemberState::Online,
            ));
        }
        NegativeCase::RequiredMemberLoss => members.clear(),
        NegativeCase::CrossAttempt => {
            observation_attempt = AttemptId::new("foreign-observation").unwrap();
            local = ObservedLocalBinding::new(
                AttemptId::new("foreign-topology").unwrap(),
                target.binding().member(),
                target.binding().process_session().clone(),
                target.binding().endpoint().clone(),
                target.binding().storage().clone(),
                target.server_uuid().clone(),
                target.member_id().clone(),
                target.binding().member_address().clone(),
                target.binding().observer_generation().clone(),
            );
        }
        NegativeCase::CrossView => {
            binding_view_id = ViewId::new("other-view").unwrap();
        }
    }
    TopologyObservation::new(
        TopologyObservationStatus::Complete,
        observation_attempt,
        local,
        attempt.group_name().clone(),
        binding_view_id,
        view_id,
        members,
        group_gtids("1"),
        TopologyInstant::new(20),
    )
}

fn authority() -> (TopologyAuthority, [NativeIdentityEnrollment; 3]) {
    let attempt = attempt("topology-attempt", 100);
    let enrollments = [
        enrollment(&attempt, MysqlMemberIndex::First),
        enrollment(&attempt, MysqlMemberIndex::Second),
        enrollment(&attempt, MysqlMemberIndex::Third),
    ];
    let mut authority = TopologyAuthority::new(attempt);
    for enrollment in &enrollments {
        authority.register_enrollment(enrollment.clone()).unwrap();
    }
    (authority, enrollments)
}

fn bootstrap_in_flight() -> (
    TopologyAuthority,
    [NativeIdentityEnrollment; 3],
    BootstrapEffect,
) {
    let (mut authority, enrollments) = authority();
    let capability = authority
        .authorize_bootstrap(MysqlMemberIndex::First, TopologyInstant::new(10))
        .unwrap();
    let effect = capability
        .record_effect(BootstrapEffect::required_steps().to_vec())
        .unwrap()
        .bind_observation_attempt(AttemptId::new("bootstrap-observation").unwrap());
    (authority, enrollments, effect)
}

fn accepted_bootstrap() -> (
    TopologyAuthority,
    [NativeIdentityEnrollment; 3],
    TransitionCredit,
) {
    let (mut authority, enrollments, effect) = bootstrap_in_flight();
    let evidence = bootstrap_observation(&enrollments);
    let credit = accepted(
        authority
            .accept_bootstrap(&effect, &evidence, TopologyInstant::new(21))
            .unwrap(),
    );
    (authority, enrollments, credit)
}

fn bootstrap_observation(enrollments: &[NativeIdentityEnrollment; 3]) -> TopologyObservation {
    observation(
        &attempt("topology-attempt", 100),
        &enrollments[0],
        "bootstrap-observation",
        "view-1",
        "view-1",
        vec![native(
            &enrollments[0],
            MemberRole::Primary,
            MemberState::Online,
        )],
        group_gtids("1"),
        TopologyObservationStatus::Complete,
        TopologyInstant::new(20),
    )
}

fn join_two_observation(
    enrollments: &[NativeIdentityEnrollment; 3],
    history: &str,
) -> TopologyObservation {
    observation(
        &attempt("topology-attempt", 100),
        &enrollments[1],
        "join-two-observation",
        "view-2",
        "view-2",
        vec![
            native(&enrollments[0], MemberRole::Primary, MemberState::Online),
            native(&enrollments[1], MemberRole::Secondary, MemberState::Online),
        ],
        group_gtids(history),
        TopologyObservationStatus::Complete,
        TopologyInstant::new(40),
    )
}

fn attempt(id: &str, deadline: u64) -> TopologyAttempt {
    TopologyAttempt::new(
        AttemptId::new(id).unwrap(),
        GroupName::new(GROUP_UUID).unwrap(),
        MysqlMemberIndex::First,
        TopologyInstant::new(0),
        TopologyInstant::new(deadline),
    )
    .unwrap()
}

fn binding(attempt: &TopologyAttempt, member: MysqlMemberIndex) -> MemberControlBinding {
    let suffix = member.as_usize() + 1;
    MemberControlBinding::new(
        attempt.id().clone(),
        member,
        ProcessSessionId::new(format!("process-{suffix}")).unwrap(),
        EndpointBinding::new(format!("owned-uds-{suffix}")).unwrap(),
        StorageBinding::new(format!("storage-{suffix}")).unwrap(),
        MemberAddress::new(format!("127.0.0.1:3306{suffix}")).unwrap(),
        CredentialGeneration::new("observer-generation").unwrap(),
        CredentialGeneration::new("recovery-generation").unwrap(),
    )
}

fn credentials(attempt: &TopologyAttempt) -> (ControlCredential, ControlCredential) {
    (
        ControlCredential::new(
            attempt.id().clone(),
            CredentialGeneration::new("observer-generation").unwrap(),
            ControlCredentialRole::Observer,
            "observer_local",
            "observer-secret",
        )
        .unwrap(),
        ControlCredential::new(
            attempt.id().clone(),
            CredentialGeneration::new("recovery-generation").unwrap(),
            ControlCredentialRole::Recovery,
            "recovery_local",
            "recovery-secret",
        )
        .unwrap(),
    )
}

fn accounts(attempt: &TopologyAttempt, member: MysqlMemberIndex) -> AccountProvisioningEvidence {
    let (observer, recovery) = credentials(attempt);
    AccountProvisioningEvidence::new(
        binding(attempt, member),
        &observer,
        &recovery,
        AccountProvisioningEvidence::required_steps().to_vec(),
    )
    .unwrap()
}

fn enrollment(attempt: &TopologyAttempt, member: MysqlMemberIndex) -> NativeIdentityEnrollment {
    let binding = binding(attempt, member);
    NativeIdentityEnrollment::new(
        binding,
        ServerUuid::new(MEMBER_UUIDS[member.as_usize()]).unwrap(),
        &accounts(attempt, member),
        true,
        NativeIdentityEnrollment::required_steps().to_vec(),
    )
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
fn observation(
    attempt: &TopologyAttempt,
    target: &NativeIdentityEnrollment,
    observation_attempt: &str,
    binding_view: &str,
    view: &str,
    members: Vec<NativeMember>,
    executed: GtidSet,
    status: TopologyObservationStatus,
    decision: TopologyInstant,
) -> TopologyObservation {
    TopologyObservation::new(
        status,
        AttemptId::new(observation_attempt).unwrap(),
        ObservedLocalBinding::from_enrollment(target),
        attempt.group_name().clone(),
        ViewId::new(binding_view).unwrap(),
        ViewId::new(view).unwrap(),
        members,
        executed,
        decision,
    )
}

fn native(
    enrollment: &NativeIdentityEnrollment,
    role: MemberRole,
    state: MemberState,
) -> NativeMember {
    NativeMember::new(
        enrollment.member_id().clone(),
        enrollment.binding().member_address().clone(),
        role,
        state,
    )
}

fn boundary(
    credit: &TransitionCredit,
    source: &NativeIdentityEnrollment,
    executed: GtidSet,
) -> SourceGtidBoundary {
    SourceGtidBoundary::new(
        source.binding().attempt().clone(),
        source.clone(),
        credit.observation_attempt().clone(),
        credit.view_id().clone(),
        executed,
    )
}

fn group_gtids(intervals: &str) -> GtidSet {
    GtidSet::from_str(&format!("{GROUP_UUID}:{intervals}")).unwrap()
}

fn accepted(evaluation: TransitionEvaluation) -> TransitionCredit {
    match evaluation {
        TransitionEvaluation::Accepted(credit) => credit,
        TransitionEvaluation::Pending => panic!("expected accepted transition"),
    }
}

fn assert_closed_no_credit(authority: &TopologyAuthority, expected_credits: u8) {
    assert_eq!(authority.lifecycle_credits(), expected_credits);
    assert!(!authority.read_access_open());
    assert!(!authority.write_access_open());
}

trait MemberIndicesForTest {
    fn all_for_test() -> [MysqlMemberIndex; 3];
}

impl MemberIndicesForTest for MysqlMemberIndex {
    fn all_for_test() -> [MysqlMemberIndex; 3] {
        [
            MysqlMemberIndex::First,
            MysqlMemberIndex::Second,
            MysqlMemberIndex::Third,
        ]
    }
}
