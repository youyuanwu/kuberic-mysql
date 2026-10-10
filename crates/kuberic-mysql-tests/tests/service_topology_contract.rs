#[path = "service_common/mod.rs"]
mod common;

use std::collections::{HashSet, VecDeque};
use std::fs;
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use common::{TestRoot, provide_socket};
use kuberic_mysql::adapter::{
    AdapterDiagnostic, ClockContext, ClockError, IdentityField, ObservationClock,
    ObservationReport, ObservationRequest,
};
use kuberic_mysql::core::{
    AttemptId, AuthorityGeneration, BoundGtidSet, ConfigurationId, CredentialGeneration,
    EndpointBinding, Epoch, GroupName, GroupReplicationAddress, GtidSet, MemberAddress, MemberId,
    MemberRole, MemberState, NativeAccessState, NativeLocalState, NativeMember,
    NativeObservationBracket, NativeObservationDraft, NativeSnapshot, NativeSwitch, NativeView,
    ObservationInstant, ObservationMetadata, ObservationSessionId, PartitionId, ProcessSessionId,
    ReplicaId, ReplicaIncarnation, ResourceId, ServerUuid, StorageBinding, ViewId,
};
use kuberic_mysql::service::{
    AccountProvisioningEvidence, BootstrapEffect, ControlCredential, ControlCredentialRole,
    ControlStage, ControlStep, JoinEffect, MemberControlBinding, MysqlInstanceManager,
    MysqlMemberIndex, MysqlTopologyError, MysqlTopologyManager, MysqlTopologyManagerError,
    MysqlTopologyMemberRuntime, MysqlTopologyState, NativeControlDeadline,
    NativeIdentityEnrollment, ObservedLocalBinding, OwnershipError, SourceGtidBoundary,
    TopologyAttempt, TopologyAuthority, TopologyAuthorityError, TopologyEvidenceError,
    TopologyGtidError, TopologyInstant, TopologyNativeStateError, TopologyObservation,
    TopologyObservationContext, TopologyObservationStatus, TopologyStateError, TransitionCredit,
    TransitionEvaluation, ViewDiscovery,
};

const GROUP_UUID: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
const MEMBER_UUIDS: [&str; 3] = [
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    "dddddddd-dddd-dddd-dddd-dddddddddddd",
];

#[test]
fn manager_owns_exactly_three_existing_instances_and_cleans_initialized_layouts() {
    let roots = [
        TestRoot::new("topology-one"),
        TestRoot::new("topology-two"),
        TestRoot::new("topology-three"),
    ];
    let instances = std::array::from_fn(|index| {
        MysqlInstanceManager::new(
            roots[index].config_for_member(MysqlMemberIndex::all_for_test()[index]),
        )
    });
    let topology_attempt = attempt("manager-layout-attempt", 100);
    let (observer, recovery) = credentials(&topology_attempt);
    let mut manager =
        MysqlTopologyManager::new(instances, topology_attempt, observer, recovery).unwrap();

    assert_eq!(manager.members().len(), 3);
    assert_eq!(manager.state(), MysqlTopologyState::Configured);
    assert!(!manager.read_access_open());
    assert!(!manager.write_access_open());
    manager.initialize().unwrap();
    assert_eq!(manager.state(), MysqlTopologyState::Initialized);

    for root in &roots {
        let names = inventory(&root.scratch);
        for forbidden in [
            "metadata", "journal", "receipt", "cursor", "adoption", "store",
        ] {
            assert!(
                names.iter().all(|name| !name.contains(forbidden)),
                "{names:?}"
            );
        }
    }

    manager.stop().unwrap();
    assert_eq!(manager.state(), MysqlTopologyState::Stopped);
    for (index, root) in roots.iter().enumerate() {
        assert!(root.data.is_dir());
        assert!(!root.scratch.exists());
        assert!(
            !manager.members()[index]
                .config()
                .runtime()
                .socket()
                .exists()
        );
        assert_endpoint_refuses(
            manager.members()[index]
                .config()
                .member()
                .group_replication_address(),
        );
    }
}

#[test]
fn manager_initialization_failure_contains_prior_members_without_starting_later_members() {
    let roots = [
        TestRoot::new("init-fail-one"),
        TestRoot::new("init-fail-two"),
        TestRoot::new("init-fail-three"),
    ];
    let instances = std::array::from_fn(|index| {
        MysqlInstanceManager::new(
            roots[index].config_for_member(MysqlMemberIndex::all_for_test()[index]),
        )
    });
    fs::set_permissions(&roots[1].launcher, fs::Permissions::from_mode(0o600)).unwrap();
    let topology_attempt = attempt("manager-init-failure", 100);
    let (observer, recovery) = credentials(&topology_attempt);
    let mut manager =
        MysqlTopologyManager::new(instances, topology_attempt, observer, recovery).unwrap();

    let result = manager.initialize();
    assert!(
        matches!(
            &result,
            Err(MysqlTopologyManagerError::Instance {
                member: MysqlMemberIndex::Second,
                ..
            })
        ),
        "{result:?}"
    );
    assert_eq!(manager.state(), MysqlTopologyState::Failed);
    assert!(roots[0].data.is_dir());
    assert!(!roots[0].scratch.exists());
    assert!(!roots[1].data.exists());
    assert!(!roots[1].scratch.exists());
    assert!(!roots[2].data.exists());
    assert!(!roots[2].scratch.exists());
    for member in manager.members() {
        assert_endpoint_refuses(member.config().member().group_replication_address());
    }
}

#[test]
fn manager_stop_aggregates_every_member_cleanup_failure() {
    let roots = [
        TestRoot::new("cleanup-one"),
        TestRoot::new("cleanup-two"),
        TestRoot::new("cleanup-three"),
    ];
    let instances = std::array::from_fn(|index| {
        MysqlInstanceManager::new(
            roots[index].config_for_member(MysqlMemberIndex::all_for_test()[index]),
        )
    });
    let topology_attempt = attempt("manager-cleanup-failure", 100);
    let (observer, recovery) = credentials(&topology_attempt);
    let mut manager =
        MysqlTopologyManager::new(instances, topology_attempt, observer, recovery).unwrap();
    manager.initialize().unwrap();
    for root in &roots {
        fs::set_permissions(&root.root, fs::Permissions::from_mode(0o500)).unwrap();
    }

    let error = manager.stop().unwrap_err();
    for root in &roots {
        fs::set_permissions(&root.root, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let MysqlTopologyManagerError::Cleanup { primary, failures } = error else {
        panic!("expected aggregate cleanup failure");
    };
    assert!(primary.is_none());
    assert_eq!(failures.len(), 3);
    assert_eq!(
        failures
            .iter()
            .map(|failure| failure.member())
            .collect::<Vec<_>>(),
        MysqlMemberIndex::all_for_test()
    );
    assert_eq!(manager.state(), MysqlTopologyState::Failed);
    for root in &roots {
        assert!(root.data.is_dir());
        assert!(root.scratch.is_dir());
    }
}

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
        NativeIdentityEnrollment::required_steps(),
        [
            ControlStep::ValidateProduct,
            ControlStep::ProveFreshGroupState,
            ControlStep::ProveFreshExecutedHistory,
            ControlStep::EnrollServerIdentity,
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
        GtidSet::empty(),
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
            GtidSet::empty(),
            NativeIdentityEnrollment::required_steps().to_vec(),
        ),
        Err(MysqlTopologyError::TopologyState(
            TopologyStateError::ExistingGroupState
        ))
    );
    assert_eq!(
        NativeIdentityEnrollment::new(
            binding.clone(),
            ServerUuid::new(MEMBER_UUIDS[0]).unwrap(),
            &accounts,
            true,
            group_gtids("1"),
            NativeIdentityEnrollment::required_steps().to_vec(),
        ),
        Err(MysqlTopologyError::TopologyState(
            TopologyStateError::ExistingTransactionHistory
        ))
    );
    assert_eq!(
        NativeIdentityEnrollment::new(
            binding,
            ServerUuid::new(MEMBER_UUIDS[0]).unwrap(),
            &accounts,
            true,
            GtidSet::empty(),
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
    let source_boundary = boundary(&bootstrap_credit, &enrollments[0]);
    let join = authority
        .authorize_join(
            MysqlMemberIndex::Second,
            source_boundary,
            TopologyInstant::new(30),
        )
        .unwrap();
    let mut effect = join
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
            boundary(&bootstrap_credit, &enrollments[0]),
            TopologyInstant::new(42),
        ),
        Err(MysqlTopologyError::Authority(
            TopologyAuthorityError::TransitionInFlight
        ))
    ));

    let online_attempt = AttemptId::new("join-two-observation-online").unwrap();
    effect.rebind_observation_attempt(online_attempt.clone());
    let online = observation(
        &attempt("topology-attempt", 100),
        &enrollments[1],
        online_attempt.as_str(),
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
            boundary(&second_credit, &enrollments[0]),
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
        let (mut authority, enrollments, predecessor) = accepted_bootstrap_with_history("1-5");
        let join = authority
            .authorize_join(
                MysqlMemberIndex::Second,
                boundary(&predecessor, &enrollments[0]),
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

    let (mut authority, enrollments, predecessor) = accepted_bootstrap_with_history("1-5");
    let join = authority
        .authorize_join(
            MysqlMemberIndex::Second,
            boundary(&predecessor, &enrollments[0]),
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
fn understated_source_boundary_cannot_admit_under_recovered_target() {
    let (mut authority, enrollments, predecessor) = accepted_bootstrap_with_history("1-100");
    let join = authority
        .authorize_join(
            MysqlMemberIndex::Second,
            boundary(&predecessor, &enrollments[0]),
            TopologyInstant::new(30),
        )
        .unwrap();
    let effect = join
        .record_effect(JoinEffect::required_steps().to_vec())
        .unwrap()
        .bind_observation_attempt(AttemptId::new("join-two-observation").unwrap());
    let under_recovered = join_two_observation(&enrollments, "1-99");
    assert_eq!(
        authority.accept_join(&effect, &under_recovered, TopologyInstant::new(45)),
        Err(MysqlTopologyError::Gtid(
            TopologyGtidError::SourceBoundaryNotContained
        ))
    );
    assert_closed_no_credit(&authority, 1);
}

#[test]
fn non_group_gtid_sources_and_wrong_source_bindings_fail_before_join_effect() {
    let (mut authority, enrollments, effect) = bootstrap_in_flight();
    let foreign = GtidSet::from_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1").unwrap();
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
        foreign,
        TopologyObservationStatus::Complete,
        TopologyInstant::new(20),
    );
    assert_eq!(
        authority.accept_bootstrap(&effect, &evidence, TopologyInstant::new(21)),
        Err(MysqlTopologyError::Gtid(TopologyGtidError::NonGroupSource))
    );
    assert_closed_no_credit(&authority, 0);

    let (authority, enrollments, predecessor) = accepted_bootstrap();
    assert_eq!(
        predecessor.source_gtid_boundary(
            &attempt("topology-attempt", 100),
            &enrollments[2],
            &source_observation(&predecessor, &enrollments[0]),
            TopologyInstant::new(25),
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
            boundary(&predecessor, &enrollments[0]),
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
        let error = reject_bootstrap_without_dependent_effect(&mut authority, &effect, &evidence);
        assert_ne!(
            error,
            MysqlTopologyError::TopologyState(TopologyStateError::InvalidTransition),
            "{case:?}"
        );
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
            reject_bootstrap_without_dependent_effect(&mut authority, &effect, &evidence),
            expected
        );
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
                reject_bootstrap_without_dependent_effect(&mut authority, &effect, &evidence),
                MysqlTopologyError::NativeState(
                    TopologyNativeStateError::Offline
                        | TopologyNativeStateError::Error
                        | TopologyNativeStateError::Unreachable
                )
            ),
            "{state:?}"
        );
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
        reject_bootstrap_without_dependent_effect(&mut authority, &effect, &expired),
        MysqlTopologyError::Deadline(ControlStage::DiscoverView)
    );
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
            boundary(&predecessor, &enrollments[0]),
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
            boundary(&predecessor, &enrollments[0]),
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
    accepted_bootstrap_with_history("1")
}

fn accepted_bootstrap_with_history(
    history: &str,
) -> (
    TopologyAuthority,
    [NativeIdentityEnrollment; 3],
    TransitionCredit,
) {
    let (mut authority, enrollments, effect) = bootstrap_in_flight();
    let evidence = bootstrap_observation_with_history(&enrollments, history);
    let credit = accepted(
        authority
            .accept_bootstrap(&effect, &evidence, TopologyInstant::new(21))
            .unwrap(),
    );
    (authority, enrollments, credit)
}

fn bootstrap_observation(enrollments: &[NativeIdentityEnrollment; 3]) -> TopologyObservation {
    bootstrap_observation_with_history(enrollments, "1")
}

fn bootstrap_observation_with_history(
    enrollments: &[NativeIdentityEnrollment; 3],
    history: &str,
) -> TopologyObservation {
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
        group_gtids(history),
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
        GtidSet::empty(),
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

fn boundary(credit: &TransitionCredit, source: &NativeIdentityEnrollment) -> SourceGtidBoundary {
    credit
        .source_gtid_boundary(
            &attempt("topology-attempt", 100),
            source,
            &source_observation(credit, source),
            TopologyInstant::new(25),
        )
        .unwrap()
}

fn source_observation(
    credit: &TransitionCredit,
    source: &NativeIdentityEnrollment,
) -> TopologyObservation {
    let attempt = attempt("topology-attempt", 100);
    let members = credit
        .members()
        .iter()
        .map(|enrollment| {
            native(
                enrollment,
                if enrollment.binding().member() == MysqlMemberIndex::First {
                    MemberRole::Primary
                } else {
                    MemberRole::Secondary
                },
                MemberState::Online,
            )
        })
        .collect();
    TopologyObservation::new(
        TopologyObservationStatus::Complete,
        AttemptId::new("fresh-source-observation").unwrap(),
        ObservedLocalBinding::from_enrollment(source),
        attempt.group_name().clone(),
        credit.view_id().clone(),
        credit.view_id().clone(),
        members,
        credit.executed().clone(),
        TopologyInstant::new(24),
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

fn reject_bootstrap_without_dependent_effect(
    authority: &mut TopologyAuthority,
    effect: &BootstrapEffect,
    evidence: &TopologyObservation,
) -> MysqlTopologyError {
    let error = authority
        .accept_bootstrap(effect, evidence, TopologyInstant::new(21))
        .unwrap_err();
    let (_, enrollments, accepted_credit) = accepted_bootstrap();
    let mut dependent_effects = 0;
    if authority
        .authorize_join(
            MysqlMemberIndex::Second,
            boundary(&accepted_credit, &enrollments[0]),
            TopologyInstant::new(22),
        )
        .is_ok()
    {
        dependent_effects += 1;
    }
    assert_eq!(dependent_effects, 0);
    assert_closed_no_credit(authority, 0);
    error
}

fn assert_closed_no_credit(authority: &TopologyAuthority, expected_credits: u8) {
    assert_eq!(authority.lifecycle_credits(), expected_credits);
    assert!(!authority.read_access_open());
    assert!(!authority.write_access_open());
}

fn inventory(root: &std::path::Path) -> Vec<String> {
    let mut names = Vec::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                pending.push(entry.path());
            }
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    names
}

fn assert_endpoint_refuses(address: std::net::SocketAddr) {
    assert!(
        TcpStream::connect_timeout(&address, Duration::from_millis(50)).is_err(),
        "{address} unexpectedly accepted a connection"
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScriptFailure {
    None,
    Initialize(MysqlMemberIndex),
    Bootstrap,
    Join(MysqlMemberIndex),
    PostEffectObservation,
    SourceBinding,
    ContextLoss,
}

#[derive(Clone, Debug)]
struct ObservationPlan {
    member: MysqlMemberIndex,
    view: &'static str,
    states: Vec<MemberState>,
    history: &'static str,
}

#[derive(Debug)]
struct ScriptState {
    failure: ScriptFailure,
    plans: VecDeque<ObservationPlan>,
    enrollments: [Option<NativeIdentityEnrollment>; 3],
    attempts: Vec<AttemptId>,
    deadlines: Vec<ObservationInstant>,
    boundary_attempts: Vec<AttemptId>,
    events: Vec<String>,
}

impl ScriptState {
    fn new(failure: ScriptFailure) -> Self {
        Self {
            failure,
            plans: VecDeque::from([
                ObservationPlan {
                    member: MysqlMemberIndex::First,
                    view: "view-1",
                    states: vec![MemberState::Online],
                    history: "1",
                },
                ObservationPlan {
                    member: MysqlMemberIndex::First,
                    view: "view-1",
                    states: vec![MemberState::Online],
                    history: "1-2",
                },
                ObservationPlan {
                    member: MysqlMemberIndex::Second,
                    view: "view-2",
                    states: vec![MemberState::Online, MemberState::Recovering],
                    history: "1-2",
                },
                ObservationPlan {
                    member: MysqlMemberIndex::Second,
                    view: "view-2",
                    states: vec![MemberState::Online, MemberState::Online],
                    history: "1-3",
                },
                ObservationPlan {
                    member: MysqlMemberIndex::Second,
                    view: "view-2",
                    states: vec![MemberState::Online, MemberState::Online],
                    history: "1-4",
                },
                ObservationPlan {
                    member: MysqlMemberIndex::Third,
                    view: "view-3",
                    states: vec![
                        MemberState::Online,
                        MemberState::Online,
                        MemberState::Online,
                    ],
                    history: "1-5",
                },
            ]),
            enrollments: std::array::from_fn(|_| None),
            attempts: Vec::new(),
            deadlines: Vec::new(),
            boundary_attempts: Vec::new(),
            events: Vec::new(),
        }
    }
}

struct ScriptedMember {
    inner: MysqlInstanceManager,
    state: Arc<Mutex<ScriptState>>,
    endpoint: Option<TcpListener>,
    socket_thread: Option<JoinHandle<()>>,
    started_pid: Option<u32>,
}

impl ScriptedMember {
    fn new(inner: MysqlInstanceManager, state: Arc<Mutex<ScriptState>>) -> Self {
        let endpoint =
            TcpListener::bind(inner.config().member().group_replication_address()).unwrap();
        Self {
            inner,
            state,
            endpoint: Some(endpoint),
            socket_thread: None,
            started_pid: None,
        }
    }

    fn member(&self) -> MysqlMemberIndex {
        self.inner.config().member_index()
    }
}

impl MysqlTopologyMemberRuntime for ScriptedMember {
    fn config(&self) -> &kuberic_mysql::service::MysqlInstanceConfig {
        self.inner.config()
    }

    fn initialize(&mut self) -> Result<(), kuberic_mysql::service::MysqlInstanceError> {
        let mut state = self.state.lock().unwrap();
        state.events.push(format!("initialize:{:?}", self.member()));
        if state.failure == ScriptFailure::Initialize(self.member()) {
            return Err(kuberic_mysql::service::MysqlInstanceError::Timeout(
                kuberic_mysql::service::LifecycleOperation::Initialization,
            ));
        }
        drop(state);
        self.inner.initialize()
    }

    fn start(&mut self) -> Result<(), kuberic_mysql::service::MysqlInstanceError> {
        self.state
            .lock()
            .unwrap()
            .events
            .push(format!("start:{:?}", self.member()));
        self.socket_thread = Some(provide_socket(
            self.inner.config().runtime().pid().to_owned(),
            self.inner.config().runtime().socket().to_owned(),
        ));
        self.inner.start()?;
        self.started_pid = Some(
            fs::read_to_string(self.inner.config().runtime().pid())
                .unwrap()
                .trim()
                .parse()
                .unwrap(),
        );
        Ok(())
    }

    fn topology_binding(
        &mut self,
        attempt: AttemptId,
        observer_generation: CredentialGeneration,
        recovery_generation: CredentialGeneration,
    ) -> Result<MemberControlBinding, MysqlTopologyError> {
        let member = self.member();
        Ok(MemberControlBinding::new(
            attempt,
            member,
            ProcessSessionId::new(format!("script-process-{}", member.as_usize())).unwrap(),
            EndpointBinding::new(format!("script-uds-{}", member.as_usize())).unwrap(),
            StorageBinding::new(format!("script-storage-{}", member.as_usize())).unwrap(),
            MemberAddress::new(self.inner.config().member().sql_address().to_string()).unwrap(),
            observer_generation,
            recovery_generation,
        ))
    }

    async fn provision_topology_accounts(
        &mut self,
        binding: MemberControlBinding,
        observer: &ControlCredential,
        recovery: &ControlCredential,
        _deadline: &NativeControlDeadline,
    ) -> Result<AccountProvisioningEvidence, MysqlTopologyError> {
        AccountProvisioningEvidence::new(
            binding,
            observer,
            recovery,
            AccountProvisioningEvidence::required_steps().to_vec(),
        )
    }

    async fn enroll_topology_identity(
        &mut self,
        binding: MemberControlBinding,
        accounts: &AccountProvisioningEvidence,
        _deadline: &NativeControlDeadline,
    ) -> Result<NativeIdentityEnrollment, MysqlTopologyError> {
        let member = self.member();
        let enrollment = NativeIdentityEnrollment::new(
            binding,
            ServerUuid::new(MEMBER_UUIDS[member.as_usize()]).unwrap(),
            accounts,
            true,
            GtidSet::empty(),
            NativeIdentityEnrollment::required_steps().to_vec(),
        )?;
        self.state.lock().unwrap().enrollments[member.as_usize()] = Some(enrollment.clone());
        Ok(enrollment)
    }

    async fn bootstrap_group_replication(
        &mut self,
        capability: kuberic_mysql::service::BootstrapCapability,
        _recovery: &ControlCredential,
        _deadline: &NativeControlDeadline,
    ) -> Result<BootstrapEffect, MysqlTopologyError> {
        let mut state = self.state.lock().unwrap();
        state.events.push("bootstrap".to_owned());
        if state.failure == ScriptFailure::Bootstrap {
            return Err(MysqlTopologyError::ControlProtocol(
                ControlStage::StartGroupReplication,
            ));
        }
        drop(state);
        capability.record_effect(BootstrapEffect::required_steps().to_vec())
    }

    async fn join_group_replication(
        &mut self,
        capability: kuberic_mysql::service::JoinCapability,
        _recovery: &ControlCredential,
        _deadline: &NativeControlDeadline,
    ) -> Result<JoinEffect, MysqlTopologyError> {
        let member = self.member();
        let mut state = self.state.lock().unwrap();
        state
            .boundary_attempts
            .push(capability.boundary().observation_attempt().clone());
        state.events.push(format!("join:{member:?}"));
        if state.failure == ScriptFailure::Join(member) {
            return Err(MysqlTopologyError::ControlProtocol(
                ControlStage::StartGroupReplication,
            ));
        }
        drop(state);
        capability.record_effect(JoinEffect::required_steps().to_vec())
    }

    async fn discover_topology_view(
        &mut self,
        enrollment: NativeIdentityEnrollment,
        group_name: GroupName,
        _deadline: &NativeControlDeadline,
    ) -> Result<ViewDiscovery, MysqlTopologyError> {
        let view = match self.member() {
            MysqlMemberIndex::First => "view-1",
            MysqlMemberIndex::Second => "view-2",
            MysqlMemberIndex::Third => "view-3",
        };
        ViewDiscovery::new(
            enrollment,
            group_name,
            ViewId::new(view).unwrap(),
            ViewDiscovery::required_steps().to_vec(),
        )
    }

    async fn observe<C: ObservationClock>(
        &mut self,
        request: ObservationRequest<C>,
    ) -> Result<ObservationReport, kuberic_mysql::service::MysqlInstanceError> {
        let plan = {
            let mut state = self.state.lock().unwrap();
            let plan = state.plans.pop_front().expect("unexpected observation");
            assert_eq!(plan.member, self.member());
            assert_eq!(request.binding().parts().view_id.as_str(), plan.view);
            state
                .attempts
                .push(request.binding().parts().attempt.clone());
            state.deadlines.push(request.clock().deadline());
            state.events.push(format!(
                "observe:{:?}:{}",
                self.member(),
                request.binding().parts().attempt.as_str()
            ));
            if state.failure == ScriptFailure::PostEffectObservation
                && self.member() == MysqlMemberIndex::First
            {
                return Err(kuberic_mysql::service::MysqlInstanceError::ObservationSocketMismatch);
            }
            if state.failure == ScriptFailure::ContextLoss {
                return Err(kuberic_mysql::service::MysqlInstanceError::Ownership(
                    OwnershipError::RootMismatch,
                ));
            }
            if state.failure == ScriptFailure::SourceBinding
                && state.events.iter().any(|event| event == "start:Second")
                && self.member() == MysqlMemberIndex::First
            {
                return Ok(unexpected_view_report(&request));
            }
            plan
        };
        Ok(valid_report(&request, &plan, &self.state))
    }

    fn contain(&mut self) -> Result<(), kuberic_mysql::service::MysqlInstanceError> {
        self.endpoint = None;
        let result = self.inner.contain();
        if let Some(thread) = self.socket_thread.take() {
            thread.join().unwrap();
        }
        result
    }
}

#[derive(Clone)]
struct ScriptClock {
    now: ObservationInstant,
    base: Instant,
}

impl ScriptClock {
    fn new(now: u64) -> Self {
        Self {
            now: ObservationInstant::new(now),
            base: Instant::now(),
        }
    }
}

impl ObservationClock for ScriptClock {
    fn now(&self) -> ObservationInstant {
        self.now
    }

    fn to_std_instant(&self, instant: ObservationInstant) -> Result<Instant, ClockError> {
        let delta = instant
            .tick()
            .checked_sub(self.now.tick())
            .ok_or(ClockError::UnrepresentableInstant)?;
        self.base
            .checked_add(Duration::from_secs(delta))
            .ok_or(ClockError::UnrepresentableInstant)
    }
}

fn observation_clock(now: u64, deadline: u64) -> ClockContext<ScriptClock> {
    ClockContext::new(ScriptClock::new(now), ObservationInstant::new(deadline)).unwrap()
}

fn observation_context(label: &str) -> TopologyObservationContext {
    TopologyObservationContext::new(
        ResourceId::new("resource").unwrap(),
        PartitionId::new("partition").unwrap(),
        ReplicaId::new("replica").unwrap(),
        ReplicaIncarnation::new("incarnation").unwrap(),
        ObservationSessionId::new(format!("session-{label}")).unwrap(),
        ConfigurationId::new("configuration").unwrap(),
        Epoch::new("epoch").unwrap(),
        AuthorityGeneration::new("authority").unwrap(),
        format!("script-{label}"),
    )
}

fn control_deadline(attempt: &TopologyAttempt) -> NativeControlDeadline {
    NativeControlDeadline::new(
        attempt.id().clone(),
        Instant::now() + Duration::from_secs(5),
    )
    .unwrap()
}

fn valid_report<C: ObservationClock>(
    request: &ObservationRequest<C>,
    plan: &ObservationPlan,
    state: &Arc<Mutex<ScriptState>>,
) -> ObservationReport {
    let enrollments = state.lock().unwrap().enrollments.clone();
    let members = plan
        .states
        .iter()
        .enumerate()
        .map(|(index, member_state)| {
            let enrollment = enrollments[index].as_ref().unwrap();
            NativeMember::new(
                enrollment.member_id().clone(),
                enrollment.binding().member_address().clone(),
                if index == 0 {
                    MemberRole::Primary
                } else {
                    MemberRole::Secondary
                },
                *member_state,
            )
        })
        .collect();
    let view = NativeView::new(
        request.binding().parts().group_name.clone(),
        request.binding().parts().view_id.clone(),
        members,
    )
    .unwrap();
    let snapshot = NativeSnapshot::new(
        view,
        NativeLocalState::new(
            GroupReplicationAddress::new("127.0.0.1:43061").unwrap(),
            NativeAccessState::new(NativeSwitch::Off, NativeSwitch::Off),
        ),
    );
    let binding = request.binding().clone();
    let start = request.clock().clock().now();
    let end = ObservationInstant::new(start.tick() + 1);
    let metadata = ObservationMetadata::new(
        binding.clone(),
        request.provenance().clone(),
        start,
        end,
        request.clock().deadline(),
        end,
    );
    let bracket = NativeObservationBracket::new(binding.clone(), snapshot);
    let outcome = NativeObservationDraft::new(metadata)
        .opening(bracket.clone())
        .executed(BoundGtidSet::new(
            binding,
            GtidSet::from_str(&format!("{GROUP_UUID}:{}", plan.history)).unwrap(),
        ))
        .closing(bracket)
        .finalize();
    assert!(outcome.valid().is_some(), "{outcome:?}");
    ObservationReport::new(outcome, AdapterDiagnostic::None)
}

fn unexpected_view_report<C: ObservationClock>(
    request: &ObservationRequest<C>,
) -> ObservationReport {
    let start = request.clock().clock().now();
    let metadata = ObservationMetadata::new(
        request.binding().clone(),
        request.provenance().clone(),
        start,
        start,
        request.clock().deadline(),
        start,
    );
    ObservationReport::new(
        NativeObservationDraft::new(metadata).finalize(),
        AdapterDiagnostic::UnexpectedIdentity {
            field: IdentityField::ViewId,
        },
    )
}

fn scripted_manager(
    label: &str,
    failure: ScriptFailure,
) -> (
    MysqlTopologyManager<ScriptedMember>,
    [TestRoot; 3],
    Arc<Mutex<ScriptState>>,
    TopologyAttempt,
) {
    let roots = [
        TestRoot::new(&format!("{label}-one")),
        TestRoot::new(&format!("{label}-two")),
        TestRoot::new(&format!("{label}-three")),
    ];
    let state = Arc::new(Mutex::new(ScriptState::new(failure)));
    let members = std::array::from_fn(|index| {
        ScriptedMember::new(
            MysqlInstanceManager::new(
                roots[index].config_for_member(MysqlMemberIndex::all_for_test()[index]),
            ),
            Arc::clone(&state),
        )
    });
    let attempt = attempt(&format!("script-{label}"), 100);
    let (observer, recovery) = credentials(&attempt);
    (
        MysqlTopologyManager::new(members, attempt.clone(), observer, recovery).unwrap(),
        roots,
        state,
        attempt,
    )
}

async fn accept_bootstrap_public(
    manager: &mut MysqlTopologyManager<ScriptedMember>,
    attempt: &TopologyAttempt,
) -> Result<(), MysqlTopologyManagerError> {
    manager.initialize()?;
    manager
        .start_designated_member(&control_deadline(attempt))
        .await?;
    manager
        .bootstrap(TopologyInstant::new(10), &control_deadline(attempt))
        .await?;
    let evaluation = manager
        .observe_pending(
            observation_context("bootstrap"),
            observation_clock(100, 110),
            TopologyInstant::new(20),
        )
        .await?;
    assert!(matches!(evaluation, TransitionEvaluation::Accepted(_)));
    Ok(())
}

async fn accept_second_public(
    manager: &mut MysqlTopologyManager<ScriptedMember>,
    attempt: &TopologyAttempt,
) -> Result<(), MysqlTopologyManagerError> {
    manager
        .start_second_member(&control_deadline(attempt))
        .await?;
    manager
        .join_second_member(
            TopologyInstant::new(30),
            &control_deadline(attempt),
            observation_context("source-second"),
            observation_clock(120, 130),
        )
        .await?;
    let pending = manager
        .observe_pending(
            observation_context("second-pending"),
            observation_clock(140, 150),
            TopologyInstant::new(40),
        )
        .await?;
    assert_eq!(pending, TransitionEvaluation::Pending);
    let accepted = manager
        .observe_pending(
            observation_context("second-online"),
            observation_clock(151, 160),
            TopologyInstant::new(41),
        )
        .await?;
    assert!(matches!(accepted, TransitionEvaluation::Accepted(_)));
    Ok(())
}

async fn accept_third_public(
    manager: &mut MysqlTopologyManager<ScriptedMember>,
    attempt: &TopologyAttempt,
) -> Result<(), MysqlTopologyManagerError> {
    manager
        .start_third_member(&control_deadline(attempt))
        .await?;
    manager
        .join_third_member(
            TopologyInstant::new(50),
            &control_deadline(attempt),
            observation_context("source-third"),
            observation_clock(170, 180),
        )
        .await?;
    let accepted = manager
        .observe_pending(
            observation_context("third-online"),
            observation_clock(190, 200),
            TopologyInstant::new(60),
        )
        .await?;
    assert!(matches!(accepted, TransitionEvaluation::Accepted(_)));
    Ok(())
}

fn assert_script_cleanup(manager: &MysqlTopologyManager<ScriptedMember>, roots: &[TestRoot; 3]) {
    for (index, member) in manager.members().iter().enumerate() {
        if let Some(pid) = member.started_pid {
            assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
        }
        assert!(!member.config().runtime().socket().exists());
        assert_endpoint_refuses(member.config().member().group_replication_address());
        if roots[index].data.exists() {
            assert!(roots[index].data.is_dir());
        }
        assert!(!roots[index].scratch.exists());
    }
}

#[tokio::test]
async fn public_manager_flow_uses_fresh_source_and_pending_observation_attempts() {
    let (mut manager, roots, state, attempt) =
        scripted_manager("public-happy", ScriptFailure::None);
    for member in manager.members() {
        TcpStream::connect(member.config().member().group_replication_address()).unwrap();
    }
    let foreign = roots[0].root.join("foreign-resource");
    fs::write(&foreign, "untouched").unwrap();

    accept_bootstrap_public(&mut manager, &attempt)
        .await
        .unwrap();
    accept_second_public(&mut manager, &attempt).await.unwrap();
    accept_third_public(&mut manager, &attempt).await.unwrap();
    assert_eq!(manager.state(), MysqlTopologyState::Complete);
    assert_eq!(
        manager
            .accepted_topology()
            .unwrap()
            .credit()
            .members()
            .len(),
        3
    );
    assert!(!manager.read_access_open());
    assert!(!manager.write_access_open());

    let state = state.lock().unwrap();
    assert_eq!(state.attempts.len(), 6);
    assert_eq!(
        state.attempts.iter().collect::<HashSet<_>>().len(),
        state.attempts.len()
    );
    assert_eq!(state.boundary_attempts.len(), 2);
    assert_eq!(state.boundary_attempts[0], state.attempts[1]);
    assert_eq!(state.boundary_attempts[1], state.attempts[4]);
    assert_ne!(state.attempts[2], state.attempts[3]);
    assert!(state.deadlines[3] > state.deadlines[2]);
    for (join, source) in [
        ("join:Second", "observe:First:"),
        ("join:Third", "observe:Second:"),
    ] {
        let join_index = state.events.iter().position(|event| event == join).unwrap();
        assert!(
            state.events[join_index - 1].starts_with(source),
            "{:?}",
            state.events
        );
    }
    drop(state);

    manager.stop().unwrap();
    assert_eq!(manager.state(), MysqlTopologyState::Stopped);
    assert_script_cleanup(&manager, &roots);
    assert_eq!(fs::read_to_string(foreign).unwrap(), "untouched");
    for root in &roots {
        let names = inventory(&root.root);
        for forbidden in [
            "metadata", "journal", "receipt", "cursor", "adoption", "store",
        ] {
            assert!(names.iter().all(|name| !name.contains(forbidden)));
        }
    }
}

#[tokio::test]
async fn public_manager_failures_invalidate_and_contain_every_allocated_endpoint() {
    for failure in [
        ScriptFailure::Initialize(MysqlMemberIndex::Second),
        ScriptFailure::Bootstrap,
        ScriptFailure::Join(MysqlMemberIndex::Second),
        ScriptFailure::Join(MysqlMemberIndex::Third),
        ScriptFailure::PostEffectObservation,
        ScriptFailure::SourceBinding,
        ScriptFailure::ContextLoss,
    ] {
        let (mut manager, roots, _state, attempt) =
            scripted_manager(&format!("failure-{failure:?}"), failure);
        let foreign = roots[0].root.join("foreign-resource");
        fs::write(&foreign, "untouched").unwrap();
        for member in manager.members() {
            TcpStream::connect(member.config().member().group_replication_address()).unwrap();
        }
        let result = async {
            accept_bootstrap_public(&mut manager, &attempt).await?;
            accept_second_public(&mut manager, &attempt).await?;
            accept_third_public(&mut manager, &attempt).await
        }
        .await;
        assert!(result.is_err(), "{failure:?}");
        assert_eq!(manager.state(), MysqlTopologyState::Failed, "{failure:?}");
        assert!(manager.accepted_topology().is_none(), "{failure:?}");
        assert!(!manager.read_access_open());
        assert!(!manager.write_access_open());
        assert_script_cleanup(&manager, &roots);
        assert_eq!(fs::read_to_string(foreign).unwrap(), "untouched");
        for root in &roots {
            let names = inventory(&root.root);
            for forbidden in [
                "metadata", "journal", "receipt", "cursor", "adoption", "store",
            ] {
                assert!(names.iter().all(|name| !name.contains(forbidden)));
            }
        }
    }
}

#[tokio::test]
async fn invalid_state_failure_invalidates_context_and_prevents_later_use() {
    let (mut manager, roots, _state, attempt) =
        scripted_manager("invalid-state", ScriptFailure::None);
    let error = manager
        .start_designated_member(&control_deadline(&attempt))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MysqlTopologyManagerError::InvalidState {
            expected: MysqlTopologyState::Initialized,
            actual: MysqlTopologyState::Configured,
        }
    ));
    assert_eq!(manager.state(), MysqlTopologyState::Failed);
    assert!(manager.initialize().is_err());
    assert_eq!(manager.state(), MysqlTopologyState::Failed);
    assert_script_cleanup(&manager, &roots);
}

#[tokio::test]
async fn pending_poll_reused_deadline_fails_closed_and_cannot_reuse_operation() {
    let (mut manager, roots, state, attempt) =
        scripted_manager("reused-deadline", ScriptFailure::None);
    accept_bootstrap_public(&mut manager, &attempt)
        .await
        .unwrap();
    manager
        .start_second_member(&control_deadline(&attempt))
        .await
        .unwrap();
    manager
        .join_second_member(
            TopologyInstant::new(30),
            &control_deadline(&attempt),
            observation_context("source-reused-deadline"),
            observation_clock(120, 130),
        )
        .await
        .unwrap();
    assert_eq!(
        manager
            .observe_pending(
                observation_context("pending-reused-deadline"),
                observation_clock(140, 150),
                TopologyInstant::new(40),
            )
            .await
            .unwrap(),
        TransitionEvaluation::Pending
    );
    let error = manager
        .observe_pending(
            observation_context("reused-deadline"),
            observation_clock(141, 150),
            TopologyInstant::new(41),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MysqlTopologyManagerError::Topology(MysqlTopologyError::Evidence(
            TopologyEvidenceError::BindingMismatch
        ))
    ));
    assert_eq!(manager.state(), MysqlTopologyState::Failed);
    assert!(manager.accepted_topology().is_none());
    assert_eq!(state.lock().unwrap().attempts.len(), 3);
    assert!(
        manager
            .observe_pending(
                observation_context("stale-operation"),
                observation_clock(151, 160),
                TopologyInstant::new(42),
            )
            .await
            .is_err()
    );
    assert_script_cleanup(&manager, &roots);
}

#[tokio::test]
async fn pending_poll_smaller_deadline_fails_closed_without_observation_credit() {
    let (mut manager, roots, state, attempt) =
        scripted_manager("regressed-deadline", ScriptFailure::None);
    accept_bootstrap_public(&mut manager, &attempt)
        .await
        .unwrap();
    manager
        .start_second_member(&control_deadline(&attempt))
        .await
        .unwrap();
    manager
        .join_second_member(
            TopologyInstant::new(30),
            &control_deadline(&attempt),
            observation_context("source-regressed-deadline"),
            observation_clock(120, 130),
        )
        .await
        .unwrap();
    assert_eq!(
        manager
            .observe_pending(
                observation_context("pending-regressed-deadline"),
                observation_clock(140, 150),
                TopologyInstant::new(40),
            )
            .await
            .unwrap(),
        TransitionEvaluation::Pending
    );

    let error = manager
        .observe_pending(
            observation_context("regressed-deadline"),
            observation_clock(141, 149),
            TopologyInstant::new(41),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        MysqlTopologyManagerError::Topology(MysqlTopologyError::Evidence(
            TopologyEvidenceError::BindingMismatch
        ))
    ));
    assert_eq!(manager.state(), MysqlTopologyState::Failed);
    assert!(manager.accepted_topology().is_none());
    assert_eq!(state.lock().unwrap().attempts.len(), 3);
    assert_script_cleanup(&manager, &roots);
}

#[tokio::test]
async fn pending_poll_deadline_a_b_a_reuse_fails_closed_without_observation_credit() {
    let (mut manager, roots, state, attempt) =
        scripted_manager("deadline-a-b-a", ScriptFailure::None);
    accept_bootstrap_public(&mut manager, &attempt)
        .await
        .unwrap();
    manager
        .start_second_member(&control_deadline(&attempt))
        .await
        .unwrap();
    manager
        .join_second_member(
            TopologyInstant::new(30),
            &control_deadline(&attempt),
            observation_context("source-deadline-a-b-a"),
            observation_clock(120, 130),
        )
        .await
        .unwrap();
    assert_eq!(
        manager
            .observe_pending(
                observation_context("pending-deadline-a"),
                observation_clock(140, 150),
                TopologyInstant::new(40),
            )
            .await
            .unwrap(),
        TransitionEvaluation::Pending
    );
    state.lock().unwrap().plans.front_mut().unwrap().states[1] = MemberState::Recovering;
    assert_eq!(
        manager
            .observe_pending(
                observation_context("pending-deadline-b"),
                observation_clock(141, 160),
                TopologyInstant::new(41),
            )
            .await
            .unwrap(),
        TransitionEvaluation::Pending
    );

    let error = manager
        .observe_pending(
            observation_context("reused-deadline-a"),
            observation_clock(142, 150),
            TopologyInstant::new(42),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        MysqlTopologyManagerError::Topology(MysqlTopologyError::Evidence(
            TopologyEvidenceError::BindingMismatch
        ))
    ));
    assert_eq!(manager.state(), MysqlTopologyState::Failed);
    assert!(manager.accepted_topology().is_none());
    assert_eq!(state.lock().unwrap().attempts.len(), 4);
    assert_script_cleanup(&manager, &roots);
}

#[tokio::test]
async fn fresh_source_observation_binding_failure_prevents_join_effect() {
    let (mut manager, roots, state, attempt) =
        scripted_manager("source-binding", ScriptFailure::SourceBinding);
    accept_bootstrap_public(&mut manager, &attempt)
        .await
        .unwrap();
    manager
        .start_second_member(&control_deadline(&attempt))
        .await
        .unwrap();
    let error = manager
        .join_second_member(
            TopologyInstant::new(30),
            &control_deadline(&attempt),
            observation_context("wrong-source-binding"),
            observation_clock(120, 130),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MysqlTopologyManagerError::Topology(MysqlTopologyError::Evidence(
            TopologyEvidenceError::BindingMismatch
        ))
    ));
    assert_eq!(manager.state(), MysqlTopologyState::Failed);
    assert!(
        state
            .lock()
            .unwrap()
            .events
            .iter()
            .all(|event| event != "join:Second")
    );
    assert_script_cleanup(&manager, &roots);
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
