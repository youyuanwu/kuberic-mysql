use std::str::FromStr;

use super::*;

const GROUP_UUID: &str = "d6f6f07e-37e1-4f7a-9105-73c13f634cb3";

fn binding() -> QualificationBinding {
    QualificationBinding {
        package: ORACLE_PACKAGE.to_owned(),
        attempt: "attempt-1".to_owned(),
        group: GROUP_UUID.to_owned(),
        opening_view: "1:3".to_owned(),
    }
}

fn complete_settings() -> Vec<SettingReadback> {
    NATIVE_PROFILE
        .iter()
        .map(|spec| SettingReadback::Value {
            variable: spec.variable.to_owned(),
            global: spec.expected.to_owned(),
            session: (spec.scope == NativeSettingScope::GlobalAndSession)
                .then(|| spec.expected.to_owned()),
        })
        .collect()
}

fn complete_evidence() -> NativeProfileEvidence {
    NativeProfileEvidence {
        binding: binding(),
        members: (1..=3)
            .map(|number| MemberProfileEvidence {
                server_uuid: format!("member-{number}"),
                process_session: format!("process-{number}"),
                settings: complete_settings(),
            })
            .collect(),
    }
}

#[test]
fn exact_profile_matrix_and_binding_qualify_all_three_members() {
    assert_eq!(NATIVE_PROFILE.len(), 9);
    assert_eq!(
        NATIVE_PROFILE
            .iter()
            .map(|spec| spec.option)
            .collect::<Vec<_>>(),
        TOPOLOGY_NATIVE_PROFILE_OPTIONS
    );
    assert!(NATIVE_PROFILE.iter().all(|spec| {
        spec.global_query.starts_with("SELECT @@GLOBAL.")
            && (spec.scope != NativeSettingScope::GlobalAndSession
                || spec.session_query == Some("SELECT @@SESSION.group_replication_consistency"))
    }));
    assert_eq!(
        NATIVE_PROFILE
            .iter()
            .map(|spec| spec.mutability)
            .collect::<Vec<_>>(),
        [
            NativeSettingMutability::RestrictedDynamicGlobal,
            NativeSettingMutability::RestrictedDynamicGlobal,
            NativeSettingMutability::GroupReboot,
            NativeSettingMutability::ReadOnlyGroupWide,
            NativeSettingMutability::DynamicGlobalAndSession,
            NativeSettingMutability::DynamicGlobal,
            NativeSettingMutability::DynamicGlobal,
            NativeSettingMutability::DynamicGlobal,
            NativeSettingMutability::DynamicGroupWide,
        ]
    );
    let qualified = complete_evidence().qualify(&binding()).unwrap();
    assert_eq!(qualified.binding, binding());
    assert_eq!(qualified.member_process_sessions.len(), 3);
}

#[test]
fn profile_rejects_wrong_binding_and_member_identity_cardinality() {
    let mutations: [fn(&mut QualificationBinding); 4] = [
        |binding: &mut QualificationBinding| {
            binding.package = "mysql-community-server-core=8.4.12".to_owned();
        },
        |binding: &mut QualificationBinding| binding.attempt = "attempt-2".to_owned(),
        |binding: &mut QualificationBinding| binding.group = "other-group".to_owned(),
        |binding: &mut QualificationBinding| binding.opening_view = "1:4".to_owned(),
    ];
    for mutate in mutations {
        let expected = binding();
        let mut evidence = complete_evidence();
        mutate(&mut evidence.binding);
        assert_eq!(
            evidence.qualify(&expected),
            Err(ProfileEvidenceError::BindingMismatch)
        );
    }

    let mut wrong_count = complete_evidence();
    wrong_count.members.pop();
    assert_eq!(
        wrong_count.qualify(&binding()),
        Err(ProfileEvidenceError::WrongMemberCount)
    );

    let mut duplicate_member = complete_evidence();
    duplicate_member.members[2].server_uuid = duplicate_member.members[1].server_uuid.clone();
    assert_eq!(
        duplicate_member.qualify(&binding()),
        Err(ProfileEvidenceError::DuplicateMember)
    );

    let mut duplicate_session = complete_evidence();
    duplicate_session.members[2].process_session =
        duplicate_session.members[1].process_session.clone();
    assert_eq!(
        duplicate_session.qualify(&binding()),
        Err(ProfileEvidenceError::DuplicateMember)
    );
}

#[test]
fn profile_rejects_missing_malformed_unsupported_and_mismatched_values() {
    let cases = [
        (
            SettingReadback::Missing {
                variable: "sync_binlog".to_owned(),
            },
            ProfileEvidenceError::Missing {
                member: "member-1".to_owned(),
                variable: "sync_binlog".to_owned(),
            },
        ),
        (
            SettingReadback::Malformed {
                variable: "sync_binlog".to_owned(),
            },
            ProfileEvidenceError::Malformed {
                member: "member-1".to_owned(),
                variable: "sync_binlog".to_owned(),
            },
        ),
        (
            SettingReadback::Unsupported {
                variable: "sync_binlog".to_owned(),
            },
            ProfileEvidenceError::Unsupported {
                member: "member-1".to_owned(),
                variable: "sync_binlog".to_owned(),
            },
        ),
        (
            SettingReadback::Value {
                variable: "sync_binlog".to_owned(),
                global: "0".to_owned(),
                session: None,
            },
            ProfileEvidenceError::Mismatch {
                member: "member-1".to_owned(),
                variable: "sync_binlog".to_owned(),
            },
        ),
    ];

    for (replacement, expected) in cases {
        let mut evidence = complete_evidence();
        let index = evidence.members[0]
            .settings
            .iter()
            .position(|setting| setting.variable() == "sync_binlog")
            .unwrap();
        evidence.members[0].settings[index] = replacement;
        assert_eq!(evidence.qualify(&binding()), Err(expected));
    }
}

#[test]
fn profile_requires_workload_session_consistency() {
    let mut evidence = complete_evidence();
    let consistency = evidence.members[0]
        .settings
        .iter_mut()
        .find(|setting| setting.variable() == "group_replication_consistency")
        .unwrap();
    *consistency = SettingReadback::Value {
        variable: "group_replication_consistency".to_owned(),
        global: "AFTER".to_owned(),
        session: Some("EVENTUAL".to_owned()),
    };
    assert_eq!(
        evidence.qualify(&binding()),
        Err(ProfileEvidenceError::Mismatch {
            member: "member-1".to_owned(),
            variable: "group_replication_consistency".to_owned(),
        })
    );
}

#[test]
fn gtid_profile_preserves_full_shape_and_rejects_every_incompatible_class() {
    assert_eq!(
        classify_gtid_profile(&GtidSet::empty(), GROUP_UUID),
        GtidProfileCompatibility::CompatibleEmpty
    );
    assert_eq!(
        classify_gtid_profile(
            &GtidSet::from_str(&format!("{GROUP_UUID}:1-8")).unwrap(),
            GROUP_UUID
        ),
        GtidProfileCompatibility::CompatibleContiguous { tail: 8 }
    );
    assert_eq!(
        classify_gtid_profile(
            &GtidSet::from_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1").unwrap(),
            GROUP_UUID
        ),
        GtidProfileCompatibility::ForeignSource
    );
    assert_eq!(
        classify_gtid_profile(
            &GtidSet::from_str(&format!("{GROUP_UUID}:tag:1")).unwrap(),
            GROUP_UUID
        ),
        GtidProfileCompatibility::TaggedSource
    );
    assert_eq!(
        classify_gtid_profile(
            &GtidSet::from_str(&format!(
                "{GROUP_UUID}:1,aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1"
            ))
            .unwrap(),
            GROUP_UUID
        ),
        GtidProfileCompatibility::MultipleSources
    );
    assert_eq!(
        classify_gtid_profile(
            &GtidSet::from_str(&format!("{GROUP_UUID}:1-2:4")).unwrap(),
            GROUP_UUID
        ),
        GtidProfileCompatibility::MultipleIntervals
    );
    assert_eq!(
        classify_gtid_profile(
            &GtidSet::from_str(&format!("{GROUP_UUID}:2-4")).unwrap(),
            GROUP_UUID
        ),
        GtidProfileCompatibility::DoesNotBeginAtOne
    );
}

#[test]
fn retained_history_keeps_purged_set_and_inventory_separate() {
    let evidence = RetainedHistoryEvidence {
        executed: GtidSet::from_str(&format!("{GROUP_UUID}:1-8")).unwrap(),
        purged: GtidSet::from_str(&format!("{GROUP_UUID}:1-3")).unwrap(),
        binary_logs: vec!["mysql-bin.000002".to_owned(), "mysql-bin.000003".to_owned()],
    };
    assert_eq!(evidence.validate_opaque_inventory(), Ok(()));

    let invalid = RetainedHistoryEvidence {
        executed: GtidSet::from_str(&format!("{GROUP_UUID}:1-3")).unwrap(),
        purged: GtidSet::from_str(&format!("{GROUP_UUID}:1-4")).unwrap(),
        binary_logs: vec!["mysql-bin.000002".to_owned()],
    };
    assert_eq!(
        invalid.validate_opaque_inventory(),
        Err(RetainedHistoryError::PurgedOutsideExecuted)
    );

    for (logs, expected) in [
        (Vec::new(), RetainedHistoryError::MissingInventory),
        (
            vec!["../mysql-bin.000001".to_owned()],
            RetainedHistoryError::MalformedInventory,
        ),
        (
            vec!["mysql-bin.000001".to_owned(), "mysql-bin.000001".to_owned()],
            RetainedHistoryError::DuplicateInventory,
        ),
    ] {
        let evidence = RetainedHistoryEvidence {
            executed: GtidSet::empty(),
            purged: GtidSet::empty(),
            binary_logs: logs,
        };
        assert_eq!(evidence.validate_opaque_inventory(), Err(expected));
    }
}

#[test]
fn revocation_never_confuses_login_denial_with_a_session_barrier() {
    for candidate in [
        RevocationCandidate::AccountLock,
        RevocationCandidate::CredentialReplacement,
        RevocationCandidate::PrivilegeRemoval,
    ] {
        let evidence = RevocationEvidence {
            candidate,
            binding: binding(),
            exact_member: "member-2".to_owned(),
            predecessor_process_session: "process-2".to_owned(),
            predecessor_credential_generation: "credential-1".to_owned(),
            replacement_credential_generation: "credential-2".to_owned(),
            opening_view: "1:3".to_owned(),
            successor_view: None,
            new_login: ProbeOutcome::Rejected,
            recovery_admission: ProbeOutcome::Rejected,
            established_participation: ProbeOutcome::Continued,
            predecessor_absence: ProbeOutcome::Unproved,
            absence_observed_before_replacement: false,
            replacement_admission: ProbeOutcome::Unproved,
            credential_bound_session_identity: false,
        };
        assert_eq!(
            evidence.verdict(),
            RevocationVerdict::SessionIdentityUnproved
        );
    }

    let stop_rejoin = RevocationEvidence {
        candidate: RevocationCandidate::StopAndRejoin,
        binding: binding(),
        exact_member: "member-2".to_owned(),
        predecessor_process_session: "process-2".to_owned(),
        predecessor_credential_generation: "credential-1".to_owned(),
        replacement_credential_generation: "credential-2".to_owned(),
        opening_view: "1:3".to_owned(),
        successor_view: Some("1:4".to_owned()),
        new_login: ProbeOutcome::Rejected,
        recovery_admission: ProbeOutcome::Rejected,
        established_participation: ProbeOutcome::Absent,
        predecessor_absence: ProbeOutcome::Absent,
        absence_observed_before_replacement: true,
        replacement_admission: ProbeOutcome::Accepted,
        credential_bound_session_identity: false,
    };
    assert_eq!(
        stop_rejoin.verdict(),
        RevocationVerdict::ExactProcessBarrier
    );

    let invalidations: [fn(&mut RevocationEvidence); 10] = [
        |evidence: &mut RevocationEvidence| evidence.binding.package = "other".to_owned(),
        |evidence: &mut RevocationEvidence| evidence.binding.attempt.clear(),
        |evidence: &mut RevocationEvidence| evidence.binding.group.clear(),
        |evidence: &mut RevocationEvidence| evidence.exact_member.clear(),
        |evidence: &mut RevocationEvidence| evidence.predecessor_process_session.clear(),
        |evidence: &mut RevocationEvidence| {
            evidence.replacement_credential_generation =
                evidence.predecessor_credential_generation.clone();
        },
        |evidence: &mut RevocationEvidence| evidence.successor_view = None,
        |evidence: &mut RevocationEvidence| {
            evidence.absence_observed_before_replacement = false;
        },
        |evidence: &mut RevocationEvidence| {
            evidence.recovery_admission = ProbeOutcome::Accepted;
        },
        |evidence: &mut RevocationEvidence| {
            evidence.established_participation = ProbeOutcome::Continued;
        },
    ];
    for invalidate in invalidations {
        let mut evidence = stop_rejoin.clone();
        invalidate(&mut evidence);
        assert_ne!(evidence.verdict(), RevocationVerdict::ExactProcessBarrier);
    }
}

#[test]
fn result_enums_keep_outcomes_and_diagnostics_distinct_and_secret_free() {
    assert_ne!(ClientOutcome::Acknowledged, ClientOutcome::ServerRejected);
    assert_ne!(
        ClientOutcome::DisconnectedAfterDispatch,
        ClientOutcome::DeadlineExpired
    );
    assert_ne!(ProbeOutcome::Accepted, ProbeOutcome::Rejected);
    assert_eq!(
        TransactionOutcomeEvidence {
            client: ClientOutcome::Acknowledged,
            exact_gtid_bound: true,
            inclusion: InclusionEvidence::PresentOnEveryRequiredMember,
        }
        .verdict(),
        TransactionVerdict::AcknowledgedDurable
    );
    assert_eq!(
        TransactionOutcomeEvidence {
            client: ClientOutcome::Acknowledged,
            exact_gtid_bound: false,
            inclusion: InclusionEvidence::IncompleteOrIncoherent,
        }
        .verdict(),
        TransactionVerdict::IncompatibleAcknowledgement
    );
    for (inclusion, verdict) in [
        (
            InclusionEvidence::PresentOnEveryRequiredMember,
            TransactionVerdict::AmbiguousIncluded,
        ),
        (
            InclusionEvidence::AbsentFromCoherentRequiredHistory,
            TransactionVerdict::AmbiguousAbsent,
        ),
        (
            InclusionEvidence::IncompleteOrIncoherent,
            TransactionVerdict::AmbiguousUnresolved,
        ),
    ] {
        assert_eq!(
            TransactionOutcomeEvidence {
                client: ClientOutcome::DisconnectedAfterDispatch,
                exact_gtid_bound: true,
                inclusion,
            }
            .verdict(),
            verdict
        );
    }
    assert_eq!(
        TransactionOutcomeEvidence {
            client: ClientOutcome::DeadlineExpired,
            exact_gtid_bound: false,
            inclusion: InclusionEvidence::PresentOnEveryRequiredMember,
        }
        .verdict(),
        TransactionVerdict::AmbiguousUnresolved
    );
    assert_eq!(
        TransactionOutcomeEvidence {
            client: ClientOutcome::ServerRejected,
            exact_gtid_bound: false,
            inclusion: InclusionEvidence::IncompleteOrIncoherent,
        }
        .verdict(),
        TransactionVerdict::Rejected
    );
    assert_eq!(
        RevocationEvidence {
            candidate: RevocationCandidate::AccountLock,
            binding: binding(),
            exact_member: "member-1".to_owned(),
            predecessor_process_session: "process-1".to_owned(),
            predecessor_credential_generation: "credential-1".to_owned(),
            replacement_credential_generation: "credential-2".to_owned(),
            opening_view: "1:3".to_owned(),
            successor_view: None,
            new_login: ProbeOutcome::Accepted,
            recovery_admission: ProbeOutcome::Accepted,
            established_participation: ProbeOutcome::Continued,
            predecessor_absence: ProbeOutcome::Unproved,
            absence_observed_before_replacement: false,
            replacement_admission: ProbeOutcome::Unproved,
            credential_bound_session_identity: true,
        }
        .verdict(),
        RevocationVerdict::Insufficient
    );

    let diagnostic = QualificationDiagnostic {
        stage: QualificationStage::ProfileReadback,
        member: Some("member-1".to_owned()),
        category: "setting-mismatch",
    };
    let rendered = format!("{diagnostic:?}");
    assert!(!rendered.contains("KmsRecoverySecret"));

    for stage in [
        QualificationStage::ProfileReadback,
        QualificationStage::GtidCheckpoint,
        QualificationStage::RetainedHistory,
        QualificationStage::Revocation,
        QualificationStage::Transaction,
        QualificationStage::Cleanup,
    ] {
        assert!(!format!("{stage:?}").is_empty());
    }
}
