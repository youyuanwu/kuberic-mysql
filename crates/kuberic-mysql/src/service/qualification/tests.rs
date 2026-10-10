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
        [
            "gtid_mode=ON",
            "enforce_gtid_consistency=ON",
            "group_replication_gtid_assignment_block_size=1",
            "group_replication_view_change_uuid=AUTOMATIC",
            "group_replication_consistency=AFTER",
            "innodb_flush_log_at_trx_commit=1",
            "sync_binlog=1",
            "binlog_expire_logs_seconds=2592000",
            "group_replication_member_expel_timeout=5",
        ]
    );
    let qualified = complete_evidence().qualify(&binding()).unwrap();
    assert_eq!(qualified.binding, binding());
    assert_eq!(qualified.member_process_sessions.len(), 3);
}

#[test]
fn profile_rejects_wrong_binding_and_member_identity_cardinality() {
    let mut wrong_package = complete_evidence();
    wrong_package.binding.package = "mysql-community-server-core=8.4.12".to_owned();
    assert_eq!(
        wrong_package.qualify(&binding()),
        Err(ProfileEvidenceError::BindingMismatch)
    );

    let mut duplicate_member = complete_evidence();
    duplicate_member.members[2].server_uuid = duplicate_member.members[1].server_uuid.clone();
    assert_eq!(
        duplicate_member.qualify(&binding()),
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
            new_login: ProbeOutcome::Rejected,
            recovery_admission: ProbeOutcome::Rejected,
            established_participation: ProbeOutcome::Continued,
            predecessor_absence: ProbeOutcome::Unproved,
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
        new_login: ProbeOutcome::Rejected,
        recovery_admission: ProbeOutcome::Rejected,
        established_participation: ProbeOutcome::Absent,
        predecessor_absence: ProbeOutcome::Absent,
        replacement_admission: ProbeOutcome::Accepted,
        credential_bound_session_identity: false,
    };
    assert_eq!(
        stop_rejoin.verdict(),
        RevocationVerdict::ExactProcessBarrier
    );
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
        RevocationEvidence {
            candidate: RevocationCandidate::AccountLock,
            new_login: ProbeOutcome::Accepted,
            recovery_admission: ProbeOutcome::Accepted,
            established_participation: ProbeOutcome::Continued,
            predecessor_absence: ProbeOutcome::Unproved,
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
