#![allow(dead_code)]

#[path = "../src/decode.rs"]
mod decode;
#[path = "../src/diagnostic.rs"]
mod diagnostic;
#[path = "../src/query.rs"]
mod query;

mod common;

use decode::{MembershipEvidence, ViewEvidence};
use diagnostic::{
    AdapterDiagnostic, AmbiguityKind, CoreOutcomeClass, EvidenceIssue, NativeSurface,
    PlaceholderKind, ProductIssue, SchemaIssue,
};
use kuberic_mysql_core::{
    AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, EndpointBinding, Epoch,
    ExactBinding, ExactBindingParts, GroupName, MemberAddress, MemberId, ObservationSessionId,
    PartitionId, ProcessSessionId, ReplicaId, ReplicaIncarnation, ResourceId, ServerUuid,
    StorageBinding, ViewId,
};

fn binding() -> ExactBinding {
    ExactBinding::new(ExactBindingParts {
        resource: ResourceId::new("resource").unwrap(),
        partition: PartitionId::new("partition").unwrap(),
        replica: ReplicaId::new("replica").unwrap(),
        incarnation: ReplicaIncarnation::new("incarnation").unwrap(),
        process_session: ProcessSessionId::new("process").unwrap(),
        observation_session: ObservationSessionId::new("observation").unwrap(),
        attempt: AttemptId::new("attempt").unwrap(),
        endpoint: EndpointBinding::new("endpoint").unwrap(),
        storage: StorageBinding::new("storage").unwrap(),
        server_uuid: ServerUuid::new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
        group_name: GroupName::new("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
        member_id: MemberId::new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
        member_address: MemberAddress::new("mysql-a:3306").unwrap(),
        configuration: ConfigurationId::new("configuration").unwrap(),
        epoch: Epoch::new("epoch").unwrap(),
        authority_generation: AuthorityGeneration::new("authority").unwrap(),
        view_id: ViewId::new("view-0001").unwrap(),
        credential_generation: CredentialGeneration::new("credential").unwrap(),
    })
}

#[test]
fn exact_product_and_online_snapshot_decode_into_core_types() {
    let binding = binding();
    let product = decode::decode_product(&common::fixture("oracle-community-8.4.11-product").raw())
        .expect("qualified product");
    decode::validate_server_identity(&binding, &product).expect("matching server");
    let local = decode::decode_local_state(&common::fixture("online-local-state").raw())
        .expect("local state");
    let members =
        decode::decode_members(&common::fixture("online-members").raw()).expect("members");
    let view =
        decode::decode_view(&common::fixture("online-local-view").raw()).expect("local view");
    let snapshot =
        decode::assemble_snapshot(&binding, local, members, view).expect("native snapshot");

    assert_eq!(snapshot.view().members().len(), 2);
    assert_eq!(
        snapshot.local().group_replication_address().as_str(),
        "127.0.0.1:33061"
    );
    assert!(!snapshot.local().access().read_only().is_on());
    assert!(!snapshot.local().access().super_read_only().is_on());
}

#[test]
fn empty_gtid_is_present_and_native_forms_are_decoded() {
    let empty = decode::decode_executed_gtids(&common::fixture("valid-empty-executed-gtids").raw())
        .expect("valid empty GTID set");
    assert_eq!(empty, kuberic_mysql_core::GtidSet::empty());

    let native =
        decode::decode_executed_gtids(&common::fixture("native-tagged-and-newline-gtids").raw())
            .expect("qualified native GTID formatting");
    assert_ne!(native, kuberic_mysql_core::GtidSet::empty());
    assert_eq!(native.entries().len(), 4);
}

#[test]
fn absence_and_oracle_placeholders_remain_distinct() {
    assert_eq!(
        decode::decode_members(&common::fixture("plugin-absent-members").raw()).unwrap(),
        MembershipEvidence::Absent
    );
    assert_eq!(
        decode::decode_members(&common::fixture("never-started-members-placeholder").raw())
            .unwrap(),
        MembershipEvidence::Placeholder(PlaceholderKind::NeverStarted)
    );
    assert_eq!(
        decode::decode_members(&common::fixture("stopped-members-placeholder").raw()).unwrap(),
        MembershipEvidence::Placeholder(PlaceholderKind::Stopped)
    );
    assert_eq!(
        decode::decode_view(&common::fixture("never-started-filtered-stats-absent").raw()).unwrap(),
        ViewEvidence::Absent
    );
    assert!(matches!(
        decode::decode_members(&common::fixture("recovering-member").raw()).unwrap(),
        MembershipEvidence::Active(_)
    ));
}

#[test]
fn null_incomplete_schema_and_switch_failures_are_typed() {
    let null = decode::decode_executed_gtids(&common::fixture("unexpected-required-null").raw())
        .unwrap_err();
    assert!(matches!(
        null.diagnostic,
        AdapterDiagnostic::Evidence {
            issue: EvidenceIssue::RequiredNull {
                column: "gtid_executed"
            },
            ..
        }
    ));
    assert!(matches!(
        null.diagnostic.core_class(),
        CoreOutcomeClass::Malformed(_)
    ));

    let incomplete =
        decode::decode_members(&common::fixture("incomplete-members-row").raw()).unwrap_err();
    assert!(matches!(
        incomplete.diagnostic,
        AdapterDiagnostic::Evidence {
            issue: EvidenceIssue::IncompleteRow { row: 0 },
            ..
        }
    ));

    let drift =
        decode::decode_members(&common::fixture("members-schema-type-drift").raw()).unwrap_err();
    assert!(matches!(
        drift.diagnostic,
        AdapterDiagnostic::Schema {
            issue: SchemaIssue::ChangedType { index: 0, .. },
            ..
        }
    ));

    let switch = decode::decode_local_state(&common::fixture("malformed-read-only-switch").raw())
        .unwrap_err();
    assert!(matches!(
        switch.diagnostic,
        AdapterDiagnostic::Evidence {
            issue: EvidenceIssue::MalformedCell {
                column: "read_only"
            },
            ..
        }
    ));
}

#[test]
fn duplicates_missing_local_and_contradictions_never_select_arbitrary_rows() {
    let binding = binding();
    let local =
        || decode::decode_local_state(&common::fixture("online-local-state").raw()).unwrap();
    let view = || decode::decode_view(&common::fixture("online-local-view").raw()).unwrap();

    for (scenario, expected) in [
        ("duplicate-member-id", AmbiguityKind::DuplicateMemberId),
        (
            "duplicate-member-address",
            AmbiguityKind::DuplicateMemberAddress,
        ),
    ] {
        let error = decode::decode_members(&common::fixture(scenario).raw())
            .expect_err("duplicate membership");
        assert_eq!(
            error.diagnostic,
            AdapterDiagnostic::Ambiguous {
                surface: NativeSurface::GroupMembers,
                kind: expected
            }
        );
    }

    let duplicate_local =
        decode::decode_view(&common::fixture("duplicate-local-stats-row").raw()).unwrap_err();
    assert_eq!(
        duplicate_local.diagnostic,
        AdapterDiagnostic::Ambiguous {
            surface: NativeSurface::LocalMemberStats,
            kind: AmbiguityKind::DuplicateLocalRow
        }
    );

    let recovering = decode::decode_members(&common::fixture("recovering-member").raw()).unwrap();
    let missing = decode::assemble_snapshot(&binding, local(), recovering, view())
        .expect_err("missing local");
    assert!(matches!(
        missing.diagnostic,
        AdapterDiagnostic::Ambiguous {
            kind: AmbiguityKind::MissingLocalMember,
            ..
        }
    ));

    let mut wrong_group = local();
    wrong_group.group_name = GroupName::new("different-group").unwrap();
    let online = decode::decode_members(&common::fixture("online-members").raw()).unwrap();
    let contradiction = decode::assemble_snapshot(&binding, wrong_group, online, view())
        .expect_err("group contradiction");
    assert!(matches!(
        contradiction.diagnostic,
        AdapterDiagnostic::UnexpectedIdentity { .. }
    ));

    let online = decode::decode_members(&common::fixture("online-members").raw()).unwrap();
    let wrong_view = ViewEvidence::Active {
        member_id: binding.parts().member_id.clone(),
        view_id: ViewId::new("view-contradiction").unwrap(),
    };
    let contradiction = decode::assemble_snapshot(&binding, local(), online, wrong_view)
        .expect_err("view contradiction");
    assert!(matches!(
        contradiction.diagnostic,
        AdapterDiagnostic::UnexpectedIdentity { .. }
    ));
}

#[test]
fn exact_product_classification_rejects_other_patch_and_lookalike() {
    let patch = decode::decode_product(&common::fixture("other-oracle-patch").raw()).unwrap_err();
    assert_eq!(
        patch.diagnostic,
        AdapterDiagnostic::Product(ProductIssue::UnsupportedPatch)
    );
    let fork = decode::decode_product(&common::fixture("non-oracle-lookalike").raw()).unwrap_err();
    assert_eq!(
        fork.diagnostic,
        AdapterDiagnostic::Product(ProductIssue::NonOracleCommunity)
    );
}

#[test]
fn future_values_schema_and_numeric_boundaries_are_explicit() {
    let role = decode::decode_members(&common::fixture("future-member-role").raw()).unwrap_err();
    assert!(matches!(
        role.diagnostic,
        AdapterDiagnostic::Evidence {
            issue: EvidenceIssue::UnsupportedNativeValue {
                column: "member_role"
            },
            ..
        }
    ));

    let schema =
        decode::decode_view(&common::fixture("future-additional-column").raw()).unwrap_err();
    assert!(matches!(
        schema.diagnostic,
        AdapterDiagnostic::Schema {
            issue: SchemaIssue::AdditionalColumns { .. },
            ..
        }
    ));

    let empty =
        decode::decode_local_state(&common::fixture("invalid-empty-group-name").raw()).unwrap_err();
    assert!(matches!(
        empty.diagnostic,
        AdapterDiagnostic::Evidence {
            issue: EvidenceIssue::EmptyRequiredString {
                column: "group_name"
            },
            ..
        }
    ));

    for scenario in [
        "gtid-boundary-9223372036854775806",
        "gtid-boundary-9223372036854775807",
    ] {
        decode::decode_executed_gtids(&common::fixture(scenario).raw())
            .expect("core retains both values pending native qualification");
    }
}

#[test]
fn stopped_placeholder_requires_valid_retained_identity_host_and_port() {
    for (scenario, column) in [
        ("stopped-placeholder-invalid-member-id", "member_id"),
        ("stopped-placeholder-invalid-host", "member_host"),
        ("stopped-placeholder-invalid-port", "member_port"),
    ] {
        let error = decode::decode_members(&common::fixture(scenario).raw())
            .expect_err("malformed retained stopped fact");
        assert!(matches!(
            error.diagnostic,
            AdapterDiagnostic::Evidence {
                surface: NativeSurface::GroupMembers,
                issue: EvidenceIssue::MalformedCell { column: actual }
                    | EvidenceIssue::EmptyRequiredString { column: actual },
            } if actual == column
        ));
    }
}
