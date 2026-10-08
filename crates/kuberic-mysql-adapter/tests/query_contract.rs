#![allow(dead_code)]

#[allow(dead_code)]
#[path = "../src/diagnostic.rs"]
mod diagnostic;
#[allow(dead_code)]
#[path = "../src/query.rs"]
mod query;

mod common;

use std::fs;
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use kuberic_mysql_adapter::{
    AdapterDiagnostic, ClockContext, ColumnKind, CoreOutcomeClass, NativeSurface, ObservationClock,
    ObservationRequest, ObservationStage, ObserverCredentials, QueryId, ServerErrorClass, SqlState,
    UnixSocketPath,
};
use kuberic_mysql_core::{
    AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, EndpointBinding, Epoch,
    ExactBinding, ExactBindingParts, GroupName, MemberAddress, MemberId, ObservationInstant,
    ObservationProvenance, ObservationSessionId, PartitionId, ProcessSessionId, ReplicaId,
    ReplicaIncarnation, ResourceId, ServerUuid, StorageBinding, ViewId,
};

#[test]
fn exact_versioned_queries_have_explicit_selected_contracts() {
    assert_eq!(QueryId::ALL.len(), 5);
    for query in QueryId::ALL {
        assert!(query.sql().starts_with("SELECT "));
        assert!(!query.sql().contains("SELECT *"));
        assert!(!query.columns().is_empty());
        assert!(
            query
                .columns()
                .iter()
                .all(|column| !column.name().is_empty())
        );
    }

    assert!(
        QueryId::Mysql8411GroupMembersV1
            .sql()
            .contains("performance_schema.replication_group_members")
    );
    assert!(
        !QueryId::Mysql8411GroupMembersV1
            .sql()
            .contains("replication_group_member_stats")
    );
    assert!(
        QueryId::Mysql8411LocalMemberStatsV1
            .sql()
            .contains("performance_schema.replication_group_member_stats")
    );
    assert!(
        !QueryId::Mysql8411LocalMemberStatsV1
            .sql()
            .contains("replication_group_members ORDER")
    );
    assert!(
        QueryId::Mysql8411GroupMembersV1
            .sql()
            .ends_with("ORDER BY MEMBER_ID")
    );
    assert_eq!(
        QueryId::Mysql8411ProductIdentityV1.sql(),
        "SELECT @@version AS version, @@version_comment AS version_comment, \
@@version_compile_machine AS version_compile_machine, \
@@version_compile_os AS version_compile_os, \
@@GLOBAL.server_uuid AS server_uuid"
    );
    assert_eq!(
        QueryId::Mysql8411LocalStateV1.sql(),
        "SELECT @@GLOBAL.group_replication_group_name AS group_name, \
@@GLOBAL.group_replication_local_address AS group_replication_address, \
@@GLOBAL.read_only AS read_only, \
@@GLOBAL.super_read_only AS super_read_only"
    );
    assert_eq!(
        QueryId::Mysql8411GroupMembersV1.sql(),
        "SELECT MEMBER_ID AS member_id, MEMBER_HOST AS member_host, \
MEMBER_PORT AS member_port, MEMBER_STATE AS member_state, MEMBER_ROLE AS member_role \
FROM performance_schema.replication_group_members ORDER BY MEMBER_ID"
    );
    assert_eq!(
        QueryId::Mysql8411LocalMemberStatsV1.sql(),
        "SELECT MEMBER_ID AS member_id, VIEW_ID AS view_id \
FROM performance_schema.replication_group_member_stats \
WHERE MEMBER_ID = @@GLOBAL.server_uuid"
    );
    assert_eq!(
        QueryId::Mysql8411ExecutedGtidsV1.sql(),
        "SELECT @@GLOBAL.gtid_executed AS gtid_executed"
    );

    let members = QueryId::Mysql8411GroupMembersV1.columns();
    assert_eq!(
        members
            .iter()
            .map(|column| (column.name(), column.kind(), column.nullable()))
            .collect::<Vec<_>>(),
        vec![
            ("member_id", ColumnKind::String, false),
            ("member_host", ColumnKind::String, false),
            ("member_port", ColumnKind::Long, true),
            ("member_state", ColumnKind::String, false),
            ("member_role", ColumnKind::String, false),
        ]
    );
    let stats = QueryId::Mysql8411LocalMemberStatsV1.columns();
    assert_eq!(
        stats
            .iter()
            .map(|column| (column.name(), column.kind(), column.nullable()))
            .collect::<Vec<_>>(),
        vec![
            ("member_id", ColumnKind::String, false),
            ("view_id", ColumnKind::String, false),
        ]
    );
}

#[test]
fn query_contract_has_immutable_oracle_source_provenance() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/mysql-8.4.11/query-contract-provenance.toml");
    let provenance = fs::read_to_string(path).unwrap();
    for required in [
        "kuberic.mysql.query-contract-provenance/v1",
        "99960bf74fa919347e4f4e3ca47672f333d6e91f",
        "table_replication_group_members.cc",
        "76f29b8caa0cb036866e1ab057b5f11d9249a2b441f62cf88d600483578d6f90",
        "table_replication_group_member_stats.cc",
        "54be38203e0c4bc0da9ee0c4d838dd5ef300534b8b5c6166e5406f1469fa601c",
    ] {
        assert!(provenance.contains(required));
    }
}

#[test]
fn every_fixture_declares_schema_outcome_diagnostic_and_eligible_origin() {
    let files = common::load_fixture_files();
    assert!(files.len() >= 3);
    let mut cases = 0;
    for file in files {
        assert_eq!(file.fixture_schema, "kuberic.mysql.native-result/v1");
        for case in file.cases {
            cases += 1;
            assert!(!case.scenario.is_empty());
            assert!(!case.expected.core_class.is_empty());
            assert!(!case.expected.diagnostic.is_empty());
            let query = case.query();
            if case.expected.diagnostic != "AdditionalColumns" {
                assert_eq!(case.columns.len(), query.columns().len());
            } else {
                assert!(case.columns.len() > query.columns().len());
            }
            match case.evidence_origin.category.as_str() {
                "native-required" => {
                    assert_eq!(
                        case.evidence_origin.qualification.as_deref(),
                        Some("required-live-8.4.11")
                    );
                }
                "oracle-owned" => {
                    assert_eq!(
                        case.evidence_origin.commit.as_deref(),
                        Some("99960bf74fa919347e4f4e3ca47672f333d6e91f")
                    );
                    assert_eq!(case.evidence_origin.tag.as_deref(), Some("mysql-8.4.11"));
                    assert!(case.evidence_origin.url.as_deref().is_some_and(|url| {
                        url.contains("99960bf74fa919347e4f4e3ca47672f333d6e91f")
                    }));
                    assert!(case.evidence_origin.path.as_deref().is_some_and(|path| {
                        path.starts_with("mysql-test/suite/group_replication/")
                    }));
                    assert_eq!(
                        case.evidence_origin.sha256.as_deref().map(str::len),
                        Some(64)
                    );
                    assert!(case.evidence_origin.range.is_some());
                    assert!(case.evidence_origin.derivation.is_some());
                }
                "synthetic" => assert!(case.evidence_origin.derivation.is_some()),
                other => panic!("unsupported evidence origin {other}"),
            }
        }
    }
    assert!(cases >= 20);
}

#[test]
fn permission_surfaces_authentication_and_unsupported_remain_distinct() {
    let members_denied = AdapterDiagnostic::Server {
        stage: ObservationStage::Query,
        surface: Some(NativeSurface::GroupMembers),
        class: ServerErrorClass::Permission,
        code: 1142,
        sql_state: SqlState::new("42000").unwrap(),
    };
    let stats_denied = AdapterDiagnostic::Server {
        stage: ObservationStage::Query,
        surface: Some(NativeSurface::LocalMemberStats),
        class: ServerErrorClass::Permission,
        code: 1142,
        sql_state: SqlState::new("42000").unwrap(),
    };
    let authentication = AdapterDiagnostic::Server {
        stage: ObservationStage::Authenticate,
        surface: None,
        class: ServerErrorClass::Authentication,
        code: 1045,
        sql_state: SqlState::new("28000").unwrap(),
    };
    let unsupported = AdapterDiagnostic::Server {
        stage: ObservationStage::Query,
        surface: Some(NativeSurface::LocalState),
        class: ServerErrorClass::Other,
        code: 1193,
        sql_state: SqlState::new("HY000").unwrap(),
    };

    assert_ne!(members_denied, stats_denied);
    assert_eq!(
        members_denied.core_class(),
        CoreOutcomeClass::PermissionDenied
    );
    assert_eq!(
        stats_denied.core_class(),
        CoreOutcomeClass::PermissionDenied
    );
    assert_eq!(
        authentication.core_class(),
        CoreOutcomeClass::AuthenticationFailure
    );
    assert!(matches!(
        unsupported.core_class(),
        CoreOutcomeClass::Unsupported(_)
    ));
}

#[test]
fn public_request_uses_only_one_validated_socket_and_redacts_secret() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/phase3-query-contract");
    fs::create_dir_all(&root).unwrap();
    let socket = root.join(format!("mysql-{}.sock", std::process::id()));
    let _ = fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket).unwrap();

    let credentials = ObserverCredentials::new("observer", "phase3-secret").unwrap();
    let debug = format!("{credentials:?}");
    assert!(!debug.contains("phase3-secret"));
    assert!(debug.contains("<redacted>"));

    let binding = binding();
    let clock = TestClock {
        tick: ObservationInstant::new(10),
        tick_origin: 10,
        runtime_origin: Instant::now(),
    };
    let request = ObservationRequest::new(
        binding.clone(),
        UnixSocketPath::new(&socket).unwrap(),
        credentials,
        ObservationProvenance::new("query-contract", binding.parts().attempt.clone()).unwrap(),
        ClockContext::new(clock, ObservationInstant::new(20)).unwrap(),
    )
    .unwrap();
    assert_eq!(request.socket().as_path(), socket);
    assert_eq!(request.binding(), &binding);

    drop(listener);
    fs::remove_file(socket).unwrap();
    fs::remove_dir(root).unwrap();
}

#[test]
fn socket_validation_rejects_relative_regular_and_symlink_targets() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/phase3-socket-validation");
    fs::create_dir_all(&root).unwrap();
    let regular = root.join("regular");
    let link = root.join("link");
    let _ = fs::remove_file(&link);
    fs::write(&regular, b"not a socket").unwrap();
    symlink(&regular, &link).unwrap();

    assert_eq!(
        UnixSocketPath::new("relative.sock").unwrap_err(),
        kuberic_mysql_adapter::SocketPathError::NotAbsolute
    );
    assert_eq!(
        UnixSocketPath::new(&regular).unwrap_err(),
        kuberic_mysql_adapter::SocketPathError::NotSocket
    );
    assert_eq!(
        UnixSocketPath::new(&link).unwrap_err(),
        kuberic_mysql_adapter::SocketPathError::Symlink
    );

    fs::remove_file(link).unwrap();
    fs::remove_file(regular).unwrap();
    fs::remove_dir(root).unwrap();
}

#[test]
fn public_types_do_not_name_client_runtime_row_protocol_or_secret_types() {
    let names = [
        std::any::type_name::<ObservationRequest<TestClock>>(),
        std::any::type_name::<kuberic_mysql_adapter::ObservationReport>(),
        std::any::type_name::<AdapterDiagnostic>(),
        std::any::type_name::<QueryId>(),
    ];
    for name in names {
        assert!(!name.contains("mysql_async"));
        assert!(!name.contains("tokio"));
        assert!(!name.contains("Row"));
        assert!(!name.contains("Value"));
        assert!(!name.contains("Secret"));
    }

    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ObservationRequest<TestClock>>();
    assert_send_sync::<kuberic_mysql_adapter::ObservationReport>();
}

#[derive(Debug)]
struct TestClock {
    tick: ObservationInstant,
    tick_origin: u64,
    runtime_origin: Instant,
}

impl ObservationClock for TestClock {
    fn now(&self) -> ObservationInstant {
        self.tick
    }

    fn to_std_instant(
        &self,
        instant: ObservationInstant,
    ) -> Result<Instant, kuberic_mysql_adapter::ClockError> {
        let delta = instant
            .tick()
            .checked_sub(self.tick_origin)
            .ok_or(kuberic_mysql_adapter::ClockError::UnrepresentableInstant)?;
        self.runtime_origin
            .checked_add(Duration::from_millis(delta))
            .ok_or(kuberic_mysql_adapter::ClockError::UnrepresentableInstant)
    }
}

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
