#![allow(dead_code)]

#[allow(dead_code)]
#[path = "../src/decode.rs"]
mod decode;
#[allow(dead_code)]
#[path = "../src/diagnostic.rs"]
mod diagnostic;
#[allow(dead_code)]
#[path = "../src/query.rs"]
mod query;
#[allow(dead_code)]
#[path = "../src/request.rs"]
mod request;
#[allow(dead_code)]
#[path = "../src/session.rs"]
mod session;
#[allow(dead_code)]
#[path = "../src/time.rs"]
mod time;

mod common;

use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use kuberic_mysql_core::{
    AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, EndpointBinding, Epoch,
    ExactBinding, ExactBindingParts, GroupName, MemberAddress, MemberId, ObservationInstant,
    ObservationProvenance, ObservationSessionId, PartitionId, ProcessSessionId, ReplicaId,
    ReplicaIncarnation, ResourceId, ServerUuid, StorageBinding, ViewId,
};
use mysql_async::{Error, IoError, ServerError};

use decode::{MembershipEvidence, ViewEvidence};
use diagnostic::{
    AdapterDiagnostic, CoreOutcomeClass, NativeSurface, ObservationStage, PlaceholderKind,
    ServerErrorClass, SqlState,
};
use query::{ColumnKind, QueryId};
use request::{ObservationRequest, ObserverCredentials, SocketPathError, UnixSocketPath};
use time::{ClockContext, ClockError, ObservationClock};

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
    let product = QueryId::Mysql8411ProductIdentityV1.columns();
    assert!(product.iter().all(|column| column.nullable()));
    let local = QueryId::Mysql8411LocalStateV1.columns();
    assert!(local.iter().all(|column| column.nullable()));
    assert_eq!(
        QueryId::Mysql8411ExecutedGtidsV1
            .columns()
            .iter()
            .map(|column| (column.name(), column.kind(), column.nullable()))
            .collect::<Vec<_>>(),
        vec![("gtid_executed", ColumnKind::VarString, true)]
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
        "Item_func_get_system_var::resolve_type",
        "686f156c15faa979bff76251efd209a2eab9998e35e4fd226e117e1a43ec019a",
        "Item::set_data_type_string",
        "ce20d81c16ff8f9b0492cc05e59ecc7aaef1d19179d0260e378ff2f6724b3c69",
        "Sys_var_gtid_executed",
        "728211abe20c7d250e0029a282c219d7133eaf8522ad086c3e667a21d1508958",
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
            case.validate_input().expect("exclusive fixture input");
            common::validate_required_origin(&case).expect("scenario-specific fixture origin");
            let query = case.query();
            if case.error.is_none() {
                if case.expected.diagnostic != "AdditionalColumns" {
                    assert_eq!(case.columns.len(), query.columns().len());
                } else {
                    assert!(case.columns.len() > query.columns().len());
                }
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
fn every_fixture_expectation_is_executable_and_scenario_specific() {
    for file in common::load_fixture_files() {
        for case in file.cases {
            let diagnostic = execute_fixture(&case);
            assert_eq!(
                core_class_name(diagnostic.core_class()),
                case.expected.core_class,
                "core class for {}",
                case.scenario
            );
            assert_eq!(
                diagnostic_name(&diagnostic),
                case.expected.diagnostic,
                "diagnostic for {}",
                case.scenario
            );
        }
    }
}

#[test]
fn named_scenarios_reject_the_wrong_origin_category() {
    for scenario in [
        "oracle-community-8.4.11-product",
        "stopped-members-placeholder",
        "members-schema-type-drift",
    ] {
        let mut case = common::fixture(scenario);
        case.evidence_origin.category = "wrong-origin".to_owned();
        assert!(
            common::validate_required_origin(&case).is_err(),
            "{scenario} accepted the wrong origin"
        );
    }
}

#[test]
fn public_request_uses_only_one_validated_socket_and_redacts_secret() {
    let root = std::env::temp_dir().join(format!("km-qc-{}", std::process::id()));
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
    let root = std::env::temp_dir().join(format!("km-qv-{}", std::process::id()));
    fs::create_dir_all(&root).unwrap();
    let regular = root.join("regular");
    let link = root.join("link");
    let _ = fs::remove_file(&link);
    fs::write(&regular, b"not a socket").unwrap();
    symlink(&regular, &link).unwrap();

    assert_eq!(
        UnixSocketPath::new("relative.sock").unwrap_err(),
        SocketPathError::NotAbsolute
    );
    assert_eq!(
        UnixSocketPath::new(&regular).unwrap_err(),
        SocketPathError::NotSocket
    );
    assert_eq!(
        UnixSocketPath::new(&link).unwrap_err(),
        SocketPathError::Symlink
    );

    fs::remove_file(link).unwrap();
    fs::remove_file(regular).unwrap();
    fs::remove_dir(root).unwrap();
}

#[test]
fn socket_validation_rejects_non_utf8_paths_without_lossy_target_change() {
    let root = std::env::temp_dir().join(format!("km-qn-{}", std::process::id()));
    fs::create_dir_all(&root).unwrap();
    let mut name = b"mysql-".to_vec();
    name.push(0xff);
    name.extend_from_slice(b".sock");
    let socket = root.join(OsString::from_vec(name));
    let listener = UnixListener::bind(&socket).unwrap();

    assert_eq!(
        UnixSocketPath::new(&socket).unwrap_err(),
        SocketPathError::NonUtf8
    );

    drop(listener);
    fs::remove_file(socket).unwrap();
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

    type PublicRequestResult = Result<
        kuberic_mysql_adapter::ObservationRequest<PublicTestClock>,
        kuberic_mysql_adapter::RequestError,
    >;
    type PublicRequestConstructor = fn(
        ExactBinding,
        kuberic_mysql_adapter::UnixSocketPath,
        kuberic_mysql_adapter::ObserverCredentials,
        ObservationProvenance,
        kuberic_mysql_adapter::ClockContext<PublicTestClock>,
    ) -> PublicRequestResult;
    const _: PublicRequestConstructor =
        kuberic_mysql_adapter::ObservationRequest::<PublicTestClock>::new;
    const _: fn(
        kuberic_mysql_core::ObservationOutcome,
        kuberic_mysql_adapter::AdapterDiagnostic,
    ) -> kuberic_mysql_adapter::ObservationReport = kuberic_mysql_adapter::ObservationReport::new;
    fn assert_report_parts(
        report: kuberic_mysql_adapter::ObservationReport,
    ) -> (
        kuberic_mysql_core::ObservationOutcome,
        kuberic_mysql_adapter::AdapterDiagnostic,
    ) {
        report.into_parts()
    }
    let _ = assert_report_parts;
}

fn execute_fixture(case: &common::FixtureCase) -> AdapterDiagnostic {
    if let Some(error) = &case.error {
        let stage = parse_stage(&error.stage);
        let surface = error.surface.as_deref().map(parse_surface);
        let client_error = match error.kind.as_str() {
            "transport" => Error::Io(IoError::Io(io::Error::from(
                io::ErrorKind::ConnectionRefused,
            ))),
            "server" => Error::Server(ServerError {
                code: error.code.expect("server code"),
                message: "fixture server error".to_owned(),
                state: error.sql_state.clone().expect("server SQLSTATE"),
            }),
            other => panic!("unknown fixture error kind {other}"),
        };
        return session::SessionError::from_client(client_error, stage, surface).diagnostic();
    }

    let result = case.raw();
    let decoded = match case.query() {
        QueryId::Mysql8411ProductIdentityV1 => decode::decode_product(&result).map(|_| None),
        QueryId::Mysql8411LocalStateV1 => decode::decode_local_state(&result).map(|_| None),
        QueryId::Mysql8411GroupMembersV1 => {
            decode::decode_members(&result).map(|evidence| match evidence {
                MembershipEvidence::Absent => Some(AdapterDiagnostic::Absent {
                    surface: NativeSurface::GroupMembers,
                }),
                MembershipEvidence::Placeholder(kind) => Some(AdapterDiagnostic::Placeholder(kind)),
                MembershipEvidence::Active(_) => None,
            })
        }
        QueryId::Mysql8411LocalMemberStatsV1 => {
            decode::decode_view(&result).map(|evidence| match evidence {
                ViewEvidence::Absent => Some(AdapterDiagnostic::Absent {
                    surface: NativeSurface::LocalMemberStats,
                }),
                ViewEvidence::Placeholder(kind) => Some(AdapterDiagnostic::Placeholder(kind)),
                ViewEvidence::Active { .. } => None,
            })
        }
        QueryId::Mysql8411ExecutedGtidsV1 => decode::decode_executed_gtids(&result).map(|_| None),
    };
    match decoded {
        Ok(Some(diagnostic)) => diagnostic,
        Ok(None) => AdapterDiagnostic::None,
        Err(error) => error.diagnostic,
    }
}

fn parse_stage(value: &str) -> ObservationStage {
    match value {
        "Connect" => ObservationStage::Connect,
        "Authenticate" => ObservationStage::Authenticate,
        "Query" => ObservationStage::Query,
        "Consume" => ObservationStage::Consume,
        "Disconnect" => ObservationStage::Disconnect,
        other => panic!("unknown observation stage {other}"),
    }
}

fn parse_surface(value: &str) -> NativeSurface {
    match value {
        "ProductIdentity" => NativeSurface::ProductIdentity,
        "LocalState" => NativeSurface::LocalState,
        "GroupMembers" => NativeSurface::GroupMembers,
        "LocalMemberStats" => NativeSurface::LocalMemberStats,
        "ExecutedGtids" => NativeSurface::ExecutedGtids,
        other => panic!("unknown native surface {other}"),
    }
}

fn core_class_name(class: CoreOutcomeClass) -> &'static str {
    match class {
        CoreOutcomeClass::Valid => "Valid",
        CoreOutcomeClass::Absent => "Absent",
        CoreOutcomeClass::Unreachable => "Unreachable",
        CoreOutcomeClass::AuthenticationFailure => "AuthenticationFailure",
        CoreOutcomeClass::PermissionDenied => "PermissionDenied",
        CoreOutcomeClass::Partial => "Partial",
        CoreOutcomeClass::Stale(_) => "Stale",
        CoreOutcomeClass::FutureDated => "FutureDated",
        CoreOutcomeClass::Malformed(_) => "Malformed",
        CoreOutcomeClass::Unsupported(_) => "Unsupported",
        CoreOutcomeClass::Incoherent(_) => "Incoherent",
    }
}

fn diagnostic_name(diagnostic: &AdapterDiagnostic) -> &'static str {
    match diagnostic {
        AdapterDiagnostic::None => "None",
        AdapterDiagnostic::Transport {
            stage: ObservationStage::Connect,
        } => "TransportConnect",
        AdapterDiagnostic::Server {
            surface: None,
            code: 1045,
            sql_state,
            ..
        } if sql_state.as_str() == "28000" => "Server1045State28000",
        AdapterDiagnostic::Server {
            surface: Some(NativeSurface::GroupMembers),
            code: 1142,
            ..
        } => "MembersServer1142",
        AdapterDiagnostic::Server {
            surface: Some(NativeSurface::LocalMemberStats),
            code: 1142,
            ..
        } => "StatsServer1142",
        AdapterDiagnostic::Product(diagnostic::ProductIssue::UnsupportedPatch) => {
            "UnsupportedPatch"
        }
        AdapterDiagnostic::Product(diagnostic::ProductIssue::NonOracleCommunity) => {
            "NonOracleCommunity"
        }
        AdapterDiagnostic::Schema {
            issue: diagnostic::SchemaIssue::ChangedType { .. },
            ..
        } => "ChangedType",
        AdapterDiagnostic::Schema {
            issue: diagnostic::SchemaIssue::AdditionalColumns { .. },
            ..
        } => "AdditionalColumns",
        AdapterDiagnostic::Evidence {
            issue: diagnostic::EvidenceIssue::RequiredNull { .. },
            ..
        } => "RequiredNull",
        AdapterDiagnostic::Evidence {
            issue: diagnostic::EvidenceIssue::IncompleteRow { .. },
            ..
        } => "IncompleteRow",
        AdapterDiagnostic::Evidence {
            issue: diagnostic::EvidenceIssue::MalformedCell { .. },
            ..
        } => "MalformedCell",
        AdapterDiagnostic::Evidence {
            issue: diagnostic::EvidenceIssue::EmptyRequiredString { .. },
            ..
        } => "EmptyRequiredString",
        AdapterDiagnostic::Evidence {
            issue: diagnostic::EvidenceIssue::UnsupportedNativeValue { .. },
            ..
        } => "UnsupportedNativeValue",
        AdapterDiagnostic::Evidence {
            issue:
                diagnostic::EvidenceIssue::MalformedGtid {
                    kind: kuberic_mysql_core::GtidParseErrorKind::SequenceOutOfRange,
                    ..
                },
            ..
        } => "MalformedGtidSequenceOutOfRange",
        AdapterDiagnostic::Ambiguous {
            kind: diagnostic::AmbiguityKind::DuplicateMemberId,
            ..
        } => "DuplicateMemberId",
        AdapterDiagnostic::Ambiguous {
            kind: diagnostic::AmbiguityKind::DuplicateMemberAddress,
            ..
        } => "DuplicateMemberAddress",
        AdapterDiagnostic::Ambiguous {
            kind: diagnostic::AmbiguityKind::DuplicateLocalRow,
            ..
        } => "DuplicateLocalRow",
        AdapterDiagnostic::Absent {
            surface: NativeSurface::GroupMembers,
        } => "AbsentGroupMembers",
        AdapterDiagnostic::Absent {
            surface: NativeSurface::LocalMemberStats,
        } => "AbsentLocalMemberStats",
        AdapterDiagnostic::Placeholder(PlaceholderKind::NeverStarted) => "PlaceholderNeverStarted",
        AdapterDiagnostic::Placeholder(PlaceholderKind::Stopped) => "PlaceholderStopped",
        other => panic!("fixture lacks diagnostic name for {other:?}"),
    }
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

    fn to_std_instant(&self, instant: ObservationInstant) -> Result<Instant, ClockError> {
        let delta = instant
            .tick()
            .checked_sub(self.tick_origin)
            .ok_or(ClockError::UnrepresentableInstant)?;
        self.runtime_origin
            .checked_add(Duration::from_millis(delta))
            .ok_or(ClockError::UnrepresentableInstant)
    }
}

#[derive(Debug)]
struct PublicTestClock;

impl kuberic_mysql_adapter::ObservationClock for PublicTestClock {
    fn now(&self) -> ObservationInstant {
        ObservationInstant::new(0)
    }

    fn to_std_instant(
        &self,
        _instant: ObservationInstant,
    ) -> Result<Instant, kuberic_mysql_adapter::ClockError> {
        Ok(Instant::now())
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
