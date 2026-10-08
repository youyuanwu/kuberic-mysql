#![allow(dead_code)]

#[path = "../src/decode.rs"]
mod decode;
#[path = "../src/diagnostic.rs"]
mod diagnostic;
#[path = "../src/error.rs"]
mod error;
#[path = "../src/observer.rs"]
mod observer;
#[path = "../src/query.rs"]
mod query;
#[path = "../src/report.rs"]
mod report;
#[path = "../src/request.rs"]
mod request;
#[path = "../src/session.rs"]
mod session;
#[path = "../src/time.rs"]
mod time;

mod common;
mod phase4_common;

pub use diagnostic::*;
pub use report::ObservationReport;
pub use request::*;
pub use time::*;

use std::io::ErrorKind;
use std::sync::atomic::Ordering;
use std::time::Duration;

use kuberic_mysql_core::{
    AuthoritySession, CollectionFailure, CompletionCredit, CompletionRejection,
    CredentialGeneration, ExactBinding, IncoherentReason, ObservationField, ObservationInstant,
    ObservationOutcome, StaleReason,
};

use observer::observe_with;
use phase4_common::{
    ConnectAction, ScriptAction, ScriptClock, ScriptStep, ScriptedConnector, SessionErrorSpec,
    TestSocket, Tracking, binding, bytes, failure_step, metadata_error, mutate_step, pending_step,
    request, result_step, success_steps,
};
use query::QueryId;

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn aggregate_phase4_server_free_matrix() {
    valid_success_and_authority_closure().await;
    product_and_binding_precedence().await;
    socket_transport_authentication_and_permissions().await;
    absent_malformed_unsupported_and_schema_cases().await;
    coherence_mutations_and_row_reordering().await;
    collection_stage_connection_loss().await;
    future_dated().await;
    deadline_boundaries_and_actual_late_completion().await;
    completion_expiry().await;
    timeout_error_precedence().await;
    disconnect_error_and_deadline().await;
    binding_and_credential_replacement().await;
    recovering_point_sample().await;
}

async fn valid_success_and_authority_closure() {
    let current = binding();
    let mut authority = AuthoritySession::new(current.clone());
    let capability = authority.begin_attempt(&current).expect("pending attempt");
    let (report, tracking) = observe_steps("gate-valid", success_steps()).await;

    assert!(report.outcome().valid().is_some());
    assert_eq!(tracking.query_count.load(Ordering::Acquire), 8);
    assert!(tracking.disconnected.load(Ordering::Acquire));
    assert_eq!(
        authority.complete(capability, report.outcome(), ObservationInstant::new(20)),
        Ok(CompletionCredit::ObservationAcknowledged)
    );
    assert!(authority.has_observation_credit());
    assert_closed(&authority);
}

async fn product_and_binding_precedence() {
    for (fixture, issue) in [
        ("other-oracle-patch", ProductIssue::UnsupportedPatch),
        ("non-oracle-lookalike", ProductIssue::NonOracleCommunity),
    ] {
        let mut steps = success_steps();
        steps[0] = result_step(QueryId::Mysql8411ProductIdentityV1, fixture);
        mutate_step(&mut steps, 0, |result| {
            result.rows[0][4] = bytes("dddddddd-dddd-dddd-dddd-dddddddddddd");
        });
        let (report, _) = observe_steps(fixture, steps).await;
        assert!(matches!(
            report.outcome(),
            ObservationOutcome::Unsupported { .. }
        ));
        assert_eq!(report.diagnostic(), &AdapterDiagnostic::Product(issue));
    }

    let mut steps = success_steps();
    mutate_step(&mut steps, 0, |result| {
        result.rows[0][4] = bytes("dddddddd-dddd-dddd-dddd-dddddddddddd");
    });
    let (report, _) = observe_steps("gate-binding", steps).await;
    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Incoherent {
            reason: IncoherentReason::NativeSnapshotMismatch,
            ..
        }
    ));
    assert_eq!(
        report.diagnostic(),
        &AdapterDiagnostic::UnexpectedIdentity {
            field: IdentityField::ServerUuid
        }
    );
}

async fn socket_transport_authentication_and_permissions() {
    for (label, failure, issue) in [
        (
            "gate-socket-missing",
            metadata_error(ErrorKind::NotFound),
            SocketIssue::Missing,
        ),
        (
            "gate-socket-type",
            SocketPathError::NotSocket,
            SocketIssue::NotSocket,
        ),
        (
            "gate-socket-access",
            metadata_error(ErrorKind::PermissionDenied),
            SocketIssue::Inaccessible,
        ),
        (
            "gate-socket-replaced",
            SocketPathError::Replaced,
            SocketIssue::Replaced,
        ),
    ] {
        let socket = TestSocket::new(label);
        let clock = ScriptClock::new(10);
        let tracking = Tracking::default();
        let connector = ScriptedConnector::new(clock.clone(), success_steps(), tracking.clone())
            .with_revalidation(Err(failure));
        let report = observe_with(&request(&socket, clock), &connector).await;
        assert!(matches!(
            report.outcome(),
            ObservationOutcome::Unreachable(_)
        ));
        assert_eq!(report.diagnostic(), &AdapterDiagnostic::Socket { issue });
        assert!(!tracking.connected.load(Ordering::Acquire));
    }

    for (label, action, expected) in [
        (
            "gate-transport",
            ConnectAction::Error(SessionErrorSpec::Transport),
            CoreOutcomeClass::Unreachable,
        ),
        (
            "gate-authentication",
            ConnectAction::Error(SessionErrorSpec::Authentication),
            CoreOutcomeClass::AuthenticationFailure,
        ),
    ] {
        let socket = TestSocket::new(label);
        let clock = ScriptClock::new(10);
        let connector = ScriptedConnector::new(clock.clone(), success_steps(), Tracking::default())
            .with_connect(action);
        let report = observe_with(&request(&socket, clock), &connector).await;
        assert_eq!(report.diagnostic().core_class(), expected);
    }

    for (index, surface) in [
        (2, NativeSurface::GroupMembers),
        (3, NativeSurface::LocalMemberStats),
    ] {
        let mut steps = success_steps();
        let query = steps[index].query;
        steps[index] = failure_step(query, SessionErrorSpec::Permission);
        let (report, _) = observe_steps(&format!("gate-permission-{index}"), steps).await;
        assert!(matches!(
            report.outcome(),
            ObservationOutcome::PermissionDenied(_)
        ));
        assert!(matches!(
            report.diagnostic(),
            AdapterDiagnostic::Server {
                surface: Some(actual),
                class: ServerErrorClass::Permission,
                ..
            } if actual == &surface
        ));
    }
}

async fn absent_malformed_unsupported_and_schema_cases() {
    let mut absent = success_steps();
    absent[2] = result_step(QueryId::Mysql8411GroupMembersV1, "plugin-absent-members");
    let (report, _) = observe_steps("gate-absent", absent).await;
    assert!(matches!(report.outcome(), ObservationOutcome::Absent(_)));
    assert_eq!(
        report.diagnostic(),
        &AdapterDiagnostic::Absent {
            surface: NativeSurface::GroupMembers
        }
    );

    let mut malformed = success_steps();
    malformed[4] = result_step(
        QueryId::Mysql8411ExecutedGtidsV1,
        "unexpected-required-null",
    );
    let (report, _) = observe_steps("gate-malformed", malformed).await;
    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Partial {
            cause: Some(CollectionFailure::Malformed(_)),
            ..
        }
    ));

    let mut unsupported = success_steps();
    unsupported[2] = result_step(QueryId::Mysql8411GroupMembersV1, "future-member-role");
    let (report, _) = observe_steps("gate-unsupported", unsupported).await;
    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Unsupported { .. }
    ));
    assert!(matches!(
        report.diagnostic(),
        AdapterDiagnostic::Evidence {
            issue: EvidenceIssue::UnsupportedNativeValue { .. },
            ..
        }
    ));

    let mut schema = success_steps();
    schema[2] = result_step(
        QueryId::Mysql8411GroupMembersV1,
        "members-schema-type-drift",
    );
    let (report, _) = observe_steps("gate-schema", schema).await;
    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Unsupported { .. }
    ));
    assert!(matches!(
        report.diagnostic(),
        AdapterDiagnostic::Schema {
            surface: NativeSurface::GroupMembers,
            ..
        }
    ));
}

async fn coherence_mutations_and_row_reordering() {
    for case in [
        "group",
        "view",
        "member-set",
        "member-uuid",
        "sql-address",
        "gr-address",
        "role",
        "state",
        "read-only",
        "super-read-only",
    ] {
        let mut steps = success_steps();
        match case {
            "group" => mutate_step(&mut steps, 5, |result| {
                result.rows[0][0] = bytes("dddddddd-dddd-dddd-dddd-dddddddddddd");
            }),
            "view" => mutate_step(&mut steps, 7, |result| {
                result.rows[0][1] = bytes("view-0002");
            }),
            "member-set" => mutate_step(&mut steps, 6, |result| {
                result.rows.pop();
            }),
            "member-uuid" => mutate_step(&mut steps, 6, |result| {
                result.rows[0][0] = bytes("dddddddd-dddd-dddd-dddd-dddddddddddd");
            }),
            "sql-address" => mutate_step(&mut steps, 6, |result| {
                result.rows[0][1] = bytes("mysql-changed");
            }),
            "gr-address" => mutate_step(&mut steps, 5, |result| {
                result.rows[0][1] = bytes("127.0.0.1:33062");
            }),
            "role" => mutate_step(&mut steps, 6, |result| {
                result.rows[0][4] = bytes("SECONDARY");
            }),
            "state" => mutate_step(&mut steps, 6, |result| {
                result.rows[0][3] = bytes("RECOVERING");
            }),
            "read-only" => mutate_step(&mut steps, 5, |result| {
                result.rows[0][2] = query::RawValue::Unsigned(1);
            }),
            "super-read-only" => mutate_step(&mut steps, 5, |result| {
                result.rows[0][3] = query::RawValue::Unsigned(1);
            }),
            _ => unreachable!(),
        }
        let (report, _) = observe_steps(&format!("gate-coherence-{case}"), steps).await;
        assert!(
            matches!(
                report.outcome(),
                ObservationOutcome::Incoherent {
                    reason: IncoherentReason::NativeSnapshotMismatch,
                    ..
                }
            ),
            "{case}: {:?}",
            report.outcome()
        );
    }

    let mut reordered = success_steps();
    mutate_step(&mut reordered, 6, |result| result.rows.reverse());
    let (report, _) = observe_steps("gate-row-reorder", reordered).await;
    assert!(report.outcome().valid().is_some());
}

async fn collection_stage_connection_loss() {
    let queries = [
        QueryId::Mysql8411ProductIdentityV1,
        QueryId::Mysql8411LocalStateV1,
        QueryId::Mysql8411GroupMembersV1,
        QueryId::Mysql8411LocalMemberStatsV1,
        QueryId::Mysql8411ExecutedGtidsV1,
        QueryId::Mysql8411LocalStateV1,
        QueryId::Mysql8411GroupMembersV1,
        QueryId::Mysql8411LocalMemberStatsV1,
    ];
    for (index, query) in queries.into_iter().enumerate() {
        let mut steps = success_steps();
        steps[index] = failure_step(query, SessionErrorSpec::Transport);
        let (report, tracking) =
            observe_steps(&format!("gate-connection-loss-{index}"), steps).await;

        match index {
            0..=3 => assert!(matches!(
                report.outcome(),
                ObservationOutcome::Unreachable(_)
            )),
            4 => assert!(matches!(
                report.outcome(),
                ObservationOutcome::Partial {
                    missing,
                    cause: Some(CollectionFailure::Unreachable),
                    ..
                } if missing == &[
                    ObservationField::ExecutedGtidSet,
                    ObservationField::ClosingNativeSnapshot,
                ]
            )),
            5..=7 => assert!(matches!(
                report.outcome(),
                ObservationOutcome::Partial {
                    missing,
                    cause: Some(CollectionFailure::Unreachable),
                    ..
                } if missing == &[ObservationField::ClosingNativeSnapshot]
            )),
            _ => unreachable!(),
        }
        assert_eq!(
            report.diagnostic(),
            &AdapterDiagnostic::Transport {
                stage: ObservationStage::Query
            }
        );
        assert_eq!(
            tracking.query_count.load(Ordering::Acquire),
            u64::try_from(index + 1).unwrap()
        );
        assert!(tracking.disconnected.load(Ordering::Acquire));
    }
}

async fn future_dated() {
    let socket = TestSocket::new("gate-future-dated");
    let clock = ScriptClock::new(10);
    let tracking = Tracking::default();
    let connector = ScriptedConnector::new(clock.clone(), success_steps(), tracking.clone())
        .with_disconnect_advance(5);
    let report = observe_with(&request(&socket, clock), &connector).await;

    assert!(matches!(
        report.outcome(),
        ObservationOutcome::FutureDated(_)
    ));
    assert_eq!(
        report.diagnostic(),
        &AdapterDiagnostic::Outcome(CoreOutcomeClass::FutureDated)
    );
    assert!(tracking.disconnected.load(Ordering::Acquire));
}

async fn deadline_boundaries_and_actual_late_completion() {
    let boundaries = [
        ("connect", None, DeadlineStage::Connect),
        ("identity", Some(0), DeadlineStage::ProductIdentity),
        ("opening-local", Some(1), DeadlineStage::OpeningLocalState),
        ("opening-members", Some(2), DeadlineStage::OpeningMembers),
        ("opening-view", Some(3), DeadlineStage::OpeningView),
        ("gtid", Some(4), DeadlineStage::ExecutedGtids),
        ("closing-local", Some(5), DeadlineStage::ClosingLocalState),
        ("closing-members", Some(6), DeadlineStage::ClosingMembers),
        ("closing-view", Some(7), DeadlineStage::ClosingView),
    ];
    for (label, query_index, stage) in boundaries {
        let socket = TestSocket::new(&format!("gate-deadline-{label}"));
        let clock = ScriptClock::new(10);
        let tracking = Tracking::default();
        let mut steps = success_steps();
        let connector = if let Some(index) = query_index {
            let query = steps[index].query;
            steps[index] = pending_step(query);
            ScriptedConnector::new(clock.clone(), steps, tracking.clone())
        } else {
            ScriptedConnector::new(clock.clone(), steps, tracking.clone())
                .with_connect(ConnectAction::Pending { advance_to: 101 })
        };
        let report = observe_with(&request(&socket, clock), &connector).await;
        assert!(matches!(
            report.outcome(),
            ObservationOutcome::Stale {
                reason: StaleReason::Expired,
                ..
            }
        ));
        assert_eq!(report.diagnostic(), &AdapterDiagnostic::Timeout { stage });
    }

    let current = binding();
    let mut authority = AuthoritySession::new(current.clone());
    let capability = authority.begin_attempt(&current).expect("pending attempt");
    let socket = TestSocket::new("gate-late-result");
    let clock = ScriptClock::new(10);
    let tracking = Tracking::default();
    let mut steps = success_steps();
    let result = take_result(&mut steps[4]);
    steps[4].action = ScriptAction::LateResult {
        result,
        delay: Duration::from_millis(1),
        advance_to: 101,
    };
    let connector = ScriptedConnector::new(clock.clone(), steps, tracking.clone());
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert!(tracking.late_completion.load(Ordering::Acquire));
    assert!(matches!(report.outcome(), ObservationOutcome::Stale { .. }));
    assert_eq!(report.outcome().metadata().binding(), &current);
    assert_eq!(report.outcome().metadata().start().tick(), 10);
    assert_eq!(report.outcome().metadata().end().tick(), 101);
    assert_eq!(report.outcome().metadata().decision().tick(), 101);
    assert_eq!(
        authority.complete(capability, report.outcome(), ObservationInstant::new(101)),
        Err(CompletionRejection::NonValidObservation)
    );
    assert!(!authority.has_observation_credit());
    assert_closed(&authority);
}

async fn completion_expiry() {
    let socket = TestSocket::new("gate-completion-expired");
    let clock = ScriptClock::new(10);
    let tracking = Tracking::default();
    let connector = ScriptedConnector::new(clock.clone(), success_steps(), tracking.clone())
        .with_disconnect_advance(101);
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Stale {
            reason: StaleReason::Expired,
            ..
        }
    ));
    assert_eq!(
        report.diagnostic(),
        &AdapterDiagnostic::Timeout {
            stage: DeadlineStage::Completion
        }
    );
    assert!(tracking.disconnected.load(Ordering::Acquire));

    let socket = TestSocket::new("gate-completion-exact");
    let clock = ScriptClock::new(10);
    let connector = ScriptedConnector::new(clock.clone(), success_steps(), Tracking::default())
        .with_disconnect_advance(100);
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert!(report.outcome().valid().is_some());
    assert_eq!(report.outcome().metadata().decision().tick(), 100);
}

async fn timeout_error_precedence() {
    let socket = TestSocket::new("gate-timeout-error-precedence");
    let clock = ScriptClock::new(10);
    let mut steps = success_steps();
    steps[4].action = ScriptAction::ErrorAt {
        error: SessionErrorSpec::Transport,
        advance_to: 101,
    };
    let connector = ScriptedConnector::new(clock.clone(), steps, Tracking::default());
    let report = observe_with(&request(&socket, clock), &connector).await;

    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Stale {
            reason: StaleReason::Expired,
            ..
        }
    ));
    assert_eq!(
        report.diagnostic(),
        &AdapterDiagnostic::Timeout {
            stage: DeadlineStage::ExecutedGtids
        }
    );
}

async fn disconnect_error_and_deadline() {
    let socket = TestSocket::new("gate-disconnect-error");
    let clock = ScriptClock::new(10);
    let tracking = Tracking::default();
    let connector = ScriptedConnector::new(clock.clone(), success_steps(), tracking.clone())
        .with_disconnect_error(SessionErrorSpec::Transport);
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Unreachable(_)
    ));
    assert_eq!(
        report.diagnostic(),
        &AdapterDiagnostic::Transport {
            stage: ObservationStage::Disconnect
        }
    );
    assert!(tracking.disconnect_started.load(Ordering::Acquire));
    assert!(tracking.disconnected.load(Ordering::Acquire));

    let socket = TestSocket::new("gate-disconnect-deadline");
    let clock = ScriptClock::new(10);
    let tracking = Tracking::default();
    let connector = ScriptedConnector::new(clock.clone(), success_steps(), tracking.clone())
        .with_pending_disconnect(101);
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Stale {
            reason: StaleReason::Expired,
            ..
        }
    ));
    assert_eq!(
        report.diagnostic(),
        &AdapterDiagnostic::Timeout {
            stage: DeadlineStage::Disconnect
        }
    );
    assert!(tracking.disconnect_started.load(Ordering::Acquire));
    assert!(!tracking.disconnected.load(Ordering::Acquire));
}

async fn binding_and_credential_replacement() {
    for credential_only in [false, true] {
        let current = binding();
        let mut authority = AuthoritySession::new(current.clone());
        let capability = authority.begin_attempt(&current).expect("pending attempt");
        let (report, _) = observe_steps(
            if credential_only {
                "gate-credential-replacement"
            } else {
                "gate-binding-replacement"
            },
            success_steps(),
        )
        .await;
        let mut parts = current.parts().clone();
        if credential_only {
            parts.credential_generation =
                CredentialGeneration::new("replacement-credential").unwrap();
        } else {
            parts.epoch = kuberic_mysql_core::Epoch::new("replacement-epoch").unwrap();
        }
        authority.replace_binding(ExactBinding::new(parts));
        assert_eq!(
            authority.complete(capability, report.outcome(), ObservationInstant::new(20)),
            Err(CompletionRejection::StaleCapability)
        );
        assert!(!authority.has_observation_credit());
        assert_closed(&authority);
    }
}

async fn recovering_point_sample() {
    let mut steps = success_steps();
    for index in [2, 6] {
        mutate_step(&mut steps, index, |result| {
            result.rows[0][3] = bytes("RECOVERING");
        });
    }
    let (report, _) = observe_steps("gate-recovering", steps).await;
    let valid = report
        .outcome()
        .valid()
        .expect("stable recovering observation");
    assert_ne!(valid.executed(), &kuberic_mysql_core::GtidSet::empty());
}

async fn observe_steps(label: &str, steps: Vec<ScriptStep>) -> (ObservationReport, Tracking) {
    let socket = TestSocket::new(label);
    let clock = ScriptClock::new(10);
    let tracking = Tracking::default();
    let connector = ScriptedConnector::new(clock.clone(), steps, tracking.clone());
    let report = observe_with(&request(&socket, clock), &connector).await;
    (report, tracking)
}

fn take_result(step: &mut ScriptStep) -> query::RawResult {
    match std::mem::replace(&mut step.action, ScriptAction::Pending { advance_to: 101 }) {
        ScriptAction::Result(result) => result,
        _ => panic!("success step must contain a result"),
    }
}

fn assert_closed(authority: &AuthoritySession) {
    assert!(!authority.access().read_open());
    assert!(!authority.access().write_open());
}

#[test]
fn ordinary_gate_has_no_live_server_container_process_or_kubernetes_input() {
    let manifest = include_str!("../Cargo.toml");
    assert!(!manifest.contains("testcontainers"));
    assert!(!manifest.contains("\nkube ="));
    assert!(!manifest.contains("\nk8s"));
    assert!(!manifest.contains("bollard"));
    assert!(!manifest.contains("command"));
    assert!(manifest.contains("mysql_async"));
}
