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

use kuberic_mysql_core::{
    CollectionFailure, IncoherentReason, ObservationField, ObservationOutcome,
};

use observer::observe_with;
use phase4_common::{
    ConnectAction, ScriptAction, ScriptClock, ScriptedConnector, SessionErrorSpec, TestSocket,
    Tracking, bytes, failure_step, metadata_error, mutate_step, replace_query_step, request,
    success_steps,
};
use query::QueryId;

#[tokio::test(flavor = "current_thread")]
async fn coherent_observation_uses_one_session_and_disconnects() {
    let socket = TestSocket::new("coherent");
    let clock = ScriptClock::new(10);
    let tracking = Tracking::default();
    let connector = ScriptedConnector::new(clock.clone(), success_steps(), tracking.clone());
    let report = observe_with(&request(&socket, clock), &connector).await;

    let valid = report
        .outcome()
        .valid()
        .expect("coherent native observation");
    let snapshot = valid.native_snapshot().expect("native profile");
    assert_eq!(snapshot.view().members().len(), 2);
    assert_eq!(
        snapshot.local().group_replication_address().as_str(),
        "127.0.0.1:33061"
    );
    assert_eq!(valid.metadata().start().tick(), 10);
    assert_eq!(valid.metadata().end().tick(), 10);
    assert_eq!(report.diagnostic(), &AdapterDiagnostic::None);
    assert!(tracking.connected.load(Ordering::Acquire));
    assert!(tracking.disconnected.load(Ordering::Acquire));
    assert_eq!(tracking.query_count.load(Ordering::Acquire), 8);
}

#[tokio::test(flavor = "current_thread")]
async fn every_bracketed_native_field_changes_independently_and_row_order_does_not() {
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
        let socket = TestSocket::new(case);
        let clock = ScriptClock::new(10);
        let tracking = Tracking::default();
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
        let connector = ScriptedConnector::new(clock.clone(), steps, tracking.clone());
        let report = observe_with(&request(&socket, clock), &connector).await;
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
        assert_ne!(report.diagnostic(), &AdapterDiagnostic::None, "{case}");
        assert!(tracking.disconnected.load(Ordering::Acquire), "{case}");
    }

    let socket = TestSocket::new("row-order");
    let clock = ScriptClock::new(10);
    let mut steps = success_steps();
    mutate_step(&mut steps, 6, |result| result.rows.reverse());
    let connector = ScriptedConnector::new(clock.clone(), steps, Tracking::default());
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert!(report.outcome().valid().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn socket_revalidation_distinguishes_missing_type_access_and_replacement() {
    for (label, failure, issue) in [
        (
            "missing",
            metadata_error(ErrorKind::NotFound),
            SocketIssue::Missing,
        ),
        (
            "not-socket",
            request::SocketPathError::NotSocket,
            SocketIssue::NotSocket,
        ),
        (
            "inaccessible",
            metadata_error(ErrorKind::PermissionDenied),
            SocketIssue::Inaccessible,
        ),
        (
            "replaced",
            request::SocketPathError::Replaced,
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
}

#[tokio::test(flavor = "current_thread")]
async fn production_revalidation_rejects_a_replaced_socket_before_connect() {
    let mut socket = TestSocket::new("production-replaced");
    let observation_request = request(&socket, ScriptClock::new(10));
    socket.replace_socket();
    let report = observer::MysqlObserver::observe(observation_request).await;
    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Unreachable(_)
    ));
    assert_eq!(
        report.diagnostic(),
        &AdapterDiagnostic::Socket {
            issue: SocketIssue::Replaced
        }
    );

    let mut socket = TestSocket::new("production-missing");
    let observation_request = request(&socket, ScriptClock::new(10));
    socket.remove_socket();
    let report = observer::MysqlObserver::observe(observation_request).await;
    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Unreachable(_)
    ));
    assert_eq!(
        report.diagnostic(),
        &AdapterDiagnostic::Socket {
            issue: SocketIssue::Missing
        }
    );

    let mut socket = TestSocket::new("production-not-socket");
    let observation_request = request(&socket, ScriptClock::new(10));
    socket.remove_socket();
    std::fs::write(socket.path(), b"not a socket").expect("replace with regular file");
    let report = observer::MysqlObserver::observe(observation_request).await;
    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Unreachable(_)
    ));
    assert_eq!(
        report.diagnostic(),
        &AdapterDiagnostic::Socket {
            issue: SocketIssue::NotSocket
        }
    );
}

#[tokio::test(flavor = "current_thread")]
async fn connection_loss_at_every_collection_stage_preserves_partial_cause() {
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
        let socket = TestSocket::new(&format!("loss-{index}"));
        let clock = ScriptClock::new(10);
        let tracking = Tracking::default();
        let mut steps = success_steps();
        steps[index] = failure_step(query, SessionErrorSpec::Transport);
        let connector = ScriptedConnector::new(clock.clone(), steps, tracking.clone());
        let report = observe_with(&request(&socket, clock), &connector).await;
        if index < 4 {
            assert!(
                matches!(report.outcome(), ObservationOutcome::Unreachable(_)),
                "stage {index}: {:?}",
                report.outcome()
            );
        } else {
            let expected_missing = if index == 4 {
                vec![
                    ObservationField::ExecutedGtidSet,
                    ObservationField::ClosingNativeSnapshot,
                ]
            } else {
                vec![ObservationField::ClosingNativeSnapshot]
            };
            assert!(matches!(
                report.outcome(),
                ObservationOutcome::Partial {
                    missing,
                    cause: Some(CollectionFailure::Unreachable),
                    ..
                } if missing == &expected_missing
            ));
        }
        assert!(tracking.disconnected.load(Ordering::Acquire));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn authentication_identity_permissions_and_capability_map_with_required_precedence() {
    let socket = TestSocket::new("auth");
    let clock = ScriptClock::new(10);
    let connector = ScriptedConnector::new(clock.clone(), success_steps(), Tracking::default())
        .with_connect(ConnectAction::Error(SessionErrorSpec::Authentication));
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert!(matches!(
        report.outcome(),
        ObservationOutcome::AuthenticationFailure(_)
    ));
    assert_eq!(
        report.outcome().metadata().binding(),
        &phase4_common::binding()
    );

    for (fixture, expected) in [
        ("other-oracle-patch", ProductIssue::UnsupportedPatch),
        ("non-oracle-lookalike", ProductIssue::NonOracleCommunity),
    ] {
        let socket = TestSocket::new(fixture);
        let clock = ScriptClock::new(10);
        let tracking = Tracking::default();
        let mut steps = success_steps();
        steps[0] = phase4_common::result_step(QueryId::Mysql8411ProductIdentityV1, fixture);
        mutate_step(&mut steps, 0, |result| {
            result.rows[0][4] = bytes("dddddddd-dddd-dddd-dddd-dddddddddddd");
        });
        let connector = ScriptedConnector::new(clock.clone(), steps, tracking.clone());
        let report = observe_with(&request(&socket, clock), &connector).await;
        assert!(matches!(
            report.outcome(),
            ObservationOutcome::Unsupported { .. }
        ));
        assert_eq!(report.diagnostic(), &AdapterDiagnostic::Product(expected));
        assert!(tracking.disconnected.load(Ordering::Acquire));
    }

    let socket = TestSocket::new("wrong-server");
    let clock = ScriptClock::new(10);
    let mut steps = success_steps();
    mutate_step(&mut steps, 0, |result| {
        result.rows[0][4] = bytes("dddddddd-dddd-dddd-dddd-dddddddddddd");
    });
    let connector = ScriptedConnector::new(clock.clone(), steps, Tracking::default());
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Incoherent { .. }
    ));

    for (index, surface) in [
        (2, NativeSurface::GroupMembers),
        (3, NativeSurface::LocalMemberStats),
    ] {
        let socket = TestSocket::new(&format!("denied-{index}"));
        let clock = ScriptClock::new(10);
        let mut steps = success_steps();
        let query = steps[index].query;
        steps[index] = failure_step(query, SessionErrorSpec::Permission);
        let connector = ScriptedConnector::new(clock.clone(), steps, Tracking::default());
        let report = observe_with(&request(&socket, clock), &connector).await;
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

    let socket = TestSocket::new("unsupported-capability");
    let clock = ScriptClock::new(10);
    let mut steps = success_steps();
    steps[1] = failure_step(
        QueryId::Mysql8411LocalStateV1,
        SessionErrorSpec::Unsupported,
    );
    let connector = ScriptedConnector::new(clock.clone(), steps, Tracking::default());
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Unsupported { .. }
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_duplicate_and_disconnect_failures_map_without_success_fallback() {
    let socket = TestSocket::new("malformed");
    let clock = ScriptClock::new(10);
    let mut steps = success_steps();
    steps[4] = phase4_common::result_step(
        QueryId::Mysql8411ExecutedGtidsV1,
        "unexpected-required-null",
    );
    let connector = ScriptedConnector::new(clock.clone(), steps, Tracking::default());
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Partial {
            cause: Some(CollectionFailure::Malformed(_)),
            ..
        }
    ));

    let socket = TestSocket::new("duplicate");
    let clock = ScriptClock::new(10);
    let mut steps = success_steps();
    steps[2] = phase4_common::result_step(QueryId::Mysql8411GroupMembersV1, "duplicate-member-id");
    let connector = ScriptedConnector::new(clock.clone(), steps, Tracking::default());
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Incoherent { .. }
    ));

    let socket = TestSocket::new("disconnect-error");
    let clock = ScriptClock::new(10);
    let tracking = Tracking::default();
    let connector = ScriptedConnector::new(clock.clone(), success_steps(), tracking.clone())
        .with_disconnect_error(SessionErrorSpec::Transport);
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Unreachable(_)
    ));
    assert!(matches!(
        report.diagnostic(),
        AdapterDiagnostic::Transport {
            stage: ObservationStage::Disconnect
        }
    ));
    assert!(tracking.disconnected.load(Ordering::Acquire));
}

#[tokio::test(flavor = "current_thread")]
async fn recovering_gtid_is_a_single_valid_point_sample() {
    let socket = TestSocket::new("recovering");
    let clock = ScriptClock::new(10);
    let mut steps = success_steps();
    for index in [2, 6] {
        mutate_step(&mut steps, index, |result| {
            result.rows[0][3] = bytes("RECOVERING");
        });
    }
    let connector = ScriptedConnector::new(clock.clone(), steps, Tracking::default());
    let report = observe_with(&request(&socket, clock), &connector).await;
    let valid = report
        .outcome()
        .valid()
        .expect("stable recovering snapshot");
    assert_ne!(valid.executed(), &kuberic_mysql_core::GtidSet::empty());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn cancelled_query_disconnects_and_forced_late_completion_has_no_result_channel() {
    let socket = TestSocket::new("late");
    let clock = ScriptClock::new(10);
    let tracking = Tracking::default();
    let mut steps = success_steps();
    replace_query_step(
        &mut steps,
        4,
        ScriptAction::Pending {
            advance_to: 101,
            force_late_completion: true,
        },
    );
    let connector = ScriptedConnector::new(clock.clone(), steps, tracking.clone());
    let report = observe_with(&request(&socket, clock), &connector).await;
    tokio::task::yield_now().await;
    assert!(matches!(report.outcome(), ObservationOutcome::Stale { .. }));
    assert!(tracking.disconnected.load(Ordering::Acquire));
    assert!(tracking.late_completion.load(Ordering::Acquire));
    assert!(report.outcome().valid().is_none());
}
