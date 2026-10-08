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

use std::sync::atomic::Ordering;

use kuberic_mysql_core::{ObservationOutcome, StaleReason};

use observer::observe_with;
use phase4_common::{
    ConnectAction, ScriptAction, ScriptClock, ScriptedConnector, SessionErrorSpec, TestSocket,
    Tracking, pending_step, replace_query_step, request, success_steps,
};

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn one_absolute_deadline_covers_connect_and_every_collection_boundary() {
    let cases = [
        ("connect", None, DeadlineStage::Connect),
        ("identity", Some(0), DeadlineStage::ProductIdentity),
        ("opening-local", Some(1), DeadlineStage::OpeningLocalState),
        ("opening-members", Some(2), DeadlineStage::OpeningMembers),
        ("opening-view", Some(3), DeadlineStage::OpeningView),
        ("after-opening", Some(4), DeadlineStage::ExecutedGtids),
        (
            "after-gtid-local",
            Some(5),
            DeadlineStage::ClosingLocalState,
        ),
        ("after-gtid-members", Some(6), DeadlineStage::ClosingMembers),
        ("during-closing", Some(7), DeadlineStage::ClosingView),
    ];
    for (label, query_index, expected_stage) in cases {
        let socket = TestSocket::new(label);
        let clock = ScriptClock::new(10);
        let tracking = Tracking::default();
        let mut steps = success_steps();
        let connector = if let Some(index) = query_index {
            let query = steps[index].query;
            steps[index] = pending_step(query, false);
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
        assert_eq!(
            report.diagnostic(),
            &AdapterDiagnostic::Timeout {
                stage: expected_stage
            }
        );
        assert!(report.outcome().valid().is_none());
        if query_index.is_some() {
            assert!(tracking.disconnected.load(Ordering::Acquire), "{label}");
        } else {
            assert!(!tracking.connected.load(Ordering::Acquire));
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn completion_after_full_collection_expires_but_exact_deadline_is_valid() {
    let socket = TestSocket::new("completion-expired");
    let clock = ScriptClock::new(10);
    let tracking = Tracking::default();
    let connector = ScriptedConnector::new(clock.clone(), success_steps(), tracking.clone())
        .with_disconnect_advance(101);
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert!(matches!(report.outcome(), ObservationOutcome::Stale { .. }));
    assert_eq!(
        report.diagnostic(),
        &AdapterDiagnostic::Timeout {
            stage: DeadlineStage::Completion
        }
    );
    assert!(tracking.disconnected.load(Ordering::Acquire));

    let socket = TestSocket::new("completion-exact");
    let clock = ScriptClock::new(10);
    let connector = ScriptedConnector::new(clock.clone(), success_steps(), Tracking::default())
        .with_disconnect_advance(100);
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert!(report.outcome().valid().is_some());
    assert_eq!(report.outcome().metadata().decision().tick(), 100);

    let socket = TestSocket::new("disconnect-expired");
    let clock = ScriptClock::new(10);
    let connector = ScriptedConnector::new(clock.clone(), success_steps(), Tracking::default())
        .with_disconnect_error(SessionErrorSpec::Transport)
        .with_disconnect_advance(101);
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert_eq!(
        report.diagnostic(),
        &AdapterDiagnostic::Timeout {
            stage: DeadlineStage::Disconnect
        }
    );
}

#[tokio::test(flavor = "current_thread")]
async fn regressing_clock_is_future_dated_and_clock_advance_during_query_is_expired() {
    let socket = TestSocket::new("future-dated");
    let clock = ScriptClock::new(10);
    let connector = ScriptedConnector::new(clock.clone(), success_steps(), Tracking::default())
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

    let socket = TestSocket::new("clock-change");
    let clock = ScriptClock::new(10);
    let mut steps = success_steps();
    replace_query_step(
        &mut steps,
        4,
        ScriptAction::Pending {
            advance_to: 500,
            force_late_completion: false,
        },
    );
    let connector = ScriptedConnector::new(clock.clone(), steps, Tracking::default());
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert_eq!(report.outcome().metadata().end().tick(), 500);
    assert!(matches!(report.outcome(), ObservationOutcome::Stale { .. }));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn deterministic_delay_within_the_same_absolute_budget_can_complete() {
    let socket = TestSocket::new("bounded-delay");
    let clock = ScriptClock::new(10);
    let mut steps = success_steps();
    let result = match std::mem::replace(
        &mut steps[4].action,
        ScriptAction::Pending {
            advance_to: 101,
            force_late_completion: false,
        },
    ) {
        ScriptAction::Result(result) => result,
        _ => unreachable!("success step"),
    };
    steps[4].action = ScriptAction::DelayedResult {
        result,
        delay: std::time::Duration::from_millis(10),
        advance_to: 20,
    };
    let connector = ScriptedConnector::new(clock.clone(), steps, Tracking::default());
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert!(report.outcome().valid().is_some());
    assert_eq!(report.outcome().metadata().end().tick(), 20);
}

#[tokio::test(flavor = "current_thread")]
async fn expiry_precedes_a_simultaneous_terminal_failure() {
    let socket = TestSocket::new("deadline-precedence");
    let clock = ScriptClock::new(10);
    let mut steps = success_steps();
    steps[4].action = ScriptAction::ErrorAt {
        error: SessionErrorSpec::Transport,
        advance_to: 101,
    };
    let connector = ScriptedConnector::new(clock.clone(), steps, Tracking::default());
    let report = observe_with(&request(&socket, clock), &connector).await;
    assert!(matches!(report.outcome(), ObservationOutcome::Stale { .. }));
    assert_eq!(
        report.diagnostic(),
        &AdapterDiagnostic::Timeout {
            stage: DeadlineStage::ExecutedGtids
        }
    );
}
