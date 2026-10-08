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

use kuberic_mysql_core::{
    AuthoritySession, CompletionCredit, CompletionRejection, ObservationInstant, ObservationOutcome,
};

use observer::observe_with;
use phase4_common::{
    ScriptClock, ScriptedConnector, TestSocket, Tracking, binding, bytes, mutate_step,
    pending_step, request, success_steps,
};

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn ordinary_gate_is_server_free_fail_closed_and_authority_safe() {
    let socket = TestSocket::new("server-free-gate");
    let clock = ScriptClock::new(10);
    let current = binding();
    let mut authority = AuthoritySession::new(current.clone());
    let capability = authority.begin_attempt(&current).unwrap();
    let report = observe_with(
        &request(&socket, clock.clone()),
        &ScriptedConnector::new(clock, success_steps(), Tracking::default()),
    )
    .await;
    assert!(report.outcome().valid().is_some());
    assert_eq!(
        authority.complete(capability, report.outcome(), ObservationInstant::new(20)),
        Ok(CompletionCredit::ObservationAcknowledged)
    );
    assert!(!authority.access().read_open());
    assert!(!authority.access().write_open());

    let socket = TestSocket::new("server-free-incoherent");
    let clock = ScriptClock::new(10);
    let mut changed = success_steps();
    mutate_step(&mut changed, 6, |result| {
        result.rows[0][4] = bytes("SECONDARY");
    });
    let report = observe_with(
        &request(&socket, clock.clone()),
        &ScriptedConnector::new(clock, changed, Tracking::default()),
    )
    .await;
    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Incoherent { .. }
    ));

    let socket = TestSocket::new("server-free-timeout");
    let clock = ScriptClock::new(10);
    let mut delayed = success_steps();
    let query = delayed[4].query;
    delayed[4] = pending_step(query, true);
    let mut authority = AuthoritySession::new(current.clone());
    let capability = authority.begin_attempt(&current).unwrap();
    let report = observe_with(
        &request(&socket, clock.clone()),
        &ScriptedConnector::new(clock, delayed, Tracking::default()),
    )
    .await;
    assert!(matches!(report.outcome(), ObservationOutcome::Stale { .. }));
    assert_eq!(
        authority.complete(capability, report.outcome(), ObservationInstant::new(101)),
        Err(CompletionRejection::NonValidObservation)
    );
    assert!(!authority.has_observation_credit());
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
