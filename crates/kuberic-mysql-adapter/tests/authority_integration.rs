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
    AuthoritySession, CompletionCredit, CompletionRejection, CredentialGeneration, ExactBinding,
    ObservationInstant, ObservationOutcome,
};

use observer::observe_with;
use phase4_common::{
    ScriptClock, ScriptedConnector, TestSocket, Tracking, binding, request, success_steps,
};

#[tokio::test(flavor = "current_thread")]
async fn valid_native_observation_receives_credit_while_access_remains_closed() {
    let socket = TestSocket::new("authority-valid");
    let clock = ScriptClock::new(10);
    let current = binding();
    let mut authority = AuthoritySession::new(current.clone());
    let capability = authority.begin_attempt(&current).expect("pending attempt");
    let connector = ScriptedConnector::new(clock.clone(), success_steps(), Tracking::default());
    let report = observe_with(&request(&socket, clock), &connector).await;

    assert_eq!(
        authority.complete(capability, report.outcome(), ObservationInstant::new(20)),
        Ok(CompletionCredit::ObservationAcknowledged)
    );
    assert!(authority.has_observation_credit());
    assert!(!authority.access().read_open());
    assert!(!authority.access().write_open());
}

#[tokio::test(flavor = "current_thread")]
async fn binding_and_credential_replacement_revoke_pending_observation() {
    for credential_only in [false, true] {
        let socket = TestSocket::new(if credential_only {
            "credential-replaced"
        } else {
            "binding-replaced"
        });
        let clock = ScriptClock::new(10);
        let current = binding();
        let mut authority = AuthoritySession::new(current.clone());
        let capability = authority.begin_attempt(&current).expect("pending attempt");
        let connector = ScriptedConnector::new(clock.clone(), success_steps(), Tracking::default());
        let report = observe_with(&request(&socket, clock), &connector).await;
        assert!(report.outcome().valid().is_some());

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
        assert!(!authority.access().read_open());
        assert!(!authority.access().write_open());
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn actual_late_completion_receives_zero_credit() {
    let socket = TestSocket::new("authority-timeout");
    let clock = ScriptClock::new(10);
    let current = binding();
    let mut authority = AuthoritySession::new(current.clone());
    let capability = authority.begin_attempt(&current).expect("pending attempt");
    let mut steps = success_steps();
    let result = match std::mem::replace(
        &mut steps[4].action,
        phase4_common::ScriptAction::Pending { advance_to: 101 },
    ) {
        phase4_common::ScriptAction::Result(result) => result,
        _ => unreachable!("success step"),
    };
    steps[4].action = phase4_common::ScriptAction::LateResult {
        result,
        delay: std::time::Duration::from_millis(1),
        advance_to: 101,
    };
    let tracking = Tracking::default();
    let connector = ScriptedConnector::new(clock.clone(), steps, tracking.clone());
    let report = observe_with(&request(&socket, clock), &connector).await;

    assert!(matches!(report.outcome(), ObservationOutcome::Stale { .. }));
    assert_eq!(report.outcome().metadata().start().tick(), 10);
    assert_eq!(report.outcome().metadata().end().tick(), 101);
    assert_eq!(report.outcome().metadata().decision().tick(), 101);
    assert_eq!(report.outcome().metadata().binding(), &current);
    assert!(
        tracking
            .late_completion
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert!(
        tracking
            .disconnect_started
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert!(
        tracking
            .disconnected
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert_eq!(
        authority.complete(capability, report.outcome(), ObservationInstant::new(101)),
        Err(CompletionRejection::NonValidObservation)
    );
    assert!(!authority.has_observation_credit());
    assert!(!authority.access().read_open());
    assert!(!authority.access().write_open());
}
