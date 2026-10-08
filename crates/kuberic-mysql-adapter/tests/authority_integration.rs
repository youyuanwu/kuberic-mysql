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
    ScriptClock, ScriptedConnector, TestSocket, Tracking, binding, pending_step, request,
    success_steps,
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
async fn timeout_and_forced_late_completion_receive_zero_credit() {
    let socket = TestSocket::new("authority-timeout");
    let clock = ScriptClock::new(10);
    let current = binding();
    let mut authority = AuthoritySession::new(current.clone());
    let capability = authority.begin_attempt(&current).expect("pending attempt");
    let mut steps = success_steps();
    let query = steps[4].query;
    steps[4] = pending_step(query, true);
    let connector = ScriptedConnector::new(clock.clone(), steps, Tracking::default());
    let report = observe_with(&request(&socket, clock), &connector).await;

    assert!(matches!(report.outcome(), ObservationOutcome::Stale { .. }));
    assert_eq!(
        authority.complete(capability, report.outcome(), ObservationInstant::new(101)),
        Err(CompletionRejection::NonValidObservation)
    );
    assert!(!authority.has_observation_credit());
    assert!(!authority.access().read_open());
    assert!(!authority.access().write_open());
}
