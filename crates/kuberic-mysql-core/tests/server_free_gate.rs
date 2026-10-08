mod common;

use std::str::FromStr;

use common::{binding, bracket, metadata};
use kuberic_mysql_core::{
    AuthoritySession, BoundGtidSet, CollectionFailure, CompletionCredit, CompletionRejection,
    GtidParseErrorKind, GtidRelation, GtidSet, ObservationDraft, ObservationInstant,
    ObservationOutcome, ReplicaIncarnation, UnsupportedOperation,
};

fn complete_outcome(binding: &kuberic_mysql_core::ExactBinding) -> ObservationOutcome {
    ObservationDraft::new(metadata(binding.clone(), 10, 20, 30, 20))
        .opening(bracket(binding.clone()))
        .executed(BoundGtidSet::new(
            binding.clone(),
            GtidSet::from_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-3").unwrap(),
        ))
        .closing(bracket(binding.clone()))
        .finalize()
}

#[test]
fn current_coherent_observation_is_credited_but_access_stays_closed() {
    let current = binding();
    let outcome = complete_outcome(&current);
    let mut authority = AuthoritySession::new(current.clone());
    let capability = authority.begin_attempt(&current).unwrap();
    assert_eq!(
        authority.complete(capability, &outcome, ObservationInstant::new(30)),
        Ok(CompletionCredit::ObservationAcknowledged)
    );
    assert!(authority.has_observation_credit());
    assert!(!authority.access().read_open());
    assert!(!authority.access().write_open());
}

#[test]
fn malformed_and_divergent_histories_remain_fail_closed_facts() {
    let malformed = GtidSet::from_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:0").unwrap_err();
    assert_eq!(malformed.kind(), &GtidParseErrorKind::SequenceOutOfRange);

    let first = GtidSet::from_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-5").unwrap();
    let second = GtidSet::from_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1").unwrap();
    assert_eq!(first.relation(&second), GtidRelation::Incomparable);
}

#[test]
fn partial_stale_future_and_incoherent_evidence_receive_no_credit() {
    let current = binding();
    let partial = ObservationDraft::new(metadata(current.clone(), 10, 20, 30, 20))
        .opening(bracket(current.clone()))
        .failure(CollectionFailure::Unreachable)
        .finalize();
    let stale = ObservationDraft::new(metadata(current.clone(), 10, 20, 30, 31))
        .opening(bracket(current.clone()))
        .executed(BoundGtidSet::new(current.clone(), GtidSet::empty()))
        .closing(bracket(current.clone()))
        .finalize();
    let future = ObservationDraft::new(metadata(current.clone(), 10, 20, 30, 19))
        .opening(bracket(current.clone()))
        .executed(BoundGtidSet::new(current.clone(), GtidSet::empty()))
        .closing(bracket(current.clone()))
        .finalize();
    let mut changed_parts = current.parts().clone();
    changed_parts.observation_session =
        kuberic_mysql_core::ObservationSessionId::new("changed").unwrap();
    let incoherent = ObservationDraft::new(metadata(current.clone(), 10, 20, 30, 20))
        .opening(bracket(current.clone()))
        .executed(BoundGtidSet::new(current.clone(), GtidSet::empty()))
        .closing(bracket(kuberic_mysql_core::ExactBinding::new(
            changed_parts,
        )))
        .finalize();

    for outcome in [partial, stale, future, incoherent] {
        let mut authority = AuthoritySession::new(current.clone());
        let capability = authority.begin_attempt(&current).unwrap();
        assert_eq!(
            authority.complete(capability, &outcome, ObservationInstant::new(20)),
            Err(CompletionRejection::NonValidObservation)
        );
        assert!(!authority.has_observation_credit());
        assert!(!authority.access().write_open());
    }
}

#[test]
fn replacement_and_unsupported_requests_cannot_mutate_authority() {
    let original = binding();
    let mut authority = AuthoritySession::new(original.clone());
    let old_capability = authority.begin_attempt(&original).unwrap();

    let mut replacement_parts = original.parts().clone();
    replacement_parts.incarnation = ReplicaIncarnation::new("replacement").unwrap();
    let replacement = kuberic_mysql_core::ExactBinding::new(replacement_parts);
    authority.replace_binding(replacement.clone());
    assert_eq!(
        authority.complete(
            old_capability,
            &complete_outcome(&original),
            ObservationInstant::new(20)
        ),
        Err(CompletionRejection::StaleCapability)
    );

    let capability = authority.begin_attempt(&replacement).unwrap();
    let before_binding = authority.current_binding().clone();
    assert_eq!(
        authority
            .reject_unsupported(UnsupportedOperation::TopologyMutation)
            .operation(),
        UnsupportedOperation::TopologyMutation
    );
    assert_eq!(authority.current_binding(), &before_binding);
    assert!(authority.has_pending_attempt());
    assert_eq!(
        authority.complete(
            capability,
            &complete_outcome(&replacement),
            ObservationInstant::new(20)
        ),
        Ok(CompletionCredit::ObservationAcknowledged)
    );
    assert!(!authority.access().write_open());
}
