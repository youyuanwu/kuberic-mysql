mod common;

use common::{binding, bracket, metadata};
use kuberic_mysql_core::{
    AdmissionError, AuthoritySession, BoundGtidSet, CollectionFailure, CompletionCredit,
    CompletionRejection, GtidSet, ObservationDraft, ObservationInstant, ObservationOutcome,
    UnsupportedOperation,
};

fn complete_outcome(
    binding: &kuberic_mysql_core::ExactBinding,
    decision: u64,
) -> ObservationOutcome {
    ObservationDraft::new(metadata(binding.clone(), 1, 2, 3, decision))
        .opening(bracket(binding.clone()))
        .executed(BoundGtidSet::new(binding.clone(), GtidSet::empty()))
        .closing(bracket(binding.clone()))
        .finalize()
}

fn valid_outcome(binding: &kuberic_mysql_core::ExactBinding) -> ObservationOutcome {
    complete_outcome(binding, 2)
}

#[test]
fn exact_current_valid_completion_receives_observation_credit_only() {
    let current = binding();
    let mut session = AuthoritySession::new(current.clone());
    let capability = session.begin_attempt(&current).unwrap();
    assert_eq!(
        session.complete(
            capability.clone(),
            &valid_outcome(&current),
            ObservationInstant::new(3)
        ),
        Ok(CompletionCredit::ObservationAcknowledged)
    );
    assert!(session.has_observation_credit());
    assert!(!session.access().read_open());
    assert!(!session.access().write_open());
    assert_eq!(
        session.complete(
            capability,
            &valid_outcome(&current),
            ObservationInstant::new(3)
        ),
        Err(CompletionRejection::StaleCapability)
    );
}

#[test]
fn every_binding_replacement_rejects_old_completion() {
    let original = binding();

    macro_rules! rejects_replacement {
        ($field:ident, $replacement:expr) => {{
            let mut session = AuthoritySession::new(original.clone());
            let capability = session.begin_attempt(&original).unwrap();
            let mut changed_parts = original.parts().clone();
            changed_parts.$field = $replacement;
            session.replace_binding(kuberic_mysql_core::ExactBinding::new(changed_parts));
            assert_eq!(
                session.complete(
                    capability,
                    &valid_outcome(&original),
                    ObservationInstant::new(2)
                ),
                Err(CompletionRejection::StaleCapability),
                stringify!($field)
            );
            assert!(!session.has_observation_credit());
        }};
    }

    rejects_replacement!(
        resource,
        kuberic_mysql_core::ResourceId::new("other").unwrap()
    );
    rejects_replacement!(
        partition,
        kuberic_mysql_core::PartitionId::new("other").unwrap()
    );
    rejects_replacement!(
        replica,
        kuberic_mysql_core::ReplicaId::new("other").unwrap()
    );
    rejects_replacement!(
        incarnation,
        kuberic_mysql_core::ReplicaIncarnation::new("other").unwrap()
    );
    rejects_replacement!(
        process_session,
        kuberic_mysql_core::ProcessSessionId::new("other").unwrap()
    );
    rejects_replacement!(
        observation_session,
        kuberic_mysql_core::ObservationSessionId::new("other").unwrap()
    );
    rejects_replacement!(
        attempt,
        kuberic_mysql_core::AttemptId::new("other").unwrap()
    );
    rejects_replacement!(
        endpoint,
        kuberic_mysql_core::EndpointBinding::new("other").unwrap()
    );
    rejects_replacement!(
        storage,
        kuberic_mysql_core::StorageBinding::new("other").unwrap()
    );
    rejects_replacement!(
        server_uuid,
        kuberic_mysql_core::ServerUuid::new("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap()
    );
    rejects_replacement!(
        group_name,
        kuberic_mysql_core::GroupName::new("other").unwrap()
    );
    rejects_replacement!(
        member_id,
        kuberic_mysql_core::MemberId::new("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap()
    );
    rejects_replacement!(
        member_address,
        kuberic_mysql_core::MemberAddress::new("other").unwrap()
    );
    rejects_replacement!(
        configuration,
        kuberic_mysql_core::ConfigurationId::new("other").unwrap()
    );
    rejects_replacement!(epoch, kuberic_mysql_core::Epoch::new("other").unwrap());
    rejects_replacement!(
        authority_generation,
        kuberic_mysql_core::AuthorityGeneration::new("other").unwrap()
    );
    rejects_replacement!(view_id, kuberic_mysql_core::ViewId::new("other").unwrap());
    rejects_replacement!(
        credential_generation,
        kuberic_mysql_core::CredentialGeneration::new("other").unwrap()
    );
}

#[test]
fn restored_equal_binding_does_not_resurrect_old_capability() {
    let original = binding();
    let mut session = AuthoritySession::new(original.clone());
    let old = session.begin_attempt(&original).unwrap();

    let mut changed_parts = original.parts().clone();
    changed_parts.incarnation = kuberic_mysql_core::ReplicaIncarnation::new("replacement").unwrap();
    session.replace_binding(kuberic_mysql_core::ExactBinding::new(changed_parts));
    session.replace_binding(original.clone());
    let current = session.begin_attempt(&original).unwrap();

    assert_eq!(
        session.complete(old, &valid_outcome(&original), ObservationInstant::new(2)),
        Err(CompletionRejection::StaleCapability)
    );
    assert!(session.has_pending_attempt());
    assert_eq!(
        session.complete(
            current,
            &valid_outcome(&original),
            ObservationInstant::new(2)
        ),
        Ok(CompletionCredit::ObservationAcknowledged)
    );
}

#[test]
fn native_identity_reuse_by_replacement_receives_no_credit() {
    let original = binding();
    let mut session = AuthoritySession::new(original.clone());
    let old = session.begin_attempt(&original).unwrap();
    let mut replacement_parts = original.parts().clone();
    replacement_parts.incarnation =
        kuberic_mysql_core::ReplicaIncarnation::new("replacement").unwrap();
    replacement_parts.storage = kuberic_mysql_core::StorageBinding::new("new-storage").unwrap();
    replacement_parts.process_session =
        kuberic_mysql_core::ProcessSessionId::new("new-process").unwrap();
    session.replace_binding(kuberic_mysql_core::ExactBinding::new(replacement_parts));
    assert_eq!(
        session.complete(old, &valid_outcome(&original), ObservationInstant::new(2)),
        Err(CompletionRejection::StaleCapability)
    );
    assert!(!session.has_observation_credit());
}

#[test]
fn every_non_valid_outcome_receives_no_credit() {
    let current = binding();
    let direct_failures = [
        CollectionFailure::Absent,
        CollectionFailure::Unreachable,
        CollectionFailure::PermissionDenied,
        CollectionFailure::AuthenticationFailure,
        CollectionFailure::Malformed(kuberic_mysql_core::MalformedReason::TimingOrder),
        CollectionFailure::Unsupported(kuberic_mysql_core::UnsupportedReason::CollectorCapability),
    ]
    .map(|failure| {
        ObservationDraft::new(metadata(current.clone(), 1, 2, 3, 2))
            .failure(failure)
            .finalize()
    });
    let partial = ObservationDraft::new(metadata(current.clone(), 1, 2, 3, 2)).finalize();
    let stale = complete_outcome(&current, 4);
    let future = complete_outcome(&current, 1);
    let mut changed_parts = current.parts().clone();
    changed_parts.view_id = kuberic_mysql_core::ViewId::new("other").unwrap();
    let incoherent = ObservationDraft::new(metadata(current.clone(), 1, 2, 3, 2))
        .opening(bracket(current.clone()))
        .executed(BoundGtidSet::new(current.clone(), GtidSet::empty()))
        .closing(bracket(kuberic_mysql_core::ExactBinding::new(
            changed_parts,
        )))
        .finalize();

    for outcome in direct_failures
        .into_iter()
        .chain([partial, stale, future, incoherent])
    {
        let mut session = AuthoritySession::new(current.clone());
        let capability = session.begin_attempt(&current).unwrap();
        assert_eq!(
            session.complete(capability, &outcome, ObservationInstant::new(2)),
            Err(CompletionRejection::NonValidObservation)
        );
        assert!(!session.has_observation_credit());
    }
}

#[test]
fn completion_rechecks_freshness() {
    let current = binding();
    let outcome = valid_outcome(&current);
    let mut expired = AuthoritySession::new(current.clone());
    let capability = expired.begin_attempt(&current).unwrap();
    assert_eq!(
        expired.complete(capability, &outcome, ObservationInstant::new(4)),
        Err(CompletionRejection::Expired)
    );

    let mut regression = AuthoritySession::new(current.clone());
    let capability = regression.begin_attempt(&current).unwrap();
    assert_eq!(
        regression.complete(capability, &outcome, ObservationInstant::new(1)),
        Err(CompletionRejection::DecisionRegression)
    );

    for decision in [2, 3] {
        let mut current_session = AuthoritySession::new(current.clone());
        let capability = current_session.begin_attempt(&current).unwrap();
        assert_eq!(
            current_session.complete(capability, &outcome, ObservationInstant::new(decision)),
            Ok(CompletionCredit::ObservationAcknowledged)
        );
    }
}

#[test]
fn valid_observation_for_different_binding_is_rejected_at_completion() {
    let current = binding();
    let mut changed_parts = current.parts().clone();
    changed_parts.attempt = kuberic_mysql_core::AttemptId::new("different").unwrap();
    let changed = kuberic_mysql_core::ExactBinding::new(changed_parts);
    let outcome = valid_outcome(&changed);

    let mut session = AuthoritySession::new(current.clone());
    let capability = session.begin_attempt(&current).unwrap();
    assert_eq!(
        session.complete(capability, &outcome, ObservationInstant::new(2)),
        Err(CompletionRejection::BindingMismatch)
    );
    assert!(!session.has_pending_attempt());
    assert!(!session.has_observation_credit());
    assert!(!session.access().read_open());
    assert!(!session.access().write_open());
}

#[test]
fn mismatched_admission_and_unsupported_operations_fail_closed() {
    let current = binding();
    let mut session = AuthoritySession::new(current.clone());
    let mut other_parts = current.parts().clone();
    other_parts.attempt = kuberic_mysql_core::AttemptId::new("other").unwrap();
    assert_eq!(
        session.begin_attempt(&kuberic_mysql_core::ExactBinding::new(other_parts)),
        Err(AdmissionError::BindingMismatch)
    );
    let _capability = session.begin_attempt(&current).unwrap();

    for operation in [
        UnsupportedOperation::TopologyMutation,
        UnsupportedOperation::Bootstrap,
        UnsupportedOperation::Join,
        UnsupportedOperation::Recovery,
        UnsupportedOperation::DestructiveRepair,
        UnsupportedOperation::DataLossAcceptance,
        UnsupportedOperation::AccessPublication,
    ] {
        let before_binding = session.current_binding().clone();
        let before_pending = session.has_pending_attempt();
        let before_credit = session.has_observation_credit();
        assert_eq!(session.reject_unsupported(operation).operation(), operation);
        assert_eq!(session.current_binding(), &before_binding);
        assert_eq!(session.has_pending_attempt(), before_pending);
        assert_eq!(session.has_observation_credit(), before_credit);
    }
    assert!(!session.access().read_open());
    assert!(!session.access().write_open());
}

#[test]
fn capabilities_cannot_cross_authority_sessions() {
    let current = binding();
    let mut first = AuthoritySession::new(current.clone());
    let mut second = AuthoritySession::new(current.clone());
    let first_capability = first.begin_attempt(&current).unwrap();
    let second_capability = second.begin_attempt(&current).unwrap();
    assert_ne!(first_capability, second_capability);

    assert_eq!(
        second.complete(
            first_capability,
            &valid_outcome(&current),
            ObservationInstant::new(2)
        ),
        Err(CompletionRejection::StaleCapability)
    );
    assert!(second.has_pending_attempt());
    assert_eq!(
        second.complete(
            second_capability,
            &valid_outcome(&current),
            ObservationInstant::new(2)
        ),
        Ok(CompletionCredit::ObservationAcknowledged)
    );
}
