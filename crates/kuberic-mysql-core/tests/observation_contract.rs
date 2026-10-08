mod common;

use std::str::FromStr;

use common::{binding, bracket, metadata, native_view};
use kuberic_mysql_core::{
    BoundGtidSet, CollectionFailure, GtidSet, IncoherentReason, MalformedReason, MemberId,
    MemberRole, MemberState, NativeField, NativeValueErrorKind, ObservationBracket,
    ObservationDraft, ObservationField, ObservationOutcome, StaleReason, UnsupportedReason,
};

fn complete_draft(start: u64, end: u64, deadline: u64, decision: u64) -> ObservationDraft {
    let binding = binding();
    ObservationDraft::new(metadata(binding.clone(), start, end, deadline, decision))
        .opening(bracket(binding.clone()))
        .executed(BoundGtidSet::new(binding.clone(), GtidSet::empty()))
        .closing(bracket(binding))
}

fn assert_metadata(
    outcome: &ObservationOutcome,
    expected_binding: &kuberic_mysql_core::ExactBinding,
    start: u64,
    end: u64,
    deadline: u64,
    decision: u64,
) {
    let metadata = outcome.metadata();
    assert_eq!(metadata.binding(), expected_binding);
    assert_eq!(metadata.provenance().origin(), "fixture");
    assert_eq!(metadata.start().tick(), start);
    assert_eq!(metadata.end().tick(), end);
    assert_eq!(metadata.deadline().tick(), deadline);
    assert_eq!(metadata.decision().tick(), decision);
}

#[test]
fn complete_explicitly_empty_observation_is_valid() {
    let outcome = complete_draft(1, 2, 3, 2).finalize();
    let valid = outcome.valid().expect("must be valid");
    assert!(valid.executed().entries().is_empty());
    assert_eq!(valid.metadata().provenance().origin(), "fixture");
}

#[test]
fn direct_failure_outcomes_remain_distinct_and_retain_metadata() {
    let failures = [
        (CollectionFailure::Absent, "absent"),
        (CollectionFailure::Unreachable, "unreachable"),
        (CollectionFailure::PermissionDenied, "permission"),
        (CollectionFailure::AuthenticationFailure, "authentication"),
        (
            CollectionFailure::Malformed(MalformedReason::TimingOrder),
            "malformed",
        ),
        (
            CollectionFailure::Unsupported(
                kuberic_mysql_core::UnsupportedReason::CollectorCapability,
            ),
            "unsupported",
        ),
    ];
    for (failure, expected) in failures {
        let attempted = binding();
        let outcome = ObservationDraft::new(metadata(attempted.clone(), 1, 2, 3, 2))
            .failure(failure)
            .finalize();
        assert_metadata(&outcome, &attempted, 1, 2, 3, 2);
        assert!(outcome.valid().is_none());
        let actual = match outcome {
            ObservationOutcome::Absent(_) => "absent",
            ObservationOutcome::Unreachable(_) => "unreachable",
            ObservationOutcome::PermissionDenied(_) => "permission",
            ObservationOutcome::AuthenticationFailure(_) => "authentication",
            ObservationOutcome::Malformed { reason, .. } => {
                assert_eq!(reason, MalformedReason::TimingOrder);
                "malformed"
            }
            ObservationOutcome::Unsupported { reason, .. } => {
                assert_eq!(reason, UnsupportedReason::CollectorCapability);
                "unsupported"
            }
            _ => panic!("unexpected outcome"),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn partial_stale_future_incoherent_and_valid_retain_full_metadata() {
        let attempted = binding();
        let partial = ObservationDraft::new(metadata(attempted.clone(), 1, 2, 3, 2)).finalize();
        assert_metadata(&partial, &attempted, 1, 2, 3, 2);

        let future = complete_draft(1, 2, 3, 1).finalize();
        assert_metadata(&future, &attempted, 1, 2, 3, 1);

        let stale = complete_draft(1, 2, 3, 4).finalize();
        assert_metadata(&stale, &attempted, 1, 2, 3, 4);

        let valid = complete_draft(1, 2, 3, 2).finalize();
        assert_metadata(&valid, &attempted, 1, 2, 3, 2);

        let mut changed_parts = attempted.parts().clone();
        changed_parts.credential_generation =
            kuberic_mysql_core::CredentialGeneration::new("other").unwrap();
        let incoherent = ObservationDraft::new(metadata(attempted.clone(), 1, 2, 3, 2))
            .opening(bracket(attempted.clone()))
            .executed(BoundGtidSet::new(attempted.clone(), GtidSet::empty()))
            .closing(bracket(kuberic_mysql_core::ExactBinding::new(
                changed_parts,
            )))
            .finalize();
        assert_metadata(&incoherent, &attempted, 1, 2, 3, 2);
    }
}

#[test]
fn missing_and_failed_partial_collection_never_become_valid() {
    let attempted = binding();
    let partial = ObservationDraft::new(metadata(attempted.clone(), 1, 2, 3, 2))
        .opening(bracket(attempted))
        .failure(CollectionFailure::AuthenticationFailure)
        .finalize();
    assert!(matches!(
        partial,
        ObservationOutcome::Partial {
            cause: Some(CollectionFailure::AuthenticationFailure),
            ..
        }
    ));

    let missing = ObservationDraft::new(metadata(binding(), 1, 2, 3, 2)).finalize();
    assert!(matches!(
        missing,
        ObservationOutcome::Partial {
            missing,
            cause: None,
            ..
        } if missing == [
            ObservationField::OpeningBracket,
            ObservationField::ExecutedGtidSet,
            ObservationField::ClosingBracket
        ]
    ));

    assert!(matches!(
        ObservationDraft::new(metadata(binding(), 1, 2, 3, 0)).finalize(),
        ObservationOutcome::Partial { .. }
    ));
    assert!(matches!(
        ObservationDraft::new(metadata(binding(), 1, 2, 3, 4)).finalize(),
        ObservationOutcome::Partial { .. }
    ));
}

#[test]
fn freshness_boundaries_are_deterministic() {
    assert!(matches!(
        complete_draft(1, 2, 3, 1).finalize(),
        ObservationOutcome::FutureDated(_)
    ));
    assert!(complete_draft(1, 2, 3, 2).finalize().valid().is_some());
    assert!(complete_draft(1, 2, 3, 3).finalize().valid().is_some());
    assert!(matches!(
        complete_draft(1, 2, 3, 4).finalize(),
        ObservationOutcome::Stale {
            reason: StaleReason::Expired,
            ..
        }
    ));
    assert!(complete_draft(2, 2, 2, 2).finalize().valid().is_some());
    assert!(matches!(
        complete_draft(2, 1, 3, 2).finalize(),
        ObservationOutcome::Malformed {
            reason: MalformedReason::TimingOrder,
            ..
        }
    ));
    assert!(matches!(
        complete_draft(1, 3, 2, 2).finalize(),
        ObservationOutcome::Malformed {
            reason: MalformedReason::TimingOrder,
            ..
        }
    ));
}

#[test]
fn changed_binding_or_view_is_incoherent() {
    let attempted = binding();
    let mut changed_parts = attempted.parts().clone();
    changed_parts.observation_session =
        kuberic_mysql_core::ObservationSessionId::new("other-session").unwrap();
    let changed = kuberic_mysql_core::ExactBinding::new(changed_parts);
    let outcome = ObservationDraft::new(metadata(attempted.clone(), 1, 2, 3, 2))
        .opening(bracket(attempted.clone()))
        .executed(BoundGtidSet::new(
            attempted.clone(),
            GtidSet::from_str("").unwrap(),
        ))
        .closing(bracket(changed))
        .finalize();
    assert!(matches!(
        outcome,
        ObservationOutcome::Incoherent {
            reason: IncoherentReason::BindingMismatch,
            ..
        }
    ));

    let changed_view = kuberic_mysql_core::NativeView::new(
        kuberic_mysql_core::GroupName::new("group").unwrap(),
        kuberic_mysql_core::ViewId::new("view").unwrap(),
        vec![kuberic_mysql_core::NativeMember::new(
            MemberId::new(common::UUID_A).unwrap(),
            kuberic_mysql_core::MemberAddress::new("a:3306").unwrap(),
            MemberRole::Secondary,
            kuberic_mysql_core::MemberState::Online,
        )],
    )
    .unwrap();
    let outcome = ObservationDraft::new(metadata(attempted.clone(), 1, 2, 3, 2))
        .opening(ObservationBracket::new(attempted.clone(), native_view()))
        .executed(BoundGtidSet::new(attempted.clone(), GtidSet::empty()))
        .closing(ObservationBracket::new(attempted, changed_view))
        .finalize();
    assert!(matches!(
        outcome,
        ObservationOutcome::Incoherent {
            reason: IncoherentReason::BracketMismatch,
            ..
        }
    ));
}

#[test]
fn stable_but_wrong_local_member_identity_is_incoherent() {
    let base = binding();
    let mut parts = base.parts().clone();
    parts.member_id = MemberId::new("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let attempted = kuberic_mysql_core::ExactBinding::new(parts);
    let wrong_view = kuberic_mysql_core::NativeView::new(
        attempted.parts().group_name.clone(),
        attempted.parts().view_id.clone(),
        vec![kuberic_mysql_core::NativeMember::new(
            MemberId::new("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
            attempted.parts().member_address.clone(),
            MemberRole::Primary,
            kuberic_mysql_core::MemberState::Online,
        )],
    )
    .unwrap();
    let outcome = ObservationDraft::new(metadata(attempted.clone(), 1, 2, 3, 2))
        .opening(ObservationBracket::new(
            attempted.clone(),
            wrong_view.clone(),
        ))
        .executed(BoundGtidSet::new(attempted.clone(), GtidSet::empty()))
        .closing(ObservationBracket::new(attempted, wrong_view))
        .finalize();
    assert!(matches!(
        outcome,
        ObservationOutcome::Incoherent {
            reason: IncoherentReason::LocalMemberMismatch,
            ..
        }
    ));
}

#[test]
fn native_decode_errors_map_to_structured_outcomes() {
    let malformed = MemberRole::from_str("").unwrap_err();
    assert_eq!(malformed.kind(), NativeValueErrorKind::Malformed);
    assert!(matches!(
        ObservationDraft::new(metadata(binding(), 1, 2, 3, 2))
            .native_decode_failure(&malformed)
            .finalize(),
        ObservationOutcome::Malformed {
            reason: MalformedReason::NativeValue(NativeField::Role),
            ..
        }
    ));

    let unsupported = MemberRole::from_str("ARBITER").unwrap_err();
    assert_eq!(unsupported.kind(), NativeValueErrorKind::Unsupported);
    assert!(matches!(
        ObservationDraft::new(metadata(binding(), 1, 2, 3, 2))
            .native_decode_failure(&unsupported)
            .finalize(),
        ObservationOutcome::Unsupported {
            reason: UnsupportedReason::NativeValue(NativeField::Role),
            ..
        }
    ));

    let malformed_state = MemberState::from_str("\n").unwrap_err();
    assert!(matches!(
        ObservationDraft::new(metadata(binding(), 1, 2, 3, 2))
            .native_decode_failure(&malformed_state)
            .finalize(),
        ObservationOutcome::Malformed {
            reason: MalformedReason::NativeValue(NativeField::State),
            ..
        }
    ));
    let unsupported_state = MemberState::from_str("DONOR").unwrap_err();
    assert!(matches!(
        ObservationDraft::new(metadata(binding(), 1, 2, 3, 2))
            .native_decode_failure(&unsupported_state)
            .finalize(),
        ObservationOutcome::Unsupported {
            reason: UnsupportedReason::NativeValue(NativeField::State),
            ..
        }
    ));
}

#[test]
fn every_binding_dimension_drift_is_incoherent() {
    let attempted = binding();

    macro_rules! reject_change {
        ($field:ident, $replacement:expr) => {{
            let mut parts = attempted.parts().clone();
            parts.$field = $replacement;
            let changed = kuberic_mysql_core::ExactBinding::new(parts);
            let outcome = ObservationDraft::new(metadata(attempted.clone(), 1, 2, 3, 2))
                .opening(bracket(attempted.clone()))
                .executed(BoundGtidSet::new(attempted.clone(), GtidSet::empty()))
                .closing(bracket(changed))
                .finalize();
            assert!(matches!(
                outcome,
                ObservationOutcome::Incoherent {
                    reason: IncoherentReason::BindingMismatch,
                    ..
                }
            ));
        }};
    }

    reject_change!(
        resource,
        kuberic_mysql_core::ResourceId::new("other").unwrap()
    );
    reject_change!(
        partition,
        kuberic_mysql_core::PartitionId::new("other").unwrap()
    );
    reject_change!(
        replica,
        kuberic_mysql_core::ReplicaId::new("other").unwrap()
    );
    reject_change!(
        incarnation,
        kuberic_mysql_core::ReplicaIncarnation::new("other").unwrap()
    );
    reject_change!(
        process_session,
        kuberic_mysql_core::ProcessSessionId::new("other").unwrap()
    );
    reject_change!(
        observation_session,
        kuberic_mysql_core::ObservationSessionId::new("other").unwrap()
    );
    reject_change!(
        attempt,
        kuberic_mysql_core::AttemptId::new("other").unwrap()
    );
    reject_change!(
        endpoint,
        kuberic_mysql_core::EndpointBinding::new("other").unwrap()
    );
    reject_change!(
        storage,
        kuberic_mysql_core::StorageBinding::new("other").unwrap()
    );
    reject_change!(
        server_uuid,
        kuberic_mysql_core::ServerUuid::new("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap()
    );
    reject_change!(
        group_name,
        kuberic_mysql_core::GroupName::new("other").unwrap()
    );
    reject_change!(
        member_id,
        MemberId::new("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap()
    );
    reject_change!(
        member_address,
        kuberic_mysql_core::MemberAddress::new("other").unwrap()
    );
    reject_change!(
        configuration,
        kuberic_mysql_core::ConfigurationId::new("other").unwrap()
    );
    reject_change!(epoch, kuberic_mysql_core::Epoch::new("other").unwrap());
    reject_change!(
        authority_generation,
        kuberic_mysql_core::AuthorityGeneration::new("other").unwrap()
    );
    reject_change!(view_id, kuberic_mysql_core::ViewId::new("other").unwrap());
    reject_change!(
        credential_generation,
        kuberic_mysql_core::CredentialGeneration::new("other").unwrap()
    );
}
