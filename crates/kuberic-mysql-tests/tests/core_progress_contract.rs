use std::str::FromStr;

use kuberic_mysql::core::{
    CatchUpCapabilityResult, CatchUpUnprovedReason, CatchUpUnsupportedReason,
    ConfiguredGtidLineage, GtidParseErrorKind, GtidSet, MAX_SEQUENCE, NonProjectableProgressReason,
    ProgressProjection, QualifiedProgressKind, project_progress,
};

const GROUP_UUID: &str = "d6f6f07e-37e1-4f7a-9105-73c13f634cb3";
const FOREIGN_UUID: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";

fn lineage() -> ConfiguredGtidLineage {
    ConfiguredGtidLineage::new(GROUP_UUID).expect("fixture lineage must be valid")
}

fn parse(value: &str) -> GtidSet {
    GtidSet::from_str(value).expect("fixture GTID set must parse")
}

#[test]
fn progress_projects_empty_and_exact_qualified_history() {
    let ProgressProjection::Projectable(initial) = project_progress(&lineage(), &GtidSet::empty())
    else {
        panic!("empty history must project");
    };
    assert_eq!(initial.value(), 0);
    assert_eq!(initial.kind(), QualifiedProgressKind::EmptyHistory);

    let executed = parse(&format!("{GROUP_UUID}:1-8"));
    let ProgressProjection::Projectable(progress) = project_progress(&lineage(), &executed) else {
        panic!("qualified history must project");
    };
    assert_eq!(progress.value(), 8);
    assert_eq!(progress.kind(), QualifiedProgressKind::ContiguousHistory);
    assert_eq!(progress.to_string(), "8");
    assert_eq!(executed.to_string(), format!("{GROUP_UUID}:1-8"));
}

#[test]
fn progress_rejects_each_non_projectable_shape_without_changing_native_history() {
    let cases = [
        (
            format!("{FOREIGN_UUID}:1"),
            NonProjectableProgressReason::ForeignSource,
        ),
        (
            format!("{GROUP_UUID}:tag:1"),
            NonProjectableProgressReason::TaggedSource,
        ),
        (
            format!("{GROUP_UUID}:1,{FOREIGN_UUID}:1"),
            NonProjectableProgressReason::MultipleSources,
        ),
        (
            format!("{GROUP_UUID}:1-2:4"),
            NonProjectableProgressReason::MultipleIntervals,
        ),
        (
            format!("{GROUP_UUID}:2-4"),
            NonProjectableProgressReason::DoesNotBeginAtOne,
        ),
    ];

    for (text, reason) in cases {
        let executed = parse(&text);
        let retained = executed.clone();
        assert_eq!(
            project_progress(&lineage(), &executed),
            ProgressProjection::NonProjectable(reason),
            "{text}"
        );
        assert_eq!(executed, retained, "{text}");
    }
}

#[test]
fn progress_projects_the_exact_maximum_and_parser_rejects_above_it() {
    let maximum = parse(&format!("{GROUP_UUID}:1-{MAX_SEQUENCE}"));
    let ProgressProjection::Projectable(progress) = project_progress(&lineage(), &maximum) else {
        panic!("maximum valid sequence must project");
    };
    assert_eq!(progress.value(), i64::try_from(MAX_SEQUENCE).unwrap());
    assert_eq!(progress.kind(), QualifiedProgressKind::ContiguousHistory);

    let error = GtidSet::from_str(&format!("{GROUP_UUID}:{}", MAX_SEQUENCE + 1)).unwrap_err();
    assert_eq!(error.kind(), &GtidParseErrorKind::SequenceOutOfRange);
}

#[test]
fn catch_up_is_independent_from_projectable_current_progress() {
    let current = project_progress(&lineage(), &parse(&format!("{GROUP_UUID}:1-8")));
    assert!(matches!(current, ProgressProjection::Projectable(_)));
    assert_eq!(
        CatchUpCapabilityResult::retained_evidence_unavailable(),
        CatchUpCapabilityResult::Unsupported(CatchUpUnsupportedReason::RetainedEvidenceUnavailable)
    );
    assert_eq!(
        CatchUpCapabilityResult::unproved(CatchUpUnprovedReason::IncompleteRetainedEvidence),
        CatchUpCapabilityResult::Unproved(CatchUpUnprovedReason::IncompleteRetainedEvidence)
    );
}
