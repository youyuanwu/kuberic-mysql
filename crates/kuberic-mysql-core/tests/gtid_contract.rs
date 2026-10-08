use std::str::FromStr;

use kuberic_mysql_core::{GtidParseErrorKind, GtidRelation, GtidSet, GtidTag, MAX_SEQUENCE};

const A: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const B: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

fn parse(value: &str) -> GtidSet {
    GtidSet::from_str(value).expect("fixture must parse")
}

#[test]
fn empty_and_equivalent_histories_normalize() {
    assert_eq!(parse("").to_string(), "");
    let set = parse(&format!("{A}:5:1-3:3-6,{A}:7:8"));
    assert_eq!(set.to_string(), format!("{A}:1-8"));
    assert_eq!(parse(&set.to_string()), set);
}

#[test]
fn sources_and_tags_remain_distinct_and_sorted() {
    let set = parse(&format!("{B}:tag_b:2,{A}:2,{B}:tag_a:1"));
    assert_eq!(set.to_string(), format!("{A}:2,{B}:tag_a:1,{B}:tag_b:2"));
    assert_eq!(GtidTag::new("_tag9").unwrap().as_str(), "_tag9");
}

#[test]
fn relations_use_only_set_containment() {
    let empty = parse("");
    let small = parse(&format!("{A}:1-2"));
    let large = parse(&format!("{A}:1-4"));
    let divergent = parse(&format!("{A}:1,{B}:1"));

    assert_eq!(small.relation(&small), GtidRelation::Equal);
    assert_eq!(empty.relation(&small), GtidRelation::ProperSubset);
    assert_eq!(small.relation(&large), GtidRelation::ProperSubset);
    assert_eq!(large.relation(&small), GtidRelation::ProperSuperset);
    assert_eq!(large.relation(&divergent), GtidRelation::Incomparable);

    let shorter_text = parse(&format!("{B}:1"));
    let longer_text = parse(&format!("{A}:1-2"));
    assert_eq!(
        shorter_text.relation(&longer_text),
        GtidRelation::Incomparable
    );
}

#[test]
fn parser_rejects_structured_malformed_inputs() {
    let cases = [
        (A.to_owned(), GtidParseErrorKind::MissingInterval),
        (format!("{A}:"), GtidParseErrorKind::EmptyInterval),
        (format!("{A}:0"), GtidParseErrorKind::SequenceOutOfRange),
        (
            format!("{A}:{}", MAX_SEQUENCE + 1),
            GtidParseErrorKind::SequenceOutOfRange,
        ),
        (format!("{A}:2-2"), GtidParseErrorKind::InvalidRange),
        (format!("{A}:3-2"), GtidParseErrorKind::InvalidRange),
        (format!("{A}:+1"), GtidParseErrorKind::InvalidSequence),
        (format!("{A}:1-2-3"), GtidParseErrorKind::InvalidRange),
        (format!("{A}:bad-tag:1"), GtidParseErrorKind::InvalidTag),
        (
            format!("{A}:{}:1", "a".repeat(33)),
            GtidParseErrorKind::InvalidTag,
        ),
        (format!("{A}:1,"), GtidParseErrorKind::EmptySource),
        (
            "00000000-0000-0000-0000-000000000000:1".to_owned(),
            GtidParseErrorKind::InvalidUuid,
        ),
        ("not-a-uuid:1".to_owned(), GtidParseErrorKind::InvalidUuid),
        (format!("{A}:1-"), GtidParseErrorKind::InvalidRange),
        (format!("{A}:-2"), GtidParseErrorKind::InvalidRange),
    ];
    for (text, expected) in cases {
        let error = GtidSet::from_str(&text).unwrap_err();
        assert_eq!(*error.kind(), expected, "{text}");
    }
}

#[test]
fn malformed_range_errors_retain_location() {
    for text in [format!("{A}:1-"), format!("{A}:-2")] {
        let error = GtidSet::from_str(&text).unwrap_err();
        assert_eq!(error.kind(), &GtidParseErrorKind::InvalidRange);
        assert_eq!(error.component(), 0);
        assert_eq!(error.token(), 1);
    }
}

#[test]
fn maximum_interval_does_not_overflow_normalization() {
    let set = parse(&format!("{A}:{}:{MAX_SEQUENCE}", MAX_SEQUENCE - 1));
    assert_eq!(
        set.to_string(),
        format!("{A}:{}-{MAX_SEQUENCE}", MAX_SEQUENCE - 1)
    );
}
