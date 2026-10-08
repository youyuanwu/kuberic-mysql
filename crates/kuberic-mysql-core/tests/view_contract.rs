use std::str::FromStr;

use kuberic_mysql_core::{
    GroupName, GtidRelation, GtidSet, MemberAddress, MemberId, MemberRole, MemberState,
    NativeMember, NativeValueErrorKind, NativeView, NativeViewError, ViewId,
};

fn member(uuid: &str, address: &str, role: MemberRole, state: MemberState) -> NativeMember {
    NativeMember::new(
        MemberId::new(uuid).unwrap(),
        MemberAddress::new(address).unwrap(),
        role,
        state,
    )
}

fn view(members: Vec<NativeMember>) -> Result<NativeView, NativeViewError> {
    NativeView::new(
        GroupName::new("group").unwrap(),
        ViewId::new("view").unwrap(),
        members,
    )
}

#[test]
fn role_and_state_decoders_are_strict() {
    assert_eq!(
        MemberRole::from_str("PRIMARY").unwrap(),
        MemberRole::Primary
    );
    assert_eq!(
        MemberRole::from_str("SECONDARY").unwrap(),
        MemberRole::Secondary
    );
    for (text, expected) in [
        ("ONLINE", MemberState::Online),
        ("RECOVERING", MemberState::Recovering),
        ("OFFLINE", MemberState::Offline),
        ("ERROR", MemberState::Error),
        ("UNREACHABLE", MemberState::Unreachable),
    ] {
        assert_eq!(MemberState::from_str(text).unwrap(), expected);
    }
    assert_eq!(
        MemberRole::from_str("").unwrap_err().kind(),
        NativeValueErrorKind::Malformed
    );
    assert_eq!(
        MemberState::from_str("UNKNOWN").unwrap_err().kind(),
        NativeValueErrorKind::Unsupported
    );
}

#[test]
fn member_order_does_not_change_view_equality() {
    let first = member(
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "a:3306",
        MemberRole::Primary,
        MemberState::Online,
    );
    let second = member(
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        "b:3306",
        MemberRole::Secondary,
        MemberState::Recovering,
    );
    assert_eq!(
        view(vec![first.clone(), second.clone()]).unwrap(),
        view(vec![second, first]).unwrap()
    );
}

#[test]
fn duplicate_member_identity_and_address_are_distinct_errors() {
    let first = member(
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "a:3306",
        MemberRole::Primary,
        MemberState::Online,
    );
    let duplicate_id = member(
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "b:3306",
        MemberRole::Secondary,
        MemberState::Online,
    );
    assert_eq!(
        view(vec![first.clone(), duplicate_id]).unwrap_err(),
        NativeViewError::DuplicateMemberId
    );

    let duplicate_address = member(
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        "a:3306",
        MemberRole::Secondary,
        MemberState::Online,
    );
    assert_eq!(
        view(vec![first, duplicate_address]).unwrap_err(),
        NativeViewError::DuplicateMemberAddress
    );
}

#[test]
fn changed_member_fact_changes_exact_view() {
    let original = view(vec![member(
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "a:3306",
        MemberRole::Primary,
        MemberState::Online,
    )])
    .unwrap();

    let variants = [
        member(
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "a:3306",
            MemberRole::Primary,
            MemberState::Online,
        ),
        member(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "b:3306",
            MemberRole::Primary,
            MemberState::Online,
        ),
        member(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "a:3306",
            MemberRole::Secondary,
            MemberState::Online,
        ),
        member(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "a:3306",
            MemberRole::Primary,
            MemberState::Recovering,
        ),
    ];
    for changed in variants {
        assert_ne!(original, view(vec![changed]).unwrap());
    }
}

#[test]
fn native_view_facts_do_not_change_gtid_relations() {
    let left: GtidSet = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1".parse().unwrap();
    let right: GtidSet = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1".parse().unwrap();
    let before = left.relation(&right);

    let _changed_view = NativeView::new(
        GroupName::new("other-group").unwrap(),
        ViewId::new("other-view").unwrap(),
        vec![member(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "a:3306",
            MemberRole::Secondary,
            MemberState::Offline,
        )],
    )
    .unwrap();

    assert_eq!(before, GtidRelation::Incomparable);
    assert_eq!(left.relation(&right), before);
}
