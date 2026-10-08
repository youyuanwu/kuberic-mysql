//! Exact schema validation and native-evidence decoding.

use core::str::FromStr;
use std::collections::HashSet;

use kuberic_mysql_core::{
    ExactBinding, GroupName, GroupReplicationAddress, GtidSet, MemberAddress, MemberId, MemberRole,
    MemberState, NativeAccessState, NativeLocalState, NativeMember, NativeSnapshot, NativeSwitch,
    NativeValueErrorKind, NativeView, NativeViewError, ServerUuid, ViewId,
};

use crate::diagnostic::{
    AdapterDiagnostic, AmbiguityKind, EvidenceIssue, IdentityField, NativeSurface, PlaceholderKind,
    ProductIssue, SchemaIssue,
};
use crate::query::{QueryId, RawResult, RawValue};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProductIdentity {
    pub(crate) server_uuid: ServerUuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MembershipEvidence {
    Absent,
    Placeholder(PlaceholderKind),
    Active(Vec<NativeMember>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ViewEvidence {
    Absent,
    Placeholder(PlaceholderKind),
    Active {
        member_id: MemberId,
        view_id: ViewId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LocalEvidence {
    pub(crate) group_name: GroupName,
    pub(crate) local: NativeLocalState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DecodeError {
    pub(crate) diagnostic: AdapterDiagnostic,
}

impl DecodeError {
    fn new(diagnostic: AdapterDiagnostic) -> Self {
        Self { diagnostic }
    }
}

pub(crate) fn decode_product(result: &RawResult) -> Result<ProductIdentity, DecodeError> {
    validate_schema(QueryId::Mysql8411ProductIdentityV1, result)?;
    let row = single_row(QueryId::Mysql8411ProductIdentityV1, result)?;
    let version = required_text(row, 0, "version", NativeSurface::ProductIdentity)?;
    let comment = required_text(row, 1, "version_comment", NativeSurface::ProductIdentity)?;
    let machine = required_text(
        row,
        2,
        "version_compile_machine",
        NativeSurface::ProductIdentity,
    )?;
    let operating_system =
        required_text(row, 3, "version_compile_os", NativeSurface::ProductIdentity)?;
    if comment != "MySQL Community Server - GPL" {
        return Err(DecodeError::new(AdapterDiagnostic::Product(
            ProductIssue::NonOracleCommunity,
        )));
    }
    if version != "8.4.11" {
        return Err(DecodeError::new(AdapterDiagnostic::Product(
            ProductIssue::UnsupportedPatch,
        )));
    }
    if machine != "x86_64" || operating_system != "Linux" {
        return Err(DecodeError::new(AdapterDiagnostic::Product(
            ProductIssue::UnsupportedPlatform,
        )));
    }
    let server_uuid = ServerUuid::new(required_text(
        row,
        4,
        "server_uuid",
        NativeSurface::ProductIdentity,
    )?)
    .map_err(|_| malformed_cell(NativeSurface::ProductIdentity, "server_uuid"))?;
    Ok(ProductIdentity { server_uuid })
}

pub(crate) fn validate_server_identity(
    binding: &ExactBinding,
    product: &ProductIdentity,
) -> Result<(), DecodeError> {
    if product.server_uuid != binding.parts().server_uuid {
        Err(DecodeError::new(AdapterDiagnostic::UnexpectedIdentity {
            field: IdentityField::ServerUuid,
        }))
    } else {
        Ok(())
    }
}

pub(crate) fn decode_local_state(result: &RawResult) -> Result<LocalEvidence, DecodeError> {
    validate_schema(QueryId::Mysql8411LocalStateV1, result)?;
    let row = single_row(QueryId::Mysql8411LocalStateV1, result)?;
    let group_name = GroupName::new(required_text(
        row,
        0,
        "group_name",
        NativeSurface::LocalState,
    )?)
    .map_err(|_| malformed_cell(NativeSurface::LocalState, "group_name"))?;
    let group_replication_address = GroupReplicationAddress::new(required_text(
        row,
        1,
        "group_replication_address",
        NativeSurface::LocalState,
    )?)
    .map_err(|_| malformed_cell(NativeSurface::LocalState, "group_replication_address"))?;
    let read_only = decode_switch(row, 2, "read_only")?;
    let super_read_only = decode_switch(row, 3, "super_read_only")?;
    Ok(LocalEvidence {
        group_name,
        local: NativeLocalState::new(
            group_replication_address,
            NativeAccessState::new(read_only, super_read_only),
        ),
    })
}

pub(crate) fn decode_members(result: &RawResult) -> Result<MembershipEvidence, DecodeError> {
    validate_schema(QueryId::Mysql8411GroupMembersV1, result)?;
    if result.rows.is_empty() {
        return Ok(MembershipEvidence::Absent);
    }
    if result.rows.len() == 1 && is_never_started_member(&result.rows[0]) {
        return Ok(MembershipEvidence::Placeholder(
            PlaceholderKind::NeverStarted,
        ));
    }
    if result.rows.len() == 1 && is_stopped_member(&result.rows[0]) {
        return Ok(MembershipEvidence::Placeholder(PlaceholderKind::Stopped));
    }

    let mut members = Vec::with_capacity(result.rows.len());
    let mut member_ids = HashSet::with_capacity(result.rows.len());
    let mut member_addresses = HashSet::with_capacity(result.rows.len());
    for (index, row) in result.rows.iter().enumerate() {
        ensure_complete_row(QueryId::Mysql8411GroupMembersV1, row, index)?;
        let id = MemberId::new(required_text(
            row,
            0,
            "member_id",
            NativeSurface::GroupMembers,
        )?)
        .map_err(|_| malformed_cell(NativeSurface::GroupMembers, "member_id"))?;
        let host = required_text(row, 1, "member_host", NativeSurface::GroupMembers)?;
        let port = required_port(row, 2, "member_port", NativeSurface::GroupMembers)?;
        let address = MemberAddress::new(format_member_address(host, port))
            .map_err(|_| malformed_cell(NativeSurface::GroupMembers, "member_host"))?;
        let state = decode_member_state(required_text(
            row,
            3,
            "member_state",
            NativeSurface::GroupMembers,
        )?)?;
        let role = decode_member_role(required_text(
            row,
            4,
            "member_role",
            NativeSurface::GroupMembers,
        )?)?;
        if !member_ids.insert(id.as_str().to_owned()) {
            return Err(DecodeError::new(AdapterDiagnostic::Ambiguous {
                surface: NativeSurface::GroupMembers,
                kind: AmbiguityKind::DuplicateMemberId,
            }));
        }
        if !member_addresses.insert(address.as_str().to_owned()) {
            return Err(DecodeError::new(AdapterDiagnostic::Ambiguous {
                surface: NativeSurface::GroupMembers,
                kind: AmbiguityKind::DuplicateMemberAddress,
            }));
        }
        members.push(NativeMember::new(id, address, role, state));
    }
    Ok(MembershipEvidence::Active(members))
}

pub(crate) fn decode_view(result: &RawResult) -> Result<ViewEvidence, DecodeError> {
    validate_schema(QueryId::Mysql8411LocalMemberStatsV1, result)?;
    if result.rows.is_empty() {
        return Ok(ViewEvidence::Absent);
    }
    if result.rows.len() > 1 {
        return Err(DecodeError::new(AdapterDiagnostic::Ambiguous {
            surface: NativeSurface::LocalMemberStats,
            kind: AmbiguityKind::DuplicateLocalRow,
        }));
    }
    let row = &result.rows[0];
    ensure_complete_row(QueryId::Mysql8411LocalMemberStatsV1, row, 0)?;
    if text(row.first()) == Some("") && text(row.get(1)) == Some("") {
        return Ok(ViewEvidence::Placeholder(PlaceholderKind::NeverStarted));
    }
    let member_id = MemberId::new(required_text(
        row,
        0,
        "member_id",
        NativeSurface::LocalMemberStats,
    )?)
    .map_err(|_| malformed_cell(NativeSurface::LocalMemberStats, "member_id"))?;
    let view_id = ViewId::new(required_text(
        row,
        1,
        "view_id",
        NativeSurface::LocalMemberStats,
    )?)
    .map_err(|_| malformed_cell(NativeSurface::LocalMemberStats, "view_id"))?;
    Ok(ViewEvidence::Active { member_id, view_id })
}

pub(crate) fn decode_executed_gtids(result: &RawResult) -> Result<GtidSet, DecodeError> {
    validate_schema(QueryId::Mysql8411ExecutedGtidsV1, result)?;
    let row = single_row(QueryId::Mysql8411ExecutedGtidsV1, result)?;
    let value = required_text(row, 0, "gtid_executed", NativeSurface::ExecutedGtids)?;
    GtidSet::from_str(value).map_err(|error| {
        DecodeError::new(AdapterDiagnostic::Evidence {
            surface: NativeSurface::ExecutedGtids,
            issue: EvidenceIssue::MalformedGtid {
                kind: *error.kind(),
                component: error.component(),
                token: error.token(),
            },
        })
    })
}

pub(crate) fn assemble_snapshot(
    binding: &ExactBinding,
    local: LocalEvidence,
    members: MembershipEvidence,
    view: ViewEvidence,
) -> Result<NativeSnapshot, DecodeError> {
    if local.group_name != binding.parts().group_name {
        return Err(DecodeError::new(AdapterDiagnostic::UnexpectedIdentity {
            field: IdentityField::GroupName,
        }));
    }
    let members = match members {
        MembershipEvidence::Absent => {
            return Err(DecodeError::new(AdapterDiagnostic::Absent {
                surface: NativeSurface::GroupMembers,
            }));
        }
        MembershipEvidence::Placeholder(kind) => {
            return Err(DecodeError::new(AdapterDiagnostic::Placeholder(kind)));
        }
        MembershipEvidence::Active(members) => members,
    };
    let (member_id, view_id) = match view {
        ViewEvidence::Absent => {
            return Err(DecodeError::new(AdapterDiagnostic::Absent {
                surface: NativeSurface::LocalMemberStats,
            }));
        }
        ViewEvidence::Placeholder(kind) => {
            return Err(DecodeError::new(AdapterDiagnostic::Placeholder(kind)));
        }
        ViewEvidence::Active { member_id, view_id } => (member_id, view_id),
    };
    if member_id != binding.parts().member_id {
        return Err(DecodeError::new(AdapterDiagnostic::UnexpectedIdentity {
            field: IdentityField::LocalMemberId,
        }));
    }
    if view_id != binding.parts().view_id {
        return Err(DecodeError::new(AdapterDiagnostic::UnexpectedIdentity {
            field: IdentityField::ViewId,
        }));
    }
    if !members
        .iter()
        .any(|member| member.id() == &binding.parts().member_id)
    {
        return Err(DecodeError::new(AdapterDiagnostic::Ambiguous {
            surface: NativeSurface::GroupMembers,
            kind: AmbiguityKind::MissingLocalMember,
        }));
    }
    let view = NativeView::new(local.group_name, view_id, members).map_err(|error| {
        let kind = match error {
            NativeViewError::DuplicateMemberId => AmbiguityKind::DuplicateMemberId,
            NativeViewError::DuplicateMemberAddress => AmbiguityKind::DuplicateMemberAddress,
        };
        DecodeError::new(AdapterDiagnostic::Ambiguous {
            surface: NativeSurface::GroupMembers,
            kind,
        })
    })?;
    Ok(NativeSnapshot::new(view, local.local))
}

fn validate_schema(query: QueryId, result: &RawResult) -> Result<(), DecodeError> {
    let expected = query.columns();
    if result.columns.len() > expected.len() {
        return Err(DecodeError::new(AdapterDiagnostic::Schema {
            surface: query.surface(),
            issue: SchemaIssue::AdditionalColumns {
                expected: expected.len(),
                actual: result.columns.len(),
            },
        }));
    }
    for (index, contract) in expected.iter().copied().enumerate() {
        let Some(actual) = result.columns.get(index) else {
            return Err(DecodeError::new(AdapterDiagnostic::Schema {
                surface: query.surface(),
                issue: SchemaIssue::MissingColumn {
                    index,
                    expected: contract.name(),
                },
            }));
        };
        if actual.name != contract.name() {
            return Err(DecodeError::new(AdapterDiagnostic::Schema {
                surface: query.surface(),
                issue: SchemaIssue::ChangedColumn {
                    index,
                    expected: contract.name(),
                    actual: actual.name.clone(),
                },
            }));
        }
        if actual.kind != contract.kind() {
            return Err(DecodeError::new(AdapterDiagnostic::Schema {
                surface: query.surface(),
                issue: SchemaIssue::ChangedType {
                    index,
                    expected: contract.kind(),
                    actual: actual.kind,
                },
            }));
        }
        if actual.nullable != contract.nullable() {
            return Err(DecodeError::new(AdapterDiagnostic::Schema {
                surface: query.surface(),
                issue: SchemaIssue::ChangedNullability {
                    index,
                    expected: contract.nullable(),
                    actual: actual.nullable,
                },
            }));
        }
    }
    Ok(())
}

fn single_row(query: QueryId, result: &RawResult) -> Result<&[RawValue], DecodeError> {
    if result.rows.is_empty() {
        return Err(DecodeError::new(AdapterDiagnostic::Absent {
            surface: query.surface(),
        }));
    }
    if result.rows.len() != 1 {
        return Err(DecodeError::new(AdapterDiagnostic::Ambiguous {
            surface: query.surface(),
            kind: AmbiguityKind::DuplicateLocalRow,
        }));
    }
    let row = &result.rows[0];
    ensure_complete_row(query, row, 0)?;
    Ok(row)
}

fn ensure_complete_row(
    query: QueryId,
    row: &[RawValue],
    row_index: usize,
) -> Result<(), DecodeError> {
    if row.len() != query.columns().len() {
        Err(DecodeError::new(AdapterDiagnostic::Evidence {
            surface: query.surface(),
            issue: EvidenceIssue::IncompleteRow { row: row_index },
        }))
    } else {
        Ok(())
    }
}

fn required_text<'a>(
    row: &'a [RawValue],
    index: usize,
    column: &'static str,
    surface: NativeSurface,
) -> Result<&'a str, DecodeError> {
    match row.get(index) {
        Some(RawValue::Null) => Err(DecodeError::new(AdapterDiagnostic::Evidence {
            surface,
            issue: EvidenceIssue::RequiredNull { column },
        })),
        Some(RawValue::Bytes(value)) => {
            let value = std::str::from_utf8(value).map_err(|_| malformed_cell(surface, column))?;
            if value.is_empty() && column != "gtid_executed" {
                Err(DecodeError::new(AdapterDiagnostic::Evidence {
                    surface,
                    issue: EvidenceIssue::EmptyRequiredString { column },
                }))
            } else {
                Ok(value)
            }
        }
        Some(_) | None => Err(malformed_cell(surface, column)),
    }
}

fn required_port(
    row: &[RawValue],
    index: usize,
    column: &'static str,
    surface: NativeSurface,
) -> Result<u16, DecodeError> {
    let value = match row.get(index) {
        Some(RawValue::Bytes(value)) => std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse::<u16>().ok()),
        Some(RawValue::Unsigned(value)) => u16::try_from(*value).ok(),
        Some(RawValue::Signed(value)) => u16::try_from(*value).ok(),
        Some(RawValue::Null) => {
            return Err(DecodeError::new(AdapterDiagnostic::Evidence {
                surface,
                issue: EvidenceIssue::RequiredNull { column },
            }));
        }
        _ => None,
    };
    value
        .filter(|value| *value != 0)
        .ok_or_else(|| malformed_cell(surface, column))
}

fn decode_switch(
    row: &[RawValue],
    index: usize,
    column: &'static str,
) -> Result<NativeSwitch, DecodeError> {
    match row.get(index) {
        Some(RawValue::Unsigned(0) | RawValue::Signed(0)) => Ok(NativeSwitch::Off),
        Some(RawValue::Unsigned(1) | RawValue::Signed(1)) => Ok(NativeSwitch::On),
        Some(RawValue::Bytes(value)) if value == b"0" || value == b"OFF" => Ok(NativeSwitch::Off),
        Some(RawValue::Bytes(value)) if value == b"1" || value == b"ON" => Ok(NativeSwitch::On),
        Some(RawValue::Null) => Err(DecodeError::new(AdapterDiagnostic::Evidence {
            surface: NativeSurface::LocalState,
            issue: EvidenceIssue::RequiredNull { column },
        })),
        Some(_) | None => Err(malformed_cell(NativeSurface::LocalState, column)),
    }
}

fn decode_member_role(value: &str) -> Result<MemberRole, DecodeError> {
    MemberRole::from_str(value).map_err(|error| {
        let issue = match error.kind() {
            NativeValueErrorKind::Malformed => EvidenceIssue::MalformedCell {
                column: "member_role",
            },
            NativeValueErrorKind::Unsupported => EvidenceIssue::UnsupportedNativeValue {
                column: "member_role",
            },
        };
        DecodeError::new(AdapterDiagnostic::Evidence {
            surface: NativeSurface::GroupMembers,
            issue,
        })
    })
}

fn decode_member_state(value: &str) -> Result<MemberState, DecodeError> {
    MemberState::from_str(value).map_err(|error| {
        let issue = match error.kind() {
            NativeValueErrorKind::Malformed => EvidenceIssue::MalformedCell {
                column: "member_state",
            },
            NativeValueErrorKind::Unsupported => EvidenceIssue::UnsupportedNativeValue {
                column: "member_state",
            },
        };
        DecodeError::new(AdapterDiagnostic::Evidence {
            surface: NativeSurface::GroupMembers,
            issue,
        })
    })
}

fn malformed_cell(surface: NativeSurface, column: &'static str) -> DecodeError {
    DecodeError::new(AdapterDiagnostic::Evidence {
        surface,
        issue: EvidenceIssue::MalformedCell { column },
    })
}

fn format_member_address(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn is_never_started_member(row: &[RawValue]) -> bool {
    row.len() == 5
        && text(row.first()) == Some("")
        && text(row.get(1)) == Some("")
        && matches!(row.get(2), Some(RawValue::Null))
        && text(row.get(3)) == Some("OFFLINE")
        && text(row.get(4)) == Some("")
}

fn is_stopped_member(row: &[RawValue]) -> bool {
    row.len() == 5
        && text(row.first()).is_some_and(|value| !value.is_empty())
        && text(row.get(1)).is_some_and(|value| !value.is_empty())
        && !matches!(row.get(2), Some(RawValue::Null) | None)
        && text(row.get(3)) == Some("OFFLINE")
        && text(row.get(4)) == Some("")
}

fn text(value: Option<&RawValue>) -> Option<&str> {
    match value {
        Some(RawValue::Bytes(value)) => std::str::from_utf8(value).ok(),
        _ => None,
    }
}
