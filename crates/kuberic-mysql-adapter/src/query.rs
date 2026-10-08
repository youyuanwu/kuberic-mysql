//! Oracle MySQL 8.4.11 selected-query contracts.

use crate::diagnostic::NativeSurface;

/// Adapter-owned logical column kinds, independent of client protocol types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnKind {
    /// Variable-length textual data.
    VarString,
    /// Fixed textual data.
    String,
    /// A 32-bit integer.
    Long,
    /// A 64-bit integer.
    LongLong,
    /// A protocol type outside the qualified contract.
    Other(u8),
}

/// One selected column's exact name, logical type, and nullability contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnContract {
    name: &'static str,
    kind: ColumnKind,
    nullable: bool,
}

impl ColumnContract {
    const fn new(name: &'static str, kind: ColumnKind, nullable: bool) -> Self {
        Self {
            name,
            kind,
            nullable,
        }
    }

    /// Returns the selected alias.
    #[must_use]
    pub const fn name(self) -> &'static str {
        self.name
    }

    /// Returns the qualified logical type.
    #[must_use]
    pub const fn kind(self) -> ColumnKind {
        self.kind
    }

    /// Returns whether SQL `NULL` is allowed by result metadata.
    #[must_use]
    pub const fn nullable(self) -> bool {
        self.nullable
    }
}

const PRODUCT_COLUMNS: &[ColumnContract] = &[
    ColumnContract::new("version", ColumnKind::VarString, false),
    ColumnContract::new("version_comment", ColumnKind::VarString, false),
    ColumnContract::new("version_compile_machine", ColumnKind::VarString, false),
    ColumnContract::new("version_compile_os", ColumnKind::VarString, false),
    ColumnContract::new("server_uuid", ColumnKind::VarString, false),
];
const LOCAL_STATE_COLUMNS: &[ColumnContract] = &[
    ColumnContract::new("group_name", ColumnKind::VarString, false),
    ColumnContract::new("group_replication_address", ColumnKind::VarString, false),
    ColumnContract::new("read_only", ColumnKind::LongLong, false),
    ColumnContract::new("super_read_only", ColumnKind::LongLong, false),
];
const MEMBERS_COLUMNS: &[ColumnContract] = &[
    ColumnContract::new("member_id", ColumnKind::String, false),
    ColumnContract::new("member_host", ColumnKind::String, false),
    ColumnContract::new("member_port", ColumnKind::Long, true),
    ColumnContract::new("member_state", ColumnKind::String, false),
    ColumnContract::new("member_role", ColumnKind::String, false),
];
const STATS_COLUMNS: &[ColumnContract] = &[
    ColumnContract::new("member_id", ColumnKind::String, false),
    ColumnContract::new("view_id", ColumnKind::String, false),
];
const GTID_COLUMNS: &[ColumnContract] = &[ColumnContract::new(
    "gtid_executed",
    ColumnKind::String,
    false,
)];

/// A stable identifier for one Oracle MySQL 8.4.11 query.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum QueryId {
    /// Product, platform, and native server identity.
    Mysql8411ProductIdentityV1,
    /// Configured group, local GR address, and access switches.
    Mysql8411LocalStateV1,
    /// Complete Group Replication membership.
    Mysql8411GroupMembersV1,
    /// Bound local member's native view identity.
    Mysql8411LocalMemberStatsV1,
    /// Local executed GTID set.
    Mysql8411ExecutedGtidsV1,
}

impl QueryId {
    /// Every query in required collection order.
    pub const ALL: [Self; 5] = [
        Self::Mysql8411ProductIdentityV1,
        Self::Mysql8411LocalStateV1,
        Self::Mysql8411GroupMembersV1,
        Self::Mysql8411LocalMemberStatsV1,
        Self::Mysql8411ExecutedGtidsV1,
    ];

    /// Returns the exact read-only SQL.
    #[must_use]
    pub const fn sql(self) -> &'static str {
        match self {
            Self::Mysql8411ProductIdentityV1 => {
                "SELECT @@version AS version, @@version_comment AS version_comment, \
@@version_compile_machine AS version_compile_machine, \
@@version_compile_os AS version_compile_os, \
@@GLOBAL.server_uuid AS server_uuid"
            }
            Self::Mysql8411LocalStateV1 => {
                "SELECT @@GLOBAL.group_replication_group_name AS group_name, \
@@GLOBAL.group_replication_local_address AS group_replication_address, \
@@GLOBAL.read_only AS read_only, \
@@GLOBAL.super_read_only AS super_read_only"
            }
            Self::Mysql8411GroupMembersV1 => {
                "SELECT MEMBER_ID AS member_id, MEMBER_HOST AS member_host, \
MEMBER_PORT AS member_port, MEMBER_STATE AS member_state, MEMBER_ROLE AS member_role \
FROM performance_schema.replication_group_members ORDER BY MEMBER_ID"
            }
            Self::Mysql8411LocalMemberStatsV1 => {
                "SELECT MEMBER_ID AS member_id, VIEW_ID AS view_id \
FROM performance_schema.replication_group_member_stats \
WHERE MEMBER_ID = @@GLOBAL.server_uuid"
            }
            Self::Mysql8411ExecutedGtidsV1 => "SELECT @@GLOBAL.gtid_executed AS gtid_executed",
        }
    }

    /// Returns the exact selected result contract.
    #[must_use]
    pub const fn columns(self) -> &'static [ColumnContract] {
        match self {
            Self::Mysql8411ProductIdentityV1 => PRODUCT_COLUMNS,
            Self::Mysql8411LocalStateV1 => LOCAL_STATE_COLUMNS,
            Self::Mysql8411GroupMembersV1 => MEMBERS_COLUMNS,
            Self::Mysql8411LocalMemberStatsV1 => STATS_COLUMNS,
            Self::Mysql8411ExecutedGtidsV1 => GTID_COLUMNS,
        }
    }

    /// Returns the native surface read by this query.
    #[must_use]
    pub const fn surface(self) -> NativeSurface {
        match self {
            Self::Mysql8411ProductIdentityV1 => NativeSurface::ProductIdentity,
            Self::Mysql8411LocalStateV1 => NativeSurface::LocalState,
            Self::Mysql8411GroupMembersV1 => NativeSurface::GroupMembers,
            Self::Mysql8411LocalMemberStatsV1 => NativeSurface::LocalMemberStats,
            Self::Mysql8411ExecutedGtidsV1 => NativeSurface::ExecutedGtids,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RawColumn {
    pub(crate) name: String,
    pub(crate) kind: ColumnKind,
    pub(crate) nullable: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RawValue {
    Null,
    Bytes(Vec<u8>),
    Signed(i64),
    Unsigned(u64),
    Float(f32),
    Double(f64),
    Date,
    Time,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RawResult {
    pub(crate) columns: Vec<RawColumn>,
    pub(crate) rows: Vec<Vec<RawValue>>,
}
