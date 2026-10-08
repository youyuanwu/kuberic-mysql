//! Secret-free, machine-matchable adapter diagnostics.

use core::fmt;

use kuberic_mysql_core::{
    GtidParseErrorKind, IncoherentReason, MalformedReason, NativeEvidenceField, UnsupportedReason,
};

/// A versioned native surface queried by the adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeSurface {
    /// Product, platform, and server identity variables.
    ProductIdentity,
    /// Group identity, local GR address, and access switches.
    LocalState,
    /// Complete `replication_group_members` contents.
    GroupMembers,
    /// Bound local row in `replication_group_member_stats`.
    LocalMemberStats,
    /// Local `gtid_executed`.
    ExecutedGtids,
}

/// The operation stage at which a client or transport failure occurred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationStage {
    /// Final socket filesystem validation.
    SocketValidation,
    /// Direct UDS connection.
    Connect,
    /// Server authentication.
    Authenticate,
    /// Query execution.
    Query,
    /// Result consumption.
    Consume,
    /// Explicit session teardown.
    Disconnect,
}

/// A selected result-schema mismatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaIssue {
    /// A selected column was missing.
    MissingColumn {
        index: usize,
        expected: &'static str,
    },
    /// A selected column name changed.
    ChangedColumn {
        index: usize,
        expected: &'static str,
        actual: String,
    },
    /// A selected column type changed.
    ChangedType {
        index: usize,
        expected: crate::query::ColumnKind,
        actual: crate::query::ColumnKind,
    },
    /// A selected column's nullability changed.
    ChangedNullability {
        index: usize,
        expected: bool,
        actual: bool,
    },
    /// Additional selected columns appeared.
    AdditionalColumns { expected: usize, actual: usize },
}

/// A native cell or row rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EvidenceIssue {
    /// A required selected cell was SQL `NULL`.
    RequiredNull { column: &'static str },
    /// A required selected string was empty.
    EmptyRequiredString { column: &'static str },
    /// A row contained fewer cells than its metadata.
    IncompleteRow { row: usize },
    /// A cell did not match the selected logical value domain.
    MalformedCell { column: &'static str },
    /// A native role or state was well formed but unknown.
    UnsupportedNativeValue { column: &'static str },
    /// Executed GTID text was malformed.
    MalformedGtid {
        kind: GtidParseErrorKind,
        component: usize,
        token: usize,
    },
}

/// Why exact Oracle Community 8.4.11 discrimination failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProductIssue {
    /// The product did not identify Oracle MySQL Community Server.
    NonOracleCommunity,
    /// The Oracle patch differed from 8.4.11.
    UnsupportedPatch,
    /// The native archive platform fields differed from Linux x86-64.
    UnsupportedPlatform,
}

/// A recognized state that intentionally carries no active native view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlaceholderKind {
    /// Group Replication is installed but has never started.
    NeverStarted,
    /// Group Replication previously ran and is now stopped.
    Stopped,
}

/// A row ambiguity that must never be resolved by arbitrary selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AmbiguityKind {
    /// Duplicate member UUIDs.
    DuplicateMemberId,
    /// Duplicate SQL member addresses.
    DuplicateMemberAddress,
    /// More than one local stats row.
    DuplicateLocalRow,
    /// The bound local member was missing.
    MissingLocalMember,
    /// Group or view identity contradicted the expected binding.
    ContradictoryIdentity,
}

/// A native identity dimension named without retaining its observed value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityField {
    /// Native server UUID.
    ServerUuid,
    /// Configured Group Replication group.
    GroupName,
    /// Local member UUID.
    LocalMemberId,
    /// Native view identifier.
    ViewId,
}

/// A server-error classification based only on numeric code and SQLSTATE.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerErrorClass {
    /// Server rejected the supplied credential.
    Authentication,
    /// Authenticated account lacks a required permission.
    Permission,
    /// Another authenticated server error.
    Other,
}

/// A validated five-byte SQLSTATE code.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlState(String);

impl SqlState {
    /// Validates an ASCII alphanumeric SQLSTATE.
    pub fn new(value: impl Into<String>) -> Result<Self, SqlStateError> {
        let value = value.into();
        if value.len() != 5 || !value.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
            return Err(SqlStateError);
        }
        Ok(Self(value))
    }

    /// Returns the validated SQLSTATE.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An invalid SQLSTATE.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SqlStateError;

impl fmt::Display for SqlStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid SQLSTATE")
    }
}

impl std::error::Error for SqlStateError {}

/// A secret-free adapter diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdapterDiagnostic {
    /// No adapter-level rejection accompanied the authoritative outcome.
    None,
    /// The UDS transport failed.
    Transport { stage: ObservationStage },
    /// The server returned a structured error.
    Server {
        stage: ObservationStage,
        surface: Option<NativeSurface>,
        class: ServerErrorClass,
        code: u16,
        sql_state: SqlState,
    },
    /// The selected client failed without a transport or server error.
    Client {
        stage: ObservationStage,
        surface: Option<NativeSurface>,
    },
    /// Product/version/platform discrimination failed.
    Product(ProductIssue),
    /// The selected result schema drifted.
    Schema {
        surface: NativeSurface,
        issue: SchemaIssue,
    },
    /// Native evidence was absent.
    Absent { surface: NativeSurface },
    /// A recognized inactive placeholder was observed.
    Placeholder(PlaceholderKind),
    /// A cell or row was malformed or unsupported.
    Evidence {
        surface: NativeSurface,
        issue: EvidenceIssue,
    },
    /// Multiple or contradictory rows prevented a unique interpretation.
    Ambiguous {
        surface: NativeSurface,
        kind: AmbiguityKind,
    },
    /// An authenticated native identity contradicted the expected binding.
    UnexpectedIdentity { field: IdentityField },
}

impl AdapterDiagnostic {
    /// Returns the broad authoritative core classification for this diagnostic.
    #[must_use]
    pub fn core_class(&self) -> CoreOutcomeClass {
        match self {
            Self::None => CoreOutcomeClass::Valid,
            Self::Transport { .. } => CoreOutcomeClass::Unreachable,
            Self::Server { class, .. } => match class {
                ServerErrorClass::Authentication => CoreOutcomeClass::AuthenticationFailure,
                ServerErrorClass::Permission => CoreOutcomeClass::PermissionDenied,
                ServerErrorClass::Other => {
                    CoreOutcomeClass::Unsupported(UnsupportedReason::CollectorCapability)
                }
            },
            Self::Client { .. } => {
                CoreOutcomeClass::Unsupported(UnsupportedReason::CollectorCapability)
            }
            Self::Product(_) | Self::Schema { .. } => {
                CoreOutcomeClass::Unsupported(UnsupportedReason::CollectorCapability)
            }
            Self::Absent { .. } | Self::Placeholder(_) => CoreOutcomeClass::Absent,
            Self::Evidence { issue, .. } => match issue {
                EvidenceIssue::UnsupportedNativeValue { .. } => {
                    CoreOutcomeClass::Unsupported(UnsupportedReason::CollectorCapability)
                }
                EvidenceIssue::MalformedGtid {
                    kind,
                    component,
                    token,
                } => CoreOutcomeClass::Malformed(MalformedReason::Gtid {
                    kind: *kind,
                    component: *component,
                    token: *token,
                }),
                EvidenceIssue::IncompleteRow { .. } => CoreOutcomeClass::Malformed(
                    MalformedReason::NativeEvidence(NativeEvidenceField::Row),
                ),
                EvidenceIssue::RequiredNull { .. }
                | EvidenceIssue::EmptyRequiredString { .. }
                | EvidenceIssue::MalformedCell { .. } => CoreOutcomeClass::Malformed(
                    MalformedReason::NativeEvidence(NativeEvidenceField::Cell),
                ),
            },
            Self::Ambiguous { .. } | Self::UnexpectedIdentity { .. } => {
                CoreOutcomeClass::Incoherent(IncoherentReason::NativeSnapshotMismatch)
            }
        }
    }
}

/// Broad core outcome classification used before an outcome envelope exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoreOutcomeClass {
    /// Complete evidence may be valid.
    Valid,
    /// Authoritative evidence was absent.
    Absent,
    /// Transport was unreachable.
    Unreachable,
    /// Authentication failed.
    AuthenticationFailure,
    /// Authenticated access was denied.
    PermissionDenied,
    /// Native evidence was malformed.
    Malformed(MalformedReason),
    /// Product, schema, or value was unsupported.
    Unsupported(UnsupportedReason),
    /// Rows or identities were incoherent.
    Incoherent(IncoherentReason),
}
