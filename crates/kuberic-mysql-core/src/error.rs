//! Structured public errors.

use core::fmt;

/// The identity field whose value failed validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum IdentityField {
    /// Resource identity.
    Resource,
    /// Partition identity.
    Partition,
    /// Logical replica identity.
    Replica,
    /// Logical replica incarnation.
    ReplicaIncarnation,
    /// Process session identity.
    ProcessSession,
    /// Observation session identity.
    ObservationSession,
    /// Operation attempt identity.
    Attempt,
    /// Endpoint binding.
    Endpoint,
    /// Storage binding.
    Storage,
    /// Native MySQL server UUID.
    ServerUuid,
    /// Group Replication group name.
    GroupName,
    /// Group Replication member UUID.
    MemberId,
    /// Group Replication member address.
    MemberAddress,
    /// Native Group Replication view identity.
    View,
    /// Kuberic configuration identity.
    Configuration,
    /// Kuberic epoch identity.
    Epoch,
    /// Independently changing authority generation.
    AuthorityGeneration,
    /// Credential generation.
    CredentialGeneration,
}

/// A machine-matchable reason an identity value was rejected.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum IdentityErrorKind {
    /// The value was empty.
    Empty,
    /// The UTF-8 representation exceeded the permitted byte length.
    TooLong {
        /// Maximum permitted UTF-8 byte length.
        maximum: usize,
        /// Supplied UTF-8 byte length.
        actual: usize,
    },
    /// The value contained a Unicode control character.
    ControlCharacter {
        /// Character index of the first control character.
        index: usize,
    },
    /// The value was not a canonicalizable hyphenated UUID.
    MalformedUuid,
    /// The UUID contained only zero bits.
    NilUuid,
}

/// A structured identity validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentityError {
    field: IdentityField,
    kind: IdentityErrorKind,
}

impl IdentityError {
    /// Creates an identity validation failure.
    #[must_use]
    pub const fn new(field: IdentityField, kind: IdentityErrorKind) -> Self {
        Self { field, kind }
    }

    /// Returns the rejected identity field.
    #[must_use]
    pub const fn field(&self) -> IdentityField {
        self.field
    }

    /// Returns the machine-matchable rejection reason.
    #[must_use]
    pub const fn kind(&self) -> &IdentityErrorKind {
        &self.kind
    }
}

impl fmt::Display for IdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid identity field {:?}: {:?}",
            self.field, self.kind
        )
    }
}

impl std::error::Error for IdentityError {}
