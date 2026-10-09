//! Validated Stage 1 identity values.

use core::fmt;
use core::str::FromStr;

use crate::core::error::{IdentityError, IdentityErrorKind, IdentityField};

const MAX_OPAQUE_BYTES: usize = 255;

fn validate_opaque(field: IdentityField, value: &str) -> Result<(), IdentityError> {
    if value.is_empty() {
        return Err(IdentityError::new(field, IdentityErrorKind::Empty));
    }
    if value.len() > MAX_OPAQUE_BYTES {
        return Err(IdentityError::new(
            field,
            IdentityErrorKind::TooLong {
                maximum: MAX_OPAQUE_BYTES,
                actual: value.len(),
            },
        ));
    }
    if let Some((index, _)) = value.chars().enumerate().find(|(_, ch)| ch.is_control()) {
        return Err(IdentityError::new(
            field,
            IdentityErrorKind::ControlCharacter { index },
        ));
    }
    Ok(())
}

macro_rules! opaque_identity {
    ($name:ident, $field:ident, $docs:literal) => {
        #[doc = $docs]
        #[derive(Clone, Debug, Eq, Hash, PartialEq)]
        pub struct $name(String);

        impl $name {
            /// Validates and constructs the identity.
            pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
                let value = value.into();
                validate_opaque(IdentityField::$field, &value)?;
                Ok(Self(value))
            }

            /// Returns the exact caller-supplied value.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdentityError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }
    };
}

opaque_identity!(ResourceId, Resource, "A Kuberic resource identity.");
opaque_identity!(PartitionId, Partition, "A Kuberic partition identity.");
opaque_identity!(ReplicaId, Replica, "A logical replica identity.");
opaque_identity!(
    ReplicaIncarnation,
    ReplicaIncarnation,
    "A logical replica incarnation identity."
);
opaque_identity!(
    ProcessSessionId,
    ProcessSession,
    "A process-session identity."
);
opaque_identity!(
    ObservationSessionId,
    ObservationSession,
    "An observation-session identity."
);
opaque_identity!(AttemptId, Attempt, "An operation-attempt identity.");
opaque_identity!(
    EndpointBinding,
    Endpoint,
    "An opaque endpoint binding identity."
);
opaque_identity!(
    StorageBinding,
    Storage,
    "An opaque storage binding identity."
);
opaque_identity!(GroupName, GroupName, "A Group Replication group identity.");
opaque_identity!(
    MemberAddress,
    MemberAddress,
    "An opaque Group Replication member address."
);
opaque_identity!(
    GroupReplicationAddress,
    GroupReplicationAddress,
    "An opaque Group Replication communication address."
);
opaque_identity!(ViewId, View, "An opaque native view identity.");
opaque_identity!(
    ConfigurationId,
    Configuration,
    "A Kuberic configuration identity."
);
opaque_identity!(Epoch, Epoch, "A Kuberic epoch identity.");
opaque_identity!(
    AuthorityGeneration,
    AuthorityGeneration,
    "An independently changing authority generation."
);
opaque_identity!(
    CredentialGeneration,
    CredentialGeneration,
    "A credential generation identity."
);

fn canonical_uuid(field: IdentityField, value: &str) -> Result<String, IdentityError> {
    let bytes = value.as_bytes();
    if bytes.len() != 36
        || bytes[8] != b'-'
        || bytes[13] != b'-'
        || bytes[18] != b'-'
        || bytes[23] != b'-'
        || bytes
            .iter()
            .enumerate()
            .any(|(index, byte)| !matches!(index, 8 | 13 | 18 | 23) && !byte.is_ascii_hexdigit())
    {
        return Err(IdentityError::new(field, IdentityErrorKind::MalformedUuid));
    }

    let canonical = value.to_ascii_lowercase();
    if canonical.bytes().all(|byte| byte == b'0' || byte == b'-') {
        return Err(IdentityError::new(field, IdentityErrorKind::NilUuid));
    }
    Ok(canonical)
}

macro_rules! uuid_identity {
    ($name:ident, $field:ident, $docs:literal) => {
        #[doc = $docs]
        #[derive(Clone, Debug, Eq, Hash, PartialEq)]
        pub struct $name(String);

        impl $name {
            /// Validates and constructs a non-nil UUID identity.
            pub fn new(value: impl AsRef<str>) -> Result<Self, IdentityError> {
                canonical_uuid(IdentityField::$field, value.as_ref()).map(Self)
            }

            /// Returns the lowercase hyphenated UUID.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdentityError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }
    };
}

uuid_identity!(ServerUuid, ServerUuid, "A native MySQL server UUID.");
uuid_identity!(MemberId, MemberId, "A Group Replication member UUID.");
