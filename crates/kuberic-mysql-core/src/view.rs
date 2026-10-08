//! Typed Group Replication membership evidence.

use core::fmt;
use core::str::FromStr;
use std::collections::HashSet;

use crate::{GroupName, MemberAddress, MemberId, ViewId};

/// The native field being decoded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeField {
    /// Group Replication member role.
    Role,
    /// Group Replication member state.
    State,
}

/// Why a native role or state value was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeValueErrorKind {
    /// The value was empty or contained control characters.
    Malformed,
    /// The value was well formed but not supported by Stage 1.
    Unsupported,
}

/// A structured native role/state decoding error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeValueError {
    field: NativeField,
    kind: NativeValueErrorKind,
}

impl NativeValueError {
    fn decode(field: NativeField, value: &str) -> Self {
        let kind = if value.is_empty() || value.chars().any(char::is_control) {
            NativeValueErrorKind::Malformed
        } else {
            NativeValueErrorKind::Unsupported
        };
        Self { field, kind }
    }

    /// Returns the native field.
    #[must_use]
    pub const fn field(&self) -> NativeField {
        self.field
    }

    /// Returns the machine-matchable rejection kind.
    #[must_use]
    pub const fn kind(&self) -> NativeValueErrorKind {
        self.kind
    }
}

impl fmt::Display for NativeValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid native {:?} value: {:?}",
            self.field, self.kind
        )
    }
}

impl std::error::Error for NativeValueError {}

/// Group Replication member role.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemberRole {
    /// The member reports the native primary role.
    Primary,
    /// The member reports the native secondary role.
    Secondary,
}

impl FromStr for MemberRole {
    type Err = NativeValueError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "PRIMARY" => Ok(Self::Primary),
            "SECONDARY" => Ok(Self::Secondary),
            _ => Err(NativeValueError::decode(NativeField::Role, value)),
        }
    }
}

/// Group Replication member state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemberState {
    /// The member reports online.
    Online,
    /// The member reports recovery in progress.
    Recovering,
    /// The member reports offline.
    Offline,
    /// The member reports an error.
    Error,
    /// The member reports unreachable.
    Unreachable,
}

impl FromStr for MemberState {
    type Err = NativeValueError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "ONLINE" => Ok(Self::Online),
            "RECOVERING" => Ok(Self::Recovering),
            "OFFLINE" => Ok(Self::Offline),
            "ERROR" => Ok(Self::Error),
            "UNREACHABLE" => Ok(Self::Unreachable),
            _ => Err(NativeValueError::decode(NativeField::State, value)),
        }
    }
}

/// One immutable native member fact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeMember {
    id: MemberId,
    address: MemberAddress,
    role: MemberRole,
    state: MemberState,
}

impl NativeMember {
    /// Creates a native member fact.
    #[must_use]
    pub const fn new(
        id: MemberId,
        address: MemberAddress,
        role: MemberRole,
        state: MemberState,
    ) -> Self {
        Self {
            id,
            address,
            role,
            state,
        }
    }

    /// Returns the member UUID.
    #[must_use]
    pub const fn id(&self) -> &MemberId {
        &self.id
    }

    /// Returns the member address.
    #[must_use]
    pub const fn address(&self) -> &MemberAddress {
        &self.address
    }

    /// Returns the native role fact.
    #[must_use]
    pub const fn role(&self) -> MemberRole {
        self.role
    }

    /// Returns the native state fact.
    #[must_use]
    pub const fn state(&self) -> MemberState {
        self.state
    }
}

/// A structured native view construction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeViewError {
    /// Two member facts used the same UUID.
    DuplicateMemberId,
    /// Two member facts used the same address.
    DuplicateMemberAddress,
}

impl fmt::Display for NativeViewError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid native view: {self:?}")
    }
}

impl std::error::Error for NativeViewError {}

/// One exact, order-independent Group Replication view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeView {
    group_name: GroupName,
    id: ViewId,
    members: Vec<NativeMember>,
}

impl NativeView {
    /// Validates uniqueness and constructs an exact native view.
    pub fn new(
        group_name: GroupName,
        id: ViewId,
        mut members: Vec<NativeMember>,
    ) -> Result<Self, NativeViewError> {
        let mut member_ids = HashSet::with_capacity(members.len());
        let mut addresses = HashSet::with_capacity(members.len());
        for member in &members {
            if !member_ids.insert(member.id.as_str()) {
                return Err(NativeViewError::DuplicateMemberId);
            }
            if !addresses.insert(member.address.as_str()) {
                return Err(NativeViewError::DuplicateMemberAddress);
            }
        }
        members.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        Ok(Self {
            group_name,
            id,
            members,
        })
    }

    /// Returns the group identity.
    #[must_use]
    pub const fn group_name(&self) -> &GroupName {
        &self.group_name
    }

    /// Returns the opaque native view identity.
    #[must_use]
    pub const fn id(&self) -> &ViewId {
        &self.id
    }

    /// Returns members in deterministic UUID order.
    #[must_use]
    pub fn members(&self) -> &[NativeMember] {
        &self.members
    }

    /// Finds a member by native UUID.
    #[must_use]
    pub fn member(&self, id: &MemberId) -> Option<&NativeMember> {
        self.members.iter().find(|member| member.id == *id)
    }
}
