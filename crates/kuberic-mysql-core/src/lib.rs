//! Deterministic, server-free safety types for the Kuberic MySQL integration.
//!
//! This crate models Stage 1 evidence only. It does not connect to MySQL,
//! perform topology changes, or grant client access.

pub mod binding;
pub mod error;
pub mod identity;

pub use binding::{ExactBinding, ExactBindingParts};
pub use error::{IdentityError, IdentityErrorKind, IdentityField};
pub use identity::{
    AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, EndpointBinding, Epoch,
    GroupName, MemberAddress, MemberId, ObservationSessionId, PartitionId, ProcessSessionId,
    ReplicaId, ReplicaIncarnation, ResourceId, ServerUuid, StorageBinding, ViewId,
};
