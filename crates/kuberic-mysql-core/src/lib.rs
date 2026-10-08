//! Deterministic, server-free safety types for the Kuberic MySQL integration.
//!
//! This crate models Stage 1 evidence only. It does not connect to MySQL,
//! perform topology changes, or grant client access.

pub mod authority;
pub mod binding;
pub mod error;
pub mod gtid;
pub mod identity;
pub mod observation;
pub mod view;

pub use authority::{
    AccessProjection, AdmissionCapability, AdmissionError, AuthoritySession, CompletionCredit,
    CompletionRejection, UnsupportedOperation, UnsupportedResult,
};
pub use binding::{ExactBinding, ExactBindingParts};
pub use error::{IdentityError, IdentityErrorKind, IdentityField};
pub use gtid::{
    GtidInterval, GtidParseError, GtidParseErrorKind, GtidRelation, GtidSet, GtidSource, GtidTag,
    MAX_SEQUENCE, SourceHistory,
};
pub use identity::{
    AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, EndpointBinding, Epoch,
    GroupName, MemberAddress, MemberId, ObservationSessionId, PartitionId, ProcessSessionId,
    ReplicaId, ReplicaIncarnation, ResourceId, ServerUuid, StorageBinding, ViewId,
};
pub use observation::{
    BoundGtidSet, CollectionFailure, FreshnessError, IncoherentReason, MalformedReason,
    ObservationBracket, ObservationDraft, ObservationField, ObservationInstant,
    ObservationMetadata, ObservationOutcome, ObservationProvenance, ProvenanceError, StaleReason,
    UnsupportedReason, ValidObservation,
};
pub use view::{
    MemberRole, MemberState, NativeField, NativeMember, NativeValueError, NativeValueErrorKind,
    NativeView, NativeViewError,
};
