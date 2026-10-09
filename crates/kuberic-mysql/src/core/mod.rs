//! Deterministic, server-free safety types for the Kuberic MySQL integration.
//!
//! This crate models Stage 1 evidence only. It does not connect to MySQL,
//! perform topology changes, or grant client access.
//!
//! GTID histories have a four-way set relation rather than scalar progress:
//!
//! ```
//! use kuberic_mysql::core::{GtidRelation, GtidSet};
//!
//! let required: GtidSet =
//!     "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-2".parse()?;
//! let candidate: GtidSet =
//!     "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-3".parse()?;
//! assert_eq!(required.relation(&candidate), GtidRelation::ProperSubset);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Even a fresh observation of a native primary grants observation credit
//! only; Stage 1 client access remains closed:
//!
//! ```
//! # use kuberic_mysql::core::*;
//! # let uuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
//! # let binding = ExactBinding::new(ExactBindingParts {
//! #   resource: ResourceId::new("r")?, partition: PartitionId::new("p")?,
//! #   replica: ReplicaId::new("replica")?, incarnation: ReplicaIncarnation::new("i")?,
//! #   process_session: ProcessSessionId::new("ps")?,
//! #   observation_session: ObservationSessionId::new("os")?,
//! #   attempt: AttemptId::new("a")?, endpoint: EndpointBinding::new("e")?,
//! #   storage: StorageBinding::new("s")?, server_uuid: ServerUuid::new(uuid)?,
//! #   group_name: GroupName::new("g")?, member_id: MemberId::new(uuid)?,
//! #   member_address: MemberAddress::new("m")?,
//! #   configuration: ConfigurationId::new("c")?, epoch: Epoch::new("epoch")?,
//! #   authority_generation: AuthorityGeneration::new("authority")?,
//! #   view_id: ViewId::new("view")?,
//! #   credential_generation: CredentialGeneration::new("credential")?,
//! # });
//! # let view = NativeView::new(
//! #   binding.parts().group_name.clone(), binding.parts().view_id.clone(),
//! #   vec![NativeMember::new(
//! #     binding.parts().member_id.clone(), binding.parts().member_address.clone(),
//! #     MemberRole::Primary, MemberState::Online,
//! #   )],
//! # )?;
//! # let metadata = ObservationMetadata::new(
//! #   binding.clone(), ObservationProvenance::new("example", binding.parts().attempt.clone())?,
//! #   ObservationInstant::new(1), ObservationInstant::new(2),
//! #   ObservationInstant::new(3), ObservationInstant::new(2),
//! # );
//! # let outcome = ObservationDraft::new(metadata)
//! #   .opening(ObservationBracket::new(binding.clone(), view.clone()))
//! #   .executed(BoundGtidSet::new(binding.clone(), GtidSet::empty()))
//! #   .closing(ObservationBracket::new(binding.clone(), view))
//! #   .finalize();
//! let mut authority = AuthoritySession::new(binding.clone());
//! let capability = authority.begin_attempt(&binding).expect("current binding");
//! authority
//!     .complete(capability, &outcome, ObservationInstant::new(2))
//!     .expect("fresh, current observation");
//! assert!(authority.has_observation_credit());
//! assert!(!authority.access().write_open());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

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
    GroupName, GroupReplicationAddress, MemberAddress, MemberId, ObservationSessionId, PartitionId,
    ProcessSessionId, ReplicaId, ReplicaIncarnation, ResourceId, ServerUuid, StorageBinding,
    ViewId,
};
pub use observation::{
    BoundGtidSet, CollectionFailure, FreshnessError, IncoherentReason, MalformedReason,
    NativeEvidenceField, NativeObservationBracket, NativeObservationDraft, ObservationBracket,
    ObservationDraft, ObservationField, ObservationInstant, ObservationMetadata,
    ObservationOutcome, ObservationProvenance, ProvenanceError, StaleReason, UnsupportedReason,
    ValidObservation,
};
pub use view::{
    MemberRole, MemberState, NativeAccessState, NativeField, NativeLocalState, NativeMember,
    NativeSnapshot, NativeSwitch, NativeValueError, NativeValueErrorKind, NativeView,
    NativeViewError,
};
