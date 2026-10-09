//! Exact identity binding for work and evidence.

use crate::core::identity::{
    AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, EndpointBinding, Epoch,
    GroupName, MemberAddress, MemberId, ObservationSessionId, PartitionId, ProcessSessionId,
    ReplicaId, ReplicaIncarnation, ResourceId, ServerUuid, StorageBinding, ViewId,
};

/// Validated components used to construct an [`ExactBinding`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactBindingParts {
    /// Resource identity.
    pub resource: ResourceId,
    /// Partition identity.
    pub partition: PartitionId,
    /// Logical replica identity.
    pub replica: ReplicaId,
    /// Logical replica incarnation.
    pub incarnation: ReplicaIncarnation,
    /// Process-session identity.
    pub process_session: ProcessSessionId,
    /// Observation-session identity.
    pub observation_session: ObservationSessionId,
    /// Operation-attempt identity.
    pub attempt: AttemptId,
    /// Endpoint binding.
    pub endpoint: EndpointBinding,
    /// Storage binding.
    pub storage: StorageBinding,
    /// Native server UUID.
    pub server_uuid: ServerUuid,
    /// Group identity.
    pub group_name: GroupName,
    /// Local member UUID.
    pub member_id: MemberId,
    /// Local member address.
    pub member_address: MemberAddress,
    /// Kuberic configuration identity.
    pub configuration: ConfigurationId,
    /// Kuberic epoch.
    pub epoch: Epoch,
    /// Independently changing authority generation.
    pub authority_generation: AuthorityGeneration,
    /// Native view identity.
    pub view_id: ViewId,
    /// Credential generation.
    pub credential_generation: CredentialGeneration,
}

/// The complete immutable identity of one unit of work or evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactBinding {
    parts: ExactBindingParts,
}

impl ExactBinding {
    /// Constructs an exact binding from already validated identity values.
    #[must_use]
    pub const fn new(parts: ExactBindingParts) -> Self {
        Self { parts }
    }

    /// Returns all validated binding components.
    #[must_use]
    pub const fn parts(&self) -> &ExactBindingParts {
        &self.parts
    }
}
