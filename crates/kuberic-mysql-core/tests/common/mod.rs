#![allow(dead_code)]

use kuberic_mysql_core::{
    AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, EndpointBinding, Epoch,
    ExactBinding, ExactBindingParts, GroupName, MemberAddress, MemberId, MemberRole, MemberState,
    NativeMember, NativeView, ObservationBracket, ObservationInstant, ObservationMetadata,
    ObservationProvenance, ObservationSessionId, PartitionId, ProcessSessionId, ReplicaId,
    ReplicaIncarnation, ResourceId, ServerUuid, StorageBinding, ViewId,
};

pub const UUID_A: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";

pub fn binding() -> ExactBinding {
    ExactBinding::new(ExactBindingParts {
        resource: ResourceId::new("resource").unwrap(),
        partition: PartitionId::new("partition").unwrap(),
        replica: ReplicaId::new("replica").unwrap(),
        incarnation: ReplicaIncarnation::new("incarnation").unwrap(),
        process_session: ProcessSessionId::new("process").unwrap(),
        observation_session: ObservationSessionId::new("observation").unwrap(),
        attempt: AttemptId::new("attempt").unwrap(),
        endpoint: EndpointBinding::new("endpoint").unwrap(),
        storage: StorageBinding::new("storage").unwrap(),
        server_uuid: ServerUuid::new(UUID_A).unwrap(),
        group_name: GroupName::new("group").unwrap(),
        member_id: MemberId::new(UUID_A).unwrap(),
        member_address: MemberAddress::new("a:3306").unwrap(),
        configuration: ConfigurationId::new("configuration").unwrap(),
        epoch: Epoch::new("epoch").unwrap(),
        authority_generation: AuthorityGeneration::new("authority").unwrap(),
        view_id: ViewId::new("view").unwrap(),
        credential_generation: CredentialGeneration::new("credentials").unwrap(),
    })
}

pub fn native_view() -> NativeView {
    NativeView::new(
        GroupName::new("group").unwrap(),
        ViewId::new("view").unwrap(),
        vec![NativeMember::new(
            MemberId::new(UUID_A).unwrap(),
            MemberAddress::new("a:3306").unwrap(),
            MemberRole::Primary,
            MemberState::Online,
        )],
    )
    .unwrap()
}

pub fn metadata(
    binding: ExactBinding,
    start: u64,
    end: u64,
    deadline: u64,
    decision: u64,
) -> ObservationMetadata {
    ObservationMetadata::new(
        binding.clone(),
        ObservationProvenance::new("fixture", binding.parts().attempt.clone()).unwrap(),
        ObservationInstant::new(start),
        ObservationInstant::new(end),
        ObservationInstant::new(deadline),
        ObservationInstant::new(decision),
    )
}

pub fn bracket(binding: ExactBinding) -> ObservationBracket {
    ObservationBracket::new(binding, native_view())
}
