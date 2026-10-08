use kuberic_mysql_core::{
    AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, EndpointBinding, Epoch,
    ExactBinding, ExactBindingParts, GroupName, GroupReplicationAddress, IdentityErrorKind,
    MemberAddress, MemberId, ObservationSessionId, PartitionId, ProcessSessionId, ReplicaId,
    ReplicaIncarnation, ResourceId, ServerUuid, StorageBinding, ViewId,
};
use std::any::TypeId;

fn opaque(value: &str) -> ResourceId {
    ResourceId::new(value).expect("fixture must be valid")
}

fn binding_parts() -> ExactBindingParts {
    ExactBindingParts {
        resource: ResourceId::new("resource").unwrap(),
        partition: PartitionId::new("partition").unwrap(),
        replica: ReplicaId::new("replica").unwrap(),
        incarnation: ReplicaIncarnation::new("incarnation").unwrap(),
        process_session: ProcessSessionId::new("process").unwrap(),
        observation_session: ObservationSessionId::new("observation").unwrap(),
        attempt: AttemptId::new("attempt").unwrap(),
        endpoint: EndpointBinding::new("mysql.example:3306").unwrap(),
        storage: StorageBinding::new("/opaque/storage/identity").unwrap(),
        server_uuid: ServerUuid::new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
        group_name: GroupName::new("group").unwrap(),
        member_id: MemberId::new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
        member_address: MemberAddress::new("mysql.example:3306").unwrap(),
        configuration: ConfigurationId::new("configuration").unwrap(),
        epoch: Epoch::new("epoch").unwrap(),
        authority_generation: AuthorityGeneration::new("authority").unwrap(),
        view_id: ViewId::new("view").unwrap(),
        credential_generation: CredentialGeneration::new("credentials").unwrap(),
    }
}

#[test]
fn opaque_values_preserve_case_and_whitespace() {
    assert_eq!(opaque("x").as_str(), "x");
    let value = opaque("  Mixed Case  ");
    assert_eq!(value.as_str(), "  Mixed Case  ");
}

#[test]
fn identity_domains_are_distinct_types() {
    assert_ne!(TypeId::of::<ResourceId>(), TypeId::of::<ReplicaId>());
    assert_ne!(TypeId::of::<ServerUuid>(), TypeId::of::<MemberId>());
    assert_ne!(
        TypeId::of::<ProcessSessionId>(),
        TypeId::of::<ObservationSessionId>()
    );
    assert_ne!(
        TypeId::of::<MemberAddress>(),
        TypeId::of::<GroupReplicationAddress>()
    );
}

#[test]
fn opaque_values_enforce_byte_bounds_and_controls() {
    assert!(matches!(
        ResourceId::new(""),
        Err(error) if error.kind() == &IdentityErrorKind::Empty
    ));
    assert!(ResourceId::new("x".repeat(255)).is_ok());
    assert!(matches!(
        ResourceId::new("x".repeat(256)),
        Err(error) if matches!(
            error.kind(),
            IdentityErrorKind::TooLong {
                maximum: 255,
                actual: 256
            }
        )
    ));
    assert!(matches!(
        ResourceId::new("bad\nvalue"),
        Err(error) if matches!(
            error.kind(),
            IdentityErrorKind::ControlCharacter { index: 3 }
        )
    ));

    let multibyte = "é".repeat(128);
    assert_eq!(multibyte.len(), 256);
    assert!(ResourceId::new(multibyte).is_err());
}

#[test]
fn uuid_values_validate_and_canonicalize() {
    let uuid = ServerUuid::new("A0B1C2D3-E4F5-6789-ABCD-EF0123456789").unwrap();
    assert_eq!(uuid.as_str(), "a0b1c2d3-e4f5-6789-abcd-ef0123456789");

    assert!(matches!(
        ServerUuid::new("not-a-uuid"),
        Err(error) if error.kind() == &IdentityErrorKind::MalformedUuid
    ));
    assert!(matches!(
        ServerUuid::new("00000000-0000-0000-0000-000000000000"),
        Err(error) if error.kind() == &IdentityErrorKind::NilUuid
    ));
}

#[test]
fn exact_binding_changes_when_any_dimension_changes() {
    let original_parts = binding_parts();
    let original = ExactBinding::new(original_parts.clone());

    macro_rules! differs {
        ($field:ident, $replacement:expr) => {{
            let mut changed = original_parts.clone();
            changed.$field = $replacement;
            assert_ne!(original, ExactBinding::new(changed), stringify!($field));
        }};
    }

    differs!(resource, ResourceId::new("resource-2").unwrap());
    differs!(partition, PartitionId::new("partition-2").unwrap());
    differs!(replica, ReplicaId::new("replica-2").unwrap());
    differs!(
        incarnation,
        ReplicaIncarnation::new("incarnation-2").unwrap()
    );
    differs!(process_session, ProcessSessionId::new("process-2").unwrap());
    differs!(
        observation_session,
        ObservationSessionId::new("observation-2").unwrap()
    );
    differs!(attempt, AttemptId::new("attempt-2").unwrap());
    differs!(
        endpoint,
        EndpointBinding::new("mysql.example:3307").unwrap()
    );
    differs!(storage, StorageBinding::new("storage-2").unwrap());
    differs!(
        server_uuid,
        ServerUuid::new("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap()
    );
    differs!(group_name, GroupName::new("group-2").unwrap());
    differs!(
        member_id,
        MemberId::new("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap()
    );
    differs!(
        member_address,
        MemberAddress::new("mysql.example:3307").unwrap()
    );
    differs!(
        configuration,
        ConfigurationId::new("configuration-2").unwrap()
    );
    differs!(epoch, Epoch::new("epoch-2").unwrap());
    differs!(
        authority_generation,
        AuthorityGeneration::new("authority-2").unwrap()
    );
    differs!(view_id, ViewId::new("view-2").unwrap());
    differs!(
        credential_generation,
        CredentialGeneration::new("credentials-2").unwrap()
    );
}
