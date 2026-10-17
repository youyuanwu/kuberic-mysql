#[path = "service_common/mod.rs"]
mod service_common;

use std::fs;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kuberic_mysql::core::{
    AttemptId, AuthorityGeneration, ConfiguredGtidLineage, CredentialGeneration, EndpointBinding,
    GroupReplicationAddress, MemberAddress, MemberId, ProcessSessionId as MysqlProcessSessionId,
    StorageBinding,
};
use kuberic_mysql::service::{
    ApplicationCredential, CallbackOperationLedger, ControlCredential, ControlCredentialRole,
    EpochTranslator, KubericTranslationError, MemberPeer, MysqlFreshMemberCredentials,
    MysqlMemberIndex, MysqlPublicV1Replicator, MysqlPublicV1ReplicatorFactory,
    MysqlPublicV1Service, MysqlPublicV1ServiceConfig, MysqlReplicaEpoch, MysqlReplicaTarget,
    ReplicationEndpoint, translate_build_replica_information, translate_process_session_id,
    translate_replica_identity, translate_replica_information, translate_replica_set_configuration,
    translate_role, translate_storage_binding,
};
use kuberic_runtime::host::ApplicationStorageState;
use kuberic_runtime::protocol::public_operations::INVALID_LSN;
use kuberic_runtime::protocol::types::{
    AgentGeneration, ConfigurationDescriptor, ConfigurationMember, Epoch, OperationId, PodUid,
    ProcessSessionId, PvcUid, ReplicaId, ReplicaIdentity, ReplicaInstanceId, ReplicaRole,
    ReplicaStorageIdentity, ReplicaStorageIncarnationContext, StorageGeneration,
};
use kuberic_runtime::replicator::{
    PrimaryReplicator, ReplicaInformation, ReplicaSetConfiguration, Replicator, ReplicatorFactory,
};
use kuberic_runtime::{StateProvider, StatefulServiceReplica};
use service_common::{GROUP_UUID, TestRoot};

const MEMBER_ONE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const MEMBER_TWO: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

fn public_identity(id: i64) -> ReplicaIdentity {
    ReplicaIdentity {
        replica_id: ReplicaId::new(id),
        instance_id: ReplicaInstanceId::new(format!("instance-{id}")),
        agent_generation: AgentGeneration::new(format!("agent-{id}")),
    }
}

fn public_storage(identity: ReplicaIdentity) -> ReplicaStorageIncarnationContext {
    let id = identity.replica_id.value();
    ReplicaStorageIncarnationContext {
        replica: identity,
        storage: ReplicaStorageIdentity::Persistent {
            pod_uid: PodUid::new(format!("pod-{id}")),
            pvc_uid: PvcUid::new(format!("pvc-{id}")),
        },
        storage_generation: StorageGeneration::new(format!("storage-{id}")),
    }
}

fn endpoint(member_id: &str, sql: &str, group: &str) -> ReplicationEndpoint {
    ReplicationEndpoint::new(
        MemberId::new(member_id).unwrap(),
        MemberAddress::new(sql).unwrap(),
        GroupReplicationAddress::new(group).unwrap(),
    )
}

fn information(
    id: i64,
    role: ReplicaRole,
    member_id: &str,
    sql: &str,
    group: &str,
) -> ReplicaInformation {
    let identity = public_identity(id);
    ReplicaInformation {
        build_id: OperationId::new(format!("build-{id}")),
        identity: identity.clone(),
        replication_address: endpoint(member_id, sql, group).encode(),
        process_session_id: ProcessSessionId::new(format!("process-{id}")),
        role,
        current_progress: id,
        catch_up_capability: INVALID_LSN,
        must_catchup: false,
        storage_incarnation: Some(public_storage(identity)),
    }
}

fn public_configuration() -> ReplicaSetConfiguration {
    let primary = information(
        1,
        ReplicaRole::Primary,
        MEMBER_ONE,
        "127.0.0.1:33061",
        "127.0.0.1:43061",
    );
    let secondary = information(
        2,
        ReplicaRole::ActiveSecondary,
        MEMBER_TWO,
        "127.0.0.1:33062",
        "127.0.0.1:43062",
    );
    ReplicaSetConfiguration {
        configuration: ConfigurationDescriptor::new(
            Epoch::new(3, 7),
            primary.identity.replica_id,
            vec![
                ConfigurationMember {
                    identity: primary.identity.clone(),
                    role: primary.role,
                },
                ConfigurationMember {
                    identity: secondary.identity.clone(),
                    role: secondary.role,
                },
            ],
            2,
        ),
        replicas: vec![secondary],
    }
}

fn enrolled_local_target(identity: &ReplicaIdentity) -> (ReplicationEndpoint, MysqlReplicaTarget) {
    let endpoint = endpoint(MEMBER_ONE, "127.0.0.1:33061", "127.0.0.1:43061");
    let target = MysqlReplicaTarget::new(
        translate_replica_identity(identity).unwrap(),
        MemberPeer::new(
            endpoint.member_id().clone(),
            endpoint.member_address().clone(),
        ),
        MysqlProcessSessionId::new("enrolled-local-process").unwrap(),
        EndpointBinding::new("enrolled-local-endpoint").unwrap(),
        StorageBinding::new("enrolled-local-storage").unwrap(),
    );
    (endpoint, target)
}

#[test]
fn exported_translation_preserves_independent_public_and_native_fences() {
    let identity = public_identity(1);
    let native_identity = translate_replica_identity(&identity).unwrap();
    assert_eq!(
        native_identity.replica_id().as_str(),
        "kuberic-public-v1-replica:1"
    );
    assert_ne!(
        native_identity.replica_id().as_str(),
        native_identity.incarnation().as_str()
    );

    let process = translate_process_session_id(&ProcessSessionId::new("process-1")).unwrap();
    let storage = translate_storage_binding(&public_storage(identity.clone()), &identity).unwrap();
    assert_eq!(process.as_str(), "process-1");
    assert_ne!(process.as_str(), storage.as_str());

    let encoded = endpoint(MEMBER_ONE, "127.0.0.1:33061", "127.0.0.1:43061").encode();
    let decoded = ReplicationEndpoint::decode(&encoded).unwrap();
    assert_eq!(decoded.encode(), encoded);
    assert_eq!(decoded.member_id().as_str(), MEMBER_ONE);
    assert!(matches!(
        ReplicationEndpoint::decode(&(encoded + "x")),
        Err(KubericTranslationError::MalformedEndpoint(_))
    ));

    let information = information(
        1,
        ReplicaRole::Primary,
        MEMBER_ONE,
        "127.0.0.1:33061",
        "127.0.0.1:43061",
    );
    let target = translate_replica_information(&information).unwrap();
    assert_eq!(target.identity(), &native_identity);
    assert_eq!(target.process_session().as_str(), "process-1");
    assert_eq!(target.storage(), &storage);

    let mut foreign = information;
    foreign.storage_incarnation = Some(public_storage(public_identity(2)));
    assert_eq!(
        translate_replica_information(&foreign),
        Err(KubericTranslationError::ForeignStorage)
    );
}

#[test]
fn build_translation_accepts_only_exact_idle_target_shape() {
    let mut idle = information(
        2,
        ReplicaRole::IdleSecondary,
        MEMBER_TWO,
        "127.0.0.1:33062",
        "127.0.0.1:43062",
    );
    idle.current_progress = INVALID_LSN;
    idle.catch_up_capability = INVALID_LSN;
    let (build_id, target) = translate_build_replica_information(&idle).unwrap();
    assert_eq!(build_id, idle.build_id);
    assert_eq!(target.process_session().as_str(), "process-2");

    let mut wrong_role = idle.clone();
    wrong_role.role = ReplicaRole::ActiveSecondary;
    assert!(matches!(
        translate_build_replica_information(&wrong_role),
        Err(KubericTranslationError::UnsupportedRole(
            ReplicaRole::ActiveSecondary
        ))
    ));

    let mut progress = idle.clone();
    progress.current_progress = 0;
    assert!(matches!(
        translate_build_replica_information(&progress),
        Err(KubericTranslationError::InvalidProgress {
            field: "idle Build current",
            value: 0
        })
    ));

    let mut catch_up = idle.clone();
    catch_up.must_catchup = true;
    assert!(matches!(
        translate_build_replica_information(&catch_up),
        Err(KubericTranslationError::InvalidPublicValue(
            "idle Build target must not require catch-up"
        ))
    ));

    let mut no_storage = idle;
    no_storage.storage_incarnation = None;
    assert_eq!(
        translate_build_replica_information(&no_storage),
        Err(KubericTranslationError::MissingStorage)
    );
}

#[test]
fn exported_configuration_translation_is_complete_canonical_and_fail_closed() {
    let configuration = public_configuration();
    let local = configuration.configuration.members[0].identity.clone();
    let (local_endpoint, local_target) = enrolled_local_target(&local);
    let mut epochs = EpochTranslator::new();
    let translated = translate_replica_set_configuration(
        &configuration,
        &mut epochs,
        &local,
        &local_endpoint,
        &local_target,
    )
    .unwrap();
    assert_eq!(translated.epoch(), MysqlReplicaEpoch::new(1));
    assert_eq!(translated.members().len(), 2);
    assert_eq!(translated.members()[0], local_target);

    let mut invalid_all_member_shape = configuration.clone();
    invalid_all_member_shape.replicas.insert(
        0,
        information(
            1,
            ReplicaRole::Primary,
            MEMBER_ONE,
            "127.0.0.1:33061",
            "127.0.0.1:43061",
        ),
    );
    assert_eq!(
        translate_replica_set_configuration(
            &invalid_all_member_shape,
            &mut EpochTranslator::new(),
            &local,
            &local_endpoint,
            &local_target,
        ),
        Err(KubericTranslationError::InvalidConfiguration(
            "replica information does not exactly cover remote configured members"
        ))
    );

    let singleton = ReplicaSetConfiguration {
        configuration: ConfigurationDescriptor::new(
            Epoch::new(3, 8),
            local.replica_id,
            vec![ConfigurationMember {
                identity: local.clone(),
                role: ReplicaRole::Primary,
            }],
            1,
        ),
        replicas: vec![],
    };
    let singleton = translate_replica_set_configuration(
        &singleton,
        &mut EpochTranslator::new(),
        &local,
        &local_endpoint,
        &local_target,
    )
    .unwrap();
    assert_eq!(singleton.members(), std::slice::from_ref(&local_target));

    let mut duplicate_process = configuration.clone();
    duplicate_process.replicas[0].process_session_id =
        ProcessSessionId::new("enrolled-local-process");
    assert_eq!(
        translate_replica_set_configuration(
            &duplicate_process,
            &mut EpochTranslator::new(),
            &local,
            &local_endpoint,
            &local_target,
        ),
        Err(KubericTranslationError::DuplicateConfigurationFence(
            "process session"
        ))
    );

    let mut unsupported_role = configuration.clone();
    unsupported_role.configuration.members[1].role = ReplicaRole::IdleSecondary;
    unsupported_role.configuration.configuration_id = unsupported_role.configuration.expected_id();
    unsupported_role.replicas[0].role = ReplicaRole::IdleSecondary;
    assert_eq!(
        translate_replica_set_configuration(
            &unsupported_role,
            &mut EpochTranslator::new(),
            &local,
            &local_endpoint,
            &local_target,
        ),
        Err(KubericTranslationError::UnsupportedRole(
            ReplicaRole::IdleSecondary
        ))
    );

    let mut invalid_progress = configuration;
    invalid_progress.replicas[0].current_progress = INVALID_LSN;
    assert_eq!(
        translate_replica_set_configuration(
            &invalid_progress,
            &mut EpochTranslator::new(),
            &local,
            &local_endpoint,
            &local_target,
        ),
        Err(KubericTranslationError::InvalidProgress {
            field: "current",
            value: INVALID_LSN,
        })
    );
}

#[test]
fn exported_retry_state_reuses_exact_values_and_rejects_conflicts_or_regression() {
    let authority = AuthorityGeneration::new("public-v1-contract").unwrap();
    let mut ledger = CallbackOperationLedger::new(authority);
    let first = ledger
        .admit("role", (Epoch::new(1, 2), ReplicaRole::Primary))
        .unwrap();
    assert_eq!(
        ledger
            .admit("role", (Epoch::new(1, 2), ReplicaRole::Primary))
            .unwrap(),
        first
    );
    assert_eq!(
        ledger.admit("role", (Epoch::new(1, 2), ReplicaRole::ActiveSecondary)),
        Err(KubericTranslationError::OperationConflict)
    );

    let mut epochs = EpochTranslator::new();
    let mapped = epochs.translate(Epoch::new(1, 9)).unwrap();
    assert_eq!(epochs.translate(Epoch::new(1, 9)).unwrap(), mapped);
    assert_eq!(
        epochs.translate(Epoch::new(1, 8)),
        Err(KubericTranslationError::StaleEpoch {
            latest_data_loss_number: 1,
            latest_configuration_number: 9,
            requested_data_loss_number: 1,
            requested_configuration_number: 8,
        })
    );
}

#[test]
fn pinned_public_v1_bundle_and_fresh_only_service_shape_are_exported() {
    fn assert_driver<T: Replicator + PrimaryReplicator + StateProvider + Send + Sync>() {}
    fn assert_factory<T: ReplicatorFactory + Send + Sync>() {}
    fn assert_service<T: StatefulServiceReplica + Send + Sync>() {}

    assert_driver::<MysqlPublicV1Replicator>();
    assert_factory::<MysqlPublicV1ReplicatorFactory>();
    assert_service::<MysqlPublicV1Service>();
    let _factory_constructor: fn(Arc<MysqlPublicV1Replicator>) -> MysqlPublicV1ReplicatorFactory =
        MysqlPublicV1ReplicatorFactory::new;

    assert_eq!(
        translate_role(ReplicaRole::None),
        Err(KubericTranslationError::UnsupportedRole(ReplicaRole::None))
    );
    assert_eq!(
        translate_role(ReplicaRole::IdleSecondary),
        Err(KubericTranslationError::UnsupportedRole(
            ReplicaRole::IdleSecondary
        ))
    );

    let root = TestRoot::new("public-v1");
    let attempt = kuberic_mysql::service::AttemptContext::new(
        AttemptId::new("public-v1-service").unwrap(),
        Instant::now() + Duration::from_secs(60),
        Duration::from_secs(10),
    )
    .unwrap();
    let credentials = MysqlFreshMemberCredentials::new(
        ControlCredential::new(
            attempt.attempt().clone(),
            CredentialGeneration::new("observer").unwrap(),
            ControlCredentialRole::Observer,
            "observer_local",
            "observer-secret",
        )
        .unwrap(),
        ControlCredential::new(
            attempt.attempt().clone(),
            CredentialGeneration::new("recovery").unwrap(),
            ControlCredentialRole::Recovery,
            "recovery_local",
            "recovery-secret",
        )
        .unwrap(),
        ApplicationCredential::new(
            attempt.attempt().clone(),
            CredentialGeneration::new("application").unwrap(),
            "application_local",
            "application-secret",
        )
        .unwrap(),
    );
    let config = MysqlPublicV1ServiceConfig::new(
        "partition-1",
        root.config_for_member(MysqlMemberIndex::First),
        ConfiguredGtidLineage::new(GROUP_UUID).unwrap(),
        credentials,
        attempt,
        NonZeroU64::MIN,
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 33070)),
    )
    .unwrap();
    assert_eq!(
        MysqlPublicV1ServiceConfig::storage_state(&config).unwrap(),
        ApplicationStorageState::FreshEmpty
    );
    fs::create_dir_all(config.instance().data_root()).unwrap();
    fs::write(config.instance().data_root().join("native-state"), b"owned").unwrap();
    assert_eq!(
        MysqlPublicV1ServiceConfig::storage_state(&config).unwrap(),
        ApplicationStorageState::Established
    );

    let service = MysqlPublicV1Service::new(config);
    assert_eq!(service.expected_partition_id(), "partition-1");
    assert_eq!(
        service.proxy_listen_address(),
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 33070))
    );
}
