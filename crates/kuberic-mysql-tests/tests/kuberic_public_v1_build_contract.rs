use std::time::{Duration, Instant};

use kuberic_mysql::core::{AttemptId, GroupReplicationAddress, MemberAddress, MemberId};
use kuberic_mysql::service::{
    AttemptCancellationState, AttemptContext, BuildAttemptGroup, KubericTranslationError,
    ReplicationEndpoint, translate_build_replica_information,
};
use kuberic_runtime::application::INVALID_LSN;
use kuberic_runtime::protocol::types::{
    AgentGeneration, OperationId, PodUid, ProcessSessionId, PvcUid, ReplicaId, ReplicaIdentity,
    ReplicaInstanceId, ReplicaRole, ReplicaStorageIdentity, ReplicaStorageIncarnationContext,
    StorageGeneration,
};
use kuberic_runtime::replicator::ReplicaInformation;

fn identity(id: i64) -> ReplicaIdentity {
    ReplicaIdentity {
        replica_id: ReplicaId::new(id),
        instance_id: ReplicaInstanceId::new(format!("build-instance-{id}")),
        agent_generation: AgentGeneration::new(format!("build-agent-{id}")),
    }
}

fn idle_target() -> ReplicaInformation {
    let identity = identity(2);
    ReplicaInformation {
        build_id: OperationId::new("build-contract"),
        identity: identity.clone(),
        replication_address: ReplicationEndpoint::new(
            MemberId::new("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
            MemberAddress::new("127.0.0.1:33062").unwrap(),
            GroupReplicationAddress::new("127.0.0.1:43062").unwrap(),
        )
        .encode(),
        process_session_id: ProcessSessionId::new("build-process-2"),
        role: ReplicaRole::IdleSecondary,
        current_progress: INVALID_LSN,
        catch_up_capability: INVALID_LSN,
        must_catchup: false,
        storage_incarnation: Some(ReplicaStorageIncarnationContext {
            replica: identity,
            storage: ReplicaStorageIdentity::Persistent {
                pod_uid: PodUid::new("build-pod-2"),
                pvc_uid: PvcUid::new("build-pvc-2"),
            },
            storage_generation: StorageGeneration::new("build-storage-2"),
        }),
    }
}

#[test]
fn idle_build_contract_rejects_every_changed_public_shape() {
    let idle = idle_target();
    translate_build_replica_information(&idle).unwrap();

    let mut role = idle.clone();
    role.role = ReplicaRole::ActiveSecondary;
    assert!(matches!(
        translate_build_replica_information(&role),
        Err(KubericTranslationError::UnsupportedRole(_))
    ));

    let mut progress = idle.clone();
    progress.current_progress = 0;
    assert!(matches!(
        translate_build_replica_information(&progress),
        Err(KubericTranslationError::InvalidProgress { .. })
    ));

    let mut catch_up = idle.clone();
    catch_up.must_catchup = true;
    assert!(matches!(
        translate_build_replica_information(&catch_up),
        Err(KubericTranslationError::InvalidPublicValue(_))
    ));

    let mut storage = idle;
    storage.storage_incarnation = None;
    assert_eq!(
        translate_build_replica_information(&storage),
        Err(KubericTranslationError::MissingStorage)
    );
}

#[test]
fn build_child_cancellation_is_shared_without_cancelling_lifetime_authority() {
    let now = Instant::now();
    let parent_one = AttemptContext::new_at(
        AttemptId::new("parent-one").unwrap(),
        now,
        now + Duration::from_secs(60),
        Duration::from_secs(10),
    )
    .unwrap();
    let parent_two = AttemptContext::new_at(
        AttemptId::new("parent-two").unwrap(),
        now,
        now + Duration::from_secs(60),
        Duration::from_secs(10),
    )
    .unwrap();
    let group = BuildAttemptGroup::new(
        AttemptId::new("shared-build").unwrap(),
        now + Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .unwrap();
    let first = group.child(parent_one.clone()).unwrap();
    let second = group.child(parent_two.clone()).unwrap();
    first.cancel();
    assert_eq!(
        second.cancellation_state(),
        AttemptCancellationState::Cancelled
    );
    assert_eq!(
        parent_one.cancellation_state(),
        AttemptCancellationState::Active
    );
    assert_eq!(
        parent_two.cancellation_state(),
        AttemptCancellationState::Active
    );
}
