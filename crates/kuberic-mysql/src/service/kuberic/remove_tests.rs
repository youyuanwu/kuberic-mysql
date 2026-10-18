use super::super::super::build::{BuildSourceBinding, CompletedBuildDonor};
use super::*;
use crate::core::{
    CredentialGeneration, EndpointBinding, GroupName, GtidSet, MemberAddress, MemberRole,
    MemberState, NativeMember, NativeView, ProcessSessionId, ServerUuid, ViewId,
};
use crate::service::{
    AccountProvisioningEvidence, AttemptCleanupState, ControlCredential, ControlCredentialRole,
    MemberControlBinding, MysqlMemberIndex, NativeIdentityEnrollment,
};
use kuberic_runtime::protocol::types::{
    AgentGeneration, ConfigurationMember, PodUid, PvcUid, ReplicaInstanceId,
    ReplicaStorageIdentity, ReplicaStorageIncarnationContext, StorageGeneration,
};

const GROUP: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";

fn authority(id: u8) -> MemberMutationAuthority {
    let attempt = AttemptId::new(format!("native-owner-{id}")).unwrap();
    let observer_generation = CredentialGeneration::new("test-observer").unwrap();
    let recovery_generation = CredentialGeneration::new("test-recovery").unwrap();
    let binding = MemberControlBinding::new(
        attempt.clone(),
        match id {
            1 => MysqlMemberIndex::First,
            2 => MysqlMemberIndex::Second,
            _ => MysqlMemberIndex::Third,
        },
        ProcessSessionId::new(format!("native-session-{id}")).unwrap(),
        EndpointBinding::new(format!("native-uds-{id}")).unwrap(),
        StorageBinding::new(format!("native-data-inode-{id}")).unwrap(),
        MemberAddress::new(format!("127.0.0.1:3306{id}")).unwrap(),
        observer_generation.clone(),
        recovery_generation.clone(),
    );
    let observer = ControlCredential::new(
        attempt.clone(),
        observer_generation,
        ControlCredentialRole::Observer,
        "fixture_observer",
        "inert-test-observer",
    )
    .unwrap();
    let recovery = ControlCredential::new(
        attempt.clone(),
        recovery_generation,
        ControlCredentialRole::Recovery,
        "fixture_recovery",
        "inert-test-recovery",
    )
    .unwrap();
    let accounts = AccountProvisioningEvidence::new(
        binding.clone(),
        &observer,
        &recovery,
        AccountProvisioningEvidence::required_steps().to_vec(),
    )
    .unwrap();
    let enrollment = NativeIdentityEnrollment::new(
        binding,
        ServerUuid::new(format!("{id:08x}-1111-1111-1111-111111111111")).unwrap(),
        &accounts,
        true,
        GtidSet::empty(),
        NativeIdentityEnrollment::required_steps().to_vec(),
    )
    .unwrap();
    MemberMutationAuthority::new(attempt, GroupName::new(GROUP).unwrap(), enrollment).unwrap()
}

pub(crate) fn information(id: u8, role: ReplicaRole) -> ReplicaInformation {
    let identity = ReplicaIdentity {
        replica_id: ReplicaId::new(i64::from(id)),
        instance_id: ReplicaInstanceId::new(format!("public-instance-{id}")),
        agent_generation: AgentGeneration::new(format!("public-agent-{id}")),
    };
    let native = authority(id);
    let endpoint = ReplicationEndpoint::new(
        native.local().member_id().clone(),
        native.local().binding().member_address().clone(),
        GroupReplicationAddress::new(format!("127.0.0.1:3316{id}")).unwrap(),
    )
    .encode();
    let storage = ReplicaStorageIncarnationContext {
        replica: identity.clone(),
        storage: ReplicaStorageIdentity::Persistent {
            pod_uid: PodUid::new(format!("pod-{id}")),
            pvc_uid: PvcUid::new(format!("pvc-{id}")),
        },
        storage_generation: StorageGeneration::new(format!("public-storage-{id}")),
    };
    ReplicaInformation {
        build_id: OperationId::new("built"),
        identity,
        replication_address: endpoint,
        process_session_id: PublicProcessSessionId::new(format!("public-session-{id}")),
        role,
        current_progress: if role == ReplicaRole::IdleSecondary {
            INVALID_LSN
        } else {
            7
        },
        catch_up_capability: INVALID_LSN,
        must_catchup: false,
        storage_incarnation: Some(storage),
    }
}

fn native_view(id: &str, include_target: bool) -> NativeView {
    let members = (1..=if include_target { 3 } else { 2 })
        .map(|id| {
            let a = authority(id);
            NativeMember::new(
                a.local().member_id().clone(),
                a.local().binding().member_address().clone(),
                if id == 1 {
                    MemberRole::Primary
                } else {
                    MemberRole::Secondary
                },
                MemberState::Online,
            )
        })
        .collect();
    NativeView::new(
        GroupName::new(GROUP).unwrap(),
        ViewId::new(id).unwrap(),
        members,
    )
    .unwrap()
}

fn fixture() -> (
    CompletedBuildOwnership,
    OwnerAdmission,
    OwnerAdmission,
    OwnerAdmission,
) {
    let source = information(1, ReplicaRole::Primary);
    let donor = information(2, ReplicaRole::ActiveSecondary);
    let target = information(3, ReplicaRole::IdleSecondary);
    let epoch = Epoch::new(0, 1);
    let configuration = ReplicaSetConfiguration {
        configuration: ConfigurationDescriptor::new(
            epoch,
            source.identity.replica_id,
            vec![
                ConfigurationMember {
                    identity: source.identity.clone(),
                    role: source.role,
                },
                ConfigurationMember {
                    identity: donor.identity.clone(),
                    role: donor.role,
                },
            ],
            2,
        ),
        replicas: vec![donor.clone()],
    };
    let source_native = translate_replica_information(&source).unwrap();
    let target_native = translate_build_replica_information(&target).unwrap().1;
    let source_authority = authority(1);
    let boundary = MemberSourceBoundary::new(
        MemberPeer::from_enrollment(source_authority.local()),
        crate::service::MemberNativeEvidence::new(
            source_authority.clone(),
            native_view("700:1", false),
            format!("{GROUP}:1-7").parse::<GtidSet>().unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let binding = Box::new(BuildBinding {
        build_id: OperationId::new("built"),
        child_attempt: AttemptId::new("old-build-child").unwrap(),
        source_parent_attempt: source_authority.attempt().clone(),
        target_parent_attempt: authority(3).attempt().clone(),
        source_generation: 41,
        target_generation: 43,
        epoch,
        configuration: configuration.configuration.configuration_id.clone(),
        group_key: GROUP.into(),
        source: BuildSourceBinding {
            identity: source.identity.clone(),
            process_session: source.process_session_id.clone(),
            replication_address: source.replication_address.clone(),
            public_storage: source_native.storage().clone(),
            native: source_native,
        },
        source_configuration: Box::new(configuration.clone()),
        target: BuildTargetBinding {
            identity: target.identity.clone(),
            process_session: target.process_session_id.clone(),
            storage: target.storage_incarnation.clone().unwrap(),
            replication_address: target.replication_address.clone(),
            native: target_native,
        },
        source_authority: source_authority.clone(),
        expected_members: vec![
            source.identity.clone(),
            donor.identity.clone(),
            target.identity.clone(),
        ],
        source_boundary: boundary.clone(),
        grant_id: "test-grant".into(),
    });
    let callback = BuildCallbackKey {
        build_id: binding.build_id.clone(),
        epoch,
        configuration: binding.configuration.clone(),
        group_key: GROUP.into(),
        source_identity: source.identity.clone(),
        source_process_session: source.process_session_id.clone(),
        source_configuration: Box::new(configuration.clone()),
        source_authority: source_authority.clone(),
        source_boundary: boundary,
        source_progress: 7,
        target: binding.target.clone(),
    };
    let credential = BuildRecoveryCredential::new(
        binding.child_attempt.clone(),
        CredentialGeneration::new("test-grant").unwrap(),
        "fixture_build",
        b"inert-fixture-only".to_vec(),
    )
    .unwrap();
    let owned = CompletedBuildOwnership {
        binding: binding.clone(),
        callback,
        account: BuildRecoveryAccountReceipt::new(&credential),
        successor: native_view("700:2", true),
        target_authority: authority(3),
        donors: vec![CompletedBuildDonor {
            identity: donor.identity.clone(),
            generation: 42,
            authority: authority(2),
        }],
    };
    let snapshot = |info: &ReplicaInformation, id: u8, generation| OwnerAdmission {
        public: PublicBinding {
            identity: info.identity.clone(),
            process_session: info.process_session_id.clone(),
            public_storage: Some(
                super::super::super::translate_storage_binding(
                    info.storage_incarnation.as_ref().unwrap(),
                    &info.identity,
                )
                .unwrap(),
            ),
            settings: ReplicatorSettings {
                replication_address: info.replication_address.clone(),
            },
        },
        authority: authority(id),
        open_authority: authority(id),
        endpoint: info.replication_address.clone(),
        group: GROUP.into(),
        generation,
        status: BuildGenerationStatus::PendingAdmission,
        epoch: Some(epoch),
        mapped_epoch_matches: true,
        role: if id == 1 {
            Some(MysqlReplicaRole::Primary)
        } else if id == 2 {
            Some(MysqlReplicaRole::Secondary)
        } else {
            None
        },
        configuration: if id == 3 {
            None
        } else {
            Some(configuration.clone())
        },
        engine_configuration: id != 3,
        pending_configuration: false,
        binding: Some(binding.clone()),
        target: Some(binding.target.clone()),
        native_successor: id != 3,
        idle_epoch: (id == 3).then_some(epoch),
        idle: id == 3,
        role_admitted: false,
        join: (id == 3).then(|| {
            (
                binding.child_attempt.clone(),
                43,
                owned.successor.clone(),
                authority(3),
            )
        }),
    };
    let s = snapshot(&source, 1, 41);
    let t = snapshot(&target, 3, 43);
    let d = snapshot(&donor, 2, 42);
    (owned, s, t, d)
}

pub(crate) fn completed_fixture() -> CompletedBuildOwnership {
    fixture().0
}
type Mutation = (&'static str, fn(&mut OwnerAdmission));

fn shared_mutations() -> Vec<Mutation> {
    vec![
        ("identity", |s| {
            s.public.identity.instance_id = ReplicaInstanceId::new("substituted")
        }),
        ("public session", |s| {
            s.public.process_session = PublicProcessSessionId::new("substituted")
        }),
        ("public storage", |s| {
            s.public.public_storage = Some(StorageBinding::new("substituted").unwrap())
        }),
        ("native authority", |s| s.authority = authority(4)),
        ("native open authority", |s| s.open_authority = authority(4)),
        ("endpoint", |s| s.endpoint.push('x')),
        ("group", |s| s.group.push('x')),
        ("generation", |s| s.generation += 1),
        ("epoch", |s| s.epoch = Some(Epoch::new(0, 2))),
        ("mapped epoch", |s| s.mapped_epoch_matches = false),
        ("configuration staged", |s| s.pending_configuration = true),
        ("binding", |s| s.binding = None),
        ("cleanup-only", |s| {
            s.status = BuildGenerationStatus::CleanupOnly
        }),
    ]
}

#[test]
fn exact_source_target_and_donor_admission_isolates_every_fence() {
    let (owned, source, target, donor) = fixture();
    let peer = &owned.callback.source_configuration.replicas[0];
    source.source(&owned, false).unwrap();
    target.target(&owned).unwrap();
    donor.donor(peer, &owned, false).unwrap();
    let mut changed_source_role = source.clone();
    changed_source_role.role = Some(MysqlReplicaRole::Secondary);
    assert!(changed_source_role.source(&owned, false).is_err());
    let mut changed_donor_role = donor.clone();
    changed_donor_role.role = Some(MysqlReplicaRole::Primary);
    assert!(changed_donor_role.donor(peer, &owned, false).is_err());
    assert_ne!(
        target.public.public_storage.as_ref().unwrap(),
        target.authority.local().binding().storage(),
        "public and native storage domains are intentionally distinct"
    );
    for (label, mutate) in shared_mutations() {
        let mut s = source.clone();
        mutate(&mut s);
        assert!(s.source(&owned, false).is_err(), "source {label}");
        let mut t = target.clone();
        mutate(&mut t);
        assert!(t.target(&owned).is_err(), "target {label}");
        let mut d = donor.clone();
        mutate(&mut d);
        assert!(d.donor(peer, &owned, false).is_err(), "donor {label}");
    }
    for (label, mutate) in [
        (
            "idle flag",
            (|s: &mut OwnerAdmission| s.idle = false) as fn(&mut OwnerAdmission),
        ),
        ("idle epoch", |s| s.idle_epoch = Some(Epoch::new(0, 2))),
        ("public activation", |s| s.role_admitted = true),
        ("native role", |s| {
            s.role = Some(MysqlReplicaRole::Secondary)
        }),
        ("native configuration", |s| s.engine_configuration = true),
        ("public configuration", |s| {
            s.configuration = fixture().1.configuration
        }),
        ("join missing", |s| s.join = None),
        ("join child", |s| {
            s.join.as_mut().unwrap().0 = AttemptId::new("other-child").unwrap()
        }),
        ("join generation", |s| s.join.as_mut().unwrap().1 += 1),
        ("join view", |s| {
            s.join.as_mut().unwrap().2 = native_view("700:3", true)
        }),
        ("join authority", |s| {
            s.join.as_mut().unwrap().3 = authority(4)
        }),
    ] {
        let mut t = target.clone();
        mutate(&mut t);
        assert!(t.target(&owned).is_err(), "target {label}");
    }
    for (label, mutate) in [
        (
            "configuration missing",
            (|s: &mut OwnerAdmission| s.configuration = None) as fn(&mut OwnerAdmission),
        ),
        ("configuration epoch", |s| {
            s.configuration.as_mut().unwrap().configuration.epoch = Epoch::new(0, 2)
        }),
        ("native configuration", |s| s.engine_configuration = false),
        ("successor missing", |s| s.native_successor = false),
        ("target binding missing", |s| s.target = None),
    ] {
        let mut s = source.clone();
        mutate(&mut s);
        assert!(s.source(&owned, false).is_err(), "source {label}");
        let mut d = donor.clone();
        mutate(&mut d);
        assert!(d.donor(peer, &owned, false).is_err(), "donor {label}");
    }
}

#[test]
fn terminal_retry_cannot_borrow_pending_or_substituted_authority() {
    let (owned, mut source, _, mut donor) = fixture();
    let peer = &owned.callback.source_configuration.replicas[0];
    for snapshot in [&mut source, &mut donor] {
        snapshot.status = BuildGenerationStatus::Available;
        snapshot.binding = None;
        snapshot.target = None;
        snapshot.native_successor = false;
    }
    source.source(&owned, true).unwrap();
    donor.donor(peer, &owned, true).unwrap();
    for (label, mutate) in shared_mutations()
        .into_iter()
        .filter(|(label, _)| *label != "binding")
    {
        let mut s = source.clone();
        mutate(&mut s);
        assert!(s.source(&owned, true).is_err(), "source retry {label}");
        let mut d = donor.clone();
        mutate(&mut d);
        assert!(d.donor(peer, &owned, true).is_err(), "donor retry {label}");
    }
    let mut stale = source;
    stale.binding = Some(owned.binding.clone());
    assert!(stale.source(&owned, true).is_err());
    donor.native_successor = true;
    assert!(donor.donor(peer, &owned, true).is_err());
}

fn fence(label: &str) -> RemoveFence {
    RemoveFence::arm(
        Arc::new(BuildGeneration::new()),
        Arc::new(BuildGeneration::new()),
        vec![Arc::new(BuildGeneration::new())],
        BuildGroupPermit::try_acquire(label).unwrap(),
    )
}

fn receipt(complete: bool) -> MemberContainmentReceipt {
    MemberContainmentReceipt::new(
        authority(3),
        if complete {
            AttemptCleanupState::Completed
        } else {
            AttemptCleanupState::Failed
        },
        complete,
        true,
        true,
        true,
        true,
    )
    .with_retired_generation(100, 1, Some(123))
}

#[test]
fn every_armed_cleanup_cut_quarantines_and_never_publishes_success() {
    for (index, cut) in [
        RemoveCheckpoint::Armed,
        RemoveCheckpoint::TargetLeft,
        RemoveCheckpoint::SurvivorsProved,
    ]
    .into_iter()
    .enumerate()
    {
        let group = format!("remove-cut-{index}");
        let mut fence = fence(&group);
        if cut != RemoveCheckpoint::Armed {
            fence.advance(RemoveCheckpoint::TargetLeft).unwrap();
        }
        if cut == RemoveCheckpoint::SurvivorsProved {
            fence.advance(RemoveCheckpoint::SurvivorsProved).unwrap();
        }
        assert!(fence.finish(&receipt(false), true, &[]).is_err());
        assert!(fence.finish(&receipt(true), false, &[]).is_err());
        let unregister_called = std::cell::Cell::new(false);
        let retained_target = Arc::new(());
        let still_owned = retained_target.clone();
        failed_remove_cleanup(
            &mut fence,
            || Err::<(), _>("unreaped child"),
            || unregister_called.set(true),
        );
        assert!(unregister_called.get());
        assert_ne!(fence.checkpoint, RemoveCheckpoint::Completed);
        assert_eq!(fence.source.status(), BuildGenerationStatus::CleanupOnly);
        assert_eq!(fence.donors[0].status(), BuildGenerationStatus::CleanupOnly);
        assert_eq!(fence.target.status(), BuildGenerationStatus::NeedsRebuild);
        assert_eq!(
            Arc::strong_count(&still_owned),
            2,
            "failed cleanup retains ownership"
        );
        drop(fence);
        assert!(BuildGroupPermit::try_acquire(group).is_none());
    }
}

#[test]
fn cleanup_reserve_exhaustion_cannot_complete_but_proven_cleanup_releases_survivors() {
    let parent = AttemptContext::new(
        AttemptId::new("remove-budget-parent").unwrap(),
        Instant::now() + Duration::from_secs(60),
        Duration::from_secs(5),
    )
    .unwrap();
    let child = BuildAttemptContext::new(
        parent.clone(),
        AttemptId::new("remove-budget-child").unwrap(),
        Instant::now() + Duration::from_millis(50),
        Duration::from_millis(20),
    )
    .unwrap();
    let mut expired = fence("remove-budget-exhausted");
    expired.advance(RemoveCheckpoint::TargetLeft).unwrap();
    expired.advance(RemoveCheckpoint::SurvivorsProved).unwrap();
    std::thread::sleep(Duration::from_millis(55));
    assert!(expired.finish(&receipt(true), true, &[&child]).is_err());
    assert_ne!(expired.checkpoint, RemoveCheckpoint::Completed);
    drop(expired);
    assert!(BuildGroupPermit::try_acquire("remove-budget-exhausted").is_none());
    let live = fresh_removal_group(
        &OperationId::new("expired-build-id"),
        43,
        Instant::now() + Duration::from_secs(30),
    )
    .unwrap()
    .child(parent)
    .unwrap();
    let mut complete = fence("remove-completion-positive");
    assert!(complete.advance(RemoveCheckpoint::SurvivorsProved).is_err());
    complete.advance(RemoveCheckpoint::TargetLeft).unwrap();
    complete.advance(RemoveCheckpoint::SurvivorsProved).unwrap();
    complete.finish(&receipt(true), true, &[&live]).unwrap();
    assert_eq!(complete.source.status(), BuildGenerationStatus::Available);
    assert_eq!(
        complete.donors[0].status(),
        BuildGenerationStatus::Available
    );
    assert_eq!(
        complete.target.status(),
        BuildGenerationStatus::NeedsRebuild
    );
    drop(complete);
    assert!(BuildGroupPermit::try_acquire("remove-completion-positive").is_some());
}

#[tokio::test]
async fn remove_partial_owner_acquisition_and_build_contention_are_bounded_without_retirement() {
    let first = AsyncMutex::new(());
    let second = AsyncMutex::new(());
    let held_second = second.lock().await;
    let permit = BuildGroupPermit::try_acquire("remove-partial-owners").unwrap();
    let armed = std::cell::Cell::new(false);
    let result = bounded_remove_work(Instant::now() + Duration::from_millis(10), async {
        let _permit = permit;
        let _source = first.lock().await;
        assert!(
            BuildGroupPermit::try_acquire("remove-partial-owners").is_none(),
            "Build cannot acquire during Remove"
        );
        let _target = second.lock().await;
        armed.set(true);
        Ok(())
    })
    .await;
    assert!(result.is_err());
    assert!(!armed.get());
    assert!(first.try_lock().is_ok());
    assert!(BuildGroupPermit::try_acquire("remove-partial-owners").is_some());
    drop(held_second);
}
