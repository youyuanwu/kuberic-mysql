use super::super::remove::deterministic_tests::{completed_fixture, information};
use super::*;
use crate::core::GtidSet;
use kuberic_runtime::protocol::types::{ConfigurationMember, StorageGeneration};

fn successor(
    owned: &super::super::super::build::CompletedBuildOwnership,
) -> ReplicaSetConfiguration {
    let mut members = owned
        .callback
        .source_configuration
        .configuration
        .members
        .clone();
    members.push(ConfigurationMember {
        identity: owned.binding.target.identity.clone(),
        role: ReplicaRole::ActiveSecondary,
    });
    ReplicaSetConfiguration {
        configuration: ConfigurationDescriptor::new(
            owned.binding.epoch,
            owned.binding.source.identity.replica_id,
            members,
            2,
        ),
        replicas: vec![
            information(2, ReplicaRole::ActiveSecondary),
            information(3, ReplicaRole::ActiveSecondary),
        ],
    }
}

fn scope(
    owned: &super::super::super::build::CompletedBuildOwnership,
    successor: &ReplicaSetConfiguration,
    target: bool,
    installed: bool,
) -> ProofScope {
    let peer = information(if target { 3 } else { 2 }, ReplicaRole::ActiveSecondary);
    let native = translate_replica_information(&peer).unwrap();
    let authority = if target {
        owned.target_authority.clone()
    } else {
        owned.donors[0].authority.clone()
    };
    let current = if installed {
        Some(successor.clone())
    } else if target {
        None
    } else {
        Some(*owned.callback.source_configuration.clone())
    };
    let current = current.map(|mut c| {
        c.replicas = (1..=if installed { 3 } else { 2 })
            .filter(|id| i64::from(*id) != peer.identity.replica_id.value())
            .map(|id| {
                information(
                    id,
                    if id == 1 {
                        ReplicaRole::Primary
                    } else {
                        ReplicaRole::ActiveSecondary
                    },
                )
            })
            .collect();
        c
    });
    ProofScope {
        binding: Some(PublicBinding {
            identity: peer.identity,
            process_session: peer.process_session_id,
            public_storage: Some(native.storage().clone()),
            settings: ReplicatorSettings {
                replication_address: peer.replication_address,
            },
        }),
        epoch: Some(MysqlReplicaEpoch::new(1)),
        role: Some(MysqlReplicaRole::Secondary),
        current,
        pending: None,
        build: (!installed).then(|| owned.binding.clone()),
        generation: if target {
            owned.binding.target_generation
        } else {
            owned.donors[0].generation
        },
        authority,
        status: if installed {
            BuildGenerationStatus::Available
        } else {
            BuildGenerationStatus::PendingAdmission
        },
        public_epoch: Some(owned.binding.epoch),
        mapped_epoch_matches: true,
        admitted_role: Some(ReplicaRole::ActiveSecondary),
        pending_join: target && !installed,
        native_identity: native.identity().clone(),
        native_successor: !target && !installed,
        idle_preparation: IdleBuildPreparation::default(),
        access: Some(MysqlAccessIntent::new(NonZeroU64::MIN, false, false)),
        proxy: ProxyState::Unprepared,
    }
}

#[test]
fn paired_scope_cannot_retag_binding_lifecycle_build_or_access_changes() {
    type ScopeMutation<'a> = (&'static str, &'a dyn Fn(&mut ProofScope));

    let owned = completed_fixture();
    let next = successor(&owned);
    let original = scope(&owned, &next, true, false);
    let mutations: &[ScopeMutation<'_>] = &[
        ("replica identity", &|s| {
            s.binding.as_mut().unwrap().identity =
                information(2, ReplicaRole::ActiveSecondary).identity;
        }),
        ("process session", &|s| {
            s.binding.as_mut().unwrap().process_session =
                PublicProcessSessionId::new("replaced-session");
        }),
        ("storage", &|s| {
            s.binding.as_mut().unwrap().public_storage = None;
        }),
        ("replication endpoint", &|s| {
            s.binding.as_mut().unwrap().settings.replication_address = "replaced-endpoint".into();
        }),
        ("native epoch", &|s| s.epoch = None),
        ("native role", &|s| {
            s.role = Some(MysqlReplicaRole::Primary);
        }),
        ("current configuration", &|s| {
            s.current = Some(next.clone());
        }),
        ("pending configuration", &|s| {
            s.pending = Some(next.clone());
        }),
        ("build binding", &|s| s.build = None),
        ("build generation", &|s| s.generation += 1),
        ("native authority", &|s| {
            s.authority = owned.binding.source_authority.clone();
        }),
        ("build status", &|s| {
            s.status = BuildGenerationStatus::NeedsRebuild;
        }),
        ("public epoch", &|s| {
            s.public_epoch = Some(Epoch::new(0, 99));
        }),
        ("epoch mapping", &|s| s.mapped_epoch_matches = false),
        ("admitted role", &|s| s.admitted_role = None),
        ("idle preparation", &|s| {
            s.idle_preparation.admit_role(owned.binding.epoch).unwrap();
        }),
        ("access", &|s| {
            s.access = Some(MysqlAccessIntent::new(NonZeroU64::MIN, true, true));
        }),
        ("proxy state", &|s| s.proxy = ProxyState::Revoked),
    ];
    for (dimension, mutate) in mutations {
        let mut changed = original.clone();
        mutate(&mut changed);
        assert_ne!(original, changed, "scope dimension {dimension}");
    }
}

fn evidence(
    authority: crate::service::MemberMutationAuthority,
    view: &crate::core::NativeView,
    history: &str,
) -> MemberNativeEvidence {
    MemberNativeEvidence::new(authority, view.clone(), history.parse::<GtidSet>().unwrap())
        .unwrap()
        .with_durable_profile()
}

#[test]
fn composed_transition_proof_is_exact_in_staged_source_first_and_installed_states() {
    let owned = completed_fixture();
    let next = successor(&owned);
    assert!(additive_build_configuration(&owned.binding, &next));
    let frozen = evidence(
        owned.binding.source_authority.clone(),
        &owned.successor,
        "cccccccc-cccc-cccc-cccc-cccccccccccc:1-7",
    );
    for (phase, installed, applicable) in [
        (
            "staged",
            false,
            vec![*owned.callback.source_configuration.clone(), next.clone()],
        ),
        ("source-first", false, vec![next.clone()]),
        ("installed", true, vec![next.clone()]),
    ] {
        for target in [false, true] {
            let peer = information(if target { 3 } else { 2 }, ReplicaRole::ActiveSecondary);
            let valid = scope(&owned, &next, target, installed);
            let observed = evidence(
                valid.authority.clone(),
                &owned.successor,
                "cccccccc-cccc-cccc-cccc-cccccccccccc:1-7",
            );
            let validate = |scope: &ProofScope,
                            peer: &ReplicaInformation,
                            observation: &MemberNativeEvidence| {
                validate_voter_observation(
                    scope,
                    std::slice::from_ref(&owned),
                    &applicable,
                    peer,
                    &frozen,
                    observation,
                )
            };
            validate(&valid, &peer, &observed).unwrap();
            let mut changed_peer = peer.clone();
            changed_peer.process_session_id = PublicProcessSessionId::new("substituted");
            assert!(
                validate(&valid, &changed_peer, &observed).is_err(),
                "phase {phase} target {target} session"
            );
            changed_peer = peer.clone();
            changed_peer
                .storage_incarnation
                .as_mut()
                .unwrap()
                .storage_generation = StorageGeneration::new("substituted");
            assert!(
                validate(&valid, &changed_peer, &observed).is_err(),
                "phase {phase} target {target} storage"
            );
            for history in [
                "cccccccc-cccc-cccc-cccc-cccccccccccc:1-6",
                "cccccccc-cccc-cccc-cccc-cccccccccccc:1-5:7-9",
            ] {
                let bad = evidence(valid.authority.clone(), &owned.successor, history);
                assert!(
                    validate(&valid, &peer, &bad).is_err(),
                    "phase {phase} target {target} complete history"
                );
                assert_eq!(peer.current_progress, 7, "advertised scalar is unchanged");
            }
            if !installed {
                let mut changed_scope = valid.clone();
                changed_scope.generation += 1;
                assert!(
                    validate(&changed_scope, &peer, &observed).is_err(),
                    "retained generation"
                );
                changed_scope = valid.clone();
                changed_scope.build = None;
                assert!(
                    validate(&changed_scope, &peer, &observed).is_err(),
                    "retained binding"
                );
                changed_scope = valid.clone();
                changed_scope.build.as_mut().unwrap().grant_id.push('x');
                assert!(
                    validate(&changed_scope, &peer, &observed).is_err(),
                    "substituted binding"
                );
                let mut changed_owned = owned.clone();
                changed_owned.successor = crate::core::NativeView::new(
                    owned.successor.group_name().clone(),
                    crate::core::ViewId::new("700:3").unwrap(),
                    owned.successor.members().to_vec(),
                )
                .unwrap();
                assert!(
                    validate_voter_observation(
                        &valid,
                        &[changed_owned],
                        &applicable,
                        &peer,
                        &frozen,
                        &observed
                    )
                    .is_err(),
                    "same-member view cannot reuse transition authority"
                );
            }
        }
    }
}

#[test]
fn exact_additive_configuration_rejects_each_peer_session_storage_and_history_epoch_substitution() {
    let owned = completed_fixture();
    let valid = successor(&owned);
    for index in 0..2 {
        let mut changed = valid.clone();
        changed.replicas[index].process_session_id = PublicProcessSessionId::new("substituted");
        assert!(!additive_build_configuration(&owned.binding, &changed));
        assert!(!applicable_configurations_supported(
            std::slice::from_ref(&owned),
            &[*owned.callback.source_configuration.clone(), changed]
        ));
        let mut changed = valid.clone();
        changed.replicas[index]
            .storage_incarnation
            .as_mut()
            .unwrap()
            .storage_generation = StorageGeneration::new("substituted");
        assert!(!additive_build_configuration(&owned.binding, &changed));
    }
    let mut changed = valid;
    changed.configuration.epoch.data_loss_number += 1;
    assert!(!additive_build_configuration(&owned.binding, &changed));
}
