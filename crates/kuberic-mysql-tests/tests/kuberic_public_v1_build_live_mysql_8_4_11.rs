#[path = "service_live_support/mod.rs"]
mod live_support;

use std::collections::BTreeSet;
use std::future::{Future, poll_fn};
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::num::NonZeroU64;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use kuberic_mysql::core::{AttemptId, ConfiguredGtidLineage, CredentialGeneration, GtidSet};
use kuberic_mysql::service::{
    ApplicationCredential, AttemptContext, ControlCredential, ControlCredentialRole,
    MysqlFreshMemberCredentials, MysqlInstanceConfig, MysqlMemberConfig, MysqlMemberIndex,
    MysqlOperationTimeouts, MysqlPublicV1Replicator, MysqlPublicV1ServiceConfig,
    MysqlTopologyConfig,
};
use kuberic_runtime::RuntimeError;
use kuberic_runtime::application::INVALID_LSN;
use kuberic_runtime::protocol::types::{
    AgentGeneration, ConfigurationDescriptor, ConfigurationMember, Epoch, OperationId, PodUid,
    ProcessSessionId, PvcUid, ReplicaId, ReplicaIdentity, ReplicaInstanceId, ReplicaRole,
    ReplicaStorageIdentity, ReplicaStorageIncarnationContext, StorageGeneration,
};
use kuberic_runtime::replicator::{
    PrimaryReplicator, ReplicaInformation, ReplicaSetConfiguration, ReplicaSetQuorumMode,
    Replicator,
};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder};
use tokio::time::sleep;

use live_support::{
    APPARMOR_EXEC, FixtureRoot, MYSQLD, ReservedPorts, assert_tcp_refuses,
    preflight_oracle_mysql_8_4_11,
};

const GROUP_UUID: &str = "b5a47b3f-bf9f-4c7c-8420-495d5a52cb09";

#[test]
fn custom_primary_builds_two_fresh_secondaries() {
    run_live_fixture(false, false);
}

#[test]
fn custom_primary_builds_two_fresh_secondaries_with_omitted_progress() {
    run_live_fixture(false, true);
}

#[test]
fn custom_second_build_drop_is_contained() {
    run_live_fixture(true, false);
}

#[test]
fn stale_source_first_target_epoch_cannot_supply_commitment() {
    run_live_scenario(false, false, 1);
}

#[test]
fn exact_build_retry_cannot_requalify_observed_undurable_history() {
    run_live_scenario(false, false, 2);
}

#[test]
fn completed_idle_successor_view_churn_and_missing_voter_are_rejected() {
    run_live_scenario(false, false, 3);
}

#[test]
fn missing_configured_voter_cannot_supply_a_retained_commitment() {
    run_live_scenario(false, false, 4);
}

#[test]
fn provider_epoch_rejects_native_latch_before_driver_progress_reentry() {
    run_live_scenario(false, false, 5);
}

#[test]
fn remove_first_completed_idle_retires_only_its_exact_owner() {
    run_live_scenario(false, false, 10);
}

#[test]
fn remove_second_completed_idle_preserves_primary_and_admitted_secondary() {
    run_live_scenario(false, false, 11);
}

#[test]
fn remove_rejects_stale_target_epoch_without_retirement() {
    run_live_scenario(false, false, 12);
}

#[test]
fn stale_remove_does_not_retire_substituted_target_incarnation() {
    run_live_scenario(false, false, 13);
}

#[test]
fn interrupted_remove_after_native_leave_is_contained_not_successful() {
    run_live_scenario(false, false, 20);
}

#[test]
fn dropped_remove_before_effects_is_retryable() {
    run_live_scenario(false, false, 21);
}

#[test]
fn fresh_role_epoch_and_idle_reports_are_coherent_without_admission() {
    run_live_scenario(false, false, 30);
}

#[test]
fn fresh_history_change_between_scalar_and_provider_cannot_return_zero() {
    run_live_scenario(false, false, 31);
}

#[test]
fn fresh_durability_loss_cannot_be_restored_into_zero_progress() {
    run_live_scenario(false, false, 32);
}

#[test]
fn cancelled_fresh_query_retires_qualification_and_releases_ownership() {
    run_live_scenario(false, false, 33);
}

#[test]
fn abort_during_fresh_query_contains_the_exact_owner() {
    run_live_scenario(false, false, 34);
}

#[test]
fn native_unwind_retains_owned_scratch_diagnostics_without_reuse() {
    assert!(
        std::panic::catch_unwind(|| run_live_scenario(false, false, 35)).is_err(),
        "controlled native unwind must reach the fixture's panic path"
    );
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("target")
        .join(format!("km-public-build-{}", std::process::id()));
    for member in 1..=3 {
        assert!(!root.join(format!("member-{member}-scratch")).exists());
        let retained = root.join(format!("member-{member}-scratch.retained"));
        assert!(retained.is_dir());
        for log in ["mysqld.err", "mysqld.out"] {
            assert!(retained.join(log).is_file(), "missing retained {log}");
        }
        assert!(
            root.join(format!("member-{member}-data/auto.cnf"))
                .is_file()
        );
    }
    assert!(
        FixtureRoot::new("public-build-live").is_err(),
        "a later fixture must not replace retained native evidence"
    );
    // This deliberately failed fixture remains local for diagnostic review.
    eprintln!("verified retained native unwind: {}", root.display());
}

fn run_live_fixture(interrupt_second: bool, omit_source_progress: bool) {
    run_live_scenario(interrupt_second, omit_source_progress, 0);
}

fn run_live_scenario(interrupt_second: bool, omit_source_progress: bool, proof_case: u8) {
    std::thread::Builder::new()
        .name("mysql-public-build-live".into())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(run_custom_build_fixture(
                    interrupt_second,
                    omit_source_progress,
                    proof_case,
                ));
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn run_custom_build_fixture(
    interrupt_second: bool,
    omit_source_progress: bool,
    proof_case: u8,
) {
    preflight_oracle_mysql_8_4_11().unwrap_or_else(|error| panic!("{error}"));
    let mut fixture = FixtureRoot::new("public-build-live").unwrap();
    let root = fixture.path().to_path_buf();
    let mut ports = ReservedPorts::allocate().unwrap();
    let sql = ports.sql_addresses();
    let group = ports.group_replication_addresses();
    let topology = topology(sql, group);
    let proxy_reservations = reserve_proxy_addresses();
    let proxy = proxy_reservations
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect::<Vec<_>>();
    ports.release();
    drop(proxy_reservations);

    let epoch = Epoch::new(0, 1);
    let identities = [identity(1), identity(2), identity(3)];
    let sessions = [
        ProcessSessionId::new("direct-process-1"),
        ProcessSessionId::new("direct-process-2"),
        ProcessSessionId::new("direct-process-3"),
    ];
    let storage = [
        storage(&identities[0]),
        storage(&identities[1]),
        storage(&identities[2]),
    ];
    let timeouts = MysqlOperationTimeouts::new(
        Duration::from_secs(180),
        Duration::from_secs(120),
        Duration::from_secs(30),
        Duration::from_secs(15),
    )
    .unwrap();

    let mut drivers = Vec::new();
    let member_indexes = [
        MysqlMemberIndex::First,
        MysqlMemberIndex::Second,
        MysqlMemberIndex::Third,
    ];
    for index in 0..3 {
        let config = direct_config(
            &root,
            topology.clone(),
            member_indexes[index],
            timeouts,
            proxy[index],
            index,
        );
        drivers.push(
            config
                .open_direct_for_qualification(
                    identities[index].clone(),
                    sessions[index].clone(),
                    storage[index].clone(),
                )
                .await
                .unwrap(),
        );
        assert_tcp_login_rejected(sql[index], "root", None).await;
        assert_tcp_login_rejected(
            sql[index],
            &format!("kms_obs_{index}"),
            Some(&format!("ObserverSecret{index}A7")),
        )
        .await;
        assert_tcp_login_rejected(
            sql[index],
            &format!("kms_rec_{index}"),
            Some(&format!("RecoverySecret{index}B9")),
        )
        .await;
        assert_tcp_login_rejected(sql[index], &format!("kms_app_{index}"), None).await;
    }
    if proof_case >= 30 {
        qualify_fresh_lifecycle(&drivers, &root, &proxy, proof_case).await;
        for driver in drivers.iter().rev() {
            driver.close().await.unwrap();
        }
        fixture.remove().unwrap();
        return;
    }
    drivers[0].update_epoch(epoch).await.unwrap();
    kuberic_runtime::application::StateProvider::update_epoch(drivers[0].as_ref(), epoch, 0)
        .await
        .unwrap();
    drivers[0]
        .change_role(epoch, ReplicaRole::Primary)
        .await
        .unwrap();
    let singleton = configuration(
        epoch,
        0,
        &[ReplicaRole::Primary],
        &drivers,
        &identities,
        &sessions,
        &storage,
        &[0],
    )
    .await;
    drivers[0]
        .update_current_replica_set_configuration(singleton.clone())
        .await
        .unwrap();
    let mut accepted_source_configuration = singleton.clone();

    let before_write = qualified_progress(drivers[0].as_ref()).await;
    commit_marker(&instance_socket(&root, 0), "coherent-provider-pair").await;
    drivers[0]
        .wait_for_catch_up_quorum(ReplicaSetQuorumMode::All)
        .await
        .unwrap();
    assert_eq!(
        kuberic_runtime::application::StateProvider::last_committed_lsn(drivers[0].as_ref())
            .await
            .unwrap(),
        before_write
    );
    assert!(qualified_progress(drivers[0].as_ref()).await > before_write);

    for target in 1..3 {
        drivers[target].update_epoch(epoch).await.unwrap();
        drivers[target]
            .change_role(epoch, ReplicaRole::IdleSecondary)
            .await
            .unwrap();
        assert_fresh_pair(drivers[target].as_ref()).await;

        let socket = instance_socket(&root, 0);
        commit_marker(&socket, &format!("build_marker_{target}")).await;
        assert!(qualified_progress(drivers[0].as_ref()).await > 0);
        let build = idle_information(
            target,
            OperationId::new(format!("direct-build-{target}")),
            &drivers,
            &identities,
            &sessions,
            &storage,
        );
        if interrupt_second && target == 2 {
            let mut build_future = Box::pin(drivers[0].build_replica(build));
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                tokio::select! {
                    result = &mut build_future => {
                        panic!("second Build completed before interruption: {result:?}");
                    }
                    () = sleep(Duration::from_millis(100)) => {
                        if group_member_count(&instance_socket(&root, 0)).await >= 3 {
                            break;
                        }
                        assert!(Instant::now() < deadline, "second Build never entered native view");
                    }
                }
            }
            drop(build_future);
            assert!(
                !instance_socket(&root, 2).exists(),
                "target socket must be absent when dropped Build returns control"
            );
            wait_for_build_accounts_removed(&instance_socket(&root, 0)).await;
            wait_for_build_accounts_removed(&instance_socket(&root, 1)).await;
            let same_request = idle_information(
                target,
                OperationId::new(format!("direct-build-{target}")),
                &drivers,
                &identities,
                &sessions,
                &storage,
            );
            assert!(matches!(
                Box::pin(drivers[0].build_replica(same_request)).await,
                Err(RuntimeError::InvalidReplication(_))
            ));
            let retry = idle_information(
                target,
                OperationId::new("direct-build-2-retry"),
                &drivers,
                &identities,
                &sessions,
                &storage,
            );
            assert!(matches!(
                Box::pin(drivers[0].build_replica(retry)).await,
                Err(RuntimeError::InvalidReplication(_))
            ));
            drivers[1].close().await.unwrap();
            drivers[0].close().await.unwrap();
            fixture.remove().unwrap();
            return;
        }
        Box::pin(drivers[0].build_replica(build.clone()))
            .await
            .unwrap();
        if proof_case >= 10 && (target == 2 || matches!(proof_case, 10 | 12 | 13)) {
            assert!(drivers[0].remove_replica(ReplicaId::new(99)).await.is_err());
            assert!(
                drivers[0]
                    .remove_replica(identities[0].replica_id)
                    .await
                    .is_err()
            );
            if target == 2 {
                assert!(
                    drivers[0]
                        .remove_replica(identities[1].replica_id)
                        .await
                        .is_err(),
                    "active membership removal is not the idle callback"
                );
            }
            if proof_case == 12 {
                drivers[target]
                    .change_role(epoch, ReplicaRole::ActiveSecondary)
                    .await
                    .unwrap();
                drivers[target]
                    .update_epoch(Epoch::new(0, 2))
                    .await
                    .unwrap();
                assert!(
                    drivers[0]
                        .remove_replica(identities[target].replica_id)
                        .await
                        .is_err()
                );
                assert!(instance_socket(&root, target).exists());
                for driver in drivers.iter().rev() {
                    driver.close().await.unwrap();
                }
                fixture.remove().unwrap();
                return;
            }
            if proof_case == 13 {
                drivers[target].close().await.unwrap();
                let mut replacement_fixture = FixtureRoot::new("public-repl").unwrap();
                let replacement_root = replacement_fixture.path().to_path_buf();
                let mut replacement_identity = identities[target].clone();
                replacement_identity.instance_id = ReplicaInstanceId::new("replacement-instance");
                replacement_identity.agent_generation = AgentGeneration::new("replacement-agent");
                let replacement_storage = self::storage(&replacement_identity);
                let replacement = direct_config(
                    &replacement_root,
                    topology.clone(),
                    member_indexes[target],
                    timeouts,
                    proxy[target],
                    target,
                )
                .open_direct_for_qualification(
                    replacement_identity,
                    ProcessSessionId::new("replacement-session"),
                    replacement_storage,
                )
                .await
                .unwrap();
                assert!(
                    drivers[0]
                        .remove_replica(identities[target].replica_id)
                        .await
                        .is_err()
                );
                assert!(
                    instance_socket(&replacement_root, target).exists(),
                    "stale removal cannot touch replacement"
                );
                replacement.close().await.unwrap();
                replacement_fixture.remove().unwrap();
                for driver in drivers.iter().rev() {
                    driver.close().await.unwrap();
                }
                fixture.remove().unwrap();
                return;
            }
            if proof_case == 21 {
                let mut removed =
                    Box::pin(drivers[0].remove_replica(identities[target].replica_id));
                assert!(
                    poll_fn(|cx| Poll::Ready(removed.as_mut().poll(cx)))
                        .await
                        .is_pending()
                );
                drop(removed);
                assert!(instance_socket(&root, target).exists());
                qualified_progress(drivers[0].as_ref()).await;
            }
            if proof_case == 20 {
                let mut removed =
                    Box::pin(drivers[0].remove_replica(identities[target].replica_id));
                let deadline = Instant::now() + Duration::from_secs(90);
                loop {
                    let status = poll_fn(|cx| Poll::Ready(removed.as_mut().poll(cx))).await;
                    assert!(
                        status.is_pending(),
                        "Remove completed before the controlled leave cut: {status:?}"
                    );
                    // The callback is not polled while independently checking
                    // native membership: it cannot race past this cut.
                    if group_member_count(&instance_socket(&root, 0)).await == target {
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "Remove did not enter native leave"
                    );
                    sleep(Duration::from_millis(5)).await;
                }
                drop(removed);
                assert!(!instance_socket(&root, target).exists());
                assert!(
                    drivers[0]
                        .remove_replica(identities[target].replica_id)
                        .await
                        .is_err()
                );
                assert!(drivers[0].build_replica(build).await.is_err());
                for member in 0..target {
                    assert!(instance_socket(&root, member).exists());
                    wait_for_build_accounts_removed(&instance_socket(&root, member)).await;
                }
                for driver in drivers[..target].iter().rev() {
                    driver.close().await.unwrap();
                }
                fixture.remove().unwrap();
                return;
            }
            drivers[0]
                .remove_replica(identities[target].replica_id)
                .await
                .unwrap();
            assert!(!instance_socket(&root, target).exists());
            assert!(!root.join(format!("member-{}-scratch", target + 1)).exists());
            assert!(
                root.join(format!("member-{}-data/auto.cnf", target + 1))
                    .is_file(),
                "Remove must retain persistent data"
            );
            assert!(drivers[target].current_progress().await.is_err());
            assert_tcp_refuses(sql[target]).unwrap();
            assert_tcp_refuses(proxy[target]).unwrap();
            assert_eq!(group_member_count(&instance_socket(&root, 0)).await, target);
            for member in 0..target {
                assert!(instance_socket(&root, member).exists());
                wait_for_build_accounts_removed(&instance_socket(&root, member)).await;
            }
            commit_marker(&instance_socket(&root, 0), "primary-after-idle-remove").await;
            qualified_progress(drivers[0].as_ref()).await;
            drivers[0]
                .remove_replica(identities[target].replica_id)
                .await
                .unwrap();
            assert!(
                drivers[0].build_replica(build.clone()).await.is_err(),
                "delayed Build cannot reinstall retired authority"
            );
            let mut substituted = build;
            substituted.build_id = OperationId::new("replacement-build");
            substituted.identity.instance_id = ReplicaInstanceId::new("replacement-instance");
            substituted.storage_incarnation.as_mut().unwrap().replica =
                substituted.identity.clone();
            assert!(
                drivers[0].build_replica(substituted).await.is_err(),
                "ReplicaId reuse is fenced in the source lifetime"
            );
            for driver in drivers.iter().rev() {
                driver.close().await.unwrap();
            }
            fixture.remove().unwrap();
            return;
        }
        assert_secondary_pair_unavailable(drivers[target].as_ref()).await;
        // A terminal exact retry is served from the retained complete
        // binding, but only after fresh Primary/authority/boundary proof.
        Box::pin(drivers[0].build_replica(build.clone()))
            .await
            .unwrap();
        if proof_case == 2 {
            let mut admin = root_connection(&instance_socket(&root, 0)).await;
            admin.query_drop("SET GLOBAL sync_binlog=0").await.unwrap();
            commit_marker(&instance_socket(&root, 0), "invalid-retry-history").await;
            assert!(drivers[0].build_replica(build).await.is_err());
            admin.query_drop("SET GLOBAL sync_binlog=1").await.unwrap();
            assert!(drivers[0].current_progress().await.is_err());
            admin.disconnect().await.unwrap();
            for driver in drivers.iter().rev() {
                driver.close().await.unwrap();
            }
            fixture.remove().unwrap();
            return;
        }
        if proof_case == 3 {
            let mut admin = root_connection(&instance_socket(&root, 0)).await;
            admin
                .query_drop(
                    "CREATE USER 'view_recovery'@'127.0.0.1' IDENTIFIED BY 'ViewRecoverySecret1A'",
                )
                .await
                .unwrap();
            admin.query_drop("GRANT REPLICATION SLAVE, CONNECTION_ADMIN ON *.* TO 'view_recovery'@'127.0.0.1'").await.unwrap();
            let mut target_admin = root_connection(&instance_socket(&root, target)).await;
            target_admin
                .query_drop("STOP GROUP_REPLICATION")
                .await
                .unwrap();
            target_admin.query_drop("START GROUP_REPLICATION USER='view_recovery', PASSWORD='ViewRecoverySecret1A', DEFAULT_AUTH='caching_sha2_password'").await.unwrap();
            let deadline = Instant::now() + Duration::from_secs(30);
            while !local_secondary_online(&instance_socket(&root, target)).await {
                assert!(Instant::now() < deadline);
                sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(group_member_count(&instance_socket(&root, 0)).await, 2);
            assert!(
                drivers[0].current_progress().await.is_err(),
                "same members in a new view cannot reuse completed-idle authority"
            );
            assert!(drivers[0].build_replica(build).await.is_err());
            admin
                .query_drop("DROP USER 'view_recovery'@'127.0.0.1'")
                .await
                .unwrap();
            admin.disconnect().await.unwrap();
            target_admin.disconnect().await.unwrap();
            for driver in drivers.iter().rev() {
                driver.close().await.unwrap();
            }
            fixture.remove().unwrap();
            return;
        }
        let (progress, competing_retry) = tokio::join!(
            drivers[0].current_progress(),
            drivers[0].build_replica(build.clone())
        );
        assert!(progress.is_ok());
        assert!(
            competing_retry.is_err(),
            "Build retry must use the same fail-fast group permit"
        );
        let source_gtids = query_gtids(&instance_socket(&root, 0)).await;
        let target_gtids = query_gtids(&instance_socket(&root, target)).await;
        assert!(!source_gtids.entries().is_empty());
        assert!(source_gtids.is_subset_of(&target_gtids));
        assert_eq!(
            group_member_count(&instance_socket(&root, target)).await,
            target + 1
        );
        let expected_members = drivers[..=target]
            .iter()
            .map(|driver| {
                kuberic_mysql::service::ReplicationEndpoint::decode(&driver.replication_endpoint())
                    .unwrap()
                    .member_id()
                    .as_str()
                    .to_owned()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            group_member_ids(&instance_socket(&root, target)).await,
            expected_members
        );
        assert!(local_secondary_online(&instance_socket(&root, target)).await);
        for member in 0..=target {
            wait_for_build_accounts_removed(&instance_socket(&root, member)).await;
        }
        for address in &proxy {
            assert_tcp_refuses(*address).unwrap();
        }

        let member_count = target + 1;
        let roles = (0..member_count)
            .map(|index| {
                if index == 0 {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                }
            })
            .collect::<Vec<_>>();
        let indexes = (0..member_count).collect::<Vec<_>>();
        let source_configuration = if omit_source_progress {
            configuration_without_remote_progress(
                epoch,
                0,
                &roles,
                &drivers,
                &identities,
                &sessions,
                &storage,
                &indexes,
            )
        } else {
            configuration(
                epoch,
                0,
                &roles,
                &drivers,
                &identities,
                &sessions,
                &storage,
                &indexes,
            )
            .await
        };

        drivers[target]
            .change_role(epoch, ReplicaRole::ActiveSecondary)
            .await
            .unwrap();
        assert_secondary_pair_unavailable(drivers[target].as_ref()).await;
        drivers[0]
            .update_catch_up_replica_set_configuration(
                source_configuration.clone(),
                accepted_source_configuration.clone(),
            )
            .await
            .unwrap();
        drivers[0]
            .wait_for_catch_up_quorum(ReplicaSetQuorumMode::All)
            .await
            .unwrap();
        drivers[0]
            .update_current_replica_set_configuration(source_configuration)
            .await
            .unwrap();
        if proof_case == 1 {
            let before = qualified_progress(drivers[0].as_ref()).await;
            assert_eq!(
                kuberic_runtime::application::StateProvider::last_committed_lsn(
                    drivers[0].as_ref()
                )
                .await
                .unwrap(),
                before
            );
            drivers[target]
                .update_epoch(Epoch::new(0, 2))
                .await
                .unwrap();
            assert!(drivers[0].current_progress().await.is_err());
            assert!(
                kuberic_runtime::application::StateProvider::last_committed_lsn(
                    drivers[0].as_ref()
                )
                .await
                .is_err()
            );
            for driver in drivers.iter().rev() {
                driver.close().await.unwrap();
            }
            fixture.remove().unwrap();
            return;
        }
        accepted_source_configuration = if omit_source_progress {
            configuration_without_remote_progress(
                epoch,
                0,
                &roles,
                &drivers,
                &identities,
                &sessions,
                &storage,
                &indexes,
            )
        } else {
            configuration(
                epoch,
                0,
                &roles,
                &drivers,
                &identities,
                &sessions,
                &storage,
                &indexes,
            )
            .await
        };
        let target_configuration = configuration(
            epoch,
            target,
            &roles,
            &drivers,
            &identities,
            &sessions,
            &storage,
            &indexes,
        )
        .await;
        for existing in 1..=target {
            let local_configuration = if existing == target {
                target_configuration.clone()
            } else {
                configuration(
                    epoch,
                    existing,
                    &roles,
                    &drivers,
                    &identities,
                    &sessions,
                    &storage,
                    &indexes,
                )
                .await
            };
            drivers[existing]
                .update_current_replica_set_configuration(local_configuration)
                .await
                .unwrap();
            assert_secondary_pair_unavailable(drivers[existing].as_ref()).await;
        }
    }

    commit_marker(&instance_socket(&root, 0), "participant-scope-refresh").await;
    let refreshed_peer = configuration(
        epoch,
        1,
        &[
            ReplicaRole::Primary,
            ReplicaRole::ActiveSecondary,
            ReplicaRole::ActiveSecondary,
        ],
        &drivers,
        &identities,
        &sessions,
        &storage,
        &[0, 1, 2],
    )
    .await;
    let retained_pair = qualified_progress(drivers[0].as_ref()).await;
    drivers[1]
        .update_current_replica_set_configuration(refreshed_peer)
        .await
        .unwrap();
    assert!(
        kuberic_runtime::application::StateProvider::last_committed_lsn(drivers[0].as_ref())
            .await
            .is_err(),
        "participant-only full configuration scope change must invalidate paired proof reuse"
    );
    assert_eq!(qualified_progress(drivers[0].as_ref()).await, retained_pair);
    assert_eq!(
        kuberic_runtime::application::StateProvider::last_committed_lsn(drivers[0].as_ref())
            .await
            .unwrap(),
        retained_pair
    );
    let (current, competing_provider) = tokio::join!(
        drivers[0].current_progress(),
        kuberic_runtime::application::StateProvider::last_committed_lsn(drivers[0].as_ref()),
    );
    assert_eq!(current.unwrap(), retained_pair);
    assert!(
        competing_provider.is_err(),
        "simultaneous proof callbacks cannot acquire opposing leases"
    );
    assert_eq!(
        kuberic_runtime::application::StateProvider::last_committed_lsn(drivers[0].as_ref())
            .await
            .unwrap(),
        retained_pair
    );
    let refreshed_source = configuration(
        epoch,
        0,
        &[
            ReplicaRole::Primary,
            ReplicaRole::ActiveSecondary,
            ReplicaRole::ActiveSecondary,
        ],
        &drivers,
        &identities,
        &sessions,
        &storage,
        &[0, 1, 2],
    )
    .await;
    qualified_progress(drivers[0].as_ref()).await;
    drivers[0]
        .update_current_replica_set_configuration(refreshed_source)
        .await
        .unwrap();
    assert!(
        kuberic_runtime::application::StateProvider::last_committed_lsn(drivers[0].as_ref())
            .await
            .is_err(),
        "source-only full configuration scope change must invalidate paired proof reuse"
    );
    let fresh_pair = qualified_progress(drivers[0].as_ref()).await;
    assert_eq!(
        kuberic_runtime::application::StateProvider::last_committed_lsn(drivers[0].as_ref())
            .await
            .unwrap(),
        fresh_pair
    );
    if proof_case == 4 {
        drivers[2].close().await.unwrap();
        assert!(drivers[0].current_progress().await.is_err());
        assert!(
            kuberic_runtime::application::StateProvider::last_committed_lsn(drivers[0].as_ref())
                .await
                .is_err()
        );
        assert!(instance_socket(&root, 0).exists());
        assert!(instance_socket(&root, 1).exists());
        for driver in drivers.iter().rev() {
            driver.close().await.unwrap();
        }
        fixture.remove().unwrap();
        return;
    }

    if proof_case == 5 {
        let committed = qualified_progress(drivers[0].as_ref()).await;
        let next = Epoch::new(0, 2);
        drivers[0].update_epoch(next).await.unwrap();
        kuberic_runtime::application::StateProvider::update_epoch(
            drivers[0].as_ref(),
            next,
            committed,
        )
        .await
        .unwrap();
        let mut admin = root_connection(&instance_socket(&root, 0)).await;
        admin.query_drop("SET GLOBAL sync_binlog=0").await.unwrap();
        drivers[0]
            .change_role(next, ReplicaRole::Primary)
            .await
            .unwrap();
        admin.query_drop("SET GLOBAL sync_binlog=1").await.unwrap();
        assert!(
            kuberic_runtime::application::StateProvider::update_epoch(
                drivers[0].as_ref(),
                next,
                committed
            )
            .await
            .is_err(),
            "native latch must fence epoch validation before any progress callback copies it"
        );
        admin.disconnect().await.unwrap();
        for driver in drivers.iter().rev() {
            driver.close().await.unwrap();
        }
        fixture.remove().unwrap();
        return;
    }

    if omit_source_progress {
        let committed = qualified_progress(drivers[0].as_ref()).await;
        drivers[0].update_epoch(Epoch::new(0, 2)).await.unwrap();
        kuberic_runtime::application::StateProvider::update_epoch(
            drivers[0].as_ref(),
            Epoch::new(0, 2),
            committed,
        )
        .await
        .unwrap();
        assert!(
            kuberic_runtime::application::StateProvider::update_epoch(
                drivers[0].as_ref(),
                Epoch::new(0, 2),
                committed + 1
            )
            .await
            .is_err()
        );
        drivers[0].update_epoch(Epoch::new(0, 3)).await.unwrap();
        assert!(
            kuberic_runtime::application::StateProvider::update_epoch(
                drivers[0].as_ref(),
                Epoch::new(0, 3),
                committed
            )
            .await
            .is_err(),
            "an unrelated older proof cannot authorize the next epoch"
        );
    } else {
        let mut peer_admin = root_connection(&instance_socket(&root, 1)).await;
        peer_admin
            .query_drop("SET GLOBAL sync_binlog=0")
            .await
            .unwrap();
        assert!(drivers[0].current_progress().await.is_err());
        peer_admin
            .query_drop("SET GLOBAL sync_binlog=1")
            .await
            .unwrap();
        assert!(
            drivers[0].current_progress().await.is_err(),
            "restored tuple cannot requalify peer history"
        );
        peer_admin.disconnect().await.unwrap();
        let mut source_admin = root_connection(&instance_socket(&root, 0)).await;
        source_admin
            .query_drop("SET GLOBAL sync_binlog=0")
            .await
            .unwrap();
        commit_marker(&instance_socket(&root, 0), "unqualified-history").await;
        assert!(drivers[0].current_progress().await.is_err());
        source_admin
            .query_drop("SET GLOBAL sync_binlog=1")
            .await
            .unwrap();
        assert!(
            drivers[0].current_progress().await.is_err(),
            "restored tuple cannot requalify source history"
        );
        source_admin.disconnect().await.unwrap();
    }

    for driver in drivers.iter().rev() {
        driver.close().await.unwrap();
    }
    fixture.remove().unwrap();
}

fn direct_config(
    root: &Path,
    topology: MysqlTopologyConfig,
    member: MysqlMemberIndex,
    timeouts: MysqlOperationTimeouts,
    proxy: SocketAddr,
    index: usize,
) -> MysqlPublicV1ServiceConfig {
    let attempt = AttemptContext::new(
        AttemptId::new(format!("direct-member-{index}-attempt")).unwrap(),
        Instant::now() + Duration::from_secs(600),
        Duration::from_secs(30),
    )
    .unwrap();
    let credentials = MysqlFreshMemberCredentials::new(
        ControlCredential::new(
            attempt.attempt().clone(),
            CredentialGeneration::new(format!("observer-{index}")).unwrap(),
            ControlCredentialRole::Observer,
            format!("kms_obs_{index}"),
            format!("ObserverSecret{index}A7"),
        )
        .unwrap(),
        ControlCredential::new(
            attempt.attempt().clone(),
            CredentialGeneration::new(format!("recovery-{index}")).unwrap(),
            ControlCredentialRole::Recovery,
            format!("kms_rec_{index}"),
            format!("RecoverySecret{index}B9"),
        )
        .unwrap(),
        ApplicationCredential::new(
            attempt.attempt().clone(),
            CredentialGeneration::new(format!("application-{index}")).unwrap(),
            format!("kms_app_{index}"),
            format!("ApplicationSecret{index}C3"),
        )
        .unwrap(),
    );
    let number = index + 1;
    let instance = MysqlInstanceConfig::new_topology_member(
        MYSQLD,
        APPARMOR_EXEC,
        root.join(format!("member-{number}-data")),
        root.join(format!("member-{number}-scratch")),
        topology,
        member,
        timeouts,
    )
    .unwrap();
    MysqlPublicV1ServiceConfig::new(
        format!("direct-partition-{index}"),
        instance,
        ConfiguredGtidLineage::new(GROUP_UUID).unwrap(),
        credentials,
        attempt,
        NonZeroU64::MIN,
        proxy,
    )
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn configuration(
    epoch: Epoch,
    local: usize,
    roles: &[ReplicaRole],
    drivers: &[Arc<MysqlPublicV1Replicator>],
    identities: &[ReplicaIdentity; 3],
    sessions: &[ProcessSessionId; 3],
    storage: &[ReplicaStorageIncarnationContext; 3],
    members: &[usize],
) -> ReplicaSetConfiguration {
    let primary = members
        .iter()
        .copied()
        .find(|index| roles[*index] == ReplicaRole::Primary)
        .unwrap();
    let descriptors = members
        .iter()
        .copied()
        .map(|index| ConfigurationMember {
            identity: identities[index].clone(),
            role: roles[index],
        })
        .collect::<Vec<_>>();
    let mut replicas = Vec::new();
    for index in members.iter().copied().filter(|index| *index != local) {
        replicas.push(active_information(
            index,
            roles[index],
            qualified_progress(drivers[index].as_ref()).await,
            drivers,
            identities,
            sessions,
            storage,
        ));
    }
    ReplicaSetConfiguration {
        configuration: ConfigurationDescriptor::new(
            epoch,
            identities[primary].replica_id,
            descriptors,
            if members.len() == 1 { 1 } else { 2 },
        ),
        replicas,
    }
}

#[allow(clippy::too_many_arguments)]
fn configuration_without_remote_progress(
    epoch: Epoch,
    local: usize,
    roles: &[ReplicaRole],
    drivers: &[Arc<MysqlPublicV1Replicator>],
    identities: &[ReplicaIdentity; 3],
    sessions: &[ProcessSessionId; 3],
    storage: &[ReplicaStorageIncarnationContext; 3],
    members: &[usize],
) -> ReplicaSetConfiguration {
    let primary = members
        .iter()
        .copied()
        .find(|index| roles[*index] == ReplicaRole::Primary)
        .unwrap();
    let descriptors = members
        .iter()
        .copied()
        .map(|index| ConfigurationMember {
            identity: identities[index].clone(),
            role: roles[index],
        })
        .collect::<Vec<_>>();
    let replicas = members
        .iter()
        .copied()
        .filter(|index| *index != local)
        .map(|index| {
            active_information(
                index,
                roles[index],
                INVALID_LSN,
                drivers,
                identities,
                sessions,
                storage,
            )
        })
        .collect();
    ReplicaSetConfiguration {
        configuration: ConfigurationDescriptor::new(
            epoch,
            identities[primary].replica_id,
            descriptors,
            if members.len() == 1 { 1 } else { 2 },
        ),
        replicas,
    }
}

fn active_information(
    index: usize,
    role: ReplicaRole,
    progress: i64,
    drivers: &[Arc<MysqlPublicV1Replicator>],
    identities: &[ReplicaIdentity; 3],
    sessions: &[ProcessSessionId; 3],
    storage: &[ReplicaStorageIncarnationContext; 3],
) -> ReplicaInformation {
    ReplicaInformation {
        build_id: OperationId::default(),
        identity: identities[index].clone(),
        replication_address: drivers[index].replication_endpoint(),
        process_session_id: sessions[index].clone(),
        role,
        current_progress: progress,
        catch_up_capability: INVALID_LSN,
        must_catchup: false,
        storage_incarnation: Some(storage[index].clone()),
    }
}

fn idle_information(
    index: usize,
    build_id: OperationId,
    drivers: &[Arc<MysqlPublicV1Replicator>],
    identities: &[ReplicaIdentity; 3],
    sessions: &[ProcessSessionId; 3],
    storage: &[ReplicaStorageIncarnationContext; 3],
) -> ReplicaInformation {
    ReplicaInformation {
        build_id,
        identity: identities[index].clone(),
        replication_address: drivers[index].replication_endpoint(),
        process_session_id: sessions[index].clone(),
        role: ReplicaRole::IdleSecondary,
        current_progress: INVALID_LSN,
        catch_up_capability: INVALID_LSN,
        must_catchup: false,
        storage_incarnation: Some(storage[index].clone()),
    }
}

fn identity(index: i64) -> ReplicaIdentity {
    ReplicaIdentity {
        replica_id: ReplicaId::new(index),
        instance_id: ReplicaInstanceId::new(format!("direct-instance-{index}")),
        agent_generation: AgentGeneration::new(format!("direct-agent-{index}")),
    }
}

fn storage(identity: &ReplicaIdentity) -> ReplicaStorageIncarnationContext {
    ReplicaStorageIncarnationContext {
        replica: identity.clone(),
        storage: ReplicaStorageIdentity::Persistent {
            pod_uid: PodUid::new(format!("direct-pod-{}", identity.replica_id.value())),
            pvc_uid: PvcUid::new(format!("direct-pvc-{}", identity.replica_id.value())),
        },
        storage_generation: StorageGeneration::new(format!(
            "direct-storage-{}",
            identity.replica_id.value()
        )),
    }
}

fn topology(sql: [SocketAddr; 3], group: [SocketAddr; 3]) -> MysqlTopologyConfig {
    MysqlTopologyConfig::new([
        MysqlMemberConfig::new(201, sql[0], group[0], GROUP_UUID, group).unwrap(),
        MysqlMemberConfig::new(202, sql[1], group[1], GROUP_UUID, group).unwrap(),
        MysqlMemberConfig::new(203, sql[2], group[2], GROUP_UUID, group).unwrap(),
    ])
    .unwrap()
}

fn reserve_proxy_addresses() -> [TcpListener; 3] {
    [
        TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap(),
        TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap(),
        TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap(),
    ]
}

fn instance_socket(root: &Path, index: usize) -> std::path::PathBuf {
    // Paths are deterministic under MysqlRuntimePaths.
    let number = index + 1;
    root.join(format!("member-{number}-scratch"))
        .join("mysql.sock")
}

async fn assert_secondary_pair_unavailable(driver: &MysqlPublicV1Replicator) {
    assert!(
        qualified_progress(driver).await >= 0,
        "accepted history must be projectable"
    );
    assert!(
        matches!(
            kuberic_runtime::application::StateProvider::last_committed_lsn(driver).await,
            Err(RuntimeError::InvalidReplication(message))
                if message == "MySQL committed progress is unproved: no independently proved committed sample"
        ),
        "local accepted history must not be silently promoted to commitment"
    );
}

async fn assert_fresh_pair(driver: &MysqlPublicV1Replicator) {
    assert_eq!(driver.current_progress().await.unwrap(), 0);
    assert_eq!(
        kuberic_runtime::application::StateProvider::last_committed_lsn(driver)
            .await
            .unwrap(),
        0
    );
}

async fn qualify_fresh_lifecycle(
    drivers: &[Arc<MysqlPublicV1Replicator>],
    root: &Path,
    proxies: &[SocketAddr],
    case: u8,
) {
    use kuberic_runtime::application::StateProvider;
    // Real host/controller observations may poll separate owners concurrently.
    // Fresh history has no peer proof and must not monopolize the group permit.
    let (first, second, third) = tokio::join!(
        drivers[0].current_progress(),
        drivers[1].current_progress(),
        drivers[2].current_progress(),
    );
    assert_eq!((first.unwrap(), second.unwrap(), third.unwrap()), (0, 0, 0));
    let (first, second, third) = tokio::join!(
        StateProvider::last_committed_lsn(drivers[0].as_ref()),
        StateProvider::last_committed_lsn(drivers[1].as_ref()),
        StateProvider::last_committed_lsn(drivers[2].as_ref()),
    );
    assert_eq!((first.unwrap(), second.unwrap(), third.unwrap()), (0, 0, 0));
    for (index, driver) in drivers.iter().enumerate() {
        assert_fresh_pair(driver.as_ref()).await;
        assert_eq!(group_member_count(&instance_socket(root, index)).await, 0);
        assert_tcp_refuses(proxies[index]).unwrap();
    }
    let driver = drivers[0].as_ref();
    let epoch = Epoch::new(0, 1);
    driver
        .change_role(epoch, ReplicaRole::Primary)
        .await
        .unwrap();
    assert_fresh_pair(driver).await;
    driver
        .change_role(epoch, ReplicaRole::Primary)
        .await
        .unwrap();
    Replicator::update_epoch(driver, epoch).await.unwrap();
    StateProvider::update_epoch(driver, epoch, 0).await.unwrap();
    assert_fresh_pair(driver).await;
    match case {
        30 => {
            assert!(matches!(
                PrimaryReplicator::on_data_loss(driver).await,
                Err(RuntimeError::InvalidReplication(message))
                    if message == "MySQL Public V1 data-loss acceptance is unsupported"
            ));
            assert!(matches!(
                StateProvider::on_data_loss(driver).await,
                Err(RuntimeError::InvalidReplication(message))
                    if message == "MySQL Public V1 StateProvider rejects data-loss acceptance"
            ));
            assert!(StateProvider::get_copy_context(driver).await.is_err());
            assert!(driver.change_role(epoch, ReplicaRole::None).await.is_err());
            assert_eq!(driver.catch_up_capability().await.unwrap(), INVALID_LSN);
            assert!(
                driver
                    .change_role(epoch, ReplicaRole::ActiveSecondary)
                    .await
                    .is_err()
            );
            let newer = Epoch::new(0, 2);
            Replicator::update_epoch(driver, newer).await.unwrap();
            assert!(StateProvider::last_committed_lsn(driver).await.is_err());
            assert!(StateProvider::update_epoch(driver, newer, 0).await.is_err());
            assert!(
                driver
                    .change_role(epoch, ReplicaRole::Primary)
                    .await
                    .is_err()
            );
            // A newer epoch also invalidates the previously accepted role.
            assert!(driver.current_progress().await.is_err());
            driver
                .change_role(newer, ReplicaRole::Primary)
                .await
                .unwrap();
            assert_fresh_pair(driver).await;
            let idle = drivers[1].as_ref();
            Replicator::update_epoch(idle, epoch).await.unwrap();
            // The old pre-epoch pair cannot be silently retagged.
            assert!(StateProvider::last_committed_lsn(idle).await.is_err());
            assert_fresh_pair(idle).await;
            idle.change_role(epoch, ReplicaRole::IdleSecondary)
                .await
                .unwrap();
            assert!(StateProvider::last_committed_lsn(idle).await.is_err());
            assert_fresh_pair(idle).await;
            idle.change_role(epoch, ReplicaRole::IdleSecondary)
                .await
                .unwrap();
            assert_fresh_pair(idle).await;
        }
        31 => {
            let mut admin = root_connection(&instance_socket(root, 0)).await;
            admin
                .query_drop("SET GLOBAL super_read_only=OFF")
                .await
                .unwrap();
            admin.query_drop("SET GLOBAL read_only=OFF").await.unwrap();
            commit_marker(&instance_socket(root, 0), "unformed-history").await;
            admin
                .query_drop("SET GLOBAL super_read_only=ON")
                .await
                .unwrap();
            admin.disconnect().await.unwrap();
            assert!(StateProvider::last_committed_lsn(driver).await.is_err());
            assert!(driver.current_progress().await.is_err());
            assert!(StateProvider::update_epoch(driver, epoch, 0).await.is_err());
        }
        32 => {
            let mut admin = root_connection(&instance_socket(root, 0)).await;
            admin.query_drop("SET GLOBAL sync_binlog=0").await.unwrap();
            assert!(driver.current_progress().await.is_err());
            admin.query_drop("SET GLOBAL sync_binlog=1").await.unwrap();
            admin.disconnect().await.unwrap();
            assert!(driver.current_progress().await.is_err());
            assert!(StateProvider::last_committed_lsn(driver).await.is_err());
            assert!(StateProvider::update_epoch(driver, epoch, 0).await.is_err());
        }
        33 | 34 => {
            let mut query = Box::pin(driver.current_progress());
            assert!(
                poll_fn(|cx| Poll::Ready(query.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            if case == 34 {
                driver.abort();
            }
            drop(query);
            assert!(driver.current_progress().await.is_err());
            assert!(StateProvider::last_committed_lsn(driver).await.is_err());
            if case == 34 {
                assert!(!instance_socket(root, 0).exists());
            }
        }
        35 => panic!("controlled native fixture unwind"),
        _ => unreachable!(),
    }
    for proxy in proxies {
        assert_tcp_refuses(*proxy).unwrap();
    }
}

async fn commit_marker(socket: &Path, marker: &str) {
    let mut connection = root_connection(socket).await;
    connection
        .query_drop("CREATE DATABASE IF NOT EXISTS kms_public_build")
        .await
        .unwrap();
    connection
        .query_drop(
            "CREATE TABLE IF NOT EXISTS kms_public_build.markers \
             (id BIGINT PRIMARY KEY AUTO_INCREMENT, marker VARCHAR(64) NOT NULL)",
        )
        .await
        .unwrap();
    connection
        .exec_drop(
            "INSERT INTO kms_public_build.markers(marker) VALUES (?)",
            (marker,),
        )
        .await
        .unwrap();
    connection.disconnect().await.unwrap();
}

async fn qualified_progress(driver: &MysqlPublicV1Replicator) -> i64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        match tokio::time::timeout_at(deadline, driver.current_progress()).await {
            Ok(Ok(progress)) => return progress,
            Ok(Err(RuntimeError::ReconfigurationPending)) => sleep(Duration::from_millis(20)).await,
            Ok(Err(error)) => panic!("qualified progress rejected authority/history: {error:?}"),
            Err(_) => {
                panic!("qualified progress did not become available before the fixed deadline")
            }
        }
    }
}

async fn root_connection(socket: &Path) -> Conn {
    let options = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(1)
        .socket(Some(socket.to_str().unwrap().to_owned()))
        .user(Some("root".to_owned()));
    Conn::new(options).await.unwrap()
}

async fn group_member_count(socket: &Path) -> usize {
    let mut connection = root_connection(socket).await;
    let count = connection
        .query_first::<u64, _>(
            "SELECT COUNT(*) FROM performance_schema.replication_group_members \
             WHERE MEMBER_ID IS NOT NULL AND MEMBER_ID <> ''",
        )
        .await
        .unwrap()
        .unwrap_or(0);
    connection.disconnect().await.unwrap();
    usize::try_from(count).unwrap()
}

async fn group_member_ids(socket: &Path) -> BTreeSet<String> {
    let mut connection = root_connection(socket).await;
    let members = connection
        .query_map(
            "SELECT MEMBER_ID FROM performance_schema.replication_group_members \
             ORDER BY MEMBER_ID",
            |member: String| member,
        )
        .await
        .unwrap()
        .into_iter()
        .collect();
    connection.disconnect().await.unwrap();
    members
}

async fn query_gtids(socket: &Path) -> GtidSet {
    let mut connection = root_connection(socket).await;
    let value = connection
        .query_first::<String, _>("SELECT @@GLOBAL.gtid_executed")
        .await
        .unwrap()
        .unwrap_or_default();
    connection.disconnect().await.unwrap();
    GtidSet::from_str(&value).unwrap()
}

async fn local_secondary_online(socket: &Path) -> bool {
    let mut connection = root_connection(socket).await;
    let row = connection
        .query_first::<(String, String), _>(
            "SELECT MEMBER_STATE, MEMBER_ROLE \
             FROM performance_schema.replication_group_members \
             WHERE MEMBER_ID = @@GLOBAL.server_uuid",
        )
        .await
        .unwrap();
    connection.disconnect().await.unwrap();
    row == Some(("ONLINE".to_owned(), "SECONDARY".to_owned()))
}

async fn assert_tcp_login_rejected(address: SocketAddr, username: &str, password: Option<&str>) {
    let options = OptsBuilder::default()
        .ip_or_hostname(address.ip().to_string())
        .tcp_port(address.port())
        .user(Some(username.to_owned()))
        .pass(password.map(str::to_owned));
    if let Ok(connection) = Conn::new(options).await {
        let _ = connection.disconnect().await;
        panic!("unexpected TCP authentication for {username} at {address}");
    }
}

async fn wait_for_build_accounts_removed(socket: &Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut connection = root_connection(socket).await;
        let count = connection
            .query_first::<u64, _>(
                "SELECT COUNT(*) FROM mysql.user \
                 WHERE User LIKE 'kms_build_%' AND Host = '127.0.0.1'",
            )
            .await
            .unwrap()
            .unwrap_or(0);
        connection.disconnect().await.unwrap();
        if count == 0 {
            return;
        }
        assert!(Instant::now() < deadline, "Build account cleanup timed out");
        sleep(Duration::from_millis(100)).await;
    }
}
