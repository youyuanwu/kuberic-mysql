#[path = "service_live_support/mod.rs"]
mod live_support;

use std::collections::BTreeSet;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::num::NonZeroU64;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
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

fn run_live_fixture(interrupt_second: bool, omit_source_progress: bool) {
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
                ));
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn run_custom_build_fixture(interrupt_second: bool, omit_source_progress: bool) {
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
    drivers[0].update_epoch(epoch).await.unwrap();
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

    for target in 1..3 {
        drivers[target].update_epoch(epoch).await.unwrap();
        drivers[target]
            .change_role(epoch, ReplicaRole::IdleSecondary)
            .await
            .unwrap();

        let socket = instance_socket(&root, 0);
        commit_marker(&socket, &format!("build_marker_{target}")).await;
        assert!(drivers[0].current_progress().await.unwrap() > 0);
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
        // A terminal exact retry is served from the retained complete
        // binding, but only after fresh Primary/authority/boundary proof.
        Box::pin(drivers[0].build_replica(build)).await.unwrap();
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
        }
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
            drivers[index]
                .current_progress()
                .await
                .unwrap_or_else(|error| {
                    panic!("progress for member {index} while local is {local}: {error:?}")
                }),
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
        .query_first::<u64, _>("SELECT COUNT(*) FROM performance_schema.replication_group_members")
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
