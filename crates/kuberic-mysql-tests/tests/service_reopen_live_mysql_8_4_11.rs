#[path = "service_live_support/mod.rs"]
mod live_support;

use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::time::{Duration, Instant};

use kuberic_mysql::core::{AttemptId, GtidSet, ServerUuid};
use kuberic_mysql::service::{
    AttemptContext, MysqlInstanceConfig, MysqlInstanceManager, MysqlMemberConfig, MysqlMemberIndex,
    MysqlOperationTimeouts, MysqlReopenAuthority, MysqlReopenBoundary, MysqlReopenError,
    MysqlReopenRenewal, MysqlStoppedReopen, MysqlTopologyConfig,
};
use kuberic_runtime::protocol::types::{
    AgentGeneration, ConfigurationId, Epoch, HistoryContext, HistoryId, OperationId, PartitionId,
    PodUid, ProcessSessionId, PublicProtocolGeneration, PvcUid, ReplicaId, ReplicaIdentity,
    ReplicaInstanceId, ReplicaStorageIdentity, ReplicaStorageIncarnationContext,
    ReplicatorCreationId, ResourceUid, StorageGeneration,
};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder};

use live_support::{
    APPARMOR_EXEC, FixtureRoot, MYSQL_X_PORT, MYSQLD, ReservedPorts, assert_no_application_store,
    assert_process_absent, assert_tcp_refuses, preflight_oracle_mysql_8_4_11,
};

fn attempt(name: &str) -> AttemptContext {
    AttemptContext::new(
        AttemptId::new(name).unwrap(),
        Instant::now() + Duration::from_secs(90),
        Duration::from_secs(15),
    )
    .unwrap()
}

fn authority() -> MysqlReopenAuthority {
    MysqlReopenAuthority {
        storage: ReplicaStorageIncarnationContext {
            replica: ReplicaIdentity {
                replica_id: ReplicaId::new(1),
                instance_id: ReplicaInstanceId::new("instance"),
                agent_generation: AgentGeneration::new("agent"),
            },
            storage: ReplicaStorageIdentity::Persistent {
                pod_uid: PodUid::new("pod"),
                pvc_uid: PvcUid::new("pvc"),
            },
            storage_generation: StorageGeneration::new("storage"),
        },
        partition: PartitionId::new("partition"),
        history: HistoryContext {
            resource_uid: ResourceUid::new("resource"),
            protocol_generation: PublicProtocolGeneration::new(1),
            history_id: HistoryId::new("history"),
            data_loss_number: 0,
        },
        process_session: ProcessSessionId::new("old-session"),
        replicator_creation: ReplicatorCreationId::new("old-creation"),
        epoch: Epoch::new(0, 1),
        configuration: ConfigurationId::new("old-config"),
        operation: OperationId::new("capture"),
        revision: 1,
    }
}

fn renewal() -> MysqlReopenRenewal {
    let predecessor = authority();
    let mut replacement = predecessor.clone();
    replacement.process_session = ProcessSessionId::new("new-session");
    replacement.replicator_creation = ReplicatorCreationId::new("new-creation");
    replacement.epoch = Epoch::new(0, 2);
    replacement.configuration = ConfigurationId::new("new-config");
    replacement.operation = OperationId::new("reopen");
    replacement.revision = 2;
    MysqlReopenRenewal {
        predecessor,
        replacement,
    }
}

async fn connect(socket: &std::path::Path) -> Conn {
    Conn::new(
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(1)
            .socket(Some(socket.to_str().unwrap().to_owned()))
            .user(Some("root".to_owned())),
    )
    .await
    .unwrap()
}

fn fresh(label: &str) -> (FixtureRoot, MysqlInstanceManager) {
    preflight_oracle_mysql_8_4_11().unwrap_or_else(|e| panic!("{e}"));
    let fixture = FixtureRoot::new(label).unwrap();
    let mut ports = ReservedPorts::allocate().unwrap();
    let sql = ports.sql_addresses();
    let group = ports.group_replication_addresses();
    let topology = MysqlTopologyConfig::new(std::array::from_fn(|i| {
        MysqlMemberConfig::new(
            u32::try_from(i + 1).unwrap(),
            sql[i],
            group[i],
            "cccccccc-cccc-cccc-cccc-cccccccccccc",
            group,
        )
        .unwrap()
    }))
    .unwrap();
    let config = MysqlInstanceConfig::new_topology_member(
        MYSQLD,
        APPARMOR_EXEC,
        fixture.path().join("d"),
        fixture.path().join("s"),
        topology,
        MysqlMemberIndex::First,
        MysqlOperationTimeouts::new(
            Duration::from_secs(90),
            Duration::from_secs(60),
            Duration::from_secs(30),
            Duration::from_secs(10),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(
        config
            .runtime()
            .socket()
            .as_os_str()
            .as_encoded_bytes()
            .len()
            < 108
    );
    ports.release();
    let mut manager = MysqlInstanceManager::new(config);
    manager.initialize().unwrap();
    manager.start().unwrap();
    (fixture, manager)
}

async fn prepared(label: &str) -> (FixtureRoot, MysqlInstanceManager, MysqlReopenBoundary) {
    let (fixture, manager) = fresh(label);
    let mut root = connect(manager.socket_path().unwrap()).await;
    for sql in [
        "CREATE DATABASE reopen_data",
        "CREATE TABLE reopen_data.proof (id INT PRIMARY KEY, value VARCHAR(32)) ENGINE=InnoDB",
        "INSERT INTO reopen_data.proof VALUES (1, 'retained')",
        "SET GLOBAL read_only=ON",
        "SET GLOBAL super_read_only=ON",
        "SET GLOBAL event_scheduler=OFF",
    ] {
        root.query_drop(sql).await.unwrap();
    }
    let (uuid, executed, purged): (String, String, String) = root
        .query_first("SELECT @@GLOBAL.server_uuid, @@GLOBAL.gtid_executed, @@GLOBAL.gtid_purged")
        .await
        .unwrap()
        .unwrap();
    root.disconnect().await.unwrap();
    let member = manager.config().member();
    let boundary = MysqlReopenBoundary {
        server_uuid: ServerUuid::new(uuid).unwrap(),
        group_name: member.group_uuid().to_owned(),
        server_id: member.server_id(),
        sql_address: member.sql_address(),
        group_address: member.group_replication_address().to_string(),
        executed: executed.parse().unwrap(),
        purged: purged.parse().unwrap(),
    };
    assert!(!boundary.executed.entries().is_empty());
    (fixture, manager, boundary)
}

fn pid(path: &std::path::Path) -> u32 {
    fs::read_to_string(path).unwrap().trim().parse().unwrap()
}

fn assert_closed(boundary: &MysqlReopenBoundary) {
    assert_tcp_refuses(boundary.sql_address).unwrap();
    assert_tcp_refuses(boundary.group_address.parse().unwrap()).unwrap();
    assert_tcp_refuses(format!("127.0.0.1:{MYSQL_X_PORT}").parse().unwrap()).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn closed_reopen_preserves_data_history_and_renews_owned_sessions() {
    let (mut fixture, manager, boundary) = prepared("reopen-ok").await;
    let socket = manager.config().runtime().socket().to_path_buf();
    let pid_path = manager.config().runtime().pid().to_path_buf();
    let old_pid = pid(&pid_path);
    let inode = fs::metadata(manager.config().data_root()).unwrap().ino();
    assert_closed(&boundary);
    // Native persisted settings must not override closed startup command flags.
    let mut root = connect(&socket).await;
    for sql in [
        "SET PERSIST_ONLY read_only=OFF",
        "SET PERSIST_ONLY super_read_only=OFF",
        "SET PERSIST_ONLY group_replication_start_on_boot=ON",
    ] {
        root.query_drop(sql).await.unwrap();
    }
    root.disconnect().await.unwrap();
    let stopped =
        MysqlStoppedReopen::capture(manager, authority(), boundary.clone(), &attempt("capture"))
            .await
            .unwrap();
    let predecessor = stopped.predecessor().clone();
    assert_process_absent(old_pid).unwrap();
    assert!(!socket.exists());
    assert_closed(&boundary);
    let mut reopened = stopped.reopen(renewal(), &attempt("reopen")).await.unwrap();
    let fresh = reopened
        .observe(&attempt("fresh-observation"))
        .await
        .unwrap();
    assert_eq!(fresh.boundary(), &boundary);
    assert_eq!(fresh.authority(), &renewal().replacement);
    assert_ne!(
        fresh.native_process_session(),
        predecessor.native_process_session()
    );
    assert_eq!(fresh.native_storage(), predecessor.native_storage());
    assert_eq!(fresh.attempt().as_str(), "fresh-observation");
    let new_pid = pid(&pid_path);
    assert_ne!(new_pid, old_pid);
    assert_eq!(fs::metadata(fixture.path().join("d")).unwrap().ino(), inode);
    assert_closed(&boundary);
    let mut root = connect(&socket).await;
    let value: String = root
        .query_first("SELECT value FROM reopen_data.proof WHERE id=1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, "retained");
    root.disconnect().await.unwrap();
    reopened.observe(&attempt("recheck")).await.unwrap();
    // An observed violation poisons this lifetime even after current settings
    // are restored; native history is never retroactively qualified.
    let mut root = connect(&socket).await;
    root.query_drop("SET GLOBAL sync_binlog=0").await.unwrap();
    root.disconnect().await.unwrap();
    assert!(matches!(
        reopened.observe(&attempt("bad-durability")).await,
        Err(MysqlReopenError::Durability),
    ));
    let mut root = connect(&socket).await;
    root.query_drop("SET GLOBAL sync_binlog=1").await.unwrap();
    root.disconnect().await.unwrap();
    assert!(matches!(
        reopened.observe(&attempt("restored-durability")).await,
        Err(MysqlReopenError::Durability),
    ));
    assert_no_application_store(fixture.path()).unwrap();
    reopened.close().unwrap();
    assert_process_absent(new_pid).unwrap();
    assert!(fixture.path().join("d").is_dir());
    assert!(!fixture.path().join("s").exists());
    fixture.remove().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn closed_reopen_rejects_stale_authority_and_never_adopts_foreign_survivor() {
    let (fixture, manager, boundary) = prepared("reopen-auth").await;
    let pid_path = manager.config().runtime().pid().to_path_buf();
    let owned = pid(&pid_path);
    let stopped =
        MysqlStoppedReopen::capture(manager, authority(), boundary.clone(), &attempt("capture"))
            .await
            .unwrap();
    assert_process_absent(owned).unwrap();
    let mut unrelated = std::process::Command::new("/usr/bin/sleep")
        .arg("60")
        .spawn()
        .unwrap();
    // An arbitrary PID file never gives ownership or permission to signal.
    fs::write(&pid_path, unrelated.id().to_string()).unwrap();
    let result = stopped.reopen(renewal(), &attempt("reopen")).await;
    assert!(matches!(result, Err(MysqlReopenError::Instance(_))));
    assert!(unrelated.try_wait().unwrap().is_none());
    unrelated.kill().unwrap();
    unrelated.wait().unwrap();
    assert!(fixture.path().join("d/auto.cnf").is_file());
    assert_closed(&boundary);

    // Independent generic preflight rejection before any replacement child.
    drop(fixture);
    let (fixture, manager, boundary) = prepared("reopen-stale").await;
    let pid_path = manager.config().runtime().pid().to_path_buf();
    let stopped = MysqlStoppedReopen::capture(manager, authority(), boundary, &attempt("capture"))
        .await
        .unwrap();
    let mut stale = renewal();
    stale.replacement.process_session = stale.predecessor.process_session.clone();
    assert!(matches!(
        stopped.reopen(stale, &attempt("reopen")).await,
        Err(MysqlReopenError::Authority)
    ));
    assert!(!pid_path.exists());
    assert!(fixture.path().join("d/auto.cnf").is_file());
}

#[tokio::test(flavor = "current_thread")]
async fn closed_capture_rejects_native_identity_history_durability_and_open_access() {
    for case in ["uuid", "history", "durability", "access", "session"] {
        let (fixture, manager, mut boundary) = prepared(case).await;
        let pid_path = manager.config().runtime().pid().to_path_buf();
        let owned = pid(&pid_path);
        let mut other = None;
        match case {
            "uuid" => {
                boundary.server_uuid =
                    ServerUuid::new("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap()
            }
            "history" => boundary.executed = GtidSet::empty(),
            "durability" | "access" => {
                let mut root = connect(manager.socket_path().unwrap()).await;
                root.query_drop(if case == "durability" {
                    "SET GLOBAL sync_binlog=0"
                } else {
                    "SET GLOBAL super_read_only=OFF"
                })
                .await
                .unwrap();
                root.disconnect().await.unwrap();
            }
            "session" => other = Some(connect(manager.socket_path().unwrap()).await),
            _ => unreachable!(),
        }
        let result =
            MysqlStoppedReopen::capture(manager, authority(), boundary, &attempt("capture")).await;
        match case {
            "uuid" => assert!(matches!(result, Err(MysqlReopenError::NativeIdentity))),
            "history" => assert!(matches!(result, Err(MysqlReopenError::History))),
            "durability" => assert!(matches!(result, Err(MysqlReopenError::Durability))),
            _ => assert!(matches!(result, Err(MysqlReopenError::AccessNotClosed))),
        }
        drop(other);
        assert_process_absent(owned).unwrap();
        assert!(fixture.path().join("d/auto.cnf").is_file());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn closed_reopen_rejects_substituted_storage_and_missing_native_logs() {
    for case in ["storage", "logs", "config", "native-history"] {
        let (fixture, manager, boundary) = prepared(case).await;
        let pid_path = manager.config().runtime().pid().to_path_buf();
        let stopped =
            MysqlStoppedReopen::capture(manager, authority(), boundary, &attempt("capture"))
                .await
                .unwrap();
        match case {
            "storage" => {
                fs::rename(fixture.path().join("d"), fixture.path().join("saved")).unwrap();
                fs::create_dir(fixture.path().join("d")).unwrap();
                fs::set_permissions(fixture.path().join("d"), fs::Permissions::from_mode(0o700))
                    .unwrap();
            }
            "logs" => fs::remove_file(fixture.path().join("s/mysql-bin.index")).unwrap(),
            "config" => fs::write(fixture.path().join("s/my.cnf"), "[mysqld]\n").unwrap(),
            "native-history" => {
                // Change the native UUID in its native auto.cnf, not app metadata.
                fs::write(
                    fixture.path().join("d/auto.cnf"),
                    "[auto]\nserver-uuid=bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\n",
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let result = stopped.reopen(renewal(), &attempt("reopen")).await;
        match case {
            "storage" | "config" => assert!(matches!(result, Err(MysqlReopenError::Instance(_)))),
            "logs" => assert!(matches!(result, Err(MysqlReopenError::NativeLogs))),
            _ => assert!(matches!(result, Err(MysqlReopenError::NativeIdentity))),
        }
        assert!(
            fixture
                .path()
                .join(if case == "storage" { "saved" } else { "d" })
                .is_dir()
        );
        if pid_path.exists() {
            assert_process_absent(pid(&pid_path)).unwrap();
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_and_dropped_reopen_owners_preserve_data_and_contain_only_owned_children() {
    let (fixture, manager, boundary) = prepared("reopen-drop").await;
    let pid_path = manager.config().runtime().pid().to_path_buf();
    let stopped = MysqlStoppedReopen::capture(manager, authority(), boundary, &attempt("capture"))
        .await
        .unwrap();
    let cancelled = attempt("cancelled");
    cancelled.cancel();
    assert!(matches!(
        stopped.reopen(renewal(), &cancelled).await,
        Err(MysqlReopenError::Attempt(_)),
    ));
    assert!(!pid_path.exists());
    assert!(fixture.path().join("d/auto.cnf").is_file());
    drop(fixture);

    let (fixture, manager, boundary) = prepared("reopen-drop2").await;
    let pid_path = manager.config().runtime().pid().to_path_buf();
    let stopped = MysqlStoppedReopen::capture(manager, authority(), boundary, &attempt("capture"))
        .await
        .unwrap();
    let reopened = stopped.reopen(renewal(), &attempt("reopen")).await.unwrap();
    let owned = pid(&pid_path);
    drop(reopened);
    assert_process_absent(owned).unwrap();
    assert!(fixture.path().join("d/auto.cnf").is_file());
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_capture_and_reopen_futures_reap_the_exact_owned_child() {
    use std::future::Future;
    use std::task::Poll;
    let (fixture, manager, boundary) = prepared("drop-capture").await;
    let owned = pid(manager.config().runtime().pid());
    let budget = attempt("drop-capture");
    let mut capture = Box::pin(MysqlStoppedReopen::capture(
        manager,
        authority(),
        boundary,
        &budget,
    ));
    std::future::poll_fn(|cx| {
        assert!(capture.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(capture);
    assert_process_absent(owned).unwrap();
    assert!(fixture.path().join("d/auto.cnf").is_file());
    drop(fixture);

    let (fixture, manager, boundary) = prepared("drop-reopen").await;
    let pid_path = manager.config().runtime().pid().to_path_buf();
    let stopped = MysqlStoppedReopen::capture(manager, authority(), boundary, &attempt("capture"))
        .await
        .unwrap();
    let budget = attempt("drop-reopen");
    let mut reopen = Box::pin(stopped.reopen(renewal(), &budget));
    std::future::poll_fn(|cx| {
        assert!(reopen.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let owned = pid(&pid_path);
    drop(reopen);
    assert_process_absent(owned).unwrap();
    assert!(fixture.path().join("d/auto.cnf").is_file());
}

#[tokio::test(flavor = "current_thread")]
async fn closed_capture_rejects_idle_published_and_unpublished_proxy_capabilities() {
    use kuberic_mysql::core::CredentialGeneration;
    use kuberic_mysql::service::{
        ApplicationCredential, ControlCredential, ControlCredentialRole, MysqlClientProxy,
    };
    for published in [false, true] {
        let (fixture, mut manager) = fresh(if published { "proxy-pub" } else { "proxy-prep" });
        let budget = attempt("proxy-owner");
        let observer = ControlCredential::new(
            budget.attempt().clone(),
            CredentialGeneration::new("observer").unwrap(),
            ControlCredentialRole::Observer,
            "reopen_observer",
            "ReopenObserverA7",
        )
        .unwrap();
        let recovery = ControlCredential::new(
            budget.attempt().clone(),
            CredentialGeneration::new("recovery").unwrap(),
            ControlCredentialRole::Recovery,
            "reopen_recovery",
            "ReopenRecoveryB9",
        )
        .unwrap();
        let native = manager
            .enroll_local_control_authority(&budget, &observer, &recovery)
            .await
            .unwrap();
        let reserved = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = reserved.local_addr().unwrap();
        drop(reserved);
        let credential = ApplicationCredential::new(
            budget.attempt().clone(),
            CredentialGeneration::new("application").unwrap(),
            "reopen_application",
            "ReopenApplicationC11",
        )
        .unwrap();
        let mut proxy =
            MysqlClientProxy::prepare(&mut manager, &native, address, credential, &budget)
                .await
                .unwrap();
        let mut root = connect(manager.socket_path().unwrap()).await;
        let (uuid, executed, purged): (String, String, String) = root
            .query_first(
                "SELECT @@GLOBAL.server_uuid, @@GLOBAL.gtid_executed, @@GLOBAL.gtid_purged",
            )
            .await
            .unwrap()
            .unwrap();
        for sql in [
            "SET GLOBAL read_only=ON",
            "SET GLOBAL super_read_only=ON",
            "SET GLOBAL event_scheduler=OFF",
        ] {
            root.query_drop(sql).await.unwrap();
        }
        root.disconnect().await.unwrap();
        if published {
            proxy
                .publish(std::num::NonZeroU64::new(1).unwrap(), true, true, &budget)
                .unwrap();
            let mut application = Conn::new(
                OptsBuilder::default()
                    .ip_or_hostname("127.0.0.1")
                    .tcp_port(address.port())
                    .prefer_socket(false)
                    .socket(None::<String>)
                    .user(Some("reopen_application"))
                    .pass(Some("ReopenApplicationC11")),
            )
            .await
            .unwrap();
            let value: u8 = application.query_first("SELECT 42").await.unwrap().unwrap();
            assert_eq!(value, 42);
            application.disconnect().await.unwrap();
            for _ in 0..100 {
                if proxy.snapshot().sessions() == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert_eq!(proxy.snapshot().sessions(), 0, "published path is idle");
        }
        let member = manager.config().member();
        let expected = MysqlReopenBoundary {
            server_uuid: ServerUuid::new(uuid).unwrap(),
            group_name: member.group_uuid().into(),
            server_id: member.server_id(),
            sql_address: member.sql_address(),
            group_address: member.group_replication_address().to_string(),
            executed: executed.parse().unwrap(),
            purged: purged.parse().unwrap(),
        };
        let owned = pid(manager.config().runtime().pid());
        let result =
            MysqlStoppedReopen::capture(manager, authority(), expected, &attempt("capture")).await;
        assert!(matches!(result, Err(MysqlReopenError::AccessNotClosed)));
        assert_process_absent(owned).unwrap();
        assert!(fixture.path().join("d/auto.cnf").is_file());
        assert!(fixture.path().join("s/mysql-bin.index").is_file());
        drop(proxy);
        tokio::task::yield_now().await;
    }
}
