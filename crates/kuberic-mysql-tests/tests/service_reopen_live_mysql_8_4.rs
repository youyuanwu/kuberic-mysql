#[path = "service_live_support/mod.rs"]
mod live_support;

use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::time::{Duration, Instant};

use kuberic_mysql::core::{AttemptId, GtidSet, ServerUuid};
use kuberic_mysql::service::{
    AttemptContext, MysqlInstanceConfig, MysqlInstanceManager, MysqlMemberConfig, MysqlMemberIndex,
    MysqlOperationTimeouts, MysqlProxyControlBoundary, MysqlReopenAuthority, MysqlReopenBoundary,
    MysqlReopenError, MysqlReopenRenewal, MysqlStoppedReopen, MysqlTopologyConfig,
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
    assert_process_absent, assert_tcp_refuses, preflight_oracle_mysql_8_4,
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
    preflight_oracle_mysql_8_4().unwrap_or_else(|e| panic!("{e}"));
    fresh_after_preflight(label)
}

// Multi-owner tests preflight once, before either owned native child exists.
fn fresh_after_preflight(label: &str) -> (FixtureRoot, MysqlInstanceManager) {
    fresh_profile_after_preflight(label, false)
}

fn fresh_profile_after_preflight(
    label: &str,
    build_tcp: bool,
) -> (FixtureRoot, MysqlInstanceManager) {
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
    let config = if build_tcp {
        use kuberic_mysql::core::{ConfiguredGtidLineage, CredentialGeneration};
        use kuberic_mysql::service::{
            ControlCredential, ControlCredentialRole, MysqlFreshMemberCredentials,
            MysqlPublicV1ServiceConfig,
        };
        let budget = attempt("build-profile");
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
        let credentials =
            MysqlFreshMemberCredentials::new(observer, recovery, proxy_credential(&budget));
        let service = MysqlPublicV1ServiceConfig::new(
            "partition",
            config,
            ConfiguredGtidLineage::new("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
            credentials,
            budget,
            std::num::NonZeroU64::MIN,
            "127.0.0.1:33070".parse().unwrap(),
        )
        .unwrap();
        service.instance().clone()
    } else {
        config
    };
    let mut manager = MysqlInstanceManager::new(config);
    manager.initialize().unwrap();
    manager.start().unwrap();
    (fixture, manager)
}

async fn enroll_proxy(
    manager: &mut MysqlInstanceManager,
    budget: &AttemptContext,
) -> kuberic_mysql::service::MemberMutationAuthority {
    use kuberic_mysql::core::CredentialGeneration;
    use kuberic_mysql::service::{ControlCredential, ControlCredentialRole};
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
    manager
        .enroll_local_control_authority(budget, &observer, &recovery)
        .await
        .unwrap()
}

fn proxy_credential(budget: &AttemptContext) -> kuberic_mysql::service::ApplicationCredential {
    kuberic_mysql::service::ApplicationCredential::new(
        budget.attempt().clone(),
        kuberic_mysql::core::CredentialGeneration::new("application").unwrap(),
        "reopen_application",
        "ReopenApplicationC11",
    )
    .unwrap()
}

async fn prepare_owned_proxy(
    manager: &mut MysqlInstanceManager,
    budget: &AttemptContext,
) -> (
    kuberic_mysql::service::MemberMutationAuthority,
    kuberic_mysql::service::MysqlClientProxy,
) {
    let native = enroll_proxy(manager, budget).await;
    let proxy = manager
        .prepare_reopenable_proxy(
            &native,
            "127.0.0.1:0".parse().unwrap(),
            proxy_credential(budget),
            budget,
        )
        .await
        .unwrap();
    assert!(
        manager
            .prepare_reopenable_proxy(
                &native,
                "127.0.0.1:0".parse().unwrap(),
                proxy_credential(budget),
                budget,
            )
            .await
            .is_err(),
        "singleton issuance must reject a second preparation"
    );
    (native, proxy)
}

async fn application(address: std::net::SocketAddr) -> Conn {
    Conn::new(
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(address.port())
            .prefer_socket(false)
            .socket(None::<String>)
            .user(Some("reopen_application"))
            .pass(Some("ReopenApplicationC11")),
    )
    .await
    .unwrap()
}

async fn close_native_for_capture(manager: &MysqlInstanceManager) {
    let mut root = connect(manager.socket_path().unwrap()).await;
    for sql in [
        "SET GLOBAL read_only=ON",
        "SET GLOBAL super_read_only=ON",
        "SET GLOBAL event_scheduler=OFF",
    ] {
        root.query_drop(sql).await.unwrap();
    }
    root.disconnect().await.unwrap();
}

async fn closed_boundary(manager: &MysqlInstanceManager) -> MysqlReopenBoundary {
    let mut root = connect(manager.socket_path().unwrap()).await;
    let (uuid, executed, purged): (String, String, String) = root
        .query_first("SELECT @@GLOBAL.server_uuid, @@GLOBAL.gtid_executed, @@GLOBAL.gtid_purged")
        .await
        .unwrap()
        .unwrap();
    root.disconnect().await.unwrap();
    let member = manager.config().member();
    MysqlReopenBoundary {
        server_uuid: ServerUuid::new(uuid).unwrap(),
        group_name: member.group_uuid().into(),
        server_id: member.server_id(),
        sql_address: member.sql_address(),
        group_address: member.group_replication_address().to_string(),
        executed: executed.parse().unwrap(),
        purged: purged.parse().unwrap(),
    }
}

async fn wait_proxy(
    proxy: &kuberic_mysql::service::MysqlClientProxy,
    predicate: impl Fn(&kuberic_mysql::service::MysqlProxySnapshot) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !predicate(&proxy.snapshot()) {
        assert!(
            Instant::now() < deadline,
            "proxy checkpoint not reached: {:?}",
            proxy.snapshot()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn assert_native_proxy_retired(socket: &std::path::Path, ids: &[u64]) {
    let mut root = connect(socket).await;
    for id in ids {
        let count: u64 = root
            .query_first(format!(
                "SELECT COUNT(*) FROM performance_schema.threads WHERE PROCESSLIST_ID={id}",
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(count, 0, "exact retired native connection {id}");
    }
    let users: u64 = root
        .query_first("SELECT COUNT(*) FROM mysql.user WHERE User='reopen_application'")
        .await
        .unwrap()
        .unwrap();
    let sessions: u64 = root.query_first(
        "SELECT COUNT(*) FROM performance_schema.threads WHERE PROCESSLIST_USER='reopen_application'",
    ).await.unwrap().unwrap();
    assert_eq!((users, sessions), (0, 0));
    root.disconnect().await.unwrap();
}

async fn wait_native_sleep(socket: &std::path::Path, connection: u64) {
    let mut root = connect(socket).await;
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let count: u64 = root
            .query_first(format!(
                "SELECT COUNT(*) FROM performance_schema.threads \
             WHERE PROCESSLIST_ID={connection} AND PROCESSLIST_INFO='SELECT SLEEP(60)'",
            ))
            .await
            .unwrap()
            .unwrap();
        if count == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "exact backend {connection} never began native SLEEP"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    root.disconnect().await.unwrap();
}

struct ReopenAdmissionGate {
    block: std::sync::atomic::AtomicBool,
    started: tokio::sync::Notify,
}
impl kuberic_mysql::service::MysqlProxyAccessGuard for ReopenAdmissionGate {
    fn access_granted(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>> {
        Box::pin(async {
            if self.block.load(std::sync::atomic::Ordering::Acquire) {
                self.started.notify_one();
                std::future::pending::<bool>().await
            } else {
                true
            }
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn proxy_terminal_proof_rejects_wrong_manager_and_frozen_incarnation_session() {
    preflight_oracle_mysql_8_4().unwrap_or_else(|e| panic!("{e}"));
    let (mut owner_root, mut owner) = fresh_after_preflight("ph-owner");
    let (mut other_root, other) = fresh_after_preflight("ph-other");
    let budget = attempt("proxy-proof-owner");
    let (native, mut proxy) = prepare_owned_proxy(&mut owner, &budget).await;
    let expected = closed_boundary(&owner).await;
    let (owner, proof) = proxy
        .close_for_reopen(owner, authority(), &budget)
        .await
        .unwrap();
    let owner_pid = pid(owner.config().runtime().pid());
    let other_pid = pid(other.config().runtime().pid());
    let native_socket = owner.socket_path().unwrap().to_path_buf();
    assert!(matches!(
        MysqlStoppedReopen::capture_with_proxy_closure(
            other,
            proof,
            authority(),
            expected,
            &attempt("wrong-manager"),
        )
        .await,
        Err(MysqlReopenError::Ownership)
    ));
    assert_process_absent(other_pid).unwrap();
    assert_eq!(pid(owner.config().runtime().pid()), owner_pid);
    let mut connection = connect(&native_socket).await;
    let _: u8 = connection.query_first("SELECT 1").await.unwrap().unwrap();
    connection.disconnect().await.unwrap();
    // Wrong-manager consumption did not grant native effects against the
    // rightful proxy owner; that owner remains exact and must be retired itself.
    let mut owner = owner;
    assert!(owner.proxy_backend_socket(&native).is_err());
    owner.stop().unwrap();
    drop(proxy);
    owner_root.remove().unwrap();
    other_root.remove().unwrap();

    for dimension in ["storage", "instance", "session", "history"] {
        let (mut fixture, mut manager) = fresh(&format!("ph-{dimension}"));
        let budget = attempt("proxy-proof-scope");
        let (_, mut proxy) = prepare_owned_proxy(&mut manager, &budget).await;
        let expected = closed_boundary(&manager).await;
        let owned = pid(manager.config().runtime().pid());
        let (manager, proof) = proxy
            .close_for_reopen(manager, authority(), &budget)
            .await
            .unwrap();
        close_native_for_capture(&manager).await;
        let mut substituted = authority();
        match dimension {
            "storage" => substituted.storage.storage_generation = StorageGeneration::new("other"),
            "instance" => substituted.storage.replica.instance_id = ReplicaInstanceId::new("other"),
            "session" => substituted.process_session = ProcessSessionId::new("other"),
            "history" => substituted.history.history_id = HistoryId::new("other"),
            _ => unreachable!(),
        }
        assert!(matches!(
            MysqlStoppedReopen::capture_with_proxy_closure(
                manager,
                proof,
                substituted,
                expected,
                &attempt("substituted-proof"),
            )
            .await,
            Err(MysqlReopenError::Authority)
        ));
        assert_process_absent(owned).unwrap();
        assert_tcp_refuses(proxy.address()).unwrap();
        drop(proxy);
        fixture.remove().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn proxy_terminal_surrender_cancellation_and_unpolled_pending_drop_contain_exact_child() {
    use std::future::Future;
    for cut in ["unpolled", "pending", "cancelled"] {
        preflight_oracle_mysql_8_4().unwrap_or_else(|e| panic!("{e}"));
        let (mut fixture, mut manager) = fresh_after_preflight(&format!("ph-drop-{cut}"));
        let (mut foreign_fixture, mut foreign) = fresh_after_preflight("ph-retained");
        let foreign_pid = pid(foreign.config().runtime().pid());
        let budget = attempt("proxy-drop");
        let (_, mut proxy) = prepare_owned_proxy(&mut manager, &budget).await;
        proxy
            .publish(std::num::NonZeroU64::MIN, true, true, &budget)
            .unwrap();
        let mut blocker = application(proxy.address()).await;
        let blocker_id = u64::from(blocker.id());
        let work = tokio::spawn(async move {
            blocker
                .query_drop("SELECT SLEEP(60)")
                .await
                .map_err(|e| e.to_string())
        });
        wait_proxy(&proxy, |s| s.active_commands() > 0).await;
        wait_native_sleep(manager.socket_path().unwrap(), blocker_id).await;
        let owned = pid(manager.config().runtime().pid());
        let address = proxy.address();
        if cut == "cancelled" {
            budget.cancel();
            assert!(matches!(
                proxy.close_for_reopen(manager, authority(), &budget).await,
                Err(MysqlReopenError::Attempt(_))
            ));
        } else {
            let mut future = Box::pin(proxy.close_for_reopen(manager, authority(), &budget));
            if cut == "pending" {
                let pending = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(future.as_mut().poll(cx).is_pending())
                })
                .await;
                assert!(
                    pending,
                    "active task join/native settlement must reach a pending checkpoint"
                );
            }
            drop(future);
        }
        assert_process_absent(owned).unwrap();
        assert_tcp_refuses(address).unwrap();
        assert!(
            proxy
                .publish(
                    std::num::NonZeroU64::new(2).unwrap(),
                    true,
                    true,
                    &attempt("stale-drop")
                )
                .is_err()
        );
        assert!(proxy.rearm(address, &attempt("stale-rearm")).await.is_err());
        assert!(fixture.path().join("d/auto.cnf").is_file());
        assert!(fixture.path().join("s/mysql-bin.index").is_file());
        assert_eq!(pid(foreign.config().runtime().pid()), foreign_pid);
        let mut root = connect(foreign.socket_path().unwrap()).await;
        let _: u8 = root.query_first("SELECT 1").await.unwrap().unwrap();
        root.disconnect().await.unwrap();
        assert!(work.await.unwrap().is_err());
        drop(proxy);
        foreign.stop().unwrap();
        fixture.remove().unwrap();
        foreign_fixture.remove().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn proxy_terminal_no_proof_raw_issuance_and_changed_history_remain_rejected() {
    for path in [
        "no-proof",
        "raw-socket",
        "raw-account",
        "raw-cancelled",
        "changed-history",
    ] {
        let (mut fixture, mut manager) = fresh(&format!("ph-rej-{path}"));
        let budget = attempt("proxy-rejection");
        let (native, mut proxy) = prepare_owned_proxy(&mut manager, &budget).await;
        let mut expected = closed_boundary(&manager).await;
        let owned = pid(manager.config().runtime().pid());
        if path.starts_with("raw") {
            if path == "raw-socket" {
                let _escaped = manager.proxy_backend_socket(&native).unwrap();
            } else if path == "raw-cancelled" {
                // A rejected/cancelled direct issuance still poisons provenance
                // before effects; it cannot silently disappear from the ledger.
                let cancelled = attempt("direct-cancelled");
                cancelled.cancel();
                assert!(
                    manager
                        .prepare_application_account(
                            &native,
                            &proxy_credential(&budget),
                            &cancelled
                        )
                        .await
                        .is_err()
                );
            } else {
                let escaped = kuberic_mysql::service::ApplicationCredential::new(
                    budget.attempt().clone(),
                    kuberic_mysql::core::CredentialGeneration::new("escaped").unwrap(),
                    "escaped_application",
                    "EscapedApplicationD13",
                )
                .unwrap();
                manager
                    .prepare_application_account(&native, &escaped, &budget)
                    .await
                    .unwrap();
            }
            assert!(matches!(
                proxy.close_for_reopen(manager, authority(), &budget).await,
                Err(MysqlReopenError::Ownership)
            ));
            drop(proxy);
        } else {
            let (manager, proof) = proxy
                .close_for_reopen(manager, authority(), &budget)
                .await
                .unwrap();
            close_native_for_capture(&manager).await;
            if path == "no-proof" {
                drop(proof);
                assert!(matches!(
                    MysqlStoppedReopen::capture(
                        manager,
                        authority(),
                        expected,
                        &attempt("no-proof"),
                    )
                    .await,
                    Err(MysqlReopenError::AccessNotClosed)
                ));
            } else {
                expected.executed = "dddddddd-dddd-dddd-dddd-dddddddddddd:1".parse().unwrap();
                assert!(matches!(
                    MysqlStoppedReopen::capture_with_proxy_closure(
                        manager,
                        proof,
                        authority(),
                        expected,
                        &attempt("history-mismatch"),
                    )
                    .await,
                    Err(MysqlReopenError::History)
                ));
            }
            drop(proxy);
        }
        assert_process_absent(owned).unwrap();
        assert!(fixture.path().join("d/auto.cnf").is_file());
        assert!(fixture.path().join("s/mysql-bin.index").is_file());
        fixture.remove().unwrap();
    }
}
#[tokio::test(flavor = "current_thread")]
async fn proxy_terminal_handoff_reopens_published_unpublished_revoked_and_rearmed_closed() {
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    for mode in 0..4 {
        let (mut fixture, mut manager) = fresh(&format!("ph-ok-{mode}"));
        let budget = attempt("proxy-handoff");
        let (native, mut proxy) = prepare_owned_proxy(&mut manager, &budget).await;
        let gate = Arc::new(ReopenAdmissionGate {
            block: AtomicBool::new(false),
            started: tokio::sync::Notify::new(),
        });
        proxy.bind_access_guard(gate.clone()).unwrap();
        let socket = manager.socket_path().unwrap().to_path_buf();
        let old_pid = pid(manager.config().runtime().pid());
        let inode = fs::metadata(manager.config().data_root()).unwrap().ino();
        let mut root = connect(&socket).await;
        root.query_drop("CREATE DATABASE kuberic_proxy_qualification")
            .await
            .unwrap();
        root.query_drop("CREATE TABLE kuberic_proxy_qualification.proof (id INT PRIMARY KEY, value INT) ENGINE=InnoDB").await.unwrap();
        root.query_drop("INSERT INTO kuberic_proxy_qualification.proof VALUES (1, 1)")
            .await
            .unwrap();
        root.disconnect().await.unwrap();
        if mode != 0 {
            proxy.publish(NonZeroU64::MIN, true, true, &budget).unwrap();
            let mut app = application(proxy.address()).await;
            app.query_drop("UPDATE kuberic_proxy_qualification.proof SET value=42 WHERE id=1")
                .await
                .unwrap();
            app.disconnect().await.unwrap();
            wait_proxy(&proxy, |s| s.sessions() == 0).await;
            if mode >= 2 {
                let receipt = manager
                    .revoke_reopenable_proxy(&mut proxy, NonZeroU64::MIN, &budget)
                    .await
                    .unwrap();
                assert!(receipt.handoff_eligible());
                if mode == 3 {
                    proxy
                        .rearm("127.0.0.1:0".parse().unwrap(), &budget)
                        .await
                        .unwrap();
                    proxy
                        .publish(NonZeroU64::new(2).unwrap(), true, true, &budget)
                        .unwrap();
                }
            }
        }
        let mut idle = None;
        let mut transaction = None;
        let mut active = None;
        let mut ids = vec![];
        if mode == 1 {
            let connection = application(proxy.address()).await;
            ids.push(u64::from(connection.id()));
            idle = Some(connection);
            let mut tx = application(proxy.address()).await;
            ids.push(u64::from(tx.id()));
            tx.query_drop("START TRANSACTION").await.unwrap();
            let _: u8 = tx.query_first("SELECT 1").await.unwrap().unwrap();
            transaction = Some(tx);
            let mut blocker = application(proxy.address()).await;
            ids.push(u64::from(blocker.id()));
            active = Some(tokio::spawn(async move {
                blocker
                    .query_drop("SELECT SLEEP(60)")
                    .await
                    .map_err(|e| e.to_string())
            }));
            wait_proxy(&proxy, |s| {
                s.active_commands() > 0 && s.backend_connections().len() == 3
            })
            .await;
            wait_native_sleep(&socket, *ids.last().unwrap()).await;
        }
        let expected = closed_boundary(&manager).await;
        assert!(!expected.executed.entries().is_empty());
        let address = proxy.address();
        // In mode 1 a real accepted connection is poised before registration.
        // The gate gives a deterministic admission/closure race without
        // guessing whether an unidentified native connect already happened.
        let racing_client = if mode == 1 {
            gate.block.store(true, Ordering::Release);
            let client = tokio::net::TcpStream::connect(address).await.unwrap();
            tokio::time::timeout(Duration::from_secs(2), gate.started.notified())
                .await
                .unwrap();
            Some(client)
        } else {
            None
        };
        let shared = Arc::new(tokio::sync::Mutex::new(proxy));
        let stale = Arc::clone(&shared);
        let contender = Arc::clone(&shared);
        let contender_budget = budget.clone();
        let mut guard = shared.lock().await;
        let pending_publish = tokio::spawn(async move {
            contender
                .lock()
                .await
                .publish(NonZeroU64::new(99).unwrap(), true, true, &contender_budget)
                .is_err()
        });
        let (mut manager, closure) = guard
            .close_for_reopen(manager, authority(), &budget)
            .await
            .unwrap();
        assert!(!guard.account_active());
        assert!(guard.credential_cleared());
        assert!(!guard.snapshot().accepting());
        assert_eq!(guard.snapshot().sessions(), 0);
        drop(guard);
        assert!(
            pending_publish.await.unwrap(),
            "racing publication cannot reopen terminal ingress"
        );
        drop(racing_client);
        if let Some(active) = active {
            assert!(active.await.unwrap().is_err());
        }
        drop(idle);
        drop(transaction);
        assert_native_proxy_retired(&socket, &ids).await;
        assert_tcp_refuses(address).unwrap();
        // Native closure is separate: retire the account before enabling
        // super-read-only. Surrender never changes native role/settings.
        close_native_for_capture(&manager).await;
        assert_eq!(closed_boundary(&manager).await, expected);
        assert!(
            manager.proxy_backend_socket(&native).is_err(),
            "post-proof raw issuance is sealed"
        );
        assert!(
            manager
                .prepare_reopenable_proxy(
                    &native,
                    "127.0.0.1:0".parse().unwrap(),
                    proxy_credential(&budget),
                    &budget
                )
                .await
                .is_err()
        );
        let stopped = MysqlStoppedReopen::capture_with_proxy_closure(
            manager,
            closure,
            authority(),
            expected.clone(),
            &attempt("proxy-capture"),
        )
        .await
        .unwrap();
        let predecessor = stopped.predecessor().clone();
        assert_process_absent(old_pid).unwrap();
        assert_tcp_refuses(address).unwrap();
        let mut reopened = stopped
            .reopen(renewal(), &attempt("proxy-reopen"))
            .await
            .unwrap();
        let observed = reopened.observe(&attempt("proxy-observe")).await.unwrap();
        assert_eq!(observed.boundary(), &expected);
        assert_ne!(
            observed.native_process_session(),
            predecessor.native_process_session()
        );
        assert_eq!(observed.native_storage(), predecessor.native_storage());
        assert_eq!(fs::metadata(fixture.path().join("d")).unwrap().ino(), inode);
        assert_closed(&expected);
        assert_tcp_refuses(address).unwrap();
        let mut stale = stale.lock().await;
        assert!(
            stale
                .publish(
                    NonZeroU64::new(3).unwrap(),
                    true,
                    true,
                    &attempt("fresh-publish")
                )
                .is_err()
        );
        assert!(stale.rearm(address, &budget).await.is_err());
        assert!(stale.bind_access_guard(gate.clone()).is_err());
        let mut root = connect(&socket).await;
        let value: u32 = root
            .query_first("SELECT value FROM kuberic_proxy_qualification.proof WHERE id=1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value, if mode == 0 { 1 } else { 42 });
        root.disconnect().await.unwrap();
        reopened.observe(&attempt("proxy-reobserve")).await.unwrap();
        // The acknowledged workload intentionally creates the native SQL
        // schema named kuberic_proxy_qualification. The generic filename
        // heuristic forbids that word and is not valid for this data directory.
        // Still inspect all runtime/scratch files and the exact native table.
        assert_no_application_store(&fixture.path().join("s")).unwrap();
        let schema_entries = fs::read_dir(fixture.path().join("d/kuberic_proxy_qualification"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(schema_entries, [std::ffi::OsString::from("proof.ibd")]);
        reopened.close().unwrap();
        drop(stale);
        drop(shared);
        fixture.remove().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn proxy_terminal_build_tcp_profile_cannot_issue_proxy_or_reopen_proof() {
    preflight_oracle_mysql_8_4().unwrap_or_else(|e| panic!("{e}"));
    let (mut fixture, mut manager) = fresh_profile_after_preflight("ph-build", true);
    let budget = attempt("build-rejection");
    let native = enroll_proxy(&mut manager, &budget).await;
    assert!(manager.config().build_recovery_networking());
    let address = manager.config().member().sql_address();
    let tcp = tokio::net::TcpStream::connect(address).await.unwrap();
    drop(tcp);
    assert!(
        manager
            .prepare_reopenable_proxy(
                &native,
                "127.0.0.1:0".parse().unwrap(),
                proxy_credential(&budget),
                &budget,
            )
            .await
            .is_err()
    );
    close_native_for_capture(&manager).await;
    let expected = closed_boundary(&manager).await;
    let owned = pid(manager.config().runtime().pid());
    assert!(matches!(
        MysqlStoppedReopen::capture(manager, authority(), expected, &attempt("build-capture"),)
            .await,
        Err(MysqlReopenError::AccessNotClosed)
    ));
    assert_process_absent(owned).unwrap();
    assert_tcp_refuses(address).unwrap();
    assert!(fixture.path().join("d/auto.cnf").is_file());
    fixture.remove().unwrap();
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
    let retained_error = fs::read(fixture.path().join("s/mysqld.err")).unwrap();
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
    let error_log: String = root
        .query_first("SELECT @@GLOBAL.log_error")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        error_log,
        fixture.path().join("s/mysqld-reopen.err").to_str().unwrap()
    );
    assert_eq!(
        fs::read(fixture.path().join("s/mysqld.err")).unwrap(),
        retained_error
    );
    assert!(
        !fs::read(fixture.path().join("s/mysqld-reopen.err"))
            .unwrap()
            .is_empty()
    );
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

#[tokio::test(flavor = "current_thread")]
async fn closed_reopen_rejects_diagnostic_symlinks_and_hardlinks_without_data_loss() {
    for name in [
        "mysqld.out",
        "mysqld.err",
        "mysqld-reopen.out",
        "mysqld-reopen.err",
    ] {
        for hardlink in [false, true] {
            let (fixture, manager, boundary) = prepared("reopen-links").await;
            let sentinel = fixture.path().join("d/retained-sentinel");
            fs::write(&sentinel, b"retained-data-must-not-be-truncated").unwrap();
            let pid_path = manager.config().runtime().pid().to_path_buf();
            let stopped =
                MysqlStoppedReopen::capture(manager, authority(), boundary, &attempt("capture"))
                    .await
                    .unwrap();
            let index_path = fixture.path().join("s/mysql-bin.index");
            let index = fs::read(&index_path).unwrap();
            let destination = fixture.path().join("s").join(name);
            if destination.exists() {
                fs::remove_file(&destination).unwrap();
            }
            if hardlink {
                fs::hard_link(&sentinel, &destination).unwrap();
            } else {
                std::os::unix::fs::symlink(&sentinel, &destination).unwrap();
            }
            let result = stopped.reopen(renewal(), &attempt("reopen")).await;
            if name.starts_with("mysqld-reopen") {
                assert!(matches!(result, Err(MysqlReopenError::Instance(_))));
            } else {
                assert!(matches!(result, Err(MysqlReopenError::NativeLogs)));
            }
            assert_eq!(
                fs::read(&sentinel).unwrap(),
                b"retained-data-must-not-be-truncated"
            );
            assert_eq!(fs::read(&index_path).unwrap(), index);
            assert!(
                !pid_path.exists(),
                "no replacement process was credited or launched"
            );
            assert!(fixture.path().join("d/auto.cnf").is_file());
        }
    }
}
