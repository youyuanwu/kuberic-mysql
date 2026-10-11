#[path = "service_live_support/mod.rs"]
mod live_support;

use std::collections::HashSet;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::num::NonZeroU64;
use std::path::Path;
use std::time::{Duration, Instant};

use kuberic_mysql::core::{AttemptId, CredentialGeneration};
use kuberic_mysql::service::{
    ApplicationCredential, AttemptContext, ControlCredential, ControlCredentialRole,
    MysqlClientProxy, MysqlInstanceConfig, MysqlInstanceManager, MysqlMemberConfig,
    MysqlMemberIndex, MysqlOperationTimeouts, MysqlTopologyConfig,
};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Pool};
use tokio::time::sleep;

use live_support::{
    APPARMOR_EXEC, FixtureRoot, MYSQLD, ReservedPorts, assert_process_absent, assert_tcp_refuses,
    preflight_oracle_mysql_8_4_11,
};

const GROUP_UUID: &str = "a4d98271-1959-45da-91c4-ff9a85b1d430";

#[tokio::test(flavor = "current_thread")]
async fn one_member_proxy_matches_oracle_mysql_8_4_11() {
    preflight_oracle_mysql_8_4_11().unwrap_or_else(|error| panic!("{error}"));
    let mut fixture =
        FixtureRoot::new("service-proxy-live").unwrap_or_else(|error| panic!("{error}"));
    let root = fixture.path().to_path_buf();
    let mut reservations = ReservedPorts::allocate().unwrap_or_else(|error| panic!("{error}"));
    let topology = topology(
        reservations.sql_addresses(),
        reservations.group_replication_addresses(),
    );
    let timeouts = MysqlOperationTimeouts::new(
        Duration::from_secs(120),
        Duration::from_secs(60),
        Duration::from_secs(30),
        Duration::from_secs(10),
    )
    .unwrap();
    let config = MysqlInstanceConfig::new_topology_member(
        MYSQLD,
        APPARMOR_EXEC,
        root.join("member-data"),
        root.join("member-scratch"),
        topology,
        MysqlMemberIndex::First,
        timeouts,
    )
    .unwrap();
    let socket = config.runtime().socket().to_owned();
    let pid_file = config.runtime().pid().to_owned();
    let sql_addresses = reservations.sql_addresses();
    let mut manager = MysqlInstanceManager::new(config);
    reservations.release();
    manager.initialize().unwrap();
    manager.start().unwrap();
    let pid: u32 = fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    let attempt = AttemptContext::new(
        AttemptId::new(format!("proxy-live-{}", std::process::id())).unwrap(),
        Instant::now() + Duration::from_secs(180),
        Duration::from_secs(40),
    )
    .unwrap();
    let observer = ControlCredential::new(
        attempt.attempt().clone(),
        CredentialGeneration::new("proxy-observer-generation").unwrap(),
        ControlCredentialRole::Observer,
        format!("proxy_obs_{}", std::process::id()),
        format!("ProxyObserver{}A7", std::process::id()),
    )
    .unwrap();
    let recovery = ControlCredential::new(
        attempt.attempt().clone(),
        CredentialGeneration::new("proxy-recovery-generation").unwrap(),
        ControlCredentialRole::Recovery,
        format!("proxy_rec_{}", std::process::id()),
        format!("ProxyRecovery{}B9", std::process::id()),
    )
    .unwrap();
    let authority = manager
        .enroll_local_control_authority(&attempt, &observer, &recovery)
        .await
        .unwrap();

    let before_account = query_gtid(&socket).await;
    let application_username = format!("proxy_app_{}", std::process::id());
    let application_password = format!("ProxyApplication{}C11", std::process::id());
    let credential = ApplicationCredential::new(
        attempt.attempt().clone(),
        CredentialGeneration::new("proxy-application-generation").unwrap(),
        application_username.clone(),
        application_password.clone(),
    )
    .unwrap();
    let reservation = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let proxy_address = reservation.local_addr().unwrap();
    drop(reservation);
    let mut proxy = MysqlClientProxy::prepare(
        &mut manager,
        &authority,
        proxy_address,
        credential,
        &attempt,
    )
    .await
    .unwrap();
    assert_eq!(proxy.address(), proxy_address);
    assert_account_profile(&socket, &application_username).await;
    create_qualification_schema(&socket).await;
    assert_eq!(query_gtid(&socket).await, before_account);

    proxy.publish(nonzero(1), true, true, &attempt).unwrap();
    let options = tcp_options(proxy_address, &application_username, &application_password);
    let mut connection = Conn::new(options.clone()).await.unwrap();
    let connection_id = u64::from(connection.id());
    await_registered(&proxy, connection_id).await;
    connection
        .query_drop(
            "INSERT INTO kuberic_proxy_qualification.tokens(token_name, token_value) \
             VALUES ('autocommit', 1)",
        )
        .await
        .unwrap();
    connection.query_drop("START TRANSACTION").await.unwrap();
    connection
        .query_drop(
            "UPDATE kuberic_proxy_qualification.tokens SET token_value = 2 \
             WHERE token_name = 'autocommit'",
        )
        .await
        .unwrap();
    connection.query_drop("COMMIT").await.unwrap();
    let value: u64 = connection
        .query_first(
            "SELECT token_value FROM kuberic_proxy_qualification.tokens \
             WHERE token_name = 'autocommit'",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, 2);
    connection.ping().await.unwrap();
    assert_all_application_sessions_registered(&socket, &application_username, &proxy).await;
    connection.disconnect().await.unwrap();
    await_sessions(&proxy, 0).await;
    await_native_absent(&socket, connection_id).await;

    let pool = Pool::new(options.clone());
    let mut pooled = pool.get_conn().await.unwrap();
    let pooled_id = u64::from(pooled.id());
    await_registered(&proxy, pooled_id).await;
    let _: u8 = pooled.query_first("SELECT 1").await.unwrap().unwrap();
    drop(pooled);
    pool.disconnect().await.unwrap();
    await_sessions(&proxy, 0).await;
    await_native_absent(&socket, pooled_id).await;

    let root_options = OptsBuilder::default()
        .ip_or_hostname(proxy_address.ip().to_string())
        .tcp_port(proxy_address.port())
        .prefer_socket(false)
        .socket(None::<String>)
        .user(Some("root".to_owned()));
    assert!(Conn::new(root_options).await.is_err());

    let mut blocker = Conn::new(options).await.unwrap();
    let blocker_id = u64::from(blocker.id());
    await_registered(&proxy, blocker_id).await;
    let workload = tokio::spawn(async move {
        blocker
            .query_drop("SELECT SLEEP(60)")
            .await
            .map_err(|error| error.to_string())
    });
    await_active(&proxy).await;
    let receipt = proxy
        .revoke(&mut manager, &authority, nonzero(1), &attempt)
        .await
        .unwrap();
    assert!(receipt.sessions_zero());
    assert!(receipt.commands_zero());
    assert!(receipt.native_absence());
    assert!(receipt.cleanup_complete());
    assert!(receipt.cancelled_commands());
    assert!(receipt.ambiguous_effect());
    assert!(!receipt.handoff_eligible());
    assert!(workload.await.unwrap().is_err());
    await_native_absent(&socket, blocker_id).await;
    assert_tcp_refuses(proxy_address).unwrap();
    assert_no_application_sessions(&socket, &application_username).await;

    let before_cleanup = query_gtid(&socket).await;
    proxy
        .cleanup_account(&mut manager, &authority, &attempt)
        .await
        .unwrap();
    assert!(proxy.credential_cleared());
    assert_account_absent(&socket, &application_username).await;
    assert_eq!(query_gtid(&socket).await, before_cleanup);

    manager.stop().unwrap();
    assert!(!socket.exists());
    assert!(!pid_file.exists());
    assert_process_absent(pid).unwrap();
    for address in sql_addresses {
        assert_tcp_refuses(address).unwrap();
    }
    fixture.remove().unwrap_or_else(|error| panic!("{error}"));
}

fn topology(
    sql_addresses: [SocketAddr; 3],
    group_addresses: [SocketAddr; 3],
) -> MysqlTopologyConfig {
    MysqlTopologyConfig::new([
        MysqlMemberConfig::new(
            201,
            sql_addresses[0],
            group_addresses[0],
            GROUP_UUID,
            group_addresses,
        )
        .unwrap(),
        MysqlMemberConfig::new(
            202,
            sql_addresses[1],
            group_addresses[1],
            GROUP_UUID,
            group_addresses,
        )
        .unwrap(),
        MysqlMemberConfig::new(
            203,
            sql_addresses[2],
            group_addresses[2],
            GROUP_UUID,
            group_addresses,
        )
        .unwrap(),
    ])
    .unwrap()
}

fn tcp_options(address: SocketAddr, username: &str, password: &str) -> OptsBuilder {
    OptsBuilder::default()
        .ip_or_hostname(address.ip().to_string())
        .tcp_port(address.port())
        .prefer_socket(false)
        .socket(None::<String>)
        .user(Some(username.to_owned()))
        .pass(Some(password.to_owned()))
        .db_name(Some("kuberic_proxy_qualification".to_owned()))
}

async fn root_connection(socket: &Path) -> Conn {
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

async fn create_qualification_schema(socket: &Path) {
    let mut root = root_connection(socket).await;
    let original: u8 = root
        .query_first("SELECT @@SESSION.sql_log_bin")
        .await
        .unwrap()
        .unwrap();
    root.query_drop("SET SESSION sql_log_bin = OFF")
        .await
        .unwrap();
    root.query_drop("CREATE DATABASE kuberic_proxy_qualification")
        .await
        .unwrap();
    root.query_drop(
        "CREATE TABLE kuberic_proxy_qualification.tokens (\
         token_name VARCHAR(64) PRIMARY KEY, token_value BIGINT UNSIGNED NOT NULL)",
    )
    .await
    .unwrap();
    root.query_drop(if original == 0 {
        "SET SESSION sql_log_bin = OFF"
    } else {
        "SET SESSION sql_log_bin = ON"
    })
    .await
    .unwrap();
    let restored: u8 = root
        .query_first("SELECT @@SESSION.sql_log_bin")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored, original);
    root.disconnect().await.unwrap();
}

async fn query_gtid(socket: &Path) -> String {
    let mut root = root_connection(socket).await;
    let value = root
        .query_first("SELECT @@GLOBAL.gtid_executed")
        .await
        .unwrap()
        .unwrap();
    root.disconnect().await.unwrap();
    value
}

async fn assert_account_profile(socket: &Path, username: &str) {
    let mut root = root_connection(socket).await;
    let plugin: String = root
        .exec_first(
            "SELECT plugin FROM mysql.user WHERE user = ? AND host = 'localhost'",
            (username,),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(plugin, "caching_sha2_password");
    let grants: Vec<String> = root
        .query(format!("SHOW GRANTS FOR '{username}'@'localhost'"))
        .await
        .unwrap();
    assert!(grants.iter().any(|grant| {
        grant.contains("GRANT SELECT, INSERT, UPDATE, DELETE ON `kuberic_proxy_qualification`.*")
    }));
    root.disconnect().await.unwrap();
}

async fn assert_account_absent(socket: &Path, username: &str) {
    let mut root = root_connection(socket).await;
    let count: u64 = root
        .exec_first(
            "SELECT COUNT(*) FROM mysql.user WHERE user = ? AND host = 'localhost'",
            (username,),
        )
        .await
        .unwrap()
        .unwrap();
    root.disconnect().await.unwrap();
    assert_eq!(count, 0);
}

async fn assert_all_application_sessions_registered(
    socket: &Path,
    username: &str,
    proxy: &MysqlClientProxy,
) {
    let mut root = root_connection(socket).await;
    let native: Vec<u64> = root
        .exec(
            "SELECT PROCESSLIST_ID FROM performance_schema.threads \
             WHERE PROCESSLIST_USER = ? AND PROCESSLIST_ID IS NOT NULL",
            (username,),
        )
        .await
        .unwrap();
    root.disconnect().await.unwrap();
    let registered = proxy
        .snapshot()
        .backend_connections()
        .iter()
        .map(|connection| connection.get())
        .collect::<HashSet<_>>();
    assert!(!native.is_empty());
    assert!(
        native
            .iter()
            .all(|connection| registered.contains(connection))
    );
}

async fn assert_no_application_sessions(socket: &Path, username: &str) {
    let mut root = root_connection(socket).await;
    let count: u64 = root
        .exec_first(
            "SELECT COUNT(*) FROM performance_schema.threads \
             WHERE PROCESSLIST_USER = ? AND PROCESSLIST_ID IS NOT NULL",
            (username,),
        )
        .await
        .unwrap()
        .unwrap();
    root.disconnect().await.unwrap();
    assert_eq!(count, 0);
}

async fn await_registered(proxy: &MysqlClientProxy, connection_id: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if proxy
            .snapshot()
            .backend_connections()
            .iter()
            .any(|connection| connection.get() == connection_id)
        {
            return;
        }
        assert!(Instant::now() < deadline, "backend ID was not registered");
        sleep(Duration::from_millis(10)).await;
    }
}

async fn await_sessions(proxy: &MysqlClientProxy, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if proxy.snapshot().sessions() == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "proxy session count did not settle"
        );
        sleep(Duration::from_millis(10)).await;
    }
}

async fn await_active(proxy: &MysqlClientProxy) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if proxy.snapshot().active_commands() == 1 {
            return;
        }
        assert!(Instant::now() < deadline, "blocking query did not dispatch");
        sleep(Duration::from_millis(10)).await;
    }
}

async fn await_native_absent(socket: &Path, connection_id: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut root = root_connection(socket).await;
        let count: u64 = root
            .exec_first(
                "SELECT COUNT(*) FROM performance_schema.threads WHERE PROCESSLIST_ID = ?",
                (connection_id,),
            )
            .await
            .unwrap()
            .unwrap();
        root.disconnect().await.unwrap();
        if count == 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "native connection remained present"
        );
        sleep(Duration::from_millis(10)).await;
    }
}

fn nonzero(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}
