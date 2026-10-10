#[path = "service_live_support/mod.rs"]
mod live_support;

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use kuberic_mysql::core::{
    AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, Epoch, GroupName,
    GtidSet, ObservationSessionId, PartitionId, ReplicaId, ReplicaIncarnation, ResourceId,
};
use kuberic_mysql::service::{
    ControlCredential, ControlCredentialRole, MysqlInstanceConfig, MysqlInstanceManager,
    MysqlMemberConfig, MysqlMemberIndex, MysqlOperationTimeouts, MysqlTopologyConfig,
    MysqlTopologyManager, MysqlTopologyState, TopologyAttempt, TopologyObservationContext,
    TransitionCredit, TransitionEvaluation,
};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder};
use tokio::time::sleep;

use live_support::{
    APPARMOR_EXEC, FixtureRoot, MYSQL_X_PORT, MYSQLD, ReservedPorts, assert_no_application_store,
    assert_process_absent, assert_tcp_refuses, preflight_oracle_mysql_8_4_11,
};

const GROUP_UUID: &str = "d6f6f07e-37e1-4f7a-9105-73c13f634cb3";
static NEXT_CONTEXT: AtomicU64 = AtomicU64::new(1);

#[tokio::test(flavor = "current_thread")]
async fn three_fresh_members_bootstrap_join_and_cleanup() {
    preflight_oracle_mysql_8_4_11().unwrap_or_else(|error| panic!("{error}"));
    let mut fixture = FixtureRoot::new("service-group-replication-live")
        .unwrap_or_else(|error| panic!("{error}"));
    let root = fixture.path().to_path_buf();
    let mut reservations = ReservedPorts::allocate().unwrap_or_else(|error| panic!("{error}"));
    let sql_addresses = reservations.sql_addresses();
    let group_addresses = reservations.group_replication_addresses();
    assert_eq!(
        sql_addresses
            .into_iter()
            .chain(group_addresses)
            .collect::<HashSet<_>>()
            .len(),
        6
    );

    let topology = topology(sql_addresses, group_addresses);
    let timeouts = MysqlOperationTimeouts::new(
        Duration::from_secs(180),
        Duration::from_secs(60),
        Duration::from_secs(30),
        Duration::from_secs(10),
    )
    .unwrap();
    let configs = [
        instance_config(&root, topology.clone(), MysqlMemberIndex::First, timeouts),
        instance_config(&root, topology.clone(), MysqlMemberIndex::Second, timeouts),
        instance_config(&root, topology, MysqlMemberIndex::Third, timeouts),
    ];
    let data_roots = configs
        .each_ref()
        .map(|config| config.data_root().to_owned());
    let scratch_roots = configs
        .each_ref()
        .map(|config| config.runtime().scratch_root().to_owned());
    let sockets = configs
        .each_ref()
        .map(|config| config.runtime().socket().to_owned());
    let instances = configs.map(MysqlInstanceManager::new);

    let attempt = TopologyAttempt::new(
        AttemptId::new(format!("gr-live-attempt-{}", std::process::id())).unwrap(),
        GroupName::new(GROUP_UUID).unwrap(),
        MysqlMemberIndex::First,
        Duration::from_secs(300),
    )
    .unwrap();
    let observer_password = format!("KmsObserver{}A7", std::process::id());
    let recovery_password = format!("KmsRecovery{}B9", std::process::id());
    let observer = ControlCredential::new(
        attempt.id().clone(),
        CredentialGeneration::new(format!("observer-generation-{}", std::process::id())).unwrap(),
        ControlCredentialRole::Observer,
        format!("kms_obs_{}", std::process::id()),
        observer_password.clone(),
    )
    .unwrap();
    let recovery = ControlCredential::new(
        attempt.id().clone(),
        CredentialGeneration::new(format!("recovery-generation-{}", std::process::id())).unwrap(),
        ControlCredentialRole::Recovery,
        format!("kms_rec_{}", std::process::id()),
        recovery_password.clone(),
    )
    .unwrap();
    let credential_debug = format!("{observer:?} {recovery:?}");
    assert!(!credential_debug.contains(&observer_password));
    assert!(!credential_debug.contains(&recovery_password));

    let mut manager = MysqlTopologyManager::new(instances, attempt.clone(), observer, recovery)
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(!manager.read_access_open());
    assert!(!manager.write_access_open());
    reservations.release();
    manager
        .initialize()
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(manager.state(), MysqlTopologyState::Initialized);
    for index in 0..3 {
        assert!(data_roots[index].is_dir());
        assert!(scratch_roots[index].is_dir());
        assert_no_application_store(&data_roots[index]).unwrap();
        assert_no_application_store(&scratch_roots[index]).unwrap();
    }

    for address in sql_addresses {
        assert_tcp_refuses(address).unwrap();
    }
    assert_tcp_refuses(([127, 0, 0, 1], MYSQL_X_PORT).into()).unwrap();

    manager
        .start_designated_member()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(manager.state(), MysqlTopologyState::BootstrapMemberEnrolled);
    let first_identity = query_server_uuid(&sockets[0]).await;
    assert_eq!(query_bootstrap_enabled(&sockets[0]).await, 0);
    assert!(!sockets[1].exists());
    assert!(!sockets[2].exists());
    manager
        .bootstrap()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let bootstrap_credit = await_accepted(&mut manager, "bootstrap")
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(manager.state(), MysqlTopologyState::BootstrapAccepted);
    assert_eq!(bootstrap_credit.members().len(), 1);
    assert_eq!(
        bootstrap_credit.members()[0].server_uuid().as_str(),
        first_identity
    );
    assert_eq!(query_bootstrap_enabled(&sockets[0]).await, 0);
    assert_group_sources(bootstrap_credit.executed());
    assert_membership(&sockets[0], std::slice::from_ref(&first_identity)).await;
    assert_tcp_refuses(sql_addresses[0]).unwrap();
    assert_tcp_refuses(([127, 0, 0, 1], MYSQL_X_PORT).into()).unwrap();

    manager
        .start_second_member()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(manager.state(), MysqlTopologyState::SecondMemberEnrolled);
    let second_identity = query_server_uuid(&sockets[1]).await;
    assert_ne!(first_identity, second_identity);
    assert_eq!(query_bootstrap_enabled(&sockets[0]).await, 0);
    assert_eq!(query_bootstrap_enabled(&sockets[1]).await, 0);
    let second_source_boundary = query_gtids(&sockets[0]).await;
    assert_group_sources(&second_source_boundary);
    manager
        .join_second_member(observation_context("source-second"))
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let second_credit = await_accepted(&mut manager, "second")
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(manager.state(), MysqlTopologyState::SecondAccepted);
    assert_eq!(second_credit.members().len(), 2);
    assert_eq!(
        second_credit.members()[1].server_uuid().as_str(),
        second_identity
    );
    assert_exact_view_successor(
        bootstrap_credit.view_id().as_str(),
        second_credit.view_id().as_str(),
    );
    assert!(second_source_boundary.is_subset_of(second_credit.executed()));
    assert_group_sources(second_credit.executed());
    assert_membership(
        &sockets[1],
        &[first_identity.clone(), second_identity.clone()],
    )
    .await;
    for address in &sql_addresses[..2] {
        assert_tcp_refuses(*address).unwrap();
    }

    manager
        .start_third_member()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(manager.state(), MysqlTopologyState::ThirdMemberEnrolled);
    let third_identity = query_server_uuid(&sockets[2]).await;
    assert_ne!(first_identity, third_identity);
    assert_ne!(second_identity, third_identity);
    for socket in &sockets {
        assert_eq!(query_bootstrap_enabled(socket).await, 0);
    }
    let third_source_boundary = query_gtids(&sockets[1]).await;
    assert_group_sources(&third_source_boundary);
    manager
        .join_third_member(observation_context("source-third"))
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    let final_credit = await_accepted(&mut manager, "third")
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(manager.state(), MysqlTopologyState::Complete);
    assert_eq!(final_credit.members().len(), 3);
    assert_exact_view_successor(
        second_credit.view_id().as_str(),
        final_credit.view_id().as_str(),
    );
    assert!(third_source_boundary.is_subset_of(final_credit.executed()));
    assert_group_sources(final_credit.executed());

    let expected_identities = [
        first_identity.clone(),
        second_identity.clone(),
        third_identity.clone(),
    ];
    let accepted_identities = final_credit
        .members()
        .iter()
        .map(|member| member.server_uuid().as_str().to_owned())
        .collect::<HashSet<_>>();
    assert_eq!(
        accepted_identities,
        expected_identities.iter().cloned().collect()
    );
    assert_membership(&sockets[0], &expected_identities).await;
    let accepted = manager.accepted_topology().expect("accepted topology");
    assert_eq!(accepted.credit(), &final_credit);
    assert!(!accepted.read_access_open());
    assert!(!accepted.write_access_open());
    assert!(!manager.read_access_open());
    assert!(!manager.write_access_open());
    for address in sql_addresses {
        assert_tcp_refuses(address).unwrap();
    }
    assert_tcp_refuses(([127, 0, 0, 1], MYSQL_X_PORT).into()).unwrap();

    let pids = sockets.each_ref().map(|socket| {
        let pid_path = socket.parent().unwrap().join("mysqld.pid");
        fs::read_to_string(&pid_path)
            .unwrap_or_else(|error| panic!("read {}: {error}", pid_path.display()))
            .trim()
            .parse::<u32>()
            .unwrap()
    });
    for index in 0..3 {
        assert_no_application_store(&data_roots[index]).unwrap();
        assert_no_application_store(&scratch_roots[index]).unwrap();
    }

    manager.stop().unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(manager.state(), MysqlTopologyState::Stopped);
    for index in 0..3 {
        assert_process_absent(pids[index]).unwrap();
        assert!(!sockets[index].exists());
        assert_tcp_refuses(group_addresses[index]).unwrap();
        assert!(data_roots[index].is_dir());
        assert!(!scratch_roots[index].exists());
        assert_no_application_store(&data_roots[index]).unwrap();
    }
    fixture.remove().unwrap_or_else(|error| panic!("{error}"));
    assert!(!root.exists());
}

fn topology(
    sql_addresses: [std::net::SocketAddr; 3],
    group_addresses: [std::net::SocketAddr; 3],
) -> MysqlTopologyConfig {
    MysqlTopologyConfig::new([
        MysqlMemberConfig::new(
            101,
            sql_addresses[0],
            group_addresses[0],
            GROUP_UUID,
            group_addresses,
        )
        .unwrap(),
        MysqlMemberConfig::new(
            102,
            sql_addresses[1],
            group_addresses[1],
            GROUP_UUID,
            group_addresses,
        )
        .unwrap(),
        MysqlMemberConfig::new(
            103,
            sql_addresses[2],
            group_addresses[2],
            GROUP_UUID,
            group_addresses,
        )
        .unwrap(),
    ])
    .unwrap()
}

fn instance_config(
    root: &Path,
    topology: MysqlTopologyConfig,
    member: MysqlMemberIndex,
    timeouts: MysqlOperationTimeouts,
) -> MysqlInstanceConfig {
    let number = member_number(member);
    MysqlInstanceConfig::new_topology_member(
        MYSQLD,
        APPARMOR_EXEC,
        root.join(format!("member-{number}-data")),
        root.join(format!("member-{number}-scratch")),
        topology,
        member,
        timeouts,
    )
    .unwrap()
}

fn member_number(member: MysqlMemberIndex) -> usize {
    match member {
        MysqlMemberIndex::First => 1,
        MysqlMemberIndex::Second => 2,
        MysqlMemberIndex::Third => 3,
    }
}

fn observation_context(label: &str) -> TopologyObservationContext {
    let serial = NEXT_CONTEXT.fetch_add(1, Ordering::Relaxed);
    TopologyObservationContext::new(
        ResourceId::new("live-topology").unwrap(),
        PartitionId::new("local").unwrap(),
        ReplicaId::new(format!("replica-{label}")).unwrap(),
        ReplicaIncarnation::new(format!("incarnation-{serial}")).unwrap(),
        ObservationSessionId::new(format!("observation-{label}-{serial}")).unwrap(),
        ConfigurationId::new("oracle-mysql-8.4.11-three-member").unwrap(),
        Epoch::new("fresh-fixture").unwrap(),
        AuthorityGeneration::new("live-attempt").unwrap(),
        format!("service-group-replication-live:{label}:{serial}"),
    )
}

async fn await_accepted(
    manager: &mut MysqlTopologyManager,
    label: &str,
) -> Result<TransitionCredit, String> {
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut poll = 0_u64;
    loop {
        poll = poll.saturating_add(1);
        let evaluation = manager
            .observe_pending(observation_context(&format!("{label}-{poll}")))
            .await
            .map_err(|error| error.to_string())?;
        match evaluation {
            TransitionEvaluation::Accepted(credit) => return Ok(credit),
            TransitionEvaluation::Pending => {
                if Instant::now() >= deadline {
                    return Err(format!(
                        "{label} remained RECOVERING until the live deadline"
                    ));
                }
                sleep(Duration::from_millis(100)).await;
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

async fn query_server_uuid(socket: &Path) -> String {
    let mut connection = root_connection(socket).await;
    let value = connection
        .query_first::<String, _>("SELECT @@GLOBAL.server_uuid")
        .await
        .unwrap()
        .expect("server UUID row");
    connection.disconnect().await.unwrap();
    value
}

async fn query_bootstrap_enabled(socket: &Path) -> u8 {
    let mut connection = root_connection(socket).await;
    let value = connection
        .query_first::<u8, _>("SELECT @@GLOBAL.group_replication_bootstrap_group")
        .await
        .unwrap()
        .expect("bootstrap mode row");
    connection.disconnect().await.unwrap();
    value
}

async fn query_gtids(socket: &Path) -> GtidSet {
    let mut connection = root_connection(socket).await;
    let value = connection
        .query_first::<String, _>("SELECT @@GLOBAL.gtid_executed")
        .await
        .unwrap()
        .expect("GTID row");
    connection.disconnect().await.unwrap();
    GtidSet::from_str(&value).unwrap()
}

async fn assert_membership(socket: &Path, expected: &[String]) {
    let mut connection = root_connection(socket).await;
    let rows = connection
        .query_map(
            "SELECT MEMBER_ID, MEMBER_STATE \
             FROM performance_schema.replication_group_members ORDER BY MEMBER_ID",
            |(member_id, state): (String, String)| (member_id, state),
        )
        .await
        .unwrap();
    connection.disconnect().await.unwrap();
    assert_eq!(rows.len(), expected.len());
    assert!(rows.iter().all(|(_, state)| state == "ONLINE"));
    assert_eq!(
        rows.into_iter()
            .map(|(member_id, _)| member_id)
            .collect::<HashSet<_>>(),
        expected.iter().cloned().collect()
    );
}

fn assert_group_sources(executed: &GtidSet) {
    assert!(
        executed
            .entries()
            .iter()
            .all(|entry| { entry.source().server_uuid().as_str() == GROUP_UUID })
    );
}

fn assert_exact_view_successor(predecessor: &str, successor: &str) {
    let (predecessor_fixed, predecessor_monotonic) = predecessor.split_once(':').unwrap();
    let (successor_fixed, successor_monotonic) = successor.split_once(':').unwrap();
    assert_eq!(
        predecessor_fixed.parse::<u64>().unwrap().to_string(),
        predecessor_fixed
    );
    assert_eq!(
        successor_fixed.parse::<u64>().unwrap().to_string(),
        successor_fixed
    );
    assert_eq!(predecessor_fixed, successor_fixed);
    assert_eq!(
        predecessor_monotonic
            .parse::<u32>()
            .unwrap()
            .checked_add(1)
            .unwrap(),
        successor_monotonic.parse::<u32>().unwrap()
    );
}
