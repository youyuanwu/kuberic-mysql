use std::collections::HashSet;
use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::core::{
    AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, Epoch, GroupName,
    ObservationSessionId, PartitionId, ReplicaId, ReplicaIncarnation, ResourceId,
};
use crate::service::{
    ControlCredential, ControlCredentialRole, MysqlInstanceConfig, MysqlInstanceManager,
    MysqlMemberConfig, MysqlMemberIndex, MysqlOperationTimeouts, MysqlTopologyConfig,
    MysqlTopologyManager, TopologyAttempt, TopologyObservationContext, TransitionCredit,
    TransitionEvaluation,
};
use tokio::time::sleep;

pub(super) const GROUP_UUID: &str = "d6f6f07e-37e1-4f7a-9105-73c13f634cb3";
const MYSQLD: &str = "/usr/sbin/mysqld";
const APPARMOR_EXEC: &str = "/usr/bin/aa-exec";
static NEXT_CONTEXT: AtomicU64 = AtomicU64::new(1);
static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

pub(super) struct LiveQualificationFixture {
    pub(super) manager: MysqlTopologyManager,
    pub(super) sockets: [PathBuf; 3],
    pub(super) sql_addresses: [SocketAddr; 3],
    pub(super) group_addresses: [SocketAddr; 3],
    pub(super) identities: [String; 3],
    root: PathBuf,
}

impl LiveQualificationFixture {
    pub(super) async fn bootstrap() -> Self {
        preflight();
        let serial = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
        let root = PathBuf::from("/tmp").join(format!("kmql-{}-{serial}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();

        let (reservations, sql_addresses, group_addresses) = reserve_ports();
        let topology = topology(sql_addresses, group_addresses);
        let timeouts = MysqlOperationTimeouts::new(
            Duration::from_secs(180),
            Duration::from_secs(60),
            Duration::from_secs(30),
            Duration::from_secs(10),
        )
        .unwrap();
        let configs = MysqlMemberIndex::all().map(|member| {
            let number = member.as_usize() + 1;
            MysqlInstanceConfig::new_topology_member(
                MYSQLD,
                APPARMOR_EXEC,
                root.join(format!("m{number}d")),
                root.join(format!("m{number}s")),
                topology.clone(),
                member,
                timeouts,
            )
            .unwrap()
        });
        let sockets = configs
            .each_ref()
            .map(|config| config.runtime().socket().to_owned());
        let attempt = TopologyAttempt::new(
            AttemptId::new(format!("native-qualification-{}", std::process::id())).unwrap(),
            GroupName::new(GROUP_UUID).unwrap(),
            MysqlMemberIndex::First,
            Duration::from_secs(300),
        )
        .unwrap();
        let observer = ControlCredential::new(
            attempt.id().clone(),
            CredentialGeneration::new(format!("native-observer-generation-{}", std::process::id()))
                .unwrap(),
            ControlCredentialRole::Observer,
            format!("kmq_obs_{}", std::process::id()),
            format!("KmqObserver{}A7", std::process::id()),
        )
        .unwrap();
        let recovery = ControlCredential::new(
            attempt.id().clone(),
            CredentialGeneration::new(format!("native-recovery-generation-{}", std::process::id()))
                .unwrap(),
            ControlCredentialRole::Recovery,
            format!("kmq_rec_{}", std::process::id()),
            format!("KmqRecovery{}B9", std::process::id()),
        )
        .unwrap();
        let mut manager = MysqlTopologyManager::new(
            configs.map(MysqlInstanceManager::new),
            attempt,
            observer,
            recovery,
        )
        .unwrap();
        drop(reservations);
        manager.initialize().unwrap();
        manager.start_designated_member().await.unwrap();
        let first = query_server_uuid(&sockets[0]).await;
        manager.bootstrap().await.unwrap();
        await_accepted(&mut manager, "bootstrap").await;
        manager.start_second_member().await.unwrap();
        let second = query_server_uuid(&sockets[1]).await;
        manager
            .join_second_member(observation_context("source-second"))
            .await
            .unwrap();
        await_accepted(&mut manager, "second").await;
        manager.start_third_member().await.unwrap();
        let third = query_server_uuid(&sockets[2]).await;
        manager
            .join_third_member(observation_context("source-third"))
            .await
            .unwrap();
        await_accepted(&mut manager, "third").await;

        Self {
            manager,
            sockets,
            sql_addresses,
            group_addresses,
            identities: [first, second, third],
            root,
        }
    }

    pub(super) fn cleanup(mut self) {
        self.manager.stop().unwrap();
        for socket in &self.sockets {
            assert!(!socket.exists());
        }
        for address in self.group_addresses {
            assert!(TcpListener::bind(address).is_ok());
        }
        fs::remove_dir_all(&self.root).unwrap();
    }
}

impl Drop for LiveQualificationFixture {
    fn drop(&mut self) {
        let _ = self.manager.stop();
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn topology(
    sql_addresses: [SocketAddr; 3],
    group_addresses: [SocketAddr; 3],
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

fn reserve_ports() -> (Vec<TcpListener>, [SocketAddr; 3], [SocketAddr; 3]) {
    let listeners = (0..6)
        .map(|_| TcpListener::bind(("127.0.0.1", 0)).unwrap())
        .collect::<Vec<_>>();
    let addresses = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(addresses.iter().collect::<HashSet<_>>().len(), 6);
    (
        listeners,
        [addresses[0], addresses[1], addresses[2]],
        [addresses[3], addresses[4], addresses[5]],
    )
}

fn observation_context(label: &str) -> TopologyObservationContext {
    let serial = NEXT_CONTEXT.fetch_add(1, Ordering::Relaxed);
    TopologyObservationContext::new(
        ResourceId::new("native-qualification").unwrap(),
        PartitionId::new("local").unwrap(),
        ReplicaId::new(format!("replica-{label}")).unwrap(),
        ReplicaIncarnation::new(format!("incarnation-{serial}")).unwrap(),
        ObservationSessionId::new(format!("observation-{label}-{serial}")).unwrap(),
        ConfigurationId::new("oracle-mysql-8.4.11-native-qualification").unwrap(),
        Epoch::new("fresh-fixture").unwrap(),
        AuthorityGeneration::new("native-qualification").unwrap(),
        format!("native-qualification:{label}:{serial}"),
    )
}

async fn await_accepted(manager: &mut MysqlTopologyManager, label: &str) -> TransitionCredit {
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut poll = 0_u64;
    loop {
        poll += 1;
        match manager
            .observe_pending(observation_context(&format!("{label}-{poll}")))
            .await
            .unwrap()
        {
            TransitionEvaluation::Accepted(credit) => return credit,
            TransitionEvaluation::Pending => {
                assert!(Instant::now() < deadline, "{label} recovery timed out");
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn query_server_uuid(socket: &Path) -> String {
    use mysql_async::prelude::Queryable;
    use mysql_async::{Conn, OptsBuilder};

    let options = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(1)
        .socket(Some(socket.to_str().unwrap().to_owned()))
        .user(Some("root".to_owned()));
    let mut connection = Conn::new(options).await.unwrap();
    let value = connection
        .query_first::<String, _>("SELECT @@GLOBAL.server_uuid")
        .await
        .unwrap()
        .unwrap();
    connection.disconnect().await.unwrap();
    value
}

fn preflight() {
    for path in [MYSQLD, APPARMOR_EXEC] {
        let metadata = fs::symlink_metadata(path).unwrap();
        assert!(!metadata.file_type().is_symlink());
        assert!(metadata.permissions().mode() & 0o111 != 0);
    }
    let version = command_stdout(
        "dpkg-query",
        &["-W", "-f=${Version}", "mysql-community-server-core"],
    );
    assert_eq!(version.trim(), "8.4.11-1ubuntu24.04");
    let owner = command_stdout("dpkg-query", &["-S", MYSQLD]);
    assert!(owner.starts_with("mysql-community-server-core:"));
    let verification = Command::new("dpkg")
        .args(["--verify", "mysql-community-server-core"])
        .output()
        .unwrap();
    assert!(verification.status.success());
    assert!(verification.stdout.is_empty());
    let active = Command::new("systemctl")
        .args(["is-active", "--quiet", "mysql.service"])
        .status()
        .unwrap();
    assert!(!active.success(), "system mysql.service must be stopped");
    let foreign = Command::new("pgrep")
        .args(["-x", "mysqld"])
        .status()
        .unwrap();
    assert!(!foreign.success(), "foreign mysqld process is active");
    let policy = command_stdout("apt-cache", &["policy", "mysql-community-server-core"]);
    assert!(policy.contains("8.4.11-1ubuntu24.04"));
    assert!(policy.contains("repo.mysql.com"));
    assert!(policy.contains("mysql-8.4-lts"));
}

fn command_stdout(program: &str, arguments: &[&str]) -> String {
    let output = Command::new(program).args(arguments).output().unwrap();
    assert!(output.status.success(), "{program} failed");
    String::from_utf8(output.stdout).unwrap()
}
