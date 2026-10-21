use std::collections::HashSet;
use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
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
    data_roots: [PathBuf; 3],
    scratch_roots: [PathBuf; 3],
    root: FixtureRoot,
    cleaned: bool,
    deadline: Instant,
}

struct FixtureRoot {
    path: PathBuf,
    device: u64,
    inode: u64,
    remove_on_drop: bool,
}

impl FixtureRoot {
    fn create(path: PathBuf) -> Self {
        assert!(!path.exists(), "qualification fixture root already exists");
        fs::create_dir_all(path.parent().expect("qualification fixture parent")).unwrap();
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        let metadata = fs::symlink_metadata(&path).unwrap();
        Self {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
            remove_on_drop: true,
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn try_remove(&mut self) -> Result<(), String> {
        let metadata = fs::symlink_metadata(&self.path)
            .map_err(|error| format!("fixture root inspection failed: {error}"))?;
        if (metadata.dev(), metadata.ino()) != (self.device, self.inode)
            || metadata.file_type().is_symlink()
            || !metadata.is_dir()
        {
            return Err("fixture root identity changed".to_owned());
        }
        fs::remove_dir_all(&self.path)
            .map_err(|error| format!("fixture root removal failed: {error}"))?;
        self.remove_on_drop = false;
        Ok(())
    }

    fn retain(&mut self) {
        self.remove_on_drop = false;
    }
}

impl Drop for FixtureRoot {
    fn drop(&mut self) {
        if self.remove_on_drop && self.path.exists() {
            let _ = self.try_remove();
        }
    }
}

struct SetupGuard {
    manager: Option<MysqlTopologyManager>,
    root: Option<FixtureRoot>,
}

impl SetupGuard {
    fn new(manager: MysqlTopologyManager, root: FixtureRoot) -> Self {
        Self {
            manager: Some(manager),
            root: Some(root),
        }
    }

    fn manager(&mut self) -> &mut MysqlTopologyManager {
        self.manager.as_mut().expect("setup retains manager")
    }

    fn complete(mut self) -> (MysqlTopologyManager, FixtureRoot) {
        (
            self.manager.take().expect("setup retains manager"),
            self.root.take().expect("setup retains root"),
        )
    }
}

impl Drop for SetupGuard {
    fn drop(&mut self) {
        let contained = self
            .manager
            .as_mut()
            .is_none_or(|manager| manager.stop().is_ok());
        if contained {
            if let Some(root) = self.root.as_mut() {
                let _ = root.try_remove();
            }
        } else if let Some(root) = self.root.as_mut() {
            root.retain();
        }
    }
}

impl LiveQualificationFixture {
    pub(super) async fn bootstrap() -> Self {
        preflight();
        let serial = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
        let root = std::env::current_dir()
            .unwrap()
            .join("target")
            .join(format!("kmql-{}-{serial}", std::process::id()));
        let root = FixtureRoot::create(root);

        let (reservations, sql_addresses, group_addresses) = reserve_ports();
        let topology = topology(sql_addresses, group_addresses);
        let timeouts = MysqlOperationTimeouts::new(
            Duration::from_secs(180),
            Duration::from_secs(30),
            Duration::from_secs(20),
            Duration::from_secs(10),
        )
        .unwrap();
        let configs = MysqlMemberIndex::all().map(|member| {
            let number = member.as_usize() + 1;
            MysqlInstanceConfig::new_topology_member(
                MYSQLD,
                APPARMOR_EXEC,
                root.path().join(format!("m{number}d")),
                root.path().join(format!("m{number}s")),
                topology.clone(),
                member,
                timeouts,
            )
            .unwrap()
        });
        let sockets = configs
            .each_ref()
            .map(|config| config.runtime().socket().to_owned());
        let data_roots = configs
            .each_ref()
            .map(|config| config.data_root().to_owned());
        let scratch_roots = configs
            .each_ref()
            .map(|config| config.runtime().scratch_root().to_owned());
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
        let manager = MysqlTopologyManager::new(
            configs.map(MysqlInstanceManager::new),
            attempt,
            observer,
            recovery,
        )
        .unwrap();
        let deadline = manager.qualification_runtime_deadline();
        let mut setup = SetupGuard::new(manager, root);
        drop(reservations);
        setup.manager().initialize().unwrap();
        setup.manager().start_designated_member().await.unwrap();
        let first = query_server_uuid(&sockets[0]).await;
        setup.manager().bootstrap().await.unwrap();
        await_accepted(setup.manager(), "bootstrap").await;
        setup.manager().start_second_member().await.unwrap();
        let second = query_server_uuid(&sockets[1]).await;
        setup
            .manager()
            .join_second_member(observation_context("source-second"))
            .await
            .unwrap();
        await_accepted(setup.manager(), "second").await;
        setup.manager().start_third_member().await.unwrap();
        let third = query_server_uuid(&sockets[2]).await;
        setup
            .manager()
            .join_third_member(observation_context("source-third"))
            .await
            .unwrap();
        await_accepted(setup.manager(), "third").await;
        let (manager, root) = setup.complete();

        Self {
            manager,
            sockets,
            sql_addresses,
            group_addresses,
            identities: [first, second, third],
            data_roots,
            scratch_roots,
            root,
            cleaned: false,
            deadline,
        }
    }

    pub(super) fn cleanup(mut self) {
        if self.sockets.iter().any(|socket| socket.exists())
            || self.scratch_roots.iter().any(|root| root.exists())
        {
            self.manager.stop().unwrap();
        }
        for index in 0..3 {
            assert!(!self.sockets[index].exists());
            assert!(self.data_roots[index].is_dir());
            assert!(!self.scratch_roots[index].exists());
            assert_no_application_store(&self.data_roots[index]);
        }
        for address in self.group_addresses {
            assert!(TcpListener::bind(address).is_ok());
        }
        for address in self.sql_addresses {
            assert!(
                std::net::TcpStream::connect_timeout(&address, Duration::from_millis(50)).is_err()
            );
        }
        let mysql_x = SocketAddr::from(([127, 0, 0, 1], 33_060));
        assert!(std::net::TcpStream::connect_timeout(&mysql_x, Duration::from_millis(50)).is_err());
        self.cleaned = true;
        self.root.try_remove().unwrap();
        assert!(
            Instant::now() <= self.deadline,
            "qualification fixture cleanup exceeded its attempt deadline"
        );
    }
}

impl Drop for LiveQualificationFixture {
    fn drop(&mut self) {
        if self.cleaned {
            return;
        }
        if self.manager.stop().is_ok() {
            let _ = self.root.try_remove();
        } else {
            self.root.retain();
        }
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
        ConfigurationId::new("oracle-mysql-8.4-native-qualification").unwrap(),
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
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut connection = Conn::new(options).await.unwrap();
        let value = connection
            .query_first::<String, _>("SELECT @@GLOBAL.server_uuid")
            .await
            .unwrap()
            .unwrap();
        connection.disconnect().await.unwrap();
        value
    })
    .await
    .expect("server UUID query timed out")
}

fn preflight() {
    assert_eq!(std::env::consts::OS, "linux");
    assert_eq!(std::env::consts::ARCH, "x86_64");
    for path in [MYSQLD, APPARMOR_EXEC] {
        let metadata = fs::symlink_metadata(path).unwrap();
        assert!(!metadata.file_type().is_symlink());
        assert!(metadata.permissions().mode() & 0o111 != 0);
    }
    let version = command_stdout(
        "dpkg-query",
        &["-W", "-f=${Version}", "mysql-community-server-core"],
    );
    let version = version.trim();
    assert!(
        crate::adapter::is_supported_package_version(version),
        "expected Oracle MySQL 8.4 LTS, found {version}"
    );
    eprintln!("MySQL qualification package: mysql-community-server-core={version}");
    let owner = command_stdout("dpkg-query", &["-S", MYSQLD]);
    assert!(owner.starts_with("mysql-community-server-core:"));
    let launcher_owner = command_stdout("dpkg-query", &["-S", APPARMOR_EXEC]);
    assert!(!launcher_owner.trim().is_empty());
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
    assert!(policy.contains(&format!("Installed: {version}")));
    assert!(
        crate::adapter::apt_repository_for_version(
            &policy,
            version,
            "repo.mysql.com",
            "mysql-8.4-lts",
        )
        .is_some(),
        "installed MySQL package lacks Oracle 8.4 LTS repository provenance"
    );
}

fn assert_no_application_store(root: &Path) {
    let forbidden = [
        "journal",
        "receipt",
        "cursor",
        "adoption",
        "metadata.db",
        "state.json",
    ];
    let mut pending = vec![root.to_owned()];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().to_ascii_lowercase();
            assert!(
                !forbidden.iter().any(|forbidden| name.contains(forbidden)),
                "private application store artifact: {}",
                entry.path().display()
            );
            if entry.file_type().unwrap().is_dir() {
                pending.push(entry.path());
            }
        }
    }
}

fn command_stdout(program: &str, arguments: &[&str]) -> String {
    let output = Command::new(program).args(arguments).output().unwrap();
    assert!(output.status.success(), "{program} failed");
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn archived_fixture_creates_missing_checkout_target_parents() {
    let serial = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir().join(format!(
        "kmql-archive-parent-{}-{serial}",
        std::process::id()
    ));
    fs::DirBuilder::new().mode(0o700).create(&base).unwrap();
    let leaf = base.join("fresh-checkout/target/fixture");
    assert!(!leaf.parent().unwrap().exists());
    let mut root = FixtureRoot::create(leaf.clone());
    assert_eq!(
        fs::metadata(&leaf).unwrap().permissions().mode() & 0o777,
        0o700
    );
    root.try_remove().unwrap();
    assert!(!leaf.exists());
    fs::remove_dir_all(base).unwrap();
}
