#![allow(dead_code)]

use std::fs;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use kuberic_mysql::service::{
    MysqlInstanceConfig, MysqlMemberConfig, MysqlMemberIndex, MysqlOperationTimeouts,
    MysqlTopologyConfig,
};

static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);
pub const GROUP_UUID: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";

pub struct TestRoot {
    pub root: PathBuf,
    pub data: PathBuf,
    pub scratch: PathBuf,
    pub launcher: PathBuf,
}

impl TestRoot {
    pub fn new(label: &str) -> Self {
        let serial = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap();
        let root = workspace.join("target").join(format!(
            "kms-{}-{serial}-{}",
            std::process::id(),
            &label[..label.len().min(8)]
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        Self {
            data: root.join("data"),
            scratch: root.join("scratch"),
            launcher: root.join("launcher"),
            root,
        }
    }

    pub fn config(&self) -> MysqlInstanceConfig {
        self.config_with_timeouts(timeouts())
    }

    pub fn config_with_timeouts(&self, timeouts: MysqlOperationTimeouts) -> MysqlInstanceConfig {
        write_launcher(&self.launcher);
        MysqlInstanceConfig::new(
            "/usr/bin/sleep",
            &self.launcher,
            &self.data,
            &self.scratch,
            timeouts,
        )
        .unwrap()
    }

    pub fn config_for_member(&self, member_index: MysqlMemberIndex) -> MysqlInstanceConfig {
        self.config_for_member_with_timeouts(member_index, timeouts())
    }

    pub fn config_for_member_with_timeouts(
        &self,
        member_index: MysqlMemberIndex,
        timeouts: MysqlOperationTimeouts,
    ) -> MysqlInstanceConfig {
        self.config_for_member_in_topology(topology(), member_index, timeouts)
    }

    pub fn config_for_member_in_topology(
        &self,
        topology: MysqlTopologyConfig,
        member_index: MysqlMemberIndex,
        timeouts: MysqlOperationTimeouts,
    ) -> MysqlInstanceConfig {
        write_launcher(&self.launcher);
        MysqlInstanceConfig::new_topology_member(
            "/usr/bin/sleep",
            &self.launcher,
            &self.data,
            &self.scratch,
            topology,
            member_index,
            timeouts,
        )
        .unwrap()
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::set_permissions(&self.root, fs::Permissions::from_mode(0o700));
        let _ = fs::remove_dir_all(&self.root);
    }
}

pub fn timeouts() -> MysqlOperationTimeouts {
    MysqlOperationTimeouts::new(
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
    )
    .unwrap()
}

pub fn topology() -> MysqlTopologyConfig {
    MysqlTopologyConfig::new(member_configs()).unwrap()
}

pub fn member_configs() -> [MysqlMemberConfig; 3] {
    let sql_addresses = [
        loopback_address(33061),
        loopback_address(33062),
        loopback_address(33063),
    ];
    let group_addresses = [
        loopback_address(43061),
        loopback_address(43062),
        loopback_address(43063),
    ];
    let seeds = [group_addresses[2], group_addresses[0], group_addresses[1]];
    [
        MysqlMemberConfig::new(1, sql_addresses[0], group_addresses[0], GROUP_UUID, seeds).unwrap(),
        MysqlMemberConfig::new(2, sql_addresses[1], group_addresses[1], GROUP_UUID, seeds).unwrap(),
        MysqlMemberConfig::new(3, sql_addresses[2], group_addresses[2], GROUP_UUID, seeds).unwrap(),
    ]
}

pub fn loopback_address(port: u16) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
}

fn write_launcher(path: &Path) {
    fs::write(
        path,
        r#"#!/bin/sh
while [ "$1" != "--" ]; do shift; done
shift
mysqld="$1"
shift
if [ "$1" = "--version" ]; then
  echo "$mysqld  Ver 8.4.11 for Linux on x86_64 (MySQL Community Server - GPL)"
  exit 0
fi
case "$*" in
  *--initialize-insecure*) exit 0 ;;
esac
config=${1#--defaults-file=}
pid_file=$(sed -n 's/^pid-file=//p' "$config")
echo $$ > "$pid_file"
exec "$mysqld" 60
"#,
    )
    .unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}

pub fn provide_socket(pid_path: PathBuf, socket_path: PathBuf) -> JoinHandle<()> {
    thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        let pid = loop {
            if let Ok(value) = fs::read_to_string(&pid_path)
                && let Ok(pid) = value.trim().parse::<u32>()
            {
                break pid;
            }
            assert!(Instant::now() < deadline, "PID file did not appear");
            thread::sleep(Duration::from_millis(10));
        };
        let listener = UnixListener::bind(&socket_path).unwrap();
        while Path::new(&format!("/proc/{pid}")).exists() {
            thread::sleep(Duration::from_millis(10));
        }
        drop(listener);
        let _ = fs::remove_file(socket_path);
    })
}
