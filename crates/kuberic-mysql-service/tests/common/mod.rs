#![allow(dead_code)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use kuberic_mysql_service::{MysqlInstanceConfig, MysqlOperationTimeouts};

static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

pub struct TestRoot {
    pub root: PathBuf,
    pub data: PathBuf,
    pub scratch: PathBuf,
    pub launcher: PathBuf,
}

impl TestRoot {
    pub fn new(label: &str) -> Self {
        let serial = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
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
