#![allow(dead_code)]

use std::collections::HashSet;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

const EXPECTED_PACKAGE: &str = "mysql-community-server-core";
const EXPECTED_VERSION: &str = "8.4.11-1ubuntu24.04";
const EXPECTED_REPOSITORY_HOST: &str = "repo.mysql.com";
const EXPECTED_REPOSITORY_COMPONENT: &str = "mysql-8.4-lts";
pub const MYSQLD: &str = "/usr/sbin/mysqld";
pub const APPARMOR_EXEC: &str = "/usr/bin/aa-exec";
pub const MYSQL_X_PORT: u16 = 33060;

pub fn preflight_oracle_mysql_8_4_11() -> Result<(), String> {
    if std::env::consts::OS != "linux" || std::env::consts::ARCH != "x86_64" {
        return Err("live qualification requires Linux x86-64".to_owned());
    }
    verify_executable(Path::new(MYSQLD))?;
    verify_executable(Path::new(APPARMOR_EXEC))?;
    verify_mysql_service_stopped()?;
    verify_no_foreign_mysqld()?;
    verify_package_owner()?;
    verify_installed_version()?;
    verify_package_files()?;
    verify_apt_policy()?;
    assert_tcp_refuses(SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::LOCALHOST,
        MYSQL_X_PORT,
    )))?;
    Ok(())
}

pub fn fresh_fixture_root(label: &str) -> Result<PathBuf, String> {
    let project = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| "tests package is not beneath the workspace root".to_owned())?;
    let root = project
        .join("qualification")
        .join("q")
        .join(format!("{label}-{}", std::process::id()));
    if root.exists() {
        fs::remove_dir_all(&root)
            .map_err(|error| format!("remove stale fixture {}: {error}", root.display()))?;
    }
    fs::create_dir_all(&root)
        .map_err(|error| format!("create fixture {}: {error}", root.display()))?;
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
        .map_err(|error| format!("protect fixture {}: {error}", root.display()))?;
    Ok(root)
}

pub struct FixtureRoot {
    root: PathBuf,
}

impl FixtureRoot {
    pub fn new(label: &str) -> Result<Self, String> {
        Ok(Self {
            root: fresh_fixture_root(label)?,
        })
    }

    pub fn path(&self) -> &Path {
        &self.root
    }

    pub fn remove(&mut self) -> Result<(), String> {
        if self.root.exists() {
            fs::remove_dir_all(&self.root)
                .map_err(|error| format!("remove fixture {}: {error}", self.root.display()))?;
        }
        Ok(())
    }
}

impl Drop for FixtureRoot {
    fn drop(&mut self) {
        let _ = fs::set_permissions(&self.root, fs::Permissions::from_mode(0o700));
        let _ = fs::remove_dir_all(&self.root);
    }
}

pub struct ReservedPorts {
    listeners: Vec<TcpListener>,
    addresses: [SocketAddr; 6],
}

impl ReservedPorts {
    pub fn allocate() -> Result<Self, String> {
        let mut listeners = Vec::with_capacity(6);
        let mut addresses = Vec::with_capacity(6);
        let mut used = HashSet::new();
        while listeners.len() < 6 {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
                .map_err(|error| format!("reserve loopback port: {error}"))?;
            let address = listener
                .local_addr()
                .map_err(|error| format!("inspect reserved loopback port: {error}"))?;
            if address.port() == MYSQL_X_PORT || !used.insert(address) {
                continue;
            }
            listeners.push(listener);
            addresses.push(address);
        }
        let addresses: [SocketAddr; 6] = addresses
            .try_into()
            .map_err(|_| "expected exactly six reserved ports".to_owned())?;
        Ok(Self {
            listeners,
            addresses,
        })
    }

    pub fn sql_addresses(&self) -> [SocketAddr; 3] {
        [self.addresses[0], self.addresses[1], self.addresses[2]]
    }

    pub fn group_replication_addresses(&self) -> [SocketAddr; 3] {
        [self.addresses[3], self.addresses[4], self.addresses[5]]
    }

    pub fn release(&mut self) {
        self.listeners.clear();
    }
}

pub fn assert_tcp_refuses(address: SocketAddr) -> Result<(), String> {
    match TcpStream::connect_timeout(&address, Duration::from_millis(250)) {
        Ok(stream) => {
            drop(stream);
            Err(format!("unexpected TCP listener at {address}"))
        }
        Err(_) => Ok(()),
    }
}

pub fn assert_process_absent(pid: u32) -> Result<(), String> {
    if Path::new(&format!("/proc/{pid}")).exists() {
        Err(format!("owned mysqld PID {pid} still exists"))
    } else {
        Ok(())
    }
}

pub fn assert_no_application_store(root: &Path) -> Result<(), String> {
    if !root.exists() {
        return Ok(());
    }
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)
            .map_err(|error| format!("inspect runtime layout {}: {error}", directory.display()))?
        {
            let entry = entry.map_err(|error| {
                format!(
                    "inspect runtime entry beneath {}: {error}",
                    directory.display()
                )
            })?;
            let name = entry.file_name().to_string_lossy().to_ascii_lowercase();
            for forbidden in [
                "kuberic",
                "journal",
                "receipt",
                "workflow-cursor",
                "workflow_cursor",
                "adoption",
                "operation-store",
                "operation_store",
            ] {
                if name.contains(forbidden) {
                    return Err(format!(
                        "unexpected application durability artifact {}",
                        entry.path().display()
                    ));
                }
            }
            if entry
                .file_type()
                .map_err(|error| format!("inspect {}: {error}", entry.path().display()))?
                .is_dir()
            {
                pending.push(entry.path());
            }
        }
    }
    Ok(())
}

fn verify_executable(path: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("inspect {}: {error}", path.display()))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.permissions().mode() & 0o111 == 0
    {
        return Err(format!(
            "{} must be an executable regular non-symlink file",
            path.display()
        ));
    }
    Ok(())
}

fn verify_mysql_service_stopped() -> Result<(), String> {
    let output = Command::new("systemctl")
        .args(["is-active", "mysql.service"])
        .output()
        .map_err(|error| format!("inspect mysql.service: {error}"))?;
    let state = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if matches!(state.as_str(), "active" | "activating" | "reloading") {
        Err(format!("mysql.service is {state}"))
    } else {
        Ok(())
    }
}

fn verify_no_foreign_mysqld() -> Result<(), String> {
    for entry in fs::read_dir("/proc").map_err(|error| format!("inspect /proc: {error}"))? {
        let entry = entry.map_err(|error| format!("inspect /proc entry: {error}"))?;
        if entry
            .file_name()
            .to_string_lossy()
            .bytes()
            .all(|byte| byte.is_ascii_digit())
            && let Ok(name) = fs::read_to_string(entry.path().join("comm"))
            && matches!(name.trim(), "mysqld" | "mysqld-debug")
        {
            return Err("a foreign mysqld process is already running".to_owned());
        }
    }
    Ok(())
}

fn verify_package_owner() -> Result<(), String> {
    let output = checked_output(
        Command::new("dpkg-query").arg("-S").arg(MYSQLD),
        "query mysqld package owner",
    )?;
    let owner = String::from_utf8_lossy(&output);
    let expected = format!("{EXPECTED_PACKAGE}: {MYSQLD}");
    if owner.lines().any(|line| line.trim() == expected) {
        Ok(())
    } else {
        Err(format!("expected package owner {expected}, found {owner}"))
    }
}

fn verify_installed_version() -> Result<(), String> {
    let output = checked_output(
        Command::new("dpkg-query").args([
            "-W",
            "-f=${Package}\t${Version}\t${Status}",
            EXPECTED_PACKAGE,
        ]),
        "query installed MySQL package",
    )?;
    let value = String::from_utf8_lossy(&output);
    let expected = format!("{EXPECTED_PACKAGE}\t{EXPECTED_VERSION}\tinstall ok installed");
    if value.trim() == expected {
        Ok(())
    } else {
        Err(format!("expected {expected}, found {}", value.trim()))
    }
}

fn verify_package_files() -> Result<(), String> {
    let output = Command::new("dpkg")
        .args(["--verify", EXPECTED_PACKAGE])
        .output()
        .map_err(|error| format!("verify installed MySQL package: {error}"))?;
    if output.status.success() && output.stdout.is_empty() && output.stderr.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "dpkg --verify failed: status={:?} stdout={} stderr={}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

fn verify_apt_policy() -> Result<(), String> {
    let output = checked_output(
        Command::new("apt-cache").args(["policy", EXPECTED_PACKAGE]),
        "query MySQL APT policy",
    )?;
    let policy = String::from_utf8_lossy(&output);
    let installed = policy
        .lines()
        .find_map(|line| line.trim().strip_prefix("Installed: "))
        .ok_or_else(|| "APT policy omitted installed version".to_owned())?;
    let candidate = policy
        .lines()
        .find_map(|line| line.trim().strip_prefix("Candidate: "))
        .ok_or_else(|| "APT policy omitted candidate version".to_owned())?;
    if installed != EXPECTED_VERSION || candidate != EXPECTED_VERSION {
        return Err(format!(
            "expected installed/candidate {EXPECTED_VERSION}, found {installed}/{candidate}"
        ));
    }
    if !policy.contains(EXPECTED_REPOSITORY_HOST) || !policy.contains(EXPECTED_REPOSITORY_COMPONENT)
    {
        return Err(format!(
            "APT policy omitted {EXPECTED_REPOSITORY_HOST} {EXPECTED_REPOSITORY_COMPONENT}"
        ));
    }
    Ok(())
}

fn checked_output(command: &mut Command, context: &str) -> Result<Vec<u8>, String> {
    let output = command
        .env("LC_ALL", "C")
        .output()
        .map_err(|error| format!("{context}: {error}"))?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(format!(
            "{context}: status={:?} stdout={} stderr={}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}
