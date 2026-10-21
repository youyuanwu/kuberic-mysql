#![allow(dead_code)]

use std::collections::HashSet;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

const EXPECTED_PACKAGE: &str = "mysql-community-server-core";
const EXPECTED_REPOSITORY_HOST: &str = "repo.mysql.com";
const EXPECTED_REPOSITORY_COMPONENT: &str = "mysql-8.4-lts";
pub const MYSQLD: &str = "/usr/sbin/mysqld";
pub const APPARMOR_EXEC: &str = "/usr/bin/aa-exec";
pub const MYSQL_X_PORT: u16 = 33060;

pub fn preflight_oracle_mysql_8_4() -> Result<(), String> {
    if std::env::consts::OS != "linux" || std::env::consts::ARCH != "x86_64" {
        return Err("live qualification requires Linux x86-64".to_owned());
    }
    verify_executable(Path::new(MYSQLD))?;
    verify_executable(Path::new(APPARMOR_EXEC))?;
    verify_mysql_service_stopped()?;
    verify_no_foreign_mysqld()?;
    verify_package_owner()?;
    let version = verify_installed_version()?;
    verify_package_files()?;
    verify_apt_policy(&version)?;
    eprintln!("MySQL qualification package: {EXPECTED_PACKAGE}={version}");
    assert_tcp_refuses(SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::LOCALHOST,
        MYSQL_X_PORT,
    )))?;
    Ok(())
}

pub fn fresh_fixture_root(label: &str) -> Result<PathBuf, String> {
    let short_label = &label[..label.len().min(12)];
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| "resolve workspace root".to_owned())?;
    let root = workspace
        .join("target")
        .join(format!("km-{short_label}-{}", std::process::id()));
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
    preserve_on_drop: bool,
}

impl FixtureRoot {
    /// Retain diagnostics and refuse to replace evidence, including on collision.
    pub fn new_retained(label: &str) -> Result<Self, String> {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .ok_or_else(|| "resolve workspace root".to_owned())?;
        let target = workspace.join("target");
        fs::create_dir_all(&target).map_err(|error| format!("create target: {error}"))?;
        let short_label = &label[..label.len().min(12)];
        let root = target.join(format!("km-{short_label}-{}", std::process::id()));
        fs::create_dir(&root)
            .map_err(|error| format!("create nonreplacing fixture {}: {error}", root.display()))?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .map_err(|error| format!("protect fixture {}: {error}", root.display()))?;
        Ok(Self {
            root,
            preserve_on_drop: true,
        })
    }

    pub fn new(label: &str) -> Result<Self, String> {
        let mut fixture = Self::new_retained(label)?;
        fixture.preserve_on_drop = false;
        Ok(fixture)
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

    pub fn preserve(&mut self) {
        self.preserve_on_drop = true;
    }
}

impl Drop for FixtureRoot {
    fn drop(&mut self) {
        if self.preserve_on_drop || std::thread::panicking() {
            eprintln!(
                "retaining native fixture diagnostics: {}",
                self.root.display()
            );
            return;
        }

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
    for _ in 0..3 {
        match TcpStream::connect_timeout(&address, Duration::from_millis(250)) {
            Ok(stream) => {
                let local = stream.local_addr().map_err(|error| error.to_string())?;
                let peer = stream.peer_addr().map_err(|error| error.to_string())?;
                drop(stream);
                // Linux can connect an ephemeral socket to itself without a listener.
                if local == peer {
                    eprintln!("resampling TCP absence after a self-connection at {address}");
                    continue;
                }
                return Err(format!(
                    "unexpected TCP listener at {address} (local={local}, peer={peer})"
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => return Ok(()),
            Err(error) => {
                return Err(format!("cannot qualify TCP refusal at {address}: {error}"));
            }
        }
    }
    Err(format!(
        "TCP absence at {address} remained ambiguous after self-connections"
    ))
}

pub fn assert_process_absent(pid: u32) -> Result<(), String> {
    if Path::new(&format!("/proc/{pid}")).exists() {
        Err(format!("owned mysqld PID {pid} still exists"))
    } else {
        Ok(())
    }
}

pub fn assert_no_application_store(root: &Path) -> Result<(), String> {
    assert_no_application_store_with_native_schemas(root, &[])
}

/// Native fixture SQL schemas are user data, not orchestration stores. Only
/// exact top-level directory names are exempted; their contents are scanned.
pub fn assert_no_application_store_with_native_schemas(
    root: &Path,
    native_schemas: &[&str],
) -> Result<(), String> {
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
            let native_schema = directory == root
                && native_schemas.contains(&name.as_str())
                && entry
                    .file_type()
                    .map_err(|error| error.to_string())?
                    .is_dir();
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
                if !native_schema && name.contains(forbidden) {
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

fn verify_installed_version() -> Result<String, String> {
    let output = checked_output(
        Command::new("dpkg-query").args([
            "-W",
            "-f=${Package}\t${Version}\t${Status}",
            EXPECTED_PACKAGE,
        ]),
        "query installed MySQL package",
    )?;
    let value = String::from_utf8_lossy(&output);
    let fields = value.trim().split('\t').collect::<Vec<_>>();
    if let [package, version, "install ok installed"] = fields.as_slice()
        && *package == EXPECTED_PACKAGE
        && kuberic_mysql::adapter::is_supported_package_version(version)
    {
        Ok((*version).to_owned())
    } else {
        Err(format!(
            "expected installed {EXPECTED_PACKAGE} from Oracle MySQL 8.4 LTS, found {}",
            value.trim()
        ))
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

fn verify_apt_policy(expected_version: &str) -> Result<(), String> {
    let output = checked_output(
        Command::new("apt-cache").args(["policy", EXPECTED_PACKAGE]),
        "query MySQL APT policy",
    )?;
    let policy = String::from_utf8_lossy(&output);
    validate_apt_policy(&policy, expected_version)
}

fn validate_apt_policy(policy: &str, expected_version: &str) -> Result<(), String> {
    let installed = policy
        .lines()
        .find_map(|line| line.trim().strip_prefix("Installed: "))
        .ok_or_else(|| "APT policy omitted installed version".to_owned())?;
    if installed != expected_version
        || !kuberic_mysql::adapter::is_supported_package_version(installed)
    {
        return Err(format!(
            "expected installed Oracle MySQL 8.4 package {expected_version}, found {installed}"
        ));
    }

    if kuberic_mysql::adapter::apt_repository_for_version(
        policy,
        installed,
        EXPECTED_REPOSITORY_HOST,
        EXPECTED_REPOSITORY_COMPONENT,
    )
    .is_none()
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

#[cfg(test)]
mod package_policy_tests {
    use super::{assert_tcp_refuses, validate_apt_policy};

    #[test]
    fn tcp_absence_rejects_a_live_listener_then_accepts_its_refusal() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        assert!(assert_tcp_refuses(address).is_err());
        drop(listener);
        assert_tcp_refuses(address).unwrap();
    }

    #[test]
    fn newer_candidate_does_not_disqualify_an_installed_supported_patch() {
        let policy = "mysql-community-server-core:\n\
          Installed: 8.4.11-1ubuntu24.04\n\
          Candidate: 8.4.13-1ubuntu24.04\n\
          Version table:\n\
         *** 8.4.11-1ubuntu24.04 500\n\
                500 http://repo.mysql.com/apt/ubuntu noble/mysql-8.4-lts amd64 Packages\n";
        assert!(validate_apt_policy(policy, "8.4.11-1ubuntu24.04").is_ok());
        assert!(validate_apt_policy(policy, "8.4.13-1ubuntu24.04").is_err());
        assert!(
            validate_apt_policy(
                &policy.replace("repo.mysql.com", "mirror.example.invalid"),
                "8.4.11-1ubuntu24.04",
            )
            .is_err()
        );
        assert!(
            validate_apt_policy(
                &policy.replace("*** 8.4.11-1ubuntu24.04", "8.4.13-1ubuntu24.04"),
                "8.4.11-1ubuntu24.04",
            )
            .is_err()
        );
    }
}

#[cfg(test)]
mod retention_tests {
    use super::*;

    #[test]
    fn failed_fixture_is_preserved_and_collision_cannot_replace_it() {
        let label = "retain-check";
        let mut fixture = FixtureRoot::new(label).unwrap();
        let root = fixture.path().to_path_buf();
        fs::write(root.join("diagnostic"), "retained").unwrap();
        std::thread::spawn(move || {
            let _owned = fixture;
            panic!("controlled fixture unwind");
        })
        .join()
        .unwrap_err();
        assert_eq!(
            fs::read_to_string(root.join("diagnostic")).unwrap(),
            "retained"
        );
        assert!(FixtureRoot::new(label).is_err());
        // This test alone owns the newly created diagnostic root.
        fixture = FixtureRoot {
            root,
            preserve_on_drop: false,
        };
        fixture.remove().unwrap();
    }
}
