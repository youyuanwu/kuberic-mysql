#![allow(dead_code)]

use std::ffi::OsStr;
use std::fs::{self, File};
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use mysql_async::{Conn, OptsBuilder};
use tokio::time::sleep;

use super::{QualificationCode, QualificationError, project_root};

const EXPECTED_PACKAGE_NAME: &str = "mysql-community-server-core";
const EXPECTED_PACKAGE_VERSION: &str = "8.4.11-1ubuntu24.04";
const EXPECTED_APT_REPOSITORY_HOST: &str = "repo.mysql.com";
const EXPECTED_APT_REPOSITORY_COMPONENT: &str = "mysql-8.4-lts";
const EXPECTED_MYSQLD: &str = "/usr/sbin/mysqld";
const EXPECTED_APPARMOR_EXEC: &str = "/usr/bin/aa-exec";
const STARTUP_TIMEOUT_MS: u64 = 30_000;
const OBSERVATION_TIMEOUT_MS: u64 = 5_000;
const CLEANUP_TIMEOUT_MS: u64 = 10_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QualificationConfig {
    pub mysqld_path: PathBuf,
    pub apparmor_exec_path: PathBuf,
    pub package_name: String,
    pub package_version: String,
    pub apt_repository_host: String,
    pub apt_repository_component: String,
    pub qualification_root: PathBuf,
    pub startup_timeout_ms: u64,
    pub observation_timeout_ms: u64,
    pub cleanup_timeout_ms: u64,
}

impl QualificationConfig {
    pub fn for_repository() -> Result<Self, QualificationError> {
        let root = project_root();
        let qualification = root.join("qualification");
        let config = Self {
            mysqld_path: EXPECTED_MYSQLD.into(),
            apparmor_exec_path: EXPECTED_APPARMOR_EXEC.into(),
            package_name: EXPECTED_PACKAGE_NAME.to_owned(),
            package_version: EXPECTED_PACKAGE_VERSION.to_owned(),
            apt_repository_host: EXPECTED_APT_REPOSITORY_HOST.to_owned(),
            apt_repository_component: EXPECTED_APT_REPOSITORY_COMPONENT.to_owned(),
            qualification_root: qualification.join("q"),
            startup_timeout_ms: STARTUP_TIMEOUT_MS,
            observation_timeout_ms: OBSERVATION_TIMEOUT_MS,
            cleanup_timeout_ms: CLEANUP_TIMEOUT_MS,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), QualificationError> {
        if self.apparmor_exec_path != Path::new(EXPECTED_APPARMOR_EXEC) {
            return Err(QualificationError::new(
                QualificationCode::MissingInput,
                "AppArmor launcher path",
                format!("must be exactly {EXPECTED_APPARMOR_EXEC}"),
            ));
        }
        for (label, path) in [
            ("mysqld path", &self.mysqld_path),
            ("AppArmor launcher path", &self.apparmor_exec_path),
        ] {
            let metadata = fs::symlink_metadata(path).map_err(|error| {
                QualificationError::new(
                    QualificationCode::MissingInput,
                    label,
                    format!("{}: {error}", path.display()),
                )
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(QualificationError::new(
                    QualificationCode::MissingInput,
                    label,
                    format!("{} must be a regular non-symlink file", path.display()),
                ));
            }
            if metadata.permissions().mode() & 0o111 == 0 {
                return Err(QualificationError::new(
                    QualificationCode::MissingInput,
                    label,
                    format!("{} must be executable", path.display()),
                ));
            }
        }
        for (label, actual, expected) in [
            ("package name", &self.package_name, EXPECTED_PACKAGE_NAME),
            (
                "package version",
                &self.package_version,
                EXPECTED_PACKAGE_VERSION,
            ),
            (
                "APT repository host",
                &self.apt_repository_host,
                EXPECTED_APT_REPOSITORY_HOST,
            ),
            (
                "APT repository component",
                &self.apt_repository_component,
                EXPECTED_APT_REPOSITORY_COMPONENT,
            ),
        ] {
            if actual != expected {
                return Err(QualificationError::new(
                    QualificationCode::UnsupportedVersion,
                    label,
                    format!("expected {expected}, found {actual}"),
                ));
            }
        }
        let qualification = project_root().join("qualification");
        if !self.qualification_root.starts_with(&qualification) {
            return Err(QualificationError::new(
                QualificationCode::NonAbsoluteInput,
                "qualification paths",
                format!(
                    "qualification root must stay beneath {}",
                    qualification.display()
                ),
            ));
        }
        if self.startup_timeout_ms == 0
            || self.observation_timeout_ms == 0
            || self.cleanup_timeout_ms == 0
        {
            return Err(QualificationError::new(
                QualificationCode::MissingInput,
                "timeouts",
                "qualification timeouts must be positive".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn startup_timeout(&self) -> Duration {
        Duration::from_millis(self.startup_timeout_ms)
    }

    pub fn observation_timeout(&self) -> Duration {
        Duration::from_millis(self.observation_timeout_ms)
    }

    pub fn cleanup_timeout(&self) -> Duration {
        Duration::from_millis(self.cleanup_timeout_ms)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Layout {
    work_root: PathBuf,
    runtime_root: PathBuf,
    data_root: PathBuf,
}

impl Layout {
    fn create(config: &QualificationConfig) -> Result<Self, QualificationError> {
        let work_root = config.qualification_root.join("work");
        if work_root.exists() {
            fs::remove_dir_all(&work_root).map_err(|error| {
                QualificationError::new(
                    QualificationCode::CleanupFailure,
                    "remove stale work root",
                    format!("{}: {error}", work_root.display()),
                )
            })?;
        }
        fs::create_dir_all(&config.qualification_root).map_err(|error| {
            QualificationError::new(
                QualificationCode::UnwritableOutput,
                "qualification root",
                format!("{}: {error}", config.qualification_root.display()),
            )
        })?;
        let layout = Self {
            work_root: work_root.clone(),
            runtime_root: work_root.join("runtime"),
            data_root: work_root.join("data"),
        };
        for directory in [&layout.work_root, &layout.runtime_root, &layout.data_root] {
            create_private_directory(directory)?;
        }
        verify_private_directories(&layout)?;
        Ok(layout)
    }
}

fn create_private_directory(path: &Path) -> Result<(), QualificationError> {
    fs::create_dir_all(path).map_err(|error| {
        QualificationError::new(
            QualificationCode::UnwritableOutput,
            "create private qualification directory",
            format!("{}: {error}", path.display()),
        )
    })?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|error| {
        QualificationError::new(
            QualificationCode::UnwritableOutput,
            "set private qualification directory mode",
            format!("{}: {error}", path.display()),
        )
    })?;
    verify_private_mode(path)
}

fn verify_private_mode(path: &Path) -> Result<(), QualificationError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        QualificationError::new(
            QualificationCode::UnwritableOutput,
            "stat private qualification directory",
            format!("{}: {error}", path.display()),
        )
    })?;
    let mode = metadata.permissions().mode() & 0o777;
    if metadata.is_dir() && !metadata.file_type().is_symlink() && mode & 0o077 == 0 {
        Ok(())
    } else {
        Err(QualificationError::new(
            QualificationCode::UnwritableOutput,
            "private qualification directory mode",
            format!(
                "{} must be an owner-only directory, mode={mode:o}",
                path.display()
            ),
        ))
    }
}

fn verify_private_directories(layout: &Layout) -> Result<(), QualificationError> {
    for path in [&layout.work_root, &layout.runtime_root, &layout.data_root] {
        verify_private_mode(path)?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ServerConfig {
    config_path: PathBuf,
    error_log_path: PathBuf,
    stdout_log_path: PathBuf,
    socket_path: PathBuf,
    pid_path: PathBuf,
    sql_port: u16,
    group_port: u16,
    group_name: String,
}

impl ServerConfig {
    fn new(layout: &Layout) -> Result<Self, QualificationError> {
        Ok(Self {
            config_path: layout.runtime_root.join("my.cnf"),
            error_log_path: layout.runtime_root.join("mysqld.err"),
            stdout_log_path: layout.runtime_root.join("mysqld.out"),
            socket_path: layout.runtime_root.join("mysql.sock"),
            pid_path: layout.runtime_root.join("mysqld.pid"),
            sql_port: allocate_loopback_port()?,
            group_port: allocate_loopback_port()?,
            group_name: "cccccccc-cccc-cccc-cccc-cccccccccccc".to_owned(),
        })
    }

    fn render(&self, data_root: &Path) -> String {
        format!(
            "[mysqld]\n\
             datadir={}\n\
             socket={}\n\
             pid-file={}\n\
             log-error={}\n\
             skip-networking=ON\n\
             mysqlx=OFF\n\
             port={}\n\
             report-host=127.0.0.1\n\
             report-port={}\n\
             server-id=1\n\
             log_bin=mysql-bin\n\
             binlog_format=ROW\n\
             binlog_checksum=NONE\n\
             relay_log_recovery=ON\n\
             gtid_mode=ON\n\
             enforce_gtid_consistency=ON\n\
             plugin-load-add=group_replication.so\n\
             loose-group_replication_group_name={}\n\
             loose-group_replication_start_on_boot=OFF\n\
             loose-group_replication_bootstrap_group=OFF\n\
             loose-group_replication_local_address=127.0.0.1:{}\n\
             loose-group_replication_group_seeds=127.0.0.1:{}\n\
             loose-group_replication_single_primary_mode=ON\n\
             loose-group_replication_enforce_update_everywhere_checks=OFF\n\
             loose-group_replication_ip_allowlist=127.0.0.1/8\n",
            data_root.display(),
            self.socket_path.display(),
            self.pid_path.display(),
            self.error_log_path.display(),
            self.sql_port,
            self.sql_port,
            self.group_name,
            self.group_port,
            self.group_port
        )
    }
}

#[derive(Clone, Debug)]
pub struct PreparedArtifact {
    qualification_config: QualificationConfig,
    layout: Layout,
    mysqld_path: PathBuf,
}

impl PreparedArtifact {
    pub fn prepare(config: &QualificationConfig) -> Result<Self, QualificationError> {
        verify_platform()?;
        preflight_no_foreign_mysql()?;
        verify_launcher(&config.apparmor_exec_path)?;
        let layout = Layout::create(config)?;
        let verified = verify_installed_package(config);
        let mysqld_path = match verified {
            Ok(verified) => verified,
            Err(prior) => {
                if let Err(error) = fs::remove_dir_all(&layout.work_root) {
                    return Err(QualificationError::new(
                        QualificationCode::CleanupFailure,
                        "cleanup failed artifact preparation",
                        format!(
                            "{}: {error}; prior failure: {prior}",
                            layout.work_root.display()
                        ),
                    ));
                }
                return Err(prior);
            }
        };

        Ok(Self {
            qualification_config: config.clone(),
            layout,
            mysqld_path,
        })
    }

    pub async fn launch(self) -> Result<RunningFixture, QualificationError> {
        let server_config = match ServerConfig::new(&self.layout) {
            Ok(config) => config,
            Err(error) => return Err(cleanup_unlaunched(&self.layout, error)),
        };
        if let Err(error) = fs::write(
            &server_config.config_path,
            server_config.render(&self.layout.data_root),
        ) {
            return Err(cleanup_unlaunched(
                &self.layout,
                QualificationError::new(
                    QualificationCode::InitializationFailure,
                    "write mysqld config",
                    format!("{}: {error}", server_config.config_path.display()),
                ),
            ));
        }

        let init_status = match mysqld_command(&self.qualification_config, &self.mysqld_path)
            .arg(format!(
                "--defaults-file={}",
                server_config.config_path.display()
            ))
            .arg("--initialize-insecure")
            .status()
        {
            Ok(status) => status,
            Err(error) => {
                return Err(cleanup_unlaunched(
                    &self.layout,
                    QualificationError::new(
                        QualificationCode::InitializationFailure,
                        "initialize mysqld",
                        format!("{}: {error}", self.mysqld_path.display()),
                    ),
                ));
            }
        };
        if !init_status.success() {
            return Err(cleanup_unlaunched(
                &self.layout,
                QualificationError::new(
                    QualificationCode::InitializationFailure,
                    "initialize mysqld",
                    format!("exit status {:?}", init_status.code()),
                ),
            ));
        }

        let stdout = match File::create(&server_config.stdout_log_path) {
            Ok(stdout) => stdout,
            Err(error) => {
                return Err(cleanup_unlaunched(
                    &self.layout,
                    QualificationError::new(
                        QualificationCode::LaunchFailure,
                        "open mysqld stdout log",
                        format!("{}: {error}", server_config.stdout_log_path.display()),
                    ),
                ));
            }
        };
        let stderr = match File::create(&server_config.error_log_path) {
            Ok(stderr) => stderr,
            Err(error) => {
                return Err(cleanup_unlaunched(
                    &self.layout,
                    QualificationError::new(
                        QualificationCode::LaunchFailure,
                        "open mysqld stderr log",
                        format!("{}: {error}", server_config.error_log_path.display()),
                    ),
                ));
            }
        };

        let mut child = match mysqld_command(&self.qualification_config, &self.mysqld_path)
            .arg(format!(
                "--defaults-file={}",
                server_config.config_path.display()
            ))
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                return Err(cleanup_unlaunched(
                    &self.layout,
                    QualificationError::new(
                        QualificationCode::LaunchFailure,
                        "spawn mysqld",
                        format!("{}: {error}", self.mysqld_path.display()),
                    ),
                ));
            }
        };

        if let Err(error) = wait_for_socket_ready(
            &mut child,
            &server_config.socket_path,
            self.qualification_config.startup_timeout(),
        )
        .await
        {
            return Err(cleanup_failed_launch(&mut child, &self.layout, error));
        }
        attest_child_executable(child.id(), &self.mysqld_path)
            .map_err(|error| cleanup_failed_launch(&mut child, &self.layout, error))?;

        let qualification_config = self.qualification_config;
        let layout = self.layout;

        Ok(RunningFixture {
            qualification_config,
            layout,
            server_config,
            child,
        })
    }
}

fn mysqld_command(config: &QualificationConfig, mysqld_path: &Path) -> Command {
    let mut command = Command::new(&config.apparmor_exec_path);
    command
        .arg("-p")
        .arg("unconfined")
        .arg("--")
        .arg(mysqld_path);
    command
}

fn preflight_no_foreign_mysql() -> Result<(), QualificationError> {
    let service = Command::new("systemctl")
        .args(["is-active", "mysql.service"])
        .output()
        .map_err(|error| {
            QualificationError::new(
                QualificationCode::ToolUnavailable,
                "mysql service preflight",
                error.to_string(),
            )
        })?;
    let state = String::from_utf8_lossy(&service.stdout).trim().to_owned();
    if matches!(state.as_str(), "active" | "activating" | "reloading") {
        return Err(QualificationError::new(
            QualificationCode::ForeignMysqlActive,
            "mysql service preflight",
            format!("mysql.service is {state}"),
        ));
    }
    let mut names = Vec::new();
    for entry in fs::read_dir("/proc").map_err(|error| {
        QualificationError::new(
            QualificationCode::ForeignMysqlActive,
            "process preflight",
            error.to_string(),
        )
    })? {
        let entry = entry.map_err(|error| {
            QualificationError::new(
                QualificationCode::ForeignMysqlActive,
                "process preflight",
                error.to_string(),
            )
        })?;
        if entry
            .file_name()
            .to_string_lossy()
            .bytes()
            .all(|byte| byte.is_ascii_digit())
            && let Ok(name) = fs::read_to_string(entry.path().join("comm"))
        {
            names.push(name.trim().to_owned());
        }
    }
    reject_foreign_process_names(&names)
}

fn reject_foreign_process_names(names: &[String]) -> Result<(), QualificationError> {
    if names
        .iter()
        .any(|name| name == "mysqld" || name == "mysqld-debug")
    {
        Err(QualificationError::new(
            QualificationCode::ForeignMysqlActive,
            "process preflight",
            "a foreign mysqld process is already running".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn verify_launcher(path: &Path) -> Result<(), QualificationError> {
    let owner = run_tool(
        "dpkg-query",
        &[OsStr::new("-S"), path.as_os_str()],
        QualificationCode::ToolUnavailable,
        "query AppArmor launcher owner",
    )?;
    let owner_text = String::from_utf8_lossy(&owner.stdout);
    let package = owner_text
        .split_once(':')
        .map(|(package, _)| package.trim().to_owned())
        .ok_or_else(|| {
            QualificationError::new(
                QualificationCode::PackageOwnershipMismatch,
                "AppArmor launcher owner",
                "dpkg-query returned no package owner".to_owned(),
            )
        })?;
    verify_dpkg_files(&package)?;
    Ok(())
}

fn attest_child_executable(pid: u32, expected: &Path) -> Result<PathBuf, QualificationError> {
    let actual = fs::read_link(format!("/proc/{pid}/exe")).map_err(|error| {
        QualificationError::new(
            QualificationCode::ChildExecutableMismatch,
            "child executable attestation",
            error.to_string(),
        )
    })?;
    let expected = fs::canonicalize(expected).map_err(|error| {
        QualificationError::new(
            QualificationCode::ChildExecutableMismatch,
            "expected executable attestation",
            error.to_string(),
        )
    })?;
    if actual == expected {
        Ok(actual)
    } else {
        Err(QualificationError::new(
            QualificationCode::ChildExecutableMismatch,
            "child executable attestation",
            format!(
                "expected {}, found {}",
                expected.display(),
                actual.display()
            ),
        ))
    }
}

fn verify_installed_package(config: &QualificationConfig) -> Result<PathBuf, QualificationError> {
    let mysqld_path = config.mysqld_path.clone();
    let owner = run_tool(
        "dpkg-query",
        &[OsStr::new("-S"), mysqld_path.as_os_str()],
        QualificationCode::ToolUnavailable,
        "query mysqld package owner",
    )?;
    let owner_text = String::from_utf8_lossy(&owner.stdout).trim().to_owned();
    validate_package_owner(&owner_text, &config.package_name, &mysqld_path)?;

    let query_format = "${Package}\t${Version}\t${Status}";
    let installed = run_tool(
        "dpkg-query",
        &[
            OsStr::new("-W"),
            OsStr::new("-f"),
            OsStr::new(query_format),
            OsStr::new(&config.package_name),
        ],
        QualificationCode::ToolUnavailable,
        "query installed package version",
    )?;
    let installed_text = String::from_utf8_lossy(&installed.stdout);
    validate_installed_metadata(
        installed_text.trim(),
        &config.package_name,
        &config.package_version,
    )?;

    verify_dpkg_files(&config.package_name)?;
    verify_apt_policy(config)?;

    verify_runtime_libraries(&mysqld_path)?;

    Ok(mysqld_path)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AptPolicy {
    installed: String,
    candidate: String,
    repository: String,
}

fn validate_package_owner(
    actual: &str,
    package_name: &str,
    mysqld_path: &Path,
) -> Result<(), QualificationError> {
    let expected = format!("{package_name}: {}", mysqld_path.display());
    if actual == expected {
        Ok(())
    } else {
        Err(QualificationError::new(
            QualificationCode::PackageOwnershipMismatch,
            "mysqld package owner",
            format!("expected {expected}, found {actual}"),
        ))
    }
}

fn validate_installed_metadata(
    actual: &str,
    package_name: &str,
    package_version: &str,
) -> Result<(), QualificationError> {
    let fields = actual.split('\t').collect::<Vec<_>>();
    if fields == [package_name, package_version, "install ok installed"] {
        Ok(())
    } else {
        Err(QualificationError::new(
            QualificationCode::PackageMetadataMismatch,
            "installed package metadata",
            format!(
                "expected {package_name} {package_version} install ok installed, found {actual}"
            ),
        ))
    }
}

fn verify_dpkg_files(package_name: &str) -> Result<(), QualificationError> {
    let output = Command::new("dpkg")
        .env("LC_ALL", "C")
        .args(["--verify", package_name])
        .output()
        .map_err(|error| {
            QualificationError::new(
                QualificationCode::ToolUnavailable,
                "verify installed package files",
                format!("dpkg: {error}"),
            )
        })?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() || !stdout.trim().is_empty() || !stderr.trim().is_empty() {
        return Err(QualificationError::new(
            QualificationCode::PackageVerificationMismatch,
            "verify installed package files",
            format!(
                "dpkg --verify status {:?}: {}{}",
                output.status.code(),
                stdout,
                stderr
            ),
        ));
    }
    Ok(())
}

fn verify_apt_policy(config: &QualificationConfig) -> Result<AptPolicy, QualificationError> {
    let output = run_tool(
        "apt-cache",
        &[OsStr::new("policy"), OsStr::new(&config.package_name)],
        QualificationCode::ToolUnavailable,
        "query APT package policy",
    )?;
    let policy = String::from_utf8_lossy(&output.stdout);
    parse_apt_policy(
        &policy,
        &config.package_version,
        &config.apt_repository_host,
        &config.apt_repository_component,
    )
}

fn parse_apt_policy(
    policy: &str,
    expected_version: &str,
    repository_host: &str,
    repository_component: &str,
) -> Result<AptPolicy, QualificationError> {
    let installed = policy
        .lines()
        .find_map(|line| line.trim().strip_prefix("Installed: "))
        .ok_or_else(|| {
            QualificationError::new(
                QualificationCode::PackageMetadataMismatch,
                "APT installed version",
                "apt-cache policy omitted Installed".to_owned(),
            )
        })?
        .to_owned();
    let candidate = policy
        .lines()
        .find_map(|line| line.trim().strip_prefix("Candidate: "))
        .ok_or_else(|| {
            QualificationError::new(
                QualificationCode::PackageMetadataMismatch,
                "APT candidate version",
                "apt-cache policy omitted Candidate".to_owned(),
            )
        })?
        .to_owned();
    if installed != expected_version || candidate != expected_version {
        return Err(QualificationError::new(
            QualificationCode::PackageMetadataMismatch,
            "APT package version",
            format!(
                "expected installed/candidate {expected_version}, found installed={installed} candidate={candidate}"
            ),
        ));
    }
    let lines = policy.lines().collect::<Vec<_>>();
    let version_index = lines
        .iter()
        .position(|line| {
            line.trim()
                .trim_start_matches("*** ")
                .split_whitespace()
                .next()
                == Some(expected_version)
        })
        .ok_or_else(|| {
            QualificationError::new(
                QualificationCode::AptProvenanceMismatch,
                "APT version stanza",
                format!("policy omitted version stanza {expected_version}"),
            )
        })?;
    let repository = lines
        .iter()
        .skip(version_index + 1)
        .take_while(|line| {
            let first = line
                .trim()
                .trim_start_matches("*** ")
                .split_whitespace()
                .next()
                .unwrap_or_default();
            !(first.contains('.')
                && first
                    .bytes()
                    .next()
                    .is_some_and(|byte| byte.is_ascii_digit()))
        })
        .map(|line| line.trim())
        .find(|line| line.contains(repository_host) && line.contains(repository_component))
        .ok_or_else(|| {
            QualificationError::new(
                QualificationCode::AptProvenanceMismatch,
                "APT repository provenance",
                format!(
                    "policy did not contain host {} and component {}",
                    repository_host, repository_component
                ),
            )
        })?
        .to_owned();
    Ok(AptPolicy {
        installed,
        candidate,
        repository,
    })
}

fn cleanup_unlaunched(layout: &Layout, prior: QualificationError) -> QualificationError {
    if !layout.work_root.exists() {
        return prior;
    }
    match fs::remove_dir_all(&layout.work_root) {
        Ok(()) => prior,
        Err(error) => QualificationError::new(
            QualificationCode::CleanupFailure,
            "cleanup before mysqld launch",
            format!(
                "{}: {error}; prior failure: {prior}",
                layout.work_root.display()
            ),
        ),
    }
}

fn cleanup_failed_launch(
    child: &mut Child,
    layout: &Layout,
    prior: QualificationError,
) -> QualificationError {
    let cleanup = (|| -> Result<(), QualificationError> {
        if child
            .try_wait()
            .map_err(|error| {
                QualificationError::new(
                    QualificationCode::CleanupFailure,
                    "check failed launch child",
                    error.to_string(),
                )
            })?
            .is_none()
        {
            child.kill().map_err(|error| {
                QualificationError::new(
                    QualificationCode::CleanupFailure,
                    "stop failed launch child",
                    error.to_string(),
                )
            })?;
            child.wait().map_err(|error| {
                QualificationError::new(
                    QualificationCode::CleanupFailure,
                    "reap failed launch child",
                    error.to_string(),
                )
            })?;
        }
        if layout.work_root.exists() {
            fs::remove_dir_all(&layout.work_root).map_err(|error| {
                QualificationError::new(
                    QualificationCode::CleanupFailure,
                    "remove failed launch work root",
                    format!("{}: {error}", layout.work_root.display()),
                )
            })?;
        }
        Ok(())
    })();
    match cleanup {
        Ok(()) => prior,
        Err(cleanup_error) => QualificationError::new(
            QualificationCode::CleanupFailure,
            "cleanup after launch failure",
            format!("{cleanup_error}; prior failure: {prior}"),
        ),
    }
}

#[derive(Debug)]
pub struct RunningFixture {
    qualification_config: QualificationConfig,
    layout: Layout,
    server_config: ServerConfig,
    child: Child,
}

impl Drop for RunningFixture {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if self.layout.work_root.exists() {
            let _ = fs::remove_dir_all(&self.layout.work_root);
        }
    }
}

impl RunningFixture {
    pub fn qualification_config(&self) -> &QualificationConfig {
        &self.qualification_config
    }

    pub fn socket_path(&self) -> &Path {
        &self.server_config.socket_path
    }

    pub fn sql_port(&self) -> u16 {
        self.server_config.sql_port
    }

    pub fn group_port(&self) -> u16 {
        self.server_config.group_port
    }

    pub fn group_name(&self) -> &str {
        &self.server_config.group_name
    }

    pub async fn connect_root(&self) -> Result<Conn, QualificationError> {
        self.connect_as("root", None).await
    }

    pub async fn connect_as(
        &self,
        username: &str,
        password: Option<&str>,
    ) -> Result<Conn, QualificationError> {
        open_connection(&self.server_config.socket_path, username, password)
            .await
            .map_err(|error| {
                QualificationError::new(
                    QualificationCode::AccountStateSetupFailure,
                    "connect local mysqld",
                    format!("user={username}: {error}"),
                )
            })
    }

    pub fn cleanup(mut self) -> Result<(), QualificationError> {
        let mut status = self.child.try_wait().map_err(|error| {
            QualificationError::new(
                QualificationCode::CleanupFailure,
                "check mysqld status",
                error.to_string(),
            )
        })?;
        if status.is_none() {
            send_signal(self.child.id(), "TERM")?;
            let deadline = Instant::now() + self.qualification_config.cleanup_timeout();
            while Instant::now() < deadline {
                status = self.child.try_wait().map_err(|error| {
                    QualificationError::new(
                        QualificationCode::CleanupFailure,
                        "wait for mysqld exit",
                        error.to_string(),
                    )
                })?;
                if status.is_some() {
                    break;
                }
                thread::sleep(Duration::from_millis(100));
            }
        }
        if status.is_none() {
            send_signal(self.child.id(), "KILL")?;
            self.child.wait().map_err(|error| {
                QualificationError::new(
                    QualificationCode::CleanupFailure,
                    "reap mysqld",
                    error.to_string(),
                )
            })?;
        }

        for path in [&self.layout.data_root, &self.layout.runtime_root] {
            if path.exists() {
                fs::remove_dir_all(path).map_err(|error| {
                    QualificationError::new(
                        QualificationCode::CleanupFailure,
                        "remove qualification directory",
                        format!("{}: {error}", path.display()),
                    )
                })?;
            }
        }
        if self.layout.work_root.exists() {
            fs::remove_dir(&self.layout.work_root).map_err(|error| {
                QualificationError::new(
                    QualificationCode::CleanupFailure,
                    "remove work root",
                    format!("{}: {error}", self.layout.work_root.display()),
                )
            })?;
        }

        if self.layout.work_root.exists() {
            return Err(QualificationError::new(
                QualificationCode::CleanupFailure,
                "verify work root removal",
                format!("{} still exists", self.layout.work_root.display()),
            ));
        }
        Ok(())
    }
}

async fn wait_for_socket_ready(
    child: &mut Child,
    socket_path: &Path,
    timeout: Duration,
) -> Result<(), QualificationError> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().map_err(|error| {
            QualificationError::new(
                QualificationCode::LaunchFailure,
                "wait for mysqld startup",
                error.to_string(),
            )
        })? {
            return Err(QualificationError::new(
                QualificationCode::LaunchFailure,
                "mysqld exited before readiness",
                format!("exit code {:?}", status.code()),
            ));
        }
        if socket_path.exists() && open_connection(socket_path, "root", None).await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(QualificationError::new(
                QualificationCode::LaunchFailure,
                "mysqld readiness",
                format!("timeout waiting for {}", socket_path.display()),
            ));
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn open_connection(
    socket_path: &Path,
    username: &str,
    password: Option<&str>,
) -> Result<Conn, mysql_async::Error> {
    let socket = socket_path
        .to_str()
        .expect("qualification socket paths stay UTF-8");
    let mut options = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(1)
        .socket(Some(socket.to_owned()))
        .user(Some(username.to_owned()));
    if let Some(password) = password {
        options = options.pass(Some(password.to_owned()));
    }
    Conn::new(options).await
}

fn verify_platform() -> Result<(), QualificationError> {
    if std::env::consts::OS != "linux" || std::env::consts::ARCH != "x86_64" {
        return Err(QualificationError::new(
            QualificationCode::IncompatiblePlatform,
            "host platform",
            format!(
                "requires linux/x86_64, found {}/{}",
                std::env::consts::OS,
                std::env::consts::ARCH
            ),
        ));
    }
    Ok(())
}

fn verify_runtime_libraries(mysqld_path: &Path) -> Result<(), QualificationError> {
    let output = run_tool(
        "ldd",
        &[mysqld_path.as_os_str()],
        QualificationCode::ToolUnavailable,
        "inspect mysqld runtime libraries",
    )?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if stdout.contains("not found") || stderr.contains("not found") {
        return Err(QualificationError::new(
            QualificationCode::IncompatiblePlatform,
            "runtime libraries",
            format!("missing runtime library for {}", mysqld_path.display()),
        ));
    }
    Ok(())
}

fn run_tool(
    program: &str,
    args: &[&OsStr],
    not_found_code: QualificationCode,
    context: &'static str,
) -> Result<Output, QualificationError> {
    let output = Command::new(program)
        .env("LC_ALL", "C")
        .args(args)
        .output()
        .map_err(|error| {
            let code = if error.kind() == io::ErrorKind::NotFound {
                not_found_code
            } else {
                QualificationCode::ToolUnavailable
            };
            QualificationError::new(code, context, format!("{program}: {error}"))
        })?;
    if output.status.success() {
        return Ok(output);
    }
    Err(QualificationError::new(
        not_found_code,
        context,
        format!(
            "{program} failed with status {:?}: {}{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    ))
}

fn send_signal(pid: u32, signal: &str) -> Result<(), QualificationError> {
    let flag = format!("-{signal}");
    run_tool(
        "kill",
        &[OsStr::new(&flag), OsStr::new(&pid.to_string())],
        QualificationCode::CleanupFailure,
        "signal mysqld",
    )?;
    Ok(())
}

fn allocate_loopback_port() -> Result<u16, QualificationError> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).map_err(|error| {
        QualificationError::new(
            QualificationCode::LaunchFailure,
            "allocate loopback port",
            error.to_string(),
        )
    })?;
    let port = listener.local_addr().map_err(|error| {
        QualificationError::new(
            QualificationCode::LaunchFailure,
            "read allocated loopback port",
            error.to_string(),
        )
    })?;
    Ok(port.port())
}

#[cfg(test)]
mod tests {
    use super::{
        EXPECTED_APT_REPOSITORY_COMPONENT, EXPECTED_APT_REPOSITORY_HOST, EXPECTED_PACKAGE_NAME,
        EXPECTED_PACKAGE_VERSION, create_private_directory, parse_apt_policy,
        reject_foreign_process_names, validate_installed_metadata, validate_package_owner,
        verify_private_mode,
    };

    #[test]
    fn qualification_package_contract_is_exact() {
        assert_eq!(EXPECTED_PACKAGE_NAME, "mysql-community-server-core");
        assert_eq!(EXPECTED_PACKAGE_VERSION, "8.4.11-1ubuntu24.04");
        assert_eq!(EXPECTED_APT_REPOSITORY_HOST, "repo.mysql.com");
        assert_eq!(EXPECTED_APT_REPOSITORY_COMPONENT, "mysql-8.4-lts");
    }

    #[test]
    fn package_owner_version_and_apt_origin_are_exact() {
        let path = std::path::Path::new("/usr/sbin/mysqld");
        assert!(
            validate_package_owner(
                "mysql-community-server-core: /usr/sbin/mysqld",
                EXPECTED_PACKAGE_NAME,
                path,
            )
            .is_ok()
        );
        assert!(
            validate_package_owner("other: /usr/sbin/mysqld", EXPECTED_PACKAGE_NAME, path).is_err()
        );
        assert!(
            validate_installed_metadata(
                "mysql-community-server-core\t8.4.11-1ubuntu24.04\tinstall ok installed",
                EXPECTED_PACKAGE_NAME,
                EXPECTED_PACKAGE_VERSION,
            )
            .is_ok()
        );

        let policy = "mysql-community-server-core:\n\
          Installed: 8.4.11-1ubuntu24.04\n\
          Candidate: 8.4.11-1ubuntu24.04\n\
          Version table:\n\
         *** 8.4.11-1ubuntu24.04 500\n\
                500 http://repo.mysql.com/apt/ubuntu noble/mysql-8.4-lts amd64 Packages\n";
        assert!(
            parse_apt_policy(
                policy,
                EXPECTED_PACKAGE_VERSION,
                EXPECTED_APT_REPOSITORY_HOST,
                EXPECTED_APT_REPOSITORY_COMPONENT,
            )
            .is_ok()
        );
        assert!(
            parse_apt_policy(
                policy,
                "8.4.12-1ubuntu24.04",
                EXPECTED_APT_REPOSITORY_HOST,
                EXPECTED_APT_REPOSITORY_COMPONENT,
            )
            .is_err()
        );
        let wrong_stanza = "mysql-community-server-core:\n\
          Installed: 8.4.11-1ubuntu24.04\n\
          Candidate: 8.4.11-1ubuntu24.04\n\
          Version table:\n\
         *** 8.4.11-1ubuntu24.04 500\n\
                500 http://mirror.example.invalid/ubuntu noble/main amd64 Packages\n\
             8.4.12-1ubuntu24.04 500\n\
                500 http://repo.mysql.com/apt/ubuntu noble/mysql-8.4-lts amd64 Packages\n";
        assert!(
            parse_apt_policy(
                wrong_stanza,
                EXPECTED_PACKAGE_VERSION,
                EXPECTED_APT_REPOSITORY_HOST,
                EXPECTED_APT_REPOSITORY_COMPONENT,
            )
            .is_err()
        );
        assert!(
            parse_apt_policy(
                policy,
                EXPECTED_PACKAGE_VERSION,
                "packages.example.invalid",
                EXPECTED_APT_REPOSITORY_COMPONENT,
            )
            .is_err()
        );
    }

    #[test]
    fn private_directories_override_umask_and_reject_broad_modes() {
        let root = super::super::absolute_test_path("private-directory-mode");
        let _ = std::fs::remove_dir_all(&root);
        create_private_directory(&root).unwrap();
        verify_private_mode(&root).unwrap();
        std::fs::set_permissions(
            &root,
            <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o750),
        )
        .unwrap();
        assert!(verify_private_mode(&root).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn process_preflight_rejects_foreign_mysqld_only() {
        assert!(reject_foreign_process_names(&["cargo".to_owned()]).is_ok());
        assert!(reject_foreign_process_names(&["mysqld".to_owned()]).is_err());
        assert!(reject_foreign_process_names(&["mysqld-debug".to_owned()]).is_err());
    }
}
