#![allow(dead_code)]

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File};
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestPath(PathBuf);

impl ManifestPath {
    pub fn from_env(name: &str) -> Result<Self, QualificationError> {
        Self::from_os_value(std::env::var_os(name), name)
    }

    pub fn from_os_value(value: Option<OsString>, name: &str) -> Result<Self, QualificationError> {
        let Some(value) = value else {
            return Err(QualificationError::new(
                QualificationCode::MissingManifestPath,
                "manifest env",
                format!("{name} must be set to one absolute manifest path"),
            ));
        };
        let path = PathBuf::from(value);
        if !path.is_absolute() {
            return Err(QualificationError::new(
                QualificationCode::ManifestPathNotAbsolute,
                "manifest env",
                format!("{name} must be absolute: {}", path.display()),
            ));
        }
        Ok(Self(path))
    }

    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QualificationManifest {
    pub manifest_path: PathBuf,
    pub mysqld_path: PathBuf,
    pub apparmor_exec_path: PathBuf,
    pub package_name: String,
    pub package_version: String,
    pub apt_repository_host: String,
    pub apt_repository_component: String,
    pub expected_mysqld_sha256: String,
    pub qualification_root: PathBuf,
    pub output_record_path: PathBuf,
    pub validation_receipt_dir: PathBuf,
    pub feature_graph_path: PathBuf,
    pub startup_timeout_ms: u64,
    pub observation_timeout_ms: u64,
    pub cleanup_timeout_ms: u64,
}

impl QualificationManifest {
    pub fn load(path: &Path) -> Result<Self, QualificationError> {
        if !path.is_absolute() {
            return Err(QualificationError::new(
                QualificationCode::ManifestPathNotAbsolute,
                "manifest path",
                format!("manifest path must be absolute: {}", path.display()),
            ));
        }
        let text = fs::read_to_string(path).map_err(|error| {
            QualificationError::new(
                QualificationCode::ManifestReadFailure,
                "read manifest",
                format!("{}: {error}", path.display()),
            )
        })?;
        let mut values = parse_manifest_map(&text)?;
        let manifest = Self {
            manifest_path: path.to_path_buf(),
            mysqld_path: parse_absolute_path(&mut values, "mysqld_path")?,
            apparmor_exec_path: parse_absolute_path(&mut values, "apparmor_exec_path")?,
            package_name: take_required(&mut values, "package_name")?,
            package_version: take_required(&mut values, "package_version")?,
            apt_repository_host: take_required(&mut values, "apt_repository_host")?,
            apt_repository_component: take_required(&mut values, "apt_repository_component")?,
            expected_mysqld_sha256: normalize_hex(
                &take_required(&mut values, "expected_mysqld_sha256")?,
                64,
                QualificationCode::ManifestSyntax,
                "mysqld digest",
            )?,
            qualification_root: parse_project_local_root(&mut values, "qualification_root")?,
            output_record_path: parse_project_local_output(&mut values, "output_record_path")?,
            validation_receipt_dir: parse_project_local_root(
                &mut values,
                "validation_receipt_dir",
            )?,
            feature_graph_path: parse_project_local_root(&mut values, "feature_graph_path")?,
            startup_timeout_ms: parse_u64(&mut values, "startup_timeout_ms")?,
            observation_timeout_ms: parse_u64(&mut values, "observation_timeout_ms")?,
            cleanup_timeout_ms: parse_u64(&mut values, "cleanup_timeout_ms")?,
        };
        if !values.is_empty() {
            let keys = values.keys().cloned().collect::<Vec<_>>().join(", ");
            return Err(QualificationError::new(
                QualificationCode::ManifestSyntax,
                "manifest keys",
                format!("unexpected keys: {keys}"),
            ));
        }
        manifest.validate()?;
        Ok(manifest)
    }

    fn validate(&self) -> Result<(), QualificationError> {
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
        if self
            .output_record_path
            .starts_with(self.qualification_root.join("work"))
        {
            return Err(QualificationError::new(
                QualificationCode::NonAbsoluteInput,
                "output path",
                "output_record_path must not be inside qualification_root/work".to_owned(),
            ));
        }
        if !self.validation_receipt_dir.is_dir() || !self.feature_graph_path.is_file() {
            return Err(QualificationError::new(
                QualificationCode::MissingInput,
                "validation receipts",
                "validation_receipt_dir and feature_graph_path must exist".to_owned(),
            ));
        }
        if self.startup_timeout_ms == 0
            || self.observation_timeout_ms == 0
            || self.cleanup_timeout_ms == 0
        {
            return Err(QualificationError::new(
                QualificationCode::ManifestSyntax,
                "timeouts",
                "timeouts must be positive millisecond values".to_owned(),
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
pub struct SetupReceipt {
    pub package_name: String,
    pub package_version: String,
    pub package_owner_verified: bool,
    pub package_files_verified: bool,
    pub apt_installed_version: String,
    pub apt_candidate_version: String,
    pub apt_repository: String,
    pub mysqld_sha256: String,
    pub mysqld_path: PathBuf,
    pub process_launcher: PathBuf,
    pub init_exit_code: i32,
    pub process_id: u32,
    pub socket_path: PathBuf,
    pub socket_device: u64,
    pub socket_inode: u64,
    pub private_directory_mode: u32,
    pub private_directories_verified: bool,
    pub product_version: Option<String>,
    pub version_comment: Option<String>,
    pub version_compile_machine: Option<String>,
    pub version_compile_os: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupReceipt {
    pub exit_code: Option<i32>,
    pub signal: Option<&'static str>,
    pub removed_paths: Vec<PathBuf>,
    pub work_root_removed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Layout {
    work_root: PathBuf,
    runtime_root: PathBuf,
    data_root: PathBuf,
}

impl Layout {
    fn create(manifest: &QualificationManifest) -> Result<Self, QualificationError> {
        let work_root = manifest.qualification_root.join("work");
        if work_root.exists() {
            fs::remove_dir_all(&work_root).map_err(|error| {
                QualificationError::new(
                    QualificationCode::CleanupFailure,
                    "remove stale work root",
                    format!("{}: {error}", work_root.display()),
                )
            })?;
        }
        fs::create_dir_all(&manifest.qualification_root).map_err(|error| {
            QualificationError::new(
                QualificationCode::UnwritableOutput,
                "qualification root",
                format!("{}: {error}", manifest.qualification_root.display()),
            )
        })?;
        let output_parent = manifest
            .output_record_path
            .parent()
            .expect("absolute output path has parent");
        fs::create_dir_all(output_parent).map_err(|error| {
            QualificationError::new(
                QualificationCode::UnwritableOutput,
                "output parent",
                format!("{}: {error}", output_parent.display()),
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
    manifest: QualificationManifest,
    layout: Layout,
    package: PackageEvidence,
    mysqld_sha256: String,
    mysqld_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PackageEvidence {
    name: String,
    version: String,
    owner_verified: bool,
    files_verified: bool,
    apt_installed_version: String,
    apt_candidate_version: String,
    apt_repository: String,
}

impl PreparedArtifact {
    pub fn prepare(manifest: &QualificationManifest) -> Result<Self, QualificationError> {
        verify_platform()?;
        let layout = Layout::create(manifest)?;
        let verified = verify_installed_package(manifest);
        let (package, mysqld_sha256, mysqld_path) = match verified {
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
            manifest: manifest.clone(),
            layout,
            package,
            mysqld_sha256,
            mysqld_path,
        })
    }

    pub async fn launch(self) -> Result<RunningFixture, QualificationError> {
        let config = match ServerConfig::new(&self.layout) {
            Ok(config) => config,
            Err(error) => return Err(cleanup_unlaunched(&self.layout, error)),
        };
        if let Err(error) = fs::write(&config.config_path, config.render(&self.layout.data_root)) {
            return Err(cleanup_unlaunched(
                &self.layout,
                QualificationError::new(
                    QualificationCode::InitializationFailure,
                    "write mysqld config",
                    format!("{}: {error}", config.config_path.display()),
                ),
            ));
        }

        let init_status = match mysqld_command(&self.manifest, &self.mysqld_path)
            .arg(format!("--defaults-file={}", config.config_path.display()))
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

        let stdout = match File::create(&config.stdout_log_path) {
            Ok(stdout) => stdout,
            Err(error) => {
                return Err(cleanup_unlaunched(
                    &self.layout,
                    QualificationError::new(
                        QualificationCode::LaunchFailure,
                        "open mysqld stdout log",
                        format!("{}: {error}", config.stdout_log_path.display()),
                    ),
                ));
            }
        };
        let stderr = match File::create(&config.error_log_path) {
            Ok(stderr) => stderr,
            Err(error) => {
                return Err(cleanup_unlaunched(
                    &self.layout,
                    QualificationError::new(
                        QualificationCode::LaunchFailure,
                        "open mysqld stderr log",
                        format!("{}: {error}", config.error_log_path.display()),
                    ),
                ));
            }
        };

        let mut child = match mysqld_command(&self.manifest, &self.mysqld_path)
            .arg(format!("--defaults-file={}", config.config_path.display()))
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
            &config.socket_path,
            self.manifest.startup_timeout(),
        )
        .await
        {
            return Err(cleanup_failed_launch(&mut child, &self.layout, error));
        }

        let socket_metadata = match fs::metadata(&config.socket_path) {
            Ok(metadata) => metadata,
            Err(error) => {
                let launch_error = QualificationError::new(
                    QualificationCode::LaunchFailure,
                    "stat ready socket",
                    format!("{}: {error}", config.socket_path.display()),
                );
                return Err(cleanup_failed_launch(
                    &mut child,
                    &self.layout,
                    launch_error,
                ));
            }
        };

        let manifest = self.manifest;
        let layout = self.layout;
        let mysqld_path = self.mysqld_path.clone();
        let process_id = child.id();
        let socket_path = config.socket_path.clone();
        let setup_receipt = SetupReceipt {
            package_name: self.package.name,
            package_version: self.package.version,
            package_owner_verified: self.package.owner_verified,
            package_files_verified: self.package.files_verified,
            apt_installed_version: self.package.apt_installed_version,
            apt_candidate_version: self.package.apt_candidate_version,
            apt_repository: self.package.apt_repository,
            mysqld_sha256: self.mysqld_sha256,
            mysqld_path: mysqld_path.clone(),
            process_launcher: manifest.apparmor_exec_path.clone(),
            init_exit_code: init_status.code().unwrap_or_default(),
            process_id,
            socket_path,
            socket_device: socket_metadata.dev(),
            socket_inode: socket_metadata.ino(),
            private_directory_mode: 0o700,
            private_directories_verified: true,
            product_version: None,
            version_comment: None,
            version_compile_machine: None,
            version_compile_os: None,
        };

        Ok(RunningFixture {
            manifest,
            layout,
            config,
            child,
            setup_receipt,
        })
    }
}

fn mysqld_command(manifest: &QualificationManifest, mysqld_path: &Path) -> Command {
    let mut command = Command::new(&manifest.apparmor_exec_path);
    command
        .arg("-p")
        .arg("unconfined")
        .arg("--")
        .arg(mysqld_path);
    command
}

fn verify_installed_package(
    manifest: &QualificationManifest,
) -> Result<(PackageEvidence, String, PathBuf), QualificationError> {
    let mysqld_path = manifest.mysqld_path.clone();
    let owner = run_tool(
        "dpkg-query",
        &[OsStr::new("-S"), mysqld_path.as_os_str()],
        QualificationCode::ToolUnavailable,
        "query mysqld package owner",
    )?;
    let owner_text = String::from_utf8_lossy(&owner.stdout).trim().to_owned();
    validate_package_owner(&owner_text, &manifest.package_name, &mysqld_path)?;

    let query_format = "${Package}\t${Version}\t${Status}";
    let installed = run_tool(
        "dpkg-query",
        &[
            OsStr::new("-W"),
            OsStr::new("-f"),
            OsStr::new(query_format),
            OsStr::new(&manifest.package_name),
        ],
        QualificationCode::ToolUnavailable,
        "query installed package version",
    )?;
    let installed_text = String::from_utf8_lossy(&installed.stdout);
    validate_installed_metadata(
        installed_text.trim(),
        &manifest.package_name,
        &manifest.package_version,
    )?;

    verify_dpkg_files(&manifest.package_name)?;
    let apt = verify_apt_policy(manifest)?;

    let mysqld_sha256 = compute_sha256(&mysqld_path)?;
    verify_match(
        "mysqld sha256",
        &manifest.expected_mysqld_sha256,
        &mysqld_sha256,
        QualificationCode::DigestMismatch,
    )?;
    verify_runtime_libraries(&mysqld_path)?;

    Ok((
        PackageEvidence {
            name: manifest.package_name.clone(),
            version: manifest.package_version.clone(),
            owner_verified: true,
            files_verified: true,
            apt_installed_version: apt.installed,
            apt_candidate_version: apt.candidate,
            apt_repository: apt.repository,
        },
        mysqld_sha256,
        mysqld_path,
    ))
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

fn verify_apt_policy(manifest: &QualificationManifest) -> Result<AptPolicy, QualificationError> {
    let output = run_tool(
        "apt-cache",
        &[OsStr::new("policy"), OsStr::new(&manifest.package_name)],
        QualificationCode::ToolUnavailable,
        "query APT package policy",
    )?;
    let policy = String::from_utf8_lossy(&output.stdout);
    parse_apt_policy(
        &policy,
        &manifest.package_version,
        &manifest.apt_repository_host,
        &manifest.apt_repository_component,
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
    manifest: QualificationManifest,
    layout: Layout,
    config: ServerConfig,
    child: Child,
    setup_receipt: SetupReceipt,
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
    pub fn manifest(&self) -> &QualificationManifest {
        &self.manifest
    }

    pub fn socket_path(&self) -> &Path {
        &self.config.socket_path
    }

    pub fn sql_port(&self) -> u16 {
        self.config.sql_port
    }

    pub fn group_port(&self) -> u16 {
        self.config.group_port
    }

    pub fn group_name(&self) -> &str {
        &self.config.group_name
    }

    pub fn setup_receipt(&self) -> &SetupReceipt {
        &self.setup_receipt
    }

    pub fn note_product_identity(
        &mut self,
        version: &str,
        comment: &str,
        machine: &str,
        operating_system: &str,
    ) {
        self.setup_receipt.product_version = Some(version.to_owned());
        self.setup_receipt.version_comment = Some(comment.to_owned());
        self.setup_receipt.version_compile_machine = Some(machine.to_owned());
        self.setup_receipt.version_compile_os = Some(operating_system.to_owned());
    }

    pub async fn connect_root(&self) -> Result<Conn, QualificationError> {
        self.connect_as("root", None).await
    }

    pub async fn connect_as(
        &self,
        username: &str,
        password: Option<&str>,
    ) -> Result<Conn, QualificationError> {
        open_connection(&self.config.socket_path, username, password)
            .await
            .map_err(|error| {
                QualificationError::new(
                    QualificationCode::AccountStateSetupFailure,
                    "connect local mysqld",
                    format!("user={username}: {error}"),
                )
            })
    }

    pub fn cleanup(mut self) -> Result<CleanupReceipt, QualificationError> {
        let mut signal = None;
        let mut status = self.child.try_wait().map_err(|error| {
            QualificationError::new(
                QualificationCode::CleanupFailure,
                "check mysqld status",
                error.to_string(),
            )
        })?;
        if status.is_none() {
            send_signal(self.child.id(), "TERM")?;
            signal = Some("TERM");
            let deadline = Instant::now() + self.manifest.cleanup_timeout();
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
            signal = Some("KILL");
            status = Some(self.child.wait().map_err(|error| {
                QualificationError::new(
                    QualificationCode::CleanupFailure,
                    "reap mysqld",
                    error.to_string(),
                )
            })?);
        }

        let mut removed_paths = Vec::new();
        for path in [&self.layout.data_root, &self.layout.runtime_root] {
            if path.exists() {
                fs::remove_dir_all(path).map_err(|error| {
                    QualificationError::new(
                        QualificationCode::CleanupFailure,
                        "remove qualification directory",
                        format!("{}: {error}", path.display()),
                    )
                })?;
                removed_paths.push(path.clone());
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
            removed_paths.push(self.layout.work_root.clone());
        }

        Ok(CleanupReceipt {
            exit_code: status.and_then(|status| status.code()),
            signal,
            removed_paths,
            work_root_removed: !self.layout.work_root.exists(),
        })
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

fn compute_sha256(path: &Path) -> Result<String, QualificationError> {
    let output = run_tool(
        "sha256sum",
        &[path.as_os_str()],
        QualificationCode::ToolUnavailable,
        "compute sha256",
    )?;
    let text = String::from_utf8_lossy(&output.stdout);
    let digest = text.split_whitespace().next().ok_or_else(|| {
        QualificationError::new(
            QualificationCode::DigestMismatch,
            "sha256 output",
            format!("missing digest output for {}", path.display()),
        )
    })?;
    normalize_hex(
        digest,
        64,
        QualificationCode::DigestMismatch,
        "sha256 output",
    )
}

fn verify_match(
    label: &'static str,
    expected: &str,
    actual: &str,
    code: QualificationCode,
) -> Result<(), QualificationError> {
    if expected == actual {
        Ok(())
    } else {
        Err(QualificationError::new(
            code,
            label,
            format!("expected {expected}, found {actual}"),
        ))
    }
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

fn parse_manifest_map(text: &str) -> Result<BTreeMap<String, String>, QualificationError> {
    let mut values = BTreeMap::new();
    for (index, raw_line) in text.lines().enumerate() {
        let line = raw_line.split('#').next().unwrap_or_default().trim();
        if line.is_empty() {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(QualificationError::new(
                QualificationCode::ManifestSyntax,
                "manifest line",
                format!("line {} is not key = value", index + 1),
            ));
        };
        let key = key.trim().to_owned();
        if values.contains_key(&key) {
            return Err(QualificationError::new(
                QualificationCode::ManifestSyntax,
                "manifest line",
                format!("duplicate key on line {}: {key}", index + 1),
            ));
        }
        values.insert(key, parse_manifest_value(value.trim(), index + 1)?);
    }
    Ok(values)
}

fn parse_manifest_value(value: &str, line: usize) -> Result<String, QualificationError> {
    if let Some(stripped) = value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
    {
        let mut rendered = String::new();
        let mut escape = false;
        for character in stripped.chars() {
            if escape {
                rendered.push(match character {
                    '"' | '\\' => character,
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    other => {
                        return Err(QualificationError::new(
                            QualificationCode::ManifestSyntax,
                            "manifest string",
                            format!("unsupported escape \\{other} on line {line}"),
                        ));
                    }
                });
                escape = false;
            } else if character == '\\' {
                escape = true;
            } else {
                rendered.push(character);
            }
        }
        if escape {
            return Err(QualificationError::new(
                QualificationCode::ManifestSyntax,
                "manifest string",
                format!("dangling escape on line {line}"),
            ));
        }
        Ok(rendered)
    } else if value.starts_with('"') || value.ends_with('"') {
        Err(QualificationError::new(
            QualificationCode::ManifestSyntax,
            "manifest string",
            format!("unterminated quoted string on line {line}"),
        ))
    } else {
        Ok(value.to_owned())
    }
}

fn take_required(
    values: &mut BTreeMap<String, String>,
    key: &str,
) -> Result<String, QualificationError> {
    values.remove(key).ok_or_else(|| {
        QualificationError::new(
            QualificationCode::ManifestSyntax,
            "manifest keys",
            format!("missing required key {key}"),
        )
    })
}

fn parse_absolute_path(
    values: &mut BTreeMap<String, String>,
    key: &str,
) -> Result<PathBuf, QualificationError> {
    normalize_project_path(&take_required(values, key)?, key, false)
}

fn parse_project_local_root(
    values: &mut BTreeMap<String, String>,
    key: &str,
) -> Result<PathBuf, QualificationError> {
    let path = normalize_project_path(&take_required(values, key)?, key, true)?;
    let required_root = project_root().join("qualification");
    if !path.starts_with(&required_root) {
        return Err(QualificationError::new(
            QualificationCode::NonAbsoluteInput,
            "project-local qualification path",
            format!(
                "{key}={} must stay beneath {}",
                path.display(),
                required_root.display()
            ),
        ));
    }
    Ok(path)
}

fn parse_project_local_output(
    values: &mut BTreeMap<String, String>,
    key: &str,
) -> Result<PathBuf, QualificationError> {
    parse_project_local_root(values, key)
}

fn normalize_project_path(
    raw: &str,
    key: &str,
    require_project_local: bool,
) -> Result<PathBuf, QualificationError> {
    let path = Path::new(raw);
    if !path.is_absolute() {
        return Err(QualificationError::new(
            QualificationCode::NonAbsoluteInput,
            "absolute path input",
            format!("{key} must be absolute: {raw}"),
        ));
    }
    let mut normalized = PathBuf::from("/");
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(value) => normalized.push(value),
            Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) => {
                return Err(QualificationError::new(
                    QualificationCode::NonAbsoluteInput,
                    "absolute path input",
                    format!("{key} must not contain parent traversal: {raw}"),
                ));
            }
        }
    }
    if require_project_local {
        let root = project_root();
        if !normalized.starts_with(&root) {
            return Err(QualificationError::new(
                QualificationCode::NonAbsoluteInput,
                "project-local path",
                format!("{key} must stay beneath {}", root.display()),
            ));
        }
    }
    Ok(normalized)
}

fn parse_u64(values: &mut BTreeMap<String, String>, key: &str) -> Result<u64, QualificationError> {
    let value = take_required(values, key)?;
    value.parse::<u64>().map_err(|error| {
        QualificationError::new(
            QualificationCode::ManifestSyntax,
            "integer manifest field",
            format!("{key} must be an integer: {error}"),
        )
    })
}

pub(crate) fn normalize_hex(
    raw: &str,
    expected_len: usize,
    code: QualificationCode,
    context: &'static str,
) -> Result<String, QualificationError> {
    let normalized = raw.trim().to_ascii_uppercase();
    if normalized.len() != expected_len || !normalized.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(QualificationError::new(
            code,
            context,
            format!("expected {expected_len} hexadecimal characters, found {raw}"),
        ));
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::{
        EXPECTED_APT_REPOSITORY_COMPONENT, EXPECTED_APT_REPOSITORY_HOST, EXPECTED_PACKAGE_NAME,
        EXPECTED_PACKAGE_VERSION, ManifestPath, create_private_directory, normalize_hex,
        normalize_project_path, parse_apt_policy, parse_manifest_map, validate_installed_metadata,
        validate_package_owner, verify_match, verify_private_mode,
    };

    #[test]
    fn missing_and_non_absolute_manifest_paths_are_rejected() {
        let missing = ManifestPath::from_os_value(None, "KUBERIC_MYSQL_8_4_11_MANIFEST");
        assert!(missing.is_err());
        let relative = ManifestPath::from_os_value(
            Some("relative.toml".into()),
            "KUBERIC_MYSQL_8_4_11_MANIFEST",
        );
        assert!(relative.is_err());
    }

    #[test]
    fn manifest_requires_absolute_project_local_paths() {
        let root = super::project_root();
        let allowed = normalize_project_path(
            &root.join("qualification/tests/live").display().to_string(),
            "qualification_root",
            true,
        )
        .unwrap();
        assert!(allowed.starts_with(root.join("qualification")));

        let rejected = normalize_project_path(
            &root.join("../outside").display().to_string(),
            "qualification_root",
            true,
        );
        assert!(rejected.is_err());

        assert_eq!(EXPECTED_PACKAGE_NAME, "mysql-community-server-core");
        assert_eq!(EXPECTED_PACKAGE_VERSION, "8.4.11-1ubuntu24.04");
        assert_eq!(EXPECTED_APT_REPOSITORY_HOST, "repo.mysql.com");
        assert_eq!(EXPECTED_APT_REPOSITORY_COMPONENT, "mysql-8.4-lts");
    }

    #[test]
    fn malformed_manifest_syntax_is_rejected() {
        assert!(parse_manifest_map("mysqld_path").is_err());
        assert!(parse_manifest_map("mysqld_path = \"/one\"\nmysqld_path = \"/two\"\n").is_err());
        assert!(parse_manifest_map("mysqld_path = \"unterminated").is_err());
    }

    #[test]
    fn digest_and_package_match_helpers_are_exact() {
        let digest = normalize_hex("aa", 64, super::QualificationCode::DigestMismatch, "digest");
        assert!(digest.is_err());
        let digest = normalize_hex(
            &"a".repeat(64),
            64,
            super::QualificationCode::DigestMismatch,
            "digest",
        )
        .unwrap();
        assert_eq!(digest, "A".repeat(64));

        let mismatch = verify_match(
            "mysqld digest",
            "A".repeat(64).as_str(),
            "B".repeat(64).as_str(),
            super::QualificationCode::DigestMismatch,
        );
        assert!(mismatch.is_err());
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
}
