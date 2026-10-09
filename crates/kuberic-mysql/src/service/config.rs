//! Validated process and filesystem configuration.

use std::fs;
use std::net::SocketAddr;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant};

use crate::core::ServerUuid;
use crate::service::ConfigError;

const CONFIG_FILE: &str = "my.cnf";
const SOCKET_FILE: &str = "mysql.sock";
const PID_FILE: &str = "mysqld.pid";
const TOPOLOGY_MEMBER_COUNT: usize = 3;

/// One exact position in the fixed three-member topology.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MysqlMemberIndex {
    /// The first configured member.
    First,
    /// The second configured member.
    Second,
    /// The third configured member.
    Third,
}

impl MysqlMemberIndex {
    const fn as_usize(self) -> usize {
        match self {
            Self::First => 0,
            Self::Second => 1,
            Self::Third => 2,
        }
    }
}

/// One member's explicit native topology values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlMemberConfig {
    server_id: u32,
    sql_address: SocketAddr,
    group_replication_address: SocketAddr,
    group_uuid: String,
    group_seeds: [SocketAddr; 3],
}

impl MysqlMemberConfig {
    /// Validates one member's nonzero ID, loopback addresses, group UUID, and
    /// three distinct loopback seeds.
    pub fn new(
        server_id: u32,
        sql_address: SocketAddr,
        group_replication_address: SocketAddr,
        group_uuid: impl AsRef<str>,
        mut group_seeds: [SocketAddr; 3],
    ) -> Result<Self, ConfigError> {
        if server_id == 0 {
            return Err(ConfigError::ZeroServerId);
        }
        validate_sql_address(sql_address)?;
        validate_group_replication_address(group_replication_address)?;
        for seed in group_seeds {
            validate_group_replication_seed(seed)?;
        }
        group_seeds.sort_unstable();
        if group_seeds.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ConfigError::DuplicateGroupReplicationSeed);
        }
        let group_uuid = ServerUuid::new(group_uuid)
            .map_err(|_| ConfigError::InvalidGroupUuid)?
            .to_string();
        Ok(Self {
            server_id,
            sql_address,
            group_replication_address,
            group_uuid,
            group_seeds,
        })
    }

    /// Unique native server ID.
    #[must_use]
    pub const fn server_id(&self) -> u32 {
        self.server_id
    }

    /// Loopback SQL address reported in native membership evidence.
    #[must_use]
    pub const fn sql_address(&self) -> SocketAddr {
        self.sql_address
    }

    /// Loopback Group Replication transport address.
    #[must_use]
    pub const fn group_replication_address(&self) -> SocketAddr {
        self.group_replication_address
    }

    /// Canonical lowercase Group Replication group UUID.
    #[must_use]
    pub fn group_uuid(&self) -> &str {
        &self.group_uuid
    }

    /// Canonically ordered exact three-member seed set.
    #[must_use]
    pub const fn group_seeds(&self) -> &[SocketAddr; 3] {
        &self.group_seeds
    }
}

/// Validated fixed three-member Group Replication topology.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlTopologyConfig {
    members: [MysqlMemberConfig; 3],
}

impl MysqlTopologyConfig {
    /// Validates unique member values, one shared group UUID, and an exact
    /// three-member seed set on every member.
    pub fn new(members: [MysqlMemberConfig; 3]) -> Result<Self, ConfigError> {
        for left in 0..TOPOLOGY_MEMBER_COUNT {
            for right in (left + 1)..TOPOLOGY_MEMBER_COUNT {
                if members[left].server_id == members[right].server_id {
                    return Err(ConfigError::DuplicateServerId);
                }
                if members[left].sql_address == members[right].sql_address {
                    return Err(ConfigError::DuplicateSqlAddress);
                }
                if members[left].group_replication_address
                    == members[right].group_replication_address
                {
                    return Err(ConfigError::DuplicateGroupReplicationAddress);
                }
            }
        }
        if members[1..]
            .iter()
            .any(|member| member.group_uuid != members[0].group_uuid)
        {
            return Err(ConfigError::MismatchedGroupUuid);
        }

        let mut expected_seeds = [
            members[0].group_replication_address,
            members[1].group_replication_address,
            members[2].group_replication_address,
        ];
        expected_seeds.sort_unstable();
        if members
            .iter()
            .any(|member| member.group_seeds != expected_seeds)
        {
            return Err(ConfigError::GroupReplicationSeedSetMismatch);
        }
        Ok(Self { members })
    }

    /// The exact three configured members.
    #[must_use]
    pub const fn members(&self) -> &[MysqlMemberConfig; 3] {
        &self.members
    }
}

/// Positive deadlines for one process generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MysqlOperationTimeouts {
    /// Product inspection and `--initialize-insecure` deadline.
    initialization: Duration,
    /// Socket readiness and ownership-attestation deadline.
    startup: Duration,
    /// TERM/KILL/reap deadline.
    shutdown: Duration,
    /// Socket disappearance deadline after reaping.
    socket_disappearance: Duration,
}

impl MysqlOperationTimeouts {
    /// Validates positive operation deadlines.
    pub fn new(
        initialization: Duration,
        startup: Duration,
        shutdown: Duration,
        socket_disappearance: Duration,
    ) -> Result<Self, ConfigError> {
        if [initialization, startup, shutdown, socket_disappearance]
            .into_iter()
            .any(|timeout| timeout.is_zero())
        {
            return Err(ConfigError::ZeroTimeout);
        }
        let timeouts = Self {
            initialization,
            startup,
            shutdown,
            socket_disappearance,
        };
        timeouts.validate()?;
        Ok(timeouts)
    }

    /// Product inspection and initialization deadline.
    #[must_use]
    pub const fn initialization(self) -> Duration {
        self.initialization
    }

    /// Startup readiness deadline.
    #[must_use]
    pub const fn startup(self) -> Duration {
        self.startup
    }

    /// End-to-end TERM, KILL, and reap deadline.
    #[must_use]
    pub const fn shutdown(self) -> Duration {
        self.shutdown
    }

    /// Socket disappearance deadline.
    #[must_use]
    pub const fn socket_disappearance(self) -> Duration {
        self.socket_disappearance
    }

    fn validate(self) -> Result<(), ConfigError> {
        if [
            self.initialization,
            self.startup,
            self.shutdown,
            self.socket_disappearance,
        ]
        .into_iter()
        .any(|timeout| Instant::now().checked_add(timeout).is_none())
        {
            return Err(ConfigError::UnrepresentableTimeout);
        }
        Ok(())
    }
}

/// Deterministic generated paths beneath disposable scratch storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlRuntimePaths {
    scratch_root: PathBuf,
    config: PathBuf,
    socket: PathBuf,
    pid: PathBuf,
    error_log: PathBuf,
    stdout_log: PathBuf,
    init_stdout_log: PathBuf,
    init_stderr_log: PathBuf,
    version_log: PathBuf,
    temporary: PathBuf,
    secure_files: PathBuf,
    binary_log: PathBuf,
    relay_log: PathBuf,
}

impl MysqlRuntimePaths {
    fn new(scratch_root: PathBuf) -> Self {
        Self {
            config: scratch_root.join(CONFIG_FILE),
            socket: scratch_root.join(SOCKET_FILE),
            pid: scratch_root.join(PID_FILE),
            error_log: scratch_root.join("mysqld.err"),
            stdout_log: scratch_root.join("mysqld.out"),
            init_stdout_log: scratch_root.join("initialize.out"),
            init_stderr_log: scratch_root.join("initialize.err"),
            version_log: scratch_root.join("version.out"),
            temporary: scratch_root.join("tmp"),
            secure_files: scratch_root.join("secure-files"),
            binary_log: scratch_root.join("mysql-bin"),
            relay_log: scratch_root.join("relay-bin"),
            scratch_root,
        }
    }

    /// Disposable root containing every generated runtime artifact.
    #[must_use]
    pub fn scratch_root(&self) -> &Path {
        &self.scratch_root
    }

    /// Generated MySQL configuration path.
    #[must_use]
    pub fn config(&self) -> &Path {
        &self.config
    }

    /// Private Unix-domain socket path.
    #[must_use]
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Retained-child PID file path.
    #[must_use]
    pub fn pid(&self) -> &Path {
        &self.pid
    }

    pub(crate) fn error_log(&self) -> &Path {
        &self.error_log
    }

    pub(crate) fn stdout_log(&self) -> &Path {
        &self.stdout_log
    }

    pub(crate) fn init_stdout_log(&self) -> &Path {
        &self.init_stdout_log
    }

    pub(crate) fn init_stderr_log(&self) -> &Path {
        &self.init_stderr_log
    }

    pub(crate) fn version_log(&self) -> &Path {
        &self.version_log
    }
}

/// Exact executable, storage, and timeout configuration for one fresh
/// generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlInstanceConfig {
    mysqld: PathBuf,
    mysqld_identity: FileIdentity,
    launcher: PathBuf,
    launcher_identity: FileIdentity,
    data_root: PathBuf,
    runtime: MysqlRuntimePaths,
    topology: MysqlTopologyConfig,
    member_index: MysqlMemberIndex,
    timeouts: MysqlOperationTimeouts,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OwnedRoot {
    path: PathBuf,
    device: u64,
    inode: u64,
    uid: u32,
    mode: u32,
    parent_device: u64,
    parent_inode: u64,
}

impl OwnedRoot {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) const fn device(&self) -> u64 {
        self.device
    }

    pub(crate) const fn inode(&self) -> u64 {
        self.inode
    }

    pub(crate) const fn parent_device(&self) -> u64 {
        self.parent_device
    }

    pub(crate) const fn parent_inode(&self) -> u64 {
        self.parent_inode
    }

    pub(crate) fn revalidate(&self) -> Result<(), crate::service::OwnershipError> {
        let metadata = fs::symlink_metadata(&self.path)
            .map_err(|_| crate::service::OwnershipError::RootMismatch)?;
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || metadata.dev() != self.device
            || metadata.ino() != self.inode
            || metadata.uid() != self.uid
            || metadata.permissions().mode() & 0o777 != self.mode
        {
            return Err(crate::service::OwnershipError::RootMismatch);
        }
        Ok(())
    }
}

pub(crate) struct OwnedRoots {
    pub(crate) data: OwnedRoot,
    pub(crate) scratch: OwnedRoot,
}

pub(crate) struct LayoutCreationError {
    pub(crate) error: ConfigError,
    pub(crate) created_roots: Vec<OwnedRoot>,
}

impl MysqlInstanceConfig {
    /// Validates exact executable paths, one member of an exact topology, and
    /// two distinct, absent fresh roots.
    pub fn new(
        mysqld: impl Into<PathBuf>,
        launcher: impl Into<PathBuf>,
        data_root: impl Into<PathBuf>,
        scratch_root: impl Into<PathBuf>,
        topology: MysqlTopologyConfig,
        member_index: MysqlMemberIndex,
        timeouts: MysqlOperationTimeouts,
    ) -> Result<Self, ConfigError> {
        if std::env::consts::OS != "linux" || std::env::consts::ARCH != "x86_64" {
            return Err(ConfigError::UnsupportedPlatform);
        }
        let mysqld = mysqld.into();
        let launcher = launcher.into();
        let data_root = data_root.into();
        let scratch_root = scratch_root.into();
        timeouts.validate()?;

        let mysqld_identity = validate_executable(&mysqld)?;
        let launcher_identity = validate_executable(&launcher)?;
        validate_path_shape(&data_root)?;
        validate_path_shape(&scratch_root)?;
        if scratch_root.join(SOCKET_FILE).as_os_str().as_bytes().len() >= 108 {
            return Err(ConfigError::PathNotRepresentable);
        }
        if paths_overlap(&data_root, &scratch_root) {
            return Err(ConfigError::RootOverlap);
        }
        validate_fresh_root(&data_root)?;
        validate_fresh_root(&scratch_root)?;

        Ok(Self {
            mysqld,
            mysqld_identity,
            launcher,
            launcher_identity,
            data_root,
            runtime: MysqlRuntimePaths::new(scratch_root),
            topology,
            member_index,
            timeouts,
        })
    }

    /// Exact server executable selected by the caller.
    #[must_use]
    pub fn mysqld(&self) -> &Path {
        &self.mysqld
    }

    /// Exact process launcher selected by the caller.
    #[must_use]
    pub fn launcher(&self) -> &Path {
        &self.launcher
    }

    /// Persistent fresh MySQL data root.
    #[must_use]
    pub fn data_root(&self) -> &Path {
        &self.data_root
    }

    /// Generated runtime layout beneath disposable scratch.
    #[must_use]
    pub const fn runtime(&self) -> &MysqlRuntimePaths {
        &self.runtime
    }

    /// Validated fixed topology shared by this process generation.
    #[must_use]
    pub const fn topology(&self) -> &MysqlTopologyConfig {
        &self.topology
    }

    /// This process generation's exact topology member.
    #[must_use]
    pub fn member(&self) -> &MysqlMemberConfig {
        &self.topology.members[self.member_index.as_usize()]
    }

    /// Positive operation deadlines.
    #[must_use]
    pub const fn timeouts(&self) -> MysqlOperationTimeouts {
        self.timeouts
    }

    /// Renders the deterministic exact-target server configuration.
    #[must_use]
    pub fn render_server_config(&self) -> String {
        let member = self.member();
        let seeds = member.group_seeds.map(|seed| seed.to_string()).join(",");
        format!(
            "[mysqld]\n\
             datadir={}\n\
             socket={}\n\
             pid-file={}\n\
             log-error={}\n\
             tmpdir={}\n\
             secure-file-priv={}\n\
             log-bin={}\n\
             relay-log={}\n\
             skip-networking=ON\n\
             mysqlx=OFF\n\
             port={}\n\
             report-host={}\n\
             report-port={}\n\
             server-id={}\n\
             binlog-format=ROW\n\
             binlog-checksum=NONE\n\
             relay-log-recovery=ON\n\
             gtid-mode=ON\n\
             enforce-gtid-consistency=ON\n\
             plugin-load-add=group_replication.so\n\
             loose-group-replication-group-name={}\n\
             loose-group-replication-local-address={}\n\
             loose-group-replication-group-seeds={}\n\
             loose-group-replication-single-primary-mode=ON\n\
             loose-group-replication-enforce-update-everywhere-checks=OFF\n\
             loose-group-replication-start-on-boot=OFF\n\
             loose-group-replication-bootstrap-group=OFF\n",
            display(&self.data_root),
            display(&self.runtime.socket),
            display(&self.runtime.pid),
            display(&self.runtime.error_log),
            display(&self.runtime.temporary),
            display(&self.runtime.secure_files),
            display(&self.runtime.binary_log),
            display(&self.runtime.relay_log),
            member.sql_address.port(),
            member.sql_address.ip(),
            member.sql_address.port(),
            member.server_id,
            member.group_uuid,
            member.group_replication_address,
            seeds,
        )
    }

    pub(crate) fn create_layout(&self) -> Result<OwnedRoots, LayoutCreationError> {
        // Revalidate immediately before ownership is claimed.
        validate_fresh_root(&self.data_root).map_err(|error| LayoutCreationError {
            error,
            created_roots: Vec::new(),
        })?;
        validate_fresh_root(&self.runtime.scratch_root).map_err(|error| LayoutCreationError {
            error,
            created_roots: Vec::new(),
        })?;
        let data =
            create_private_directory(&self.data_root).map_err(|error| LayoutCreationError {
                error,
                created_roots: Vec::new(),
            })?;
        let mut created_roots = vec![data.clone()];
        let scratch = create_private_directory(&self.runtime.scratch_root).map_err(|error| {
            LayoutCreationError {
                error,
                created_roots: created_roots.clone(),
            }
        })?;
        created_roots.push(scratch.clone());
        for directory in [&self.runtime.temporary, &self.runtime.secure_files] {
            if let Err(error) = create_private_directory(directory) {
                return Err(LayoutCreationError {
                    error,
                    created_roots,
                });
            }
        }
        Ok(OwnedRoots { data, scratch })
    }

    pub(crate) fn revalidate_executables(&self) -> Result<(), ConfigError> {
        if validate_executable(&self.mysqld)? != self.mysqld_identity
            || validate_executable(&self.launcher)? != self.launcher_identity
        {
            return Err(ConfigError::ExecutableChanged);
        }
        Ok(())
    }
}

fn validate_sql_address(address: SocketAddr) -> Result<(), ConfigError> {
    if address.port() == 0 {
        return Err(ConfigError::ZeroSqlPort);
    }
    if has_noncanonical_ipv6_metadata(address) {
        return Err(ConfigError::NonCanonicalSqlAddress);
    }
    if !address.ip().is_loopback() {
        return Err(ConfigError::NonLoopbackSqlAddress);
    }
    Ok(())
}

fn validate_group_replication_address(address: SocketAddr) -> Result<(), ConfigError> {
    if address.port() == 0 {
        return Err(ConfigError::ZeroGroupReplicationPort);
    }
    if has_noncanonical_ipv6_metadata(address) {
        return Err(ConfigError::NonCanonicalGroupReplicationAddress);
    }
    if !address.ip().is_loopback() {
        return Err(ConfigError::NonLoopbackGroupReplicationAddress);
    }
    Ok(())
}

fn validate_group_replication_seed(address: SocketAddr) -> Result<(), ConfigError> {
    if address.port() == 0 {
        return Err(ConfigError::ZeroGroupReplicationSeedPort);
    }
    if has_noncanonical_ipv6_metadata(address) {
        return Err(ConfigError::NonCanonicalGroupReplicationSeed);
    }
    if !address.ip().is_loopback() {
        return Err(ConfigError::NonLoopbackGroupReplicationSeed);
    }
    Ok(())
}

fn has_noncanonical_ipv6_metadata(address: SocketAddr) -> bool {
    matches!(address, SocketAddr::V6(address) if address.flowinfo() != 0 || address.scope_id() != 0)
}

fn display(path: &Path) -> &str {
    path.to_str()
        .expect("validated configuration paths remain UTF-8")
}

fn validate_executable(path: &Path) -> Result<FileIdentity, ConfigError> {
    validate_path_shape(path)?;
    validate_existing_components(path, false)?;
    let metadata =
        fs::symlink_metadata(path).map_err(|error| ConfigError::Filesystem(error.kind()))?;
    if !metadata.is_file() || metadata.mode() & 0o111 == 0 {
        return Err(ConfigError::NotExecutableFile);
    }
    Ok(FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

fn validate_fresh_root(path: &Path) -> Result<(), ConfigError> {
    validate_path_shape(path)?;
    if path.exists() || fs::symlink_metadata(path).is_ok() {
        return Err(ConfigError::RootAlreadyExists);
    }
    validate_existing_components(path, true)?;
    let parent = path.parent().ok_or(ConfigError::InvalidRootParent)?;
    let metadata = fs::symlink_metadata(parent).map_err(|_| ConfigError::InvalidRootParent)?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != effective_uid()
        || metadata.permissions().mode() & 0o022 != 0
    {
        return Err(ConfigError::InvalidRootParent);
    }
    let canonical_parent =
        fs::canonicalize(parent).map_err(|error| ConfigError::Filesystem(error.kind()))?;
    let file_name = path.file_name().ok_or(ConfigError::InvalidRootParent)?;
    if canonical_parent.join(file_name) != path {
        return Err(ConfigError::PathNotNormalized);
    }
    Ok(())
}

fn validate_path_shape(path: &Path) -> Result<(), ConfigError> {
    if !path.is_absolute() {
        return Err(ConfigError::PathNotAbsolute);
    }
    if path
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(ConfigError::PathNotNormalized);
    }
    let value = path.to_str().ok_or(ConfigError::PathNotRepresentable)?;
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-'))
    {
        return Err(ConfigError::PathNotRepresentable);
    }
    Ok(())
}

fn validate_existing_components(path: &Path, allow_missing_final: bool) -> Result<(), ConfigError> {
    let mut current = PathBuf::new();
    let last = path.components().count().saturating_sub(1);
    for (index, component) in path.components().enumerate() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => return Err(ConfigError::Symlink),
            Ok(_) => {}
            Err(error)
                if allow_missing_final
                    && index == last
                    && error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(ConfigError::Filesystem(error.kind())),
        }
    }
    Ok(())
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left == right || left.starts_with(right) || right.starts_with(left)
}

fn create_private_directory(path: &Path) -> Result<OwnedRoot, ConfigError> {
    let parent = path.parent().ok_or(ConfigError::InvalidRootParent)?;
    let parent_metadata =
        fs::symlink_metadata(parent).map_err(|error| ConfigError::Filesystem(error.kind()))?;
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder
        .create(path)
        .map_err(|error| ConfigError::Filesystem(error.kind()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|error| ConfigError::Filesystem(error.kind()))?;
    let metadata =
        fs::symlink_metadata(path).map_err(|error| ConfigError::Filesystem(error.kind()))?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.uid() != effective_uid()
    {
        return Err(ConfigError::InvalidRootParent);
    }
    Ok(OwnedRoot {
        path: path.to_owned(),
        device: metadata.dev(),
        inode: metadata.ino(),
        uid: metadata.uid(),
        mode: metadata.permissions().mode() & 0o777,
        parent_device: parent_metadata.dev(),
        parent_inode: parent_metadata.ino(),
    })
}

fn effective_uid() -> u32 {
    // Reading the effective UID through procfs avoids unsafe libc calls while
    // still checking that newly created roots are owned by this process.
    fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with("Uid:"))
                .and_then(|line| line.split_whitespace().nth(2))
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or_else(|| fs::metadata(".").map_or(u32::MAX, |metadata| metadata.uid()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_rejects_zero() {
        assert_eq!(
            MysqlOperationTimeouts::new(
                Duration::ZERO,
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
            Err(ConfigError::ZeroTimeout)
        );
        assert_eq!(
            MysqlOperationTimeouts::new(
                Duration::MAX,
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
            Err(ConfigError::UnrepresentableTimeout)
        );
    }

    #[test]
    fn overlap_is_component_aware() {
        assert!(paths_overlap(
            Path::new("/a/data"),
            Path::new("/a/data/run")
        ));
        assert!(!paths_overlap(
            Path::new("/a/data"),
            Path::new("/a/database")
        ));
    }

    #[test]
    fn generated_names_are_stable() {
        let paths = MysqlRuntimePaths::new(PathBuf::from("/scratch"));
        assert_eq!(paths.config(), Path::new("/scratch/my.cnf"));
        assert_eq!(paths.socket(), Path::new("/scratch/mysql.sock"));
        assert_eq!(paths.pid(), Path::new("/scratch/mysqld.pid"));
    }
}
