//! One-generation in-memory lifecycle state machine.

use std::fs::{self, File};
use std::os::unix::fs::FileTypeExt;
use std::process::{Child, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::adapter::{
    MysqlObserver, ObservationClock, ObservationReport, ObservationRequest, UnixSocketPath,
};
use rustix::fd::{AsFd, OwnedFd};
use rustix::fs::{
    AtFlags, Dir, FileType, FlockOperation, Mode, OFlags, RenameFlags, flock, fstat, open, openat,
    renameat_with, statat, unlinkat,
};

use crate::core::GroupName;
use crate::core::{
    AttemptId, CredentialGeneration, EndpointBinding, MemberAddress, ProcessSessionId,
    StorageBinding,
};
use crate::service::config::OwnedRoot;
use crate::service::control::OwnedControlTarget;
use crate::service::process::{
    attest_child, launcher_command, terminate_and_reap, verify_product, wait_bounded,
    wait_for_absence,
};
use crate::service::{
    AccountProvisioningEvidence, BootstrapCapability, BootstrapEffect, ControlCredential,
    JoinCapability, JoinEffect, LifecycleOperation, MemberControlBinding, MysqlInstanceConfig,
    MysqlInstanceError, MysqlTopologyError, NativeControlDeadline, NativeIdentityEnrollment,
    TopologyStateError, ViewDiscovery,
};

const READY_POLL: Duration = Duration::from_millis(25);
static CLEANUP_SERIAL: AtomicU64 = AtomicU64::new(1);

/// Explicit state of the sole in-memory process generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MysqlInstanceState {
    /// Configuration is validated, but no roots are owned.
    Configured,
    /// Fresh data was initialized and scratch is prepared.
    Initialized,
    /// The exact retained child is live and its private socket is ready.
    Running,
    /// The exact retained child was reaped and scratch was removed.
    Stopped,
    /// A lifecycle failure left ownership requiring explicit fixture reset.
    Faulted,
}

/// Owns one fresh MySQL generation and its sole retained child.
pub struct MysqlInstanceManager {
    config: MysqlInstanceConfig,
    state: MysqlInstanceState,
    child: Option<Child>,
    data_root: Option<OwnedRoot>,
    scratch_root: Option<OwnedRoot>,
}

impl MysqlInstanceManager {
    /// Creates a manager without touching the configured fresh roots.
    #[must_use]
    pub const fn new(config: MysqlInstanceConfig) -> Self {
        Self {
            config,
            state: MysqlInstanceState::Configured,
            child: None,
            data_root: None,
            scratch_root: None,
        }
    }

    /// Returns the current in-memory lifecycle state.
    #[must_use]
    pub const fn state(&self) -> MysqlInstanceState {
        self.state
    }

    /// Returns the immutable validated configuration.
    #[must_use]
    pub const fn config(&self) -> &MysqlInstanceConfig {
        &self.config
    }

    /// Returns the owned private socket only while the exact child is running.
    #[must_use]
    pub fn socket_path(&self) -> Option<&std::path::Path> {
        (self.state == MysqlInstanceState::Running).then(|| self.config.runtime().socket())
    }

    /// Claims fresh roots, validates the exact product, renders configuration,
    /// and performs bounded insecure initialization.
    pub fn initialize(&mut self) -> Result<(), MysqlInstanceError> {
        self.require_state(MysqlInstanceState::Configured)?;
        let roots = match self.config.create_layout() {
            Ok(roots) => roots,
            Err(layout) => {
                self.state = MysqlInstanceState::Faulted;
                let prior = MysqlInstanceError::Config(layout.error);
                return match cleanup_claimed_roots(&layout.created_roots) {
                    Ok(()) => Err(prior),
                    Err(cleanup) => Err(MysqlInstanceError::CleanupAfter {
                        prior: Box::new(prior),
                        cleanup: Box::new(cleanup),
                    }),
                };
            }
        };
        self.data_root = Some(roots.data);
        self.scratch_root = Some(roots.scratch);
        let result = self.initialize_inner();
        match result {
            Ok(()) => {
                self.state = MysqlInstanceState::Initialized;
                Ok(())
            }
            Err(prior) => {
                self.state = MysqlInstanceState::Faulted;
                if self.child.is_some() {
                    return Err(prior);
                }
                match self.cleanup_all_roots() {
                    Ok(()) => Err(prior),
                    Err(cleanup) => Err(MysqlInstanceError::CleanupAfter {
                        prior: Box::new(prior),
                        cleanup: Box::new(cleanup),
                    }),
                }
            }
        }
    }

    fn initialize_inner(&mut self) -> Result<(), MysqlInstanceError> {
        verify_product(&self.config, &mut self.child)?;
        fs::write(
            self.config.runtime().config(),
            self.config.render_server_config(),
        )
        .map_err(|error| MysqlInstanceError::Io {
            operation: LifecycleOperation::Initialization,
            kind: error.kind(),
        })?;
        let stdout = File::create(self.config.runtime().init_stdout_log()).map_err(|error| {
            MysqlInstanceError::Io {
                operation: LifecycleOperation::Initialization,
                kind: error.kind(),
            }
        })?;
        let stderr = File::create(self.config.runtime().init_stderr_log()).map_err(|error| {
            MysqlInstanceError::Io {
                operation: LifecycleOperation::Initialization,
                kind: error.kind(),
            }
        })?;
        let child = launcher_command(&self.config)
            .arg(format!(
                "--defaults-file={}",
                self.config.runtime().config().display()
            ))
            .arg("--initialize-insecure")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .map_err(|error| MysqlInstanceError::Io {
                operation: LifecycleOperation::Initialization,
                kind: error.kind(),
            })?;
        self.child = Some(child);
        let result = wait_bounded(
            self.child
                .as_mut()
                .expect("initializer child retained before waiting"),
            self.config.timeouts().initialization(),
            self.config.timeouts().shutdown(),
            LifecycleOperation::Initialization,
        );
        let reaped = self
            .child
            .as_mut()
            .is_some_and(|child| child.try_wait().ok().flatten().is_some());
        if reaped {
            self.child = None;
        }
        let status = result?;
        if !status.success() {
            return Err(MysqlInstanceError::ChildFailure {
                operation: LifecycleOperation::Initialization,
                code: status.code(),
            });
        }
        Ok(())
    }

    /// Launches and attests the exact child, then waits for the owned private
    /// socket within the startup deadline.
    pub fn start(&mut self) -> Result<(), MysqlInstanceError> {
        self.require_state(MysqlInstanceState::Initialized)?;
        self.config.revalidate_executables()?;
        if let Err(error) = self.revalidate_roots() {
            self.state = MysqlInstanceState::Faulted;
            return Err(error);
        }
        let stdout = File::create(self.config.runtime().stdout_log()).map_err(|error| {
            MysqlInstanceError::Io {
                operation: LifecycleOperation::Startup,
                kind: error.kind(),
            }
        })?;
        let stderr = File::create(self.config.runtime().error_log()).map_err(|error| {
            MysqlInstanceError::Io {
                operation: LifecycleOperation::Startup,
                kind: error.kind(),
            }
        })?;
        let child = launcher_command(&self.config)
            .arg(format!(
                "--defaults-file={}",
                self.config.runtime().config().display()
            ))
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .map_err(|error| MysqlInstanceError::Io {
                operation: LifecycleOperation::Startup,
                kind: error.kind(),
            })?;
        self.child = Some(child);
        if let Err(prior) = self.wait_until_ready() {
            self.state = MysqlInstanceState::Faulted;
            let cleanup = self
                .child
                .as_mut()
                .map(|child| terminate_and_reap(child, self.config.timeouts().shutdown()))
                .transpose();
            return match cleanup {
                Ok(_) => {
                    self.child = None;
                    Err(prior)
                }
                Err(cleanup) => Err(MysqlInstanceError::CleanupAfter {
                    prior: Box::new(prior),
                    cleanup: Box::new(cleanup),
                }),
            };
        }
        self.state = MysqlInstanceState::Running;
        Ok(())
    }

    fn wait_until_ready(&mut self) -> Result<(), MysqlInstanceError> {
        let deadline = checked_deadline(
            self.config.timeouts().startup(),
            LifecycleOperation::Startup,
        )?;
        loop {
            let child = self.child.as_mut().expect("child retained during startup");
            if let Some(status) = child.try_wait().map_err(|error| MysqlInstanceError::Io {
                operation: LifecycleOperation::Startup,
                kind: error.kind(),
            })? {
                return Err(MysqlInstanceError::ChildFailure {
                    operation: LifecycleOperation::Startup,
                    code: status.code(),
                });
            }
            let socket_ready =
                fs::symlink_metadata(self.config.runtime().socket()).is_ok_and(|metadata| {
                    !metadata.file_type().is_symlink() && metadata.file_type().is_socket()
                });
            if socket_ready && self.config.runtime().pid().is_file() {
                attest_child(child, &self.config)?;
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(MysqlInstanceError::Timeout(LifecycleOperation::Startup));
            }
            thread::sleep(READY_POLL);
        }
    }

    /// Delegates one request to the existing adapter after proving it selects
    /// the owned socket and the exact retained child is still live.
    pub async fn observe<C: ObservationClock>(
        &mut self,
        request: ObservationRequest<C>,
    ) -> Result<ObservationReport, MysqlInstanceError> {
        self.require_state(MysqlInstanceState::Running)?;
        if let Err(error) = self.revalidate_roots() {
            self.state = MysqlInstanceState::Faulted;
            return Err(error);
        }
        if request.socket().as_path() != self.config.runtime().socket() {
            return Err(MysqlInstanceError::ObservationSocketMismatch);
        }
        if let Err(error) = attest_child(
            self.child
                .as_mut()
                .expect("running state retains the exact child"),
            &self.config,
        ) {
            self.state = MysqlInstanceState::Faulted;
            return Err(error);
        }
        Ok(MysqlObserver::observe(request).await)
    }

    /// Creates exact minimum observer and recovery accounts through the owned
    /// private UDS, with account statements excluded from binary logging.
    pub async fn provision_topology_accounts(
        &mut self,
        binding: MemberControlBinding,
        observer: &ControlCredential,
        recovery: &ControlCredential,
        deadline: &NativeControlDeadline,
    ) -> Result<AccountProvisioningEvidence, MysqlTopologyError> {
        let target = self.prepare_control_target()?;
        crate::service::control::provision_accounts(target, binding, observer, recovery, deadline)
            .await
    }

    /// Enrolls `@@server_uuid` before this member enters any topology effect.
    pub async fn enroll_topology_identity(
        &mut self,
        binding: MemberControlBinding,
        accounts: &AccountProvisioningEvidence,
        deadline: &NativeControlDeadline,
    ) -> Result<NativeIdentityEnrollment, MysqlTopologyError> {
        let target = self.prepare_control_target()?;
        crate::service::control::enroll_identity(target, binding, accounts, deadline).await
    }

    /// Enters the one designated bootstrap effect and proves bootstrap mode is
    /// off before returning effect evidence.
    pub async fn bootstrap_group_replication(
        &mut self,
        capability: BootstrapCapability,
        recovery: &ControlCredential,
        deadline: &NativeControlDeadline,
    ) -> Result<BootstrapEffect, MysqlTopologyError> {
        let target = self.prepare_control_target()?;
        crate::service::control::bootstrap(target, capability, recovery, deadline).await
    }

    /// Enters one non-bootstrap join with process-memory-only recovery
    /// credentials.
    pub async fn join_group_replication(
        &mut self,
        capability: JoinCapability,
        recovery: &ControlCredential,
        deadline: &NativeControlDeadline,
    ) -> Result<JoinEffect, MysqlTopologyError> {
        let target = self.prepare_control_target()?;
        crate::service::control::join(target, capability, recovery, deadline).await
    }

    /// Discovers a proposed post-effect view without replacing enrollment or
    /// granting lifecycle credit.
    pub async fn discover_topology_view(
        &mut self,
        enrollment: NativeIdentityEnrollment,
        group_name: GroupName,
        deadline: &NativeControlDeadline,
    ) -> Result<ViewDiscovery, MysqlTopologyError> {
        let target = self.prepare_control_target()?;
        crate::service::control::discover_view(target, enrollment, group_name, deadline).await
    }

    /// Attests, terminates, and reaps the exact retained child, proves socket
    /// disappearance, and removes only disposable scratch. Persistent data is
    /// retained.
    pub fn stop(&mut self) -> Result<(), MysqlInstanceError> {
        self.require_state(MysqlInstanceState::Running)?;
        if let Err(error) = self.revalidate_roots() {
            self.state = MysqlInstanceState::Faulted;
            return Err(error);
        }
        let child = self
            .child
            .as_mut()
            .expect("running state retains the exact child");
        if let Err(error) = attest_child(child, &self.config) {
            self.state = MysqlInstanceState::Faulted;
            return Err(error);
        }
        if let Err(error) = terminate_and_reap(child, self.config.timeouts().shutdown()) {
            self.state = MysqlInstanceState::Faulted;
            return Err(error);
        }
        self.child = None;
        if let Err(error) = wait_for_absence(
            self.config.runtime().socket(),
            self.config.timeouts().socket_disappearance(),
        ) {
            self.state = MysqlInstanceState::Faulted;
            return Err(error);
        }
        let scratch = self
            .scratch_root
            .as_ref()
            .expect("running state retains scratch ownership");
        if let Err(error) = remove_owned_root(scratch, LifecycleOperation::ScratchCleanup) {
            self.state = MysqlInstanceState::Faulted;
            return Err(error);
        }
        self.scratch_root = None;
        self.state = MysqlInstanceState::Stopped;
        Ok(())
    }

    fn require_state(&self, expected: MysqlInstanceState) -> Result<(), MysqlInstanceError> {
        if self.state == expected {
            Ok(())
        } else {
            Err(MysqlInstanceError::InvalidState {
                expected,
                actual: self.state,
            })
        }
    }

    fn revalidate_roots(&self) -> Result<(), MysqlInstanceError> {
        for root in [&self.data_root, &self.scratch_root] {
            root.as_ref()
                .ok_or(MysqlInstanceError::Ownership(
                    crate::service::OwnershipError::RootMismatch,
                ))?
                .revalidate()
                .map_err(MysqlInstanceError::Ownership)?;
        }
        Ok(())
    }

    fn prepare_control_target(&mut self) -> Result<OwnedControlTarget, MysqlTopologyError> {
        if self.state != MysqlInstanceState::Running {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::InvalidTransition,
            ));
        }
        if self.revalidate_roots().is_err() {
            self.state = MysqlInstanceState::Faulted;
            return Err(MysqlTopologyError::OwnershipContextLoss);
        }
        if attest_child(
            self.child
                .as_mut()
                .expect("running state retains the exact child"),
            &self.config,
        )
        .is_err()
        {
            self.state = MysqlInstanceState::Faulted;
            return Err(MysqlTopologyError::OwnershipContextLoss);
        }
        let socket = UnixSocketPath::new(self.config.runtime().socket()).map_err(|_| {
            MysqlTopologyError::ControlTransport(crate::service::ControlStage::SocketValidation)
        })?;
        Ok(OwnedControlTarget::new(
            socket,
            self.config.member_index(),
            self.config.member().clone(),
        ))
    }

    pub(crate) fn topology_binding(
        &mut self,
        attempt: AttemptId,
        observer_generation: CredentialGeneration,
        recovery_generation: CredentialGeneration,
    ) -> Result<MemberControlBinding, MysqlTopologyError> {
        self.prepare_control_target()?;
        let child = self
            .child
            .as_ref()
            .expect("running state retains the exact child");
        let data = self
            .data_root
            .as_ref()
            .expect("running state retains exact data ownership");
        let scratch = self
            .scratch_root
            .as_ref()
            .expect("running state retains exact scratch ownership");
        let process_session = ProcessSessionId::new(format!(
            "member-{}-pid-{}-scratch-{}-{}",
            self.config.member_index().as_usize() + 1,
            child.id(),
            scratch.device(),
            scratch.inode()
        ))
        .map_err(|_| MysqlTopologyError::OwnershipContextLoss)?;
        let endpoint = EndpointBinding::new(format!(
            "uds:{}:{}:{}",
            scratch.device(),
            scratch.inode(),
            self.config.runtime().socket().display()
        ))
        .map_err(|_| MysqlTopologyError::OwnershipContextLoss)?;
        let storage = StorageBinding::new(format!("data:{}:{}", data.device(), data.inode()))
            .map_err(|_| MysqlTopologyError::OwnershipContextLoss)?;
        let member_address = MemberAddress::new(self.config.member().sql_address().to_string())
            .map_err(|_| MysqlTopologyError::OwnershipContextLoss)?;
        Ok(MemberControlBinding::new(
            attempt,
            self.config.member_index(),
            process_session,
            endpoint,
            storage,
            member_address,
            observer_generation,
            recovery_generation,
        ))
    }

    /// Contains any exact process generation owned by this manager and removes
    /// disposable scratch even when startup or topology control failed.
    pub fn contain(&mut self) -> Result<(), MysqlInstanceError> {
        match self.state {
            MysqlInstanceState::Configured | MysqlInstanceState::Stopped => return Ok(()),
            MysqlInstanceState::Running => return self.stop(),
            MysqlInstanceState::Initialized | MysqlInstanceState::Faulted => {}
        }
        if self.child.is_none() && self.data_root.is_none() && self.scratch_root.is_none() {
            self.state = MysqlInstanceState::Stopped;
            return Ok(());
        }
        if let Some(child) = self.child.as_mut() {
            terminate_and_reap(child, self.config.timeouts().shutdown())?;
            self.child = None;
            wait_for_absence(
                self.config.runtime().socket(),
                self.config.timeouts().socket_disappearance(),
            )?;
        }
        self.revalidate_roots()?;
        if let Some(scratch) = self.scratch_root.as_ref() {
            remove_owned_root(scratch, LifecycleOperation::ScratchCleanup)?;
            self.scratch_root = None;
        }
        self.state = MysqlInstanceState::Stopped;
        Ok(())
    }

    fn cleanup_all_roots(&mut self) -> Result<(), MysqlInstanceError> {
        let roots = [self.data_root.clone(), self.scratch_root.clone()]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let result = cleanup_claimed_roots(&roots);
        if result.is_ok() {
            self.data_root = None;
            self.scratch_root = None;
        }
        result
    }
}

impl Drop for MysqlInstanceManager {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut()
            && child.try_wait().ok().flatten().is_none()
        {
            let _ = terminate_and_reap(child, self.config.timeouts().shutdown());
        }
    }
}

fn checked_deadline(
    timeout: Duration,
    operation: LifecycleOperation,
) -> Result<Instant, MysqlInstanceError> {
    Instant::now()
        .checked_add(timeout)
        .ok_or(MysqlInstanceError::Timeout(operation))
}

fn cleanup_claimed_roots(roots: &[OwnedRoot]) -> Result<(), MysqlInstanceError> {
    let mut first_error = None;
    for root in roots.iter().rev() {
        if let Err(error) = remove_owned_root(root, LifecycleOperation::OwnedRootCleanup)
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn remove_owned_root(
    root: &OwnedRoot,
    operation: LifecycleOperation,
) -> Result<(), MysqlInstanceError> {
    remove_owned_root_with_hook(root, operation, |_| {})
}

fn remove_owned_root_with_hook<F>(
    root: &OwnedRoot,
    operation: LifecycleOperation,
    hook: F,
) -> Result<(), MysqlInstanceError>
where
    F: FnOnce(&std::path::Path),
{
    root.revalidate().map_err(MysqlInstanceError::Ownership)?;
    let parent = root.path().parent().ok_or(MysqlInstanceError::Ownership(
        crate::service::OwnershipError::RootMismatch,
    ))?;
    let root_name = root
        .path()
        .file_name()
        .ok_or(MysqlInstanceError::Ownership(
            crate::service::OwnershipError::RootMismatch,
        ))?;
    let parent_fd = open(
        parent,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| io_error(operation, error))?;
    lock_directory(&parent_fd, operation)?;
    let parent_stat = fstat(&parent_fd).map_err(|error| io_error(operation, error))?;
    if parent_stat.st_dev != root.parent_device() || parent_stat.st_ino != root.parent_inode() {
        return Err(MysqlInstanceError::Ownership(
            crate::service::OwnershipError::RootMismatch,
        ));
    }
    let root_fd = openat(
        &parent_fd,
        root_name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| MysqlInstanceError::Ownership(crate::service::OwnershipError::RootMismatch))?;
    lock_directory(&root_fd, operation)?;
    verify_fd_identity(&root_fd, root.device(), root.inode())?;

    let serial = CLEANUP_SERIAL.fetch_add(1, Ordering::Relaxed);
    let quarantine_name = format!(".kuberic-cleanup-{}-{serial}", std::process::id());
    let quarantine_path = parent.join(&quarantine_name);
    renameat_with(
        &parent_fd,
        root_name,
        &parent_fd,
        quarantine_name.as_str(),
        RenameFlags::NOREPLACE,
    )
    .map_err(|error| io_error(operation, error))?;
    let moved = statat(
        &parent_fd,
        quarantine_name.as_str(),
        AtFlags::SYMLINK_NOFOLLOW,
    )
    .map_err(|error| io_error(operation, error))?;
    if moved.st_dev != root.device() || moved.st_ino != root.inode() {
        return Err(MysqlInstanceError::Ownership(
            crate::service::OwnershipError::RootMismatch,
        ));
    }

    remove_directory_contents(&root_fd, operation)?;

    let current = statat(
        &parent_fd,
        quarantine_name.as_str(),
        AtFlags::SYMLINK_NOFOLLOW,
    )
    .map_err(|_| MysqlInstanceError::Ownership(crate::service::OwnershipError::RootMismatch))?;
    if current.st_dev != root.device() || current.st_ino != root.inode() {
        return Err(MysqlInstanceError::Ownership(
            crate::service::OwnershipError::RootMismatch,
        ));
    }
    hook(&quarantine_path);
    let final_current = statat(
        &parent_fd,
        quarantine_name.as_str(),
        AtFlags::SYMLINK_NOFOLLOW,
    )
    .map_err(|_| MysqlInstanceError::Ownership(crate::service::OwnershipError::RootMismatch))?;
    if final_current.st_dev != root.device() || final_current.st_ino != root.inode() {
        return Err(MysqlInstanceError::Ownership(
            crate::service::OwnershipError::RootMismatch,
        ));
    }
    unlinkat(&parent_fd, quarantine_name.as_str(), AtFlags::REMOVEDIR)
        .map_err(|error| io_error(operation, error))
}

fn remove_directory_contents(
    directory: &OwnedFd,
    operation: LifecycleOperation,
) -> Result<(), MysqlInstanceError> {
    lock_directory(directory, operation)?;
    loop {
        let mut entries = Dir::read_from(directory).map_err(|error| io_error(operation, error))?;
        let entry = entries
            .find_map(|entry| match entry {
                Ok(entry)
                    if entry.file_name().to_bytes() != b"."
                        && entry.file_name().to_bytes() != b".." =>
                {
                    Some(Ok(entry.file_name().to_owned()))
                }
                Ok(_) => None,
                Err(error) => Some(Err(io_error(operation, error))),
            })
            .transpose()?;
        let Some(name) = entry else {
            return Ok(());
        };
        let original = statat(directory, &name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|error| io_error(operation, error))?;
        let serial = CLEANUP_SERIAL.fetch_add(1, Ordering::Relaxed);
        let quarantine = format!(".kuberic-entry-{}-{serial}", std::process::id());
        renameat_with(
            directory,
            &name,
            directory,
            quarantine.as_str(),
            RenameFlags::NOREPLACE,
        )
        .map_err(|error| io_error(operation, error))?;
        let moved = statat(directory, quarantine.as_str(), AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|error| io_error(operation, error))?;
        if moved.st_dev != original.st_dev || moved.st_ino != original.st_ino {
            return Err(MysqlInstanceError::Ownership(
                crate::service::OwnershipError::RootMismatch,
            ));
        }
        if FileType::from_raw_mode(moved.st_mode) == FileType::Directory {
            let child = openat(
                directory,
                quarantine.as_str(),
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|error| io_error(operation, error))?;
            lock_directory(&child, operation)?;
            verify_fd_identity(&child, moved.st_dev, moved.st_ino)?;
            remove_directory_contents(&child, operation)?;
            let current = statat(directory, quarantine.as_str(), AtFlags::SYMLINK_NOFOLLOW)
                .map_err(|_| {
                    MysqlInstanceError::Ownership(crate::service::OwnershipError::RootMismatch)
                })?;
            if current.st_dev != moved.st_dev || current.st_ino != moved.st_ino {
                return Err(MysqlInstanceError::Ownership(
                    crate::service::OwnershipError::RootMismatch,
                ));
            }

            unlinkat(directory, quarantine.as_str(), AtFlags::REMOVEDIR)
                .map_err(|error| io_error(operation, error))?;
        } else {
            unlinkat(directory, quarantine.as_str(), AtFlags::empty())
                .map_err(|error| io_error(operation, error))?;
        }
    }
}

fn lock_directory<Fd: AsFd>(
    fd: Fd,
    operation: LifecycleOperation,
) -> Result<(), MysqlInstanceError> {
    flock(fd, FlockOperation::NonBlockingLockExclusive).map_err(|error| io_error(operation, error))
}

fn verify_fd_identity<Fd: AsFd>(fd: Fd, device: u64, inode: u64) -> Result<(), MysqlInstanceError> {
    let stat = fstat(fd)
        .map_err(|_| MysqlInstanceError::Ownership(crate::service::OwnershipError::RootMismatch))?;
    if stat.st_dev == device && stat.st_ino == inode {
        Ok(())
    } else {
        Err(MysqlInstanceError::Ownership(
            crate::service::OwnershipError::RootMismatch,
        ))
    }
}

fn io_error(operation: LifecycleOperation, error: rustix::io::Errno) -> MysqlInstanceError {
    MysqlInstanceError::Io {
        operation,
        kind: std::io::Error::from_raw_os_error(error.raw_os_error()).kind(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::{
        MysqlMemberConfig, MysqlMemberIndex, MysqlOperationTimeouts, MysqlTopologyConfig,
    };
    use std::net::SocketAddr;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn cleanup_remains_bound_to_open_root_when_quarantine_is_replaced() {
        let parent = std::env::temp_dir().join(format!(
            "kms-cleanup-race-{}-{}",
            std::process::id(),
            CLEANUP_SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&parent);
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        let config = MysqlInstanceConfig::new(
            "/usr/bin/sleep",
            "/usr/bin/env",
            parent.join("data"),
            parent.join("scratch"),
            topology(),
            MysqlMemberIndex::First,
            MysqlOperationTimeouts::new(
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .unwrap(),
        )
        .unwrap();
        let roots = match config.create_layout() {
            Ok(roots) => roots,
            Err(_) => panic!("test layout"),
        };
        fs::write(roots.scratch.path().join("owned"), b"owned").unwrap();
        let moved_exact = parent.join("moved-exact");
        let replacement_file = parent.join("replacement-file");

        let error = remove_owned_root_with_hook(
            &roots.scratch,
            LifecycleOperation::ScratchCleanup,
            |quarantine| {
                fs::rename(quarantine, &moved_exact).unwrap();
                fs::create_dir(quarantine).unwrap();
                fs::set_permissions(quarantine, fs::Permissions::from_mode(0o700)).unwrap();
                fs::write(quarantine.join("foreign"), b"foreign").unwrap();
                fs::write(&replacement_file, quarantine.as_os_str().as_encoded_bytes()).unwrap();
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            MysqlInstanceError::Ownership(crate::service::OwnershipError::RootMismatch)
        ));
        let replacement = std::path::PathBuf::from(
            String::from_utf8(fs::read(&replacement_file).unwrap()).unwrap(),
        );
        assert!(replacement.join("foreign").is_file());
        assert!(!moved_exact.join("owned").exists());

        fs::remove_dir_all(replacement).unwrap();
        fs::remove_dir(moved_exact).unwrap();
        fs::remove_dir_all(roots.data.path()).unwrap();
        fs::remove_file(replacement_file).unwrap();
        fs::remove_dir(parent).unwrap();
    }

    fn topology() -> MysqlTopologyConfig {
        let sql = [
            SocketAddr::from(([127, 0, 0, 1], 33061)),
            SocketAddr::from(([127, 0, 0, 1], 33062)),
            SocketAddr::from(([127, 0, 0, 1], 33063)),
        ];
        let group = [
            SocketAddr::from(([127, 0, 0, 1], 43061)),
            SocketAddr::from(([127, 0, 0, 1], 43062)),
            SocketAddr::from(([127, 0, 0, 1], 43063)),
        ];
        MysqlTopologyConfig::new([
            MysqlMemberConfig::new(
                1,
                sql[0],
                group[0],
                "cccccccc-cccc-cccc-cccc-cccccccccccc",
                group,
            )
            .unwrap(),
            MysqlMemberConfig::new(
                2,
                sql[1],
                group[1],
                "cccccccc-cccc-cccc-cccc-cccccccccccc",
                group,
            )
            .unwrap(),
            MysqlMemberConfig::new(
                3,
                sql[2],
                group[2],
                "cccccccc-cccc-cccc-cccc-cccccccccccc",
                group,
            )
            .unwrap(),
        ])
        .unwrap()
    }
}
