//! One-generation in-memory lifecycle state machine.

use std::fs::{self, File};
use std::os::unix::fs::FileTypeExt;
#[cfg(test)]
use std::os::unix::fs::MetadataExt;
use std::path::Path;
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
#[cfg(test)]
use crate::service::process::qualification_kill_and_reap;
use crate::service::process::{
    attest_child, launcher_command, terminate_and_reap, terminate_and_reap_before, verify_product,
    verify_product_with_attempt, wait_bounded, wait_bounded_with_attempt, wait_for_absence,
    wait_for_absence_before,
};
use crate::service::{
    AccountProvisioningEvidence, ApplicationAccountReceipt, ApplicationCredential,
    AttemptAdmissionError, AttemptCleanupState, AttemptContext, AttemptDeadlineState,
    BackendConnectionId, BackendConnectionReceipt, BootstrapCapability, BootstrapEffect,
    ControlCredential, FreshBootstrapInput, FreshJoinInput, JoinCapability, JoinEffect,
    LifecycleOperation, MemberAuthorityError, MemberControlBinding, MemberEffectKind,
    MemberEffectReceipt, MemberEffectRequest, MemberMutationAuthority, MemberNativeEvidence,
    MemberTransitionError, MysqlInstanceConfig, MysqlInstanceError, MysqlMemberEffectBoundary,
    MysqlProxyControlBoundary, MysqlProxyError, MysqlProxyPhase, MysqlTopologyError,
    NativeControlDeadline, NativeIdentityEnrollment, PairedAttemptOutcome, PrimaryTransferInput,
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

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum QualificationStopKind {
    Graceful,
    Abrupt,
}

/// Owns one fresh MySQL generation and its sole retained child.
///
/// Topology control methods remain bound to the currently attested private UDS
/// and do not independently publish client access or provide restart adoption.
pub struct MysqlInstanceManager {
    config: MysqlInstanceConfig,
    state: MysqlInstanceState,
    child: Option<Child>,
    data_root: Option<OwnedRoot>,
    scratch_root: Option<OwnedRoot>,
    process_generation: u64,
    cleanup_quarantined: bool,
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
            process_generation: 0,
            cleanup_quarantined: false,
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
        self.initialize_bounded(None)
    }

    /// Performs fresh initialization while observing the shared attempt work
    /// cutoff and cancellation during every child poll.
    pub fn initialize_with_attempt(
        &mut self,
        attempt: &AttemptContext,
    ) -> Result<(), MysqlInstanceError> {
        self.initialize_bounded(Some(attempt))
    }

    fn initialize_bounded(
        &mut self,
        attempt: Option<&AttemptContext>,
    ) -> Result<(), MysqlInstanceError> {
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
        let result = self.initialize_inner(attempt);
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

    fn initialize_inner(
        &mut self,
        attempt: Option<&AttemptContext>,
    ) -> Result<(), MysqlInstanceError> {
        if let Some(attempt) = attempt {
            verify_product_with_attempt(&self.config, &mut self.child, attempt)?;
        } else {
            verify_product(&self.config, &mut self.child)?;
        }
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
        let child = self
            .child
            .as_mut()
            .expect("initializer child retained before waiting");
        let result = if let Some(attempt) = attempt {
            wait_bounded_with_attempt(
                child,
                self.config.timeouts().initialization(),
                self.config.timeouts().shutdown(),
                LifecycleOperation::Initialization,
                attempt,
            )
        } else {
            wait_bounded(
                child,
                self.config.timeouts().initialization(),
                self.config.timeouts().shutdown(),
                LifecycleOperation::Initialization,
            )
        };
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
        self.start_bounded(None)
    }

    /// Starts the exact child while observing the shared attempt work cutoff
    /// and cancellation during readiness polling.
    pub fn start_with_attempt(
        &mut self,
        attempt: &AttemptContext,
    ) -> Result<(), MysqlInstanceError> {
        self.start_bounded(Some(attempt))
    }

    fn start_bounded(
        &mut self,
        attempt: Option<&AttemptContext>,
    ) -> Result<(), MysqlInstanceError> {
        self.require_state(MysqlInstanceState::Initialized)?;
        self.config.revalidate_executables()?;
        if let Err(error) = self.revalidate_roots() {
            self.state = MysqlInstanceState::Faulted;
            return Err(error);
        }
        let next_generation =
            self.process_generation
                .checked_add(1)
                .ok_or(MysqlInstanceError::Ownership(
                    crate::service::OwnershipError::InspectionFailed,
                ))?;
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
        if let Err(prior) = self.wait_until_ready(attempt) {
            self.state = MysqlInstanceState::Faulted;
            let cleanup = self
                .child
                .as_mut()
                .map(|child| {
                    if let Some(attempt) = attempt {
                        terminate_and_reap_before(child, attempt.deadline())
                    } else {
                        terminate_and_reap(child, self.config.timeouts().shutdown())
                    }
                })
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
        self.process_generation = next_generation;
        self.state = MysqlInstanceState::Running;
        Ok(())
    }

    fn wait_until_ready(
        &mut self,
        attempt: Option<&AttemptContext>,
    ) -> Result<(), MysqlInstanceError> {
        let local_deadline = checked_deadline(
            self.config.timeouts().startup(),
            LifecycleOperation::Startup,
        )?;
        let deadline = attempt.map_or(local_deadline, |attempt| {
            local_deadline.min(attempt.work_deadline())
        });
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
            if let Some(error) = attempt.and_then(|attempt| attempt.admit_work().err()) {
                return Err(MysqlInstanceError::Attempt(error));
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
    pub(crate) async fn provision_topology_accounts(
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

    /// Provisions the existing private observer/recovery identities and
    /// enrolls this exact running generation without starting Group
    /// Replication. This is the one-member control preparation used by the
    /// bounded proxy compatibility gate.
    pub async fn enroll_local_control_authority(
        &mut self,
        attempt: &AttemptContext,
        observer: &ControlCredential,
        recovery: &ControlCredential,
    ) -> Result<MemberMutationAuthority, MemberTransitionError> {
        attempt
            .admit_work()
            .map_err(member_attempt_admission_error)?;
        let binding = self.topology_binding(
            attempt.attempt().clone(),
            observer.generation().clone(),
            recovery.generation().clone(),
        )?;
        let deadline = attempt
            .native_deadline()
            .map_err(MemberTransitionError::Native)?;
        let accounts = self
            .provision_topology_accounts(binding.clone(), observer, recovery, &deadline)
            .await?;
        let enrollment = self
            .enroll_topology_identity(binding, &accounts, &deadline)
            .await?;
        MemberMutationAuthority::new(
            attempt.attempt().clone(),
            GroupName::new(self.config.member().group_uuid().to_owned()).map_err(|_| {
                MemberTransitionError::Native(MysqlTopologyError::OwnershipContextLoss)
            })?,
            enrollment,
        )
    }

    /// Enrolls `@@server_uuid` before this member enters any topology effect.
    pub(crate) async fn enroll_topology_identity(
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
    pub(crate) async fn bootstrap_group_replication(
        &mut self,
        capability: BootstrapCapability,
        recovery: &ControlCredential,
        attempt: &AttemptContext,
    ) -> Result<BootstrapEffect, MemberTransitionError> {
        let input = capability.member_input()?;
        self.bootstrap_member(input, recovery, attempt).await?;
        capability
            .record_effect(BootstrapEffect::required_steps().to_vec())
            .map_err(MemberTransitionError::Native)
    }

    /// Enters one non-bootstrap join with process-memory-only recovery
    /// credentials.
    pub(crate) async fn join_group_replication(
        &mut self,
        capability: JoinCapability,
        recovery: &ControlCredential,
        attempt: &AttemptContext,
    ) -> Result<JoinEffect, MemberTransitionError> {
        let input = capability.member_input()?;
        self.join_member(input, recovery, attempt).await?;
        capability
            .record_effect(JoinEffect::required_steps().to_vec())
            .map_err(MemberTransitionError::Native)
    }

    /// Discovers a proposed post-effect view without replacing enrollment or
    /// granting lifecycle credit.
    pub(crate) async fn discover_topology_view(
        &mut self,
        enrollment: NativeIdentityEnrollment,
        group_name: GroupName,
        deadline: &NativeControlDeadline,
    ) -> Result<ViewDiscovery, MysqlTopologyError> {
        let target = self.prepare_control_target()?;
        crate::service::control::discover_view(target, enrollment, group_name, deadline).await
    }

    /// Executes one fresh member-scoped bootstrap and returns creditable
    /// successor evidence only after exact local validation.
    pub async fn bootstrap_member(
        &mut self,
        input: FreshBootstrapInput,
        recovery: &ControlCredential,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError> {
        let target = self.prepare_member_effect_target(input.authority(), attempt)?;
        let request = input.request().cloned();
        let authority = input.authority().clone();
        let mut containment = MemberEffectCancellationContainment::new(self, attempt);
        let result = async {
            let deadline = Self::member_effect_deadline(attempt)?;
            crate::service::control::member_bootstrap(
                target.clone(),
                &authority,
                recovery,
                &deadline,
            )
            .await?;
            poll_member_receipt(
                &mut containment,
                target,
                MemberEffectKind::Bootstrap,
                MemberReceiptContext {
                    authority,
                    request,
                    predecessor: None,
                },
                attempt,
                &deadline,
            )
            .await
        }
        .await;
        containment.finish(result)
    }

    /// Executes one fresh member-scoped non-bootstrap join without acquiring
    /// ownership of any predecessor process.
    pub async fn join_member(
        &mut self,
        input: FreshJoinInput,
        recovery: &ControlCredential,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError> {
        let target = self.prepare_member_effect_target(input.authority(), attempt)?;
        let request = input.request().cloned();
        let authority = input.authority().clone();
        let predecessor = input.source().evidence().clone();
        let mut containment = MemberEffectCancellationContainment::new(self, attempt);
        let result = async {
            let deadline = Self::member_effect_deadline(attempt)?;
            crate::service::control::member_join(target.clone(), &authority, recovery, &deadline)
                .await?;
            poll_member_receipt(
                &mut containment,
                target,
                MemberEffectKind::Join,
                MemberReceiptContext {
                    authority,
                    request,
                    predecessor: Some(predecessor),
                },
                attempt,
                &deadline,
            )
            .await
        }
        .await;
        containment.finish(result)
    }

    /// Reads one bracketed coherent native member view after exact local
    /// process, root, socket, endpoint, storage, and enrollment validation.
    pub async fn read_member_view(
        &mut self,
        authority: &MemberMutationAuthority,
        attempt: &AttemptContext,
    ) -> Result<MemberNativeEvidence, MemberTransitionError> {
        let target = self.prepare_member_effect_target(authority, attempt)?;
        let deadline = Self::member_effect_deadline(attempt)?;
        crate::service::control::read_member_view(target, authority, &deadline).await
    }

    /// Reads one request-bound coherent native member view and returns an
    /// exact receipt suitable for lifecycle credit.
    pub async fn read_member_view_for_request(
        &mut self,
        request: MemberEffectRequest,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError> {
        let authority = request.authority().clone();
        let target = self.prepare_member_effect_target(&authority, attempt)?;
        let deadline = Self::member_effect_deadline(attempt)?;
        let evidence =
            crate::service::control::read_member_view(target.clone(), &authority, &deadline)
                .await?;
        let final_binding = self.revalidate_member_effect_binding(&authority)?;
        target.revalidate_socket()?;
        MemberEffectReceipt::for_request(
            MemberEffectKind::ViewReadback,
            request,
            None,
            evidence,
            &final_binding,
            attempt,
            AttemptCleanupState::NotRequired,
        )
    }

    /// Transfers native primary authority from this exact local source to one
    /// exact immutable target identity.
    pub async fn transfer_primary(
        &mut self,
        input: PrimaryTransferInput,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError> {
        let target = self.prepare_member_effect_target(input.authority(), attempt)?;
        let request = input.request().cloned();
        let authority = input.authority().clone();
        let predecessor = input.source().evidence().clone();
        let transfer_target = input.target().clone();
        let mut containment = MemberEffectCancellationContainment::new(self, attempt);
        let result = async {
            let deadline = Self::member_effect_deadline(attempt)?;
            let current =
                crate::service::control::read_member_view(target.clone(), &authority, &deadline)
                    .await?;
            if current != predecessor {
                return Err(MemberTransitionError::View(
                    crate::service::MemberViewError::StaleView,
                ));
            }
            crate::service::control::transfer_primary(
                target.clone(),
                &authority,
                &transfer_target,
                &deadline,
            )
            .await?;
            poll_member_receipt(
                &mut containment,
                target,
                MemberEffectKind::PrimaryTransfer {
                    target: transfer_target.member_id().clone(),
                },
                MemberReceiptContext {
                    authority,
                    request,
                    predecessor: Some(predecessor),
                },
                attempt,
                &deadline,
            )
            .await
        }
        .await;
        containment.finish(result)
    }

    /// Terminates one exact local native backend connection and proves it is
    /// absent before returning.
    pub async fn terminate_backend_connection(
        &mut self,
        authority: &MemberMutationAuthority,
        connection: BackendConnectionId,
        attempt: &AttemptContext,
    ) -> Result<BackendConnectionReceipt, MemberTransitionError> {
        let target = self.prepare_member_effect_target(authority, attempt)?;
        let authority = authority.clone();
        let mut containment = MemberEffectCancellationContainment::new(self, attempt);
        let result = async {
            let deadline = Self::member_effect_deadline(attempt)?;
            crate::service::control::terminate_backend_connection(
                target.clone(),
                &authority,
                std::num::NonZeroU64::new(connection.get())
                    .expect("BackendConnectionId is nonzero"),
                &deadline,
            )
            .await?;
            crate::service::control::read_member_view(target.clone(), &authority, &deadline)
                .await?;
            let final_binding = containment.revalidate_binding(&authority)?;
            target.revalidate_socket()?;
            BackendConnectionReceipt::new(
                authority,
                connection,
                &final_binding,
                attempt,
                AttemptCleanupState::Completed,
            )
        }
        .await;
        containment.finish(result)
    }

    fn member_effect_deadline(
        attempt: &AttemptContext,
    ) -> Result<NativeControlDeadline, MemberTransitionError> {
        attempt
            .admit_work()
            .map_err(member_attempt_admission_error)?;
        attempt
            .native_deadline()
            .map_err(MemberTransitionError::Native)
    }

    fn prepare_member_effect_target(
        &mut self,
        authority: &MemberMutationAuthority,
        attempt: &AttemptContext,
    ) -> Result<OwnedControlTarget, MemberTransitionError> {
        if attempt.attempt() != authority.attempt() {
            return Err(MemberTransitionError::Authority(
                MemberAuthorityError::AttemptMismatch,
            ));
        }
        let target = self.prepare_control_target()?;
        self.revalidate_member_effect_binding(authority)?;
        Ok(target)
    }

    fn revalidate_member_effect_binding(
        &mut self,
        authority: &MemberMutationAuthority,
    ) -> Result<MemberControlBinding, MemberTransitionError> {
        let binding = self.topology_binding(
            authority.attempt().clone(),
            authority.local().binding().observer_generation().clone(),
            authority.local().binding().recovery_generation().clone(),
        )?;
        if &binding != authority.local().binding()
            || self.config.member_index() != authority.member()
            || self.config.member().group_uuid() != authority.group_name().as_str()
            || self.config.member().sql_address().to_string()
                != authority.local().binding().member_address().as_str()
        {
            return Err(MemberTransitionError::Authority(
                MemberAuthorityError::LocalBindingMismatch,
            ));
        }
        Ok(binding)
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

    #[cfg(test)]
    pub(super) fn qualification_stop_preserving_roots(
        &mut self,
        kind: QualificationStopKind,
    ) -> Result<(), MysqlInstanceError> {
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
        let child_id = child.id();
        let pid_metadata = fs::symlink_metadata(self.config.runtime().pid()).map_err(|error| {
            self.state = MysqlInstanceState::Faulted;
            MysqlInstanceError::Io {
                operation: LifecycleOperation::Shutdown,
                kind: error.kind(),
            }
        })?;
        if pid_metadata.file_type().is_symlink()
            || !pid_metadata.file_type().is_file()
            || fs::read_to_string(self.config.runtime().pid())
                .ok()
                .and_then(|value| value.trim().parse::<u32>().ok())
                != Some(child_id)
        {
            self.state = MysqlInstanceState::Faulted;
            return Err(MysqlInstanceError::Ownership(
                crate::service::OwnershipError::InvalidPidFile,
            ));
        }
        let pid_identity = (pid_metadata.dev(), pid_metadata.ino());
        let abrupt_socket_identity = if kind == QualificationStopKind::Abrupt {
            let metadata =
                fs::symlink_metadata(self.config.runtime().socket()).map_err(|error| {
                    self.state = MysqlInstanceState::Faulted;
                    MysqlInstanceError::Io {
                        operation: LifecycleOperation::Shutdown,
                        kind: error.kind(),
                    }
                })?;
            if metadata.file_type().is_symlink() || !metadata.file_type().is_socket() {
                self.state = MysqlInstanceState::Faulted;
                return Err(MysqlInstanceError::Ownership(
                    crate::service::OwnershipError::InspectionFailed,
                ));
            }
            Some((metadata.dev(), metadata.ino()))
        } else {
            None
        };
        let stop = match kind {
            QualificationStopKind::Graceful => {
                terminate_and_reap(child, self.config.timeouts().shutdown())
            }
            QualificationStopKind::Abrupt => {
                qualification_kill_and_reap(child, self.config.timeouts().shutdown())
            }
        };
        if let Err(error) = stop {
            self.state = MysqlInstanceState::Faulted;
            return Err(error);
        }
        self.child = None;
        if kind == QualificationStopKind::Abrupt {
            match fs::symlink_metadata(self.config.runtime().socket()) {
                Ok(metadata)
                    if !metadata.file_type().is_symlink() && metadata.file_type().is_socket() =>
                {
                    if abrupt_socket_identity != Some((metadata.dev(), metadata.ino())) {
                        self.state = MysqlInstanceState::Faulted;
                        self.cleanup_quarantined = true;
                        return Err(MysqlInstanceError::Ownership(
                            crate::service::OwnershipError::RootMismatch,
                        ));
                    }
                    fs::remove_file(self.config.runtime().socket()).map_err(|error| {
                        self.state = MysqlInstanceState::Faulted;
                        MysqlInstanceError::Io {
                            operation: LifecycleOperation::SocketDisappearance,
                            kind: error.kind(),
                        }
                    })?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Ok(_) => {
                    self.state = MysqlInstanceState::Faulted;
                    self.cleanup_quarantined = true;
                    return Err(MysqlInstanceError::Ownership(
                        crate::service::OwnershipError::InspectionFailed,
                    ));
                }
                Err(error) => {
                    self.state = MysqlInstanceState::Faulted;
                    return Err(MysqlInstanceError::Io {
                        operation: LifecycleOperation::SocketDisappearance,
                        kind: error.kind(),
                    });
                }
            }
        }
        if let Err(error) = wait_for_absence(
            self.config.runtime().socket(),
            self.config.timeouts().socket_disappearance(),
        ) {
            self.state = MysqlInstanceState::Faulted;
            return Err(error);
        }
        match fs::symlink_metadata(self.config.runtime().pid()) {
            Ok(metadata)
                if !metadata.file_type().is_symlink()
                    && metadata.file_type().is_file()
                    && (metadata.dev(), metadata.ino()) == pid_identity
                    && fs::read_to_string(self.config.runtime().pid())
                        .ok()
                        .and_then(|value| value.trim().parse::<u32>().ok())
                        == Some(child_id) =>
            {
                fs::remove_file(self.config.runtime().pid()).map_err(|error| {
                    self.state = MysqlInstanceState::Faulted;
                    MysqlInstanceError::Io {
                        operation: LifecycleOperation::Shutdown,
                        kind: error.kind(),
                    }
                })?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => {
                self.state = MysqlInstanceState::Faulted;
                return Err(MysqlInstanceError::Ownership(
                    crate::service::OwnershipError::InvalidPidFile,
                ));
            }
            Err(error) => {
                self.state = MysqlInstanceState::Faulted;
                return Err(MysqlInstanceError::Io {
                    operation: LifecycleOperation::Shutdown,
                    kind: error.kind(),
                });
            }
        }
        self.state = MysqlInstanceState::Initialized;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn qualification_restart_retained(&mut self) -> Result<(), MysqlInstanceError> {
        self.require_state(MysqlInstanceState::Initialized)?;
        if self.child.is_some() {
            self.state = MysqlInstanceState::Faulted;
            return Err(MysqlInstanceError::Ownership(
                crate::service::OwnershipError::InspectionFailed,
            ));
        }
        for path in [self.config.runtime().socket(), self.config.runtime().pid()] {
            match fs::symlink_metadata(path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Ok(_) => {
                    self.state = MysqlInstanceState::Faulted;
                    return Err(if path == self.config.runtime().socket() {
                        MysqlInstanceError::SocketStillPresent
                    } else {
                        MysqlInstanceError::Ownership(
                            crate::service::OwnershipError::InvalidPidFile,
                        )
                    });
                }
                Err(error) => {
                    self.state = MysqlInstanceState::Faulted;
                    return Err(MysqlInstanceError::Io {
                        operation: LifecycleOperation::Startup,
                        kind: error.kind(),
                    });
                }
            }
        }
        self.start()
    }

    #[cfg(test)]
    pub(super) async fn qualification_start_group_replication(
        &mut self,
        recovery: &ControlCredential,
        deadline: &NativeControlDeadline,
    ) -> Result<(), MysqlTopologyError> {
        let target = self.prepare_control_target()?;
        crate::service::control::qualification_start(target, recovery, deadline).await
    }

    #[cfg(test)]
    pub(super) fn qualification_child_pid(&self) -> Option<u32> {
        self.child.as_ref().map(std::process::Child::id)
    }

    #[cfg(test)]
    pub(super) async fn qualification_mutate_recovery(
        &mut self,
        current: &ControlCredential,
        mutation: crate::service::control::QualificationRecoveryMutation<'_>,
        deadline: &NativeControlDeadline,
    ) -> Result<(), MysqlTopologyError> {
        let target = self.prepare_control_target()?;
        crate::service::control::qualification_mutate_recovery(target, current, mutation, deadline)
            .await
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
        if self.config.topology().is_none() {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::TopologyConfigurationRequired,
            ));
        }
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
            "member-{}-generation-{}-pid-{}-scratch-{}-{}",
            self.config.member_index().as_usize() + 1,
            self.process_generation,
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
        let member = self.config.member();
        let member_index = self.config.member_index();
        let member_address = MemberAddress::new(member.sql_address().to_string())
            .map_err(|_| MysqlTopologyError::OwnershipContextLoss)?;
        Ok(MemberControlBinding::new(
            attempt,
            member_index,
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
    ///
    /// Persistent data is retained unless fresh initialization itself failed
    /// before a usable generation was established.
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
        if self.cleanup_quarantined {
            return Err(MysqlInstanceError::Ownership(
                crate::service::OwnershipError::RootMismatch,
            ));
        }
        if let Some(scratch) = self.scratch_root.as_ref() {
            remove_owned_root(scratch, LifecycleOperation::ScratchCleanup)?;
            self.scratch_root = None;
        }
        self.state = MysqlInstanceState::Stopped;
        Ok(())
    }

    fn contain_with_attempt(
        &mut self,
        attempt: &AttemptContext,
    ) -> Result<AttemptCleanupState, MysqlInstanceError> {
        attempt
            .admit_cleanup()
            .map_err(|_| MysqlInstanceError::Timeout(LifecycleOperation::Shutdown))?;
        self.contain_before(attempt.deadline())?;
        Ok(AttemptCleanupState::Completed)
    }

    fn contain_before(&mut self, deadline: Instant) -> Result<(), MysqlInstanceError> {
        match self.state {
            MysqlInstanceState::Configured | MysqlInstanceState::Stopped => return Ok(()),
            MysqlInstanceState::Running
            | MysqlInstanceState::Initialized
            | MysqlInstanceState::Faulted => {}
        }
        ensure_cleanup_deadline(deadline, LifecycleOperation::Shutdown)?;
        if self.child.is_none() && self.data_root.is_none() && self.scratch_root.is_none() {
            self.state = MysqlInstanceState::Stopped;
            return Ok(());
        }
        self.revalidate_roots()?;
        if let Some(child) = self.child.as_mut() {
            attest_child(child, &self.config)?;
            terminate_and_reap_before(child, deadline)?;
            self.child = None;
            wait_for_absence_before(self.config.runtime().socket(), deadline)?;
        }
        self.revalidate_roots()?;
        if self.cleanup_quarantined {
            return Err(MysqlInstanceError::Ownership(
                crate::service::OwnershipError::RootMismatch,
            ));
        }
        ensure_cleanup_deadline(deadline, LifecycleOperation::ScratchCleanup)?;
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

impl MysqlMemberEffectBoundary for MysqlInstanceManager {
    fn config(&self) -> &MysqlInstanceConfig {
        self.config()
    }

    async fn bootstrap_member(
        &mut self,
        input: FreshBootstrapInput,
        recovery: &ControlCredential,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError> {
        MysqlInstanceManager::bootstrap_member(self, input, recovery, attempt).await
    }

    async fn join_member(
        &mut self,
        input: FreshJoinInput,
        recovery: &ControlCredential,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError> {
        MysqlInstanceManager::join_member(self, input, recovery, attempt).await
    }

    async fn read_member_view(
        &mut self,
        request: MemberEffectRequest,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError> {
        MysqlInstanceManager::read_member_view_for_request(self, request, attempt).await
    }

    async fn transfer_primary(
        &mut self,
        input: PrimaryTransferInput,
        attempt: &AttemptContext,
    ) -> Result<MemberEffectReceipt, MemberTransitionError> {
        MysqlInstanceManager::transfer_primary(self, input, attempt).await
    }

    async fn terminate_backend_connection(
        &mut self,
        authority: &MemberMutationAuthority,
        connection: BackendConnectionId,
        attempt: &AttemptContext,
    ) -> Result<BackendConnectionReceipt, MemberTransitionError> {
        MysqlInstanceManager::terminate_backend_connection(self, authority, connection, attempt)
            .await
    }

    fn contain_member(
        &mut self,
        attempt: &AttemptContext,
    ) -> Result<AttemptCleanupState, MysqlInstanceError> {
        self.contain_with_attempt(attempt)
    }

    fn contain_member_exact(
        &mut self,
        authority: &MemberMutationAuthority,
        attempt: &AttemptContext,
    ) -> Result<crate::service::MemberContainmentReceipt, MysqlInstanceError> {
        self.revalidate_member_effect_binding(authority)
            .map_err(|error| {
                MysqlInstanceError::Ownership(match error {
                    MemberTransitionError::Authority(
                        crate::service::MemberAuthorityError::LocalBindingMismatch
                        | crate::service::MemberAuthorityError::AttemptMismatch
                        | crate::service::MemberAuthorityError::AuthoritySubstitution
                        | crate::service::MemberAuthorityError::SourceMismatch
                        | crate::service::MemberAuthorityError::WrongTransferTarget,
                    ) => crate::service::OwnershipError::RootMismatch,
                    _ => crate::service::OwnershipError::InspectionFailed,
                })
            })?;
        let cleanup = self.contain_with_attempt(attempt)?;
        let process_absent = self.child.is_none();
        let pid_absent =
            exact_path_absent(self.config.runtime().pid(), LifecycleOperation::Shutdown)?;
        let socket_absent = exact_path_absent(
            self.config.runtime().socket(),
            LifecycleOperation::SocketDisappearance,
        )?;
        let scratch_removed = self.scratch_root.is_none()
            && exact_path_absent(
                self.config.runtime().scratch_root(),
                LifecycleOperation::ScratchCleanup,
            )?;
        Ok(crate::service::MemberContainmentReceipt::new(
            authority.clone(),
            cleanup,
            process_absent,
            pid_absent,
            socket_absent,
            socket_absent,
            scratch_removed,
        ))
    }
}

fn exact_path_absent(
    path: &Path,
    operation: LifecycleOperation,
) -> Result<bool, MysqlInstanceError> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Ok(_) => Ok(false),
        Err(error) => Err(MysqlInstanceError::Io {
            operation,
            kind: error.kind(),
        }),
    }
}

impl MysqlProxyControlBoundary for MysqlInstanceManager {
    fn contain_proxy_preparation(
        &mut self,
        authority: &MemberMutationAuthority,
        attempt: &AttemptContext,
    ) -> Result<(), MysqlProxyError> {
        let containment = self
            .contain_member_exact(authority, attempt)
            .map_err(|_| MysqlProxyError::InvalidState)?;
        if containment.is_complete() {
            Ok(())
        } else {
            Err(MysqlProxyError::InvalidState)
        }
    }

    fn proxy_backend_socket(
        &mut self,
        authority: &MemberMutationAuthority,
    ) -> Result<UnixSocketPath, MysqlProxyError> {
        let target = self
            .prepare_control_target()
            .map_err(MysqlProxyError::Account)?;
        self.revalidate_member_effect_binding(authority)
            .map_err(MysqlProxyError::Native)?;
        target
            .revalidate_socket()
            .map_err(MysqlProxyError::Native)?;
        Ok(target.socket())
    }

    async fn prepare_application_account(
        &mut self,
        authority: &MemberMutationAuthority,
        credential: &ApplicationCredential,
        attempt: &AttemptContext,
    ) -> Result<ApplicationAccountReceipt, MysqlProxyError> {
        attempt.admit_work().map_err(|error| match error {
            AttemptAdmissionError::Cancelled => MysqlProxyError::Cancelled,
            AttemptAdmissionError::CleanupReserve | AttemptAdmissionError::Expired => {
                MysqlProxyError::Deadline(MysqlProxyPhase::AccountLifecycle)
            }
        })?;
        let target = self
            .prepare_member_effect_target(authority, attempt)
            .map_err(MysqlProxyError::Native)?;
        let mut cancellation_containment = MemberEffectCancellationContainment::new(self, attempt);
        let result = crate::service::control::prepare_application_account(
            target,
            authority,
            credential,
            attempt.work_deadline(),
            attempt.deadline(),
        )
        .await
        .map_err(MysqlProxyError::Account);
        cancellation_containment.disarm();
        result
    }

    async fn remove_application_account(
        &mut self,
        authority: &MemberMutationAuthority,
        receipt: &ApplicationAccountReceipt,
        attempt: &AttemptContext,
    ) -> Result<(), MysqlProxyError> {
        attempt
            .admit_cleanup()
            .map_err(|_| MysqlProxyError::Deadline(MysqlProxyPhase::AccountLifecycle))?;
        let target = self
            .prepare_control_target()
            .map_err(MysqlProxyError::Account)?;
        self.revalidate_member_effect_binding(authority)
            .map_err(MysqlProxyError::Native)?;
        let deadline = cleanup_deadline(attempt, MysqlProxyPhase::AccountLifecycle)?;
        crate::service::control::remove_application_account(target, authority, receipt, deadline)
            .await
            .map_err(MysqlProxyError::Account)
    }

    async fn terminate_proxy_backend(
        &mut self,
        authority: &MemberMutationAuthority,
        session: &crate::service::BackendSessionIdentity,
        attempt: &AttemptContext,
        revocation_deadline: Instant,
    ) -> Result<(), MysqlProxyError> {
        attempt
            .admit_cleanup()
            .map_err(|_| MysqlProxyError::Deadline(MysqlProxyPhase::Revocation))?;
        let target = self
            .prepare_control_target()
            .map_err(MysqlProxyError::Account)?;
        self.revalidate_member_effect_binding(authority)
            .map_err(MysqlProxyError::Native)?;
        let deadline =
            cleanup_deadline(attempt, MysqlProxyPhase::Revocation)?.min(revocation_deadline);
        if deadline <= Instant::now() {
            return Err(MysqlProxyError::Deadline(MysqlProxyPhase::Revocation));
        }
        crate::service::control::terminate_proxy_backend_before(
            target, authority, session, deadline,
        )
        .await
        .map_err(MysqlProxyError::Native)
    }
}

fn cleanup_deadline(
    attempt: &AttemptContext,
    phase: MysqlProxyPhase,
) -> Result<Instant, MysqlProxyError> {
    let now = Instant::now();
    let local = now
        .checked_add(Duration::from_secs(20))
        .ok_or(MysqlProxyError::Deadline(phase))?;
    let deadline = local.min(attempt.deadline());
    if deadline <= now {
        Err(MysqlProxyError::Deadline(phase))
    } else {
        Ok(deadline)
    }
}

struct MemberReceiptContext {
    authority: MemberMutationAuthority,
    request: Option<MemberEffectRequest>,
    predecessor: Option<MemberNativeEvidence>,
}

async fn poll_member_receipt(
    containment: &mut MemberEffectCancellationContainment<'_>,
    target: OwnedControlTarget,
    kind: MemberEffectKind,
    context: MemberReceiptContext,
    attempt: &AttemptContext,
    deadline: &NativeControlDeadline,
) -> Result<MemberEffectReceipt, MemberTransitionError> {
    let MemberReceiptContext {
        authority,
        request,
        predecessor,
    } = context;
    let cleanup = if kind == MemberEffectKind::Bootstrap {
        AttemptCleanupState::Completed
    } else {
        AttemptCleanupState::NotRequired
    };
    loop {
        attempt
            .admit_work()
            .map_err(member_attempt_admission_error)?;
        let evidence =
            crate::service::control::read_member_view(target.clone(), &authority, deadline).await?;
        let final_binding = containment.revalidate_binding(&authority)?;
        target.revalidate_socket()?;
        let receipt = if let Some(request) = request.clone() {
            MemberEffectReceipt::for_request(
                kind.clone(),
                request,
                predecessor.clone(),
                evidence,
                &final_binding,
                attempt,
                cleanup,
            )
        } else {
            MemberEffectReceipt::new(
                kind.clone(),
                authority.clone(),
                predecessor.clone(),
                evidence,
                &final_binding,
                attempt,
                cleanup,
            )
        };
        match receipt {
            Ok(receipt) => return Ok(receipt),
            Err(MemberTransitionError::Receipt(
                crate::service::MemberReceiptError::SuccessorMismatch,
            ))
            | Err(MemberTransitionError::View(
                crate::service::MemberViewError::MemberNotOnline
                | crate::service::MemberViewError::MemberRoleMismatch,
            )) => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn member_attempt_admission_error(error: AttemptAdmissionError) -> MemberTransitionError {
    match error {
        AttemptAdmissionError::Cancelled => MemberTransitionError::Cancelled,
        AttemptAdmissionError::CleanupReserve => {
            MemberTransitionError::Deadline(AttemptDeadlineState::CleanupReserve)
        }
        AttemptAdmissionError::Expired => {
            MemberTransitionError::Deadline(AttemptDeadlineState::Expired)
        }
    }
}

struct MemberEffectCancellationContainment<'a> {
    instance: &'a mut MysqlInstanceManager,
    attempt: AttemptContext,
    armed: bool,
}

impl<'a> MemberEffectCancellationContainment<'a> {
    fn new(instance: &'a mut MysqlInstanceManager, attempt: &AttemptContext) -> Self {
        Self {
            instance,
            attempt: attempt.clone(),
            armed: true,
        }
    }

    fn revalidate_binding(
        &mut self,
        authority: &MemberMutationAuthority,
    ) -> Result<MemberControlBinding, MemberTransitionError> {
        self.instance.revalidate_member_effect_binding(authority)
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    fn finish<T>(
        mut self,
        result: Result<T, MemberTransitionError>,
    ) -> Result<T, MemberTransitionError> {
        match result {
            Ok(value) => {
                self.armed = false;
                Ok(value)
            }
            Err(primary) => {
                self.armed = false;
                let paired =
                    PairedAttemptOutcome::<T, MemberTransitionError, MysqlInstanceError>::new(
                        Err(primary),
                        self.instance.contain_with_attempt(&self.attempt),
                    );
                match paired.into_parts() {
                    (Err(primary), Ok(AttemptCleanupState::Completed)) => Err(primary),
                    (Err(primary), Err(cleanup)) => Err(MemberTransitionError::Cleanup {
                        primary: Box::new(primary),
                        cleanup: Box::new(cleanup),
                    }),
                    (Err(_), Ok(_)) => {
                        unreachable!("attempt containment only returns completed cleanup")
                    }
                    (Ok(_), _) => unreachable!("failure containment retains a primary error"),
                }
            }
        }
    }
}

impl Drop for MemberEffectCancellationContainment<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.instance.contain_with_attempt(&self.attempt);
        }
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

fn ensure_cleanup_deadline(
    deadline: Instant,
    operation: LifecycleOperation,
) -> Result<(), MysqlInstanceError> {
    if Instant::now() < deadline {
        Ok(())
    } else {
        Err(MysqlInstanceError::Timeout(operation))
    }
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
        let config = MysqlInstanceConfig::new_topology_member(
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
