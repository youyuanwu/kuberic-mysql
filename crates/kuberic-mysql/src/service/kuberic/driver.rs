use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as SyncMutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use kuberic_runtime::application::{INVALID_LSN, OperationDataStream, StateProvider};
use kuberic_runtime::protocol::public_operations::{
    RemoteBuildTargetRebuildRequired, ReplicaRebuildReason, ReplicaRebuildRequired,
};
use kuberic_runtime::protocol::types::{
    AccessStatus, ConfigurationId as PublicConfigurationId, Epoch, OperationId,
    ProcessSessionId as PublicProcessSessionId, ReplicaIdentity, ReplicaRole,
};
use kuberic_runtime::replicator::{
    PrimaryReplicator, ReplicaInformation, ReplicaSetConfiguration, ReplicaSetQuorumMode,
    Replicator, ReplicatorCapabilities, ReplicatorFactory, ReplicatorFactoryContext,
    ReplicatorInterfaces, ReplicatorSettings,
};
use kuberic_runtime::{Result, RuntimeError};
use tokio::sync::{Mutex as AsyncMutex, MutexGuard};

use crate::core::{
    AttemptId, AuthorityGeneration, ConfiguredGtidLineage, GroupReplicationAddress,
    ProgressProjection, QualifiedProgress, StorageBinding, project_progress,
};
use crate::service::{
    AttemptContext, BuildAttemptGroup, MemberPeer, MemberSourceBoundary, MysqlAccessIntent,
    MysqlInstanceManager, MysqlOpenReceipt, MysqlOperationIdentity, MysqlProxyAccessGuard,
    MysqlReplicaConfiguration, MysqlReplicaEpoch, MysqlReplicaRole, MysqlReplicaTarget,
    MysqlReplicator, MysqlReplicatorError, MysqlRuntimeAccessAuthority,
};

use super::{
    CallbackOperationLedger, EpochTranslator, KubericTranslationError, ReplicationEndpoint,
    build::{
        BuildBinding, BuildCallbackKey, BuildCheckpoint, BuildEndpointRegistration,
        BuildGeneration, BuildGenerationStatus, BuildGroupPermit, BuildJoinReceipt,
        BuildOrchestration, BuildRecoveryAccountReceipt, BuildRecoveryCredential,
        BuildResultLedger, BuildSourceBinding, BuildSourceCleanup, BuildTargetBinding,
        BuildTerminalResult, IdleBuildPreparation, lookup_build_endpoint,
    },
    translate_build_replica_information, translate_replica_identity, translate_replica_information,
    translate_replica_set_configuration, translate_role,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct EpochKey(i64, i64);

impl From<Epoch> for EpochKey {
    fn from(epoch: Epoch) -> Self {
        Self(epoch.data_loss_number, epoch.configuration_number)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum OperationKey {
    Open,
    AdvanceEpoch(EpochKey),
    FormationRole(EpochKey, u8),
    Role(EpochKey, u8),
    Configuration(String),
    Progress(u64),
    Publish(EpochKey, String, u64),
    Revoke(u64),
    Build(String),
    BuildSuccessor(String),
    Close,
    Abort,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OperationValue {
    Open,
    Epoch(MysqlReplicaEpoch),
    Role(MysqlReplicaEpoch, MysqlReplicaRole),
    Configuration(MysqlReplicaConfiguration),
    Progress,
    Access(MysqlAccessIntent),
    Build(MysqlReplicaTarget),
    BuildSuccessor(MysqlReplicaConfiguration),
    Terminal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PublicBinding {
    identity: ReplicaIdentity,
    process_session: PublicProcessSessionId,
    public_storage: Option<StorageBinding>,
    settings: ReplicatorSettings,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RetainedReplicaProgress {
    identity: ReplicaIdentity,
    process_session: PublicProcessSessionId,
    storage: kuberic_runtime::protocol::types::ReplicaStorageIncarnationContext,
    current: i64,
    catch_up: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProxyState {
    Unprepared,
    Prepared(SocketAddr),
    Published {
        endpoint: SocketAddr,
        epoch: Epoch,
        configuration: PublicConfigurationId,
    },
    Revoked,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TerminalFailure {
    Engine(String),
}

impl TerminalFailure {
    fn diagnostic(&self) -> &str {
        match self {
            Self::Engine(diagnostic) => diagnostic,
        }
    }

    fn runtime_error(&self) -> RuntimeError {
        RuntimeError::InvalidReplication(self.diagnostic().into())
    }
}

struct DriverState {
    engine: MysqlReplicator<MysqlInstanceManager>,
    open_receipt: MysqlOpenReceipt,
    binding: Option<PublicBinding>,
    epochs: EpochTranslator,
    operations: CallbackOperationLedger<OperationKey, OperationValue>,
    build_results: BuildResultLedger<BuildCallbackKey>,
    build_generation: Arc<BuildGeneration>,
    pending_build_target: Option<BuildTargetBinding>,
    idle_build_preparation: IdleBuildPreparation,
    retained_replica_progress: Vec<RetainedReplicaProgress>,
    progress_sequence: u64,
    last_reported_progress: Option<i64>,
    fresh_open_progress: Option<QualifiedProgress>,
    current_configuration: Option<ReplicaSetConfiguration>,
    pending_configuration: Option<ReplicaSetConfiguration>,
    pending_native_configuration: Option<MysqlReplicaConfiguration>,
    cached_source_boundary: Option<MemberSourceBoundary>,
    cached_source_progress: Option<QualifiedProgress>,
    pre_epoch_role: Option<MysqlReplicaRole>,
    observed_roles: BTreeMap<EpochKey, ReplicaRole>,
    completed_roles: BTreeMap<EpochKey, ReplicaRole>,
    catch_up_proven: bool,
    access_generation: NonZeroU64,
    proxy: ProxyState,
    suppress_public_access: bool,
    opened: bool,
    closed: bool,
}

/// Member-local Kuberic Public V1 adapter over one exact MySQL owner.
///
/// Every callback is serialized through one async owner. The adapter is
/// therefore safe to share through `Arc` without creating another MySQL
/// process owner or another lifecycle engine.
pub struct MysqlPublicV1Replicator {
    callbacks: AsyncMutex<()>,
    state: SyncMutex<Option<DriverState>>,
    attempt: AttemptContext,
    group_key: String,
    endpoint: ReplicationEndpoint,
    proxy_listen_address: SocketAddr,
    abort_requested: AtomicBool,
    build_registration: SyncMutex<Option<BuildEndpointRegistration>>,
    terminal_failure: SyncMutex<Option<TerminalFailure>>,
}

struct DriverStateLease<'a> {
    owner: &'a MysqlPublicV1Replicator,
    _callbacks: MutexGuard<'a, ()>,
    state: Option<DriverState>,
}

pub(crate) struct BuildTargetOwner<'a> {
    state: Option<DriverStateLease<'a>>,
    token: Option<crate::service::MysqlBuildAbortGeneration>,
    generation: Arc<BuildGeneration>,
    armed: bool,
}

impl BuildTargetOwner<'_> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn join(
        &mut self,
        binding: &BuildBinding,
        operation: MysqlOperationIdentity,
        configuration: MysqlReplicaConfiguration,
        source: MemberSourceBoundary,
        peers: Vec<MemberPeer>,
        recovery: &BuildRecoveryCredential,
        build: &crate::service::BuildAttemptContext,
    ) -> Result<BuildJoinReceipt> {
        let state = self
            .state
            .as_mut()
            .expect("Build target owner retains its driver lease");
        if binding.target_generation != self.generation.id()
            || binding.child_attempt != *build.build()
            || binding.target_parent_attempt != *build.parent_attempt()
        {
            return Err(RuntimeError::AuthorityMismatch(
                "Build target owner differs from the exact request binding".into(),
            ));
        }
        if self.token.is_none() {
            self.token = Some(
                state
                    .engine
                    .arm_build_abort_generation()
                    .map_err(map_mysql)?,
            );
        }
        let authority = state.engine.current_member_authority().map_err(map_mysql)?;
        let local = authority.local();
        let local_target = MysqlReplicaTarget::new(
            state.engine.identity().clone(),
            MemberPeer::from_enrollment(local),
            local.binding().process_session().clone(),
            local.binding().endpoint().clone(),
            local.binding().storage().clone(),
        );
        let configuration = MysqlReplicaConfiguration::new(
            configuration.id().clone(),
            configuration.epoch(),
            configuration
                .members()
                .iter()
                .map(|member| {
                    if member.identity() == state.engine.identity() {
                        local_target.clone()
                    } else {
                        member.clone()
                    }
                })
                .collect(),
        );
        let receipt = state
            .engine
            .build_join(
                operation,
                configuration,
                source,
                peers,
                recovery,
                build,
                self.generation.id(),
            )
            .await
            .map_err(map_mysql)?;
        if receipt.build() != build.build()
            || receipt.parent_attempt() != build.parent_attempt()
            || receipt.target_generation() != self.generation.id()
        {
            return Err(RuntimeError::AuthorityMismatch(
                "Build join receipt differs from the target owner".into(),
            ));
        }
        let lineage = ConfiguredGtidLineage::new(binding.source_authority.group_name().as_str())
            .map_err(|error| RuntimeError::InvalidReplication(error.to_string()))?;
        match project_progress(&lineage, receipt.member().successor().executed()) {
            ProgressProjection::Projectable(progress) => {
                state.cached_source_progress = Some(progress);
                state.last_reported_progress = Some(progress.value());
                state.fresh_open_progress = None;
            }
            ProgressProjection::NonProjectable(_) => {
                return Err(RuntimeError::InvalidReplication(
                    "Build target history is not a qualified configured lineage".into(),
                ));
            }
        }
        state.pending_build_target = Some(binding.target.clone());
        Ok(receipt)
    }

    pub(crate) fn disarm(mut self) {
        self.armed = false;
        self.generation.set(BuildGenerationStatus::PendingAdmission);
    }

    pub(crate) async fn verify_recovery_account(
        &mut self,
        receipt: &super::build::BuildRecoveryAccountReceipt,
        expected_present: bool,
        build: &crate::service::BuildAttemptContext,
    ) -> Result<()> {
        self.state
            .as_mut()
            .expect("Build target owner retains its driver lease")
            .engine
            .verify_build_recovery_account(receipt, expected_present, build)
            .await
            .map_err(map_mysql)
    }

    pub(crate) async fn verify_fresh(
        &mut self,
        build: &crate::service::BuildAttemptContext,
    ) -> Result<()> {
        self.state
            .as_mut()
            .expect("Build target owner retains its driver lease")
            .engine
            .verify_build_target_fresh(build)
            .await
            .map_err(map_mysql)
    }

    pub(crate) async fn refresh_progress(
        &mut self,
        build: &crate::service::BuildAttemptContext,
    ) -> Result<QualifiedProgress> {
        build
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        let progress = tokio::time::timeout_at(
            tokio::time::Instant::from_std(build.work_deadline()),
            self.state
                .as_mut()
                .expect("Build target owner retains its driver lease")
                .engine
                .refresh_pending_build_progress(),
        )
        .await
        .map_err(|_| RuntimeError::OperationCancelled)?
        .map_err(map_mysql)?;
        build
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        match progress.progress() {
            ProgressProjection::Projectable(progress) => Ok(progress),
            ProgressProjection::NonProjectable(_) => Err(RuntimeError::InvalidReplication(
                "post-cleanup target history is not projectable".into(),
            )),
        }
    }

    pub(crate) async fn graceful_leave(
        &mut self,
        build: &crate::service::BuildAttemptContext,
    ) -> Result<()> {
        self.state
            .as_mut()
            .expect("Build target owner retains its driver lease")
            .engine
            .leave_build_member(build)
            .await
            .map_err(map_mysql)
    }
}

impl Drop for BuildTargetOwner<'_> {
    fn drop(&mut self) {
        if self.armed
            && let Some(token) = self.token.as_ref()
        {
            self.generation.set(BuildGenerationStatus::NeedsRebuild);
            if let Some(state) = self.state.as_mut() {
                let _ = state.engine.force_retire_owned_generation(token);
            }
        } else if self.armed {
            self.generation.set(BuildGenerationStatus::Available);
        }
    }
}

impl Deref for DriverStateLease<'_> {
    type Target = DriverState;

    fn deref(&self) -> &Self::Target {
        self.state
            .as_ref()
            .expect("driver state lease always owns state")
    }
}

impl DerefMut for DriverStateLease<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.state
            .as_mut()
            .expect("driver state lease always owns state")
    }
}

impl Drop for DriverStateLease<'_> {
    fn drop(&mut self) {
        let mut state = self
            .state
            .take()
            .expect("driver state lease always returns state");
        let mut slot = self
            .owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(slot.is_none());
        if self.owner.abort_requested.load(Ordering::Acquire) && !state.closed {
            self.owner.abort_state(&mut state);
        }
        *slot = Some(state);
    }
}

struct KubericProxyAccessGuard {
    context: ReplicatorFactoryContext,
}

struct DirectClosedAccessGuard;

impl MysqlProxyAccessGuard for DirectClosedAccessGuard {
    fn access_granted(&self) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
        Box::pin(async { false })
    }
}

impl MysqlProxyAccessGuard for KubericProxyAccessGuard {
    fn access_granted(&self) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
        Box::pin(async move {
            let read = self.context.read_status().await;
            let write = self.context.write_status().await;
            matches!(read, Ok(AccessStatus::Granted)) && matches!(write, Ok(AccessStatus::Granted))
        })
    }
}

impl MysqlPublicV1Replicator {
    /// Creates the adapter only for the service's admitted `OpenMode::New`
    /// freshly prepared owner.
    pub(super) fn new_fresh_open(
        engine: MysqlReplicator<MysqlInstanceManager>,
        open_receipt: MysqlOpenReceipt,
        attempt: AttemptContext,
        group_replication_address: GroupReplicationAddress,
        proxy_listen_address: SocketAddr,
    ) -> Result<Arc<Self>> {
        if engine.identity() != open_receipt.identity()
            || open_receipt.authority().attempt() != attempt.attempt()
            || !engine.attempt().shares_cancellation_source(&attempt)
        {
            return Err(RuntimeError::AuthorityMismatch(
                "prepared MySQL owner does not match its open receipt and attempt".into(),
            ));
        }
        let local = MemberPeer::from_enrollment(open_receipt.authority().local());
        let group_key = open_receipt.authority().group_name().as_str().to_owned();
        let endpoint = ReplicationEndpoint::new(
            local.member_id().clone(),
            local.address().clone(),
            group_replication_address,
        );
        let operation_authority =
            AuthorityGeneration::new(attempt.attempt().as_str()).map_err(|_| {
                RuntimeError::InvalidReplication(
                    "MySQL attempt cannot identify Public V1 callback operations".into(),
                )
            })?;
        let access_generation = open_receipt.access_generation();
        Ok(Arc::new(Self {
            callbacks: AsyncMutex::new(()),
            state: SyncMutex::new(Some(DriverState {
                engine,
                open_receipt,
                binding: None,
                epochs: EpochTranslator::new(),
                operations: CallbackOperationLedger::new(operation_authority),
                build_results: BuildResultLedger::default(),
                build_generation: Arc::new(BuildGeneration::new()),
                pending_build_target: None,
                idle_build_preparation: IdleBuildPreparation::default(),
                retained_replica_progress: Vec::new(),
                progress_sequence: 0,
                last_reported_progress: None,
                fresh_open_progress: None,
                current_configuration: None,
                pending_configuration: None,
                pending_native_configuration: None,
                cached_source_boundary: None,
                cached_source_progress: None,
                pre_epoch_role: None,
                observed_roles: BTreeMap::new(),
                completed_roles: BTreeMap::new(),
                catch_up_proven: false,
                access_generation,
                proxy: ProxyState::Unprepared,
                suppress_public_access: false,
                opened: false,
                closed: false,
            })),
            attempt,
            group_key,
            endpoint,
            proxy_listen_address,
            abort_requested: AtomicBool::new(false),
            build_registration: SyncMutex::new(None),
            terminal_failure: SyncMutex::new(None),
        }))
    }

    /// Opaque, non-secret endpoint published by `Replicator::open`.
    #[must_use]
    pub fn replication_endpoint(&self) -> String {
        self.endpoint.encode()
    }

    pub(crate) fn build_attempt_parent(&self) -> AttemptContext {
        self.attempt.clone()
    }

    pub(crate) async fn acquire_build_target(
        &self,
        group_key: &str,
        epoch: Epoch,
        target: &super::build::BuildTargetBinding,
        target_generation: u64,
        build: &crate::service::BuildAttemptContext,
    ) -> Result<BuildTargetOwner<'_>> {
        let mut state = tokio::time::timeout_at(
            tokio::time::Instant::from_std(build.work_deadline()),
            self.lease(),
        )
        .await
        .map_err(|_| RuntimeError::OperationCancelled)?;
        require_bound_open(&state)?;
        if state.build_generation.status() != BuildGenerationStatus::Available
            || state.engine.has_pending_build_successor()
        {
            return Err(RuntimeError::InvalidReplication(
                "target has a prior Build pending admission or cleanup".into(),
            ));
        }
        let public = state
            .binding
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let local = state.open_receipt.authority().local();
        let local_binding = local.binding();
        if group_key != self.group_key
            || target_generation != state.build_generation.id()
            || target.identity != public.identity
            || target.process_session != public.process_session
            || target.replication_address != self.endpoint.encode()
            || target.native.identity() != state.engine.identity()
            || public
                .public_storage
                .as_ref()
                .is_none_or(|storage| target.native.storage() != storage)
            || target.native.member().member_id() != local.member_id()
            || target.native.member().address() != local_binding.member_address()
            || state.idle_build_preparation.epoch() != Some(epoch)
            || !state.idle_build_preparation.is_idle()
            || state.current_configuration.is_some()
            || state.pending_configuration.is_some()
            || state.engine.configuration().is_some()
        {
            return Err(RuntimeError::AuthorityMismatch(
                "Build target binding differs from the prepared idle owner".into(),
            ));
        }

        state
            .engine
            .verify_build_target_fresh(build)
            .await
            .map_err(map_mysql)?;
        if !state.build_generation.reserve() {
            return Err(RuntimeError::InvalidReplication(
                "Build target generation is already reserved or terminal".into(),
            ));
        }
        let generation = state.build_generation.clone();
        Ok(BuildTargetOwner {
            state: Some(state),
            token: None,
            generation,
            armed: true,
        })
    }

    fn normalize_local_configuration(
        state: &DriverState,
        configuration: &MysqlReplicaConfiguration,
    ) -> Result<MysqlReplicaConfiguration> {
        let authority = state.engine.current_member_authority().map_err(map_mysql)?;
        let local = authority.local();
        let local_target = MysqlReplicaTarget::new(
            state.engine.identity().clone(),
            MemberPeer::from_enrollment(local),
            local.binding().process_session().clone(),
            local.binding().endpoint().clone(),
            local.binding().storage().clone(),
        );
        Ok(MysqlReplicaConfiguration::new(
            configuration.id().clone(),
            configuration.epoch(),
            configuration
                .members()
                .iter()
                .map(|member| {
                    if member.identity() == state.engine.identity() {
                        local_target.clone()
                    } else {
                        member.clone()
                    }
                })
                .collect(),
        ))
    }

    async fn observe_direct_build_successor(
        &self,
        build_id: &OperationId,
        configuration: &MysqlReplicaConfiguration,
        callback: &BuildCallbackKey,
        group: &BuildAttemptGroup,
    ) -> Result<()> {
        let child = group
            .child(self.attempt.clone())
            .map_err(|_| RuntimeError::OperationCancelled)?;
        child
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        let mut state = tokio::time::timeout_at(
            tokio::time::Instant::from_std(child.work_deadline()),
            self.lease(),
        )
        .await
        .map_err(|_| RuntimeError::OperationCancelled)?;
        self.require_direct_build_donor_binding(&state, callback)?;
        let configuration = Self::normalize_local_configuration(&state, configuration)?;
        let operation = operation(
            &mut state,
            OperationKey::BuildSuccessor(build_id.as_str().to_owned()),
            OperationValue::BuildSuccessor(configuration.clone()),
        )?;
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(child.work_deadline()),
            state
                .engine
                .observe_build_successor(operation, configuration),
        )
        .await
        .map_err(|_| RuntimeError::OperationCancelled)?
        .map_err(map_mysql)?;
        child
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        self.require_direct_build_donor_binding(&state, callback)?;
        state
            .build_generation
            .set(BuildGenerationStatus::PendingAdmission);
        state.pending_build_target = Some(callback.target.clone());
        Ok(())
    }

    pub(crate) async fn verify_direct_build_account(
        &self,
        receipt: &super::build::BuildRecoveryAccountReceipt,
        expected_present: bool,
        group: &BuildAttemptGroup,
        callback: &BuildCallbackKey,
    ) -> Result<()> {
        let child = group
            .child(self.attempt.clone())
            .map_err(|_| RuntimeError::OperationCancelled)?;
        if expected_present {
            child
                .admit_work()
                .map_err(|_| RuntimeError::OperationCancelled)?;
        } else {
            child
                .admit_cleanup()
                .map_err(|_| RuntimeError::OperationCancelled)?;
        }
        let deadline = if expected_present {
            child.work_deadline()
        } else {
            child.deadline()
        };
        let mut state =
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), self.lease())
                .await
                .map_err(|_| RuntimeError::OperationCancelled)?;
        self.require_direct_build_donor_binding(&state, callback)?;
        state
            .engine
            .verify_build_recovery_account(receipt, expected_present, &child)
            .await
            .map_err(map_mysql)?;
        if expected_present {
            child
                .admit_work()
                .map_err(|_| RuntimeError::OperationCancelled)?;
        } else {
            child
                .admit_cleanup()
                .map_err(|_| RuntimeError::OperationCancelled)?;
        }
        Ok(())
    }

    fn require_direct_build_donor_binding(
        &self,
        state: &DriverState,
        callback: &BuildCallbackKey,
    ) -> Result<()> {
        require_bound_open(state)?;
        let public = state
            .binding
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let expected = callback
            .source_configuration
            .replicas
            .iter()
            .find(|replica| {
                replica.identity == public.identity
                    && replica.replication_address == self.endpoint.encode()
            })
            .ok_or_else(|| {
                RuntimeError::AuthorityMismatch(
                    "Build donor is absent from the original callback binding".into(),
                )
            })?;
        let expected_native = translate_replica_information(expected).map_err(map_translation)?;
        if state
            .current_configuration
            .as_ref()
            .map(|current| &current.configuration)
            != Some(&callback.source_configuration.configuration)
            || expected.process_session_id != public.process_session
            || Some(expected_native.storage()) != public.public_storage.as_ref()
        {
            return Err(RuntimeError::AuthorityMismatch(
                "Build donor session, storage, or configuration changed".into(),
            ));
        }
        Ok(())
    }

    async fn enter_direct_build_cleanup(
        &self,
        callback: &BuildCallbackKey,
        group: &BuildAttemptGroup,
    ) -> Result<()> {
        let state = tokio::time::timeout_at(
            tokio::time::Instant::from_std(group.deadline()),
            self.lease(),
        )
        .await
        .map_err(|_| RuntimeError::OperationCancelled)?;
        self.require_direct_build_donor_binding(&state, callback)?;
        if state.build_generation.status() != BuildGenerationStatus::PendingAdmission {
            return Err(RuntimeError::AuthorityMismatch(
                "Build donor is not pending admission".into(),
            ));
        }
        state
            .build_generation
            .set(BuildGenerationStatus::CleanupOnly);
        Ok(())
    }

    async fn finish_direct_build_cleanup(
        &self,
        callback: &BuildCallbackKey,
        group: &BuildAttemptGroup,
    ) -> Result<()> {
        let state = tokio::time::timeout_at(
            tokio::time::Instant::from_std(group.deadline()),
            self.lease(),
        )
        .await
        .map_err(|_| RuntimeError::OperationCancelled)?;
        self.require_direct_build_donor_binding(&state, callback)?;
        if state.build_generation.status() != BuildGenerationStatus::CleanupOnly {
            return Err(RuntimeError::AuthorityMismatch(
                "Build donor is not in cleanup-only state".into(),
            ));
        }
        state
            .build_generation
            .set(BuildGenerationStatus::PendingAdmission);
        Ok(())
    }

    async fn observe_direct_build_progress(
        &self,
        callback: &BuildCallbackKey,
        group: &BuildAttemptGroup,
    ) -> Result<i64> {
        let child = group
            .child(self.attempt.clone())
            .map_err(|_| RuntimeError::OperationCancelled)?;
        child
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        let mut state = tokio::time::timeout_at(
            tokio::time::Instant::from_std(child.work_deadline()),
            self.lease(),
        )
        .await
        .map_err(|_| RuntimeError::OperationCancelled)?;
        self.require_direct_build_donor_binding(&state, callback)?;
        state.progress_sequence = state.progress_sequence.checked_add(1).ok_or_else(|| {
            RuntimeError::InvalidReplication("progress sequence exhausted".into())
        })?;
        let sequence = state.progress_sequence;
        let operation = operation(
            &mut state,
            OperationKey::Progress(sequence),
            OperationValue::Progress,
        )?;
        let progress = tokio::time::timeout_at(
            tokio::time::Instant::from_std(child.work_deadline()),
            state.engine.read_progress(operation),
        )
        .await
        .map_err(|_| RuntimeError::OperationCancelled)?
        .map_err(map_mysql)?;
        child
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        self.require_direct_build_donor_binding(&state, callback)?;
        match progress.progress() {
            ProgressProjection::Projectable(progress) => Ok(progress.value()),
            ProgressProjection::NonProjectable(_) => Err(RuntimeError::InvalidReplication(
                "Build donor history is not projectable".into(),
            )),
        }
    }

    async fn retains_complete_build_history(
        &self,
        callback: &BuildCallbackKey,
        group: &BuildAttemptGroup,
    ) -> Result<bool> {
        let child = group
            .child(self.attempt.clone())
            .map_err(|_| RuntimeError::OperationCancelled)?;
        child
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        let mut state = tokio::time::timeout_at(
            tokio::time::Instant::from_std(child.work_deadline()),
            self.lease(),
        )
        .await
        .map_err(|_| RuntimeError::OperationCancelled)?;
        self.require_direct_build_donor_binding(&state, callback)?;
        state.progress_sequence = state.progress_sequence.checked_add(1).ok_or_else(|| {
            RuntimeError::InvalidReplication("progress sequence exhausted".into())
        })?;
        let progress_sequence = state.progress_sequence;
        let operation = operation(
            &mut state,
            OperationKey::Progress(progress_sequence),
            OperationValue::Progress,
        )?;
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(child.work_deadline()),
            state.engine.read_progress(operation),
        )
        .await
        .map_err(|_| RuntimeError::OperationCancelled)?
        .map_err(map_mysql)?;
        child
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        self.require_direct_build_donor_binding(&state, callback)?;
        state
            .engine
            .retains_complete_build_history()
            .map_err(map_mysql)
    }

    async fn ensure_direct_build_available(&self, group: &BuildAttemptGroup) -> Result<()> {
        let child = group
            .child(self.attempt.clone())
            .map_err(|_| RuntimeError::OperationCancelled)?;
        child
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        let state = tokio::time::timeout_at(
            tokio::time::Instant::from_std(child.work_deadline()),
            self.lease(),
        )
        .await
        .map_err(|_| RuntimeError::OperationCancelled)?;
        if matches!(
            state.build_generation.status(),
            BuildGenerationStatus::CleanupOnly
                | BuildGenerationStatus::NeedsRebuild
                | BuildGenerationStatus::PendingAdmission
        ) || state.engine.pending_build().is_some()
            || state.engine.has_pending_build_successor()
        {
            return Err(RuntimeError::InvalidReplication(
                "prior Build is pending admission, cleanup, or reset".into(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn cleanup_build_account_after_drop(
        &self,
        receipt: &super::build::BuildRecoveryAccountReceipt,
        build: &crate::service::BuildAttemptContext,
    ) -> Result<()> {
        build
            .admit_cleanup()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        let mut state = tokio::time::timeout_at(
            tokio::time::Instant::from_std(build.deadline()),
            self.lease(),
        )
        .await
        .map_err(|_| RuntimeError::OperationCancelled)?;
        state
            .engine
            .remove_build_recovery_account(receipt, build)
            .await
            .map_err(map_mysql)
    }

    /// Last terminal diagnostic retained by abort or close settlement.
    #[must_use]
    pub fn terminal_diagnostic(&self) -> Option<String> {
        self.terminal_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|failure| failure.diagnostic().to_owned())
    }

    async fn lease(&self) -> DriverStateLease<'_> {
        let callbacks = self.callbacks.lock().await;
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("serialized callback owns the driver state slot");
        DriverStateLease {
            owner: self,
            _callbacks: callbacks,
            state: Some(state),
        }
    }

    async fn bind_public_context(
        self: &Arc<Self>,
        context: ReplicatorFactoryContext,
        settings: ReplicatorSettings,
    ) -> Result<()> {
        let identity = context.identity()?;
        let process_session = context.process_session_id()?;
        if process_session.is_empty() {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }

        if settings.replication_address != self.endpoint.encode() {
            return Err(RuntimeError::AuthorityMismatch(
                "replicator settings changed the enrolled MySQL replication endpoint".into(),
            ));
        }
        let translated = translate_replica_identity(&identity).map_err(map_translation)?;
        let mut state = self.lease().await;
        if state.engine.identity() != &translated || state.open_receipt.identity() != &translated {
            return Err(RuntimeError::AuthorityMismatch(
                "Kuberic identity differs from the enrolled MySQL owner".into(),
            ));
        }
        let requested = PublicBinding {
            identity,
            process_session,
            public_storage: None,
            settings,
        };
        if let Some(bound) = &state.binding {
            if bound != &requested {
                return Err(RuntimeError::AuthorityMismatch(
                    "Kuberic factory identity, process session, or settings changed".into(),
                ));
            }
            let generation = state.build_generation.clone();
            drop(state);
            self.ensure_build_registration(&generation)?;
            return Ok(());
        }
        state
            .engine
            .bind_proxy_access_guard(Arc::new(KubericProxyAccessGuard { context }))
            .map_err(map_mysql)?;
        state.binding = Some(requested);
        let generation = state.build_generation.clone();
        drop(state);
        self.ensure_build_registration(&generation)?;
        Ok(())
    }

    pub(super) async fn bind_direct_public_context(
        self: &Arc<Self>,
        identity: ReplicaIdentity,
        process_session: PublicProcessSessionId,
        storage: kuberic_runtime::protocol::types::ReplicaStorageIncarnationContext,
    ) -> Result<()> {
        if process_session.is_empty() {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        let translated = translate_replica_identity(&identity).map_err(map_translation)?;
        let public_storage =
            super::translate_storage_binding(&storage, &identity).map_err(map_translation)?;
        let mut state = self.lease().await;
        if state.engine.identity() != &translated || state.open_receipt.identity() != &translated {
            return Err(RuntimeError::AuthorityMismatch(
                "direct Build identity differs from the enrolled MySQL owner".into(),
            ));
        }
        let requested = PublicBinding {
            identity,
            process_session,
            public_storage: Some(public_storage),
            settings: ReplicatorSettings {
                replication_address: self.endpoint.encode(),
            },
        };
        if state
            .binding
            .as_ref()
            .is_some_and(|bound| bound != &requested)
        {
            return Err(RuntimeError::AuthorityMismatch(
                "direct Build binding changed".into(),
            ));
        }
        if state.binding.is_none() {
            state
                .engine
                .bind_proxy_access_guard(Arc::new(DirectClosedAccessGuard))
                .map_err(map_mysql)?;
            state.binding = Some(requested);
            state.suppress_public_access = true;
        }
        let generation = state.build_generation.clone();
        drop(state);
        self.ensure_build_registration(&generation)
    }

    fn ensure_build_registration(
        self: &Arc<Self>,
        generation: &Arc<BuildGeneration>,
    ) -> Result<()> {
        let mut registration = self
            .build_registration
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registration.is_none() {
            *registration = Some(
                BuildEndpointRegistration::register(self.endpoint.encode(), self, generation)
                    .map_err(map_mysql)?,
            );
        }
        Ok(())
    }

    /// Prepares or rearms the unpublished client gateway for an application
    /// role callback. This method never publishes secondary access.
    pub async fn prepare_client_proxy(&self) -> Result<SocketAddr> {
        let mut state = self.lease().await;
        require_bound_open(&state)?;
        if state.engine.role() != Some(MysqlReplicaRole::Primary)
            || state.engine.configuration().is_none()
        {
            return Err(RuntimeError::NotPrimary);
        }
        match state.proxy.clone() {
            ProxyState::Prepared(endpoint) | ProxyState::Published { endpoint, .. } => Ok(endpoint),
            ProxyState::Unprepared => {
                let endpoint = state
                    .engine
                    .prepare_staged_client_proxy(self.proxy_listen_address)
                    .await
                    .map_err(map_mysql)?;
                state.proxy = ProxyState::Prepared(endpoint);
                Ok(endpoint)
            }
            ProxyState::Revoked => {
                let endpoint = state
                    .engine
                    .rearm_contained_client_proxy(self.proxy_listen_address)
                    .await
                    .map_err(map_mysql)?;
                state.proxy = ProxyState::Prepared(endpoint);
                Ok(endpoint)
            }
        }
    }

    /// Publishes a prepared proxy only for the exact accepted current primary
    /// epoch and configuration.
    pub async fn publish_current_primary_proxy(&self) -> Result<SocketAddr> {
        let mut state = self.lease().await;
        require_bound_open(&state)?;
        Self::publish_current_primary_proxy_locked(&mut state, self.proxy_listen_address).await
    }

    async fn publish_current_primary_proxy_locked(
        state: &mut DriverState,
        proxy_listen_address: SocketAddr,
    ) -> Result<SocketAddr> {
        if state.suppress_public_access {
            state.proxy = ProxyState::Revoked;
            return Ok(proxy_listen_address);
        }
        if let ProxyState::Published { endpoint, .. } = &state.proxy {
            return Ok(*endpoint);
        }
        let public = state
            .current_configuration
            .clone()
            .ok_or(RuntimeError::ReconfigurationPending)?;
        let epoch = public.configuration.epoch;
        let configuration = public.configuration.configuration_id.clone();
        if public.configuration.primary_id
            != state
                .binding
                .as_ref()
                .expect("bound state was required")
                .identity
                .replica_id
            || state.engine.role() != Some(MysqlReplicaRole::Primary)
        {
            return Err(RuntimeError::NotPrimary);
        }
        let native = state
            .engine
            .configuration()
            .cloned()
            .ok_or(RuntimeError::ReconfigurationPending)?;
        if native.id().as_str() != configuration.as_str() {
            return Err(RuntimeError::AuthorityMismatch(
                "public and native configuration identities differ".into(),
            ));
        }
        let mapped_epoch = state
            .epochs
            .latest()
            .filter(|(retained, _)| *retained == epoch)
            .map(|(_, mapped)| mapped)
            .ok_or(RuntimeError::ReconfigurationPending)?;
        let prepared = match state.proxy.clone() {
            ProxyState::Prepared(endpoint) => endpoint,
            ProxyState::Unprepared => {
                let endpoint = state
                    .engine
                    .prepare_staged_client_proxy(proxy_listen_address)
                    .await
                    .map_err(map_mysql)?;
                state.proxy = ProxyState::Prepared(endpoint);
                endpoint
            }
            ProxyState::Revoked => {
                let endpoint = state
                    .engine
                    .rearm_contained_client_proxy(proxy_listen_address)
                    .await
                    .map_err(map_mysql)?;
                state.proxy = ProxyState::Prepared(endpoint);
                endpoint
            }
            ProxyState::Published { .. } => unreachable!("published proxy returned above"),
        };
        if prepared != proxy_listen_address {
            return Err(RuntimeError::AuthorityMismatch(
                "prepared MySQL proxy differs from its fixed service endpoint".into(),
            ));
        }
        let next_generation = next_generation(state.access_generation)?;
        let intent = MysqlAccessIntent::new(next_generation, true, true);
        let operation = operation(
            state,
            OperationKey::Publish(
                epoch.into(),
                configuration.as_str().to_owned(),
                next_generation.get(),
            ),
            OperationValue::Access(intent),
        )?;
        let authority = MysqlRuntimeAccessAuthority::new(
            mapped_epoch,
            MysqlReplicaRole::Primary,
            native.id().clone(),
        );
        let outcome = state
            .engine
            .publish_access_with_runtime_authority(operation, intent, authority)
            .await
            .map_err(map_mysql)?;
        let endpoint = outcome.endpoint().ok_or_else(|| {
            RuntimeError::InvalidReplication("MySQL proxy publication returned no endpoint".into())
        })?;
        state.access_generation = next_generation;
        state.proxy = ProxyState::Published {
            endpoint,
            epoch,
            configuration,
        };
        Ok(endpoint)
    }

    /// Revokes and contains a published client generation. Unpublished
    /// secondary state remains closed and requires no success-shaped fallback.
    pub async fn revoke_client_proxy(&self) -> Result<()> {
        let mut state = self.lease().await;
        require_bound_open(&state)?;
        Self::revoke_client_proxy_locked(&mut state).await
    }

    async fn revoke_client_proxy_locked(state: &mut DriverState) -> Result<()> {
        if !proxy_requires_physical_containment(&state.proxy) {
            return Ok(());
        }
        let next_generation = next_generation(state.access_generation)?;
        let intent = MysqlAccessIntent::new(next_generation, false, false);
        let operation = operation(
            state,
            OperationKey::Revoke(next_generation.get()),
            OperationValue::Access(intent),
        )?;
        state
            .engine
            .revoke_access(operation, intent)
            .await
            .map_err(map_mysql)?;
        state.access_generation = next_generation;
        state.proxy = ProxyState::Revoked;
        Ok(())
    }

    async fn ensure_epoch(state: &mut DriverState, epoch: Epoch) -> Result<MysqlReplicaEpoch> {
        let mapped = state.epochs.translate(epoch).map_err(map_translation)?;
        if state.engine.epoch() == Some(mapped) {
            return Ok(mapped);
        }
        Self::revoke_client_proxy_locked(state).await?;
        if state.engine.configuration().is_some() {
            Self::capture_current_boundary(state, true).await?;
        } else {
            state.cached_source_boundary = None;
            state.cached_source_progress = None;
            state.pre_epoch_role = None;
        }
        let epoch_operation = operation(
            state,
            OperationKey::AdvanceEpoch(epoch.into()),
            OperationValue::Epoch(mapped),
        )?;
        state
            .engine
            .advance_epoch(epoch_operation, mapped)
            .map_err(map_mysql)?;
        state.catch_up_proven = false;
        Ok(mapped)
    }

    async fn capture_current_boundary(
        state: &mut DriverState,
        allow_transitioned_native_role: bool,
    ) -> Result<()> {
        if state.engine.configuration().is_none() {
            return Err(RuntimeError::ReconfigurationPending);
        }
        state.progress_sequence = state.progress_sequence.checked_add(1).ok_or_else(|| {
            RuntimeError::InvalidReplication("progress sequence exhausted".into())
        })?;
        let progress_operation = operation(
            state,
            OperationKey::Progress(state.progress_sequence),
            OperationValue::Progress,
        )?;
        let progress = if allow_transitioned_native_role {
            state
                .engine
                .observe_transition_progress(progress_operation)
                .await
        } else {
            state.engine.read_progress(progress_operation).await
        }
        .map_err(map_mysql)?;
        let qualified = match progress.progress() {
            ProgressProjection::Projectable(progress) => progress,
            ProgressProjection::NonProjectable(_) => {
                return Err(RuntimeError::InvalidReplication(
                    "current MySQL history is not projectable to a qualified LSN".into(),
                ));
            }
        };
        let pre_epoch_role = state.engine.role().ok_or_else(|| {
            RuntimeError::InvalidReplication(
                "pre-epoch progress observation requires retained role intent".into(),
            )
        })?;
        state.cached_source_boundary = if pre_epoch_role == MysqlReplicaRole::Primary {
            Some(state.engine.source_boundary().map_err(map_mysql)?)
        } else {
            None
        };
        state.cached_source_progress = Some(qualified);
        state.pre_epoch_role = Some(pre_epoch_role);
        Ok(())
    }

    fn translate_configuration(
        state: &mut DriverState,
        endpoint: &ReplicationEndpoint,
        configuration: &ReplicaSetConfiguration,
    ) -> Result<(ReplicaSetConfiguration, MysqlReplicaConfiguration)> {
        let binding = state
            .binding
            .clone()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        let normalized =
            normalize_configuration_progress(&state.retained_replica_progress, configuration)?;
        let local_target = state.engine.local_target().map_err(map_mysql)?;
        let native = translate_replica_set_configuration(
            &normalized,
            &mut state.epochs,
            &binding.identity,
            endpoint,
            &local_target,
        )
        .map_err(map_translation)?;
        Ok((normalized, native))
    }

    fn retain_configuration_progress(
        state: &mut DriverState,
        configuration: &ReplicaSetConfiguration,
    ) -> Result<()> {
        for replica in &configuration.replicas {
            Self::retain_replica_progress(
                state,
                replica,
                replica.current_progress,
                replica.catch_up_capability,
            )?;
        }
        Ok(())
    }

    fn validate_pending_build_admission(
        state: &DriverState,
        configuration: &ReplicaSetConfiguration,
        endpoint: &ReplicationEndpoint,
    ) -> Result<()> {
        let Some(target) = state.pending_build_target.as_ref() else {
            return Ok(());
        };
        let member = configuration
            .configuration
            .members
            .iter()
            .find(|member| member.identity == target.identity)
            .ok_or_else(|| {
                RuntimeError::AuthorityMismatch(
                    "configuration admission omits the exact built target".into(),
                )
            })?;
        if member.role != ReplicaRole::ActiveSecondary {
            return Err(RuntimeError::AuthorityMismatch(
                "built target admission requires ActiveSecondary".into(),
            ));
        }
        let public = state
            .binding
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        if public.identity == target.identity {
            if public.process_session != target.process_session
                || endpoint.encode() != target.replication_address
                || Some(target.native.storage()) != public.public_storage.as_ref()
            {
                return Err(RuntimeError::AuthorityMismatch(
                    "local built target admission changed session, endpoint, or storage".into(),
                ));
            }
            return Ok(());
        }
        let replica = configuration
            .replicas
            .iter()
            .find(|replica| replica.identity == target.identity)
            .ok_or_else(|| {
                RuntimeError::AuthorityMismatch(
                    "configuration admission lacks built target information".into(),
                )
            })?;
        let native = translate_replica_information(replica).map_err(map_translation)?;
        if replica.process_session_id != target.process_session
            || replica.storage_incarnation.as_ref() != Some(&target.storage)
            || replica.replication_address != target.replication_address
            || native != target.native
        {
            return Err(RuntimeError::AuthorityMismatch(
                "configuration admission substituted the built target binding".into(),
            ));
        }
        Ok(())
    }

    fn retain_replica_progress(
        state: &mut DriverState,
        replica: &ReplicaInformation,
        current: i64,
        catch_up: i64,
    ) -> Result<()> {
        let storage = replica.storage_incarnation.clone().ok_or_else(|| {
            RuntimeError::InvalidReplication(
                "retained replica progress requires exact storage".into(),
            )
        })?;
        let retained = RetainedReplicaProgress {
            identity: replica.identity.clone(),
            process_session: replica.process_session_id.clone(),
            storage,
            current,
            catch_up,
        };
        if let Some(existing) = state.retained_replica_progress.iter_mut().find(|known| {
            known.identity == retained.identity
                && known.process_session == retained.process_session
                && known.storage == retained.storage
        }) {
            *existing = retained;
        } else {
            state.retained_replica_progress.push(retained);
        }
        Ok(())
    }

    fn stage_configuration(
        state: &mut DriverState,
        public: ReplicaSetConfiguration,
        native: MysqlReplicaConfiguration,
    ) -> Result<()> {
        if let Some(pending) = &state.pending_configuration {
            if pending.configuration == public.configuration
                && state.pending_native_configuration.as_ref() == Some(&native)
            {
                if pending != &public {
                    return Err(RuntimeError::AuthorityMismatch(
                        "configuration retry changed normalized progress".into(),
                    ));
                }

                state.pending_configuration = Some(public);
                return Ok(());
            }
            if pending != &public || state.pending_native_configuration.as_ref() != Some(&native) {
                return Err(RuntimeError::AuthorityMismatch(
                    "a different configuration is already staged".into(),
                ));
            }
            return Ok(());
        }
        state.pending_configuration = Some(public);
        state.pending_native_configuration = Some(native);
        state.catch_up_proven = false;
        Ok(())
    }

    fn refresh_identical_configuration(
        state: &mut DriverState,
        public: ReplicaSetConfiguration,
        native: &MysqlReplicaConfiguration,
    ) -> Result<bool> {
        let identical = state
            .current_configuration
            .as_ref()
            .is_some_and(|current| current.configuration == public.configuration)
            && state.engine.configuration() == Some(native)
            && state
                .pending_configuration
                .as_ref()
                .is_none_or(|pending| pending.configuration == public.configuration);
        if !identical {
            return Ok(false);
        }
        if state.current_configuration.as_ref() != Some(&public)
            || state
                .pending_configuration
                .as_ref()
                .is_some_and(|pending| pending != &public)
        {
            return Err(RuntimeError::AuthorityMismatch(
                "identical configuration retry changed normalized progress".into(),
            ));
        }
        state.current_configuration = Some(public);
        state.pending_configuration = None;
        state.pending_native_configuration = None;
        state.pending_build_target = None;
        state.cached_source_boundary = None;
        state.cached_source_progress = None;
        state.pre_epoch_role = None;
        Ok(true)
    }

    async fn complete_identical_configuration_refresh(
        state: &mut DriverState,
        proxy_listen_address: SocketAddr,
        public: &ReplicaSetConfiguration,
    ) -> Result<()> {
        if identical_refresh_requires_primary_publication(Self::local_public_role(state, public)?) {
            Self::publish_current_primary_proxy_locked(state, proxy_listen_address).await?;
        }
        Ok(())
    }

    fn local_public_role(
        state: &DriverState,
        configuration: &ReplicaSetConfiguration,
    ) -> Result<ReplicaRole> {
        let local = &state
            .binding
            .as_ref()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?
            .identity;
        configuration
            .configuration
            .members
            .iter()
            .find(|member| &member.identity == local)
            .map(|member| member.role)
            .ok_or_else(|| {
                RuntimeError::InvalidReplication(
                    "configuration omits the bound local replica".into(),
                )
            })
    }

    async fn complete_configuration(
        state: &mut DriverState,
        public: ReplicaSetConfiguration,
        native: MysqlReplicaConfiguration,
    ) -> Result<()> {
        let key = EpochKey::from(public.configuration.epoch);
        let local_role = Self::local_public_role(state, &public)?;
        if state.observed_roles.get(&key) != Some(&local_role) {
            return Err(RuntimeError::ReconfigurationPending);
        }
        let operation = operation(
            state,
            OperationKey::Configuration(native.id().as_str().to_owned()),
            OperationValue::Configuration(native.clone()),
        )?;
        state
            .engine
            .observe_configuration(operation, native)
            .await
            .map_err(map_mysql)?;
        state.current_configuration = Some(public);
        state.pending_build_target = None;
        state.pending_configuration = None;
        state.pending_native_configuration = None;
        state.cached_source_boundary = None;
        state.cached_source_progress = None;
        state.fresh_open_progress = None;
        state.pre_epoch_role = None;
        state.catch_up_proven = false;
        state.completed_roles.insert(key, local_role);
        if state.build_generation.status() == BuildGenerationStatus::PendingAdmission {
            state.build_generation.set(BuildGenerationStatus::Available);
        }
        Ok(())
    }

    fn abort_state(&self, state: &mut DriverState) {
        if state.closed {
            return;
        }
        let operation = match operation(state, OperationKey::Abort, OperationValue::Terminal) {
            Ok(operation) => operation,
            Err(error) => {
                self.record_terminal_failure(TerminalFailure::Engine(error.to_string()));
                return;
            }
        };
        match state.engine.abort_with_proxy_sync(operation) {
            Ok(_) => state.closed = true,
            Err(error) => {
                self.record_terminal_failure(TerminalFailure::Engine(error.to_string()));
            }
        }
    }

    fn record_terminal_failure(&self, failure: TerminalFailure) {
        let mut retained = self
            .terminal_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if retained.is_none() {
            *retained = Some(failure);
        }
    }
}

/// Factory for the exact member-local Public V1 driver.
pub struct MysqlPublicV1ReplicatorFactory {
    replicator: Arc<MysqlPublicV1Replicator>,
}

impl MysqlPublicV1ReplicatorFactory {
    /// Creates a factory that can bind its driver exactly once.
    #[must_use]
    pub const fn new(replicator: Arc<MysqlPublicV1Replicator>) -> Self {
        Self { replicator }
    }

    /// Returns the driver retained by this member-local factory.
    #[must_use]
    pub fn replicator(&self) -> Arc<MysqlPublicV1Replicator> {
        self.replicator.clone()
    }
}

#[async_trait]
impl ReplicatorFactory for MysqlPublicV1ReplicatorFactory {
    async fn create_replicator(
        &self,
        context: ReplicatorFactoryContext,
        state_provider: Option<Arc<dyn StateProvider>>,
        settings: ReplicatorSettings,
    ) -> Result<ReplicatorInterfaces> {
        let provided = state_provider.ok_or_else(|| {
            RuntimeError::InvalidReplication(
                "MySQL Public V1 requires its progress-only StateProvider facade".into(),
            )
        })?;
        let expected: Arc<dyn StateProvider> = self.replicator.clone();
        if !Arc::ptr_eq(&expected, &provided) {
            return Err(RuntimeError::AuthorityMismatch(
                "MySQL Public V1 requires its exact member-local StateProvider facade".into(),
            ));
        }
        self.replicator
            .bind_public_context(context, settings)
            .await?;
        Ok(ReplicatorInterfaces::primary(
            self.replicator.clone(),
            None,
            ReplicatorCapabilities::conservative(),
        ))
    }
}

#[async_trait]
impl Replicator for MysqlPublicV1Replicator {
    async fn open(&self) -> Result<String> {
        let mut state = self.lease().await;
        if state.binding.is_none() {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        if state.closed {
            return Err(RuntimeError::Closed);
        }
        let operation = operation(&mut state, OperationKey::Open, OperationValue::Open)?;
        let receipt = state.open_receipt.clone();
        state.engine.open(operation, receipt).map_err(map_mysql)?;
        let fresh_progress = state.engine.fresh_open_progress().map_err(map_mysql)?;
        let (fresh_open_progress, committed_progress) =
            retained_fresh_open_progress(fresh_progress);
        state.fresh_open_progress = fresh_open_progress;
        state.last_reported_progress = committed_progress;
        state.opened = true;
        Ok(self.endpoint.encode())
    }

    async fn change_role(&self, epoch: Epoch, role: ReplicaRole) -> Result<()> {
        let mut state = self.lease().await;
        require_bound_open(&state)?;
        require_not_cleanup_only(&state)?;
        let key = EpochKey::from(epoch);
        if role == ReplicaRole::IdleSecondary {
            state
                .idle_build_preparation
                .validate_epoch(epoch)
                .map_err(map_mysql)?;
            let fresh = state.current_configuration.is_none()
                && state.pending_configuration.is_none()
                && state.engine.configuration().is_none();
            if !fresh {
                return Err(RuntimeError::AuthorityMismatch(
                    "IdleSecondary preparation requires an unconfigured fresh target".into(),
                ));
            }
            if state.idle_build_preparation.is_idle() {
                return Ok(());
            }
            if state.engine.role().is_some() {
                return Err(RuntimeError::AuthorityMismatch(
                    "IdleSecondary preparation cannot replace a native role".into(),
                ));
            }
            Self::ensure_epoch(&mut state, epoch).await?;
            state
                .idle_build_preparation
                .admit_role(epoch)
                .map_err(map_mysql)?;
            return Ok(());
        }
        let native_role = translate_role(role).map_err(map_translation)?;
        if state.idle_build_preparation.is_idle() {
            if role == ReplicaRole::ActiveSecondary && state.engine.pending_build().is_some() {
                state
                    .idle_build_preparation
                    .validate_epoch(epoch)
                    .map_err(map_mysql)?;
                let mapped = Self::ensure_epoch(&mut state, epoch).await?;
                let role_operation = operation(
                    &mut state,
                    OperationKey::FormationRole(key, role_code(role)),
                    OperationValue::Role(mapped, native_role),
                )?;
                state
                    .engine
                    .accept_built_secondary_role(role_operation, mapped)
                    .map_err(map_mysql)?;
                if state.build_generation.status() != BuildGenerationStatus::PendingAdmission {
                    return Err(RuntimeError::AuthorityMismatch(
                        "built target lacks pending-admission generation state".into(),
                    ));
                }
                state.build_generation.set(BuildGenerationStatus::Available);
                state.idle_build_preparation.complete();
                state.observed_roles.insert(key, role);
                state.completed_roles.insert(key, role);
                return Ok(());
            }
            return Err(RuntimeError::AuthorityMismatch(
                "prepared IdleSecondary requires exact Build completion before another role".into(),
            ));
        }
        let mapped = Self::ensure_epoch(&mut state, epoch).await?;
        state.idle_build_preparation.clear_uncommitted_epoch();
        if let Some(retained) = state.completed_roles.get(&key) {
            if *retained != role {
                return Err(RuntimeError::AuthorityMismatch(
                    "role callback was retried with a different role".into(),
                ));
            }
            if role == ReplicaRole::Primary
                && state
                    .current_configuration
                    .as_ref()
                    .is_some_and(|current| current.configuration.epoch == epoch)
            {
                Self::publish_current_primary_proxy_locked(&mut state, self.proxy_listen_address)
                    .await?;
            }
            return Ok(());
        }
        let fresh = state.current_configuration.is_none()
            && state.pending_configuration.is_none()
            && state.engine.configuration().is_none();
        if fresh {
            if role != ReplicaRole::Primary {
                return Err(RuntimeError::InvalidReplication(
                    "fresh ActiveSecondary admission requires unsupported Build/reseed".into(),
                ));
            }
            let role_operation = operation(
                &mut state,
                OperationKey::FormationRole(key, role_code(role)),
                OperationValue::Role(mapped, native_role),
            )?;
            state
                .engine
                .intend_formation_role(role_operation, mapped, native_role)
                .map_err(map_mysql)?;
            return Ok(());
        }

        let role_operation = operation(
            &mut state,
            OperationKey::Role(key, role_code(role)),
            OperationValue::Role(mapped, native_role),
        )?;
        let staged = state
            .pending_configuration
            .clone()
            .zip(state.pending_native_configuration.clone());
        if let Some((public, native)) = staged {
            if public.configuration.epoch != epoch || native.epoch() != mapped {
                return Err(RuntimeError::AuthorityMismatch(
                    "staged configuration epoch differs from the role callback".into(),
                ));
            }
            if Self::local_public_role(&state, &public)? != role {
                return Err(RuntimeError::AuthorityMismatch(
                    "staged local role differs from the role callback".into(),
                ));
            }
            let external_target_promotion = state.pre_epoch_role
                == Some(MysqlReplicaRole::Secondary)
                && native_role == MysqlReplicaRole::Primary;
            if !state.catch_up_proven && !external_target_promotion {
                return Err(RuntimeError::ReconfigurationPending);
            }
            if state.pre_epoch_role == Some(MysqlReplicaRole::Primary)
                && native_role == MysqlReplicaRole::Secondary
            {
                let boundary = state.cached_source_boundary.clone().ok_or_else(|| {
                    RuntimeError::InvalidReplication(
                        "source transfer lacks a qualified pre-epoch boundary".into(),
                    )
                })?;
                let primary_identity = public
                    .configuration
                    .members
                    .iter()
                    .find(|member| member.identity.replica_id == public.configuration.primary_id)
                    .map(|member| translate_replica_identity(&member.identity))
                    .transpose()
                    .map_err(map_translation)?
                    .ok_or_else(|| {
                        RuntimeError::InvalidReplication(
                            "staged configuration has no primary target".into(),
                        )
                    })?;
                let target = native
                    .members()
                    .iter()
                    .find(|member| member.identity() == &primary_identity)
                    .ok_or_else(|| {
                        RuntimeError::InvalidReplication(
                            "native staged configuration has no primary target".into(),
                        )
                    })?
                    .member()
                    .clone();
                state
                    .engine
                    .transfer_role(role_operation, mapped, native_role, boundary, target)
                    .await
                    .map_err(map_mysql)?;
            } else if external_target_promotion {
                if state
                    .engine
                    .observe_role_if_ready(role_operation, mapped, native_role)
                    .await
                    .map_err(map_mysql)?
                    .is_none()
                {
                    return Err(RuntimeError::ReconfigurationPending);
                }
            } else {
                state
                    .engine
                    .observe_role(role_operation, mapped, native_role)
                    .await
                    .map_err(map_mysql)?;
            }
            state.observed_roles.insert(key, role);
            Self::complete_configuration(&mut state, public, native).await?;
            if role == ReplicaRole::Primary {
                Self::publish_current_primary_proxy_locked(&mut state, self.proxy_listen_address)
                    .await?;
            }
        } else {
            if state.pre_epoch_role == Some(MysqlReplicaRole::Secondary)
                && native_role == MysqlReplicaRole::Primary
            {
                if state
                    .engine
                    .observe_role_if_ready(role_operation, mapped, native_role)
                    .await
                    .map_err(map_mysql)?
                    .is_none()
                {
                    // The pinned controller pre-stages the target role before
                    // dispatching the source's native transfer. Access remains
                    // fenced, configuration stays pending, and no native role
                    // credit is retained until the post-transfer retry.
                    return Ok(());
                }
            } else {
                state
                    .engine
                    .observe_role(role_operation, mapped, native_role)
                    .await
                    .map_err(map_mysql)?;
            }
            state.observed_roles.insert(key, role);
        }
        Ok(())
    }

    async fn update_epoch(&self, epoch: Epoch) -> Result<()> {
        let mut state = self.lease().await;
        require_bound_open(&state)?;
        require_not_cleanup_only(&state)?;
        let fresh = state.current_configuration.is_none()
            && state.pending_configuration.is_none()
            && state.engine.configuration().is_none()
            && state.engine.role().is_none();
        if fresh {
            state
                .idle_build_preparation
                .observe_epoch(epoch)
                .map_err(map_mysql)?;
        } else {
            state
                .idle_build_preparation
                .validate_epoch(epoch)
                .map_err(map_mysql)?;
        }
        Self::ensure_epoch(&mut state, epoch).await?;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        let mut state = self.lease().await;
        if state.closed {
            return self
                .terminal_failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .map_or(Ok(()), |failure| Err(failure.runtime_error()));
        }
        let operation = operation(&mut state, OperationKey::Close, OperationValue::Terminal)?;
        let result = if self.abort_requested.load(Ordering::Acquire) {
            state.engine.abort_with_proxy(operation).await
        } else {
            state.engine.close_with_proxy(operation).await
        };
        match result {
            Ok(_) => {
                state.closed = true;
                self.terminal_failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                    .map_or(Ok(()), |failure| Err(failure.runtime_error()))
            }
            Err(error) => {
                self.record_terminal_failure(TerminalFailure::Engine(error.to_string()));
                Err(RuntimeError::Application(error.to_string()))
            }
        }
    }

    fn abort(&self) {
        self.abort_requested.store(true, Ordering::Release);
        self.attempt.cancel();
        let mut slot = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(mut state) = slot.take() {
            self.abort_state(&mut state);
            *slot = Some(state);
        }
    }

    async fn current_progress(&self) -> Result<i64> {
        let mut state = self.lease().await;
        require_bound_open(&state)?;
        if matches!(
            state.build_generation.status(),
            BuildGenerationStatus::CleanupOnly | BuildGenerationStatus::NeedsRebuild
        ) {
            return Err(RuntimeError::ReconfigurationPending);
        }
        if state.engine.configuration().is_none() {
            if state.engine.pending_build().is_some() {
                let progress = state
                    .engine
                    .refresh_pending_build_progress()
                    .await
                    .map_err(map_mysql)?;
                return match progress.progress() {
                    ProgressProjection::Projectable(progress) => {
                        retain_reported_progress(
                            &mut state.last_reported_progress,
                            progress.value(),
                        );
                        state.cached_source_progress = Some(progress);
                        Ok(progress.value())
                    }
                    ProgressProjection::NonProjectable(_) => Err(RuntimeError::InvalidReplication(
                        "built MySQL history cannot be projected".into(),
                    )),
                };
            }
            let progress = progress_without_current_configuration(
                state.cached_source_progress.map(QualifiedProgress::value),
                state.fresh_open_progress.map(QualifiedProgress::value),
                state.engine.access_intent(),
            );
            if let Ok(progress) = progress {
                retain_reported_progress(&mut state.last_reported_progress, progress);
            }
            return progress;
        }
        state.progress_sequence = state.progress_sequence.checked_add(1).ok_or_else(|| {
            RuntimeError::InvalidReplication("progress sequence exhausted".into())
        })?;
        let progress_sequence = state.progress_sequence;
        let operation = operation(
            &mut state,
            OperationKey::Progress(progress_sequence),
            OperationValue::Progress,
        )?;
        let progress = state
            .engine
            .read_progress(operation)
            .await
            .map_err(map_mysql)?;
        match progress.progress() {
            ProgressProjection::Projectable(progress) => {
                retain_reported_progress(&mut state.last_reported_progress, progress.value());
                Ok(progress.value())
            }
            ProgressProjection::NonProjectable(_) => Err(RuntimeError::InvalidReplication(
                "MySQL history cannot be projected to a qualified public LSN".into(),
            )),
        }
    }

    async fn catch_up_capability(&self) -> Result<i64> {
        let state = self.lease().await;
        require_bound_open(&state)?;
        require_not_cleanup_only(&state)?;
        Ok(INVALID_LSN)
    }
}

#[async_trait]
impl StateProvider for MysqlPublicV1Replicator {
    async fn update_epoch(&self, epoch: Epoch, previous_epoch_last_lsn: i64) -> Result<()> {
        let state = self.lease().await;
        require_bound_open(&state)?;
        require_not_cleanup_only(&state)?;
        let exact_epoch = state.epochs.latest().is_some_and(|(public, mapped)| {
            public == epoch && state.engine.epoch() == Some(mapped)
        });
        if !exact_epoch || state.last_reported_progress != Some(previous_epoch_last_lsn) {
            return Err(RuntimeError::AuthorityMismatch(
                "StateProvider epoch validation requires the exact accepted epoch and reported committed tail"
                    .into(),
            ));
        }
        Ok(())
    }

    async fn last_committed_lsn(&self) -> Result<i64> {
        let state = self.lease().await;
        require_bound_open(&state)?;
        require_not_cleanup_only(&state)?;
        retained_committed_progress(state.last_reported_progress)
    }

    async fn get_copy_context(&self) -> Result<OperationDataStream> {
        Err(RuntimeError::InvalidReplication(
            "MySQL Public V1 exposes no application copy bytes".into(),
        ))
    }

    async fn get_copy_state(
        &self,
        _up_to_lsn: i64,
        _copy_context: OperationDataStream,
    ) -> Result<OperationDataStream> {
        Err(RuntimeError::InvalidReplication(
            "MySQL Public V1 exposes no application copy state".into(),
        ))
    }

    async fn on_data_loss(&self) -> Result<bool> {
        Err(RuntimeError::InvalidReplication(
            "MySQL Public V1 StateProvider rejects data-loss acceptance".into(),
        ))
    }
}

#[async_trait]
impl PrimaryReplicator for MysqlPublicV1Replicator {
    async fn on_data_loss(&self) -> Result<bool> {
        Err(RuntimeError::InvalidReplication(
            "MySQL Public V1 data-loss acceptance is unsupported".into(),
        ))
    }

    async fn update_catch_up_replica_set_configuration(
        &self,
        current: ReplicaSetConfiguration,
        previous: ReplicaSetConfiguration,
    ) -> Result<()> {
        let mut state = self.lease().await;
        require_bound_open(&state)?;
        require_not_cleanup_only(&state)?;
        if state.idle_build_preparation.is_idle() {
            return Err(RuntimeError::ReconfigurationPending);
        }
        if state
            .current_configuration
            .as_ref()
            .is_none_or(|accepted| accepted.configuration != previous.configuration)
        {
            return Err(RuntimeError::AuthorityMismatch(
                "catch-up previous descriptor differs from the accepted current configuration"
                    .into(),
            ));
        }
        let (current, native) =
            Self::translate_configuration(&mut state, &self.endpoint, &current)?;
        Self::validate_pending_build_admission(&state, &current, &self.endpoint)?;
        if Self::refresh_identical_configuration(&mut state, current.clone(), &native)? {
            Self::complete_identical_configuration_refresh(
                &mut state,
                self.proxy_listen_address,
                &current,
            )
            .await?;
            Self::retain_configuration_progress(&mut state, &current)?;
            return Ok(());
        }
        Self::stage_configuration(&mut state, current.clone(), native)?;
        Self::retain_configuration_progress(&mut state, &current)
    }

    async fn wait_for_catch_up_quorum(&self, mode: ReplicaSetQuorumMode) -> Result<()> {
        let mut state = self.lease().await;
        require_bound_open(&state)?;
        require_not_cleanup_only(&state)?;
        if catch_up_requires_fresh_boundary(
            state.engine.configuration().is_some(),
            state.cached_source_progress.is_some(),
        )? {
            Self::capture_current_boundary(&mut state, false).await?;
        }
        let boundary = state
            .cached_source_progress
            .ok_or_else(|| {
                RuntimeError::InvalidReplication(
                    "catch-up lacks a retained qualified pre-epoch source boundary".into(),
                )
            })?
            .value();
        let configuration = state
            .pending_configuration
            .as_ref()
            .or(state.current_configuration.as_ref())
            .ok_or(RuntimeError::ReconfigurationPending)?;
        let local = &state
            .binding
            .as_ref()
            .expect("bound state was required")
            .identity;
        if !configuration_satisfies_catch_up(configuration, local, boundary, mode)? {
            return Err(RuntimeError::ReconfigurationPending);
        }
        state.catch_up_proven = true;
        if let (Some(current), Some(pending)) = (
            state.current_configuration.as_ref(),
            state.pending_configuration.as_ref(),
        ) && current.configuration == pending.configuration
        {
            state.current_configuration = state.pending_configuration.take();
            state.pending_native_configuration = None;
        }
        Ok(())
    }

    async fn update_current_replica_set_configuration(
        &self,
        current: ReplicaSetConfiguration,
    ) -> Result<()> {
        let mut state = self.lease().await;
        require_bound_open(&state)?;
        require_not_cleanup_only(&state)?;
        if state.idle_build_preparation.is_idle() {
            return Err(RuntimeError::ReconfigurationPending);
        }
        let (current, native) =
            Self::translate_configuration(&mut state, &self.endpoint, &current)?;
        Self::validate_pending_build_admission(&state, &current, &self.endpoint)?;
        if Self::refresh_identical_configuration(&mut state, current.clone(), &native)? {
            Self::complete_identical_configuration_refresh(
                &mut state,
                self.proxy_listen_address,
                &current,
            )
            .await?;
            Self::retain_configuration_progress(&mut state, &current)?;
            return Ok(());
        }
        let fresh = state.current_configuration.is_none()
            && state.pending_configuration.is_none()
            && state.engine.configuration().is_none();
        if fresh {
            let key = EpochKey::from(current.configuration.epoch);
            if state.engine.pending_build().is_some()
                && Self::local_public_role(&state, &current)? == ReplicaRole::ActiveSecondary
            {
                let _mapped = state
                    .epochs
                    .latest()
                    .filter(|(epoch, _)| *epoch == current.configuration.epoch)
                    .map(|(_, mapped)| mapped)
                    .ok_or(RuntimeError::ReconfigurationPending)?;
                let operation = operation(
                    &mut state,
                    OperationKey::Configuration(native.id().as_str().to_owned()),
                    OperationValue::Configuration(native.clone()),
                )?;
                state
                    .engine
                    .accept_built_configuration(operation, native)
                    .map_err(map_mysql)?;
                state
                    .observed_roles
                    .insert(key, ReplicaRole::ActiveSecondary);
                state
                    .completed_roles
                    .insert(key, ReplicaRole::ActiveSecondary);
                state.current_configuration = Some(current);
                state.pending_build_target = None;
                let accepted = state
                    .current_configuration
                    .clone()
                    .expect("just accepted Build configuration");
                Self::retain_configuration_progress(&mut state, &accepted)?;
                state.fresh_open_progress = None;
                state.pre_epoch_role = None;
                return Ok(());
            }
            let local = state
                .binding
                .as_ref()
                .expect("bound state was required")
                .identity
                .clone();
            if current.configuration.members.len() != 1
                || current.configuration.primary_id != local.replica_id
                || current.configuration.members[0].identity != local
                || current.configuration.members[0].role != ReplicaRole::Primary
            {
                return Err(RuntimeError::InvalidReplication(
                    "fresh MySQL bootstrap requires exactly the local Primary member".into(),
                ));
            }
            let mapped = state
                .epochs
                .latest()
                .filter(|(epoch, _)| *epoch == current.configuration.epoch)
                .map(|(_, mapped)| mapped)
                .ok_or(RuntimeError::ReconfigurationPending)?;
            if state.engine.role() != Some(MysqlReplicaRole::Primary) || native.epoch() != mapped {
                return Err(RuntimeError::ReconfigurationPending);
            }
            let operation = operation(
                &mut state,
                OperationKey::Configuration(native.id().as_str().to_owned()),
                OperationValue::Configuration(native.clone()),
            )?;
            state
                .engine
                .bootstrap_configuration(operation, native)
                .await
                .map_err(map_mysql)?;
            state.observed_roles.insert(key, ReplicaRole::Primary);
            state.completed_roles.insert(key, ReplicaRole::Primary);
            state.current_configuration = Some(current);
            state.cached_source_boundary = None;
            state.cached_source_progress = None;
            state.fresh_open_progress = None;
            state.pre_epoch_role = None;
            Self::publish_current_primary_proxy_locked(&mut state, self.proxy_listen_address)
                .await?;
            return Ok(());
        }
        let key = EpochKey::from(current.configuration.epoch);
        let local_role = Self::local_public_role(&state, &current)?;
        if configuration_can_complete(
            &state.observed_roles,
            key,
            local_role,
            state.engine.epoch(),
            native.epoch(),
        ) {
            Self::complete_configuration(&mut state, current, native).await?;
            let accepted = state
                .current_configuration
                .clone()
                .expect("completed configuration is retained");
            Self::retain_configuration_progress(&mut state, &accepted)?;
            if local_role == ReplicaRole::Primary {
                Self::publish_current_primary_proxy_locked(&mut state, self.proxy_listen_address)
                    .await?;
            }
            Ok(())
        } else {
            Self::stage_configuration(&mut state, current.clone(), native)?;
            Self::retain_configuration_progress(&mut state, &current)
        }
    }

    async fn build_replica(&self, replica: ReplicaInformation) -> Result<()> {
        let (build_id, target_native) =
            translate_build_replica_information(&replica).map_err(map_translation)?;
        if replica.replication_address == self.endpoint.encode() {
            return Err(RuntimeError::AuthorityMismatch(
                "Build target cannot be the local source".into(),
            ));
        }
        let group_permit =
            BuildGroupPermit::try_acquire(self.group_key.clone()).ok_or_else(|| {
                RuntimeError::InvalidReplication(
                    "Build group is busy or blocked pending cleanup/reset".into(),
                )
            })?;
        let source_endpoint = lookup_build_endpoint(&self.endpoint.encode()).ok_or_else(|| {
            RuntimeError::InvalidReplication(
                "Build source endpoint is not registered in this process".into(),
            )
        })?;
        let target_endpoint =
            lookup_build_endpoint(&replica.replication_address).ok_or_else(|| {
                RuntimeError::InvalidReplication(
                    "Build target endpoint is not registered in this process".into(),
                )
            })?;

        let target_storage = replica
            .storage_incarnation
            .clone()
            .ok_or_else(|| RuntimeError::AuthorityMismatch("Build target lacks storage".into()))?;
        let target_binding = BuildTargetBinding {
            identity: replica.identity.clone(),
            process_session: replica.process_session_id.clone(),
            storage: target_storage,
            replication_address: replica.replication_address.clone(),
            native: target_native.clone(),
        };
        let source_parent = self.attempt.clone();
        let target_parent = target_endpoint.owner.build_attempt_parent();
        let child_id = AttemptId::new(build_id.as_str()).map_err(|error| {
            RuntimeError::InvalidReplication(format!("Build id is not an attempt id: {error}"))
        })?;
        let desired_deadline = Instant::now()
            .checked_add(Duration::from_secs(120))
            .ok_or_else(|| RuntimeError::InvalidReplication("Build deadline overflow".into()))?;
        let deadline = desired_deadline
            .min(source_parent.deadline())
            .min(target_parent.deadline());
        let group = BuildAttemptGroup::new(child_id.clone(), deadline, Duration::from_secs(15))
            .map_err(|_| RuntimeError::OperationCancelled)?;
        let source_child = group
            .child(source_parent.clone())
            .map_err(|_| RuntimeError::OperationCancelled)?;
        let target_child = group
            .child(target_parent.clone())
            .map_err(|_| RuntimeError::OperationCancelled)?;
        let mut orchestration = BuildOrchestration::new(group.clone());

        let (
            callback_key,
            operation,
            public_configuration,
            native_configuration,
            source_binding,
            source_generation,
        ) = {
            source_child
                .admit_work()
                .map_err(|_| RuntimeError::OperationCancelled)?;
            let mut state = orchestration.run_work(self.lease()).await?;
            require_bound_open(&state)?;
            let public_configuration = state
                .current_configuration
                .clone()
                .ok_or(RuntimeError::ReconfigurationPending)?;
            if Self::local_public_role(&state, &public_configuration)? != ReplicaRole::Primary {
                return Err(RuntimeError::NotPrimary);
            }
            if public_configuration
                .configuration
                .members
                .iter()
                .any(|member| member.identity == replica.identity)
            {
                return Err(RuntimeError::AuthorityMismatch(
                    "Build target already belongs to the Kuberic configuration".into(),
                ));
            }
            let public = state
                .binding
                .clone()
                .ok_or(RuntimeError::AuthorityNotAdmitted)?;
            let native_configuration = state
                .engine
                .configuration()
                .cloned()
                .ok_or(RuntimeError::ReconfigurationPending)?;
            let source_native = native_configuration
                .members()
                .iter()
                .find(|member| member.identity() == state.engine.identity())
                .cloned()
                .ok_or_else(|| {
                    RuntimeError::AuthorityMismatch(
                        "current native configuration lacks the local source".into(),
                    )
                })?;
            if let Some((retained, terminal)) = state.build_results.retained(build_id.as_str()) {
                if retained.build_id != build_id
                    || retained.epoch != public_configuration.configuration.epoch
                    || retained.configuration != public_configuration.configuration.configuration_id
                    || retained.group_key != self.group_key
                    || retained.source_identity != public.identity
                    || retained.source_process_session != public.process_session
                    || retained.source_configuration != public_configuration
                    || retained.target != target_binding
                {
                    return Err(map_mysql(MysqlReplicatorError::OperationConflict));
                }
                match terminal {
                    Some(BuildTerminalResult::Succeeded) => {
                        self.attempt
                            .admit_work()
                            .map_err(|_| RuntimeError::OperationCancelled)?;
                        if state.engine.current_member_authority().map_err(map_mysql)?
                            != retained.source_authority
                            || state.last_reported_progress != Some(retained.source_progress)
                        {
                            return Err(RuntimeError::AuthorityMismatch(
                                "completed Build retry lost its source authority or invocation progress"
                                    .into(),
                            ));
                        }
                        tokio::time::timeout_at(
                            tokio::time::Instant::from_std(source_child.work_deadline()),
                            Self::capture_current_boundary(&mut state, false),
                        )
                        .await
                        .map_err(|_| RuntimeError::OperationCancelled)??;
                        let latest = state.cached_source_boundary.as_ref().ok_or_else(|| {
                            RuntimeError::InvalidReplication(
                                "completed Build retry lacks source evidence".into(),
                            )
                        })?;
                        if !retained
                            .source_boundary
                            .evidence()
                            .executed()
                            .is_subset_of(latest.evidence().executed())
                        {
                            return Err(RuntimeError::AuthorityMismatch(
                                "completed Build retry lost its frozen source boundary".into(),
                            ));
                        }
                        self.attempt
                            .admit_work()
                            .map_err(|_| RuntimeError::OperationCancelled)?;
                        return Ok(());
                    }
                    Some(BuildTerminalResult::Failed(detail)) => {
                        return Err(RuntimeError::InvalidReplication(detail));
                    }
                    Some(BuildTerminalResult::RebuildRequired(detail)) => {
                        return Err(RuntimeError::ReplicaRebuildRequired(Box::new(detail)));
                    }
                    None => {}
                }
            }
            if state.build_generation.status() != BuildGenerationStatus::Available
                || state.engine.has_pending_build_successor()
            {
                return Err(RuntimeError::InvalidReplication(
                    "source has a prior Build pending admission or cleanup".into(),
                ));
            }
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(source_child.work_deadline()),
                Self::capture_current_boundary(&mut state, false),
            )
            .await
            .map_err(|_| RuntimeError::OperationCancelled)??;
            let source_boundary = state.cached_source_boundary.clone().ok_or_else(|| {
                RuntimeError::InvalidReplication(
                    "Build source lacks a fresh structured boundary".into(),
                )
            })?;
            let source_progress = state.cached_source_progress.ok_or_else(|| {
                RuntimeError::InvalidReplication(
                    "Build source boundary lacks a qualified projection".into(),
                )
            })?;
            if state.last_reported_progress != Some(source_progress.value()) {
                return Err(RuntimeError::InvalidReplication(
                    "Build source boundary differs from last reported Primary progress".into(),
                ));
            }
            let source_authority = state.engine.current_member_authority().map_err(map_mysql)?;
            let callback_key = BuildCallbackKey {
                build_id: build_id.clone(),
                epoch: public_configuration.configuration.epoch,
                configuration: public_configuration.configuration.configuration_id.clone(),
                group_key: self.group_key.clone(),
                source_identity: public.identity.clone(),
                source_process_session: public.process_session.clone(),
                source_configuration: public_configuration.clone(),
                source_authority: source_authority.clone(),
                source_boundary,
                source_progress: source_progress.value(),
                target: target_binding.clone(),
            };
            if state
                .build_results
                .admit(build_id.as_str(), &callback_key)
                .map_err(map_mysql)?
                .is_some()
            {
                return Err(RuntimeError::InvalidReplication(
                    "new Build unexpectedly matched a terminal result".into(),
                ));
            }
            let operation = operation(
                &mut state,
                OperationKey::Build(build_id.as_str().to_owned()),
                OperationValue::Build(target_native.clone()),
            )?;
            (
                callback_key,
                operation,
                public_configuration,
                native_configuration,
                BuildSourceBinding {
                    identity: public.identity,
                    process_session: public.process_session,
                    replication_address: self.endpoint.encode(),
                    native: source_native,
                },
                state.build_generation.clone(),
            )
        };

        for peer in &public_configuration.replicas {
            let donor = lookup_build_endpoint(&peer.replication_address).ok_or_else(|| {
                RuntimeError::InvalidReplication(
                    "configured Build donor is not registered in this process".into(),
                )
            })?;
            donor.owner.ensure_direct_build_available(&group).await?;
        }

        let cleanup_donors = public_configuration
            .replicas
            .iter()
            .map(|peer| {
                lookup_build_endpoint(&peer.replication_address)
                    .map(|endpoint| (Arc::downgrade(&endpoint.owner), endpoint.generation.clone()))
                    .ok_or_else(|| {
                        RuntimeError::InvalidReplication(
                            "configured Build donor is not registered in this process".into(),
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut source_cleanup = BuildSourceCleanup::new(
            Arc::downgrade(&source_endpoint.owner),
            source_child.clone(),
            public_configuration.configuration.members.len(),
            source_generation.clone(),
            callback_key.clone(),
            cleanup_donors,
            group_permit,
        );
        let mut target_owner = target_endpoint
            .owner
            .acquire_build_target(
                &self.group_key,
                public_configuration.configuration.epoch,
                &target_binding,
                target_endpoint.generation.id(),
                &target_child,
            )
            .await?;
        orchestration
            .advance(BuildCheckpoint::TargetFresh)
            .map_err(map_mysql)?;
        let credential = BuildRecoveryCredential::random(child_id.clone()).map_err(map_mysql)?;
        // Own the exact account identity before CREATE/GRANT begins. If the
        // future is interrupted after either statement commits, Drop can
        // still remove only this request-owned account.
        source_cleanup.set_account(BuildRecoveryAccountReceipt::new(&credential));
        orchestration
            .advance(BuildCheckpoint::AccountCleanupOwned)
            .map_err(map_mysql)?;

        let execution: Result<()> = async {
            // Re-prove freshness after reservation and immediately before the
            // first source-side effect; reservation alone is not evidence that
            // native state remained empty while callback work was scheduled.
            target_owner.verify_fresh(&target_child).await?;
            let (
                account_receipt,
                source_boundary,
                source_authority,
                build_native_configuration,
                peers,
                binding,
                source_retains_history,
            ) = {
                let mut state = orchestration.run_work(self.lease()).await?;
                require_bound_open(&state)?;
                if state.current_configuration.as_ref() != Some(&public_configuration)
                    || Self::local_public_role(&state, &public_configuration)?
                        != ReplicaRole::Primary
                {
                    return Err(RuntimeError::AuthorityMismatch(
                        "Build source configuration or role changed".into(),
                    ));
                }
                tokio::time::timeout_at(
                    tokio::time::Instant::from_std(source_child.work_deadline()),
                    Self::capture_current_boundary(&mut state, false),
                )
                .await
                .map_err(|_| RuntimeError::OperationCancelled)??;
                let latest = state.cached_source_boundary.as_ref().ok_or_else(|| {
                    RuntimeError::InvalidReplication(
                        "Build source lacks a fresh structured boundary".into(),
                    )
                })?;
                if state.last_reported_progress != Some(callback_key.source_progress)
                    || state.engine.current_member_authority().map_err(map_mysql)?
                        != callback_key.source_authority
                    || !callback_key
                        .source_boundary
                        .evidence()
                        .executed()
                        .is_subset_of(latest.evidence().executed())
                {
                    return Err(RuntimeError::AuthorityMismatch(
                        "Build source changed from the retained invocation boundary".into(),
                    ));
                }
                let source_boundary = callback_key.source_boundary.clone();
                let source_retains_history =
                    source_boundary.evidence().purged().entries().is_empty();
                let source_authority =
                    state.engine.current_member_authority().map_err(map_mysql)?;
                let account_receipt = state
                    .engine
                    .prepare_build_recovery_account(&credential, &source_child)
                    .await
                    .map_err(map_mysql)?;
                if source_cleanup.account() != Some(&account_receipt) {
                    return Err(RuntimeError::AuthorityMismatch(
                        "created Build account differs from pre-armed cleanup ownership".into(),
                    ));
                }
                state
                    .engine
                    .verify_build_recovery_account(&account_receipt, true, &source_child)
                    .await
                    .map_err(map_mysql)?;
                orchestration
                    .advance(BuildCheckpoint::AccountPresent)
                    .map_err(map_mysql)?;
                let mut members = native_configuration.members().to_vec();
                members.push(target_native.clone());
                let build_native_configuration = MysqlReplicaConfiguration::new(
                    native_configuration.id().clone(),
                    native_configuration.epoch(),
                    members,
                );
                let peers = native_configuration
                    .members()
                    .iter()
                    .map(|member| member.member().clone())
                    .collect::<Vec<_>>();
                let mut expected_members = public_configuration
                    .configuration
                    .members
                    .iter()
                    .map(|member| member.identity.clone())
                    .collect::<Vec<_>>();
                expected_members.push(replica.identity.clone());
                let binding = BuildBinding {
                    build_id: build_id.clone(),
                    child_attempt: child_id.clone(),
                    source_parent_attempt: source_parent.attempt().clone(),
                    target_parent_attempt: target_parent.attempt().clone(),
                    source_generation: source_generation.id(),
                    target_generation: target_endpoint.generation.id(),
                    epoch: public_configuration.configuration.epoch,
                    configuration: public_configuration.configuration.configuration_id.clone(),
                    group_key: self.group_key.clone(),
                    source: source_binding.clone(),
                    target: target_binding.clone(),
                    source_authority,
                    expected_members,
                    source_boundary: source_boundary.clone(),
                    grant_id: credential.generation().as_str().to_owned(),
                };
                (
                    account_receipt,
                    source_boundary,
                    binding.source_authority.clone(),
                    build_native_configuration,
                    peers,
                    binding,
                    source_retains_history,
                )
            };

            let mut retained_history_available = source_retains_history;
            for peer in &public_configuration.replicas {
                let donor = lookup_build_endpoint(&peer.replication_address).ok_or_else(|| {
                    RuntimeError::InvalidReplication(
                        "configured Build donor is not registered in this process".into(),
                    )
                })?;
                donor
                    .owner
                    .verify_direct_build_account(&account_receipt, true, &group, &callback_key)
                    .await?;
                retained_history_available |= donor
                    .owner
                    .retains_complete_build_history(&callback_key, &group)
                    .await?;
            }
            if !retained_history_available {
                return Err(RuntimeError::ReplicaRebuildRequired(Box::new(
                    ReplicaRebuildRequired::RemoteBuildTarget(Box::new(
                        RemoteBuildTargetRebuildRequired {
                            reason: ReplicaRebuildReason::IncompatibleRetainedHistory,
                            epoch: public_configuration.configuration.epoch,
                            target: replica.identity.clone(),
                            target_session: replica.process_session_id.clone(),
                        },
                    )),
                )));
            }
            orchestration
                .advance(BuildCheckpoint::DonorsPrepared)
                .map_err(map_mysql)?;
            target_owner
                .verify_recovery_account(&account_receipt, false, &target_child)
                .await?;
            {
                source_child
                    .admit_work()
                    .map_err(|_| RuntimeError::OperationCancelled)?;
                let mut state = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(source_child.work_deadline()),
                    self.lease(),
                )
                .await
                .map_err(|_| RuntimeError::OperationCancelled)?;
                if state.engine.current_member_authority().map_err(map_mysql)?
                    != binding.source_authority
                    || state.last_reported_progress != Some(callback_key.source_progress)
                    || state.current_configuration.as_ref() != Some(&public_configuration)
                    || state.engine.configuration() != Some(&native_configuration)
                    || Self::local_public_role(&state, &public_configuration)?
                        != ReplicaRole::Primary
                {
                    return Err(RuntimeError::AuthorityMismatch(
                        "Build source authority changed during donor preparation".into(),
                    ));
                }
                tokio::time::timeout_at(
                    tokio::time::Instant::from_std(source_child.work_deadline()),
                    Self::capture_current_boundary(&mut state, false),
                )
                .await
                .map_err(|_| RuntimeError::OperationCancelled)??;
                let latest = state.cached_source_boundary.as_ref().ok_or_else(|| {
                    RuntimeError::InvalidReplication(
                        "Build source lost structured evidence before join".into(),
                    )
                })?;
                if !binding
                    .source_boundary
                    .evidence()
                    .executed()
                    .is_subset_of(latest.evidence().executed())
                {
                    return Err(RuntimeError::InvalidReplication(
                        "Build source no longer contains the frozen boundary".into(),
                    ));
                }
                source_child
                    .admit_work()
                    .map_err(|_| RuntimeError::OperationCancelled)?;
            }
            orchestration
                .advance(BuildCheckpoint::SourceRevalidated)
                .map_err(map_mysql)?;

            // Arm interruption disposition before the native effect future is
            // entered; dropping anywhere inside recovery must quarantine the
            // source and every already configured donor synchronously.
            source_cleanup.mark_native_effect();
            let _receipt = orchestration
                .run_work(target_owner.join(
                    &binding,
                    operation.clone(),
                    build_native_configuration.clone(),
                    source_boundary,
                    peers,
                    &credential,
                    &target_child,
                ))
                .await??;
            orchestration
                .advance(BuildCheckpoint::TargetJoined)
                .map_err(map_mysql)?;

            for peer in &public_configuration.replicas {
                let donor = lookup_build_endpoint(&peer.replication_address).ok_or_else(|| {
                    RuntimeError::InvalidReplication(
                        "configured Build donor is not registered in this process".into(),
                    )
                })?;
                donor
                    .owner
                    .observe_direct_build_successor(
                        &build_id,
                        &build_native_configuration,
                        &callback_key,
                        &group,
                    )
                    .await?;
            }

            source_child
                .admit_work()
                .map_err(|_| RuntimeError::OperationCancelled)?;
            let mut state = tokio::time::timeout_at(
                tokio::time::Instant::from_std(source_child.work_deadline()),
                self.lease(),
            )
            .await
            .map_err(|_| RuntimeError::OperationCancelled)?;
            require_bound_open(&state)?;
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(source_child.work_deadline()),
                state
                    .engine
                    .observe_build_successor(operation, build_native_configuration),
            )
            .await
            .map_err(|_| RuntimeError::OperationCancelled)?
            .map_err(map_mysql)?;
            source_child
                .admit_work()
                .map_err(|_| RuntimeError::OperationCancelled)?;
            state
                .build_generation
                .set(BuildGenerationStatus::PendingAdmission);
            state.pending_build_target = Some(callback_key.target.clone());
            if state.current_configuration.as_ref() != Some(&public_configuration)
                || state.engine.current_member_authority().map_err(map_mysql)? != source_authority
                || Self::local_public_role(&state, &public_configuration)? != ReplicaRole::Primary
            {
                return Err(RuntimeError::AuthorityMismatch(
                    "Build source changed before completion".into(),
                ));
            }
            orchestration
                .advance(BuildCheckpoint::SuccessorsObserved)
                .map_err(map_mysql)?;
            drop(state);
            for peer in &public_configuration.replicas {
                let donor = lookup_build_endpoint(&peer.replication_address).ok_or_else(|| {
                    RuntimeError::InvalidReplication(
                        "configured Build donor disappeared before cleanup".into(),
                    )
                })?;
                donor
                    .owner
                    .enter_direct_build_cleanup(&callback_key, &group)
                    .await?;
            }
            let mut state = orchestration.run_work(self.lease()).await?;
            state
                .build_generation
                .set(BuildGenerationStatus::CleanupOnly);
            orchestration
                .advance(BuildCheckpoint::CleanupOnly)
                .map_err(map_mysql)?;
            state
                .engine
                .remove_build_recovery_account(&account_receipt, &source_child)
                .await
                .map_err(map_mysql)?;
            drop(state);
            for peer in &public_configuration.replicas {
                let donor = lookup_build_endpoint(&peer.replication_address).ok_or_else(|| {
                    RuntimeError::InvalidReplication(
                        "configured Build donor disappeared during cleanup".into(),
                    )
                })?;
                donor
                    .owner
                    .verify_direct_build_account(&account_receipt, false, &group, &callback_key)
                    .await?;
                donor
                    .owner
                    .finish_direct_build_cleanup(&callback_key, &group)
                    .await?;
            }
            target_owner
                .verify_recovery_account(&account_receipt, false, &target_child)
                .await?;
            orchestration
                .advance(BuildCheckpoint::AccountAbsent)
                .map_err(map_mysql)?;
            let target_progress = target_owner.refresh_progress(&target_child).await?;
            let mut donor_progress = Vec::new();
            for peer in &public_configuration.replicas {
                let donor = lookup_build_endpoint(&peer.replication_address).ok_or_else(|| {
                    RuntimeError::InvalidReplication(
                        "configured Build donor disappeared before progress refresh".into(),
                    )
                })?;
                donor_progress.push((
                    peer.clone(),
                    donor
                        .owner
                        .observe_direct_build_progress(&callback_key, &group)
                        .await?,
                ));
            }
            orchestration
                .advance(BuildCheckpoint::ProgressRefreshed)
                .map_err(map_mysql)?;
            source_child
                .admit_work()
                .map_err(|_| RuntimeError::OperationCancelled)?;
            target_child
                .admit_work()
                .map_err(|_| RuntimeError::OperationCancelled)?;
            let mut state = tokio::time::timeout_at(
                tokio::time::Instant::from_std(source_child.work_deadline()),
                self.lease(),
            )
            .await
            .map_err(|_| RuntimeError::OperationCancelled)?;
            require_bound_open(&state)?;
            let public = state
                .binding
                .as_ref()
                .ok_or(RuntimeError::AuthorityNotAdmitted)?;
            if state.current_configuration.as_ref() != Some(&public_configuration)
                || Self::local_public_role(&state, &public_configuration)? != ReplicaRole::Primary
                || state.engine.current_member_authority().map_err(map_mysql)?
                    != binding.source_authority
                || state.build_generation.id() != binding.source_generation
                || public.identity != binding.source.identity
                || public.process_session != binding.source.process_session
                || self.endpoint.encode() != binding.source.replication_address
                || state.last_reported_progress != Some(callback_key.source_progress)
            {
                return Err(RuntimeError::AuthorityMismatch(
                    "Build source binding or configuration changed before terminal success".into(),
                ));
            }
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(source_child.work_deadline()),
                Self::capture_current_boundary(&mut state, false),
            )
            .await
            .map_err(|_| RuntimeError::OperationCancelled)??;
            let final_boundary = state.cached_source_boundary.as_ref().ok_or_else(|| {
                RuntimeError::InvalidReplication(
                    "Build source lost structured evidence before terminal success".into(),
                )
            })?;
            if !binding
                .source_boundary
                .evidence()
                .executed()
                .is_subset_of(final_boundary.evidence().executed())
            {
                return Err(RuntimeError::InvalidReplication(
                    "Build source no longer contains the frozen boundary at terminal success"
                        .into(),
                ));
            }
            source_child
                .admit_work()
                .map_err(|_| RuntimeError::OperationCancelled)?;
            target_child
                .admit_work()
                .map_err(|_| RuntimeError::OperationCancelled)?;
            orchestration
                .advance(BuildCheckpoint::FinalRevalidated)
                .map_err(map_mysql)?;
            source_cleanup.clear_account();
            state
                .build_generation
                .set(BuildGenerationStatus::PendingAdmission);
            Self::retain_replica_progress(
                &mut state,
                &replica,
                target_progress.value(),
                INVALID_LSN,
            )?;
            for (peer, progress) in donor_progress {
                Self::retain_replica_progress(
                    &mut state,
                    &peer,
                    progress,
                    peer.catch_up_capability,
                )?;
            }
            state
                .build_results
                .complete(
                    build_id.as_str(),
                    &callback_key,
                    BuildTerminalResult::Succeeded,
                )
                .map_err(map_mysql)?;
            orchestration
                .advance(BuildCheckpoint::Completed)
                .map_err(map_mysql)?;
            Ok(())
        }
        .await;

        match execution {
            Ok(()) => {
                target_owner.disarm();
                source_cleanup.disarm();
                Ok(())
            }
            Err(primary) => {
                if orchestration.interruption_requires_reset() {
                    source_cleanup.block_for_reset();
                }
                let _ = target_owner.graceful_leave(&target_child).await;
                for peer in &public_configuration.replicas {
                    if let Some(donor) = lookup_build_endpoint(&peer.replication_address) {
                        let _ = donor
                            .owner
                            .enter_direct_build_cleanup(&callback_key, &group)
                            .await;
                    }
                }
                if orchestration.account_cleanup_owned()
                    && let Some(receipt) = source_cleanup.account().cloned()
                {
                    let source_absent = match orchestration.run_cleanup(self.lease()).await {
                        Ok(mut state) => state
                            .engine
                            .remove_build_recovery_account(&receipt, &source_child)
                            .await
                            .is_ok(),
                        Err(_) => false,
                    };
                    let mut participants_absent = source_absent;
                    if participants_absent {
                        for peer in &public_configuration.replicas {
                            let Some(donor) = lookup_build_endpoint(&peer.replication_address)
                            else {
                                participants_absent = false;
                                break;
                            };
                            if donor
                                .owner
                                .verify_direct_build_account(&receipt, false, &group, &callback_key)
                                .await
                                .is_err()
                            {
                                participants_absent = false;
                                break;
                            }
                        }
                    }
                    if participants_absent
                        && target_owner
                            .verify_recovery_account(&receipt, false, &target_child)
                            .await
                            .is_ok()
                    {
                        source_cleanup.clear_account();
                    }
                }
                let terminal = match &primary {
                    RuntimeError::ReplicaRebuildRequired(detail) => {
                        BuildTerminalResult::RebuildRequired((**detail).clone())
                    }
                    _ => BuildTerminalResult::Failed(primary.to_string()),
                };
                if let Ok(mut state) = orchestration.run_cleanup(self.lease()).await {
                    let _ =
                        state
                            .build_results
                            .complete(build_id.as_str(), &callback_key, terminal);
                }
                Err(primary)
            }
        }
    }

    async fn remove_replica(
        &self,
        _replica_id: kuberic_runtime::protocol::types::ReplicaId,
    ) -> Result<()> {
        Err(RuntimeError::InvalidReplication(
            "MySQL Public V1 Remove is unsupported".into(),
        ))
    }
}

fn operation(
    state: &mut DriverState,
    key: OperationKey,
    value: OperationValue,
) -> Result<MysqlOperationIdentity> {
    state.operations.admit(key, value).map_err(map_translation)
}

fn next_generation(current: NonZeroU64) -> Result<NonZeroU64> {
    current
        .get()
        .checked_add(1)
        .and_then(NonZeroU64::new)
        .ok_or_else(|| RuntimeError::InvalidReplication("access generation exhausted".into()))
}

fn proxy_requires_physical_containment(proxy: &ProxyState) -> bool {
    matches!(
        proxy,
        ProxyState::Prepared(_) | ProxyState::Published { .. }
    )
}

fn configuration_satisfies_catch_up(
    configuration: &ReplicaSetConfiguration,
    local: &ReplicaIdentity,
    boundary: i64,
    mode: ReplicaSetQuorumMode,
) -> Result<bool> {
    let required = match mode {
        ReplicaSetQuorumMode::WriteQuorum => {
            usize::try_from(configuration.configuration.write_quorum).map_err(|_| {
                RuntimeError::InvalidReplication("write quorum does not fit usize".into())
            })?
        }
        ReplicaSetQuorumMode::All => configuration.configuration.members.len(),
    };
    let local_member = configuration
        .configuration
        .members
        .iter()
        .find(|member| &member.identity == local)
        .ok_or_else(|| {
            RuntimeError::InvalidReplication(
                "catch-up configuration omits the bound local replica".into(),
            )
        })?;
    let local_satisfied = usize::from(matches!(
        local_member.role,
        ReplicaRole::Primary | ReplicaRole::ActiveSecondary
    ));
    let satisfied = local_satisfied
        + configuration
            .replicas
            .iter()
            .filter(|replica| {
                matches!(
                    replica.role,
                    ReplicaRole::Primary | ReplicaRole::ActiveSecondary
                ) && replica.current_progress != INVALID_LSN
                    && replica.current_progress >= boundary
            })
            .count();
    let new_primary_proved = if local.replica_id == configuration.configuration.primary_id {
        local_member.role == ReplicaRole::Primary
    } else {
        configuration
            .replicas
            .iter()
            .find(|replica| replica.identity.replica_id == configuration.configuration.primary_id)
            .is_some_and(|replica| {
                replica.role == ReplicaRole::Primary
                    && replica.current_progress != INVALID_LSN
                    && replica.current_progress >= boundary
            })
    };
    Ok(satisfied >= required && new_primary_proved)
}

fn configuration_can_complete(
    completed_roles: &BTreeMap<EpochKey, ReplicaRole>,
    epoch: EpochKey,
    role: ReplicaRole,
    engine_epoch: Option<MysqlReplicaEpoch>,
    configuration_epoch: MysqlReplicaEpoch,
) -> bool {
    completed_roles.get(&epoch) == Some(&role) && engine_epoch == Some(configuration_epoch)
}

const fn identical_refresh_requires_primary_publication(role: ReplicaRole) -> bool {
    matches!(role, ReplicaRole::Primary)
}

fn normalize_configuration_progress(
    retained: &[RetainedReplicaProgress],
    configuration: &ReplicaSetConfiguration,
) -> Result<ReplicaSetConfiguration> {
    let mut normalized = configuration.clone();
    for replica in &mut normalized.replicas {
        let storage = replica.storage_incarnation.as_ref().ok_or_else(|| {
            RuntimeError::InvalidReplication(
                "configured replica omitted storage incarnation".into(),
            )
        })?;
        let known = retained.iter().find(|known| {
            known.identity == replica.identity
                && known.process_session == replica.process_session_id
                && &known.storage == storage
        });
        if replica.current_progress == INVALID_LSN {
            replica.current_progress = known.map(|known| known.current).ok_or_else(|| {
                RuntimeError::InvalidReplication(
                    "configured replica omitted current progress without exact retained state"
                        .into(),
                )
            })?;
        }
        if replica.catch_up_capability == INVALID_LSN {
            if let Some(known) = known {
                replica.catch_up_capability = known.catch_up;
            } else if replica.must_catchup {
                return Err(RuntimeError::InvalidReplication(
                    "required catch-up capability is unknown".into(),
                ));
            }
        }
        if replica.must_catchup && replica.catch_up_capability == INVALID_LSN {
            return Err(RuntimeError::InvalidReplication(
                "required catch-up capability remained unknown".into(),
            ));
        }
    }
    Ok(normalized)
}

fn progress_without_current_configuration(
    cached_pre_epoch_progress: Option<i64>,
    fresh_open_progress: Option<i64>,
    access: Option<MysqlAccessIntent>,
) -> Result<i64> {
    if access.is_none_or(|intent| intent.read() || intent.write()) {
        return Err(RuntimeError::InvalidReplication(
            "MySQL progress without a current configuration requires closed access".into(),
        ));
    }
    cached_pre_epoch_progress
        .or(fresh_open_progress)
        .ok_or_else(|| {
            RuntimeError::InvalidReplication(
                "MySQL progress has no qualified current, pre-epoch, or fresh-open observation"
                    .into(),
            )
        })
}

fn retain_reported_progress(retained: &mut Option<i64>, progress: i64) {
    *retained = Some(progress);
}

fn retained_fresh_open_progress(
    progress: QualifiedProgress,
) -> (Option<QualifiedProgress>, Option<i64>) {
    (Some(progress), Some(progress.value()))
}

fn retained_committed_progress(retained: Option<i64>) -> Result<i64> {
    retained.ok_or_else(|| {
        RuntimeError::InvalidReplication(
            "committed progress requires a preceding exact current-progress observation".into(),
        )
    })
}

fn catch_up_requires_fresh_boundary(
    has_current_configuration: bool,
    has_cached_pre_epoch_progress: bool,
) -> Result<bool> {
    if has_current_configuration {
        Ok(true)
    } else if has_cached_pre_epoch_progress {
        Ok(false)
    } else {
        Err(RuntimeError::ReconfigurationPending)
    }
}

fn require_bound_open(state: &DriverState) -> Result<()> {
    if state.closed || state.engine.terminal_state().is_some() {
        Err(RuntimeError::Closed)
    } else if state.binding.is_none() {
        Err(RuntimeError::AuthorityNotAdmitted)
    } else if !state.opened {
        Err(RuntimeError::NotOpen)
    } else {
        Ok(())
    }
}

fn require_not_cleanup_only(state: &DriverState) -> Result<()> {
    if matches!(
        state.build_generation.status(),
        BuildGenerationStatus::CleanupOnly | BuildGenerationStatus::NeedsRebuild
    ) {
        Err(RuntimeError::ReconfigurationPending)
    } else {
        Ok(())
    }
}

const fn role_code(role: ReplicaRole) -> u8 {
    match role {
        ReplicaRole::Primary => 1,
        ReplicaRole::ActiveSecondary => 2,
        ReplicaRole::IdleSecondary => 3,
        ReplicaRole::None => 4,
    }
}

fn map_translation(error: KubericTranslationError) -> RuntimeError {
    RuntimeError::InvalidReplication(error.to_string())
}

fn map_mysql(error: MysqlReplicatorError) -> RuntimeError {
    match error {
        MysqlReplicatorError::NotOpen => RuntimeError::NotOpen,
        MysqlReplicatorError::PrimaryRequired => RuntimeError::NotPrimary,
        MysqlReplicatorError::Cancelled
        | MysqlReplicatorError::LateReceipt { .. }
        | MysqlReplicatorError::Deadline(_) => RuntimeError::OperationCancelled,
        MysqlReplicatorError::Terminal => RuntimeError::Closed,
        MysqlReplicatorError::ConfigurationRequired
        | MysqlReplicatorError::EpochBarrierRequired
        | MysqlReplicatorError::RoleRequired
        | MysqlReplicatorError::AccessRevocationRequired => RuntimeError::ReconfigurationPending,
        MysqlReplicatorError::OpenConflict
        | MysqlReplicatorError::IdentityMismatch
        | MysqlReplicatorError::InvalidMemberConfiguration
        | MysqlReplicatorError::ApplicationCredentialRequired
        | MysqlReplicatorError::StaleOperationGeneration
        | MysqlReplicatorError::OperationConflict
        | MysqlReplicatorError::OperationUncertain
        | MysqlReplicatorError::StaleEpoch
        | MysqlReplicatorError::SelfTarget
        | MysqlReplicatorError::InvalidConfiguration
        | MysqlReplicatorError::StaleAccessGeneration
        | MysqlReplicatorError::ReceiptMismatch
        | MysqlReplicatorError::NativeRoleMismatch
        | MysqlReplicatorError::HandoffProofRequired
        | MysqlReplicatorError::HandoffProofDuplicate
        | MysqlReplicatorError::HandoffProofConflict
        | MysqlReplicatorError::CloseUnsettled
        | MysqlReplicatorError::AbortUnsettled => {
            RuntimeError::InvalidReplication("MySQL lifecycle evidence was rejected".into())
        }
        MysqlReplicatorError::MemberTransition(_)
        | MysqlReplicatorError::FreshPreparationCleanup { .. }
        | MysqlReplicatorError::ConfigurationCleanup { .. }
        | MysqlReplicatorError::Proxy(_)
        | MysqlReplicatorError::ProxyCleanup { .. }
        | MysqlReplicatorError::Instance(_) => {
            RuntimeError::Application("MySQL member-local operation failed".into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kuberic_runtime::protocol::types::{
        AgentGeneration, ConfigurationDescriptor, ConfigurationMember, OperationId, PodUid,
        ProcessSessionId, PvcUid, ReplicaId, ReplicaInstanceId, ReplicaStorageIdentity,
        ReplicaStorageIncarnationContext, StorageGeneration,
    };

    fn identity(id: i64) -> ReplicaIdentity {
        ReplicaIdentity {
            replica_id: ReplicaId::new(id),
            instance_id: ReplicaInstanceId::new(format!("instance-{id}")),
            agent_generation: AgentGeneration::new(format!("generation-{id}")),
        }
    }

    fn information(id: i64, role: ReplicaRole, progress: i64) -> ReplicaInformation {
        ReplicaInformation {
            build_id: OperationId::new(format!("build-{id}")),
            identity: identity(id),
            replication_address: format!("endpoint-{id}"),
            process_session_id: ProcessSessionId::new(format!("session-{id}")),
            role,
            current_progress: progress,
            catch_up_capability: INVALID_LSN,
            must_catchup: false,
            storage_incarnation: None,
        }
    }

    fn storage(id: i64) -> ReplicaStorageIncarnationContext {
        ReplicaStorageIncarnationContext {
            replica: identity(id),
            storage: ReplicaStorageIdentity::Persistent {
                pod_uid: PodUid::new(format!("pod-{id}")),
                pvc_uid: PvcUid::new(format!("pvc-{id}")),
            },
            storage_generation: StorageGeneration::new(format!("storage-{id}")),
        }
    }

    fn one_remote(replica: ReplicaInformation) -> ReplicaSetConfiguration {
        ReplicaSetConfiguration {
            configuration: ConfigurationDescriptor::new(
                Epoch::new(1, 2),
                ReplicaId::new(1),
                vec![
                    ConfigurationMember {
                        identity: identity(1),
                        role: ReplicaRole::Primary,
                    },
                    ConfigurationMember {
                        identity: replica.identity.clone(),
                        role: replica.role,
                    },
                ],
                2,
            ),
            replicas: vec![replica],
        }
    }

    #[test]
    fn omitted_progress_reuse_is_exact_per_field_and_binding() {
        let mut replica = information(2, ReplicaRole::ActiveSecondary, INVALID_LSN);
        replica.storage_incarnation = Some(storage(2));
        let retained = vec![RetainedReplicaProgress {
            identity: replica.identity.clone(),
            process_session: replica.process_session_id.clone(),
            storage: storage(2),
            current: 0,
            catch_up: INVALID_LSN,
        }];
        let normalized =
            normalize_configuration_progress(&retained, &one_remote(replica.clone())).unwrap();
        assert_eq!(normalized.replicas[0].current_progress, 0);
        assert_eq!(normalized.replicas[0].catch_up_capability, INVALID_LSN);

        assert!(normalize_configuration_progress(&[], &one_remote(replica.clone())).is_err());

        let mut first_known_unknown = replica.clone();
        first_known_unknown.current_progress = 7;
        let normalized =
            normalize_configuration_progress(&[], &one_remote(first_known_unknown.clone()))
                .unwrap();
        assert_eq!(normalized.replicas[0].catch_up_capability, INVALID_LSN);
        first_known_unknown.must_catchup = true;
        assert!(normalize_configuration_progress(&[], &one_remote(first_known_unknown)).is_err());

        let numeric = vec![RetainedReplicaProgress {
            catch_up: 4,
            ..retained[0].clone()
        }];
        let normalized =
            normalize_configuration_progress(&numeric, &one_remote(replica.clone())).unwrap();
        assert_eq!(normalized.replicas[0].catch_up_capability, 4);

        let mut rejected_explicit = replica.clone();
        rejected_explicit.current_progress = 99;
        let _candidate =
            normalize_configuration_progress(&retained, &one_remote(rejected_explicit)).unwrap();
        let omitted_after_rejection =
            normalize_configuration_progress(&retained, &one_remote(replica.clone())).unwrap();
        assert_eq!(
            omitted_after_rejection.replicas[0].current_progress, 0,
            "normalization must not contaminate retained state before acceptance"
        );

        let mut changed_session = replica.clone();
        changed_session.process_session_id = ProcessSessionId::new("replacement-session");
        assert!(normalize_configuration_progress(&retained, &one_remote(changed_session)).is_err());

        let mut changed_storage = replica.clone();
        changed_storage.storage_incarnation = Some(storage(3));
        assert!(normalize_configuration_progress(&retained, &one_remote(changed_storage)).is_err());

        let mut changed_replica = replica;
        changed_replica.identity = identity(3);
        changed_replica.storage_incarnation = Some(storage(3));
        assert!(normalize_configuration_progress(&retained, &one_remote(changed_replica)).is_err());
    }

    #[test]
    fn access_generation_never_wraps_or_uses_zero() {
        assert_eq!(
            next_generation(NonZeroU64::new(7).unwrap()).unwrap().get(),
            8
        );
        assert!(next_generation(NonZeroU64::new(u64::MAX).unwrap()).is_err());
    }

    #[test]
    fn prepared_and_published_proxy_generations_require_physical_containment() {
        let endpoint: SocketAddr = "127.0.0.1:3307".parse().unwrap();
        assert!(!proxy_requires_physical_containment(
            &ProxyState::Unprepared
        ));
        assert!(!proxy_requires_physical_containment(&ProxyState::Revoked));
        assert!(proxy_requires_physical_containment(&ProxyState::Prepared(
            endpoint
        )));
        assert!(proxy_requires_physical_containment(
            &ProxyState::Published {
                endpoint,
                epoch: Epoch::new(1, 2),
                configuration: PublicConfigurationId::new("configuration"),
            }
        ));
    }

    #[test]
    fn public_driver_conforms_to_the_pinned_trait_bundle() {
        fn assert_driver<T: Replicator + PrimaryReplicator + StateProvider + Send + Sync>() {}
        fn assert_factory<T: ReplicatorFactory + Send + Sync>() {}

        assert_driver::<MysqlPublicV1Replicator>();
        assert_factory::<MysqlPublicV1ReplicatorFactory>();
    }

    #[test]
    fn catch_up_predicate_never_counts_missing_or_invalid_progress() {
        let primary = information(1, ReplicaRole::Primary, 9);
        let secondary = information(2, ReplicaRole::ActiveSecondary, INVALID_LSN);
        let descriptor = ConfigurationDescriptor::new(
            Epoch::new(1, 2),
            primary.identity.replica_id,
            vec![
                ConfigurationMember {
                    identity: primary.identity.clone(),
                    role: primary.role,
                },
                ConfigurationMember {
                    identity: secondary.identity.clone(),
                    role: secondary.role,
                },
            ],
            2,
        );
        let configuration = ReplicaSetConfiguration {
            configuration: descriptor,
            replicas: vec![secondary],
        };
        assert!(
            !configuration_satisfies_catch_up(
                &configuration,
                &primary.identity,
                9,
                ReplicaSetQuorumMode::WriteQuorum,
            )
            .unwrap()
        );
        assert!(
            !configuration_satisfies_catch_up(
                &configuration,
                &primary.identity,
                9,
                ReplicaSetQuorumMode::All,
            )
            .unwrap()
        );
    }

    #[test]
    fn ordinary_formed_configuration_completes_only_after_role_for_the_epoch() {
        let epoch = Epoch::new(4, 9);
        let key = EpochKey::from(epoch);
        let mapped = MysqlReplicaEpoch::new(3);
        let mut completed = BTreeMap::new();
        assert!(!configuration_can_complete(
            &completed,
            key,
            ReplicaRole::ActiveSecondary,
            Some(mapped),
            mapped,
        ));
        completed.insert(key, ReplicaRole::ActiveSecondary);
        assert!(configuration_can_complete(
            &completed,
            key,
            ReplicaRole::ActiveSecondary,
            Some(mapped),
            mapped,
        ));
        assert!(!configuration_can_complete(
            &completed,
            key,
            ReplicaRole::Primary,
            Some(mapped),
            mapped,
        ));
    }

    #[test]
    fn planned_swap_catch_up_counts_synthesized_local_and_exact_remote_peers() {
        let source = identity(1);
        let target = information(2, ReplicaRole::Primary, 12);
        let remaining = information(3, ReplicaRole::ActiveSecondary, 12);
        let descriptor = ConfigurationDescriptor::new(
            Epoch::new(2, 5),
            target.identity.replica_id,
            vec![
                ConfigurationMember {
                    identity: source.clone(),
                    role: ReplicaRole::ActiveSecondary,
                },
                ConfigurationMember {
                    identity: target.identity.clone(),
                    role: target.role,
                },
                ConfigurationMember {
                    identity: remaining.identity.clone(),
                    role: remaining.role,
                },
            ],
            2,
        );
        let configuration = ReplicaSetConfiguration {
            configuration: descriptor,
            replicas: vec![target, remaining],
        };
        assert!(
            configuration_satisfies_catch_up(
                &configuration,
                &source,
                12,
                ReplicaSetQuorumMode::All,
            )
            .unwrap()
        );
    }

    #[test]
    fn exact_old_role_epoch_is_stale_after_a_newer_epoch_mapping() {
        let mut epochs = EpochTranslator::new();
        epochs.translate(Epoch::new(1, 4)).unwrap();
        epochs.translate(Epoch::new(1, 5)).unwrap();
        assert!(matches!(
            epochs.translate(Epoch::new(1, 4)),
            Err(KubericTranslationError::StaleEpoch { .. })
        ));
    }

    #[test]
    fn progress_without_configuration_requires_an_exact_cached_observation_while_closed() {
        let closed = MysqlAccessIntent::new(NonZeroU64::MIN, false, false);
        let open = MysqlAccessIntent::new(NonZeroU64::MIN, true, true);

        assert!(progress_without_current_configuration(None, None, Some(closed)).is_err());
        assert_eq!(
            progress_without_current_configuration(Some(17), None, Some(closed)).unwrap(),
            17
        );
        assert_eq!(
            progress_without_current_configuration(None, Some(0), Some(closed)).unwrap(),
            0
        );
        assert!(progress_without_current_configuration(Some(17), Some(0), Some(open)).is_err());
        assert!(progress_without_current_configuration(None, None, None).is_err());
    }

    #[test]
    fn identical_primary_refresh_retries_publication_only_for_primary() {
        assert!(identical_refresh_requires_primary_publication(
            ReplicaRole::Primary
        ));
        assert!(!identical_refresh_requires_primary_publication(
            ReplicaRole::ActiveSecondary
        ));
    }

    #[test]
    fn state_provider_returns_the_exact_last_reported_scalar_without_a_zero_fallback() {
        let mut retained = None;
        assert!(retained_committed_progress(retained).is_err());

        retain_reported_progress(&mut retained, 0);
        assert_eq!(retained_committed_progress(retained).unwrap(), 0);

        retain_reported_progress(&mut retained, 73);
        assert_eq!(retained_committed_progress(retained).unwrap(), 73);
    }

    #[test]
    fn qualified_fresh_open_progress_is_retained_for_applied_and_committed_reads() {
        let lineage =
            crate::core::ConfiguredGtidLineage::new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                .unwrap();
        let progress = match crate::core::project_progress(&lineage, &crate::core::GtidSet::empty())
        {
            ProgressProjection::Projectable(progress) => progress,
            other => panic!("unexpected fresh projection: {other:?}"),
        };
        let closed = MysqlAccessIntent::new(NonZeroU64::MIN, false, false);
        let (fresh, committed) = retained_fresh_open_progress(progress);

        assert_eq!(
            progress_without_current_configuration(
                None,
                fresh.map(QualifiedProgress::value),
                Some(closed),
            )
            .unwrap(),
            0
        );
        assert_eq!(retained_committed_progress(committed).unwrap(), 0);
    }

    #[test]
    fn catch_up_refreshes_current_boundaries_and_only_reuses_the_epoch_gap_cache() {
        assert!(catch_up_requires_fresh_boundary(true, false).unwrap());
        assert!(catch_up_requires_fresh_boundary(true, true).unwrap());
        assert!(!catch_up_requires_fresh_boundary(false, true).unwrap());
        assert!(catch_up_requires_fresh_boundary(false, false).is_err());
    }
}
