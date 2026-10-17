use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::io::Read;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use kuberic_runtime::RuntimeError;
use kuberic_runtime::protocol::types::{
    ConfigurationId, Epoch, OperationId, ProcessSessionId, ReplicaIdentity,
    ReplicaStorageIncarnationContext,
};
use kuberic_runtime::replicator::ReplicaSetConfiguration;
use zeroize::Zeroize;

use crate::core::AttemptId;
use crate::core::CredentialGeneration;
use crate::service::{
    BuildAttemptContext, BuildAttemptGroup, MemberEffectReceipt, MemberMutationAuthority,
    MemberSourceBoundary, MysqlReplicaTarget, MysqlReplicatorError,
};

use super::driver::MysqlPublicV1Replicator;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct IdleBuildPreparation {
    epoch: Option<Epoch>,
    idle_role: bool,
}

impl IdleBuildPreparation {
    pub(crate) fn observe_epoch(&mut self, epoch: Epoch) -> Result<(), MysqlReplicatorError> {
        if self.epoch.is_some_and(|retained| retained != epoch) {
            return Err(MysqlReplicatorError::OperationConflict);
        }
        self.epoch = Some(epoch);
        Ok(())
    }

    pub(crate) fn admit_role(&mut self, epoch: Epoch) -> Result<(), MysqlReplicatorError> {
        self.observe_epoch(epoch)?;
        self.idle_role = true;
        Ok(())
    }

    pub(crate) fn validate_epoch(&self, epoch: Epoch) -> Result<(), MysqlReplicatorError> {
        if self.epoch.is_some_and(|retained| retained != epoch) {
            Err(MysqlReplicatorError::OperationConflict)
        } else {
            Ok(())
        }
    }

    pub(crate) const fn epoch(&self) -> Option<Epoch> {
        self.epoch
    }

    pub(crate) const fn is_idle(&self) -> bool {
        self.idle_role
    }

    pub(crate) fn clear_uncommitted_epoch(&mut self) {
        if !self.idle_role {
            self.epoch = None;
        }
    }

    pub(crate) fn complete(&mut self) {
        self.epoch = None;
        self.idle_role = false;
    }
}

/// Exact public and native target values admitted for one Build callback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BuildTargetBinding {
    pub(crate) identity: ReplicaIdentity,
    pub(crate) process_session: ProcessSessionId,
    pub(crate) storage: ReplicaStorageIncarnationContext,
    pub(crate) replication_address: String,
    pub(crate) native: MysqlReplicaTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BuildSourceBinding {
    pub(crate) identity: ReplicaIdentity,
    pub(crate) process_session: ProcessSessionId,
    pub(crate) replication_address: String,
    pub(crate) native: MysqlReplicaTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BuildCallbackKey {
    pub(crate) build_id: OperationId,
    pub(crate) epoch: Epoch,
    pub(crate) configuration: ConfigurationId,
    pub(crate) group_key: String,
    pub(crate) source_identity: ReplicaIdentity,
    pub(crate) source_process_session: ProcessSessionId,
    pub(crate) source_configuration: ReplicaSetConfiguration,
    pub(crate) source_authority: MemberMutationAuthority,
    pub(crate) source_boundary: MemberSourceBoundary,
    pub(crate) source_progress: i64,
    pub(crate) target: BuildTargetBinding,
}

/// Immutable application-owned authority for one direct custom Build.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BuildBinding {
    pub(crate) build_id: OperationId,
    pub(crate) child_attempt: AttemptId,
    pub(crate) source_parent_attempt: AttemptId,
    pub(crate) target_parent_attempt: AttemptId,
    pub(crate) source_generation: u64,
    pub(crate) target_generation: u64,
    pub(crate) epoch: Epoch,
    pub(crate) configuration: ConfigurationId,
    pub(crate) group_key: String,
    pub(crate) source: BuildSourceBinding,
    pub(crate) target: BuildTargetBinding,
    pub(crate) source_authority: MemberMutationAuthority,
    pub(crate) expected_members: Vec<ReplicaIdentity>,
    pub(crate) source_boundary: MemberSourceBoundary,
    pub(crate) grant_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BuildTerminalResult {
    Succeeded,
    Failed(String),
    RebuildRequired(kuberic_runtime::protocol::public_operations::ReplicaRebuildRequired),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BuildRecord<B> {
    binding: B,
    terminal: Option<BuildTerminalResult>,
}

/// Exact-retry ledger for application-owned Build callbacks.
pub(crate) struct BuildResultLedger<B = BuildBinding> {
    records: BTreeMap<String, BuildRecord<B>>,
}

impl<B> Default for BuildResultLedger<B> {
    fn default() -> Self {
        Self {
            records: BTreeMap::new(),
        }
    }
}

impl<B> BuildResultLedger<B>
where
    B: Clone + Eq,
{
    pub(crate) fn retained(&self, key: &str) -> Option<(B, Option<BuildTerminalResult>)> {
        self.records
            .get(key)
            .map(|record| (record.binding.clone(), record.terminal.clone()))
    }

    pub(crate) fn admit(
        &mut self,
        key: &str,
        binding: &B,
    ) -> Result<Option<BuildTerminalResult>, MysqlReplicatorError> {
        if let Some(record) = self.records.get(key) {
            if record.binding != *binding {
                return Err(MysqlReplicatorError::OperationConflict);
            }
            return Ok(record.terminal.clone());
        }
        self.records.insert(
            key.to_owned(),
            BuildRecord {
                binding: binding.clone(),
                terminal: None,
            },
        );
        Ok(None)
    }

    pub(crate) fn complete(
        &mut self,
        key: &str,
        binding: &B,
        terminal: BuildTerminalResult,
    ) -> Result<(), MysqlReplicatorError> {
        let record = self
            .records
            .get_mut(key)
            .ok_or(MysqlReplicatorError::OperationConflict)?;
        if record.binding != *binding {
            return Err(MysqlReplicatorError::OperationConflict);
        }
        if let Some(retained) = &record.terminal {
            if retained != &terminal {
                return Err(MysqlReplicatorError::OperationConflict);
            }
            return Ok(());
        }
        record.terminal = Some(terminal);
        Ok(())
    }
}

/// Secret bytes never rendered through Debug and cleared when ownership ends.
pub(crate) struct BuildSecret(Box<[u8]>);

impl BuildSecret {
    pub(crate) fn new(bytes: impl Into<Box<[u8]>>) -> Result<Self, MysqlReplicatorError> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(MysqlReplicatorError::IdentityMismatch);
        }
        Ok(Self(bytes))
    }

    pub(crate) fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for BuildSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BuildSecret([REDACTED])")
    }
}

impl Drop for BuildSecret {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// One random Build-scoped credential accepted only by the private recovery
/// listener.
pub(crate) struct BuildRecoveryCredential {
    build: AttemptId,
    generation: CredentialGeneration,
    username: String,
    password: BuildSecret,
}

impl BuildRecoveryCredential {
    pub(crate) fn random(build: AttemptId) -> Result<Self, MysqlReplicatorError> {
        static NEXT_CREDENTIAL: AtomicU64 = AtomicU64::new(1);
        let serial = NEXT_CREDENTIAL.fetch_add(1, Ordering::Relaxed);
        let generation = CredentialGeneration::new(format!("build-{serial}"))
            .map_err(|_| MysqlReplicatorError::IdentityMismatch)?;
        let username = format!("kms_build_{serial}");
        let mut random = [0_u8; 16];
        std::fs::File::open("/dev/urandom")
            .and_then(|mut file| file.read_exact(&mut random))
            .map_err(|_| MysqlReplicatorError::IdentityMismatch)?;
        let mut password = String::with_capacity(random.len() * 2);
        for byte in random {
            use std::fmt::Write as _;
            write!(&mut password, "{byte:02x}")
                .map_err(|_| MysqlReplicatorError::IdentityMismatch)?;
        }
        Self::new(build, generation, username, password.into_bytes())
    }

    pub(crate) fn new(
        build: AttemptId,
        generation: CredentialGeneration,
        username: impl Into<String>,
        password: impl Into<Box<[u8]>>,
    ) -> Result<Self, MysqlReplicatorError> {
        let username = username.into();
        let password = password.into();
        if username.is_empty()
            || username.len() > 32
            || !username
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            || password.is_empty()
            || password.len() > 128
            || password
                .iter()
                .any(|byte| !byte.is_ascii_graphic() || matches!(byte, b'\'' | b'"' | b'\\' | b'`'))
        {
            return Err(MysqlReplicatorError::IdentityMismatch);
        }
        Ok(Self {
            build,
            generation,
            username,
            password: BuildSecret::new(password)?,
        })
    }

    pub(crate) const fn build(&self) -> &AttemptId {
        &self.build
    }

    pub(crate) const fn generation(&self) -> &CredentialGeneration {
        &self.generation
    }

    pub(crate) fn username(&self) -> &str {
        &self.username
    }

    pub(crate) fn password(&self) -> &str {
        std::str::from_utf8(self.password.expose())
            .expect("Build password is validated UTF-8 at creation")
    }
}

impl fmt::Debug for BuildRecoveryCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BuildRecoveryCredential")
            .field("build", &self.build)
            .field("generation", &self.generation)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// Secret-free proof of one exact Build recovery account.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BuildRecoveryAccountReceipt {
    build: AttemptId,
    generation: CredentialGeneration,
    username: String,
    marker: String,
}

/// Request-bound native join proof retained without admitting Kuberic
/// configuration membership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BuildJoinReceipt {
    build: AttemptId,
    parent_attempt: AttemptId,
    target_generation: u64,
    member: MemberEffectReceipt,
}

impl BuildJoinReceipt {
    pub(crate) fn new(
        build: &BuildAttemptContext,
        target_generation: u64,
        member: MemberEffectReceipt,
    ) -> Result<Self, MysqlReplicatorError> {
        if member.authority().attempt() != build.parent_attempt() {
            return Err(MysqlReplicatorError::IdentityMismatch);
        }
        Ok(Self {
            build: build.build().clone(),
            parent_attempt: build.parent_attempt().clone(),
            target_generation,
            member,
        })
    }

    pub(crate) const fn build(&self) -> &AttemptId {
        &self.build
    }

    pub(crate) const fn parent_attempt(&self) -> &AttemptId {
        &self.parent_attempt
    }

    pub(crate) const fn target_generation(&self) -> u64 {
        self.target_generation
    }

    pub(crate) const fn member(&self) -> &MemberEffectReceipt {
        &self.member
    }
}

impl BuildRecoveryAccountReceipt {
    pub(crate) fn new(credential: &BuildRecoveryCredential) -> Self {
        Self {
            build: credential.build.clone(),
            generation: credential.generation.clone(),
            username: credential.username.clone(),
            marker: build_account_marker(&credential.build, &credential.generation),
        }
    }

    pub(crate) const fn build(&self) -> &AttemptId {
        &self.build
    }

    pub(crate) const fn generation(&self) -> &CredentialGeneration {
        &self.generation
    }

    pub(crate) fn username(&self) -> &str {
        &self.username
    }

    pub(crate) fn marker(&self) -> &str {
        &self.marker
    }

    pub(crate) fn marker_is_well_formed(&self) -> bool {
        self.marker == build_account_marker(&self.build, self.generation())
    }
}

fn build_account_marker(build: &AttemptId, generation: &CredentialGeneration) -> String {
    format!(
        "kuberic-mysql-build-v1:{}:{}",
        hex_encode(build.as_str().as_bytes()),
        hex_encode(generation.as_str().as_bytes())
    )
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum BuildGenerationStatus {
    Available = 0,
    Reserved = 1,
    CleanupOnly = 2,
    NeedsRebuild = 3,
    PendingAdmission = 4,
}

/// Ordered checkpoints for the production Build coordinator. Keeping this
/// sequence explicit makes interruption disposition deterministic rather than
/// inferred from whichever async call happened to fail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum BuildCheckpoint {
    TargetFresh = 0,
    AccountCleanupOwned = 1,
    AccountPresent = 2,
    DonorsPrepared = 3,
    SourceRevalidated = 4,
    TargetJoined = 5,
    SuccessorsObserved = 6,
    CleanupOnly = 7,
    AccountAbsent = 8,
    ProgressRefreshed = 9,
    FinalRevalidated = 10,
    Completed = 11,
}

#[derive(Debug)]
pub(crate) struct BuildOrchestration {
    group: BuildAttemptGroup,
    last: Option<BuildCheckpoint>,
}

impl BuildOrchestration {
    pub(crate) fn new(group: BuildAttemptGroup) -> Self {
        Self { group, last: None }
    }

    pub(crate) async fn run_work<T>(
        &self,
        future: impl Future<Output = T>,
    ) -> Result<T, RuntimeError> {
        if self.group.cancellation_state() == crate::service::AttemptCancellationState::Cancelled {
            return Err(RuntimeError::OperationCancelled);
        }
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(self.group.work_deadline()),
            future,
        )
        .await
        .map_err(|_| RuntimeError::OperationCancelled)
    }

    pub(crate) async fn run_cleanup<T>(
        &self,
        future: impl Future<Output = T>,
    ) -> Result<T, RuntimeError> {
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(self.group.deadline()),
            future,
        )
        .await
        .map_err(|_| RuntimeError::OperationCancelled)
    }

    pub(crate) fn advance(
        &mut self,
        checkpoint: BuildCheckpoint,
    ) -> Result<(), MysqlReplicatorError> {
        let expected = self.last.map_or(0, |last| last as u8 + 1);
        if checkpoint as u8 != expected {
            return Err(MysqlReplicatorError::OperationConflict);
        }
        self.last = Some(checkpoint);
        Ok(())
    }

    pub(crate) fn account_cleanup_owned(&self) -> bool {
        self.last.is_some_and(|last| {
            last as u8 >= BuildCheckpoint::AccountCleanupOwned as u8
                && (last as u8) < BuildCheckpoint::AccountAbsent as u8
        })
    }

    pub(crate) fn interruption_requires_reset(&self) -> bool {
        self.last
            .is_some_and(|last| last as u8 >= BuildCheckpoint::TargetJoined as u8)
    }
}

pub(crate) struct BuildGeneration {
    id: u64,
    status: AtomicU8,
}

impl BuildGeneration {
    pub(crate) fn new() -> Self {
        static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
        Self {
            id: NEXT_GENERATION.fetch_add(1, Ordering::Relaxed),
            status: AtomicU8::new(BuildGenerationStatus::Available as u8),
        }
    }

    pub(crate) const fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn status(&self) -> BuildGenerationStatus {
        match self.status.load(Ordering::Acquire) {
            0 => BuildGenerationStatus::Available,
            1 => BuildGenerationStatus::Reserved,
            2 => BuildGenerationStatus::CleanupOnly,
            3 => BuildGenerationStatus::NeedsRebuild,
            4 => BuildGenerationStatus::PendingAdmission,
            _ => unreachable!("BuildGeneration stores only known states"),
        }
    }

    pub(crate) fn reserve(&self) -> bool {
        self.status
            .compare_exchange(
                BuildGenerationStatus::Available as u8,
                BuildGenerationStatus::Reserved as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub(crate) fn set(&self, status: BuildGenerationStatus) {
        self.status.store(status as u8, Ordering::Release);
    }
}

struct EndpointRegistry {
    entries: BTreeMap<String, RegisteredEndpoint>,
}

struct RegisteredEndpoint {
    owner: Weak<MysqlPublicV1Replicator>,
    generation: Weak<BuildGeneration>,
    generation_id: u64,
}

fn registry() -> &'static Mutex<EndpointRegistry> {
    static REGISTRY: OnceLock<Mutex<EndpointRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        Mutex::new(EndpointRegistry {
            entries: BTreeMap::new(),
        })
    })
}

/// Process-local endpoint ownership. Dropping it unregisters only the same driver.
pub(crate) struct BuildEndpointRegistration {
    endpoint: String,
    generation: u64,
}

impl BuildEndpointRegistration {
    pub(crate) fn register(
        endpoint: String,
        owner: &Arc<MysqlPublicV1Replicator>,
        generation: &Arc<BuildGeneration>,
    ) -> Result<Self, MysqlReplicatorError> {
        Self::register_weak(endpoint, Arc::downgrade(owner), generation)
    }

    fn register_weak(
        endpoint: String,
        weak: Weak<MysqlPublicV1Replicator>,
        generation: &Arc<BuildGeneration>,
    ) -> Result<Self, MysqlReplicatorError> {
        let weak_generation = Arc::downgrade(generation);
        let mut registry = registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = registry.entries.get(&endpoint)
            && existing.generation.strong_count() > 0
            && existing.generation_id != generation.id()
        {
            return Err(MysqlReplicatorError::IdentityMismatch);
        }
        registry.entries.insert(
            endpoint.clone(),
            RegisteredEndpoint {
                owner: weak.clone(),
                generation: weak_generation,
                generation_id: generation.id(),
            },
        );
        Ok(Self {
            endpoint,
            generation: generation.id(),
        })
    }
}

impl Drop for BuildEndpointRegistration {
    fn drop(&mut self) {
        let mut registry = registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registry
            .entries
            .get(&self.endpoint)
            .is_some_and(|current| current.generation_id == self.generation)
        {
            registry.entries.remove(&self.endpoint);
        }
    }
}

pub(crate) struct BuildEndpoint {
    pub(crate) owner: Arc<MysqlPublicV1Replicator>,
    pub(crate) generation: Arc<BuildGeneration>,
}

pub(crate) fn lookup_build_endpoint(endpoint: &str) -> Option<BuildEndpoint> {
    let registered = {
        let registry = registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry.entries.get(endpoint).map(|registered| {
            (
                registered.owner.clone(),
                registered.generation.clone(),
                registered.generation_id,
            )
        })
    };
    let endpoint_owner = registered.and_then(|(owner, generation, generation_id)| {
        let owner = owner.upgrade()?;
        let generation = generation.upgrade()?;
        (generation.id() == generation_id).then_some(BuildEndpoint { owner, generation })
    });
    if endpoint_owner.is_none() {
        let mut registry = registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registry
            .entries
            .get(endpoint)
            .is_some_and(|registered| registered.owner.strong_count() == 0)
        {
            registry.entries.remove(endpoint);
        }
    }
    endpoint_owner
}

/// Process-wide fail-fast serialization for one native group.
pub(crate) struct BuildGroupPermit {
    group: String,
}

struct GroupRegistry {
    active: BTreeSet<String>,
    blocked: BTreeSet<String>,
}

/// Source-side account owner that survives cancellation of the async callback
/// future. Target containment is owned by the separately stacked target guard.
pub(crate) struct BuildSourceCleanup {
    source: Weak<MysqlPublicV1Replicator>,
    account: Option<BuildRecoveryAccountReceipt>,
    build: BuildAttemptContext,
    predecessor_members: usize,
    source_generation: Arc<BuildGeneration>,
    callback: Option<BuildCallbackKey>,
    donors: Vec<(Weak<MysqlPublicV1Replicator>, Arc<BuildGeneration>)>,
    group: crate::service::BuildAttemptGroup,
    permit: Option<BuildGroupPermit>,
    native_effect: bool,
    armed: bool,
}

impl BuildSourceCleanup {
    pub(crate) fn new(
        source: Weak<MysqlPublicV1Replicator>,
        build: BuildAttemptContext,
        predecessor_members: usize,
        source_generation: Arc<BuildGeneration>,
        callback: BuildCallbackKey,
        donors: Vec<(Weak<MysqlPublicV1Replicator>, Arc<BuildGeneration>)>,
        permit: BuildGroupPermit,
    ) -> Self {
        let group = build.group();
        Self {
            source,
            account: None,
            build,
            predecessor_members,
            source_generation,
            callback: Some(callback),
            donors,
            group,
            permit: Some(permit),
            native_effect: false,
            armed: true,
        }
    }

    pub(crate) fn set_account(&mut self, account: BuildRecoveryAccountReceipt) {
        self.account = Some(account);
    }

    pub(crate) fn account(&self) -> Option<&BuildRecoveryAccountReceipt> {
        self.account.as_ref()
    }

    pub(crate) fn clear_account(&mut self) {
        self.account = None;
    }

    pub(crate) fn mark_native_effect(&mut self) {
        self.native_effect = true;
    }

    pub(crate) fn block_for_reset(&mut self) {
        self.source_generation
            .set(BuildGenerationStatus::CleanupOnly);
        if let Some(permit) = self.permit.as_mut() {
            permit.block_for_reset();
        }
    }

    pub(crate) fn disarm(mut self) {
        self.armed = false;
    }

    #[cfg(test)]
    fn for_test(
        build: BuildAttemptContext,
        source_generation: Arc<BuildGeneration>,
        donor_generation: Arc<BuildGeneration>,
        permit: BuildGroupPermit,
    ) -> Self {
        Self {
            source: Weak::new(),
            account: None,
            build: build.clone(),
            predecessor_members: 2,
            source_generation,
            callback: None,
            donors: vec![(Weak::new(), donor_generation)],
            group: build.group(),
            permit: Some(permit),
            native_effect: false,
            armed: true,
        }
    }
}

impl Drop for BuildSourceCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.source_generation
            .set(BuildGenerationStatus::CleanupOnly);
        if self.native_effect {
            for (_, generation) in &self.donors {
                generation.set(BuildGenerationStatus::CleanupOnly);
            }
        }
        let Some(account) = self.account.take() else {
            if self.native_effect {
                if let Some(permit) = self.permit.as_mut() {
                    permit.block_for_reset();
                }
            } else {
                self.source_generation.set(BuildGenerationStatus::Available);
            }
            return;
        };
        // One surviving member in a two-member view cannot safely commit the
        // replicated DROP. The fixture remains blocked for reset.
        if self.predecessor_members < 2 {
            if let Some(permit) = self.permit.as_mut() {
                permit.block_for_reset();
            }
            return;
        }
        let Some(source) = self.source.upgrade() else {
            if let Some(permit) = self.permit.as_mut() {
                permit.block_for_reset();
            }
            return;
        };
        let build = self.build.clone();
        let source_generation = self.source_generation.clone();
        let donors = self.donors.clone();
        let callback = self.callback.clone();
        let group = self.group.clone();
        let native_effect = self.native_effect;
        if let Some(permit) = self.permit.as_mut() {
            permit.block_for_reset();
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let permit = self.permit.take();
        runtime.spawn(async move {
            let mut permit = permit;
            let result = source
                .cleanup_build_account_after_drop(&account, &build)
                .await;
            let mut result = result;
            if result.is_ok() {
                for donor in donors.into_iter().filter_map(|(donor, _)| donor.upgrade()) {
                    let Some(callback) = callback.as_ref() else {
                        result = Err(RuntimeError::InvalidReplication(
                            "Build cleanup lost its callback binding".into(),
                        ));
                        break;
                    };
                    if let Err(error) = donor
                        .verify_direct_build_account(&account, false, &group, callback)
                        .await
                    {
                        result = Err(error);
                        break;
                    }
                }
            }
            if result.is_ok() && !native_effect {
                if let Some(permit) = permit.as_mut() {
                    permit.clear_reset_block();
                }
                source_generation.set(BuildGenerationStatus::Available);
            } else if let Some(permit) = permit.as_mut() {
                permit.block_for_reset();
            }
        });
    }
}

impl BuildGroupPermit {
    pub(crate) fn try_acquire(group: impl Into<String>) -> Option<Self> {
        let group = group.into();
        let mut groups = group_registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if groups.blocked.contains(&group) || !groups.active.insert(group.clone()) {
            None
        } else {
            Some(Self { group })
        }
    }

    pub(crate) fn block_for_reset(&mut self) {
        group_registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .blocked
            .insert(self.group.clone());
    }

    pub(crate) fn clear_reset_block(&mut self) {
        group_registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .blocked
            .remove(&self.group);
    }
}

impl Drop for BuildGroupPermit {
    fn drop(&mut self) {
        group_registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
            .remove(&self.group);
    }
}

fn group_registry() -> &'static Mutex<GroupRegistry> {
    static GROUPS: OnceLock<Mutex<GroupRegistry>> = OnceLock::new();
    GROUPS.get_or_init(|| {
        Mutex::new(GroupRegistry {
            active: BTreeSet::new(),
            blocked: BTreeSet::new(),
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    use crate::service::AttemptContext;

    #[test]
    fn generation_is_fail_closed_after_reservation() {
        let generation = BuildGeneration::new();
        assert_ne!(generation.id(), BuildGeneration::new().id());
        assert!(generation.reserve());
        assert!(!generation.reserve());
        generation.set(BuildGenerationStatus::NeedsRebuild);
        assert_eq!(generation.status(), BuildGenerationStatus::NeedsRebuild);
    }

    #[test]
    fn orchestration_checkpoints_classify_every_interruption_cut() {
        let checkpoints = [
            BuildCheckpoint::TargetFresh,
            BuildCheckpoint::AccountCleanupOwned,
            BuildCheckpoint::AccountPresent,
            BuildCheckpoint::DonorsPrepared,
            BuildCheckpoint::SourceRevalidated,
            BuildCheckpoint::TargetJoined,
            BuildCheckpoint::SuccessorsObserved,
            BuildCheckpoint::CleanupOnly,
            BuildCheckpoint::AccountAbsent,
            BuildCheckpoint::ProgressRefreshed,
            BuildCheckpoint::FinalRevalidated,
            BuildCheckpoint::Completed,
        ];
        for (cut, checkpoint) in checkpoints.into_iter().enumerate() {
            let mut sequence = test_orchestration(&format!("checkpoint-{cut}"));
            for admitted in checkpoints.into_iter().take(cut + 1) {
                sequence.advance(admitted).unwrap();
            }
            assert_eq!(
                sequence.account_cleanup_owned(),
                (BuildCheckpoint::AccountCleanupOwned as u8..BuildCheckpoint::AccountAbsent as u8)
                    .contains(&(checkpoint as u8))
            );
            assert_eq!(
                sequence.interruption_requires_reset(),
                checkpoint as u8 >= BuildCheckpoint::TargetJoined as u8
            );
        }
    }

    #[test]
    fn orchestration_rejects_skipped_or_reordered_effects() {
        let mut sequence = test_orchestration("reordered");
        assert!(
            sequence
                .advance(BuildCheckpoint::AccountCleanupOwned)
                .is_err()
        );
        sequence.advance(BuildCheckpoint::TargetFresh).unwrap();
        sequence
            .advance(BuildCheckpoint::AccountCleanupOwned)
            .unwrap();
        assert!(
            sequence
                .advance(BuildCheckpoint::AccountCleanupOwned)
                .is_err()
        );
        assert!(sequence.advance(BuildCheckpoint::DonorsPrepared).is_err());
    }

    fn test_orchestration(name: &str) -> BuildOrchestration {
        BuildOrchestration::new(
            BuildAttemptGroup::new(
                AttemptId::new(format!("{name}-build")).unwrap(),
                Instant::now() + Duration::from_secs(10),
                Duration::from_secs(2),
            )
            .unwrap(),
        )
    }

    #[tokio::test(flavor = "current_thread")]
    async fn production_coordinator_bounds_scripted_work_and_cleanup_effects() {
        let group = BuildAttemptGroup::new(
            AttemptId::new("scripted-deadline-build").unwrap(),
            Instant::now() + Duration::from_millis(80),
            Duration::from_millis(30),
        )
        .unwrap();
        let coordinator = BuildOrchestration::new(group);
        assert!(
            coordinator
                .run_work(tokio::time::sleep(Duration::from_millis(100)))
                .await
                .is_err()
        );
        assert!(
            coordinator
                .run_cleanup(tokio::time::sleep(Duration::from_millis(100)))
                .await
                .is_err()
        );
    }

    fn cleanup_test_child(name: &str) -> BuildAttemptContext {
        let now = Instant::now();
        let parent = AttemptContext::new_at(
            AttemptId::new(format!("{name}-parent")).unwrap(),
            now,
            now + Duration::from_secs(30),
            Duration::from_secs(5),
        )
        .unwrap();
        BuildAttemptContext::new(
            parent,
            AttemptId::new(format!("{name}-build")).unwrap(),
            now + Duration::from_secs(20),
            Duration::from_secs(5),
        )
        .unwrap()
    }

    #[test]
    fn production_cleanup_drop_releases_pre_effect_cut_without_reset() {
        let group = "cleanup-pre-effect-cut";
        let source = Arc::new(BuildGeneration::new());
        let donor = Arc::new(BuildGeneration::new());
        let owner = BuildSourceCleanup::for_test(
            cleanup_test_child(group),
            source.clone(),
            donor,
            BuildGroupPermit::try_acquire(group).unwrap(),
        );
        drop(owner);
        assert_eq!(source.status(), BuildGenerationStatus::Available);
        assert!(BuildGroupPermit::try_acquire(group).is_some());
    }

    #[test]
    fn production_cleanup_drop_synchronously_quarantines_native_effect_cut() {
        let group = "cleanup-native-effect-cut";
        let source = Arc::new(BuildGeneration::new());
        let donor = Arc::new(BuildGeneration::new());
        let mut owner = BuildSourceCleanup::for_test(
            cleanup_test_child(group),
            source.clone(),
            donor.clone(),
            BuildGroupPermit::try_acquire(group).unwrap(),
        );
        owner.mark_native_effect();
        drop(owner);
        assert_eq!(source.status(), BuildGenerationStatus::CleanupOnly);
        assert_eq!(donor.status(), BuildGenerationStatus::CleanupOnly);
        assert!(BuildGroupPermit::try_acquire(group).is_none());
    }

    #[test]
    fn group_permit_is_fail_fast_and_released_on_drop() {
        let first = BuildGroupPermit::try_acquire("phase-one-test").unwrap();
        assert!(BuildGroupPermit::try_acquire("phase-one-test").is_none());
        drop(first);
        assert!(BuildGroupPermit::try_acquire("phase-one-test").is_some());
    }

    #[test]
    fn blocked_group_requires_process_reset() {
        let group = "phase-reset-test";
        let mut permit = BuildGroupPermit::try_acquire(group).unwrap();
        permit.block_for_reset();
        drop(permit);
        assert!(BuildGroupPermit::try_acquire(group).is_none());
    }

    #[test]
    fn secret_debug_is_redacted() {
        let secret = BuildSecret::new(vec![1_u8, 2, 3]).unwrap();
        assert_eq!(format!("{secret:?}"), "BuildSecret([REDACTED])");
        assert_eq!(secret.expose(), &[1, 2, 3]);
    }

    #[test]
    fn recovery_credential_is_build_bound_and_redacted() {
        let credential = BuildRecoveryCredential::new(
            AttemptId::new("build-credential").unwrap(),
            CredentialGeneration::new("generation").unwrap(),
            "build_recovery_1",
            b"recovery-secret".to_vec(),
        )
        .unwrap();
        assert_eq!(credential.build().as_str(), "build-credential");
        assert_eq!(credential.password(), "recovery-secret");
        assert!(!format!("{credential:?}").contains("recovery-secret"));
        let receipt = BuildRecoveryAccountReceipt::new(&credential);
        assert_eq!(receipt.username(), "build_recovery_1");
        assert_eq!(receipt.generation(), credential.generation());
    }

    #[test]
    fn recovery_account_marker_hex_encodes_opaque_sql_metacharacters() {
        let credential = BuildRecoveryCredential::new(
            AttemptId::new("build-'; DROP USER root; --").unwrap(),
            CredentialGeneration::new("generation'quoted").unwrap(),
            "build_recovery_safe",
            b"recovery-secret".to_vec(),
        )
        .unwrap();
        let receipt = BuildRecoveryAccountReceipt::new(&credential);
        assert!(receipt.marker_is_well_formed());
        assert!(!receipt.marker().contains('\''));
        assert!(!receipt.marker().contains(';'));
        assert!(!receipt.marker().contains(' '));
    }

    #[test]
    fn result_ledger_reuses_exact_binding_and_rejects_conflicts() {
        let mut ledger = BuildResultLedger::<String>::default();
        assert_eq!(ledger.admit("build", &"exact".into()).unwrap(), None);
        ledger
            .complete("build", &"exact".into(), BuildTerminalResult::Succeeded)
            .unwrap();
        assert_eq!(
            ledger.admit("build", &"exact".into()).unwrap(),
            Some(BuildTerminalResult::Succeeded)
        );
        assert!(matches!(
            ledger.admit("build", &"changed".into()),
            Err(MysqlReplicatorError::OperationConflict)
        ));
    }

    #[test]
    fn endpoint_registry_rejects_live_replacement_and_unregisters_exact_generation() {
        let endpoint = "registry-phase-one-test".to_owned();
        let first_generation = Arc::new(BuildGeneration::new());
        let first = BuildEndpointRegistration::register_weak(
            endpoint.clone(),
            Weak::new(),
            &first_generation,
        )
        .unwrap();
        let second_generation = Arc::new(BuildGeneration::new());
        assert!(matches!(
            BuildEndpointRegistration::register_weak(
                endpoint.clone(),
                Weak::new(),
                &second_generation,
            ),
            Err(MysqlReplicatorError::IdentityMismatch)
        ));
        drop(first);
        assert!(
            BuildEndpointRegistration::register_weak(endpoint, Weak::new(), &second_generation,)
                .is_ok()
        );
    }

    #[test]
    fn idle_preparation_accepts_both_orders_but_rejects_changed_epoch() {
        let epoch = Epoch::new(2, 3);
        let mut role_first = IdleBuildPreparation::default();
        role_first.admit_role(epoch).unwrap();
        role_first.validate_epoch(epoch).unwrap();
        assert_eq!(role_first.epoch(), Some(epoch));

        let mut epoch_first = IdleBuildPreparation::default();
        epoch_first.observe_epoch(epoch).unwrap();
        epoch_first.admit_role(epoch).unwrap();
        assert!(matches!(
            epoch_first.validate_epoch(Epoch::new(2, 4)),
            Err(MysqlReplicatorError::OperationConflict)
        ));
    }
}
