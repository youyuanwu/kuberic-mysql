#![allow(
    dead_code,
    reason = "Phase 1 Build primitives are wired into execution in later reviewed phases"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use kuberic_runtime::protocol::types::{
    ConfigurationId, Epoch, OperationId, ProcessSessionId, ReplicaIdentity,
    ReplicaStorageIncarnationContext,
};
use zeroize::Zeroize;

use crate::core::AttemptId;
use crate::service::{
    MemberMutationAuthority, MemberSourceBoundary, MysqlReplicaTarget, MysqlReplicatorError,
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
    pub(crate) source: BuildTargetBinding,
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
    RebuildRequired(String),
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum BuildGenerationStatus {
    Available = 0,
    Reserved = 1,
    CleanupPending = 2,
    NeedsRebuild = 3,
    Completed = 4,
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
            2 => BuildGenerationStatus::CleanupPending,
            3 => BuildGenerationStatus::NeedsRebuild,
            4 => BuildGenerationStatus::Completed,
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

impl BuildGroupPermit {
    pub(crate) fn try_acquire(group: impl Into<String>) -> Option<Self> {
        let group = group.into();
        let mut groups = group_registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if groups.insert(group.clone()) {
            Some(Self { group })
        } else {
            None
        }
    }
}

impl Drop for BuildGroupPermit {
    fn drop(&mut self) {
        group_registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.group);
    }
}

fn group_registry() -> &'static Mutex<BTreeSet<String>> {
    static GROUPS: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();
    GROUPS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_is_fail_closed_after_reservation() {
        let generation = BuildGeneration::new();
        assert_ne!(generation.id(), BuildGeneration::new().id());
        assert!(generation.reserve());
        assert!(!generation.reserve());
        generation.set(BuildGenerationStatus::CleanupPending);
        assert_eq!(generation.status(), BuildGenerationStatus::CleanupPending);
        generation.set(BuildGenerationStatus::NeedsRebuild);
        assert_eq!(generation.status(), BuildGenerationStatus::NeedsRebuild);
    }

    #[test]
    fn group_permit_is_fail_fast_and_released_on_drop() {
        let first = BuildGroupPermit::try_acquire("phase-one-test").unwrap();
        assert!(BuildGroupPermit::try_acquire("phase-one-test").is_none());
        drop(first);
        assert!(BuildGroupPermit::try_acquire("phase-one-test").is_some());
    }

    #[test]
    fn secret_debug_is_redacted() {
        let secret = BuildSecret::new(vec![1_u8, 2, 3]).unwrap();
        assert_eq!(format!("{secret:?}"), "BuildSecret([REDACTED])");
        assert_eq!(secret.expose(), &[1, 2, 3]);
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
