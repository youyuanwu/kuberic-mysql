//! Closed-only process reopening with a linear, in-memory graceful-stop owner.
//!
//! This is not controller recovery. Generic authority is supplied by a trusted
//! embedding; it is neither discovered in MySQL nor authenticated by this API.
//! Losing the stopped owner, abrupt shutdown and survivor adoption are unsupported.

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use kuberic_runtime::protocol::types::{
    ConfigurationId, Epoch, HistoryContext, OperationId, PartitionId, ProcessSessionId,
    ReplicaStorageIncarnationContext, ReplicatorCreationId,
};

use crate::core::{AttemptId, GtidSet, ServerUuid};
use crate::service::{
    AttemptAdmissionError, AttemptContext, MysqlInstanceError, MysqlInstanceManager,
    MysqlTopologyError,
};

/// Caller-owned generic authority for a closed observation, not a serving grant.
///
/// These values must come from the embedding's accepted generic Kuberic facts.
/// Validating their shape/equality does not authenticate a caller or durably
/// install a session renewal in Kuberic. No field restores a native role.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlReopenAuthority {
    /// Exact public replica and storage incarnation.
    pub storage: ReplicaStorageIncarnationContext,
    /// Public partition scope.
    pub partition: PartitionId,
    /// Accepted generic history scope.
    pub history: HistoryContext,
    /// Current generic process session, distinct from the native child fence.
    pub process_session: ProcessSessionId,
    /// Exact generic replicator creation.
    pub replicator_creation: ReplicatorCreationId,
    /// Accepted epoch; never inferred from retained data.
    pub epoch: Epoch,
    /// Accepted configuration scope; not installed as a native configuration.
    pub configuration: ConfigurationId,
    /// Generic operation being qualified.
    pub operation: OperationId,
    /// Accepted generic operation revision.
    pub revision: u64,
}

impl MysqlReopenAuthority {
    fn validate(&self) -> Result<(), MysqlReopenError> {
        if self.storage.validate().is_err()
            || self.history.validate().is_err()
            || self.partition.is_empty()
            || self.process_session.is_empty()
            || self.replicator_creation.is_empty()
            || self.configuration.is_empty()
            || self.operation.is_empty()
            || self.revision == 0
            || self.epoch.data_loss_number != self.history.data_loss_number
            || self.epoch.configuration_number < 0
        {
            return Err(MysqlReopenError::Authority);
        }
        Ok(())
    }
}

/// Exact expected native boundary, supplied independently of the read being
/// qualified. Full structured sets are compared, never scalar tails or digests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlReopenBoundary {
    /// Native data's server UUID.
    pub server_uuid: ServerUuid,
    /// Native configured Group Replication UUID.
    pub group_name: String,
    /// Configured server ID.
    pub server_id: u32,
    /// Configured report address (not an exposed listener).
    pub sql_address: std::net::SocketAddr,
    /// Configured Group Replication address (group remains stopped).
    pub group_address: String,
    /// Complete executed transaction set.
    pub executed: GtidSet,
    /// Complete purged transaction set.
    pub purged: GtidSet,
}

impl MysqlReopenBoundary {
    pub(crate) fn validate(&self) -> Result<(), MysqlReopenError> {
        if !self.purged.is_subset_of(&self.executed) {
            return Err(MysqlReopenError::History);
        }
        Ok(())
    }

    fn matches(&self, observed: &Self) -> Result<(), MysqlReopenError> {
        self.validate()?;
        observed.validate()?;
        if self.server_uuid != observed.server_uuid
            || self.group_name != observed.group_name
            || self.server_id != observed.server_id
            || self.sql_address != observed.sql_address
            || self.group_address != observed.group_address
        {
            return Err(MysqlReopenError::NativeIdentity);
        }
        if self.executed != observed.executed || self.purged != observed.purged {
            return Err(MysqlReopenError::History);
        }
        Ok(())
    }
}

/// Explicit exact-predecessor/new-session authority, consumed only while closed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlReopenRenewal {
    /// Must equal the entire sealed predecessor scope.
    pub predecessor: MysqlReopenAuthority,
    /// Same storage/history; newer same-data-loss epoch and fresh session,
    /// creation, configuration, operation and revision.
    pub replacement: MysqlReopenAuthority,
}

impl MysqlReopenRenewal {
    fn validate(&self, sealed: &MysqlReopenAuthority) -> Result<(), MysqlReopenError> {
        self.predecessor.validate()?;
        self.replacement.validate()?;
        let old = &self.predecessor;
        let new = &self.replacement;
        if old != sealed
            || new.storage != old.storage
            || new.partition != old.partition
            || new.history != old.history
            || new.epoch.data_loss_number != old.epoch.data_loss_number
            || new.epoch.configuration_number <= old.epoch.configuration_number
            || new.process_session == old.process_session
            || new.replicator_creation == old.replicator_creation
            || new.configuration == old.configuration
            || new.operation == old.operation
            || new.revision <= old.revision
        {
            return Err(MysqlReopenError::Authority);
        }
        Ok(())
    }
}

/// Facts for one closed observation. This is not a reusable serving receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MysqlReopenEvidence {
    authority: MysqlReopenAuthority,
    boundary: MysqlReopenBoundary,
    native_process_session: String,
    native_storage: String,
    attempt: AttemptId,
}

impl MysqlReopenEvidence {
    /// Exact generic caller scope under which this observation was qualified.
    pub const fn authority(&self) -> &MysqlReopenAuthority {
        &self.authority
    }
    /// Complete native identity and structured history.
    pub const fn boundary(&self) -> &MysqlReopenBoundary {
        &self.boundary
    }
    /// Independently derived owned native-child fence, not caller-selected.
    pub fn native_process_session(&self) -> &str {
        &self.native_process_session
    }
    /// Exact device/inode binding, separate from generic storage generation.
    pub fn native_storage(&self) -> &str {
        &self.native_storage
    }
    /// Local observation attempt, independent of generic operation authority.
    pub const fn attempt(&self) -> &AttemptId {
        &self.attempt
    }
}

/// Noncloneable, nonserializable, one-use proof of an exactly owned graceful
/// stop. Native data AND scratch logs stay intact. Drop never adopts a survivor.
///
/// Only quiet, closed, inactive topology-member managers which have never
/// issued an application/proxy capability are admitted. Idle, unpublished or
/// previously revoked proxies do not supply an exact closed-owner handoff.
/// Ordinary `MysqlInstanceManager::stop` does not produce this capability.
pub struct MysqlStoppedReopen {
    manager: MysqlInstanceManager,
    sealed: MysqlReopenEvidence,
    logs: NativeLogs,
}

impl MysqlStoppedReopen {
    /// Consumes an owned manager, qualifies the caller's expected boundary and
    /// effective closed state, then gracefully stops/reaps exactly that child.
    ///
    /// No success is returned after escalation, unknown historical durability,
    /// cancellation or lost ownership. On error/drop, the retained child owner
    /// contains only its own process; existing data is never deleted.
    pub async fn capture(
        mut manager: MysqlInstanceManager,
        authority: MysqlReopenAuthority,
        expected: MysqlReopenBoundary,
        attempt: &AttemptContext,
    ) -> Result<Self, MysqlReopenError> {
        authority.validate()?;
        expected.validate()?;
        admit(attempt)?;
        let observed = read_closed(&mut manager, attempt, false).await?;
        expected.matches(&observed)?;
        let sealed = evidence(&mut manager, authority, observed, attempt)?;
        manager.stop_for_reopen(attempt)?;
        admit(attempt)?;
        manager.revalidate_roots()?;
        let logs = NativeLogs::capture(manager.config().runtime().scratch_root())?;
        Ok(Self {
            manager,
            sealed,
            logs,
        })
    }

    /// The fully bound pre-stop observation, not recovered role/access.
    pub const fn predecessor(&self) -> &MysqlReopenEvidence {
        &self.sealed
    }

    /// Starts a new exact child on retained storage, validates fresh native
    /// evidence and returns only a closed owner. Never initializes existing data.
    pub async fn reopen(
        mut self,
        renewal: MysqlReopenRenewal,
        attempt: &AttemptContext,
    ) -> Result<MysqlClosedReopened, MysqlReopenError> {
        admit(attempt)?;
        renewal.validate(&self.sealed.authority)?;
        self.manager.revalidate_roots()?;
        self.logs
            .verify(self.manager.config().runtime().scratch_root())?;
        self.manager.start_for_reopen(attempt)?;
        let observed = read_closed(&mut self.manager, attempt, true).await?;
        self.sealed.boundary.matches(&observed)?;
        let current = evidence(&mut self.manager, renewal.replacement, observed, attempt)?;
        if current.native_process_session == self.sealed.native_process_session
            || current.native_storage != self.sealed.native_storage
        {
            return Err(MysqlReopenError::Ownership);
        }
        admit(attempt)?;
        Ok(MysqlClosedReopened {
            manager: self.manager,
            current,
        })
    }
}

/// Sole owner of a newly attested, closed reopened child.
///
/// Deliberately no manager extraction, socket accessor, engine-open conversion,
/// role/configuration/Build/access callback or progress publication API.
/// Dropping this handle contains the exact retained child via its manager.
pub struct MysqlClosedReopened {
    manager: MysqlInstanceManager,
    current: MysqlReopenEvidence,
}

impl MysqlClosedReopened {
    /// Re-read effective closure and native facts under this exact new scope.
    /// Historical snapshots grant no later readiness or serving rights.
    pub async fn observe(
        &mut self,
        attempt: &AttemptContext,
    ) -> Result<MysqlReopenEvidence, MysqlReopenError> {
        let observed = read_closed(&mut self.manager, attempt, true).await?;
        self.current.boundary.matches(&observed)?;
        let observed = evidence(
            &mut self.manager,
            self.current.authority.clone(),
            observed,
            attempt,
        )?;
        if observed.native_process_session != self.current.native_process_session
            || observed.native_storage != self.current.native_storage
        {
            return Err(MysqlReopenError::Ownership);
        }
        self.current = observed.clone();
        Ok(observed)
    }

    /// Stops the exact reopened child and removes disposable scratch, retaining
    /// data. This terminal close does not issue another reopen capability.
    pub fn close(mut self) -> Result<(), MysqlReopenError> {
        self.manager.contain()?;
        Ok(())
    }
}

/// Machine-matchable closed-reopen rejection. No error grants reconstruction.
#[derive(Debug)]
pub enum MysqlReopenError {
    /// Invalid, stale or substituted generic authority.
    Authority,
    /// Wrong native UUID/topology/server/address.
    NativeIdentity,
    /// Changed/inconsistent complete executed or purged history.
    History,
    /// An effective client path, group state or other client session is active.
    AccessNotClosed,
    /// Lifetime durability is unqualified, even if current settings are valid.
    Durability,
    /// Exact native-log files are missing or substituted.
    NativeLogs,
    /// Root/child/session ownership cannot be proven.
    Ownership,
    /// Attempt cancellation/deadline admission failed.
    Attempt(AttemptAdmissionError),
    /// Owned process lifecycle failure.
    Instance(MysqlInstanceError),
    /// Private-UDS native read failure.
    Native(MysqlTopologyError),
}

impl std::fmt::Display for MysqlReopenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "closed MySQL reopen rejected: {self:?}")
    }
}
impl std::error::Error for MysqlReopenError {}
impl From<MysqlInstanceError> for MysqlReopenError {
    fn from(error: MysqlInstanceError) -> Self {
        Self::Instance(error)
    }
}
impl From<MysqlTopologyError> for MysqlReopenError {
    fn from(error: MysqlTopologyError) -> Self {
        Self::Native(error)
    }
}

fn admit(attempt: &AttemptContext) -> Result<(), MysqlReopenError> {
    attempt.admit_work().map_err(MysqlReopenError::Attempt)
}

async fn read_closed(
    manager: &mut MysqlInstanceManager,
    attempt: &AttemptContext,
    reopened: bool,
) -> Result<MysqlReopenBoundary, MysqlReopenError> {
    admit(attempt)?;
    if manager.config().topology().is_none()
        || manager.config().build_recovery_networking()
        || manager.application_capability_issued()
    {
        return Err(MysqlReopenError::AccessNotClosed);
    }
    if !manager.native_durability_qualified() {
        return Err(MysqlReopenError::Durability);
    }
    let target = manager.prepare_control_target()?;
    let mut cancellation = attempt.cancellation_receiver();
    let boundary = tokio::select! {
        biased;
        _ = cancellation.wait_for(|cancelled| *cancelled) => {
            return Err(MysqlReopenError::Attempt(AttemptAdmissionError::Cancelled));
        }
        boundary = crate::service::control::read_closed_reopen(
            target, attempt.work_deadline(), reopened,
        ) => boundary?,
    };
    admit(attempt)?;
    // Attest both sides of native IO; a successful SQL reply is not ownership.
    manager.prepare_control_target()?;
    Ok(boundary)
}

fn evidence(
    manager: &mut MysqlInstanceManager,
    authority: MysqlReopenAuthority,
    boundary: MysqlReopenBoundary,
    attempt: &AttemptContext,
) -> Result<MysqlReopenEvidence, MysqlReopenError> {
    let native_process_session = manager.reopen_process_session()?;
    let metadata = fs::symlink_metadata(manager.config().data_root())
        .map_err(|_| MysqlReopenError::Ownership)?;
    Ok(MysqlReopenEvidence {
        authority,
        boundary,
        native_process_session,
        native_storage: format!("data:{}:{}", metadata.dev(), metadata.ino()),
        attempt: attempt.attempt().clone(),
    })
}

/// Native log identity inventory only; this is never persisted as a manifest.
struct NativeLogs(BTreeMap<String, (u64, u64, u64)>);

impl NativeLogs {
    fn capture(root: &Path) -> Result<Self, MysqlReopenError> {
        let mut files = BTreeMap::new();
        for entry in fs::read_dir(root).map_err(|_| MysqlReopenError::NativeLogs)? {
            let entry = entry.map_err(|_| MysqlReopenError::NativeLogs)?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| MysqlReopenError::NativeLogs)?;
            if name.starts_with("mysql-bin.") || name.starts_with("relay-bin.") {
                let metadata =
                    fs::symlink_metadata(entry.path()).map_err(|_| MysqlReopenError::NativeLogs)?;
                if !metadata.is_file() || metadata.file_type().is_symlink() {
                    return Err(MysqlReopenError::NativeLogs);
                }
                files.insert(name, (metadata.dev(), metadata.ino(), metadata.len()));
            }
        }
        if !files.contains_key("mysql-bin.index")
            || !files.keys().any(|name| name.starts_with("mysql-bin.0"))
        {
            return Err(MysqlReopenError::NativeLogs);
        }
        Ok(Self(files))
    }
    fn verify(&self, root: &Path) -> Result<(), MysqlReopenError> {
        if Self::capture(root)?.0 == self.0 {
            Ok(())
        } else {
            Err(MysqlReopenError::NativeLogs)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kuberic_runtime::protocol::types::{
        AgentGeneration, HistoryId, PodUid, PublicProtocolGeneration, PvcUid, ReplicaId,
        ReplicaIdentity, ReplicaInstanceId, ReplicaStorageIdentity, ResourceUid, StorageGeneration,
    };

    fn authority() -> MysqlReopenAuthority {
        MysqlReopenAuthority {
            storage: ReplicaStorageIncarnationContext {
                replica: ReplicaIdentity {
                    replica_id: ReplicaId::new(1),
                    instance_id: ReplicaInstanceId::new("instance"),
                    agent_generation: AgentGeneration::new("agent"),
                },
                storage: ReplicaStorageIdentity::Persistent {
                    pod_uid: PodUid::new("pod"),
                    pvc_uid: PvcUid::new("pvc"),
                },
                storage_generation: StorageGeneration::new("storage"),
            },
            partition: PartitionId::new("partition"),
            history: HistoryContext {
                resource_uid: ResourceUid::new("resource"),
                protocol_generation: PublicProtocolGeneration::new(1),
                history_id: HistoryId::new("history"),
                data_loss_number: 0,
            },
            process_session: ProcessSessionId::new("old"),
            replicator_creation: ReplicatorCreationId::new("old-creation"),
            epoch: Epoch::new(0, 1),
            configuration: ConfigurationId::new("old-config"),
            operation: OperationId::new("old-op"),
            revision: 1,
        }
    }

    fn renewal() -> MysqlReopenRenewal {
        let predecessor = authority();
        let mut replacement = predecessor.clone();
        replacement.process_session = ProcessSessionId::new("new");
        replacement.replicator_creation = ReplicatorCreationId::new("new-creation");
        replacement.epoch = Epoch::new(0, 2);
        replacement.configuration = ConfigurationId::new("new-config");
        replacement.operation = OperationId::new("new-op");
        replacement.revision = 2;
        MysqlReopenRenewal {
            predecessor,
            replacement,
        }
    }

    #[test]
    fn reopen_renewal_rejects_each_stale_or_substituted_authority_dimension() {
        let good = renewal();
        good.validate(&authority()).unwrap();
        let mutations: Vec<fn(&mut MysqlReopenAuthority)> = vec![
            |a| a.storage.replica.replica_id = ReplicaId::new(2),
            |a| a.storage.replica.instance_id = ReplicaInstanceId::new("other"),
            |a| a.storage.replica.agent_generation = AgentGeneration::new("other"),
            |a| a.storage.storage_generation = StorageGeneration::new("other"),
            |a| {
                a.storage.storage = ReplicaStorageIdentity::Volatile {
                    identity: "other".into(),
                }
            },
            |a| a.partition = PartitionId::new("other"),
            |a| a.history.resource_uid = ResourceUid::new("other"),
            |a| a.history.protocol_generation = PublicProtocolGeneration::new(2),
            |a| a.history.history_id = HistoryId::new("other"),
            |a| {
                a.history.data_loss_number = 1;
                a.epoch.data_loss_number = 1;
            },
            |a| a.process_session = ProcessSessionId::new("old"),
            |a| a.replicator_creation = ReplicatorCreationId::new("old-creation"),
            |a| a.epoch = Epoch::new(0, 1),
            |a| a.configuration = ConfigurationId::new("old-config"),
            |a| a.operation = OperationId::new("old-op"),
            |a| a.revision = 1,
        ];
        for mutate in mutations {
            let mut bad = good.clone();
            mutate(&mut bad.replacement);
            assert!(matches!(
                bad.validate(&authority()),
                Err(MysqlReopenError::Authority)
            ));
        }
        // Exact predecessor binding includes every field even if its shape is valid.
        let mutations: Vec<fn(&mut MysqlReopenAuthority)> = vec![
            |a| a.process_session = ProcessSessionId::new("unrelated"),
            |a| a.configuration = ConfigurationId::new("unrelated"),
            |a| a.operation = OperationId::new("unrelated"),
            |a| a.epoch = Epoch::new(0, 0),
            |a| a.revision = 9,
        ];
        for mutate in mutations {
            let mut bad = good.clone();
            mutate(&mut bad.predecessor);
            assert!(bad.validate(&authority()).is_err());
        }
    }

    #[test]
    fn reopen_authority_rejects_incomplete_and_cross_history_epoch_values() {
        let mutations: Vec<fn(&mut MysqlReopenAuthority)> = vec![
            |a| a.storage.replica.replica_id = ReplicaId::new(0),
            |a| a.storage.storage_generation = StorageGeneration::new(""),
            |a| a.partition = PartitionId::new(""),
            |a| a.history.resource_uid = ResourceUid::new(""),
            |a| a.history.protocol_generation = PublicProtocolGeneration::new(0),
            |a| a.history.history_id = HistoryId::new(""),
            |a| a.history.data_loss_number = -1,
            |a| a.process_session = ProcessSessionId::new(""),
            |a| a.replicator_creation = ReplicatorCreationId::new(""),
            |a| a.epoch = Epoch::new(1, 2),
            |a| a.epoch = Epoch::new(0, -1),
            |a| a.configuration = ConfigurationId::new(""),
            |a| a.operation = OperationId::new(""),
            |a| a.revision = 0,
        ];
        for mutate in mutations {
            let mut bad = authority();
            mutate(&mut bad);
            assert!(bad.validate().is_err());
        }
    }

    fn boundary(history: &str, purged: &str) -> MysqlReopenBoundary {
        MysqlReopenBoundary {
            server_uuid: ServerUuid::new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
            group_name: "cccccccc-cccc-cccc-cccc-cccccccccccc".into(),
            server_id: 1,
            sql_address: "127.0.0.1:33061".parse().unwrap(),
            group_address: "127.0.0.1:43061".into(),
            executed: history.parse().unwrap(),
            purged: purged.parse().unwrap(),
        }
    }

    #[test]
    fn reopen_history_keeps_tags_gaps_sources_and_full_purged_semantics() {
        const A: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
        const B: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
        for history in [
            String::new(),
            format!("{A}:1-7"),
            format!("{A}:1-3:7"),
            format!("{A}:tag:1-7"),
            format!("{B}:1-7"),
            format!("{A}:1-7,{B}:1-7"),
        ] {
            let expected = boundary(&history, "");
            expected.matches(&expected.clone()).unwrap();
            for changed in [
                format!("{A}:1-6"),
                format!("{A}:1-8"),
                format!("{A}:1-3:7"),
                format!("{A}:tag:1-7"),
                format!("{B}:1-7"),
                format!("{A}:1-7,{B}:1-7"),
            ] {
                if changed != history {
                    assert!(matches!(
                        expected.matches(&boundary(&changed, "")),
                        Err(MysqlReopenError::History),
                    ));
                }
            }
        }
        assert!(
            boundary(&format!("{A}:1-7"), &format!("{B}:1"))
                .validate()
                .is_err()
        );
        assert!(
            boundary(&format!("{A}:1-7"), &format!("{A}:1-8"))
                .validate()
                .is_err()
        );
        let retained = boundary(&format!("{A}:1-7"), &format!("{A}:1-3"));
        assert!(
            retained
                .matches(&boundary(&format!("{A}:1-7"), ""))
                .is_err()
        );
        assert!("not-a-gtid".parse::<GtidSet>().is_err());
        assert!(
            format!("{A}:1-9223372036854775807")
                .parse::<GtidSet>()
                .is_err()
        );
    }

    #[test]
    fn reopen_native_identity_dimensions_are_independent() {
        let expected = boundary("", "");
        let mutations: Vec<fn(&mut MysqlReopenBoundary)> = vec![
            |b| b.server_uuid = ServerUuid::new("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
            |b| b.group_name = "other".into(),
            |b| b.server_id = 2,
            |b| b.sql_address = "127.0.0.1:33062".parse().unwrap(),
            |b| b.group_address = "127.0.0.1:43062".into(),
        ];
        for mutate in mutations {
            let mut bad = expected.clone();
            mutate(&mut bad);
            assert!(matches!(
                expected.matches(&bad),
                Err(MysqlReopenError::NativeIdentity)
            ));
        }
    }

    #[test]
    fn reopen_attempt_admission_rejects_cancelled_reserve_and_expired_work() {
        use std::time::{Duration, Instant};
        let now = Instant::now();
        let cancelled = AttemptContext::new(
            AttemptId::new("cancelled").unwrap(),
            now + Duration::from_secs(10),
            Duration::from_secs(1),
        )
        .unwrap();
        cancelled.cancel();
        assert!(matches!(
            admit(&cancelled),
            Err(MysqlReopenError::Attempt(AttemptAdmissionError::Cancelled))
        ));
        let reserve = AttemptContext::new_at(
            AttemptId::new("reserve").unwrap(),
            now - Duration::from_secs(2),
            now + Duration::from_secs(1),
            Duration::from_secs(2),
        )
        .unwrap();
        assert!(matches!(
            admit(&reserve),
            Err(MysqlReopenError::Attempt(
                AttemptAdmissionError::CleanupReserve
            ))
        ));
        let expired = AttemptContext::new_at(
            AttemptId::new("expired").unwrap(),
            now - Duration::from_secs(3),
            now - Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(matches!(
            admit(&expired),
            Err(MysqlReopenError::Attempt(AttemptAdmissionError::Expired))
        ));
    }

    #[test]
    fn reopen_graceful_stop_never_credits_signal_death_or_an_exited_child() {
        let mut owned = std::process::Command::new("/usr/bin/sleep")
            .arg("60")
            .spawn()
            .unwrap();
        let mut foreign = std::process::Command::new("/usr/bin/sleep")
            .arg("60")
            .spawn()
            .unwrap();
        let result = crate::service::process::gracefully_reap_before(
            &mut owned,
            std::time::Instant::now() + std::time::Duration::from_secs(2),
        );
        assert!(result.is_err(), "SIGTERM death is not a clean MySQL exit");
        assert!(owned.try_wait().unwrap().is_some());
        assert!(
            crate::service::process::gracefully_reap_before(
                &mut owned,
                std::time::Instant::now() + std::time::Duration::from_secs(2),
            )
            .is_err()
        );
        assert!(foreign.try_wait().unwrap().is_none());
        foreign.kill().unwrap();
        foreign.wait().unwrap();
    }
}
