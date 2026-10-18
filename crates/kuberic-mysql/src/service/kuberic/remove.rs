use super::super::build::CompletedBuildOwnership;
use super::*;
use crate::core::NativeView;
use crate::service::{BuildAttemptContext, MemberContainmentReceipt, MemberMutationAuthority};
use kuberic_runtime::protocol::types::ReplicaId;

#[derive(Clone)]
struct DonorReceipt {
    generation: u64,
    authority: MemberMutationAuthority,
}

#[derive(Clone)]
struct RemoveReceipt {
    target: MemberContainmentReceipt,
    successor: NativeView,
    donors: Vec<DonorReceipt>,
}

#[derive(Clone)]
pub(super) struct RemoveRecord {
    owned: CompletedBuildOwnership,
    terminal: Option<RemoveReceipt>,
    retained_target: Option<Arc<MysqlPublicV1Replicator>>,
}

impl RemoveRecord {
    pub(super) fn target_id(&self) -> ReplicaId {
        self.owned.binding.target.identity.replica_id
    }
}

struct RemoveOwners<'a> {
    source: DriverStateLease<'a>,
    target: DriverStateLease<'a>,
    donors: Vec<DriverStateLease<'a>>,
    fence: RemoveFence,
    token: crate::service::MysqlBuildAbortGeneration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RemoveCheckpoint {
    Armed,
    TargetLeft,
    SurvivorsProved,
    Completed,
}

struct RemoveFence {
    source: Arc<BuildGeneration>,
    target: Arc<BuildGeneration>,
    donors: Vec<Arc<BuildGeneration>>,
    permit: BuildGroupPermit,
    checkpoint: RemoveCheckpoint,
}

impl RemoveFence {
    fn arm(
        source: Arc<BuildGeneration>,
        target: Arc<BuildGeneration>,
        donors: Vec<Arc<BuildGeneration>>,
        permit: BuildGroupPermit,
    ) -> Self {
        source.set(BuildGenerationStatus::CleanupOnly);
        target.set(BuildGenerationStatus::CleanupOnly);
        for donor in &donors {
            donor.set(BuildGenerationStatus::CleanupOnly);
        }
        Self {
            source,
            target,
            donors,
            permit,
            checkpoint: RemoveCheckpoint::Armed,
        }
    }

    fn advance(&mut self, next: RemoveCheckpoint) -> Result<()> {
        if !matches!(
            (self.checkpoint, next),
            (RemoveCheckpoint::Armed, RemoveCheckpoint::TargetLeft)
                | (
                    RemoveCheckpoint::TargetLeft,
                    RemoveCheckpoint::SurvivorsProved
                )
        ) {
            return Err(rejected("Remove cleanup checkpoints are out of order"));
        }
        self.checkpoint = next;
        Ok(())
    }

    fn finish(
        &mut self,
        receipt: &MemberContainmentReceipt,
        registry_absent: bool,
        children: &[&BuildAttemptContext],
    ) -> Result<()> {
        if self.checkpoint != RemoveCheckpoint::SurvivorsProved
            || !receipt.is_complete()
            || !registry_absent
        {
            return Err(rejected("Remove completion lacks ordered cleanup proof"));
        }
        for child in children {
            child
                .admit_work()
                .map_err(|_| RuntimeError::OperationCancelled)?;
        }
        self.source.set(BuildGenerationStatus::Available);
        self.target.set(BuildGenerationStatus::NeedsRebuild);
        for donor in &self.donors {
            donor.set(BuildGenerationStatus::Available);
        }
        self.checkpoint = RemoveCheckpoint::Completed;
        Ok(())
    }

    fn quarantine(&mut self) {
        self.permit.block_for_reset();
        self.source.set(BuildGenerationStatus::CleanupOnly);
        self.target.set(BuildGenerationStatus::NeedsRebuild);
        for donor in &self.donors {
            donor.set(BuildGenerationStatus::CleanupOnly);
        }
    }
}

impl Drop for RemoveFence {
    fn drop(&mut self) {
        if self.checkpoint != RemoveCheckpoint::Completed {
            self.quarantine();
        }
    }
}

fn failed_remove_cleanup<T>(
    fence: &mut RemoveFence,
    retire: impl FnOnce() -> T,
    unregister: impl FnOnce(),
) {
    fence.quarantine();
    let _ = retire();
    unregister();
}

impl Drop for RemoveOwners<'_> {
    fn drop(&mut self) {
        if self.fence.checkpoint == RemoveCheckpoint::Completed {
            return;
        }
        self.source.committed_sample = None;
        for donor in &mut self.donors {
            donor.committed_sample = None;
        }
        // This receipt is deliberately NOT promoted to Remove success: the
        // survivor view may be unknown even when local containment completes.
        let owner = self.target.owner;
        failed_remove_cleanup(
            &mut self.fence,
            || {
                self.target
                    .engine
                    .force_retire_owned_generation(&self.token)
            },
            || unregister(owner),
        );
    }
}

impl MysqlPublicV1Replicator {
    pub(super) async fn remove_completed_idle(&self, replica_id: ReplicaId) -> Result<()> {
        let permit = self.acquire_proof_permit()?;
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(120))
            .ok_or(RuntimeError::OperationCancelled)?
            .min(self.attempt.deadline());
        let work_deadline = deadline
            .checked_sub(Duration::from_secs(15))
            .ok_or(RuntimeError::OperationCancelled)?;
        bounded_remove_work(
            work_deadline,
            self.remove_owned(replica_id, permit, deadline),
        )
        .await
    }

    async fn remove_owned(
        &self,
        replica_id: ReplicaId,
        permit: BuildGroupPermit,
        deadline: Instant,
    ) -> Result<()> {
        let mut source = self.lease().await;
        require_bound_open(&source)?;
        require_not_cleanup_only(&source)?;
        if let Some(record) = source
            .removals
            .iter()
            .find(|r| r.target_id() == replica_id)
            .cloned()
        {
            let receipt = record
                .terminal
                .clone()
                .ok_or(RuntimeError::ReconfigurationPending)?;
            return self
                .retry_remove(&mut source, &record.owned, &receipt, deadline)
                .await;
        }
        let owned = source
            .completed_builds
            .iter()
            .find(|completed| completed.binding.target.identity.replica_id == replica_id)
            .cloned()
            .ok_or_else(|| rejected("no completed idle Build is owned for this id"))?;
        require_source(&source, &owned, false)?;
        Self::require_native_durability(&mut source)?;
        let target_endpoint = lookup_build_endpoint(&owned.binding.target.replication_address)
            .ok_or_else(|| rejected("the retained target owner is absent"))?;
        if std::ptr::eq(target_endpoint.owner.as_ref(), self) {
            return Err(rejected("target aliases source"));
        }
        let mut target = target_endpoint.owner.lease().await;
        require_target(&target, target_endpoint.owner.as_ref(), &owned)?;
        Self::require_native_durability(&mut target)?;
        let mut donor_endpoints = Vec::new();
        for peer in &owned.callback.source_configuration.replicas {
            let endpoint = lookup_build_endpoint(&peer.replication_address)
                .ok_or_else(|| rejected("an admitted donor owner is absent"))?;
            if std::ptr::eq(endpoint.owner.as_ref(), self)
                || Arc::ptr_eq(&endpoint.owner, &target_endpoint.owner)
            {
                return Err(rejected("donor aliases another participant"));
            }
            donor_endpoints.push(endpoint);
        }
        let mut donors = Vec::new();
        let mut deadline = deadline.min(target_endpoint.owner.attempt.deadline());
        for (peer, endpoint) in owned
            .callback
            .source_configuration
            .replicas
            .iter()
            .zip(&donor_endpoints)
        {
            let donor = endpoint.owner.lease().await;
            require_donor(&donor, endpoint.owner.as_ref(), peer, &owned)?;
            deadline = deadline.min(endpoint.owner.attempt.deadline());
            donors.push(donor);
        }
        let group = removal_group(&owned, deadline)?;
        let source_child = group
            .child(self.attempt.clone())
            .map_err(|_| RuntimeError::OperationCancelled)?;
        let target_child = group
            .child(target_endpoint.owner.attempt.clone())
            .map_err(|_| RuntimeError::OperationCancelled)?;
        let donor_children = donor_endpoints
            .iter()
            .map(|endpoint| {
                group
                    .child(endpoint.owner.attempt.clone())
                    .map_err(|_| RuntimeError::OperationCancelled)
            })
            .collect::<Result<Vec<_>>>()?;

        // Preflight holds every exact owner. No target effect or tombstone is
        // installed until all participants and account absence are proved.
        let (_, before) = Self::sample_native_progress(&mut source).await?;
        if before.view() != &owned.successor {
            return Err(rejected("completed Build successor view changed"));
        }
        source
            .engine
            .verify_build_recovery_account(&owned.account, false, &source_child, false)
            .await
            .map_err(map_mysql)?;
        target
            .engine
            .verify_build_recovery_account(&owned.account, false, &target_child, false)
            .await
            .map_err(map_mysql)?;
        let (_, target_observation) = Self::sample_native_progress(&mut target).await?;
        proof::require_durable_containment(&before, &target_observation)?;
        for (donor, child) in donors.iter_mut().zip(&donor_children) {
            donor
                .engine
                .verify_build_recovery_account(&owned.account, false, child, false)
                .await
                .map_err(map_mysql)?;
            let (_, observation) = Self::sample_native_progress(donor).await?;
            proof::require_durable_containment(&before, &observation)?;
        }
        source_child
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        target_child
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        let token = target
            .engine
            .arm_build_abort_generation()
            .map_err(map_mysql)?;
        source
            .build_results
            .retire(owned.callback.build_id.as_str(), &owned.callback)
            .map_err(map_mysql)?;
        let record_index = source.removals.len();
        source.removals.push(RemoveRecord {
            owned: owned.clone(),
            terminal: None,
            retained_target: Some(target_endpoint.owner.clone()),
        });
        let fence = RemoveFence::arm(
            source.build_generation.clone(),
            target.build_generation.clone(),
            donors.iter().map(|d| d.build_generation.clone()).collect(),
            permit,
        );
        let mut owners = RemoveOwners {
            source,
            target,
            donors,
            fence,
            token,
        };
        owners
            .target
            .engine
            .leave_build_member(&target_child)
            .await
            .map_err(map_mysql)?;
        owners.fence.advance(RemoveCheckpoint::TargetLeft)?;
        let successor = reconcile_survivor(&mut owners.source, &owned, &source_child).await?;
        for (donor, child) in owners.donors.iter_mut().zip(&donor_children) {
            let observation = reconcile_survivor(donor, &owned, child).await?;
            if observation.view() != successor.view() {
                return Err(rejected(
                    "survivors did not prove the identical successor view",
                ));
            }
        }
        // Verify account absence again after native departure on every
        // surviving owner, rather than treating successful STOP as cleanup.
        owners
            .source
            .engine
            .verify_build_recovery_account(&owned.account, false, &source_child, false)
            .await
            .map_err(map_mysql)?;
        for (donor, child) in owners.donors.iter_mut().zip(&donor_children) {
            donor
                .engine
                .verify_build_recovery_account(&owned.account, false, child, false)
                .await
                .map_err(map_mysql)?;
        }
        let final_source = reconcile_survivor(&mut owners.source, &owned, &source_child).await?;
        if final_source.view() != successor.view() {
            return Err(rejected(
                "source successor changed before target retirement",
            ));
        }
        owners.fence.advance(RemoveCheckpoint::SurvivorsProved)?;
        source_child
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        target_child
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        let target_receipt = owners
            .target
            .engine
            .retire_idle_build_before(&owners.token, group.deadline())
            .map_err(map_mysql)?;
        if !target_receipt.is_complete() {
            return Err(rejected(
                "exact target process/socket containment is incomplete",
            ));
        }
        source_child
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        target_child
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        for child in &donor_children {
            child
                .admit_work()
                .map_err(|_| RuntimeError::OperationCancelled)?;
        }
        let donor_receipts = owners
            .donors
            .iter()
            .map(|donor| DonorReceipt {
                generation: donor.build_generation.id(),
                authority: donor.open_receipt.authority().clone(),
            })
            .collect();
        unregister(owners.target.owner);
        if lookup_build_endpoint(&owned.binding.target.replication_address).is_some() {
            return Err(rejected("target endpoint registration was not retired"));
        }
        owners.target.closed = true;
        clear_build_state(&mut owners.target, &owned);
        clear_build_state(&mut owners.source, &owned);
        for donor in &mut owners.donors {
            clear_build_state(donor, &owned);
        }
        let mut children = vec![&source_child, &target_child];
        children.extend(donor_children.iter());
        owners.fence.finish(&target_receipt, true, &children)?;
        owners.source.removals[record_index].terminal = Some(RemoveReceipt {
            target: target_receipt,
            successor: successor.view().clone(),
            donors: donor_receipts,
        });
        owners.source.removals[record_index].retained_target = None;
        Ok(())
    }

    async fn retry_remove(
        &self,
        source: &mut DriverState,
        owned: &CompletedBuildOwnership,
        receipt: &RemoveReceipt,
        deadline: Instant,
    ) -> Result<()> {
        require_source(source, owned, true)?;
        if !receipt.target.is_complete()
            || lookup_build_endpoint(&owned.binding.target.replication_address).is_some()
        {
            return Err(rejected(
                "retired target proof is incomplete or endpoint was replaced",
            ));
        }
        let mut endpoints = Vec::new();
        for peer in &owned.callback.source_configuration.replicas {
            endpoints.push(
                lookup_build_endpoint(&peer.replication_address)
                    .ok_or_else(|| rejected("retry donor owner is absent"))?,
            );
        }
        if endpoints.len() != receipt.donors.len() {
            return Err(rejected("retry donor scope differs"));
        }
        let mut donors = Vec::new();
        let mut deadline = deadline;
        for ((peer, endpoint), retained) in owned
            .callback
            .source_configuration
            .replicas
            .iter()
            .zip(&endpoints)
            .zip(&receipt.donors)
        {
            let donor = endpoint.owner.lease().await;
            require_bound_open(&donor)?;
            require_not_cleanup_only(&donor)?;
            proof::validate_peer_owner(peer, endpoint.owner.as_ref(), &donor)?;
            OwnerAdmission::capture(&donor, Some(endpoint.owner.as_ref()))?
                .donor(peer, owned, true)?;
            if donor.build_generation.id() != retained.generation
                || donor.open_receipt.authority() != &retained.authority
                || donor
                    .current_configuration
                    .as_ref()
                    .map(|c| &c.configuration)
                    != Some(&owned.callback.source_configuration.configuration)
                || donor.pending_build_binding.is_some()
            {
                return Err(rejected("retry donor incarnation/configuration changed"));
            }
            deadline = deadline.min(endpoint.owner.attempt.deadline());
            donors.push(donor);
        }
        let group = removal_group(owned, deadline)?;
        let source_child = group
            .child(self.attempt.clone())
            .map_err(|_| RuntimeError::OperationCancelled)?;
        source
            .engine
            .verify_build_recovery_account(&owned.account, false, &source_child, false)
            .await
            .map_err(map_mysql)?;
        let (_, observed) = Self::sample_native_progress(source).await?;
        if observed.view() != &receipt.successor {
            return Err(rejected("retry source successor scope changed"));
        }
        for (donor, endpoint) in donors.iter_mut().zip(&endpoints) {
            let child = group
                .child(endpoint.owner.attempt.clone())
                .map_err(|_| RuntimeError::OperationCancelled)?;
            donor
                .engine
                .verify_build_recovery_account(&owned.account, false, &child, false)
                .await
                .map_err(map_mysql)?;
            let (_, observed) = Self::sample_native_progress(donor).await?;
            if observed.view() != &receipt.successor {
                return Err(rejected("retry donor successor scope changed"));
            }
        }
        source_child
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        Ok(())
    }
}

fn require_source(state: &DriverState, owned: &CompletedBuildOwnership, retry: bool) -> Result<()> {
    require_bound_open(state)?;
    OwnerAdmission::capture(state, None)?.source(owned, retry)
}

#[derive(Clone)]
struct OwnerAdmission {
    public: PublicBinding,
    authority: MemberMutationAuthority,
    open_authority: MemberMutationAuthority,
    endpoint: String,
    group: String,
    generation: u64,
    status: BuildGenerationStatus,
    epoch: Option<Epoch>,
    mapped_epoch_matches: bool,
    role: Option<MysqlReplicaRole>,
    configuration: Option<ReplicaSetConfiguration>,
    engine_configuration: bool,
    pending_configuration: bool,
    binding: Option<Box<BuildBinding>>,
    target: Option<BuildTargetBinding>,
    native_successor: bool,
    idle_epoch: Option<Epoch>,
    idle: bool,
    role_admitted: bool,
    join: Option<(AttemptId, u64, NativeView, MemberMutationAuthority)>,
}

impl OwnerAdmission {
    fn capture(state: &DriverState, owner: Option<&MysqlPublicV1Replicator>) -> Result<Self> {
        let public = state
            .binding
            .clone()
            .ok_or(RuntimeError::AuthorityNotAdmitted)?;
        Ok(Self {
            endpoint: owner.map_or_else(
                || public.settings.replication_address.clone(),
                |o| o.endpoint.encode(),
            ),
            group: owner.map_or_else(
                || {
                    state
                        .open_receipt
                        .authority()
                        .group_name()
                        .as_str()
                        .to_owned()
                },
                |o| o.group_key.clone(),
            ),
            public,
            authority: state.engine.current_member_authority().map_err(map_mysql)?,
            open_authority: state.open_receipt.authority().clone(),
            generation: state.build_generation.id(),
            status: state.build_generation.status(),
            epoch: state.epochs.latest().map(|(e, _)| e),
            mapped_epoch_matches: state
                .epochs
                .latest()
                .is_some_and(|(_, mapped)| state.engine.epoch() == Some(mapped)),
            role: state.engine.role(),
            configuration: state.current_configuration.clone(),
            engine_configuration: state.engine.configuration().is_some(),
            pending_configuration: state.pending_configuration.is_some(),
            binding: state.pending_build_binding.clone(),
            target: state.pending_build_target.clone(),
            native_successor: state.engine.has_pending_build_successor(),
            idle_epoch: state.idle_build_preparation.epoch(),
            idle: state.idle_build_preparation.is_idle(),
            role_admitted: owner.is_some_and(|o| o.build_role_admitted.load(Ordering::Acquire)),
            join: state.engine.pending_build().map(|receipt| {
                (
                    receipt.build().clone(),
                    receipt.target_generation(),
                    receipt.member().successor().view().clone(),
                    receipt.member().authority().clone(),
                )
            }),
        })
    }

    fn source(&self, owned: &CompletedBuildOwnership, retry: bool) -> Result<()> {
        let build = owned.binding.as_ref();
        if self.configuration.as_ref() != Some(&owned.callback.source_configuration)
            || self.pending_configuration
            || !self.engine_configuration
            || self.role != Some(MysqlReplicaRole::Primary)
            || self.epoch != Some(build.epoch)
            || !self.mapped_epoch_matches
            || self.authority != build.source_authority
            || self.open_authority != self.authority
            || self.public.identity != build.source.identity
            || self.public.process_session != build.source.process_session
            || self.public.public_storage.as_ref() != Some(&build.source.public_storage)
            || self.endpoint != build.source.replication_address
            || self.group != build.group_key
            || self.generation != build.source_generation
            || self.configuration.as_ref().is_some_and(|c| {
                c.configuration
                    .members
                    .iter()
                    .any(|m| m.identity.replica_id == build.target.identity.replica_id)
            })
            || if retry {
                self.status != BuildGenerationStatus::Available
                    || self.binding.is_some()
                    || self.target.is_some()
                    || self.native_successor
            } else {
                self.status != BuildGenerationStatus::PendingAdmission
                    || self.binding.as_deref() != Some(build)
                    || self.target.as_ref() != Some(&build.target)
                    || !self.native_successor
            }
        {
            return Err(rejected(
                "source no longer owns the exact completed public-idle Build",
            ));
        }
        Ok(())
    }

    fn target(&self, owned: &CompletedBuildOwnership) -> Result<()> {
        let build = owned.binding.as_ref();
        if self.public.identity != build.target.identity
            || self.public.process_session != build.target.process_session
            || self.public.public_storage.as_ref() != Some(build.target.native.storage())
            || self.endpoint != build.target.replication_address
            || self.group != build.group_key
            || self.authority != owned.target_authority
            || self.open_authority != self.authority
            || self.generation != build.target_generation
            || self.status != BuildGenerationStatus::PendingAdmission
            || self.binding.as_deref() != Some(build)
            || self.target.as_ref() != Some(&build.target)
            || self.idle_epoch != Some(build.epoch)
            || !self.idle
            || self.role_admitted
            || self.role.is_some()
            || self.engine_configuration
            || self.configuration.is_some()
            || self.pending_configuration
            || self.epoch != Some(build.epoch)
            || !self.mapped_epoch_matches
            || self
                .join
                .as_ref()
                .is_none_or(|(id, generation, view, authority)| {
                    id != &build.child_attempt
                        || *generation != build.target_generation
                        || view != &owned.successor
                        || authority != &owned.target_authority
                })
        {
            return Err(rejected(
                "target is stale, substituted, admitted, or not the completed idle owner",
            ));
        }
        Ok(())
    }

    fn donor(
        &self,
        peer: &ReplicaInformation,
        owned: &CompletedBuildOwnership,
        retry: bool,
    ) -> Result<()> {
        let native = translate_replica_information(peer).map_err(map_translation)?;
        if self.public.identity != peer.identity
            || self.public.process_session != peer.process_session_id
            || self.public.public_storage.as_ref() != Some(native.storage())
            || self.endpoint != peer.replication_address
            || self.group != owned.binding.group_key
            || self.authority.local().member_id() != native.member().member_id()
            || self.authority.local().binding().member_address() != native.member().address()
            || self.open_authority != self.authority
            || self.configuration.as_ref().map(|c| &c.configuration)
                != Some(&owned.callback.source_configuration.configuration)
            || !self.engine_configuration
            || self.pending_configuration
            || self.role != Some(MysqlReplicaRole::Secondary)
            || self.epoch != Some(owned.binding.epoch)
            || !self.mapped_epoch_matches
            || owned
                .donors
                .iter()
                .find(|d| d.identity == peer.identity)
                .is_none_or(|d| d.generation != self.generation || d.authority != self.authority)
            || if retry {
                self.status != BuildGenerationStatus::Available
                    || self.binding.is_some()
                    || self.target.is_some()
                    || self.native_successor
            } else {
                self.status != BuildGenerationStatus::PendingAdmission
                    || self.binding.as_deref() != Some(owned.binding.as_ref())
                    || self.target.as_ref() != Some(&owned.binding.target)
                    || !self.native_successor
            }
        {
            return Err(rejected("donor lost the exact completed Build authority"));
        }
        Ok(())
    }
}

fn require_target(
    state: &DriverState,
    owner: &MysqlPublicV1Replicator,
    owned: &CompletedBuildOwnership,
) -> Result<()> {
    require_bound_open(state)?;
    OwnerAdmission::capture(state, Some(owner))?.target(owned)
}

fn require_donor(
    state: &DriverState,
    owner: &MysqlPublicV1Replicator,
    peer: &ReplicaInformation,
    owned: &CompletedBuildOwnership,
) -> Result<()> {
    require_bound_open(state)?;
    OwnerAdmission::capture(state, Some(owner))?.donor(peer, owned, false)
}

async fn reconcile_survivor(
    state: &mut DriverState,
    owned: &CompletedBuildOwnership,
    child: &BuildAttemptContext,
) -> Result<crate::service::MemberNativeEvidence> {
    loop {
        child
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        state.progress_sequence = state
            .progress_sequence
            .checked_add(1)
            .ok_or(RuntimeError::OperationCancelled)?;
        let operation = operation(
            state,
            OperationKey::Progress(state.progress_sequence),
            OperationValue::Progress,
        )?;
        if let Some(observed) = state
            .engine
            .observe_idle_remove_successor(
                operation,
                &owned.successor,
                owned.binding.target.native.member().member_id(),
            )
            .await
            .map_err(map_mysql)?
        {
            return Ok(observed);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn removal_group(owned: &CompletedBuildOwnership, deadline: Instant) -> Result<BuildAttemptGroup> {
    fresh_removal_group(
        &owned.binding.build_id,
        owned.binding.target_generation,
        deadline,
    )
}

fn fresh_removal_group(
    build_id: &OperationId,
    target_generation: u64,
    deadline: Instant,
) -> Result<BuildAttemptGroup> {
    let id = AttemptId::new(format!(
        "remove-{}-{}",
        build_id.as_str(),
        target_generation,
    ))
    .map_err(|error| rejected(&error.to_string()))?;
    BuildAttemptGroup::new(id, deadline, Duration::from_secs(15))
        .map_err(|_| RuntimeError::OperationCancelled)
}

fn clear_build_state(state: &mut DriverState, owned: &CompletedBuildOwnership) {
    state.pending_build_target = None;
    state.pending_build_binding = None;
    state.pending_configuration = None;
    state.pending_native_configuration = None;
    state.committed_sample = None;
    state.epoch_commit = None;
    state.fresh_open_progress = None;
    state.cached_source_boundary = None;
    state.cached_source_progress = None;
    state.last_reported_progress = None;
    state.catch_up_proven = false;
    state
        .retained_replica_progress
        .retain(|r| r.identity != owned.binding.target.identity);
    state
        .completed_builds
        .retain(|r| r.binding.build_id != owned.binding.build_id);
}

fn unregister(owner: &MysqlPublicV1Replicator) {
    owner
        .build_registration
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    owner.build_role_admitted.store(false, Ordering::Release);
    owner
        .build_catch_up_admitted
        .store(false, Ordering::Release);
    *owner
        .build_admitted_configuration
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
}

fn rejected(message: &str) -> RuntimeError {
    RuntimeError::InvalidReplication(format!("MySQL idle Remove rejected: {message}"))
}

async fn bounded_remove_work<T>(
    deadline: Instant,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future)
        .await
        .map_err(|_| RuntimeError::OperationCancelled)?
}

#[cfg(test)]
#[path = "remove_tests.rs"]
pub(super) mod deterministic_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removal_uses_current_parents_not_expired_or_cancelled_build_child() {
        let parent = AttemptContext::new(
            AttemptId::new("remove-live-parent").unwrap(),
            Instant::now() + Duration::from_secs(60),
            Duration::from_secs(5),
        )
        .unwrap();
        let old = BuildAttemptContext::new(
            parent.clone(),
            AttemptId::new("old-build").unwrap(),
            Instant::now() + Duration::from_millis(30),
            Duration::from_millis(5),
        )
        .unwrap();
        std::thread::sleep(Duration::from_millis(35));
        old.cancel();
        assert!(old.admit_work().is_err());
        let group = fresh_removal_group(
            &OperationId::new("old-build"),
            7,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();
        let current = group.child(parent.clone()).unwrap();
        assert!(current.admit_work().is_ok());
        assert_ne!(current.build(), old.build());
        assert!(parent.admit_work().is_ok());
        assert!(fresh_removal_group(&OperationId::new("old-build"), 7, Instant::now()).is_err());
    }
}
