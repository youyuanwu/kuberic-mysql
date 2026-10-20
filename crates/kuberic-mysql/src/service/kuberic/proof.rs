use super::*;
use crate::service::MemberNativeEvidence;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProofScope {
    binding: Option<PublicBinding>,
    epoch: Option<MysqlReplicaEpoch>,
    role: Option<MysqlReplicaRole>,
    current: Option<ReplicaSetConfiguration>,
    pending: Option<ReplicaSetConfiguration>,
    build: Option<Box<BuildBinding>>,
    generation: u64,
    authority: crate::service::MemberMutationAuthority,
    status: BuildGenerationStatus,
    public_epoch: Option<Epoch>,
    mapped_epoch_matches: bool,
    admitted_role: Option<ReplicaRole>,
    pending_join: bool,
    native_identity: crate::service::MysqlReplicaIdentity,
    native_successor: bool,
    idle_preparation: IdleBuildPreparation,
    access: Option<MysqlAccessIntent>,
    proxy: ProxyState,
}

impl ProofScope {
    pub(super) fn capture(state: &DriverState) -> Self {
        Self {
            binding: state.binding.clone(),
            epoch: state.engine.epoch(),
            role: state.engine.role(),
            current: state.current_configuration.clone(),
            pending: state.pending_configuration.clone(),
            build: state.pending_build_binding.clone(),
            generation: state.build_generation.id(),
            authority: state.open_receipt.authority().clone(),
            status: state.build_generation.status(),
            public_epoch: state.epochs.latest().map(|(epoch, _)| epoch),
            mapped_epoch_matches: state
                .epochs
                .latest()
                .is_some_and(|(_, mapped)| state.engine.epoch() == Some(mapped)),
            admitted_role: state
                .epochs
                .latest()
                .and_then(|(epoch, _)| state.completed_roles.get(&EpochKey::from(epoch)).copied()),
            pending_join: state.engine.pending_build().is_some(),
            native_identity: state.engine.identity().clone(),
            native_successor: state.engine.has_pending_build_successor(),
            idle_preparation: state.idle_build_preparation.clone(),
            access: state.engine.access_intent(),
            proxy: state.proxy.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CommittedSample {
    scope: ProofScope,
    evidence: MemberNativeEvidence,
    value: i64,
    participants: Vec<(String, ProofScope)>,
    configurations: Vec<ReplicaSetConfiguration>,
}

impl CommittedSample {
    pub(super) fn matches_scope(&self, state: &DriverState) -> bool {
        self.scope == ProofScope::capture(state)
    }

    pub(super) fn matches_boundary(&self, state: &DriverState) -> bool {
        state
            .cached_source_boundary
            .as_ref()
            .is_some_and(|boundary| {
                self.evidence.executed() == boundary.evidence().executed()
                    && self.evidence.authority() == boundary.evidence().authority()
            })
    }
}

pub(super) fn epoch_commit_matches(state: &DriverState, epoch: Epoch, value: i64) -> bool {
    if !epoch_profile_qualified(
        state.durability_qualified,
        state.engine.native_durability_qualified(),
    ) {
        return false;
    }
    let fresh = state.fresh_epoch == Some(epoch)
        && value == 0
        && state.fresh_open_progress.is_some()
        && state.engine.native_evidence().is_none();
    fresh
        || state
            .epoch_commit
            .as_ref()
            .is_some_and(|(expected, sample)| {
                *expected == epoch
                    && sample.value == value
                    && sample.scope.authority == *state.open_receipt.authority()
                    && sample.scope.generation == state.build_generation.id()
                    && sample.scope.binding == state.binding
            })
}

fn epoch_profile_qualified(driver_copy: bool, owner_latch: bool) -> bool {
    driver_copy && owner_latch
}

impl MysqlPublicV1Replicator {
    pub(super) fn require_native_durability(state: &mut DriverState) -> Result<()> {
        state.durability_qualified &= state.engine.native_durability_qualified()
            && state
                .engine
                .native_evidence()
                .is_none_or(|e| e.has_durable_profile());
        if !state.durability_qualified {
            return Err(unproved(
                "exact owner lifetime lost native durability qualification",
            ));
        }
        if let Some(binding) = state.pending_build_binding.as_deref()
            && let Some(completed) = state
                .completed_builds
                .iter()
                .find(|c| c.binding.as_ref() == binding)
            && state
                .engine
                .native_evidence()
                .is_none_or(|e| e.view() != &completed.successor)
        {
            return Err(unproved("completed Build successor view changed"));
        }
        Ok(())
    }

    pub(super) fn require_proved_build_boundary(state: &DriverState) -> Result<()> {
        let sample = state
            .committed_sample
            .as_ref()
            .ok_or_else(|| unproved("Build has no independently committed invocation boundary"))?;
        let boundary = state
            .cached_source_boundary
            .as_ref()
            .ok_or(RuntimeError::ReconfigurationPending)?;
        if sample.scope != ProofScope::capture(state) || sample.evidence != *boundary.evidence() {
            return Err(unproved(
                "Build boundary differs from independently proved commitment",
            ));
        }
        Ok(())
    }
    pub(super) fn acquire_proof_permit(&self) -> Result<BuildGroupPermit> {
        BuildGroupPermit::try_acquire(self.group_key.clone())
            .ok_or(RuntimeError::ReconfigurationPending)
    }

    /// Only stable, unformed owner-local reads may bypass the peer-proof
    /// permit. A leased/transitioning owner remains fail-fast. Callers recheck
    /// after acquiring their local lease before using either proof path.
    pub(super) fn progress_permit(&self) -> Result<Option<BuildGroupPermit>> {
        let slot = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = slot.as_ref().ok_or(RuntimeError::ReconfigurationPending)?;
        if state.fresh_open_progress.is_some()
            && state.engine.configuration().is_none()
            && state.engine.pending_build().is_none()
        {
            Ok(None)
        } else {
            self.acquire_proof_permit().map(Some)
        }
    }

    pub(super) fn proof_deadline(&self) -> Instant {
        self.attempt.work_deadline().min(
            Instant::now()
                .checked_add(Duration::from_secs(15))
                .expect("bounded progress deadline"),
        )
    }

    pub(super) async fn sample_native_progress(
        state: &mut DriverState,
    ) -> Result<(i64, MemberNativeEvidence)> {
        require_bound_open(state)?;
        require_not_cleanup_only(state)?;
        Self::require_native_durability(state)?;
        if !state.durability_qualified {
            return Err(unproved(
                "owner lifetime lost historical durability qualification",
            ));
        }
        let progress = if state.engine.configuration().is_some() {
            Self::capture_current_boundary(state, false).await?;
            state
                .cached_source_progress
                .ok_or(RuntimeError::ReconfigurationPending)?
        } else if state.engine.pending_build().is_some() {
            match state
                .engine
                .refresh_pending_build_progress()
                .await
                .map_err(map_mysql)?
                .progress()
            {
                ProgressProjection::Projectable(progress) => progress,
                ProgressProjection::NonProjectable(_) => {
                    return Err(unproved("non-projectable built history"));
                }
            }
        } else {
            return Err(RuntimeError::ReconfigurationPending);
        };
        let evidence = state
            .engine
            .native_evidence()
            .cloned()
            .ok_or(RuntimeError::ReconfigurationPending)?;
        if !evidence.has_durable_profile() {
            state.durability_qualified = false;
            return Err(unproved("native durability profile is not qualified"));
        }
        Ok((progress.value(), evidence))
    }

    pub(super) async fn prove_primary_sample(
        &self,
        state: &mut DriverState,
        value: i64,
        sample: MemberNativeEvidence,
        configurations: &[ReplicaSetConfiguration],
        deadline: Instant,
    ) -> Result<CommittedSample> {
        if state.engine.role() != Some(MysqlReplicaRole::Primary)
            || sample.primary().id() != state.open_receipt.authority().local().member_id()
        {
            return Err(unproved(
                "commitment proof requires the exact native primary",
            ));
        }
        if !applicable_configurations_supported(&state.completed_builds, configurations) {
            return Err(unproved(
                "differing configurations lack an exact additive Build transition",
            ));
        }
        let scope = ProofScope::capture(state);
        let mut peers = BTreeMap::new();
        for configuration in configurations {
            for peer in &configuration.replicas {
                if let Some(previous) = peers.insert(peer.identity.replica_id, peer.clone())
                    && !same_peer(&previous, peer)
                {
                    return Err(unproved("applicable configurations substitute a voter"));
                }
            }
        }
        let mut endpoints = Vec::new();
        for peer in peers.values() {
            let endpoint = lookup_build_endpoint(&peer.replication_address)
                .ok_or_else(|| unproved("exact voter owner is unavailable"))?;
            if std::ptr::eq(endpoint.owner.as_ref(), self) {
                return Err(unproved("voter aliases the source owner"));
            }
            endpoints.push((peer, endpoint));
        }
        let mut leases = Vec::new();
        let mut participants = Vec::new();
        for (peer, endpoint) in &endpoints {
            let mut voter = Self::bounded_acquisition(deadline, endpoint.owner.lease()).await?;
            require_bound_open(&voter)?;
            require_not_cleanup_only(&voter)?;
            validate_peer_owner(peer, endpoint.owner.as_ref(), &voter)?;
            if endpoint.owner.group_key != self.group_key
                || !voter_configuration_authorized(state, &voter, configurations, peer, &sample)
            {
                return Err(unproved(
                    "voter lacks exact current or completed-Build transition authority",
                ));
            }
            let (_, observation) = Self::sample_native_progress(&mut voter).await?;
            validate_voter_observation(
                &ProofScope::capture(&voter),
                &state.completed_builds,
                configurations,
                peer,
                &sample,
                &observation,
            )?;
            participants.push((
                peer.replication_address.clone(),
                ProofScope::capture(&voter),
            ));
            leases.push(voter);
        }
        let (_, latest) = Self::sample_native_progress(state).await?;
        require_durable_containment(&sample, &latest)?;
        self.attempt
            .admit_work()
            .map_err(|_| RuntimeError::OperationCancelled)?;
        if scope != ProofScope::capture(state) {
            return Err(unproved("source proof scope changed"));
        }
        let proof = CommittedSample {
            scope,
            evidence: sample,
            value,
            participants,
            configurations: configurations.to_vec(),
        };
        drop(leases);
        Ok(proof)
    }

    async fn bounded_acquisition<T>(
        deadline: Instant,
        future: impl Future<Output = T>,
    ) -> Result<T> {
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future)
            .await
            .map_err(|_| RuntimeError::OperationCancelled)
    }

    pub(super) async fn coherent_committed_sample(&self, state: &mut DriverState) -> Result<i64> {
        Self::require_native_durability(state)?;
        if state.fresh_open_progress.is_some() {
            let sampled = state
                .fresh_sample
                .take()
                .ok_or_else(|| unproved("no coherent fresh historical sample"))?;
            if sampled != ProofScope::capture(state) || state.last_reported_progress != Some(0) {
                return Err(unproved("fresh historical sample scope changed"));
            }
            let value = Self::sample_fresh_progress(state).await?;
            state.fresh_sample = Some(sampled);
            return Ok(value);
        }
        let retained = state
            .committed_sample
            .clone()
            .ok_or_else(|| unproved("no independently proved committed sample"))?;
        if retained.scope != ProofScope::capture(state) {
            return Err(unproved(
                "committed sample authority or configuration changed",
            ));
        }
        let (_, fresh) = Self::sample_native_progress(state).await?;
        require_durable_containment(&retained.evidence, &fresh)?;
        let revalidated = self
            .prove_primary_sample(
                state,
                retained.value,
                retained.evidence.clone(),
                &retained.configurations,
                self.proof_deadline(),
            )
            .await?;
        if retained.participants != revalidated.participants {
            return Err(unproved(
                "participant proof scope changed between paired callbacks",
            ));
        }
        Ok(retained.value)
    }

    pub(super) async fn sample_fresh_progress(state: &mut DriverState) -> Result<i64> {
        require_bound_open(state)?;
        require_not_cleanup_only(state)?;
        if state.fresh_open_progress.is_none()
            || state.current_configuration.is_some()
            || state.pending_configuration.is_some()
            || state.pending_native_configuration.is_some()
            || state.pending_build_binding.is_some()
            || state.build_generation.status() != BuildGenerationStatus::Available
            || !matches!(state.proxy, ProxyState::Unprepared | ProxyState::Revoked)
        {
            return Err(unproved(
                "fresh historical evidence is no longer applicable",
            ));
        }
        let scope = ProofScope::capture(state);
        let progress = state
            .engine
            .revalidate_fresh_progress()
            .await
            .map_err(map_mysql)?;
        Self::require_native_durability(state)?;
        if scope != ProofScope::capture(state) {
            return Err(unproved(
                "fresh owner scope changed during native observation",
            ));
        }
        Ok(progress.value())
    }
}

fn same_peer(a: &ReplicaInformation, b: &ReplicaInformation) -> bool {
    a.identity == b.identity
        && a.process_session_id == b.process_session_id
        && a.storage_incarnation == b.storage_incarnation
        && a.replication_address == b.replication_address
        && a.role == b.role
}

pub(super) fn validate_peer_owner(
    peer: &ReplicaInformation,
    owner: &MysqlPublicV1Replicator,
    state: &DriverState,
) -> Result<()> {
    let public = state
        .binding
        .as_ref()
        .ok_or(RuntimeError::AuthorityNotAdmitted)?;
    let native = translate_replica_information(peer).map_err(map_translation)?;
    if peer.identity != public.identity
        || peer.process_session_id != public.process_session
        || peer.replication_address != owner.endpoint.encode()
        || Some(native.storage()) != public.public_storage.as_ref()
        || native.member().member_id() != state.open_receipt.authority().local().member_id()
        || native.identity() != state.engine.identity()
    {
        return Err(unproved(
            "live voter identity/session/storage/endpoint substituted",
        ));
    }
    Ok(())
}

fn voter_configuration_authorized(
    source: &DriverState,
    voter: &DriverState,
    applicable: &[ReplicaSetConfiguration],
    peer: &ReplicaInformation,
    sample: &MemberNativeEvidence,
) -> bool {
    voter_authority_is_allowed(
        &ProofScope::capture(voter),
        &source.completed_builds,
        applicable,
        peer,
        sample.view(),
    )
}

fn voter_authority_is_allowed(
    voter: &ProofScope,
    completed: &[super::super::build::CompletedBuildOwnership],
    applicable: &[ReplicaSetConfiguration],
    peer: &ReplicaInformation,
    sample_view: &crate::core::NativeView,
) -> bool {
    let expected_epoch = applicable[0].configuration.epoch;
    if voter.public_epoch.is_none_or(|epoch| {
        !voter_epoch_role_authorized(
            epoch,
            expected_epoch,
            voter.mapped_epoch_matches,
            voter.role,
            voter.admitted_role,
            voter.status,
        )
    }) {
        return false;
    }
    if let Some(binding) = voter.build.as_deref()
        && !completed.iter().any(|owned| {
            owned.binding.as_ref() == binding
                && if peer.identity == binding.target.identity {
                    voter.generation == binding.target_generation
                        && voter.authority == owned.target_authority
                } else {
                    owned.donors.iter().any(|donor| {
                        donor.identity == peer.identity
                            && donor.generation == voter.generation
                            && donor.authority == voter.authority
                    })
                }
        })
    {
        return false;
    }
    if voter.current.as_ref().is_some_and(|installed| {
        applicable
            .iter()
            .any(|c| c.configuration == installed.configuration)
            && !completed.iter().any(|owned| {
                installed.configuration == owned.binding.source_configuration.configuration
                    && applicable
                        .iter()
                        .any(|c| additive_build_configuration(&owned.binding, c))
            })
    }) {
        return true;
    }
    completed.iter().any(|completed| {
        let build = completed.binding.as_ref();
        let proposed = applicable
            .iter()
            .find(|c| additive_build_configuration(build, c));
        proposed.is_some()
            && &completed.successor == sample_view
            && voter.build.as_deref() == Some(build)
            && voter.status == BuildGenerationStatus::PendingAdmission
            && (voter.current.as_ref().is_some_and(|c| {
                c.configuration == build.source_configuration.configuration
                    && voter.native_successor
            }) || (peer.identity == build.target.identity && voter.pending_join))
    })
}

fn applicable_configurations_supported(
    completed: &[super::super::build::CompletedBuildOwnership],
    configurations: &[ReplicaSetConfiguration],
) -> bool {
    !configurations.is_empty()
        && (configurations[1..]
            .iter()
            .all(|c| c.configuration == configurations[0].configuration)
            || completed.iter().any(|owned| {
                configurations
                    .iter()
                    .any(|c| c.configuration == owned.binding.source_configuration.configuration)
                    && configurations.iter().all(|c| {
                        c.configuration == owned.binding.source_configuration.configuration
                            || additive_build_configuration(&owned.binding, c)
                    })
            }))
}

fn validate_voter_observation(
    voter: &ProofScope,
    completed: &[super::super::build::CompletedBuildOwnership],
    applicable: &[ReplicaSetConfiguration],
    peer: &ReplicaInformation,
    frozen: &MemberNativeEvidence,
    observed: &MemberNativeEvidence,
) -> Result<()> {
    let public = voter
        .binding
        .as_ref()
        .ok_or(RuntimeError::AuthorityNotAdmitted)?;
    let native = translate_replica_information(peer).map_err(map_translation)?;
    if public.identity != peer.identity
        || public.process_session != peer.process_session_id
        || public.public_storage.as_ref() != Some(native.storage())
        || public.settings.replication_address != peer.replication_address
        || voter.native_identity != *native.identity()
        || voter.authority.local().member_id() != native.member().member_id()
        || observed.authority() != &voter.authority
        || !applicable_configurations_supported(completed, applicable)
        || !voter_authority_is_allowed(voter, completed, applicable, peer, frozen.view())
    {
        return Err(unproved(
            "live voter does not retain the complete transition scope",
        ));
    }
    require_durable_containment(frozen, observed)
}

fn voter_epoch_role_authorized(
    actual: Epoch,
    expected: Epoch,
    mapped: bool,
    role: Option<MysqlReplicaRole>,
    admitted: Option<ReplicaRole>,
    status: BuildGenerationStatus,
) -> bool {
    actual == expected
        && mapped
        && role == Some(MysqlReplicaRole::Secondary)
        && admitted == Some(ReplicaRole::ActiveSecondary)
        && matches!(
            status,
            BuildGenerationStatus::Available | BuildGenerationStatus::PendingAdmission
        )
}

pub(super) fn additive_build_configuration(
    build: &BuildBinding,
    proposed: &ReplicaSetConfiguration,
) -> bool {
    let old = &build.source_configuration.configuration;
    let new = &proposed.configuration;
    if new.epoch != old.epoch
        || new.primary_id != old.primary_id
        || new.members.len() != old.members.len() + 1
        || !old
            .members
            .iter()
            .all(|member| new.members.contains(member))
        || !new.members.iter().any(|member| {
            member.identity == build.target.identity && member.role == ReplicaRole::ActiveSecondary
        })
    {
        return false;
    }
    build.source_configuration.replicas.iter().all(|old_peer| {
        proposed
            .replicas
            .iter()
            .any(|peer| same_peer(old_peer, peer))
    }) && proposed.replicas.iter().any(|peer| {
        peer.identity == build.target.identity
            && peer.process_session_id == build.target.process_session
            && peer.storage_incarnation.as_ref() == Some(&build.target.storage)
            && peer.replication_address == build.target.replication_address
    })
}

pub(super) fn require_durable_containment(
    frozen: &MemberNativeEvidence,
    observed: &MemberNativeEvidence,
) -> Result<()> {
    if !frozen.has_durable_profile()
        || !observed.has_durable_profile()
        || frozen.view() != observed.view()
    {
        return Err(unproved(
            "missing exact-view native durability qualification",
        ));
    }
    if !durable_containment(
        frozen.has_durable_profile() && observed.has_durable_profile(),
        frozen.view() == observed.view(),
        frozen.executed(),
        observed.executed(),
    ) {
        if observed.executed().is_subset_of(frozen.executed()) {
            return Err(RuntimeError::ReconfigurationPending);
        }
        return Err(unproved("divergent durable structured history"));
    }
    Ok(())
}

fn durable_containment(
    qualified: bool,
    exact_view: bool,
    frozen: &crate::core::GtidSet,
    observed: &crate::core::GtidSet,
) -> bool {
    qualified && exact_view && frozen.is_subset_of(observed)
}

fn unproved(message: &str) -> RuntimeError {
    RuntimeError::InvalidReplication(format!("MySQL committed progress is unproved: {message}"))
}

#[cfg(test)]
#[path = "proof_transition_tests.rs"]
mod transition_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::GtidSet;
    use std::str::FromStr;

    #[test]
    fn epoch_validation_observes_native_invalidation_before_driver_copy_is_updated() {
        assert!(epoch_profile_qualified(true, true));
        assert!(!epoch_profile_qualified(true, false));
        assert!(!epoch_profile_qualified(false, true));
        assert!(!epoch_profile_qualified(false, false));
    }

    #[tokio::test]
    async fn partial_peer_acquisition_timeout_and_drop_release_leases_and_group_permit() {
        let first = AsyncMutex::new(());
        let second = AsyncMutex::new(());
        let held_second = second.lock().await;
        let permit = BuildGroupPermit::try_acquire("proof-partial-acquisition").unwrap();
        let acquisition = async {
            let _permit = permit;
            let _first = first.lock().await;
            assert!(BuildGroupPermit::try_acquire("proof-partial-acquisition").is_none());
            let _second = second.lock().await;
        };
        assert!(
            MysqlPublicV1Replicator::bounded_acquisition(
                Instant::now() + Duration::from_millis(10),
                acquisition
            )
            .await
            .is_err()
        );
        assert!(first.try_lock().is_ok());
        assert!(BuildGroupPermit::try_acquire("proof-partial-acquisition").is_some());
        drop(held_second);
        let permit = BuildGroupPermit::try_acquire("proof-cancel-acquisition").unwrap();
        let held_second = second.lock().await;
        let mut acquisition = Box::pin(async {
            let _permit = permit;
            let _first = first.lock().await;
            let _second = second.lock().await;
        });
        tokio::select! {
            () = &mut acquisition => panic!("blocked acquisition completed"),
            () = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
        drop(acquisition);
        assert!(first.try_lock().is_ok());
        assert!(BuildGroupPermit::try_acquire("proof-cancel-acquisition").is_some());
        drop(held_second);
    }

    #[test]
    fn voter_epoch_role_authority_rejects_each_stale_or_unadmitted_state() {
        let epoch = Epoch::new(0, 1);
        let valid = |actual, mapped, role, admitted, status| {
            voter_epoch_role_authorized(actual, epoch, mapped, role, admitted, status)
        };
        for status in [
            BuildGenerationStatus::Available,
            BuildGenerationStatus::PendingAdmission,
        ] {
            assert!(valid(
                epoch,
                true,
                Some(MysqlReplicaRole::Secondary),
                Some(ReplicaRole::ActiveSecondary),
                status
            ));
        }
        assert!(!valid(
            Epoch::new(0, 2),
            true,
            Some(MysqlReplicaRole::Secondary),
            Some(ReplicaRole::ActiveSecondary),
            BuildGenerationStatus::PendingAdmission
        ));
        assert!(!valid(
            epoch,
            false,
            Some(MysqlReplicaRole::Secondary),
            Some(ReplicaRole::ActiveSecondary),
            BuildGenerationStatus::PendingAdmission
        ));
        assert!(!valid(
            epoch,
            true,
            None,
            Some(ReplicaRole::ActiveSecondary),
            BuildGenerationStatus::PendingAdmission
        ));
        assert!(!valid(
            epoch,
            true,
            Some(MysqlReplicaRole::Primary),
            Some(ReplicaRole::ActiveSecondary),
            BuildGenerationStatus::PendingAdmission
        ));
        assert!(!valid(
            epoch,
            true,
            Some(MysqlReplicaRole::Secondary),
            Some(ReplicaRole::IdleSecondary),
            BuildGenerationStatus::PendingAdmission
        ));
        assert!(!valid(
            epoch,
            true,
            Some(MysqlReplicaRole::Secondary),
            None,
            BuildGenerationStatus::PendingAdmission
        ));
        for status in [
            BuildGenerationStatus::CleanupOnly,
            BuildGenerationStatus::NeedsRebuild,
            BuildGenerationStatus::Reserved,
        ] {
            assert!(!valid(
                epoch,
                true,
                Some(MysqlReplicaRole::Secondary),
                Some(ReplicaRole::ActiveSecondary),
                status
            ));
        }
    }

    #[test]
    fn peer_proof_binding_does_not_reduce_to_identity_or_scalar() {
        use kuberic_runtime::protocol::types::{
            AgentGeneration, PodUid, PvcUid, ReplicaId, ReplicaInstanceId, ReplicaStorageIdentity,
            ReplicaStorageIncarnationContext, StorageGeneration,
        };
        let identity = ReplicaIdentity {
            replica_id: ReplicaId::new(2),
            instance_id: ReplicaInstanceId::new("instance"),
            agent_generation: AgentGeneration::new("agent"),
        };
        let mut peer = ReplicaInformation::active_peer(
            identity.clone(),
            "endpoint".into(),
            PublicProcessSessionId::new("session"),
            7,
            INVALID_LSN,
            false,
        )
        .unwrap();
        peer.storage_incarnation = Some(ReplicaStorageIncarnationContext {
            replica: identity,
            storage: ReplicaStorageIdentity::Persistent {
                pod_uid: PodUid::new("pod"),
                pvc_uid: PvcUid::new("pvc"),
            },
            storage_generation: StorageGeneration::new("generation"),
        });
        let mut explicit_later = peer.clone();
        explicit_later.current_progress = 99;
        assert!(
            same_peer(&peer, &explicit_later),
            "proof binding does not trust advertised scalar"
        );
        let mut changed = peer.clone();
        changed.process_session_id = PublicProcessSessionId::new("replacement");
        assert!(!same_peer(&peer, &changed));
        changed = peer.clone();
        changed
            .storage_incarnation
            .as_mut()
            .unwrap()
            .storage_generation = StorageGeneration::new("replacement");
        assert!(!same_peer(&peer, &changed));
        changed = peer.clone();
        changed.replication_address.push('x');
        assert!(!same_peer(&peer, &changed));
        changed = peer.clone();
        changed.identity.agent_generation = AgentGeneration::new("replacement");
        assert!(!same_peer(&peer, &changed));
        changed = peer.clone();
        changed.role = ReplicaRole::Primary;
        assert!(!same_peer(&peer, &changed));
    }

    #[test]
    fn commitment_requires_qualified_exact_view_structured_containment() {
        let frozen = GtidSet::from_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-7").unwrap();
        let later = GtidSet::from_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-9").unwrap();
        let gap = GtidSet::from_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-5:7-9").unwrap();
        let foreign = GtidSet::from_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-9").unwrap();
        assert!(durable_containment(true, true, &frozen, &frozen));
        assert!(durable_containment(true, true, &frozen, &later));
        assert!(!durable_containment(false, true, &frozen, &later));
        assert!(!durable_containment(true, false, &frozen, &later));
        assert!(!durable_containment(true, true, &later, &frozen));
        assert!(!durable_containment(true, true, &frozen, &gap));
        assert!(!durable_containment(true, true, &frozen, &foreign));
        assert!(!durable_containment(false, true, &GtidSet::empty(), &later));
    }
}
