use std::collections::HashSet;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, Instant};

use crate::core::{CredentialGeneration, GtidSet};
use crate::service::config::TOPOLOGY_NATIVE_PROFILE_OPTIONS;
use crate::service::control::QualificationRecoveryMutation;
use crate::service::instance::QualificationStopKind;
use crate::service::topology::QualificationManagerParts;
use crate::service::{
    ControlCredential, ControlStage, MemberCleanupFailure, MemberControlBinding,
    MysqlInstanceError, MysqlInstanceManager, MysqlMemberIndex, MysqlTopologyError,
    MysqlTopologyManager, MysqlTopologyManagerError, NativeControlDeadline, TransitionCredit,
};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder};
use tokio::time::sleep;

const ORACLE_PACKAGE: &str = "mysql-community-server-core=8.4.11-1ubuntu24.04";
const CLEANUP_RESERVE: Duration = Duration::from_secs(120);
static NEXT_TRANSACTION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QualificationMemberPhase {
    Running,
    ProcessStopped,
    Restarting,
    Rejoining,
    Contained,
    ContainmentFailed,
}

#[derive(Debug)]
struct QualificationCleanupReport {
    failures: Vec<MemberCleanupFailure>,
    deadline_overrun: bool,
}

#[derive(Debug)]
struct QualificationOperationFailure {
    primary: MysqlInstanceError,
    cleanup: QualificationCleanupReport,
}

trait QualificationOwnedMember {
    fn qualification_stop(&mut self, kind: QualificationStopKind)
    -> Result<(), MysqlInstanceError>;
    fn qualification_restart(&mut self) -> Result<(), MysqlInstanceError>;
    fn qualification_contain(&mut self) -> Result<(), MysqlInstanceError>;
}

impl QualificationOwnedMember for MysqlInstanceManager {
    fn qualification_stop(
        &mut self,
        kind: QualificationStopKind,
    ) -> Result<(), MysqlInstanceError> {
        self.qualification_stop_preserving_roots(kind)
    }

    fn qualification_restart(&mut self) -> Result<(), MysqlInstanceError> {
        self.qualification_restart_retained()
    }

    fn qualification_contain(&mut self) -> Result<(), MysqlInstanceError> {
        self.contain()
    }
}

struct QualificationOwnership<'a, M: QualificationOwnedMember> {
    members: &'a mut [M; 3],
    phases: [QualificationMemberPhase; 3],
    work_cutoff: Instant,
    runtime_deadline: Instant,
    armed: bool,
}

impl<'a, M: QualificationOwnedMember> QualificationOwnership<'a, M> {
    fn new(members: &'a mut [M; 3], runtime_deadline: Instant) -> Result<Self, ()> {
        let work_cutoff = qualification_work_cutoff(runtime_deadline)?;
        Ok(Self::with_deadlines(members, work_cutoff, runtime_deadline))
    }

    fn with_deadlines(
        members: &'a mut [M; 3],
        work_cutoff: Instant,
        runtime_deadline: Instant,
    ) -> Self {
        Self {
            members,
            phases: [QualificationMemberPhase::Running; 3],
            work_cutoff,
            runtime_deadline,
            armed: true,
        }
    }

    fn require_work_budget(&self) -> Result<(), ()> {
        (Instant::now() < self.work_cutoff).then_some(()).ok_or(())
    }

    fn require_operation_budget(&self, duration: Duration) -> Result<(), ()> {
        Instant::now()
            .checked_add(duration)
            .filter(|deadline| *deadline <= self.work_cutoff)
            .map(|_| ())
            .ok_or(())
    }

    fn phase(&self, member: MysqlMemberIndex) -> QualificationMemberPhase {
        self.phases[member.as_usize()]
    }

    fn stop(
        &mut self,
        member: MysqlMemberIndex,
        kind: QualificationStopKind,
    ) -> Result<(), QualificationOperationFailure> {
        if self
            .require_operation_budget(Duration::from_secs(20))
            .is_err()
        {
            return Err(self.fail(MysqlInstanceError::Timeout(
                crate::service::LifecycleOperation::Shutdown,
            )));
        }
        if self.phase(member) != QualificationMemberPhase::Running {
            return Err(self.fail(MysqlInstanceError::InvalidState {
                expected: crate::service::MysqlInstanceState::Running,
                actual: crate::service::MysqlInstanceState::Initialized,
            }));
        }
        match self.members[member.as_usize()].qualification_stop(kind) {
            Ok(()) => {
                self.phases[member.as_usize()] = QualificationMemberPhase::ProcessStopped;
                Ok(())
            }
            Err(error) => Err(self.fail(error)),
        }
    }

    fn restart(&mut self, member: MysqlMemberIndex) -> Result<(), QualificationOperationFailure> {
        if self
            .require_operation_budget(Duration::from_secs(30))
            .is_err()
        {
            return Err(self.fail(MysqlInstanceError::Timeout(
                crate::service::LifecycleOperation::Startup,
            )));
        }
        if self.phase(member) != QualificationMemberPhase::ProcessStopped {
            return Err(self.fail(MysqlInstanceError::InvalidState {
                expected: crate::service::MysqlInstanceState::Initialized,
                actual: crate::service::MysqlInstanceState::Running,
            }));
        }
        self.phases[member.as_usize()] = QualificationMemberPhase::Restarting;
        match self.members[member.as_usize()].qualification_restart() {
            Ok(()) => Ok(()),
            Err(error) => Err(self.fail(error)),
        }
    }

    fn finish_restart(&mut self, member: MysqlMemberIndex) -> Result<(), ()> {
        if self.phase(member) != QualificationMemberPhase::Restarting {
            return Err(());
        }
        self.phases[member.as_usize()] = QualificationMemberPhase::Running;
        Ok(())
    }

    fn reject_restart_binding(
        &mut self,
        member: MysqlMemberIndex,
    ) -> Result<QualificationCleanupReport, ()> {
        if self.phase(member) != QualificationMemberPhase::Restarting {
            return Err(());
        }
        Ok(self.contain())
    }

    fn begin_rejoin(
        &mut self,
        member: MysqlMemberIndex,
    ) -> Result<QualificationOperationGuard<'_, 'a, M>, QualificationOperationFailure> {
        if self.require_work_budget().is_err() {
            return Err(self.fail(MysqlInstanceError::Timeout(
                crate::service::LifecycleOperation::Startup,
            )));
        }
        if self.phase(member) != QualificationMemberPhase::Running {
            return Err(self.fail(MysqlInstanceError::InvalidState {
                expected: crate::service::MysqlInstanceState::Running,
                actual: crate::service::MysqlInstanceState::Initialized,
            }));
        }
        self.phases[member.as_usize()] = QualificationMemberPhase::Rejoining;
        Ok(QualificationOperationGuard {
            ownership: self,
            member,
            armed: true,
        })
    }

    fn fail(&mut self, primary: MysqlInstanceError) -> QualificationOperationFailure {
        QualificationOperationFailure {
            primary,
            cleanup: self.contain(),
        }
    }

    fn contain(&mut self) -> QualificationCleanupReport {
        let mut failures = Vec::new();
        for member in MysqlMemberIndex::all() {
            if let Err(error) = self.members[member.as_usize()].qualification_contain() {
                failures.push(MemberCleanupFailure::new(member, error));
                self.phases[member.as_usize()] = QualificationMemberPhase::ContainmentFailed;
            } else {
                self.phases[member.as_usize()] = QualificationMemberPhase::Contained;
            }
        }
        self.armed = false;
        QualificationCleanupReport {
            failures,
            deadline_overrun: Instant::now() > self.runtime_deadline,
        }
    }
}

fn qualification_work_cutoff(runtime_deadline: Instant) -> Result<Instant, ()> {
    let work_cutoff = runtime_deadline.checked_sub(CLEANUP_RESERVE).ok_or(())?;
    (Instant::now() < work_cutoff)
        .then_some(work_cutoff)
        .ok_or(())
}

impl<M: QualificationOwnedMember> Drop for QualificationOwnership<'_, M> {
    fn drop(&mut self) {
        if self.armed {
            let _report = self.contain();
        }
    }
}

struct QualificationOperationGuard<'guard, 'members, M: QualificationOwnedMember> {
    ownership: &'guard mut QualificationOwnership<'members, M>,
    member: MysqlMemberIndex,
    armed: bool,
}

impl<M: QualificationOwnedMember> QualificationOperationGuard<'_, '_, M> {
    fn complete(mut self) {
        self.ownership.phases[self.member.as_usize()] = QualificationMemberPhase::Running;
        self.armed = false;
    }
}

impl<M: QualificationOwnedMember> Drop for QualificationOperationGuard<'_, '_, M> {
    fn drop(&mut self) {
        if self.armed {
            let _report = self.ownership.contain();
        }
    }
}

#[allow(dead_code)]
struct MysqlNativeQualification<'a> {
    ownership: QualificationOwnership<'a, MysqlInstanceManager>,
    attempt: &'a crate::service::TopologyAttempt,
    observer: &'a crate::service::ControlCredential,
    recovery: &'a crate::service::ControlCredential,
    native_deadline: NativeControlDeadline,
    admitted_credit: TransitionCredit,
}

#[allow(dead_code)]
impl<'a> MysqlNativeQualification<'a> {
    fn enter(manager: &'a mut MysqlTopologyManager) -> Result<Self, MysqlTopologyManagerError> {
        let QualificationManagerParts {
            instances,
            attempt,
            observer,
            recovery,
            runtime_deadline,
            admitted_credit,
        } = manager.enter_native_qualification()?;
        let work_cutoff = match qualification_work_cutoff(runtime_deadline) {
            Ok(work_cutoff) => work_cutoff,
            Err(()) => {
                let mut failures = Vec::new();
                for member in MysqlMemberIndex::all() {
                    if let Err(error) = instances[member.as_usize()].contain() {
                        failures.push(MemberCleanupFailure::new(member, error));
                    }
                }
                let primary = MysqlTopologyManagerError::Topology(MysqlTopologyError::Deadline(
                    ControlStage::ProductValidation,
                ));
                return if failures.is_empty() {
                    Err(primary)
                } else {
                    Err(MysqlTopologyManagerError::Cleanup {
                        primary: Some(Box::new(primary)),
                        failures,
                    })
                };
            }
        };
        let ownership =
            QualificationOwnership::with_deadlines(instances, work_cutoff, runtime_deadline);
        let native_deadline = NativeControlDeadline::new(attempt.id().clone(), work_cutoff)
            .map_err(MysqlTopologyManagerError::Topology)?;
        Ok(Self {
            ownership,
            attempt,
            observer,
            recovery,
            native_deadline,
            admitted_credit,
        })
    }

    fn stop_member(
        &mut self,
        member: MysqlMemberIndex,
        kind: QualificationStopKind,
    ) -> Result<(), MysqlTopologyManagerError> {
        self.ownership
            .stop(member, kind)
            .map_err(|failure| Self::qualification_operation_error(member, failure))
    }

    fn restart_member(
        &mut self,
        member: MysqlMemberIndex,
    ) -> Result<MemberControlBinding, MysqlTopologyManagerError> {
        self.ownership
            .restart(member)
            .map_err(|failure| Self::qualification_operation_error(member, failure))?;
        let binding = self.ownership.members[member.as_usize()]
            .topology_binding(
                self.attempt.id().clone(),
                self.observer.generation().clone(),
                self.recovery.generation().clone(),
            )
            .map_err(MysqlTopologyManagerError::Topology);
        match binding {
            Ok(binding) => {
                self.ownership
                    .finish_restart(member)
                    .expect("successful retained restart remains in restarting phase");
                Ok(binding)
            }
            Err(primary) => {
                let cleanup = self
                    .ownership
                    .reject_restart_binding(member)
                    .expect("failed binding follows a retained restart");
                let failures = Self::qualification_cleanup_failures(member, cleanup);
                if failures.is_empty() {
                    Err(primary)
                } else {
                    Err(MysqlTopologyManagerError::Cleanup {
                        primary: Some(Box::new(primary)),
                        failures,
                    })
                }
            }
        }
    }

    fn qualification_operation_error(
        member: MysqlMemberIndex,
        failure: QualificationOperationFailure,
    ) -> MysqlTopologyManagerError {
        let primary = MysqlTopologyManagerError::Instance {
            member,
            error: failure.primary,
        };
        let failures = Self::qualification_cleanup_failures(member, failure.cleanup);
        if failures.is_empty() {
            primary
        } else {
            MysqlTopologyManagerError::Cleanup {
                primary: Some(Box::new(primary)),
                failures,
            }
        }
    }

    fn qualification_cleanup_failures(
        member: MysqlMemberIndex,
        mut cleanup: QualificationCleanupReport,
    ) -> Vec<MemberCleanupFailure> {
        if cleanup.deadline_overrun {
            cleanup.failures.push(MemberCleanupFailure::new(
                member,
                MysqlInstanceError::Timeout(crate::service::LifecycleOperation::Shutdown),
            ));
        }
        cleanup.failures
    }

    fn phase(&self, member: MysqlMemberIndex) -> QualificationMemberPhase {
        self.ownership.phase(member)
    }

    fn finalize(mut self) -> Result<(), String> {
        let cleanup = self.ownership.contain();
        if cleanup.failures.is_empty() && !cleanup.deadline_overrun {
            Ok(())
        } else {
            Err(format!(
                "qualification cleanup failed: {} member failures, deadline_overrun={}",
                cleanup.failures.len(),
                cleanup.deadline_overrun
            ))
        }
    }

    async fn qualify_profile_gtid_transitions(&mut self) -> QualificationLiveOutcome {
        let deadline = tokio::time::Instant::from_std(self.ownership.work_cutoff);
        match tokio::time::timeout_at(deadline, self.qualify_profile_gtid_transitions_inner()).await
        {
            Ok(Ok(report)) => QualificationLiveOutcome::Qualified(Box::new(report)),
            Ok(Err(QualificationFailure::Unsupported { stage, reason })) => {
                QualificationLiveOutcome::Unsupported { stage, reason }
            }
            Ok(Err(QualificationFailure::MalformedOrAbsent { stage, reason })) => {
                QualificationLiveOutcome::MalformedOrAbsent { stage, reason }
            }
            Ok(Err(QualificationFailure::Incompatible { stage, reason })) => {
                QualificationLiveOutcome::Incompatible { stage, reason }
            }
            Ok(Err(QualificationFailure::OperationallyUnproved { stage, reason })) => {
                QualificationLiveOutcome::Unproved { stage, reason }
            }
            Err(_) => QualificationLiveOutcome::Unproved {
                stage: QualificationStage::Cleanup,
                reason: "profile/GTID qualification reached scenario work cutoff".to_owned(),
            },
        }
    }

    async fn qualify_profile_gtid_transitions_inner(
        &mut self,
    ) -> Result<ProfileGtidQualificationReport, QualificationFailure> {
        let profile = self.read_profile_evidence().await?;
        let owned_identities: [String; 3] = profile
            .members
            .iter()
            .map(|member| member.server_uuid.clone())
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| "profile member cardinality changed".to_owned())?;
        let expected = profile.binding.clone();
        let qualified_profile = profile
            .qualify(&expected)
            .map_err(QualificationFailure::from_profile_error)?;

        self.prepare_transaction_table().await?;
        let mut checkpoints = Vec::new();
        checkpoints.push(self.commit_token("before-primary-transfer").await?);
        self.transfer_primary(&owned_identities).await?;
        checkpoints.push(self.commit_token("after-primary-transfer").await?);

        let primary = self.current_primary().await?;
        let departure = MysqlMemberIndex::all()
            .into_iter()
            .find(|member| *member != primary)
            .expect("three-member topology has a secondary");
        checkpoints.push(self.commit_token("before-secondary-departure").await?);
        let opening_view = self.snapshots().await?[0].view_id.clone();
        self.stop_group_replication(departure).await?;
        let expected_without = owned_identities
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != departure.as_usize())
            .map(|(_, identity)| identity.clone())
            .collect::<HashSet<_>>();
        let departure_view = self
            .wait_exact_membership(
                &expected_without,
                &owned_identities,
                None,
                Some(&opening_view),
            )
            .await?
            .view_id;
        self.start_group_replication(departure).await?;
        self.wait_exact_membership(
            &owned_identities.iter().cloned().collect(),
            &owned_identities,
            None,
            Some(&departure_view),
        )
        .await?;
        checkpoints.push(self.commit_token("after-secondary-rejoin").await?);

        let mut restart_bindings = Vec::new();
        for member in MysqlMemberIndex::all() {
            checkpoints.push(
                self.commit_token(&format!("before-member-{}-restart", member.as_usize() + 1))
                    .await?,
            );
            if self.current_primary().await? == member {
                self.transfer_primary(&owned_identities).await?;
            }
            let opening_view = self.snapshots().await?[0].view_id.clone();
            self.stop_group_replication(member).await?;
            let expected_without = owned_identities
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != member.as_usize())
                .map(|(_, identity)| identity.clone())
                .collect::<HashSet<_>>();
            let departure_view = self
                .wait_exact_membership(
                    &expected_without,
                    &owned_identities,
                    None,
                    Some(&opening_view),
                )
                .await?
                .view_id;
            let old = self.member_binding(member)?;
            self.stop_member(member, QualificationStopKind::Graceful)
                .map_err(|error| format!("owned stop failed: {error}"))?;
            let new = self
                .restart_member(member)
                .map_err(|error| format!("retained restart failed: {error}"))?;
            if old.process_session() == new.process_session() {
                return Err("retained restart reused a process session"
                    .to_owned()
                    .into());
            }
            restart_bindings.push((
                old.process_session().as_str().to_owned(),
                new.process_session().as_str().to_owned(),
            ));
            self.start_group_replication(member).await?;
            self.wait_exact_membership(
                &owned_identities.iter().cloned().collect(),
                &owned_identities,
                None,
                Some(&departure_view),
            )
            .await?;
            let rebound_profile = self.read_profile_evidence().await?;
            let rebound_expected = rebound_profile.binding.clone();
            rebound_profile
                .qualify(&rebound_expected)
                .map_err(QualificationFailure::from_profile_error)?;
            checkpoints.push(
                self.commit_token(&format!("after-member-{}-restart", member.as_usize() + 1))
                    .await?,
            );
        }

        checkpoints.push(self.commit_token("before-purge").await?);
        let (purged, binary_logs) = self.rotate_and_purge_binary_logs().await?;
        checkpoints.push(self.commit_token("after-purge").await?);
        let final_snapshots = self.snapshots().await?;
        for pair in checkpoints.windows(2) {
            for member in MysqlMemberIndex::all() {
                if !pair[0].snapshots[member.as_usize()]
                    .executed
                    .is_subset_of(&pair[1].snapshots[member.as_usize()].executed)
                {
                    return Err(QualificationFailure::Incompatible {
                        stage: QualificationStage::GtidCheckpoint,
                        reason: format!(
                            "checkpoint {} lost predecessor history at member {}",
                            pair[1].label,
                            member.as_usize() + 1
                        ),
                    });
                }
            }
        }
        for snapshot in &final_snapshots {
            if !purged.is_subset_of(&snapshot.executed) {
                return Err(QualificationFailure::Incompatible {
                    stage: QualificationStage::RetainedHistory,
                    reason: "purged history was outside executed history".to_owned(),
                });
            }
            match classify_gtid_profile(&snapshot.executed, self.attempt.group_name().as_str()) {
                GtidProfileCompatibility::CompatibleContiguous { .. } => {}
                compatibility => {
                    return Err(QualificationFailure::Incompatible {
                        stage: QualificationStage::GtidCheckpoint,
                        reason: format!("final GTID history was incompatible: {compatibility:?}"),
                    });
                }
            }
        }

        Ok(ProfileGtidQualificationReport {
            qualified_profile,
            checkpoints,
            restart_bindings,
            purged,
            binary_logs,
            final_pids: MysqlMemberIndex::all().map(|member| {
                self.ownership.members[member.as_usize()]
                    .qualification_child_pid()
                    .expect("qualified running member retains a child")
            }),
            final_members: final_snapshots
                .iter()
                .flat_map(|snapshot| snapshot.members.iter())
                .map(|member| member.member_id.clone())
                .collect::<HashSet<_>>(),
        })
    }

    async fn read_profile_evidence(&mut self) -> Result<NativeProfileEvidence, String> {
        let snapshots = self.snapshots().await?;
        let opening_view = snapshots[0].view_id.clone();
        if snapshots.iter().any(|snapshot| {
            snapshot.view_id != opening_view || snapshot.members != snapshots[0].members
        }) {
            return Err("profile readback did not begin in one coherent view".to_owned());
        }
        let mut members = Vec::new();
        for member in MysqlMemberIndex::all() {
            let binding = self.member_binding(member)?;
            let admitted = self
                .admitted_credit
                .members()
                .iter()
                .find(|enrollment| enrollment.binding().member() == member)
                .ok_or_else(|| "admitted topology lacked an expected member".to_owned())?;
            if snapshots[member.as_usize()].server_uuid != admitted.server_uuid().as_str()
                || binding.attempt() != admitted.binding().attempt()
                || binding.endpoint() != admitted.binding().endpoint()
                || binding.storage() != admitted.binding().storage()
                || binding.member_address() != admitted.binding().member_address()
                || binding.observer_generation() != admitted.binding().observer_generation()
                || binding.recovery_generation() != admitted.binding().recovery_generation()
            {
                return Err(format!(
                    "member {} drifted from admitted native/storage binding",
                    member.as_usize() + 1
                ));
            }
            let mut connection = self.root_connection(member).await?;
            let mut settings = Vec::new();
            for spec in NATIVE_PROFILE {
                let global = match connection.query_first::<String, _>(spec.global_query).await {
                    Ok(Some(value)) => value,
                    Ok(None) => {
                        settings.push(SettingReadback::Missing {
                            variable: spec.variable.to_owned(),
                        });
                        continue;
                    }
                    Err(mysql_async::Error::Server(error)) if error.code == 1193 => {
                        settings.push(SettingReadback::Unsupported {
                            variable: spec.variable.to_owned(),
                        });
                        continue;
                    }
                    Err(error) => {
                        return Err(format!(
                            "setting readback operational failure for {}: {}",
                            spec.variable, error
                        ));
                    }
                };
                if global.is_empty() {
                    settings.push(SettingReadback::Malformed {
                        variable: spec.variable.to_owned(),
                    });
                    continue;
                }
                let session = match spec.session_query {
                    Some(query) => match connection.query_first::<String, _>(query).await {
                        Ok(Some(value)) => Some(value),
                        Ok(None) => {
                            settings.push(SettingReadback::Missing {
                                variable: spec.variable.to_owned(),
                            });
                            continue;
                        }
                        Err(mysql_async::Error::Server(error)) if error.code == 1193 => {
                            settings.push(SettingReadback::Unsupported {
                                variable: spec.variable.to_owned(),
                            });
                            continue;
                        }
                        Err(error) => {
                            return Err(format!(
                                "session setting readback operational failure for {}: {}",
                                spec.variable, error
                            ));
                        }
                    },
                    None => None,
                };
                if session.as_deref() == Some("") {
                    settings.push(SettingReadback::Malformed {
                        variable: spec.variable.to_owned(),
                    });
                    continue;
                }
                settings.push(SettingReadback::Value {
                    variable: spec.variable.to_owned(),
                    global,
                    session,
                });
            }
            connection
                .disconnect()
                .await
                .map_err(|_| "profile connection disconnect failed".to_owned())?;
            members.push(MemberProfileEvidence {
                server_uuid: snapshots[member.as_usize()].server_uuid.clone(),
                process_session: binding.process_session().as_str().to_owned(),
                settings,
            });
        }
        let closing = self.snapshots().await?;
        if closing.iter().zip(snapshots.iter()).any(|(after, before)| {
            after.server_uuid != before.server_uuid
                || after.view_id != before.view_id
                || after.members != before.members
        }) {
            return Err("profile evidence changed across its settings bracket".to_owned());
        }
        Ok(NativeProfileEvidence {
            binding: QualificationBinding {
                package: ORACLE_PACKAGE.to_owned(),
                attempt: self.attempt.id().as_str().to_owned(),
                group: self.attempt.group_name().as_str().to_owned(),
                opening_view,
            },
            members,
        })
    }

    fn member_binding(&mut self, member: MysqlMemberIndex) -> Result<MemberControlBinding, String> {
        self.ownership.members[member.as_usize()]
            .topology_binding(
                self.attempt.id().clone(),
                self.observer.generation().clone(),
                self.recovery.generation().clone(),
            )
            .map_err(|error| format!("member binding failed: {error}"))
    }

    async fn root_connection(&self, member: MysqlMemberIndex) -> Result<Conn, String> {
        root_connection(self.member_socket(member)).await
    }

    fn member_socket(&self, member: MysqlMemberIndex) -> &Path {
        self.ownership.members[member.as_usize()]
            .config()
            .runtime()
            .socket()
    }

    async fn snapshots(&mut self) -> Result<[NativeMemberSnapshot; 3], String> {
        let mut snapshots = Vec::new();
        for member in MysqlMemberIndex::all() {
            let process_session = self
                .member_binding(member)?
                .process_session()
                .as_str()
                .to_owned();
            snapshots.push(read_snapshot(self.member_socket(member), process_session).await?);
        }
        snapshots
            .try_into()
            .map_err(|_| "snapshot cardinality changed".to_owned())
    }

    async fn prepare_transaction_table(&mut self) -> Result<(), String> {
        let primary = self.current_primary().await?;
        let mut connection = self.root_connection(primary).await?;
        connection
            .query_drop("CREATE DATABASE IF NOT EXISTS kuberic_native_qualification")
            .await
            .map_err(|_| "qualification database creation failed".to_owned())?;
        connection
            .query_drop(
                "CREATE TABLE IF NOT EXISTS kuberic_native_qualification.tokens (\
                 token_name VARCHAR(96) PRIMARY KEY, token_value BIGINT NOT NULL) ENGINE=InnoDB",
            )
            .await
            .map_err(|_| "qualification table creation failed".to_owned())?;
        connection
            .disconnect()
            .await
            .map_err(|_| "transaction setup disconnect failed".to_owned())
    }

    async fn commit_token(&mut self, label: &str) -> Result<GtidCheckpoint, String> {
        self.ownership
            .require_work_budget()
            .map_err(|()| "scenario work cutoff reached".to_owned())?;
        let before = self.wait_common_tail(0).await?;
        let before_tail = compatible_common_tail(&before, self.attempt.group_name().as_str())?;
        let primary = current_primary_from(&before)?;
        let serial = NEXT_TRANSACTION.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let token = format!("{label}-{serial}");
        let mut connection = self.root_connection(primary).await?;
        let effective_consistency = connection
            .query_first::<String, _>(
                "SELECT CAST(@@SESSION.group_replication_consistency AS CHAR)",
            )
            .await
            .map_err(|_| "workload consistency readback failed".to_owned())?
            .ok_or_else(|| "workload consistency readback returned no row".to_owned())?;
        if effective_consistency != "AFTER" {
            return Err(format!(
                "workload session consistency was {effective_consistency}, not AFTER"
            ));
        }
        connection
            .exec_drop(
                "INSERT INTO kuberic_native_qualification.tokens(token_name, token_value) \
                 VALUES (?, ?)",
                (token.clone(), serial),
            )
            .await
            .map_err(|_| "qualification transaction failed".to_owned())?;
        connection
            .disconnect()
            .await
            .map_err(|_| "qualification transaction disconnect failed".to_owned())?;
        let after = self.wait_common_tail(before_tail + 1).await?;
        let tail = compatible_common_tail(&after, self.attempt.group_name().as_str())?;
        if tail != before_tail + 1 {
            return Err(format!(
                "transaction advanced GTID tail from {before_tail} to {tail}"
            ));
        }
        if after[0].view_id != before[0].view_id {
            return Err("transaction crossed a native view change".to_owned());
        }
        let binary_logs = self.binary_log_inventory(primary).await?;
        Ok(GtidCheckpoint {
            label: label.to_owned(),
            token,
            token_value: serial,
            before_tail,
            transaction_tail: tail,
            opening_view: before[0].view_id.clone(),
            view_id: after[0].view_id.clone(),
            members: after[0]
                .members
                .iter()
                .map(|member| member.member_id.clone())
                .collect(),
            snapshots: after,
            binary_logs,
        })
    }

    async fn binary_log_inventory(&self, member: MysqlMemberIndex) -> Result<Vec<String>, String> {
        let mut connection = self.root_connection(member).await?;
        let rows = connection
            .query::<mysql_async::Row, _>("SHOW BINARY LOGS")
            .await
            .map_err(|_| "binary-log inventory failed".to_owned())?;
        let inventory = decode_binary_log_inventory(&rows)?;
        connection
            .disconnect()
            .await
            .map_err(|_| "binary-log inventory disconnect failed".to_owned())?;
        Ok(inventory)
    }

    async fn wait_common_tail(
        &mut self,
        minimum_tail: u64,
    ) -> Result<[NativeMemberSnapshot; 3], String> {
        loop {
            self.ownership
                .require_work_budget()
                .map_err(|()| "GTID wait reached scenario cutoff".to_owned())?;
            let snapshots = self.snapshots().await?;
            if require_coherent_full_view(&snapshots).is_ok() {
                match compatible_common_tail(&snapshots, self.attempt.group_name().as_str()) {
                    Ok(tail) if tail >= minimum_tail => return Ok(snapshots),
                    Ok(_) => {}
                    Err(reason) if reason.contains("incompatible") => return Err(reason),
                    Err(_) => {}
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn current_primary(&mut self) -> Result<MysqlMemberIndex, String> {
        current_primary_from(&self.snapshots().await?)
    }

    async fn transfer_primary(&mut self, owned_identities: &[String; 3]) -> Result<(), String> {
        let snapshots = self.snapshots().await?;
        require_coherent_full_view(&snapshots)?;
        let opening_view = snapshots[0].view_id.clone();
        let current = current_primary_from(&snapshots)?;
        let target = MysqlMemberIndex::all()
            .into_iter()
            .find(|member| *member != current)
            .unwrap();
        let target_uuid = snapshots[target.as_usize()].server_uuid.clone();
        crate::core::ServerUuid::new(target_uuid.clone())
            .map_err(|_| "primary target UUID was malformed".to_owned())?;
        let mut connection = self.root_connection(current).await?;
        connection
            .query_drop(format!(
                "SELECT group_replication_set_as_primary('{target_uuid}')"
            ))
            .await
            .map_err(|error| format!("primary transfer function failed: {error}"))?;
        connection
            .disconnect()
            .await
            .map_err(|_| "primary transfer disconnect failed".to_owned())?;
        let accepted = self
            .wait_exact_membership(
                &owned_identities.iter().cloned().collect(),
                owned_identities,
                Some(&target_uuid),
                None,
            )
            .await?;
        if accepted.view_id != opening_view {
            return Err("primary transfer unexpectedly changed the native view".to_owned());
        }
        Ok(())
    }

    async fn stop_group_replication(&self, member: MysqlMemberIndex) -> Result<(), String> {
        let mut connection = self.root_connection(member).await?;
        connection
            .query_drop("STOP GROUP_REPLICATION")
            .await
            .map_err(|_| "Group Replication stop failed".to_owned())?;
        connection
            .disconnect()
            .await
            .map_err(|_| "Group Replication stop disconnect failed".to_owned())
    }

    async fn start_group_replication(&mut self, member: MysqlMemberIndex) -> Result<(), String> {
        self.start_group_replication_with(member, self.recovery)
            .await
    }

    async fn start_group_replication_with(
        &mut self,
        member: MysqlMemberIndex,
        recovery: &crate::service::ControlCredential,
    ) -> Result<(), String> {
        let guard = self
            .ownership
            .begin_rejoin(member)
            .map_err(|failure| format!("rejoin admission failed: {:?}", failure.primary))?;
        let result = guard.ownership.members[member.as_usize()]
            .qualification_start_group_replication(recovery, &self.native_deadline)
            .await;
        match result {
            Ok(()) => {
                guard.complete();
                Ok(())
            }
            Err(error) => Err(format!("Group Replication start failed: {error}")),
        }
    }

    async fn probe_group_replication_start(
        &mut self,
        member: MysqlMemberIndex,
        recovery: &crate::service::ControlCredential,
    ) -> Result<(), MysqlTopologyError> {
        self.ownership.members[member.as_usize()]
            .qualification_start_group_replication(recovery, &self.native_deadline)
            .await
    }

    async fn qualify_peer_revocation(
        &mut self,
        candidate: RevocationCandidate,
    ) -> QualificationPeerOutcome {
        let deadline = tokio::time::Instant::from_std(self.ownership.work_cutoff);
        match tokio::time::timeout_at(deadline, self.qualify_peer_revocation_inner(candidate)).await
        {
            Ok(Ok((evidence, expected))) => {
                let verdict = evidence.verdict(&expected);
                QualificationPeerOutcome::Qualified {
                    evidence: Box::new(evidence),
                    verdict,
                }
            }
            Ok(Err(reason)) if reason.contains("unsupported") => {
                QualificationPeerOutcome::Unsupported { candidate, reason }
            }
            Ok(Err(reason)) => QualificationPeerOutcome::Unproved { candidate, reason },
            Err(_) => QualificationPeerOutcome::Unproved {
                candidate,
                reason: "peer scenario reached scenario work cutoff".to_owned(),
            },
        }
    }

    async fn qualify_peer_revocation_inner(
        &mut self,
        candidate: RevocationCandidate,
    ) -> Result<(RevocationEvidence, RevocationExpectation), String> {
        let profile = self.read_profile_evidence().await?;
        let expected_profile = profile.binding.clone();
        profile
            .qualify(&expected_profile)
            .map_err(|error| format!("profile evidence rejected: {error:?}"))?;
        self.prepare_transaction_table().await?;
        self.commit_token("peer-baseline").await?;
        let owned_identities: [String; 3] = profile
            .members
            .iter()
            .map(|member| member.server_uuid.clone())
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| "profile member cardinality changed".to_owned())?;
        let primary = self.current_primary().await?;
        let target = MysqlMemberIndex::all()
            .into_iter()
            .find(|member| *member != primary)
            .unwrap();
        let target_identity = owned_identities[target.as_usize()].clone();
        let predecessor_binding = self.member_binding(target)?;
        let opening_view = self.snapshots().await?[0].view_id.clone();
        let replacement = ControlCredential::new(
            self.attempt.id().clone(),
            CredentialGeneration::new(format!(
                "peer-replacement-{}-{}",
                target.as_usize() + 1,
                NEXT_TRANSACTION.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ))
            .map_err(|_| "replacement credential generation was invalid".to_owned())?,
            crate::service::ControlCredentialRole::Recovery,
            self.recovery.username().to_owned(),
            format!(
                "KmqReplacement{}C5",
                NEXT_TRANSACTION.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ),
        )
        .map_err(|_| "replacement credential was invalid".to_owned())?;
        let expected = RevocationExpectation {
            binding: expected_profile.clone(),
            exact_member: target_identity.clone(),
            predecessor_process_session: predecessor_binding.process_session().as_str().to_owned(),
            predecessor_credential_generation: self.recovery.generation().as_str().to_owned(),
            replacement_credential_generation: replacement.generation().as_str().to_owned(),
        };

        if candidate == RevocationCandidate::StopAndRejoin {
            self.stop_group_replication(target).await?;
        } else {
            let mutation = match candidate {
                RevocationCandidate::AccountLock => QualificationRecoveryMutation::Lock,
                RevocationCandidate::CredentialReplacement => {
                    QualificationRecoveryMutation::Replace(&replacement)
                }
                RevocationCandidate::PrivilegeRemoval => QualificationRecoveryMutation::Revoke,
                RevocationCandidate::StopAndRejoin => unreachable!(),
            };
            self.ownership.members[target.as_usize()]
                .qualification_mutate_recovery(self.recovery, mutation, &self.native_deadline)
                .await
                .map_err(|error| format!("recovery mutation failed: {error}"))?;
            self.commit_token("peer-after-mutation").await?;
            self.stop_group_replication(target).await?;
        }

        let expected_without = owned_identities
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != target.as_usize())
            .map(|(_, identity)| identity.clone())
            .collect();
        let mut successor = self
            .wait_exact_membership(
                &expected_without,
                &owned_identities,
                None,
                Some(&opening_view),
            )
            .await?;
        if candidate == RevocationCandidate::StopAndRejoin {
            self.ownership.members[target.as_usize()]
                .qualification_mutate_recovery(
                    self.recovery,
                    QualificationRecoveryMutation::Replace(&replacement),
                    &self.native_deadline,
                )
                .await
                .map_err(|error| format!("recovery replacement failed: {error}"))?;
        }
        let new_login = self.probe_recovery_login(target, self.recovery).await;
        let old_recovery = match self
            .probe_group_replication_start(target, self.recovery)
            .await
        {
            Ok(()) => ProbeOutcome::Accepted,
            Err(
                MysqlTopologyError::ControlAuthentication(_)
                | MysqlTopologyError::ControlPermission(_),
            ) => ProbeOutcome::Rejected,
            Err(_) => ProbeOutcome::Unproved,
        };

        let replacement_admission = if candidate == RevocationCandidate::StopAndRejoin {
            if old_recovery == ProbeOutcome::Accepted {
                self.wait_exact_membership(
                    &owned_identities.iter().cloned().collect(),
                    &owned_identities,
                    None,
                    Some(&successor.view_id),
                )
                .await?;
                let rejoined_view = self.snapshots().await?[0].view_id.clone();
                self.stop_group_replication(target).await?;
                successor = self
                    .wait_exact_membership(
                        &expected_without,
                        &owned_identities,
                        None,
                        Some(&rejoined_view),
                    )
                    .await?;
            }
            self.start_group_replication_with(target, &replacement)
                .await?;
            self.wait_exact_membership(
                &owned_identities.iter().cloned().collect(),
                &owned_identities,
                None,
                Some(&successor.view_id),
            )
            .await?;
            ProbeOutcome::Accepted
        } else {
            ProbeOutcome::Unproved
        };

        let evidence = RevocationEvidence {
            candidate,
            binding: expected_profile,
            exact_member: target_identity,
            predecessor_process_session: predecessor_binding.process_session().as_str().to_owned(),
            predecessor_credential_generation: self.recovery.generation().as_str().to_owned(),
            replacement_credential_generation: replacement.generation().as_str().to_owned(),
            opening_view,
            successor_view: Some(successor.view_id),
            new_login,
            start_command: old_recovery,
            recovery_authentication: ProbeOutcome::Unproved,
            established_participation: if candidate == RevocationCandidate::StopAndRejoin {
                ProbeOutcome::Absent
            } else {
                ProbeOutcome::Continued
            },
            predecessor_absence: ProbeOutcome::Absent,
            absence_observed_before_replacement: candidate == RevocationCandidate::StopAndRejoin,
            replacement_admission,
            credential_bound_session_identity: false,
        };
        Ok((evidence, expected))
    }

    async fn probe_recovery_login(
        &self,
        member: MysqlMemberIndex,
        credential: &crate::service::ControlCredential,
    ) -> ProbeOutcome {
        let options = OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(1)
            .socket(Some(
                self.member_socket(member).to_str().unwrap().to_owned(),
            ))
            .user(Some(credential.username().to_owned()))
            .pass(Some(credential.password().to_owned()));
        match Conn::new(options).await {
            Ok(connection) => {
                let _ = connection.disconnect().await;
                ProbeOutcome::Accepted
            }
            Err(mysql_async::Error::Server(error)) if matches!(error.code, 1045 | 3118 | 3119) => {
                ProbeOutcome::Rejected
            }
            Err(_) => ProbeOutcome::Unproved,
        }
    }

    async fn qualify_normal_acknowledgement(&mut self) -> QualificationTransactionOutcome {
        self.run_transaction_scenario(false).await
    }

    async fn qualify_ambiguous_disconnect(&mut self) -> QualificationTransactionOutcome {
        self.run_transaction_scenario(true).await
    }

    async fn run_transaction_scenario(
        &mut self,
        disconnect_after_barrier: bool,
    ) -> QualificationTransactionOutcome {
        let deadline = tokio::time::Instant::from_std(self.ownership.work_cutoff);
        match tokio::time::timeout_at(
            deadline,
            self.run_transaction_scenario_inner(disconnect_after_barrier),
        )
        .await
        {
            Ok(Ok(report)) => QualificationTransactionOutcome::Qualified(Box::new(report)),
            Ok(Err(reason)) if reason.contains("unsupported") => {
                QualificationTransactionOutcome::Unsupported { reason }
            }
            Ok(Err(reason)) => QualificationTransactionOutcome::Unproved { reason },
            Err(_) => QualificationTransactionOutcome::Unproved {
                reason: "transaction scenario reached scenario work cutoff".to_owned(),
            },
        }
    }

    async fn run_transaction_scenario_inner(
        &mut self,
        disconnect_after_barrier: bool,
    ) -> Result<TransactionQualificationReport, String> {
        let profile = self.read_profile_evidence().await?;
        let expected_profile = profile.binding.clone();
        profile
            .qualify(&expected_profile)
            .map_err(|error| format!("profile evidence rejected: {error:?}"))?;
        self.prepare_transaction_table().await?;

        if !disconnect_after_barrier {
            let checkpoint = self.commit_token("normal-acknowledgement").await?;
            let exact_gtid = GtidSet::from_str(&format!(
                "{}:{}",
                self.attempt.group_name().as_str(),
                checkpoint.transaction_tail
            ))
            .map_err(|error| format!("normal GTID construction failed: {error}"))?;
            return Ok(TransactionQualificationReport {
                binding: expected_profile,
                outcome: TransactionOutcomeEvidence {
                    client: ClientOutcome::Acknowledged,
                    exact_gtid_bound: true,
                    inclusion: InclusionEvidence::PresentOnEveryRequiredMember,
                },
                exact_gtid,
                opening_view: checkpoint.opening_view,
                closing_view: checkpoint.view_id,
                before_tail: checkpoint.before_tail,
                after_tail: checkpoint.transaction_tail,
                snapshots: checkpoint.snapshots,
                applier_barrier: None,
            });
        }

        let baseline = self.commit_token("ambiguous-lock-row").await?;
        let primary = self.current_primary().await?;
        let secondary = MysqlMemberIndex::all()
            .into_iter()
            .find(|member| *member != primary)
            .unwrap();
        let lock_token = baseline.token.clone();
        let mut lock_connection = self.root_connection(secondary).await?;
        lock_connection
            .query_drop("SET GLOBAL super_read_only = OFF")
            .await
            .map_err(|_| "secondary super-read-only release failed".to_owned())?;
        lock_connection
            .query_drop("SET GLOBAL read_only = OFF")
            .await
            .map_err(|_| "secondary read-only release failed".to_owned())?;
        lock_connection
            .query_drop("LOCK TABLES kuberic_native_qualification.tokens WRITE")
            .await
            .map_err(|_| "secondary table lock failed".to_owned())?;
        let locked_value = lock_connection
            .exec_first::<u64, _, _>(
                "SELECT token_value FROM kuberic_native_qualification.tokens \
                         WHERE token_name = ?",
                (lock_token.clone(),),
            )
            .await
            .map_err(|_| "secondary locked-row read failed".to_owned())?
            .ok_or_else(|| "secondary locked-row read found no exact token".to_owned())?;
        if locked_value != baseline.token_value {
            restore_secondary_blocker(&mut lock_connection).await?;
            return Err("secondary locked-row read found an unexpected token value".to_owned());
        }

        let mut workload = self.root_connection(primary).await?;
        let effective = workload
            .query_first::<String, _>(
                "SELECT CAST(@@SESSION.group_replication_consistency AS CHAR)",
            )
            .await
            .map_err(|_| "workload consistency readback failed".to_owned())?
            .ok_or_else(|| "workload consistency readback returned no row".to_owned())?;
        if effective != "AFTER" {
            return Err(format!("workload session consistency was {effective}"));
        }
        let update_token = lock_token.clone();
        let update = tokio::spawn(async move {
            let result = workload
                .exec_drop(
                    "UPDATE kuberic_native_qualification.tokens \
                             SET token_value = token_value + 1 WHERE token_name = ?",
                    (update_token,),
                )
                .await;
            (workload, result)
        });
        let mut update = AbortWorkloadOnDrop::new(update);

        let applying_gtid = match self
            .wait_exact_applier_gtid(secondary, baseline.transaction_tail + 1)
            .await
        {
            Ok(gtid) => gtid,
            Err(error) => {
                restore_secondary_blocker(&mut lock_connection).await?;
                let (connection, result) = update.join().await?;
                let _ = connection.disconnect().await;
                result
                    .map_err(|_| "calibration transaction failed after lock release".to_owned())?;
                return Err(error);
            }
        };
        if update.is_finished() {
            restore_secondary_blocker(&mut lock_connection).await?;
            let (connection, _) = update.join().await?;
            let _ = connection.disconnect().await;
            return Err("client completed before the applier barrier".to_owned());
        }
        if !update.abort_and_join().await {
            restore_secondary_blocker(&mut lock_connection).await?;
            return Err("workload completed before cancellation could prove disconnect".to_owned());
        }
        restore_secondary_blocker(&mut lock_connection).await?;

        let after = self.wait_common_tail(baseline.transaction_tail + 1).await?;
        let after_tail = compatible_common_tail(&after, self.attempt.group_name().as_str())?;
        if after_tail != baseline.transaction_tail + 1 {
            return Err("ambiguous transaction produced an unexpected GTID delta".to_owned());
        }
        let closing_view = after[0].view_id.clone();
        if closing_view != baseline.view_id {
            return Err("ambiguous transaction crossed a native view change".to_owned());
        }
        let opening_view = baseline.view_id.clone();
        Ok(TransactionQualificationReport {
            binding: expected_profile,
            outcome: TransactionOutcomeEvidence {
                client: ClientOutcome::DisconnectedAfterDispatch,
                exact_gtid_bound: true,
                inclusion: InclusionEvidence::PresentOnEveryRequiredMember,
            },
            exact_gtid: applying_gtid.clone(),
            opening_view: opening_view.clone(),
            closing_view,
            before_tail: baseline.transaction_tail,
            after_tail,
            snapshots: after,
            applier_barrier: Some(ApplierBarrierEvidence {
                member: profile.members[secondary.as_usize()].server_uuid.clone(),
                process_session: self
                    .member_binding(secondary)?
                    .process_session()
                    .as_str()
                    .to_owned(),
                source_process_session: self
                    .member_binding(primary)?
                    .process_session()
                    .as_str()
                    .to_owned(),
                opening_view,
                applying_gtid,
            }),
        })
    }

    async fn wait_exact_applier_gtid(
        &self,
        member: MysqlMemberIndex,
        expected_sequence: u64,
    ) -> Result<GtidSet, String> {
        let barrier_deadline = Instant::now() + Duration::from_secs(10);
        loop {
            self.ownership
                .require_work_budget()
                .map_err(|()| "applier barrier reached scenario cutoff".to_owned())?;
            let applying =
                tokio::time::timeout_at(tokio::time::Instant::from_std(barrier_deadline), async {
                    let mut connection = self.root_connection(member).await?;
                    let applying = connection
                        .query_first::<String, _>(
                            "SELECT APPLYING_TRANSACTION \
                             FROM performance_schema.replication_applier_status_by_worker \
                             WHERE CHANNEL_NAME = 'group_replication_applier' \
                             AND APPLYING_TRANSACTION IS NOT NULL \
                             AND APPLYING_TRANSACTION <> '' LIMIT 1",
                        )
                        .await
                        .map_err(|_| "applier barrier query failed".to_owned())?;
                    let _ = connection.disconnect().await;
                    Ok::<_, String>(applying)
                })
                .await
                .map_err(|_| {
                    "authoritative applier GTID barrier was unavailable on Oracle 8.4.11".to_owned()
                })??;
            if let Some(text) = applying {
                return validate_applying_gtid(
                    &text,
                    self.attempt.group_name().as_str(),
                    expected_sequence,
                );
            }
            if Instant::now() >= barrier_deadline {
                return Err(
                    "authoritative applier GTID barrier was unavailable on Oracle 8.4.11"
                        .to_owned(),
                );
            }
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn qualify_secondary_loss_during_after(&mut self) -> SecondaryLossQualification {
        let profile = match self.read_profile_evidence().await {
            Ok(profile) => profile,
            Err(reason) => {
                return SecondaryLossQualification::Unproved {
                    reason,
                    fault_injected: false,
                };
            }
        };
        let expected = profile.binding.clone();
        if let Err(error) = profile.qualify(&expected) {
            return SecondaryLossQualification::Unproved {
                reason: format!("source-wait profile admission failed: {error:?}"),
                fault_injected: false,
            };
        }
        SecondaryLossQualification::Unproved {
            reason:
                "no authoritative current-transaction source AFTER wait surface was established"
                    .to_owned(),
            fault_injected: false,
        }
    }

    async fn qualify_post_ack_primary_crash_restart(&mut self) -> PrimaryCrashQualification {
        let deadline = tokio::time::Instant::from_std(self.ownership.work_cutoff);
        match tokio::time::timeout_at(deadline, self.primary_crash_restart_inner()).await {
            Ok(Ok(report)) => PrimaryCrashQualification::Qualified(Box::new(report)),
            Ok(Err(reason)) => PrimaryCrashQualification::Unproved { reason },
            Err(_) => PrimaryCrashQualification::Unproved {
                reason: "primary crash scenario reached scenario work cutoff".to_owned(),
            },
        }
    }

    async fn primary_crash_restart_inner(&mut self) -> Result<PrimaryCrashReport, String> {
        let profile = self.read_profile_evidence().await?;
        let binding = profile.binding.clone();
        profile
            .qualify(&binding)
            .map_err(|error| format!("profile evidence rejected: {error:?}"))?;
        let owned_identities: [String; 3] = profile
            .members
            .iter()
            .map(|member| member.server_uuid.clone())
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| "profile member cardinality changed".to_owned())?;
        self.prepare_transaction_table().await?;
        let acknowledged = self.commit_token("post-ack-primary-crash").await?;
        let exact_gtid = GtidSet::from_str(&format!(
            "{}:{}",
            self.attempt.group_name().as_str(),
            acknowledged.transaction_tail
        ))
        .map_err(|error| format!("acknowledged GTID construction failed: {error}"))?;
        if acknowledged
            .snapshots
            .iter()
            .any(|snapshot| !exact_gtid.is_subset_of(&snapshot.executed))
        {
            return Err("acknowledged GTID was not on all three members before crash".to_owned());
        }
        let crashed = current_primary_from(&acknowledged.snapshots)?;
        let crashed_identity = owned_identities[crashed.as_usize()].clone();
        let old_binding = self.member_binding(crashed)?;
        let killed_pid = self.ownership.members[crashed.as_usize()]
            .qualification_child_pid()
            .ok_or_else(|| "primary had no retained child before crash".to_owned())?;
        let socket_metadata = fs::symlink_metadata(self.member_socket(crashed))
            .map_err(|error| format!("primary socket identity could not be captured: {error}"))?;
        let socket_identity = (socket_metadata.dev(), socket_metadata.ino());
        let opening_view = acknowledged.view_id.clone();
        self.stop_member(crashed, QualificationStopKind::Abrupt)
            .map_err(|error| format!("abrupt primary termination failed: {error}"))?;
        let expected_without = owned_identities
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != crashed.as_usize())
            .map(|(_, identity)| identity.clone())
            .collect::<HashSet<_>>();
        let successor = self
            .wait_exact_membership(
                &expected_without,
                &owned_identities,
                None,
                Some(&opening_view),
            )
            .await?;
        if successor
            .members
            .iter()
            .any(|member| member.member_id == crashed_identity)
        {
            return Err("crashed primary remained in successor membership".to_owned());
        }
        let mut successor_histories = Vec::new();
        for member in MysqlMemberIndex::all() {
            if member == crashed {
                continue;
            }
            let process_session = self
                .member_binding(member)?
                .process_session()
                .as_str()
                .to_owned();
            let snapshot = read_snapshot(self.member_socket(member), process_session).await?;
            if snapshot.view_id != successor.view_id
                || snapshot.members != successor.members
                || !exact_gtid.is_subset_of(&snapshot.executed)
            {
                return Err(format!(
                    "successor member {} lacked coherent acknowledged history",
                    member.as_usize() + 1
                ));
            }
            successor_histories.push(snapshot.executed);
        }

        let new_binding = self
            .restart_member(crashed)
            .map_err(|error| format!("crashed primary restart failed: {error}"))?;
        if old_binding.process_session() == new_binding.process_session() {
            return Err("crashed primary reused its process session".to_owned());
        }
        self.start_group_replication(crashed).await?;
        self.wait_exact_membership(
            &owned_identities.iter().cloned().collect(),
            &owned_identities,
            None,
            Some(&successor.view_id),
        )
        .await?;
        let mut restart_bindings = vec![(
            old_binding.process_session().as_str().to_owned(),
            new_binding.process_session().as_str().to_owned(),
        )];

        for member in MysqlMemberIndex::all() {
            if member == crashed {
                continue;
            }
            if self.current_primary().await? == member {
                self.transfer_primary(&owned_identities).await?;
            }
            let opening = self.snapshots().await?[0].view_id.clone();
            self.stop_group_replication(member).await?;
            let expected_without = owned_identities
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != member.as_usize())
                .map(|(_, identity)| identity.clone())
                .collect::<HashSet<_>>();
            let departure_view = self
                .wait_exact_membership(&expected_without, &owned_identities, None, Some(&opening))
                .await?
                .view_id;
            let old = self.member_binding(member)?;
            self.stop_member(member, QualificationStopKind::Graceful)
                .map_err(|error| format!("participant stop failed: {error}"))?;
            let new = self
                .restart_member(member)
                .map_err(|error| format!("participant restart failed: {error}"))?;
            restart_bindings.push((
                old.process_session().as_str().to_owned(),
                new.process_session().as_str().to_owned(),
            ));
            self.start_group_replication(member).await?;
            self.wait_exact_membership(
                &owned_identities.iter().cloned().collect(),
                &owned_identities,
                None,
                Some(&departure_view),
            )
            .await?;
        }

        let final_profile = self.read_profile_evidence().await?;
        let final_binding = final_profile.binding.clone();
        final_profile
            .qualify(&final_binding)
            .map_err(|error| format!("post-restart profile evidence rejected: {error:?}"))?;
        let final_snapshots = self.wait_common_tail(acknowledged.transaction_tail).await?;
        if final_snapshots
            .iter()
            .any(|snapshot| !exact_gtid.is_subset_of(&snapshot.executed))
        {
            return Err("acknowledged GTID was lost after participant restarts".to_owned());
        }
        Ok(PrimaryCrashReport {
            binding,
            crashed_member: crashed_identity,
            acknowledged_gtid: exact_gtid,
            pre_crash_snapshots: acknowledged.snapshots,
            opening_view,
            successor_snapshot: successor,
            successor_histories,
            kill_evidence: AbruptKillEvidence {
                pid: killed_pid,
                socket_identity,
                signal: 9,
            },
            restart_bindings,
            final_snapshots,
            client_outcome: ClientOutcome::Acknowledged,
        })
    }

    async fn wait_exact_membership(
        &mut self,
        expected: &HashSet<String>,
        owned_identities: &[String; 3],
        expected_primary: Option<&str>,
        predecessor_view: Option<&str>,
    ) -> Result<NativeMemberSnapshot, String> {
        loop {
            self.ownership
                .require_work_budget()
                .map_err(|()| "membership wait reached scenario cutoff".to_owned())?;
            let mut accepted = Vec::new();
            for member in MysqlMemberIndex::all() {
                let Ok(binding) = self.member_binding(member) else {
                    continue;
                };
                let Ok(snapshot) = read_snapshot(
                    self.member_socket(member),
                    binding.process_session().as_str().to_owned(),
                )
                .await
                else {
                    continue;
                };
                let member_ids = snapshot
                    .members
                    .iter()
                    .map(|native| native.member_id.clone())
                    .collect::<HashSet<_>>();
                let exact_addresses = snapshot.members.iter().all(|native| {
                    MysqlMemberIndex::all().into_iter().any(|owned| {
                        let address = self.ownership.members[owned.as_usize()]
                            .config()
                            .member()
                            .sql_address();
                        native.member_id == owned_identities[owned.as_usize()]
                            && native.member_host == address.ip().to_string()
                            && native.member_port == address.port()
                    })
                });
                if member_ids == *expected
                    && exact_addresses
                    && snapshot
                        .members
                        .iter()
                        .all(|native| native.state == "ONLINE")
                    && snapshot
                        .members
                        .iter()
                        .filter(|native| native.role == "PRIMARY")
                        .count()
                        == 1
                    && predecessor_view
                        .is_none_or(|view| qualified_view_advanced(view, &snapshot.view_id))
                    && expected_primary.is_none_or(|identity| {
                        snapshot
                            .members
                            .iter()
                            .any(|native| native.member_id == identity && native.role == "PRIMARY")
                    })
                {
                    accepted.push(snapshot);
                }
            }
            if accepted.len() == expected.len()
                && accepted.iter().all(|snapshot| {
                    snapshot.view_id == accepted[0].view_id
                        && snapshot.members == accepted[0].members
                })
            {
                return Ok(accepted.remove(0));
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn rotate_and_purge_binary_logs(&mut self) -> Result<(GtidSet, Vec<String>), String> {
        let primary = self.current_primary().await?;
        let mut connection = self.root_connection(primary).await?;
        connection
            .query_drop("FLUSH BINARY LOGS")
            .await
            .map_err(|_| "first binary-log rotation failed".to_owned())?;
        connection
            .query_drop("FLUSH BINARY LOGS")
            .await
            .map_err(|_| "second binary-log rotation failed".to_owned())?;
        let rows = connection
            .query::<mysql_async::Row, _>("SHOW BINARY LOGS")
            .await
            .map_err(|_| "binary-log inventory failed".to_owned())?;
        let before = decode_binary_log_inventory(&rows)?;
        if before.len() < 2 {
            return Err("binary-log rotation produced no purge boundary".to_owned());
        }
        let retained = before.last().unwrap();
        if !retained
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err("binary-log name used an unsupported alphabet".to_owned());
        }
        connection
            .query_drop(format!("PURGE BINARY LOGS TO '{retained}'"))
            .await
            .map_err(|_| "binary-log purge failed".to_owned())?;
        let rows = connection
            .query::<mysql_async::Row, _>("SHOW BINARY LOGS")
            .await
            .map_err(|_| "post-purge binary-log inventory failed".to_owned())?;
        let after = decode_binary_log_inventory(&rows)?;
        if after.first() != Some(retained)
            || after
                .iter()
                .any(|name| before[..before.len() - 1].contains(name))
        {
            return Err("binary-log purge retained an unexpected inventory".to_owned());
        }

        let purged_text = connection
            .query_first::<String, _>("SELECT @@GLOBAL.gtid_purged")
            .await
            .map_err(|_| "purged GTID query failed".to_owned())?
            .ok_or_else(|| "purged GTID query returned no row".to_owned())?;
        connection
            .disconnect()
            .await
            .map_err(|_| "binary-log connection disconnect failed".to_owned())?;
        let purged = GtidSet::from_str(&purged_text)
            .map_err(|error| format!("purged GTID was malformed: {error}"))?;
        Ok((purged, after))
    }
}

fn qualified_view_advanced(predecessor: &str, successor: &str) -> bool {
    let Some((predecessor_fixed, predecessor_monotonic)) = predecessor.split_once(':') else {
        return false;
    };
    let Some((successor_fixed, successor_monotonic)) = successor.split_once(':') else {
        return false;
    };
    let fixed = predecessor_fixed.parse::<u64>().ok();
    let successor_fixed_value = successor_fixed.parse::<u64>().ok();
    let before = predecessor_monotonic.parse::<u32>().ok();
    let after = successor_monotonic.parse::<u32>().ok();
    predecessor_fixed == successor_fixed
        && fixed.is_some_and(|value| value.to_string() == predecessor_fixed)
        && successor_fixed_value.is_some_and(|value| value.to_string() == successor_fixed)
        && before.is_some_and(|value| value.to_string() == predecessor_monotonic)
        && after.is_some_and(|value| value.to_string() == successor_monotonic)
        && before
            .zip(after)
            .is_some_and(|(before, after)| before.checked_add(1) == Some(after))
}

fn validate_applying_gtid(
    text: &str,
    group_uuid: &str,
    expected_sequence: u64,
) -> Result<GtidSet, String> {
    let gtid =
        GtidSet::from_str(text).map_err(|error| format!("applying GTID was malformed: {error}"))?;
    if matches!(
        classify_gtid_profile(&gtid, group_uuid),
        GtidProfileCompatibility::DoesNotBeginAtOne
            | GtidProfileCompatibility::CompatibleContiguous { .. }
    ) && gtid.entries().len() == 1
        && gtid.entries()[0].intervals().len() == 1
        && gtid.entries()[0].intervals()[0].start() == expected_sequence
        && gtid.entries()[0].intervals()[0].end() == expected_sequence
    {
        Ok(gtid)
    } else {
        Err(format!("unexpected applying GTID: {text}"))
    }
}

fn decode_binary_log_inventory(rows: &[mysql_async::Row]) -> Result<Vec<String>, String> {
    rows.iter()
        .map(|row| {
            row.get::<String, _>(0)
                .filter(|name| {
                    !name.is_empty()
                        && name.bytes().all(|byte| {
                            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
                        })
                })
                .ok_or_else(|| "binary-log name was malformed".to_owned())
        })
        .collect()
}

async fn restore_secondary_blocker(connection: &mut Conn) -> Result<(), String> {
    connection
        .query_drop("UNLOCK TABLES")
        .await
        .map_err(|_| "secondary table unlock failed".to_owned())?;
    connection
        .query_drop("SET GLOBAL read_only = ON")
        .await
        .map_err(|_| "secondary read-only restoration failed".to_owned())?;
    connection
        .query_drop("SET GLOBAL super_read_only = ON")
        .await
        .map_err(|_| "secondary super-read-only restoration failed".to_owned())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeMemberRow {
    member_id: String,
    member_host: String,
    member_port: u16,
    role: String,
    state: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeMemberSnapshot {
    server_uuid: String,
    process_session: String,
    view_id: String,
    members: Vec<NativeMemberRow>,
    executed: GtidSet,
    purged: GtidSet,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GtidCheckpoint {
    label: String,
    token: String,
    token_value: u64,
    before_tail: u64,
    transaction_tail: u64,
    opening_view: String,
    view_id: String,
    members: HashSet<String>,
    snapshots: [NativeMemberSnapshot; 3],
    binary_logs: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProfileGtidQualificationReport {
    qualified_profile: QualifiedNativeProfile,
    checkpoints: Vec<GtidCheckpoint>,
    restart_bindings: Vec<(String, String)>,
    purged: GtidSet,
    binary_logs: Vec<String>,
    final_pids: [u32; 3],
    final_members: HashSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum QualificationLiveOutcome {
    Qualified(Box<ProfileGtidQualificationReport>),
    Unsupported {
        stage: QualificationStage,
        reason: String,
    },
    MalformedOrAbsent {
        stage: QualificationStage,
        reason: String,
    },
    Incompatible {
        stage: QualificationStage,
        reason: String,
    },
    Unproved {
        stage: QualificationStage,
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum QualificationFailure {
    Unsupported {
        stage: QualificationStage,
        reason: String,
    },
    MalformedOrAbsent {
        stage: QualificationStage,
        reason: String,
    },
    Incompatible {
        stage: QualificationStage,
        reason: String,
    },
    OperationallyUnproved {
        stage: QualificationStage,
        reason: String,
    },
}

impl QualificationFailure {
    fn from_profile_error(error: ProfileEvidenceError) -> Self {
        match error {
            ProfileEvidenceError::Missing { .. }
            | ProfileEvidenceError::Malformed { .. }
            | ProfileEvidenceError::WrongMemberCount => Self::MalformedOrAbsent {
                stage: QualificationStage::ProfileReadback,
                reason: format!("profile evidence was malformed or absent: {error:?}"),
            },
            ProfileEvidenceError::Unsupported { .. } => Self::Unsupported {
                stage: QualificationStage::ProfileReadback,
                reason: format!("profile surface was unsupported: {error:?}"),
            },
            ProfileEvidenceError::ProfileMatrixMismatch
            | ProfileEvidenceError::BindingMismatch
            | ProfileEvidenceError::DuplicateMember
            | ProfileEvidenceError::Mismatch { .. }
            | ProfileEvidenceError::UnexpectedSetting { .. } => Self::Incompatible {
                stage: QualificationStage::ProfileReadback,
                reason: format!("profile evidence was incompatible: {error:?}"),
            },
        }
    }
}

impl From<String> for QualificationFailure {
    fn from(reason: String) -> Self {
        if reason.contains("incompatible")
            || reason.contains("lost predecessor history")
            || reason.contains("unexpected GTID delta")
        {
            Self::Incompatible {
                stage: QualificationStage::GtidCheckpoint,
                reason,
            }
        } else if reason.contains("malformed") || reason.contains("returned no row") {
            Self::MalformedOrAbsent {
                stage: QualificationStage::GtidCheckpoint,
                reason,
            }
        } else if reason.contains("unsupported") || reason.contains("function failed") {
            Self::Unsupported {
                stage: QualificationStage::GtidCheckpoint,
                reason,
            }
        } else {
            Self::OperationallyUnproved {
                stage: QualificationStage::GtidCheckpoint,
                reason,
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum QualificationPeerOutcome {
    Qualified {
        evidence: Box<RevocationEvidence>,
        verdict: RevocationVerdict,
    },
    Unsupported {
        candidate: RevocationCandidate,
        reason: String,
    },
    Unproved {
        candidate: RevocationCandidate,
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ApplierBarrierEvidence {
    member: String,
    process_session: String,
    source_process_session: String,
    opening_view: String,
    applying_gtid: GtidSet,
}

struct AbortWorkloadOnDrop {
    handle: Option<tokio::task::JoinHandle<(Conn, Result<(), mysql_async::Error>)>>,
}

impl AbortWorkloadOnDrop {
    fn new(handle: tokio::task::JoinHandle<(Conn, Result<(), mysql_async::Error>)>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    fn is_finished(&self) -> bool {
        self.handle
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
    }

    async fn join(&mut self) -> Result<(Conn, Result<(), mysql_async::Error>), String> {
        let result = self
            .handle
            .as_mut()
            .expect("workload task is retained until joined")
            .await
            .map_err(|_| "workload task was cancelled unexpectedly".to_owned());
        self.handle.take();
        result
    }

    async fn abort_and_join(&mut self) -> bool {
        if let Some(handle) = self.handle.take() {
            handle.abort();
            return handle.await.is_err_and(|error| error.is_cancelled());
        }
        false
    }
}

impl Drop for AbortWorkloadOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TransactionQualificationReport {
    binding: QualificationBinding,
    outcome: TransactionOutcomeEvidence,
    exact_gtid: GtidSet,
    opening_view: String,
    closing_view: String,
    before_tail: u64,
    after_tail: u64,
    snapshots: [NativeMemberSnapshot; 3],
    applier_barrier: Option<ApplierBarrierEvidence>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum QualificationTransactionOutcome {
    Qualified(Box<TransactionQualificationReport>),
    Unsupported { reason: String },
    Unproved { reason: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SecondaryLossQualification {
    Unproved {
        reason: String,
        fault_injected: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PrimaryCrashReport {
    binding: QualificationBinding,
    crashed_member: String,
    acknowledged_gtid: GtidSet,
    pre_crash_snapshots: [NativeMemberSnapshot; 3],
    opening_view: String,
    successor_snapshot: NativeMemberSnapshot,
    successor_histories: Vec<GtidSet>,
    kill_evidence: AbruptKillEvidence,
    restart_bindings: Vec<(String, String)>,
    final_snapshots: [NativeMemberSnapshot; 3],
    client_outcome: ClientOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AbruptKillEvidence {
    pid: u32,
    socket_identity: (u64, u64),
    signal: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PrimaryCrashQualification {
    Qualified(Box<PrimaryCrashReport>),
    Unproved { reason: String },
}

async fn root_connection(socket: &Path) -> Result<Conn, String> {
    let options = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(1)
        .socket(Some(
            socket.to_str().ok_or("socket was not UTF-8")?.to_owned(),
        ))
        .user(Some("root".to_owned()));
    Conn::new(options)
        .await
        .map_err(|_| "root UDS connection failed".to_owned())
}

async fn read_snapshot(
    socket: &Path,
    process_session: String,
) -> Result<NativeMemberSnapshot, String> {
    let mut connection = root_connection(socket).await?;
    let server_uuid = connection
        .query_first::<String, _>("SELECT @@GLOBAL.server_uuid")
        .await
        .map_err(|_| "server UUID query failed".to_owned())?
        .ok_or_else(|| "server UUID query returned no row".to_owned())?;
    let view_id = connection
        .query_first::<String, _>(
            "SELECT VIEW_ID FROM performance_schema.replication_group_member_stats \
             WHERE MEMBER_ID = @@GLOBAL.server_uuid",
        )
        .await
        .map_err(|_| "view query failed".to_owned())?
        .ok_or_else(|| "view query returned no local row".to_owned())?;
    let members = connection
        .query_map(
            "SELECT MEMBER_ID, MEMBER_HOST, MEMBER_PORT, MEMBER_ROLE, MEMBER_STATE \
             FROM performance_schema.replication_group_members ORDER BY MEMBER_ID",
            |(member_id, member_host, member_port, role, state): (
                String,
                String,
                u16,
                String,
                String,
            )| NativeMemberRow {
                member_id,
                member_host,
                member_port,
                role,
                state,
            },
        )
        .await
        .map_err(|_| "membership query failed".to_owned())?;
    let executed = connection
        .query_first::<String, _>("SELECT @@GLOBAL.gtid_executed")
        .await
        .map_err(|_| "executed GTID query failed".to_owned())?
        .ok_or_else(|| "executed GTID query returned no row".to_owned())?;
    let purged = connection
        .query_first::<String, _>("SELECT @@GLOBAL.gtid_purged")
        .await
        .map_err(|_| "purged GTID query failed".to_owned())?
        .ok_or_else(|| "purged GTID query returned no row".to_owned())?;
    let closing_server_uuid = connection
        .query_first::<String, _>("SELECT @@GLOBAL.server_uuid")
        .await
        .map_err(|_| "closing server UUID query failed".to_owned())?
        .ok_or_else(|| "closing server UUID query returned no row".to_owned())?;
    let closing_view_id = connection
        .query_first::<String, _>(
            "SELECT VIEW_ID FROM performance_schema.replication_group_member_stats \
             WHERE MEMBER_ID = @@GLOBAL.server_uuid",
        )
        .await
        .map_err(|_| "closing view query failed".to_owned())?
        .ok_or_else(|| "closing view query returned no local row".to_owned())?;
    let closing_members = connection
        .query_map(
            "SELECT MEMBER_ID, MEMBER_HOST, MEMBER_PORT, MEMBER_ROLE, MEMBER_STATE \
             FROM performance_schema.replication_group_members ORDER BY MEMBER_ID",
            |(member_id, member_host, member_port, role, state): (
                String,
                String,
                u16,
                String,
                String,
            )| NativeMemberRow {
                member_id,
                member_host,
                member_port,
                role,
                state,
            },
        )
        .await
        .map_err(|_| "closing membership query failed".to_owned())?;
    if closing_server_uuid != server_uuid
        || closing_view_id != view_id
        || closing_members != members
    {
        return Err("native snapshot changed across its history bracket".to_owned());
    }
    connection
        .disconnect()
        .await
        .map_err(|_| "snapshot disconnect failed".to_owned())?;
    Ok(NativeMemberSnapshot {
        server_uuid,
        process_session,
        view_id,
        members,
        executed: GtidSet::from_str(&executed)
            .map_err(|error| format!("executed GTID was malformed: {error}"))?,
        purged: GtidSet::from_str(&purged)
            .map_err(|error| format!("purged GTID was malformed: {error}"))?,
    })
}

fn require_coherent_full_view(snapshots: &[NativeMemberSnapshot; 3]) -> Result<(), String> {
    let first = &snapshots[0];
    if first.members.len() != 3
        || first.members.iter().any(|member| member.state != "ONLINE")
        || snapshots.iter().any(|snapshot| {
            snapshot.view_id != first.view_id
                || snapshot.members != first.members
                || !snapshot
                    .members
                    .iter()
                    .any(|member| member.member_id == snapshot.server_uuid)
        })
    {
        return Err("native snapshots did not form one coherent online view".to_owned());
    }
    Ok(())
}

fn compatible_common_tail(
    snapshots: &[NativeMemberSnapshot; 3],
    group_uuid: &str,
) -> Result<u64, String> {
    let tails = snapshots
        .iter()
        .map(
            |snapshot| match classify_gtid_profile(&snapshot.executed, group_uuid) {
                GtidProfileCompatibility::CompatibleContiguous { tail } => Ok(tail),
                GtidProfileCompatibility::CompatibleEmpty => Ok(0),
                compatibility => Err(format!("GTID profile was incompatible: {compatibility:?}")),
            },
        )
        .collect::<Result<Vec<_>, _>>()?;
    if tails.iter().any(|tail| *tail != tails[0]) {
        return Err("members did not share one executed GTID tail".to_owned());
    }
    Ok(tails[0])
}

fn current_primary_from(snapshots: &[NativeMemberSnapshot; 3]) -> Result<MysqlMemberIndex, String> {
    require_coherent_full_view(snapshots)?;
    let primary = snapshots[0]
        .members
        .iter()
        .find(|member| member.role == "PRIMARY")
        .ok_or_else(|| "coherent view had no primary".to_owned())?;
    MysqlMemberIndex::all()
        .into_iter()
        .find(|member| snapshots[member.as_usize()].server_uuid == primary.member_id)
        .ok_or_else(|| "primary identity did not match an owned member".to_owned())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeSettingScope {
    Global,
    GlobalAndSession,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeSettingMutability {
    RestrictedDynamicGlobal,
    GroupReboot,
    ReadOnlyGroupWide,
    DynamicGlobal,
    DynamicGlobalAndSession,
    DynamicGroupWide,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NativeSettingSpec {
    option: &'static str,
    variable: &'static str,
    global_query: &'static str,
    session_query: Option<&'static str>,
    expected: &'static str,
    scope: NativeSettingScope,
    mutability: NativeSettingMutability,
}

const NATIVE_PROFILE: [NativeSettingSpec; 9] = [
    NativeSettingSpec {
        option: "gtid-mode=ON",
        variable: "gtid_mode",
        global_query: "SELECT CAST(@@GLOBAL.gtid_mode AS CHAR)",
        session_query: None,
        expected: "ON",
        scope: NativeSettingScope::Global,
        mutability: NativeSettingMutability::RestrictedDynamicGlobal,
    },
    NativeSettingSpec {
        option: "enforce-gtid-consistency=ON",
        variable: "enforce_gtid_consistency",
        global_query: "SELECT CAST(@@GLOBAL.enforce_gtid_consistency AS CHAR)",
        session_query: None,
        expected: "ON",
        scope: NativeSettingScope::Global,
        mutability: NativeSettingMutability::RestrictedDynamicGlobal,
    },
    NativeSettingSpec {
        option: "loose-group-replication-gtid-assignment-block-size=1",
        variable: "group_replication_gtid_assignment_block_size",
        global_query: "SELECT CAST(@@GLOBAL.group_replication_gtid_assignment_block_size AS CHAR)",
        session_query: None,
        expected: "1",
        scope: NativeSettingScope::Global,
        mutability: NativeSettingMutability::GroupReboot,
    },
    NativeSettingSpec {
        option: "loose-group-replication-view-change-uuid=AUTOMATIC",
        variable: "group_replication_view_change_uuid",
        global_query: "SELECT CAST(@@GLOBAL.group_replication_view_change_uuid AS CHAR)",
        session_query: None,
        expected: "AUTOMATIC",
        scope: NativeSettingScope::Global,
        mutability: NativeSettingMutability::ReadOnlyGroupWide,
    },
    NativeSettingSpec {
        option: "loose-group-replication-consistency=AFTER",
        variable: "group_replication_consistency",
        global_query: "SELECT CAST(@@GLOBAL.group_replication_consistency AS CHAR)",
        session_query: Some("SELECT CAST(@@SESSION.group_replication_consistency AS CHAR)"),
        expected: "AFTER",
        scope: NativeSettingScope::GlobalAndSession,
        mutability: NativeSettingMutability::DynamicGlobalAndSession,
    },
    NativeSettingSpec {
        option: "innodb-flush-log-at-trx-commit=1",
        variable: "innodb_flush_log_at_trx_commit",
        global_query: "SELECT CAST(@@GLOBAL.innodb_flush_log_at_trx_commit AS CHAR)",
        session_query: None,
        expected: "1",
        scope: NativeSettingScope::Global,
        mutability: NativeSettingMutability::DynamicGlobal,
    },
    NativeSettingSpec {
        option: "sync-binlog=1",
        variable: "sync_binlog",
        global_query: "SELECT CAST(@@GLOBAL.sync_binlog AS CHAR)",
        session_query: None,
        expected: "1",
        scope: NativeSettingScope::Global,
        mutability: NativeSettingMutability::DynamicGlobal,
    },
    NativeSettingSpec {
        option: "binlog-expire-logs-seconds=2592000",
        variable: "binlog_expire_logs_seconds",
        global_query: "SELECT CAST(@@GLOBAL.binlog_expire_logs_seconds AS CHAR)",
        session_query: None,
        expected: "2592000",
        scope: NativeSettingScope::Global,
        mutability: NativeSettingMutability::DynamicGlobal,
    },
    NativeSettingSpec {
        option: "loose-group-replication-member-expel-timeout=5",
        variable: "group_replication_member_expel_timeout",
        global_query: "SELECT CAST(@@GLOBAL.group_replication_member_expel_timeout AS CHAR)",
        session_query: None,
        expected: "5",
        scope: NativeSettingScope::Global,
        mutability: NativeSettingMutability::DynamicGroupWide,
    },
];

#[derive(Clone, Debug, Eq, PartialEq)]
struct QualificationBinding {
    package: String,
    attempt: String,
    group: String,
    opening_view: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SettingReadback {
    Value {
        variable: String,
        global: String,
        session: Option<String>,
    },
    Missing {
        variable: String,
    },
    Malformed {
        variable: String,
    },
    Unsupported {
        variable: String,
    },
}

impl SettingReadback {
    fn variable(&self) -> &str {
        match self {
            Self::Value { variable, .. }
            | Self::Missing { variable }
            | Self::Malformed { variable }
            | Self::Unsupported { variable } => variable,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MemberProfileEvidence {
    server_uuid: String,
    process_session: String,
    settings: Vec<SettingReadback>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeProfileEvidence {
    binding: QualificationBinding,
    members: Vec<MemberProfileEvidence>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QualifiedNativeProfile {
    binding: QualificationBinding,
    member_process_sessions: Vec<(String, String)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProfileEvidenceError {
    ProfileMatrixMismatch,
    BindingMismatch,
    WrongMemberCount,
    DuplicateMember,
    Missing { member: String, variable: String },
    Malformed { member: String, variable: String },
    Unsupported { member: String, variable: String },
    Mismatch { member: String, variable: String },
    UnexpectedSetting { member: String, variable: String },
}

impl NativeProfileEvidence {
    fn qualify(
        &self,
        expected: &QualificationBinding,
    ) -> Result<QualifiedNativeProfile, ProfileEvidenceError> {
        if !NATIVE_PROFILE
            .iter()
            .map(|setting| setting.option)
            .eq(TOPOLOGY_NATIVE_PROFILE_OPTIONS)
        {
            return Err(ProfileEvidenceError::ProfileMatrixMismatch);
        }
        if &self.binding != expected || self.binding.package != ORACLE_PACKAGE {
            return Err(ProfileEvidenceError::BindingMismatch);
        }
        if self.members.len() != 3 {
            return Err(ProfileEvidenceError::WrongMemberCount);
        }
        let unique_members = self
            .members
            .iter()
            .map(|member| member.server_uuid.as_str())
            .collect::<HashSet<_>>();
        let unique_sessions = self
            .members
            .iter()
            .map(|member| member.process_session.as_str())
            .collect::<HashSet<_>>();
        if unique_members.len() != 3 || unique_sessions.len() != 3 {
            return Err(ProfileEvidenceError::DuplicateMember);
        }

        for member in &self.members {
            let mut seen = HashSet::new();
            for readback in &member.settings {
                let variable = readback.variable();
                if !seen.insert(variable) {
                    return Err(ProfileEvidenceError::UnexpectedSetting {
                        member: member.server_uuid.clone(),
                        variable: variable.to_owned(),
                    });
                }
                let Some(spec) = NATIVE_PROFILE.iter().find(|spec| spec.variable == variable)
                else {
                    return Err(ProfileEvidenceError::UnexpectedSetting {
                        member: member.server_uuid.clone(),
                        variable: variable.to_owned(),
                    });
                };
                match readback {
                    SettingReadback::Value {
                        global, session, ..
                    } => {
                        let session_matches = match spec.scope {
                            NativeSettingScope::Global => session.is_none(),
                            NativeSettingScope::GlobalAndSession => {
                                session.as_deref() == Some(spec.expected)
                            }
                        };
                        if global != spec.expected || !session_matches {
                            return Err(ProfileEvidenceError::Mismatch {
                                member: member.server_uuid.clone(),
                                variable: variable.to_owned(),
                            });
                        }
                    }
                    SettingReadback::Missing { .. } => {
                        return Err(ProfileEvidenceError::Missing {
                            member: member.server_uuid.clone(),
                            variable: variable.to_owned(),
                        });
                    }
                    SettingReadback::Malformed { .. } => {
                        return Err(ProfileEvidenceError::Malformed {
                            member: member.server_uuid.clone(),
                            variable: variable.to_owned(),
                        });
                    }
                    SettingReadback::Unsupported { .. } => {
                        return Err(ProfileEvidenceError::Unsupported {
                            member: member.server_uuid.clone(),
                            variable: variable.to_owned(),
                        });
                    }
                }
            }
            if let Some(missing) = NATIVE_PROFILE
                .iter()
                .find(|spec| !seen.contains(spec.variable))
            {
                return Err(ProfileEvidenceError::Missing {
                    member: member.server_uuid.clone(),
                    variable: missing.variable.to_owned(),
                });
            }
        }

        Ok(QualifiedNativeProfile {
            binding: self.binding.clone(),
            member_process_sessions: self
                .members
                .iter()
                .map(|member| (member.server_uuid.clone(), member.process_session.clone()))
                .collect(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum GtidProfileCompatibility {
    CompatibleEmpty,
    CompatibleContiguous { tail: u64 },
    ForeignSource,
    TaggedSource,
    MultipleSources,
    MultipleIntervals,
    DoesNotBeginAtOne,
}

fn classify_gtid_profile(executed: &GtidSet, group_uuid: &str) -> GtidProfileCompatibility {
    let entries = executed.entries();
    if entries.is_empty() {
        return GtidProfileCompatibility::CompatibleEmpty;
    }
    if entries.len() != 1 {
        return GtidProfileCompatibility::MultipleSources;
    }
    let history = &entries[0];
    if history.source().server_uuid().as_str() != group_uuid {
        return GtidProfileCompatibility::ForeignSource;
    }
    if history.source().tag().is_some() {
        return GtidProfileCompatibility::TaggedSource;
    }
    if history.intervals().len() != 1 {
        return GtidProfileCompatibility::MultipleIntervals;
    }
    let interval = history.intervals()[0];
    if interval.start() != 1 {
        return GtidProfileCompatibility::DoesNotBeginAtOne;
    }
    GtidProfileCompatibility::CompatibleContiguous {
        tail: interval.end(),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RetainedHistoryEvidence {
    executed: GtidSet,
    purged: GtidSet,
    binary_logs: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RetainedHistoryError {
    PurgedOutsideExecuted,
    MissingInventory,
    MalformedInventory,
    DuplicateInventory,
}

impl RetainedHistoryEvidence {
    fn validate_opaque_inventory(&self) -> Result<(), RetainedHistoryError> {
        if !self.purged.is_subset_of(&self.executed) {
            return Err(RetainedHistoryError::PurgedOutsideExecuted);
        }
        if self.binary_logs.is_empty() {
            return Err(RetainedHistoryError::MissingInventory);
        }
        if self
            .binary_logs
            .iter()
            .any(|name| name.is_empty() || name.contains('/') || name.contains('\0'))
        {
            return Err(RetainedHistoryError::MalformedInventory);
        }
        if self.binary_logs.iter().collect::<HashSet<_>>().len() != self.binary_logs.len() {
            return Err(RetainedHistoryError::DuplicateInventory);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClientOutcome {
    Acknowledged,
    ServerRejected,
    DisconnectedAfterDispatch,
    DeadlineExpired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InclusionEvidence {
    PresentOnEveryRequiredMember,
    AbsentFromCoherentRequiredHistory,
    IncompleteOrIncoherent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TransactionOutcomeEvidence {
    client: ClientOutcome,
    exact_gtid_bound: bool,
    inclusion: InclusionEvidence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransactionVerdict {
    AcknowledgedDurable,
    AmbiguousIncluded,
    AmbiguousAbsent,
    AmbiguousUnresolved,
    Rejected,
    IncompatibleAcknowledgement,
}

impl TransactionOutcomeEvidence {
    fn verdict(self) -> TransactionVerdict {
        match (self.client, self.exact_gtid_bound, self.inclusion) {
            (
                ClientOutcome::Acknowledged,
                true,
                InclusionEvidence::PresentOnEveryRequiredMember,
            ) => TransactionVerdict::AcknowledgedDurable,
            (ClientOutcome::Acknowledged, _, _) => TransactionVerdict::IncompatibleAcknowledgement,
            (
                ClientOutcome::DisconnectedAfterDispatch | ClientOutcome::DeadlineExpired,
                true,
                InclusionEvidence::PresentOnEveryRequiredMember,
            ) => TransactionVerdict::AmbiguousIncluded,
            (
                ClientOutcome::DisconnectedAfterDispatch | ClientOutcome::DeadlineExpired,
                true,
                InclusionEvidence::AbsentFromCoherentRequiredHistory,
            ) => TransactionVerdict::AmbiguousAbsent,
            (ClientOutcome::DisconnectedAfterDispatch | ClientOutcome::DeadlineExpired, _, _) => {
                TransactionVerdict::AmbiguousUnresolved
            }
            (ClientOutcome::ServerRejected, _, _) => TransactionVerdict::Rejected,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RevocationCandidate {
    AccountLock,
    CredentialReplacement,
    PrivilegeRemoval,
    StopAndRejoin,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProbeOutcome {
    Accepted,
    Rejected,
    Continued,
    Absent,
    Unproved,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RevocationEvidence {
    candidate: RevocationCandidate,
    binding: QualificationBinding,
    exact_member: String,
    predecessor_process_session: String,
    predecessor_credential_generation: String,
    replacement_credential_generation: String,
    opening_view: String,
    successor_view: Option<String>,
    new_login: ProbeOutcome,
    start_command: ProbeOutcome,
    recovery_authentication: ProbeOutcome,
    established_participation: ProbeOutcome,
    predecessor_absence: ProbeOutcome,
    absence_observed_before_replacement: bool,
    replacement_admission: ProbeOutcome,
    credential_bound_session_identity: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RevocationExpectation {
    binding: QualificationBinding,
    exact_member: String,
    predecessor_process_session: String,
    predecessor_credential_generation: String,
    replacement_credential_generation: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RevocationVerdict {
    ExactProcessBarrier,
    Insufficient,
    SessionIdentityUnproved,
}

impl RevocationEvidence {
    fn verdict(&self, expected: &RevocationExpectation) -> RevocationVerdict {
        let exact_binding = self.binding == expected.binding
            && self.binding.package == ORACLE_PACKAGE
            && self.opening_view == self.binding.opening_view
            && self.exact_member == expected.exact_member
            && self.predecessor_process_session == expected.predecessor_process_session
            && self.predecessor_credential_generation == expected.predecessor_credential_generation
            && self.replacement_credential_generation == expected.replacement_credential_generation
            && self.predecessor_credential_generation != self.replacement_credential_generation
            && self
                .successor_view
                .as_ref()
                .is_some_and(|successor| successor != &self.opening_view);
        if self.candidate == RevocationCandidate::StopAndRejoin
            && exact_binding
            && self.recovery_authentication == ProbeOutcome::Rejected
            && self.established_participation == ProbeOutcome::Absent
            && self.predecessor_absence == ProbeOutcome::Absent
            && self.absence_observed_before_replacement
            && self.replacement_admission == ProbeOutcome::Accepted
        {
            RevocationVerdict::ExactProcessBarrier
        } else if !self.credential_bound_session_identity {
            RevocationVerdict::SessionIdentityUnproved
        } else {
            RevocationVerdict::Insufficient
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QualificationStage {
    ProfileReadback,
    GtidCheckpoint,
    RetainedHistory,
    Revocation,
    Transaction,
    Cleanup,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QualificationDiagnostic {
    stage: QualificationStage,
    member: Option<String>,
    category: &'static str,
}

#[cfg(test)]
mod live;
#[cfg(test)]
mod tests;
