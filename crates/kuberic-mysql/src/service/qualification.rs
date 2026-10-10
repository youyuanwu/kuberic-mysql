use std::collections::HashSet;

use crate::core::GtidSet;
use crate::service::config::TOPOLOGY_NATIVE_PROFILE_OPTIONS;

const ORACLE_PACKAGE: &str = "mysql-community-server-core=8.4.11-1ubuntu24.04";

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
        global_query: "SELECT @@GLOBAL.gtid_mode",
        session_query: None,
        expected: "ON",
        scope: NativeSettingScope::Global,
        mutability: NativeSettingMutability::RestrictedDynamicGlobal,
    },
    NativeSettingSpec {
        option: "enforce-gtid-consistency=ON",
        variable: "enforce_gtid_consistency",
        global_query: "SELECT @@GLOBAL.enforce_gtid_consistency",
        session_query: None,
        expected: "ON",
        scope: NativeSettingScope::Global,
        mutability: NativeSettingMutability::RestrictedDynamicGlobal,
    },
    NativeSettingSpec {
        option: "loose-group-replication-gtid-assignment-block-size=1",
        variable: "group_replication_gtid_assignment_block_size",
        global_query: "SELECT @@GLOBAL.group_replication_gtid_assignment_block_size",
        session_query: None,
        expected: "1",
        scope: NativeSettingScope::Global,
        mutability: NativeSettingMutability::GroupReboot,
    },
    NativeSettingSpec {
        option: "loose-group-replication-view-change-uuid=AUTOMATIC",
        variable: "group_replication_view_change_uuid",
        global_query: "SELECT @@GLOBAL.group_replication_view_change_uuid",
        session_query: None,
        expected: "AUTOMATIC",
        scope: NativeSettingScope::Global,
        mutability: NativeSettingMutability::ReadOnlyGroupWide,
    },
    NativeSettingSpec {
        option: "loose-group-replication-consistency=AFTER",
        variable: "group_replication_consistency",
        global_query: "SELECT @@GLOBAL.group_replication_consistency",
        session_query: Some("SELECT @@SESSION.group_replication_consistency"),
        expected: "AFTER",
        scope: NativeSettingScope::GlobalAndSession,
        mutability: NativeSettingMutability::DynamicGlobalAndSession,
    },
    NativeSettingSpec {
        option: "innodb-flush-log-at-trx-commit=1",
        variable: "innodb_flush_log_at_trx_commit",
        global_query: "SELECT @@GLOBAL.innodb_flush_log_at_trx_commit",
        session_query: None,
        expected: "1",
        scope: NativeSettingScope::Global,
        mutability: NativeSettingMutability::DynamicGlobal,
    },
    NativeSettingSpec {
        option: "sync-binlog=1",
        variable: "sync_binlog",
        global_query: "SELECT @@GLOBAL.sync_binlog",
        session_query: None,
        expected: "1",
        scope: NativeSettingScope::Global,
        mutability: NativeSettingMutability::DynamicGlobal,
    },
    NativeSettingSpec {
        option: "binlog-expire-logs-seconds=2592000",
        variable: "binlog_expire_logs_seconds",
        global_query: "SELECT @@GLOBAL.binlog_expire_logs_seconds",
        session_query: None,
        expected: "2592000",
        scope: NativeSettingScope::Global,
        mutability: NativeSettingMutability::DynamicGlobal,
    },
    NativeSettingSpec {
        option: "loose-group-replication-member-expel-timeout=5",
        variable: "group_replication_member_expel_timeout",
        global_query: "SELECT @@GLOBAL.group_replication_member_expel_timeout",
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
    exact_member: String,
    predecessor_process_session: String,
    opening_view: String,
    successor_view: Option<String>,
    new_login: ProbeOutcome,
    recovery_admission: ProbeOutcome,
    established_participation: ProbeOutcome,
    predecessor_absence: ProbeOutcome,
    absence_observed_before_replacement: bool,
    replacement_admission: ProbeOutcome,
    credential_bound_session_identity: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RevocationVerdict {
    ExactProcessBarrier,
    Insufficient,
    SessionIdentityUnproved,
}

impl RevocationEvidence {
    fn verdict(&self) -> RevocationVerdict {
        let exact_binding = !self.exact_member.is_empty()
            && !self.predecessor_process_session.is_empty()
            && !self.opening_view.is_empty()
            && self
                .successor_view
                .as_ref()
                .is_some_and(|successor| successor != &self.opening_view);
        if self.candidate == RevocationCandidate::StopAndRejoin
            && exact_binding
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
mod tests;
