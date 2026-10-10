use std::fs;
use std::net::SocketAddr;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use super::*;
use crate::core::{AttemptId, CredentialGeneration, GroupName};
use crate::service::{
    ControlCredential, ControlCredentialRole, MysqlInstanceConfig, MysqlMemberConfig,
    MysqlOperationTimeouts, MysqlTopologyConfig, MysqlTopologyState, TopologyAttempt,
};

const GROUP_UUID: &str = "d6f6f07e-37e1-4f7a-9105-73c13f634cb3";
static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Default)]
struct ScriptedOwnedMember {
    stops: Vec<QualificationStopKind>,
    restarts: usize,
    contains: usize,
    fail_stop: bool,
    fail_restart: bool,
    fail_contain: bool,
    contain_delay: Duration,
}

impl QualificationOwnedMember for ScriptedOwnedMember {
    fn qualification_stop(
        &mut self,
        kind: QualificationStopKind,
    ) -> Result<(), MysqlInstanceError> {
        self.stops.push(kind);
        if self.fail_stop {
            Err(scripted_instance_error())
        } else {
            Ok(())
        }
    }

    fn qualification_restart(&mut self) -> Result<(), MysqlInstanceError> {
        self.restarts += 1;
        if self.fail_restart {
            Err(scripted_instance_error())
        } else {
            Ok(())
        }
    }

    fn qualification_contain(&mut self) -> Result<(), MysqlInstanceError> {
        self.contains += 1;
        thread::sleep(self.contain_delay);
        if self.fail_contain {
            Err(scripted_instance_error())
        } else {
            Ok(())
        }
    }
}

fn scripted_instance_error() -> MysqlInstanceError {
    MysqlInstanceError::InvalidState {
        expected: crate::service::MysqlInstanceState::Running,
        actual: crate::service::MysqlInstanceState::Configured,
    }
}

fn configured_manager() -> (MysqlTopologyManager, PathBuf) {
    let serial = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
    let root = PathBuf::from("/tmp").join(format!(
        "kuberic-mysql-qualification-contract-{}-{serial}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&root);
    fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
    let sql_addresses = [
        SocketAddr::from(([127, 0, 0, 1], 33_061)),
        SocketAddr::from(([127, 0, 0, 1], 33_062)),
        SocketAddr::from(([127, 0, 0, 1], 33_063)),
    ];
    let group_addresses = [
        SocketAddr::from(([127, 0, 0, 1], 43_061)),
        SocketAddr::from(([127, 0, 0, 1], 43_062)),
        SocketAddr::from(([127, 0, 0, 1], 43_063)),
    ];
    let topology = MysqlTopologyConfig::new([
        MysqlMemberConfig::new(
            1,
            sql_addresses[0],
            group_addresses[0],
            GROUP_UUID,
            group_addresses,
        )
        .unwrap(),
        MysqlMemberConfig::new(
            2,
            sql_addresses[1],
            group_addresses[1],
            GROUP_UUID,
            group_addresses,
        )
        .unwrap(),
        MysqlMemberConfig::new(
            3,
            sql_addresses[2],
            group_addresses[2],
            GROUP_UUID,
            group_addresses,
        )
        .unwrap(),
    ])
    .unwrap();
    let timeouts = MysqlOperationTimeouts::new(
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_secs(1),
    )
    .unwrap();
    let configs = MysqlMemberIndex::all().map(|member| {
        let number = member.as_usize() + 1;
        MysqlInstanceConfig::new_topology_member(
            "/usr/sbin/mysqld",
            "/usr/bin/aa-exec",
            root.join(format!("member-{number}-data")),
            root.join(format!("member-{number}-scratch")),
            topology.clone(),
            member,
            timeouts,
        )
        .unwrap()
    });
    let attempt = TopologyAttempt::new(
        AttemptId::new("qualification-contract-attempt").unwrap(),
        GroupName::new(GROUP_UUID).unwrap(),
        MysqlMemberIndex::First,
        Duration::from_secs(300),
    )
    .unwrap();
    let observer = ControlCredential::new(
        attempt.id().clone(),
        CredentialGeneration::new("observer-generation").unwrap(),
        ControlCredentialRole::Observer,
        "qualification_observer",
        "ObserverSecret1",
    )
    .unwrap();
    let recovery = ControlCredential::new(
        attempt.id().clone(),
        CredentialGeneration::new("recovery-generation").unwrap(),
        ControlCredentialRole::Recovery,
        "qualification_recovery",
        "RecoverySecret1",
    )
    .unwrap();
    (
        MysqlTopologyManager::new(
            configs.map(MysqlInstanceManager::new),
            attempt,
            observer,
            recovery,
        )
        .unwrap(),
        root,
    )
}

fn fake_instance(label: &str) -> (MysqlInstanceManager, PathBuf) {
    let serial = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
    let root = PathBuf::from("/tmp").join(format!("kmq-{}-{serial}-{label}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
    let launcher = root.join("launcher");
    fs::write(
        &launcher,
        r#"#!/bin/sh
while [ "$1" != "--" ]; do shift; done
shift
mysqld="$1"
shift
if [ "$1" = "--version" ]; then
  echo "$mysqld  Ver 8.4.11 for Linux on x86_64 (MySQL Community Server - GPL)"
  exit 0
fi
case "$*" in
  *--initialize-insecure*) exit 0 ;;
esac
config=${1#--defaults-file=}
pid_file=$(sed -n 's/^pid-file=//p' "$config")
echo $$ > "$pid_file"
exec "$mysqld" 60
"#,
    )
    .unwrap();
    fs::set_permissions(&launcher, fs::Permissions::from_mode(0o700)).unwrap();
    let group_addresses = [
        SocketAddr::from(([127, 0, 0, 1], 43_061)),
        SocketAddr::from(([127, 0, 0, 1], 43_062)),
        SocketAddr::from(([127, 0, 0, 1], 43_063)),
    ];
    let topology = MysqlTopologyConfig::new([
        MysqlMemberConfig::new(
            1,
            SocketAddr::from(([127, 0, 0, 1], 33_061)),
            group_addresses[0],
            GROUP_UUID,
            group_addresses,
        )
        .unwrap(),
        MysqlMemberConfig::new(
            2,
            SocketAddr::from(([127, 0, 0, 1], 33_062)),
            group_addresses[1],
            GROUP_UUID,
            group_addresses,
        )
        .unwrap(),
        MysqlMemberConfig::new(
            3,
            SocketAddr::from(([127, 0, 0, 1], 33_063)),
            group_addresses[2],
            GROUP_UUID,
            group_addresses,
        )
        .unwrap(),
    ])
    .unwrap();
    let config = MysqlInstanceConfig::new_topology_member(
        "/usr/bin/sleep",
        &launcher,
        root.join("d"),
        root.join("s"),
        topology,
        MysqlMemberIndex::First,
        MysqlOperationTimeouts::new(
            Duration::from_secs(2),
            Duration::from_secs(2),
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .unwrap(),
    )
    .unwrap();
    (MysqlInstanceManager::new(config), root)
}

fn provide_socket(pid_path: PathBuf, socket_path: PathBuf) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        let pid = loop {
            if let Ok(value) = fs::read_to_string(&pid_path)
                && let Ok(pid) = value.trim().parse::<u32>()
            {
                break pid;
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(10));
        };
        let listener = UnixListener::bind(&socket_path).unwrap();
        while Path::new(&format!("/proc/{pid}")).exists() {
            thread::sleep(Duration::from_millis(10));
        }
        drop(listener);
        let _ = fs::remove_file(socket_path);
    })
}

fn binding() -> QualificationBinding {
    QualificationBinding {
        package: ORACLE_PACKAGE.to_owned(),
        attempt: "attempt-1".to_owned(),
        group: GROUP_UUID.to_owned(),
        opening_view: "1:3".to_owned(),
    }
}

fn complete_settings() -> Vec<SettingReadback> {
    NATIVE_PROFILE
        .iter()
        .map(|spec| SettingReadback::Value {
            variable: spec.variable.to_owned(),
            global: spec.expected.to_owned(),
            session: (spec.scope == NativeSettingScope::GlobalAndSession)
                .then(|| spec.expected.to_owned()),
        })
        .collect()
}

fn complete_evidence() -> NativeProfileEvidence {
    NativeProfileEvidence {
        binding: binding(),
        members: (1..=3)
            .map(|number| MemberProfileEvidence {
                server_uuid: format!("member-{number}"),
                process_session: format!("process-{number}"),
                settings: complete_settings(),
            })
            .collect(),
    }
}

fn revocation_expectation(member: &str, process: &str) -> RevocationExpectation {
    RevocationExpectation {
        binding: binding(),
        exact_member: member.to_owned(),
        predecessor_process_session: process.to_owned(),
        predecessor_credential_generation: "credential-1".to_owned(),
        replacement_credential_generation: "credential-2".to_owned(),
    }
}

#[test]
fn qualification_ownership_stops_restarts_and_rebinds_phase() {
    let mut members = std::array::from_fn(|_| ScriptedOwnedMember::default());
    {
        let mut ownership =
            QualificationOwnership::new(&mut members, Instant::now() + Duration::from_secs(121))
                .unwrap();
        ownership
            .stop(MysqlMemberIndex::Second, QualificationStopKind::Graceful)
            .unwrap();
        assert_eq!(
            ownership.phase(MysqlMemberIndex::Second),
            QualificationMemberPhase::ProcessStopped
        );
        ownership.restart(MysqlMemberIndex::Second).unwrap();
        assert_eq!(
            ownership.phase(MysqlMemberIndex::Second),
            QualificationMemberPhase::Restarting
        );
        ownership.finish_restart(MysqlMemberIndex::Second).unwrap();
        assert_eq!(
            ownership.phase(MysqlMemberIndex::Second),
            QualificationMemberPhase::Running
        );
    }
    assert_eq!(members[1].stops, [QualificationStopKind::Graceful]);
    assert_eq!(members[1].restarts, 1);
    assert_eq!(members.each_ref().map(|member| member.contains), [1, 1, 1]);
}

#[test]
fn qualification_entry_rejects_precompletion_without_mutation() {
    let (mut manager, root) = configured_manager();
    assert!(matches!(
        MysqlNativeQualification::enter(&mut manager),
        Err(MysqlTopologyManagerError::InvalidState {
            expected: MysqlTopologyState::Complete,
            actual: MysqlTopologyState::Configured,
        })
    ));
    assert_eq!(manager.state(), MysqlTopologyState::Configured);
    fs::remove_dir(root).unwrap();
}

#[test]
fn qualification_entry_invalidates_authority_and_drop_contains_members() {
    let (mut manager, root) = configured_manager();
    manager.mark_complete_for_qualification_contract();
    {
        let qualification = MysqlNativeQualification::enter(&mut manager).unwrap();
        assert_eq!(
            qualification.phase(MysqlMemberIndex::First),
            QualificationMemberPhase::Running
        );
    }
    assert_eq!(manager.state(), MysqlTopologyState::Failed);
    assert!(manager.qualification_authority_is_invalidated());
    manager.stop().unwrap();
    assert_eq!(manager.state(), MysqlTopologyState::Stopped);
    fs::remove_dir(root).unwrap();
}

#[test]
fn retained_restart_rejects_foreign_endpoints_and_changes_process_session() {
    for foreign_pid in [true, false] {
        let (mut instance, root) = fake_instance(if foreign_pid { "pid" } else { "socket" });
        instance.initialize().unwrap();
        if foreign_pid {
            fs::write(instance.config().runtime().pid(), "99999\n").unwrap();
        } else {
            let listener = UnixListener::bind(instance.config().runtime().socket()).unwrap();
            assert!(matches!(
                instance.qualification_restart_retained(),
                Err(MysqlInstanceError::SocketStillPresent)
            ));
            drop(listener);
            instance.contain().unwrap();
            fs::remove_dir_all(root).unwrap();
            continue;
        }
        assert!(matches!(
            instance.qualification_restart_retained(),
            Err(MysqlInstanceError::Ownership(
                crate::service::OwnershipError::InvalidPidFile
            ))
        ));
        instance.contain().unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    let (mut instance, root) = fake_instance("generation");
    instance.initialize().unwrap();
    let first_socket = provide_socket(
        instance.config().runtime().pid().to_owned(),
        instance.config().runtime().socket().to_owned(),
    );
    instance.start().unwrap();
    let first = instance
        .topology_binding(
            AttemptId::new("restart-attempt").unwrap(),
            CredentialGeneration::new("observer-generation").unwrap(),
            CredentialGeneration::new("recovery-generation").unwrap(),
        )
        .unwrap();
    instance
        .qualification_stop_preserving_roots(QualificationStopKind::Graceful)
        .unwrap();
    first_socket.join().unwrap();
    let second_socket = provide_socket(
        instance.config().runtime().pid().to_owned(),
        instance.config().runtime().socket().to_owned(),
    );
    instance.qualification_restart_retained().unwrap();
    let second = instance
        .topology_binding(
            AttemptId::new("restart-attempt").unwrap(),
            CredentialGeneration::new("observer-generation").unwrap(),
            CredentialGeneration::new("recovery-generation").unwrap(),
        )
        .unwrap();
    assert_ne!(first.process_session(), second.process_session());
    instance.contain().unwrap();
    second_socket.join().unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn qualification_ownership_contains_on_failure_and_cancelled_rejoin() {
    let mut failed_members = std::array::from_fn(|_| ScriptedOwnedMember::default());
    failed_members[0].fail_stop = true;
    {
        let mut ownership = QualificationOwnership::new(
            &mut failed_members,
            Instant::now() + Duration::from_secs(121),
        )
        .unwrap();
        assert!(
            ownership
                .stop(MysqlMemberIndex::First, QualificationStopKind::Abrupt)
                .is_err()
        );
        assert_eq!(
            ownership.phase(MysqlMemberIndex::First),
            QualificationMemberPhase::Contained
        );
    }
    assert_eq!(
        failed_members.each_ref().map(|member| member.contains),
        [1, 1, 1]
    );

    let mut restart_members = std::array::from_fn(|_| ScriptedOwnedMember::default());
    restart_members[1].fail_restart = true;
    {
        let mut ownership = QualificationOwnership::new(
            &mut restart_members,
            Instant::now() + Duration::from_secs(121),
        )
        .unwrap();
        ownership
            .stop(MysqlMemberIndex::Second, QualificationStopKind::Graceful)
            .unwrap();
        assert!(ownership.restart(MysqlMemberIndex::Second).is_err());
        assert_eq!(
            ownership.phase(MysqlMemberIndex::Second),
            QualificationMemberPhase::Contained
        );
    }
    assert_eq!(
        restart_members.each_ref().map(|member| member.contains),
        [1, 1, 1]
    );

    let mut cancelled_members = std::array::from_fn(|_| ScriptedOwnedMember::default());
    {
        let mut ownership = QualificationOwnership::new(
            &mut cancelled_members,
            Instant::now() + Duration::from_secs(121),
        )
        .unwrap();
        drop(ownership.begin_rejoin(MysqlMemberIndex::Third).unwrap());
        assert_eq!(
            ownership.phase(MysqlMemberIndex::Third),
            QualificationMemberPhase::Contained
        );
    }
    assert_eq!(
        cancelled_members.each_ref().map(|member| member.contains),
        [1, 1, 1]
    );
}

#[test]
fn qualification_ownership_completes_rejoin_and_reserves_cleanup_budget() {
    let mut members = std::array::from_fn(|_| ScriptedOwnedMember::default());
    assert!(
        QualificationOwnership::new(&mut members, Instant::now() + Duration::from_secs(120))
            .is_err()
    );

    {
        let mut ownership =
            QualificationOwnership::new(&mut members, Instant::now() + Duration::from_secs(121))
                .unwrap();
        assert!(ownership.restart(MysqlMemberIndex::First).is_err());
        assert_eq!(
            ownership.phase(MysqlMemberIndex::First),
            QualificationMemberPhase::Contained
        );
    }

    let mut rejoin_members = std::array::from_fn(|_| ScriptedOwnedMember::default());
    {
        let mut ownership = QualificationOwnership::new(
            &mut rejoin_members,
            Instant::now() + Duration::from_secs(121),
        )
        .unwrap();
        ownership
            .begin_rejoin(MysqlMemberIndex::First)
            .unwrap()
            .complete();
        assert_eq!(
            ownership.phase(MysqlMemberIndex::First),
            QualificationMemberPhase::Running
        );
    }
}

#[test]
fn qualification_cleanup_reports_failures_and_deadline_overrun() {
    let mut failing_members = std::array::from_fn(|_| ScriptedOwnedMember::default());
    failing_members[1].fail_contain = true;
    let mut ownership = QualificationOwnership::with_deadlines(
        &mut failing_members,
        Instant::now() + Duration::from_secs(1),
        Instant::now() + Duration::from_secs(1),
    );
    let report = ownership.contain();
    assert_eq!(report.failures.len(), 1);
    assert!(!report.deadline_overrun);
    assert_eq!(
        ownership.phase(MysqlMemberIndex::Second),
        QualificationMemberPhase::ContainmentFailed
    );

    let mut slow_members = std::array::from_fn(|_| ScriptedOwnedMember::default());
    slow_members[0].contain_delay = Duration::from_millis(5);
    let mut ownership = QualificationOwnership::with_deadlines(
        &mut slow_members,
        Instant::now() + Duration::from_secs(1),
        Instant::now() + Duration::from_millis(1),
    );
    let report = ownership.contain();
    assert!(report.deadline_overrun);
    assert!(report.failures.is_empty());

    let mut cutoff_members = std::array::from_fn(|_| ScriptedOwnedMember::default());
    let mut ownership = QualificationOwnership::with_deadlines(
        &mut cutoff_members,
        Instant::now() - Duration::from_millis(1),
        Instant::now() + Duration::from_secs(1),
    );
    assert!(
        ownership
            .stop(MysqlMemberIndex::First, QualificationStopKind::Abrupt)
            .is_err()
    );
    assert_eq!(
        ownership.phase(MysqlMemberIndex::First),
        QualificationMemberPhase::Contained
    );
}

#[test]
fn qualification_rebinding_failure_contains_and_propagates_cleanup_overrun() {
    let mut members = std::array::from_fn(|_| ScriptedOwnedMember::default());
    members[0].contain_delay = Duration::from_millis(5);
    let mut ownership = QualificationOwnership::with_deadlines(
        &mut members,
        Instant::now() + Duration::from_secs(1),
        Instant::now() + Duration::from_millis(1),
    );
    ownership
        .stop(MysqlMemberIndex::First, QualificationStopKind::Graceful)
        .unwrap();
    ownership.restart(MysqlMemberIndex::First).unwrap();
    assert_eq!(
        ownership.phase(MysqlMemberIndex::First),
        QualificationMemberPhase::Restarting
    );
    let cleanup = ownership
        .reject_restart_binding(MysqlMemberIndex::First)
        .unwrap();
    assert!(cleanup.deadline_overrun);
    assert_eq!(
        ownership.phase(MysqlMemberIndex::First),
        QualificationMemberPhase::Contained
    );
    let failures =
        MysqlNativeQualification::qualification_cleanup_failures(MysqlMemberIndex::First, cleanup);
    assert_eq!(failures.len(), 1);
}

#[test]
fn invalid_or_late_rejoin_contains_immediately() {
    let mut invalid_members = std::array::from_fn(|_| ScriptedOwnedMember::default());
    let mut ownership = QualificationOwnership::with_deadlines(
        &mut invalid_members,
        Instant::now() + Duration::from_secs(1),
        Instant::now() + Duration::from_secs(2),
    );
    ownership
        .stop(MysqlMemberIndex::First, QualificationStopKind::Graceful)
        .unwrap();
    assert!(ownership.begin_rejoin(MysqlMemberIndex::First).is_err());
    assert_eq!(
        ownership.phase(MysqlMemberIndex::First),
        QualificationMemberPhase::Contained
    );

    let mut late_members = std::array::from_fn(|_| ScriptedOwnedMember::default());
    let mut ownership = QualificationOwnership::with_deadlines(
        &mut late_members,
        Instant::now() - Duration::from_millis(1),
        Instant::now() + Duration::from_secs(1),
    );
    assert!(ownership.begin_rejoin(MysqlMemberIndex::First).is_err());
    assert_eq!(
        ownership.phase(MysqlMemberIndex::First),
        QualificationMemberPhase::Contained
    );
}

#[test]
fn exact_profile_matrix_and_binding_qualify_all_three_members() {
    assert_eq!(NATIVE_PROFILE.len(), 9);
    assert_eq!(
        NATIVE_PROFILE
            .iter()
            .map(|spec| spec.option)
            .collect::<Vec<_>>(),
        TOPOLOGY_NATIVE_PROFILE_OPTIONS
    );
    assert!(NATIVE_PROFILE.iter().all(|spec| {
        spec.global_query.starts_with("SELECT @@GLOBAL.")
            && (spec.scope != NativeSettingScope::GlobalAndSession
                || spec.session_query == Some("SELECT @@SESSION.group_replication_consistency"))
    }));
    assert_eq!(
        NATIVE_PROFILE
            .iter()
            .map(|spec| spec.mutability)
            .collect::<Vec<_>>(),
        [
            NativeSettingMutability::RestrictedDynamicGlobal,
            NativeSettingMutability::RestrictedDynamicGlobal,
            NativeSettingMutability::GroupReboot,
            NativeSettingMutability::ReadOnlyGroupWide,
            NativeSettingMutability::DynamicGlobalAndSession,
            NativeSettingMutability::DynamicGlobal,
            NativeSettingMutability::DynamicGlobal,
            NativeSettingMutability::DynamicGlobal,
            NativeSettingMutability::DynamicGroupWide,
        ]
    );
    let qualified = complete_evidence().qualify(&binding()).unwrap();
    assert_eq!(qualified.binding, binding());
    assert_eq!(qualified.member_process_sessions.len(), 3);
}

#[test]
fn profile_rejects_wrong_binding_and_member_identity_cardinality() {
    let mutations: [fn(&mut QualificationBinding); 4] = [
        |binding: &mut QualificationBinding| {
            binding.package = "mysql-community-server-core=8.4.12".to_owned();
        },
        |binding: &mut QualificationBinding| binding.attempt = "attempt-2".to_owned(),
        |binding: &mut QualificationBinding| binding.group = "other-group".to_owned(),
        |binding: &mut QualificationBinding| binding.opening_view = "1:4".to_owned(),
    ];
    for mutate in mutations {
        let expected = binding();
        let mut evidence = complete_evidence();
        mutate(&mut evidence.binding);
        assert_eq!(
            evidence.qualify(&expected),
            Err(ProfileEvidenceError::BindingMismatch)
        );
    }

    let mut wrong_count = complete_evidence();
    wrong_count.members.pop();
    assert_eq!(
        wrong_count.qualify(&binding()),
        Err(ProfileEvidenceError::WrongMemberCount)
    );

    let mut duplicate_member = complete_evidence();
    duplicate_member.members[2].server_uuid = duplicate_member.members[1].server_uuid.clone();
    assert_eq!(
        duplicate_member.qualify(&binding()),
        Err(ProfileEvidenceError::DuplicateMember)
    );

    let mut duplicate_session = complete_evidence();
    duplicate_session.members[2].process_session =
        duplicate_session.members[1].process_session.clone();
    assert_eq!(
        duplicate_session.qualify(&binding()),
        Err(ProfileEvidenceError::DuplicateMember)
    );
}

#[test]
fn profile_rejects_missing_malformed_unsupported_and_mismatched_values() {
    let cases = [
        (
            SettingReadback::Missing {
                variable: "sync_binlog".to_owned(),
            },
            ProfileEvidenceError::Missing {
                member: "member-1".to_owned(),
                variable: "sync_binlog".to_owned(),
            },
        ),
        (
            SettingReadback::Malformed {
                variable: "sync_binlog".to_owned(),
            },
            ProfileEvidenceError::Malformed {
                member: "member-1".to_owned(),
                variable: "sync_binlog".to_owned(),
            },
        ),
        (
            SettingReadback::Unsupported {
                variable: "sync_binlog".to_owned(),
            },
            ProfileEvidenceError::Unsupported {
                member: "member-1".to_owned(),
                variable: "sync_binlog".to_owned(),
            },
        ),
        (
            SettingReadback::Value {
                variable: "sync_binlog".to_owned(),
                global: "0".to_owned(),
                session: None,
            },
            ProfileEvidenceError::Mismatch {
                member: "member-1".to_owned(),
                variable: "sync_binlog".to_owned(),
            },
        ),
    ];

    for (replacement, expected) in cases {
        let mut evidence = complete_evidence();
        let index = evidence.members[0]
            .settings
            .iter()
            .position(|setting| setting.variable() == "sync_binlog")
            .unwrap();
        evidence.members[0].settings[index] = replacement;
        assert_eq!(evidence.qualify(&binding()), Err(expected));
    }
}

#[test]
fn profile_requires_workload_session_consistency() {
    let mut evidence = complete_evidence();
    let consistency = evidence.members[0]
        .settings
        .iter_mut()
        .find(|setting| setting.variable() == "group_replication_consistency")
        .unwrap();
    *consistency = SettingReadback::Value {
        variable: "group_replication_consistency".to_owned(),
        global: "AFTER".to_owned(),
        session: Some("EVENTUAL".to_owned()),
    };
    assert_eq!(
        evidence.qualify(&binding()),
        Err(ProfileEvidenceError::Mismatch {
            member: "member-1".to_owned(),
            variable: "group_replication_consistency".to_owned(),
        })
    );
}

#[test]
fn gtid_profile_preserves_full_shape_and_rejects_every_incompatible_class() {
    assert_eq!(
        classify_gtid_profile(&GtidSet::empty(), GROUP_UUID),
        GtidProfileCompatibility::CompatibleEmpty
    );
    assert_eq!(
        classify_gtid_profile(
            &GtidSet::from_str(&format!("{GROUP_UUID}:1-8")).unwrap(),
            GROUP_UUID
        ),
        GtidProfileCompatibility::CompatibleContiguous { tail: 8 }
    );
    assert_eq!(
        classify_gtid_profile(
            &GtidSet::from_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1").unwrap(),
            GROUP_UUID
        ),
        GtidProfileCompatibility::ForeignSource
    );
    assert_eq!(
        classify_gtid_profile(
            &GtidSet::from_str(&format!("{GROUP_UUID}:tag:1")).unwrap(),
            GROUP_UUID
        ),
        GtidProfileCompatibility::TaggedSource
    );
    assert_eq!(
        classify_gtid_profile(
            &GtidSet::from_str(&format!(
                "{GROUP_UUID}:1,aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1"
            ))
            .unwrap(),
            GROUP_UUID
        ),
        GtidProfileCompatibility::MultipleSources
    );
    assert_eq!(
        classify_gtid_profile(
            &GtidSet::from_str(&format!("{GROUP_UUID}:1-2:4")).unwrap(),
            GROUP_UUID
        ),
        GtidProfileCompatibility::MultipleIntervals
    );
    assert_eq!(
        classify_gtid_profile(
            &GtidSet::from_str(&format!("{GROUP_UUID}:2-4")).unwrap(),
            GROUP_UUID
        ),
        GtidProfileCompatibility::DoesNotBeginAtOne
    );
}

#[test]
fn retained_history_keeps_purged_set_and_inventory_separate() {
    let evidence = RetainedHistoryEvidence {
        executed: GtidSet::from_str(&format!("{GROUP_UUID}:1-8")).unwrap(),
        purged: GtidSet::from_str(&format!("{GROUP_UUID}:1-3")).unwrap(),
        binary_logs: vec!["mysql-bin.000002".to_owned(), "mysql-bin.000003".to_owned()],
    };
    assert_eq!(evidence.validate_opaque_inventory(), Ok(()));

    let invalid = RetainedHistoryEvidence {
        executed: GtidSet::from_str(&format!("{GROUP_UUID}:1-3")).unwrap(),
        purged: GtidSet::from_str(&format!("{GROUP_UUID}:1-4")).unwrap(),
        binary_logs: vec!["mysql-bin.000002".to_owned()],
    };
    assert_eq!(
        invalid.validate_opaque_inventory(),
        Err(RetainedHistoryError::PurgedOutsideExecuted)
    );

    for (logs, expected) in [
        (Vec::new(), RetainedHistoryError::MissingInventory),
        (
            vec!["../mysql-bin.000001".to_owned()],
            RetainedHistoryError::MalformedInventory,
        ),
        (
            vec!["mysql-bin.000001".to_owned(), "mysql-bin.000001".to_owned()],
            RetainedHistoryError::DuplicateInventory,
        ),
    ] {
        let evidence = RetainedHistoryEvidence {
            executed: GtidSet::empty(),
            purged: GtidSet::empty(),
            binary_logs: logs,
        };
        assert_eq!(evidence.validate_opaque_inventory(), Err(expected));
    }
}

#[test]
fn revocation_never_confuses_login_denial_with_a_session_barrier() {
    for candidate in [
        RevocationCandidate::AccountLock,
        RevocationCandidate::CredentialReplacement,
        RevocationCandidate::PrivilegeRemoval,
    ] {
        let evidence = RevocationEvidence {
            candidate,
            binding: binding(),
            exact_member: "member-2".to_owned(),
            predecessor_process_session: "process-2".to_owned(),
            predecessor_credential_generation: "credential-1".to_owned(),
            replacement_credential_generation: "credential-2".to_owned(),
            opening_view: "1:3".to_owned(),
            successor_view: None,
            new_login: ProbeOutcome::Rejected,
            recovery_admission: ProbeOutcome::Rejected,
            established_participation: ProbeOutcome::Continued,
            predecessor_absence: ProbeOutcome::Unproved,
            absence_observed_before_replacement: false,
            replacement_admission: ProbeOutcome::Unproved,
            credential_bound_session_identity: false,
        };
        assert_eq!(
            evidence.verdict(&revocation_expectation("member-2", "process-2")),
            RevocationVerdict::SessionIdentityUnproved
        );
    }

    let stop_rejoin = RevocationEvidence {
        candidate: RevocationCandidate::StopAndRejoin,
        binding: binding(),
        exact_member: "member-2".to_owned(),
        predecessor_process_session: "process-2".to_owned(),
        predecessor_credential_generation: "credential-1".to_owned(),
        replacement_credential_generation: "credential-2".to_owned(),
        opening_view: "1:3".to_owned(),
        successor_view: Some("1:4".to_owned()),
        new_login: ProbeOutcome::Rejected,
        recovery_admission: ProbeOutcome::Rejected,
        established_participation: ProbeOutcome::Absent,
        predecessor_absence: ProbeOutcome::Absent,
        absence_observed_before_replacement: true,
        replacement_admission: ProbeOutcome::Accepted,
        credential_bound_session_identity: false,
    };
    assert_eq!(
        stop_rejoin.verdict(&revocation_expectation("member-2", "process-2")),
        RevocationVerdict::ExactProcessBarrier
    );

    let invalidations: [fn(&mut RevocationEvidence); 10] = [
        |evidence: &mut RevocationEvidence| evidence.binding.package = "other".to_owned(),
        |evidence: &mut RevocationEvidence| evidence.binding.attempt.clear(),
        |evidence: &mut RevocationEvidence| evidence.binding.group.clear(),
        |evidence: &mut RevocationEvidence| evidence.exact_member.clear(),
        |evidence: &mut RevocationEvidence| evidence.predecessor_process_session.clear(),
        |evidence: &mut RevocationEvidence| {
            evidence.replacement_credential_generation =
                evidence.predecessor_credential_generation.clone();
        },
        |evidence: &mut RevocationEvidence| evidence.successor_view = None,
        |evidence: &mut RevocationEvidence| {
            evidence.absence_observed_before_replacement = false;
        },
        |evidence: &mut RevocationEvidence| {
            evidence.recovery_admission = ProbeOutcome::Accepted;
        },
        |evidence: &mut RevocationEvidence| {
            evidence.established_participation = ProbeOutcome::Continued;
        },
    ];
    for invalidate in invalidations {
        let mut evidence = stop_rejoin.clone();
        invalidate(&mut evidence);
        assert_ne!(
            evidence.verdict(&revocation_expectation("member-2", "process-2")),
            RevocationVerdict::ExactProcessBarrier
        );
    }

    let expectation_mutations: [fn(&mut RevocationExpectation); 7] = [
        |expected: &mut RevocationExpectation| expected.binding.attempt = "attempt-2".to_owned(),
        |expected: &mut RevocationExpectation| expected.binding.group = "other-group".to_owned(),
        |expected: &mut RevocationExpectation| {
            expected.binding.opening_view = "1:2".to_owned();
        },
        |expected: &mut RevocationExpectation| expected.exact_member = "member-3".to_owned(),
        |expected: &mut RevocationExpectation| {
            expected.predecessor_process_session = "process-3".to_owned();
        },
        |expected: &mut RevocationExpectation| {
            expected.predecessor_credential_generation = "credential-old".to_owned();
        },
        |expected: &mut RevocationExpectation| {
            expected.replacement_credential_generation = "credential-new".to_owned();
        },
    ];
    for mutate in expectation_mutations {
        let mut expected = revocation_expectation("member-2", "process-2");
        mutate(&mut expected);
        assert_ne!(
            stop_rejoin.verdict(&expected),
            RevocationVerdict::ExactProcessBarrier
        );
    }
}

#[test]
fn result_enums_keep_outcomes_and_diagnostics_distinct_and_secret_free() {
    assert_ne!(ClientOutcome::Acknowledged, ClientOutcome::ServerRejected);
    assert_ne!(
        ClientOutcome::DisconnectedAfterDispatch,
        ClientOutcome::DeadlineExpired
    );
    assert_ne!(ProbeOutcome::Accepted, ProbeOutcome::Rejected);
    assert_eq!(
        TransactionOutcomeEvidence {
            client: ClientOutcome::Acknowledged,
            exact_gtid_bound: true,
            inclusion: InclusionEvidence::PresentOnEveryRequiredMember,
        }
        .verdict(),
        TransactionVerdict::AcknowledgedDurable
    );
    assert_eq!(
        TransactionOutcomeEvidence {
            client: ClientOutcome::Acknowledged,
            exact_gtid_bound: false,
            inclusion: InclusionEvidence::IncompleteOrIncoherent,
        }
        .verdict(),
        TransactionVerdict::IncompatibleAcknowledgement
    );
    for (inclusion, verdict) in [
        (
            InclusionEvidence::PresentOnEveryRequiredMember,
            TransactionVerdict::AmbiguousIncluded,
        ),
        (
            InclusionEvidence::AbsentFromCoherentRequiredHistory,
            TransactionVerdict::AmbiguousAbsent,
        ),
        (
            InclusionEvidence::IncompleteOrIncoherent,
            TransactionVerdict::AmbiguousUnresolved,
        ),
    ] {
        assert_eq!(
            TransactionOutcomeEvidence {
                client: ClientOutcome::DisconnectedAfterDispatch,
                exact_gtid_bound: true,
                inclusion,
            }
            .verdict(),
            verdict
        );
    }
    assert_eq!(
        TransactionOutcomeEvidence {
            client: ClientOutcome::DeadlineExpired,
            exact_gtid_bound: false,
            inclusion: InclusionEvidence::PresentOnEveryRequiredMember,
        }
        .verdict(),
        TransactionVerdict::AmbiguousUnresolved
    );
    assert_eq!(
        TransactionOutcomeEvidence {
            client: ClientOutcome::ServerRejected,
            exact_gtid_bound: false,
            inclusion: InclusionEvidence::IncompleteOrIncoherent,
        }
        .verdict(),
        TransactionVerdict::Rejected
    );
    assert_eq!(
        RevocationEvidence {
            candidate: RevocationCandidate::AccountLock,
            binding: binding(),
            exact_member: "member-1".to_owned(),
            predecessor_process_session: "process-1".to_owned(),
            predecessor_credential_generation: "credential-1".to_owned(),
            replacement_credential_generation: "credential-2".to_owned(),
            opening_view: "1:3".to_owned(),
            successor_view: None,
            new_login: ProbeOutcome::Accepted,
            recovery_admission: ProbeOutcome::Accepted,
            established_participation: ProbeOutcome::Continued,
            predecessor_absence: ProbeOutcome::Unproved,
            absence_observed_before_replacement: false,
            replacement_admission: ProbeOutcome::Unproved,
            credential_bound_session_identity: true,
        }
        .verdict(&revocation_expectation("member-1", "process-1")),
        RevocationVerdict::Insufficient
    );

    let diagnostic = QualificationDiagnostic {
        stage: QualificationStage::ProfileReadback,
        member: Some("member-1".to_owned()),
        category: "setting-mismatch",
    };
    let rendered = format!("{diagnostic:?}");
    assert!(!rendered.contains("KmsRecoverySecret"));

    for stage in [
        QualificationStage::ProfileReadback,
        QualificationStage::GtidCheckpoint,
        QualificationStage::RetainedHistory,
        QualificationStage::Revocation,
        QualificationStage::Transaction,
        QualificationStage::Cleanup,
    ] {
        assert!(!format!("{stage:?}").is_empty());
    }
}
