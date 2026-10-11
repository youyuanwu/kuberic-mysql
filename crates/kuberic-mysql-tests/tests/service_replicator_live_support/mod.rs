#![allow(dead_code)]

#[path = "../service_live_support/mod.rs"]
mod live_support;

use std::collections::HashSet;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use kuberic_mysql::core::{
    AttemptId, AuthorityGeneration, ConfigurationId, ConfiguredGtidLineage, CredentialGeneration,
    ProgressProjection, ReplicaId, ReplicaIncarnation,
};
use kuberic_mysql::service::{
    ApplicationCredential, AttemptContext, CanonicalViewId, ControlCredential,
    ControlCredentialRole, MysqlFreshMemberCredentials, MysqlFreshMemberInput, MysqlInstanceConfig,
    MysqlMemberConfig, MysqlMemberIndex, MysqlOperationIdentity, MysqlOperationTimeouts,
    MysqlReplicaConfiguration, MysqlReplicaEpoch, MysqlReplicaIdentity, MysqlReplicaRole,
    MysqlReplicaTarget, MysqlReplicator, MysqlTopologyConfig,
};

pub use live_support::preflight_oracle_mysql_8_4_11;
use live_support::{
    APPARMOR_EXEC, FixtureRoot, MYSQLD, ReservedPorts, assert_no_application_store,
    assert_process_absent, assert_tcp_refuses,
};

pub const GROUP_UUID: &str = "49a138d2-ceee-49eb-993e-c6506c495481";
const MEMBERS: [MysqlMemberIndex; 3] = [
    MysqlMemberIndex::First,
    MysqlMemberIndex::Second,
    MysqlMemberIndex::Third,
];
const ATTEMPT_BUDGET: Duration = Duration::from_secs(420);
const CLEANUP_RESERVE: Duration = Duration::from_secs(60);

pub struct FormationEvidence {
    targets: [MysqlReplicaTarget; 3],
    first_view: String,
    second_view: String,
    final_view: String,
    progress: ProgressProjection,
}

impl FormationEvidence {
    pub fn targets(&self) -> &[MysqlReplicaTarget; 3] {
        &self.targets
    }

    pub fn first_view(&self) -> &str {
        &self.first_view
    }

    pub fn second_view(&self) -> &str {
        &self.second_view
    }

    pub fn final_view(&self) -> &str {
        &self.final_view
    }

    pub const fn progress(&self) -> ProgressProjection {
        self.progress
    }
}

pub struct ReplicatorLiveFixture {
    root: FixtureRoot,
    root_path: PathBuf,
    attempt: AttemptContext,
    members: [MemberOwner; 3],
    workload: WorkloadCredentialOwner,
    sql_addresses: [SocketAddr; 3],
    group_addresses: [SocketAddr; 3],
    closed: bool,
}

impl ReplicatorLiveFixture {
    pub async fn prepare() -> Result<Self, String> {
        let root = FixtureRoot::new("replicator-live")?;
        let root_path = root.path().to_path_buf();
        let mut native_ports = ReservedPorts::allocate()?;
        let sql_addresses = native_ports.sql_addresses();
        let group_addresses = native_ports.group_replication_addresses();
        let mut proxy_ports = ProxyPortReservations::allocate(
            sql_addresses.into_iter().chain(group_addresses).collect(),
        )?;
        let proxy_addresses = proxy_ports.addresses;
        let all_addresses = sql_addresses
            .into_iter()
            .chain(group_addresses)
            .chain(proxy_addresses)
            .collect::<HashSet<_>>();
        if all_addresses.len() != 9 {
            return Err("formation ports were not distinct".to_owned());
        }

        let attempt = AttemptContext::new(
            AttemptId::new(format!("replicator-live-{}", std::process::id()))
                .map_err(|error| error.to_string())?,
            Instant::now() + ATTEMPT_BUDGET,
            CLEANUP_RESERVE,
        )
        .map_err(|error| format!("{error:?}"))?;
        let topology = topology(sql_addresses, group_addresses)?;
        let timeouts = MysqlOperationTimeouts::new(
            Duration::from_secs(180),
            Duration::from_secs(60),
            Duration::from_secs(30),
            Duration::from_secs(10),
        )
        .map_err(|error| format!("{error:?}"))?;
        let configs =
            MEMBERS.map(|member| instance_config(&root_path, topology.clone(), member, timeouts));
        let identities = [
            replica_identity("one"),
            replica_identity("two"),
            replica_identity("three"),
        ];

        let mut recovery_secret =
            SecretProducer::new(format!("ReplicatorRecovery{}B9", std::process::id()));
        let mut recovery_credentials = MEMBERS.map(|_| {
            Some(
                ControlCredential::new(
                    attempt.attempt().clone(),
                    CredentialGeneration::new("replicator-recovery-generation").unwrap(),
                    ControlCredentialRole::Recovery,
                    format!("kmr_rec_{}", std::process::id()),
                    recovery_secret.copy(),
                )
                .unwrap(),
            )
        });
        recovery_secret.clear();
        if !recovery_secret.is_cleared() {
            return Err("shared recovery producer was not cleared".to_owned());
        }

        let mut application_secret =
            SecretProducer::new(format!("ReplicatorApplication{}C11", std::process::id()));
        let application_username = format!("kmr_app_{}", std::process::id());
        let mut application_credentials = MEMBERS.map(|_| {
            Some(
                ApplicationCredential::new(
                    attempt.attempt().clone(),
                    CredentialGeneration::new("replicator-application-generation").unwrap(),
                    application_username.clone(),
                    application_secret.copy(),
                )
                .unwrap(),
            )
        });
        let workload = WorkloadCredentialOwner::new(
            application_username,
            application_secret.copy().into_bytes(),
        );
        application_secret.clear();
        if !application_secret.is_cleared() {
            return Err("application credential producer was not cleared".to_owned());
        }

        native_ports.release();
        proxy_ports.release();
        let mut prepared = Vec::with_capacity(3);
        for index in 0..3 {
            let mut observer_secret = SecretProducer::new(format!(
                "ReplicatorObserver{}{}A7",
                std::process::id(),
                index + 1
            ));
            let observer = ControlCredential::new(
                attempt.attempt().clone(),
                CredentialGeneration::new(format!("replicator-observer-generation-{}", index + 1))
                    .unwrap(),
                ControlCredentialRole::Observer,
                format!("kmr_obs_{}_{}", std::process::id(), index + 1),
                observer_secret.copy(),
            )
            .unwrap();
            observer_secret.clear();
            if !observer_secret.is_cleared() {
                return Err(format!(
                    "member {} observer producer was not cleared",
                    index + 1
                ));
            }
            let data_root = configs[index].data_root().to_owned();
            let scratch_root = configs[index].runtime().scratch_root().to_owned();
            let socket = configs[index].runtime().socket().to_owned();
            let pid_file = configs[index].runtime().pid().to_owned();
            let credentials = MysqlFreshMemberCredentials::new(
                observer,
                recovery_credentials[index]
                    .take()
                    .expect("member recovery credential transfers once"),
                application_credentials[index]
                    .take()
                    .expect("member application credential transfers once"),
            );
            let input = MysqlFreshMemberInput::new(
                configs[index].clone(),
                identities[index].clone(),
                ConfiguredGtidLineage::new(GROUP_UUID).map_err(|error| error.to_string())?,
                credentials,
                attempt.clone(),
                NonZeroU64::new(1).unwrap(),
            );
            let (mut engine, receipt) = MysqlReplicator::prepare_fresh_member(input)
                .await
                .map_err(|error| format!("{error:?}"))?;
            engine
                .open(operation(&format!("open-member-{}", index + 1), 1), receipt)
                .map_err(|error| format!("{error:?}"))?;
            let pid = fs::read_to_string(&pid_file)
                .map_err(|error| format!("read {}: {error}", pid_file.display()))?
                .trim()
                .parse::<u32>()
                .map_err(|error| format!("parse {}: {error}", pid_file.display()))?;
            prepared.push(MemberOwner {
                engine: Some(engine),
                data_root,
                scratch_root,
                socket,
                pid_file,
                pid,
                proxy_address: proxy_addresses[index],
                next_generation: 2,
                armed: true,
            });
        }
        let members: [MemberOwner; 3] = prepared
            .try_into()
            .map_err(|_| "expected three prepared member owners".to_owned())?;
        Ok(Self {
            root,
            root_path,
            attempt,
            members,
            workload,
            sql_addresses,
            group_addresses,
            closed: false,
        })
    }

    pub async fn form(&mut self) -> Result<FormationEvidence, String> {
        let targets = std::array::from_fn(|index| {
            self.members[index]
                .engine()
                .local_target()
                .expect("prepared engine is open")
        });
        if targets
            .iter()
            .map(|target| target.member().member_id())
            .collect::<HashSet<_>>()
            .len()
            != 3
        {
            return Err("native member identities were not distinct".to_owned());
        }
        for index in 0..3 {
            let role = if index == 0 {
                MysqlReplicaRole::Primary
            } else {
                MysqlReplicaRole::Secondary
            };
            let epoch_operation = self.members[index].operation("epoch");
            self.members[index]
                .engine_mut()
                .advance_epoch(epoch_operation, MysqlReplicaEpoch::new(1))
                .map_err(|error| format!("{error:?}"))?;
            let role_operation = self.members[index].operation("formation-role");
            self.members[index]
                .engine_mut()
                .intend_formation_role(role_operation, MysqlReplicaEpoch::new(1), role)
                .map_err(|error| format!("{error:?}"))?;
        }

        let first_configuration = configuration("formation-one", vec![targets[0].clone()]);
        let bootstrap_operation = self.members[0].operation("bootstrap");
        self.members[0]
            .engine_mut()
            .bootstrap_configuration(bootstrap_operation, first_configuration)
            .await
            .map_err(|error| format!("{error:?}"))?;
        let first_evidence = self.members[0]
            .engine()
            .native_evidence()
            .cloned()
            .ok_or_else(|| "bootstrap returned no exact native evidence".to_owned())?;

        let second_configuration = configuration(
            "formation-two",
            vec![targets[0].clone(), targets[1].clone()],
        );
        let second_source = self.members[0]
            .engine()
            .source_boundary()
            .map_err(|error| format!("{error:?}"))?;
        let second_operation = self.members[1].operation("join-second");
        self.members[1]
            .engine_mut()
            .join_configuration(
                second_operation,
                second_configuration.clone(),
                second_source,
                vec![targets[0].member().clone()],
            )
            .await
            .map_err(|error| error.to_string())?;
        let observe_second_operation = self.members[0].operation("observe-second");
        self.members[0]
            .engine_mut()
            .observe_configuration(observe_second_operation, second_configuration)
            .await
            .map_err(|error| error.to_string())?;
        let second_evidence = self.members[1]
            .engine()
            .native_evidence()
            .cloned()
            .ok_or_else(|| "second join returned no exact native evidence".to_owned())?;

        let final_configuration = configuration("formation-three", targets.to_vec());
        let third_source = self.members[1]
            .engine()
            .source_boundary()
            .map_err(|error| error.to_string())?;
        let third_operation = self.members[2].operation("join-third");
        self.members[2]
            .engine_mut()
            .join_configuration(
                third_operation,
                final_configuration.clone(),
                third_source,
                vec![targets[0].member().clone(), targets[1].member().clone()],
            )
            .await
            .map_err(|error| error.to_string())?;
        for index in 0..2 {
            let observe_operation = self.members[index].operation("observe-final");
            self.members[index]
                .engine_mut()
                .observe_configuration(observe_operation, final_configuration.clone())
                .await
                .map_err(|error| error.to_string())?;
        }

        let final_evidence = self.members[2]
            .engine()
            .native_evidence()
            .cloned()
            .ok_or_else(|| "third join returned no exact native evidence".to_owned())?;
        CanonicalViewId::parse(first_evidence.view().id())
            .and_then(|first| {
                CanonicalViewId::parse(second_evidence.view().id())
                    .and_then(|second| first.require_successor(second))
            })
            .map_err(|error| format!("{error:?}"))?;
        CanonicalViewId::parse(second_evidence.view().id())
            .and_then(|second| {
                CanonicalViewId::parse(final_evidence.view().id())
                    .and_then(|final_view| second.require_successor(final_view))
            })
            .map_err(|error| format!("{error:?}"))?;

        let mut common_progress = None;
        for index in 0..3 {
            let progress_operation = self.members[index].operation("progress");
            let progress = self.members[index]
                .engine_mut()
                .read_progress(progress_operation)
                .await
                .map_err(|error| error.to_string())?
                .progress();
            if let Some(expected) = common_progress {
                if progress != expected {
                    return Err("formation members did not report common progress".to_owned());
                }
            } else {
                common_progress = Some(progress);
            }
            let engine = self.members[index].engine();
            if engine.configuration() != Some(&final_configuration)
                || engine.epoch() != Some(MysqlReplicaEpoch::new(1))
                || engine.role()
                    != Some(if index == 0 {
                        MysqlReplicaRole::Primary
                    } else {
                        MysqlReplicaRole::Secondary
                    })
                || engine.proxy_snapshot().is_some()
                || !engine.credentials_cleared()
                || !engine.application_credential_staged()
            {
                return Err(format!(
                    "member {} formation state was incomplete",
                    index + 1
                ));
            }
            let evidence = engine
                .native_evidence()
                .ok_or_else(|| format!("member {} final evidence missing", index + 1))?;
            if evidence.view() != final_evidence.view()
                || evidence.executed() != final_evidence.executed()
                || evidence
                    .executed()
                    .entries()
                    .iter()
                    .any(|entry| entry.source().server_uuid().as_str() != GROUP_UUID)
            {
                return Err(format!("member {} final native state differed", index + 1));
            }
            assert_tcp_refuses(self.members[index].proxy_address)?;
            assert_tcp_refuses(self.sql_addresses[index])?;
        }
        Ok(FormationEvidence {
            targets,
            first_view: first_evidence.view().id().as_str().to_owned(),
            second_view: second_evidence.view().id().as_str().to_owned(),
            final_view: final_evidence.view().id().as_str().to_owned(),
            progress: common_progress.expect("three members produced progress"),
        })
    }

    pub async fn form_and_finalize(mut self) -> Result<FormationEvidence, String> {
        let formation = self.form().await;
        let cleanup_failures = self.cleanup_failures();
        finalize_attempt(formation, cleanup_failures)
    }

    fn cleanup_failures(&mut self) -> Vec<String> {
        if self.closed {
            return Vec::new();
        }
        self.attempt.cancel();
        let mut failures = Vec::new();
        for (index, owner) in self.members.iter_mut().enumerate() {
            if let Err(error) = owner.cleanup() {
                failures.push(format!("member {} cleanup: {error}", index + 1));
            }
        }
        self.workload.clear();
        for owner in &self.members {
            if let Err(error) = assert_process_absent(owner.pid) {
                failures.push(error);
            }
            if owner.socket.exists() {
                failures.push(format!("member socket survived {}", owner.socket.display()));
            }
            if owner.pid_file.exists() {
                failures.push(format!(
                    "member PID file survived {}",
                    owner.pid_file.display()
                ));
            }
            if let Err(error) = assert_tcp_refuses(owner.proxy_address) {
                failures.push(error);
            }
            if let Err(error) = assert_no_application_store(&owner.data_root) {
                failures.push(error);
            }
            if owner.scratch_root.exists() {
                failures.push(format!(
                    "member scratch root survived {}",
                    owner.scratch_root.display()
                ));
            }
        }
        for address in self.sql_addresses.into_iter().chain(self.group_addresses) {
            if let Err(error) = assert_tcp_refuses(address) {
                failures.push(error);
            }
        }
        if let Err(error) = self.root.remove() {
            failures.push(error);
        }
        if self.root_path.exists() {
            failures.push(format!(
                "fixture root survived {}",
                self.root_path.display()
            ));
        }
        self.closed = true;
        failures
    }
}

impl Drop for ReplicatorLiveFixture {
    fn drop(&mut self) {
        let failures = self.cleanup_failures();
        if !failures.is_empty() {
            eprintln!(
                "emergency replicator fixture cleanup failed: {}",
                failures.join("; ")
            );
        }
    }
}

fn finalize_attempt<T>(
    primary: Result<T, String>,
    cleanup_failures: Vec<String>,
) -> Result<T, String> {
    match (primary, cleanup_failures.is_empty()) {
        (Ok(value), true) => Ok(value),
        (Err(primary), true) => Err(format!("formation failed: {primary}")),
        (Ok(_), false) => Err(format!(
            "formation cleanup failed: {}",
            cleanup_failures.join("; ")
        )),
        (Err(primary), false) => Err(format!(
            "formation failed: {primary}; cleanup failures: {}",
            cleanup_failures.join("; ")
        )),
    }
}

struct MemberOwner {
    engine: Option<MysqlReplicator>,
    data_root: PathBuf,
    scratch_root: PathBuf,
    socket: PathBuf,
    pid_file: PathBuf,
    pid: u32,
    proxy_address: SocketAddr,
    next_generation: u64,
    armed: bool,
}

impl MemberOwner {
    fn engine(&self) -> &MysqlReplicator {
        self.engine.as_ref().expect("armed member retains engine")
    }

    fn engine_mut(&mut self) -> &mut MysqlReplicator {
        self.engine.as_mut().expect("armed member retains engine")
    }

    fn operation(&mut self, label: &str) -> MysqlOperationIdentity {
        let generation = self.next_generation;
        self.next_generation += 1;
        operation(
            &format!("{label}-{}", self.engine().identity().replica_id().as_str()),
            generation,
        )
    }

    fn cleanup(&mut self) -> Result<(), String> {
        if !self.armed {
            return Ok(());
        }
        let operation = operation(
            &format!("cleanup-{}", self.engine().identity().replica_id().as_str()),
            u64::MAX,
        );
        let result = if self.engine().terminal_state().is_some() {
            Ok(())
        } else {
            self.engine_mut()
                .abort(operation)
                .map(|_| ())
                .map_err(|error| error.to_string())
        };
        self.armed = false;
        result
    }
}

impl Drop for MemberOwner {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            eprintln!("emergency member-owner cleanup failed: {error}");
        }
    }
}

struct WorkloadCredentialOwner {
    username: String,
    password: Box<[u8]>,
}

impl WorkloadCredentialOwner {
    fn new(username: String, password: Vec<u8>) -> Self {
        Self {
            username,
            password: password.into_boxed_slice(),
        }
    }

    fn clear(&mut self) {
        self.password.fill(0);
    }
}

impl Drop for WorkloadCredentialOwner {
    fn drop(&mut self) {
        self.clear();
    }
}

struct SecretProducer {
    value: Vec<u8>,
}

impl SecretProducer {
    fn new(value: String) -> Self {
        Self {
            value: value.into_bytes(),
        }
    }

    fn copy(&self) -> String {
        String::from_utf8(self.value.clone()).expect("producer secret is valid UTF-8")
    }

    fn clear(&mut self) {
        self.value.fill(0);
    }

    fn is_cleared(&self) -> bool {
        self.value.iter().all(|byte| *byte == 0)
    }
}

impl Drop for SecretProducer {
    fn drop(&mut self) {
        self.clear();
    }
}

#[test]
fn attempt_finalization_preserves_primary_and_every_cleanup_failure_in_order() {
    let error = finalize_attempt::<()>(
        Err("primary formation failure".to_owned()),
        vec![
            "member 1 cleanup: first failure".to_owned(),
            "member 2 cleanup: second failure".to_owned(),
            "member 3 cleanup: third failure".to_owned(),
        ],
    )
    .unwrap_err();
    assert_eq!(
        error,
        "formation failed: primary formation failure; cleanup failures: \
         member 1 cleanup: first failure; member 2 cleanup: second failure; \
         member 3 cleanup: third failure"
    );

    assert!(matches!(
        finalize_attempt(Ok::<_, String>("formation"), Vec::new()),
        Ok("formation")
    ));
    assert_eq!(
        finalize_attempt::<()>(Ok(()), vec!["member 2 cleanup: cleanup failure".to_owned()])
            .unwrap_err(),
        "formation cleanup failed: member 2 cleanup: cleanup failure"
    );
}

struct ProxyPortReservations {
    listeners: Vec<TcpListener>,
    addresses: [SocketAddr; 3],
}

impl ProxyPortReservations {
    fn allocate(mut used: HashSet<SocketAddr>) -> Result<Self, String> {
        let mut listeners = Vec::with_capacity(3);
        let mut addresses = Vec::with_capacity(3);
        while listeners.len() < 3 {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
                .map_err(|error| format!("reserve proxy loopback port: {error}"))?;
            let address = listener
                .local_addr()
                .map_err(|error| format!("inspect proxy loopback port: {error}"))?;
            if !used.insert(address) {
                continue;
            }
            listeners.push(listener);
            addresses.push(address);
        }
        Ok(Self {
            listeners,
            addresses: addresses
                .try_into()
                .map_err(|_| "expected exactly three proxy ports".to_owned())?,
        })
    }

    fn release(&mut self) {
        self.listeners.clear();
    }
}

fn topology(
    sql_addresses: [SocketAddr; 3],
    group_addresses: [SocketAddr; 3],
) -> Result<MysqlTopologyConfig, String> {
    MysqlTopologyConfig::new([
        MysqlMemberConfig::new(
            101,
            sql_addresses[0],
            group_addresses[0],
            GROUP_UUID,
            group_addresses,
        )
        .map_err(|error| format!("{error:?}"))?,
        MysqlMemberConfig::new(
            102,
            sql_addresses[1],
            group_addresses[1],
            GROUP_UUID,
            group_addresses,
        )
        .map_err(|error| format!("{error:?}"))?,
        MysqlMemberConfig::new(
            103,
            sql_addresses[2],
            group_addresses[2],
            GROUP_UUID,
            group_addresses,
        )
        .map_err(|error| format!("{error:?}"))?,
    ])
    .map_err(|error| format!("{error:?}"))
}

fn instance_config(
    root: &Path,
    topology: MysqlTopologyConfig,
    member: MysqlMemberIndex,
    timeouts: MysqlOperationTimeouts,
) -> MysqlInstanceConfig {
    let number = member_number(member);
    MysqlInstanceConfig::new_topology_member(
        MYSQLD,
        APPARMOR_EXEC,
        root.join(format!("member-{number}-data")),
        root.join(format!("member-{number}-scratch")),
        topology,
        member,
        timeouts,
    )
    .unwrap()
}

fn replica_identity(label: &str) -> MysqlReplicaIdentity {
    MysqlReplicaIdentity::new(
        ReplicaId::new(format!("replicator-live-{label}")).unwrap(),
        ReplicaIncarnation::new(format!("replicator-live-{label}-{}", std::process::id())).unwrap(),
    )
}

fn configuration(label: &str, members: Vec<MysqlReplicaTarget>) -> MysqlReplicaConfiguration {
    MysqlReplicaConfiguration::new(
        ConfigurationId::new(label).unwrap(),
        MysqlReplicaEpoch::new(1),
        members,
    )
}

fn operation(label: &str, generation: u64) -> MysqlOperationIdentity {
    MysqlOperationIdentity::new(
        AuthorityGeneration::new(label).unwrap(),
        NonZeroU64::new(generation).unwrap(),
    )
}

fn member_number(member: MysqlMemberIndex) -> usize {
    match member {
        MysqlMemberIndex::First => 1,
        MysqlMemberIndex::Second => 2,
        MysqlMemberIndex::Third => 3,
    }
}
