mod common;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use common::{TestRoot, provide_socket};
use kuberic_mysql_adapter::{
    ClockContext, ObservationClock, ObservationRequest, ObserverCredentials, UnixSocketPath,
};
use kuberic_mysql_core::{
    AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, EndpointBinding, Epoch,
    ExactBinding, ExactBindingParts, GroupName, MemberAddress, MemberId, ObservationInstant,
    ObservationProvenance, ObservationSessionId, PartitionId, ProcessSessionId, ReplicaId,
    ReplicaIncarnation, ResourceId, ServerUuid, StorageBinding, ViewId,
};
use kuberic_mysql_service::{
    LifecycleOperation, MysqlInstanceError, MysqlInstanceManager, MysqlInstanceState,
    OwnershipError,
};

#[derive(Clone)]
struct TestClock {
    origin: Instant,
}

impl ObservationClock for TestClock {
    fn now(&self) -> ObservationInstant {
        ObservationInstant::new(
            u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX),
        )
    }

    fn to_std_instant(
        &self,
        instant: ObservationInstant,
    ) -> Result<Instant, kuberic_mysql_adapter::ClockError> {
        self.origin
            .checked_add(Duration::from_millis(instant.tick()))
            .ok_or(kuberic_mysql_adapter::ClockError::UnrepresentableInstant)
    }
}

#[test]
fn one_generation_transitions_and_removes_only_scratch() {
    let root = TestRoot::new("lifecycle");
    let mut manager = MysqlInstanceManager::new(root.config());
    assert_eq!(manager.state(), MysqlInstanceState::Configured);
    manager.initialize().unwrap();
    assert_eq!(manager.state(), MysqlInstanceState::Initialized);
    assert!(matches!(
        manager.initialize(),
        Err(MysqlInstanceError::InvalidState { .. })
    ));

    let socket = provide_socket(
        manager.config().runtime().pid().to_owned(),
        manager.config().runtime().socket().to_owned(),
    );
    manager.start().unwrap();
    assert_eq!(manager.state(), MysqlInstanceState::Running);
    assert_eq!(
        manager.socket_path(),
        Some(manager.config().runtime().socket())
    );

    let names = inventory(&root.scratch);
    for forbidden in ["metadata", "journal", "receipt", "state.json", "adopt"] {
        assert!(
            names.iter().all(|name| !name.contains(forbidden)),
            "{names:?}"
        );
    }

    manager.stop().unwrap();
    socket.join().unwrap();
    assert_eq!(manager.state(), MysqlInstanceState::Stopped);
    assert!(root.data.is_dir());
    assert!(!root.scratch.exists());
    assert!(!manager.config().runtime().socket().exists());
    assert!(matches!(
        manager.start(),
        Err(MysqlInstanceError::InvalidState { .. })
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn observation_rejects_every_socket_except_the_owned_private_uds() {
    let root = TestRoot::new("observation-mismatch");
    let mut manager = MysqlInstanceManager::new(root.config());
    manager.initialize().unwrap();
    let socket_thread = provide_socket(
        manager.config().runtime().pid().to_owned(),
        manager.config().runtime().socket().to_owned(),
    );
    manager.start().unwrap();

    let foreign_path = root.root.join("foreign.sock");
    let foreign = UnixListener::bind(&foreign_path).unwrap();
    let foreign_request = request(&foreign_path);
    let error = manager.observe(foreign_request).await.unwrap_err();
    assert!(matches!(
        error,
        MysqlInstanceError::ObservationSocketMismatch
    ));
    drop(foreign);
    fs::remove_file(&foreign_path).unwrap();

    let owned = manager.config().runtime().socket().to_owned();
    fs::write(manager.config().runtime().pid(), "1\n").unwrap();
    let error = manager.observe(request(&owned)).await.unwrap_err();
    assert!(matches!(
        error,
        MysqlInstanceError::Ownership(OwnershipError::PidMismatch)
    ));
    assert_eq!(manager.state(), MysqlInstanceState::Faulted);
    assert!(manager.socket_path().is_none());
    drop(manager);
    socket_thread.join().unwrap();
}

#[test]
fn pid_mismatch_fails_closed_without_signaling_a_different_process() {
    let root = TestRoot::new("pid-mismatch");
    let mut manager = MysqlInstanceManager::new(root.config());
    manager.initialize().unwrap();
    let socket_thread = provide_socket(
        manager.config().runtime().pid().to_owned(),
        manager.config().runtime().socket().to_owned(),
    );
    manager.start().unwrap();
    fs::write(manager.config().runtime().pid(), "1\n").unwrap();

    let error = manager.stop().unwrap_err();
    assert!(matches!(
        error,
        MysqlInstanceError::Ownership(OwnershipError::PidMismatch)
    ));
    assert_eq!(manager.state(), MysqlInstanceState::Faulted);
    drop(manager);
    socket_thread.join().unwrap();
}

#[test]
fn unexpected_child_exit_is_an_explicit_ownership_failure() {
    let root = TestRoot::new("unexpected-exit");
    let mut manager = MysqlInstanceManager::new(root.config());
    manager.initialize().unwrap();
    let socket_thread = provide_socket(
        manager.config().runtime().pid().to_owned(),
        manager.config().runtime().socket().to_owned(),
    );
    manager.start().unwrap();
    let pid = fs::read_to_string(manager.config().runtime().pid())
        .unwrap()
        .trim()
        .to_owned();
    assert!(
        Command::new("/bin/kill")
            .args(["-KILL", &pid])
            .status()
            .unwrap()
            .success()
    );
    std::thread::sleep(Duration::from_millis(50));

    let error = manager.stop().unwrap_err();
    assert!(matches!(
        error,
        MysqlInstanceError::Ownership(OwnershipError::ChildExited)
    ));
    socket_thread.join().unwrap();
}

#[test]
fn scratch_cleanup_failure_is_explicit_and_preserves_data() {
    let root = TestRoot::new("cleanup-error");
    let mut manager = MysqlInstanceManager::new(root.config());
    manager.initialize().unwrap();
    let socket_thread = provide_socket(
        manager.config().runtime().pid().to_owned(),
        manager.config().runtime().socket().to_owned(),
    );
    manager.start().unwrap();
    fs::set_permissions(&root.root, fs::Permissions::from_mode(0o500)).unwrap();

    let result = manager.stop();
    fs::set_permissions(&root.root, fs::Permissions::from_mode(0o700)).unwrap();
    socket_thread.join().unwrap();
    assert!(matches!(
        result,
        Err(MysqlInstanceError::Io {
            operation: LifecycleOperation::ScratchCleanup,
            ..
        })
    ));
    assert!(root.data.is_dir());
    assert!(root.scratch.is_dir());
}

fn inventory(root: &Path) -> Vec<String> {
    let mut names = Vec::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                pending.push(entry.path());
            }
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    names
}

fn request(socket: &Path) -> ObservationRequest<TestClock> {
    let binding = ExactBinding::new(ExactBindingParts {
        resource: ResourceId::new("resource").unwrap(),
        partition: PartitionId::new("partition").unwrap(),
        replica: ReplicaId::new("replica").unwrap(),
        incarnation: ReplicaIncarnation::new("incarnation").unwrap(),
        process_session: ProcessSessionId::new("process").unwrap(),
        observation_session: ObservationSessionId::new("observation").unwrap(),
        attempt: AttemptId::new("attempt").unwrap(),
        endpoint: EndpointBinding::new("endpoint").unwrap(),
        storage: StorageBinding::new("storage").unwrap(),
        server_uuid: ServerUuid::new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
        group_name: GroupName::new("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
        member_id: MemberId::new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
        member_address: MemberAddress::new("private").unwrap(),
        configuration: ConfigurationId::new("configuration").unwrap(),
        epoch: Epoch::new("epoch").unwrap(),
        authority_generation: AuthorityGeneration::new("authority").unwrap(),
        view_id: ViewId::new("unstarted").unwrap(),
        credential_generation: CredentialGeneration::new("credential").unwrap(),
    });
    let clock = TestClock {
        origin: Instant::now(),
    };
    ObservationRequest::new(
        binding.clone(),
        UnixSocketPath::new(socket).unwrap(),
        ObserverCredentials::new("root", "").unwrap(),
        ObservationProvenance::new("service-contract", binding.parts().attempt.clone()).unwrap(),
        ClockContext::new(clock, ObservationInstant::new(1_000)).unwrap(),
    )
    .unwrap()
}
