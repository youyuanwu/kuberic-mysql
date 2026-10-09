use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use kuberic_mysql::adapter::{
    AdapterDiagnostic, ClockContext, ObservationClock, ObservationRequest, ObserverCredentials,
    PlaceholderKind, UnixSocketPath,
};
use kuberic_mysql::core::{
    AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, EndpointBinding, Epoch,
    ExactBinding, ExactBindingParts, GroupName, MemberAddress, MemberId, ObservationInstant,
    ObservationOutcome, ObservationProvenance, ObservationSessionId, PartitionId, ProcessSessionId,
    ReplicaId, ReplicaIncarnation, ResourceId, ServerUuid, StorageBinding, ViewId,
};
use kuberic_mysql::service::{MysqlInstanceConfig, MysqlInstanceManager, MysqlOperationTimeouts};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder};

#[derive(Clone)]
struct SystemClock {
    origin: Instant,
}

impl ObservationClock for SystemClock {
    fn now(&self) -> ObservationInstant {
        ObservationInstant::new(
            u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX),
        )
    }

    fn to_std_instant(
        &self,
        instant: ObservationInstant,
    ) -> Result<Instant, kuberic_mysql::adapter::ClockError> {
        self.origin
            .checked_add(Duration::from_millis(instant.tick()))
            .ok_or(kuberic_mysql::adapter::ClockError::UnrepresentableInstant)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn one_fresh_owned_instance_lifecycle() {
    let root = live_root();
    let data = root.join("data");
    let scratch = root.join("scratch");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();

    let timeouts = MysqlOperationTimeouts::new(
        Duration::from_secs(90),
        Duration::from_secs(60),
        Duration::from_secs(30),
        Duration::from_secs(10),
    )
    .unwrap();
    let config = MysqlInstanceConfig::new(
        "/usr/sbin/mysqld",
        "/usr/bin/aa-exec",
        &data,
        &scratch,
        timeouts,
    )
    .unwrap();
    let mut manager = MysqlInstanceManager::new(config);
    manager.initialize().unwrap();
    manager.start().unwrap();

    let socket = manager.socket_path().unwrap().to_owned();
    let mut connection = connect_root(&socket).await;
    let (server_uuid, group_name): (String, String) = connection
        .query_first("SELECT @@GLOBAL.server_uuid, @@GLOBAL.group_replication_group_name")
        .await
        .unwrap()
        .expect("one server identity row");
    connection.disconnect().await.unwrap();

    let binding = binding(&server_uuid, &group_name);
    let clock = SystemClock {
        origin: Instant::now(),
    };
    let request = ObservationRequest::new(
        binding.clone(),
        UnixSocketPath::new(&socket).unwrap(),
        ObserverCredentials::new("root", "").unwrap(),
        ObservationProvenance::new("service-live-mysql-8.4.11", binding.parts().attempt.clone())
            .unwrap(),
        ClockContext::new(clock, ObservationInstant::new(30_000)).unwrap(),
    )
    .unwrap();
    let report = manager.observe(request).await.unwrap();
    manager.stop().unwrap();
    assert!(!socket.exists());
    assert!(data.is_dir());
    assert!(!scratch.exists());
    fs::remove_dir_all(&root).unwrap();

    assert!(
        matches!(report.outcome(), ObservationOutcome::Absent(_)),
        "unexpected outcome: {:?}; diagnostic: {:?}",
        report.outcome(),
        report.diagnostic()
    );
    assert!(
        matches!(
            report.diagnostic(),
            AdapterDiagnostic::Absent { .. }
                | AdapterDiagnostic::Placeholder(
                    PlaceholderKind::NeverStarted | PlaceholderKind::Stopped
                )
        ),
        "unexpected diagnostic: {:?}",
        report.diagnostic()
    );
}

async fn connect_root(socket: &std::path::Path) -> Conn {
    let options = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(1)
        .socket(Some(socket.to_str().unwrap().to_owned()))
        .user(Some("root".to_owned()));
    Conn::new(options).await.unwrap()
}

fn live_root() -> PathBuf {
    std::env::temp_dir().join(format!("kms-live-{}", std::process::id()))
}

fn binding(server_uuid: &str, group_name: &str) -> ExactBinding {
    ExactBinding::new(ExactBindingParts {
        resource: ResourceId::new("resource").unwrap(),
        partition: PartitionId::new("partition").unwrap(),
        replica: ReplicaId::new("replica").unwrap(),
        incarnation: ReplicaIncarnation::new("incarnation").unwrap(),
        process_session: ProcessSessionId::new("process").unwrap(),
        observation_session: ObservationSessionId::new("observation").unwrap(),
        attempt: AttemptId::new("attempt").unwrap(),
        endpoint: EndpointBinding::new("private-uds").unwrap(),
        storage: StorageBinding::new("fresh-data-root").unwrap(),
        server_uuid: ServerUuid::new(server_uuid).unwrap(),
        group_name: GroupName::new(group_name).unwrap(),
        member_id: MemberId::new(server_uuid).unwrap(),
        member_address: MemberAddress::new("127.0.0.1:33061").unwrap(),
        configuration: ConfigurationId::new("single-instance").unwrap(),
        epoch: Epoch::new("fresh").unwrap(),
        authority_generation: AuthorityGeneration::new("one").unwrap(),
        view_id: ViewId::new("group-replication-not-started").unwrap(),
        credential_generation: CredentialGeneration::new("initialize-insecure").unwrap(),
    })
}
