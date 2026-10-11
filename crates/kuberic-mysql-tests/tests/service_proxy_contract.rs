#[path = "service_common/mod.rs"]
mod service_common;

use std::collections::BTreeSet;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kuberic_mysql::adapter::UnixSocketPath;
use kuberic_mysql::core::{
    AttemptId, CredentialGeneration, EndpointBinding, GroupName, GtidSet, MemberAddress,
    ProcessSessionId, ServerUuid, StorageBinding,
};
use kuberic_mysql::service::{
    AccountProvisioningEvidence, ApplicationAccountReceipt, ApplicationCredential, AttemptContext,
    BackendSessionIdentity, ControlCredential, ControlCredentialRole, MemberControlBinding,
    MemberMutationAuthority, MysqlClientProxy, MysqlMemberIndex, MysqlProxyControlBoundary,
    MysqlProxyError, MysqlProxySettlementReason, NativeIdentityEnrollment,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpStream, UnixListener, UnixStream};
use tokio::sync::Notify;

use service_common::{GROUP_UUID, TestRoot};

const CAPABILITIES: u32 = 0x0038_a20d;
const CLIENT_CAPABILITIES: u32 = CAPABILITIES & !0x0000_0008;
const CONNECTION_ID: u32 = 91;

#[derive(Clone, Copy)]
enum AuthMode {
    Fast,
    Full,
    Switch,
    Rsa,
    WithheldGreeting,
    DelayedGreeting,
    MalformedGreeting,
    CancelDuringConnect,
}

#[derive(Clone)]
struct BackendPlan {
    auth: AuthMode,
    block_query: bool,
}

struct FakeControl {
    socket: UnixSocketPath,
    prepared: usize,
    removed: usize,
    terminated: BTreeSet<u64>,
    fail_socket: bool,
    fail_remove: bool,
    fail_terminate: bool,
    terminate_started: Option<Arc<AtomicU32>>,
    terminate_release: Option<Arc<Notify>>,
}

impl MysqlProxyControlBoundary for FakeControl {
    fn contain_proxy_preparation(
        &mut self,
        _authority: &MemberMutationAuthority,
        _attempt: &AttemptContext,
    ) -> Result<(), MysqlProxyError> {
        Ok(())
    }

    fn proxy_backend_socket(
        &mut self,
        _authority: &MemberMutationAuthority,
    ) -> Result<UnixSocketPath, MysqlProxyError> {
        if self.fail_socket {
            Err(MysqlProxyError::InvalidState)
        } else {
            Ok(self.socket.clone())
        }
    }

    async fn prepare_application_account(
        &mut self,
        _authority: &MemberMutationAuthority,
        credential: &ApplicationCredential,
        _attempt: &AttemptContext,
    ) -> Result<ApplicationAccountReceipt, MysqlProxyError> {
        self.prepared += 1;
        Ok(ApplicationAccountReceipt::new(credential))
    }

    async fn remove_application_account(
        &mut self,
        _authority: &MemberMutationAuthority,
        _receipt: &ApplicationAccountReceipt,
        _attempt: &AttemptContext,
    ) -> Result<(), MysqlProxyError> {
        self.removed += 1;
        if self.fail_remove {
            Err(MysqlProxyError::InvalidState)
        } else {
            Ok(())
        }
    }

    async fn terminate_proxy_backend(
        &mut self,
        authority: &MemberMutationAuthority,
        session: &BackendSessionIdentity,
        _attempt: &AttemptContext,
        _deadline: Instant,
    ) -> Result<(), MysqlProxyError> {
        if let Some(started) = self.terminate_started.as_ref() {
            started.fetch_add(1, Ordering::Release);
        }
        if let Some(release) = self.terminate_release.as_ref() {
            release.notified().await;
        }
        if self.fail_terminate {
            return Err(MysqlProxyError::InvalidState);
        }
        assert_eq!(session.authority(), authority);
        assert_eq!(session.socket(), &self.socket);
        assert_eq!(session.account().username(), "application_local");
        assert_eq!(
            session.account().generation().as_str(),
            "application-generation"
        );
        self.terminated.insert(session.connection().get());
        Ok(())
    }
}

struct Fixture {
    _root: TestRoot,
    proxy: MysqlClientProxy,
    control: FakeControl,
    authority: MemberMutationAuthority,
    attempt: AttemptContext,
    backend_commands: Arc<Mutex<Vec<u8>>>,
    backend_settled: Arc<AtomicU32>,
    backend: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new(label: &str, plan: BackendPlan) -> Self {
        Self::new_with_budget(
            label,
            plan,
            Duration::from_secs(60),
            Duration::from_secs(20),
        )
        .await
    }

    async fn new_with_budget(
        label: &str,
        plan: BackendPlan,
        total: Duration,
        cleanup: Duration,
    ) -> Self {
        let root = TestRoot::new(label);
        let socket_path = root.root.join("backend.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let socket = UnixSocketPath::new(&socket_path).unwrap();
        let attempt = AttemptContext::new(
            AttemptId::new(format!("{label}-attempt")).unwrap(),
            Instant::now() + total,
            cleanup,
        )
        .unwrap();
        let commands = Arc::new(Mutex::new(Vec::new()));
        let backend_commands = Arc::clone(&commands);
        let settled = Arc::new(AtomicU32::new(0));
        let backend_settled = Arc::clone(&settled);
        let backend_attempt = attempt.clone();
        let backend = tokio::spawn(async move {
            run_fake_backend(
                listener,
                plan,
                backend_commands,
                backend_settled,
                backend_attempt,
            )
            .await;
        });
        let authority = authority(&attempt);
        let mut control = FakeControl {
            socket,
            prepared: 0,
            removed: 0,
            terminated: BTreeSet::new(),
            fail_socket: false,
            fail_remove: false,
            fail_terminate: false,
            terminate_started: None,
            terminate_release: None,
        };
        let credential = ApplicationCredential::new(
            attempt.attempt().clone(),
            CredentialGeneration::new("application-generation").unwrap(),
            "application_local",
            "secret-value",
        )
        .unwrap();
        let proxy = MysqlClientProxy::prepare(
            &mut control,
            &authority,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
            credential,
            &attempt,
        )
        .await
        .unwrap();
        Self {
            _root: root,
            proxy,
            control,
            authority,
            attempt,
            backend_commands: commands,
            backend_settled: settled,
            backend,
        }
    }

    fn publish(&mut self) -> SocketAddr {
        self.proxy
            .publish(nonzero(1), true, true, &self.attempt)
            .unwrap()
    }

    async fn revoke(&mut self) -> kuberic_mysql::service::MysqlProxyContainmentReceipt {
        self.proxy
            .revoke(
                &mut self.control,
                &self.authority,
                nonzero(1),
                &self.attempt,
            )
            .await
            .unwrap()
    }

    async fn cleanup(mut self) {
        self.proxy
            .cleanup_account(&mut self.control, &self.authority, &self.attempt)
            .await
            .unwrap();
        assert_eq!(self.control.prepared, 1);
        assert_eq!(self.control.removed, 1);
        self.backend.abort();
        let _ = self.backend.await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn warmed_fast_auth_query_ping_quit_and_cleanup_are_bounded() {
    let mut fixture = Fixture::new(
        "proxy-success",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: false,
        },
    )
    .await;
    let address = fixture.publish();
    let mut client = authenticate(address, "application_local", CLIENT_CAPABILITIES).await;

    write_packet(&mut client, 0, b"\x03ok").await;
    assert_eq!(read_packet(&mut client).await.1[0], 0x00);
    write_packet(&mut client, 0, b"\x03err").await;
    assert_eq!(read_packet(&mut client).await.1[0], 0xff);
    write_packet(&mut client, 0, b"\x03rows").await;
    for _ in 0..5 {
        let _ = read_packet(&mut client).await;
    }
    for query in [b"null-first".as_slice(), b"null-later", b"null-all"] {
        let mut command = vec![0x03];
        command.extend(query);
        write_packet(&mut client, 0, &command).await;
        for _ in 0..6 {
            let _ = read_packet(&mut client).await;
        }
    }
    write_packet(&mut client, 0, b"\x0e").await;
    assert_eq!(read_packet(&mut client).await.1[0], 0x00);
    write_packet(&mut client, 0, b"\x01").await;
    assert_eq!(client.read(&mut [0_u8; 1]).await.unwrap(), 0);

    wait_sessions_zero(&fixture.proxy).await;
    let receipt = fixture.revoke().await;
    assert!(receipt.cleanup_complete());
    assert!(receipt.handoff_eligible());
    assert_eq!(receipt.diagnostics().normal_completions(), 8);
    assert_eq!(receipt.diagnostics().auth_rejections(), 0);
    assert!(
        fixture
            .control
            .terminated
            .contains(&u64::from(CONNECTION_ID))
    );
    assert_eq!(
        *fixture.backend_commands.lock().unwrap(),
        vec![0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x0e, 0x01]
    );
    fixture.cleanup().await;
}

#[tokio::test(flavor = "current_thread")]
async fn exact_username_capabilities_and_warmed_auth_fail_closed() {
    for (label, username, capabilities, auth) in [
        ("proxy-user", "root", CLIENT_CAPABILITIES, AuthMode::Fast),
        (
            "proxy-cap",
            "application_local",
            CLIENT_CAPABILITIES | 0x0000_0800,
            AuthMode::Fast,
        ),
        (
            "proxy-cold",
            "application_local",
            CLIENT_CAPABILITIES,
            AuthMode::Full,
        ),
    ] {
        let mut fixture = Fixture::new(
            label,
            BackendPlan {
                auth,
                block_query: false,
            },
        )
        .await;
        let address = fixture.publish();
        let result = try_authenticate(address, username, capabilities).await;
        assert!(result.is_err(), "{label} unexpectedly authenticated");
        wait_sessions_zero(&fixture.proxy).await;
        let receipt = fixture.revoke().await;
        assert!(receipt.handoff_eligible());
        assert!(receipt.diagnostics().auth_rejections() >= 1);
        assert!(fixture.backend_commands.lock().unwrap().is_empty());
        fixture.cleanup().await;
    }

    let mut plugin = Fixture::new(
        "proxy-plugin",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: false,
        },
    )
    .await;
    let address = plugin.publish();
    assert!(
        try_authenticate_with_plugin(
            address,
            "application_local",
            CLIENT_CAPABILITIES,
            "mysql_native_password",
        )
        .await
        .is_err()
    );
    wait_sessions_zero(&plugin.proxy).await;
    assert!(plugin.revoke().await.handoff_eligible());
    plugin.cleanup().await;

    for (label, auth) in [
        ("proxy-auth-switch", AuthMode::Switch),
        ("proxy-auth-rsa", AuthMode::Rsa),
    ] {
        let mut fixture = Fixture::new(
            label,
            BackendPlan {
                auth,
                block_query: false,
            },
        )
        .await;
        let address = fixture.publish();
        assert!(
            try_authenticate(address, "application_local", CLIENT_CAPABILITIES)
                .await
                .is_err()
        );
        wait_sessions_zero(&fixture.proxy).await;
        assert!(fixture.revoke().await.handoff_eligible());
        fixture.cleanup().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn unsupported_command_and_oversized_packet_never_dispatch() {
    let mut fixture = Fixture::new(
        "proxy-reject",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: false,
        },
    )
    .await;
    let address = fixture.publish();
    let mut client = authenticate(address, "application_local", CLIENT_CAPABILITIES).await;
    write_packet(&mut client, 0, b"\x16prepared").await;
    assert_eq!(client.read(&mut [0_u8; 1]).await.unwrap(), 0);
    wait_sessions_zero(&fixture.proxy).await;

    let mut oversized = authenticate(address, "application_local", CLIENT_CAPABILITIES).await;
    oversized
        .write_all(&[0x01, 0x00, 0x10, 0x00])
        .await
        .unwrap();
    assert_eq!(oversized.read(&mut [0_u8; 1]).await.unwrap(), 0);
    wait_sessions_zero(&fixture.proxy).await;

    for bytes in [
        vec![0xff, 0xff, 0xff, 0x00],
        vec![0x02, 0x00],
        vec![0x04, 0x00, 0x00, 0x00, 0x03],
    ] {
        let mut client = authenticate(address, "application_local", CLIENT_CAPABILITIES).await;
        client.write_all(&bytes).await.unwrap();
        client.shutdown().await.unwrap();
        wait_sessions_zero(&fixture.proxy).await;
    }

    let mut maximum = authenticate(address, "application_local", CLIENT_CAPABILITIES).await;
    let mut payload = vec![0_u8; 1024 * 1024];
    payload[0] = 0x16;
    write_packet(&mut maximum, 0, &payload).await;
    assert_eq!(maximum.read(&mut [0_u8; 1]).await.unwrap(), 0);
    wait_sessions_zero(&fixture.proxy).await;
    let receipt = fixture.revoke().await;
    assert!(receipt.handoff_eligible());
    assert!(fixture.backend_commands.lock().unwrap().is_empty());
    fixture.cleanup().await;
}

#[tokio::test(flavor = "current_thread")]
async fn ssl_compression_local_file_multi_statement_and_multi_result_capabilities_are_rejected() {
    for (label, capability) in [
        ("proxy-cap-ssl", 0x0000_0800),
        ("proxy-cap-compress", 0x0000_0020),
        ("proxy-cap-local-file", 0x0000_0080),
        ("proxy-cap-multi-statement", 0x0001_0000),
        ("proxy-cap-multi-result", 0x0002_0000),
        ("proxy-cap-prepared-result", 0x0004_0000),
    ] {
        let mut fixture = Fixture::new(
            label,
            BackendPlan {
                auth: AuthMode::Fast,
                block_query: false,
            },
        )
        .await;
        let address = fixture.publish();
        assert!(
            try_authenticate(
                address,
                "application_local",
                CLIENT_CAPABILITIES | capability,
            )
            .await
            .is_err()
        );
        wait_sessions_zero(&fixture.proxy).await;
        assert!(fixture.revoke().await.handoff_eligible());
        assert!(fixture.backend_commands.lock().unwrap().is_empty());
        fixture.cleanup().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn all_partial_permission_modes_are_rejected() {
    for (read, write) in [(false, false), (true, false), (false, true)] {
        let mut fixture = Fixture::new(
            "proxy-permission",
            BackendPlan {
                auth: AuthMode::Fast,
                block_query: false,
            },
        )
        .await;
        assert!(matches!(
            fixture
                .proxy
                .publish(nonzero(1), read, write, &fixture.attempt),
            Err(MysqlProxyError::BothPermissionsRequired)
        ));
        fixture.cleanup().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn preparation_preserves_primary_and_account_cleanup_failures() {
    let root = TestRoot::new("proxy-prepare-cleanup");
    let socket_path = root.root.join("backend.sock");
    let _listener = UnixListener::bind(&socket_path).unwrap();
    let attempt = AttemptContext::new(
        AttemptId::new("proxy-prepare-cleanup-attempt").unwrap(),
        Instant::now() + Duration::from_secs(30),
        Duration::from_secs(10),
    )
    .unwrap();
    let authority = authority(&attempt);
    let mut control = FakeControl {
        socket: UnixSocketPath::new(&socket_path).unwrap(),
        prepared: 0,
        removed: 0,
        terminated: BTreeSet::new(),
        fail_socket: true,
        fail_remove: true,
        fail_terminate: false,
        terminate_started: None,
        terminate_release: None,
    };
    let credential = ApplicationCredential::new(
        attempt.attempt().clone(),
        CredentialGeneration::new("application-generation").unwrap(),
        "application_local",
        "secret-value",
    )
    .unwrap();
    let error = match MysqlClientProxy::prepare(
        &mut control,
        &authority,
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)),
        credential,
        &attempt,
    )
    .await
    {
        Ok(_) => panic!("preparation unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(matches!(error, MysqlProxyError::Cleanup { .. }));
    assert_eq!(control.prepared, 1);
    assert_eq!(control.removed, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn account_removal_requires_full_containment_and_can_be_retried_after_failure() {
    let mut fixture = Fixture::new(
        "proxy-cleanup-order",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: false,
        },
    )
    .await;
    fixture.publish();
    assert!(matches!(
        fixture
            .proxy
            .cleanup_account(&mut fixture.control, &fixture.authority, &fixture.attempt)
            .await,
        Err(MysqlProxyError::InvalidState)
    ));
    fixture.proxy.seal_now();
    assert!(matches!(
        fixture
            .proxy
            .cleanup_account(&mut fixture.control, &fixture.authority, &fixture.attempt)
            .await,
        Err(MysqlProxyError::InvalidState)
    ));
    let containment = fixture.revoke().await;
    assert_eq!(
        containment.settlement_reason(),
        MysqlProxySettlementReason::TerminalSeal
    );
    assert!(containment.handoff_eligible());
    fixture.control.fail_remove = true;
    assert!(
        fixture
            .proxy
            .cleanup_account(&mut fixture.control, &fixture.authority, &fixture.attempt)
            .await
            .is_err()
    );
    assert_eq!(fixture.proxy.snapshot().diagnostics().cleanup_failures(), 1);
    assert!(fixture.proxy.credential_cleared());
    fixture.control.fail_remove = false;
    fixture
        .proxy
        .cleanup_account(&mut fixture.control, &fixture.authority, &fixture.attempt)
        .await
        .unwrap();
    assert_eq!(fixture.control.removed, 2);
    fixture.backend.abort();
    let _ = fixture.backend.await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_termination_failure_never_credits_containment_and_is_retryable() {
    let mut fixture = Fixture::new(
        "proxy-kill-failure",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: false,
        },
    )
    .await;
    let address = fixture.publish();
    let _client = authenticate(address, "application_local", CLIENT_CAPABILITIES).await;
    await_session_count(&fixture.proxy, 1).await;
    fixture.control.fail_terminate = true;
    assert!(
        fixture
            .proxy
            .revoke(
                &mut fixture.control,
                &fixture.authority,
                nonzero(1),
                &fixture.attempt,
            )
            .await
            .is_err()
    );
    assert!(!fixture.proxy.is_fully_contained());
    assert!(matches!(
        fixture
            .proxy
            .cleanup_account(&mut fixture.control, &fixture.authority, &fixture.attempt)
            .await,
        Err(MysqlProxyError::InvalidState)
    ));
    fixture.control.fail_terminate = false;
    assert!(fixture.revoke().await.handoff_eligible());
    fixture.cleanup().await;
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_revocation_future_retains_armed_task_and_backend_ownership() {
    let mut fixture = Fixture::new(
        "proxy-drop-revoke",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: false,
        },
    )
    .await;
    let address = fixture.publish();
    let _client = authenticate(address, "application_local", CLIENT_CAPABILITIES).await;
    await_session_count(&fixture.proxy, 1).await;
    let started = Arc::new(AtomicU32::new(0));
    let release = Arc::new(Notify::new());
    fixture.control.terminate_started = Some(Arc::clone(&started));
    fixture.control.terminate_release = Some(Arc::clone(&release));
    let mut revoke = Box::pin(fixture.proxy.revoke(
        &mut fixture.control,
        &fixture.authority,
        nonzero(1),
        &fixture.attempt,
    ));
    tokio::select! {
        result = &mut revoke => panic!("revocation unexpectedly completed: {result:?}"),
        () = wait_atomic_count(&started, 1) => {}
    }
    drop(revoke);
    assert!(!fixture.proxy.snapshot().accepting());
    assert!(!fixture.proxy.is_fully_contained());
    release.notify_one();
    let receipt = fixture.revoke().await;
    assert!(receipt.handoff_eligible());
    fixture.cleanup().await;
}

#[tokio::test(flavor = "current_thread")]
async fn active_revocation_contains_native_work_but_is_not_handoff_eligible() {
    let mut fixture = Fixture::new(
        "proxy-active",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: true,
        },
    )
    .await;
    let address = fixture.publish();
    let mut client = authenticate(address, "application_local", CLIENT_CAPABILITIES).await;
    write_packet(&mut client, 0, b"\x03block").await;
    wait_active_command(&fixture.proxy).await;
    fixture.attempt.cancel();
    let receipt = fixture.revoke().await;
    assert!(receipt.cleanup_complete());
    assert!(receipt.cancelled_commands());
    assert!(receipt.ambiguous_effect());
    assert!(!receipt.handoff_eligible());
    assert!(
        fixture
            .control
            .terminated
            .contains(&u64::from(CONNECTION_ID))
    );
    assert_eq!(client.read(&mut [0_u8; 1]).await.unwrap(), 0);
    fixture.cleanup().await;
}

#[tokio::test(flavor = "current_thread")]
async fn publication_honors_shared_cancellation_and_deadline() {
    let mut cancelled = Fixture::new(
        "proxy-cancel",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: false,
        },
    )
    .await;
    cancelled.attempt.cancel();
    assert!(matches!(
        cancelled
            .proxy
            .publish(nonzero(1), true, true, &cancelled.attempt),
        Err(MysqlProxyError::Cancelled)
    ));
    cancelled.cleanup().await;

    let mut expired = Fixture::new_with_budget(
        "proxy-deadline",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: false,
        },
        Duration::from_millis(300),
        Duration::from_millis(150),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(175)).await;
    assert!(matches!(
        expired
            .proxy
            .publish(nonzero(1), true, true, &expired.attempt),
        Err(MysqlProxyError::Deadline(_))
    ));
    expired.cleanup().await;
}

#[tokio::test(flavor = "current_thread")]
async fn published_attempt_cancellation_seals_idle_and_poised_command_work_without_revoke() {
    let mut idle = Fixture::new(
        "proxy-published-cancel-idle",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: false,
        },
    )
    .await;
    let address = idle.publish();
    let _client = authenticate(address, "application_local", CLIENT_CAPABILITIES).await;
    idle.attempt.cancel();
    wait_sessions_zero(&idle.proxy).await;
    assert!(!idle.proxy.snapshot().accepting());
    assert!(TcpStream::connect(address).await.is_err());
    let receipt = idle.revoke().await;
    assert_eq!(
        receipt.settlement_reason(),
        MysqlProxySettlementReason::AttemptCancellation
    );
    assert!(receipt.diagnostics().cancellations() >= 1);
    assert_eq!(receipt.diagnostics().deadlines(), 0);
    idle.cleanup().await;

    let mut poised = Fixture::new(
        "proxy-published-cancel-command",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: false,
        },
    )
    .await;
    let address = poised.publish();
    let mut client = authenticate(address, "application_local", CLIENT_CAPABILITIES).await;
    poised.attempt.cancel();
    let _ = write_packet_result(&mut client, 0, b"\x03must_not_dispatch").await;
    wait_sessions_zero(&poised.proxy).await;
    assert!(poised.backend_commands.lock().unwrap().is_empty());
    assert!(!poised.proxy.snapshot().accepting());
    let receipt = poised.revoke().await;
    assert_eq!(
        receipt.settlement_reason(),
        MysqlProxySettlementReason::AttemptCancellation
    );
    assert!(receipt.diagnostics().cancellations() >= 1);
    assert_eq!(receipt.diagnostics().deadlines(), 0);
    poised.cleanup().await;
}

#[tokio::test(flavor = "current_thread")]
async fn published_idle_listener_expires_and_stops_serving_without_revoke() {
    let mut fixture = Fixture::new_with_budget(
        "proxy-published-expiry",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: false,
        },
        Duration::from_millis(240),
        Duration::from_millis(120),
    )
    .await;
    let address = fixture.publish();
    tokio::time::sleep(Duration::from_millis(145)).await;
    assert!(!fixture.proxy.snapshot().accepting());
    assert!(TcpStream::connect(address).await.is_err());
    let receipt = fixture.revoke().await;
    assert_eq!(
        receipt.settlement_reason(),
        MysqlProxySettlementReason::WorkDeadlineExpired
    );
    assert!(receipt.handoff_eligible());
    fixture.cleanup().await;
}

#[tokio::test(flavor = "current_thread")]
async fn authenticated_idle_and_active_work_cutoff_expiry_record_deadlines_not_cancellation() {
    let mut idle = Fixture::new_with_budget(
        "proxy-published-expiry-authenticated-idle",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: false,
        },
        Duration::from_millis(500),
        Duration::from_millis(200),
    )
    .await;
    let address = idle.publish();
    let _client = authenticate(address, "application_local", CLIENT_CAPABILITIES).await;
    wait_sessions_zero(&idle.proxy).await;
    let receipt = idle.revoke().await;
    assert_eq!(
        receipt.settlement_reason(),
        MysqlProxySettlementReason::WorkDeadlineExpired
    );
    assert_eq!(receipt.diagnostics().deadlines(), 1);
    assert_eq!(receipt.diagnostics().cancellations(), 0);
    assert_eq!(receipt.diagnostics().revocations(), 0);
    assert!(receipt.handoff_eligible());
    idle.cleanup().await;

    let mut active = Fixture::new_with_budget(
        "proxy-published-expiry-active-command",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: true,
        },
        Duration::from_millis(500),
        Duration::from_millis(200),
    )
    .await;
    let address = active.publish();
    let mut client = authenticate(address, "application_local", CLIENT_CAPABILITIES).await;
    write_packet(&mut client, 0, b"\x03block").await;
    wait_active_command(&active.proxy).await;
    wait_sessions_zero(&active.proxy).await;
    let receipt = active.revoke().await;
    assert_eq!(
        receipt.settlement_reason(),
        MysqlProxySettlementReason::WorkDeadlineExpired
    );
    assert_eq!(receipt.diagnostics().deadlines(), 1);
    assert_eq!(receipt.diagnostics().cancellations(), 0);
    assert_eq!(receipt.diagnostics().revocations(), 0);
    assert!(receipt.cancelled_commands());
    assert!(receipt.ambiguous_effect());
    assert!(!receipt.handoff_eligible());
    active.cleanup().await;
}

#[tokio::test(flavor = "current_thread")]
async fn unidentified_pre_greeting_backends_never_credit_vacuous_native_absence() {
    for (index, auth) in [
        AuthMode::WithheldGreeting,
        AuthMode::DelayedGreeting,
        AuthMode::MalformedGreeting,
    ]
    .into_iter()
    .enumerate()
    {
        let mut fixture = Fixture::new(
            &format!("proxy-unidentified-{index}"),
            BackendPlan {
                auth,
                block_query: false,
            },
        )
        .await;
        let address = fixture.publish();
        let _client = TcpStream::connect(address).await.unwrap();
        if matches!(auth, AuthMode::MalformedGreeting) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        } else {
            await_session_count(&fixture.proxy, 1).await;
            fixture.attempt.cancel();
        }
        wait_sessions_zero(&fixture.proxy).await;
        assert_eq!(fixture.proxy.snapshot().unidentified_backends(), 1);
        assert!(matches!(
            fixture
                .proxy
                .revoke(
                    &mut fixture.control,
                    &fixture.authority,
                    nonzero(1),
                    &fixture.attempt,
                )
                .await,
            Err(MysqlProxyError::UnidentifiedBackend)
        ));
        wait_backend_settled(&fixture.backend_settled, 1).await;
        assert!(!fixture.proxy.is_fully_contained());
        fixture.backend.abort();
        let _ = fixture.backend.await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn accept_before_connect_completion_cancellation_retains_backend_obligation() {
    let mut fixture = Fixture::new(
        "proxy-connect-cancel",
        BackendPlan {
            auth: AuthMode::CancelDuringConnect,
            block_query: false,
        },
    )
    .await;
    let address = fixture.publish();
    let _client = TcpStream::connect(address).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while fixture.attempt.cancellation_state()
        == kuberic_mysql::service::AttemptCancellationState::Active
    {
        assert!(
            Instant::now() < deadline,
            "backend never accepted connection"
        );
        tokio::task::yield_now().await;
    }
    wait_sessions_zero(&fixture.proxy).await;
    assert_eq!(fixture.backend_settled.load(Ordering::Acquire), 0);
    assert_eq!(fixture.proxy.snapshot().unidentified_backends(), 1);
    assert!(matches!(
        fixture
            .proxy
            .revoke(
                &mut fixture.control,
                &fixture.authority,
                nonzero(1),
                &fixture.attempt,
            )
            .await,
        Err(MysqlProxyError::UnidentifiedBackend)
    ));
    assert!(!fixture.proxy.is_fully_contained());
    fixture.backend.abort();
    let _ = fixture.backend.await;
}

#[tokio::test(flavor = "current_thread")]
async fn idle_and_transaction_holding_sessions_are_terminated_without_ambiguity() {
    let mut fixture = Fixture::new(
        "proxy-idle",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: false,
        },
    )
    .await;
    let address = fixture.publish();
    let _idle = authenticate(address, "application_local", CLIENT_CAPABILITIES).await;
    let mut transaction = authenticate(address, "application_local", CLIENT_CAPABILITIES).await;
    write_packet(&mut transaction, 0, b"\x03transaction").await;
    assert_eq!(read_packet(&mut transaction).await.1[0], 0x00);
    await_session_count(&fixture.proxy, 2).await;
    let receipt = fixture.revoke().await;
    assert_eq!(
        receipt.settlement_reason(),
        MysqlProxySettlementReason::ExplicitRevocation
    );
    assert_eq!(receipt.diagnostics().revocations(), 2);
    assert_eq!(receipt.diagnostics().cancellations(), 0);
    assert_eq!(receipt.diagnostics().deadlines(), 0);
    assert!(!receipt.cancelled_commands());
    assert!(!receipt.ambiguous_effect());
    assert!(receipt.handoff_eligible());
    fixture.cleanup().await;
}

#[tokio::test(flavor = "current_thread")]
async fn local_infile_more_results_and_sequence_errors_are_ambiguous_and_ineligible() {
    for query in [
        b"local".as_slice(),
        b"more".as_slice(),
        b"sequence".as_slice(),
    ] {
        let mut fixture = Fixture::new(
            "proxy-result",
            BackendPlan {
                auth: AuthMode::Fast,
                block_query: false,
            },
        )
        .await;
        let address = fixture.publish();
        let mut client = authenticate(address, "application_local", CLIENT_CAPABILITIES).await;
        let mut command = vec![0x03];
        command.extend(query);
        write_packet(&mut client, 0, &command).await;
        let mut discarded = Vec::new();
        client.read_to_end(&mut discarded).await.unwrap();
        wait_sessions_zero(&fixture.proxy).await;
        let receipt = fixture.revoke().await;
        assert!(receipt.ambiguous_effect());
        assert!(!receipt.handoff_eligible());
        fixture.cleanup().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_ok_err_column_eof_and_row_packets_are_ambiguous_and_ineligible() {
    for (index, query) in [
        b"ok-short".as_slice(),
        b"ok-lenenc".as_slice(),
        b"err-short".as_slice(),
        b"err-marker".as_slice(),
        b"column-count".as_slice(),
        b"column-short".as_slice(),
        b"metadata-eof".as_slice(),
        b"metadata-more".as_slice(),
        b"row-short".as_slice(),
        b"row-extra".as_slice(),
        b"row-more".as_slice(),
    ]
    .into_iter()
    .enumerate()
    {
        let mut fixture = Fixture::new(
            &format!("proxy-malformed-{index}"),
            BackendPlan {
                auth: AuthMode::Fast,
                block_query: false,
            },
        )
        .await;
        let address = fixture.publish();
        let mut client = authenticate(address, "application_local", CLIENT_CAPABILITIES).await;
        let mut command = vec![0x03];
        command.extend(query);
        write_packet(&mut client, 0, &command).await;
        let mut discarded = Vec::new();
        client.read_to_end(&mut discarded).await.unwrap();
        wait_sessions_zero(&fixture.proxy).await;
        let receipt = fixture.revoke().await;
        assert!(receipt.ambiguous_effect(), "{index} was credited as normal");
        assert!(!receipt.handoff_eligible());
        fixture.cleanup().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn listener_revocation_race_admits_no_late_backend_command() {
    let mut fixture = Fixture::new(
        "proxy-race",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: false,
        },
    )
    .await;
    let address = fixture.publish();
    let _unauthed = TcpStream::connect(address).await.unwrap();
    let receipt = fixture.revoke().await;
    assert!(receipt.handoff_eligible());
    assert!(TcpStream::connect(address).await.is_err());
    assert!(fixture.backend_commands.lock().unwrap().is_empty());
    fixture.cleanup().await;
}

#[tokio::test(flavor = "current_thread")]
async fn drop_fallback_seals_listener_and_cancels_owned_session_work() {
    let mut fixture = Fixture::new(
        "proxy-drop",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: true,
        },
    )
    .await;
    let address = fixture.publish();
    let mut client = authenticate(address, "application_local", CLIENT_CAPABILITIES).await;
    write_packet(&mut client, 0, b"\x03block").await;
    wait_active_command(&fixture.proxy).await;
    let Fixture { proxy, backend, .. } = fixture;
    drop(proxy);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(TcpStream::connect(address).await.is_err());
    assert_eq!(client.read(&mut [0_u8; 1]).await.unwrap(), 0);
    backend.abort();
    let _ = backend.await;
}

#[tokio::test(flavor = "current_thread")]
async fn stale_generation_cannot_revoke_or_replace_the_published_listener() {
    let mut fixture = Fixture::new(
        "proxy-generation",
        BackendPlan {
            auth: AuthMode::Fast,
            block_query: false,
        },
    )
    .await;
    fixture.publish();
    assert!(matches!(
        fixture
            .proxy
            .publish(nonzero(2), true, true, &fixture.attempt),
        Err(MysqlProxyError::InvalidState)
    ));
    assert!(matches!(
        fixture
            .proxy
            .revoke(
                &mut fixture.control,
                &fixture.authority,
                nonzero(2),
                &fixture.attempt,
            )
            .await,
        Err(MysqlProxyError::GenerationMismatch)
    ));
    fixture.proxy.seal_now();
    assert!(matches!(
        fixture
            .proxy
            .revoke(
                &mut fixture.control,
                &fixture.authority,
                nonzero(2),
                &fixture.attempt,
            )
            .await,
        Err(MysqlProxyError::GenerationMismatch)
    ));
    assert!(fixture.revoke().await.handoff_eligible());
    fixture.cleanup().await;
}

#[test]
fn diagnostics_redact_application_secret() {
    let credential = ApplicationCredential::new(
        AttemptId::new("redaction-attempt").unwrap(),
        CredentialGeneration::new("application-generation").unwrap(),
        "application_local",
        "never-print-this",
    )
    .unwrap();
    let rendered = format!("{credential:?} {}", MysqlProxyError::InvalidConfiguration);
    assert!(!rendered.contains("never-print-this"));
    assert!(rendered.contains("<redacted>"));
}

async fn run_fake_backend(
    listener: UnixListener,
    plan: BackendPlan,
    commands: Arc<Mutex<Vec<u8>>>,
    settled: Arc<AtomicU32>,
    attempt: AttemptContext,
) {
    let next_connection = Arc::new(AtomicU32::new(CONNECTION_ID));
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        if matches!(plan.auth, AuthMode::CancelDuringConnect) {
            attempt.cancel();
        }
        let plan = plan.clone();
        let commands = Arc::clone(&commands);
        let settled = Arc::clone(&settled);
        let connection_id = next_connection.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            struct Settlement(Arc<AtomicU32>);
            impl Drop for Settlement {
                fn drop(&mut self) {
                    self.0.fetch_add(1, Ordering::Release);
                }
            }
            let _settlement = Settlement(settled);
            match plan.auth {
                AuthMode::WithheldGreeting => {
                    let mut byte = [0_u8; 1];
                    let _ = stream.read(&mut byte).await;
                    return;
                }
                AuthMode::DelayedGreeting => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                AuthMode::MalformedGreeting => {
                    write_packet(&mut stream, 0, b"malformed").await;
                    let mut byte = [0_u8; 1];
                    let _ = stream.read(&mut byte).await;
                    return;
                }
                AuthMode::CancelDuringConnect => {
                    let mut byte = [0_u8; 1];
                    let _ = stream.read(&mut byte).await;
                    return;
                }
                AuthMode::Fast | AuthMode::Full | AuthMode::Switch | AuthMode::Rsa => {}
            }
            write_fragmented(&mut stream, &packet(0, &greeting(connection_id))).await;
            let _ = read_packet(&mut stream).await;
            match plan.auth {
                AuthMode::Fast => {
                    let mut authentication = packet(2, &[0x01, 0x03]);
                    authentication.extend(packet(3, &ok_packet()));
                    stream.write_all(&authentication).await.unwrap();
                }
                AuthMode::Full => {
                    write_packet(&mut stream, 2, &[0x01, 0x04]).await;
                    return;
                }
                AuthMode::Switch => {
                    write_packet(&mut stream, 2, b"\xfecaching_sha2_password\0challenge").await;
                    return;
                }
                AuthMode::Rsa => {
                    write_packet(&mut stream, 2, b"\x01-----BEGIN PUBLIC KEY-----").await;
                    return;
                }
                AuthMode::WithheldGreeting
                | AuthMode::DelayedGreeting
                | AuthMode::MalformedGreeting
                | AuthMode::CancelDuringConnect => {
                    unreachable!("greeting-only modes returned or continued as fast auth")
                }
            }
            loop {
                let Ok(command) = read_packet_result(&mut stream).await else {
                    return;
                };
                let byte = command.1[0];
                commands.lock().unwrap().push(byte);
                match byte {
                    0x03 => {
                        let sql = &command.1[1..];
                        if plan.block_query && sql == b"block" {
                            let mut byte = [0_u8; 1];
                            let _ = stream.read(&mut byte).await;
                            return;
                        }
                        if sql == b"local" {
                            write_packet(&mut stream, 1, &[0xfb]).await;
                        } else if sql == b"more" {
                            let mut more = ok_packet();
                            more[3] = 0x0a;
                            write_packet(&mut stream, 1, &more).await;
                        } else if sql == b"sequence" {
                            write_packet(&mut stream, 2, &ok_packet()).await;
                        } else if sql == b"ok-short" {
                            write_packet(&mut stream, 1, &[0x00, 0x00, 0x00, 0x02, 0x00]).await;
                        } else if sql == b"ok-lenenc" {
                            write_packet(&mut stream, 1, &[0x00, 0xfc, 0x01]).await;
                        } else if sql == b"err" {
                            write_packet(&mut stream, 1, b"\xff\x28\x04#HY000error").await;
                        } else if sql == b"err-short" {
                            write_packet(&mut stream, 1, &[0xff, 0x28, 0x04]).await;
                        } else if sql == b"err-marker" {
                            write_packet(&mut stream, 1, b"\xff\x28\x04!HY000").await;
                        } else if sql == b"column-count" {
                            write_packet(&mut stream, 1, &[0x01, 0x00]).await;
                        } else if sql == b"column-short" {
                            write_packet(&mut stream, 1, &[0x01]).await;
                            write_packet(&mut stream, 2, b"\x03def").await;
                        } else if matches!(
                            sql,
                            b"metadata-eof"
                                | b"metadata-more"
                                | b"row-short"
                                | b"row-extra"
                                | b"row-more"
                        ) {
                            write_packet(&mut stream, 1, &[0x01]).await;
                            write_packet(&mut stream, 2, &column_definition()).await;
                            let metadata = if sql == b"metadata-eof" {
                                vec![0xfe, 0x00, 0x00, 0x02]
                            } else if sql == b"metadata-more" {
                                vec![0xfe, 0x00, 0x00, 0x0a, 0x00]
                            } else {
                                eof_packet()
                            };
                            write_packet(&mut stream, 3, &metadata).await;
                            if sql == b"row-short" {
                                write_packet(&mut stream, 4, &[0x02, b'x']).await;
                            } else if sql == b"row-extra" {
                                write_packet(&mut stream, 4, &[0x01, b'x', b'y']).await;
                            } else if sql == b"row-more" {
                                write_packet(&mut stream, 4, &[0xfe, 0x00, 0x00, 0x0a, 0x00]).await;
                            }
                        } else if sql == b"rows" {
                            for (sequence, payload) in [
                                (1, vec![1]),
                                (2, column_definition()),
                                (3, eof_packet()),
                                (4, vec![1, b'x']),
                                (5, eof_packet()),
                            ] {
                                write_packet(&mut stream, sequence, &payload).await;
                            }
                        } else if matches!(sql, b"null-first" | b"null-later" | b"null-all") {
                            write_packet(&mut stream, 1, &[0x02]).await;
                            write_packet(&mut stream, 2, &column_definition()).await;
                            write_packet(&mut stream, 3, &column_definition()).await;
                            write_packet(&mut stream, 4, &eof_packet()).await;
                            let row = if sql == b"null-first" {
                                vec![0xfb, 0x01, b'x']
                            } else if sql == b"null-later" {
                                vec![0x01, b'x', 0xfb]
                            } else {
                                vec![0xfb, 0xfb]
                            };
                            write_packet(&mut stream, 5, &row).await;
                            write_packet(&mut stream, 6, &eof_packet()).await;
                        } else {
                            write_packet(&mut stream, 1, &ok_packet()).await;
                        }
                    }
                    0x0e => write_packet(&mut stream, 1, &ok_packet()).await,
                    0x01 => return,
                    _ => return,
                }
            }
        });
    }
}

fn greeting(connection_id: u32) -> Vec<u8> {
    let mut payload = vec![10];
    payload.extend(b"8.4.11\0");
    payload.extend(connection_id.to_le_bytes());
    payload.extend(b"12345678");
    payload.push(0);
    payload.extend((CAPABILITIES as u16).to_le_bytes());
    payload.push(45);
    payload.extend(2_u16.to_le_bytes());
    payload.extend(((CAPABILITIES >> 16) as u16).to_le_bytes());
    payload.push(21);
    payload.extend([0_u8; 10]);
    payload.extend(b"abcdefghijkl\0");
    payload.extend(b"caching_sha2_password\0");
    payload
}

fn ok_packet() -> Vec<u8> {
    vec![0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]
}

fn eof_packet() -> Vec<u8> {
    vec![0xfe, 0x00, 0x00, 0x02, 0x00]
}

fn column_definition() -> Vec<u8> {
    let mut payload = Vec::new();
    for value in [
        b"def".as_slice(),
        b"db",
        b"table",
        b"table",
        b"name",
        b"name",
    ] {
        payload.push(value.len() as u8);
        payload.extend(value);
    }
    payload.push(0x0c);
    payload.extend(45_u16.to_le_bytes());
    payload.extend(1024_u32.to_le_bytes());
    payload.push(0xfd);
    payload.extend(0_u16.to_le_bytes());
    payload.push(0);
    payload.extend([0_u8; 2]);
    payload
}

async fn authenticate(address: SocketAddr, username: &str, capabilities: u32) -> TcpStream {
    try_authenticate(address, username, capabilities)
        .await
        .unwrap()
}

async fn try_authenticate(
    address: SocketAddr,
    username: &str,
    capabilities: u32,
) -> Result<TcpStream, String> {
    try_authenticate_with_plugin(address, username, capabilities, "caching_sha2_password").await
}

async fn try_authenticate_with_plugin(
    address: SocketAddr,
    username: &str,
    capabilities: u32,
    plugin: &str,
) -> Result<TcpStream, String> {
    let mut stream = TcpStream::connect(address)
        .await
        .map_err(|error| error.to_string())?;
    let greeting = read_packet_result(&mut stream)
        .await
        .map_err(|error| error.to_string())?;
    if greeting.0 != 0 {
        return Err("invalid greeting sequence".to_owned());
    }
    let mut response = Vec::new();
    response.extend(capabilities.to_le_bytes());
    response.extend((1024_u32 * 1024).to_le_bytes());
    response.push(45);
    response.extend([0_u8; 23]);
    response.extend(username.as_bytes());
    response.push(0);
    response.push(1);
    response.push(7);
    response.extend(plugin.as_bytes());
    response.push(0);
    response.push(0);
    write_packet(&mut stream, 1, &response).await;
    let fast = read_packet_result(&mut stream)
        .await
        .map_err(|error| error.to_string())?;
    if fast.1 != [0x01, 0x03] {
        return Err("fast authentication was not accepted".to_owned());
    }
    let ok = read_packet_result(&mut stream)
        .await
        .map_err(|error| error.to_string())?;
    if ok.1.first() != Some(&0x00) {
        return Err("final authentication OK was not accepted".to_owned());
    }
    Ok(stream)
}

async fn write_fragmented(stream: &mut UnixStream, bytes: &[u8]) {
    for byte in bytes {
        stream.write_all(&[*byte]).await.unwrap();
    }
}

fn packet(sequence: u8, payload: &[u8]) -> Vec<u8> {
    let length = payload.len() as u32;
    let mut packet = vec![
        length as u8,
        (length >> 8) as u8,
        (length >> 16) as u8,
        sequence,
    ];
    packet.extend(payload);
    packet
}

async fn write_packet<W: AsyncWrite + Unpin>(stream: &mut W, sequence: u8, payload: &[u8]) {
    stream.write_all(&packet(sequence, payload)).await.unwrap();
}

async fn write_packet_result<W: AsyncWrite + Unpin>(
    stream: &mut W,
    sequence: u8,
    payload: &[u8],
) -> std::io::Result<()> {
    stream.write_all(&packet(sequence, payload)).await
}

async fn read_packet<R: AsyncRead + Unpin>(stream: &mut R) -> (u8, Vec<u8>) {
    read_packet_result(stream).await.unwrap()
}

async fn read_packet_result<R: AsyncRead + Unpin>(
    stream: &mut R,
) -> std::io::Result<(u8, Vec<u8>)> {
    let mut header = [0_u8; 4];
    stream.read_exact(&mut header).await?;
    let length =
        usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload).await?;
    Ok((header[3], payload))
}

async fn wait_sessions_zero(proxy: &MysqlClientProxy) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if proxy.snapshot().sessions() == 0 {
            return;
        }
        assert!(Instant::now() < deadline, "proxy session did not retire");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_active_command(proxy: &MysqlClientProxy) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if proxy.snapshot().active_commands() == 1 {
            return;
        }
        assert!(Instant::now() < deadline, "proxy command did not dispatch");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn await_session_count(proxy: &MysqlClientProxy, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if proxy.snapshot().sessions() == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "proxy session count did not settle"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_backend_settled(settled: &AtomicU32, expected: u32) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if settled.load(Ordering::Acquire) >= expected {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "fake backend connection did not settle"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_atomic_count(value: &AtomicU32, expected: u32) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while value.load(Ordering::Acquire) < expected {
        assert!(Instant::now() < deadline, "scripted cleanup did not start");
        tokio::task::yield_now().await;
    }
}

fn authority(attempt: &AttemptContext) -> MemberMutationAuthority {
    let binding = MemberControlBinding::new(
        attempt.attempt().clone(),
        MysqlMemberIndex::First,
        ProcessSessionId::new("process-session").unwrap(),
        EndpointBinding::new("private-uds").unwrap(),
        StorageBinding::new("storage").unwrap(),
        MemberAddress::new("127.0.0.1:33061").unwrap(),
        CredentialGeneration::new("observer-generation").unwrap(),
        CredentialGeneration::new("recovery-generation").unwrap(),
    );
    let observer = ControlCredential::new(
        attempt.attempt().clone(),
        CredentialGeneration::new("observer-generation").unwrap(),
        ControlCredentialRole::Observer,
        "observer_local",
        "observer-secret",
    )
    .unwrap();
    let recovery = ControlCredential::new(
        attempt.attempt().clone(),
        CredentialGeneration::new("recovery-generation").unwrap(),
        ControlCredentialRole::Recovery,
        "recovery_local",
        "recovery-secret",
    )
    .unwrap();
    let accounts = AccountProvisioningEvidence::new(
        binding.clone(),
        &observer,
        &recovery,
        AccountProvisioningEvidence::required_steps().to_vec(),
    )
    .unwrap();
    let enrollment = NativeIdentityEnrollment::new(
        binding,
        ServerUuid::new("11111111-1111-1111-1111-111111111111").unwrap(),
        &accounts,
        true,
        GtidSet::empty(),
        NativeIdentityEnrollment::required_steps().to_vec(),
    )
    .unwrap();
    MemberMutationAuthority::new(
        attempt.attempt().clone(),
        GroupName::new(GROUP_UUID).unwrap(),
        enrollment,
    )
    .unwrap()
}

fn nonzero(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}
