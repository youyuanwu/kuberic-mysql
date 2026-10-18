//! Private settlement tests never manufacture a production reopen success.
use super::*;
use crate::core::{
    EndpointBinding, GroupName, GtidSet, MemberAddress, ProcessSessionId, ServerUuid,
    StorageBinding,
};
use crate::service::{
    AccountProvisioningEvidence, ControlCredential, ControlCredentialRole, MemberControlBinding,
    MysqlMemberIndex, NativeIdentityEnrollment,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

static SERIAL: AtomicUsize = AtomicUsize::new(0);

struct Control {
    socket: UnixSocketPath,
    terminated: Vec<u64>,
    removed: usize,
    fail: bool,
    gate: Option<Arc<Notify>>,
    remove_gate: Option<Arc<Notify>>,
    started: Arc<Notify>,
}

impl MysqlProxyControlBoundary for Control {
    fn contain_proxy_preparation(
        &mut self,
        _: &MemberMutationAuthority,
        _: &AttemptContext,
    ) -> Result<(), MysqlProxyError> {
        Ok(())
    }
    fn proxy_backend_socket(
        &mut self,
        _: &MemberMutationAuthority,
    ) -> Result<UnixSocketPath, MysqlProxyError> {
        Ok(self.socket.clone())
    }
    async fn prepare_application_account(
        &mut self,
        _: &MemberMutationAuthority,
        credential: &ApplicationCredential,
        _: &AttemptContext,
    ) -> Result<ApplicationAccountReceipt, MysqlProxyError> {
        Ok(ApplicationAccountReceipt::new(credential))
    }
    async fn remove_application_account(
        &mut self,
        _: &MemberMutationAuthority,
        _: &ApplicationAccountReceipt,
        _: &AttemptContext,
    ) -> Result<(), MysqlProxyError> {
        self.removed += 1;
        if let Some(gate) = &self.remove_gate {
            self.started.notify_one();
            gate.notified().await;
        }
        if self.fail {
            Err(MysqlProxyError::InvalidState)
        } else {
            Ok(())
        }
    }
    async fn terminate_proxy_backend(
        &mut self,
        _: &MemberMutationAuthority,
        session: &BackendSessionIdentity,
        _: &AttemptContext,
        _: Instant,
    ) -> Result<(), MysqlProxyError> {
        self.started.notify_one();
        if let Some(gate) = &self.gate {
            gate.notified().await;
        }
        if self.fail {
            return Err(MysqlProxyError::InvalidState);
        }
        self.terminated.push(session.connection().get());
        Ok(())
    }
}

struct Fixture {
    proxy: MysqlClientProxy,
    control: Control,
    attempt: AttemptContext,
    _socket: std::os::unix::net::UnixListener,
    path: std::path::PathBuf,
}

impl Fixture {
    async fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "pt-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&path).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket = std::os::unix::net::UnixListener::bind(path.join("b")).unwrap();
        let attempt = AttemptContext::new(
            AttemptId::new("terminal").unwrap(),
            Instant::now() + Duration::from_secs(30),
            Duration::from_secs(5),
        )
        .unwrap();
        let authority = authority(&attempt);
        let mut control = Control {
            socket: UnixSocketPath::new(path.join("b")).unwrap(),
            terminated: vec![],
            removed: 0,
            fail: false,
            gate: None,
            remove_gate: None,
            started: Arc::new(Notify::new()),
        };
        let credential = ApplicationCredential::new(
            attempt.attempt().clone(),
            CredentialGeneration::new("app").unwrap(),
            "application_local",
            "secret",
        )
        .unwrap();
        let proxy = MysqlClientProxy::prepare(
            &mut control,
            &authority,
            "127.0.0.1:0".parse().unwrap(),
            credential,
            &attempt,
        )
        .await
        .unwrap();
        Self {
            proxy,
            control,
            attempt,
            _socket: socket,
            path,
        }
    }

    fn witness(&mut self) {
        self.proxy
            .attach_reopen_issuance(Arc::new(crate::service::instance::MysqlProxyIssuance {
                authority: self.proxy.authority.clone(),
                owner_generation: 1,
                process_generation: 1,
            }));
    }

    fn backend_obligation(&mut self, interrupted: bool) {
        let mut registry = self.proxy.shared.lock();
        registry.retired_backends.insert(
            91,
            BackendSessionIdentity::new(
                BackendConnectionId::new(NonZeroU64::new(91).unwrap()),
                self.proxy.account.clone(),
                self.proxy.authority.clone(),
                self.proxy.backend.clone(),
            ),
        );
        registry.cancelled_commands = interrupted;
        registry.ambiguous_effect = interrupted;
        self.proxy.native_absence_proved = false;
    }

    async fn publish(&mut self) {
        self.proxy
            .publish(NonZeroU64::MIN, true, true, &self.attempt)
            .unwrap();
        tokio::task::yield_now().await;
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn authority(attempt: &AttemptContext) -> MemberMutationAuthority {
    let binding = MemberControlBinding::new(
        attempt.attempt().clone(),
        MysqlMemberIndex::First,
        ProcessSessionId::new("process").unwrap(),
        EndpointBinding::new("uds").unwrap(),
        StorageBinding::new("storage").unwrap(),
        MemberAddress::new("127.0.0.1:33061").unwrap(),
        CredentialGeneration::new("observer").unwrap(),
        CredentialGeneration::new("recovery").unwrap(),
    );
    let observer = ControlCredential::new(
        attempt.attempt().clone(),
        CredentialGeneration::new("observer").unwrap(),
        ControlCredentialRole::Observer,
        "observer_local",
        "secret",
    )
    .unwrap();
    let recovery = ControlCredential::new(
        attempt.attempt().clone(),
        CredentialGeneration::new("recovery").unwrap(),
        ControlCredentialRole::Recovery,
        "recovery_local",
        "secret",
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
        ServerUuid::new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
        &accounts,
        true,
        GtidSet::empty(),
        NativeIdentityEnrollment::required_steps().to_vec(),
    )
    .unwrap();
    MemberMutationAuthority::new(
        attempt.attempt().clone(),
        GroupName::new("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
        enrollment,
    )
    .unwrap()
}

#[tokio::test]
async fn terminal_unpublished_and_published_shared_facades_never_rearm_or_publish() {
    for published in [false, true] {
        let mut fixture = Fixture::new().await;
        if published {
            fixture.publish().await;
        }
        let address = fixture.proxy.address();
        let shared = Arc::new(tokio::sync::Mutex::new(&mut fixture.proxy));
        let stale = Arc::clone(&shared);
        {
            let mut proxy = shared.lock().await;
            proxy.terminal_now();
            proxy
                .settle_terminal(&mut fixture.control, &fixture.attempt)
                .await
                .unwrap();
        }
        let mut proxy = stale.lock().await;
        assert!(
            proxy
                .publish(NonZeroU64::MIN, true, true, &fixture.attempt)
                .is_err()
        );
        assert!(proxy.rearm(address, &fixture.attempt).await.is_err());
        assert!(proxy.bind_access_guard(granted_proxy_access()).is_err());
        assert!(TcpStream::connect(address).await.is_err());
        assert!(proxy.credential_cleared());
        assert!(!proxy.account_active());
        assert_eq!(fixture.control.removed, 1);
    }
}

#[tokio::test]
async fn terminal_genuine_revoked_rearmed_generations_keep_lifetime_fence() {
    let mut fixture = Fixture::new().await;
    fixture.witness();
    fixture.publish().await;
    fixture.backend_obligation(false);
    let authority = fixture.proxy.authority.clone();
    fixture
        .proxy
        .revoke_owned(
            &mut fixture.control,
            &authority,
            NonZeroU64::MIN,
            &fixture.attempt,
        )
        .await
        .unwrap();
    assert_eq!(fixture.control.terminated, [91]);
    fixture
        .proxy
        .rearm("127.0.0.1:0".parse().unwrap(), &fixture.attempt)
        .await
        .unwrap();
    fixture.publish().await;
    fixture.backend_obligation(false);
    assert!(
        fixture
            .proxy
            .revoke(
                &mut fixture.control,
                &authority,
                NonZeroU64::MIN,
                &fixture.attempt,
            )
            .await
            .is_err()
    );
    assert!(
        fixture
            .proxy
            .cleanup_account(&mut fixture.control, &authority, &fixture.attempt,)
            .await
            .is_err()
    );
    assert_eq!(fixture.proxy.shared.lock().retired_backends.len(), 1);
    fixture.proxy.terminal_now();
    fixture
        .proxy
        .settle_terminal(&mut fixture.control, &fixture.attempt)
        .await
        .unwrap();
    assert!(
        fixture
            .proxy
            .rearm("127.0.0.1:0".parse().unwrap(), &fixture.attempt)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn terminal_account_removal_failure_and_drop_never_undo_completed_fence() {
    let mut fixture = Fixture::new().await;
    fixture.proxy.terminal_now();
    fixture.control.fail = true;
    assert!(
        fixture
            .proxy
            .settle_terminal(&mut fixture.control, &fixture.attempt)
            .await
            .is_err()
    );
    assert!(fixture.proxy.account_active());
    assert!(fixture.proxy.tasks_joined && fixture.proxy.native_absence_proved);
    fixture.control.fail = false;
    fixture.control.remove_gate = Some(Arc::new(Notify::new()));
    let started = Arc::clone(&fixture.control.started);
    let mut future = Box::pin(
        fixture
            .proxy
            .settle_terminal(&mut fixture.control, &fixture.attempt),
    );
    tokio::select! { biased; result = &mut future => panic!("unexpected {result:?}"), () = started.notified() => {} }
    drop(future);
    assert!(fixture.proxy.account_active());
    assert!(!fixture.proxy.listener_is_bound());
    assert!(
        fixture
            .proxy
            .rearm("127.0.0.1:0".parse().unwrap(), &fixture.attempt)
            .await
            .is_err()
    );
    fixture.control.remove_gate = None;
    fixture
        .proxy
        .settle_terminal(&mut fixture.control, &fixture.attempt)
        .await
        .unwrap();
    assert!(!fixture.proxy.account_active());
}

#[tokio::test]
async fn terminal_owning_unpolled_future_runs_synchronous_fence_and_drops_owner() {
    let mut fixture = Fixture::new().await;
    fixture.witness();
    let config = crate::service::MysqlInstanceConfig::new(
        "/usr/bin/sleep",
        "/usr/bin/env",
        fixture.path.join("d"),
        fixture.path.join("s"),
        crate::service::MysqlOperationTimeouts::new(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap(),
    )
    .unwrap();
    let manager = crate::service::MysqlInstanceManager::new(config);
    let issuance = fixture.proxy.reopen_issuance.clone();
    let address = fixture.proxy.address();
    // Isolate the actual post-validation owning boundary. No native success or
    // production token is minted: this future is deliberately never polled.
    let future = fixture.proxy.close_validated(
        manager,
        issuance,
        crate::service::reopen::tests::authority(),
        &fixture.attempt,
        Ok(()),
    );
    drop(future);
    assert!(fixture.proxy.shared.lock().terminal);
    assert!(!fixture.proxy.listener_is_bound());
    assert!(!fixture.proxy.closure_minted);
    assert!(TcpStream::connect(address).await.is_err());
    assert!(
        fixture
            .proxy
            .publish(NonZeroU64::MIN, true, true, &fixture.attempt)
            .is_err()
    );
    assert!(
        fixture
            .proxy
            .rearm(address, &fixture.attempt)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn terminal_rejects_fake_lifecycle_evidence_before_callbacks_and_history_reset() {
    let mut fixture = Fixture::new().await;
    fixture.witness();
    fixture.publish().await;
    fixture.backend_obligation(false);
    let authority = fixture.proxy.authority.clone();
    assert!(
        fixture
            .proxy
            .revoke(
                &mut fixture.control,
                &authority,
                NonZeroU64::MIN,
                &fixture.attempt
            )
            .await
            .is_err()
    );
    assert!(
        fixture
            .proxy
            .cleanup_account(&mut fixture.control, &authority, &fixture.attempt)
            .await
            .is_err()
    );
    assert!(
        fixture
            .proxy
            .rearm("127.0.0.1:0".parse().unwrap(), &fixture.attempt)
            .await
            .is_err()
    );
    assert!(fixture.control.terminated.is_empty());
    assert_eq!(fixture.control.removed, 0);
    assert_eq!(fixture.proxy.shared.lock().retired_backends.len(), 1);
    fixture.proxy.terminal_now();
    fixture
        .proxy
        .settle_terminal(&mut fixture.control, &fixture.attempt)
        .await
        .unwrap();
    assert_eq!(fixture.control.terminated, [91]);
}

#[tokio::test]
async fn terminal_interrupted_work_is_contained_without_serving_handoff_credit() {
    let mut fixture = Fixture::new().await;
    fixture.publish().await;
    fixture.backend_obligation(true);
    fixture.proxy.terminal_now();
    let authority = fixture.proxy.authority.clone();
    let receipt = fixture
        .proxy
        .revoke_owned(
            &mut fixture.control,
            &authority,
            NonZeroU64::MIN,
            &fixture.attempt,
        )
        .await
        .unwrap();
    assert!(receipt.cleanup_complete());
    assert!(!receipt.handoff_eligible());
    fixture
        .proxy
        .settle_terminal(&mut fixture.control, &fixture.attempt)
        .await
        .unwrap();
    assert!(!fixture.proxy.account_active());
}

#[tokio::test]
async fn terminal_failed_and_dropped_native_settlement_retains_obligations_and_fence() {
    let mut fixture = Fixture::new().await;
    fixture.publish().await;
    fixture.backend_obligation(false);
    fixture.proxy.terminal_now();
    fixture.control.fail = true;
    assert!(
        fixture
            .proxy
            .settle_terminal(&mut fixture.control, &fixture.attempt)
            .await
            .is_err()
    );
    assert!(!fixture.proxy.native_absence_proved);
    fixture.control.fail = false;
    fixture.control.gate = Some(Arc::new(Notify::new()));
    let started = Arc::clone(&fixture.control.started);
    // Remove the permit from the preceding failed invocation.
    started.notified().await;
    let mut pending = Box::pin(
        fixture
            .proxy
            .settle_terminal(&mut fixture.control, &fixture.attempt),
    );
    tokio::select! { biased; result = &mut pending => panic!("unexpected {result:?}"), () = started.notified() => {} }
    drop(pending);
    assert_eq!(fixture.proxy.shared.lock().retired_backends.len(), 1);
    assert!(!fixture.proxy.native_absence_proved);
    assert!(
        fixture
            .proxy
            .publish(NonZeroU64::MIN, true, true, &fixture.attempt)
            .is_err()
    );
    fixture.control.gate = None;
    fixture
        .proxy
        .settle_terminal(&mut fixture.control, &fixture.attempt)
        .await
        .unwrap();
}

#[tokio::test]
async fn terminal_unidentified_backend_never_credits_later_idle_snapshot() {
    let mut fixture = Fixture::new().await;
    fixture.publish().await;
    fixture.proxy.shared.lock().unidentified_backends = 1;
    fixture.proxy.terminal_now();
    assert!(matches!(
        fixture
            .proxy
            .settle_terminal(&mut fixture.control, &fixture.attempt)
            .await,
        Err(MysqlProxyError::UnidentifiedBackend)
    ));
    assert!(fixture.proxy.account_active());
}

#[tokio::test]
async fn terminal_root_join_failure_cannot_be_laundered_by_retry_or_rearm() {
    let mut fixture = Fixture::new().await;
    fixture.publish().await;
    let task = fixture.proxy.accept_task.take().unwrap();
    task.abort();
    let _ = task.await;
    fixture.proxy.accept_task = Some(tokio::spawn(async {
        panic!("injected root task failure");
    }));
    fixture.proxy.terminal_now();
    assert!(
        fixture
            .proxy
            .settle_terminal(&mut fixture.control, &fixture.attempt)
            .await
            .is_err()
    );
    assert!(fixture.proxy.shared.lock().task_failure);
    assert!(
        fixture
            .proxy
            .settle_terminal(&mut fixture.control, &fixture.attempt)
            .await
            .is_err()
    );
    assert!(
        fixture
            .proxy
            .rearm("127.0.0.1:0".parse().unwrap(), &fixture.attempt)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn terminal_child_join_failure_poisons_normally_returning_root() {
    let mut fixture = Fixture::new().await;
    fixture.publish().await;
    let task = fixture.proxy.accept_task.take().unwrap();
    task.abort();
    let _ = task.await;
    let shared = Arc::clone(&fixture.proxy.shared);
    fixture.proxy.accept_task = Some(tokio::spawn(async move {
        let mut children = tokio::task::JoinSet::new();
        children.spawn(async {
            panic!("injected session task failure");
        });
        join_sessions(&mut children, &shared).await;
    }));
    fixture.proxy.terminal_now();
    assert!(
        fixture
            .proxy
            .settle_terminal(&mut fixture.control, &fixture.attempt)
            .await
            .is_err()
    );
    assert!(fixture.proxy.shared.lock().task_failure);
    assert!(!fixture.proxy.tasks_joined);
    assert!(
        fixture
            .proxy
            .settle_terminal(&mut fixture.control, &fixture.attempt)
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn terminal_root_timeout_retains_permanent_failure_after_abort_join() {
    let mut fixture = Fixture::new().await;
    fixture.publish().await;
    let task = fixture.proxy.accept_task.take().unwrap();
    task.abort();
    let _ = task.await;
    fixture.proxy.accept_task = Some(tokio::spawn(std::future::pending()));
    fixture.proxy.terminal_now();
    assert!(matches!(
        fixture
            .proxy
            .settle_terminal(&mut fixture.control, &fixture.attempt)
            .await,
        Err(MysqlProxyError::Deadline(MysqlProxyPhase::Revocation)),
    ));
    assert!(fixture.proxy.shared.lock().task_failure);
    assert!(
        fixture
            .proxy
            .settle_terminal(&mut fixture.control, &fixture.attempt)
            .await
            .is_err()
    );
    assert!(
        fixture
            .proxy
            .rearm("127.0.0.1:0".parse().unwrap(), &fixture.attempt)
            .await
            .is_err()
    );
}

struct GatedAccess {
    started: Arc<Notify>,
    release: Arc<Notify>,
}
impl MysqlProxyAccessGuard for GatedAccess {
    fn access_granted(&self) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
        Box::pin(async {
            self.started.notify_one();
            self.release.notified().await;
            true
        })
    }
}

#[tokio::test]
async fn terminal_admission_fence_joins_accepted_unregistered_guard_wait() {
    let mut fixture = Fixture::new().await;
    let started = Arc::new(Notify::new());
    fixture
        .proxy
        .bind_access_guard(Arc::new(GatedAccess {
            started: Arc::clone(&started),
            release: Arc::new(Notify::new()),
        }))
        .unwrap();
    fixture.publish().await;
    let client = TcpStream::connect(fixture.proxy.address()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    assert_eq!(fixture.proxy.snapshot().sessions(), 0);
    fixture.proxy.terminal_now();
    tokio::time::timeout(
        Duration::from_secs(2),
        fixture
            .proxy
            .settle_terminal(&mut fixture.control, &fixture.attempt),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(fixture.proxy.tasks_joined);
    assert!(!fixture.proxy.snapshot().accepting());
    drop(client);
}

#[tokio::test]
async fn terminal_drop_pending_root_join_retains_handle_and_cannot_republish() {
    let mut fixture = Fixture::new().await;
    fixture.publish().await;
    let old = fixture.proxy.accept_task.take().unwrap();
    old.abort();
    let _ = old.await;
    let release = Arc::new(Notify::new());
    let gate = Arc::clone(&release);
    fixture.proxy.accept_task = Some(tokio::spawn(async move {
        gate.notified().await;
    }));
    fixture.proxy.terminal_now();
    let mut pending = Box::pin(
        fixture
            .proxy
            .settle_terminal(&mut fixture.control, &fixture.attempt),
    );
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx).is_pending()))
            .await
    );
    drop(pending);
    assert!(fixture.proxy.accept_task.is_some());
    assert!(!fixture.proxy.tasks_joined);
    assert!(
        fixture
            .proxy
            .publish(NonZeroU64::MIN, true, true, &fixture.attempt)
            .is_err()
    );
    release.notify_one();
    fixture
        .proxy
        .settle_terminal(&mut fixture.control, &fixture.attempt)
        .await
        .unwrap();
}
