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
    BackendConnectionId, ControlCredential, ControlCredentialRole, MemberControlBinding,
    MemberMutationAuthority, MysqlClientProxy, MysqlMemberIndex, MysqlProxyControlBoundary,
    MysqlProxyError, NativeIdentityEnrollment,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpStream, UnixListener, UnixStream};

use service_common::{GROUP_UUID, TestRoot};

const CAPABILITIES: u32 = 0x0038_a20d;
const CLIENT_CAPABILITIES: u32 = CAPABILITIES & !0x0000_0008;
const CONNECTION_ID: u32 = 91;

#[derive(Clone, Copy)]
enum AuthMode {
    Fast,
    Cold,
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
}

impl MysqlProxyControlBoundary for FakeControl {
    fn proxy_backend_socket(
        &mut self,
        _authority: &MemberMutationAuthority,
    ) -> Result<UnixSocketPath, MysqlProxyError> {
        Ok(self.socket.clone())
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
        Ok(())
    }

    async fn terminate_proxy_backend(
        &mut self,
        _authority: &MemberMutationAuthority,
        connection: BackendConnectionId,
        _attempt: &AttemptContext,
        _deadline: Instant,
    ) -> Result<(), MysqlProxyError> {
        self.terminated.insert(connection.get());
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
        let commands = Arc::new(Mutex::new(Vec::new()));
        let backend_commands = Arc::clone(&commands);
        let backend = tokio::spawn(async move {
            run_fake_backend(listener, plan, backend_commands).await;
        });
        let attempt = AttemptContext::new(
            AttemptId::new(format!("{label}-attempt")).unwrap(),
            Instant::now() + total,
            cleanup,
        )
        .unwrap();
        let authority = authority(&attempt);
        let mut control = FakeControl {
            socket,
            prepared: 0,
            removed: 0,
            terminated: BTreeSet::new(),
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
    write_packet(&mut client, 0, b"\x0e").await;
    assert_eq!(read_packet(&mut client).await.1[0], 0x00);
    write_packet(&mut client, 0, b"\x01").await;
    assert_eq!(client.read(&mut [0_u8; 1]).await.unwrap(), 0);

    wait_sessions_zero(&fixture.proxy).await;
    let receipt = fixture.revoke().await;
    assert!(receipt.cleanup_complete());
    assert!(receipt.handoff_eligible());
    assert!(
        fixture
            .control
            .terminated
            .contains(&u64::from(CONNECTION_ID))
    );
    assert_eq!(
        *fixture.backend_commands.lock().unwrap(),
        vec![0x03, 0x03, 0x03, 0x0e, 0x01]
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
            AuthMode::Cold,
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
    let receipt = fixture.revoke().await;
    assert!(receipt.handoff_eligible());
    assert!(fixture.backend_commands.lock().unwrap().is_empty());
    fixture.cleanup().await;
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
        assert_eq!(client.read(&mut [0_u8; 1]).await.unwrap(), 0);
        wait_sessions_zero(&fixture.proxy).await;
        let receipt = fixture.revoke().await;
        assert!(receipt.ambiguous_effect());
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
) {
    let next_connection = Arc::new(AtomicU32::new(CONNECTION_ID));
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let plan = plan.clone();
        let commands = Arc::clone(&commands);
        let connection_id = next_connection.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            write_fragmented(&mut stream, &packet(0, &greeting(connection_id))).await;
            let _ = read_packet(&mut stream).await;
            match plan.auth {
                AuthMode::Fast => {
                    let mut authentication = packet(2, &[0x01, 0x03]);
                    authentication.extend(packet(3, &ok_packet()));
                    stream.write_all(&authentication).await.unwrap();
                }
                AuthMode::Cold => {
                    write_packet(&mut stream, 2, &[0x01, 0x04]).await;
                    return;
                }
            }
            loop {
                let command = read_packet(&mut stream).await;
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
                        } else if sql == b"err" {
                            write_packet(&mut stream, 1, &[0xff, 0x28, 0x04]).await;
                        } else if sql == b"rows" {
                            for (sequence, payload) in [
                                (1, vec![1]),
                                (2, b"column".to_vec()),
                                (3, eof_packet()),
                                (4, vec![1, b'x']),
                                (5, eof_packet()),
                            ] {
                                write_packet(&mut stream, sequence, &payload).await;
                            }
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
