#![allow(dead_code)]

#[path = "../src/decode.rs"]
mod decode;
#[path = "../src/diagnostic.rs"]
mod diagnostic;
#[path = "../src/error.rs"]
mod error;
#[path = "../src/observer.rs"]
mod observer;
#[path = "../src/query.rs"]
mod query;
#[path = "../src/report.rs"]
mod report;
#[path = "../src/request.rs"]
mod request;
#[path = "../src/session.rs"]
mod session;
#[path = "../src/time.rs"]
mod time;

mod common;
mod phase4_common;

pub use diagnostic::*;
pub use report::ObservationReport;
pub use request::*;
pub use time::*;

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use kuberic_mysql_core::{
    AuthoritySession, CompletionRejection, ObservationInstant, ObservationOutcome,
    ObservationProvenance, StaleReason,
};
use mysql_async::consts::ColumnType;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Notify, oneshot};

use query::QueryId;
use session::{MysqlNativeSession, NativeSession};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct RealClock {
    origin: Instant,
}

impl ObservationClock for RealClock {
    fn now(&self) -> ObservationInstant {
        ObservationInstant::new(self.origin.elapsed().as_millis() as u64)
    }

    fn to_std_instant(&self, instant: ObservationInstant) -> Result<Instant, ClockError> {
        self.origin
            .checked_add(Duration::from_millis(instant.tick()))
            .ok_or(ClockError::UnrepresentableInstant)
    }
}

struct ProtocolSocket {
    path: PathBuf,
    directory: PathBuf,
}

impl ProtocolSocket {
    fn new(label: &str) -> (Self, UnixListener) {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "km-nr-{}-{}-{sequence}",
            &label[..label.len().min(8)],
            std::process::id()
        ));
        fs::create_dir_all(&directory).expect("create protocol test directory");
        let path = directory.join("mysql.sock");
        let listener = UnixListener::bind(&path).expect("bind protocol test socket");
        (Self { path, directory }, listener)
    }

    fn request(&self, deadline_ms: u64) -> ObservationRequest<RealClock> {
        request(&self.path, deadline_ms)
    }
}

impl Drop for ProtocolSocket {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        fs::remove_dir(&self.directory).expect("remove protocol test directory");
    }
}

#[derive(Clone, Copy)]
enum StallPoint {
    Connected,
    ProductQuery,
    ProductRows,
}

#[tokio::test(flavor = "current_thread")]
async fn result_consumption_timeout_returns_stale_before_late_rows_arrive() {
    let (socket, listener) = ProtocolSocket::new("result-consumption");
    let ready = Arc::new(Notify::new());
    let (release, release_rx) = oneshot::channel();
    let server = tokio::spawn(protocol_server(
        listener,
        StallPoint::ProductRows,
        Arc::clone(&ready),
        release_rx,
        true,
    ));
    let current = phase4_common::binding();
    let mut authority = AuthoritySession::new(current.clone());
    let capability = authority.begin_attempt(&current).expect("pending attempt");

    let started = Instant::now();
    let report = tokio::time::timeout(
        Duration::from_millis(500),
        observer::MysqlObserver::observe(socket.request(120)),
    )
    .await
    .expect("observer must return after its own absolute deadline");
    let elapsed = started.elapsed();

    assert!(matches!(
        report.outcome(),
        ObservationOutcome::Stale {
            reason: StaleReason::Expired,
            ..
        }
    ));
    assert_eq!(
        report.diagnostic(),
        &AdapterDiagnostic::Timeout {
            stage: DeadlineStage::ProductIdentity
        }
    );
    assert_eq!(report.outcome().metadata().binding(), &current);
    assert!(report.outcome().metadata().end().tick() >= 120);
    assert!(report.outcome().metadata().decision().tick() >= 120);
    assert!(elapsed < Duration::from_millis(400));
    assert_eq!(
        authority.complete(
            capability,
            report.outcome(),
            report.outcome().metadata().decision()
        ),
        Err(CompletionRejection::NonValidObservation)
    );
    assert!(!authority.has_observation_credit());
    assert!(!authority.access().read_open());
    assert!(!authority.access().write_open());

    release.send(()).expect("release late product rows");
    join_server(server).await;
    assert!(report.outcome().valid().is_none());
    assert!(!authority.has_observation_credit());
}

#[tokio::test(flavor = "current_thread")]
async fn dirty_native_disconnect_is_dropped_when_the_same_deadline_is_exhausted() {
    let (socket, listener) = ProtocolSocket::new("blocked-disconnect");
    let ready = Arc::new(Notify::new());
    let (release, release_rx) = oneshot::channel();
    let server = tokio::spawn(protocol_server(
        listener,
        StallPoint::ProductRows,
        Arc::clone(&ready),
        release_rx,
        true,
    ));
    let request = socket.request(1000);
    let mut session = tokio::time::timeout(
        Duration::from_millis(500),
        MysqlNativeSession::connect(&request),
    )
    .await
    .expect("connection timeout")
    .expect("native connection");
    let started = Instant::now();
    let query_deadline = tokio::time::Instant::now() + Duration::from_millis(80);
    let absolute = tokio::time::Instant::now() + Duration::from_millis(120);

    assert!(
        tokio::time::timeout_at(
            query_deadline,
            session.query(QueryId::Mysql8411ProductIdentityV1)
        )
        .await
        .is_err(),
        "row consumption must remain pending"
    );
    ready.notified().await;
    let teardown_started = Instant::now();
    assert!(
        tokio::time::timeout_at(absolute, Box::new(session).disconnect())
            .await
            .is_err(),
        "dirty disconnect must not receive a new timeout budget"
    );
    assert!(teardown_started.elapsed() >= Duration::from_millis(20));
    assert!(teardown_started.elapsed() < Duration::from_millis(80));
    assert!(started.elapsed() < Duration::from_millis(180));

    release.send(()).expect("release blocked native cleanup");
    join_server(server).await;
}

#[tokio::test(flavor = "current_thread")]
async fn caller_cancellation_after_connection_and_during_query_never_panics() {
    for (label, stall) in [
        ("after-connect", StallPoint::Connected),
        ("during-query", StallPoint::ProductQuery),
    ] {
        let (socket, listener) = ProtocolSocket::new(label);
        let ready = Arc::new(Notify::new());
        let (stop, stop_rx) = oneshot::channel();
        let server = tokio::spawn(protocol_server(
            listener,
            stall,
            Arc::clone(&ready),
            stop_rx,
            false,
        ));
        let current = phase4_common::binding();
        let mut authority = AuthoritySession::new(current.clone());
        let _capability = authority.begin_attempt(&current).expect("pending attempt");
        let observation = tokio::spawn(observer::MysqlObserver::observe(socket.request(1000)));
        tokio::time::timeout(Duration::from_millis(500), ready.notified())
            .await
            .expect("server reached cancellation point");

        observation.abort();
        let join_error = observation
            .await
            .expect_err("cancelled observation has no report");
        assert!(
            join_error.is_cancelled(),
            "{label}: caller cancellation panicked instead: {join_error}"
        );

        stop.send(()).expect("stop protocol server");
        join_server(server).await;
        assert!(!authority.has_observation_credit());
        assert!(!authority.access().read_open());
        assert!(!authority.access().write_open());
    }
}

async fn join_server(server: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("protocol server join deadline")
        .expect("protocol server task");
}

async fn protocol_server(
    listener: UnixListener,
    stall: StallPoint,
    ready: Arc<Notify>,
    release: oneshot::Receiver<()>,
    send_late_product: bool,
) {
    let (mut stream, _) = listener.accept().await.expect("accept native client");
    write_packet(&mut stream, 0, &handshake()).await;
    let authentication = read_packet(&mut stream)
        .await
        .expect("authentication packet");
    assert!(!authentication.is_empty());
    write_packet(&mut stream, 2, &[0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]).await;

    let settings = read_packet(&mut stream).await.expect("settings query");
    assert_eq!(settings.first(), Some(&0x03));
    assert_eq!(
        std::str::from_utf8(&settings[1..]).expect("settings SQL"),
        "SELECT @@max_allowed_packet,@@wait_timeout"
    );
    write_packet(&mut stream, 1, &[0x02]).await;
    write_packet(&mut stream, 2, &column("@@max_allowed_packet")).await;
    write_packet(&mut stream, 3, &column("@@wait_timeout")).await;
    write_packet(&mut stream, 4, &[0xfe, 0x00, 0x00, 0x02, 0x00]).await;
    write_packet(
        &mut stream,
        5,
        &row(&[b"67108864".as_slice(), b"28800".as_slice()]),
    )
    .await;
    write_packet(&mut stream, 6, &[0xfe, 0x00, 0x00, 0x02, 0x00]).await;

    if matches!(stall, StallPoint::Connected) {
        ready.notify_one();
        let _ = release.await;
        return;
    }

    let product = read_packet(&mut stream).await.expect("product query");
    assert_eq!(product.first(), Some(&0x03));
    assert_eq!(
        std::str::from_utf8(&product[1..]).expect("product SQL"),
        QueryId::Mysql8411ProductIdentityV1.sql()
    );
    if matches!(stall, StallPoint::ProductQuery) {
        ready.notify_one();
        let _ = release.await;
        return;
    }

    write_packet(&mut stream, 1, &[0x05]).await;
    for (index, contract) in QueryId::Mysql8411ProductIdentityV1
        .columns()
        .iter()
        .enumerate()
    {
        write_packet(&mut stream, (index + 2) as u8, &column(contract.name())).await;
    }
    write_packet(&mut stream, 7, &[0xfe, 0x00, 0x00, 0x02, 0x00]).await;
    ready.notify_one();
    let _ = release.await;
    if send_late_product
        && try_write_packet(
            &mut stream,
            8,
            &row(&[
                b"8.4.11".as_slice(),
                b"MySQL Community Server - GPL".as_slice(),
                b"x86_64".as_slice(),
                b"Linux".as_slice(),
                b"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".as_slice(),
            ]),
        )
        .await
        .is_ok()
    {
        let _ = try_write_packet(&mut stream, 9, &[0xfe, 0x00, 0x00, 0x02, 0x00]).await;
    }
}

async fn write_packet(stream: &mut UnixStream, sequence: u8, payload: &[u8]) {
    try_write_packet(stream, sequence, payload)
        .await
        .expect("write protocol packet");
}

async fn try_write_packet(stream: &mut UnixStream, sequence: u8, payload: &[u8]) -> io::Result<()> {
    let length = payload.len();
    stream
        .write_all(&[
            length as u8,
            (length >> 8) as u8,
            (length >> 16) as u8,
            sequence,
        ])
        .await?;
    stream.write_all(payload).await
}

async fn read_packet(stream: &mut UnixStream) -> io::Result<Vec<u8>> {
    let mut header = [0_u8; 4];
    stream.read_exact(&mut header).await?;
    let length =
        usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload).await?;
    Ok(payload)
}

fn handshake() -> Vec<u8> {
    let capabilities = 0x0008_8200_u32;
    let mut payload = Vec::new();
    payload.push(10);
    payload.extend_from_slice(b"8.4.11\0");
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.extend_from_slice(b"12345678");
    payload.push(0);
    payload.extend_from_slice(&(capabilities as u16).to_le_bytes());
    payload.push(45);
    payload.extend_from_slice(&2_u16.to_le_bytes());
    payload.extend_from_slice(&((capabilities >> 16) as u16).to_le_bytes());
    payload.push(21);
    payload.extend_from_slice(&[0_u8; 10]);
    payload.extend_from_slice(b"abcdefghijkl\0");
    payload.extend_from_slice(b"mysql_native_password\0");
    payload
}

fn column(name: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    for value in ["def", "", "", "", name, ""] {
        payload.push(value.len() as u8);
        payload.extend_from_slice(value.as_bytes());
    }
    payload.push(0x0c);
    payload.extend_from_slice(&45_u16.to_le_bytes());
    payload.extend_from_slice(&255_u32.to_le_bytes());
    payload.push(ColumnType::MYSQL_TYPE_VAR_STRING as u8);
    payload.extend_from_slice(&0_u16.to_le_bytes());
    payload.push(0);
    payload.extend_from_slice(&[0_u8; 2]);
    payload
}

fn row(values: &[&[u8]]) -> Vec<u8> {
    let mut payload = Vec::new();
    for value in values {
        payload.push(value.len() as u8);
        payload.extend_from_slice(value);
    }
    payload
}

fn request(path: &Path, deadline_ms: u64) -> ObservationRequest<RealClock> {
    let binding = phase4_common::binding();
    ObservationRequest::new(
        binding.clone(),
        UnixSocketPath::new(path).expect("validated protocol socket"),
        ObserverCredentials::new("observer", "protocol-secret").expect("credentials"),
        ObservationProvenance::new(
            "native-protocol-regression",
            binding.parts().attempt.clone(),
        )
        .expect("provenance"),
        ClockContext::new(
            RealClock {
                origin: Instant::now(),
            },
            ObservationInstant::new(deadline_ms),
        )
        .expect("clock context"),
    )
    .expect("observation request")
}
