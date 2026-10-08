#![cfg(unix)]

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use mysql_async::{Conn, Error, IoError, Opts, OptsBuilder, ServerError};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, UnixListener};
use tokio::time::timeout;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ErrorClass {
    Transport,
    Authentication,
    AuthenticatedServer,
    Other,
}

fn classify(error: &Error) -> ErrorClass {
    match error {
        Error::Io(_) => ErrorClass::Transport,
        Error::Server(server) if server.code == 1045 || server.state == "28000" => {
            ErrorClass::Authentication
        }
        Error::Server(_) => ErrorClass::AuthenticatedServer,
        Error::Driver(_) | Error::Other(_) | Error::Url(_) => ErrorClass::Other,
    }
}

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(label: &str) -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("km-cp-{label}-{}-{sequence}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create preflight directory");
        Self { path }
    }

    fn socket(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).expect("remove preflight directory");
    }
}

fn socket_opts(socket: &Path) -> Opts {
    OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(1)
        .user(Some("observer"))
        .pass(Some("not-a-real-secret"))
        .socket(Some(
            socket
                .to_str()
                .expect("test socket path is UTF-8")
                .to_owned(),
        ))
        .into()
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_socket_never_falls_back_to_tcp() {
    let directory = TestDirectory::new("no-fallback");
    let missing_socket = directory.socket("missing.sock");
    let tcp = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind fallback detector");
    let port = tcp.local_addr().expect("fallback detector address").port();
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .socket(Some(
            missing_socket
                .to_str()
                .expect("test socket path is UTF-8")
                .to_owned(),
        ))
        .into();

    assert_eq!(opts.socket(), missing_socket.to_str());
    assert!(matches!(
        timeout(Duration::from_secs(1), Conn::new(opts))
            .await
            .expect("missing socket must fail promptly"),
        Err(Error::Io(IoError::Io(_)))
    ));
    assert!(
        timeout(Duration::from_millis(50), tcp.accept())
            .await
            .is_err(),
        "an explicit socket must not attempt the configured TCP endpoint"
    );
}

#[test]
fn client_errors_preserve_transport_authentication_and_server_classes() {
    let transport = Error::Io(IoError::Io(io::Error::from(
        io::ErrorKind::ConnectionRefused,
    )));
    let authentication = Error::Server(ServerError {
        code: 1045,
        message: "access denied".to_owned(),
        state: "28000".to_owned(),
    });
    let authenticated_server = Error::Server(ServerError {
        code: 1142,
        message: "command denied".to_owned(),
        state: "42000".to_owned(),
    });

    assert_eq!(classify(&transport), ErrorClass::Transport);
    assert_eq!(classify(&authentication), ErrorClass::Authentication);
    assert_eq!(
        classify(&authenticated_server),
        ErrorClass::AuthenticatedServer
    );
    assert!(transport.is_fatal());
    assert!(!authentication.is_fatal());
    assert!(!authenticated_server.is_fatal());
}

#[tokio::test(flavor = "current_thread")]
async fn timed_out_connect_never_admits_late_completion() {
    let directory = TestDirectory::new("deadline");
    let socket = directory.socket("mysql.sock");
    let listener = UnixListener::bind(&socket).expect("bind stalled handshake socket");
    let peer_closed = Arc::new(AtomicBool::new(false));
    let server_peer_closed = Arc::clone(&peer_closed);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept UDS client");
        let mut byte = [0_u8; 1];
        let read = stream.read(&mut byte).await.expect("observe client close");
        assert_eq!(read, 0);
        server_peer_closed.store(true, Ordering::Release);
    });

    let admitted = Arc::new(AtomicBool::new(false));
    let completion_admitted = Arc::clone(&admitted);
    let connect = async move {
        let _connection = Conn::new(socket_opts(&socket)).await?;
        completion_admitted.store(true, Ordering::Release);
        Ok::<(), Error>(())
    };

    assert!(
        timeout(Duration::from_millis(50), connect).await.is_err(),
        "a server that never sends a handshake must exhaust the deadline"
    );
    timeout(Duration::from_secs(1), server)
        .await
        .expect("server must observe client-internal cancellation cleanup")
        .expect("stalled server task must finish");
    assert!(
        peer_closed.load(Ordering::Acquire),
        "client-internal cleanup may continue only to close its transport"
    );
    tokio::task::yield_now().await;
    assert!(
        !admitted.load(Ordering::Acquire),
        "the dropped adapter future has no late evidence or admission channel"
    );
}
