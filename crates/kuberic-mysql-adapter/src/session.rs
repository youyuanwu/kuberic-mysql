//! Private direct native-session boundary.

use core::future::Future;
use core::pin::Pin;

use mysql_async::consts::{ColumnFlags, ColumnType};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Error, OptsBuilder, Row, Value};

use crate::query::{ColumnKind, QueryId, RawColumn, RawResult, RawValue};
use crate::{
    AdapterDiagnostic, ObservationClock, ObservationRequest, ObservationStage, ServerErrorClass,
    SqlState,
};

pub(crate) trait NativeSession {
    fn query<'a>(
        &'a mut self,
        query: QueryId,
    ) -> Pin<Box<dyn Future<Output = Result<RawResult, SessionError>> + Send + 'a>>;

    fn disconnect(
        self: Box<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SessionError>> + Send + 'static>>;
}

#[must_use = "live native sessions must be explicitly disconnected"]
pub(crate) struct MysqlNativeSession {
    connection: Option<Conn>,
}

impl Drop for MysqlNativeSession {
    fn drop(&mut self) {
        assert!(
            self.connection.is_none() || std::thread::panicking(),
            "live native sessions must be explicitly disconnected"
        );
    }
}

impl MysqlNativeSession {
    pub(crate) async fn connect<C: ObservationClock>(
        request: &ObservationRequest<C>,
    ) -> Result<Self, SessionError> {
        let options = OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(1)
            .socket(Some(
                request
                    .socket()
                    .as_path()
                    .to_str()
                    .expect("UnixSocketPath rejects non-UTF-8 paths")
                    .to_owned(),
            ))
            .user(Some(request.credentials().username()))
            .pass(Some(request.credentials().password()));
        let connection = Conn::new(options)
            .await
            .map_err(|error| SessionError::from_client(error, ObservationStage::Connect, None))?;
        Ok(Self {
            connection: Some(connection),
        })
    }
}

impl NativeSession for MysqlNativeSession {
    fn query<'a>(
        &'a mut self,
        query: QueryId,
    ) -> Pin<Box<dyn Future<Output = Result<RawResult, SessionError>> + Send + 'a>> {
        Box::pin(async move {
            let connection = self
                .connection
                .as_mut()
                .expect("connected session is consumed only by disconnect");
            let result = connection.query_iter(query.sql()).await.map_err(|error| {
                SessionError::from_client(error, ObservationStage::Query, Some(query.surface()))
            })?;
            let columns = result
                .columns_ref()
                .iter()
                .map(|column| RawColumn {
                    name: column.name_str().into_owned(),
                    kind: column_kind(column.column_type()),
                    nullable: !column.flags().contains(ColumnFlags::NOT_NULL_FLAG),
                })
                .collect();
            let rows: Vec<Row> = result.collect_and_drop().await.map_err(|error| {
                SessionError::from_client(error, ObservationStage::Consume, Some(query.surface()))
            })?;
            Ok(RawResult {
                columns,
                rows: rows
                    .iter()
                    .map(|row| {
                        (0..row.len())
                            .map(|index| row.as_ref(index).map(raw_value).unwrap_or(RawValue::Null))
                            .collect()
                    })
                    .collect(),
            })
        })
    }

    fn disconnect(
        mut self: Box<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SessionError>> + Send + 'static>> {
        Box::pin(async move {
            let connection = self
                .connection
                .take()
                .expect("connected session is disconnected exactly once");
            connection.disconnect().await.map_err(|error| {
                SessionError::from_client(error, ObservationStage::Disconnect, None)
            })
        })
    }
}

fn column_kind(kind: ColumnType) -> ColumnKind {
    match kind {
        ColumnType::MYSQL_TYPE_VAR_STRING | ColumnType::MYSQL_TYPE_VARCHAR => ColumnKind::VarString,
        ColumnType::MYSQL_TYPE_STRING => ColumnKind::String,
        ColumnType::MYSQL_TYPE_LONG => ColumnKind::Long,
        ColumnType::MYSQL_TYPE_LONGLONG => ColumnKind::LongLong,
        other => ColumnKind::Other(other as u8),
    }
}

fn raw_value(value: &Value) -> RawValue {
    match value {
        Value::NULL => RawValue::Null,
        Value::Bytes(value) => RawValue::Bytes(value.clone()),
        Value::Int(value) => RawValue::Signed(*value),
        Value::UInt(value) => RawValue::Unsigned(*value),
        Value::Float(value) => RawValue::Float(*value),
        Value::Double(value) => RawValue::Double(*value),
        Value::Date(..) => RawValue::Date,
        Value::Time(..) => RawValue::Time,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use kuberic_mysql_core::{
        AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, EndpointBinding,
        Epoch, ExactBinding, ExactBindingParts, GroupName, MemberAddress, MemberId,
        ObservationInstant, ObservationProvenance, ObservationSessionId, PartitionId,
        ProcessSessionId, ReplicaId, ReplicaIncarnation, ResourceId, ServerUuid, StorageBinding,
        ViewId,
    };
    use mysql_async::consts::ColumnType;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixListener;
    use tokio::time::timeout;

    use super::{ColumnKind, MysqlNativeSession, NativeSession, column_kind};
    use crate::{
        ClockContext, ClockError, ObservationClock, ObservationRequest, ObserverCredentials,
        UnixSocketPath,
    };

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn int24_protocol_metadata_does_not_alias_long() {
        assert_eq!(
            column_kind(ColumnType::MYSQL_TYPE_INT24),
            ColumnKind::Other(ColumnType::MYSQL_TYPE_INT24 as u8)
        );
        assert_eq!(column_kind(ColumnType::MYSQL_TYPE_LONG), ColumnKind::Long);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn connected_session_cancellation_explicitly_disconnects_without_late_admission() {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "session-cancellation-{}-{sequence}",
                std::process::id()
            ));
        fs::create_dir_all(&directory).expect("create session test directory");
        let socket = directory.join("mysql.sock");
        let listener = UnixListener::bind(&socket).expect("bind fake MySQL socket");
        let quit_seen = Arc::new(AtomicBool::new(false));
        let server_quit_seen = Arc::clone(&quit_seen);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept native session");
            write_packet(&mut stream, 0, &handshake()).await;
            let authentication = read_packet(&mut stream)
                .await
                .expect("authentication packet");
            assert!(!authentication.is_empty());
            write_packet(&mut stream, 2, &[0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]).await;

            let settings = read_packet(&mut stream)
                .await
                .expect("settings query packet");
            assert_eq!(settings.first(), Some(&0x03));
            assert_eq!(
                std::str::from_utf8(&settings[1..]).expect("settings SQL"),
                "SELECT @@max_allowed_packet,@@wait_timeout"
            );
            write_packet(&mut stream, 1, &[0x02]).await;
            write_packet(&mut stream, 2, &column_definition("@@max_allowed_packet")).await;
            write_packet(&mut stream, 3, &column_definition("@@wait_timeout")).await;
            write_packet(&mut stream, 4, &[0xfe, 0x00, 0x00, 0x02, 0x00]).await;
            write_packet(
                &mut stream,
                5,
                &row(&[b"67108864".as_slice(), b"28800".as_slice()]),
            )
            .await;
            write_packet(&mut stream, 6, &[0xfe, 0x00, 0x00, 0x02, 0x00]).await;

            let command = read_packet(&mut stream)
                .await
                .expect("explicit disconnect packet");
            assert_eq!(command, [0x01], "cancellation must send only COM_QUIT");
            server_quit_seen.store(true, Ordering::Release);
            let mut byte = [0_u8; 1];
            assert_eq!(
                stream.read(&mut byte).await.expect("connection close"),
                0,
                "explicit disconnect must close the connected stream"
            );
        });

        let request = request(&socket);
        let session = timeout(
            Duration::from_secs(1),
            MysqlNativeSession::connect(&request),
        )
        .await
        .expect("connect deadline")
        .expect("connected native session");
        let admitted = Arc::new(AtomicBool::new(false));

        timeout(Duration::from_secs(1), Box::new(session).disconnect())
            .await
            .expect("bounded explicit teardown")
            .expect("clean explicit teardown");
        timeout(Duration::from_secs(1), server)
            .await
            .expect("server completion deadline")
            .expect("fake server");
        tokio::task::yield_now().await;

        assert!(quit_seen.load(Ordering::Acquire));
        assert!(
            !admitted.load(Ordering::Acquire),
            "a cancelled connected session must never admit a late result"
        );

        fs::remove_file(socket).expect("remove fake socket");
        fs::remove_dir(directory).expect("remove session test directory");
    }

    async fn write_packet(stream: &mut tokio::net::UnixStream, sequence: u8, payload: &[u8]) {
        let length = payload.len();
        let header = [
            length as u8,
            (length >> 8) as u8,
            (length >> 16) as u8,
            sequence,
        ];
        stream
            .write_all(&header)
            .await
            .expect("write packet header");
        stream
            .write_all(payload)
            .await
            .expect("write packet payload");
    }

    async fn read_packet(stream: &mut tokio::net::UnixStream) -> io::Result<Vec<u8>> {
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

    fn column_definition(name: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        for value in ["def", "", "", "", name, ""] {
            payload.push(value.len() as u8);
            payload.extend_from_slice(value.as_bytes());
        }
        payload.push(0x0c);
        payload.extend_from_slice(&45_u16.to_le_bytes());
        payload.extend_from_slice(&20_u32.to_le_bytes());
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

    fn request(socket: &std::path::Path) -> ObservationRequest<TestClock> {
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
            member_address: MemberAddress::new("mysql-a:3306").unwrap(),
            configuration: ConfigurationId::new("configuration").unwrap(),
            epoch: Epoch::new("epoch").unwrap(),
            authority_generation: AuthorityGeneration::new("authority").unwrap(),
            view_id: ViewId::new("view-0001").unwrap(),
            credential_generation: CredentialGeneration::new("credential").unwrap(),
        });
        ObservationRequest::new(
            binding.clone(),
            UnixSocketPath::new(socket).unwrap(),
            ObserverCredentials::new("observer", "fixture-secret").unwrap(),
            ObservationProvenance::new("session-contract", binding.parts().attempt.clone())
                .unwrap(),
            ClockContext::new(
                TestClock {
                    origin: Instant::now(),
                },
                ObservationInstant::new(1000),
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[derive(Debug)]
    struct TestClock {
        origin: Instant,
    }

    impl ObservationClock for TestClock {
        fn now(&self) -> ObservationInstant {
            ObservationInstant::new(0)
        }

        fn to_std_instant(&self, instant: ObservationInstant) -> Result<Instant, ClockError> {
            self.origin
                .checked_add(Duration::from_millis(instant.tick()))
                .ok_or(ClockError::UnrepresentableInstant)
        }
    }
}

#[derive(Debug)]
pub(crate) enum SessionError {
    Transport {
        stage: ObservationStage,
    },
    Server {
        stage: ObservationStage,
        surface: Option<crate::NativeSurface>,
        code: u16,
        sql_state: SqlState,
    },
    Client {
        stage: ObservationStage,
        surface: Option<crate::NativeSurface>,
    },
}

impl SessionError {
    pub(crate) fn from_client(
        error: Error,
        stage: ObservationStage,
        surface: Option<crate::NativeSurface>,
    ) -> Self {
        match error {
            Error::Io(_) => Self::Transport { stage },
            Error::Server(server) => match SqlState::new(server.state) {
                Ok(sql_state) => Self::Server {
                    stage: if server.code == 1045 || sql_state.as_str() == "28000" {
                        ObservationStage::Authenticate
                    } else {
                        stage
                    },
                    surface,
                    code: server.code,
                    sql_state,
                },
                Err(_) => Self::Client { stage, surface },
            },
            Error::Driver(_) | Error::Other(_) | Error::Url(_) => Self::Client { stage, surface },
        }
    }

    pub(crate) fn diagnostic(&self) -> AdapterDiagnostic {
        match self {
            Self::Transport { stage } => AdapterDiagnostic::Transport { stage: *stage },
            Self::Server {
                stage,
                surface,
                code,
                sql_state,
            } => {
                let class = if *code == 1045 || sql_state.as_str() == "28000" {
                    ServerErrorClass::Authentication
                } else if matches!(*code, 1142 | 1143 | 1227) {
                    ServerErrorClass::Permission
                } else {
                    ServerErrorClass::Other
                };
                AdapterDiagnostic::Server {
                    stage: *stage,
                    surface: *surface,
                    class,
                    code: *code,
                    sql_state: sql_state.clone(),
                }
            }
            Self::Client { stage, surface } => AdapterDiagnostic::Client {
                stage: *stage,
                surface: *surface,
            },
        }
    }
}
