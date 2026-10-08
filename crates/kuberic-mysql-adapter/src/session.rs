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
}

pub(crate) struct MysqlNativeSession {
    connection: Conn,
}

impl MysqlNativeSession {
    pub(crate) async fn connect<C: ObservationClock>(
        request: &ObservationRequest<C>,
    ) -> Result<Self, SessionError> {
        let options = OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(1)
            .socket(Some(
                request.socket().as_path().to_string_lossy().into_owned(),
            ))
            .user(Some(request.credentials().username()))
            .pass(Some(request.credentials().password()));
        let connection = Conn::new(options)
            .await
            .map_err(|error| SessionError::from_client(error, ObservationStage::Connect, None))?;
        Ok(Self { connection })
    }
}

impl NativeSession for MysqlNativeSession {
    fn query<'a>(
        &'a mut self,
        query: QueryId,
    ) -> Pin<Box<dyn Future<Output = Result<RawResult, SessionError>> + Send + 'a>> {
        Box::pin(async move {
            let result = self
                .connection
                .query_iter(query.sql())
                .await
                .map_err(|error| {
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
}

fn column_kind(kind: ColumnType) -> ColumnKind {
    match kind {
        ColumnType::MYSQL_TYPE_VAR_STRING | ColumnType::MYSQL_TYPE_VARCHAR => ColumnKind::VarString,
        ColumnType::MYSQL_TYPE_STRING => ColumnKind::String,
        ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => ColumnKind::Long,
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
    fn from_client(
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
