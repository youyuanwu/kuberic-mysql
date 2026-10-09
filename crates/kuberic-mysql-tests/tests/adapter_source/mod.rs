#[path = "../../../kuberic-mysql/src/adapter/decode.rs"]
pub(crate) mod decode;
#[path = "../../../kuberic-mysql/src/adapter/diagnostic.rs"]
pub(crate) mod diagnostic;
#[path = "../../../kuberic-mysql/src/adapter/error.rs"]
pub(crate) mod error;
#[path = "../../../kuberic-mysql/src/adapter/observer.rs"]
pub(crate) mod observer;
#[path = "../../../kuberic-mysql/src/adapter/query.rs"]
pub(crate) mod query;
#[path = "../../../kuberic-mysql/src/adapter/report.rs"]
pub(crate) mod report;
#[path = "../../../kuberic-mysql/src/adapter/request.rs"]
pub(crate) mod request;
#[path = "../../../kuberic-mysql/src/adapter/session.rs"]
pub(crate) mod session;
#[path = "../../../kuberic-mysql/src/adapter/time.rs"]
pub(crate) mod time;

pub use diagnostic::{
    AdapterDiagnostic, AmbiguityKind, CoreOutcomeClass, DeadlineStage, EvidenceIssue,
    IdentityField, NativeSurface, ObservationStage, PlaceholderKind, ProductIssue, SchemaIssue,
    ServerErrorClass, SocketIssue, SqlState,
};
pub use observer::MysqlObserver;
pub use query::{ColumnContract, ColumnKind, QueryId};
pub use report::ObservationReport;
pub use request::{
    ObservationRequest, ObserverCredentials, RequestError, SocketPathError, UnixSocketPath,
};
pub use time::{ClockContext, ClockError, ObservationClock};
