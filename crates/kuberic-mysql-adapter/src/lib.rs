//! Read-only Oracle MySQL native-observation boundary.
//!
//! The public API contains validated request values, an adapter-owned query
//! contract, monotonic clock types, and secret-free reports. Client-library,
//! runtime, SQL row, and protocol types remain private.

#[allow(dead_code)]
mod decode;
mod diagnostic;
mod query;
mod report;
mod request;
#[allow(dead_code)]
mod session;
mod time;

pub use diagnostic::{
    AdapterDiagnostic, AmbiguityKind, CoreOutcomeClass, EvidenceIssue, IdentityField,
    NativeSurface, ObservationStage, PlaceholderKind, ProductIssue, SchemaIssue, ServerErrorClass,
    SqlState,
};
pub use query::{ColumnContract, ColumnKind, QueryId};
pub use report::ObservationReport;
pub use request::{
    ObservationRequest, ObserverCredentials, RequestError, SocketPathError, UnixSocketPath,
};
pub use time::{ClockContext, ClockError, ObservationClock};
