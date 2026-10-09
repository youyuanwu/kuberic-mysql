//! Read-only Oracle MySQL native-observation boundary.
//!
//! The public API contains validated request values, an adapter-owned query
//! contract, monotonic clock types, and secret-free reports. Client-library,
//! runtime, SQL row, and protocol types remain private.
//!
//! Request values fail closed before any connection attempt:
//!
//! ```
//! use kuberic_mysql::adapter::{SocketPathError, UnixSocketPath};
//!
//! assert_eq!(
//!     UnixSocketPath::new("relative.sock").unwrap_err(),
//!     SocketPathError::NotAbsolute,
//! );
//! ```
//!
//! Callers inspect the authoritative outcome and the separate, secret-free
//! diagnostic. Native role and read-only switches are evidence only; this
//! adapter never opens Kuberic client access.
//!
//! ```
//! use kuberic_mysql::adapter::{CoreOutcomeClass, ObservationReport};
//!
//! fn inspect(report: &ObservationReport) {
//!     if report.outcome().valid().is_some() {
//!         assert_eq!(report.diagnostic().core_class(), CoreOutcomeClass::Valid);
//!     } else {
//!         let _machine_matchable_class = report.diagnostic().core_class();
//!     }
//! }
//! ```
//!
//! The standard repository test gate runs the Oracle MySQL 8.4.11 live
//! qualification; lifecycle automation is not part of this crate's production
//! API.

#[allow(dead_code)]
mod decode;
mod diagnostic;
mod error;
mod observer;
mod query;
mod report;
mod request;
#[allow(dead_code)]
mod session;
mod time;

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
