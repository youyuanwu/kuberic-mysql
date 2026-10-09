//! Bounded, restart-stateless ownership of one local Oracle MySQL process.
//!
//! This crate owns exactly one fresh Oracle MySQL Community Server 8.4.11
//! generation. Process identity and lifecycle state exist only in memory.
//! Losing the manager means losing ownership context: callers must reset the
//! fixture rather than discover, adopt, or continue a surviving process.
//!
//! Persistent MySQL files live beneath the caller-selected data root. Generated
//! configuration, logs, temporary files, PID state, and the private Unix socket
//! live beneath a separate disposable scratch root. No application metadata
//! file, journal, receipt database, state JSON, or adoption record is created.
//!
//! The manager does not publish client access, bootstrap Group Replication, or
//! implement Kuberic callbacks. Observation is delegated unchanged to
//! [`crate::adapter::MysqlObserver`] after checking that the request
//! selects the currently owned private socket.

mod config;
mod error;
mod instance;
mod process;

pub use config::{MysqlInstanceConfig, MysqlOperationTimeouts, MysqlRuntimePaths};
pub use error::{
    ConfigError, LifecycleOperation, MysqlInstanceError, OwnershipError, ProductError,
};
pub use instance::{MysqlInstanceManager, MysqlInstanceState};
