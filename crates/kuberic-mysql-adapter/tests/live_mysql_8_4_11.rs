#![cfg(feature = "live-mysql-8-4-11")]
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
mod live_support;
mod phase4_common;

pub use diagnostic::*;
pub use report::ObservationReport;
pub use request::*;
pub use time::*;

use live_support::{MANIFEST_ENV, fixture};

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires explicit Oracle MySQL 8.4.11 installed-package manifest"]
async fn qualify_oracle_mysql_8_4_11() {
    let manifest =
        fixture::ManifestPath::from_env(MANIFEST_ENV).unwrap_or_else(|error| panic!("{error}"));
    live_support::run(manifest.as_path())
        .await
        .unwrap_or_else(|error| panic!("{error}"));
}
