#![allow(dead_code)]
#![allow(hidden_glob_reexports)]
#![allow(unused_imports)]

pub use kuberic_mysql::core;

#[path = "adapter_source/mod.rs"]
mod adapter;
pub use adapter::*;
use adapter::{decode, diagnostic, error, observer, query, report, request, session, time};

#[path = "adapter_common/mod.rs"]
mod common;
#[path = "adapter_live_support/mod.rs"]
mod live_support;
#[path = "adapter_phase4_common/mod.rs"]
mod phase4_common;

#[tokio::test(flavor = "current_thread")]
async fn qualify_oracle_mysql_8_4_11() {
    live_support::run()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
}
