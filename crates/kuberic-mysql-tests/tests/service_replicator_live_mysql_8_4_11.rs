#[path = "service_replicator_live_support/mod.rs"]
mod replicator_live_support;

use std::collections::HashSet;

use kuberic_mysql::core::ProgressProjection;

use replicator_live_support::{ReplicatorLiveFixture, preflight_oracle_mysql_8_4_11};

#[tokio::test(flavor = "current_thread")]
async fn fresh_formation_and_controlled_handoff() {
    preflight_oracle_mysql_8_4_11().unwrap_or_else(|error| panic!("{error}"));
    let formation = ReplicatorLiveFixture::prepare()
        .await
        .unwrap_or_else(|error| panic!("{error}"))
        .form_and_finalize()
        .await
        .unwrap_or_else(|error| panic!("{error}"));

    assert_eq!(
        formation
            .targets()
            .iter()
            .map(|target| target.member().member_id())
            .collect::<HashSet<_>>()
            .len(),
        3
    );
    assert_ne!(formation.first_view(), formation.second_view());
    assert_ne!(formation.second_view(), formation.final_view());
    assert!(matches!(
        formation.progress(),
        ProgressProjection::Projectable(_)
    ));
}
