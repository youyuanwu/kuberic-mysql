#[path = "service_replicator_live_support/mod.rs"]
mod replicator_live_support;

use replicator_live_support::{HandoffStage, ReplicatorLiveFixture, preflight_oracle_mysql_8_4};

#[tokio::test(flavor = "current_thread")]
async fn controlled_handoff_closes_source_before_target_publication() {
    preflight_oracle_mysql_8_4().unwrap_or_else(|error| panic!("{error}"));
    let evidence = ReplicatorLiveFixture::prepare()
        .await
        .unwrap_or_else(|error| panic!("{error}"))
        .controlled_handoff_and_finalize()
        .await
        .unwrap_or_else(|error| panic!("{error}"));

    let expected = [
        HandoffStage::InitialPublicationHistoryAccepted,
        HandoffStage::CancellationGenerationPublished,
        HandoffStage::CancellationContained,
        HandoffStage::SourceGenerationPublished,
        HandoffStage::SourceAutocommitCompleted,
        HandoffStage::SourceTransactionCompleted,
        HandoffStage::SourcePingQuitCompleted,
        HandoffStage::SourceRevokedEligible,
        HandoffStage::BoundaryFrozen,
        HandoffStage::EpochBarriersAccepted,
        HandoffStage::SourceTransferCompleted,
        HandoffStage::TargetPromotionCompleted,
        HandoffStage::RemainingSameRoleCompleted,
        HandoffStage::ConfigurationAccepted,
        HandoffStage::SourceStoppedAndReaped,
        HandoffStage::RemainingSuccessorAccepted,
        HandoffStage::TargetSuccessorAccepted,
        HandoffStage::PublicationProofAccepted,
        HandoffStage::TargetPublished,
        HandoffStage::TargetMutationCompleted,
        HandoffStage::SourceUnavailableConfirmed,
        HandoffStage::CleanupCompleted,
    ];
    assert_eq!(
        evidence
            .trace()
            .iter()
            .map(|entry| entry.stage)
            .collect::<Vec<_>>(),
        expected
    );
    assert!(
        evidence
            .trace()
            .windows(2)
            .all(|entries| entries[0].elapsed <= entries[1].elapsed)
    );
    assert!(
        evidence
            .trace()
            .iter()
            .all(|entry| !entry.evidence.is_empty())
    );
    assert_ne!(evidence.source_endpoint(), evidence.target_endpoint());
    assert!(evidence.boundary().executed().entries().len() == 1);
}

#[tokio::test(flavor = "current_thread")]
async fn prepared_and_published_owner_failures_use_exact_fixture_cleanup() {
    preflight_oracle_mysql_8_4().unwrap_or_else(|error| panic!("{error}"));
    for publish in [false, true] {
        ReplicatorLiveFixture::prepare()
            .await
            .unwrap_or_else(|error| panic!("{error}"))
            .inject_proxy_failure_and_finalize(publish)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
    }
}
