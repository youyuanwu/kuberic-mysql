#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::net::UnixListener;
use std::time::{Duration, Instant};

use kuberic_mysql_adapter as public_adapter;
use kuberic_mysql_core::{
    AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, EndpointBinding, Epoch,
    ExactBinding, ExactBindingParts, GroupName, IncoherentReason, MemberAddress, MemberId,
    ObservationInstant, ObservationOutcome, ObservationProvenance, ObservationSessionId,
    PartitionId, ProcessSessionId, ReplicaId, ReplicaIncarnation, ResourceId, ServerUuid,
    StaleReason, StorageBinding, ViewId,
};

use crate::common::{self, FixtureCase};
use crate::observer::observe_with;
use crate::phase4_common::{
    ConnectAction, ScriptClock, ScriptedConnector, TestSocket, bytes, mutate_step, pending_step,
    replace_query_step, request as scripted_request, success_steps,
};
use crate::query::{QueryId, RawValue};

use super::accounts::FixtureAccounts;
use super::fixture::RunningFixture;
use super::state::{BaselineEvidence, OnlineIdentity};
use super::{QualificationCode, QualificationError};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum EvidenceOriginKind {
    NativeRequired,
    OracleOwned,
    Synthetic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScenarioMode {
    DirectSetup,
    PublicObserver,
    FixtureDecode,
    ScriptedObserver,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OracleProvenance {
    pub scenario: &'static str,
    pub commit: &'static str,
    pub tag: &'static str,
    pub url: &'static str,
    pub path: &'static str,
    pub sha256: &'static str,
    pub range: &'static str,
    pub derivation: &'static str,
}

const COMMIT: &str = "99960bf74fa919347e4f4e3ca47672f333d6e91f";
#[rustfmt::skip]
pub const ORACLE_PROVENANCE: [OracleProvenance; 5] = [
    OracleProvenance { scenario: "plugin-absent-members", commit: COMMIT, tag: "mysql-8.4.11", url: "https://raw.githubusercontent.com/mysql/mysql-server/99960bf74fa919347e4f4e3ca47672f333d6e91f/mysql-test/suite/group_replication/r/gr_uninstall_install_plugin.result", path: "mysql-test/suite/group_replication/r/gr_uninstall_install_plugin.result", sha256: "c9e374b6f66c88322c2022197558be23b26e08c8bb43d0d2bb88c57157aefee5", range: "lines 18-21", derivation: "Represent both asserted-empty Group Replication Performance Schema tables as exact selected metadata with zero rows." },
    OracleProvenance { scenario: "never-started-members-placeholder", commit: COMMIT, tag: "mysql-8.4.11", url: "https://raw.githubusercontent.com/mysql/mysql-server/99960bf74fa919347e4f4e3ca47672f333d6e91f/mysql-test/suite/group_replication/r/gr_perfschema_group_members.result", path: "mysql-test/suite/group_replication/r/gr_perfschema_group_members.result", sha256: "19bb50fd40f07c6e22fc7b6f7589a946ce9ad2815eccd1bf9471d5caa5e17db5", range: "lines 3-6", derivation: "Map asserted empty MEMBER_ID/HOST, NULL MEMBER_PORT, and OFFLINE state before first start; the unasserted role remains the documented empty placeholder." },
    OracleProvenance { scenario: "stopped-members-placeholder", commit: COMMIT, tag: "mysql-8.4.11", url: "https://raw.githubusercontent.com/mysql/mysql-server/99960bf74fa919347e4f4e3ca47672f333d6e91f/mysql-test/suite/group_replication/r/gr_perfschema_group_members.result", path: "mysql-test/suite/group_replication/r/gr_perfschema_group_members.result", sha256: "19bb50fd40f07c6e22fc7b6f7589a946ce9ad2815eccd1bf9471d5caa5e17db5", range: "lines 8-17", derivation: "Retain the asserted post-start identity/address facts and asserted OFFLINE state after STOP; represent the unavailable active role as empty." },
    OracleProvenance { scenario: "recovering-member", commit: COMMIT, tag: "mysql-8.4.11", url: "https://raw.githubusercontent.com/mysql/mysql-server/99960bf74fa919347e4f4e3ca47672f333d6e91f/mysql-test/suite/group_replication/r/gr_recovery_errors_report.result", path: "mysql-test/suite/group_replication/r/gr_recovery_errors_report.result", sha256: "2ebe750c1156c8fea79c00629eaaf1b6caf1ed976d6f06c86bbe2a467e727d87", range: "lines 31-35", derivation: "Represent the asserted RECOVERING member state with deterministic non-secret fixture identity/address and the secondary role used by the decoder contract." },
    OracleProvenance { scenario: "never-started-filtered-stats-absent", commit: COMMIT, tag: "mysql-8.4.11", url: "https://raw.githubusercontent.com/mysql/mysql-server/99960bf74fa919347e4f4e3ca47672f333d6e91f/mysql-test/suite/group_replication/r/gr_perfschema_group_member_stats.result", path: "mysql-test/suite/group_replication/r/gr_perfschema_group_member_stats.result", sha256: "57ba8d2f87870bdb32a286749b8e1796cd279ce5318617e90b3e09251867be85", range: "lines 14-15", derivation: "The upstream unfiltered table contains an empty MEMBER_ID placeholder before Group Replication starts; the adapter query filters on MEMBER_ID = @@GLOBAL.server_uuid, so that row cannot match and the selected result has zero rows." },
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScenarioResult {
    pub name: String,
    pub origin: EvidenceOriginKind,
    pub mode: ScenarioMode,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScenarioResults {
    results: Vec<ScenarioResult>,
}

impl ScenarioResults {
    pub fn new(results: Vec<ScenarioResult>) -> Self {
        Self { results }
    }

    pub fn validate_complete(&self) -> Result<(), QualificationError> {
        let expected = required_inventory()?;
        let mut actual = BTreeMap::new();
        for result in &self.results {
            actual.insert((result.name.clone(), result.origin), result);
        }
        for (name, origin) in &expected {
            let Some(result) = actual.get(&(name.clone(), *origin)) else {
                return Err(QualificationError::new(
                    QualificationCode::ScenarioFailure,
                    "scenario completeness",
                    format!("missing required scenario {name} ({origin:?})"),
                ));
            };
            if !result.passed {
                return Err(QualificationError::new(
                    QualificationCode::ScenarioFailure,
                    "scenario completeness",
                    format!("required scenario failed: {name}: {}", result.detail),
                ));
            }
        }
        Ok(())
    }
}

pub async fn run_all(
    fixture: &RunningFixture,
    accounts: &FixtureAccounts,
    baseline: &BaselineEvidence,
    online: &OnlineIdentity,
) -> Result<(), QualificationError> {
    let mut results = vec![
        ScenarioResult {
            name: "live-product-version".to_owned(),
            origin: EvidenceOriginKind::NativeRequired,
            mode: ScenarioMode::DirectSetup,
            passed: baseline.product.version == "8.4.11"
                && baseline.product.version_comment == "MySQL Community Server - GPL",
            detail: format!(
                "{} / {}",
                baseline.product.version, baseline.product.version_comment
            ),
        },
        ScenarioResult {
            name: "live-empty-gtid-history".to_owned(),
            origin: EvidenceOriginKind::NativeRequired,
            mode: ScenarioMode::DirectSetup,
            passed: baseline.empty_gtid_history.is_empty(),
            detail: if baseline.empty_gtid_history.is_empty() {
                "empty gtid_executed".to_owned()
            } else {
                format!("gtid_executed={}", baseline.empty_gtid_history)
            },
        },
        ScenarioResult {
            name: "live-native-tagged-gtid-functions".to_owned(),
            origin: EvidenceOriginKind::NativeRequired,
            mode: ScenarioMode::DirectSetup,
            passed: baseline.gtid_forms.oracle == "GTID_SUBSET"
                && baseline.gtid_forms.response.is_exact_one()
                && baseline.gtid_forms.code.is_none()
                && baseline.gtid_forms.sql_state.is_none(),
            detail: format!(
                "response={:?} code={:?} state={:?}",
                baseline.gtid_forms.response,
                baseline.gtid_forms.code,
                baseline.gtid_forms.sql_state
            ),
        },
        ScenarioResult {
            name: "live-gtid-boundary-9223372036854775806".to_owned(),
            origin: EvidenceOriginKind::NativeRequired,
            mode: ScenarioMode::DirectSetup,
            passed: lower_boundary_qualified(&baseline.lower_boundary),
            detail: format!(
                "accepted={} code={:?} state={:?}",
                baseline.lower_boundary.accepted,
                baseline.lower_boundary.code,
                baseline.lower_boundary.sql_state
            ),
        },
        ScenarioResult {
            name: "live-gtid-boundary-9223372036854775807".to_owned(),
            origin: EvidenceOriginKind::NativeRequired,
            mode: ScenarioMode::DirectSetup,
            passed: upper_boundary_qualified(&baseline.upper_boundary),
            detail: format!(
                "accepted={} code={:?} state={:?}",
                baseline.upper_boundary.accepted,
                baseline.upper_boundary.code,
                baseline.upper_boundary.sql_state
            ),
        },
    ];

    let success = observe_live(fixture, online, accounts.observer(), "success").await?;
    let success_passed = success.outcome().valid().is_some();
    results.push(ScenarioResult {
        name: "live-uds-observer-success".to_owned(),
        origin: EvidenceOriginKind::NativeRequired,
        mode: ScenarioMode::PublicObserver,
        passed: success_passed,
        detail: format!("{:?} / {:?}", success.outcome(), success.diagnostic()),
    });
    results.push(ScenarioResult {
        name: "live-strict-uds-only".to_owned(),
        origin: EvidenceOriginKind::NativeRequired,
        mode: ScenarioMode::PublicObserver,
        passed: baseline.skip_networking && success_passed,
        detail: format!(
            "skip_networking={} valid={success_passed}",
            baseline.skip_networking
        ),
    });
    let missing_socket = fixture.socket_path().with_file_name("x");
    let _ = fs::remove_file(&missing_socket);
    let listener = UnixListener::bind(&missing_socket).map_err(|error| {
        QualificationError::new(
            QualificationCode::LaunchFailure,
            "create transport failure socket",
            error.to_string(),
        )
    })?;
    let validated = public_adapter::UnixSocketPath::new(&missing_socket).map_err(|error| {
        QualificationError::new(
            QualificationCode::LaunchFailure,
            "validate transport failure socket",
            error.to_string(),
        )
    })?;
    drop(listener);
    fs::remove_file(&missing_socket).map_err(|error| {
        QualificationError::new(
            QualificationCode::CleanupFailure,
            "remove transport failure socket",
            error.to_string(),
        )
    })?;
    let transport = observe_live_on_socket(
        fixture,
        online,
        accounts.observer().username(),
        accounts.observer().password(),
        "transport-failure",
        validated,
    )
    .await?;
    results.push(ScenarioResult {
        name: "live-uds-transport-failure".to_owned(),
        origin: EvidenceOriginKind::NativeRequired,
        mode: ScenarioMode::PublicObserver,
        passed: matches!(transport.outcome(), ObservationOutcome::Unreachable(_))
            && matches!(
                transport.diagnostic(),
                public_adapter::AdapterDiagnostic::Socket {
                    issue: public_adapter::SocketIssue::Missing
                }
            ),
        detail: format!("{:?} / {:?}", transport.outcome(), transport.diagnostic()),
    });

    let auth = observe_live_with_password(
        fixture,
        online,
        accounts.observer().username(),
        accounts.invalid_observer_password(),
        "auth-failure",
    )
    .await?;
    results.push(ScenarioResult {
        name: "live-authentication-failure".to_owned(),
        origin: EvidenceOriginKind::NativeRequired,
        mode: ScenarioMode::PublicObserver,
        passed: matches!(auth.outcome(), ObservationOutcome::AuthenticationFailure(_)),
        detail: format!("{:?} / {:?}", auth.outcome(), auth.diagnostic()),
    });

    let members =
        observe_live(fixture, online, accounts.members_denied(), "members-denied").await?;
    results.push(ScenarioResult {
        name: "live-members-table-permission-denied".to_owned(),
        origin: EvidenceOriginKind::NativeRequired,
        mode: ScenarioMode::PublicObserver,
        passed: matches!(members.outcome(), ObservationOutcome::PermissionDenied(_)),
        detail: format!("{:?} / {:?}", members.outcome(), members.diagnostic()),
    });
    let stats = observe_live(fixture, online, accounts.stats_denied(), "stats-denied").await?;
    results.push(ScenarioResult {
        name: "live-stats-table-permission-denied".to_owned(),
        origin: EvidenceOriginKind::NativeRequired,
        mode: ScenarioMode::PublicObserver,
        passed: matches!(stats.outcome(), ObservationOutcome::PermissionDenied(_)),
        detail: format!("{:?} / {:?}", stats.outcome(), stats.diagnostic()),
    });

    results.extend(run_fixture_origin_scenarios(
        EvidenceOriginKind::OracleOwned,
    )?);
    results.extend(run_fixture_origin_scenarios(EvidenceOriginKind::Synthetic)?);
    results.extend(run_scripted_scenarios().await);

    ScenarioResults::new(results).validate_complete()
}

pub fn validate_fixture_origin_contract(
    case: &FixtureCase,
) -> Result<EvidenceOriginKind, QualificationError> {
    common::validate_required_origin(case).map_err(|error| {
        QualificationError::new(
            QualificationCode::OriginMismatch,
            "fixture origin category",
            format!("{}: {error}", case.scenario),
        )
    })?;
    match case.evidence_origin.category.as_str() {
        "native-required" => {
            if case.evidence_origin.qualification.as_deref() != Some("required-live-8.4.11") {
                return Err(QualificationError::new(
                    QualificationCode::OriginMismatch,
                    "native fixture qualification tag",
                    format!("{} missing required-live-8.4.11", case.scenario),
                ));
            }
            Ok(EvidenceOriginKind::NativeRequired)
        }
        "oracle-owned" => {
            let Some(expected) = ORACLE_PROVENANCE
                .iter()
                .find(|entry| entry.scenario == case.scenario)
            else {
                return Err(QualificationError::new(
                    QualificationCode::OriginMismatch,
                    "oracle-owned fixture inventory",
                    format!("{} is not pinned", case.scenario),
                ));
            };
            if case.evidence_origin.commit.as_deref() != Some(expected.commit)
                || case.evidence_origin.tag.as_deref() != Some(expected.tag)
                || case.evidence_origin.url.as_deref() != Some(expected.url)
                || case.evidence_origin.path.as_deref() != Some(expected.path)
                || case.evidence_origin.sha256.as_deref() != Some(expected.sha256)
                || case.evidence_origin.range.as_deref() != Some(expected.range)
                || case.evidence_origin.derivation.as_deref() != Some(expected.derivation)
            {
                return Err(QualificationError::new(
                    QualificationCode::OriginMismatch,
                    "oracle-owned fixture provenance",
                    format!("{} has incomplete Oracle provenance", case.scenario),
                ));
            }
            Ok(EvidenceOriginKind::OracleOwned)
        }
        "synthetic" => {
            if case.evidence_origin.derivation.is_none() {
                return Err(QualificationError::new(
                    QualificationCode::OriginMismatch,
                    "synthetic fixture provenance",
                    format!("{} missing derivation", case.scenario),
                ));
            }
            Ok(EvidenceOriginKind::Synthetic)
        }
        other => Err(QualificationError::new(
            QualificationCode::OriginMismatch,
            "fixture origin category",
            format!("{} declared unsupported origin {other}", case.scenario),
        )),
    }
}

fn run_fixture_origin_scenarios(
    required: EvidenceOriginKind,
) -> Result<Vec<ScenarioResult>, QualificationError> {
    let mut results = Vec::new();
    for file in common::load_fixture_files() {
        for case in file.cases {
            let origin = validate_fixture_origin_contract(&case)?;
            if origin != required {
                continue;
            }
            let diagnostic = execute_fixture(&case);
            results.push(ScenarioResult {
                name: case.scenario.clone(),
                origin,
                mode: ScenarioMode::FixtureDecode,
                passed: core_class_name(diagnostic.core_class()) == case.expected.core_class
                    && diagnostic_name(&diagnostic) == case.expected.diagnostic,
                detail: format!(
                    "{} / {}",
                    core_class_name(diagnostic.core_class()),
                    diagnostic_name(&diagnostic)
                ),
            });
        }
    }
    Ok(results)
}

async fn run_scripted_scenarios() -> Vec<ScenarioResult> {
    let mut results = Vec::new();

    results.push(
        run_scripted_incoherent("scripted-group-name-change", |steps| {
            mutate_step(steps, 5, |result| {
                result.rows[0][0] = bytes("dddddddd-dddd-dddd-dddd-dddddddddddd")
            });
        })
        .await,
    );
    results.push(
        run_scripted_incoherent("scripted-group-replication-address-change", |steps| {
            mutate_step(steps, 5, |result| {
                result.rows[0][1] = bytes("127.0.0.1:33062")
            });
        })
        .await,
    );
    results.push(
        run_scripted_incoherent("scripted-member-address-change", |steps| {
            mutate_step(steps, 6, |result| result.rows[0][1] = bytes("mysql-b"));
        })
        .await,
    );
    results.push(
        run_scripted_incoherent("scripted-member-role-change", |steps| {
            mutate_step(steps, 6, |result| result.rows[0][4] = bytes("SECONDARY"));
        })
        .await,
    );
    results.push(
        run_scripted_incoherent("scripted-member-state-change", |steps| {
            mutate_step(steps, 6, |result| result.rows[0][3] = bytes("RECOVERING"));
        })
        .await,
    );
    results.push(
        run_scripted_incoherent("scripted-membership-change", |steps| {
            mutate_step(steps, 6, |result| {
                let mut second = result.rows[0].clone();
                second[0] = bytes("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
                second[1] = bytes("mysql-b");
                second[2] = RawValue::Signed(3307);
                result.rows.push(second);
            });
        })
        .await,
    );
    results.push(
        run_scripted_incoherent("scripted-view-id-change", |steps| {
            mutate_step(steps, 7, |result| result.rows[0][1] = bytes("view-0002"));
        })
        .await,
    );
    results.push(
        run_scripted_incoherent("scripted-read-only-change", |steps| {
            mutate_step(steps, 5, |result| result.rows[0][2] = RawValue::Signed(1));
        })
        .await,
    );
    results.push(
        run_scripted_incoherent("scripted-super-read-only-change", |steps| {
            mutate_step(steps, 5, |result| result.rows[0][3] = RawValue::Signed(1));
        })
        .await,
    );

    results.push(
        run_scripted_stale("scripted-connect-timeout", |connector| {
            connector.with_connect(ConnectAction::Pending { advance_to: 101 })
        })
        .await,
    );
    results.push(
        run_scripted_stale("scripted-query-timeout", |_connector| {
            let mut steps = success_steps();
            replace_query_step(
                &mut steps,
                0,
                pending_step(QueryId::Mysql8411ProductIdentityV1).action,
            );
            ScriptedConnector::new(ScriptClock::new(10), steps, Default::default())
        })
        .await,
    );
    results.push(
        run_scripted_stale("scripted-disconnect-timeout", |connector| {
            connector.with_pending_disconnect(101)
        })
        .await,
    );

    results
}

async fn run_scripted_incoherent(
    name: &str,
    mutate: impl FnOnce(&mut Vec<crate::phase4_common::ScriptStep>),
) -> ScenarioResult {
    let clock = ScriptClock::new(10);
    let socket = TestSocket::new(name);
    let mut steps = success_steps();
    mutate(&mut steps);
    let connector = ScriptedConnector::new(clock.clone(), steps, Default::default());
    let report = observe_with(&scripted_request(&socket, clock), &connector).await;
    ScenarioResult {
        name: name.to_owned(),
        origin: EvidenceOriginKind::Synthetic,
        mode: ScenarioMode::ScriptedObserver,
        passed: matches!(
            report.outcome(),
            ObservationOutcome::Incoherent {
                reason: IncoherentReason::NativeSnapshotMismatch,
                ..
            }
        ),
        detail: format!("{:?} / {:?}", report.outcome(), report.diagnostic()),
    }
}

async fn run_scripted_stale(
    name: &str,
    configure: impl FnOnce(ScriptedConnector) -> ScriptedConnector,
) -> ScenarioResult {
    let clock = ScriptClock::new(10);
    let socket = TestSocket::new(name);
    let connector = configure(ScriptedConnector::new(
        clock.clone(),
        success_steps(),
        Default::default(),
    ));
    let report = observe_with(&scripted_request(&socket, clock), &connector).await;
    ScenarioResult {
        name: name.to_owned(),
        origin: EvidenceOriginKind::Synthetic,
        mode: ScenarioMode::ScriptedObserver,
        passed: matches!(
            report.outcome(),
            ObservationOutcome::Stale {
                reason: StaleReason::Expired,
                ..
            }
        ),
        detail: format!("{:?} / {:?}", report.outcome(), report.diagnostic()),
    }
}

async fn observe_live(
    fixture: &RunningFixture,
    online: &OnlineIdentity,
    credentials: &super::accounts::MysqlCredentials,
    attempt: &str,
) -> Result<public_adapter::ObservationReport, QualificationError> {
    observe_live_with_password(
        fixture,
        online,
        credentials.username(),
        credentials.password(),
        attempt,
    )
    .await
}

async fn observe_live_with_password(
    fixture: &RunningFixture,
    online: &OnlineIdentity,
    username: &str,
    password: &str,
    attempt: &str,
) -> Result<public_adapter::ObservationReport, QualificationError> {
    let socket = public_adapter::UnixSocketPath::new(fixture.socket_path()).map_err(|error| {
        QualificationError::new(
            QualificationCode::LaunchFailure,
            "validate live socket path",
            error.to_string(),
        )
    })?;
    observe_live_on_socket(fixture, online, username, password, attempt, socket).await
}

async fn observe_live_on_socket(
    fixture: &RunningFixture,
    online: &OnlineIdentity,
    username: &str,
    password: &str,
    attempt: &str,
    socket: public_adapter::UnixSocketPath,
) -> Result<public_adapter::ObservationReport, QualificationError> {
    let binding = live_binding(online, attempt)?;
    let request = public_adapter::ObservationRequest::new(
        binding.clone(),
        socket,
        public_adapter::ObserverCredentials::new(username, password).map_err(|error| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                "observer credentials",
                error.to_string(),
            )
        })?,
        ObservationProvenance::new("live-mysql-8.4.11", binding.parts().attempt.clone())
            .expect("stable provenance"),
        public_adapter::ClockContext::new(
            SystemClock {
                origin: Instant::now(),
            },
            ObservationInstant::new(fixture.qualification_config().observation_timeout_ms),
        )
        .map_err(|error| {
            QualificationError::new(
                QualificationCode::LaunchFailure,
                "observer clock context",
                error.to_string(),
            )
        })?,
    )
    .map_err(|error| {
        QualificationError::new(
            QualificationCode::AccountStateSetupFailure,
            "build live observation request",
            error.to_string(),
        )
    })?;
    Ok(public_adapter::MysqlObserver::observe(request).await)
}

fn live_binding(
    online: &OnlineIdentity,
    attempt_suffix: &str,
) -> Result<ExactBinding, QualificationError> {
    Ok(ExactBinding::new(ExactBindingParts {
        resource: ResourceId::new("resource").unwrap(),
        partition: PartitionId::new("partition").unwrap(),
        replica: ReplicaId::new("replica").unwrap(),
        incarnation: ReplicaIncarnation::new("incarnation").unwrap(),
        process_session: ProcessSessionId::new("process").unwrap(),
        observation_session: ObservationSessionId::new(format!("observation-{attempt_suffix}"))
            .unwrap(),
        attempt: AttemptId::new(format!("attempt-{attempt_suffix}")).unwrap(),
        endpoint: EndpointBinding::new("endpoint").unwrap(),
        storage: StorageBinding::new("storage").unwrap(),
        server_uuid: ServerUuid::new(&online.server_uuid).map_err(|error| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                "server_uuid binding",
                error.to_string(),
            )
        })?,
        group_name: GroupName::new(&online.group_name).map_err(|error| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                "group_name binding",
                error.to_string(),
            )
        })?,
        member_id: MemberId::new(&online.member_id).map_err(|error| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                "member_id binding",
                error.to_string(),
            )
        })?,
        member_address: MemberAddress::new(&online.member_address).map_err(|error| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                "member_address binding",
                error.to_string(),
            )
        })?,
        configuration: ConfigurationId::new("configuration").unwrap(),
        epoch: Epoch::new("epoch").unwrap(),
        authority_generation: AuthorityGeneration::new("authority").unwrap(),
        view_id: ViewId::new(&online.view_id).map_err(|error| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                "view_id binding",
                error.to_string(),
            )
        })?,
        credential_generation: CredentialGeneration::new("credential").unwrap(),
    }))
}

fn lower_boundary_qualified(boundary: &super::state::GtidBoundaryProbe) -> bool {
    boundary.value == kuberic_mysql_core::MAX_SEQUENCE
        && boundary.accepted
        && boundary.code.is_none()
        && boundary.sql_state.is_none()
}

fn upper_boundary_qualified(boundary: &super::state::GtidBoundaryProbe) -> bool {
    boundary.value == kuberic_mysql_core::MAX_SEQUENCE + 1
        && !boundary.accepted
        && boundary.code == Some(1772)
        && boundary.sql_state.as_deref() == Some("HY000")
}

fn required_inventory() -> Result<BTreeSet<(String, EvidenceOriginKind)>, QualificationError> {
    let mut required = BTreeSet::new();
    for name in [
        "live-product-version",
        "live-empty-gtid-history",
        "live-native-tagged-gtid-functions",
        "live-gtid-boundary-9223372036854775806",
        "live-gtid-boundary-9223372036854775807",
        "live-uds-observer-success",
        "live-strict-uds-only",
        "live-uds-transport-failure",
        "live-authentication-failure",
        "live-members-table-permission-denied",
        "live-stats-table-permission-denied",
        "scripted-group-name-change",
        "scripted-group-replication-address-change",
        "scripted-member-address-change",
        "scripted-member-role-change",
        "scripted-member-state-change",
        "scripted-membership-change",
        "scripted-view-id-change",
        "scripted-read-only-change",
        "scripted-super-read-only-change",
        "scripted-connect-timeout",
        "scripted-query-timeout",
        "scripted-disconnect-timeout",
    ] {
        required.insert((
            name.to_owned(),
            if name.starts_with("live-") {
                EvidenceOriginKind::NativeRequired
            } else {
                EvidenceOriginKind::Synthetic
            },
        ));
    }
    for file in common::load_fixture_files() {
        for case in file.cases {
            let origin = validate_fixture_origin_contract(&case)?;
            match origin {
                EvidenceOriginKind::OracleOwned | EvidenceOriginKind::Synthetic => {
                    required.insert((case.scenario.clone(), origin));
                }
                EvidenceOriginKind::NativeRequired => {}
            }
        }
    }
    Ok(required)
}

fn execute_fixture(case: &FixtureCase) -> crate::AdapterDiagnostic {
    if let Some(error) = &case.error {
        let stage = match error.stage.as_str() {
            "Connect" => crate::ObservationStage::Connect,
            "Authenticate" => crate::ObservationStage::Authenticate,
            "Query" => crate::ObservationStage::Query,
            "Consume" => crate::ObservationStage::Consume,
            "Disconnect" => crate::ObservationStage::Disconnect,
            other => panic!("unknown observation stage {other}"),
        };
        let surface = error.surface.as_deref().map(|surface| match surface {
            "ProductIdentity" => crate::NativeSurface::ProductIdentity,
            "LocalState" => crate::NativeSurface::LocalState,
            "GroupMembers" => crate::NativeSurface::GroupMembers,
            "LocalMemberStats" => crate::NativeSurface::LocalMemberStats,
            "ExecutedGtids" => crate::NativeSurface::ExecutedGtids,
            other => panic!("unknown native surface {other}"),
        });
        let client_error = match error.kind.as_str() {
            "transport" => mysql_async::Error::Io(mysql_async::IoError::Io(std::io::Error::from(
                std::io::ErrorKind::ConnectionRefused,
            ))),
            "server" => mysql_async::Error::Server(mysql_async::ServerError {
                code: error.code.expect("server code"),
                message: "fixture server error".to_owned(),
                state: error.sql_state.clone().expect("server SQLSTATE"),
            }),
            other => panic!("unknown fixture error kind {other}"),
        };
        return crate::session::SessionError::from_client(client_error, stage, surface)
            .diagnostic();
    }

    let result = case.raw();
    let decoded = match case.query() {
        QueryId::Mysql8411ProductIdentityV1 => crate::decode::decode_product(&result).map(|_| None),
        QueryId::Mysql8411LocalStateV1 => crate::decode::decode_local_state(&result).map(|_| None),
        QueryId::Mysql8411GroupMembersV1 => {
            crate::decode::decode_members(&result).map(|evidence| match evidence {
                crate::decode::MembershipEvidence::Absent => {
                    Some(crate::AdapterDiagnostic::Absent {
                        surface: crate::NativeSurface::GroupMembers,
                    })
                }
                crate::decode::MembershipEvidence::Placeholder(kind) => {
                    Some(crate::AdapterDiagnostic::Placeholder(kind))
                }
                crate::decode::MembershipEvidence::Active(_) => None,
            })
        }
        QueryId::Mysql8411LocalMemberStatsV1 => {
            crate::decode::decode_view(&result).map(|evidence| match evidence {
                crate::decode::ViewEvidence::Absent => Some(crate::AdapterDiagnostic::Absent {
                    surface: crate::NativeSurface::LocalMemberStats,
                }),
                crate::decode::ViewEvidence::Placeholder(kind) => {
                    Some(crate::AdapterDiagnostic::Placeholder(kind))
                }
                crate::decode::ViewEvidence::Active { .. } => None,
            })
        }
        QueryId::Mysql8411ExecutedGtidsV1 => {
            crate::decode::decode_executed_gtids(&result).map(|_| None)
        }
    };
    match decoded {
        Ok(Some(diagnostic)) => diagnostic,
        Ok(None) => crate::AdapterDiagnostic::None,
        Err(error) => error.diagnostic,
    }
}

fn core_class_name(class: crate::CoreOutcomeClass) -> &'static str {
    match class {
        crate::CoreOutcomeClass::Valid => "Valid",
        crate::CoreOutcomeClass::Absent => "Absent",
        crate::CoreOutcomeClass::Unreachable => "Unreachable",
        crate::CoreOutcomeClass::AuthenticationFailure => "AuthenticationFailure",
        crate::CoreOutcomeClass::PermissionDenied => "PermissionDenied",
        crate::CoreOutcomeClass::Partial => "Partial",
        crate::CoreOutcomeClass::Stale(_) => "Stale",
        crate::CoreOutcomeClass::FutureDated => "FutureDated",
        crate::CoreOutcomeClass::Malformed(_) => "Malformed",
        crate::CoreOutcomeClass::Unsupported(_) => "Unsupported",
        crate::CoreOutcomeClass::Incoherent(_) => "Incoherent",
    }
}

fn diagnostic_name(diagnostic: &crate::AdapterDiagnostic) -> &'static str {
    match diagnostic {
        crate::AdapterDiagnostic::None => "None",
        crate::AdapterDiagnostic::Transport {
            stage: crate::ObservationStage::Connect,
        } => "TransportConnect",
        crate::AdapterDiagnostic::Server {
            surface: None,
            code: 1045,
            sql_state,
            ..
        } if sql_state.as_str() == "28000" => "Server1045State28000",
        crate::AdapterDiagnostic::Server {
            surface: Some(crate::NativeSurface::GroupMembers),
            code: 1142,
            ..
        } => "MembersServer1142",
        crate::AdapterDiagnostic::Server {
            surface: Some(crate::NativeSurface::LocalMemberStats),
            code: 1142,
            ..
        } => "StatsServer1142",
        crate::AdapterDiagnostic::Product(crate::ProductIssue::UnsupportedPatch) => {
            "UnsupportedPatch"
        }
        crate::AdapterDiagnostic::Product(crate::ProductIssue::NonOracleCommunity) => {
            "NonOracleCommunity"
        }
        crate::AdapterDiagnostic::Schema {
            issue: crate::SchemaIssue::ChangedType { .. },
            ..
        } => "ChangedType",
        crate::AdapterDiagnostic::Schema {
            issue: crate::SchemaIssue::AdditionalColumns { .. },
            ..
        } => "AdditionalColumns",
        crate::AdapterDiagnostic::Evidence {
            issue: crate::EvidenceIssue::RequiredNull { .. },
            ..
        } => "RequiredNull",
        crate::AdapterDiagnostic::Evidence {
            issue: crate::EvidenceIssue::IncompleteRow { .. },
            ..
        } => "IncompleteRow",
        crate::AdapterDiagnostic::Evidence {
            issue: crate::EvidenceIssue::MalformedCell { .. },
            ..
        } => "MalformedCell",
        crate::AdapterDiagnostic::Evidence {
            issue: crate::EvidenceIssue::EmptyRequiredString { .. },
            ..
        } => "EmptyRequiredString",
        crate::AdapterDiagnostic::Evidence {
            issue: crate::EvidenceIssue::UnsupportedNativeValue { .. },
            ..
        } => "UnsupportedNativeValue",
        crate::AdapterDiagnostic::Evidence {
            issue:
                crate::EvidenceIssue::MalformedGtid {
                    kind: kuberic_mysql_core::GtidParseErrorKind::SequenceOutOfRange,
                    ..
                },
            ..
        } => "MalformedGtidSequenceOutOfRange",
        crate::AdapterDiagnostic::Ambiguous {
            kind: crate::AmbiguityKind::DuplicateMemberId,
            ..
        } => "DuplicateMemberId",
        crate::AdapterDiagnostic::Ambiguous {
            kind: crate::AmbiguityKind::DuplicateMemberAddress,
            ..
        } => "DuplicateMemberAddress",
        crate::AdapterDiagnostic::Ambiguous {
            kind: crate::AmbiguityKind::DuplicateLocalRow,
            ..
        } => "DuplicateLocalRow",
        crate::AdapterDiagnostic::Absent {
            surface: crate::NativeSurface::GroupMembers,
        } => "AbsentGroupMembers",
        crate::AdapterDiagnostic::Absent {
            surface: crate::NativeSurface::LocalMemberStats,
        } => "AbsentLocalMemberStats",
        crate::AdapterDiagnostic::Placeholder(crate::PlaceholderKind::NeverStarted) => {
            "PlaceholderNeverStarted"
        }
        crate::AdapterDiagnostic::Placeholder(crate::PlaceholderKind::Stopped) => {
            "PlaceholderStopped"
        }
        other => panic!("fixture diagnostic name missing for {other:?}"),
    }
}

#[derive(Clone, Debug)]
struct SystemClock {
    origin: Instant,
}

impl crate::ObservationClock for SystemClock {
    fn now(&self) -> ObservationInstant {
        ObservationInstant::new(self.origin.elapsed().as_millis() as u64)
    }

    fn to_std_instant(&self, instant: ObservationInstant) -> Result<Instant, crate::ClockError> {
        self.origin
            .checked_add(Duration::from_millis(instant.tick()))
            .ok_or(crate::ClockError::UnrepresentableInstant)
    }
}

impl public_adapter::ObservationClock for SystemClock {
    fn now(&self) -> ObservationInstant {
        ObservationInstant::new(self.origin.elapsed().as_millis() as u64)
    }

    fn to_std_instant(
        &self,
        instant: ObservationInstant,
    ) -> Result<Instant, public_adapter::ClockError> {
        self.origin
            .checked_add(Duration::from_millis(instant.tick()))
            .ok_or(public_adapter::ClockError::UnrepresentableInstant)
    }
}

#[cfg(test)]
mod tests {
    use super::{EvidenceOriginKind, required_inventory, validate_fixture_origin_contract};
    use crate::common;

    #[test]
    fn required_inventory_covers_live_and_fixture_origins() {
        let inventory = required_inventory().unwrap();
        assert!(inventory.contains(&(
            "live-uds-observer-success".to_owned(),
            EvidenceOriginKind::NativeRequired
        )));
        assert!(
            inventory
                .iter()
                .any(|(_, origin)| *origin == EvidenceOriginKind::OracleOwned)
        );
        assert!(
            inventory
                .iter()
                .any(|(_, origin)| *origin == EvidenceOriginKind::Synthetic)
        );
    }

    #[test]
    fn origin_enforcement_rejects_wrong_category() {
        let mut case = common::fixture("stopped-members-placeholder");
        case.evidence_origin.category = "synthetic".to_owned();
        assert!(validate_fixture_origin_contract(&case).is_err());
    }

    #[test]
    fn oracle_inventory_rejects_every_provenance_mutation() {
        for pinned in super::ORACLE_PROVENANCE {
            let case = common::fixture(pinned.scenario);
            for mutate in 0..7 {
                let mut changed = case.clone();
                match mutate {
                    0 => changed.evidence_origin.commit = Some("wrong".to_owned()),
                    1 => changed.evidence_origin.tag = Some("wrong".to_owned()),
                    2 => changed.evidence_origin.url = Some("wrong".to_owned()),
                    3 => changed.evidence_origin.path = Some("wrong".to_owned()),
                    4 => changed.evidence_origin.sha256 = Some("0".repeat(64)),
                    5 => changed.evidence_origin.range = Some("wrong".to_owned()),
                    6 => changed.evidence_origin.derivation = Some("wrong".to_owned()),
                    _ => unreachable!(),
                }
                assert!(
                    validate_fixture_origin_contract(&changed).is_err(),
                    "{} mutation {mutate} was accepted",
                    pinned.scenario
                );
            }
        }
    }
}
