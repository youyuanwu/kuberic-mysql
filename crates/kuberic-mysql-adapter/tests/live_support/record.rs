#![allow(dead_code)]

use std::fmt::Write as _;
use std::fs;
use std::path::Path;
use std::process::Command;

use crate::query::QueryId;

use super::accounts::FixtureAccounts;
use super::fixture::{CleanupReceipt, QualificationManifest, SetupReceipt};
use super::scenarios::{EvidenceOriginKind, ORACLE_PROVENANCE, ScenarioReceipt};
use super::state::{BaselineEvidence, OnlineIdentity};
use super::{QualificationCode, QualificationError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QualificationRecord {
    output_path: std::path::PathBuf,
    setup: SetupReceipt,
    cleanup: CleanupReceipt,
    baseline: BaselineEvidence,
    accounts: Vec<super::accounts::AccountReceipt>,
    online: OnlineIdentity,
    scenarios: ScenarioReceipt,
    workspace_rust_version: String,
    pinned_toolchain: String,
    reproducibility: ReproducibilityEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReproducibilityEvidence {
    host_os: String,
    host_arch: String,
    host_release: String,
    mysql_async_version: String,
    mysql_async_features: Vec<String>,
    tokio_version: String,
    tokio_features: Vec<String>,
    cargo_lock_sha256: String,
    source_sha256: String,
    feature_graph_sha256: String,
    validation_results: Vec<String>,
}

impl QualificationRecord {
    pub fn from_parts(
        manifest: &QualificationManifest,
        setup: &SetupReceipt,
        cleanup: CleanupReceipt,
        baseline: &BaselineEvidence,
        accounts: &FixtureAccounts,
        online: &OnlineIdentity,
        scenarios: ScenarioReceipt,
    ) -> Result<Self, QualificationError> {
        if setup.product_version.as_deref() != Some("8.4.11") {
            return Err(QualificationError::new(
                QualificationCode::UnsupportedVersion,
                "record product version",
                format!(
                    "record only accepts 8.4.11, found {:?}",
                    setup.product_version
                ),
            ));
        }
        validate_package_gate(manifest, setup)?;
        validate_cleanup_gate(&cleanup)?;
        scenarios.validate_complete()?;
        validate_gtid_boundaries(baseline)?;
        validate_observer_grants(accounts)?;

        let workspace_rust_version = read_workspace_rust_version()?;
        let pinned_toolchain = read_pinned_toolchain()?;
        let reproducibility = collect_reproducibility(manifest)?;
        validate_reproducibility(&reproducibility)?;
        Ok(Self {
            output_path: manifest.output_record_path.clone(),
            setup: setup.clone(),
            cleanup,
            baseline: baseline.clone(),
            accounts: accounts.receipts().to_vec(),
            online: online.clone(),
            scenarios,
            workspace_rust_version,
            pinned_toolchain,
            reproducibility,
        })
    }

    pub fn write_output(&self, output_path: &Path) -> Result<(), QualificationError> {
        if output_path != self.output_path {
            return Err(QualificationError::new(
                QualificationCode::OutputGated,
                "record output path",
                format!(
                    "record path changed from {} to {}",
                    self.output_path.display(),
                    output_path.display()
                ),
            ));
        }
        let parent = output_path
            .parent()
            .expect("absolute output path has parent");
        fs::create_dir_all(parent).map_err(|error| {
            QualificationError::new(
                QualificationCode::UnwritableOutput,
                "create record parent",
                format!("{}: {error}", parent.display()),
            )
        })?;
        fs::write(output_path, self.render()).map_err(|error| {
            QualificationError::new(
                QualificationCode::UnwritableOutput,
                "write qualification record",
                format!("{}: {error}", output_path.display()),
            )
        })
    }

    fn render(&self) -> String {
        let mut output = String::new();
        line(
            &mut output,
            "record_schema",
            "kuberic.mysql.live-qualification/v1",
        );
        line(
            &mut output,
            "product_version",
            self.setup.product_version.as_deref().unwrap_or(""),
        );
        line(
            &mut output,
            "workspace_rust_version",
            &self.workspace_rust_version,
        );
        line(&mut output, "pinned_toolchain", &self.pinned_toolchain);
        line(
            &mut output,
            "supported_claim",
            "Oracle MySQL Community Server 8.4.11 one-server read-only observation over a private Unix-domain socket only",
        );

        output.push_str("\n[host]\n");
        line(&mut output, "os", &self.reproducibility.host_os);
        line(&mut output, "arch", &self.reproducibility.host_arch);
        line(&mut output, "release", &self.reproducibility.host_release);

        output.push_str("\n[client_graph]\n");
        line(
            &mut output,
            "mysql_async_version",
            &self.reproducibility.mysql_async_version,
        );
        array_line(
            &mut output,
            "mysql_async_features",
            &self.reproducibility.mysql_async_features,
        );
        line(
            &mut output,
            "tokio_version",
            &self.reproducibility.tokio_version,
        );
        array_line(
            &mut output,
            "tokio_features",
            &self.reproducibility.tokio_features,
        );
        line(
            &mut output,
            "cargo_lock_sha256",
            &self.reproducibility.cargo_lock_sha256,
        );
        line(
            &mut output,
            "source_sha256",
            &self.reproducibility.source_sha256,
        );
        line(
            &mut output,
            "feature_graph_sha256",
            &self.reproducibility.feature_graph_sha256,
        );
        array_line(
            &mut output,
            "validation_results",
            &self.reproducibility.validation_results,
        );

        output.push_str("\n[package]\n");
        line(&mut output, "name", &self.setup.package_name);
        line(&mut output, "version", &self.setup.package_version);
        bool_line(
            &mut output,
            "owner_verified",
            self.setup.package_owner_verified,
        );
        bool_line(
            &mut output,
            "files_verified",
            self.setup.package_files_verified,
        );
        line(
            &mut output,
            "apt_installed_version",
            &self.setup.apt_installed_version,
        );
        line(
            &mut output,
            "apt_candidate_version",
            &self.setup.apt_candidate_version,
        );
        line(&mut output, "apt_repository", &self.setup.apt_repository);
        line(
            &mut output,
            "mysqld_path",
            &self.setup.mysqld_path.display().to_string(),
        );
        line(
            &mut output,
            "process_launcher",
            &self.setup.process_launcher.display().to_string(),
        );
        line(
            &mut output,
            "launcher_package",
            &self.setup.launcher_package,
        );
        line(&mut output, "launcher_sha256", &self.setup.launcher_sha256);
        bool_line(
            &mut output,
            "launcher_verified",
            self.setup.launcher_verified,
        );
        line(
            &mut output,
            "child_executable",
            &self.setup.child_executable.display().to_string(),
        );
        bool_line(
            &mut output,
            "child_executable_verified",
            self.setup.child_executable_verified,
        );
        line(&mut output, "mysqld_sha256", &self.setup.mysqld_sha256);

        output.push_str("\n[setup]\n");
        integer_line(&mut output, "process_id", i64::from(self.setup.process_id));
        integer_line(
            &mut output,
            "init_exit_code",
            i64::from(self.setup.init_exit_code),
        );
        line(
            &mut output,
            "socket_path",
            &self.setup.socket_path.display().to_string(),
        );
        integer_line(
            &mut output,
            "socket_device",
            self.setup.socket_device as i64,
        );
        integer_line(&mut output, "socket_inode", self.setup.socket_inode as i64);
        integer_line(
            &mut output,
            "private_directory_mode",
            i64::from(self.setup.private_directory_mode),
        );
        bool_line(
            &mut output,
            "private_directories_verified",
            self.setup.private_directories_verified,
        );
        line(
            &mut output,
            "version_comment",
            self.setup.version_comment.as_deref().unwrap_or(""),
        );
        line(
            &mut output,
            "version_compile_machine",
            self.setup.version_compile_machine.as_deref().unwrap_or(""),
        );
        line(
            &mut output,
            "version_compile_os",
            self.setup.version_compile_os.as_deref().unwrap_or(""),
        );

        output.push_str("\n[cleanup]\n");
        integer_option_line(&mut output, "exit_code", self.cleanup.exit_code);
        if let Some(signal) = self.cleanup.signal {
            line(&mut output, "signal", signal);
        } else {
            line(&mut output, "signal", "");
        }
        bool_line(
            &mut output,
            "work_root_removed",
            self.cleanup.work_root_removed,
        );

        output.push_str("\n[baseline]\n");
        bool_line(
            &mut output,
            "skip_networking",
            self.baseline.skip_networking,
        );
        line(
            &mut output,
            "empty_gtid_history",
            &self.baseline.empty_gtid_history,
        );
        integer_line(
            &mut output,
            "gtid_forms_response",
            match &self.baseline.gtid_forms.response {
                super::state::GtidScalarResponse::Value(value) => i64::from(*value),
                super::state::GtidScalarResponse::MissingRow
                | super::state::GtidScalarResponse::Null => {
                    unreachable!("record eligibility requires scalar response")
                }
            },
        );
        line(
            &mut output,
            "gtid_forms_oracle",
            &self.baseline.gtid_forms.oracle,
        );
        line(
            &mut output,
            "gtid_forms_input",
            &self.baseline.gtid_forms.input,
        );
        integer_option_line(
            &mut output,
            "gtid_forms_code",
            self.baseline.gtid_forms.code.map(i32::from),
        );
        line(
            &mut output,
            "gtid_forms_sql_state",
            self.baseline.gtid_forms.sql_state.as_deref().unwrap_or(""),
        );
        integer_line(
            &mut output,
            "gtid_lower_boundary",
            self.baseline.lower_boundary.value as i64,
        );
        bool_line(
            &mut output,
            "gtid_lower_boundary_accepted",
            self.baseline.lower_boundary.accepted,
        );
        integer_option_line(
            &mut output,
            "gtid_lower_boundary_code",
            self.baseline.lower_boundary.code.map(i32::from),
        );
        line(
            &mut output,
            "gtid_lower_boundary_sql_state",
            self.baseline
                .lower_boundary
                .sql_state
                .as_deref()
                .unwrap_or(""),
        );
        integer_line(
            &mut output,
            "gtid_upper_boundary",
            self.baseline.upper_boundary.value as i64,
        );
        bool_line(
            &mut output,
            "gtid_upper_boundary_accepted",
            self.baseline.upper_boundary.accepted,
        );
        integer_option_line(
            &mut output,
            "gtid_upper_boundary_code",
            self.baseline.upper_boundary.code.map(i32::from),
        );
        line(
            &mut output,
            "gtid_upper_boundary_sql_state",
            self.baseline
                .upper_boundary
                .sql_state
                .as_deref()
                .unwrap_or(""),
        );

        output.push_str("\n[identity]\n");
        line(&mut output, "server_uuid", &self.online.server_uuid);
        line(&mut output, "group_name", &self.online.group_name);
        line(
            &mut output,
            "group_replication_address",
            &self.online.group_replication_address,
        );
        line(&mut output, "member_id", &self.online.member_id);
        line(&mut output, "member_address", &self.online.member_address);
        line(&mut output, "view_id", &self.online.view_id);
        line(&mut output, "member_role", &self.online.member_role);
        line(&mut output, "member_state", &self.online.member_state);
        integer_line(&mut output, "read_only", self.online.read_only);
        integer_line(&mut output, "super_read_only", self.online.super_read_only);

        for query in QueryId::ALL {
            output.push_str("\n[[query]]\n");
            line(&mut output, "id", &format!("{query:?}"));
            line(&mut output, "sql", query.sql());
            array_line(
                &mut output,
                "columns",
                &query
                    .columns()
                    .iter()
                    .map(|column| {
                        format!(
                            "{}:{:?}:nullable={}",
                            column.name(),
                            column.kind(),
                            column.nullable()
                        )
                    })
                    .collect::<Vec<_>>(),
            );
        }

        for account in &self.accounts {
            output.push_str("\n[[grant]]\n");
            line(&mut output, "username", &account.username);
            array_line(&mut output, "grants", &account.grants);
        }

        for scenario in self.scenarios.results() {
            output.push_str("\n[[scenario]]\n");
            line(&mut output, "name", &scenario.name);
            line(&mut output, "origin", origin_name(scenario.origin));
            line(&mut output, "mode", mode_name(scenario.mode));
            bool_line(&mut output, "passed", scenario.passed);
            line(&mut output, "detail", &scenario.detail);
        }

        for provenance in ORACLE_PROVENANCE {
            output.push_str("\n[[oracle_provenance]]\n");
            line(&mut output, "scenario", provenance.scenario);
            line(&mut output, "commit", provenance.commit);
            line(&mut output, "tag", provenance.tag);
            line(&mut output, "url", provenance.url);
            line(&mut output, "path", provenance.path);
            line(&mut output, "sha256", provenance.sha256);
            line(&mut output, "range", provenance.range);
            line(&mut output, "derivation", provenance.derivation);
        }

        output.push_str("\n[non_claims]\n");
        array_line(
            &mut output,
            "items",
            &[
                "no container or Kubernetes qualification".to_owned(),
                "no production lifecycle automation".to_owned(),
                "no broader patch-range support".to_owned(),
                "no fencing, failover, topology control, or TLS claim".to_owned(),
            ],
        );
        output
    }
}

fn validate_package_gate(
    manifest: &QualificationManifest,
    setup: &SetupReceipt,
) -> Result<(), QualificationError> {
    if setup.package_name == manifest.package_name
        && setup.package_version == manifest.package_version
        && setup.apt_installed_version == manifest.package_version
        && setup.apt_candidate_version == manifest.package_version
        && setup.package_owner_verified
        && setup.package_files_verified
        && setup.mysqld_path == manifest.mysqld_path
        && setup.process_launcher == manifest.apparmor_exec_path
        && !setup.launcher_package.is_empty()
        && setup.launcher_sha256.len() == 64
        && setup.launcher_verified
        && setup.child_executable == manifest.mysqld_path
        && setup.child_executable_verified
        && setup.mysqld_sha256 == manifest.expected_mysqld_sha256
        && setup.private_directory_mode & 0o077 == 0
        && setup.private_directories_verified
        && setup.apt_repository.contains(&manifest.apt_repository_host)
        && setup
            .apt_repository
            .contains(&manifest.apt_repository_component)
    {
        Ok(())
    } else {
        Err(QualificationError::new(
            QualificationCode::OutputGated,
            "record package gate",
            "installed package evidence does not match the exact manifest".to_owned(),
        ))
    }
}

fn validate_observer_grants(accounts: &FixtureAccounts) -> Result<(), QualificationError> {
    let receipt = accounts
        .receipts()
        .iter()
        .find(|receipt| receipt.username == accounts.observer().username());
    if receipt.is_some_and(|receipt| {
        receipt.grants.len() == 2
            && receipt
                .grants
                .iter()
                .any(|grant| grant.contains("replication_group_members "))
            && receipt
                .grants
                .iter()
                .any(|grant| grant.contains("replication_group_member_stats "))
    }) {
        Ok(())
    } else {
        Err(QualificationError::new(
            QualificationCode::OutputGated,
            "observer grant inventory",
            "complete least-privilege observer grants are required".to_owned(),
        ))
    }
}

#[rustfmt::skip]
const COMMANDS: [(&str, &str); 4] = [
    ("fmt", "cargo fmt --all -- --check"),
    ("test", "CARGO_BUILD_JOBS=1 cargo test --locked --offline --workspace --all-features -- --test-threads=1"),
    ("clippy", "CARGO_BUILD_JOBS=1 cargo clippy --locked --offline --workspace --all-targets --all-features -- -D warnings"),
    ("feature-graph", "CARGO_BUILD_JOBS=1 cargo tree --locked --offline -e features -p kuberic-mysql-adapter"),
];

fn collect_reproducibility(
    manifest: &QualificationManifest,
) -> Result<ReproducibilityEvidence, QualificationError> {
    let root = super::project_root();
    let lock = root.join("Cargo.lock");
    let runner = root.join("qualification/mysql-uds-observation/run-qualification.sh");
    let cargo_lock_sha256 = runner_digest(&runner, "--lock-digest")?;
    let source_sha256 = runner_digest(&runner, "--source-digest")?;
    let lock_text = fs::read_to_string(lock).map_err(|error| {
        QualificationError::new(
            QualificationCode::OutputGated,
            "Cargo.lock versions",
            error.to_string(),
        )
    })?;
    let mysql_async_version = locked_version(&lock_text, "mysql_async")?;
    let tokio_version = locked_version(&lock_text, "tokio")?;
    let cargo_toml = fs::read_to_string(root.join("crates/kuberic-mysql-adapter/Cargo.toml"))
        .map_err(|error| {
            QualificationError::new(
                QualificationCode::OutputGated,
                "adapter Cargo.toml",
                error.to_string(),
            )
        })?;
    let mysql_async_features = dependency_features(&cargo_toml, "mysql_async")?;
    let tokio_features = dependency_features(&cargo_toml, "tokio")?;
    let graph = fs::read_to_string(&manifest.feature_graph_path).map_err(|error| {
        QualificationError::new(
            QualificationCode::OutputGated,
            "feature graph",
            error.to_string(),
        )
    })?;
    let feature_graph_sha256 = file_sha256(&manifest.feature_graph_path)?;
    validate_feature_graph(
        &graph,
        &mysql_async_version,
        &mysql_async_features,
        &tokio_version,
        &tokio_features,
    )?;
    let mut validation_results = Vec::new();
    for (id, command) in COMMANDS {
        let receipt = parse_receipt(&manifest.validation_receipt_dir.join(format!("{id}.toml")))?;
        validate_receipt(
            &receipt,
            id,
            command,
            &cargo_lock_sha256,
            &source_sha256,
            if id == "feature-graph" {
                Some(&feature_graph_sha256)
            } else {
                None
            },
        )?;
        validation_results.push(format!("{id}: success; command={command}"));
    }
    let release = fs::read_to_string("/etc/os-release")
        .ok()
        .and_then(|text| {
            text.lines()
                .find_map(|line| line.strip_prefix("PRETTY_NAME="))
                .map(|value| value.trim_matches('"').to_owned())
        })
        .unwrap_or_else(|| "unknown".to_owned());
    Ok(ReproducibilityEvidence {
        host_os: std::env::consts::OS.to_owned(),
        host_arch: std::env::consts::ARCH.to_owned(),
        host_release: release,
        mysql_async_version,
        mysql_async_features,
        tokio_version,
        tokio_features,
        cargo_lock_sha256,
        source_sha256,
        feature_graph_sha256,
        validation_results,
    })
}

fn validate_feature_graph(
    graph: &str,
    mysql_version: &str,
    mysql_features: &[String],
    tokio_version: &str,
    tokio_features: &[String],
) -> Result<(), QualificationError> {
    if !graph.contains(&format!("mysql_async v{mysql_version}"))
        || !graph.contains(&format!("tokio v{tokio_version}"))
        || mysql_features
            .iter()
            .any(|feature| !graph.contains(&format!("mysql_async feature \"{feature}\"")))
        || tokio_features
            .iter()
            .any(|feature| !graph.contains(&format!("tokio feature \"{feature}\"")))
    {
        return Err(QualificationError::new(
            QualificationCode::OutputGated,
            "effective feature graph",
            "cargo tree output does not contain manifest and lockfile client claims".to_owned(),
        ));
    }
    Ok(())
}

fn validate_receipt(
    receipt: &std::collections::BTreeMap<String, String>,
    id: &str,
    command: &str,
    lock_digest: &str,
    source_digest: &str,
    output_digest: Option<&String>,
) -> Result<(), QualificationError> {
    if receipt.get("schema").map(String::as_str) == Some("kuberic.mysql.validation-receipt/v1")
        && receipt.get("command_id").map(String::as_str) == Some(id)
        && receipt.get("command").map(String::as_str) == Some(command)
        && receipt.get("success").map(String::as_str) == Some("true")
        && receipt.get("lock_sha256").map(String::as_str) == Some(lock_digest)
        && receipt.get("source_sha256").map(String::as_str) == Some(source_digest)
        && output_digest.is_none_or(|digest| receipt.get("output_sha256") == Some(digest))
    {
        Ok(())
    } else {
        Err(QualificationError::new(
            QualificationCode::OutputGated,
            "validation receipt",
            format!("invalid, failed, altered, or stale receipt {id}"),
        ))
    }
}

#[rustfmt::skip]
fn runner_digest(runner: &Path, argument: &str) -> Result<String, QualificationError> {
    let output = Command::new(runner).arg(argument).output().map_err(|error| {
        QualificationError::new(QualificationCode::OutputGated, "qualification digest", error.to_string())
    })?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    } else {
        Err(QualificationError::new(
            QualificationCode::OutputGated,
            "qualification digest",
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ))
    }
}

#[rustfmt::skip]
fn file_sha256(path: &Path) -> Result<String, QualificationError> {
    let output = Command::new("sha256sum").arg(path).output().map_err(|error| {
        QualificationError::new(QualificationCode::OutputGated, "file digest", error.to_string())
    })?;
    Ok(String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase())
}

#[rustfmt::skip]
fn dependency_features(toml: &str, dependency: &str) -> Result<Vec<String>, QualificationError> {
    let needle = format!("{dependency} = ");
    let mut features = Vec::new();
    for line in toml.lines().filter(|line| line.trim_start().starts_with(&needle)) {
        if let Some(list) = line.split("features = [").nth(1).and_then(|rest| rest.split(']').next()) {
            for feature in list.split(',') {
                let feature = feature.trim().trim_matches('"');
                if !feature.is_empty() && !features.iter().any(|known| known == feature) {
                    features.push(feature.to_owned());
                }
            }
        }
    }
    features.sort();
    if features.is_empty() {
        Err(QualificationError::new(
            QualificationCode::OutputGated,
            "manifest dependency features",
            format!("missing selected features for {dependency}"),
        ))
    } else {
        Ok(features)
    }
}

#[rustfmt::skip]
fn parse_receipt(path: &Path) -> Result<std::collections::BTreeMap<String, String>, QualificationError> {
    let text = fs::read_to_string(path).map_err(|error| {
        QualificationError::new(QualificationCode::OutputGated, "validation receipt", format!("{}: {error}", path.display()))
    })?;
    let mut values = std::collections::BTreeMap::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let (key, value) = line.split_once('=').ok_or_else(|| {
            QualificationError::new(QualificationCode::OutputGated, "validation receipt", "malformed receipt")
        })?;
        values.insert(key.trim().to_owned(), value.trim().trim_matches('"').to_owned());
    }
    Ok(values)
}

fn locked_version(lock: &str, package: &str) -> Result<String, QualificationError> {
    let marker = format!("name = \"{package}\"");
    let section = lock
        .split("[[package]]")
        .find(|section| section.contains(&marker));
    section
        .and_then(|section| {
            section
                .lines()
                .find_map(|line| line.trim().strip_prefix("version = \""))
        })
        .and_then(|version| version.strip_suffix('"'))
        .map(str::to_owned)
        .ok_or_else(|| {
            QualificationError::new(
                QualificationCode::OutputGated,
                "Cargo.lock package version",
                format!("missing {package}"),
            )
        })
}

fn validate_reproducibility(evidence: &ReproducibilityEvidence) -> Result<(), QualificationError> {
    if evidence.host_os.is_empty()
        || evidence.host_arch.is_empty()
        || evidence.host_release.is_empty()
        || evidence.mysql_async_version != "0.37.1"
        || evidence.mysql_async_features != ["minimal-rust"]
        || evidence.tokio_version.is_empty()
        || evidence.tokio_features.is_empty()
        || evidence.cargo_lock_sha256.len() != 64
        || evidence.source_sha256.len() != 64
        || evidence.feature_graph_sha256.len() != 64
        || evidence.validation_results.len() != 4
        || QueryId::ALL.iter().any(|query| query.columns().is_empty())
        || ORACLE_PROVENANCE.len() != 5
    {
        Err(QualificationError::new(
            QualificationCode::OutputGated,
            "reproducibility evidence",
            "required client, lockfile, query, validation, or provenance evidence is missing"
                .to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn validate_cleanup_gate(cleanup: &CleanupReceipt) -> Result<(), QualificationError> {
    if cleanup.work_root_removed {
        Ok(())
    } else {
        Err(QualificationError::new(
            QualificationCode::OutputGated,
            "record cleanup gate",
            "work root must be removed before a record is eligible".to_owned(),
        ))
    }
}

fn validate_gtid_boundaries(baseline: &BaselineEvidence) -> Result<(), QualificationError> {
    if baseline.gtid_forms.oracle != "GTID_SUBSET"
        || !baseline.gtid_forms.response.is_exact_one()
        || baseline.gtid_forms.code.is_some()
        || baseline.gtid_forms.sql_state.is_some()
        || !baseline.gtid_forms.input.contains('\n')
        || !baseline.gtid_forms.input.contains(":tag_live:")
        || baseline.gtid_forms.input.matches(',').count() < 2
    {
        return Err(QualificationError::new(
            QualificationCode::OutputGated,
            "GTID forms evidence",
            "tagged, newline, multi-source GTID_SUBSET evidence is incomplete".to_owned(),
        ));
    }
    if baseline.lower_boundary.value != kuberic_mysql_core::MAX_SEQUENCE
        || !baseline.lower_boundary.accepted
        || baseline.lower_boundary.code.is_some()
        || baseline.lower_boundary.sql_state.is_some()
    {
        return Err(QualificationError::new(
            QualificationCode::OutputGated,
            "GTID lower boundary",
            "9223372036854775806 must be recorded as accepted before a record is eligible"
                .to_owned(),
        ));
    }
    if baseline.upper_boundary.value == kuberic_mysql_core::MAX_SEQUENCE + 1
        && !baseline.upper_boundary.accepted
        && baseline.upper_boundary.code == Some(1772)
        && baseline.upper_boundary.sql_state.as_deref() == Some("HY000")
    {
        return Ok(());
    }
    Err(QualificationError::new(
        QualificationCode::OutputGated,
        "GTID upper boundary",
        "9223372036854775807 must be rejected with MySQL 1772/HY000; any other result requires specification review".to_owned(),
    ))
}

fn line(output: &mut String, key: &str, value: &str) {
    let _ = writeln!(output, "{} = \"{}\"", key, escape(value));
}

fn bool_line(output: &mut String, key: &str, value: bool) {
    let _ = writeln!(output, "{key} = {value}");
}

fn integer_line(output: &mut String, key: &str, value: i64) {
    let _ = writeln!(output, "{key} = {value}");
}

fn integer_option_line(output: &mut String, key: &str, value: Option<i32>) {
    match value {
        Some(value) => {
            let _ = writeln!(output, "{key} = {value}");
        }
        None => {
            let _ = writeln!(output, "{key} = \"\"");
        }
    }
}

fn array_line(output: &mut String, key: &str, values: &[String]) {
    let escaped = values
        .iter()
        .map(|value| format!("\"{}\"", escape(value)))
        .collect::<Vec<_>>()
        .join(", ");
    let _ = writeln!(output, "{key} = [{escaped}]");
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn origin_name(origin: EvidenceOriginKind) -> &'static str {
    match origin {
        EvidenceOriginKind::NativeRequired => "native-required",
        EvidenceOriginKind::OracleOwned => "oracle-owned",
        EvidenceOriginKind::Synthetic => "synthetic",
    }
}

fn mode_name(mode: super::scenarios::ScenarioMode) -> &'static str {
    match mode {
        super::scenarios::ScenarioMode::DirectSetup => "direct-setup",
        super::scenarios::ScenarioMode::PublicObserver => "public-observer",
        super::scenarios::ScenarioMode::FixtureDecode => "fixture-decode",
        super::scenarios::ScenarioMode::ScriptedObserver => "scripted-observer",
    }
}

fn read_workspace_rust_version() -> Result<String, QualificationError> {
    let cargo = fs::read_to_string(super::project_root().join("Cargo.toml")).map_err(|error| {
        QualificationError::new(
            QualificationCode::OutputGated,
            "read workspace Cargo.toml",
            error.to_string(),
        )
    })?;
    cargo
        .lines()
        .find_map(|line| line.trim().strip_prefix("rust-version = "))
        .map(|line| line.trim_matches('"').to_owned())
        .ok_or_else(|| {
            QualificationError::new(
                QualificationCode::OutputGated,
                "workspace rust-version",
                "missing rust-version in Cargo.toml".to_owned(),
            )
        })
}

fn read_pinned_toolchain() -> Result<String, QualificationError> {
    let toolchain =
        fs::read_to_string(super::project_root().join("rust-toolchain.toml")).map_err(|error| {
            QualificationError::new(
                QualificationCode::OutputGated,
                "read rust-toolchain.toml",
                error.to_string(),
            )
        })?;
    toolchain
        .lines()
        .find_map(|line| line.trim().strip_prefix("channel = "))
        .map(|line| line.trim_matches('"').to_owned())
        .ok_or_else(|| {
            QualificationError::new(
                QualificationCode::OutputGated,
                "pinned toolchain",
                "missing channel in rust-toolchain.toml".to_owned(),
            )
        })
}

#[cfg(test)]
#[rustfmt::skip]
mod tests {
    use super::super::accounts::{AccountReceipt, FixtureAccounts, MysqlCredentials};
    use super::super::fixture::{CleanupReceipt, QualificationManifest, SetupReceipt};
    use super::super::scenarios::{EvidenceOriginKind, ScenarioMode};
    use super::super::scenarios::{ScenarioReceipt, ScenarioResult};
    use super::super::state::{
        BaselineEvidence, GtidBoundaryProbe, GtidFunctionProbe, GtidScalarResponse, OnlineIdentity,
        ProductFields,
    };
    use super::{
        QualificationRecord, ReproducibilityEvidence, validate_cleanup_gate, validate_package_gate,
        validate_feature_graph, validate_gtid_boundaries, validate_receipt,
        validate_reproducibility,
    };

    fn baseline() -> BaselineEvidence {
        BaselineEvidence {
            product: ProductFields {
                version: "8.4.11".to_owned(),
                version_comment: "MySQL Community Server - GPL".to_owned(),
                version_compile_machine: "x86_64".to_owned(),
                version_compile_os: "Linux".to_owned(),
                server_uuid: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_owned(),
            },
            empty_gtid_history: String::new(),
            skip_networking: true,
            gtid_forms: GtidFunctionProbe {
                oracle: "GTID_SUBSET".to_owned(),
                input: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-3,\naaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:tag_live:4-5,\nbbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:7-9".to_owned(),
                response: GtidScalarResponse::Value(1),
                code: None,
                sql_state: None,
            },
            lower_boundary: GtidBoundaryProbe {
                value: 9223372036854775806,
                accepted: true,
                code: None,
                sql_state: None,
            },
            upper_boundary: GtidBoundaryProbe {
                value: 9223372036854775807,
                accepted: false,
                code: Some(1772),
                sql_state: Some("HY000".to_owned()),
            },
        }
    }

    #[test]
    fn wrong_version_record_is_rejected() {
        let manifest = QualificationManifest {
            manifest_path: super::super::project_root().join("qualification/tests/manifest.toml"),
            mysqld_path: "/usr/sbin/mysqld".into(),
            apparmor_exec_path: "/usr/bin/aa-exec".into(),
            package_name: "mysql-community-server-core".to_owned(),
            package_version: "8.4.11-1ubuntu24.04".to_owned(),
            apt_repository_host: "repo.mysql.com".to_owned(),
            apt_repository_component: "mysql-8.4-lts".to_owned(),
            expected_mysqld_sha256: "C".repeat(64),
            qualification_root: super::super::project_root().join("qualification/tests/run"),
            output_record_path: super::super::project_root()
                .join("qualification/tests/run/out.toml"),
            validation_receipt_dir: super::super::project_root()
                .join("qualification/tests/receipts"),
            feature_graph_path: super::super::project_root()
                .join("qualification/tests/receipts/feature-graph.txt"),
            startup_timeout_ms: 1,
            observation_timeout_ms: 1,
            cleanup_timeout_ms: 1,
        };
        let setup = SetupReceipt {
            package_name: "mysql-community-server-core".to_owned(),
            package_version: "8.4.11-1ubuntu24.04".to_owned(),
            package_owner_verified: true,
            package_files_verified: true,
            apt_installed_version: "8.4.11-1ubuntu24.04".to_owned(),
            apt_candidate_version: "8.4.11-1ubuntu24.04".to_owned(),
            apt_repository:
                "500 http://repo.mysql.com/apt/ubuntu noble/mysql-8.4-lts amd64 Packages".to_owned(),
            mysqld_sha256: "C".repeat(64),
            mysqld_path: "/usr/sbin/mysqld".into(),
            process_launcher: "/usr/bin/aa-exec".into(),
            launcher_package: "apparmor-utils".to_owned(),
            launcher_sha256: "D".repeat(64),
            launcher_verified: true,
            child_executable: "/usr/sbin/mysqld".into(),
            child_executable_verified: true,
            init_exit_code: 0,
            process_id: 1,
            socket_path: "/socket".into(),
            socket_device: 1,
            socket_inode: 1,
            private_directory_mode: 0o700,
            private_directories_verified: true,
            product_version: Some("8.4.12".to_owned()),
            version_comment: Some("MySQL Community Server - GPL".to_owned()),
            version_compile_machine: Some("x86_64".to_owned()),
            version_compile_os: Some("Linux".to_owned()),
        };
        let mut wrong_package = setup.clone();
        wrong_package.package_version = "8.4.12-1ubuntu24.04".to_owned();
        assert!(validate_package_gate(&manifest, &wrong_package).is_err());
        let mut wrong_child = setup.clone();
        wrong_child.child_executable = "/usr/bin/aa-exec".into();
        assert!(validate_package_gate(&manifest, &wrong_child).is_err());
        let mut unverified_launcher = setup.clone();
        unverified_launcher.launcher_verified = false;
        assert!(validate_package_gate(&manifest, &unverified_launcher).is_err());
        let cleanup = CleanupReceipt {
            exit_code: Some(0),
            signal: None,
            removed_paths: Vec::new(),
            work_root_removed: true,
        };
        let accounts = FixtureAccounts {
            setup: MysqlCredentials::new("setup"),
            observer: MysqlCredentials::new("observer"),
            observer_invalid_password: "wrong".to_owned(),
            members_denied: MysqlCredentials::new("members"),
            stats_denied: MysqlCredentials::new("stats"),
            recovery: MysqlCredentials::new("recovery"),
            receipts: vec![AccountReceipt {
                username: "observer".to_owned(),
                grants: vec!["grant".to_owned()],
            }],
        };
        let online = OnlineIdentity {
            server_uuid: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_owned(),
            group_name: "cccccccc-cccc-cccc-cccc-cccccccccccc".to_owned(),
            group_replication_address: "127.0.0.1:33061".to_owned(),
            member_id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_owned(),
            member_address: "127.0.0.1:33060".to_owned(),
            view_id: "view-0001".to_owned(),
            member_role: "PRIMARY".to_owned(),
            member_state: "ONLINE".to_owned(),
            read_only: 1,
            super_read_only: 1,
        };
        let scenarios = ScenarioReceipt::new(vec![ScenarioResult {
            name: "live-product-version".to_owned(),
            origin: EvidenceOriginKind::NativeRequired,
            mode: ScenarioMode::DirectSetup,
            passed: true,
            detail: "ok".to_owned(),
        }]);
        assert!(
            QualificationRecord::from_parts(
                &manifest,
                &setup,
                cleanup,
                &baseline(),
                &accounts,
                &online,
                scenarios
            )
            .is_err()
        );
    }

    #[test]
    fn output_is_gated_until_cleanup_and_complete_scenarios() {
        let cleanup = CleanupReceipt {
            exit_code: Some(0),
            signal: None,
            removed_paths: Vec::new(),
            work_root_removed: false,
        };
        let error = validate_cleanup_gate(&cleanup).unwrap_err();
        assert_eq!(error.code(), super::super::QualificationCode::OutputGated);
        assert!(error.to_string().contains("record cleanup gate"));
    }

    #[test]
    fn reproducibility_gate_rejects_missing_client_and_lock_evidence() {
        let evidence = ReproducibilityEvidence {
            host_os: "linux".to_owned(),
            host_arch: "x86_64".to_owned(),
            host_release: "Ubuntu 24.04".to_owned(),
            mysql_async_version: String::new(),
            mysql_async_features: Vec::new(),
            tokio_version: String::new(),
            tokio_features: Vec::new(),
            cargo_lock_sha256: String::new(),
            source_sha256: String::new(),
            feature_graph_sha256: String::new(),
            validation_results: Vec::new(),
        };
        assert!(validate_reproducibility(&evidence).is_err());
    }

    #[test]
    fn validation_receipts_reject_failure_wrong_command_and_stale_digests() {
        let mut receipt = std::collections::BTreeMap::from([
            ("schema".to_owned(), "kuberic.mysql.validation-receipt/v1".to_owned()),
            ("command_id".to_owned(), "fmt".to_owned()),
            ("command".to_owned(), "cargo fmt --all -- --check".to_owned()),
            ("success".to_owned(), "true".to_owned()),
            ("lock_sha256".to_owned(), "A".repeat(64)),
            ("source_sha256".to_owned(), "B".repeat(64)),
            ("output_sha256".to_owned(), String::new()),
        ]);
        assert!(validate_receipt(&receipt, "fmt", "cargo fmt --all -- --check", &"A".repeat(64), &"B".repeat(64), None).is_ok());
        for (field, value) in [
            ("success", "false"),
            ("command", "wrong"),
            ("lock_sha256", "stale"),
            ("source_sha256", "stale"),
        ] {
            let original = receipt.insert(field.to_owned(), value.to_owned()).unwrap();
            assert!(validate_receipt(&receipt, "fmt", "cargo fmt --all -- --check", &"A".repeat(64), &"B".repeat(64), None).is_err());
            receipt.insert(field.to_owned(), original);
        }
    }

    #[test]
    fn feature_graph_rejects_altered_graph_and_manifest_feature_mismatch() {
        let graph = "mysql_async feature \"minimal-rust\"\nmysql_async v0.37.1\ntokio feature \"time\"\ntokio v1.53.2\n";
        assert!(validate_feature_graph(graph, "0.37.1", &["minimal-rust".to_owned()], "1.53.2", &["time".to_owned()]).is_ok());
        assert!(validate_feature_graph("altered", "0.37.1", &["minimal-rust".to_owned()], "1.53.2", &["time".to_owned()]).is_err());
        assert!(validate_feature_graph(graph, "0.37.1", &["not-selected".to_owned()], "1.53.2", &["time".to_owned()]).is_err());
    }

    #[test]
    fn gtid_eligibility_requires_exact_maximum_rejection_oracle() {
        let qualified = baseline();
        assert!(validate_gtid_boundaries(&qualified).is_ok());
        for mutate in 0..4 {
            let mut invalid = qualified.clone();
            match mutate {
                0 => invalid.upper_boundary.accepted = true,
                1 => invalid.upper_boundary.code = Some(1234),
                2 => invalid.upper_boundary.sql_state = Some("42000".to_owned()),
                3 => invalid.lower_boundary.accepted = false,
                _ => unreachable!(),
            }
            assert!(validate_gtid_boundaries(&invalid).is_err());
        }
        for response in [
            GtidScalarResponse::MissingRow,
            GtidScalarResponse::Null,
            GtidScalarResponse::Value(0),
            GtidScalarResponse::Value(2),
        ] {
            let mut invalid = qualified.clone();
            invalid.gtid_forms.response = response;
            assert!(validate_gtid_boundaries(&invalid).is_err());
        }
    }
}
