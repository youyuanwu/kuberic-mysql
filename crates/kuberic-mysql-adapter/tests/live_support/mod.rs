pub mod accounts;
pub mod fixture;
pub mod record;
pub mod scenarios;
pub mod state;

use std::error::Error;
use std::fmt;
use std::path::Path;

use accounts::FixtureAccounts;
use fixture::{QualificationManifest, RunningFixture};
use scenarios::ScenarioReceipt;
use state::{BaselineEvidence, OnlineIdentity};

pub const MANIFEST_ENV: &str = "KUBERIC_MYSQL_8_4_11_MANIFEST";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum QualificationCode {
    MissingManifestPath = 2,
    ManifestPathNotAbsolute = 3,
    ManifestReadFailure = 4,
    ManifestSyntax = 5,
    MissingInput = 6,
    NonAbsoluteInput = 7,
    ToolUnavailable = 8,
    PackageOwnershipMismatch = 9,
    DigestMismatch = 10,
    PackageVerificationMismatch = 11,
    UnsupportedVersion = 12,
    IncompatiblePlatform = 13,
    InitializationFailure = 14,
    LaunchFailure = 15,
    AccountStateSetupFailure = 16,
    CleanupFailure = 17,
    UnwritableOutput = 18,
    OriginMismatch = 19,
    OutputGated = 20,
    PackageMetadataMismatch = 21,
    AptProvenanceMismatch = 22,
}

impl QualificationCode {
    pub const fn name(self) -> &'static str {
        match self {
            Self::MissingManifestPath => "MISSING_MANIFEST_PATH",
            Self::ManifestPathNotAbsolute => "MANIFEST_PATH_NOT_ABSOLUTE",
            Self::ManifestReadFailure => "MANIFEST_READ_FAILURE",
            Self::ManifestSyntax => "MANIFEST_SYNTAX",
            Self::MissingInput => "MISSING_INPUT",
            Self::NonAbsoluteInput => "NON_ABSOLUTE_INPUT",
            Self::ToolUnavailable => "TOOL_UNAVAILABLE",
            Self::PackageOwnershipMismatch => "PACKAGE_OWNERSHIP_MISMATCH",
            Self::DigestMismatch => "DIGEST_MISMATCH",
            Self::PackageVerificationMismatch => "PACKAGE_VERIFICATION_MISMATCH",
            Self::UnsupportedVersion => "UNSUPPORTED_VERSION",
            Self::IncompatiblePlatform => "INCOMPATIBLE_PLATFORM",
            Self::InitializationFailure => "INITIALIZATION_FAILURE",
            Self::LaunchFailure => "LAUNCH_FAILURE",
            Self::AccountStateSetupFailure => "ACCOUNT_STATE_SETUP_FAILURE",
            Self::CleanupFailure => "CLEANUP_FAILURE",
            Self::UnwritableOutput => "UNWRITABLE_OUTPUT",
            Self::OriginMismatch => "ORIGIN_MISMATCH",
            Self::OutputGated => "OUTPUT_GATED",
            Self::PackageMetadataMismatch => "PACKAGE_METADATA_MISMATCH",
            Self::AptProvenanceMismatch => "APT_PROVENANCE_MISMATCH",
        }
    }

    pub const fn exit_status(self) -> i32 {
        self as i32
    }
}

#[derive(Debug)]
pub struct QualificationError {
    code: QualificationCode,
    context: &'static str,
    detail: String,
}

impl QualificationError {
    pub fn new(code: QualificationCode, context: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            context,
            detail: detail.into(),
        }
    }

    pub fn code(&self) -> QualificationCode {
        self.code
    }

    pub fn context(&self) -> &'static str {
        self.context
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for QualificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}({}): {}: {}",
            self.code.name(),
            self.code.exit_status(),
            self.context,
            self.detail
        )
    }
}

impl Error for QualificationError {}

pub async fn run(manifest_path: &Path) -> Result<(), QualificationError> {
    let manifest = QualificationManifest::load(manifest_path)?;
    let prepared = fixture::PreparedArtifact::prepare(&manifest)?;
    let mut running = prepared.launch().await?;

    let execution = execute_qualification(&mut running).await;
    let setup_receipt = running.setup_receipt().clone();
    let cleanup_result = running.cleanup();

    let cleanup = match cleanup_result {
        Ok(cleanup) => cleanup,
        Err(error) => {
            if let Err(run_error) = execution {
                return Err(QualificationError::new(
                    QualificationCode::CleanupFailure,
                    "cleanup after prior failure",
                    format!("{error}; prior failure: {run_error}"),
                ));
            }
            return Err(error);
        }
    };

    let (baseline, accounts, online, scenarios) = execution?;
    let record = record::QualificationRecord::from_parts(
        &manifest,
        &setup_receipt,
        cleanup,
        &baseline,
        &accounts,
        &online,
        scenarios,
    )?;
    record.write_output(&manifest.output_record_path)?;
    Ok(())
}

async fn execute_qualification(
    running: &mut RunningFixture,
) -> Result<
    (
        BaselineEvidence,
        FixtureAccounts,
        OnlineIdentity,
        ScenarioReceipt,
    ),
    QualificationError,
> {
    let baseline = state::collect_pre_account(running).await?;
    let accounts = accounts::provision_accounts(running).await?;
    let online = state::bring_group_replication_online(running, &accounts).await?;
    let scenarios = scenarios::run_all(running, &accounts, &baseline, &online).await?;
    Ok((baseline, accounts, online, scenarios))
}

#[allow(clippy::std_instead_of_alloc)]
pub(crate) fn project_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate parent")
        .parent()
        .expect("project root")
        .to_path_buf()
}

pub(crate) fn mysql_error_detail(error: &mysql_async::Error) -> String {
    match error {
        mysql_async::Error::Io(inner) => format!("transport {inner:?}"),
        mysql_async::Error::Server(server) => format!(
            "server code={} state={} message={}",
            server.code, server.state, server.message
        ),
        mysql_async::Error::Driver(inner) => format!("driver {inner:?}"),
        mysql_async::Error::Url(inner) => format!("url {inner:?}"),
        mysql_async::Error::Other(inner) => format!("other {inner:?}"),
    }
}

pub(crate) fn absolute_test_path(name: &str) -> std::path::PathBuf {
    project_root()
        .join("qualification")
        .join("tests")
        .join(name)
}

#[cfg(test)]
mod tests {
    use super::{QualificationCode, QualificationError};

    #[test]
    fn display_includes_named_code_and_context() {
        let error = QualificationError::new(
            QualificationCode::DigestMismatch,
            "mysqld digest",
            "expected deadbeef, found cafe",
        );
        let text = error.to_string();
        assert!(text.contains("DIGEST_MISMATCH"));
        assert!(text.contains("mysqld digest"));
        assert!(text.contains("deadbeef"));
    }
}
