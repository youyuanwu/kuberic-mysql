pub mod accounts;
pub mod fixture;
pub mod scenarios;
pub mod state;

use std::error::Error;
use std::fmt;
use std::path::Path;

use fixture::{QualificationConfig, RunningFixture};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum QualificationCode {
    MissingInput = 6,
    NonAbsoluteInput = 7,
    ToolUnavailable = 8,
    PackageOwnershipMismatch = 9,
    PackageVerificationMismatch = 11,
    UnsupportedVersion = 12,
    IncompatiblePlatform = 13,
    InitializationFailure = 14,
    LaunchFailure = 15,
    AccountStateSetupFailure = 16,
    CleanupFailure = 17,
    UnwritableOutput = 18,
    OriginMismatch = 19,
    ScenarioFailure = 20,
    PackageMetadataMismatch = 21,
    AptProvenanceMismatch = 22,
    ForeignMysqlActive = 23,
    ChildExecutableMismatch = 24,
}

impl QualificationCode {
    pub const fn name(self) -> &'static str {
        match self {
            Self::MissingInput => "MISSING_INPUT",
            Self::NonAbsoluteInput => "NON_ABSOLUTE_INPUT",
            Self::ToolUnavailable => "TOOL_UNAVAILABLE",
            Self::PackageOwnershipMismatch => "PACKAGE_OWNERSHIP_MISMATCH",
            Self::PackageVerificationMismatch => "PACKAGE_VERIFICATION_MISMATCH",
            Self::UnsupportedVersion => "UNSUPPORTED_VERSION",
            Self::IncompatiblePlatform => "INCOMPATIBLE_PLATFORM",
            Self::InitializationFailure => "INITIALIZATION_FAILURE",
            Self::LaunchFailure => "LAUNCH_FAILURE",
            Self::AccountStateSetupFailure => "ACCOUNT_STATE_SETUP_FAILURE",
            Self::CleanupFailure => "CLEANUP_FAILURE",
            Self::UnwritableOutput => "UNWRITABLE_OUTPUT",
            Self::OriginMismatch => "ORIGIN_MISMATCH",
            Self::ScenarioFailure => "SCENARIO_FAILURE",
            Self::PackageMetadataMismatch => "PACKAGE_METADATA_MISMATCH",
            Self::AptProvenanceMismatch => "APT_PROVENANCE_MISMATCH",
            Self::ForeignMysqlActive => "FOREIGN_MYSQL_ACTIVE",
            Self::ChildExecutableMismatch => "CHILD_EXECUTABLE_MISMATCH",
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

pub async fn run() -> Result<(), QualificationError> {
    let config = QualificationConfig::for_repository()?;
    let prepared = fixture::PreparedArtifact::prepare(&config)?;
    let mut running = prepared.launch().await?;

    let execution = execute_qualification(&mut running).await;
    let cleanup_result = running.cleanup();

    match cleanup_result {
        Ok(()) => {}
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
    }
    execution
}

async fn execute_qualification(running: &mut RunningFixture) -> Result<(), QualificationError> {
    let baseline = state::collect_pre_account(running).await?;
    let accounts = accounts::provision_accounts(running).await?;
    let online = state::bring_group_replication_online(running, &accounts).await?;
    scenarios::run_all(running, &accounts, &baseline, &online).await?;
    Ok(())
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
        mysql_async::Error::Io(_) => "transport failure".to_owned(),
        mysql_async::Error::Server(server) => {
            format!("server code={} state={}", server.code, server.state)
        }
        mysql_async::Error::Driver(_) => "driver failure".to_owned(),
        mysql_async::Error::Url(_) => "connection configuration failure".to_owned(),
        mysql_async::Error::Other(_) => "client failure".to_owned(),
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
    use super::{QualificationCode, QualificationError, mysql_error_detail};

    #[test]
    fn display_includes_named_code_and_context() {
        let error = QualificationError::new(
            QualificationCode::PackageVerificationMismatch,
            "package integrity",
            "dpkg verification failed",
        );
        let text = error.to_string();
        assert!(text.contains("PACKAGE_VERIFICATION_MISMATCH"));
        assert!(text.contains("package integrity"));
        assert!(text.contains("dpkg verification failed"));
    }

    #[test]
    fn mysql_errors_never_retain_server_messages_or_secrets() {
        let error = mysql_async::Error::Server(mysql_async::ServerError {
            code: 1045,
            message: "password=hunter2 secret-token".to_owned(),
            state: "28000".to_owned(),
        });
        let detail = mysql_error_detail(&error);
        let qualification =
            QualificationError::new(QualificationCode::AccountStateSetupFailure, "query", detail);
        let rendered = format!("{qualification:?} {qualification}");
        assert!(!rendered.contains("hunter2"));
        assert!(!rendered.contains("secret-token"));
        assert!(rendered.contains("code=1045"));
        assert!(rendered.contains("state=28000"));
    }
}
