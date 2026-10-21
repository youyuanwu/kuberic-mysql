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
//! The standard repository test gate runs the Oracle MySQL 8.4 LTS live
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

/// Accepts numeric MySQL 8.4 patch versions. Vendor and platform checks are separate.
pub fn is_supported_server_version(version: &str) -> bool {
    version
        .strip_prefix("8.4.")
        .is_some_and(|patch| !patch.is_empty() && patch.bytes().all(|byte| byte.is_ascii_digit()))
}

/// Checks the server-version portion of a Debian package version, not its provenance.
pub fn is_supported_package_version(version: &str) -> bool {
    version.rsplit_once('-').is_some_and(|(server, revision)| {
        is_supported_server_version(server) && !revision.is_empty()
    })
}

/// Finds repository evidence in the requested version's APT policy stanza only.
pub fn apt_repository_for_version<'a>(
    policy: &'a str,
    version: &str,
    host: &str,
    component: &str,
) -> Option<&'a str> {
    fn first_word(line: &str) -> Option<&str> {
        line.trim()
            .trim_start_matches("*** ")
            .split_whitespace()
            .next()
    }
    let mut lines = policy.lines();
    lines.find(|line| first_word(line) == Some(version))?;
    lines
        .take_while(|line| {
            !first_word(line).is_some_and(|word| {
                word.contains('.')
                    && word
                        .bytes()
                        .next()
                        .is_some_and(|byte| byte.is_ascii_digit())
            })
        })
        .map(str::trim)
        .find(|line| {
            let words = line.split_whitespace().collect::<Vec<_>>();
            words.iter().any(|word| {
                word.strip_prefix("https://")
                    .or_else(|| word.strip_prefix("http://"))
                    .and_then(|url| url.split('/').next())
                    == Some(host)
            }) && words.iter().any(|word| {
                word.rsplit_once('/')
                    .is_some_and(|(_, name)| name == component)
            })
        })
}

#[cfg(test)]
mod version_tests {
    use super::{
        apt_repository_for_version, is_supported_package_version, is_supported_server_version,
    };

    #[test]
    fn supported_line_accepts_patch_updates_but_not_other_lines_or_malformed_versions() {
        for version in ["8.4.0", "8.4.11", "8.4.13", "8.4.100"] {
            assert!(is_supported_server_version(version), "{version}");
            assert!(is_supported_package_version(&format!(
                "{version}-1ubuntu24.04"
            )));
        }
        for version in [
            "",
            "8.4",
            "8.4.",
            "8.4.x",
            "8.4.13-debug",
            "8.40.13",
            "8.0.13",
            "9.4.13",
        ] {
            assert!(!is_supported_server_version(version), "{version}");
            assert!(!is_supported_package_version(&format!(
                "{version}-1ubuntu24.04"
            )));
        }
        for version in ["8.4.13", "8.4.13-", "8.4.13.1-1ubuntu24.04"] {
            assert!(!is_supported_package_version(version), "{version}");
        }
    }

    #[test]
    fn newer_candidate_cannot_supply_installed_package_provenance() {
        let policy = "mysql-community-server-core:\n\
          Installed: 8.4.11-1ubuntu24.04\n\
          Candidate: 8.4.13-1ubuntu24.04\n\
          Version table:\n\
             8.4.13-1ubuntu24.04 500\n\
                500 http://repo.mysql.com/apt/ubuntu noble/mysql-8.4-lts amd64 Packages\n\
         *** 8.4.11-1ubuntu24.04 500\n\
                500 http://mirror.example.invalid/ubuntu noble/main amd64 Packages\n";
        let origin = |policy: &str, version| {
            apt_repository_for_version(policy, version, "repo.mysql.com", "mysql-8.4-lts").is_some()
        };
        assert!(origin(policy, "8.4.13-1ubuntu24.04"));
        assert!(!origin(policy, "8.4.11-1ubuntu24.04"));
        assert!(origin(
            &policy.replace(
                "http://mirror.example.invalid/ubuntu noble/main",
                "https://repo.mysql.com/apt/ubuntu noble/mysql-8.4-lts",
            ),
            "8.4.11-1ubuntu24.04",
        ));
        assert!(!origin(
            &policy.replace("repo.mysql.com/", "repo.mysql.com.example.invalid/"),
            "8.4.13-1ubuntu24.04",
        ));
    }
}
