#![allow(dead_code)]

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use mysql_async::prelude::Queryable;

use super::fixture::RunningFixture;
use super::{QualificationCode, QualificationError, mysql_error_detail};

static NEXT_ACCOUNT: AtomicU64 = AtomicU64::new(1);

const OBSERVER_GRANTS: [&str; 2] = [
    "GRANT SELECT ON performance_schema.replication_group_members TO {account}",
    "GRANT SELECT ON performance_schema.replication_group_member_stats TO {account}",
];
const MEMBERS_DENIED_GRANTS: [&str; 1] =
    ["GRANT SELECT ON performance_schema.replication_group_member_stats TO {account}"];
const STATS_DENIED_GRANTS: [&str; 1] =
    ["GRANT SELECT ON performance_schema.replication_group_members TO {account}"];
const RECOVERY_GRANTS: [&str; 1] = ["GRANT REPLICATION SLAVE ON *.* TO {account}"];

#[derive(Clone, Eq, PartialEq)]
pub struct MysqlCredentials {
    username: String,
    password: String,
}

impl MysqlCredentials {
    pub(crate) fn new(role: &str) -> Self {
        let sequence = NEXT_ACCOUNT.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        Self {
            username: format!("q{}_{}_{}", pid, role, sequence),
            password: format!("secret{}_{}_{}", pid, role, sequence),
        }
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn password(&self) -> &str {
        &self.password
    }
}

impl fmt::Debug for MysqlCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MysqlCredentials")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct FixtureAccounts {
    pub(crate) setup: MysqlCredentials,
    pub(crate) observer: MysqlCredentials,
    pub(crate) observer_invalid_password: String,
    pub(crate) members_denied: MysqlCredentials,
    pub(crate) stats_denied: MysqlCredentials,
    pub(crate) recovery: MysqlCredentials,
}

impl FixtureAccounts {
    pub fn setup(&self) -> &MysqlCredentials {
        &self.setup
    }

    pub fn observer(&self) -> &MysqlCredentials {
        &self.observer
    }

    pub fn invalid_observer_password(&self) -> &str {
        &self.observer_invalid_password
    }

    pub fn members_denied(&self) -> &MysqlCredentials {
        &self.members_denied
    }

    pub fn stats_denied(&self) -> &MysqlCredentials {
        &self.stats_denied
    }

    pub fn recovery(&self) -> &MysqlCredentials {
        &self.recovery
    }
}

pub async fn provision_accounts(
    fixture: &RunningFixture,
) -> Result<FixtureAccounts, QualificationError> {
    let setup = MysqlCredentials::new("setup");
    let observer = MysqlCredentials::new("observer");
    let members_denied = MysqlCredentials::new("nomembers");
    let stats_denied = MysqlCredentials::new("nostats");
    let recovery = MysqlCredentials::new("recovery");

    let mut root = fixture.connect_root().await?;
    create_user(&mut root, &setup, "setup bootstrap").await?;
    run_drop(
        &mut root,
        &format!(
            "GRANT ALL PRIVILEGES ON *.* TO {} WITH GRANT OPTION",
            account_target(setup.username())
        ),
        "grant setup privileges",
    )
    .await?;
    let _ = root.disconnect().await;

    let mut setup_conn = fixture
        .connect_as(setup.username(), Some(setup.password()))
        .await?;
    run_drop(
        &mut setup_conn,
        "SET SESSION SQL_LOG_BIN = 0",
        "disable setup binlog",
    )
    .await?;

    create_account_with_grants(&mut setup_conn, &observer, &OBSERVER_GRANTS, "observer").await?;
    create_account_with_grants(
        &mut setup_conn,
        &members_denied,
        &MEMBERS_DENIED_GRANTS,
        "members denied",
    )
    .await?;
    create_account_with_grants(
        &mut setup_conn,
        &stats_denied,
        &STATS_DENIED_GRANTS,
        "stats denied",
    )
    .await?;
    create_account_with_grants(&mut setup_conn, &recovery, &RECOVERY_GRANTS, "recovery").await?;
    let _ = setup_conn.disconnect().await;

    Ok(FixtureAccounts {
        setup,
        observer_invalid_password: format!("{}-wrong", observer.password()),
        observer,
        members_denied,
        stats_denied,
        recovery,
    })
}

async fn create_account_with_grants(
    connection: &mut mysql_async::Conn,
    credentials: &MysqlCredentials,
    grants: &[&str],
    context: &'static str,
) -> Result<(), QualificationError> {
    create_user(connection, credentials, context).await?;
    let target = account_target(credentials.username());
    for grant in grants {
        let statement = grant.replace("{account}", &target);
        run_drop(connection, &statement, context).await?;
    }
    Ok(())
}

async fn create_user(
    connection: &mut mysql_async::Conn,
    credentials: &MysqlCredentials,
    context: &'static str,
) -> Result<(), QualificationError> {
    run_drop(
        connection,
        &format!(
            "CREATE USER {} IDENTIFIED BY {}",
            account_target(credentials.username()),
            sql_literal(credentials.password())
        ),
        context,
    )
    .await
}

async fn run_drop(
    connection: &mut mysql_async::Conn,
    sql: &str,
    context: &'static str,
) -> Result<(), QualificationError> {
    connection.query_drop(sql).await.map_err(|error| {
        QualificationError::new(
            QualificationCode::AccountStateSetupFailure,
            context,
            mysql_error_detail(&error),
        )
    })
}

fn account_target(username: &str) -> String {
    format!("{}@'localhost'", sql_literal(username))
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::{
        MEMBERS_DENIED_GRANTS, MysqlCredentials, OBSERVER_GRANTS, RECOVERY_GRANTS,
        STATS_DENIED_GRANTS, account_target,
    };

    #[test]
    fn generated_accounts_are_ephemeral_and_distinct() {
        let setup = MysqlCredentials::new("setup");
        let observer = MysqlCredentials::new("observer");
        assert_ne!(setup.username(), observer.username());
        assert!(!setup.password().contains('\''));
        assert!(!observer.password().contains('\''));
        assert!(account_target(observer.username()).ends_with("@'localhost'"));
        let debug = format!("{observer:?}");
        assert!(!debug.contains(observer.password()));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn grant_inventory_is_minimal_and_explicit() {
        assert_eq!(OBSERVER_GRANTS.len(), 2);
        assert_eq!(MEMBERS_DENIED_GRANTS.len(), 1);
        assert_eq!(STATS_DENIED_GRANTS.len(), 1);
        assert_eq!(
            RECOVERY_GRANTS,
            ["GRANT REPLICATION SLAVE ON *.* TO {account}"]
        );
        assert!(OBSERVER_GRANTS[0].contains("replication_group_members"));
        assert!(OBSERVER_GRANTS[1].contains("replication_group_member_stats"));
    }
}
