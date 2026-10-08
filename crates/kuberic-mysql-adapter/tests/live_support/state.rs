#![allow(dead_code)]

use mysql_async::prelude::Queryable;

use super::accounts::FixtureAccounts;
use super::fixture::RunningFixture;
use super::{QualificationCode, QualificationError, mysql_error_detail};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProductFields {
    pub version: String,
    pub version_comment: String,
    pub version_compile_machine: String,
    pub version_compile_os: String,
    pub server_uuid: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GtidBoundaryProbe {
    pub value: u64,
    pub accepted: bool,
    pub code: Option<u16>,
    pub sql_state: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GtidFunctionProbe {
    pub input: String,
    pub accepted: bool,
    pub code: Option<u16>,
    pub sql_state: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaselineEvidence {
    pub product: ProductFields,
    pub empty_gtid_history: String,
    pub skip_networking: bool,
    pub gtid_forms: GtidFunctionProbe,
    pub lower_boundary: GtidBoundaryProbe,
    pub upper_boundary: GtidBoundaryProbe,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OnlineIdentity {
    pub server_uuid: String,
    pub group_name: String,
    pub group_replication_address: String,
    pub member_id: String,
    pub member_address: String,
    pub view_id: String,
    pub member_role: String,
    pub member_state: String,
    pub read_only: i64,
    pub super_read_only: i64,
}

pub async fn collect_pre_account(
    fixture: &mut RunningFixture,
) -> Result<BaselineEvidence, QualificationError> {
    let mut connection = fixture.connect_root().await?;
    let product = query_product(&mut connection).await?;
    validate_product(&product)?;
    fixture.note_product_identity(
        &product.version,
        &product.version_comment,
        &product.version_compile_machine,
        &product.version_compile_os,
    );
    let empty_gtid_history = query_string(
        &mut connection,
        "SELECT @@GLOBAL.gtid_executed AS gtid_executed",
        "read empty GTID history",
    )
    .await?;
    let skip_networking = query_integer(
        &mut connection,
        "SELECT @@GLOBAL.skip_networking AS skip_networking",
        "read skip_networking",
    )
    .await?
        == 1;
    let gtid_forms = probe_gtid_function(&mut connection, &product.server_uuid).await?;
    let lower_boundary =
        probe_gtid_boundary(&mut connection, &product.server_uuid, 9223372036854775806).await?;
    let upper_boundary =
        probe_gtid_boundary(&mut connection, &product.server_uuid, 9223372036854775807).await?;
    let _ = connection.disconnect().await;

    Ok(BaselineEvidence {
        product,
        empty_gtid_history,
        skip_networking,
        gtid_forms,
        lower_boundary,
        upper_boundary,
    })
}

pub async fn bring_group_replication_online(
    fixture: &RunningFixture,
    accounts: &FixtureAccounts,
) -> Result<OnlineIdentity, QualificationError> {
    let mut connection = fixture
        .connect_as(
            accounts.setup().username(),
            Some(accounts.setup().password()),
        )
        .await?;
    ensure_group_replication_plugin(&mut connection).await?;
    for statement in bootstrap_group_replication_statements(accounts) {
        run_drop(&mut connection, &statement, "bootstrap group replication").await?;
    }

    let deadline = std::time::Instant::now() + fixture.manifest().startup_timeout();
    loop {
        let identity = query_online_identity(&mut connection).await?;
        if identity.member_state == "ONLINE" && !identity.view_id.is_empty() {
            let _ = connection.disconnect().await;
            return Ok(identity);
        }
        if std::time::Instant::now() >= deadline {
            return Err(QualificationError::new(
                QualificationCode::LaunchFailure,
                "group replication readiness",
                "member never reached ONLINE".to_owned(),
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

fn validate_product(product: &ProductFields) -> Result<(), QualificationError> {
    if product.version != "8.4.11" || product.version_comment != "MySQL Community Server - GPL" {
        return Err(QualificationError::new(
            QualificationCode::UnsupportedVersion,
            "Oracle MySQL product",
            format!(
                "expected Oracle MySQL Community 8.4.11, found {} / {}",
                product.version, product.version_comment
            ),
        ));
    }
    if product.version_compile_machine != "x86_64" || product.version_compile_os != "Linux" {
        return Err(QualificationError::new(
            QualificationCode::IncompatiblePlatform,
            "Oracle MySQL platform",
            format!(
                "expected Linux/x86_64, found {}/{}",
                product.version_compile_os, product.version_compile_machine
            ),
        ));
    }
    Ok(())
}

async fn ensure_group_replication_plugin(
    connection: &mut mysql_async::Conn,
) -> Result<(), QualificationError> {
    let installed = connection
        .query_first::<Option<String>, _>(
            "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS \
             WHERE PLUGIN_NAME = 'group_replication'",
        )
        .await
        .map_err(|error| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                "query group replication plugin",
                mysql_error_detail(&error),
            )
        })?
        .flatten();
    if installed.is_none() {
        run_drop(
            connection,
            "INSTALL PLUGIN group_replication SONAME 'group_replication.so'",
            "install group replication plugin",
        )
        .await?;
    }
    Ok(())
}

fn bootstrap_group_replication_statements(accounts: &FixtureAccounts) -> Vec<String> {
    vec![
        format!(
            "CHANGE REPLICATION SOURCE TO \
             SOURCE_USER = {}, \
             SOURCE_PASSWORD = {} \
             FOR CHANNEL 'group_replication_recovery'",
            sql_literal(accounts.recovery().username()),
            sql_literal(accounts.recovery().password())
        ),
        "SET GLOBAL group_replication_bootstrap_group = ON".to_owned(),
        "START GROUP_REPLICATION".to_owned(),
        "SET GLOBAL group_replication_bootstrap_group = OFF".to_owned(),
    ]
}

async fn query_product(
    connection: &mut mysql_async::Conn,
) -> Result<ProductFields, QualificationError> {
    let row = connection
        .query_first::<(
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ), _>(crate::query::QueryId::Mysql8411ProductIdentityV1.sql())
        .await
        .map_err(|error| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                "query product identity",
                mysql_error_detail(&error),
            )
        })?
        .ok_or_else(|| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                "query product identity",
                "no product row returned".to_owned(),
            )
        })?;
    Ok(ProductFields {
        version: required_field(row.0, "version")?,
        version_comment: required_field(row.1, "version_comment")?,
        version_compile_machine: required_field(row.2, "version_compile_machine")?,
        version_compile_os: required_field(row.3, "version_compile_os")?,
        server_uuid: required_field(row.4, "server_uuid")?,
    })
}

async fn query_online_identity(
    connection: &mut mysql_async::Conn,
) -> Result<OnlineIdentity, QualificationError> {
    let local = connection
        .query_first::<(Option<String>, Option<String>, Option<i64>, Option<i64>), _>(
            crate::query::QueryId::Mysql8411LocalStateV1.sql(),
        )
        .await
        .map_err(|error| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                "query local state",
                mysql_error_detail(&error),
            )
        })?
        .ok_or_else(|| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                "query local state",
                "no local state row returned".to_owned(),
            )
        })?;
    let member = connection
        .query_first::<(String, String, Option<i64>, String, String), _>(
            crate::query::QueryId::Mysql8411GroupMembersV1.sql(),
        )
        .await
        .map_err(|error| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                "query group members",
                mysql_error_detail(&error),
            )
        })?
        .ok_or_else(|| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                "query group members",
                "no group member row returned".to_owned(),
            )
        })?;
    let view = connection
        .query_first::<(String, String), _>(
            crate::query::QueryId::Mysql8411LocalMemberStatsV1.sql(),
        )
        .await
        .map_err(|error| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                "query local member view",
                mysql_error_detail(&error),
            )
        })?
        .ok_or_else(|| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                "query local member view",
                "no local member view row returned".to_owned(),
            )
        })?;
    Ok(OnlineIdentity {
        server_uuid: view.0.clone(),
        group_name: required_field(local.0, "group_name")?,
        group_replication_address: required_field(local.1, "group_replication_address")?,
        member_id: view.0,
        member_address: format!("{}:{}", member.1, member.2.unwrap_or_default()),
        view_id: view.1,
        member_role: member.4,
        member_state: member.3,
        read_only: local.2.unwrap_or_default(),
        super_read_only: local.3.unwrap_or_default(),
    })
}

async fn query_string(
    connection: &mut mysql_async::Conn,
    sql: &str,
    context: &'static str,
) -> Result<String, QualificationError> {
    connection
        .query_first::<Option<String>, _>(sql)
        .await
        .map_err(|error| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                context,
                mysql_error_detail(&error),
            )
        })?
        .ok_or_else(|| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                context,
                "query returned no row".to_owned(),
            )
        })
        .and_then(|value| {
            value.ok_or_else(|| {
                QualificationError::new(
                    QualificationCode::AccountStateSetupFailure,
                    context,
                    "query returned SQL NULL".to_owned(),
                )
            })
        })
}

async fn query_integer(
    connection: &mut mysql_async::Conn,
    sql: &str,
    context: &'static str,
) -> Result<i64, QualificationError> {
    connection
        .query_first::<Option<Option<i64>>, _>(sql)
        .await
        .map_err(|error| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                context,
                mysql_error_detail(&error),
            )
        })?
        .flatten()
        .flatten()
        .ok_or_else(|| {
            QualificationError::new(
                QualificationCode::AccountStateSetupFailure,
                context,
                "query returned no integer".to_owned(),
            )
        })
}

async fn probe_gtid_function(
    connection: &mut mysql_async::Conn,
    server_uuid: &str,
) -> Result<GtidFunctionProbe, QualificationError> {
    let input = format!(
        "{server_uuid}:1-3,\n{server_uuid}:tag_live:4-5,\n\
bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:7-9"
    );
    let sql = format!(
        "SELECT GTID_SUBSET({}, {})",
        sql_literal(&input),
        sql_literal(&input)
    );
    match connection.query_first::<Option<Option<u8>>, _>(&sql).await {
        Ok(_) => Ok(GtidFunctionProbe {
            input,
            accepted: true,
            code: None,
            sql_state: None,
        }),
        Err(mysql_async::Error::Server(server)) => Ok(GtidFunctionProbe {
            input,
            accepted: false,
            code: Some(server.code),
            sql_state: Some(server.state),
        }),
        Err(error) => Err(QualificationError::new(
            QualificationCode::AccountStateSetupFailure,
            "probe native GTID forms",
            mysql_error_detail(&error),
        )),
    }
}

async fn probe_gtid_boundary(
    connection: &mut mysql_async::Conn,
    server_uuid: &str,
    value: u64,
) -> Result<GtidBoundaryProbe, QualificationError> {
    let gtid = format!("{server_uuid}:{value}");
    let sql = format!(
        "SELECT GTID_SUBSET({}, {})",
        sql_literal(&gtid),
        sql_literal(&gtid)
    );
    match connection.query_first::<Option<Option<u8>>, _>(&sql).await {
        Ok(_) => Ok(GtidBoundaryProbe {
            value,
            accepted: true,
            code: None,
            sql_state: None,
        }),
        Err(mysql_async::Error::Server(server)) => Ok(GtidBoundaryProbe {
            value,
            accepted: false,
            code: Some(server.code),
            sql_state: Some(server.state),
        }),
        Err(error) => Err(QualificationError::new(
            QualificationCode::AccountStateSetupFailure,
            "probe GTID numeric boundary",
            mysql_error_detail(&error),
        )),
    }
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

fn required_field(
    value: Option<String>,
    field: &'static str,
) -> Result<String, QualificationError> {
    value.ok_or_else(|| {
        QualificationError::new(
            QualificationCode::AccountStateSetupFailure,
            field,
            format!("{field} must not be NULL"),
        )
    })
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::sql_literal;

    #[test]
    fn gtid_boundary_probe_values_are_exact() {
        let sql = format!(
            "SELECT GTID_SUBSET({}, {})",
            sql_literal("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:9223372036854775806"),
            sql_literal("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:9223372036854775806")
        );
        assert!(sql.contains("9223372036854775806"));
        assert!(!sql.contains("9223372036854775807"));
    }

    #[test]
    fn sql_literal_escapes_single_quotes() {
        assert_eq!(sql_literal("o'hara"), "'o''hara'");
    }
}
