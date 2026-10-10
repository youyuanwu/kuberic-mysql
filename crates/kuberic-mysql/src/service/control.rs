//! Private-UDS native topology control, separate from read-only observation.

use core::future::Future;
use std::time::Instant;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Error, OptsBuilder};

use crate::adapter::UnixSocketPath;
use crate::core::{GroupName, ServerUuid, ViewId};
use crate::service::{
    AccountProvisioningEvidence, BootstrapCapability, BootstrapEffect, ControlCredential,
    ControlCredentialRole, ControlStage, JoinCapability, JoinEffect, MemberControlBinding,
    MysqlMemberConfig, MysqlMemberIndex, MysqlTopologyError, NativeControlDeadline,
    NativeIdentityEnrollment, TopologyAuthorityError, TopologyEvidenceError, ViewDiscovery,
};

pub(crate) struct OwnedControlTarget {
    socket: UnixSocketPath,
    member_index: MysqlMemberIndex,
    member: MysqlMemberConfig,
}

impl OwnedControlTarget {
    pub(crate) fn new(
        socket: UnixSocketPath,
        member_index: MysqlMemberIndex,
        member: MysqlMemberConfig,
    ) -> Self {
        Self {
            socket,
            member_index,
            member,
        }
    }
}

pub(crate) async fn provision_accounts(
    target: OwnedControlTarget,
    binding: MemberControlBinding,
    observer: &ControlCredential,
    recovery: &ControlCredential,
    control_deadline: &NativeControlDeadline,
) -> Result<AccountProvisioningEvidence, MysqlTopologyError> {
    verify_target(&target, &binding)?;
    verify_credential(&binding, observer, ControlCredentialRole::Observer)?;
    verify_credential(&binding, recovery, ControlCredentialRole::Recovery)?;
    let deadline = verified_deadline(
        control_deadline,
        binding.attempt(),
        ControlStage::ProductValidation,
    )?;
    let mut connection = connect_root(&target, deadline).await?;
    let result = async {
        validate_product(&mut connection, deadline).await?;
        let original: u8 = query_one(
            &mut connection,
            "SELECT @@SESSION.sql_log_bin",
            deadline,
            ControlStage::ReadBinaryLogging,
        )
        .await?;
        execute(
            &mut connection,
            "SET SESSION sql_log_bin = OFF",
            deadline,
            ControlStage::DisableBinaryLogging,
        )
        .await?;

        let primary = provision_accounts_inner(&mut connection, observer, recovery, deadline).await;
        let restore_sql = if original == 0 {
            "SET SESSION sql_log_bin = OFF"
        } else {
            "SET SESSION sql_log_bin = ON"
        };
        let restore = execute(
            &mut connection,
            restore_sql,
            deadline,
            ControlStage::RestoreBinaryLogging,
        )
        .await
        .and_then(|()| {
            if Instant::now() >= deadline {
                Err(MysqlTopologyError::Deadline(
                    ControlStage::ProveBinaryLoggingRestored,
                ))
            } else {
                Ok(())
            }
        });
        let restore = match restore {
            Ok(()) => match query_one::<u8>(
                &mut connection,
                "SELECT @@SESSION.sql_log_bin",
                deadline,
                ControlStage::ProveBinaryLoggingRestored,
            )
            .await
            {
                Ok(current) if current == original => Ok(()),
                Ok(_) => Err(MysqlTopologyError::ControlProtocol(
                    ControlStage::ProveBinaryLoggingRestored,
                )),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };
        combine_primary_cleanup(primary, restore)?;
        AccountProvisioningEvidence::new(
            binding,
            observer,
            recovery,
            AccountProvisioningEvidence::required_steps().to_vec(),
        )
    }
    .await;
    disconnect(connection, result, deadline).await
}

async fn provision_accounts_inner(
    connection: &mut Conn,
    observer: &ControlCredential,
    recovery: &ControlCredential,
    deadline: Instant,
) -> Result<(), MysqlTopologyError> {
    let mut observer_create = SecretStatement::new(format!(
        "CREATE USER '{}'@'localhost' IDENTIFIED BY '{}'",
        observer.username(),
        observer.password()
    ));
    execute(
        connection,
        observer_create.as_str(),
        deadline,
        ControlStage::CreateObserver,
    )
    .await?;
    observer_create.clear();
    execute(
        connection,
        &format!(
            "GRANT SELECT ON performance_schema.replication_group_members TO '{}'@'localhost'",
            observer.username()
        ),
        deadline,
        ControlStage::GrantObserverMembers,
    )
    .await?;
    execute(
        connection,
        &format!(
            "GRANT SELECT ON performance_schema.replication_group_member_stats TO '{}'@'localhost'",
            observer.username()
        ),
        deadline,
        ControlStage::GrantObserverStats,
    )
    .await?;
    let mut recovery_create = SecretStatement::new(format!(
        "CREATE USER '{}'@'localhost' IDENTIFIED BY '{}'",
        recovery.username(),
        recovery.password()
    ));
    execute(
        connection,
        recovery_create.as_str(),
        deadline,
        ControlStage::CreateRecovery,
    )
    .await?;
    recovery_create.clear();
    execute(
        connection,
        &format!(
            "GRANT REPLICATION SLAVE, CONNECTION_ADMIN ON *.* TO '{}'@'localhost'",
            recovery.username()
        ),
        deadline,
        ControlStage::GrantRecovery,
    )
    .await
}

pub(crate) async fn enroll_identity(
    target: OwnedControlTarget,
    binding: MemberControlBinding,
    accounts: &AccountProvisioningEvidence,
    control_deadline: &NativeControlDeadline,
) -> Result<NativeIdentityEnrollment, MysqlTopologyError> {
    verify_target(&target, &binding)?;
    let deadline = verified_deadline(
        control_deadline,
        binding.attempt(),
        ControlStage::ProductValidation,
    )?;
    let mut connection = connect_root(&target, deadline).await?;
    let result = async {
        let product_server_uuid = validate_product(&mut connection, deadline).await?;
        let active: u64 = query_one(
            &mut connection,
            "SELECT COUNT(*) FROM performance_schema.replication_group_members \
             WHERE MEMBER_ID IS NOT NULL AND MEMBER_ID <> ''",
            deadline,
            ControlStage::InspectExistingGroup,
        )
        .await?;
        let enrolled_server_uuid: String = query_one(
            &mut connection,
            "SELECT @@GLOBAL.server_uuid",
            deadline,
            ControlStage::EnrollIdentity,
        )
        .await?;
        let server_uuid = ServerUuid::new(enrolled_server_uuid)
            .map_err(|_| MysqlTopologyError::ControlProductCompatibility)?;
        if server_uuid != product_server_uuid {
            return Err(MysqlTopologyError::Evidence(
                TopologyEvidenceError::IdentityDrift,
            ));
        }
        NativeIdentityEnrollment::new(
            binding,
            server_uuid,
            accounts,
            active == 0,
            NativeIdentityEnrollment::required_steps().to_vec(),
        )
    }
    .await;
    disconnect(connection, result, deadline).await
}

pub(crate) async fn bootstrap(
    target: OwnedControlTarget,
    capability: BootstrapCapability,
    recovery: &ControlCredential,
    control_deadline: &NativeControlDeadline,
) -> Result<BootstrapEffect, MysqlTopologyError> {
    verify_capability_target(&target, capability.target().binding(), capability.attempt())?;
    verify_credential(
        capability.target().binding(),
        recovery,
        ControlCredentialRole::Recovery,
    )?;
    let deadline = verified_deadline(
        control_deadline,
        capability.attempt().id(),
        ControlStage::EnableBootstrap,
    )?;
    let mut connection = connect_root(&target, deadline).await?;
    let result = async {
        validate_product(&mut connection, deadline).await?;
        execute(
            &mut connection,
            "SET GLOBAL group_replication_bootstrap_group = ON",
            deadline,
            ControlStage::EnableBootstrap,
        )
        .await?;
        let mut start = start_statement(recovery);
        let primary = execute(
            &mut connection,
            start.as_str(),
            deadline,
            ControlStage::StartGroupReplication,
        )
        .await;
        start.clear();
        let cleanup = execute(
            &mut connection,
            "SET GLOBAL group_replication_bootstrap_group = OFF",
            deadline,
            ControlStage::DisableBootstrap,
        )
        .await
        .and_then(|()| {
            if Instant::now() >= deadline {
                Err(MysqlTopologyError::Deadline(
                    ControlStage::ProveBootstrapDisabled,
                ))
            } else {
                Ok(())
            }
        });
        let cleanup = match cleanup {
            Ok(()) => prove_bootstrap_off(&mut connection, deadline).await,
            Err(error) => Err(error),
        };
        combine_primary_cleanup(primary, cleanup)?;
        capability.record_effect(BootstrapEffect::required_steps().to_vec())
    }
    .await;
    disconnect(connection, result, deadline).await
}

pub(crate) async fn join(
    target: OwnedControlTarget,
    capability: JoinCapability,
    recovery: &ControlCredential,
    control_deadline: &NativeControlDeadline,
) -> Result<JoinEffect, MysqlTopologyError> {
    verify_capability_target(&target, capability.target().binding(), capability.attempt())?;
    let deadline = verified_deadline(
        control_deadline,
        capability.attempt().id(),
        ControlStage::StartGroupReplication,
    )?;
    verify_credential(
        capability.target().binding(),
        recovery,
        ControlCredentialRole::Recovery,
    )?;
    let mut connection = connect_root(&target, deadline).await?;
    let result = async {
        validate_product(&mut connection, deadline).await?;
        prove_bootstrap_off(&mut connection, deadline).await?;
        let mut start = start_statement(recovery);
        let started = execute(
            &mut connection,
            start.as_str(),
            deadline,
            ControlStage::StartGroupReplication,
        )
        .await;
        start.clear();
        started?;
        capability.record_effect(JoinEffect::required_steps().to_vec())
    }
    .await;
    disconnect(connection, result, deadline).await
}

pub(crate) async fn discover_view(
    target: OwnedControlTarget,
    enrollment: NativeIdentityEnrollment,
    group_name: GroupName,
    control_deadline: &NativeControlDeadline,
) -> Result<ViewDiscovery, MysqlTopologyError> {
    verify_target(&target, enrollment.binding())?;
    let deadline = verified_deadline(
        control_deadline,
        enrollment.binding().attempt(),
        ControlStage::DiscoverView,
    )?;
    let mut connection = connect_root(&target, deadline).await?;
    let result = async {
        validate_product(&mut connection, deadline).await?;
        let observed_server_uuid: String = query_one(
            &mut connection,
            "SELECT @@GLOBAL.server_uuid",
            deadline,
            ControlStage::VerifyEnrolledIdentity,
        )
        .await?;
        let observed_server_uuid = ServerUuid::new(observed_server_uuid)
            .map_err(|_| MysqlTopologyError::ControlProductCompatibility)?;
        if observed_server_uuid != *enrollment.server_uuid() {
            return Err(MysqlTopologyError::Evidence(
                TopologyEvidenceError::IdentityDrift,
            ));
        }
        let (native_group, member_id, host, port, view_id): (String, String, String, u16, String) =
            query_one(
                &mut connection,
                "SELECT @@GLOBAL.group_replication_group_name, m.MEMBER_ID, m.MEMBER_HOST, \
             m.MEMBER_PORT, s.VIEW_ID \
             FROM performance_schema.replication_group_members AS m \
             JOIN performance_schema.replication_group_member_stats AS s \
             ON s.MEMBER_ID = m.MEMBER_ID \
             WHERE m.MEMBER_ID = @@GLOBAL.server_uuid",
                deadline,
                ControlStage::DiscoverView,
            )
            .await?;
        if native_group != group_name.as_str() {
            return Err(MysqlTopologyError::Evidence(
                TopologyEvidenceError::GroupMismatch,
            ));
        }
        if member_id != enrollment.member_id().as_str()
            || format!("{host}:{port}") != enrollment.binding().member_address().as_str()
        {
            return Err(MysqlTopologyError::Evidence(
                TopologyEvidenceError::IdentityDrift,
            ));
        }
        let view_id = ViewId::new(view_id)
            .map_err(|_| MysqlTopologyError::ControlProtocol(ControlStage::DiscoverView))?;
        ViewDiscovery::new(
            enrollment,
            group_name,
            view_id,
            ViewDiscovery::required_steps().to_vec(),
        )
    }
    .await;
    disconnect(connection, result, deadline).await
}

fn verify_target(
    target: &OwnedControlTarget,
    binding: &MemberControlBinding,
) -> Result<(), MysqlTopologyError> {
    if target.member_index != binding.member()
        || target.member.sql_address().to_string() != binding.member_address().as_str()
    {
        return Err(MysqlTopologyError::Evidence(
            TopologyEvidenceError::OwnershipDrift,
        ));
    }
    Ok(())
}

fn verify_capability_target(
    target: &OwnedControlTarget,
    binding: &MemberControlBinding,
    attempt: &crate::service::TopologyAttempt,
) -> Result<(), MysqlTopologyError> {
    verify_target(target, binding)?;
    if binding.attempt() != attempt.id()
        || target.member.group_uuid() != attempt.group_name().as_str()
    {
        return Err(MysqlTopologyError::Authority(
            TopologyAuthorityError::AttemptMismatch,
        ));
    }
    Ok(())
}

fn verify_credential(
    binding: &MemberControlBinding,
    credential: &ControlCredential,
    role: ControlCredentialRole,
) -> Result<(), MysqlTopologyError> {
    if credential.attempt() != binding.attempt() {
        return Err(MysqlTopologyError::Authority(
            TopologyAuthorityError::AttemptMismatch,
        ));
    }
    let expected = match role {
        ControlCredentialRole::Observer => binding.observer_generation(),
        ControlCredentialRole::Recovery => binding.recovery_generation(),
    };
    if credential.role() != role || credential.generation() != expected || credential.is_cleared() {
        return Err(MysqlTopologyError::Authority(
            TopologyAuthorityError::CredentialGenerationMismatch,
        ));
    }
    Ok(())
}

fn verified_deadline(
    deadline: &NativeControlDeadline,
    attempt: &crate::core::AttemptId,
    stage: ControlStage,
) -> Result<Instant, MysqlTopologyError> {
    if deadline.attempt() != attempt {
        return Err(MysqlTopologyError::Authority(
            TopologyAuthorityError::AttemptMismatch,
        ));
    }
    let instant = deadline.instant();
    if instant <= Instant::now() {
        Err(MysqlTopologyError::Deadline(stage))
    } else {
        Ok(instant)
    }
}

async fn connect_root(
    target: &OwnedControlTarget,
    deadline: Instant,
) -> Result<Conn, MysqlTopologyError> {
    target
        .socket
        .revalidate()
        .map_err(|_| MysqlTopologyError::ControlTransport(ControlStage::SocketValidation))?;
    let options = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(1)
        .socket(Some(
            target
                .socket
                .as_path()
                .to_str()
                .expect("UnixSocketPath rejects non-UTF-8 paths")
                .to_owned(),
        ))
        .user(Some("root".to_owned()));
    bounded(deadline, ControlStage::Connect, Conn::new(options)).await
}

async fn validate_product(
    connection: &mut Conn,
    deadline: Instant,
) -> Result<ServerUuid, MysqlTopologyError> {
    let (version, comment, machine, operating_system, server_uuid): (
        String,
        String,
        String,
        String,
        String,
    ) = query_one(
        connection,
        "SELECT @@version, @@version_comment, @@version_compile_machine, \
         @@version_compile_os, @@GLOBAL.server_uuid",
        deadline,
        ControlStage::ProductValidation,
    )
    .await?;
    if version != "8.4.11"
        || comment != "MySQL Community Server - GPL"
        || machine != "x86_64"
        || operating_system != "Linux"
    {
        return Err(MysqlTopologyError::ControlProductCompatibility);
    }
    ServerUuid::new(server_uuid).map_err(|_| MysqlTopologyError::ControlProductCompatibility)
}

async fn prove_bootstrap_off(
    connection: &mut Conn,
    deadline: Instant,
) -> Result<(), MysqlTopologyError> {
    let enabled: u8 = query_one(
        connection,
        "SELECT @@GLOBAL.group_replication_bootstrap_group",
        deadline,
        ControlStage::ProveBootstrapDisabled,
    )
    .await?;
    if enabled == 0 {
        Ok(())
    } else {
        Err(MysqlTopologyError::ControlProtocol(
            ControlStage::ProveBootstrapDisabled,
        ))
    }
}

fn start_statement(recovery: &ControlCredential) -> SecretStatement {
    SecretStatement::new(format!(
        "START GROUP_REPLICATION USER='{}', PASSWORD='{}', \
         DEFAULT_AUTH='caching_sha2_password'",
        recovery.username(),
        recovery.password()
    ))
}

async fn execute(
    connection: &mut Conn,
    sql: &str,
    deadline: Instant,
    stage: ControlStage,
) -> Result<(), MysqlTopologyError> {
    bounded(deadline, stage, connection.query_drop(sql)).await
}

async fn query_one<T>(
    connection: &mut Conn,
    sql: &str,
    deadline: Instant,
    stage: ControlStage,
) -> Result<T, MysqlTopologyError>
where
    T: mysql_async::prelude::FromRow + Send + 'static,
{
    bounded(deadline, stage, connection.query_first(sql))
        .await?
        .ok_or(MysqlTopologyError::ControlProtocol(stage))
}

async fn bounded<T, F>(
    deadline: Instant,
    stage: ControlStage,
    future: F,
) -> Result<T, MysqlTopologyError>
where
    F: Future<Output = Result<T, Error>>,
{
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(MysqlTopologyError::Deadline(stage))?;
    let value = tokio::time::timeout(remaining, future)
        .await
        .map_err(|_| MysqlTopologyError::Deadline(stage))?
        .map_err(|error| map_client_error(error, stage))?;
    if Instant::now() >= deadline {
        Err(MysqlTopologyError::Deadline(stage))
    } else {
        Ok(value)
    }
}

fn map_client_error(error: Error, stage: ControlStage) -> MysqlTopologyError {
    match error {
        Error::Io(_) => MysqlTopologyError::ControlTransport(stage),
        Error::Server(server) => {
            if server.code == 1045 || server.state == "28000" {
                MysqlTopologyError::ControlAuthentication(ControlStage::Authenticate)
            } else if matches!(server.code, 1142 | 1143 | 1227) {
                MysqlTopologyError::ControlPermission(stage)
            } else {
                MysqlTopologyError::ControlProtocol(stage)
            }
        }
        Error::Driver(_) | Error::Other(_) | Error::Url(_) => {
            MysqlTopologyError::ControlProtocol(stage)
        }
    }
}

fn combine_primary_cleanup(
    primary: Result<(), MysqlTopologyError>,
    cleanup: Result<(), MysqlTopologyError>,
) -> Result<(), MysqlTopologyError> {
    match (primary, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(prior), Ok(())) => Err(prior),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(prior), Err(cleanup)) => Err(MysqlTopologyError::PairedControl {
            prior: Box::new(prior),
            cleanup: Box::new(cleanup),
        }),
    }
}

async fn disconnect<T>(
    connection: Conn,
    result: Result<T, MysqlTopologyError>,
    deadline: Instant,
) -> Result<T, MysqlTopologyError> {
    let disconnected = bounded(deadline, ControlStage::Disconnect, connection.disconnect()).await;
    match (result, disconnected) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(prior), Ok(())) => Err(prior),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(prior), Err(cleanup)) => Err(MysqlTopologyError::PairedControl {
            prior: Box::new(prior),
            cleanup: Box::new(cleanup),
        }),
    }
}

struct SecretStatement {
    bytes: Vec<u8>,
}

impl SecretStatement {
    fn new(value: String) -> Self {
        Self {
            bytes: value.into_bytes(),
        }
    }

    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes).expect("control statement originated as UTF-8")
    }

    fn clear(&mut self) {
        self.bytes.fill(0);
    }
}

impl Drop for SecretStatement {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::AttemptId;
    use mysql_async::{IoError, ServerError};
    use std::io;
    use std::time::Duration;

    #[test]
    fn paired_control_retains_both_secret_free_failures() {
        let error = combine_primary_cleanup(
            Err(MysqlTopologyError::ControlPermission(
                ControlStage::StartGroupReplication,
            )),
            Err(MysqlTopologyError::ControlTransport(
                ControlStage::DisableBootstrap,
            )),
        )
        .unwrap_err();
        assert_eq!(
            error,
            MysqlTopologyError::PairedControl {
                prior: Box::new(MysqlTopologyError::ControlPermission(
                    ControlStage::StartGroupReplication
                )),
                cleanup: Box::new(MysqlTopologyError::ControlTransport(
                    ControlStage::DisableBootstrap
                )),
            }
        );
    }

    #[test]
    fn secret_statement_is_explicitly_zeroed() {
        let mut statement = SecretStatement::new("PASSWORD='fixture-secret'".to_owned());
        statement.clear();
        assert!(statement.bytes.iter().all(|byte| *byte == 0));
    }

    #[test]
    fn client_failures_map_to_distinct_secret_free_classes() {
        assert_eq!(
            map_client_error(
                Error::Io(IoError::Io(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "fixture transport details",
                ))),
                ControlStage::Connect,
            ),
            MysqlTopologyError::ControlTransport(ControlStage::Connect)
        );
        assert_eq!(
            map_client_error(
                Error::Server(ServerError {
                    code: 1045,
                    message: "secret-bearing authentication message".to_owned(),
                    state: "28000".to_owned(),
                }),
                ControlStage::Connect,
            ),
            MysqlTopologyError::ControlAuthentication(ControlStage::Authenticate)
        );
        assert_eq!(
            map_client_error(
                Error::Server(ServerError {
                    code: 1227,
                    message: "secret-bearing permission message".to_owned(),
                    state: "42000".to_owned(),
                }),
                ControlStage::GrantRecovery,
            ),
            MysqlTopologyError::ControlPermission(ControlStage::GrantRecovery)
        );
    }

    #[test]
    fn native_deadline_is_attempt_bound_before_control() {
        let deadline = NativeControlDeadline::new(
            AttemptId::new("attempt-one").unwrap(),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(
            verified_deadline(
                &deadline,
                &AttemptId::new("attempt-two").unwrap(),
                ControlStage::Connect,
            ),
            Err(MysqlTopologyError::Authority(
                TopologyAuthorityError::AttemptMismatch
            ))
        );
    }
}
