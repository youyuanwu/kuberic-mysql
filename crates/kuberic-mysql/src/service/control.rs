//! Private-UDS native topology control, separate from read-only observation.

use core::future::Future;
use core::str::FromStr;
use std::time::Instant;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Error, OptsBuilder};

use crate::adapter::UnixSocketPath;
use crate::core::{GroupName, ServerUuid, ViewId};
use crate::service::{
    AccountProvisioningEvidence, BootstrapCapability, BootstrapEffect, ControlCredential,
    ControlCredentialRole, ControlStage, JoinCapability, JoinEffect, MemberControlBinding,
    MysqlMemberConfig, MysqlMemberIndex, MysqlTopologyError, NativeControlDeadline,
    NativeIdentityEnrollment, TopologyAuthorityError, TopologyEvidenceError, TopologyStateError,
    ViewDiscovery,
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum ControlQueryResult {
    Product {
        version: String,
        comment: String,
        machine: String,
        operating_system: String,
        server_uuid: String,
    },
    Unsigned8(u8),
    Unsigned64(u64),
    Text(String),
    View(String, String, String, u16, String),
}

trait ControlSession {
    async fn execute(
        &mut self,
        sql: &str,
        deadline: Instant,
        stage: ControlStage,
    ) -> Result<(), MysqlTopologyError>;

    async fn query(
        &mut self,
        sql: &str,
        deadline: Instant,
        stage: ControlStage,
    ) -> Result<ControlQueryResult, MysqlTopologyError>;

    async fn disconnect(self, deadline: Instant) -> Result<(), MysqlTopologyError>;
}

trait ControlConnector {
    type Session: ControlSession;

    async fn connect(
        &self,
        target: &OwnedControlTarget,
        deadline: Instant,
    ) -> Result<Self::Session, MysqlTopologyError>;
}

struct MysqlControlConnector;

struct MysqlControlSession {
    connection: Conn,
}

pub(crate) async fn provision_accounts(
    target: OwnedControlTarget,
    binding: MemberControlBinding,
    observer: &ControlCredential,
    recovery: &ControlCredential,
    control_deadline: &NativeControlDeadline,
) -> Result<AccountProvisioningEvidence, MysqlTopologyError> {
    provision_accounts_with(
        &MysqlControlConnector,
        target,
        binding,
        observer,
        recovery,
        control_deadline,
    )
    .await
}

async fn provision_accounts_with<C: ControlConnector>(
    connector: &C,
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
    let mut connection = connector.connect(&target, deadline).await?;
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

async fn provision_accounts_inner<S: ControlSession>(
    connection: &mut S,
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
    enroll_identity_with(
        &MysqlControlConnector,
        target,
        binding,
        accounts,
        control_deadline,
    )
    .await
}

async fn enroll_identity_with<C: ControlConnector>(
    connector: &C,
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
    let mut connection = connector.connect(&target, deadline).await?;
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
        if active != 0 {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::ExistingGroupState,
            ));
        }
        let executed: String = query_one(
            &mut connection,
            "SELECT @@GLOBAL.gtid_executed",
            deadline,
            ControlStage::InspectExistingHistory,
        )
        .await?;
        let executed = crate::core::GtidSet::from_str(&executed).map_err(|_| {
            MysqlTopologyError::ControlProtocol(ControlStage::InspectExistingHistory)
        })?;
        if !executed.entries().is_empty() {
            return Err(MysqlTopologyError::TopologyState(
                TopologyStateError::ExistingTransactionHistory,
            ));
        }
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
            true,
            executed,
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
    bootstrap_with(
        &MysqlControlConnector,
        target,
        capability,
        recovery,
        control_deadline,
    )
    .await
}

async fn bootstrap_with<C: ControlConnector>(
    connector: &C,
    target: OwnedControlTarget,
    capability: BootstrapCapability,
    recovery: &ControlCredential,
    control_deadline: &NativeControlDeadline,
) -> Result<BootstrapEffect, MysqlTopologyError> {
    verify_capability_target(&target, capability.target().binding(), capability.attempt())?;
    verify_recovery_credential(capability.target(), recovery)?;
    let deadline = verified_deadline(
        control_deadline,
        capability.attempt().id(),
        ControlStage::EnableBootstrap,
    )?;
    let mut connection = connector.connect(&target, deadline).await?;
    let result = async {
        validate_product(&mut connection, deadline).await?;
        let effect_deadline = reserve_cleanup_deadline(deadline, ControlStage::EnableBootstrap)?;
        let enable = execute(
            &mut connection,
            "SET GLOBAL group_replication_bootstrap_group = ON",
            effect_deadline,
            ControlStage::EnableBootstrap,
        )
        .await;
        let primary = match enable {
            Ok(()) => {
                let mut start = start_statement(recovery);
                let result = execute(
                    &mut connection,
                    start.as_str(),
                    effect_deadline,
                    ControlStage::StartGroupReplication,
                )
                .await;
                start.clear();
                result
            }
            Err(error) => Err(error),
        };
        let disable = execute(
            &mut connection,
            "SET GLOBAL group_replication_bootstrap_group = OFF",
            deadline,
            ControlStage::DisableBootstrap,
        )
        .await;
        let proof = prove_bootstrap_off(&mut connection, deadline).await;
        let cleanup = combine_primary_cleanup(disable, proof);
        combine_primary_cleanup(primary, cleanup)?;
        capability.record_effect(BootstrapEffect::required_steps().to_vec())
    }
    .await;
    disconnect(connection, result, deadline).await
}

fn reserve_cleanup_deadline(
    deadline: Instant,
    stage: ControlStage,
) -> Result<Instant, MysqlTopologyError> {
    let now = Instant::now();
    let remaining = deadline
        .checked_duration_since(now)
        .filter(|remaining| !remaining.is_zero())
        .ok_or(MysqlTopologyError::Deadline(stage))?;
    now.checked_add(remaining / 2)
        .filter(|effect_deadline| *effect_deadline < deadline)
        .ok_or(MysqlTopologyError::Deadline(stage))
}

pub(crate) async fn join(
    target: OwnedControlTarget,
    capability: JoinCapability,
    recovery: &ControlCredential,
    control_deadline: &NativeControlDeadline,
) -> Result<JoinEffect, MysqlTopologyError> {
    join_with(
        &MysqlControlConnector,
        target,
        capability,
        recovery,
        control_deadline,
    )
    .await
}

async fn join_with<C: ControlConnector>(
    connector: &C,
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
    verify_recovery_credential(capability.target(), recovery)?;
    let mut connection = connector.connect(&target, deadline).await?;
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
    discover_view_with(
        &MysqlControlConnector,
        target,
        enrollment,
        group_name,
        control_deadline,
    )
    .await
}

async fn discover_view_with<C: ControlConnector>(
    connector: &C,
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
    let mut connection = connector.connect(&target, deadline).await?;
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

fn verify_recovery_credential(
    enrollment: &NativeIdentityEnrollment,
    credential: &ControlCredential,
) -> Result<(), MysqlTopologyError> {
    verify_credential(
        enrollment.binding(),
        credential,
        ControlCredentialRole::Recovery,
    )?;
    if credential.username() != enrollment.recovery_username() {
        return Err(MysqlTopologyError::Authority(
            TopologyAuthorityError::PrincipalMismatch,
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

impl ControlConnector for MysqlControlConnector {
    type Session = MysqlControlSession;

    async fn connect(
        &self,
        target: &OwnedControlTarget,
        deadline: Instant,
    ) -> Result<Self::Session, MysqlTopologyError> {
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
        let connection = bounded(deadline, ControlStage::Connect, Conn::new(options)).await?;
        Ok(MysqlControlSession { connection })
    }
}

impl ControlSession for MysqlControlSession {
    async fn execute(
        &mut self,
        sql: &str,
        deadline: Instant,
        stage: ControlStage,
    ) -> Result<(), MysqlTopologyError> {
        bounded(deadline, stage, self.connection.query_drop(sql)).await
    }

    async fn query(
        &mut self,
        sql: &str,
        deadline: Instant,
        stage: ControlStage,
    ) -> Result<ControlQueryResult, MysqlTopologyError> {
        match stage {
            ControlStage::ProductValidation => {
                let (version, comment, machine, operating_system, server_uuid) =
                    query_one_mysql(&mut self.connection, sql, deadline, stage).await?;
                Ok(ControlQueryResult::Product {
                    version,
                    comment,
                    machine,
                    operating_system,
                    server_uuid,
                })
            }
            ControlStage::ReadBinaryLogging
            | ControlStage::ProveBinaryLoggingRestored
            | ControlStage::ProveBootstrapDisabled => Ok(ControlQueryResult::Unsigned8(
                query_one_mysql(&mut self.connection, sql, deadline, stage).await?,
            )),
            ControlStage::InspectExistingGroup => Ok(ControlQueryResult::Unsigned64(
                query_one_mysql(&mut self.connection, sql, deadline, stage).await?,
            )),
            ControlStage::InspectExistingHistory
            | ControlStage::EnrollIdentity
            | ControlStage::VerifyEnrolledIdentity => Ok(ControlQueryResult::Text(
                query_one_mysql(&mut self.connection, sql, deadline, stage).await?,
            )),
            ControlStage::DiscoverView => {
                let (group, member_id, host, port, view_id) =
                    query_one_mysql(&mut self.connection, sql, deadline, stage).await?;
                Ok(ControlQueryResult::View(
                    group, member_id, host, port, view_id,
                ))
            }
            _ => Err(MysqlTopologyError::ControlProtocol(stage)),
        }
    }

    async fn disconnect(self, deadline: Instant) -> Result<(), MysqlTopologyError> {
        bounded(
            deadline,
            ControlStage::Disconnect,
            self.connection.disconnect(),
        )
        .await
    }
}

async fn validate_product<S: ControlSession>(
    connection: &mut S,
    deadline: Instant,
) -> Result<ServerUuid, MysqlTopologyError> {
    let ControlQueryResult::Product {
        version,
        comment,
        machine,
        operating_system,
        server_uuid,
    } = connection
        .query(
            "SELECT @@version, @@version_comment, @@version_compile_machine, \
         @@version_compile_os, @@GLOBAL.server_uuid",
            deadline,
            ControlStage::ProductValidation,
        )
        .await?
    else {
        return Err(MysqlTopologyError::ControlProtocol(
            ControlStage::ProductValidation,
        ));
    };
    if version != "8.4.11"
        || comment != "MySQL Community Server - GPL"
        || machine != "x86_64"
        || operating_system != "Linux"
    {
        return Err(MysqlTopologyError::ControlProductCompatibility);
    }
    ServerUuid::new(server_uuid).map_err(|_| MysqlTopologyError::ControlProductCompatibility)
}

async fn prove_bootstrap_off<S: ControlSession>(
    connection: &mut S,
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
    connection: &mut impl ControlSession,
    sql: &str,
    deadline: Instant,
    stage: ControlStage,
) -> Result<(), MysqlTopologyError> {
    connection.execute(sql, deadline, stage).await
}

async fn query_one<T>(
    connection: &mut impl ControlSession,
    sql: &str,
    deadline: Instant,
    stage: ControlStage,
) -> Result<T, MysqlTopologyError>
where
    T: FromControlQuery,
{
    T::from_control_query(connection.query(sql, deadline, stage).await?, stage)
}

trait FromControlQuery: Sized {
    fn from_control_query(
        result: ControlQueryResult,
        stage: ControlStage,
    ) -> Result<Self, MysqlTopologyError>;
}

impl FromControlQuery for u8 {
    fn from_control_query(
        result: ControlQueryResult,
        stage: ControlStage,
    ) -> Result<Self, MysqlTopologyError> {
        match result {
            ControlQueryResult::Unsigned8(value) => Ok(value),
            _ => Err(MysqlTopologyError::ControlProtocol(stage)),
        }
    }
}

impl FromControlQuery for u64 {
    fn from_control_query(
        result: ControlQueryResult,
        stage: ControlStage,
    ) -> Result<Self, MysqlTopologyError> {
        match result {
            ControlQueryResult::Unsigned64(value) => Ok(value),
            _ => Err(MysqlTopologyError::ControlProtocol(stage)),
        }
    }
}

impl FromControlQuery for String {
    fn from_control_query(
        result: ControlQueryResult,
        stage: ControlStage,
    ) -> Result<Self, MysqlTopologyError> {
        match result {
            ControlQueryResult::Text(value) => Ok(value),
            _ => Err(MysqlTopologyError::ControlProtocol(stage)),
        }
    }
}

impl FromControlQuery for (String, String, String, u16, String) {
    fn from_control_query(
        result: ControlQueryResult,
        stage: ControlStage,
    ) -> Result<Self, MysqlTopologyError> {
        match result {
            ControlQueryResult::View(group, member_id, host, port, view_id) => {
                Ok((group, member_id, host, port, view_id))
            }
            _ => Err(MysqlTopologyError::ControlProtocol(stage)),
        }
    }
}

async fn query_one_mysql<T>(
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

async fn disconnect<T, S: ControlSession>(
    connection: S,
    result: Result<T, MysqlTopologyError>,
    deadline: Instant,
) -> Result<T, MysqlTopologyError> {
    let disconnected = connection.disconnect(deadline).await;
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
    use crate::core::{
        AttemptId, CredentialGeneration, EndpointBinding, GtidSet, MemberAddress, MemberRole,
        MemberState, NativeMember, ProcessSessionId, StorageBinding,
    };
    use crate::service::{
        ControlCredentialRole, MysqlTopologyConfig, ObservedLocalBinding, TopologyAttempt,
        TopologyAuthority, TopologyInstant, TopologyObservation, TopologyObservationStatus,
        TopologyStateError, TransitionEvaluation,
    };
    use mysql_async::{IoError, ServerError};
    use std::collections::VecDeque;
    use std::fs;
    use std::io;
    use std::net::SocketAddr;
    use std::os::unix::net::UnixListener;
    use std::path::{Path, PathBuf};
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    const GROUP_UUID: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
    const MEMBER_UUIDS: [&str; 3] = [
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        "dddddddd-dddd-dddd-dddd-dddddddddddd",
    ];

    #[derive(Clone)]
    struct ScriptConnector {
        state: Arc<Mutex<ScriptState>>,
        connect_error: Option<MysqlTopologyError>,
    }

    struct ScriptSession {
        state: Arc<Mutex<ScriptState>>,
    }

    struct ScriptState {
        expected_socket: PathBuf,
        selected_socket: Option<PathBuf>,
        actions: VecDeque<ScriptAction>,
        start_effects: usize,
        disconnects: usize,
    }

    enum ScriptAction {
        Query {
            stage: ControlStage,
            sql: String,
            result: Result<ControlQueryResult, MysqlTopologyError>,
        },
        Execute {
            stage: ControlStage,
            sql: String,
            result: Result<(), MysqlTopologyError>,
        },
    }

    impl ScriptConnector {
        fn new(socket: &Path, actions: Vec<ScriptAction>) -> Self {
            Self {
                state: Arc::new(Mutex::new(ScriptState {
                    expected_socket: socket.to_path_buf(),
                    selected_socket: None,
                    actions: actions.into(),
                    start_effects: 0,
                    disconnects: 0,
                })),
                connect_error: None,
            }
        }

        fn failing(socket: &Path, error: MysqlTopologyError) -> Self {
            let mut connector = Self::new(socket, Vec::new());
            connector.connect_error = Some(error);
            connector
        }

        fn assert_complete(&self, expected_start_effects: usize, expected_disconnects: usize) {
            let state = self.state.lock().unwrap();
            assert_eq!(
                state.selected_socket.as_deref(),
                Some(state.expected_socket.as_path())
            );
            assert!(state.actions.is_empty(), "unconsumed scripted actions");
            assert_eq!(state.start_effects, expected_start_effects);
            assert_eq!(state.disconnects, expected_disconnects);
        }

        fn assert_rejected_before_connect(&self) {
            let state = self.state.lock().unwrap();
            assert!(state.selected_socket.is_none());
            assert!(state.actions.is_empty());
            assert_eq!(state.start_effects, 0);
            assert_eq!(state.disconnects, 0);
        }
    }

    impl ControlConnector for ScriptConnector {
        type Session = ScriptSession;

        async fn connect(
            &self,
            target: &OwnedControlTarget,
            _deadline: Instant,
        ) -> Result<Self::Session, MysqlTopologyError> {
            let mut state = self.state.lock().unwrap();
            state.selected_socket = Some(target.socket.as_path().to_path_buf());
            assert_eq!(state.selected_socket.as_ref(), Some(&state.expected_socket));
            drop(state);
            if let Some(error) = &self.connect_error {
                return Err(error.clone());
            }
            Ok(ScriptSession {
                state: Arc::clone(&self.state),
            })
        }
    }

    impl ControlSession for ScriptSession {
        async fn execute(
            &mut self,
            sql: &str,
            _deadline: Instant,
            stage: ControlStage,
        ) -> Result<(), MysqlTopologyError> {
            let mut state = self.state.lock().unwrap();
            let Some(ScriptAction::Execute {
                stage: expected_stage,
                sql: expected_sql,
                result,
            }) = state.actions.pop_front()
            else {
                panic!("expected scripted execute at {stage:?}: {sql}");
            };
            assert_eq!(stage, expected_stage);
            assert_eq!(sql, expected_sql);
            if result.is_ok() && stage == ControlStage::StartGroupReplication {
                state.start_effects += 1;
            }
            result
        }

        async fn query(
            &mut self,
            sql: &str,
            _deadline: Instant,
            stage: ControlStage,
        ) -> Result<ControlQueryResult, MysqlTopologyError> {
            let mut state = self.state.lock().unwrap();
            let Some(ScriptAction::Query {
                stage: expected_stage,
                sql: expected_sql,
                result,
            }) = state.actions.pop_front()
            else {
                panic!("expected scripted query at {stage:?}: {sql}");
            };
            assert_eq!(stage, expected_stage);
            assert_eq!(sql, expected_sql);
            result
        }

        async fn disconnect(self, _deadline: Instant) -> Result<(), MysqlTopologyError> {
            self.state.lock().unwrap().disconnects += 1;
            Ok(())
        }
    }

    struct SocketFixture {
        path: PathBuf,
        _listener: UnixListener,
    }

    impl SocketFixture {
        fn new(label: &str) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(1);
            let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
            let root = std::env::current_dir()
                .unwrap()
                .join("target")
                .join("cs")
                .join(format!(
                    "{}-{}-{sequence}",
                    &label[..label.len().min(3)],
                    std::process::id()
                ));
            fs::create_dir_all(&root).unwrap();
            let path = root.join("s");
            let listener = UnixListener::bind(&path).unwrap();
            Self {
                path,
                _listener: listener,
            }
        }
    }

    impl Drop for SocketFixture {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
            if let Some(parent) = self.path.parent() {
                let _ = fs::remove_dir(parent);
            }
        }
    }

    fn query_action(stage: ControlStage, sql: &str, result: ControlQueryResult) -> ScriptAction {
        ScriptAction::Query {
            stage,
            sql: sql.to_owned(),
            result: Ok(result),
        }
    }

    fn execute_action(stage: ControlStage, sql: impl Into<String>) -> ScriptAction {
        ScriptAction::Execute {
            stage,
            sql: sql.into(),
            result: Ok(()),
        }
    }

    fn failed_execute(
        stage: ControlStage,
        sql: impl Into<String>,
        error: MysqlTopologyError,
    ) -> ScriptAction {
        ScriptAction::Execute {
            stage,
            sql: sql.into(),
            result: Err(error),
        }
    }

    fn product(server_uuid: &str) -> ControlQueryResult {
        ControlQueryResult::Product {
            version: "8.4.11".to_owned(),
            comment: "MySQL Community Server - GPL".to_owned(),
            machine: "x86_64".to_owned(),
            operating_system: "Linux".to_owned(),
            server_uuid: server_uuid.to_owned(),
        }
    }

    fn product_query(server_uuid: &str) -> ScriptAction {
        query_action(
            ControlStage::ProductValidation,
            "SELECT @@version, @@version_comment, @@version_compile_machine, \
                 @@version_compile_os, @@GLOBAL.server_uuid",
            product(server_uuid),
        )
    }

    fn test_attempt() -> TopologyAttempt {
        TopologyAttempt::new(
            AttemptId::new("script-attempt").unwrap(),
            GroupName::new(GROUP_UUID).unwrap(),
            MysqlMemberIndex::First,
            Duration::from_nanos(100),
        )
        .unwrap()
    }

    fn test_topology() -> MysqlTopologyConfig {
        let seeds = [
            "127.0.0.1:34061".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:34062".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:34063".parse::<SocketAddr>().unwrap(),
        ];
        MysqlTopologyConfig::new([
            MysqlMemberConfig::new(
                1,
                "127.0.0.1:33061".parse().unwrap(),
                seeds[0],
                GROUP_UUID,
                seeds,
            )
            .unwrap(),
            MysqlMemberConfig::new(
                2,
                "127.0.0.1:33062".parse().unwrap(),
                seeds[1],
                GROUP_UUID,
                seeds,
            )
            .unwrap(),
            MysqlMemberConfig::new(
                3,
                "127.0.0.1:33063".parse().unwrap(),
                seeds[2],
                GROUP_UUID,
                seeds,
            )
            .unwrap(),
        ])
        .unwrap()
    }

    fn test_binding(attempt: &TopologyAttempt, member: MysqlMemberIndex) -> MemberControlBinding {
        let suffix = member.as_usize() + 1;
        MemberControlBinding::new(
            attempt.id().clone(),
            member,
            ProcessSessionId::new(format!("process-{suffix}")).unwrap(),
            EndpointBinding::new(format!("owned-uds-{suffix}")).unwrap(),
            StorageBinding::new(format!("storage-{suffix}")).unwrap(),
            MemberAddress::new(format!("127.0.0.1:3306{suffix}")).unwrap(),
            CredentialGeneration::new("observer-generation").unwrap(),
            CredentialGeneration::new("recovery-generation").unwrap(),
        )
    }

    fn test_credentials(attempt: &TopologyAttempt) -> (ControlCredential, ControlCredential) {
        (
            ControlCredential::new(
                attempt.id().clone(),
                CredentialGeneration::new("observer-generation").unwrap(),
                ControlCredentialRole::Observer,
                "observer_local",
                "observer-secret",
            )
            .unwrap(),
            ControlCredential::new(
                attempt.id().clone(),
                CredentialGeneration::new("recovery-generation").unwrap(),
                ControlCredentialRole::Recovery,
                "recovery_local",
                "recovery-secret",
            )
            .unwrap(),
        )
    }

    fn test_accounts(
        attempt: &TopologyAttempt,
        member: MysqlMemberIndex,
    ) -> AccountProvisioningEvidence {
        let (observer, recovery) = test_credentials(attempt);
        AccountProvisioningEvidence::new(
            test_binding(attempt, member),
            &observer,
            &recovery,
            AccountProvisioningEvidence::required_steps().to_vec(),
        )
        .unwrap()
    }

    fn test_enrollment(
        attempt: &TopologyAttempt,
        member: MysqlMemberIndex,
    ) -> NativeIdentityEnrollment {
        NativeIdentityEnrollment::new(
            test_binding(attempt, member),
            ServerUuid::new(MEMBER_UUIDS[member.as_usize()]).unwrap(),
            &test_accounts(attempt, member),
            true,
            GtidSet::empty(),
            NativeIdentityEnrollment::required_steps().to_vec(),
        )
        .unwrap()
    }

    fn test_target(socket: &SocketFixture, member: MysqlMemberIndex) -> OwnedControlTarget {
        let topology = test_topology();
        OwnedControlTarget::new(
            UnixSocketPath::new(&socket.path).unwrap(),
            member,
            topology.members()[member.as_usize()].clone(),
        )
    }

    fn test_deadline(attempt: &TopologyAttempt) -> NativeControlDeadline {
        NativeControlDeadline::new(
            attempt.id().clone(),
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap()
    }

    fn bootstrap_capability() -> (
        TopologyAuthority,
        TopologyAttempt,
        NativeIdentityEnrollment,
        BootstrapCapability,
    ) {
        let attempt = test_attempt();
        let enrollment = test_enrollment(&attempt, MysqlMemberIndex::First);
        let mut authority = TopologyAuthority::new(attempt.clone());
        authority.register_enrollment(enrollment.clone()).unwrap();
        let capability = authority
            .authorize_bootstrap(MysqlMemberIndex::First, TopologyInstant::new(10))
            .unwrap();
        (authority, attempt, enrollment, capability)
    }

    fn join_capability() -> (
        TopologyAuthority,
        TopologyAttempt,
        NativeIdentityEnrollment,
        JoinCapability,
    ) {
        let attempt = test_attempt();
        let enrollments = [
            test_enrollment(&attempt, MysqlMemberIndex::First),
            test_enrollment(&attempt, MysqlMemberIndex::Second),
            test_enrollment(&attempt, MysqlMemberIndex::Third),
        ];
        let mut authority = TopologyAuthority::new(attempt.clone());
        for enrollment in &enrollments {
            authority.register_enrollment(enrollment.clone()).unwrap();
        }
        let bootstrap = authority
            .authorize_bootstrap(MysqlMemberIndex::First, TopologyInstant::new(10))
            .unwrap()
            .record_effect(BootstrapEffect::required_steps().to_vec())
            .unwrap()
            .bind_observation_attempt(AttemptId::new("bootstrap-observation").unwrap());
        let evidence = TopologyObservation::new(
            TopologyObservationStatus::Complete,
            AttemptId::new("bootstrap-observation").unwrap(),
            ObservedLocalBinding::from_enrollment(&enrollments[0]),
            attempt.group_name().clone(),
            ViewId::new("700:1").unwrap(),
            ViewId::new("700:1").unwrap(),
            vec![NativeMember::new(
                enrollments[0].member_id().clone(),
                enrollments[0].binding().member_address().clone(),
                MemberRole::Primary,
                MemberState::Online,
            )],
            GtidSet::from_str(&format!("{GROUP_UUID}:1")).unwrap(),
            TopologyInstant::new(20),
        );
        let credit = match authority
            .accept_bootstrap(&bootstrap, &evidence, TopologyInstant::new(21))
            .unwrap()
        {
            TransitionEvaluation::Accepted(credit) => credit,
            TransitionEvaluation::Pending => panic!("bootstrap should be accepted"),
        };
        let source_evidence = TopologyObservation::new(
            TopologyObservationStatus::Complete,
            AttemptId::new("join-source-observation").unwrap(),
            ObservedLocalBinding::from_enrollment(&enrollments[0]),
            attempt.group_name().clone(),
            ViewId::new("700:1").unwrap(),
            ViewId::new("700:1").unwrap(),
            vec![NativeMember::new(
                enrollments[0].member_id().clone(),
                enrollments[0].binding().member_address().clone(),
                MemberRole::Primary,
                MemberState::Online,
            )],
            GtidSet::from_str(&format!("{GROUP_UUID}:1-2")).unwrap(),
            TopologyInstant::new(25),
        );
        let boundary = credit
            .source_gtid_boundary(
                &attempt,
                &enrollments[0],
                &source_evidence,
                TopologyInstant::new(26),
            )
            .unwrap();
        let capability = authority
            .authorize_join(MysqlMemberIndex::Second, boundary, TopologyInstant::new(30))
            .unwrap();
        (authority, attempt, enrollments[1].clone(), capability)
    }

    fn assert_closed(authority: &TopologyAuthority, credits: u8) {
        assert_eq!(authority.lifecycle_credits(), credits);
        assert!(!authority.read_access_open());
        assert!(!authority.write_access_open());
    }

    #[tokio::test]
    async fn scripted_provisioning_uses_owned_uds_and_restores_binary_logging() {
        let socket = SocketFixture::new("accounts");
        let attempt = test_attempt();
        let binding = test_binding(&attempt, MysqlMemberIndex::First);
        let (observer, recovery) = test_credentials(&attempt);
        let connector = ScriptConnector::new(
            &socket.path,
            vec![
                product_query(MEMBER_UUIDS[0]),
                query_action(
                    ControlStage::ReadBinaryLogging,
                    "SELECT @@SESSION.sql_log_bin",
                    ControlQueryResult::Unsigned8(1),
                ),
                execute_action(
                    ControlStage::DisableBinaryLogging,
                    "SET SESSION sql_log_bin = OFF",
                ),
                execute_action(
                    ControlStage::CreateObserver,
                    "CREATE USER 'observer_local'@'localhost' IDENTIFIED BY 'observer-secret'",
                ),
                execute_action(
                    ControlStage::GrantObserverMembers,
                    "GRANT SELECT ON performance_schema.replication_group_members TO \
                         'observer_local'@'localhost'",
                ),
                execute_action(
                    ControlStage::GrantObserverStats,
                    "GRANT SELECT ON performance_schema.replication_group_member_stats TO \
                         'observer_local'@'localhost'",
                ),
                execute_action(
                    ControlStage::CreateRecovery,
                    "CREATE USER 'recovery_local'@'localhost' IDENTIFIED BY 'recovery-secret'",
                ),
                execute_action(
                    ControlStage::GrantRecovery,
                    "GRANT REPLICATION SLAVE, CONNECTION_ADMIN ON *.* TO \
                         'recovery_local'@'localhost'",
                ),
                execute_action(
                    ControlStage::RestoreBinaryLogging,
                    "SET SESSION sql_log_bin = ON",
                ),
                query_action(
                    ControlStage::ProveBinaryLoggingRestored,
                    "SELECT @@SESSION.sql_log_bin",
                    ControlQueryResult::Unsigned8(1),
                ),
            ],
        );
        let evidence = provision_accounts_with(
            &connector,
            test_target(&socket, MysqlMemberIndex::First),
            binding,
            &observer,
            &recovery,
            &test_deadline(&attempt),
        )
        .await
        .unwrap();
        assert_eq!(
            evidence.steps(),
            AccountProvisioningEvidence::required_steps()
        );
        connector.assert_complete(0, 1);
    }

    #[tokio::test]
    async fn scripted_provisioning_restores_logging_after_primary_failure() {
        let socket = SocketFixture::new("account-cleanup");
        let attempt = test_attempt();
        let binding = test_binding(&attempt, MysqlMemberIndex::First);
        let (observer, recovery) = test_credentials(&attempt);
        let permission = MysqlTopologyError::ControlPermission(ControlStage::GrantRecovery);
        let connector = ScriptConnector::new(
            &socket.path,
            vec![
                product_query(MEMBER_UUIDS[0]),
                query_action(
                    ControlStage::ReadBinaryLogging,
                    "SELECT @@SESSION.sql_log_bin",
                    ControlQueryResult::Unsigned8(1),
                ),
                execute_action(
                    ControlStage::DisableBinaryLogging,
                    "SET SESSION sql_log_bin = OFF",
                ),
                execute_action(
                    ControlStage::CreateObserver,
                    "CREATE USER 'observer_local'@'localhost' IDENTIFIED BY 'observer-secret'",
                ),
                execute_action(
                    ControlStage::GrantObserverMembers,
                    "GRANT SELECT ON performance_schema.replication_group_members TO \
                         'observer_local'@'localhost'",
                ),
                execute_action(
                    ControlStage::GrantObserverStats,
                    "GRANT SELECT ON performance_schema.replication_group_member_stats TO \
                         'observer_local'@'localhost'",
                ),
                execute_action(
                    ControlStage::CreateRecovery,
                    "CREATE USER 'recovery_local'@'localhost' IDENTIFIED BY 'recovery-secret'",
                ),
                failed_execute(
                    ControlStage::GrantRecovery,
                    "GRANT REPLICATION SLAVE, CONNECTION_ADMIN ON *.* TO \
                         'recovery_local'@'localhost'",
                    permission.clone(),
                ),
                execute_action(
                    ControlStage::RestoreBinaryLogging,
                    "SET SESSION sql_log_bin = ON",
                ),
                query_action(
                    ControlStage::ProveBinaryLoggingRestored,
                    "SELECT @@SESSION.sql_log_bin",
                    ControlQueryResult::Unsigned8(1),
                ),
            ],
        );
        let error = provision_accounts_with(
            &connector,
            test_target(&socket, MysqlMemberIndex::First),
            binding,
            &observer,
            &recovery,
            &test_deadline(&attempt),
        )
        .await
        .unwrap_err();
        assert_eq!(error, permission);
        assert!(!format!("{error:?}").contains("recovery-secret"));
        connector.assert_complete(0, 1);
    }

    #[tokio::test]
    async fn scripted_enrollment_checks_membership_then_exact_empty_history() {
        for (history, expected) in [
            (String::new(), Ok(())),
            (
                format!("{GROUP_UUID}:1"),
                Err(MysqlTopologyError::TopologyState(
                    TopologyStateError::ExistingTransactionHistory,
                )),
            ),
        ] {
            let socket = SocketFixture::new("enrollment");
            let attempt = test_attempt();
            let binding = test_binding(&attempt, MysqlMemberIndex::First);
            let accounts = test_accounts(&attempt, MysqlMemberIndex::First);
            let fresh = history.is_empty();
            let mut actions = vec![
                product_query(MEMBER_UUIDS[0]),
                query_action(
                    ControlStage::InspectExistingGroup,
                    "SELECT COUNT(*) FROM performance_schema.replication_group_members \
                         WHERE MEMBER_ID IS NOT NULL AND MEMBER_ID <> ''",
                    ControlQueryResult::Unsigned64(0),
                ),
                query_action(
                    ControlStage::InspectExistingHistory,
                    "SELECT @@GLOBAL.gtid_executed",
                    ControlQueryResult::Text(history),
                ),
            ];
            if fresh {
                actions.push(query_action(
                    ControlStage::EnrollIdentity,
                    "SELECT @@GLOBAL.server_uuid",
                    ControlQueryResult::Text(MEMBER_UUIDS[0].to_owned()),
                ));
            }
            let connector = ScriptConnector::new(&socket.path, actions);
            let result = enroll_identity_with(
                &connector,
                test_target(&socket, MysqlMemberIndex::First),
                binding,
                &accounts,
                &test_deadline(&attempt),
            )
            .await
            .map(|_| ());
            assert_eq!(result, expected);
            let authority = TopologyAuthority::new(attempt);
            assert_closed(&authority, 0);
            connector.assert_complete(0, 1);
        }

        let socket = SocketFixture::new("existing-group");
        let attempt = test_attempt();
        let binding = test_binding(&attempt, MysqlMemberIndex::First);
        let accounts = test_accounts(&attempt, MysqlMemberIndex::First);
        let connector = ScriptConnector::new(
            &socket.path,
            vec![
                product_query(MEMBER_UUIDS[0]),
                query_action(
                    ControlStage::InspectExistingGroup,
                    "SELECT COUNT(*) FROM performance_schema.replication_group_members \
                         WHERE MEMBER_ID IS NOT NULL AND MEMBER_ID <> ''",
                    ControlQueryResult::Unsigned64(1),
                ),
            ],
        );
        assert_eq!(
            enroll_identity_with(
                &connector,
                test_target(&socket, MysqlMemberIndex::First),
                binding,
                &accounts,
                &test_deadline(&attempt),
            )
            .await,
            Err(MysqlTopologyError::TopologyState(
                TopologyStateError::ExistingGroupState
            ))
        );
        let authority = TopologyAuthority::new(attempt);
        assert_closed(&authority, 0);
        connector.assert_complete(0, 1);

        let socket = SocketFixture::new("wrong-enrolled-uuid");
        let attempt = test_attempt();
        let binding = test_binding(&attempt, MysqlMemberIndex::First);
        let accounts = test_accounts(&attempt, MysqlMemberIndex::First);
        let connector = ScriptConnector::new(
            &socket.path,
            vec![
                product_query(MEMBER_UUIDS[0]),
                query_action(
                    ControlStage::InspectExistingGroup,
                    "SELECT COUNT(*) FROM performance_schema.replication_group_members \
                         WHERE MEMBER_ID IS NOT NULL AND MEMBER_ID <> ''",
                    ControlQueryResult::Unsigned64(0),
                ),
                query_action(
                    ControlStage::InspectExistingHistory,
                    "SELECT @@GLOBAL.gtid_executed",
                    ControlQueryResult::Text(String::new()),
                ),
                query_action(
                    ControlStage::EnrollIdentity,
                    "SELECT @@GLOBAL.server_uuid",
                    ControlQueryResult::Text(MEMBER_UUIDS[1].to_owned()),
                ),
            ],
        );
        assert_eq!(
            enroll_identity_with(
                &connector,
                test_target(&socket, MysqlMemberIndex::First),
                binding,
                &accounts,
                &test_deadline(&attempt),
            )
            .await,
            Err(MysqlTopologyError::Evidence(
                TopologyEvidenceError::IdentityDrift
            ))
        );
        let authority = TopologyAuthority::new(attempt);
        assert_closed(&authority, 0);
        connector.assert_complete(0, 1);
    }

    #[tokio::test]
    async fn scripted_bootstrap_enforces_principal_and_always_cleans_up() {
        let socket = SocketFixture::new("bootstrap");
        let (authority, attempt, enrollment, capability) = bootstrap_capability();
        let (_, recovery) = test_credentials(&attempt);
        let start = "START GROUP_REPLICATION USER='recovery_local', PASSWORD='recovery-secret', \
                         DEFAULT_AUTH='caching_sha2_password'";
        let connector = ScriptConnector::new(
            &socket.path,
            vec![
                product_query(MEMBER_UUIDS[0]),
                execute_action(
                    ControlStage::EnableBootstrap,
                    "SET GLOBAL group_replication_bootstrap_group = ON",
                ),
                execute_action(ControlStage::StartGroupReplication, start),
                execute_action(
                    ControlStage::DisableBootstrap,
                    "SET GLOBAL group_replication_bootstrap_group = OFF",
                ),
                query_action(
                    ControlStage::ProveBootstrapDisabled,
                    "SELECT @@GLOBAL.group_replication_bootstrap_group",
                    ControlQueryResult::Unsigned8(0),
                ),
            ],
        );
        let effect = bootstrap_with(
            &connector,
            test_target(&socket, MysqlMemberIndex::First),
            capability,
            &recovery,
            &test_deadline(&attempt),
        )
        .await
        .unwrap();
        assert_eq!(effect.steps(), BootstrapEffect::required_steps());
        assert_closed(&authority, 0);
        connector.assert_complete(1, 1);

        let socket = SocketFixture::new("bootstrap-ambiguous-enable");
        let (authority, attempt, _enrollment, capability) = bootstrap_capability();
        let (_, recovery) = test_credentials(&attempt);
        let ambiguous = MysqlTopologyError::ControlTransport(ControlStage::EnableBootstrap);
        let connector = ScriptConnector::new(
            &socket.path,
            vec![
                product_query(MEMBER_UUIDS[0]),
                failed_execute(
                    ControlStage::EnableBootstrap,
                    "SET GLOBAL group_replication_bootstrap_group = ON",
                    ambiguous.clone(),
                ),
                execute_action(
                    ControlStage::DisableBootstrap,
                    "SET GLOBAL group_replication_bootstrap_group = OFF",
                ),
                query_action(
                    ControlStage::ProveBootstrapDisabled,
                    "SELECT @@GLOBAL.group_replication_bootstrap_group",
                    ControlQueryResult::Unsigned8(0),
                ),
            ],
        );
        assert_eq!(
            bootstrap_with(
                &connector,
                test_target(&socket, MysqlMemberIndex::First),
                capability,
                &recovery,
                &test_deadline(&attempt),
            )
            .await,
            Err(ambiguous)
        );
        assert_closed(&authority, 0);
        connector.assert_complete(0, 1);

        let socket = SocketFixture::new("bootstrap-wrong-user");
        let (authority, attempt, _enrollment, capability) = bootstrap_capability();
        let wrong = ControlCredential::new(
            attempt.id().clone(),
            CredentialGeneration::new("recovery-generation").unwrap(),
            ControlCredentialRole::Recovery,
            "other_recovery",
            "recovery-secret",
        )
        .unwrap();
        let connector = ScriptConnector::new(&socket.path, Vec::new());
        assert_eq!(
            bootstrap_with(
                &connector,
                test_target(&socket, MysqlMemberIndex::First),
                capability,
                &wrong,
                &test_deadline(&attempt),
            )
            .await,
            Err(MysqlTopologyError::Authority(
                TopologyAuthorityError::PrincipalMismatch
            ))
        );
        assert_eq!(enrollment.recovery_username(), "recovery_local");
        assert_closed(&authority, 0);
        connector.assert_rejected_before_connect();

        let socket = SocketFixture::new("bootstrap-paired");
        let (authority, attempt, _enrollment, capability) = bootstrap_capability();
        let (_, recovery) = test_credentials(&attempt);
        let primary = MysqlTopologyError::ControlPermission(ControlStage::StartGroupReplication);
        let cleanup = MysqlTopologyError::ControlTransport(ControlStage::DisableBootstrap);
        let connector = ScriptConnector::new(
            &socket.path,
            vec![
                product_query(MEMBER_UUIDS[0]),
                execute_action(
                    ControlStage::EnableBootstrap,
                    "SET GLOBAL group_replication_bootstrap_group = ON",
                ),
                failed_execute(ControlStage::StartGroupReplication, start, primary.clone()),
                failed_execute(
                    ControlStage::DisableBootstrap,
                    "SET GLOBAL group_replication_bootstrap_group = OFF",
                    cleanup.clone(),
                ),
                query_action(
                    ControlStage::ProveBootstrapDisabled,
                    "SELECT @@GLOBAL.group_replication_bootstrap_group",
                    ControlQueryResult::Unsigned8(0),
                ),
            ],
        );
        let error = bootstrap_with(
            &connector,
            test_target(&socket, MysqlMemberIndex::First),
            capability,
            &recovery,
            &test_deadline(&attempt),
        )
        .await
        .unwrap_err();
        assert_eq!(
            error,
            MysqlTopologyError::PairedControl {
                prior: Box::new(primary),
                cleanup: Box::new(cleanup),
            }
        );
        assert!(!format!("{error:?}").contains("recovery-secret"));
        assert_closed(&authority, 0);
        connector.assert_complete(0, 1);
    }

    #[tokio::test]
    async fn scripted_join_is_non_bootstrap_and_wrong_username_has_no_effect() {
        let socket = SocketFixture::new("join");
        let (authority, attempt, _target_enrollment, capability) = join_capability();
        let (_, recovery) = test_credentials(&attempt);
        let start = "START GROUP_REPLICATION USER='recovery_local', PASSWORD='recovery-secret', \
                         DEFAULT_AUTH='caching_sha2_password'";
        let connector = ScriptConnector::new(
            &socket.path,
            vec![
                product_query(MEMBER_UUIDS[1]),
                query_action(
                    ControlStage::ProveBootstrapDisabled,
                    "SELECT @@GLOBAL.group_replication_bootstrap_group",
                    ControlQueryResult::Unsigned8(0),
                ),
                execute_action(ControlStage::StartGroupReplication, start),
            ],
        );
        join_with(
            &connector,
            test_target(&socket, MysqlMemberIndex::Second),
            capability,
            &recovery,
            &test_deadline(&attempt),
        )
        .await
        .unwrap();
        assert_closed(&authority, 1);
        connector.assert_complete(1, 1);

        let socket = SocketFixture::new("join-wrong-user");
        let (authority, attempt, _target_enrollment, capability) = join_capability();
        let wrong = ControlCredential::new(
            attempt.id().clone(),
            CredentialGeneration::new("recovery-generation").unwrap(),
            ControlCredentialRole::Recovery,
            "other_recovery",
            "recovery-secret",
        )
        .unwrap();
        let connector = ScriptConnector::new(&socket.path, Vec::new());
        assert_eq!(
            join_with(
                &connector,
                test_target(&socket, MysqlMemberIndex::Second),
                capability,
                &wrong,
                &test_deadline(&attempt),
            )
            .await,
            Err(MysqlTopologyError::Authority(
                TopologyAuthorityError::PrincipalMismatch
            ))
        );
        assert_closed(&authority, 1);
        connector.assert_rejected_before_connect();
    }

    #[tokio::test]
    async fn scripted_control_failure_classes_are_distinct_secret_free_and_effect_free() {
        let cases = [
            MysqlTopologyError::ControlTransport(ControlStage::Connect),
            MysqlTopologyError::ControlAuthentication(ControlStage::Authenticate),
        ];
        for error in cases {
            let socket = SocketFixture::new("connect-failure");
            let attempt = test_attempt();
            let binding = test_binding(&attempt, MysqlMemberIndex::First);
            let accounts = test_accounts(&attempt, MysqlMemberIndex::First);
            let connector = ScriptConnector::failing(&socket.path, error.clone());
            let actual = enroll_identity_with(
                &connector,
                test_target(&socket, MysqlMemberIndex::First),
                binding,
                &accounts,
                &test_deadline(&attempt),
            )
            .await
            .unwrap_err();
            assert_eq!(actual, error);
            assert!(!format!("{actual:?}").contains("fixture-secret"));
            connector.assert_complete(0, 0);
        }

        let socket = SocketFixture::new("product-failure");
        let attempt = test_attempt();
        let binding = test_binding(&attempt, MysqlMemberIndex::First);
        let accounts = test_accounts(&attempt, MysqlMemberIndex::First);
        let connector = ScriptConnector::new(
            &socket.path,
            vec![query_action(
                ControlStage::ProductValidation,
                "SELECT @@version, @@version_comment, @@version_compile_machine, \
                     @@version_compile_os, @@GLOBAL.server_uuid",
                ControlQueryResult::Product {
                    version: "8.4.10".to_owned(),
                    comment: "MySQL Community Server - GPL".to_owned(),
                    machine: "x86_64".to_owned(),
                    operating_system: "Linux".to_owned(),
                    server_uuid: MEMBER_UUIDS[0].to_owned(),
                },
            )],
        );
        assert_eq!(
            enroll_identity_with(
                &connector,
                test_target(&socket, MysqlMemberIndex::First),
                binding,
                &accounts,
                &test_deadline(&attempt),
            )
            .await,
            Err(MysqlTopologyError::ControlProductCompatibility)
        );
        connector.assert_complete(0, 1);

        let socket = SocketFixture::new("permission-failure");
        let (authority, attempt, _enrollment, capability) = bootstrap_capability();
        let (_, recovery) = test_credentials(&attempt);
        let error = MysqlTopologyError::ControlPermission(ControlStage::StartGroupReplication);
        let start = "START GROUP_REPLICATION USER='recovery_local', PASSWORD='recovery-secret', \
                         DEFAULT_AUTH='caching_sha2_password'";
        let connector = ScriptConnector::new(
            &socket.path,
            vec![
                product_query(MEMBER_UUIDS[0]),
                execute_action(
                    ControlStage::EnableBootstrap,
                    "SET GLOBAL group_replication_bootstrap_group = ON",
                ),
                failed_execute(ControlStage::StartGroupReplication, start, error.clone()),
                execute_action(
                    ControlStage::DisableBootstrap,
                    "SET GLOBAL group_replication_bootstrap_group = OFF",
                ),
                query_action(
                    ControlStage::ProveBootstrapDisabled,
                    "SELECT @@GLOBAL.group_replication_bootstrap_group",
                    ControlQueryResult::Unsigned8(0),
                ),
            ],
        );
        let actual = bootstrap_with(
            &connector,
            test_target(&socket, MysqlMemberIndex::First),
            capability,
            &recovery,
            &test_deadline(&attempt),
        )
        .await
        .unwrap_err();
        assert_eq!(actual, error);
        assert!(!format!("{actual:?}").contains("recovery-secret"));
        assert_closed(&authority, 0);
        connector.assert_complete(0, 1);
    }

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
