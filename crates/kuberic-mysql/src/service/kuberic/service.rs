use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kuberic_runtime::application::{OpenContext, OpenMode, RoleChange, StateProvider};
use kuberic_runtime::host::ApplicationStorageState;
use kuberic_runtime::protocol::types::{
    ProcessSessionId, ReplicaIdentity, ReplicaRole, ReplicaStorageIncarnationContext,
};
use kuberic_runtime::replicator::{Replicator, ReplicatorSettings};
use kuberic_runtime::{Result, RuntimeError, StatefulServiceReplica};
use tokio::sync::Notify;

use crate::core::{ConfiguredGtidLineage, GroupReplicationAddress};
use crate::service::{
    AttemptContext, MysqlFreshMemberCredentials, MysqlFreshMemberInput, MysqlInstanceConfig,
    MysqlReplicator,
};

use super::{MysqlPublicV1Replicator, MysqlPublicV1ReplicatorFactory, translate_replica_identity};

struct FreshServiceInput {
    instance: MysqlInstanceConfig,
    lineage: ConfiguredGtidLineage,
    credentials: MysqlFreshMemberCredentials,
    attempt: AttemptContext,
    access_generation: NonZeroU64,
    group_replication_address: GroupReplicationAddress,
}

/// Validated one-use inputs for one fresh Public V1 MySQL member.
pub struct MysqlPublicV1ServiceConfig {
    expected_partition_id: String,
    fresh: FreshServiceInput,
    proxy_listen_address: SocketAddr,
}

impl MysqlPublicV1ServiceConfig {
    /// Validates exact partition authority and a fixed nonzero loopback proxy.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        expected_partition_id: impl Into<String>,
        instance: MysqlInstanceConfig,
        lineage: ConfiguredGtidLineage,
        credentials: MysqlFreshMemberCredentials,
        attempt: AttemptContext,
        access_generation: NonZeroU64,
        proxy_listen_address: SocketAddr,
    ) -> Result<Self> {
        let expected_partition_id = expected_partition_id.into();
        let instance = instance.with_build_recovery_networking();
        validate_service_bindings(&expected_partition_id, proxy_listen_address)?;
        if instance.topology().is_none() {
            return Err(RuntimeError::InvalidReplication(
                "MySQL Public V1 requires an explicit topology member configuration".into(),
            ));
        }
        let group_replication_address =
            GroupReplicationAddress::new(instance.member().group_replication_address().to_string())
                .map_err(|error| RuntimeError::InvalidReplication(error.to_string()))?;
        Ok(Self {
            expected_partition_id,
            fresh: FreshServiceInput {
                instance,
                lineage,
                credentials,
                attempt,
                access_generation,
                group_replication_address,
            },
            proxy_listen_address,
        })
    }

    /// Exact Kuberic partition string admitted by service Open.
    #[must_use]
    pub fn expected_partition_id(&self) -> &str {
        &self.expected_partition_id
    }

    /// Predetermined application endpoint returned by Primary role callbacks.
    #[must_use]
    pub const fn proxy_listen_address(&self) -> SocketAddr {
        self.proxy_listen_address
    }

    /// Opens one direct custom-trait fixture without a hosted controller.
    ///
    /// This is the PostgreSQL-parity qualification seam and makes no
    /// production-controller integration claim.
    #[doc(hidden)]
    pub async fn open_direct_for_qualification(
        self,
        identity: ReplicaIdentity,
        process_session: ProcessSessionId,
        storage: ReplicaStorageIncarnationContext,
    ) -> Result<Arc<MysqlPublicV1Replicator>> {
        let native_identity = translate_replica_identity(&identity)
            .map_err(|error| RuntimeError::InvalidReplication(error.to_string()))?;
        let fresh = self.fresh;
        let attempt = fresh.attempt.clone();
        let group_replication_address = fresh.group_replication_address;
        let input = MysqlFreshMemberInput::new(
            fresh.instance,
            native_identity,
            fresh.lineage,
            fresh.credentials,
            attempt.clone(),
            fresh.access_generation,
        );
        let (engine, receipt) = MysqlReplicator::prepare_fresh_member(input)
            .await
            .map_err(|error| RuntimeError::Application(error.to_string()))?;
        let driver = MysqlPublicV1Replicator::new_fresh_open(
            engine,
            receipt,
            attempt,
            group_replication_address,
            self.proxy_listen_address,
        )?;
        driver
            .bind_direct_public_context(identity, process_session, storage)
            .await?;
        Replicator::open(driver.as_ref()).await?;
        Ok(driver)
    }

    /// Classifies the configured roots without creating or opening MySQL state.
    pub fn storage_state(
        config: &MysqlPublicV1ServiceConfig,
    ) -> std::io::Result<ApplicationStorageState> {
        if is_empty(config.instance().data_root())?
            && is_empty(config.instance().runtime().scratch_root())?
        {
            Ok(ApplicationStorageState::FreshEmpty)
        } else {
            Ok(ApplicationStorageState::Established)
        }
    }

    /// Exact fresh MySQL process configuration.
    #[must_use]
    pub const fn instance(&self) -> &MysqlInstanceConfig {
        &self.fresh.instance
    }

    /// Shared attempt context transferred into the fresh member.
    #[must_use]
    pub const fn attempt(&self) -> &AttemptContext {
        &self.fresh.attempt
    }

    /// Initial closed application-access generation.
    #[must_use]
    pub const fn access_generation(&self) -> NonZeroU64 {
        self.fresh.access_generation
    }
}

struct ServiceState {
    fresh: Option<FreshServiceInput>,
    driver: Option<Arc<MysqlPublicV1Replicator>>,
    opening_attempt: Option<AttemptContext>,
    opening: bool,
    closed: bool,
}

/// Fresh-only Kuberic Public V1 application service.
pub struct MysqlPublicV1Service {
    expected_partition_id: String,
    proxy_listen_address: SocketAddr,
    state: Mutex<ServiceState>,
    opening_settled: Notify,
}

struct OpenSettlementGuard {
    service: Arc<MysqlPublicV1Service>,
    armed: bool,
}

impl OpenSettlementGuard {
    fn new(service: Arc<MysqlPublicV1Service>) -> Self {
        Self {
            service,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for OpenSettlementGuard {
    fn drop(&mut self) {
        if self.armed {
            self.service.settle_dropped_open();
        }
    }
}

impl MysqlPublicV1Service {
    /// Stores one fresh input without creating, opening, or classifying MySQL state.
    #[must_use]
    pub fn new(config: MysqlPublicV1ServiceConfig) -> Self {
        Self {
            expected_partition_id: config.expected_partition_id,
            proxy_listen_address: config.proxy_listen_address,
            state: Mutex::new(ServiceState {
                fresh: Some(config.fresh),
                driver: None,
                opening_attempt: None,
                opening: false,
                closed: false,
            }),
            opening_settled: Notify::new(),
        }
    }

    /// Exact Kuberic partition string admitted by service Open.
    #[must_use]
    pub fn expected_partition_id(&self) -> &str {
        &self.expected_partition_id
    }

    /// Predetermined fixed application endpoint.
    #[must_use]
    pub const fn proxy_listen_address(&self) -> SocketAddr {
        self.proxy_listen_address
    }

    fn driver(&self) -> Result<Arc<MysqlPublicV1Replicator>> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return Err(RuntimeError::Closed);
        }
        state.driver.clone().ok_or(RuntimeError::NotOpen)
    }

    fn begin_open(&self) -> Result<FreshServiceInput> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed || state.opening || state.driver.is_some() {
            return Err(RuntimeError::Closed);
        }
        let fresh = state.fresh.take().ok_or(RuntimeError::Closed)?;
        state.opening_attempt = Some(fresh.attempt.clone());
        state.opening = true;
        Ok(fresh)
    }

    fn retain_driver(&self, driver: Arc<MysqlPublicV1Replicator>) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return false;
        }
        debug_assert!(state.driver.is_none());
        state.driver = Some(driver);
        true
    }

    fn finish_successful_open(&self) -> bool {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed {
                return false;
            }
            state.opening = false;
            state.opening_attempt = None;
        }
        self.opening_settled.notify_waiters();
        true
    }

    fn finish_failed_open(&self) {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.opening = false;
            state.opening_attempt = None;
            state.closed = true;
        }
        self.opening_settled.notify_waiters();
    }

    fn settle_dropped_open(&self) {
        let driver = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Self::settle_dropped_open_state(&mut state)
        };
        if let Some(driver) = driver {
            driver.abort();
        }
        self.opening_settled.notify_waiters();
    }

    fn abort_driver_after(
        &self,
        driver: &MysqlPublicV1Replicator,
        primary: RuntimeError,
    ) -> RuntimeError {
        driver.abort();
        match driver.terminal_diagnostic() {
            Some(cleanup) => RuntimeError::Application(format!(
                "{primary}; MySQL post-preparation cleanup: {cleanup}"
            )),
            None => primary,
        }
    }

    fn settle_dropped_open_state(state: &mut ServiceState) -> Option<Arc<MysqlPublicV1Replicator>> {
        if !state.opening {
            return None;
        }
        state.closed = true;
        state.opening = false;
        state.fresh.take();
        if let Some(attempt) = state.opening_attempt.take() {
            attempt.cancel();
        }
        state.driver.take()
    }
}

#[async_trait]
impl StatefulServiceReplica for MysqlPublicV1Service {
    async fn open(self: Arc<Self>, context: OpenContext) -> Result<Arc<dyn Replicator>> {
        if context.mode != OpenMode::New {
            return Err(RuntimeError::InvalidReplication(
                "MySQL Public V1 restart adoption is unsupported".into(),
            ));
        }
        if context
            .partition
            .get_partition_information()
            .partition_id
            .as_str()
            != self.expected_partition_id
        {
            return Err(RuntimeError::AuthorityNotAdmitted);
        }
        let identity = translate_replica_identity(&context.identity)
            .map_err(|error| RuntimeError::InvalidReplication(error.to_string()))?;
        let fresh = self.begin_open()?;
        let mut settlement = OpenSettlementGuard::new(self.clone());
        let attempt = fresh.attempt.clone();
        let group_replication_address = fresh.group_replication_address.clone();
        let input = MysqlFreshMemberInput::new(
            fresh.instance,
            identity,
            fresh.lineage,
            fresh.credentials,
            attempt.clone(),
            fresh.access_generation,
        );
        let (engine, receipt) = match MysqlReplicator::prepare_fresh_member(input).await {
            Ok(prepared) => prepared,
            Err(error) => {
                self.finish_failed_open();
                settlement.disarm();
                return Err(RuntimeError::Application(error.to_string()));
            }
        };
        let driver = match MysqlPublicV1Replicator::new_fresh_open(
            engine,
            receipt,
            attempt,
            group_replication_address,
            self.proxy_listen_address,
        ) {
            Ok(driver) => driver,
            Err(error) => {
                self.finish_failed_open();
                settlement.disarm();
                return Err(error);
            }
        };
        if !self.retain_driver(driver.clone()) {
            let error = self.abort_driver_after(&driver, RuntimeError::Closed);
            self.finish_failed_open();
            settlement.disarm();
            return Err(error);
        }
        let interfaces = match context
            .partition
            .with_factory(Arc::new(MysqlPublicV1ReplicatorFactory::new(
                driver.clone(),
            )))
            .create_replicator(
                Some(driver.clone() as Arc<dyn StateProvider>),
                Some(ReplicatorSettings {
                    replication_address: driver.replication_endpoint(),
                }),
            )
            .await
        {
            Ok(interfaces) => interfaces,
            Err(primary) => {
                let error = self.abort_driver_after(&driver, primary);
                self.finish_failed_open();
                settlement.disarm();
                return Err(error);
            }
        };
        let control = interfaces.replicator();
        if !self.finish_successful_open() {
            let error = self.abort_driver_after(&driver, RuntimeError::Closed);
            self.finish_failed_open();
            settlement.disarm();
            return Err(error);
        }
        settlement.disarm();
        Ok(control)
    }

    async fn change_role(&self, role: ReplicaRole) -> Result<RoleChange> {
        let driver = self.driver()?;
        if role != ReplicaRole::Primary {
            driver.revoke_client_proxy().await?;
            return Ok(RoleChange {
                service_address: None,
            });
        }
        Ok(primary_role_change(self.proxy_listen_address))
    }

    async fn close(&self) -> Result<()> {
        loop {
            let notified = self.opening_settled.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (driver, opening) = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.closed = true;
                state.fresh.take();
                if let Some(attempt) = &state.opening_attempt {
                    attempt.cancel();
                }
                if state.opening {
                    (state.driver.clone(), true)
                } else {
                    (state.driver.take(), false)
                }
            };
            if opening {
                if let Some(driver) = driver {
                    driver.abort();
                }
                notified.await;
                continue;
            }
            if let Some(driver) = driver {
                return driver.close().await;
            }
            return Ok(());
        }
    }

    fn abort(&self) {
        let driver = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            state.fresh.take();
            if let Some(attempt) = state.opening_attempt.take() {
                attempt.cancel();
            }
            state.driver.clone()
        };
        if let Some(driver) = driver {
            driver.abort();
        }
    }
}

impl Drop for MysqlPublicV1Service {
    fn drop(&mut self) {
        self.abort();
    }
}

fn validate_service_bindings(
    expected_partition_id: &str,
    proxy_listen_address: SocketAddr,
) -> Result<()> {
    if expected_partition_id.is_empty() {
        return Err(RuntimeError::AuthorityNotAdmitted);
    }
    let noncanonical_ipv6 = match proxy_listen_address {
        SocketAddr::V4(_) => false,
        SocketAddr::V6(address) => address.flowinfo() != 0 || address.scope_id() != 0,
    };
    if !proxy_listen_address.ip().is_loopback()
        || proxy_listen_address.port() == 0
        || noncanonical_ipv6
    {
        return Err(RuntimeError::InvalidReplication(
            "MySQL Public V1 proxy address must be fixed, loopback, and nonzero".into(),
        ));
    }
    Ok(())
}

fn is_empty(path: &std::path::Path) -> std::io::Result<bool> {
    match std::fs::read_dir(path) {
        Ok(mut entries) => Ok(entries.next().transpose()?.is_none()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error),
    }
}

fn primary_role_change(endpoint: SocketAddr) -> RoleChange {
    RoleChange {
        service_address: Some(endpoint.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::AttemptId;
    use crate::service::AttemptCancellationState;
    use std::time::{Duration, Instant};

    #[test]
    fn service_bindings_reject_empty_partition_and_dynamic_or_public_proxy() {
        assert!(matches!(
            validate_service_bindings("", "127.0.0.1:3307".parse().unwrap()),
            Err(RuntimeError::AuthorityNotAdmitted)
        ));
        assert!(matches!(
            validate_service_bindings("mysql", "127.0.0.1:0".parse().unwrap()),
            Err(RuntimeError::InvalidReplication(_))
        ));
        assert!(matches!(
            validate_service_bindings("mysql", "0.0.0.0:3307".parse().unwrap()),
            Err(RuntimeError::InvalidReplication(_))
        ));
        assert!(matches!(
            validate_service_bindings(
                "mysql",
                SocketAddr::V6(std::net::SocketAddrV6::new(
                    std::net::Ipv6Addr::LOCALHOST,
                    3307,
                    1,
                    0,
                )),
            ),
            Err(RuntimeError::InvalidReplication(_))
        ));
        validate_service_bindings(" mysql ", "[::1]:3307".parse().unwrap()).unwrap();
    }

    #[test]
    fn primary_role_address_is_exact() {
        let endpoint: SocketAddr = "127.0.0.1:3307".parse().unwrap();
        assert_eq!(
            primary_role_change(endpoint).service_address.as_deref(),
            Some("127.0.0.1:3307")
        );
    }

    #[test]
    fn public_service_conforms_to_the_pinned_trait() {
        fn assert_service<T: StatefulServiceReplica + Send + Sync>() {}
        assert_service::<MysqlPublicV1Service>();
    }

    #[test]
    fn dropped_open_settlement_cancels_and_notifies_a_terminal_state_transition() {
        let attempt = AttemptContext::new(
            AttemptId::new("dropped-open").unwrap(),
            Instant::now() + Duration::from_secs(30),
            Duration::from_secs(10),
        )
        .unwrap();
        let mut state = ServiceState {
            fresh: None,
            driver: None,
            opening_attempt: Some(attempt.clone()),
            opening: true,
            closed: false,
        };

        assert!(MysqlPublicV1Service::settle_dropped_open_state(&mut state).is_none());
        assert!(!state.opening);
        assert!(state.closed);
        assert!(state.opening_attempt.is_none());
        assert_eq!(
            attempt.cancellation_state(),
            AttemptCancellationState::Cancelled
        );
    }
}
