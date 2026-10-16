use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::num::NonZeroU64;

use kuberic_runtime::protocol::public_operations::INVALID_LSN;
use kuberic_runtime::protocol::types::{
    Epoch, ProcessSessionId as KubericProcessSessionId, ReplicaIdentity, ReplicaRole,
    ReplicaStorageIdentity, ReplicaStorageIncarnationContext,
};
use kuberic_runtime::replicator::{ReplicaInformation, ReplicaSetConfiguration};

use crate::core::{
    AuthorityGeneration, ConfigurationId, EndpointBinding, GroupReplicationAddress, IdentityError,
    MemberAddress, MemberId, ProcessSessionId, ReplicaId, ReplicaIncarnation, StorageBinding,
};
use crate::service::{
    MemberPeer, MysqlOperationIdentity, MysqlReplicaConfiguration, MysqlReplicaEpoch,
    MysqlReplicaIdentity, MysqlReplicaRole, MysqlReplicaTarget,
};

const REPLICA_PREFIX: &str = "kuberic-public-v1-replica:";
const INCARNATION_PREFIX: &str = "kuberic-public-v1-incarnation:";
const STORAGE_PREFIX: &str = "kuberic-public-v1-storage:";
const ENDPOINT_PREFIX: &str = "kuberic-mysql-public-v1:";

/// Result returned by strict Kuberic Public V1 translation.
pub type TranslationResult<T> = Result<T, KubericTranslationError>;

/// Fail-closed errors produced before values reach the MySQL lifecycle engine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KubericTranslationError {
    /// A Kuberic replica number was not positive.
    InvalidReplicaId(i64),
    /// A Kuberic public value failed its own completeness contract.
    InvalidPublicValue(&'static str),
    /// A translated identity failed the MySQL identity constructor.
    InvalidMysqlIdentity(IdentityError),
    /// Storage belongs to a different replica incarnation.
    ForeignStorage,
    /// An epoch contained a negative component.
    InvalidEpoch {
        /// Rejected data-loss component.
        data_loss_number: i64,
        /// Rejected configuration component.
        configuration_number: i64,
    },
    /// An epoch preceded the latest retained Kuberic epoch.
    StaleEpoch {
        /// Latest retained data-loss component.
        latest_data_loss_number: i64,
        /// Latest retained configuration component.
        latest_configuration_number: i64,
        /// Rejected data-loss component.
        requested_data_loss_number: i64,
        /// Rejected configuration component.
        requested_configuration_number: i64,
    },
    /// No further monotonic MySQL epoch ordinal can be allocated.
    EpochExhausted,
    /// A retained epoch mapping used the reserved zero ordinal.
    InvalidRetainedEpoch,
    /// The Kuberic role cannot authorize a native MySQL role.
    UnsupportedRole(ReplicaRole),
    /// An opaque replication endpoint was malformed or non-canonical.
    MalformedEndpoint(&'static str),
    /// A progress value was invalid for an active configured replica.
    InvalidProgress {
        /// Public field that failed validation.
        field: &'static str,
        /// Rejected scalar value.
        value: i64,
    },
    /// Active replica information omitted its exact storage incarnation.
    MissingStorage,
    /// A replica-set configuration violated a strict structural invariant.
    InvalidConfiguration(&'static str),
    /// Two configured replicas reused a correctness fence.
    DuplicateConfigurationFence(&'static str),
    /// A callback key was retried with a different value.
    OperationConflict,
    /// No further nonzero callback operation generation can be allocated.
    OperationExhausted,
}

impl fmt::Display for KubericTranslationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidReplicaId(value) => {
                write!(
                    formatter,
                    "Kuberic replica id must be positive, got {value}"
                )
            }
            Self::InvalidPublicValue(message) => {
                write!(formatter, "invalid Kuberic Public V1 value: {message}")
            }
            Self::InvalidMysqlIdentity(error) => error.fmt(formatter),
            Self::ForeignStorage => {
                formatter.write_str("storage incarnation belongs to another replica")
            }
            Self::InvalidEpoch {
                data_loss_number,
                configuration_number,
            } => write!(
                formatter,
                "Kuberic epoch components must be nonnegative, got ({data_loss_number}, {configuration_number})"
            ),
            Self::StaleEpoch {
                latest_data_loss_number,
                latest_configuration_number,
                requested_data_loss_number,
                requested_configuration_number,
            } => write!(
                formatter,
                "stale Kuberic epoch ({requested_data_loss_number}, {requested_configuration_number}); latest is ({latest_data_loss_number}, {latest_configuration_number})"
            ),
            Self::EpochExhausted => {
                formatter.write_str("monotonic MySQL replica epoch space is exhausted")
            }
            Self::InvalidRetainedEpoch => {
                formatter.write_str("retained MySQL replica epoch must be nonzero")
            }
            Self::UnsupportedRole(role) => {
                write!(formatter, "Kuberic role {role:?} has no native MySQL role")
            }
            Self::MalformedEndpoint(message) => {
                write!(formatter, "malformed Kuberic MySQL endpoint: {message}")
            }
            Self::InvalidProgress { field, value } => {
                write!(formatter, "invalid {field} progress value {value}")
            }
            Self::MissingStorage => {
                formatter.write_str("active replica information lacks storage incarnation")
            }
            Self::InvalidConfiguration(message) => {
                write!(formatter, "invalid replica-set configuration: {message}")
            }
            Self::DuplicateConfigurationFence(fence) => {
                write!(formatter, "configured replicas reuse the {fence} fence")
            }
            Self::OperationConflict => {
                formatter.write_str("callback operation key was reused with a conflicting value")
            }
            Self::OperationExhausted => {
                formatter.write_str("callback operation generation space is exhausted")
            }
        }
    }
}

impl std::error::Error for KubericTranslationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidMysqlIdentity(error) => Some(error),
            _ => None,
        }
    }
}

impl From<IdentityError> for KubericTranslationError {
    fn from(error: IdentityError) -> Self {
        Self::InvalidMysqlIdentity(error)
    }
}

/// Native facts carried by the versioned non-secret replication address.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationEndpoint {
    member_id: MemberId,
    member_address: MemberAddress,
    group_replication_address: GroupReplicationAddress,
}

impl ReplicationEndpoint {
    /// Constructs an endpoint from already validated native identity values.
    #[must_use]
    pub const fn new(
        member_id: MemberId,
        member_address: MemberAddress,
        group_replication_address: GroupReplicationAddress,
    ) -> Self {
        Self {
            member_id,
            member_address,
            group_replication_address,
        }
    }

    /// Decodes an exact canonical Public V1 replication address.
    pub fn decode(encoded: &str) -> TranslationResult<Self> {
        let mut cursor = ENDPOINT_PREFIX.len();
        if !encoded.starts_with(ENDPOINT_PREFIX) {
            return Err(KubericTranslationError::MalformedEndpoint(
                "unsupported endpoint version",
            ));
        }
        let member_id = take_part(encoded, &mut cursor)?;
        let member_address = take_part(encoded, &mut cursor)?;
        let group_replication_address = take_part(encoded, &mut cursor)?;
        if cursor != encoded.len() {
            return Err(KubericTranslationError::MalformedEndpoint(
                "trailing endpoint data",
            ));
        }

        let endpoint = Self::new(
            MemberId::new(member_id)?,
            MemberAddress::new(member_address)?,
            GroupReplicationAddress::new(group_replication_address)?,
        );
        if endpoint.encode() != encoded {
            return Err(KubericTranslationError::MalformedEndpoint(
                "endpoint is not canonical",
            ));
        }
        endpoint.binding()?;
        Ok(endpoint)
    }

    /// Encodes the endpoint without credentials or process/storage authority.
    #[must_use]
    pub fn encode(&self) -> String {
        let mut encoded = String::from(ENDPOINT_PREFIX);
        push_part(&mut encoded, self.member_id.as_str());
        push_part(&mut encoded, self.member_address.as_str());
        push_part(&mut encoded, self.group_replication_address.as_str());
        encoded
    }

    /// Converts the complete opaque endpoint into its independent endpoint fence.
    pub fn binding(&self) -> TranslationResult<EndpointBinding> {
        EndpointBinding::new(self.encode()).map_err(Into::into)
    }

    /// Native Group Replication member UUID.
    #[must_use]
    pub const fn member_id(&self) -> &MemberId {
        &self.member_id
    }

    /// Native SQL member address.
    #[must_use]
    pub const fn member_address(&self) -> &MemberAddress {
        &self.member_address
    }

    /// Native Group Replication communication address.
    #[must_use]
    pub const fn group_replication_address(&self) -> &GroupReplicationAddress {
        &self.group_replication_address
    }
}

/// Retained lexicographic Kuberic epoch to monotonic MySQL epoch mapping.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EpochTranslator {
    latest: Option<(Epoch, MysqlReplicaEpoch)>,
}

impl EpochTranslator {
    /// Creates an empty translator whose first admitted epoch maps to ordinal one.
    #[must_use]
    pub const fn new() -> Self {
        Self { latest: None }
    }

    /// Restores an exact retained mapping.
    pub fn from_retained(epoch: Epoch, mapped: MysqlReplicaEpoch) -> TranslationResult<Self> {
        validate_epoch(epoch)?;
        if mapped.value() == 0 {
            return Err(KubericTranslationError::InvalidRetainedEpoch);
        }
        Ok(Self {
            latest: Some((epoch, mapped)),
        })
    }

    /// Maps an exact retry to the retained ordinal and a newer epoch to its successor.
    pub fn translate(&mut self, epoch: Epoch) -> TranslationResult<MysqlReplicaEpoch> {
        validate_epoch(epoch)?;
        let mapped = match self.latest {
            None => MysqlReplicaEpoch::new(1),
            Some((latest, mapped)) if epoch == latest => return Ok(mapped),
            Some((latest, _)) if epoch < latest => {
                return Err(KubericTranslationError::StaleEpoch {
                    latest_data_loss_number: latest.data_loss_number,
                    latest_configuration_number: latest.configuration_number,
                    requested_data_loss_number: epoch.data_loss_number,
                    requested_configuration_number: epoch.configuration_number,
                });
            }
            Some((_, mapped)) => MysqlReplicaEpoch::new(
                mapped
                    .value()
                    .checked_add(1)
                    .ok_or(KubericTranslationError::EpochExhausted)?,
            ),
        };
        self.latest = Some((epoch, mapped));
        Ok(mapped)
    }

    /// Latest retained mapping, if any.
    #[must_use]
    pub const fn latest(&self) -> Option<(Epoch, MysqlReplicaEpoch)> {
        self.latest
    }
}

/// Retained exact-retry ledger for callback values.
#[derive(Clone, Debug)]
pub struct CallbackOperationLedger<K, V> {
    authority: AuthorityGeneration,
    next_generation: Option<NonZeroU64>,
    entries: BTreeMap<K, (V, MysqlOperationIdentity)>,
}

impl<K, V> CallbackOperationLedger<K, V>
where
    K: Ord,
    V: Eq,
{
    /// Creates a ledger that allocates operation generations starting at one.
    #[must_use]
    pub fn new(authority: AuthorityGeneration) -> Self {
        Self::with_next_generation(authority, NonZeroU64::MIN)
    }

    /// Restores a ledger with the next unallocated generation.
    #[must_use]
    pub fn with_next_generation(
        authority: AuthorityGeneration,
        next_generation: NonZeroU64,
    ) -> Self {
        Self {
            authority,
            next_generation: Some(next_generation),
            entries: BTreeMap::new(),
        }
    }

    /// Returns the same identity for an exact retry and rejects conflicting reuse.
    pub fn admit(&mut self, key: K, value: V) -> TranslationResult<MysqlOperationIdentity> {
        if let Some((retained, identity)) = self.entries.get(&key) {
            return if retained == &value {
                Ok(identity.clone())
            } else {
                Err(KubericTranslationError::OperationConflict)
            };
        }

        let generation = self
            .next_generation
            .ok_or(KubericTranslationError::OperationExhausted)?;
        let identity = MysqlOperationIdentity::new(self.authority.clone(), generation);
        self.next_generation = generation.get().checked_add(1).and_then(NonZeroU64::new);
        self.entries.insert(key, (value, identity.clone()));
        Ok(identity)
    }

    /// Number of retained callback operations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no callback operation has been retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Translates a complete Kuberic replica authority identity.
pub fn translate_replica_identity(
    identity: &ReplicaIdentity,
) -> TranslationResult<MysqlReplicaIdentity> {
    validate_replica_identity(identity)?;
    let replica_id = ReplicaId::new(format!("{REPLICA_PREFIX}{}", identity.replica_id.value()))?;
    let mut incarnation = String::from(INCARNATION_PREFIX);
    push_part(&mut incarnation, identity.instance_id.as_str());
    push_part(&mut incarnation, identity.agent_generation.as_str());
    Ok(MysqlReplicaIdentity::new(
        replica_id,
        ReplicaIncarnation::new(incarnation)?,
    ))
}

/// Translates an exact Kuberic process-session fence without inventing a default.
pub fn translate_process_session_id(
    process_session: &KubericProcessSessionId,
) -> TranslationResult<ProcessSessionId> {
    ProcessSessionId::new(process_session.as_str()).map_err(Into::into)
}

/// Translates an exact Kuberic storage incarnation bound to the expected replica.
pub fn translate_storage_binding(
    storage: &ReplicaStorageIncarnationContext,
    expected_replica: &ReplicaIdentity,
) -> TranslationResult<StorageBinding> {
    validate_replica_identity(expected_replica)?;
    storage
        .validate()
        .map_err(KubericTranslationError::InvalidPublicValue)?;
    if &storage.replica != expected_replica {
        return Err(KubericTranslationError::ForeignStorage);
    }

    let mut encoded = String::from(STORAGE_PREFIX);
    push_part(
        &mut encoded,
        &expected_replica.replica_id.value().to_string(),
    );
    push_part(&mut encoded, expected_replica.instance_id.as_str());
    push_part(&mut encoded, expected_replica.agent_generation.as_str());
    match &storage.storage {
        ReplicaStorageIdentity::Persistent { pod_uid, pvc_uid } => {
            encoded.push('p');
            push_part(&mut encoded, pod_uid.as_str());
            push_part(&mut encoded, pvc_uid.as_str());
        }
        ReplicaStorageIdentity::Volatile { identity } => {
            encoded.push('v');
            push_part(&mut encoded, identity);
        }
    }
    push_part(&mut encoded, storage.storage_generation.as_str());
    StorageBinding::new(encoded).map_err(Into::into)
}

/// Maps only Kuberic roles that describe an active native member.
pub fn translate_role(role: ReplicaRole) -> TranslationResult<MysqlReplicaRole> {
    match role {
        ReplicaRole::Primary => Ok(MysqlReplicaRole::Primary),
        ReplicaRole::ActiveSecondary => Ok(MysqlReplicaRole::Secondary),
        ReplicaRole::IdleSecondary | ReplicaRole::None => {
            Err(KubericTranslationError::UnsupportedRole(role))
        }
    }
}

/// Translates active Kuberic replica information into independent MySQL fences.
pub fn translate_replica_information(
    information: &ReplicaInformation,
) -> TranslationResult<MysqlReplicaTarget> {
    translate_replica_information_parts(information).map(|(target, _)| target)
}

/// Translates a runtime-shaped remote-peer configuration plus enrolled local authority.
///
/// Kuberic Public V1 omits the local replica from `replicas`; the descriptor
/// still contains every member. The local target is therefore synthesized only
/// from the separately bound replica identity and the MySQL-owned enrolled
/// process/session/storage/endpoint authority.
pub fn translate_replica_set_configuration(
    configuration: &ReplicaSetConfiguration,
    epochs: &mut EpochTranslator,
    local_identity: &ReplicaIdentity,
    local_endpoint: &ReplicationEndpoint,
    local_target: &MysqlReplicaTarget,
) -> TranslationResult<MysqlReplicaConfiguration> {
    let descriptor = &configuration.configuration;
    if descriptor.configuration_id != descriptor.expected_id() {
        return Err(KubericTranslationError::InvalidConfiguration(
            "configuration id differs from canonical content",
        ));
    }
    if descriptor.members.is_empty() {
        return Err(KubericTranslationError::InvalidConfiguration(
            "configuration has no members",
        ));
    }
    if descriptor.write_quorum == 0 || descriptor.write_quorum as usize > descriptor.members.len() {
        return Err(KubericTranslationError::InvalidConfiguration(
            "write quorum is outside the member set",
        ));
    }
    let translated_local_identity = translate_replica_identity(local_identity)?;
    if local_target.identity() != &translated_local_identity
        || local_endpoint.member_id() != local_target.member().member_id()
        || local_endpoint.member_address() != local_target.member().address()
    {
        return Err(KubericTranslationError::InvalidConfiguration(
            "local public identity or endpoint differs from enrolled MySQL authority",
        ));
    }
    let local_members = descriptor
        .members
        .iter()
        .filter(|member| &member.identity == local_identity)
        .count();
    if local_members != 1 {
        return Err(KubericTranslationError::InvalidConfiguration(
            "configuration must contain the exact local replica once",
        ));
    }
    if configuration.replicas.len().saturating_add(1) != descriptor.members.len() {
        return Err(KubericTranslationError::InvalidConfiguration(
            "replica information does not exactly cover remote configured members",
        ));
    }

    let mut configured_ids = HashSet::new();
    let mut configured_identities = HashSet::new();
    let mut primary_count = 0;
    for member in &descriptor.members {
        validate_replica_identity(&member.identity)?;
        translate_role(member.role)?;
        if !configured_ids.insert(member.identity.replica_id.value()) {
            return Err(KubericTranslationError::InvalidConfiguration(
                "logical replica id is duplicated",
            ));
        }
        if !configured_identities.insert(member.identity.clone()) {
            return Err(KubericTranslationError::InvalidConfiguration(
                "replica incarnation is duplicated",
            ));
        }
        if member.role == ReplicaRole::Primary {
            primary_count += 1;
            if member.identity.replica_id != descriptor.primary_id {
                return Err(KubericTranslationError::InvalidConfiguration(
                    "Primary member differs from primary_id",
                ));
            }
        }
    }
    if primary_count != 1 {
        return Err(KubericTranslationError::InvalidConfiguration(
            "configuration must contain exactly one Primary",
        ));
    }

    let mut information_by_identity = BTreeMap::new();
    for information in &configuration.replicas {
        if &information.identity == local_identity {
            return Err(KubericTranslationError::InvalidConfiguration(
                "replica information must not repeat local authority",
            ));
        }
        if information_by_identity
            .insert(information.identity.clone(), information)
            .is_some()
        {
            return Err(KubericTranslationError::InvalidConfiguration(
                "replica information is duplicated",
            ));
        }
    }

    let mut native_member_ids = HashSet::new();
    let mut member_addresses = HashSet::new();
    let mut group_replication_addresses = HashSet::new();
    let mut process_sessions = HashSet::new();
    let mut endpoint_bindings = HashSet::new();
    let mut storage_bindings = HashSet::new();
    let mut targets = Vec::with_capacity(descriptor.members.len());

    require_unique(
        &mut native_member_ids,
        local_endpoint.member_id().as_str(),
        "native member id",
    )?;
    require_unique(
        &mut member_addresses,
        local_endpoint.member_address().as_str(),
        "SQL member address",
    )?;
    require_unique(
        &mut group_replication_addresses,
        local_endpoint.group_replication_address().as_str(),
        "Group Replication address",
    )?;
    require_unique(
        &mut process_sessions,
        local_target.process_session().as_str(),
        "process session",
    )?;
    require_unique(
        &mut endpoint_bindings,
        local_target.endpoint().as_str(),
        "endpoint",
    )?;
    require_unique(
        &mut storage_bindings,
        local_target.storage().as_str(),
        "storage",
    )?;

    for member in &descriptor.members {
        if &member.identity == local_identity {
            targets.push(local_target.clone());
            continue;
        }
        let information = information_by_identity.get(&member.identity).ok_or(
            KubericTranslationError::InvalidConfiguration(
                "configured remote member lacks replica information",
            ),
        )?;
        if information.role != member.role {
            return Err(KubericTranslationError::InvalidConfiguration(
                "replica role differs from configured member role",
            ));
        }
        let (target, endpoint) = translate_replica_information_parts(information)?;
        require_unique(
            &mut native_member_ids,
            endpoint.member_id().as_str(),
            "native member id",
        )?;
        require_unique(
            &mut member_addresses,
            endpoint.member_address().as_str(),
            "SQL member address",
        )?;
        require_unique(
            &mut group_replication_addresses,
            endpoint.group_replication_address().as_str(),
            "Group Replication address",
        )?;
        require_unique(
            &mut process_sessions,
            target.process_session().as_str(),
            "process session",
        )?;
        require_unique(
            &mut endpoint_bindings,
            target.endpoint().as_str(),
            "endpoint",
        )?;
        require_unique(&mut storage_bindings, target.storage().as_str(), "storage")?;
        targets.push(target);
    }

    let configuration_id = ConfigurationId::new(descriptor.configuration_id.as_str())
        .map_err(KubericTranslationError::from)?;
    let epoch = epochs.translate(descriptor.epoch)?;
    Ok(MysqlReplicaConfiguration::new(
        configuration_id,
        epoch,
        targets,
    ))
}

fn translate_replica_information_parts(
    information: &ReplicaInformation,
) -> TranslationResult<(MysqlReplicaTarget, ReplicationEndpoint)> {
    validate_replica_identity(&information.identity)?;
    translate_role(information.role)?;
    validate_progress("current", information.current_progress, false)?;
    validate_progress("catch-up capability", information.catch_up_capability, true)?;
    if information.must_catchup && information.catch_up_capability == INVALID_LSN {
        return Err(KubericTranslationError::InvalidProgress {
            field: "must-catchup capability",
            value: information.catch_up_capability,
        });
    }

    let process_session = translate_process_session_id(&information.process_session_id)?;
    let storage = translate_storage_binding(
        information
            .storage_incarnation
            .as_ref()
            .ok_or(KubericTranslationError::MissingStorage)?,
        &information.identity,
    )?;
    let endpoint = ReplicationEndpoint::decode(&information.replication_address)?;
    let target = MysqlReplicaTarget::new(
        translate_replica_identity(&information.identity)?,
        MemberPeer::new(
            endpoint.member_id().clone(),
            endpoint.member_address().clone(),
        ),
        process_session,
        endpoint.binding()?,
        storage,
    );
    Ok((target, endpoint))
}

fn validate_replica_identity(identity: &ReplicaIdentity) -> TranslationResult<()> {
    if identity.replica_id.value() <= 0 {
        return Err(KubericTranslationError::InvalidReplicaId(
            identity.replica_id.value(),
        ));
    }
    if identity.instance_id.is_empty() {
        return Err(KubericTranslationError::InvalidPublicValue(
            "replica instance id is empty",
        ));
    }
    if identity.agent_generation.is_empty() {
        return Err(KubericTranslationError::InvalidPublicValue(
            "agent generation is empty",
        ));
    }
    Ok(())
}

fn validate_epoch(epoch: Epoch) -> TranslationResult<()> {
    if epoch.data_loss_number < 0 || epoch.configuration_number < 0 {
        return Err(KubericTranslationError::InvalidEpoch {
            data_loss_number: epoch.data_loss_number,
            configuration_number: epoch.configuration_number,
        });
    }
    Ok(())
}

fn validate_progress(
    field: &'static str,
    value: i64,
    invalid_sentinel_allowed: bool,
) -> TranslationResult<()> {
    if value < INVALID_LSN || (!invalid_sentinel_allowed && value == INVALID_LSN) {
        return Err(KubericTranslationError::InvalidProgress { field, value });
    }
    Ok(())
}

fn require_unique(
    values: &mut HashSet<String>,
    value: &str,
    fence: &'static str,
) -> TranslationResult<()> {
    if values.insert(value.to_owned()) {
        Ok(())
    } else {
        Err(KubericTranslationError::DuplicateConfigurationFence(fence))
    }
}

fn push_part(encoded: &mut String, value: &str) {
    encoded.push_str(&value.len().to_string());
    encoded.push(':');
    encoded.push_str(value);
}

fn take_part<'a>(encoded: &'a str, cursor: &mut usize) -> TranslationResult<&'a str> {
    let remaining = encoded
        .get(*cursor..)
        .ok_or(KubericTranslationError::MalformedEndpoint(
            "field offset is not UTF-8 aligned",
        ))?;
    let separator = remaining
        .find(':')
        .ok_or(KubericTranslationError::MalformedEndpoint(
            "field length is missing",
        ))?;
    let length_text = &remaining[..separator];
    if length_text.is_empty()
        || !length_text.bytes().all(|byte| byte.is_ascii_digit())
        || (length_text.len() > 1 && length_text.starts_with('0'))
    {
        return Err(KubericTranslationError::MalformedEndpoint(
            "field length is not canonical",
        ));
    }
    let length = length_text
        .parse::<usize>()
        .map_err(|_| KubericTranslationError::MalformedEndpoint("field length overflows"))?;
    let start =
        (*cursor)
            .checked_add(separator + 1)
            .ok_or(KubericTranslationError::MalformedEndpoint(
                "field offset overflows",
            ))?;
    let end = start
        .checked_add(length)
        .ok_or(KubericTranslationError::MalformedEndpoint(
            "field extent overflows",
        ))?;
    let value = encoded
        .get(start..end)
        .ok_or(KubericTranslationError::MalformedEndpoint(
            "field is truncated or not UTF-8 aligned",
        ))?;
    *cursor = end;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kuberic_runtime::protocol::types::{
        AgentGeneration, ConfigurationDescriptor, ConfigurationMember, OperationId, PodUid, PvcUid,
        ReplicaId as KubericReplicaId, ReplicaInstanceId, StorageGeneration,
    };

    const MEMBER_ONE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    const MEMBER_TWO: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

    fn identity(id: i64) -> ReplicaIdentity {
        ReplicaIdentity {
            replica_id: KubericReplicaId::new(id),
            instance_id: ReplicaInstanceId::new(format!("instance-{id}")),
            agent_generation: AgentGeneration::new(format!("agent-{id}")),
        }
    }

    fn storage(replica: ReplicaIdentity) -> ReplicaStorageIncarnationContext {
        let id = replica.replica_id.value();
        ReplicaStorageIncarnationContext {
            replica,
            storage: ReplicaStorageIdentity::Persistent {
                pod_uid: PodUid::new(format!("pod-{id}")),
                pvc_uid: PvcUid::new(format!("pvc-{id}")),
            },
            storage_generation: StorageGeneration::new(format!("storage-{id}")),
        }
    }

    fn endpoint(member_id: &str, sql: &str, group_replication: &str) -> ReplicationEndpoint {
        ReplicationEndpoint::new(
            MemberId::new(member_id).unwrap(),
            MemberAddress::new(sql).unwrap(),
            GroupReplicationAddress::new(group_replication).unwrap(),
        )
    }

    fn information(
        id: i64,
        role: ReplicaRole,
        member_id: &str,
        sql: &str,
        group_replication: &str,
    ) -> ReplicaInformation {
        let identity = identity(id);
        ReplicaInformation {
            build_id: OperationId::new(format!("build-{id}")),
            identity: identity.clone(),
            replication_address: endpoint(member_id, sql, group_replication).encode(),
            process_session_id: KubericProcessSessionId::new(format!("session-{id}")),
            role,
            current_progress: id,
            catch_up_capability: 0,
            must_catchup: false,
            storage_incarnation: Some(storage(identity)),
        }
    }

    #[test]
    fn identity_session_and_storage_use_independent_exact_fences() {
        let replica = identity(7);
        let translated = translate_replica_identity(&replica).unwrap();
        assert_eq!(
            translated.replica_id().as_str(),
            "kuberic-public-v1-replica:7"
        );
        assert_ne!(
            translated.replica_id().as_str(),
            translated.incarnation().as_str()
        );

        let session =
            translate_process_session_id(&KubericProcessSessionId::new("session-7")).unwrap();
        let storage_binding =
            translate_storage_binding(&storage(replica.clone()), &replica).unwrap();
        assert_eq!(session.as_str(), "session-7");
        assert!(storage_binding.as_str().starts_with(STORAGE_PREFIX));

        let mut foreign = storage(replica.clone());
        foreign.replica = identity(8);
        assert_eq!(
            translate_storage_binding(&foreign, &replica),
            Err(KubericTranslationError::ForeignStorage)
        );
    }

    #[test]
    fn incomplete_public_identities_never_default() {
        let mut replica = identity(1);
        replica.replica_id = KubericReplicaId::new(0);
        assert_eq!(
            translate_replica_identity(&replica),
            Err(KubericTranslationError::InvalidReplicaId(0))
        );
        assert!(matches!(
            translate_process_session_id(&KubericProcessSessionId::new("")),
            Err(KubericTranslationError::InvalidMysqlIdentity(_))
        ));
    }

    #[test]
    fn epoch_mapping_is_lexicographic_retained_and_fail_closed() {
        let mut epochs = EpochTranslator::new();
        let first = epochs.translate(Epoch::new(0, 9)).unwrap();
        assert_eq!(first, MysqlReplicaEpoch::new(1));
        assert_eq!(epochs.translate(Epoch::new(0, 9)).unwrap(), first);
        assert_eq!(
            epochs.translate(Epoch::new(1, 0)).unwrap(),
            MysqlReplicaEpoch::new(2)
        );
        assert!(matches!(
            epochs.translate(Epoch::new(0, 10)),
            Err(KubericTranslationError::StaleEpoch { .. })
        ));
        assert!(matches!(
            epochs.translate(Epoch::new(-1, 0)),
            Err(KubericTranslationError::InvalidEpoch { .. })
        ));

        let mut exhausted =
            EpochTranslator::from_retained(Epoch::new(1, 1), MysqlReplicaEpoch::new(u64::MAX))
                .unwrap();
        assert_eq!(
            exhausted.translate(Epoch::new(1, 2)),
            Err(KubericTranslationError::EpochExhausted)
        );
    }

    #[test]
    fn native_roles_reject_idle_and_none() {
        assert_eq!(
            translate_role(ReplicaRole::Primary),
            Ok(MysqlReplicaRole::Primary)
        );
        assert_eq!(
            translate_role(ReplicaRole::ActiveSecondary),
            Ok(MysqlReplicaRole::Secondary)
        );
        assert_eq!(
            translate_role(ReplicaRole::IdleSecondary),
            Err(KubericTranslationError::UnsupportedRole(
                ReplicaRole::IdleSecondary
            ))
        );
        assert_eq!(
            translate_role(ReplicaRole::None),
            Err(KubericTranslationError::UnsupportedRole(ReplicaRole::None))
        );
    }

    #[test]
    fn endpoint_round_trip_is_versioned_exact_and_canonical() {
        let endpoint = endpoint(MEMBER_ONE, "mysql-a:3306", "mysql-a:33061");
        let encoded = endpoint.encode();
        assert_eq!(ReplicationEndpoint::decode(&encoded).unwrap(), endpoint);
        assert_eq!(
            ReplicationEndpoint::decode("other-v1:0:"),
            Err(KubericTranslationError::MalformedEndpoint(
                "unsupported endpoint version"
            ))
        );

        let uppercase = encoded.replace(MEMBER_ONE, &MEMBER_ONE.to_ascii_uppercase());
        assert_eq!(
            ReplicationEndpoint::decode(&uppercase),
            Err(KubericTranslationError::MalformedEndpoint(
                "endpoint is not canonical"
            ))
        );
        assert!(matches!(
            ReplicationEndpoint::decode(&(encoded + "x")),
            Err(KubericTranslationError::MalformedEndpoint(_))
        ));
    }

    #[test]
    fn replica_information_preserves_endpoint_session_and_storage_fences() {
        let information = information(
            1,
            ReplicaRole::Primary,
            MEMBER_ONE,
            "mysql-a:3306",
            "mysql-a:33061",
        );
        let target = translate_replica_information(&information).unwrap();
        assert_eq!(target.process_session().as_str(), "session-1");
        assert_eq!(target.member().member_id().as_str(), MEMBER_ONE);
        assert_eq!(target.member().address().as_str(), "mysql-a:3306");
        assert_eq!(target.endpoint().as_str(), information.replication_address);
        assert!(target.storage().as_str().starts_with(STORAGE_PREFIX));

        let mut missing = information.clone();
        missing.storage_incarnation = None;
        assert_eq!(
            translate_replica_information(&missing),
            Err(KubericTranslationError::MissingStorage)
        );

        let mut invalid_progress = information;
        invalid_progress.current_progress = INVALID_LSN;
        assert_eq!(
            translate_replica_information(&invalid_progress),
            Err(KubericTranslationError::InvalidProgress {
                field: "current",
                value: INVALID_LSN,
            })
        );
    }

    #[test]
    fn configuration_translation_requires_exact_complete_active_members() {
        let primary = information(
            1,
            ReplicaRole::Primary,
            MEMBER_ONE,
            "mysql-a:3306",
            "mysql-a:33061",
        );
        let secondary = information(
            2,
            ReplicaRole::ActiveSecondary,
            MEMBER_TWO,
            "mysql-b:3306",
            "mysql-b:33061",
        );
        let descriptor = ConfigurationDescriptor::new(
            Epoch::new(4, 2),
            primary.identity.replica_id,
            vec![
                ConfigurationMember {
                    identity: primary.identity.clone(),
                    role: primary.role,
                },
                ConfigurationMember {
                    identity: secondary.identity.clone(),
                    role: secondary.role,
                },
            ],
            2,
        );
        let configuration = ReplicaSetConfiguration {
            configuration: descriptor,
            replicas: vec![secondary.clone()],
        };
        let local_endpoint = ReplicationEndpoint::decode(&primary.replication_address).unwrap();
        let local_target = MysqlReplicaTarget::new(
            translate_replica_identity(&primary.identity).unwrap(),
            MemberPeer::new(
                local_endpoint.member_id().clone(),
                local_endpoint.member_address().clone(),
            ),
            ProcessSessionId::new("enrolled-local-process").unwrap(),
            EndpointBinding::new("enrolled-local-endpoint").unwrap(),
            StorageBinding::new("enrolled-local-storage").unwrap(),
        );
        let mut epochs = EpochTranslator::new();
        let translated = translate_replica_set_configuration(
            &configuration,
            &mut epochs,
            &primary.identity,
            &local_endpoint,
            &local_target,
        )
        .unwrap();
        assert_eq!(translated.epoch(), MysqlReplicaEpoch::new(1));
        assert_eq!(translated.members().len(), 2);
        assert_eq!(translated.members()[0], local_target);

        let mut role_mismatch = configuration.clone();
        role_mismatch.replicas[0].role = ReplicaRole::Primary;
        assert_eq!(
            translate_replica_set_configuration(
                &role_mismatch,
                &mut EpochTranslator::new(),
                &primary.identity,
                &local_endpoint,
                &local_target,
            ),
            Err(KubericTranslationError::InvalidConfiguration(
                "replica role differs from configured member role"
            ))
        );

        let mut duplicate_endpoint = configuration;
        duplicate_endpoint.replicas[0].replication_address = primary.replication_address.clone();
        assert_eq!(
            translate_replica_set_configuration(
                &duplicate_endpoint,
                &mut EpochTranslator::new(),
                &primary.identity,
                &local_endpoint,
                &local_target,
            ),
            Err(KubericTranslationError::DuplicateConfigurationFence(
                "native member id"
            ))
        );
    }

    #[test]
    fn singleton_configuration_synthesizes_local_authority_without_local_replica_information() {
        let primary = information(
            1,
            ReplicaRole::Primary,
            MEMBER_ONE,
            "mysql-a:3306",
            "mysql-a:33061",
        );
        let descriptor = ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            primary.identity.replica_id,
            vec![ConfigurationMember {
                identity: primary.identity.clone(),
                role: primary.role,
            }],
            1,
        );
        let configuration = ReplicaSetConfiguration {
            configuration: descriptor,
            replicas: vec![],
        };
        let public_endpoint = ReplicationEndpoint::decode(&primary.replication_address).unwrap();
        let enrolled = MysqlReplicaTarget::new(
            translate_replica_identity(&primary.identity).unwrap(),
            MemberPeer::new(
                public_endpoint.member_id().clone(),
                public_endpoint.member_address().clone(),
            ),
            ProcessSessionId::new("native-process-session").unwrap(),
            EndpointBinding::new("native-uds-endpoint").unwrap(),
            StorageBinding::new("native-storage-binding").unwrap(),
        );

        let translated = translate_replica_set_configuration(
            &configuration,
            &mut EpochTranslator::new(),
            &primary.identity,
            &public_endpoint,
            &enrolled,
        )
        .unwrap();

        assert_eq!(translated.members(), &[enrolled]);
    }

    #[test]
    fn callback_ledger_reuses_exact_retries_and_rejects_conflicts_and_exhaustion() {
        let authority = AuthorityGeneration::new("public-v1-callbacks").unwrap();
        let mut ledger = CallbackOperationLedger::new(authority.clone());
        let first = ledger.admit("role-1", (Epoch::new(1, 2), ReplicaRole::Primary));
        let first = first.unwrap();
        assert_eq!(
            ledger
                .admit("role-1", (Epoch::new(1, 2), ReplicaRole::Primary))
                .unwrap(),
            first
        );
        assert_eq!(
            ledger.admit("role-1", (Epoch::new(1, 2), ReplicaRole::ActiveSecondary)),
            Err(KubericTranslationError::OperationConflict)
        );
        assert_eq!(ledger.len(), 1);

        let mut exhausted = CallbackOperationLedger::with_next_generation(
            authority,
            NonZeroU64::new(u64::MAX).unwrap(),
        );
        let retained = exhausted.admit("last", 1).unwrap();
        assert_eq!(exhausted.admit("last", 1).unwrap(), retained);
        assert_eq!(
            exhausted.admit("next", 2),
            Err(KubericTranslationError::OperationExhausted)
        );
    }
}
