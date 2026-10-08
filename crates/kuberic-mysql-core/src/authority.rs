//! Minimal fail-closed authority and observation-credit state.

use crate::{ExactBinding, FreshnessError, ObservationInstant, ObservationOutcome};

/// A session-private, non-reusable admission capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionCapability {
    generation: u64,
}

/// A failure to begin a new pending attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionError {
    /// The offered binding was not the exact current binding.
    BindingMismatch,
    /// The session-local capability generation was exhausted.
    GenerationExhausted,
}

/// A structured completion rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionRejection {
    /// No work is currently pending.
    NoPendingAttempt,
    /// The capability belongs to revoked, consumed, or older work.
    StaleCapability,
    /// The evidence did not use the exact current pending binding.
    BindingMismatch,
    /// The supplied observation outcome was not valid.
    NonValidObservation,
    /// Completion decision time preceded observation completion.
    FutureDated,
    /// The observation expired before completion.
    Expired,
}

/// Positive credit granted by Stage 1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionCredit {
    /// A current observation attempt completed with valid evidence.
    ObservationAcknowledged,
}

/// Client-access projection exposed by Stage 1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AccessProjection {
    read_open: bool,
    write_open: bool,
}

impl AccessProjection {
    const CLOSED: Self = Self {
        read_open: false,
        write_open: false,
    };

    /// Returns whether read access is open.
    #[must_use]
    pub const fn read_open(self) -> bool {
        self.read_open
    }

    /// Returns whether write access is open.
    #[must_use]
    pub const fn write_open(self) -> bool {
        self.write_open
    }
}

/// Stage 1 operations that are explicitly unsupported.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnsupportedOperation {
    /// Any Group Replication topology mutation.
    TopologyMutation,
    /// Group bootstrap.
    Bootstrap,
    /// Member join.
    Join,
    /// Automated recovery.
    Recovery,
    /// Destructive repair.
    DestructiveRepair,
    /// Data-loss acceptance.
    DataLossAcceptance,
    /// Client-access publication.
    AccessPublication,
}

/// Explicit result for an unsupported request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnsupportedResult {
    operation: UnsupportedOperation,
}

impl UnsupportedResult {
    /// Returns the rejected operation category.
    #[must_use]
    pub const fn operation(self) -> UnsupportedOperation {
        self.operation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingAttempt {
    capability: AdmissionCapability,
    binding: ExactBinding,
}

/// An in-memory Stage 1 session that grants observation credit only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoritySession {
    current: ExactBinding,
    next_generation: u64,
    pending: Option<PendingAttempt>,
    observation_credited: bool,
}

impl AuthoritySession {
    /// Creates a session with closed access and no pending work.
    #[must_use]
    pub const fn new(current: ExactBinding) -> Self {
        Self {
            current,
            next_generation: 0,
            pending: None,
            observation_credited: false,
        }
    }

    /// Returns the exact current binding.
    #[must_use]
    pub const fn current_binding(&self) -> &ExactBinding {
        &self.current
    }

    /// Returns whether current observation completion has been acknowledged.
    #[must_use]
    pub const fn has_observation_credit(&self) -> bool {
        self.observation_credited
    }

    /// Returns the always-closed Stage 1 client-access projection.
    #[must_use]
    pub const fn access(&self) -> AccessProjection {
        AccessProjection::CLOSED
    }

    /// Returns whether one observation attempt is pending.
    #[must_use]
    pub const fn has_pending_attempt(&self) -> bool {
        self.pending.is_some()
    }

    /// Replaces the current context and permanently revokes pending/credited work.
    pub fn replace_binding(&mut self, current: ExactBinding) {
        self.current = current;
        self.pending = None;
        self.observation_credited = false;
    }

    /// Begins work for the exact current binding and mints a private capability.
    pub fn begin_attempt(
        &mut self,
        offered: &ExactBinding,
    ) -> Result<AdmissionCapability, AdmissionError> {
        if offered != &self.current {
            return Err(AdmissionError::BindingMismatch);
        }
        let generation = self
            .next_generation
            .checked_add(1)
            .ok_or(AdmissionError::GenerationExhausted)?;
        self.next_generation = generation;
        let capability = AdmissionCapability { generation };
        self.pending = Some(PendingAttempt {
            capability,
            binding: offered.clone(),
        });
        self.observation_credited = false;
        Ok(capability)
    }

    /// Completes current work only for exact, fresh, valid evidence.
    pub fn complete(
        &mut self,
        capability: AdmissionCapability,
        outcome: &ObservationOutcome,
        decision: ObservationInstant,
    ) -> Result<CompletionCredit, CompletionRejection> {
        let Some(pending) = self.pending.as_ref() else {
            return Err(CompletionRejection::StaleCapability);
        };
        if pending.capability != capability {
            return Err(CompletionRejection::StaleCapability);
        }

        let pending = self.pending.take().expect("pending checked above");
        let Some(valid) = outcome.valid() else {
            self.observation_credited = false;
            return Err(CompletionRejection::NonValidObservation);
        };
        if pending.binding != self.current || valid.metadata().binding() != &pending.binding {
            self.observation_credited = false;
            return Err(CompletionRejection::BindingMismatch);
        }
        if let Err(error) = valid.check_freshness(decision) {
            self.observation_credited = false;
            return Err(match error {
                FreshnessError::FutureDated => CompletionRejection::FutureDated,
                FreshnessError::Expired => CompletionRejection::Expired,
            });
        }

        self.observation_credited = true;
        Ok(CompletionCredit::ObservationAcknowledged)
    }

    /// Rejects a Stage 1 operation without changing session state.
    #[must_use]
    pub const fn reject_unsupported(&self, operation: UnsupportedOperation) -> UnsupportedResult {
        UnsupportedResult { operation }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AttemptId, AuthorityGeneration, ConfigurationId, CredentialGeneration, EndpointBinding,
        Epoch, ExactBindingParts, GroupName, MemberAddress, MemberId, ObservationSessionId,
        PartitionId, ProcessSessionId, ReplicaId, ReplicaIncarnation, ResourceId, ServerUuid,
        StorageBinding, ViewId,
    };

    fn binding() -> ExactBinding {
        ExactBinding::new(ExactBindingParts {
            resource: ResourceId::new("r").unwrap(),
            partition: PartitionId::new("p").unwrap(),
            replica: ReplicaId::new("replica").unwrap(),
            incarnation: ReplicaIncarnation::new("i").unwrap(),
            process_session: ProcessSessionId::new("ps").unwrap(),
            observation_session: ObservationSessionId::new("os").unwrap(),
            attempt: AttemptId::new("a").unwrap(),
            endpoint: EndpointBinding::new("e").unwrap(),
            storage: StorageBinding::new("s").unwrap(),
            server_uuid: ServerUuid::new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
            group_name: GroupName::new("g").unwrap(),
            member_id: MemberId::new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
            member_address: MemberAddress::new("m").unwrap(),
            configuration: ConfigurationId::new("c").unwrap(),
            epoch: Epoch::new("epoch").unwrap(),
            authority_generation: AuthorityGeneration::new("authority").unwrap(),
            view_id: ViewId::new("view").unwrap(),
            credential_generation: CredentialGeneration::new("credential").unwrap(),
        })
    }

    #[test]
    fn capability_generation_exhaustion_fails_closed() {
        let current = binding();
        let mut session = AuthoritySession {
            current: current.clone(),
            next_generation: u64::MAX,
            pending: None,
            observation_credited: false,
        };
        assert_eq!(
            session.begin_attempt(&current),
            Err(AdmissionError::GenerationExhausted)
        );
        assert!(!session.has_pending_attempt());
        assert!(!session.access().write_open());
    }
}
