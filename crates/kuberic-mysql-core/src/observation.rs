//! Coherent, deterministic observation assembly and outcomes.

use core::fmt;

use crate::{
    AttemptId, ExactBinding, GtidSet, NativeField, NativeValueError, NativeValueErrorKind,
    NativeView,
};

/// A caller-supplied monotonic instant within one process.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ObservationInstant(u64);

impl ObservationInstant {
    /// Creates an instant from an opaque monotonic tick.
    #[must_use]
    pub const fn new(tick: u64) -> Self {
        Self(tick)
    }

    /// Returns the opaque monotonic tick.
    #[must_use]
    pub const fn tick(self) -> u64 {
        self.0
    }
}

/// Non-secret provenance for one collection attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservationProvenance {
    origin: String,
    attempt: AttemptId,
}

impl ObservationProvenance {
    /// Validates and constructs provenance.
    pub fn new(origin: impl Into<String>, attempt: AttemptId) -> Result<Self, ProvenanceError> {
        let origin = origin.into();
        if origin.is_empty() {
            return Err(ProvenanceError::EmptyOrigin);
        }
        if origin.len() > 255 {
            return Err(ProvenanceError::OriginTooLong);
        }
        if origin.chars().any(char::is_control) {
            return Err(ProvenanceError::ControlCharacter);
        }
        Ok(Self { origin, attempt })
    }

    /// Returns the non-secret collection origin label.
    #[must_use]
    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// Returns the collection attempt identity.
    #[must_use]
    pub const fn attempt(&self) -> &AttemptId {
        &self.attempt
    }
}

/// A structured provenance validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProvenanceError {
    /// The origin label was empty.
    EmptyOrigin,
    /// The origin label exceeded 255 UTF-8 bytes.
    OriginTooLong,
    /// The origin label contained a control character.
    ControlCharacter,
}

impl fmt::Display for ProvenanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid observation provenance: {self:?}")
    }
}

impl std::error::Error for ProvenanceError {}

/// Shared identity and timing retained by every observation outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservationMetadata {
    binding: ExactBinding,
    provenance: ObservationProvenance,
    start: ObservationInstant,
    end: ObservationInstant,
    deadline: ObservationInstant,
    decision: ObservationInstant,
}

impl ObservationMetadata {
    /// Creates an outcome metadata envelope.
    #[must_use]
    pub const fn new(
        binding: ExactBinding,
        provenance: ObservationProvenance,
        start: ObservationInstant,
        end: ObservationInstant,
        deadline: ObservationInstant,
        decision: ObservationInstant,
    ) -> Self {
        Self {
            binding,
            provenance,
            start,
            end,
            deadline,
            decision,
        }
    }

    /// Returns the attempted exact binding.
    #[must_use]
    pub const fn binding(&self) -> &ExactBinding {
        &self.binding
    }

    /// Returns collection provenance.
    #[must_use]
    pub const fn provenance(&self) -> &ObservationProvenance {
        &self.provenance
    }

    /// Returns the collection start.
    #[must_use]
    pub const fn start(&self) -> ObservationInstant {
        self.start
    }

    /// Returns the collection end.
    #[must_use]
    pub const fn end(&self) -> ObservationInstant {
        self.end
    }

    /// Returns the collection deadline.
    #[must_use]
    pub const fn deadline(&self) -> ObservationInstant {
        self.deadline
    }

    /// Returns the decision instant used for this outcome.
    #[must_use]
    pub const fn decision(&self) -> ObservationInstant {
        self.decision
    }
}

/// A binding and exact view sampled at one observation bracket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservationBracket {
    binding: ExactBinding,
    view: NativeView,
}

impl ObservationBracket {
    /// Creates a bracketed view sample.
    #[must_use]
    pub const fn new(binding: ExactBinding, view: NativeView) -> Self {
        Self { binding, view }
    }
}

/// An executed GTID set sampled under one exact binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundGtidSet {
    binding: ExactBinding,
    executed: GtidSet,
}

impl BoundGtidSet {
    /// Creates an explicitly present executed-set sample.
    #[must_use]
    pub const fn new(binding: ExactBinding, executed: GtidSet) -> Self {
        Self { binding, executed }
    }
}

/// A required fact that was not collected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationField {
    /// Opening identity/view bracket.
    OpeningBracket,
    /// Explicitly present executed GTID set.
    ExecutedGtidSet,
    /// Closing identity/view bracket.
    ClosingBracket,
}

/// A structured malformed-evidence reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MalformedReason {
    /// Collection timestamps violated `start <= end <= deadline`.
    TimingOrder,
    /// A native role or state value was syntactically malformed.
    NativeValue(NativeField),
}

/// A structured unsupported-evidence reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnsupportedReason {
    /// A well-formed native role or state value is unknown to Stage 1.
    NativeValue(NativeField),
    /// A collector schema or capability is outside Stage 1.
    CollectorCapability,
}

/// Why an otherwise coherent observation is stale.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaleReason {
    /// The decision was made after the observation deadline.
    Expired,
}

/// Why sampled facts could not form one coherent observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncoherentReason {
    /// A sample or provenance used a different exact binding/attempt.
    BindingMismatch,
    /// Opening and closing views differed.
    BracketMismatch,
    /// The bound local member was absent or contradicted its binding.
    LocalMemberMismatch,
}

/// A terminal collection failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectionFailure {
    /// Authoritative data was absent.
    Absent,
    /// The source was unreachable.
    Unreachable,
    /// Permission was denied.
    PermissionDenied,
    /// Authentication failed.
    AuthenticationFailure,
    /// Collected data was malformed.
    Malformed(MalformedReason),
    /// The collector encountered an unsupported value/capability.
    Unsupported(UnsupportedReason),
}

/// Immutable evidence that passed completeness, coherence, and freshness checks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidObservation {
    metadata: ObservationMetadata,
    view: NativeView,
    executed: GtidSet,
}

impl ValidObservation {
    /// Returns the shared binding/timing/provenance envelope.
    #[must_use]
    pub const fn metadata(&self) -> &ObservationMetadata {
        &self.metadata
    }

    /// Returns the exact native view.
    #[must_use]
    pub const fn view(&self) -> &NativeView {
        &self.view
    }

    /// Returns the explicitly observed executed GTID set.
    #[must_use]
    pub const fn executed(&self) -> &GtidSet {
        &self.executed
    }

    /// Rechecks freshness at a later explicit decision instant.
    pub fn check_freshness(&self, decision: ObservationInstant) -> Result<(), FreshnessError> {
        if decision < self.metadata.end {
            Err(FreshnessError::FutureDated)
        } else if decision > self.metadata.deadline {
            Err(FreshnessError::Expired)
        } else {
            Ok(())
        }
    }
}

/// A completion-time freshness rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FreshnessError {
    /// Decision time preceded collection completion.
    FutureDated,
    /// Decision time followed the deadline.
    Expired,
}

/// The complete typed result of one observation attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservationOutcome {
    /// Authoritative evidence was absent.
    Absent(ObservationMetadata),
    /// The evidence source was unreachable.
    Unreachable(ObservationMetadata),
    /// Permission was denied.
    PermissionDenied(ObservationMetadata),
    /// Authentication failed.
    AuthenticationFailure(ObservationMetadata),
    /// Evidence was malformed.
    Malformed {
        /// Shared outcome metadata.
        metadata: ObservationMetadata,
        /// Structured rejection reason.
        reason: MalformedReason,
    },
    /// Evidence used an unsupported value or capability.
    Unsupported {
        /// Shared outcome metadata.
        metadata: ObservationMetadata,
        /// Structured rejection reason.
        reason: UnsupportedReason,
    },
    /// Collection ended without every required coherent fact.
    Partial {
        /// Shared outcome metadata.
        metadata: ObservationMetadata,
        /// Required facts that were never collected.
        missing: Vec<ObservationField>,
        /// Optional terminal cause after partial progress.
        cause: Option<CollectionFailure>,
    },
    /// Evidence expired before the decision.
    Stale {
        /// Shared outcome metadata.
        metadata: ObservationMetadata,
        /// Structured stale reason.
        reason: StaleReason,
    },
    /// Decision time preceded collection completion.
    FutureDated(ObservationMetadata),
    /// Facts came from different bindings or brackets.
    Incoherent {
        /// Shared outcome metadata.
        metadata: ObservationMetadata,
        /// Structured incoherence reason.
        reason: IncoherentReason,
    },
    /// Complete, coherent, fresh evidence.
    Valid(ValidObservation),
}

impl ObservationOutcome {
    /// Returns the metadata retained by this outcome.
    #[must_use]
    pub const fn metadata(&self) -> &ObservationMetadata {
        match self {
            Self::Absent(metadata)
            | Self::Unreachable(metadata)
            | Self::PermissionDenied(metadata)
            | Self::AuthenticationFailure(metadata)
            | Self::FutureDated(metadata) => metadata,
            Self::Malformed { metadata, .. }
            | Self::Unsupported { metadata, .. }
            | Self::Partial { metadata, .. }
            | Self::Stale { metadata, .. }
            | Self::Incoherent { metadata, .. } => metadata,
            Self::Valid(observation) => observation.metadata(),
        }
    }

    /// Returns valid evidence only for the valid outcome.
    #[must_use]
    pub const fn valid(&self) -> Option<&ValidObservation> {
        if let Self::Valid(observation) = self {
            Some(observation)
        } else {
            None
        }
    }
}

/// Incomplete observation state that can only become evidence through validation.
#[derive(Clone, Debug)]
pub struct ObservationDraft {
    metadata: ObservationMetadata,
    opening: Option<ObservationBracket>,
    executed: Option<BoundGtidSet>,
    closing: Option<ObservationBracket>,
    failure: Option<CollectionFailure>,
}

impl ObservationDraft {
    /// Begins an incomplete collection.
    #[must_use]
    pub const fn new(metadata: ObservationMetadata) -> Self {
        Self {
            metadata,
            opening: None,
            executed: None,
            closing: None,
            failure: None,
        }
    }

    /// Adds the opening bracket.
    #[must_use]
    pub fn opening(mut self, bracket: ObservationBracket) -> Self {
        self.opening = Some(bracket);
        self
    }

    /// Adds the explicitly observed executed GTID set.
    #[must_use]
    pub fn executed(mut self, executed: BoundGtidSet) -> Self {
        self.executed = Some(executed);
        self
    }

    /// Adds the closing bracket.
    #[must_use]
    pub fn closing(mut self, bracket: ObservationBracket) -> Self {
        self.closing = Some(bracket);
        self
    }

    /// Records a terminal collection failure.
    #[must_use]
    pub const fn failure(mut self, failure: CollectionFailure) -> Self {
        self.failure = Some(failure);
        self
    }

    /// Records a native role/state decode failure with structured classification.
    #[must_use]
    pub fn native_decode_failure(self, error: &NativeValueError) -> Self {
        let failure = match error.kind() {
            NativeValueErrorKind::Malformed => {
                CollectionFailure::Malformed(MalformedReason::NativeValue(error.field()))
            }
            NativeValueErrorKind::Unsupported => {
                CollectionFailure::Unsupported(UnsupportedReason::NativeValue(error.field()))
            }
        };
        self.failure(failure)
    }

    /// Consumes the draft and returns one fail-closed typed outcome.
    #[must_use]
    pub fn finalize(self) -> ObservationOutcome {
        if self.metadata.start > self.metadata.end || self.metadata.end > self.metadata.deadline {
            return ObservationOutcome::Malformed {
                metadata: self.metadata,
                reason: MalformedReason::TimingOrder,
            };
        }

        if self.metadata.provenance.attempt() != &self.metadata.binding.parts().attempt
            || self
                .opening
                .as_ref()
                .is_some_and(|sample| sample.binding != self.metadata.binding)
            || self
                .executed
                .as_ref()
                .is_some_and(|sample| sample.binding != self.metadata.binding)
            || self
                .closing
                .as_ref()
                .is_some_and(|sample| sample.binding != self.metadata.binding)
        {
            return ObservationOutcome::Incoherent {
                metadata: self.metadata,
                reason: IncoherentReason::BindingMismatch,
            };
        }

        if let (Some(opening), Some(closing)) = (&self.opening, &self.closing)
            && opening.view != closing.view
        {
            return ObservationOutcome::Incoherent {
                metadata: self.metadata,
                reason: IncoherentReason::BracketMismatch,
            };
        }

        let has_progress =
            self.opening.is_some() || self.executed.is_some() || self.closing.is_some();
        if let Some(failure) = self.failure {
            if has_progress {
                return ObservationOutcome::Partial {
                    metadata: self.metadata,
                    missing: missing_fields(&self.opening, &self.executed, &self.closing),
                    cause: Some(failure),
                };
            }
            return failure_outcome(self.metadata, failure);
        }

        if self.metadata.decision < self.metadata.end {
            return ObservationOutcome::FutureDated(self.metadata);
        }
        if self.metadata.decision > self.metadata.deadline {
            return ObservationOutcome::Stale {
                metadata: self.metadata,
                reason: StaleReason::Expired,
            };
        }

        let missing = missing_fields(&self.opening, &self.executed, &self.closing);
        if !missing.is_empty() {
            return ObservationOutcome::Partial {
                metadata: self.metadata,
                missing,
                cause: None,
            };
        }

        let opening = self.opening.expect("missing fields checked");
        let closing = self.closing.expect("missing fields checked");
        let executed = self.executed.expect("missing fields checked");
        if !view_matches_binding(&opening.view, &self.metadata.binding)
            || !view_matches_binding(&closing.view, &self.metadata.binding)
        {
            return ObservationOutcome::Incoherent {
                metadata: self.metadata,
                reason: IncoherentReason::LocalMemberMismatch,
            };
        }

        ObservationOutcome::Valid(ValidObservation {
            metadata: self.metadata,
            view: opening.view,
            executed: executed.executed,
        })
    }
}

fn missing_fields(
    opening: &Option<ObservationBracket>,
    executed: &Option<BoundGtidSet>,
    closing: &Option<ObservationBracket>,
) -> Vec<ObservationField> {
    let mut missing = Vec::new();
    if opening.is_none() {
        missing.push(ObservationField::OpeningBracket);
    }
    if executed.is_none() {
        missing.push(ObservationField::ExecutedGtidSet);
    }
    if closing.is_none() {
        missing.push(ObservationField::ClosingBracket);
    }
    missing
}

fn failure_outcome(
    metadata: ObservationMetadata,
    failure: CollectionFailure,
) -> ObservationOutcome {
    match failure {
        CollectionFailure::Absent => ObservationOutcome::Absent(metadata),
        CollectionFailure::Unreachable => ObservationOutcome::Unreachable(metadata),
        CollectionFailure::PermissionDenied => ObservationOutcome::PermissionDenied(metadata),
        CollectionFailure::AuthenticationFailure => {
            ObservationOutcome::AuthenticationFailure(metadata)
        }
        CollectionFailure::Malformed(reason) => ObservationOutcome::Malformed { metadata, reason },
        CollectionFailure::Unsupported(reason) => {
            ObservationOutcome::Unsupported { metadata, reason }
        }
    }
}

fn view_matches_binding(view: &NativeView, binding: &ExactBinding) -> bool {
    let parts = binding.parts();
    view.group_name() == &parts.group_name
        && view.id() == &parts.view_id
        && view.member(&parts.member_id).is_some_and(|member| {
            parts.server_uuid.as_str() == member.id().as_str()
                && member.address() == &parts.member_address
        })
}
