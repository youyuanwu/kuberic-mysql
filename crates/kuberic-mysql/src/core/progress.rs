//! Qualified scalar progress projections over structured GTID history.
//!
//! The complete [`GtidSet`] remains the caller-owned native authority. A
//! [`QualifiedProgress`] is only a public scalar projection of that evidence;
//! it does not replace or retain the structured history. Catch-up capability is
//! evaluated separately and is never inferred from current progress.

use core::fmt;

use crate::core::{GtidSet, IdentityError, ServerUuid};

/// The configured untagged GTID source eligible for scalar projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredGtidLineage {
    group_uuid: ServerUuid,
}

impl ConfiguredGtidLineage {
    /// Validates the configured Group Replication UUID.
    pub fn new(group_uuid: impl AsRef<str>) -> Result<Self, IdentityError> {
        ServerUuid::new(group_uuid).map(|group_uuid| Self { group_uuid })
    }

    /// Returns the exact configured source UUID.
    #[must_use]
    pub const fn group_uuid(&self) -> &ServerUuid {
        &self.group_uuid
    }
}

impl fmt::Display for ConfiguredGtidLineage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.group_uuid.fmt(formatter)
    }
}

/// The structured history shape that produced qualified scalar progress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QualifiedProgressKind {
    /// No transaction has been executed.
    EmptyHistory,
    /// One contiguous configured lineage begins at sequence one.
    ContiguousHistory,
}

/// A signed scalar projected from a qualified complete GTID set.
///
/// This value carries the qualified history shape, but not the source
/// [`GtidSet`]. Callers must retain that complete structured evidence whenever
/// they need to justify or reconstruct the projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QualifiedProgress {
    value: i64,
    kind: QualifiedProgressKind,
}

impl QualifiedProgress {
    const fn initial() -> Self {
        Self {
            value: 0,
            kind: QualifiedProgressKind::EmptyHistory,
        }
    }

    /// Returns `0` for empty history or the inclusive interval tail.
    #[must_use]
    pub const fn value(self) -> i64 {
        self.value
    }

    /// Returns the structured history shape that justified this projection.
    #[must_use]
    pub const fn kind(self) -> QualifiedProgressKind {
        self.kind
    }
}

impl fmt::Display for QualifiedProgress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(formatter)
    }
}

/// A deterministic reason complete native history cannot be projected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NonProjectableProgressReason {
    /// More than one GTID source is present.
    MultipleSources,
    /// The only source is not the configured Group Replication UUID.
    ForeignSource,
    /// The configured source carries a transaction tag.
    TaggedSource,
    /// The configured source contains more than one normalized interval.
    MultipleIntervals,
    /// The only interval does not begin at sequence one.
    DoesNotBeginAtOne,
}

/// The result of projecting complete structured GTID history to a scalar.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgressProjection {
    /// The complete history satisfies the qualified scalar profile.
    Projectable(QualifiedProgress),
    /// The complete history must remain structured and cannot be flattened.
    NonProjectable(NonProjectableProgressReason),
}

/// Projects complete native history only when it has the qualified shape.
///
/// The input is borrowed and remains the authoritative native evidence. Empty
/// history maps to `0`; compatible non-empty history maps to its exact interval
/// tail. Every other parsed shape returns one deterministic non-projectable
/// reason without flattening or changing the [`GtidSet`].
#[must_use]
pub fn project_progress(lineage: &ConfiguredGtidLineage, executed: &GtidSet) -> ProgressProjection {
    let entries = executed.entries();
    if entries.is_empty() {
        return ProgressProjection::Projectable(QualifiedProgress::initial());
    }
    if entries.len() != 1 {
        return ProgressProjection::NonProjectable(NonProjectableProgressReason::MultipleSources);
    }

    let history = &entries[0];
    if history.source().server_uuid() != lineage.group_uuid() {
        return ProgressProjection::NonProjectable(NonProjectableProgressReason::ForeignSource);
    }
    if history.source().tag().is_some() {
        return ProgressProjection::NonProjectable(NonProjectableProgressReason::TaggedSource);
    }
    if history.intervals().len() != 1 {
        return ProgressProjection::NonProjectable(NonProjectableProgressReason::MultipleIntervals);
    }

    let interval = history.intervals()[0];
    if interval.start() != 1 {
        return ProgressProjection::NonProjectable(NonProjectableProgressReason::DoesNotBeginAtOne);
    }

    ProgressProjection::Projectable(QualifiedProgress {
        value: i64::try_from(interval.end())
            .expect("validated GTID sequence always fits signed scalar progress"),
        kind: QualifiedProgressKind::ContiguousHistory,
    })
}

/// Why Stage 2 cannot offer an incremental catch-up boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CatchUpUnsupportedReason {
    /// Retained donor and binary-log evidence is not available to this boundary.
    RetainedEvidenceUnavailable,
}

/// Why available retained evidence did not prove a catch-up boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CatchUpUnprovedReason {
    /// Donor identity or retained binary-log coverage was incomplete.
    IncompleteRetainedEvidence,
    /// Retained history was not projectable under the configured lineage.
    NonProjectableRetainedHistory,
}

/// An independently evaluated Stage 2 catch-up capability outcome.
///
/// Neither variant carries a scalar fallback. In particular, projectable
/// current progress does not imply that a donor retains the native history
/// needed for incremental catch-up.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatchUpCapabilityResult {
    /// This stage does not have the retained evidence required by the operation.
    Unsupported(CatchUpUnsupportedReason),
    /// Evidence was supplied but did not prove an incremental boundary.
    Unproved(CatchUpUnprovedReason),
}

impl CatchUpCapabilityResult {
    /// Returns the Stage 2 result when retained donor/binlog evidence is absent.
    #[must_use]
    pub const fn retained_evidence_unavailable() -> Self {
        Self::Unsupported(CatchUpUnsupportedReason::RetainedEvidenceUnavailable)
    }

    /// Returns an unproved result without substituting current progress.
    #[must_use]
    pub const fn unproved(reason: CatchUpUnprovedReason) -> Self {
        Self::Unproved(reason)
    }
}
