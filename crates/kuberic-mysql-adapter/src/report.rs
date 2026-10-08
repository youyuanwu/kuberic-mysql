//! Public secret-free observation report.

use kuberic_mysql_core::ObservationOutcome;

use crate::AdapterDiagnostic;

/// The authoritative core outcome plus adapter-specific diagnostic context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservationReport {
    outcome: ObservationOutcome,
    diagnostic: AdapterDiagnostic,
}

impl ObservationReport {
    /// Creates a report from one authoritative outcome and one diagnostic.
    #[must_use]
    pub const fn new(outcome: ObservationOutcome, diagnostic: AdapterDiagnostic) -> Self {
        Self {
            outcome,
            diagnostic,
        }
    }

    /// Returns the authoritative core outcome.
    #[must_use]
    pub const fn outcome(&self) -> &ObservationOutcome {
        &self.outcome
    }

    /// Returns the secret-free adapter diagnostic.
    #[must_use]
    pub const fn diagnostic(&self) -> &AdapterDiagnostic {
        &self.diagnostic
    }

    /// Consumes the report into its public, dependency-neutral parts.
    #[must_use]
    pub fn into_parts(self) -> (ObservationOutcome, AdapterDiagnostic) {
        (self.outcome, self.diagnostic)
    }
}
