//! Caller-established monotonic clock and deadline mapping.

use core::fmt;
use std::time::Instant;

use kuberic_mysql_core::ObservationInstant;

/// Maps core observation ticks into one caller-owned monotonic runtime domain.
pub trait ObservationClock: Send + Sync {
    /// Returns the current core observation instant.
    fn now(&self) -> ObservationInstant;

    /// Maps a core instant into the same monotonic domain as [`Instant`].
    fn to_std_instant(&self, instant: ObservationInstant) -> Result<Instant, ClockError>;
}

/// One clock and one absolute observation deadline.
#[derive(Debug)]
pub struct ClockContext<C> {
    clock: C,
    deadline: ObservationInstant,
}

impl<C: ObservationClock> ClockContext<C> {
    /// Validates that the deadline is not already behind the caller clock.
    pub fn new(clock: C, deadline: ObservationInstant) -> Result<Self, ClockError> {
        if deadline < clock.now() {
            return Err(ClockError::DeadlineBeforeNow);
        }
        clock.to_std_instant(deadline)?;
        Ok(Self { clock, deadline })
    }

    /// Returns the caller clock.
    #[must_use]
    pub const fn clock(&self) -> &C {
        &self.clock
    }

    /// Returns the deadline in the core monotonic domain.
    #[must_use]
    pub const fn deadline(&self) -> ObservationInstant {
        self.deadline
    }

    /// Returns the deadline in the runtime monotonic domain.
    pub fn runtime_deadline(&self) -> Result<Instant, ClockError> {
        self.clock.to_std_instant(self.deadline)
    }
}

/// A caller-clock validation or mapping failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClockError {
    /// The requested deadline is already behind the clock.
    DeadlineBeforeNow,
    /// The clock cannot represent the core instant in the runtime domain.
    UnrepresentableInstant,
}

impl fmt::Display for ClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid observation clock context: {self:?}")
    }
}

impl std::error::Error for ClockError {}
