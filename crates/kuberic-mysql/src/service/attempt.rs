//! Shared deadline, cancellation, and cleanup accounting for one live attempt.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::core::AttemptId;
use crate::service::{MysqlTopologyError, NativeControlDeadline};

/// Current position relative to one attempt's work and cleanup deadlines.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptDeadlineState {
    /// Primary work may still start or continue.
    Work,
    /// Only mandatory cleanup may continue.
    CleanupReserve,
    /// The absolute attempt deadline has been reached.
    Expired,
}

/// Current cancellation state shared by all work in one attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptCancellationState {
    /// No cancellation has been requested.
    Active,
    /// Cancellation was requested and no new primary work may begin.
    Cancelled,
}

/// Cleanup evidence paired with a primary attempt outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptCleanupState {
    /// The successful effect required no compensating cleanup.
    NotRequired,
    /// Mandatory cleanup or restoration completed.
    Completed,
    /// Mandatory cleanup or restoration failed.
    Failed,
}

/// Why primary work could not be admitted under an attempt context.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptAdmissionError {
    /// Cancellation was requested.
    Cancelled,
    /// The attempt has entered its cleanup-only reserve.
    CleanupReserve,
    /// The absolute attempt deadline has expired.
    Expired,
}

/// A primary result paired with its mandatory cleanup result.
#[derive(Debug)]
pub struct PairedAttemptOutcome<T, E, C = E> {
    primary: Result<T, E>,
    cleanup: Result<AttemptCleanupState, C>,
}

impl<T, E, C> PairedAttemptOutcome<T, E, C> {
    /// Pairs a primary result with cleanup evidence.
    #[must_use]
    pub const fn new(primary: Result<T, E>, cleanup: Result<AttemptCleanupState, C>) -> Self {
        Self { primary, cleanup }
    }

    /// Primary operation result.
    pub const fn primary(&self) -> &Result<T, E> {
        &self.primary
    }

    /// Cleanup result retained independently from the primary result.
    pub const fn cleanup(&self) -> &Result<AttemptCleanupState, C> {
        &self.cleanup
    }

    /// Splits the paired result without discarding either side.
    pub fn into_parts(self) -> (Result<T, E>, Result<AttemptCleanupState, C>) {
        (self.primary, self.cleanup)
    }
}

/// One shared process-monotonic attempt budget and cancellation source.
#[derive(Clone, Debug)]
pub struct AttemptContext {
    attempt: AttemptId,
    deadline: Instant,
    work_deadline: Instant,
    cleanup_reserve: Duration,
    cancelled: Arc<AtomicBool>,
    cancellation: watch::Sender<bool>,
}

impl AttemptContext {
    /// Creates one future absolute deadline with a strictly smaller cleanup
    /// reserve.
    pub fn new(
        attempt: AttemptId,
        deadline: Instant,
        cleanup_reserve: Duration,
    ) -> Result<Self, AttemptAdmissionError> {
        Self::new_at(attempt, Instant::now(), deadline, cleanup_reserve)
    }

    /// Creates a context relative to an explicit monotonic instant.
    pub fn new_at(
        attempt: AttemptId,
        now: Instant,
        deadline: Instant,
        cleanup_reserve: Duration,
    ) -> Result<Self, AttemptAdmissionError> {
        if deadline <= now {
            return Err(AttemptAdmissionError::Expired);
        }
        if cleanup_reserve.is_zero() {
            return Err(AttemptAdmissionError::CleanupReserve);
        }
        let work_deadline = deadline
            .checked_sub(cleanup_reserve)
            .filter(|work_deadline| *work_deadline > now)
            .ok_or(AttemptAdmissionError::CleanupReserve)?;
        let (cancellation, _) = watch::channel(false);
        Ok(Self {
            attempt,
            deadline,
            work_deadline,
            cleanup_reserve,
            cancelled: Arc::new(AtomicBool::new(false)),
            cancellation,
        })
    }

    /// Attempt identity shared by every member effect.
    #[must_use]
    pub const fn attempt(&self) -> &AttemptId {
        &self.attempt
    }

    /// Returns whether both handles share the exact cancellation source.
    #[must_use]
    pub fn shares_cancellation_source(&self, other: &Self) -> bool {
        self.attempt == other.attempt && Arc::ptr_eq(&self.cancelled, &other.cancelled)
    }

    /// Absolute attempt deadline.
    #[must_use]
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Absolute cutoff for primary work.
    #[must_use]
    pub const fn work_deadline(&self) -> Instant {
        self.work_deadline
    }

    /// Duration reserved exclusively for cleanup.
    #[must_use]
    pub const fn cleanup_reserve(&self) -> Duration {
        self.cleanup_reserve
    }

    /// Requests cancellation for every clone of this context.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        let _ = self.cancellation.send(true);
    }

    /// Current shared cancellation state.
    #[must_use]
    pub fn cancellation_state(&self) -> AttemptCancellationState {
        if self.cancelled.load(Ordering::Acquire) {
            AttemptCancellationState::Cancelled
        } else {
            AttemptCancellationState::Active
        }
    }

    /// Classifies an explicit monotonic instant.
    #[must_use]
    pub fn deadline_state_at(&self, now: Instant) -> AttemptDeadlineState {
        if now >= self.deadline {
            AttemptDeadlineState::Expired
        } else if now >= self.work_deadline {
            AttemptDeadlineState::CleanupReserve
        } else {
            AttemptDeadlineState::Work
        }
    }

    /// Classifies the current process-monotonic instant.
    #[must_use]
    pub fn deadline_state(&self) -> AttemptDeadlineState {
        self.deadline_state_at(Instant::now())
    }

    /// Admits primary work only while active and before the cleanup reserve.
    pub fn admit_work(&self) -> Result<(), AttemptAdmissionError> {
        if self.cancellation_state() == AttemptCancellationState::Cancelled {
            return Err(AttemptAdmissionError::Cancelled);
        }
        match self.deadline_state() {
            AttemptDeadlineState::Work => Ok(()),
            AttemptDeadlineState::CleanupReserve => Err(AttemptAdmissionError::CleanupReserve),
            AttemptDeadlineState::Expired => Err(AttemptAdmissionError::Expired),
        }
    }

    /// Admits mandatory cleanup until the absolute attempt deadline,
    /// regardless of cancellation or whether the cleanup reserve has begun.
    pub fn admit_cleanup(&self) -> Result<(), AttemptAdmissionError> {
        if self.deadline_state() == AttemptDeadlineState::Expired {
            Err(AttemptAdmissionError::Expired)
        } else {
            Ok(())
        }
    }

    pub(crate) fn cancellation_receiver(&self) -> watch::Receiver<bool> {
        self.cancellation.subscribe()
    }

    pub(crate) fn native_deadline(&self) -> Result<NativeControlDeadline, MysqlTopologyError> {
        NativeControlDeadline::new(self.attempt.clone(), self.work_deadline)
    }
}
