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

/// One Build-scoped child budget under a member's lifetime attempt authority.
///
/// Cancelling the child never cancels its parent. The child deadline can only
/// shorten the parent budget and every request can retain both identities.
#[derive(Clone, Debug)]
pub struct BuildAttemptContext {
    parent: AttemptContext,
    group: BuildAttemptGroup,
}

/// Shared identity, deadline, and cancellation source for all participants in
/// one Build.
#[derive(Clone, Debug)]
pub struct BuildAttemptGroup {
    build: AttemptId,
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

impl BuildAttemptContext {
    /// Derives a child for `parent` under one common Build group.
    pub fn new(
        parent: AttemptContext,
        build: AttemptId,
        deadline: Instant,
        cleanup_reserve: Duration,
    ) -> Result<Self, AttemptAdmissionError> {
        BuildAttemptGroup::new(build, deadline, cleanup_reserve)?.child(parent)
    }

    /// Parent lifetime authority identity.
    #[must_use]
    pub const fn parent_attempt(&self) -> &AttemptId {
        self.parent.attempt()
    }

    pub(crate) const fn parent_context(&self) -> &AttemptContext {
        &self.parent
    }

    /// Exact child Build identity.
    #[must_use]
    pub const fn build(&self) -> &AttemptId {
        &self.group.build
    }

    pub(crate) fn group(&self) -> BuildAttemptGroup {
        self.group.clone()
    }

    /// Common absolute Build deadline, bounded by the parent.
    #[must_use]
    pub const fn deadline(&self) -> Instant {
        self.group.deadline
    }

    /// Absolute primary-work cutoff.
    #[must_use]
    pub const fn work_deadline(&self) -> Instant {
        self.group.work_deadline
    }

    /// Reserved cleanup duration.
    #[must_use]
    pub const fn cleanup_reserve(&self) -> Duration {
        self.group.cleanup_reserve
    }

    /// Cancels only this child Build.
    pub fn cancel(&self) {
        self.group.cancel();
    }

    /// Child cancellation state.
    #[must_use]
    pub fn cancellation_state(&self) -> AttemptCancellationState {
        self.group.cancellation_state()
    }

    /// Admits Build work under both parent and child budgets.
    pub fn admit_work(&self) -> Result<(), AttemptAdmissionError> {
        self.parent.admit_work()?;
        if self.cancellation_state() == AttemptCancellationState::Cancelled {
            return Err(AttemptAdmissionError::Cancelled);
        }
        let now = Instant::now();
        if now >= self.group.deadline {
            Err(AttemptAdmissionError::Expired)
        } else if now >= self.group.work_deadline {
            Err(AttemptAdmissionError::CleanupReserve)
        } else {
            Ok(())
        }
    }

    /// Admits bounded Build cleanup while the child deadline remains.
    pub fn admit_cleanup(&self) -> Result<(), AttemptAdmissionError> {
        self.parent.admit_cleanup()?;
        (Instant::now() < self.group.deadline)
            .then_some(())
            .ok_or(AttemptAdmissionError::Expired)
    }
}

impl BuildAttemptGroup {
    /// Creates the common Build budget shared by every participant.
    pub fn new(
        build: AttemptId,
        deadline: Instant,
        cleanup_reserve: Duration,
    ) -> Result<Self, AttemptAdmissionError> {
        let now = Instant::now();
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
            build,
            deadline,
            work_deadline,
            cleanup_reserve,
            cancelled: Arc::new(AtomicBool::new(false)),
            cancellation,
        })
    }

    /// Derives one participant child and rejects a shorter parent budget.
    pub fn child(
        &self,
        parent: AttemptContext,
    ) -> Result<BuildAttemptContext, AttemptAdmissionError> {
        if self.deadline > parent.deadline() {
            return Err(AttemptAdmissionError::Expired);
        }

        Ok(BuildAttemptContext {
            parent,
            group: self.clone(),
        })
    }

    #[must_use]
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    #[must_use]
    pub const fn work_deadline(&self) -> Instant {
        self.work_deadline
    }

    /// Cancels every participant child without cancelling any parent.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        let _ = self.cancellation.send(true);
    }

    /// Common Build cancellation state.
    #[must_use]
    pub fn cancellation_state(&self) -> AttemptCancellationState {
        if self.cancelled.load(Ordering::Acquire) {
            AttemptCancellationState::Cancelled
        } else {
            AttemptCancellationState::Active
        }
    }
}

#[cfg(test)]
mod build_attempt_tests {
    use super::*;

    #[test]
    fn child_cancellation_does_not_cancel_parent() {
        let now = Instant::now();
        let parent = AttemptContext::new_at(
            AttemptId::new("parent").unwrap(),
            now,
            now + Duration::from_secs(60),
            Duration::from_secs(10),
        )
        .unwrap();
        let child = BuildAttemptContext::new(
            parent.clone(),
            AttemptId::new("build").unwrap(),
            now + Duration::from_secs(30),
            Duration::from_secs(5),
        )
        .unwrap();
        child.cancel();
        assert_eq!(
            child.cancellation_state(),
            AttemptCancellationState::Cancelled
        );
        assert_eq!(
            parent.cancellation_state(),
            AttemptCancellationState::Active
        );
    }

    #[test]
    fn child_deadline_cannot_exceed_parent() {
        let now = Instant::now();
        let parent = AttemptContext::new_at(
            AttemptId::new("parent-bound").unwrap(),
            now,
            now + Duration::from_secs(30),
            Duration::from_secs(5),
        )
        .unwrap();
        assert!(matches!(
            BuildAttemptContext::new(
                parent,
                AttemptId::new("build-bound").unwrap(),
                now + Duration::from_secs(31),
                Duration::from_secs(5),
            ),
            Err(AttemptAdmissionError::Expired)
        ));
    }

    #[test]
    fn participant_children_share_build_cancellation_not_parent_cancellation() {
        let now = Instant::now();
        let parent_one = AttemptContext::new_at(
            AttemptId::new("parent-one").unwrap(),
            now,
            now + Duration::from_secs(60),
            Duration::from_secs(10),
        )
        .unwrap();
        let parent_two = AttemptContext::new_at(
            AttemptId::new("parent-two").unwrap(),
            now,
            now + Duration::from_secs(60),
            Duration::from_secs(10),
        )
        .unwrap();
        let group = BuildAttemptGroup::new(
            AttemptId::new("shared-build").unwrap(),
            now + Duration::from_secs(30),
            Duration::from_secs(5),
        )
        .unwrap();
        let child_one = group.child(parent_one.clone()).unwrap();
        let child_two = group.child(parent_two.clone()).unwrap();
        child_one.cancel();
        assert_eq!(
            child_two.cancellation_state(),
            AttemptCancellationState::Cancelled
        );
        assert_eq!(
            parent_one.cancellation_state(),
            AttemptCancellationState::Active
        );
        assert_eq!(
            parent_two.cancellation_state(),
            AttemptCancellationState::Active
        );

        let parent_three = AttemptContext::new_at(
            AttemptId::new("parent-three").unwrap(),
            now,
            now + Duration::from_secs(60),
            Duration::from_secs(10),
        )
        .unwrap();
        let child_three = BuildAttemptGroup::new(
            AttemptId::new("parent-cancel-build").unwrap(),
            now + Duration::from_secs(30),
            Duration::from_secs(5),
        )
        .unwrap()
        .child(parent_three.clone())
        .unwrap();
        parent_three.cancel();
        assert_eq!(
            child_three.admit_work(),
            Err(AttemptAdmissionError::Cancelled)
        );
    }

    #[test]
    fn child_reserves_cleanup_and_never_credits_late_work() {
        let now = Instant::now();
        let parent = AttemptContext::new_at(
            AttemptId::new("cleanup-parent").unwrap(),
            now,
            now + Duration::from_secs(1),
            Duration::from_millis(100),
        )
        .unwrap();
        let group = BuildAttemptGroup::new(
            AttemptId::new("cleanup-build").unwrap(),
            Instant::now() + Duration::from_millis(80),
            Duration::from_millis(40),
        )
        .unwrap();
        let child = group.child(parent).unwrap();
        child.admit_work().unwrap();
        std::thread::sleep(Duration::from_millis(45));
        assert_eq!(
            child.admit_work(),
            Err(AttemptAdmissionError::CleanupReserve)
        );
        child.admit_cleanup().unwrap();
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(child.admit_cleanup(), Err(AttemptAdmissionError::Expired));
    }
}
