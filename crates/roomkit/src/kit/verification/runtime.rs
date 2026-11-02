//! What the kit holds for identity verification (RFC §11.7): its policy,
//! the locks that serialize one identity's transitions, and the timers that
//! end verifications and requests when their time runs out (RFC §11.7.5).

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::VerificationPolicy;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tokio::task::AbortHandle;

/// The kit's verification state, Python's `_verification_policy`,
/// `_verification_inbound` and `_verification_timers`, and the identity
/// locks it takes from its lock manager.
#[derive(Debug)]
pub(crate) struct Verifications {
    /// The policy the kit was built with, the defaults for none.
    pub(super) policy: VerificationPolicy,
    /// Whether the kit was built with a policy: the inbound pipeline then
    /// labels and holds (RFC §11.7.3).
    #[expect(dead_code, reason = "the inbound step 5b reads it (RMK-1172)")]
    pub(super) inbound: bool,
    locks: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
    timers: Mutex<HashMap<String, Timer>>,
    /// The generation of the next timer armed.
    generation: AtomicU64,
}

/// An armed timer: what aborts it, and the generation that tells it from a
/// newer timer of its key.
#[derive(Debug)]
struct Timer {
    generation: u64,
    abort: AbortHandle,
}

impl Verifications {
    /// The state of a kit built with `policy`.
    pub(crate) fn new(policy: Option<VerificationPolicy>) -> Self {
        Self {
            inbound: policy.is_some(),
            policy: policy.unwrap_or_default(),
            locks: Mutex::default(),
            timers: Mutex::default(),
            generation: AtomicU64::new(0),
        }
    }

    /// Waits for the lock of `key`, one identity's, and returns its guard.
    pub(super) async fn lock(&self, key: &str) -> IdentityGuard<'_> {
        let mutex = Arc::clone(lock(&self.locks).entry(key.to_owned()).or_default());
        let mut guard = IdentityGuard {
            guard: None,
            waiting: Some(Arc::clone(&mutex)),
            key: key.to_owned(),
            verifications: self,
        };
        guard.guard = Some(mutex.lock_owned().await);
        guard.waiting = None;
        guard
    }

    /// Runs `work` as the timer of `key`, replacing (and aborting) the one
    /// armed under it; `work` is handed the generation it forgets itself by
    /// once it fires ([`fired`](Self::fired)).
    pub(super) fn arm<F>(&self, key: String, work: impl FnOnce(u64) -> F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let generation = self.generation.fetch_add(1, Ordering::Relaxed);
        let task = tokio::spawn(work(generation));
        let timer = Timer {
            generation,
            abort: task.abort_handle(),
        };
        if let Some(replaced) = lock(&self.timers).insert(key, timer) {
            replaced.abort.abort();
        }
    }

    /// Aborts the timer of `key`, if one is armed.
    pub(super) fn disarm(&self, key: &str) {
        if let Some(timer) = lock(&self.timers).remove(key) {
            timer.abort.abort();
        }
    }

    /// Forgets the timer of `key` that fired, unless a newer one took its
    /// key: from then on it runs to its end, no disarm cutting it.
    pub(super) fn fired(&self, key: &str, generation: u64) {
        let mut timers = lock(&self.timers);
        if timers
            .get(key)
            .is_some_and(|timer| timer.generation == generation)
        {
            timers.remove(key);
        }
    }

    /// Aborts every timer: the kit is closing (Python's
    /// `_cancel_verification_timers`).
    pub(crate) fn cancel_timers(&self) {
        for (_, timer) in lock(&self.timers).drain() {
            timer.abort.abort();
        }
    }
}

/// A held identity lock; dropping it releases the lock, and forgets it once
/// nobody holds or waits for it.
#[derive(Debug)]
pub(super) struct IdentityGuard<'verifications> {
    guard: Option<OwnedMutexGuard<()>>,
    /// The waiter's handle on the mutex, until it holds it.
    waiting: Option<Arc<AsyncMutex<()>>>,
    key: String,
    verifications: &'verifications Verifications,
}

impl Drop for IdentityGuard<'_> {
    fn drop(&mut self) {
        drop(self.guard.take());
        drop(self.waiting.take());
        let mut locks = lock(&self.verifications.locks);
        // A waiter cloned the mutex under this same lock before waiting, so a
        // mutex only the map still holds has neither holder nor waiter.
        if locks
            .get(&self.key)
            .is_some_and(|mutex| Arc::strong_count(mutex) == 1)
        {
            locks.remove(&self.key);
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_released_identity_lock_leaves_nothing_behind() {
        let verifications = Verifications::new(None);
        drop(verifications.lock("verification::alice").await);
        assert!(lock(&verifications.locks).is_empty());
    }

    // Python: core/mixins/verification.py::_forget_timer, "unless a newer one
    // took its key".
    #[tokio::test]
    async fn a_fired_timer_forgets_itself_but_not_a_newer_one() {
        let verifications = Verifications::new(None);
        verifications.arm("k".to_owned(), |_| std::future::pending());
        verifications.arm("k".to_owned(), |_| std::future::pending());
        verifications.fired("k", 0);
        assert!(lock(&verifications.timers).contains_key("k"));
        verifications.fired("k", 1);
        assert!(lock(&verifications.timers).is_empty());
    }
}
