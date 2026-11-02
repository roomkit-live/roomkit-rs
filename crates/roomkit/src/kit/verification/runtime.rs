//! What the kit holds for identity verification (RFC §11.7): its policy and
//! the timers that end verifications and requests when their time runs out
//! (RFC §11.7.5). One identity's transitions are serialized by the kit's
//! lock manager, under the identity's key (`transitions::lock_identity`).

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use roomkit_core::model::VerificationPolicy;
use tokio::task::AbortHandle;

/// The kit's verification state, Python's `_verification_policy` and
/// `_verification_timers`; whether its pipeline labels and holds (Python's
/// `_verification_inbound`) is the core's `KitConfig::verification`.
#[derive(Debug)]
pub(crate) struct Verifications {
    /// The policy the kit was built with, the defaults for none.
    pub(super) policy: VerificationPolicy,
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
            policy: policy.unwrap_or_default(),
            timers: Mutex::default(),
            generation: AtomicU64::new(0),
        }
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

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

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
