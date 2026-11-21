//! The agent's permission requests still waiting on their decision: Python's
//! `PendingPermissions`.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::Notify;

/// One permission request, its decision being made.
#[derive(Default)]
struct Asking {
    abandoned: AtomicBool,
    /// Notified once the session's cancellation is out: the request is
    /// answered.
    woken: Notify,
}

/// Each session's pending requests.
type BySession = HashMap<String, Vec<Arc<Asking>>>;

/// The permission requests of each session whose decision is pending.
///
/// ACP: a client that sends `session/cancel` MUST answer every pending
/// `session/request_permission` of that session with the `cancelled`
/// outcome. Cancelling a session abandons the decisions still pending in it,
/// the external tool handler's included (RFC §6.4).
#[derive(Default)]
pub(in crate::channel) struct PendingPermissions {
    by_session: Mutex<BySession>,
}

impl PendingPermissions {
    fn lock(&self) -> MutexGuard<'_, BySession> {
        self.by_session
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The result of `decision`, a permission request's decision made in
    /// `session`; `None` once the session's prompt was cancelled while it
    /// was pending, the decision dropped.
    pub(in crate::channel) async fn decide<F: Future>(
        &self,
        session: &str,
        decision: F,
    ) -> Option<F::Output> {
        let asking = Arc::new(Asking::default());
        self.lock()
            .entry(session.to_owned())
            .or_default()
            .push(Arc::clone(&asking));
        let forget = Forget {
            pending: self,
            session,
            asking: &asking,
        };
        let decided = tokio::select! {
            biased;
            () = asking.woken.notified() => None,
            decided = decision => Some(decided),
        };
        drop(forget);
        if decided.is_some() && asking.abandoned.load(Ordering::Acquire) {
            // Made while the cancellation was being sent: dropped, and the
            // request answered once the cancellation is out.
            asking.woken.notified().await;
            return None;
        }
        decided
    }

    /// Abandon the decisions pending in `session` until the returned guard
    /// drops, once the session's cancellation is sent: a decision made
    /// meanwhile is dropped, and when the guard drops, however the sending
    /// ended, each request is answered `cancelled` and its decision dropped.
    pub(in crate::channel) fn abandon(&self, session: &str) -> Abandoned {
        // Marked under the lock a decision takes its request off the list
        // under: a request still listed reads its mark once decided.
        let by_session = self.lock();
        let asked = by_session.get(session).cloned().unwrap_or_default();
        for asking in &asked {
            asking.abandoned.store(true, Ordering::Release);
        }
        drop(by_session);
        Abandoned(asked)
    }
}

/// The requests a cancellation abandoned, woken when it drops.
#[must_use = "dropping it at once answers the requests before the cancellation goes"]
pub(in crate::channel) struct Abandoned(Vec<Arc<Asking>>);

impl Drop for Abandoned {
    fn drop(&mut self) {
        for asking in &self.0 {
            asking.woken.notify_one();
        }
    }
}

/// Takes a request off its session's list however its decision ends.
struct Forget<'a> {
    pending: &'a PendingPermissions,
    session: &'a str,
    asking: &'a Arc<Asking>,
}

impl Drop for Forget<'_> {
    fn drop(&mut self) {
        let mut by_session = self.pending.lock();
        if let Some(asked) = by_session.get_mut(self.session) {
            asked.retain(|asking| !Arc::ptr_eq(asking, self.asking));
            if asked.is_empty() {
                by_session.remove(self.session);
            }
        }
    }
}
