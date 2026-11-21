//! The agent's permission requests still waiting on their decision: Python's
//! `PendingPermissions`.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use tokio::sync::oneshot;

/// Each session's pending requests: an id, and what abandons its decision.
type BySession = HashMap<String, Vec<(u64, oneshot::Sender<()>)>>;

/// The permission requests of each session whose decision is pending.
///
/// ACP: a client that sends `session/cancel` MUST answer every pending
/// `session/request_permission` of that session with the `cancelled`
/// outcome. Cancelling a session abandons the decisions still pending in it,
/// the external tool handler's included (RFC §6.4).
#[derive(Default)]
pub(in crate::channel) struct PendingPermissions {
    by_session: Mutex<BySession>,
    next: AtomicU64,
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
        let (abandon, mut abandoned) = oneshot::channel();
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.lock()
            .entry(session.to_owned())
            .or_default()
            .push((id, abandon));
        let _forget = Forget {
            pending: self,
            session,
            id,
        };
        let decided = tokio::select! {
            biased;
            Ok(()) = &mut abandoned => None,
            decided = decision => Some(decided),
        };
        // A decision made as the session was cancelled is abandoned all the same.
        decided.filter(|_| abandoned.try_recv().is_err())
    }

    /// Abandon every decision pending in `session`: each request is answered
    /// `cancelled`, and its decision is dropped.
    pub(in crate::channel) fn abandon(&self, session: &str) {
        let asking = self.lock().remove(session).unwrap_or_default();
        for (_, abandon) in asking {
            let _decided = abandon.send(());
        }
    }
}

/// Takes a request off its session's list however its decision ends.
struct Forget<'a> {
    pending: &'a PendingPermissions,
    session: &'a str,
    id: u64,
}

impl Drop for Forget<'_> {
    fn drop(&mut self) {
        let mut by_session = self.pending.lock();
        if let Some(asking) = by_session.get_mut(self.session) {
            asking.retain(|(id, _)| *id != self.id);
            if asking.is_empty() {
                by_session.remove(self.session);
            }
        }
    }
}
