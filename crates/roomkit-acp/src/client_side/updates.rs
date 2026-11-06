//! The agent's session updates, a queue per session: each session's
//! updates taken by the client one after the other, in the order the
//! agent sent them, apart from the other sessions', and never by the
//! reader, which goes on reading.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use agent_client_protocol_schema::v1::SessionNotification;
use tokio::sync::Notify;

tokio::task_local! {
    /// The connection and the session whose update the client is taking,
    /// in the task taking it.
    static TAKING: (usize, String);
}

/// The sessions' queues, the updates numbered in the order they were read.
#[derive(Default)]
pub(super) struct Updates {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    next: u64,
    sessions: HashMap<String, Queue>,
    /// The connection's tasks were stopped: nothing is taken any more.
    closed: bool,
}

/// One session's updates: those waiting, the one the client is taking, and
/// what tells a waiter one was taken.
#[derive(Default)]
struct Queue {
    waiting: VecDeque<(u64, SessionNotification)>,
    current: Option<u64>,
    /// A task takes this session's updates.
    running: bool,
    changed: Arc<Notify>,
}

impl Queue {
    /// Whether every update numbered `target` or before was taken.
    fn took(&self, target: u64) -> bool {
        let oldest = self
            .current
            .or(self.waiting.front().map(|(number, _)| *number));
        oldest.is_none_or(|oldest| oldest > target)
    }
}

impl Updates {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queues `notification` on its session; the session's id when no task
    /// takes its updates yet, the caller then starting one.
    pub(super) fn queue(&self, notification: SessionNotification) -> Option<String> {
        let mut inner = self.lock();
        let number = inner.next;
        inner.next += 1;
        let session = notification.session_id.0.to_string();
        let queue = inner.sessions.entry(session.clone()).or_default();
        queue.waiting.push_back((number, notification));
        if queue.running {
            return None;
        }
        queue.running = true;
        Some(session)
    }

    /// The next update of `session` for the task taking them; none once its
    /// queue is empty, the queue then forgotten.
    pub(super) fn next(&self, session: &str) -> Option<SessionNotification> {
        let mut inner = self.lock();
        let queue = inner.sessions.get_mut(session)?;
        if let Some((number, notification)) = queue.waiting.pop_front() {
            queue.current = Some(number);
            return Some(notification);
        }
        if let Some(queue) = inner.sessions.remove(session) {
            queue.changed.notify_waiters();
        }
        None
    }

    /// The client took `session`'s current update.
    pub(super) fn took(&self, session: &str) {
        if let Some(queue) = self.lock().sessions.get_mut(session) {
            queue.current = None;
            queue.changed.notify_waiters();
        }
    }

    /// Nothing is taken any more: every waiter goes.
    pub(super) fn close(&self) {
        let mut inner = self.lock();
        inner.closed = true;
        for queue in inner.sessions.values() {
            queue.changed.notify_waiters();
        }
    }

    /// Runs `taking`, the client taking an update of `session` on the
    /// connection `connection`, marked as such.
    pub(super) async fn taking<F: Future>(
        connection: usize,
        session: String,
        taking: F,
    ) -> F::Output {
        TAKING.scope((connection, session), taking).await
    }

    /// Waits until `session` took every update queued now, the Python
    /// SDK's prompt waiting for its session's updates; at once when called
    /// from within one of them, which those after it wait behind.
    pub(super) async fn taken(&self, connection: usize, session: &str) {
        let within = TAKING
            .try_with(|(on, taking)| *on == connection && taking == session)
            .unwrap_or(false);
        if within {
            return;
        }
        let target = {
            let inner = self.lock();
            let Some(queue) = inner.sessions.get(session) else {
                return;
            };
            match queue
                .waiting
                .back()
                .map(|(number, _)| *number)
                .or(queue.current)
            {
                Some(target) => target,
                None => return,
            }
        };
        loop {
            let Some(changed) = self.waited(session, target) else {
                return;
            };
            let notified = changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.waited(session, target).is_none() {
                return;
            }
            notified.await;
        }
    }

    /// What tells that `session` took an update, while one numbered
    /// `target` or before is still to take; none once all are, or nothing
    /// is taken any more.
    fn waited(&self, session: &str, target: u64) -> Option<Arc<Notify>> {
        let inner = self.lock();
        if inner.closed {
            return None;
        }
        let queue = inner.sessions.get(session)?;
        (!queue.took(target)).then(|| Arc::clone(&queue.changed))
    }
}
