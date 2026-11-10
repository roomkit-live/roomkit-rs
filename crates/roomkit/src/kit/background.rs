//! The work a kit runs in the background, which it tracks (decision 3 of
//! step 2b), and the kit's close, which refuses every new call, cuts what
//! has not started, gives the running work one grace, then cuts it, in the
//! kit's one shutdown (`shutdown`).

use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

use super::RoomKit;
use super::shutdown::{self, Joined, Starting};
use crate::context::ExecutionContext;
use crate::error::RoomKitError;
use crate::store::ConversationStore;
use crate::tasks::CUT_UNWIND;

/// The one grace the kit's close gives its running work, a hook, a store
/// write, a transport send, from its start, before it cuts it (RFC
/// §12.10.4): the Python implementation's `CLOSE_GRACE_S`.
const CLOSE_GRACE: Duration = Duration::from_secs(5);

impl<S: ConversationStore> RoomKit<S> {
    /// Closes the kit: refuses every new call, cuts its work, closes every
    /// channel (RFC §12.10.4), as the Python implementation's `close` does.
    ///
    /// As it starts, every call made from then on returns
    /// [`RoomKitError::KitClosed`], whoever makes it, an outside caller, a
    /// hook, a tool, a channel's close: every call that runs an operation,
    /// and the registrations of a channel or a hook; the reads,
    /// [`get_room`](Self::get_room), [`get_timeline`](Self::get_timeline),
    /// [`get_binding`](Self::get_binding),
    /// [`list_bindings`](Self::list_bindings),
    /// [`list_members`](Self::list_members),
    /// [`get_channel`](Self::get_channel) and [`store`](Self::store),
    /// answer during the close and after it. What has not started is cut
    /// at once: a delivery queued in a room's lane is dropped, never sent,
    /// its call reading `cancellation_reason` `kit_closed` with no delivery
    /// result; a call waiting for its room's lock or its admission ticket
    /// is cut there, nothing of it stored, a message reading
    /// [`InboundResult`](crate::InboundResult) `blocked`, its `reason` and
    /// `cancellation_reason` `kit_closed`, any other call
    /// [`RoomKitError::KitClosed`]; every model turn is cut, its stream
    /// and its tool calls (RFC §10.6.8), what it streamed stored marked
    /// `cancelled`, an open tool call given its end row, `failed`, error
    /// `kit_closed`. A delivery set that would take its place in a room's
    /// lane from then on is dropped the same way.
    ///
    /// The work still running, a hook, a store write, a room change, a
    /// transport send, a cut turn storing what it said, gets one grace of 5
    /// seconds from the close's start; then what is left is cut: a call
    /// before its commit as one waiting for the lock, one whose commit had
    /// started reading its event back by its id within the unwind, stored
    /// (`kit_closed`) or not (a warning saying a read-back that failed),
    /// a delivery's sends `failed`, code `kit_closed`, retryable, each
    /// target delivered keeping `sent`, a store write and an observer hook
    /// aborted. A call under way when the close starts reads `kit_closed`
    /// in its `cancellation_reason`, even when its sends end within the
    /// grace, with the answers already stored; a call that commits within
    /// the grace is stored and delivered to no one. What is left after the
    /// grace is given one second to unwind; then the turn backend takes no
    /// more turns, its turns still running given up within a second of
    /// their own; then each channel registered is closed
    /// ([`Channel::close`](crate::Channel::close)), one after the other,
    /// once, a channel whose close panics noted and the next one closed
    /// all the same; what their closes started is cut and given a second,
    /// and the kit is sealed; last, its store is closed
    /// ([`ConversationStore::close`]), as Python's close releases it, and a
    /// read of the kit the store then fails answers
    /// [`RoomKitError::KitClosed`]. The close takes the grace, plus the turn
    /// backend's second for its turns, plus the channels' closes, plus the
    /// store's close, plus at
    /// most a second twice, which only work that ignores its cut spends, a
    /// task blocked in synchronous code, which an abort reaches once it
    /// yields; such work still running then is named in one warning.
    ///
    /// The kit has one shutdown: the first `close` starts it in a task of
    /// its own, and every `close`, concurrent or later, waits for that one,
    /// and returns once it ended; it never runs twice. A caller that stops
    /// waiting, its future dropped, by a `tokio::time::timeout` for
    /// instance, stops only its own wait: the shutdown runs to its end.
    ///
    /// A `close` from the kit's own work, a hook's body, a tool's call, a
    /// channel's delivery, a hook a delivery fired or a task spawned with
    /// [`keep_context`](crate::keep_context), neither waits for that work
    /// nor cuts it, whether it started the shutdown or joined one under
    /// way: the work it runs within is spared, and ends on its own once the
    /// call returns, storing and delivering nothing once the kit is sealed.
    /// The model turn such a call runs in is cut all the same, as every
    /// turn: the call awaiting `close` itself runs on, its open tool call's
    /// end row stored before the close returns, and what the turn does
    /// after the close is refused quietly; a call awaiting a close it
    /// started on a task of its own is cut with its turn, the close running
    /// on. A `close` from an observer the kit fires and forgets spares that
    /// observer alone. A `close` from the shutdown's own work, a channel's
    /// close, returns at once.
    ///
    /// # Errors
    ///
    /// Every caller, the first included, receives how the shutdown ended:
    /// [`RoomKitError::ExceptionGroup`] when a channel's close panicked,
    /// the channels after it closed all the same and the rest of the
    /// shutdown run to its end, as the Python implementation's `close`
    /// raises its `ExceptionGroup`; [`RoomKitError::ShutdownCancelled`]
    /// when the shutdown never ended, its task dropped with its runtime or
    /// panicked, as Python's raises `ShutdownCancelledError`.
    pub async fn close(&self) -> Result<(), RoomKitError> {
        let joined = self.inner.shutdown.join(&self.inner.tasks, |starting| {
            // Admission stops, and what has not started is cut, as the close
            // starts: the runs are told, and every turn is cut, the one a
            // close's own call runs in included (RFC §12.10.4).
            self.inner.tasks.close();
            self.inner.turns.cut_all();
            self.inner.verification.cancel_timers();
            let Starting { marks, ended } = starting;
            let kit = self.clone();
            let context = ExecutionContext::current();
            tokio::spawn(marks.scoped(async move {
                let failures = kit.shut_down(context).await;
                ended.send_replace(Some(failures));
            }));
        });
        match joined {
            Joined::Own => Ok(()),
            Joined::Waits(ended) => shutdown::wait_for(ended).await,
        }
    }

    /// The kit's own work stops within `grace`: see [`shut_down`](Self::shut_down).
    async fn close_kit_work(&self, grace: Instant) {
        // The kit's own work stops within the grace too: its delivery
        // backend's worker, its items kept (RFC §22.2), as the Python
        // implementation's close stops it while the channels are open.
        if let Some(backend) = &self.inner.delivery_backend
            && let Ok(Err(error)) = tokio::time::timeout_at(grace, backend.close()).await
        {
            tracing::warn!(target: "roomkit::kit", %error, "the delivery backend's close failed");
        }
        // So do its delegated tasks: each one its runner holds ends
        // cancelled, its observers told, nothing handed back (RFC §23.3), as
        // the Python implementation's close closes its task runner; and each
        // inline one, as its close cancels the runs it holds.
        if tokio::time::timeout_at(grace, self.inner.task_runner.close())
            .await
            .is_err()
        {
            tracing::warn!(target: "roomkit::kit", "the task runner's close outlasted the grace");
        }
        if tokio::time::timeout_at(grace, self.inner.inline_tasks.close())
            .await
            .is_err()
        {
            tracing::warn!(target: "roomkit::kit", "the inline delegations' close outlasted the grace");
        }
    }

    /// The kit's one shutdown, in the steps [`close`](Self::close) lists:
    /// the channels whose close failed.
    async fn shut_down(&self, context: ExecutionContext) -> shutdown::Ended {
        let (tasks, turns) = (&self.inner.tasks, &self.inner.turns);
        // The running work's one grace, the cut turns storing what they
        // said included, the one a close's own call runs in among them.
        let grace = Instant::now() + CLOSE_GRACE;
        // Beside the kit's own work, each within the one grace, neither holding
        // the other back: the discussions' turns, in the context of the close's
        // caller, so that a close from a hook's body under the room's lock takes
        // no lock that body holds.
        let stopping = context.scope(self.inner.discussions.stop_all());
        let stopping = async {
            if tokio::time::timeout_at(grace, stopping).await.is_err() {
                tracing::warn!(target: "roomkit::kit", "stopping the discussions outlasted the grace");
            }
        };
        tokio::join!(stopping, self.close_kit_work(grace));
        let readers_done = || turns.reads_none();
        let waited = tasks.unspared_idle(readers_done, turns.reads_changed());
        drop(tokio::time::timeout_at(grace, waited).await);
        tasks.cut();
        self.unwind().await;
        // The turn backend takes no more turns; those still running are given
        // up within an unwind of their own, which waits for the turns alone,
        // as the Python implementation's turn backend's close.
        turns.close();
        self.unwind_turns().await;
        let failures = self.close_channels_with_observers().await;
        // What the channels' closes started is cut, its grace over.
        tasks.cut();
        self.unwind().await;
        let left = tasks.left();
        if !left.is_empty() {
            tracing::warn!(
                target: "roomkit::kit",
                left = ?left,
                "closing with work still running after its cut: it is abandoned"
            );
        }
        tasks.seal();
        // The store goes last, once no work can reach it, as the Python
        // implementation's close releases it after its channels (RFC
        // §12.10.4).
        self.inner.store_released.store(true, Ordering::Release);
        self.inner.store.close().await;
        // Then its lock managers (an advisory lock's pool), as the Python
        // implementation's close.
        self.inner.close_lock_managers().await;
        // Then the status bus, as the Python implementation's close.
        self.inner.status_bus.close().await;
        // Then the telemetry, which ends its active spans and exports them,
        // as the Python implementation's close.
        self.inner.close_telemetry().await;
        failures
    }

    /// A channel may wait for its final observer to start. Admission
    /// gives that observer no exemption from the last cut and ends here,
    /// before the cut. The usual rule still spares an observer's own close.
    async fn close_channels_with_observers(&self) -> shutdown::Ended {
        let _observers = self.inner.tasks.closing_observers();
        let failures = self.inner.close_channels().await;
        // Subscriptions end with the channels, as in Python's close.
        self.inner.realtime.close().await;
        failures
    }

    /// Lets the turns the turn backend's close gave up unwind, within
    /// [`CUT_UNWIND`]; what is left of them then is aborted.
    async fn unwind_turns(&self) {
        let tasks = &self.inner.tasks;
        if tokio::time::timeout(CUT_UNWIND, tasks.unspared_turns_idle())
            .await
            .is_err()
        {
            tasks.abort_left_turns();
            tokio::task::yield_now().await;
        }
    }

    /// Lets the work the close cut unwind, within [`CUT_UNWIND`]; what is
    /// left of it then is aborted, the runs included.
    async fn unwind(&self) {
        let tasks = &self.inner.tasks;
        let none = Notify::new();
        let unwound = tasks.unspared_idle(|| true, &none);
        if tokio::time::timeout(CUT_UNWIND, unwound).await.is_err() {
            tasks.abort_left();
            // The aborts are taken at the tasks' next poll.
            tokio::task::yield_now().await;
        }
    }
}

#[cfg(test)]
#[path = "tests/shutdown_contracts.rs"]
mod shutdown_contracts;

#[cfg(test)]
#[path = "tests/closing_observers.rs"]
mod closing_observers;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kit::RoomKitOptions;
    use crate::tasks::Kind;

    /// RFC §12.10.4: the production bound, pinned once: the tests run it on
    /// tokio's paused clock.
    #[test]
    fn the_close_gives_five_seconds_of_grace_and_one_second_of_unwind() {
        assert_eq!(
            (CLOSE_GRACE, CUT_UNWIND),
            (Duration::from_secs(5), Duration::from_secs(1))
        );
    }

    // Python: tests/test_kit_close.py::test_work_ignoring_its_cut_is_abandoned_once_the_unwind_ran_out
    // These retained guards exercise the defensive tracker bound. Tokio
    // destroys cooperative futures on abort; this does not simulate a
    // Python coroutine catching cancellation or a remote executor.
    #[tokio::test(start_paused = true)]
    async fn retained_work_is_abandoned_once_and_named_after_its_unwinds() {
        for turn in [false, true] {
            let (logs, _capture) = crate::test_warnings::Warnings::capture();
            let kit = RoomKit::new(RoomKitOptions::default());
            let mut kinds = vec![
                Kind::Observer,
                Kind::Observer,
                Kind::Write,
                Kind::Run,
                Kind::Call,
            ];
            if turn {
                kinds.push(Kind::Turn);
            }
            let held: Vec<_> = kinds
                .into_iter()
                .map(|kind| kit.inner.tasks.track_unabortable(kind))
                .collect();
            let started = Instant::now();
            kit.close().await.unwrap();
            let unwinds = if turn { 3 } else { 2 };
            assert_eq!(started.elapsed(), CLOSE_GRACE + CUT_UNWIND * unwinds);
            let mut left = vec![
                "a call",
                "a store write",
                "an inbound run",
                "an observer hook",
            ];
            if turn {
                left.insert(2, "a turn");
            }
            assert_eq!(kit.inner.tasks.left(), left);
            let expected = format!("left={left:?}");
            let warnings = logs.containing("");
            assert_eq!(warnings.len(), 1, "{warnings:?}");
            assert!(
                warnings[0]
                    .contains("closing with work still running after its cut: it is abandoned")
            );
            assert!(warnings[0].contains(&expected), "{warnings:?}");
            kit.close().await.unwrap();
            assert_eq!(logs.containing(""), warnings);
            drop(held);
            assert_eq!(kit.inner.tasks.left(), Vec::<&str>::new());
        }
    }
}
