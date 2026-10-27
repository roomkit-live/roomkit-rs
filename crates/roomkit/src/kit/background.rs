//! The work a kit runs in the background, which it tracks (decision 3 of
//! step 2b): waiting until none runs, and ending it when the kit shuts
//! down.

use super::RoomKit;
use crate::store::ConversationStore;

impl<S: ConversationStore> RoomKit<S> {
    /// Waits until the kit runs nothing in the background: no observer hook
    /// running after the fact (RFC §9.3), no timer sweep under way
    /// ([`run_timers`](Self::run_timers)), and no inbound call running,
    /// whether its caller waits for it, was answered at its decision (RFC
    /// §10.1 step 18) or stopped waiting. What one of
    /// them starts, an observer its delivery fires, a call its channel
    /// makes, is waited for too. A call that runs in its caller's own task,
    /// a lifecycle change, a direct change of a stored event or
    /// [`commit_event`](Self::commit_event), is its caller's to wait for.
    ///
    /// Work that keeps coming keeps the kit busy: `idle` returns at a moment
    /// nothing runs. Awaited from the kit's own work, a hook or a channel,
    /// it waits for itself: the host, a test or an example awaits it once
    /// its calls returned.
    pub async fn idle(&self) {
        self.inner.tasks.idle().await;
    }

    /// Shuts the kit down, and returns once the work it ran in the
    /// background is over:
    ///
    /// - every call that runs an operation fails from now on with
    ///   [`KitError::ShutDown`](crate::KitError::ShutDown): an inbound call,
    ///   a lifecycle change, a direct change of a stored event,
    ///   [`commit_event`](Self::commit_event) and
    ///   [`check_timers`](Self::check_timers). Registering channels and
    ///   hooks, subscribing and reading the store go on working;
    /// - the background work is ended at its next wait. An observer hook
    ///   running after the fact is dropped, and so is a timer sweep under
    ///   way, the room lock it holds let go. An inbound call running stops
    ///   and lets go of what it holds, its room lock, its places in the
    ///   room's lane and its admission ticket; its caller, if it still
    ///   waits, receives `KitError::ShutDown`. An event it committed and
    ///   had not delivered yet stays undelivered, and the cascade it started
    ///   stops there;
    /// - a call that runs in its caller's own task, a lifecycle change, a
    ///   direct change of a stored event or `commit_event`, is not ended: it
    ///   runs to its end, and the observer hooks it would start run no more;
    /// - a timer sweeper ([`run_timers`](Self::run_timers)) stops.
    ///
    /// To let the background work finish first, await [`idle`](Self::idle)
    /// before, within the grace the host gives it:
    ///
    /// ```no_run
    /// # async fn stop(kit: roomkit::RoomKit) {
    /// let grace = std::time::Duration::from_secs(10);
    /// let _finished = tokio::time::timeout(grace, kit.idle()).await;
    /// kit.shutdown().await;
    /// # }
    /// ```
    ///
    /// Called again, it returns once the work the first call ended is over.
    ///
    /// The turns running are cut first (RFC §10.6.8: the kit closing
    /// cancels their cascades): each crosses its turn backend as `cancel`,
    /// its executor cuts the tool calls it runs, reported `cancelled` to the
    /// kit's observers, and the reader of a stream stores what it read, the
    /// text it was handed marked `cancelled` and each call it opened ended
    /// `cancelled`. The shutdown waits for that within twice the turns'
    /// cancel bound, 10 seconds by default; with no turn running, it waits
    /// for nothing. Then the turn backend takes no more turns, and each
    /// channel registered is closed ([`Channel::close`](crate::Channel::close)),
    /// one after the other, once: an AI agent stops the MCP servers whose
    /// tools it serves.
    pub async fn shutdown(&self) {
        self.inner.tasks.close();
        self.inner.turns.cut_all().await;
        self.inner.tasks.shut_down().await;
        self.inner.turns.close();
        self.inner.close_channels().await;
    }
}
