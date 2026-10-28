//! The work a kit runs in the background, which it tracks (decision 3 of
//! step 2b), and the kit's close, which drains it, then ends what is left.

use std::time::Duration;

use tokio::time::Instant;

use super::RoomKit;
use crate::store::ConversationStore;
use crate::tasks::{Kind, Tasks};

/// How long the kit's close lets the work under way run to its end, each
/// room's deliveries and the turns they run, once it drains them, before it
/// cuts it: the Python implementation's lane `close_grace`.
const CLOSE_GRACE: Duration = Duration::from_secs(5);

/// How long the kit's close waits for the store's writes decided for a
/// caller, from its start and once more from the channels' close: the
/// Python implementation's `CLOSE_WAIT_S`. A write still running then is
/// left to end on its own, never cancelled.
const CLOSE_WAIT: Duration = Duration::from_secs(5);

impl<S: ConversationStore> RoomKit<S> {
    /// Closes the kit: the work under way drains, then what is left is
    /// ended, and the channels close, as the Python implementation's
    /// `close` does.
    ///
    /// 1. The close begins, and a second `close` returns at once. Calls go
    ///    on being taken until the kit is sealed (step 5), as Python's are
    ///    until its store is.
    /// 2. The store's writes decided for a caller, an event's commit among
    ///    them, get up to 5 seconds from the close's start to end, the work
    ///    going on meanwhile, as Python's `CLOSE_WAIT_S`; an inbound call's
    ///    commit's write ends once its event took its place in the room's
    ///    lane, as Python's decided write covers the lane's enqueue. A write
    ///    still running then is left to end on its own, never cancelled,
    ///    and counted in a warning: a caller still waiting for its call
    ///    reads what the write came to, its event or the store's error; a
    ///    write that fails once its caller is gone is logged as an error.
    /// 3. The inbound calls running then drain for up to 5 seconds, whether
    ///    their caller waits for them, was answered at the decision (RFC
    ///    §10.1 step 18) or left: each room's deliveries in its lane, the
    ///    turns they run, the observers their deliveries fire, and a call
    ///    still before its own commit. A call whose commit that decides it
    ///    is in flight is a decided write, which the drain does not wait
    ///    for: the waits for the writes do, as they do Python's. One that
    ///    ends meanwhile reports its result, as it would have. A delivery
    ///    set that would take its place in a room's lane from now on is
    ///    dropped, its event stored, the call's result reading
    ///    `cancellation_reason` `kit_closed`, as Python's sealed lanes drop
    ///    it.
    /// 4. The turns still running are then cut (RFC §10.6.8: the kit
    ///    closing cancels their cascades): each crosses its turn backend as
    ///    `cancel`, its executor cuts the tool calls it runs, reported
    ///    `cancelled` to the kit's observers, and the reader of a stream
    ///    stores what it read, the text it was handed marked `cancelled`
    ///    and each call it opened ended `cancelled`. The close waits for
    ///    that within twice the turns' cancel bound, 10 seconds by default.
    ///    What is left of the inbound calls is then aborted at its next
    ///    wait, letting go of what it holds, its room lock, its places in
    ///    the room's lane and its admission ticket: an event committed and
    ///    not delivered stays undelivered, and the cascade it started stops
    ///    there. A caller still waiting receives its result, its
    ///    `cancellation_reason` `lane_cancelled` for a delivery cut
    ///    running and `kit_closed` for one still queued or a commit that
    ///    ended after the abort, as Python's lanes report them, the
    ///    store's error for such a commit that failed, or
    ///    [`KitError::ShutDown`](crate::KitError::ShutDown) for a call cut
    ///    before its own commit, whatever its run committed for other
    ///    events first (RMK-1010 item 41).
    /// 5. The other observer hooks still running after the fact (RFC §9.3)
    ///    finish, each within its hook's timeout.
    /// 6. The kit is sealed: every call that runs an operation fails from
    ///    now on with `KitError::ShutDown`, an inbound call, a lifecycle
    ///    change, a direct change of a stored event,
    ///    [`commit_event`](Self::commit_event) and
    ///    [`check_all_timers`](Self::check_all_timers); registering
    ///    channels and hooks, subscribing and reading the store go on
    ///    working. What is left runs no more, but the store's writes.
    /// 7. The turn backend takes no more turns, and the store's writes get
    ///    what is left of their 5 seconds, those still running counted in a
    ///    warning again. Each channel registered is then closed
    ///    ([`Channel::close`](crate::Channel::close)), one after the other,
    ///    once, and the writes get 5 seconds more, counted from the
    ///    channels' close, as Python's third wait.
    ///
    /// A call that runs in its caller's own task, a lifecycle change, a
    /// direct change of a stored event or `commit_event`, is not ended: it
    /// runs to its end, and the observer hooks it would start run no more
    /// once the kit is sealed.
    ///
    /// The Python implementation drains its rooms' lanes one after the
    /// other, each for up to 5 seconds; this one drains them together,
    /// under that bound.
    pub async fn close(&self) {
        let tasks = &self.inner.tasks;
        if !tasks.close() {
            return;
        }
        let writes = Instant::now() + CLOSE_WAIT;
        wait_for_writes(tasks, writes).await;
        tasks.drain();
        let _drained = tokio::time::timeout(CLOSE_GRACE, tasks.drained()).await;
        tasks.cut();
        self.inner.turns.cut_all().await;
        tasks.abort(Kind::Work).await;
        tasks.idle_of_kind(Kind::Observer).await;
        tasks.shut_down().await;
        self.inner.turns.close();
        wait_for_writes(tasks, writes).await;
        self.inner.close_channels().await;
        wait_for_writes(tasks, Instant::now() + CLOSE_WAIT).await;
    }
}

/// Waits for the store's writes running until `deadline` at most, never
/// cutting one: those still running then are counted in the log and left
/// to end on their own, as Python's `wait_for_held` names them.
async fn wait_for_writes(tasks: &Tasks, deadline: Instant) {
    let ended = tokio::time::timeout_at(deadline, tasks.idle_of_kind(Kind::Write)).await;
    let running = tasks.running_of(Kind::Write);
    if ended.is_err() && running > 0 {
        tracing::warn!(
            target: "roomkit::kit",
            writes = running,
            "closing with decided writes still running, left to end on their own"
        );
    }
}
