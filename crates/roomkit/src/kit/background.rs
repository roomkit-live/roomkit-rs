//! The work a kit runs in the background, which it tracks (decision 3 of
//! step 2b), and the kit's close, which drains it, then ends what is left,
//! in the kit's one shutdown (`shutdown`).

use std::collections::HashSet;
use std::time::Duration;

use tokio::time::Instant;

use super::RoomKit;
use super::shutdown::{self, Joined, Starting};
use crate::error::KitError;
use crate::store::ConversationStore;
use crate::tasks::{Kind, Tasks, Unit};

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
    /// The kit has one shutdown (RFC §12.10.4): the first `close` starts it
    /// in a task of its own, and every `close`, concurrent or later, waits
    /// for that one, and returns once it ended; it never runs twice. A
    /// caller that stops waiting, its future dropped, by a
    /// `tokio::time::timeout` for instance, stops only its own wait: the
    /// shutdown runs to its end.
    ///
    /// A `close` from the kit's own work, a hook's body, a tool's call, a
    /// channel's delivery, a hook a delivery fired or a task spawned with
    /// [`keep_context`](crate::keep_context), neither waits for that work
    /// nor cuts it, whether it started the shutdown or joined one under
    /// way: the work it runs within is spared, with the turns of a spared
    /// run's delivery set, the shutdown closes everything else, and the
    /// work ends on its own once the call returns, storing and delivering
    /// nothing once the kit is sealed: its writes and its channel calls are
    /// refused, a message whose commit is so refused failing
    /// [`KitError::ShutDown`], nothing of it stored, one refused past its
    /// commit reading `cancellation_reason` `kit_closed`. A `close` from an
    /// observer the kit fires and forgets spares that observer alone. What
    /// waits behind spared work is dropped at once: the deliveries queued
    /// behind its place in its room's lane, `kit_closed`, and the messages
    /// waiting for the room's lock or admission ticket it holds, refused
    /// [`KitError::ShutDown`]. A `close` from the shutdown's own work, a
    /// channel's close, returns at once.
    ///
    /// # Errors
    ///
    /// Every caller, the first included, receives how the shutdown ended:
    /// [`KitError::ChannelsFailedToClose`] when a channel's close panicked,
    /// the channels after it closed all the same and the rest of the
    /// shutdown run to its end, as the Python implementation's `close`
    /// raises its `ExceptionGroup`; [`KitError::ShutdownCancelled`] when
    /// the shutdown never ended, its task dropped with its runtime or
    /// panicked, as Python's raises `RoomKitError`.
    ///
    /// 1. The close begins. Calls go on being taken until the kit is sealed
    ///    (step 5), as Python's are until its store is.
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
    ///    until it is committed, whatever holds it, a hook or its own
    ///    commit's write (RMK-1010 item 41; Python's drain waits for
    ///    neither, and aligns). An inbound call whose commit runs past 10 s
    ///    so holds the close up to 15 s, its write's third wait ending 5 s
    ///    after the channels' close. One that ends meanwhile reports its
    ///    result, as it would have. A delivery set that would take its
    ///    place in a room's lane from now on is dropped, its event stored,
    ///    the call's result reading `cancellation_reason` `kit_closed`, as
    ///    Python's sealed lanes drop it.
    /// 4. The turns still running are then cut (RFC §10.6.8: the kit
    ///    closing cancels their cascades): each crosses its turn backend as
    ///    `cancel`, its executor cuts the tool calls it runs, reported
    ///    `cancelled` to the kit's observers, and the reader of a stream
    ///    stores what it read, the text it was handed marked `cancelled`
    ///    and each call it opened ended `cancelled`. The close waits for
    ///    that, then for the inbound calls whose turns it cut to commit what
    ///    their readers read, and no longer, within twice the turns' cancel
    ///    bound, 10 seconds by default. A turn the close spares is not cut.
    ///    What is left of the inbound calls is then aborted at its next
    ///    wait, letting go of what it holds, its room lock, its places in
    ///    the room's lane and its admission ticket: an event committed and
    ///    not delivered stays undelivered, and the cascade it started stops
    ///    there. A caller still waiting receives its result, its
    ///    `cancellation_reason` `lane_cancelled` for a delivery cut
    ///    running and `kit_closed` for one still queued or a commit that
    ///    ended after the abort or whose set the close dropped first, as
    ///    Python's lanes report them, the
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
    ///    once, a channel whose close panics noted and the next one closed
    ///    all the same, and the writes get 5 seconds more, counted from the
    ///    channels' close, as Python's third wait. A warning counts the
    ///    writes whose store call still runs.
    ///
    /// A call that runs in its caller's own task, a lifecycle change, a
    /// direct change of a stored event or `commit_event`, is not ended: it
    /// runs to its end, and the observer hooks it would start run no more
    /// once the kit is sealed.
    ///
    /// The Python implementation drains its rooms' lanes one after the
    /// other, each for up to 5 seconds; this one drains them together,
    /// under that bound.
    pub async fn close(&self) -> Result<(), KitError> {
        let joined = self.inner.shutdown.join(&self.inner.tasks, |starting| {
            self.inner.tasks.close();
            let Starting { marks, ended } = starting;
            let kit = self.clone();
            tokio::spawn(marks.scoped(async move {
                let failures = kit.shut_down().await;
                ended.send_replace(Some(failures));
            }));
        });
        match joined {
            Joined::Own => Ok(()),
            Joined::Waits(ended) => shutdown::wait_for(ended).await,
        }
    }

    /// The kit's one shutdown, in the steps [`close`](Self::close) lists:
    /// the channels whose close failed.
    async fn shut_down(&self) -> shutdown::Ended {
        let tasks = &self.inner.tasks;
        let writes = Instant::now() + CLOSE_WAIT;
        wait_for_writes(tasks, writes).await;
        self.drain().await;
        tasks.cut();
        let cut = self.inner.turns.cut_all(tasks.spared()).await;
        // A run whose turn was cut commits what its reader read before the
        // rest of the work is aborted, within the same bound: the commit's
        // write, which ends once the run went on from it.
        let commits = tasks.idle_of_children(Kind::Write, &cut.runs);
        let _committed = tokio::time::timeout_at(cut.until, commits).await;
        tasks.abort(Kind::Work).await;
        tasks.idle_of_kind(Kind::Observer).await;
        tasks.shut_down().await;
        self.inner.turns.close();
        wait_for_writes(tasks, writes).await;
        let failures = self.inner.close_channels().await;
        wait_for_writes(tasks, Instant::now() + CLOSE_WAIT).await;
        failures
    }

    /// Drains the work under way for up to the close's grace, but the work
    /// a caller of the close runs within: the work stuck behind spared
    /// work is dropped then, and again each time a caller joins (RFC
    /// §12.10.4), as it would move on only once the close returned.
    async fn drain(&self) {
        let tasks = &self.inner.tasks;
        tasks.drain();
        let draining = async {
            loop {
                let seen = tasks.spared_changes();
                tasks.drop_work_of(&self.stuck_behind(&tasks.spared()));
                if tasks.idle_or_spared(Kind::Work, seen).await {
                    return;
                }
            }
        };
        let _drained = tokio::time::timeout(CLOSE_GRACE, draining).await;
    }

    /// The units of the work stuck behind work in `spared`: the runs whose
    /// place in a room's lane, admission ticket or wait for a room's lock
    /// comes after a spared run's.
    fn stuck_behind(&self, spared: &HashSet<Unit>) -> HashSet<Unit> {
        let inner = &self.inner;
        let mut stuck = inner.lanes.queued_behind(spared);
        stuck.extend(inner.tickets.queued_behind(spared));
        stuck.extend(inner.locks.queued_behind(spared));
        stuck
    }
}

/// Waits for the store's writes running until `deadline` at most, never
/// cutting one: those still running then are counted in the log and left
/// to end on their own, as Python's `wait_for_held` names them.
async fn wait_for_writes(tasks: &Tasks, deadline: Instant) {
    let ended = tokio::time::timeout_at(deadline, tasks.idle_of_kind(Kind::Write)).await;
    // A write whose store call ended is over, its task's end only waiting
    // for its run to go on from it.
    let running = tasks.writes_storing();
    if ended.is_err() && running > 0 {
        tracing::warn!(
            target: "roomkit::kit",
            writes = running,
            "closing with decided writes still running, left to end on their own"
        );
    }
}
