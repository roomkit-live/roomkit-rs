//! The work a kit runs in the background, which it tracks (decision 3 of
//! step 2b), and the kit's close, which drains it, then ends what is left.

use std::time::Duration;

use super::RoomKit;
use crate::store::ConversationStore;
use crate::tasks::Kind;

/// How long the kit's close lets the work under way run to its end, each
/// room's deliveries and the turns they run, before it cuts it: the Python
/// implementation's lane `close_grace`.
const CLOSE_GRACE: Duration = Duration::from_secs(5);

impl<S: ConversationStore> RoomKit<S> {
    /// Closes the kit: the work under way drains, then what is left is
    /// ended, and the channels close, as the Python implementation's
    /// `close` does.
    ///
    /// 1. The kit is closed: every call that runs an operation fails from
    ///    now on with [`KitError::ShutDown`](crate::KitError::ShutDown): an
    ///    inbound call, a lifecycle change, a direct change of a stored
    ///    event, [`commit_event`](Self::commit_event) and
    ///    [`check_all_timers`](Self::check_all_timers). Registering
    ///    channels and hooks, subscribing and reading the store go on
    ///    working. A second `close` returns at once.
    /// 2. The inbound calls running drain for up to 5 seconds, whether
    ///    their caller waits for them, was answered at the decision (RFC
    ///    §10.1 step 18) or left: each room's deliveries in its lane, the
    ///    turns they run, the writes decided for a caller gone. One that
    ///    ends meanwhile reports its result, as it would have.
    /// 3. The turns still running are then cut (RFC §10.6.8: the kit
    ///    closing cancels their cascades): each crosses its turn backend as
    ///    `cancel`, its executor cuts the tool calls it runs, reported
    ///    `cancelled` to the kit's observers, and the reader of a stream
    ///    stores what it read, the text it was handed marked `cancelled`
    ///    and each call it opened ended `cancelled`. The close waits for
    ///    that within twice the turns' cancel bound, 10 seconds by default.
    /// 4. The observer hooks still running after the fact (RFC §9.3)
    ///    finish, each within its hook's timeout.
    /// 5. What is left is aborted at its next wait: an inbound call lets go
    ///    of what it holds, its room lock, its places in the room's lane and
    ///    its admission ticket, and its caller, if it still waits, receives
    ///    `KitError::ShutDown`. An event it committed and had not delivered
    ///    yet stays undelivered, and the cascade it started stops there.
    /// 6. The turn backend takes no more turns, and each channel registered
    ///    is closed ([`Channel::close`](crate::Channel::close)), one after
    ///    the other, once: an AI agent stops the MCP servers whose tools it
    ///    serves.
    ///
    /// A call that runs in its caller's own task, a lifecycle change, a
    /// direct change of a stored event or `commit_event`, is not ended: it
    /// runs to its end, and the observer hooks it would start run no more
    /// once the work is aborted.
    ///
    /// The Python implementation drains its rooms' lanes one after the
    /// other, each for up to 5 seconds; this one drains them together,
    /// under that bound.
    pub async fn close(&self) {
        let tasks = &self.inner.tasks;
        if !tasks.close() {
            return;
        }
        let _drained = tokio::time::timeout(CLOSE_GRACE, tasks.idle_of_kind(Kind::Work)).await;
        tasks.cut();
        self.inner.turns.cut_all().await;
        tasks.idle_of_kind(Kind::Observer).await;
        tasks.shut_down().await;
        self.inner.turns.close();
        self.inner.close_channels().await;
    }
}
