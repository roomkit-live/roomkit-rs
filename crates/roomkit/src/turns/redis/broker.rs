//! What the backend asks of the services that carry its messages between
//! processes and keep what they share (RFC §10.6.7, §10.6.8): the
//! messages, and the registry. One service may give both. The backend
//! reaches them through these traits alone.
use std::collections::BTreeMap;
use std::time::Duration;

use super::registry::Beat;
use super::stream::Message;
use crate::RoomKitError;
use crate::turns::TurnFuture;

/// An entry read from an inbox: its id, and its message unless malformed.
pub(super) struct Entry {
    pub(super) id: String,
    pub(super) message: Option<Message>,
}

/// The executors and the room owners beating, as the registry keeps them.
pub(super) struct View {
    /// The registry's time, in milliseconds: the clock of every beat.
    pub(super) now: i64,
    /// Each executor's last beat, and its declaration when it has one.
    pub(super) executors: BTreeMap<String, (i64, Option<String>)>,
    /// Each room owner's last beat, by its node.
    pub(super) owners: BTreeMap<String, i64>,
}

/// What [`TurnRegistry::beats`] reads.
pub(super) struct Beats {
    /// The last beat of each executor asked, in the order asked; none for
    /// one the registry does not know.
    pub(super) last: Vec<Option<i64>>,
    /// For each node asked, whether it runs each of the turns named.
    pub(super) running: Vec<Vec<bool>>,
}

/// A reader of one node's inbox, on a connection of its own.
pub(super) trait Inbox: Send {
    /// The entries after the one `last` names, in the order posted, waiting
    /// for the first no longer than the reader was told; from the inbox's
    /// first entry for an empty `last`.
    fn read<'a>(&'a mut self, last: &'a str) -> TurnFuture<'a, Result<Vec<Entry>, RoomKitError>>;
}

/// A writer of beats on a connection of its own: the heartbeat's, which
/// runs apart from the turns.
pub(super) trait Beater: Send {
    /// As [`TurnRegistry::beat`].
    fn beat<'a>(&'a mut self, beat: &'a Beat) -> TurnFuture<'a, Result<(), RoomKitError>>;
}

/// The messages between processes: an inbox a node, read in the order
/// posted.
pub(super) trait TurnBroker: Send + Sync + 'static {
    /// Add `message` to `node`'s inbox, after those it holds.
    fn post<'a>(
        &'a self,
        node: &'a str,
        message: &'a Message,
    ) -> TurnFuture<'a, Result<(), RoomKitError>>;

    /// A reader of `node`'s inbox, each read waiting `block` at most.
    fn inbox<'a>(
        &'a self,
        node: &'a str,
        block: Duration,
    ) -> TurnFuture<'a, Result<Box<dyn Inbox>, RoomKitError>>;

    /// Let go of entries read from `node`'s inbox: `ids` in the order
    /// read, each entry before the last of them read too.
    fn acknowledge<'a>(
        &'a self,
        node: &'a str,
        ids: &'a [String],
    ) -> TurnFuture<'a, Result<(), RoomKitError>>;
}

/// What the processes share: who beats, who holds a room, the presence
/// last published.
pub(super) trait TurnRegistry: Send + Sync + 'static {
    /// Reach the registry, a first time only.
    fn connect(&self) -> TurnFuture<'_, Result<(), RoomKitError>>;

    /// Whether the registry was reached: a node that never reached it has
    /// nothing to withdraw there.
    fn connected(&self) -> bool;

    /// Write a node's beat: what it declares, what it withdrew, the turns
    /// it runs, at the registry's time and as one change. A beat older than
    /// the node's last one is ignored.
    fn beat<'a>(&'a self, beat: &'a Beat) -> TurnFuture<'a, Result<(), RoomKitError>>;

    /// A writer of beats on a connection of its own.
    fn beater(&self) -> TurnFuture<'_, Result<Box<dyn Beater>, RoomKitError>>;

    /// The executors and the owners beating; what last beat more than
    /// `forget_after_ms` ago is forgotten.
    fn view(&self, forget_after_ms: i64) -> TurnFuture<'_, Result<View, RoomKitError>>;

    /// `node` is no room owner any more.
    fn forget_owner<'a>(&'a self, node: &'a str) -> TurnFuture<'a, Result<(), RoomKitError>>;

    /// The last beat of each of `members`, and for each node of `running`
    /// whether it runs each of the turns named. What is read of one node is
    /// read as one of its beats left it.
    fn beats<'a>(
        &'a self,
        members: &'a [&'a str],
        running: &'a [(&'a str, Vec<&'a str>)],
    ) -> TurnFuture<'a, Result<Beats, RoomKitError>>;

    /// The executor holding the room's state of `channel`.
    fn holder<'a>(
        &'a self,
        channel: &'a str,
        room: &'a str,
    ) -> TurnFuture<'a, Result<Option<String>, RoomKitError>>;

    /// Record `member` as the executor holding the room's state of
    /// `channel` from now.
    fn hold<'a>(
        &'a self,
        channel: &'a str,
        room: &'a str,
        member: &'a str,
    ) -> TurnFuture<'a, Result<(), RoomKitError>>;

    /// Forget the room's holder of `channel`.
    fn forget_holder<'a>(
        &'a self,
        channel: &'a str,
        room: &'a str,
    ) -> TurnFuture<'a, Result<(), RoomKitError>>;

    /// Record the presence last published of `channel_id` in `room_id`:
    /// whether it is a change, which one caller alone is told, the one that
    /// then publishes it; `None` forgets it.
    fn claim_presence<'a>(
        &'a self,
        channel_id: &'a str,
        room_id: &'a str,
        online: Option<bool>,
    ) -> TurnFuture<'a, Result<bool, RoomKitError>>;
}
