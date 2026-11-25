//! What the backend asks of the service that carries its messages between
//! processes and keeps their registry: a node's inbox, the processes'
//! beats, a room's holder and the presence last published (RFC §10.6.7,
//! §10.6.8). The backend reaches that service through this trait alone.
use std::time::Duration;

use super::registry::{Beat, View};
use super::stream::Message;
use crate::RoomKitError;
use crate::turns::TurnFuture;

/// An entry read from an inbox: its id, and its message unless malformed.
pub(super) struct Entry {
    pub(super) id: String,
    pub(super) message: Option<Message>,
}

/// A reader of one node's inbox, on a connection of its own.
pub(super) trait Inbox: Send {
    /// The entries after `last`, in order, waiting for the first no longer
    /// than the reader was told.
    fn read<'a>(&'a mut self, last: &'a str) -> TurnFuture<'a, Result<Vec<Entry>, RoomKitError>>;
}

/// A writer of beats on a connection of its own: the heartbeat's, which
/// runs apart from the turns.
pub(super) trait Beater: Send {
    /// As [`TurnBroker::beat`].
    fn beat<'a>(&'a mut self, beat: &'a Beat) -> TurnFuture<'a, Result<(), RoomKitError>>;
}

pub(super) trait TurnBroker: Send + Sync + 'static {
    /// Reach the service, a first time only.
    fn connect(&self) -> TurnFuture<'_, Result<(), RoomKitError>>;

    /// Whether the service was reached: a node that never reached it has
    /// nothing to withdraw there.
    fn connected(&self) -> bool;

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

    /// Delete from `node`'s inbox the entries read.
    fn acknowledge<'a>(
        &'a self,
        node: &'a str,
        ids: &'a [String],
    ) -> TurnFuture<'a, Result<(), RoomKitError>>;

    /// Write a node's beat: what it declares, what it withdrew, the turns
    /// it runs, at the service's time and as one change. A beat older than
    /// the node's last one is ignored.
    fn beat<'a>(&'a self, beat: &'a Beat) -> TurnFuture<'a, Result<(), RoomKitError>>;

    /// A writer of beats on a connection of its own.
    fn beater(&self) -> TurnFuture<'_, Result<Box<dyn Beater>, RoomKitError>>;

    /// The executors and the owners beating, with the service's time; what
    /// last beat more than `forget_after_ms` ago is forgotten.
    fn view(&self, forget_after_ms: i64) -> TurnFuture<'_, Result<View, RoomKitError>>;

    /// `node` is no room owner any more.
    fn forget_owner<'a>(&'a self, node: &'a str) -> TurnFuture<'a, Result<(), RoomKitError>>;

    /// The last beat of each of `members`, then, for each node of
    /// `running`, whether it runs each of the turns named, all read at one
    /// instant.
    fn beats<'a>(
        &'a self,
        members: &'a [&'a str],
        running: &'a [(&'a str, Vec<&'a str>)],
    ) -> TurnFuture<'a, Result<Vec<Vec<Option<i64>>>, RoomKitError>>;

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
    /// whether it is a change, for the one caller that then publishes it;
    /// `None` forgets it.
    fn claim_presence<'a>(
        &'a self,
        channel_id: &'a str,
        room_id: &'a str,
        online: Option<bool>,
    ) -> TurnFuture<'a, Result<bool, RoomKitError>>;
}
