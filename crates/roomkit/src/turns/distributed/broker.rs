//! What a distributed turn backend asks of the services that carry its
//! messages between processes and keep what they share (RFC §10.6.7,
//! §10.6.8): a broker for the messages, a registry for the rest. One
//! service may give both. The backend reaches them through these traits
//! alone.
use std::collections::BTreeMap;
use std::time::Duration;

use serde::Serialize;

use crate::RoomKitError;
use crate::turns::TurnFuture;

/// A message of a node's inbox. The backend writes and reads its fields;
/// a broker carries them as they are.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BrokerMessage {
    /// What the message is: a turn's request, a frame, a control, an answer.
    pub kind: String,
    /// The node that posted it.
    pub sender: String,
    /// Its content, serialized.
    pub body: String,
    /// What it is for on the node that reads it: a turn, an executor.
    pub target: String,
    /// The reference an answer names, empty when it expects none.
    pub reference: String,
}

impl BrokerMessage {
    /// A message of `kind` from `sender`, for nothing in particular on the
    /// node that reads it and expecting no answer: `target` and `reference`
    /// empty, set by struct update.
    pub fn new(kind: &str, sender: &str, body: String) -> Self {
        Self {
            kind: kind.into(),
            sender: sender.into(),
            body,
            ..Self::default()
        }
    }

    /// A message for `target` on the node that reads it, naming
    /// `reference`: the backend's own messages.
    pub(crate) fn addressed(
        kind: &str,
        sender: &str,
        body: String,
        target: &str,
        reference: &str,
    ) -> Self {
        Self {
            target: target.into(),
            reference: reference.into(),
            ..Self::new(kind, sender, body)
        }
    }
}

/// An entry read from an inbox.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboxEntry {
    /// The entry's id in its inbox: the cursor of a later read, and what
    /// an acknowledgement names.
    pub id: String,
    /// Its message; none for an entry the broker could not read as one.
    pub message: Option<BrokerMessage>,
}

/// A node's beat: what it declares, what it withdrew and the turns it
/// runs, written to the registry as one change.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Beat {
    /// The node that beats.
    pub node: String,
    /// The count of the node's changes: a beat older than the node's last
    /// one is ignored.
    pub generation: u64,
    /// Each executor the node declares, by its member name
    /// (`<node>:<executor id>`), with its declaration, serialized.
    pub declared: BTreeMap<String, String>,
    /// The executors that still beat and declare nothing.
    pub undeclared: Vec<String>,
    /// The executors that are gone: forgotten.
    pub gone: Vec<String>,
    /// The turns the node runs.
    pub running: Vec<String>,
    /// Whether the node is a room owner.
    pub owner: bool,
}

impl Beat {
    /// A beat of `node` at its change `generation` that declares, withdraws
    /// and runs nothing, and is no room owner's: the rest set by struct
    /// update.
    pub fn new(node: &str, generation: u64) -> Self {
        Self {
            node: node.into(),
            generation,
            ..Self::default()
        }
    }
}

/// The executors and the room owners beating, as the registry keeps them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistryView {
    /// The registry's time, in milliseconds since the Unix epoch: the clock
    /// of every beat, which an executor's last beat reads.
    pub now: i64,
    /// Each executor's last beat, and its declaration when it has one.
    pub executors: BTreeMap<String, (i64, Option<String>)>,
    /// Each room owner's last beat, by its node.
    pub owners: BTreeMap<String, i64>,
}

/// What [`TurnRegistry::beats`] reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Beats {
    /// The last beat of each executor asked, in the order asked; none for
    /// one the registry does not know.
    pub last: Vec<Option<i64>>,
    /// For each node asked, whether it runs each of the turns named.
    pub running: Vec<Vec<bool>>,
}

/// A reader of one node's inbox, on a connection of its own.
pub trait TurnInbox: Send {
    /// The entries after the one `last` names, in the order posted, waiting
    /// for the first no longer than the reader was told; from the inbox's
    /// first entry for an empty `last`.
    fn read<'a>(
        &'a mut self,
        last: &'a str,
    ) -> TurnFuture<'a, Result<Vec<InboxEntry>, RoomKitError>>;
}

/// A writer of beats on a connection of its own: the heartbeat's, which
/// runs apart from the turns.
pub trait BeatWriter: Send {
    /// As [`TurnRegistry::beat`].
    fn beat<'a>(&'a mut self, beat: &'a Beat) -> TurnFuture<'a, Result<(), RoomKitError>>;
}

/// The messages between the processes of a distributed turn backend: an
/// inbox a node, read in the order posted.
pub trait TurnBroker: Send + Sync + 'static {
    /// Add `messages` to `node`'s inbox, in their order, after those it
    /// holds.
    fn post<'a>(
        &'a self,
        node: &'a str,
        messages: &'a [BrokerMessage],
    ) -> TurnFuture<'a, Result<(), RoomKitError>>;

    /// A reader of `node`'s inbox, each read waiting `block` at most.
    fn inbox<'a>(
        &'a self,
        node: &'a str,
        block: Duration,
    ) -> TurnFuture<'a, Result<Box<dyn TurnInbox>, RoomKitError>>;

    /// Let go of entries read from `node`'s inbox: `ids` in the order
    /// read, each entry before the last of them read too.
    fn acknowledge<'a>(
        &'a self,
        node: &'a str,
        ids: &'a [String],
    ) -> TurnFuture<'a, Result<(), RoomKitError>>;
}

/// What the processes of a distributed turn backend share: who beats, who
/// holds a room, the presence last published.
pub trait TurnRegistry: Send + Sync + 'static {
    /// Reach the registry, a first time only.
    fn connect(&self) -> TurnFuture<'_, Result<(), RoomKitError>>;

    /// Whether the registry was reached: a node that never reached it has
    /// nothing to withdraw there.
    fn connected(&self) -> bool;

    /// Write a node's beat at the registry's time and as one change. A
    /// beat older than the node's last one is ignored.
    fn beat<'a>(&'a self, beat: &'a Beat) -> TurnFuture<'a, Result<(), RoomKitError>>;

    /// A writer of beats on a connection of its own.
    fn beater(&self) -> TurnFuture<'_, Result<Box<dyn BeatWriter>, RoomKitError>>;

    /// The executors and the owners beating; what last beat more than
    /// `forget_after_ms` ago is forgotten.
    fn view(&self, forget_after_ms: i64) -> TurnFuture<'_, Result<RegistryView, RoomKitError>>;

    /// `node` is no room owner any more.
    fn forget_owner<'a>(&'a self, node: &'a str) -> TurnFuture<'a, Result<(), RoomKitError>>;

    /// The last beat of each of `members`, and for each node of `running`
    /// whether it runs each of the turns named; either may name none. What
    /// is read of one node is read as one of its beats left it.
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
