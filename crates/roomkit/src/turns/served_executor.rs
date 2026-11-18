//! The executor interface used by a turn backend, independent of its conversation store.
use std::collections::BTreeSet;
use std::sync::Arc;

use roomkit_core::model::ChannelId;

use super::{ChannelControl, ChannelResult, ExecutorLink, TakesOptions, TurnFuture, TurnTimeouts};
use crate::{ConversationStore, RoomKitError, TurnExecutor};

mod native;
mod owner;
pub(crate) use owner::owner_executor;

mod private {
    pub(crate) trait Sealed {
        fn apply_room_hooks(&self, room: &roomkit_core::model::RoomId, names: Vec<String>);
        fn transport(&self, key: super::native::Key) -> &dyn super::native::Native;
    }
}

impl<S: ConversationStore> private::Sealed for TurnExecutor<S> {
    fn apply_room_hooks(&self, room: &roomkit_core::model::RoomId, names: Vec<String>) {
        self.hooks().use_room_hooks(room, names);
    }
    fn transport(&self, _: native::Key) -> &dyn native::Native {
        self
    }
}

/// The executor a backend serves (RFC §10.6). A backend can keep executors
/// backed by different conversation stores in the same registry. Applications
/// obtain this view from a [`TurnExecutor`].
///
/// ```
/// use std::sync::Arc;
/// use roomkit::{ConversationStore, RoomKit};
/// use roomkit::turns::ServedExecutor;
///
/// fn served<S: ConversationStore>(kit: &RoomKit<S>) -> Arc<dyn ServedExecutor> {
///     Arc::new(kit.turn_executor())
/// }
/// ```
#[expect(
    private_bounds,
    reason = "backends receive the executor view from a RoomKit"
)]
pub trait ServedExecutor: private::Sealed + Send + Sync + 'static {
    /// Identifier sent in the turn's acceptance frame.
    fn executor_id(&self) -> &str;
    /// Whether this executor has stopped accepting turns.
    fn closed(&self) -> bool;
    /// Refuse new turns and finish closing this executor's work.
    fn close(&self) -> TurnFuture<'_, ()>;
    /// Whether this open executor declares the channel.
    fn declares(&self, channel_id: &ChannelId) -> bool;
    /// Whether the executor hosts this channel, including after it closes.
    fn hosts_channel(&self, channel_id: &ChannelId) -> bool;
    /// The exact IDs and prefixes advertised to other processes, when finite.
    fn hosted(&self) -> Option<BTreeSet<String>>;
    /// The served organizations. `None` serves rooms without an organization.
    fn organizations(&self) -> Option<BTreeSet<String>>;
    /// Whether a room's organization is served by this executor.
    fn serves(&self, organization_id: Option<&str>) -> bool;
    /// Names of the room hooks currently defined on this executor.
    fn room_hooks(&self) -> BTreeSet<String>;
    /// Whether this executor can take work under all the room's requirements.
    fn takes(&self, channel_id: &ChannelId, options: TakesOptions) -> bool;
    /// Time bounds used by the turns this executor runs.
    fn timeouts(&self) -> TurnTimeouts;
    /// Give new turns the port they use to reach their owner after their link ends.
    fn reach_owner(&self, owner: Arc<dyn super::RoomOwner>);
    /// Admit the request on the backend's executor link.
    ///
    /// # Errors
    /// A closed executor or a duplicate admission is refused.
    fn take(&self, link: Arc<dyn ExecutorLink>) -> Result<(), RoomKitError>;
    /// Apply a channel call, committed event or room lifecycle notification.
    fn control(
        &self,
        message: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>>;
}

impl<S: ConversationStore> ServedExecutor for TurnExecutor<S> {
    fn executor_id(&self) -> &str {
        self.executor_id()
    }
    fn closed(&self) -> bool {
        self.closed()
    }
    fn close(&self) -> TurnFuture<'_, ()> {
        Box::pin(self.close())
    }
    fn declares(&self, channel_id: &ChannelId) -> bool {
        self.declares(channel_id)
    }
    fn hosts_channel(&self, channel_id: &ChannelId) -> bool {
        self.hosts_channel(channel_id)
    }
    fn hosted(&self) -> Option<BTreeSet<String>> {
        self.hosted()
    }
    fn organizations(&self) -> Option<BTreeSet<String>> {
        self.organizations()
    }
    fn serves(&self, organization_id: Option<&str>) -> bool {
        self.serves(organization_id)
    }
    fn room_hooks(&self) -> BTreeSet<String> {
        self.hooks().defined_room_hooks()
    }
    fn takes(&self, channel_id: &ChannelId, options: TakesOptions) -> bool {
        self.takes(channel_id, options)
    }
    fn timeouts(&self) -> TurnTimeouts {
        self.timeouts()
    }
    fn reach_owner(&self, owner: Arc<dyn super::RoomOwner>) {
        self.reach_owner(owner);
    }
    fn take(&self, link: Arc<dyn ExecutorLink>) -> Result<(), RoomKitError> {
        self.take(link)
    }
    fn control(
        &self,
        message: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>> {
        Box::pin(self.control(message))
    }
}
