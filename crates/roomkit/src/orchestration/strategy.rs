//! A strategy installed on a room (RFC §19.7): Python's `Orchestration`,
//! the agents it attaches and what its install sets up.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use roomkit_core::model::{ChannelId, RoomId};

use crate::channel::{Channel, DynChannel, config_of};
use crate::error::RoomKitError;
use crate::kit::RoomKit;
use crate::store::InMemoryStore;

/// What a strategy's [`install`](Orchestration::install) or
/// [`uninstall`](Orchestration::uninstall) returns: a boxed future, as the
/// kit holds strategies of any type.
pub type StrategyFuture<'call> =
    Pin<Box<dyn Future<Output = Result<(), RoomKitError>> + Send + 'call>>;

/// An orchestration strategy (RFC §19.7): Python's abstract
/// `Orchestration`, whose `agents()` and `install(kit, room_id)` a strategy
/// implements and whose `uninstall(kit, room_id)` does nothing by default.
///
/// A room holds one strategy at a time. [`RoomKit::install_strategy`],
/// `RoomKitOptions::orchestration` and `CreateRoomOptions::orchestration`
/// install one: its agents are registered and attached to the room as
/// intelligence channels, then its install runs, and the kit records what
/// the install added to the room (its room hooks, the agents it attached,
/// the room metadata it wrote), which [`RoomKit::uninstall_strategy`] takes
/// back after the strategy's own uninstall step.
///
/// ```
/// use std::sync::Arc;
///
/// use roomkit::model::RoomId;
/// use roomkit::orchestration::{StrategyAgent, StrategyFuture};
/// use roomkit::{Orchestration, RoomKit, UpdateRoomMetadataOptions};
///
/// struct Tagged;
///
/// impl Orchestration for Tagged {
///     fn agents(&self) -> Vec<StrategyAgent> {
///         Vec::new()
///     }
///
///     fn install<'call>(&'call self, kit: &'call RoomKit, room_id: &'call RoomId) -> StrategyFuture<'call> {
///         Box::pin(async move {
///             let mut metadata = roomkit::model::Metadata::new();
///             metadata.insert("tagged".to_owned(), true.into());
///             kit.update_room_metadata(room_id, metadata, UpdateRoomMetadataOptions::default())
///                 .await?;
///             Ok(())
///         })
///     }
/// }
///
/// # async fn install(kit: &RoomKit) -> Result<(), roomkit::RoomKitError> {
/// let room = RoomId::new("r1")?;
/// kit.install_strategy(&room, Arc::new(Tagged), Default::default()).await?;
/// # Ok(())
/// # }
/// ```
pub trait Orchestration<S = InMemoryStore>: Send + Sync + 'static {
    /// The agents to register and attach to the room: Python's
    /// `agents()`. An agent given live is registered unless the kit holds a
    /// channel under its id; one named by its id is a channel the kit holds,
    /// an intelligence channel.
    fn agents(&self) -> Vec<StrategyAgent>;

    /// Sets up the strategy in `room_id` once its agents are attached:
    /// Python's `install(kit, room_id)`. Its refusal of the room is a
    /// [`RoomKitError::Value`], Python's `ValueError`; the kit then takes
    /// back what the install added.
    fn install<'call>(
        &'call self,
        kit: &'call RoomKit<S>,
        room_id: &'call RoomId,
    ) -> StrategyFuture<'call>;

    /// Undoes what the install set up that the kit does not record:
    /// Python's `uninstall(kit, room_id)`, which
    /// [`RoomKit::uninstall_strategy`] runs first. Nothing by default.
    fn uninstall<'call>(
        &'call self,
        kit: &'call RoomKit<S>,
        room_id: &'call RoomId,
    ) -> StrategyFuture<'call> {
        let _ = (kit, room_id);
        Box::pin(async { Ok(()) })
    }
}

/// One of a strategy's agents: Python's `AIChannel | ACPChannel | str`,
/// the agent given live or named by the channel id of one the kit holds
/// (RFC §19.7).
#[derive(Clone)]
pub struct StrategyAgent {
    channel_id: ChannelId,
    live: Option<LiveAgent>,
}

/// An agent given live, ready for the kit to register.
#[derive(Clone)]
pub(crate) struct LiveAgent {
    pub(crate) channel: Arc<dyn DynChannel>,
    pub(crate) config: roomkit_core::ops::ChannelConfig,
    pub(crate) human_input: Option<crate::human_input::HumanInputHandler>,
}

impl StrategyAgent {
    /// The agent `channel`, given live: registered on the kit at the
    /// install unless the kit holds a channel under its id. A strategy that
    /// keeps its agent gives a clone, which shares the agent's state.
    pub fn channel(channel: impl Channel) -> Self {
        let config = config_of(&channel);
        let human_input = channel.human_input_handler();
        Self {
            channel_id: config.channel_id.clone(),
            live: Some(LiveAgent {
                channel: Arc::new(channel),
                config,
                human_input,
            }),
        }
    }

    /// The agent the kit holds under `channel_id`, live or registered
    /// before the install.
    pub fn id(channel_id: ChannelId) -> Self {
        Self {
            channel_id,
            live: None,
        }
    }

    /// The agent's channel id.
    pub fn channel_id(&self) -> &ChannelId {
        &self.channel_id
    }

    /// The agent given live, if it was.
    pub(crate) fn live(&self) -> Option<&LiveAgent> {
        self.live.as_ref()
    }
}

impl From<ChannelId> for StrategyAgent {
    fn from(channel_id: ChannelId) -> Self {
        Self::id(channel_id)
    }
}

impl fmt::Debug for StrategyAgent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StrategyAgent")
            .field("channel_id", &self.channel_id)
            .field("live", &self.live.is_some())
            .finish()
    }
}
