//! An agent's greeting, its text path (RFC §19.1): what an agent says first
//! in a room, on demand or when a session starts there. The voice and
//! realtime paths come with the voice channels.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};

use roomkit_core::model::{ChannelCategory, ChannelId, HookResult, RoomContext, RoomId};
use roomkit_core::ops::{ChannelConfig, Entry, InboundResult, KIT_CLOSED};
use roomkit_core::rules::hook_registry::{HookExecution, HookTrigger};

use super::RoomKit;
use crate::channel::{AgentIdentity, DynChannel};
use crate::error::RoomKitError;
use crate::hooks::{HookError, HookOptions, SessionStartedEvent};
use crate::store::ConversationStore;

/// What [`RoomKit::send_greeting`] takes beside the room, Python's
/// `send_greeting` keywords that its text path reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SendGreetingOptions {
    /// The channel to greet as, when it is an agent: as `agent_id`, which
    /// goes first.
    pub channel_id: Option<ChannelId>,
    /// The agent to greet as, when it is one; the first agent the room binds
    /// otherwise.
    pub agent_id: Option<ChannelId>,
    /// What it says: the agent's own greeting when unset or empty.
    pub greeting: Option<String>,
    /// The organization the caller acts for (RFC §17.2): the room is read
    /// with it first. Unset, no room is read for the scope.
    pub organization_id: Option<String>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// Says an agent's greeting in the room `room_id` (RFC §19.1): stored as
    /// the agent's message, marked `auto_greeting` in its metadata, and
    /// delivered on the room's lane, the answers of the room's other agents
    /// re-entering. Python's `send_greeting`, without a session: its voice
    /// and realtime paths come with the voice channels.
    ///
    /// The agent is the one `options` name by `agent_id` or `channel_id`, an
    /// intelligence channel with an identity, else the first one the room
    /// binds. Its words are the greeting `options` give, else its identity's.
    /// With no agent or no words, nothing is done, and nothing fails.
    ///
    /// The greeting is committed by a path of its own (RFC §7.5 rule 2): no
    /// `BEFORE_BROADCAST` hook sees it, and nothing checks its agent's right
    /// to write. An agent that cannot write in the room, muted or read-only,
    /// has it stored and delivered to no one, and one the room does not bind
    /// has it stored with no delivery set at all. A room missing, or whose
    /// status refuses events (RFC §5.1), takes nothing, and nothing fails.
    ///
    /// Made in the room's own work, a hook's body or a delivery, the greeting
    /// is at least as deep as that work, and what answers it draws on that
    /// work's reentry budget, so that a hook greeting on every answer stops
    /// at the limit (RFC §8.3); its own commit draws nothing.
    ///
    /// Returns once its delivery is over; called under the room's lock or
    /// from a delivery in the room, once it is committed, its delivery
    /// following in the room's lane (RFC §13.5, §10.1 step 18). Dropped before
    /// its commit, nothing of it is stored; dropped after, its delivery goes
    /// on.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::KitClosed`] once the kit's close started, or when it
    /// cut the call before its commit (RFC §12.10.4); [`RoomKitError::RoomNotFound`] for a room `organization_id`
    /// does not reach, missing or another's (RFC §17.2); or the store's or
    /// the room lock's failure.
    pub async fn send_greeting(
        &self,
        room_id: &RoomId,
        options: SendGreetingOptions,
    ) -> Result<(), RoomKitError> {
        self.inner.open()?;
        self.reached(room_id, options.organization_id.as_deref())
            .await?;
        let named = options.agent_id.as_ref().or(options.channel_id.as_ref());
        let Some((agent_id, identity)) = self.greeting_agent(room_id, named).await? else {
            return Ok(());
        };
        let text = options
            .greeting
            .filter(|text| !text.is_empty())
            .or(identity.greeting)
            .filter(|text| !text.is_empty());
        let Some(text) = text else {
            return Ok(());
        };
        let entry = Entry::Greeting {
            room_id: room_id.clone(),
            agent_id,
            text,
            chain_depth: 0,
        };
        match self.enter(entry, SEND_GREETING, false).await {
            Ok(entered) => cut_by_the_close(&entered.result),
            Err(RoomKitError::RoomNotFound(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// The agent that greets `room_id` and its identity: `named`'s when it is
    /// an agent, else the first agent the room binds.
    async fn greeting_agent(
        &self,
        room_id: &RoomId,
        named: Option<&ChannelId>,
    ) -> Result<Option<(ChannelId, AgentIdentity)>, RoomKitError> {
        if let Some(identity) = named.and_then(|channel_id| self.agent_identity(channel_id)) {
            return Ok(named.cloned().map(|channel_id| (channel_id, identity)));
        }
        let bindings = self.store().list_bindings(room_id).await?;
        Ok(bindings.into_iter().find_map(|binding| {
            let identity = self.agent_identity(&binding.channel_id)?;
            Some((binding.channel_id, identity))
        }))
    }

    /// The identity of `channel_id`, when it is an agent: a registered
    /// intelligence channel whose declaration says who it is.
    fn agent_identity(&self, channel_id: &ChannelId) -> Option<AgentIdentity> {
        let category = self.config().channel(channel_id)?.category;
        if category != ChannelCategory::Intelligence {
            return None;
        }
        self.inner.channel(channel_id)?.declaration().identity
    }

    /// Registers the kit-wide hook that greets as `agent_id` when a session
    /// starts (RFC §19.1).
    pub(super) fn register_auto_greet_hook(&self, agent_id: ChannelId) -> Result<(), RoomKitError> {
        let options = HookOptions {
            execution: HookExecution::Async,
            name: auto_greet_hook_name(&agent_id),
            ..HookOptions::default()
        };
        let kit = self.downgrade();
        let greeted = Arc::new(Mutex::new(GreetedRooms::default()));
        let hook = move |event: SessionStartedEvent, _context: RoomContext| {
            let (kit, agent_id, greeted) = (kit.upgrade(), agent_id.clone(), Arc::clone(&greeted));
            async move {
                let Some(kit) = kit else {
                    return Ok(None);
                };
                kit.auto_greet(&event.room_id, &agent_id, &greeted)
                    .await
                    .map(|()| None::<HookResult<SessionStartedEvent>>)
                    .map_err(HookError::from)
            }
        };
        self.hook(HookTrigger::OnSessionStarted, hook, options)
    }

    /// Removes an unregistered agent's hook: one registered again under its
    /// id greets by its own setup.
    pub(super) fn forget_auto_greet(&self, channel_id: &ChannelId) {
        self.hook_engine()
            .remove_global_hook(&auto_greet_hook_name(channel_id));
    }

    /// Greets `room_id` as `agent_id` on a session's start there: once per
    /// room, and only in a room that binds it, which may be attached later
    /// and greet then.
    async fn auto_greet(
        &self,
        room_id: &RoomId,
        agent_id: &ChannelId,
        greeted: &Mutex<GreetedRooms>,
    ) -> Result<(), RoomKitError> {
        let lock = || greeted.lock().unwrap_or_else(PoisonError::into_inner);
        if !lock().first_time(room_id) {
            return Ok(());
        }
        let bindings = self.store().list_bindings(room_id).await?;
        if !bindings.iter().any(|b| &b.channel_id == agent_id) {
            lock().forget(room_id);
            return Ok(());
        }
        let options = SendGreetingOptions {
            agent_id: Some(agent_id.clone()),
            ..SendGreetingOptions::default()
        };
        self.send_greeting(room_id, options).await
    }
}

/// What a greeting's call came to: the kit's close cut it before its commit,
/// nothing of it stored, as [`send_event`](RoomKit::send_event) reads it (RFC
/// §12.10.4); a greeting a room's status refused fails no one.
fn cut_by_the_close(result: &InboundResult) -> Result<(), RoomKitError> {
    if result.event.is_none() && result.cancellation_reason.as_deref() == Some(KIT_CLOSED) {
        return Err(RoomKitError::KitClosed);
    }
    Ok(())
}

/// The agent `channel`, registering as `config` declares it, that greets on a
/// session's start: an intelligence channel whose identity has a greeting and
/// `auto_greet`.
pub(super) fn auto_greeting_agent(
    config: &ChannelConfig,
    channel: &dyn DynChannel,
) -> Option<ChannelId> {
    if config.category != ChannelCategory::Intelligence {
        return None;
    }
    let identity = channel.declaration().identity?;
    let greets = identity.auto_greet && identity.greeting.is_some_and(|text| !text.is_empty());
    greets.then(|| config.channel_id.clone())
}

/// The name of an agent's auto-greet hook: a name starting with `_`, which a
/// session's start awaits before its message goes on (RFC §10.1 step 2).
fn auto_greet_hook_name(agent_id: &ChannelId) -> String {
    format!("_agent_auto_greet:{agent_id}")
}

/// How many rooms an agent remembers having greeted.
const GREETED_ROOMS_MAX: usize = 10_000;

/// The rooms an agent greeted on a session's start, bounded for a kit that
/// runs for long: past [`GREETED_ROOMS_MAX`], the oldest tenth goes, which
/// keeps most of them from being greeted twice, as clearing them all would.
#[derive(Debug, Default)]
struct GreetedRooms {
    rooms: HashSet<RoomId>,
    /// The same rooms, oldest first.
    order: VecDeque<RoomId>,
}

impl GreetedRooms {
    /// Whether `room_id` is greeted for the first time, then remembered.
    fn first_time(&mut self, room_id: &RoomId) -> bool {
        if self.rooms.contains(room_id) {
            return false;
        }
        if self.rooms.len() >= GREETED_ROOMS_MAX {
            for oldest in self.order.drain(..GREETED_ROOMS_MAX / 10) {
                self.rooms.remove(&oldest);
            }
        }
        self.rooms.insert(room_id.clone());
        self.order.push_back(room_id.clone());
        true
    }

    /// Forgets `room_id`, whose greeting did not happen.
    fn forget(&mut self, room_id: &RoomId) {
        if self.rooms.remove(room_id)
            && let Some(position) = self.order.iter().rposition(|room| room == room_id)
        {
            self.order.remove(position);
        }
    }
}

/// The kit's call that greets, as an error names it.
const SEND_GREETING: &str = "send_greeting";

#[cfg(test)]
mod tests {
    use super::*;

    fn room(number: usize) -> RoomId {
        RoomId::new(format!("room-{number}")).unwrap()
    }

    /// Python: `_register_auto_greet_hook`'s bound on `greeted_rooms`, an
    /// LRU evicting the oldest 10% on overflow rather than clearing it all.
    #[test]
    fn the_rooms_greeted_are_bounded_and_the_oldest_tenth_goes() {
        let mut greeted = GreetedRooms::default();
        for number in 0..GREETED_ROOMS_MAX {
            assert!(greeted.first_time(&room(number)));
        }
        assert!(!greeted.first_time(&room(0)), "still held at the bound");
        assert!(greeted.first_time(&room(GREETED_ROOMS_MAX)));
        assert_eq!(greeted.rooms.len(), GREETED_ROOMS_MAX * 9 / 10 + 1);
        assert_eq!(greeted.order.len(), greeted.rooms.len());
        assert!(!greeted.first_time(&room(GREETED_ROOMS_MAX / 10)));
        assert!(greeted.first_time(&room(GREETED_ROOMS_MAX / 10 - 1)));
    }

    /// Python: `_auto_greet_handler` drops its key when the agent is not
    /// bound to the room: a later session may greet.
    #[test]
    fn a_room_forgotten_may_be_greeted_again() {
        let mut greeted = GreetedRooms::default();
        assert!(greeted.first_time(&room(1)));
        assert!(greeted.first_time(&room(2)));
        greeted.forget(&room(1));
        assert_eq!(greeted.order, [room(2)]);
        assert!(greeted.first_time(&room(1)));
        assert!(!greeted.first_time(&room(2)));
    }
}
