//! The turn runners a room's owner keeps (RFC §10.6.1, §19.7): Python's
//! `core/_turn_runners.py`.
//!
//! A strategy that takes an agent's turns in a room (a loop's produce and
//! review) is kept here, by the room's owner, per channel and room, never
//! on the shared channel: the runner runs on the owner, and each
//! generation it asks of the agent is a turn of its own.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::{
    ChannelBinding, ChannelId, ChannelOutput, EventContent, Metadata, RoomContext, RoomEvent,
    RoomId,
};

use crate::channel::{BoxFuture, ChannelError};
use crate::error::RoomKitError;
use crate::kit::RoomKit;

/// What a runner does with a turn of its channel: Python's runner either
/// asks the channel's own turn (`kit._strategy_turn`) or answers itself.
pub(crate) enum Taken {
    /// The channel's own turn, its event handed back.
    Own(Box<(RoomEvent, ChannelBinding, RoomContext)>),
    /// The runner's answer, once it is over, or what it failed with.
    Answered(BoxFuture<'static, Result<ChannelOutput, ChannelError>>),
    /// A first pass of the channel's own turn, read as a streamed turn
    /// whose calls are the room's rows and whose text is kept out of the
    /// room, then what the runner makes of it (Python's supervisor reading
    /// its first pass through `persist_tool_calls`, RFC §19.7.3).
    Passes(Passes),
    /// The runner's work before the channel's own turn, then that turn on
    /// the event it gives ([`Next::Own`]), or what the work failed with.
    Later(BoxFuture<'static, Result<Next, RoomKitError>>),
}

/// A first pass of a channel's own turn, and what follows it.
pub(crate) struct Passes {
    /// The first pass: the channel's own turn on this event and binding.
    pub(crate) first: Box<(RoomEvent, ChannelBinding, RoomContext)>,
    /// What follows it, handed what it gave.
    pub(crate) then: Then,
}

/// What follows a first pass, once it was read.
pub(crate) struct Then(
    pub(crate) Box<dyn FnOnce(FirstPass) -> BoxFuture<'static, Result<Next, RoomKitError>> + Send>,
);

impl std::fmt::Debug for Then {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Then")
    }
}

/// What a first pass gave: its answer, how its turn ended and its record
/// (Python's `_Pass1`).
#[derive(Debug, Default)]
pub(crate) struct FirstPass {
    /// Its final answer: the text it ended on when it completed, else
    /// nothing (RFC §6.4).
    pub(crate) answer: String,
    /// How its turn ended, when its record says: `completed`,
    /// `max_rounds`, `cancelled`...
    pub(crate) end: Option<String>,
    /// Its turn's record.
    pub(crate) record: Metadata,
}

impl FirstPass {
    /// A first pass that answered at once, with no stream: the text of its
    /// first response event that says something, and no record (Python's
    /// `_extract_output_text`).
    pub(crate) fn answered(output: &ChannelOutput) -> Self {
        let answer = output
            .response_events
            .iter()
            .filter_map(|event| {
                if let EventContent::Text(text) = &event.content {
                    Some(&text.body)
                } else {
                    None
                }
            })
            .find(|body| !body.is_empty())
            .cloned()
            .unwrap_or_default();
        Self {
            answer,
            ..Self::default()
        }
    }
}

/// The channel's own turn on `event`, standing in for `asked`: its
/// history the room's as it was when `asked` came.
#[derive(Debug)]
pub(crate) struct Answering {
    pub(crate) event: RoomEvent,
    pub(crate) binding: ChannelBinding,
    pub(crate) context: RoomContext,
    pub(crate) asked: RoomEvent,
}

/// What follows a first pass whose turn did not fail.
#[derive(Debug)]
pub(crate) enum Next {
    /// The channel's own turn on an event standing in for the one asked:
    /// its answer is the room's.
    Own(Box<Answering>),
    /// The first pass's own output answers, carrying its record.
    FirstStands,
    /// The channel's fallback text answers, carrying this record (Python's
    /// `_pass1_answer`).
    Fallback(String, Metadata),
}

/// What takes a channel's turns in a room in place of the channel's own
/// answer: Python's `TurnRunner`.
pub(crate) trait TurnRunner<S>: Send + Sync {
    /// Takes the turn of `binding`'s channel on `event`, or hands it back
    /// to the channel's own turn.
    fn take(
        &self,
        kit: RoomKit<S>,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Taken;
}

/// A runner, and the install that took the turns: the address of the
/// strategy object, Python's `owner`.
struct Held<S> {
    runner: Arc<dyn TurnRunner<S>>,
    owner: usize,
}

/// The runners of the strategies installed in this owner's rooms.
pub(crate) struct TurnRunners<S> {
    held: Mutex<HashMap<(RoomId, ChannelId), Held<S>>>,
}

impl<S> Default for TurnRunners<S> {
    fn default() -> Self {
        Self {
            held: Mutex::default(),
        }
    }
}

impl<S> std::fmt::Debug for TurnRunners<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnRunners")
            .field("held", &self.held().len())
            .finish()
    }
}

impl<S> TurnRunners<S> {
    fn held(&self) -> MutexGuard<'_, HashMap<(RoomId, ChannelId), Held<S>>> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes `channel_id`'s turns in `room_id` with `runner`, for the
    /// install of the strategy at `owner`: one runner per channel and
    /// room, which only the install that set it replaces.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Value`] when another strategy's install took them.
    pub(crate) fn take(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        runner: Arc<dyn TurnRunner<S>>,
        owner: usize,
    ) -> Result<(), RoomKitError> {
        let mut held = self.held();
        let key = (room_id.clone(), channel_id.clone());
        if held.get(&key).is_some_and(|held| held.owner != owner) {
            return Err(RoomKitError::Value(format!(
                "Room '{room_id}' already has the turns of '{channel_id}' taken by another \
                 strategy"
            )));
        }
        held.insert(key, Held { runner, owner });
        Ok(())
    }

    /// The runner taking `channel_id`'s turns in `room_id`, if one does.
    pub(crate) fn get(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Option<Arc<dyn TurnRunner<S>>> {
        let key = (room_id.clone(), channel_id.clone());
        self.held().get(&key).map(|held| Arc::clone(&held.runner))
    }

    /// Stops taking `channel_id`'s turns in `room_id` (an uninstall, RFC
    /// §19.7).
    pub(crate) fn release(&self, room_id: &RoomId, channel_id: &ChannelId) {
        self.held().remove(&(room_id.clone(), channel_id.clone()));
    }

    /// The channels whose turns a runner takes in `room_id`.
    pub(crate) fn channels(&self, room_id: &RoomId) -> BTreeSet<ChannelId> {
        self.held()
            .keys()
            .filter(|(held_room, _)| held_room == room_id)
            .map(|(_, channel_id)| channel_id.clone())
            .collect()
    }
}

#[cfg(test)]
#[path = "turn_runners_tests.rs"]
mod contract_tests;

#[cfg(test)]
mod tests {
    use roomkit_core::model::{
        ChannelId, ChannelOutput, ChannelType, EventSource, MediaContent, RoomEvent, RoomId,
        TextContent,
    };

    use super::FirstPass;

    fn answered(events: Vec<RoomEvent>) -> String {
        let output = ChannelOutput {
            responded: true,
            response_events: events,
            ..ChannelOutput::default()
        };
        FirstPass::answered(&output).answer
    }

    fn event(content: impl Into<roomkit_core::model::EventContent>) -> RoomEvent {
        let room = RoomId::new("r1").unwrap_or_else(|error| panic!("{error}"));
        let channel = ChannelId::new("ch").unwrap_or_else(|error| panic!("{error}"));
        RoomEvent::new(room, EventSource::new(channel, ChannelType::AI), content)
    }

    // Python: tests/test_supervisor.py::TestExtractOutputText::test_from_response_events
    // Python: tests/test_supervisor.py::TestExtractOutputText::test_empty_output
    // Python: tests/test_supervisor.py::TestExtractOutputText::test_non_text_events_skipped
    #[test]
    fn a_pass_answered_at_once_hands_on_its_first_text() {
        assert_eq!(
            answered(vec![event(TextContent::new("Response text"))]),
            "Response text"
        );
        assert_eq!(answered(Vec::new()), "");
        let image = MediaContent::new("https://example.com/img.png", "image/png")
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(answered(vec![event(image)]), "");
    }
}
