//! Which skills are active in each room an agent serves (RFC §24.4), in
//! memory, as the reference implementation keeps them.
//!
//! An activation counts for the conversation, not the turn: once its call
//! is served, the skill's body rides the system prompt of the room's next
//! turns, and a repeated activation answers that the skill is active. The
//! record is the body's reason to be in the prompt, and the prompt's
//! reason to acknowledge: losing one loses the other. A room keeps at most
//! four active skills, the one used least recently retired as if never
//! activated, and the record keeps at most a hundred rooms. A room the
//! agent serves for the first time in this process is read again from the
//! calls its stored rows show served (a restart, an agent replaced), so
//! that the body comes back in the prompt.

use roomkit::is_deleted;
use roomkit::model::{
    ChannelId, EventContent, EventType, RoomEvent, RoomId, ToolCallContent, ToolCallOutcome,
};
use serde_json::Value;

use super::texts::ACTIVATE_SKILL;

/// The most skills active at once in a room.
const MAX_ACTIVE: usize = 4;

/// The most rooms the record keeps, the least recently active dropped.
const MAX_ROOMS: usize = 100;

/// A room's active skills, least recently used first.
#[derive(Debug)]
struct Room {
    id: RoomId,
    names: Vec<String>,
    /// Whether its stored rows were read already: once per room.
    hydrated: bool,
}

impl Room {
    fn new(id: &RoomId) -> Self {
        Self {
            id: id.clone(),
            names: Vec::new(),
            hydrated: false,
        }
    }
}

/// The active skills of each room, the least recently active room first.
#[derive(Debug, Default)]
pub(crate) struct Activations {
    rooms: Vec<Room>,
}

impl Activations {
    /// Records `name` as active in `room`, the most recently used.
    pub(crate) fn activate(&mut self, room: &RoomId, name: &str) {
        let mut held = self.take(room).unwrap_or_else(|| Room::new(room));
        held.names.retain(|active| active != name);
        held.names.push(name.to_owned());
        if held.names.len() > MAX_ACTIVE {
            held.names.remove(0);
        }
        self.keep(held);
    }

    /// Whether `name` is active in `room`.
    pub(crate) fn is_active(&self, room: &RoomId, name: &str) -> bool {
        self.find(room)
            .is_some_and(|held| held.names.iter().any(|active| active == name))
    }

    /// The skills active in `room`, least recently used first.
    pub(crate) fn active(&self, room: &RoomId) -> Vec<String> {
        self.find(room)
            .map(|held| held.names.clone())
            .unwrap_or_default()
    }

    /// Whether `room`'s stored rows should be read: it has no activation
    /// and was never read in this process.
    pub(crate) fn needs_hydration(&self, room: &RoomId) -> bool {
        self.find(room)
            .is_none_or(|held| !held.hydrated && held.names.is_empty())
    }

    /// Records, oldest first, the skills `names` as active in `room`, and
    /// that its stored rows were read.
    pub(crate) fn hydrate(&mut self, room: &RoomId, names: &[String]) {
        for name in names {
            self.activate(room, name);
        }
        let mut held = self.take(room).unwrap_or_else(|| Room::new(room));
        held.hydrated = true;
        self.keep(held);
    }

    fn find(&self, room: &RoomId) -> Option<&Room> {
        self.rooms.iter().find(|held| &held.id == room)
    }

    /// Keeps `held` as the most recently active room, the least recently
    /// active dropped past the bound.
    fn keep(&mut self, held: Room) {
        self.rooms.push(held);
        if self.rooms.len() > MAX_ROOMS {
            self.rooms.remove(0);
        }
    }

    fn take(&mut self, room: &RoomId) -> Option<Room> {
        let at = self.rooms.iter().position(|held| &held.id == room)?;
        Some(self.rooms.remove(at))
    }
}

/// The skills `agent`'s stored rows among `events` show activated, oldest
/// first: each `activate_skill` call served, whose result is an answer and
/// no error, a row deleted since left out. A call a hook withheld, one
/// refused or one that failed never put the rules before the model.
pub(crate) fn activated_in(events: &[RoomEvent], agent: &ChannelId) -> Vec<String> {
    events
        .iter()
        .filter(|event| {
            event.event_type == EventType::ToolCallEnd
                && &event.source.channel_id == agent
                && !is_deleted(event)
        })
        .filter_map(|event| {
            let EventContent::ToolCall(row) = &event.content else {
                return None;
            };
            activation_of(row)
        })
        .collect()
}

/// The skill a stored end row activated, when it reads as a served
/// activation (an end stored without an outcome read by its result).
fn activation_of(row: &ToolCallContent) -> Option<String> {
    if row.tool_name != ACTIVATE_SKILL
        || row
            .outcome
            .is_some_and(|outcome| outcome != ToolCallOutcome::Served)
    {
        return None;
    }
    let answered = row
        .result
        .as_str()
        .and_then(|result| serde_json::from_str::<Value>(result).ok())
        .is_some_and(|answer| {
            answer
                .as_object()
                .is_some_and(|answer| !answer.contains_key("error"))
        });
    let name = row.arguments.get("name").and_then(Value::as_str)?;
    (answered && !name.is_empty()).then(|| name.to_owned())
}
