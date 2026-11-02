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

use roomkit::model::{
    ChannelId, EventContent, EventDirection, EventType, RoomEvent, RoomId, ToolCallContent,
    ToolCallOutcome,
};
use roomkit_core::rules::edits::is_deleted;
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

    /// Drops `room`'s record: its next turn reads its stored rows again
    /// (RFC §10.6.3). Python's `SkillActivationMemory.forget()`.
    pub(crate) fn forget(&mut self, room: &RoomId) {
        self.take(room);
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
/// no error, in a row the agent wrote (outbound), a row deleted since left
/// out. A call a hook withheld, one
/// refused or one that failed never put the rules before the model.
pub(crate) fn activated_in(events: &[RoomEvent], agent: &ChannelId) -> Vec<String> {
    events
        .iter()
        .filter(|event| {
            event.r#type == EventType::ToolCallEnd
                && &event.source.channel_id == agent
                // The agent writes its rows outbound: one that entered the
                // room inbound under its id is no call it made.
                && event.source.direction == EventDirection::Outbound
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

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use roomkit::model::{ChannelType, EventDirection, EventId, EventSource, Metadata, Timestamp};
    use serde_json::json;

    use super::*;

    /// A served activation's end row from the agent `assistant`, in the
    /// `direction` its source names.
    fn end_row(direction: EventDirection) -> RoomEvent {
        let mut row = ToolCallContent::new(ACTIVATE_SKILL, "call_1");
        let mut arguments = Metadata::new();
        arguments.insert("name".to_owned(), json!("doc-helper"));
        row.arguments = arguments;
        row.result = json!("{\"name\": \"doc-helper\"}");
        row.outcome = Some(ToolCallOutcome::Served);
        RoomEvent {
            id: EventId::new("evt_1").unwrap(),
            r#type: EventType::ToolCallEnd,
            created_at: Timestamp::UNIX_EPOCH,
            ..RoomEvent::new(
                RoomId::new("room_1").unwrap(),
                EventSource {
                    direction,
                    ..EventSource::new(ChannelId::new("assistant").unwrap(), ChannelType::AI)
                },
                EventContent::ToolCall(row),
            )
        }
    }

    /// RFC §10.6.3: a room forgotten is read again from its stored rows,
    /// the other rooms kept.
    // Python: channels/_skill_activation.py SkillActivationMemory.forget, as
    // AIChannel._forget_room calls it.
    #[test]
    fn a_forgotten_room_is_read_again_from_its_rows() {
        let (room, other) = (RoomId::new("r1").unwrap(), RoomId::new("r2").unwrap());
        let mut activations = Activations::default();
        activations.hydrate(&room, &["doc-helper".to_owned()]);
        activations.hydrate(&other, &["doc-helper".to_owned()]);
        activations.forget(&room);
        assert_eq!(
            (
                activations.needs_hydration(&room),
                activations.active(&room),
                activations.active(&other)
            ),
            (true, Vec::<String>::new(), vec!["doc-helper".to_owned()])
        );
    }

    /// RFC §24.4: only the agent's own rows count, which it writes
    /// outbound; a row that entered the room inbound under its id is no
    /// activation it made.
    #[test]
    fn only_a_row_the_agent_wrote_outbound_counts_as_its_activation() {
        let agent = ChannelId::new("assistant").unwrap();
        assert_eq!(
            activated_in(&[end_row(EventDirection::Outbound)], &agent),
            ["doc-helper"]
        );
        assert_eq!(
            activated_in(&[end_row(EventDirection::Inbound)], &agent),
            Vec::<String>::new()
        );
    }
}
