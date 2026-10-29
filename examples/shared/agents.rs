//! An agent that stands in for a model: it answers from a script and keeps
//! what it read.

use std::future::{Future, ready};

use roomkit::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
    ChannelOutput, ChannelType, EventDirection, EventSource, RoomContext, RoomEvent, TextContent,
};
use roomkit::{Channel, ChannelDeclaration, ChannelError, text_of, turn_context};

use crate::Log;

/// What an agent answers, by the text it reads.
pub type Script = &'static [(&'static str, &'static str)];

/// An intelligence channel standing in for a model: at each turn it answers
/// the text it reads with the script's line for it, and stays silent on any
/// other text. It keeps what each turn read: the history it is handed, none
/// of it for a standalone instruction, which the turn reads nothing of the
/// room for (RFC §10.1.1 rule 7, `turn_context`). Cloning it shares what it
/// kept.
#[derive(Clone, Debug)]
pub struct ScriptedAgent {
    declaration: ChannelDeclaration,
    script: Script,
    turns: Log<Turn>,
}

/// What one turn of a [`ScriptedAgent`] read.
#[derive(Clone, Debug)]
pub struct Turn {
    /// The event the turn was asked about.
    pub event: RoomEvent,
    /// The texts of the history the room handed the turn, oldest first.
    pub history: Vec<String>,
}

impl ScriptedAgent {
    /// An agent named `channel_id`, answering from `script`.
    pub fn new(channel_id: ChannelId, script: Script) -> Self {
        let mut declaration = ChannelDeclaration::new(
            channel_id,
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        );
        declaration.provider_name = Some("ScriptedAgent".to_owned());
        Self {
            declaration,
            script,
            turns: Log::new(),
        }
    }

    /// The same agent, declaring `capabilities`: one that edits and deletes
    /// natively is handed an edit or a delete as it is (RFC §6.6).
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: ChannelCapabilities) -> Self {
        self.declaration.capabilities = capabilities;
        self
    }

    /// The channel's id.
    pub fn id(&self) -> &ChannelId {
        &self.declaration.channel_id
    }

    /// What its turns read so far, in order.
    pub fn turns(&self) -> Vec<Turn> {
        self.turns.entries()
    }

    /// The texts of the events its turns were asked about, in order.
    pub fn read(&self) -> Vec<String> {
        self.turns()
            .iter()
            .map(|turn| text_of(&turn.event.content))
            .collect()
    }

    /// The script's answer to `event`, as the agent's response.
    fn answer(&self, event: &RoomEvent) -> Option<RoomEvent> {
        let read = text_of(&event.content);
        let (_, reply) = self.script.iter().find(|(line, _)| *line == read)?;
        let source = EventSource {
            direction: EventDirection::Outbound,
            ..EventSource::new(self.declaration.channel_id.clone(), ChannelType::AI)
        };
        // The pipeline gives the response its own id, room and depth, and
        // makes it answer the event that asked.
        Some(RoomEvent::new(
            event.room_id.clone(),
            source,
            TextContent::new(*reply),
        ))
    }
}

impl Channel for ScriptedAgent {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let output = match self.answer(&event) {
            Some(answer) => ChannelOutput {
                responded: true,
                response_events: vec![answer],
                ..ChannelOutput::empty()
            },
            None => ChannelOutput::default(),
        };
        let history = turn_context(&event, context)
            .recent_events
            .iter()
            .map(|event| text_of(&event.content))
            .collect();
        self.turns.push(Turn { event, history });
        ready(Ok(output))
    }
}
