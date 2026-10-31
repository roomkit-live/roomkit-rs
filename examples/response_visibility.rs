//! Who sees what (RFC §5.12, §7.3, §7.5 rule 8), in a hybrid voice and text
//! interface: the user speaks on a call and types on a screen, an assistant
//! answers, and a supervisor watches. Each event carries a visibility of
//! its own, and its source's binding has one: a channel sees the event only
//! when both include it, and the framework stores that intersection on the
//! event when it commits it. A message also says where its answers may go,
//! its `response_visibility`: each answer keeps the visibility its channel
//! gave it within that one, and is never shown wider. The answer carries
//! that scope as its own `response_visibility`, stored with it (RFC §5.2,
//! §8.5), so what answers the answer stays within it too.
//!
//! - A question typed on the screen carries `intelligence`: the assistant
//!   reads it, the call never speaks it. It asks for its answers on the
//!   screen: the assistant's answer, which it writes for everyone, is
//!   stored visible to the screen alone, shown and never spoken, and
//!   carries the screen as where its own answers may go.
//! - A spoken question restricts neither who sees it nor where its answers
//!   go: its answer reaches the call and the screen.
//! - The supervisor's binding is visible to `transport`: what they write
//!   reaches the call and the screen, and no agent reads it. A note they
//!   address to the screen alone is stored visible to it only.
//!
//! ```sh
//! cargo run --example response_visibility
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::future::{Future, ready};
use std::process::ExitCode;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelSet,
    ChannelType, EventDirection, EventSource, InboundMessage, RoomContext, RoomEvent, RoomId,
    TextContent, Visibility,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, CreateRoomOptions,
    ProcessInboundOptions, RoomKit, text_of,
};

use shared::{Checks, recording};

/// An assistant that answers each question, for everyone: where an answer
/// goes is the question's to say.
struct Assistant {
    declaration: ChannelDeclaration,
}

impl Assistant {
    fn new(channel_id: ChannelId) -> Self {
        let declaration = ChannelDeclaration::new(
            channel_id,
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        );
        Self { declaration }
    }

    fn answer(&self, question: &RoomEvent) -> RoomEvent {
        let source = EventSource {
            direction: EventDirection::Outbound,
            ..EventSource::new(self.declaration.channel_id.clone(), ChannelType::AI)
        };
        let text = format!("Answer to {:?}", text_of(&question.content));
        // The pipeline gives the answer its own id, room and depth, and
        // narrows it to where the question's answers may go.
        RoomEvent {
            id: question.id.clone(),
            created_at: question.created_at,
            ..RoomEvent::new(question.room_id.clone(), source, TextContent::new(text))
        }
    }
}

impl Channel for Assistant {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let output = if text_of(&event.content).ends_with('?') {
            ChannelOutput {
                responded: true,
                response_events: vec![self.answer(&event)],
                ..ChannelOutput::empty()
            }
        } else {
            ChannelOutput::default()
        };
        ready(Ok(output))
    }
}

/// `from`'s `text`, the event visible to `visibility`.
fn message(from: &RecordingTransport, text: &str, visibility: Visibility) -> InboundMessage {
    InboundMessage {
        visibility,
        ..InboundMessage::new(from.channel_id().clone(), "user", TextContent::new(text))
    }
}

/// Runs `message` into `room`.
async fn writes(
    kit: &RoomKit,
    room: &RoomId,
    message: InboundMessage,
) -> Result<(), Box<dyn Error>> {
    let answers = message
        .response_visibility
        .as_ref()
        .map_or_else(|| "anywhere".to_owned(), ToString::to_string);
    println!(
        "{} writes {:?}, visible to {}, its answers to {answers}.",
        message.channel_id,
        text_of(&message.content),
        message.visibility
    );
    kit.process_inbound(
        message,
        ProcessInboundOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        },
    )
    .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = shared::kit();
    let call = recording("call", ChannelType::VOICE)?;
    let screen = recording("screen", ChannelType::WEBSOCKET)?;
    let supervisor = recording("supervisor", ChannelType::WEBSOCKET)?;
    let assistant = ChannelId::new("assistant")?;
    kit.register_channel(call.clone())?;
    kit.register_channel(screen.clone())?;
    kit.register_channel(supervisor.clone())?;
    kit.register_channel(Assistant::new(assistant.clone()))?;

    let room = RoomId::new("hybrid")?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    kit.attach_channel(&room, call.channel_id(), AttachChannelOptions::default())
        .await?;
    kit.attach_channel(&room, screen.channel_id(), AttachChannelOptions::default())
        .await?;
    kit.attach_channel(&room, &assistant, AttachChannelOptions::default())
        .await?;
    // What the supervisor writes is for the people in the room, not for
    // its agents.
    let for_people = AttachChannelOptions {
        visibility: Visibility::Transport,
        ..Default::default()
    };
    kit.attach_channel(&room, supervisor.channel_id(), for_people)
        .await?;

    let spoken = "What time is it?";
    writes(&kit, &room, message(&call, spoken, Visibility::All)).await?;
    // Typed on the screen, the question asks for its answers there.
    let typed = "What is the meaning of life?";
    let on_screen = InboundMessage {
        response_visibility: Some(Visibility::Channels(ChannelSet::new([screen
            .channel_id()
            .clone()])?)),
        ..message(&screen, typed, Visibility::Intelligence)
    };
    writes(&kit, &room, on_screen).await?;
    let notice = "Can you keep your answers short?";
    writes(&kit, &room, message(&supervisor, notice, Visibility::All)).await?;
    let note = Visibility::Channels(ChannelSet::new([screen.channel_id().clone()])?);
    let noted = "Note: call recorded.";
    writes(&kit, &room, message(&supervisor, noted, note)).await?;

    let timeline = shared::timeline(&kit, &room).await?;
    shared::print_timeline(&timeline);
    let (heard, shown) = (call.texts(), screen.texts());
    println!("Spoken on the call: {heard:?}");
    println!("Shown on the screen: {shown:?}");
    let stored_event = |text: &str| {
        timeline
            .iter()
            .find(|event| text_of(&event.content) == text)
    };
    let stored = |text: &str| stored_event(text).map(|event| event.visibility.to_string());
    let carried = |text: &str| {
        stored_event(text)
            .and_then(|event| event.response_visibility.as_ref())
            .map(ToString::to_string)
    };
    let (spoken_answer, typed_answer) = (
        format!("Answer to {spoken:?}"),
        format!("Answer to {typed:?}"),
    );
    println!(
        "The typed question's answer is stored with its answers to {}.",
        carried(&typed_answer).as_deref().unwrap_or("anywhere")
    );

    let mut checks = Checks::default();
    checks.expect(
        "an answer to a spoken question reaches the call and the screen",
        heard.contains(&spoken_answer) && shown.contains(&spoken_answer),
    );
    checks.expect(
        "a typed question visible to intelligence channels is never spoken",
        !heard.iter().any(|text| text == typed),
    );
    checks.expect(
        "its answer, written for everyone, is stored within its response visibility: shown, never spoken",
        stored(&typed_answer).as_deref() == Some("screen")
            && shown.contains(&typed_answer)
            && !heard.contains(&typed_answer),
    );
    checks.expect(
        "that answer carries the question's response visibility, so what answers it stays on the screen",
        carried(&typed_answer).as_deref() == Some("screen") && carried(&spoken_answer).is_none(),
    );
    checks.expect(
        "the supervisor's question reaches the call and the screen, and the assistant never reads it",
        heard.contains(&notice.to_owned())
            && shown.contains(&notice.to_owned())
            && stored(notice).as_deref() == Some("transport")
            && stored(&format!("Answer to {notice:?}")).is_none(),
    );
    checks.expect(
        "the note to the screen is stored visible to the screen alone, and is never spoken",
        stored(noted).as_deref() == Some("screen")
            && shown.contains(&noted.to_owned())
            && !heard.contains(&noted.to_owned()),
    );
    kit.close().await?;
    Ok(checks.finish())
}
