//! Who sees what (RFC §5.12, §7.3, §7.5 rule 8), in a hybrid voice and text
//! interface: the user speaks on a call and types on a screen, an assistant
//! answers, and a supervisor watches. Each event carries a visibility of
//! its own, and its source's binding has one: a channel sees the event only
//! when both include it, and the framework stores that intersection on the
//! event when it commits it. A message also says where its answers may go,
//! its `response_visibility`: each answer keeps the visibility its channel
//! gave it within that one, and is never shown wider.
//!
//! - A question typed on the screen carries `intelligence`: the assistant
//!   reads it, the call never speaks it. It asks for its answers on the
//!   screen: the assistant's answer, which it writes for everyone, is
//!   stored visible to the screen alone, shown and never spoken.
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
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, EventDirection, EventSource, EventType, InboundMessage, RoomContext, RoomEvent,
    RoomId, Visibility,
};
use roomkit::testing::RecordingTransport;
use roomkit::{Channel, ChannelDeclaration, ChannelError, RoomKit, text_of};

use shared::Checks;

/// An assistant that answers each question, for everyone: where an answer
/// goes is the question's to say.
struct Assistant {
    declaration: ChannelDeclaration,
}

impl Assistant {
    fn new(channel_id: ChannelId) -> Self {
        let declaration = ChannelDeclaration::new(
            channel_id,
            ChannelType::Ai,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        );
        Self { declaration }
    }

    fn answer(&self, question: &RoomEvent) -> RoomEvent {
        let source = EventSource::new(
            self.declaration.channel_id.clone(),
            ChannelType::Ai,
            EventDirection::Outbound,
        );
        let text = format!("Answer to {:?}", text_of(&question.content));
        // The pipeline gives the answer its own id, room and depth, and
        // narrows it to where the question's answers may go.
        RoomEvent::new(
            question.id.clone(),
            question.room_id.clone(),
            EventType::Message,
            source,
            EventContent::text(text),
            question.created_at,
        )
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
            ChannelOutput::respond(vec![self.answer(&event)])
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
        ..InboundMessage::new(from.id().clone(), "user", EventContent::text(text))
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
    kit.process_inbound(message, Some(room)).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = shared::kit();
    let call = RecordingTransport::new(ChannelId::new("call")?, ChannelType::Voice);
    let screen = RecordingTransport::new(ChannelId::new("screen")?, ChannelType::Websocket);
    let supervisor = RecordingTransport::new(ChannelId::new("supervisor")?, ChannelType::Websocket);
    let assistant = ChannelId::new("assistant")?;
    kit.register_channel(call.clone())?;
    kit.register_channel(screen.clone())?;
    kit.register_channel(supervisor.clone())?;
    kit.register_channel(Assistant::new(assistant.clone()))?;

    let room = RoomId::new("hybrid")?;
    kit.create_room(&room).await?;
    kit.attach(&room, call.id()).await?;
    kit.attach(&room, screen.id()).await?;
    kit.attach(&room, &assistant).await?;
    // What the supervisor writes is for the people in the room, not for
    // its agents.
    kit.attach_with(&room, supervisor.id(), |binding| {
        binding.visibility = Visibility::Transport;
    })
    .await?;

    let spoken = "What time is it?";
    writes(&kit, &room, message(&call, spoken, Visibility::All)).await?;
    // Typed on the screen, the question asks for its answers there.
    let typed = "What is the meaning of life?";
    let on_screen = InboundMessage {
        response_visibility: Some(Visibility::channels([screen.id().clone()])?),
        ..message(&screen, typed, Visibility::Intelligence)
    };
    writes(&kit, &room, on_screen).await?;
    let notice = "Can you keep your answers short?";
    writes(&kit, &room, message(&supervisor, notice, Visibility::All)).await?;
    let note = Visibility::channels([screen.id().clone()])?;
    let noted = "Note: call recorded.";
    writes(&kit, &room, message(&supervisor, noted, note)).await?;

    let timeline = shared::timeline(&kit, &room).await?;
    shared::print_timeline(&timeline);
    let (heard, shown) = (call.texts(), screen.texts());
    println!("Spoken on the call: {heard:?}");
    println!("Shown on the screen: {shown:?}");
    let stored = |text: &str| {
        timeline
            .iter()
            .find(|event| text_of(&event.content) == text)
            .map(|event| event.visibility.to_string())
    };
    let (spoken_answer, typed_answer) = (
        format!("Answer to {spoken:?}"),
        format!("Answer to {typed:?}"),
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
    Ok(checks.finish())
}
