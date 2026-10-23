//! Who sees what (RFC §7.3, §7.5 rule 8), in a hybrid voice and text
//! interface: the user speaks on a call and types on a screen, an assistant
//! answers, and a supervisor watches. Each event carries a visibility of
//! its own, and its source's binding has one: a channel sees the event only
//! when both include it, and the framework stores that intersection on the
//! event when it commits it.
//!
//! - A question typed on the screen carries `intelligence`: the assistant
//!   reads it, the call never speaks it.
//! - The assistant answers where it was asked: its answer to a typed
//!   question carries the screen's id, so it is shown and never spoken; its
//!   answer to a spoken question reaches both.
//! - The supervisor's binding is visible to `transport`: what they write
//!   reaches the call and the screen, and no agent reads it. A note they
//!   address to the screen alone is stored visible to it only.
//!
//! The RFC's `response_visibility` on an inbound message, where its answers
//! may go, is not applied by roomkit-rs yet: here the assistant narrows its
//! own answers.
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
use roomkit::ops::ChannelDeclaration;
use roomkit::testing::RecordingTransport;
use roomkit::{Channel, ChannelError, RoomKit, text_of};

use shared::Checks;

/// An assistant that answers each question where it was asked: on the
/// screen alone when it was typed there, to everyone otherwise.
struct Assistant {
    declaration: ChannelDeclaration,
    screen: ChannelId,
}

impl Assistant {
    fn new(channel_id: ChannelId, screen: ChannelId) -> Self {
        let declaration = ChannelDeclaration::new(
            channel_id,
            ChannelType::Ai,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        );
        Self {
            declaration,
            screen,
        }
    }

    fn answer(&self, question: &RoomEvent) -> Result<RoomEvent, ChannelError> {
        let source = EventSource::new(
            self.declaration.channel_id.clone(),
            ChannelType::Ai,
            EventDirection::Outbound,
        );
        let text = format!("Answer to {:?}", text_of(&question.content));
        // The pipeline gives the answer its own id, room and depth.
        let answer = RoomEvent::new(
            question.id.clone(),
            question.room_id.clone(),
            EventType::Message,
            source,
            EventContent::text(text),
            question.created_at,
        );
        // Typed on the screen, the answer is for the screen alone.
        let visibility = if question.source.channel_id == self.screen {
            Visibility::channels([self.screen.clone()])?
        } else {
            Visibility::All
        };
        Ok(RoomEvent {
            visibility,
            ..answer
        })
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
            self.answer(&event)
                .map(|answer| ChannelOutput::respond(vec![answer]))
        } else {
            Ok(ChannelOutput::default())
        };
        ready(output)
    }
}

/// `from` writes `text` into `room`, the event visible to `visibility`.
async fn writes(
    kit: &RoomKit,
    room: &RoomId,
    from: &RecordingTransport,
    text: &str,
    visibility: Visibility,
) -> Result<(), Box<dyn Error>> {
    println!("{} writes {text:?}, visible to {visibility}.", from.id());
    let message = InboundMessage {
        visibility,
        ..InboundMessage::new(from.id().clone(), "user", EventContent::text(text))
    };
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
    kit.register_channel(Assistant::new(assistant.clone(), screen.id().clone()))?;

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

    writes(&kit, &room, &call, "What time is it?", Visibility::All).await?;
    let typed = "What is the meaning of life?";
    writes(&kit, &room, &screen, typed, Visibility::Intelligence).await?;
    let notice = "Can you keep your answers short?";
    writes(&kit, &room, &supervisor, notice, Visibility::All).await?;
    let note = Visibility::channels([screen.id().clone()])?;
    writes(&kit, &room, &supervisor, "Note: call recorded.", note).await?;

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

    let mut checks = Checks::default();
    checks.expect(
        "an answer to a spoken question reaches the call and the screen",
        heard.contains(&"Answer to \"What time is it?\"".to_owned())
            && shown.contains(&"Answer to \"What time is it?\"".to_owned()),
    );
    checks.expect(
        "a typed question visible to intelligence channels is never spoken",
        !heard.iter().any(|text| text == typed),
    );
    checks.expect(
        "its answer, visible to the screen alone, is shown and never spoken",
        shown.contains(&format!("Answer to {typed:?}"))
            && !heard.contains(&format!("Answer to {typed:?}")),
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
        stored("Note: call recorded.").as_deref() == Some("screen")
            && shown.contains(&"Note: call recorded.".to_owned())
            && !heard.contains(&"Note: call recorded.".to_owned()),
    );
    Ok(checks.finish())
}
