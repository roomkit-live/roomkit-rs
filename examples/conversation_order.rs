//! Read a conversation in the order it was written, live and from the store
//! (RFC §6.4).
//!
//! An agent asks another session a question and starts writing "I wait for
//! its answer." The answer arrives before the agent's first word: the
//! application stores it in the room and hands it to the running turn, which
//! then gives the value. The room's timeline keeps the order the rows were
//! stored in, the answer before the wait. Each streamed text also names the
//! row the room held when its writing began (`metadata["follows_index"]`),
//! and the event a streaming channel is handed with the stream names it
//! before the first word. A screen that places each text after the row it
//! follows shows the wait, the answer, the value, while the turn runs and
//! when the conversation is opened again. The Python implementation's
//! `examples/conversation_order.py`.
//!
//! It runs on a stand-in model, so it needs no network and no key.
//!
//! ```sh
//! cargo run --example conversation_order
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::future::{Future, ready};
use std::process::ExitCode;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, EventSource, EventType,
    InboundMessage, InjectMessage, RoomContext, RoomEvent, RoomId, TextContent, Visibility,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CommitEventOptions, CreateRoomOptions,
    DeliveryStream, GetTimelineOptions, ProcessInboundOptions, RoomKit, RoomKitOptions,
    SteerOptions, StreamItem,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiMessage, AiProvider, AiResponse, ProviderError,
};
use serde_json::Value;
use tokio::sync::watch;

use shared::{Checks, recording, text_of};

const ASK: &str = "Ask @review for the value in its file.";
const WAIT: &str = "I asked @review and wait for its answer.";
const REPLY: &str = "Reply from @review: MAC-ERABLE";
const VALUE: &str = "The value is MAC-ERABLE.";

/// The row an event names as the one it follows.
fn followed(event: &RoomEvent) -> Option<u64> {
    event.metadata.get("follows_index").and_then(Value::as_u64)
}

/// Where a row reads: after the row it follows, else at its index.
type Place = (u64, u64, String);

fn place(event: &RoomEvent) -> Place {
    (
        followed(event).unwrap_or(event.index),
        event.index,
        text_of(&event.content),
    )
}

/// The texts of `rows` in the order they read.
fn read(mut rows: Vec<Place>) -> Vec<String> {
    rows.sort();
    rows.into_iter().map(|(_, _, text)| text).collect()
}

/// A stand-in model: it says it waits; asked again once the answer came, it
/// gives the value. Its first call says it started, then waits for its
/// first word to be let out.
struct Asker {
    generating: watch::Sender<bool>,
    first_word: watch::Receiver<bool>,
}

impl AiProvider for Asker {
    fn model_name(&self) -> &'static str {
        "asker"
    }

    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        let answered = context
            .messages
            .iter()
            .map(AiMessage::text)
            .any(|text| text.contains("MAC-ERABLE"));
        if answered {
            return Ok(AiResponse::new(VALUE));
        }
        self.generating.send_replace(true);
        // The model call is under way, its first word not written.
        drop(self.first_word.clone().wait_for(|out| *out).await);
        Ok(AiResponse::new(WAIT))
    }
}

/// What a screen shows.
#[derive(Default)]
struct View {
    shown: Vec<Place>,
    /// The text under way, after the row it follows, below every stored row.
    writing: Option<Place>,
}

impl View {
    fn read(&self) -> Vec<String> {
        read(self.shown.iter().chain(&self.writing).cloned().collect())
    }
}

/// A streaming display: each text goes after the row it follows.
#[derive(Clone, Default)]
struct Screen(Arc<Mutex<View>>);

impl Screen {
    fn view(&self) -> MutexGuard<'_, View> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn show(&self, event: &RoomEvent) {
        self.view().shown.push(place(event));
    }
}

impl Channel for Screen {
    fn channel_id(&self) -> ChannelId {
        ChannelId::new("screen").unwrap_or_else(|_| unreachable!("a channel id"))
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Transport
    }

    fn recipient_key(&self) -> Option<String> {
        None
    }

    fn supports_streaming_delivery(&self) -> bool {
        true
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        self.show(&event);
        ready(Ok(ChannelOutput::empty()))
    }

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        event: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        // The event the response is rendered under names the row its first
        // text follows; each row of the stream is the one the next follows.
        let mut follows = followed(&event).ok_or("the stream names no row its text follows")?;
        while let Some(item) = stream.next().await {
            let item = item?;
            let mut view = self.view();
            if let StreamItem::Text(delta) = &item {
                let written = view.writing.take().map(|(_, _, text)| text);
                let text = written.unwrap_or_default() + delta;
                view.writing = Some((follows, u64::MAX, text));
                println!("Live, at a delta: {:?}", view.read());
            } else if let StreamItem::Row(row) = &item {
                view.writing = None;
                view.shown.push(place(row));
                follows = row.index;
            }
        }
        Ok(ChannelOutput::empty())
    }
}

/// The other session's answer, as the application keeps it in the room: a
/// record the screen alone reads.
fn answer(room: &RoomId, screen: &ChannelId) -> RoomEvent {
    RoomEvent {
        r#type: EventType::System,
        visibility: Visibility::channels([screen.clone()]).unwrap_or(Visibility::None),
        ..RoomEvent::new(
            room.clone(),
            EventSource::new(screen.clone(), ChannelType::WEBSOCKET),
            TextContent::new(REPLY),
        )
    }
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let (generating, mut started) = watch::channel(false);
    let (first_word, waiting) = watch::channel(false);
    let asker = Asker {
        generating,
        first_word: waiting,
    };
    let (room, api) = (RoomId::new("session")?, ChannelId::new("api")?);
    let screen = Screen::default();
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(recording("ws", ChannelType::WEBSOCKET)?)?;
    kit.register_channel(screen.clone())?;
    kit.register_channel(AiChannel::new(
        api.clone(),
        asker,
        AiChannelOptions::default(),
    )?)?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    for channel in ["ws", "screen", "api"] {
        let channel = ChannelId::new(channel)?;
        kit.attach_channel(&room, &channel, AttachChannelOptions::default())
            .await?;
    }

    let question = InboundMessage::new(ChannelId::new("ws")?, "dev", TextContent::new(ASK));
    let options = ProcessInboundOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    };
    let asking = kit.clone();
    let turn = tokio::spawn(async move { asking.process_inbound(question, options).await });
    started.wait_for(|started| *started).await?;

    // The other session answers while the model call is under way: the
    // application keeps the answer in the room and hands it to the turn.
    let record = answer(&room, &screen.channel_id());
    let stored = kit
        .commit_event(&room, record, CommitEventOptions::default())
        .await?;
    screen.show(&stored);
    let told = InjectMessage::new("@review answered: MAC-ERABLE");
    let reached = kit
        .steer(&room, &api, told, SteerOptions::default())
        .await?;
    first_word.send_replace(true);
    turn.await??;

    let timeline = kit
        .get_timeline(&room, GetTimelineOptions::default())
        .await?;
    kit.close().await?;
    let conversation: Vec<&RoomEvent> = timeline
        .iter()
        .filter(|event| event.visibility != Visibility::Internal)
        .collect();
    let by_index: Vec<String> = conversation
        .iter()
        .map(|event| text_of(&event.content))
        .collect();
    let read_back = read(conversation.iter().map(|event| place(event)).collect());
    let live = screen.view().read();
    println!("Stored, by index: {by_index:?}");
    println!("Live, at the end: {live:?}");
    println!("Opened again:     {read_back:?}");

    let mut checks = Checks::default();
    checks.expect("the answer reached the running turn", reached == 1);
    checks.expect(
        "the room stores the answer before the wait, the order they were written in",
        by_index == [ASK, REPLY, WAIT, VALUE],
    );
    checks.expect(
        "the screen shows the wait above the answer while the turn runs",
        live == [ASK, WAIT, REPLY, VALUE],
    );
    checks.expect(
        "opened again, the conversation reads the same",
        read_back == live,
    );
    Ok(checks.finish())
}
