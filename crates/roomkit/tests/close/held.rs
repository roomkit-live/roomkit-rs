//! What the rule-by-rule tests of the kit's close hold its work with: a
//! transport whose sends of some texts wait, and a model that writes then
//! says nothing.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_core::Stream;
use roomkit::model::{
    ChannelBinding, ChannelOutput, ChannelType, EventSource, InboundMessage, RoomContext,
    RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions,
    InboundResult, ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions,
};
use roomkit_ai::{
    AiContext, AiProvider, AiResponse, ProviderError, ProviderStream, StreamDone, StreamEvent,
    StreamTextDelta,
};
use roomkit_core::ops::ChannelConfig;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Sleep;

use crate::support::{channel, transport};
use roomkit_core::rules::transcoding::text_of;

/// How long a send held for its duration waits, at most a test's.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Hold {
    /// It waits this long.
    For(Duration),
    /// It waits until the transport is released.
    UntilReleased,
    /// It never ends.
    ForEver,
}

/// A transport whose send of a text it holds waits as its [`Hold`] says;
/// it tells each send's start, keeps the texts it sent, and notes, in
/// `steps`, a send dropped before it ended ("send cut") and its own close
/// ("closed").
#[derive(Clone)]
pub(crate) struct Holding {
    id: &'static str,
    holds: Arc<HashMap<&'static str, Hold>>,
    entered: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
    pub(crate) release: Arc<Notify>,
    sent: Arc<Mutex<Vec<String>>>,
    pub(crate) steps: Arc<Mutex<Vec<&'static str>>>,
}

impl Holding {
    pub(crate) fn new<const N: usize>(id: &'static str, holds: [(&'static str, Hold); N]) -> Self {
        Self {
            id,
            holds: Arc::new(holds.into_iter().collect()),
            entered: Arc::default(),
            release: Arc::default(),
            sent: Arc::default(),
            steps: Arc::default(),
        }
    }

    fn entered_on(&self, text: &str) -> Arc<Notify> {
        let mut entered = self.entered.lock().unwrap();
        Arc::clone(entered.entry(text.to_owned()).or_default())
    }

    /// Waits until a send of `text` started.
    pub(crate) async fn entered(&self, text: &str) {
        self.entered_on(text).notified().await;
    }

    /// The texts it sent, in order.
    pub(crate) fn sent(&self) -> Vec<String> {
        self.sent.lock().unwrap().clone()
    }

    /// What it noted.
    pub(crate) fn steps(&self) -> Vec<&'static str> {
        self.steps.lock().unwrap().clone()
    }
}

/// Notes a send dropped before it ended.
struct SendNote {
    steps: Arc<Mutex<Vec<&'static str>>>,
    done: bool,
}

impl Drop for SendNote {
    fn drop(&mut self) {
        if !self.done {
            self.steps.lock().unwrap().push("send cut");
        }
    }
}

impl Holding {
    /// The configuration the kit keeps of the channel.
    fn config(&self) -> ChannelConfig {
        transport(self.id)
    }
}

impl Channel for Holding {
    fn channel_id(&self) -> roomkit::model::ChannelId {
        self.config().channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.config().channel_type.clone()
    }

    fn category(&self) -> roomkit::model::ChannelCategory {
        self.config().category
    }

    fn direction(&self) -> roomkit::model::ChannelDirection {
        self.config().direction
    }

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
        self.config().capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        self.config().provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        self.config().recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.config().supports_streaming_delivery
    }

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = self.config();
        let addressing = roomkit::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: false,
        };
        (addressing, config.country_code.clone())
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let this = self.clone();
        async move {
            let text = text_of(&event.content);
            let mut note = SendNote {
                steps: Arc::clone(&this.steps),
                done: false,
            };
            this.entered_on(&text).notify_one();
            match this.holds.get(text.as_str()) {
                Some(Hold::For(delay)) => tokio::time::sleep(*delay).await,
                Some(Hold::UntilReleased) => this.release.notified().await,
                Some(Hold::ForEver) => std::future::pending().await,
                None => {}
            }
            note.done = true;
            this.sent.lock().unwrap().push(text);
            Ok(ChannelOutput::default())
        }
    }

    fn close(&self) -> impl Future<Output = ()> + Send {
        let steps = Arc::clone(&self.steps);
        async move { steps.lock().unwrap().push("closed") }
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(&message, &context)))
    }
}

pub(crate) fn room(id: &str) -> RoomId {
    RoomId::new(id).unwrap()
}

/// A kit on `store` with an SMS transport and `targets`, each attached to
/// every room of `rooms`.
pub(crate) async fn kit_of<S: ConversationStore>(
    store: S,
    targets: &[&Holding],
    rooms: &[&str],
) -> RoomKit<S> {
    let kit = RoomKit::new(RoomKitOptions::with_store(store));
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    for target in targets {
        kit.register_channel((*target).clone()).unwrap();
    }
    for id in rooms {
        let room_id = room(id);
        let options = CreateRoomOptions {
            room_id: Some(room_id.clone()),
            ..Default::default()
        };
        kit.create_room(options).await.unwrap();
        let mut ids = vec!["sms"];
        ids.extend(targets.iter().map(|target| target.id));
        for id in ids {
            kit.attach_channel(&room_id, &channel(id), AttachChannelOptions::default())
                .await
                .unwrap();
        }
    }
    kit
}

/// A message of `text` from the SMS transport.
pub(crate) fn said(text: &str) -> InboundMessage {
    InboundMessage::new(channel("sms"), "+15551234567", TextContent::new(text))
}

/// `text` into `room_id`, its delivery deferred.
pub(crate) async fn deferred<S: ConversationStore>(
    kit: &RoomKit<S>,
    text: &str,
    room_id: &str,
) -> InboundResult {
    let options = ProcessInboundOptions {
        room_id: Some(room(room_id)),
        defer_delivery: true,
        ..Default::default()
    };
    kit.process_inbound(said(text), options).await.unwrap()
}

/// Keeps the close in its grace: a send of "held" in `room_id` that waits
/// for `target`'s release.
pub(crate) async fn a_send_held<S: ConversationStore>(
    kit: &RoomKit<S>,
    target: &Holding,
    room_id: &str,
) -> InboundResult {
    let held = deferred(kit, "held", room_id).await;
    target.entered("held").await;
    held
}

/// A model that writes `first`, then says nothing for 30 s before it ends
/// its round.
pub(crate) struct Pausing {
    pub(crate) first: &'static str,
}

struct PausingStream {
    first: Option<&'static str>,
    pause: Pin<Box<Sleep>>,
    over: bool,
}

impl Stream for PausingStream {
    type Item = Result<StreamEvent, ProviderError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(text) = self.first.take() {
            return Poll::Ready(Some(Ok(StreamEvent::TextDelta(StreamTextDelta {
                text: text.to_owned(),
            }))));
        }
        if self.over {
            return Poll::Ready(None);
        }
        ready!(self.pause.as_mut().poll(cx));
        self.over = true;
        Poll::Ready(Some(Ok(StreamEvent::Done(StreamDone::default()))))
    }
}

impl AiProvider for Pausing {
    fn model_name(&self) -> &'static str {
        "pausing"
    }

    fn generate<'a>(
        &'a self,
        _context: &'a AiContext,
    ) -> impl std::future::Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        std::future::ready(Err("this model only streams".into()))
    }

    fn generate_structured_stream<'a>(&'a self, _context: &'a AiContext) -> ProviderStream<'a> {
        Box::pin(PausingStream {
            first: Some(self.first),
            pause: Box::pin(tokio::time::sleep(Duration::from_secs(30))),
            over: false,
        })
    }
}

/// Less than the close's grace: a wait that ends within it ended at once.
pub(crate) const AT_ONCE: Duration = Duration::from_secs(1);

/// The result's `blocked`, `reason` and `cancellation_reason`.
pub(crate) fn cut(result: &InboundResult) -> (bool, Option<&str>, Option<&str>) {
    (
        result.blocked,
        result.reason.as_deref(),
        result.cancellation_reason.as_deref(),
    )
}

/// What a message cut before its commit reads, Python's `cut_by_close()`.
pub(crate) const CUT_BY_CLOSE: (bool, Option<&str>, Option<&str>) =
    (true, Some("kit_closed"), Some("kit_closed"));

/// A record of `text` from the SMS transport, for room `r`.
pub(crate) fn record(text: &str) -> RoomEvent {
    let source = EventSource::new(channel("sms"), ChannelType::SMS);
    RoomEvent::new(room("r"), source, TextContent::new(text))
}

/// The texts `room_id` stores, its lifecycle records aside.
pub(crate) async fn stored<S: ConversationStore>(kit: &RoomKit<S>, room_id: &str) -> Vec<String> {
    let query = roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        ..roomkit::store::ListEventsOptions::default()
    };
    let events = kit
        .store()
        .list_events(&room(room_id), query)
        .await
        .unwrap();
    events
        .iter()
        .filter(|event| event.source.channel_type != ChannelType::SYSTEM)
        .map(|event| text_of(&event.content))
        .collect()
}

/// The kit's close, on a task of its own.
pub(crate) fn spawn_close<S: ConversationStore>(kit: &RoomKit<S>) -> Closing {
    let kit = kit.clone();
    tokio::spawn(async move { kit.close().await })
}

/// A close under way.
pub(crate) type Closing = JoinHandle<Result<(), RoomKitError>>;

/// A close that started.
pub(crate) async fn closing<S: ConversationStore>(kit: &RoomKit<S>) -> Closing {
    let close = spawn_close(kit);
    tokio::task::yield_now().await;
    close
}

/// Waits for `close`, which returns `Ok`.
pub(crate) async fn closed(close: Closing) {
    tokio::time::timeout(Duration::from_secs(60), close)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

/// Into `room_id`, its delivery awaited.
pub(crate) fn into(room_id: &str) -> ProcessInboundOptions {
    ProcessInboundOptions {
        room_id: Some(room(room_id)),
        ..Default::default()
    }
}

/// `text` into `room_id`, on a task of its own, its delivery awaited.
pub(crate) fn spawn_said<S: ConversationStore>(
    kit: &RoomKit<S>,
    text: &str,
    room_id: &str,
) -> JoinHandle<Result<InboundResult, RoomKitError>> {
    let (kit, message, options) = (kit.clone(), said(text), into(room_id));
    tokio::spawn(async move { kit.process_inbound(message, options).await })
}

/// What `call` came to, within the bound.
pub(crate) async fn result_of(
    call: JoinHandle<Result<InboundResult, RoomKitError>>,
) -> InboundResult {
    tokio::time::timeout(Duration::from_secs(60), call)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

/// `model`'s agent, with `tools`, attached to `r`.
pub(crate) async fn attach_agent<S: ConversationStore>(
    kit: &RoomKit<S>,
    model: impl AiProvider,
    tools: roomkit_ai::Tools,
) {
    let agent = roomkit_ai::AiAgent::new(channel("agent"), model)
        .with_tools(tools)
        .unwrap();
    kit.register_channel(agent).unwrap();
    kit.attach_channel(
        &room("r"),
        &channel("agent"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
}
