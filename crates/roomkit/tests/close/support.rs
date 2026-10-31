//! A room with an SMS transport and a witness channel, the kit's own work
//! closing the kit from one door or another, and what the tests read of a
//! call and of the room.
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    HookResult, InboundMessage, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, ConversationStore,
    CreateRoomOptions, EventFilter, EventQuery, HookExecution, HookOptions, HookTrigger,
    InboundResult, ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiAgent, AiProvider, ToolDefinition, ToolOutput, Tools};
use roomkit_core::rules::transcoding::text_of;
use serde_json::json;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// How long a wait that should end may take before the test fails.
pub(crate) const BOUND: Duration = Duration::from_secs(5);

/// Less than the close's grace, 5 seconds, which a close that waited for
/// the work it was called from, or for what waits behind that work, would
/// take.
pub(crate) const AT_ONCE: Duration = Duration::from_secs(1);

pub(crate) fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

pub(crate) fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

pub(crate) fn message(text: &str) -> InboundMessage {
    InboundMessage::new(channel("sms"), "+15551234567", TextContent::new(text))
}

pub(crate) fn into(room_id: RoomId) -> ProcessInboundOptions {
    ProcessInboundOptions {
        room_id: Some(room_id),
        ..Default::default()
    }
}

pub(crate) fn transport(id: &str) -> ChannelDeclaration {
    ChannelDeclaration::new(
        channel(id),
        ChannelType::WEBSOCKET,
        ChannelCategory::Transport,
        ChannelDirection::Bidirectional,
    )
}

/// A channel that says whether it was closed and keeps the texts it
/// delivered, each marked when it came once the channel was closed; a
/// closer set on it closes the kit from its first delivery, and a delivery
/// of the text `slow` names takes its time, once its gate let it go.
#[derive(Clone, Default)]
pub(crate) struct Witness {
    pub(crate) closed: Arc<AtomicBool>,
    delivered: Arc<Mutex<Vec<String>>>,
    pub(crate) closes_on_delivery: Arc<Mutex<Option<Closer>>>,
    slow: Option<(&'static str, Duration)>,
    gate: Option<Gate>,
}

/// A slow delivery's gate: the witness tells the delivery began, then
/// waits to be let go before it takes its time, so that a test acts while
/// it delivers whatever the load, not by racing a timer against it.
#[derive(Clone, Default)]
pub(crate) struct Gate {
    began: Arc<Notify>,
    go: Arc<Notify>,
}

impl Gate {
    /// Waits for the slow delivery to begin.
    pub(crate) async fn began(&self) {
        self.began.notified().await;
    }

    /// Lets the slow delivery go on to take its time.
    pub(crate) fn go(&self) {
        self.go.notify_one();
    }
}

impl Witness {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// A witness whose delivery of `text` takes `delay`.
    pub(crate) fn slow(text: &'static str, delay: Duration) -> Self {
        Self {
            slow: Some((text, delay)),
            ..Self::default()
        }
    }

    /// The same witness, its slow delivery held by `gate`.
    pub(crate) fn gated(self, gate: Gate) -> Self {
        Self {
            gate: Some(gate),
            ..self
        }
    }

    /// The texts delivered, in order, ` (closed)` after one that came once
    /// the channel was closed.
    pub(crate) fn delivered(&self) -> Vec<String> {
        self.delivered.lock().unwrap().clone()
    }

    /// Whether `text` was delivered, and nothing once the channel was
    /// closed.
    pub(crate) fn delivered_before_its_close(&self, text: &str) -> (bool, bool) {
        let delivered = self.delivered();
        (
            delivered.iter().any(|delivered| delivered == text),
            delivered
                .iter()
                .all(|delivered| !delivered.ends_with(" (closed)")),
        )
    }
}

impl Channel for Witness {
    fn declaration(&self) -> ChannelDeclaration {
        transport("witness")
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
            let mark = if this.closed.load(Ordering::SeqCst) {
                " (closed)"
            } else {
                ""
            };
            this.delivered.lock().unwrap().push(format!("{text}{mark}"));
            if let Some((slow, delay)) = this.slow
                && slow == text
            {
                if let Some(gate) = &this.gate {
                    gate.began.notify_one();
                    gate.go.notified().await;
                }
                tokio::time::sleep(delay).await;
            }
            let closer = this.closes_on_delivery.lock().unwrap().take();
            if let Some(closer) = closer {
                closer.close().await;
            }
            Ok(ChannelOutput::default())
        }
    }

    fn close(&self) -> impl Future<Output = ()> + Send {
        let closed = Arc::clone(&self.closed);
        async move { closed.store(true, Ordering::SeqCst) }
    }
}

/// What a work's close answered, and whether the witness was closed then.
type Returned = (Result<(), RoomKitError>, bool);

/// The kit's own work closing the kit, once, once let go when it is gated;
/// it keeps what its close answered and whether the witness was closed by
/// then.
#[derive(Clone)]
pub(crate) struct Closer {
    kit: RoomKit,
    witness: Arc<AtomicBool>,
    gated: bool,
    pub(crate) entered: Arc<Notify>,
    pub(crate) go: Arc<Notify>,
    /// Told once its `close()` started: polled once, the shutdown begun.
    pub(crate) closing: Arc<Notify>,
    once: Arc<AtomicBool>,
    returned: Arc<Mutex<Option<Returned>>>,
    returned_now: Arc<Notify>,
}

impl Closer {
    pub(crate) fn new(kit: &RoomKit, witness: &Witness, gated: bool) -> Self {
        Self {
            kit: kit.clone(),
            witness: Arc::clone(&witness.closed),
            gated,
            entered: Arc::default(),
            go: Arc::default(),
            closing: Arc::default(),
            once: Arc::default(),
            returned: Arc::default(),
            returned_now: Arc::default(),
        }
    }

    pub(crate) async fn close(&self) {
        if self.once.swap(true, Ordering::SeqCst) {
            return;
        }
        self.entered.notify_one();
        if self.gated {
            self.go.notified().await;
        }
        let mut close = std::pin::pin!(self.kit.close());
        // The close starts at its first poll.
        let first = std::future::poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx))).await;
        self.closing.notify_one();
        let closed = match first {
            Poll::Ready(closed) => closed,
            Poll::Pending => close.await,
        };
        *self.returned.lock().unwrap() = Some((closed, self.witness.load(Ordering::SeqCst)));
        self.returned_now.notify_one();
    }

    /// Whether the work's close returned `Ok`, within a minute, the witness
    /// closed by then.
    pub(crate) async fn returned_once_closed(&self) -> Option<bool> {
        let returned = self.returned_now.notified();
        drop(tokio::time::timeout(Duration::from_secs(60), returned).await);
        let returned = self.returned.lock().unwrap();
        returned
            .as_ref()
            .map(|(closed, witness)| closed.is_ok() && *witness)
    }
}

/// A kit with an SMS transport and `witness` attached to `room_1`.
pub(crate) async fn kit_with(witness: &Witness) -> RoomKit {
    kit_on(RoomKit::new(RoomKitOptions::default()), witness).await
}

/// `kit`, with an SMS transport and `witness` attached to `room_1`.
pub(crate) async fn kit_on<S: ConversationStore>(kit: RoomKit<S>, witness: &Witness) -> RoomKit<S> {
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(witness.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "witness"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    kit
}

/// Whether the kit is sealed: a room is refused.
pub(crate) async fn sealed(kit: &RoomKit) -> bool {
    let options = CreateRoomOptions {
        room_id: Some(RoomId::new("after").unwrap()),
        ..Default::default()
    };
    matches!(kit.create_room(options).await, Err(RoomKitError::KitClosed))
}

/// The texts `room_1` stores, its lifecycle records aside.
pub(crate) async fn stored<S: ConversationStore>(kit: &RoomKit<S>) -> Vec<String> {
    stored_in(kit, &room_id()).await
}

/// The texts `room_id` stores, its lifecycle records aside.
pub(crate) async fn stored_in<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
) -> Vec<String> {
    let query = EventQuery {
        limit: u64::MAX,
        event_filter: Some(EventFilter {
            include_blocked: true,
            ..EventFilter::default()
        }),
        ..EventQuery::default()
    };
    let events = kit.store().list_events(room_id, &query).await.unwrap();
    events
        .iter()
        .filter(|event| event.source.channel_type != ChannelType::SYSTEM)
        .map(|event| text_of(&event.content))
        .collect()
}

pub(crate) type Call = JoinHandle<Result<InboundResult, RoomKitError>>;

pub(crate) fn spawn_message<S: ConversationStore>(
    kit: &RoomKit<S>,
    text: &str,
    room_id: RoomId,
) -> Call {
    let kit = kit.clone();
    let message = message(text);
    tokio::spawn(async move { kit.process_inbound(message, into(room_id)).await })
}

/// What a call came to: its error, or its result's `cancellation_reason`
/// and the type of its result's error.
pub(crate) type Answered = Result<(Option<String>, Option<String>), RoomKitError>;

/// A call's result, its `cancellation_reason` and its error's type.
#[expect(
    clippy::unnecessary_wraps,
    reason = "the `Ok` arm of what a call came to, compared with `ended`'s"
)]
pub(crate) fn ok(reason: Option<&str>, error: Option<&str>) -> Answered {
    Ok((reason.map(str::to_owned), error.map(str::to_owned)))
}

/// What a call came to, within the bound; `None` for one that did not end.
pub(crate) async fn ended(call: Call) -> Option<Answered> {
    let joined = tokio::time::timeout(BOUND, call).await.ok()?.unwrap();
    Some(joined.map(|result| {
        let error = result.error.map(|error| error.error_type);
        (result.cancellation_reason, error)
    }))
}

/// Where the kit's own work closes the kit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Door {
    /// A SYNC `BEFORE_BROADCAST` hook's body, under the room's lock.
    SyncHook,
    /// An ASYNC `AFTER_BROADCAST` hook the delivery fires.
    DeliveryHook,
    /// The witness's delivery.
    Delivery,
    /// An ASYNC `BEFORE_BROADCAST` observer, run after the fact.
    Observer,
    /// An agent's tool call, in its room's turn.
    ToolCall,
}

pub(crate) const DOORS: [Door; 5] = [
    Door::SyncHook,
    Door::DeliveryHook,
    Door::Delivery,
    Door::Observer,
    Door::ToolCall,
];

impl Door {
    /// What the call of the message whose work closes the kit comes to,
    /// `joining` a close begun from outside or not, as the Python
    /// implementation's call does (`rmk1030-rs/py_doors.out`): a call under
    /// way as the close starts reads `kit_closed` (RFC §12.10.4), the SYNC
    /// hook's as one cut before its commit, which the sealed kit refused,
    /// nothing of it stored. Rust's delivery does not wait for the ASYNC
    /// `AFTER_BROADCAST` hook it fires, a difference of its own: its call is
    /// over before that hook closes the kit, and reads no reason. An
    /// observer joining a close begun from outside runs after its call
    /// ended, which reads no reason either.
    pub(crate) fn call_ends(self, joining: bool) -> Answered {
        match self {
            Self::DeliveryHook => ok(None, None),
            Self::Observer if joining => ok(None, None),
            Self::SyncHook | Self::Delivery | Self::Observer | Self::ToolCall => {
                ok(Some("kit_closed"), None)
            }
        }
    }

    /// What `room_1` stores once the call ended: nothing of the message
    /// whose commit was refused, and the tool's start and end rows, the
    /// end row stored as the close cut the turn (RFC §12.10.4).
    pub(crate) fn stores(self) -> Vec<String> {
        let stored: &[&str] = match self {
            Self::SyncHook => &[],
            Self::ToolCall => &["Bye.", "[Tool call: shut_down]", "[Tool call: shut_down]"],
            Self::DeliveryHook | Self::Delivery | Self::Observer => &["Bye."],
        };
        stored.iter().map(|text| (*text).to_owned()).collect()
    }

    /// Whether the witness reads "Bye.": not when the commit refused it.
    /// An observer closing the kit runs once its delivery set took its
    /// place in the idle lane, running from it, as Python's (`py_doors.out`,
    /// `rmk1030-rs-review-B/py_probe.out` observer door), so its set
    /// delivers within the close's grace; on 01c8420 it ran as the commit
    /// was written and the close dropped the set.
    pub(crate) fn delivers(self) -> bool {
        !matches!(self, Self::SyncHook)
    }
}

/// The door's kit, its closer, and the message whose work closes the kit,
/// running.
pub(crate) async fn open(door: Door, witness: &Witness, gated: bool) -> (RoomKit, Closer, Call) {
    let kit = kit_with(witness).await;
    let closer = Closer::new(&kit, witness, gated);
    let hook = |trigger, execution| {
        let work = closer.clone();
        let spec = HookOptions {
            execution,
            name: "quit".to_owned(),
            ..Default::default()
        };
        kit.hook(
            trigger,
            move |_event: RoomEvent, _context| {
                let work = work.clone();
                async move {
                    work.close().await;
                    Ok(HookResult::allow(AllowOptions::default()))
                }
            },
            spec,
        )
        .unwrap();
    };
    match door {
        Door::SyncHook => hook(HookTrigger::BeforeBroadcast, HookExecution::Sync),
        Door::DeliveryHook => hook(HookTrigger::AfterBroadcast, HookExecution::Async),
        Door::Observer => hook(HookTrigger::BeforeBroadcast, HookExecution::Async),
        Door::Delivery => *witness.closes_on_delivery.lock().unwrap() = Some(closer.clone()),
        Door::ToolCall => closing_agent(&kit, &closer).await,
    }
    let call = spawn_message(&kit, "Bye.", room_id());
    (kit, closer, call)
}

/// An agent in `room_1` whose one tool closes the kit, then answers.
async fn closing_agent(kit: &RoomKit, closer: &Closer) {
    let work = closer.clone();
    let tools = Tools::new().tool(
        ToolDefinition::new("shut_down", "Shut down.", json!({ "type": "object" })),
        move |_call, _context| {
            let work = work.clone();
            async move {
                work.close().await;
                Ok(ToolOutput::Text("closed".to_owned()))
            }
        },
    );
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "shut_down", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    attach_agent(kit, model, tools).await;
}

/// `model`'s agent, with `tools`, attached to `room_1`.
pub(crate) async fn attach_agent<S: ConversationStore>(
    kit: &RoomKit<S>,
    model: impl AiProvider,
    tools: Tools,
) {
    let agent = AiAgent::new(channel("agent"), model)
        .with_tools(tools)
        .unwrap();
    kit.register_channel(agent).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("agent"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
}
