//! A held message released by a `BEFORE_BROADCAST` hook whose work goes on
//! after the release (RFC §11.7.6), on the store a test gives: the Python
//! implementation's `tests/test_verification_release_cancel.py::_a_hook_releases`.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::identity::{MockIdentityResolver, MockIdentityResolverOptions};
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, HookResult, Identity,
    IdentityId, InboundMessage, RoomContext, RoomEvent, RoomId, TextContent, VerificationPolicy,
};
use roomkit::store::ListEventsOptions;
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, EventTap, RecordingTransport, RecordingTransportOptions,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CloseRoomOptions, CompleteVerificationOptions,
    ConversationStore, CreateRoomOptions, HookOptions, HookTrigger, InboundResult,
    ProcessInboundOptions, RequestVerificationOptions, RoomKit, RoomKitOptions,
};
use roomkit_core::rules::transcoding::text_of;
use serde_json::Value;
use tokio::sync::watch;

const ALICE: &str = "+15550000001";
/// The held message's delivery key, which names the release's answer.
const KEY: &str = "task-42";
/// How long the hook's own work lasts at most, once it completed the request.
const ITS_WORK: Duration = Duration::from_millis(20);
const BOUND: Duration = Duration::from_secs(2);

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room() -> RoomId {
    RoomId::new("room-1").unwrap()
}

/// What the kit did once the hook's call returned and the release's answer
/// ended.
pub(crate) struct Released<S: ConversationStore> {
    pub(crate) kit: RoomKit<S>,
    /// The held message.
    pub(crate) held: RoomEvent,
    /// The result of the console's note, whose hook released it.
    pub(crate) note: InboundResult,
    /// The text of each event the agent was asked to answer, in order.
    pub(crate) asked: Vec<String>,
    /// The operation of each `room_refused_event`, in order.
    pub(crate) refused: Vec<String>,
}

impl<S: ConversationStore> Released<S> {
    /// The agent's answers stored in the room, by index.
    pub(crate) async fn replies(&self) -> Vec<RoomEvent> {
        let options = ListEventsOptions {
            limit: 50,
            ..ListEventsOptions::default()
        };
        let events = self.kit.store().list_events(&room(), options).await;
        events
            .unwrap()
            .into_iter()
            .filter(|event| event.source.channel_id == channel("ai"))
            .collect()
    }
}

/// An echo agent that notes each event it is asked about, and says it was
/// asked through `over`.
struct Asked {
    inner: EchoAgent,
    noted: Arc<Mutex<Vec<String>>>,
    over: Arc<watch::Sender<bool>>,
}

impl Channel for Asked {
    fn channel_id(&self) -> ChannelId {
        self.inner.channel_id().clone()
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        self.inner.handle_inbound(message, context)
    }

    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        self.inner.deliver(event, binding, context)
    }

    fn on_event(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        self.noted.lock().unwrap().push(text_of(&event.content));
        self.over.send_replace(true);
        self.inner.on_event(event, binding, context)
    }
}

/// A kit over `store` verifying its senders, whose room binds Alice's SMS
/// channel, the console's WebSocket channel `bank` and an agent.
async fn kit_on<S: ConversationStore>(
    store: S,
    asked: &Arc<Mutex<Vec<String>>>,
    over: &Arc<watch::Sender<bool>>,
) -> RoomKit<S> {
    let resolver = MockIdentityResolver::new(MockIdentityResolverOptions {
        mapping: BTreeMap::from([(
            ALICE.to_owned(),
            Identity::new(IdentityId::new("id-alice").unwrap()),
        )]),
        ..MockIdentityResolverOptions::default()
    });
    let kit = RoomKit::new(RoomKitOptions {
        identity_resolver: Some(Arc::new(resolver)),
        verification_policy: Some(VerificationPolicy::default()),
        ..RoomKitOptions::with_store(store)
    });
    for (id, kind) in [("sms", ChannelType::SMS), ("bank", ChannelType::WEBSOCKET)] {
        let transport =
            RecordingTransport::new(channel(id), kind, RecordingTransportOptions::default());
        kit.register_channel(transport).unwrap();
    }
    kit.register_channel(Asked {
        inner: EchoAgent::new(channel("ai"), "answer", EchoAgentOptions::default()),
        noted: Arc::clone(asked),
        over: Arc::clone(over),
    })
    .unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    for id in ["sms", "bank", "ai"] {
        kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    kit
}

/// `sender`'s message `body` through `channel_id`, its delivery named `key`.
async fn send<S: ConversationStore>(
    kit: &RoomKit<S>,
    channel_id: &str,
    sender: &str,
    body: &str,
    key: Option<&str>,
) -> InboundResult {
    let mut message = InboundMessage::new(channel(channel_id), sender, TextContent::new(body));
    message.delivery_key = key.map(str::to_owned);
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(message, options).await.unwrap()
}

/// A held message from Alice, then a console note whose sync hook
/// completes her request, goes on with its own work (an await, as an HTTP
/// call, until the release's answer ends or `ITS_WORK` passes), closes the
/// room when `closes`, and lets the note through; returned once the
/// release's answer ended, the agent asked or the answer refused.
pub(crate) async fn a_hook_releases<S: ConversationStore>(store: S, closes: bool) -> Released<S> {
    let asked = Arc::new(Mutex::new(Vec::new()));
    let over = Arc::new(watch::Sender::new(false));
    let kit = kit_on(store, &asked, &over).await;
    let held = send(&kit, "sms", ALICE, "Wire 500 to Bob", Some(KEY))
        .await
        .event
        .unwrap();
    let options = RequestVerificationOptions {
        window_start: Some(held.created_at),
        ..RequestVerificationOptions::default()
    };
    let identity = IdentityId::new("id-alice").unwrap();
    let (request, secret) = kit
        .request_verification(&identity, &room(), &channel("sms"), options)
        .await
        .unwrap();
    let refusals = EventTap::on(&kit, &["room_refused_event"]);
    let ended = Arc::clone(&over);
    kit.on("room_refused_event", move |event| {
        if event.data.get("operation").and_then(Value::as_str) == Some("regenerate") {
            ended.send_replace(true);
        }
        std::future::ready(())
    })
    .unwrap();
    let weak = kit.downgrade();
    let work = Arc::clone(&over);
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let (weak, work) = (weak.clone(), Arc::clone(&work));
            let (request_id, secret) = (request.id.clone(), secret.clone());
            async move {
                if event.source.channel_id != channel("bank") {
                    return HookResult::allow(AllowOptions::default());
                }
                let kit = weak.upgrade().unwrap();
                let options = CompleteVerificationOptions::default();
                kit.complete_verification(&request_id, &secret, options)
                    .await
                    .unwrap();
                let mut answer = work.subscribe();
                drop(tokio::time::timeout(ITS_WORK, answer.wait_for(|ended| *ended)).await);
                if closes {
                    kit.close_room(&room(), CloseRoomOptions::default())
                        .await
                        .unwrap();
                }
                HookResult::allow(AllowOptions::default())
            }
        },
        HookOptions {
            name: "console".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();

    let note = send(&kit, "bank", "advisor", "Verified by phone", None).await;
    let mut answer = over.subscribe();
    tokio::time::timeout(BOUND, answer.wait_for(|ended| *ended))
        .await
        .expect("the release's answer never ended")
        .unwrap();
    tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
        .await
        .expect("the kit's work never ended");
    let asked = asked.lock().unwrap().clone();
    let refused = refusals
        .events()
        .into_iter()
        .filter_map(|event| Some(event.data.get("operation")?.as_str()?.to_owned()))
        .collect();
    Released {
        kit,
        held,
        note,
        asked,
        refused,
    }
}
