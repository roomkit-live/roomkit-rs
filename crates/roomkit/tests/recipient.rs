//! A binding's recipient as routing reads it, on the tokio runtime (RFC
//! §5.7, §10.4 step 3, §10.1 step 2): no value names no one, while a value
//! under the recipient key that is not a recipient (a boolean, a float, an
//! array, an object) is the address of someone other than the sender. A
//! room the host opened with one admits no routed sender, whatever strategy
//! chose it, keeps the value as written, and refuses its deliveries
//! `no_recipient`.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::collections::BTreeMap;
use std::future::{Future, ready};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    InboundMessage, Metadata, ParticipantId, RoomContext, RoomEvent, RoomId, TextContent,
    Timestamp,
};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
    SequentialEnvironment, kit_with_environment,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ChannelOf, ConversationStore, CreateRoomOptions,
    DefaultInboundRoomRouter, DefaultInboundRoomRouterOptions, HookExecution, HookOptions,
    HookTrigger, InMemoryStore, InboundRoomRouter, ProcessInboundOptions, RoomKit, RoomKitOptions,
    RouteOptions, RouterFuture, SendEventOptions, WeakRoomKit,
};
use roomkit_core::ops::ChannelConfig;
use serde_json::{Value, json};

#[path = "support/warnings.rs"]
mod warnings;

use roomkit_core::rules::transcoding::text_of;
use warnings::Warnings;

const ALICE: &str = "+15550000001";
const BOB: &str = "+15550000002";

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room(id: &str) -> RoomId {
    RoomId::new(id).unwrap()
}

/// What the host may write under the SMS recipient key that is not a
/// recipient: a boolean, an object, an array, a float, and Bob's own
/// number written as a float.
fn not_recipients() -> [Value; 5] {
    [
        json!(true),
        json!({ "number": BOB }),
        json!([BOB]),
        json!(15_550_000_001.0),
        json!(15_550_000_002.0),
    ]
}

/// An SMS transport: it delivers to the address its binding names under
/// `phone_number`, and records where it sent what.
#[derive(Clone)]
struct Sms {
    sent: Arc<Mutex<Vec<(String, String)>>>,
}

impl Sms {
    fn new() -> Self {
        Self {
            sent: Arc::default(),
        }
    }

    /// Where it sent what, in order.
    fn sent(&self) -> Vec<(String, String)> {
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Sms {
    /// The configuration the kit keeps of the channel.
    fn config() -> ChannelConfig {
        ChannelConfig::new(
            channel("sms"),
            ChannelType::SMS,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        )
    }
}

impl Channel for Sms {
    fn channel_id(&self) -> ChannelId {
        Self::config().channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        Self::config().channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        Self::config().category
    }

    fn direction(&self) -> ChannelDirection {
        Self::config().direction
    }

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
        Self::config().capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        Self::config().provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        Self::config().recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        Self::config().supports_streaming_delivery
    }

    // Its addresses as a transport of its type writes them, Python's
    // `TransportChannel` (an `SMSChannel` is one).
    fn normalize_address(&self, address: &str) -> String {
        as_transport(self).normalize_address(address)
    }

    fn recipient_address(&self, binding: &ChannelBinding) -> Option<String> {
        as_transport(self).recipient_address(binding)
    }

    fn recipient_metadata(&self, address: &str) -> BTreeMap<String, String> {
        as_transport(self).recipient_metadata(address)
    }

    fn conversation_address(&self, message: &InboundMessage) -> String {
        as_transport(self).conversation_address(message)
    }

    fn reply_metadata(&self, message: &InboundMessage) -> BTreeMap<String, String> {
        as_transport(self).reply_metadata(message)
    }

    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let to = binding
            .metadata
            .get("phone_number")
            .map(|to| to.as_str().map_or_else(|| to.to_string(), str::to_owned))
            .unwrap_or_default();
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((to, text_of(&event.content)));
        ready(Ok(ChannelOutput::default()))
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
}

/// A kit with an SMS number and an agent an `ON_ROOM_CREATED` hook attaches
/// to every room created for a message (RFC Appendix B.1).
fn kit_with_agent() -> (RoomKit, Sms) {
    kit_with_agent_on(RoomKitOptions::default())
}

/// [`kit_with_agent`], routed by what `wrap` makes of the default strategy
/// over the kit's store and its registered channels.
fn kit_wrapping_the_default(
    wrap: impl FnOnce(DefaultInboundRoomRouter<InMemoryStore>) -> Arc<dyn InboundRoomRouter>,
) -> (RoomKit, Sms) {
    let options = RoomKitOptions::default();
    let built: Arc<OnceLock<WeakRoomKit>> = Arc::default();
    let reads = Arc::clone(&built);
    let channel_of: ChannelOf =
        Arc::new(move |channel_id| reads.get()?.upgrade()?.get_channel(channel_id));
    let default = DefaultInboundRoomRouter::new(
        Arc::clone(&options.store),
        DefaultInboundRoomRouterOptions {
            channel_of: Some(channel_of),
        },
    );
    let (kit, sms) = kit_with_agent_on(RoomKitOptions {
        inbound_router: Some(wrap(default)),
        ..options
    });
    built.set(kit.downgrade()).unwrap();
    (kit, sms)
}

/// [`kit_with_agent`], built with `options`.
fn kit_with_agent_on(options: RoomKitOptions) -> (RoomKit, Sms) {
    let kit = kit_with_environment(
        options,
        SequentialEnvironment::new(Timestamp::from_unix_millis(1_792_677_780_000).unwrap()),
    );
    let sms = Sms::new();
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(EchoAgent::new(
        channel("ai"),
        "You said: ",
        EchoAgentOptions { quote: true },
    ))
    .unwrap();
    let attaching = kit.clone();
    kit.hook(
        HookTrigger::OnRoomCreated,
        move |event: RoomEvent, _| {
            let kit = attaching.clone();
            async move {
                kit.attach_channel(
                    &event.room_id,
                    &channel("ai"),
                    AttachChannelOptions::default(),
                )
                .await
                .unwrap();
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "welcome".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    (kit, sms)
}

/// The room `id`, opened by the host with the agent and the SMS number
/// attached, its SMS binding holding `held` under `phone_number` (nothing
/// when `None`), declared `group` when `group`.
async fn open_room(kit: &RoomKit, id: &str, held: Option<Value>, group: bool) -> RoomId {
    let room_id = room(id);
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room_id, &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();
    let mut metadata = Metadata::new();
    if let Some(held) = held {
        metadata.insert("phone_number".to_owned(), held);
    }
    let options = AttachChannelOptions {
        group,
        metadata,
        ..Default::default()
    };
    kit.attach_channel(&room_id, &channel("sms"), options)
        .await
        .unwrap();
    room_id
}

fn message(from: &str, text: &str) -> InboundMessage {
    InboundMessage::new(channel("sms"), from, TextContent::new(text))
}

/// Routes `message` and returns the room it landed in.
async fn routed(kit: &RoomKit, message: InboundMessage) -> RoomId {
    let result = kit
        .process_inbound(message, ProcessInboundOptions::default())
        .await
        .unwrap();
    result.event.unwrap().room_id
}

/// The room's SMS binding, as stored.
async fn sms_binding(kit: &RoomKit, room_id: &RoomId) -> ChannelBinding {
    kit.store()
        .list_bindings(room_id)
        .await
        .unwrap()
        .into_iter()
        .find(|binding| binding.channel_id == channel("sms"))
        .unwrap()
}

/// A router that counts its calls and asks `R`.
struct Counted<R> {
    router: R,
    calls: Arc<AtomicUsize>,
}

impl<R: InboundRoomRouter> InboundRoomRouter for Counted<R> {
    fn route<'a>(
        &'a self,
        channel_id: &'a ChannelId,
        channel_type: &'a ChannelType,
        options: RouteOptions,
    ) -> RouterFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.router.route(channel_id, channel_type, options)
    }
}

/// A custom strategy that routes every message to one room.
struct ToRoom(RoomId);

impl InboundRoomRouter for ToRoom {
    fn route<'a>(
        &'a self,
        _channel_id: &'a ChannelId,
        _channel_type: &'a ChannelType,
        _options: RouteOptions,
    ) -> RouterFuture<'a> {
        Box::pin(ready(Ok(Some(self.0.clone()))))
    }
}

/// RFC §5.7, §10.4 step 3, §10.1 step 2: a value under the recipient key
/// that is not a recipient is someone else's address. The default strategy
/// admits no one into the room through it, asked once; a strategy that
/// returns the room anyway has it let go under the room lock, and is asked
/// once more. Bob gets a room of his own and is answered there; the
/// binding is left as the host wrote it, and a warning names the binding
/// and the value's type, never the value.
#[tokio::test]
async fn a_room_whose_recipient_is_not_one_admits_no_routed_sender() {
    let mut landed = Vec::new();
    for held in not_recipients() {
        for custom in [false, true] {
            let calls = Arc::new(AtomicUsize::new(0));
            let counted = Arc::clone(&calls);
            let (kit, sms) = kit_wrapping_the_default(move |default| {
                let router: Arc<dyn InboundRoomRouter> = if custom {
                    let router = ToRoom(room("free"));
                    Arc::new(Counted {
                        router,
                        calls: counted,
                    })
                } else {
                    Arc::new(Counted {
                        router: default,
                        calls: counted,
                    })
                };
                router
            });
            let free = open_room(&kit, "free", Some(held.clone()), false).await;
            let (warnings, guard) = Warnings::capture();
            let bobs = routed(&kit, message(BOB, "Hello")).await;
            drop(guard);
            let binding = sms_binding(&kit, &free).await;
            let warned = warnings.containing("not a recipient");
            let named = warned.iter().all(|warning| {
                warning.contains("room_id=free")
                    && warning.contains("channel_id=sms")
                    && !warning.contains("555000000")
            });
            landed.push((
                held.to_string(),
                custom,
                bobs == free,
                calls.load(Ordering::SeqCst),
                binding.participant_id.is_none(),
                binding.metadata.get("phone_number") == Some(&held),
                sms.sent().iter().any(|(to, _)| to == BOB),
                !warned.is_empty() && named,
            ));
        }
    }
    let expected: Vec<_> = not_recipients()
        .iter()
        .flat_map(|held| {
            [false, true].map(|custom| {
                let calls = if custom { 2 } else { 1 };
                (
                    held.to_string(),
                    custom,
                    false,
                    calls,
                    true,
                    true,
                    true,
                    true,
                )
            })
        })
        .collect();
    assert_eq!(landed, expected);
}

/// RFC §5.7: no value under the recipient key (null, an empty or a blank
/// string) names no one: the room's first sender is recorded there, their
/// address its recipient, and the next sender gets a room of their own. The
/// key absent is `routing.rs`'s
/// `a_room_opened_naming_no_one_takes_its_first_sender_only`.
#[tokio::test]
async fn a_room_holding_no_value_under_its_recipient_key_takes_its_first_sender() {
    let mut landed = Vec::new();
    for held in [Value::Null, json!(""), json!("  ")] {
        let (kit, _sms) = kit_with_agent();
        let free = open_room(&kit, "free", Some(held.clone()), false).await;
        let alices = routed(&kit, message(ALICE, "Bonjour")).await;
        let bobs = routed(&kit, message(BOB, "Hello")).await;
        let binding = sms_binding(&kit, &free).await;
        landed.push((
            held.to_string(),
            alices == free,
            bobs == free,
            binding.participant_id.map(String::from),
            binding.metadata.get("phone_number").cloned(),
        ));
    }
    let expected: Vec<_> = [Value::Null, json!(""), json!("  ")]
        .iter()
        .map(|held| {
            (
                held.to_string(),
                true,
                false,
                Some(ALICE.to_owned()),
                Some(json!(ALICE)),
            )
        })
        .collect();
    assert_eq!(landed, expected);
}

/// RFC §5.7, §10.4 steps 1 and 3: routing does not read a `group`
/// binding's recipient, whatever it holds under the key, and warns of
/// nothing. Alone on the channel, the room takes every sender. Beside a
/// room whose binding names Alice, it is not Bob's conversation, his own
/// number under its key included: two rooms bound to the channel, Bob gets
/// a room of his own.
#[tokio::test]
async fn a_group_binding_s_recipient_is_not_read() {
    let mut shared = Vec::new();
    for held in [None, Some(json!(15_550_000_001.0)), Some(json!(BOB))] {
        let (kit, _sms) = kit_with_agent();
        let team = open_room(&kit, "team", held.clone(), true).await;
        let (warnings, guard) = Warnings::capture();
        let alone = [
            routed(&kit, message(ALICE, "Bonjour")).await,
            routed(&kit, message(BOB, "Hello")).await,
        ];
        drop(guard);
        let (kit, _sms) = kit_with_agent();
        let team_beside = open_room(&kit, "team", held.clone(), true).await;
        let desk = room("desk");
        kit.create_room(CreateRoomOptions {
            room_id: Some(desk.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        let alices = AttachChannelOptions {
            participant_id: Some(ParticipantId::new(ALICE).unwrap()),
            ..Default::default()
        };
        kit.attach_channel(&desk, &channel("sms"), alices)
            .await
            .unwrap();
        let (beside_warnings, guard) = Warnings::capture();
        let bobs = routed(&kit, message(BOB, "Hello")).await;
        drop(guard);
        shared.push((
            held.map(|held| held.to_string()),
            alone == [team.clone(), team.clone()],
            bobs != team_beside && bobs != desk,
            warnings.containing("not a recipient").len()
                + beside_warnings.containing("not a recipient").len(),
        ));
    }
    assert_eq!(
        shared,
        [
            (None, true, true, 0),
            (Some("15550000001.0".to_owned()), true, true, 0),
            (Some(format!("\"{BOB}\"")), true, true, 0),
        ]
    );
}

/// End to end, as an integrator meets it (RFC §5.7, §10.4 step 3, §10.2
/// step 3d): the host opens `host_room` with its `phone_number` written as
/// a float, and Bob texts the number. Routed, he gets a room of his own and
/// is answered there; `host_room` stores nothing of his, keeps the field
/// the host wrote, and a warning names its binding. Named by its room id,
/// his message lands in `host_room` as before, and the agent's answer
/// there is refused `no_recipient` rather than sent to anyone.
#[tokio::test]
async fn a_room_whose_recipient_is_not_one_is_no_stranger_s() {
    let (kit, sms) = kit_with_agent();
    let host_room = open_room(&kit, "host_room", Some(json!(15_550_000_001.0)), false).await;
    let (warnings, guard) = Warnings::capture();

    let bobs = routed(&kit, message(BOB, "bob: hello")).await;
    let after_routed = sms.sent().len();
    let stored = kit
        .store()
        .list_events(&host_room, every_row())
        .await
        .unwrap();
    let bob_in_host_room = stored.iter().any(|event| {
        event
            .source
            .participant_id
            .as_ref()
            .is_some_and(|id| id.as_str() == BOB)
    });
    let warned = warnings.containing("not a recipient");

    let named = kit
        .process_inbound(
            message(BOB, "bob: hello"),
            ProcessInboundOptions {
                room_id: Some(host_room.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    drop(guard);
    let binding = sms_binding(&kit, &host_room).await;
    let answered_in_host_room = kit
        .store()
        .list_events(&host_room, every_row())
        .await
        .unwrap()
        .iter()
        .any(|event| event.source.channel_id == channel("ai"));

    assert_eq!(
        (
            bobs == host_room,
            sms.sent()
                .iter()
                .map(|(to, _)| to.clone())
                .collect::<Vec<_>>(),
            after_routed,
            bob_in_host_room,
            warned
                .first()
                .is_some_and(|warning| warning.contains("room_id=host_room")
                    && warning.contains("held=float")),
            named.event.map(|event| event.room_id),
            answered_in_host_room,
            warnings
                .containing("the binding has no recipient")
                .iter()
                .any(|warning| warning.contains("room_id=host_room")),
            binding.participant_id,
            Value::Object(binding.metadata),
        ),
        (
            false,
            vec![BOB.to_owned()],
            1,
            false,
            true,
            Some(host_room.clone()),
            true,
            true,
            None,
            json!({ "phone_number": 15_550_000_001.0 }),
        )
    );
}

/// Every row of a room's timeline, the refused ones included.
/// A kit with no agent, the SMS number and a `bank` channel a host sends from.
fn kit_of_a_bank() -> (RoomKit, Sms) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = Sms::new();
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(RecordingTransport::new(
        channel("bank"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    (kit, sms)
}

/// The room `id` binding `bank` and the SMS number, holding `held` under
/// `phone_number`.
async fn open_bank_room(kit: &RoomKit, id: &str, held: Option<Value>) -> RoomId {
    let room_id = room(id);
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room_id, &channel("bank"), AttachChannelOptions::default())
        .await
        .unwrap();
    let mut metadata = Metadata::new();
    if let Some(held) = held {
        metadata.insert("phone_number".to_owned(), held);
    }
    let options = AttachChannelOptions {
        metadata,
        ..Default::default()
    };
    kit.attach_channel(&room_id, &channel("sms"), options)
        .await
        .unwrap();
    room_id
}

// Python: tests/test_transport_recipient.py::test_no_recipient_is_refused_before_any_send_and_spares_the_breaker
// Six refusals in one room; another room on the same channel still delivers.
#[tokio::test]
async fn no_recipient_is_refused_before_any_send_and_spares_the_breaker() {
    let values = [
        None,
        Some(json!(true)),
        Some(json!("  ")),
        Some(json!({ "number": ALICE })),
        Some(json!(15_550_000_001.0)),
    ];
    for held in values {
        let (kit, sms) = kit_of_a_bank();
        let failed: Arc<Mutex<Vec<Option<Value>>>> = Arc::default();
        let noted = Arc::clone(&failed);
        kit.on("delivery_failed", move |event: roomkit::FrameworkEvent| {
            noted.lock().unwrap().push(event.data.get("error").cloned());
            async {}
        })
        .unwrap();
        let nobody = open_bank_room(&kit, "nobody", held.clone()).await;
        let alice = open_bank_room(&kit, "alice", Some(json!(ALICE))).await;

        for n in 0..6 {
            let notice = TextContent::new(format!("notice {n}"));
            kit.send_event(
                &nobody,
                &channel("bank"),
                notice,
                SendEventOptions::default(),
            )
            .await
            .unwrap();
        }
        assert_eq!(sms.sent(), Vec::new(), "{held:?}");
        assert_eq!(
            failed.lock().unwrap().clone(),
            vec![Some(json!("no_recipient")); 6],
            "{held:?}"
        );

        let appointment = TextContent::new("your appointment");
        kit.send_event(
            &alice,
            &channel("bank"),
            appointment,
            SendEventOptions::default(),
        )
        .await
        .unwrap();
        let to: Vec<String> = sms.sent().into_iter().map(|(to, _)| to).collect();
        assert_eq!(to, [ALICE], "{held:?}");
        kit.close().await.unwrap();
    }
}

// Python: tests/test_transport_recipient.py::test_no_recipient_is_logged_as_a_refusal_without_traceback
// A warning naming the key. A traceback has no Rust form.
#[tokio::test]
async fn no_recipient_is_logged_as_a_refusal() {
    let (kit, _sms) = kit_of_a_bank();
    let nobody = open_bank_room(&kit, "nobody", None).await;
    let (warnings, guard) = Warnings::capture();

    kit.send_event(
        &nobody,
        &channel("bank"),
        TextContent::new("notice"),
        SendEventOptions::default(),
    )
    .await
    .unwrap();
    drop(guard);

    let refused = warnings.containing("no recipient");
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert!(refused[0].starts_with("WARN"), "{refused:?}");
    assert!(refused[0].contains("phone_number"), "{refused:?}");
    assert_eq!(warnings.containing("ERROR"), Vec::<String>::new());
    kit.close().await.unwrap();
}

fn every_row() -> roomkit::store::ListEventsOptions {
    roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::store::ListEventsOptions::default()
    }
}

/// A transport of `channel`'s id and type, which answers its address
/// methods as Python's `TransportChannel` does.
fn as_transport(channel: &impl Channel) -> roomkit::TransportChannel {
    roomkit::TransportChannel::new(
        channel.channel_id(),
        channel.channel_type(),
        roomkit::TransportChannelOptions::default(),
    )
}
