//! A binding's recipient as routing reads it, on the tokio runtime (RFC
//! §5.7, §10.4 step 3, §10.1 step 2): no value names no one, while a value
//! under the recipient key that is not a recipient (a boolean, a float, an
//! array, an object) is the address of someone other than the sender. A
//! room the host opened with one admits no routed sender, whatever strategy
//! chose it, keeps the value as written, and refuses its deliveries
//! `no_recipient`.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::fmt::{Debug, Write as _};
use std::future::{Future, ready};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use pretty_assertions::assert_eq;
use roomkit::model::{
    BindingOptions, ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput,
    ChannelType, EventContent, InboundMessage, Metadata, RoomContext, RoomEvent, RoomId, Timestamp,
};
use roomkit::testing::EchoAgent;
use roomkit::{
    Channel, ChannelDeclaration, ChannelError, ConversationStore, DefaultRouter, HookTrigger,
    InboundRoomRouter, RoomKit, RouteQuery, RouterError, SequentialEnvironment, text_of,
};
use serde_json::{Value, json};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::{DefaultGuard, Interest};
use tracing::{Event, Level, Metadata as Callsite, Subscriber};

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

impl Channel for Sms {
    fn declaration(&self) -> ChannelDeclaration {
        ChannelDeclaration::new(
            channel("sms"),
            ChannelType::Sms,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        )
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
}

/// A kit with an SMS number and an agent an `ON_ROOM_CREATED` hook attaches
/// to every room created for a message (RFC Appendix B.1).
fn kit_with_agent() -> (RoomKit, Sms) {
    let kit = RoomKit::new();
    kit.set_environment(SequentialEnvironment::new(
        Timestamp::from_unix_millis(1_792_677_780_000).unwrap(),
    ));
    let sms = Sms::new();
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(EchoAgent::new(channel("ai"), "You said: ").quoting())
        .unwrap();
    let attaching = kit.clone();
    kit.observe("welcome", HookTrigger::OnRoomCreated, move |event, _| {
        let kit = attaching.clone();
        async move {
            kit.attach(&event.room_id, &channel("ai")).await.unwrap();
        }
    })
    .unwrap();
    (kit, sms)
}

/// The room `id`, opened by the host with the agent and the SMS number
/// attached, its SMS binding holding `held` under `phone_number` (nothing
/// when `None`), declared `group` when `group`.
async fn open_room(kit: &RoomKit, id: &str, held: Option<Value>, group: bool) -> RoomId {
    let room_id = room(id);
    kit.create_room(&room_id).await.unwrap();
    kit.attach(&room_id, &channel("ai")).await.unwrap();
    let mut metadata = Metadata::new();
    if let Some(held) = held {
        metadata.insert("phone_number".to_owned(), held);
    }
    let options = BindingOptions {
        group,
        metadata,
        ..BindingOptions::default()
    };
    kit.attach_with(&room_id, &channel("sms"), options)
        .await
        .unwrap();
    room_id
}

fn message(from: &str, text: &str) -> InboundMessage {
    InboundMessage::new(channel("sms"), from, EventContent::text(text))
}

/// Routes `message` and returns the room it landed in.
async fn routed(kit: &RoomKit, message: InboundMessage) -> RoomId {
    let result = kit.process_inbound(message).await.unwrap();
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

impl<S: ConversationStore, R: InboundRoomRouter<S>> InboundRoomRouter<S> for Counted<R> {
    fn route(
        &self,
        store: &S,
        query: &RouteQuery,
    ) -> impl Future<Output = Result<Option<RoomId>, RouterError>> + Send {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.router.route(store, query)
    }
}

/// A custom strategy that routes every message to one room.
struct ToRoom(RoomId);

impl<S: ConversationStore> InboundRoomRouter<S> for ToRoom {
    fn route(
        &self,
        _store: &S,
        _query: &RouteQuery,
    ) -> impl Future<Output = Result<Option<RoomId>, RouterError>> + Send {
        ready(Ok(Some(self.0.clone())))
    }
}

/// The warnings `tracing` emits on this thread while the guard lives, each
/// as its message and fields written out.
#[derive(Clone, Default)]
struct Warnings(Arc<Mutex<Vec<String>>>);

impl Warnings {
    fn capture() -> (Self, DefaultGuard) {
        let warnings = Self::default();
        let guard = tracing::subscriber::set_default(warnings.clone());
        (warnings, guard)
    }

    /// The warnings that contain `text`.
    fn containing(&self, text: &str) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|warning| warning.contains(text))
            .cloned()
            .collect()
    }
}

struct Line<'line>(&'line mut String);

impl Visit for Line<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        write!(self.0, "{}={value:?} ", field.name()).unwrap();
    }
}

impl Subscriber for Warnings {
    fn register_callsite(&self, _callsite: &'static Callsite<'static>) -> Interest {
        Interest::sometimes()
    }

    fn enabled(&self, callsite: &Callsite<'_>) -> bool {
        *callsite.level() == Level::WARN
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut line = String::new();
        event.record(&mut Line(&mut line));
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(line);
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
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
            let (kit, sms) = kit_with_agent();
            let calls = Arc::new(AtomicUsize::new(0));
            if custom {
                let router = ToRoom(room("free"));
                let calls = Arc::clone(&calls);
                kit.set_router(Counted { router, calls });
            } else {
                let router = DefaultRouter;
                let calls = Arc::clone(&calls);
                kit.set_router(Counted { router, calls });
            }
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

/// RFC §5.7: the warning names the value's type, never the value.
#[tokio::test]
async fn the_warning_names_the_type_of_what_is_not_a_recipient() {
    let mut kinds = Vec::new();
    for held in not_recipients() {
        let (kit, _sms) = kit_with_agent();
        open_room(&kit, "free", Some(held), false).await;
        let (warnings, guard) = Warnings::capture();
        routed(&kit, message(BOB, "Hello")).await;
        drop(guard);
        let warned = warnings.containing("not a recipient");
        let kind = warned
            .first()
            .and_then(|warning| warning.split("held=").nth(1))
            .and_then(|rest| rest.split_whitespace().next())
            .map(str::to_owned);
        kinds.push(kind);
    }
    assert_eq!(
        kinds,
        ["boolean", "object", "array", "float", "float"].map(|kind| Some(kind.to_owned()))
    );
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

/// RFC §5.7: routing does not read a `group` binding's recipient. Every
/// sender shares the room, whatever it holds under the key, and routing
/// warns of nothing.
#[tokio::test]
async fn a_group_binding_s_recipient_is_not_read() {
    let mut shared = Vec::new();
    for held in [None, Some(json!(15_550_000_001.0))] {
        let (kit, _sms) = kit_with_agent();
        let team = open_room(&kit, "team", held, true).await;
        let (warnings, guard) = Warnings::capture();
        let rooms = [
            routed(&kit, message(ALICE, "Bonjour")).await,
            routed(&kit, message(BOB, "Hello")).await,
        ];
        drop(guard);
        shared.push((
            rooms == [team.clone(), team.clone()],
            sms_binding(&kit, &team).await.participant_id,
            warnings.containing("not a recipient").len(),
        ));
    }
    assert_eq!(shared, [(true, None, 0), (true, None, 0)]);
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
    let stored = kit.store().list_events(&host_room, true).await.unwrap();
    let bob_in_host_room = stored.iter().any(|event| {
        event
            .source
            .participant_id
            .as_ref()
            .is_some_and(|id| id.as_str() == BOB)
    });
    let warned = warnings.containing("not a recipient");

    let named = kit
        .process_inbound_in(&host_room, message(BOB, "bob: hello"))
        .await
        .unwrap();
    drop(guard);
    let binding = sms_binding(&kit, &host_room).await;
    let answered_in_host_room = kit
        .store()
        .list_events(&host_room, true)
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
