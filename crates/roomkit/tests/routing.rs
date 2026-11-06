//! Messages that name no room, on the tokio runtime (RFC §10.4, §10.1 step
//! 2, §5.7): the default router over `InMemoryStore`, a member found by an
//! address linked to their identity, custom routers, a router that fails,
//! the caller's organization, a chat channel answered in its chat, and two
//! first messages arriving at once, which never get two senders recorded on
//! one binding.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::collections::BTreeMap;
use std::future::{Future, ready};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    ErrorRecord, EventSource, Identity, IdentityId, InboundMessage, Participant, ParticipantId,
    Room, RoomContext, RoomEvent, RoomId, TextContent, Timestamp,
};
use roomkit::testing::{EchoAgent, EchoAgentOptions, SequentialEnvironment, kit_with_environment};
use roomkit::{
    Addressing, AttachChannelOptions, Channel, ChannelError, ChannelOf, CloseRoomOptions,
    ConversationStore, CreateRoomOptions, DefaultInboundRoomRouter,
    DefaultInboundRoomRouterOptions, HookExecution, HookOptions, HookTrigger, InMemoryStore,
    InboundRoomRouter, ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions, RouteOptions,
    RouterError, RouterFuture, WeakRoomKit,
};
use roomkit_core::ops::ChannelConfig;
use roomkit_core::rules::routing::HISTORY_PAGE;
use roomkit_core::rules::transcoding::text_of;
use tokio::sync::{Barrier, Notify};

const ALICE: &str = "+15550000001";
const BOB: &str = "+15550000002";

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room(id: &str) -> RoomId {
    RoomId::new(id).unwrap()
}

/// A transport as a provider is: it delivers to the address its binding
/// names under its declaration's recipient key, and records where it sent
/// what.
#[derive(Clone)]
struct Provider {
    declaration: ChannelConfig,
    sent: Arc<Mutex<Vec<(String, String)>>>,
}

impl Provider {
    fn new(id: &str, channel_type: ChannelType) -> Self {
        Self {
            declaration: ChannelConfig::new(
                channel(id),
                channel_type,
                ChannelCategory::Transport,
                ChannelDirection::Bidirectional,
            ),
            sent: Arc::default(),
        }
    }

    /// A transport of its id, type and declared addressing, which answers
    /// its address methods as Python's `TransportChannel` does: a chat
    /// channel replies to its chat only, any other to its senders.
    fn as_transport(&self) -> roomkit::TransportChannel {
        let config = &self.declaration;
        let addressing = roomkit::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: config.addressing != Addressing::Chat,
        };
        roomkit::TransportChannel::new(
            config.channel_id.clone(),
            config.channel_type.clone(),
            roomkit::TransportChannelOptions {
                addressing: Some(addressing),
                country_code: config.country_code.clone(),
                ..roomkit::TransportChannelOptions::default()
            },
        )
    }

    /// Where it sent what, in order.
    fn sent(&self) -> Vec<(String, String)> {
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Channel for Provider {
    fn channel_id(&self) -> ChannelId {
        self.declaration.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.declaration.channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        self.declaration.category
    }

    fn direction(&self) -> ChannelDirection {
        self.declaration.direction
    }

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
        self.declaration.capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        self.declaration.provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        self.declaration.recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.declaration.supports_streaming_delivery
    }

    // Its addresses as a transport of its declaration writes them, Python's
    // `TransportChannel` (an `SMSChannel` is one).
    fn normalize_address(&self, address: &str) -> String {
        self.as_transport().normalize_address(address)
    }

    fn recipient_address(&self, binding: &ChannelBinding) -> Option<String> {
        self.as_transport().recipient_address(binding)
    }

    fn recipient_metadata(&self, address: &str) -> BTreeMap<String, String> {
        self.as_transport().recipient_metadata(address)
    }

    fn conversation_address(&self, message: &InboundMessage) -> String {
        self.as_transport().conversation_address(message)
    }

    fn reply_metadata(&self, message: &InboundMessage) -> BTreeMap<String, String> {
        self.as_transport().reply_metadata(message)
    }

    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let key = self.declaration.recipient_key.clone().unwrap_or_default();
        let to = binding
            .metadata
            .get(&key)
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
fn kit_with_agent() -> (RoomKit, Provider) {
    kit_with_agent_on(RoomKitOptions::default())
}

/// [`kit_with_agent`], routed by `router`.
fn kit_routed_by(router: Arc<dyn InboundRoomRouter>) -> (RoomKit, Provider) {
    kit_with_agent_on(RoomKitOptions {
        inbound_router: Some(router),
        ..Default::default()
    })
}

/// [`kit_with_agent`], routed by what `wrap` makes of the default strategy
/// over the kit's store and its registered channels, as an integrator's
/// router wrapping it is built.
fn kit_wrapping_the_default(
    wrap: impl FnOnce(DefaultInboundRoomRouter<InMemoryStore>) -> Arc<dyn InboundRoomRouter>,
) -> (RoomKit, Provider) {
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
fn kit_with_agent_on(options: RoomKitOptions) -> (RoomKit, Provider) {
    kit_with_agent_over(options, Provider::new("sms", ChannelType::SMS))
}

/// [`kit_with_agent_on`], its transport `sms` in place of the SMS number.
fn kit_with_agent_over(options: RoomKitOptions, sms: Provider) -> (RoomKit, Provider) {
    let kit = kit_with_environment(
        options,
        SequentialEnvironment::new(Timestamp::from_unix_millis(1_792_677_780_000).unwrap()),
    );
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

/// The correspondent the room's SMS binding names.
async fn correspondent(kit: &RoomKit, room_id: &RoomId) -> Option<String> {
    kit.store()
        .list_bindings(room_id)
        .await
        .unwrap()
        .into_iter()
        .find(|binding| binding.channel_id == channel("sms"))
        .and_then(|binding| binding.participant_id.map(String::from))
}

/// The texts of the room's timeline, its lifecycle records left out (gap
/// G-046).
async fn texts(kit: &RoomKit, room_id: &RoomId) -> Vec<String> {
    kit.store()
        .list_events(room_id, every_row())
        .await
        .unwrap()
        .iter()
        .filter(|event| event.source.channel_type != ChannelType::SYSTEM)
        .map(|event| text_of(&event.content))
        .collect()
}

/// A room the host opens with the SMS number attached at its defaults.
async fn open_room(kit: &RoomKit, id: &str) -> RoomId {
    let room_id = room(id);
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room_id, &channel("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit.attach_channel(&room_id, &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();
    room_id
}

/// RFC Appendix B.1: no room yet for the sender, so one is created, bound to
/// them, and the agent its hook attaches answers at their number.
#[tokio::test]
async fn a_first_message_opens_a_room_bound_to_its_sender_and_is_answered_there() {
    let (kit, sms) = kit_with_agent();
    let landed = routed(&kit, message(ALICE, "Bonjour")).await;
    assert_eq!(
        (
            landed.to_string(),
            correspondent(&kit, &landed).await,
            sms.sent()
        ),
        (
            "new_room_1".to_owned(),
            Some(ALICE.to_owned()),
            vec![(ALICE.to_owned(), "You said: Bonjour".to_owned())]
        )
    );
}

/// The WhatsApp and email cases of the test above: a room opened for a
/// sender names them under the channel's recipient key and answers them
/// there.
// Python: tests/test_transport_recipient.py::test_room_opened_for_a_sender_replies_to_them
#[tokio::test]
async fn a_room_opened_for_a_sender_replies_to_them() {
    let cases = [
        (ChannelType::WHATSAPP, "phone_number", ALICE),
        (ChannelType::EMAIL, "email_address", "a@x.test"),
    ];
    for (channel_type, key, sender) in cases {
        let provider = Provider::new("ch", channel_type.clone());
        let (kit, ch) = kit_with_agent_over(RoomKitOptions::default(), provider);
        let message = InboundMessage::new(channel("ch"), sender, TextContent::new("Hi"));
        let landed = routed(&kit, message).await;
        let binding = kit
            .store()
            .list_bindings(&landed)
            .await
            .unwrap()
            .into_iter()
            .find(|binding| binding.channel_id == channel("ch"))
            .unwrap();
        assert_eq!(
            (
                binding.participant_id.map(String::from),
                binding.metadata.get(key).cloned(),
                ch.sent()
            ),
            (
                Some(sender.to_owned()),
                Some(sender.into()),
                vec![(sender.to_owned(), "You said: Hi".to_owned())]
            ),
            "{channel_type:?}"
        );
        kit.close().await.unwrap();
    }
}

/// RFC §10.4's example: alice and bob on one SMS number each get a room of
/// their own, and alice's next message is routed back to hers.
#[tokio::test]
async fn two_senders_on_one_number_keep_a_conversation_each() {
    let (kit, sms) = kit_with_agent();
    let alices = routed(&kit, message(ALICE, "Bonjour")).await;
    let bobs = routed(&kit, message(BOB, "Hello")).await;
    let again = routed(&kit, message(ALICE, "Still there?")).await;
    assert_eq!(
        (
            again == alices && alices != bobs,
            texts(&kit, &alices).await,
            sms.sent().last().cloned()
        ),
        (
            true,
            [
                "Bonjour",
                "You said: Bonjour",
                "Still there?",
                "You said: Still there?"
            ]
            .map(str::to_owned)
            .to_vec(),
            Some((ALICE.to_owned(), "You said: Still there?".to_owned()))
        )
    );
}

/// RFC §10.4 step 1: a member the host added under their identity is found
/// by an address linked to it, although two rooms are bound to the number.
#[tokio::test]
async fn a_member_is_found_by_an_address_linked_to_their_identity() {
    let (kit, _sms) = kit_with_agent();
    let jeans = open_room(&kit, "jean_room").await;
    open_room(&kit, "other_room").await;
    let mut jean = Participant::new(
        ParticipantId::new("p_jean").unwrap(),
        jeans.clone(),
        channel("sms"),
    );
    jean.identity_id = Some(IdentityId::new("id_jean").unwrap());
    kit.store().add_participant(jean).await.unwrap();
    kit.store()
        .create_identity(Identity::new(IdentityId::new("id_jean").unwrap()))
        .await
        .unwrap();
    kit.store()
        .link_address(
            &IdentityId::new("id_jean").unwrap(),
            &ChannelType::SMS,
            ALICE,
            roomkit::store::LinkAddressOptions::default(),
        )
        .await
        .unwrap();
    let landed = routed(&kit, message("+1 (555) 000-0001", "Bonjour")).await;
    assert_eq!(
        (landed, correspondent(&kit, &jeans).await),
        (jeans, Some(ALICE.to_owned()))
    );
}

/// RFC §5.6: an address moved from one identity to another, an unlink then
/// a link, routes to its new person's room; the rooms are left as they are.
#[tokio::test]
async fn an_address_moved_to_another_identity_routes_to_its_new_person() {
    let (kit, _sms) = kit_with_agent();
    let jeans = open_room(&kit, "jean_room").await;
    let maries = open_room(&kit, "marie_room").await;
    for (room_id, person) in [(&jeans, "jean"), (&maries, "marie")] {
        let identity_id = IdentityId::new(format!("id_{person}")).unwrap();
        let mut member = Participant::new(
            ParticipantId::new(format!("p_{person}")).unwrap(),
            room_id.clone(),
            channel("sms"),
        );
        member.identity_id = Some(identity_id.clone());
        kit.store().add_participant(member).await.unwrap();
        kit.store()
            .create_identity(Identity::new(identity_id))
            .await
            .unwrap();
    }
    let (jean, marie) = (
        IdentityId::new("id_jean").unwrap(),
        IdentityId::new("id_marie").unwrap(),
    );
    let store = kit.store();
    store
        .link_address(
            &jean,
            &ChannelType::SMS,
            ALICE,
            roomkit::store::LinkAddressOptions::default(),
        )
        .await
        .unwrap();
    store
        .unlink_address(
            &jean,
            &ChannelType::SMS,
            ALICE,
            roomkit::store::UnlinkAddressOptions::default(),
        )
        .await
        .unwrap();
    store
        .link_address(
            &marie,
            &ChannelType::SMS,
            ALICE,
            roomkit::store::LinkAddressOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(routed(&kit, message(ALICE, "Bonjour")).await, maries);
}

/// RFC §10.4 step 3: a room opened naming no one admits its first sender,
/// records them, and refuses the next one, who gets a room of their own.
#[tokio::test]
async fn a_room_opened_naming_no_one_takes_its_first_sender_only() {
    let (kit, _sms) = kit_with_agent();
    let opened = open_room(&kit, "support").await;
    let alices = routed(&kit, message(ALICE, "Bonjour")).await;
    let bobs = routed(&kit, message(BOB, "Hello")).await;
    assert_eq!(
        (
            alices == opened,
            correspondent(&kit, &opened).await,
            bobs == opened
        ),
        (true, Some(ALICE.to_owned()), false)
    );
}

/// RFC §17.2: routing reads the caller's organization only; the room it
/// creates is the organization's.
#[tokio::test]
async fn a_message_is_routed_among_its_caller_s_organization_only() {
    let (kit, _sms) = kit_with_agent();
    let banks = room("bank_room");
    let bank = || Some("bank".to_owned());
    let options = CreateRoomOptions {
        room_id: Some(banks.clone()),
        organization_id: bank(),
        ..Default::default()
    };
    kit.create_room(options).await.unwrap();
    let options = AttachChannelOptions {
        organization_id: bank(),
        ..Default::default()
    };
    kit.attach_channel(&banks, &channel("sms"), options)
        .await
        .unwrap();
    let clinic = ProcessInboundOptions {
        organization_id: Some("clinic".to_owned()),
        ..Default::default()
    };
    let result = kit
        .process_inbound(message(ALICE, "Bonjour"), clinic)
        .await
        .unwrap();
    let landed = result.event.unwrap().room_id;
    let organization = kit
        .store()
        .get_room(&landed)
        .await
        .unwrap()
        .and_then(|room| room.organization_id);
    assert_eq!(
        (
            landed == banks,
            organization,
            correspondent(&kit, &banks).await
        ),
        (false, Some("clinic".to_owned()), None)
    );
}

/// Routes every message to the room its metadata names, counting its calls.
#[derive(Default)]
struct ByTicket {
    calls: AtomicUsize,
}

impl InboundRoomRouter for ByTicket {
    fn route<'a>(
        &'a self,
        _channel_id: &'a ChannelId,
        _channel_type: &'a ChannelType,
        options: RouteOptions,
    ) -> RouterFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let ticket = options
            .channel_data
            .as_ref()
            .and_then(|data| data.get("ticket"))
            .and_then(|value| value.as_str());
        let routed = ticket
            .map(RoomId::new)
            .transpose()
            .map_err(RouterError::from);
        Box::pin(ready(routed))
    }
}

fn on_ticket(from: &str, ticket: &str) -> InboundMessage {
    let mut message = message(from, "About my ticket");
    message.metadata.insert("ticket".to_owned(), ticket.into());
    message
}

/// RFC §10.4: a custom strategy; the sender is recorded on the room it
/// returns (RFC §10.1 step 2).
#[tokio::test]
async fn a_custom_router_routes_to_the_room_it_chooses() {
    let (kit, _sms) = kit_routed_by(Arc::new(ByTicket::default()));
    let ticket = open_room(&kit, "ticket_42").await;
    open_room(&kit, "ticket_43").await;
    let landed = routed(&kit, on_ticket(ALICE, "ticket_42")).await;
    assert_eq!(
        (landed, correspondent(&kit, &ticket).await),
        (ticket, Some(ALICE.to_owned()))
    );
}

/// Gap G-055: a custom router that sends a second sender to a room recorded
/// for the first is asked once more, then the message gets a new room.
#[tokio::test]
async fn a_custom_router_s_room_recorded_for_another_sender_gives_the_message_a_new_room() {
    let router = Arc::new(ByTicket::default());
    let (kit, _sms) = kit_routed_by(Arc::<ByTicket>::clone(&router));
    let ticket = open_room(&kit, "ticket_42").await;
    routed(&kit, on_ticket(ALICE, "ticket_42")).await;
    let bobs = routed(&kit, on_ticket(BOB, "ticket_42")).await;
    assert_eq!(
        (
            bobs == ticket,
            correspondent(&kit, &ticket).await,
            correspondent(&kit, &bobs).await,
            router.calls.load(Ordering::SeqCst)
        ),
        (false, Some(ALICE.to_owned()), Some(BOB.to_owned()), 3)
    );
}

/// RFC §10.4: a strategy that sends several senders to one room declares
/// its binding `group`, and nobody is recorded on it.
#[tokio::test]
async fn a_custom_router_s_group_room_takes_every_sender() {
    let (kit, _sms) = kit_routed_by(Arc::new(ByTicket::default()));
    let group = room("group");
    kit.create_room(CreateRoomOptions {
        room_id: Some(group.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    let options = AttachChannelOptions {
        group: true,
        ..Default::default()
    };
    kit.attach_channel(&group, &channel("sms"), options)
        .await
        .unwrap();
    let landed = [
        routed(&kit, on_ticket(ALICE, "group")).await,
        routed(&kit, on_ticket(BOB, "group")).await,
    ];
    assert_eq!(
        (landed, correspondent(&kit, &group).await),
        ([group.clone(), group], None)
    );
}

/// Gap G-055: a room a router returns that no longer accepts the message is
/// not the message's; it is not refused `room_closed`, but routed once
/// more, then given a new room.
#[tokio::test]
async fn a_custom_router_s_closed_room_gives_the_message_a_new_room() {
    let router = Arc::new(ByTicket::default());
    let (kit, _sms) = kit_routed_by(Arc::<ByTicket>::clone(&router));
    let ticket = open_room(&kit, "ticket_42").await;
    kit.close_room(&ticket, CloseRoomOptions::default())
        .await
        .unwrap();
    let result = kit
        .process_inbound(
            on_ticket(ALICE, "ticket_42"),
            ProcessInboundOptions::default(),
        )
        .await
        .unwrap();
    let landed = result.event.map(|event| event.room_id);
    assert_eq!(
        (
            result.blocked,
            landed.is_some_and(|landed| landed != ticket),
            router.calls.load(Ordering::SeqCst)
        ),
        (false, true, 2)
    );
}

/// A router that cannot reach what it routes by.
struct Unreachable;

impl InboundRoomRouter for Unreachable {
    fn route<'a>(
        &'a self,
        _channel_id: &'a ChannelId,
        _channel_type: &'a ChannelType,
        _options: RouteOptions,
    ) -> RouterFuture<'a> {
        Box::pin(ready(Err("the CRM is unreachable".into())))
    }
}

/// RFC §10.4: a failing router fails the message before any write.
#[tokio::test]
async fn a_failing_router_fails_the_message_with_nothing_written() {
    let (kit, sms) = kit_routed_by(Arc::new(Unreachable));
    let failed = kit
        .process_inbound(message(ALICE, "Bonjour"), ProcessInboundOptions::default())
        .await;
    let rooms = kit
        .store()
        .list_rooms(roomkit::store::ListRoomsOptions::default())
        .await
        .unwrap();
    assert_eq!(
        (failed, rooms.len(), sms.sent().len()),
        (
            Err(RoomKitError::Router(ErrorRecord::new(
                "RouterError",
                "the CRM is unreachable"
            ))),
            0,
            0
        )
    );
}

/// A channel of a type the RFC does not list, `slack`, whose declaration
/// addresses it by chat, as Teams is: its chat under `channel`, delivered to
/// `slack_channel_id`.
fn slack() -> Provider {
    let mut slack = Provider::new("slack", ChannelType::new("slack").unwrap());
    slack.declaration.addressing = Addressing::Chat;
    slack.declaration.chat_key = Some("channel".to_owned());
    slack.declaration.recipient_key = Some("slack_channel_id".to_owned());
    slack
}

/// RFC §5.7: on a chat channel the chat is the conversation. A room opened
/// for a chat message answers in the chat, and the chat's next message,
/// whoever writes it, lands in the same room: on Telegram, which the RFC
/// addresses by chat, and on a channel whose declaration does (docs/rfc-gaps.md,
/// "Found by step 4's shared SQLite file").
#[tokio::test]
async fn a_chat_is_answered_in_its_chat_and_keeps_its_room() {
    let mut answered = Vec::new();
    for (chat_channel, chat_key) in [
        (Provider::new("telegram", ChannelType::TELEGRAM), "chat_id"),
        (slack(), "channel"),
    ] {
        let kit = RoomKit::new(RoomKitOptions::default());
        let channel_id = chat_channel.declaration.channel_id.clone();
        kit.register_channel(chat_channel.clone()).unwrap();
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
        let in_the_chat = |author: &str, text: &str| {
            let mut message =
                InboundMessage::new(channel_id.clone(), author, TextContent::new(text));
            message
                .metadata
                .insert(chat_key.to_owned(), (-100_777).into());
            message
        };
        let first = routed(&kit, in_the_chat("4242", "Hi")).await;
        let second = routed(&kit, in_the_chat("4343", "Hello")).await;
        // Without its chat, a message names no conversation: not its
        // author's, whose replies go to the chat.
        let unchatted = InboundMessage::new(channel_id.clone(), "4242", TextContent::new("Hey"));
        let third = routed(&kit, unchatted).await;
        answered.push((first == second, first == third, chat_channel.sent()));
    }
    let in_the_chat = (
        true,
        false,
        vec![
            ("-100777".to_owned(), "You said: Hi".to_owned()),
            ("-100777".to_owned(), "You said: Hello".to_owned()),
        ],
    );
    assert_eq!(answered, [in_the_chat.clone(), in_the_chat]);
}

/// RFC §5.7: an integer names the address its digits write. A room the
/// host opened with the chat id `7`, written as an integer or as a string,
/// is the chat `"7"`'s: its message lands there, the binding records the
/// chat as a string, and the answer goes to the chat.
#[tokio::test]
async fn a_room_the_host_opened_for_a_chat_id_is_the_chat_s() {
    let mut landed = Vec::new();
    for held in [7.into(), "7".into()] {
        let kit = RoomKit::new(RoomKitOptions::default());
        let telegram = Provider::new("tg", ChannelType::TELEGRAM);
        kit.register_channel(telegram.clone()).unwrap();
        kit.register_channel(EchoAgent::new(
            channel("ai"),
            "You said: ",
            EchoAgentOptions { quote: true },
        ))
        .unwrap();
        kit.create_room(CreateRoomOptions {
            room_id: Some(room("chat-room")),
            ..Default::default()
        })
        .await
        .unwrap();
        kit.attach_channel(
            &room("chat-room"),
            &channel("ai"),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
        let mut options = AttachChannelOptions::default();
        options.metadata.insert("telegram_chat_id".to_owned(), held);
        kit.attach_channel(&room("chat-room"), &channel("tg"), options)
            .await
            .unwrap();
        let mut message = InboundMessage::new(channel("tg"), "7", TextContent::new("Hi"));
        message.metadata.insert("chat_id".to_owned(), "7".into());
        let landed_in = routed(&kit, message).await;
        let recorded = kit
            .store()
            .list_bindings(&landed_in)
            .await
            .unwrap()
            .into_iter()
            .find(|binding| binding.channel_id == channel("tg"))
            .and_then(|binding| binding.participant_id.map(String::from));
        landed.push((landed_in.to_string(), recorded, telegram.sent()));
    }
    let expected = (
        "chat-room".to_owned(),
        Some("7".to_owned()),
        vec![("7".to_owned(), "You said: Hi".to_owned())],
    );
    assert_eq!(landed, [expected.clone(), expected]);
}

/// Two tasks on a multi-threaded runtime send the first messages of two
/// senders on one number at the same instant, with no room yet: each opens
/// a room of its own, bound to its sender.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_first_messages_at_once_open_a_room_each() {
    let (kit, _sms) = kit_with_agent();
    let start = Arc::new(Barrier::new(2));
    let tasks = [ALICE, BOB].map(|sender| {
        let (kit, start) = (kit.clone(), Arc::clone(&start));
        tokio::spawn(async move {
            start.wait().await;
            routed(&kit, message(sender, "Hi")).await
        })
    });
    let mut landed = Vec::new();
    for task in tasks {
        landed.push(task.await.unwrap());
    }
    let named = [
        correspondent(&kit, &landed[0]).await,
        correspondent(&kit, &landed[1]).await,
    ];
    assert_eq!(
        (landed[0] != landed[1], named),
        (true, [Some(ALICE.to_owned()), Some(BOB.to_owned())])
    );
}

/// The default router, whose first two answers wait for each other: both
/// messages are routed before either is recorded.
struct BothRoutedFirst {
    default: DefaultInboundRoomRouter<InMemoryStore>,
    gate: Barrier,
    calls: AtomicUsize,
}

impl InboundRoomRouter for BothRoutedFirst {
    fn route<'a>(
        &'a self,
        channel_id: &'a ChannelId,
        channel_type: &'a ChannelType,
        options: RouteOptions,
    ) -> RouterFuture<'a> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let answer = self.default.route(channel_id, channel_type, options).await;
            if call < 2 {
                self.gate.wait().await;
            }
            answer
        })
    }
}

/// RFC §10.4: two first messages of two senders, arriving at once on a
/// number whose one room names no one, are both routed to it; the first to
/// take the room's lock is recorded, and the second, finding the binding
/// naming the first, is routed once more, into a room of its own. Never are
/// two senders recorded on one binding.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_first_messages_into_one_open_room_at_once_record_one_and_route_the_other_again() {
    let router = OnceLock::new();
    let (kit, _sms) = kit_wrapping_the_default(|default| {
        let wrapped = Arc::new(BothRoutedFirst {
            default,
            gate: Barrier::new(2),
            calls: AtomicUsize::new(0),
        });
        router.set(Arc::clone(&wrapped)).ok();
        wrapped
    });
    let router = router.get().unwrap();
    let opened = open_room(&kit, "support").await;
    let tasks = [ALICE, BOB].map(|sender| {
        let kit = kit.clone();
        tokio::spawn(async move { (sender, routed(&kit, message(sender, "Hi")).await) })
    });
    let mut landed = Vec::new();
    for task in tasks {
        landed.push(task.await.unwrap());
    }
    let recorded = correspondent(&kit, &opened).await.unwrap();
    let (first, second) = if landed[0].0 == recorded {
        (&landed[0], &landed[1])
    } else {
        (&landed[1], &landed[0])
    };
    assert_eq!(
        (
            first.1 == opened,
            second.1 == opened,
            correspondent(&kit, &second.1).await.as_deref(),
            texts(&kit, &opened).await.len(),
            router.calls.load(Ordering::SeqCst)
        ),
        (true, false, Some(second.0), 2, 3)
    );
}

/// The default router, whose first answer is held until the test lets it
/// go: another message can reach the room it chose before this one is
/// recorded there.
struct HeldFirst {
    default: DefaultInboundRoomRouter<InMemoryStore>,
    routed: Notify,
    go: Notify,
    calls: AtomicUsize,
}

impl InboundRoomRouter for HeldFirst {
    fn route<'a>(
        &'a self,
        channel_id: &'a ChannelId,
        channel_type: &'a ChannelType,
        options: RouteOptions,
    ) -> RouterFuture<'a> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let answer = self.default.route(channel_id, channel_type, options).await;
            if call == 0 {
                self.routed.notify_one();
                self.go.notified().await;
            }
            answer
        })
    }
}

/// RFC §10.4 step 3, gap G-055: alice's first message is admitted into the
/// one room bound to the number, whose binding names no one; before she is
/// recorded there, bob's message reaches the room by its id and is
/// committed on the number. By the lock the room has heard bob, so alice is
/// not recorded on its binding, which would send bob's answers to her: she
/// is routed once more, into a room of her own.
#[tokio::test]
async fn a_room_that_heard_another_sender_before_the_record_routes_the_message_once_more() {
    let router = OnceLock::new();
    let (kit, sms) = kit_wrapping_the_default(|default| {
        let wrapped = Arc::new(HeldFirst {
            default,
            routed: Notify::new(),
            go: Notify::new(),
            calls: AtomicUsize::new(0),
        });
        router.set(Arc::clone(&wrapped)).ok();
        wrapped
    });
    let router = router.get().unwrap();
    let opened = open_room(&kit, "support").await;
    let alices = {
        let kit = kit.clone();
        tokio::spawn(async move { routed(&kit, message(ALICE, "Bonjour")).await })
    };
    router.routed.notified().await;
    kit.process_inbound(
        message(BOB, "Hello"),
        ProcessInboundOptions {
            room_id: Some(opened.clone()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    router.go.notify_one();
    let landed = alices.await.unwrap();
    assert_eq!(
        (
            landed == opened,
            correspondent(&kit, &opened).await,
            correspondent(&kit, &landed).await,
            sms.sent()
                .iter()
                .any(|(to, text)| to == ALICE && text.contains("Hello")),
            router.calls.load(Ordering::SeqCst)
        ),
        (false, None, Some(ALICE.to_owned()), false, 2)
    );
}

/// Every row of a room's timeline, the refused ones included.
// Python: tests/test_inbound_routing_ambiguity.py::TestStepThreeAdmitsOneConversation::test_another_sender_found_past_the_first_page_refuses
// The history the default router reads goes past its first page.
#[tokio::test]
async fn another_sender_found_past_the_first_page_refuses() {
    let store = Arc::new(InMemoryStore::default());
    let room_id = room("r1");
    store.create_room(Room::new(room_id.clone())).await.unwrap();
    let binding = ChannelBinding::new(channel("ws"), room_id.clone(), ChannelType::WEBSOCKET);
    store.add_binding(binding).await.unwrap();
    let said_by = |sender: &str| {
        let source = EventSource {
            participant_id: Some(ParticipantId::new(sender).unwrap()),
            ..EventSource::new(channel("ws"), ChannelType::WEBSOCKET)
        };
        RoomEvent::new(room_id.clone(), source, TextContent::new("hi"))
    };
    for _ in 0..=HISTORY_PAGE {
        store
            .add_event_auto_index(&room_id, said_by("bob"))
            .await
            .unwrap();
    }
    let route = |sender: &str| {
        let router = DefaultInboundRoomRouter::new(
            Arc::clone(&store),
            DefaultInboundRoomRouterOptions::default(),
        );
        let options = RouteOptions {
            participant_id: Some(sender.to_owned()),
            ..RouteOptions::default()
        };
        async move {
            router
                .route(&channel("ws"), &ChannelType::WEBSOCKET, options)
                .await
                .unwrap()
        }
    };
    assert_eq!(route("bob").await, Some(room_id.clone()));

    store
        .add_event_auto_index(&room_id, said_by("alice"))
        .await
        .unwrap();

    assert_eq!(route("bob").await, None);
}

/// A kit answering on `provider`: its rooms take an agent as they open.
fn kit_answering_on(provider: &Provider) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(provider.clone()).unwrap();
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
    kit
}

// Python: tests/test_chat_conversations.py::test_a_chat_message_without_its_chat_has_no_address
#[tokio::test]
async fn a_chat_message_without_its_chat_has_no_address() {
    for carried in [
        None,
        Some(serde_json::json!(true)),
        Some(serde_json::json!("  ")),
    ] {
        let telegram = Provider::new("telegram", ChannelType::TELEGRAM);
        let kit = kit_answering_on(&telegram);
        let written = |key_value: Option<serde_json::Value>, text: &str| {
            let mut message =
                InboundMessage::new(channel("telegram"), "4242", TextContent::new(text));
            if let Some(value) = key_value {
                message.metadata.insert("chat_id".to_owned(), value);
            }
            message
        };
        let private = routed(&kit, written(Some(4242.into()), "hi")).await;
        let sent = telegram.sent().len();

        let lost = kit
            .process_inbound(
                written(carried.clone(), "lost"),
                ProcessInboundOptions::default(),
            )
            .await
            .unwrap();

        let event = lost.event.unwrap();
        assert_eq!(
            event
                .source
                .participant_id
                .as_ref()
                .map(ParticipantId::as_str),
            Some("4242"),
            "{carried:?}"
        );
        assert_ne!(event.room_id, private, "{carried:?}");
        let options = roomkit::GetBindingOptions::default();
        let binding = kit
            .get_binding(&event.room_id, &channel("telegram"), options)
            .await
            .unwrap();
        assert_eq!(
            binding.participant_id.as_ref().map(ParticipantId::as_str),
            Some("system"),
            "{carried:?}"
        );
        assert!(binding.metadata.is_empty(), "{carried:?}");
        assert_eq!(telegram.sent().len(), sent, "{carried:?}");
        kit.close().await.unwrap();
    }
}

// Python: tests/test_chat_conversations.py::test_whatsapp_personal_answers_a_private_chat_by_number_and_a_group_in_the_group
#[tokio::test]
async fn whatsapp_personal_answers_a_private_chat_by_number_and_a_group_in_the_group() {
    let whatsapp = Provider::new("wa", ChannelType::WHATSAPP_PERSONAL);
    let kit = kit_answering_on(&whatsapp);
    let write = |sender: &'static str, group: &'static str| {
        let (kit, whatsapp) = (kit.clone(), whatsapp.clone());
        async move {
            let mut message = InboundMessage::new(channel("wa"), sender, TextContent::new("hi"));
            message
                .metadata
                .insert("group_jid".to_owned(), group.into());
            let room = routed(&kit, message).await;
            let last = whatsapp.sent().last().cloned().unwrap();
            (room, last.0)
        }
    };
    let (private, to_private) = write(ALICE, "").await;
    let (alice_group, to_alice_group) = write(ALICE, "120363@g.us").await;
    let (bob_group, to_bob_group) = write(BOB, "120363@g.us").await;

    assert_eq!(to_private, ALICE);
    assert_eq!(to_alice_group, "120363@g.us");
    assert_eq!(to_bob_group, "120363@g.us");
    assert_eq!(alice_group, bob_group);
    assert_ne!(alice_group, private);
    kit.close().await.unwrap();
}

// Python: tests/test_shared_channel_routing.py::TestEitherNameFindsTheNewestRoom::test_an_older_room_found_by_the_address_does_not_hide_a_newer_one
#[tokio::test]
async fn an_older_room_found_by_the_address_does_not_hide_a_newer_one() {
    let (kit, _sms) = kit_with_agent();
    let id_alice = IdentityId::new("id-alice").unwrap();
    kit.store()
        .create_identity(Identity::new(id_alice.clone()))
        .await
        .unwrap();
    kit.store()
        .link_address(
            &id_alice,
            &ChannelType::SMS,
            ALICE,
            roomkit::store::LinkAddressOptions::default(),
        )
        .await
        .unwrap();
    for (id, named) in [("by-address", ALICE), ("by-identity", "id-alice")] {
        let room_id = room(id);
        kit.create_room(CreateRoomOptions {
            room_id: Some(room_id.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        let options = AttachChannelOptions {
            participant_id: Some(ParticipantId::new(named).unwrap()),
            ..Default::default()
        };
        kit.attach_channel(&room_id, &channel("sms"), options)
            .await
            .unwrap();
    }

    let landed = routed(&kit, message(ALICE, "Bonjour")).await;

    assert_eq!(landed, room("by-identity"));
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

/// A router recording who it is told of each message.
struct Told(Arc<Mutex<Vec<Option<String>>>>);

impl InboundRoomRouter for Told {
    fn route<'a>(
        &'a self,
        _channel_id: &'a ChannelId,
        _channel_type: &'a ChannelType,
        options: RouteOptions,
    ) -> RouterFuture<'a> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(options.participant_id);
        Box::pin(async { Ok(None) })
    }
}

// Python: tests/test_shared_channel_routing.py::TestAMessageWithNoAddress::test_a_router_is_told_no_address_as_none
#[tokio::test]
async fn a_router_is_told_no_address_as_none() {
    let told = Arc::new(Mutex::new(Vec::new()));
    let (kit, _) = kit_with_agent_on(RoomKitOptions {
        inbound_router: Some(Arc::new(Told(Arc::clone(&told)))),
        ..RoomKitOptions::default()
    });
    for sender in ["", "  ", "bob"] {
        kit.process_inbound(
            InboundMessage::new(channel("sms"), sender, TextContent::new("x")),
            ProcessInboundOptions::default(),
        )
        .await
        .unwrap();
    }
    kit.close().await.unwrap();
    assert_eq!(
        *told.lock().unwrap_or_else(PoisonError::into_inner),
        [None, None, Some("bob".to_owned())]
    );
}

/// RFC §10.1 step 2, §10.4: the default router admits Bob at step 3 to the
/// room Alice's first message created, a hook having attached the number to
/// it while her creation hooks run; once they are over her message leaves
/// the room, which stays Bob's, and nothing of hers is stored there or sent
/// to his number (RMK-1396). The hook attaches, then waits, in that order: a
/// conformance scenario's hooks run at once.
// Python: tests/test_shared_channel_routing.py::TestARoomBeingCreated::test_a_sender_step_three_admits_meanwhile_keeps_the_room
#[tokio::test]
async fn a_sender_step_three_admits_meanwhile_keeps_the_room() {
    let (kit, sms) = kit_with_agent();
    let rooms: Arc<Mutex<Vec<RoomId>>> = Arc::default();
    let (held, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let hook_kit = kit.clone();
    let (hook_rooms, hook_held, hook_release) =
        (Arc::clone(&rooms), Arc::clone(&held), Arc::clone(&release));
    kit.hook(
        HookTrigger::OnRoomCreated,
        move |event: RoomEvent, _| {
            let (kit, rooms) = (hook_kit.clone(), Arc::clone(&hook_rooms));
            let (held, release) = (Arc::clone(&hook_held), Arc::clone(&hook_release));
            async move {
                kit.attach_channel(
                    &event.room_id,
                    &channel("sms"),
                    AttachChannelOptions::default(),
                )
                .await
                .unwrap();
                let first = {
                    let mut rooms = rooms.lock().unwrap_or_else(PoisonError::into_inner);
                    rooms.push(event.room_id.clone());
                    rooms.len() == 1
                };
                if first {
                    held.notify_one();
                    release.notified().await;
                }
            }
        },
        HookOptions {
            name: "bind_then_hold".to_owned(),
            ..HookOptions::default()
        },
    )
    .unwrap();
    let alice = {
        let kit = kit.clone();
        tokio::spawn(async move { routed(&kit, message(ALICE, "alice: my secret")).await })
    };
    held.notified().await;
    let bobs = routed(&kit, message(BOB, "bob: my secret")).await;
    release.notify_one();
    let alices = alice.await.unwrap();
    let first = rooms.lock().unwrap_or_else(PoisonError::into_inner)[0].clone();
    assert_eq!(bobs, first);
    assert_ne!(alices, first);
    assert_eq!(correspondent(&kit, &first).await, Some(BOB.to_owned()));
    assert_eq!(correspondent(&kit, &alices).await, Some(ALICE.to_owned()));
    let in_bobs = texts(&kit, &first).await;
    assert!(
        in_bobs.iter().all(|text| !text.contains("alice")),
        "{in_bobs:?}"
    );
    let to_bob = sms
        .sent
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    assert!(
        to_bob
            .iter()
            .all(|(to, text)| to != BOB || !text.contains("alice")),
        "{to_bob:?}"
    );
}
