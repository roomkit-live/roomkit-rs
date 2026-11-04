//! `RoomKit::post`, the host's own voice in a room (RFC §10.5, Appendix
//! A.14.1, RMK-947): the host channel, a transport of type `HOST` that
//! brings the host's words in and is never delivered to, attached on a
//! room's first post; who a post speaks as; what it solicits; what refuses
//! it, with nothing written; and its key.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    Access, AgentResponsePolicy, ChannelBinding, ChannelCategory, ChannelDirection, ChannelId,
    ChannelMediaType, ChannelOutput, ChannelType, EventContent, EventDirection, EventId,
    EventSource, HookResult, Identity, IdentityId, InboundMessage, Metadata, Observation,
    Participant, ParticipantId, Room, RoomContext, RoomEvent, RoomId, Task, TextContent, Timestamp,
};
use roomkit::store;
use roomkit::testing::EventTap;
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CloseRoomOptions, CommitEventOptions,
    ConversationStore, CreateRoomOptions, DetachChannelOptions, EventFilter, HookOptions,
    HookTrigger, HostChannel, InMemoryStore, PostOptions, ProcessInboundOptions, RoomKit,
    RoomKitError, RoomKitOptions, SendEventOptions, StoreError,
};
use roomkit_core::ops::ChannelConfig;
use roomkit_core::rules::transcoding::text_of;
use tokio::sync::Notify;

const CUSTOMER: &str = "+15550100001";

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

/// A `process_timeout` no test outlasts.
const UNBOUNDED: Duration = Duration::from_secs(3600);

fn room_id() -> RoomId {
    RoomId::new("r").unwrap()
}

fn text(body: &str) -> EventContent {
    TextContent::new(body).into()
}

/// An SMS provider: it delivers to the binding's `phone_number`, and
/// records where it sent what.
#[derive(Clone, Default)]
struct Sms {
    sent: Arc<Mutex<Vec<(String, String)>>>,
}

impl Sms {
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

    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let to = binding
            .metadata
            .get("phone_number")
            .and_then(|to| to.as_str())
            .unwrap_or_default()
            .to_owned();
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

/// An agent that answers `reply` to every event it is asked, and keeps the
/// events it was asked.
#[derive(Clone)]
struct Agent {
    id: ChannelId,
    reply: &'static str,
    asked: Arc<Mutex<Vec<RoomEvent>>>,
}

impl Agent {
    fn new(id: &str, reply: &'static str) -> Self {
        Self {
            id: channel(id),
            reply,
            asked: Arc::default(),
        }
    }

    fn asked(&self) -> Vec<RoomEvent> {
        self.asked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Agent {
    /// The configuration the kit keeps of the channel.
    fn config(&self) -> ChannelConfig {
        ChannelConfig::new(
            self.id.clone(),
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        )
    }
}

impl Channel for Agent {
    fn channel_id(&self) -> ChannelId {
        self.config().channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.config().channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        self.config().category
    }

    fn direction(&self) -> ChannelDirection {
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

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        self.asked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(event.clone());
        let answer = RoomEvent {
            id: event.id.clone(),
            created_at: event.created_at,
            ..RoomEvent::new(
                event.room_id.clone(),
                EventSource {
                    direction: EventDirection::Outbound,
                    ..EventSource::new(self.id.clone(), ChannelType::AI)
                },
                text(self.reply),
            )
        };
        ready(Ok(ChannelOutput {
            responded: true,
            response_events: vec![answer],
            ..ChannelOutput::empty()
        }))
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl std::future::Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
}

/// A host channel that counts what it is asked to deliver: a host channel
/// is never delivered to (RFC §10.5).
#[derive(Clone)]
struct CountingHost {
    declaration: ChannelConfig,
    delivered: Arc<Mutex<usize>>,
}

impl CountingHost {
    fn new(id: &str) -> Self {
        Self {
            declaration: ChannelConfig::host(channel(id)),
            delivered: Arc::default(),
        }
    }

    fn delivered(&self) -> usize {
        *self
            .delivered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl Channel for CountingHost {
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

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        *self
            .delivered
            .lock()
            .unwrap_or_else(PoisonError::into_inner) += 1;
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

/// Room `r`, of `organization` when given: the customer on SMS, its binding
/// naming their number.
async fn sms_room<S: ConversationStore>(
    kit: &RoomKit<S>,
    organization: Option<&str>,
    options: CreateRoomOptions<S>,
) -> Sms {
    let sms = Sms::default();
    kit.register_channel(sms.clone()).unwrap();
    let organization_id = organization.map(str::to_owned);
    let options = CreateRoomOptions {
        room_id: Some(room_id()),
        organization_id: organization_id.clone(),
        ..options
    };
    kit.create_room(options).await.unwrap();
    let binding = AttachChannelOptions {
        organization_id,
        metadata: Metadata::from_iter([("phone_number".to_owned(), CUSTOMER.into())]),
        ..Default::default()
    };
    kit.attach_channel(&room_id(), &channel("sms"), binding)
        .await
        .unwrap();
    sms
}

/// What the room holds: its events, refused ones included, its bindings
/// and the room itself.
async fn written<S: ConversationStore>(kit: &RoomKit<S>) -> String {
    let events = kit
        .store()
        .list_events(&room_id(), every_row())
        .await
        .unwrap();
    let bindings = kit.store().list_bindings(&room_id()).await.unwrap();
    let room = kit.store().get_room(&room_id()).await.unwrap();
    format!("{events:?}\n{bindings:?}\n{room:?}")
}

/// The room's binding of `channel_id`.
async fn binding_of<S: ConversationStore>(
    kit: &RoomKit<S>,
    channel_id: &str,
) -> Option<ChannelBinding> {
    kit.store()
        .list_bindings(&room_id())
        .await
        .unwrap()
        .into_iter()
        .find(|binding| binding.channel_id == channel(channel_id))
}

/// A participant `id` of room `r` the room reached through `channel_id`.
fn participant(id: &str, channel_id: &str) -> Participant {
    Participant::new(
        ParticipantId::new(id).unwrap(),
        room_id(),
        channel(channel_id),
    )
}

/// RFC §10.5: a post reaches the room's correspondent as a party's
/// message, `INBOUND` from the host channel, asks no agent, keeps the
/// metadata it came with; the host channel attached on the first post
/// declares every media type. A post names no provider, as Python's
/// `HostChannel` has none (decision 20).
#[tokio::test]
async fn a_post_texts_the_correspondent_and_solicits_no_agent() {
    for organization in [None, Some("acme")] {
        let kit = RoomKit::new(RoomKitOptions::default());
        let sms = sms_room(&kit, organization, CreateRoomOptions::default()).await;
        let agent = Agent::new("assistant", "should not answer");
        kit.register_channel(agent.clone()).unwrap();
        kit.attach_channel(
            &room_id(),
            &channel("assistant"),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
        let post = PostOptions {
            metadata: Metadata::from_iter([("job".to_owned(), "shipping-notice".into())]),
            organization_id: organization.map(str::to_owned),
            ..Default::default()
        };
        let event = kit
            .post(&room_id(), text("Your order shipped."), post)
            .await
            .unwrap();
        roomkit::testing::idle(&kit).await;
        let host = binding_of(&kit, "host").await.unwrap();
        assert_eq!(
            (
                sms.sent(),
                agent.asked().len(),
                (
                    event.source.channel_type,
                    event.source.direction,
                    event.source.provider.clone()
                ),
                event.metadata.get("job").cloned(),
                host.capabilities.media_types.len(),
                host.capabilities
                    .media_types
                    .contains(&ChannelMediaType::Location),
            ),
            (
                vec![(CUSTOMER.to_owned(), "Your order shipped.".to_owned())],
                0,
                (ChannelType::HOST, EventDirection::Inbound, None),
                Some("shipping-notice".into()),
                7,
                true
            ),
            "organization {organization:?}"
        );
    }
}

/// RFC §10.5, §19.3: a post addressed to an agent asks it, and speaks as
/// the participant the host channel it comes from reaches; the agent's
/// answer reaches the customer.
#[tokio::test]
async fn a_post_addressed_to_an_agent_is_answered_and_names_its_speaker() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = sms_room(&kit, None, CreateRoomOptions::default()).await;
    let agent = Agent::new("assistant", "It left the warehouse today.");
    kit.register_channel(agent.clone()).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("assistant"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let host = CountingHost::new("desk-host");
    kit.register_channel(host.clone()).unwrap();
    kit.store()
        .add_participant(participant("desk", "desk-host"))
        .await
        .unwrap();
    let post = PostOptions {
        participant_id: Some(ParticipantId::new("desk").unwrap()),
        addressed_to: vec![channel("assistant")],
        channel_id: Some(channel("desk-host")),
        ..Default::default()
    };
    kit.post(
        &room_id(),
        text("Tell the customer where the order is."),
        post,
    )
    .await
    .unwrap();
    roomkit::testing::idle(&kit).await;
    let asked = agent.asked();
    assert_eq!(
        (
            asked.len(),
            asked[0]
                .source
                .participant_id
                .as_ref()
                .map(ToString::to_string),
            sms.sent().last().cloned(),
            host.delivered()
        ),
        (
            1,
            Some("desk".to_owned()),
            Some((
                CUSTOMER.to_owned(),
                "It left the warehouse today.".to_owned()
            )),
            0
        )
    );
}

/// RFC §10.5: the first post's attach is one section under the room lock:
/// concurrent first posts attach the host channel once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_first_posts_attach_the_host_channel_once() {
    let kit = RoomKit::new(RoomKitOptions::default());
    sms_room(&kit, None, CreateRoomOptions::default()).await;
    let tap = EventTap::on(&kit, &["room_channel_attached"]);
    let posts = (0..5).map(|n| {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.post(
                &room_id(),
                text(&format!("Notice {n}.")),
                PostOptions::default(),
            )
            .await
        })
    });
    for post in posts.collect::<Vec<_>>() {
        post.await.unwrap().unwrap();
    }
    roomkit::testing::idle(&kit).await;
    let attached = tap
        .events()
        .iter()
        .filter(|event| {
            event.r#type == "room_channel_attached"
                && event
                    .channel_id
                    .as_ref()
                    .is_some_and(|id| id.as_str() == "host")
        })
        .count();
    assert_eq!(
        (attached, binding_of(&kit, "host").await.is_some()),
        (1, true)
    );
}

/// RFC §10.5: a binding of the host channel the integrator made, before
/// the first post or concurrently with it, is kept as it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_host_binding_the_integrator_sets_keeps_its_settings() {
    for concurrently in [false, true] {
        let kit = RoomKit::new(RoomKitOptions::default());
        sms_room(&kit, None, CreateRoomOptions::default()).await;
        kit.register_channel(HostChannel::default()).unwrap();
        let mut options = AttachChannelOptions {
            access: Access::WriteOnly,
            ..Default::default()
        };
        options.metadata.insert("desk".to_owned(), "1".into());
        let attaching = {
            let kit = kit.clone();
            async move {
                kit.attach_channel(&room_id(), &channel("host"), options)
                    .await
            }
        };
        if concurrently {
            let room = room_id();
            let (attached, posted) = tokio::join!(
                attaching,
                kit.post(&room, text("Notice."), PostOptions::default())
            );
            attached.unwrap();
            posted.unwrap();
        } else {
            attaching.await.unwrap();
            kit.post(&room_id(), text("Notice."), PostOptions::default())
                .await
                .unwrap();
        }
        let host = binding_of(&kit, "host").await.unwrap();
        assert_eq!(
            (host.access, host.metadata.get("desk").cloned()),
            (Access::WriteOnly, Some("1".into())),
            "concurrently {concurrently}"
        );
    }
}

/// RFC §10.5, §13.6: the first post's wait for the room lock runs within
/// the call's `process_timeout`: past it, the post is refused
/// `process_timeout`, its framework event emitted, nothing written.
#[tokio::test]
async fn a_first_post_waits_for_the_room_lock_within_process_timeout() {
    let (kit, sms) = gated_room(Duration::from_millis(50)).await;
    // A record's commit holds the room lock while its read of the room
    // waits at the gate, bounded by no `process_timeout`.
    kit.store().read.arm();
    let holder = {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.commit_event(
                &room_id(),
                record("A record."),
                CommitEventOptions::default(),
            )
            .await
        })
    };
    kit.store().read.reached.notified().await;
    let before = written(&kit).await;
    let tap = EventTap::on(&kit, &["process_timeout"]);
    let refused = kit
        .post(&room_id(), text("Notice."), PostOptions::default())
        .await;
    let after = written(&kit).await;
    kit.store().read.open.notify_one();
    holder.await.unwrap().unwrap();
    roomkit::testing::idle(&kit).await;
    let timed_out = tap.events().len();
    assert_eq!(
        (refused, after == before, timed_out, sms.sent().len()),
        (
            Err(RoomKitError::ProcessTimeout { room_id: room_id() }),
            true,
            1,
            0
        )
    );
}

/// A record of the SMS channel saying `body`, for `commit_event`.
fn record(body: &str) -> RoomEvent {
    RoomEvent {
        id: EventId::new("record_1").unwrap(),
        created_at: Timestamp::UNIX_EPOCH,
        ..RoomEvent::new(
            room_id(),
            EventSource::new(channel("sms"), ChannelType::SMS),
            text(body),
        )
    }
}

/// RFC §10.5, §19.3: the host's words are a person's message: a post
/// addressing no one asks no agent of an `ADDRESSED_ONLY` room, one
/// addressing `a` asks `a`, and the host channel's unaddressed event asks
/// every agent, where an agent's output would ask none.
#[tokio::test]
async fn the_host_s_words_are_a_person_s_message_to_the_room_s_routing() {
    let mut turns = Vec::new();
    for speak in ["post", "post to a", "unaddressed"] {
        let kit = RoomKit::new(RoomKitOptions::default());
        let options = CreateRoomOptions {
            agent_response_policy: Some(AgentResponsePolicy::AddressedOnly),
            ..Default::default()
        };
        let sms = sms_room(&kit, None, options).await;
        let (a, b) = (Agent::new("a", "A says."), Agent::new("b", "B says."));
        for agent in [&a, &b] {
            kit.register_channel(agent.clone()).unwrap();
            kit.attach_channel(&room_id(), &agent.id, AttachChannelOptions::default())
                .await
                .unwrap();
        }
        if speak == "unaddressed" {
            kit.post(
                &room_id(),
                text("Your order shipped."),
                PostOptions::default(),
            )
            .await
            .unwrap();
            let unaddressed = SendEventOptions::default();
            kit.send_event(
                &room_id(),
                &channel("host"),
                text("Any question?"),
                unaddressed,
            )
            .await
            .unwrap();
        } else {
            let mut post = PostOptions::default();
            if speak == "post to a" {
                post.addressed_to = vec![channel("a")];
            }
            kit.post(&room_id(), text("Any question?"), post)
                .await
                .unwrap();
        }
        roomkit::testing::idle(&kit).await;
        let texted = sms
            .sent()
            .contains(&(CUSTOMER.to_owned(), "Any question?".to_owned()));
        turns.push((a.asked().len(), b.asked().len(), texted));
    }
    assert_eq!(turns, [(0, 0, true), (1, 0, true), (1, 1, true)]);
}

/// What sets the room up before a refused post.
#[derive(Clone, Copy, Debug)]
enum Setup {
    Nothing,
    DetachAfterAPost,
    Close,
    CloseAfterAPost,
    TakeTheHostId,
    CustomerOnSms,
    AnAgent,
}

async fn set_up(kit: &RoomKit, setup: Setup) {
    let acme = || Some("acme".to_owned());
    let notice = || async {
        let options = PostOptions {
            idempotency_key: Some("notice".to_owned()),
            organization_id: acme(),
            ..Default::default()
        };
        kit.post(&room_id(), text("An earlier notice."), options)
            .await
            .unwrap();
    };
    let close = || async {
        let options = CloseRoomOptions {
            organization_id: acme(),
        };
        kit.close_room(&room_id(), options).await.unwrap();
    };
    match setup {
        Setup::Nothing => {}
        Setup::DetachAfterAPost => {
            notice().await;
            let options = DetachChannelOptions {
                organization_id: acme(),
            };
            kit.detach_channel(&room_id(), &channel("host"), options)
                .await
                .unwrap();
        }
        Setup::Close => close().await,
        Setup::CloseAfterAPost => {
            notice().await;
            close().await;
        }
        Setup::TakeTheHostId => {
            kit.register_channel(Agent::new("host", "unused")).unwrap();
        }
        Setup::CustomerOnSms => {
            kit.store()
                .add_participant(participant(CUSTOMER, "sms"))
                .await
                .unwrap();
        }
        Setup::AnAgent => {
            kit.register_channel(Agent::new("assistant", "unused"))
                .unwrap();
            let options = AttachChannelOptions {
                organization_id: acme(),
                ..Default::default()
            };
            kit.attach_channel(&room_id(), &channel("assistant"), options)
                .await
                .unwrap();
        }
    }
}

/// What a refused post changes of its options, the organization it is made
/// for among them.
type Arguments = fn(&mut PostOptions);

/// The posts refused before anything is written, with what sets each one
/// up and its error (RFC §10.5).
fn refusals() -> Vec<(&'static str, Setup, Arguments, RoomKitError)> {
    let closed = || RoomKitError::RoomClosed { room_id: room_id() };
    let unbound = || RoomKitError::ChannelNotFound {
        room_id: room_id(),
        channel_id: channel("host"),
    };
    let not_found = || RoomKitError::RoomNotFound(room_id());
    let unreached = |speaker: &str, reached_elsewhere| RoomKitError::ParticipantNotFound {
        room_id: room_id(),
        participant_id: ParticipantId::new(speaker).unwrap(),
        channel_id: Some(channel("host")),
        reached_elsewhere,
    };
    let not_a_host = |id: &str| {
        RoomKitError::Value(format!(
            "post() speaks from a channel of type HOST; '{id}' is not one"
        ))
    };
    vec![
        (
            "a channel of another type",
            Setup::Nothing,
            |post| post.channel_id = Some(channel("sms")),
            not_a_host("sms"),
        ),
        (
            "an unregistered channel",
            Setup::Nothing,
            |post| post.channel_id = Some(channel("ghost")),
            RoomKitError::ChannelNotRegistered {
                channel_id: channel("ghost"),
                message: "Channel ghost not registered".to_owned(),
            },
        ),
        (
            "host taken by another type",
            Setup::TakeTheHostId,
            |_| {},
            not_a_host("host"),
        ),
        ("a closed room", Setup::Close, |_| {}, closed()),
        (
            "a detached host channel",
            Setup::DetachAfterAPost,
            |_| {},
            unbound(),
        ),
        (
            "a replay into a room closed since",
            Setup::CloseAfterAPost,
            |post| post.idempotency_key = Some("notice".to_owned()),
            closed(),
        ),
        (
            "a replay after a detach",
            Setup::DetachAfterAPost,
            |post| post.idempotency_key = Some("notice".to_owned()),
            unbound(),
        ),
        (
            "another organization's room",
            Setup::Nothing,
            |post| post.organization_id = Some("other".to_owned()),
            not_found(),
        ),
        (
            "an unknown speaker",
            Setup::Nothing,
            |post| post.participant_id = Some(ParticipantId::new("ghost").unwrap()),
            unreached("ghost", false),
        ),
        (
            "a speaker reached through SMS only",
            Setup::CustomerOnSms,
            |post| post.participant_id = Some(ParticipantId::new(CUSTOMER).unwrap()),
            unreached(CUSTOMER, true),
        ),
        (
            "an agent's channel id as the speaker",
            Setup::AnAgent,
            |post| post.participant_id = Some(ParticipantId::new("assistant").unwrap()),
            unreached("assistant", false),
        ),
        (
            "an unknown speaker in another organization's room",
            Setup::Nothing,
            |post| {
                post.participant_id = Some(ParticipantId::new("ghost").unwrap());
                post.organization_id = Some("other".to_owned());
            },
            not_found(),
        ),
    ]
}

/// RFC §10.5: a refused post writes nothing: no binding, no event, the
/// room as it was, a detached channel's revocation kept. The room's status
/// and binding are read before the key, so a replay is refused alike.
#[tokio::test]
async fn a_refused_post_writes_nothing() {
    for (case, setup, arguments, expected) in refusals() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let sms = sms_room(&kit, Some("acme"), CreateRoomOptions::default()).await;
        set_up(&kit, setup).await;
        let (before, texted) = (written(&kit).await, sms.sent());
        let mut post = PostOptions {
            organization_id: Some("acme".to_owned()),
            ..Default::default()
        };
        arguments(&mut post);
        let posted = kit.post(&room_id(), text("Never sent."), post).await;
        roomkit::testing::idle(&kit).await;
        let error = match posted {
            Err(error) => error,
            Ok(event) => panic!("{case}: posted {event:?}"),
        };
        assert_eq!(
            (error, written(&kit).await == before, sms.sent() == texted),
            (expected, true, true),
            "{case}"
        );
    }
}

/// RFC §10.5: the refusal of a speaker says why.
#[tokio::test]
async fn a_refused_speaker_is_told_why() {
    let kit = RoomKit::new(RoomKitOptions::default());
    sms_room(&kit, None, CreateRoomOptions::default()).await;
    set_up(&kit, Setup::CustomerOnSms).await;
    let mut told = Vec::new();
    for speaker in ["ghost", CUSTOMER] {
        let post = PostOptions {
            participant_id: Some(ParticipantId::new(speaker).unwrap()),
            ..Default::default()
        };
        told.push(
            kit.post(&room_id(), text("Never sent."), post)
                .await
                .unwrap_err()
                .to_string(),
        );
    }
    let refused = |speaker: &str, why: &str| {
        format!(
            "post() speaks as a participant of room r the host channel 'host' reaches, and \
             '{speaker}' {why}. The host's own speakers are members added on that channel: \
             kit.add_member(room_id, 'host', speaker_id)"
        )
    };
    assert_eq!(
        told,
        [
            refused("ghost", "names nobody of the room"),
            refused(
                CUSTOMER,
                "names a participant reached through other channels only"
            )
        ]
    );
}

/// RFC §10.5, §5.5: a post speaks as a participant its host channel
/// reaches, by their id or their identity's, an SMS member added on the
/// host channel too among them, or as nobody: the post is sent, its source
/// the id it was given.
#[tokio::test]
async fn a_post_speaks_as_a_participant_its_host_channel_reaches() {
    let mut spoken = Vec::new();
    for speaker in ["desk", "id-desk", CUSTOMER, ""] {
        let kit = RoomKit::new(RoomKitOptions::default());
        let sms = sms_room(&kit, None, CreateRoomOptions::default()).await;
        let mut desk = participant("desk", "host");
        desk.identity_id = Some(IdentityId::new("id-desk").unwrap());
        let mut customer = participant(CUSTOMER, "sms");
        customer.connected_via.push(channel("host"));
        for member in [desk, customer] {
            kit.store().add_participant(member).await.unwrap();
        }
        let post = PostOptions {
            participant_id: (!speaker.is_empty()).then(|| ParticipantId::new(speaker).unwrap()),
            ..Default::default()
        };
        let event = kit
            .post(&room_id(), text("Your order shipped."), post)
            .await
            .unwrap();
        roomkit::testing::idle(&kit).await;
        spoken.push((
            event.source.participant_id.map(String::from),
            sms.sent().len(),
        ));
    }
    let speaking = |id: &str| (Some(id.to_owned()), 1);
    assert_eq!(
        spoken,
        [
            speaking("desk"),
            speaking("id-desk"),
            speaking(CUSTOMER),
            (None, 1)
        ]
    );
}

/// RFC §10.1 step 1, §10.5: the host channel receives no external payload:
/// a message on it is refused at the door, named or routed, no room
/// opened, nothing attached.
#[tokio::test]
async fn a_message_on_the_host_channel_is_refused_before_anything_is_written() {
    let kit = RoomKit::new(RoomKitOptions::default());
    sms_room(&kit, None, CreateRoomOptions::default()).await;
    kit.post(&room_id(), text("Notice."), PostOptions::default())
        .await
        .unwrap();
    roomkit::testing::idle(&kit).await;
    let before = written(&kit).await;
    let message = || InboundMessage::new(channel("host"), "x", text("hi"));
    let named = kit
        .process_inbound(
            message(),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await;
    let routed = kit
        .process_inbound(message(), ProcessInboundOptions::default())
        .await;
    let rooms = kit
        .store()
        .list_rooms(store::ListRoomsOptions::default())
        .await
        .unwrap();
    let refused = || {
        Err(RoomKitError::Value(
            "Channel 'host' is a host channel and receives no external payload: the host \
             speaks through RoomKit.post() or send_event()"
                .to_owned(),
        ))
    };
    assert_eq!(
        (named, routed, rooms.len(), written(&kit).await == before),
        (refused(), refused(), 1, true)
    );
}

/// A channel `id` of type `channel_type`, as `category` and `direction`.
struct Claiming(&'static str, ChannelType, ChannelCategory, ChannelDirection);

impl Claiming {
    /// The configuration the kit keeps of the channel.
    fn config(&self) -> ChannelConfig {
        ChannelConfig::new(channel(self.0), self.1.clone(), self.2, self.3)
    }
}

impl Channel for Claiming {
    fn channel_id(&self) -> ChannelId {
        self.config().channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.config().channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        self.config().category
    }

    fn direction(&self) -> ChannelDirection {
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

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl std::future::Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
}

/// RFC §5.3: `system` is the framework's own records' type, which no
/// channel claims: its registration is refused, as a `ValueError`. A
/// channel of type `host` registers whatever its category and direction,
/// as the Python implementation registers it (step 5, point 4, probe
/// `tasks/probes/step5-lot2/host_type_registration.py`).
#[tokio::test]
async fn only_a_channel_claiming_the_system_type_is_refused() {
    use ChannelCategory::{Intelligence, Transport};
    use ChannelDirection::{Bidirectional, Inbound};
    let kit = RoomKit::new(RoomKitOptions::default());
    let claims = [
        Claiming("host_agent", ChannelType::HOST, Intelligence, Inbound),
        Claiming("host_both", ChannelType::HOST, Transport, Bidirectional),
        Claiming("system", ChannelType::SYSTEM, Transport, Inbound),
    ];
    let registered: Vec<Result<(), RoomKitError>> = claims
        .into_iter()
        .map(|claim| kit.register_channel(claim))
        .collect();
    assert_eq!(
        (
            registered,
            kit.register_channel(HostChannel::new(roomkit::HostChannelOptions {
                channel_id: channel("desk")
            }))
        ),
        (
            vec![
                Ok(()),
                Ok(()),
                Err(RoomKitError::Value(
                    "channel system claims the reserved type system".to_owned()
                ))
            ],
            Ok(())
        )
    );
}

/// RFC §6.2, §10.5: the host's words never come from an intelligence
/// channel: a host channel attached as one, by the category the attach
/// names, is refused, nothing bound.
#[tokio::test]
async fn a_host_channel_is_never_bound_as_an_intelligence_channel() {
    let kit = RoomKit::new(RoomKitOptions::default());
    sms_room(&kit, None, CreateRoomOptions::default()).await;
    kit.register_channel(HostChannel::default()).unwrap();
    let options = AttachChannelOptions {
        category: Some(ChannelCategory::Intelligence),
        ..Default::default()
    };
    let attached = kit
        .attach_channel(&room_id(), &channel("host"), options)
        .await;
    assert_eq!(
        (attached.err(), binding_of(&kit, "host").await.is_none()),
        (
            Some(RoomKitError::Value(
                "Channel 'host' is a host channel: it can be attached as a transport only, \
                 never as an intelligence channel"
                    .to_owned()
            )),
            true
        )
    );
}

/// RFC §13.4, §10.5: a key the host channel holds is a replay that
/// returns the original, sending nothing again; a blank key is no key.
#[tokio::test]
async fn a_post_replayed_with_its_key_is_not_texted_again() {
    let mut replays = Vec::new();
    for (key, texts) in [("notice-1042", 1), ("", 2), ("   ", 2)] {
        let kit = RoomKit::new(RoomKitOptions::default());
        let sms = sms_room(&kit, None, CreateRoomOptions::default()).await;
        let post = || PostOptions {
            idempotency_key: Some(key.to_owned()),
            ..Default::default()
        };
        let shipped = || text("Your order shipped.");
        let first = kit.post(&room_id(), shipped(), post()).await.unwrap();
        let again = kit.post(&room_id(), shipped(), post()).await.unwrap();
        roomkit::testing::idle(&kit).await;
        replays.push((
            (sms.sent().len(), again.id == first.id),
            (texts, texts == 1),
        ));
    }
    for (got, expected) in replays {
        assert_eq!(got, expected);
    }
}

/// RFC §13.4, §9.5.1: a hook posts with one key more times than its
/// cascade's budget holds passes, then posts anew: the first post attaches
/// the host channel from within the room's work, each replay returns the
/// original, read before the budget, and the new post still has its pass.
#[tokio::test]
async fn a_post_replayed_from_the_room_s_work_draws_no_pass() {
    for needs_lock in [true, false] {
        let kit = RoomKit::new(RoomKitOptions {
            max_chain_depth: 1,
            ..Default::default()
        });
        sms_room(&kit, None, CreateRoomOptions::default()).await;
        let posted = Arc::new(Mutex::new(Vec::new()));
        let (hook_kit, hook_posted) = (kit.clone(), Arc::clone(&posted));
        let framework = EventTap::on(&kit, &["room_channel_attached"]);
        let ack = HookOptions {
            name: "ack".to_owned(),
            needs_lock,
            ..Default::default()
        };
        kit.hook(
            HookTrigger::BeforeBroadcast,
            move |event: RoomEvent, _| {
                let (kit, posted) = (hook_kit.clone(), Arc::clone(&hook_posted));
                async move {
                    if event.source.channel_id == channel("sms") {
                        for _ in 0..12 {
                            let ack = PostOptions {
                                idempotency_key: Some("ack".to_owned()),
                                ..Default::default()
                            };
                            let event = kit.post(&room_id(), text("Received."), ack).await?;
                            posted.lock().unwrap().push(event.id);
                        }
                        let event = kit
                            .post(
                                &room_id(),
                                text("We answer within the hour."),
                                PostOptions::default(),
                            )
                            .await?;
                        posted.lock().unwrap().push(event.id);
                    }
                    Ok(HookResult::allow(AllowOptions::default()))
                }
            },
            ack,
        )
        .unwrap();
        let message = InboundMessage::new(channel("sms"), CUSTOMER, text("Hello?"));
        tokio::time::timeout(
            Duration::from_secs(5),
            kit.process_inbound(
                message,
                ProcessInboundOptions {
                    room_id: Some(room_id()),
                    ..Default::default()
                },
            ),
        )
        .await
        .unwrap()
        .unwrap();
        roomkit::testing::idle(&kit).await;
        let posted = posted.lock().unwrap().clone();
        let host: Vec<(String, Option<String>)> = kit
            .store()
            .list_events(&room_id(), every_row())
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.source.channel_id == channel("host"))
            .map(|event| (text_of(&event.content), event.blocked_by))
            .collect();
        roomkit::testing::idle(&kit).await;
        let attaches = framework
            .events()
            .iter()
            .filter(|event| {
                event.r#type == "room_channel_attached"
                    && event
                        .channel_id
                        .as_ref()
                        .is_some_and(|id| id.as_str() == "host")
            })
            .count();
        let replayed_the_first = posted[..12].iter().all(|id| *id == posted[0]);
        assert_eq!(
            (replayed_the_first, host, attaches),
            (
                true,
                vec![
                    ("Received.".to_owned(), None),
                    ("We answer within the hour.".to_owned(), None)
                ],
                1
            ),
            "needs_lock {needs_lock}"
        );
    }
}

/// A gate a store's call waits at once armed, as a slow database holds it.
#[derive(Debug, Default)]
struct Gate {
    armed: AtomicBool,
    reached: Notify,
    open: Notify,
}

impl Gate {
    /// Holds the next call that reaches the gate until it opens.
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    async fn pass(&self) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.reached.notify_one();
            self.open.notified().await;
        }
    }
}

/// An in-memory store whose binding writes of the host channel, and whose
/// room reads, wait at a gate once armed.
#[derive(Debug, Default)]
struct GatedStore {
    store: InMemoryStore,
    binding: Gate,
    read: Gate,
}

impl ConversationStore for GatedStore {
    async fn create_room(&self, room: Room) -> Result<Room, StoreError> {
        self.store.create_room(room).await
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        self.store.get_room(room_id).await
    }

    async fn update_room(&self, room: Room) -> Result<Room, StoreError> {
        self.store.update_room(room).await
    }

    async fn patch_room_metadata(
        &self,
        room_id: &RoomId,
        patch: Metadata,
        options: store::PatchRoomMetadataOptions,
    ) -> Result<Option<Room>, StoreError> {
        self.store
            .patch_room_metadata(room_id, patch, options)
            .await
    }

    async fn delete_room(&self, room_id: &RoomId) -> Result<bool, StoreError> {
        self.store.delete_room(room_id).await
    }

    async fn list_rooms(&self, options: store::ListRoomsOptions) -> Result<Vec<Room>, StoreError> {
        self.store.list_rooms(options).await
    }

    async fn find_rooms(&self, options: store::FindRoomsOptions) -> Result<Vec<Room>, StoreError> {
        self.store.find_rooms(options).await
    }

    async fn find_room_ids_by_channel(
        &self,
        channel_id: &ChannelId,
        options: store::FindRoomIdsByChannelOptions,
    ) -> Result<Vec<RoomId>, StoreError> {
        self.store
            .find_room_ids_by_channel(channel_id, options)
            .await
    }

    async fn find_room_id_by_binding(
        &self,
        channel_id: &ChannelId,
        participant_id: &str,
        options: store::FindRoomIdByBindingOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        self.store
            .find_room_id_by_binding(channel_id, participant_id, options)
            .await
    }

    async fn find_room_id_by_participant(
        &self,
        channel_id: &ChannelId,
        participant_id: &str,
        options: store::FindRoomIdByParticipantOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        self.store
            .find_room_id_by_participant(channel_id, participant_id, options)
            .await
    }

    async fn get_delivered_index(&self, room_id: &RoomId) -> Result<i64, StoreError> {
        self.store.get_delivered_index(room_id).await
    }

    async fn advance_delivered_index(
        &self,
        room_id: &RoomId,
        index: u64,
        options: store::AdvanceDeliveredIndexOptions,
    ) -> Result<bool, StoreError> {
        self.store
            .advance_delivered_index(room_id, index, options)
            .await
    }

    async fn get_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
    ) -> Result<Option<Metadata>, StoreError> {
        self.store.get_private_state(room_id, key).await
    }

    async fn set_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
        value: Option<Metadata>,
    ) -> Result<(), StoreError> {
        self.store.set_private_state(room_id, key, value).await
    }

    async fn add_binding(&self, binding: ChannelBinding) -> Result<ChannelBinding, StoreError> {
        if binding.channel_id.as_str() == "host" {
            self.binding.pass().await;
        }
        self.store.add_binding(binding).await
    }

    async fn get_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<Option<ChannelBinding>, StoreError> {
        self.store.get_binding(room_id, channel_id).await
    }

    async fn update_binding(&self, binding: ChannelBinding) -> Result<ChannelBinding, StoreError> {
        if binding.channel_id.as_str() == "host" {
            self.binding.pass().await;
        }
        self.store.update_binding(binding).await
    }

    async fn remove_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<bool, StoreError> {
        self.store.remove_binding(room_id, channel_id).await
    }

    async fn list_bindings(&self, room_id: &RoomId) -> Result<Vec<ChannelBinding>, StoreError> {
        self.store.list_bindings(room_id).await
    }

    async fn load_room_context(
        &self,
        room_id: &RoomId,
    ) -> Result<(Option<Room>, Vec<ChannelBinding>, Vec<Participant>), StoreError> {
        self.read.pass().await;
        self.store.load_room_context(room_id).await
    }

    async fn add_participant(&self, participant: Participant) -> Result<Participant, StoreError> {
        self.store.add_participant(participant).await
    }

    async fn get_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<Option<Participant>, StoreError> {
        self.store.get_participant(room_id, participant_id).await
    }

    async fn update_participant(
        &self,
        participant: Participant,
    ) -> Result<Participant, StoreError> {
        self.store.update_participant(participant).await
    }

    async fn delete_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<(), StoreError> {
        self.store.delete_participant(room_id, participant_id).await
    }

    async fn list_participants(&self, room_id: &RoomId) -> Result<Vec<Participant>, StoreError> {
        self.store.list_participants(room_id).await
    }

    async fn create_identity(&self, identity: Identity) -> Result<Identity, StoreError> {
        self.store.create_identity(identity).await
    }

    async fn get_identity(&self, identity_id: &IdentityId) -> Result<Option<Identity>, StoreError> {
        self.store.get_identity(identity_id).await
    }

    async fn resolve_identity(
        &self,
        channel_type: &ChannelType,
        address: &str,
        options: store::ResolveIdentityOptions,
    ) -> Result<Option<Identity>, StoreError> {
        self.store
            .resolve_identity(channel_type, address, options)
            .await
    }

    async fn link_address(
        &self,
        identity_id: &IdentityId,
        channel_type: &ChannelType,
        address: &str,
        options: store::LinkAddressOptions,
    ) -> Result<(), StoreError> {
        self.store
            .link_address(identity_id, channel_type, address, options)
            .await
    }

    async fn unlink_address(
        &self,
        identity_id: &IdentityId,
        channel_type: &ChannelType,
        address: &str,
        options: store::UnlinkAddressOptions,
    ) -> Result<(), StoreError> {
        self.store
            .unlink_address(identity_id, channel_type, address, options)
            .await
    }

    async fn check_idempotency(
        &self,
        room_id: &RoomId,
        key: &str,
        channel_id: Option<&ChannelId>,
    ) -> Result<bool, StoreError> {
        self.store.check_idempotency(room_id, key, channel_id).await
    }

    async fn get_event_by_idempotency_key(
        &self,
        room_id: &RoomId,
        key: &str,
        channel_id: Option<&ChannelId>,
    ) -> Result<Option<RoomEvent>, StoreError> {
        self.store
            .get_event_by_idempotency_key(room_id, key, channel_id)
            .await
    }

    async fn commit_event(
        &self,
        room_id: &RoomId,
        event: RoomEvent,
        options: store::CommitEventOptions,
    ) -> Result<RoomEvent, StoreError> {
        self.store.commit_event(room_id, event, options).await
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Option<RoomEvent>, StoreError> {
        self.store.get_event(event_id).await
    }

    async fn update_event(&self, event: RoomEvent) -> Result<RoomEvent, StoreError> {
        self.store.update_event(event).await
    }

    async fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        options: store::DeleteEventOptions,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.store.delete_event(room_id, event_id, options).await
    }

    async fn get_event_count(
        &self,
        room_id: &RoomId,
        options: store::GetEventCountOptions,
    ) -> Result<u64, StoreError> {
        self.store.get_event_count(room_id, options).await
    }

    async fn list_events(
        &self,
        room_id: &RoomId,
        options: store::ListEventsOptions,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.store.list_events(room_id, options).await
    }

    async fn add_task(&self, task: Task) -> Result<Task, StoreError> {
        self.store.add_task(task).await
    }

    async fn add_observation(&self, observation: Observation) -> Result<Observation, StoreError> {
        self.store.add_observation(observation).await
    }

    async fn list_tasks(
        &self,
        room_id: &RoomId,
        options: store::ListTasksOptions,
    ) -> Result<Vec<Task>, StoreError> {
        self.store.list_tasks(room_id, options).await
    }

    async fn list_observations(&self, room_id: &RoomId) -> Result<Vec<Observation>, StoreError> {
        self.store.list_observations(room_id).await
    }

    async fn get_task(&self, task_id: &roomkit::model::TaskId) -> Result<Option<Task>, StoreError> {
        self.store.get_task(task_id).await
    }

    async fn update_task(&self, task: Task) -> Result<Task, StoreError> {
        self.store.update_task(task).await
    }

    async fn add_event(&self, event: RoomEvent) -> Result<RoomEvent, StoreError> {
        self.store.add_event(event).await
    }

    async fn mark_read(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        event_id: &EventId,
    ) -> Result<(), StoreError> {
        self.store.mark_read(room_id, channel_id, event_id).await
    }

    async fn mark_all_read(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<(), StoreError> {
        self.store.mark_all_read(room_id, channel_id).await
    }

    async fn get_unread_count(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<u64, StoreError> {
        self.store.get_unread_count(room_id, channel_id).await
    }

    async fn list_read_markers(
        &self,
        room_id: &RoomId,
    ) -> Result<std::collections::BTreeMap<ChannelId, u64>, StoreError> {
        self.store.list_read_markers(room_id).await
    }

    async fn find_latest_room(
        &self,
        participant_id: &ParticipantId,
        options: store::FindLatestRoomOptions,
    ) -> Result<Option<Room>, StoreError> {
        self.store.find_latest_room(participant_id, options).await
    }

    async fn find_room_id_by_channel(
        &self,
        channel_id: &ChannelId,
        options: store::FindRoomIdByChannelOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        self.store
            .find_room_id_by_channel(channel_id, options)
            .await
    }

    async fn get_thread_summaries(
        &self,
        room_id: &RoomId,
        root_event_ids: &[EventId],
    ) -> Result<std::collections::BTreeMap<EventId, roomkit::model::ThreadSummary>, StoreError>
    {
        self.store
            .get_thread_summaries(room_id, root_event_ids)
            .await
    }
}

/// A kit on a gated store, with room `r` and its SMS, its calls bounded by
/// `process_timeout`.
async fn gated_room(process_timeout: Duration) -> (RoomKit<GatedStore>, Sms) {
    let kit = RoomKit::new(RoomKitOptions {
        process_timeout,
        ..RoomKitOptions::with_store(GatedStore::default())
    });
    let sms = sms_room(&kit, None, CreateRoomOptions::default()).await;
    (kit, sms)
}

/// How many times the host channel was attached, as the kit reported it,
/// once its background work is over.
async fn host_attaches<S: ConversationStore>(kit: &RoomKit<S>, tap: &EventTap) -> usize {
    roomkit::testing::idle(kit).await;
    tap.events()
        .iter()
        .filter(|event| {
            event.r#type == "room_channel_attached"
                && event
                    .channel_id
                    .as_ref()
                    .is_some_and(|id| id.as_str() == "host")
        })
        .count()
}

/// The room's messages from the host channel.
async fn host_messages<S: ConversationStore>(kit: &RoomKit<S>) -> usize {
    kit.store()
        .list_events(&room_id(), every_row())
        .await
        .unwrap()
        .iter()
        .filter(|event| event.source.channel_id.as_str() == "host")
        .count()
}

/// RFC §10.5, §13.6: one `process_timeout` from the call's start to its
/// commit, not one for the attach and another for the message: an attach
/// that outlasts it runs to its end and stays, and the post is refused,
/// its message not written.
#[tokio::test]
async fn a_first_post_s_attach_spends_the_deadline_its_message_shares() {
    let (kit, sms) = gated_room(Duration::from_millis(50)).await;
    let tap = EventTap::on(&kit, &["room_channel_attached"]);
    kit.store().binding.arm();
    let posting = {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.post(&room_id(), text("Notice."), PostOptions::default())
                .await
        })
    };
    kit.store().binding.reached.notified().await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    kit.store().binding.open.notify_one();
    let refused = posting.await.unwrap();
    roomkit::testing::idle(&kit).await;
    assert_eq!(
        (
            refused,
            binding_of(&kit, "host").await.is_some(),
            host_attaches(&kit, &tap).await,
            host_messages(&kit).await,
            sms.sent().len()
        ),
        (
            Err(RoomKitError::ProcessTimeout { room_id: room_id() }),
            true,
            1,
            0,
            0
        )
    );
}

/// RFC §10.5, §13.6: once the lock is held, the first post's attach runs
/// to its end whatever becomes of its caller: a post dropped meanwhile
/// leaves the host channel attached, and sends nothing.
#[tokio::test]
async fn a_cancelled_first_post_attaches_the_host_channel_to_its_end() {
    let (kit, sms) = gated_room(UNBOUNDED).await;
    let tap = EventTap::on(&kit, &["room_channel_attached"]);
    kit.store().binding.arm();
    let posting = {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.post(&room_id(), text("Notice."), PostOptions::default())
                .await
        })
    };
    kit.store().binding.reached.notified().await;
    posting.abort();
    assert!(posting.await.is_err_and(|ended| ended.is_cancelled()));
    kit.store().binding.open.notify_one();
    roomkit::testing::idle(&kit).await;
    assert_eq!(
        (
            binding_of(&kit, "host").await.is_some(),
            host_attaches(&kit, &tap).await,
            host_messages(&kit).await,
            sms.sent().len()
        ),
        (true, 1, 0, 0)
    );
}

/// RFC §10.5, §13.6: the post's read of its room, before the lock, spends
/// the call's `process_timeout` too: past it, the post is refused, nothing
/// written.
#[tokio::test]
async fn a_first_post_s_read_of_its_room_waits_within_process_timeout() {
    let (kit, sms) = gated_room(Duration::from_millis(50)).await;
    let before = written(&kit).await;
    kit.store().read.arm();
    let refused = kit
        .post(&room_id(), text("Notice."), PostOptions::default())
        .await;
    kit.store().read.open.notify_one();
    roomkit::testing::idle(&kit).await;
    assert_eq!(
        (refused, written(&kit).await == before, sms.sent().len()),
        (
            Err(RoomKitError::ProcessTimeout { room_id: room_id() }),
            true,
            0
        )
    );
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> store::ListEventsOptions {
    store::ListEventsOptions {
        limit: u64::MAX,
        event_filter: Some(EventFilter {
            include_blocked: true,
            ..EventFilter::default()
        }),
        ..store::ListEventsOptions::default()
    }
}
