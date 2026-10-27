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
use roomkit::model::{
    Access, AgentResponsePolicy, BindingOptions, ChannelBinding, ChannelCategory, ChannelDirection,
    ChannelId, ChannelMediaType, ChannelOutput, ChannelType, DirectEvent, EventContent,
    EventDirection, EventId, EventSource, EventType, EventUpdates, HookResult, Identity,
    IdentityId, InboundMessage, Observation, Participant, ParticipantId, Post, Room, RoomContext,
    RoomEvent, RoomId, RoomOptions, RoomUpdates, Task, Timestamp,
};
use roomkit::{
    Channel, ChannelDeclaration, ChannelError, ConfigError, ConversationStore, EventFilter,
    EventQuery, FrameworkEvent, HookExecution, HookSpec, HookTrigger, HostChannel, InMemoryStore,
    InboundError, KitError, LifecycleError, RoomCounters, RoomFilter, RoomKit, RoomKitOptions,
    StoreError, TargetUpdate, text_of,
};
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
    EventContent::text(body)
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

impl Channel for Sms {
    fn declaration(&self) -> ChannelDeclaration {
        ChannelDeclaration::new(
            channel("sms"),
            ChannelType::SMS,
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
            .and_then(|to| to.as_str())
            .unwrap_or_default()
            .to_owned();
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((to, text_of(&event.content)));
        ready(Ok(ChannelOutput::default()))
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

impl Channel for Agent {
    fn declaration(&self) -> ChannelDeclaration {
        ChannelDeclaration::new(
            self.id.clone(),
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        )
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
        let answer = RoomEvent::new(
            event.id.clone(),
            event.room_id.clone(),
            EventType::Message,
            EventSource::new(self.id.clone(), ChannelType::AI, EventDirection::Outbound),
            text(self.reply),
            event.created_at,
        );
        ready(Ok(ChannelOutput::respond(vec![answer])))
    }
}

/// A host channel that counts what it is asked to deliver: a host channel
/// is never delivered to (RFC §10.5).
#[derive(Clone)]
struct CountingHost {
    declaration: ChannelDeclaration,
    delivered: Arc<Mutex<usize>>,
}

impl CountingHost {
    fn new(id: &str) -> Self {
        Self {
            declaration: ChannelDeclaration::host(channel(id)),
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
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
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
}

/// Room `r`, of `organization` when given: the customer on SMS, its binding
/// naming their number.
async fn sms_room<S: ConversationStore>(
    kit: &RoomKit<S>,
    organization: Option<&str>,
    options: RoomOptions,
) -> Sms {
    let sms = Sms::default();
    kit.register_channel(sms.clone()).unwrap();
    let mut binding = BindingOptions::default();
    binding
        .metadata
        .insert("phone_number".to_owned(), CUSTOMER.into());
    if let Some(organization) = organization {
        let owner = kit.for_organization(organization);
        owner.create_room_with(&room_id(), options).await.unwrap();
        owner
            .attach_with(&room_id(), &channel("sms"), binding)
            .await
            .unwrap();
    } else {
        kit.create_room_with(&room_id(), options).await.unwrap();
        kit.attach_with(&room_id(), &channel("sms"), binding)
            .await
            .unwrap();
    }
    sms
}

/// What the room holds: its events, refused ones included, its bindings
/// and the room itself.
async fn written<S: ConversationStore>(kit: &RoomKit<S>) -> String {
    let events = kit
        .store()
        .list_events(&room_id(), &every_row())
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
        Timestamp::UNIX_EPOCH,
    )
}

/// RFC §10.5: a post reaches the room's correspondent as a party's
/// message, `INBOUND` from the host channel, asks no agent, keeps the
/// metadata it came with; the host channel attached on the first post
/// declares every media type.
#[tokio::test]
async fn a_post_texts_the_correspondent_and_solicits_no_agent() {
    for organization in [None, Some("acme")] {
        let kit = RoomKit::new(Default::default());
        let sms = sms_room(&kit, organization, RoomOptions::default()).await;
        let agent = Agent::new("assistant", "should not answer");
        kit.register_channel(agent.clone()).unwrap();
        kit.attach(&room_id(), &channel("assistant")).await.unwrap();
        let mut post = Post::new(text("Your order shipped."));
        post.metadata
            .insert("job".to_owned(), "shipping-notice".into());
        let event = match organization {
            Some(organization) => {
                kit.for_organization(organization)
                    .post(&room_id(), post)
                    .await
            }
            None => kit.post(&room_id(), post).await,
        }
        .unwrap();
        kit.idle().await;
        let host = binding_of(&kit, "host").await.unwrap();
        assert_eq!(
            (
                sms.sent(),
                agent.asked().len(),
                (event.source.channel_type, event.source.direction),
                event.metadata.get("job").cloned(),
                host.capabilities.media_types.len(),
                host.capabilities
                    .media_types
                    .contains(&ChannelMediaType::Location),
            ),
            (
                vec![(CUSTOMER.to_owned(), "Your order shipped.".to_owned())],
                0,
                (ChannelType::HOST, EventDirection::Inbound),
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
    let kit = RoomKit::new(Default::default());
    let sms = sms_room(&kit, None, RoomOptions::default()).await;
    let agent = Agent::new("assistant", "It left the warehouse today.");
    kit.register_channel(agent.clone()).unwrap();
    kit.attach(&room_id(), &channel("assistant")).await.unwrap();
    let host = CountingHost::new("desk-host");
    kit.register_channel(host.clone()).unwrap();
    kit.store()
        .create_participant(participant("desk", "desk-host"))
        .await
        .unwrap();
    let mut post = Post::new(text("Tell the customer where the order is."));
    post.participant_id = Some(ParticipantId::new("desk").unwrap());
    post.addressed_to = vec![channel("assistant")];
    post.channel_id = Some(channel("desk-host"));
    kit.post(&room_id(), post).await.unwrap();
    kit.idle().await;
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
    let kit = RoomKit::new(Default::default());
    sms_room(&kit, None, RoomOptions::default()).await;
    let mut events = kit.subscribe();
    let posts = (0..5).map(|n| {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.post(&room_id(), Post::new(text(&format!("Notice {n}."))))
                .await
        })
    });
    for post in posts.collect::<Vec<_>>() {
        post.await.unwrap().unwrap();
    }
    let mut attached = 0;
    while let Ok(event) = events.try_recv() {
        if matches!(&event, FrameworkEvent::RoomChannelAttached { channel_id, .. } if channel_id.as_str() == "host")
        {
            attached += 1;
        }
    }
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
        let kit = RoomKit::new(Default::default());
        sms_room(&kit, None, RoomOptions::default()).await;
        kit.register_channel(HostChannel::new()).unwrap();
        let mut options = BindingOptions::default();
        options.access = Access::WriteOnly;
        options.metadata.insert("desk".to_owned(), "1".into());
        let attaching = {
            let kit = kit.clone();
            async move { kit.attach_with(&room_id(), &channel("host"), options).await }
        };
        if concurrently {
            let room = room_id();
            let (attached, posted) =
                tokio::join!(attaching, kit.post(&room, Post::new(text("Notice."))));
            attached.unwrap();
            posted.unwrap();
        } else {
            attaching.await.unwrap();
            kit.post(&room_id(), Post::new(text("Notice.")))
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
        tokio::spawn(async move { kit.commit_event(&room_id(), record("A record.")).await })
    };
    kit.store().read.reached.notified().await;
    let before = written(&kit).await;
    let mut events = kit.subscribe();
    let refused = kit.post(&room_id(), Post::new(text("Notice."))).await;
    let after = written(&kit).await;
    kit.store().read.open.notify_one();
    holder.await.unwrap().unwrap();
    let timed_out = std::iter::from_fn(|| events.try_recv().ok())
        .filter(|event| matches!(event, FrameworkEvent::ProcessTimeout { .. }))
        .count();
    assert_eq!(
        (refused, after == before, timed_out, sms.sent().len()),
        (
            Err(KitError::Inbound(InboundError::Refused {
                reason: "process_timeout".to_owned()
            })),
            true,
            1,
            0
        )
    );
}

/// A record of the SMS channel saying `body`, for `commit_event`.
fn record(body: &str) -> RoomEvent {
    RoomEvent::new(
        EventId::new("record_1").unwrap(),
        room_id(),
        EventType::Message,
        EventSource::new(channel("sms"), ChannelType::SMS, EventDirection::Inbound),
        text(body),
        Timestamp::UNIX_EPOCH,
    )
}

/// RFC §10.5, §19.3: the host's words are a person's message: a post
/// addressing no one asks no agent of an `ADDRESSED_ONLY` room, one
/// addressing `a` asks `a`, and the host channel's unaddressed event asks
/// every agent, where an agent's output would ask none.
#[tokio::test]
async fn the_host_s_words_are_a_person_s_message_to_the_room_s_routing() {
    let mut turns = Vec::new();
    for speak in ["post", "post to a", "unaddressed"] {
        let kit = RoomKit::new(Default::default());
        let mut options = RoomOptions::default();
        options.agent_response_policy = AgentResponsePolicy::AddressedOnly;
        let sms = sms_room(&kit, None, options).await;
        let (a, b) = (Agent::new("a", "A says."), Agent::new("b", "B says."));
        for agent in [&a, &b] {
            kit.register_channel(agent.clone()).unwrap();
            kit.attach(&room_id(), &agent.id).await.unwrap();
        }
        if speak == "unaddressed" {
            kit.post(&room_id(), Post::new(text("Your order shipped.")))
                .await
                .unwrap();
            let unaddressed = DirectEvent::new(channel("host"), text("Any question?"));
            kit.send_event(&room_id(), unaddressed).await.unwrap();
        } else {
            let mut post = Post::new(text("Any question?"));
            if speak == "post to a" {
                post.addressed_to = vec![channel("a")];
            }
            kit.post(&room_id(), post).await.unwrap();
        }
        kit.idle().await;
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
    let notice = || {
        let mut post = Post::new(text("An earlier notice."));
        post.idempotency_key = Some("notice".to_owned());
        post
    };
    let acme = kit.for_organization("acme");
    match setup {
        Setup::Nothing => {}
        Setup::DetachAfterAPost => {
            acme.post(&room_id(), notice()).await.unwrap();
            acme.detach(&room_id(), &channel("host")).await.unwrap();
        }
        Setup::Close => {
            acme.close_room(&room_id()).await.unwrap();
        }
        Setup::CloseAfterAPost => {
            acme.post(&room_id(), notice()).await.unwrap();
            acme.close_room(&room_id()).await.unwrap();
        }
        Setup::TakeTheHostId => {
            kit.register_channel(Agent::new("host", "unused")).unwrap();
        }
        Setup::CustomerOnSms => {
            kit.store()
                .create_participant(participant(CUSTOMER, "sms"))
                .await
                .unwrap();
        }
        Setup::AnAgent => {
            kit.register_channel(Agent::new("assistant", "unused"))
                .unwrap();
            acme.attach(&room_id(), &channel("assistant"))
                .await
                .unwrap();
        }
    }
}

/// What a refused post changes of its arguments, the organization it is
/// made for among them.
type Arguments = fn(&mut Post, &mut Option<&str>);

/// The posts refused before anything is written, with what sets each one
/// up and its error (RFC §10.5).
fn refusals() -> Vec<(&'static str, Setup, Arguments, KitError)> {
    let refused = |reason: &str| {
        KitError::Inbound(InboundError::Refused {
            reason: reason.to_owned(),
        })
    };
    let not_found = || KitError::Inbound(InboundError::RoomNotFound(room_id()));
    let unreached = |speaker: &str, reached_elsewhere| {
        KitError::Inbound(InboundError::ParticipantNotFound {
            room_id: room_id(),
            participant_id: ParticipantId::new(speaker).unwrap(),
            channel_id: channel("host"),
            reached_elsewhere,
        })
    };
    vec![
        (
            "a channel of another type",
            Setup::Nothing,
            |post, _| post.channel_id = Some(channel("sms")),
            KitError::Inbound(InboundError::NotAHostChannel(channel("sms"))),
        ),
        (
            "an unregistered channel",
            Setup::Nothing,
            |post, _| post.channel_id = Some(channel("ghost")),
            KitError::Inbound(InboundError::UnknownChannel(channel("ghost"))),
        ),
        (
            "host taken by another type",
            Setup::TakeTheHostId,
            |_, _| {},
            KitError::Inbound(InboundError::NotAHostChannel(channel("host"))),
        ),
        (
            "a closed room",
            Setup::Close,
            |_, _| {},
            refused("room_closed"),
        ),
        (
            "a detached host channel",
            Setup::DetachAfterAPost,
            |_, _| {},
            refused("source_unbound"),
        ),
        (
            "a replay into a room closed since",
            Setup::CloseAfterAPost,
            |post, _| post.idempotency_key = Some("notice".to_owned()),
            refused("room_closed"),
        ),
        (
            "a replay after a detach",
            Setup::DetachAfterAPost,
            |post, _| post.idempotency_key = Some("notice".to_owned()),
            refused("source_unbound"),
        ),
        (
            "another organization's room",
            Setup::Nothing,
            |_, organization| *organization = Some("other"),
            not_found(),
        ),
        (
            "an unknown speaker",
            Setup::Nothing,
            |post, _| post.participant_id = Some(ParticipantId::new("ghost").unwrap()),
            unreached("ghost", false),
        ),
        (
            "a speaker reached through SMS only",
            Setup::CustomerOnSms,
            |post, _| post.participant_id = Some(ParticipantId::new(CUSTOMER).unwrap()),
            unreached(CUSTOMER, true),
        ),
        (
            "an agent's channel id as the speaker",
            Setup::AnAgent,
            |post, _| post.participant_id = Some(ParticipantId::new("assistant").unwrap()),
            unreached("assistant", false),
        ),
        (
            "an unknown speaker in another organization's room",
            Setup::Nothing,
            |post, organization| {
                post.participant_id = Some(ParticipantId::new("ghost").unwrap());
                *organization = Some("other");
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
        let kit = RoomKit::new(Default::default());
        let sms = sms_room(&kit, Some("acme"), RoomOptions::default()).await;
        set_up(&kit, setup).await;
        let (before, texted) = (written(&kit).await, sms.sent());
        let mut post = Post::new(text("Never sent."));
        let mut organization = Some("acme");
        arguments(&mut post, &mut organization);
        let posted = match organization {
            Some(organization) => {
                kit.for_organization(organization)
                    .post(&room_id(), post)
                    .await
            }
            None => kit.post(&room_id(), post).await,
        };
        kit.idle().await;
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
    let kit = RoomKit::new(Default::default());
    sms_room(&kit, None, RoomOptions::default()).await;
    set_up(&kit, Setup::CustomerOnSms).await;
    let mut told = Vec::new();
    for speaker in ["ghost", CUSTOMER] {
        let mut post = Post::new(text("Never sent."));
        post.participant_id = Some(ParticipantId::new(speaker).unwrap());
        told.push(kit.post(&room_id(), post).await.unwrap_err().to_string());
    }
    assert!(
        told[0].ends_with("ghost names nobody of the room")
            && told[1].ends_with("names a participant reached through other channels only"),
        "{told:?}"
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
        let kit = RoomKit::new(Default::default());
        let sms = sms_room(&kit, None, RoomOptions::default()).await;
        let mut desk = participant("desk", "host");
        desk.identity_id = Some(IdentityId::new("id-desk").unwrap());
        let mut customer = participant(CUSTOMER, "sms");
        customer.connected_via.push(channel("host"));
        for member in [desk, customer] {
            kit.store().create_participant(member).await.unwrap();
        }
        let mut post = Post::new(text("Your order shipped."));
        post.participant_id = (!speaker.is_empty()).then(|| ParticipantId::new(speaker).unwrap());
        let event = kit.post(&room_id(), post).await.unwrap();
        kit.idle().await;
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
    let kit = RoomKit::new(Default::default());
    sms_room(&kit, None, RoomOptions::default()).await;
    kit.post(&room_id(), Post::new(text("Notice.")))
        .await
        .unwrap();
    kit.idle().await;
    let before = written(&kit).await;
    let message = || InboundMessage::new(channel("host"), "x", text("hi"));
    let named = kit.process_inbound_in(&room_id(), message()).await;
    let routed = kit.process_inbound(message()).await;
    let rooms = kit
        .store()
        .list_rooms(&RoomFilter::default())
        .await
        .unwrap();
    let refused = || {
        Err(KitError::Inbound(InboundError::InboundOnHostChannel(
            channel("host"),
        )))
    };
    assert_eq!(
        (named, routed, rooms.len(), written(&kit).await == before),
        (refused(), refused(), 1, true)
    );
}

/// A channel of type `channel_type`, as `category` and `direction`.
struct Claiming(ChannelType, ChannelCategory, ChannelDirection);

impl Channel for Claiming {
    fn declaration(&self) -> ChannelDeclaration {
        ChannelDeclaration::new(channel("claiming"), self.0.clone(), self.1, self.2)
    }
}

/// RFC §6.2, §10.5: `system` is the framework's own records' type, which
/// no channel claims, and `host` a host channel's, a transport whose
/// direction is `INBOUND`: a channel claiming either otherwise is refused
/// at its registration, and a host channel is registered.
#[tokio::test]
async fn a_channel_claiming_a_reserved_type_is_refused() {
    use ChannelCategory::{Intelligence, Transport};
    use ChannelDirection::{Bidirectional, Inbound};
    let kit = RoomKit::new(Default::default());
    let claims = [
        Claiming(ChannelType::HOST, Intelligence, Inbound),
        Claiming(ChannelType::HOST, Transport, Bidirectional),
        Claiming(ChannelType::SYSTEM, Transport, Inbound),
    ];
    let refused: Vec<Result<(), KitError>> = claims
        .into_iter()
        .map(|claim| kit.register_channel(claim))
        .collect();
    let reserved = |channel_type: ChannelType| {
        Err(KitError::Config(ConfigError::ReservedChannelType {
            channel_id: channel("claiming"),
            channel_type,
        }))
    };
    assert_eq!(
        (
            refused,
            kit.register_channel(HostChannel::with_id(channel("desk")))
        ),
        (
            vec![
                reserved(ChannelType::HOST),
                reserved(ChannelType::HOST),
                reserved(ChannelType::SYSTEM)
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
    let kit = RoomKit::new(Default::default());
    sms_room(&kit, None, RoomOptions::default()).await;
    kit.register_channel(HostChannel::new()).unwrap();
    let mut options = BindingOptions::default();
    options.category = Some(ChannelCategory::Intelligence);
    let attached = kit.attach_with(&room_id(), &channel("host"), options).await;
    assert_eq!(
        (attached.err(), binding_of(&kit, "host").await.is_none()),
        (
            Some(KitError::Lifecycle(LifecycleError::HostAsIntelligence(
                channel("host")
            ))),
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
        let kit = RoomKit::new(Default::default());
        let sms = sms_room(&kit, None, RoomOptions::default()).await;
        let post = || {
            let mut post = Post::new(text("Your order shipped."));
            post.idempotency_key = Some(key.to_owned());
            post
        };
        let first = kit.post(&room_id(), post()).await.unwrap();
        let again = kit.post(&room_id(), post()).await.unwrap();
        kit.idle().await;
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
        sms_room(&kit, None, RoomOptions::default()).await;
        let posted = Arc::new(Mutex::new(Vec::new()));
        let (hook_kit, hook_posted) = (kit.clone(), Arc::clone(&posted));
        let mut framework = kit.subscribe();
        let mut ack = HookSpec::new("ack", HookTrigger::BeforeBroadcast, HookExecution::Sync);
        ack.needs_lock = needs_lock;
        kit.hook(ack, move |event, _| {
            let (kit, posted) = (hook_kit.clone(), Arc::clone(&hook_posted));
            async move {
                if event.source.channel_id == channel("sms") {
                    for _ in 0..12 {
                        let mut ack = Post::new(text("Received."));
                        ack.idempotency_key = Some("ack".to_owned());
                        let event = kit.post(&room_id(), ack).await?;
                        posted.lock().unwrap().push(event.id);
                    }
                    let event = kit
                        .post(&room_id(), Post::new(text("We answer within the hour.")))
                        .await?;
                    posted.lock().unwrap().push(event.id);
                }
                Ok(HookResult::allow())
            }
        })
        .unwrap();
        let message = InboundMessage::new(channel("sms"), CUSTOMER, text("Hello?"));
        tokio::time::timeout(
            Duration::from_secs(5),
            kit.process_inbound_in(&room_id(), message),
        )
        .await
        .unwrap()
        .unwrap();
        kit.idle().await;
        let posted = posted.lock().unwrap().clone();
        let host: Vec<(String, Option<String>)> = kit
            .store()
            .list_events(&room_id(), &every_row())
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.source.channel_id == channel("host"))
            .map(|event| (text_of(&event.content), event.blocked_by))
            .collect();
        let attaches = std::iter::from_fn(|| framework.try_recv().ok())
            .filter(|event| matches!(event, FrameworkEvent::RoomChannelAttached { channel_id, .. } if channel_id.as_str() == "host"))
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
    async fn create_room(&self, room: Room) -> Result<(), StoreError> {
        self.store.create_room(room).await
    }

    async fn update_room(
        &self,
        room_id: &RoomId,
        updates: RoomUpdates,
    ) -> Result<Room, StoreError> {
        self.store.update_room(room_id, updates).await
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        self.store.get_room(room_id).await
    }

    async fn advance_delivered_index(
        &self,
        room_id: &RoomId,
        index: u64,
        force: bool,
    ) -> Result<bool, StoreError> {
        self.store
            .advance_delivered_index(room_id, index, force)
            .await
    }

    async fn list_rooms(&self, filter: &RoomFilter) -> Result<Vec<Room>, StoreError> {
        self.store.list_rooms(filter).await
    }

    async fn find_room_id_by_binding(
        &self,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        identity_id: Option<&IdentityId>,
        filter: &RoomFilter,
    ) -> Result<Option<RoomId>, StoreError> {
        self.store
            .find_room_id_by_binding(channel_id, participant_id, identity_id, filter)
            .await
    }

    async fn find_room_id_by_participant(
        &self,
        channel_id: &ChannelId,
        participant_id: &ParticipantId,
        identity_id: Option<&IdentityId>,
        filter: &RoomFilter,
    ) -> Result<Option<RoomId>, StoreError> {
        self.store
            .find_room_id_by_participant(channel_id, participant_id, identity_id, filter)
            .await
    }

    async fn store_binding(&self, binding: ChannelBinding) -> Result<(), StoreError> {
        if binding.channel_id.as_str() == "host" {
            self.binding.pass().await;
        }
        self.store.store_binding(binding).await
    }

    async fn delete_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<(), StoreError> {
        self.store.delete_binding(room_id, channel_id).await
    }

    async fn list_bindings(&self, room_id: &RoomId) -> Result<Vec<ChannelBinding>, StoreError> {
        self.store.list_bindings(room_id).await
    }

    async fn load_context(
        &self,
        room_id: &RoomId,
        recent_events: usize,
    ) -> Result<Option<RoomContext>, StoreError> {
        self.read.pass().await;
        self.store.load_context(room_id, recent_events).await
    }

    async fn create_participant(&self, participant: Participant) -> Result<(), StoreError> {
        self.store.create_participant(participant).await
    }

    async fn get_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<Option<Participant>, StoreError> {
        self.store.get_participant(room_id, participant_id).await
    }

    async fn update_participant(&self, participant: Participant) -> Result<(), StoreError> {
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

    async fn create_identity(&self, identity: Identity) -> Result<(), StoreError> {
        self.store.create_identity(identity).await
    }

    async fn get_identity(&self, identity_id: &IdentityId) -> Result<Option<Identity>, StoreError> {
        self.store.get_identity(identity_id).await
    }

    async fn resolve_identity(
        &self,
        channel_type: ChannelType,
        address: &str,
        organization_id: Option<&str>,
    ) -> Result<Option<Identity>, StoreError> {
        self.store
            .resolve_identity(channel_type, address, organization_id)
            .await
    }

    async fn link_address(
        &self,
        identity_id: &IdentityId,
        channel_type: ChannelType,
        address: &str,
        organization_id: Option<&str>,
    ) -> Result<(), StoreError> {
        self.store
            .link_address(identity_id, channel_type, address, organization_id)
            .await
    }

    async fn unlink_address(
        &self,
        identity_id: &IdentityId,
        channel_type: ChannelType,
        address: &str,
        organization_id: Option<&str>,
    ) -> Result<(), StoreError> {
        self.store
            .unlink_address(identity_id, channel_type, address, organization_id)
            .await
    }

    async fn get_event_by_idempotency_key(
        &self,
        room_id: &RoomId,
        channel_id: Option<&ChannelId>,
        key: &str,
    ) -> Result<Option<RoomEvent>, StoreError> {
        self.store
            .get_event_by_idempotency_key(room_id, channel_id, key)
            .await
    }

    async fn commit(
        &self,
        event: RoomEvent,
        counters: RoomCounters,
        target: Option<TargetUpdate>,
    ) -> Result<(), StoreError> {
        self.store.commit(event, counters, target).await
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Option<RoomEvent>, StoreError> {
        self.store.get_event(event_id).await
    }

    async fn update_event(
        &self,
        event_id: &EventId,
        updates: EventUpdates,
    ) -> Result<RoomEvent, StoreError> {
        self.store.update_event(event_id, updates).await
    }

    async fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        cascade_replies: bool,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.store
            .delete_event(room_id, event_id, cascade_replies)
            .await
    }

    async fn get_event_count(
        &self,
        room_id: &RoomId,
        filter: Option<EventFilter>,
    ) -> Result<u64, StoreError> {
        self.store.get_event_count(room_id, filter).await
    }

    async fn persist_side_effects(
        &self,
        room_id: &RoomId,
        tasks: Vec<Task>,
        observations: Vec<Observation>,
    ) -> Result<(), StoreError> {
        self.store
            .persist_side_effects(room_id, tasks, observations)
            .await
    }

    async fn list_events(
        &self,
        room_id: &RoomId,
        query: &EventQuery,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.store.list_events(room_id, query).await
    }

    async fn list_tasks(&self, room_id: &RoomId) -> Result<Vec<Task>, StoreError> {
        self.store.list_tasks(room_id).await
    }

    async fn list_observations(&self, room_id: &RoomId) -> Result<Vec<Observation>, StoreError> {
        self.store.list_observations(room_id).await
    }
}

/// A kit on a gated store, with room `r` and its SMS, its calls bounded by
/// `process_timeout`.
async fn gated_room(process_timeout: Duration) -> (RoomKit<GatedStore>, Sms) {
    let kit = RoomKit::new(RoomKitOptions {
        process_timeout,
        ..RoomKitOptions::with_store(GatedStore::default())
    });
    let sms = sms_room(&kit, None, RoomOptions::default()).await;
    (kit, sms)
}

/// How many times the host channel was attached, as the kit reported it.
fn host_attaches(events: &mut tokio::sync::broadcast::Receiver<FrameworkEvent>) -> usize {
    std::iter::from_fn(|| events.try_recv().ok())
        .filter(|event| {
            matches!(event, FrameworkEvent::RoomChannelAttached { channel_id, .. } if channel_id.as_str() == "host")
        })
        .count()
}

/// The room's messages from the host channel.
async fn host_messages<S: ConversationStore>(kit: &RoomKit<S>) -> usize {
    kit.store()
        .list_events(&room_id(), &every_row())
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
    let mut events = kit.subscribe();
    kit.store().binding.arm();
    let posting = {
        let kit = kit.clone();
        tokio::spawn(async move { kit.post(&room_id(), Post::new(text("Notice."))).await })
    };
    kit.store().binding.reached.notified().await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    kit.store().binding.open.notify_one();
    let refused = posting.await.unwrap();
    kit.idle().await;
    assert_eq!(
        (
            refused,
            binding_of(&kit, "host").await.is_some(),
            host_attaches(&mut events),
            host_messages(&kit).await,
            sms.sent().len()
        ),
        (
            Err(KitError::Inbound(InboundError::Refused {
                reason: "process_timeout".to_owned()
            })),
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
    let mut events = kit.subscribe();
    kit.store().binding.arm();
    let posting = {
        let kit = kit.clone();
        tokio::spawn(async move { kit.post(&room_id(), Post::new(text("Notice."))).await })
    };
    kit.store().binding.reached.notified().await;
    posting.abort();
    assert!(posting.await.is_err_and(|ended| ended.is_cancelled()));
    kit.store().binding.open.notify_one();
    kit.idle().await;
    assert_eq!(
        (
            binding_of(&kit, "host").await.is_some(),
            host_attaches(&mut events),
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
    let refused = kit.post(&room_id(), Post::new(text("Notice."))).await;
    kit.store().read.open.notify_one();
    kit.idle().await;
    assert_eq!(
        (refused, written(&kit).await == before, sms.sent().len()),
        (
            Err(KitError::Inbound(InboundError::Refused {
                reason: "process_timeout".to_owned()
            })),
            true,
            0
        )
    );
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> EventQuery {
    EventQuery {
        limit: u64::MAX,
        event_filter: Some(EventFilter {
            include_blocked: true,
            ..EventFilter::default()
        }),
        ..EventQuery::default()
    }
}
