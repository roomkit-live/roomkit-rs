//! `RoomKit::post`, the host's own voice in a room (RFC §10.5, Appendix
//! A.14.1, RMK-947): the host channel, a transport of type `HOST` that
//! brings the host's words in and is never delivered to, attached on a
//! room's first post; who a post speaks as; what it solicits; what refuses
//! it, with nothing written; and its key.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{Future, ready};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    Access, AgentResponsePolicy, BindingOptions, ChannelBinding, ChannelCategory, ChannelDirection,
    ChannelId, ChannelMediaType, ChannelOutput, ChannelType, DirectEvent, EventContent,
    EventDirection, EventSource, EventType, HookResult, IdentityId, InboundMessage, Participant,
    ParticipantId, Post, RoomContext, RoomEvent, RoomId, RoomOptions, Timestamp,
};
use roomkit::{
    Channel, ChannelDeclaration, ChannelError, ConversationStore, FrameworkEvent, HookExecution,
    HookSpec, HookTrigger, HostChannel, InboundError, KitError, LifecycleError, RoomKit, text_of,
};
use tokio::sync::Notify;

const CUSTOMER: &str = "+15550100001";

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

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
            ChannelType::Ai,
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
            EventSource::new(self.id.clone(), ChannelType::Ai, EventDirection::Outbound),
            text(self.reply),
            event.created_at,
        );
        ready(Ok(ChannelOutput::respond(vec![answer])))
    }
}

/// Room `r`, of `organization` when given: the customer on SMS, its binding
/// naming their number.
async fn sms_room(kit: &RoomKit, organization: Option<&str>, options: RoomOptions) -> Sms {
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
async fn written(kit: &RoomKit) -> String {
    let events = kit.store().list_events(&room_id(), true).await.unwrap();
    let bindings = kit.store().list_bindings(&room_id()).await.unwrap();
    let room = kit.store().get_room(&room_id()).await.unwrap();
    format!("{events:?}\n{bindings:?}\n{room:?}")
}

/// The room's binding of `channel_id`.
async fn binding_of(kit: &RoomKit, channel_id: &str) -> Option<ChannelBinding> {
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
        let kit = RoomKit::new();
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
                (ChannelType::Host, EventDirection::Inbound),
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
    let kit = RoomKit::new();
    let sms = sms_room(&kit, None, RoomOptions::default()).await;
    let agent = Agent::new("assistant", "It left the warehouse today.");
    kit.register_channel(agent.clone()).unwrap();
    kit.attach(&room_id(), &channel("assistant")).await.unwrap();
    kit.register_channel(HostChannel::with_id(channel("desk-host")))
        .unwrap();
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
            sms.sent().last().cloned()
        ),
        (
            1,
            Some("desk".to_owned()),
            Some((
                CUSTOMER.to_owned(),
                "It left the warehouse today.".to_owned()
            ))
        )
    );
}

/// RFC §10.5: the first post's attach is one section under the room lock:
/// concurrent first posts attach the host channel once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_first_posts_attach_the_host_channel_once() {
    let kit = RoomKit::new();
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
        let kit = RoomKit::new();
        sms_room(&kit, None, RoomOptions::default()).await;
        kit.register_channel(HostChannel::new()).unwrap();
        let mut options = BindingOptions {
            access: Access::WriteOnly,
            ..BindingOptions::default()
        };
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
    let kit = RoomKit::new();
    let sms = sms_room(&kit, None, RoomOptions::default()).await;
    let (holding, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (held, freed) = (Arc::clone(&holding), Arc::clone(&release));
    kit.hook(
        HookSpec {
            needs_lock: true,
            ..HookSpec::new("holder", HookTrigger::BeforeBroadcast, HookExecution::Sync)
        },
        move |_, _| {
            let (held, freed) = (Arc::clone(&held), Arc::clone(&freed));
            async move {
                held.notify_one();
                freed.notified().await;
                Ok(HookResult::allow())
            }
        },
    )
    .unwrap();
    let holder = {
        let kit = kit.clone();
        tokio::spawn(async move {
            let message = InboundMessage::new(channel("sms"), CUSTOMER, text("Hello?"));
            kit.process_inbound_in(&room_id(), message).await
        })
    };
    holding.notified().await;
    let before = written(&kit).await;
    kit.set_process_timeout(Some(Duration::from_millis(50)));
    let mut events = kit.subscribe();
    let refused = kit.post(&room_id(), Post::new(text("Notice."))).await;
    let after = written(&kit).await;
    release.notify_one();
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

/// RFC §10.5, §19.3: the host's words are a person's message: a post
/// addressing no one asks no agent of an `ADDRESSED_ONLY` room, one
/// addressing `a` asks `a`, and the host channel's unaddressed event asks
/// every agent, where an agent's output would ask none.
#[tokio::test]
async fn the_host_s_words_are_a_person_s_message_to_the_room_s_routing() {
    let mut turns = Vec::new();
    for speak in ["post", "post to a", "unaddressed"] {
        let kit = RoomKit::new();
        let options = RoomOptions {
            agent_response_policy: AgentResponsePolicy::AddressedOnly,
            ..RoomOptions::default()
        };
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
        let kit = RoomKit::new();
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
    let kit = RoomKit::new();
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
        let kit = RoomKit::new();
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
    let kit = RoomKit::new();
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
        .list_rooms(&roomkit::RoomFilter::default())
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

/// A host channel that declares itself an intelligence channel.
struct IntelligentHost;

impl Channel for IntelligentHost {
    fn declaration(&self) -> ChannelDeclaration {
        ChannelDeclaration::new(
            channel("host"),
            ChannelType::Host,
            ChannelCategory::Intelligence,
            ChannelDirection::Inbound,
        )
    }
}

/// RFC §6.2, §10.5: the host's words never come from an intelligence
/// channel: a host channel attached as one, by the category the attach
/// names or the one the channel declares, is refused, nothing bound.
#[tokio::test]
async fn a_host_channel_is_never_bound_as_an_intelligence_channel() {
    let mut refused = Vec::new();
    for declared in [false, true] {
        let kit = RoomKit::new();
        sms_room(&kit, None, RoomOptions::default()).await;
        let options = if declared {
            kit.register_channel(IntelligentHost).unwrap();
            BindingOptions::default()
        } else {
            kit.register_channel(HostChannel::new()).unwrap();
            BindingOptions {
                category: Some(ChannelCategory::Intelligence),
                ..BindingOptions::default()
            }
        };
        let attached = kit.attach_with(&room_id(), &channel("host"), options).await;
        refused.push((attached.err(), binding_of(&kit, "host").await.is_none()));
    }
    let refusal = || {
        (
            Some(KitError::Lifecycle(LifecycleError::HostAsIntelligence(
                channel("host"),
            ))),
            true,
        )
    };
    assert_eq!(refused, [refusal(), refusal()]);
}

/// RFC §13.4, §10.5: a key the host channel holds is a replay that
/// returns the original, sending nothing again; a blank key is no key.
#[tokio::test]
async fn a_post_replayed_with_its_key_is_not_texted_again() {
    let mut replays = Vec::new();
    for (key, texts) in [("notice-1042", 1), ("", 2), ("   ", 2)] {
        let kit = RoomKit::new();
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
        let kit = RoomKit::new();
        kit.set_max_chain_depth(1);
        sms_room(&kit, None, RoomOptions::default()).await;
        let posted = Arc::new(Mutex::new(Vec::new()));
        let (hook_kit, hook_posted) = (kit.clone(), Arc::clone(&posted));
        let mut framework = kit.subscribe();
        kit.hook(
            HookSpec {
                needs_lock,
                ..HookSpec::new("ack", HookTrigger::BeforeBroadcast, HookExecution::Sync)
            },
            move |event, _| {
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
            },
        )
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
            .list_events(&room_id(), true)
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
