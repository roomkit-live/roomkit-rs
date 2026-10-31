//! A message into a room its caller names, on the tokio runtime (RFC §10.1
//! step 2, §10.2 step 3d, §17.2): a missing room created once however many
//! messages name it, the `ON_ROOM_CREATED` hooks awaited before the channel
//! is attached and bounded by their timeout, a room created whole whoever
//! stops waiting for its message, the recipient an SMS delivery needs, the
//! caller's organization, and the participants the host stores.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
use std::future::{Future, ready};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    HookResult, InboundMessage, Metadata, Participant, ParticipantId, RoomContext, RoomEvent,
    RoomId, TextContent, Timestamp,
};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
    SequentialEnvironment, kit_with_environment,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, ConversationStore,
    CreateRoomOptions, FrameworkEvent, HookExecution, HookSpec, HookTrigger, ProcessInboundOptions,
    RoomFilter, RoomKit, RoomKitError, RoomKitOptions,
};
use tokio::sync::{Notify, broadcast, mpsc};

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

fn message(from: &RecordingTransport, text: &str) -> InboundMessage {
    InboundMessage::new(
        from.channel_id().clone(),
        "+15551234567",
        TextContent::new(text),
    )
}

/// A kit with no room, an SMS transport and an echo agent registered.
fn kit_without_rooms() -> (RoomKit, RecordingTransport) {
    let kit = kit_with_environment(
        RoomKitOptions::default(),
        SequentialEnvironment::new(Timestamp::from_unix_millis(1_792_677_780_000).unwrap()),
    );
    let sms = RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(EchoAgent::new(
        channel("ai"),
        "You said: ",
        EchoAgentOptions { quote: true },
    ))
    .unwrap();
    (kit, sms)
}

/// How many `room_created` events `events` holds.
fn rooms_created(events: &mut broadcast::Receiver<FrameworkEvent>) -> usize {
    let mut created = 0;
    while let Ok(event) = events.try_recv() {
        created += usize::from(matches!(event, FrameworkEvent::RoomCreated { .. }));
    }
    created
}

/// The room's lane runs one message at a time: the second finds the room
/// the first created.
#[tokio::test]
async fn two_messages_naming_one_missing_room_create_it_once() {
    let (kit, sms) = kit_without_rooms();
    let mut events = kit.subscribe();
    let room = room_id();
    let (first, second) = tokio::join!(
        kit.process_inbound(
            message(&sms, "one"),
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            }
        ),
        kit.process_inbound(
            message(&sms, "two"),
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            }
        ),
    );
    first.unwrap();
    second.unwrap();
    let indexes: Vec<u64> = kit
        .store()
        .list_events(&room, &every_row())
        .await
        .unwrap()
        .iter()
        .map(|event| event.index)
        .collect();
    let rooms = kit
        .store()
        .list_rooms(&RoomFilter::default())
        .await
        .unwrap();
    assert_eq!(
        (rooms.len(), rooms_created(&mut events), indexes),
        // The attach's lifecycle record, then the two messages (gap G-046).
        (1, 1, vec![0, 1, 2])
    );
}

/// RFC Appendix B.1, gap G-050: a hook on `ON_ROOM_CREATED` attaches the
/// agent, and the step waits for it, so the agent answers the first
/// message.
#[tokio::test]
async fn an_agent_attached_by_an_on_room_created_hook_answers_the_first_message() {
    let (kit, sms) = kit_without_rooms();
    let attaching = kit.clone();
    kit.observe("welcome", HookTrigger::OnRoomCreated, move |event, _| {
        let kit = attaching.clone();
        async move {
            // Slow enough that an attach not awaited would come too late.
            tokio::time::sleep(Duration::from_millis(20)).await;
            kit.attach_channel(
                &event.room_id,
                &channel("ai"),
                AttachChannelOptions::default(),
            )
            .await
            .unwrap();
        }
    })
    .unwrap();
    kit.process_inbound(
        message(&sms, "Bonjour"),
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(sms.texts(), ["You said: Bonjour"]);
}

#[tokio::test]
async fn a_room_created_for_an_organization_belongs_to_it() {
    let (kit, sms) = kit_without_rooms();
    let options = ProcessInboundOptions {
        room_id: Some(room_id()),
        organization_id: Some("bank".to_owned()),
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(message(&sms, "Bonjour"), options)
        .await
        .unwrap();
    let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
    assert_eq!(room.organization_id.as_deref(), Some("bank"));
}

/// RFC §17.2, §5.1: another organization's room is not found, and the
/// message fails with nothing written.
#[tokio::test]
async fn a_room_of_another_organization_is_not_found_with_nothing_written() {
    let (kit, sms) = kit_without_rooms();
    let bank = CreateRoomOptions {
        room_id: Some(room_id()),
        organization_id: Some("bank".to_owned()),
        ..Default::default()
    };
    kit.create_room(bank).await.unwrap();
    let clinic = ProcessInboundOptions {
        room_id: Some(room_id()),
        organization_id: Some("clinic".to_owned()),
        ..ProcessInboundOptions::default()
    };
    let refused = kit.process_inbound(message(&sms, "Bonjour"), clinic).await;
    assert_eq!(
        (
            refused,
            kit.store().list_bindings(&room_id()).await.unwrap().len()
        ),
        (Err(RoomKitError::RoomNotFound(room_id())), 0)
    );
}

/// RFC §10.1 step 3: the participants the host stores are in the context.
#[tokio::test]
async fn a_participant_the_host_stores_is_in_the_context_a_hook_reads() {
    let (kit, sms) = kit_without_rooms();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    let jean = Participant::new(
        ParticipantId::new("+15551234567").unwrap(),
        room_id(),
        channel("sms"),
    );
    kit.store().create_participant(jean.clone()).await.unwrap();
    let (seen, mut read) = mpsc::unbounded_channel();
    kit.before_broadcast("roster", move |_, context| {
        let seen = seen.clone();
        async move {
            seen.send(context.participants).unwrap();
            Ok(HookResult::allow(AllowOptions::default()))
        }
    })
    .unwrap();
    kit.process_inbound(
        message(&sms, "Bonjour"),
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(read.recv().await, Some(vec![jean]));
}

/// An `ON_ROOM_CREATED` hook that never returns holds step 2 up to its
/// timeout only: the channel is attached, the message delivered, and the
/// timeout reported (gap G-050).
#[tokio::test]
async fn an_on_room_created_hook_that_hangs_is_bounded_by_its_timeout() {
    let (kit, sms) = kit_without_rooms();
    let mut events = kit.subscribe();
    let mut spec = HookSpec::new("stuck", HookTrigger::OnRoomCreated, HookExecution::Async);
    spec.timeout = Duration::from_millis(50);
    kit.hook(spec, |_, _| async {
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(HookResult::allow(AllowOptions::default()))
    })
    .unwrap();
    let room = room_id();
    let sent = tokio::time::timeout(
        Duration::from_secs(5),
        kit.process_inbound(
            message(&sms, "Bonjour"),
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        ),
    )
    .await;
    let mut timed_out = false;
    while let Ok(event) = events.try_recv() {
        timed_out |= matches!(event, FrameworkEvent::HookTimeout { .. });
    }
    assert_eq!(
        (
            sent.map(|result| result.map(|result| result.blocked)),
            timed_out,
            kit.store().list_bindings(&room).await.unwrap().len()
        ),
        (Ok(Ok(false)), true, 1)
    );
}

/// Decision 1 of step 2b, gap G-068: the caller gives up while the hooks of
/// the room its message created run. The room is completed all the same:
/// the hooks finish, the channel is attached with a binding naming the
/// sender, and the session starts. The message, not decided by then, stops
/// there, nothing of it committed.
#[tokio::test]
async fn a_room_created_for_a_dropped_call_is_attached_all_the_same() {
    let (kit, sms) = kit_without_rooms();
    let (observing, go) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (started, released) = (Arc::clone(&observing), Arc::clone(&go));
    kit.observe("welcome", HookTrigger::OnRoomCreated, move |_, _| {
        let (started, released) = (Arc::clone(&started), Arc::clone(&released));
        async move {
            started.notify_one();
            released.notified().await;
        }
    })
    .unwrap();
    let mut events = kit.subscribe();
    let room = room_id();
    let call = kit.process_inbound(
        message(&sms, "Bonjour"),
        ProcessInboundOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        },
    );
    tokio::select! {
        result = call => panic!("the call ended before its room's hooks: {result:?}"),
        () = observing.notified() => {}
    }
    go.notify_one();
    let session = async {
        loop {
            if let Ok(FrameworkEvent::SessionStarted { .. }) = events.recv().await {
                return;
            }
        }
    };
    let started = tokio::time::timeout(Duration::from_secs(5), session).await;
    let bindings = kit.store().list_bindings(&room).await.unwrap();
    let timeline = kit.store().list_events(&room, &every_row()).await.unwrap();
    assert_eq!(
        (
            started.is_ok(),
            bindings
                .iter()
                .map(|binding| {
                    let correspondent = binding.participant_id.as_ref().map(ToString::to_string);
                    (binding.channel_id.to_string(), correspondent)
                })
                .collect::<Vec<_>>(),
            timeline.len()
        ),
        (
            true,
            vec![("sms".to_owned(), Some("+15551234567".to_owned()))],
            // The attach's lifecycle record alone (gap G-046).
            1
        )
    );
}

/// An SMS channel as a provider is: it delivers to the number its binding
/// names under its declaration's recipient key, `phone_number` by default
/// for its type (RFC Appendix A.1), and records where it sent.
#[derive(Clone)]
struct Sms {
    declaration: ChannelDeclaration,
    sent_to: Arc<Mutex<Vec<String>>>,
}

impl Sms {
    fn new() -> Self {
        Self {
            declaration: ChannelDeclaration::new(
                channel("sms"),
                ChannelType::SMS,
                ChannelCategory::Transport,
                ChannelDirection::Bidirectional,
            ),
            sent_to: Arc::default(),
        }
    }

    fn sent_to(&self) -> Vec<String> {
        self.sent_to
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Channel for Sms {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let to = match binding.metadata.get("phone_number") {
            Some(number) => number.as_str().unwrap_or_default().to_owned(),
            None => String::new(),
        };
        self.sent_to
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(to);
        ready(Ok(ChannelOutput::default()))
    }
}

/// A kit with the SMS channel and an echo agent, and a room the agent is
/// attached to; the SMS channel too, when `attach_sms` names the number its
/// binding delivers to.
async fn sms_room(attach_sms: Option<&str>) -> (RoomKit, Sms) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = Sms::new();
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(EchoAgent::new(
        channel("ai"),
        "You said: ",
        EchoAgentOptions { quote: true },
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room_id(), &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();
    if let Some(number) = attach_sms {
        let recipient = AttachChannelOptions {
            metadata: Metadata::from_iter([("phone_number".to_owned(), number.into())]),
            ..Default::default()
        };
        kit.attach_channel(&room_id(), &channel("sms"), recipient)
            .await
            .unwrap();
    }
    (kit, sms)
}

fn customer(text: &str) -> InboundMessage {
    InboundMessage::new(channel("sms"), "+1 (555) 123-4567", TextContent::new(text))
}

/// RFC §10.1 step 2: with a room id given, only a room step 2 creates
/// records its sender. The SMS channel attached to an existing room names
/// no recipient, and the agent's answer is refused before any send.
#[tokio::test]
async fn an_sms_channel_attached_to_an_existing_room_names_no_recipient_and_sends_nothing() {
    let (kit, sms) = sms_room(None).await;
    kit.process_inbound(
        customer("Bonjour"),
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let bound = kit.store().list_bindings(&room_id()).await.unwrap();
    let sms_binding = bound
        .iter()
        .find(|binding| binding.channel_id == channel("sms"))
        .unwrap();
    assert_eq!(
        (sms_binding.metadata.len(), sms.sent_to()),
        (0, Vec::<String>::new())
    );
}

#[tokio::test]
async fn an_sms_binding_the_integrator_addresses_receives_the_answer() {
    let (kit, sms) = sms_room(Some("+15551234567")).await;
    kit.process_inbound(
        customer("Bonjour"),
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(sms.sent_to(), ["+15551234567"]);
}

/// RFC §5.7, §10.1 step 2: a room step 2 creates addresses its SMS binding
/// to the sender, in E.164, and the agent its hook attaches answers there.
#[tokio::test]
async fn a_room_created_for_an_sms_sender_answers_at_their_number() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = Sms::new();
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(EchoAgent::new(
        channel("ai"),
        "You said: ",
        EchoAgentOptions { quote: true },
    ))
    .unwrap();
    let attaching = kit.clone();
    kit.observe("welcome", HookTrigger::OnRoomCreated, move |event, _| {
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
    })
    .unwrap();
    kit.process_inbound(
        customer("Bonjour"),
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(sms.sent_to(), ["+15551234567"]);
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> roomkit::EventQuery {
    roomkit::EventQuery {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::EventQuery::default()
    }
}
