//! The kit end to end, on the tokio runtime: the RFC's Appendix B flows, and
//! what only a real driver shows (concurrency, observers, timeouts, a channel
//! that panics).

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a test fails on a broken fixture"
)]

use std::future::{Future, ready};
use std::pin::Pin;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventId, EventStatus, HookResult, InboundMessage, RoomContext, RoomEvent, RoomId, TextContent,
    Timestamp,
};
use roomkit::testing::{
    EchoAgent, RecordingTransport, SequentialEnvironment, kit_with_environment,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, CloseRoomOptions,
    ConversationStore, CreateRoomOptions, EventDraft, FrameworkEvent, HookExecution, HookHandler,
    HookReturn, HookSpec, HookTrigger, KitError, ProcessInboundOptions, RoomKit, RoomKitOptions,
    default_inbound_event, text_of,
};
use tokio::sync::mpsc;

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

fn message(from: &RecordingTransport, text: &str) -> InboundMessage {
    InboundMessage::new(from.id().clone(), "+15551234567", TextContent::new(text))
}

/// A kit with one room, an SMS transport and an echo agent, all attached.
async fn sms_and_agent() -> (RoomKit, RecordingTransport) {
    let kit = kit_with_environment(
        RoomKitOptions::default(),
        SequentialEnvironment::new(Timestamp::from_unix_millis(1_792_677_780_000).unwrap()),
    );
    let sms = RecordingTransport::new(channel("sms"), ChannelType::SMS);
    let agent = EchoAgent::new(channel("ai"), "You said: ").quoting();
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(agent.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room_id(), sms.id(), AttachChannelOptions::default())
        .await
        .unwrap();
    kit.attach_channel(&room_id(), agent.id(), AttachChannelOptions::default())
        .await
        .unwrap();
    (kit, sms)
}

async fn timeline(kit: &RoomKit) -> Vec<RoomEvent> {
    kit.store()
        .list_events(&room_id(), &every_row())
        .await
        .unwrap()
}

/// Waits for the next value an observer sends, or fails after a second.
async fn next<T>(receiver: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn an_agent_s_answer_reaches_the_customer() {
    let (kit, sms) = sms_and_agent().await;
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
async fn the_timeline_reads_without_the_refused_events_by_default() {
    let (kit, sms) = sms_and_agent().await;
    kit.before_broadcast("refuse_all", |event, _| {
        let refuse = event.source.channel_id.as_str() == "sms";
        async move {
            Ok(if refuse {
                HookResult::block("closed for the night")
            } else {
                HookResult::allow()
            })
        }
    })
    .unwrap();
    kit.process_inbound(
        message(&sms, "Hello?"),
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let received = kit
        .store()
        .list_events(&room_id(), &received())
        .await
        .unwrap();
    // The two attaches' lifecycle records are received (gap G-046); the
    // refused message is read only when asked for.
    assert_eq!((received.len(), timeline(&kit).await.len()), (2, 3));
}

#[tokio::test]
async fn two_messages_sent_together_take_distinct_indexes() {
    let (kit, sms) = sms_and_agent().await;
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
    let indexes: Vec<u64> = timeline(&kit)
        .await
        .iter()
        .map(|event| event.index)
        .collect();
    // After the two attaches' lifecycle records (gap G-046).
    assert_eq!(indexes, [0, 1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn an_after_broadcast_observer_runs_after_the_delivery() {
    let (kit, sms) = sms_and_agent().await;
    let (seen, mut observed) = mpsc::unbounded_channel();
    kit.observe("audit", HookTrigger::AfterBroadcast, move |event, _| {
        let seen = seen.clone();
        async move {
            seen.send(text_of(&event.content)).unwrap();
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
    assert_eq!(next(&mut observed).await, "Bonjour");
}

/// RFC §9.2, §9.3: a hook registered ASYNC on `BEFORE_BROADCAST` observes
/// the event the SYNC chain allowed, as its hooks left it.
#[tokio::test]
async fn an_async_hook_on_before_broadcast_observes_the_payload_the_chain_left() {
    let (kit, sms) = sms_and_agent().await;
    kit.before_broadcast("rewrite", |event, _| {
        let mut rewritten = event;
        rewritten.content = TextContent::new("rewritten").into();
        ready(Ok(HookResult::modify(rewritten)))
    })
    .unwrap();
    let (seen, mut observed) = mpsc::unbounded_channel();
    kit.observe("watcher", HookTrigger::BeforeBroadcast, move |event, _| {
        let seen = seen.clone();
        async move {
            seen.send(text_of(&event.content)).unwrap();
        }
    })
    .unwrap();
    kit.process_inbound(
        message(&sms, "original"),
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(next(&mut observed).await, "rewritten");
}

#[tokio::test]
async fn a_hook_past_its_timeout_lets_the_message_through_and_is_reported() {
    let (kit, sms) = sms_and_agent().await;
    let mut events = kit.subscribe();
    let mut spec = HookSpec::new(
        "slow_scan",
        HookTrigger::BeforeBroadcast,
        HookExecution::Sync,
    );
    spec.timeout = Duration::from_millis(20);
    kit.hook(spec, |_, _| async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        Ok(HookResult::allow())
    })
    .unwrap();
    let result = kit
        .process_inbound(
            message(&sms, "Bonjour"),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut timed_out = false;
    while let Ok(event) = events.try_recv() {
        timed_out |= matches!(event, FrameworkEvent::HookTimeout { .. });
    }
    assert_eq!((result.blocked, timed_out), (false, true));
}

/// RFC §9.1: `hook` types its closure itself, so the closure reads its
/// event without annotations.
#[tokio::test]
async fn a_hook_s_closure_reads_its_event_without_annotations() {
    let (kit, sms) = sms_and_agent().await;
    let spec = HookSpec::new(
        "spam_filter",
        HookTrigger::BeforeBroadcast,
        HookExecution::Sync,
    );
    kit.hook(spec, |event, _| {
        let spam = text_of(&event.content).contains("spam");
        async move {
            Ok(if spam {
                HookResult::block("spam")
            } else {
                HookResult::allow()
            })
        }
    })
    .unwrap();
    let result = kit
        .process_inbound(
            message(&sms, "Cheap spam here"),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.reason.as_deref(), Some("spam"));
}

/// A hook written as a type of its own, which blocks a message naming its
/// word.
struct Blocklist(&'static str);

impl HookHandler for Blocklist {
    fn call(
        &self,
        event: RoomEvent,
        _context: RoomContext,
    ) -> Pin<Box<dyn Future<Output = HookReturn> + Send>> {
        let named = text_of(&event.content).contains(self.0);
        let reason = format!("blocklisted {:?}", self.0);
        Box::pin(async move {
            Ok(if named {
                HookResult::block(reason)
            } else {
                HookResult::allow()
            })
        })
    }
}

#[tokio::test]
async fn a_hook_handler_of_its_own_type_is_registered_with_hook_handler() {
    let (kit, sms) = sms_and_agent().await;
    let spec = HookSpec::new(
        "blocklist",
        HookTrigger::BeforeBroadcast,
        HookExecution::Sync,
    );
    kit.hook_handler(spec, Blocklist("scam")).unwrap();
    let result = kit
        .process_inbound(
            message(&sms, "Not a scam"),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.reason.as_deref(), Some("blocklisted \"scam\""));
}

#[tokio::test]
async fn a_closed_room_refuses_the_next_message() {
    let (kit, sms) = sms_and_agent().await;
    kit.close_room(&room_id(), CloseRoomOptions::default())
        .await
        .unwrap();
    let result = kit
        .process_inbound(
            message(&sms, "Hello?"),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.reason.as_deref(), Some("room_closed"));
}

#[tokio::test]
async fn a_message_on_an_unregistered_channel_is_refused() {
    let (kit, _sms) = sms_and_agent().await;
    let stranger = RecordingTransport::new(channel("telegram"), ChannelType::TELEGRAM);
    let refused = kit
        .process_inbound(
            message(&stranger, "Hi"),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(refused, Err(KitError::Inbound(_))));
}

/// A transport whose deliveries panic.
struct Panicking {
    declaration: ChannelDeclaration,
}

impl Channel for Panicking {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        panic!("the provider SDK panicked");
        #[expect(
            unreachable_code,
            reason = "the future type is named by its last expression"
        )]
        ready(Ok(ChannelOutput::default()))
    }
}

/// A transport that parses every message under its provider's id.
struct ProviderIds {
    declaration: ChannelDeclaration,
}

impl Channel for ProviderIds {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        _context: RoomContext,
        draft: EventDraft,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        let event = default_inbound_event(
            &message,
            &self.declaration,
            draft.room_id,
            EventId::new("SM1").unwrap(),
            draft.now,
        );
        ready(Ok(event))
    }
}

#[tokio::test]
async fn an_inbound_event_is_committed_under_an_id_of_the_pipeline() {
    let (kit, _) = sms_and_agent().await;
    kit.register_channel(ProviderIds {
        declaration: ChannelDeclaration::new(
            channel("whatsapp"),
            ChannelType::WHATSAPP,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        ),
    })
    .unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("whatsapp"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let mut ids = Vec::new();
    for text in ["one", "two"] {
        let sent = InboundMessage::new(channel("whatsapp"), "+15551234567", TextContent::new(text));
        let result = kit
            .process_inbound(
                sent,
                ProcessInboundOptions {
                    room_id: Some(room_id()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        ids.push(result.event.unwrap().id.to_string());
    }
    assert!(
        ids[0] != ids[1] && !ids.contains(&"SM1".to_owned()),
        "{ids:?}"
    );
}

#[tokio::test]
async fn a_channel_that_panics_fails_its_delivery_and_the_kit_goes_on() {
    let (kit, sms) = sms_and_agent().await;
    kit.register_channel(Panicking {
        declaration: ChannelDeclaration::new(
            channel("broken"),
            ChannelType::WEBHOOK,
            ChannelCategory::Transport,
            ChannelDirection::Outbound,
        ),
    })
    .unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("broken"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let result = kit
        .process_inbound(
            message(&sms, "Bonjour"),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let broken = result
        .delivery_results
        .get("broken")
        .map(|delivery| delivery.status);
    assert_eq!(
        (broken, sms.texts().len()),
        (Some(roomkit::model::DeliveryResultStatus::Failed), 1)
    );
}

#[tokio::test]
async fn a_muted_agent_is_asked_and_its_answer_stored_blocked() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(channel("sms"), ChannelType::SMS);
    let agent = EchoAgent::new(channel("ai"), "muted answer");
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(agent.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room_id(), sms.id(), AttachChannelOptions::default())
        .await
        .unwrap();
    let muted = AttachChannelOptions {
        muted: true,
        ..Default::default()
    };
    kit.attach_channel(&room_id(), agent.id(), muted)
        .await
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
    let answer = timeline(&kit).await.pop().unwrap();
    assert_eq!(
        (
            answer.status,
            answer.blocked_by.as_deref(),
            sms.texts().len()
        ),
        (EventStatus::Blocked, Some("source_muted"), 0)
    );
}

#[tokio::test]
async fn registering_a_channel_is_announced_to_subscribers() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let mut events = kit.subscribe();
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
        .unwrap();
    assert_eq!(
        events.recv().await.unwrap(),
        FrameworkEvent::channel_registered(channel("sms"), ChannelType::SMS)
    );
}

#[tokio::test]
async fn registering_a_channel_twice_is_refused() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(channel("sms"), ChannelType::SMS);
    kit.register_channel(sms.clone()).unwrap();
    assert!(matches!(
        kit.register_channel(sms),
        Err(KitError::Config(_))
    ));
}

#[tokio::test]
async fn a_kit_is_shared_by_its_clones() {
    let (kit, sms) = sms_and_agent().await;
    let clone = kit.clone();
    let task = tokio::spawn(async move {
        let sms = RecordingTransport::new(channel("sms"), ChannelType::SMS);
        clone
            .process_inbound(
                message(&sms, "from a task"),
                ProcessInboundOptions {
                    room_id: Some(room_id()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    });
    task.await.unwrap();
    assert_eq!(sms.texts(), ["You said: from a task"]);
}

#[tokio::test]
async fn kits_do_not_share_their_store() {
    let (_kit, _sms) = sms_and_agent().await;
    let other = RoomKit::new(RoomKitOptions::default());
    assert_eq!(other.store().get_room(&room_id()).await.unwrap(), None);
}

#[test]
fn a_kit_can_be_shared_across_tasks_and_threads() {
    fn shareable<T: Send + Sync + 'static>() {}
    shareable::<RoomKit>();
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

/// Every event of a room's timeline the room received.
fn received() -> roomkit::EventQuery {
    roomkit::EventQuery {
        limit: u64::MAX,
        ..roomkit::EventQuery::default()
    }
}
