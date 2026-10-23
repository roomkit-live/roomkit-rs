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
    EventContent, EventId, EventStatus, HookResult, InboundMessage, RoomContext, RoomEvent, RoomId,
    Timestamp,
};
use roomkit::ops::{
    ChannelDeclaration, FrameworkEvent, SequentialEnvironment, default_inbound_event,
};
use roomkit::testing::{EchoAgent, RecordingTransport};
use roomkit::{
    Channel, ChannelError, ConversationStore, EventDraft, HookExecution, HookHandler, HookReturn,
    HookSpec, HookTrigger, KitError, RoomKit, text_of,
};
use tokio::sync::mpsc;

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

fn message(from: &RecordingTransport, text: &str) -> InboundMessage {
    InboundMessage::new(from.id().clone(), "+15551234567", EventContent::text(text))
}

/// A kit with one room, an SMS transport and an echo agent, all attached.
async fn sms_and_agent() -> (RoomKit, RecordingTransport) {
    let kit = RoomKit::new();
    kit.set_environment(SequentialEnvironment::new(
        Timestamp::from_unix_millis(1_792_677_780_000).unwrap(),
    ));
    let sms = RecordingTransport::new(channel("sms"), ChannelType::Sms);
    let agent = EchoAgent::new(channel("ai"), "You said: ").quoting();
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(agent.clone()).unwrap();
    kit.create_room(&room_id()).await.unwrap();
    kit.attach(&room_id(), sms.id()).await.unwrap();
    kit.attach(&room_id(), agent.id()).await.unwrap();
    (kit, sms)
}

async fn timeline(kit: &RoomKit) -> Vec<RoomEvent> {
    kit.store().list_events(&room_id(), true).await.unwrap()
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
    kit.process_inbound(message(&sms, "Bonjour"), Some(&room_id()))
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
    kit.process_inbound(message(&sms, "Hello?"), Some(&room_id()))
        .await
        .unwrap();
    let received = kit.store().list_events(&room_id(), false).await.unwrap();
    assert_eq!((received.len(), timeline(&kit).await.len()), (0, 1));
}

#[tokio::test]
async fn two_messages_sent_together_take_distinct_indexes() {
    let (kit, sms) = sms_and_agent().await;
    let room = room_id();
    let (first, second) = tokio::join!(
        kit.process_inbound(message(&sms, "one"), Some(&room)),
        kit.process_inbound(message(&sms, "two"), Some(&room)),
    );
    first.unwrap();
    second.unwrap();
    let indexes: Vec<u64> = timeline(&kit)
        .await
        .iter()
        .map(|event| event.index)
        .collect();
    assert_eq!(indexes, [0, 1, 2, 3]);
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
    kit.process_inbound(message(&sms, "Bonjour"), Some(&room_id()))
        .await
        .unwrap();
    assert_eq!(next(&mut observed).await, "Bonjour");
}

#[tokio::test]
async fn a_hook_past_its_timeout_lets_the_message_through_and_is_reported() {
    let (kit, sms) = sms_and_agent().await;
    let mut events = kit.subscribe();
    let spec = HookSpec {
        timeout: Duration::from_millis(20),
        ..HookSpec::new(
            "slow_scan",
            HookTrigger::BeforeBroadcast,
            HookExecution::Sync,
        )
    };
    kit.hook(spec, |_, _| async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        Ok(HookResult::allow())
    })
    .unwrap();
    let result = kit
        .process_inbound(message(&sms, "Bonjour"), Some(&room_id()))
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
        .process_inbound(message(&sms, "Cheap spam here"), Some(&room_id()))
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
        .process_inbound(message(&sms, "Not a scam"), Some(&room_id()))
        .await
        .unwrap();
    assert_eq!(result.reason.as_deref(), Some("blocklisted \"scam\""));
}

#[tokio::test]
async fn a_closed_room_refuses_the_next_message() {
    let (kit, sms) = sms_and_agent().await;
    kit.close_room(&room_id()).await.unwrap();
    let result = kit
        .process_inbound(message(&sms, "Hello?"), Some(&room_id()))
        .await
        .unwrap();
    assert_eq!(result.reason.as_deref(), Some("room_closed"));
}

#[tokio::test]
async fn a_message_on_an_unregistered_channel_is_refused() {
    let (kit, _sms) = sms_and_agent().await;
    let stranger = RecordingTransport::new(channel("telegram"), ChannelType::Telegram);
    let refused = kit
        .process_inbound(message(&stranger, "Hi"), Some(&room_id()))
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
            ChannelType::Whatsapp,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        ),
    })
    .unwrap();
    kit.attach(&room_id(), &channel("whatsapp")).await.unwrap();
    let mut ids = Vec::new();
    for text in ["one", "two"] {
        let sent = InboundMessage::new(
            channel("whatsapp"),
            "+15551234567",
            EventContent::text(text),
        );
        let result = kit.process_inbound(sent, Some(&room_id())).await.unwrap();
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
            ChannelType::Webhook,
            ChannelCategory::Transport,
            ChannelDirection::Outbound,
        ),
    })
    .unwrap();
    kit.attach(&room_id(), &channel("broken")).await.unwrap();
    let result = kit
        .process_inbound(message(&sms, "Bonjour"), Some(&room_id()))
        .await
        .unwrap();
    let broken = result
        .delivery_results
        .get("broken")
        .map(|delivery| delivery.status);
    assert_eq!(
        (broken, sms.texts().len()),
        (Some(roomkit::model::DeliveryStatus::Failed), 1)
    );
}

#[tokio::test]
async fn a_muted_agent_is_asked_and_its_answer_stored_blocked() {
    let kit = RoomKit::new();
    let sms = RecordingTransport::new(channel("sms"), ChannelType::Sms);
    let agent = EchoAgent::new(channel("ai"), "muted answer");
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(agent.clone()).unwrap();
    kit.create_room(&room_id()).await.unwrap();
    kit.attach(&room_id(), sms.id()).await.unwrap();
    kit.attach_with(&room_id(), agent.id(), |binding| binding.muted = true)
        .await
        .unwrap();
    kit.process_inbound(message(&sms, "Bonjour"), Some(&room_id()))
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
async fn configuring_a_binding_cannot_move_it_to_another_room() {
    let kit = RoomKit::new();
    let sms = RecordingTransport::new(channel("sms"), ChannelType::Sms);
    kit.register_channel(sms.clone()).unwrap();
    let other = RoomId::new("room_2").unwrap();
    kit.create_room(&room_id()).await.unwrap();
    kit.create_room(&other).await.unwrap();
    kit.attach_with(&room_id(), sms.id(), |binding| {
        binding.room_id = other.clone();
    })
    .await
    .unwrap();
    let bound = (
        kit.store().list_bindings(&room_id()).await.unwrap().len(),
        kit.store().list_bindings(&other).await.unwrap().len(),
    );
    assert_eq!(bound, (1, 0));
}

#[tokio::test]
async fn registering_a_channel_is_announced_to_subscribers() {
    let kit = RoomKit::new();
    let mut events = kit.subscribe();
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::Sms))
        .unwrap();
    assert_eq!(
        events.recv().await.unwrap(),
        FrameworkEvent::ChannelRegistered {
            channel_id: channel("sms"),
            channel_type: ChannelType::Sms,
        }
    );
}

#[tokio::test]
async fn registering_a_channel_twice_is_refused() {
    let kit = RoomKit::new();
    let sms = RecordingTransport::new(channel("sms"), ChannelType::Sms);
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
        let sms = RecordingTransport::new(channel("sms"), ChannelType::Sms);
        clone
            .process_inbound(message(&sms, "from a task"), Some(&room_id()))
            .await
            .unwrap();
    });
    task.await.unwrap();
    assert_eq!(sms.texts(), ["You said: from a task"]);
}

#[tokio::test]
async fn kits_do_not_share_their_store() {
    let (_kit, _sms) = sms_and_agent().await;
    let other = RoomKit::new();
    assert_eq!(other.store().get_room(&room_id()).await.unwrap(), None);
}

#[test]
fn a_kit_can_be_shared_across_tasks_and_threads() {
    fn shareable<T: Send + Sync + 'static>() {}
    shareable::<RoomKit>();
}
