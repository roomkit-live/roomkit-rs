//! The kit end to end, on the tokio runtime: the RFC's Appendix B flows, and
//! what only a real driver shows (concurrency, observers, timeouts, a channel
//! that panics).

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a test fails on a broken fixture"
)]
use std::future::{Future, ready};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::{AllowOptions, BlockOptions, ModifyOptions};
use roomkit::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
    ChannelMediaType, ChannelOutput, ChannelType, CompositeContent, DeliveryError,
    DeliveryResultStatus, EditContent, EventContent, EventDirection, EventId, EventSource,
    EventStatus, HookResult, InboundMessage, InjectedEvent, MediaContent, RateLimit, RetryPolicy,
    RoomContext, RoomEvent, RoomId, TextContent, Timestamp,
};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, EventTap, RecordingTransport, RecordingTransportOptions,
    SequentialEnvironment, kit_with_environment,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CloseRoomOptions, CommitEventOptions,
    ConversationStore, CreateRoomOptions, HookExecution, HookFuture, HookHandler, HookOptions,
    HookTrigger, PostOptions, ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions,
    SendEventOptions, UpdateEventOptions, WeakRoomKit, default_inbound_event,
};
use roomkit_core::ops::ChannelConfig;
use roomkit_core::rules::transcoding::text_of;
use serde_json::{Value, json};
use tokio::sync::mpsc;

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

/// A kit with one room, an SMS transport and an echo agent, all attached.
async fn sms_and_agent() -> (RoomKit, RecordingTransport) {
    let kit = kit_with_environment(
        RoomKitOptions::default(),
        SequentialEnvironment::new(Timestamp::from_unix_millis(1_792_677_780_000).unwrap()),
    );
    let sms = RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    let agent = EchoAgent::new(
        channel("ai"),
        "You said: ",
        EchoAgentOptions { quote: true },
    );
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(agent.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(
        &room_id(),
        sms.channel_id(),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    kit.attach_channel(
        &room_id(),
        agent.channel_id(),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    (kit, sms)
}

async fn timeline(kit: &RoomKit) -> Vec<RoomEvent> {
    kit.store()
        .list_events(&room_id(), every_row())
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

/// Python's `InboundResult.duplicate`, `unavailable_targets` and
/// `response_events` (probe `tasks/probes/step5-lot2/inbound_result_fields.py`):
/// a replay of a key is a duplicate with no plan resolved; the addresses
/// the committed plan asks no turn of, an agent registered but not
/// attached and a channel never registered, in the order the message names
/// them; the answers the call's cascade delivered, never one stored
/// blocked.
#[tokio::test]
async fn an_inbound_result_reports_its_replay_answers_and_unreached_addresses() {
    let (kit, sms) = sms_and_agent().await;
    kit.register_channel(EchoAgent::new(
        channel("idle"),
        "never",
        EchoAgentOptions::default(),
    ))
    .unwrap();
    let routed = || ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };
    let sent = |text: &str, key: Option<&str>, addressed_to: Option<&[&str]>| {
        let mut message = message(&sms, text);
        message.idempotency_key = key.map(str::to_owned);
        message.addressed_to = addressed_to.map(|ids| ids.iter().map(|id| channel(id)).collect());
        message
    };
    let read = |result: roomkit::InboundResult| {
        let answers: Vec<String> = result
            .response_events
            .iter()
            .map(|event| text_of(&event.content))
            .collect();
        let unavailable = result
            .unavailable_targets
            .map(|targets| targets.iter().map(ToString::to_string).collect::<Vec<_>>());
        (result.duplicate, unavailable, answers)
    };
    let first = kit
        .process_inbound(sent("hi", Some("k1"), None), routed())
        .await
        .unwrap();
    let replay = kit
        .process_inbound(sent("hi", Some("k1"), None), routed())
        .await
        .unwrap();
    let addressed = kit
        .process_inbound(
            sent("you two", None, Some(&["ai", "idle", "ghost"])),
            routed(),
        )
        .await
        .unwrap();
    kit.mute(&room_id(), &channel("ai"), roomkit::MuteOptions::default())
        .await
        .unwrap();
    let muted = kit
        .process_inbound(sent("muted?", None, Some(&["ai"])), routed())
        .await
        .unwrap();
    assert_eq!(
        [read(first), read(replay), read(addressed), read(muted)],
        [
            (false, Some(vec![]), vec!["You said: hi".to_owned()]),
            (true, None, vec![]),
            (
                false,
                Some(vec!["idle".to_owned(), "ghost".to_owned()]),
                vec!["You said: you two".to_owned()]
            ),
            (false, Some(vec![]), vec![]),
        ]
    );
}

/// A transport that unregisters the channel `ai` when it is told of an
/// event: the agent's turn, owed by the plan the commit made, is asked
/// after it, and never taken.
#[derive(Default)]
struct Unregistering {
    kit: Arc<OnceLock<WeakRoomKit>>,
}

impl Unregistering {
    /// The configuration the kit keeps of the channel.
    fn config() -> ChannelConfig {
        ChannelConfig::new(
            channel("gate"),
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        )
    }
}

impl Channel for Unregistering {
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

    fn capabilities(&self) -> ChannelCapabilities {
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

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = Self::config();
        let addressing = roomkit::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: false,
        };
        (addressing, config.country_code.clone())
    }

    fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        if let Some(kit) = self.kit.get().and_then(WeakRoomKit::upgrade) {
            kit.unregister_channel(&channel("ai")).unwrap();
        }
        ready(Ok(ChannelOutput::default()))
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(default_inbound_event(self, &message, &context)))
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

/// Python's `_report_root_pass` refreshes `unavailable_targets` once the
/// trigger's delivery set is over (`lane_execution.py`,
/// `_unavailable_targets`; probe
/// `tasks/probes/step5-lot2-review-A/not_ported_inbound.py`): an addressed
/// agent the committed plan held, unregistered before its turn, reads as
/// unavailable, its delivery result failed `channel_not_registered`, not
/// retryable, as Python's `_delivery_error` writes it.
#[tokio::test]
async fn an_addressed_agent_unregistered_before_its_turn_reads_as_unavailable() {
    let (kit, sms) = sms_and_agent().await;
    let gate = Unregistering::default();
    gate.kit.set(kit.downgrade()).unwrap();
    kit.register_channel(gate).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("gate"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let mut addressed = message(&sms, "hi");
    addressed.addressed_to = Some(vec![channel("ai")]);
    let routed = ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };
    let result = kit.process_inbound(addressed, routed).await.unwrap();
    let failed = result
        .delivery_results
        .get(&channel("ai"))
        .map(|delivered| (delivered.status, delivered.error.clone()));
    assert_eq!(
        (result.unavailable_targets, failed),
        (
            Some(vec![channel("ai")]),
            Some((
                DeliveryResultStatus::Failed,
                Some(DeliveryError {
                    code: "channel_not_registered".to_owned(),
                    message: "channel_not_registered".to_owned(),
                    retryable: false,
                })
            ))
        )
    );
}

#[tokio::test]
async fn the_timeline_reads_without_the_refused_events_by_default() {
    let (kit, sms) = sms_and_agent().await;
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |event: RoomEvent, _| {
            let refuse = event.source.channel_id.as_str() == "sms";
            async move {
                Ok(if refuse {
                    HookResult::block("closed for the night", BlockOptions::default())
                } else {
                    HookResult::allow(AllowOptions::default())
                })
            }
        },
        HookOptions {
            name: "refuse_all".to_owned(),
            ..Default::default()
        },
    )
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
        .list_events(&room_id(), received())
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
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |event: RoomEvent, _| {
            let seen = seen.clone();
            async move {
                seen.send(text_of(&event.content)).unwrap();
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "audit".to_owned(),
            ..Default::default()
        },
    )
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

/// RFC §8.2: an inbound event's `event_processed` follows its
/// `AFTER_BROADCAST` hooks, once they are over, as the Python
/// implementation emits it (RMK-1020).
#[tokio::test]
async fn event_processed_follows_the_after_broadcast_hooks() {
    let (kit, sms) = sms_and_agent().await;
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let release = Arc::new(tokio::sync::Notify::new());
    let (hooked, released) = (Arc::clone(&order), Arc::clone(&release));
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |event: RoomEvent, _| {
            let (hooked, released) = (Arc::clone(&hooked), Arc::clone(&released));
            async move {
                if event.source.channel_id == channel("sms") {
                    released.notified().await;
                    hooked.lock().unwrap().push("after_broadcast");
                }
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "audit".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let noted = Arc::clone(&order);
    kit.on("event_processed", move |_| {
        noted.lock().unwrap().push("event_processed");
        ready(())
    })
    .unwrap();
    kit.process_inbound(
        message(&sms, "Hello"),
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    release.notify_one();
    roomkit::testing::idle(&kit).await;
    assert_eq!(
        order.lock().unwrap().clone(),
        ["after_broadcast", "event_processed"]
    );
    kit.close().await.unwrap();
}

/// RFC §9.2, §9.3: a hook registered ASYNC on `BEFORE_BROADCAST` observes
/// the event the SYNC chain allowed, as its hooks left it.
#[tokio::test]
async fn an_async_hook_on_before_broadcast_observes_the_payload_the_chain_left() {
    let (kit, sms) = sms_and_agent().await;
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |event: RoomEvent, _| {
            let mut rewritten = event;
            rewritten.content = TextContent::new("rewritten").into();
            ready(Ok(HookResult::modify(rewritten, ModifyOptions::default())))
        },
        HookOptions {
            name: "rewrite".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let (seen, mut observed) = mpsc::unbounded_channel();
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let seen = seen.clone();
            async move {
                seen.send(text_of(&event.content)).unwrap();
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "watcher".to_owned(),
            ..Default::default()
        },
    )
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
    let tap = EventTap::on(&kit, &["hook_timeout"]);
    let spec = HookOptions {
        name: "slow_scan".to_owned(),
        timeout: Duration::from_millis(20),
        ..Default::default()
    };
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |_: RoomEvent, _| async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok(HookResult::allow(AllowOptions::default()))
        },
        spec,
    )
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
    roomkit::testing::idle(&kit).await;
    let timed_out = !tap.events().is_empty();
    assert_eq!((result.blocked, timed_out), (false, true));
}

/// RFC §9.1: `hook` types its closure itself, so the closure reads its
/// event without annotations.
#[tokio::test]
async fn a_hook_s_closure_reads_its_event_without_annotations() {
    let (kit, sms) = sms_and_agent().await;
    let spec = HookOptions {
        name: "spam_filter".to_owned(),
        ..Default::default()
    };
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |event: RoomEvent, _| {
            let spam = text_of(&event.content).contains("spam");
            async move {
                Ok(if spam {
                    HookResult::block("spam", BlockOptions::default())
                } else {
                    HookResult::allow(AllowOptions::default())
                })
            }
        },
        spec,
    )
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
    fn call(&self, event: RoomEvent, _context: RoomContext) -> HookFuture {
        let named = text_of(&event.content).contains(self.0);
        let reason = format!("blocklisted {:?}", self.0);
        Box::pin(async move {
            Ok(Some(if named {
                HookResult::block(reason, BlockOptions::default())
            } else {
                HookResult::allow(AllowOptions::default())
            }))
        })
    }
}

#[tokio::test]
async fn a_hook_handler_of_its_own_type_is_registered_with_hook_handler() {
    let (kit, sms) = sms_and_agent().await;
    let spec = HookOptions {
        name: "blocklist".to_owned(),
        ..Default::default()
    };
    kit.hook_handler(HookTrigger::BeforeBroadcast, Blocklist("scam"), spec)
        .unwrap();
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

/// RFC §9.3: a SYNC hook that returns nothing, Python's `None`, gives an
/// unusable result: the message goes through, unless the hook fails
/// closed, which blocks it naming the hook.
#[tokio::test]
async fn a_sync_hook_returning_nothing_fails_open_unless_it_fails_closed() {
    let mut outcomes = Vec::new();
    for fail_closed in [false, true] {
        let (kit, sms) = sms_and_agent().await;
        let silent = HookOptions {
            name: "silent".to_owned(),
            fail_closed,
            ..Default::default()
        };
        kit.hook(
            HookTrigger::BeforeBroadcast,
            |_: RoomEvent, _| async {},
            silent,
        )
        .unwrap();
        let result = kit
            .process_inbound(
                message(&sms, "Hello"),
                ProcessInboundOptions {
                    room_id: Some(room_id()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        outcomes.push((result.blocked, result.reason));
        kit.close().await.unwrap();
    }
    assert_eq!(
        outcomes,
        [
            (false, None),
            (true, Some("hook_invalid_result:silent".to_owned()))
        ]
    );
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
    let stranger = RecordingTransport::new(
        channel("telegram"),
        ChannelType::TELEGRAM,
        RecordingTransportOptions::default(),
    );
    let refused = kit
        .process_inbound(
            message(&stranger, "Hi"),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(
        refused.err(),
        Some(RoomKitError::ChannelNotRegistered {
            channel_id: channel("telegram"),
            message: "Channel telegram not registered".to_owned(),
        })
    );
}

/// A transport whose deliveries panic.
struct Panicking {
    declaration: ChannelConfig,
}

impl Channel for Panicking {
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

    fn capabilities(&self) -> ChannelCapabilities {
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

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = &self.declaration;
        let addressing = roomkit::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: false,
        };
        (addressing, config.country_code.clone())
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

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(default_inbound_event(self, &message, &context)))
    }
}

/// A transport that parses every message under its provider's id.
struct ProviderIds {
    declaration: ChannelConfig,
}

impl Channel for ProviderIds {
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

    fn capabilities(&self) -> ChannelCapabilities {
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

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = &self.declaration;
        let addressing = roomkit::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: false,
        };
        (addressing, config.country_code.clone())
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        let event = default_inbound_event(self, &message, &context);
        ready(Ok(event))
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

#[tokio::test]
async fn an_inbound_event_is_committed_under_an_id_of_the_pipeline() {
    let (kit, _) = sms_and_agent().await;
    kit.register_channel(ProviderIds {
        declaration: ChannelConfig::new(
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
        declaration: ChannelConfig::new(
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
        (Some(DeliveryResultStatus::Failed), 1)
    );
}

/// A media its model refuses, which a struct literal can hold past the
/// check `MediaContent::new` runs (RFC §6.4).
fn javascript_media() -> EventContent {
    MediaContent {
        url: "javascript:alert(1)".to_owned(),
        ..MediaContent::new("https://example.com/a.png", "image/png").unwrap()
    }
    .into()
}

/// Python's message refusing `javascript_media`.
const JAVASCRIPT_REFUSED: &str = "URL must be a link (http://, https://), a data: URI, or a \
     reference <scheme>://... (two letters of scheme at least, not file, ftp, javascript or \
     vbscript, no '..' segment and no backslash): \"javascript:alert(1)\"";

/// A content its model refuses, which a struct literal can hold, is
/// refused by each of the kit's calls that would write it, as Python's
/// `ValueError` when its model is built: before anything is read, so a
/// missing room is not even looked for, and nothing written. The content
/// itself, among a composite's parts or as an edit's new content at
/// `send_event`, and at `post`, `process_inbound`, `commit_event` and
/// `update_event`. Stored, it would fail every later read of the room.
#[tokio::test]
async fn a_content_its_model_refuses_is_refused_by_every_call_before_any_read() {
    let (kit, sms) = sms_and_agent().await;
    let before = timeline(&kit).await;
    let ghost = RoomId::new("ghost").unwrap();
    let mut record = before[0].clone();
    record.room_id = ghost.clone();
    record.content = javascript_media();
    let mut message = message(&sms, "x");
    message.content = javascript_media();
    let updated = UpdateEventOptions {
        content: Some(javascript_media()),
        ..Default::default()
    };
    let routed = ProcessInboundOptions {
        room_id: Some(ghost.clone()),
        ..Default::default()
    };
    let composite: EventContent =
        CompositeContent::new(vec![TextContent::new("see").into(), javascript_media()])
            .unwrap()
            .into();
    let edit_to_it: EventContent =
        EditContent::new(before[0].id.clone(), javascript_media()).into();
    let sent = |content: EventContent| {
        kit.send_event(
            &ghost,
            sms.channel_id(),
            content,
            SendEventOptions::default(),
        )
    };
    let refused = [
        sent(javascript_media()).await.map(drop),
        sent(composite).await.map(drop),
        sent(edit_to_it).await.map(drop),
        kit.post(&ghost, javascript_media(), PostOptions::default())
            .await
            .map(drop),
        kit.process_inbound(message, routed).await.map(drop),
        kit.commit_event(&ghost, record, CommitEventOptions::default())
            .await
            .map(drop),
        kit.update_event(&ghost, &before[0].id, updated)
            .await
            .map(drop),
    ];
    let media = || Err(RoomKitError::Value(JAVASCRIPT_REFUSED.to_owned()));
    assert_eq!(
        (refused, timeline(&kit).await == before, rooms(&kit).await),
        (
            [
                media(),
                media(),
                media(),
                media(),
                media(),
                media(),
                media()
            ],
            true,
            1
        )
    );
}

/// A binding's value its model refuses, which a struct literal can hold,
/// is refused as Python's model refuses it when built, before anything is
/// read: a rate limit or a retry policy not above 0 at `attach_channel`,
/// and a limit of 0 in the capabilities a channel `register_channel` is
/// given declares. Stored, it would fail every later read of the room's
/// bindings.
#[tokio::test]
async fn a_binding_value_its_model_refuses_is_refused_before_any_read() {
    let (kit, _sms) = sms_and_agent().await;
    let ghost = RoomId::new("ghost").unwrap();
    let limited = AttachChannelOptions {
        rate_limit: Some(RateLimit {
            max_per_minute: Some(-1.0),
            ..RateLimit::default()
        }),
        ..Default::default()
    };
    let retried = AttachChannelOptions {
        retry_policy: Some(RetryPolicy {
            base_delay_seconds: 0.0,
            ..RetryPolicy::default()
        }),
        ..Default::default()
    };
    let unbounded = RecordingTransport::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions {
            capabilities: Some(ChannelCapabilities {
                max_length: Some(0),
                ..ChannelCapabilities::default()
            }),
        },
    );
    let refused = [
        kit.attach_channel(&ghost, &channel("ai"), limited)
            .await
            .map(drop),
        kit.attach_channel(&ghost, &channel("ai"), retried)
            .await
            .map(drop),
        kit.register_channel(unbounded),
    ];
    let positive = |name: &str, value: &str| {
        Err(RoomKitError::Value(format!(
            "{name}: Input should be greater than 0 [input_value={value}]"
        )))
    };
    assert_eq!(
        (
            refused,
            rooms(&kit).await,
            kit.get_channel(&channel("ws")).is_some()
        ),
        (
            [
                positive("max_per_minute", "-1.0"),
                positive("base_delay_seconds", "0.0"),
                positive("max_length", "0"),
            ],
            1,
            false
        )
    );
}

/// How many rooms the kit's store holds.
async fn rooms(kit: &RoomKit) -> usize {
    kit.store()
        .list_rooms(roomkit::store::ListRoomsOptions::default())
        .await
        .unwrap()
        .len()
}

/// A media whose link's scheme a struct literal wrote in capitals, past
/// the lower case `MediaContent::new` writes.
fn shouting_media() -> EventContent {
    MediaContent {
        url: "HTTPS://Example.com/a.png".to_owned(),
        ..MediaContent::new("https://example.com/a.png", "image/png").unwrap()
    }
    .into()
}

/// The URL of the media `content` is.
fn media_url(content: &EventContent) -> Option<String> {
    if let EventContent::Media(media) = content {
        Some(media.url.clone())
    } else {
        None
    }
}

/// A link's scheme a struct literal holds in capitals enters the room in
/// lower case at each of the kit's calls, as Python's model writes it when
/// built: what a `BEFORE_BROADCAST` hook reads of `process_inbound`'s
/// message, and what `send_event`, `post` and `commit_event` return. So it
/// does at every other door, what a transport is delivered telling: a
/// channel's parse, a hook's replacement and the event it injects, an
/// agent's answer (light review A, `door_rewrite_probe.rs`).
#[tokio::test]
async fn a_link_s_scheme_a_struct_literal_holds_enters_in_lower_case() {
    let doors = ["shout", "modify", "inject", "answer"];
    let mut delivered = Vec::new();
    for door in doors {
        delivered.push((door, delivered_through(door).await));
    }
    let lower = || Some("https://Example.com/a.png".to_owned());
    assert_eq!(delivered, doors.map(|door| (door, vec![lower()])));
    let (kit, sms) = sms_and_agent().await;
    let (read, mut hook_read) = mpsc::unbounded_channel();
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _context| {
            read.send(media_url(&event.content)).unwrap();
            ready(Ok(HookResult::allow(AllowOptions::default())))
        },
        HookOptions {
            name: "media_reader".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let mut inbound = message(&sms, "x");
    inbound.content = shouting_media();
    let routed = ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };
    kit.process_inbound(inbound, routed).await.unwrap();
    let sent = kit
        .send_event(
            &room_id(),
            sms.channel_id(),
            shouting_media(),
            SendEventOptions::default(),
        )
        .await
        .unwrap();
    let posted = kit
        .post(&room_id(), shouting_media(), PostOptions::default())
        .await
        .unwrap();
    let mut record = sent.clone();
    record.id = EventId::new("evt_record").unwrap();
    record.content = shouting_media();
    let committed = kit
        .commit_event(&room_id(), record, CommitEventOptions::default())
        .await
        .unwrap();
    let lower = || Some("https://Example.com/a.png".to_owned());
    assert_eq!(
        [
            next(&mut hook_read).await,
            media_url(&sent.content),
            media_url(&posted.content),
            media_url(&committed.content)
        ],
        [lower(), lower(), lower(), lower()]
    );
}

/// A transport whose parse writes the message `shout` as a media whose
/// link's scheme is in capitals.
struct ShoutingGate;

impl ShoutingGate {
    /// The configuration the kit keeps of the channel.
    fn config() -> ChannelConfig {
        let mut declaration = ChannelConfig::new(
            channel("gate"),
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        );
        declaration.recipient_key = None;
        declaration
    }
}

impl Channel for ShoutingGate {
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

    fn capabilities(&self) -> ChannelCapabilities {
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

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = Self::config();
        let addressing = roomkit::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: false,
        };
        (addressing, config.country_code.clone())
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        let mut event = default_inbound_event(self, &message, &context);
        if text_of(&event.content) == "shout" {
            event.content = shouting_media();
        }
        ready(Ok(event))
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

/// An agent answering the message `answer` with a media whose link's
/// scheme is in capitals.
struct ShoutingAgent;

impl ShoutingAgent {
    /// The configuration the kit keeps of the channel.
    fn config() -> ChannelConfig {
        ChannelConfig::new(
            channel("shouter"),
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        )
    }
}

impl Channel for ShoutingAgent {
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

    fn capabilities(&self) -> ChannelCapabilities {
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

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = Self::config();
        let addressing = roomkit::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: false,
        };
        (addressing, config.country_code.clone())
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let source = EventSource {
            direction: EventDirection::Outbound,
            ..EventSource::new(channel("shouter"), ChannelType::AI)
        };
        let answers = if text_of(&event.content) == "answer" {
            vec![RoomEvent::new(event.room_id, source, shouting_media())]
        } else {
            Vec::new()
        };
        ready(Ok(ChannelOutput {
            responded: !answers.is_empty(),
            response_events: answers,
            ..ChannelOutput::empty()
        }))
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(default_inbound_event(self, &message, &context)))
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

/// The media URLs a transport carrying media is delivered once the
/// message `text` came in on `gate`: a hook replaces `modify` with a media
/// whose link's scheme is in capitals, and injects one beside `inject`.
async fn delivered_through(text: &str) -> Vec<Option<String>> {
    let kit = RoomKit::new(RoomKitOptions::default());
    let audit = RecordingTransport::new(
        channel("audit"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions {
            capabilities: Some(ChannelCapabilities {
                media_types: [ChannelMediaType::Text, ChannelMediaType::Media]
                    .into_iter()
                    .collect(),
                supports_media: true,
                ..ChannelCapabilities::default()
            }),
        },
    );
    kit.register_channel(ShoutingGate).unwrap();
    kit.register_channel(ShoutingAgent).unwrap();
    kit.register_channel(audit.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["gate", "shouter", "audit"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let spec = HookOptions {
        name: "shouter".to_owned(),
        ..Default::default()
    };
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |mut event: RoomEvent, _context| {
            let answer = match text_of(&event.content).as_str() {
                "modify" => {
                    event.content = shouting_media();
                    HookResult::modify(event, ModifyOptions::default())
                }
                "inject" => {
                    let source = EventSource::new(channel("gate"), ChannelType::WEBSOCKET);
                    let injected = InjectedEvent {
                        target_channel_ids: Some(vec![channel("audit")]),
                        ..InjectedEvent::new(RoomEvent::new(
                            event.room_id,
                            source,
                            shouting_media(),
                        ))
                    };
                    HookResult::allow(AllowOptions {
                        injected: Some(vec![injected]),
                    })
                }
                _ => HookResult::allow(AllowOptions::default()),
            };
            ready(Ok(answer))
        },
        spec,
    )
    .unwrap();
    let message = InboundMessage::new(channel("gate"), "u1", TextContent::new(text));
    let routed = ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };
    kit.process_inbound(message, routed).await.unwrap();
    roomkit::testing::idle(&kit).await;
    let delivered = audit
        .deliveries()
        .iter()
        .filter_map(|delivery| media_url(&delivery.event.content))
        .map(Some)
        .collect();
    kit.close().await.unwrap();
    delivered
}

/// A SYNC hook that answers with a content its model refuses, the
/// message's replacement or an event it injects, reads as a hook that
/// raised, as Python's hook raises building it: it fails open, the message
/// is stored as it came, and nothing the hook answered is.
#[tokio::test]
async fn a_hook_answering_a_content_its_model_refuses_reads_as_raising() {
    let raised = [
        raised_by_answering(false).await,
        raised_by_answering(true).await,
    ];
    let failed_open = || (true, vec!["Bonjour".to_owned()]);
    assert_eq!(
        raised,
        [
            ("rewriter".to_owned(), failed_open()),
            ("injector".to_owned(), failed_open())
        ]
    );
}

/// The hook that raised, whether its error names the refused URL, and the
/// texts the timeline holds, once a SYNC hook answered a message with a
/// content its model refuses: its replacement, or with `injecting`, an
/// event it injects.
async fn raised_by_answering(injecting: bool) -> (String, (bool, Vec<String>)) {
    let (kit, sms) = sms_and_agent().await;
    let name = if injecting { "injector" } else { "rewriter" };
    let spec = HookOptions {
        name: name.to_owned(),
        ..Default::default()
    };
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |mut event: RoomEvent, _context| {
            let answer = if injecting {
                let injected = RoomEvent::new(
                    event.room_id.clone(),
                    EventSource::new(channel("sms"), ChannelType::SMS),
                    javascript_media(),
                );
                HookResult::allow(AllowOptions {
                    injected: Some(vec![InjectedEvent::new(injected)]),
                })
            } else {
                event.content = javascript_media();
                HookResult::modify(event, ModifyOptions::default())
            };
            ready(Ok(answer))
        },
        spec,
    )
    .unwrap();
    let errors = EventTap::on(&kit, &["hook_error"]);
    kit.process_inbound(
        message(&sms, "Bonjour"),
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let raised = tokio::time::timeout(Duration::from_secs(1), errors.wait_for(|_| true))
        .await
        .unwrap();
    let member = |name: &str| {
        let value = raised.data.get(name).and_then(Value::as_str);
        value.unwrap().to_owned()
    };
    let raised = (member("hook"), member("error"));
    let stored = timeline(&kit)
        .await
        .into_iter()
        .filter(|event| event.source.channel_id == channel("sms"))
        .map(|event| text_of(&event.content))
        .collect();
    (raised.0, (raised.1.contains("javascript:alert(1)"), stored))
}

/// A transport whose parse of a message and whose delivery answer with a
/// content its model refuses.
struct AnsweringJavascript {
    declaration: ChannelConfig,
}

impl Channel for AnsweringJavascript {
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

    fn capabilities(&self) -> ChannelCapabilities {
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

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = &self.declaration;
        let addressing = roomkit::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: false,
        };
        (addressing, config.country_code.clone())
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        let mut event = default_inbound_event(self, &message, &context);
        event.content = javascript_media();
        ready(Ok(event))
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let source = EventSource {
            direction: EventDirection::Outbound,
            ..EventSource::new(self.declaration.channel_id.clone(), ChannelType::WEBHOOK)
        };
        let response = RoomEvent::new(event.room_id, source, javascript_media());
        ready(Ok(ChannelOutput {
            responded: true,
            response_events: vec![response],
            ..ChannelOutput::empty()
        }))
    }
}

/// A channel that answers with a content its model refuses reads as one
/// that raised, as Python's raises building it: its delivery fails, and
/// nothing of its answer is stored; its parse of a message fails the
/// message, nothing stored.
#[tokio::test]
async fn a_channel_answering_a_content_its_model_refuses_fails_its_call() {
    let (kit, sms) = sms_and_agent().await;
    kit.register_channel(AnsweringJavascript {
        declaration: ChannelConfig::new(
            channel("hook"),
            ChannelType::WEBHOOK,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        ),
    })
    .unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("hook"),
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
    roomkit::testing::idle(&kit).await;
    let delivered = result
        .delivery_results
        .get("hook")
        .map(|delivery| delivery.status);
    let answers = timeline(&kit)
        .await
        .iter()
        .filter(|event| event.source.channel_id == channel("hook"))
        .count();
    let stored = timeline(&kit).await.len();
    let parsed = kit
        .process_inbound(
            InboundMessage::new(channel("hook"), "client", TextContent::new("hi")),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
        .map(drop)
        .map_err(|error| error.to_string());
    assert_eq!(
        (delivered, answers, parsed, timeline(&kit).await.len()),
        (
            Some(DeliveryResultStatus::Failed),
            0,
            Err(format!("channel hook failed: {JAVASCRIPT_REFUSED}")),
            stored
        )
    );
}

#[tokio::test]
async fn a_muted_agent_is_asked_and_its_answer_stored_blocked() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    let agent = EchoAgent::new(channel("ai"), "muted answer", EchoAgentOptions::default());
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(agent.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(
        &room_id(),
        sms.channel_id(),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let muted = AttachChannelOptions {
        muted: true,
        ..Default::default()
    };
    kit.attach_channel(&room_id(), agent.channel_id(), muted)
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
    let tap = EventTap::on(&kit, &["channel_registered"]);
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    let registered = tokio::time::timeout(Duration::from_secs(1), tap.wait_for(|_| true))
        .await
        .unwrap();
    assert_eq!(
        (registered.channel_id, Value::Object(registered.data)),
        (
            Some(channel("sms")),
            json!({ "channel_id": "sms", "channel_type": "sms" })
        )
    );
}

/// Python's `ChannelAlreadyRegisteredError`, its message as Python's
/// (probe `tasks/probes/step5-lot2-review-A/kit_refusals.py`, E22).
#[tokio::test]
async fn registering_a_channel_twice_is_refused() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms.clone()).unwrap();
    let refused = kit.register_channel(sms).unwrap_err();
    assert_eq!(
        (refused.to_string(), refused),
        (
            "Channel 'sms' is already registered. Call unregister_channel('sms') first to \
             replace it deliberately."
                .to_owned(),
            RoomKitError::ChannelAlreadyRegistered(channel("sms"))
        )
    );
}

#[tokio::test]
async fn a_kit_is_shared_by_its_clones() {
    let (kit, sms) = sms_and_agent().await;
    let clone = kit.clone();
    let task = tokio::spawn(async move {
        let sms = RecordingTransport::new(
            channel("sms"),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        );
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

/// Every event of a room's timeline the room received.
fn received() -> roomkit::store::ListEventsOptions {
    roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        ..roomkit::store::ListEventsOptions::default()
    }
}
