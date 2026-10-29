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
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::{AllowOptions, BlockOptions, ModifyOptions};
use roomkit::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
    ChannelOutput, ChannelType, DeliveryError, DeliveryResultStatus, EventContent, EventDirection,
    EventId, EventSource, EventStatus, HookResult, InboundMessage, MediaContent, RateLimit,
    RetryPolicy, RoomContext, RoomEvent, RoomId, TextContent, Timestamp,
};
use roomkit::testing::{
    EchoAgent, RecordingTransport, SequentialEnvironment, kit_with_environment,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, CloseRoomOptions,
    CommitEventOptions, ConversationStore, CreateRoomOptions, EventDraft, FrameworkEvent,
    HookExecution, HookHandler, HookReturn, HookSpec, HookTrigger, PostOptions,
    ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions, SendEventOptions,
    UpdateEventOptions, WeakRoomKit, default_inbound_event, text_of,
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
    kit.register_channel(EchoAgent::new(channel("idle"), "never"))
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

impl Channel for Unregistering {
    fn declaration(&self) -> ChannelDeclaration {
        ChannelDeclaration::new(
            channel("gate"),
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        )
    }

    fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        if let Some(kit) = self.kit.get().and_then(WeakRoomKit::upgrade) {
            kit.unregister_channel(&channel("ai"));
        }
        ready(Ok(ChannelOutput::default()))
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
    kit.before_broadcast("refuse_all", |event, _| {
        let refuse = event.source.channel_id.as_str() == "sms";
        async move {
            Ok(if refuse {
                HookResult::block("closed for the night", BlockOptions::default())
            } else {
                HookResult::allow(AllowOptions::default())
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
        ready(Ok(HookResult::modify(rewritten, ModifyOptions::default())))
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
        Ok(HookResult::allow(AllowOptions::default()))
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
                HookResult::block("spam", BlockOptions::default())
            } else {
                HookResult::allow(AllowOptions::default())
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
                HookResult::block(reason, BlockOptions::default())
            } else {
                HookResult::allow(AllowOptions::default())
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
    assert_eq!(
        refused.err(),
        Some(RoomKitError::ChannelNotRegistered(channel("telegram")))
    );
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

/// A value its model refuses, which a struct literal can hold, is refused
/// by each of the kit's calls that would write it, as Python's `ValueError`
/// when its model is built: before anything is read, so a missing room is
/// not even looked for, and nothing written. A content at `send_event`,
/// `post`, `process_inbound`, `commit_event` and `update_event`, a rate
/// limit or a retry policy not above 0 at `attach_channel`, a limit of 0 in
/// the capabilities `register_channel` is given. Stored, it would fail every
/// later read of the room.
#[tokio::test]
async fn a_value_its_model_refuses_is_refused_by_every_call_before_any_read() {
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
    let routed = ProcessInboundOptions {
        room_id: Some(ghost.clone()),
        ..Default::default()
    };
    let unbounded = RecordingTransport::new(channel("ws"), ChannelType::WEBSOCKET)
        .with_capabilities(ChannelCapabilities {
            max_length: Some(0),
            ..ChannelCapabilities::default()
        });
    let refused = [
        kit.send_event(
            &ghost,
            sms.id(),
            javascript_media(),
            SendEventOptions::default(),
        )
        .await
        .map(drop),
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
        kit.attach_channel(&ghost, &channel("ai"), limited)
            .await
            .map(drop),
        kit.attach_channel(&ghost, &channel("ai"), retried)
            .await
            .map(drop),
        kit.register_channel(unbounded),
    ];
    let media = || Err(RoomKitError::Value(JAVASCRIPT_REFUSED.to_owned()));
    let positive = |name: &str, value: &str| {
        Err(RoomKitError::Value(format!(
            "{name}: Input should be greater than 0 [input_value={value}]"
        )))
    };
    let rooms = kit
        .store()
        .list_rooms(&roomkit::RoomFilter::default())
        .await
        .unwrap();
    let registered = kit.get_channel(&channel("ws")).is_some();
    assert_eq!(
        (
            refused,
            timeline(&kit).await == before,
            rooms.len(),
            registered
        ),
        (
            [
                media(),
                media(),
                media(),
                media(),
                media(),
                positive("max_per_minute", "-1"),
                positive("base_delay_seconds", "0"),
                positive("max_length", "0"),
            ],
            true,
            1,
            false
        )
    );
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
/// message, and what `send_event`, `post` and `commit_event` return.
#[tokio::test]
async fn a_link_s_scheme_a_struct_literal_holds_enters_in_lower_case() {
    let (kit, sms) = sms_and_agent().await;
    let (read, mut hook_read) = mpsc::unbounded_channel();
    kit.hook(
        HookSpec::new(
            "media_reader",
            HookTrigger::BeforeBroadcast,
            HookExecution::Sync,
        ),
        move |event: RoomEvent, _context| {
            read.send(media_url(&event.content)).unwrap();
            ready(Ok(HookResult::allow(AllowOptions::default())))
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
            sms.id(),
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

/// A SYNC hook that replaces the message with a content its model refuses
/// reads as a hook that raised, as Python's hook raises building it: it
/// fails open, and the message is stored as it came.
#[tokio::test]
async fn a_hook_answering_a_content_its_model_refuses_reads_as_raising() {
    let (kit, sms) = sms_and_agent().await;
    kit.hook(
        HookSpec::new(
            "rewriter",
            HookTrigger::BeforeBroadcast,
            HookExecution::Sync,
        ),
        |mut event: RoomEvent, _context| async move {
            event.content = javascript_media();
            Ok(HookResult::modify(event, ModifyOptions::default()))
        },
    )
    .unwrap();
    let mut errors = kit.subscribe();
    kit.process_inbound(
        message(&sms, "Bonjour"),
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let raised = loop {
        if let FrameworkEvent::HookError { hook, error, .. } = errors.recv().await.unwrap() {
            break (hook, error);
        }
    };
    let stored = timeline(&kit)
        .await
        .into_iter()
        .find(|event| event.source.channel_id == channel("sms"))
        .map(|event| text_of(&event.content));
    assert_eq!(
        (raised.0, raised.1.contains("javascript:alert(1)"), stored),
        ("rewriter".to_owned(), true, Some("Bonjour".to_owned()))
    );
}

/// A transport whose delivery answers with a content its model refuses.
struct AnsweringJavascript {
    declaration: ChannelDeclaration,
}

impl Channel for AnsweringJavascript {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
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
/// nothing of its answer is stored.
#[tokio::test]
async fn a_channel_answering_a_content_its_model_refuses_fails_its_call() {
    let (kit, sms) = sms_and_agent().await;
    kit.register_channel(AnsweringJavascript {
        declaration: ChannelDeclaration::new(
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
    assert_eq!(
        (delivered, answers),
        (Some(DeliveryResultStatus::Failed), 0)
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

/// Python's `ChannelAlreadyRegisteredError`, its message as Python's
/// (probe `tasks/probes/step5-lot2-review-A/kit_refusals.py`, E22).
#[tokio::test]
async fn registering_a_channel_twice_is_refused() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(channel("sms"), ChannelType::SMS);
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
