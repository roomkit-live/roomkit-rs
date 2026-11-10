//! A named call spends its budget before asking its channel (RFC §10.1 step 1).

use super::*;
use pretty_assertions::assert_eq;
use roomkit::InboundResult;
use roomkit::telemetry::{MockTelemetryProvider, TelemetryConfig};
use std::collections::BTreeMap;

pub(super) struct AddressCalls(
    pub(super) &'static str,
    pub(super) Arc<Mutex<Vec<&'static str>>>,
);

impl Channel for AddressCalls {
    fn channel_id(&self) -> ChannelId {
        channel(self.0)
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::SMS
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }

    fn normalize_address(&self, address: &str) -> String {
        self.1.lock().unwrap().push("normalize");
        address.to_lowercase()
    }

    fn conversation_address(&self, message: &InboundMessage) -> String {
        self.1.lock().unwrap().push("conversation");
        message.sender_id.clone()
    }

    fn reply_metadata(&self, _message: &InboundMessage) -> BTreeMap<String, String> {
        self.1.lock().unwrap().push("reply");
        BTreeMap::new()
    }
}

type Answers = Arc<Mutex<Vec<InboundResult>>>;

async fn spend(kit: &RoomKit, answers: &Answers) {
    for _ in 0..9 {
        let answer = kit
            .process_inbound(message("again"), in_room(room_id()))
            .await
            .unwrap();
        answers.lock().unwrap().push(answer);
    }
    let scope = kit
        .process_inbound(
            InboundMessage::new(channel("late"), "RAW", TextContent::new("scope")),
            ProcessInboundOptions {
                organization_id: Some("other".to_owned()),
                ..in_room(room_id())
            },
        )
        .await;
    assert!(
        matches!(scope, Err(RoomKitError::RoomNotFound(_))),
        "{scope:?}"
    );
    let last = kit
        .process_inbound(message("again"), in_room(room_id()))
        .await
        .unwrap();
    answers.lock().unwrap().push(last);
    let late = InboundMessage::new(channel("late"), "RAW", TextContent::new("x"));
    let answer = kit.process_inbound(late, in_room(room_id())).await.unwrap();
    answers.lock().unwrap().push(answer);
}

pub(super) fn install_work(kit: &RoomKit, work: &str, run: Call) {
    if work == "delivery" {
        kit.register_channel(Scripted::transport("spender", run))
            .unwrap();
        return;
    }
    let observer = work == "observer";
    kit.hook(
        if observer {
            HookTrigger::AfterBroadcast
        } else {
            HookTrigger::BeforeBroadcast
        },
        move |event: RoomEvent, _: RoomContext| {
            let run = Arc::clone(&run);
            async move {
                run(event).await.unwrap();
                HookResult::allow(AllowOptions::default())
            }
        },
        HookOptions {
            name: "spender".to_owned(),
            execution: if observer {
                HookExecution::Async
            } else {
                HookExecution::Sync
            },
            needs_lock: work != "off-lock",
            ..Default::default()
        },
    )
    .unwrap();
}

async fn check(work: &str) {
    let telemetry = Arc::new(MockTelemetryProvider::default());
    let (kit, _) = room_with(RoomKitOptions {
        max_chain_depth: 1,
        telemetry: Some(TelemetryConfig {
            record_sensitive_data: true,
            ..TelemetryConfig::from(Arc::clone(&telemetry))
        }),
        ..Default::default()
    })
    .await;
    let callbacks = Arc::new(Mutex::new(Vec::new()));
    kit.register_channel(AddressCalls("late", Arc::clone(&callbacks)))
        .unwrap();
    let before = callbacks.lock().unwrap().clone();
    let answers = Arc::new(Mutex::new(Vec::new()));
    let weak = kit.downgrade();
    let saved = Arc::clone(&answers);
    install_work(
        &kit,
        work,
        Arc::new(move |event| {
            let kit = weak.upgrade().unwrap();
            let saved = Arc::clone(&saved);
            Box::pin(async move {
                if text_of(&event.content) == "hello" {
                    spend(&kit, &saved).await;
                }
                Ok(ChannelOutput::empty())
            })
        }),
    );
    if work == "delivery" {
        kit.attach_channel(
            &room_id(),
            &channel("spender"),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    send(&kit, "hello").await.unwrap();
    tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
        .await
        .unwrap();
    let answers = std::mem::take(&mut *answers.lock().unwrap());
    assert_eq!(answers.len(), 11, "{work}");
    assert!(
        answers
            .iter()
            .take(10)
            .all(|answer| !answer.blocked && answer.event.is_some()),
        "{work}: {answers:?}"
    );
    let refused = answers.last().unwrap();
    assert!(refused.blocked);
    assert_eq!(refused.reason.as_deref(), Some("reentry_loop_cap"));
    assert!(refused.event.is_none());
    assert_eq!(*callbacks.lock().unwrap(), before, "{work}");
    assert!(
        kit.store()
            .get_binding(&room_id(), &channel("late"))
            .await
            .unwrap()
            .is_none()
    );
    let mut texts = message_texts(&kit).await;
    texts.sort();
    let expected: Vec<_> = std::iter::repeat_n("again".to_owned(), 10)
        .chain(["hello".to_owned()])
        .collect();
    assert_eq!(texts, expected);
    let spans: Vec<_> = telemetry
        .completed_spans()
        .into_iter()
        .filter(|span| {
            span.name == "framework.inbound" && span.channel_id.as_deref() == Some("late")
        })
        .collect();
    assert_eq!(spans.len(), 1);
    let span = spans.first().unwrap();
    assert_eq!(span.room_id.as_deref(), Some(room_id().as_str()));
    assert_eq!(span.attributes.get("blocked"), Some(&true.into()));
    assert_eq!(span.attributes.get("sender_id"), Some(&"RAW".into()));
    kit.close().await.unwrap();
}

// Python: tests/test_reentrance_preflight.py::test_a_named_call_past_its_budget_asks_no_address_callback
#[tokio::test]
async fn a_named_call_past_its_budget_asks_no_address_callback() {
    for work in ["delivery", "locked", "off-lock", "observer"] {
        check(work).await;
    }
}
