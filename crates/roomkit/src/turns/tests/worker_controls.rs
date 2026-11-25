//! The worker, outside a turn, receives the same channel controls as Python.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    Metadata, RoomContext, RoomEvent, RoomId,
};
use serde_json::{Value, json};

use crate::turns::wire::ChannelControlEnvelope;
use crate::{Channel, ChannelError, TurnExecutor, TurnExecutorOptions};

type Events = Arc<Mutex<Vec<(usize, &'static str)>>>;

pub(super) struct Observed {
    id: ChannelId,
    instance: usize,
    events: Events,
    state: Mutex<Metadata>,
    wait: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
}

impl Observed {
    pub(super) fn built(id: ChannelId, instance: usize, events: Events) -> Self {
        Self {
            id,
            instance,
            events,
            state: Mutex::default(),
            wait: None,
        }
    }

    fn note(&self, action: &'static str) {
        self.events.lock().unwrap().push((self.instance, action));
    }
}

impl Channel for Observed {
    fn channel_id(&self) -> ChannelId {
        self.id.clone()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(crate::default_inbound_event(self, &message, &context)))
    }
    fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
    async fn on_event(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        if let Some((started, finish)) = &self.wait {
            let host = crate::current_turn_host().unwrap();
            let rows = host.channel_rows();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].id.as_str(), "tool-row");
            assert!(context.recent_events.iter().all(|row| row.id != rows[0].id));
            started.notify_one();
            finish.notified().await;
            self.note("finished");
        }
        Ok(ChannelOutput::empty())
    }
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "fixture observations start when the framework polls the callback"
    )]
    async fn on_room_attached(&self, _: RoomId, _: ChannelBinding) -> Result<(), ChannelError> {
        self.note("attach");
        Ok(())
    }
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "fixture observations start when the framework polls the callback"
    )]
    async fn on_room_detached(&self, _: RoomId) -> Result<(), ChannelError> {
        self.note("detach");
        Ok(())
    }
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "fixture observations start when the framework polls the callback"
    )]
    async fn on_room_event(&self, _: RoomId, _: RoomEvent) -> Result<(), ChannelError> {
        self.note("event");
        Ok(())
    }
    fn room_state(&self, _: &RoomId) -> Result<Metadata, ChannelError> {
        Ok(self.state.lock().unwrap().clone())
    }
    fn restore_room_state(&self, _: &RoomId, state: Metadata) -> Result<(), ChannelError> {
        *self.state.lock().unwrap() = state;
        Ok(())
    }
    #[expect(
        clippy::unused_async_trait_impl,
        reason = "fixture observations start when the framework polls the callback"
    )]
    async fn channel_call(
        &self,
        _: RoomId,
        verb: String,
        arguments: Metadata,
    ) -> Result<Value, ChannelError> {
        self.note("call");
        let mut state = self.state.lock().unwrap();
        let before = state.get("cursor").cloned();
        state.insert("cursor".into(), json!(17));
        if verb == "fail" {
            return Err(crate::RoomKitError::Value("failed verb".into()).into());
        }
        Ok(json!([before, arguments["a"]]))
    }
    async fn close(&self) {
        self.note("close");
    }
}

pub(super) fn envelope(
    kind: &str,
    update: impl FnOnce(&mut Value),
) -> crate::turns::ChannelControl {
    let mut value = include_str!("../../../tests/fixtures/turns/python-rpc.jsonl")
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|row| {
            row["direction"] == "channel_control" && row["message"]["message"]["type"] == kind
        })
        .unwrap()["message"]
        .take();
    update(&mut value["message"]);
    serde_json::from_value::<ChannelControlEnvelope>(value)
        .unwrap()
        .message
}

#[tokio::test]
async fn genuine_controls_prepare_call_restore_and_release_the_built_channel() {
    let builds = Arc::new(AtomicUsize::new(0));
    let events = Events::default();
    let executor = TurnExecutor::new(
        {
            let builds = Arc::clone(&builds);
            let events = Arc::clone(&events);
            move |_| {
                std::future::ready(Ok(Some(Observed {
                    id: super::support::channel("ai"),
                    instance: builds.fetch_add(1, Ordering::SeqCst),
                    events: Arc::clone(&events),
                    state: Mutex::default(),
                    wait: None,
                })))
            }
        },
        TurnExecutorOptions {
            executor_id: Some("worker".into()),
            organizations: Some(BTreeSet::from(["org".into()])),
            ..Default::default()
        },
    )
    .unwrap();

    for kind in ["room_attached", "room_detached"] {
        assert!(
            executor
                .control(envelope(kind, |_| {}))
                .await
                .unwrap()
                .is_none()
        );
    }
    let refused = executor
        .control(envelope("channel_call", |m| {
            m["organization_id"] = json!("foreign");
        }))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(refused.error.unwrap().attributes["reason"], "no_executor");
    assert_eq!(builds.load(Ordering::SeqCst), 0);
    executor
        .control(envelope("committed_event", |m| m["room_hooks"] = json!([])))
        .await
        .unwrap();
    executor
        .control(envelope("room_attached", |_| {}))
        .await
        .unwrap();
    let call = executor
        .control(envelope("channel_call", |m| {
            m["previous_turn"] = Value::Null;
        }))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(call.result, json!([3, 1]));
    assert!(call.error.is_none());
    assert_eq!(call.room_state.unwrap()["cursor"], 17);
    assert_eq!(call.executor_id.as_deref(), Some("worker"));
    let failed = executor
        .control(envelope("channel_call", |m| {
            m["verb"] = json!("fail");
            m["previous_turn"] = json!({"turn_id":m["call_id"],"executor_id":"worker"});
        }))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.error.unwrap().message, "failed verb");
    assert_eq!(failed.room_state.unwrap()["cursor"], 17);
    executor
        .control(envelope("room_detached", |_| {}))
        .await
        .unwrap();
    executor
        .control(envelope("channel_released", |_| {}))
        .await
        .unwrap();
    executor
        .control(envelope("committed_event", |m| m["room_hooks"] = json!([])))
        .await
        .unwrap();
    executor.close().await;
    assert_eq!(builds.load(Ordering::SeqCst), 2);
    assert_eq!(
        *events.lock().unwrap(),
        [
            (0, "event"),
            (0, "attach"),
            (0, "call"),
            (0, "call"),
            (0, "detach"),
            (0, "close"),
            (1, "event"),
            (1, "close")
        ]
    );
}

#[tokio::test]
async fn a_release_lets_the_next_build_run_before_the_old_turn_finishes() {
    use super::worker::{frame, request};
    use roomkit_core::model::{EventId, EventType};
    use tokio::sync::Notify;
    use tokio::time::{Duration, timeout};

    let builds = Arc::new(AtomicUsize::new(0));
    let events = Events::default();
    let started = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let executor = TurnExecutor::new(
        {
            let builds = Arc::clone(&builds);
            let events = Arc::clone(&events);
            let wait = (Arc::clone(&started), Arc::clone(&finish));
            move |_| {
                std::future::ready(Ok(Some(Observed {
                    id: super::support::channel("ai"),
                    instance: builds.fetch_add(1, Ordering::SeqCst),
                    events: Arc::clone(&events),
                    state: Mutex::default(),
                    wait: Some(wait.clone()),
                })))
            }
        },
        TurnExecutorOptions::default(),
    )
    .unwrap();

    let mut submitted = request("old", None);
    let mut row = submitted.event.clone();
    row.id = EventId::new("tool-row").unwrap();
    row.r#type = EventType::ToolCallEnd;
    row.source.channel_id = super::support::channel("ai");
    submitted.channel_rows.push(row.clone());
    row.source.channel_id = super::support::channel("someone-else");
    submitted.channel_rows.push(row);
    let mut turn = executor.take_carried(submitted).unwrap();
    frame(&mut turn, "old", 1, "accepted").await;
    timeout(Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    timeout(
        Duration::from_secs(5),
        executor.control(envelope("channel_released", |_| {})),
    )
    .await
    .unwrap()
    .unwrap();
    executor
        .control(envelope("committed_event", |m| {
            m["organization_id"] = Value::Null;
            m["room_hooks"] = json!([]);
        }))
        .await
        .unwrap();
    assert_eq!(*events.lock().unwrap(), [(1, "event")]);
    finish.notify_one();
    frame(&mut turn, "old", 2, "output").await;
    frame(&mut turn, "old", 3, "end").await;
    executor.close().await;
    assert_eq!(
        *events.lock().unwrap(),
        [(1, "event"), (0, "finished"), (0, "close"), (1, "close")]
    );
}

mod owner_lifecycle;

fn closing_worker(owns_channels: bool, builds: Arc<AtomicUsize>, events: Events) -> TurnExecutor {
    TurnExecutor::new(
        move |id| {
            std::future::ready(Ok(Some(Observed::built(
                id,
                builds.fetch_add(1, Ordering::SeqCst),
                Arc::clone(&events),
            ))))
        },
        TurnExecutorOptions {
            // Backend placement uses this list. Direct controls explicitly name "ai".
            hosts: Some(BTreeSet::from(["listed".to_owned()])),
            owns_channels,
            executor_id: Some("host".into()),
            ..Default::default()
        },
    )
    .unwrap()
}

fn local_control(kind: &str) -> crate::turns::ChannelControl {
    envelope(kind, |m| {
        m["organization_id"] = Value::Null;
        m["room_hooks"] = json!([]);
        m["previous_turn"] = Value::Null;
    })
}

#[tokio::test]
async fn direct_controls_after_close_respect_who_owns_the_channels() {
    for owned in [true, false] {
        let events = Events::default();
        let builds = Arc::new(AtomicUsize::new(0));
        let worker = closing_worker(owned, Arc::clone(&builds), Arc::clone(&events));
        worker.close().await;

        for kind in [
            "channel_released",
            "room_attached",
            "room_detached",
            "committed_event",
        ] {
            assert!(worker.control(local_control(kind)).await.unwrap().is_none());
        }
        let answer = worker
            .control(local_control("channel_call"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(answer.executor_id.as_deref(), Some("host"));
        if owned {
            let error = answer.error.unwrap();
            assert_eq!(error.kind, "builtins:RuntimeError");
            assert_eq!(
                error.message,
                "The executor is closed: it builds no channel"
            );
            assert!(answer.room_state.is_none());
            assert!(events.lock().unwrap().is_empty());
            assert_eq!(builds.load(Ordering::SeqCst), 0);
        } else {
            assert!(answer.error.is_none());
            assert_eq!(answer.room_state.unwrap()["cursor"], 17);
            let actions: Vec<_> = events
                .lock()
                .unwrap()
                .iter()
                .map(|(_, action)| *action)
                .collect();
            assert_eq!(actions, ["attach", "detach", "event", "call"]);
            assert_eq!(builds.load(Ordering::SeqCst), 4);
        }
        let foreign = worker
            .control(envelope("channel_call", |m| {
                m["organization_id"] = json!("elsewhere");
            }))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(foreign.error.unwrap().attributes["reason"], "no_executor");
    }
}
