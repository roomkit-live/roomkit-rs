//! Public composition contracts, paired with Python's
//! `tests/test_ai_composition_contract.py` (RFC §6.4).

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::model::Cancel;
use roomkit::realtime::{EphemeralEvent, EphemeralEventType};
use roomkit::{SteerOptions, SubscribeRoomOptions};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, AiToolCall, MockAiProvider,
    MockAiProviderOptions, ProviderError, ProviderStream, StreamDone, StreamEvent,
    StreamThinkingDelta, StreamToolCall, StreamToolCallDelta,
};
use serde_json::json;
use tokio::sync::{Notify, Semaphore};

use super::support::{Room, channel, room_id};

#[tokio::test]
async fn mock_fragments_keep_python_json_and_complete_call() {
    for streaming in [false, true] {
        for chunks in [-1, 0, 1, 4, 100] {
            let arguments = json!({"q": "é猫🙂"}).as_object().unwrap().clone();
            let response = AiResponse {
                tool_calls: vec![AiToolCall {
                    arguments: arguments.clone(),
                    ..AiToolCall::new("c1", "lookup")
                }],
                ..AiResponse::new("")
            };
            let provider = MockAiProvider::new(MockAiProviderOptions {
                streaming,
                tool_call_delta_chunks: chunks,
                ai_responses: Some(vec![response]),
                ..MockAiProviderOptions::default()
            });
            let context = AiContext::default();
            let mut stream = provider.generate_structured_stream(&context);
            let mut fragments = Vec::new();
            let mut calls = Vec::new();
            while let Some(event) = poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
                match event.unwrap() {
                    StreamEvent::ToolCallDelta(delta) => {
                        assert_eq!((&*delta.id, &*delta.name, delta.index), ("c1", "lookup", 0));
                        fragments.push(delta.arguments_delta);
                    }
                    StreamEvent::ToolCall(call) => calls.push(call),
                    StreamEvent::TextDelta(_)
                    | StreamEvent::Done(_)
                    | StreamEvent::ThinkingDelta(_) => {}
                }
            }
            // Python json.dumps uses ASCII escapes, spaces, and surrogate pairs.
            let payload = r#"{"q": "\u00e9\u732b\ud83d\ude42"}"#;
            let expected = if streaming && chunks > 0 {
                let size = payload.len().div_ceil(usize::try_from(chunks).unwrap());
                payload
                    .as_bytes()
                    .chunks(size)
                    .map(|p| String::from_utf8(p.to_vec()).unwrap())
                    .collect()
            } else {
                Vec::new()
            };
            assert_eq!(
                fragments, expected,
                "streaming={streaming}, chunks={chunks}"
            );
            assert_eq!(calls.len(), 1);
            assert_eq!(calls.first().unwrap().arguments, arguments);
        }
    }
}

/// Waits after the first fragment, so cancellation never races completion.
struct Composing {
    later_thinking: bool,
    cancel: bool,
    ready: Arc<Notify>,
    release: Arc<Semaphore>,
}

impl AiProvider for Composing {
    fn model_name(&self) -> &'static str {
        "composing"
    }

    fn generate(
        &self,
        _context: &AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send {
        std::future::ready(Ok(AiResponse::new("")))
    }

    fn generate_structured_stream<'a>(&'a self, _context: &'a AiContext) -> ProviderStream<'a> {
        let release = Arc::clone(&self.release);
        Box::pin(Fragments {
            later_thinking: self.later_thinking,
            cancel: self.cancel,
            position: 0,
            ready: Arc::clone(&self.ready),
            release: Box::pin(async move {
                let _permit = release.acquire_owned().await.unwrap();
            }),
        })
    }
}

struct Fragments {
    later_thinking: bool,
    cancel: bool,
    position: u8,
    ready: Arc<Notify>,
    release: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl Stream for Fragments {
    type Item = Result<StreamEvent, ProviderError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let event = match this.position {
            0 if this.later_thinking => fragment("SECRETé"),
            1 if this.later_thinking => {
                StreamEvent::ThinkingDelta(StreamThinkingDelta::new("searching"))
            }
            0 => StreamEvent::ThinkingDelta(StreamThinkingDelta::new("searching")),
            1 => fragment("SECRETé"),
            2 => {
                this.ready.notify_one();
                if this.release.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                if this.cancel {
                    fragment("unread")
                } else {
                    StreamEvent::ToolCall(StreamToolCall::new("c1", "lookup"))
                }
            }
            3 => StreamEvent::Done(StreamDone {
                finish_reason: Some("tool_calls".to_owned()),
                ..StreamDone::default()
            }),
            _ => return Poll::Ready(None),
        };
        this.position += 1;
        Poll::Ready(Some(Ok(event)))
    }
}

fn fragment(text: &str) -> StreamEvent {
    StreamEvent::ToolCallDelta(StreamToolCallDelta {
        arguments_delta: text.to_owned(),
        ..StreamToolCallDelta::new("c1", "lookup")
    })
}

#[tokio::test]
async fn interrupted_composition_closes_on_the_public_bus() {
    for cancel in [false, true] {
        interrupted(cancel).await;
    }
}

async fn interrupted(cancel: bool) {
    let ready = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let provider = Composing {
        later_thinking: false,
        cancel,
        ready: Arc::clone(&ready),
        release: Arc::clone(&release),
    };
    let options = AiChannelOptions {
        thinking_coalesce_ms: 0.0,
        tool_loop_timeout: Some(Duration::from_millis(if cancel { 5000 } else { 50 })),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(channel("assistant"), provider, options).unwrap();
    let room = Arc::new(Room::with_agent(agent).await);
    let (events, _, _) = listen(&room).await;
    let running = Arc::clone(&room);
    let turn = tokio::spawn(async move { running.says("go").await });
    tokio::time::timeout(Duration::from_secs(2), ready.notified())
        .await
        .unwrap();
    if cancel {
        assert_eq!(
            room.kit
                .steer(
                    &room_id(),
                    &channel("assistant"),
                    Cancel::default(),
                    SteerOptions::default()
                )
                .await
                .unwrap(),
            1
        );
    } else {
        // The loop budget is checked at the round boundary, as in Python.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), turn)
        .await
        .unwrap()
        .unwrap();
    roomkit::testing::idle(&room.kit).await;
    room.kit.close().await.unwrap();
    check_events(&events.lock().unwrap(), false);
}

fn check_events(events: &[EphemeralEvent], later_thinking: bool) {
    use EphemeralEventType::{
        ThinkingDelta, ThinkingEnd, ThinkingStart, ToolCallDelta, ToolCallEnd, ToolCallStart,
    };
    let events: Vec<_> = events
        .iter()
        .filter(|e| {
            matches!(
                e.r#type,
                ThinkingStart
                    | ThinkingDelta
                    | ThinkingEnd
                    | ToolCallDelta
                    | ToolCallStart
                    | ToolCallEnd
            )
        })
        .collect();
    assert_eq!(
        events.iter().map(|e| e.r#type).collect::<Vec<_>>(),
        if later_thinking {
            [
                ToolCallDelta,
                ThinkingStart,
                ThinkingDelta,
                ThinkingEnd,
                ToolCallDelta,
            ]
        } else {
            [
                ThinkingStart,
                ThinkingDelta,
                ThinkingEnd,
                ToolCallDelta,
                ToolCallDelta,
            ]
        }
    );
    let deltas: Vec<_> = events
        .iter()
        .filter(|e| e.r#type == ToolCallDelta)
        .collect();
    assert_eq!(
        json!(deltas.first().unwrap().data),
        json!({
            "tool_calls": [{"id":"c1", "name":"lookup", "arguments_chars":7}],
            "round":0, "channel_id":"assistant",
        })
    );
    assert_eq!(
        json!(deltas.last().unwrap().data),
        json!({
            "tool_calls": [], "round":0, "channel_id":"assistant",
        })
    );
}

#[tokio::test]
async fn abandoned_composition_closes_on_the_public_bus() {
    for later_thinking in [false, true] {
        abandoned(later_thinking).await;
    }
}

async fn abandoned(later_thinking: bool) {
    let ready = Arc::new(Notify::new());
    let provider = Composing {
        later_thinking,
        cancel: true,
        ready: Arc::clone(&ready),
        release: Arc::new(Semaphore::new(0)),
    };
    let agent = AiChannel::new(
        channel("assistant"),
        provider,
        AiChannelOptions {
            thinking_coalesce_ms: 0.0,
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    let room = Arc::new(Room::with_agent(agent).await);
    let (events, first, terminal) = listen(&room).await;
    let running = Arc::clone(&room);
    let turn = tokio::spawn(async move { running.says("go").await });
    tokio::time::timeout(Duration::from_secs(2), ready.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), first.notified())
        .await
        .unwrap();
    turn.abort();
    assert!(turn.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(2), terminal.notified())
        .await
        .unwrap();
    roomkit::testing::idle(&room.kit).await;
    room.kit.close().await.unwrap();
    check_events(&events.lock().unwrap(), later_thinking);
}

/// Subscribe before starting the turn and signal its first and terminal frames.
async fn listen(room: &Room) -> (Arc<Mutex<Vec<EphemeralEvent>>>, Arc<Notify>, Arc<Notify>) {
    let events: Arc<Mutex<Vec<EphemeralEvent>>> = Arc::default();
    let first = Arc::new(Notify::new());
    let terminal = Arc::new(Notify::new());
    let seen = Arc::clone(&events);
    let opened = Arc::clone(&first);
    let closed = Arc::clone(&terminal);
    room.kit
        .subscribe_room(
            &room_id(),
            move |event| {
                let seen = Arc::clone(&seen);
                let opened = Arc::clone(&opened);
                let closed = Arc::clone(&closed);
                async move {
                    if event.r#type == EphemeralEventType::ToolCallDelta {
                        if event.data.get("tool_calls") == Some(&json!([])) {
                            closed.notify_one();
                        } else {
                            opened.notify_one();
                        }
                    }
                    seen.lock().unwrap().push(event);
                }
            },
            SubscribeRoomOptions::default(),
        )
        .await
        .unwrap();
    (events, first, terminal)
}
