//! Native ownership and deadline boundaries of the public continuation policy.
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use futures_core::Stream;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, EventSource,
    RetryPolicy, Room, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, AiTool, ContinuationPolicy,
    ProviderError, ProviderErrorOptions, ProviderStream,
};

#[derive(Clone)]
struct Model {
    inner: ScriptedProvider,
    streaming: bool,
    delay: Duration,
}
impl AiProvider for Model {
    fn model_name(&self) -> &'static str {
        "continuation"
    }
    fn supports_structured_streaming(&self) -> bool {
        self.streaming
    }
    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        tokio::time::sleep(self.delay).await;
        self.inner.generate(context).await
    }
    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        Box::pin(Delayed {
            wait: Box::pin(tokio::time::sleep(self.delay)),
            ready: false,
            inner: self.inner.generate_structured_stream(context),
        })
    }
}

struct Delayed<'a> {
    wait: Pin<Box<tokio::time::Sleep>>,
    ready: bool,
    inner: ProviderStream<'a>,
}
impl Stream for Delayed<'_> {
    type Item = Result<roomkit_ai::StreamEvent, ProviderError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if !this.ready {
            std::task::ready!(this.wait.as_mut().poll(cx));
            this.ready = true;
        }
        this.inner.as_mut().poll_next(cx)
    }
}

async fn turn<P: AiProvider>(agent: &AiChannel<P>) -> Result<ChannelOutput, roomkit::ChannelError> {
    let room = RoomId::new("r1").unwrap();
    let event = RoomEvent::new(
        room.clone(),
        EventSource::new(ChannelId::new("ws").unwrap(), ChannelType::WEBSOCKET),
        TextContent::new("Go."),
    );
    let binding = ChannelBinding {
        category: ChannelCategory::Intelligence,
        ..ChannelBinding::new(
            ChannelId::new("agent").unwrap(),
            room.clone(),
            ChannelType::AI,
        )
    };
    agent
        .on_event(event, binding, RoomContext::new(Room::new(room)))
        .await
}

fn options(policy: ContinuationPolicy) -> AiChannelOptions {
    AiChannelOptions {
        tools: vec![AiTool::new("lookup", "Look it up").unwrap().into()],
        continuation: Some(policy),
        ..Default::default()
    }
}

// Python: tests/test_continuation_policy_boundaries.py (exact callable errors).
#[tokio::test]
async fn callback_errors_keep_their_source_and_are_never_provider_retried() {
    for streaming in [false, true] {
        let failure = ProviderError::new(
            "policy failed",
            ProviderErrorOptions {
                retryable: true,
                ..Default::default()
            },
        )
        .with_source(std::io::Error::other("host cause"));
        let expected = failure.clone();
        let model = Model {
            inner: ScriptedProvider::new([ScriptedRound::answering("I will check.")]),
            streaming,
            delay: Duration::ZERO,
        };
        let agent = AiChannel::new(
            "agent".parse().unwrap(),
            model.clone(),
            AiChannelOptions {
                retry_policy: Some(RetryPolicy {
                    max_retries: 3,
                    ..Default::default()
                }),
                ..options(Arc::new(move |_| Err(failure.clone())))
            },
        )
        .unwrap();
        let error = turn(&agent).await.unwrap_err();
        let mut source: &(dyn Error + 'static) = &*error;
        while !source.is::<ProviderError>() {
            source = source.source().unwrap();
        }
        let actual = source.downcast_ref::<ProviderError>().unwrap();
        assert!(std::ptr::eq(
            actual.source().unwrap(),
            expected.source().unwrap()
        ));
        assert_eq!(model.inner.requests().len(), 1);
        agent.close().await;
    }
}

// Python: tests/test_continuation_policy.py::test_no_continuation_past_the_turns_deadline
#[tokio::test(start_paused = true)]
async fn no_continuation_past_the_turns_deadline() {
    for streaming in [false, true] {
        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let read = Arc::clone(&calls);
        let policy: ContinuationPolicy = Arc::new(move |text| {
            read.lock().unwrap().push(text.to_owned());
            Ok(Some("Continue.".into()))
        });
        let model = Model {
            inner: ScriptedProvider::new([ScriptedRound::answering("I will check.")]),
            streaming,
            delay: Duration::from_millis(100),
        };
        let agent = AiChannel::new(
            "agent".parse().unwrap(),
            model.clone(),
            AiChannelOptions {
                tool_loop_timeout: Some(Duration::from_millis(50)),
                ..options(policy)
            },
        )
        .unwrap();
        let output = turn(&agent).await.unwrap();
        assert_eq!(model.inner.requests().len(), 1);
        assert_eq!(*calls.lock().unwrap(), ["I will check."]);
        assert!(
            output
                .response_events
                .iter()
                .any(|event| event.metadata.get("loop_end_reason") == Some(&"timeout".into()))
        );
        agent.close().await;
    }
}

// The approved Rust callable representation: clones share the same host state.
#[tokio::test]
async fn channel_and_options_clones_share_the_callback_without_changing_the_default() {
    assert!(AiChannelOptions::default().continuation.is_none());
    for streaming in [false, true] {
        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let read = Arc::clone(&calls);
        let given = options(Arc::new(move |text| {
            read.lock().unwrap().push(text.to_owned());
            Ok(None)
        }));
        let copied = given.clone();
        assert!(Arc::ptr_eq(
            given.continuation.as_ref().unwrap(),
            copied.continuation.as_ref().unwrap()
        ));
        let model = Model {
            inner: ScriptedProvider::new([
                ScriptedRound::answering("First."),
                ScriptedRound::answering("Second."),
            ]),
            streaming,
            delay: Duration::ZERO,
        };
        let agent = AiChannel::new("agent".parse().unwrap(), model, copied).unwrap();
        turn(&agent).await.unwrap();
        turn(&agent.clone()).await.unwrap();
        assert_eq!(*calls.lock().unwrap(), ["First.", "Second."]);
        agent.close().await;
    }
}

// Python: tests/test_continuation_policy_boundaries.py::test_a_policy_can_cancel_before_its_nudge_spends_a_retry
#[tokio::test]
async fn a_policy_can_cancel_before_its_nudge_spends_a_retry() {
    for streaming in [false, true] {
        let model = Model {
            inner: ScriptedProvider::new([ScriptedRound::answering("I will check.")]),
            streaming,
            delay: Duration::ZERO,
        };
        let agent: Arc<AiChannel<Model>> =
            Arc::new_cyclic(|weak: &std::sync::Weak<AiChannel<Model>>| {
                let weak = std::sync::Weak::clone(weak);
                AiChannel::new(
                    "agent".parse().unwrap(),
                    model.clone(),
                    options(Arc::new(move |_| {
                        let agent = weak.upgrade().unwrap();
                        agent
                            .steer(
                                roomkit::model::Cancel::default(),
                                roomkit_ai::SteerOptions {
                                    room_id: Some("r1".parse().unwrap()),
                                    ..Default::default()
                                },
                            )
                            .unwrap();
                        Ok(Some("Continue.".into()))
                    })),
                )
                .unwrap()
            });
        let output = turn(&*agent).await.unwrap();
        assert_eq!(model.inner.requests().len(), 1);
        assert_eq!(output.response_events.len(), 1);
        assert_eq!(
            output
                .response_events
                .first()
                .unwrap()
                .metadata
                .get("loop_end_reason"),
            Some(&"unfinished".into())
        );
        agent.close().await;
    }
}
