//! A thought computed while a real answer is pending has nothing left to offer.

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use futures_core::Stream;
use roomkit_ai::{AiContext, AiProvider, AiResponse, ProviderError, ProviderStream, StreamEvent};
use tokio::sync::Notify;

use super::*;

#[derive(Clone)]
struct HeldProvider {
    model: MockAiProvider,
    streaming: bool,
    calls: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl HeldProvider {
    async fn gate(&self) {
        if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
}

struct Gated<'a> {
    gate: Pin<Box<dyn Future<Output = ()> + Send + 'a>>,
    stream: ProviderStream<'a>,
    ready: bool,
}

impl Stream for Gated<'_> {
    type Item = Result<StreamEvent, ProviderError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.ready {
            if self.gate.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.ready = true;
        }
        self.stream.as_mut().poll_next(cx)
    }
}

impl AiProvider for HeldProvider {
    fn model_name(&self) -> &'static str {
        "held"
    }
    fn supports_structured_streaming(&self) -> bool {
        self.streaming
    }
    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        self.gate().await;
        self.model.generate(context).await
    }
    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        Box::pin(Gated {
            gate: Box::pin(self.gate()),
            stream: self.model.generate_structured_stream(context),
            ready: false,
        })
    }
}

// Python: tests/test_speak_decision_hooks.py::test_a_thought_taken_while_the_agent_answers_wants_to_say_nothing
#[tokio::test]
async fn a_thought_taken_while_the_provider_answers_is_already_said() {
    for streaming in [false, true] {
        let provider = HeldProvider {
            model: model(streaming),
            streaming,
            calls: Arc::default(),
            entered: Arc::default(),
            release: Arc::default(),
        };
        let thought = Thought {
            text: "I know the price.".into(),
            want_to_say: vec!["It costs twelve.".into()],
            urgent: true,
        };
        let thinker = Arc::new(MockThinker::new(MockThinkerOptions {
            thoughts: vec![thought.clone()],
            ..MockThinkerOptions::default()
        }));
        let policy = Arc::new(MockSpeakPolicy::new(MockSpeakPolicyOptions {
            decisions: vec![
                SpeakMode::Speak.into(),
                SpeakMode::Silent.into(),
                SpeakMode::Speak.into(),
            ],
            ..MockSpeakPolicyOptions::default()
        }));
        let channel = AiChannel::new(
            id("ai"),
            provider.clone(),
            AiChannelOptions {
                speak_policy: Some(Arc::<MockSpeakPolicy>::clone(&policy)),
                thinker: Some(Arc::<MockThinker>::clone(&thinker)),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let first = channel.clone();
        let running = tokio::spawn(async move {
            let (event, binding, context) = input("Nova?");
            first.on_event(event, binding, context).await.unwrap()
        });
        tokio::time::timeout(Duration::from_secs(3), provider.entered.notified())
            .await
            .unwrap();
        assert!(!running.is_finished());
        let (event, binding, context) = input("I have it.");
        assert!(
            !channel
                .on_event(event, binding, context)
                .await
                .unwrap()
                .responded
        );
        assert_eq!(thinker.calls().len(), 1);
        assert_eq!(policy.turns().len(), 2); // no extra decision for the wanted answer
        provider.release.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_secs(3), running)
                .await
                .unwrap()
                .unwrap()
                .responded
        );
        let (event, binding, context) = input("Nova, go on.");
        assert!(
            channel
                .on_event(event, binding, context)
                .await
                .unwrap()
                .responded
        );
        assert_eq!(policy.turns().last().unwrap().thought, Some(thought.said()));
        assert!(
            !provider
                .model
                .calls()
                .last()
                .unwrap()
                .messages
                .iter()
                .any(|m| m.text().contains("It costs twelve."))
        );
        channel.close().await;
    }
}
