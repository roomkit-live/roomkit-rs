use super::*;
use futures_core::Stream;
use roomkit_ai::{ProviderError, ProviderStream};
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};
use tokio::sync::Barrier;

struct RoomPlans {
    a: MockAiProvider,
    b: MockAiProvider,
    first: Mutex<HashSet<String>>,
    both: Barrier,
    streaming: bool,
}

impl RoomPlans {
    fn new(streaming: bool) -> Self {
        let room = |name| {
            model(
                vec![
                    plan(tasks(name)),
                    AiResponse::new("Done."),
                    AiResponse::new("Done."),
                ],
                streaming,
            )
        };
        Self {
            a: room("SECRET_a"),
            b: room("SECRET_b"),
            first: Mutex::default(),
            both: Barrier::new(2),
            streaming,
        }
    }
    fn room<'a>(&'a self, context: &AiContext) -> &'a MockAiProvider {
        if context.room.as_ref().unwrap().room.id.as_str() == "a" {
            &self.a
        } else {
            &self.b
        }
    }
    async fn meet(&self, context: &AiContext) {
        let first = self
            .first
            .lock()
            .unwrap()
            .insert(context.room.as_ref().unwrap().room.id.to_string());
        if first {
            tokio::time::timeout(BOUND, self.both.wait()).await.unwrap();
        }
    }
}

impl AiProvider for RoomPlans {
    fn model_name(&self) -> &'static str {
        "room-plans"
    }
    fn supports_structured_streaming(&self) -> bool {
        self.streaming
    }
    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        self.meet(context).await;
        self.room(context).generate(context).await
    }
    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        Box::pin(Waiting {
            wait: Some(Box::pin(self.meet(context))),
            inner: self.room(context).generate_structured_stream(context),
        })
    }
}

struct Waiting<'a> {
    wait: Option<Pin<Box<dyn Future<Output = ()> + Send + 'a>>>,
    inner: ProviderStream<'a>,
}

impl Stream for Waiting<'_> {
    type Item = Result<roomkit_ai::StreamEvent, ProviderError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(wait) = &mut this.wait {
            std::task::ready!(wait.as_mut().poll(cx));
            this.wait = None;
        }
        this.inner.as_mut().poll_next(cx)
    }
}

#[tokio::test]
async fn two_live_rooms_keep_separate_plans_in_their_real_messages() {
    for streaming in [false, true] {
        let provider = RoomPlans::new(streaming);
        let (a, b) = (provider.a.clone(), provider.b.clone());
        let kit = setup(provider, &["a", "b"], RoomKitOptions::default()).await;
        for body in ["Plan.", "Continue."] {
            tokio::join!(say(&kit, "a", body), say(&kit, "b", body));
        }
        for (model, own, other) in [(a, "SECRET_a", "SECRET_b"), (b, "SECRET_b", "SECRET_a")] {
            let calls = model.calls();
            assert_eq!(calls.len(), 3);
            assert!(input(calls.last().unwrap()).contains(own));
            assert!(
                calls
                    .iter()
                    .all(|call| !format!("{:?}", call.messages).contains(other))
            );
        }
        kit.close().await.unwrap();
    }
}
