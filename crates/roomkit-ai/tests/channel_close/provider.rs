//! A pending provider is cooperative: close waits once, at most five seconds.
use super::*;

#[derive(Clone)]
struct HeldProvider {
    trace: Trace,
    started: Arc<Notify>,
    release: Arc<Notify>,
}
impl AiProvider for HeldProvider {
    fn model_name(&self) -> &'static str {
        "held"
    }
    async fn generate(&self, _: &AiContext) -> Result<AiResponse, ProviderError> {
        let _dropped = Dropped(Arc::clone(&self.trace));
        self.started.notify_one();
        self.release.notified().await;
        Ok(AiResponse::new("late"))
    }
    async fn close(&self) {
        self.trace.lock().unwrap().push("provider closed");
    }
}

// Python: tests/test_channel_close_turns.py::test_a_second_close_does_not_wait_again_for_a_turn_the_first_cut
// Python: _ai_steering._end_running_turns and _ai_stream_round._StreamRound (provider yields cooperatively).
#[tokio::test(start_paused = true)]
async fn a_second_close_does_not_wait_again_for_a_turn_the_first_cut() {
    let model = HeldProvider {
        trace: Arc::default(),
        started: Arc::default(),
        release: Arc::default(),
    };
    let agent = AiChannel::new(
        "ai".parse().unwrap(),
        model.clone(),
        AiChannelOptions::default(),
    )
    .unwrap();
    let task = tokio::spawn(turn(agent.clone()));
    model.started.notified().await;
    let began = tokio::time::Instant::now();
    agent.close().await;
    let first = began.elapsed();
    let began = tokio::time::Instant::now();
    agent.close().await;
    let second = began.elapsed();
    let active = agent.active_turns();
    let before_release = model.trace.lock().unwrap().clone();
    model.release.notify_one();
    task.await.unwrap();
    assert_eq!(first, Duration::from_secs(5));
    assert_eq!(second, Duration::ZERO);
    assert_eq!(active, 1);
    // Neither close ended the call, nor closed the provider the channel was
    // handed, its owner's (RFC §6.4).
    assert!(before_release.is_empty(), "{before_release:?}");
    assert_eq!(agent.active_turns(), 0);
}
