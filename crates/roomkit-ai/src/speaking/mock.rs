//! Scripted policies and thinkers, retaining the inputs they were asked about.

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use roomkit::ChannelError;

use super::{SpeakDecision, SpeakMode, SpeakPolicy, SpeakTurn, SpeakingFuture, Thinker, Thought};
use crate::SharedAiContext;
use crate::classifiers::error::invalid;
use crate::shared::{Shared, failure};

/// Python's `MockSpeakPolicy` arguments, with independent empty defaults.
#[derive(Clone, Debug, Default)]
pub struct MockSpeakPolicyOptions {
    /// Decisions in order, with the last repeating; empty means speak.
    pub decisions: Vec<SpeakDecision>,
    /// Seconds before answering or raising. Negative delays still yield once.
    pub delay: f64,
    /// An error raised after recording the turn, retaining this original cause.
    pub error: Option<Arc<dyn Error + Send + Sync>>,
}

/// Returns scripted decisions and records the live input handles.
#[derive(Clone, Debug)]
pub struct MockSpeakPolicy {
    options: Arc<MockSpeakPolicyOptions>,
    turns: Shared<Vec<SpeakTurn>>,
}

impl MockSpeakPolicy {
    /// A scripted policy; an empty script repeats an ordinary speak decision.
    pub fn new(options: MockSpeakPolicyOptions) -> Self {
        Self {
            options: Arc::new(options),
            turns: Shared::new(Vec::new()),
        }
    }

    /// The recorded turns, retaining their event and mapping handles.
    pub fn turns(&self) -> Vec<SpeakTurn> {
        self.turns.snapshot()
    }
}

impl SpeakPolicy for MockSpeakPolicy {
    fn decide<'a>(&'a self, turn: &'a SpeakTurn) -> SpeakingFuture<'a, SpeakDecision> {
        Box::pin(async move {
            self.turns.update(|turns| turns.push(turn.clone()));
            delay(self.options.delay).await?;
            if let Some(error) = &self.options.error {
                return Err(failure::retained(error));
            }
            // Python chooses after its delay, so concurrent calls may advance
            // the recorded count before either one selects its script entry.
            let count = self.turns.update(|turns| turns.len());
            Ok(self
                .options
                .decisions
                .get(count.saturating_sub(1))
                .or_else(|| self.options.decisions.last())
                .cloned()
                .unwrap_or_else(|| SpeakMode::Speak.into()))
        })
    }
}

/// Python's `MockThinker` arguments.
#[derive(Clone, Debug, Default)]
pub struct MockThinkerOptions {
    /// Thoughts in order, with the last repeating; empty keeps the previous one.
    pub thoughts: Vec<Thought>,
    /// Seconds before answering or raising.
    pub delay: f64,
    /// An error raised after recording the call, retaining the original cause.
    pub error: Option<Arc<dyn Error + Send + Sync>>,
}

/// Records each previous thought and the exact context passed to it.
#[derive(Clone, Debug)]
pub struct MockThinker {
    options: Arc<MockThinkerOptions>,
    calls: Shared<Vec<(Thought, SharedAiContext)>>,
}

impl MockThinker {
    /// A thinker that keeps the previous thought when no script was supplied.
    pub fn new(options: MockThinkerOptions) -> Self {
        Self {
            options: Arc::new(options),
            calls: Shared::new(Vec::new()),
        }
    }

    /// The recorded calls. Their contexts remain the caller's shared objects.
    pub fn calls(&self) -> Vec<(Thought, SharedAiContext)> {
        self.calls.snapshot()
    }
}

impl Thinker for MockThinker {
    fn think<'a>(
        &'a self,
        previous: &'a Thought,
        context: &'a SharedAiContext,
    ) -> SpeakingFuture<'a, Thought> {
        Box::pin(async move {
            self.calls
                .update(|calls| calls.push((previous.clone(), context.clone())));
            delay(self.options.delay).await?;
            if let Some(error) = &self.options.error {
                return Err(failure::retained(error));
            }
            let count = self.calls.update(|calls| calls.len());
            Ok(self
                .options
                .thoughts
                .get(count.saturating_sub(1))
                .or_else(|| self.options.thoughts.last())
                .cloned()
                .unwrap_or_else(|| previous.clone()))
        })
    }
}

async fn delay(seconds: f64) -> Result<(), ChannelError> {
    if seconds == 0.0 {
        return Ok(());
    }
    if seconds <= 0.0 {
        tokio::task::yield_now().await;
        return Ok(());
    }
    if seconds.is_nan() {
        return Err(invalid("ValueError", "Invalid delay: NaN (not a number)"));
    }
    match Duration::try_from_secs_f64(seconds)
        .ok()
        .and_then(|duration| tokio::time::Instant::now().checked_add(duration))
    {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
    Ok(())
}
