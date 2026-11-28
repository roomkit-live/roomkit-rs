//! Each script records before waiting or raising; its last entry repeats.
use std::sync::{Arc, Weak};

use roomkit::{ChannelError, ClassedError};
use roomkit_ai::SharedAiContext;
use roomkit_ai::speaking::{
    SpeakDecision, SpeakPolicy, SpeakTurn, SpeakingFuture, Thinker, Thought,
};
use serde_json::{Value, json};

use crate::run::{Run, lock};

struct Script {
    entries: Vec<Value>,
    id: String,
    run: Weak<Run>,
}
impl Script {
    fn new(spec: &Value, id: &str, run: &Arc<Run>, thinking: bool) -> Result<Self, String> {
        let beside: &[&str] = if thinking { &[] } else { &["answer_only"] };
        super::shape::script_with(spec, thinking, beside)?;
        Ok(Self {
            entries: spec[if thinking { "thoughts" } else { "decisions" }]
                .as_array()
                .ok_or("script entries")?
                .clone(),
            id: id.into(),
            run: Arc::downgrade(run),
        })
    }
    async fn entry(&self, value: Value, thinking: bool) -> Result<Value, ChannelError> {
        let run = self.run.upgrade().ok_or("scripted speaking run ended")?;
        let count = {
            let mut observed = lock(&run.observed);
            let calls = if thinking {
                &mut observed.thinker_calls
            } else {
                &mut observed.speak_turns
            };
            let calls = calls.entry(self.id.clone()).or_default();
            calls.push(value);
            calls.len()
        };
        let entry = &self.entries[count.min(self.entries.len()) - 1];
        if let Some(gate) = entry.get("gate").and_then(Value::as_str) {
            run.gates.hold(gate).await;
        }
        if let Some(message) = entry.get("fail").and_then(Value::as_str) {
            return Err(Box::new(ClassedError::new(
                if thinking {
                    "ThinkingFailedError"
                } else {
                    "SpeakPolicyFailedError"
                },
                std::io::Error::other(message.to_owned()),
            )));
        }
        Ok(entry[if thinking { "thought" } else { "decision" }].clone())
    }
}

pub(super) struct Policy(Script);
impl Policy {
    pub(super) fn new(spec: &Value, id: &str, run: &Arc<Run>) -> Result<Self, String> {
        Ok(Self(Script::new(spec, id, run, false)?))
    }
}
impl SpeakPolicy for Policy {
    fn decide<'a>(&'a self, turn: &'a SpeakTurn) -> SpeakingFuture<'a, SpeakDecision> {
        Box::pin(async move {
            let value = self.0.entry(super::turn_record(turn), false).await?;
            super::decision(&value).map_err(Into::into)
        })
    }
}

pub(super) struct Thinking(Script);
impl Thinking {
    pub(super) fn new(spec: &Value, id: &str, run: &Arc<Run>) -> Result<Self, String> {
        Ok(Self(Script::new(spec, id, run, true)?))
    }
}
impl Thinker for Thinking {
    fn think<'a>(
        &'a self,
        previous: &'a Thought,
        context: &'a SharedAiContext,
    ) -> SpeakingFuture<'a, Thought> {
        Box::pin(async move {
            let value = self.0.entry(json!({"previous":previous,"context":super::context_record(&context.snapshot())}), true).await?;
            serde_json::from_value(value).map_err(Into::into)
        })
    }
}
