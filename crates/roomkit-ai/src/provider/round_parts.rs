//! The parts an assistant round is replayed as: its reasoning, its text and
//! its calls (RFC §6.4): Python's `round_parts` and `RoundTranscript`.
//!
//! One assembly for every reader of a round. The tool loop saw the round
//! stream, so it knows where each reasoning block came relative to the text
//! and the calls and replays them there. A caller of `generate()` has only
//! the response, which does not say where its blocks came: they go first,
//! then the text, then the calls.

use roomkit::RoomKitError;
use roomkit::tool_intent::INTENT_PROPERTY;
use serde_json::Value;

use super::context::{AiContentPart, AiTextPart, AiThinkingPart, AiToolCallPart};
use super::response::{AiToolCall, StreamThinkingDelta};
use super::thinking_blocks::ThinkingBlocks;

/// The history part of `call`: its intent, as normalized, back among its
/// arguments when it has one (RFC §6.4), its metadata (a thought signature)
/// kept. Python's `call_part`.
fn call_part(call: &AiToolCall) -> AiToolCallPart {
    let mut arguments = call.arguments.clone();
    if let Some(intent) = &call.intent {
        arguments.insert(INTENT_PROPERTY.to_owned(), Value::String(intent.clone()));
    }
    AiToolCallPart {
        arguments,
        metadata: call.metadata.clone(),
        ..AiToolCallPart::new(call.id.clone(), call.name.clone())
    }
}

/// A round whose reasoning has no known place: the reasoning, then the text,
/// then the calls.
#[must_use]
pub fn round_parts(
    thinking: &[AiThinkingPart],
    text: &str,
    calls: &[AiToolCall],
) -> Vec<AiContentPart> {
    let mut parts: Vec<AiContentPart> = thinking
        .iter()
        .cloned()
        .map(AiContentPart::Thinking)
        .collect();
    if !text.is_empty() {
        parts.push(AiContentPart::Text(AiTextPart::new(text)));
    }
    parts.extend(
        calls
            .iter()
            .map(|call| AiContentPart::ToolCall(call_part(call))),
    );
    parts
}

/// One entry of what a round said, in the order it came.
#[derive(Clone, Debug)]
enum Entry {
    Thinking(Option<u32>),
    Text(usize),
    Call(String),
}

/// What a round said, in the order it came: its reasoning blocks, the
/// stretches of text between them and its calls.
#[derive(Clone, Debug, Default)]
pub struct RoundTranscript {
    thinking: ThinkingBlocks,
    entries: Vec<Entry>,
    segments: Vec<String>,
}

impl RoundTranscript {
    /// Nothing said yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The round's reasoning blocks.
    #[must_use]
    pub fn thinking(&self) -> &ThinkingBlocks {
        &self.thinking
    }

    /// Everything the round said, its stretches joined.
    #[must_use]
    pub fn text(&self) -> String {
        self.segments.concat()
    }

    /// Keeps `delta`, in its block.
    pub fn add_thinking(&mut self, delta: &StreamThinkingDelta) {
        if self.thinking.add(delta) {
            self.entries.push(Entry::Thinking(delta.block));
        }
    }

    /// Keeps `text`, in the stretch under way or a new one.
    pub fn add_text(&mut self, text: &str) {
        if let (Some(Entry::Text(_)), Some(segment)) =
            (self.entries.last(), self.segments.last_mut())
        {
            segment.push_str(text);
            return;
        }
        self.entries.push(Entry::Text(self.segments.len()));
        self.segments.push(text.to_owned());
    }

    /// Keeps where the call `call_id` came.
    pub fn add_call(&mut self, call_id: impl Into<String>) {
        self.entries.push(Entry::Call(call_id.into()));
    }

    /// The round as the next one replays it, keeping `calls` of the ones it
    /// made. Reasoning a vendor sent in blocks goes back block by block, each
    /// where it came; a single block goes first, then the text, then the
    /// calls.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Runtime`] for a call in `calls` the round never made,
    /// as Python raises `RuntimeError`.
    pub fn parts(&self, calls: &[AiToolCall]) -> Result<Vec<AiContentPart>, RoomKitError> {
        if !self.thinking.keyed() {
            return Ok(round_parts(&self.thinking.parts(), &self.text(), calls));
        }
        let mut pending: Vec<&AiToolCall> = calls.iter().collect();
        let mut parts = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            match entry {
                Entry::Thinking(block) => {
                    parts.extend(self.thinking.part(*block).map(AiContentPart::Thinking));
                }
                Entry::Text(segment) => {
                    let text = self.segments.get(*segment).filter(|text| !text.is_empty());
                    parts.extend(text.map(|text| AiContentPart::Text(AiTextPart::new(text))));
                }
                Entry::Call(id) => {
                    if let Some(at) = pending.iter().position(|call| call.id == *id) {
                        let call = pending.remove(at);
                        parts.push(AiContentPart::ToolCall(call_part(call)));
                    }
                }
            }
        }
        if pending.is_empty() {
            return Ok(parts);
        }
        let mut ids: Vec<&str> = pending.iter().map(|call| call.id.as_str()).collect();
        ids.sort_unstable();
        Err(RoomKitError::Runtime(format!(
            "replaying calls the round never made: {ids:?}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    // Python: tests/test_providers/test_thinking_blocks.py::test_replaying_a_call_the_round_never_made_is_an_error
    #[test]
    fn replaying_a_call_the_round_never_made_is_an_error() {
        let mut transcript = RoundTranscript::new();
        transcript.add_thinking(&StreamThinkingDelta {
            block: Some(0),
            signature: Some("S0".to_owned()),
            ..StreamThinkingDelta::new("a")
        });
        let error = transcript
            .parts(&[AiToolCall::new("ghost", "x")])
            .expect_err("a call the round never made");
        assert!(error.to_string().contains("never made"), "{error}");
    }

    // A call past the round's bound stays out, the transcript keeping its
    // place alone (Python's `_cap_round_tool_calls` before `parts`).
    #[test]
    fn a_call_it_made_and_the_loop_dropped_stays_out() {
        let mut transcript = RoundTranscript::new();
        transcript.add_thinking(&StreamThinkingDelta {
            block: Some(0),
            signature: Some("S0".to_owned()),
            ..StreamThinkingDelta::new("a")
        });
        transcript.add_call("kept");
        transcript.add_call("dropped");
        let parts = transcript
            .parts(&[AiToolCall::new("kept", "x")])
            .expect("the round made it");
        assert_eq!(parts.len(), 2);
        assert!(matches!(&parts[1], AiContentPart::ToolCall(call) if call.id == "kept"));
    }

    // A call's intent goes back among its arguments (RFC §6.4).
    #[test]
    fn a_call_part_writes_its_intent_back() {
        let call = AiToolCall {
            intent: Some("Look it up.".to_owned()),
            ..AiToolCall::new("c1", "lookup")
        };
        let parts = round_parts(&[], "", &[call]);
        assert!(matches!(
            &parts[0],
            AiContentPart::ToolCall(part)
                if part.arguments.get(INTENT_PROPERTY).and_then(Value::as_str) == Some("Look it up.")
        ));
    }
}
