//! An Anthropic message's indexed content blocks, folded into RoomKit events.
use crate::error;
use roomkit_ai::{
    AiToolCall, CallIds, ProviderError, StreamDone, StreamEvent, StreamTextDelta,
    StreamThinkingDelta, StreamToolCallDelta,
};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};

#[derive(Default)]
struct Call {
    id: String,
    name: String,
    arguments: String,
}

pub(crate) struct Folding {
    calls: BTreeMap<u32, Call>,
    blocks: BTreeMap<u32, (String, bool)>,
    ids: CallIds,
    done: StreamDone,
    started: bool,
    pub(crate) stopped: bool,
}

impl Folding {
    pub(crate) fn new(model: &str) -> Self {
        let mut done = StreamDone::default();
        done.metadata.insert("model".into(), model.into());
        Self {
            calls: BTreeMap::new(),
            blocks: BTreeMap::new(),
            ids: CallIds::new(),
            done,
            started: false,
            stopped: false,
        }
    }

    pub(crate) fn event(
        &mut self,
        value: &Value,
        ready: &mut VecDeque<StreamEvent>,
        secrets: &[&str],
    ) -> Result<(), ProviderError> {
        let kind = string(value, "type")?;
        if kind == "error" {
            return Err(error::response(value, None, secrets));
        }
        if self.stopped {
            return Ok(());
        }
        if !self.started
            && matches!(
                kind,
                "message_delta"
                    | "content_block_start"
                    | "content_block_delta"
                    | "content_block_stop"
            )
        {
            return Err(error::failed("content arrived before message_start"));
        }
        match kind {
            "message_start" => self.start(value),
            "message_delta" => {
                self.message_delta(value)?;
                Ok(())
            }
            "content_block_start" => self.open(value, ready),
            "content_block_delta" => self.delta(value, ready),
            "content_block_stop" => {
                let index = index(value)?;
                let block = self
                    .blocks
                    .get_mut(&index)
                    .filter(|(_, open)| *open)
                    .ok_or_else(|| error::failed("a content block closed without opening"))?;
                block.1 = false;
                self.close(index, ready);
                Ok(())
            }
            "message_stop" => self.finish(ready),
            _ => Ok(()), // The API may add event types; ping is not output.
        }
    }

    fn start(&mut self, value: &Value) -> Result<(), ProviderError> {
        if self.started {
            return Err(error::failed("a message started twice"));
        }
        let message = value
            .get("message")
            .filter(|v| v.is_object())
            .ok_or_else(|| error::failed("message_start has no message"))?;
        self.started = true;
        if let Some(model) = message
            .get("model")
            .and_then(Value::as_str)
            .filter(|model| !model.is_empty())
        {
            self.done.metadata.insert("model".into(), model.into());
        }
        self.usage(message.get("usage"));
        Ok(())
    }

    fn message_delta(&mut self, value: &Value) -> Result<(), ProviderError> {
        if !value.get("delta").is_some_and(Value::is_object) {
            return Err(error::failed("message_delta has no delta"));
        }
        if let Some(delta) = value.get("delta") {
            if let Some(reason) = delta.get("stop_reason").and_then(Value::as_str) {
                self.done.finish_reason = Some(reason.to_owned());
            }
            if let Some(category) = delta
                .pointer("/stop_details/category")
                .and_then(Value::as_str)
            {
                self.done.metadata.insert("refusal".into(), category.into());
            }
        }
        self.usage(value.get("usage"));
        Ok(())
    }

    fn usage(&mut self, usage: Option<&Value>) {
        let Some(usage) = usage else {
            return;
        };
        for key in [
            "input_tokens",
            "output_tokens",
            "cache_creation_input_tokens",
            "cache_read_input_tokens",
        ] {
            if let Some(value) = usage.get(key).and_then(Value::as_u64) {
                if value > 0 || matches!(key, "input_tokens" | "output_tokens") {
                    self.done.usage.insert(key.into(), value.into());
                } else {
                    self.done.usage.shift_remove(key);
                }
            }
        }
        if let Some(value) = usage
            .pointer("/output_tokens_details/thinking_tokens")
            .and_then(Value::as_u64)
            .filter(|value| *value > 0)
        {
            self.done
                .usage
                .insert("reasoning_tokens".into(), value.into());
        }
    }

    fn open(
        &mut self,
        value: &Value,
        ready: &mut VecDeque<StreamEvent>,
    ) -> Result<(), ProviderError> {
        let index = index(value)?;
        let block = value
            .get("content_block")
            .ok_or_else(|| error::failed("content_block_start has no block"))?;
        let kind = string(block, "type")?;
        if self.blocks.insert(index, (kind.to_owned(), true)).is_some() {
            return Err(error::failed("a content block opened twice"));
        }
        match kind {
            "tool_use" => {
                let name = string(block, "name")?.to_owned();
                let id = self.ids.call(
                    block.get("id").and_then(Value::as_str).unwrap_or_default(),
                    &name,
                );
                ready.push_back(StreamEvent::ToolCallDelta(StreamToolCallDelta {
                    index,
                    ..StreamToolCallDelta::new(&id, &name)
                }));
                self.calls.insert(
                    index,
                    Call {
                        id,
                        name,
                        arguments: String::new(),
                    },
                );
            }
            "redacted_thinking" => {
                ready.push_back(StreamEvent::ThinkingDelta(StreamThinkingDelta {
                    block: Some(index),
                    redacted: Some(string(block, "data")?.to_owned()),
                    ..StreamThinkingDelta::default()
                }));
            }
            _ => {}
        }
        Ok(())
    }

    fn delta(
        &mut self,
        value: &Value,
        ready: &mut VecDeque<StreamEvent>,
    ) -> Result<(), ProviderError> {
        let block = index(value)?;
        let delta = value
            .get("delta")
            .ok_or_else(|| error::failed("content_block_delta has no delta"))?;
        let kind = string(delta, "type")?;
        let (opened, _) = self
            .blocks
            .get(&block)
            .filter(|(_, open)| *open)
            .ok_or_else(|| error::failed("a delta names no open content block"))?;
        let expected = match kind {
            "text_delta" => Some("text"),
            "thinking_delta" | "signature_delta" => Some("thinking"),
            "input_json_delta" => Some("tool_use"),
            _ => None,
        };
        if expected.is_some_and(|expected| expected != opened) {
            return Err(error::failed("a delta does not match its content block"));
        }
        match kind {
            "text_delta" => ready.push_back(StreamEvent::TextDelta(StreamTextDelta::new(string(
                delta, "text",
            )?))),
            "thinking_delta" => ready.push_back(StreamEvent::ThinkingDelta(StreamThinkingDelta {
                block: Some(block),
                ..StreamThinkingDelta::new(string(delta, "thinking")?)
            })),
            "signature_delta" => ready.push_back(StreamEvent::ThinkingDelta(StreamThinkingDelta {
                block: Some(block),
                signature: Some(string(delta, "signature")?.to_owned()),
                ..StreamThinkingDelta::default()
            })),
            "input_json_delta" => {
                if let Some(call) = self.calls.get_mut(&block) {
                    let fragment = string(delta, "partial_json")?;
                    call.arguments.push_str(fragment);
                    ready.push_back(StreamEvent::ToolCallDelta(StreamToolCallDelta {
                        id: call.id.clone(),
                        name: call.name.clone(),
                        index: block,
                        arguments_delta: fragment.into(),
                    }));
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn close(&mut self, index: u32, ready: &mut VecDeque<StreamEvent>) {
        if let Some(call) = self.calls.remove(&index) {
            // A closed Anthropic call whose JSON does not read was cut, not garbled.
            let mut call =
                AiToolCall::read(call.id, call.name, &call.arguments, Some("tool_use"), true);
            call.garbled = false;
            ready.push_back(StreamEvent::ToolCall(call.into()));
        }
    }

    fn finish(&mut self, ready: &mut VecDeque<StreamEvent>) -> Result<(), ProviderError> {
        if !self.started {
            return Err(error::failed("message_stop arrived before message_start"));
        }
        for (_, call) in std::mem::take(&mut self.calls) {
            let call = AiToolCall::read(
                call.id,
                call.name,
                &call.arguments,
                self.done.finish_reason.as_deref(),
                true,
            );
            ready.push_back(StreamEvent::ToolCall(call.into()));
        }
        self.stopped = true;
        ready.push_back(StreamEvent::Done(std::mem::take(&mut self.done)));
        Ok(())
    }
}

fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str, ProviderError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| error::failed(format!("the stream event has no string '{key}'")))
}

fn index(value: &Value) -> Result<u32, ProviderError> {
    value
        .get("index")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| error::failed("the content block has no valid index"))
}
