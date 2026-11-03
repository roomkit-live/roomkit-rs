//! The two protocols share transport bounds, but have distinct end markers.

use crate::error::OpenAiError;
use crate::{OpenAiApi, responses, wire};
use roomkit_ai::StreamEvent;

use crate::request::{push_end, push_text, push_thinking};
use crate::think_tags::Segment;
use std::collections::VecDeque;

pub(super) enum Folding {
    Chat(wire::Folding),
    Responses(responses::Folding),
}

impl Folding {
    pub(super) fn new(api: OpenAiApi, namespace: Option<&str>) -> Self {
        match api {
            OpenAiApi::ChatCompletions => Self::Chat(wire::Folding::default()),
            OpenAiApi::Responses => {
                let mut folding = responses::Folding::default();
                folding.namespace = namespace.map(str::to_owned);
                Self::Responses(folding)
            }
        }
    }

    pub(super) fn take(
        &mut self,
        data: &str,
        secret: &str,
        ready: &mut VecDeque<StreamEvent>,
    ) -> Result<bool, OpenAiError> {
        match self {
            Self::Chat(folding) => {
                if data == "[DONE]" {
                    finish_chat(std::mem::take(folding), ready);
                    return Ok(true);
                }
                push_segments(ready, folding.take(data, secret)?);
                Ok(false)
            }
            Self::Responses(folding) => folding.take(data, secret, ready),
        }
    }

    pub(super) fn eof(self, ready: &mut VecDeque<StreamEvent>) -> Result<(), OpenAiError> {
        match self {
            Self::Chat(folding) => {
                finish_chat(folding, ready);
                Ok(())
            }
            Self::Responses(_) => Err(OpenAiError::Malformed {
                reason: "Responses stream ended before its terminal event".into(),
            }),
        }
    }
}

fn finish_chat(mut folding: wire::Folding, ready: &mut VecDeque<StreamEvent>) {
    push_segments(ready, folding.flush());
    let (calls, done) = folding.finish();
    push_end(ready, calls, done);
}

/// Queues `segments`, each as the event its kind makes.
fn push_segments(ready: &mut VecDeque<StreamEvent>, segments: Vec<Segment>) {
    for segment in segments {
        match segment {
            Segment::Thinking(thinking) => push_thinking(ready, thinking),
            Segment::Text(text) => push_text(ready, text),
        }
    }
}
