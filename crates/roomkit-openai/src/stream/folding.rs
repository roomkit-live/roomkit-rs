//! The two protocols share transport bounds, but have distinct end markers.

use crate::{OpenAiApi, OpenAiError, responses, wire};
use roomkit_ai::ProviderEvent;
use std::collections::VecDeque;

pub(super) enum Folding {
    Chat(wire::Folding),
    Responses(responses::Folding),
}

impl Folding {
    pub(super) fn new(api: OpenAiApi) -> Self {
        match api {
            OpenAiApi::ChatCompletions => Self::Chat(wire::Folding::default()),
            OpenAiApi::Responses => Self::Responses(responses::Folding::default()),
        }
    }

    pub(super) fn take(
        &mut self,
        data: &str,
        secret: &str,
        ready: &mut VecDeque<ProviderEvent>,
    ) -> Result<bool, OpenAiError> {
        match self {
            Self::Chat(folding) => {
                if data == "[DONE]" {
                    finish_chat(std::mem::take(folding), ready);
                    return Ok(true);
                }
                if let Some(text) = folding.take(data, secret)? {
                    ready.push_back(ProviderEvent::Text(text));
                }
                Ok(false)
            }
            Self::Responses(folding) => folding.take(data, secret, ready),
        }
    }

    pub(super) fn eof(self, ready: &mut VecDeque<ProviderEvent>) -> Result<(), OpenAiError> {
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

fn finish_chat(folding: wire::Folding, ready: &mut VecDeque<ProviderEvent>) {
    let (calls, done) = folding.finish();
    ready.extend(calls.into_iter().map(ProviderEvent::ToolCall));
    ready.push_back(ProviderEvent::Done(done));
}
