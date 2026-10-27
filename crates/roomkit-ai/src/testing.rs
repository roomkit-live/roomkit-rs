//! A scripted provider, for tests and examples: a model that answers from a
//! script and keeps what it was asked.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit::model::Metadata;

use crate::provider::{
    AiProvider, AiRequest, Done, ProviderError, ProviderEvent, ProviderStream, ProviderToolCall,
};
use crate::stream::{Yielder, items};

/// A provider answering each generation with the next round of its script,
/// and keeping each request it was asked. A generation past the script
/// fails. Cloning it shares its script and its requests, so a test keeps a
/// clone to read them once the agent holds the provider.
///
/// ```
/// use roomkit::model::ChannelId;
/// use roomkit_ai::AiAgent;
/// use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
///
/// let model = ScriptedProvider::new([
///     ScriptedRound::calling("Let me check.", [("call_1", "lookup_order", r#"{"order": "42"}"#)]),
///     ScriptedRound::answering("Order 42 shipped on Monday."),
/// ]);
/// let agent = AiAgent::new(ChannelId::new("assistant")?, model.clone());
/// # let _ = agent;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, Default)]
pub struct ScriptedProvider {
    shared: Arc<Mutex<Script>>,
}

#[derive(Debug, Default)]
struct Script {
    rounds: VecDeque<ScriptedRound>,
    requests: Vec<AiRequest>,
}

/// One generation's answer: its text, its calls, why it stopped and what it
/// used; or its text, then a failure.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ScriptedRound {
    /// What the model says.
    pub text: String,
    /// The pieces it writes its text in, one text event each; none writes
    /// the text in one.
    pub deltas: Vec<String>,
    /// The calls it makes, each with its argument text.
    pub calls: Vec<ProviderToolCall>,
    /// How the generation ends.
    pub done: Done,
    /// The failure the generation ends on, once its text is written, before
    /// its calls and its done event: the provider's stream fails.
    pub fails: Option<String>,
}

impl ScriptedRound {
    /// A round answering `text`, calling nothing, ended `stop`.
    pub fn answering(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            deltas: Vec::new(),
            calls: Vec::new(),
            done: Done::new(Some("stop".to_owned()), Metadata::new()),
            fails: None,
        }
    }

    /// A round answering the text `deltas` join to, written in those
    /// pieces, calling nothing, ended `stop`.
    pub fn writing<'a>(deltas: impl IntoIterator<Item = &'a str>) -> Self {
        let deltas: Vec<String> = deltas.into_iter().map(str::to_owned).collect();
        Self {
            deltas: deltas.clone(),
            ..Self::answering(deltas.concat())
        }
    }

    /// A round saying `text` and making `calls`, each an id, a tool's name
    /// and its argument text, ended `tool_calls`.
    pub fn calling<'a>(
        text: impl Into<String>,
        calls: impl IntoIterator<Item = (&'a str, &'a str, &'a str)>,
    ) -> Self {
        Self {
            text: text.into(),
            deltas: Vec::new(),
            calls: calls
                .into_iter()
                .map(|(id, name, arguments)| ProviderToolCall::new(id, name, arguments))
                .collect(),
            done: Done::new(Some("tool_calls".to_owned()), Metadata::new()),
            fails: None,
        }
    }

    /// The same round, written in the pieces `deltas`, which join to its
    /// text.
    #[must_use]
    pub fn in_pieces<'a>(mut self, deltas: impl IntoIterator<Item = &'a str>) -> Self {
        self.deltas = deltas.into_iter().map(str::to_owned).collect();
        self.text = self.deltas.concat();
        self
    }

    /// The same round, failing with `message` once its text is written:
    /// the provider's stream fails mid-answer.
    #[must_use]
    pub fn failing(mut self, message: impl Into<String>) -> Self {
        self.fails = Some(message.into());
        self
    }

    /// The same round, ending for `finish_reason`.
    #[must_use]
    pub fn with_finish_reason(mut self, finish_reason: impl Into<String>) -> Self {
        self.done.finish_reason = Some(finish_reason.into());
        self
    }

    /// The same round, having used `usage`.
    #[must_use]
    pub fn with_usage(mut self, usage: Metadata) -> Self {
        self.done.usage = usage;
        self
    }
}

impl ScriptedProvider {
    /// A provider answering with `rounds`, in order.
    pub fn new(rounds: impl IntoIterator<Item = ScriptedRound>) -> Self {
        Self {
            shared: Arc::new(Mutex::new(Script {
                rounds: rounds.into_iter().collect(),
                requests: Vec::new(),
            })),
        }
    }

    /// The requests it was asked, in order.
    pub fn requests(&self) -> Vec<AiRequest> {
        self.lock().requests.clone()
    }

    fn lock(&self) -> MutexGuard<'_, Script> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl AiProvider for ScriptedProvider {
    fn name(&self) -> Option<&str> {
        Some("scripted")
    }

    fn generate<'a>(&'a self, request: &'a AiRequest) -> ProviderStream<'a> {
        let round = {
            let mut script = self.lock();
            script.requests.push(request.clone());
            script.rounds.pop_front()
        };
        Box::pin(items(
            move |out: Yielder<Result<ProviderEvent, ProviderError>>| async move {
                let Some(round) = round else {
                    return out
                        .give(Err("the scripted model has no round left".into()))
                        .await;
                };
                for delta in round.pieces() {
                    out.give(Ok(ProviderEvent::Text(delta))).await;
                }
                if let Some(failure) = round.fails {
                    return out.give(Err(failure.into())).await;
                }
                for call in round.calls {
                    out.give(Ok(ProviderEvent::ToolCall(call))).await;
                }
                out.give(Ok(ProviderEvent::Done(round.done))).await;
            },
        ))
    }
}

impl ScriptedRound {
    /// The pieces the round writes its text in.
    fn pieces(&self) -> Vec<String> {
        if !self.deltas.is_empty() {
            return self.deltas.clone();
        }
        if self.text.is_empty() {
            return Vec::new();
        }
        vec![self.text.clone()]
    }
}
