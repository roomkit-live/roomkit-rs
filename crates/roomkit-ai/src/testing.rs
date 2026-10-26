//! A scripted provider, for tests and examples: a model that answers from a
//! script and keeps what it was asked.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit::model::Metadata;

use crate::provider::{
    AiProvider, AiRequest, Done, ProviderResponse, ProviderToolCall, ResponseStream,
};

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
/// used.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ScriptedRound {
    /// What the model says.
    pub text: String,
    /// The calls it makes, each with its argument text.
    pub calls: Vec<ProviderToolCall>,
    /// How the generation ends.
    pub done: Done,
}

impl ScriptedRound {
    /// A round answering `text`, calling nothing, ended `stop`.
    pub fn answering(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            calls: Vec::new(),
            done: Done::new(Some("stop".to_owned()), Metadata::new()),
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
            calls: calls
                .into_iter()
                .map(|(id, name, arguments)| ProviderToolCall::new(id, name, arguments))
                .collect(),
            done: Done::new(Some("tool_calls".to_owned()), Metadata::new()),
        }
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

    fn generate<'a>(&'a self, request: &'a AiRequest) -> ResponseStream<'a> {
        let round = {
            let mut script = self.lock();
            script.requests.push(request.clone());
            script.rounds.pop_front()
        };
        ProviderResponse::into_stream(async move {
            let round = round.ok_or("the scripted model has no round left")?;
            Ok(ProviderResponse::new(round.text, round.calls, round.done))
        })
    }
}
