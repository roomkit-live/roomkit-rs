//! A tool call's intent in the loop (RFC §6.4): the schemas the model is
//! shown, the calls it makes and the conversation it reads back, when the
//! agent asks its model for each call's intent.

use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

use roomkit::tool_intent::{
    as_written, declares_intent, declares_intent_alias, presented_parameters_with_requirement,
    take_intent,
};
use roomkit_core::rules::transcript::Message;

use super::Loop;
use super::generation::Called;
use crate::tools::AiTool;

impl<P> Loop<P> {
    /// `declared` as the requests show it: each tool's schema with the
    /// reserved intent property, one that cannot take it as it is, or one
    /// the agent's list does not name. The toolbox keeps the tools' own
    /// schemas, which calls are checked against.
    pub(super) fn shown_tools(&self, declared: Vec<AiTool>) -> Vec<AiTool> {
        if !self.intents {
            return declared;
        }
        declared
            .into_iter()
            .map(|mut tool| {
                if self
                    .intents_only
                    .as_ref()
                    .is_some_and(|only| !only.contains(&tool.name))
                {
                    return tool;
                }
                if let Some(shown) =
                    presented_parameters_with_requirement(&tool.parameters, self.intents_required)
                {
                    tool.parameters = shown;
                } else {
                    log_unpresented(&tool.name);
                }
                tool
            })
            .collect()
    }

    /// `messages` as the request shows them: each call the model made with
    /// its intent, as normalized, back among its arguments.
    pub(super) fn shown_messages(&self, messages: &[Message]) -> Vec<Message> {
        if !self.intents {
            return messages.to_vec();
        }
        messages
            .iter()
            .map(|message| {
                let mut shown = message.clone();
                shown.tool_calls = message.tool_calls.iter().map(as_written).collect();
                shown
            })
            .collect()
    }

    /// Takes each call's intent out of its arguments, before its start and
    /// its gate: not a call that never runs, nor one to a tool whose own
    /// schema declares the property. A tool that declares the property's
    /// name without its underscore keeps that one.
    pub(super) fn take_intents(&self, calls: &mut [Called]) {
        if !self.intents {
            return;
        }
        for called in calls.iter_mut().filter(|called| called.partial.is_none()) {
            let parameters = self
                .toolbox
                .find(&called.call.name)
                .map(|(tool, _)| &tool.parameters);
            if parameters.is_some_and(declares_intent) {
                continue;
            }
            take_intent(
                &mut called.call,
                parameters.is_some_and(declares_intent_alias),
            );
        }
    }
}

/// Says once per process that the tool `name` is shown without the intent
/// property, as the reference implementation says it.
fn log_unpresented(name: &str) {
    static LOGGED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let first = LOGGED
        .get_or_init(Mutex::default)
        .lock()
        .is_ok_and(|mut logged| logged.insert(name.to_owned()));
    if first {
        tracing::warn!(
            target: "roomkit::ai",
            tool = %name,
            "a tool is shown without the intent property: its schema declares it or cannot accept it; its calls carry no intent"
        );
    }
}
