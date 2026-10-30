//! A tool call's intent in the loop (RFC §6.4): the schemas the model is
//! shown, the calls it makes and the conversation it reads back, when the
//! agent asks its model for each call's intent.

use roomkit::tool_intent::{as_written, declares_intent, presented_parameters, take_intent};
use roomkit::transcript::Message;

use super::Loop;
use super::generation::Called;
use crate::tools::ToolDefinition;

impl<P> Loop<P> {
    /// `declared` as the requests show it: each tool's schema with the
    /// reserved intent property, one that cannot take it as it is. The
    /// toolbox keeps the tools' own schemas, which calls are checked
    /// against.
    pub(super) fn shown_tools(&self, declared: Vec<ToolDefinition>) -> Vec<ToolDefinition> {
        if !self.intents {
            return declared;
        }
        declared
            .into_iter()
            .map(|mut tool| {
                if let Some(shown) = presented_parameters(&tool.parameters) {
                    tool.parameters = shown;
                } else {
                    tracing::warn!(
                        target: "roomkit::ai",
                        tool = %tool.name,
                        "a tool is shown without the intent property: its schema declares it, or is no object; its calls carry no intent"
                    );
                }
                tool
            })
            .collect()
    }

    /// `messages` as the request shows them: each call the model made with
    /// its intent back among its arguments, as it wrote it.
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
    /// schema declares the property.
    pub(super) fn take_intents(&self, calls: &mut [Called]) {
        if !self.intents {
            return;
        }
        for called in calls.iter_mut().filter(|called| called.partial.is_none()) {
            let owned = self
                .toolbox
                .find(&called.call.name)
                .is_some_and(|(tool, _)| declares_intent(&tool.parameters));
            if !owned {
                take_intent(&mut called.call);
            }
        }
    }
}
