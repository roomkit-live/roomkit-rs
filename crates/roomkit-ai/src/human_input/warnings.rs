//! The channel's human tools that no turn offers to its model.

use std::collections::BTreeSet;
use std::sync::{Mutex, PoisonError};

use super::HumanInputToolHandler;
use crate::tools::toolbox::Toolbox;

#[derive(Default)]
pub(crate) struct Warnings(Mutex<BTreeSet<String>>);

impl Warnings {
    /// Check before policy and Tool Search filter the turn's catalogue:
    /// a host or handler must declare every name the handler intercepts.
    pub(crate) fn check(&self, channel: &str, human: &HumanInputToolHandler, tools: &Toolbox) {
        let mut warned = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let missing: Vec<_> = human
            .tool_names()
            .iter()
            .filter(|name| tools.find(name).is_none() && warned.insert((*name).clone()))
            .collect();
        if !missing.is_empty() {
            tracing::warn!(target: "roomkit::ai", channel, tools = ?missing,
                "the channel intercepts human-input tools but never offers them to the model: declare them via HumanInputToolHandler tool_definitions or the channel's tools, or no human will ever be asked");
        }
    }
}
