//! Deferred declarations, reveal references, and reopening a room's tools.
//! Mirrors Python's `_ai_policy` and `_tool_reopen` (RFC §6.4).
use std::collections::HashSet;
use std::sync::{Mutex, PoisonError};

use roomkit_core::rules::transcript::Message;
use serde_json::json;

use super::Loop;
use crate::loop_events::{DeclaredTool, FromToolOptions};
use crate::provider::{
    AiContentPart, AiContext, AiMessage, AiMessageContent, AiRole, AiToolCallPart,
    AiToolResultPart, CallIds,
};
use crate::tool_search::render_find_payload;
use crate::tools::AiTool;

#[derive(Debug, Default)]
pub(crate) struct DeferredTools {
    first: Option<HashSet<String>>,
    referenced: Mutex<HashSet<String>>,
    /// Visible definitions, in the order the model first received them.
    declared: Mutex<Vec<DeclaredTool>>,
}

impl Loop {
    /// Record only ordinary declarations or newly granted references.
    pub(super) fn record_tools<'a>(&self, tools: impl IntoIterator<Item = &'a AiTool>) {
        let mut declared = self
            .deferred
            .declared
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for tool in tools {
            if !tool.defer_loading && !declared.iter().any(|known| known.name == tool.name) {
                let origin = self.search.origin(&tool.name);
                declared.push(DeclaredTool::from_tool(tool, FromToolOptions { origin }));
            }
        }
    }

    pub(super) fn reported_tools(&self) -> Vec<DeclaredTool> {
        self.deferred
            .declared
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(super) fn keep_round_results(&mut self, at: usize, results: &[AiToolResultPart]) {
        self.media.keep_results(at, results);
        for (offset, result) in results.iter().enumerate() {
            if !result.references.is_empty() {
                self.said.insert(
                    at + 1 + offset,
                    vec![AiContentPart::ToolResult(result.clone())],
                );
            }
        }
    }

    /// Fix the ordinary declarations before a generation can reveal anything.
    pub(super) fn open_declaration(&mut self, messages: &[Message]) {
        self.find_turn_input(messages, self.system_prompt.clone());
        if !self.provider.dyn_supports_deferred_tools() {
            return;
        }
        let shown = self.first_declaration();
        let names: HashSet<String> = shown.iter().map(|tool| tool.name.clone()).collect();
        let mut first = names.clone();
        if !self.context.stands_alone {
            if let Some(kept) = self.tool_usage.declaration(&self.context.room_id) {
                let never = self.search.never_held();
                first.retain(|name| kept.contains(name) || never.contains(name));
            }
            let opened: Vec<&AiTool> = shown
                .iter()
                .filter(|tool| !first.contains(&tool.name))
                .collect();
            if !opened.is_empty() && self.reopen(messages, &first, &opened) {
                self.record_tools(shown.iter().filter(|tool| first.contains(&tool.name)));
                self.record_tools(opened.iter().copied());
                self.deferred
                    .referenced
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .extend(opened.iter().map(|tool| tool.name.clone()));
            } else {
                first = names;
            }
            self.tool_usage
                .keep_declaration(&self.context.room_id, first.clone());
        }
        self.deferred.first = Some(first);
    }

    fn reopen(
        &mut self,
        messages: &[Message],
        first: &HashSet<String>,
        opened: &[&AiTool],
    ) -> bool {
        let Some(at) = self
            .adopted
            .turn_input
            .filter(|_| first.contains("find_tools"))
        else {
            return false;
        };
        let mut context = self.round_context(messages, self.system_prompt.clone());
        let exchanges = reopening(opened);
        if at >= context.messages.len() {
            return false;
        }
        let count = exchanges.len();
        context.messages.splice(at..at, exchanges);
        self.adopted.messages = Some((context.messages, self.shown(messages).len()));
        self.adopted.turn_input = Some(at + count);
        if let Some(input) = self
            .adopted
            .answered_input
            .as_mut()
            .filter(|input| **input >= at)
        {
            *input += count;
        }
        true
    }

    /// Held definitions remain in their initial order as references reveal them.
    pub(super) fn held_declaration(&self, shown: Vec<AiTool>) -> Vec<AiTool> {
        let Some(first) = &self.deferred.first else {
            return shown;
        };
        let ordinary = shown.into_iter().filter(|tool| first.contains(&tool.name));
        let held = self
            .declared()
            .into_iter()
            .filter(|tool| !first.contains(&tool.name))
            .map(|tool| AiTool {
                defer_loading: true,
                ..tool
            });
        ordinary.chain(held).collect()
    }

    /// Called after each individual call settles, before a sibling can reveal
    /// its own result. A held tool is referenced once per turn.
    pub(super) fn reference_shown(&self) -> Vec<String> {
        let Some(first) = &self.deferred.first else {
            return Vec::new();
        };
        let mut referenced = self
            .deferred
            .referenced
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let shown: Vec<AiTool> = self
            .search
            .collapsed(self.declared())
            .into_iter()
            .filter(|tool| !first.contains(&tool.name) && referenced.insert(tool.name.clone()))
            .collect();
        self.record_tools(&shown);
        shown.into_iter().map(|tool| tool.name).collect()
    }

    /// References removed by summarization must become ordinary declarations.
    pub(super) fn show_lost_references(
        &mut self,
        before: &[AiMessage],
        after: &[AiMessage],
        tools: &mut [AiTool],
    ) {
        let Some(first) = &mut self.deferred.first else {
            return;
        };
        let kept = references(after);
        first.extend(references(before).difference(&kept).cloned());
        for tool in tools {
            if first.contains(&tool.name) {
                tool.defer_loading = false;
            }
        }
    }
}

fn reopening(opened: &[&AiTool]) -> Vec<AiMessage> {
    let name = "find_tools";
    let id = CallIds::new().call("", name);
    let mut call = AiToolCallPart::new(id.clone(), name);
    call.arguments
        .insert("query".into(), "tools this conversation can call".into());
    let mut result = AiToolResultPart::new(id, name, render_find_payload(opened, None, &[]));
    result.references = opened.iter().map(|tool| tool.name.clone()).collect();
    let mut messages = vec![
        AiMessage::new(AiRole::Assistant, vec![AiContentPart::ToolCall(call)]),
        AiMessage::new(AiRole::Tool, vec![AiContentPart::ToolResult(result)]),
    ];
    for message in &mut messages {
        message.metadata.insert("reopening".into(), json!(true));
    }
    messages
}

pub(super) fn references(messages: &[AiMessage]) -> HashSet<String> {
    messages
        .iter()
        .flat_map(AiMessage::tool_results)
        .flat_map(|result| result.references.iter().cloned())
        .collect()
}

/// Adapt a turn to a fallback or gateway that cannot consume references.
pub(super) fn declared_for(supports_deferred: bool, context: &AiContext) -> AiContext {
    let mut context = context.clone();
    if supports_deferred {
        return context;
    }
    let referenced = references(&context.messages);
    if referenced.is_empty() && !context.tools.iter().any(|tool| tool.defer_loading) {
        return context;
    }
    context
        .tools
        .retain(|tool| !tool.defer_loading || referenced.contains(&tool.name));
    for tool in &mut context.tools {
        tool.defer_loading = false;
    }
    context
        .messages
        .retain(|message| message.metadata.get("reopening") != Some(&json!(true)));
    for message in &mut context.messages {
        if let AiMessageContent::Parts(parts) = &mut message.content {
            for part in parts {
                if let AiContentPart::ToolResult(result) = part {
                    result.references.clear();
                }
            }
        }
    }
    context
}
