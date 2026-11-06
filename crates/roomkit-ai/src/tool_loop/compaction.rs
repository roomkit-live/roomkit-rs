//! A turn's context compacted once when its provider refuses it as too
//! long for the model, for the call's replay (RFC §6.4 "A turn
//! compacted"): Python's `_compact_context` and `channels/_compaction.py`;
//! `resilience` replays the call.
//!
//! The turn's input stays whole. When it falls in the older half of the
//! messages, the history before it is summarized and the long results of
//! the rounds between the input and the half are stored for re-reading, as
//! a large result is (RFC §21.5), a preview in their place; otherwise the
//! older half is summarized, never parting a call from its result. The
//! summary joins the user message after it.

use roomkit_core::rules::transcript::{
    COMPACTION_HEADER, FENCED_TAGS, Message, named_blocks, quoted,
};

use super::Loop;
use super::generation::Failed;
use crate::provider::{
    AiContentPart, AiContext, AiMessage, AiMessageContent, AiProvider, AiRole, AiTextPart,
    AiToolResultPart, ProviderError,
};
use crate::tool_eviction::{
    REREAD_TOOL, ToolEviction, char_len, is_placeholder, kept_whole, texts_of,
};
use crate::tools::ToolResult;

/// How much of each summarized message the summary quotes.
const SUMMARY_MESSAGE_CHARS: usize = 500;
/// How much of each text part of a summarized message the summary takes.
const SUMMARY_PART_CHARS: usize = 200;
/// A context this short has nothing to compact.
const MIN_COMPACTED: usize = 5;
/// A result of an older round longer than this, in characters, is stored
/// at compaction, with a preview this long: its placeholder costs less
/// than what it replaces.
const STORED_OVER_CHARS: usize = 2000;
const STORED_PREVIEW_CHARS: usize = 1000;

impl<P: AiProvider> Loop<P> {
    /// Keeps where the turn's input is among the messages of its first
    /// round, `messages` its conversation: the last one when it is a
    /// user's, what a compaction keeps whole (Python's `turn_input`, set
    /// once the hooks left the context).
    pub(super) fn find_turn_input(&mut self, messages: &[Message], system_prompt: Option<String>) {
        let first = self.round_context(messages, system_prompt).messages;
        self.adopted.turn_input = first
            .last()
            .filter(|last| last.role == AiRole::User)
            .map(|_| first.len() - 1);
    }

    /// Compacts `context`, which the provider refused as too long for the
    /// model, for its one replay, `messages` the round's conversation; the
    /// turn's later rounds build on the compacted context.
    ///
    /// # Errors
    ///
    /// A compaction that cannot make room fails the call with its own
    /// error, before anything streamed.
    pub(super) fn compact(
        &mut self,
        context: &mut AiContext,
        messages: &[Message],
        error: &ProviderError,
    ) -> Result<(), Failed> {
        tracing::warn!(target: "roomkit::ai", %error, "context overflow: compacting and replaying");
        let room = self.context.room_id.as_str();
        let (compacted, turn_input) = compacted(
            &context.messages,
            self.adopted.turn_input,
            &self.eviction,
            room,
        )
        .map_err(Failed::before_streaming)?;
        // Counted as the provider reads them: a round's results are one message.
        let shown = self.shown(messages).len();
        self.adopted.messages = Some((compacted.clone(), shown));
        self.adopted.turn_input = turn_input;
        context.messages = compacted;
        Ok(())
    }
}

/// `messages` compacted to make room, the turn's input, at `turn_input`,
/// kept whole, and where the input now is (Python's `_compact_context`):
/// the long results it shortens are stored in `room` of `eviction`.
///
/// # Errors
///
/// A context of four messages or fewer, or one with nothing before its
/// input to summarize and no result to store, cannot be compacted: an
/// error not retried, of no status.
fn compacted(
    messages: &[AiMessage],
    turn_input: Option<usize>,
    eviction: &ToolEviction,
    room: &str,
) -> Result<(Vec<AiMessage>, Option<usize>), ProviderError> {
    if messages.len() < MIN_COMPACTED {
        return Err(ProviderError::from(
            "Context too large but cannot compact further (<=4 messages)",
        ));
    }
    let (summarized, shortened) = compaction_cut(messages, turn_input);
    let (before, after) = messages.split_at(summarized);
    let (rounds, rest) = after.split_at(shortened - summarized);
    let (mut kept, stored) = with_results_stored(rounds, eviction, room);
    // A result stored frees room even with nothing before the input.
    if before.is_empty() && !stored {
        return Err(ProviderError::from(
            "Context too large but nothing left to compact before the turn's input",
        ));
    }
    kept.extend_from_slice(rest);
    let summary = summary_text(before);
    // A summary before an assistant's message is a user message of its own.
    let apart = summary.is_some() && kept.first().is_none_or(|first| first.role != AiRole::User);
    let turn_input = turn_input.map(|at| at - summarized + usize::from(apart));
    Ok((with_leading_text(summary, kept), turn_input))
}

/// Where the summarized messages end, and where the shortened ones end
/// (Python's `compaction_cut`): the first half is summarized, never
/// parting a call from its result. When the turn's input, at
/// `turn_input`, falls in that half, what precedes it is summarized
/// instead, and the rounds after it, up to the half, shortened.
fn compaction_cut(messages: &[AiMessage], turn_input: Option<usize>) -> (usize, usize) {
    let half = past_results(messages, messages.len() / 2);
    match turn_input {
        Some(at) if at < half => (at, half),
        _ => (half, half),
    }
}

/// `index`, moved past the tool results there, which belong to the call
/// before.
fn past_results(messages: &[AiMessage], mut index: usize) -> usize {
    while messages
        .get(index)
        .is_some_and(|message| message.role == AiRole::Tool)
    {
        index += 1;
    }
    index
}

/// `messages` with each long tool result stored in `room` for re-reading, a
/// preview in its place, and whether one was (Python's
/// `with_results_stored`): a message none of whose results changes is kept
/// as it is.
fn with_results_stored(
    messages: &[AiMessage],
    eviction: &ToolEviction,
    room: &str,
) -> (Vec<AiMessage>, bool) {
    let mut stored = false;
    let messages = messages
        .iter()
        .map(|message| {
            with_stored_results(message, eviction, room).map_or_else(
                || message.clone(),
                |shortened| {
                    stored = true;
                    shortened
                },
            )
        })
        .collect();
    (messages, stored)
}

/// `message` with its long results stored, or `None` when none is.
fn with_stored_results(
    message: &AiMessage,
    eviction: &ToolEviction,
    room: &str,
) -> Option<AiMessage> {
    let AiMessageContent::Parts(parts) = &message.content else {
        return None;
    };
    if message.role != AiRole::Tool {
        return None;
    }
    let mut changed = false;
    let parts = parts
        .iter()
        .map(|part| {
            let AiContentPart::ToolResult(answer) = part else {
                return part.clone();
            };
            stored_result(answer, eviction, room).map_or_else(
                || part.clone(),
                |result| {
                    changed = true;
                    AiContentPart::ToolResult(AiToolResultPart {
                        result,
                        ..answer.clone()
                    })
                },
            )
        })
        .collect();
    changed.then(|| AiMessage {
        content: AiMessageContent::Parts(parts),
        ..message.clone()
    })
}

/// What stands for `answer`'s result once stored, when it is a long one a
/// model may page through; `None` to keep it as it is (Python's `_stored`).
///
/// A skill's instructions are read whole, a page of `read_stored_result`
/// already comes from the store, and a placeholder is never stored again.
/// A JSON result, which the loop never hands a provider, is kept.
fn stored_result(
    answer: &AiToolResultPart,
    eviction: &ToolEviction,
    room: &str,
) -> Option<ToolResult> {
    if kept_whole(&answer.name) || answer.name == REREAD_TOOL {
        return None;
    }
    let call_id = answer.tool_call_id.as_str();
    match &answer.result {
        ToolResult::Text(text) => (char_len(text) > STORED_OVER_CHARS && !is_placeholder(text))
            .then(|| {
                ToolResult::Text(eviction.evict(room, text, call_id, Some(STORED_PREVIEW_CHARS)))
            }),
        ToolResult::Parts(parts) => {
            let texts = texts_of(parts);
            (char_len(&texts.join("\n")) > STORED_OVER_CHARS
                && !texts.iter().any(|text| is_placeholder(text)))
            .then(|| {
                ToolResult::Parts(eviction.evict_parts(
                    room,
                    parts.clone(),
                    call_id,
                    Some(STORED_PREVIEW_CHARS),
                ))
            })
        }
        ToolResult::Json(_) => None,
    }
}

/// The summary of `messages`, one line each after its header, or `None`
/// when there are none (Python's `summary_text`).
fn summary_text(messages: &[AiMessage]) -> Option<String> {
    if messages.is_empty() {
        return None;
    }
    let lines = messages
        .iter()
        .map(|message| format!("[{}]: {}", message.role.as_str(), said(message)));
    Some(
        std::iter::once(COMPACTION_HEADER.to_owned())
            .chain(lines)
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

/// What the summary quotes of `message`: its text, a line per part, each
/// delimited block named rather than quoted, cut short and quoted on one
/// line (Python's `_said`, of a message whose speaker the context does not
/// name: roomkit-rs keeps no speaker on a message, RMK-1229).
fn said(message: &AiMessage) -> String {
    let text = match &message.content {
        AiMessageContent::Text(text) => text.clone(),
        AiMessageContent::Parts(parts) => {
            parts.iter().map(part_said).collect::<Vec<_>>().join("\n")
        }
    };
    quoted(&named_blocks(&text, FENCED_TAGS), SUMMARY_MESSAGE_CHARS)
}

/// A part's own words as the summary takes them, its blocks named, cut to
/// a part's share; another part by its type (Python's `_part_said`).
fn part_said(part: &AiContentPart) -> String {
    match part {
        AiContentPart::Text(part) => named_blocks(&part.text, FENCED_TAGS)
            .chars()
            .take(SUMMARY_PART_CHARS)
            .collect(),
        AiContentPart::Image(_) => "[image]".to_owned(),
        AiContentPart::ToolCall(_) => "[tool_call]".to_owned(),
        AiContentPart::ToolResult(_) => "[tool_result]".to_owned(),
        AiContentPart::Thinking(_) => "[thinking]".to_owned(),
    }
}

/// `text` ahead of `messages`, joined to the first when it is a user
/// message, so that no two user messages follow each other; a message of
/// its own otherwise (Python's `with_leading_text`).
fn with_leading_text(text: Option<String>, mut messages: Vec<AiMessage>) -> Vec<AiMessage> {
    let Some(text) = text else {
        return messages;
    };
    match messages.first_mut() {
        Some(first) if first.role == AiRole::User => joined_before(first, text),
        _ => messages.insert(0, AiMessage::new(AiRole::User, text)),
    }
    messages
}

/// `message` with `text` joined before its own content: a text content
/// stays text, a blank line between, a list gains a text part (Python's
/// `joined(..., before=True)`).
fn joined_before(message: &mut AiMessage, text: String) {
    message.content =
        match std::mem::replace(&mut message.content, AiMessageContent::Text(String::new())) {
            AiMessageContent::Text(own) if own.is_empty() => AiMessageContent::Text(text),
            AiMessageContent::Text(own) if text.is_empty() => AiMessageContent::Text(own),
            AiMessageContent::Text(own) => AiMessageContent::Text(format!("{text}\n\n{own}")),
            AiMessageContent::Parts(parts) => AiMessageContent::Parts(
                std::iter::once(AiContentPart::Text(AiTextPart::new(text)))
                    .chain(parts)
                    .collect(),
            ),
        };
}

#[cfg(test)]
mod tests;
