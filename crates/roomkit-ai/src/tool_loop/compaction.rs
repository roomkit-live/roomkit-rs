//! A turn's context compacted once when its provider refuses it as too
//! long for the model, for the call's replay (RFC §6.4 "A turn
//! compacted"): Python's `_compact_context` and `channels/_compaction.py`;
//! `resilience` replays the call.
//!
//! The turn's input stays whole. When it falls in the older half of the
//! messages, the history before it is summarized; otherwise the older half
//! is, never parting a call from its result. The summary joins the user
//! message after it. Python also stores the long results of the rounds
//! between the input and the half for re-reading, as a large result is
//! (RFC §21.5), which roomkit-rs does not have yet: those rounds are kept
//! as they are (RMK-1228).

use roomkit_core::rules::transcript::{
    COMPACTION_HEADER, FENCED_TAGS, Message, named_blocks, quoted,
};

use super::Loop;
use super::generation::Failed;
use crate::provider::{
    AiContentPart, AiContext, AiMessage, AiMessageContent, AiProvider, AiRole, AiTextPart,
    ProviderError,
};

/// How much of each summarized message the summary quotes.
const SUMMARY_MESSAGE_CHARS: usize = 500;
/// How much of each text part of a summarized message the summary takes.
const SUMMARY_PART_CHARS: usize = 200;
/// A context this short has nothing to compact.
const MIN_COMPACTED: usize = 5;

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
        let (compacted, turn_input) = compacted(&context.messages, self.adopted.turn_input)
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
/// kept whole, and where the input now is (Python's `_compact_context`).
///
/// # Errors
///
/// A context of four messages or fewer, or one with nothing before its
/// input to summarize, cannot be compacted: an error not retried, of no
/// status.
fn compacted(
    messages: &[AiMessage],
    turn_input: Option<usize>,
) -> Result<(Vec<AiMessage>, Option<usize>), ProviderError> {
    if messages.len() < MIN_COMPACTED {
        return Err(ProviderError::from(
            "Context too large but cannot compact further (<=4 messages)",
        ));
    }
    // What Python shortens, up to the half, is kept as it is (RMK-1228).
    let (summarized, _shortened) = compaction_cut(messages, turn_input);
    let (before, kept) = messages.split_at(summarized);
    if before.is_empty() {
        return Err(ProviderError::from(
            "Context too large but nothing left to compact before the turn's input",
        ));
    }
    // A summary before an assistant's message is a user message of its own.
    let apart = kept.first().is_none_or(|first| first.role != AiRole::User);
    let turn_input = turn_input.map(|at| at - summarized + usize::from(apart));
    Ok((
        with_leading_text(summary_text(before), kept.to_vec()),
        turn_input,
    ))
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
