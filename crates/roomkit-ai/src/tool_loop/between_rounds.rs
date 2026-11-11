//! A turn's history compacted between two of its tool rounds (RFC §6.4,
//! §20.1): Python's `_compact_after_round` and `with_compacted_history`.
//!
//! After each round it ran, the loop hands its memory the history before
//! the turn's input and what the round's generation measured
//! ([`MemoryProvider::compact`](crate::memory::MemoryProvider::compact)).
//! A history the memory returns stands for the old one in the next
//! requests: the input whole, the long results of the turn's earlier
//! rounds stored for re-reading, the last round's whole.

use roomkit::model::Metadata;
use roomkit_core::rules::transcript::Message;

use super::Loop;
use super::compaction::with_results_stored;
use super::hooks::Ran;
use crate::memory::{CompactOptions, after_memory, runtime_or_cleaned};
use crate::provider::{AiMessage, AiRole};
use crate::tool_eviction::ToolEviction;

impl Loop {
    /// What follows round `round_index` once its calls `ran`, `messages`
    /// the conversation and `usage` what the round's generation measured:
    /// `AFTER_TOOL_ROUND`, its messages read after the results, then the
    /// memory's compaction of the history; nothing for a round that ran no
    /// call (Python's `_run_announced_calls`).
    pub(super) async fn between_rounds(
        &mut self,
        round_index: u32,
        ran: Ran,
        messages: &mut Vec<Message>,
        usage: &Metadata,
    ) {
        let ran_calls = !ran.calls.is_empty();
        messages.extend(self.after_tool_round(round_index, ran).await);
        if ran_calls {
            self.compact_after_round(messages, usage).await;
        }
    }

    /// Hands the memory the history before the turn's input, `messages`
    /// the round's conversation, and `usage`, what the round's generation
    /// measured, then puts in place the history it returns, as
    /// [`with_compacted_history`] places it. A memory that returns none
    /// changes nothing, and one that fails is logged.
    async fn compact_after_round(&mut self, messages: &[Message], usage: &Metadata) {
        let Some(at) = self.adopted.turn_input else {
            return;
        };
        let asked = self.round_context(messages, None).messages;
        let Some(history) = asked.get(..at) else {
            return;
        };
        let options = CompactOptions {
            channel_id: Some(self.context.channel_id.clone()),
        };
        let compacted = match self
            .memory
            .compact(&self.context.room_id, history, usage, options)
            .await
        {
            Ok(Some(compacted)) => compacted,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(target: "roomkit::ai", %error, "memory compaction between two tool rounds failed");
                return;
            }
        };
        let room = self.context.room_id.as_str();
        let placed = with_compacted_history(&asked, at, compacted, &self.eviction, room);
        let input = placed.len() - (asked.len() - at);
        // Counted as the provider reads them: a round's results are one message.
        let shown = self.shown(messages).len();
        self.show_lost_references(&asked, &placed, &mut []);
        self.adopted.messages = Some((placed, shown));
        self.adopted.turn_input = Some(input);
    }
}

/// `asked` with `compacted`, the history a memory returned between two
/// tool rounds, in place of the messages before the turn's input at `at`
/// (Python's `with_compacted_history`); the long results it stores are
/// stored in `room` of `eviction`.
///
/// The input stays whole, and the long results of the turn's earlier
/// rounds are stored for re-reading, as an emergency compaction stores
/// them; the last round's, which the model has not read yet, stay whole. A
/// message of `compacted` that is not one of the history's is text from
/// outside, a copy of a runtime mark in it replaced unless a memory of the
/// runtime built it, and a user text the messages the memory wrote ahead
/// of the history's own end on joins the user message after them, as a
/// memory's messages do at the turn's start (RFC §6.4).
fn with_compacted_history(
    asked: &[AiMessage],
    at: usize,
    compacted: Vec<AiMessage>,
    eviction: &ToolEviction,
    room: &str,
) -> Vec<AiMessage> {
    let (history, after) = asked.split_at(at.min(asked.len()));
    let last = last_round(after);
    let (input, rounds) = after.split_at(1.min(after.len()));
    let (earlier, latest) = rounds.split_at(last.saturating_sub(1).min(rounds.len()));
    let mut rest = input.to_vec();
    rest.extend(with_results_stored(earlier, eviction, room).0);
    rest.extend_from_slice(latest);
    let built = compacted
        .iter()
        .position(|message| history.contains(message))
        .unwrap_or(compacted.len());
    let placed: Vec<AiMessage> = compacted
        .into_iter()
        .map(|message| {
            if history.contains(&message) {
                message
            } else {
                runtime_or_cleaned(message)
            }
        })
        .collect();
    let (memory, kept) = placed.split_at(built);
    after_memory(memory, kept.iter().cloned().chain(rest).collect())
}

/// Where the turn's last round starts among `after`, the messages from the
/// turn's input on: its assistant message, which holds the calls whose
/// results the model has not read yet; the end when there is none after
/// the input.
fn last_round(after: &[AiMessage]) -> usize {
    after
        .iter()
        .enumerate()
        .skip(1)
        .rev()
        .find(|(_, message)| message.role == AiRole::Assistant)
        .map_or(after.len(), |(at, _)| at)
}
