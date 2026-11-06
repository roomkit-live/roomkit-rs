//! What a turn takes of the context window besides its history (RFC §20):
//! Python's `TurnFootprint` and `current_turn_footprint()`, and what a
//! memory's window leaves the history once the turn is paid for.

use std::future::Future;

use roomkit::model::RoomEvent;

use super::MemoryResult;
use super::token_estimator::{HistoryBudgetOptions, estimate_notes_tokens, history_budget};

/// What a turn takes of the context window besides its history (RFC §20),
/// measured by the AI channel as the turn's first round will send it,
/// before it reads its memory: Python's `TurnFootprint`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TurnFootprint {
    /// The system prompt, the tools declared, room for the notes the
    /// channel adds to the turn's input (the speaker attribution, a cut
    /// answer's note) and the earlier rounds of tool calls the history
    /// replays.
    pub input_tokens: usize,
    /// The reply budget the turn requests; 0 when the channel sets none
    /// and the provider applies its own.
    pub reply_tokens: usize,
}

impl TurnFootprint {
    /// A turn taking `input_tokens` of the window and asking for a reply of
    /// `reply_tokens`.
    pub fn new(input_tokens: usize, reply_tokens: usize) -> Self {
        Self {
            input_tokens,
            reply_tokens,
        }
    }
}

tokio::task_local! {
    /// The footprint of the turn whose memory is reading the room.
    static FOOTPRINT: TurnFootprint;
}

/// What the running turn takes of the context window besides its history
/// (RFC §20): Python's `current_turn_footprint()`. The AI channel measures
/// it before it reads its memory, so that a memory reading the room for a
/// turn sizes the history to what the window leaves; `None` outside that
/// read, and in a tool handler, whose call is its own.
pub fn current_turn_footprint() -> Option<TurnFootprint> {
    FOOTPRINT.try_with(|footprint| *footprint).ok()
}

/// Runs `read`, a memory's read for a turn, with the turn's `footprint`.
pub(crate) async fn measured<F: Future>(footprint: TurnFootprint, read: F) -> F::Output {
    FOOTPRINT.scope(footprint, read).await
}

/// [`history_budget`] for the turn whose memory reads the room (RFC §20):
/// Python's `turn_history_budget` (`roomkit/memory/_turn_budget.py`).
///
/// The turn's input the AI channel measured ([`current_turn_footprint`])
/// is reserved, or `reserved_tokens` when larger: a floor on that same
/// input, never added to it. The reply is reserved once, the larger of the
/// safety margin and the turn's reply budget. `carried` is what a wrapped
/// memory returned: its messages and its notes occupy the window and no
/// trimming cuts them. Outside a turn nothing is measured.
pub(crate) fn turn_history_budget(
    max_context_tokens: usize,
    safety_margin_ratio: f64,
    current_event: &RoomEvent,
    reserved_tokens: usize,
    carried: Option<&MemoryResult>,
) -> usize {
    let footprint = current_turn_footprint();
    let measured = footprint.map_or(0, |footprint| footprint.input_tokens);
    let notes = carried.map_or(0, |carried| estimate_notes_tokens(&carried.notes));
    let options = HistoryBudgetOptions {
        safety_margin_ratio,
        reserved_tokens: reserved_tokens.max(measured) + notes,
        messages: carried.map(|carried| carried.messages.as_slice()),
        current_event: Some(current_event),
        reply_tokens: footprint.map_or(0, |footprint| footprint.reply_tokens),
    };
    history_budget(max_context_tokens, options)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Python: tests/test_turn_footprint.py::test_outside_a_turn_there_is_no_footprint
    #[tokio::test]
    async fn outside_a_turn_there_is_no_footprint() {
        assert_eq!(current_turn_footprint(), None);
        let footprint = TurnFootprint::new(12, 400);
        let read = measured(footprint, async { current_turn_footprint() }).await;
        assert_eq!(read, Some(footprint));
    }
}
