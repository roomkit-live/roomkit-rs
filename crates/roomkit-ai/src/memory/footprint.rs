//! What a turn takes of the context window besides its history (RFC §20):
//! Python's `TurnFootprint` and `current_turn_footprint()`, and what a
//! memory's window leaves the history once the turn is paid for.

use std::future::Future;

use roomkit::model::RoomEvent;

use super::MemoryResult;
use super::token_estimator::{
    EstimateEventTokensOptions, HistoryBudgetOptions, estimate_event_tokens, estimate_notes_tokens,
    history_budget,
};

/// What a turn takes of the context window besides its history (RFC §20),
/// measured by the AI channel as the turn's first round will send it,
/// before it reads its memory: Python's `TurnFootprint`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnFootprint {
    /// The system prompt, the tools declared, and room for the notes the
    /// channel adds to the turn's input (the speaker attribution, a cut
    /// answer's note).
    pub input_tokens: usize,
    /// The reply budget the turn requests; 0 when the channel sets none
    /// and the provider applies its own.
    pub reply_tokens: usize,
    /// What the history replays of the channel's earlier turns beside their
    /// messages, each round of tool calls and each instruction they read as
    /// `(index, turn, tokens)`: its timeline index, the turn it belongs to
    /// (its rows' correlation id) and what it costs. Not reserved up front:
    /// a memory charges each to the history event it reads with and keeps or
    /// cuts it with that event (Python's `history_event_costs`).
    pub replayed: Vec<(u64, Option<String>, usize)>,
}

impl TurnFootprint {
    /// A turn taking `input_tokens` of the window and asking for a reply of
    /// `reply_tokens`, replaying nothing of its earlier turns.
    pub fn new(input_tokens: usize, reply_tokens: usize) -> Self {
        Self {
            input_tokens,
            reply_tokens,
            replayed: Vec::new(),
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
    FOOTPRINT.try_with(TurnFootprint::clone).ok()
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
    let footprint = FOOTPRINT
        .try_with(|footprint| (footprint.input_tokens, footprint.reply_tokens))
        .ok();
    let measured = footprint.map_or(0, |(input, _)| input);
    let notes = carried.map_or(0, |carried| estimate_notes_tokens(&carried.notes));
    let options = HistoryBudgetOptions {
        safety_margin_ratio,
        reserved_tokens: reserved_tokens.max(measured) + notes,
        messages: carried.map(|carried| carried.messages.as_slice()),
        current_event: Some(current_event),
        reply_tokens: footprint.map_or(0, |(_, reply)| reply),
    };
    history_budget(max_context_tokens, options)
}

/// What each of `events`, a history oldest first, costs the window with
/// what the turn's history replays with it (RFC §20): its own tokens, and
/// each earlier round of tool calls and instruction the AI channel measured
/// ([`TurnFootprint::replayed`]), charged to the event it reads with, so
/// that a memory keeps or cuts a turn whole. Python's `history_event_costs`.
///
/// A replayed piece reads with its turn's first event after it (the turn's
/// answer, which an instruction and its rounds read before), else after the
/// last event before it; one charged to no event (its turn and its place
/// out of `events`) is not replayed, and costs nothing.
pub(crate) fn history_event_costs(events: &[RoomEvent]) -> Vec<usize> {
    let mut costs: Vec<usize> = events
        .iter()
        .map(|event| estimate_event_tokens(event, EstimateEventTokensOptions::default()))
        .collect();
    let indexes: Vec<u64> = events.iter().map(|event| event.index).collect();
    // Read in place: the footprint is the turn's, its pieces left with it.
    let _read = FOOTPRINT.try_with(|footprint| {
        for (index, turn, tokens) in &footprint.replayed {
            if let Some(cost) =
                charged(events, &indexes, *index, turn.as_deref()).and_then(|at| costs.get_mut(at))
            {
                *cost += tokens;
            }
        }
    });
    costs
}

/// The position among `events` of the one a replayed piece at timeline
/// `index` of `turn` reads with, `None` for none (Python's `_charged`).
fn charged(events: &[RoomEvent], indexes: &[u64], index: u64, turn: Option<&str>) -> Option<usize> {
    let after = indexes.partition_point(|known| *known < index);
    if let Some(turn) = turn {
        let later = events.get(after..).unwrap_or_default();
        if let Some(at) = later
            .iter()
            .position(|event| event.correlation_id.as_deref() == Some(turn))
        {
            return Some(after + at);
        }
    }
    after.checked_sub(1)
}

#[cfg(test)]
mod tests {
    use roomkit::model::{ChannelId, ChannelType, EventSource, RoomId, TextContent};

    use super::*;

    /// A message at `index` of the turn `turn`.
    fn said(index: u64, turn: Option<&str>) -> RoomEvent {
        let source = EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS);
        let mut event = RoomEvent::new(RoomId::new("r1").unwrap(), source, TextContent::new("hi"));
        event.index = index;
        event.correlation_id = turn.map(str::to_owned);
        event
    }

    // Python: memory/_turn_budget.py history_event_costs and _charged
    // (tests/test_channels/test_tool_history.py::test_a_budget_memory_keeps_whole_turns_with_their_rounds).
    /// RFC §20: a replayed piece is charged to its turn's first event after
    /// it, else to the last event before it; one before every event whose
    /// turn has none costs nothing.
    #[tokio::test]
    async fn a_replayed_piece_is_charged_to_the_event_it_reads_with() {
        let events = [said(2, None), said(5, Some("t1")), said(7, None)];
        let own: Vec<usize> = events
            .iter()
            .map(|event| estimate_event_tokens(event, EstimateEventTokensOptions::default()))
            .collect();
        let footprint = TurnFootprint {
            replayed: vec![
                (3, Some("t1".to_owned()), 10),
                (0, Some("t1".to_owned()), 100),
                (6, Some("t9".to_owned()), 1000),
                (1, None, 10_000),
            ],
            ..TurnFootprint::new(0, 0)
        };
        let costs = measured(footprint, async { history_event_costs(&events) }).await;
        // t1's pieces read with its answer; t9's, whose turn has no event,
        // with the last event before it; the turn-less one before every
        // event with none.
        assert_eq!(costs, [own[0], own[1] + 1110, own[2]]);
        assert_eq!(history_event_costs(&events), own);
    }

    // Python: tests/test_turn_footprint.py::test_outside_a_turn_there_is_no_footprint
    #[tokio::test]
    async fn outside_a_turn_there_is_no_footprint() {
        assert_eq!(current_turn_footprint(), None);
        let footprint = TurnFootprint::new(12, 400);
        let read = measured(footprint.clone(), async { current_turn_footprint() }).await;
        assert_eq!(read, Some(footprint));
    }
}
