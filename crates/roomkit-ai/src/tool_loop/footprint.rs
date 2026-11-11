//! What a turn takes of the context window besides its history, measured
//! as its first round will send it, before the memory reads the room (RFC
//! §20): Python's `AIChannel._measure_turn`.

use roomkit_core::rules::transcript::{
    Message, Replayed, SPEAKER_ATTRIBUTION_NOTE, cleaned_notes, people_note, turn_notes,
};

use super::Loop;
use crate::memory::{
    TurnFootprint, estimate_message_tokens, estimate_tokens, estimate_tool_tokens,
};
use crate::provider::{AiMessage, AiProvider, AiRole, ai_messages};

/// The note Python's runtime gives a turn whose history holds an answer a
/// barge-in cut: a turn's measure keeps room for it, whether its history
/// holds one or not, as Python's does.
const CUT_NOTE: &str = "[Cut answers, from the runtime: when the person cuts you off, your answer \
     stays whole in the conversation, followed by a mark that says how far they heard it. Only \
     an answer followed by such a mark was cut; every answer of yours without one was heard to \
     its end.]";

impl<P: AiProvider> Loop<P> {
    /// The turn's footprint: its system prompt, the tools its first round
    /// declares, measured on their own schemas (the intent property is the
    /// request's), the notes the channel adds to its input (room for the
    /// speaker attribution and a cut answer's note, and the note naming the
    /// `people` taking part when two or more do, then the `discussion`
    /// notes), and the images its input
    /// replays; its reply budget apart; and each piece of its `earlier`
    /// turns its history replays (a round of tool calls, an instruction a
    /// turn read), measured apart by its index and its turn, all of them,
    /// for the memory to charge to the history event it reads with (RFC
    /// §20, Python's `_replayed_costs`).
    pub(crate) fn footprint(
        &self,
        earlier: &[Replayed],
        people: &[String],
        discussion: &[String],
    ) -> TurnFootprint {
        let prompt = estimate_tokens(self.system_prompt.as_deref().unwrap_or(""));
        let declared = self.first_declaration();
        let tools: usize = declared.iter().map(estimate_tool_tokens).sum();
        let room: Vec<String> = [SPEAKER_ATTRIBUTION_NOTE.to_owned()]
            .into_iter()
            .chain(people_note(people))
            .chain([CUT_NOTE.to_owned()])
            .chain(cleaned_notes(discussion))
            .collect();
        let notes = estimate_tokens(&turn_notes(&room).unwrap_or_default());
        let images = self.turn_images_tokens();
        let reply = self.max_tokens.map_or(0, |tokens| tokens as usize);
        let replayed = earlier
            .iter()
            .map(|piece| {
                let tokens = self.round_tokens(&piece.messages);
                (piece.index, piece.turn.clone(), tokens)
            })
            .collect();
        TurnFootprint {
            replayed,
            ..TurnFootprint::new(prompt + tools + notes + images, reply)
        }
    }

    /// What the images the turn's input replays cost, as a message of
    /// their own: an image a thousand tokens (Python's `_measure_turn`).
    fn turn_images_tokens(&self) -> usize {
        let replayed = self.media.replayed();
        if replayed.is_empty() {
            return 0;
        }
        estimate_message_tokens(&AiMessage::new(AiRole::User, replayed.to_vec()))
    }

    /// What a replayed piece costs, as the provider reads it: a round's
    /// calls, then its results together in one message; an instruction's
    /// message.
    fn round_tokens(&self, round: &[Message]) -> usize {
        ai_messages(&self.shown_messages(round))
            .iter()
            .map(estimate_message_tokens)
            .sum()
    }
}
