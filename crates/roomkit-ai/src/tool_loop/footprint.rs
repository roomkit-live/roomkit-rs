//! What a turn takes of the context window besides its history, measured
//! as its first round will send it, before the memory reads the room (RFC
//! §20): Python's `AIChannel._measure_turn`.

use roomkit_core::rules::transcript::{Message, SPEAKER_ATTRIBUTION_NOTE, turn_notes};

use super::Loop;
use crate::memory::{
    TurnFootprint, estimate_message_tokens, estimate_tokens, estimate_tool_tokens,
};
use crate::provider::{AiProvider, ai_messages};

/// The note Python's runtime gives a turn whose history holds an answer a
/// barge-in cut: a turn's measure keeps room for it, whether its history
/// holds one or not, as Python's does.
const CUT_NOTE: &str = "[Cut answers, from the runtime: when the person cuts you off, your answer \
     stays whole in the conversation, followed by a mark that says how far they heard it. Only \
     an answer followed by such a mark was cut; every answer of yours without one was heard to \
     its end.]";

impl<P: AiProvider> Loop<P> {
    /// The turn's footprint: its system prompt, the tools its first round
    /// declares, room for the notes the channel may add to its input (the
    /// speaker attribution, a cut answer's note), and the earlier `rounds`
    /// of tool calls its history replays, all of them (the window may drop
    /// some, never add one); its reply budget apart.
    pub(crate) fn footprint(&self, rounds: &[Vec<Message>]) -> TurnFootprint {
        let prompt = estimate_tokens(self.system_prompt.as_deref().unwrap_or(""));
        let declared = self.shown_tools(self.toolbox.declared(&self.policy));
        let tools: usize = declared.iter().map(estimate_tool_tokens).sum();
        let room = [SPEAKER_ATTRIBUTION_NOTE.to_owned(), CUT_NOTE.to_owned()];
        let notes = estimate_tokens(&turn_notes(&room).unwrap_or_default());
        let replayed: usize = rounds.iter().map(|round| self.round_tokens(round)).sum();
        let reply = self.max_tokens.map_or(0, |tokens| tokens as usize);
        TurnFootprint::new(prompt + tools + notes + replayed, reply)
    }

    /// What a replayed round costs, as the provider reads it: its calls,
    /// then its results together in one message.
    fn round_tokens(&self, round: &[Message]) -> usize {
        ai_messages(&self.shown_messages(round))
            .iter()
            .map(estimate_message_tokens)
            .sum()
    }
}
