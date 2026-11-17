//! Pair every published external START with one END, at the reader boundary.

use roomkit::TurnHost;
use roomkit::model::{ToolCall, ToolCallOutcome};

use super::super::tool_events::{Ended, ToolEvents};
use super::ToolEnd;

pub(super) struct Closing {
    events: Option<ToolEvents>,
    turn: Option<TurnHost>,
    call: ToolCall,
    ended: Option<(Ended, u64)>,
    handing: bool,
}

impl Closing {
    pub(super) fn started(
        events: Option<ToolEvents>,
        turn: Option<TurnHost>,
        call: &ToolCall,
    ) -> Self {
        if let Some(events) = &events {
            events.started([call]);
        }
        Self {
            events,
            turn,
            call: call.clone(),
            ended: None,
            handing: false,
        }
    }

    pub(super) fn ended(&mut self, end: &ToolEnd) {
        self.ended = Some((
            Ended {
                call: end.call.clone(),
                result: end.result.as_text(),
                failed: end.outcome != ToolCallOutcome::Served,
            },
            u64::try_from(end.duration.as_millis()).unwrap_or(u64::MAX),
        ));
    }

    pub(super) fn handing(&mut self) {
        self.handing = true;
    }

    pub(super) fn handed(mut self) {
        self.publish(false);
    }

    fn publish(&mut self, cut: bool) {
        let Some(events) = self.events.take() else {
            return;
        };
        if !cut && let Some((ended, duration)) = &self.ended {
            events.ended(std::slice::from_ref(ended), Some(*duration));
        } else {
            events.ended(&[Ended::aborted(self.call.clone())], None);
        }
    }
}

impl Drop for Closing {
    fn drop(&mut self) {
        let cut = self.handing && self.turn.as_ref().is_some_and(TurnHost::stream_was_cut);
        self.publish(cut);
    }
}
