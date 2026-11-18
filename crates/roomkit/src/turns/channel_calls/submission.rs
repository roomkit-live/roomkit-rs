//! Restore an unsubmitted remote call's previous entry, including on cancellation.

use roomkit_core::model::{ChannelId, RoomId, Timestamp};

use super::{ChannelCall, ChannelResult, Handover, PreviousTurn, Turns};

pub(super) struct SubmittedRemoteCall<'a> {
    turns: &'a Turns,
    channel: ChannelId,
    room: RoomId,
    id: String,
    previous: Option<PreviousTurn>,
    handover: Handover,
    answered: bool,
}

impl<'a> SubmittedRemoteCall<'a> {
    pub(super) fn new(
        turns: &'a Turns,
        call: &mut ChannelCall,
        created_at: impl Into<Option<Timestamp>>,
    ) -> Self {
        let created_at = created_at.into();
        let mut records = turns.records();
        if let Some(mark) = created_at {
            records.mark_room(&call.room_id, mark);
        }
        let previous = records.get(&call.channel_id, &call.room_id).cloned();
        call.previous_turn.clone_from(&previous);
        let made = PreviousTurn::after(&call.call_id, previous.as_ref(), None);
        records.put(&call.channel_id, &call.room_id, made);
        if let Some(mark) = created_at {
            records.mark_room(&call.room_id, mark);
        }
        Self {
            turns,
            channel: call.channel_id.clone(),
            room: call.room_id.clone(),
            id: call.call_id.clone(),
            previous,
            handover: call.handover.clone(),
            answered: false,
        }
    }

    pub(super) fn complete(mut self, answer: Option<&ChannelResult>) {
        self.answered = true;
        if let Some(executor) = answer.and_then(|a| a.executor_id.as_ref()) {
            self.turns
                .took(&self.channel, &self.room, &self.id, executor);
        }
    }
}

impl Drop for SubmittedRemoteCall<'_> {
    fn drop(&mut self) {
        if self.answered || self.handover.handed() {
            return;
        }
        self.turns.records().undo_submission(
            &self.channel,
            &self.room,
            &self.id,
            self.previous.take(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn call(id: &str) -> ChannelCall {
        serde_json::from_value(json!({
            "call_id": id, "channel_id": "agent", "room_id": "room", "verb": "freeze"
        }))
        .unwrap()
    }

    fn previous(turns: &Turns, call: &ChannelCall) -> Option<PreviousTurn> {
        turns
            .records()
            .get(&call.channel_id, &call.room_id)
            .cloned()
    }

    #[test]
    fn cancellation_retains_the_submission_only_after_handover() {
        let turns = Turns::default();
        let mut first = call("first");
        let mark = Timestamp::UNIX_EPOCH;
        drop(SubmittedRemoteCall::new(&turns, &mut first, mark));
        assert!(previous(&turns, &first).is_none());

        let submitted = SubmittedRemoteCall::new(&turns, &mut first, mark);
        first.hand_over();
        drop(submitted);
        assert_eq!(previous(&turns, &first).unwrap().turn_id, "first");

        let mut retry = call("retry");
        let submitted = SubmittedRemoteCall::new(&turns, &mut retry, mark);
        assert_eq!(retry.previous_turn.as_ref().unwrap().turn_id, "first");
        retry.hand_over();
        retry.refused();
        drop(submitted);
        assert_eq!(previous(&turns, &retry), retry.previous_turn);
    }

    #[test]
    fn cancelling_an_earlier_call_preserves_a_later_successful_answer() {
        let turns = Turns::default();
        let mark = Timestamp::UNIX_EPOCH;
        let mut earlier = call("earlier");
        let mut later = call("later");
        let earlier_attempt = SubmittedRemoteCall::new(&turns, &mut earlier, mark);
        let later_attempt = SubmittedRemoteCall::new(&turns, &mut later, mark);
        later_attempt.complete(Some(&ChannelResult {
            result: json!(true),
            error: None,
            room_state: None,
            executor_id: Some("worker".to_owned()),
        }));
        drop(earlier_attempt);
        let record = previous(&turns, &later).unwrap();
        assert_eq!(record.turn_id, "later");
        assert_eq!(record.executor_id.as_deref(), Some("worker"));
    }
}
