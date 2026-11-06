//! The owner's pump of one turn: it reads every frame as it comes, applies
//! at once those that change what the owner holds or ask it to act, queues
//! the others for the turn's reader, beats every interval, and watches the
//! executor (RFC §10.6.3, §10.6.8).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use roomkit_core::model::{EventId, Metadata};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tokio::time::Instant;

use super::{Facts, Main, OwnerState, verbs};
use crate::context::ExecutionContext;
use crate::turn::TurnKit;
use crate::turns::error::{TurnError, one};
use crate::turns::pipe::PipeReceiver;
use crate::turns::timeouts::Beat;
use crate::turns::wire::{CallContext, Control, Frame, Item, WireError};
use crate::turns::{InputTurn, Turns, lock};

/// How the pump's watch of a turn ended.
#[derive(Debug)]
enum Ending {
    /// The executor's end came.
    Ended,
    /// A cancelled turn did not end within its bound.
    GaveUp,
    /// The link failed, or the executor fell silent.
    Failed(TurnError),
}

/// The framework events a turn's executor emits (RFC §8.2), Python's
/// `TURN_FRAMEWORK_EVENTS`: the owner emits no other a `framework_event`
/// frame names.
const TURN_FRAMEWORK_EVENTS: [&str; 6] = [
    "tool_call",
    "before_tool_use",
    "user_input_required",
    "ai_response",
    "before_ai_generation",
    "hook_timeout",
];

/// Whether a turn of `facts` may emit `event_type` naming `room_id` and
/// `channel_id`: one of [`TURN_FRAMEWORK_EVENTS`], about its own room and
/// channel or none (Python's `names_its_room`).
fn emitted_by_its_turn(
    facts: &Facts,
    event_type: &str,
    room_id: Option<&str>,
    channel_id: Option<&str>,
) -> bool {
    TURN_FRAMEWORK_EVENTS.contains(&event_type)
        && room_id.is_none_or(|named| named == facts.room_id.as_str())
        && channel_id.is_none_or(|named| named == facts.channel_id.as_str())
}

/// The owner's reading of one turn's frames.
pub(super) struct Pump {
    state: Arc<OwnerState>,
    frames: PipeReceiver<Frame>,
    main: mpsc::UnboundedSender<Main>,
    kit: Arc<dyn TurnKit>,
    turns: Arc<Turns>,
    /// The verbs running for the turn, by their call.
    calls: HashMap<String, AbortHandle>,
    accepted: bool,
    /// The items received, deltas counted: never more than granted.
    received: u64,
}

impl Pump {
    pub(super) fn new(
        state: Arc<OwnerState>,
        frames: PipeReceiver<Frame>,
        main: mpsc::UnboundedSender<Main>,
        kit: Arc<dyn TurnKit>,
        turns: Arc<Turns>,
    ) -> Self {
        Self {
            state,
            frames,
            main,
            kit,
            turns,
            calls: HashMap::new(),
            accepted: false,
            received: 0,
        }
    }

    /// Reads the turn's frames until its end, beating and watching the
    /// executor meanwhile.
    pub(super) async fn run(mut self) {
        let timeouts = self.state.turn.timeouts;
        let mut beat = Beat::new(&timeouts, false);
        let mut cancelled = self.state.cancel.subscribe();
        let mut deadline = cancelled
            .borrow_and_update()
            .is_some()
            .then(|| Instant::now() + timeouts.cancel);
        let ending = loop {
            tokio::select! {
                biased;
                frame = self.frames.get() => {
                    beat.heard();
                    match frame {
                        Ok((seq, frame)) => {
                            if let Some(ending) = self.apply(seq, frame) {
                                break ending;
                            }
                        }
                        Err(failure) => break Ending::Failed(TurnError::link(&failure)),
                    }
                }
                Ok(()) = cancelled.changed(), if deadline.is_none() => {
                    // A steering cut waits for the end as after a close
                    // (RFC §10.6.8).
                    if !self.state.steer_cut.load(Ordering::Acquire) {
                        deadline = Some(Instant::now() + timeouts.cancel);
                    }
                }
                () = until(deadline) => break Ending::GaveUp,
                silent = beat.tick() => {
                    let limit = if self.accepted {
                        timeouts.silent_intervals()
                    } else {
                        timeouts.accept_intervals()
                    };
                    if silent >= limit {
                        break self.lost();
                    }
                    drop(self.state.turn.controls.put(&Control::Heartbeat));
                }
            }
        };
        self.finish(ending);
    }

    /// Applies one frame; how the watch ends, when the frame ends it.
    fn apply(&mut self, seq: u64, frame: Frame) -> Option<Ending> {
        match frame {
            Frame::Accepted { executor_id } => {
                self.accepted = true;
                let facts = &self.state.facts;
                self.turns.took(
                    &facts.channel_id,
                    &facts.room_id,
                    &self.state.turn.turn_id,
                    &executor_id,
                );
            }
            // A beat says an executor runs the turn, as `accepted` does.
            Frame::Heartbeat => self.accepted = true,
            Frame::Output { output, streams } => self.hand(Main::Output(output, streams)),
            Frame::Item { item } => return self.item(seq, item),
            Frame::ToolArgs { start, arguments } => {
                lock(&self.state.ran_with).insert(start, arguments);
            }
            Frame::Error { error } => self.hand(Main::Error(error)),
            Frame::End => {
                self.hand(Main::End);
                return Some(Ending::Ended);
            }
            Frame::HostCall {
                call_id,
                verb,
                arguments,
                context,
            } => self.serve(call_id, verb, arguments, context),
            Frame::ToolCall {
                call_id,
                name,
                arguments,
                context,
            } => self.serve_tool(call_id, name, arguments, &context),
            Frame::CallCancel { call_id } => {
                if let Some(call) = self.calls.remove(&call_id) {
                    call.abort();
                }
            }
            Frame::FrameworkEvent {
                event_type,
                room_id,
                channel_id,
                event_id,
                data,
            } => self.publish(
                event_type,
                room_id.as_deref(),
                channel_id.as_deref(),
                event_id,
                data,
            ),
            // What the reader settles itself, before it closes (a call's
            // early end), and the ephemerals no owner of 0.1 publishes.
            Frame::SteerResult { steer_id, loops } => {
                self.state.steers.answer(&steer_id, loops);
            }
            Frame::Record { record } => self.hand(Main::Record(record)),
            // The turn's human-input requests, which the owner answers
            // (RFC §10.6.5).
            Frame::InputRequest { request_id, .. } => {
                self.turns.inputs.opened(self.input_turn(), &request_id);
            }
            Frame::InputClosed { request_id, .. } => {
                self.turns.inputs.closed(&self.input_turn(), &request_id);
            }
            Frame::InputTaken { request_id, taken } => {
                let turn = self.input_turn();
                self.turns.inputs.taken(&turn, &request_id, taken);
            }
            Frame::ToolEnded { .. } | Frame::Ephemeral { .. } => {
                tracing::trace!(target: "roomkit::turns", turn = %self.state.turn.turn_id, "a frame left unread");
            }
        }
        None
    }

    /// An item for the reader, within what it granted.
    fn item(&mut self, seq: u64, item: Item) -> Option<Ending> {
        let counted = match &item {
            Item::Text {
                deltas: Some(deltas),
                ..
            } => u64::from(*deltas),
            Item::Text { deltas: None, .. }
            | Item::Thinking { .. }
            | Item::ToolStart { .. }
            | Item::ToolEnd { .. }
            | Item::SegmentBreak
            | Item::Usage { .. }
            | Item::LoopEnd(_)
            | Item::StreamEnd => 1,
        };
        self.received = self.received.saturating_add(counted);
        if self.received > self.state.granted.load(Ordering::Acquire) {
            return Some(Ending::Failed(TurnError::new(
                "frame_invalid",
                "the executor sent an item past what its reader granted",
            )));
        }
        self.hand(Main::Item(seq, item));
        None
    }

    /// Runs a verb the turn's host asks, in a copy of the context of the
    /// call waiting on the turn, as a task the turn cancels when it ends.
    fn serve(&mut self, call_id: String, verb: String, arguments: Metadata, context: CallContext) {
        let waiting = lock(&self.state.waiting).clone();
        let state = Arc::clone(&self.state);
        let kit = Arc::clone(&self.kit);
        let answering = call_id.clone();
        let task = async move {
            let served = verbs::serve(&kit, &state.facts, &verb, arguments, &context);
            let answered = waiting.scope(served).await;
            answer(&state, answering, answered);
        };
        if let Some(running) = self.kit.spawn_tracked(Box::pin(task)) {
            self.calls.retain(|_, call| !call.is_finished());
            self.calls.insert(call_id, running);
        } else {
            let refused = one("backend_closed", "the kit is shut down".to_owned());
            answer(&self.state, call_id, Err(Box::new(refused)));
        }
    }

    /// Serves the call `call_id` of the owner's tool `name` with
    /// `arguments` (RFC §10.6.5), in the turn's room for its channel, at
    /// least as deep as the turn's response, its answer sent back as a
    /// `tool_result`; cancelled, as a verb is, by its `call_cancel`.
    fn serve_tool(
        &mut self,
        call_id: String,
        name: String,
        arguments: Metadata,
        context: &CallContext,
    ) {
        let waiting = lock(&self.state.waiting).clone();
        let state = Arc::clone(&self.state);
        let kit = Arc::clone(&self.kit);
        let floor = context.chain_depth.max(state.facts.response_depth);
        let call = crate::turn::OwnerCall {
            room_id: state.facts.room_id.clone(),
            channel_id: state.facts.channel_id.clone(),
            tool_call_id: context.tool_call_id.clone(),
            arguments,
        };
        let answering = call_id.clone();
        let task = async move {
            let served = kit.serve_owner_tool(name, call);
            let answered = waiting.at_least(floor).scope(served).await;
            answer_tool(&state, answering, answered);
        };
        if let Some(running) = self.kit.spawn_tracked(Box::pin(task)) {
            self.calls.retain(|_, call| !call.is_finished());
            self.calls.insert(call_id, running);
        } else {
            let refused = crate::turn::OwnerToolAnswer::Failed {
                kind: "backend_closed".to_owned(),
                message: "the kit is shut down".to_owned(),
            };
            answer_tool(&self.state, call_id, refused);
        }
    }

    /// Emits a framework event the turn emitted on its executor (RFC
    /// §10.6.3), as the turn's channel in the turn's room: one of the types
    /// a turn emits ([`TURN_FRAMEWORK_EVENTS`]), naming that room and that
    /// channel or none, as Python's `publish_frame` checks it. Any other is
    /// dropped, so an executor cannot report a room's lifecycle.
    fn publish(
        &self,
        event_type: String,
        room_id: Option<&str>,
        channel_id: Option<&str>,
        event_id: Option<String>,
        data: Metadata,
    ) {
        let facts = &self.state.facts;
        if !emitted_by_its_turn(facts, &event_type, room_id, channel_id) {
            tracing::warn!(
                target: "roomkit::turns",
                room_id = %facts.room_id,
                channel_id = %facts.channel_id,
                %event_type,
                "a turn emitted a framework event outside its room or kind: dropped"
            );
            return;
        }
        let ids = (
            Some(facts.room_id.clone()),
            Some(facts.channel_id.clone()),
            event_id.and_then(|id| EventId::new(id).ok()),
        );
        self.kit.emit(event_type, ids, data);
    }

    /// The executor fell silent, or never said it took the turn within the
    /// accept bound: the turn fails `executor_lost`, an executor having
    /// been handed it (the kit's own always is: `no_executor` is for a turn
    /// no executor could take), and the executor is told, best effort (RFC
    /// §10.6.8).
    fn lost(&self) -> Ending {
        let reason = "executor_lost";
        tracing::warn!(
            target: "roomkit::turns",
            turn = %self.state.turn.turn_id,
            accepted = self.accepted,
            "a turn's executor fell silent"
        );
        drop(self.state.turn.controls.put(&Control::Cancel {
            reason: reason.to_owned(),
        }));
        Ending::Failed(TurnError::link_reason(reason))
    }

    fn hand(&self, main: Main) {
        drop(self.main.send(main));
    }

    /// The turn as the owner's record of its human-input requests names it.
    fn input_turn(&self) -> Arc<dyn InputTurn> {
        let turn: Arc<crate::turns::MemoryTurn> = Arc::clone(&self.state.turn);
        turn
    }

    /// The turn is over for the owner: its reader learns why; dropped, the
    /// pump cancels its verbs and drops later frames. Over without its
    /// executor's end, the turn's end is owed, told before it is over.
    fn finish(self, ending: Ending) {
        if !matches!(ending, Ending::Ended) {
            self.state.end_owed.store(true, Ordering::Release);
        }
        match ending {
            Ending::Ended => {}
            Ending::GaveUp => {
                tracing::warn!(
                    target: "roomkit::turns",
                    turn = %self.state.turn.turn_id,
                    bound_s = self.state.turn.timeouts.cancel.as_secs_f64(),
                    "a turn did not end within its cancel bound: drainage incomplete"
                );
                self.hand(Main::Failed(TurnError::new(
                    "cancelled",
                    "the turn did not end within its cancel bound: drainage incomplete",
                )));
            }
            Ending::Failed(error) => self.hand(Main::Failed(error)),
        }
    }
}

impl Drop for Pump {
    /// However the pump ends, its task aborted with the kit's shutdown
    /// included: the turn is over for the owner, its verbs cancelled, its
    /// pipes shut, and whoever awaits its end told.
    fn drop(&mut self) {
        for call in self.calls.values() {
            call.abort();
        }
        self.state.over.send_replace(true);
        // Its human-input requests are withdrawn, an answer awaiting its
        // verdict not taken (RFC §10.6.5).
        self.turns.inputs.turn_ended(&self.input_turn());
        // Over first: a steer that comes after reads it and waits for none.
        self.state.steers.end();
        self.state.turn.shut();
        *lock(&self.state.waiting) = ExecutionContext::default();
    }
}

/// Answers the call `call_id` with what its verb made, while the turn is
/// the owner's; a result that cannot cross is answered as a failure.
fn answer(state: &OwnerState, call_id: String, answered: Result<Value, Box<WireError>>) {
    if *state.over.borrow() {
        return;
    }
    let (result, error) = match answered {
        Ok(result) => (result, None),
        Err(error) => (Value::Null, Some(*error)),
    };
    let control = Control::HostResult {
        call_id: call_id.clone(),
        result,
        error,
    };
    if let Err(uncarried) = state.turn.controls.put(&control) {
        let refused = one(
            "TypeError",
            format!(
                "the result of a call cannot cross the turn backend: {}",
                uncarried.0
            ),
        );
        drop(state.turn.controls.put(&Control::HostResult {
            call_id,
            result: Value::Null,
            error: Some(refused),
        }));
    }
}

/// Answers the tool call `call_id` with what its handler made, while the
/// turn is the owner's, as a `tool_result`: served, the answer under
/// `value` as Python's codec writes it.
fn answer_tool(state: &OwnerState, call_id: String, answered: crate::turn::OwnerToolAnswer) {
    use crate::turn::OwnerToolAnswer;
    use crate::turns::wire::ToolOutcome;
    if *state.over.borrow() {
        return;
    }
    let failure = |kind: &str, message: String| Some(one(kind, message));
    let (outcome, result, error) = match answered {
        OwnerToolAnswer::Served(value) => (
            ToolOutcome::Served,
            Some(serde_json::json!({ "value": value })),
            None,
        ),
        OwnerToolAnswer::Refused(message) => (
            ToolOutcome::Refused,
            None,
            failure("ToolRefusedError", message),
        ),
        OwnerToolAnswer::Unserved(message) => (
            ToolOutcome::Unserved,
            None,
            failure("UnservedToolCallError", message),
        ),
        OwnerToolAnswer::Failed { kind, message } => {
            (ToolOutcome::Failed, None, failure(&kind, message))
        }
    };
    let control = Control::ToolResult {
        call_id,
        outcome,
        result,
        error,
        structured_content: None,
    };
    drop(state.turn.controls.put(&control));
}

/// Returns at `deadline`, never without one.
async fn until(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use roomkit_core::model::{ChannelId, RoomId};

    use super::*;

    /// RFC §10.6.3, as Python's `publish_frame`: a turn's executor reports
    /// its own room's and channel's events of the types a turn emits; a
    /// room's lifecycle, or another room's or channel's event, is dropped.
    #[test]
    fn a_turn_emits_only_its_kinds_about_its_own_room_and_channel() {
        let facts = Facts {
            room_id: RoomId::new("r1").unwrap(),
            channel_id: ChannelId::new("ai").unwrap(),
            response_depth: 1,
            organization_id: None,
        };
        let cases = [
            ("tool_call", None, None),
            ("tool_call", Some("r1"), Some("ai")),
            ("room_closed", Some("r1"), None),
            ("tool_call", Some("r2"), None),
            ("before_tool_use", None, Some("sms")),
        ];
        assert_eq!(
            cases.map(|(event_type, room_id, channel_id)| {
                emitted_by_its_turn(&facts, event_type, room_id, channel_id)
            }),
            [true, true, false, false, false]
        );
    }
}
