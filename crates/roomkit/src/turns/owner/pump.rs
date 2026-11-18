//! The owner's pump of one turn: it reads every frame as it comes, applies
//! at once those that change what the owner holds or ask it to act, queues
//! the others for the turn's reader, beats every interval, and watches the
//! executor (RFC §10.6.3, §10.6.8).

mod drain;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use roomkit_core::model::{ChannelId, EventId, Metadata, RoomId};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::{Facts, Main, OwnerState, call_context};
use crate::context::ExecutionContext;
use crate::tasks::Unit;
use crate::turn::TurnKit;
use crate::turns::codec::decode_end;
use crate::turns::context::TurnContext;
use crate::turns::error::{TurnError, one};
use crate::turns::pipe::PipeReceiver;
use crate::turns::timeouts::Beat;
use crate::turns::wire::{CallContext, Control, Frame, Item};
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

/// Emits `event_type`, which the turn of `facts` emitted on its executor
/// naming `room_id`, `channel_id` and `event_id`, as [`Pump::publish`]
/// does: on `kit`'s bus, its handlers at least as deep as the turn's
/// response, as those of an event a turn of the kit's own executor emits
/// (RFC §8.3).
fn publish_for_turn(
    kit: &dyn TurnKit,
    facts: &Facts,
    event_type: String,
    (room_id, channel_id, event_id): (Option<&str>, Option<&str>, Option<String>),
    data: Metadata,
) {
    publish_for_room(
        kit,
        (&facts.room_id, &facts.channel_id, facts.response_depth),
        event_type,
        (room_id, channel_id, event_id),
        data,
    );
}

/// Publish a framework event during or after its turn within the owner's scope.
pub(crate) fn publish_for_room(
    kit: &dyn TurnKit,
    (room, channel, depth): (&RoomId, &ChannelId, u32),
    event_type: String,
    (room_id, channel_id, event_id): (Option<&str>, Option<&str>, Option<String>),
    data: Metadata,
) {
    if !emitted_by_its_turn(room, channel, &event_type, room_id, channel_id) {
        tracing::warn!(target: "roomkit::turns", room_id = %room, channel_id = %channel, %event_type,
            "a turn emitted a framework event outside its room or kind: dropped");
        return;
    }
    let ids = (
        Some(room.clone()),
        Some(channel.clone()),
        event_id.and_then(|id| EventId::new(id).ok()),
    );
    ExecutionContext::current()
        .at_least(depth)
        .sync_scope(|| kit.emit(event_type, ids, data));
}

pub(in crate::turns) fn emitted_by_its_turn(
    room: &RoomId,
    channel: &ChannelId,
    event_type: &str,
    room_id: Option<&str>,
    channel_id: Option<&str>,
) -> bool {
    TURN_FRAMEWORK_EVENTS.contains(&event_type)
        && room_id.is_none_or(|named| named == room.as_str())
        && channel_id.is_none_or(|named| named == channel.as_str())
}

/// A handler's own unit, excluding the turn unit its siblings share.
struct RunningCall {
    task: JoinHandle<()>,
    unit: Unit,
}

/// The owner's reading of one turn's frames.
pub(super) struct Pump {
    state: Arc<OwnerState>,
    frames: PipeReceiver<Frame>,
    main: mpsc::UnboundedSender<Main>,
    kit: Arc<dyn TurnKit>,
    turns: Arc<Turns>,
    /// The verbs running for the turn, by their call.
    calls: HashMap<String, RunningCall>,
    /// Withdrawn calls still unwinding; their inline work remains the turn's.
    withdrawn: Vec<RunningCall>,
    publisher: Arc<crate::turn::publish::Publisher>,
    client_publications: crate::turns::client::publications::Publications,
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
            withdrawn: Vec::new(),
            publisher: Arc::default(),
            client_publications: Default::default(),
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
        let mut deadline = *cancelled.borrow_and_update();
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
                        deadline = *cancelled.borrow_and_update();
                    }
                }
                () = until(deadline) => break Ending::GaveUp,
                silent = beat.tick_with(|| {
                    self.accepted && self.state.turn.heard_executor()
                }) => {
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
        let cancelling =
            self.state.cancel.borrow().is_some() && !self.state.steer_cut.load(Ordering::Acquire);
        let ending = if cancelling && matches!(ending, Ending::Ended) {
            self.end_calls().await
        } else {
            ending
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
                if executor_id != self.turns.executor_id() {
                    self.state.remote_executor.get_or_init(|| executor_id);
                }
            }
            // A beat says an executor runs the turn, as `accepted` does.
            Frame::Heartbeat => self.accepted = true,
            Frame::Output { output, streams } => self.hand(Main::Output(output, streams)),
            Frame::Item { item } => return self.item(seq, item),
            Frame::ToolArgs { start, arguments } => {
                lock(&self.state.ran_with).insert(start, arguments);
            }
            // A call's end told before its end item: a cut closes the call
            // with it (RFC §12.2 step 13s).
            Frame::ToolEnded { start, end } => {
                lock(&self.state.told_ends).insert(start, decode_end(end));
            }
            Frame::Error { error } => self.hand(Main::Error(error)),
            Frame::End => return Some(Ending::Ended),
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
            } => self.serve_tool(call_id, name, arguments, context),
            Frame::CallCancel { call_id } => {
                if let Some(call) = self.calls.remove(&call_id)
                    && !self.kit.is_close_caller(call.task.id(), call.unit)
                {
                    call.task.abort();
                    self.withdrawn.retain(|call| !call.task.is_finished());
                    self.withdrawn.push(call);
                }
            }
            frame @ (Frame::FrameworkEvent { .. } | Frame::Ephemeral { .. })
                if self.state.facts.client.is_some() =>
            {
                self.client_publications.queue(
                    self.kit.as_ref(),
                    &self.state.facts,
                    lock(&self.state.waiting).clone(),
                    frame,
                    self.state.turn.traced.load(Ordering::Acquire),
                );
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
            Frame::SteerResult { steer_id, loops } => {
                self.state.steers.answer(&steer_id, loops);
            }
            Frame::Record { record } => self.hand(Main::Record(record)),
            // The turn's human-input requests, which the owner answers
            // (RFC §10.6.5).
            Frame::InputRequest { request } => super::input_request::opened(
                &self.state,
                &self.turns.inputs,
                self.input_turn(),
                *request,
            ),
            Frame::InputClosed { request_id, .. } => {
                self.turns.inputs.closed(&self.input_turn(), &request_id);
            }
            Frame::InputTaken { request_id, taken } => {
                let turn = self.input_turn();
                self.turns.inputs.taken(&turn, &request_id, taken);
            }
            Frame::Ephemeral {
                channel: topic,
                event,
            } => {
                let facts = &self.state.facts;
                if let Some(event) = crate::turns::outside::ephemeral(
                    &facts.room_id,
                    &facts.channel_id,
                    facts.answer_unscoped,
                    self.state.turn.traced.load(Ordering::Acquire),
                    &topic,
                    event,
                ) {
                    self.publisher.enqueue(
                        &self.kit,
                        &facts.room_id,
                        &facts.channel_id,
                        facts.room.created_at,
                        event,
                    );
                }
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
            | Item::StreamEnd
            | Item::InstructionRead { .. } => 1,
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
        let waiting = lock(&self.state.waiting)
            .clone()
            .in_turn(Arc::clone(&self.state.turn.turn_id));
        let state = Arc::clone(&self.state);
        let kit = Arc::clone(&self.kit);
        let answering = call_id.clone();
        let task = async move {
            let served = call_context::serve(&kit, &state.facts, &verb, arguments, &context);
            let answered = waiting.scope(served).await;
            answer(&state, answering, answered);
        };
        if let Some((task, unit)) = self.kit.spawn_owner_call(Box::pin(task)) {
            self.calls.retain(|_, call| !call.task.is_finished());
            self.calls.insert(call_id, RunningCall { task, unit });
        } else {
            let refused = one("backend_closed", "the kit is shut down".to_owned());
            answer(
                &self.state,
                call_id,
                call_context::HostAnswer::failed(refused),
            );
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
        context: CallContext,
    ) {
        let waiting = lock(&self.state.waiting)
            .clone()
            .in_turn(Arc::clone(&self.state.turn.turn_id));
        let state = Arc::clone(&self.state);
        let kit = Arc::clone(&self.kit);
        let floor = context.chain_depth.max(state.facts.response_depth);
        let answering = call_id.clone();
        let task = async move {
            let call = crate::turns::wire::ToolCall {
                call_id: answering.clone(),
                name,
                arguments,
                context,
            };
            let served = call_context::serve_tool(&kit, &state.facts, call);
            let answered = waiting.at_least(floor).scope(served).await;
            answer_tool(&state, answering, answered);
        };
        if let Some((task, unit)) = self.kit.spawn_owner_call(Box::pin(task)) {
            self.calls.retain(|_, call| !call.task.is_finished());
            self.calls.insert(call_id, RunningCall { task, unit });
        } else {
            let refused = crate::turn::OwnerToolAnswer::Failed {
                kind: "backend_closed".to_owned(),
                message: "the kit is shut down".to_owned(),
            };
            answer_tool(&self.state, call_id, refused.into());
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
        let named = (room_id, channel_id, event_id);
        publish_for_turn(
            self.kit.as_ref(),
            &self.state.facts,
            event_type,
            named,
            data,
        );
    }

    /// The executor fell silent, or never said it took the turn within the
    /// accept bound: the turn fails `executor_lost`, an executor having
    /// been handed it (the kit's own always is: `no_executor` is for a turn
    /// no executor could take), and the executor is told, best effort (RFC
    /// §10.6.8).
    fn lost(&self) -> Ending {
        let reason = if !self.accepted
            && self
                .state
                .turn
                .application_link
                .get()
                .is_some_and(|link| !link.taken())
        {
            "no_executor"
        } else {
            "executor_lost"
        };
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
            self.state.end_owed.owe();
        }
        match ending {
            Ending::Ended => self.hand(Main::End),
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
            if !self.kit.is_close_caller(call.task.id(), call.unit) {
                call.task.abort();
            }
        }
        self.state.over.send_replace(true);
        // Its human-input requests are withdrawn, an answer awaiting its
        // verdict not taken (RFC §10.6.5).
        self.turns.inputs.turn_ended(&self.input_turn());
        // Over first: a steer that comes after reads it and waits for none.
        self.state.steers.end();
        self.state.turn.shut();
        *lock(&self.state.waiting) = TurnContext::default();
    }
}

/// Answers the call `call_id` with what its verb made, while the turn is
/// the owner's; a result that cannot cross is answered as a failure.
fn answer(state: &OwnerState, call_id: String, answered: call_context::HostAnswer) {
    if *state.over.borrow() {
        return;
    }
    let (result, error) = match answered.outcome {
        Ok(result) => (result, None),
        Err(error) => (Value::Null, Some(*error)),
    };
    let control = Control::HostResult {
        call_id: call_id.clone(),
        result,
        error,
        record_changes: answered.record_changes,
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
            record_changes: None,
        }));
    }
}

/// Answers the tool call `call_id` with what its handler made, while the
/// turn is the owner's, as a `tool_result`: served, the answer under
/// `value` as Python's codec writes it.
fn answer_tool(state: &OwnerState, call_id: String, answered: call_context::ToolAnswer) {
    use crate::turn::OwnerToolAnswer;
    use crate::turns::wire::ToolOutcome;
    if *state.over.borrow() {
        return;
    }
    let failure = |kind: &str, message: String| Some(one(kind, message));
    let (outcome, result, error) = match answered.outcome {
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
        error: answered.error.or(error),
        structured_content: answered.structured_content,
        record_changes: answered.record_changes,
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

    /// RFC §8.3, as Python's owner publishes a turn's frames: a handler of
    /// a framework event another process's executor emitted for a turn runs
    /// at least as deep as the turn's response.
    #[tokio::test]
    async fn a_foreign_turn_s_event_handlers_run_at_its_depth() {
        let kit = crate::RoomKit::new(crate::RoomKitOptions::default());
        let (seen, mut depths) = mpsc::unbounded_channel();
        kit.on("tool_call", move |_event| {
            let seen = seen.clone();
            async move {
                seen.send(ExecutionContext::current().turn_depth()).unwrap();
            }
        })
        .unwrap();
        let facts = Facts {
            room_id: RoomId::new("r1").unwrap(),
            room: Arc::new(roomkit_core::model::Room::new(RoomId::new("r1").unwrap())),
            room_metadata: Arc::default(),
            channel_id: ChannelId::new("ai").unwrap(),
            channel_type: roomkit_core::model::ChannelType::AI,
            event_created_at: roomkit_core::model::Timestamp::UNIX_EPOCH,
            response_depth: 3,
            answer_unscoped: true,
            organization_id: None,
        };
        let turn_kit: Arc<dyn TurnKit> = Arc::new(Arc::downgrade(&kit.inner));
        let named = (Some("r1"), None, None);
        publish_for_turn(
            turn_kit.as_ref(),
            &facts,
            "tool_call".to_owned(),
            named,
            Metadata::new(),
        );
        assert_eq!(depths.recv().await, Some(Some(3)));
        kit.close().await.unwrap();
    }

    /// RFC §10.6.3, as Python's `publish_frame`: a turn's executor reports
    /// its own room's and channel's events of the types a turn emits; a
    /// room's lifecycle, or another room's or channel's event, is dropped.
    #[test]
    fn a_turn_emits_only_its_kinds_about_its_own_room_and_channel() {
        let facts = Facts {
            room_id: RoomId::new("r1").unwrap(),
            room: Arc::new(roomkit_core::model::Room::new(RoomId::new("r1").unwrap())),
            room_metadata: Arc::default(),
            channel_id: ChannelId::new("ai").unwrap(),
            channel_type: roomkit_core::model::ChannelType::AI,
            event_created_at: roomkit_core::model::Timestamp::UNIX_EPOCH,
            response_depth: 1,
            answer_unscoped: true,
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
                emitted_by_its_turn(
                    &facts.room_id,
                    &facts.channel_id,
                    event_type,
                    room_id,
                    channel_id,
                )
            }),
            [true, true, false, false, false]
        );
    }
}
