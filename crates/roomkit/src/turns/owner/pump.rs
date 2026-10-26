//! The owner's pump of one turn: it reads every frame as it comes, applies
//! at once those that change what the owner holds or ask it to act, queues
//! the others for the turn's reader, beats every interval, and watches the
//! executor (RFC §10.6.3, §10.6.8).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use roomkit_core::model::Metadata;
use roomkit_core::ops::FrameworkEvent;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tokio::time::Instant;

use super::{Main, OwnerState, verbs};
use crate::context::ExecutionContext;
use crate::turn::TurnKit;
use crate::turns::error::{TurnError, one};
use crate::turns::pipe::PipeReceiver;
use crate::turns::timeouts::Beat;
use crate::turns::wire::{CallContext, Control, Frame, Item, WireError};
use crate::turns::{Turns, lock};

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
                    deadline = Some(Instant::now() + timeouts.cancel);
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
            Frame::Heartbeat => {}
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
            } => self.publish(event_type, room_id, channel_id, event_id, data),
            // What the reader settles itself, before it closes or once it
            // reads the end (a call's early end, a record no row reads), and
            // the ephemerals no owner of 0.1 publishes.
            Frame::ToolEnded { .. } | Frame::Record { .. } | Frame::Ephemeral { .. } => {
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
            | Item::LoopEnd { .. }
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

    /// Emits a framework event the turn emitted on its executor (RFC
    /// §10.6.3), when it reads as one of RFC §8.2's.
    fn publish(
        &self,
        event_type: String,
        room_id: Option<String>,
        channel_id: Option<String>,
        event_id: Option<String>,
        data: Metadata,
    ) {
        let mut fields = data;
        fields.insert("event".to_owned(), Value::String(event_type));
        for (name, value) in [
            ("room_id", room_id),
            ("channel_id", channel_id),
            ("event_id", event_id),
        ] {
            if let Some(value) = value {
                fields
                    .entry(name.to_owned())
                    .or_insert(Value::String(value));
            }
        }
        match serde_json::from_value::<FrameworkEvent>(Value::Object(fields)) {
            Ok(event) => self.kit.publish(event),
            Err(error) => {
                tracing::debug!(target: "roomkit::turns", %error, "a framework event of the turn reads as none of RFC §8.2's");
            }
        }
    }

    /// The executor fell silent: the turn fails, the executor told, best
    /// effort (RFC §10.6.8).
    fn lost(&self) -> Ending {
        let reason = if self.accepted {
            "executor_lost"
        } else {
            "no_executor"
        };
        tracing::warn!(target: "roomkit::turns", turn = %self.state.turn.turn_id, reason, "a turn's executor fell silent");
        drop(self.state.turn.controls.put(&Control::Cancel {
            reason: reason.to_owned(),
        }));
        Ending::Failed(TurnError::new(reason, "the turn's executor fell silent"))
    }

    fn hand(&self, main: Main) {
        drop(self.main.send(main));
    }

    /// The turn is over for the owner: its reader learns why; dropped, the
    /// pump cancels its verbs and drops later frames.
    fn finish(self, ending: Ending) {
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

/// Returns at `deadline`, never without one.
async fn until(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}
