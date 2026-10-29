//! The kit's own executor: it takes each turn the backend hands it, runs
//! the channel's `on_event()`, and advances its stream as the owner's
//! pulls allow, every frame it sends serialized (RFC §10.6.2, §10.6.4,
//! §10.6.8).
//!
//! A turn runs as one task of the kit's: its work, the turn's
//! `on_event()` then its stream, polled beside the owner's control
//! messages and the executor's beat. The work runs in a copy of the
//! context of the owner's call that submitted the turn, its stream in a
//! copy of the context of the call that first read it, each within the
//! turn's host ([`crate::current_turn`]).
//!
//! - A `cancel` drops the work: the channel's `on_event()` or its stream,
//!   and the tool calls it was running, which are reported `cancelled` to
//!   the kit's observers; then the end.
//! - A `close` stops the stream: at once when it reads ahead past a
//!   delta, after the read under way otherwise, a call's end read then
//!   sent as `tool_ended`; then the end, once every verb the turn asked
//!   was answered.
//! - An owner silent for `heartbeat_timeout` cuts the turn `owner_lost`:
//!   nothing more is sent.
//! - A stream advances only within what the owner granted, and never past
//!   an item that is no delta until a later pull (`pulls`).

mod link;
mod work;

use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::Arc;

use roomkit_core::rules::chain_depth::response_depth;

pub(crate) use self::link::HostLink;
use self::link::{FrameSender, TurnFacts};
use self::work::{Flow, work};
use super::error::{TurnError, one};
use super::pipe::{LinkFailure, MAX_MESSAGE_BYTES, PipeReceiver, Uncarried};
use super::timeouts::Beat;
use super::wire::{ContextRest, Control, Frame, RequestRest, TurnRequest};
use super::{CarriedRequest, MemoryTurn, Turns, lock};
use crate::channel::DynChannel;
use crate::turn::{self, TurnHost, TurnKit};
use tokio::task::AbortHandle;
use tokio::time::{Instant, MissedTickBehavior};

/// The kind of the refusal of a turn whose channel the executor does not
/// hold, as the core records a channel that is no longer registered.
const CHANNEL_NOT_REGISTERED: &str = roomkit_core::ops::CHANNEL_NOT_REGISTERED;

/// Takes the turn `turn` carries, its request `carried`, as a task of the
/// kit's; `false` when the kit runs no task any more.
pub(super) fn take(
    turns: &Arc<Turns>,
    turn: Arc<MemoryTurn>,
    controls: PipeReceiver<Control>,
    carried: CarriedRequest,
    kit: &Arc<dyn TurnKit>,
) -> bool {
    let executor = Executor {
        id: turns.executor_id.clone(),
        turns: Arc::clone(turns),
        sender: Arc::new(FrameSender::new(Arc::clone(&turn))),
        turn,
        kit: Arc::clone(kit),
    };
    kit.spawn(Box::pin(executor.run(controls, carried)))
}

/// What one turn's task runs with.
struct Executor {
    /// The executor's id.
    id: String,
    turns: Arc<Turns>,
    turn: Arc<MemoryTurn>,
    sender: Arc<FrameSender>,
    kit: Arc<dyn TurnKit>,
}

/// What ended the turn's work early.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Cut {
    /// The owner cancelled the turn, for its reason.
    Cancelled(String),
    /// The owner closed the stream while it read ahead of its reader.
    Closed,
    /// The owner is lost, or its link broke: nothing more is sent.
    Lost(TurnError),
}

impl Executor {
    /// Runs the turn to its end, which the owner is told unless lost.
    async fn run(self, mut controls: PipeReceiver<Control>, carried: CarriedRequest) {
        if self
            .send(&Frame::Accepted {
                executor_id: self.id.clone(),
            })
            .is_err()
        {
            return;
        }
        let panicked = Panicked(Some(Arc::clone(&self.sender)));
        let Some((request, channel)) = self.ready(carried) else {
            drop(self.send(&Frame::End));
            panicked.disarm();
            return;
        };
        let facts = TurnFacts {
            turn: request.turn_id.clone(),
            room: request.room_id.clone(),
            channel: request.channel_id.clone(),
            executor: self.id.clone(),
        };
        let link = Arc::new(HostLink::new(
            facts,
            Arc::clone(&self.sender),
            &self.turns,
            Arc::clone(&self.kit),
        ));
        let host = turn::host(
            Arc::clone(&self.kit),
            request.channel_id.clone(),
            request.binding.channel_type.clone(),
            request.context.clone(),
            Arc::clone(&link),
            response_depth(&request.event),
        );
        let flow = Arc::new(Flow::default());
        let _released = Released(Arc::clone(&link));
        let work: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(work(
            Arc::clone(&self.turn),
            Arc::clone(&self.sender),
            host.clone(),
            Arc::clone(&flow),
            request,
            channel,
        ));
        let beating = Beating::start(&self.kit, &self.sender, &self.turn);
        let cut = self.watch(&mut controls, work, &flow, &link).await;
        drop(beating);
        self.end(cut, &host, &link);
        panicked.disarm();
    }

    /// The turn's request, parsed anew around its events, and its channel;
    /// `None`, its error sent, when either is missing.
    fn ready(&self, carried: CarriedRequest) -> Option<(TurnRequest, Arc<dyn DynChannel>)> {
        let request = match parse(carried) {
            Ok(request) => request,
            Err(why) => {
                let error = one(
                    "frame_invalid",
                    format!("the turn's request could not be read: {why}"),
                );
                drop(self.send(&Frame::Error { error }));
                return None;
            }
        };
        let Some(channel) = self.kit.channel(&request.channel_id) else {
            let error = one(
                CHANNEL_NOT_REGISTERED,
                format!("no channel {} is registered", request.channel_id),
            );
            drop(self.send(&Frame::Error { error }));
            return None;
        };
        Some((request, channel))
    }

    /// Polls the turn's work beside the owner's control messages and the
    /// executor's beat, until the work is over and every verb it asked was
    /// answered, or the turn is cut.
    async fn watch(
        &self,
        controls: &mut PipeReceiver<Control>,
        work: Pin<Box<dyn Future<Output = ()> + Send>>,
        flow: &Flow,
        link: &HostLink,
    ) -> Option<Cut> {
        let timeouts = self.turn.timeouts;
        let mut beat = Beat::new(&timeouts, true);
        let mut work = Some(work);
        let mut cut = None;
        loop {
            if work.is_none() && (cut.is_some() || link.is_settled()) {
                return cut;
            }
            let idle = work.is_none();
            let working = async {
                match work.as_mut() {
                    Some(work) => work.await,
                    None => pending().await,
                }
            };
            tokio::select! {
                biased;
                control = controls.get() => {
                    beat.heard();
                    match control {
                        Ok((_, control)) => {
                            if let Some(cutting) = apply(control, flow, link) {
                                work = None;
                                cut = Some(cutting);
                            }
                        }
                        Err(failure) => return Some(Cut::Lost(lost(&failure))),
                    }
                }
                () = working => {
                    work = None;
                    link.close();
                }
                () = link.settled(), if idle => {}
                // The beat is sent from a task of its own (`Beating`); here
                // the owner's silence is counted.
                silent = beat.tick() => {
                    if silent >= timeouts.silent_intervals() {
                        tracing::warn!(target: "roomkit::turns", turn = %self.turn.turn_id, "a turn's owner fell silent");
                        return Some(Cut::Lost(TurnError::new("owner_lost", "the turn's owner fell silent")));
                    }
                }
            }
        }
    }

    /// Ends the turn as `cut` leaves it: the calls it cut reported, the
    /// verbs it asked and that wait still failed, then its end, unless its
    /// owner is lost. No verb waits for an answer once the turn is over.
    fn end(&self, cut: Option<Cut>, host: &TurnHost, link: &HostLink) {
        link.close();
        match cut {
            None | Some(Cut::Closed) => {
                link.fail_all(&TurnError::new(
                    "cancelled",
                    "the turn ended before the verb's answer",
                ));
            }
            Some(Cut::Cancelled(reason)) => {
                host.report_cut(&self.turn.stream_context(), &reason);
                link.fail_all(&TurnError::new(
                    "cancelled",
                    format!("the turn was cut: {reason}"),
                ));
            }
            Some(Cut::Lost(error)) => {
                self.sender.silence();
                host.report_cut(&self.turn.stream_context(), &error.kind);
                link.fail_all(&error);
                return;
            }
        }
        drop(self.send(&Frame::End));
    }

    fn send(&self, frame: &Frame) -> Result<u64, Uncarried> {
        self.sender.send(frame)
    }
}

/// The executor's heartbeat (RFC §10.6.8), sent every interval from a task
/// of its own beside the turn's, so that a turn whose work blocks its
/// thread (a synchronous call, a CPU-bound job) still beats on a runtime
/// of several threads; stopped as the turn ends, however it ends.
struct Beating(Option<AbortHandle>);

impl Beating {
    fn start(kit: &Arc<dyn TurnKit>, sender: &Arc<FrameSender>, turn: &Arc<MemoryTurn>) -> Self {
        let (sender, turn) = (Arc::clone(sender), Arc::clone(turn));
        let period = turn.timeouts.interval();
        let beating = async move {
            let mut tick = tokio::time::interval_at(Instant::now() + period, period);
            tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if !turn.frames.is_open() {
                    return;
                }
                drop(sender.send(&Frame::Heartbeat));
            }
        };
        Self(kit.spawn_tracked(Box::pin(beating)))
    }
}

impl Drop for Beating {
    fn drop(&mut self) {
        if let Some(beating) = self.0.take() {
            beating.abort();
        }
    }
}

/// The turn's link let go of however its task ends, aborted with the kit's
/// shutdown included: its host's verbs cross outside the turn from then
/// on, and a verb still waiting for its answer fails, so that no task the
/// turn spawned waits for ever.
struct Released(Arc<HostLink>);

impl Drop for Released {
    fn drop(&mut self) {
        self.0.close();
        self.0.fail_all(&TurnError::new(
            "backend_closed",
            "the turn ended before the verb's answer",
        ));
    }
}

/// The turn's task unwinding from a panic of its work (a tool, the
/// channel): the owner is told at once, as a call that panicked fails its
/// effect, rather than learn it from the executor's silence.
struct Panicked(Option<Arc<FrameSender>>);

impl Panicked {
    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for Panicked {
    fn drop(&mut self) {
        let Some(sender) = self.0.take() else {
            return;
        };
        if std::thread::panicking() {
            let error = one("Panic", "the turn's work panicked".to_owned());
            drop(sender.send(&Frame::Error { error }));
            drop(sender.send(&Frame::End));
        }
    }
}

/// Applies one control message of the owner's; how it cuts the work, when
/// it does.
fn apply(control: Control, flow: &Flow, link: &HostLink) -> Option<Cut> {
    match control {
        Control::Pull { count, read } => {
            if count > 0 {
                lock(&flow.gate).grant(count, read);
                flow.wake.notify_one();
            }
            None
        }
        Control::Close => {
            let cut_read = lock(&flow.gate).close();
            flow.wake.notify_one();
            // A read ahead of the reader is cut at once; one it asked for (a
            // call running) finishes first (RFC §10.6.4, §12.2 step 13s).
            cut_read.then_some(Cut::Closed)
        }
        Control::Cancel { reason } => Some(Cut::Cancelled(reason)),
        Control::Heartbeat => None,
        Control::HostResult {
            call_id,
            result,
            error,
        } => {
            link.resolve(&call_id, result, error.as_ref());
            None
        }
    }
}

/// What a broken link from the owner is to the executor.
fn lost(failure: &LinkFailure) -> TurnError {
    match failure {
        LinkFailure::Lost { .. } | LinkFailure::Invalid(_) => TurnError::link(failure),
        LinkFailure::Closed => TurnError::new("owner_lost", "the turn's owner let go of it"),
    }
}

/// The request `carried` brings, its rest parsed anew around its events.
fn parse(carried: CarriedRequest) -> Result<TurnRequest, String> {
    if carried.rest.len() > MAX_MESSAGE_BYTES {
        return Err(format!(
            "{} bytes, past the bound of {MAX_MESSAGE_BYTES}",
            carried.rest.len()
        ));
    }
    let rest: RequestRest =
        serde_json::from_str(&carried.rest).map_err(|error| error.to_string())?;
    let ContextRest {
        room,
        bindings,
        participants,
        author_register,
    } = rest.context;
    let mut context = roomkit_core::model::RoomContext::new(room);
    context.bindings = bindings;
    context.participants = participants;
    context.author_register = author_register;
    context.recent_events = carried.history;
    Ok(TurnRequest {
        turn_id: rest.turn_id,
        room_id: rest.room_id,
        channel_id: rest.channel_id,
        event: carried.event,
        binding: rest.binding,
        context,
        strategy_turn: rest.strategy_turn,
        notes: rest.notes,
        in_place: rest.in_place,
        owner_tools: rest.owner_tools,
        room_hooks: rest.room_hooks,
        previous_turn: rest.previous_turn,
        trace: rest.trace,
    })
}

#[cfg(test)]
mod tests {
    use roomkit_core::model::{
        ChannelId, ChannelType, EventDirection, EventId, EventSource, EventType, RoomEvent, RoomId,
        TextContent, Timestamp,
    };

    use super::*;

    /// RFC §10.6.3: a request is checked against its bound before it is
    /// parsed.
    #[test]
    fn a_request_past_its_bound_is_refused_before_it_is_parsed() {
        let at: Timestamp = "2026-10-26T12:00:00Z".parse().unwrap();
        let source = EventSource::new(
            ChannelId::new("sms").unwrap(),
            ChannelType::SMS,
            EventDirection::Inbound,
        );
        let event = RoomEvent::new(
            EventId::new("evt_1").unwrap(),
            RoomId::new("room_1").unwrap(),
            EventType::Message,
            source,
            TextContent::new("Hi").into(),
            at,
        );
        let carried = CarriedRequest {
            rest: " ".repeat(MAX_MESSAGE_BYTES + 1),
            event,
            history: Vec::new(),
        };
        let refused = parse(carried).unwrap_err();
        assert!(refused.contains("past the bound"), "{refused}");
    }
}
