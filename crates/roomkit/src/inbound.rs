//! What an inbound call comes to ([`InboundResult`], RFC §10.1), and, for a
//! call deferred at its commit, its completion handle ([`DeliveryHandle`],
//! RFC §10.1 step 18 detached completion).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use roomkit_core::model::{
    ChannelId, DeliveryResult, ErrorRecord, Metadata, ResponseMetadata, RoomEvent,
};
use roomkit_core::ops::{self, KIT_CLOSED};
use serde_json::Value;
use tokio::sync::{Mutex, OnceCell};
use tokio::task::{AbortHandle, JoinHandle};

use crate::context::{Completion, ExecutionContext};
use crate::driver::{self, InboundOutput, Record};
use crate::error::RoomKitError;
use crate::tasks::Tasks;

/// What became of an inbound message (RFC §10.1 `InboundResult`), as the
/// Python implementation's `InboundResult` reports it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InboundResult {
    /// The committed event: delivered, or the record stored `BLOCKED` when a
    /// hook or the source's permissions refused it (gap G-026). `None` when
    /// nothing was committed, as when the room refused the message or step
    /// 8a refused an edit or a delete.
    ///
    /// An instruction, which is never committed, is the exception (RFC
    /// §10.1.1 rule 5): once its pass decided it, the result carries it
    /// uncommitted, `DELIVERED` or `BLOCKED` with its `blocked_by`, at the
    /// index its hooks were shown, which the next event takes (gap G-065).
    pub event: Option<RoomEvent>,
    /// Whether the call is a replay of an idempotency key its channel holds
    /// in the room (RFC §13.4), not a new turn: `event` is the event the key
    /// committed.
    pub duplicate: bool,
    /// The channels the event addressed (`addressed_to`) that its committed
    /// delivery plan asks no turn of, in the order the event names them
    /// (RFC §10.1 step 18): unbound, unregistered, or no intelligence
    /// channel. `None` when no plan was resolved: a replay, a refused or a
    /// blocked event.
    pub unavailable_targets: Option<Vec<ChannelId>>,
    /// Whether the message was blocked: refused by the room, step 8a, a hook
    /// or its source's permissions, an instruction refused before anything
    /// was read, or a call past its `process_timeout` (RFC §13.6).
    pub blocked: bool,
    /// Why it was blocked, as the core's result names it: a hook's reason,
    /// `room_closed`, `source_muted`, `process_timeout`, ...
    pub reason: Option<String>,
    /// The first error met once the message was decided: a turn's, a
    /// later commit's, or the room lock's release (gap G-025). What was
    /// decided stands (RFC §13.6).
    pub error: Option<ErrorRecord>,
    /// Why the call's delivery was cancelled, the event it committed
    /// standing: `kit_closed` for a call the kit's close cut, its delivery
    /// dropped or cut running, or under way as the close started (RFC
    /// §12.10.4), as the Python implementation's `cancellation_reason`
    /// reads; `None` for a delivery that ran.
    pub cancellation_reason: Option<String>,
    /// The merged response metadata of the turns the message asked for: a
    /// deferred call's [`DeliveryHandle::wait`] fills the record the call
    /// returned at its commit, as Python's `result.response_metadata`.
    pub response_metadata: ResponseMetadata,
    /// The responses this call's cascade committed and delivered, in their
    /// commit order: the answers to its event and to their answers, a
    /// streamed answer's rows among them, never the event itself, a response
    /// stored `BLOCKED`, nor another call's. A deferred call's result
    /// returned at its commit holds none: its [`DeliveryHandle::wait`]
    /// reports them.
    pub response_events: Vec<RoomEvent>,
    /// The deliveries of the event to its transport channels. A deferred
    /// call's result returned at its commit holds none, unless its delivery
    /// ran meanwhile; its [`DeliveryHandle::wait`] reports them.
    pub delivery_results: BTreeMap<ChannelId, DeliveryResult>,
    /// For a call made with `defer_delivery`, its completion handle: what
    /// remains of it once its commit returned, its delivery set, the passes
    /// it spawns and their turns, in the room's lane (RFC §10.1 step 18).
    /// `None` on the waiting path, and for a deferred call refused before
    /// the locked region, which has no deferred tail.
    pub delivery: Option<DeliveryHandle>,
}

impl InboundResult {
    /// `result`, the core's, with `delivery` for a deferred call.
    pub(crate) fn of(result: ops::InboundResult, delivery: Option<DeliveryHandle>) -> Self {
        let ops::InboundResult {
            event,
            duplicate,
            unavailable_targets,
            blocked,
            reason,
            error,
            response_metadata,
            response_events,
            delivery_results,
            cancellation_reason,
            refused_before_the_lock: _,
        } = result;
        let response_metadata = match &delivery {
            Some(handle) => handle.shared.reported(response_metadata),
            None => response_metadata.into(),
        };
        Self {
            event,
            duplicate,
            unavailable_targets,
            blocked,
            reason,
            error,
            cancellation_reason,
            response_metadata,
            response_events,
            delivery_results,
            delivery,
        }
    }
}

impl From<ops::InboundResult> for InboundResult {
    fn from(result: ops::InboundResult) -> Self {
        Self::of(result, None)
    }
}

/// What [`DeliveryHandle::cancel`] takes: Python's
/// `cancel(*, reason='caller_cancelled', timeout=5.0)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelOptions {
    /// Python `reason='caller_cancelled'`: why, the result's
    /// `cancellation_reason`; a handle cancelled already keeps its first.
    pub reason: String,
    /// Python `timeout=5.0`: how long the cancel waits for the turns it
    /// cuts to be over. It bounds this wait only: past it the cleanup goes
    /// on, and [`DeliveryHandle::wait`] reads the whole record.
    pub timeout: Duration,
}

impl Default for CancelOptions {
    fn default() -> Self {
        Self {
            reason: driver::CALLER_CANCELLED.to_owned(),
            timeout: Duration::from_secs(5),
        }
    }
}

/// A deferred call's completion handle (RFC §10.1 step 18), the Python
/// implementation's `DeliveryHandle`: what remains of the call once its
/// commit returned, the event's delivery set, the reentry passes it spawns,
/// an agent's answer among them, and their streamed responses, in the
/// room's lane. Cloning it shares it; dropping every clone lets the
/// delivery run on.
///
/// A call adopted by the room's admission work that made it, an off-lock
/// hook's body or a parse (RFC §9.5.1 Reentrance), has its delivery set
/// run by that work, ahead of the event: a wait from that work, or from
/// anywhere once it ran, returns with its delivery results.
#[derive(Clone)]
pub struct DeliveryHandle {
    shared: Arc<Shared>,
}

/// The key of a caller's record under which each turn's end is (RFC §6.4).
const TURNS: &str = "turns";

/// What every clone of a handle shares.
struct Shared {
    /// The result at the commit, which the call returned.
    decided: ops::InboundResult,
    /// The record the call returned at its commit, each wait filling it.
    record: ResponseMetadata,
    following: Following,
    /// What the delivery came to, once a wait saw its end.
    ended: OnceCell<Ended>,
    tasks: Arc<Tasks>,
}

/// What a delivery came to, as each wait on it reads it.
enum Ended {
    /// The call's result, its delivery over.
    Over(Box<ops::InboundResult>),
    /// The run failed past the commit, a defect of this crate: the first
    /// wait takes the error, the next ones read the result at the commit.
    Failed(std::sync::Mutex<Option<RoomKitError>>),
}

/// What a handle waits for.
enum Following {
    /// The call's run, going on past its decision, with its record.
    Run {
        run: Mutex<Option<JoinHandle<InboundOutput>>>,
        finished: AbortHandle,
        record: Arc<Record>,
    },
    /// The end of the delivery set of a call the room's work adopted, as
    /// the adopting run reports it.
    Adopted(Completion),
    /// Nothing: the call ended at its decision.
    Nothing,
}

impl DeliveryHandle {
    /// The handle of the call that returned `decided` at its commit, its
    /// run going on as `run`.
    pub(crate) fn of_run(
        decided: ops::InboundResult,
        run: JoinHandle<InboundOutput>,
        record: Arc<Record>,
        tasks: Arc<Tasks>,
    ) -> Self {
        let finished = run.abort_handle();
        let following = Following::Run {
            run: Mutex::new(Some(run)),
            finished,
            record,
        };
        Self::new(decided, following, tasks)
    }

    /// The handle of the adopted call that returned `decided` at its
    /// commit, its delivery's end told by `completion`.
    pub(crate) fn adopted(
        decided: ops::InboundResult,
        completion: Completion,
        tasks: Arc<Tasks>,
    ) -> Self {
        Self::new(decided, Following::Adopted(completion), tasks)
    }

    /// The handle of a call that ended at its decision, `decided`.
    pub(crate) fn over(decided: ops::InboundResult, tasks: Arc<Tasks>) -> Self {
        Self::new(decided, Following::Nothing, tasks)
    }

    fn new(decided: ops::InboundResult, following: Following, tasks: Arc<Tasks>) -> Self {
        Self {
            shared: Arc::new(Shared {
                record: ResponseMetadata::new(),
                decided,
                following,
                ended: OnceCell::new(),
                tasks,
            }),
        }
    }

    /// Whether the delivery will make no further progress: it ran to its
    /// end, or the kit's close ended it.
    pub fn done(&self) -> bool {
        let shared = &self.shared;
        shared.ended.initialized()
            || match &shared.following {
                Following::Run { finished, .. } => finished.is_finished(),
                Following::Adopted(completion) => {
                    let delivered = &completion.delivered;
                    delivered.borrow().is_some() || delivered.has_changed().is_err()
                }
                Following::Nothing => true,
            }
    }

    /// Stops this call's turns and waits for them to be over (RFC §10.1
    /// "Explicit delivery cancellation"), then reports the call as
    /// [`wait`](Self::wait) does, `cancellation_reason` the reason given: a
    /// turn not started never starts, a running one is cut, and nothing
    /// answers the message after. The message itself stays stored and is
    /// still delivered to the room's other channels; a delivery still
    /// waiting in the room's lane follows in lane order. Other turns are not
    /// touched. A second cancel keeps the first reason; a handle done
    /// returns its result unchanged. Code without the handle cancels the
    /// same way by the message's `delivery_key`
    /// ([`cancel_delivery`](crate::RoomKit::cancel_delivery)).
    ///
    /// A streaming target it finds reading is handed the stream's end, and
    /// what it does past it is part of the cleanup (Python's `finally`); one
    /// past the stream already is dropped where it waits.
    ///
    /// Dropped before it returns, the cancel stands: the cut turns end
    /// without this caller, and [`wait`](Self::wait) joins them.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Timeout`] when the cut turns are not over within
    /// `timeout`: their cleanup still runs, and [`wait`](Self::wait) joins
    /// it; [`RoomKitError::Runtime`] when called, the delivery not done,
    /// from the room's own work where its cleanup would wait for this call,
    /// a turn or a delivery in the room, or a hook under its lock: nothing is
    /// cancelled; what [`wait`](Self::wait) fails with.
    pub async fn cancel(&self, options: CancelOptions) -> Result<InboundResult, RoomKitError> {
        let CancelOptions { reason, timeout } = options;
        if self.done() {
            return self.wait().await;
        }
        if self.shared.waits_for_itself() {
            return Err(RoomKitError::Runtime(
                "Cannot drain delivery from its lane or room lock".to_owned(),
            ));
        }
        self.shared.request_cancel(&reason);
        tokio::time::timeout(timeout, self.wait())
            .await
            .map_err(|_elapsed| RoomKitError::Timeout)?
    }

    /// What [`cancel`](Self::cancel) asks for `reason`, without waiting for
    /// the turns it cuts: a caller dropped while it waited for the delivery
    /// asks it so, as Python's `deliver_to_channel` cancels the handle on its
    /// caller's cancellation. Nothing is asked of a delivery done, nor from
    /// the room's own work, where `cancel` refuses.
    pub(crate) fn cancel_unwaited(&self, reason: &str) {
        if self.done() || self.shared.waits_for_itself() {
            return;
        }
        self.shared.request_cancel(reason);
    }

    /// Waits until the delivery is over, then reports the call as one that
    /// waited for it would have: its `delivery_results`, `error`,
    /// `response_metadata`, `response_events` and `cancellation_reason`
    /// filled, its `unavailable_targets` refreshed, the handle on the
    /// result. It may be called again, and from any clone: each reads
    /// the same end.
    ///
    /// Called, the delivery not done, from the room's own work where a call
    /// that waits returns at its decision, a delivery or a turn in the room
    /// or a hook under its lock, it returns the result at once, unwaited:
    /// the delivery could only wait for this wait (RFC §10.1 step 18). It
    /// follows in the room's lane. That work is read from the caller's
    /// execution context: a task the room's work spawned and awaits must be
    /// spawned with [`keep_context`](crate::keep_context), or this wait
    /// blocks behind the delivery that awaits the task.
    ///
    /// # Errors
    ///
    /// Returns what a call that waited would have returned past its commit:
    /// a defect of this crate, as [`RoomKitError::Internal`], to the first
    /// wait that reads it; another wait then reads the result the call
    /// returned at its commit.
    pub async fn wait(&self) -> Result<InboundResult, RoomKitError> {
        let shared = &self.shared;
        if !self.done() && shared.waits_for_itself() {
            return Ok(self.with_handle(shared.decided.clone()));
        }
        let ended = shared.ended.get_or_init(|| shared.end()).await;
        let result = match ended {
            Ended::Over(result) => (**result).clone(),
            Ended::Failed(error) => {
                let taken = error.lock().unwrap_or_else(PoisonError::into_inner).take();
                if let Some(error) = taken {
                    return Err(error);
                }
                shared.decided.clone()
            }
        };
        Ok(self.with_handle(result))
    }

    fn with_handle(&self, result: ops::InboundResult) -> InboundResult {
        InboundResult::of(result, Some(self.clone()))
    }
}

impl Shared {
    /// The call's record, `reported` merged into it, each channel's end beside
    /// the others': Python's `merge_caller_record`.
    fn reported(&self, mut reported: Metadata) -> ResponseMetadata {
        let mut turns = match self.record.get(TURNS) {
            Some(Value::Object(turns)) => turns,
            _ => Metadata::new(),
        };
        if let Some(Value::Object(ended)) = reported.get(TURNS) {
            turns.extend(ended.clone());
        }
        reported.shift_remove(TURNS);
        self.record.update(reported);
        if !turns.is_empty() {
            self.record.insert(TURNS, Value::Object(turns));
        }
        self.record.clone()
    }

    /// Asks the run that delivers the call, or the run that adopted it, to
    /// cut its turns, for `reason`.
    fn request_cancel(&self, reason: &str) {
        match &self.following {
            Following::Run { record, .. } => {
                record.request_cancel(reason);
            }
            Following::Adopted(completion) => {
                completion.cancel.request(reason);
            }
            Following::Nothing => {}
        }
    }

    /// Whether a wait in the current task's context would wait for itself:
    /// the context is work of the event's room that the delivery waits for.
    fn waits_for_itself(&self) -> bool {
        self.decided
            .event
            .as_ref()
            .is_some_and(|event| ExecutionContext::current().waits_for_itself(&event.room_id))
    }

    /// What the delivery came to, once over.
    async fn end(&self) -> Ended {
        match &self.following {
            Following::Run { run, record, .. } => {
                let mut run = run.lock().await;
                let Some(running) = run.as_mut() else {
                    return Ended::Over(Box::new(self.decided.clone()));
                };
                let joined = tokio::select! {
                    biased;
                    joined = running => joined,
                    // Its delivery cancelled, the call is over once its
                    // turns are: the delivery goes on, detached (RFC §10.1
                    // "Explicit delivery cancellation").
                    result = driver::cancelled_end(record) => {
                        return Ended::Over(Box::new(result));
                    }
                };
                *run = None;
                match driver::ended(joined, record, &self.tasks) {
                    Ok(Ok(result)) => Ended::Over(Box::new(result)),
                    Ok(Err(error)) => Ended::Failed(std::sync::Mutex::new(Some(error.into()))),
                    Err(error) => Ended::Failed(std::sync::Mutex::new(Some(error))),
                }
            }
            Following::Adopted(completion) => {
                let mut told = completion.delivered.clone();
                let delivered = told
                    .wait_for(Option::is_some)
                    .await
                    .ok()
                    .and_then(|delivered| delivered.clone());
                let told = delivered.is_some();
                let mut result = self.backfilled(delivered);
                // The adopting run over without telling it, its close's
                // abort: the answers its cascade stored, as Python's caller
                // reads its cascade's.
                if !told {
                    result.response_events = completion.answers.stored();
                }
                Ended::Over(Box::new(result))
            }
            Following::Nothing => Ended::Over(Box::new(self.decided.clone())),
        }
    }

    /// The decided result, with what an adopted call's delivery set
    /// reported and the answers its cascade delivered, `delivered`; with
    /// none, the adopting run over without it, the delivery reads as one
    /// the kit's close dropped, when it closes.
    fn backfilled(&self, delivered: Option<ops::InboundResult>) -> ops::InboundResult {
        let mut result = self.decided.clone();
        let Some(delivered) = delivered else {
            if self.tasks.is_closing() {
                result.cancellation_reason = Some(KIT_CLOSED.to_owned());
            }
            return result;
        };
        result.delivery_results = delivered.delivery_results;
        result.response_metadata = delivered.response_metadata;
        result.response_events = delivered.response_events;
        if delivered.unavailable_targets.is_some() {
            result.unavailable_targets = delivered.unavailable_targets;
        }
        result.error = result.error.or(delivered.error);
        result.cancellation_reason = delivered.cancellation_reason;
        result
    }
}

impl PartialEq for DeliveryHandle {
    /// The same handle: clones of one.
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
}

impl fmt::Debug for DeliveryHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryHandle")
            .field("done", &self.done())
            .finish_non_exhaustive()
    }
}
