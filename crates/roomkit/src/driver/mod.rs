//! The driver: runs any operation of the core to its end, executing the
//! effects it asks for and performing its notices.
//!
//! An effect is carried out as soon as the operation asks for it, and
//! completed as soon as it is answered: the driver polls the operation
//! between completions. A release, a place in a room's lane and a ticket
//! are carried out at once; a lock, a place's turn or a ticket's turn is a
//! wait the driver polls itself (`holds`). A place left and a lock let go
//! that end the turn of an index the run committed are answered once the
//! room's delivery cursor caught up (`indexes`, `crate::cursors`, gap
//! G-091). A call on the store, a channel,
//! a hook or the router is a task (`calls`), run within its execution
//! context, so that code it runs may take the lock the operation holds on
//! its behalf again, or call into the room under its lock or in its
//! admission order: such a call is handed to the operation, which adopts
//! it, and its caller is answered when the operation settles it (RFC §13.5,
//! §9.5.1 Reentrance, §10.1 step 18). An inbound call's operation runs on a
//! task of its own, within its caller's execution context, answers a
//! caller that cannot wait for its delivery at its decision, and goes on
//! detached when its caller stops waiting once the call is decided
//! (`inbound`).
//!
//! The driver never waits for what its own context holds: a lock its
//! context holds a lease on or stands in the admission order of, asked by
//! an inbound call, or a ticket's turn in a room's admission order its
//! context stands in, fails the call with [`RoomKitError::ReentrantCall`], since
//! that run waits for this one. An effect
//! that carries a deadline (RFC §13.6) is given up on past it: a lock or a
//! turn waited for is not taken, a call in flight is dropped, and the
//! operation is answered `DeadlinePassed`.

mod calls;
mod holds;
mod in_flight;
mod inbound;
mod indexes;

use std::collections::HashMap;
use std::future::poll_fn;
use std::sync::Arc;
use std::task::Poll;

use roomkit_core::model::{ErrorRecord, RoomEvent, RoomId, Timestamp};
use roomkit_core::ops::{
    AnswerOf, Delivered, Effect, EffectId, EntryId, Environment, HoldOutcome, HoldRequest,
    InboundResult, Next, Notice, Operation, Outcome, ProtocolError, Request, Settled, StoreRequest,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{self, AbortHandle, JoinError, JoinHandle, JoinSet};
use tokio::time::Instant;

pub(crate) use self::calls::cut_by_close;
use self::calls::{WentOn, Written};
use self::holds::{Held, Hold, Leases, Rooms, Waited, Waiting};
use self::in_flight::InFlight;
use self::inbound::Caller;
pub(crate) use self::inbound::{InboundOutput, Ran, Record, StoredAnswers, ended, run_inbound};
use self::indexes::Indexes;
use crate::context::{Adoption, ExecutionContext, Frame, Reply, RunShared, Work};
use crate::error::RoomKitError;
use crate::kit::Inner;
use crate::store::ConversationStore;
use crate::tasks::{CUT_UNWIND, Kind, Marks, Phase};
use crate::turns::Canceller;

/// The kit's clock and the runtime's, read together at a run's start: a
/// deadline the core set on the kit's clock is waited for on the runtime's,
/// whether the kit's clock is the system's or one a test moves by hand.
#[derive(Clone, Copy, Debug)]
struct Clocks {
    kit: Timestamp,
    runtime: Instant,
}

impl Clocks {
    /// The run's clock, `environment`'s, and the runtime's, read together.
    fn read(environment: &dyn Environment) -> Self {
        Self {
            kit: environment.now(),
            runtime: Instant::now(),
        }
    }

    /// The runtime instant of `deadline`, at once when it passed already;
    /// `None` past the runtime clock's range, where nothing bounds the
    /// request.
    fn instant_of(self, deadline: Timestamp) -> Option<Instant> {
        self.runtime
            .checked_add(deadline.duration_since(self.kit).unwrap_or_default())
    }
}

/// What ends a run before its operation is over.
#[derive(Debug)]
pub(crate) enum Stop {
    /// The kit's call failed.
    Failed(RoomKitError),
    /// The kit's close cut the run (RFC §12.10.4): the event its commit
    /// stored, read back by its id, when that commit had started.
    Cut(Option<Box<Committed>>),
}

impl From<RoomKitError> for Stop {
    fn from(error: RoomKitError) -> Self {
        Self::Failed(error)
    }
}

impl From<ProtocolError> for Stop {
    fn from(error: ProtocolError) -> Self {
        Self::Failed(error.into())
    }
}

impl Stop {
    /// The error of a call that reads a run so stopped as a failure: the
    /// kit's call's, or [`RoomKitError::KitClosed`] for one the close cut.
    pub(crate) fn into_error(self) -> RoomKitError {
        match self {
            Self::Failed(error) => error,
            Self::Cut(_) => RoomKitError::KitClosed,
        }
    }
}

/// The commit of a run the kit's close cut, read back: the event as the
/// store holds it, and the call's result at that commit, for an inbound
/// call's own event.
#[derive(Debug)]
pub(crate) struct Committed {
    pub(crate) event: RoomEvent,
    pub(crate) decides: Option<Box<InboundResult>>,
}

/// Runs `operation`, made by the kit's call `call`, until it is ready; a
/// lock its execution context holds a live lease on is granted to it (RFC
/// §13.5).
///
/// # Errors
///
/// Returns [`RoomKitError::KitClosed`] once the kit's close started, and
/// for a run the close cut; [`RoomKitError::ReentrantCall`] when the
/// operation waited for a ticket's turn in an admission order its own
/// execution context stands in, which was refused; a protocol error or a
/// stall is a defect of this crate.
pub(crate) async fn run<S: ConversationStore, O: Operation + Send + 'static>(
    inner: &Arc<Inner<S>>,
    call: &'static str,
    operation: O,
) -> Result<O::Output, RoomKitError>
where
    O::Output: Send + 'static,
{
    run_on(inner, inner.environment(), call, operation).await
}

/// Runs `operation` as [`run`] does, on `environment`: the environment its
/// call read when it started, to stamp what it hands the operation, a new
/// room's instants or a binding's, with the clock the run reads.
///
/// # Errors
///
/// As [`run`].
pub(crate) async fn run_on<S: ConversationStore, O: Operation + Send + 'static>(
    inner: &Arc<Inner<S>>,
    environment: Arc<dyn Environment + Send + Sync>,
    call: &'static str,
    operation: O,
) -> Result<O::Output, RoomKitError>
where
    O::Output: Send + 'static,
{
    inner.open()?;
    Box::pin(run_in_its_task(inner, environment, call, operation, false))
        .await
        .map_err(Stop::into_error)
}

/// Runs `operation` as [`run`] does, a run the kit's close cut reading its
/// commit back: what `commit_event` runs.
///
/// # Errors
///
/// As [`run`], the close's cut as [`Stop::Cut`].
pub(crate) async fn run_reading_back<S: ConversationStore, O: Operation + Send + 'static>(
    inner: &Arc<Inner<S>>,
    call: &'static str,
    operation: O,
) -> Result<O::Output, Stop>
where
    O::Output: Send + 'static,
{
    inner.open()?;
    Box::pin(run_in_its_task(
        inner,
        inner.environment(),
        call,
        operation,
        false,
    ))
    .await
}

/// Runs `operation`, a step of the kit's own work, a turn's record written
/// on its last message: taken once the close started, which cuts it only
/// once its grace ran out.
///
/// # Errors
///
/// As [`run`].
pub(crate) async fn run_own<S: ConversationStore, O: Operation + Send + 'static>(
    inner: &Arc<Inner<S>>,
    call: &'static str,
    operation: O,
) -> Result<O::Output, RoomKitError>
where
    O::Output: Send + 'static,
{
    if inner.tasks.is_sealed() {
        return Err(RoomKitError::KitClosed);
    }
    Box::pin(run_in_its_task(
        inner,
        inner.environment(),
        call,
        operation,
        true,
    ))
    .await
    .map_err(Stop::into_error)
}

/// Runs `operation` on a task of its own, within its caller's execution
/// context, as an inbound call's run is, tracked as a call the kit's close
/// gives its grace, then cuts: at once while it waits for a room's lock,
/// unless it is the kit's `own` work; aborted once the unwind the cut gives
/// it ran out, a commit it had in flight then reported as a read-back that
/// failed (RFC §12.10.4). A caller that keeps the call's future but stops
/// polling it holds no work the close waits for; one that drops it cancels
/// the call, its task aborted, as when the call ran in the caller's own
/// task. Boxed by its callers: the operation and the run's state would make
/// every caller's future large.
async fn run_in_its_task<S: ConversationStore, O: Operation + Send + 'static>(
    inner: &Arc<Inner<S>>,
    environment: Arc<dyn Environment + Send + Sync>,
    call: &'static str,
    operation: O,
    own: bool,
) -> Result<O::Output, Stop>
where
    O::Output: Send + 'static,
{
    let in_flight = Arc::new(InFlight::default());
    let (kit, noting) = (Arc::clone(inner), Arc::clone(&in_flight));
    let run = async move {
        let how = Driven::Call {
            own,
            in_flight: noting,
        };
        drive(&kit, environment, call, operation, Leases::Granted, how).await
    };
    let context = ExecutionContext::current();
    let Some(task) = inner.tasks.spawn(Kind::Call, context.scope(run)) else {
        return Err(Stop::Failed(RoomKitError::KitClosed));
    };
    let mut task = Owned(task);
    match (&mut task.0).await {
        Ok(output) => output,
        // A panic of the driver's own loop is a defect of this crate: the
        // caller receives it as it would from a run it drove itself.
        Err(failure) if failure.is_panic() => std::panic::resume_unwind(failure.into_panic()),
        Err(aborted) if inner.tasks.ended_by_close(aborted.id()) => {
            in_flight.aborted();
            Err(Stop::Cut(None))
        }
        Err(_cancelled) => Err(Stop::Failed(RoomKitError::Cancelled)),
    }
}

/// A call's task, aborted when its caller's future is dropped.
struct Owned<T>(JoinHandle<T>);

impl<T> Drop for Owned<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// What a run is: an inbound call's, offering its decision to its caller,
/// or a direct call's, noting the commit it has in flight.
enum Driven<'run> {
    Inbound(&'run mut Caller),
    Call { own: bool, in_flight: Arc<InFlight> },
}

/// Runs `operation`, made by the kit's call `call`, until it is ready,
/// treating a lock its execution context holds a live lease on as `leases`
/// says, and offering its decision to its caller, when the run is an
/// inbound call's. Such a run is never dropped for its caller: a caller
/// gone before the call's decision expires the operation
/// (`Operation::expire`), which goes on to its end, delivering what it
/// committed (gap G-076). The kit's close cuts it (RFC §12.10.4).
async fn drive<S: ConversationStore, O: Operation>(
    inner: &Arc<Inner<S>>,
    environment: Arc<dyn Environment + Send + Sync>,
    call: &'static str,
    operation: O,
    leases: Leases,
    how: Driven<'_>,
) -> Result<O::Output, Stop> {
    let (caller, own, in_flight) = match how {
        Driven::Inbound(caller) => (Some(caller), false, None),
        Driven::Call { own, in_flight } => (None, own, Some(in_flight)),
    };
    let (shared, adoptions) = RunShared::new();
    let driver = Driver {
        calls: JoinSet::new(),
        waits: Vec::new(),
        inner,
        call,
        context: ExecutionContext::current(),
        marks: Marks::current(),
        held: Held::new(Arc::clone(&shared), leases),
        indexes: Indexes::default(),
        shared,
        adoptions,
        effect_of_task: HashMap::new(),
        call_of_effect: HashMap::new(),
        callers: HashMap::new(),
        deliveries: HashMap::new(),
        entry_answers: HashMap::new(),
        clocks: Clocks::read(&*environment),
        environment,
        refused: None,
        caller,
        cut: Vec::new(),
        writing: HashMap::new(),
        written: Vec::new(),
        phases: inner.tasks.phases(),
        phase: Phase::Open,
        own,
        committing: None,
        stop: None,
        cut_now: false,
        in_flight,
    };
    // Boxed: the waits and calls a run polls would make every caller's
    // future large.
    Box::pin(driver.drive(operation)).await
}

/// One run of an operation: what it holds, waits for and calls, and the
/// callers of the entries its operation adopted.
///
/// Dropped, whatever ends it, the run declines the calls handed to it it
/// did not adopt, so that they run apart, then aborts its calls before it
/// lets go of what it holds: the fields drop in that order.
struct Driver<'kit, S> {
    calls: JoinSet<Outcome>,
    /// The waits the run polls, each by its effect, with what it waits for.
    waits: Vec<(EffectId, Wait, Waiting<'kit>)>,
    inner: &'kit Arc<Inner<S>>,
    /// The kit's call the run serves, which an error names.
    call: &'static str,
    /// The execution context the run is made in.
    context: ExecutionContext,
    /// The units of the kit's work the run runs within, which its calls run
    /// within too (RFC §12.10.4).
    marks: Marks,
    held: Held<'kit>,
    /// The indexes the run commits, until their turn is over.
    indexes: Indexes,
    shared: Arc<RunShared>,
    /// The calls into the room its work hands the run.
    adoptions: mpsc::UnboundedReceiver<Adoption>,
    /// The effect each call answers, by its task.
    effect_of_task: HashMap<task::Id, EffectId>,
    /// The call of each effect, until it finishes, to abort it when the
    /// operation gives the effect up.
    call_of_effect: HashMap<EffectId, AbortHandle>,
    /// The callers of the entries the operation adopted, until settled.
    callers: HashMap<EntryId, oneshot::Sender<Reply>>,
    /// Where the delivery's end of each entry the operation adopted goes,
    /// until its set ran.
    deliveries: HashMap<EntryId, watch::Sender<Option<InboundResult>>>,
    /// The answers of each adopted entry's cascade, which its handle reads
    /// when the run ends without telling its delivery's end.
    entry_answers: HashMap<EntryId, Arc<StoredAnswers>>,
    clocks: Clocks,
    /// The environment the run started with, which every poll of its
    /// operation and every call it makes read.
    environment: Arc<dyn Environment + Send + Sync>,
    /// The room whose lock or ticket's turn the run was refused, its context
    /// holding it.
    refused: Option<RoomId>,
    /// The caller of an inbound call's run, which the operation's decision
    /// goes to.
    caller: Option<&'kit mut Caller>,
    /// The turns the run cut as it gave their calls up, whose end it joins
    /// before it ends (RFC §10.1 "Explicit delivery cancellation").
    cut: Vec<Canceller>,
    /// An inbound run's hold on each write it asked for, by its effect,
    /// until the operation takes the write's outcome.
    writing: HashMap<EffectId, WentOn>,
    /// The holds on the writes whose outcome the operation took, until what
    /// that outcome led to is started.
    written: Vec<WentOn>,
    /// The kit's close's phase, told as it changes (RFC §12.10.4).
    phases: watch::Receiver<Phase>,
    /// The phase the run last acted on.
    phase: Phase,
    /// Whether the run is a step of the kit's own work, which the close
    /// cuts only once its grace ran out.
    own: bool,
    /// The commit that decides the call, while it runs or once it ran: the
    /// write's outcome, its event and the call's result at it; the commit
    /// of a call run in its caller's task.
    committing: Option<(Written, RoomEvent, Option<Box<InboundResult>>)>,
    /// Why the run stops at once, the kit's close cutting it as a wait
    /// starts.
    stop: Option<Stop>,
    /// Whether the kit's close cut the run past its decision: its calls in
    /// flight and from now on fail `kit_closed`, so that it reports what it
    /// delivered (RFC §12.10.4).
    cut_now: bool,
    /// Where a direct call's run notes the commit it has in flight, which a
    /// run the close aborted warns of; an inbound run's record notes its.
    in_flight: Option<Arc<InFlight>>,
}

/// What a wait of the run waits for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Wait {
    /// A room's lock or admission ticket: what a call waits for before its
    /// commit, which the kit's close cuts at once.
    Admission,
    /// A place's turn in a room's delivery lane: a delivery queued, which
    /// the kit's close drops at once.
    Lane,
    /// The turn of a place taken first in its room's lane, the lane idle:
    /// a delivery that runs from its placement, which the close does not
    /// drop (RFC §12.10.4).
    Running,
    /// Anything else, a room's delivery cursor.
    Other,
}

impl<S> Drop for Driver<'_, S> {
    fn drop(&mut self) {
        self.adoptions.close();
        while let Ok(adoption) = self.adoptions.try_recv() {
            let declined = Reply::Declined(Box::new(adoption.entry));
            drop(adoption.reply.send(declined));
        }
    }
}

/// What the run waits for next.
enum Event<'kit> {
    Waited(EffectId, Waited<'kit>),
    Joined(Result<(task::Id, Outcome), JoinError>),
    Adoption(Adoption),
    /// The caller of an inbound call stopped waiting.
    CallerGone,
    /// The kit's close moved on to this phase.
    Phase(Phase),
}

impl<'kit, S: ConversationStore> Driver<'kit, S> {
    async fn drive<O: Operation>(mut self, mut operation: O) -> Result<O::Output, Stop> {
        loop {
            let step = operation.poll(&*self.environment);
            for notice in step.notices {
                self.notice(notice).await;
            }
            for settled in step.settled {
                self.answer(settled);
            }
            for delivered in step.delivered {
                self.delivered(delivered);
            }
            if let Some(decided) = step.decided
                && let Some(caller) = self.caller.as_deref_mut()
            {
                caller.offer(&self.context, self.call, decided, self.refused.as_ref());
                // A caller gone before a commit in flight decided the call.
                self.cut_turns_once_gone(&mut operation);
            }
            let effects = match step.next {
                Next::Ready(output) => {
                    self.join_given_up().await;
                    return self.finish(output);
                }
                Next::Waiting => Vec::new(),
                Next::Effects(effects) => effects,
            };
            let answered = self.start_all(&mut operation, effects)?;
            if let Some(stop) = self.stop.take() {
                return Err(stop);
            }
            if !answered {
                self.wait(&mut operation).await?;
            }
        }
    }

    /// Notes a commit the run starts, `written` telling its outcome: the
    /// commit that decides an inbound call, `decides`, or a direct call's
    /// commit, which the kit's close reads back once it cut the run; an
    /// answer of the call's cascade or of an entry it adopted, `answer`,
    /// which a run the close aborted reports.
    fn note_the_commit(
        &mut self,
        event: &RoomEvent,
        decides: Option<Box<InboundResult>>,
        answer: Option<(AnswerOf, RoomEvent)>,
        written: &Written,
    ) {
        // An answer of an entry the run adopted is its handle's, whatever
        // the run's caller.
        if let Some((AnswerOf::Entry(entry), answer)) = &answer
            && let Some(answers) = self.entry_answers.get(entry)
        {
            answers.commits(answer.clone(), written.clone());
        }
        let Some(caller) = self.caller.as_deref() else {
            if let Some(in_flight) = &self.in_flight {
                in_flight.commits(self.call, &event.id);
            }
            self.committing = Some((written.clone(), event.clone(), None));
            return;
        };
        if let Some((AnswerOf::Call, answer)) = answer {
            caller.answers(answer, written.clone());
        }
        if decides.is_some() {
            caller.commits(self.call, &event.id);
            self.committing = Some((written.clone(), event.clone(), decides));
        }
    }

    /// Starts `effects`, handing `operation` the answers carried out at
    /// once; whether there was one. The writes whose outcome the operation
    /// took on its previous poll are let go then: what their outcome led
    /// to is started, a commit's place in its room's lane taken.
    fn start_all<O: Operation>(
        &mut self,
        operation: &mut O,
        effects: Vec<Effect>,
    ) -> Result<bool, RoomKitError> {
        let mut answered = false;
        for effect in effects {
            if let Some((id, outcome)) = self.start(effect) {
                operation.complete(id, outcome)?;
                answered = true;
            }
        }
        self.written.clear();
        Ok(answered)
    }

    /// Waits for the next wait over, call finished or call into the room
    /// handed to the run, and hands it to the operation. A caller gone
    /// before the call's decision expires the operation, whose requests
    /// within the call's bound the run then drops: the operation goes on to
    /// its end, detached (decision 1 of step 2b, gap G-076).
    async fn wait<O: Operation>(&mut self, operation: &mut O) -> Result<(), Stop> {
        loop {
            match self.next_event().await {
                Some(Event::Phase(phase)) => {
                    self.phase = phase;
                    return self.closing(operation, phase).await;
                }
                Some(Event::CallerGone) => {
                    if self.caller.as_deref_mut().is_some_and(Caller::expires) {
                        let given_up = operation.expire();
                        self.give_up(&given_up);
                        return Ok(());
                    }
                    if self.cut_turns_once_gone(operation) {
                        return Ok(());
                    }
                }
                Some(Event::Waited(id, waited)) => {
                    let outcome = self.held.settle(waited);
                    return Ok(operation.complete(id, outcome)?);
                }
                Some(Event::Joined(joined)) => {
                    // A call the operation gave up on answers nothing.
                    if let Some((id, outcome)) = self.joined(joined) {
                        return Ok(operation.complete(id, outcome)?);
                    }
                }
                Some(Event::Adoption(adoption)) => {
                    self.adopt(operation, adoption);
                    return Ok(());
                }
                // The operation waits for effects that were never issued.
                None => return Err(stalled().into()),
            }
        }
    }

    /// Acts on the kit's close reaching `phase` (RFC §12.10.4), but for a
    /// run the close spares, whose queued deliveries alone are dropped. As
    /// the close starts, a delivery set queued in a room's lane is dropped,
    /// and a call waiting for its room's lock or admission ticket before
    /// its commit is cut. Once its grace ran out, a call before its commit
    /// is cut, reading its commit back when that commit had started, and a
    /// call past it has its calls in flight, and from then on, fail
    /// `kit_closed`, so that it reports what it delivered.
    async fn closing<O: Operation>(&mut self, operation: &mut O, phase: Phase) -> Result<(), Stop> {
        self.drop_queued(operation)?;
        if self.inner.tasks.spares(&self.marks) {
            return Ok(());
        }
        let decided = self.caller.as_deref().is_some_and(Caller::is_decided);
        if phase == Phase::Closing {
            let waits = self
                .waits
                .iter()
                .any(|(_, wait, _)| *wait == Wait::Admission);
            if waits && !decided && !self.own && self.committing.is_none() {
                return Err(Stop::Cut(None));
            }
            return Ok(());
        }
        if !decided {
            return Err(Stop::Cut(self.read_back().await));
        }
        self.cut_past_the_decision(operation)
    }

    /// Answers the run's waits for a place's turn in a room's lane at once:
    /// the kit's close drops the delivery sets queued (RFC §12.10.4).
    fn drop_queued<O: Operation>(&mut self, operation: &mut O) -> Result<(), RoomKitError> {
        let queued: Vec<EffectId> = self
            .waits
            .iter()
            .filter(|(_, wait, _)| *wait == Wait::Lane)
            .map(|(effect, _, _)| *effect)
            .collect();
        self.waits.retain(|(_, wait, _)| *wait != Wait::Lane);
        for effect in queued {
            operation.complete(effect, HoldOutcome::LaneClosed.into())?;
        }
        Ok(())
    }

    /// The commit that decides the call, read back by its id once its write
    /// ended, the kit's close having cut it, within the unwind the cut
    /// gives it, from the cut's instant, as the Python implementation's
    /// `timeout_at(cut_at + CUT_UNWIND_S)` (RFC §12.10.4): its event, `None`
    /// when the store holds none, or when the read fails or does not end
    /// within the unwind, as a warning says. A run the close aborts first,
    /// at the same instant, warns through its note of the commit in flight;
    /// a run a close spared meanwhile is never aborted, and ends so.
    async fn read_back(&mut self) -> Option<Box<Committed>> {
        let (mut written, event, decides) = self.committing.take()?;
        let cut_at = self.inner.tasks.cut_at().unwrap_or_else(Instant::now);
        let unwind = cut_at + CUT_UNWIND;
        drop(tokio::time::timeout_at(unwind, calls::outcome_of(&mut written)).await);
        let read = tokio::time::timeout_at(unwind, self.inner.store.get_event(&event.id)).await;
        self.read_back_over();
        let failure = match read {
            Ok(Ok(stored)) => {
                return stored
                    .filter(|stored| stored.room_id == event.room_id)
                    .map(|event| Box::new(Committed { event, decides }));
            }
            Ok(Err(error)) => error.to_string(),
            Err(_elapsed) => "it did not end within the close's unwind".to_owned(),
        };
        in_flight::read_back_failed(self.call, &event.id, &failure);
        None
    }

    /// The run's read-back is over: an abort landing now warns of nothing
    /// more, the read-back having said what it came to.
    fn read_back_over(&self) {
        if let Some(in_flight) = &self.in_flight {
            in_flight.settled();
        }
        if let Some(caller) = self.caller.as_deref() {
            caller.read_back_over();
        }
    }

    /// Cuts a run past its decision once the close's grace ran out: its
    /// calls in flight are dropped, each answered `kit_closed`, as its
    /// waits for a lock or a ticket are, and from now on so is each call it
    /// starts, but a read of the store: a write it asks is refused at its
    /// spawn, the grace run out, so that nothing else of the cut run's
    /// aftermath is stored, as in the Python implementation (RFC §12.10.4).
    fn cut_past_the_decision<O: Operation>(&mut self, operation: &mut O) -> Result<(), Stop> {
        self.cut_now = true;
        let waiting: Vec<EffectId> = self
            .waits
            .iter()
            .filter(|(_, wait, _)| *wait == Wait::Admission)
            .map(|(effect, _, _)| *effect)
            .collect();
        self.waits.retain(|(_, wait, _)| *wait != Wait::Admission);
        let running: Vec<(EffectId, AbortHandle)> = self.call_of_effect.drain().collect();
        for (effect, call) in running {
            call.abort();
            self.effect_of_task.remove(&call.id());
            self.shared.answered(effect);
            self.writing.remove(&effect);
            operation.complete(effect, Outcome::Failed(cut_by_close()))?;
        }
        for effect in waiting {
            operation.complete(effect, Outcome::Failed(cut_by_close()))?;
        }
        Ok(())
    }

    /// Performs `notice`: a read the operation stops is the run's own, any
    /// other the kit's. A framework event's handlers are waited for before
    /// the run goes on, as Python's `_emit_framework_event` awaits them.
    async fn notice(&mut self, notice: Notice) {
        match notice {
            Notice::StopReading { effect } => self.shared.stop_reading(effect),
            notice @ (Notice::FrameworkEvent(_) | Notice::AsyncHooks { .. }) => {
                if let Some(handled) = calls::perform(self.inner, notice) {
                    handled.wait().await;
                }
            }
        }
    }

    /// Cuts the turns of the call's cascade once its caller is gone and the
    /// call decided, once (RFC §10.1 "Explicit delivery cancellation"): the
    /// operation gives them up, and their calls are aborted. Whether it did.
    fn cut_turns_once_gone<O: Operation>(&mut self, operation: &mut O) -> bool {
        if !self.caller.as_deref_mut().is_some_and(Caller::cuts) {
            return false;
        }
        tracing::debug!(
            target: "roomkit::driver",
            call = self.call,
            "the caller of a decided call stopped waiting; its turns are cut, its events delivered"
        );
        let cut = operation.cancel_turns();
        self.give_up(&cut);
        true
    }

    /// Joins the calls the operation gave up on, aborted: the run ends once
    /// their work and its finalizers are over, a cut turn's among them (RFC
    /// §10.1 "Explicit delivery cancellation": the cancellation joins the
    /// turns it cuts). Nothing else is left running once the operation is
    /// over.
    async fn join_given_up(&mut self) {
        while self.calls.join_next().await.is_some() {}
        for turn in std::mem::take(&mut self.cut) {
            turn.ended().await;
        }
    }

    /// Drops the waits and the calls of `effects`, which the operation gave
    /// up on: a call is aborted, its frame standing in no admission order
    /// any more, and neither is ever answered.
    fn give_up(&mut self, effects: &[EffectId]) {
        self.waits
            .retain(|(effect, _, _)| !effects.contains(effect));
        for effect in effects {
            // The turn the call awaits is cut before the call is dropped,
            // so its executor reads its cancel before it goes on.
            self.cut.extend(self.shared.cut_turn(*effect));
            if let Some(call) = self.call_of_effect.remove(effect) {
                call.abort();
                self.effect_of_task.remove(&call.id());
                self.shared.answered(*effect);
            }
            self.writing.remove(effect);
        }
    }

    /// Starts `effect`: a hold is answered at once when it can be, or
    /// waited for; any other family is a call, run by its family's handler.
    fn start(&mut self, effect: Effect) -> Option<(EffectId, Outcome)> {
        let work = Work::of(&effect);
        let Effect {
            id,
            request,
            deadline,
            decides,
            answers,
            ..
        } = effect;
        let deadline = deadline.and_then(|deadline| self.clocks.instant_of(deadline));
        let inner = Arc::clone(self.inner);
        let mut commit = None;
        if let Request::Store(StoreRequest::Commit { event, .. }) = &request {
            commit = Some(((**event).clone(), answers.map(|of| (of, (**event).clone()))));
            self.indexes.committing(id, event);
        }
        if self.cut_now && !matches!(&request, Request::Hold(_) | Request::Store(_)) {
            // A run the close cut past its decision calls nothing more; a
            // write it asks is refused at its spawn, the grace run out.
            return Some((id, Outcome::Failed(cut_by_close())));
        }
        match request {
            Request::Hold(hold) => return self.hold(id, hold, deadline),
            Request::Store(request) if writes(&request) => {
                let (writing, written, went_on) = calls::write(&inner, request);
                if let Some((event, answer)) = commit {
                    self.note_the_commit(&event, decides, answer, &written);
                }
                // Only an inbound run's commit takes a place in a room's
                // lane, before its write's task ends: a direct call's
                // writes end with their write.
                if self.caller.is_some() {
                    self.writing.insert(id, went_on);
                }
                self.spawn(id, work, deadline, writing);
            }
            Request::Store(request) => self.spawn(id, work, deadline, async move {
                calls::store(&*inner.store, request).await
            }),
            Request::Channel(request) => {
                let environment = Arc::clone(&self.environment);
                let shared = Arc::clone(&self.shared);
                self.spawn(id, work, deadline, async move {
                    Box::pin(calls::channel(&inner, &*environment, &shared, id, request)).await
                });
            }
            Request::Hook(request) => self.spawn(id, work, deadline, async move {
                calls::hook(&inner, request).await
            }),
            Request::Route { query } => self.spawn(id, work, deadline, async move {
                calls::route(&inner, &query).await
            }),
        }
        None
    }

    /// Carries out the hold `request` for `effect`, within `deadline`: at
    /// once, refused, or as a wait the run polls. Once the kit's close
    /// started, a delivery set takes no place in a room's lane, and one
    /// placed behind another waits for no turn there: its delivery is
    /// dropped, as Python's sealed lanes drop what they queue; a set placed
    /// first, the lane idle, runs from its placement and takes its turn
    /// (`Held::runs`); a call before its commit that would wait for its
    /// room's lock or admission ticket is cut there, and one past it, once
    /// the close's grace ran out, has the wait fail (RFC §12.10.4).
    fn hold(
        &mut self,
        id: EffectId,
        request: HoldRequest,
        deadline: Option<Instant>,
    ) -> Option<(EffectId, Outcome)> {
        let rooms = Rooms {
            locks: &self.inner.locks,
            lanes: &self.inner.lanes,
            tickets: &self.inner.tickets,
        };
        let closing = self.inner.tasks.is_closing();
        let runs = matches!(
            &request,
            HoldRequest::AwaitLane { room_id, event_id } if self.held.runs(room_id, event_id)
        );
        if closing
            && !runs
            && matches!(
                request,
                HoldRequest::EnterLane { .. } | HoldRequest::AwaitLane { .. }
            )
        {
            return Some((id, HoldOutcome::LaneClosed.into()));
        }
        let wait = match request {
            HoldRequest::AcquireLock { .. } | HoldRequest::AwaitTicket { .. } => Wait::Admission,
            HoldRequest::AwaitLane { .. } if runs => Wait::Running,
            HoldRequest::AwaitLane { .. } => Wait::Lane,
            HoldRequest::ReleaseLock { .. }
            | HoldRequest::EnterLane { .. }
            | HoldRequest::LeaveLane { .. }
            | HoldRequest::TakeTicket { .. }
            | HoldRequest::ReleaseTicket { .. } => Wait::Other,
        };
        let over = self.turns_over(&request);
        match self.held.hold(rooms, &self.context, id, request, deadline) {
            Hold::Wait(_) if wait == Wait::Admission && self.cut_now => {
                Some((id, Outcome::Failed(cut_by_close())))
            }
            Hold::Wait(_)
                if wait == Wait::Admission && closing && self.cuts_before_its_commit() =>
            {
                self.stop = Some(Stop::Cut(None));
                None
            }
            Hold::Wait(waiting) => {
                self.waits.push((id, wait, waiting));
                None
            }
            Hold::Now(outcome) => self.after_the_cursor(id, outcome, over),
            Hold::Refused(room_id) => {
                tracing::warn!(
                    target: "roomkit::driver",
                    call = self.call,
                    room_id = %room_id,
                    "a call would wait for a lock or a ticket its own caller holds; it fails"
                );
                let error = holds::reentrant(&room_id);
                self.refused.get_or_insert(room_id);
                Some((id, Outcome::Failed(error)))
            }
        }
    }

    /// Whether the kit's close, started, cuts the run at a wait for its
    /// room's lock or admission ticket: a call before its commit, but one
    /// the close spares or the kit's own work (RFC §12.10.4).
    fn cuts_before_its_commit(&self) -> bool {
        let decided = self.caller.as_deref().is_some_and(Caller::is_decided);
        !decided && !self.own && self.committing.is_none() && !self.inner.tasks.spares(&self.marks)
    }

    /// The room and the indexes whose turn `request` ends (gap G-091): a
    /// set's once it leaves its place, those the run committed and placed
    /// nowhere once it lets the room's lock go.
    fn turns_over(&mut self, request: &HoldRequest) -> Option<(RoomId, Vec<u64>)> {
        let (room_id, over) = match request {
            HoldRequest::EnterLane { room_id, event_id } => {
                self.indexes.entered(room_id, event_id);
                return None;
            }
            HoldRequest::LeaveLane { room_id, event_id } => (
                room_id,
                self.indexes.left(room_id, event_id).into_iter().collect(),
            ),
            HoldRequest::ReleaseLock { room_id } => (room_id, self.indexes.released(room_id)),
            HoldRequest::AcquireLock { .. }
            | HoldRequest::AwaitLane { .. }
            | HoldRequest::TakeTicket { .. }
            | HoldRequest::AwaitTicket { .. }
            | HoldRequest::ReleaseTicket { .. } => return None,
        };
        (!over.is_empty()).then(|| (room_id.clone(), over))
    }

    /// Answers a hold carried out at once, `outcome`, once the room's
    /// delivery cursor caught up with the turns it ended, `over`: the run
    /// waits for the cursor's writes, the room's lane and lock going on
    /// meanwhile (RFC §13.5, gap G-091).
    fn after_the_cursor(
        &mut self,
        id: EffectId,
        outcome: Outcome,
        over: Option<(RoomId, Vec<u64>)>,
    ) -> Option<(EffectId, Outcome)> {
        let Some((room_id, indexes)) = over else {
            return Some((id, outcome));
        };
        let inner: &'kit Arc<Inner<S>> = self.inner;
        inner.cursors.over(&room_id, indexes);
        let caught_up = async move {
            inner.cursors.catch_up(&*inner.store, &room_id).await;
            (id, Waited::Answered(outcome))
        };
        self.waits.push((id, Wait::Other, Box::pin(caught_up)));
        None
    }

    /// Runs `call`, the call of `effect`, as a task within its execution
    /// context and the run's marks, the run's frame recording `work` for
    /// it, and within `deadline`.
    fn spawn(
        &mut self,
        effect: EffectId,
        work: Work,
        deadline: Option<Instant>,
        call: impl Future<Output = Outcome> + Send + 'static,
    ) {
        let frame = Frame::new(&self.shared, effect, work);
        self.shared.calling(effect);
        let context = self.context.within(frame);
        let task = self
            .calls
            .spawn(self.marks.clone().scoped(context.scope(async move {
                within(deadline, call)
                    .await
                    .unwrap_or(Outcome::DeadlinePassed)
            })));
        self.effect_of_task.insert(task.id(), effect);
        self.call_of_effect.insert(effect, task);
    }

    /// The next wait over, call finished, call into the room handed to the
    /// run, or the run's caller gone; `None` when the run waits for
    /// nothing.
    async fn next_event(&mut self) -> Option<Event<'kit>> {
        if self.waits.is_empty() && self.calls.is_empty() {
            return None;
        }
        let Self {
            waits,
            calls,
            adoptions,
            caller,
            phases,
            phase,
            ..
        } = self;
        let seen = *phase;
        let moved = async {
            let now = phases.wait_for(|now| *now > seen).await.map(|now| *now);
            match now {
                Ok(now) => now,
                // The kit is gone: the run's calls end with it.
                Err(_closed) => std::future::pending().await,
            }
        };
        let event = poll_fn(|cx| {
            if let Some(caller) = caller.as_deref_mut()
                && caller.poll_gone(cx).is_ready()
            {
                return Poll::Ready(Some(Event::CallerGone));
            }
            let ready = waits
                .iter_mut()
                .enumerate()
                .find_map(|(index, (_, _, wait))| match wait.as_mut().poll(cx) {
                    Poll::Ready(waited) => Some((index, waited)),
                    Poll::Pending => None,
                });
            if let Some((index, (effect, waited))) = ready {
                drop(waits.swap_remove(index));
                return Poll::Ready(Some(Event::Waited(effect, waited)));
            }
            if let Poll::Ready(Some(joined)) = calls.poll_join_next_with_id(cx) {
                return Poll::Ready(Some(Event::Joined(joined)));
            }
            if let Poll::Ready(Some(adoption)) = adoptions.poll_recv(cx) {
                return Poll::Ready(Some(Event::Adoption(adoption)));
            }
            Poll::Pending
        });
        tokio::select! {
            biased;
            now = moved => Some(Event::Phase(now)),
            event = event => event,
        }
    }

    /// The effect a finished call answers, and its outcome: a call that
    /// panicked or was cancelled fails its effect instead of the kit.
    /// `None` for a call the operation gave up on.
    fn joined(
        &mut self,
        joined: Result<(task::Id, Outcome), JoinError>,
    ) -> Option<(EffectId, Outcome)> {
        let (task, outcome) = match joined {
            Ok(finished) => finished,
            Err(failure) => {
                let error = if failure.is_panic() {
                    calls::panicked()
                } else {
                    ErrorRecord::new("Cancelled", "the call was cancelled")
                };
                (failure.id(), Outcome::Failed(error))
            }
        };
        let effect = self.effect_of_task.remove(&task)?;
        self.call_of_effect.remove(&effect);
        self.written.extend(self.writing.remove(&effect));
        self.shared.answered(effect);
        self.indexes.answered(effect, &outcome);
        Some((effect, outcome))
    }

    /// Hands the operation a call into its room made by one of its calls;
    /// a call it does not adopt is declined, and runs apart.
    fn adopt<O: Operation>(&mut self, operation: &mut O, adoption: Adoption) {
        let Adoption {
            entry,
            origin,
            reply,
            delivered,
            answers,
        } = adoption;
        match operation.adopt(entry, origin) {
            Ok(id) => {
                self.callers.insert(id, reply);
                self.deliveries.insert(id, delivered);
                self.entry_answers.insert(id, answers);
            }
            Err(entry) => {
                tracing::debug!(
                    target: "roomkit::driver",
                    call = self.call,
                    room_id = %entry.room_id(),
                    "a call into the room was not adopted: its caller's call is over; it runs apart"
                );
                drop(reply.send(Reply::Declined(entry)));
            }
        }
    }

    /// Tells the caller of an adopted entry its delivery's end (RFC §10.1
    /// step 18).
    fn delivered(&mut self, delivered: Delivered) {
        self.entry_answers.remove(&delivered.entry);
        if let Some(completion) = self.deliveries.remove(&delivered.entry) {
            completion.send_replace(Some(delivered.result));
        }
    }

    /// Answers the caller of a settled entry, at its commit.
    fn answer(&mut self, settled: Settled) {
        let Some(caller) = self.callers.remove(&settled.entry) else {
            return;
        };
        if caller
            .send(Reply::Settled(Box::new(settled.result)))
            .is_err()
        {
            tracing::debug!(
                target: "roomkit::driver",
                call = self.call,
                "the caller of an adopted entry stopped waiting for its result"
            );
        }
    }

    /// The operation's output, or the refusal of a wait on what the run's
    /// own context holds.
    fn finish<T>(mut self, output: T) -> Result<T, Stop> {
        match self.refused.take() {
            Some(room_id) => Err(RoomKitError::ReentrantCall { room_id }.into()),
            None => Ok(output),
        }
    }
}

/// Whether `request` writes the store: a write runs to its end whatever
/// becomes of its run (RFC §13.6), as the Python implementation runs its
/// decided writes; a read is the run's own.
fn writes(request: &StoreRequest) -> bool {
    !matches!(
        request,
        StoreRequest::LoadContext { .. }
            | StoreRequest::GetEventByIdempotencyKey { .. }
            | StoreRequest::GetEvent { .. }
            | StoreRequest::ListEvents { .. }
            | StoreRequest::ResolveIdentity { .. }
    )
}

/// What `future` produces by `deadline`, `None` once it passed, the future
/// dropped then, or never polled when the deadline passed already: a
/// request held back until then is given up on, not granted (RFC §13.6);
/// with no deadline, what it produces.
async fn within<T>(deadline: Option<Instant>, future: impl Future<Output = T>) -> Option<T> {
    match deadline {
        Some(deadline) if deadline <= Instant::now() => None,
        Some(deadline) => tokio::time::timeout_at(deadline, future).await.ok(),
        None => Some(future.await),
    }
}

/// What a run that waits for effects the driver never received fails with,
/// or one answered for a call it never made: a defect of this crate.
pub(crate) fn stalled() -> RoomKitError {
    RoomKitError::Internal(
        "an operation's run stalled: it waited for effects the driver never received".to_owned(),
    )
}
