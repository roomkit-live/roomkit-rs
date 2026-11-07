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
mod spans;

use std::collections::HashMap;
use std::future::poll_fn;
use std::sync::Arc;
use std::task::Poll;

use roomkit_core::model::{ErrorRecord, RoomEvent, RoomId, Timestamp};
use roomkit_core::ops::failure_log;
use roomkit_core::ops::{
    AnswerOf, ChannelRequest, Delivered, Effect, EffectId, EntryId, Environment, HoldOutcome,
    HoldRequest, InboundError, InboundResult, KIT_CLOSED, Next, Notice, Operation, Outcome,
    ProtocolError, Request, Settled, StoreOutcome, StoreRequest,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{self, AbortHandle, JoinError, JoinHandle, JoinSet};
use tokio::time::Instant;

pub(crate) use self::calls::cut_by_close;
pub(crate) use self::calls::load_context;
pub(crate) use self::calls::perform;
use self::calls::{WentOn, Written};
use self::holds::{Answer, Held, Hold, Leases, Rooms, Waited, Waiting};
use self::in_flight::InFlight;
use self::inbound::Caller;
pub(crate) use self::inbound::{
    CALLER_CANCELLED, EntryCleanup, InboundOutput, Ran, Record, StoredAnswers, cancelled_end,
    ended, run_inbound, run_rerun,
};
use self::indexes::Indexes;
use crate::context::{Adoption, ExecutionContext, Frame, Reply, RunShared, Work};
use crate::delivery_keys::{CancelRequest, Named, NamedGuard};
use crate::error::RoomKitError;
use crate::gaps::{CursorTurn, Turned};
use crate::kit::Inner;
use crate::store::ConversationStore;
use crate::stream::KeptStream;
use crate::tasks::{CUT_UNWIND, Kind, Marks, Phase};
use crate::telemetry::within_kit;
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
    // Boxed whole: the operation waits there for the kit's subscription.
    Box::pin(async move {
        inner.subscribed().await;
        // A close that ended the wait found the call past its door, which
        // Python's finds under its room's lock: the call has the close's grace.
        let own = inner.tasks.is_closing();
        run_in_its_task(inner, environment, call, operation, own).await
    })
    .await
    .map_err(Stop::into_error)
}

/// Runs `operation` as [`run`] does, without the kit's subscription: a call
/// that reads no context of its room, a change of an event no hook reads.
///
/// # Errors
///
/// As [`run`].
pub(crate) async fn run_unread<S: ConversationStore, O: Operation + Send + 'static>(
    inner: &Arc<Inner<S>>,
    call: &'static str,
    operation: O,
) -> Result<O::Output, RoomKitError>
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
    let (mut caller, own, in_flight) = match how {
        Driven::Inbound(caller) => (Some(caller), false, None),
        Driven::Call { own, in_flight } => (None, own, Some(in_flight)),
    };
    let (shared, adoptions) = RunShared::new();
    let mut held = Held::new(Arc::clone(&shared), leases);
    if let Some(caller) = &mut caller {
        match caller.hand.take() {
            Some(inbound::Under::Hand(room_id, guard)) => held.take_hand(room_id, guard),
            Some(inbound::Under::Lease(lease)) => held.take_lease(lease),
            None => {}
        }
    }
    let driver = Driver {
        calls: JoinSet::new(),
        waits: Vec::new(),
        spans: spans::RunSpans::new(inner.telemetry.clone()),
        inner,
        call,
        context: ExecutionContext::current(),
        marks: Marks::current(),
        held,
        indexes: Indexes::default(),
        shared,
        adoptions,
        effect_of_task: HashMap::new(),
        call_of_effect: HashMap::new(),
        callers: HashMap::new(),
        following: HashMap::new(),
        muted: Vec::new(),
        deliveries: HashMap::new(),
        entry_answers: HashMap::new(),
        entry_cleanups: HashMap::new(),
        entry_cancels: HashMap::new(),
        entry_cut: HashMap::new(),
        entry_named: HashMap::new(),
        entry_names: HashMap::new(),
        clocks: Clocks::read(&*environment),
        environment,
        refused: None,
        caller,
        cut: Vec::new(),
        unread: Vec::new(),
        answered: None,
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
    /// The `framework.broadcast` spans of the run's delivery sets.
    spans: spans::RunSpans,
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
    /// The origin of each entry the operation adopted whose call that
    /// origin's code made in its own task, until settled: the entry follows
    /// that code, given up with it (RFC §9.5.1 Reentrance).
    following: HashMap<EntryId, EffectId>,
    /// The callers of the entries cut as the code that made them was given
    /// up, never answered: that code is dropped as it is aborted, and never
    /// reads the entry's refusal, until the run ends.
    muted: Vec<oneshot::Sender<Reply>>,
    /// Where the delivery's end of each entry the operation adopted goes,
    /// until its cascade is over.
    deliveries: HashMap<EntryId, watch::Sender<Option<InboundResult>>>,
    /// The answers of each adopted entry's cascade, which its handle reads
    /// when the run ends without telling its delivery's end.
    entry_answers: HashMap<EntryId, Arc<StoredAnswers>>,
    /// What each adopted entry's cut leaves its handle to wait for.
    entry_cleanups: HashMap<EntryId, Arc<EntryCleanup>>,
    /// The cut each adopted entry's caller may ask of its turns, until
    /// asked (RFC §10.1 "Explicit delivery cancellation").
    entry_cancels: HashMap<EntryId, Arc<CancelRequest>>,
    /// Why each adopted entry's turns were cut, once asked: its caller's
    /// `cancellation_reason` (Python's `DeliveryCascade.cancelled`).
    entry_cut: HashMap<EntryId, String>,
    /// The name each adopted entry's delivery key gives it, with the cut
    /// its caller may ask, until the entry settles.
    entry_named: HashMap<EntryId, (Arc<Named>, Arc<CancelRequest>)>,
    /// The name of each settled entry's delivery, held until its set ran.
    entry_names: HashMap<EntryId, NamedGuard>,
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
    /// The streams the cut turns answered that no read took, until they are
    /// closed: Python's cascade cleanup closes each and waits for its end.
    unread: Vec<KeptStream>,
    /// The outcome an inbound call answers its caller with before the
    /// entries it adopted are over (`Notice::Answered`), until its step's
    /// decision is offered.
    answered: Option<Result<InboundResult, InboundError>>,
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
    /// A room's lock to record a delivery set (RFC §14.3): work past its
    /// event's commit, which the kit's close fails once its grace ran out.
    Record,
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
    /// The caller of an entry the operation adopted stopped waiting for its
    /// answer.
    EntryCallerGone(EntryId),
    /// The delivery of an entry the operation adopted was cancelled, for
    /// this reason.
    EntryCancelled(EntryId, String),
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
                self.end_cancelled_once_over();
            }
            if let Some(outcome) = self.answered.take()
                && let Some(caller) = self.caller.as_deref_mut()
            {
                caller.answer_now(outcome);
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
                        self.give_up_with_followers(operation, given_up);
                        return Ok(());
                    }
                    if self.cut_turns_once_gone(operation) {
                        self.end_cancelled_once_over();
                        return Ok(());
                    }
                }
                Some(Event::EntryCallerGone(entry)) => {
                    self.following.remove(&entry);
                    let given_up = operation.entry_caller_gone(entry);
                    self.give_up_with_followers(operation, given_up);
                    return Ok(());
                }
                Some(Event::EntryCancelled(entry, reason)) => {
                    self.entry_cut.insert(entry, reason);
                    let cut = operation.cancel_entry_turns(entry);
                    self.give_up(&cut);
                    self.close_unread_of(entry);
                    return Ok(());
                }
                Some(Event::Waited(id, waited)) => {
                    let outcome = self.held.settle(waited);
                    self.spans.answered(&outcome);
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
            .filter(|(_, wait, _)| matches!(wait, Wait::Admission | Wait::Record))
            .map(|(effect, _, _)| *effect)
            .collect();
        self.waits
            .retain(|(_, wait, _)| !matches!(wait, Wait::Admission | Wait::Record));
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
            Notice::CutReadOver {
                response_metadata,
                error,
                rewritten,
            } => {
                if let Some(caller) = self.caller.as_deref() {
                    caller
                        .record()
                        .cut_read_over(response_metadata, error, rewritten);
                }
            }
            Notice::Span(note) => {
                if let Some(caller) = self.caller.as_deref() {
                    caller.noted(&note);
                }
                self.spans.noted(note);
            }
            // Answered once the step's decision is offered (`drive`).
            Notice::Answered(outcome) => self.answered = Some(*outcome),
            Notice::Failure {
                log,
                what,
                error,
                caller_logs,
            } => {
                // A caller answered at the decision, or gone, never reads
                // the failure: the line is the framework's (RFC §15.2).
                let receives = caller_logs && self.caller.as_deref().is_none_or(Caller::reads);
                failure_log::log_failure(log, &error, &what, receives);
            }
            notice @ (Notice::FrameworkEvent(_) | Notice::AsyncHooks { .. }) => {
                if let Some(handled) = perform(self.inner, notice) {
                    handled.wait().await;
                }
            }
        }
    }

    /// Cuts the turns of the call's cascade once its caller is gone and the
    /// call decided, or once its delivery is cancelled, once (RFC §10.1
    /// "Explicit delivery cancellation"): the operation gives them up, and
    /// their calls are aborted. Whether it did.
    fn cut_turns_once_gone<O: Operation>(&mut self, operation: &mut O) -> bool {
        if !self.caller.as_deref_mut().is_some_and(Caller::cuts) {
            return false;
        }
        tracing::debug!(
            target: "roomkit::driver",
            call = self.call,
            "a call's caller stopped waiting or cancelled its delivery; its turns are cut, its events delivered"
        );
        if operation.tells_its_cut()
            && let Some(caller) = self.caller.as_deref()
        {
            caller.record().await_cut_read();
        }
        let cut = operation.cancel_turns();
        self.give_up(&cut);
        true
    }

    /// Reports the call as its cancellation ends it once the turns it cut
    /// are over, once its delivery cancelled and the call decided: its
    /// caller then reads it without waiting for the delivery that goes on
    /// (RFC §10.1 "Explicit delivery cancellation"), then closes the streams
    /// the cut left unread, which the call and its handle wait for. A cut
    /// nobody reads as cancelled closes them alone.
    fn end_cancelled_once_over(&mut self) {
        let ends = self
            .caller
            .as_deref_mut()
            .is_some_and(Caller::ends_cancelled);
        let record = self.caller.as_deref().filter(|_| ends).map(Caller::record);
        let Some(record) = record.map(Arc::clone) else {
            return self.close_unread_alone();
        };
        let unread = std::mem::take(&mut self.unread);
        // Told before the run can end: its caller's end waits for the
        // closing, however the task that closes ends.
        let closing = (!unread.is_empty()).then(|| record.closes_unread());
        let ending = inbound::end_once_over(
            record,
            self.cut.clone(),
            self.shared.read_turns(),
            (unread, closing),
        );
        drop(self.inner.tasks.spawn_alone(Kind::Turn, ending));
    }

    /// Closes the streams the cut of the adopted entry `entry` left unread
    /// one after the other, each waiting for its end, on a task of the
    /// kit's, which the entry's handle waits for, as Python's adopted call's
    /// own cascade cleanup (RFC §10.1 "Explicit delivery cancellation").
    fn close_unread_of(&mut self, entry: EntryId) {
        // Adopted with its cancel, an entry the cut reaches has its cleanup.
        let Some(cleanup) = self.entry_cleanups.get(&entry).map(Arc::clone) else {
            return;
        };
        let unread = std::mem::take(&mut self.unread);
        if unread.is_empty() {
            return;
        }
        let closing = cleanup.closes_unread(unread);
        // Refused, the closing is dropped, and with it what the handle waits
        // for.
        drop(self.inner.tasks.spawn_alone(Kind::Turn, closing));
    }

    /// Closes the streams the cut left unread one after the other, each
    /// waiting for its end, on a task of the kit's nobody waits for: the
    /// cut of a call no caller reads as cancelled.
    fn close_unread_alone(&mut self) {
        let unread = std::mem::take(&mut self.unread);
        if unread.is_empty() {
            return;
        }
        let closing = async move {
            inbound::close_unread(unread).await;
        };
        drop(self.inner.tasks.spawn_alone(Kind::Turn, closing));
    }

    /// Locates the call's delivery once `outcome`, a call's, reads its
    /// room: the first room the run reads is the call's (RFC §10.1 step 3),
    /// whose organization a scoped cancellation by key reaches (RFC §17.2).
    fn locate(&self, outcome: &Outcome) {
        if let (Some(caller), Outcome::Store(StoreOutcome::Context(context))) =
            (self.caller.as_deref(), outcome)
        {
            caller
                .record()
                .locate(context.room.organization_id.as_deref());
        }
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
        // A turn whose stream the run read, over without its executor's
        // end, leaves a cut call's cleanup incomplete, told before the run
        // ends (RFC §10.6.8).
        if self.shared.a_read_turn_s_end_is_owed()
            && let Some(caller) = self.caller.as_deref()
        {
            caller.record().owe_cleanup();
        }
    }

    /// Gives up on `effects` ([`give_up`](Self::give_up)), then on every
    /// entry whose call the code of one of them made in its own task: that
    /// code is aborted, and the entry it awaits ends with it before its
    /// commit (RFC §9.5.1 Reentrance), its caller never answered; and so on
    /// for what each such entry gives up.
    fn give_up_with_followers<O: Operation>(&mut self, operation: &mut O, effects: Vec<EffectId>) {
        let mut effects = effects;
        while !effects.is_empty() {
            self.give_up(&effects);
            let followers: Vec<EntryId> = self
                .following
                .iter()
                .filter(|(_, origin)| effects.contains(origin))
                .map(|(entry, _)| *entry)
                .collect();
            effects = Vec::new();
            for entry in followers {
                self.following.remove(&entry);
                self.muted.extend(self.callers.remove(&entry));
                effects.extend(operation.entry_caller_gone(entry));
            }
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
            } else {
                // A turn over that answered with a stream no read took: the
                // stream is closed. One a turn in flight answers is dropped
                // unread with the run, as Python's cancelled cascade drops a
                // stream it is handed.
                self.unread.extend(self.shared.take_unread_stream(*effect));
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
        let span = self.spans.requested(&request);
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
                self.spawn(id, work, deadline, None, writing);
            }
            Request::Store(request) => self.spawn(id, work, deadline, None, async move {
                calls::store_followed(&inner, request).await
            }),
            Request::Channel(request) => {
                self.reads_for_entry(id, &request, answers);
                let shared = Arc::clone(&self.shared);
                self.spawn(id, work, deadline, span, async move {
                    Box::pin(calls::channel(&inner, &shared, id, request)).await
                });
            }
            Request::Hook(request) => self.spawn(id, work, deadline, None, async move {
                calls::hook(&inner, request).await
            }),
            Request::Route { query } => self.spawn(id, work, deadline, None, async move {
                calls::route(&inner, &query).await
            }),
            Request::Discussion(request) => self.spawn(id, work, deadline, None, async move {
                calls::discussion(&inner, request).await
            }),
            Request::Identity(request) => self.spawn(id, work, deadline, None, async move {
                calls::identity(&inner, request).await
            }),
        }
        None
    }

    /// Makes `effect`'s read, `request`, of the cascade of the adopted entry
    /// `answers` names, tell that entry's cleanup whether its turn owes its
    /// end (`Effect::answers`, RFC §10.6.8).
    fn reads_for_entry(
        &self,
        effect: EffectId,
        request: &ChannelRequest,
        answers: Option<AnswerOf>,
    ) {
        let (ChannelRequest::ReadStream { .. }, Some(AnswerOf::Entry(entry))) = (request, answers)
        else {
            return;
        };
        if let Some(cleanup) = self.entry_cleanups.get(&entry) {
            self.shared.reads_for_entry(effect, cleanup.read_turns());
        }
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
            HoldRequest::AcquireLockForRecord { .. } => Wait::Record,
            HoldRequest::AwaitLane { .. } if runs => Wait::Running,
            HoldRequest::AwaitLane { .. } => Wait::Lane,
            HoldRequest::ReleaseLock { .. }
            | HoldRequest::EnterLane { .. }
            | HoldRequest::LeaveLane { .. }
            | HoldRequest::TakeTicket { .. }
            | HoldRequest::ReleaseTicket { .. }
            | HoldRequest::Expiry { .. } => Wait::Other,
        };
        let over = self.turns_over(&request);
        let across = self.across_processes(&request);
        let turned = if let HoldRequest::LeaveLane { room_id, event_id } = &request {
            self.held.take_turn(room_id, event_id)
        } else {
            None
        };
        match self.held.hold(rooms, &self.context, id, request, deadline) {
            Hold::Wait(_) if matches!(wait, Wait::Admission | Wait::Record) && self.cut_now => {
                Some((id, Outcome::Failed(cut_by_close())))
            }
            Hold::Wait(_)
                if wait == Wait::Admission && closing && self.cuts_before_its_commit() =>
            {
                self.stop = Some(Stop::Cut(None));
                None
            }
            Hold::Wait(waiting) => {
                let waiting = match across {
                    Some((room_id, index)) => self.cursor_turn(waiting, room_id, index, deadline),
                    None => waiting,
                };
                self.waits.push((id, wait, waiting));
                None
            }
            Hold::Now(outcome) => self.after_the_cursor(id, Answer::Now(outcome), over, turned),
            Hold::Releasing(release) => {
                self.after_the_cursor(id, Answer::Release(release), over, turned)
            }
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
            | HoldRequest::AcquireLockForRecord { .. }
            | HoldRequest::AwaitLane { .. }
            | HoldRequest::TakeTicket { .. }
            | HoldRequest::AwaitTicket { .. }
            | HoldRequest::ReleaseTicket { .. }
            | HoldRequest::Expiry { .. } => return None,
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
        answer: Answer,
        over: Option<(RoomId, Vec<u64>)>,
        turned: Option<Turned>,
    ) -> Option<(EffectId, Outcome)> {
        let skipped = turned
            .as_ref()
            .is_some_and(|turned| !turned.skipped.is_empty());
        if over.is_none() && !skipped {
            let answer = match answer {
                Answer::Now(outcome) => return Some((id, outcome)),
                release @ Answer::Release(_) => release,
            };
            let released = async move { (id, Waited::Answered(answer.outcome().await)) };
            self.waits.push((id, Wait::Other, Box::pin(released)));
            return None;
        }
        let inner: &'kit Arc<Inner<S>> = self.inner;
        let room_id = over.map(|(room_id, indexes)| {
            inner.cursors.over(&room_id, indexes);
            room_id
        });
        let caught_up = async move {
            let outcome = answer.outcome().await;
            if let Some(room_id) = room_id {
                inner.cursors.catch_up(&*inner.store, &room_id).await;
            }
            // The room's delivery claim goes once the cursor moved past the
            // set's index; the handlers of the skips its wait emitted are
            // waited for once it left its place (RFC §13.5).
            if let Some(Turned { claim, skipped }) = turned {
                drop(claim);
                for handled in skipped {
                    handled.wait().await;
                }
            }
            (id, Waited::Answered(outcome))
        };
        self.waits.push((id, Wait::Other, Box::pin(caught_up)));
        None
    }

    /// The room and the index of the set whose lane turn `request` waits
    /// for, when the run committed its event: its turn waits for the room's
    /// cursor too (RFC §13.5 "Delivery lanes across processes"). An
    /// instruction's set, or a rerun's, has none.
    fn across_processes(&self, request: &HoldRequest) -> Option<(RoomId, u64)> {
        let HoldRequest::AwaitLane { room_id, event_id } = request else {
            return None;
        };
        let index = self.indexes.placed_index(room_id, event_id)?;
        Some((room_id.clone(), index))
    }

    /// `waiting`, the wait for a place's turn in its room's lane, then for
    /// the turn the room's cursor gives its set, with the room's delivery
    /// claim held for it, within `deadline` (`gaps`).
    fn cursor_turn(
        &self,
        waiting: Waiting<'kit>,
        room_id: RoomId,
        index: u64,
        deadline: Option<Instant>,
    ) -> Waiting<'kit> {
        let inner: &'kit Arc<Inner<S>> = self.inner;
        Box::pin(async move {
            let (effect, waited) = waiting.await;
            let Waited::Place(key, place, true) = waited else {
                return (effect, waited);
            };
            match within(deadline, crate::gaps::cursor_turn(inner, &room_id, index)).await {
                Some(CursorTurn::Turn(turned)) => (effect, Waited::Claimed(key, place, turned)),
                Some(CursorTurn::Stale) => (effect, Waited::Stale(key, place)),
                None => (effect, Waited::Place(key, place, false)),
            }
        })
    }

    /// Runs `call`, the call of `effect`, as a task within its execution
    /// context and the run's marks, the run's frame recording `work` for
    /// it, and within `deadline`.
    fn spawn(
        &mut self,
        effect: EffectId,
        work: Work,
        deadline: Option<Instant>,
        span: Option<String>,
        call: impl Future<Output = Outcome> + Send + 'static,
    ) {
        let frame = Frame::new(&self.shared, effect, work);
        self.shared.calling(effect);
        let context = self.context.within(frame);
        let telemetry = &self.inner.telemetry;
        let call = within_kit(telemetry, span, async move {
            within(deadline, call)
                .await
                .unwrap_or(Outcome::DeadlinePassed)
        });
        let task = self
            .calls
            .spawn(self.marks.clone().scoped(context.scope(call)));
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
            callers,
            entry_cancels,
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
            // Before any answer: the code a call's end dropped no longer
            // waits for the entries it made (RFC §9.5.1 Reentrance).
            let gone = callers
                .iter_mut()
                .find_map(|(entry, reply)| reply.poll_closed(cx).is_ready().then_some(*entry));
            if let Some(entry) = gone {
                callers.remove(&entry);
                return Poll::Ready(Some(Event::EntryCallerGone(entry)));
            }
            let cancelled = entry_cancels
                .iter()
                .find_map(|(entry, cancel)| cancel.poll(cx).then(|| (*entry, cancel.reason())));
            if let Some((entry, reason)) = cancelled {
                entry_cancels.remove(&entry);
                let reason = reason.unwrap_or_else(|| CALLER_CANCELLED.to_owned());
                return Poll::Ready(Some(Event::EntryCancelled(entry, reason)));
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
        self.spans.answered(&outcome);
        self.locate(&outcome);
        self.call_of_effect.remove(&effect);
        self.written.extend(self.writing.remove(&effect));
        self.shared.answered(effect);
        if let Some((room_id, index)) = self.indexes.answered(effect, &outcome) {
            self.inner.cursors.committed(&room_id, index);
        }
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
            cancel,
            cleanup,
            named,
            late,
            span,
            task,
        } = adoption;
        match operation.adopt(entry, origin) {
            Ok(id) => {
                // Made by its origin's code in that code's own task, not in
                // a task the code spawned: it follows that code.
                if task.is_some_and(|task| self.effect_of_task.get(&task) == Some(&origin)) {
                    self.following.insert(id, origin);
                    operation.entry_follows_its_origin(id);
                }
                self.spans.adopted(id, late, span);
                if let Some(named) = named {
                    self.entry_named.insert(id, (named, Arc::clone(&cancel)));
                }
                self.callers.insert(id, reply);
                self.deliveries.insert(id, delivered);
                self.entry_answers.insert(id, answers);
                self.entry_cancels.insert(id, cancel);
                self.entry_cleanups.insert(id, cleanup);
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
    /// step 18), its cut's reason among it once asked, `kit_closed` for an
    /// entry the kit's close found under way (RFC §12.10.4), and stops
    /// naming that delivery.
    fn delivered(&mut self, delivered: Delivered) {
        let Delivered { entry, mut result } = delivered;
        self.entry_answers.remove(&entry);
        self.entry_cancels.remove(&entry);
        let cleanup = self.entry_cleanups.remove(&entry);
        let cut = self.entry_cut.remove(&entry);
        if let (Some(_), Some(cleanup)) = (&cut, cleanup) {
            cleanup.cut_over();
        }
        let closing = self.inner.tasks.is_closing() && !self.inner.tasks.spares(&self.marks);
        let reason = cut.or_else(|| closing.then(|| KIT_CLOSED.to_owned()));
        if let Some(reason) = reason {
            result.cancellation_reason.get_or_insert(reason);
        }
        if let Some(completion) = self.deliveries.remove(&entry) {
            completion.send_replace(Some(result));
        }
        // Its cascade is over: the delivery is, but for an event it holds.
        self.entry_names.remove(&entry);
    }

    /// Answers the caller of a settled entry, at its commit, once its
    /// delivery is named.
    fn answer(&mut self, settled: Settled) {
        self.name_settled(&settled);
        self.following.remove(&settled.entry);
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

    /// Names the delivery of `settled`, an adopted entry its key names,
    /// until its set ran, its cut that of the entry's turns in this run; an
    /// event of it held for verification is held by the key until its
    /// release (RFC §11.7.6). Done as the entry settles, under the room's
    /// lock, as Python's `_name_held`: no release reaches the event before.
    fn name_settled(&mut self, settled: &Settled) {
        let Some((named, cancel)) = self.entry_named.remove(&settled.entry) else {
            return;
        };
        if let Ok(result) = &settled.result {
            named.decided(result);
        }
        let mut guard = NamedGuard::of(named);
        guard.attach(cancel);
        self.entry_names.insert(settled.entry, guard);
    }

    /// The operation's output, or the refusal of a wait on what the run's
    /// own context holds.
    fn finish<T>(mut self, output: T) -> Result<T, Stop> {
        self.spans.over(self.refused.as_ref());
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
            | StoreRequest::ListEventsPage { .. }
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
