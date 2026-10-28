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
//! context stands in, fails the call with [`KitError::Reentrant`], since
//! that run waits for this one. An effect
//! that carries a deadline (RFC §13.6) is given up on past it: a lock or a
//! turn waited for is not taken, a call in flight is dropped, and the
//! operation is answered `DeadlinePassed`.

mod calls;
mod holds;
mod inbound;
mod indexes;

use std::collections::HashMap;
use std::future::poll_fn;
use std::sync::Arc;
use std::task::Poll;

use roomkit_core::model::{ErrorRecord, EventStatus, RoomId, Timestamp};
use roomkit_core::ops::{
    Delivered, Effect, EffectId, EntryId, Environment, HoldOutcome, HoldRequest, InboundResult,
    Next, Notice, Operation, Outcome, Request, Settled, StoreRequest,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{self, AbortHandle, JoinError, JoinSet};
use tokio::time::Instant;

use self::calls::WentOn;
use self::holds::{Held, Hold, Leases, Rooms, Waited, Waiting};
use self::inbound::Caller;
pub(crate) use self::inbound::{InboundOutput, Ran, Record, ended, run_inbound};
use self::indexes::Indexes;
use crate::context::{Adoption, ExecutionContext, Frame, Reply, RunShared, Work};
use crate::error::KitError;
use crate::kit::Inner;
use crate::store::ConversationStore;
use crate::tasks::Marks;
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

/// Runs `operation`, made by the kit's call `call`, until it is ready; a
/// lock its execution context holds a live lease on is granted to it (RFC
/// §13.5).
///
/// # Errors
///
/// Returns [`KitError::ShutDown`] once the kit is shut down, and
/// [`KitError::Reentrant`] when the operation waited for a ticket's turn in
/// an admission order its own execution context stands in, which was
/// refused; a protocol error or a stall is a defect of this crate.
pub(crate) async fn run<S: ConversationStore, O: Operation>(
    inner: &Arc<Inner<S>>,
    call: &'static str,
    operation: O,
) -> Result<O::Output, KitError> {
    run_on(inner, inner.environment(), call, operation).await
}

/// Runs `operation` as [`run`] does, on `environment`: the environment its
/// call read when it started, to stamp what it hands the operation, a new
/// room's instants or a binding's, with the clock the run reads.
///
/// # Errors
///
/// As [`run`].
pub(crate) async fn run_on<S: ConversationStore, O: Operation>(
    inner: &Arc<Inner<S>>,
    environment: Arc<dyn Environment + Send + Sync>,
    call: &'static str,
    operation: O,
) -> Result<O::Output, KitError> {
    inner.open()?;
    drive(inner, environment, call, operation, Leases::Granted, None).await
}

/// Runs `operation`, made by the kit's call `call`, until it is ready,
/// treating a lock its execution context holds a live lease on as `leases`
/// says, and offering its decision to its `caller`, when the run is an
/// inbound call's. Such a run is never dropped for its caller: a caller
/// gone before the call's decision expires the operation
/// (`Operation::expire`), which goes on to its end, delivering what it
/// committed (gap G-076).
async fn drive<S: ConversationStore, O: Operation>(
    inner: &Arc<Inner<S>>,
    environment: Arc<dyn Environment + Send + Sync>,
    call: &'static str,
    operation: O,
    leases: Leases,
    caller: Option<&mut Caller>,
) -> Result<O::Output, KitError> {
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
        clocks: Clocks::read(&*environment),
        environment,
        refused: None,
        caller,
        cut: Vec::new(),
        writing: HashMap::new(),
        written: Vec::new(),
        out_of_lanes: false,
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
    /// The waits the run polls, each by its effect.
    waits: Vec<(EffectId, Waiting<'kit>)>,
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
    /// Whether an inbound run told the kit it is out of its rooms' lanes.
    out_of_lanes: bool,
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
}

impl<'kit, S: ConversationStore> Driver<'kit, S> {
    async fn drive<O: Operation>(mut self, mut operation: O) -> Result<O::Output, KitError> {
        loop {
            let step = operation.poll(&*self.environment);
            for notice in step.notices {
                self.notice(notice);
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
            if !self.start_all(&mut operation, effects)? {
                self.note_the_lanes();
                self.wait(&mut operation).await?;
            }
        }
    }

    /// Tells an inbound run's caller of a write it starts, `written` telling
    /// its outcome: the commit that decides the call, `decides`, or another
    /// commit, of its cascade, of an event to deliver, when it `commits`.
    fn note_the_commit(
        &self,
        id: EffectId,
        decides: Option<Box<InboundResult>>,
        commits: bool,
        written: calls::Written,
    ) {
        let Some(caller) = self.caller.as_deref() else {
            return;
        };
        match decides {
            Some(result) => caller.commits(*result, written),
            None if commits => caller.cascade_commits(id, written),
            None => {}
        }
    }

    /// Tells the kit's close whether an inbound run is out of its rooms'
    /// lanes as it waits: it holds no lock, place in a lane or admission
    /// ticket. The close cuts such a run's turns once the lanes are idle,
    /// each of the streams it reads waiting (RFC §10.6.8), as the Python
    /// implementation's `turns.close` follows its `lanes.aclose`; a run
    /// not decided yet reads none.
    fn note_the_lanes(&mut self) {
        if self.caller.is_none() {
            return;
        }
        // A guard: a run reads its turns' streams once its sets left their
        // places, so a run reading holds none; were it to, a delivery set
        // of it would still run, and its lanes would not be idle.
        let out = !self.held.holds_any();
        if out == self.out_of_lanes {
            return;
        }
        self.out_of_lanes = out;
        if let Some(unit) = self.marks.innermost() {
            self.inner.tasks.out_of_lanes(unit, out);
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
    ) -> Result<bool, KitError> {
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
    async fn wait<O: Operation>(&mut self, operation: &mut O) -> Result<(), KitError> {
        loop {
            match self.next_event().await {
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
                    if let (Waited::Place((_, event_id), _, true), Some(caller)) =
                        (&waited, self.caller.as_deref())
                    {
                        caller.turn_came(event_id);
                    }
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
                None => return Err(KitError::Stalled),
            }
        }
    }

    /// Performs `notice`: a read the operation stops is the run's own, any
    /// other the kit's.
    fn notice(&self, notice: Notice) {
        match notice {
            Notice::StopReading { effect } => self.shared.stop_reading(effect),
            notice @ (Notice::FrameworkEvent(_) | Notice::AsyncHooks { .. }) => {
                calls::perform(self.inner, notice);
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
        self.waits.retain(|(effect, _)| !effects.contains(effect));
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
            if let Some(caller) = self.caller.as_deref() {
                caller.went_on(*effect, false);
            }
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
            ..
        } = effect;
        let deadline = deadline.and_then(|deadline| self.clocks.instant_of(deadline));
        let inner = Arc::clone(self.inner);
        // A commit of an event to deliver: one stored BLOCKED never is.
        let mut commits = false;
        if let Request::Store(StoreRequest::Commit { event, .. }) = &request {
            commits = event.status != EventStatus::Blocked;
            self.indexes.committing(id, event);
        }
        match request {
            Request::Hold(hold) => return self.hold(id, hold, deadline),
            Request::Store(request) if writes(&request) => {
                let (writing, written, went_on) = calls::write(&inner, request);
                self.note_the_commit(id, decides, commits, written);
                // Only an inbound run's commit takes a place in a room's
                // lane, and its run, a task of its own, is always polled; a
                // run in its caller's own task may sit unpolled, and its
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
    /// once, refused, or as a wait the run polls.
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
        if matches!(request, HoldRequest::EnterLane { .. }) && self.inner.tasks.is_draining() {
            // The kit's close drops a delivery set that would take its place
            // now, as the Python implementation's sealed lanes drop it: its
            // index has no place, its turn over at the lock's release.
            return Some((id, HoldOutcome::LaneClosed.into()));
        }
        if let (HoldRequest::LeaveLane { event_id, .. }, Some(caller)) =
            (&request, self.caller.as_deref())
        {
            caller.left(event_id);
        }
        let behind = self.waits_behind(&request);
        let over = self.turns_over(&request);
        match self.held.hold(rooms, &self.context, id, request, deadline) {
            Hold::Wait(wait) if behind => {
                // Stuck behind work the kit's close spares, which moves on
                // only once the close returned: the run is dropped at once,
                // as those the close's drain found there (RFC §12.10.4).
                self.waits.push((id, wait));
                let own = self.marks.innermost().into_iter().collect();
                self.inner.tasks.drop_work_of(&own);
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
            Hold::Wait(wait) => {
                self.waits.push((id, wait));
                None
            }
        }
    }

    /// Whether `request`, a wait for a room's lock or admission ticket an
    /// inbound run makes while the kit's close drains, would wait behind
    /// work the close spares, which holds them until the close returned.
    fn waits_behind(&self, request: &HoldRequest) -> bool {
        if self.caller.is_none() || !self.inner.tasks.is_draining() {
            return false;
        }
        let spared = self.inner.tasks.spared();
        match request {
            HoldRequest::AcquireLock { room_id } => {
                self.context.lease(room_id).is_none() && self.inner.locks.held_by(room_id, &spared)
            }
            HoldRequest::AwaitTicket { room_id } => self.inner.tickets.led_by(room_id, &spared),
            HoldRequest::ReleaseLock { .. }
            | HoldRequest::EnterLane { .. }
            | HoldRequest::AwaitLane { .. }
            | HoldRequest::LeaveLane { .. }
            | HoldRequest::TakeTicket { .. }
            | HoldRequest::ReleaseTicket { .. } => false,
        }
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
        self.waits.push((id, Box::pin(caught_up)));
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
            ..
        } = self;
        poll_fn(|cx| {
            if let Some(caller) = caller.as_deref_mut()
                && caller.poll_gone(cx).is_ready()
            {
                return Poll::Ready(Some(Event::CallerGone));
            }
            let ready = waits.iter_mut().enumerate().find_map(|(index, (_, wait))| {
                match wait.as_mut().poll(cx) {
                    Poll::Ready(waited) => Some((index, waited)),
                    Poll::Pending => None,
                }
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
        })
        .await
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
        if let Some(caller) = self.caller.as_deref() {
            // A write that ends once the close cut the work: the run is
            // aborted before what its outcome leads to starts.
            let cut = self.inner.tasks.cuts(self.marks.innermost());
            caller.went_on(effect, cut);
        }
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
        } = adoption;
        match operation.adopt(entry, origin) {
            Ok(id) => {
                self.callers.insert(id, reply);
                self.deliveries.insert(id, delivered);
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
    fn finish<T>(mut self, output: T) -> Result<T, KitError> {
        match self.refused.take() {
            Some(room_id) => Err(KitError::Reentrant {
                room_id,
                call: self.call,
            }),
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
