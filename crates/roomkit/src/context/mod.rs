//! The execution context of the calls the driver makes for an operation
//! (RFC §13.5: "the room lock MUST be reentrant within one execution
//! context (a task and the tasks it awaits as its own)").
//!
//! Every hook and channel call the driver spawns for an operation runs in a
//! tokio task-local context: a stack of frames, the innermost last, each
//! naming a run of the driver (what it holds now, read live, and the queue
//! that hands it calls into its room), the effect whose call this is, when
//! the operation made the request under a room's lock (`Effect::locked`),
//! a lease on that lock's tenure, the room in whose admission order the
//! request runs (`Effect::admitted`), the room a delivery set delivers in,
//! for its `on_event()` and `deliver()` calls, and, for a turn, the depth
//! of its response. A call made inside a call keeps the outer frames: a hook
//! of room S run inside a hook of room R's pass holds R's lease too.
//!
//! The driver reads the context of the task that runs it:
//!
//! - a lock a frame holds a live lease on is granted at once to a separate
//!   operation (counted in the run that asks, released with the lease
//!   holder's release only), and refused to an inbound call;
//! - a ticket's turn in the admission order of a room a frame stands in is
//!   never waited for: the frame's run holds the room's ticket, or is about
//!   to take it, and waits for this call (RFC §9.5.1 Reentrance); nor is
//!   that room's lock given to an inbound call, which would commit ahead of
//!   the event the frame works for, outside every order (gap G-070,
//!   decision 4);
//! - a call into a room is handed to the innermost frame that runs under
//!   the room's lock or in its admission order, whose run's operation adopts
//!   it or declines it;
//! - an inbound call into a room its context works in, under the room's
//!   lock, in its admission order or delivering in it, returns at its
//!   decision rather than wait for its delivery, which may wait in the
//!   room's lane behind its own caller's set (RFC §10.1 step 18);
//! - a call made while a turn runs, into whatever room, is at least as deep
//!   as the turn's response (RFC §10.6), so that an agent that calls into
//!   its room on every turn stops at the chain-depth limit (RFC §8.3);
//! - a call into the room a delivery set delivers in, made while the set's
//!   call runs, draws on the budget of the cascade that delivers; a call
//!   into another room has a budget of its own (gap G-070);
//! - the body of an ASYNC hook a cascade fires once an event's delivery set
//!   has run is that cascade's work while it runs (a [`HookBody`]): a call
//!   it makes into the room draws one of the cascade's hook calls, and one
//!   into any room is at least one deeper than the hook's event (gap
//!   G-087);
//! - a direct change of a stored event is made in the room's work of the
//!   innermost call that works in the room, under its lock, in its
//!   admission order or delivering in it, whose cascade's budget the call
//!   carries; outside every such call, in the hook body's that works in
//!   the room. Only a hook body's draws a hook call (RFC §10.3, gap
//!   G-087).
//!
//! A refusal fails the call ([`KitError::Reentrant`](crate::KitError::Reentrant))
//! rather than let it wait for itself.
//!
//! A lease is live while the run that holds the lock keeps the tenure it
//! was given under, so a context kept past the release grants nothing. A
//! frame lends its lease, stands in an admission order, lends its turn's
//! depth and lends its cascade's budget while its call runs, so a context kept past the call's
//! end, by a run the call started that goes on detached or a task it left
//! running, stands in none and lends nothing: that work starts fresh, at
//! depth 0 and on a budget of its own.
//!
//! What a run holds, its lock tenures and the leases on them, is `run`'s;
//! the hand-off of a call into a room to the run that adopts it is
//! `adoption`'s.
//!
//! tokio hands no task-local to a spawned task. A call that spawns a task
//! wraps the task's future in [`keep_context`], so that it runs within the
//! call's context and acts for the call while the call runs, awaited or not
//! (decision 2 of step 3), its turn's host given too
//! ([`current_turn`](crate::current_turn)); a task spawned without it has
//! none, and waits for the room as any task does. A frame lends its lease on a lock only
//! while its call runs, as it stands in an admission order: once a locked
//! hook is over, a task it left running holds nothing of the lock its run
//! still holds, and its calls are new arrivals (RFC §9.5.1).

mod adoption;
mod run;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use roomkit_core::model::{ChannelCategory, RoomId};
use roomkit_core::ops::{ChannelRequest, Effect, EffectId, Request};
use roomkit_core::rules::chain_depth::{self, Draw, ReentryBudget};

pub(crate) use self::adoption::{Adopting, Adoption, Completion, Reply, adopt, adopt_by_own_run};
use self::run::Lease;
pub(crate) use self::run::{LeftOpen, RunShared, Written};
use crate::tasks::Marks;

tokio::task_local! {
    static CONTEXT: ExecutionContext;
}

/// One frame of a context: a run, and the effect whose call this is.
#[derive(Clone, Debug)]
pub(crate) struct Frame {
    run: Arc<RunShared>,
    effect: EffectId,
    lease: Option<Lease>,
    work: Work,
}

/// What a frame records of the request whose call it is.
#[derive(Clone, Debug, Default)]
pub(crate) struct Work {
    /// The room whose lock the operation holds on the call's behalf
    /// (`Effect::locked`).
    pub(crate) locked: Option<RoomId>,
    /// The room in whose admission order the call runs (`Effect::admitted`,
    /// RFC §9.5.1).
    pub(crate) admitted: Option<RoomId>,
    /// The room a delivery set delivers in, for its `on_event()` and
    /// `deliver()` calls (RFC §10.2).
    pub(crate) delivering: Option<RoomId>,
    /// The depth of a turn's response, for a turn: an intelligence
    /// channel's `on_event()` (RFC §8.3, §10.6).
    pub(crate) response_depth: Option<u32>,
    /// The reentry budget of the cascade whose work the call is
    /// (`Effect::budget`): the one that delivers, for a delivery set's
    /// calls (gap G-070); the one its section works for, for a hook, a
    /// parse or a route under a lock or in an admission order (gap G-087).
    pub(crate) budget: Option<ReentryBudget>,
}

/// The frames of the calls the current task runs within, the innermost
/// last, the body of the ASYNC hook it runs within, outside them all, and
/// the depth its calls keep whatever runs (`floor`): the depth a tool
/// call's observers read as the call's trigger fired (RFC §8.3).
#[derive(Clone, Debug, Default)]
pub(crate) struct ExecutionContext {
    frames: Arc<[Frame]>,
    body: Option<HookBody>,
    floor: Option<u32>,
}

/// The body of an ASYNC hook a cascade fires once an event's delivery set
/// has run, or a direct change does, or of an ASYNC hook of a SYNC trigger,
/// while it runs: the cascade's work in its room (RFC §8.3, §9.2, §9.5.1,
/// §10.1 step 16, §10.3, gap G-087). A call it makes into the room draws on
/// the cascade's hook calls, and one it makes into any room is at least
/// `floor` deep. Once the body is over, a task it left running stands for
/// no work: its calls start fresh.
#[derive(Clone, Debug)]
pub(crate) struct HookBody {
    room_id: RoomId,
    /// The cascade's budget, whose hook calls the body's calls draw.
    budget: ReentryBudget,
    floor: u32,
    /// Whether a call the body makes into the room returns at its decision
    /// (RFC §10.1 step 18): an `AFTER_BROADCAST` body's, or a change's, does,
    /// as the Python implementation's does; an observer of a SYNC trigger's
    /// waits for its delivery, as Python's does, nothing waiting for it.
    detaches: bool,
    /// Whether the body still runs.
    live: Arc<AtomicBool>,
}

/// A hook's body that runs, over once dropped.
#[derive(Debug)]
pub(crate) struct BodyRunning(Arc<AtomicBool>);

impl Drop for BodyRunning {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

impl HookBody {
    /// The body of a hook of `room_id`'s cascade, on `budget`, its calls at
    /// least `floor` deep, running until it is [`over`](Self::over); a call
    /// it makes into the room returns at its decision when it `detaches`.
    pub(crate) fn new(room_id: RoomId, budget: ReentryBudget, floor: u32, detaches: bool) -> Self {
        Self {
            room_id,
            budget,
            floor,
            detaches,
            live: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Marks the body over when the returned guard drops, as the body
    /// ends or is cancelled: a copy of its context stands for no work any
    /// more.
    pub(crate) fn running(&self) -> BodyRunning {
        BodyRunning(Arc::clone(&self.live))
    }

    fn runs(&self) -> bool {
        self.live.load(Ordering::Relaxed)
    }
}

impl Work {
    /// What the frame of `effect`'s call records.
    pub(crate) fn of(effect: &Effect) -> Self {
        let (delivering, response_depth) = match &effect.request {
            Request::Channel(ChannelRequest::OnEvent { binding, event, .. }) => {
                let turn = binding.category == ChannelCategory::Intelligence;
                let depth = turn.then(|| chain_depth::response_depth(event));
                (Some(binding.room_id.clone()), depth)
            }
            Request::Channel(ChannelRequest::Deliver { binding, .. }) => {
                (Some(binding.room_id.clone()), None)
            }
            // A turn's stream is read as the turn's work: it delivers in the
            // room, its tools' calls at its response's depth.
            Request::Channel(ChannelRequest::ReadStream { answers, .. }) => (
                Some(answers.room_id.clone()),
                Some(chain_depth::response_depth(answers)),
            ),
            Request::Channel(ChannelRequest::HandleInbound { .. })
            | Request::Hold(_)
            | Request::Store(_)
            | Request::Hook(_)
            | Request::Route { .. } => (None, None),
        };
        Self {
            locked: effect.locked.clone(),
            admitted: effect.admitted.clone(),
            delivering,
            response_depth,
            budget: effect.budget.clone(),
        }
    }
}

impl Frame {
    /// The frame of the call for `effect`, made by `run`, recording `work`;
    /// a call under a lock the run holds takes a lease on its tenure.
    pub(crate) fn new(run: &Arc<RunShared>, effect: EffectId, work: Work) -> Self {
        Self {
            run: Arc::clone(run),
            effect,
            lease: work
                .locked
                .as_ref()
                .and_then(|room_id| run.lease_of(room_id)),
            work,
        }
    }

    /// Whether the frame's call runs under `room_id`'s lock or in its
    /// admission order: a call it makes into the room is its run's to
    /// adopt (RFC §9.5.1 Reentrance).
    fn adopts_into(&self, room_id: &RoomId) -> bool {
        self.lease_on(room_id).is_some() || self.is_admitted_to(room_id)
    }

    /// The frame's lease on `room_id`'s lock while its call runs: a frame
    /// kept past its call's end, by a task the call spawned and left
    /// running, holds the room's lock for nothing it does (RFC §9.5.1: once
    /// a hook's body is over, the work it began calls as a new arrival).
    fn lease_on(&self, room_id: &RoomId) -> Option<&Lease> {
        self.lease
            .as_ref()
            .filter(|lease| lease.room_id() == room_id && self.runs())
    }

    /// Whether the frame's call stands in `room_id`'s admission order: it
    /// was made there (`Effect::admitted`) and still runs, its run's
    /// operation not answered it yet, as a lease lives while its tenure
    /// does. A frame kept past its call's end, by a run the call started
    /// that goes on detached, stands in no order: the ticket its run held,
    /// or was about to take, waits for nothing that run does any more.
    fn is_admitted_to(&self, room_id: &RoomId) -> bool {
        self.work.admitted.as_ref() == Some(room_id) && self.runs()
    }

    /// Whether the frame's call still runs, its run's operation not answered
    /// it yet: a frame kept past its call's end stands for no work.
    fn runs(&self) -> bool {
        self.run.is_calling(self.effect)
    }

    /// Whether the frame's call is work of `room_id` that still runs: under
    /// a live lease on the room's lock, in its admission order, or
    /// delivering in it.
    fn works_in(&self, room_id: &RoomId) -> bool {
        let leased = self
            .lease
            .as_ref()
            .is_some_and(|lease| lease.room_id() == room_id && lease.is_live());
        let delivering = self.work.delivering.as_ref() == Some(room_id);
        self.runs() && (leased || delivering || self.work.admitted.as_ref() == Some(room_id))
    }
}

impl ExecutionContext {
    /// The context of the current task, empty outside every call the
    /// driver makes.
    pub(crate) fn current() -> Self {
        CONTEXT.try_with(Clone::clone).unwrap_or_default()
    }

    /// The context of a call made within this one, as `frame`.
    pub(crate) fn within(&self, frame: Frame) -> Self {
        let frames: Vec<Frame> = self.frames.iter().cloned().chain([frame]).collect();
        Self {
            frames: frames.into(),
            body: self.body.clone(),
            floor: self.floor,
        }
    }

    /// The same context, whose calls are at least `floor` deep for as long
    /// as it lasts, its frames' turns over or not: the context of a tool
    /// call's observers, which keep the depth of the turn that made the call
    /// (RFC §8.3, "The hook calls of a cascade").
    pub(crate) fn at_least(mut self, floor: u32) -> Self {
        self.floor = Some(self.floor.map_or(floor, |kept| kept.max(floor)));
        self
    }

    /// The context of a hook's `body`, which runs outside every call.
    pub(crate) fn of_hook_body(body: HookBody) -> Self {
        Self {
            frames: Arc::default(),
            body: Some(body),
            floor: None,
        }
    }

    /// The hook body this context runs within, while it runs.
    fn running_body(&self) -> Option<&HookBody> {
        self.body.as_ref().filter(|body| body.runs())
    }

    /// The hook body this context runs within, while it runs and works in
    /// `room_id`.
    fn body_in(&self, room_id: &RoomId) -> Option<&HookBody> {
        self.running_body().filter(|body| &body.room_id == room_id)
    }

    /// Runs `future` within this context.
    pub(crate) async fn scope<F: Future>(self, future: F) -> F::Output {
        CONTEXT.scope(self, future).await
    }

    /// A live lease on `room_id`'s lock, which a call of this context runs
    /// under and still runs.
    pub(crate) fn lease(&self, room_id: &RoomId) -> Option<Lease> {
        self.frames
            .iter()
            .rev()
            .filter_map(|frame| frame.lease_on(room_id))
            .find(|lease| lease.is_live())
            .cloned()
    }

    /// Whether a frame of this context stands in `room_id`'s admission
    /// order (RFC §9.5.1): its run holds the room's ticket, or takes one
    /// once the frame's call is over, so a turn behind it is never waited
    /// for.
    pub(crate) fn is_admitted_to(&self, room_id: &RoomId) -> bool {
        self.frames
            .iter()
            .any(|frame| frame.is_admitted_to(room_id))
    }

    /// Whether an inbound call of this context into `room_id` returns at
    /// its decision: a frame whose call still runs delivers in the room or
    /// stands in its admission order, or the context holds a live lease on
    /// its lock, and its run's set or section would wait for this call (RFC
    /// §10.1 step 18). A frame kept past its call's end, as a run started
    /// from a delivery carries it, stands for no work.
    pub(crate) fn detaches(&self, room_id: &RoomId) -> bool {
        self.lease(room_id).is_some()
            || self.frames.iter().any(|frame| {
                (frame.runs() && frame.work.delivering.as_ref() == Some(room_id))
                    || frame.is_admitted_to(room_id)
            })
            || self.body_in(room_id).is_some_and(|body| body.detaches)
    }

    /// Whether a wait in this context on a delivery in `room_id` would wait
    /// for itself (RFC §10.1 step 18): it holds a live lease on the room's
    /// lock, a frame whose call still runs delivers in the room, or it runs
    /// a hook body there whose calls return at their decision. Admission
    /// work does not: what it adopts delivers ahead of its event (RFC
    /// §9.5.1).
    pub(crate) fn waits_for_itself(&self, room_id: &RoomId) -> bool {
        self.lease(room_id).is_some()
            || self
                .frames
                .iter()
                .any(|frame| frame.runs() && frame.work.delivering.as_ref() == Some(room_id))
            || self.body_in(room_id).is_some_and(|body| body.detaches)
    }

    /// The least chain depth of a call made in this context, into whatever
    /// room: the response depth of the deepest turn it runs within that
    /// still runs, the floor of the hook body it runs within while that
    /// body runs, one deeper than the hook's event, or the floor the context
    /// keeps (a tool call's observers'), whichever is deepest; `None`
    /// outside every running turn and body, with no floor kept (RFC §10.6: a
    /// call a turn makes is at least as deep as the turn's response; RFC
    /// §8.3; gaps G-070, G-087).
    pub(crate) fn turn_depth(&self) -> Option<u32> {
        let turns = self
            .frames
            .iter()
            .filter(|frame| frame.runs())
            .filter_map(|frame| frame.work.response_depth);
        let body = self.running_body().map(|body| body.floor);
        turns.chain(body).chain(self.floor).max()
    }

    /// What a call into `room_id` made in this context draws, and on which
    /// cascade's budget: a pass of the cascade whose delivery set runs the
    /// innermost running frame of a delivery in that room (gap G-070); else,
    /// while it runs, a hook call of the cascade whose hook body this
    /// context runs within, when that body works in the room (gap G-087);
    /// `None` otherwise, the call on a budget of its own.
    pub(crate) fn budget(&self, room_id: &RoomId) -> Option<Draw> {
        self.frames
            .iter()
            .rev()
            .filter(|frame| frame.runs() && frame.work.delivering.as_ref() == Some(room_id))
            .find_map(|frame| frame.work.budget.clone().map(Draw::Pass))
            .or_else(|| {
                self.body_in(room_id)
                    .map(|body| Draw::HookCall(body.budget.clone()))
            })
    }

    /// The room the innermost running frame of a delivery delivers in, and
    /// what a message the router places draws on its cascade's budget once
    /// routed into that room: a pass; else the room of the running hook
    /// body this context runs within, and one of its cascade's hook calls
    /// (gaps G-070, G-087).
    pub(crate) fn delivery(&self) -> Option<(RoomId, Draw)> {
        self.frames
            .iter()
            .rev()
            .filter(|frame| frame.runs())
            .find_map(|frame| {
                let budget = frame.work.budget.clone()?;
                Some((frame.work.delivering.clone()?, Draw::Pass(budget)))
            })
            .or_else(|| {
                self.running_body()
                    .map(|body| (body.room_id.clone(), Draw::HookCall(body.budget.clone())))
            })
    }

    /// The room's work a direct change of a stored event in `room_id`, made
    /// in this context, is made in (RFC §10.3 "A direct change from the
    /// room's work", gap G-087), and the least depth of the calls its
    /// triggers' bodies make ([`turn_depth`](Self::turn_depth)): the
    /// innermost running frame that works in the room, under its lock, in
    /// its admission order or delivering in it, whatever kind it is, which
    /// draws nothing on its cascade's budget ([`Draw::Pass`]); else the
    /// running hook body that works in the room, which draws one of its
    /// cascade's hook calls ([`Draw::HookCall`]). `None` outside the room's
    /// work, and for a frame that carries no budget: the change then runs
    /// on a budget of its own.
    pub(crate) fn room_work(&self, room_id: &RoomId) -> Option<(Draw, u32)> {
        let draw = match self
            .frames
            .iter()
            .rev()
            .find(|frame| frame.works_in(room_id))
        {
            Some(frame) => Draw::Pass(frame.work.budget.clone()?),
            None => Draw::HookCall(self.body_in(room_id)?.budget.clone()),
        };
        Some((draw, self.turn_depth().unwrap_or(0)))
    }

    /// Whether this context is work of `room_id` that still runs: a frame
    /// under a live lease on its lock, in its admission order or delivering
    /// in it, or a hook body that works in it.
    pub(crate) fn works_in_room(&self, room_id: &RoomId) -> bool {
        self.frames.iter().any(|frame| frame.works_in(room_id)) || self.body_in(room_id).is_some()
    }

    /// The innermost frame that runs under `room_id`'s lock or in its
    /// admission order, whose run a call into the room is handed to.
    fn taker(&self, room_id: &RoomId) -> Option<&Frame> {
        self.frames
            .iter()
            .rev()
            .find(|frame| frame.adopts_into(room_id))
    }
}

/// Raises `chain_depth`, the depth of a call the current task makes into a
/// room, to the response depth of the deepest turn the task runs within
/// (RFC §10.6: a call a turn makes is at least as deep as the turn's
/// response), so that an agent calling into its room on every turn stops at
/// the chain-depth limit, as its responses do (RFC §8.3).
pub(crate) fn as_deep_as_its_turn(chain_depth: &mut u32) {
    if let Some(depth) = ExecutionContext::current().turn_depth() {
        *chain_depth = (*chain_depth).max(depth);
    }
}

/// Runs `future` within the execution context of the current task, for a
/// task the current call spawns (RFC §13.5, §9.5.1; decision 2 of step 3).
///
/// tokio hands a spawned task none of its spawner's context: a hook or a
/// turn that runs part of its work in a task it spawns wraps that task's
/// future with this, so that the task acts for the call while the call
/// runs, whether the call awaits it or not, as a task spawned in a Python
/// hook does: its calls into the room take the lock again and are adopted
/// as the hook's or the turn's own would be. Once the call is over, the
/// task acts for it no more: its calls are new arrivals, which wait for
/// the room, its lock included, as any task's, on a budget and at a depth
/// of their own. They wait even when a later hook of the same pass awaits
/// the task: that hook waits until its own timeout, the pass holding the
/// lock the task's call waits for (decided in step 3 milestone 2). A task
/// spawned without it acts for nothing from its start.
///
/// It is the Rust form of the Python implementation's `create_task`,
/// which copies its caller's context. A task spawned in the kit's work, a
/// delivery, a turn or a hook, that calls the kit and waits on its own
/// room must be wrapped: unwrapped, its call is a stranger's, and its wait,
/// on the call or on the call's [`DeliveryHandle`](crate::DeliveryHandle),
/// blocks behind the very delivery that awaits the task, whatever the
/// kit's `process_timeout`. The task is the kit's work its spawner runs
/// within, too: a [`RoomKit::close`](crate::RoomKit::close) it calls
/// neither waits for that work nor cuts it (RFC §12.10.4).
///
/// ```no_run
/// # use roomkit::model::{ChannelId, EventContent, RoomId};
/// # async fn turn(kit: roomkit::RoomKit, room: RoomId, agent: ChannelId) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// // Inside an agent's turn: the tool runs in a task of its own, awaited.
/// let tool = tokio::spawn(roomkit::keep_context(async move {
///     let note = EventContent::text("Looking it up.");
///     kit.send_event(&room, &agent, note, Default::default()).await
/// }));
/// let injected = tool.await??;
/// # Ok(())
/// # }
/// ```
pub fn keep_context<F>(future: F) -> impl Future<Output = F::Output> + Send + 'static
where
    F: Future + Send + 'static,
{
    CONTEXT.scope(
        ExecutionContext::current(),
        Marks::current().scoped(crate::turn::within_current_turn(future)),
    )
}

#[cfg(test)]
mod tests {
    use roomkit_core::model::{
        ChannelDirection, ChannelId, ChannelType, EventContent, EventDirection, EventSource,
        EventType, Room, RoomContext, RoomEvent, Timestamp,
    };
    use roomkit_core::ops::{
        ChannelDeclaration, CheckTimers, KitConfig, Next, Operation, SequentialEnvironment,
    };

    use super::*;

    fn room_id() -> RoomId {
        RoomId::new("room_1").unwrap()
    }

    /// An effect id, the first an operation issues: any id serves here.
    fn effect() -> EffectId {
        let mut operation = CheckTimers::new(Arc::new(KitConfig::new()), room_id(), None);
        match operation.poll(&SequentialEnvironment::default()).next {
            Next::Effects(effects) => effects.first().unwrap().id,
            Next::Waiting | Next::Ready(_) => panic!("a timer check asks for the lock first"),
        }
    }

    #[test]
    fn a_lease_lives_while_its_tenure_is_held() {
        let (run, _queue) = RunShared::new();
        run.locked(room_id());
        run.calling(effect());
        let context = ExecutionContext::default().within(Frame::new(
            &run,
            effect(),
            Work {
                locked: Some(room_id()),
                ..Work::default()
            },
        ));
        assert!(context.lease(&room_id()).is_some());
    }

    /// RFC §13.5: a context kept past the release grants nothing.
    #[test]
    fn a_lease_dies_with_the_release() {
        let (run, _queue) = RunShared::new();
        run.locked(room_id());
        let context = ExecutionContext::default().within(Frame::new(
            &run,
            effect(),
            Work {
                locked: Some(room_id()),
                ..Work::default()
            },
        ));
        run.released(&room_id());
        assert!(context.lease(&room_id()).is_none());
    }

    /// A lease on one tenure is not a lease on the next one, the same run's.
    #[test]
    fn a_lease_dies_with_its_tenure_even_when_the_lock_is_taken_again() {
        let (run, _queue) = RunShared::new();
        run.locked(room_id());
        let context = ExecutionContext::default().within(Frame::new(
            &run,
            effect(),
            Work {
                locked: Some(room_id()),
                ..Work::default()
            },
        ));
        run.released(&room_id());
        run.locked(room_id());
        assert!(context.lease(&room_id()).is_none());
    }

    #[test]
    fn a_call_made_outside_the_lock_holds_no_lease() {
        let (run, _queue) = RunShared::new();
        run.locked(room_id());
        let context =
            ExecutionContext::default().within(Frame::new(&run, effect(), Work::default()));
        assert!(context.lease(&room_id()).is_none());
    }

    /// RFC §9.5.1 Reentrance: code run in a room's admission order stands
    /// in it, and in no other room's.
    #[test]
    fn a_call_made_in_a_room_s_admission_order_stands_in_it() {
        let (run, _queue) = RunShared::new();
        run.calling(effect());
        let context = ExecutionContext::default().within(Frame::new(
            &run,
            effect(),
            Work {
                admitted: Some(room_id()),
                ..Work::default()
            },
        ));
        let other = RoomId::new("room_2").unwrap();
        assert_eq!(
            (
                context.is_admitted_to(&room_id()),
                context.is_admitted_to(&other)
            ),
            (true, false)
        );
    }

    /// Gap G-070: a frame kept past its call's end, by a run the call
    /// started that goes on detached, stands in no admission order: it
    /// takes no call, refuses no ticket's turn and detaches no call.
    #[test]
    fn a_call_over_stands_in_no_admission_order() {
        let (run, _queue) = RunShared::new();
        run.calling(effect());
        let context = ExecutionContext::default().within(Frame::new(
            &run,
            effect(),
            Work {
                admitted: Some(room_id()),
                ..Work::default()
            },
        ));
        run.answered(effect());
        assert_eq!(
            (
                context.is_admitted_to(&room_id()),
                context.taker(&room_id()).is_some(),
                context.detaches(&room_id())
            ),
            (false, false, false)
        );
    }

    /// RFC §9.5.1, decision 2 of step 3: a frame kept past its call's end,
    /// by a task a locked hook spawned and left running, holds no lease on
    /// the lock its run still holds: it is granted nothing, takes no call
    /// and detaches none, and the task's calls are new arrivals.
    #[test]
    fn a_call_over_holds_no_lease_on_the_lock_its_run_still_holds() {
        let (run, _queue) = RunShared::new();
        run.locked(room_id());
        run.calling(effect());
        let context = ExecutionContext::default().within(Frame::new(
            &run,
            effect(),
            Work {
                locked: Some(room_id()),
                ..Work::default()
            },
        ));
        let running = (
            context.lease(&room_id()).is_some(),
            context.taker(&room_id()).is_some(),
            context.detaches(&room_id()),
        );
        run.answered(effect());
        let over = (
            context.lease(&room_id()).is_some(),
            context.taker(&room_id()).is_some(),
            context.detaches(&room_id()),
        );
        assert_eq!((running, over), ((true, true, true), (false, false, false)));
    }

    #[test]
    fn a_call_is_handed_to_the_innermost_frame_under_its_room_s_lock_or_in_its_order() {
        let (outer, _outer_queue) = RunShared::new();
        let (inner, _inner_queue) = RunShared::new();
        outer.locked(room_id());
        outer.calling(effect());
        inner.calling(effect());
        let other = RoomId::new("room_2").unwrap();
        let context = ExecutionContext::default()
            .within(Frame::new(
                &outer,
                effect(),
                Work {
                    locked: Some(room_id()),
                    ..Work::default()
                },
            ))
            .within(Frame::new(
                &inner,
                effect(),
                Work {
                    admitted: Some(other.clone()),
                    ..Work::default()
                },
            ));
        let taker = |room_id: &RoomId| {
            context
                .taker(room_id)
                .map(|frame| Arc::ptr_eq(&frame.run, &outer))
        };
        assert_eq!(
            (
                taker(&room_id()),
                taker(&other),
                taker(&RoomId::new("room_3").unwrap())
            ),
            (Some(true), Some(false), None)
        );
    }

    /// The effect of `on_event()` to a channel of `category`, for an event
    /// at `depth`.
    fn on_event(category: ChannelCategory, depth: u32) -> Effect {
        let channel = ChannelId::new("ai").unwrap();
        let declaration = ChannelDeclaration::new(
            channel.clone(),
            ChannelType::AI,
            category,
            ChannelDirection::Bidirectional,
        );
        let source = EventSource::new(channel, ChannelType::AI, EventDirection::Inbound);
        let mut event = RoomEvent::new(
            "evt_1".parse().unwrap(),
            room_id(),
            EventType::Message,
            source,
            EventContent::text("Hi"),
            Timestamp::UNIX_EPOCH,
        );
        event.chain_depth = depth;
        Effect {
            id: effect(),
            request: ChannelRequest::OnEvent {
                binding: declaration.binding(room_id(), Timestamp::UNIX_EPOCH),
                event,
                context: Box::new(RoomContext::new(Room::new(
                    room_id(),
                    Timestamp::UNIX_EPOCH,
                ))),
            }
            .into(),
            deadline: None,
            locked: None,
            admitted: None,
            budget: None,
            decides: None,
        }
    }

    /// RFC §10.6, §8.3: a turn's frame gives the calls made within it the
    /// depth of the turn's response while the turn runs, and none once it
    /// is over (gap G-070); a transport told of the event gives none.
    #[test]
    fn a_turn_gives_the_calls_made_within_it_its_response_depth() {
        let (run, _queue) = RunShared::new();
        run.calling(effect());
        let depth_within = |category| {
            let work = Work::of(&on_event(category, 2));
            ExecutionContext::default()
                .within(Frame::new(&run, effect(), work))
                .turn_depth()
        };
        let running = (
            depth_within(ChannelCategory::Intelligence),
            depth_within(ChannelCategory::Transport),
        );
        run.answered(effect());
        assert_eq!(
            (running, depth_within(ChannelCategory::Intelligence)),
            ((Some(3), None), None)
        );
    }

    /// RFC §10.1 step 18: a turn's call into its own room returns at its
    /// decision, and is not adopted; one into another room waits for its
    /// delivery, and so does one once the delivery's call is over.
    #[test]
    fn a_context_delivering_in_a_room_detaches_from_that_room_only() {
        let (run, _queue) = RunShared::new();
        run.calling(effect());
        let context = ExecutionContext::default().within(Frame::new(
            &run,
            effect(),
            Work {
                delivering: Some(room_id()),
                ..Work::default()
            },
        ));
        let delivering = (
            context.detaches(&room_id()),
            context.detaches(&RoomId::new("room_2").unwrap()),
            context.taker(&room_id()).is_some(),
        );
        run.answered(effect());
        assert_eq!(
            (delivering, context.detaches(&room_id())),
            ((true, false, false), false)
        );
    }
}
