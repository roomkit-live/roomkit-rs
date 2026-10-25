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
//!   G-087).
//!
//! A refusal fails the call ([`KitError::Reentrant`](crate::KitError::Reentrant))
//! rather than let it wait for itself.
//!
//! A lease is live while the run that holds the lock keeps the tenure it
//! was given under, so a context kept past the release grants nothing. A
//! frame stands in an admission order, lends its turn's depth and lends its
//! cascade's budget while its call runs, so a context kept past the call's
//! end, by a run the call started that goes on detached or a task it left
//! running, stands in none and lends nothing: that work starts fresh, at
//! depth 0 and on a budget of its own.
//!
//! What a run holds, its lock tenures and the leases on them, is `run`'s;
//! the hand-off of a call into a room to the run that adopts it is
//! `adoption`'s.
//!
//! tokio hands no task-local to a spawned task. A call that spawns a task
//! and awaits it as its own (RFC §13.5: "the tasks it awaits as its own")
//! wraps the task's future in [`keep_context`], so that it runs within the
//! call's context; a task spawned without it, awaited or not, has none, and
//! waits for the room as any task does.

mod adoption;
mod run;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use roomkit_core::model::{ChannelCategory, RoomId};
use roomkit_core::ops::{ChannelRequest, Effect, EffectId, Request};
use roomkit_core::rules::chain_depth::{self, ReentryBudget};

pub(crate) use self::adoption::{Adopting, Adoption, Reply, adopt};
use self::run::Lease;
pub(crate) use self::run::RunShared;

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
    /// The reentry budget of the cascade that delivers, for a delivery
    /// set's calls (`Effect::budget`, gap G-070).
    pub(crate) budget: Option<ReentryBudget>,
}

/// The frames of the calls the current task runs within, the innermost
/// last, and the bodies of the ASYNC hooks it runs within.
#[derive(Clone, Debug, Default)]
pub(crate) struct ExecutionContext {
    frames: Arc<[Frame]>,
    bodies: Arc<[HookBody]>,
}

/// The body of an ASYNC hook a cascade fires once an event's delivery set
/// has run, or a direct change does, while it runs: the cascade's work in
/// its room (RFC §8.3, §9.5.1, §10.1 step 16, §10.3, gap G-087). A call it
/// makes into the room draws on the cascade's hook calls, and one it makes
/// into any room is at least `floor` deep. Once the body is over, a task it
/// left running stands for no work: its calls start fresh.
#[derive(Clone, Debug)]
pub(crate) struct HookBody {
    room_id: RoomId,
    /// The cascade's budget, seen for its hooks' calls.
    budget: ReentryBudget,
    floor: u32,
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
    /// least `floor` deep, running until it is [`over`](Self::over).
    pub(crate) fn new(room_id: RoomId, budget: ReentryBudget, floor: u32) -> Self {
        Self {
            room_id,
            budget,
            floor,
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
        self.lease.as_ref().map(Lease::room_id) == Some(room_id) || self.is_admitted_to(room_id)
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
            bodies: Arc::clone(&self.bodies),
        }
    }

    /// The context of a hook's `body`, which runs outside every call.
    pub(crate) fn of_hook_body(body: HookBody) -> Self {
        Self {
            frames: Arc::default(),
            bodies: Arc::new([body]),
        }
    }

    /// The running hook bodies of this context that work in `room_id`, the
    /// innermost last.
    fn bodies_in<'context>(
        &'context self,
        room_id: &'context RoomId,
    ) -> impl DoubleEndedIterator<Item = &'context HookBody> + 'context {
        self.bodies
            .iter()
            .filter(move |body| body.runs() && &body.room_id == room_id)
    }

    /// Runs `future` within this context.
    pub(crate) async fn scope<F: Future>(self, future: F) -> F::Output {
        CONTEXT.scope(self, future).await
    }

    /// A live lease on `room_id`'s lock, which a call of this context runs
    /// under.
    pub(crate) fn lease(&self, room_id: &RoomId) -> Option<Lease> {
        self.frames
            .iter()
            .rev()
            .filter_map(|frame| frame.lease.as_ref())
            .find(|lease| lease.room_id() == room_id && lease.is_live())
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
    /// its decision: a frame delivers in the room, stands in its admission
    /// order, or holds a live lease on its lock, and its run's set or
    /// section would wait for this call (RFC §10.1 step 18).
    pub(crate) fn detaches(&self, room_id: &RoomId) -> bool {
        self.lease(room_id).is_some()
            || self.frames.iter().any(|frame| {
                frame.work.delivering.as_ref() == Some(room_id) || frame.is_admitted_to(room_id)
            })
            || self.bodies_in(room_id).next().is_some()
    }

    /// The least chain depth of a call made in this context, into whatever
    /// room: the response depth of the deepest turn it runs within that
    /// still runs, `None` outside every running turn (RFC §10.6: a call a
    /// turn makes is at least as deep as the turn's response; gap G-070).
    pub(crate) fn turn_depth(&self) -> Option<u32> {
        let turns = self
            .frames
            .iter()
            .filter(|frame| frame.runs())
            .filter_map(|frame| frame.work.response_depth);
        let bodies = self
            .bodies
            .iter()
            .filter(|body| body.runs())
            .map(|body| body.floor);
        turns.chain(bodies).max()
    }

    /// The reentry budget a call into `room_id` made in this context draws
    /// on: the budget of the cascade whose delivery set runs the innermost
    /// running frame of a delivery in that room, `None` otherwise (gap
    /// G-070).
    pub(crate) fn budget(&self, room_id: &RoomId) -> Option<ReentryBudget> {
        self.frames
            .iter()
            .rev()
            .filter(|frame| frame.runs() && frame.work.delivering.as_ref() == Some(room_id))
            .find_map(|frame| frame.work.budget.clone())
            .or_else(|| {
                self.bodies_in(room_id)
                    .next_back()
                    .map(|body| body.budget.clone())
            })
    }

    /// The room the innermost running frame of a delivery delivers in, and
    /// the budget of its cascade: what a message the router places draws
    /// on once routed into that room (gap G-070).
    pub(crate) fn delivery(&self) -> Option<(RoomId, ReentryBudget)> {
        self.frames
            .iter()
            .rev()
            .filter(|frame| frame.runs())
            .find_map(|frame| Some((frame.work.delivering.clone()?, frame.work.budget.clone()?)))
            .or_else(|| {
                self.bodies
                    .iter()
                    .rev()
                    .find(|body| body.runs())
                    .map(|body| (body.room_id.clone(), body.budget.clone()))
            })
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
/// task the current call spawns and awaits as its own (RFC §13.5: the room
/// lock is reentrant within "a task and the tasks it awaits as its own").
///
/// tokio hands a spawned task none of its spawner's context: a hook or a
/// turn that runs part of its work in a task it spawns, then awaits, wraps
/// that task's future with this, so that the task's calls into the room
/// take the lock again and are adopted as the hook's or the turn's own
/// would be. A task the call does not await must not be wrapped: its calls
/// would run within a call that may be over; they take the lock for real,
/// and wait for the room, as any task's.
///
/// ```no_run
/// # async fn turn(kit: roomkit::RoomKit, room: roomkit::model::RoomId, event: roomkit::model::DirectEvent) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// // Inside an agent's turn: the tool runs in a task of its own, awaited.
/// let tool = tokio::spawn(roomkit::keep_context(async move {
///     kit.send_event(&room, event).await
/// }));
/// let injected = tool.await??;
/// # Ok(())
/// # }
/// ```
pub fn keep_context<F>(future: F) -> impl Future<Output = F::Output> + Send + 'static
where
    F: Future + Send + 'static,
{
    CONTEXT.scope(ExecutionContext::current(), future)
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
        let mut operation = CheckTimers::new(Arc::new(KitConfig::new()), room_id());
        match operation.poll(&SequentialEnvironment::default()).next {
            Next::Effects(effects) => effects.first().unwrap().id,
            Next::Waiting | Next::Ready(_) => panic!("a timer check asks for the lock first"),
        }
    }

    #[test]
    fn a_lease_lives_while_its_tenure_is_held() {
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

    #[test]
    fn a_call_is_handed_to_the_innermost_frame_under_its_room_s_lock_or_in_its_order() {
        let (outer, _outer_queue) = RunShared::new();
        let (inner, _inner_queue) = RunShared::new();
        outer.locked(room_id());
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
            ChannelType::Ai,
            category,
            ChannelDirection::Bidirectional,
        );
        let source = EventSource::new(channel, ChannelType::Ai, EventDirection::Inbound);
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
    /// delivery.
    #[test]
    fn a_context_delivering_in_a_room_detaches_from_that_room_only() {
        let (run, _queue) = RunShared::new();
        let context = ExecutionContext::default().within(Frame::new(
            &run,
            effect(),
            Work {
                delivering: Some(room_id()),
                ..Work::default()
            },
        ));
        assert_eq!(
            (
                context.detaches(&room_id()),
                context.detaches(&RoomId::new("room_2").unwrap()),
                context.taker(&room_id()).is_some()
            ),
            (true, false, false)
        );
    }
}
