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
//!   into another room has a budget of its own (gap G-070).
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
//! tokio hands no task-local to a spawned task. A call that spawns a task
//! and awaits it as its own (RFC §13.5: "the tasks it awaits as its own")
//! wraps the task's future in [`keep_context`], so that it runs within the
//! call's context; a task spawned without it, awaited or not, has none, and
//! waits for the room as any task does.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::{ChannelCategory, RoomId};
use roomkit_core::ops::{
    ChannelRequest, Effect, EffectId, Entry, InboundError, InboundResult, Request,
};
use roomkit_core::rules::chain_depth::{self, ReentryBudget};
use tokio::sync::{mpsc, oneshot};

tokio::task_local! {
    static CONTEXT: ExecutionContext;
}

/// What an adopted entry's caller receives (RFC §10.1 step 18).
pub(crate) type EntryResult = Result<InboundResult, InboundError>;

/// A call into a room, handed to the run whose work made it.
#[derive(Debug)]
pub(crate) struct Adoption {
    /// The call.
    pub(crate) entry: Entry,
    /// The effect whose call made it, in that run.
    pub(crate) origin: EffectId,
    /// Where its answer goes.
    pub(crate) reply: oneshot::Sender<Reply>,
}

/// The answer of the run a call into its room was handed to.
#[derive(Debug)]
pub(crate) enum Reply {
    /// The run's operation adopted the call and decided it.
    Settled(Box<EntryResult>),
    /// The run did not adopt the call: its origin was answered already, or
    /// the run is over. The call runs apart.
    Declined(Box<Entry>),
}

/// What a run holds now, as the calls it makes read it.
#[derive(Debug, Default)]
struct Holdings {
    locks: HashMap<RoomId, Tenure>,
    /// The effects whose calls run now: the operation has not been answered
    /// them yet.
    calls: HashSet<EffectId>,
}

/// How a run holds a room's lock.
#[derive(Clone, Debug)]
enum Tenure {
    /// The lock itself, its tenure numbered.
    Own(u64),
    /// Granted on a lease its context holds.
    Granted(Lease),
}

/// A run's side shared with the calls it makes: what it holds, and the
/// queue that hands it calls into its room.
#[derive(Debug)]
pub(crate) struct RunShared {
    holdings: Mutex<Holdings>,
    adoptions: mpsc::UnboundedSender<Adoption>,
    tenures: AtomicU64,
}

/// A claim on one tenure of a room's lock, which code run under that lock
/// holds.
#[derive(Clone, Debug)]
pub(crate) struct Lease {
    holder: Arc<RunShared>,
    room_id: RoomId,
    tenure: u64,
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
/// last.
#[derive(Clone, Debug, Default)]
pub(crate) struct ExecutionContext {
    frames: Arc<[Frame]>,
}

/// What became of a call handed to the run whose work made it.
#[derive(Debug)]
pub(crate) enum Adopting {
    /// No frame of the caller's context works in the call's room, or its
    /// run declined the call: the call runs apart, as any call does.
    Apart(Entry),
    /// The run's operation adopted the call and decided it.
    Settled(Box<EntryResult>),
    /// The run adopted the call, then was dropped before deciding it.
    Abandoned,
}

impl RunShared {
    /// A run's shared side, with the queue its driver receives calls on.
    pub(crate) fn new() -> (Arc<Self>, mpsc::UnboundedReceiver<Adoption>) {
        let (adoptions, received) = mpsc::unbounded_channel();
        let shared = Arc::new(Self {
            holdings: Mutex::new(Holdings::default()),
            adoptions,
            tenures: AtomicU64::new(0),
        });
        (shared, received)
    }

    fn holdings(&self) -> MutexGuard<'_, Holdings> {
        self.holdings.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records that the run took `room_id`'s lock itself.
    pub(crate) fn locked(&self, room_id: RoomId) {
        let tenure = self.tenures.fetch_add(1, Ordering::Relaxed);
        self.holdings().locks.insert(room_id, Tenure::Own(tenure));
    }

    /// Records that the run was granted `room_id`'s lock on `lease`.
    pub(crate) fn granted(&self, lease: Lease) {
        self.holdings()
            .locks
            .insert(lease.room_id.clone(), Tenure::Granted(lease));
    }

    /// Records that the run let `room_id`'s lock go: every lease on its
    /// tenure dies with it.
    pub(crate) fn released(&self, room_id: &RoomId) {
        self.holdings().locks.remove(room_id);
    }

    /// Records that the call for `effect` runs.
    pub(crate) fn calling(&self, effect: EffectId) {
        self.holdings().calls.insert(effect);
    }

    /// Records that the call for `effect` is over, its effect answered:
    /// its frame stands in no admission order any more.
    pub(crate) fn answered(&self, effect: EffectId) {
        self.holdings().calls.remove(&effect);
    }

    /// Whether the call for `effect` still runs.
    fn is_calling(&self, effect: EffectId) -> bool {
        self.holdings().calls.contains(&effect)
    }

    /// Forgets everything the run held: it is over.
    pub(crate) fn clear(&self) {
        *self.holdings() = Holdings::default();
    }

    /// The lease on the lock of `room_id` the run holds, by its own tenure
    /// or by the lease it was granted on.
    fn lease_of(self: &Arc<Self>, room_id: &RoomId) -> Option<Lease> {
        match self.holdings().locks.get(room_id)? {
            Tenure::Own(tenure) => Some(Lease {
                holder: Arc::clone(self),
                room_id: room_id.clone(),
                tenure: *tenure,
            }),
            Tenure::Granted(lease) => Some(lease.clone()),
        }
    }
}

impl Lease {
    /// The room whose lock the lease is on.
    pub(crate) fn room_id(&self) -> &RoomId {
        &self.room_id
    }

    /// Whether the holder still holds the lock under the lease's tenure.
    fn is_live(&self) -> bool {
        matches!(
            self.holder.holdings().locks.get(&self.room_id),
            Some(Tenure::Own(tenure)) if *tenure == self.tenure
        )
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

    /// Hands `entry` to the frame's run, and waits for its decision.
    async fn adopt(&self, entry: Entry) -> Adopting {
        let (reply, answer) = oneshot::channel();
        let adoption = Adoption {
            entry,
            origin: self.effect,
            reply,
        };
        if let Err(unsent) = self.run.adoptions.send(adoption) {
            // The run is over: it holds nothing of the room any more.
            return Adopting::Apart(unsent.0.entry);
        }
        match answer.await {
            Ok(Reply::Settled(result)) => Adopting::Settled(result),
            Ok(Reply::Declined(entry)) => Adopting::Apart(*entry),
            Err(_dropped) => Adopting::Abandoned,
        }
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
        }
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
    }

    /// The least chain depth of a call made in this context, into whatever
    /// room: the response depth of the deepest turn it runs within that
    /// still runs, `None` outside every running turn (RFC §10.6: a call a
    /// turn makes is at least as deep as the turn's response; gap G-070).
    pub(crate) fn turn_depth(&self) -> Option<u32> {
        self.frames
            .iter()
            .filter(|frame| frame.runs())
            .filter_map(|frame| frame.work.response_depth)
            .max()
    }

    /// The reentry budget a call into `room_id` made in this context draws
    /// on: the budget of the cascade whose delivery set runs the innermost
    /// running frame of a delivery in that room, `None` otherwise (gap
    /// G-070). `None` for `room_id` is a message the router places: its
    /// room known only once routed, it draws on the innermost running
    /// delivery's, so a channel routing back into its room on every
    /// delivery stays bounded.
    pub(crate) fn budget(&self, room_id: Option<&RoomId>) -> Option<ReentryBudget> {
        self.frames
            .iter()
            .rev()
            .filter(|frame| frame.runs())
            .filter(|frame| room_id.is_none_or(|room| frame.work.delivering.as_ref() == Some(room)))
            .find_map(|frame| frame.work.budget.clone())
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

/// Hands `entry` to the run of the current task's context whose work under
/// its room's lock or in its admission order made it, if any: the call is
/// adopted by that run's operation, or declined (RFC §13.5, §9.5.1
/// Reentrance).
pub(crate) async fn adopt(entry: Entry) -> Adopting {
    let context = ExecutionContext::current();
    let Some(frame) = context.taker(entry.room_id()).cloned() else {
        return Adopting::Apart(entry);
    };
    frame.adopt(entry).await
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
