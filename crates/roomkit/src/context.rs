//! The execution context of the calls the driver makes for an operation
//! (RFC §13.5: "the room lock MUST be reentrant within one execution
//! context (a task and the tasks it awaits as its own)").
//!
//! Every hook and channel call the driver spawns for an operation runs in a
//! tokio task-local context: a stack of frames, the innermost last, each
//! naming a run of the driver (what it holds now, read live, and the queue
//! that hands it calls into its room), the effect whose call this is, and,
//! when the operation made the request under a room's lock
//! (`Effect::locked`), a lease on that lock's tenure. A call made inside a
//! call keeps the outer frames: a hook of room S run inside a hook of room
//! R's pass holds R's lease too.
//!
//! The driver reads the context of the task that runs it:
//!
//! - a lock a frame holds a live lease on is granted at once (counted in
//!   the run that asks, released with the lease holder's release only);
//! - a lane or a ticket a frame's run holds is never waited for: that run
//!   waits for this call, so the call fails instead
//!   ([`KitError::Reentrant`](crate::KitError::Reentrant));
//! - a call into the room whose lane a frame's run holds is adopted by that
//!   run's operation instead of run apart.
//!
//! A lease is live while the run that holds the lock keeps the tenure it
//! was given under, so a context kept past the release grants nothing.
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

use roomkit_core::model::RoomId;
use roomkit_core::ops::{EffectId, Entry, InboundError, InboundResult};
use tokio::sync::{mpsc, oneshot};

tokio::task_local! {
    static CONTEXT: ExecutionContext;
}

/// What an adopted entry's caller receives (RFC §10.1 step 18).
pub(crate) type EntryResult = Result<InboundResult, InboundError>;

/// A call into a room, handed to the run that holds the room's lane.
#[derive(Debug)]
pub(crate) struct Adoption {
    /// The call.
    pub(crate) entry: Entry,
    /// The effect whose call made it, in the run that holds the lane.
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
    lanes: HashSet<RoomId>,
    tickets: HashSet<RoomId>,
    locks: HashMap<RoomId, Tenure>,
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
}

/// The frames of the calls the current task runs within, the innermost
/// last.
#[derive(Clone, Debug, Default)]
pub(crate) struct ExecutionContext {
    frames: Arc<[Frame]>,
}

/// What became of a call handed to the run that holds its room's lane.
#[derive(Debug)]
pub(crate) enum Adopting {
    /// No run of the caller's context holds the room's lane, or the run
    /// declined it: the call runs apart, waiting for the lane as any call
    /// does, unless its own context holds it.
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

    /// Records whether the run holds `room_id`'s lane.
    pub(crate) fn set_lane(&self, room_id: &RoomId, held: bool) {
        let mut holdings = self.holdings();
        if held {
            holdings.lanes.insert(room_id.clone());
        } else {
            holdings.lanes.remove(room_id);
        }
    }

    /// Records whether the run holds a ticket of `room_id`.
    pub(crate) fn set_ticket(&self, room_id: &RoomId, held: bool) {
        let mut holdings = self.holdings();
        if held {
            holdings.tickets.insert(room_id.clone());
        } else {
            holdings.tickets.remove(room_id);
        }
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

    fn holds_lane(&self, room_id: &RoomId) -> bool {
        self.holdings().lanes.contains(room_id)
    }

    fn holds_ticket(&self, room_id: &RoomId) -> bool {
        self.holdings().tickets.contains(room_id)
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

impl Frame {
    /// The frame of the call for `effect`, made by `run`, under the lock of
    /// `locked` when the operation holds it on the call's behalf.
    pub(crate) fn new(run: &Arc<RunShared>, effect: EffectId, locked: Option<&RoomId>) -> Self {
        Self {
            run: Arc::clone(run),
            effect,
            lease: locked.and_then(|room_id| run.lease_of(room_id)),
        }
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
            // The run is over: its lane is free.
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

    /// Whether a run of this context holds `room_id`'s lane: that run waits
    /// for this call, and so may not be waited for.
    pub(crate) fn holds_lane(&self, room_id: &RoomId) -> bool {
        self.lane_holder(room_id).is_some()
    }

    /// Whether a run of this context holds a ticket of `room_id`.
    pub(crate) fn holds_ticket(&self, room_id: &RoomId) -> bool {
        self.frames
            .iter()
            .any(|frame| frame.run.holds_ticket(room_id))
    }

    /// The innermost frame whose run holds `room_id`'s lane.
    fn lane_holder(&self, room_id: &RoomId) -> Option<&Frame> {
        self.frames
            .iter()
            .rev()
            .find(|frame| frame.run.holds_lane(room_id))
    }
}

/// Hands `entry` to the run of the current task's context that holds its
/// room's lane, if any: the call is adopted by that run's operation (RFC
/// §13.5, §10.1 step 18).
pub(crate) async fn adopt(entry: Entry) -> Adopting {
    let context = ExecutionContext::current();
    let Some(frame) = context.lane_holder(entry.room_id()).cloned() else {
        return Adopting::Apart(entry);
    };
    frame.adopt(entry).await
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
    use roomkit_core::ops::{CheckTimers, KitConfig, Next, Operation, SequentialEnvironment};

    use super::*;

    fn room_id() -> RoomId {
        RoomId::new("room_1").unwrap()
    }

    /// An effect id, the first an operation issues: any id serves here.
    fn effect() -> EffectId {
        let mut operation = CheckTimers::new(Arc::new(KitConfig::new()), room_id());
        match operation.poll(&mut SequentialEnvironment::default()).next {
            Next::Effects(effects) => effects.first().unwrap().id,
            Next::Waiting | Next::Ready(_) => panic!("a timer check asks for the lock first"),
        }
    }

    #[test]
    fn a_lease_lives_while_its_tenure_is_held() {
        let (run, _queue) = RunShared::new();
        run.locked(room_id());
        let context =
            ExecutionContext::default().within(Frame::new(&run, effect(), Some(&room_id())));
        assert!(context.lease(&room_id()).is_some());
    }

    /// RFC §13.5: a context kept past the release grants nothing.
    #[test]
    fn a_lease_dies_with_the_release() {
        let (run, _queue) = RunShared::new();
        run.locked(room_id());
        let context =
            ExecutionContext::default().within(Frame::new(&run, effect(), Some(&room_id())));
        run.released(&room_id());
        assert!(context.lease(&room_id()).is_none());
    }

    /// A lease on one tenure is not a lease on the next one, the same run's.
    #[test]
    fn a_lease_dies_with_its_tenure_even_when_the_lock_is_taken_again() {
        let (run, _queue) = RunShared::new();
        run.locked(room_id());
        let context =
            ExecutionContext::default().within(Frame::new(&run, effect(), Some(&room_id())));
        run.released(&room_id());
        run.locked(room_id());
        assert!(context.lease(&room_id()).is_none());
    }

    #[test]
    fn a_call_made_outside_the_lock_holds_no_lease() {
        let (run, _queue) = RunShared::new();
        run.locked(room_id());
        let context = ExecutionContext::default().within(Frame::new(&run, effect(), None));
        assert!(context.lease(&room_id()).is_none());
    }

    #[test]
    fn a_lane_is_held_while_the_run_holds_it() {
        let (run, _queue) = RunShared::new();
        run.set_lane(&room_id(), true);
        let context = ExecutionContext::default().within(Frame::new(&run, effect(), None));
        let held = context.holds_lane(&room_id());
        run.set_lane(&room_id(), false);
        assert_eq!((held, context.holds_lane(&room_id())), (true, false));
    }
}
