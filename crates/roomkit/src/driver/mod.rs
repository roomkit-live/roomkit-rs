//! The driver: runs any operation of the core to its end, executing the
//! effects it asks for and performing its notices.
//!
//! An effect is carried out as soon as the operation asks for it, and
//! completed as soon as it is answered: the driver polls the operation
//! between completions. A release, a place in a room's lane and a ticket
//! are carried out at once; a lock, a place's turn or a ticket's turn is a
//! wait the driver polls itself (`holds`); a call on the store, a channel,
//! a hook or the router is a task (`calls`), run within its execution
//! context, so that code it runs may take the lock the operation holds on
//! its behalf again, or call into the room under its lock or in its
//! admission order: such a call is handed to the operation, which adopts
//! it, and its caller is answered when the operation settles it (RFC §13.5,
//! §9.5.1 Reentrance, §10.1 step 18). An inbound call's operation runs on a
//! task of its own, within its caller's execution context, and answers a
//! caller that cannot wait for its delivery at its decision (`inbound`).
//!
//! The driver never waits for what its own context holds: a lock its
//! context holds a lease on, asked by an inbound call, or a ticket's turn
//! in a room's admission order its context stands in, fails the call with
//! [`KitError::Reentrant`], since that run waits for this one. An effect
//! that carries a deadline (RFC §13.6) is given up on past it: a lock or a
//! turn waited for is not taken, a call in flight is dropped, and the
//! operation is answered `DeadlinePassed`.

mod calls;
mod holds;
mod inbound;

use std::collections::HashMap;
use std::future::poll_fn;
use std::sync::Arc;
use std::task::Poll;

use roomkit_core::model::{ErrorRecord, RoomId, Timestamp};
use roomkit_core::ops::{Effect, EffectId, EntryId, Next, Operation, Outcome, Request, Settled};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{self, JoinError, JoinSet};
use tokio::time::Instant;

use self::holds::{Held, Hold, Leases, Rooms, Waited, Waiting};
use self::inbound::Decision;
pub(crate) use self::inbound::run_inbound;
use crate::context::{Adoption, ExecutionContext, Frame, Reply, RunShared, Work};
use crate::error::KitError;
use crate::kit::Inner;
use crate::store::ConversationStore;

/// The kit's clock and the runtime's, read together at a run's start: a
/// deadline the core set on the kit's clock is waited for on the runtime's,
/// whether the kit's clock is the system's or one a test moves by hand.
#[derive(Clone, Copy, Debug)]
struct Clocks {
    kit: Timestamp,
    runtime: Instant,
}

impl Clocks {
    fn read<S>(inner: &Inner<S>) -> Self {
        Self {
            kit: inner.environment().now(),
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
/// Returns [`KitError::Reentrant`] when the operation waited for a ticket's
/// turn in an admission order its own execution context stands in, which
/// was refused; a protocol error or a stall is a defect of this crate.
pub(crate) async fn run<S: ConversationStore, O: Operation>(
    inner: &Arc<Inner<S>>,
    call: &'static str,
    operation: O,
) -> Result<O::Output, KitError> {
    drive(
        inner,
        call,
        operation,
        Leases::Granted,
        &mut Decision::Unwanted,
    )
    .await
}

/// Runs `operation`, made by the kit's call `call`, until it is ready,
/// treating a lock its execution context holds a live lease on as `leases`
/// says, and offering its decision to `decision`.
async fn drive<S: ConversationStore, O: Operation>(
    inner: &Arc<Inner<S>>,
    call: &'static str,
    operation: O,
    leases: Leases,
    decision: &mut Decision,
) -> Result<O::Output, KitError> {
    let (shared, adoptions) = RunShared::new();
    let driver = Driver {
        calls: JoinSet::new(),
        waits: Vec::new(),
        inner,
        call,
        context: ExecutionContext::current(),
        held: Held::new(Arc::clone(&shared), leases),
        shared,
        adoptions,
        effect_of_task: HashMap::new(),
        callers: HashMap::new(),
        clocks: Clocks::read(inner),
        refused: None,
        decision,
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
    waits: Vec<Waiting<'kit>>,
    inner: &'kit Arc<Inner<S>>,
    /// The kit's call the run serves, which an error names.
    call: &'static str,
    /// The execution context the run is made in.
    context: ExecutionContext,
    held: Held<'kit>,
    shared: Arc<RunShared>,
    /// The calls into the room its work hands the run.
    adoptions: mpsc::UnboundedReceiver<Adoption>,
    effect_of_task: HashMap<task::Id, EffectId>,
    /// The callers of the entries the operation adopted, until settled.
    callers: HashMap<EntryId, oneshot::Sender<Reply>>,
    clocks: Clocks,
    /// The room whose lock or ticket's turn the run was refused, its context
    /// holding it.
    refused: Option<RoomId>,
    /// Where the operation's decision goes.
    decision: &'kit mut Decision,
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
}

impl<'kit, S: ConversationStore> Driver<'kit, S> {
    async fn drive<O: Operation>(mut self, mut operation: O) -> Result<O::Output, KitError> {
        loop {
            let step = {
                let mut environment = self.inner.environment();
                operation.poll(&mut **environment)
            };
            for notice in step.notices {
                calls::perform(self.inner, notice);
            }
            for settled in step.settled {
                self.answer(settled);
            }
            if let Some(decided) = step.decided {
                let refused = self.refused.as_ref();
                self.decision
                    .offer(&self.context, self.call, decided, refused);
            }
            let mut answered = false;
            match step.next {
                Next::Ready(output) => return self.finish(output),
                Next::Waiting => {}
                Next::Effects(effects) => {
                    for effect in effects {
                        if let Some((id, outcome)) = self.start(effect) {
                            operation.complete(id, outcome)?;
                            answered = true;
                        }
                    }
                }
            }
            if answered {
                continue;
            }
            match self.next_event().await {
                Some(Event::Waited(id, waited)) => {
                    let outcome = self.held.settle(waited);
                    operation.complete(id, outcome)?;
                }
                Some(Event::Joined(joined)) => {
                    let (id, outcome) = self.joined(joined)?;
                    operation.complete(id, outcome)?;
                }
                Some(Event::Adoption(adoption)) => self.adopt(&mut operation, adoption),
                // The operation waits for effects that were never issued.
                None => return Err(KitError::Stalled),
            }
        }
    }

    /// Starts `effect`: answered at once when it can be, a wait or a call
    /// otherwise.
    fn start(&mut self, effect: Effect) -> Option<(EffectId, Outcome)> {
        let work = Work::of(&effect);
        let Effect {
            id,
            request,
            deadline,
            ..
        } = effect;
        let deadline = deadline.and_then(|deadline| self.clocks.instant_of(deadline));
        let rooms = Rooms {
            locks: &self.inner.locks,
            lanes: &self.inner.lanes,
            tickets: &self.inner.tickets,
        };
        match self.held.hold(rooms, &self.context, id, request, deadline) {
            Hold::Now(outcome) => Some((id, outcome)),
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
                self.waits.push(wait);
                None
            }
            Hold::Call(request) => {
                let frame = Frame::new(&self.shared, id, work);
                self.spawn(frame, *request, deadline);
                None
            }
        }
    }

    /// Runs `request` as a task, within its execution context: the run's
    /// `frame` for its effect.
    fn spawn(&mut self, frame: Frame, request: Request, deadline: Option<Instant>) {
        let effect = frame.effect();
        let context = self.context.within(frame);
        let inner = Arc::clone(self.inner);
        let task = self.calls.spawn(context.scope(async move {
            within(deadline, calls::call(&inner, request))
                .await
                .unwrap_or(Outcome::DeadlinePassed)
        }));
        self.effect_of_task.insert(task.id(), effect);
    }

    /// The next wait over, call finished or call into the room handed to
    /// the run; `None` when the run waits for nothing.
    async fn next_event(&mut self) -> Option<Event<'kit>> {
        if self.waits.is_empty() && self.calls.is_empty() {
            return None;
        }
        let Self {
            waits,
            calls,
            adoptions,
            ..
        } = self;
        poll_fn(|cx| {
            let ready = waits.iter_mut().enumerate().find_map(|(index, wait)| {
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
    fn joined(
        &mut self,
        joined: Result<(task::Id, Outcome), JoinError>,
    ) -> Result<(EffectId, Outcome), KitError> {
        let (task, outcome) = match joined {
            Ok(finished) => finished,
            Err(failure) => {
                let error = if failure.is_panic() {
                    ErrorRecord::new("Panic", "the call panicked")
                } else {
                    ErrorRecord::new("Cancelled", "the call was cancelled")
                };
                (failure.id(), Outcome::Failed(error))
            }
        };
        let effect = self.effect_of_task.remove(&task).ok_or(KitError::Stalled)?;
        Ok((effect, outcome))
    }

    /// Hands the operation a call into its room made by one of its calls;
    /// a call it does not adopt is declined, and runs apart.
    fn adopt<O: Operation>(&mut self, operation: &mut O, adoption: Adoption) {
        let Adoption {
            entry,
            origin,
            reply,
        } = adoption;
        match operation.adopt(entry, origin) {
            Ok(id) => {
                self.callers.insert(id, reply);
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
