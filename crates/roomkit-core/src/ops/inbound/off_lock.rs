//! The off-lock check (RFC §9.5.1, §10.1 step 5a): the SYNC
//! `BEFORE_BROADCAST` hooks registered with `needs_lock = false` run before
//! the room lock, so that a check that only reads the event (a PII scan, a
//! moderation call) keeps its I/O off it.
//!
//! The trigger of a call, once arrived, takes the room's admission ticket
//! when an off-lock hook fires for it; an event no off-lock hook fires for
//! takes none. Its off-lock hooks then run in chain order, on the context
//! step 2 left, each within its own timeout and the call's deadline, and a
//! block stops them; the hooks that produced no usable result are reported
//! as they are met (`hook_error`, `hook_timeout`). The check then waits for
//! every earlier ticket of the room to be released, and hands its chain to
//! the trigger's pass: step 9 applies its outcome after the status,
//! idempotency and edit gates, and runs only the hooks that need the lock.
//! An event a gate refuses before step 9 drops the outcome with its side
//! effects, as if no hook had run (gap G-067).
//!
//! The call holds the ticket until its trigger's pass is over, and releases
//! it on every path (`holds`). The checks of two calls into one room
//! overlap, and their tickets order their commits (RFC §9.5.1 Order). A
//! response, a hook's injected event, a refused record and an entry adopted
//! from the room's own work run every hook under the lock, and take no
//! ticket (RFC §9.5.1, gap G-070).

use std::mem;

use super::arrival::Arrived;
use super::chain::HookRun;
use super::deadline::Stopped;
use super::{InboundError, Machine, Stage, report};
use crate::model::{RoomContext, RoomEvent, RoomId};
use crate::ops::io::Bound;
use crate::ops::protocol::{EffectId, HoldRequest, Outcome};
use crate::ops::{Cx, Progress};
use crate::rules::hook_chain::HookChain;
use crate::rules::hook_registry::HookId;

/// One event's off-lock check.
#[derive(Debug)]
pub(super) struct OffLockCheck {
    event: RoomEvent,
    /// The room's context as step 2 left it, which the hooks read (RFC
    /// §9.5.1).
    context: Box<RoomContext>,
    bound: Bound,
    /// Whether the check holds the room's admission ticket.
    ticketed: bool,
    state: State,
}

/// Where the check stands, with the run of its hooks.
#[derive(Debug)]
enum State {
    Start(Box<HookRun>),
    Taking { effect: EffectId, run: Box<HookRun> },
    Running { effect: EffectId, run: Box<HookRun> },
    Awaiting { effect: EffectId, run: Box<HookRun> },
    Done,
}

/// What a check hands the trigger's pass: its event and the chain its
/// off-lock hooks left.
#[derive(Debug)]
pub(super) struct Checked {
    pub(super) event: RoomEvent,
    pub(super) chain: HookChain<RoomEvent>,
}

/// Where a check stands after an answer.
type Checking = Progress<Result<Checked, Stopped>>;

impl OffLockCheck {
    /// The check of the event that `arrived`, running `hooks`, the
    /// off-lock hooks that fire for it, within the call's `bound`.
    pub(super) fn new(hooks: Vec<HookId>, arrived: Arrived, bound: Bound) -> Self {
        let run = Box::new(HookRun::of(hooks, &arrived.event));
        Self {
            event: arrived.event,
            context: arrived.context,
            bound,
            ticketed: false,
            state: State::Start(run),
        }
    }

    /// Takes the room's admission ticket: it never waits.
    pub(super) fn start(&mut self, cx: &mut Cx<'_>) {
        self.state = match mem::replace(&mut self.state, State::Done) {
            State::Start(run) => {
                let room_id = self.event.room_id.clone();
                let effect = cx.io.issue(HoldRequest::TakeTicket { room_id });
                State::Taking { effect, run }
            }
            state @ (State::Taking { .. }
            | State::Running { .. }
            | State::Awaiting { .. }
            | State::Done) => state,
        };
    }

    /// The room whose ticket the check holds, which its caller releases.
    pub(super) fn ticket(&self) -> Option<&RoomId> {
        self.ticketed.then_some(&self.event.room_id)
    }

    /// Takes the driver's answer to the check's outstanding effect; done
    /// once every earlier ticket is released, or with why the check
    /// stopped.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Checking {
        match mem::replace(&mut self.state, State::Done) {
            State::Taking {
                effect: expected,
                run,
            } if expected == effect => self.on_ticketed(cx, run, outcome),
            State::Running {
                effect: expected,
                run,
            } if expected == effect => self.on_hook(cx, run, outcome),
            State::Awaiting {
                effect: expected,
                run,
            } if expected == effect => self.on_admitted(*run, outcome),
            state @ (State::Start(_)
            | State::Taking { .. }
            | State::Running { .. }
            | State::Awaiting { .. }
            | State::Done) => {
                debug_assert!(false, "the off-lock check received {effect:?} in {state:?}");
                self.state = state;
                Progress::Waiting
            }
        }
    }

    /// The ticket is held: the hooks run. A ticket that cannot be taken
    /// fails the call, with nothing written.
    fn on_ticketed(&mut self, cx: &mut Cx<'_>, run: Box<HookRun>, outcome: Outcome) -> Checking {
        if let Err(error) = outcome.into_ticketed() {
            return Progress::Done(Err(Stopped::Failed(InboundError::Ticket(error))));
        }
        self.ticketed = true;
        self.next_hook(cx, run)
    }

    /// One off-lock hook answered: what it produced is applied, and an
    /// unusable result reported at once (RFC §26.1, steps 5a and 9).
    fn on_hook(&mut self, cx: &mut Cx<'_>, mut run: Box<HookRun>, outcome: Outcome) -> Checking {
        if outcome == Outcome::DeadlinePassed {
            return Progress::Done(Err(Stopped::Expired));
        }
        run.apply(cx, outcome, &self.context.bindings);
        let incidents = run.chain_mut().take_incidents();
        report::hook_incidents(cx, &self.event, &incidents);
        self.next_hook(cx, run)
    }

    /// Runs the next off-lock hook, in the room's admission order (RFC
    /// §9.5.1 Reentrance), or, once a hook blocked or none is left, waits for
    /// every earlier ticket of the room.
    fn next_hook(&mut self, cx: &mut Cx<'_>, mut run: Box<HookRun>) -> Checking {
        let room_id = self.event.room_id.clone();
        let asked = cx.in_admission_order(&room_id, |cx| {
            run.ask_next(cx, &self.event, &self.context, self.bound, None)
        });
        if let Some(effect) = asked {
            self.state = State::Running { effect, run };
            return Progress::Waiting;
        }
        let room_id = self.event.room_id.clone();
        let effect = cx
            .io
            .issue_within(HoldRequest::AwaitTicket { room_id }, self.bound);
        self.state = State::Awaiting { effect, run };
        Progress::Waiting
    }

    /// The check's turn: its chain goes to the trigger's pass.
    fn on_admitted(&self, run: HookRun, outcome: Outcome) -> Checking {
        if outcome == Outcome::DeadlinePassed {
            return Progress::Done(Err(Stopped::Expired));
        }
        if let Err(error) = outcome.into_admitted() {
            return Progress::Done(Err(Stopped::Failed(InboundError::Ticket(error))));
        }
        Progress::Done(Ok(Checked {
            event: self.event.clone(),
            chain: run.into_chain(),
        }))
    }
}

impl Machine {
    /// The off-lock hooks `event`'s check runs before the room lock: none
    /// in the room the call is nested under (`Inbound::under_lock`), whose
    /// pass runs every hook under the lock, as an adopted entry's does
    /// (RFC §9.5.1).
    pub(super) fn off_lock_checks(&self, cx: &Cx<'_>, event: &RoomEvent) -> Vec<HookId> {
        if self.call.under_lock.as_ref() == Some(&event.room_id) {
            return Vec::new();
        }
        cx.config.hooks.off_lock_checks(event)
    }

    /// The trigger arrived in its room: its off-lock check runs when an
    /// off-lock hook fires for it; otherwise its pass starts at once, with
    /// no ticket (RFC §9.5.1).
    pub(super) fn after_arrival(&mut self, cx: &mut Cx<'_>, arrived: Arrived) {
        let hooks = self.off_lock_checks(cx, &arrived.event);
        if hooks.is_empty() {
            return self.start_trigger(arrived.event, None);
        }
        let mut check = Box::new(OffLockCheck::new(hooks, arrived, self.call.bound));
        check.start(cx);
        self.stage.enter(Stage::Checking(check));
    }

    /// Takes an answer of the off-lock check; once it is over, the call
    /// holds the ticket it took, and its trigger's pass starts, or the call
    /// ends.
    pub(super) fn on_check(
        &mut self,
        cx: &mut Cx<'_>,
        mut check: Box<OffLockCheck>,
        effect: EffectId,
        outcome: Outcome,
    ) {
        let Progress::Done(done) = check.accept(cx, effect, outcome) else {
            self.stage.enter(Stage::Checking(check));
            return;
        };
        self.trigger.ticket = check.ticket().cloned();
        match done {
            Ok(Checked { event, chain }) => self.start_trigger(event, Some(chain)),
            Err(Stopped::Failed(error)) => self.end(cx, Err(error)),
            Err(Stopped::Expired) => self.expire(cx, None),
            Err(Stopped::PastBudget) => self.end(cx, Ok(super::result::past_the_budget())),
            Err(Stopped::Refused(refused)) => self.end(cx, Ok(*refused)),
        }
    }
}
