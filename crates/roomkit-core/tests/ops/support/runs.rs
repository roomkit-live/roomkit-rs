//! The runs of the simulated driver: the operation a test runs, and the
//! calls its hooks and channels make that no run adopts, each run as an
//! operation of its own, an arrival, as the runtime runs a call apart
//! (docs/architecture.md, "Reentrancy").
//!
//! The driver polls one run at a time, the newest that has something to
//! take in, and hands each the answers and the calls meant for it in the
//! order they came. A call is handed to the run its context names, which
//! adopts it or declines it; a declined call, or one no run of its context
//! takes, becomes an arrival, which runs within the context of the call
//! that made it.
//!
//! A request for a lock another run holds, a ticket's turn behind another
//! run's ticket, or a set's turn behind another place of the room's lane,
//! waits: it is granted once the holder lets it go, in the order the
//! requests came, or given up on at its deadline once no run has anything
//! left to do. Runs that all wait for each other, with no deadline to pass,
//! fail the test: a deadlock never hangs it.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use roomkit_core::model::{EventId, RoomId, Timestamp};
use roomkit_core::ops::{
    ChannelRequest, Decided, Effect, EffectId, Entry, EntryId, Environment, HoldOutcome, Inbound,
    InboundTarget, Next, Notice, Operation, Outcome, Request, Settled, StoreRequest,
};
use roomkit_core::rules::chain_depth::{Draw, ReentryBudget};
use roomkit_core::rules::transcoding::text_of;

use super::{Called, Caller, Sim, Then, journal_line};

/// A run of the simulated driver, by the order it started in: the
/// operation the test runs first.
pub(super) type RunId = usize;

/// The operation the test runs.
pub(super) const ROOT: RunId = 0;

/// The holder of the place `place_ahead` names: another call, which runs
/// nothing here.
const ANOTHER_CALL: RunId = RunId::MAX;

/// One frame of a call's execution context: the run, its effect whose call
/// this is, and what the driver knows of that effect.
#[derive(Clone, Debug)]
pub(super) struct Frame {
    pub(super) run: RunId,
    pub(super) effect: EffectId,
    /// The room whose lock the run holds on the effect's behalf.
    locked: Option<RoomId>,
    /// The room in whose admission order the effect runs.
    admitted: Option<RoomId>,
    /// The room a delivery set delivers in, for its calls.
    delivering: Option<RoomId>,
    /// The reentry budget of the cascade whose work the effect is: the
    /// one that delivers, for a delivery's calls; the one its section works
    /// for, for a hook under a lock or in an admission order.
    budget: Option<ReentryBudget>,
}

impl Frame {
    fn of(run: RunId, effect: &Effect) -> Self {
        let delivering = if let Request::Channel(
            ChannelRequest::OnEvent { binding, .. } | ChannelRequest::Deliver { binding, .. },
        ) = &effect.request
        {
            Some(binding.room_id.clone())
        } else {
            None
        };
        Self {
            run,
            effect: effect.id,
            locked: effect.locked.clone(),
            admitted: effect.admitted.clone(),
            delivering,
            budget: effect.budget.clone(),
        }
    }

    /// Whether the frame's run takes a call into `room_id` its effect's code
    /// makes: code under the room's lock or in its admission order (RFC
    /// §9.5.1 Reentrance).
    fn takes(&self, room_id: &RoomId) -> bool {
        [&self.locked, &self.admitted]
            .into_iter()
            .any(|room| room.as_ref() == Some(room_id))
    }

    /// Whether the frame's code works in `room_id`, under its lock, in its
    /// admission order or delivering in it: a call it makes into the room
    /// returns at its decision (RFC §10.1 step 18).
    fn works_in(&self, room_id: &RoomId) -> bool {
        self.takes(room_id) || self.delivering.as_ref() == Some(room_id)
    }
}

/// A call into a room a hook or a channel made, until its caller is
/// answered.
pub(super) struct Call {
    /// The context the call is made within, the innermost frame last: the
    /// caller's context and its own frame, or none for a call made outside
    /// it.
    pub(super) context: Vec<Frame>,
    /// The frame of the effect whose call this is.
    pub(super) frame: Frame,
    /// Whether the caller waits for the call's result.
    pub(super) waits: bool,
    pub(super) label: String,
    pub(super) caller: Caller,
}

/// What a run takes in before its next poll, in the order it came.
#[expect(
    clippy::large_enum_variant,
    reason = "a mail is moved once, from the queue to the run: an answer carries its outcome whole"
)]
enum Mail {
    /// The answer to one of its effects.
    Answer(EffectId, Outcome),
    /// Its operation answered effects of its own, given up on: it is
    /// polled again to take them in.
    Expired,
    /// A call into its room, made within its effect `origin`, for it to
    /// adopt.
    Call {
        entry: Box<Entry>,
        origin: EffectId,
        call: Box<Call>,
    },
}

/// When a run's caller stopped waiting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Gone {
    BeforeTheDecision,
    OnceDecided,
}

/// What the driver keeps of one run.
pub(super) struct RunState {
    /// The context the run is made within, the innermost frame last.
    context: Vec<Frame>,
    mail: VecDeque<Mail>,
    /// Whether the run was polled yet.
    started: bool,
    /// Whether it published its decision.
    decided: bool,
    /// When its caller stopped waiting, if it did: before the decision,
    /// its turns are cut once it is decided, as the runtime's driver cuts
    /// them.
    gone: Option<Gone>,
    done: bool,
    /// Its effects not answered yet.
    outstanding: BTreeSet<EffectId>,
    /// The effects it gave up on, expired: never answered.
    given_up: BTreeSet<EffectId>,
    /// The entries it adopted, until they are settled.
    pub(super) adopted: Vec<(EntryId, Call)>,
    /// The call an arrival is, until its caller is answered.
    serves: Option<Call>,
    pub(super) locks: BTreeSet<RoomId>,
    /// The places it holds in the rooms' lanes, by room and event.
    places: BTreeSet<(RoomId, EventId)>,
    /// The places whose turn came.
    turns: BTreeSet<(RoomId, EventId)>,
    pub(super) tickets: BTreeSet<RoomId>,
    /// The rooms whose ticket's turn came.
    pub(super) admitted: BTreeSet<RoomId>,
}

impl RunState {
    fn new(context: Vec<Frame>, serves: Option<Call>) -> Self {
        Self {
            context,
            mail: VecDeque::new(),
            started: false,
            decided: false,
            gone: None,
            done: false,
            outstanding: BTreeSet::new(),
            given_up: BTreeSet::new(),
            adopted: Vec::new(),
            serves,
            locks: BTreeSet::new(),
            places: BTreeSet::new(),
            turns: BTreeSet::new(),
            tickets: BTreeSet::new(),
            admitted: BTreeSet::new(),
        }
    }

    /// Whether the run has something to take in.
    fn is_ready(&self) -> bool {
        !self.done && (!self.started || !self.mail.is_empty())
    }
}

/// What a request that waits waits for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Awaited {
    Lock(RoomId),
    /// The turn of the run's place for the event in the room's lane.
    Place(RoomId, EventId),
    /// The turn of the run's ticket of the room.
    Turn(RoomId),
}

/// A request that waits for what another run holds.
#[derive(Debug)]
pub(super) struct Wait {
    pub(super) run: RunId,
    pub(super) effect: EffectId,
    pub(super) deadline: Option<Timestamp>,
    pub(super) awaited: Awaited,
}

impl Awaited {
    /// The trace line of the request once it is answered.
    fn line(&self) -> String {
        match self {
            Self::Lock(room_id) => format!("lock {room_id}"),
            Self::Place(room_id, event_id) => format!("await lane {room_id} {event_id}"),
            Self::Turn(room_id) => format!("await ticket {room_id}"),
        }
    }
}

/// What the driver holds for the runs: the room locks by holder, each
/// room's lane places and tickets in the order they were taken, and the
/// requests waiting.
#[derive(Debug, Default)]
pub(super) struct Holders {
    locks: BTreeMap<RoomId, RunId>,
    places: BTreeMap<RoomId, Vec<(RunId, EventId)>>,
    tickets: BTreeMap<RoomId, Vec<RunId>>,
    waits: Vec<Wait>,
}

impl Sim {
    /// Runs `operation` to its end, and every arrival its calls make to
    /// theirs, executing every effect they ask for, and completing an
    /// effect whose call made an entry or an arrival once its caller is
    /// answered.
    pub(crate) fn run<O: Operation>(&mut self, operation: O) -> O::Output {
        self.runs = vec![RunState::new(Vec::new(), None)];
        self.holders = Holders::default();
        if let Some(event_id) = self.place_ahead.clone() {
            let ahead = (ANOTHER_CALL, event_id);
            self.holders.places.insert(super::room_id(), vec![ahead]);
        }
        let mut root = operation;
        let mut arrivals: Vec<Inbound> = Vec::new();
        let mut output = None;
        while let Some(run) = self.next_run() {
            self.current = run;
            if let Some(index) = run.checked_sub(1) {
                let arrival = arrivals.get_mut(index).unwrap();
                if let Some(called) = self.advance(arrival) {
                    self.arrived(run, &called);
                }
            } else if let Some(done) = self.advance(&mut root) {
                output = Some(done);
            }
            arrivals.append(&mut self.spawned);
        }
        output.unwrap_or_else(|| panic!("the operation never ended"))
    }

    /// The state of the run `run`.
    pub(super) fn state(&self, run: RunId) -> &RunState {
        self.runs.get(run).unwrap()
    }

    pub(super) fn state_mut(&mut self, run: RunId) -> &mut RunState {
        self.runs.get_mut(run).unwrap()
    }

    /// The newest run with something to take in; once none has, a request
    /// waiting with a deadline is given up on; `None` once every run is over.
    fn next_run(&mut self) -> Option<RunId> {
        loop {
            if let Some(run) = (0..self.runs.len())
                .rev()
                .find(|run| self.state(*run).is_ready())
            {
                return Some(run);
            }
            if self.runs.iter().all(|run| run.done) {
                return None;
            }
            assert!(
                self.expire_first_wait(),
                "the runs wait for each other: {:?}",
                self.holders.waits
            );
        }
    }

    /// Hands the current run what came for it, then polls it once and
    /// executes the effects it asks for; its output once it is over.
    fn advance<O: Operation>(&mut self, operation: &mut O) -> Option<O::Output> {
        let run = self.current;
        self.state_mut(run).started = true;
        while let Some(mail) = self.state_mut(run).mail.pop_front() {
            match mail {
                Mail::Answer(effect, outcome) => {
                    self.state_mut(run).outstanding.remove(&effect);
                    operation.complete(effect, outcome).unwrap();
                }
                // Its answers are its operation's own: the poll takes them.
                Mail::Expired => {}
                Mail::Call {
                    entry,
                    origin,
                    call,
                } => self.offer(operation, *entry, origin, *call),
            }
        }
        let step = operation.poll(&self.env);
        self.journal.extend(step.notices.iter().map(journal_line));
        for notice in &step.notices {
            if let Notice::AsyncHooks { trigger, event, .. } = notice {
                let locked = self.state(run).locks.contains(&event.room_id);
                self.fired.push((*trigger, locked));
            }
        }
        self.notices.extend(step.notices);
        for settled in step.settled {
            self.answer_settled(settled);
        }
        if let Some(decided) = step.decided {
            self.record_decision(decided);
            // A caller gone before a commit in flight decided the call.
            if self.state(run).gone == Some(Gone::BeforeTheDecision) {
                let given_up = operation.cancel_turns();
                self.drop_given_up(run, &given_up);
            }
        }
        match step.next {
            Next::Ready(output) => {
                self.end_run();
                return Some(output);
            }
            Next::Waiting => {
                let state = self.state(run);
                assert!(
                    !state.mail.is_empty() || !state.outstanding.is_empty(),
                    "the operation waits for nothing the driver does"
                );
            }
            Next::Effects(effects) => {
                // An effect of this step the expiry gave up on is skipped; one
                // given up on at an earlier step never comes again.
                let mut expired_now = false;
                for effect in effects {
                    if run == ROOT && self.caller_leaves_before(&effect.request) {
                        // Before the decision the call expires; once decided,
                        // its turns are cut (RFC §10.1, §13.6), as the
                        // runtime's driver does.
                        let given_up = if self.state(run).decided {
                            self.state_mut(run).gone = Some(Gone::OnceDecided);
                            operation.cancel_turns()
                        } else {
                            self.state_mut(run).gone = Some(Gone::BeforeTheDecision);
                            operation.expire()
                        };
                        self.drop_given_up(run, &given_up);
                        expired_now = true;
                    }
                    if self.state(run).given_up.contains(&effect.id) {
                        assert!(
                            expired_now,
                            "the operation issued {:?}, which it gave up on",
                            effect.request
                        );
                        continue;
                    }
                    self.state_mut(run).outstanding.insert(effect.id);
                    self.execute_effect(effect);
                }
            }
        }
        None
    }

    /// Posts `outcome`, the answer to `effect`, to the run `run`, unless
    /// the run gave the effect up.
    pub(super) fn post(&mut self, run: RunId, effect: EffectId, outcome: Outcome) {
        let state = self.state_mut(run);
        if !state.given_up.contains(&effect) {
            state.mail.push_back(Mail::Answer(effect, outcome));
        }
    }

    /// Whether the call's caller stops waiting before `request` is
    /// executed: once, before the first request whose label starts with
    /// `leaves_before`.
    fn caller_leaves_before(&mut self, request: &Request) -> bool {
        let leaves = self
            .leaves_before
            .is_some_and(|start| self.label(request).starts_with(start));
        if leaves {
            self.leaves_before = None;
            self.note("caller gone".to_owned());
        }
        leaves
    }

    /// Drops what the run `run` gave up on: its waits, the answers posted
    /// to it, and any answer to come. It is polled again, its operation
    /// having answered them itself.
    fn drop_given_up(&mut self, run: RunId, given_up: &[EffectId]) {
        self.holders
            .waits
            .retain(|wait| wait.run != run || !given_up.contains(&wait.effect));
        let state = self.state_mut(run);
        state.given_up.extend(given_up.iter().copied());
        state
            .outstanding
            .retain(|effect| !given_up.contains(effect));
        state.mail.retain(|mail| match mail {
            Mail::Answer(effect, _) => !given_up.contains(effect),
            Mail::Expired | Mail::Call { .. } => true,
        });
        state.mail.push_back(Mail::Expired);
    }

    /// Hands `entry`, made by `call`, to the run its caller's context names
    /// for its room, or runs it as an arrival when none does.
    fn place_call(&mut self, entry: Entry, call: Call) {
        match Self::taker(&call.context, entry.room_id()) {
            Some(frame) => self.state_mut(frame.run).mail.push_back(Mail::Call {
                entry: Box::new(entry),
                origin: frame.effect,
                call: Box::new(call),
            }),
            None => self.spawn(entry, call),
        }
    }

    /// The frame of `context` whose run takes a call into `room_id`: the
    /// innermost whose effect's code runs under the room's lock or in its
    /// admission order.
    fn taker(context: &[Frame], room_id: &RoomId) -> Option<Frame> {
        context
            .iter()
            .rev()
            .find(|frame| frame.takes(room_id))
            .cloned()
    }

    /// The context a call made within `frame` runs in: its run's, then the
    /// frame.
    fn context_of(&self, frame: &Frame) -> Vec<Frame> {
        let mut context = self.state(frame.run).context.clone();
        context.push(frame.clone());
        context
    }

    /// The current run adopts `entry`, or declines it, which then runs as
    /// an arrival.
    fn offer<O: Operation>(
        &mut self,
        operation: &mut O,
        entry: Entry,
        origin: EffectId,
        call: Call,
    ) {
        match operation.adopt(entry, origin) {
            Ok(id) => {
                let run = self.current;
                self.state_mut(run).adopted.push((id, call));
            }
            Err(entry) => self.spawn(*entry, call),
        }
    }

    /// Starts `entry` as an inbound operation of its own, within the
    /// context `call` was made in, answering `call` once it is over; made
    /// within a delivery, it draws on the budget of the cascade that
    /// delivers (gap G-070).
    fn spawn(&mut self, entry: Entry, call: Call) {
        let config = self.shared_config();
        let operation = match entry {
            Entry::Message {
                room_id,
                organization_id,
                message,
                ..
            } => Inbound::new(
                config,
                InboundTarget::Room(room_id),
                organization_id,
                *message,
            ),
            Entry::Direct {
                room_id,
                organization_id,
                event,
            } => Inbound::inject(config, room_id, organization_id, *event),
            Entry::Post {
                room_id,
                organization_id,
                post,
            } => Inbound::post(config, room_id, organization_id, *post),
            Entry::Greeting {
                room_id,
                agent_id,
                text,
                chain_depth,
            } => Inbound::greeting(config, room_id, agent_id, text, chain_depth),
            // No scripted channel streams: no stream's row is handed back.
            Entry::Row { .. } => unreachable!("the simulated driver reads no stream"),
        };
        let operation = match call
            .context
            .iter()
            .rev()
            .find_map(|frame| frame.delivering.as_ref().and(frame.budget.clone()))
        {
            Some(budget) => operation.drawing_on(Draw::Pass(budget)),
            None => operation,
        };
        let context = call.context.clone();
        self.runs.push(RunState::new(context, Some(call)));
        self.spawned.push(operation);
    }

    /// Answers the call that made `settled`'s entry, the current run's.
    fn answer_settled(&mut self, settled: Settled) {
        let run = self.current;
        let index = self
            .state(run)
            .adopted
            .iter()
            .position(|(entry, _)| *entry == settled.entry)
            .unwrap_or_else(|| panic!("an entry settled that no call made: {settled:?}"));
        let (_, call) = self.state_mut(run).adopted.remove(index);
        self.note(format!("{} settled", call.label));
        if call.waits {
            self.answer_call(&call, &settled.result);
        }
        self.settled.push(settled);
    }

    /// Records the current run's decision, which it publishes once, and
    /// answers the caller of an arrival with it when the caller's context
    /// works in the call's room, as the runtime does.
    fn record_decision(&mut self, decided: Decided) {
        let run = self.current;
        assert!(
            !self.state(run).decided,
            "an operation published its decision twice"
        );
        self.state_mut(run).decided = true;
        let detaches = self.state(run).serves.as_ref().is_some_and(|call| {
            call.waits
                && decided
                    .room_id
                    .as_ref()
                    .is_some_and(|room_id| call.context.iter().any(|frame| frame.works_in(room_id)))
        });
        if detaches && let Some(call) = self.state_mut(run).serves.take() {
            self.answer_call(&call, &decided.result);
        }
        self.decisions.push((self.trace.last().cloned(), decided));
        if run == ROOT && self.place_ahead.is_some() {
            self.note("another call's set is over".to_owned());
            if let Some(places) = self.holders.places.get_mut(&super::room_id()) {
                places.retain(|(holder, _)| *holder != ANOTHER_CALL);
            }
            self.grant_waits();
        }
    }

    /// The arrival `run` is over: its caller is answered when it waits.
    fn arrived(&mut self, run: RunId, called: &Called) {
        let Some(call) = self.state_mut(run).serves.take() else {
            return;
        };
        if call.waits {
            self.answer_call(&call, called);
        }
    }

    /// Completes the effect whose call is `call` with what its caller
    /// answers from `called`.
    fn answer_call(&mut self, call: &Call, called: &Called) {
        let outcome = self.answer_from(&call.caller, called);
        self.note(format!("{} returns", call.label));
        self.post(call.frame.run, call.frame.effect, outcome);
    }

    /// Writes `line` to the trace and the journal.
    pub(super) fn note(&mut self, line: String) {
        self.trace.push(line.clone());
        self.journal.push(line);
    }

    /// The current run is over: every entry it adopted is settled, and it
    /// holds nothing.
    fn end_run(&mut self) {
        let run = self.current;
        assert!(
            self.state(run).adopted.is_empty(),
            "the operation ended with an entry unsettled"
        );
        let state = self.state(run);
        assert!(
            state.locks.is_empty(),
            "the operation ended holding the lock of {:?}",
            state.locks
        );
        assert!(
            state.places.is_empty(),
            "the operation ended holding the places {:?}",
            state.places
        );
        assert!(
            state.tickets.is_empty(),
            "the operation ended holding the ticket of {:?}",
            state.tickets
        );
        self.state_mut(run).done = true;
    }

    /// Takes the room's lock for the current run, or makes `effect` wait
    /// for the run that holds it.
    pub(super) fn acquire(
        &mut self,
        effect: EffectId,
        room_id: RoomId,
        deadline: Option<Timestamp>,
    ) -> Option<Outcome> {
        let run = self.current;
        let state = self.state(run);
        assert!(
            !state.locks.contains(&room_id),
            "the room lock was taken twice"
        );
        // An entry adopted from the operation's own work takes no ticket and
        // commits ahead of the event whose check emitted it (RFC §9.5.1).
        assert!(
            !state.tickets.contains(&room_id)
                || state.admitted.contains(&room_id)
                || !state.adopted.is_empty(),
            "a ticketed event took the room lock before its turn"
        );
        if self.holders.locks.contains_key(&room_id) {
            return self.wait(effect, deadline, Awaited::Lock(room_id));
        }
        self.take_lock(run, room_id);
        Some(Outcome::Hold(HoldOutcome::Locked))
    }

    /// Gives `run` the room's lock. A sender recorded meanwhile on the room
    /// is recorded just before the lock is granted, as a concurrent message
    /// that held it first would have.
    fn take_lock(&mut self, run: RunId, room_id: RoomId) {
        self.trace.push(format!("lock {room_id}"));
        self.record_meanwhile(&room_id);
        self.holders.locks.insert(room_id.clone(), run);
        self.state_mut(run).locks.insert(room_id);
    }

    /// Lets the current run's lock of the room go, which it must hold.
    pub(super) fn let_lock_go(&mut self, room_id: &RoomId) {
        let run = self.current;
        assert!(
            self.state_mut(run).locks.remove(room_id),
            "the room lock was released without being held"
        );
        self.holders.locks.remove(room_id);
        self.grant_waits();
    }

    /// Takes the current run's place for `event_id` in the room's lane,
    /// behind every place taken before: under the room lock, so that the
    /// places follow the commits (RFC §10.2). It never waits.
    pub(super) fn take_place(&mut self, room_id: RoomId, event_id: EventId) {
        let run = self.current;
        assert!(
            self.state(run).locks.contains(&room_id),
            "a place in the room's lane was taken outside the room lock"
        );
        let place = (room_id.clone(), event_id.clone());
        assert!(
            self.state_mut(run).places.insert(place),
            "an event's place was taken twice"
        );
        self.holders
            .places
            .entry(room_id)
            .or_default()
            .push((run, event_id));
    }

    /// The turn of the current run's place for `event_id`: at once when no
    /// earlier place of the room is held, once they are left otherwise.
    pub(super) fn await_place(
        &mut self,
        effect: EffectId,
        room_id: RoomId,
        event_id: EventId,
        deadline: Option<Timestamp>,
    ) -> Option<Outcome> {
        let run = self.current;
        let place = (room_id.clone(), event_id.clone());
        assert!(
            self.state(run).places.contains(&place),
            "a set awaited the turn of a place it does not hold"
        );
        if !self.is_first_place(run, &room_id, &event_id) {
            return self.wait(effect, deadline, Awaited::Place(room_id, event_id));
        }
        self.give_turn(run, room_id, event_id);
        Some(Outcome::Hold(HoldOutcome::LaneTurn))
    }

    fn give_turn(&mut self, run: RunId, room_id: RoomId, event_id: EventId) {
        if let Some(status) = self.status_at_lane_turn.take() {
            self.room.status = status;
        }
        self.trace.push(format!("await lane {room_id} {event_id}"));
        self.state_mut(run).turns.insert((room_id, event_id));
    }

    /// Whether `run`'s place for `event_id` is the room's first.
    fn is_first_place(&self, run: RunId, room_id: &RoomId, event_id: &EventId) -> bool {
        self.holders
            .places
            .get(room_id)
            .and_then(|places| places.first())
            .is_some_and(|(holder, first)| *holder == run && first == event_id)
    }

    /// Lets the current run's place for `event_id` go, which it must hold.
    pub(super) fn leave_place(&mut self, room_id: &RoomId, event_id: &EventId) {
        let run = self.current;
        let place = (room_id.clone(), event_id.clone());
        let state = self.state_mut(run);
        assert!(
            state.places.remove(&place),
            "a place in the room's lane was left without being held"
        );
        state.turns.remove(&place);
        if let Some(places) = self.holders.places.get_mut(room_id) {
            places.retain(|(holder, held)| (*holder, held) != (run, event_id));
        }
        self.grant_waits();
    }

    /// Checks that `run` calls a channel for `event_id` in the turn of the
    /// event's place: one event's set completes before the next one's
    /// begins (RFC §10.2). An event whose place could not be taken, or whose
    /// turn could not be awaited, is delivered out of the lane.
    pub(super) fn assert_in_turn(&self, run: RunId, room_id: &RoomId, event_id: &EventId) {
        if self.placeless.contains(event_id) {
            return;
        }
        let place = (room_id.clone(), event_id.clone());
        assert!(
            self.state(run).turns.contains(&place),
            "a channel was called for {event_id} outside its place's turn"
        );
    }

    /// Takes the room's next ticket for the current run: it never waits.
    pub(super) fn take_a_ticket(&mut self, room_id: RoomId) {
        let run = self.current;
        assert!(
            self.state_mut(run).tickets.insert(room_id.clone()),
            "the room's ticket was taken twice"
        );
        self.holders.tickets.entry(room_id).or_default().push(run);
    }

    /// The turn of the current run's ticket of the room: at once when no
    /// earlier ticket is held, once they are released otherwise.
    pub(super) fn turn(
        &mut self,
        effect: EffectId,
        room_id: RoomId,
        deadline: Option<Timestamp>,
    ) -> Option<Outcome> {
        let run = self.current;
        assert!(
            self.state(run).tickets.contains(&room_id),
            "a ticket's turn was awaited without the ticket"
        );
        if !self.is_first(run, &room_id) {
            return self.wait(effect, deadline, Awaited::Turn(room_id));
        }
        self.admit(run, room_id);
        Some(Outcome::Hold(HoldOutcome::Admitted))
    }

    fn admit(&mut self, run: RunId, room_id: RoomId) {
        self.trace.push(format!("await ticket {room_id}"));
        self.state_mut(run).admitted.insert(room_id);
    }

    /// Whether no ticket of the room taken before `run`'s is held.
    fn is_first(&self, run: RunId, room_id: &RoomId) -> bool {
        self.holders
            .tickets
            .get(room_id)
            .and_then(|order| order.first())
            .is_none_or(|first| *first == run)
    }

    /// Lets the current run's ticket of the room go, which it must hold.
    pub(super) fn let_ticket_go(&mut self, room_id: &RoomId) {
        let run = self.current;
        let state = self.state_mut(run);
        assert!(
            state.tickets.remove(room_id),
            "the room's ticket was released without being held"
        );
        state.admitted.remove(room_id);
        if let Some(order) = self.holders.tickets.get_mut(room_id) {
            order.retain(|holder| *holder != run);
        }
        self.grant_waits();
    }

    /// Makes `effect` of the current run wait for `awaited`.
    fn wait(
        &mut self,
        effect: EffectId,
        deadline: Option<Timestamp>,
        awaited: Awaited,
    ) -> Option<Outcome> {
        self.holders.waits.push(Wait {
            run: self.current,
            effect,
            deadline,
            awaited,
        });
        None
    }

    /// Grants every waiting request whose holder let go of what it waits
    /// for, in the order they came; one past its deadline is given up on.
    fn grant_waits(&mut self) {
        while let Some(index) = self
            .holders
            .waits
            .iter()
            .position(|wait| self.is_free(wait))
        {
            let wait = self.holders.waits.remove(index);
            let now = self.env.now();
            if wait.deadline.is_some_and(|deadline| now > deadline) {
                self.give_up(&wait);
                continue;
            }
            let outcome = match wait.awaited {
                Awaited::Lock(room_id) => {
                    self.take_lock(wait.run, room_id);
                    Outcome::Hold(HoldOutcome::Locked)
                }
                Awaited::Place(room_id, event_id) => {
                    self.give_turn(wait.run, room_id, event_id);
                    Outcome::Hold(HoldOutcome::LaneTurn)
                }
                Awaited::Turn(room_id) => {
                    self.admit(wait.run, room_id);
                    Outcome::Hold(HoldOutcome::Admitted)
                }
            };
            self.post(wait.run, wait.effect, outcome);
        }
    }

    /// Whether what `wait` waits for is free for it.
    fn is_free(&self, wait: &Wait) -> bool {
        match &wait.awaited {
            Awaited::Lock(room_id) => !self.holders.locks.contains_key(room_id),
            Awaited::Place(room_id, event_id) => self.is_first_place(wait.run, room_id, event_id),
            Awaited::Turn(room_id) => self.is_first(wait.run, room_id),
        }
    }

    /// Gives up on the waiting request whose deadline comes first, the
    /// clock moved to it (RFC §13.6); `false` when none has a deadline.
    fn expire_first_wait(&mut self) -> bool {
        let Some(index) = self
            .holders
            .waits
            .iter()
            .enumerate()
            .filter_map(|(index, wait)| wait.deadline.map(|deadline| (deadline, index)))
            .min()
            .map(|(_, index)| index)
        else {
            return false;
        };
        let wait = self.holders.waits.remove(index);
        if let Some(deadline) = wait.deadline {
            let now = self.env.now();
            self.env
                .advance(deadline.duration_since(now).unwrap_or_default());
        }
        self.give_up(&wait);
        true
    }

    /// Answers `wait` past its deadline: what it waited for is not taken.
    fn give_up(&mut self, wait: &Wait) {
        self.trace
            .push(format!("{} deadline passed", wait.awaited.line()));
        self.post(wait.run, wait.effect, Outcome::DeadlinePassed);
    }

    /// Notes `effect` when it is a commit that decides its call: the text
    /// it commits, whether the result it decides is that event's, blocked
    /// or not, and its reason.
    fn note_deciding(&mut self, effect: &Effect) {
        let (Request::Store(StoreRequest::Commit { event, .. }), Some(result)) =
            (&effect.request, &effect.decides)
        else {
            return;
        };
        let own = result.event.as_ref() == Some(&**event);
        let decided = if result.blocked {
            "blocked"
        } else {
            "delivered"
        };
        let line = format!(
            "{}: {decided} {:?}{}",
            text_of(&event.content),
            result.reason.as_deref().unwrap_or_default(),
            if own { "" } else { " for another event" }
        );
        self.deciding.push(line);
    }

    /// Executes `effect` for the current run, or, for a hook or a channel
    /// that calls into its room, hands the call on and keeps the effect
    /// outstanding until its caller is answered.
    fn execute_effect(&mut self, effect: Effect) {
        let run = self.current;
        if let Some(room) = &effect.locked {
            assert!(
                self.state(run).locks.contains(room),
                "a request made under the lock of {room} while the lock is free"
            );
            let line = format!("{} under {room}", self.label(&effect.request));
            self.under_lock.push(line);
        }
        if let Some(room) = &effect.admitted {
            let line = format!("{} in {room}", self.label(&effect.request));
            self.in_admission_order.push(line);
        }
        self.note_deciding(&effect);
        let traced = self.trace.len();
        if let Some((caller, entries)) = self.reentry(&effect) {
            self.make_calls(&effect, &caller, entries);
            return;
        }
        let outcome = self.execute_timed(effect.id, effect.request, effect.deadline);
        self.journal.extend(self.trace.iter().skip(traced).cloned());
        if let Some(outcome) = outcome {
            self.post(run, effect.id, outcome);
        }
    }

    /// Hands on the calls a hook or a channel makes while it runs `effect`,
    /// and answers `effect` at once for a caller that does not wait.
    fn make_calls(&mut self, effect: &Effect, caller: &Caller, entries: Vec<Entry>) {
        let run = self.current;
        let label = self.label(&effect.request);
        let frame = Frame::of(run, effect);
        let (context, at_once) = match self.then_of(caller) {
            Then::Waits => (self.context_of(&frame), None),
            Then::AtOnce(outcome) => (self.context_of(&frame), Some(outcome)),
            Then::Apart(outcome) => (Vec::new(), Some(outcome)),
        };
        assert!(
            at_once.is_some() || entries.len() == 1,
            "a caller that waits makes one call"
        );
        for entry in entries {
            self.note(format!("{label} calls {}", super::entry_name(&entry)));
            let call = Call {
                context: context.clone(),
                frame: frame.clone(),
                waits: at_once.is_none(),
                label: label.clone(),
                caller: caller.clone(),
            };
            self.place_call(entry, call);
        }
        if let Some(outcome) = at_once {
            self.note(format!("{label} returns"));
            self.post(run, effect.id, outcome);
        }
    }
}
