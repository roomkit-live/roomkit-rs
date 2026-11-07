//! The calls into the room that the operation's own work makes (RFC §13.5,
//! §9.5.1 Reentrance, §10.1 step 18, gap G-070).
//!
//! A hook or a parse of the operation that calls `send_event` or
//! `process_inbound` for the operation's room, under the room's lock or in
//! its admission order, would, run apart, wait for what the operation
//! holds: the room lock its section holds, the admission ticket its check
//! holds or is about to take. The driver hands such a call to the operation
//! instead: it is adopted, as an entry of its own, with its own result
//! (RFC §9.5.1 Reentrance). A call from a turn or a delivery waits for
//! nothing the operation holds: it is a new arrival, its own operation,
//! drawing on the reentry budget of the cascade that delivers
//! (`Inbound::drawing_on`, gap G-070). Where an entry runs depends on the
//! call that made it, its origin:
//!
//! - a call the operation made under the room's lock (`Effect::locked`): a
//!   hook a pass runs, or any call of an entry nested already. The entry is
//!   nested in the section that holds the lock. It takes no lock of its own
//!   (`Io` answers its requests for it), commits ahead of the section's
//!   event, which takes the room's next index once its hooks are over, and
//!   the call's own answer is held back until the entry is settled, so the
//!   section never goes on, nor lets the lock go, while an entry nested in
//!   it works. Two entries nested under one call run one after the other,
//!   never as two sections within one tenure;
//! - a call made in the room's admission order (`Effect::admitted`: an
//!   off-lock hook, a message's parse, any call of an entry beside already):
//!   the entry runs beside the operation's work, taking the room lock as any
//!   section does, after the operation's other section that holds it, if
//!   any, and every request it makes stands in the room's admission order.
//!
//! Either way the entry takes no admission ticket and runs every hook under
//! the lock: the operation it joins stands in the room's order already (RFC
//! §9.5.1). It has a deadline of its own, its `process_timeout` from its
//! adoption, as every call's runs from its start: past it, before its
//! commit, it is refused `process_timeout` and reports it (RFC §13.6). It
//! follows its caller, the code that waits for its answer: once that code
//! is gone (the hook that made it cut at the call's deadline or at its own
//! timeout, or dropped with the call's caller), the entry ends with it
//! before its commit, nothing of it stored and nothing reported, as the
//! Python implementation cancels a hook's body and the call it awaits; an
//! entry committed by then goes on. It draws on the operation's reentry budget, past which it is
//! stored `BLOCKED` with `reentry_loop_cap` (RFC §8.3); a message is charged
//! first thing, before its channel is looked up, and refused with no event
//! past the budget (RFC §10.1 step 1). Its caller receives its result at
//! its commit (detached completion: no delivery results, no completion
//! handle), an entry beside the work once the events its hooks injected are
//! committed too, a message's result carrying their failed release (HITL
//! b779fc14), and its event takes its place in the room's lane at that
//! commit. A message's arrival runs its step 2, then its channel's parse. A
//! direct injection is admitted and stamped at once, and its room, then its
//! key, when it carries one, read before it draws on the budget: a call its
//! room refuses and a replay draw nothing (RFC §9.5.1 Past the cap, §13.4,
//! §17.2).

use std::collections::BTreeSet;
use std::mem;

use super::arrival::{self, Arrival};
use super::cascade::{self, entry_result, replay_result};
use super::deadline::{self, PROCESS_TIMEOUT, Stopped};
use super::direct::{self, DirectCall};
use super::greeting;
use super::host_post::HostPost;
use super::identity::{Held, Identified, Participation};
use super::pass::{CommitPass, PassOrigin, PassReport, PassResult};
use super::queues::Lineage;
use super::record::{RecordPass, RecordReport};
use super::work_read::{Read, WorkRead};
use super::{InboundError, InboundResult, Machine, MessageAnswers, result};
use crate::model::{
    ChannelId, ErrorRecord, EventId, InboundMessage, InjectedEvent, Post, RoomEvent, RoomId,
    Timestamp,
};
use crate::ops::io::{Bound, Io};
use crate::ops::protocol::{EffectId, Entry, EntryId, Notice, Outcome, SpanNote};
use crate::ops::{Abort, Cx, FrameworkEvent, KitConfig, Progress, RefusedOperation};
use crate::rules::chain_depth::{self, Draw};

/// The entries adopted from the operation's own work.
#[derive(Debug, Default)]
pub(super) struct Adoptions {
    /// The last id given out.
    last: Option<EntryId>,
    /// Adopted since the last poll: the next poll starts them.
    adopted: Vec<(Adopted, Box<Entry>)>,
    running: Vec<AdoptedRun>,
    /// The answers of the hooks entries are nested under, held back until
    /// those entries are settled.
    deferred: Vec<(EffectId, Outcome)>,
    /// The entries cut for their caller gone, their results held back
    /// until the effect whose code made each one is answered too.
    cut: Vec<(Adopted, InboundResult)>,
    /// The entries whose caller is gone (`Machine::entry_caller_gone`): cut
    /// before their commit, they end without a report.
    gone: BTreeSet<EntryId>,
    /// The entries committed whose answer waits for the commits of the
    /// events their hooks injected (`Machine::answer_owed`).
    pending: Vec<Pending>,
    /// Each entry nested in the section of an adopted entry's pass, and the
    /// entry beside the operation's work whose answer waits for what its
    /// hooks inject: that section's entry, or the one that entry is owed
    /// to, as Python's nested call queues them on that section (RFC §9.4,
    /// §9.5.1 Reentrance).
    owners: Vec<(EntryId, EntryId)>,
}

/// An adopted entry committed, whose answer waits for the commits of the
/// events its hooks injected, and of those their hooks injected, as
/// Python's adopted call commits them before it returns (RFC §9.4, §9.5.1
/// Reentrance, HITL b779fc14): what it answers then.
#[derive(Debug)]
struct Pending {
    entry: Adopted,
    result: InboundResult,
    /// Whether the entry's delivery set follows; with none, its delivery is
    /// over with its answer.
    delivers: bool,
}

/// What the operation knows of an entry it adopted.
#[derive(Clone, Debug)]
struct Adopted {
    id: EntryId,
    /// The room the call names, the operation's.
    room_id: RoomId,
    /// The effect whose call made the entry.
    origin: EffectId,
    /// The room whose lock the entry is nested under: its origin is a hook
    /// a pass runs under that lock.
    nested: Option<RoomId>,
    /// Whether the entry is a direct injection, whose refusal before its
    /// commit is an error (RFC §10.5).
    direct: bool,
    /// The first failure of the release of a lock the entry's step 2 took,
    /// once its message arrived: its result's error when it met none
    /// before (RFC §13.5), as Python's nested call collects its own.
    release_error: Option<ErrorRecord>,
    /// What bounds the entry up to its commit, set once it is stamped: its
    /// own deadline, from its adoption (RFC §13.6), or, for an entry that
    /// follows its origin, that origin's deadline when it has one.
    bound: Bound,
    /// Whose deadline `bound` holds.
    deadline: Deadline,
    /// The deadline of the work its origin is, when it has one: the call's
    /// for work before the call's decision, an entry's for that entry's.
    origin_deadline: Option<Timestamp>,
    /// The channel its event comes from, which its `process_timeout` names
    /// before the room lock (RFC §8.2).
    channel_id: Option<ChannelId>,
}

/// Whose deadline bounds an adopted entry up to its commit (RFC §13.6,
/// §9.5.1 Reentrance).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Deadline {
    /// None yet: it is set on the poll after the adoption. `follows` says
    /// whether the code of the entry's origin made it in its own task, and
    /// awaits it (`Machine::entry_follows_its_origin`).
    Unset { follows: bool },
    /// Its own, its `process_timeout` from its adoption: past it, it reports
    /// its `process_timeout`.
    Own,
    /// Its origin's, which cuts the origin's code with it: past it, nothing
    /// reports the entry.
    Borrowed,
}

/// One adopted entry at work.
#[derive(Debug)]
struct AdoptedRun {
    entry: Adopted,
    state: RunState,
}

#[derive(Debug)]
enum RunState {
    /// A message's steps 2 to 4.
    Arriving(Box<Arrival>),
    /// A post's read of its room and its host channel's first attach.
    Posting(Box<HostPost>),
    /// A direct injection's room, then its key, read before it draws on
    /// the budget.
    ReadingWork(Box<WorkRead>),
    /// The entry's pass.
    Passing(Box<CommitPass>),
    /// The entry stored blocked, past its caller's reentry budget.
    Recording(Box<RecordPass>),
}

/// Where an adopted entry stands after an answer.
enum Advanced {
    Waiting,
    /// The message arrived, with what step 5 decided of its sender.
    Arrived(Box<RoomEvent>, Identified),
    /// A post's room took it: it is injected.
    Posted,
    /// A direct injection's room and key were read: the room took it, as
    /// a replay or not.
    WorkRead(Read),
    /// The entry stopped before its pass, with what its caller receives.
    Stopped(Box<Result<InboundResult, InboundError>>),
    /// The call's deadline cut the entry before its pass.
    Cut(Box<InboundResult>),
    Passed(Result<PassReport, Abort>),
    Recorded(Result<RecordReport, Abort>),
}

impl Adoptions {
    /// Whether no entry is adopted, running or cut and not settled yet.
    pub(super) fn is_idle(&self) -> bool {
        self.adopted.is_empty() && self.running.is_empty() && self.cut.is_empty()
    }

    /// Whether no entry's answer waits for what its hooks injected.
    pub(super) fn answers_none(&self) -> bool {
        self.pending.is_empty()
    }

    /// Whether the answer of `entry` waits for what its hooks injected.
    pub(super) fn is_pending(&self, entry: EntryId) -> bool {
        self.pending.iter().any(|pending| pending.entry.id == entry)
    }

    /// The entry beside the operation's work that what the hooks of a call
    /// nested in the section of `entry`'s pass inject is owed to: `entry`
    /// itself when it stands beside, else the one it is owed to.
    fn owing(&self, entry: EntryId) -> Option<EntryId> {
        if let Some(owner) = self.owner_of(entry) {
            return Some(owner);
        }
        self.running
            .iter()
            .any(|run| run.entry.id == entry && run.entry.nested.is_none())
            .then_some(entry)
    }

    fn owner_of(&self, entry: EntryId) -> Option<EntryId> {
        self.owners
            .iter()
            .find(|(nested, _)| *nested == entry)
            .map(|(_, owner)| *owner)
    }

    /// The entry `entry`'s injected events are owed to, when `entry` is
    /// nested in the section of one still to be answered.
    fn owed_to(&self, entry: EntryId) -> Option<EntryId> {
        let owner = self.owner_of(entry)?;
        let unanswered =
            self.running.iter().any(|run| run.entry.id == owner) || self.is_pending(owner);
        unanswered.then_some(owner)
    }

    /// Notes `error`, the failed release of a pass the answer of `entry`
    /// waits for, on that answer, when it met no failure before (RFC
    /// §13.5); whether it did. A direct injection's answer is its event: the
    /// failure is left to its caller's call, as Python's `send_event`
    /// collects none of its own.
    pub(super) fn note_pending_error(
        &mut self,
        entry: EntryId,
        error: Option<ErrorRecord>,
    ) -> bool {
        let Some(pending) = self
            .pending
            .iter_mut()
            .find(|pending| pending.entry.id == entry && !pending.entry.direct)
        else {
            return false;
        };
        if pending.result.error.is_none() {
            pending.result.error = error;
        }
        true
    }

    /// Whether a running entry waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        self.running.iter().any(|run| run.awaits(effect))
    }

    /// Whether `effect` is a hook some entry is nested under, not settled
    /// yet: its answer waits for it.
    pub(super) fn defers(&self, effect: EffectId) -> bool {
        self.adopted
            .iter()
            .map(|(adopted, _)| adopted)
            .chain(self.running.iter().map(|run| &run.entry))
            .any(|adopted| adopted.nested.is_some() && adopted.origin == effect)
    }

    /// Holds `outcome` back until the entries nested under `effect` are
    /// settled.
    pub(super) fn defer(&mut self, effect: EffectId, outcome: Outcome) {
        self.deferred.push((effect, outcome));
    }

    /// Whether an entry nested under `origin` runs: another one nested
    /// under it waits for its settlement.
    fn nests_under(&self, origin: EffectId) -> bool {
        self.running
            .iter()
            .any(|run| run.entry.nested.is_some() && run.entry.origin == origin)
    }

    /// The answers held back for `origin`, once no entry is nested under it.
    fn released(&mut self, origin: EffectId) -> Vec<(EffectId, Outcome)> {
        if self.defers(origin) {
            return Vec::new();
        }
        let (released, kept) = mem::take(&mut self.deferred)
            .into_iter()
            .partition(|(effect, _)| *effect == origin);
        self.deferred = kept;
        released
    }

    fn next_id(&mut self) -> EntryId {
        let id = self.last.map_or_else(EntryId::first, EntryId::next);
        self.last = Some(id);
        id
    }
}

impl AdoptedRun {
    fn awaits(&self, effect: EffectId) -> bool {
        match &self.state {
            RunState::Arriving(arrival) => arrival.awaits(effect),
            RunState::Posting(post) => post.awaits(effect),
            RunState::ReadingWork(read) => read.awaits(effect),
            RunState::Passing(pass) => pass.awaits(effect),
            RunState::Recording(record) => record.awaits(effect),
        }
    }

    /// The failure of a release the entry's step 2 met, taken from its
    /// arrival once its message arrived or stopped arriving (`advanced`):
    /// its result's error (RFC §13.5).
    fn arrival_release_error(&mut self, advanced: &Advanced) -> Option<ErrorRecord> {
        let RunState::Arriving(arrival) = &mut self.state else {
            return None;
        };
        if matches!(advanced, Advanced::Waiting) {
            return None;
        }
        arrival.take_release_error()
    }

    /// Hands the entry the answer to one of its effects.
    fn accept(&mut self, cx: &mut Cx<'_>, effect: EffectId, outcome: Outcome) -> Advanced {
        match &mut self.state {
            RunState::Arriving(arrival) => match arrival.accept(cx, effect, outcome) {
                Progress::Waiting => Advanced::Waiting,
                Progress::Done(Ok(arrived)) => {
                    Advanced::Arrived(Box::new(arrived.event), arrival.take_identified())
                }
                Progress::Done(Err(Stopped::Failed(error))) => {
                    Advanced::Stopped(Box::new(Err(error)))
                }
                Progress::Done(Err(Stopped::Expired)) => {
                    Advanced::Cut(Box::new(cut_at_the_deadline(false, None)))
                }
                // An adopted message is charged as its arrival starts.
                Progress::Done(Err(Stopped::PastBudget)) => {
                    Advanced::Stopped(Box::new(Ok(result::past_the_budget())))
                }
                Progress::Done(Err(Stopped::Refused(refused))) => {
                    Advanced::Stopped(Box::new(Ok(*refused)))
                }
            },
            RunState::Posting(post) => match post.accept(cx, effect, outcome) {
                Progress::Waiting => Advanced::Waiting,
                Progress::Done(Ok(())) => Advanced::Posted,
                Progress::Done(Err(Stopped::Failed(error))) => {
                    Advanced::Stopped(Box::new(Err(error)))
                }
                Progress::Done(Err(Stopped::Expired)) => {
                    Advanced::Cut(Box::new(cut_at_the_deadline(false, None)))
                }
                // A post stops expired or failed only.
                Progress::Done(Err(Stopped::PastBudget | Stopped::Refused(_))) => {
                    Advanced::Stopped(Box::new(Err(InboundError::Store(
                        Abort::expired().into_record(),
                    ))))
                }
            },
            RunState::ReadingWork(read) => match read.accept(cx, effect, outcome) {
                Progress::Waiting => Advanced::Waiting,
                Progress::Done(Ok(read)) => Advanced::WorkRead(read),
                Progress::Done(Err(Stopped::Failed(error))) => {
                    Advanced::Stopped(Box::new(Err(error)))
                }
                Progress::Done(Err(Stopped::Expired)) => {
                    Advanced::Cut(Box::new(cut_at_the_deadline(false, None)))
                }
                // A read stops expired or failed only.
                Progress::Done(Err(Stopped::PastBudget | Stopped::Refused(_))) => {
                    Advanced::Stopped(Box::new(Err(InboundError::Store(
                        Abort::expired().into_record(),
                    ))))
                }
            },
            RunState::Passing(pass) => match pass.accept(cx, effect, outcome) {
                Progress::Waiting => Advanced::Waiting,
                Progress::Done(passed) => Advanced::Passed(passed),
            },
            RunState::Recording(record) => match record.accept(cx, effect, outcome) {
                Progress::Waiting => Advanced::Waiting,
                Progress::Done(recorded) => Advanced::Recorded(recorded),
            },
        }
    }
}

/// The result of an entry its call's deadline cut before its commit (RFC
/// §13.6, §9.5.1 Reentrance): refused `process_timeout` with no event,
/// `locked` when its pass held the room lock by then, that release's failure
/// its error (RFC §13.5). Nothing reports it: the call's own expiry reports
/// its `process_timeout`, and the hook whose code made the entry is cut with
/// it, its caller gone as a rule.
fn cut_at_the_deadline(locked: bool, release_failure: Option<ErrorRecord>) -> InboundResult {
    InboundResult {
        blocked: true,
        reason: Some(PROCESS_TIMEOUT.to_owned()),
        refused_before_the_lock: !locked,
        error: release_failure,
        ..InboundResult::default()
    }
}

/// Reports the expiry of `adopted`, past its own deadline before its commit
/// (RFC §13.6, §8.2): `process_timeout`, naming the channel its event comes
/// from before the room lock and `locked_event`, the event whose pass held
/// the lock, under it, as a call's own expiry does.
fn report_entry_expiry(cx: &mut Cx<'_>, adopted: &Adopted, locked_event: Option<EventId>) {
    let timeout = cx
        .config
        .process_timeout
        .map_or(0.0, |timeout| timeout.as_secs_f64());
    let channel_id = match locked_event {
        Some(_) => None,
        None => adopted.channel_id.clone(),
    };
    tracing::info!(
        target: "roomkit::core::inbound",
        room_id = %adopted.room_id,
        channel_id = ?channel_id,
        event_id = ?locked_event,
        timeout,
        "a call the room's work adopted passed its process_timeout before its commit; nothing of its event is written"
    );
    cx.io
        .notify(Notice::FrameworkEvent(FrameworkEvent::ProcessTimeout {
            room_id: Some(adopted.room_id.clone()),
            channel_id,
            event_id: locked_event,
            timeout,
            operation: None,
        }));
}

/// The channel the event of `entry` comes from, as its call names it.
fn channel_of(entry: &Entry) -> Option<ChannelId> {
    match entry {
        Entry::Message { message, .. } => Some(message.channel_id.clone()),
        Entry::Direct { event, .. } => Some(event.channel_id.clone()),
        Entry::Post { post, .. } => post.channel_id.clone(),
        Entry::Greeting { .. } | Entry::Row { .. } => None,
    }
}

impl Adopted {
    /// Sets the entry's bound, once: its own deadline, its
    /// `process_timeout` from `now`, its adoption's poll, as every call's
    /// runs from its start (RFC §13.6), none for a greeting, which takes
    /// none (RFC §19.1); for an entry that follows its origin, or is nested
    /// in the section its origin runs in, that origin's deadline when it
    /// has one, which passes first and cuts the origin's code with the
    /// entry, as Python's outer deadline cancels the call its hook awaits
    /// before that call's own.
    fn stamp(&mut self, config: &KitConfig, now: Timestamp, entry: &Entry) {
        let Deadline::Unset { follows } = self.deadline else {
            return;
        };
        let own = match entry {
            Entry::Greeting { .. } => None,
            Entry::Message { .. }
            | Entry::Direct { .. }
            | Entry::Post { .. }
            | Entry::Row { .. } => deadline::of_call(config, now),
        };
        // One nested under its origin's lock holds that section, which
        // cannot outlive the origin's deadline.
        let borrows = follows || self.nested.is_some();
        let (deadline, at) = match self.origin_deadline {
            Some(origin) if borrows => (Deadline::Borrowed, Some(origin)),
            Some(_) | None => (Deadline::Own, own),
        };
        self.deadline = deadline;
        self.bound = Bound::Adopted(self.id, at);
    }
}

/// Runs `advance` for `adopted`: nested under the lock of the section its
/// origin runs under, its requests for that lock answered at once, or beside
/// the operation's work, every request it makes in the room's admission
/// order (RFC §9.5.1 Reentrance).
fn within<T>(cx: &mut Cx<'_>, adopted: &Adopted, advance: impl FnOnce(&mut Cx<'_>) -> T) -> T {
    let beside = adopted.nested.is_none().then(|| adopted.room_id.clone());
    let outer_lock = cx.io.nest(adopted.nested.clone());
    let outer_order = cx.io.admit(beside);
    let advanced = advance(cx);
    cx.io.admit(outer_order);
    cx.io.nest(outer_lock);
    advanced
}

impl Machine {
    /// Adopts `entry`, a call into the room made by the code effect
    /// `origin` runs (RFC §13.5, §9.5.1 Reentrance): nested in the section
    /// whose lock `origin` was made under, beside the operation's work when
    /// `origin` stands in the room's admission order.
    ///
    /// # Errors
    ///
    /// Hands `entry` back when `origin` is not outstanding, or was made
    /// neither under the room's lock nor in its admission order: a turn's or
    /// a delivery's call is a new arrival.
    pub(super) fn adopt(
        &mut self,
        io: &Io,
        entry: Entry,
        origin: EffectId,
    ) -> Result<EntryId, Box<Entry>> {
        if matches!(entry, Entry::Row { .. }) {
            let id = self.adopted.next_id();
            return self.cascade.take_row(io, id, entry, origin);
        }
        let room_id = entry.room_id();
        let nested = io.locked_for(origin) == Some(room_id);
        let beside = io.admitted_for(origin) == Some(room_id);
        if !io.is_outstanding(origin) || !(nested || beside) {
            return Err(Box::new(entry));
        }
        let nested = nested.then(|| room_id.clone());
        let id = self.adopted.next_id();
        // Nested in an adopted entry's own section, its origin works within
        // that entry's bound.
        if let Bound::Adopted(section, _) = io.bound_of(origin)
            && nested.is_some()
            && let Some(owner) = self.adopted.owing(section)
        {
            self.adopted.owners.push((id, owner));
        }
        let adopted = Adopted {
            id,
            room_id: entry.room_id().clone(),
            origin,
            nested,
            direct: matches!(entry, Entry::Direct { .. } | Entry::Post { .. }),
            release_error: None,
            bound: Bound::Adopted(id, None),
            deadline: Deadline::Unset { follows: false },
            origin_deadline: io.bound_of(origin).deadline(),
            channel_id: channel_of(&entry),
        };
        self.adopted.adopted.push((adopted, Box::new(entry)));
        Ok(id)
    }

    /// Starts the entries adopted since the last poll, but an entry nested
    /// under a call another entry nested under it still runs for: it waits
    /// for that one's settlement.
    pub(super) fn start_adopted(&mut self, cx: &mut Cx<'_>) {
        let now = cx.env.now();
        for (adopted, entry) in &mut self.adopted.adopted {
            adopted.stamp(cx.config, now, entry);
        }
        // An entry whose caller left before it started ends unstarted.
        let (gone, waiting): (Vec<_>, Vec<_>) = mem::take(&mut self.adopted.adopted)
            .into_iter()
            .partition(|(adopted, _)| self.adopted.gone.contains(&adopted.id));
        self.adopted.adopted = waiting;
        for (adopted, _) in gone {
            self.settle_cut(cx, adopted, cut_at_the_deadline(false, None), None);
        }
        // One at a time: the others stay adopted, and hold back the answer
        // of the call they are nested under, while one starts.
        while let Some(index) = self.adopted.adopted.iter().position(|(adopted, _)| {
            adopted.nested.is_none() || !self.adopted.nests_under(adopted.origin)
        }) {
            let (adopted, entry) = self.adopted.adopted.remove(index);
            self.start_entry(cx, adopted, *entry);
        }
    }

    fn start_entry(&mut self, cx: &mut Cx<'_>, adopted: Adopted, entry: Entry) {
        match entry {
            Entry::Direct {
                room_id,
                organization_id,
                event,
            } => match direct::admit(cx, room_id, *event) {
                Ok(direct::Admitted::Event(event)) => {
                    let call = DirectCall { organization_id };
                    self.read_key(cx, adopted, *event, call);
                }
                Ok(direct::Admitted::Refused(refused)) => {
                    self.settle_entry(cx, &adopted, Ok(*refused));
                }
                Err(error) => self.settle_entry(cx, &adopted, Err(error)),
            },
            Entry::Message {
                room_id,
                organization_id,
                message,
                answers,
            } => {
                let answers = answers.map(|answers| *answers);
                self.arrive(cx, adopted, room_id, organization_id, *message, answers);
            }
            Entry::Post {
                room_id,
                organization_id,
                post,
            } => self.post(cx, adopted, room_id, organization_id, *post),
            Entry::Greeting {
                room_id,
                agent_id,
                text,
                chain_depth,
            } => {
                let event = greeting::event(cx, room_id, agent_id, text, chain_depth);
                self.greet_within(cx, adopted, event);
            }
            // A row is taken by its stream's read as it is adopted.
            Entry::Row { .. } => {
                debug_assert!(false, "a row is never started as an adopted entry");
            }
        }
    }

    /// A greeting made in the room's own work is that work's write: it draws
    /// a pass of the budget it joins, and past it is stored `BLOCKED`, no hook
    /// run and no delivery (RFC §8.3, §19.1).
    fn greet_within(&mut self, cx: &mut Cx<'_>, adopted: Adopted, mut event: RoomEvent) {
        let state = if self.cascade.budget.take_pass() {
            let pass = CommitPass::new(PassOrigin::Adopted(adopted.id), event).greeting();
            let mut pass = Box::new(pass.bounded(adopted.bound));
            within(cx, &adopted, |cx| pass.start(cx));
            RunState::Passing(pass)
        } else {
            event.blocked_by = Some(chain_depth::REENTRY_LOOP_CAP.to_owned());
            let record = RecordPass::refusing(event, RefusedOperation::Inbound);
            let mut record = Box::new(record.waiting_within(adopted.bound));
            within(cx, &adopted, |cx| record.start(cx));
            RunState::Recording(record)
        };
        self.adopted.running.push(AdoptedRun {
            entry: adopted,
            state,
        });
    }

    /// A post's way to its injection (RFC §10.5): its room read, its speaker
    /// checked and its host channel attached on its first post, within the
    /// adoption, then its key read as any injection's.
    fn post(
        &mut self,
        cx: &mut Cx<'_>,
        adopted: Adopted,
        room_id: RoomId,
        organization_id: Option<String>,
        post: Post,
    ) {
        let (event, binding) = match direct::admit_post(cx, room_id, post) {
            Ok((direct::Admitted::Event(event), binding)) => (*event, binding),
            Ok((direct::Admitted::Refused(refused), _)) => {
                return self.settle_entry(cx, &adopted, Ok(*refused));
            }
            Err(error) => return self.settle_entry(cx, &adopted, Err(error)),
        };
        let call = DirectCall { organization_id };
        let post = within(cx, &adopted, |cx| {
            HostPost::start(cx, event, call, binding, adopted.bound)
        });
        self.adopted.running.push(AdoptedRun {
            entry: adopted,
            state: RunState::Posting(Box::new(post)),
        });
    }

    /// The post's room took it: its key is read, then it goes to its pass.
    fn on_entry_posted(&mut self, cx: &mut Cx<'_>, adopted: Adopted, run: AdoptedRun) {
        let RunState::Posting(post) = run.state else {
            debug_assert!(false, "a post answered an entry that posts nothing");
            return;
        };
        let (event, call) = post.into_parts();
        self.read_key(cx, adopted, event, call);
    }

    /// A message's steps 1 to 4, in the operation's room, once admitted
    /// (`arrival::admit`). It is charged on the operation's budget before
    /// its parse: a channel whose parse calls into its room for every
    /// message is bounded too.
    fn arrive(
        &mut self,
        cx: &mut Cx<'_>,
        adopted: Adopted,
        room_id: RoomId,
        organization_id: Option<String>,
        message: InboundMessage,
        answers: Option<MessageAnswers>,
    ) {
        let pass = Draw::Pass(self.cascade.budget.clone());
        let (channel, message) = match arrival::admit(cx.config, message, Some(&pass)) {
            Ok(arrival::Admitted::Message { channel, message }) => (*channel, *message),
            Ok(arrival::Admitted::Refused(refused)) => {
                return self.settle_entry(cx, &adopted, Ok(*refused));
            }
            Err(error) => return self.settle_entry(cx, &adopted, Err(error)),
        };
        let arrival = Arrival::adopted(
            adopted.id,
            room_id,
            organization_id,
            channel,
            message,
            adopted.bound,
        );
        let mut arrival = Box::new(arrival.answered(answers));
        within(cx, &adopted, |cx| arrival.start(cx));
        self.adopted.running.push(AdoptedRun {
            entry: adopted,
            state: RunState::Arriving(arrival),
        });
    }

    /// A direct injection's room, then its key, when it carries one, are
    /// read before it draws on its caller's budget (RFC §9.5.1 Past the
    /// cap, §13.4, §17.2).
    fn read_key(&mut self, cx: &mut Cx<'_>, adopted: Adopted, event: RoomEvent, call: DirectCall) {
        let read = within(cx, &adopted, |cx| {
            WorkRead::start(cx, event, call, adopted.bound)
        });
        self.adopted.running.push(AdoptedRun {
            entry: adopted,
            state: RunState::ReadingWork(Box::new(read)),
        });
    }

    /// The entry's room and key are read: a replay answers its caller with
    /// the event the key committed, drawing nothing; any other event goes to
    /// its pass.
    fn on_entry_key(&mut self, cx: &mut Cx<'_>, adopted: Adopted, run: AdoptedRun, read: Read) {
        if let Read::Replay(original) = read {
            return self.settle_entry(cx, &adopted, Ok(replay_result(&original)));
        }
        let RunState::ReadingWork(read) = run.state else {
            debug_assert!(false, "a key read answered an entry that read none");
            return;
        };
        let (event, call) = read.into_parts();
        self.enter_pass(cx, adopted, event, Some(call), None);
    }

    /// The entry's event goes through its pass, within its caller's reentry
    /// budget (RFC §8.3), charged already for a message, whose arrival took
    /// it, and when it is `direct`, an injection's, for that call; past the
    /// budget, it is stored blocked.
    fn enter_pass(
        &mut self,
        cx: &mut Cx<'_>,
        adopted: Adopted,
        mut event: RoomEvent,
        direct: Option<DirectCall>,
        participation: Option<Participation>,
    ) {
        let charged = direct.is_none();
        let state = if charged || self.cascade.budget.take_pass() {
            if !charged {
                let entry = Some(adopted.id);
                cx.io.notify(Notice::Span(SpanNote::Injecting { entry }));
            }
            let pass = CommitPass::new(PassOrigin::Adopted(adopted.id), event)
                .bounded(adopted.bound)
                .checking(direct)
                .participating(participation);
            let mut pass = Box::new(pass);
            within(cx, &adopted, |cx| pass.start(cx));
            RunState::Passing(pass)
        } else {
            event.blocked_by = Some(chain_depth::REENTRY_LOOP_CAP.to_owned());
            let record = RecordPass::refusing(event, RefusedOperation::Inbound)
                .checking(direct)
                .waiting_within(adopted.bound);
            let mut record = Box::new(record);
            within(cx, &adopted, |cx| record.start(cx));
            RunState::Recording(record)
        };
        self.adopted.running.push(AdoptedRun {
            entry: adopted,
            state,
        });
    }

    /// Hands an adopted entry the answer to one of its effects.
    pub(super) fn on_adopted_outcome(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) {
        let Some(index) = self
            .adopted
            .running
            .iter()
            .position(|run| run.awaits(effect))
        else {
            return;
        };
        let mut run = self.adopted.running.remove(index);
        let mut adopted = run.entry.clone();
        let advanced = within(cx, &adopted, |cx| run.accept(cx, effect, outcome));
        adopted.release_error = run
            .arrival_release_error(&advanced)
            .or(adopted.release_error);
        match advanced {
            Advanced::Waiting => self.adopted.running.insert(index, run),
            Advanced::Arrived(event, Identified::Proceed(participation)) => {
                self.enter_pass(cx, adopted, *event, None, participation);
            }
            Advanced::Arrived(event, Identified::Held(held)) => {
                self.hold_entry(cx, &adopted, held, &event);
            }
            Advanced::Posted => self.on_entry_posted(cx, adopted, run),
            Advanced::WorkRead(read) => self.on_entry_key(cx, adopted, run, read),
            Advanced::Stopped(stopped) => self.settle_entry(cx, &adopted, *stopped),
            Advanced::Cut(cut) => self.settle_cut(cx, adopted, *cut, None),
            Advanced::Passed(Ok(report)) => self.on_adopted_pass(cx, &adopted, report),
            Advanced::Passed(Err(Abort::DeadlinePassed {
                locked_event,
                release_failure,
            }))
            | Advanced::Recorded(Err(Abort::DeadlinePassed {
                locked_event,
                release_failure,
            })) => {
                let cut = cut_at_the_deadline(locked_event.is_some(), release_failure);
                self.settle_cut(cx, adopted, cut, locked_event);
            }
            Advanced::Passed(Err(abort)) | Advanced::Recorded(Err(abort)) => {
                let error = cascade::failure(abort, self.call.room_id.clone());
                self.settle_entry(cx, &adopted, Err(error));
            }
            Advanced::Recorded(Ok(report)) => self.on_adopted_record(cx, &adopted, report),
        }
    }

    /// The entry's message step 5 held (RFC §10.1 step 5): the challenge a
    /// hook put to its sender is queued as an injected event of the entry's
    /// cascade, which runs ahead of the call's own passes, and its caller
    /// answered with the refusal, nothing of the message stored. An entry
    /// beside the operation's work answers once the challenge committed and
    /// carries its failed release, as Python's held call commits it before
    /// it returns (RFC §13.5, §9.5.1 Reentrance).
    fn hold_entry(&mut self, cx: &mut Cx<'_>, adopted: &Adopted, held: Held, event: &RoomEvent) {
        let challenge: Vec<InjectedEvent> = held.challenge.into_iter().map(|c| *c).collect();
        let owed = adopted.nested.is_none().then_some(adopted.id);
        for job in self.injected(cx, challenge, event, Lineage::Entry(adopted.id), owed) {
            self.cascade.queue(job);
        }
        let refused = InboundResult {
            blocked: true,
            reason: Some(held.reason),
            refused_before_the_lock: true,
            ..InboundResult::default()
        };
        if self.cascade.owes(adopted.id) {
            self.adopted.pending.push(Pending {
                entry: adopted.clone(),
                result: refused,
                delivers: false,
            });
            return self.answer_owed(cx);
        }
        self.settle_entry(cx, adopted, Ok(refused));
    }

    /// The entry's pass is over: the pass adds to the cascade as any pass
    /// does, its delivery set in the place its event took at its commit
    /// (RFC §10.1 step 18), and its caller is answered. An entry the room's
    /// admission order adopted answers once the events its hooks injected
    /// are committed, each in a section of its own, and carries their
    /// failed release, as Python's adopted call commits them before it
    /// returns (RFC §9.4, §9.5.1 Reentrance, HITL b779fc14); one nested
    /// under a lock answers at its commit, what it injected committed after
    /// the section it is nested in, as Python's, and owed to the entry
    /// whose section that is: that entry answers once they are committed
    /// too.
    fn on_adopted_pass(&mut self, cx: &mut Cx<'_>, adopted: &Adopted, mut report: PassReport) {
        let mut result = entry_result(&report.result);
        result.error = report.release_error.take();
        let delivers = matches!(report.result, PassResult::Delivered { .. });
        let committed = matches!(
            report.result,
            PassResult::Delivered { .. } | PassResult::Blocked { .. } | PassResult::Stored { .. }
        );
        let waits = !report.injected_events.is_empty() || self.cascade.owes(adopted.id);
        if adopted.nested.is_none() && committed && waits {
            self.adopted.pending.push(Pending {
                entry: adopted.clone(),
                result,
                delivers,
            });
            self.add_to_cascade(cx, report, &PassOrigin::Adopted(adopted.id));
            return self.answer_owed(cx);
        }
        if delivers {
            self.answer_entry(cx, adopted, Ok(result));
        } else {
            self.settle_entry(cx, adopted, Ok(result));
        }
        self.add_to_cascade(cx, report, &PassOrigin::Adopted(adopted.id));
    }

    /// Answers each entry whose answer waited for the commits of the events
    /// its hooks injected once none of them is queued or running: committed,
    /// refused, or dropped with a cascade that stopped.
    pub(super) fn answer_owed(&mut self, cx: &mut Cx<'_>) {
        let (owed, answered): (Vec<_>, Vec<_>) = mem::take(&mut self.adopted.pending)
            .into_iter()
            .partition(|pending| self.cascade.owes(pending.entry.id));
        self.adopted.pending = owed;
        for Pending {
            entry,
            result,
            delivers,
        } in answered
        {
            if delivers {
                self.answer_entry(cx, &entry, Ok(result));
            } else {
                self.settle_entry(cx, &entry, Ok(result));
            }
        }
    }

    /// The adopted entry whose answer waits for the commits of what the
    /// hooks of the pass from `origin` inject: the entry itself, the entry
    /// whose section an entry nested in, or the entry an injected event's
    /// pass is owed to, while it waits.
    pub(super) fn owed_by(&self, origin: &PassOrigin) -> Option<EntryId> {
        let entry = match origin {
            PassOrigin::Adopted(entry) => {
                if let Some(owner) = self.adopted.owed_to(*entry) {
                    return Some(owner);
                }
                Some(*entry)
            }
            PassOrigin::Injected { owed, .. } => *owed,
            PassOrigin::Trigger | PassOrigin::Response { .. } => None,
        };
        entry.filter(|entry| self.adopted.is_pending(*entry))
    }

    /// Notes `error`, a pass's failed release, on the answer of `owed`, the
    /// entry that waits for that pass, or else on the call's result (RFC
    /// §13.5).
    pub(super) fn note_release_error(&mut self, owed: Option<EntryId>, error: Option<ErrorRecord>) {
        if let Some(entry) = owed
            && self.adopted.note_pending_error(entry, error.clone())
        {
            return;
        }
        self.trigger.note_error(error);
    }

    /// The entry was stored blocked past its caller's reentry budget, or
    /// refused by the room's status.
    fn on_adopted_record(&mut self, cx: &mut Cx<'_>, adopted: &Adopted, report: RecordReport) {
        self.settle_entry(cx, adopted, Ok(cascade::record_result(report)));
    }

    /// Settles an entry cut before its commit, `cut`, `locked_event` the
    /// event whose pass held the room lock then. Past its own deadline it
    /// reports its `process_timeout` and its caller reads the refusal at
    /// once, as the Python implementation's call refused at its deadline
    /// (RFC §13.6). Cut for its caller gone, it reports nothing, and is
    /// settled once the effect whose code made it is answered too: that
    /// code is dropped, so that it never reads the entry's refusal and runs
    /// on, where the Python implementation cancels it with its hook.
    fn settle_cut(
        &mut self,
        cx: &mut Cx<'_>,
        adopted: Adopted,
        cut: InboundResult,
        locked_event: Option<EventId>,
    ) {
        let gone = self.adopted.gone.remove(&adopted.id);
        if !gone && adopted.deadline == Deadline::Own {
            report_entry_expiry(cx, &adopted, locked_event);
        } else if cx.io.is_outstanding(adopted.origin) {
            self.adopted.cut.push((adopted, cut));
            return;
        }
        self.settle_entry(cx, &adopted, Ok(cut));
    }

    /// The entry `entry`, not started yet, was made by its origin's code in
    /// that code's own task, which awaits it: it follows that code, its
    /// deadline the origin's when it has one (RFC §9.5.1 Reentrance).
    pub(super) fn entry_follows_its_origin(&mut self, entry: EntryId) {
        if let Some((adopted, _)) = self
            .adopted
            .adopted
            .iter_mut()
            .find(|(adopted, _)| adopted.id == entry)
            && let Deadline::Unset { follows } = &mut adopted.deadline
        {
            *follows = true;
        }
    }

    /// The caller of the entry `entry` is gone before its answer (RFC
    /// §9.5.1 Reentrance, §13.6): the entry ends before its commit, every
    /// request within its bound given up on, nothing of it stored and
    /// nothing reported, as the Python implementation cancels a call with
    /// the code that awaits it. An entry committed by then, or settled,
    /// goes on. Returns the effects the driver holds among those given up.
    pub(super) fn entry_caller_gone(&mut self, io: &mut Io, entry: EntryId) -> Vec<EffectId> {
        let queued = self
            .adopted
            .adopted
            .iter()
            .any(|(adopted, _)| adopted.id == entry);
        let running = self.adopted.running.iter().any(|run| run.entry.id == entry);
        if !queued && !running {
            return Vec::new();
        }
        self.adopted.gone.insert(entry);
        if queued {
            return Vec::new();
        }
        io.expire_entry(entry)
    }

    /// Settles the cut entries whose origin is answered by now.
    pub(super) fn settle_cut_entries(&mut self, cx: &mut Cx<'_>) {
        let (answered, held): (Vec<_>, Vec<_>) = mem::take(&mut self.adopted.cut)
            .into_iter()
            .partition(|(adopted, _)| !cx.io.is_outstanding(adopted.origin));
        self.adopted.cut = held;
        for (adopted, cut) in answered {
            self.settle_entry(cx, &adopted, Ok(cut));
        }
    }

    /// Answers the entry's caller with `result`, a direct injection's
    /// refusal before its commit raised (RFC §10.5), then hands back the
    /// answer of the hook it was nested under, once no other entry is.
    fn settle_entry(
        &mut self,
        cx: &mut Cx<'_>,
        adopted: &Adopted,
        result: Result<InboundResult, InboundError>,
    ) {
        self.answer_entry(cx, adopted, result);
        // No delivery set follows: what its completion handle waits for is
        // over (RFC §10.1 step 18).
        cx.io.delivered(adopted.id, InboundResult::default());
    }

    /// Answers the entry's caller with `result`, as
    /// [`settle_entry`](Self::settle_entry) does, for an entry whose
    /// delivery set follows: its end is reported once it ran.
    fn answer_entry(
        &mut self,
        cx: &mut Cx<'_>,
        adopted: &Adopted,
        result: Result<InboundResult, InboundError>,
    ) {
        let result = if adopted.direct {
            result.and_then(result::committed)
        } else {
            // Step 2's release came before anything the entry met since
            // (RFC §13.5).
            result.map(|mut result| {
                result.error = adopted.release_error.clone().or(result.error);
                result
            })
        };
        cx.io.settle(adopted.id, result);
        for (effect, outcome) in self.adopted.released(adopted.origin) {
            cx.io.requeue(effect, outcome);
        }
    }
}
