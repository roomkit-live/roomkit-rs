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
//! §9.5.1). It draws on the operation's reentry budget, past which it is
//! stored `BLOCKED` with `reentry_loop_cap` (RFC §8.3); a message is charged
//! first thing, before its channel is looked up, and refused with no event
//! past the budget (RFC §10.1 step 1). Its caller receives its result at
//! its commit (detached completion: no delivery results, no completion
//! handle), and its event takes its place in the room's lane at that
//! commit. A message's arrival runs its step 2, then its channel's parse. A
//! direct injection is admitted and stamped at once, and its room, then its
//! key, when it carries one, read before it draws on the budget: a call its
//! room refuses and a replay draw nothing (RFC §9.5.1 Past the cap, §13.4,
//! §17.2).

use std::mem;

use super::arrival::{self, Arrival};
use super::cascade::{self, entry_result, replay_result};
use super::deadline::Stopped;
use super::direct::{self, DirectCall};
use super::greeting;
use super::host_post::HostPost;
use super::identity::{Held, Identified, Participation};
use super::pass::{CommitPass, PassOrigin, PassReport, PassResult};
use super::queues::Lineage;
use super::record::{RecordPass, RecordReport};
use super::work_read::{Read, WorkRead};
use super::{InboundError, InboundResult, Machine, MessageAnswers, result};
use crate::model::{InboundMessage, InjectedEvent, Post, RoomEvent, RoomId};
use crate::ops::io::{Bound, Io};
use crate::ops::protocol::{EffectId, Entry, EntryId, Notice, Outcome, SpanNote};
use crate::ops::{Abort, Cx, Progress, RefusedOperation};
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
    Passed(Result<PassReport, Abort>),
    Recorded(Result<RecordReport, Abort>),
}

impl Adoptions {
    /// Whether no entry is adopted or running.
    pub(super) fn is_idle(&self) -> bool {
        self.adopted.is_empty() && self.running.is_empty()
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
                    // An adopted message carries no deadline (gap G-070).
                    Advanced::Stopped(Box::new(Err(InboundError::Store(
                        Abort::DeadlinePassed(None).into_record(),
                    ))))
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
                // An adopted entry carries no deadline (gap G-070).
                Progress::Done(Err(
                    Stopped::Expired | Stopped::PastBudget | Stopped::Refused(_),
                )) => Advanced::Stopped(Box::new(Err(InboundError::Store(
                    Abort::DeadlinePassed(None).into_record(),
                )))),
            },
            RunState::ReadingWork(read) => match read.accept(cx, effect, outcome) {
                Progress::Waiting => Advanced::Waiting,
                Progress::Done(Ok(read)) => Advanced::WorkRead(read),
                Progress::Done(Err(Stopped::Failed(error))) => {
                    Advanced::Stopped(Box::new(Err(error)))
                }
                // An adopted entry carries no deadline (gap G-070), and is
                // never refused past the budget before its room is read.
                Progress::Done(Err(
                    Stopped::Expired | Stopped::PastBudget | Stopped::Refused(_),
                )) => Advanced::Stopped(Box::new(Err(InboundError::Store(
                    Abort::DeadlinePassed(None).into_record(),
                )))),
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
        let adopted = Adopted {
            id,
            room_id: entry.room_id().clone(),
            origin,
            nested,
            direct: matches!(entry, Entry::Direct { .. } | Entry::Post { .. }),
        };
        self.adopted.adopted.push((adopted, Box::new(entry)));
        Ok(id)
    }

    /// Starts the entries adopted since the last poll, but an entry nested
    /// under a call another entry nested under it still runs for: it waits
    /// for that one's settlement.
    pub(super) fn start_adopted(&mut self, cx: &mut Cx<'_>) {
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
            let mut pass =
                Box::new(CommitPass::new(PassOrigin::Adopted(adopted.id), event).greeting());
            within(cx, &adopted, |cx| pass.start(cx));
            RunState::Passing(pass)
        } else {
            event.blocked_by = Some(chain_depth::REENTRY_LOOP_CAP.to_owned());
            let mut record = Box::new(RecordPass::refusing(event, RefusedOperation::Inbound));
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
            HostPost::start(cx, event, call, binding, Bound::Unbounded)
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
        let mut arrival = Box::new(
            Arrival::adopted(adopted.id, room_id, organization_id, channel, message)
                .answered(answers),
        );
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
            WorkRead::start(cx, event, call, Bound::Unbounded)
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
                .checking(direct)
                .participating(participation);
            let mut pass = Box::new(pass);
            within(cx, &adopted, |cx| pass.start(cx));
            RunState::Passing(pass)
        } else {
            event.blocked_by = Some(chain_depth::REENTRY_LOOP_CAP.to_owned());
            let record = RecordPass::refusing(event, RefusedOperation::Inbound).checking(direct);
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
        let adopted = run.entry.clone();
        let advanced = within(cx, &adopted, |cx| run.accept(cx, effect, outcome));
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
            Advanced::Passed(Ok(report)) => self.on_adopted_pass(cx, &adopted, report),
            Advanced::Passed(Err(abort)) | Advanced::Recorded(Err(abort)) => {
                let error = cascade::failure(abort, self.call.room_id.clone());
                self.settle_entry(cx, &adopted, Err(error));
            }
            Advanced::Recorded(Ok(report)) => self.on_adopted_record(cx, &adopted, report),
        }
    }

    /// The entry's message step 5 held (RFC §10.1 step 5): the challenge a
    /// hook put to its sender is queued as an injected event of the
    /// cascade, and its caller answered with the refusal, nothing of the
    /// message stored.
    fn hold_entry(&mut self, cx: &mut Cx<'_>, adopted: &Adopted, held: Held, event: &RoomEvent) {
        let challenge: Vec<InjectedEvent> = held.challenge.into_iter().map(|c| *c).collect();
        for job in self.injected(cx, challenge, event, Lineage::Call) {
            self.cascade.queue(job);
        }
        let refused = InboundResult {
            blocked: true,
            reason: Some(held.reason),
            refused_before_the_lock: true,
            ..InboundResult::default()
        };
        self.settle_entry(cx, adopted, Ok(refused));
    }

    /// The entry's pass is over: its caller is answered, and the pass adds
    /// to the cascade as any pass does, its delivery set in the place its
    /// event took at its commit (RFC §10.1 step 18).
    fn on_adopted_pass(&mut self, cx: &mut Cx<'_>, adopted: &Adopted, mut report: PassReport) {
        let mut result = entry_result(&report.result);
        result.error = report.release_error.take();
        if matches!(report.result, PassResult::Delivered { .. }) {
            self.answer_entry(cx, adopted, Ok(result));
        } else {
            self.settle_entry(cx, adopted, Ok(result));
        }
        self.add_to_cascade(cx, report, &PassOrigin::Adopted(adopted.id));
    }

    /// The entry was stored blocked past its caller's reentry budget, or
    /// refused by the room's status.
    fn on_adopted_record(&mut self, cx: &mut Cx<'_>, adopted: &Adopted, report: RecordReport) {
        self.settle_entry(cx, adopted, Ok(cascade::record_result(report)));
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
            result
        };
        cx.io.settle(adopted.id, result);
        for (effect, outcome) in self.adopted.released(adopted.origin) {
            cx.io.requeue(effect, outcome);
        }
    }
}
