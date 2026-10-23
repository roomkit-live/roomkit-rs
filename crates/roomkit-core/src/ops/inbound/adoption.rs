//! The calls into the room that the operation's own work makes (RFC §13.5,
//! §9.5.1 Reentrance, §10.1 step 18, gap G-070).
//!
//! A hook, a turn or a delivery of the operation that calls `send_event` or
//! `process_inbound` for the room whose lane the operation holds would wait
//! for that lane until the operation ends, which waits for the call. The
//! driver hands such a call to the operation instead: it is adopted, as an
//! entry of its own, with its own result. Where it runs depends on the call
//! that made it, its origin:
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
//! - any other call (an off-lock hook, a delivery set's turn or delivery,
//!   step 2's awaited observers, a channel's parse): the entry runs beside
//!   the lane's work, taking the room lock as any section does, after the
//!   operation's other section that holds it, if any.
//!
//! Either way the entry takes no admission ticket and runs every hook under
//! the lock: the operation it joins stands in the room's order already (RFC
//! §9.5.1). It draws on the operation's reentry budget, past which it is
//! stored `BLOCKED` with `reentry_loop_cap` (RFC §8.3); a message is charged
//! as its arrival starts, before its channel's parse, and refused with no
//! event past the budget. Its caller receives its result at its commit
//! (detached completion: no delivery results, no completion handle), and
//! its delivery set joins the back of the lane. A message's arrival runs in
//! the lane the operation holds: its step 2, then its channel's parse. A
//! direct injection is admitted and stamped at once.

use std::mem;

use super::arrival::{self, Arrival};
use super::cascade::{self, entry_result};
use super::deadline::Stopped;
use super::direct::{self, Admitted, DirectCall};
use super::instructions::{self, Admission};
use super::pass::{CommitPass, PassOrigin, PassReport};
use super::record::{RecordPass, RecordReport, RecordResult};
use super::{InboundError, InboundResult, Machine, result};
use crate::model::{InboundMessage, RoomEvent, RoomId};
use crate::ops::io::Io;
use crate::ops::protocol::{EffectId, Entry, EntryId, Outcome};
use crate::ops::{Abort, Cx, Progress, RefusedOperation};
use crate::rules::chain_depth;

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
    /// A message's steps 2 to 4, in the lane the operation holds.
    Arriving(Box<Arrival>),
    /// The entry's pass.
    Passing(Box<CommitPass>),
    /// The entry stored blocked, past its caller's reentry budget.
    Recording(Box<RecordPass>),
}

/// Where an adopted entry stands after an answer.
enum Advanced {
    Waiting,
    Arrived(Box<RoomEvent>),
    Stopped(InboundError),
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
            RunState::Passing(pass) => pass.awaits(effect),
            RunState::Recording(record) => record.awaits(effect),
        }
    }

    /// Hands the entry the answer to one of its effects.
    fn accept(&mut self, cx: &mut Cx<'_>, effect: EffectId, outcome: Outcome) -> Advanced {
        match &mut self.state {
            RunState::Arriving(arrival) => match arrival.accept(cx, effect, outcome) {
                Progress::Waiting => Advanced::Waiting,
                Progress::Done(Ok(arrived)) => Advanced::Arrived(Box::new(arrived.event)),
                Progress::Done(Err(Stopped::Failed(error))) => Advanced::Stopped(error),
                Progress::Done(Err(Stopped::Expired)) => {
                    // An adopted message carries no deadline (gap G-070).
                    Advanced::Stopped(InboundError::Store(
                        Abort::DeadlinePassed(None).into_record(),
                    ))
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

/// Runs `advance` for an entry nested under the lock of `nested`, its
/// requests for that lock answered at once, or for an entry of its own.
fn within<T>(
    cx: &mut Cx<'_>,
    nested: Option<&RoomId>,
    advance: impl FnOnce(&mut Cx<'_>) -> T,
) -> T {
    let outer = cx.io.nest(nested.cloned());
    let advanced = advance(cx);
    cx.io.nest(outer);
    advanced
}

impl Machine {
    /// Adopts `entry`, a call into the room made by the code effect
    /// `origin` runs (RFC §13.5): nested in the pass whose hook `origin` is,
    /// beside the lane's work otherwise.
    ///
    /// # Errors
    ///
    /// Hands `entry` back when the operation holds no lane of its room, or
    /// `origin` is not outstanding.
    pub(super) fn adopt(
        &mut self,
        io: &Io,
        entry: Entry,
        origin: EffectId,
    ) -> Result<EntryId, Box<Entry>> {
        if self.lane_room() != Some(entry.room_id()) || !io.is_outstanding(origin) {
            return Err(Box::new(entry));
        }
        let nested = io
            .locked_for(origin)
            .filter(|room_id| *room_id == entry.room_id())
            .cloned();
        let id = self.adopted.next_id();
        let adopted = Adopted {
            id,
            origin,
            nested,
            direct: matches!(entry, Entry::Direct { .. }),
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
                Ok(Admitted::Event(event)) => {
                    let call = DirectCall { organization_id };
                    self.enter_pass(cx, adopted, *event, Some(call));
                }
                Ok(Admitted::Refused(refused)) => self.settle_entry(cx, &adopted, Ok(*refused)),
                Err(error) => self.settle_entry(cx, &adopted, Err(error)),
            },
            Entry::Message {
                room_id,
                organization_id,
                message,
            } => self.arrive(cx, adopted, room_id, organization_id, *message),
        }
    }

    /// A message's steps 1 to 4 in the lane the operation holds; refused
    /// before anything is read when it cannot be one.
    fn arrive(
        &mut self,
        cx: &mut Cx<'_>,
        adopted: Adopted,
        room_id: RoomId,
        organization_id: Option<String>,
        message: InboundMessage,
    ) {
        let (channel, message) = match arrival::resolve(cx.config, message) {
            Ok(resolved) => resolved,
            Err(error) => return self.settle_entry(cx, &adopted, Err(error)),
        };
        match instructions::admit(cx.config, (&message).into()) {
            Ok(Admission::Admitted) => {}
            Ok(Admission::Refused(refusal)) => {
                return self.settle_entry(cx, &adopted, Ok(instructions::refused(refusal)));
            }
            Err(error) => return self.settle_entry(cx, &adopted, Err(error)),
        }
        // Charged before its parse: a channel whose parse calls into its room
        // for every message is bounded too.
        if !self.budget.take() {
            let refused = InboundResult {
                blocked: true,
                reason: Some(chain_depth::REENTRY_LOOP_CAP.to_owned()),
                ..InboundResult::default()
            };
            return self.settle_entry(cx, &adopted, Ok(refused));
        }
        let mut arrival = Box::new(Arrival::within_lane(
            room_id,
            organization_id,
            channel,
            message,
        ));
        within(cx, adopted.nested.as_ref(), |cx| arrival.start(cx));
        self.adopted.running.push(AdoptedRun {
            entry: adopted,
            state: RunState::Arriving(arrival),
        });
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
    ) {
        let charged = direct.is_none();
        let state = if charged || self.budget.take() {
            let pass = CommitPass::new(PassOrigin::Adopted(adopted.id), event).checking(direct);
            let mut pass = Box::new(pass);
            within(cx, adopted.nested.as_ref(), |cx| pass.start(cx));
            RunState::Passing(pass)
        } else {
            event.blocked_by = Some(chain_depth::REENTRY_LOOP_CAP.to_owned());
            let record = RecordPass::refusing(event, RefusedOperation::Inbound).checking(direct);
            let mut record = Box::new(record);
            within(cx, adopted.nested.as_ref(), |cx| record.start(cx));
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
        let nested = run.entry.nested.clone();
        let advanced = within(cx, nested.as_ref(), |cx| run.accept(cx, effect, outcome));
        let adopted = run.entry.clone();
        match advanced {
            Advanced::Waiting => self.adopted.running.insert(index, run),
            Advanced::Arrived(event) => self.enter_pass(cx, adopted, *event, None),
            Advanced::Stopped(error) => self.settle_entry(cx, &adopted, Err(error)),
            Advanced::Passed(Ok(report)) => self.on_adopted_pass(cx, &adopted, report),
            Advanced::Passed(Err(abort)) | Advanced::Recorded(Err(abort)) => {
                let error = cascade::failure(abort, self.room_id.clone());
                self.settle_entry(cx, &adopted, Err(error));
            }
            Advanced::Recorded(Ok(report)) => self.on_adopted_record(cx, &adopted, report),
        }
    }

    /// The entry's pass is over: its caller is answered, and the pass adds
    /// to the cascade as any pass does, its delivery set at the back of the
    /// lane (RFC §10.1 step 18).
    fn on_adopted_pass(&mut self, cx: &mut Cx<'_>, adopted: &Adopted, mut report: PassReport) {
        let mut result = entry_result(&report.result);
        result.error = report.release_error.take();
        self.settle_entry(cx, adopted, Ok(result));
        self.add_to_cascade(cx, report, &PassOrigin::Adopted(adopted.id));
    }

    /// The entry was stored blocked past its caller's reentry budget, or
    /// refused by the room's status.
    fn on_adopted_record(&mut self, cx: &mut Cx<'_>, adopted: &Adopted, report: RecordReport) {
        let result = match report.result {
            RecordResult::Committed(record) => InboundResult {
                blocked: true,
                reason: record.blocked_by.clone(),
                event: Some(*record),
                error: report.release_error,
                ..InboundResult::default()
            },
            RecordResult::Refused => InboundResult {
                blocked: true,
                reason: Some("room_closed".to_owned()),
                ..InboundResult::default()
            },
        };
        self.settle_entry(cx, adopted, Ok(result));
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
