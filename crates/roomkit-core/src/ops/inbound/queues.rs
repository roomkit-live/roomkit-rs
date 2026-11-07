//! The operation's work in its room (RFC §10.1 steps 6 to 14, §10.2): its
//! passes through the room lock, one at a time in the order they are
//! queued, and its delivery sets, one at a time, each run once its place's
//! turn in the room's delivery lane comes.
//!
//! The two queues run beside each other: a pass never waits behind a set.
//! A set may wait in the room's lane behind another call's set, whose turn
//! may wait for this call's decision, as an agent's turn waits for its call
//! into the room: the trigger's pass, queued behind the set of an entry its
//! off-lock hook made, would never run. Passes run once the trigger's pass
//! is queued, at their head, but those of an entry the call's admission
//! adopted, which run ahead of it with their streams, so that the entry's
//! whole cascade is over before the event commits when that work waits for
//! it (RFC §9.5.1 Reentrance); a set runs as soon as it is queued and the set
//! before it is over, its record under the room's lock included (RFC §14.3),
//! whatever the call is doing, the set of an entry its parse or its off-lock
//! hook made included, so that nothing behind its place waits for the call's
//! own steps.
//!
//! An event's place is taken at its commit, under the lock (`pass`), so the
//! places follow the commits; its set joins `sets` once its pass is over,
//! in the same order. The events its pass's hooks injected are held with
//! the set and join `passes` once it is over, before the records and the
//! responses the set produced (RFC §9.4, §10.1 step 14). A cascade that
//! stops, failed or past its deadline, runs no pass any more, and still
//! delivers every event committed (RFC §13.6).
//!
//! The streams of its turns wait in `streams`, in the order the turns
//! answered, and one is read at a time once no pass nor set is queued or
//! running (RFC §10.1 step 14, `stream`): each row its reader hands back
//! takes the head of the passes, so that the reader waits for nothing but
//! its rows, and its delivery set runs while the stream goes on.

use std::collections::VecDeque;

use serde_json::Value;

use super::delivery::{DeliverySet, SetOutcome};
use super::direct::DirectCall;
use super::job::{Job, RecordNote, RowNote, Run};
use super::pass::{CommitPass, PassOrigin};
use super::record::RecordPass;
use super::stream::{self, PendingRow, Read, StreamEnd, StreamJob, StreamRead};
use super::{Machine, Stage};
use crate::model::{EventId, Metadata, RoomEvent, RoomId};
use crate::ops::failure_log::Log;
use crate::ops::io::Io;
use crate::ops::protocol::{AnswerOf, EffectId, Entry, EntryId, Notice, Outcome, SpanNote};
use crate::ops::{Cx, Progress, RefusedOperation};
use crate::rules::chain_depth::{self, Draw, ReentryBudget};
use crate::rules::hook_chain::HookChain;

/// Why a row of a cascade that stopped is not committed.
const STOPPED_REASON: &str = "cascade_stopped";

/// Whose cascade a pass or a delivery set belongs to: the call's own, or
/// the cascade of an entry the operation adopted (gap G-070), whose caller
/// was answered at its commit. Cutting the call's turns, once its caller
/// stopped waiting, cuts its own cascade's alone (RFC §10.1 "Explicit
/// delivery cancellation", gap G-076).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Lineage {
    /// The call's own cascade.
    Call,
    /// The cascade of the entry the operation adopted.
    Entry(EntryId),
}

impl Lineage {
    /// The call whose cascade it is, as an effect names it
    /// (`Effect::answers`).
    pub(super) fn answers(self) -> AnswerOf {
        match self {
            Self::Call => AnswerOf::Call,
            Self::Entry(entry) => AnswerOf::Entry(entry),
        }
    }
}

/// A committed event's delivery set, and the passes that follow it.
#[derive(Debug)]
pub(super) struct Delivery {
    pub(super) set: Box<DeliverySet>,
    /// The events the event's hooks injected, which join the passes once
    /// the set is over (RFC §9.4).
    pub(super) then: Vec<Job>,
}

/// The operation's cascades, the call's and its adopted entries' (gap
/// G-070), as two queues: the passes queued and the one running, and the
/// delivery sets queued, in the order of their places, and the one running;
/// whether the cascade stopped; and the reentry budget its passes draw on.
#[derive(Debug)]
pub(super) struct Cascade {
    passes: VecDeque<Job>,
    pass: Option<Job>,
    sets: VecDeque<Delivery>,
    delivery: Option<Delivery>,
    /// The sets dropped before they started, their room's lane sealed by
    /// the kit's close, storing what their events' hooks gave while the
    /// running set delivers on (RFC §10.1 step 15, §12.10.4).
    dropping: Vec<Delivery>,
    /// Once the kit's close sealed the rooms' delivery lanes, the events
    /// whose set runs from its placement, placed first in an idle lane: a
    /// set queued holding another place there is dropped at once.
    lanes_sealed: Option<Vec<EventId>>,
    /// Whether the cascade stopped, failed or past its deadline: no pass
    /// runs any more, committed events are still delivered.
    stopping: bool,
    /// The cascades that stopped while the others go on: the call's own,
    /// past its deadline, its adopted entries' running to their end (RFC
    /// §13.6, §9.5.1 Reentrance).
    stopped: Vec<Lineage>,
    /// The cascades whose turns are cut: the call's own, its caller gone
    /// once the call was decided or its delivery cancelled, and those of
    /// the entries whose delivery was cancelled. No turn of such a cascade
    /// starts, and no response of it re-enters (RFC §10.1 "Explicit
    /// delivery cancellation").
    cut: Vec<Lineage>,
    /// The reentry budget every cascade of the operation draws on, an
    /// adopted entry's on its caller's (RFC §8.3), and the calls its
    /// delivery sets' targets make into a room too (gap G-070).
    pub(super) budget: ReentryBudget,
    /// The budget of the cascade whose delivery, or whose ASYNC hook's
    /// body, made the call, as its trigger is charged on it as it starts: a
    /// pass for a delivery's call, a hook call for a hook body's (RFC §8.3,
    /// gaps G-070, G-087).
    drawn: Option<Draw>,
    /// For a message a delivery's code sent, the room that delivery
    /// delivers in and its budget, which the message draws on once routed
    /// into that room or once it creates a room (gap G-070).
    from_delivery: Option<(RoomId, Draw)>,
    /// The streams of the cascade's turns waiting to be read, in the order
    /// the turns answered (RFC §10.1 step 14).
    streams: VecDeque<StreamJob>,
    /// The stream being read.
    stream: Option<StreamRead>,
    /// The rows a stream's reader handed back, waiting for their pass.
    rows: Vec<PendingRow>,
}

impl Cascade {
    /// An empty cascade drawing on a budget of its own.
    pub(super) fn new(budget: ReentryBudget) -> Self {
        Self {
            passes: VecDeque::new(),
            pass: None,
            sets: VecDeque::new(),
            delivery: None,
            dropping: Vec::new(),
            lanes_sealed: None,
            stopping: false,
            stopped: Vec::new(),
            cut: Vec::new(),
            budget,
            drawn: None,
            from_delivery: None,
            streams: VecDeque::new(),
            stream: None,
            rows: Vec::new(),
        }
    }

    /// Draws on `budget` only once the call's message is routed into
    /// `room_id`, the room whose delivery made the call, or creates a room
    /// (gap G-070).
    pub(super) fn draw_on_from_delivery(&mut self, room_id: RoomId, draw: Draw) {
        self.from_delivery = Some((room_id, draw));
    }

    /// The room and budget a delivery's message draws on once routed into
    /// that room or once it creates one, taken by its arrival.
    pub(super) fn take_from_delivery(&mut self) -> Option<(RoomId, Draw)> {
        self.from_delivery.take()
    }

    /// Draws on `budget`, the budget of the cascade whose delivery or whose
    /// hook's body made the call (gaps G-070, G-087): the trigger is charged
    /// on it as it is seen, a pass or a hook call, and the cascade's own
    /// passes draw on its passes.
    pub(super) fn draw_on(&mut self, draw: Draw) {
        self.budget = draw.budget().clone();
        self.drawn = Some(draw);
    }

    /// The budget of the cascade whose delivery or hook's body made the
    /// call, when it draws on one, as the call's trigger is charged on it
    /// as it starts.
    pub(super) fn drawn_on(&self) -> Option<&Draw> {
        self.drawn.as_ref()
    }

    /// Queues `job` at the head of the passes, ahead of what the sets of
    /// the adopted entries queued meanwhile: the trigger's pass or record.
    pub(super) fn lead(&mut self, job: Job) {
        self.passes.push_front(job);
    }

    /// Queues `job` behind the cascade's passes; once the cascade stops, no
    /// pass is queued.
    pub(super) fn queue(&mut self, job: Job) {
        if !self.stops(job.lineage()) {
            self.passes.push_back(job);
        }
    }

    /// Queues a committed event's delivery set, which runs whatever comes
    /// next; once the cascade stops, the passes it holds are dropped.
    pub(super) fn queue_set(&mut self, mut delivery: Delivery) {
        if self.stops(delivery.set.lineage()) {
            delivery.then.clear();
        }
        self.sets.push_back(delivery);
    }

    /// The stream being read, if any.
    pub(super) fn stream(&self) -> Option<&StreamRead> {
        self.stream.as_ref()
    }

    /// Queues the stream of a turn to be read, once the cascade's passes
    /// and sets are over; once the cascade stops, it is never read.
    pub(super) fn queue_stream(&mut self, job: StreamJob) {
        if !self.stops(job.lineage) {
            self.streams.push_back(job);
        }
    }

    /// Stops the cascade: no pass nor record runs any more, and no stream
    /// waiting is read, but the delivery set of every event committed
    /// already still runs, in index order, so an event that is stored is
    /// delivered (RFC §13.6), an adopted entry's caller having been told it
    /// is committed. The stream being read goes on, its rows refused.
    pub(super) fn stop(&mut self) {
        self.stopping = true;
        self.passes.clear();
        self.streams.clear();
        for delivery in self.all_sets_mut() {
            delivery.then.clear();
        }
    }

    /// Stops the cascade of `lineage` alone, as [`stop`](Self::stop) stops
    /// them all: its passes and its streams waiting dropped, its events
    /// committed still delivered, the rows of its stream being read refused;
    /// the other cascades go on.
    pub(super) fn stop_lineage(&mut self, lineage: Lineage) {
        if !self.stopped.contains(&lineage) {
            self.stopped.push(lineage);
        }
        self.passes.retain(|job| job.lineage() != lineage);
        self.streams.retain(|job| job.lineage != lineage);
        for delivery in self.all_sets_mut() {
            if delivery.set.lineage() == lineage {
                delivery.then.clear();
            }
        }
    }

    /// Whether the cascade of `lineage` stopped, alone or with them all.
    fn stops(&self, lineage: Lineage) -> bool {
        self.stopping || self.stopped.contains(&lineage)
    }

    /// Whether a read of a stream of the cascade of `lineage` is under way.
    pub(super) fn reads_stream_of(&self, lineage: Lineage) -> bool {
        self.stream
            .as_ref()
            .is_some_and(|read| read.lineage() == lineage && read.reading().is_some())
    }

    /// Whether the turns of the cascade of `lineage` are cut.
    pub(super) fn is_cut(&self, lineage: Lineage) -> bool {
        self.cut.contains(&lineage)
    }

    /// Cuts the turns of the cascade of `lineage`, the call's own or an
    /// adopted entry's (RFC §10.1 "Explicit delivery cancellation", gap
    /// G-076): the sets of that cascade queued ask no agent, the turns its
    /// running set asked are given up on, answered here, and the responses
    /// of that cascade queued do not re-enter. Its events are still
    /// delivered to every other target, and the other cascades keep their
    /// turns. Returns the turns given up on, which the driver cuts and
    /// never answers, then the turns of that cascade whose stream no read
    /// took, which the driver closes (Python's cascade cleanup closes each
    /// stream its turns answered).
    pub(super) fn cut_turns(&mut self, io: &mut Io, lineage: Lineage) -> Vec<EffectId> {
        if !self.is_cut(lineage) {
            self.cut.push(lineage);
        }
        self.passes.retain(|job| match job {
            Job::Pass(pass, _) => !(pass.is_response() && pass.lineage() == lineage),
            // The rows of the stream being read are what its reader was
            // handed: they are stored, as its text is (RFC §12.2 step 13s).
            Job::Record(..) | Job::Row(..) => true,
        });
        for delivery in &mut self.sets {
            if delivery.set.lineage() == lineage {
                delivery.set.cut();
            }
        }
        let mut unread: Vec<EffectId> = self
            .streams
            .iter()
            .filter(|job| job.lineage == lineage)
            .map(|job| job.turn)
            .collect();
        self.streams.retain(|job| job.lineage != lineage);
        // The stream being read is a turn of the cascade: its reader is
        // stopped, the text it was handed stored marked cancelled (RFC
        // §12.2 step 13s), and the read answered as any. One whose read is
        // only asked yet is never read.
        if let Some(read) = self
            .stream
            .as_ref()
            .filter(|read| read.lineage() == lineage)
        {
            match read.reading() {
                Some(reading) => io.notify(Notice::StopReading { effect: reading }),
                None => unread.push(read.turn()),
            }
        }
        // The streams the turns of a set still running answered are never
        // queued, and the run drops them unread, as Python's cancelled
        // cascade drops the streams a delivery set hands it once over.
        let mut cut = match self.delivery.as_mut() {
            Some(delivery) if delivery.set.lineage() == lineage => {
                let turns = delivery.set.cut();
                io.cut(&turns)
            }
            Some(_) | None => Vec::new(),
        };
        cut.extend(unread);
        cut
    }

    /// Whether the cascade of `lineage` has work queued or running: a
    /// delivery set, a pass, a stream, or a row waiting for its pass, which
    /// counts for every lineage.
    pub(super) fn holds(&self, lineage: Lineage) -> bool {
        let of = |job: &Job| match job {
            Job::Pass(pass, _) | Job::Row(pass, _) => pass.lineage() == lineage,
            Job::Record(..) => false,
        };
        self.all_sets()
            .any(|delivery| delivery.set.lineage() == lineage)
            || self.passes.iter().chain(&self.pass).any(of)
            || self.streams.iter().any(|job| job.lineage == lineage)
            || self
                .stream
                .as_ref()
                .is_some_and(|read| read.lineage() == lineage)
            || !self.rows.is_empty()
    }

    /// The adopted entry the running pass is owed to, when `effect` is one
    /// of that pass's: what a call `effect`'s code makes under the lock
    /// injects joins that pass's section, which the entry waits for (RFC
    /// §9.4, §9.5.1 Reentrance).
    pub(super) fn owed_by_the_section_of(&self, effect: EffectId) -> Option<EntryId> {
        self.pass
            .as_ref()
            .filter(|job| job.awaits(effect))
            .and_then(Job::owed)
    }

    /// Whether a pass or a record whose commit the answer of the adopted
    /// entry `entry` waits for is queued or running, held with a set or not
    /// (`Job::owed`).
    pub(super) fn owes(&self, entry: EntryId) -> bool {
        let then = self.all_sets().flat_map(|delivery| &delivery.then);
        self.passes
            .iter()
            .chain(&self.pass)
            .chain(then)
            .any(|job| job.owed() == Some(entry))
    }

    /// Whether all that is left is the delivery of the entries the call
    /// adopted, their sets and their streams: no pass nor record queued or
    /// running, of any cascade, held with a set or waiting as a row, and
    /// nothing of the call's own cascade (RFC §9.5.1 Reentrance).
    pub(super) fn leaves_entries_deliveries_only(&self) -> bool {
        let holds_a_pass = self.all_sets().any(|delivery| !delivery.then.is_empty());
        !self.is_idle()
            && self.passes.is_empty()
            && self.pass.is_none()
            && self.rows.is_empty()
            && !holds_a_pass
            && !self.holds(Lineage::Call)
    }

    /// Whether the cascade has nothing queued nor running, no stream to
    /// read included.
    pub(super) fn is_idle(&self) -> bool {
        self.passes.is_empty()
            && self.sets.is_empty()
            && self.pass.is_none()
            && self.delivery.is_none()
            && self.dropping.is_empty()
            && self.streams.is_empty()
            && self.stream.is_none()
            && self.rows.is_empty()
    }

    /// Whether the cascade runs no pass nor set, and none is queued: a
    /// stream is read only then (RFC §10.1 step 14).
    fn is_between_streams(&self) -> bool {
        self.passes.is_empty()
            && self.sets.is_empty()
            && self.pass.is_none()
            && self.delivery.is_none()
            && self.dropping.is_empty()
            && self.stream.is_none()
            && self.rows.is_empty()
    }

    /// Whether the stream being read waits for `effect`.
    pub(super) fn stream_awaits(&self, effect: EffectId) -> bool {
        self.stream.as_ref().is_some_and(|read| read.awaits(effect))
    }

    /// Takes `entry`, a row the reader of the stream `origin` reads handed
    /// back, for its pass; hands it back when no stream is read by
    /// `origin`.
    pub(super) fn take_row(
        &mut self,
        io: &Io,
        id: EntryId,
        entry: Entry,
        origin: EffectId,
    ) -> Result<EntryId, Box<Entry>> {
        let reads = self.stream.as_ref().is_some_and(|read| read.reads(origin));
        let Entry::Row {
            event, streamed, ..
        } = entry
        else {
            return Err(Box::new(entry));
        };
        if !reads || !io.is_outstanding(origin) {
            return Err(Box::new(Entry::Row {
                room_id: event.room_id.clone(),
                event,
                streamed,
            }));
        }
        self.rows.push(PendingRow {
            entry: id,
            origin,
            event,
            streamed,
        });
        Ok(id)
    }

    /// Whether the running pass waits for `effect`.
    pub(super) fn pass_awaits(&self, effect: EffectId) -> bool {
        self.pass.as_ref().is_some_and(|job| job.awaits(effect))
    }

    /// Whether the running set, or a set dropped before it started,
    /// waits for `effect`.
    pub(super) fn set_awaits(&self, effect: EffectId) -> bool {
        self.delivery
            .iter()
            .chain(&self.dropping)
            .any(|delivery| delivery.set.awaits(effect))
    }

    /// Every set the cascade holds: queued, running, or dropped before it
    /// started and storing what its event's hooks gave.
    fn all_sets(&self) -> impl Iterator<Item = &Delivery> {
        self.sets.iter().chain(&self.delivery).chain(&self.dropping)
    }

    /// Every set the cascade holds, as [`all_sets`](Self::all_sets).
    fn all_sets_mut(&mut self) -> impl Iterator<Item = &mut Delivery> {
        self.sets
            .iter_mut()
            .chain(self.delivery.as_mut())
            .chain(&mut self.dropping)
    }

    /// The kit's close sealed the rooms' delivery lanes (RFC §12.10.4):
    /// the sets queued holding a place there are dropped at once, but those
    /// of the events of `runs`, which run from their placement.
    pub(super) fn seal_lanes(&mut self, runs: Vec<EventId>) {
        self.lanes_sealed = Some(runs);
    }

    /// Takes out the queued sets the room's lane, sealed, drops: those
    /// holding a place there once the kit's close sealed it, but a set that
    /// runs from its placement, and those it took no place for, the kit
    /// closing; the others stay queued in their order.
    fn take_sealed_out(&mut self) -> Vec<Delivery> {
        let sealed = self.lanes_sealed.as_deref();
        let drops = |delivery: &Delivery| delivery.set.drops_in_a_sealed_lane(sealed);
        if !self.sets.iter().any(drops) {
            return Vec::new();
        }
        let (dropped, kept): (Vec<Delivery>, Vec<Delivery>) =
            std::mem::take(&mut self.sets).into_iter().partition(drops);
        self.sets = kept.into();
        dropped
    }
}

impl Machine {
    /// Queues the trigger's pass at the head of the passes, ahead of what
    /// the sets of its adopted entries queued meanwhile, bounded by the
    /// call's deadline and going on with the chain its off-lock check left,
    /// if any.
    pub(super) fn start_trigger(
        &mut self,
        event: RoomEvent,
        checked: Option<HookChain<RoomEvent>>,
    ) {
        let pass = CommitPass::trigger(event, self.call.bound, checked)
            .checking(self.call.direct.clone())
            .participating(self.call.participation.take());
        self.cascade
            .lead(Job::Pass(Box::new(pass), PassOrigin::Trigger));
        self.stage.enter(Stage::Cascade);
    }

    /// Queues the trigger, past the reentry budget it draws on, as its
    /// record at the head of the passes: stored `BLOCKED` with
    /// `reentry_loop_cap`, checked first for the refusals `direct` names,
    /// with no hook run for it (RFC §8.3, gap G-070).
    pub(super) fn start_capped_trigger(&mut self, mut event: RoomEvent, direct: DirectCall) {
        event.blocked_by = Some(chain_depth::REENTRY_LOOP_CAP.to_owned());
        let record = RecordPass::refusing(event, RefusedOperation::Inbound)
            .checking(Some(direct))
            .deciding();
        self.cascade
            .lead(Job::Record(Box::new(record), RecordNote::Trigger));
        self.stage.enter(Stage::Cascade);
    }

    /// Hands the running pass the answer to one of its effects.
    pub(super) fn on_pass_outcome(&mut self, cx: &mut Cx<'_>, effect: EffectId, outcome: Outcome) {
        let Some(job) = self.cascade.pass.take() else {
            debug_assert!(false, "inbound received {effect:?} with no pass running");
            return;
        };
        match job.accept(cx, effect, outcome) {
            Run::Running(job) => self.cascade.pass = Some(job),
            Run::Finished(finished) => self.settle(cx, *finished),
        }
    }

    /// Hands the running set the answer to one of its effects.
    pub(super) fn on_set_outcome(&mut self, cx: &mut Cx<'_>, effect: EffectId, outcome: Outcome) {
        let dropped = self
            .cascade
            .dropping
            .iter()
            .position(|delivery| delivery.set.awaits(effect));
        if let Some(position) = dropped {
            let mut delivery = self.cascade.dropping.remove(position);
            match delivery.set.accept(cx, effect, outcome) {
                Progress::Waiting => self.cascade.dropping.insert(position, delivery),
                Progress::Done(outcome) => self.set_over(cx, outcome, delivery),
            }
            return;
        }
        let Some(mut delivery) = self.cascade.delivery.take() else {
            debug_assert!(false, "inbound received {effect:?} with no set running");
            return;
        };
        match delivery.set.accept(cx, effect, outcome) {
            Progress::Waiting => self.cascade.delivery = Some(delivery),
            Progress::Done(outcome) => self.set_over(cx, outcome, delivery),
        }
    }

    /// Drops at once the queued sets the room's lane, sealed by the kit's
    /// close, drops, as Python's sealed lane drops the entries it holds
    /// (`_abort_pending`): each stores what its event's hooks gave within
    /// the close's grace, whatever set of the operation still delivers
    /// ahead of it, then its call is told (RFC §10.1 step 15, §12.10.4).
    /// Whether it dropped one.
    fn drop_sealed_sets(&mut self, cx: &mut Cx<'_>) -> bool {
        let dropped = self.cascade.take_sealed_out();
        let any = !dropped.is_empty();
        for mut delivery in dropped {
            match delivery.set.drop_unstarted(cx) {
                Progress::Waiting => self.cascade.dropping.push(delivery),
                Progress::Done(outcome) => self.set_over(cx, outcome, delivery),
            }
        }
        any
    }

    /// A set is over: the passes it held join the queue, then what the set
    /// produced (RFC §10.1 step 14).
    fn set_over(&mut self, cx: &mut Cx<'_>, outcome: SetOutcome, delivery: Delivery) {
        let Delivery { set, then } = delivery;
        for job in then {
            self.cascade.queue(job);
        }
        self.on_set(cx, outcome, &set);
    }

    /// Starts the next pass, once the trigger's is queued or for an adopted
    /// entry, and the next set while none of each runs, the call ending
    /// included; once both queues are empty and no adopted entry may still
    /// add to them, the cascade ends.
    pub(super) fn run_queues(&mut self, cx: &mut Cx<'_>) {
        self.start_rows(cx);
        loop {
            // An entry the call's admission adopted runs its whole cascade
            // ahead of the call's own passes, its answers and their streams
            // included, so that a wait on it from that work returns with them
            // (RFC §9.5.1 Reentrance, Python's cascade of its own).
            let passing = matches!(self.stage.current(), Stage::Cascade | Stage::Ending(_));
            let runs = |lineage: Lineage| passing || matches!(lineage, Lineage::Entry(_));
            if self.cascade.pass.is_none()
                && self
                    .cascade
                    .passes
                    .front()
                    .is_some_and(|job| runs(job.lineage()))
                && let Some(mut job) = self.cascade.passes.pop_front()
            {
                job.start(cx);
                self.cascade.pass = Some(job);
                continue;
            }
            if self.drop_sealed_sets(cx) {
                continue;
            }
            if self.cascade.delivery.is_none()
                && let Some(mut delivery) = self.cascade.sets.pop_front()
            {
                match delivery.set.start(cx) {
                    Progress::Waiting => self.cascade.delivery = Some(delivery),
                    Progress::Done(outcome) => self.set_over(cx, outcome, delivery),
                }
                continue;
            }
            if self.cascade.is_between_streams()
                && self.adopted.is_idle()
                && self
                    .cascade
                    .streams
                    .front()
                    .is_some_and(|job| runs(job.lineage))
                && let Some(job) = self.cascade.streams.pop_front()
            {
                self.start_stream(cx, job);
                continue;
            }
            break;
        }
        if matches!(self.stage.current(), Stage::Cascade)
            && self.cascade.is_idle()
            && self.adopted.is_idle()
        {
            self.persist_side_effects(cx);
        } else {
            self.answer_once_its_own_cascade_is_over(cx);
        }
    }
}

impl Machine {
    /// Starts the passes of the rows a stream's reader handed back, each at
    /// the head of the passes; a row of a stream no longer read, or of a
    /// cascade that stopped, is refused at once. The rows of a stream whose
    /// turns are cut are stored: its reader was handed them (RFC §12.2
    /// step 13s), and their sets ask no agent.
    fn start_rows(&mut self, cx: &mut Cx<'_>) {
        for row in std::mem::take(&mut self.cascade.rows) {
            let Some(read) = self
                .cascade
                .stream
                .as_ref()
                .filter(|read| read.reads(row.origin))
            else {
                cx.io
                    .settle(row.entry, Ok(stream::row_refused("stream_over")));
                continue;
            };
            if self.cascade.stops(read.lineage()) {
                cx.io
                    .settle(row.entry, Ok(stream::row_refused(STOPPED_REASON)));
                continue;
            }
            let (pass, skips) = read.row_pass(cx, *row.event, row.streamed);
            let note = RowNote {
                entry: row.entry,
                skips,
                own: read.is_own(),
            };
            self.cascade.lead(Job::Row(Box::new(pass), note));
        }
    }

    /// Starts the read of `job`'s stream, within the cascade's reentry
    /// budget for a chained one (RFC §8.3): past it, the stream is closed
    /// unread, and its agent's answer is stored refused `reentry_loop_cap`,
    /// within where the answers to the event it answers may go.
    fn start_stream(&mut self, cx: &mut Cx<'_>, job: StreamJob) {
        match job.admit(cx, &self.cascade.budget) {
            Ok(()) => self.cascade.stream = Some(StreamRead::start(cx, job)),
            Err(record) => {
                let record = super::stamp::answer_within(
                    *record,
                    job.answers_within.as_ref(),
                    &job.bindings,
                );
                self.queue_refused(record, chain_depth::REENTRY_LOOP_CAP, job.lineage);
            }
        }
    }

    /// Hands the stream being read the answer to one of its requests.
    pub(super) fn on_stream_outcome(&mut self, cx: &mut Cx<'_>, outcome: Outcome) {
        let Some(read) = self.cascade.stream.take() else {
            debug_assert!(
                false,
                "inbound received a stream's outcome with no stream read"
            );
            return;
        };
        let cut = self.cascade.is_cut(read.lineage());
        let own = read.lineage() == Lineage::Call;
        match read.accept(cx, &self.cascade.budget, outcome, cut) {
            Read::Reading(read) => self.cascade.stream = Some(*read),
            Read::Over(end) => {
                let rewritten = end
                    .output
                    .as_ref()
                    .map_or_else(Vec::new, |output| output.response_events.clone());
                self.stream_over(cx, *end);
                if cut && own {
                    cx.io.notify(Notice::CutReadOver {
                        response_metadata: self.trigger.result.response_metadata.clone(),
                        error: self.trigger.result.error.clone(),
                        rewritten,
                    });
                }
            }
        }
    }

    /// A stream is read: its failure, the response's or the target's, fires
    /// `ON_ERROR` and is the call's error when the stream is the call's own
    /// (RFC §12.2 step 13s): a chained stream's is not its caller's. The
    /// turn's record goes to the call once its stream is read, the stream
    /// the call's own, and how the turn ended under the call's `turns` (RFC
    /// §6.4, Python's `merge_channel_record` and `_add_turn`). Nothing of the
    /// streaming target's own output is kept, as Python's `_deliver_segments`
    /// keeps nothing of `deliver_stream()` (RFC §10.1 step 15): the rows the
    /// stream commits are delivered to the others, whose outputs are. The
    /// read's answer holds in `response_events` the rows it wrote the turn's
    /// record on, as stored.
    fn stream_over(&mut self, cx: &mut Cx<'_>, mut end: StreamEnd) {
        let (Some(output), Some(context)) = (end.output.take(), end.context.take()) else {
            return;
        };
        let own = !end.job.chained && end.job.lineage == Lineage::Call;
        self.rewrite_answers(end.job.lineage, &output.response_events);
        self.keep_stream_end(&end.job, &output);
        let Some(error) = output.error else {
            return;
        };
        stream::report_failure(cx, &end.job, &context, &error);
        cx.io.notify(Notice::Failure {
            log: Log::Framework,
            what: end.failed_what(),
            error: error.clone(),
            caller_logs: self.call.failures.receives(own, end.target.is_some()),
        });
        if own {
            let told = error.clone();
            cx.io
                .notify(Notice::Span(SpanNote::CascadeFailed { error: told }));
            self.trigger.note_error(Some(error));
        }
    }

    /// What a stream's read leaves its caller: the turn's record, how it ended
    /// under `turns`, and for an adopted entry's own stream its failure, which
    /// its wait reads (RFC §6.4, §10.1 step 18).
    fn keep_stream_end(&mut self, job: &StreamJob, output: &crate::model::ChannelOutput) {
        if job.chained {
            return;
        }
        let mut record = output.response_metadata.to_metadata();
        let turns = record.shift_remove(TURNS);
        let merge = |caller: &mut Metadata| {
            super::reply_record::merge_channel_record(caller, &record);
            if let Some(turns) = turns.clone() {
                add_turns(caller, turns);
            }
        };
        let Lineage::Entry(entry) = job.lineage else {
            return merge(&mut self.trigger.result.response_metadata);
        };
        if let Some(delivered) = self.adopted_deliveries.get_mut(&entry) {
            merge(&mut delivered.response_metadata);
            delivered.error = delivered.error.take().or_else(|| output.error.clone());
        }
    }
}

/// The key of a caller's record under which each turn's end is (RFC §6.4).
const TURNS: &str = "turns";

/// Merges `turns`, a stream's entries by channel, into `record`'s, entry
/// by entry, so that one channel's end never replaces another's (Python's
/// `add_turn_entry`).
fn add_turns(record: &mut Metadata, turns: Value) {
    let Value::Object(turns) = turns else {
        return;
    };
    let into = record
        .entry(TURNS.to_owned())
        .or_insert_with(|| Value::Object(Metadata::new()));
    if !into.is_object() {
        *into = Value::Object(Metadata::new());
    }
    if let Value::Object(into) = into {
        into.extend(turns);
    }
}
