//! The turns of one room's discussion, one at a time (RFC §19.7.5 rules 6, 7,
//! 9, 11, 15, 16): Python's `orchestration/strategies/discussion/_driver.py`
//! but for the posts of task results (rule 19). The driver looks at the
//! stored queue when woken, and at least once a second; a turn is the rerun
//! of the event it answers, planned for one agent under the room's lock by
//! the core, which asks the discussion for it
//! ([`DiscussionRoom::planned_turn`]), and run in the room's lane. While it
//! runs, the driver reads the stored queue once a second: what another
//! process stored cuts it, the lease is renewed at each read, and the turn
//! is cut once the lease is another process's or ends unrenewed (rules 7
//! and 16).

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use roomkit_core::model::{ChannelId, Metadata, RoomContext, RoomEvent, Timestamp};
use roomkit_core::ops::{DiscussionTurn, NextTurn, StoppedTurn};
use roomkit_core::rules::discussion::config::DiscussionConfig;
use roomkit_core::rules::discussion::people::{person_label, records_people};
use roomkit_core::rules::discussion::{CONFIG_KEY, STATE_KEY, takes_turn};
use roomkit_core::rules::transcoding::text_of;
use roomkit_core::rules::transcript::LABEL_LIMIT;
use roomkit_core::rules::verification::release::withdrawn;
use serde_json::Value;

use super::SpeakQueueChange;
use super::notes::{AgentLine, TurnKind, TurnNotes, turn_notes};
use super::queue::{Entry, SpeakQueueState};
use super::room::{Change, DiscussionRoom, GivenTurn, read_queue};
use crate::InboundResult;
use crate::context::Section;
use crate::delivery_keys::NamedGuard;
use crate::discussion_turn::{issue_mark, retire_mark};
use crate::driver;
use crate::error::RoomKitError;
use crate::kit::RoomKit;
use crate::store::{ConversationStore, PatchRoomMetadataOptions};
use crate::tasks::text::quoted;
use crate::unwind;

/// How often the driver reads the stored queue again (rule 16).
const POLL: Duration = Duration::from_secs(1);

/// Why a turn is cut once another process uninstalled the discussion.
const UNINSTALLED: &str = "discussion_uninstalled";

/// Why a turn is cut once another process took its lease over (rule 16).
const LEASE_LOST: &str = "lease_lost";

/// Why a turn is cut once its lease ends before this process renews it
/// (rule 7).
const LEASE_EXPIRED: &str = "lease_expired";

/// The longest a failure holds the next look back.
const RETRY_MAX: Duration = Duration::from_secs(30);

/// How long a person's label and a message are quoted in a turn's notes:
/// a label whole, so a ranked one keeps the name it says its sender is not
/// (RFC §6.4).
const LABEL: usize = LABEL_LIMIT;
const QUOTE: usize = 160;

/// What a look at the queue came to.
enum Looked {
    Nothing,
    Ran,
    Gone,
}

/// What stopped a look of the watch short.
enum Stopped {
    /// The lease the turn runs under ends before this process could read
    /// the queue or renew it (Python's `_LeaseEndedError`); what failed,
    /// when a step failed rather than outlasted it.
    LeaseEnded(Option<RoomKitError>),
    /// A step failed with a read left to try it again before the lease
    /// ends.
    Failed(RoomKitError),
}

impl From<RoomKitError> for Stopped {
    fn from(error: RoomKitError) -> Self {
        Self::Failed(error)
    }
}

/// The lease's clock: seconds since the epoch, as Python's `time.time()`.
pub(super) fn now<S: ConversationStore>(kit: &RoomKit<S>) -> f64 {
    kit.inner
        .environment()
        .now()
        .duration_since(Timestamp::UNIX_EPOCH)
        .map_or(0.0, |elapsed| elapsed.as_secs_f64())
}

/// The end of a look's run: its cut is no longer the turn's, and a cut
/// waiting for it is told, however the run ends.
struct TurnOver<'room, S: ConversationStore>(&'room DiscussionRoom<S>);

/// Tells `TurnEnded` for its agent once dropped: a turn's end is told
/// however its record ends (rules 6 and 17).
struct TellsEnd<'room, S: ConversationStore>(&'room DiscussionRoom<S>, Option<ChannelId>);

impl<S: ConversationStore> Drop for TellsEnd<'_, S> {
    fn drop(&mut self) {
        if let Some(agent) = self.1.take() {
            self.0.fire(SpeakQueueChange::TurnEnded, vec![agent], None);
        }
    }
}

impl<S: ConversationStore> Drop for TurnOver<'_, S> {
    fn drop(&mut self) {
        self.0.state().cutter = None;
        self.0.turn_over.notify_waiters();
    }
}

/// Whether `state` holds a turn to give or a message waiting for a dispatch
/// policy: what the discussion's `done` is asked about.
fn holds_work(state: &SpeakQueueState) -> bool {
    !state.entries.is_empty() || !state.dispatching.is_empty()
}

impl<S: ConversationStore> DiscussionRoom<S> {
    /// Starts giving the turns, on a task of its own: it inherits no lease on
    /// the room's lock its installer holds.
    pub(crate) fn start(self: &Arc<Self>) {
        self.driven.store(true, Ordering::SeqCst);
        let room = Arc::clone(self);
        let task = tokio::spawn(room.drive());
        *self
            .task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(task);
        self.rouse();
    }

    async fn drive(self: Arc<Self>) {
        let mut failures = 0u32;
        while !self.stopping.load(Ordering::SeqCst) {
            // Cleared before the queue is read: a wake meanwhile is kept.
            self.woken.store(false, Ordering::SeqCst);
            let pause = match self.look().await {
                Ok(Looked::Ran) => {
                    failures = 0;
                    continue;
                }
                Ok(Looked::Gone) => return,
                Ok(Looked::Nothing) => {
                    failures = 0;
                    self.settled.store(true, Ordering::SeqCst);
                    if self.woken.load(Ordering::SeqCst) {
                        self.settled.store(false, Ordering::SeqCst);
                    }
                    POLL
                }
                Err(error) => {
                    failures = failures.saturating_add(1);
                    tracing::error!(
                        target: "roomkit::orchestration::discussion",
                        room_id = %self.room_id,
                        %error,
                        "the discussion could not give a turn"
                    );
                    RETRY_MAX.min(Duration::from_secs(1 << failures.min(5).saturating_sub(1)))
                }
            };
            tokio::select! {
                () = self.wake.notified() => {}
                () = tokio::time::sleep(pause) => {}
            }
            self.settled.store(false, Ordering::SeqCst);
        }
    }

    // -- Giving a turn --

    async fn look(&self) -> Result<Looked, RoomKitError> {
        let Some(kit) = self.kit.upgrade() else {
            return Ok(Looked::Gone);
        };
        let room = kit.store().get_room(&self.room_id).await?;
        let metadata = room.map(|room| room.metadata).unwrap_or_default();
        let peek = read_queue(&metadata);
        let seen = {
            let mut live = self.state();
            live.queue = peek.clone();
            live.seen_version
        };
        let stored = DiscussionConfig::stored(&metadata);
        let forgotten = self.state().forgotten;
        let Some(stored) = stored.filter(|_| !forgotten) else {
            // Another process uninstalled the discussion, whether or not one
            // was installed again since: this process leaves it, writing
            // nothing back (rule 16).
            let this = std::ptr::from_ref(self).cast::<()>() as usize;
            let left = kit.leave_uninstalled_here(&self.room_id, this).await;
            return Ok(if left { Looked::Gone } else { Looked::Nothing });
        };
        // Routed by the configuration the room stores from then on.
        self.follow(stored);
        if !self.gives_turns_of(&kit, &peek).await? {
            return Ok(Looked::Nothing);
        }
        let time = now(&kit);
        // A lease past its end stays its holder's one read more: a holder
        // that could not renew it cut its turn at its end (rule 7).
        if peek.over || peek.held_elsewhere(&self.me, time, POLL.as_secs_f64()) {
            return Ok(Looked::Nothing);
        }
        if seen == Some(peek.version)
            && peek.holds(&self.me, time)
            && !peek.renew_due(&self.me, time)
        {
            return Ok(Looked::Nothing);
        }
        // Asked off the room's lock, of a queue that holds something: done
        // may uninstall the discussion it is asked about.
        let done = if holds_work(&peek) {
            Some(self.ask_done().await)
        } else {
            None
        };
        if self.stopping.load(Ordering::SeqCst) || self.state().closed {
            return Ok(Looked::Nothing);
        }
        self.state().done = done;
        // What a cut of the turn this look gives cancels (Python's
        // `_abandon`).
        let named = kit.inner.delivery_keys.naming(None, None);
        self.state().cutter = Some(Arc::clone(named.named()));
        let ran = {
            // Over however the run ends, a stop's abort included.
            let _over = TurnOver(self);
            self.run_watched(&kit, named).await
        };
        let (given, deciding) = {
            let mut live = self.state();
            (live.turn.take(), live.deciding.take())
        };
        if let Some(pending) = deciding {
            ran?;
            // Off the lock, before any other turn: the policy may take its
            // bound.
            self.decide(&kit, pending).await;
            self.rouse();
            return Ok(Looked::Ran);
        }
        let Some(given) = given else {
            ran?;
            return Ok(Looked::Nothing);
        };
        let responses = match ran {
            Ok(result) => result.response_events,
            Err(error) => {
                tracing::error!(
                    target: "roomkit::orchestration::discussion",
                    room_id = %self.room_id,
                    agent = %given.entry.agent,
                    %error,
                    "a discussion's turn failed"
                );
                Vec::new()
            }
        };
        retire_mark(&given.mark);
        self.ended(&kit, &given.entry, responses).await;
        Ok(Looked::Ran)
    }

    // -- Running it --

    /// Runs the look's turn, watching the stored queue while it runs
    /// (Python's `_run` and its `_watch`): the watch ends with the run.
    async fn run_watched(
        &self,
        kit: &RoomKit<S>,
        named: NamedGuard,
    ) -> Result<InboundResult, RoomKitError> {
        let running = kit.run_discussion_turn(&self.room_id, named);
        tokio::select! {
            ran = running => ran,
            never = self.watch(kit) => match never {},
        }
    }

    /// While a turn this process gave runs: what another process stored
    /// cuts it (the discussion uninstalled, its agent set to listen or
    /// removed), and the lease is renewed at each read. A lease another
    /// process took, or one this process cannot renew before it ends, cuts
    /// it too: past its end, another process may take the lease and give
    /// the next turn (rules 6, 7 and 16).
    async fn watch(&self, kit: &RoomKit<S>) -> Infallible {
        loop {
            tokio::time::sleep(POLL).await;
            if let Err(error) = self.watch_once(kit).await {
                tracing::warn!(
                    target: "roomkit::orchestration::discussion",
                    room_id = %self.room_id,
                    %error,
                    "watching a discussion's turn failed"
                );
            }
        }
    }

    /// One look of the watch at the stored queue (Python's `_look`): what
    /// it says is applied, then the lease renewed, both before the lease
    /// ends.
    async fn watch_once(&self, kit: &RoomKit<S>) -> Result<(), RoomKitError> {
        // Planned under the room's lock first: no turn, nothing to watch.
        if self.state().turn.is_none() {
            return Ok(());
        }
        match self.look_before_the_lease_ends(kit).await {
            Ok(()) => Ok(()),
            Err(Stopped::Failed(error)) => Err(error),
            Err(Stopped::LeaseEnded(cause)) => {
                if let Some(agent) = self.uncut_agent() {
                    let cause =
                        cause.map_or_else(|| "timed out".to_owned(), |error| error.to_string());
                    tracing::warn!(
                        target: "roomkit::orchestration::discussion",
                        room_id = %self.room_id,
                        %agent,
                        %cause,
                        "the lease ends before this process renews it; the turn is cut"
                    );
                }
                self.cut_running(kit, LEASE_EXPIRED).await
            }
        }
    }

    /// The read and the renewal of a look, each bounded by the lease's end.
    async fn look_before_the_lease_ends(&self, kit: &RoomKit<S>) -> Result<(), Stopped> {
        let deadline = self.deadline(kit)?;
        let reading = async { Ok(kit.store().get_room(&self.room_id).await?) };
        let room = self.within(kit, deadline, reading).await?;
        let metadata = room.map(|room| room.metadata).unwrap_or_default();
        let state = read_queue(&metadata);
        let stored = DiscussionConfig::stored(&metadata);
        let uninstalled = stored.is_none();
        if let Some(stored) = stored {
            // Routed by the configuration the room stores from then on.
            self.follow(stored);
        }
        if let Some(reason) = self.read_cut(&state, uninstalled) {
            self.cut_running(kit, reason).await?;
        }
        if self.within(kit, deadline, self.renew(kit, &state)).await? {
            // Its taking the lease over ended this turn (rule 16).
            if let Some(agent) = self.uncut_agent() {
                tracing::warn!(
                    target: "roomkit::orchestration::discussion",
                    room_id = %self.room_id,
                    %agent,
                    "another process took the lease over; the turn is cut"
                );
            }
            self.cut_running(kit, LEASE_LOST).await?;
        }
        Ok(())
    }

    /// Until when, on tokio's clock, a look may read the queue and renew
    /// the lease (Python's `_deadline`): the lease's end; for a look that
    /// wakes past it, the process held meanwhile (its turn with it), one
    /// read later, when another process may take it over. `None`: no
    /// instant tokio's clock holds, which bounds nothing.
    fn deadline(&self, kit: &RoomKit<S>) -> Result<Option<tokio::time::Instant>, Stopped> {
        let mut left = self.lease_left(kit);
        if left <= 0.0 {
            left += POLL.as_secs_f64();
        }
        if left <= 0.0 {
            return Err(Stopped::LeaseEnded(None));
        }
        Ok(Duration::try_from_secs_f64(left)
            .ok()
            .and_then(|left| tokio::time::Instant::now().checked_add(left)))
    }

    /// How long the lease the running turn runs under has left, in seconds,
    /// as this process last stored it.
    fn lease_left(&self, kit: &RoomKit<S>) -> f64 {
        let ends = self
            .state()
            .turn
            .as_ref()
            .map_or(0.0, |turn| turn.lease_ends);
        ends - now(kit)
    }

    /// `step`, awaited before `deadline` (Python's `_within`). One that
    /// outlasts it, or fails with no read left to try it again before the
    /// lease ends, ends the lease; another failure is tried again on the
    /// next look.
    async fn within<T>(
        &self,
        kit: &RoomKit<S>,
        deadline: Option<tokio::time::Instant>,
        step: impl Future<Output = Result<T, RoomKitError>>,
    ) -> Result<T, Stopped> {
        let done = match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, step)
                .await
                .map_err(|_| Stopped::LeaseEnded(None))?,
            None => step.await,
        };
        done.map_err(|error| {
            if self.lease_left(kit) < POLL.as_secs_f64() {
                Stopped::LeaseEnded(Some(error))
            } else {
                Stopped::Failed(error)
            }
        })
    }

    /// Renews the lease the running turn runs under, read as `state`
    /// (Python's `_renew`); whether another process took it over. One no
    /// process holds (the discussion uninstalled elsewhere, maybe installed
    /// again since) is the driver's next look's.
    async fn renew(&self, kit: &RoomKit<S>, state: &SpeakQueueState) -> Result<bool, RoomKitError> {
        let me = Some(self.me.as_str());
        if state.lease_holder.as_deref() != me {
            return Ok(state.lease_holder.is_some());
        }
        let renewed = self
            .editing(kit, |editing, _| {
                if editing.lease_holder.as_deref() != me {
                    return Err(editing.lease_holder.is_some());
                }
                editing.take_lease(&self.me, now(kit), 0.0);
                Ok(editing.lease_expires)
            })
            .await?;
        match renewed {
            Err(taken) => Ok(taken),
            Ok(lease_ends) => {
                // Renewed once the edit is stored: a write that failed
                // renewed nothing.
                if let Some(turn) = self.state().turn.as_mut() {
                    turn.lease_ends = lease_ends;
                }
                Ok(false)
            }
        }
    }

    /// What the stored queue, just read as `state`, makes of the running
    /// turn: kept as the queue read, the turn's agent listening as read,
    /// and why it cuts the turn, if it does.
    fn read_cut(&self, state: &SpeakQueueState, uninstalled: bool) -> Option<&'static str> {
        let mut live = self.state();
        live.queue = state.clone();
        let turn = live.turn.as_mut()?;
        let reason = self.cut_reason(turn, state, uninstalled);
        turn.listening = state.listening.contains(&turn.entry.agent);
        reason
    }

    /// The agent of the running turn, unless it was cut already.
    fn uncut_agent(&self) -> Option<ChannelId> {
        let live = self.state();
        let turn = live.turn.as_ref().filter(|turn| !turn.cut)?;
        Some(turn.entry.agent.clone())
    }

    /// Cuts the running turn once, as a `Cancel` does (rule 12; Python's
    /// `_cut_running`). A turn whose discussion another process
    /// uninstalled is gone for every process: its end is recorded nowhere
    /// (rule 1).
    async fn cut_running(
        &self,
        kit: &RoomKit<S>,
        reason: &'static str,
    ) -> Result<(), RoomKitError> {
        let agent = {
            let mut live = self.state();
            let Some(turn) = live.turn.as_mut().filter(|turn| !turn.cut) else {
                return Ok(());
            };
            turn.cut = true;
            let agent = turn.entry.agent.clone();
            if reason == UNINSTALLED {
                live.forgotten = true;
            }
            agent
        };
        self.cut(kit, &agent, reason).await
    }

    /// Why the stored queue, just read as `state`, cuts `turn`, if it does:
    /// Python's `_cut_reason`.
    fn cut_reason(
        &self,
        turn: &GivenTurn,
        state: &SpeakQueueState,
        uninstalled: bool,
    ) -> Option<&'static str> {
        let agent = &turn.entry.agent;
        if uninstalled {
            return Some(UNINSTALLED);
        }
        if state.listening.contains(agent) && !turn.listening {
            return Some("listen_only");
        }
        if !self.config().agents.contains(agent) {
            return Some("agent_removed");
        }
        None
    }

    /// What `done` answers; a failure reads as not done.
    async fn ask_done(&self) -> bool {
        let Some(done) = self.done_fn() else {
            return false;
        };
        let asked = done(self.room_id.clone());
        match unwind::unwinding(asked).await {
            Ok(answer) => answer,
            Err(panic) => {
                tracing::error!(
                    target: "roomkit::orchestration::discussion",
                    room_id = %self.room_id,
                    message = %unwind::message(&*panic),
                    "the discussion's done failed"
                );
                false
            }
        }
    }

    /// The turn to give, the room's lock held: told by the core when the
    /// driver asks for a turn. `done` is what the discussion's `done` answered
    /// off the lock, none when the queue peeked held nothing to ask it about:
    /// one that holds something now is planned on the next look, once `done`
    /// was asked (rule 15).
    pub(super) async fn planned_turn(
        &self,
        context: RoomContext,
    ) -> Result<NextTurn, RoomKitError> {
        let kit = self.kit()?;
        let done = self.state().done.take();
        let room = kit.store().get_room(&self.room_id).await?;
        let metadata = room.map(|room| room.metadata).unwrap_or_default();
        // Uninstalled or stopped since the peek: no turn.
        let Some(stored) = DiscussionConfig::stored(&metadata) else {
            return Ok(NextTurn::default());
        };
        if self.state().closed {
            return Ok(NextTurn::default());
        }
        // Routed by the configuration the room stores (Python's `editing`).
        self.follow(stored);
        let before = read_queue(&metadata);
        let mut state = before.clone();
        if !state.take_lease(&self.me, now(&kit), POLL.as_secs_f64()) {
            return Ok(NextTurn::default());
        }
        let mut changes = Vec::new();
        // Holding something now that it held nothing at the peek: done was not
        // asked, the next look asks it, which this one's version must not
        // read as nothing new.
        let unasked = done.is_none() && holds_work(&state);
        let pending =
            Self::waiting_decision(&state, done == Some(true), self.max_turns_given(&state));
        let next = if unasked {
            self.rouse();
            NextTurn::default()
        } else if let Some(pending) = pending {
            // Decided by the look once this plan is over, off the lock.
            self.state().deciding = Some(pending);
            NextTurn::default()
        } else {
            self.pick(&kit, &mut state, &context, done == Some(true), &mut changes)
                .await?
        };
        if state.queue_differs(&before) {
            state.version += 1;
        }
        self.state().queue = state.clone();
        if state != before
            && let Err(error) = self.store_queue(&kit, &state).await
        {
            // No turn was given: a stored queue that cannot be written is the
            // look's failure, which backs off. The queue read shows the change
            // until the next look, as Python's state changed in place does.
            if let Some(given) = self.state().turn.take() {
                retire_mark(&given.mark);
            }
            return Err(error);
        }
        // Once stored, as Python's: a refused plan is planned again, whatever
        // message lands on the number the refused write would have had.
        if !unasked {
            self.state().seen_version = Some(state.version);
        }
        self.fire_all(changes);
        Ok(next)
    }

    /// The first message waiting for the dispatch policy, unless the
    /// discussion is over or ends now, its queue dropped with them.
    fn waiting_decision(
        state: &SpeakQueueState,
        done: bool,
        max_turns_given: bool,
    ) -> Option<super::queue::Pending> {
        if state.over || done || max_turns_given {
            return None;
        }
        state.dispatching.first().cloned()
    }

    async fn store_queue(
        &self,
        kit: &RoomKit<S>,
        state: &SpeakQueueState,
    ) -> Result<(), RoomKitError> {
        let value =
            serde_json::to_value(state).map_err(|error| RoomKitError::Value(error.to_string()))?;
        let patch = Metadata::from_iter([(STATE_KEY.to_owned(), value)]);
        kit.store()
            .patch_room_metadata(&self.room_id, patch, PatchRoomMetadataOptions::default())
            .await?;
        Ok(())
    }

    async fn pick(
        &self,
        kit: &RoomKit<S>,
        state: &mut SpeakQueueState,
        context: &RoomContext,
        done: bool,
        changes: &mut Vec<Change>,
    ) -> Result<NextTurn, RoomKitError> {
        let mut next = NextTurn::default();
        if state.over {
            return Ok(next);
        }
        if done || self.max_turns_given(state) {
            self.end(state, changes);
            return Ok(next);
        }
        loop {
            let pick = state.next_turn(self.config().max_depth);
            for entry in pick
                .stopped
                .iter()
                .chain(pick.dropped.iter().filter(|entry| entry.regenerate))
            {
                next.stopped.extend(self.stopped(kit, entry).await?);
            }
            self.drop_instructions(&pick.dropped, state, changes);
            let Some(mut entry) = pick.entry else {
                Self::wait_or_idle(state, context, changes);
                return Ok(next);
            };
            if self.hand_over(kit, state, &mut entry, changes) {
                return Ok(next);
            }
            if !state.entries.contains(&entry) {
                continue;
            }
            if let Some(turn) = self.give(kit, state, entry, context, changes).await? {
                next.turn = Some(Box::new(turn));
                return Ok(next);
            }
            // Stopped while it read: the entry is still queued, and stays.
            if self.state().closed {
                return Ok(next);
            }
        }
    }

    /// An instruction whose text another process holds: the lease goes to that
    /// process, once; whether it went. Back here, the instruction is dropped:
    /// its process did not take its turn (rule 16).
    fn hand_over(
        &self,
        kit: &RoomKit<S>,
        state: &mut SpeakQueueState,
        entry: &mut Entry,
        changes: &mut Vec<Change>,
    ) -> bool {
        let Some(holder) = entry.holder.clone().filter(|holder| *holder != self.me) else {
            return false;
        };
        if entry.instruction.is_none() {
            return false;
        }
        if entry.handed {
            state.remove(entry);
            self.drop_instructions(std::slice::from_ref(entry), state, changes);
            return false;
        }
        if state.mark_handed(entry) {
            state.grant_lease(&holder, now(kit));
        }
        true
    }

    /// `entry`'s turn; `None`, the entry dropped, when it cannot be given: the
    /// event it answers is gone, or the agent may no longer read it.
    async fn give(
        &self,
        kit: &RoomKit<S>,
        state: &mut SpeakQueueState,
        entry: Entry,
        context: &RoomContext,
        changes: &mut Vec<Change>,
    ) -> Result<Option<DiscussionTurn>, RoomKitError> {
        let answered = self.answered(kit, &entry).await?;
        let answered = answered.filter(|answered| takes_turn(answered, context, &entry.agent));
        let Some(mut answered) = answered else {
            self.not_given(state, &entry, changes);
            return Ok(None);
        };
        if !entry.post
            && kit
                .known_offline(
                    std::slice::from_ref(&entry.agent),
                    &self.room_id,
                    self.organization_id.as_deref(),
                )
                .await
                .contains(&entry.agent)
        {
            self.skip_offline(state, &entry, &answered, changes);
            return Ok(None);
        }
        // At the depth its ask stands at when that is deeper than its own.
        if let Some(ask) = entry.answered() {
            answered.chain_depth = answered.chain_depth.max(ask.depth);
        }
        let notes = self.notes(kit, &entry, &answered, context);
        let mark = issue_mark(vec![notes]);
        {
            // One read with the set: a stop either sees this turn to cut, or
            // this sees the stop and gives none.
            let mut live = self.state();
            let listening = state.listening.contains(&entry.agent);
            if !live.closed && state.take(&entry) {
                live.turn_rows.clear();
                live.turn = Some(GivenTurn {
                    entry: entry.clone(),
                    mark: mark.clone(),
                    listening,
                    cut: false,
                    lease_ends: state.lease_expires,
                });
            } else {
                drop(live);
                retire_mark(&mark);
                return Ok(None);
            }
        }
        if self.max_turns_given(state) {
            // Over once the last turn is given; that turn ends as it would.
            self.end(state, changes);
        }
        changes.push((
            SpeakQueueChange::TurnGiven,
            vec![entry.agent.clone()],
            Some(answered.id.to_string()),
        ));
        Ok(Some(DiscussionTurn {
            agent: entry.agent,
            answered: Box::new(answered),
            mark,
            max_depth: self.config().max_depth,
        }))
    }

    fn skip_offline(
        &self,
        state: &mut SpeakQueueState,
        entry: &Entry,
        answered: &RoomEvent,
        changes: &mut Vec<Change>,
    ) {
        state.remove(entry);
        changes.push((
            SpeakQueueChange::TurnSkipped,
            vec![entry.agent.clone()],
            Some(answered.id.to_string()),
        ));
        self.drop_instructions(std::slice::from_ref(entry), state, changes);
        tracing::info!(target: "roomkit::orchestration::discussion", room_id = %self.room_id, agent = %entry.agent, "No online executor serves this queued agent");
    }

    fn not_given(&self, state: &mut SpeakQueueState, entry: &Entry, changes: &mut Vec<Change>) {
        state.remove(entry);
        self.drop_instructions(std::slice::from_ref(entry), state, changes);
        tracing::info!(
            target: "roomkit::orchestration::discussion",
            room_id = %self.room_id,
            agent = %entry.agent,
            "a discussion's turn not given: nothing it can answer"
        );
    }

    fn max_turns_given(&self, state: &SpeakQueueState) -> bool {
        self.config()
            .max_turns
            .is_some_and(|max_turns| state.turns_given >= max_turns)
    }

    async fn answered(
        &self,
        kit: &RoomKit<S>,
        entry: &Entry,
    ) -> Result<Option<RoomEvent>, RoomKitError> {
        if let Some(instruction) = &entry.instruction {
            return Ok(self.state().instructions.get(instruction).cloned());
        }
        let Some(ask) = entry.answered() else {
            return Ok(None);
        };
        let Ok(id) = ask.event_id.parse() else {
            return Ok(None);
        };
        let event = kit.store().get_event(&id).await?;
        // A message deleted while the turn waited is gone too (RFC §10.3).
        Ok(event.filter(|event| event.room_id == self.room_id && !withdrawn(event)))
    }

    /// The record of a turn the depth limit stopped, once (RFC §8.3).
    async fn stopped(
        &self,
        kit: &RoomKit<S>,
        entry: &Entry,
    ) -> Result<Option<StoppedTurn>, RoomKitError> {
        let Some(ask) = entry.answered() else {
            return Ok(None);
        };
        let Ok(id) = ask.event_id.parse() else {
            return Ok(None);
        };
        let event = kit.store().get_event(&id).await?;
        let event = event.filter(|event| event.room_id == self.room_id);
        Ok(event.map(|answered| StoppedTurn {
            agent: entry.agent.clone(),
            answered: Box::new(answered),
            max_depth: self.config().max_depth,
        }))
    }

    fn notes(
        &self,
        kit: &RoomKit<S>,
        entry: &Entry,
        answered: &RoomEvent,
        context: &RoomContext,
    ) -> String {
        let config = self.config();
        let others: Vec<AgentLine> = config
            .agents
            .iter()
            .filter(|agent| **agent != entry.agent)
            .map(|agent| self.agent_line(kit, agent))
            .collect();
        let kind = if entry.instruction.is_some() {
            TurnKind::Instruction
        } else if entry.regenerate {
            TurnKind::Regenerate
        } else {
            TurnKind::Answer
        };
        let askers: Vec<String> = entry
            .askers()
            .iter()
            .map(|(asker, person)| {
                if *person {
                    quoted(asker, LABEL)
                } else {
                    ChannelId::new(asker)
                        .map_or_else(|_| format!("@{asker}"), |agent| config.label(&agent))
                }
            })
            .collect();
        let author = if config.agents.contains(&answered.source.channel_id) {
            config.label(&answered.source.channel_id)
        } else {
            quoted(&person_label(answered, context), LABEL)
        };
        let answering = format!(
            "the message from {author}, {}",
            quoted(&text_of(&answered.content), QUOTE)
        );
        turn_notes(
            config.handle(&entry.agent),
            &others,
            &self.people(context),
            TurnNotes {
                kind,
                answering: &answering,
                askers: &askers,
                silent_token: &config.silent_token,
                rules: config.rules.as_deref(),
            },
        )
    }

    fn agent_line(&self, kit: &RoomKit<S>, agent: &ChannelId) -> AgentLine {
        let channel = kit.inner.channel(agent);
        AgentLine {
            handle: self.config().handle(agent).to_owned(),
            name: channel.as_ref().and_then(|channel| channel.name()),
            role: channel.as_ref().and_then(|channel| channel.role()),
            description: channel.as_ref().and_then(|channel| channel.description()),
        }
    }

    // -- A turn's end --

    /// The turn has ended (rule 6): told once its end is recorded, or its
    /// record cut by a stop, unless the discussion is being forgotten and its
    /// queue goes, when nothing is recorded (rules 1 and 17).
    async fn ended(&self, kit: &RoomKit<S>, entry: &Entry, responses: Vec<RoomEvent>) {
        // Told however the record ends, a stop's abort included, as Python's
        // `_ended` tells it in a `finally`.
        let told = TellsEnd(self, Some(entry.agent.clone()));
        let forgotten = self.state().forgotten;
        let queued = if forgotten {
            Vec::new()
        } else {
            match self.record_end(kit, entry, responses).await {
                Ok(queued) => queued,
                Err(error) => {
                    tracing::error!(
                        target: "roomkit::orchestration::discussion",
                        room_id = %self.room_id,
                        agent = %entry.agent,
                        %error,
                        "the end of a discussion's turn was not read"
                    );
                    Vec::new()
                }
            }
        };
        drop(told);
        if !queued.is_empty() {
            self.fire(SpeakQueueChange::Queued, queued, None);
        }
    }

    /// Records the end of the turn in the stored queue, where what it
    /// delivered queues whom it names; the agents it queued.
    async fn record_end(
        &self,
        kit: &RoomKit<S>,
        entry: &Entry,
        responses: Vec<RoomEvent>,
    ) -> Result<Vec<ChannelId>, RoomKitError> {
        let section = Section::enter(&kit.inner.locks, &self.room_id).await?;
        let recording = async {
            let reading = kit.inner.config().recent_events().read_by_the_caller();
            let context = driver::load_context(kit.store(), &self.room_id, reading).await?;
            let Some(context) = context else {
                return Ok(Vec::new());
            };
            self.edit(kit, |state, live| {
                if !state.held_elsewhere(&self.me, now(kit), 0.0) {
                    // Else the process that took the lease over ended this
                    // turn, and `speaking` names its own (rule 16).
                    state.ended(&entry.agent);
                }
                if let Some(instruction) = &entry.instruction {
                    forget_instruction(instruction, state, &mut live.instructions);
                }
                let mut rows = std::mem::take(&mut live.turn_rows);
                for response in responses {
                    if !rows.iter().any(|row| row.id == response.id) {
                        rows.push(response);
                    }
                }
                let mut queued: Vec<ChannelId> = Vec::new();
                if !state.over {
                    for row in &rows {
                        for agent in self.queue_turn_names(state, &entry.agent, row, &context) {
                            if !queued.contains(&agent) {
                                queued.push(agent);
                            }
                        }
                    }
                }
                queued
            })
            .await
        };
        section.scope(recording).await
    }

    // -- Waiting, ending --

    /// No agent can take a turn: wait for a person when one is owed (rule 10),
    /// else stay idle until an event asks for a turn.
    fn wait_or_idle(state: &mut SpeakQueueState, context: &RoomContext, changes: &mut Vec<Change>) {
        let has_people = state.people_spoke || records_people(context);
        if state.waiting || !state.owes_a_person(has_people) {
            return;
        }
        state.waiting = true;
        changes.push((SpeakQueueChange::Waiting, Vec::new(), None));
    }

    /// The discussion is over (rule 15): no further turn, the queue dropped.
    fn end(&self, state: &mut SpeakQueueState, changes: &mut Vec<Change>) {
        if state.over {
            return;
        }
        state.over = true;
        let dropped = state.drop_all();
        self.drop_instructions(&dropped, state, changes);
        changes.push((SpeakQueueChange::Over, Vec::new(), None));
    }

    /// Forgets the text of instruction turns no longer queued, and tells them.
    pub(super) fn drop_instructions(
        &self,
        dropped: &[Entry],
        state: &SpeakQueueState,
        changes: &mut Vec<Change>,
    ) {
        let mut live = self.state();
        for entry in dropped {
            let Some(instruction) = &entry.instruction else {
                continue;
            };
            forget_instruction(instruction, state, &mut live.instructions);
            changes.push((
                SpeakQueueChange::InstructionDropped,
                vec![entry.agent.clone()],
                Some(instruction.clone()),
            ));
        }
    }

    // -- Stopping --

    /// Stops the driver: a turn running is cut, what it committed kept.
    pub(super) async fn stop_driver(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        self.rouse();
        let task = self
            .task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(task) = task else {
            return;
        };
        // From the driver's own task (a done that uninstalls), it ends once
        // this call returns.
        if tokio::task::try_id() == Some(task.id()) {
            return;
        }
        task.abort();
        if let Err(error) = task.await
            && !error.is_cancelled()
        {
            tracing::error!(
                target: "roomkit::orchestration::discussion",
                room_id = %self.room_id,
                %error,
                "a discussion's driver failed"
            );
        }
        // A plan under way holds the room's lock: its turn's given told first,
        // then its end, whichever stops it.
        if let Some(kit) = self.kit.upgrade() {
            drop(Section::enter(&kit.inner.locks, &self.room_id).await);
        }
        let given = self.state().turn.take();
        let Some(given) = given else {
            return;
        };
        retire_mark(&given.mark);
        // Its end recorded, what it committed queueing whom it names, unless
        // the queue goes with it (rule 1).
        match self.kit.upgrade() {
            Some(kit) => self.ended(&kit, &given.entry, Vec::new()).await,
            None => self.fire(SpeakQueueChange::TurnEnded, vec![given.entry.agent], None),
        }
    }

    /// Stops taking part: the lease given up, the instruction turns only this
    /// process could give told dropped; the queue stays (rule 16).
    pub(super) async fn stop_taking_part(&self) {
        self.state().closed = true;
        self.stop_driver().await;
        let Some(kit) = self.kit.upgrade() else {
            return;
        };
        if let Err(error) = self.give_up(&kit, false).await {
            tracing::error!(
                target: "roomkit::orchestration::discussion",
                room_id = %self.room_id,
                %error,
                "the speak queue was not stored"
            );
        }
    }

    /// Gives no turn from this process: the lease released, the instruction
    /// turns only this process could give told dropped; the rest of the
    /// queue stays (rule 16). Python's `give_up`. `unless_it_drives`: not
    /// when, read under the room's lock, the room stores this process's
    /// install again (one it made meanwhile).
    async fn give_up(&self, kit: &RoomKit<S>, unless_it_drives: bool) -> Result<(), RoomKitError> {
        let (mine, state) = self
            .editing(kit, |state, _| {
                // An install of this process took the room back meanwhile,
                // under its lock: it gives the turns again.
                if unless_it_drives && self.gives_turns() {
                    return (Vec::new(), state.clone());
                }
                state.release_lease(&self.me);
                let mine: Vec<Entry> = state
                    .entries
                    .iter()
                    .filter(|entry| entry.holder.as_deref() == Some(self.me.as_str()))
                    .cloned()
                    .collect();
                state.entries.retain(|entry| !mine.contains(entry));
                (mine, state.clone())
            })
            .await?;
        let mut changes = Vec::new();
        self.drop_instructions(&mine, &state, &mut changes);
        self.fire_all(changes);
        Ok(())
    }

    /// Whether this process gives the turns of the discussion the room
    /// stores, `peek` its queue as just read: one whose host installed
    /// another gives up the lease and the turns only it could give, queued
    /// before (rule 16). Python's `_gives_turns`.
    async fn gives_turns_of(
        &self,
        kit: &RoomKit<S>,
        peek: &SpeakQueueState,
    ) -> Result<bool, RoomKitError> {
        if self.gives_turns() {
            return Ok(true);
        }
        let held = peek
            .entries
            .iter()
            .any(|entry| entry.holder.as_deref() == Some(self.me.as_str()));
        if held || peek.holds(&self.me, now(kit)) {
            self.give_up(kit, true).await?;
        }
        Ok(false)
    }

    /// Leaves the discussion, for every process (rule 1): the turns stopped,
    /// the running one cut with its end recorded nowhere, then the queue and
    /// the configuration forgotten.
    pub(super) async fn forget_discussion(&self) -> Result<(), RoomKitError> {
        {
            let mut live = self.state();
            live.closed = true;
            live.forgotten = true;
        }
        self.stop_driver().await;
        let kit = self.kit()?;
        let section = Section::enter(&kit.inner.locks, &self.room_id).await?;
        let patch = Metadata::from_iter([
            (STATE_KEY.to_owned(), Value::Null),
            (CONFIG_KEY.to_owned(), Value::Null),
        ]);
        let forgetting = async {
            let room = kit.store().get_room(&self.room_id).await?;
            let metadata = room.map(|room| room.metadata).unwrap_or_default();
            let state = read_queue(&metadata);
            kit.store()
                .patch_room_metadata(&self.room_id, patch, PatchRoomMetadataOptions::default())
                .await?;
            Ok::<_, RoomKitError>(state)
        };
        let state = section.scope(forgetting).await?;
        drop(section);
        for entry in &state.entries {
            if entry.holder.as_deref() == Some(self.me.as_str()) {
                self.fire(
                    SpeakQueueChange::InstructionDropped,
                    vec![entry.agent.clone()],
                    entry.instruction.clone(),
                );
            }
        }
        Ok(())
    }
}

/// Forgets an instruction's text once no queued turn takes it: one
/// instruction may be addressed to several agents.
fn forget_instruction(
    instruction: &str,
    state: &SpeakQueueState,
    instructions: &mut std::collections::HashMap<String, RoomEvent>,
) {
    let held = state
        .entries
        .iter()
        .any(|entry| entry.instruction.as_deref() == Some(instruction));
    if !held {
        instructions.remove(instruction);
    }
}

#[cfg(test)]
mod tests {
    use roomkit_core::model::{
        ChannelBinding, ChannelCategory, ChannelDirection, ChannelType, EventSource, RoomId,
        TextContent,
    };

    use roomkit_core::ops::SequentialEnvironment;

    use super::*;
    use crate::kit::{CreateRoomOptions, RoomKitOptions};
    use crate::orchestration::strategies::discussion::queue::Ask;
    use crate::store::CommitEventOptions;

    // Python: _driver.py `_next_turn` reads `closed` off the lock, the cancel
    // acting under it: a stop during the plan finds a turn to cut, or none is
    // given, and no end of the discussion is written for it.
    #[tokio::test]
    async fn a_room_stopped_since_the_peek_gives_no_turn() {
        let room_id = RoomId::new("r1").unwrap();
        let agent = ChannelId::new("a").unwrap();
        let ops = ChannelId::new("ops").unwrap();
        let kit = RoomKit::new(RoomKitOptions::default());
        let options = CreateRoomOptions {
            room_id: Some(room_id.clone()),
            ..Default::default()
        };
        let created = kit.create_room(options).await.unwrap();
        let mut asked = RoomEvent::new(
            room_id.clone(),
            EventSource::new(ops.clone(), ChannelType::SMS),
            TextContent::new("@a hello"),
        );
        asked.index = 1;
        let asked = kit
            .store()
            .commit_event(&room_id, asked, CommitEventOptions::default())
            .await
            .unwrap();
        let config: DiscussionConfig = serde_json::from_value(serde_json::json!({
            "agents": ["a"], "max_depth": 5
        }))
        .unwrap();
        let install = super::super::room::Install {
            config,
            done: None,
            dispatch: None,
            owner: 0,
        };
        let room = DiscussionRoom::new(&kit, room_id.clone(), None, install);
        room.load().await.unwrap();
        room.editing(&kit, |state, _| {
            let ask = Ask {
                event_id: asked.id.to_string(),
                depth: 0,
                asker: "ops".to_owned(),
                index: asked.index,
                person: true,
                named: true,
            };
            state.ask(&agent, ask, true, None);
        })
        .await
        .unwrap();
        let mut context = RoomContext::new(created);
        for (channel, channel_type, category) in [
            (&ops, ChannelType::SMS, ChannelCategory::Transport),
            (&agent, ChannelType::AI, ChannelCategory::Intelligence),
        ] {
            context.bindings.push(ChannelBinding::at(
                channel.clone(),
                room_id.clone(),
                channel_type,
                category,
                ChannelDirection::Bidirectional,
                Timestamp::UNIX_EPOCH,
            ));
        }

        room.state().done = Some(false);
        room.state().closed = true;
        let stopped = room.planned_turn(context.clone()).await.unwrap();
        assert!(stopped.turn.is_none());
        assert!(room.state().turn.is_none());
        // Nor does a done that holds end it, once stopped.
        room.state().done = Some(true);
        room.planned_turn(context.clone()).await.unwrap();
        let stored = kit.store().get_room(&room_id).await.unwrap().unwrap();
        assert!(!read_queue(&stored.metadata).over);

        // Not stopped, the same plan gives it: what the first read kept out.
        room.state().closed = false;
        room.state().done = Some(false);
        let given = room.planned_turn(context).await.unwrap();
        assert_eq!(given.turn.map(|turn| turn.agent), Some(agent));
    }

    // Python: tests/orchestration/test_discussion_dispatch.py::test_a_message_the_peek_missed_asks_done_first[waiting-for-dispatch]:
    // a message left to the policy between the peek and the plan is planned
    // once done was asked, which ends the discussion before any decision.
    #[tokio::test]
    async fn a_message_the_peek_missed_asks_done_first() {
        let room_id = RoomId::new("r1").unwrap();
        let kit = RoomKit::new(RoomKitOptions::default());
        let options = CreateRoomOptions {
            room_id: Some(room_id.clone()),
            ..Default::default()
        };
        let created = kit.create_room(options).await.unwrap();
        let config: DiscussionConfig = serde_json::from_value(serde_json::json!({
            "agents": ["a"], "max_depth": 5, "dispatch": true
        }))
        .unwrap();
        let install = super::super::room::Install {
            config,
            done: None,
            dispatch: None,
            owner: 0,
        };
        let room = DiscussionRoom::new(&kit, room_id.clone(), None, install);
        room.load().await.unwrap();
        room.editing(&kit, |state, _| {
            let pending = super::super::queue::Pending {
                event_id: "m1".to_owned(),
                depth: 0,
                asker: "ops".to_owned(),
                index: 1,
                candidates: vec![ChannelId::new("a").unwrap()],
                asked: Vec::new(),
                seq: state.reserve_seq(),
            };
            drop(state.wait_for_decision(pending));
        })
        .await
        .unwrap();
        let context = RoomContext::new(created);

        // The peek held nothing: done was not asked, so nothing is decided.
        room.state().done = None;
        let missed = room.planned_turn(context.clone()).await.unwrap();
        assert!(missed.turn.is_none());
        assert!(room.state().deciding.is_none());
        assert!(room.woken.load(Ordering::SeqCst));

        // The next look asked done, which holds: over, nothing decided.
        room.state().done = Some(true);
        room.planned_turn(context).await.unwrap();
        assert!(room.state().deciding.is_none());
        let stored = kit.store().get_room(&room_id).await.unwrap().unwrap();
        let queue = read_queue(&stored.metadata);
        assert!(queue.over && queue.dispatching.is_empty());
    }

    // Python: tests/orchestration/test_discussion_dispatch.py::test_a_message_the_peek_missed_asks_done_first[addressed]:
    // a turn queued between the peek and the plan is given only once done
    // was asked, which ends the discussion first.
    #[tokio::test]
    async fn a_turn_the_peek_missed_asks_done_first() {
        let room_id = RoomId::new("r1").unwrap();
        let kit = RoomKit::new(RoomKitOptions::default());
        let options = CreateRoomOptions {
            room_id: Some(room_id.clone()),
            ..Default::default()
        };
        let created = kit.create_room(options).await.unwrap();
        let config: DiscussionConfig = serde_json::from_value(serde_json::json!({
            "agents": ["a"], "max_depth": 5, "dispatch": true
        }))
        .unwrap();
        let install = super::super::room::Install {
            config,
            done: None,
            dispatch: None,
            owner: 0,
        };
        let room = DiscussionRoom::new(&kit, room_id.clone(), None, install);
        room.load().await.unwrap();
        room.editing(&kit, |state, _| {
            let ask = Ask {
                event_id: "m1".to_owned(),
                depth: 0,
                asker: "ops".to_owned(),
                index: 1,
                person: true,
                named: true,
            };
            state.ask(&ChannelId::new("a").unwrap(), ask, true, None);
        })
        .await
        .unwrap();
        let context = RoomContext::new(created);

        room.state().done = None;
        let missed = room.planned_turn(context.clone()).await.unwrap();
        assert!(missed.turn.is_none());
        assert!(room.woken.load(Ordering::SeqCst));

        room.state().done = Some(true);
        let ended = room.planned_turn(context).await.unwrap();
        assert!(ended.turn.is_none());
        let stored = kit.store().get_room(&room_id).await.unwrap().unwrap();
        let queue = read_queue(&stored.metadata);
        assert!(queue.over && queue.entries.is_empty());
    }
    // Python: tests/orchestration/test_discussion_processes.py::test_a_process_that_stopped_hands_the_room_over_once_its_lease_expires:
    // a process that stopped holding the lease, without giving it back,
    // keeps the turns from another process until the lease expires; then
    // the other takes it and gives the next turn (rule 16).
    #[tokio::test]
    async fn a_process_that_stopped_hands_the_room_over_once_its_lease_expires() {
        let room_id = RoomId::new("r1").unwrap();
        let ops = ChannelId::new("ops").unwrap();
        let store = Arc::new(crate::InMemoryStore::default());
        let start = Timestamp::from_unix_millis(1_800_000_000_000).unwrap();
        let clock = Arc::new(SequentialEnvironment::new(start));
        let kit_of = || {
            let options = RoomKitOptions {
                store: Arc::clone(&store),
                ..RoomKitOptions::default()
            };
            let clock: Arc<SequentialEnvironment> = Arc::clone(&clock);
            let environment: Arc<dyn roomkit_core::ops::Environment + Send + Sync> = clock;
            RoomKit::with_environment(options, environment)
        };
        let (a_kit, b_kit) = (kit_of(), kit_of());
        let options = CreateRoomOptions {
            room_id: Some(room_id.clone()),
            ..Default::default()
        };
        let created = a_kit.create_room(options).await.unwrap();
        let mut context = RoomContext::new(created);
        let mut asks = Vec::new();
        for (index, agent) in ["a", "b"].into_iter().enumerate() {
            let agent = ChannelId::new(agent).unwrap();
            let mut asked = RoomEvent::new(
                room_id.clone(),
                EventSource::new(ops.clone(), ChannelType::SMS),
                TextContent::new(format!("@{agent} hello")),
            );
            asked.index = u64::try_from(index).unwrap() + 1;
            let asked = a_kit
                .store()
                .commit_event(&room_id, asked, CommitEventOptions::default())
                .await
                .unwrap();
            context.bindings.push(ChannelBinding::at(
                agent.clone(),
                room_id.clone(),
                ChannelType::AI,
                ChannelCategory::Intelligence,
                ChannelDirection::Bidirectional,
                Timestamp::UNIX_EPOCH,
            ));
            asks.push((agent, asked));
        }
        context.bindings.push(ChannelBinding::at(
            ops.clone(),
            room_id.clone(),
            ChannelType::SMS,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        ));
        let install = || super::super::room::Install {
            config: serde_json::from_value(serde_json::json!({
                "agents": ["a", "b"], "max_depth": 5
            }))
            .unwrap(),
            done: None,
            dispatch: None,
            owner: 0,
        };
        let a = DiscussionRoom::new(&a_kit, room_id.clone(), None, install());
        a.load().await.unwrap();
        let b = DiscussionRoom::new(&b_kit, room_id.clone(), None, install());
        b.load().await.unwrap();
        a.editing(&a_kit, |state, _| {
            for (agent, asked) in &asks {
                let ask = Ask {
                    event_id: asked.id.to_string(),
                    depth: 0,
                    asker: "ops".to_owned(),
                    index: asked.index,
                    person: true,
                    named: true,
                };
                state.ask(agent, ask, true, None);
            }
        })
        .await
        .unwrap();

        // A gives a's turn, then stops without giving the lease back.
        a.state().done = Some(false);
        let given = a.planned_turn(context.clone()).await.unwrap();
        assert_eq!(
            given.turn.map(|turn| turn.agent.to_string()),
            Some("a".to_owned())
        );

        // B gives no turn while A's lease runs.
        b.state().done = Some(false);
        let refused = b.planned_turn(context.clone()).await.unwrap();
        assert!(refused.turn.is_none());

        // Once it expired, B takes it and gives the next turn.
        clock.advance(Duration::from_secs(16));
        b.state().done = Some(false);
        let handed = b.planned_turn(context).await.unwrap();
        assert_eq!(
            handed.turn.map(|turn| turn.agent.to_string()),
            Some("b".to_owned())
        );
    }
}
