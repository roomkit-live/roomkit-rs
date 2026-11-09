//! What a run of the driver holds, as the calls it makes read it: the
//! tenures of the room locks it holds, the leases code run under one of them
//! holds on its tenure, the calls it runs, and the queue that hands it the
//! calls into its room (RFC §13.5).
//!
//! A lease is live while the run that holds the lock keeps the tenure it
//! was given under, so a context kept past the release grants nothing.
//! Code may wait for a lease's end: the end of the section that holds the
//! lock under its tenure.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::RoomId;
use roomkit_core::ops::{EffectId, Outcome};
use tokio::sync::{Notify, mpsc, watch};

use super::adoption::Adoption;
use crate::delivery_keys::CancelRequest;
use crate::room_turns::Queued;
use crate::stream::KeptStream;
use crate::turns::{Canceller, OwedEnd};

/// What a run holds now, as the calls it makes read it.
#[derive(Debug, Default)]
struct Holdings {
    locks: HashMap<RoomId, Tenure>,
    /// The effects whose calls run now: the operation has not been answered
    /// them yet.
    calls: HashSet<EffectId>,
}

/// What a write of the store came to, once it ended; nothing while it runs.
pub(crate) type Written = watch::Receiver<Option<Outcome>>;

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
    /// Told each time a tenure of the run ends or begins: what waits for a
    /// lease's end reads it again then.
    tenure_changes: Notify,
    /// The streams the run's turns answered with, by their turn's effect,
    /// until they are read (RFC §6.1 `response_stream`).
    streams: Mutex<HashMap<EffectId, KeptStream>>,
    /// The signal each read of a stream is stopped with, by its effect.
    stops: Mutex<HashMap<EffectId, watch::Sender<bool>>>,
    /// The turns the run's calls submitted and await the output of, by
    /// their effect: a call given up cuts its turn at once.
    turns: Mutex<HashMap<EffectId, Canceller>>,
    /// Which delivery cuts the turn or stream read of each effect.
    cancellations: Mutex<HashMap<EffectId, Arc<CancelRequest>>>,
    /// The turns asked that wait their place in their channel's line in
    /// the room, by their turn's effect, until their stream is read (RFC
    /// §6.4): one never read gives its place up.
    queued: Mutex<HashMap<EffectId, Queued>>,
    /// Whether each turn whose stream the run read, or reads, owes its end:
    /// a cut call reads it (RFC §10.6.8). Never cleared, a cut call's end
    /// reading it once the run is over.
    read_turns: ReadTurns,
    /// Where each read of an adopted entry's cascade also tells whether its
    /// turn owes its end, by the read's effect, until the read takes it:
    /// the entry's own cleanup, which its cut reads (RFC §10.6.8).
    entry_reads: Mutex<HashMap<EffectId, ReadTurns>>,
}

/// Whether each turn whose stream a run read owes its end (RFC §10.6.8),
/// as Python's cascade reads `end_owed` on each of its `streams`: a turn
/// cut before its output, a strategy's first pass read under a cascade of
/// its own, is none of them.
#[derive(Clone, Debug, Default)]
pub(crate) struct ReadTurns(Arc<Mutex<Vec<OwedEnd>>>);

impl ReadTurns {
    /// The run reads the stream of a turn whose end `owed` tells.
    pub(crate) fn reads(&self, owed: OwedEnd) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(owed);
    }

    /// Whether one of them owes its end.
    pub(crate) fn any_owed(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(OwedEnd::is_owed)
    }
}

/// A claim on one tenure of a room's lock, which code run under that lock
/// holds.
#[derive(Clone, Debug)]
pub(crate) struct Lease {
    holder: Arc<RunShared>,
    room_id: RoomId,
    tenure: u64,
}

impl RunShared {
    /// A run's shared side, with the queue its driver receives calls on.
    pub(crate) fn new() -> (Arc<Self>, mpsc::UnboundedReceiver<Adoption>) {
        let (adoptions, received) = mpsc::unbounded_channel();
        let shared = Arc::new(Self {
            holdings: Mutex::new(Holdings::default()),
            adoptions,
            tenures: AtomicU64::new(0),
            tenure_changes: Notify::new(),
            streams: Mutex::new(HashMap::new()),
            stops: Mutex::new(HashMap::new()),
            turns: Mutex::new(HashMap::new()),
            cancellations: Mutex::new(HashMap::new()),
            queued: Mutex::new(HashMap::new()),
            read_turns: ReadTurns::default(),
            entry_reads: Mutex::new(HashMap::new()),
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
        self.tenure_changes.notify_waiters();
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
        self.tenure_changes.notify_waiters();
    }

    /// Records that the call for `effect` runs.
    pub(crate) fn calling(&self, effect: EffectId) {
        self.holdings().calls.insert(effect);
    }

    /// Records that the call for `effect` is over, its effect answered:
    /// its frame stands in no admission order any more.
    pub(crate) fn answered(&self, effect: EffectId) {
        self.holdings().calls.remove(&effect);
        self.cancellations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&effect);
    }

    /// Whether the call for `effect` still runs.
    pub(super) fn is_calling(&self, effect: EffectId) -> bool {
        self.holdings().calls.contains(&effect)
    }

    /// Hands `adoption` to the run's driver; hands it back when the run is
    /// over.
    pub(super) fn hand(&self, adoption: Adoption) -> Result<(), Box<Adoption>> {
        self.adoptions
            .send(adoption)
            .map_err(|unsent| Box::new(unsent.0))
    }

    /// Forgets everything the run held: it is over. A stream never read is
    /// dropped: nothing of it was generated.
    pub(crate) fn clear(&self) {
        *self.holdings() = Holdings::default();
        self.tenure_changes.notify_waiters();
        self.streams
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.stops
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.turns
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.queued
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.cancellations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }

    /// Keeps the turn of `effect` waiting its place, until its stream is
    /// read.
    pub(crate) fn keep_queued(&self, effect: EffectId, queued: Queued) {
        self.queued
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(effect, queued);
    }

    /// The turn of `effect` waiting its place, taken to be read.
    pub(crate) fn take_queued(&self, effect: EffectId) -> Option<Queued> {
        self.queued
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&effect)
    }

    /// The call of `effect` submitted a turn, which `turn` cuts.
    pub(crate) fn turn_submitted(
        &self,
        effect: EffectId,
        turn: Canceller,
    ) -> Option<crate::delivery_keys::Following> {
        let following = self
            .cancellation(effect)
            .map(|cancel| cancel.follows(&turn));
        self.turns
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(effect, turn);
        following
    }

    /// Names the delivery's cut before the request is spawned.
    pub(crate) fn cancels_with(&self, effect: EffectId, cancel: Arc<CancelRequest>) {
        self.cancellations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(effect, cancel);
    }

    /// The cut a turn submission or a stream read follows.
    pub(crate) fn cancellation(&self, effect: EffectId) -> Option<Arc<CancelRequest>> {
        self.cancellations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&effect)
            .cloned()
    }

    /// The call of `effect` read its turn's output.
    pub(crate) fn turn_answered(&self, effect: EffectId) {
        self.turns
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&effect);
    }

    /// Cuts the turn the call of `effect` awaits, if any (RFC §10.1
    /// "Explicit delivery cancellation"): what awaits its end.
    pub(crate) fn cut_turn(&self, effect: EffectId) -> Option<Canceller> {
        let turn = self
            .turns
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&effect)?;
        turn.cancel("cancelled");
        Some(turn)
    }

    /// Where each read of the run tells whether its turn owes its end.
    pub(crate) fn read_turns(&self) -> ReadTurns {
        self.read_turns.clone()
    }

    /// Makes the read of `effect`, a read of an adopted entry's cascade,
    /// tell `entry` too whether its turn owes its end.
    pub(crate) fn reads_for_entry(&self, effect: EffectId, entry: ReadTurns) {
        self.entry_reads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(effect, entry);
    }

    /// Where the read of `effect` tells whether its turn owes its end,
    /// beside the run: its entry's, for a read of an adopted entry's
    /// cascade, taken.
    pub(crate) fn entry_read_turns(&self, effect: EffectId) -> Option<ReadTurns> {
        self.entry_reads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&effect)
    }

    /// Whether a turn whose stream the run read is over for its owner
    /// without its executor's end (RFC §10.6.8).
    pub(crate) fn a_read_turn_s_end_is_owed(&self) -> bool {
        self.read_turns.any_owed()
    }

    /// Keeps the stream the turn of `effect` answered with, until it is read.
    pub(crate) fn keep_stream(&self, effect: EffectId, stream: KeptStream) {
        self.streams
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(effect, stream);
    }

    /// The signal the read of `effect` is stopped with: `true` once the
    /// operation stops it (RFC §12.2 step 13s).
    pub(crate) fn stop_signal(&self, effect: EffectId) -> watch::Receiver<bool> {
        self.stops
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(effect)
            .or_insert_with(|| watch::Sender::new(false))
            .subscribe()
    }

    /// Stops the read of `effect`, now or as it starts.
    pub(crate) fn stop_reading(&self, effect: EffectId) {
        self.stops
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(effect)
            .or_insert_with(|| watch::Sender::new(false))
            .send_replace(true);
    }

    /// The stream the turn of `effect` answered with, taken to be read.
    pub(crate) fn take_stream(&self, effect: EffectId) -> Option<KeptStream> {
        self.streams
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&effect)
    }

    /// The stream the turn of `effect` answered with, that no read took,
    /// taken to be closed: a strategy's first pass, read under a cascade of
    /// its own, is left (RFC §19.7.3).
    pub(crate) fn take_unread_stream(&self, effect: EffectId) -> Option<KeptStream> {
        let mut streams = self.streams.lock().unwrap_or_else(PoisonError::into_inner);
        if streams.get(&effect)?.then.is_some() {
            return None;
        }
        streams.remove(&effect)
    }

    /// The lease on the lock of `room_id` the run holds, by its own tenure
    /// or by the lease it was granted on.
    pub(super) fn lease_of(self: &Arc<Self>, room_id: &RoomId) -> Option<Lease> {
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

    /// Waits until the lease is no longer live: the run that holds the
    /// lock let its tenure go, releasing the lock or ending. A lease that
    /// is not live any more is over at once.
    pub(crate) async fn over(&self) {
        loop {
            let changed = self.holder.tenure_changes.notified();
            let mut changed = std::pin::pin!(changed);
            // Registered before the read: a change between the two wakes it.
            changed.as_mut().enable();
            if !self.is_live() {
                return;
            }
            changed.await;
        }
    }

    /// Whether the holder still holds the lock under the lease's tenure.
    pub(super) fn is_live(&self) -> bool {
        matches!(
            self.holder.holdings().locks.get(&self.room_id),
            Some(Tenure::Own(tenure)) if *tenure == self.tenure
        )
    }
}
