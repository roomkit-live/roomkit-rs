//! What a run of the driver holds, as the calls it makes read it: the
//! tenures of the room locks it holds, the leases code run under one of them
//! holds on its tenure, the calls it runs, and the queue that hands it the
//! calls into its room (RFC §13.5).
//!
//! A lease is live while the run that holds the lock keeps the tenure it
//! was given under, so a context kept past the release grants nothing.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::RoomId;
use roomkit_core::ops::EffectId;
use tokio::sync::{mpsc, watch};

use super::adoption::Adoption;
use crate::stream::KeptStream;
use crate::turns::Canceller;

/// What a run holds now, as the calls it makes read it.
#[derive(Debug, Default)]
struct Holdings {
    locks: HashMap<RoomId, Tenure>,
    /// The effects whose calls run now: the operation has not been answered
    /// them yet.
    calls: HashSet<EffectId>,
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
    /// The streams the run's turns answered with, by their turn's effect,
    /// until they are read (RFC §6.1 `response_stream`).
    streams: Mutex<HashMap<EffectId, KeptStream>>,
    /// The signal each read of a stream is stopped with, by its effect.
    stops: Mutex<HashMap<EffectId, watch::Sender<bool>>>,
    /// The turns the run's calls submitted and await the output of, by
    /// their effect: a call given up cuts its turn at once.
    turns: Mutex<HashMap<EffectId, Canceller>>,
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
            streams: Mutex::new(HashMap::new()),
            stops: Mutex::new(HashMap::new()),
            turns: Mutex::new(HashMap::new()),
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

    /// Records that the call for `effect` runs.
    pub(crate) fn calling(&self, effect: EffectId) {
        self.holdings().calls.insert(effect);
    }

    /// Records that the call for `effect` is over, its effect answered:
    /// its frame stands in no admission order any more.
    pub(crate) fn answered(&self, effect: EffectId) {
        self.holdings().calls.remove(&effect);
    }

    /// Whether the call for `effect` still runs.
    pub(super) fn is_calling(&self, effect: EffectId) -> bool {
        self.holdings().calls.contains(&effect)
    }

    /// Hands `adoption` to the run's driver; hands it back when the run is
    /// over.
    pub(super) fn hand(&self, adoption: Adoption) -> Result<(), Adoption> {
        self.adoptions.send(adoption).map_err(|unsent| unsent.0)
    }

    /// Forgets everything the run held: it is over. A stream never read is
    /// dropped: nothing of it was generated.
    pub(crate) fn clear(&self) {
        *self.holdings() = Holdings::default();
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
    }

    /// The call of `effect` submitted a turn, which `turn` cuts.
    pub(crate) fn turn_submitted(&self, effect: EffectId, turn: Canceller) {
        self.turns
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(effect, turn);
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

    /// Whether the run keeps the stream of a turn it has not read.
    pub(crate) fn keeps_streams(&self) -> bool {
        !self
            .streams
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
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

    /// Whether the holder still holds the lock under the lease's tenure.
    pub(super) fn is_live(&self) -> bool {
        matches!(
            self.holder.holdings().locks.get(&self.room_id),
            Some(Tenure::Own(tenure)) if *tenure == self.tenure
        )
    }
}
