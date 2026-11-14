//! What a test holds in one process of a cluster: its reads of the room,
//! its room locks, its writes of the speak queue, as a stalled database or
//! lock holds them (Python's `_hold`), and the lease's clock (Python's
//! `_Clock`). Each process reaches the cluster's one store and one lock
//! manager through handles of its own, which change nothing until a test
//! holds them.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, Identity, IdentityId, Metadata, Observation,
    ObservationId, Participant, ParticipantId, Room, RoomEvent, RoomId, RoomStatus, Task, TaskId,
    ThreadSummary, Timestamp, Verification, VerificationRequest,
};
use roomkit::telemetry::TelemetryProvider;
use roomkit::testing::Environment;
use roomkit::{
    ConversationStore, InMemoryStore, RoomKitError, RoomLock, RoomLockManager, StoreError,
    StoreFailure, store,
};
use tokio::sync::{Notify, watch};

use crate::clock::TokioClock;

/// The room metadata key a discussion stores its speak queue under.
const SPEAK_QUEUE: &str = "_speak_queue";

/// Holds the calls that reach it from when a test holds it until it lets
/// them go, as a stalled store or lock does.
#[derive(Debug)]
pub(crate) struct Gate {
    held: watch::Sender<bool>,
    entered: Notify,
}

impl Default for Gate {
    fn default() -> Self {
        Self {
            held: watch::channel(false).0,
            entered: Notify::new(),
        }
    }
}

impl Gate {
    /// Holds every call that reaches the gate from now on.
    pub(crate) fn hold(&self) {
        self.held.send_replace(true);
    }

    /// Lets the calls held go, and those that come after.
    pub(crate) fn release(&self) {
        self.held.send_replace(false);
    }

    /// Until a call waits at the gate.
    pub(crate) async fn entered(&self) {
        self.entered.notified().await;
    }

    async fn pass(&self) {
        let mut held = self.held.subscribe();
        if *held.borrow_and_update() {
            self.entered.notify_one();
            drop(held.wait_for(|held| !*held).await);
        }
    }
}

/// What a test holds in one process.
#[derive(Debug, Default)]
pub(crate) struct Holds {
    /// The process's reads of a room made while it holds no room's lock:
    /// the stored queue its discussion reads without that lock (Python's
    /// `room.read`). A read under the lock, an edit's, passes.
    pub(crate) reads: Gate,
    /// The process's room locks: each change of the stored queue, a lease
    /// renewal among them, waits for one first (Python's `room.editing`).
    pub(crate) locks: Gate,
    /// While set, the process's writes of the speak queue fail, as a store
    /// that refuses them.
    failing_queue_writes: AtomicBool,
    /// How many room locks the process holds now.
    room_locks: AtomicUsize,
}

impl Holds {
    /// Fails the process's writes of the speak queue until
    /// [`release`](Self::release).
    pub(crate) fn fail_queue_writes(&self) {
        self.failing_queue_writes.store(true, Ordering::SeqCst);
    }

    /// Lets go of all it holds and fails.
    pub(crate) fn release(&self) {
        self.failing_queue_writes.store(false, Ordering::SeqCst);
        self.reads.release();
        self.locks.release();
    }
}

/// One process's handle on the cluster's store.
pub(crate) struct HeldStore {
    store: Arc<InMemoryStore>,
    pub(crate) holds: Arc<Holds>,
}

impl HeldStore {
    pub(crate) fn new(store: &Arc<InMemoryStore>, holds: &Arc<Holds>) -> Self {
        Self {
            store: Arc::clone(store),
            holds: Arc::clone(holds),
        }
    }
}

macro_rules! forward {
    ($($name:ident($($argument:ident: $kind:ty),*) -> $answer:ty;)*) => {
        $(
            async fn $name(&self, $($argument: $kind),*) -> Result<$answer, StoreError> {
                self.store.$name($($argument),*).await
            }
        )*
    };
}

impl ConversationStore for HeldStore {
    fn is_process_local(&self) -> bool {
        self.store.is_process_local()
    }

    fn set_telemetry(&self, provider: Arc<dyn TelemetryProvider>, record_sensitive_data: bool) {
        self.store.set_telemetry(provider, record_sensitive_data);
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        if self.holds.room_locks.load(Ordering::SeqCst) == 0 {
            self.holds.reads.pass().await;
        }
        self.store.get_room(room_id).await
    }

    async fn patch_room_metadata(
        &self,
        room_id: &RoomId,
        patch: Metadata,
        options: store::PatchRoomMetadataOptions,
    ) -> Result<Option<Room>, StoreError> {
        if self.holds.failing_queue_writes.load(Ordering::SeqCst) && patch.contains_key(SPEAK_QUEUE)
        {
            let refused = StoreFailure::new("the store refused the write");
            return Err(StoreError::Unavailable(refused));
        }
        self.store
            .patch_room_metadata(room_id, patch, options)
            .await
    }

    async fn close(&self) {
        self.store.close().await;
    }

    forward! {
        create_room(room: Room) -> Room;
        room_exists(room_id: &RoomId) -> bool;
        update_room(room: Room) -> Room;
        update_room_if(
            room: Room,
            status: RoomStatus,
            paused_at: Option<Timestamp>,
            last_activity_at: Option<Timestamp>
        ) -> Option<Room>;
        delete_room(room_id: &RoomId) -> bool;
        list_rooms(options: store::ListRoomsOptions) -> Vec<Room>;
        find_rooms(options: store::FindRoomsOptions) -> Vec<Room>;
        find_latest_room(
            participant_id: &ParticipantId,
            options: store::FindLatestRoomOptions
        ) -> Option<Room>;
        find_room_id_by_channel(
            channel_id: &ChannelId,
            options: store::FindRoomIdByChannelOptions
        ) -> Option<RoomId>;
        find_room_ids_by_channel(
            channel_id: &ChannelId,
            options: store::FindRoomIdsByChannelOptions
        ) -> Vec<RoomId>;
        find_room_id_by_binding(
            channel_id: &ChannelId,
            participant_id: &str,
            options: store::FindRoomIdByBindingOptions
        ) -> Option<RoomId>;
        find_room_id_by_participant(
            channel_id: &ChannelId,
            participant_id: &str,
            options: store::FindRoomIdByParticipantOptions
        ) -> Option<RoomId>;
        get_delivered_index(room_id: &RoomId) -> i64;
        advance_delivered_index(
            room_id: &RoomId,
            index: u64,
            options: store::AdvanceDeliveredIndexOptions
        ) -> bool;
        get_private_state(room_id: &RoomId, key: &str) -> Option<Metadata>;
        set_private_state(room_id: &RoomId, key: &str, value: Option<Metadata>) -> ();
        add_binding(binding: ChannelBinding) -> ChannelBinding;
        get_binding(room_id: &RoomId, channel_id: &ChannelId) -> Option<ChannelBinding>;
        binding_exists(room_id: &RoomId, channel_id: &ChannelId) -> bool;
        update_binding(binding: ChannelBinding) -> ChannelBinding;
        remove_binding(room_id: &RoomId, channel_id: &ChannelId) -> bool;
        list_bindings(room_id: &RoomId) -> Vec<ChannelBinding>;
        load_room_context(
            room_id: &RoomId
        ) -> (Option<Room>, Vec<ChannelBinding>, Vec<Participant>);
        get_conversation(
            room_id: &RoomId,
            options: store::GetConversationOptions
        ) -> Vec<RoomEvent>;
        get_timeline(room_id: &RoomId, options: store::GetTimelineOptions) -> Vec<RoomEvent>;
        add_participant(participant: Participant) -> Participant;
        get_participant(room_id: &RoomId, participant_id: &ParticipantId) -> Option<Participant>;
        update_participant(participant: Participant) -> Participant;
        delete_participant(room_id: &RoomId, participant_id: &ParticipantId) -> ();
        list_participants(room_id: &RoomId) -> Vec<Participant>;
        create_identity(identity: Identity) -> Identity;
        get_identity(identity_id: &IdentityId) -> Option<Identity>;
        resolve_identity(
            channel_type: &ChannelType,
            address: &str,
            options: store::ResolveIdentityOptions
        ) -> Option<Identity>;
        link_address(
            identity_id: &IdentityId,
            channel_type: &ChannelType,
            address: &str,
            options: store::LinkAddressOptions
        ) -> ();
        unlink_address(
            identity_id: &IdentityId,
            channel_type: &ChannelType,
            address: &str,
            options: store::UnlinkAddressOptions
        ) -> ();
        get_verification(
            identity_id: &IdentityId,
            options: store::GetVerificationOptions
        ) -> Option<Verification>;
        put_verification(verification: Verification) -> Verification;
        list_verifications_in_force() -> Vec<Verification>;
        create_verification_request(request: VerificationRequest) -> VerificationRequest;
        get_verification_request(request_id: &str) -> Option<VerificationRequest>;
        update_verification_request(request: VerificationRequest) -> VerificationRequest;
        check_idempotency(room_id: &RoomId, key: &str, channel_id: Option<&ChannelId>) -> bool;
        get_event_by_idempotency_key(
            room_id: &RoomId,
            key: &str,
            channel_id: Option<&ChannelId>
        ) -> Option<RoomEvent>;
        add_event(event: RoomEvent) -> RoomEvent;
        add_event_auto_index(room_id: &RoomId, event: RoomEvent) -> RoomEvent;
        commit_event(
            room_id: &RoomId,
            event: RoomEvent,
            options: store::CommitEventOptions
        ) -> RoomEvent;
        get_event(event_id: &EventId) -> Option<RoomEvent>;
        update_event(event: RoomEvent) -> RoomEvent;
        record_delivery_results(room_id: &RoomId, event_id: &EventId, results: Metadata) -> ();
        write_change_target(room_id: &RoomId, target: RoomEvent) -> ();
        delete_event(
            room_id: &RoomId,
            event_id: &EventId,
            options: store::DeleteEventOptions
        ) -> Vec<RoomEvent>;
        get_event_count(room_id: &RoomId, options: store::GetEventCountOptions) -> u64;
        list_events(room_id: &RoomId, options: store::ListEventsOptions) -> Vec<RoomEvent>;
        get_thread_summaries(
            room_id: &RoomId,
            root_event_ids: &[EventId]
        ) -> BTreeMap<EventId, ThreadSummary>;
        add_task(task: Task) -> Task;
        add_observation(observation: Observation) -> Observation;
        get_task(task_id: &TaskId) -> Option<Task>;
        update_task(task: Task) -> Task;
        list_tasks(room_id: &RoomId, options: store::ListTasksOptions) -> Vec<Task>;
        list_observations(room_id: &RoomId) -> Vec<Observation>;
        mark_read(room_id: &RoomId, channel_id: &ChannelId, event_id: &EventId) -> ();
        mark_all_read(room_id: &RoomId, channel_id: &ChannelId) -> ();
        get_unread_count(room_id: &RoomId, channel_id: &ChannelId) -> u64;
        list_read_markers(room_id: &RoomId) -> BTreeMap<ChannelId, u64>;
    }
}

type LockAnswer<'a> =
    Pin<Box<dyn Future<Output = Result<Box<dyn RoomLock>, RoomKitError>> + Send + 'a>>;

/// A room's lock a process holds, counted while it does.
struct CountedLock {
    lock: Option<Box<dyn RoomLock>>,
    holds: Arc<Holds>,
}

impl RoomLock for CountedLock {
    fn release(
        mut self: Box<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<(), RoomKitError>> + Send + 'static>> {
        let lock = self.lock.take().unwrap();
        Box::pin(async move {
            let released = lock.release().await;
            drop(self);
            released
        })
    }
}

impl Drop for CountedLock {
    fn drop(&mut self) {
        self.holds.room_locks.fetch_sub(1, Ordering::SeqCst);
    }
}

/// One process's handle on the cluster's lock manager.
pub(crate) struct HeldLocks {
    locks: Arc<dyn RoomLockManager>,
    holds: Arc<Holds>,
}

impl HeldLocks {
    pub(crate) fn new(locks: &Arc<dyn RoomLockManager>, holds: &Arc<Holds>) -> Self {
        Self {
            locks: Arc::clone(locks),
            holds: Arc::clone(holds),
        }
    }
}

impl RoomLockManager for HeldLocks {
    fn locked<'a>(&'a self, room_id: &'a RoomId) -> LockAnswer<'a> {
        Box::pin(async move {
            // A room's own lock; the kit leads the keys of its other kinds
            // of locks (delivery claims, verifications) with a NUL.
            if room_id.as_str().starts_with('\0') {
                return self.locks.locked(room_id).await;
            }
            self.holds.locks.pass().await;
            let lock = self.locks.locked(room_id).await?;
            self.holds.room_locks.fetch_add(1, Ordering::SeqCst);
            let counted: Box<dyn RoomLock> = Box::new(CountedLock {
                lock: Some(lock),
                holds: Arc::clone(&self.holds),
            });
            Ok(counted)
        })
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = Result<(), RoomKitError>> + Send + '_>> {
        self.locks.close()
    }
}

/// The kits' clock: tokio's, which a paused test advances, until a test
/// stops it at a time of its own (Python's `_Clock`, the lease's clock a
/// test stops).
#[derive(Clone)]
pub(crate) struct LeaseClock {
    tokio: TokioClock,
    stopped: Arc<Mutex<Option<Timestamp>>>,
}

impl LeaseClock {
    pub(crate) fn new() -> Self {
        Self {
            tokio: TokioClock::new(),
            stopped: Arc::default(),
        }
    }

    /// Stops the clock at `seconds` since the epoch, as a lease's expiry
    /// reads.
    pub(crate) fn stop_at(&self, seconds: f64) {
        let since = Duration::from_secs_f64(seconds);
        let at = Timestamp::UNIX_EPOCH.checked_add(since);
        *self.stopped.lock().unwrap() = at;
    }

    /// The time now, in seconds since the epoch, as a lease's expiry reads.
    pub(crate) fn seconds(&self) -> f64 {
        let since = self.now().duration_since(Timestamp::UNIX_EPOCH);
        since.unwrap().as_secs_f64()
    }
}

impl Environment for LeaseClock {
    fn now(&self) -> Timestamp {
        let stopped = *self.stopped.lock().unwrap();
        stopped.unwrap_or_else(|| self.tokio.now())
    }

    fn new_event_id(&self) -> EventId {
        self.tokio.new_event_id()
    }

    fn new_observation_id(&self) -> ObservationId {
        self.tokio.new_observation_id()
    }

    fn new_room_id(&self) -> RoomId {
        self.tokio.new_room_id()
    }
}
