//! The run's store, wrapped so that a step makes one of its operations fail
//! or hold (roomkit-specs `conformance/README.md`, "Setup":
//! `kit.store_faults`; "Steps": `store_fault`).
//!
//! Every operation of the store's interface (RFC §14.1) runs as the wrapped
//! store's own, unless a `store_fault` step armed a fault on it: the first
//! call of one of the fault's operations made after the step, on its
//! `room_id` when it names one, takes the fault. With `fail` that call fails
//! with [`StoreError::Unavailable`] in place of the operation; with `gate` it
//! holds at the gate once the operation answered, or in its place before it
//! fails. Every runner's kit is built over this store; without a fault armed
//! it is the wrapped store, operation for operation.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, Identity, IdentityId, Metadata, Observation,
    Participant, ParticipantId, Room, RoomEvent, RoomId, RoomStatus, Task, TaskId, ThreadSummary,
    Timestamp, Verification, VerificationRequest,
};
use roomkit::store::{
    AdvanceDeliveredIndexOptions, CommitEventOptions, DeleteEventOptions, FindLatestRoomOptions,
    FindRoomIdByBindingOptions, FindRoomIdByChannelOptions, FindRoomIdByParticipantOptions,
    FindRoomIdsByChannelOptions, FindRoomsOptions, GetConversationOptions, GetEventCountOptions,
    GetTimelineOptions, GetVerificationOptions, LinkAddressOptions, ListEventsOptions,
    ListRoomsOptions, ListTasksOptions, PatchRoomMetadataOptions, ResolveIdentityOptions,
    UnlinkAddressOptions,
};
use roomkit::telemetry::TelemetryProvider;
use roomkit::{ConversationStore, StoreError};
use serde_json::Value;

/// Holds a call at the run's gate of that name until a step opens it.
pub(crate) type Hold =
    Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// The operations a fault may name: those of the store's interface this
/// store wraps.
const OPERATIONS: &[&str] = &[
    "add_binding",
    "add_event",
    "add_event_auto_index",
    "add_observation",
    "add_participant",
    "add_task",
    "advance_delivered_index",
    "binding_exists",
    "check_idempotency",
    "commit_event",
    "create_identity",
    "create_room",
    "create_verification_request",
    "delete_event",
    "delete_participant",
    "delete_room",
    "find_latest_room",
    "find_room_id_by_binding",
    "find_room_id_by_channel",
    "find_room_id_by_participant",
    "find_room_ids_by_channel",
    "find_rooms",
    "get_binding",
    "get_conversation",
    "get_delivered_index",
    "get_event",
    "get_event_by_idempotency_key",
    "get_event_count",
    "get_identity",
    "get_participant",
    "get_private_state",
    "get_room",
    "get_task",
    "get_thread_summaries",
    "get_timeline",
    "get_unread_count",
    "get_verification",
    "get_verification_request",
    "link_address",
    "list_bindings",
    "list_events",
    "list_observations",
    "list_participants",
    "list_read_markers",
    "list_rooms",
    "list_tasks",
    "list_verifications_in_force",
    "load_room_context",
    "mark_all_read",
    "mark_read",
    "patch_room_metadata",
    "put_verification",
    "record_delivery_results",
    "remove_binding",
    "resolve_identity",
    "room_exists",
    "set_private_state",
    "unlink_address",
    "update_binding",
    "update_event",
    "update_participant",
    "update_room",
    "update_room_if",
    "update_task",
    "update_verification_request",
    "write_change_target",
];

/// What an operation a store fault fails gives its caller as its cause:
/// the store's own failure, its message the step's `fail`.
#[derive(Debug)]
pub(crate) struct StoreFault(String);

impl fmt::Display for StoreFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for StoreFault {}

/// A fault a step armed, which one call takes.
struct Fault {
    operations: Vec<String>,
    room_id: Option<String>,
    fail: Option<String>,
    gate: Option<String>,
    path: String,
    hold: Hold,
}

/// The faults the steps armed, each taken by one call.
#[derive(Default)]
pub(crate) struct StoreFaults {
    /// Whether the scenario's setup declares them (`kit.store_faults`).
    declared: AtomicBool,
    armed: Mutex<Vec<Fault>>,
}

impl StoreFaults {
    /// The setup declares the faults (`kit.store_faults`): its steps may
    /// arm them.
    pub(crate) fn declare(&self) {
        self.declared.store(true, Ordering::SeqCst);
    }

    /// Arms the fault `step` names for the next call it matches: on
    /// `room_id` alone when it names one, held at a gate through `hold`.
    pub(crate) fn arm(
        &self,
        step: &Value,
        path: &str,
        room_id: Option<String>,
        hold: Hold,
    ) -> Result<(), String> {
        if !self.declared.load(Ordering::SeqCst) {
            return Err(format!(
                "{path}: a store_fault step needs setup.kit.store_faults"
            ));
        }
        let named = &step["store_fault"];
        let operations: Vec<String> = match named {
            Value::String(operation) => vec![operation.clone()],
            Value::Array(listed) => listed
                .iter()
                .map(|operation| operation.as_str().unwrap_or_default().to_owned())
                .collect(),
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::Object(_) => Vec::new(),
        };
        if let Some(unknown) = operations
            .iter()
            .find(|operation| !OPERATIONS.contains(&operation.as_str()))
        {
            return Err(format!("{path}: no store operation {unknown:?}"));
        }
        self.armed().push(Fault {
            operations,
            room_id,
            fail: step["fail"].as_str().map(str::to_owned),
            gate: step["gate"].as_str().map(str::to_owned),
            path: path.to_owned(),
            hold,
        });
        Ok(())
    }

    /// What is left armed, each a problem of its step; disarms it.
    pub(crate) fn untaken(&self) -> Vec<String> {
        self.armed()
            .drain(..)
            .map(|fault| format!("{}: no store call took the fault", fault.path))
            .collect()
    }

    fn armed(&self) -> std::sync::MutexGuard<'_, Vec<Fault>> {
        self.armed.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The first armed fault a call of `operation` on `room` takes,
    /// disarmed.
    fn take(&self, operation: &str, room: Option<&RoomId>) -> Option<Fault> {
        let mut armed = self.armed();
        let position = armed.iter().position(|fault| {
            fault.operations.iter().any(|named| named == operation)
                && fault
                    .room_id
                    .as_deref()
                    .is_none_or(|wanted| room.is_some_and(|room| room.as_str() == wanted))
        })?;
        Some(armed.remove(position))
    }

    /// `work`, the wrapped store's operation, as the fault its call takes
    /// makes it: held at its gate, then answered or failed.
    async fn run<T>(
        &self,
        operation: &str,
        room: Option<&RoomId>,
        work: impl Future<Output = Result<T, StoreError>>,
    ) -> Result<T, StoreError> {
        let Some(fault) = self.take(operation, room) else {
            return work.await;
        };
        if let Some(message) = fault.fail {
            if let Some(gate) = fault.gate {
                (fault.hold)(gate).await;
            }
            return Err(StoreError::unavailable(StoreFault(message)));
        }
        let answered = work.await;
        if let Some(gate) = fault.gate {
            (fault.hold)(gate).await;
        }
        answered
    }
}

/// `inner`, each operation of its interface run as its own, unless a fault
/// of its [`StoreFaults`] takes the call.
pub(crate) struct FaultingStore<S> {
    inner: S,
    faults: StoreFaults,
}

impl<S> FaultingStore<S> {
    /// `inner`, with no fault armed.
    pub(crate) fn new(inner: S) -> Self {
        Self {
            inner,
            faults: StoreFaults::default(),
        }
    }

    /// The faults the steps arm on it.
    pub(crate) fn faults(&self) -> &StoreFaults {
        &self.faults
    }
}

impl<S> fmt::Debug for FaultingStore<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FaultingStore")
            .finish_non_exhaustive()
    }
}

impl<S: ConversationStore> ConversationStore for FaultingStore<S> {
    fn connection<'a, W, T, E>(&'a self, work: W) -> impl Future<Output = Result<T, E>> + Send + 'a
    where
        W: Future<Output = Result<T, E>> + Send + 'a,
        T: Send + 'a,
        E: From<StoreError> + Send + 'a,
    {
        self.inner.connection(work)
    }

    fn is_process_local(&self) -> bool {
        self.inner.is_process_local()
    }

    fn set_telemetry(&self, provider: Arc<dyn TelemetryProvider>, record_sensitive_data: bool) {
        self.inner.set_telemetry(provider, record_sensitive_data);
    }

    async fn close(&self) {
        self.inner.close().await;
    }

    async fn create_room(&self, room: Room) -> Result<Room, StoreError> {
        self.faults
            .run("create_room", None, Box::pin(self.inner.create_room(room)))
            .await
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        self.faults
            .run(
                "get_room",
                Some(room_id),
                Box::pin(self.inner.get_room(room_id)),
            )
            .await
    }

    async fn room_exists(&self, room_id: &RoomId) -> Result<bool, StoreError> {
        self.faults
            .run(
                "room_exists",
                Some(room_id),
                Box::pin(self.inner.room_exists(room_id)),
            )
            .await
    }

    async fn update_room(&self, room: Room) -> Result<Room, StoreError> {
        self.faults
            .run("update_room", None, Box::pin(self.inner.update_room(room)))
            .await
    }

    async fn update_room_if(
        &self,
        room: Room,
        status: RoomStatus,
        paused_at: Option<Timestamp>,
        last_activity_at: Option<Timestamp>,
    ) -> Result<Option<Room>, StoreError> {
        self.faults
            .run(
                "update_room_if",
                None,
                Box::pin(
                    self.inner
                        .update_room_if(room, status, paused_at, last_activity_at),
                ),
            )
            .await
    }

    async fn patch_room_metadata(
        &self,
        room_id: &RoomId,
        patch: Metadata,
        options: PatchRoomMetadataOptions,
    ) -> Result<Option<Room>, StoreError> {
        self.faults
            .run(
                "patch_room_metadata",
                Some(room_id),
                Box::pin(self.inner.patch_room_metadata(room_id, patch, options)),
            )
            .await
    }

    async fn delete_room(&self, room_id: &RoomId) -> Result<bool, StoreError> {
        self.faults
            .run(
                "delete_room",
                Some(room_id),
                Box::pin(self.inner.delete_room(room_id)),
            )
            .await
    }

    async fn list_rooms(&self, options: ListRoomsOptions) -> Result<Vec<Room>, StoreError> {
        self.faults
            .run("list_rooms", None, Box::pin(self.inner.list_rooms(options)))
            .await
    }

    async fn find_rooms(&self, options: FindRoomsOptions) -> Result<Vec<Room>, StoreError> {
        self.faults
            .run("find_rooms", None, Box::pin(self.inner.find_rooms(options)))
            .await
    }

    async fn find_latest_room(
        &self,
        participant_id: &ParticipantId,
        options: FindLatestRoomOptions,
    ) -> Result<Option<Room>, StoreError> {
        self.faults
            .run(
                "find_latest_room",
                None,
                Box::pin(self.inner.find_latest_room(participant_id, options)),
            )
            .await
    }

    async fn find_room_id_by_channel(
        &self,
        channel_id: &ChannelId,
        options: FindRoomIdByChannelOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        self.faults
            .run(
                "find_room_id_by_channel",
                None,
                Box::pin(self.inner.find_room_id_by_channel(channel_id, options)),
            )
            .await
    }

    async fn find_room_ids_by_channel(
        &self,
        channel_id: &ChannelId,
        options: FindRoomIdsByChannelOptions,
    ) -> Result<Vec<RoomId>, StoreError> {
        self.faults
            .run(
                "find_room_ids_by_channel",
                None,
                Box::pin(self.inner.find_room_ids_by_channel(channel_id, options)),
            )
            .await
    }

    async fn find_room_id_by_binding(
        &self,
        channel_id: &ChannelId,
        participant_id: &str,
        options: FindRoomIdByBindingOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        self.faults
            .run(
                "find_room_id_by_binding",
                None,
                Box::pin(
                    self.inner
                        .find_room_id_by_binding(channel_id, participant_id, options),
                ),
            )
            .await
    }

    async fn find_room_id_by_participant(
        &self,
        channel_id: &ChannelId,
        participant_id: &str,
        options: FindRoomIdByParticipantOptions,
    ) -> Result<Option<RoomId>, StoreError> {
        self.faults
            .run(
                "find_room_id_by_participant",
                None,
                Box::pin(self.inner.find_room_id_by_participant(
                    channel_id,
                    participant_id,
                    options,
                )),
            )
            .await
    }

    async fn get_delivered_index(&self, room_id: &RoomId) -> Result<i64, StoreError> {
        self.faults
            .run(
                "get_delivered_index",
                Some(room_id),
                Box::pin(self.inner.get_delivered_index(room_id)),
            )
            .await
    }

    async fn advance_delivered_index(
        &self,
        room_id: &RoomId,
        index: u64,
        options: AdvanceDeliveredIndexOptions,
    ) -> Result<bool, StoreError> {
        self.faults
            .run(
                "advance_delivered_index",
                Some(room_id),
                Box::pin(self.inner.advance_delivered_index(room_id, index, options)),
            )
            .await
    }

    async fn get_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
    ) -> Result<Option<Metadata>, StoreError> {
        self.faults
            .run(
                "get_private_state",
                Some(room_id),
                Box::pin(self.inner.get_private_state(room_id, key)),
            )
            .await
    }

    async fn set_private_state(
        &self,
        room_id: &RoomId,
        key: &str,
        value: Option<Metadata>,
    ) -> Result<(), StoreError> {
        self.faults
            .run(
                "set_private_state",
                Some(room_id),
                Box::pin(self.inner.set_private_state(room_id, key, value)),
            )
            .await
    }

    async fn add_binding(&self, binding: ChannelBinding) -> Result<ChannelBinding, StoreError> {
        self.faults
            .run(
                "add_binding",
                None,
                Box::pin(self.inner.add_binding(binding)),
            )
            .await
    }

    async fn get_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<Option<ChannelBinding>, StoreError> {
        self.faults
            .run(
                "get_binding",
                Some(room_id),
                Box::pin(self.inner.get_binding(room_id, channel_id)),
            )
            .await
    }

    async fn binding_exists(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<bool, StoreError> {
        self.faults
            .run(
                "binding_exists",
                Some(room_id),
                Box::pin(self.inner.binding_exists(room_id, channel_id)),
            )
            .await
    }

    async fn update_binding(&self, binding: ChannelBinding) -> Result<ChannelBinding, StoreError> {
        self.faults
            .run(
                "update_binding",
                None,
                Box::pin(self.inner.update_binding(binding)),
            )
            .await
    }

    async fn remove_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<bool, StoreError> {
        self.faults
            .run(
                "remove_binding",
                Some(room_id),
                Box::pin(self.inner.remove_binding(room_id, channel_id)),
            )
            .await
    }

    async fn list_bindings(&self, room_id: &RoomId) -> Result<Vec<ChannelBinding>, StoreError> {
        self.faults
            .run(
                "list_bindings",
                Some(room_id),
                Box::pin(self.inner.list_bindings(room_id)),
            )
            .await
    }

    async fn load_room_context(
        &self,
        room_id: &RoomId,
    ) -> Result<(Option<Room>, Vec<ChannelBinding>, Vec<Participant>), StoreError> {
        self.faults
            .run(
                "load_room_context",
                Some(room_id),
                Box::pin(self.inner.load_room_context(room_id)),
            )
            .await
    }

    async fn get_conversation(
        &self,
        room_id: &RoomId,
        options: GetConversationOptions,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.faults
            .run(
                "get_conversation",
                Some(room_id),
                Box::pin(self.inner.get_conversation(room_id, options)),
            )
            .await
    }

    async fn get_timeline(
        &self,
        room_id: &RoomId,
        options: GetTimelineOptions,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.faults
            .run(
                "get_timeline",
                Some(room_id),
                Box::pin(self.inner.get_timeline(room_id, options)),
            )
            .await
    }

    async fn add_participant(&self, participant: Participant) -> Result<Participant, StoreError> {
        self.faults
            .run(
                "add_participant",
                None,
                Box::pin(self.inner.add_participant(participant)),
            )
            .await
    }

    async fn get_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<Option<Participant>, StoreError> {
        self.faults
            .run(
                "get_participant",
                Some(room_id),
                Box::pin(self.inner.get_participant(room_id, participant_id)),
            )
            .await
    }

    async fn update_participant(
        &self,
        participant: Participant,
    ) -> Result<Participant, StoreError> {
        self.faults
            .run(
                "update_participant",
                None,
                Box::pin(self.inner.update_participant(participant)),
            )
            .await
    }

    async fn delete_participant(
        &self,
        room_id: &RoomId,
        participant_id: &ParticipantId,
    ) -> Result<(), StoreError> {
        self.faults
            .run(
                "delete_participant",
                Some(room_id),
                Box::pin(self.inner.delete_participant(room_id, participant_id)),
            )
            .await
    }

    async fn list_participants(&self, room_id: &RoomId) -> Result<Vec<Participant>, StoreError> {
        self.faults
            .run(
                "list_participants",
                Some(room_id),
                Box::pin(self.inner.list_participants(room_id)),
            )
            .await
    }

    async fn create_identity(&self, identity: Identity) -> Result<Identity, StoreError> {
        self.faults
            .run(
                "create_identity",
                None,
                Box::pin(self.inner.create_identity(identity)),
            )
            .await
    }

    async fn get_identity(&self, identity_id: &IdentityId) -> Result<Option<Identity>, StoreError> {
        self.faults
            .run(
                "get_identity",
                None,
                Box::pin(self.inner.get_identity(identity_id)),
            )
            .await
    }

    async fn resolve_identity(
        &self,
        channel_type: &ChannelType,
        address: &str,
        options: ResolveIdentityOptions,
    ) -> Result<Option<Identity>, StoreError> {
        self.faults
            .run(
                "resolve_identity",
                None,
                Box::pin(self.inner.resolve_identity(channel_type, address, options)),
            )
            .await
    }

    async fn link_address(
        &self,
        identity_id: &IdentityId,
        channel_type: &ChannelType,
        address: &str,
        options: LinkAddressOptions,
    ) -> Result<(), StoreError> {
        self.faults
            .run(
                "link_address",
                None,
                Box::pin(
                    self.inner
                        .link_address(identity_id, channel_type, address, options),
                ),
            )
            .await
    }

    async fn unlink_address(
        &self,
        identity_id: &IdentityId,
        channel_type: &ChannelType,
        address: &str,
        options: UnlinkAddressOptions,
    ) -> Result<(), StoreError> {
        self.faults
            .run(
                "unlink_address",
                None,
                Box::pin(
                    self.inner
                        .unlink_address(identity_id, channel_type, address, options),
                ),
            )
            .await
    }

    async fn get_verification(
        &self,
        identity_id: &IdentityId,
        options: GetVerificationOptions,
    ) -> Result<Option<Verification>, StoreError> {
        self.faults
            .run(
                "get_verification",
                None,
                Box::pin(self.inner.get_verification(identity_id, options)),
            )
            .await
    }

    async fn put_verification(
        &self,
        verification: Verification,
    ) -> Result<Verification, StoreError> {
        self.faults
            .run(
                "put_verification",
                None,
                Box::pin(self.inner.put_verification(verification)),
            )
            .await
    }

    async fn list_verifications_in_force(&self) -> Result<Vec<Verification>, StoreError> {
        self.faults
            .run(
                "list_verifications_in_force",
                None,
                Box::pin(self.inner.list_verifications_in_force()),
            )
            .await
    }

    async fn create_verification_request(
        &self,
        request: VerificationRequest,
    ) -> Result<VerificationRequest, StoreError> {
        self.faults
            .run(
                "create_verification_request",
                None,
                Box::pin(self.inner.create_verification_request(request)),
            )
            .await
    }

    async fn get_verification_request(
        &self,
        request_id: &str,
    ) -> Result<Option<VerificationRequest>, StoreError> {
        self.faults
            .run(
                "get_verification_request",
                None,
                Box::pin(self.inner.get_verification_request(request_id)),
            )
            .await
    }

    async fn update_verification_request(
        &self,
        request: VerificationRequest,
    ) -> Result<VerificationRequest, StoreError> {
        self.faults
            .run(
                "update_verification_request",
                None,
                Box::pin(self.inner.update_verification_request(request)),
            )
            .await
    }

    async fn check_idempotency(
        &self,
        room_id: &RoomId,
        key: &str,
        channel_id: Option<&ChannelId>,
    ) -> Result<bool, StoreError> {
        self.faults
            .run(
                "check_idempotency",
                Some(room_id),
                Box::pin(self.inner.check_idempotency(room_id, key, channel_id)),
            )
            .await
    }

    async fn get_event_by_idempotency_key(
        &self,
        room_id: &RoomId,
        key: &str,
        channel_id: Option<&ChannelId>,
    ) -> Result<Option<RoomEvent>, StoreError> {
        self.faults
            .run(
                "get_event_by_idempotency_key",
                Some(room_id),
                Box::pin(
                    self.inner
                        .get_event_by_idempotency_key(room_id, key, channel_id),
                ),
            )
            .await
    }

    async fn add_event(&self, event: RoomEvent) -> Result<RoomEvent, StoreError> {
        self.faults
            .run("add_event", None, Box::pin(self.inner.add_event(event)))
            .await
    }

    async fn add_event_auto_index(
        &self,
        room_id: &RoomId,
        event: RoomEvent,
    ) -> Result<RoomEvent, StoreError> {
        self.faults
            .run(
                "add_event_auto_index",
                Some(room_id),
                Box::pin(self.inner.add_event_auto_index(room_id, event)),
            )
            .await
    }

    async fn commit_event(
        &self,
        room_id: &RoomId,
        event: RoomEvent,
        options: CommitEventOptions,
    ) -> Result<RoomEvent, StoreError> {
        self.faults
            .run(
                "commit_event",
                Some(room_id),
                Box::pin(self.inner.commit_event(room_id, event, options)),
            )
            .await
    }

    async fn get_event(&self, event_id: &EventId) -> Result<Option<RoomEvent>, StoreError> {
        self.faults
            .run("get_event", None, Box::pin(self.inner.get_event(event_id)))
            .await
    }

    async fn update_event(&self, event: RoomEvent) -> Result<RoomEvent, StoreError> {
        self.faults
            .run(
                "update_event",
                None,
                Box::pin(self.inner.update_event(event)),
            )
            .await
    }

    async fn record_delivery_results(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        results: Metadata,
    ) -> Result<(), StoreError> {
        self.faults
            .run(
                "record_delivery_results",
                Some(room_id),
                Box::pin(
                    self.inner
                        .record_delivery_results(room_id, event_id, results),
                ),
            )
            .await
    }

    async fn write_change_target(
        &self,
        room_id: &RoomId,
        target: RoomEvent,
    ) -> Result<(), StoreError> {
        self.faults
            .run(
                "write_change_target",
                Some(room_id),
                Box::pin(self.inner.write_change_target(room_id, target)),
            )
            .await
    }

    async fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        options: DeleteEventOptions,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.faults
            .run(
                "delete_event",
                Some(room_id),
                Box::pin(self.inner.delete_event(room_id, event_id, options)),
            )
            .await
    }

    async fn get_event_count(
        &self,
        room_id: &RoomId,
        options: GetEventCountOptions,
    ) -> Result<u64, StoreError> {
        self.faults
            .run(
                "get_event_count",
                Some(room_id),
                Box::pin(self.inner.get_event_count(room_id, options)),
            )
            .await
    }

    async fn list_events(
        &self,
        room_id: &RoomId,
        options: ListEventsOptions,
    ) -> Result<Vec<RoomEvent>, StoreError> {
        self.faults
            .run(
                "list_events",
                Some(room_id),
                Box::pin(self.inner.list_events(room_id, options)),
            )
            .await
    }

    async fn get_thread_summaries(
        &self,
        room_id: &RoomId,
        root_event_ids: &[EventId],
    ) -> Result<BTreeMap<EventId, ThreadSummary>, StoreError> {
        self.faults
            .run(
                "get_thread_summaries",
                Some(room_id),
                Box::pin(self.inner.get_thread_summaries(room_id, root_event_ids)),
            )
            .await
    }

    async fn add_task(&self, task: Task) -> Result<Task, StoreError> {
        self.faults
            .run("add_task", None, Box::pin(self.inner.add_task(task)))
            .await
    }

    async fn add_observation(&self, observation: Observation) -> Result<Observation, StoreError> {
        self.faults
            .run(
                "add_observation",
                None,
                Box::pin(self.inner.add_observation(observation)),
            )
            .await
    }

    async fn get_task(&self, task_id: &TaskId) -> Result<Option<Task>, StoreError> {
        self.faults
            .run("get_task", None, Box::pin(self.inner.get_task(task_id)))
            .await
    }

    async fn update_task(&self, task: Task) -> Result<Task, StoreError> {
        self.faults
            .run("update_task", None, Box::pin(self.inner.update_task(task)))
            .await
    }

    async fn list_tasks(
        &self,
        room_id: &RoomId,
        options: ListTasksOptions,
    ) -> Result<Vec<Task>, StoreError> {
        self.faults
            .run(
                "list_tasks",
                Some(room_id),
                Box::pin(self.inner.list_tasks(room_id, options)),
            )
            .await
    }

    async fn list_observations(&self, room_id: &RoomId) -> Result<Vec<Observation>, StoreError> {
        self.faults
            .run(
                "list_observations",
                Some(room_id),
                Box::pin(self.inner.list_observations(room_id)),
            )
            .await
    }

    async fn mark_read(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        event_id: &EventId,
    ) -> Result<(), StoreError> {
        self.faults
            .run(
                "mark_read",
                Some(room_id),
                Box::pin(self.inner.mark_read(room_id, channel_id, event_id)),
            )
            .await
    }

    async fn mark_all_read(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<(), StoreError> {
        self.faults
            .run(
                "mark_all_read",
                Some(room_id),
                Box::pin(self.inner.mark_all_read(room_id, channel_id)),
            )
            .await
    }

    async fn get_unread_count(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<u64, StoreError> {
        self.faults
            .run(
                "get_unread_count",
                Some(room_id),
                Box::pin(self.inner.get_unread_count(room_id, channel_id)),
            )
            .await
    }

    async fn list_read_markers(
        &self,
        room_id: &RoomId,
    ) -> Result<BTreeMap<ChannelId, u64>, StoreError> {
        self.faults
            .run(
                "list_read_markers",
                Some(room_id),
                Box::pin(self.inner.list_read_markers(room_id)),
            )
            .await
    }
}
