//! What `delegate` and the background helpers refuse, and in which words
//! (RFC §23.3, §23.4): a parent room the store fails to read, a hand-back
//! they cannot keep, an agent that is none of the room's, each as the
//! Python implementation's (`core/mixins/delegation.py`,
//! `core/mixins/background.py`, `tasks/background.py`); a delegation
//! the kit's close lands in before its task ran (RFC §12.10.4); and the
//! store write a task's child room's events go through (RFC §14.3).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::collections::{BTreeSet, VecDeque};
use std::future::{Future, pending, ready};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, Identity, IdentityId, Observation,
    Participant, ParticipantId, Room, RoomEvent, RoomId, Task,
};
use roomkit::model::{
    ChannelCategory, ChannelOutput, DeliveryOutcome, DeliveryOutcomeStatus, EventContent,
    EventType, RoomContext, TaskStatus, ToolCallContent,
};
use roomkit::orchestration::RecentOptions;
use roomkit::tasks::{DelegatedTask, DelegatedTaskResult, SubmitOptions, TaskRunner, WaitOptions};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions, idle,
};
use roomkit::{
    AttachChannelOptions, CancelTaskOptions, ConversationStore, CreateRoomOptions, DelegateOptions,
    DeliveryContext, DeliveryStrategy, HookExecution, HookOptions, HookTrigger, InMemoryStore,
    ResponseItem, RoomKit, RoomKitError, RoomKitOptions, StartTaskOptions, StoreError,
    StoreFailure, current_turn_host, store,
};
use roomkit_core::rules::transcoding::text_of;
use tokio::sync::Notify;

/// How long a wait that should end may take before the test fails it.
const BOUND: Duration = Duration::from_secs(5);

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room() -> RoomId {
    RoomId::new("r").unwrap()
}

/// Forwards each of the store's methods named to the in-memory store.
macro_rules! forward {
    ($($name:ident($($argument:ident: $kind:ty),*) -> $answer:ty;)*) => {
        $(
            async fn $name(&self, $($argument: $kind),*) -> Result<$answer, StoreError> {
                self.store.$name($($argument),*).await
            }
        )*
    };
}

/// An in-memory store whose reads of a room fail once `down` is set, as a
/// database gone does; once `held` is set, its read of the room `r`'s
/// participants, which a context of it reads, tells `entered`, then waits
/// for `go`; while `contexts_down` is set, that read fails, the room
/// itself still read.
#[derive(Debug, Default)]
struct RoomReadsFail {
    store: InMemoryStore,
    down: AtomicBool,
    held: AtomicBool,
    entered: Notify,
    go: Notify,
    contexts_down: AtomicBool,
}

impl ConversationStore for RoomReadsFail {
    async fn list_participants(&self, room_id: &RoomId) -> Result<Vec<Participant>, StoreError> {
        if *room_id == room() && self.held.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.go.notified().await;
        }
        if *room_id == room() && self.contexts_down.load(Ordering::SeqCst) {
            return Err(StoreError::Unavailable(StoreFailure::new(
                "the store is down",
            )));
        }
        self.store.list_participants(room_id).await
    }

    async fn get_room(&self, room_id: &RoomId) -> Result<Option<Room>, StoreError> {
        if self.down.load(Ordering::SeqCst) {
            return Err(StoreError::Unavailable(StoreFailure::new(
                "the database is gone",
            )));
        }
        self.store.get_room(room_id).await
    }

    forward! {
        create_room(room: Room) -> Room;
        update_room(room: Room) -> Room;
        delete_room(room_id: &RoomId) -> bool;
        list_rooms(options: store::ListRoomsOptions) -> Vec<Room>;
        find_rooms(options: store::FindRoomsOptions) -> Vec<Room>;
        advance_delivered_index(
            room_id: &RoomId,
            index: u64,
            options: store::AdvanceDeliveredIndexOptions
        ) -> bool;
        add_binding(binding: ChannelBinding) -> ChannelBinding;
        get_binding(room_id: &RoomId, channel_id: &ChannelId) -> Option<ChannelBinding>;
        update_binding(binding: ChannelBinding) -> ChannelBinding;
        remove_binding(room_id: &RoomId, channel_id: &ChannelId) -> bool;
        list_bindings(room_id: &RoomId) -> Vec<ChannelBinding>;
        add_participant(participant: Participant) -> Participant;
        get_participant(room_id: &RoomId, participant_id: &ParticipantId) -> Option<Participant>;
        update_participant(participant: Participant) -> Participant;
        delete_participant(room_id: &RoomId, participant_id: &ParticipantId) -> ();
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
        check_idempotency(room_id: &RoomId, key: &str, channel_id: Option<&ChannelId>) -> bool;
        commit_event(
            room_id: &RoomId,
            event: RoomEvent,
            options: store::CommitEventOptions
        ) -> RoomEvent;
        get_event(event_id: &EventId) -> Option<RoomEvent>;
        update_event(event: RoomEvent) -> RoomEvent;
        delete_event(
            room_id: &RoomId,
            event_id: &EventId,
            options: store::DeleteEventOptions
        ) -> Vec<RoomEvent>;
        get_event_count(room_id: &RoomId, options: store::GetEventCountOptions) -> u64;
        list_events(room_id: &RoomId, options: store::ListEventsOptions) -> Vec<RoomEvent>;
        add_task(task: Task) -> Task;
        add_observation(observation: Observation) -> Observation;
        list_tasks(room_id: &RoomId, options: store::ListTasksOptions) -> Vec<Task>;
        list_observations(room_id: &RoomId) -> Vec<Observation>;
        get_task(task_id: &roomkit::model::TaskId) -> Option<Task>;
        update_task(task: Task) -> Task;
        add_event(event: RoomEvent) -> RoomEvent;
        mark_read(room_id: &RoomId, channel_id: &ChannelId, event_id: &EventId) -> ();
        mark_all_read(room_id: &RoomId, channel_id: &ChannelId) -> ();
        get_unread_count(room_id: &RoomId, channel_id: &ChannelId) -> u64;
        list_read_markers(room_id: &RoomId) -> std::collections::BTreeMap<ChannelId, u64>;
        find_latest_room(
            participant_id: &ParticipantId,
            options: store::FindLatestRoomOptions
        ) -> Option<Room>;
        find_room_id_by_channel(
            channel_id: &ChannelId,
            options: store::FindRoomIdByChannelOptions
        ) -> Option<RoomId>;
        get_thread_summaries(
            room_id: &RoomId,
            root_event_ids: &[EventId]
        ) -> std::collections::BTreeMap<EventId, roomkit::model::ThreadSummary>;
    }
}

/// A kit holding the room `r` and the agent `worker`, its agent nowhere.
async fn kit_with<S: ConversationStore>(store: S) -> RoomKit<S> {
    let kit = RoomKit::new(RoomKitOptions::with_store(store));
    let worker = EchoAgent::new(channel("worker"), "Found it.", EchoAgentOptions::default());
    kit.register_channel(worker).unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    kit
}

// Python: core/mixins/delegation.py delegate, whose get_room lets the
// store's failure through; RoomNotFoundError only for a room missing or
// another organization's (no test of its own: RMK-1192).
#[tokio::test]
async fn a_parent_room_the_store_fails_to_read_fails_the_delegation_with_its_error() {
    let kit = kit_with(RoomReadsFail::default()).await;
    kit.store().down.store(true, Ordering::SeqCst);

    let refused = kit
        .delegate(
            &room(),
            &channel("worker"),
            "Find it.",
            DelegateOptions::default(),
        )
        .await;

    let refusal = refused.err().unwrap();
    assert_eq!(
        (
            matches!(refusal, RoomKitError::Store(_)),
            refusal.to_string().contains("the database is gone")
                || std::error::Error::source(&refusal)
                    .is_some_and(|cause| cause.to_string().contains("the database is gone"))
        ),
        (true, true),
        "{refusal:?}"
    );
    kit.close().await.unwrap();
}

// Python: tests/tasks/test_delegation_ends_failed.py::test_a_task_failing_before_it_is_announced_fires_no_completion[inline], [background]
// RFC §23.3: the parent room's context, read to announce the task, fails
// once the room itself was read: the delegation fails with the store's
// error before ON_TASK_DELEGATED fires, and nothing reports the end of a
// task never announced. Its span is not asserted: roomkit-rs has no
// telemetry.
#[tokio::test]
#[ignore = "RMK-1208: the announce swallows its context read's failure and the task runs"]
async fn a_task_failing_before_it_is_announced_fires_no_completion() {
    for wait in [true, false] {
        let kit = kit_with(RoomReadsFail::default()).await;
        let heard: Arc<Mutex<Vec<&'static str>>> = Arc::default();
        for (trigger, name) in [
            (HookTrigger::OnTaskDelegated, "delegated"),
            (HookTrigger::OnTaskCompleted, "completed"),
        ] {
            let noted = Arc::clone(&heard);
            let observer = HookOptions {
                execution: HookExecution::Async,
                ..HookOptions::default()
            };
            let record = move |_event: RoomEvent, _| {
                noted.lock().unwrap().push(name);
                ready(())
            };
            kit.hook(trigger, record, observer).unwrap();
        }
        let ended: Arc<Mutex<Vec<TaskStatus>>> = Arc::default();
        let noted = Arc::clone(&ended);
        let options = DelegateOptions {
            wait,
            on_complete: Some(Arc::new(move |result: DelegatedTaskResult| {
                noted.lock().unwrap().push(result.status);
                Box::pin(async {})
            })),
            ..DelegateOptions::default()
        };

        kit.store().contexts_down.store(true, Ordering::SeqCst);
        let refused = kit
            .delegate(&room(), &channel("worker"), "Count.", options)
            .await;
        kit.store().contexts_down.store(false, Ordering::SeqCst);
        idle(&kit).await;

        let refusal = refused.err().map(|refusal| {
            refusal.to_string().contains("the store is down")
                || std::error::Error::source(&refusal)
                    .is_some_and(|cause| cause.to_string().contains("the store is down"))
        });
        let recent = RecentOptions {
            agent_id: Some("worker".to_owned()),
            ..RecentOptions::default()
        };
        let lines = kit.status_bus().recent(recent).await.len();
        assert_eq!(
            (
                wait,
                refusal,
                heard.lock().unwrap().clone(),
                ended.lock().unwrap().clone(),
                lines
            ),
            (wait, Some(true), Vec::new(), Vec::new(), 0)
        );
        kit.close().await.unwrap();
    }
}

// Python: tests/tasks/test_delegation_brief_and_hand_back.py::test_an_unknown_hand_back_and_a_negative_conversation_are_refused
// (the conversation, a u64 here, is never negative); its words, the
// name quoted as Python's `!r` writes it.
#[tokio::test]
async fn an_unknown_hand_back_and_a_post_by_no_agent_of_the_room_are_refused() {
    let kit = kit_with(InMemoryStore::default()).await;
    let unknown = DelegateOptions {
        hand_back: "shout".to_owned(),
        ..DelegateOptions::default()
    };
    let posted = DelegateOptions {
        hand_back: "post".to_owned(),
        ..DelegateOptions::default()
    };

    // The words of a ValueError alone.
    let words = |outcome: Result<_, RoomKitError>| {
        if let Some(RoomKitError::Value(message)) = outcome.err() {
            Some(message)
        } else {
            None
        }
    };
    let refusals = (
        words(
            kit.delegate(&room(), &channel("worker"), "Count.", unknown)
                .await,
        ),
        words(
            kit.delegate(&room(), &channel("worker"), "Count.", posted)
                .await,
        ),
    );

    assert_eq!(
        refusals,
        (
            Some("hand_back must be one of ('relay', 'post'), not 'shout'".to_owned()),
            Some(
                "hand_back='post' posts as 'worker', which must be an intelligence channel \
                 attached to room 'r'"
                    .to_owned()
            )
        )
    );
    kit.close().await.unwrap();
}

// Python: tests/tasks/test_background_tasks.py::test_start_task_refuses_a_channel_that_is_no_agent_of_the_room
#[tokio::test]
async fn start_task_refuses_a_channel_that_is_no_agent_of_the_room() {
    let kit = kit_with(InMemoryStore::default()).await;

    let refused = kit
        .start_task(
            &room(),
            &channel("worker"),
            "Count.",
            StartTaskOptions::default(),
        )
        .await;

    assert_eq!(
        matches!(
            refused,
            Err(RoomKitError::Value(message)) if message == "'worker' is not an agent of room 'r'"
        ),
        true
    );
    kit.close().await.unwrap();
}

/// An agent counting the turns it is asked.
#[derive(Clone, Default)]
struct Counted {
    asked: Arc<Mutex<usize>>,
}

impl roomkit::Channel for Counted {
    fn channel_id(&self) -> ChannelId {
        channel("worker")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        _message: roomkit::model::InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, roomkit::ChannelError>> + Send {
        ready(Err("an agent is sent no message".into()))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, roomkit::ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }

    fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, roomkit::ChannelError>> + Send {
        *self.asked.lock().unwrap() += 1;
        ready(Ok(ChannelOutput::empty()))
    }
}

// Python: tests/test_kit_close_doors.py::test_a_task_announced_as_the_kit_closes_ends_cancelled[inline], [background]
// The kit's close lands between the task's announcement, whose observers'
// context is read from the store, and its run: the task ends cancelled
// there, the kit spawning nothing more, and its worker never runs.
#[tokio::test]
async fn a_task_announced_as_the_kit_closes_ends_cancelled() {
    for wait in [true, false] {
        let worker = Counted::default();
        let kit = RoomKit::new(RoomKitOptions::with_store(RoomReadsFail::default()));
        kit.register_channel(worker.clone()).unwrap();
        let create = CreateRoomOptions {
            room_id: Some(room()),
            ..CreateRoomOptions::default()
        };
        kit.create_room(create).await.unwrap();
        let observer = HookOptions {
            execution: HookExecution::Async,
            ..HookOptions::default()
        };
        kit.hook(
            HookTrigger::OnTaskDelegated,
            |_event: RoomEvent, _| ready(()),
            observer,
        )
        .unwrap();
        kit.store().held.store(true, Ordering::SeqCst);
        let options = DelegateOptions {
            wait,
            ..DelegateOptions::default()
        };

        let delegating = {
            let kit = kit.clone();
            tokio::spawn(async move {
                kit.delegate(&room(), &channel("worker"), "Go.", options)
                    .await
            })
        };
        tokio::time::timeout(BOUND, kit.store().entered.notified())
            .await
            .unwrap();
        tokio::time::timeout(BOUND, kit.close())
            .await
            .unwrap()
            .unwrap();
        kit.store().go.notify_one();
        let task = tokio::time::timeout(BOUND, delegating)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(
            (
                wait,
                task.result().map(|result| result.status),
                *worker.asked.lock().unwrap()
            ),
            (wait, Some(TaskStatus::Cancelled), 0)
        );
    }
}

/// A runner that never takes a task, keeping the one it is handed.
#[derive(Default)]
struct Keeps {
    handed: Arc<Mutex<Option<DelegatedTask>>>,
    submitted: Arc<Notify>,
}

impl TaskRunner<RoomReadsFail> for Keeps {
    fn submit(
        &self,
        _kit: RoomKit<RoomReadsFail>,
        task: DelegatedTask,
        _options: SubmitOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), RoomKitError>> + Send>> {
        *self.handed.lock().unwrap() = Some(task);
        self.submitted.notify_one();
        Box::pin(pending())
    }

    fn cancel(&self, _task_id: &str) -> Pin<Box<dyn Future<Output = bool> + Send>> {
        Box::pin(ready(false))
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(ready(()))
    }
}

// Python: core/mixins/delegation.py delegate, whose caller cut before the
// task ran ends it (_end_cut, shielded), its handle given its cancelled
// result however the kit's close left its store and hooks: a caller cut
// once the kit's close is over still ends the task its runner was handed
// (RMK-1192, the light review's edge a).
#[tokio::test]
async fn a_delegation_cut_once_the_kit_closed_still_ends_its_task() {
    let runner = Keeps::default();
    let (handed, submitted) = (Arc::clone(&runner.handed), Arc::clone(&runner.submitted));
    let kit = RoomKit::new(RoomKitOptions {
        task_runner: Some(Arc::new(runner)),
        ..RoomKitOptions::with_store(RoomReadsFail::default())
    });
    kit.register_channel(Counted::default()).unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();

    let delegating = {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.delegate(
                &room(),
                &channel("worker"),
                "Go.",
                DelegateOptions::default(),
            )
            .await
        })
    };
    tokio::time::timeout(BOUND, submitted.notified())
        .await
        .unwrap();
    tokio::time::timeout(BOUND, kit.close())
        .await
        .unwrap()
        .unwrap();
    delegating.abort();
    drop(delegating.await);

    let task = handed.lock().unwrap().clone().unwrap();
    assert_eq!(
        task.result().map(|result| (result.status, result.error)),
        Some((TaskStatus::Cancelled, Some("cancelled".to_owned())))
    );
}

/// An in-memory store recording which of its writes stored each event, by
/// the event's id; its commit of the text `held` names, once, tells
/// `entered`, then waits for `go`.
#[derive(Debug, Default)]
struct RecordsEventWrites {
    store: InMemoryStore,
    writes: Mutex<Vec<(EventId, &'static str)>>,
    held: Mutex<Option<&'static str>>,
    entered: Notify,
    go: Notify,
}

impl RecordsEventWrites {
    fn note(&self, event: &RoomEvent, write: &'static str) {
        self.writes.lock().unwrap().push((event.id.clone(), write));
    }

    /// The writes that stored `event`.
    fn writes_of(&self, event: &RoomEvent) -> Vec<&'static str> {
        let writes = self.writes.lock().unwrap();
        let of_it = writes.iter().filter(|(id, _)| *id == event.id);
        of_it.map(|(_, write)| *write).collect()
    }
}

impl ConversationStore for RecordsEventWrites {
    async fn commit_event(
        &self,
        room_id: &RoomId,
        event: RoomEvent,
        options: store::CommitEventOptions,
    ) -> Result<RoomEvent, StoreError> {
        self.note(&event, "commit_event");
        let holds = {
            let mut held = self.held.lock().unwrap();
            let text = text_of(&event.content);
            held.is_some_and(|held| held == text) && held.take().is_some()
        };
        if holds {
            self.entered.notify_one();
            self.go.notified().await;
        }
        self.store.commit_event(room_id, event, options).await
    }

    async fn add_event(&self, event: RoomEvent) -> Result<RoomEvent, StoreError> {
        self.note(&event, "add_event");
        self.store.add_event(event).await
    }

    async fn add_event_auto_index(
        &self,
        room_id: &RoomId,
        event: RoomEvent,
    ) -> Result<RoomEvent, StoreError> {
        self.note(&event, "add_event_auto_index");
        self.store.add_event_auto_index(room_id, event).await
    }

    forward! {
        create_room(room: Room) -> Room;
        get_room(room_id: &RoomId) -> Option<Room>;
        update_room(room: Room) -> Room;
        delete_room(room_id: &RoomId) -> bool;
        list_rooms(options: store::ListRoomsOptions) -> Vec<Room>;
        find_rooms(options: store::FindRoomsOptions) -> Vec<Room>;
        advance_delivered_index(
            room_id: &RoomId,
            index: u64,
            options: store::AdvanceDeliveredIndexOptions
        ) -> bool;
        add_binding(binding: ChannelBinding) -> ChannelBinding;
        get_binding(room_id: &RoomId, channel_id: &ChannelId) -> Option<ChannelBinding>;
        update_binding(binding: ChannelBinding) -> ChannelBinding;
        remove_binding(room_id: &RoomId, channel_id: &ChannelId) -> bool;
        list_bindings(room_id: &RoomId) -> Vec<ChannelBinding>;
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
        check_idempotency(room_id: &RoomId, key: &str, channel_id: Option<&ChannelId>) -> bool;
        get_event(event_id: &EventId) -> Option<RoomEvent>;
        update_event(event: RoomEvent) -> RoomEvent;
        delete_event(
            room_id: &RoomId,
            event_id: &EventId,
            options: store::DeleteEventOptions
        ) -> Vec<RoomEvent>;
        get_event_count(room_id: &RoomId, options: store::GetEventCountOptions) -> u64;
        list_events(room_id: &RoomId, options: store::ListEventsOptions) -> Vec<RoomEvent>;
        add_task(task: Task) -> Task;
        add_observation(observation: Observation) -> Observation;
        list_tasks(room_id: &RoomId, options: store::ListTasksOptions) -> Vec<Task>;
        list_observations(room_id: &RoomId) -> Vec<Observation>;
        get_task(task_id: &roomkit::model::TaskId) -> Option<Task>;
        update_task(task: Task) -> Task;
        mark_read(room_id: &RoomId, channel_id: &ChannelId, event_id: &EventId) -> ();
        mark_all_read(room_id: &RoomId, channel_id: &ChannelId) -> ();
        get_unread_count(room_id: &RoomId, channel_id: &ChannelId) -> u64;
        list_read_markers(room_id: &RoomId) -> std::collections::BTreeMap<ChannelId, u64>;
        find_latest_room(
            participant_id: &ParticipantId,
            options: store::FindLatestRoomOptions
        ) -> Option<Room>;
        find_room_id_by_channel(
            channel_id: &ChannelId,
            options: store::FindRoomIdByChannelOptions
        ) -> Option<RoomId>;
        get_thread_summaries(
            room_id: &RoomId,
            root_event_ids: &[EventId]
        ) -> std::collections::BTreeMap<EventId, roomkit::model::ThreadSummary>;
    }
}

// Python: tests/tasks/test_runner.py::TestInMemoryTaskRunner::test_events_committed_atomically
// RFC §10.1, §14.3: the task's description and its worker's answer are each
// stored by the one atomic commit, never by `add_event` nor
// `add_event_auto_index`, whose index and counters are not one write.
#[tokio::test]
async fn a_background_task_commits_each_event_of_its_child_room() {
    let kit = kit_with(RecordsEventWrites::default()).await;
    let task = kit
        .delegate(
            &room(),
            &channel("worker"),
            "Find it.",
            DelegateOptions::default(),
        )
        .await
        .unwrap();
    let waited = WaitOptions {
        timeout: Some(BOUND),
    };
    task.wait(waited).await.unwrap();

    let timeline = kit
        .store()
        .list_events(&task.child_room_id, store::ListEventsOptions::default())
        .await
        .unwrap();
    // The room's lifecycle records (RFC §5.1), which a store may hold, are
    // committed too: only the texts are named.
    let texts: Vec<&str> = timeline
        .iter()
        .filter_map(|event| {
            let EventContent::Text(content) = &event.content else {
                return None;
            };
            Some(content.body.as_str())
        })
        .collect();
    let stored_by: BTreeSet<Vec<&str>> = timeline
        .iter()
        .map(|event| kit.store().writes_of(event))
        .collect();
    let every_write: BTreeSet<&str> = kit
        .store()
        .writes
        .lock()
        .unwrap()
        .iter()
        .map(|(_, write)| *write)
        .collect();
    assert_eq!(
        (texts, stored_by, every_write),
        (
            vec!["Find it.", "Found it."],
            BTreeSet::from([vec!["commit_event"]]),
            BTreeSet::from(["commit_event"])
        )
    );
    kit.close().await.unwrap();
}

/// The agent `looker`, whose turn answers with a stream writing text, the
/// start of a call `search` and text again; `closed` says its stream was
/// dropped.
#[derive(Clone, Default)]
struct Looker {
    closed: Arc<AtomicBool>,
}

/// A `Looker`'s response.
struct Looking {
    items: VecDeque<ResponseItem>,
    closed: Arc<AtomicBool>,
}

impl Stream for Looking {
    type Item = Result<ResponseItem, roomkit::ChannelError>;

    fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().items.pop_front().map(Ok))
    }
}

impl Drop for Looking {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}

impl roomkit::Channel for Looker {
    fn channel_id(&self) -> ChannelId {
        channel("looker")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        _message: roomkit::model::InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, roomkit::ChannelError>> + Send {
        ready(Err("an agent is sent no message".into()))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, roomkit::ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, roomkit::ChannelError>> + Send {
        let output = if event.source.channel_id == self.channel_id() {
            ChannelOutput::empty()
        } else {
            let response = Looking {
                items: VecDeque::from([
                    ResponseItem::Text("Looking. ".to_owned()),
                    ResponseItem::ToolCallStart(ToolCallContent::new("search", "t1")),
                    ResponseItem::Text("never read".to_owned()),
                ]),
                closed: Arc::clone(&self.closed),
            };
            current_turn_host()
                .unwrap()
                .respond_with_stream(Box::pin(response))
        };
        ready(Ok(output))
    }
}

// Python: tests/tasks/test_child_room_trace.py::TestAChildTraceCutShort::test_a_delegation_cancelled_mid_write_closes_its_response
// RFC §12.2 step 13s: nothing else reads a delegated response, so a task
// cancelled while its trace stores a row closes it, the rows it began kept
// and its open call ended.
#[tokio::test]
#[ignore = "RMK-1208: a cancelled task's work is aborted mid-commit, its trace rows dropped"]
async fn a_task_cancelled_while_its_trace_stores_a_row_closes_its_response() {
    let store = RecordsEventWrites {
        held: Mutex::new(Some("Looking. ")),
        ..RecordsEventWrites::default()
    };
    let kit = kit_with(store).await;
    let looker = Looker::default();
    kit.register_channel(looker.clone()).unwrap();
    let task = kit
        .delegate(
            &room(),
            &channel("looker"),
            "Find it.",
            DelegateOptions::default(),
        )
        .await
        .unwrap();
    tokio::time::timeout(BOUND, kit.store().entered.notified())
        .await
        .unwrap();

    let cancelling = {
        let (kit, task_id) = (kit.clone(), task.id.clone());
        tokio::spawn(async move {
            kit.cancel_task(&task_id, CancelTaskOptions::default())
                .await
        })
    };
    tokio::task::yield_now().await;
    kit.store().go.notify_one();
    let cancelled = tokio::time::timeout(BOUND, cancelling)
        .await
        .unwrap()
        .unwrap();

    let rows: Vec<EventType> = kit
        .store()
        .list_events(&task.child_room_id, store::ListEventsOptions::default())
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.source.channel_id == channel("looker"))
        .map(|event| event.r#type)
        .collect();
    assert_eq!(
        (cancelled, looker.closed.load(Ordering::SeqCst), rows),
        (
            true,
            true,
            vec![
                EventType::Message,
                EventType::ToolCallStart,
                EventType::ToolCallEnd
            ]
        )
    );
    kit.close().await.unwrap();
}

/// A delivery strategy that records whether each delivery it is given is an
/// instruction, and sends it.
#[derive(Default)]
struct RecordsInstructions {
    given: Mutex<Vec<bool>>,
}

impl DeliveryStrategy for RecordsInstructions {
    fn deliver(
        &self,
        ctx: DeliveryContext,
    ) -> Pin<Box<dyn Future<Output = Result<Option<DeliveryOutcome>, RoomKitError>> + Send + '_>>
    {
        Box::pin(async move {
            self.given.lock().unwrap().push(ctx.instruction);
            Ok(Some(DeliveryOutcome::new(DeliveryOutcomeStatus::Sent)))
        })
    }
}

// Python: tests/tasks/test_delegation_result_delivery.py::test_the_kit_s_delivery_strategy_carries_the_result
// RFC §23.3 step 8: a background task's result is handed back through
// `deliver()`, as an instruction, by the kit's strategy.
#[tokio::test]
async fn the_kit_s_strategy_hands_a_task_s_result_back_as_an_instruction() {
    let strategy = Arc::new(RecordsInstructions::default());
    let shared: Arc<dyn DeliveryStrategy> = Arc::<RecordsInstructions>::clone(&strategy);
    let kit = RoomKit::new(RoomKitOptions {
        delivery_strategy: Some(shared),
        ..RoomKitOptions::default()
    });
    let text = RecordingTransport::new(
        channel("text"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(text).unwrap();
    for (id, reply) in [("assistant", "ok"), ("worker", "Findings.")] {
        let agent = EchoAgent::new(channel(id), reply, EchoAgentOptions::default());
        kit.register_channel(agent).unwrap();
    }
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    kit.attach_channel(&room(), &channel("text"), AttachChannelOptions::default())
        .await
        .unwrap();
    let as_agent = AttachChannelOptions {
        category: Some(ChannelCategory::Intelligence),
        ..AttachChannelOptions::default()
    };
    kit.attach_channel(&room(), &channel("assistant"), as_agent)
        .await
        .unwrap();
    let delegation = DelegateOptions {
        notify: Some(channel("assistant")),
        ..DelegateOptions::default()
    };
    let task = kit
        .delegate(&room(), &channel("worker"), "task", delegation)
        .await
        .unwrap();
    task.wait(WaitOptions {
        timeout: Some(BOUND),
    })
    .await
    .unwrap();
    idle(&kit).await;

    assert_eq!(*strategy.given.lock().unwrap(), vec![true]);
    kit.close().await.unwrap();
}
