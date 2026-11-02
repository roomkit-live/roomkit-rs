//! What `delegate` and the background helpers refuse, and in which words
//! (RFC §23.3, §23.4): a parent room the store fails to read, a hand-back
//! they cannot keep, an agent that is none of the room's, each as the
//! Python implementation's (`core/mixins/delegation.py`,
//! `core/mixins/background.py`, `tasks/background.py`); and a delegation
//! the kit's close lands in before its task ran (RFC §12.10.4).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{pending, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, Identity, IdentityId, Observation,
    Participant, ParticipantId, Room, RoomEvent, RoomId, Task,
};
use roomkit::model::{ChannelOutput, RoomContext, TaskStatus};
use roomkit::tasks::{DelegatedTask, SubmitOptions, TaskRunner};
use roomkit::testing::{EchoAgent, EchoAgentOptions};
use roomkit::{
    ConversationStore, CreateRoomOptions, DelegateOptions, HookExecution, HookOptions, HookTrigger,
    InMemoryStore, RoomKit, RoomKitError, RoomKitOptions, StartTaskOptions, StoreError,
    StoreFailure, store,
};
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
/// for `go`.
#[derive(Debug, Default)]
struct RoomReadsFail {
    store: InMemoryStore,
    down: AtomicBool,
    held: AtomicBool,
    entered: Notify,
    go: Notify,
}

impl ConversationStore for RoomReadsFail {
    async fn list_participants(&self, room_id: &RoomId) -> Result<Vec<Participant>, StoreError> {
        if *room_id == room() && self.held.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.go.notified().await;
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

    fn category(&self) -> roomkit::model::ChannelCategory {
        roomkit::model::ChannelCategory::Intelligence
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
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), RoomKitError>> + Send>> {
        *self.handed.lock().unwrap() = Some(task);
        self.submitted.notify_one();
        Box::pin(pending())
    }

    fn cancel(&self, _task_id: &str) -> std::pin::Pin<Box<dyn Future<Output = bool> + Send>> {
        Box::pin(ready(false))
    }

    fn close(&self) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> {
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
