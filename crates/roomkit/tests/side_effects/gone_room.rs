//! The side effects of a delivery set whose room is deleted while it runs,
//! created again under its id or not (RFC §13.5, §10.1 step 15): none is
//! written, and a line says why; a read of the room that fails writes them
//! (Python's `tests/test_hook_side_effects.py::TestGoneRoomSideEffects`).

use super::*;
use pretty_assertions::assert_eq;
use std::time::Duration;
use tracing::Level;

/// The line a set whose room is gone logs, the room's `r1`.
const GONE_LINE: &str = "Room r1 is gone, deleted or created again: its delivery set's 1 \
                         task(s), 1 observation(s) and metadata updates ['stale'] are not written";

/// A transport whose delivery of `held` waits until the test opens it,
/// telling it once it started, and returns the room metadata update
/// `stale`, Python's `_GatedUpdater`.
struct Stale {
    reached: Arc<tokio::sync::Notify>,
    opened: Arc<tokio::sync::Notify>,
}

impl Channel for Stale {
    fn channel_id(&self) -> ChannelId {
        channel("ws1")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let held = text_of(&event) == Some("held");
        let (reached, opened) = (Arc::clone(&self.reached), Arc::clone(&self.opened));
        async move {
            if !held {
                return Ok(ChannelOutput::default());
            }
            reached.notify_one();
            opened.notified().await;
            let mut output = ChannelOutput::default();
            output
                .metadata_updates
                .insert("stale".to_owned(), true.into());
            Ok(output)
        }
    }
}

/// What lets the test hold the set's delivery, and the ids of the tasks
/// whose `ON_TASK_CREATED` ran.
struct HeldRoom {
    reached: Arc<tokio::sync::Notify>,
    opened: Arc<tokio::sync::Notify>,
    created: Arc<Mutex<Vec<String>>>,
}

/// Room `r1` on `kit` binding `sms1` and [`Stale`] as `ws1`; a hook gives
/// each event the task `t1` and the observation `o1`, Python's
/// `_kit_with_a_held_set`.
async fn held_room<S: ConversationStore>(kit: &RoomKit<S>) -> HeldRoom {
    let (reached, opened) = (
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(tokio::sync::Notify::new()),
    );
    let ws1 = Stale {
        reached: Arc::clone(&reached),
        opened: Arc::clone(&opened),
    };
    sms_and(kit, ws1).await;
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |_: RoomEvent, _| async {
            let task = Task::new(TaskId::new("t1").unwrap(), room_id(), "Stale");
            let id = ObservationId::new("o1").unwrap();
            let observation = Observation::new(id, room_id(), channel("sms1"), "stale");
            Ok::<_, ChannelError>(HookResult {
                tasks: vec![task],
                observations: vec![observation],
                ..HookResult::allow(AllowOptions::default())
            })
        },
        HookOptions {
            name: "triage".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let created = notes_created(kit);
    HeldRoom {
        reached,
        opened,
        created,
    }
}

/// `kit`'s call of the message `held` into `r1`, started in a task.
fn held_from<S: ConversationStore>(
    kit: &RoomKit<S>,
) -> tokio::task::JoinHandle<Result<roomkit::InboundResult, roomkit::RoomKitError>> {
    let caller = kit.clone();
    tokio::spawn(async move {
        let options = ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        };
        caller.process_inbound(sms("held"), options).await
    })
}

// Python: tests/test_hook_side_effects.py::TestGoneRoomSideEffects::test_a_set_whose_room_is_gone_writes_no_side_effects_and_says_why
// (RMK-1710): a room deleted while one of its sets runs, created again under
// its id or not, takes none of the set's side effects, and no ON_TASK_CREATED
// fires; one INFO line says why. The shared scenarios
// locks/the-side-effects-of-a-set-... pin what is written; this pins the line.
#[tokio::test]
async fn a_set_whose_room_is_gone_writes_no_side_effects_and_says_why() {
    for again in [false, true] {
        let kit = RoomKit::new(RoomKitOptions::default());
        let held = held_room(&kit).await;
        let (warnings, capturing) = Warnings::capture_at(Level::INFO);
        let call = held_from(&kit);
        tokio::time::timeout(Duration::from_secs(5), held.reached.notified())
            .await
            .unwrap();
        kit.store().delete_room(&room_id()).await.unwrap();
        if again {
            kit.create_room(CreateRoomOptions {
                room_id: Some(room_id()),
                ..Default::default()
            })
            .await
            .unwrap();
        }
        held.opened.notify_one();
        let result = call.await.unwrap().unwrap();
        roomkit::testing::idle(&kit).await;
        drop(capturing);
        let store = kit.store();
        let tasks = store
            .list_tasks(&room_id(), store::ListTasksOptions::default())
            .await
            .unwrap();
        let observations = store.list_observations(&room_id()).await.unwrap();
        let room = store.get_room(&room_id()).await.unwrap();
        kit.close().await.unwrap();

        let stale = room
            .as_ref()
            .is_some_and(|room| room.metadata.contains_key("stale"));
        let gone = warnings.containing("is gone");
        assert_eq!(
            (
                result.error.is_none(),
                tasks.len(),
                observations.len(),
                held.created.lock().unwrap().clone(),
                (room.is_some(), stale),
                gone.len(),
                gone.iter().all(|line| line.contains(GONE_LINE)),
            ),
            (true, 0, 0, Vec::<String>::new(), (again, false), 1, true),
            "created again: {again}, {gone:?}"
        );
    }
}

// Python: tests/test_hook_side_effects.py::TestGoneRoomSideEffects::test_a_set_whose_room_read_fails_still_writes_its_side_effects
// (RMK-1710, RFC §10.1 step 15, §15.2): a set whose room cannot be read
// before its side effects writes them all the same, the failure an ERROR
// line, and the call goes on.
#[tokio::test]
async fn a_set_whose_room_read_fails_still_writes_its_side_effects() {
    let kit = RoomKit::new(RoomKitOptions::with_store(FailingWrites::new(
        Failing::RoomRead,
    )));
    let held = held_room(&kit).await;
    let (warnings, capturing) = Warnings::capture();
    let call = held_from(&kit);
    tokio::time::timeout(Duration::from_secs(5), held.reached.notified())
        .await
        .unwrap();
    // The set's pass has read the room: the next read is its side effects'.
    kit.store().armed.store(true, Ordering::SeqCst);
    held.opened.notify_one();
    let result = call.await.unwrap().unwrap();
    roomkit::testing::idle(&kit).await;
    drop(capturing);
    kit.store().armed.store(false, Ordering::SeqCst);
    let store = kit.store();
    let tasks = store
        .list_tasks(&room_id(), store::ListTasksOptions::default())
        .await
        .unwrap();
    let observations = store.list_observations(&room_id()).await.unwrap();
    let room = store.get_room(&room_id()).await.unwrap().unwrap();
    kit.close().await.unwrap();

    let unread = warnings.containing("Could not read room r1 before writing its side effects");
    assert_eq!(
        (
            result.error.is_none(),
            sorted(tasks.into_iter().map(|task| task.id)),
            sorted(observations.into_iter().map(|o| o.id)),
            held.created.lock().unwrap().clone(),
            room.metadata.get("stale").cloned(),
            unread.len(),
        ),
        (
            true,
            sorted(["t1"]),
            sorted(["o1"]),
            vec!["t1".to_owned()],
            Some(true.into()),
            1
        )
    );
}
