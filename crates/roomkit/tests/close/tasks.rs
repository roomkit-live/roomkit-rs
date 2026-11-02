//! The kit's task, delegation and feedback calls once its close started
//! (RFC §12.10.4, §23): refused but for the reads, which answer, and
//! `cancel_task`, a teardown call, which returns doing nothing (Python's
//! `_REFUSED`, `_READS` and `_TEARDOWN` in `tests/test_kit_close.py`).

use pretty_assertions::assert_eq;
use roomkit::model::Metadata;
use roomkit::{
    CancelTaskOptions, ConversationStore, DelegateOptions, InMemoryStore, ListObservationsOptions,
    ListTasksOptions, RejectInputOptions, RoomKit, RoomKitError, SetupBackgroundOptions,
    StartTaskOptions, SubmitFeedbackOptions, UpdateRoomMetadataOptions,
};

use crate::held::{Hold, Holding, a_send_held, closed, closing, kit_of, room};
use crate::support::channel;

/// Whether `result` is the refusal of a closing kit.
fn refused<T>(result: Result<T, RoomKitError>) -> bool {
    matches!(result.err(), Some(RoomKitError::KitClosed))
}

/// The kit's task and feedback calls that write or hand their work off,
/// each made once: whether it was refused. Their arguments are never read:
/// the agent is registered nowhere.
async fn the_kit_s_task_calls<S: ConversationStore>(kit: &RoomKit<S>) -> Vec<(&'static str, bool)> {
    let (r, ghost) = (room("r"), channel("ghost"));
    vec![
        (
            "delegate",
            refused(
                kit.delegate(&r, &ghost, "Find it.", DelegateOptions::default())
                    .await,
            ),
        ),
        (
            "start_task",
            refused(
                kit.start_task(&r, &ghost, "Find it.", StartTaskOptions::default())
                    .await,
            ),
        ),
        (
            "setup_background",
            refused(kit.setup_background(&ghost, SetupBackgroundOptions::default())),
        ),
        ("setup_task_tools", refused(kit.setup_task_tools(&ghost))),
        (
            "submit_feedback",
            refused(
                kit.submit_feedback(&r, 4.0, SubmitFeedbackOptions::default())
                    .await,
            ),
        ),
        (
            "update_room_metadata",
            refused(
                kit.update_room_metadata(&r, Metadata::new(), UpdateRoomMetadataOptions::default())
                    .await,
            ),
        ),
        (
            "answer_input",
            refused(kit.answer_input("req", "yes").await),
        ),
        (
            "reject_input",
            refused(kit.reject_input("req", RejectInputOptions::default()).await),
        ),
    ]
}

/// RFC §12.10.4: every task and feedback call but a read is refused
/// `KitClosed` once the close started, during it and after it, before it
/// reads its arguments (Python's
/// `test_every_other_call_is_refused_once_the_close_started`). On a6aa969
/// `submit_feedback`, `start_task`, `setup_background` and
/// `setup_task_tools` were taken.
#[tokio::test(start_paused = true)]
async fn every_task_call_is_refused_once_the_close_started() {
    let held = Holding::new("ws", [("held", Hold::UntilReleased)]);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["r"]).await;
    a_send_held(&kit, &held, "r").await;
    let close = closing(&kit).await;

    let during = the_kit_s_task_calls(&kit).await;
    held.release.notify_one();
    closed(close).await;
    let after = the_kit_s_task_calls(&kit).await;

    let all_refused: Vec<(&str, bool)> = during.iter().map(|&(name, _)| (name, true)).collect();
    assert_eq!((during, after), (all_refused.clone(), all_refused));
}

/// RFC §12.10.4: the task reads answer during the close, and `cancel_task`
/// returns `false` doing nothing (Python's
/// `test_a_read_answers_during_the_close_and_after_it_while_its_store_is_open`,
/// `test_a_teardown_call_returns_doing_nothing_once_the_close_started`).
#[tokio::test(start_paused = true)]
async fn the_task_reads_answer_and_a_cancel_does_nothing_once_the_close_started() {
    let held = Holding::new("ws", [("held", Hold::UntilReleased)]);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["r"]).await;
    a_send_held(&kit, &held, "r").await;
    let close = closing(&kit).await;

    let tasks = kit
        .list_tasks(&room("r"), ListTasksOptions::default())
        .await;
    let observations = kit
        .list_observations(&room("r"), ListObservationsOptions::default())
        .await;
    let cancelled = kit.cancel_task("task", CancelTaskOptions::default()).await;
    held.release.notify_one();
    closed(close).await;

    assert_eq!(
        (
            tasks.map(|tasks| tasks.len()),
            observations.map(|observations| observations.len()),
            cancelled
        ),
        (Ok(0), Ok(0), false)
    );
}
