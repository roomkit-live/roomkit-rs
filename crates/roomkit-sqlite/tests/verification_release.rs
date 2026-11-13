//! A held message released by a hook whose work goes on after the release,
//! on the SQLite store (RFC §11.7.6): the store's own timing hides no
//! answer that leaves the room's lane before that work is over.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
#![expect(
    clippy::expect_used,
    reason = "a test fails on a condition not met in time"
)]

#[path = "../../roomkit/tests/support/hook_release.rs"]
mod hook_release;
mod support;

use roomkit_sqlite::{SqliteStore, SqliteStoreOptions};

use crate::support::TempFile;

fn store(file: &TempFile) -> SqliteStore {
    SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    })
}

// Python: tests/test_verification_release_cancel.py::test_a_release_from_a_hook_whose_work_then_closes_the_room_is_answered_by_no_agent[sqlite]
/// The released answer leaves its place in the lane once the hook's work is
/// over, on the room as that work left it: closed, it refuses the answer
/// before any agent runs.
#[tokio::test]
async fn a_release_from_a_hook_whose_work_then_closes_the_room_is_answered_by_no_agent() {
    let file = TempFile::new("hook-release-closes");
    let released = hook_release::a_hook_releases(store(&file), true).await;

    assert_eq!(
        (
            released.note.blocked,
            released.note.reason.as_deref(),
            released.asked.clone(),
            released.refused.clone()
        ),
        (
            true,
            Some("room_closed"),
            Vec::<String>::new(),
            vec!["inbound".to_owned(), "regenerate".to_owned()]
        )
    );
    released.kit.close().await.unwrap();
}

// Python: tests/test_verification_release_cancel.py::test_a_release_from_a_hook_whose_work_then_lets_its_event_through_is_answered_first[sqlite]
/// The released answer's place in the lane is ahead of the hook's event's
/// delivery: the released message is answered first.
#[tokio::test]
async fn a_release_from_a_hook_whose_work_then_lets_its_event_through_is_answered_first() {
    let file = TempFile::new("hook-release-through");
    let released = hook_release::a_hook_releases(store(&file), false).await;
    let note = released.note.event.clone().unwrap();
    let replies = released.replies().await;

    assert_eq!(
        (
            released.note.blocked,
            released.asked.clone(),
            replies
                .iter()
                .map(|reply| reply.responds_to.clone())
                .collect::<Vec<_>>(),
            released.refused.clone()
        ),
        (
            false,
            vec!["Wire 500 to Bob".to_owned(), "Verified by phone".to_owned()],
            vec![Some(released.held.id.clone()), Some(note.id.clone())],
            Vec::<String>::new()
        )
    );
    assert!(note.index < replies[0].index);
    released.kit.close().await.unwrap();
}
