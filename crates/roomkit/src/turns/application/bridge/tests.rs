use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::turns::{
    Carried, Observers, TurnContext, TurnFuture, TurnRequest, TurnTimeouts, current_turn_id, lock,
    pipe::pipe,
};

struct Link {
    request: TurnRequest,
    frames: Mutex<VecDeque<(u64, Frame)>>,
    contexts: Mutex<Vec<Option<String>>>,
    released: AtomicUsize,
}

impl Link {
    fn new(frames: impl IntoIterator<Item = (u64, Frame)>) -> Self {
        let fixture = include_str!("../../../../tests/fixtures/turns/python-case1.jsonl");
        let row: serde_json::Value = serde_json::from_str(fixture.lines().next().unwrap()).unwrap();
        Self {
            request: serde_json::from_value(row["message"].clone()).unwrap(),
            frames: Mutex::new(frames.into_iter().collect()),
            contexts: Mutex::default(),
            released: AtomicUsize::default(),
        }
    }
}

impl TurnLink for Link {
    fn request(&self) -> &TurnRequest {
        &self.request
    }
    fn taken(&self) -> bool {
        true
    }
    fn receive(&self) -> TurnFuture<'_, Result<(u64, Frame), RoomKitError>> {
        Box::pin(async { Ok(lock(&self.frames).pop_front().unwrap()) })
    }
    fn send(&self, _: Control) -> TurnFuture<'_, Result<(), RoomKitError>> {
        // A backend is free to capture context before it returns the future.
        lock(&self.contexts).push(current_turn_id());
        Box::pin(async { Ok(()) })
    }
    fn release(&self) {
        self.released.fetch_add(1, Ordering::SeqCst);
    }
}

fn memory() -> (Arc<MemoryTurn>, PipeReceiver<Frame>, PipeReceiver<Control>) {
    let id: Arc<str> = Arc::from("turn");
    let (frames, incoming) = pipe(Arc::clone(&id), Carried::Frame, Observers::default());
    let (controls, outgoing) = pipe(Arc::clone(&id), Carried::Control, Observers::default());
    (
        Arc::new(MemoryTurn::new(
            id,
            frames,
            controls,
            TurnTimeouts::default(),
        )),
        incoming,
        outgoing,
    )
}

#[tokio::test]
async fn a_pull_carries_the_context_of_the_first_reader() {
    let (memory, _, controls) = memory();
    let link = Link::new([]);
    TurnContext::default()
        .in_turn(Arc::from("first-reader"))
        .scope(async {
            memory.lend_stream_context();
        })
        .await;
    let pull: Control =
        serde_json::from_value(serde_json::json!({"type": "pull", "count": 1})).unwrap();
    memory.controls.put(&Control::Heartbeat).unwrap();
    memory.controls.put(&pull).unwrap();
    memory.controls.shut();
    send(&link, &memory, controls).await.unwrap();
    assert_eq!(
        *lock(&link.contexts),
        vec![None, Some("first-reader".to_owned())]
    );
}

#[tokio::test]
async fn a_duplicate_stays_out_of_the_stream_and_a_gap_fails_it() {
    let link = Link::new([
        (1, Frame::Heartbeat),
        (1, Frame::Heartbeat),
        (3, Frame::End),
    ]);
    let (memory, mut incoming, _) = memory();
    let error = receive(&link, &memory).await.unwrap_err();
    assert!(matches!(error, RoomKitError::TurnLink { reason, .. } if reason == "frame_lost"));
    memory.frames.shut();
    assert_eq!(incoming.get().await.unwrap(), (1, Frame::Heartbeat));
    assert!(incoming.get().await.is_err());
}

#[test]
fn refusing_to_spawn_the_driver_still_releases_its_link_once() {
    let link = Arc::new(Link::new([]));
    let (memory, _, controls) = memory();
    let driver = run_link(
        Arc::<Link>::clone(&link),
        memory,
        controls,
        Box::pin(std::future::pending()),
    );
    drop(driver);
    assert_eq!(link.released.load(Ordering::SeqCst), 1);
}
