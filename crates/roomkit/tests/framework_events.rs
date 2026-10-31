//! Framework events through `RoomKit::on` (RFC §8.2, decision 10 of step
//! 5): the Rust twins of the probe `step5/framework_handler_inline.py`,
//! which printed `after create_room returned: [('room_created', 'r1',
//! ['room_id'], True)]`.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::RoomId;
use roomkit::{CreateRoomOptions, RoomKit, RoomKitOptions};

#[path = "support/warnings.rs"]
mod warnings;

use warnings::Warnings;

/// What a handler noted of an event: its type, its room and its data's
/// members.
type Noted = Arc<Mutex<Vec<(String, Option<String>, Vec<String>)>>>;

fn room_r1() -> CreateRoomOptions {
    CreateRoomOptions {
        room_id: Some(RoomId::new("r1").unwrap()),
        ..Default::default()
    }
}

/// Python awaits each handler before the call that emitted the event goes
/// on: one that takes its time has finished once `create_room` returned.
#[tokio::test(start_paused = true)]
async fn a_handler_finishes_before_its_call_returns() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let noted: Noted = Arc::default();
    let record = Arc::clone(&noted);
    kit.on("room_created", move |event| {
        let record = Arc::clone(&record);
        async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            record.lock().unwrap().push((
                event.r#type,
                event.room_id.map(String::from),
                event.data.keys().cloned().collect(),
            ));
        }
    });

    kit.create_room(room_r1()).await.unwrap();

    assert_eq!(
        noted.lock().unwrap().clone(),
        [(
            "room_created".to_owned(),
            Some("r1".to_owned()),
            vec!["room_id".to_owned()]
        )]
    );
}

/// A handler that fails is logged, the call succeeds, and the handler
/// registered after it still runs, as Python's loop goes on past it.
#[tokio::test]
async fn a_failing_handler_is_logged() {
    let (warnings, _capturing) = Warnings::capture();
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.on("room_created", |_| async { panic!("handler failed") });
    let ran = Arc::new(Mutex::new(false));
    let runs = Arc::clone(&ran);
    kit.on("room_created", move |_| {
        *runs.lock().unwrap() = true;
        std::future::ready(())
    });

    let created = kit.create_room(room_r1()).await;

    assert_eq!(
        (
            created.is_ok(),
            *ran.lock().unwrap(),
            warnings
                .containing("a framework event handler failed")
                .len()
        ),
        (true, true, 1)
    );
}
