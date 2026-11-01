//! Proactive delivery and what providers report of it (RFC §10.4, §22),
//! what the shared scenarios cannot register: the `on_delivery_status`
//! handler, which takes the status alone.

use std::time::Duration;

use roomkit::model::{DeliveryStatus, RoomId};
use roomkit::{CreateRoomOptions, ProcessDeliveryStatusOptions, RoomKit, RoomKitOptions};
use tokio::sync::mpsc;

/// How long a test waits for what the kit does in the background.
const BOUND: Duration = Duration::from_secs(5);

// Python: src/roomkit/core/mixins/hooks_api.py on_delivery_status, an
// ASYNC ON_DELIVERY_STATUS hook given the status alone, which
// process_delivery_status runs and does not wait for.
#[tokio::test]
async fn an_on_delivery_status_handler_is_given_each_status_dispatched() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let room = RoomId::new("room-1").unwrap();
    let options = CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(options).await.unwrap();
    let (seen, mut statuses) = mpsc::unbounded_channel();
    kit.on_delivery_status(move |status: DeliveryStatus| {
        let seen = seen.clone();
        async move {
            drop(seen.send(status));
        }
    })
    .unwrap();
    let status = DeliveryStatus {
        room_id: Some(room),
        ..DeliveryStatus::new("mock", "m1", "delivered")
    };

    let options = ProcessDeliveryStatusOptions::default();
    kit.process_delivery_status(status.clone(), options)
        .await
        .unwrap();

    let given = tokio::time::timeout(BOUND, statuses.recv()).await.unwrap();
    assert_eq!(given, Some(status));
    kit.close().await.unwrap();
}
