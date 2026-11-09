//! Observers start in firing and registration order, but finish independently
//! (RFC §9.2, §9.3; Python's `HookEngine._start_async_hooks`).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::time::Duration;

use roomkit::model::{DeliveryStatus, Observation, RoomId};
use roomkit::orchestration::{PostAsyncOptions, StatusEntry, StatusLevel};
use roomkit::{
    CreateRoomOptions, HookExecution, HookOptions, HookPayload, HookTrigger,
    ProcessDeliveryStatusOptions, RoomKit, RoomKitOptions, SubmitFeedbackOptions,
};
use tokio::sync::{mpsc, watch};

const COUNT: usize = 100;
const BOUND: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
enum Source {
    Feedback,
    Delivery,
    Status,
}

fn register<P: HookPayload>(
    kit: &RoomKit,
    trigger: HookTrigger,
    read: fn(P) -> String,
    seen: &mpsc::UnboundedSender<(String, usize)>,
    release: &watch::Receiver<bool>,
) {
    for position in 0..2 {
        let seen = seen.clone();
        let release = release.clone();
        kit.hook(
            trigger,
            move |payload: P, _| {
                let seen = seen.clone();
                let mut release = release.clone();
                async move {
                    seen.send((read(payload), position)).unwrap();
                    // Every observer must start while none can finish.
                    release.wait_for(|released| *released).await.unwrap();
                }
            },
            HookOptions {
                name: format!("observer-{position}"),
                execution: HookExecution::Async,
                ..HookOptions::default()
            },
        )
        .unwrap();
    }
}

async fn fire(kit: &RoomKit, room: &RoomId, source: Source, number: usize) {
    let number = number.to_string();
    match source {
        Source::Feedback => kit
            .submit_feedback(
                room,
                0.5,
                SubmitFeedbackOptions {
                    comment: number,
                    ..SubmitFeedbackOptions::default()
                },
            )
            .await
            .unwrap(),
        Source::Delivery => kit
            .process_delivery_status(
                DeliveryStatus {
                    room_id: Some(room.clone()),
                    ..DeliveryStatus::new("mock", number, "delivered")
                },
                ProcessDeliveryStatusOptions::default(),
            )
            .await
            .unwrap(),
        Source::Status => {
            kit.status_bus()
                .post_async(
                    "agent",
                    number,
                    StatusLevel::Ok,
                    PostAsyncOptions {
                        metadata: Some(
                            [(String::from("room_id"), room.as_str().into())]
                                .into_iter()
                                .collect(),
                        ),
                        ..PostAsyncOptions::default()
                    },
                )
                .await;
        }
    }
}

async fn ordered(source: Source) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let room = kit.create_room(CreateRoomOptions::default()).await.unwrap();
    let (seen, mut heard) = mpsc::unbounded_channel();
    let (release, released) = watch::channel(false);
    match source {
        Source::Feedback => register(
            &kit,
            HookTrigger::OnFeedback,
            |event: Observation| {
                event
                    .metadata
                    .get("comment")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_owned()
            },
            &seen,
            &released,
        ),
        Source::Delivery => register(
            &kit,
            HookTrigger::OnDeliveryStatus,
            |event: DeliveryStatus| event.message_id,
            &seen,
            &released,
        ),
        Source::Status => register(
            &kit,
            HookTrigger::OnStatusPosted,
            |event: StatusEntry| event.action,
            &seen,
            &released,
        ),
    }
    for number in 0..COUNT {
        fire(&kit, &room.id, source, number).await;
    }
    let actual = tokio::time::timeout(BOUND, async {
        let mut actual = Vec::new();
        for _ in 0..COUNT * 2 {
            actual.push(heard.recv().await.unwrap());
        }
        actual
    })
    .await;
    release.send(true).unwrap();
    tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
        .await
        .unwrap();
    kit.close().await.unwrap();
    let expected: Vec<_> = (0..COUNT)
        .flat_map(|number| [(number.to_string(), 0), (number.to_string(), 1)])
        .collect();
    assert_eq!(actual.unwrap(), expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn feedback_observers_start_in_order_without_waiting_for_their_predecessors_to_end() {
    ordered(Source::Feedback).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivery_status_observers_start_in_order_without_waiting_for_their_predecessors_to_end() {
    ordered(Source::Delivery).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_observers_start_in_order_without_waiting_for_their_predecessors_to_end() {
    ordered(Source::Status).await;
}
