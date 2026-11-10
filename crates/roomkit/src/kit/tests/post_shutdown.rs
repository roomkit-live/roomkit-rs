//! A real result post retained before its first poll meets the close's admission.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::*;
use crate::tasks::Kind;
use crate::testing::{EchoAgent, EchoAgentOptions};
use crate::{AttachChannelOptions, CreateRoomOptions, RoomKitOptions};
use roomkit_core::rules::transcoding::text_of;

// Python: tests/test_kit_close_doors.py::test_a_result_post_held_just_before_the_close_posts_nothing
// The task result post is the local core equivalent of private send_posted;
// discussion/post-turn orchestration remains a separate contract.
#[tokio::test(start_paused = true)]
async fn a_held_result_post_first_polled_during_close_posts_nothing() {
    for closing in [false, true] {
        let kit = RoomKit::new(RoomKitOptions::default());
        let room = RoomId::new("r").unwrap();
        let agent = ChannelId::new("agent").unwrap();
        kit.register_channel(EchoAgent::new(
            agent.clone(),
            "Unused",
            EchoAgentOptions::default(),
        ))
        .unwrap();
        kit.create_room(CreateRoomOptions {
            room_id: Some(room.clone()),
            ..CreateRoomOptions::default()
        })
        .await
        .unwrap();
        kit.attach_channel(&room, &agent, AttachChannelOptions::default())
            .await
            .unwrap();
        let result = DelegatedTaskResult {
            output: Some("result".to_owned()),
            ..DelegatedTaskResult::new(
                "task",
                RoomId::new("child").unwrap(),
                room.clone(),
                agent.clone(),
            )
        };
        let started = Arc::new(AtomicBool::new(false));
        let posted = kit
            .inner
            .tasks
            .spawn(Kind::Observer, {
                let (kit, started) = (kit.clone(), Arc::clone(&started));
                async move {
                    started.store(true, Ordering::SeqCst);
                    kit.post_result(&result, &agent, 0, "Find it.").await;
                }
            })
            .unwrap();
        assert!(!started.load(Ordering::SeqCst));
        if closing {
            kit.close().await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(1), posted)
            .await
            .unwrap()
            .unwrap();
        assert!(started.load(Ordering::SeqCst));
        let rows = kit
            .store()
            .list_events(&room, crate::store::ListEventsOptions::default())
            .await
            .unwrap();
        let texts: Vec<_> = rows
            .iter()
            .filter(|row| row.r#type == EventType::Message)
            .map(|row| text_of(&row.content))
            .collect();
        assert_eq!(
            texts,
            if closing {
                vec![]
            } else {
                vec!["Result of the task “Find it.”:\n\nresult".to_owned()]
            }
        );
        assert_eq!(kit.inner.tasks.left(), Vec::<&str>::new());
        kit.close().await.unwrap();
    }
}
