//! An unhandled reentry cap ends the application's hook loop with one
//! warning, not an exception traceback (RFC §8.3, §9.3).

use super::*;
use pretty_assertions::assert_eq;

// Python: tests/test_reentrance.py::TestADirectChangeFromAHook::test_a_cap_error_a_body_lets_escape_is_logged_as_a_warning
#[tokio::test]
async fn a_cap_error_a_body_lets_escape_is_logged_as_one_warning() {
    let kit = room(1).await;
    let (warnings, _capturing) = warnings::Warnings::capture();
    let weak = kit.downgrade();
    kit.hook(
        HookTrigger::OnEventUpdated,
        move |event: RoomEvent, _| {
            let kit = weak.upgrade().unwrap();
            async move {
                let n = event.metadata["n"].as_u64().unwrap();
                // A fixture bound above the cap keeps a broken implementation
                // finite, without making its missing warning pass.
                if n < 20 {
                    let metadata =
                        roomkit::model::Metadata::from_iter([("n".to_owned(), (n + 1).into())]);
                    kit.update_event(
                        &room_id(),
                        &event.id,
                        UpdateEventOptions {
                            metadata: Some(metadata),
                            ..Default::default()
                        },
                    )
                    .await?;
                }
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        HookOptions {
            name: "again".to_owned(),
            execution: HookExecution::Async,
            ..Default::default()
        },
    )
    .unwrap();
    let event = kit
        .send_event(
            &room_id(),
            &channel("sms"),
            TextContent::new("v"),
            SendEventOptions::default(),
        )
        .await
        .unwrap();
    let metadata = roomkit::model::Metadata::from_iter([("n".to_owned(), 0.into())]);
    kit.update_event(
        &room_id(),
        &event.id,
        UpdateEventOptions {
            metadata: Some(metadata),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), roomkit::testing::idle(&kit))
        .await
        .unwrap();
    let lines = warnings.containing("hook=again");
    // Compare every emitted field: a cap warning carries the hook and
    // error message, with no exception/stack attachment.
    assert_eq!(
        lines,
        vec![format!(
            "WARN message=an observer hook raised hook=again error={} ",
            RoomKitError::ReentryLoopCap(room_id()),
        )]
    );
    kit.close().await.unwrap();
}
