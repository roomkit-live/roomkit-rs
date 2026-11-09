//! Public core operations added after the original shutdown matrix.

use pretty_assertions::assert_eq;
use roomkit::model::{AgentResponsePolicy, RoomTimers};

use super::*;

pub(super) async fn operations<S: ConversationStore>(
    kit: &RoomKit<S>,
) -> Vec<(&'static str, bool)> {
    let r = room("r");
    let sms = channel("sms");
    let participant = "u1".parse().unwrap();
    vec![
        (
            "add_room_hook",
            refused(
                kit.add_room_hook(
                    &r,
                    HookTrigger::BeforeBroadcast,
                    HookExecution::Sync,
                    |_: RoomEvent, _| async { HookResult::allow(AllowOptions::default()) },
                    Default::default(),
                )
                .await,
            ),
        ),
        (
            "remove_room_hook",
            refused(
                kit.remove_room_hook(&r, "unknown", Default::default())
                    .await,
            ),
        ),
        (
            "regenerate_response",
            refused(kit.regenerate_response(&r, Default::default()).await),
        ),
        (
            "send_greeting",
            refused(kit.send_greeting(&r, Default::default()).await),
        ),
        (
            "set_room_timers",
            refused(
                kit.set_room_timers(&r, RoomTimers::default(), Default::default())
                    .await,
            ),
        ),
        (
            "set_agent_response_policy",
            refused(
                kit.set_agent_response_policy(
                    &r,
                    AgentResponsePolicy::AddressedOnly,
                    Default::default(),
                )
                .await,
            ),
        ),
        (
            "deliver",
            refused(kit.deliver(&r, "late", Default::default()).await),
        ),
        (
            "mute_output",
            refused(kit.mute_output(&r, &sms, Default::default()).await),
        ),
        (
            "unmute_output",
            refused(kit.unmute_output(&r, &sms, Default::default()).await),
        ),
        (
            "record_failed_attempt",
            refused(
                kit.record_failed_attempt("unknown", Default::default())
                    .await,
            ),
        ),
        (
            "add_member",
            refused(
                kit.add_member(&r, &sms, &participant, Default::default())
                    .await,
            ),
        ),
        (
            "remove_member",
            refused(
                kit.remove_member(&r, &participant, Default::default())
                    .await,
            ),
        ),
        (
            "rename_member",
            refused(
                kit.rename_member(&r, &participant, "Late", Default::default())
                    .await,
            ),
        ),
        ("start", refused(kit.start().await)),
    ]
}

// Python: tests/test_kit_close.py::test_a_teardown_call_returns_doing_nothing_once_the_close_started
#[tokio::test(start_paused = true)]
async fn cancelling_a_delivery_once_the_close_started_does_nothing() {
    let held = Holding::new("ws", [("held", Hold::UntilReleased)]);
    let kit = kit_of(InMemoryStore::default(), &[&held], &["r"]).await;
    let mut message = said("held");
    message.delivery_key = Some("held".to_owned());
    let initial = kit
        .process_inbound(
            message,
            ProcessInboundOptions {
                defer_delivery: true,
                ..into("r")
            },
        )
        .await
        .unwrap();
    held.entered("held").await;
    let close = closing(&kit).await;
    let during = kit.cancel_delivery("held", Default::default()).await;
    held.release.notify_one();
    closed(close).await;
    let result = initial.delivery.unwrap().wait().await.unwrap();
    assert_eq!(
        (
            during,
            kit.cancel_delivery("held", Default::default()).await,
            result.cancellation_reason.as_deref()
        ),
        (0, 0, Some("kit_closed"))
    );
}
