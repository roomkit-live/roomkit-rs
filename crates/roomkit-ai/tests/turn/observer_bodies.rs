//! The lifetime of a turn observer's own body, independently of its siblings.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::hook::AllowOptions;
use roomkit::model::{HookResult, RoomEvent, TextContent};
use roomkit::{HookExecution, HookOptions, HookTrigger, RoomKit, RoomKitOptions, SendEventOptions};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, AiResponseEvent};
use roomkit_core::rules::transcoding::text_of;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::support::{Room, channel, room_id};

const RETRIES: usize = 5;
const BOUND: Duration = Duration::from_secs(5);
type Calls = Arc<Mutex<Vec<JoinHandle<RoomEvent>>>>;

// Python: tests/test_reentrance.py::TestACallFromAnAsyncHook::test_a_relaunch_a_turn_s_observer_starts_begins_a_new_chain
#[tokio::test]
async fn a_relaunch_a_turn_s_observer_starts_begins_a_new_chain() {
    for execution in [HookExecution::Async, HookExecution::Sync] {
        let model = ScriptedProvider::new((0..=RETRIES).map(|_| ScriptedRound::answering("ok")));
        let agent = AiChannel::new(
            channel("assistant"),
            model.clone(),
            AiChannelOptions::default(),
        )
        .unwrap();
        let room = Room::on_kit(
            RoomKit::new(RoomKitOptions {
                max_chain_depth: 5,
                ..Default::default()
            }),
            agent,
        )
        .await;
        let calls: Calls = Arc::default();
        let (auditing, audit_started) = watch::channel(0_usize);
        let (deciding, decided) = watch::channel(0_usize);
        register_relaunch(&room.kit, execution, Arc::clone(&calls), audit_started);
        register_audit(&room.kit, auditing, decided);
        room.kit
            .hook(
                HookTrigger::BeforeBroadcast,
                move |event: RoomEvent, _| {
                    if text_of(&event.content) == "Gate failed: retry." {
                        deciding.send_modify(|count| *count += 1);
                    }
                    std::future::ready(Ok(HookResult::allow(AllowOptions::default())))
                },
                HookOptions {
                    name: "decided".to_owned(),
                    ..Default::default()
                },
            )
            .unwrap();
        tokio::time::timeout(BOUND, room.says("Write the report."))
            .await
            .unwrap();
        tokio::time::timeout(BOUND, roomkit::testing::idle(&room.kit))
            .await
            .unwrap();
        let calls = std::mem::take(&mut *calls.lock().unwrap());
        let mut depths = Vec::new();
        for call in calls {
            let event = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
            assert_eq!(event.blocked_by, None);
            depths.push(event.chain_depth);
        }
        assert_eq!(depths, vec![0; RETRIES], "{execution:?}");
        assert_eq!(model.requests().len(), RETRIES + 1);
        room.kit.close().await.unwrap();
    }
}

fn register_relaunch(
    kit: &RoomKit,
    execution: HookExecution,
    calls: Calls,
    audit_started: watch::Receiver<usize>,
) {
    let weak = kit.downgrade();
    kit.hook(
        HookTrigger::OnAiResponse,
        move |_event: AiResponseEvent, _| {
            let (weak, calls, mut audit) =
                (weak.clone(), Arc::clone(&calls), audit_started.clone());
            async move {
                let number = calls.lock().unwrap().len() + 1;
                if number > RETRIES {
                    return;
                }
                let task = tokio::spawn(roomkit::keep_context(async move {
                    // On this single-thread runtime the spawning body has returned
                    // before this task is polled. Its sibling still holds the turn.
                    audit.wait_for(|started| *started >= number).await.unwrap();
                    weak.upgrade()
                        .unwrap()
                        .send_event(
                            &room_id(),
                            &channel("sms"),
                            TextContent::new("Gate failed: retry."),
                            SendEventOptions::default(),
                        )
                        .await
                        .unwrap()
                }));
                calls.lock().unwrap().push(task);
            }
        },
        HookOptions {
            name: "gate".to_owned(),
            execution,
            ..Default::default()
        },
    )
    .unwrap();
}

fn register_audit(kit: &RoomKit, auditing: watch::Sender<usize>, decided: watch::Receiver<usize>) {
    kit.hook(
        HookTrigger::OnAiResponse,
        move |_event: AiResponseEvent, _| {
            let (auditing, mut decided) = (auditing.clone(), decided.clone());
            async move {
                let mut number = 0;
                auditing.send_modify(|count| {
                    *count += 1;
                    number = *count;
                });
                if number <= RETRIES {
                    // The new call's depth is already fixed when BEFORE_BROADCAST
                    // releases us; waiting for its delivery would wait on this turn.
                    decided.wait_for(|count| *count >= number).await.unwrap();
                }
            }
        },
        HookOptions {
            name: "audit".to_owned(),
            execution: HookExecution::Async,
            ..Default::default()
        },
    )
    .unwrap();
}
