//! Python's `test_declared_host_close`: close returns, while a send through a
//! closing hook then fails against the sealed kit. Cleanup still runs.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::hook::AllowOptions;
use roomkit::model::{HookResult, InboundMessage, Metadata, RoomEvent, TextContent};
use roomkit::turns::{DeclareHostVerbOptions, DeclaredVerb};
use roomkit::{
    ChannelError, HookOptions, HookTrigger, ProcessInboundOptions, RoomKitError, SendEventOptions,
    current_turn_host,
};
use roomkit_ai::{ToolHandler, ToolResult};
use serde_json::Value;
use tokio::sync::Notify;

type Stages = Arc<Mutex<Vec<&'static str>>>;

struct Finished {
    stages: Stages,
    done: Arc<Notify>,
}
impl Drop for Finished {
    fn drop(&mut self) {
        self.stages.lock().unwrap().push("callback finished");
        self.done.notify_one();
    }
}

#[tokio::test(start_paused = true)]
async fn a_declared_callback_sees_its_close_through_directly_or_in_a_hook() {
    for streaming in [false, true] {
        for through_hook in [false, true] {
            check(streaming, through_hook).await;
        }
    }
}

async fn check(streaming: bool, through_hook: bool) {
    let handler = ToolHandler::new(|_, _| async {
        let _answer = current_turn_host().unwrap().call("app.close", None).await;
        Ok(ToolResult::Text("closed".into()))
    });
    let kit = super::kit(streaming, handler).await;
    let stages: Stages = Arc::default();
    let done = Arc::new(Notify::new());
    closing_hook(&kit, &stages);
    let weak = kit.downgrade();
    let (writes, ended) = (Arc::clone(&stages), Arc::clone(&done));
    let verb = DeclaredVerb::new(move |_: Metadata| {
        let (weak, stages, done) = (weak.clone(), Arc::clone(&writes), Arc::clone(&ended));
        async move {
            stages.lock().unwrap().push("callback entered");
            let _finished = Finished {
                stages: Arc::clone(&stages),
                done,
            };
            let kit = weak.upgrade().unwrap();
            if through_hook {
                let result = kit
                    .send_event(
                        &"r".parse().unwrap(),
                        &"ai".parse().unwrap(),
                        TextContent::new("close owner"),
                        SendEventOptions::default(),
                    )
                    .await;
                assert!(
                    matches!(&result, Err(RoomKitError::KitClosed)),
                    "send_event after close: {result:?}"
                );
                stages
                    .lock()
                    .unwrap()
                    .push("send_event raised KitClosedError");
                let error: ChannelError = Box::new(RoomKitError::KitClosed);
                return Err(error);
            }
            kit.close().await?;
            stages.lock().unwrap().push("close returned");
            Ok(Value::Null)
        }
    });
    kit.declare_host_verb("app.close", verb, DeclareHostVerbOptions::default())
        .unwrap();
    let asking = tokio::spawn({
        let kit = kit.clone();
        async move {
            kit.process_inbound(
                InboundMessage::new("web".parse().unwrap(), "alice", TextContent::new("go")),
                ProcessInboundOptions {
                    room_id: Some("r".parse().unwrap()),
                    ..Default::default()
                },
            )
            .await
        }
    });
    let ended = tokio::time::timeout(Duration::from_secs(30), done.notified()).await;
    assert!(
        ended.is_ok(),
        "callback stages: {:?}",
        stages.lock().unwrap()
    );
    let _answer = asking.await.unwrap();
    let expected = if through_hook {
        vec![
            "callback entered",
            "hook entered",
            "hook returned",
            "send_event raised KitClosedError",
            "callback finished",
        ]
    } else {
        vec!["callback entered", "close returned", "callback finished"]
    };
    assert_eq!(*stages.lock().unwrap(), expected);
    roomkit::testing::idle(&kit).await;
}

fn closing_hook(kit: &roomkit::RoomKit, stages: &Stages) {
    let weak = kit.downgrade();
    let stages = Arc::clone(stages);
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
            let (weak, stages) = (weak.clone(), Arc::clone(&stages));
            async move {
                if roomkit_core::rules::transcoding::text_of(&event.content) == "close owner" {
                    stages.lock().unwrap().push("hook entered");
                    weak.upgrade().unwrap().close().await?;
                    stages.lock().unwrap().push("hook returned");
                }
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        HookOptions::default(),
    )
    .unwrap();
}
