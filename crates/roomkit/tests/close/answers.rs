//! RFC §10.1 step 18, §12.10.4: a call whose run the close aborts reports
//! the answers its cascade stored, as Python's caller reads its cascade's
//! `response_events` (light review A, A4; probe
//! `tasks/probes/step5-lot2/close_while_held.py`).

use std::future::Future;
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelOutput, ChannelType, HookResult,
    RoomContext, RoomEvent,
};
use roomkit::testing::{EchoAgent, RecordingTransport};
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, CreateRoomOptions,
    DeliveryHandle, HookExecution, HookSpec, HookTrigger, ProcessInboundOptions, RoomKit,
    RoomKitOptions, text_of,
};
use tokio::sync::Notify;

use crate::support::{BOUND, channel, into, message, room_id};

/// An agent whose turn on the echo of `side` never ends, until the close
/// cuts it, and that answers nothing.
struct Stuck {
    started: Arc<Notify>,
}

impl Channel for Stuck {
    fn declaration(&self) -> ChannelDeclaration {
        ChannelDeclaration::new(
            channel("stuck"),
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        )
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let stuck = text_of(&event.content) == "You said: side";
        let started = Arc::clone(&self.started);
        async move {
            if stuck {
                started.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(ChannelOutput::empty())
        }
    }
}

/// A locked `BEFORE_BROADCAST` hook makes the deferred call `side` when it
/// sees `hello`, keeping its handle; an agent echoes every message, and a
/// second one's turn on the echo of `side` never ends. Once that turn
/// started, the kit closes, its grace running out: the call `hello`,
/// aborted, reads the answer its cascade stored, `You said: hello`, as
/// Python's does.
#[tokio::test(start_paused = true)]
async fn a_call_the_close_aborts_reads_the_answers_its_cascade_stored() {
    let started = Arc::new(Notify::new());
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
        .unwrap();
    kit.register_channel(EchoAgent::new(channel("ai"), "You said: ").quoting())
        .unwrap();
    kit.register_channel(Stuck {
        started: Arc::clone(&started),
    })
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ai", "stuck"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let handles: Arc<Mutex<Vec<DeliveryHandle>>> = Arc::default();
    let (hook_kit, hook_handles) = (kit.clone(), Arc::clone(&handles));
    let spec = HookSpec::new("side", HookTrigger::BeforeBroadcast, HookExecution::Sync);
    kit.hook(spec, move |event, _| {
        let (kit, handles) = (hook_kit.clone(), Arc::clone(&hook_handles));
        async move {
            if text_of(&event.content) == "hello" {
                let deferred = ProcessInboundOptions {
                    defer_delivery: true,
                    ..into(room_id())
                };
                let side = kit.process_inbound(message("side"), deferred).await?;
                handles.lock().unwrap().push(side.delivery.unwrap());
            }
            Ok(HookResult::allow(AllowOptions::default()))
        }
    })
    .unwrap();
    let outer_kit = kit.clone();
    let outer = tokio::spawn(async move {
        outer_kit
            .process_inbound(message("hello"), into(room_id()))
            .await
    });
    tokio::time::timeout(BOUND, started.notified())
        .await
        .unwrap();
    tokio::time::timeout(BOUND * 4, kit.close())
        .await
        .unwrap()
        .unwrap();
    let outer = tokio::time::timeout(BOUND, outer)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let answers: Vec<String> = outer
        .response_events
        .iter()
        .map(|event| text_of(&event.content))
        .collect();
    assert_eq!(answers, ["You said: hello"]);
}
