//! RFC §10.1 step 18, §12.10.4: a call whose run the close cuts reports
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
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, DeliveryHandle, HookOptions,
    HookTrigger, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use roomkit_core::ops::ChannelConfig;
use tokio::sync::Notify;

use crate::support::{BOUND, channel, into, message, room_id};
use roomkit_core::rules::transcoding::text_of;

/// An agent whose turn on the echo of `side` never ends, until the close
/// cuts it, and that answers nothing.
struct Stuck {
    started: Arc<Notify>,
}

impl Stuck {
    /// The configuration the kit keeps of the channel.
    fn config() -> ChannelConfig {
        ChannelConfig::new(
            channel("stuck"),
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        )
    }
}

impl Channel for Stuck {
    fn channel_id(&self) -> roomkit::model::ChannelId {
        Self::config().channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        Self::config().channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        Self::config().category
    }

    fn direction(&self) -> ChannelDirection {
        Self::config().direction
    }

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
        Self::config().capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        Self::config().provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        Self::config().recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        Self::config().supports_streaming_delivery
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

    fn handle_inbound(
        &self,
        message: roomkit::model::InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl std::future::Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
}

/// A locked `BEFORE_BROADCAST` hook makes the deferred call `side` when it
/// sees `hello`, keeping its handle; an agent echoes every message, and a
/// second one's turn on the echo of `side` never ends. Once that turn
/// started, the kit closes, cutting it at once: the call `hello` reads
/// the answer its cascade stored, `You said: hello`, and
/// the adopted call's handle `You said: side`, as Python's do
/// (`close_while_held.out`).
#[tokio::test(start_paused = true)]
async fn a_call_the_close_aborts_reads_the_answers_its_cascade_stored() {
    assert_eq!(answers_under_the_close().await, said(["hello", "side"]));
}

/// [`a_call_the_close_aborts_reads_the_answers_its_cascade_stored`] on a
/// runtime of several threads, where the adopted call's report comes before
/// the cut. The close cuts the stuck turn at once, with no grace, so it
/// runs with the others (on fefbfac it waited the grace, 5 s of real time,
/// and ran on demand).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_the_close_aborts_reads_its_answers_on_threads() {
    assert_eq!(answers_under_the_close().await, said(["hello", "side"]));
}

/// The echoes of `texts`.
fn said<const N: usize>(texts: [&str; N]) -> [Vec<String>; N] {
    texts.map(|text| vec![format!("You said: {text}")])
}

/// What the call `hello` and the adopted call's handle read of their
/// answers, once the close aborted them.
async fn answers_under_the_close() -> [Vec<String>; 2] {
    let started = Arc::new(Notify::new());
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(EchoAgent::new(
        channel("ai"),
        "You said: ",
        EchoAgentOptions { quote: true },
    ))
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
    let spec = HookOptions {
        name: "side".to_owned(),
        ..Default::default()
    };
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _| {
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
        },
        spec,
    )
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
    let handle = handles.lock().unwrap().pop().unwrap();
    tokio::time::timeout(BOUND * 4, kit.close())
        .await
        .unwrap()
        .unwrap();
    let outer = tokio::time::timeout(BOUND, outer)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let adopted = tokio::time::timeout(BOUND, handle.wait())
        .await
        .unwrap()
        .unwrap();
    [outer, adopted].map(|result| {
        result
            .response_events
            .iter()
            .map(|event| text_of(&event.content))
            .collect()
    })
}
