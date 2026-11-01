//! How many of a room's latest events its context holds (RFC §6.1
//! `recent_events_window`, §10.1 step 3), as the Python implementation's
//! kit loads them (`_resolve_recent_events_limit`, probed in
//! `tasks/probes/step5-lot5/py_context_window.py` on roomkit 38ef0625): the
//! largest window a channel bound to the room declares, at least 50 while a
//! hook is registered. A reader's broadcast context holds the event it is
//! about besides, as Python's does (RMK-1070); a hook reads the window.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{Future, ready};
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, HookResult,
    InboundMessage, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, HookOptions, HookTrigger,
    ProcessInboundOptions, RoomKit, RoomKitOptions,
};

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// A transport declaring `window`, noting how many events each context it
/// is handed holds.
#[derive(Clone)]
struct Reader {
    window: usize,
    seen: Arc<Mutex<Vec<usize>>>,
}

impl Channel for Reader {
    fn channel_id(&self) -> ChannelId {
        channel("reader")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Transport
    }

    fn recipient_key(&self) -> Option<String> {
        None
    }

    fn recent_events_window(&self) -> usize {
        self.window
    }

    fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        self.seen.lock().unwrap().push(context.recent_events.len());
        ready(Ok(ChannelOutput::empty()))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
}

/// What the reader of `window` (none bound when `None`) and a
/// `BEFORE_BROADCAST` hook, when there is one, read of the room's history
/// at the last of 85 messages.
async fn last_contexts(window: Option<usize>, hook: bool) -> (Option<usize>, Option<usize>) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let reader = window.map(|window| Reader {
        window,
        seen: Arc::default(),
    });
    if let Some(reader) = &reader {
        kit.register_channel(reader.clone()).unwrap();
    }
    let hooked: Arc<Mutex<Vec<usize>>> = Arc::default();
    if hook {
        let noting = Arc::clone(&hooked);
        kit.hook(
            HookTrigger::BeforeBroadcast,
            move |_: RoomEvent, context: RoomContext| {
                noting.lock().unwrap().push(context.recent_events.len());
                async { Ok(HookResult::allow(AllowOptions::default())) }
            },
            HookOptions {
                name: "counting".to_owned(),
                ..Default::default()
            },
        )
        .unwrap();
    }
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    let bound = if reader.is_some() {
        ["sms", "reader"].as_slice()
    } else {
        ["sms"].as_slice()
    };
    for id in bound {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    for n in 0..85 {
        let text = TextContent::new(format!("m{n}"));
        let message = InboundMessage::new(channel("sms"), "+15550001", text);
        let options = ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        };
        kit.process_inbound(message, options).await.unwrap();
    }
    kit.close().await.unwrap();
    let last = |seen: &Arc<Mutex<Vec<usize>>>| seen.lock().unwrap().last().copied();
    (
        reader.as_ref().and_then(|reader| last(&reader.seen)),
        last(&hooked),
    )
}

/// RFC §10.1 step 3: the context holds the largest window a bound channel
/// declares, at least 50 while a hook is registered, and a delivery's its
/// event besides. Python: `tasks/probes/step5-lot5/py_context_window.out`.
#[tokio::test]
async fn a_room_s_context_holds_the_window_its_channels_declare() {
    // The reader's window, whether a hook is registered; what the reader
    // and the hook read, Python's counts.
    let cases = [
        (Some(3), false, (Some(4), None)),
        (Some(3), true, (Some(51), Some(50))),
        (Some(80), false, (Some(81), None)),
        (Some(80), true, (Some(81), Some(80))),
        (Some(0), true, (Some(51), Some(50))),
        (None, true, (None, Some(50))),
    ];
    for (window, hook, expected) in cases {
        assert_eq!(
            last_contexts(window, hook).await,
            expected,
            "{window:?} {hook}"
        );
    }
}
