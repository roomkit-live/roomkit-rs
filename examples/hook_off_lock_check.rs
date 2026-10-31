//! A slow content check run off the room lock, and the processing timeout
//! that bounds it (RFC §9.5.1, §13.6, §8.2). A `BEFORE_BROADCAST` scanner
//! that calls a remote service would hold the room lock for its whole round
//! trip, and the room's messages would queue behind each other's scans.
//! Registered with `needs_lock = false`, it runs before the lock is taken:
//! the scans of two messages overlap, and a per-room admission ticket keeps
//! the messages in the order they arrived. A scanner that never answers is
//! bounded by the kit's `process_timeout`: the message is refused with
//! nothing written, and the room is free for the next one.
//!
//! ```sh
//! cargo run --example hook_off_lock_check
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]
use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

use roomkit::model::hook::AllowOptions;
use roomkit::model::{ChannelId, ChannelType, HookResult, InboundMessage, RoomId, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, HookExecution, HookSpec, HookTrigger, InboundResult,
    ProcessInboundOptions, RoomKit, RoomKitError, RoomKitOptions,
};

use shared::text_of;
use shared::{Checks, Log};

/// How long the scanner's round trip takes.
const SCAN: Duration = Duration::from_millis(200);
/// How long a message may take up to its decision: a scan's round trip
/// and some.
const PROCESS_TIMEOUT: Duration = Duration::from_secs(1);

/// Registers the scanner, off the room lock. It notes in `scans` when each
/// scan starts and ends; the message `hang` it never answers.
fn scan_off_the_lock(kit: &RoomKit, scans: &Log<String>) -> Result<(), Box<dyn Error>> {
    let mut spec = HookSpec::new(
        "pii_scan",
        HookTrigger::BeforeBroadcast,
        HookExecution::Sync,
    );
    spec.needs_lock = false;
    let scans = scans.clone();
    kit.hook(spec, move |event, _| {
        let (scans, text) = (scans.clone(), text_of(&event.content));
        async move {
            scans.push(format!("{text} scan starts"));
            if text == "hang" {
                std::future::pending::<()>().await;
            }
            // Stands in for the scanner's HTTP round trip.
            tokio::time::sleep(SCAN).await;
            scans.push(format!("{text} scan ends"));
            Ok(HookResult::allow(AllowOptions::default()))
        }
    })?;
    Ok(())
}

/// The room the user writes in. Cloning it is cheap: a message can be sent
/// from a task of its own while the example sends another.
#[derive(Clone)]
struct Chat {
    kit: RoomKit,
    room: RoomId,
    user: ChannelId,
}

impl Chat {
    async fn says(self, text: &'static str) -> Result<InboundResult, RoomKitError> {
        let message = InboundMessage::new(self.user, "user", TextContent::new(text));
        self.kit
            .process_inbound(
                message,
                ProcessInboundOptions {
                    room_id: Some(self.room.clone()),
                    ..Default::default()
                },
            )
            .await
    }
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = shared::kit_with(RoomKitOptions {
        process_timeout: PROCESS_TIMEOUT,
        ..Default::default()
    });
    let user = RecordingTransport::new(
        ChannelId::new("ws_user")?,
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    let agent = RecordingTransport::new(
        ChannelId::new("ws_agent")?,
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(user.clone())?;
    kit.register_channel(agent.clone())?;
    let scans = Log::new();
    scan_off_the_lock(&kit, &scans)?;
    let room = RoomId::new("room")?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    kit.attach_channel(&room, user.channel_id(), AttachChannelOptions::default())
        .await?;
    kit.attach_channel(&room, agent.channel_id(), AttachChannelOptions::default())
        .await?;
    let chat = Chat {
        kit: kit.clone(),
        room: room.clone(),
        user: user.channel_id().clone(),
    };
    let mut checks = Checks::default();

    println!("Two messages, the second sent while the first is being scanned.");
    let first = tokio::spawn(chat.clone().says("first"));
    scans.wait_for(1).await;
    let second = chat.clone().says("second").await?;
    let first = first.await??;
    println!("  Scans: {:?}", scans.entries());
    println!("  Delivered to the agent: {:?}", agent.texts());
    checks.expect(
        "the second scan starts while the first still runs: the checks overlap",
        scans.entries().get(..2)
            == Some(
                &[
                    "first scan starts".to_owned(),
                    "second scan starts".to_owned(),
                ][..],
            ),
    );
    // After the two attaches' lifecycle records (RFC §5.1, gap G-046).
    checks.expect(
        "the messages commit and are delivered in the order they arrived",
        first.event.map(|event| event.index) == Some(2)
            && second.event.map(|event| event.index) == Some(3)
            && agent.texts() == ["first", "second"],
    );

    println!("A message the scanner never answers, with a {PROCESS_TIMEOUT:?} process_timeout.");
    let events = shared::framework_events(&kit, &["process_timeout"]);
    let hung = chat.clone().says("hang").await?;
    let reported = events.entries();
    let written = shared::timeline(&kit, &room).await?.len();
    let next = chat.says("after").await?;
    println!("  hang: blocked {}, reason {:?}", hung.blocked, hung.reason);
    checks.expect(
        "the hung message is refused process_timeout, with nothing written",
        hung.blocked
            && hung.reason.as_deref() == Some("process_timeout")
            && hung.event.is_none()
            && written == 4,
    );
    checks.expect(
        "a process_timeout framework event reports it",
        !reported.is_empty(),
    );
    checks.expect(
        "the room is free: the next message is delivered",
        next.event.is_some() && agent.texts().last().map(String::as_str) == Some("after"),
    );
    Ok(checks.finish())
}
