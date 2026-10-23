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

use roomkit::model::{
    ChannelId, ChannelType, EventContent, HookResult, InboundMessage, RoomContext, RoomEvent,
    RoomId,
};
use roomkit::ops::{FrameworkEvent, InboundResult};
use roomkit::testing::RecordingTransport;
use roomkit::{HookExecution, HookSpec, HookTrigger, KitError, RoomKit, text_of};

use shared::{Checks, Log};

/// How long the scanner's round trip takes.
const SCAN: Duration = Duration::from_millis(200);
/// How long a message may take up to its decision.
const PROCESS_TIMEOUT: Duration = Duration::from_millis(300);

/// Registers the scanner, off the room lock. It notes in `scans` when each
/// scan starts and ends; the message `hang` it never answers.
fn scan_off_the_lock(kit: &RoomKit, scans: &Log<String>) -> Result<(), Box<dyn Error>> {
    let spec = HookSpec {
        needs_lock: false,
        ..HookSpec::new(
            "pii_scan",
            HookTrigger::BeforeBroadcast,
            HookExecution::Sync,
        )
    };
    let scans = scans.clone();
    kit.hook(spec, move |event: RoomEvent, _: RoomContext| {
        let (scans, text) = (scans.clone(), text_of(&event.content));
        async move {
            scans.push(format!("{text} scan starts"));
            if text == "hang" {
                std::future::pending::<()>().await;
            }
            // Stands in for the scanner's HTTP round trip.
            tokio::time::sleep(SCAN).await;
            scans.push(format!("{text} scan ends"));
            Ok(HookResult::allow())
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
    async fn says(self, text: &'static str) -> Result<InboundResult, KitError> {
        let message = InboundMessage::new(self.user, "user", EventContent::text(text));
        self.kit.process_inbound(message, Some(&self.room)).await
    }
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let kit = shared::kit();
    let user = RecordingTransport::new(ChannelId::new("ws_user")?, ChannelType::Websocket);
    let agent = RecordingTransport::new(ChannelId::new("ws_agent")?, ChannelType::Websocket);
    kit.register_channel(user.clone())?;
    kit.register_channel(agent.clone())?;
    let scans = Log::new();
    scan_off_the_lock(&kit, &scans)?;
    let room = RoomId::new("room")?;
    kit.create_room(&room).await?;
    kit.attach(&room, user.id()).await?;
    kit.attach(&room, agent.id()).await?;
    let chat = Chat {
        kit: kit.clone(),
        room: room.clone(),
        user: user.id().clone(),
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
    checks.expect(
        "the messages commit and are delivered in the order they arrived",
        first.event.map(|event| event.index) == Some(0)
            && second.event.map(|event| event.index) == Some(1)
            && agent.texts() == ["first", "second"],
    );

    println!("A message the scanner never answers, with a {PROCESS_TIMEOUT:?} process_timeout.");
    kit.set_process_timeout(Some(PROCESS_TIMEOUT));
    let mut events = kit.subscribe();
    let hung = chat.clone().says("hang").await?;
    let reported = shared::framework_events(&mut events);
    let written = shared::timeline(&kit, &room).await?.len();
    let next = chat.says("after").await?;
    println!("  hang: blocked {}, reason {:?}", hung.blocked, hung.reason);
    checks.expect(
        "the hung message is refused process_timeout, with nothing written",
        hung.blocked
            && hung.reason.as_deref() == Some("process_timeout")
            && hung.event.is_none()
            && written == 2,
    );
    checks.expect(
        "a process_timeout framework event reports it",
        reported
            .iter()
            .any(|event| matches!(event, FrameworkEvent::ProcessTimeout { .. })),
    );
    checks.expect(
        "the room is free: the next message is delivered",
        next.event.is_some() && agent.texts().last().map(String::as_str) == Some("after"),
    );
    Ok(checks.finish())
}
