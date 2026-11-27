//! How long the kit's close takes against its bound (RFC §12.10.4,
//! RK-12.10.4-056), on the same cases as the Python implementation's
//! `python -m benchmarks.media close`.
//!
//! The close takes the grace, plus the turn backend's budget, plus the
//! channels' and the framework's other backends' closes, each on its own
//! budget, plus at most two unwinds of 1 second, which only work that ignores
//! its cancellation spends. Each sample builds a fresh kit with a short
//! `close_grace` on a fresh runtime, starts one piece of work the close has to
//! cut, and times `kit.close()` on the real clock:
//!
//! - `hook`: a synchronous `BeforeBroadcast` hook that waits;
//! - `tool`: a model turn's tool call that waits.
//!
//! The work either honours its cancellation (`-honours`: it awaits, so the
//! cut drops it), or ignores it until the sample releases it once the close
//! returned (`-ignores`: it blocks its thread, the one way a future does not
//! take its cut).

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelId, ChannelType, HookResult, InboundMessage, RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, HookOptions, HookTrigger, ProcessInboundOptions,
    RoomKit, RoomKitOptions,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, AiTool, ToolHandler, ToolResult};
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::machine::Samples;

/// The kit's `close_grace` in every sample.
pub(crate) const GRACE: Duration = Duration::from_millis(50);
/// The RFC's unwind, which the close gives cut work twice at most.
pub(crate) const UNWIND: Duration = Duration::from_secs(1);
/// The turn backend in the owner's process is given one unwind.
const TURN_BACKEND: Duration = UNWIND;
/// An agent's close waits for its turns on a budget of its own.
const AGENT_CLOSE: Duration = Duration::from_secs(5);
/// The sum bounds the close's waits: its own work between them (releasing
/// the store, closing a channel that waits for nothing) is allowed this much
/// on top (decided 2026-11-27).
pub(crate) const OWN_WORK: Duration = Duration::from_millis(50);
/// How long a sample lets the work its close abandoned end, once released.
const SETTLE: Duration = Duration::from_millis(200);
/// How long a sample waits for its work to start.
const START_BOUND: Duration = Duration::from_secs(5);

/// The cases, in the order a pass runs them, and the samples each takes.
pub(crate) const CASES: [(&str, usize); 4] = [
    ("hook-honours", 20),
    ("hook-ignores", 4),
    ("tool-honours", 20),
    ("tool-ignores", 2),
];

/// The RFC's sum for `case`: what its close may take at most.
pub(crate) fn bound(case: &str) -> Duration {
    let channel_closes = if case.starts_with("tool") {
        AGENT_CLOSE
    } else {
        Duration::ZERO
    };
    GRACE + TURN_BACKEND + channel_closes + UNWIND * 2
}

/// A wait the close cuts, honoured or ignored until [`Work::release`].
#[derive(Debug)]
struct Work {
    ignores: bool,
    started: Notify,
    released: AtomicBool,
    released_notify: Notify,
}

impl Work {
    fn new(ignores: bool) -> Arc<Self> {
        Arc::new(Self {
            ignores,
            started: Notify::new(),
            released: AtomicBool::new(false),
            released_notify: Notify::new(),
        })
    }

    async fn run(&self) {
        self.started.notify_one();
        if self.ignores {
            // Blocks its thread until released: an abort waits for its next
            // poll, which comes once the close is over.
            tokio::task::block_in_place(|| {
                while !self.released.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(1));
                }
            });
            return;
        }
        let released = self.released_notify.notified();
        if !self.released.load(Ordering::Acquire) {
            released.await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.released_notify.notify_waiters();
    }
}

fn id(name: &str) -> Result<ChannelId, Box<dyn Error>> {
    Ok(ChannelId::new(name)?)
}

fn hook(kit: &RoomKit, work: &Arc<Work>) -> Result<(), Box<dyn Error>> {
    let work = Arc::clone(work);
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |_event: RoomEvent, _context| {
            let work = Arc::clone(&work);
            async move {
                work.run().await;
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        HookOptions {
            name: "held".to_owned(),
            ..Default::default()
        },
    )?;
    Ok(())
}

async fn tool(kit: &RoomKit, room: &RoomId, work: &Arc<Work>) -> Result<(), Box<dyn Error>> {
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "lookup", "{}")]),
        ScriptedRound::answering("Found it."),
    ]);
    let work = Arc::clone(work);
    let handler = ToolHandler::new(move |_, _| {
        let work = Arc::clone(&work);
        async move {
            work.run().await;
            Ok(ToolResult::Text("found".into()))
        }
    });
    let options = AiChannelOptions {
        tools: vec![AiTool::new("lookup", "Look up.")?.into()],
        tool_handler: Some(handler),
        ..AiChannelOptions::default()
    };
    kit.register_channel(AiChannel::new(id("agent")?, model, options)?)?;
    kit.attach_channel(room, &id("agent")?, AttachChannelOptions::default())
        .await?;
    Ok(())
}

/// One close of a kit running `case`'s work: how long it took.
async fn sample(case: &str) -> Result<Duration, Box<dyn Error>> {
    let work = Work::new(case.ends_with("-ignores"));
    let kit = RoomKit::new(RoomKitOptions {
        close_grace: GRACE,
        ..Default::default()
    });
    kit.register_channel(RecordingTransport::new(
        id("sms")?,
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))?;
    let room = RoomId::new("r")?;
    let options = CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    };
    kit.create_room(options).await?;
    kit.attach_channel(&room, &id("sms")?, AttachChannelOptions::default())
        .await?;
    if case.starts_with("hook") {
        hook(&kit, &work)?;
    } else {
        tool(&kit, &room, &work).await?;
    }
    let message = InboundMessage::new(id("sms")?, "u1", TextContent::new("Go."));
    let into = ProcessInboundOptions {
        room_id: Some(room),
        ..Default::default()
    };
    let call = {
        let kit = kit.clone();
        tokio::spawn(async move { kit.process_inbound(message, into).await })
    };
    let took = async {
        tokio::time::timeout(START_BOUND, work.started.notified()).await?;
        let began = Instant::now();
        kit.close().await?;
        Ok::<_, Box<dyn Error>>(began.elapsed())
    }
    .await;
    work.release();
    drop(call.await);
    kit.close().await?;
    if work.ignores {
        // The work the close abandoned runs on once released: let it end
        // before its runtime does, so that it finds its timers still there.
        tokio::time::sleep(SETTLE).await;
    }
    took
}

/// One pass: every case's samples, each on a runtime of its own.
pub(crate) fn pass() -> Result<Samples, Box<dyn Error>> {
    let mut out = Vec::new();
    for (case, samples) in CASES {
        let mut seconds = Vec::with_capacity(samples);
        for _ in 0..samples {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()?;
            seconds.push(runtime.block_on(sample(case))?.as_secs_f64());
        }
        out.push((case.to_owned(), seconds));
    }
    Ok(out)
}
