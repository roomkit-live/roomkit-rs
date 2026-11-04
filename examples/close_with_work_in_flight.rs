//! Close a kit while work is in flight, and read what each call got back
//! (RFC §12.10.4), as the Python implementation's
//! `examples/close_with_work_in_flight.py` does.
//!
//! `kit.close()` refuses every new call at once, but the reads; cuts what
//! has not started and every model turn at once; gives the rest of the
//! running work one grace of five seconds; then cuts what is left. Each
//! call it cut returns one result, `cancellation_reason` `kit_closed`, with
//! what it delivered.
//!
//! This example closes a kit that holds, all at once:
//!
//! - a message whose send to `slow-sms` never ends (cut after the grace,
//!   reported `failed`, code `kit_closed`, retryable), while `web` got it;
//! - a message queued behind it in the same room (dropped, never sent);
//! - an agent streaming a long answer in another room (cut at once, the
//!   text it streamed stored, marked `cancelled`);
//! - a message whose `BEFORE_BROADCAST` hook runs one second (it ends in
//!   the grace: stored, its delivery dropped), and one waiting for that
//!   room's lock (cut at once, nothing stored);
//! - a call made once the close started (refused, `KitClosed`), while the
//!   reads answer: the timelines are read through the kit after it.
//!
//! It takes about five seconds: the grace.
//!
//! ```sh
//! cargo run --example close_with_work_in_flight
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::collections::BTreeMap;
use std::error::Error;
use std::future::Future;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, DeliveryResultStatus, EventType,
    HookResult, InboundMessage, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, GetTimelineOptions,
    HookOptions, HookTrigger, InboundResult, ProcessInboundOptions, RoomKit, RoomKitError,
    SendEventOptions,
};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, ProviderError, ProviderStream,
    StreamDone, StreamEvent, StreamTextDelta,
};
use serde_json::json;
use tokio::sync::Notify;
use tokio::time::Instant;

use shared::Checks;
use shared::text_of;

/// A transport whose provider never answers: every send hangs.
#[derive(Clone, Default)]
struct StuckSms {
    sending: Arc<Notify>,
}

impl Channel for StuckSms {
    fn channel_id(&self) -> ChannelId {
        id("slow-sms")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let sending = Arc::clone(&self.sending);
        async move {
            sending.notify_one();
            std::future::pending().await
        }
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
}

/// A model streaming a long answer, a word every 50 ms; `started` once its
/// second word is out.
struct LongAnswer {
    started: Arc<Notify>,
}

impl AiProvider for LongAnswer {
    fn model_name(&self) -> &'static str {
        "long-answer"
    }

    fn generate<'a>(
        &'a self,
        _context: &'a AiContext,
    ) -> impl std::future::Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        std::future::ready(Err("this model only streams".into()))
    }

    fn generate_structured_stream<'a>(&'a self, _context: &'a AiContext) -> ProviderStream<'a> {
        let words = ["a", "long", "answer", "that", "goes", "on"].repeat(100);
        let started = Arc::clone(&self.started);
        let said = stream::unfold(0_usize, move |at| {
            let (words, started) = (words.clone(), Arc::clone(&started));
            async move {
                let text = |text: String| StreamEvent::TextDelta(StreamTextDelta { text });
                let event = match at {
                    0 => text("Here is ".to_owned()),
                    at if at <= words.len() => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        started.notify_one();
                        text(format!("{} ", words.get(at - 1)?))
                    }
                    at if at == words.len() + 1 => StreamEvent::Done(StreamDone::default()),
                    _ => return None,
                };
                Some((Ok(event), at + 1))
            }
        });
        Box::pin(said)
    }
}

fn id(name: &str) -> ChannelId {
    ChannelId::new(name).unwrap_or_else(|_| unreachable!("{name} is a channel id"))
}

fn room(name: &str) -> RoomId {
    RoomId::new(name).unwrap_or_else(|_| unreachable!("{name} is a room id"))
}

fn message(text: &str) -> InboundMessage {
    InboundMessage::new(id("chat"), "user", TextContent::new(text))
}

fn into(room_id: &str, defer_delivery: bool) -> ProcessInboundOptions {
    ProcessInboundOptions {
        room_id: Some(room(room_id)),
        defer_delivery,
        ..Default::default()
    }
}

/// What a call came to: its `cancellation_reason`, whether it was
/// blocked and why, and each target's delivery, with a failure's code.
fn summary(result: &InboundResult) -> String {
    let deliveries: BTreeMap<&str, (DeliveryResultStatus, Option<&str>)> = result
        .delivery_results
        .iter()
        .map(|(channel, delivered)| {
            let code = delivered.error.as_ref().map(|error| error.code.as_str());
            (channel.as_str(), (delivered.status, code))
        })
        .collect();
    format!(
        "cancellation_reason={:?} blocked={} reason={:?} delivery_results={deliveries:?}",
        result.cancellation_reason, result.blocked, result.reason
    )
}

/// The kit: a sender `chat`, a fast transport `web` and a stuck one in
/// `sends`, the agent in `agent-room`, and a hook of 1 s in `hooked`.
async fn kit(
    stuck: &StuckSms,
    started: &Arc<Notify>,
    in_hook: &Arc<Notify>,
) -> Result<RoomKit, Box<dyn Error>> {
    let kit = shared::kit();
    kit.register_channel(RecordingTransport::new(
        id("chat"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))?;
    kit.register_channel(RecordingTransport::new(
        id("web"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))?;
    kit.register_channel(stuck.clone())?;
    let provider = LongAnswer {
        started: Arc::clone(started),
    };
    let options = AiChannelOptions::default();
    kit.register_channel(AiChannel::new(id("agent"), provider, options)?)?;
    for room_id in ["sends", "agent-room", "hooked"] {
        let options = CreateRoomOptions {
            room_id: Some(room(room_id)),
            ..Default::default()
        };
        kit.create_room(options).await?;
        kit.attach_channel(&room(room_id), &id("chat"), AttachChannelOptions::default())
            .await?;
    }
    for (room_id, channel) in [
        ("sends", "web"),
        ("sends", "slow-sms"),
        ("agent-room", "agent"),
    ] {
        kit.attach_channel(
            &room(room_id),
            &id(channel),
            AttachChannelOptions::default(),
        )
        .await?;
    }
    let in_hook = Arc::clone(in_hook);
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _context| {
            let in_hook = Arc::clone(&in_hook);
            let checked = text_of(&event.content) == "checked slowly";
            async move {
                if checked {
                    in_hook.notify_one();
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        HookOptions {
            name: "slow-check".to_owned(),
            ..Default::default()
        },
    )?;
    Ok(kit)
}

/// The messages `room_id` stores, each marked when it is `cancelled`.
async fn stored(kit: &RoomKit, room_id: &str) -> Result<Vec<String>, RoomKitError> {
    let timeline = kit
        .get_timeline(&room(room_id), GetTimelineOptions::default())
        .await?;
    Ok(timeline
        .iter()
        .filter(|event| event.r#type == EventType::Message)
        .map(|event| {
            let cancelled = event.metadata.get("cancelled") == Some(&json!(true));
            let mark = if cancelled { " (cancelled)" } else { "" };
            format!("{:?}{mark}", text_of(&event.content))
        })
        .collect())
}

/// What the calls in flight came to once the kit closed.
struct Calls {
    cut_send: InboundResult,
    queued: InboundResult,
    answer: InboundResult,
    hooked: InboundResult,
    waiting: InboundResult,
}

/// Starts the work in flight, closes the kit while it runs, and tries a
/// call once the close started: what each call came to, what that call
/// came to, and how long the close took.
async fn close_with_work_in_flight(
    kit: &RoomKit,
    stuck: &StuckSms,
    started: &Notify,
    in_hook: &Notify,
) -> Result<(Calls, Result<RoomEvent, RoomKitError>, Duration), Box<dyn Error>> {
    // A send that never ends, and a message queued behind it.
    let cut_send = kit
        .process_inbound(message("hello"), into("sends", true))
        .await?;
    stuck.sending.notified().await;
    let queued = kit
        .process_inbound(message("queued"), into("sends", true))
        .await?;
    // An agent streaming, a hook running, a call waiting for its room's lock.
    let spawn = |text: &'static str, room_id: &'static str| {
        let kit = kit.clone();
        tokio::spawn(async move {
            kit.process_inbound(message(text), into(room_id, false))
                .await
        })
    };
    let answer = spawn("Tell me", "agent-room");
    started.notified().await;
    let hooked = spawn("checked slowly", "hooked");
    in_hook.notified().await;
    let waiting = spawn("waiting", "hooked");
    tokio::time::sleep(Duration::from_millis(10)).await;

    let began = Instant::now();
    let closing = {
        let kit = kit.clone();
        tokio::spawn(async move { kit.close().await })
    };
    tokio::task::yield_now().await;
    let too_late = kit
        .send_event(
            &room("hooked"),
            &id("chat"),
            TextContent::new("too late"),
            SendEventOptions::default(),
        )
        .await;
    closing.await??;
    let took = began.elapsed();

    let (Some(cut_send), Some(queued)) = (cut_send.delivery, queued.delivery) else {
        return Err("a deferred call has no delivery handle".into());
    };
    let calls = Calls {
        cut_send: cut_send.wait().await?,
        queued: queued.wait().await?,
        answer: answer.await??,
        hooked: hooked.await??,
        waiting: waiting.await??,
    };
    Ok((calls, too_late, took))
}

fn kit_closed(result: &InboundResult) -> bool {
    result.cancellation_reason.as_deref() == Some("kit_closed")
}

/// What `channel` read of `result`'s delivery: its status, and a failure's
/// code and whether it is retryable.
fn delivered(
    result: &InboundResult,
    channel: &str,
) -> Option<(DeliveryResultStatus, Option<(String, bool)>)> {
    let delivered = result.delivery_results.get(&id(channel))?;
    let error = delivered
        .error
        .as_ref()
        .map(|error| (error.code.clone(), error.retryable));
    Some((delivered.status, error))
}

/// Checks what each call came to against the close's rules.
fn checked(
    calls: &Calls,
    rooms: &BTreeMap<&str, Vec<String>>,
    too_late: &Result<RoomEvent, RoomKitError>,
    took: Duration,
) -> ExitCode {
    let Calls {
        cut_send,
        queued,
        answer,
        hooked,
        waiting,
    } = calls;
    let mut checks = Checks::default();
    checks.expect(
        "a call once the close started is refused",
        matches!(too_late, Err(RoomKitError::KitClosed)),
    );
    checks.expect(
        "the close takes its grace, 5 s, and no more",
        took >= Duration::from_secs(5) && took < Duration::from_secs(7),
    );
    let cut = (
        DeliveryResultStatus::Failed,
        Some(("kit_closed".to_owned(), true)),
    );
    checks.expect(
        "the send that never ends is cut, reported failed, kit_closed, retryable; web got it",
        kit_closed(cut_send)
            && delivered(cut_send, "slow-sms") == Some(cut)
            && delivered(cut_send, "web") == Some((DeliveryResultStatus::Sent, None)),
    );
    checks.expect(
        "the message queued behind it is dropped, never sent",
        kit_closed(queued) && queued.delivery_results.is_empty(),
    );
    let streamed = rooms.get("agent-room").and_then(|messages| messages.last());
    checks.expect(
        "the streaming turn is cut at once, its text stored marked cancelled",
        kit_closed(answer)
            && streamed.is_some_and(|text| {
                text.starts_with("\"Here is ") && text.ends_with("(cancelled)")
            }),
    );
    checks.expect(
        "the message whose hook ran 1 s is stored, its delivery dropped",
        kit_closed(hooked) && hooked.delivery_results.is_empty() && !hooked.blocked,
    );
    checks.expect(
        "the message waiting for its lock is cut, nothing stored",
        waiting.blocked
            && waiting.reason.as_deref() == Some("kit_closed")
            && kit_closed(waiting)
            && rooms
                .get("hooked")
                .is_some_and(|messages| messages == &["\"checked slowly\""]),
    );
    checks.finish()
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let (stuck, started, in_hook) = (StuckSms::default(), Arc::default(), Arc::default());
    let kit = kit(&stuck, &started, &in_hook).await?;
    let (calls, too_late, took) =
        close_with_work_in_flight(&kit, &stuck, &started, &in_hook).await?;
    println!(
        "A call once the close started: {:?}",
        too_late.as_ref().err()
    );
    println!("close() returned after {:.1} s", took.as_secs_f64());
    println!("The send that never ends: {}", summary(&calls.cut_send));
    println!("The message queued behind it: {}", summary(&calls.queued));
    let ended = calls
        .answer
        .response_metadata
        .get("turns")
        .and_then(|turns| turns.get("agent")?.get("loop_end_reason").cloned())
        .and_then(|reason| reason.as_str().map(str::to_owned))
        .map_or_else(|| "None".to_owned(), |reason| format!("'{reason}'"));
    println!(
        "The streaming turn: {}, its turn ended {ended}",
        summary(&calls.answer)
    );
    println!("The message whose hook ran 1 s: {}", summary(&calls.hooked));
    println!(
        "The message waiting for its lock: {}",
        summary(&calls.waiting)
    );
    // The reads answer once the kit is closed: the store is still open.
    let mut rooms = BTreeMap::new();
    for room_id in ["sends", "agent-room", "hooked"] {
        let messages = stored(&kit, room_id).await?;
        println!("Stored in {room_id}: {}", messages.join(", "));
        rooms.insert(room_id, messages);
    }
    Ok(checked(&calls, &rooms, &too_late, took))
}
