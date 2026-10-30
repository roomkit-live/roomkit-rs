//! RFC §12.2 step 13s, §12.10.4 (RMK-1017, RMK-1030): the reader of a
//! turn's stream the kit's close cuts, as it cuts every turn at once: the
//! text a streaming target was handed is stored, marked `cancelled`,
//! exactly what it received; a target that stalls holds the close no
//! longer; a target cut is dropped where it waits, never reading an end
//! its stream did not have.
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::model::{ChannelBinding, ChannelOutput, ChannelType, RoomContext};
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, ConversationStore,
    DeliveryStream, EventQuery, RoomKit, StreamItem, text_of,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiProvider, AiRequest, Done, ProviderError, ProviderEvent, ProviderStream, ToolDefinition,
    ToolOutput, Tools,
};
use serde_json::json;
use tokio::time::{Instant, Sleep};

use crate::support::{
    Answered, BOUND, Witness, attach_agent, channel, ended, kit_with, ok, room_id, spawn_message,
    transport,
};

/// A model that writes `first`, if any, then says nothing for 30 s before
/// it ends its round.
struct Pausing {
    first: Option<&'static str>,
}

struct PausingStream {
    first: Option<&'static str>,
    pause: Pin<Box<Sleep>>,
    over: bool,
}

impl Stream for PausingStream {
    type Item = Result<ProviderEvent, ProviderError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(text) = self.first.take() {
            return Poll::Ready(Some(Ok(ProviderEvent::Text(text.to_owned()))));
        }
        if self.over {
            return Poll::Ready(None);
        }
        ready!(self.pause.as_mut().poll(cx));
        self.over = true;
        Poll::Ready(Some(Ok(ProviderEvent::Done(Done::default()))))
    }
}

impl AiProvider for Pausing {
    fn generate<'a>(&'a self, _request: &'a AiRequest) -> ProviderStream<'a> {
        Box::pin(PausingStream {
            first: self.first,
            pause: Box::pin(tokio::time::sleep(Duration::from_secs(30))),
            over: false,
        })
    }
}

/// What the turn the close cuts does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Left {
    /// It writes "Think", then waits 30 s.
    Stream,
    /// It writes 100 deltas at once.
    Deltas,
    /// It writes "Thinking", then calls a tool of 30 s.
    Tool,
}

/// The agent whose turn does what `left` says, attached to `room_1`.
async fn attach_left<S: ConversationStore>(kit: &RoomKit<S>, left: Left) {
    match left {
        Left::Stream => {
            let pausing = Pausing {
                first: Some("Think"),
            };
            attach_agent(kit, pausing, Tools::new()).await;
        }
        Left::Deltas | Left::Tool => attach_scripted(kit, left).await,
    }
}

/// The scripted agent whose turn writes deltas or calls a slow tool.
async fn attach_scripted<S: ConversationStore>(kit: &RoomKit<S>, left: Left) {
    match left {
        Left::Stream => {}
        Left::Deltas => {
            let words: Vec<String> = (0..100).map(|n| format!("w{n} ")).collect();
            let round = ScriptedRound::writing(words.iter().map(String::as_str));
            attach_agent(kit, ScriptedProvider::new([round]), Tools::new()).await;
        }
        Left::Tool => {
            let tools = Tools::new().tool(
                ToolDefinition::new("slow", "Slow.", json!({ "type": "object" })),
                |_call, _context| async {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    Ok(ToolOutput::Text("slow".to_owned()))
                },
            );
            let model = ScriptedProvider::new([
                ScriptedRound::calling("Thinking", [("c1", "slow", "{}")]),
                ScriptedRound::answering("Done."),
            ]);
            attach_agent(kit, model, tools).await;
        }
    }
}

/// How a [`Receiving`] target reads its stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pace {
    /// It reads the first item, then stalls 30 s before it reads the rest.
    Stall,
    /// It reads the next item 10 ms after the last one.
    Ms10,
    /// It reads the next item at once.
    Now,
}

/// A streaming WebSocket target that keeps the texts it received, reading
/// at its pace, and says whether it read its stream's end, and whether its
/// `deliver_stream()` was dropped before it ended.
#[derive(Clone)]
struct Receiving {
    pace: Pace,
    received: Arc<Mutex<Vec<String>>>,
    ended: Arc<AtomicBool>,
    dropped: Arc<AtomicBool>,
}

/// Notes a `deliver_stream()` dropped before it ended.
struct DropNote {
    dropped: Arc<AtomicBool>,
    done: bool,
}

impl Drop for DropNote {
    fn drop(&mut self) {
        if !self.done {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
}

impl Receiving {
    fn new(pace: Pace) -> Self {
        Self {
            pace,
            received: Arc::default(),
            ended: Arc::default(),
            dropped: Arc::default(),
        }
    }

    /// The texts it received, joined.
    fn received(&self) -> String {
        self.received.lock().unwrap().concat()
    }
}

impl Channel for Receiving {
    fn declaration(&self) -> ChannelDeclaration {
        let mut declaration = transport("ws");
        declaration.supports_streaming_delivery = true;
        declaration.recipient_key = None;
        declaration
    }

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let mut note = DropNote {
            dropped: Arc::clone(&self.dropped),
            done: false,
        };
        while let Some(item) = stream.next().await {
            if let Ok(StreamItem::Text(text)) = item {
                self.received.lock().unwrap().push(text);
            }
            let pause = match self.pace {
                Pace::Stall => Duration::from_secs(30),
                Pace::Ms10 => Duration::from_millis(10),
                Pace::Now => continue,
            };
            tokio::time::sleep(pause).await;
        }
        self.ended.store(true, Ordering::SeqCst);
        note.done = true;
        Ok(ChannelOutput::default())
    }
}

/// The texts `room_1` stores past its first event, each with whether it
/// is marked `cancelled`.
async fn answers(kit: &RoomKit) -> Vec<(String, bool)> {
    let query = EventQuery {
        limit: u64::MAX,
        ..EventQuery::default()
    };
    let events = kit.store().list_events(&room_id(), &query).await.unwrap();
    events
        .iter()
        .filter(|event| event.source.channel_type != ChannelType::SYSTEM)
        .skip(1)
        .map(|event| {
            let cancelled = event.metadata.get("cancelled") == Some(&json!(true));
            (text_of(&event.content), cancelled)
        })
        .collect()
}

/// Waits until `target` received something of its stream.
async fn first_received(target: &Receiving) {
    let received = async {
        while target.received.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(BOUND, received).await.unwrap();
}

/// N6, N1's shape: "One." asks the agent, its answer streamed to a target
/// that reads its first item, then stalls 30 s; the kit is closed from
/// outside once the target received that item. What the close took, in
/// tenths of a second, the call, and the answers stored, for each of what
/// the turn does.
async fn stalled() -> Vec<Stalled> {
    let mut rows = Vec::new();
    for left in [Left::Stream, Left::Deltas, Left::Tool] {
        let witness = Witness::new();
        let kit = kit_with(&witness).await;
        let target = Receiving::new(Pace::Stall);
        kit.register_channel(target.clone()).unwrap();
        kit.attach_channel(&room_id(), &channel("ws"), AttachChannelOptions::default())
            .await
            .unwrap();
        attach_left(&kit, left).await;
        let call = spawn_message(&kit, "One.", room_id());
        first_received(&target).await;
        let started = Instant::now();
        tokio::time::timeout(Duration::from_secs(60), kit.close())
            .await
            .unwrap()
            .unwrap();
        let took = started.elapsed().as_millis() / 100;
        let call = ended(call).await.unwrap();
        rows.push((left, took, call, answers(&kit).await));
    }
    rows
}

/// What a stalled target's scenario came to: what the turn did, what the
/// close took, the call, and the answers stored, each with whether it is
/// marked `cancelled`.
type Stalled = (Left, u128, Answered, Vec<(String, bool)>);

/// What the stalled target's rows read: the text it was handed stored,
/// marked `cancelled`, the call `kit_closed`.
fn stalled_rows(took: u128) -> Vec<Stalled> {
    let row = |left, handed: &str| {
        (
            left,
            took,
            ok(Some("kit_closed"), None),
            vec![(handed.to_owned(), true)],
        )
    };
    // The deltas' first burst: the 64 the first pull grants, as one item.
    let words: Vec<String> = (0..64).map(|n| format!("w{n} ")).collect();
    let burst = words.concat();
    vec![
        row(Left::Stream, "Think"),
        row(Left::Deltas, &burst),
        row(Left::Tool, "Thinking"),
    ]
}

/// N6 and N7's shapes, each run once, the target reading at `pace`, the
/// kit closed from outside once the target received its first item:
/// whether what `room_1` stores of the answer is exactly what the target
/// received, marked `cancelled`, the call reading `kit_closed`.
async fn stores_what_it_received(pace: Pace) -> bool {
    let witness = Witness::new();
    let kit = kit_with(&witness).await;
    let target = Receiving::new(pace);
    kit.register_channel(target.clone()).unwrap();
    kit.attach_channel(&room_id(), &channel("ws"), AttachChannelOptions::default())
        .await
        .unwrap();
    let left = if pace == Pace::Stall {
        Left::Stream
    } else {
        Left::Deltas
    };
    attach_left(&kit, left).await;
    let call = spawn_message(&kit, "One.", room_id());
    first_received(&target).await;
    tokio::time::timeout(Duration::from_secs(60), kit.close())
        .await
        .unwrap()
        .unwrap();
    let call = ended(call).await.unwrap();
    let received = target.received();
    !received.is_empty()
        && call == ok(Some("kit_closed"), None)
        && answers(&kit).await == [(received, true)]
}

/// RFC §12.2 step 13s (RMK-1017, light review 5, fix 1): the text a cut
/// stream's reader stores is exactly the text its target received, never
/// one handed to a target dropped before it took it. N6's stalled target
/// and N7's flood to a target reading every 10 ms, each run 40 times; on
/// 8a2ca47 the target of 10 of 40 runs never got "Think", which was stored
/// all the same (`rmk1017-lightreview5/lr5_paused_head.out` N6R).
#[tokio::test(start_paused = true)]
async fn a_cut_target_s_stored_text_is_what_it_received() {
    let mut wrong = Vec::new();
    for pace in [Pace::Stall, Pace::Ms10] {
        for run in 0..40 {
            let right = stores_what_it_received(pace).await;
            if !right {
                wrong.push((pace, run));
            }
        }
    }
    assert_eq!(wrong, []);
}

/// The same on a multi-thread runtime, 20 runs each; on 8a2ca47, 8 of 40
/// runs of N6 went wrong (`lr5_threads_head.out`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_target_s_stored_text_is_what_it_received_on_threads() {
    let mut wrong = Vec::new();
    for pace in [Pace::Stall, Pace::Ms10] {
        for run in 0..20 {
            let right = stores_what_it_received(pace).await;
            if !right {
                wrong.push((pace, run));
            }
        }
    }
    assert_eq!(wrong, []);
}

/// RFC §10.6.8, §12.2 step 13s (RMK-1017, light review 4, N6): a turn
/// streams to a target that stalls; cut as the kit closes, the turn's
/// reader holds the close no longer: the target is cut with it, as Python
/// cancels its readers, and the text it was handed is stored, marked
/// `cancelled`, the call reading `kit_closed`; the close returns at once.
/// On 463c245 the close took 21 s or 15.1 s, nothing of the turn stored,
/// the call reading no reason (`lr4_probe_n6_head.out`).
#[tokio::test(start_paused = true)]
async fn a_cut_turn_s_stalled_target_holds_the_close_no_longer() {
    assert_eq!(stalled().await, stalled_rows(0));
}

/// The same, on a multi-thread runtime: the close returns within a
/// second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_turn_s_stalled_target_holds_the_close_no_longer_on_threads() {
    let rows: Vec<_> = stalled()
        .await
        .into_iter()
        .map(|(left, took, call, answers)| (left, u128::from(took < 10), call, answers))
        .collect();
    assert_eq!(rows, stalled_rows(1));
}

/// N1's shape, the turn doing what `left` says, its answer streamed to a
/// target that pulls each item at once, the kit closed from outside once
/// the target received its first item: whether the target read its
/// stream's end, and whether it was dropped before it ended.
async fn ended_or_dropped(left: Left) -> (bool, bool) {
    let witness = Witness::new();
    let kit = kit_with(&witness).await;
    let target = Receiving::new(Pace::Now);
    kit.register_channel(target.clone()).unwrap();
    kit.attach_channel(&room_id(), &channel("ws"), AttachChannelOptions::default())
        .await
        .unwrap();
    attach_left(&kit, left).await;
    let call = spawn_message(&kit, "One.", room_id());
    first_received(&target).await;
    kit.close().await.unwrap();
    drop(ended(call).await);
    (
        target.ended.load(Ordering::SeqCst),
        target.dropped.load(Ordering::SeqCst),
    )
}

/// RFC §12.2 step 13s (RMK-1017, light review 6, fix 1): a target the
/// close cuts is dropped where it waits, as Python raises
/// `CancelledError` in it: it never reads an end its stream did not have.
/// N1's stream and tool shapes, the target pulling each item at once,
/// waiting on the next. On c4cc718 the target's last poll ran once the
/// producer's pulls were gone: it read `None`, ran its end and returned
/// (`rmk1017-lightreview6/lr6b_head.out`).
#[tokio::test(start_paused = true)]
async fn a_cut_target_is_dropped_where_it_waits() {
    let mut rows = Vec::new();
    for left in [Left::Stream, Left::Tool] {
        let row = ended_or_dropped(left).await;
        rows.push((left, row));
    }
    assert_eq!(
        rows,
        [(Left::Stream, (false, true)), (Left::Tool, (false, true))]
    );
}

/// The same on a multi-thread runtime, 10 runs of each shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_target_is_dropped_where_it_waits_on_threads() {
    let mut wrong = Vec::new();
    for left in [Left::Stream, Left::Tool] {
        for run in 0..10 {
            let row = ended_or_dropped(left).await;
            if row != (false, true) {
                wrong.push((left, run, row));
            }
        }
    }
    assert_eq!(wrong, []);
}
