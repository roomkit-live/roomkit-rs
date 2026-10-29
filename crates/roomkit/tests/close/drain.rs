//! RFC §10.6.8, §12.10.4 (RMK-1017): the turns of the close's drain. The
//! streams read as the close starts are cut at once, and those whose read
//! begins while it waits for them are cut in turn; a turn asked as the
//! lanes drain runs within their grace; once the lanes are idle, the
//! turns still running are cut then, what they said read and stored, as
//! the Python implementation's `turns.close` follows its `lanes.aclose`.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::model::{ChannelBinding, ChannelOutput, ChannelType, HookResult, RoomContext, RoomId};
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, ConversationStore,
    CreateRoomOptions, DeliveryStream, EventQuery, HookExecution, HookSpec, HookTrigger, RoomKit,
    RoomKitOptions, StreamItem, text_of,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiProvider, AiRequest, Done, ProviderError, ProviderEvent, ProviderStream, ToolDefinition,
    ToolOutput, Tools,
};
use serde_json::json;
use tokio::time::{Instant, Sleep};

use crate::store::SlowCommits;
use crate::support::{
    Answered, Call, Witness, attach_agent, channel, ended, kit_on, kit_with, ok, room_id,
    spawn_message, stored, stored_in, transport,
};

/// RFC §10.6.8, §12.10.4: the close cuts at once only the turns whose
/// streams are read as it starts; a turn asked as the lanes drain runs,
/// within their grace, as Python's, whose turn backend takes turns until
/// its lanes are done. The witness takes 1 s to deliver "One.", the agent
/// asked after it, answering "A."; in the second row "Two." comes at 50
/// ms, its delivery set queued behind One's, the agent answering "B." to
/// it; the kit is closed from outside at 0.1 s. Each turn runs as the
/// lanes drain, its answer stored, and the answer's delivery set, which
/// would take its place in the lane once the close began, is dropped, so
/// each call reads `kit_closed`, as Python's (`rmk1017-fix3/py_p9.out` P9,
/// P9b). On 94c751f the turns were refused: the model was never asked, and
/// Two's call carried `TurnLinkError`.
#[tokio::test(start_paused = true)]
async fn a_turn_asked_as_the_lanes_drain_runs_within_their_grace() {
    let mut rows = Vec::new();
    for queued in [false, true] {
        let witness = Witness::slow("One.", Duration::from_secs(1));
        let kit = kit_with(&witness).await;
        let model = ScriptedProvider::new([
            ScriptedRound::answering("A."),
            ScriptedRound::answering("B."),
        ]);
        let asked = model.clone();
        attach_agent(&kit, model, Tools::new()).await;
        let one = spawn_message(&kit, "One.", room_id());
        tokio::time::sleep(Duration::from_millis(50)).await;
        let two = queued.then(|| spawn_message(&kit, "Two.", room_id()));
        tokio::time::sleep(Duration::from_millis(50)).await;
        let started = Instant::now();
        tokio::time::timeout(Duration::from_secs(60), kit.close())
            .await
            .unwrap()
            .unwrap();
        let took = started.elapsed().as_millis() / 100;
        let one = ended(one).await;
        let two = match two {
            Some(two) => ended(two).await,
            None => None,
        };
        rows.push((
            queued,
            took,
            (one, two),
            asked.requests().len(),
            stored(&kit).await,
            witness.delivered(),
        ));
    }
    let kit_closed = || Some(ok(Some("kit_closed"), None));
    let texts = |texts: &[&str]| {
        texts
            .iter()
            .map(|&text| text.to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        rows,
        [
            (
                false,
                9,
                (kit_closed(), None),
                1,
                texts(&["One.", "A."]),
                texts(&["One."])
            ),
            (
                true,
                9,
                (kit_closed(), kit_closed()),
                2,
                texts(&["One.", "Two.", "A.", "B."]),
                texts(&["One.", "Two."])
            ),
        ]
    );
}

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

/// What the turn the drain leaves does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Left {
    /// It writes "Think", then waits 30 s.
    Stream,
    /// It writes 100 deltas at once.
    Deltas,
    /// It writes "Thinking", then calls a tool of 30 s.
    Tool,
    /// It says nothing for 30 s.
    Output,
}

/// The agent whose turn does what `left` says, attached to `room_1`.
pub(crate) async fn attach_left<S: ConversationStore>(kit: &RoomKit<S>, left: Left) {
    let first = match left {
        Left::Stream => Some("Think"),
        Left::Output => None,
        Left::Deltas | Left::Tool => return attach_scripted(kit, left).await,
    };
    attach_agent(kit, Pausing { first }, Tools::new()).await;
}

/// The scripted agent whose turn writes deltas or calls a slow tool.
async fn attach_scripted<S: ConversationStore>(kit: &RoomKit<S>, left: Left) {
    match left {
        Left::Stream | Left::Output => {}
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

/// A call's result, as `(cancellation_reason, "type: message")` of its
/// error; `None` for a call that failed or did not end.
pub(crate) async fn told(call: Call) -> Option<(Option<String>, Option<String>)> {
    let result = tokio::time::timeout(Duration::from_secs(60), call)
        .await
        .ok()?
        .unwrap()
        .ok()?;
    let error = result
        .error
        .map(|error| format!("{}: {}", error.error_type, error.message));
    Some((result.cancellation_reason, error))
}

pub(crate) fn texts(texts: &[&str]) -> Vec<String> {
    texts.iter().map(|&text| text.to_owned()).collect()
}

/// RFC §10.6.8 (RMK-1017, light review 3): the turns still running once
/// the lanes are idle are cut then, not at the lanes' 5 s grace, as
/// Python's `turns.close` follows its `lanes.aclose`. The witness takes 1
/// s to deliver "One."; the agent, asked after it as the lanes drain,
/// writes "Think" then waits 30 s, writes "Thinking" and calls a tool of
/// 30 s, or says nothing for 30 s; the kit is closed from outside at 0.1
/// s. The close returns at 1 s, as Python's. What the turn said is
/// stored, its delivery dropped, the call reading `kit_closed`, as
/// Python's (`rmk1017-lightreview3/lr3_probe_py.out` N1); a turn that
/// said nothing, still open as the kit closes, fails the turn link's
/// `backend_closed` (RFC §10.6.8, decided 2026-10-28; on e569601 it
/// read `executor_lost`, as Python's still does). On 181eaa5 the close
/// returned at 5.1 s, nothing of the turn stored but the tool's rows, and
/// the call read no reason.
#[tokio::test(start_paused = true)]
async fn the_turns_left_once_the_lanes_are_idle_are_cut_then() {
    let mut rows = Vec::new();
    for left in [Left::Stream, Left::Tool, Left::Output] {
        let witness = Witness::slow("One.", Duration::from_secs(1));
        let kit = kit_with(&witness).await;
        attach_left(&kit, left).await;
        let call = spawn_message(&kit, "One.", room_id());
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = Instant::now();
        tokio::time::timeout(Duration::from_secs(60), kit.close())
            .await
            .unwrap()
            .unwrap();
        let took = started.elapsed().as_millis() / 100;
        rows.push((left, took, told(call).await, stored(&kit).await));
    }
    let kit_closed = Some((Some("kit_closed".to_owned()), None));
    let backend_closed = Some((
        None,
        Some("TurnLinkError: backend_closed: the turn backend closed".to_owned()),
    ));
    assert_eq!(
        rows,
        [
            (
                Left::Stream,
                9,
                kit_closed.clone(),
                texts(&["One.", "Think"])
            ),
            (
                Left::Tool,
                9,
                kit_closed,
                texts(&["One.", "Thinking", "[Tool call: slow]", "[Tool call: slow]"])
            ),
            (Left::Output, 9, backend_closed, texts(&["One."])),
        ]
    );
}

/// RFC §10.6.8 (RMK-1017, light review 3): a stream whose read begins
/// while the close waits for the streams it cut is cut in turn, as
/// Python's close cuts its stream readers again until none is left.
/// `room_1`: "Go." asks the agent, which writes "Think" then waits, the
/// commit of "Think" taking 3 s. `room_2`: "Two." comes at once, the
/// witness taking 1 s to deliver it, then the agent is asked there, its
/// stream read from 1 s on. The kit is closed from outside at 0.1 s: it
/// cuts `room_1`'s stream, waits 3 s for its reader, then cuts `room_2`'s,
/// both answers delivered as the lanes drain, and returns at 3.1 s, as
/// Python's at 3.0 s (`rmk1017-fix4/py_n5.out`). On 181eaa5 the close
/// returned at 8.1 s, `room_2`'s answer cut at the grace and never
/// delivered.
#[tokio::test(start_paused = true)]
async fn a_stream_read_while_the_close_waits_for_a_cut_one_is_cut_in_turn() {
    let store = SlowCommits::delaying("Think", room_id(), Duration::from_secs(3));
    let witness = Witness::slow("Two.", Duration::from_secs(1));
    let kit = kit_on(RoomKit::new(RoomKitOptions::with_store(store)), &witness).await;
    let room_2 = RoomId::new("room_2").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_2.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    attach_agent(
        &kit,
        Pausing {
            first: Some("Think"),
        },
        Tools::new(),
    )
    .await;
    for id in ["sms", "witness", "agent"] {
        kit.attach_channel(&room_2, &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let go = spawn_message(&kit, "Go.", room_id());
    let two = spawn_message(&kit, "Two.", room_2.clone());
    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = Instant::now();
    tokio::time::timeout(Duration::from_secs(60), kit.close())
        .await
        .unwrap()
        .unwrap();
    let took = started.elapsed().as_millis() / 100;
    assert_eq!(
        (
            took,
            (ended(go).await, ended(two).await),
            (stored(&kit).await, stored_in(&kit, &room_2).await),
            witness.delivered()
        ),
        (
            30,
            (Some(ok(None, None)), Some(ok(None, None))),
            (texts(&["Go.", "Think"]), texts(&["Two.", "Think"])),
            texts(&["Go.", "Two.", "Think", "Think"])
        )
    );
}

/// RFC §12.10.4 (RMK-1017, light review 3, N2): a turn asked as the lanes
/// drain answers "A.", whose commit takes 3 s or 6 s: the drain waits for
/// that commit within its grace, the answer's delivery dropped, the call
/// reading `kit_closed`; past the grace the run is aborted while the
/// commit runs, the answer stored all the same and never delivered, and
/// the call reads `kit_closed` too, as Python's, whose close waits for the
/// commit (`rmk1017-lightreview3/lr3_probe_py.out` N2). A commit of 12 s
/// outlives the close's bound: the close returns at 10 s, the answer
/// stored once the write ends, after it, where Python's close waits for it
/// without bound. On 181eaa5 the 6 s row read no reason; with the cut past
/// the grace waiting again for the turn cut once the lanes were idle, the
/// 12 s row's close held until the write ended
/// (`rmk1017-lightreview5/x_cut_again.out`).
#[tokio::test(start_paused = true)]
async fn an_answer_committed_past_the_grace_reads_as_dropped() {
    let mut rows = Vec::new();
    for commit in [3, 6, 12] {
        let store = SlowCommits::delaying("A.", room_id(), Duration::from_secs(commit));
        let witness = Witness::slow("One.", Duration::from_secs(1));
        let kit = kit_on(RoomKit::new(RoomKitOptions::with_store(store)), &witness).await;
        let model = ScriptedProvider::new([ScriptedRound::answering("A.")]);
        attach_agent(&kit, model, Tools::new()).await;
        let one = spawn_message(&kit, "One.", room_id());
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = Instant::now();
        tokio::time::timeout(Duration::from_secs(60), kit.close())
            .await
            .unwrap()
            .unwrap();
        let took = started.elapsed().as_millis() / 100;
        let at_the_return = stored(&kit).await;
        rows.push((
            commit,
            took,
            at_the_return,
            ended(one).await,
            stored(&kit).await,
        ));
    }
    let row = |commit, took, at_the_return: &[&str]| {
        (
            commit,
            took,
            texts(at_the_return),
            Some(ok(Some("kit_closed"), None)),
            texts(&["One.", "A."]),
        )
    };
    assert_eq!(
        rows,
        [
            row(3, 39, &["One.", "A."]),
            row(6, 69, &["One.", "A."]),
            row(12, 100, &["One."])
        ]
    );
}

/// RFC §12.10.4 (RMK-1017, light review 4, nit 3): N2's shape, a SYNC hook
/// blocking "A.", whose commit, stored `BLOCKED`, takes 0 s or 6 s: an
/// event stored `BLOCKED` is never delivered, so a run aborted past the
/// grace while that commit runs reads no reason, as Python's
/// (`rmk1017-fix5/py_n2_blocked.out`). On 463c245 the 6 s row read
/// `kit_closed`.
#[tokio::test(start_paused = true)]
async fn a_blocked_answer_committed_past_the_grace_reads_no_reason() {
    let mut rows = Vec::new();
    for commit in [0, 6] {
        let store = SlowCommits::delaying("A.", room_id(), Duration::from_secs(commit));
        let witness = Witness::slow("One.", Duration::from_secs(1));
        let kit = kit_on(RoomKit::new(RoomKitOptions::with_store(store)), &witness).await;
        let spec = HookSpec::new("block", HookTrigger::BeforeBroadcast, HookExecution::Sync);
        kit.hook(spec, |event, _context| {
            let is_answer = text_of(&event.content) == "A.";
            async move {
                Ok(if is_answer {
                    HookResult::block("no")
                } else {
                    HookResult::allow()
                })
            }
        })
        .unwrap();
        let model = ScriptedProvider::new([ScriptedRound::answering("A.")]);
        attach_agent(&kit, model, Tools::new()).await;
        let one = spawn_message(&kit, "One.", room_id());
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = Instant::now();
        tokio::time::timeout(Duration::from_secs(60), kit.close())
            .await
            .unwrap()
            .unwrap();
        let took = started.elapsed().as_millis() / 100;
        rows.push((commit, took, ended(one).await, stored(&kit).await));
    }
    let row = |commit, took| (commit, took, Some(ok(None, None)), texts(&["One.", "A."]));
    assert_eq!(rows, [row(0, 9), row(6, 69)]);
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

/// N6, N1's shape: the witness takes `before` to deliver "One.", the
/// agent asked after it, its answer streamed to a target that reads its
/// first item, then stalls 30 s; the kit is closed from outside at 0.1 s.
/// What the close took, in tenths of a second, the call, and the answers
/// stored, for each of what the turn does.
async fn stalled(before: Duration) -> Vec<Stalled> {
    let mut rows = Vec::new();
    for left in [Left::Stream, Left::Deltas, Left::Tool] {
        let witness = Witness::slow("One.", before);
        let kit = kit_with(&witness).await;
        kit.register_channel(Receiving::new(Pace::Stall)).unwrap();
        kit.attach_channel(&room_id(), &channel("ws"), AttachChannelOptions::default())
            .await
            .unwrap();
        attach_left(&kit, left).await;
        let call = spawn_message(&kit, "One.", room_id());
        tokio::time::sleep(Duration::from_millis(100)).await;
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
/// witness taking `before` to deliver "One.", the kit closed from outside
/// at `close_at`: whether what `room_1` stores of the answer is exactly
/// what the target received, marked `cancelled`, the call reading
/// `kit_closed`.
async fn stores_what_it_received(pace: Pace, before: Duration, close_at: Duration) -> bool {
    let witness = Witness::slow("One.", before);
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
    tokio::time::sleep(close_at).await;
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
            let right =
                stores_what_it_received(pace, Duration::from_secs(1), Duration::from_millis(100))
                    .await;
            if !right {
                wrong.push((pace, run));
            }
        }
    }
    assert_eq!(wrong, []);
}

/// The same on a multi-thread runtime, 20 runs each, the witness taking
/// 50 ms, the kit closed at 20 ms; on 8a2ca47, 8 of 40 runs of N6 went
/// wrong (`lr5_threads_head.out`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_target_s_stored_text_is_what_it_received_on_threads() {
    let mut wrong = Vec::new();
    for pace in [Pace::Stall, Pace::Ms10] {
        for run in 0..20 {
            let right =
                stores_what_it_received(pace, Duration::from_millis(50), Duration::from_millis(20))
                    .await;
            if !right {
                wrong.push((pace, run));
            }
        }
    }
    assert_eq!(wrong, []);
}

/// RFC §10.6.8, §12.2 step 13s (RMK-1017, light review 4, N6): the turn
/// left once the lanes are idle streams to a target that stalls; once
/// cut, the turn's reader holds the close no longer: the target is cut
/// with it, as Python cancels its readers, and the text it was handed is
/// stored, marked `cancelled`, its delivery dropped, the call reading
/// `kit_closed`, as Python's at 0.9 s (`rmk1017-lightreview4/n6_py.out`).
/// On 463c245 the close took 21 s or 15.1 s, nothing of the turn stored,
/// the call reading no reason (`lr4_probe_n6_head.out`).
#[tokio::test(start_paused = true)]
async fn a_cut_turn_s_stalled_target_holds_the_close_no_longer() {
    assert_eq!(stalled(Duration::from_secs(1)).await, stalled_rows(9));
}

/// The same, on a multi-thread runtime, the witness taking 0.1 s: the
/// close returns within a second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_turn_s_stalled_target_holds_the_close_no_longer_on_threads() {
    let rows: Vec<_> = stalled(Duration::from_millis(100))
        .await
        .into_iter()
        .map(|(left, took, call, answers)| (left, u128::from(took < 10), call, answers))
        .collect();
    assert_eq!(rows, stalled_rows(1));
}

/// RFC §12.2 step 13s (RMK-1017, light review 5, fix 1): a healthy target
/// reads on once the turn is cut: N1's tool shape, the answer streamed to
/// a target that pulls each item at once; the turn cut once the lanes are
/// idle, its reader polled once before each check, the target reads
/// "Thinking", and the tool call it opened is stored with its end, as
/// Python stores them (`rmk1017-fix6/py_n1_tool_now.out`). With the
/// drained read checking for a pull without polling its reader first, the
/// read stopped at once, "Thinking" stored alone, marked `cancelled`
/// (`rmk1017-fix6/mutation_yield.out`).
#[tokio::test(start_paused = true)]
async fn a_healthy_target_reads_on_once_its_turn_is_cut() {
    let witness = Witness::slow("One.", Duration::from_secs(1));
    let kit = kit_with(&witness).await;
    let target = Receiving::new(Pace::Now);
    kit.register_channel(target.clone()).unwrap();
    kit.attach_channel(&room_id(), &channel("ws"), AttachChannelOptions::default())
        .await
        .unwrap();
    attach_left(&kit, Left::Tool).await;
    let call = spawn_message(&kit, "One.", room_id());
    tokio::time::sleep(Duration::from_millis(100)).await;
    kit.close().await.unwrap();
    let call = ended(call).await.unwrap();
    let row = |text: &str| (text.to_owned(), false);
    assert_eq!(
        (call, target.received(), answers(&kit).await),
        (
            ok(Some("kit_closed"), None),
            "Thinking".to_owned(),
            vec![
                row("Thinking"),
                row("[Tool call: slow]"),
                row("[Tool call: slow]")
            ]
        )
    );
}

/// N1's shape, the turn doing what `left` says, its answer streamed to a
/// target that pulls each item at once, the witness taking `before` to
/// deliver "One.", the kit closed from outside at `close_at`: whether the
/// target read its stream's end, and whether it was dropped before it
/// ended.
async fn ended_or_dropped(left: Left, before: Duration, close_at: Duration) -> (bool, bool) {
    let witness = Witness::slow("One.", before);
    let kit = kit_with(&witness).await;
    let target = Receiving::new(Pace::Now);
    kit.register_channel(target.clone()).unwrap();
    kit.attach_channel(&room_id(), &channel("ws"), AttachChannelOptions::default())
        .await
        .unwrap();
    attach_left(&kit, left).await;
    let call = spawn_message(&kit, "One.", room_id());
    tokio::time::sleep(close_at).await;
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
/// closed at 0.1 s. On c4cc718 the target's last poll ran once the
/// producer's pulls were gone: it read `None`, ran its end and returned
/// (`rmk1017-lightreview6/lr6b_head.out`).
#[tokio::test(start_paused = true)]
async fn a_cut_target_is_dropped_where_it_waits() {
    let mut rows = Vec::new();
    for left in [Left::Stream, Left::Tool] {
        let row = ended_or_dropped(left, Duration::from_secs(1), Duration::from_millis(100)).await;
        rows.push((left, row));
    }
    assert_eq!(
        rows,
        [(Left::Stream, (false, true)), (Left::Tool, (false, true))]
    );
}

/// The same on a multi-thread runtime, 10 runs of each shape, the witness
/// taking 50 ms, the kit closed at 20 ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_target_is_dropped_where_it_waits_on_threads() {
    let mut wrong = Vec::new();
    for left in [Left::Stream, Left::Tool] {
        for run in 0..10 {
            let row =
                ended_or_dropped(left, Duration::from_millis(50), Duration::from_millis(20)).await;
            if row != (false, true) {
                wrong.push((left, run, row));
            }
        }
    }
    assert_eq!(wrong, []);
}
