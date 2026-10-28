//! RFC §10.6.8, §12.10.4 (RMK-1017): the turns of the close's drain. The
//! streams read as the close starts are cut at once, and those whose read
//! begins while it waits for them are cut in turn; a turn asked as the
//! lanes drain runs within their grace; once the lanes are idle, the
//! turns still running are cut then, what they said read and stored, as
//! the Python implementation's `turns.close` follows its `lanes.aclose`.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::model::{ChannelBinding, ChannelOutput, ChannelType, HookResult, RoomContext, RoomId};
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, ConversationStore,
    CreateRoomOptions, DeliveryStream, EventQuery, HookExecution, HookSpec, HookTrigger, RoomKit,
    RoomKitOptions, text_of,
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
enum Left {
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
async fn attach_left(kit: &RoomKit, left: Left) {
    let first = match left {
        Left::Stream => Some("Think"),
        Left::Output => None,
        Left::Deltas | Left::Tool => return attach_scripted(kit, left).await,
    };
    attach_agent(kit, Pausing { first }, Tools::new()).await;
}

/// The scripted agent whose turn writes deltas or calls a slow tool.
async fn attach_scripted(kit: &RoomKit, left: Left) {
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
async fn told(call: Call) -> Option<(Option<String>, Option<String>)> {
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

fn texts(texts: &[&str]) -> Vec<String> {
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
/// commit (`rmk1017-lightreview3/lr3_probe_py.out` N2). On 181eaa5 the
/// 6 s row read no reason.
#[tokio::test(start_paused = true)]
async fn an_answer_committed_past_the_grace_reads_as_dropped() {
    let mut rows = Vec::new();
    for commit in [3, 6] {
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
        rows.push((commit, took, ended(one).await, stored(&kit).await));
    }
    let row = |commit, took| {
        (
            commit,
            took,
            Some(ok(Some("kit_closed"), None)),
            texts(&["One.", "A."]),
        )
    };
    assert_eq!(rows, [row(3, 39), row(6, 69)]);
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

/// A streaming WebSocket target that reads the first item of a stream,
/// then stalls 30 s before it reads the rest.
#[derive(Clone)]
struct Stalling;

impl Channel for Stalling {
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
        drop(stream.next().await);
        tokio::time::sleep(Duration::from_secs(30)).await;
        while stream.next().await.is_some() {}
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
        kit.register_channel(Stalling).unwrap();
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
    vec![
        row(Left::Stream, "Think"),
        row(Left::Deltas, "w0 "),
        row(Left::Tool, "Thinking"),
    ]
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
