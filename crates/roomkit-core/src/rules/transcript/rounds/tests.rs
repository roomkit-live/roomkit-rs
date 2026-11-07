//! The rounds an agent's stored rows replay (RFC §6.4 "The tools of earlier
//! turns"), as the reference implementation's `tool_rounds` reads them.

use pretty_assertions::assert_eq;
use serde_json::{Value, json};

use super::{ToolRound, tool_rounds};
use crate::model::{
    ChannelId, ChannelType, EventContent, EventDirection, EventId, EventSource, EventType,
    RoomEvent, RoomId, TextContent, Timestamp, ToolCallContent, ToolCallOutcome, ToolCallStatus,
};
use crate::rules::transcript::Role;

const AGENT: &str = "assistant";

/// A row of `kind` the agent wrote at `index` for the call `id` of
/// `tool`, in the turn `turn`: its `correlation_id`, and the trigger it
/// answers.
fn row(kind: EventType, index: u64, id: &str, tool: &str, turn: &str) -> RoomEvent {
    let source = EventSource {
        direction: EventDirection::Outbound,
        ..EventSource::new(ChannelId::new(AGENT).unwrap(), ChannelType::AI)
    };
    let mut event = RoomEvent::at(
        EventId::new(format!("evt_{index}")).unwrap(),
        RoomId::new("room_1").unwrap(),
        kind,
        source,
        EventContent::ToolCall(ToolCallContent::new(tool, id)),
        Timestamp::UNIX_EPOCH,
    );
    event.index = index;
    event.correlation_id = Some(turn.to_owned());
    event.responds_to = Some(EventId::new(turn).unwrap());
    event
}

/// `event`, in the turn `turn` answering the trigger `trigger`.
fn in_turn(mut event: RoomEvent, turn: &str, trigger: &str) -> RoomEvent {
    event.correlation_id = Some(turn.to_owned());
    event.responds_to = Some(EventId::new(trigger).unwrap());
    event
}

fn start(index: u64, id: &str) -> RoomEvent {
    row(EventType::ToolCallStart, index, id, "lookup", "t1")
}

/// The end row of the call `id` at `index`, `result` read, ended `outcome`.
fn end(index: u64, id: &str, result: Value, outcome: Option<ToolCallOutcome>) -> RoomEvent {
    let mut event = row(EventType::ToolCallEnd, index, id, "lookup", "t1");
    let EventContent::ToolCall(call) = &mut event.content else {
        unreachable!()
    };
    call.result = result;
    call.status = if outcome == Some(ToolCallOutcome::Failed) {
        ToolCallStatus::Failed
    } else {
        ToolCallStatus::Completed
    };
    call.outcome = outcome;
    event
}

/// A served one-call round: its start at `index`, its end right after.
fn served(index: u64, id: &str, result: &str) -> [RoomEvent; 2] {
    let ended = end(index + 1, id, json!(result), Some(ToolCallOutcome::Served));
    [start(index, id), ended]
}

fn rounds(rows: &[RoomEvent]) -> Vec<ToolRound> {
    let agent = ChannelId::new(AGENT).unwrap();
    tool_rounds(rows, &agent)
}

/// Each round's call ids.
fn ids(rounds: &[ToolRound]) -> Vec<Vec<String>> {
    rounds
        .iter()
        .map(|round| {
            round
                .messages
                .iter()
                .flat_map(|message| &message.tool_calls)
                .map(|call| call.id.clone())
                .collect()
        })
        .collect()
}

/// The results the rounds replay, in order.
fn results(rounds: &[ToolRound]) -> Vec<String> {
    rounds
        .iter()
        .flat_map(|round| &round.messages)
        .filter(|message| message.role == Role::Tool)
        .map(|message| message.text.clone())
        .collect()
}

/// The arguments the model wrote are replayed: the start row's, never the
/// end's, which a `BEFORE_TOOL_USE` hook may have rewritten with the very
/// value it kept from the model.
#[test]
fn a_round_replays_the_arguments_the_model_wrote() {
    let mut written = start(1, "c1");
    let mut ran = end(2, "c1", json!("sent"), Some(ToolCallOutcome::Served));
    for (row, to) in [(&mut written, "<EMAIL_1>"), (&mut ran, "a@b.c")] {
        let EventContent::ToolCall(call) = &mut row.content else {
            unreachable!()
        };
        call.arguments = json!({ "to": to }).as_object().unwrap().clone();
    }
    let [round] = rounds(&[written, ran]).try_into().unwrap();
    let call = round.messages[0].tool_calls[0].clone();
    assert_eq!(
        (round.messages[0].role, call.arguments),
        (
            Role::Assistant,
            json!({ "to": "<EMAIL_1>" }).as_object().unwrap().clone()
        )
    );
    assert_eq!(round.messages[1].tool_call_id.as_deref(), Some("c1"));
}

// Python: tests/test_channels/test_tool_history.py::test_every_call_the_turn_ended_is_replayed_but_a_cancelled_one
/// Whatever its outcome or tool: the model read each of them in its turn, a
/// refusal, a call nothing served and a tool of the channel's own included
/// (RMK-1370). Not a call cancelled before it answered, whose result no
/// model read.
#[test]
fn every_call_the_turn_ended_is_replayed_but_a_cancelled_one() {
    let mut rows = Vec::new();
    for (index, (id, outcome)) in [
        ("c1", Some(ToolCallOutcome::Served)),
        ("c2", Some(ToolCallOutcome::Failed)),
        ("c3", Some(ToolCallOutcome::Blocked)),
        ("c4", Some(ToolCallOutcome::Refused)),
        ("c5", Some(ToolCallOutcome::Unserved)),
        ("c6", Some(ToolCallOutcome::Cancelled)),
        ("c7", None),
    ]
    .into_iter()
    .enumerate()
    {
        let index = 2 * u64::try_from(index).unwrap() + 1;
        rows.extend([start(index, id), end(index + 1, id, json!(id), outcome)]);
    }
    rows.push(row(
        EventType::ToolCallStart,
        15,
        "c8",
        "activate_skill",
        "t1",
    ));
    rows.push(row(
        EventType::ToolCallEnd,
        16,
        "c8",
        "activate_skill",
        "t1",
    ));
    let replayed = rounds(&rows);
    assert_eq!(
        ids(&replayed).concat(),
        ["c1", "c2", "c3", "c4", "c5", "c7", "c8"]
    );
    let errors: Vec<bool> = replayed
        .iter()
        .flat_map(|round| &round.messages)
        .filter(|message| message.role == Role::Tool)
        .map(|message| message.is_error)
        .collect();
    assert_eq!(errors, [false, true, false, false, false, false, false]);
}

#[test]
fn a_call_with_no_end_row_is_not_replayed() {
    let [started, ended] = served(2, "c2", "ok");
    assert_eq!(results(&rounds(&[start(1, "c1"), started, ended])), ["ok"]);
}

// Python: tests/test_channels/test_tool_history.py::test_calls_started_together_are_one_round_and_turns_their_own
/// Calls started together are one round; an end pairs with the start of
/// the same turn, and each call is replayed under the id the model gave
/// it, as the turn sent it: an id a later turn reused too (RMK-1370).
#[test]
fn calls_started_together_are_one_round_and_turns_their_own() {
    let [third, third_end] = served(5, "c3", "c");
    let mut rows = vec![
        start(1, "c1"),
        start(2, "c2"),
        end(3, "c1", json!("a"), Some(ToolCallOutcome::Served)),
        end(4, "c2", json!("b"), Some(ToolCallOutcome::Served)),
        third,
        third_end,
    ];
    rows.extend(served(7, "c1", "d").map(|row| in_turn(row, "t2", "t2")));
    let rounds = rounds(&rows);
    assert_eq!(ids(&rounds), [vec!["c1", "c2"], vec!["c3"], vec!["c1"]]);
    let turns: Vec<Option<&str>> = rounds.iter().map(|round| round.turn.as_deref()).collect();
    assert_eq!(turns, [Some("t1"), Some("t1"), Some("t2")]);
    assert_eq!(
        rounds.iter().map(|round| round.index).collect::<Vec<_>>(),
        [1, 5, 7]
    );
    assert_eq!(results(&rounds), ["a", "b", "c", "d"]);
}

/// The start of the call `id` at `index`, asking `q`.
fn asking(index: u64, id: &str, q: &str) -> RoomEvent {
    let mut row = start(index, id);
    let EventContent::ToolCall(call) = &mut row.content else {
        unreachable!()
    };
    call.arguments = json!({ "q": q }).as_object().unwrap().clone();
    row
}

/// Each replayed call's id and what it asked, in order.
fn asked(rounds: &[ToolRound]) -> Vec<(String, Option<Value>)> {
    rounds
        .iter()
        .flat_map(|round| &round.messages)
        .flat_map(|message| &message.tool_calls)
        .map(|call| (call.id.clone(), call.arguments.get("q").cloned()))
        .collect()
}

// Python: tests/test_channels/test_tool_history.py::test_a_round_s_results_pair_to_its_calls_first_in_first_out
#[test]
fn a_round_s_results_pair_to_its_calls_first_in_first_out() {
    let rows = [
        asking(1, "c1", "first"),
        asking(2, "c1", "second"),
        end(3, "c1", json!("a"), Some(ToolCallOutcome::Served)),
        end(4, "c1", json!("b"), Some(ToolCallOutcome::Served)),
    ];
    let rounds = rounds(&rows);
    assert_eq!(
        asked(&rounds),
        [
            ("c1".to_owned(), Some(json!("first"))),
            ("c1".to_owned(), Some(json!("second")))
        ]
    );
    assert_eq!(results(&rounds), ["a", "b"]);
}

// Python: tests/test_channels/test_tool_history.py::test_a_later_round_s_result_is_not_for_a_call_left_without_its_end
#[test]
fn a_later_round_s_result_is_not_for_a_call_left_without_its_end() {
    let [other, other_end] = served(2, "c2", "other");
    let rows = [
        asking(1, "c1", "left"),
        other,
        other_end,
        asking(4, "c1", "new"),
        end(5, "c1", json!("its own"), Some(ToolCallOutcome::Served)),
    ];
    let rounds = rounds(&rows);
    assert_eq!(
        asked(&rounds),
        [
            ("c2".to_owned(), None),
            ("c1".to_owned(), Some(json!("new")))
        ]
    );
    assert_eq!(results(&rounds), ["other", "its own"]);
}

// Python: tests/test_channels/test_tool_history.py::test_a_withheld_end_row_leaves_its_call_without_a_later_call_s_result
#[test]
fn a_withheld_end_row_leaves_its_call_without_a_later_call_s_result() {
    let mut deleted = end(2, "c1", json!(null), Some(ToolCallOutcome::Served));
    deleted
        .metadata
        .insert("deleted".to_owned(), Value::Bool(true));
    let rows = [
        asking(1, "c1", "left"),
        deleted,
        asking(3, "c1", "new"),
        end(4, "c1", json!("its own"), Some(ToolCallOutcome::Served)),
    ];
    let rounds = rounds(&rows);
    assert_eq!(asked(&rounds), [("c1".to_owned(), Some(json!("new")))]);
    assert_eq!(results(&rounds), ["its own"]);
}

// Python: tests/test_channels/test_tool_history.py::test_another_turn_among_a_round_s_starts_leaves_its_results_paired
#[test]
fn another_turn_among_a_round_s_starts_leaves_its_results_paired() {
    let theirs = |row| in_turn(row, "t2", "t2");
    let rows = [
        asking(1, "c1", "first"),
        theirs(asking(2, "c9", "theirs")),
        asking(3, "c1", "second"),
        theirs(end(4, "c9", json!("zed"), Some(ToolCallOutcome::Served))),
        end(5, "c1", json!("fine"), Some(ToolCallOutcome::Served)),
        end(6, "c1", json!("refused"), Some(ToolCallOutcome::Refused)),
    ];
    let rounds = rounds(&rows);
    assert_eq!(
        asked(&rounds),
        [
            ("c1".to_owned(), Some(json!("first"))),
            ("c9".to_owned(), Some(json!("theirs"))),
            ("c1".to_owned(), Some(json!("second")))
        ]
    );
    assert_eq!(results(&rounds), ["fine", "zed", "refused"]);
}

// Python: tests/test_channels/test_tool_history.py::test_every_result_is_replayed_whole_however_many_calls_follow
/// No window from turn to turn: what a later turn reads of a round does not
/// change as calls come after it (RMK-1370). The characters are Unicode's.
#[test]
fn every_result_is_replayed_whole_however_many_calls_follow() {
    let long = "é".repeat(7000);
    let rows: Vec<RoomEvent> = (0..12)
        .flat_map(|n| served(2 * n + 1, &format!("c{n}"), &long))
        .collect();
    assert_eq!(results(&rounds(&rows)), vec![long; 12]);
}

/// Two turns overlap, the model reusing its id: each end pairs with the
/// start of its own turn, its `correlation_id`, whether they answer one
/// trigger or two, as the reference implementation pairs them (review
/// probe `py_turn_key.out`).
#[test]
fn overlapping_turns_pair_each_end_with_the_start_of_its_own_turn() {
    for (first, second) in [("t1", "t1"), ("t1", "t2")] {
        let ended = |index, turn, trigger, answer: &str| {
            let served = end(index, "c1", json!(answer), Some(ToolCallOutcome::Served));
            in_turn(served, turn, trigger)
        };
        let rows = [
            in_turn(start(1, "c1"), "A", first),
            in_turn(start(2, "c1"), "B", second),
            ended(3, "A", first, "A's"),
            ended(4, "B", second, "B's"),
        ];
        let replayed = rounds(&rows);
        assert_eq!(
            ids(&replayed),
            [vec!["c1"], vec!["c1"]],
            "{first}, {second}"
        );
        assert_eq!(results(&replayed), ["A's", "B's"], "{first}, {second}");
    }
}

/// A deleted start row ends the round it stood in, its call left out, as
/// the reference implementation's withheld row does (probe
/// `py_deleted_start.out`): handed whole, or with its content withheld, as
/// a channel's history hands it (`visible_events`, light review).
#[test]
fn a_deleted_start_row_ends_its_round() {
    for withheld in [false, true] {
        let mut deleted = start(2, "c2");
        deleted
            .metadata
            .insert("deleted".to_owned(), Value::Bool(true));
        if withheld {
            deleted.content = TextContent::new("").into();
        }
        let answered = |index, id: &str| {
            end(
                index,
                id,
                json!(format!("{id}'s")),
                Some(ToolCallOutcome::Served),
            )
        };
        let rows = [
            start(1, "c1"),
            deleted,
            start(3, "c3"),
            answered(4, "c1"),
            answered(5, "c2"),
            answered(6, "c3"),
        ];
        let replayed = rounds(&rows);
        assert_eq!(
            ids(&replayed),
            [vec!["c1"], vec!["c3"]],
            "withheld: {withheld}"
        );
        assert_eq!(
            replayed.iter().map(|round| round.index).collect::<Vec<_>>(),
            [1, 3]
        );
    }
}

/// A result stored as parts reads as its texts, a part other than text
/// named; one stored as `null` reads as nothing.
#[test]
fn a_result_of_parts_reads_as_its_texts() {
    let parts = json!([{ "type": "text", "text": "a caption" }, { "type": "image", "url": "x" }]);
    let rows = [
        start(1, "c1"),
        end(2, "c1", parts, Some(ToolCallOutcome::Served)),
        start(3, "c2"),
        end(4, "c2", Value::Null, Some(ToolCallOutcome::Served)),
    ];
    assert_eq!(results(&rounds(&rows)), ["a caption [non-text part]", ""]);
}

/// Only the agent's own rows: another channel's, one that entered the room
/// inbound under its id, and a deleted one are not replayed.
#[test]
fn a_row_the_agent_did_not_write_or_that_was_deleted_is_not_replayed() {
    let alterations: [fn(&mut RoomEvent); 3] = [
        |row| row.source.channel_id = ChannelId::new("advisor").unwrap(),
        |row| row.source.direction = EventDirection::Inbound,
        |row| {
            row.metadata.insert("deleted".to_owned(), Value::Bool(true));
        },
    ];
    let mut rows: Vec<RoomEvent> = Vec::new();
    for (n, alter) in alterations.into_iter().enumerate() {
        let mut call = served(
            2 * u64::try_from(n).unwrap() + 1,
            &format!("c{n}"),
            "theirs",
        );
        call.iter_mut().for_each(alter);
        rows.extend(call);
    }
    rows.extend(served(7, "own", "mine"));
    assert_eq!(results(&rounds(&rows)), ["mine"]);
}
