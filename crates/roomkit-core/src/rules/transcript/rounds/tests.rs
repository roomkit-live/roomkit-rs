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
    let source = EventSource::new(
        ChannelId::new(AGENT).unwrap(),
        ChannelType::AI,
        EventDirection::Outbound,
    );
    let mut event = RoomEvent::new(
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
    tool_rounds(rows, &agent, &|tool| tool != "activate_skill")
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

/// Served, failed or blocked, or an end stored without an outcome; never a
/// refusal, a call nothing served or one cancelled, nor a call to a tool
/// that is no work of the agent's.
#[test]
fn only_the_answers_a_tool_gave_are_replayed() {
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
    assert_eq!(results(&rounds(&rows)), ["c1", "c2", "c3", "c7"]);
}

#[test]
fn a_call_with_no_end_row_is_not_replayed() {
    let [started, ended] = served(2, "c2", "ok");
    assert_eq!(results(&rounds(&[start(1, "c1"), started, ended])), ["ok"]);
}

/// Calls started together are one round; an end pairs with the start of
/// the same turn, and an id a provider reused from an earlier turn is
/// replayed under one of its own.
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
    assert_eq!(ids(&rounds), [vec!["c1", "c2"], vec!["c3"], vec!["c1_7"]]);
    assert_eq!(
        rounds.iter().map(|round| round.index).collect::<Vec<_>>(),
        [1, 5, 7]
    );
    assert_eq!(results(&rounds), ["a", "b", "c", "d"]);
}

/// The last 8 calls are replayed, the 3 most recent with their result
/// whole, the older ones as a preview that says the rest is not shown: the
/// reference implementation's bounds, written as numbers.
#[test]
fn the_last_calls_are_replayed_the_most_recent_whole() {
    let long = "x".repeat(200);
    let rows: Vec<RoomEvent> = (0..10)
        .flat_map(|n| served(2 * n + 1, &format!("c{n}"), &long))
        .collect();
    let replayed = rounds(&rows);
    assert_eq!(
        ids(&replayed).concat(),
        ["c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"]
    );
    let results = results(&replayed);
    let (older, recent) = results.split_at(5);
    assert_eq!(recent, vec![long; 3]);
    let preview = format!(
        "{}… [the rest is not shown here: call the tool again for it]",
        "x".repeat(120)
    );
    assert!(older.iter().all(|result| *result == preview), "{older:?}");
}

/// An older result's preview reads on one line, its spacing collapsed as
/// the reference implementation's `" ".join(text.split())` collapses it.
#[test]
fn a_preview_reads_its_result_on_one_line() {
    let mut rows: Vec<RoomEvent> = served(1, "c0", "Order 42:\n  shipped\tMonday\u{1f}").into();
    for n in 1..=3 {
        rows.extend(served(2 * n + 1, &format!("c{n}"), "ok"));
    }
    assert_eq!(results(&rounds(&rows))[0], "Order 42: shipped Monday");
}

/// A recent result over 6,000 characters is cut there, saying how long it
/// is; one of 6,000 is whole. The characters are Unicode's, not bytes.
#[test]
fn a_long_result_is_cut_at_the_kept_size_and_says_so() {
    let result = "é".repeat(6500);
    let [shown] = results(&rounds(&served(1, "c1", &result)))
        .try_into()
        .unwrap();
    assert_eq!(
        shown,
        format!(
            "{}\n[first 6000 of 6500 characters; the rest is not shown here: call the tool \
             again for it]",
            "é".repeat(6000)
        )
    );
    let exact = "é".repeat(6000);
    assert_eq!(results(&rounds(&served(1, "c1", &exact))), [exact]);
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
            [vec!["c1"], vec!["c1_2"]],
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
