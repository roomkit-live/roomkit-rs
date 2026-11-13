//! Formatter boundaries that the integration runner does not fabricate.

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelType, EventId,
    EventSource, EventType, Room, RoomContext, RoomEvent, RoomId, TextContent, Timestamp,
};

use super::{Input, Seen};

fn event(index: u64) -> RoomEvent {
    let mut event = RoomEvent::at(
        EventId::new(format!("event-{index}")).unwrap(),
        RoomId::new("room").unwrap(),
        EventType::Message,
        EventSource::new(ChannelId::new("ws").unwrap(), ChannelType::WEBSOCKET),
        TextContent::new(format!("m{index}")),
        Timestamp::UNIX_EPOCH,
    );
    event.index = index;
    event
}

fn input(events: Vec<RoomEvent>, trigger: RoomEvent) -> Input {
    let room = Room::at(trigger.room_id.clone(), Timestamp::UNIX_EPOCH);
    let binding = ChannelBinding::at(
        ChannelId::new("agent").unwrap(),
        room.id.clone(),
        ChannelType::AI,
        ChannelCategory::Intelligence,
        ChannelDirection::Bidirectional,
        Timestamp::UNIX_EPOCH,
    );
    let context = RoomContext {
        bindings: vec![binding],
        recent_events: events,
        ..RoomContext::new(room)
    };
    Input {
        context,
        trigger,
        text: "go".to_owned(),
        blocks: Vec::new(),
    }
}

fn prompt(input: &Input, after: u64, limit: usize) -> String {
    input
        .prompt(
            &ChannelId::new("agent").unwrap(),
            Seen {
                index: Some(after),
                labelled: false,
            },
            limit,
        )
        .0
}

// Python: tests/test_channels/test_acp_catch_up_integration.py::test_a_tail_of_nothing_new_still_says_the_gap_was_not_loaded
#[test]
fn test_a_tail_of_nothing_new_still_says_the_gap_was_not_loaded() {
    let mut tools: Vec<_> = (20..25).map(event).collect();
    for tool in &mut tools {
        tool.r#type = EventType::ToolCallStart;
    }
    tools.push(event(25));
    assert_eq!(
        prompt(&input(tools, event(25)), 3, 5),
        "[Room context — none of the loaded messages are new to you; up to 16 earlier room events were not loaded. Context only; the request follows.]\n\ngo"
    );
}

// Python: tests/test_channels/test_acp_catch_up_integration.py::test_both_cuts_are_named_when_both_apply
#[test]
fn test_both_cuts_are_named_when_both_apply() {
    let sent = prompt(&input((20..=30).map(event).collect(), event(30)), 3, 5);
    assert_eq!(
        sent.lines().next().unwrap(),
        "[Room context — the 5 most recent of 10 loaded messages you did not receive; up to 16 earlier room events were not loaded. Context only; the request follows.]"
    );
    assert!(!sent.contains("m24"));
    assert!(sent.contains("[1] @ws: “m25”"));
}

// Python: tests/test_channels/test_acp_catch_up_integration.py::test_one_of_each_reads_in_the_singular
#[test]
fn test_one_of_each_reads_in_the_singular() {
    let sent = prompt(&input(vec![event(5), event(6)], event(6)), 3, 5);
    assert_eq!(
        sent.lines().next().unwrap(),
        "[Room context — the 1 most recent message you did not receive; up to 1 earlier room event was not loaded. Context only; the request follows.]"
    );
}

// Python: tests/test_channels/test_acp.py::TestRoomCatchUp::test_its_own_words_are_not_quoted_back
#[test]
fn a_standalone_reply_is_the_agents_own_words_from_a_different_session() {
    let mut own = event(0);
    own.source.channel_id = ChannelId::new("agent").unwrap();
    own.metadata
        .insert("acp".to_owned(), serde_json::json!({"standalone":true}));
    let sent = input(vec![own, event(1)], event(1))
        .prompt(&ChannelId::new("agent").unwrap(), Seen::default(), 20)
        .0;
    assert!(sent.contains("[1] you (in a separate session): “m0”"));
}

#[test]
fn the_cursor_covers_the_latest_seen_event_even_when_the_trigger_is_an_instruction() {
    let mut trigger = event(0);
    trigger.r#type = EventType::Instruction;
    let input = input(vec![event(4), event(8)], trigger);
    assert_eq!(input.seen_index(), 8);
}
