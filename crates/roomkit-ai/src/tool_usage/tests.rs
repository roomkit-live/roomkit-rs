//! Python: `tests/test_tool_result_images.py`, what a scenario cannot say.

use pretty_assertions::assert_eq;
use roomkit::model::{Metadata, RoomId};
use roomkit_core::rules::transcript::turn_notes;

use super::*;
use crate::tools::ToolResultPart;

const PNG: &str = "data:image/png;base64,iVBORw0KGgo=";

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

fn images(url: &str) -> ToolResult {
    ToolResult::Parts(vec![ToolResultPart::Image(AiImagePart::new(url))])
}

fn replayed(memory: &ToolUsageMemory) -> Vec<String> {
    memory
        .turn_images(&room())
        .into_iter()
        .filter_map(|part| match part {
            AiContentPart::Image(image) => Some(image.url),
            AiContentPart::Text(_)
            | AiContentPart::Thinking(_)
            | AiContentPart::ToolCall(_)
            | AiContentPart::ToolResult(_) => None,
        })
        .collect()
}

// Python: test_only_an_inline_image_every_provider_takes_is_replayed, the
// image past any vendor's size; the others are a scenario's
// (tools/only-an-inline-tool-image-of-a-vision-type-is-replayed).
#[test]
fn an_image_past_two_million_characters_is_not_replayed() {
    let head = "data:image/png;base64,";
    let memory = ToolUsageMemory::default();
    let at_bound = format!("{head}{}", "A".repeat(2_000_000 - head.len()));
    memory.record(&room(), "draw", &Metadata::new(), &images(&at_bound));
    assert_eq!(replayed(&memory).len(), 1);
    let past = format!("{head}{}", "A".repeat(2_000_000));
    memory.forget_images(&room());
    memory.record(&room(), "draw", &Metadata::new(), &images(&past));
    assert_eq!(replayed(&memory), Vec::<String>::new());
}

// Python: test_a_text_result_and_a_seeded_one_bring_no_image. A result read
// back from the store is never recorded: the channel records live calls.
#[test]
fn a_text_result_brings_no_image() {
    let memory = ToolUsageMemory::default();
    memory.record(
        &room(),
        "lookup",
        &Metadata::new(),
        &ToolResult::from("{\"status\": \"shipped\"}"),
    );
    assert_eq!(memory.turn_images(&room()), Vec::new());
}

// Python: test_only_a_request_that_replays_images_can_have_them_forgotten.
#[test]
fn only_a_request_that_replays_images_carries_them() {
    let asked = vec![AiMessage::new(AiRole::User, "set my lights to its colours")];
    let mut media = TurnMedia::replaying(vec![
        AiContentPart::Text(AiTextPart::new(TURN_IMAGES_HEADER)),
        AiContentPart::Image(AiImagePart::new(PNG)),
    ]);
    media.place(&asked);
    assert!(carries_turn_images(
        &media.with_turn_images(asked.clone(), asked.len())
    ));
    assert!(!carries_turn_images(&asked));
}

// Python: test_a_note_a_hook_adds_joins_the_notes_before_the_images, its
// first half: a text input's notes become a part of their own before the
// images, its words the first part.
#[test]
fn a_text_input_s_notes_are_a_part_of_their_own_before_the_images() {
    let notes = turn_notes(&["a".to_owned()]).unwrap();
    let input = AiMessage::new(AiRole::User, format!("set my lights\n\n{notes}"));
    let header = AiContentPart::Text(AiTextPart::new(TURN_IMAGES_HEADER));
    let mut media = TurnMedia::replaying(vec![header.clone()]);
    media.place(std::slice::from_ref(&input));
    let applied = media.with_turn_images(vec![input], 1);
    let AiMessageContent::Parts(parts) = &applied[0].content else {
        panic!("parts");
    };
    let expected = vec![
        AiContentPart::Text(AiTextPart::new("set my lights")),
        AiContentPart::Text(AiTextPart::new(notes)),
        header,
    ];
    assert_eq!(parts, &expected);
}

// Python: _turn_notes.with_turn_images, a conversation that does not end
// on a user message: the images go as a user message of their own.
#[test]
fn images_after_an_assistant_s_message_are_a_user_message_of_their_own() {
    let shown = vec![AiMessage::new(AiRole::Assistant, "done")];
    let image = AiContentPart::Image(AiImagePart::new(PNG));
    let mut media = TurnMedia::replaying(vec![image.clone()]);
    media.place(&shown);
    let applied = media.with_turn_images(shown, 1);
    assert_eq!(applied.len(), 2);
    assert_eq!(applied[1], AiMessage::new(AiRole::User, vec![image]));
}

// Python: tests/test_channels/test_tool_usage_memory.py::TestToolUsageMemory::test_record_revealed_feeds_reveal
#[test]
fn record_revealed_feeds_reveal() {
    let memory = ToolUsageMemory::default();
    memory.record_revealed(&room(), &["spotify_play".to_owned()]);
    assert_eq!(memory.tool_names(&room()), ["spotify_play"]);
}

// Python: tests/test_channels/test_tool_usage_memory.py::TestToolUsageMemory::test_record_revealed_filters_infra_and_shares_the_reveal_cap
#[test]
fn record_revealed_shares_the_reveal_cap() {
    let memory = ToolUsageMemory::default();
    let result = ToolResult::Text("ok".to_owned());
    memory.record(&room(), "tool_used", &Metadata::new(), &result);
    let burst: Vec<String> = (0..12).map(|at| format!("tool_{at}")).collect();
    memory.record_revealed(&room(), &burst);
    // The burst ages the older used tool out of their shared recency window.
    assert_eq!(memory.tool_names(&room()), burst);
    memory.record(&room(), "tool_0", &Metadata::new(), &result);
    assert_eq!(
        memory.tool_names(&room()).last().map(String::as_str),
        Some("tool_0")
    );
}

// Python: tests/test_channels/test_tool_usage_memory.py::TestToolUsageMemory::test_live_entries_preempt_hydration
#[test]
fn live_entries_preempt_hydration() {
    let memory = ToolUsageMemory::default();
    let result = ToolResult::Text("ok".to_owned());
    memory.record(&room(), "live", &Metadata::new(), &result);
    memory.hydrate(&room(), ["stale"].into_iter());
    assert_eq!(memory.tool_names(&room()), ["live"]);
}

// Python: tests/test_channels/test_tool_usage_memory.py::TestToolUsageInContext::test_build_context_hydrates_from_loader_once
#[test]
fn a_room_is_hydrated_once_even_from_nothing() {
    let memory = ToolUsageMemory::default();
    memory.hydrate(&room(), std::iter::empty());
    memory.hydrate(&room(), ["late"].into_iter());
    assert_eq!(memory.tool_names(&room()), Vec::<String>::new());
    memory.forget(&room());
    memory.hydrate(&room(), ["read_again"].into_iter());
    assert_eq!(memory.tool_names(&room()), ["read_again"]);
}

// Python: channels/_tool_usage.py's needs_hydration(), which moves no room, and
// _MAX_ROOMS: only a record makes a room the latest
#[test]
fn a_room_that_talks_without_a_call_ages_out() {
    let memory = ToolUsageMemory::default();
    let result = ToolResult::Text("ok".to_owned());
    memory.record(&room(), "used", &Metadata::new(), &result);
    for at in 0..MAX_ROOMS - 1 {
        let other = RoomId::new(format!("other-{at}")).unwrap();
        memory.record(&other, "used", &Metadata::new(), &result);
    }
    memory.hydrate(&room(), std::iter::empty());
    let last = RoomId::new("last").unwrap();
    memory.record(&last, "used", &Metadata::new(), &result);
    assert_eq!(memory.tool_names(&room()), Vec::<String>::new());
    assert_eq!(memory.tool_names(&last), ["used"]);
}

// The bound of channels/_tool_usage.py's _MAX_ROOMS across the rooms a shared
// channel serves, a room with no stored rows counted too. Python's seed() leaves it
// to the next record, holding 150 rooms here until then.
#[test]
fn hydrating_rooms_from_nothing_stays_under_the_rooms_bound() {
    let memory = ToolUsageMemory::default();
    for at in 0..MAX_ROOMS + 50 {
        let room = RoomId::new(format!("room-{at}")).unwrap();
        memory.hydrate(&room, std::iter::empty());
    }
    let kept = memory.by_room.lock().unwrap().len();
    assert_eq!(kept, MAX_ROOMS);
}
