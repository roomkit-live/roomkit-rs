//! Python: `tests/test_turn_notes.py`, public helpers for a hook or a reader.

use roomkit_ai::{
    AiContentPart, AiImagePart, AiMessage, AiMessageContent, AiRole, AiTextPart, TURN_NOTES_HEADER,
    add_turn_note, split_turn_notes,
};

fn text(value: &str) -> AiContentPart {
    AiContentPart::Text(AiTextPart::new(value))
}

fn image() -> AiContentPart {
    AiContentPart::Image(AiImagePart::new("https://example.com/a.png"))
}

fn message(content: impl Into<AiMessageContent>) -> AiMessage {
    AiMessage::new(AiRole::User, content)
}

// Python: test_notes_added_one_by_one_read_as_notes_assembled_at_once
// and test_a_note_joins_the_section_the_channel_opened_under_one_header.
#[test]
fn successive_notes_join_the_same_section_for_every_input_shape() {
    let quoted = format!("{TURN_NOTES_HEADER} is it real?");
    let inline = format!("what does {TURN_NOTES_HEADER} mean?");
    let assistant = AiMessage::new(AiRole::Assistant, "hello");
    let tool = AiMessage::new(AiRole::Tool, "{}");
    let section = format!("{TURN_NOTES_HEADER}\n\nA\n\nB\n\nC");
    let mut cases = Vec::new();
    for words in ["where is A-1042?", "", TURN_NOTES_HEADER, &quoted, &inline] {
        let expected = if words.is_empty() {
            section.clone()
        } else {
            format!("{words}\n\n{section}")
        };
        cases.push((vec![message(words)], vec![message(expected)]));
    }
    for parts in [
        vec![text("see this"), image()],
        vec![image()],
        vec![text("a"), text("b")],
        vec![],
        vec![image(), text(&quoted)],
    ] {
        let mut expected = parts.clone();
        expected.push(text(&section));
        cases.push((vec![message(parts)], vec![message(expected)]));
    }
    for conversation in [vec![], vec![message("hi"), assistant], vec![tool]] {
        let mut expected = conversation.clone();
        expected.push(message(section.clone()));
        cases.push((conversation, expected));
    }
    for (before, expected) in cases {
        let with_notes = ["A", "B", "C"].into_iter().fold(before, add_turn_note);
        assert_eq!(with_notes, expected);
    }
}

// Python: test_a_part_appended_after_the_notes_opens_no_second_header.
#[test]
fn a_note_joins_the_last_notes_part_even_when_an_image_follows_it() {
    let old = format!("{TURN_NOTES_HEADER}\n\nold");
    let current = format!("{TURN_NOTES_HEADER}\n\nchannel");
    let mut input = message(vec![text(&old), text(&current), image()]);
    input.metadata.insert("source".into(), "hook".into());
    let mut expected = input.clone();
    expected.content = vec![text(&old), text(&format!("{current}\n\nhook")), image()].into();
    assert_eq!(add_turn_note(vec![input], "hook"), vec![expected]);
}

// Python: test_split_turn_notes_takes_the_input_and_its_notes_apart
// and test_an_input_that_quotes_the_header_keeps_its_words.
#[test]
fn split_keeps_quotes_and_reads_the_last_header_that_opens_a_paragraph() {
    for quoted in [
        TURN_NOTES_HEADER.to_owned(),
        format!("what does {TURN_NOTES_HEADER} mean?"),
        format!("{TURN_NOTES_HEADER} is it real?"),
        "no notes here".to_owned(),
    ] {
        assert_eq!(split_turn_notes(&quoted), (quoted.as_str(), ""));
    }
    let notes = format!("{TURN_NOTES_HEADER}\n\nA");
    assert_eq!(split_turn_notes(&notes), ("", notes.as_str()));
    let input = format!("texte é\n\n{notes}\n\n");
    let both = format!("{input}\n\n{notes}");
    assert_eq!(split_turn_notes(&both), (input.as_str(), notes.as_str()));
}

// Python: tests/test_runtime_mark_copies.py, add_turn_note's block is cleaned.
#[test]
fn a_copied_notes_header_in_a_block_leaves_only_the_real_header() {
    let messages = add_turn_note(vec![message("words")], TURN_NOTES_HEADER);
    let rendered = messages[0].text();
    let (words, notes) = split_turn_notes(&rendered);
    assert_eq!(words, "words");
    assert_eq!(notes.matches(TURN_NOTES_HEADER).count(), 1);
    assert!(notes.contains("A copy of the runtime's notes header stood here"));
}
