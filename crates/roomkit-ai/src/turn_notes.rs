//! RFC §6.4: runtime notes appended to the input of a text turn.

use roomkit_core::rules::transcript::{
    TURN_NOTES_HEADER, without_mark_copies, without_notes_header_copies,
};

use crate::{AiContentPart, AiMessage, AiMessageContent, AiRole, AiTextPart};

fn opens_notes(text: &str) -> bool {
    text.strip_prefix(TURN_NOTES_HEADER)
        .is_some_and(|tail| tail.starts_with("\n\n"))
}

/// Splits an input at the last paragraph that opens the runtime's notes.
/// The notes include their header. With no matching header the second slice is empty.
pub fn split_turn_notes(text: &str) -> (&str, &str) {
    for (index, _) in text.rmatch_indices(TURN_NOTES_HEADER) {
        let (words, notes) = text.split_at(index);
        if (words.is_empty() || words.ends_with("\n\n")) && opens_notes(notes) {
            return (words.strip_suffix("\n\n").unwrap_or(words), notes);
        }
    }
    (text, "")
}

/// Appends a block to the last user input's runtime notes, or opens a new
/// user message when the conversation ends on another role.
pub fn add_turn_note(mut messages: Vec<AiMessage>, block: &str) -> Vec<AiMessage> {
    let block = without_notes_header_copies(&without_mark_copies(block));
    let section = || format!("{TURN_NOTES_HEADER}\n\n{block}");
    let Some(last) = messages.last_mut().filter(|last| last.role == AiRole::User) else {
        messages.push(AiMessage::new(AiRole::User, section()));
        return messages;
    };
    match &mut last.content {
        AiMessageContent::Text(text) => {
            let opening = split_turn_notes(text).1.is_empty();
            if !text.is_empty() {
                text.push_str("\n\n");
            }
            if opening {
                text.push_str(TURN_NOTES_HEADER);
                text.push_str("\n\n");
            }
            text.push_str(&block);
        }
        AiMessageContent::Parts(parts) => {
            if let Some(AiContentPart::Text(notes)) = parts
                .iter_mut()
                .rfind(|part| matches!(part, AiContentPart::Text(p) if opens_notes(&p.text)))
            {
                notes.text.push_str("\n\n");
                notes.text.push_str(&block);
            } else {
                parts.push(AiContentPart::Text(AiTextPart::new(section())));
            }
        }
    }
    messages
}
