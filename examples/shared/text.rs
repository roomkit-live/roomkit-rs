//! What a person reads of a content, as the examples print it.

use roomkit::model::EventContent;

/// What a person reads of `content`: a text's body, a rich text's plain
/// text, a system notice's body, a correction's new text; an empty string
/// for the contents the examples do not print.
pub fn text_of(content: &EventContent) -> String {
    match content {
        EventContent::Text(text) => text.body.clone(),
        EventContent::Rich(rich) => rich.plain_text.clone().unwrap_or_else(|| rich.body.clone()),
        EventContent::System(system) => system.body.clone(),
        EventContent::Edit(edit) => format!("Correction: {}", text_of(&edit.new_content)),
        EventContent::Delete(_) => "[Message deleted]".to_owned(),
        EventContent::Media(_)
        | EventContent::Location(_)
        | EventContent::Audio(_)
        | EventContent::Video(_)
        | EventContent::Composite(_)
        | EventContent::Template(_)
        | EventContent::ToolCall(_)
        | _ => String::new(),
    }
}
