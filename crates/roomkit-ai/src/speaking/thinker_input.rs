//! A thinker's bounded transcript, keeping the context's attribution.

use roomkit_core::rules::{
    reads_as,
    transcript::{TURN_IMAGES_HEADER, quoted},
};
use serde_json::Value;

use super::Thought;
use crate::{AiContentPart, AiMessage, AiMessageContent, AiRole, split_turn_notes};

pub(super) fn thinker_input(previous: &Thought, messages: &[AiMessage]) -> String {
    let wanted = previous
        .want_to_say
        .iter()
        .map(|text| json_line(text))
        .collect::<Vec<_>>()
        .join(", ");
    let state = format!(
        "{{\"thinking\": {}, \"want_to_say\": [{wanted}], \"urgent\": {}}}",
        json_line(&previous.text),
        previous.urgent
    );
    let mut lines = vec![
        format!("Your previous thought: {state}"),
        "The conversation, up to what was just said:".to_owned(),
    ];
    lines.extend(
        messages
            .iter()
            .map(transcript_line)
            .filter(|line| !line.is_empty()),
    );
    lines.push("Your thought, now:".to_owned());
    lines.join("\n")
}

fn json_line(text: &str) -> String {
    Value::String(text.into())
        .to_string()
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
        .replace('\u{85}', "\\u0085")
}

fn transcript_line(message: &AiMessage) -> String {
    if message.role == AiRole::Tool {
        return String::new();
    }
    let text = match &message.content {
        AiMessageContent::Text(text) => split_turn_notes(text).0.to_owned(),
        AiMessageContent::Parts(parts) => parts.iter()
            .take_while(|part| !matches!(part, AiContentPart::Text(text) if text.text == TURN_IMAGES_HEADER))
            .map(|part| match part {
                AiContentPart::Text(text) => split_turn_notes(&text.text).0,
                AiContentPart::Image(_) | AiContentPart::Thinking(_) | AiContentPart::ToolCall(_) | AiContentPart::ToolResult(_) => "",
            }).collect::<Vec<_>>().join("\n"),
    };
    let text = text.trim_matches(space);
    if text.is_empty() {
        return String::new();
    }
    if message.role == AiRole::Assistant {
        return format!("You: {}", quoted(text, 2000));
    }
    let speaker = message.metadata.get("speaker");
    if speaker.is_none_or(Value::is_null) {
        return said_by(text, None);
    }
    let (lead, own) = leading_text(message, text);
    [
        (!lead.is_empty()).then(|| quoted(lead, 2000)),
        (!own.is_empty()).then(|| said_by(own, speaker.and_then(Value::as_str))),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("\n")
}

fn leading_text<'a>(message: &'a AiMessage, text: &'a str) -> (&'a str, &'a str) {
    let lead = message
        .metadata
        .get("leading_text")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim_matches(space);
    if !lead.is_empty()
        && let Some(own) = text.trim_start_matches(space).strip_prefix(lead)
    {
        return (lead, own.trim_matches(space));
    }
    ("", text)
}

fn said_by(text: &str, speaker: Option<&str>) -> String {
    let Some(speaker) = speaker else {
        return quoted(text, 2000);
    };
    let prefix = format!("{speaker}: ");
    let bare = format!("{speaker}:");
    let lines = lines(text);
    let first = lines
        .iter()
        .find(|line| !line.trim_matches(space).is_empty())
        .copied()
        .unwrap_or_default();
    if !first.starts_with(&prefix) && first.trim_matches(space) != bare {
        return quoted(text, 2000);
    }
    let own = lines
        .into_iter()
        .filter(|line| line.trim_matches(space) != bare)
        .map(|line| read_line(line, &prefix))
        .collect::<String>();
    let label = if reads_as(speaker, "you") {
        format!("{speaker} (a participant)")
    } else {
        speaker.to_owned()
    };
    format!("{label}: {}", quoted(&own, 2000))
}

fn read_line(line: &str, prefix: &str) -> String {
    let Some(body) = line.strip_prefix(prefix) else {
        return line.to_owned();
    };
    let content = body.trim_end_matches(['\r', '\n']);
    let read = serde_json::from_str::<String>(content)
        .or_else(|_| serde_json::from_str::<String>(&format!("{content}\"")));
    format!(
        "{}{}",
        read.as_deref().unwrap_or(content),
        body.get(content.len()..).unwrap_or_default()
    )
}

fn lines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut chars = text.char_indices().peekable();
    while let Some((at, character)) = chars.next() {
        if matches!(
            character,
            '\n' | '\r' | '\u{b}' | '\u{c}' | '\u{1c}'
                ..='\u{1e}' | '\u{85}' | '\u{2028}' | '\u{2029}'
        ) {
            let mut end = at + character.len_utf8();
            if character == '\r' && chars.peek().is_some_and(|(_, next)| *next == '\n') {
                chars.next();
                end += 1;
            }
            if let Some(line) = text.get(start..end) {
                lines.push(line);
            }
            start = end;
        }
    }
    if start < text.len()
        && let Some(line) = text.get(start..)
    {
        lines.push(line);
    }
    lines
}

fn space(character: char) -> bool {
    character.is_whitespace() || matches!(character, '\u{1c}'..='\u{1f}')
}
