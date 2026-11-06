//! What an ACP agent reads of a room event.

use roomkit::model::{EventContent, RoomEvent};
use roomkit_core::rules::transcoding::text_of;
use roomkit_core::rules::transcript::{RUNTIME_RECORD, without_mark_copies};

/// The text an ACP agent reads for `event`: its prompt, and each line of the
/// room context it is given (RFC §6.4, ACP agent channel, item 4). Python's
/// `acp_event_text`.
///
/// Rich content is offered as its plain text: the prompt is a string, and a
/// session that received the markup would answer about it. A media reads as
/// its label, a composite as its parts, one per line (RFC §6.6). A copy of a
/// runtime mark in it is replaced, so that it cannot pass for the runtime's
/// (RFC §6.4), but in the runtime's own record (a handoff's).
pub fn acp_event_text(event: &RoomEvent) -> String {
    let text = content_text(&event.content).unwrap_or_else(|| {
        if let EventContent::Text(text) = &event.content {
            text.body.clone()
        } else {
            String::new()
        }
    });
    let by_runtime = event
        .metadata
        .get(RUNTIME_RECORD)
        .is_some_and(serde_json::Value::is_string);
    if by_runtime {
        text
    } else {
        without_mark_copies(&text)
    }
}

/// What a text-only reader reads of `content` (RFC §6.6): its text (a rich
/// content's plain text), each media as its label, a composite's parts
/// joined, one per line; none for content of another kind. Python's
/// `content_text`.
fn content_text(content: &EventContent) -> Option<String> {
    match content {
        EventContent::Text(text) => Some(text.body.clone()),
        EventContent::Rich(rich) => Some(
            rich.plain_text
                .clone()
                .filter(|plain| !plain.is_empty())
                .unwrap_or_else(|| rich.body.clone()),
        ),
        EventContent::Media(_) | EventContent::Audio(_) | EventContent::Video(_) => {
            Some(text_of(content))
        }
        EventContent::Composite(composite) => Some(
            composite
                .parts()
                .iter()
                .filter_map(content_text)
                .filter(|text| !text.is_empty())
                .collect::<Vec<_>>()
                .join("\n"),
        ),
        EventContent::Location(_)
        | EventContent::System(_)
        | EventContent::Template(_)
        | EventContent::Edit(_)
        | EventContent::Delete(_)
        | EventContent::ToolCall(_)
        | _ => None,
    }
}

#[cfg(test)]
mod tests {
    use roomkit::model::{
        ChannelId, ChannelType, EventSource, LocationContent, RichContent, RoomId, TextContent,
    };
    use roomkit::model::{Latitude, Longitude};

    use super::*;

    fn event(content: impl Into<EventContent>) -> RoomEvent {
        let source = EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS);
        RoomEvent::new(RoomId::new("room-1").unwrap(), source, content)
    }

    // Python: tests/test_channels/test_acp_event_text.py::test_text_is_its_body
    #[test]
    fn text_is_its_body() {
        assert_eq!(acp_event_text(&event(TextContent::new("hello"))), "hello");
    }

    // Python: tests/test_channels/test_acp_event_text.py::test_rich_content_is_read_as_its_plain_text
    #[test]
    fn rich_content_is_read_as_its_plain_text() {
        let rich = RichContent {
            plain_text: Some("Invoice ready".to_owned()),
            ..RichContent::new("**Invoice** ready")
        };
        assert_eq!(acp_event_text(&event(rich)), "Invoice ready");
    }

    // Python: tests/test_channels/test_acp_event_text.py::test_rich_content_without_plain_text_falls_back_to_its_body
    #[test]
    fn rich_content_without_plain_text_falls_back_to_its_body() {
        assert_eq!(
            acp_event_text(&event(RichContent::new("**bold**"))),
            "**bold**"
        );
    }

    // Python: tests/test_channels/test_acp_event_text.py::test_other_content_carries_no_text_as_on_any_channel
    #[test]
    fn other_content_carries_no_text_as_on_any_channel() {
        let location = LocationContent {
            label: Some("Montreal".to_owned()),
            ..LocationContent::new(Latitude::new(45.5).unwrap(), Longitude::new(-73.6).unwrap())
        };
        assert_eq!(acp_event_text(&event(location)), "");
    }
}
