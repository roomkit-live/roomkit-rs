//! What a channel receives when it cannot carry an event's content
//! (RFC §6.6).

use crate::model::{
    ChannelCapabilities, ChannelMediaType, CompositeContent, EventContent, LocationContent,
    TextContent,
};

/// The content a channel with `capabilities` receives for `content`
/// (RFC §6.6).
///
/// - A content the channel carries is delivered unchanged; one it does not
///   is replaced by its text form ([`text_of`]), the fallback every channel
///   takes (gap G-019).
/// - An edit or a delete is delivered unchanged to a channel that supports
///   the operation natively, as its text form otherwise.
/// - A composite is transcoded part by part; when every part ends up as
///   text, it becomes one text, its text form: the parts joined by a
///   newline, an empty one skipped (gaps G-007, G-072).
/// - System and tool-call contents have no row in the RFC's table and are
///   delivered unchanged (gap G-019).
/// - Text is then cut to the channel's `max_length`, counted in Unicode code
///   points; a cut text ends with `...` within the limit, unless the limit
///   is below 4 code points (gap G-008).
pub(crate) fn transcode(
    content: &EventContent,
    capabilities: &ChannelCapabilities,
) -> EventContent {
    let transcoded = transcode_content(content, capabilities);
    match capabilities.max_length {
        Some(max_length) => truncate(transcoded, max_length),
        None => transcoded,
    }
}

/// The text a reader takes from a content (RFC §6.6): its text, each media
/// as its label, a location's coordinates in decimal with a point
/// (`45.0`), a system notice as its body, a tool call as
/// `[Tool call: <name>]`, a composite's parts in order, one per line, an
/// empty one skipped (gap G-072). An instruction's fingerprint is of it.
///
/// A media's label names its URL only when the URL is an `http` or `https`
/// link, its MIME type otherwise: never a `data:` URI or an application
/// reference.
pub fn text_of(content: &EventContent) -> String {
    match content {
        EventContent::Text(text) => text.body.clone(),
        EventContent::Rich(rich) => rich.plain_text.clone().unwrap_or_else(|| rich.body.clone()),
        EventContent::Media(media) => {
            let label = media
                .caption
                .as_deref()
                .or(media.filename.as_deref())
                .unwrap_or_else(|| link_or_type(&media.url, &media.mime_type));
            format!("[Media: {label}]")
        }
        EventContent::Audio(audio) => audio.transcript.clone().unwrap_or_else(|| {
            format!(
                "[Voice message: {}]",
                link_or_type(&audio.url, &audio.mime_type)
            )
        }),
        EventContent::Video(video) => {
            format!("[Video: {}]", link_or_type(&video.url, &video.mime_type))
        }
        EventContent::Location(location) => location_text(location),
        EventContent::Composite(composite) => lines(composite.parts()),
        EventContent::System(system) => system.body.clone(),
        EventContent::Template(template) => template
            .body
            .clone()
            .unwrap_or_else(|| format!("[Template: {}]", template.template_id)),
        EventContent::Edit(edit) => format!("Correction: {}", text_of(&edit.new_content)),
        EventContent::Delete(_) => "[Message deleted]".to_owned(),
        EventContent::ToolCall(tool_call) => format!("[Tool call: {}]", tool_call.tool_name),
    }
}

/// The texts of `parts`, one per line, an empty one taking none (gap
/// G-072).
fn lines(parts: &[EventContent]) -> String {
    parts
        .iter()
        .map(text_of)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn transcode_content(content: &EventContent, capabilities: &ChannelCapabilities) -> EventContent {
    match content {
        EventContent::Text(_) | EventContent::System(_) | EventContent::ToolCall(_) => {
            content.clone()
        }
        EventContent::Rich(_) => carried_or_text(content, ChannelMediaType::Rich, capabilities),
        EventContent::Media(_) => carried_or_text(content, ChannelMediaType::Media, capabilities),
        EventContent::Location(_) => {
            carried_or_text(content, ChannelMediaType::Location, capabilities)
        }
        EventContent::Audio(_) => carried_or_text(content, ChannelMediaType::Audio, capabilities),
        EventContent::Video(_) => carried_or_text(content, ChannelMediaType::Video, capabilities),
        EventContent::Template(_) => {
            carried_or_text(content, ChannelMediaType::Template, capabilities)
        }
        EventContent::Edit(_) => native_or_text(content, capabilities.supports_edit),
        EventContent::Delete(_) => native_or_text(content, capabilities.supports_delete),
        EventContent::Composite(composite) => transcode_composite(composite, capabilities),
    }
}

fn carried_or_text(
    content: &EventContent,
    media_type: ChannelMediaType,
    capabilities: &ChannelCapabilities,
) -> EventContent {
    native_or_text(content, capabilities.carries(media_type))
}

fn native_or_text(content: &EventContent, native: bool) -> EventContent {
    if native {
        content.clone()
    } else {
        EventContent::text(text_of(content))
    }
}

fn transcode_composite(
    composite: &CompositeContent,
    capabilities: &ChannelCapabilities,
) -> EventContent {
    let parts: Vec<EventContent> = composite
        .parts()
        .iter()
        .map(|part| transcode_content(part, capabilities))
        .collect();
    if parts
        .iter()
        .all(|part| matches!(part, EventContent::Text(_)))
    {
        return EventContent::text(lines(&parts));
    }
    // Each part is replaced by its own transcoding, which never nests deeper.
    EventContent::Composite(CompositeContent::from_derived_parts(parts))
}

fn truncate(content: EventContent, max_length: u64) -> EventContent {
    match content {
        EventContent::Text(text) => EventContent::Text(truncate_text(text, max_length)),
        EventContent::Composite(composite) => {
            let parts = composite
                .into_parts()
                .into_iter()
                .map(|part| truncate(part, max_length))
                .collect();
            // Truncation changes no part's kind.
            EventContent::Composite(CompositeContent::from_derived_parts(parts))
        }
        EventContent::Rich(_)
        | EventContent::Media(_)
        | EventContent::Location(_)
        | EventContent::Audio(_)
        | EventContent::Video(_)
        | EventContent::System(_)
        | EventContent::Template(_)
        | EventContent::Edit(_)
        | EventContent::Delete(_)
        | EventContent::ToolCall(_) => content,
    }
}

/// What ends a cut text: three ASCII dots, since U+2026 would turn an SMS
/// into UCS-2 and shrink its segments (gap G-008).
const CUT_MARKER: &str = "...";

fn truncate_text(text: TextContent, max_length: u64) -> TextContent {
    let max_length = usize::try_from(max_length).unwrap_or(usize::MAX);
    if text.body.chars().count() <= max_length {
        return text;
    }
    let marker_length = CUT_MARKER.chars().count();
    let body = if max_length > marker_length {
        let kept: String = text.body.chars().take(max_length - marker_length).collect();
        kept + CUT_MARKER
    } else {
        text.body.chars().take(max_length).collect()
    };
    TextContent { body, ..text }
}

fn link_or_type<'content>(url: &'content str, mime_type: &'content str) -> &'content str {
    if is_link(url) { url } else { mime_type }
}

fn is_link(url: &str) -> bool {
    url.split_once("://").is_some_and(|(scheme, _)| {
        scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
    })
}

fn location_text(location: &LocationContent) -> String {
    let coordinates = format!(
        "({}, {})",
        coordinate_text(location.latitude.degrees()),
        coordinate_text(location.longitude.degrees())
    );
    match location.label.as_deref().or(location.address.as_deref()) {
        Some(name) => format!("[Location: {name} {coordinates}]"),
        None => format!("[Location: {coordinates}]"),
    }
}

/// A coordinate as a text form writes it (RFC §6.6, gap G-072): the
/// shortest decimal that reads back as the same binary64, which `f64`'s
/// `Display` writes with no exponent, and a digit after the point at least,
/// so that `45` reads `45.0` as every implementation writes it.
fn coordinate_text(degrees: f64) -> String {
    let text = degrees.to_string();
    if text.contains('.') {
        text
    } else {
        text + ".0"
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use pretty_assertions::assert_eq;
    use serde_json::Value;

    use super::*;
    use crate::model::{
        AudioContent, DeleteContent, DeleteType, EditContent, EventId, Latitude, Longitude,
        MediaContent, Metadata, RichContent, RichFormat, SystemContent, TemplateContent,
        ToolCallContent, ToolCallStatus, VideoContent,
    };

    fn text_only() -> ChannelCapabilities {
        ChannelCapabilities::default()
    }

    fn carrying(media_types: &[ChannelMediaType]) -> ChannelCapabilities {
        ChannelCapabilities {
            media_types: media_types.iter().copied().collect::<BTreeSet<_>>(),
            ..ChannelCapabilities::default()
        }
    }

    fn body_of(content: &EventContent) -> &str {
        let EventContent::Text(text) = content else {
            panic!("expected text content, got {content:?}");
        };
        &text.body
    }

    fn rich(body: &str, plain_text: Option<&str>) -> EventContent {
        EventContent::Rich(RichContent {
            body: body.to_owned(),
            format: RichFormat::Markdown,
            plain_text: plain_text.map(str::to_owned),
            buttons: Vec::new(),
            cards: Vec::new(),
            quick_replies: Vec::new(),
        })
    }

    fn media(url: &str, caption: Option<&str>, filename: Option<&str>) -> EventContent {
        EventContent::Media(MediaContent {
            url: url.to_owned(),
            mime_type: "image/jpeg".to_owned(),
            filename: filename.map(str::to_owned),
            caption: caption.map(str::to_owned),
            size_bytes: None,
        })
    }

    fn audio(url: &str, transcript: Option<&str>) -> EventContent {
        EventContent::Audio(AudioContent {
            url: url.to_owned(),
            duration_seconds: None,
            mime_type: "audio/ogg".to_owned(),
            transcript: transcript.map(str::to_owned),
        })
    }

    fn location(label: Option<&str>, address: Option<&str>) -> EventContent {
        EventContent::Location(LocationContent {
            latitude: Latitude::new(45.5).unwrap(),
            longitude: Longitude::new(-73.6).unwrap(),
            label: label.map(str::to_owned),
            address: address.map(str::to_owned),
        })
    }

    fn template(body: Option<&str>) -> EventContent {
        EventContent::Template(TemplateContent {
            template_id: "welcome".to_owned(),
            language: "en".to_owned(),
            parameters: BTreeMap::new(),
            body: body.map(str::to_owned),
        })
    }

    fn edit(new_body: &str) -> EventContent {
        EventContent::Edit(EditContent {
            target_event_id: EventId::new("evt_1").unwrap(),
            new_content: Box::new(EventContent::text(new_body)),
            edit_source: None,
        })
    }

    fn delete() -> EventContent {
        EventContent::Delete(DeleteContent {
            target_event_id: EventId::new("evt_1").unwrap(),
            delete_type: DeleteType::Sender,
            reason: None,
        })
    }

    fn composite(parts: Vec<EventContent>) -> EventContent {
        EventContent::Composite(CompositeContent::new(parts).unwrap())
    }

    fn transcoded_text(content: &EventContent) -> String {
        body_of(&transcode(content, &text_only())).to_owned()
    }

    #[test]
    fn a_carried_content_is_delivered_unchanged() {
        let content = rich("**hi**", None);
        assert_eq!(
            transcode(&content, &carrying(&[ChannelMediaType::Rich])),
            content
        );
    }

    #[test]
    fn rich_content_falls_back_to_its_plain_text() {
        assert_eq!(transcoded_text(&rich("**hi**", Some("hi"))), "hi");
    }

    #[test]
    fn rich_content_without_plain_text_falls_back_to_its_body_unchanged() {
        assert_eq!(transcoded_text(&rich("**hi**", None)), "**hi**");
    }

    #[test]
    fn media_falls_back_to_its_caption() {
        let content = media(
            "https://example.com/a.jpg",
            Some("The receipt"),
            Some("a.jpg"),
        );
        assert_eq!(transcoded_text(&content), "[Media: The receipt]");
    }

    #[test]
    fn media_without_caption_falls_back_to_its_filename() {
        let content = media("https://example.com/a.jpg", None, Some("a.jpg"));
        assert_eq!(transcoded_text(&content), "[Media: a.jpg]");
    }

    #[test]
    fn media_with_only_a_link_falls_back_to_the_link() {
        let content = media("https://example.com/a.jpg", None, None);
        assert_eq!(
            transcoded_text(&content),
            "[Media: https://example.com/a.jpg]"
        );
    }

    #[test]
    fn media_behind_a_data_uri_falls_back_to_its_type() {
        let content = media("data:image/jpeg;base64,AAAA", None, None);
        assert_eq!(transcoded_text(&content), "[Media: image/jpeg]");
    }

    #[test]
    fn audio_falls_back_to_its_transcript() {
        let content = audio("https://example.com/a.ogg", Some("call me back"));
        assert_eq!(transcoded_text(&content), "call me back");
    }

    #[test]
    fn audio_without_transcript_falls_back_to_a_voice_message_label() {
        let content = audio("https://example.com/a.ogg", None);
        assert_eq!(
            transcoded_text(&content),
            "[Voice message: https://example.com/a.ogg]"
        );
    }

    #[test]
    fn audio_behind_a_reference_never_shows_the_reference() {
        let content = audio("store://recordings/42", None);
        assert_eq!(transcoded_text(&content), "[Voice message: audio/ogg]");
    }

    #[test]
    fn video_falls_back_to_a_video_label() {
        let content = EventContent::Video(VideoContent {
            url: "https://example.com/a.mp4".to_owned(),
            duration_seconds: None,
            mime_type: "video/mp4".to_owned(),
            thumbnail_url: None,
        });
        assert_eq!(
            transcoded_text(&content),
            "[Video: https://example.com/a.mp4]"
        );
    }

    #[test]
    fn location_falls_back_to_its_label_and_coordinates() {
        let content = location(Some("Head office"), Some("1 Main St"));
        assert_eq!(
            transcoded_text(&content),
            "[Location: Head office (45.5, -73.6)]"
        );
    }

    #[test]
    fn location_without_label_falls_back_to_its_address() {
        let content = location(None, Some("1 Main St"));
        assert_eq!(
            transcoded_text(&content),
            "[Location: 1 Main St (45.5, -73.6)]"
        );
    }

    #[test]
    fn location_without_name_falls_back_to_its_coordinates() {
        assert_eq!(
            transcoded_text(&location(None, None)),
            "[Location: (45.5, -73.6)]"
        );
    }

    /// RFC §6.6, gap G-072: a coordinate reads as the shortest decimal that
    /// reads back as the same binary64, with a digit after the point at
    /// least and never an exponent, the cases the Python implementation
    /// tests too.
    #[test]
    fn a_location_s_coordinates_read_in_decimal_with_a_point() {
        let read: Vec<String> = [(45.0, -73.0), (5e-05, -1.5e-07), (45.5, -73.6)]
            .into_iter()
            .map(|(latitude, longitude)| {
                text_of(&EventContent::Location(LocationContent {
                    latitude: Latitude::new(latitude).unwrap(),
                    longitude: Longitude::new(longitude).unwrap(),
                    label: None,
                    address: None,
                }))
            })
            .collect();
        assert_eq!(
            read,
            [
                "[Location: (45.0, -73.0)]",
                "[Location: (0.00005, -0.00000015)]",
                "[Location: (45.5, -73.6)]",
            ]
        );
    }

    #[test]
    fn template_falls_back_to_its_body() {
        assert_eq!(transcoded_text(&template(Some("Welcome!"))), "Welcome!");
    }

    #[test]
    fn template_without_body_falls_back_to_its_id() {
        assert_eq!(transcoded_text(&template(None)), "[Template: welcome]");
    }

    #[test]
    fn an_edit_falls_back_to_a_correction() {
        assert_eq!(transcoded_text(&edit("meet at 3")), "Correction: meet at 3");
    }

    /// RFC §6.6: a channel that does not edit natively receives one text,
    /// `Correction: ` and the text form of the new content, even when that
    /// content is a composite holding a part the channel carries (gap
    /// G-072).
    #[test]
    fn an_edit_of_a_composite_falls_back_to_one_correction() {
        let new_content = composite(vec![
            EventContent::text("Here is the receipt"),
            media("https://example.com/a.jpg", None, Some("a.jpg")),
        ]);
        let edit = EventContent::Edit(EditContent {
            target_event_id: EventId::new("evt_1").unwrap(),
            new_content: Box::new(new_content),
            edit_source: None,
        });
        let capabilities = carrying(&[ChannelMediaType::Text, ChannelMediaType::Media]);
        assert_eq!(
            transcode(&edit, &capabilities),
            EventContent::text("Correction: Here is the receipt\n[Media: a.jpg]")
        );
    }

    #[test]
    fn an_edit_reaches_a_channel_that_edits_natively_unchanged() {
        let capabilities = ChannelCapabilities {
            supports_edit: true,
            ..text_only()
        };
        assert_eq!(
            transcode(&edit("meet at 3"), &capabilities),
            edit("meet at 3")
        );
    }

    #[test]
    fn a_delete_falls_back_to_a_deleted_notice() {
        assert_eq!(transcoded_text(&delete()), "[Message deleted]");
    }

    #[test]
    fn a_delete_reaches_a_channel_that_deletes_natively_unchanged() {
        let capabilities = ChannelCapabilities {
            supports_delete: true,
            ..text_only()
        };
        assert_eq!(transcode(&delete(), &capabilities), delete());
    }

    /// An empty part takes no line of its own (gap G-072).
    #[test]
    fn a_composite_of_text_fallbacks_becomes_one_text() {
        let content = composite(vec![
            EventContent::text("Here is the receipt"),
            EventContent::text(""),
            media("https://example.com/a.jpg", None, Some("a.jpg")),
        ]);
        assert_eq!(
            transcoded_text(&content),
            "Here is the receipt\n[Media: a.jpg]"
        );
    }

    #[test]
    fn a_composite_keeps_the_parts_the_channel_carries() {
        let photo = media("https://example.com/a.jpg", None, Some("a.jpg"));
        let content = composite(vec![EventContent::text("Receipt"), photo.clone()]);
        let capabilities = carrying(&[ChannelMediaType::Text, ChannelMediaType::Media]);
        assert_eq!(transcode(&content, &capabilities), content);
    }

    #[test]
    fn a_system_content_is_delivered_unchanged() {
        let content = EventContent::System(SystemContent::new("room created"));
        assert_eq!(transcode(&content, &text_only()), content);
    }

    #[test]
    fn a_tool_call_content_is_delivered_unchanged() {
        let content = EventContent::ToolCall(ToolCallContent {
            tool_name: "lookup".to_owned(),
            tool_id: "call_1".to_owned(),
            arguments: Metadata::new(),
            result: Value::Null,
            status: ToolCallStatus::Pending,
            duration_ms: None,
            error: None,
            structured_content: None,
            outcome: None,
            refused_but_ran: false,
        });
        assert_eq!(transcode(&content, &text_only()), content);
    }

    fn cut_to(max_length: u64, body: &str) -> String {
        let capabilities = ChannelCapabilities {
            max_length: Some(max_length),
            ..text_only()
        };
        body_of(&transcode(&EventContent::text(body), &capabilities)).to_owned()
    }

    #[test]
    fn a_cut_text_ends_with_the_marker_within_the_limit() {
        assert_eq!(cut_to(6, "élèves présents"), "élè...");
    }

    #[test]
    fn a_cut_counts_code_points() {
        assert_eq!(cut_to(4, "élève!").chars().count(), 4);
    }

    #[test]
    fn a_limit_below_four_cuts_without_the_marker() {
        assert_eq!(cut_to(3, "élève!"), "élè");
    }

    #[test]
    fn text_within_the_max_length_is_unchanged() {
        let capabilities = ChannelCapabilities {
            max_length: Some(10),
            ..text_only()
        };
        let content = EventContent::text("short");
        assert_eq!(transcode(&content, &capabilities), content);
    }

    #[test]
    fn a_fallback_text_is_cut_to_the_max_length() {
        let capabilities = ChannelCapabilities {
            max_length: Some(7),
            ..text_only()
        };
        assert_eq!(body_of(&transcode(&delete(), &capabilities)), "[Mes...");
    }

    #[test]
    fn the_text_parts_of_a_kept_composite_are_cut_to_the_max_length() {
        let photo = media("https://example.com/a.jpg", None, None);
        let content = composite(vec![EventContent::text("Receipt attached"), photo.clone()]);
        let capabilities = ChannelCapabilities {
            max_length: Some(7),
            ..carrying(&[ChannelMediaType::Text, ChannelMediaType::Media])
        };
        assert_eq!(
            transcode(&content, &capabilities),
            composite(vec![EventContent::text("Rece..."), photo])
        );
    }
}
