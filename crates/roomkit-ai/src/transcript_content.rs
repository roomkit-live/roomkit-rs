//! What an event says in an AI channel's turn (RFC §6.4, §6.6): Python's
//! `_transcript_content`, `_extract_content` and `_extract_text`, which
//! read the event's content as the turn's author images decided.

use std::sync::Arc;

use roomkit::model::{EventContent, RoomEvent};
use roomkit_core::rules::transcoding::text_of;
use roomkit_core::rules::transcript::{Part, TurnContent};

use crate::author_images::AuthorImages;

/// The host's describer of an event whose content reads as nothing in the
/// turn's transcript: a text, or `None` to leave the event out. Python's
/// `EmptyEventDescriber`.
pub(crate) type Describer = Arc<dyn Fn(&RoomEvent) -> Option<String> + Send + Sync>;

/// How a turn reads each event: its author images as decided, whether the
/// model sees, and the host's describer.
pub(crate) struct TurnReader<'a> {
    pub(crate) images: &'a AuthorImages,
    pub(crate) vision: bool,
    pub(crate) describer: Option<&'a Describer>,
}

impl TurnReader<'_> {
    /// What `event` says in the turn's transcript, history and input alike:
    /// its content, or, when that reads as nothing, what the describer says
    /// of it, else a lone media's label (RFC §6.6). Nothing leaves the
    /// event out. Python's `_transcript_content`.
    pub(crate) fn read(&self, event: &RoomEvent) -> TurnContent {
        let content = self.extract(event);
        if !says_nothing(&content) {
            return content;
        }
        let described = self.describer.and_then(|describe| describe(event));
        match (described, &event.content) {
            (Some(text), _) if !text.is_empty() => TurnContent::Text(text),
            // A media the host does not describe reads as its label.
            (_, EventContent::Media(_)) => TurnContent::Text(text_of(&event.content)),
            (_, _) => content,
        }
    }

    /// What `event` says to the model: its text, and for a model that sees,
    /// each media as the images decided (its image, else a mention).
    /// Python's `_extract_content`.
    fn extract(&self, event: &RoomEvent) -> TurnContent {
        if !self.vision {
            return TurnContent::Text(self.extract_text(event));
        }
        let id = event.id.as_str();
        match &event.content {
            EventContent::Text(text) => TurnContent::Text(text.body.clone()),
            EventContent::Media(_) => TurnContent::Parts(self.images.parts(id, 0, &event.content)),
            EventContent::Composite(composite) => {
                let parts: Vec<Part> = composite
                    .parts()
                    .iter()
                    .enumerate()
                    .flat_map(|(index, part)| self.part_of(id, index, part))
                    .collect();
                if parts.is_empty() {
                    TurnContent::Text(String::new())
                } else {
                    TurnContent::Parts(parts)
                }
            }
            EventContent::Rich(_)
            | EventContent::Location(_)
            | EventContent::Audio(_)
            | EventContent::Video(_)
            | EventContent::System(_)
            | EventContent::Template(_)
            | EventContent::Edit(_)
            | EventContent::Delete(_)
            | EventContent::ToolCall(_)
            | _ => TurnContent::Text(self.extract_text(event)),
        }
    }

    /// What the part `index` of a composite, of the event `id`, reads as
    /// for a model that sees: a media as the images decided, any other
    /// part as a text-only reader reads it, nothing for an empty one.
    fn part_of(&self, id: &str, index: usize, part: &EventContent) -> Vec<Part> {
        if matches!(part, EventContent::Media(_)) {
            return self.images.parts(id, index, part);
        }
        content_text(part)
            .filter(|text| !text.is_empty())
            .map(Part::Text)
            .into_iter()
            .collect()
    }

    /// What `event` says to a model that does not see: its text, an image
    /// as the images decided (a mention that the model does not see it),
    /// any other media as its label (RFC §6.6), never bytes; nothing for a
    /// lone media without a caption that is no image, which the host's
    /// describer has the word on first. Python's `_extract_text`.
    fn extract_text(&self, event: &RoomEvent) -> String {
        let id = event.id.as_str();
        if let EventContent::Media(media) = &event.content {
            let captioned = media
                .caption
                .as_deref()
                .is_some_and(|caption| !caption.is_empty());
            if !captioned && !self.images.decides(id, 0) {
                return String::new();
            }
            return texts_of(&self.images.parts(id, 0, &event.content));
        }
        let EventContent::Composite(composite) = &event.content else {
            return content_text(&event.content).unwrap_or_default();
        };
        composite
            .parts()
            .iter()
            .enumerate()
            .map(|(index, part)| {
                if matches!(part, EventContent::Media(_)) {
                    texts_of(&self.images.parts(id, index, part))
                } else {
                    content_text(part).unwrap_or_default()
                }
            })
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// Whether `content` says nothing: an empty text, or no part.
fn says_nothing(content: &TurnContent) -> bool {
    match content {
        TurnContent::Text(text) => text.is_empty(),
        TurnContent::Parts(parts) => parts.is_empty(),
    }
}

/// The text parts of `parts`, one per line. Python's `_text_of`.
fn texts_of(parts: &[Part]) -> String {
    parts
        .iter()
        .filter_map(|part| match part {
            Part::Text(text) => Some(text.as_str()),
            Part::Image(_) | _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// What a text-only reader reads of `content` (RFC §6.6): its text (a rich
/// content's plain text), each media as its label, a composite's parts
/// joined, one per line; `None` for content of another kind. Python's
/// `content_text`.
pub(crate) fn content_text(content: &EventContent) -> Option<String> {
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
