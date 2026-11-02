//! What an event says in a turn's transcript (RFC §6.4), as the channel
//! reading it decides: a text, or parts in order, an author's image among
//! them for a model that sees. Python's `str | list[AITextPart |
//! AIImagePart]`, which the channel's context builder writes.

use super::images::Image;
use super::speakers::labelled_lines;

/// One part of a message's content, in order: Python's `AITextPart` or
/// `AIImagePart`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Part {
    /// A text.
    Text(String),
    /// An author's image, sent as an image (RFC §6.4).
    Image(Image),
}

/// What an event says in a turn's transcript: a text, or its parts in
/// order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TurnContent {
    /// A text alone.
    Text(String),
    /// Parts in order: what a model that sees reads of an author's media
    /// (RFC §6.4).
    Parts(Vec<Part>),
}

impl TurnContent {
    /// Whether the content says nothing, so that the event reads as no
    /// turn: a blank text, or no part.
    pub(super) fn is_blank(&self) -> bool {
        match self {
            Self::Text(text) => text.trim().is_empty(),
            Self::Parts(parts) => parts.is_empty(),
        }
    }

    /// The texts the content holds, in order: its text, or its text parts.
    pub(super) fn texts(&self) -> Vec<String> {
        match self {
            Self::Text(text) => vec![text.clone()],
            Self::Parts(parts) => parts
                .iter()
                .filter_map(|part| match part {
                    Part::Text(text) => Some(text.clone()),
                    Part::Image(_) => None,
                })
                .collect(),
        }
    }

    /// The content with its texts replaced by `texts`, in order: what
    /// [`texts`](Self::texts) read, cleaned. A text part left empty is
    /// dropped.
    pub(super) fn with_texts(self, texts: impl IntoIterator<Item = String>) -> Self {
        let mut texts = texts.into_iter();
        match self {
            Self::Text(text) => Self::Text(texts.next().unwrap_or(text)),
            Self::Parts(parts) => Self::Parts(
                parts
                    .into_iter()
                    .filter_map(|part| match part {
                        Part::Text(text) => {
                            let text = texts.next().unwrap_or(text);
                            (!text.is_empty()).then_some(Part::Text(text))
                        }
                        Part::Image(image) => Some(Part::Image(image)),
                    })
                    .collect(),
            ),
        }
    }

    /// The content of a participant's turn under its author's `label`
    /// (RFC §6.4): each line of each text as `label: "line"`, and parts
    /// that open with an image or a blank text led by a part `label:`.
    /// Python's `_with_speaker_prefix`.
    pub(super) fn labelled(self, label: &str) -> Self {
        match self {
            Self::Text(text) => Self::Text(labelled_lines(&text, label)),
            Self::Parts(parts) => {
                let opens_with_words =
                    matches!(parts.first(), Some(Part::Text(text)) if !text.trim().is_empty());
                let lead = (!opens_with_words).then(|| Part::Text(format!("{label}:")));
                let labelled = parts.into_iter().map(|part| match part {
                    Part::Text(text) => Part::Text(labelled_lines(&text, label)),
                    Part::Image(image) => Part::Image(image),
                });
                Self::Parts(lead.into_iter().chain(labelled).collect())
            }
        }
    }

    /// The content with `text` after it (RFC §6.4, the turn's notes): a
    /// text a blank line after a text, a part of its own after parts.
    /// Python's `joined(message, text, before=False)`.
    pub(super) fn followed_by(self, text: &str) -> Self {
        match self {
            Self::Text(own) if own.is_empty() => Self::Text(text.to_owned()),
            Self::Text(own) => Self::Text(format!("{own}\n\n{text}")),
            Self::Parts(mut parts) => {
                parts.push(Part::Text(text.to_owned()));
                Self::Parts(parts)
            }
        }
    }
}
