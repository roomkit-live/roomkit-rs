//! What a tool's call answers (RFC §21.4), Python's `tools/result.py`: a
//! text, a JSON value, or content parts, a text and the images a model
//! that sees reads (RFC §6.4).

use serde_json::{Map, Value};

use crate::provider::{AiImagePart, AiTextPart};
use crate::skills::dumps_unescaped;

/// What stands for an image in a result's text: Python's
/// `AIToolResultPart.as_text`.
const IMAGE_MARK: &str = "[image]";

/// What a tool message says of a result of images alone, its images going
/// after it: Python's `split_for_message`.
const IMAGE_BELOW: &str = "[see image below]";

/// The URL characters of images a call's end row keeps: Python's
/// `TOOL_EVENT_IMAGE_MAX_CHARS`.
const ROW_IMAGE_MAX_CHARS: usize = 512 * 1024;

/// What a tool's call answers with (RFC §21.4): Python's `ToolResult`,
/// `str | list[AITextPart | AIImagePart]`. A text the model reads as it
/// is; a JSON value it reads as its JSON, as Python dumps a mapping, a
/// list of mappings each in a part's exact shape read as those parts; or
/// parts in order, a text and its images.
///
/// ```
/// use roomkit_ai::{AiImagePart, AiTextPart, ToolResult, ToolResultPart};
///
/// let screenshot = ToolResult::Parts(vec![
///     ToolResultPart::Text(AiTextPart::new("the page")),
///     ToolResultPart::Image(AiImagePart {
///         url: "data:image/png;base64,iVBORw0KGgo=".into(),
///         mime_type: Some("image/png".into()),
///     }),
/// ]);
/// assert_eq!(screenshot.as_text(), "the page\n[image]");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolResult {
    /// Text.
    Text(String),
    /// A JSON value.
    Json(Value),
    /// Parts in order: Python's `list[AITextPart | AIImagePart]`.
    Parts(Vec<ToolResultPart>),
}

/// One part of a tool's result: Python's `AITextPart | AIImagePart`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolResultPart {
    /// A text.
    Text(AiTextPart),
    /// An image (RFC §6.4).
    Image(AiImagePart),
}

impl ToolResult {
    /// The result as a reader that takes no image reads it: a text as it
    /// is, a JSON value as its JSON, parts as their texts and an `[image]`
    /// mark for each image, one per line. Python's
    /// `AIToolResultPart.as_text`.
    #[must_use]
    pub fn as_text(&self) -> String {
        match self {
            Self::Text(text) => text.clone(),
            Self::Json(value) => match as_parts(value) {
                Some(parts) => Self::Parts(parts).as_text(),
                None => dumps_unescaped(value),
            },
            Self::Parts(parts) => parts
                .iter()
                .map(|part| match part {
                    ToolResultPart::Text(part) => part.text.as_str(),
                    ToolResultPart::Image(_) => IMAGE_MARK,
                })
                .collect::<Vec<_>>()
                .join("\n"),
        }
    }

    /// The text a tool message keeps and the images that go after it:
    /// the texts joined, one per line, or `[see image below]` for images
    /// alone. A text or a JSON value has no image. Python's
    /// `AIToolResultPart.split_for_message`.
    #[must_use]
    pub fn split_for_message(&self) -> (String, Vec<AiImagePart>) {
        let parts = match self {
            Self::Parts(parts) => parts.clone(),
            Self::Json(value) => match as_parts(value) {
                Some(parts) => parts,
                None => return (self.as_text(), Vec::new()),
            },
            Self::Text(_) => return (self.as_text(), Vec::new()),
        };
        let mut texts = Vec::new();
        let mut images = Vec::new();
        for part in parts {
            match part {
                ToolResultPart::Text(part) => texts.push(part.text),
                ToolResultPart::Image(image) => images.push(image),
            }
        }
        let text = match (texts.is_empty(), images.is_empty()) {
            (true, false) => IMAGE_BELOW.to_owned(),
            _ => texts.join("\n"),
        };
        (text, images)
    }

    /// The result with a JSON value that is a list of mappings each in a
    /// part's exact shape read as those parts, as Python's
    /// `as_tool_result` reads a handler's answer; any other as it is.
    pub(crate) fn with_parts_read(self) -> Self {
        match self {
            Self::Json(value) => match as_parts(&value) {
                Some(parts) => Self::Parts(parts),
                None => Self::Json(value),
            },
            Self::Text(_) | Self::Parts(_) => self,
        }
    }

    /// The result as its hooks and its end row read it: a text as a JSON
    /// string, parts each as Python's model dumps them, `{"type": "text",
    /// "text"}` or `{"type": "image", "url", "mime_type"}`.
    pub(crate) fn into_value(self) -> Value {
        match self {
            Self::Text(text) => Value::String(text),
            Self::Json(value) => value,
            Self::Parts(parts) => Value::Array(parts.into_iter().map(part_value).collect()),
        }
    }

    /// The result as the room's end row keeps it: each image kept while
    /// the images kept so far, its own included, stay within 512 KiB of
    /// URL, else a note naming its type and size, a later, smaller one
    /// still kept. Python's `tool_event_payload` on a result of parts.
    pub(crate) fn into_row_value(self) -> Value {
        let Self::Parts(parts) = self else {
            return self.into_value();
        };
        let mut used = 0;
        let kept = parts
            .into_iter()
            .map(|part| match part {
                ToolResultPart::Image(image) => {
                    let size = image.url.chars().count();
                    if used + size <= ROW_IMAGE_MAX_CHARS {
                        used += size;
                        return ToolResultPart::Image(image);
                    }
                    let note = row_note(image.mime_type.as_deref(), size);
                    ToolResultPart::Text(AiTextPart::new(note))
                }
                ToolResultPart::Text(_) => part,
            })
            .collect();
        Self::Parts(kept).into_value()
    }

    /// The images among the result's parts, in order.
    pub(crate) fn images(&self) -> Vec<&AiImagePart> {
        match self {
            Self::Parts(parts) => parts
                .iter()
                .filter_map(|part| match part {
                    ToolResultPart::Image(image) => Some(image),
                    ToolResultPart::Text(_) => None,
                })
                .collect(),
            Self::Text(_) | Self::Json(_) => Vec::new(),
        }
    }
}

impl From<String> for ToolResult {
    fn from(text: String) -> Self {
        Self::Text(text)
    }
}

impl From<&str> for ToolResult {
    fn from(text: &str) -> Self {
        Self::Text(text.to_owned())
    }
}

impl From<Value> for ToolResult {
    fn from(value: Value) -> Self {
        Self::Json(value)
    }
}

impl From<Vec<ToolResultPart>> for ToolResult {
    fn from(parts: Vec<ToolResultPart>) -> Self {
        Self::Parts(parts)
    }
}

/// `value`, a hook's or a handler's, as the model reads it: a text as it
/// is, parts when it is a list of mappings each in a part's exact shape,
/// else its JSON (RFC §21.4). Python's `as_tool_result`.
pub(crate) fn read(value: &Value) -> ToolResult {
    if let Value::String(text) = value {
        return ToolResult::Text(text.clone());
    }
    match as_parts(value) {
        Some(parts) => ToolResult::Parts(parts),
        None => ToolResult::Text(dumps_unescaped(value)),
    }
}

/// `value` as parts, when it is a non-empty list whose every item is a
/// mapping with a part's type and only that part's fields, each of its
/// type; `None` otherwise. Python's `_content_parts`.
fn as_parts(value: &Value) -> Option<Vec<ToolResultPart>> {
    let Value::Array(items) = value else {
        return None;
    };
    if items.is_empty() {
        return None;
    }
    items.iter().map(as_part).collect()
}

/// `item` as a part: a mapping of a part's type with only its fields, as
/// pydantic validates them (a missing `mime_type` is none).
fn as_part(item: &Value) -> Option<ToolResultPart> {
    let Value::Object(fields) = item else {
        return None;
    };
    let only = |names: &[&str]| fields.keys().all(|key| names.contains(&key.as_str()));
    match fields.get("type")?.as_str()? {
        "text" if only(&["type", "text"]) => {
            let text = fields.get("text")?.as_str()?;
            Some(ToolResultPart::Text(AiTextPart::new(text)))
        }
        "image" if only(&["type", "url", "mime_type"]) => {
            let url = fields.get("url")?.as_str()?.to_owned();
            let mime_type = match fields.get("mime_type") {
                None | Some(Value::Null) => None,
                Some(Value::String(kind)) => Some(kind.clone()),
                Some(_) => return None,
            };
            Some(ToolResultPart::Image(AiImagePart { url, mime_type }))
        }
        _ => None,
    }
}

/// The note an end row keeps in place of an image of `mime` and `size` URL
/// characters: Python's `_note`, its size decoded, in KB.
fn row_note(mime: Option<&str>, size: usize) -> String {
    let label = match mime {
        Some(mime) if !mime.is_empty() => format!("image {mime}"),
        _ => "image".to_owned(),
    };
    let decoded_kb = (size * 3 / 4 / 1024).max(1);
    format!("[{label}, {decoded_kb} KB, not kept in the event]")
}

/// `part` as Python's model dumps it.
fn part_value(part: ToolResultPart) -> Value {
    let mut fields = Map::new();
    match part {
        ToolResultPart::Text(part) => {
            fields.insert("type".to_owned(), Value::from("text"));
            fields.insert("text".to_owned(), Value::String(part.text));
        }
        ToolResultPart::Image(image) => {
            fields.insert("type".to_owned(), Value::from("image"));
            fields.insert("url".to_owned(), Value::String(image.url));
            fields.insert(
                "mime_type".to_owned(),
                image.mime_type.map_or(Value::Null, Value::String),
            );
        }
    }
    Value::Object(fields)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::{ToolResult, ToolResultPart, read};
    use crate::provider::{AiImagePart, AiTextPart};

    fn image(url: &str) -> ToolResultPart {
        ToolResultPart::Image(AiImagePart::new(url))
    }

    // Python: tests/test_tool_results.py::test_content_parts_given_as_mappings_stay_content_parts
    // and test_a_list_of_plain_mappings_is_json.
    #[test]
    fn content_parts_given_as_mappings_stay_content_parts() {
        let parts = read(&json!([
            { "type": "text", "text": "a screenshot" },
            { "type": "image", "url": "data:image/png;base64,AAAA" },
        ]));
        assert_eq!(
            parts,
            ToolResult::Parts(vec![
                ToolResultPart::Text(AiTextPart::new("a screenshot")),
                image("data:image/png;base64,AAAA"),
            ])
        );
        assert_eq!(
            read(&json!([{ "id": 1 }])),
            ToolResult::Text("[{\"id\": 1}]".into())
        );
        // A field a part does not have, a field of the wrong type, no item.
        for value in [
            json!([{ "type": "text", "text": "x", "extra": 1 }]),
            json!([{ "type": "image", "url": 3 }]),
            json!([]),
        ] {
            assert!(matches!(read(&value), ToolResult::Text(_)), "{value}");
        }
    }

    fn sized(kb: usize) -> ToolResultPart {
        let header = "data:image/png;base64,";
        ToolResultPart::Image(AiImagePart {
            url: format!("{header}{}", "A".repeat(kb * 1024 - header.len())),
            mime_type: Some("image/png".to_owned()),
        })
    }

    // Python: tests/test_tool_event_result.py::TestToolEventResult::test_images_are_kept_while_they_fit_and_noted_past_the_bound
    // and test_a_result_under_the_bound_is_unchanged.
    #[test]
    fn a_row_keeps_images_while_they_fit_and_notes_the_rest() {
        let page = ToolResultPart::Text(AiTextPart::new("page"));
        let parts = vec![page.clone(), sized(300), sized(300), sized(100)];
        let kept = ToolResult::Parts(parts.clone()).into_row_value();
        let note = "[image image/png, 225 KB, not kept in the event]";
        let expected = ToolResult::Parts(vec![
            page.clone(),
            parts[1].clone(),
            ToolResultPart::Text(AiTextPart::new(note)),
            parts[3].clone(),
        ]);
        assert_eq!(kept, expected.into_value());
        let under = ToolResult::Parts(vec![page, sized(200)]);
        assert_eq!(under.clone().into_row_value(), under.into_value());
    }

    // Python: tests/test_providers/test_anthropic.py::test_as_text_flattens_parts
    // and test_split_for_message_separates_text_and_images.
    #[test]
    fn a_result_splits_into_its_text_and_its_images() {
        let both = ToolResult::Parts(vec![
            ToolResultPart::Text(AiTextPart::new("a")),
            image("data:image/png;base64,AAAA"),
            ToolResultPart::Text(AiTextPart::new("b")),
        ]);
        assert_eq!(both.as_text(), "a\n[image]\nb");
        let (text, images) = both.split_for_message();
        assert_eq!((text.as_str(), images.len()), ("a\nb", 1));
        let alone = ToolResult::Parts(vec![image("data:image/png;base64,AAAA")]);
        assert_eq!(alone.split_for_message().0, "[see image below]");
        assert_eq!(
            ToolResult::from("t").split_for_message(),
            ("t".into(), vec![])
        );
    }
}
