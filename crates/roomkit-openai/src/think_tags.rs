//! A model's reasoning as OpenAI-compatible servers write it (RFC §6.7):
//! inside the `<think>` block that opens the answer, or in a field beside
//! `content`.
//! Python's `ThinkTagParser`, `extract_think_tags`, `field_reasoning` and
//! `merge_thinking`.

use serde_json::Value;

/// A piece of an answer's text, as its tags class it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Segment {
    /// Text inside `<think>` tags, or from a reasoning field.
    Thinking(String),
    /// The answer's text.
    Text(String),
}

const OPEN: &str = "<think>";
const CLOSE: &str = "</think>";

/// What may stand before the block that opens a generation: spaces, tabs
/// and line breaks, which stay the answer's (Python's `_LEADING_SPACE`).
const LEADING_SPACE: [char; 4] = [' ', '\t', '\r', '\n'];

/// Where a generation's text stands against the block that may open it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Place {
    /// Nothing but leading spaces yet: a `<think>` here opens the block.
    #[default]
    Start,
    /// Inside the opening block, up to its `</think>`.
    Block,
    /// The answer, a `<think>` it writes included.
    Answer,
}

/// Classes one generation's text stream into [`Segment`]s (RFC §6.7): only
/// a block that opens the generation, after nothing but spaces, tabs and
/// line breaks, is reasoning; once the text says anything else, or the block
/// closes, the rest is the answer. A tag split across chunks reads as it does
/// whole: a start that may still become `<think>`, and a suffix that may
/// start `</think>`, wait for the next chunk.
#[derive(Debug, Default)]
pub(crate) struct ThinkTagParser {
    place: Place,
    held: String,
}

impl ThinkTagParser {
    /// What `chunk` settles, in order.
    pub(crate) fn feed(&mut self, chunk: &str) -> Vec<Segment> {
        self.held.push_str(chunk);
        let mut segments = Vec::new();
        if self.place == Place::Start {
            self.read_start(&mut segments);
        }
        if self.place == Place::Block {
            self.read_block(&mut segments);
        }
        if self.place == Place::Answer {
            push(&mut segments, Segment::Text(std::mem::take(&mut self.held)));
        }
        segments
    }

    /// Settles whether the generation opens with a block: its leading
    /// spaces are text, and a start that may still become the tag is held.
    fn read_start(&mut self, segments: &mut Vec<Segment>) {
        let spaces = self.held.len() - self.held.trim_start_matches(LEADING_SPACE).len();
        push(segments, Segment::Text(self.held.drain(..spaces).collect()));
        if self.held.starts_with(OPEN) {
            self.held.drain(..OPEN.len());
            self.place = Place::Block;
        } else if !OPEN.starts_with(self.held.as_str()) {
            self.place = Place::Answer;
        }
    }

    /// Hands on the block's reasoning up to its close, holding a suffix
    /// that may start the close.
    fn read_block(&mut self, segments: &mut Vec<Segment>) {
        if let Some(at) = self.held.find(CLOSE) {
            push(segments, Segment::Thinking(self.held.drain(..at).collect()));
            self.held.drain(..CLOSE.len());
            self.place = Place::Answer;
            return;
        }
        let settled = self.held.len() - partial_tag_len(&self.held, CLOSE);
        push(
            segments,
            Segment::Thinking(self.held.drain(..settled).collect()),
        );
    }

    /// What is still held once the stream is over: a start that never
    /// became the tag is text, the end of a block never closed reasoning.
    pub(crate) fn flush(&mut self) -> Vec<Segment> {
        let held = std::mem::take(&mut self.held);
        let mut segments = Vec::new();
        push(
            &mut segments,
            if self.place == Place::Block {
                Segment::Thinking(held)
            } else {
                Segment::Text(held)
            },
        );
        segments
    }
}

/// `segment` added to `segments`, unless it is empty.
fn push(segments: &mut Vec<Segment>, segment: Segment) {
    let (Segment::Thinking(text) | Segment::Text(text)) = &segment;
    if !text.is_empty() {
        segments.push(segment);
    }
}

/// The length of the longest suffix of `text` that starts `tag`.
fn partial_tag_len(text: &str, tag: &str) -> usize {
    let longest = text.len().min(tag.len() - 1);
    (1..=longest)
        .rev()
        .find(|&length| tag.get(..length).is_some_and(|start| text.ends_with(start)))
        .unwrap_or(0)
}

/// A whole answer's `text` split as [`ThinkTagParser`] splits a stream, so
/// that it reads the same streamed or not (RFC §6.4, §6.7): a block it
/// stopped before closing is reasoning too, and a tag the answer writes is
/// answer. The reasoning is `None` for no opening block or an empty one.
pub(crate) fn extract_think_tags(text: &str) -> (Option<String>, String) {
    if !text.contains(OPEN) {
        return (None, text.to_owned());
    }
    let mut parser = ThinkTagParser::default();
    let mut segments = parser.feed(text);
    segments.extend(parser.flush());
    let (mut thinking, mut answer) = (String::new(), String::new());
    for segment in segments {
        match segment {
            Segment::Thinking(piece) => thinking.push_str(&piece),
            Segment::Text(piece) => answer.push_str(&piece),
        }
    }
    (Some(thinking).filter(|read| !read.is_empty()), answer)
}

/// The reasoning a delta or a message carries beside its `content`:
/// `reasoning_content` (DeepSeek, Qwen, vLLM's parsers), else `reasoning`
/// (OpenRouter), a value that is no text read as none (Python's
/// `field_reasoning`).
pub(crate) fn field_reasoning(
    reasoning_content: Option<&Value>,
    reasoning: Option<&Value>,
) -> Option<String> {
    let first = reasoning_content.filter(|value| is_truthy(value));
    let value = first.or_else(|| reasoning.filter(|value| is_truthy(value)))?;
    value.as_str().map(str::to_owned)
}

/// Whether Python's `or` keeps `value`.
fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64() != Some(0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(fields) => !fields.is_empty(),
    }
}

/// The two conventions joined: a server uses one, and neither is dropped
/// if it emits both (Python's `merge_thinking`).
pub(crate) fn merge_thinking(inline: Option<String>, field: Option<String>) -> Option<String> {
    match (inline, field) {
        (inline, None) => inline,
        (Some(inline), Some(field)) => Some(inline + &field),
        (None, field) => field,
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    fn text(text: &str) -> Segment {
        Segment::Text(text.to_owned())
    }

    /// What `chunks` give joined per kind: the reasoning, then the text.
    fn read(chunks: &[&str]) -> (String, String) {
        let mut parser = ThinkTagParser::default();
        let mut segments: Vec<Segment> = chunks.iter().flat_map(|c| parser.feed(c)).collect();
        segments.extend(parser.flush());
        let (mut reasoning, mut answer) = (String::new(), String::new());
        for segment in segments {
            match segment {
                Segment::Thinking(piece) => reasoning.push_str(&piece),
                Segment::Text(piece) => answer.push_str(&piece),
            }
        }
        (reasoning, answer)
    }

    // Python: tests/test_providers/test_openai.py::TestThinkTagParser::test_no_tags
    #[test]
    fn no_tags() {
        let mut parser = ThinkTagParser::default();
        assert_eq!(parser.feed("hello world"), [text("hello world")]);
        assert_eq!(parser.flush(), []);
    }

    // Python: tests/test_providers/test_openai.py::TestThinkTagParser::test_empty_think_block
    #[test]
    fn empty_think_block() {
        let mut parser = ThinkTagParser::default();
        assert_eq!(parser.feed("<think></think>answer"), [text("answer")]);
    }

    // Python: tests/test_providers/test_openai.py::TestThinkTagParser::test_multiple_small_chunks
    #[test]
    fn multiple_small_chunks() {
        let chars: Vec<String> = "<think>abc</think>xyz".chars().map(String::from).collect();
        let chunks: Vec<&str> = chars.iter().map(String::as_str).collect();
        assert_eq!(read(&chunks), ("abc".to_owned(), "xyz".to_owned()));
    }

    // Python: tests/test_providers/test_openai.py::TestExtractThinkTags (each of its tests)
    #[test]
    fn extract_think_tags_reads_a_response_as_its_stream_reads() {
        let cases = [
            ("plain response", None, "plain response"),
            (
                "<think>Let me think</think>The answer is 42.",
                Some("Let me think"),
                "The answer is 42.",
            ),
            (
                "<think>Step 1: analyze\nStep 2: solve</think>Done.",
                Some("Step 1: analyze\nStep 2: solve"),
                "Done.",
            ),
            ("<think></think>answer", None, "answer"),
            ("<think>  \n  </think>answer", Some("  \n  "), "answer"),
            // tests/text_conformance/test_responses.py::test_a_think_block_the_cap_cut_is_reasoning
            ("<think>still weighing", Some("still weighing"), ""),
            // ::test_a_think_block_reads_the_same_streamed_or_not
            (
                "<think> weighing it \n</think>\n\nThe answer.  ",
                Some(" weighing it \n"),
                "\n\nThe answer.  ",
            ),
        ];
        for (whole, reasoning, answer) in cases {
            let expected = (reasoning.map(str::to_owned), answer.to_owned());
            assert_eq!(extract_think_tags(whole), expected);
        }
    }

    // Python: tests/test_providers/test_openai.py::TestThinkTagParser::test_where_a_think_tag_stands
    // (RMK-1327): one generation's tags by where they stand, streamed in
    // chunks and read whole alike.
    #[test]
    fn where_a_think_tag_stands() {
        let bom = '\u{feff}';
        let after_a_bom = format!("{bom}<think>a</think>answer");
        let cases: [(&[&str], Option<&str>, &str); 12] = [
            // A tag the answer writes.
            (
                &["The parser reads the <think> tag at the start. Everything after is lost."],
                None,
                "The parser reads the <think> tag at the start. Everything after is lost.",
            ),
            // An open block alone.
            (&["<think>weighing"], Some("weighing"), ""),
            // A block at the start.
            (&["<think>a</think>answer"], Some("a"), "answer"),
            // Whitespace before the block.
            (
                &[" \t\r\n<think>a</think>answer"],
                Some("a"),
                " \t\r\nanswer",
            ),
            // Whitespace, then a split tag.
            (&["\n", "<th", "ink>a</think>b"], Some("a"), "\nb"),
            // Another tag at the start.
            (
                &["<thinking>x</thinking>y"],
                None,
                "<thinking>x</thinking>y",
            ),
            // A BOM before the block.
            (&[&after_a_bom], None, &after_a_bom),
            // A tag split at the start.
            (&["<thi", "nk>a</th", "ink>answer"], Some("a"), "answer"),
            // A tag split in the answer.
            (
                &["The parser reads the <thi", "nk> tag."],
                None,
                "The parser reads the <think> tag.",
            ),
            // A second block after the answer.
            (
                &["<think>a</think>answer <think>b</think> more"],
                Some("a"),
                "answer <think>b</think> more",
            ),
            // A second block right after the first.
            (
                &["<think>a</think><think>b</think>answer"],
                Some("a"),
                "<think>b</think>answer",
            ),
            // A second block after whitespace.
            (
                &["<think>a</think>\n\n<think>b</think>answer"],
                Some("a"),
                "\n\n<think>b</think>answer",
            ),
        ];
        for (chunks, reasoning, answer) in cases {
            let expected = (reasoning.map(str::to_owned), answer.to_owned());
            let (streamed, said) = read(chunks);
            let streamed = Some(streamed).filter(|read| !read.is_empty());
            assert_eq!((streamed, said), expected, "streamed: {chunks:?}");
            assert_eq!(
                extract_think_tags(&chunks.concat()),
                expected,
                "whole: {chunks:?}"
            );
        }
    }

    // Python: tests/test_providers/test_openai.py::TestThinkTagParser::test_a_start_that_cannot_be_the_tag_streams_at_once
    // (RMK-1327): only a start that may still become `<think>` waits.
    #[test]
    fn a_start_that_cannot_be_the_tag_streams_at_once() {
        let mut parser = ThinkTagParser::default();
        assert_eq!(parser.feed("<"), []);
        assert_eq!(parser.feed("b>bold"), [text("<b>bold")]);
        assert_eq!(parser.feed(" <"), [text(" <")]);
    }

    // Python: providers/ai/openai_dialect.py ThinkTagParser, which works on
    // characters: a multi-byte text around a tag stays whole.
    #[test]
    fn a_multi_byte_text_splits_at_its_tags() {
        let read = read(&["<think>é", "è</think>ç<"]);
        assert_eq!(read, ("éè".to_owned(), "ç<".to_owned()));
    }

    // Python: providers/ai/openai_dialect.py field_reasoning, merge_thinking
    // (test_generate_reads_openrouter_reasoning_field,
    // test_generate_merges_both_reasoning_conventions).
    #[test]
    fn a_reasoning_field_reads_as_python_reads_it() {
        let (first, second) = (json!("a"), json!("b"));
        assert_eq!(
            field_reasoning(Some(&first), Some(&second)).as_deref(),
            Some("a")
        );
        assert_eq!(field_reasoning(None, Some(&second)).as_deref(), Some("b"));
        let empty = json!("");
        assert_eq!(
            field_reasoning(Some(&empty), Some(&second)).as_deref(),
            Some("b")
        );
        // A truthy value that is no text stops the reading.
        let object = json!({ "tokens": 3 });
        assert_eq!(field_reasoning(Some(&object), Some(&second)), None);
        let merged = merge_thinking(Some("inline.".to_owned()), Some("field.".to_owned()));
        assert_eq!(merged.as_deref(), Some("inline.field."));
    }
}
