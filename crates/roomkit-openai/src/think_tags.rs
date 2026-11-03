//! A model's reasoning as OpenAI-compatible servers write it (RFC §6.7):
//! inside `<think>` tags before the answer, or in a field beside `content`.
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

/// Classes a text stream's chunks into [`Segment`]s, a tag split across
/// chunks included: a suffix that may start a tag waits for the next chunk.
#[derive(Debug, Default)]
pub(crate) struct ThinkTagParser {
    in_thinking: bool,
    held: String,
}

impl ThinkTagParser {
    /// What `chunk` settles, in order.
    pub(crate) fn feed(&mut self, chunk: &str) -> Vec<Segment> {
        self.held.push_str(chunk);
        let mut segments = Vec::new();
        while !self.held.is_empty() {
            let tag = if self.in_thinking { CLOSE } else { OPEN };
            if let Some(at) = self.held.find(tag) {
                let before = self.held.drain(..at).collect::<String>();
                self.held.drain(..tag.len());
                self.push(&mut segments, before);
                self.in_thinking = !self.in_thinking;
                continue;
            }
            let hold = partial_tag_len(&self.held, tag);
            let settled = self.held.len() - hold;
            let emit = self.held.drain(..settled).collect::<String>();
            self.push(&mut segments, emit);
            break;
        }
        segments
    }

    /// What is still held once the stream is over, a partial tag read as
    /// the text it was.
    pub(crate) fn flush(&mut self) -> Vec<Segment> {
        let held = std::mem::take(&mut self.held);
        let mut segments = Vec::new();
        self.push(&mut segments, held);
        segments
    }

    fn push(&self, segments: &mut Vec<Segment>, text: String) {
        if text.is_empty() {
            return;
        }
        segments.push(if self.in_thinking {
            Segment::Thinking(text)
        } else {
            Segment::Text(text)
        });
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
/// that it reads the same streamed or not (RFC §6.4): a block it stopped
/// before closing is reasoning too. The reasoning is `None` for no tags or
/// an empty block.
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

    fn thinking(text: &str) -> Segment {
        Segment::Thinking(text.to_owned())
    }

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

    // Python: tests/test_providers/test_openai.py::TestThinkTagParser::test_complete_tag_single_chunk
    #[test]
    fn complete_tag_single_chunk() {
        let mut parser = ThinkTagParser::default();
        let segments = parser.feed("<think>reasoning</think>answer");
        assert_eq!(segments, [thinking("reasoning"), text("answer")]);
    }

    // Python: tests/test_providers/test_openai.py::TestThinkTagParser::test_tag_split_across_chunks
    #[test]
    fn tag_split_across_chunks() {
        let read = read(&["<thi", "nk>reason", "</think>ans"]);
        assert_eq!(read, ("reason".to_owned(), "ans".to_owned()));
    }

    // Python: tests/test_providers/test_openai.py::TestThinkTagParser::test_close_tag_split
    #[test]
    fn close_tag_split() {
        let read = read(&["<think>ok</th", "ink>done"]);
        assert_eq!(read, ("ok".to_owned(), "done".to_owned()));
    }

    // Python: tests/test_providers/test_openai.py::TestThinkTagParser::test_empty_think_block
    #[test]
    fn empty_think_block() {
        let mut parser = ThinkTagParser::default();
        assert_eq!(parser.feed("<think></think>answer"), [text("answer")]);
    }

    // Python: tests/test_providers/test_openai.py::TestThinkTagParser::test_only_thinking_no_text
    #[test]
    fn only_thinking_no_text() {
        let read = read(&["<think>just thinking"]);
        assert_eq!(read, ("just thinking".to_owned(), String::new()));
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
