//! Public block naming follows Python's `roomkit.tools` boundary (RFC §6.4).

use roomkit::tools::{FENCED_TAGS, named_blocks};

// Python: tests/tools/test_named_blocks.py::test_default_tags_replace_the_whole_block
#[test]
fn default_tags_replace_the_whole_block() {
    for tag in [
        "tool_result",
        "worker_output",
        "knowledge",
        "conversation_summary",
        "context",
        "task",
        "vision",
        "conversation",
        "task_context",
        "previous_tasks",
    ] {
        let text = format!("before <{tag}>untrusted text</{tag}> after");
        assert_eq!(named_blocks(&text, None), format!("before [{tag}] after"));
        assert_eq!(
            named_blocks(&text, Some(FENCED_TAGS)),
            format!("before [{tag}] after")
        );
    }
}

// Python: tests/tools/test_named_blocks.py::test_custom_tags_replace_only_the_selected_blocks
#[test]
fn custom_tags_replace_only_the_selected_blocks() {
    let text = "a <custom>secret</custom> b <tool_result>kept</tool_result> c";
    assert_eq!(
        named_blocks(text, Some(&["custom"])),
        "a [custom] b <tool_result>kept</tool_result> c"
    );
}

// Python: tests/tools/test_named_blocks.py::test_empty_tags_preserve_the_text
#[test]
fn empty_tags_preserve_the_text() {
    let text = "a <tool_result>kept</tool_result> b <custom>also kept";
    assert_eq!(named_blocks(text, Some(&[])), text);
}

// Python: tests/tools/test_named_blocks.py::test_custom_tags_are_applied_in_the_given_order
#[test]
fn custom_tags_are_applied_in_the_given_order() {
    for (tags, expected) in [
        (["CUSTOM", "custom"], "[CUSTOM]"),
        (["custom", "CUSTOM"], "[custom]"),
    ] {
        assert_eq!(named_blocks("<custom>x</custom>", Some(&tags)), expected);
    }
}
