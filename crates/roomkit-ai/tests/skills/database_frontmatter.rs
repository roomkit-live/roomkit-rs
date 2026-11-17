//! An application reads a locally stored manifest as text, without a directory.

use roomkit_ai::parse_frontmatter;
use serde_json::json;

#[test]
fn standalone_frontmatter_preserves_unknown_fields_order_lists_and_body() {
    let (values, body) = parse_frontmatter(concat!(
        "\u{feff}---\ncustom: [z, a]\nallowed_tools: [read_*, write_*]\n",
        "other:\n  - a\n  - null\n  - b\nempty: []\nmissing: null\ncontext: worker\n---\n\n# Body\ntext\n"
    )).unwrap();
    assert_eq!(
        values.keys().map(String::as_str).collect::<Vec<_>>(),
        ["custom", "allowed_tools", "other", "empty", "context"]
    );
    assert_eq!(
        json!(values),
        json!({
            "custom": ["z", "a"], "allowed_tools": ["read_*", "write_*"],
            "other": ["a", "b"], "empty": [], "context": "worker"
        })
    );
    assert_eq!(body, "# Body\ntext");
}

#[test]
fn standalone_frontmatter_reads_a_fallback_glob_without_a_skill_name() {
    let (values, body) = parse_frontmatter(concat!(
        "---\ndescription: Refunds: process them\nallowed_tools: [*_admin, read_*]\n",
        "requires:\n  - search\n---\n Body "
    ))
    .unwrap();
    assert_eq!(
        json!(values),
        json!({
            "description": "Refunds: process them", "allowed_tools": ["*_admin","read_*"],
            "requires": ["search"]
        })
    );
    assert_eq!(body, "Body");
}

#[test]
fn standalone_frontmatter_checks_boundaries_before_returning_any_fields() {
    for (text, reason) in [
        (
            "body".to_owned(),
            "SKILL.md must start with '---' frontmatter delimiter",
        ),
        (
            "---\na: value".to_owned(),
            "SKILL.md missing closing '---' frontmatter delimiter",
        ),
        (
            format!("---\nx: {}\n---", "a".repeat(65536)),
            "the frontmatter is past 65536 characters",
        ),
        (
            format!("---\nx: {}{}\n---", "[".repeat(65), "]".repeat(65)),
            "the frontmatter nests too deep",
        ),
    ] {
        assert_eq!(parse_frontmatter(&text).unwrap_err().to_string(), reason);
    }
    assert!(parse_frontmatter("---\nx: &d word\ny: *d\n---").is_err());
    assert!(parse_frontmatter("---\nrequires: [[read]]\n---").is_err());
}

#[test]
fn fallback_applies_to_the_whole_document_and_keeps_repeated_gates() {
    let tail = "custom: [z, a]\nmissing: null\nnote: x # note\nallowed_tools: [first]\nallowed_tools: [second]\n---\n";
    let (simple, _) = parse_frontmatter(&format!(
        "---\ndescription: \"Refunds: process them\"\n{tail}"
    ))
    .unwrap();
    let (lines, _) =
        parse_frontmatter(&format!("---\ndescription: Refunds: process them\n{tail}")).unwrap();
    assert_eq!(simple["allowed_tools"], json!(["second"]));
    assert_eq!(simple["custom"], json!(["z", "a"]));
    assert_eq!(simple["note"], "x");
    assert!(!simple.contains_key("missing"));
    assert_eq!(lines["allowed_tools"], json!(["first", "second"]));
    assert_eq!(lines["custom"], "[z, a]");
    assert_eq!(lines["note"], "x # note");
    assert_eq!(lines["missing"], "null");
}

#[test]
fn fallback_keeps_empty_gates_and_appends_block_entries_verbatim() {
    let (fields, _) = parse_frontmatter(concat!(
        "---\ndescription: Refunds: expand\nrequires: null\nallowed_tools: first\n",
        "allowed_tools:\n- first\n- ''\n- later\nother:\n- ''\n---"
    ))
    .unwrap();
    assert_eq!(fields["requires"], json!([]));
    assert_eq!(fields["allowed_tools"], json!(["first", "first", "later"]));
    assert_eq!(fields["other"], json!([]));
}
