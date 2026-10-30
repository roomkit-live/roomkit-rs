//! The line reader on each form the reference implementation's YAML reads
//! (`tasks/probes/step4-m8/skills_m8_probe.out`, "Frontmatter forms"), and
//! on each it refuses (RFC §24.1).

use pretty_assertions::assert_eq;

use super::*;

/// The description `frontmatter` gives after a valid name.
fn description(frontmatter: &str) -> Result<Option<String>, String> {
    read(format!("name: x\n{frontmatter}").trim_end()).map(|fields| fields.description)
}

#[test]
fn each_value_form_reads_as_yaml_reads_it() {
    let read = [
        (
            "description: |\n  Line one\n  line two",
            "Line one\nline two",
        ),
        (
            "description: |\n  Line one\n  line two\nlicense: MIT",
            "Line one\nline two\n",
        ),
        (
            "description: |-\n  one\n\n  two\nlicense: MIT",
            "one\n\ntwo",
        ),
        ("description: |+\n  one\n\nlicense: MIT", "one\n\n"),
        (
            "description: >-\n  Folded one\n  folded two",
            "Folded one folded two",
        ),
        ("description: >\n  one\n\n  two", "one\ntwo"),
        (
            "description: Wrapped plain\n  continued here",
            "Wrapped plain continued here",
        ),
        ("description: one\n\n  two", "one\ntwo"),
        (r#"description: "Say \"hi\" \u00e9""#, "Say \"hi\" \u{e9}"),
        ("description: 'It''s here'", "It's here"),
        ("description: text # a comment", "text"),
        ("description: C# guide", "C# guide"),
        ("description: Use when: asked", "Use when: asked"),
        ("description: *bold*", "*bold*"),
        ("description: [a, 'b', \"c\"]", "a, b, c"),
        ("description:\n  - a\n  # between\n  - 'b'", "a, b"),
        ("description:\n- a\n- b", "a, b"),
        ("description: \"x\" # done", "x"),
    ];
    for (frontmatter, expected) in read {
        assert_eq!(
            description(frontmatter),
            Ok(Some(expected.to_owned())),
            "{frontmatter:?}"
        );
    }
}

#[test]
fn a_null_or_no_value_gives_no_field() {
    for frontmatter in [
        "description: ~",
        "description: null",
        "description:",
        "description: # c",
    ] {
        assert_eq!(description(frontmatter), Ok(None), "{frontmatter:?}");
    }
}

#[test]
fn the_last_of_a_key_given_twice_holds() {
    assert_eq!(
        description("description: a\ndescription: b"),
        Ok(Some("b".to_owned()))
    );
}

/// The keys the registry does not read are not read: a nested map, a flow
/// list, a block, Agent Skills' `allowed-tools`, which gates nothing here.
#[test]
fn another_key_s_value_is_left_unread() {
    let frontmatter = "description: x\nmetadata:\n  author: acme\n  version: \"1.0\"\n\
                       tags: [a, b]\nnotes: |\n  anything: at all\n  - here\n\
                       custom-field: Bash(git:*)\nbroken: [\nitems:\n- one";
    assert_eq!(description(frontmatter), Ok(Some("x".to_owned())));
}

/// RFC §24.1: a skill that gates or requires tools is refused, whatever
/// its value, since no tool is gated here.
#[test]
fn a_gating_key_is_refused_whatever_its_value() {
    for frontmatter in [
        "allowed_tools: []",
        "allowed_tools:\n  - \"search_*\"",
        "requires: lookup",
        "\"requires\": lookup",
        "'allowed_tools' : x",
    ] {
        let refused = description(frontmatter).unwrap_err();
        assert!(refused.contains("declares"), "{frontmatter:?}: {refused}");
    }
}

/// RFC §24.1: a line where YAML may read a key this reading cannot name
/// is refused, so that no gating key hides there.
#[test]
fn a_line_that_may_hide_a_key_is_refused() {
    for frontmatter in [
        "? allowed_tools\n: x",
        "!!str allowed_tools: x",
        "&a allowed_tools: x",
        "*a : x",
        "<<: {allowed_tools: x}",
        r#""allowed_tools": x"#,
        "{allowed_tools: x}",
        "- allowed_tools",
        "key with spaces: x",
        "description: x\u{2028}allowed_tools: y",
        "description: x\rallowed_tools: y",
        "description: x\u{85}allowed_tools: y",
    ] {
        assert!(description(frontmatter).is_err(), "{frontmatter:?}");
    }
}

/// A field this reading cannot read whole is refused, never read in part.
#[test]
fn a_field_read_in_part_is_refused() {
    for frontmatter in [
        "description: \"opened\n  and closed\"",
        "description: 'opened",
        r#"description: "an \q escape""#,
        "description: \"x\" trailing",
        "description: |2\n  indented",
        "description: >\n  one\n    more indented",
        "description: |\n    four\n  two",
        "description: &anchor x",
        "description: !!str x",
        "description: {a: b}",
        "description: [a, [b]]",
        "description: [a, b",
        "description:\n  author: acme",
        "description:\n  - a\n    - b",
        "description:\n  - [a]",
        "description: one # comment\n  two",
        "description: one\n  key: value",
        "description: - x",
    ] {
        assert!(description(frontmatter).is_err(), "{frontmatter:?}");
    }
}

/// RFC §24.1: the bounds, each before anything is read.
#[test]
fn a_frontmatter_past_a_bound_is_refused() {
    let long = format!("description: {}", "d".repeat(FRONTMATTER_MAX));
    assert_eq!(
        read(&long),
        Err("the frontmatter is past 65536 characters".to_owned())
    );
    let deep = format!("description: x\nk: {}{}", "[".repeat(65), "]".repeat(65));
    assert_eq!(
        read(&deep),
        Err("the frontmatter nests too deep".to_owned())
    );
    let at_the_bound = format!("description: x\nk: {}{}", "[".repeat(64), "]".repeat(64));
    assert!(read(&at_the_bound).is_ok());
}

#[test]
fn the_frontmatter_lies_between_its_two_delimiters() {
    assert_eq!(
        split("\u{feff}---\nname: x\n---\n\n## Body\n\n"),
        Ok(("name: x", "## Body"))
    );
    assert_eq!(split("---\n---\nbody"), Ok(("", "body")));
    assert_eq!(
        split("name: x\n"),
        Err("SKILL.md must start with '---' frontmatter delimiter".to_owned())
    );
    assert_eq!(
        split("---\nname: x\n"),
        Err("SKILL.md missing closing '---' frontmatter delimiter".to_owned())
    );
}

#[test]
fn each_field_the_registry_reads_is_read() {
    let fields = read(
        "name: doc-helper\ndescription: d\nlicense: MIT\ncompatibility: Needs nothing\nversion: \"1.2\"",
    );
    assert_eq!(
        fields,
        Ok(Fields {
            name: Some("doc-helper".to_owned()),
            description: Some("d".to_owned()),
            license: Some("MIT".to_owned()),
            compatibility: Some("Needs nothing".to_owned()),
        })
    );
}
