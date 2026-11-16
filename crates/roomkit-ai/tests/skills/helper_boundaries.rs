//! Source witnesses for the rules shared by standalone and metadata helpers.

use roomkit_ai::{SkillMetadata, SkillToolNames, parse_tool_names};
use serde_json::json;

// Python: tests/skills/test_standalone_helpers.py::test_parse_tool_names_uses_python_whitespace
#[test]
fn test_parse_tool_names_uses_python_whitespace() {
    for space in [
        ' ', '\t', '\u{a0}', '\u{2003}', '\u{1c}', '\u{1d}', '\u{1e}', '\u{1f}',
    ] {
        assert_eq!(
            parse_tool_names(&json!(format!("{space}[{space}lookup{space}]{space}"))),
            ["lookup"],
            "{space:?}"
        );
        assert_eq!(
            parse_tool_names(&json!([
                format!("{space}'lookup'{space}"),
                space.to_string()
            ])),
            ["lookup"],
            "{space:?}"
        );
        let mut metadata = SkillMetadata::new("local", "");
        metadata.allowed_tools = Some(SkillToolNames::Text(format!(
            "{space}[{space}lookup{space}]{space}"
        )));
        assert_eq!(metadata.gated_tool_names(), ["lookup"], "{space:?}");
        metadata.allowed_tools = Some(SkillToolNames::List(vec![
            format!("{space}'lookup'{space}"),
            space.to_string(),
        ]));
        assert_eq!(metadata.gated_tool_names(), ["lookup"], "{space:?}");
    }
}
