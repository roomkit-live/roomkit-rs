//! Python: what a scenario cannot say of `should_activate_tool_search` and of the
//! constructor: `tests/test_channels/test_stored_result_search.py`,
//! `tests/test_policy_served_exemption.py`, `tests/test_tool_search_names_export.py`.

use pretty_assertions::assert_eq;
use serde_json::json;

use super::*;

fn options() -> ToolSearchOptions {
    ToolSearchOptions {
        mode: None,
        pinned: Vec::new(),
        threshold: DEFAULT_TOOL_SEARCH_THRESHOLD,
        threshold_pct: DEFAULT_TOOL_SEARCH_THRESHOLD_PCT,
        miss_hint: None,
        threshold_tokens: Some(DEFAULT_TOOL_SEARCH_THRESHOLD_TOKENS),
    }
}

fn catalogue(count: usize) -> Vec<AiTool> {
    (0..count)
        .map(|at| AiTool {
            parameters: json!({ "type": "object", "properties": { "id": { "type": "string" } } }),
            ..AiTool::own(
                &format!("report_{at}"),
                &"Report on something with a long description of its filters. ".repeat(10),
                json!({}),
            )
        })
        .collect()
}

fn hides(options: ToolSearchOptions, tools: &[AiTool], window: Option<u64>) -> bool {
    let tools: Vec<&AiTool> = tools.iter().collect();
    ToolSearch::checked(options).unwrap().hides(&tools, window)
}

// Python: tests/test_channels/test_stored_result_search.py::test_a_catalogue_past_the_token_cap_goes_behind_tool_search
#[test]
fn a_catalogue_past_the_token_cap_goes_behind_tool_search() {
    let (small, large) = (catalogue(4), catalogue(80));
    let tokens = |tools: &[AiTool]| tools.iter().map(estimate_tool_tokens).sum::<usize>();
    assert!(tokens(&small) < 8000 && 8000 < tokens(&large));
    let no_cap = || ToolSearchOptions {
        threshold_tokens: None,
        ..options()
    };
    assert!(hides(options(), &large, Some(1_000_000)));
    assert!(!hides(options(), &small, Some(1_000_000)));
    assert!(!hides(no_cap(), &large, Some(1_000_000)));
    assert!(hides(options(), &large, None));
    assert!(!hides(options(), &small, None));
}

// Python: channels/_tool_search.py's `if window:`
#[test]
fn a_window_of_nothing_falls_back_to_the_tool_count() {
    let tools = catalogue(3);
    let counting = |threshold| ToolSearchOptions {
        threshold,
        ..options()
    };
    assert!(hides(counting(2), &tools, Some(0)));
    assert!(!hides(counting(3), &tools, Some(0)));
}

// Python: tests/test_channels/test_stored_result_search.py::test_the_token_cap_is_a_positive_number_or_none
#[test]
fn the_token_cap_is_a_positive_number_or_none() {
    let zero = ToolSearchOptions {
        threshold_tokens: Some(0),
        ..options()
    };
    assert!(matches!(
        ToolSearch::checked(zero),
        Err(AiError::ToolSearchThresholdTokens)
    ));
    let none = ToolSearchOptions {
        threshold_tokens: None,
        ..options()
    };
    assert!(ToolSearch::checked(none).is_ok());
}

// Python: tests/test_tool_search_names_export.py::test_the_names_are_exported_from_roomkit_and_roomkit_channels
#[test]
fn the_names_are_the_two_tools_tool_search_adds() {
    assert_eq!(
        TOOL_SEARCH_INFRA_TOOL_NAMES,
        [TOOL_FIND_TOOLS, TOOL_LIST_TOOLS]
    );
    assert_eq!(
        (TOOL_FIND_TOOLS, TOOL_LIST_TOOLS),
        ("find_tools", "list_tools")
    );
    let served: Vec<String> = search_tool_defs()
        .into_iter()
        .map(|tool| tool.name)
        .collect();
    assert_eq!(served, TOOL_SEARCH_INFRA_TOOL_NAMES);
}
