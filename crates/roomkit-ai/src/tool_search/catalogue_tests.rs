//! Python: `tests/test_tool_search_ranking.py`, `tests/test_related_family_tools.py`,
//! `tests/test_realtime_tool_search.py`'s pure helpers: what a scenario cannot say.

use std::collections::HashSet;

use pretty_assertions::assert_eq;
use serde_json::{Value, json};

use super::*;
use crate::tools::AiTool;

fn tool(name: &str, description: &str, tags: &[&str]) -> AiTool {
    AiTool {
        name: name.to_owned(),
        description: description.to_owned(),
        parameters: json!({}),
        tags: tags.iter().map(|tag| (*tag).to_owned()).collect(),
    }
}

/// A source served as one tool per action, each tagged with the source's words.
fn source(prefix: &str, actions: &[&str], tags: &[&str]) -> Vec<AiTool> {
    actions
        .iter()
        .map(|action| {
            let mut words = action.replace('_', " ");
            words.get_mut(..1).map(str::make_ascii_uppercase);
            let tagged: Vec<&str> = tags.iter().copied().chain([*action]).collect();
            tool(&format!("{prefix}_{action}"), &format!("{words}."), &tagged)
        })
        .collect()
}

const MAIL_ACTIONS: [&str; 5] = [
    "search_messages",
    "send_message",
    "read_message",
    "list_messages",
    "move_message",
];
const MAIL_TAGS: [&str; 6] = ["mail", "message", "search", "send", "read", "inbox"];

fn mail_catalogue() -> Vec<AiTool> {
    let mut catalogue = source("acmemail", &MAIL_ACTIONS, &MAIL_TAGS);
    catalogue.extend(source("globexmail", &MAIL_ACTIONS, &MAIL_TAGS));
    catalogue.extend(source(
        "calendar",
        &["list_events", "create_event"],
        &["calendar", "event"],
    ));
    catalogue.extend(source(
        "boards",
        &["create_card", "move_card"],
        &["board", "card", "kanban"],
    ));
    catalogue.push(tool("weather", "Get the forecast.", &[]));
    catalogue.push(tool(
        "search_messages",
        "Look through the local archive.",
        &[],
    ));
    catalogue
}

fn search<'a>(catalogue: &'a [AiTool], query: &str, max: usize) -> Vec<&'a str> {
    search_catalogue(catalogue, query, max, &HashSet::new())
        .into_iter()
        .map(|tool| tool.name.as_str())
        .collect()
}

// Python: tests/test_tool_search_ranking.py::TestExactName::test_the_named_tool_comes_first_ahead_of_richer_matches
#[test]
fn the_named_tool_comes_first_ahead_of_richer_matches() {
    let catalogue = mail_catalogue();
    assert_eq!(
        search(&catalogue, "search_messages", 5).first(),
        Some(&"search_messages")
    );
}

// Python: tests/test_tool_search_ranking.py::TestExactName::test_the_name_is_compared_without_case_or_surrounding_space
#[test]
fn the_name_is_compared_without_case_or_surrounding_space() {
    let catalogue = mail_catalogue();
    assert_eq!(
        search(&catalogue, "  Search_Messages ", 5).first(),
        Some(&"search_messages")
    );
}

// Python: tests/test_tool_search_ranking.py::TestExactName::test_it_holds_the_single_place_a_one_result_search_has
#[test]
fn it_holds_the_single_place_a_one_result_search_has() {
    let catalogue = mail_catalogue();
    assert_eq!(
        search(&catalogue, "search_messages", 1),
        vec!["search_messages"]
    );
}

// Python: tests/test_tool_search_ranking.py::TestExactName::test_an_excluded_tool_is_never_named_even_by_its_name
#[test]
fn an_excluded_tool_is_never_named_even_by_its_name() {
    let catalogue = mail_catalogue();
    let exclude = HashSet::from(["search_messages".to_owned()]);
    let ranked = search_catalogue(&catalogue, "search_messages", 5, &exclude);
    assert!(ranked.iter().all(|tool| tool.name != "search_messages"));
}

// Python: str.casefold(), which folds these to ASCII letters where lower() does not
#[test]
fn a_name_is_compared_as_python_folds_case() {
    let catalogue = [
        tool("strasse_search", "An unrelated word.", &[]),
        tool("other", "Strasse search strasse.", &[]),
    ];
    assert_eq!(
        search(&catalogue, "STRAẞE_SEARCH", 5).first(),
        Some(&"strasse_search")
    );
}

// Python: tests/test_tool_search_ranking.py::TestAWordCountsOnce::test_tags_repeating_the_name_add_nothing
#[test]
fn tags_repeating_the_name_add_nothing() {
    let tagged = tool(
        "alpha_lookup_order",
        "Look up an order.",
        &["lookup", "order"],
    );
    let untagged = tool("beta_lookup_order", "Look up an order.", &[]);
    for twins in [
        [tagged.clone(), untagged.clone()],
        [untagged.clone(), tagged.clone()],
    ] {
        let mut catalogue = twins.to_vec();
        catalogue.extend(source("calendar", &["list_events"], &["calendar"]));
        catalogue.push(tool("weather", "", &[]));
        assert_eq!(
            search(&catalogue, "lookup order", 5)[..2],
            [twins[0].name.as_str(), twins[1].name.as_str()]
        );
    }
}

// Python: tests/test_tool_search_ranking.py::TestAWordCountsOnce::test_a_word_in_the_name_outweighs_it_as_a_source_wide_tag
#[test]
fn a_word_in_the_name_outweighs_it_as_a_source_wide_tag() {
    let actions = [
        "send_message",
        "read_message",
        "list_messages",
        "search_messages",
    ];
    let mut catalogue: Vec<AiTool> = actions
        .iter()
        .map(|action| {
            let tags: Vec<&str> = MAIL_TAGS.iter().copied().chain([*action]).collect();
            tool(&format!("acmemail_{action}"), "An Acme mail action.", &tags)
        })
        .collect();
    catalogue.extend(source("calendar", &["list_events"], &["calendar"]));
    catalogue.push(tool("weather", "Get the forecast.", &[]));
    assert_eq!(
        search(&catalogue, "search messages", 5).first(),
        Some(&"acmemail_search_messages")
    );
}

// Python: tests/test_tool_search_ranking.py::TestACatalogueAsItComes::test_a_verb_shared_by_many_names_stays_a_common_word
#[test]
fn a_verb_shared_by_many_names_stays_a_common_word() {
    let catalogue = [
        tool("get_weather", "Get the current weather for a city.", &[]),
        tool(
            "get_forecast",
            "Get the weather forecast for the next days.",
            &[],
        ),
        tool("get_time", "Get the current time in a timezone.", &[]),
        tool("get_stock_price", "Get the latest price of a stock.", &[]),
        tool("get_news", "Get the latest news headlines.", &[]),
        tool(
            "get_contact",
            "Get a contact's phone number and email.",
            &[],
        ),
        tool("send_sms", "Send an SMS to a phone number.", &[]),
        tool("send_email", "Send an email to an address.", &[]),
        tool("create_event", "Create a calendar event.", &[]),
        tool("list_events", "List the calendar events of a day.", &[]),
        tool("delete_event", "Delete a calendar event.", &[]),
        tool("search_web", "Search the web for a query.", &[]),
        tool("search_files", "Search the files of the workspace.", &[]),
        tool("list_files", "List the files in a directory.", &[]),
        tool("read_file", "Read the content of a file.", &[]),
    ];
    assert_eq!(
        search(&catalogue, "get the calendar events", 5).first(),
        Some(&"list_events")
    );
    assert_eq!(
        search(&catalogue, "get my files", 5)[..3],
        ["search_files", "list_files", "read_file"]
    );
}

// Python: the _CAMEL_BOUNDARY and _ACRONYM_BOUNDARY of channels/_tool_search.py
#[test]
fn a_name_splits_where_its_case_does() {
    assert_eq!(
        tokenize("SpotifySearch HTTPServer_v2 sendSMS été"),
        [
            "spotify", "search", "http", "server", "v2", "send", "sms", "été"
        ]
    );
}

// Python: the docstring of channels/_tool_search.py's fold()
#[test]
fn a_number_folds_but_never_a_short_word_or_a_doubled_s() {
    for (word, folded) in [
        ("workflows", "workflow"),
        ("rencontres", "rencontre"),
        ("entries", "entry"),
        ("matches", "match"),
        ("boxes", "box"),
        ("sms", "sms"),
        ("api", "api"),
        ("process", "process"),
        ("class", "class"),
    ] {
        assert_eq!(fold(word), folded);
    }
}

// Python: channels/_tool_search.py's normalize_max_results()
#[test]
fn max_results_is_clamped_or_read_as_the_default() {
    let cases = [
        (None, 5),
        (Some(json!(3)), 3),
        (Some(json!(3.7)), 3),
        (Some(json!(0)), 1),
        (Some(json!(-4)), 1),
        (Some(json!(500)), 20),
    ];
    for (raw, count) in cases {
        assert_eq!(normalize_max_results(raw.as_ref(), 20), count);
    }
}

// Python: tests/test_related_family_tools.py::TestRenderFindPayloadRelated::test_related_rendered_with_steering_note
#[test]
fn related_rendered_with_steering_note() {
    let found = tool("square_get-menu", "Menu", &[]);
    let written = render_find_payload(&[&found], None, &["square_create-cart".to_owned()]);
    let payload: Value = serde_json::from_str(&written).unwrap();
    assert_eq!(
        payload["related_tools_same_source"],
        json!(["square_create-cart"])
    );
    assert!(
        payload["_note"]
            .as_str()
            .unwrap()
            .contains("run find_tools for one of those")
    );
}

// Python: tests/test_related_family_tools.py::TestRenderFindPayloadRelated::test_no_related_keeps_payload_unchanged
#[test]
fn no_related_keeps_payload_unchanged() {
    let found = tool("square_get-menu", "Menu", &[]);
    let written = render_find_payload(&[&found], None, &[]);
    assert_eq!(
        written,
        render_find_payload(&[&found], Some("Unused on a match."), &[])
    );
    let payload: Value = serde_json::from_str(&written).unwrap();
    assert!(payload.get("related_tools_same_source").is_none());
}

// Python: tests/test_related_family_tools.py::TestRenderFindPayloadRelated::test_miss_keeps_the_miss_note_without_related
#[test]
fn miss_keeps_the_miss_note_without_related() {
    let written = render_find_payload(&[], None, &["square_x".to_owned()]);
    let payload: Value = serde_json::from_str(&written).unwrap();
    assert!(payload.get("related_tools_same_source").is_none());
    assert_eq!(
        payload["_note"],
        "No tools matched. Try a different query, or call list_tools to see all available tools."
    );
}

// Python: the family rule of channels/_tool_search.py's related_family_tools()
#[test]
fn a_family_names_only_tools_unmatched_and_not_excluded() {
    let catalogue: Vec<AiTool> = ["crm_a", "crm_b", "crm_pinned", "other_c", "plain"]
        .iter()
        .map(|name| tool(name, "", &[]))
        .collect();
    let exclude = HashSet::from(["crm_pinned".to_owned()]);
    let matched = catalogue.first().map(|tool| vec![tool]).unwrap();
    assert_eq!(
        related_family_tools(&catalogue, &matched, &exclude),
        ["crm_b"]
    );
    let plain = catalogue.last().map(|tool| vec![tool]).unwrap();
    assert_eq!(
        related_family_tools(&catalogue, &plain, &exclude),
        Vec::<String>::new()
    );
}

// Python: the bound of channels/_tool_search.py's related_family_tools()
#[test]
fn a_family_names_at_most_ten() {
    let catalogue: Vec<AiTool> = (0..15)
        .map(|at| tool(&format!("crm_{at}"), "", &[]))
        .collect();
    let matched = catalogue.first().map(|tool| vec![tool]).unwrap();
    assert_eq!(
        related_family_tools(&catalogue, &matched, &HashSet::new()).len(),
        10
    );
}

// Python: the cuts of channels/_tool_search.py's render_find_payload and render_list_payload
#[test]
fn a_description_is_cut_by_characters() {
    let long = tool("long", &"é".repeat(300), &[]);
    let found: Value = serde_json::from_str(&render_find_payload(&[&long], None, &[])).unwrap();
    assert_eq!(
        found["matches"][0]["description"]
            .as_str()
            .unwrap()
            .chars()
            .count(),
        200
    );
    let listed: Value = serde_json::from_str(&render_list_payload(
        std::slice::from_ref(&long),
        "",
        &HashSet::new(),
    ))
    .unwrap();
    assert_eq!(
        listed["tools"][0]["description"]
            .as_str()
            .unwrap()
            .chars()
            .count(),
        64
    );
}

// Python: channels/_tool_search.py's render_list_payload(limit=60), _spread()
#[test]
fn a_listing_past_its_bound_is_sampled_across_the_catalogue() {
    let catalogue: Vec<AiTool> = (0..120)
        .map(|at| tool(&format!("t{at}"), "", &[]))
        .collect();
    let listed: Value =
        serde_json::from_str(&render_list_payload(&catalogue, "", &HashSet::new())).unwrap();
    assert_eq!(listed["count"], 60);
    assert_eq!(listed["total"], 120);
    assert_eq!(listed["truncated"], true);
    assert_eq!(
        listed["_note"],
        "Showing 60 of 120 tools, sampled across the catalogue. Use category= to filter, or \
         prefer find_tools(query=...) for action."
    );
    assert_eq!(listed["tools"][0]["name"], "t0");
    assert_eq!(listed["tools"][59]["name"], "t118");
}

// Python: channels/_tool_search.py's _score_tool(), `score += 3 * w; score += w`
/// Two words in a tool's name and description against the same two in a tag:
/// half of the first is the second, but for the last bit of a sum Python adds up
/// term by term, which cuts the second here, under either order of the words.
#[test]
fn a_score_adds_up_term_by_term_as_python_s() {
    let catalogue = [
        tool("foo_bar", "foo bar", &[]),
        tool("beta", "", &["foo", "bar"]),
        tool("gamma", "bar", &[]),
        tool("n0", "", &[]),
        tool("n1", "", &[]),
        tool("n2", "", &[]),
    ];
    assert_eq!(search(&catalogue, "foo bar", 5), ["foo_bar"]);
}
