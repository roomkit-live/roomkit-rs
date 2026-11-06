//! The store of large results, its preview and its reads, as Python's tests
//! of `ToolEviction` check them: `tests/test_tool_eviction_preview.py`,
//! `tests/test_tool_eviction_room_scope.py`,
//! `tests/test_channels/test_stored_result_search.py` and
//! `tests/test_channels/test_ai_tool_loop.py` (`TestToolResultTruncation`,
//! `TestEvictedResultReading`).

use pretty_assertions::assert_eq;
use serde_json::{Value, json};

use super::text::char_len;
use super::*;
use crate::provider::AiImagePart;

const PREVIEW_LABEL: &str = "\n\nPreview:\n";

/// The preview a placeholder carries.
fn preview_of(placeholder: &str) -> &str {
    placeholder
        .split_once(PREVIEW_LABEL)
        .map_or("", |(_, preview)| preview)
}

/// Python's documented bound: 8000 characters, or half of what is stored.
fn budget(threshold: usize) -> usize {
    8000.min(2 * threshold)
}

/// `result` bounded in room `room` by a store of `threshold` tokens, and
/// the store.
fn evicted(result: &str, threshold: u64) -> (ToolEviction, String) {
    let eviction = ToolEviction::new(threshold);
    let out = eviction.maybe_evict("", result.to_owned(), "tc1");
    (eviction, out)
}

impl ToolEviction {
    /// The text `room` stores under `id`.
    fn stored(&self, room: &str, id: &str) -> Option<String> {
        let store = self.lock();
        store
            .position(room, id)
            .map(|at| store.results[at].1.to_string())
    }
}

/// The JSON the model reads of `read_stored_result` with `arguments` in
/// `room`: the page, or the refusal of an id the room does not hold.
fn answer(eviction: &ToolEviction, room: &str, arguments: &Value) -> Value {
    let arguments = arguments.as_object().cloned().unwrap_or_default();
    let read = eviction
        .read(room, &arguments)
        .unwrap_or_else(|refused| refused);
    serde_json::from_str(&read).unwrap()
}

/// The id `placeholder` names.
fn id_of(placeholder: &str) -> &str {
    placeholder.split('\'').nth(1).unwrap_or_default()
}

fn image() -> ToolResultPart {
    let mut image = AiImagePart::new(format!("data:image/png;base64,{}", "A".repeat(400_000)));
    image.mime_type = Some("image/png".to_owned());
    ToolResultPart::Image(image)
}

fn text(text: &str) -> ToolResultPart {
    ToolResultPart::Text(AiTextPart::new(text.to_owned()))
}

// Python: tests/test_tool_eviction_preview.py::TestGiantLine::test_giant_line_in_a_long_result_is_clipped
#[test]
fn giant_line_in_a_long_result_is_clipped() {
    let mut lines: Vec<String> = (0..12).map(|i| format!("line {i}")).collect();
    lines[2] = "x".repeat(500_000);
    let result = lines.join("\n");
    let (eviction, out) = evicted(&result, 5000);
    let preview = preview_of(&out);
    assert!(char_len(preview) <= budget(5000));
    assert!(char_len(preview) > budget(5000) - 100);
    let (before, after) = preview.split_once(" [... ").unwrap();
    let kept = before.rsplit('\n').next().unwrap();
    let truncated: usize = after.split(' ').next().unwrap().parse().unwrap();
    assert!(kept.chars().all(|c| c == 'x'));
    assert_eq!(kept.len() + truncated, 500_000);
    assert!(preview.ends_with("line 11"));
    assert!(preview.contains("lines omitted"));
    assert_eq!(eviction.stored("", "evicted_tc1"), Some(result));
}

// Python: tests/test_tool_eviction_preview.py::TestGiantLine::test_giant_line_in_a_short_result_is_marked
#[test]
fn giant_line_in_a_short_result_is_marked() {
    let result = format!("first\n{}\nlast", "y".repeat(100_000));
    let (_, out) = evicted(&result, 5000);
    let preview = preview_of(&out);
    assert!(char_len(preview) <= budget(5000));
    assert!(preview.starts_with("first\n"));
    assert!(preview.contains(" chars truncated ...]"));
}

// Python: tests/test_tool_eviction_preview.py::TestGiantLine::test_short_result_still_shows_its_last_line
#[test]
fn short_result_still_shows_its_last_line() {
    let result = format!("HEADER\n{}\nSTATUS: done", "x".repeat(50_000));
    let (_, out) = evicted(&result, 100);
    let preview = preview_of(&out);
    assert!(char_len(preview) <= budget(100));
    assert!(preview.starts_with("HEADER\n"));
    assert!(preview.ends_with("STATUS: done"));
}

// Python: tests/test_tool_eviction_preview.py::TestGiantLine::test_single_line_uses_the_whole_budget
#[test]
fn single_line_uses_the_whole_budget() {
    let (_, out) = evicted(&"z".repeat(100_000), 5000);
    let preview = preview_of(&out);
    assert!(char_len(preview) <= budget(5000));
    assert!(char_len(preview) > budget(5000) - 50);
}

// Python: tests/test_tool_eviction_preview.py::TestGiantLine::test_giant_last_line_does_not_hide_the_end
#[test]
fn giant_last_line_does_not_hide_the_end() {
    let mut lines: Vec<String> = (0..30).map(|i| format!("row {i}")).collect();
    lines[27] = "w".repeat(200_000);
    let (_, out) = evicted(&lines.join("\n"), 5000);
    let preview = preview_of(&out);
    assert!(char_len(preview) <= budget(5000));
    assert!(preview.ends_with("row 28\nrow 29"));
}

// Python: tests/test_tool_eviction_preview.py::TestShortLines::test_short_lines_keep_the_head_tail_format
#[test]
fn short_lines_keep_the_head_tail_format() {
    let lines: Vec<String> = (0..5000)
        .map(|i| format!("line {i} {}", "x".repeat(50)))
        .collect();
    let (_, out) = evicted(&lines.join("\n"), 5000);
    let (head, tail) = (lines[..5].join("\n"), lines[4995..].join("\n"));
    assert_eq!(
        preview_of(&out),
        format!("{head}\n\n[... 4990 lines omitted ...]\n\n{tail}")
    );
}

// Python: tests/test_tool_eviction_preview.py::TestShortLines::test_small_threshold_shrinks_the_preview
#[test]
fn small_threshold_shrinks_the_preview() {
    let result = vec!["v".repeat(999); 5].join("\n");
    let (_, out) = evicted(&result, 1000);
    assert!(char_len(preview_of(&out)) <= budget(1000));
    assert!(char_len(preview_of(&out)) < char_len(&result));
}

/// `count` times the letter `g`: a slice of Python's giant line.
fn g(count: usize) -> String {
    "g".repeat(count)
}

/// Python's `_layouts`: results of every line shape.
fn layouts() -> Vec<String> {
    let giant = "g".repeat(150_000);
    let short: Vec<String> = (0..40).map(|i| format!("line {i}")).collect();
    let joined = |lines: &[&str]| lines.join("\n");
    let shorts: Vec<&str> = short.iter().map(String::as_str).collect();
    vec![
        giant.clone(),
        joined(&[&[giant.as_str()], &shorts[..]].concat()),
        joined(&[&shorts[..20], &[giant.as_str()], &shorts[20..]].concat()),
        joined(&[&shorts[..], &[giant.as_str()]].concat()),
        joined(&[&shorts[..], &[giant.as_str(), "end"]].concat()),
        [g(20_000).as_str(); 12].join("\n"),
        [g(15_000).as_str(); 10].join("\n"),
        [g(15_000).as_str(); 11].join("\n"),
        "\n".repeat(100_000),
        [g(30_000).as_str(); 4].join("\r\n"),
        (0..60_000)
            .map(|i| format!("r{i}"))
            .collect::<Vec<_>>()
            .join("\n"),
    ]
}

// Python: tests/test_tool_eviction_preview.py::test_preview_never_exceeds_the_budget
#[test]
fn preview_never_exceeds_the_budget() {
    for result in layouts() {
        for threshold in [1, 10, 100, 1000, 2000, 5000, 20_000] {
            let (eviction, out) = evicted(&result, threshold);
            assert!(is_placeholder(&out));
            let threshold = usize::try_from(threshold).unwrap();
            assert!(char_len(preview_of(&out)) <= budget(threshold));
            assert_eq!(eviction.stored("", "evicted_tc1").as_ref(), Some(&result));
        }
    }
}

// Python: tests/test_tool_eviction_preview.py::TestContentParts::test_small_text_leaves_the_parts_untouched,
// test_images_do_not_count_towards_the_threshold
#[test]
fn small_text_and_images_leave_the_parts_untouched() {
    let eviction = ToolEviction::new(5000);
    for parts in [vec![text("caption"), image()], vec![image(), image()]] {
        assert_eq!(eviction.maybe_evict_parts("", parts.clone(), "tc1"), parts);
    }
    assert!(!eviction.has_evicted(""));
}

// Python: tests/test_tool_eviction_preview.py::TestContentParts::test_oversized_text_is_replaced_by_one_placeholder
#[test]
fn oversized_text_is_replaced_by_one_placeholder() {
    let big = "x".repeat(150_000);
    let parts = vec![text("header"), image(), text(&big), image()];
    let eviction = ToolEviction::new(5000);
    let out = eviction.maybe_evict_parts("", parts, "tc1");
    let [ToolResultPart::Text(placeholder), second, third] = out.as_slice() else {
        panic!("{out:?}");
    };
    assert_eq!((second, third), (&image(), &image()));
    assert!(is_placeholder(&placeholder.text));
    assert!(char_len(preview_of(&placeholder.text)) <= budget(5000));
    assert_eq!(
        eviction.stored("", "evicted_tc1"),
        Some(format!("header\n{big}"))
    );
}

// Python: tests/test_tool_eviction_preview.py::TestContentParts::test_medium_parts_adding_up_are_evicted
#[test]
fn medium_parts_adding_up_are_evicted() {
    let parts = vec![text(&"y".repeat(3000)); 10];
    let out = ToolEviction::new(5000).maybe_evict_parts("", parts, "tc1");
    let [ToolResultPart::Text(placeholder)] = out.as_slice() else {
        panic!("{out:?}");
    };
    assert!(is_placeholder(&placeholder.text));
}

// Python: tests/test_tool_eviction_preview.py::TestPlaceholderIsNotEvictedAgain::test_a_placeholder_string_is_kept,
// test_a_placeholder_part_is_kept
#[test]
fn a_placeholder_is_kept() {
    let eviction = ToolEviction::new(10);
    let placeholder = eviction.maybe_evict("", "x".repeat(10_000), "tc1");
    assert_eq!(
        eviction.maybe_evict("", placeholder.clone(), "tc1"),
        placeholder
    );
    let bounded = eviction.maybe_evict_parts("", vec![text(&"x".repeat(10_000)), image()], "tc2");
    assert_eq!(
        eviction.maybe_evict_parts("", bounded.clone(), "tc2"),
        bounded
    );
    assert_eq!(eviction.stored("", "evicted_tc1"), Some("x".repeat(10_000)));
    assert_eq!(eviction.stored("", "evicted_tc2"), Some("x".repeat(10_000)));
}

// Python: tests/test_tool_eviction_preview.py::test_part_preview_never_exceeds_the_budget
#[test]
fn part_preview_never_exceeds_the_budget() {
    let giant = "g".repeat(150_000);
    let rows = (0..40_000)
        .map(|i| format!("r{i}"))
        .collect::<Vec<_>>()
        .join("\n");
    let layouts = [
        vec![text(&giant), image()],
        vec![image(), text(&giant)],
        (0..4).flat_map(|_| [text(&g(40_000)), image()]).collect(),
        vec![text(&rows), image()],
    ];
    for parts in layouts {
        for threshold in [10, 100, 1000, 5000, 20_000] {
            let eviction = ToolEviction::new(threshold);
            let out = eviction.maybe_evict_parts("", parts.clone(), "tc1");
            let texts = texts_of(&out);
            assert_eq!(texts.len(), 1);
            assert!(is_placeholder(texts[0]));
            let threshold = usize::try_from(threshold).unwrap();
            assert!(char_len(preview_of(texts[0])) <= budget(threshold));
            let images = |parts: &[ToolResultPart]| -> Vec<ToolResultPart> {
                parts
                    .iter()
                    .filter(|part| matches!(part, ToolResultPart::Image(_)))
                    .cloned()
                    .collect()
            };
            assert_eq!(images(&out), images(&parts));
            assert_eq!(
                eviction.stored("", "evicted_tc1"),
                Some(texts_of(&parts).join("\n"))
            );
        }
    }
}

// Python: tests/test_tool_eviction_preview.py::test_a_page_of_non_ascii_text_stays_under_the_threshold
#[test]
fn a_page_of_non_ascii_text_stays_under_the_threshold() {
    let line = format!("订单 编号 {}", "中文内容".repeat(20));
    let result = vec![line.as_str(); 400].join("\n");
    let eviction = ToolEviction::new(5000);
    let placeholder = eviction.maybe_evict("", result, "c0");
    let arguments = json!({"result_id": id_of(&placeholder)});
    let page = eviction.read("", arguments.as_object().unwrap()).unwrap();
    assert!(estimate_tokens(&page) <= 5000);
    assert!(page.contains("中文内容"));
}

// Python: tests/test_tool_eviction_room_scope.py::TestRoomScope::test_read_is_scoped_to_the_evicting_room,
// test_has_evicted_is_per_room
#[test]
fn read_is_scoped_to_the_evicting_room() {
    let eviction = ToolEviction::new(5000);
    eviction.maybe_evict("room-a", "line\n".repeat(20_000), "tc1");
    assert!(eviction.has_evicted("room-a"));
    assert!(!eviction.has_evicted("room-b"));
    let refused = answer(&eviction, "room-b", &json!({"result_id": "evicted_tc1"}));
    assert_eq!(
        refused,
        json!({"error": "Result 'evicted_tc1' not found", "available": []})
    );
    let read = answer(&eviction, "room-a", &json!({"result_id": "evicted_tc1"}));
    assert!(read.get("content").is_some());
}

/// Stores a small result in `room` under `call_id`; returns the id its
/// placeholder names.
fn evict_in(eviction: &ToolEviction, room: &str, result: &str, call_id: &str) -> String {
    id_of(&eviction.maybe_evict(room, result.to_owned(), call_id)).to_owned()
}

/// Whether `room` reads `result_id` back.
fn reads(eviction: &ToolEviction, room: &str, result_id: &str) -> bool {
    answer(eviction, room, &json!({"result_id": result_id}))
        .get("error")
        .is_none()
}

fn small() -> String {
    "line\n".repeat(200)
}

// Python: tests/test_tool_eviction_room_scope.py::TestCapacity::test_other_rooms_evictions_do_not_push_a_rooms_result_out
#[test]
fn other_rooms_evictions_do_not_push_a_rooms_result_out() {
    let eviction = ToolEviction::new(10);
    let kept = evict_in(&eviction, "room-a", &small(), "call_0");
    for i in 0..60 {
        evict_in(&eviction, &format!("room-{i}"), &small(), &format!("c{i}"));
    }
    assert!(reads(&eviction, "room-a", &kept));
}

// Python: tests/test_tool_eviction_room_scope.py::TestCapacity::test_a_quiet_room_keeps_its_result_while_full_rooms_evict
#[test]
fn a_quiet_room_keeps_its_result_while_full_rooms_evict() {
    let eviction = ToolEviction::new(10);
    let kept = evict_in(&eviction, "quiet", &small(), "c");
    for room in ["w", "x", "y", "z"] {
        for i in 0..50 {
            evict_in(&eviction, room, &small(), &format!("c{i}"));
        }
    }
    assert!(reads(&eviction, "quiet", &kept));
}

// Python: tests/test_tool_eviction_room_scope.py::TestCapacity::test_a_room_keeps_its_fifty_most_recently_read
#[test]
fn a_room_keeps_its_fifty_most_recently_read() {
    let eviction = ToolEviction::new(10);
    let ids: Vec<String> = (0..50)
        .map(|i| evict_in(&eviction, "room-a", &small(), &format!("c{i}")))
        .collect();
    assert!(reads(&eviction, "room-a", &ids[0]));
    evict_in(&eviction, "room-a", &small(), "c50");
    assert!(reads(&eviction, "room-a", &ids[0]));
    assert!(!reads(&eviction, "room-a", &ids[1]));
}

// Python: tests/test_tool_eviction_room_scope.py::TestCapacity::test_the_whole_store_stays_bounded
#[test]
fn the_whole_store_stays_bounded() {
    let eviction = ToolEviction::new(10);
    let ids: Vec<String> = (0..300)
        .map(|i| evict_in(&eviction, &format!("room-{i}"), &small(), "c"))
        .collect();
    let readable = ids
        .iter()
        .enumerate()
        .filter(|(i, id)| reads(&eviction, &format!("room-{i}"), id))
        .count();
    assert_eq!(readable, 200);
}

// Python: tests/test_tool_eviction_room_scope.py::TestIds::test_an_id_that_left_the_store_is_not_given_again
#[test]
fn an_id_that_left_the_store_is_not_given_again() {
    let eviction = ToolEviction::new(10);
    let old = evict_in(&eviction, "room-a", &"ROUND ONE\n".repeat(100), "call_0");
    for i in 0..60 {
        evict_in(&eviction, "room-a", &small(), &format!("c{i}"));
    }
    let new = evict_in(&eviction, "room-a", &"ROUND TWO\n".repeat(100), "call_0");
    assert_ne!(new, old);
    assert!(!reads(&eviction, "room-a", &old));
}

/// Python's `_EXPORT`: 4000 order lines, one of them lost.
fn export() -> String {
    (0..4000)
        .map(|n| {
            let state = if n == 1042 { "LOST" } else { "ok" };
            format!("order A-{n:04} shipped {state} total {n}.00")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// `text` stored in a store of `threshold` tokens, under the id it returns.
fn holding(text: &str, threshold: u64) -> (ToolEviction, String) {
    let eviction = ToolEviction::new(threshold);
    let id = evict_in(&eviction, "", text, "c0");
    (eviction, id)
}

// Python: tests/test_channels/test_ai_tool_loop.py::TestToolResultTruncation::test_small_result_unchanged,
// test_result_at_exact_limit_unchanged
#[test]
fn a_result_up_to_the_threshold_is_unchanged() {
    let eviction = ToolEviction::new(5000);
    for result in ["small result".to_owned(), "x".repeat(19_996)] {
        assert_eq!(eviction.maybe_evict("", result.clone(), "tc1"), result);
    }
    assert!(!eviction.has_evicted(""));
}

// Python: tests/test_channels/test_ai_tool_loop.py::TestEvictedResultReading::test_single_line_json_paginates
#[test]
fn single_line_json_paginates() {
    let emails: Vec<Value> = (0..80)
        .map(|i| json!({"id": format!("msg-{i}"), "subject": "x".repeat(40)}))
        .collect();
    let large = dumps(&json!({ "emails": emails }));
    assert!(!large.contains('\n'));
    let (eviction, id) = holding(&large, 400);
    let mut pages: Vec<String> = Vec::new();
    let mut offset = Value::from(0);
    loop {
        let page = answer(&eviction, "", &json!({"result_id": id, "offset": offset}));
        pages.push(page["content"].as_str().unwrap().to_owned());
        if page["has_more"] == false {
            assert_eq!(page["next_offset"], Value::Null);
            break;
        }
        offset = page["next_offset"].clone();
    }
    assert_eq!(pages.concat(), large);
    assert!(pages.len() > 1);
}

// Python: tests/test_channels/test_ai_tool_loop.py::TestEvictedResultReading::test_read_output_never_reevicted
#[test]
fn read_output_never_reevicted() {
    let large = dumps(&json!({"k": "\"".repeat(3000)}));
    let (eviction, id) = holding(&large, 100);
    let page = eviction
        .read("", json!({"result_id": id}).as_object().unwrap())
        .unwrap();
    assert_eq!(eviction.maybe_evict("", page.clone(), "tc2"), page);
}

// Python: tests/test_channels/test_stored_result_search.py::test_a_search_returns_the_matching_lines_with_their_neighbours
#[test]
fn a_search_returns_the_matching_lines_with_their_neighbours() {
    let (eviction, id) = holding(&export(), 500);
    let found = answer(&eviction, "", &json!({"result_id": id, "query": "a-1042"}));
    assert_eq!(
        (&found["total_matches"], &found["has_more"]),
        (&json!(1), &json!(false))
    );
    let expected: Vec<String> = (1040..1045)
        .map(|n| {
            let state = if n == 1042 { "LOST" } else { "ok" };
            format!("{}: order A-{n:04} shipped {state} total {n}.00", n + 1)
        })
        .collect();
    assert_eq!(
        found["content"]
            .as_str()
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        expected
    );
}

// Python: tests/test_channels/test_stored_result_search.py::test_a_search_that_finds_nothing_says_the_text_is_absent
#[test]
fn a_search_that_finds_nothing_says_the_text_is_absent() {
    let (eviction, id) = holding(&export(), 500);
    let found = answer(&eviction, "", &json!({"result_id": id, "query": "A-9999"}));
    assert_eq!(
        (&found["total_matches"], &found["content"]),
        (&json!(0), &json!(""))
    );
    assert!(found["note"].as_str().unwrap().contains("it is absent"));
}

// Python: tests/test_channels/test_stored_result_search.py::test_matches_past_a_page_are_flagged_and_continue_from_next_offset
#[test]
fn matches_past_a_page_are_flagged_and_continue_from_next_offset() {
    let (eviction, id) = holding(&export(), 500);
    let query = |offset: Value| {
        answer(
            &eviction,
            "",
            &json!({"result_id": id, "query": "shipped ok", "offset": offset}),
        )
    };
    let first = query(json!(0));
    let rest = query(first["next_offset"].clone());
    let past = query(json!(5000));
    assert_eq!(
        (&first["has_more"], &first["total_matches"]),
        (&json!(true), &json!(3999))
    );
    assert!(
        first["warning"]
            .as_str()
            .unwrap()
            .starts_with("PARTIAL CONTENT")
    );
    let number = |found: &Value| {
        let content = found["content"].as_str().unwrap().to_owned();
        content
            .lines()
            .next()
            .unwrap()
            .split(':')
            .next()
            .unwrap()
            .to_owned()
    };
    assert_ne!(number(&rest), number(&first));
    assert!(
        past["note"]
            .as_str()
            .unwrap()
            .contains("past the last match")
    );
}

// Python: tests/test_channels/test_stored_result_search.py::test_a_text_across_the_cut_of_a_long_line_is_found
#[test]
fn a_text_across_the_cut_of_a_long_line_is_found() {
    let ids: Vec<String> = (0..3000).map(|n| format!("A-{n:05}")).collect();
    let rows: Vec<String> = ids
        .iter()
        .map(|id| format!("{{\"id\":\"{id}\",\"status\":\"shipped\"}}"))
        .collect();
    let text = format!("[{}]", rows.join(","));
    let (eviction, id) = holding(&text, 500);
    let chunks: Vec<String> = text
        .chars()
        .collect::<Vec<_>>()
        .chunks(eviction.page_budget())
        .map(|chunk| chunk.iter().collect())
        .collect();
    let straddling: Vec<&String> = ids
        .iter()
        .filter(|id| !chunks.iter().any(|chunk| chunk.contains(id.as_str())))
        .collect();
    assert_ne!(straddling, Vec::<&String>::new());
    let found = answer(
        &eviction,
        "",
        &json!({"result_id": id, "query": straddling[0]}),
    );
    assert_eq!(found["total_matches"], 1);
    assert!(
        found["content"]
            .as_str()
            .unwrap()
            .contains(straddling[0].as_str())
    );
}

// Python: tests/test_channels/test_stored_result_search.py::test_a_search_is_bounded_like_a_page
#[test]
fn a_search_is_bounded_like_a_page() {
    let hits = (0..200)
        .map(|n| format!("hit {n} {}", "x".repeat(4000)))
        .collect::<Vec<_>>()
        .join("\n");
    let ids = (0..12_000)
        .map(|n| format!("{{\"id\":\"A-{n:05}\"}}"))
        .collect::<Vec<_>>()
        .join(",");
    for text in [hits, format!("[{ids}]")] {
        let (eviction, id) = holding(&text, 5000);
        for query in ["hit 7 ", "A-06000"] {
            let arguments = json!({"result_id": id, "query": query});
            let found = eviction.read("", arguments.as_object().unwrap()).unwrap();
            assert!(estimate_tokens(&found) <= 5000);
        }
    }
}

// Python: tests/test_channels/test_stored_result_search.py::test_a_blank_query_reads_a_page_and_a_query_on_two_lines_is_refused
#[test]
fn a_blank_query_reads_a_page_and_a_query_on_two_lines_is_refused() {
    let (eviction, id) = holding(&export(), 500);
    let blank = answer(&eviction, "", &json!({"result_id": id, "query": "  "}));
    assert_eq!(blank["offset"], 0);
    let refused = answer(
        &eviction,
        "",
        &json!({"result_id": id, "query": "A-1\nA-2"}),
    );
    assert!(refused["error"].as_str().unwrap().contains("one line"));
}
