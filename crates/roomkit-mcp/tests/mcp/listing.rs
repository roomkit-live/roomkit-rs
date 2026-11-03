//! A server's tools, listed once at its connection (RFC §21.2), following
//! the cursor within bounds, each tool one a model can be handed (RFC
//! §6.7, §21.1).

use std::time::Duration;

use pretty_assertions::assert_eq;

use super::support::{connected, fake};

async fn names(mode: &str) -> Vec<String> {
    let server = connected(mode).await;
    let names = server.tool_names().unwrap();
    server.close().await;
    names
}

/// RFC §21.2: the listing follows the cursor across every page.
#[tokio::test]
async fn the_tools_are_listed_across_every_page_of_the_cursor() {
    assert_eq!(names("pages:3").await, ["t0", "t1", "t2"]);
}

/// RFC §21.2: a cursor handed out twice ends the listing, the page it
/// brings kept, as the reference implementation keeps it.
#[tokio::test]
async fn a_cursor_handed_out_twice_ends_the_listing() {
    assert_eq!(names("same-cursor").await, ["t1", "t2"]);
}

/// RFC §21.2: an empty page that still carries a cursor ends the listing.
#[tokio::test]
async fn an_empty_page_that_carries_a_cursor_ends_the_listing() {
    assert_eq!(names("empty-page").await, ["first"]);
}

/// RFC §21.2: a server that pages forever does not hold the connection:
/// the listing ends at its bound of pages, here 1,000 pages that each list
/// the same tool, well within the connect bound.
#[tokio::test]
async fn a_listing_without_end_stops_at_its_bound_of_pages() {
    let listed = tokio::time::timeout(Duration::from_secs(10), names("endless-same")).await;
    assert_eq!(listed.ok(), Some(vec!["t_same".to_owned()]));
}

/// A listing keeps 512 tools at most, whether one page or many brings
/// them.
#[tokio::test]
async fn a_listing_keeps_at_most_512_tools() {
    let (one_page, pages) = (names("many").await, names("endless").await);
    assert_eq!(
        (
            one_page.len(),
            pages.len(),
            one_page.last().map(String::as_str)
        ),
        (512, 512, Some("t511"))
    );
}

/// A tool filter chooses among every tool the server lists, the 512 kept
/// counting only those it admits: tools past the 512th that the filter
/// selects are kept, as Python filters the listing it reads (RMK-1099).
#[tokio::test]
async fn a_tool_filter_chooses_before_the_listing_s_bound() {
    let server = fake("many")
        .tools(&["t3", "t520", "t599"])
        .connect()
        .await
        .unwrap();
    let names = server.tool_names().unwrap();
    server.close().await;
    assert_eq!(names, ["t3", "t520", "t599"]);
}

/// RFC §6.7: a tool whose name no vendor accepts, or longer than 128
/// characters, is left out and the others kept; so is a name listed again
/// (RFC §21.1) and a schema over 32 KiB; a description is cut at 4,096
/// characters, at a word.
#[tokio::test]
async fn a_tool_no_model_can_be_handed_is_left_out_and_a_description_cut() {
    let server = connected("names").await;
    let tools = server.get_tools().unwrap();
    server.close().await;
    let names: Vec<&str> = tools.iter().map(|tool| tool.name.as_str()).collect();
    let long = "y".repeat(128);
    let description = tools.last().map(|tool| tool.description.as_str()).unwrap();
    assert_eq!(
        (
            names,
            description.chars().count(),
            description.ends_with("word word…"),
        ),
        (
            vec![
                "good_name",
                long.as_str(),
                "dotted.name:v1",
                "long_description"
            ],
            4095,
            true
        )
    );
}
