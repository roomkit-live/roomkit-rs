//! How a provider agrees on the MCP revision with its server over stdio
//! (RFC §21.2, RK-21.2-056): Python's `tests/test_mcp_discovery.py`, on a
//! scripted server. A probe never answered and one answered late are proven
//! in `server.rs`'s unit tests, on tokio's paused clock: rmcp bounds the
//! probe at 10 seconds, which a test cannot shorten.

use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit_mcp::{FromCommandOptions, McpError, McpToolProvider};
use serde_json::{Value, json};

use super::support::Scripted;

fn tools() -> Value {
    json!({"result":{"tools":[{"name":"lookup","inputSchema":{"type":"object"}}]}})
}

fn handshake() -> Value {
    json!({"result":{
        "protocolVersion":"2025-11-25",
        "capabilities":{"tools":{}},
        "serverInfo":{"name":"legacy","version":"1"},
        "instructions":"from the handshake",
    }})
}

fn discovery() -> Value {
    json!({"result":{
        "supportedVersions":["2026-07-28"],
        "capabilities":{"tools":{},"extensions":{"io.example/ext":{"level":1}}},
        "instructions":"from discovery",
        "resultType":"complete","ttlMs":0,"cacheScope":"public",
        "_meta":{"io.modelcontextprotocol/serverInfo":{"name":"modern","version":"2","vendor":"x"}},
    }})
}

fn refused() -> Value {
    json!({"error":{"code":-32601,"message":"Method not found"}})
}

/// What the provider read of its server once connected: its tools, what it
/// said of itself, its instructions and its extensions.
fn read(provider: &McpToolProvider) -> Value {
    json!({
        "tools": provider.tool_names().unwrap(),
        "server_info": provider.server_info().unwrap(),
        "instructions": provider.instructions().unwrap(),
        "extensions": provider.server_extensions().unwrap(),
    })
}

#[tokio::test]
async fn a_server_answering_discovery_is_reached_without_a_handshake() {
    let scripted = Scripted::new();
    let plan = json!({"server/discover": discovery(), "tools/list": tools()});
    let provider = scripted.server(&plan).connect().await.unwrap();
    let read = read(&provider);
    provider.close().await;
    assert_eq!(
        read,
        json!({
            "tools": ["lookup"],
            "server_info": {"name":"modern","version":"2","vendor":"x"},
            "instructions": "from discovery",
            "extensions": {"io.example/ext":{"level":1}},
        })
    );
    let received = scripted.received();
    assert_eq!(scripted.methods(), ["server/discover", "tools/list"]);
    // Every request of a 2026-07-28 connection names its protocol revision.
    let meta = &received[1]["params"]["_meta"];
    assert_eq!(
        meta["io.modelcontextprotocol/protocolVersion"],
        "2026-07-28"
    );
}

/// A probe the server refuses falls back to `initialize`.
#[tokio::test]
async fn a_server_that_refuses_discovery_is_reached_by_the_handshake() {
    let scripted = Scripted::new();
    let plan =
        json!({"server/discover": refused(), "initialize": handshake(), "tools/list": tools()});
    let provider = scripted.server(&plan).connect().await.unwrap();
    let read = read(&provider);
    provider.close().await;
    assert_eq!(
        (
            read["tools"].clone(),
            read["server_info"].clone(),
            read["instructions"].clone()
        ),
        (
            json!(["lookup"]),
            json!({"name":"legacy","version":"1"}),
            json!("from the handshake")
        )
    );
    let methods = scripted.methods();
    assert_eq!(
        methods[..3],
        ["server/discover", "initialize", "notifications/initialized"]
    );
    assert_eq!(methods.last().map(String::as_str), Some("tools/list"));
}

/// The probe counts within `connect_timeout`: one that outlasts it fails the
/// connection, though the server would take the handshake after it.
#[tokio::test]
async fn a_probe_past_the_connect_bound_fails_the_connection() {
    let scripted = Scripted::new();
    let plan =
        json!({"server/discover": "silent", "initialize": handshake(), "tools/list": tools()});
    let provider = scripted
        .server(&plan)
        .connect_timeout(Duration::from_millis(200))
        .provider();
    let refused = provider.connect().await.err();
    assert!(
        matches!(refused, Some(McpError::Timeout { after, .. }) if after == Duration::from_millis(200)),
        "{refused:?}"
    );
    assert!(!provider.connected());
    assert_eq!(scripted.methods(), ["server/discover"]);
}

fn headed_tools() -> Value {
    json!({"result":{"tools":[
        {"name":"routed","inputSchema":{"type":"object",
            "properties":{"region":{"type":"string","x-mcp-header":"Region"}}}},
        // x-mcp-header names a scalar argument only: this one is invalid.
        {"name":"misrouted","inputSchema":{"type":"object",
            "properties":{"blob":{"type":"object","x-mcp-header":"Blob"}}}},
        {"name":"plain","inputSchema":{"type":"object"}},
    ]}})
}

/// MCP 2026-07-28 has a client leave out a tool whose `x-mcp-header`
/// annotations are invalid; an earlier revision knows no such annotation.
#[tokio::test]
async fn a_tool_whose_header_annotation_is_invalid_is_left_out_on_2026_07_28() {
    for (probe, kept) in [
        (discovery(), vec!["routed", "plain"]),
        (refused(), vec!["routed", "misrouted", "plain"]),
    ] {
        let scripted = Scripted::new();
        let plan = json!({"server/discover": probe, "initialize": handshake(), "tools/list": headed_tools()});
        let provider = scripted.server(&plan).connect().await.unwrap();
        let names = provider.tool_names().unwrap();
        provider.close().await;
        assert_eq!(names, kept);
    }
}

/// `protocol="legacy"` sends no probe, for a server that would fail on it,
/// even to a server that would answer it.
#[tokio::test]
async fn the_legacy_protocol_goes_straight_to_the_handshake() {
    let scripted = Scripted::new();
    let plan =
        json!({"server/discover": discovery(), "initialize": handshake(), "tools/list": tools()});
    let provider = scripted
        .server(&plan)
        .protocol("legacy")
        .connect()
        .await
        .unwrap();
    let info = provider.server_info().unwrap();
    provider.close().await;
    assert_eq!(json!(info), json!({"name":"legacy","version":"1"}));
    assert_eq!(
        scripted.methods(),
        ["initialize", "notifications/initialized", "tools/list"]
    );
}

#[test]
fn a_protocol_other_than_auto_or_legacy_is_refused() {
    let options = FromCommandOptions {
        protocol: "2026-07-28".to_owned(),
        ..FromCommandOptions::default()
    };
    assert!(matches!(
        McpToolProvider::from_command("python", options),
        Err(McpError::Configuration {
            reason: "protocol must be 'auto' or 'legacy'"
        })
    ));
}

/// A discovery whose extension settings are not all objects is read as
/// Python's provider shows it to its SDK, those settings left out: it still
/// agrees on 2026-07-28, and the other extensions are kept.
#[tokio::test]
async fn an_extension_that_is_no_object_leaves_the_discovery_agreed() {
    let mut odd = discovery();
    odd["result"]["capabilities"]["extensions"] = json!({"io.x/good":{"a":1},"io.x/bad":5});
    let scripted = Scripted::new();
    let plan = json!({"server/discover": odd, "initialize": handshake(), "tools/list": tools()});
    let provider = scripted.server(&plan).connect().await.unwrap();
    let read = read(&provider);
    provider.close().await;
    assert_eq!(
        (
            scripted.methods(),
            read["server_info"]["name"].clone(),
            read["extensions"].clone()
        ),
        (
            vec!["server/discover".to_owned(), "tools/list".to_owned()],
            json!("modern"),
            json!({"io.x/good":{"a":1}})
        )
    );
}

/// A discovery result says what the server is in its `_meta`: a
/// `serverInfo` beside it, which only a handshake result has, is not read.
#[tokio::test]
async fn what_a_discovery_says_of_the_server_is_read_from_its_meta() {
    let mut stray = discovery();
    stray["result"]["serverInfo"] = json!({"name":"stray","version":"0"});
    let scripted = Scripted::new();
    let plan = json!({"server/discover": stray, "tools/list": tools()});
    let provider = scripted.server(&plan).connect().await.unwrap();
    let info = provider.server_info().unwrap();
    provider.close().await;
    assert_eq!(
        json!(info),
        json!({"name":"modern","version":"2","vendor":"x"})
    );
}

/// The provider reads no cache hint: a 2026-07-28 listing whose hints are
/// outside the revision's values is listed all the same.
#[tokio::test]
async fn a_listing_s_cache_hints_do_not_fail_the_connection() {
    for envelope in [json!({"cacheScope":"shared"}), json!({"ttlMs":-5})] {
        let mut listing = tools();
        listing["result"]["resultType"] = json!("complete");
        listing["result"]
            .as_object_mut()
            .unwrap()
            .extend(envelope.as_object().unwrap().clone());
        let scripted = Scripted::new();
        let plan = json!({"server/discover": discovery(), "tools/list": listing});
        let provider = scripted.server(&plan).connect().await.unwrap();
        let names = provider.tool_names().unwrap();
        provider.close().await;
        assert_eq!(names, ["lookup"], "{envelope}");
    }
}
