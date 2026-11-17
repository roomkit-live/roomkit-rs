//! Python twins: case-distinct order and per-connection retained dictionaries.

use super::peer::{Mode, Peer};
use roomkit_mcp::McpHeaders;
use std::time::Duration;

#[tokio::test]
async fn case_distinct_headers_keep_each_value_in_insertion_order() {
    for reverse in [false, true] {
        let mut pairs = vec![
            ("x-capture".to_owned(), "first".to_owned()),
            ("X-Capture".to_owned(), "second".to_owned()),
        ];
        if reverse {
            pairs.reverse();
        }
        let peer = Peer::start(Mode::Normal).await;
        let provider = peer.provider(Some(McpHeaders::new(pairs.clone())), Duration::from_secs(3));
        provider.connect().await.unwrap();
        assert_eq!(
            provider
                .call_tool(
                    "echo",
                    serde_json::Map::default(),
                    roomkit_mcp::CallToolOptions::default()
                )
                .await
                .unwrap(),
            "done"
        );
        provider.close().await;
        peer.closed().await;
        let expected: Vec<_> = pairs.iter().map(|(_, value)| value.as_str()).collect();
        for (_, _, headers) in &peer.shared.seen.lock().unwrap().requests {
            let observed: Vec<_> = headers
                .get_all("x-capture")
                .iter()
                .map(|value| value.to_str().unwrap())
                .collect();
            assert_eq!(observed, expected);
        }
    }
}

#[tokio::test]
async fn headers_are_not_revalidated_and_each_connection_takes_its_snapshot() {
    let peer = Peer::start(Mode::Normal).await;
    let headers = McpHeaders::new([("X-Capture".into(), "before".into())]);
    let provider = peer.provider(Some(headers.clone()), Duration::from_secs(3));
    headers.insert("X-Late".into(), "x".repeat(8193));
    for value in ["first", "second"] {
        headers.insert("X-Capture".into(), value.into());
        let before = peer.shared.seen.lock().unwrap().requests.len();
        provider.connect().await.unwrap();
        headers.insert("X-Capture".into(), "changed-during-connection".into());
        assert_eq!(
            provider
                .call_tool(
                    "echo",
                    serde_json::Map::default(),
                    roomkit_mcp::CallToolOptions::default()
                )
                .await
                .unwrap(),
            "done"
        );
        provider.close().await;
        peer.closed().await;
        for (_, _, sent) in peer
            .shared
            .seen
            .lock()
            .unwrap()
            .requests
            .iter()
            .skip(before)
        {
            assert_eq!(sent.get("x-capture").unwrap(), value);
            assert_eq!(sent.get("x-late").unwrap(), &"x".repeat(8193));
        }
    }
}
