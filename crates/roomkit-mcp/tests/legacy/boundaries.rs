//! Python twins: `tests/test_mcp_legacy_boundaries.py`.

use super::peer::{Mode, Peer};
use axum::http::{Method, StatusCode};
use roomkit_mcp::{McpError, McpHeaders};
use std::time::Duration;

#[tokio::test]
async fn redirects_keep_the_origin_method_and_custom_headers() {
    for (method, status) in [
        (Method::GET, 301),
        (Method::GET, 302),
        (Method::GET, 303),
        (Method::GET, 307),
        (Method::GET, 308),
        (Method::POST, 307),
        (Method::POST, 308),
    ] {
        let peer = Peer::start(Mode::Redirect {
            method: method.clone(),
            status: StatusCode::from_u16(status).unwrap(),
            outside: None,
        })
        .await;
        let provider = peer.provider(
            Some(McpHeaders::new([("X-Capture".into(), "kept".into())])),
            Duration::from_secs(3),
        );
        provider.connect().await.unwrap();
        assert!(provider.connected());
        provider.close().await;
        peer.closed().await;
        let seen = peer.shared.seen.lock().unwrap();
        assert!(
            seen.requests
                .iter()
                .any(|(verb, target, _)| *verb == method && target == "/accepted")
        );
        assert!(
            seen.requests
                .iter()
                .all(|(_, _, headers)| headers.get("x-capture").unwrap() == "kept")
        );
        assert_eq!(
            seen.messages
                .iter()
                .filter(|message| message.get("method").unwrap() == "initialize")
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn refused_redirects_do_not_egress_or_rewrite_a_post() {
    for (method, status) in [(Method::GET, 307), (Method::POST, 307), (Method::POST, 302)] {
        let outside = Peer::start(Mode::Normal).await;
        let peer = Peer::start(Mode::Redirect {
            method: method.clone(),
            status: StatusCode::from_u16(status).unwrap(),
            outside: (status == 307).then(|| outside.url.clone()),
        })
        .await;
        let provider = peer.provider(None, Duration::from_millis(50));
        let error = provider.connect().await.unwrap_err();
        if method == Method::GET {
            assert!(
                matches!(
                    error,
                    McpError::Http {
                        status: Some(307),
                        ..
                    }
                ),
                "{error:?}"
            );
        } else {
            assert!(
                matches!(
                    error,
                    McpError::Http {
                        reason: "connection timed out",
                        ..
                    }
                ),
                "{error:?}"
            );
        }
        assert!(!provider.connected());
        peer.closed().await;
        assert_eq!(outside.shared.seen.lock().unwrap().requests.len(), 0);
        let seen = peer.shared.seen.lock().unwrap();
        assert!(
            seen.requests
                .iter()
                .all(|(_, target, _)| target != "/accepted")
        );
        assert_eq!(seen.messages, Vec::<serde_json::Value>::new());
    }
}

#[tokio::test]
async fn endpoint_announcements_compare_the_configured_raw_authority() {
    for kind in ["relative", "absolute", "host-case", "userinfo"] {
        let peer = Peer::start(Mode::Endpoint(kind)).await;
        let refused = matches!(kind, "host-case" | "userinfo");
        let provider = peer.provider(
            None,
            if refused {
                Duration::from_millis(50)
            } else {
                Duration::from_secs(3)
            },
        );
        let result = provider.connect().await;
        if refused {
            assert!(
                matches!(
                    result,
                    Err(McpError::Http {
                        reason: "connection timed out",
                        ..
                    })
                ),
                "{result:?}"
            );
        } else {
            result.unwrap();
            assert!(provider.connected());
            provider.close().await;
        }
        peer.closed().await;
        assert!(!provider.connected());
        let seen = peer.shared.seen.lock().unwrap();
        if refused {
            assert_eq!(seen.requests.len(), 1);
            assert_eq!(seen.messages, Vec::<serde_json::Value>::new());
        } else {
            assert_eq!(
                seen.messages
                    .iter()
                    .filter(|message| message.get("method").unwrap() == "initialize")
                    .count(),
                1
            );
        }
    }
}
