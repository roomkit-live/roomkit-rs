//! A completed producer's usage is fixed before its result is read.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use agent_client_protocol_schema::v1::{
    PromptRequest, RequestPermissionRequest, RequestPermissionResponse, SessionNotification,
};
use roomkit::model::ResponseMetadata;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::oneshot;

use crate::channel::{preparation::Preparation, usage::Usage};
use crate::{AcpClient, AcpError, ClientSideConnection};

struct Client;
impl AcpClient for Client {
    fn session_update(
        &self,
        _: SessionNotification,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }
    fn request_permission(
        &self,
        _: RequestPermissionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<RequestPermissionResponse, AcpError>> + Send + '_>>
    {
        Box::pin(async {
            Err(AcpError::Runtime(
                "unexpected permission request".to_owned(),
            ))
        })
    }
}

fn envelope() -> Value {
    json!({"session_id":"original-session", "session_epoch":"epoch-1", "result_id":"result-2",
        "generation":0, "replayed":true, "usage_report":{
            "report_id":"report-1", "source_result_id":"result-1", "source":"session/update",
            "scope":"session", "update":{"used":0,"size":100,"cost":{"amount":0,"currency":"USD"}}
        }
    })
}

// Python: tests/test_channels/test_acp_usage_boundaries.py::test_late_notification_cannot_overwrite_terminal_snapshot
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_late_notification_cannot_overwrite_terminal_snapshot() {
    for transport_report in [false, true] {
        tokio::time::timeout(Duration::from_secs(10), check_late(transport_report))
            .await
            .unwrap();
    }
}

async fn check_late(transport_report: bool) {
    let (ours, theirs) = tokio::io::duplex(1 << 16);
    let (reader, writer) = tokio::io::split(ours);
    let connection = Arc::new(ClientSideConnection::new(Arc::new(Client), reader, writer));
    let usage = Arc::new(Usage::new(
        "session-1",
        "event-1",
        "piped",
        true,
        None,
        None,
    ));
    let record = ResponseMetadata::new();
    record.insert("acp", json!({}));
    let (start, ready) = oneshot::channel();
    let task = tokio::spawn(super::prompt_task(
        ready,
        connection,
        PromptRequest::new("session-1", vec!["recover".into()]),
        record,
        Arc::new(Preparation::default()),
        || {},
        Arc::clone(&usage),
    ));
    start.send(()).unwrap();
    let (reader, mut writer) = tokio::io::split(theirs);
    let mut lines = BufReader::new(reader).lines();
    let request: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    let response = json!({"jsonrpc":"2.0", "id":request["id"], "result":{
        "stopReason":"end_turn", "_meta":{"roomkit.live/usage":envelope()}
    }});
    writer
        .write_all(format!("{response}\n").as_bytes())
        .await
        .unwrap();
    // Observe completion without consuming its result. No delay guesses at the boundary.
    while !task.is_finished() {
        tokio::task::yield_now().await;
    }
    let mut late = json!({"sessionUpdate":"usage_update", "used":99, "size":200,
        "cost":{"amount":99,"currency":"EUR"}});
    if transport_report {
        let mut report = envelope();
        report["session_id"] = json!("session-1");
        report["usage_report"]["update"] = late.clone();
        late["_meta"] = json!({"roomkit.live/usage":report});
    }
    usage.observe(&late, None);
    let answer = task.await.unwrap().unwrap().unwrap();
    assert_eq!(answer.usage.metadata["session_id"], "original-session");
    assert_eq!(
        answer.usage.metadata["usage_report"],
        envelope()["usage_report"]
    );
    assert_eq!(answer.usage.counters["cost"], 0.0);
    assert_eq!(answer.usage.counters["currency"], "USD");
}
