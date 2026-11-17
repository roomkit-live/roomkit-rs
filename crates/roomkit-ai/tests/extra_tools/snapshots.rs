//! Owned snapshots of host definitions, replaced through shared handles.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::Metadata;
use roomkit_ai::{
    AiChannelOptions, AiContext, AiProvider, AiResponse, AiTool, MockAiProvider, ProviderError,
};
use serde_json::json;
use tokio::sync::Notify;

use super::support::Door;

// Python: tests/test_channels/test_ai_extra_tools_turns.py::test_retained_nested_values_write_until_their_field_is_replaced
// Rust's approved form requires replace; retained snapshots do not write through.
#[test]
fn retained_nested_values_are_snapshots_until_replaced() {
    for agent in [false, true] {
        let definition = AiTool {
            parameters: json!({"nested":{"version":1}}),
            tags: vec!["old".into()],
            ..AiTool::new("read", "read").unwrap()
        };
        let door = Door::new(
            agent,
            MockAiProvider::new(roomkit_ai::MockAiProviderOptions::default()),
            AiChannelOptions {
                tools: vec![definition.clone().into()],
                ..Default::default()
            },
        );
        let handle = door.tools().remove(0);
        let mut snapshot = handle.get();
        snapshot.parameters["nested"]["version"] = json!(2);
        snapshot.tags.push("live".into());
        assert_eq!(handle.get(), definition);
        handle.replace(snapshot.clone());
        assert_eq!(door.tools()[0].get(), snapshot);
        let replacement = AiTool {
            parameters: json!({"replacement":3}),
            tags: vec!["replacement".into()],
            ..definition
        };
        handle.replace(replacement.clone());
        snapshot.parameters["nested"]["version"] = json!(4);
        snapshot.tags.push("detached".into());
        assert_eq!(door.tools()[0].get(), replacement);
    }
}

struct PausedProvider {
    ready: Arc<Notify>,
    release: Arc<Notify>,
    seen: Arc<Mutex<Vec<String>>>,
    calls: AtomicUsize,
}

impl AiProvider for PausedProvider {
    fn model_name(&self) -> &'static str {
        "paused"
    }
    fn name(&self) -> &'static str {
        "paused"
    }
    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        let tool = context
            .tools
            .iter()
            .find(|tool| tool.name == "read")
            .unwrap();
        self.seen.lock().unwrap().push(tool.description.clone());
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.ready.notify_one();
            self.release.notified().await;
            self.seen.lock().unwrap().push(tool.description.clone());
        }
        Ok(AiResponse::new("done"))
    }
}

// Python: tests/test_channels/test_ai_extra_tools_turns.py::test_a_provider_already_generating_reads_a_live_host_definition
// The approved Rust boundary keeps an already prepared request unchanged.
#[tokio::test]
async fn a_provider_already_generating_keeps_its_snapshot() {
    for agent in [false, true] {
        let ready = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let provider = PausedProvider {
            ready: Arc::clone(&ready),
            release: Arc::clone(&release),
            seen: Arc::clone(&seen),
            calls: AtomicUsize::new(0),
        };
        let door = Door::new(
            agent,
            provider,
            AiChannelOptions {
                tools: vec![AiTool::new("read", "before").unwrap().into()],
                tool_search: Some(false),
                ..Default::default()
            },
        );
        let edit = async {
            ready.notified().await;
            let handle = door.tools().remove(0);
            let mut definition = handle.get();
            definition.description = "while generating".into();
            handle.replace(definition);
            release.notify_one();
        };
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(door.turn(Metadata::new()), edit)
        })
        .await;
        if result.is_ok() {
            door.turn(Metadata::new()).await;
        }
        door.close().await;
        result.unwrap();
        assert_eq!(
            *seen.lock().unwrap(),
            ["before", "before", "while generating"]
        );
    }
}
