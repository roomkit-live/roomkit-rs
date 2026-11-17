use super::capture::Lines;
use super::support::{SERIAL, Turn, call};
use roomkit_ai::{ToolError, ToolHandler};
use serde_json::json;

#[tokio::test]
async fn declines_do_not_log_the_private_message() {
    let _serial = SERIAL.lock().await;
    for streaming in [false, true] {
        let lines = Lines::default();
        let _capture = tracing::subscriber::set_default(lines.clone());
        let turn = Turn::new(
            streaming,
            vec![call(json!({}))],
            ToolHandler::new(|_, _| async { Err(ToolError::unserved("synthetic-decline")) }),
            None,
        )
        .await;
        let results = turn.run().await;
        assert_eq!(
            results[0].as_text(),
            "{\"error\": \"No handler for tool lookup\"}"
        );
        let records = lines.read();
        assert!(
            !records
                .iter()
                .any(|line| line.contains("synthetic-decline")),
            "{records:?}"
        );
        assert_eq!(
            records
                .iter()
                .filter(|line| line.starts_with("INFO") && line.contains("nothing served it"))
                .count(),
            1,
            "{records:?}"
        );
    }
}

#[tokio::test]
async fn handler_refusals_and_failures_keep_their_source_levels() {
    let _serial = SERIAL.lock().await;
    let mut mismatches = Vec::new();
    for streaming in [false, true] {
        for (error, level, message) in [
            (
                ToolError::Refused("synthetic-refusal".into()),
                "INFO",
                "synthetic-refusal",
            ),
            (
                ToolError::Failed("synthetic-failure".into()),
                "WARN",
                "synthetic-failure",
            ),
            (
                ToolError::raised_as("ValueError", "synthetic-error"),
                "WARN",
                "synthetic-error",
            ),
        ] {
            let lines = Lines::default();
            let _capture = tracing::subscriber::set_default(lines.clone());
            let handler = ToolHandler::new(move |_, _| {
                let error = error.clone();
                async move { Err(error) }
            });
            let turn = Turn::new(streaming, vec![call(json!({}))], handler, None).await;
            let results = turn.run().await;
            assert!(results[0].is_error);
            if message == "synthetic-error" {
                assert!(!results[0].as_text().contains(message));
            }
            let records = lines.read();
            let errors: Vec<_> = records
                .iter()
                .filter(|line| line.contains(message))
                .collect();
            if errors.len() != 1 || !errors[0].starts_with(level) {
                mismatches.push(format!(
                    "streaming={streaming}, expected {level} {message}: {records:?}"
                ));
            }
            assert!(
                !records
                    .iter()
                    .any(|line| line.contains("Tool lookup result:")),
                "{records:?}"
            );
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
}
