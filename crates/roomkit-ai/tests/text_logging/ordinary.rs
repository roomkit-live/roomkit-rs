use super::capture::Lines;
use super::support::{SERIAL, Turn, call};
use roomkit::{content_logging_enabled, set_content_logging};
use roomkit_ai::{AiTextPart, ToolHandler, ToolResult, ToolResultPart};
use serde_json::json;

pub(super) struct Switch(bool);
impl Switch {
    pub(super) fn set(enabled: bool) -> Self {
        let original = content_logging_enabled();
        set_content_logging(enabled);
        Self(original)
    }
}
impl Drop for Switch {
    fn drop(&mut self) {
        set_content_logging(self.0);
    }
}

#[tokio::test]
async fn ordinary_turns_gate_debug_without_changing_the_tool_or_result() {
    let _serial = SERIAL.lock().await;
    for streaming in [false, true] {
        for enabled in [false, true] {
            let _switch = Switch::set(enabled);
            for answer in [
                "synthetic-result".to_owned(),
                format!("{}tail", "é\n".repeat(300)),
            ] {
                let lines = Lines::default();
                let _capture = tracing::subscriber::set_default(lines.clone());
                let returned = answer.clone();
                let handler = ToolHandler::new(move |name, arguments| {
                    assert_eq!(name, "lookup");
                    assert_eq!(arguments["query"], "synthetic-input");
                    let returned = returned.clone();
                    async move { Ok(ToolResult::Text(returned)) }
                });
                let turn = Turn::new(
                    streaming,
                    vec![call(json!({"query":"synthetic-input"}))],
                    handler,
                    None,
                )
                .await;
                let result = turn.run().await;
                assert_eq!(result[0].as_text(), answer);
                let records = lines.read();
                assert!(
                    records.iter().any(|line| line.starts_with("INFO")
                        && line.contains("Executing tool lookup (call c1) with query")),
                    "{records:?}"
                );
                assert!(
                    records.iter().any(|line| line
                        .contains(&format!("returned {} chars in ", answer.chars().count()))),
                    "{records:?}"
                );
                let preview = answer.replace('\n', " ");
                let preview = if preview.chars().count() > 500 {
                    format!(
                        "{}… ({} chars)",
                        preview.chars().take(500).collect::<String>(),
                        preview.chars().count()
                    )
                } else {
                    preview
                };
                let expected = if enabled {
                    preview.clone()
                } else {
                    format!("<redacted:{} chars>", preview.chars().count())
                };
                assert!(
                    records
                        .iter()
                        .any(|line| line.contains(&format!("Tool lookup result: {expected}"))),
                    "{records:?}"
                );
                let info: Vec<_> = records
                    .iter()
                    .filter(|line| line.starts_with("INFO"))
                    .collect();
                assert!(!info.iter().any(|line| line.contains("synthetic-input")
                    || line.contains("synthetic-result")
                    || line.contains('é')));
                assert_eq!(
                    records.iter().any(|line| line.contains("synthetic-input")),
                    enabled
                );
            }
        }
    }
}

#[tokio::test]
async fn null_json_and_text_parts_keep_their_source_preview() {
    let _serial = SERIAL.lock().await;
    let _switch = Switch::set(true);
    for streaming in [false, true] {
        for (answer, size, preview) in [
            (ToolResult::Json(json!(null)), "4", "null"),
            (ToolResult::Json(json!({"x":"é"})), "10", "{\"x\": \"é\"}"),
            (
                ToolResult::Parts(vec![ToolResultPart::Text(AiTextPart::new(
                    "synthetic-text",
                ))]),
                "-1",
                "[\"type='text' text='synthetic-text'\"]",
            ),
        ] {
            let lines = Lines::default();
            let _capture = tracing::subscriber::set_default(lines.clone());
            let handler = ToolHandler::new(move |_, _| {
                let answer = answer.clone();
                async move { Ok(answer) }
            });
            let turn = Turn::new(streaming, vec![call(json!({}))], handler, None).await;
            turn.run().await;
            let records = lines.read();
            assert!(
                records
                    .iter()
                    .any(|line| line.contains(&format!("returned {size} chars in "))),
                "{records:?}"
            );
            assert!(
                records
                    .iter()
                    .any(|line| line.contains(&format!("Tool lookup result: {preview}"))),
                "{records:?}"
            );
        }
    }
}
