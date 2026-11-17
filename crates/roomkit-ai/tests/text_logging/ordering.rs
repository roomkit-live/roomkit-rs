use super::capture::Lines;
use super::ordinary::Switch;
use super::support::{SERIAL, Turn, call};
use roomkit::model::HookResult;
use roomkit::model::hook::{AllowOptions, BlockOptions};
use roomkit::{HookOptions, HookTrigger, ToolCallEvent};
use roomkit_ai::{ToolHandler, ToolPolicy, ToolPolicyOptions, ToolResult};
use serde_json::json;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn logs_precede_a_policy_refusal_and_a_held_id_does_not_log_twice() {
    let _serial = SERIAL.lock().await;
    let _switch = Switch::set(true);
    for streaming in [false, true] {
        for duplicate in [false, true] {
            let lines = Lines::default();
            let _capture = tracing::subscriber::set_default(lines.clone());
            let ran = Arc::new(Mutex::new(0));
            let counter = Arc::clone(&ran);
            let handler = ToolHandler::new(move |_, _| {
                *counter.lock().unwrap() += 1;
                async { Ok(ToolResult::Text("synthetic-result".into())) }
            });
            let mut calls = vec![call(json!({"z":"synthetic-z","a":"synthetic-a"}))];
            if duplicate {
                calls.push(call(json!({"duplicate":"synthetic-held"})));
            }
            let policy = (!duplicate).then(|| {
                ToolPolicy::new(ToolPolicyOptions {
                    deny: vec!["lookup".into()],
                    ..Default::default()
                })
                .unwrap()
            });
            let turn = Turn::new(streaming, calls, handler, policy).await;
            let parts = turn.run().await;
            assert_eq!(parts.len(), if duplicate { 2 } else { 1 });
            assert_eq!(*ran.lock().unwrap(), usize::from(duplicate));
            let records = lines.read();
            assert_eq!(
                records
                    .iter()
                    .filter(|line| line.contains("Executing tool lookup (call c1) with a, z"))
                    .count(),
                1,
                "{records:?}"
            );
            assert!(!records.iter().any(|line| line.contains("synthetic-held")));
            if !duplicate {
                let call = records
                    .iter()
                    .position(|line| line.contains("Executing tool lookup"))
                    .unwrap();
                let refusal = records
                    .iter()
                    .position(|line| {
                        line.starts_with("WARN") && line.contains("Tool lookup blocked by policy")
                    })
                    .unwrap();
                assert!(call < refusal);
                assert!(
                    !records
                        .iter()
                        .any(|line| line.contains("returned ")
                            || line.contains("Tool lookup result:"))
                );
            }
        }
    }
}

#[tokio::test]
async fn argument_and_result_hooks_read_after_the_original_values_were_logged() {
    let _serial = SERIAL.lock().await;
    let _switch = Switch::set(true);
    for streaming in [false, true] {
        for block in [false, true] {
            let lines = Lines::default();
            let _capture = tracing::subscriber::set_default(lines.clone());
            let seen = Arc::new(Mutex::new(Vec::new()));
            let handling = Arc::clone(&seen);
            let handler = ToolHandler::new(move |_, arguments| {
                assert_eq!(arguments["query"], "synthetic-rewritten");
                handling.lock().unwrap().push("handler");
                async { Ok(ToolResult::Text("synthetic-original".into())) }
            });
            let turn = Turn::new(
                streaming,
                vec![call(json!({"query":"synthetic-model"}))],
                handler,
                None,
            )
            .await;
            let before_lines = lines.clone();
            let before_seen = Arc::clone(&seen);
            turn.kit
                .hook(
                    HookTrigger::BeforeToolUse,
                    move |_event: ToolCallEvent, _context| {
                        assert!(before_lines.read().iter().any(|line| {
                            line.contains("Tool lookup arguments: {\"query\": \"synthetic-model\"}")
                        }));
                        before_seen.lock().unwrap().push("before");
                        async {
                            let mut result = HookResult::allow(AllowOptions::default());
                            result
                                .metadata
                                .insert("arguments".into(), json!({"query":"synthetic-rewritten"}));
                            Ok(result)
                        }
                    },
                    HookOptions {
                        name: "before".into(),
                        ..Default::default()
                    },
                )
                .unwrap();
            let after_lines = lines.clone();
            let after_seen = Arc::clone(&seen);
            turn.kit.hook(HookTrigger::OnToolCall, move |event: ToolCallEvent, _context| {
                assert_eq!(event.result, Some(json!("synthetic-original")));
                assert!(after_lines.read().iter().any(|line| line.contains("Tool lookup result: synthetic-original")));
                after_seen.lock().unwrap().push("after");
                async move { if block { Ok(HookResult::block("synthetic-block", BlockOptions::default())) } else { let mut result = HookResult::allow(AllowOptions::default()); result.metadata.insert("result".into(), json!("synthetic-replacement")); Ok(result) } }
            }, HookOptions { name: "after".into(), ..Default::default() }).unwrap();
            let parts = turn.run().await;
            assert_eq!(*seen.lock().unwrap(), ["before", "handler", "after"]);
            assert!(parts[0].as_text().contains(if block {
                "synthetic-block"
            } else {
                "synthetic-replacement"
            }));
            assert!(
                !lines
                    .read()
                    .iter()
                    .any(|line| line.contains("Tool lookup result:")
                        && line.contains("synthetic-replacement"))
            );
        }
    }
}

#[tokio::test]
async fn rewritten_argument_diagnostic_has_no_json_envelope() {
    let _serial = SERIAL.lock().await;
    for streaming in [false, true] {
        let lines = Lines::default();
        let _capture = tracing::subscriber::set_default(lines.clone());
        let handler = ToolHandler::new(|_, _| async { panic!("a refused tool must not run") });
        let turn = Turn::new(
            streaming,
            vec![call(json!({"query":"synthetic-input"}))],
            handler,
            None,
        )
        .await;
        turn.kit
            .hook(
                HookTrigger::BeforeToolUse,
                |_event: ToolCallEvent, _context| async {
                    let mut answer = HookResult::allow(AllowOptions::default());
                    answer
                        .metadata
                        .insert("arguments".into(), json!({"query":42}));
                    answer
                },
                HookOptions {
                    name: "invalid rewrite".into(),
                    ..Default::default()
                },
            )
            .unwrap();
        let results = turn.run().await;
        assert!(results[0].is_error);
        let records = lines.read();
        let invalid: Vec<_> = records
            .iter()
            .filter(|line| line.contains("Invalid rewritten arguments"))
            .collect();
        assert_eq!(invalid.len(), 1);
        assert!(invalid[0].starts_with("WARN"));
        assert!(
            invalid[0].contains("Tool lookup: Invalid rewritten arguments for 'lookup':"),
            "{records:?}"
        );
    }
}
