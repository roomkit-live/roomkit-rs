//! Owner-served tools keep their call context and record (RFC §10.6.5).
//! Python: `tests/test_turns_owner_tools.py` and `test_turns_owner_tool_records.py`.

mod support;

use std::sync::{Arc, Mutex};

use roomkit_core::model::Metadata;
use serde_json::{Value, json};
use tokio::sync::Barrier;

use self::support::{call, kit, serve};
use super::support::says;
use crate::turn::OwnerToolAnswer;
use crate::{current_tool_call_scope, current_turn_host, current_turn_record};

#[tokio::test]
async fn an_owner_tool_reads_the_context_its_executor_carried() {
    let seen = Arc::new(Mutex::new(None));
    let executor = Arc::clone(&seen);
    let kit = kit(
        Arc::new(move || {
            let seen = Arc::clone(&executor);
            Box::pin(async move {
                let own_task = tokio::task::id();
                let answer = call(serde_json::from_value(json!({"q": "x"})).unwrap()).await;
                *seen.lock().unwrap() = Some((own_task, answer));
            })
        }),
        Metadata::new(),
    )
    .await;
    let owner = Arc::new(Mutex::new(None));
    let held = Arc::clone(&owner);
    serve(&kit, move |call| {
        let held = Arc::clone(&held);
        Box::pin(async move {
            let scope = current_tool_call_scope().map(|scope| scope.owner_context());
            *held.lock().unwrap() = Some((
                tokio::task::id(),
                scope,
                current_turn_record().is_some(),
                current_turn_host().is_some(),
            ));
            OwnerToolAnswer::Served(Value::Object(call.arguments))
        })
    });
    says(&kit, "go").await.unwrap();
    kit.close().await.unwrap();
    let (owner_task, context, record, host) = owner.lock().unwrap().take().unwrap();
    let (executor_task, answer) = seen.lock().unwrap().take().unwrap();
    assert_ne!(owner_task, executor_task);
    assert_eq!(answer, OwnerToolAnswer::Served(json!({"q": "x"})));
    let context = context.expect("owner installs the carried tool scope");
    assert_eq!(
        context.room_id.as_ref().map(ToString::to_string).as_deref(),
        Some("room_1")
    );
    assert_eq!(context.room.unwrap().id.as_str(), "room_1");
    assert_eq!(context.actor_id.as_deref(), Some("alice"));
    assert_eq!(context.requester.as_deref(), Some("Coordinator"));
    assert_eq!(context.allowed_names, Some(["probe".to_owned()].into()));
    assert_eq!(context.chain_depth, 3);
    let identity = context.call.unwrap();
    assert_eq!(identity.tool_call_id, "c1");
    assert_eq!(identity.channel_id.unwrap().as_str(), "ai");
    assert!(record);
    assert!(!host);
}

#[tokio::test]
async fn record_changes_are_applied_before_every_owner_tool_outcome() {
    for answer in [
        OwnerToolAnswer::Served(json!("ok")),
        OwnerToolAnswer::Refused("no".into()),
        OwnerToolAnswer::Unserved("absent".into()),
        OwnerToolAnswer::Failed {
            kind: "ValueError".into(),
            message: "failed".into(),
        },
    ] {
        let seen = Arc::new(Mutex::new(None));
        let executor = Arc::clone(&seen);
        let kit = kit(
            Arc::new(move || {
                let seen = Arc::clone(&executor);
                Box::pin(async move {
                    let answer = call(Metadata::new()).await;
                    *seen.lock().unwrap() =
                        Some((answer, current_turn_record().unwrap().get("learned")));
                })
            }),
            Metadata::new(),
        )
        .await;
        let returned = answer.clone();
        serve(&kit, move |_| {
            let answer = returned.clone();
            Box::pin(async move {
                if let Some(record) = current_turn_record() {
                    record.insert("learned", json!(true));
                }
                answer
            })
        });
        let result = says(&kit, "go").await.unwrap();
        kit.close().await.unwrap();
        assert_eq!(
            seen.lock().unwrap().take(),
            Some((answer, Some(json!(true))))
        );
        assert_eq!(result.response_metadata.get("learned"), Some(json!(true)));
    }
}

#[tokio::test]
async fn parallel_owner_tools_merge_only_changes_to_independent_copies() {
    let snapshots = Arc::new(Mutex::new(Vec::new()));
    let kit = kit(
        Arc::new(|| {
            Box::pin(async {
                let b = serde_json::from_value(json!({"name": "b"})).unwrap();
                let c = serde_json::from_value(json!({"name": "c"})).unwrap();
                let answers = tokio::join!(call(b), call(c));
                assert_eq!(
                    answers,
                    (
                        OwnerToolAnswer::Served(json!("b")),
                        OwnerToolAnswer::Served(json!("c"))
                    )
                );
            })
        }),
        serde_json::from_value(json!({"names": {"a": 1}, "stale": true})).unwrap(),
    )
    .await;
    let both = Arc::new(Barrier::new(2));
    let held = Arc::clone(&snapshots);
    serve(&kit, move |call| {
        let both = Arc::clone(&both);
        let held = Arc::clone(&held);
        Box::pin(async move {
            let Some(record) = current_turn_record() else {
                return OwnerToolAnswer::Failed {
                    kind: "MissingRecord".into(),
                    message: "owner has no record".into(),
                };
            };
            let name = call.arguments["name"].as_str().unwrap();
            let before = record.get("names");
            both.wait().await;
            record.with_value("names", |value| {
                let names = value.as_object_mut().unwrap();
                names.insert(name.to_owned(), json!(1));
                if name == "b" {
                    names.shift_remove("a");
                }
            });
            if name == "b" {
                record.pop("stale");
            } else {
                record.insert("new", json!("kept"));
            }
            both.wait().await;
            held.lock()
                .unwrap()
                .push((name.to_owned(), before, record.get("names")));
            OwnerToolAnswer::Served(json!(name))
        })
    });
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), says(&kit, "go"))
        .await
        .unwrap()
        .unwrap();
    kit.close().await.unwrap();
    let mut snapshots = snapshots.lock().unwrap().clone();
    snapshots.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        snapshots,
        vec![
            ("b".into(), Some(json!({"a": 1})), Some(json!({"b": 1}))),
            (
                "c".into(),
                Some(json!({"a": 1})),
                Some(json!({"a": 1, "c": 1}))
            )
        ]
    );
    assert_eq!(
        result.response_metadata.get("names"),
        Some(json!({"b": 1, "c": 1}))
    );
    assert_eq!(result.response_metadata.get("new"), Some(json!("kept")));
    assert_eq!(result.response_metadata.get("stale"), None);
}

#[tokio::test]
async fn another_room_or_channel_is_refused_before_the_handler_runs() {
    for room in [false, true] {
        let seen = Arc::new(Mutex::new(None));
        let executor = Arc::clone(&seen);
        let kit = kit(
            Arc::new(move || {
                let seen = Arc::clone(&executor);
                Box::pin(async move {
                    let mut context = current_tool_call_scope().unwrap().owner_context();
                    if room {
                        context.room_id = Some("other".parse().unwrap());
                    } else {
                        context.call.as_mut().unwrap().channel_id = Some("other".parse().unwrap());
                    }
                    let answer = crate::within_tool_call_scope(
                        Arc::new(support::Scope::new(context)),
                        call(Metadata::new()),
                    )
                    .await;
                    *seen.lock().unwrap() = Some(answer);
                })
            }),
            Metadata::new(),
        )
        .await;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        serve(&kit, move |_| {
            counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Box::pin(async { OwnerToolAnswer::Served(json!("ran")) })
        });
        says(&kit, "go").await.unwrap();
        kit.close().await.unwrap();
        assert!(matches!(
            seen.lock().unwrap().as_ref(),
            Some(OwnerToolAnswer::Unserved(_))
        ));
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    }
}

#[test]
fn tool_results_keep_record_changes_and_default_the_missing_fields() {
    let message = json!({"type": "tool_result", "call_id": "c1", "outcome": "served", "result": {"value": "ok"}});
    let legacy: crate::turns::wire::Control = serde_json::from_value(message.clone()).unwrap();
    let rewritten = serde_json::to_value(legacy).unwrap();
    assert_eq!(rewritten.get("record_changes"), Some(&Value::Null));
    assert_eq!(rewritten.get("structured_content"), Some(&Value::Null));
    let mut current = message;
    current["record_changes"] = json!({"written": {"read": true}, "removed": ["stale"]});
    current["structured_content"] = json!({"rows": 2});
    let parsed: crate::turns::wire::Control = serde_json::from_value(current.clone()).unwrap();
    let rewritten = serde_json::to_value(parsed).unwrap();
    assert_eq!(rewritten["record_changes"], current["record_changes"]);
    assert_eq!(
        rewritten["structured_content"],
        current["structured_content"]
    );
}

#[tokio::test]
async fn a_structured_copy_returns_to_the_executor_only_after_a_served_call() {
    for served in [false, true] {
        let seen = Arc::new(Mutex::new(None));
        let executor = Arc::clone(&seen);
        let kit = kit(
            Arc::new(move || {
                let seen = Arc::clone(&executor);
                Box::pin(async move {
                    let answer = call(Metadata::new()).await;
                    *seen.lock().unwrap() = Some((
                        answer,
                        current_tool_call_scope().unwrap().structured_content(),
                    ));
                })
            }),
            Metadata::new(),
        )
        .await;
        serve(&kit, move |_| {
            Box::pin(async move {
                let scope = current_tool_call_scope()
                    .unwrap()
                    .into_any()
                    .downcast::<crate::OwnerToolScope>()
                    .unwrap();
                let adapter =
                    scope.adapter(|context| Arc::new(support::Scope::new(context.clone())));
                adapter.set_structured_content(serde_json::from_value(json!({"rows": 2})).unwrap());
                if served {
                    OwnerToolAnswer::Served(json!("ok"))
                } else {
                    OwnerToolAnswer::Refused("no".into())
                }
            })
        });
        says(&kit, "go").await.unwrap();
        kit.close().await.unwrap();
        let (answer, copy) = seen.lock().unwrap().take().unwrap();
        assert_eq!(matches!(answer, OwnerToolAnswer::Served(_)), served);
        assert_eq!(
            copy,
            served.then(|| serde_json::from_value(json!({"rows": 2})).unwrap())
        );
    }
}
