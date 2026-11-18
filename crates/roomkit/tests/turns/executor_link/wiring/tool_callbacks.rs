//! A channel obtains its four text tool callbacks from a standalone executor.
use super::{Env, link, worker};
use roomkit::model::tool_verdict::VerdictResult;
use roomkit::model::{
    ChannelId, ChannelType, HookResult, Metadata, RoomContext, hook::AllowOptions,
};
use roomkit::turns::HookOptions;
use roomkit::{HookExecution, HookTrigger, ToolCallEvent};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn application_callbacks_keep_the_result_claim_and_original_external_report() {
    let (link, _, _) = link(false);
    let req = &link.request;
    let worker = worker();
    let env = Arc::new(Env::new(req.context.clone()));
    worker.use_wiring(Arc::<Env>::clone(&env));
    let tools = worker.wiring().tools();
    let channel = ChannelId::new("bound").unwrap();
    let gate = tools.build_before_tool_call_hook(&channel);
    let judge = tools.build_tool_call_hook(&channel);
    let report = tools.build_tool_report_hook(&channel);
    let observe = tools.build_tool_observer_hook(&channel);
    let mut event = ToolCallEvent::new(
        req.channel_id.clone(),
        ChannelType::new("custom").unwrap(),
        "c",
        "lookup",
        Metadata::new(),
    );
    assert!(gate(event.clone()).await.unwrap().allowed);
    assert!(judge(event.clone(), None).await.unwrap().is_none());
    assert!(env.events.lock().unwrap().is_empty());
    event.room_id = Some(req.context.room.id.clone());
    assert!(gate(event.clone()).await.unwrap().allowed);
    let sync_count = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&sync_count);
    worker
        .hook(
            HookTrigger::OnToolCall,
            move |_: ToolCallEvent, _: RoomContext| {
                count.fetch_add(1, Ordering::Relaxed);
                async {
                    let mut answer = HookResult::allow(AllowOptions::default());
                    answer
                        .metadata
                        .insert("result".into(), json!({"quote": "é", "float": 1e-5}));
                    answer
                }
            },
            HookOptions::default(),
        )
        .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    worker
        .hook(
            HookTrigger::OnToolCall,
            move |e: ToolCallEvent, _: RoomContext| {
                let tx = tx.clone();
                async move {
                    tx.send(e).unwrap();
                }
            },
            HookOptions {
                execution: HookExecution::Async,
                ..Default::default()
            },
        )
        .unwrap();
    event.result = Some(json!("original"));
    let ignored = judge(event.clone(), Some(Arc::new(|| false)))
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(ignored.result(), Some(VerdictResult::Text(s)) if s == r#"{"quote": "é", "float": 1e-05}"#)
    );
    let applied = judge(event.clone(), None).await.unwrap().unwrap();
    assert!(!applied.blocked());
    let rewritten = rx.recv().await.unwrap();
    assert_eq!(
        rewritten.result,
        Some(json!(r#"{"quote": "é", "float": 1e-05}"#))
    );
    report(event.clone()).await.unwrap();
    assert_eq!(rx.recv().await.unwrap().result, Some(json!("original")));
    event.refused = true;
    observe(event.clone()).await.unwrap();
    assert!(rx.recv().await.unwrap().refused);
    report(event).await.unwrap();
    assert!(rx.recv().await.unwrap().refused);
    assert_eq!(sync_count.load(Ordering::Relaxed), 3);
    assert_eq!(
        &*env.events.lock().unwrap(),
        &[
            "before_tool_use",
            "tool_call",
            "tool_call",
            "tool_call",
            "tool_call"
        ]
    );
    let fields = env.fields.lock().unwrap().clone();
    assert!(
        fields
            .iter()
            .all(|options| options.channel_id.as_ref() == Some(&channel))
    );
    assert_eq!(fields[1].data.as_ref().unwrap()["channel_type"], "custom");
    worker.close().await;
}

#[tokio::test]
async fn public_tool_verdict_distinguishes_an_absent_result_a_clear_and_normalized_parts() {
    use roomkit::model::tool_verdict::VerdictValue;
    let (link, _, _) = link(false);
    let req = &link.request;
    let worker = worker();
    worker.use_wiring(Arc::new(Env::new(req.context.clone())));
    let judge = worker
        .wiring()
        .tools()
        .build_tool_call_hook(&req.channel_id);
    let mut event = ToolCallEvent::new(
        req.channel_id.clone(),
        ChannelType::AI,
        "c",
        "unchanged",
        Metadata::new(),
    );
    event.room_id = Some(req.room_id.clone());
    let unchanged = judge(event.clone(), None).await.unwrap().unwrap();
    assert!(unchanged.result().is_none());
    assert!(!unchanged.replaces_structured());
    worker
        .hook(
            HookTrigger::OnToolCall,
            |e: ToolCallEvent, _: RoomContext| async move {
                let mut answer = HookResult::allow(AllowOptions::default());
                let data = match e.name.as_str() {
                    "clear" => json!(null),
                    "image" => json!([{"type":"image","url":"example.png"}]),
                    _ => json!([{"type":"text","text":"x","extra":"kept"}]),
                };
                answer.metadata.insert("result".into(), data);
                answer
                    .metadata
                    .insert("structured_content".into(), json!({"ok":true}));
                answer
            },
            HookOptions::default(),
        )
        .unwrap();
    event.result = Some(json!("original"));
    event.name = "clear".into();
    let cleared = judge(event.clone(), None).await.unwrap().unwrap();
    assert!(matches!(cleared.result(),Some(VerdictResult::Text(s)) if s=="null"));
    assert!(cleared.replaces_structured());
    assert!(
        matches!(cleared.structured_content().unwrap().get("ok"),Some(VerdictValue::Json(value)) if value==true)
    );
    event.name = "image".into();
    let image = judge(event.clone(), None).await.unwrap().unwrap();
    let Some(VerdictResult::Parts(parts)) = image.result() else {
        panic!("missing parts")
    };
    assert!(
        matches!(&parts.snapshot()[..], [VerdictValue::Json(part)] if *part==json!({"type":"image", "url":"example.png","mime_type":null}))
    );
    event.name = "data".into();
    let data = judge(event, None).await.unwrap().unwrap();
    assert!(
        matches!(data.result(),Some(VerdictResult::Text(s)) if s.contains(r#""extra": "kept""#))
    );
    worker.close().await;
}

#[tokio::test]
async fn the_gate_denies_an_unreadable_room_and_a_fail_closed_result_is_withheld() {
    let (link, _, _) = link(false);
    let req = &link.request;
    let worker = worker();
    for trigger in [HookTrigger::BeforeToolUse, HookTrigger::OnToolCall] {
        worker
            .hook(
                trigger,
                |_: ToolCallEvent, _: RoomContext| async {
                    HookResult::allow(AllowOptions::default())
                },
                HookOptions {
                    fail_closed: true,
                    name: if trigger == HookTrigger::OnToolCall {
                        "guard"
                    } else {
                        "gate"
                    }
                    .into(),
                    ..Default::default()
                },
            )
            .unwrap();
    }
    let mut e = ToolCallEvent::new(
        req.channel_id.clone(),
        ChannelType::AI,
        "c",
        "lookup",
        Metadata::new(),
    );
    e.room_id = Some(req.room_id.clone());
    e.result = Some(json!("internal value"));
    let tools = worker.wiring().tools();
    assert!(
        !tools.build_before_tool_call_hook(&req.channel_id)(e.clone())
            .await
            .unwrap()
            .allowed
    );
    let verdict = tools.build_tool_call_hook(&req.channel_id)(e, None)
        .await
        .unwrap()
        .unwrap();
    assert!(verdict.blocked());
    assert!(
        matches!(verdict.result(),Some(VerdictResult::Text(s)) if s == r#"{"error": "hook_error:guard"}"#)
    );
    worker.close().await;
}
