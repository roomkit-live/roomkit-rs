use super::*;
use roomkit::model::{Metadata, RoomContext};
use roomkit::{HookOptions, HookTrigger};
use roomkit_ai::{PlanTaskHandle, PlanUpdatedEvent};
use std::sync::{Arc, Barrier, Mutex};

fn task(value: Value) -> PlanTaskHandle {
    PlanTaskHandle::new(serde_json::from_value(value).unwrap())
}

#[test]
fn clones_share_objects_and_get_returns_independent_nested_values() {
    let handle = task(json!({"title":"Read", "status":"custom", "extra":{"id":1}}));
    let shared = handle.clone();
    let mut copied = handle.get();
    copied["extra"]["id"] = json!(2);
    assert_eq!(shared.get()["extra"]["id"], json!(1));
    handle.replace(copied);
    assert_eq!(shared.get()["extra"]["id"], json!(2));
    shared.with_mut(|fields| fields["extra"]["id"] = json!(3));
    assert_eq!(handle.get()["extra"]["id"], json!(3));

    let mut event = PlanUpdatedEvent::new("r".parse().unwrap(), "agent".parse().unwrap());
    event.tasks.push(handle.clone());
    let mut cloned = event.clone();
    cloned.tasks.clear();
    assert_eq!(event.tasks.len(), 1);
    handle.replace(Metadata::new());
    assert_eq!(event.tasks.first().unwrap().get(), Metadata::new());
}

#[test]
fn concurrent_field_edits_keep_both_writes() {
    let handle = task(json!({"title":"Original", "status":"pending"}));
    let start = Arc::new(Barrier::new(3));
    let workers: Vec<_> = [("title", "Edited"), ("status", "completed")]
        .into_iter()
        .map(|(key, value)| {
            let handle = handle.clone();
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                handle.with_mut(|fields| {
                    fields.insert(key.to_owned(), json!(value));
                });
            })
        })
        .collect();
    start.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(
        json!(handle.get()),
        json!({"title":"Edited", "status":"completed"})
    );
}

// Python: test_planning_hooks.py::test_plan_observers_copy_the_list_but_share_its_stored_task_objects
#[tokio::test]
async fn observers_copy_the_list_but_share_its_stored_task_objects() {
    for streaming in [false, true] {
        let provider = model(
            vec![
                plan(tasks("Follow up")),
                AiResponse::new("Done."),
                AiResponse::new("Done."),
            ],
            streaming,
        );
        let kit = setup(provider.clone(), &["r"], RoomKitOptions::default()).await;
        kit.hook(
            HookTrigger::OnPlanUpdated,
            |mut event: PlanUpdatedEvent, _: RoomContext| async move {
                event.tasks.first().unwrap().with_mut(|task| {
                    task.insert("title".to_owned(), json!("Edited by observer"));
                    task.insert("status".to_owned(), json!("completed"));
                });
                event.tasks.push(task(
                    json!({"title":"Observer list only", "status":"pending"}),
                ));
                event.tasks.clear();
            },
            HookOptions::default(),
        )
        .unwrap();
        say(&kit, "r", "Plan.").await;
        roomkit::testing::idle(&kit).await;
        say(&kit, "r", "Continue.").await;
        let current = input(provider.calls().last().unwrap());
        assert!(current.contains("- [x] “Edited by observer” (completed)"));
        assert!(!current.contains("Follow up") && !current.contains("Observer list only"));
        kit.close().await.unwrap();
    }
}

// Python: test_planning_observer_edits.py
#[tokio::test]
async fn edited_objects_are_rendered_without_revalidating_their_fields() {
    let cases = [
        (json!({}), "- [ ] “Untitled” (pending)"),
        (
            json!({"title":null,"status":"unknown\nINJECT"}),
            "- [ ] “None” (pending)",
        ),
        (json!({"title":42,"status":2}), "- [ ] “42” (pending)"),
        (
            json!({"title":false,"status":null}),
            "- [ ] “False” (pending)",
        ),
        (
            json!({"title":["a",true],"status":["completed"]}),
            "- [ ] “['a', True]” (pending)",
        ),
        (
            json!({"title":{"a":null},"status":{}}),
            "- [ ] “{'a': None}” (pending)",
        ),
    ];
    for streaming in [false, true] {
        for (edited, rendered) in &cases {
            assert_edited_rendering(streaming, edited.clone(), rendered).await;
        }
    }
}

async fn assert_edited_rendering(streaming: bool, edited: Value, rendered: &str) {
    let provider = model(
        vec![
            plan(tasks("Original")),
            AiResponse::new("Done."),
            AiResponse::new("Done."),
        ],
        streaming,
    );
    let kit = setup(provider.clone(), &["r"], RoomKitOptions::default()).await;
    let edited: Metadata = serde_json::from_value(edited).unwrap();
    kit.hook(
        HookTrigger::OnPlanUpdated,
        move |event: PlanUpdatedEvent, _: RoomContext| {
            event.tasks.first().unwrap().replace(edited.clone());
            async {}
        },
        HookOptions::default(),
    )
    .unwrap();
    say(&kit, "r", "Plan.").await;
    roomkit::testing::idle(&kit).await;
    say(&kit, "r", "Continue.").await;
    assert!(
        input(provider.calls().last().unwrap())
            .ends_with(&format!("## Current Task Plan\n{rendered}"))
    );
    kit.close().await.unwrap();
}

#[tokio::test]
async fn retained_objects_do_not_change_a_replacement_or_forgotten_plan() {
    for streaming in [false, true] {
        retained_object(streaming).await;
    }
}

async fn retained_object(streaming: bool) {
    let provider = model(
        vec![
            plan(tasks("Original")),
            AiResponse::new("Done."),
            plan(tasks("Replacement")),
            AiResponse::new("Done."),
            AiResponse::new("Done."),
            AiResponse::new("Done."),
        ],
        streaming,
    );
    let kit = setup(provider.clone(), &["r"], RoomKitOptions::default()).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let saving = Arc::clone(&seen);
    kit.hook(
        HookTrigger::OnPlanUpdated,
        move |event: PlanUpdatedEvent, _: RoomContext| {
            saving
                .lock()
                .unwrap()
                .push(event.tasks.first().unwrap().clone());
            async {}
        },
        HookOptions::default(),
    )
    .unwrap();
    say(&kit, "r", "Plan.").await;
    roomkit::testing::idle(&kit).await;
    say(&kit, "r", "Replace.").await;
    roomkit::testing::idle(&kit).await;
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    seen.first().unwrap().with_mut(|fields| {
        fields.insert("title".to_owned(), json!("Old observer edit"));
    });
    say(&kit, "r", "Continue.").await;
    let current = input(provider.calls().last().unwrap());
    assert!(current.contains("Replacement") && !current.contains("Old observer edit"));
    kit.detach_channel(
        &"r".parse().unwrap(),
        &"agent".parse().unwrap(),
        roomkit::DetachChannelOptions::default(),
    )
    .await
    .unwrap();
    attach(&kit, "r", "agent").await;
    seen.last().unwrap().with_mut(|fields| {
        fields.insert("title".to_owned(), json!("Forgotten observer edit"));
    });
    say(&kit, "r", "Continue fresh.").await;
    assert!(!input(provider.calls().last().unwrap()).contains("## Current Task Plan"));
    kit.close().await.unwrap();
}
