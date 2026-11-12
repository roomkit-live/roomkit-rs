//! Preparation interrupted before announcement leaves no child task pending.

use super::*;
use pretty_assertions::assert_eq;

#[path = "preparation_cleanup.rs"]
mod cleanup;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Boundary {
    BeforeCreate,
    AfterCreate,
    BeforeAnnounce,
}

#[derive(Debug)]
pub(super) struct Preparation {
    boundary: Boundary,
    armed: AtomicBool,
    fails: AtomicBool,
    reached: Notify,
    release: Notify,
    pub(super) read_fails: AtomicBool,
    pub(super) foreign: AtomicBool,
    write_fails: AtomicBool,
    write_held: AtomicBool,
    write_entered: Notify,
    write_release: Notify,
}

impl Preparation {
    fn new(boundary: Boundary) -> Self {
        Self {
            boundary,
            armed: AtomicBool::new(false),
            fails: AtomicBool::new(true),
            reached: Notify::new(),
            release: Notify::new(),
            read_fails: AtomicBool::new(false),
            foreign: AtomicBool::new(false),
            write_fails: AtomicBool::new(false),
            write_held: AtomicBool::new(false),
            write_entered: Notify::new(),
            write_release: Notify::new(),
        }
    }

    pub(super) async fn stop(&self, boundary: Boundary) -> Result<(), StoreError> {
        if self.boundary != boundary || !self.armed.swap(false, Ordering::SeqCst) {
            return Ok(());
        }
        self.reached.notify_one();
        self.release.notified().await;
        if self.fails.load(Ordering::SeqCst) {
            return Err(StoreError::Unavailable(StoreFailure::new(
                "preparation failed",
            )));
        }
        Ok(())
    }

    pub(super) async fn patch(&self) -> Result<(), StoreError> {
        if self.write_held.swap(false, Ordering::SeqCst) {
            self.write_entered.notify_one();
            self.write_release.notified().await;
        }
        if self.write_fails.load(Ordering::SeqCst) {
            return Err(StoreError::Unavailable(StoreFailure::new(
                "cleanup unavailable",
            )));
        }
        Ok(())
    }
}

// Python: tests/tasks/test_delegation_preparation.py::test_interrupted_preparation_keeps_no_child_task_pending
#[tokio::test]
async fn interrupted_preparation_keeps_no_child_task_pending() {
    let mut observations = Vec::new();
    for boundary in [
        Boundary::BeforeCreate,
        Boundary::AfterCreate,
        Boundary::BeforeAnnounce,
    ] {
        for wait in [true, false] {
            for cancelled in [true, false] {
                let observed = interrupted(boundary, wait, cancelled).await;
                let expected = if boundary == Boundary::BeforeCreate && !cancelled {
                    Vec::new()
                } else {
                    vec![if cancelled { "cancelled" } else { "failed" }.to_owned()]
                };
                observations.push((
                    format!("{boundary:?}, wait={wait}, cancelled={cancelled}"),
                    observed,
                    expected,
                ));
            }
        }
    }
    let mismatches: Vec<_> = observations
        .into_iter()
        .filter(|(_, actual, expected)| actual != expected)
        .collect();
    assert_eq!(mismatches, Vec::new());
}

async fn interrupted(boundary: Boundary, wait: bool, cancelled: bool) -> Vec<String> {
    let telemetry = Arc::new(MockTelemetryProvider::default());
    let kit = kit_configured(RoomKitOptions {
        telemetry: Some(Arc::clone(&telemetry).into()),
        ..RoomKitOptions::with_store(RoomReadsFail {
            preparation: Some(Preparation::new(boundary)),
            ..Default::default()
        })
    })
    .await;
    let heard: Arc<Mutex<Vec<&'static str>>> = Arc::default();
    for (trigger, name) in [
        (HookTrigger::OnTaskDelegated, "delegated"),
        (HookTrigger::OnTaskCompleted, "completed"),
    ] {
        let seen = Arc::clone(&heard);
        kit.hook(
            trigger,
            move |_: RoomEvent, _| {
                seen.lock().unwrap().push(name);
                ready(())
            },
            HookOptions {
                execution: HookExecution::Async,
                ..Default::default()
            },
        )
        .unwrap();
    }
    let seen = Arc::clone(&heard);
    let options = DelegateOptions {
        wait,
        on_complete: Some(Arc::new(move |_: DelegatedTaskResult| {
            seen.lock().unwrap().push("callback");
            Box::pin(async {})
        })),
        ..Default::default()
    };
    let preparation = kit.store().preparation.as_ref().unwrap();
    preparation.fails.store(!cancelled, Ordering::SeqCst);
    preparation.armed.store(true, Ordering::SeqCst);
    let (parent, worker) = (room(), channel("worker"));
    let mut call = Box::pin(kit.delegate(&parent, &worker, "Count.", options));
    tokio::time::timeout(BOUND, async {
        tokio::select! {
            result = &mut call => panic!("delegation returned before its gate: {result:?}"),
            () = preparation.reached.notified() => (),
        }
    })
    .await
    .unwrap();
    if cancelled {
        drop(call);
        // Room creation runs on the kit's lifecycle task. Let that write
        // settle after its caller was dropped, before inspecting its child.
        preparation.release.notify_one();
    } else {
        preparation.release.notify_one();
        let error = tokio::time::timeout(BOUND, call)
            .await
            .unwrap()
            .err()
            .expect("the preparation's failure reaches its caller");
        let cause = std::error::Error::source(&error)
            .map(ToString::to_string)
            .unwrap_or_else(|| error.to_string());
        assert!(
            cause.contains("preparation failed"),
            "wrong error: {error}: {cause}"
        );
    }
    tokio::time::timeout(BOUND, idle(&kit)).await.unwrap();
    let children = kit
        .store()
        .list_rooms(store::ListRoomsOptions::default())
        .await
        .unwrap();
    let mut statuses = Vec::new();
    for child in children.into_iter().filter(|child| child.id != room()) {
        assert_eq!(child.metadata["parent_room_id"], "r");
        assert_eq!(
            kit.store()
                .get_event_count(&child.id, store::GetEventCountOptions::default())
                .await
                .unwrap(),
            u64::from(boundary == Boundary::BeforeAnnounce)
        );
        statuses.push(child.metadata["task_status"].as_str().unwrap().to_owned());
    }
    assert!(
        kit.status_bus()
            .recent(RecentOptions::default())
            .await
            .is_empty()
    );
    kit.close().await.unwrap();
    assert!(heard.lock().unwrap().is_empty());
    let spans = telemetry.get_spans(SpanKind::Delegation);
    assert_eq!(spans.len(), 1);
    assert_eq!(
        spans[0].status,
        if cancelled { "cancelled" } else { "error" }
    );
    assert!(telemetry.get_active_spans().is_empty());
    statuses
}
