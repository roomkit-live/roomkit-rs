//! The bounded and registry-backed forms of Python's TurnExecutor constructor.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::Value;

use super::support::channel;
use super::worker_controls::{Observed, envelope};
use crate::{RoomKitError, TurnExecutor, TurnExecutorOptions};

type Log = Arc<Mutex<Vec<(usize, &'static str)>>>;

fn make(options: TurnExecutorOptions) -> (TurnExecutor, Log, Arc<AtomicUsize>) {
    let log = Log::default();
    let calls = Arc::new(AtomicUsize::default());
    let executor = TurnExecutor::new(
        {
            let events = Arc::clone(&log);
            let count = Arc::clone(&calls);
            move |id| {
                std::future::ready(Ok(Some(Observed::built(
                    id,
                    count.fetch_add(1, Ordering::SeqCst),
                    Arc::clone(&events),
                ))))
            }
        },
        options,
    )
    .unwrap();
    (executor, log, calls)
}

async fn control(executor: &TurnExecutor, kind: &str, id: &str) {
    executor
        .control(envelope(kind, |m| {
            m["channel_id"] = id.into();
            m["organization_id"] = Value::Null;
            if kind == "committed_event" {
                m["room_hooks"] = serde_json::json!([]);
            }
            if kind == "channel_call" {
                m["previous_turn"] = Value::Null;
            }
        }))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_bound_evicts_the_least_recent_build_and_prefers_a_committed_event() {
    let (executor, log, builds) = make(TurnExecutorOptions {
        max_channels: Some(2),
        ..Default::default()
    });
    for (kind, id) in [
        ("channel_call", "alpha"),
        ("channel_call", "beta"),
        ("channel_call", "alpha"),
        ("committed_event", "gamma"),
        ("channel_call", "delta"),
        ("channel_call", "alpha"),
    ] {
        control(&executor, kind, id).await;
    }
    executor.close().await;
    assert_eq!(builds.load(Ordering::SeqCst), 4);
    let actions = log.lock().unwrap();
    assert_eq!(
        actions
            .iter()
            .filter(|(id, action)| *id == 0 && *action == "call")
            .count(),
        3
    );
    let closing = actions
        .iter()
        .filter(|(_, action)| *action == "close")
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    assert_eq!(closing.len(), 4);
    assert!(closing.iter().position(|id| *id == 2) < closing.iter().position(|id| *id == 0));
    assert!(closing.iter().position(|id| *id == 1) < closing.iter().position(|id| *id == 0));
}

#[tokio::test]
async fn a_host_owned_factory_is_read_every_time_and_never_closed() {
    let (executor, log, builds) = make(TurnExecutorOptions {
        owns_channels: false,
        max_channels: Some(1),
        ..Default::default()
    });
    for kind in [
        "room_attached",
        "channel_call",
        "channel_released",
        "committed_event",
        "room_detached",
    ] {
        control(&executor, kind, "alpha").await;
    }
    executor.close().await;
    assert_eq!(builds.load(Ordering::SeqCst), 4);
    assert_eq!(
        *log.lock().unwrap(),
        [(0, "attach"), (1, "call"), (2, "event"), (3, "detach")]
    );
}

#[test]
fn host_predicates_stay_local_and_the_bound_is_positive() {
    let factory = |_| std::future::ready(Ok(None::<Observed>));
    let dynamic = TurnExecutor::new_with_host_predicate(
        factory,
        |id| id.as_str().starts_with("bot-"),
        TurnExecutorOptions::default(),
    )
    .unwrap();
    assert!(dynamic.hosts_channel(&channel("bot-one")));
    assert!(!dynamic.hosts_channel(&channel("agent")));
    assert_eq!(dynamic.hosted(), None);
    let options = TurnExecutorOptions {
        max_channels: Some(0),
        ..Default::default()
    };
    assert!(matches!(
        TurnExecutor::new(factory, options),
        Err(RoomKitError::Value(_))
    ));
    let options = TurnExecutorOptions {
        hosts: Some(BTreeSet::from(["agent".into()])),
        ..Default::default()
    };
    assert!(TurnExecutor::new_with_host_predicate(factory, |_| true, options).is_err());
}
