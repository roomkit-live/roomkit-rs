//! RFC §12.10.4: a delegation's terminal callback can own or join shutdown.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::TaskStatus;
use roomkit::tasks::OnCompleteCallback;
use roomkit::{CancelTaskOptions, DelegateOptions};
use roomkit_ai::Tools;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};

use crate::delegation::worker;
use crate::held::{closed, spawn_close};
use crate::stream_witness::WaitingModel;
use crate::support::{Closer, Witness, channel, kit_with, room_id, sealed};

fn closing_callback(closer: &Closer, endings: &Arc<Mutex<Vec<TaskStatus>>>) -> OnCompleteCallback {
    let (closer, endings) = (closer.clone(), Arc::clone(endings));
    Arc::new(move |result| {
        let (closer, endings) = (closer.clone(), Arc::clone(&endings));
        Box::pin(async move {
            endings.lock().unwrap().push(result.status);
            closer.close().await;
        })
    })
}

async fn close_from_end(cancelled: bool, joining: bool) {
    let witness = Witness::new();
    let kit = kit_with(&witness).await;
    let closer = Closer::new(&kit, &witness, joining);
    let endings = Arc::new(Mutex::new(Vec::new()));
    let waiting = WaitingModel::new("Working ", false);
    if cancelled {
        worker(&kit, waiting.clone(), Tools::new());
    } else {
        worker(
            &kit,
            ScriptedProvider::new([ScriptedRound::answering("Done.")]),
            Tools::new(),
        );
    }
    let task = kit
        .delegate(
            &room_id(),
            &channel("worker"),
            "Go.",
            DelegateOptions {
                on_complete: Some(closing_callback(&closer, &endings)),
                ..DelegateOptions::default()
            },
        )
        .await
        .unwrap();
    let cancel = if cancelled {
        tokio::time::timeout(Duration::from_secs(1), waiting.started.notified())
            .await
            .unwrap();
        Some(tokio::spawn({
            let kit = kit.clone();
            let id = task.id.clone();
            async move { kit.cancel_task(&id, CancelTaskOptions::default()).await }
        }))
    } else {
        None
    };
    tokio::time::timeout(Duration::from_secs(1), closer.entered.notified())
        .await
        .unwrap();
    if joining {
        let mut first = spawn_close(&kit);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut first)
                .await
                .is_err()
        );
        closer.go.notify_one();
        closed(first).await;
    }
    assert_eq!(closer.returned_once_closed().await, Some(true));
    if let Some(cancel) = cancel {
        assert!(
            tokio::time::timeout(Duration::from_secs(1), cancel)
                .await
                .unwrap()
                .unwrap()
        );
    }
    tokio::time::timeout(Duration::from_secs(1), roomkit::testing::idle(&kit))
        .await
        .unwrap();
    let expected = if cancelled {
        TaskStatus::Cancelled
    } else {
        TaskStatus::Completed
    };
    assert_eq!(*endings.lock().unwrap(), [expected]);
    assert_eq!(task.result().unwrap().status, expected);
    assert!(sealed(&kit).await);
}

// Python: tests/test_kit_close_doors.py::test_a_close_from_the_kit_s_own_work_returns_and_closes_the_kit
// Python: tests/test_kit_close_doors.py::test_a_close_from_the_kit_s_own_work_joining_another_spares_that_work
// The delegation-end and cancelled-delegation-end doors of both tests.
#[tokio::test(start_paused = true)]
async fn a_delegations_terminal_callback_can_own_or_join_the_close() {
    for cancelled in [false, true] {
        for joining in [false, true] {
            close_from_end(cancelled, joining).await;
        }
    }
}
