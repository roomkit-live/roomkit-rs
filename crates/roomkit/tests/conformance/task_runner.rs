//! The kit's task runner and a task's completion callback, scripted for a
//! run (roomkit-specs `conformance/README.md`: "Setup" `kit.task_runner`;
//! "Calls" `delegate`'s `on_complete` and `task_runner.close`;
//! "Expectations" `completions`).
//!
//! A scripted task runner (RFC §23.1) refuses each task it is handed: its
//! `submit` fails with the runner's own failure, a `RoomKitError::Runtime`
//! `task runner refused: <message>`, and takes nothing, so the delegation
//! ends the task itself (RFC §23.3). A scripted completion callback records
//! the status of the result it was handed once it returned, or once it was
//! cut (the kit's close dropping it, RFC §12.10.4), after holding at its
//! gate.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use roomkit::RoomKitError;
use roomkit::tasks::{
    DelegatedTask, DelegatedTaskResult, OnCompleteCallback, SubmitOptions, TaskRunner,
};
use serde_json::{Value, json};

use crate::run::{Run, lock};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// The members of `kit.task_runner`.
pub(crate) const TASK_RUNNER: &[&str] = &["refuses"];
/// The members of `delegate`'s `on_complete`.
pub(crate) const ON_COMPLETE: &[&str] = &["name", "gate"];

/// What the scripted runner's `submit` fails with, before its message.
pub(crate) const REFUSED: &str = "task runner refused: ";

/// A task runner whose every `submit` refuses its task, which it never
/// takes, and so never ends (RFC §23.1).
pub(crate) struct RefusingTaskRunner {
    message: String,
}

impl<S: roomkit::ConversationStore> TaskRunner<S> for RefusingTaskRunner {
    fn submit(
        &self,
        _kit: roomkit::RoomKit<S>,
        _task: DelegatedTask,
        _options: SubmitOptions,
    ) -> BoxFuture<Result<(), RoomKitError>> {
        let refused = format!("{REFUSED}{}", self.message);
        Box::pin(async move { Err(RoomKitError::Runtime(refused)) })
    }

    fn cancel(&self, _task_id: &str) -> BoxFuture<bool> {
        Box::pin(async { false })
    }

    fn close(&self) -> BoxFuture<()> {
        Box::pin(async {})
    }
}

/// The kit's scripted task runner when the setup's `kit.task_runner` names
/// one.
pub(crate) fn task_runner<S: roomkit::ConversationStore>(
    kit: &Value,
) -> Option<Arc<dyn TaskRunner<S>>> {
    let message = kit["task_runner"]["refuses"].as_str()?;
    Some(Arc::new(RefusingTaskRunner {
        message: message.to_owned(),
    }))
}

/// Records a call of the completion callback `name` once it ends: cut
/// when dropped before it returned.
struct Ending {
    run: Weak<Run>,
    name: String,
    status: Value,
    returned: bool,
}

impl Ending {
    fn record(&self, cut: bool) {
        if let Some(run) = self.run.upgrade() {
            run.record_completion(&self.name, json!({ "status": self.status, "cut": cut }));
        }
    }
}

impl Drop for Ending {
    fn drop(&mut self) {
        if !self.returned {
            self.record(true);
        }
    }
}

/// A scripted completion callback (`delegate`'s `on_complete`): it holds at
/// its gate, then returns; each call is recorded under `completions` by its
/// name once it returned, or once it was cut.
pub(crate) fn completion_callback(run: &Arc<Run>, spec: &Value) -> Option<OnCompleteCallback> {
    let name = spec["name"].as_str()?.to_owned();
    let gate = spec["gate"].as_str().map(str::to_owned);
    let run = Arc::downgrade(run);
    Some(Arc::new(move |result: DelegatedTaskResult| {
        let (run, name, gate) = (Weak::clone(&run), name.clone(), gate.clone());
        Box::pin(async move {
            let mut ending = Ending {
                run: Weak::clone(&run),
                name,
                status: json!(result.status),
                returned: false,
            };
            if let (Some(gate), Some(held)) = (gate, run.upgrade()) {
                held.gates.hold(&gate).await;
            }
            ending.returned = true;
            ending.record(false);
        })
    }))
}

impl Run {
    /// A call of the scripted completion callback `name` that ended.
    pub(crate) fn record_completion(&self, name: &str, call: Value) {
        if self.recording.load(std::sync::atomic::Ordering::SeqCst) {
            lock(&self.observed)
                .completions
                .entry(name.to_owned())
                .or_default()
                .push(call);
        }
    }
}
