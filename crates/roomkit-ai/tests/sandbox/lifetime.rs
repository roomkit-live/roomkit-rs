use std::future::{pending, ready};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::{Metadata, ToolCallOutcome};
use roomkit::{Channel, RoomKit, RoomKitOptions};
use roomkit_ai::{AiChannel, AiChannelOptions, SandboxExecutor, SandboxResult, ToolError};
use serde_json::json;
use tokio::sync::Notify;

use super::support::*;

type Trace = Arc<Mutex<Vec<&'static str>>>;
struct Ended(Trace);
impl Drop for Ended {
    fn drop(&mut self) {
        self.0.lock().unwrap().push("call ended");
    }
}
struct Held {
    name: &'static str,
    entered: Notify,
    release: Notify,
    trace: Trace,
    own: Mutex<Option<AiChannel<Model>>>,
}
impl Held {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            entered: Notify::new(),
            release: Notify::new(),
            trace: Arc::default(),
            own: Mutex::default(),
        }
    }
}
impl SandboxExecutor for Held {
    fn tool_definitions(&self) -> Result<Vec<Metadata>, ToolError> {
        Ok(vec![json!({"name":self.name}).as_object().unwrap().clone()])
    }
    fn execute<'a>(&'a self, _: &'a str, _: Option<&'a Metadata>) -> ResultFuture<'a> {
        Box::pin(async move {
            let _ended = Ended(Arc::clone(&self.trace));
            self.entered.notify_one();
            let own = self.own.lock().unwrap().clone();
            if let Some(channel) = own {
                channel.close().await;
                self.trace.lock().unwrap().push("own close returned");
            } else {
                self.release.notified().await;
            }
            Ok(SandboxResult {
                output: "finished".into(),
                ..SandboxResult::new(0)
            })
        })
    }
    fn close(&self) -> CloseFuture<'_> {
        self.trace.lock().unwrap().push("sandbox closed");
        Box::pin(ready(()))
    }
}

// Python: test_sandbox_contract.py::test_only_bash_escapes_the_default_and_a_named_bound_still_wins.
#[tokio::test(start_paused = true)]
async fn only_bash_escapes_the_default_and_a_named_bound_wins() {
    for streaming in [false, true] {
        for (name, named, expires) in [
            ("sandbox_bash", None, false),
            ("sandbox_read", None, true),
            ("sandbox_custom", None, true),
            ("sandbox_bash", Some(Some(Duration::from_millis(20))), true),
            ("sandbox_read", Some(None), false),
        ] {
            let sandbox = Arc::new(Held::new(name));
            let mut options = AiChannelOptions {
                sandbox: Some(Arc::<Held>::clone(&sandbox)),
                tool_timeout: Some(Duration::from_millis(20)),
                ..AiChannelOptions::default()
            };
            if let Some(bound) = named {
                options.tool_timeouts.insert(name.into(), bound);
            }
            let agent =
                AiChannel::new("ai".parse().unwrap(), model(streaming, name), options).unwrap();
            let task_agent = agent.clone();
            let mut task = tokio::spawn(async move { answer(&task_agent).await });
            bounded(sandbox.entered.notified()).await;
            if expires {
                let output = bounded(&mut task).await.unwrap();
                assert_eq!(*sandbox.trace.lock().unwrap(), ["call ended"]);
                let calls = ends(&output);
                assert_eq!(calls.len(), 1);
                assert_eq!(
                    calls.first().unwrap().outcome,
                    Some(ToolCallOutcome::Failed)
                );
                assert!(
                    calls
                        .first()
                        .unwrap()
                        .result
                        .to_string()
                        .contains("ToolTimeoutError")
                );
            } else {
                assert!(
                    tokio::time::timeout(Duration::from_millis(60), &mut task)
                        .await
                        .is_err()
                );
                assert!(sandbox.trace.lock().unwrap().is_empty());
                sandbox.release.notify_one();
                let output = bounded(&mut task).await.unwrap();
                assert_eq!(
                    ends(&output).first().unwrap().outcome,
                    Some(ToolCallOutcome::Served)
                );
                assert!(
                    ends(&output)
                        .first()
                        .unwrap()
                        .result
                        .to_string()
                        .contains("finished")
                );
            }
            agent.close().await;
        }
    }
}

#[tokio::test]
async fn close_cuts_active_commands_before_releasing_the_executor_and_spares_itself() {
    for streaming in [false, true] {
        for own in [false, true] {
            let sandbox = Arc::new(Held::new("sandbox_bash"));
            let model = model(streaming, "sandbox_bash");
            let agent = channel(model.clone(), Arc::<Held>::clone(&sandbox));
            if own {
                *sandbox.own.lock().unwrap() = Some(agent.clone());
            }
            let task_agent = agent.clone();
            let task = tokio::spawn(async move { answer(&task_agent).await });
            bounded(sandbox.entered.notified()).await;
            if !own {
                bounded(agent.close()).await;
            }
            let output = bounded(task).await.unwrap();
            assert_eq!(model.inner.requests().len(), 1);
            if own {
                assert_eq!(
                    *sandbox.trace.lock().unwrap(),
                    ["sandbox closed", "own close returned", "call ended"]
                );
                assert_eq!(
                    ends(&output).first().unwrap().outcome,
                    Some(ToolCallOutcome::Served)
                );
                sandbox.own.lock().unwrap().take();
            } else {
                assert_eq!(
                    *sandbox.trace.lock().unwrap(),
                    ["call ended", "sandbox closed"]
                );
            }
        }
    }
}

struct Closing {
    trace: Trace,
    fails: bool,
}
impl SandboxExecutor for Closing {
    fn tool_definitions(&self) -> Result<Vec<Metadata>, ToolError> {
        Ok(Vec::new())
    }
    fn execute<'a>(&'a self, _: &'a str, _: Option<&'a Metadata>) -> ResultFuture<'a> {
        Box::pin(pending())
    }
    fn close(&self) -> CloseFuture<'_> {
        Box::pin(async move {
            self.trace.lock().unwrap().push("closed");
            assert!(!self.fails, "sandbox cleanup failed");
        })
    }
}
#[tokio::test]
async fn an_empty_executor_is_closed_and_cleanup_errors_follow_channel_conventions() {
    let trace: Trace = Arc::default();
    channel(
        model(false, "unused"),
        Arc::new(Closing {
            trace: Arc::clone(&trace),
            fails: false,
        }),
    )
    .close()
    .await;
    assert_eq!(*trace.lock().unwrap(), ["closed"]);
    let agent = channel(
        model(false, "unused"),
        Arc::new(Closing {
            trace: Arc::clone(&trace),
            fails: true,
        }),
    );
    let failed = tokio::spawn(async move { agent.close().await })
        .await
        .unwrap_err();
    assert!(failed.is_panic());
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(channel(
        model(false, "unused"),
        Arc::new(Closing {
            trace: Arc::clone(&trace),
            fails: true,
        }),
    ))
    .unwrap();
    let failure = kit.close().await.unwrap_err();
    assert!(
        matches!(failure,roomkit::RoomKitError::ExceptionGroup{failures} if failures.len()==1 && failures.first().unwrap().1.contains("sandbox cleanup failed"))
    );
    assert_eq!(trace.lock().unwrap().len(), 3);
}
