//! The live depth of an explicitly scoped tool turn (RFC §8.3, §21.4).

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use super::{BodyRunning, ExecutionContext, floors::Floor};

/// Runs `work` with the tool turn's live depth. Its readable context is
/// carried separately: a child keeps that snapshot after this scope ends,
/// but no longer borrows its depth. For `roomkit_ai::tool_turn_context`.
#[doc(hidden)]
pub async fn within_tool_turn_depth<F: Future>(depth: u32, work: F) -> F::Output {
    let live = Arc::new(AtomicBool::new(true));
    let mut context = ExecutionContext::current();
    // One slot, not a stack: an expired inner scope still masks its outer
    // tool scope in a retained child. Runtime frames remain independent.
    context.tool_depth = Some(Floor::while_running(depth, Arc::clone(&live)));
    let _running = BodyRunning(live);
    context.scope(work).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tool_scopes_leave_independent_runtime_floors_in_force() {
        ExecutionContext::default()
            .at_least(4)
            .scope(async {
                within_tool_turn_depth(1, async {
                    assert_eq!(ExecutionContext::current().turn_depth(), Some(4));
                    within_tool_turn_depth(6, async {
                        assert_eq!(ExecutionContext::current().turn_depth(), Some(6));
                    })
                    .await;
                    assert_eq!(ExecutionContext::current().turn_depth(), Some(4));
                })
                .await;
                assert_eq!(ExecutionContext::current().turn_depth(), Some(4));
            })
            .await;
        assert_eq!(ExecutionContext::current().turn_depth(), None);
    }

    // Python: tests/test_tool_turn_context_observers.py::test_an_ended_observer_stops_lending_a_live_tool_scope_to_its_child;
    // the observer borrows inherited depth for its own body only.
    #[tokio::test]
    async fn observer_children_stop_borrowing_depth_even_when_the_tool_scope_lives() {
        within_tool_turn_depth(3, async {
            let (resume, ready) = tokio::sync::oneshot::channel();
            let child = ExecutionContext::current()
                .observing(1, async {
                    assert_eq!(ExecutionContext::current().turn_depth(), Some(3));
                    tokio::spawn(crate::keep_context(async {
                        ready.await.unwrap();
                        ExecutionContext::current().turn_depth()
                    }))
                })
                .await;
            resume.send(()).unwrap();
            assert_eq!(child.await.unwrap(), None);
            assert_eq!(ExecutionContext::current().turn_depth(), Some(3));
        })
        .await;
    }

    // Python: tests/test_tool_turn_context_observers.py::test_a_live_observer_stops_borrowing_an_ended_tool_scope
    #[tokio::test]
    async fn a_live_observer_stops_borrowing_an_ended_tool_scope() {
        for floor in [0, 2] {
            let (entered, started) = tokio::sync::oneshot::channel();
            let (finish, resume) = tokio::sync::oneshot::channel();
            let observer = within_tool_turn_depth(3, async {
                let observer = tokio::spawn(crate::keep_context(async move {
                    ExecutionContext::current()
                        .observing(floor, async move {
                            assert_eq!(ExecutionContext::current().turn_depth(), Some(3));
                            entered.send(()).unwrap();
                            resume.await.unwrap();
                            ExecutionContext::current().turn_depth()
                        })
                        .await
                }));
                started.await.unwrap();
                observer
            })
            .await;
            finish.send(()).unwrap();
            assert_eq!(observer.await.unwrap(), Some(floor));
        }
    }
}
