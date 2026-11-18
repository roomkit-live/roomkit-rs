//! Turn ancestry travels with a turn's work and its owner's waiting calls,
//! independently of the broader task marks used to shut down the whole kit.

use std::future::Future;
use std::sync::Arc;

use crate::context::ExecutionContext;

tokio::task_local! {
    static TURNS: TurnScope;
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TurnScope(Arc<[Arc<str>]>);

impl TurnScope {
    pub(crate) fn current() -> Self {
        TURNS.try_with(Clone::clone).unwrap_or_default()
    }

    pub(crate) fn contains(&self, id: &str) -> bool {
        self.0.iter().any(|turn| &**turn == id)
    }

    /// The immediate turn whose owner call is running, not an ancestor.
    pub(crate) fn innermost(&self) -> Option<Arc<str>> {
        self.0.last().cloned()
    }

    fn with(&self, id: Arc<str>) -> Self {
        if self.contains(&id) {
            return self.clone();
        }
        Self(self.0.iter().cloned().chain([id]).collect())
    }

    pub(crate) fn scope<F: Future>(self, future: F) -> impl Future<Output = F::Output> {
        TURNS.scope(self, future)
    }
}

/// A snapshot taken only at the turn boundary. Driver/delivery contexts
/// deliberately do not carry turn ancestry into an independent delivery.
#[derive(Clone, Debug, Default)]
pub struct TurnContext {
    execution: ExecutionContext,
    turns: TurnScope,
}

impl TurnContext {
    /// Capture the current execution context and turn ancestry for a local backend.
    pub fn current() -> Self {
        Self {
            execution: ExecutionContext::current(),
            turns: TurnScope::current(),
        }
    }

    pub(crate) fn execution(&self) -> &ExecutionContext {
        &self.execution
    }

    pub(crate) fn in_turn(mut self, id: Arc<str>) -> Self {
        self.turns = self.turns.with(id);
        self
    }

    pub(crate) fn at_least(mut self, floor: u32) -> Self {
        self.execution = self.execution.at_least(floor);
        self
    }

    pub(crate) fn scope<F: Future>(self, future: F) -> impl Future<Output = F::Output> {
        self.execution.scope(self.turns.scope(future))
    }
}

#[cfg(test)]
mod tests {
    use super::TurnContext;
    use crate::{current_turn_id, keep_context};

    #[tokio::test]
    async fn a_nested_turn_restores_the_parent_and_a_spawn_can_keep_it() {
        TurnContext::current()
            .in_turn("outer".into())
            .scope(async {
                TurnContext::current()
                    .in_turn("inner".into())
                    .scope(async {
                        assert_eq!(current_turn_id().as_deref(), Some("inner"));
                        let fresh = tokio::spawn(async { current_turn_id() });
                        let retained = tokio::spawn(keep_context(async { current_turn_id() }));
                        assert_eq!(fresh.await.ok(), Some(None));
                        assert_eq!(retained.await.ok(), Some(Some("inner".into())));
                    })
                    .await;
                assert_eq!(current_turn_id().as_deref(), Some("outer"));
            })
            .await;
        assert!(current_turn_id().is_none());
    }
}
