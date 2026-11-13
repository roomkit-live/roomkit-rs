//! Host-owned context, resolved anew for an eligible ACP turn.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use roomkit::model::{RoomContext, RoomEvent};

use crate::AcpError;

type BlocksFuture = Pin<Box<dyn Future<Output = Result<Vec<String>, AcpError>> + Send>>;

/// Python's `ACPContextContributor`: background blocks before the room's
/// catch-up and the current request. A single block is a one-element list.
/// An error omits the blocks, while cancellation drops the pending future.
#[derive(Clone)]
pub struct AcpContextContributor(Arc<dyn Fn(RoomContext, RoomEvent) -> BlocksFuture + Send + Sync>);

impl AcpContextContributor {
    /// Wrap an async callback receiving this turn's actual room tail and event.
    ///
    /// ```
    /// use roomkit_acp::AcpContextContributor;
    /// let context = AcpContextContributor::new(|room, event| async move {
    ///     let _ = (room, event);
    ///     Ok(vec!["Policy: be brief".to_owned()])
    /// });
    /// ```
    pub fn new<F, Fut>(callback: F) -> Self
    where
        F: Fn(RoomContext, RoomEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<String>, AcpError>> + Send + 'static,
    {
        Self(Arc::new(move |context, event| {
            Box::pin(callback(context, event))
        }))
    }

    pub(crate) async fn blocks(&self, context: &RoomContext, event: &RoomEvent) -> Vec<String> {
        match (self.0)(context.clone(), event.clone()).await {
            Ok(blocks) => blocks
                .into_iter()
                .map(|block| {
                    block
                        .trim_matches(|c: char| {
                            c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
                        })
                        .to_owned()
                })
                .filter(|block| !block.is_empty())
                .collect(),
            Err(error) => {
                tracing::error!(%error, "ACP context contributor failed; prompting without it");
                Vec::new()
            }
        }
    }
}

impl fmt::Debug for AcpContextContributor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AcpContextContributor")
    }
}
