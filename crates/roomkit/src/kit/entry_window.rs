//! Preparatory reads share the call's pre-commit deadline.

use roomkit_core::model::ChannelId;
use roomkit_core::ops::{Entry, FrameworkEvent, InboundError, InboundResult};

use super::RoomKit;
use super::inject::{Entered, Following, refusal_of};
use crate::context::ExecutionContext;
use crate::deadline::CallDeadline;
use crate::delivery_keys::NamedGuard;
use crate::error::RoomKitError;
use crate::store::ConversationStore;

impl<S: ConversationStore> RoomKit<S> {
    /// Checks scope before the reentry budget is drawn, without renewing
    /// the call's deadline when the driver or its adopting run takes over.
    pub(super) async fn prepare_entry(
        &self,
        entry: &Entry,
        named: Option<&NamedGuard>,
        deadline: CallDeadline,
    ) -> Result<Option<Entered>, RoomKitError> {
        let reads = async {
            self.reached_from_its_work(entry).await?;
            if let Some(named) = named.filter(|named| named.named().has_key())
                && ExecutionContext::current().works_in_room(entry.room_id())
            {
                self.locate_in_room(named.named(), entry.room_id()).await;
            }
            Ok::<_, RoomKitError>(())
        };
        // Subscription's own lifetime is unchanged; the subsequent
        // scope reads spend the window already started by the call.
        self.inner.subscribed().await;
        let prepared = deadline.until(reads).await;
        match prepared {
            Some(result) => result.map(|()| None),
            None => self.entry_timed_out(entry).await.map(Some),
        }
    }

    async fn entry_timed_out(&self, entry: &Entry) -> Result<Entered, RoomKitError> {
        let channel_id = match entry {
            Entry::Message { message, .. } => message.channel_id.clone(),
            Entry::Direct { event, .. } => event.channel_id.clone(),
            Entry::Post { post, .. } => post.channel_id.clone().unwrap_or_else(ChannelId::host),
            Entry::Row { event, .. } => event.source.channel_id.clone(),
            Entry::Greeting { agent_id, .. } => agent_id.clone(),
        };
        let timeout = self
            .config()
            .process_timeout
            .map_or(0.0, |bound| bound.as_secs_f64());
        tracing::info!(target: "roomkit::inbound", room_id = %entry.room_id(), %channel_id, timeout,
            "the call passed its process_timeout during its preparatory reads");
        if let Some(handled) = self.inner.emit(FrameworkEvent::process_timeout(
            Some(entry.room_id().clone()),
            Some(channel_id),
            None,
            timeout,
        )) {
            handled.wait().await;
        }
        if matches!(entry, Entry::Direct { .. } | Entry::Post { .. }) {
            return Err(refusal_of(entry)(InboundError::Refused {
                reason: "process_timeout".to_owned(),
            }));
        }
        Ok(Entered {
            result: InboundResult {
                blocked: true,
                reason: Some("process_timeout".to_owned()),
                refused_before_the_lock: true,
                ..Default::default()
            },
            following: Following::Nothing,
        })
    }
}
