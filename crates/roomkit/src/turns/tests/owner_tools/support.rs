//! A real turn calling the private owner registry, as the Python source tests do.

use std::any::Any;
use std::sync::{Arc, Mutex};

use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    Metadata, RoomContext, RoomEvent,
};
use serde_json::json;

use super::super::support::{channel, kit_with};
use crate::channel::BoxFuture;
use crate::turn::{OwnerCall, OwnerToolAnswer};
use crate::turns::served::OwnerToolEntry;
use crate::{
    Channel, ChannelError, OwnerToolContext, OwnerToolIdentity, RoomKit, ToolCallScope, TurnRecord,
    within_tool_call_scope,
};

pub(super) type Work = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

pub(super) struct Scope {
    context: OwnerToolContext,
    structured: Mutex<Option<Metadata>>,
}

impl Scope {
    pub(super) fn new(context: OwnerToolContext) -> Self {
        Self {
            context,
            structured: Mutex::new(None),
        }
    }
}

impl ToolCallScope for Scope {
    fn structured_content(&self) -> Option<Metadata> {
        self.structured.lock().unwrap().clone()
    }

    fn set_structured_content(&self, value: Metadata) {
        *self.structured.lock().unwrap() = Some(value);
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    fn owner_context(&self) -> OwnerToolContext {
        self.context.clone()
    }
}

struct Calling {
    work: Work,
    initial: Metadata,
}

impl Channel for Calling {
    fn channel_id(&self) -> ChannelId {
        channel("ai")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(crate::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }

    async fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let scope = Arc::new(Scope::new(OwnerToolContext {
            room_id: Some(event.room_id.clone()),
            room: Some(Arc::new(context.room)),
            actor_id: Some("alice".into()),
            requester: Some("Coordinator".into()),
            allowed_names: Some(["probe".to_owned()].into()),
            chain_depth: 3,
            call: Some(OwnerToolIdentity {
                room_id: Some(event.room_id),
                channel_id: Some(channel("ai")),
                tool_call_id: "c1".into(),
            }),
        }));
        let record = TurnRecord::default();
        record.adopt(self.initial.clone().into());
        let work = async { (self.work)().await };
        record
            .clone()
            .scope(within_tool_call_scope(scope, work))
            .await;
        Ok(ChannelOutput {
            response_metadata: record.current(),
            ..ChannelOutput::empty()
        })
    }
}

pub(super) async fn kit(work: Work, initial: Metadata) -> RoomKit {
    kit_with(Calling { work, initial }).await
}

pub(super) fn serve(
    kit: &RoomKit,
    handler: impl Fn(OwnerCall) -> BoxFuture<'static, OwnerToolAnswer> + Send + Sync + 'static,
) {
    kit.register_owner_tools(&channel("ai"), vec![OwnerToolEntry {
        definition: json!({"name": "probe", "description": "Probe the owner", "parameters": {"type": "object"}}),
        serve: Arc::new(move |_, call| handler(call)),
        waits: false,
        ends_turn: false,
        owner: None,
    }]).unwrap();
}

pub(super) async fn call(arguments: Metadata) -> OwnerToolAnswer {
    crate::current_turn_host()
        .unwrap()
        .call_owner_tool("probe", arguments, "c1", Some("Coordinator".into()))
        .await
}
