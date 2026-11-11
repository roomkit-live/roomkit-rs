//! Python: tests/test_executor_close_contract.py; actual backend ancestry.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    RoomContext, RoomEvent, RoomId,
};

use super::support::{channel, kit_with, room_id, says};
use crate::turn::TurnKit;
use crate::{Channel, ChannelError, ConversationStore, CreateRoomOptions, WeakRoomKit};

struct Nested {
    kit: Arc<OnceLock<WeakRoomKit>>,
    completed: Arc<Mutex<Vec<RoomId>>>,
}

impl Channel for Nested {
    fn channel_id(&self) -> ChannelId {
        channel("ai")
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }
    async fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> Result<RoomEvent, ChannelError> {
        Ok(crate::default_inbound_event(self, &message, &context))
    }
    async fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        Ok(ChannelOutput::empty())
    }
    async fn on_event(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let kit = self.kit.get().unwrap().upgrade().unwrap();
        if event.room_id == room_id() {
            let nested = RoomId::new("nested").unwrap();
            let context = RoomContext::new(kit.store().get_room(&nested).await.unwrap().unwrap());
            let binding = ChannelBinding {
                room_id: nested.clone(),
                ..binding
            };
            let event = RoomEvent {
                room_id: nested,
                ..event.clone()
            };
            let turn_kit: Arc<dyn TurnKit> = Arc::new(Arc::downgrade(&kit.inner));
            let answer = kit
                .inner
                .turns
                .run(&turn_kit, &binding, event, context, |_| ())
                .await;
            assert!(answer.is_ok());
        } else {
            kit.turn_executor().close().await;
        }
        self.completed.lock().unwrap().push(event.room_id);
        Ok(ChannelOutput::empty())
    }
}

// Python: test_nested_turn_client_preserves_both_ancestors_when_closing
// The direct backend call really runs under both turns, unlike another delivery.
#[tokio::test]
async fn nested_turn_client_preserves_both_ancestors_when_closing() {
    let held = Arc::new(OnceLock::new());
    let completed = Arc::new(Mutex::new(Vec::new()));
    let kit = kit_with(Nested {
        kit: Arc::clone(&held),
        completed: Arc::clone(&completed),
    })
    .await;
    held.set(kit.downgrade()).unwrap();
    let nested = RoomId::new("nested").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(nested.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), says(&kit, "hello"))
        .await
        .unwrap()
        .unwrap();
    assert!(result.error.is_none());
    assert_eq!(*completed.lock().unwrap(), [nested, room_id()]);
    kit.close().await.unwrap();
}

struct CallsOwner;
impl Channel for CallsOwner {
    fn channel_id(&self) -> ChannelId {
        channel("ai")
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }
    async fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> Result<RoomEvent, ChannelError> {
        Ok(crate::default_inbound_event(self, &message, &context))
    }
    async fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        Ok(ChannelOutput::empty())
    }
    async fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let host = crate::current_turn_host().unwrap();
        let answer = host
            .call_owner_tool("close", Default::default(), "c1")
            .await;
        assert!(matches!(answer, crate::turn::OwnerToolAnswer::Served(_)));
        Ok(ChannelOutput::empty())
    }
}

// Python: test_a_close_from_an_owner_served_tool_spares_its_turn
#[tokio::test]
async fn a_close_from_an_owner_served_tool_spares_its_turn() {
    let kit = kit_with(CallsOwner).await;
    let entry = crate::turns::served::OwnerToolEntry {
        definition: serde_json::json!({"name": "close", "description": "Close executor", "parameters": {"type": "object"}}),
        serve: Arc::new(|kit, _call| {
            Box::pin(async move {
                assert!(crate::current_turn_host().is_none());
                kit.turn_executor().close().await;
                crate::turn::OwnerToolAnswer::Served(serde_json::json!("closed"))
            })
        }),
        waits: false,
        owner: None,
    };
    kit.inner
        .owner_tools
        .register_all(&channel("ai"), vec![entry])
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), says(&kit, "hello"))
        .await
        .unwrap()
        .unwrap();
    assert!(result.error.is_none());
    assert!(kit.turn_executor().closed());
    kit.close().await.unwrap();
}
