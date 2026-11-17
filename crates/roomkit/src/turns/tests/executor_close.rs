//! Python: `tests/test_executor_close_contract.py`; actual backend ancestry.

use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    Metadata, RoomContext, RoomEvent, RoomId,
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
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(crate::default_inbound_event(self, &message, &context)))
    }
    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
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

struct CallsOwner {
    expects_answer: bool,
    sibling: bool,
}
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
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(crate::default_inbound_event(self, &message, &context)))
    }
    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
    async fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let host = crate::current_turn_host().unwrap();
        let answer = if self.sibling {
            tokio::join!(
                host.call_owner_tool("close", Metadata::new(), "c1", None),
                host.call_owner_tool("wait", Metadata::new(), "c2", None),
            )
            .0
        } else {
            host.call_owner_tool("close", Metadata::new(), "c1", None)
                .await
        };
        if self.expects_answer {
            assert!(matches!(answer, crate::turn::OwnerToolAnswer::Served(_)));
        }
        Ok(ChannelOutput::empty())
    }
}

// Python: test_a_close_from_an_owner_served_tool_spares_its_turn
#[tokio::test]
async fn a_close_from_an_owner_served_tool_spares_its_turn() {
    let kit = kit_with(CallsOwner {
        expects_answer: true,
        sibling: false,
    })
    .await;
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
        ends_turn: false,
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

#[tokio::test(start_paused = true)]
async fn a_kit_close_from_an_owner_served_tool_finishes_its_call() {
    for sibling in [false, true] {
        for via_hook in [false, true] {
            closes_with_owner_sibling(sibling, via_hook).await;
        }
    }
}

struct Dropped(Arc<AtomicBool>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

async fn closes_with_owner_sibling(sibling: bool, via_hook: bool) {
    let kit = kit_with(CallsOwner {
        expects_answer: false,
        sibling,
    })
    .await;
    let hook_returned = closing_owner_hook(&kit, via_hook);
    let closed = Arc::new(tokio::sync::Notify::new());
    let told = Arc::clone(&closed);
    let started = Arc::new(tokio::sync::Notify::new());
    let begins = Arc::clone(&started);
    let entry = crate::turns::served::OwnerToolEntry {
        definition: serde_json::json!({"name": "close", "description": "Close kit", "parameters": {"type": "object"}}),
        serve: Arc::new(move |kit, _call| {
            let told = Arc::clone(&told);
            let begins = Arc::clone(&begins);
            Box::pin(async move {
                assert!(crate::current_turn_host().is_none());
                if sibling {
                    begins.notified().await;
                }
                if via_hook {
                    drop(
                        kit.send_event(
                            &room_id(),
                            &channel("ai"),
                            roomkit_core::model::TextContent::new("close owner"),
                            crate::SendEventOptions::default(),
                        )
                        .await,
                    );
                } else {
                    kit.close().await.unwrap();
                }
                told.notify_one();
                crate::turn::OwnerToolAnswer::Served(serde_json::json!("closed"))
            })
        }),
        waits: false,
        owner: None,
    };
    let dropped = Arc::new(AtomicBool::new(false));
    let tracks = Arc::clone(&dropped);
    let waiting = crate::turns::served::OwnerToolEntry {
        definition: serde_json::json!({"name": "wait", "description": "Wait", "parameters": {"type": "object"}}),
        serve: Arc::new(move |_, _| {
            let (started, tracks) = (Arc::clone(&started), Arc::clone(&tracks));
            Box::pin(async move {
                let _dropped = Dropped(tracks);
                started.notify_one();
                std::future::pending().await
            })
        }),
        waits: false,
        owner: None,
    };
    kit.inner
        .owner_tools
        .register_all(&channel("ai"), vec![entry, waiting])
        .unwrap();
    let asking = tokio::spawn({
        let kit = kit.clone();
        async move { says(&kit, "hello").await }
    });
    tokio::time::timeout(Duration::from_secs(30), closed.notified())
        .await
        .unwrap();
    drop(asking.await.unwrap());
    crate::testing::idle(&kit).await;
    assert_eq!(dropped.load(Ordering::SeqCst), sibling);
    assert_eq!(hook_returned.load(Ordering::SeqCst), via_hook);
}

fn closing_owner_hook(kit: &crate::RoomKit, enabled: bool) -> Arc<AtomicBool> {
    let returned = Arc::new(AtomicBool::new(false));
    if !enabled {
        return returned;
    }
    let (weak, notes) = (kit.downgrade(), Arc::clone(&returned));
    kit.hook(
        crate::HookTrigger::BeforeBroadcast,
        move |event: RoomEvent, _context| {
            let (weak, notes) = (weak.clone(), Arc::clone(&notes));
            async move {
                if roomkit_core::rules::transcoding::text_of(&event.content) == "close owner" {
                    weak.upgrade().unwrap().close().await.unwrap();
                    notes.store(true, Ordering::SeqCst);
                }
                Ok(roomkit_core::model::HookResult::allow(
                    roomkit_core::model::hook::AllowOptions::default(),
                ))
            }
        },
        crate::HookOptions {
            name: "close_from_owner".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    returned
}
