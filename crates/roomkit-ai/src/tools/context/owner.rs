//! Capture readable context and adapt each owner's fresh call scope.

use std::any::Any;
use std::collections::BTreeSet;
use std::sync::Arc;

use roomkit::{OwnerToolContext, OwnerToolIdentity, OwnerToolScope, ToolCallScope};

use super::{Catalogue, Scope, ToolCallContext, ToolTurnContextOptions};
use crate::shutdown::ClosingContext;

impl ToolCallScope for Scope {
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    fn owner_context(&self) -> OwnerToolContext {
        OwnerToolContext {
            room_id: self.turn.room_id.clone(),
            room: self.turn.room.clone(),
            actor_id: self.turn.actor_id.clone(),
            requester: self.turn.requester.clone(),
            allowed_names: match &self.turn.tools {
                Some(tools) => Some(tools.iter().map(|tool| tool.name.clone()).collect()),
                None => self.catalogue.names(),
            },
            chain_depth: self.turn.chain_depth,
            call: self.turn.call.as_ref().map(|call| OwnerToolIdentity {
                room_id: call.room_id.clone(),
                channel_id: call.channel_id.clone(),
                tool_call_id: call.tool_call_id.clone(),
            }),
        }
    }

    fn structured_content(&self) -> Option<roomkit::model::Metadata> {
        self.turn
            .call
            .as_ref()?
            .structured_content
            .get()
            .map(|record| record.to_metadata())
    }

    fn set_structured_content(&self, value: roomkit::model::Metadata) {
        if let Some(call) = &self.turn.call {
            call.structured_content.replace(Some(value.into()));
        }
    }
}

pub(super) fn current() -> Option<Arc<Scope>> {
    let scope = roomkit::current_tool_call_scope()?.into_any();
    match scope.downcast::<Scope>() {
        Ok(scope) => Some(scope),
        Err(scope) => scope
            .downcast::<OwnerToolScope>()
            .ok()?
            .adapter(|context| Arc::new(adapt(context)))
            .into_any()
            .downcast::<Scope>()
            .ok(),
    }
}

fn adapt(context: &OwnerToolContext) -> Scope {
    let catalogue = Catalogue::default();
    if let Some(names) = &context.allowed_names {
        catalogue.resolve(names.clone(), &BTreeSet::new());
    }
    Scope {
        turn: ToolTurnContextOptions {
            room_id: context.room_id.clone(),
            room: context.room.clone(),
            actor_id: context.actor_id.clone(),
            requester: context.requester.clone(),
            chain_depth: context.chain_depth,
            call: context.call.as_ref().map(|call| ToolCallContext {
                room_id: call.room_id.clone(),
                channel_id: call.channel_id.clone(),
                tool_call_id: call.tool_call_id.clone(),
                ..ToolCallContext::default()
            }),
            tools: None,
        },
        room_metadata: context
            .room
            .as_ref()
            .map(|room| Arc::new(room.metadata.clone())),
        catalogue,
        closing: ClosingContext::default(),
        reports: None,
        reporting: None,
    }
}

#[cfg(test)]
mod tests {
    use roomkit::model::{ResponseMetadata, Room};

    use super::*;
    use crate::tools::{
        current_tool_actor_id, current_tool_allowed_names, current_tool_call,
        current_tool_chain_depth, current_tool_requester, current_tool_room, current_tool_room_id,
        turn_report_claim,
    };

    #[test]
    fn capturing_reads_the_current_catalogue_without_reallocating_the_scope() {
        let scope = Arc::new(Scope::new(ToolTurnContextOptions::default()));
        assert!(scope.owner_context().allowed_names.is_none());
        scope
            .catalogue
            .resolve(["one".to_owned()].into(), &BTreeSet::new());
        scope.catalogue.record("two");
        assert_eq!(
            scope.owner_context().allowed_names,
            Some(["one".to_owned(), "two".to_owned()].into())
        );
        let held: Arc<dyn ToolCallScope> = Arc::<Scope>::clone(&scope);
        let restored = held.into_any().downcast::<Scope>().expect("AI scope");
        assert!(Arc::ptr_eq(&scope, &restored));
    }

    #[tokio::test]
    async fn owner_accessors_share_one_fresh_call_and_restore_the_outer_scope() {
        let room_id = "room".parse().expect("room id");
        let room = Arc::new(Room::new(room_id));
        let context = OwnerToolContext {
            room_id: Some(room.id.clone()),
            room: Some(Arc::clone(&room)),
            actor_id: Some("alice".into()),
            requester: Some("Alice".into()),
            allowed_names: Some(["lookup".into()].into()),
            chain_depth: 3,
            call: Some(OwnerToolIdentity {
                room_id: Some(room.id.clone()),
                channel_id: Some("agent".parse().expect("channel id")),
                tool_call_id: "c1".into(),
            }),
        };
        let outer = Arc::new(OwnerToolScope::new(context.clone()));
        roomkit::within_tool_call_scope(outer, async {
            assert_eq!(current_tool_room_id(), Some(room.id.clone()));
            assert!(Arc::ptr_eq(&current_tool_room().expect("room"), &room));
            assert_eq!(current_tool_actor_id().as_deref(), Some("alice"));
            assert_eq!(current_tool_requester().as_deref(), Some("Alice"));
            assert_eq!(current_tool_allowed_names(), context.allowed_names);
            assert_eq!(current_tool_chain_depth(), 3);
            let first = current_tool_call().expect("call");
            first
                .structured_content
                .replace(Some(ResponseMetadata::new()));
            assert!(
                current_tool_call()
                    .expect("same call")
                    .structured_content
                    .get()
                    .is_some()
            );
            assert!(turn_report_claim("c1", "agent").is_none());
            assert!(roomkit::current_turn_host().is_none());
            roomkit::within_tool_call_scope(Arc::new(OwnerToolScope::new(context)), async {
                assert!(
                    current_tool_call()
                        .expect("fresh call")
                        .structured_content
                        .get()
                        .is_none()
                );
            })
            .await;
            assert!(
                current_tool_call()
                    .expect("outer call")
                    .structured_content
                    .get()
                    .is_some()
            );
        })
        .await;
        assert!(current_tool_call().is_none());
        assert!(current_tool_actor_id().is_none());
    }

    #[tokio::test]
    async fn structured_copy_survives_owner_scope_exit_and_reaches_public_accessors() {
        let context = OwnerToolContext {
            call: Some(OwnerToolIdentity::default()),
            ..OwnerToolContext::default()
        };
        let owner = Arc::new(OwnerToolScope::new(context.clone()));
        let held = roomkit::within_tool_call_scope(Arc::<OwnerToolScope>::clone(&owner), async {
            let copy = ResponseMetadata::new();
            copy.insert("rows", serde_json::json!({"count": 2}));
            current_tool_call()
                .expect("owner call")
                .structured_content
                .replace(Some(copy.clone()));
            copy
        })
        .await;
        let copied = owner.structured_content().expect("copy after scope exit");
        held.with_value("rows", |value| value["count"] = 9.into());
        assert_eq!(copied.get("rows"), Some(&serde_json::json!({"count": 2})));
        let executor = Arc::new(adapt(&context));
        executor.set_structured_content(copied);
        roomkit::within_tool_call_scope(executor, async {
            let copy = current_tool_call()
                .expect("executor call")
                .structured_content
                .get()
                .expect("restored copy");
            assert_eq!(copy.get("rows"), Some(serde_json::json!({"count": 2})));
        })
        .await;
    }
}
