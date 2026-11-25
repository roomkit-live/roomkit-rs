//! Live channels belong exclusively to their owner's executor (§10.6.7).
use std::collections::BTreeSet;
use std::future::ready;
use std::sync::{Arc, Mutex};

use crate::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, InboundMessage, RoomContext, RoomEvent,
    RoomId,
};
use crate::turns::{ChannelControl, ServedExecutor};
use crate::{Channel, ChannelError, RoomKit, RoomKitError};

struct Notices(Arc<Mutex<Vec<&'static str>>>);

impl Channel for Notices {
    fn channel_id(&self) -> ChannelId {
        "agent".parse().unwrap()
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        ctx: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(crate::default_inbound_event(self, &message, &ctx)))
    }

    fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }

    fn channel_call(
        &self,
        _: RoomId,
        _: String,
        _: crate::model::Metadata,
    ) -> impl Future<Output = Result<serde_json::Value, ChannelError>> + Send {
        self.0.lock().unwrap().push("called");
        ready(Ok(serde_json::json!("owner")))
    }

    fn on_room_attached(
        &self,
        _: RoomId,
        _: ChannelBinding,
    ) -> impl Future<Output = Result<(), ChannelError>> + Send {
        self.0.lock().unwrap().push("attached");
        ready(Ok(()))
    }

    fn on_room_detached(&self, _: RoomId) -> impl Future<Output = Result<(), ChannelError>> + Send {
        self.0.lock().unwrap().push("detached");
        ready(Ok(()))
    }
}

fn kit() -> (RoomKit, Arc<Mutex<Vec<&'static str>>>) {
    let kit = RoomKit::new(crate::RoomKitOptions::default());
    let notices = Arc::new(Mutex::new(Vec::new()));
    kit.register_channel(Notices(Arc::clone(&notices))).unwrap();
    (kit, notices)
}

fn changes(agent: &ChannelId, room: &RoomId) -> [ChannelControl; 2] {
    let binding = Box::new(ChannelBinding::new(
        agent.clone(),
        room.clone(),
        ChannelType::AI,
    ));
    [
        ChannelControl::RoomAttached {
            channel_id: agent.clone(),
            room_id: room.clone(),
            organization_id: None,
            binding,
        },
        ChannelControl::RoomDetached {
            channel_id: agent.clone(),
            room_id: room.clone(),
            organization_id: None,
        },
    ]
}

#[tokio::test]
async fn the_attended_executor_alone_receives_its_live_channels_work() {
    let Some((node, remote)) = super::nodes() else {
        return;
    };
    let (owner, here) = kit();
    let (other, there) = kit();
    let agent = "agent".parse().unwrap();
    let room: RoomId = "live".parse().unwrap();

    remote.serve(super::worker("remote")).await.unwrap();
    assert_eq!(super::choose(&node, room.as_str()).await, "remote");
    node.serve(other.turn_executor()).await.unwrap();
    let own: Arc<dyn ServedExecutor> = Arc::new(owner.turn_executor());
    node.serve_erased(Arc::clone(&own)).await.unwrap();
    node.reserve_owner(&own);
    assert_eq!(super::choose(&node, room.as_str()).await, own.executor_id());
    let refused = BTreeSet::from([format!("{}:{}", node.node_id(), own.executor_id())]);
    assert!(matches!(
        node.place(&agent, room.as_str(), &[], None, &refused, true).await,
        Err(RoomKitError::TurnLink { reason, .. }) if reason == "no_executor"
    ));
    let changes = changes(&agent, &room);
    for control in &changes {
        node.control_remote(control).await.unwrap();
    }
    own.close().await;
    assert!(matches!(
        node.place(&agent, room.as_str(), &[], None, &BTreeSet::new(), true).await,
        Err(RoomKitError::TurnLink { reason, .. }) if reason == "no_executor"
    ));
    for control in &changes {
        node.control_remote(control).await.unwrap();
    }
    assert_eq!(*here.lock().unwrap(), ["attached", "detached"]);
    assert!(there.lock().unwrap().is_empty());
    node.close().await.unwrap();
    remote.close().await.unwrap();
}

#[tokio::test]
async fn a_looped_back_verb_stays_with_the_kit_after_its_executor_id_is_reused() {
    let Some((node, _)) = super::nodes_with_loop_back(true) else {
        return;
    };
    let (owner, notices) = kit();
    let own: Arc<dyn ServedExecutor> = Arc::new(owner.turn_executor());
    node.reserve_owner(&own);
    node.serve_erased(Arc::clone(&own)).await.unwrap();
    node.serve(super::collision::replacement(own.executor_id(), false))
        .await
        .unwrap();
    let call =
        crate::turns::ChannelCall::new("agent".parse().unwrap(), "room".parse().unwrap(), "who");
    let result = node
        .control_remote(&ChannelControl::ChannelCall(Box::new(call)))
        .await
        .unwrap()
        .unwrap();
    assert!(result.error.is_none());
    assert_eq!(result.result, serde_json::json!("owner"));
    assert_eq!(notices.lock().unwrap().as_slice(), ["called"]);
    node.close().await.unwrap();
    owner.close().await.unwrap();
}
