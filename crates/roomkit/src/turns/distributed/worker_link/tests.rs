//! What the executor's end of a turn makes of what its owner's node sends it.
use std::collections::BTreeSet;
use std::future::{Future, pending, ready};

use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    RoomContext, RoomEvent, TextContent,
};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::timeout;

use super::super::shutdown_tests::{LIMIT, pair, request};
use super::super::stream::Message;
use crate::{
    Channel, ChannelError, RoomKitError, SendEventOptions, TurnExecutor, TurnExecutorOptions,
    current_turn_host,
};

/// An agent whose turn sends an event through its owner from a task of its
/// own, which notes what the send came to; the turn itself never ends.
struct Sending {
    came_to: mpsc::UnboundedSender<String>,
}

impl Channel for Sending {
    fn channel_id(&self) -> ChannelId {
        "ai".parse().unwrap()
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
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let host = current_turn_host().unwrap();
        let (room, agent, came_to) = (event.room_id, self.channel_id(), self.came_to.clone());
        // The turn's cut drops its work: a task of the turn's outlives it, and
        // reads what its verb came to.
        tokio::spawn(async move {
            let (note, options) = (TextContent::new("note"), SendEventOptions::default());
            let came = match host.send_event(&room, &agent, note, options).await {
                Ok(_) => "sent".to_owned(),
                Err(RoomKitError::TurnLink { reason, .. }) => reason,
                Err(RoomKitError::RemoteTurn { kind, .. }) => kind,
                Err(error) => format!("{error:?}"),
            };
            drop(came_to.send(came));
        });
        pending::<()>().await;
        Ok(ChannelOutput::empty())
    }
}

/// A control the executor cannot read is a link that failed: the turn is cut
/// as one whose owner is lost, as any link's error but a gap cuts it
/// (RFC §10.6.8), and a verb still waiting for its owner's answer fails so.
#[tokio::test]
async fn an_unreadable_control_cuts_the_turn_as_its_owner_lost() {
    let Some((host, owner)) = pair() else { return };
    let (came_to, mut verbs) = mpsc::unbounded_channel();
    let worker = TurnExecutor::new(
        move |_| {
            let came_to = came_to.clone();
            async move { Ok(Some(Sending { came_to })) }
        },
        TurnExecutorOptions {
            executor_id: Some("worker".into()),
            hosts: Some(BTreeSet::from(["ai".into()])),
            ..Default::default()
        },
    )
    .unwrap();
    host.serve(worker.clone()).await.unwrap();
    let mut link = owner.route_turn("turn", host.node_id(), false).unwrap();
    let taken = owner
        .ask(host.node_id(), "request", request("turn"), "worker", LIMIT)
        .await
        .unwrap();
    assert_eq!(taken, "taken");
    // The turn's verb reached its owner's node, which leaves it unanswered.
    loop {
        let frame = timeout(LIMIT, link.inbox.recv()).await.unwrap().unwrap();
        let frame: Value = serde_json::from_str(&frame.body).unwrap();
        if frame["message"]["type"] == "host_call" {
            break;
        }
    }
    let unreadable = Message::new(
        "control",
        owner.node_id(),
        "not a control".into(),
        "turn",
        "",
    );
    owner.post(host.node_id(), &unreadable).await.unwrap();
    let came_to = timeout(LIMIT, verbs.recv()).await.unwrap().unwrap();
    assert_eq!(came_to, "owner_lost");
    worker.close().await;
    host.close().await.unwrap();
    owner.close().await.unwrap();
}
