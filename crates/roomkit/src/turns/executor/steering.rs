//! Await a channel's steering while its turn's work continues to run.

use super::{Frame, FrameSender};
use crate::{channel::DynChannel, steering::SteeringDirective};
use std::{future::Future, pin::Pin, sync::Arc};

pub(super) struct Steering {
    channel: Arc<dyn DynChannel>,
    turn_id: String,
    sender: Arc<FrameSender>,
    applying: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl Steering {
    pub(super) fn new(
        channel: Arc<dyn DynChannel>,
        turn_id: String,
        sender: Arc<FrameSender>,
    ) -> Self {
        Self {
            channel,
            turn_id,
            sender,
            applying: None,
        }
    }

    pub(super) fn pending(&self) -> bool {
        self.applying.is_some()
    }

    /// Controls retain their order while steering waits, as Python's control
    /// reader does; work and heartbeats are polled independently. Dropping
    /// the executor drops this future too, including an unfinished write.
    pub(super) fn apply(&mut self, steer_id: String, directive: SteeringDirective) {
        let channel = Arc::clone(&self.channel);
        let turn_id = self.turn_id.clone();
        let sender = Arc::clone(&self.sender);
        self.applying = Some(Box::pin(async move {
            let loops = channel.steer_turn_async(&directive, &turn_id).await
                .unwrap_or_else(|error| {
                    tracing::error!(target: "roomkit::turns", turn = %turn_id, error = %error, "the channel's steering failed");
                    Some(0)
                })
                .map(|loops| u64::try_from(loops).unwrap_or(u64::MAX));
            drop(sender.send(&Frame::SteerResult { steer_id, loops }));
        }));
    }

    pub(super) async fn applied(&mut self) {
        if let Some(applying) = self.applying.as_mut() {
            applying.await;
        }
        self.applying = None;
    }
}
