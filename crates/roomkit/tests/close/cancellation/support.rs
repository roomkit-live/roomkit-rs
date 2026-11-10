//! Gates at pre-commit boundaries, and a counted intelligence channel.

use std::future::{Future, ready};
use std::sync::{Arc, Mutex};

use roomkit::identity::{IdentityFuture, IdentityResolver};
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelOutput, ChannelType, IdentificationStatus,
    IdentityResult, InboundMessage, RoomContext, RoomEvent,
};
use roomkit::{Channel, ChannelError};
use roomkit_core::rules::transcoding::text_of;
use tokio::sync::Notify;

use crate::support::channel;

#[derive(Clone, Default)]
pub(super) struct Pause {
    pub(super) reached: Arc<Notify>,
    pub(super) release: Arc<Notify>,
}

impl Pause {
    async fn wait(&self) {
        self.reached.notify_one();
        self.release.notified().await;
    }
}

pub(super) struct Input(pub(super) Option<Pause>);

impl Channel for Input {
    fn channel_id(&self) -> roomkit::model::ChannelId {
        channel("sms")
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::SMS
    }
    async fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> Result<RoomEvent, ChannelError> {
        if let Some(pause) = &self.0 {
            pause.wait().await;
        }
        Ok(roomkit::default_inbound_event(self, &message, &context))
    }
    fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
}

pub(super) struct Identifying(pub(super) Pause);

impl IdentityResolver for Identifying {
    fn resolve<'a>(&'a self, _: &'a InboundMessage, _: &'a RoomContext) -> IdentityFuture<'a> {
        Box::pin(async move {
            self.0.wait().await;
            Ok(IdentityResult::new(IdentificationStatus::Unknown))
        })
    }
}

#[derive(Clone, Default)]
pub(super) struct Agent {
    pub(super) started: Arc<Mutex<Vec<String>>>,
    pub(super) finished: Arc<Mutex<Vec<String>>>,
    pub(super) held: Pause,
}

impl Channel for Agent {
    fn channel_id(&self) -> roomkit::model::ChannelId {
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
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
    fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
    async fn on_event(
        &self,
        event: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let body = text_of(&event.content);
        self.started.lock().unwrap().push(body.clone());
        if body == "task A" {
            self.held.wait().await;
        }
        self.finished.lock().unwrap().push(body);
        Ok(ChannelOutput::empty())
    }
}
