//! A scripted streaming channel that keeps the implementation's default
//! `deliver_stream` (roomkit-specs `conformance/README.md`, Setup,
//! `default_deliver_stream`): every call goes to its scripted channel but
//! `deliver_stream`, which is `Channel`'s own default, reading the stream
//! itself and delivering the text it accumulated through `deliver`.
//! Python's `ScriptedChannel` calls `super().deliver_stream` for it.

use std::collections::BTreeMap;

use roomkit::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
    ChannelOutput, ChannelType, InboundMessage, Metadata, RoomContext, RoomEvent, RoomId,
};
use roomkit::{Channel, ChannelError};
use serde_json::Value;

use crate::scripted::ScriptedChannel;

/// The scripted channel it holds, its `deliver_stream` the trait's default.
pub(crate) struct DefaultReader(pub(crate) ScriptedChannel);

impl Channel for DefaultReader {
    fn channel_id(&self) -> ChannelId {
        self.0.channel_id()
    }

    fn channel_type(&self) -> ChannelType {
        self.0.channel_type()
    }

    fn category(&self) -> ChannelCategory {
        self.0.category()
    }

    fn direction(&self) -> ChannelDirection {
        self.0.direction()
    }

    fn capabilities(&self) -> ChannelCapabilities {
        self.0.capabilities()
    }

    fn provider_name(&self) -> Option<String> {
        self.0.provider_name()
    }

    fn recipient_key(&self) -> Option<String> {
        self.0.recipient_key()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.0.supports_streaming_delivery()
    }

    fn recent_events_window(&self) -> usize {
        self.0.recent_events_window()
    }

    fn sender_is_participant(&self) -> bool {
        self.0.sender_is_participant()
    }

    fn steers_room_turns(&self) -> bool {
        self.0.steers_room_turns()
    }

    fn serializes_room_turns(&self) -> bool {
        self.0.serializes_room_turns()
    }

    fn supports_streaming_delivery_for(&self, room_id: &RoomId) -> bool {
        self.0.supports_streaming_delivery_for(room_id)
    }

    fn normalize_address(&self, address: &str) -> String {
        self.0.normalize_address(address)
    }

    fn conversation_address(&self, message: &InboundMessage) -> String {
        self.0.conversation_address(message)
    }

    fn reply_metadata(&self, message: &InboundMessage) -> BTreeMap<String, String> {
        self.0.reply_metadata(message)
    }

    fn recipient_metadata(&self, address: &str) -> BTreeMap<String, String> {
        self.0.recipient_metadata(address)
    }

    fn recipient_address(&self, binding: &ChannelBinding) -> Option<String> {
        self.0.recipient_address(binding)
    }

    async fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> Result<RoomEvent, ChannelError> {
        self.0.handle_inbound(message, context).await
    }

    async fn on_event(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        self.0.on_event(event, binding, context).await
    }

    async fn channel_call(
        &self,
        room_id: RoomId,
        verb: String,
        arguments: Metadata,
    ) -> Result<Value, ChannelError> {
        self.0.channel_call(room_id, verb, arguments).await
    }

    fn room_state(&self, room_id: &RoomId) -> Result<Metadata, ChannelError> {
        self.0.room_state(room_id)
    }

    fn update_binding(
        &self,
        room_id: &RoomId,
        binding: &ChannelBinding,
    ) -> Result<(), ChannelError> {
        self.0.update_binding(room_id, binding)
    }

    fn restore_room_state(&self, room_id: &RoomId, state: Metadata) -> Result<(), ChannelError> {
        self.0.restore_room_state(room_id, state)
    }

    async fn on_room_attached(
        &self,
        room_id: RoomId,
        binding: ChannelBinding,
    ) -> Result<(), ChannelError> {
        self.0.on_room_attached(room_id, binding).await
    }

    async fn close(&self) {
        self.0.close().await;
    }

    async fn on_room_detached(&self, room_id: RoomId) -> Result<(), ChannelError> {
        self.0.on_room_detached(room_id).await
    }

    async fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        self.0.deliver(event, binding, context).await
    }

    async fn deliver_cut(
        &self,
        event: RoomEvent,
        row: Option<RoomEvent>,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<(), ChannelError> {
        self.0.deliver_cut(event, row, binding, context).await
    }
}
