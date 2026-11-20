//! Observe what the worker actually calls on each channel build (RFC §10.6.2).
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, Weak};

use roomkit::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelId, ChannelOutput, ChannelType,
    InboundMessage, Metadata, RoomContext, RoomEvent, RoomId, SteeringDirective,
};
use roomkit::{Channel, ChannelDeclaration, ChannelError};
use roomkit_acp::AcpChannel;
use roomkit_ai::{Agent, AiChannel};
use serde_json::{Value, json};

use crate::acp_agents::ScriptedTransport;
use crate::agents::{self, Paced, Scripted};
use crate::run::{Run, lock};
use crate::scripted::ScriptedChannel;

enum Built {
    Model(Box<AiChannel<Paced>>),
    Agent(Box<Agent<Paced>>),
    Script(Box<ScriptedChannel>),
    Acp(Box<AcpChannel<ScriptedTransport>>),
}

impl Built {
    fn from_spec(spec: &Value, run: &Arc<Run>) -> Result<Self, String> {
        if spec.get("acp").is_some() {
            return Self::acp(spec, run);
        }
        if spec.get("model").is_none() {
            return ScriptedChannel::new(spec, run)
                .map(Box::new)
                .map(Self::Script);
        }
        let (agent, models) = agents::scripted_agent_of(spec, run, true)?;
        let built = match agent {
            Scripted::Channel(channel) => Self::Model(channel),
            Scripted::Agent(agent) => Self::Agent(agent),
            Scripted::RoutedChannel(_) | Scripted::RoutedAgent(_) => {
                return Err("this executor scenario requires an unsupported scripted agent".into());
            }
        };
        lock(&run.models).extend(models);
        Ok(built)
    }

    /// The executor's own copy of a declared ACP channel, with an agent of
    /// its own, whose wire and pipe the run reads in place of the kit's copy
    /// (README, "Scripted ACP agents").
    fn acp(spec: &Value, run: &Arc<Run>) -> Result<Self, String> {
        let id = ChannelId::new(spec["id"].as_str().unwrap_or_default())
            .map_err(|error| format!("acp: {error}"))?;
        let (channel, wire, pipe) = crate::acp_agents::scripted_acp_channel(spec, id.clone(), run)?;
        lock(&run.acp_wires).insert(id.to_string(), wire);
        lock(&run.acp_pipes).insert(id.to_string(), pipe);
        Ok(Self::Acp(Box::new(channel)))
    }
}

pub(super) struct WorkerChannel {
    inner: Built,
    worker: String,
    build: usize,
    seen: Mutex<BTreeSet<RoomId>>,
    run: Weak<Run>,
}

// Both fixture channels implement the native Channel trait, whose async methods
// return different futures. Await those futures in their concrete match arm.
macro_rules! forward {
    ($this:expr, $method:ident($($arg:expr),*) $(.$wait:ident)?) => {
        match &$this.inner {
            Built::Model(channel) => Channel::$method(&**channel $(,$arg)*)$(.$wait)?,
            Built::Agent(channel) => Channel::$method(&**channel $(,$arg)*)$(.$wait)?,
            Built::Script(channel) => Channel::$method(&**channel $(,$arg)*)$(.$wait)?,
            Built::Acp(channel) => Channel::$method(&**channel $(,$arg)*)$(.$wait)?,
        }
    };
}

impl WorkerChannel {
    pub(super) fn build(
        spec: &Value,
        run: &Arc<Run>,
        worker: &super::Worker,
        channel_id: &str,
    ) -> Result<Self, String> {
        let channel = Self {
            inner: Built::from_spec(spec, run)?,
            worker: worker.id.clone(),
            build: worker.next_build(channel_id),
            seen: Mutex::default(),
            run: Arc::downgrade(run),
        };
        channel.note("built", None, None);
        Ok(channel)
    }

    /// Records a call the executor made on this build, the place of its
    /// record among the executor's, none when nothing is recorded.
    fn note(&self, call: &str, room: Option<&RoomId>, verb: Option<&str>) -> Option<usize> {
        if let Some(room) = room {
            let first = lock(&self.seen).insert(room.clone());
            // A detach on a build which never entered this room releases nothing.
            if first && call == "on_room_detached" {
                return None;
            }
        }
        let run = self.run.upgrade()?;
        if !run.recording.load(Ordering::SeqCst) {
            return None;
        }
        let mut record = json!({"call":call, "channel_id":self.channel_id(), "build":self.build});
        if let Some(room) = room {
            record["room_id"] = json!(room);
        }
        if let Some(verb) = verb {
            record["verb"] = json!(verb);
        }
        if call == "on_event" {
            record["tools"] = json!([]);
        }
        let mut observed = lock(&run.observed);
        let records = observed.executors.entry(self.worker.clone()).or_default();
        records.push(record);
        Some(records.len() - 1)
    }
}

impl Channel for WorkerChannel {
    fn channel_id(&self) -> ChannelId {
        forward!(self, channel_id())
    }
    fn channel_type(&self) -> ChannelType {
        forward!(self, channel_type())
    }
    fn category(&self) -> ChannelCategory {
        forward!(self, category())
    }
    fn set_turn_wiring(&self, wiring: roomkit::turns::TurnWiring) {
        forward!(self, set_turn_wiring(wiring));
    }
    fn turn_wiring(&self) -> Option<roomkit::turns::TurnWiring> {
        forward!(self, turn_wiring())
    }
    fn capabilities(&self) -> ChannelCapabilities {
        forward!(self, capabilities())
    }
    fn declaration(&self) -> ChannelDeclaration {
        forward!(self, declaration())
    }
    fn provider_name(&self) -> Option<String> {
        forward!(self, provider_name())
    }
    fn recent_events_window(&self) -> usize {
        forward!(self, recent_events_window())
    }
    fn serializes_room_turns(&self) -> bool {
        forward!(self, serializes_room_turns())
    }
    fn steers_room_turns(&self) -> bool {
        forward!(self, steers_room_turns())
    }
    fn active_turns(&self) -> usize {
        forward!(self, active_turns())
    }
    fn own_tool_names(&self) -> Vec<String> {
        forward!(self, own_tool_names())
    }
    fn human_input_handler(&self) -> Option<roomkit::human_input::HumanInputHandler> {
        forward!(self, human_input_handler())
    }
    fn human_input_registered(&self, registration: u64) {
        forward!(self, human_input_registered(registration));
    }
    fn external_tool_bridge(&self) -> Option<roomkit::external_tools::ExternalToolBridge> {
        forward!(self, external_tool_bridge())
    }
    fn steer_turn(&self, directive: &SteeringDirective, turn_id: &str) -> Option<usize> {
        forward!(self, steer_turn(directive, turn_id))
    }
    async fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> Result<RoomEvent, ChannelError> {
        forward!(self, handle_inbound(message, context).await)
    }
    async fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        forward!(self, deliver(event, binding, context).await)
    }
    async fn on_event(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        if let (Some(run), Some(host)) = (self.run.upgrade(), roomkit::current_turn_host()) {
            lock(&run.turn_workers).insert(host.turn_id().to_owned(), self.worker.clone());
        }
        let at = self.note("on_event", Some(&event.room_id), None);
        if let (Some(at), Some(run), Some(turn)) =
            (at, self.run.upgrade(), roomkit::current_turn_id())
        {
            run.keep_executor_turn(turn, self.worker.clone(), at);
        }
        forward!(self, on_event(event, binding, context).await)
    }
    async fn on_room_attached(
        &self,
        room: RoomId,
        binding: ChannelBinding,
    ) -> Result<(), ChannelError> {
        self.note("on_room_attached", Some(&room), None);
        forward!(self, on_room_attached(room, binding).await)
    }
    async fn on_room_detached(&self, room: RoomId) -> Result<(), ChannelError> {
        self.note("on_room_detached", Some(&room), None);
        forward!(self, on_room_detached(room).await)
    }
    async fn on_room_event(&self, room: RoomId, event: RoomEvent) -> Result<(), ChannelError> {
        forward!(self, on_room_event(room, event).await)
    }
    async fn channel_call(
        &self,
        room: RoomId,
        verb: String,
        arguments: Metadata,
    ) -> Result<Value, ChannelError> {
        self.note("channel_call", Some(&room), Some(&verb));
        forward!(self, channel_call(room, verb, arguments).await)
    }
    fn room_state(&self, room: &RoomId) -> Result<Metadata, ChannelError> {
        forward!(self, room_state(room))
    }
    fn restore_room_state(&self, room: &RoomId, state: Metadata) -> Result<(), ChannelError> {
        forward!(self, restore_room_state(room, state))
    }
    async fn close(&self) {
        self.note("close", None, None);
        forward!(self, close().await);
    }
}
