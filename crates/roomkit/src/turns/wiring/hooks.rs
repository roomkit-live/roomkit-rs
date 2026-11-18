//! A channel can fire its executor's hooks before joining a registry.
use super::TurnWiring;
use crate::HookTrigger;
use crate::channel::BoxFuture;
use crate::model::{ChannelId, ChannelType, Metadata, RoomContext, RoomId};
use crate::turn::{TurnHost, TurnKit};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::HookSpec;
use std::sync::Arc;

/// A channel's callbacks on its executor, usable inside or outside a turn.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ChannelTurnHooks(Binding);

#[derive(Clone, Debug)]
enum Binding {
    Turn(TurnHost),
    Wired(Arc<Wired>),
}
#[derive(Debug)]
struct Wired {
    wiring: TurnWiring,
    prompt: Option<super::DirectPrompt>,
    room: RoomId,
    channel: ChannelId,
    kind: ChannelType,
    publisher: Arc<crate::turn::publish::Publisher>,
}

impl From<TurnHost> for ChannelTurnHooks {
    fn from(host: TurnHost) -> Self {
        Self(Binding::Turn(host))
    }
}

impl TurnWiring {
    /// The callbacks for a channel's text work in this room. They retain the
    /// current turn when one exists and create no turn when called independently.
    #[doc(hidden)]
    pub fn channel_hooks(
        &self,
        room: &RoomId,
        channel: &ChannelId,
        kind: &ChannelType,
    ) -> ChannelTurnHooks {
        if let Some(host) = crate::turn::turn_in_scope()
            .filter(|host| host.room_id() == room && host.channel() == channel)
        {
            return host.into();
        }
        ChannelTurnHooks(Binding::Wired(Arc::new(Wired {
            wiring: self.clone(),
            prompt: super::DirectPrompt::current(),
            room: room.clone(),
            channel: channel.clone(),
            kind: kind.clone(),
            publisher: Arc::default(),
        })))
    }
}

impl ChannelTurnHooks {
    pub(crate) fn wired(&self) -> Option<(&TurnWiring, &RoomId, &ChannelId)> {
        match &self.0 {
            Binding::Wired(bound) => Some((&bound.wiring, &bound.room, &bound.channel)),
            Binding::Turn(_) => None,
        }
    }

    pub(crate) fn host(&self) -> Option<&TurnHost> {
        match &self.0 {
            Binding::Turn(host) => Some(host),
            Binding::Wired(_) => None,
        }
    }

    pub(crate) fn parts(&self) -> (&Arc<dyn TurnKit>, &RoomId, &ChannelId, &ChannelType) {
        match &self.0 {
            Binding::Turn(host) => host.callback_parts(),
            Binding::Wired(bound) => (&bound.wiring.kit, &bound.room, &bound.channel, &bound.kind),
        }
    }

    pub(crate) async fn context(&self, trigger: HookTrigger) -> Option<RoomContext> {
        match &self.0 {
            Binding::Turn(host) => Some(host.initial_context().clone()),
            Binding::Wired(bound) => bound.wiring.hook_context(&bound.room, trigger, None).await,
        }
    }

    pub(crate) fn spawn(&self, task: BoxFuture<'static, ()>) -> bool {
        if let Binding::Wired(bound) = &self.0
            && let Some(prompt) = bound.prompt.clone()
        {
            return bound.wiring.kit.spawn_observer(Box::pin(prompt.run(task)));
        }
        self.parts().0.spawn_observer(task)
    }

    pub(crate) fn timeout<P>(&self, spec: &HookSpec, ran: &HookOutcome<P>) {
        if let Some(data) = crate::turn::timeout_data(spec, ran) {
            self.emit_later("hook_timeout", data);
        }
    }

    /// Queue a room notification in order through this channel's outlet.
    pub fn publish_ephemeral(&self, event: crate::realtime::EphemeralEvent) {
        match &self.0 {
            Binding::Turn(host) => host.publish_from_channel(event),
            Binding::Wired(bound) => {
                if let Some(prompt) = &bound.prompt {
                    bound
                        .publisher
                        .enqueue_outside(&bound.wiring, prompt, event);
                }
            }
        }
    }

    /// Publish a prompt's awaited protocol notification with the same audience
    /// as its queued tool and thinking notifications.
    pub async fn publish_now(&self, event: crate::realtime::EphemeralEvent) {
        match &self.0 {
            Binding::Turn(host) => host.publish_from_channel(event),
            Binding::Wired(bound) => {
                if let Some(prompt) = &bound.prompt {
                    prompt.publish(&bound.wiring, event).await;
                }
            }
        }
    }

    /// Emit a framework fact after the hooks' synchronous decision.
    pub async fn emit(&self, kind: &str, data: Metadata) {
        match &self.0 {
            Binding::Turn(host) => host.emit_turn_event(kind, data),
            Binding::Wired(bound) => {
                bound
                    .wiring
                    .notify(kind, &bound.room, &bound.channel, data)
                    .await;
            }
        }
    }

    /// Queue a fact from a synchronous observer completion.
    pub fn emit_later(&self, kind: &'static str, data: Metadata) {
        if let Some(host) = self.host() {
            host.emit_turn_event(kind, data);
            return;
        }
        let bound = self.clone();
        self.spawn(Box::pin(async move {
            bound.emit(kind, data).await;
        }));
    }
}
