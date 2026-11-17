//! Observe the actual removed binding without changing the core protocol.
//! A callback failure still forgets presence, after the room lock is released.

use std::sync::Arc;

use roomkit_core::model::{ChannelBinding, ChannelCategory, EventId, RoomId, Timestamp};
use roomkit_core::ops::{
    ChannelRequest, DetachChannel, EffectId, Entry, EntryId, Environment, FrameworkEvent,
    LifecycleError, Next, Notice, Operation, Outcome, ProtocolError, Request, Step,
};
use roomkit_core::rules::hook_registry::HookRegistry;

use crate::kit::Inner;
use crate::{ConversationStore, RoomKitError};

type Detached = Result<ChannelBinding, LifecycleError>;

pub(in crate::kit) async fn run<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    operation: DetachChannel,
) -> Result<Detached, RoomKitError> {
    let tracked = Detach {
        operation,
        binding: None,
        announced: false,
    };
    let (result, forgotten) = crate::driver::run(inner, "detach_channel", tracked).await?;
    if let Some(binding) = forgotten {
        let owner = Arc::clone(inner);
        super::call::run(inner, async move {
            owner
                .agent_presence
                .detached(&binding.room_id, &binding.channel_id)
                .await;
        })
        .await?;
    }
    Ok(result)
}

struct Detach {
    operation: DetachChannel,
    binding: Option<ChannelBinding>,
    announced: bool,
}

// Keep every non-poll operation entry point a direct delegation. Only the
// final output gains the binding that was actually removed and announced.
macro_rules! relay {
    ($($name:ident(&mut self $(, $arg:ident: $kind:ty)*) -> $answer:ty;)*) => {
        $(fn $name(&mut self, $($arg: $kind),*) -> $answer {
            self.operation.$name($($arg),*)
        })*
    };
}

impl Operation for Detach {
    type Output = (Detached, Option<ChannelBinding>);

    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output> {
        let step = self.operation.poll(environment);
        if let Next::Effects(effects) = &step.next {
            for effect in effects {
                if let Request::Channel(ChannelRequest::RoomDetached { binding }) = &effect.request
                {
                    self.binding = Some(binding.clone());
                }
            }
        }
        for notice in &step.notices {
            if let Notice::FrameworkEvent(FrameworkEvent::RoomChannelDetached {
                room_id,
                channel_id,
                ..
            }) = notice
            {
                self.announced |= self.binding.as_ref().is_some_and(|binding| {
                    &binding.room_id == room_id && &binding.channel_id == channel_id
                });
            }
        }
        let next = match step.next {
            Next::Effects(effects) => Next::Effects(effects),
            Next::Waiting => Next::Waiting,
            Next::Ready(result) => {
                let forgotten = self.binding.take().filter(|binding| {
                    self.announced && binding.category == ChannelCategory::Intelligence
                });
                Next::Ready((result, forgotten))
            }
        };
        Step {
            notices: step.notices,
            settled: step.settled,
            delivered: step.delivered,
            decided: step.decided,
            next,
        }
    }

    relay! {
        complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError>;
        expire(&mut self) -> Vec<EffectId>;
        call_deadline(&mut self, deadline: Timestamp) -> ();
        entry_deadline(&mut self, entry: EntryId, deadline: Timestamp) -> ();
        entry_precharged(&mut self, entry: EntryId) -> ();
        adopt(&mut self, entry: Entry, origin: EffectId) -> Result<EntryId, Box<Entry>>;
        adopt_room_hooks(&mut self, room_id: &RoomId, hooks: &HookRegistry) -> ();
        cancel_turns(&mut self) -> Vec<EffectId>;
        entry_follows_its_origin(&mut self, entry: EntryId) -> ();
        entry_caller_gone(&mut self, entry: EntryId) -> Vec<EffectId>;
        cancel_entry_turns(&mut self, entry: EntryId) -> Vec<EffectId>;
        lanes_sealed(&mut self, runs: &[EventId]) -> ();
    }

    fn tells_its_cut(&self) -> bool {
        self.operation.tells_its_cut()
    }
    fn locks_held_back(&self) -> Vec<(RoomId, usize)> {
        self.operation.locks_held_back()
    }
}
