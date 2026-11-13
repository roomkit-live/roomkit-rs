//! RFC §10.6.5, §10.1 step 18: a call a turn makes to its room's owner, a
//! host verb or one of the owner's tools, holds the reentrancy of the call
//! that submitted the turn from the turn's submission on, before that call
//! waits on the turn's output too: an event it sends into the turn's own
//! room returns at its decision, rather than wait for its delivery, which
//! waits in the room's lane behind the set that waits on the turn
//! (RMK-1693).

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, EventContent,
    EventSource, InboundMessage, Metadata, Room, RoomContext, RoomEvent, TextContent,
};
use serde_json::{Value, json};
use tokio::sync::Notify;

use super::support::{bind, channel, kit_with, room_id, says};
use crate::channel::BoxFuture;
use crate::turn::{OwnerCall, OwnerToolAnswer, TurnKit};
use crate::turns::Turns;
use crate::turns::served::OwnerToolEntry;
use crate::{
    Channel, ChannelError, ConversationStore, HookOptions, HookTrigger, RoomKit, SendEventOptions,
    current_turn_host,
};

/// The owner's tool through which the agent's turn sends into its room.
const TOOL: &str = "say_again";

/// How the agent's turn sends `again` into its room.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Through {
    /// Its host's `send_event`, a verb the owner serves.
    Verb,
    /// The owner's tool [`TOOL`], whose handler sends it.
    Tool,
}

/// The text of `event`, if it is a text.
fn text(event: &RoomEvent) -> Option<&str> {
    if let EventContent::Text(text) = &event.content {
        return Some(&text.body);
    }
    None
}

/// Sends `again` into the room as the agent.
async fn say_again(kit: &RoomKit) -> bool {
    let (room, agent) = (room_id(), channel("ai"));
    let again = TextContent::new("again");
    let options = SendEventOptions::default();
    let sent = kit.send_event(&room, &agent, again, options);
    sent.await.is_ok()
}

/// An agent whose turn on `go` sends `again` into its room, as `through`
/// says, noting whether the send returned; it answers nothing.
struct Sending {
    through: Through,
    sent: Arc<Mutex<Option<bool>>>,
}

impl Channel for Sending {
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
        std::future::ready(Ok(crate::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }

    async fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        if text(&event) != Some("go") {
            return Ok(ChannelOutput::default());
        }
        let host = current_turn_host().unwrap();
        let sent = match self.through {
            Through::Verb => {
                let (room, agent) = (room_id(), channel("ai"));
                let again = TextContent::new("again");
                let options = SendEventOptions::default();
                let sent = host.send_event(&room, &agent, again, options);
                sent.await.is_ok()
            }
            Through::Tool => {
                let called = host.call_owner_tool(TOOL, Metadata::new(), "call-1");
                matches!(called.await, OwnerToolAnswer::Served(_))
            }
        };
        *self.sent.lock().unwrap_or_else(PoisonError::into_inner) = Some(sent);
        Ok(ChannelOutput::default())
    }
}

/// What the relay submits the agent's turn with.
struct Submitting {
    turns: Arc<Turns>,
    kit: Arc<dyn TurnKit>,
    binding: ChannelBinding,
    room: Room,
}

/// A transport whose delivery of the room's first message submits the
/// agent's turn on `go` itself, and waits on the turn's output only once
/// the owner served the turn's send (`served`): the call that submitted the
/// turn is a delivery set's in the room, as an agent's own `on_event` is,
/// and waits on the turn after the owner served the call. It notes whether
/// the send was served, and whether the output came, each within a bound.
struct Relay {
    submitting: Arc<Mutex<Option<Submitting>>>,
    served: Arc<Notify>,
    seen: Arc<Mutex<Option<(bool, bool)>>>,
}

impl Relay {
    async fn submit_and_wait(&self, submitting: Submitting) {
        let Submitting {
            turns,
            kit,
            binding,
            room,
        } = submitting;
        let source = EventSource::new(channel("sms"), ChannelType::SMS);
        let event = RoomEvent::new(room_id(), source, TextContent::new("go"));
        let context = RoomContext::new(room);
        let (mut turn, ()) = turns
            .submit(&kit, &binding, event, context, |_| ())
            .unwrap();
        let bound = Duration::from_secs(5);
        let served = tokio::time::timeout(bound, self.served.notified()).await;
        let bound = Duration::from_secs(2);
        let output = tokio::time::timeout(bound, turn.output(turns.cutting())).await;
        let seen = (served.is_ok(), matches!(output, Ok(Ok(_))));
        *self.seen.lock().unwrap_or_else(PoisonError::into_inner) = Some(seen);
    }
}

impl Channel for Relay {
    fn channel_id(&self) -> ChannelId {
        channel("relay")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(crate::default_inbound_event(self, &message, &context)))
    }

    async fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let first = self
            .submitting
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(submitting) = first.filter(|_| text(&event) == Some("hi")) {
            self.submit_and_wait(submitting).await;
        }
        Ok(ChannelOutput::empty())
    }
}

/// Serves [`TOOL`] to the agent: its handler sends `again` into the room.
fn serve_tool(kit: &RoomKit) {
    let definition = json!({
        "name": TOOL,
        "description": "Says again.",
        "parameters": {"type": "object", "properties": {}},
    });
    let entry = OwnerToolEntry {
        definition,
        serve: Arc::new(|kit: RoomKit, _call: OwnerCall| -> BoxFuture<'static, _> {
            Box::pin(async move {
                if say_again(&kit).await {
                    OwnerToolAnswer::Served(Value::Null)
                } else {
                    OwnerToolAnswer::Unserved("not sent".to_owned())
                }
            })
        }),
        waits: false,
        owner: None,
    };
    kit.register_owner_tools(&channel("ai"), vec![entry])
        .unwrap();
}

/// Says on `served` when the room's first `again` is about to be
/// committed: the owner serves the turn's send.
fn tell_served(kit: &RoomKit, served: Arc<Notify>) {
    let telling = move |event: RoomEvent, _: RoomContext| {
        if text(&event) == Some("again") {
            served.notify_one();
        }
        async {}
    };
    kit.hook(
        HookTrigger::BeforeBroadcast,
        telling,
        HookOptions::default(),
    )
    .unwrap();
}

/// Alice writes `hi` by SMS; the relay's delivery of it submits the
/// agent's turn, which sends `again` into the room `through` the owner.
/// Whether the send was served, the turn's output came while its submitting
/// call waited, and the agent's send returned.
async fn sent_before_the_wait(through: Through) -> (bool, bool, Option<bool>) {
    let sent = Arc::new(Mutex::new(None));
    let agent = Sending {
        through,
        sent: Arc::clone(&sent),
    };
    let kit = kit_with(agent).await;
    let (served, seen) = (Arc::new(Notify::new()), Arc::new(Mutex::new(None)));
    let submitting = Arc::new(Mutex::new(None));
    let relay = Relay {
        submitting: Arc::clone(&submitting),
        served: Arc::clone(&served),
        seen: Arc::clone(&seen),
    };
    bind(&kit, relay).await;
    serve_tool(&kit);
    tell_served(&kit, served);
    let store = kit.store();
    let binding = store.get_binding(&room_id(), &channel("ai")).await;
    let room = store.get_room(&room_id()).await.unwrap().unwrap();
    *submitting.lock().unwrap() = Some(Submitting {
        turns: Arc::clone(&kit.inner.turns),
        kit: Arc::new(Arc::downgrade(&kit.inner)),
        binding: binding.unwrap().unwrap(),
        room,
    });
    let bound = Duration::from_secs(10);
    let said = tokio::time::timeout(bound, says(&kit, "hi")).await;
    assert!(said.is_ok_and(|said| said.is_ok()), "{through:?}");
    crate::testing::idle(&kit).await;
    kit.close().await.unwrap();
    let (served, answered) = seen.lock().unwrap().unwrap_or_default();
    let sent = *sent.lock().unwrap();
    (served, answered, sent)
}

/// A turn is submitted from a delivery set's call in its room, and no call
/// waits on its output until the owner served its send into that room: on
/// several workers, the turn can run and the owner serve the call before the
/// call that submitted the turn reaches its output. Through a host verb as
/// through the owner's tool, the send returns at its decision, and the turn
/// answers the call that waits on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_event_a_turn_sends_into_its_room_before_it_is_waited_on_returns() {
    let mut seen = Vec::new();
    for through in [Through::Verb, Through::Tool] {
        seen.push((through, sent_before_the_wait(through).await));
    }
    let returned = (true, true, Some(true));
    assert_eq!(seen, [(Through::Verb, returned), (Through::Tool, returned)]);
}
