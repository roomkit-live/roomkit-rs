//! A room with an SMS transport and an AI agent on a scripted model.

use std::sync::{Arc, Mutex};

use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, DirectEvent, EventContent,
    InboundMessage, RoomContext, RoomEvent, RoomId,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    Channel, ChannelDeclaration, ChannelError, ConversationStore, InboundResult, RoomKit,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiAgent, ToolDefinition, ToolOutput, Tools};
use serde_json::json;
use tokio::sync::watch;

pub(crate) fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

pub(crate) fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// `lookup_order`, which takes an order's number.
pub(crate) fn lookup() -> ToolDefinition {
    ToolDefinition::new(
        "lookup_order",
        "Find an order by its number",
        json!({
            "type": "object",
            "properties": { "order": { "type": "string" } },
            "required": ["order"],
        }),
    )
}

/// `lookup_order`, finding every order shipped.
pub(crate) fn served() -> Tools {
    Tools::new().tool(lookup(), |call, _context| async move {
        let order = call.arguments.get("order").cloned().unwrap_or_default();
        Ok(ToolOutput::Json(
            json!({ "order": order, "status": "shipped" }),
        ))
    })
}

/// A room where Alice writes by SMS to an agent.
pub(crate) struct Room {
    pub(crate) kit: RoomKit,
    pub(crate) sms: RecordingTransport,
}

impl Room {
    pub(crate) async fn new(model: ScriptedProvider, tools: Tools) -> Self {
        Self::equipped(model, tools, |agent| agent).await
    }

    /// The room, its agent adjusted by `adjust`.
    pub(crate) async fn equipped(
        model: ScriptedProvider,
        tools: Tools,
        adjust: impl FnOnce(AiAgent<ScriptedProvider>) -> AiAgent<ScriptedProvider>,
    ) -> Self {
        let agent = AiAgent::new(channel("assistant"), model)
            .with_tools(tools)
            .unwrap();
        let kit = RoomKit::new();
        let sms = RecordingTransport::new(channel("sms"), ChannelType::SMS);
        kit.register_channel(sms.clone()).unwrap();
        kit.register_channel(adjust(agent)).unwrap();
        kit.create_room(&room_id()).await.unwrap();
        for id in ["sms", "assistant"] {
            kit.attach(&room_id(), &channel(id)).await.unwrap();
        }
        Self { kit, sms }
    }

    pub(crate) async fn says(&self, text: &str) -> InboundResult {
        let message = InboundMessage::new(channel("sms"), "+15550001", EventContent::text(text));
        self.kit
            .process_inbound_in(&room_id(), message)
            .await
            .unwrap()
    }

    pub(crate) async fn timeline(&self) -> Vec<RoomEvent> {
        self.kit
            .store()
            .list_events(&room_id(), true)
            .await
            .unwrap()
    }

    /// What the observers of tool calls read on each call's
    /// `error_detail`, in the order the calls ended.
    pub(crate) fn observed_details(&self) -> Arc<Mutex<Vec<Option<String>>>> {
        let details = Arc::new(Mutex::new(Vec::new()));
        let noted = Arc::clone(&details);
        self.kit
            .observe_tool_calls("audit", move |event, _context| {
                noted.lock().unwrap().push(event.error_detail.clone());
                async {}
            })
            .unwrap();
        details
    }
}

/// An SMS transport that counts the agent's answers it delivered: each
/// is delivered once its turn is over and its answer stored.
struct Counting {
    sms: RecordingTransport,
    answers: watch::Sender<usize>,
}

impl Channel for Counting {
    fn declaration(&self) -> ChannelDeclaration {
        self.sms.declaration()
    }

    async fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let answer = roomkit::text_of(&event.content).starts_with("Answer ");
        let output = self.sms.deliver(event, binding, context).await;
        if answer {
            self.answers.send_modify(|answers| *answers += 1);
        }
        output
    }
}

/// The depths of what an observer of tool calls posted, in the room's
/// order, at `max_chain_depth` 3: on each call, the observer posts into the
/// room, the agent answers the post by calling again. With
/// `after_the_turn`, each post waits until the turn that made its call is
/// over and its answer delivered (RMK-978).
pub(crate) async fn observer_loop(after_the_turn: bool) -> Vec<u32> {
    const ROUNDS: usize = 20;
    let rounds = (0..ROUNDS).flat_map(|n| {
        [
            ScriptedRound::calling("", [("call", "lookup_order", r#"{"order": "1"}"#)]),
            ScriptedRound::answering(format!("Answer {n}.")),
        ]
    });
    let (answers, turns) = watch::channel(0);
    let agent = AiAgent::new(channel("assistant"), ScriptedProvider::new(rounds))
        .with_tools(served())
        .unwrap();
    let kit = RoomKit::new();
    kit.set_max_chain_depth(3);
    let sms = RecordingTransport::new(channel("sms"), ChannelType::SMS);
    kit.register_channel(Counting { sms, answers }).unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(&room_id()).await.unwrap();
    for id in ["sms", "assistant"] {
        kit.attach(&room_id(), &channel(id)).await.unwrap();
    }
    let posted = Arc::new(Mutex::new(Vec::new()));
    let (posting, weak) = (Arc::clone(&posted), kit.downgrade());
    let calls = Arc::new(Mutex::new(0_usize));
    kit.observe_tool_calls("audit", move |_event, _context| {
        let (posted, kit, mut turns) = (Arc::clone(&posting), weak.upgrade(), turns.clone());
        let call = {
            let mut calls = calls.lock().unwrap();
            *calls += 1;
            *calls
        };
        async move {
            if after_the_turn {
                // The turn that made the call is over, its answer
                // delivered, as a slow progress card would find it.
                let _over = turns.wait_for(|answered| *answered >= call).await;
            }
            let Some(kit) = kit else { return };
            if posted.lock().unwrap().len() >= ROUNDS {
                return;
            }
            let post = DirectEvent::new(channel("sms"), EventContent::text("called"));
            if let Ok(event) = kit.send_event(&room_id(), post).await {
                posted
                    .lock()
                    .unwrap()
                    .push((event.index, event.chain_depth));
            }
        }
    })
    .unwrap();
    let message = InboundMessage::new(channel("sms"), "+15550001", EventContent::text("Go"));
    kit.process_inbound_in(&room_id(), message).await.unwrap();
    kit.idle().await;
    kit.shutdown().await;
    let mut depths = posted.lock().unwrap().clone();
    depths.sort_unstable();
    depths.into_iter().map(|(_, depth)| depth).collect()
}
