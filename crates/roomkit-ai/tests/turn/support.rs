//! A room with an SMS transport and an AI agent on a scripted model.
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, HookResult, InboundMessage, RoomContext,
    RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions,
    HookExecution, HookOptions, HookTrigger, InboundResult, PostOptions, ProcessInboundOptions,
    RoomKit, RoomKitOptions, SendEventOptions, ToolCallEvent,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, AiRole, AiTool, AiToolCall,
    ProviderError, ToolResult, Tools, tool_arguments,
};
use roomkit_core::ops::ChannelConfig;
use serde_json::json;
use tokio::sync::watch;

pub(crate) fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

pub(crate) fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// `lookup_order`, which takes an order's number.
pub(crate) fn lookup() -> AiTool {
    AiTool {
        parameters: json!({
            "type": "object",
            "properties": { "order": { "type": "string" } },
            "required": ["order"],
        }),
        ..AiTool::new("lookup_order", "Find an order by its number").unwrap()
    }
}

/// `lookup_order`, finding every order shipped.
pub(crate) fn served() -> Tools {
    Tools::new().tool(lookup(), |call, _context| async move {
        let order = call.arguments.get("order").cloned().unwrap_or_default();
        Ok(ToolResult::Json(
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
        Self::equipped(model, tools, |options| options).await
    }

    /// The room, its agent adjusted by `adjust`.
    pub(crate) async fn equipped(
        model: ScriptedProvider,
        tools: Tools,
        adjust: impl FnOnce(AiChannelOptions) -> AiChannelOptions,
    ) -> Self {
        let options = AiChannelOptions {
            tools: tools.into(),
            ..AiChannelOptions::default()
        };
        let agent = AiChannel::new(channel("assistant"), model, adjust(options)).unwrap();
        Self::with_agent(agent).await
    }

    /// The room, its agent `agent`, on any provider.
    pub(crate) async fn with_agent<P: AiProvider>(agent: AiChannel<P>) -> Self {
        let kit = RoomKit::new(RoomKitOptions::default());
        let sms = RecordingTransport::new(
            channel("sms"),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        );
        kit.register_channel(sms.clone()).unwrap();
        kit.register_channel(agent).unwrap();
        kit.create_room(CreateRoomOptions {
            room_id: Some(room_id()),
            ..Default::default()
        })
        .await
        .unwrap();
        for id in ["sms", "assistant"] {
            kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
                .await
                .unwrap();
        }
        Self { kit, sms }
    }

    pub(crate) async fn says(&self, text: &str) -> InboundResult {
        Self::says_on(&self.kit, text).await
    }

    /// A message from the room's transport, as [`says`](Self::says), on
    /// `kit`, which a task of its own may hold.
    pub(crate) async fn says_on(kit: &RoomKit, text: &str) -> InboundResult {
        let message = InboundMessage::new(channel("sms"), "+15550001", TextContent::new(text));
        kit.process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    }

    pub(crate) async fn timeline(&self) -> Vec<RoomEvent> {
        self.kit
            .store()
            .list_events(&room_id(), every_row())
            .await
            .unwrap()
    }

    /// What the observers of tool calls read on each call's
    /// `error_detail`, in the order the calls ended.
    pub(crate) fn observed_details(&self) -> Arc<Mutex<Vec<Option<String>>>> {
        let details = Arc::new(Mutex::new(Vec::new()));
        let noted = Arc::clone(&details);
        self.kit
            .hook(
                HookTrigger::OnToolCall,
                move |event: ToolCallEvent, _context| {
                    noted.lock().unwrap().push(event.error_detail.clone());
                    async {}
                },
                HookOptions {
                    execution: HookExecution::Async,
                    name: "audit".to_owned(),
                    ..Default::default()
                },
            )
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

impl Counting {
    /// The configuration the kit keeps of the channel.
    fn config(&self) -> ChannelConfig {
        let mut config = ChannelConfig::new(
            Channel::channel_id(&self.sms),
            Channel::channel_type(&self.sms),
            Channel::category(&self.sms),
            Channel::direction(&self.sms),
        );
        config.capabilities = Channel::capabilities(&self.sms);
        config.provider_name = Channel::provider_name(&self.sms);
        config.recipient_key = Channel::recipient_key(&self.sms);
        config
    }
}

impl Channel for Counting {
    fn channel_id(&self) -> ChannelId {
        self.config().channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.config().channel_type.clone()
    }

    fn category(&self) -> roomkit::model::ChannelCategory {
        self.config().category
    }

    fn direction(&self) -> roomkit::model::ChannelDirection {
        self.config().direction
    }

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
        self.config().capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        self.config().provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        self.config().recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.config().supports_streaming_delivery
    }

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = self.config();
        let addressing = roomkit::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: false,
        };
        (addressing, config.country_code.clone())
    }

    async fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let answer =
            roomkit_core::rules::transcoding::text_of(&event.content).starts_with("Answer ");
        let output = self.sms.deliver(event, binding, context).await;
        if answer {
            self.answers.send_modify(|answers| *answers += 1);
        }
        output
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
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
    let agent = AiChannel::new(
        channel("assistant"),
        ScriptedProvider::new(rounds),
        AiChannelOptions {
            tools: served().into(),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    let kit = RoomKit::new(RoomKitOptions {
        max_chain_depth: 3,
        ..Default::default()
    });
    let sms = RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(Counting { sms, answers }).unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "assistant"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let posted = Arc::new(Mutex::new(Vec::new()));
    let (posting, weak) = (Arc::clone(&posted), kit.downgrade());
    let calls = Arc::new(Mutex::new(0_usize));
    kit.hook(
        HookTrigger::OnToolCall,
        move |_event: ToolCallEvent, _context| {
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
                let post = SendEventOptions::default();
                if let Ok(event) = kit
                    .send_event(
                        &room_id(),
                        &channel("sms"),
                        TextContent::new("called"),
                        post,
                    )
                    .await
                {
                    posted
                        .lock()
                        .unwrap()
                        .push((event.index, event.chain_depth));
                }
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "audit".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let message = InboundMessage::new(channel("sms"), "+15550001", TextContent::new("Go"));
    kit.process_inbound(
        message,
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    roomkit::testing::idle(&kit).await;
    kit.close().await.unwrap();
    let mut depths = posted.lock().unwrap().clone();
    depths.sort_unstable();
    depths.into_iter().map(|(_, depth)| depth).collect()
}

/// A model that answers each turn with `rounds` rounds of a text and one
/// call, then its answer, for `MOST_TURNS` turns; past them it answers
/// nothing, so that a loop no budget bounds ends all the same, its turns
/// counted, instead of running the machine out of memory.
#[derive(Clone)]
pub(crate) struct Endless {
    rounds: usize,
    pub(crate) turns: Arc<AtomicUsize>,
}

/// The turns an `Endless` model takes at most: far over any budget a test
/// checks.
const MOST_TURNS: usize = 400;

impl Endless {
    pub(crate) fn new(rounds: usize) -> Self {
        Self {
            rounds,
            turns: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Endless {
    /// The answer to `context`: a round of a text and a call until the turn
    /// made its rounds, then an answer; silence past the most turns.
    fn answer(&self, context: &AiContext) -> AiResponse {
        // The rounds of calls the turn made so far: its trailing assistant
        // calls and their results.
        let done = context
            .messages
            .iter()
            .rev()
            .take_while(|message| {
                message.role == AiRole::Tool
                    || (message.role == AiRole::Assistant && !message.tool_calls().is_empty())
            })
            .filter(|message| message.role == AiRole::Assistant)
            .count();
        let ended = |content: String, finish_reason: &str| AiResponse {
            finish_reason: Some(finish_reason.to_owned()),
            ..AiResponse::new(content)
        };
        if done == 0 && self.turns.fetch_add(1, Ordering::SeqCst) >= MOST_TURNS {
            return ended(String::new(), "stop");
        }
        if done < self.rounds {
            let mut call = AiToolCall::new(format!("c{done}"), "lookup_order");
            call.arguments = tool_arguments(r#"{"order": "1"}"#);
            return AiResponse {
                tool_calls: vec![call],
                ..ended(format!("Round {done}."), "tool_calls")
            };
        }
        ended("Answer.".to_owned(), "stop")
    }
}

impl AiProvider for Endless {
    fn model_name(&self) -> &'static str {
        "endless"
    }

    fn generate<'a>(
        &'a self,
        context: &'a AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        std::future::ready(Ok(self.answer(context)))
    }
}

/// The turns two agents with a tool take, answering each other from one
/// SMS, each turn `rounds` rounds of a text and a call, then an answer, at
/// the kit's default limits.
pub(crate) async fn two_agents(rounds: usize) -> usize {
    let (alpha, beta) = (Endless::new(rounds), Endless::new(rounds));
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    for (id, model) in [("alpha", alpha.clone()), ("beta", beta.clone())] {
        let agent = AiChannel::new(
            channel(id),
            model,
            AiChannelOptions {
                tools: served().into(),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        kit.register_channel(agent).unwrap();
    }
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "alpha", "beta"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let message = InboundMessage::new(channel("sms"), "+15550001", TextContent::new("Go"));
    kit.process_inbound(
        message,
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    roomkit::testing::idle(&kit).await;
    kit.close().await.unwrap();
    alpha.turns.load(Ordering::SeqCst) + beta.turns.load(Ordering::SeqCst)
}

/// How a hook calls into the room on every row of the agent's turn.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Echo {
    /// A locked SYNC `BEFORE_BROADCAST` hook's `send_event` from the SMS
    /// channel: an entry the hook's operation adopts, which draws one pass
    /// of the cascade's reentry budget as it starts (RFC §8.3, §9.5.1).
    HookSend,
    /// An ASYNC `AFTER_BROADCAST` observer's `post` addressed to the
    /// agent: a call of a hook's body, which draws one of the cascade's
    /// hook calls as it starts, at least one deeper than the row (RFC §8.3
    /// "The hook calls of a cascade").
    ObserverPost,
}

/// The turns an agent takes when `echo` calls into the room on every row of
/// its turns, each call asking the agent again, at `max_chain_depth`; and
/// the calls stored refused past the cascade's budget.
pub(crate) async fn a_hook_calling_per_row(echo: Echo, max_chain_depth: u32) -> (usize, usize) {
    let model = Endless::new(2);
    let kit = RoomKit::new(RoomKitOptions {
        max_chain_depth,
        ..Default::default()
    });
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    let agent = AiChannel::new(
        channel("assistant"),
        model.clone(),
        AiChannelOptions {
            tools: served().into(),
            ..AiChannelOptions::default()
        },
    )
    .unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "assistant"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    register_echo(&kit, echo);
    let message = InboundMessage::new(channel("sms"), "+15550001", TextContent::new("Go"));
    kit.process_inbound(
        message,
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    roomkit::testing::idle(&kit).await;
    let capped = kit
        .store()
        .list_events(&room_id(), every_row())
        .await
        .unwrap()
        .iter()
        .filter(|event| event.blocked_by.as_deref() == Some("reentry_loop_cap"))
        .count();
    kit.close().await.unwrap();
    (model.turns.load(Ordering::SeqCst), capped)
}

/// Registers the hook `echo` names, calling into the room on every row of
/// the agent's turns.
fn register_echo(kit: &RoomKit, echo: Echo) {
    let calling = kit.downgrade();
    match echo {
        Echo::HookSend => {
            let spec = HookOptions {
                name: "echo".to_owned(),
                ..Default::default()
            };
            kit.hook(
                HookTrigger::BeforeBroadcast,
                move |event: RoomEvent, _context| {
                    let kit = calling
                        .upgrade()
                        .filter(|_| event.source.channel_id == channel("assistant"));
                    async move {
                        if let Some(kit) = kit {
                            let echo = SendEventOptions::default();
                            // Past the budget the call is refused, its event
                            // stored refused.
                            let _sent = kit
                                .send_event(
                                    &room_id(),
                                    &channel("sms"),
                                    TextContent::new("Echo"),
                                    echo,
                                )
                                .await;
                        }
                        Ok(HookResult::allow(AllowOptions::default()))
                    }
                },
                spec,
            )
            .unwrap();
        }
        Echo::ObserverPost => {
            kit.hook(
                HookTrigger::AfterBroadcast,
                move |event: RoomEvent, _context| {
                    let kit = calling
                        .upgrade()
                        .filter(|_| event.source.channel_id == channel("assistant"));
                    async move {
                        if let Some(kit) = kit {
                            let post = PostOptions {
                                addressed_to: vec![channel("assistant")],
                                ..Default::default()
                            };
                            // Past the hook calls the post is stored refused.
                            let echo = TextContent::new("Echo");
                            let _posted = kit.post(&room_id(), echo, post).await;
                        }
                    }
                },
                HookOptions {
                    execution: HookExecution::Async,
                    name: "echo".to_owned(),
                    ..Default::default()
                },
            )
            .unwrap();
        }
    }
}

/// Every row of a room's timeline, the refused ones included.
pub(crate) fn every_row() -> roomkit::store::ListEventsOptions {
    roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::store::ListEventsOptions::default()
    }
}
