//! Each turn on a local or a hosted model, picked by a router (RFC §6.4).
//!
//! An AI channel built with `AiChannel::routed` asks its router which
//! provider serves each turn, at the turn's start: the turn's context is
//! built for that model, and every tool round of the turn stays on it. Once
//! a generation's retries are spent before it answered anything, the router
//! is asked again and may hand the generation to another model.
//!
//! The router here, `LocalOrHosted`, decides in plain code, with no model:
//!
//! - once the local model's retries are spent, the hosted one takes the
//!   generation;
//! - when the machine is loaded (the one-minute load average per core past
//!   `MAX_LOAD`, 4 by default), the hosted model, so the local one is not
//!   queued behind work;
//! - otherwise from how much reasoning the message needs, read by a few
//!   rules on its length and its words: a simple one goes to the local
//!   model, a complex one to the hosted one.
//!
//! Both chat models are mock providers, so the example runs offline. In an
//! application, `local` would be, say, a llama.cpp or vLLM server through an
//! OpenAI-compatible provider, and `hosted` a provider such as Kimi or GLM.
//! The local model refuses its second request as overloaded: the router
//! hands that generation to the hosted one.
//!
//! ```sh
//! cargo run --example ai_router_local_or_hosted
//! MAX_LOAD=0 cargo run --example ai_router_local_or_hosted   # every turn hosted
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use roomkit::model::{ChannelId, InboundMessage, RetryPolicy, RoomId, TextContent};
use roomkit::{AttachChannelOptions, Channel, CreateRoomOptions, ProcessInboundOptions};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, AiRouter, AiRouterFuture,
    DynAiProvider, MockAiProvider, MockAiProviderOptions, ProviderError, ProviderErrorOptions,
    RouteReason, RouteRequest,
};

use shared::text_of;
use shared::{Checks, SmsNumber};

/// The messages the customer sends, one turn each.
const MESSAGES: [&str; 3] = [
    "Hi! What time do you open on Saturday?",
    "Compare the two plans for me and explain which one fits a team of five.",
    "Thanks, and where do I park?",
];

/// Words that ask for reasoning, in the messages this example sends.
const REASONING: [&str; 6] = ["why", "compare", "plan", "design", "explain", "debug"];

/// How much reasoning `text` needs, from rules: long, code, or words that ask
/// for reasoning is complex. No model: a script that decides, often enough
/// to route a turn.
fn is_complex(text: &str) -> bool {
    let words: Vec<String> = text
        .split(|c: char| !c.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(str::to_lowercase)
        .collect();
    let step_by_step = words
        .windows(3)
        .any(|three| three == ["step", "by", "step"]);
    text.chars().count() > 280
        || text.contains("```")
        || step_by_step
        || words.iter().any(|word| REASONING.contains(&word.as_str()))
}

/// The one-minute load average per core, 0 where the system does not say
/// (`/proc/loadavg` is Linux's).
fn load_per_core() -> f64 {
    let load = std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|loadavg| loadavg.split_whitespace().next()?.parse::<f64>().ok())
        .unwrap_or(0.0);
    let cores = std::thread::available_parallelism().map_or(1, usize::from);
    load / f64::from(u32::try_from(cores).unwrap_or(u32::MAX))
}

/// Each turn on `local` or `hosted`, from the machine's load and how much
/// reasoning the message needs; `hosted` once `local` spent its retries.
/// It keeps what it picked, which the checks read.
struct LocalOrHosted {
    local: Arc<dyn DynAiProvider>,
    hosted: Arc<dyn DynAiProvider>,
    max_load: f64,
    picked: Arc<Mutex<Vec<(RouteReason, String)>>>,
}

impl AiRouter for LocalOrHosted {
    fn route(&self, request: RouteRequest) -> AiRouterFuture<'_> {
        let picked = if request.reason == RouteReason::Retry {
            let was_local = request.previous.as_ref().is_some_and(|previous| {
                std::ptr::addr_eq(Arc::as_ptr(previous), Arc::as_ptr(&self.local))
            });
            was_local.then(|| Arc::clone(&self.hosted))
        } else {
            // A turn's start, as Python's `else` reads any reason but a retry.
            let load = load_per_core();
            let picked = if load > self.max_load {
                println!(
                    "  router: load {load:.2} per core > {:.2}, hosted",
                    self.max_load
                );
                &self.hosted
            } else if is_complex(&request.text) {
                println!("  router: load {load:.2} per core, complex");
                &self.hosted
            } else {
                println!("  router: load {load:.2} per core, simple");
                &self.local
            };
            Some(Arc::clone(picked))
        };
        let name = picked
            .as_ref()
            .map_or("none", |p| p.dyn_model_name())
            .to_owned();
        self.picked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((request.reason, name));
        Box::pin(std::future::ready(picked))
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {
            self.local.dyn_close().await;
            self.hosted.dyn_close().await;
        })
    }
}

/// A scripted chat model named `name`, standing in for a real one, which
/// refuses its `busy_at`-th request as overloaded, if any.
struct Model {
    name: &'static str,
    mock: MockAiProvider,
    busy_at: Option<usize>,
    asked: AtomicUsize,
}

impl Model {
    fn new(name: &'static str, responses: &[&str], busy_at: Option<usize>) -> Self {
        let mock = MockAiProvider::new(MockAiProviderOptions {
            responses: responses.iter().map(|&text| text.to_owned()).collect(),
            ..MockAiProviderOptions::default()
        });
        Self {
            name,
            mock,
            busy_at,
            asked: AtomicUsize::new(0),
        }
    }
}

impl AiProvider for Model {
    fn model_name(&self) -> &str {
        self.name
    }

    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        let asked = self.asked.fetch_add(1, Ordering::SeqCst) + 1;
        if self.busy_at == Some(asked) {
            let options = ProviderErrorOptions {
                retryable: true,
                status_code: Some(503),
                ..ProviderErrorOptions::default()
            };
            return Err(ProviderError::new("local server overloaded", options));
        }
        self.mock.generate(context).await
    }
}

/// What the router picked, by reason, in order.
type Picked = Arc<Mutex<Vec<(RouteReason, String)>>>;

/// The router over the two models, sending the turns to the hosted one past
/// `max_load` per core, keeping what it picks in `picked`. The local model is
/// busy on its second request: the router hands that generation to the
/// hosted one.
fn local_or_hosted(max_load: f64, picked: &Picked) -> LocalOrHosted {
    let local = Model::new(
        "qwen-local",
        &[
            "We open at 9 on Saturday.",
            "Parking is behind the building.",
        ],
        Some(2),
    );
    let hosted = Model::new(
        "kimi-hosted",
        &["Plan B fits five people: ...", "Parking is behind."],
        None,
    );
    LocalOrHosted {
        local: Arc::new(local),
        hosted: Arc::new(hosted),
        max_load,
        picked: Arc::clone(picked),
    }
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let max_load = std::env::var("MAX_LOAD")
        .ok()
        .and_then(|max| max.parse().ok())
        .unwrap_or(4.0);
    let picked = Picked::default();
    let options = AiChannelOptions {
        retry_policy: Some(RetryPolicy {
            max_retries: 0,
            ..RetryPolicy::default()
        }),
        ..AiChannelOptions::default()
    };
    let router = local_or_hosted(max_load, &picked);
    let agent = AiChannel::routed(ChannelId::new("assistant")?, router, options)?;

    let kit = shared::kit();
    let sms = SmsNumber::new(ChannelId::new("sms")?);
    let agent_id = agent.channel_id();
    kit.register_channel(sms.clone())?;
    kit.register_channel(agent)?;
    let room = RoomId::new("shop")?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    for channel in [sms.id(), &agent_id] {
        kit.attach_channel(&room, channel, AttachChannelOptions::default())
            .await?;
    }

    let mut served = Vec::new();
    for text in MESSAGES {
        println!("user: {text:?}");
        let message = InboundMessage::new(sms.id().clone(), "+15551234567", TextContent::new(text));
        let options = ProcessInboundOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        };
        kit.process_inbound(message, options).await?;
        let timeline = shared::timeline(&kit, &room).await?;
        let Some(answer) = timeline
            .last()
            .filter(|event| event.source.channel_id == agent_id)
        else {
            println!("  no answer");
            continue;
        };
        // The model that served the turn is on its answer (RFC §6.4).
        let model = answer
            .metadata
            .get("ai_model")
            .and_then(|model| model.as_str());
        let model = model.unwrap_or("?").to_owned();
        println!("  {model}: {:?}", text_of(&answer.content));
        served.push(model);
    }
    kit.close().await?;
    let picked = picked
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    Ok(checked(&served, &picked).finish())
}

/// The checks on the models that `served` the turns, as the router `picked`
/// them: the local model's second request, refused, is served by the hosted
/// one.
fn checked(served: &[String], picked: &[(RouteReason, String)]) -> Checks {
    let picks = |of: RouteReason| -> Vec<&str> {
        let picked = picked.iter().filter(|(reason, _)| *reason == of);
        picked.map(|(_, model)| model.as_str()).collect()
    };
    let (turns, retries) = (picks(RouteReason::Turn), picks(RouteReason::Retry));
    let mut checks = Checks::default();
    checks.expect(
        "each message gets one answer",
        served.len() == MESSAGES.len(),
    );
    checks.expect(
        "the router is asked at each turn's start",
        turns.len() == MESSAGES.len(),
    );
    let mut locals = 0;
    let expected: Vec<&str> = turns
        .iter()
        .map(|&model| {
            locals += usize::from(model == "qwen-local");
            if model == "qwen-local" && locals == 2 {
                "kimi-hosted"
            } else {
                model
            }
        })
        .collect();
    if locals >= 2 {
        let handed = retries == ["kimi-hosted"];
        checks.expect(
            "the local model's refused generation is handed to the hosted one",
            handed,
        );
    } else {
        checks.skip(
            "the local model's refused generation is handed to the hosted one",
            "the machine is loaded: the router sent the turns to the hosted model",
        );
    }
    checks.expect(
        "each answer names the model that served it",
        served == expected,
    );
    checks
}
