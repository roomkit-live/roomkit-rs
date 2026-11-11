//! A channel whose router picks each turn's provider (RFC §6.4; Python's
//! `tests/test_channels/test_ai_router.py`).
//!
//! The shared scenarios under `ai-router/` pin what a room observes: which
//! model serves each turn and round, what the router is asked, the context
//! built for the model it picks. These tests pin the channel's own surface:
//! what it names, what it closes, the window and the price of the model it
//! picks.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use pretty_assertions::assert_eq;
use roomkit::Channel;
use roomkit::model::RetryPolicy;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContentPart, AiContext, AiMessageContent, AiProvider,
    AiResponse, AiRouter, AiRouterFuture, AiTool, DynAiProvider, FallbackRouter, ProviderError,
    RouteReason, RouteRequest, ToolResult, Tools,
};

use super::bounds::Priced;
use super::support::{Room, channel};

/// Names `provider` for every request, and keeps the requests.
struct Picks {
    provider: Option<Arc<dyn DynAiProvider>>,
    requests: Arc<Mutex<Vec<RouteRequest>>>,
    closed: Arc<AtomicBool>,
}

impl Picks {
    fn naming(provider: Option<Arc<dyn DynAiProvider>>) -> Self {
        Self {
            provider,
            requests: Arc::default(),
            closed: Arc::default(),
        }
    }
}

impl AiRouter for Picks {
    fn route(&self, request: RouteRequest) -> AiRouterFuture<'_> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(request);
        Box::pin(std::future::ready(self.provider.clone()))
    }

    fn close(&self) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.closed.store(true, Ordering::SeqCst);
        Box::pin(async {})
    }
}

/// A scripted model whose context window is `.1` tokens.
struct Window(ScriptedProvider, u64);

impl AiProvider for Window {
    fn model_name(&self) -> &'static str {
        "windowed"
    }

    fn context_window(&self) -> Option<u64> {
        Some(self.1)
    }

    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        self.0.generate(context).await
    }
}

/// A model that records its close.
struct Closing(Arc<AtomicBool>);

impl AiProvider for Closing {
    fn model_name(&self) -> &'static str {
        "closing"
    }

    fn generate(
        &self,
        _context: &AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send {
        std::future::ready(Ok(AiResponse::new("")))
    }

    async fn close(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn answering(text: &str) -> ScriptedProvider {
    ScriptedProvider::new([ScriptedRound::answering(text)])
}

fn routed(router: impl AiRouter, options: AiChannelOptions) -> AiChannel<roomkit_ai::Routed> {
    AiChannel::routed(channel("assistant"), router, options).unwrap()
}

// Python: tests/test_channels/test_ai_router.py::test_a_channel_with_a_router_names_the_router_not_a_provider
#[test]
fn a_channel_with_a_router_names_the_router_not_a_provider() {
    let agent = routed(
        Picks::naming(Some(Arc::new(answering("ok")))),
        AiChannelOptions::default(),
    );

    assert!(agent.provider().is_none());
    assert_eq!(agent.router().map(AiRouter::name), Some("Picks"));
    assert_eq!(Channel::provider_name(&agent).as_deref(), Some("Picks"));
}

// Python: tests/test_channels/test_ai_router.py::test_a_router_that_names_no_provider_for_a_turn_fails_it
#[tokio::test]
async fn a_router_that_names_no_provider_for_a_turn_fails_it() {
    let router = Picks::naming(None);
    let requests = Arc::clone(&router.requests);
    let room = Room::with_agent(routed(router, AiChannelOptions::default())).await;

    let result = room.says("Hello?").await;
    room.kit.close().await.unwrap();

    let error = result.error.unwrap();
    assert_eq!(
        (error.error_type.as_str(), error.message.as_str()),
        ("ValueError", "router 'Picks' named no provider for a turn")
    );
    let requests = requests.lock().unwrap_or_else(PoisonError::into_inner);
    let asked: Vec<_> = requests
        .iter()
        .map(|request| (request.reason, request.text.as_str()))
        .collect();
    assert_eq!(asked, [(RouteReason::Turn, "Hello?")]);
}

/// Thirty schemas, which pass 10 % of a 4,000-token window, never of a
/// million's.
fn catalogue() -> Tools {
    (0..30).fold(Tools::new(), |tools, index| {
        let tool = AiTool {
            parameters: serde_json::json!({
                "type": "object", "properties": { "id": { "type": "string" } },
            }),
            ..AiTool::new(
                format!("crm_action_{index}"),
                format!(
                    "Perform CRM operation number {index} on a customer record, with filters, \
                     pagination and audit metadata."
                ),
            )
            .unwrap()
        };
        tools.tool(tool, |_call, _context| async {
            Ok(ToolResult::Text("ok".to_owned()))
        })
    })
}

// Python: tests/test_channels/test_ai_router.py::test_the_window_of_the_model_the_router_picks_decides_tool_search
#[tokio::test]
async fn the_window_of_the_model_the_router_picks_decides_tool_search() {
    for (window, searched) in [(4_000, true), (1_000_000, false)] {
        let model = answering("ok");
        let picked = Window(model.clone(), window);
        let options = AiChannelOptions {
            tools: catalogue().into(),
            ..AiChannelOptions::default()
        };
        let room = Room::with_agent(routed(Picks::naming(Some(Arc::new(picked))), options)).await;

        room.says("go").await;
        room.kit.close().await.unwrap();

        let requests = model.requests();
        let declared: Vec<&str> = requests[0].tools.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(
            (
                declared.contains(&"find_tools"),
                declared.contains(&"crm_action_0")
            ),
            (searched, !searched),
            "window {window}"
        );
    }
}

// Python: tests/test_channels/test_turn_budget.py::test_a_cost_budget_is_checked_on_the_model_the_router_picks
#[tokio::test]
async fn a_cost_budget_is_checked_on_the_model_the_router_picks() {
    for priced in [false, true] {
        let model = answering("Done.");
        let picked: Arc<dyn DynAiProvider> = if priced {
            Arc::new(Priced(model))
        } else {
            Arc::new(model)
        };
        let router = FallbackRouter::new(picked, Arc::new(answering("unused")));
        let options = AiChannelOptions {
            turn_budget_usd: Some(1.0),
            ..AiChannelOptions::default()
        };
        // The channel has no provider to price when it is built.
        let room = Room::with_agent(routed(router, options)).await;

        let result = room.says("Order 1?").await;
        room.kit.close().await.unwrap();

        let failed = result.error.map(|error| error.message);
        if priced {
            assert_eq!(failed, None);
        } else {
            let failed = failed.unwrap_or_default();
            assert!(
                failed.starts_with("turn_budget_usd needs a model with a known price"),
                "{failed}"
            );
        }
    }
}

// Python: tests/test_channels/test_ai_router.py::test_the_channel_closes_its_router_and_a_fallback_router_both_providers
#[tokio::test]
async fn the_channel_closes_its_router_and_a_fallback_router_both_providers() {
    let router = Picks::naming(None);
    let closed = Arc::clone(&router.closed);
    let (primary, fallback) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let both = FallbackRouter::new(
        Arc::new(Closing(Arc::clone(&primary))),
        Arc::new(Closing(Arc::clone(&fallback))),
    );

    routed(router, AiChannelOptions::default()).close().await;
    routed(both, AiChannelOptions::default()).close().await;

    assert_eq!(
        (
            closed.load(Ordering::SeqCst),
            primary.load(Ordering::SeqCst),
            fallback.load(Ordering::SeqCst)
        ),
        (true, true, true)
    );
}

/// A scripted model that holds tools unseen, as Anthropic's does.
struct Holding(ScriptedProvider);

impl AiProvider for Holding {
    fn model_name(&self) -> &'static str {
        "holding"
    }

    fn supports_deferred_tools(&self) -> bool {
        true
    }

    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        self.0.generate(context).await
    }
}

/// `lookup_order` and `track_shipment`, each answering `ok`.
fn shop() -> Tools {
    ["lookup_order", "track_shipment"]
        .into_iter()
        .fold(Tools::new(), |tools, name| {
            let tool = AiTool::new(name, format!("{name} for a customer")).unwrap();
            tools.tool(tool, |_call, _context| async {
                Ok(ToolResult::Text("ok".to_owned()))
            })
        })
}

// Python: tests/test_deferred_tools.py::test_the_rounds_after_a_hand_over_reach_the_new_provider_without_references
#[tokio::test]
async fn the_rounds_after_a_hand_over_reach_the_new_provider_without_references() {
    // The turn starts on a provider that holds tools: find_tools reveals
    // track_shipment by reference. Its next round is handed to one that
    // cannot hold them, and every round after stays on it (RFC §6.4).
    let reveal = r#"{"query": "track shipment", "max_results": 1}"#;
    let holding = Holding(ScriptedProvider::new([
        ScriptedRound::calling("", [("c0", "find_tools", reveal)]),
        ScriptedRound::answering("")
            .refusing(503, "down")
            .retrying(),
    ]));
    let plain = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "track_shipment", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    let router = FallbackRouter::new(Arc::new(holding), Arc::new(plain.clone()));
    let options = AiChannelOptions {
        tools: shop().into(),
        tool_search: Some(true),
        retry_policy: Some(RetryPolicy {
            max_retries: 0,
            ..RetryPolicy::default()
        }),
        ..AiChannelOptions::default()
    };
    let room = Room::with_agent(routed(router, options)).await;

    let result = room.says("Where is my parcel?").await;
    room.kit.close().await.unwrap();

    assert!(result.error.is_none(), "{:?}", result.error);
    let requests = plain.requests();
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert!(request.tools.iter().all(|tool| !tool.defer_loading));
        let parts = request
            .messages
            .iter()
            .flat_map(|message| match &message.content {
                AiMessageContent::Parts(parts) => parts.clone(),
                AiMessageContent::Text(_) => Vec::new(),
            });
        let mut references = parts.filter_map(|part| {
            let AiContentPart::ToolResult(result) = part else {
                return None;
            };
            Some(result.references)
        });
        assert!(references.all(|named| named.is_empty()));
    }
    let declared: Vec<&str> = requests[1].tools.iter().map(|t| t.name.as_str()).collect();
    assert!(declared.contains(&"track_shipment"), "{declared:?}");
}
