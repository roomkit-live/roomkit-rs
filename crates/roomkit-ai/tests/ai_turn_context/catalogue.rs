use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::{RoomContext, RoomEvent, RoomId, TextContent};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, HookOptions, HookTrigger, SendEventOptions,
};
use roomkit_ai::memory::{MemoryProvider, MemoryResult, RetrieveOptions};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiTool, ToolEntry, ToolHandler, ToolResult, ToolRoundEvent,
    ToolTurnContextOptions, current_tool_allowed_names, tool_turn_context,
};
use tokio::sync::oneshot;

use super::support::{binding, event, model};

type Names = Option<HashSet<String>>;
type Readings = Arc<Mutex<Vec<(&'static str, Names)>>>;
type Child = (oneshot::Sender<()>, tokio::task::JoinHandle<Names>);

fn names(values: &[&str]) -> HashSet<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn tools(values: &[&str]) -> Vec<ToolEntry> {
    values
        .iter()
        .map(|name| ToolEntry::Definition(AiTool::new(*name, *name).unwrap()))
        .collect()
}

struct Memory(Readings);

impl MemoryProvider for Memory {
    fn retrieve<'a>(
        &'a self,
        _: &'a RoomId,
        _: &'a RoomEvent,
        _: &'a RoomContext,
        _: RetrieveOptions,
    ) -> Pin<Box<dyn Future<Output = Result<MemoryResult, ChannelError>> + Send + 'a>> {
        self.0
            .lock()
            .unwrap()
            .push(("memory", current_tool_allowed_names()));
        Box::pin(async { Ok(MemoryResult::default()) })
    }
}

// Python: test_channels/test_allowed_names_lifetime.py::test_resolution_distinguishes_unresolved_from_empty
#[tokio::test]
async fn resolution_distinguishes_unresolved_from_empty() {
    for streaming in [false, true] {
        for offered in [vec![], vec!["check"]] {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let configured = Arc::clone(&seen);
            let agent = AiChannel::new(
                "ai1".parse().unwrap(),
                model(streaming, false),
                AiChannelOptions {
                    memory: Some(Arc::new(Memory(Arc::clone(&seen)))),
                    config_provider: Some(Arc::new(move |_, _| {
                        configured
                            .lock()
                            .unwrap()
                            .push(("config", current_tool_allowed_names()));
                        Box::pin(async { Ok(None) })
                    })),
                    tools: tools(&offered),
                    tool_search: Some(false),
                    ..AiChannelOptions::default()
                },
            )
            .unwrap();
            let (input, context) = event("r1", None);
            let mut binding = binding("r1");
            binding.metadata.clear();
            tool_turn_context(
                ToolTurnContextOptions {
                    tools: Some(vec![AiTool::new("outer", "outer").unwrap()]),
                    ..ToolTurnContextOptions::default()
                },
                async {
                    agent.on_event(input, binding, context).await.unwrap();
                    assert_eq!(current_tool_allowed_names(), Some(names(&["outer"])));
                },
            )
            .await
            .unwrap();
            assert_eq!(current_tool_allowed_names(), None);
            assert_eq!(
                *seen.lock().unwrap(),
                vec![("config", None), ("memory", Some(names(&offered)))]
            );
            agent.close().await;
        }
    }
}

fn child() -> Child {
    let (resume, ready) = oneshot::channel();
    let task = tokio::spawn(roomkit::keep_context(async move {
        ready.await.unwrap();
        current_tool_allowed_names()
    }));
    (resume, task)
}

async fn finish(child: Child) -> Names {
    child.0.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), child.1)
        .await
        .unwrap()
        .unwrap()
}

// Python: test_channels/test_allowed_names_lifetime.py::test_preparation_keeps_its_base_but_shares_round_declarations
#[tokio::test]
async fn preparation_keeps_its_base_but_shares_round_declarations() {
    for streaming in [false, true] {
        let kit = super::lifetime::kit().await;
        let preparation = Arc::new(Mutex::new(None));
        let consumption = Arc::new(Mutex::new(None));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let prepared = Arc::clone(&preparation);
        let consumed = Arc::clone(&consumption);
        let called = Arc::clone(&calls);
        let agent = AiChannel::new(
            "ai1".parse().unwrap(),
            model(streaming, true),
            AiChannelOptions {
                config_provider: Some(Arc::new(move |_, _| {
                    *prepared.lock().unwrap() = Some(child());
                    Box::pin(async { Ok(None) })
                })),
                tools: tools(&["check", "book"]),
                tool_search: Some(false),
                tool_handler: Some(ToolHandler::new(move |_, _| {
                    let mut calls = called.lock().unwrap();
                    if calls.is_empty() {
                        *consumed.lock().unwrap() = Some(child());
                    }
                    calls.push(current_tool_allowed_names());
                    async { Ok(ToolResult::Text("done".into())) }
                })),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        kit.register_channel(agent).unwrap();
        kit.attach_channel(
            &"r1".parse().unwrap(),
            &"ai1".parse().unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
        kit.hook(
            HookTrigger::AfterToolRound,
            |mut event: ToolRoundEvent, _| async move {
                event.withdraw(["book", "read_stored_result"]);
                roomkit::model::HookResult::modify(
                    event,
                    roomkit::model::hook::ModifyOptions::default(),
                )
            },
            HookOptions::default(),
        )
        .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            kit.send_event(
                &"r1".parse().unwrap(),
                &"sms1".parse().unwrap(),
                TextContent::new("go"),
                SendEventOptions::default(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        let prepared = preparation.lock().unwrap().take().unwrap();
        let consumed = consumption.lock().unwrap().take().unwrap();
        assert_eq!(
            finish(prepared).await,
            Some(names(&["check", "book", "read_stored_result"]))
        );
        assert_eq!(finish(consumed).await, Some(names(&["check"])));
        assert_eq!(
            *calls.lock().unwrap(),
            vec![
                Some(names(&["check", "book", "read_stored_result"])),
                Some(names(&["check"]))
            ]
        );
        assert_eq!(current_tool_allowed_names(), None);
        kit.close().await.unwrap();
    }
}
