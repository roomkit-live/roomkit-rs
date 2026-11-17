use std::sync::{Arc, Mutex};

use super::support::{Door, tool};
use roomkit::model::Metadata;
use roomkit_ai::{
    AiChannelOptions, AiResponse, AiTool, AiToolCall, MockAiProvider, MockAiProviderOptions,
    ToolHandler, ToolResult,
};
use serde_json::json;

// Python: tests/test_channels/test_ai_extra_tools_turns.py::test_renaming_a_definition_does_not_rename_its_handler
#[tokio::test]
async fn renaming_a_definition_does_not_rename_its_handler() {
    for agent in [false, true] {
        for streaming in [false, true] {
            for self_served in [false, true] {
                let called = Arc::new(Mutex::new(Vec::new()));
                let calls = Arc::clone(&called);
                let handler = ToolHandler::new(move |name, _| {
                    calls.lock().unwrap().push(name.clone());
                    async move { Ok(ToolResult::Text(format!("served:{name}"))) }
                });
                let model = MockAiProvider::new(MockAiProviderOptions {
                    streaming,
                    ai_responses: Some(vec![
                        AiResponse {
                            tool_calls: vec![AiToolCall::new("c1", "renamed")],
                            ..AiResponse::new("")
                        },
                        AiResponse::new("done"),
                    ]),
                    ..Default::default()
                });
                let definition = if self_served {
                    tool("original")
                } else {
                    AiTool::new("original", "host tool").unwrap().into()
                };
                let door = Door::new(
                    agent,
                    model.clone(),
                    AiChannelOptions {
                        tools: vec![definition],
                        tool_handler: (!self_served).then_some(handler),
                        tool_search: Some(false),
                        ..Default::default()
                    },
                );
                let selected = door.tools().remove(0);
                let mut renamed = selected.get();
                renamed.name = "renamed".into();
                selected.replace(renamed);
                door.turn(Metadata::new()).await;
                door.close().await;
                let requests = model.calls();
                assert_eq!(
                    requests[0]
                        .tools
                        .iter()
                        .map(|t| t.name.as_str())
                        .collect::<Vec<_>>(),
                    ["renamed", "read_stored_result"]
                );
                let results: Vec<_> = requests[1]
                    .messages
                    .iter()
                    .flat_map(|m| m.tool_results())
                    .collect();
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].is_error, self_served);
                if self_served {
                    assert!(called.lock().unwrap().is_empty());
                    assert_eq!(
                        results[0].outcome,
                        Some(roomkit::model::ToolCallOutcome::Unserved)
                    );
                    assert_eq!(
                        results[0].as_text(),
                        "{\"error\": \"No handler for tool renamed\"}"
                    );
                } else {
                    assert_eq!(*called.lock().unwrap(), ["renamed"]);
                    assert_eq!(results[0].as_text(), "served:renamed");
                }
            }
        }
    }
}

// Python: tests/test_channels/test_ai_extra_tools.py::test_binding_tools_are_offered_without_becoming_host_tools
#[tokio::test]
async fn binding_tools_are_offered_without_becoming_host_tools() {
    for agent in [false, true] {
        for streaming in [false, true] {
            let model = MockAiProvider::new(MockAiProviderOptions {
                streaming,
                responses: vec!["done".into()],
                ..Default::default()
            });
            let host = AiTool::new("host", "constructor tool").unwrap();
            let door = Door::new(
                agent,
                model.clone(),
                AiChannelOptions {
                    tools: vec![host.clone().into()],
                    tool_search: Some(false),
                    ..Default::default()
                },
            );
            door.turn(
                serde_json::from_value(
                    json!({"tools":[{"name":"room_tool","description":"binding tool"}]}),
                )
                .unwrap(),
            )
            .await;
            door.close().await;
            assert_eq!(
                model.calls()[0]
                    .tools
                    .iter()
                    .map(|t| t.name.as_str())
                    .collect::<Vec<_>>(),
                ["room_tool", "host", "read_stored_result"]
            );
            assert_eq!(
                door.tools()
                    .iter()
                    .map(roomkit_ai::AiToolHandle::get)
                    .collect::<Vec<_>>(),
                [host]
            );
        }
    }
}

// Python: tests/test_channels/test_ai_extra_tools_preparation.py::test_a_configuration_callback_can_edit_the_host_definition
#[tokio::test]
async fn a_configuration_callback_can_edit_the_host_definition() {
    for agent in [false, true] {
        for streaming in [false, true] {
            let selected: Arc<Mutex<Option<roomkit_ai::AiToolHandle>>> = Arc::default();
            let edit = Arc::clone(&selected);
            let configured = move |_,
                                   _|
                  -> std::pin::Pin<
                Box<
                    dyn Future<
                            Output = Result<
                                Option<roomkit_ai::AiChannelTurnConfig>,
                                roomkit::ChannelError,
                            >,
                        > + Send,
                >,
            > {
                let handle = edit.lock().unwrap().as_ref().unwrap().clone();
                let mut definition = handle.get();
                definition.description = "edited while configuring".into();
                handle.replace(definition);
                Box::pin(async { Ok(Some(roomkit_ai::AiChannelTurnConfig::default())) })
            };
            let model = MockAiProvider::new(MockAiProviderOptions {
                streaming,
                responses: vec!["done".into()],
                ..Default::default()
            });
            let door = Door::new(
                agent,
                model.clone(),
                AiChannelOptions {
                    tools: vec![AiTool::new("read", "before").unwrap().into()],
                    config_provider: Some(Arc::new(configured)),
                    tool_search: Some(false),
                    ..Default::default()
                },
            );
            *selected.lock().unwrap() = Some(door.tools().remove(0));
            door.turn(Metadata::new()).await;
            door.close().await;
            assert_eq!(
                model.calls()[0].tools[0].description,
                "edited while configuring"
            );
        }
    }
}

// Python: tests/test_channels/test_ai_extra_tools.py::test_a_returned_definition_remains_live_for_the_next_turn
#[tokio::test]
async fn a_returned_definition_remains_live_for_the_next_turn() {
    for agent in [false, true] {
        for streaming in [false, true] {
            let model = MockAiProvider::new(MockAiProviderOptions {
                streaming,
                responses: vec!["done".into()],
                ..Default::default()
            });
            let hidden = AiTool {
                defer_loading: true,
                ..AiTool::new("read", "original").unwrap()
            };
            let door = Door::new(
                agent,
                model.clone(),
                AiChannelOptions {
                    tools: vec![hidden.into()],
                    tool_search: Some(false),
                    ..Default::default()
                },
            );
            door.turn(Metadata::new()).await;
            assert_eq!(
                model.calls()[0]
                    .tools
                    .iter()
                    .map(|t| t.name.as_str())
                    .collect::<Vec<_>>(),
                ["read_stored_result"]
            );
            let updated = AiTool {
                parameters: json!({"type":"object"}),
                tags: vec!["updated".into()],
                changes_state: true,
                ..AiTool::new("read", "updated through the public getter").unwrap()
            };
            door.tools()[0].replace(updated.clone());
            door.turn(Metadata::new()).await;
            door.close().await;
            let calls = model.calls();
            assert_eq!(calls.len(), 2);
            assert_eq!(calls[1].tools[0], updated);
            assert_eq!(door.tools()[0].get(), updated);
        }
    }
}
