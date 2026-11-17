//! Python's result fields at the next-generation boundary (RFC §6.4, §6.7).
use std::future::Future;

use roomkit::model::{Metadata, ResponseMetadata, ToolCallOutcome};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiMessage, AiProvider, AiResponse, AiTool,
    AiToolResultPart, ProviderError, ProviderStream, ToolError, Tools, current_tool_call,
};
use serde_json::json;

use super::support::{Room, channel};

#[derive(Clone)]
struct Model {
    script: ScriptedProvider,
    streaming: bool,
}

impl AiProvider for Model {
    fn model_name(&self) -> &'static str {
        "result-carriers"
    }

    fn supports_structured_streaming(&self) -> bool {
        self.streaming
    }

    fn generate<'a>(
        &'a self,
        context: &'a AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        self.script.generate(context)
    }

    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        self.script.generate_structured_stream(context)
    }
}

fn parts(context: &AiContext) -> Vec<&AiToolResultPart> {
    context
        .messages
        .iter()
        .flat_map(AiMessage::tool_results)
        .collect()
}

async fn two_turns(streaming: bool, copy: Option<Metadata>) -> Vec<AiContext> {
    let script = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "lookup", "{}"), ("c2", "fail", "{}")]),
        ScriptedRound::answering("First done."),
        ScriptedRound::answering("Second done."),
    ]);
    let tools = Tools::new()
        .tool(AiTool::new("lookup", "Read").unwrap(), move |_, _| {
            let copy = copy.clone();
            async move {
                current_tool_call()
                    .unwrap()
                    .structured_content
                    .replace(copy.map(ResponseMetadata::from));
                Ok("model text".into())
            }
        })
        .tool(AiTool::new("fail", "Fail").unwrap(), |_, _| async {
            Err(ToolError::raised_as("RuntimeError", "broken"))
        });
    let agent = AiChannel::new(
        channel("assistant"),
        Model {
            script: script.clone(),
            streaming,
        },
        AiChannelOptions {
            tools: tools.into(),
            ..Default::default()
        },
    )
    .unwrap();
    let room = Room::with_agent(agent).await;
    room.says("first").await;
    room.says("second").await;
    room.kit.close().await.unwrap();
    script.requests()
}

// Python: tests/test_channels/test_ai_result_carriers.py::test_next_round_reads_the_structured_copy_and_typed_outcome
// Python: tests/test_channels/test_ai_result_carriers.py::test_history_replays_the_outcome_but_not_the_structured_copy
// Python: tests/test_channels/test_ai_result_carriers.py::test_a_later_turn_replays_typed_outcomes
#[tokio::test]
async fn the_next_round_reads_the_copy_but_a_later_turn_only_replays_the_outcome() {
    for streaming in [false, true] {
        for copy in [
            None,
            Some(Metadata::new()),
            Some(json!({"page": 3}).as_object().unwrap().clone()),
        ] {
            let requests = two_turns(streaming, copy.clone()).await;
            assert_eq!(requests.len(), 3);
            for (context, expected_copy) in [
                (requests.get(1).unwrap(), copy),
                (requests.get(2).unwrap(), None),
            ] {
                let results = parts(context);
                assert_eq!(results.len(), 2);
                let served = results.first().unwrap();
                assert_eq!(served.tool_call_id, "c1");
                assert_eq!(served.structured_content, expected_copy);
                assert_eq!(served.outcome, Some(ToolCallOutcome::Served));
                assert!(!served.is_error);
                assert_eq!(served.as_text(), "model text");
                assert_eq!(
                    served.split_for_message(),
                    ("model text".to_owned(), Vec::new())
                );
                let failed = results.get(1).unwrap();
                assert_eq!(failed.tool_call_id, "c2");
                assert_eq!(failed.outcome, Some(ToolCallOutcome::Failed));
                assert!(failed.is_error);
                assert!(failed.structured_content.is_none());
            }
        }
    }
}
