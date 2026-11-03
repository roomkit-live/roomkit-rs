//! The tools that ask a person (RFC §9.3, §21.6): Python's
//! `HumanInputToolHandler`, which an AI channel given it as its
//! [`human_input_handler`](crate::AiChannelOptions::human_input_handler)
//! serves itself.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use roomkit::RoomKitError;
use roomkit::human_input::{CreateOptions, HumanInputHandler, WaitOptions};
use roomkit::model::{ChannelType, Metadata, ToolCall};
use roomkit_core::rules::python_repr;

use crate::error::AiError;
use crate::texts::error_envelope;
use crate::tools::{AiTool, BoxFuture, DynToolset, ToolContext, ToolError, ToolResult};

/// What [`HumanInputToolHandler::new`] takes beside its tool names:
/// Python's `HumanInputToolHandler(tool_names, timeout=300, handler=None,
/// tool_definitions=None)`.
#[derive(Clone, Debug)]
pub struct HumanInputToolHandlerOptions {
    /// Python `timeout=300`: how long a call waits for the person's answer,
    /// its only bound (RFC §21.6): the channel's bound per call does not
    /// apply to it.
    pub timeout: Duration,
    /// Python `handler=None`: the handler keeping the requests, a new one
    /// when `None`.
    pub handler: Option<HumanInputHandler>,
    /// Python `tool_definitions=None`: the tools declared to the model,
    /// each under one of the names served. A name served without one is
    /// declared by the channel's other tools.
    pub tool_definitions: Vec<AiTool>,
}

impl Default for HumanInputToolHandlerOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(300),
            handler: None,
            tool_definitions: Vec::new(),
        }
    }
}

/// What [`HumanInputToolHandler::ask`] takes beside the call: Python's
/// `ask(name, arguments, *, channel_type=ChannelType.AI)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AskOptions {
    /// Python `channel_type=ChannelType.AI`: the door the call came
    /// through, which the request and its `ON_USER_INPUT_REQUIRED` event
    /// name.
    pub channel_type: ChannelType,
}

impl Default for AskOptions {
    fn default() -> Self {
        Self {
            channel_type: ChannelType::AI,
        }
    }
}

/// The tools that ask a person (RFC §9.3): Python's
/// `HumanInputToolHandler`. A call to one of its names opens a request on
/// its [`handler`](Self::handler) and waits for the person's answer, which
/// the model reads; a rejection, a person's or an `ON_USER_INPUT_REQUIRED`
/// hook's block, it reads as a refusal, and no answer within its timeout as
/// a failure.
///
/// Given to an [`AiChannel`](crate::AiChannel) as its
/// [`human_input_handler`](crate::AiChannelOptions::human_input_handler),
/// the channel declares [`tools`](Self::tools) and serves its names itself,
/// before its tool handler, under its own timeout, and its close settles
/// the requests still open. Cloning it shares its handler.
#[derive(Clone, Debug)]
pub struct HumanInputToolHandler {
    tool_names: BTreeSet<String>,
    timeout: Duration,
    handler: HumanInputHandler,
    tool_definitions: Vec<AiTool>,
}

impl HumanInputToolHandler {
    /// The tools `tool_names`, as `options` configure them.
    ///
    /// # Errors
    ///
    /// As Python's constructor raises `ValueError`:
    /// [`AiError::HumanInputNames`] for no name, and
    /// [`AiError::HumanInputDefinition`] for a definition under a name not
    /// among them.
    pub fn new<I, N>(tool_names: I, options: HumanInputToolHandlerOptions) -> Result<Self, AiError>
    where
        I: IntoIterator<Item = N>,
        N: Into<String>,
    {
        let tool_names: BTreeSet<String> = tool_names.into_iter().map(Into::into).collect();
        if tool_names.is_empty() {
            return Err(AiError::HumanInputNames);
        }
        if let Some(stray) = options
            .tool_definitions
            .iter()
            .find(|tool| !tool_names.contains(&tool.name))
        {
            return Err(AiError::HumanInputDefinition {
                name: stray.name.clone(),
                tool_names: tool_names.iter().cloned().collect(),
            });
        }
        Ok(Self {
            tool_names,
            timeout: options.timeout,
            handler: options.handler.unwrap_or_default(),
            tool_definitions: options.tool_definitions,
        })
    }

    /// The handler keeping the requests, which answers and rejects them.
    pub fn handler(&self) -> &HumanInputHandler {
        &self.handler
    }

    /// The tools declared to the model.
    pub fn tools(&self) -> Vec<AiTool> {
        self.tool_definitions.clone()
    }

    /// The names it serves: a call to one asks a person.
    pub fn tool_names(&self) -> &BTreeSet<String> {
        &self.tool_names
    }

    /// How long a call waits for the person's answer.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Asks the person the call `name` with `arguments` makes, on the door
    /// `options.channel_type`, and waits for the answer: the person's
    /// answer as the call's result. The call it runs within
    /// ([`current_tool_call`](crate::current_tool_call)) names the request's
    /// room, call and channel.
    ///
    /// # Errors
    ///
    /// [`ToolError::UnservedToolCall`] for a name it does not serve, so a
    /// chain of handlers goes on to the next (RFC §21.4);
    /// [`ToolError::Refused`] for a request rejected, the model reading
    /// `{"error": "Human input rejected: <reason>"}`;
    /// [`ToolError::Failed`] when nobody answers within its timeout, the
    /// model reading `{"error": "Human input timed out after <s>s for tool
    /// '<name>'"}`; [`ToolError::Raised`] for any other failure, the
    /// handler giving the request up included, its message withheld from
    /// the model (RFC §9.3).
    pub async fn ask(
        &self,
        name: &str,
        arguments: Metadata,
        options: AskOptions,
    ) -> Result<String, ToolError> {
        let call = crate::current_tool_call().unwrap_or_default();
        let request = CreateOptions {
            room_id: call.room_id.map(|id| id.to_string()).unwrap_or_default(),
            tool_call_id: call.tool_call_id,
            channel_id: call.channel_id.map(|id| id.to_string()).unwrap_or_default(),
            channel_type: options.channel_type,
            actor_id: crate::current_tool_actor_id(),
        };
        self.asked(name, arguments, request).await
    }

    /// Opens the request `request` describes for the call `name`, and waits
    /// for its outcome, read as the model reads it (RFC §9.3).
    async fn asked(
        &self,
        name: &str,
        arguments: Metadata,
        request: CreateOptions,
    ) -> Result<String, ToolError> {
        if !self.tool_names.contains(name) {
            return Err(ToolError::unserved(format!(
                "tool {} is not served here",
                python_repr(name)
            )));
        }
        let waited = match self.handler.create(name, arguments, request).await {
            Ok(pending) => {
                let options = WaitOptions {
                    timeout: self.timeout,
                };
                self.handler.wait(&pending.pending_id, options).await
            }
            Err(error) => Err(error),
        };
        waited.map_err(|error| self.failure(name, &error))
    }

    /// How a request's failure reads to the model (RFC §9.3).
    fn failure(&self, name: &str, error: &RoomKitError) -> ToolError {
        if let RoomKitError::HumanInputRejected(reason) = error {
            return ToolError::refused(error_envelope(&format!("Human input rejected: {reason}")));
        }
        if matches!(error, RoomKitError::Timeout) {
            return ToolError::failed(error_envelope(&format!(
                "Human input timed out after {}s for tool '{name}'",
                python_seconds(self.timeout)
            )));
        }
        let kind = if let RoomKitError::RemoteTurn { kind, .. } = error {
            kind.clone()
        } else if matches!(error, RoomKitError::Runtime(_)) {
            "RuntimeError".to_owned()
        } else {
            "RoomKitError".to_owned()
        };
        ToolError::raised_as(kind, error.to_string())
    }

    /// The toolset an AI channel serves this handler's names with.
    pub(crate) fn toolset(&self) -> Arc<dyn DynToolset> {
        Arc::new(HumanInputToolset(self.clone()))
    }
}

/// `duration` as Python writes the seconds of a bound: a whole number as
/// an integer, as its default is given, any other as its float.
fn python_seconds(duration: Duration) -> String {
    if duration.subsec_nanos() == 0 {
        duration.as_secs().to_string()
    } else {
        duration.as_secs_f64().to_string()
    }
}

/// What an AI channel serves a person's tools with, on its own door.
struct HumanInputToolset(HumanInputToolHandler);

impl DynToolset for HumanInputToolset {
    fn call(
        &self,
        call: ToolCall,
        context: ToolContext,
    ) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
        Box::pin(async move {
            let request = CreateOptions {
                room_id: context.room_id.to_string(),
                tool_call_id: call.id.clone(),
                channel_id: context.channel_id.to_string(),
                channel_type: ChannelType::AI,
                actor_id: context.actor_id.clone(),
            };
            self.0
                .asked(&call.name, call.arguments, request)
                .await
                .map(ToolResult::Text)
        })
    }

    fn bounds_itself(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn asking(timeout: Duration) -> HumanInputToolHandler {
        let options = HumanInputToolHandlerOptions {
            timeout,
            ..HumanInputToolHandlerOptions::default()
        };
        HumanInputToolHandler::new(["AskUser"], options).unwrap()
    }

    /// The id of the first request `human` opens.
    async fn first_request(human: &HumanInputToolHandler) -> String {
        loop {
            if let Some(id) = human.handler().pending().keys().next() {
                return id.clone();
            }
            tokio::task::yield_now().await;
        }
    }

    // Python: tests/test_human_input.py::test_tool_handler_falls_through_for_unknown
    #[tokio::test]
    async fn a_name_it_does_not_serve_falls_through() {
        let human = asking(Duration::from_secs(1));
        let answer = human
            .ask("other", Metadata::new(), AskOptions::default())
            .await;
        // Python's `UnservedToolCallError(f"tool {name!r} is not served here")`.
        assert!(
            matches!(&answer, Err(ToolError::UnservedToolCall(text)) if text == "tool 'other' is not served here"),
            "{answer:?}"
        );
    }

    // Python: tests/test_human_input.py::test_tool_handler_blocks_and_resolves
    #[tokio::test]
    async fn a_call_waits_for_the_person_s_answer() {
        let human = asking(Duration::from_secs(2));
        let asked = tokio::spawn({
            let human = human.clone();
            async move {
                human
                    .ask("AskUser", Metadata::new(), AskOptions::default())
                    .await
            }
        });
        let id = first_request(&human).await;
        assert!(human.handler().resolve(&id, "blue"));
        assert_eq!(asked.await.unwrap(), Ok("blue".to_owned()));
    }

    // Python: tests/test_human_input.py::test_tool_handler_timeout
    #[tokio::test]
    async fn no_answer_in_time_is_a_failure_the_model_reads() {
        let human = asking(Duration::from_millis(20));
        let answer = human
            .ask("AskUser", Metadata::new(), AskOptions::default())
            .await;
        assert_eq!(
            answer,
            Err(ToolError::Failed(
                "{\"error\": \"Human input timed out after 0.02s for tool 'AskUser'\"}".to_owned()
            ))
        );
    }

    // Python: tests/test_human_input.py::test_tool_handler_rejection
    #[tokio::test]
    async fn a_rejection_is_a_refusal_the_model_reads() {
        let human = asking(Duration::from_secs(2));
        let asked = tokio::spawn({
            let human = human.clone();
            async move {
                human
                    .ask("AskUser", Metadata::new(), AskOptions::default())
                    .await
            }
        });
        let id = first_request(&human).await;
        let reason = roomkit::human_input::RejectOptions {
            reason: "Not now".to_owned(),
        };
        assert!(human.handler().reject(&id, reason));
        assert_eq!(
            asked.await.unwrap(),
            Err(ToolError::Refused(
                "{\"error\": \"Human input rejected: Not now\"}".to_owned()
            ))
        );
    }

    // Python: tests/test_human_input.py::test_tool_handler_exposes_inner_handler,
    // test_tool_handler_creates_handler_if_not_provided
    #[tokio::test]
    async fn it_answers_through_the_handler_it_was_given() {
        let handler = HumanInputHandler::default();
        let options = HumanInputToolHandlerOptions {
            handler: Some(handler.clone()),
            ..HumanInputToolHandlerOptions::default()
        };
        let human = HumanInputToolHandler::new(["AskUser"], options).unwrap();
        let asked = tokio::spawn({
            let human = human.clone();
            async move {
                human
                    .ask("AskUser", Metadata::new(), AskOptions::default())
                    .await
            }
        });
        let id = first_request(&human).await;
        assert!(handler.resolve(&id, "yes"));
        assert_eq!(asked.await.unwrap(), Ok("yes".to_owned()));
    }

    // Python: tests/test_human_input.py::test_empty_tool_names_raises, and the
    // constructor's refusal of a definition it does not serve
    #[test]
    fn it_refuses_no_name_and_a_definition_it_does_not_serve() {
        let none: [&str; 0] = [];
        assert_eq!(
            HumanInputToolHandler::new(none, HumanInputToolHandlerOptions::default()).err(),
            Some(AiError::HumanInputNames)
        );
        let options = HumanInputToolHandlerOptions {
            tool_definitions: vec![AiTool::new("other", "Not served.").unwrap()],
            ..HumanInputToolHandlerOptions::default()
        };
        let refused = HumanInputToolHandler::new(["AskUser"], options).unwrap_err();
        assert_eq!(
            refused.to_string(),
            "Tool definition 'other' not in tool_names {'AskUser'}"
        );
    }
}
