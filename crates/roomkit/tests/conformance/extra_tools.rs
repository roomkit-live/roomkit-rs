//! Host definitions read and edited through each channel's public accessor.

use std::sync::Arc;

use roomkit_ai::{AiTool, AiToolHandle};
use serde_json::{Value, json};

use crate::agents::Scripted;
use crate::calls::{Called, Unreadable, field};
use crate::run::{Run, lock};

pub(crate) type Reader = Arc<dyn Fn() -> Vec<AiToolHandle> + Send + Sync>;

pub(crate) fn reader(agent: &Scripted) -> Reader {
    match agent {
        Scripted::Channel(channel) => {
            let channel = channel.clone();
            Arc::new(move || channel.extra_tools())
        }
        Scripted::Agent(agent) => {
            let tools = agent.extra_tools();
            Arc::new(move || tools.clone())
        }
        Scripted::RoutedChannel(channel) => {
            let channel = channel.clone();
            Arc::new(move || channel.extra_tools())
        }
        Scripted::RoutedAgent(agent) => {
            let tools = agent.extra_tools();
            Arc::new(move || tools.clone())
        }
    }
}

pub(crate) fn call(run: &Run, name: &str, spec: &Value) -> Result<Called, Unreadable> {
    let id: String = field(spec, "channel_id")?;
    let read = lock(&run.host_tool_readers)
        .get(&id)
        .cloned()
        .ok_or_else(|| format!("{name}: {id} is no scripted AI channel"))?;
    let definitions = read();
    if name == "agent.extra_tools" {
        return Ok(Ok(Value::Array(
            definitions.iter().map(|tool| dump(tool.get())).collect(),
        )));
    }
    let index: usize = field(spec, "index")?;
    let tool = definitions
        .get(index)
        .ok_or_else(|| format!("{name}: no host definition at {index}"))?;
    tool.replace(definition(&spec["definition"])?);
    Ok(Ok(Value::Null))
}

fn definition(spec: &Value) -> Result<AiTool, Unreadable> {
    Ok(AiTool {
        name: field(spec, "name")?,
        description: field(spec, "description")?,
        parameters: spec.get("parameters").cloned().unwrap_or_else(|| json!({})),
        tags: spec
            .get("tags")
            .map(|_| field(spec, "tags"))
            .transpose()?
            .unwrap_or_default(),
        changes_state: spec["changes_state"].as_bool().unwrap_or(false),
        defer_loading: spec["defer_loading"].as_bool().unwrap_or(false),
    })
}

fn dump(tool: AiTool) -> Value {
    json!({"name":tool.name, "description":tool.description, "parameters":tool.parameters,
        "tags":tool.tags, "changes_state":tool.changes_state, "defer_loading":tool.defer_loading})
}
