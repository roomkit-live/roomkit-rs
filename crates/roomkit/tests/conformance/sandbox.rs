//! A host sandbox witness: records entry without implementing command semantics.
use std::collections::BTreeMap;
use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use roomkit::model::Metadata;
use roomkit_ai::{AiChannelOptions, SandboxExecutor, SandboxResult, ToolError};
use serde_json::{Value, json};

use crate::run::lock;

#[derive(Default)]
pub(crate) struct Sandboxes(Mutex<BTreeMap<String, Arc<ScriptedSandbox>>>);

struct ScriptedSandbox {
    definitions: Vec<Metadata>,
    result: SandboxResult,
    failure: Option<ToolError>,
    calls: Mutex<Vec<Value>>,
    closes: AtomicUsize,
}

impl ScriptedSandbox {
    fn new(spec: &Value) -> Result<Self, String> {
        spec.as_object().ok_or("sandbox: not an object")?;
        let result = spec
            .get("result")
            .cloned()
            .unwrap_or_else(|| json!({"exit_code":0}));
        let failure = spec
            .get("raises")
            .map(|failure| {
                let kind = failure["kind"]
                    .as_str()
                    .ok_or("sandbox error kind: not text")?;
                let message = failure["message"]
                    .as_str()
                    .ok_or("sandbox error message: not text")?;
                Ok::<_, String>(ToolError::raised_as(kind, message))
            })
            .transpose()?;
        Ok(Self {
            definitions: serde_json::from_value(
                spec.get("tools").cloned().unwrap_or_else(|| json!([])),
            )
            .map_err(|error| error.to_string())?,
            result: serde_json::from_value(result).map_err(|error| error.to_string())?,
            failure,
            calls: Mutex::default(),
            closes: AtomicUsize::new(0),
        })
    }
    fn snapshot(&self) -> Value {
        json!({"calls":lock(&self.calls).clone(),"closes":self.closes.load(Ordering::SeqCst)})
    }
}

impl SandboxExecutor for ScriptedSandbox {
    fn tool_definitions(&self) -> Result<Vec<Metadata>, ToolError> {
        Ok(self.definitions.clone())
    }
    fn execute<'a>(
        &'a self,
        command: &'a str,
        arguments: Option<&'a Metadata>,
    ) -> Pin<Box<dyn Future<Output = Result<SandboxResult, ToolError>> + Send + 'a>> {
        lock(&self.calls).push(json!({"command":command,"arguments":arguments}));
        Box::pin(ready(match &self.failure {
            Some(error) => Err(error.clone()),
            None => Ok(self.result.clone()),
        }))
    }
    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        Box::pin(ready(()))
    }
}

impl Sandboxes {
    pub(crate) fn install(
        &self,
        spec: &Value,
        options: &mut AiChannelOptions,
    ) -> Result<(), String> {
        let Some(given) = spec.get("sandbox") else {
            return Ok(());
        };
        let sandbox = Arc::new(ScriptedSandbox::new(given)?);
        options.sandbox = Some(Arc::<ScriptedSandbox>::clone(&sandbox));
        lock(&self.0).insert(spec["id"].as_str().unwrap_or_default().to_owned(), sandbox);
        Ok(())
    }
    pub(crate) fn snapshots(&self) -> BTreeMap<String, Value> {
        lock(&self.0)
            .iter()
            .map(|(id, sandbox)| (id.clone(), sandbox.snapshot()))
            .collect()
    }
}

type FieldCheck = (&'static str, fn(&Value) -> bool);

pub(crate) fn shape(spec: &Value, path: &str, found: &mut Vec<String>) {
    use crate::shape::members;
    members(spec, &["tools", "result", "raises"], path, found);
    if let Some(tools) = spec.get("tools") {
        if let Some(tools) = tools.as_array() {
            for (index, tool) in tools.iter().enumerate() {
                let at = format!("{path}.tools[{index}]");
                members(tool, &["name", "description", "parameters"], &at, found);
                if !tool.get("name").is_some_and(Value::is_string) {
                    found.push(format!("{at}.name: not a string"));
                }
                let checks: [FieldCheck; 2] = [
                    ("description", Value::is_string),
                    ("parameters", Value::is_object),
                ];
                for (field, valid) in checks {
                    if tool.get(field).is_some_and(|value| !valid(value)) {
                        found.push(format!("{at}.{field}: invalid type"));
                    }
                }
            }
        } else {
            found.push(format!("{path}.tools: not an array"));
        }
    }
    if let Some(result) = spec.get("result") {
        let at = format!("{path}.result");
        members(
            result,
            &["exit_code", "output", "error", "metadata"],
            &at,
            found,
        );
        if !result.get("exit_code").is_some_and(Value::is_i64) {
            found.push(format!("{at}.exit_code: not an integer"));
        }
        let checks: [FieldCheck; 3] = [
            ("output", Value::is_string),
            ("error", Value::is_string),
            ("metadata", Value::is_object),
        ];
        for (field, valid) in checks {
            if result.get(field).is_some_and(|value| !valid(value)) {
                found.push(format!("{at}.{field}: invalid type"));
            }
        }
    }
    if let Some(raises) = spec.get("raises") {
        let at = format!("{path}.raises");
        members(raises, &["kind", "message"], &at, found);
        for field in ["kind", "message"] {
            if !raises.get(field).is_some_and(Value::is_string) {
                found.push(format!("{at}.{field}: not a string"));
            }
        }
    }
}

#[cfg(test)]
#[path = "sandbox/tests.rs"]
mod tests;
