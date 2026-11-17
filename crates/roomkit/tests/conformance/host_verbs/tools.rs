//! Calls scripted from a real tool, including concurrent and withdrawn calls.
use crate::run::Run;
use roomkit_ai::{ToolError, ToolResult};
use serde_json::{Value, json};
use std::sync::Arc;

pub(crate) async fn call_from_tool(spec: &Value, run: &Arc<Run>) -> Result<ToolResult, ToolError> {
    let calls = spec["calls"]
        .as_array()
        .ok_or_else(|| ToolError::failed("host_calls has no calls"))?;
    let results = if spec["concurrent"] == true {
        concurrent(calls, run).await?
    } else {
        let mut results = Vec::new();
        for call in calls {
            results.push(invoke(call, run).await);
        }
        results
    };
    let results: Result<Vec<_>, _> = results.into_iter().collect();
    let results = results.map_err(ToolError::failed)?;
    let answer = json!({"calls": results, "record": super::record(&spec["record_keys"])});
    Ok(ToolResult::Text(canonical(&answer)))
}

async fn invoke(spec: &Value, run: &Run) -> Result<Value, String> {
    let called = super::invoke(spec);
    let result = if let Some(gate) = spec["cancel_after"].as_str() {
        tokio::select! {
            result = called => result?,
            reached = run.gates.wait_for(gate) => {
                if !reached { return Err(format!("call never reached {gate}")); }
                json!({"result": null, "error": {"kind": "cancelled", "message": ""}})
            }
        }
    } else {
        called.await?
    };
    if let Some(gate) = spec["returned"].as_str() {
        run.gates.reach(gate);
    }
    Ok(result)
}

/// Python's `json.dumps(sort_keys=True)`, including its default separators.
fn canonical(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let sorted: std::collections::BTreeMap<_, _> = map.iter().collect();
            let fields: Vec<_> = sorted
                .into_iter()
                .map(|(key, value)| format!("{}: {}", Value::String(key.clone()), canonical(value)))
                .collect();
            format!("{{{}}}", fields.join(", "))
        }
        Value::Array(values) => format!(
            "[{}]",
            values.iter().map(canonical).collect::<Vec<_>>().join(", ")
        ),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => value.to_string(),
    }
}

async fn concurrent(
    calls: &[Value],
    run: &Arc<Run>,
) -> Result<Vec<Result<Value, String>>, ToolError> {
    let mut running = tokio::task::JoinSet::new();
    for (index, call) in calls.iter().cloned().enumerate() {
        let run = Arc::clone(run);
        running.spawn(roomkit::keep_context(async move {
            (index, invoke(&call, &run).await)
        }));
    }
    let mut answers = std::collections::BTreeMap::new();
    while let Some(answer) = running.join_next().await {
        let (index, answer) = answer.map_err(|error| ToolError::failed(error.to_string()))?;
        answers.insert(index, answer);
    }
    Ok(answers.into_values().collect())
}
