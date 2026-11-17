//! A real host continuation callback with an exact invocation record.

use std::sync::Arc;

use roomkit_ai::{ContinuationPolicy, ProviderError, ProviderErrorOptions};
use serde_json::Value;

use crate::run::{Run, lock};

fn validate(spec: &Value) -> Result<(), String> {
    let object = spec.as_object().ok_or("continuation: expected an object")?;
    if object
        .keys()
        .any(|key| !["prefix", "nudge", "error"].contains(&key.as_str()))
    {
        return Err("continuation: unknown member".to_owned());
    }
    if !spec["prefix"].is_string() {
        return Err("continuation: prefix must be a string".to_owned());
    }
    if object.contains_key("nudge") == object.contains_key("error") {
        return Err("continuation: exactly one of nudge and error is required".to_owned());
    }
    if object
        .get("nudge")
        .is_some_and(|value| !value.is_null() && !value.is_string())
    {
        return Err("continuation: nudge must be a string or null".to_owned());
    }
    if object.get("error").is_some_and(|value| !value.is_string()) {
        return Err("continuation: error must be a string".to_owned());
    }
    Ok(())
}

pub(crate) fn check(channel: &Value, path: &str, found: &mut Vec<String>) {
    if let Some(spec) = channel.get("continuation")
        && let Err(error) = validate(spec)
    {
        found.push(format!("{path}.{error}"));
    }
}

pub(crate) fn policy(
    channel: &Value,
    run: &Arc<Run>,
) -> Result<Option<ContinuationPolicy>, String> {
    let Some(spec) = channel.get("continuation") else {
        return Ok(None);
    };
    validate(spec)?;
    let prefix = spec["prefix"]
        .as_str()
        .ok_or("continuation prefix")?
        .to_owned();
    let nudge = spec["nudge"].as_str().map(str::to_owned);
    let error = spec["error"].as_str().map(str::to_owned);
    let id = channel["id"]
        .as_str()
        .ok_or("continuation channel id")?
        .to_owned();
    lock(&run.observed)
        .continuations
        .entry(id.clone())
        .or_default();
    let run = Arc::clone(run);
    Ok(Some(Arc::new(move |text| {
        lock(&run.observed)
            .continuations
            .entry(id.clone())
            .or_default()
            .push(text.into());
        if !text.starts_with(&prefix) {
            return Ok(None);
        }
        if let Some(error) = &error {
            return Err(ProviderError::new(error, ProviderErrorOptions::default()));
        }
        Ok(nudge.clone())
    })))
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    // Python: tests/conformance/test_continuation_fixture.py.
    #[test]
    fn malformed_policies_are_fixture_errors_before_product_execution() {
        for policy in [
            Value::Null,
            json!([]),
            json!({}),
            json!({"prefix":"I"}),
            json!({"prefix":1,"nudge":"go"}),
            json!({"prefix":"I","nudge":false}),
            json!({"prefix":"I","error":null}),
            json!({"prefix":"I","nudge":null,"error":"failed"}),
            json!({"prefix":"I","nudge":"go","misspelled":true}),
        ] {
            let scenario =
                json!({"setup":{"channels":[{"id":"agent","type":"ai","continuation":policy}]}});
            assert!(
                crate::shape::unknown_members(&scenario)
                    .iter()
                    .any(|problem| problem.contains("continuation"))
            );
        }
    }

    #[test]
    fn an_explicit_absent_finish_reason_is_not_a_natural_stop() {
        for spec in [
            json!({"text":"answer","finish_reason":null}),
            json!({"text":"answer","calls":[{"id":"one","name":"lookup"}],"finish_reason":null}),
        ] {
            assert_eq!(
                crate::agents::round(&spec).unwrap().done.finish_reason,
                None
            );
        }
    }
}
