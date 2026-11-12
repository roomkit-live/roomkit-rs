//! ACP option descriptors and the values the agent reports: Python's
//! `_config_values` and `_session_setup`.

use agent_client_protocol_schema::v1::{SessionConfigOption, SessionConfigOptionValue};
use roomkit::model::Metadata;
use serde_json::Value;

use crate::error::AcpError;

pub(super) fn values(options: &[Value]) -> Metadata {
    options
        .iter()
        .filter_map(|option| {
            let id = option.get("id")?.as_str().filter(|id| !id.is_empty())?;
            let value = option
                .get("currentValue")
                .or_else(|| option.get("current_value"))?;
            (value.is_string() || value.is_boolean()).then(|| (id.to_owned(), value.clone()))
        })
        .collect()
}

pub(super) fn validate(config: Metadata) -> Result<Metadata, AcpError> {
    for (id, value) in &config {
        if id.is_empty() || !(value.is_string() || value.is_boolean()) {
            return Err(AcpError::Type(format!(
                "session_config maps an ACP config id to a string or a boolean, got {id:?}: {value}"
            )));
        }
    }
    Ok(config)
}

pub(super) fn option_value(value: &Value) -> Result<SessionConfigOptionValue, AcpError> {
    match value {
        Value::String(value) => Ok(SessionConfigOptionValue::value_id(value.clone())),
        Value::Bool(value) => Ok(SessionConfigOptionValue::boolean(*value)),
        Value::Null | Value::Number(_) | Value::Array(_) | Value::Object(_) => {
            Err(AcpError::InvalidRequest {
                method: "session/set_config_option".to_owned(),
                reason: "value must be a string or a boolean".to_owned(),
            })
        }
    }
}

pub(super) fn descriptors(options: Vec<SessionConfigOption>) -> Result<Vec<Value>, AcpError> {
    options
        .into_iter()
        .map(|option| {
            serde_json::to_value(option).map_err(|error| AcpError::InvalidResponse {
                method: "configOptions".to_owned(),
                reason: error.to_string(),
            })
        })
        .collect()
}

/// Python's repr in the setup refusal's explanation.
pub(super) fn display(value: &Value) -> String {
    match value {
        Value::String(value) => roomkit_core::rules::python_repr(value),
        Value::Bool(true) => "True".to_owned(),
        Value::Bool(false) => "False".to_owned(),
        other @ (Value::Null | Value::Number(_) | Value::Array(_) | Value::Object(_)) => {
            other.to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{validate, values};
    use serde_json::json;

    // Python: tests/test_channels/test_acp.py::TestACPChannel::test_config_values_reads_current_values
    #[test]
    fn test_config_values_reads_current_values() {
        let options = vec![
            json!({"id":"model","currentValue":"opus"}),
            json!({"id":"fast","current_value":true}),
            json!({"id":"","currentValue":"x"}),
            json!({"currentValue":"y"}),
            json!("junk"),
        ];
        assert_eq!(json!(values(&options)), json!({"model":"opus","fast":true}));
    }

    // Python: tests/test_channels/test_acp_session_setup.py::TestSessionConfig::test_values_acp_cannot_carry_are_refused
    #[test]
    fn test_values_acp_cannot_carry_are_refused() {
        for value in [json!({"effort":3}), json!({"":"x"}), json!({"mode":null})] {
            assert!(validate(value.as_object().cloned().unwrap_or_default()).is_err());
        }
    }
}
