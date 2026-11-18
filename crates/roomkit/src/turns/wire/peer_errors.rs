//! Qualify Rust's native short error kinds only at a distributed boundary.
//! Python's error registry keys classes by `module:qualified_name`.

use serde_json::{Map, Value};

/// Prepare the JSON written by the SDK's serializer for the other process.
/// Only protocol error fields carry error records. Application JSON, even a
/// verb result that contains its own `error` member, stays intact.
pub(crate) fn peer_error_payload(raw: String) -> Result<String, String> {
    if !raw.contains("\"error\"") || !raw.contains("\"kind\"") {
        return Ok(raw);
    }
    let mut root: Value = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
    let message = if root.get("seq").is_some() {
        let Some(message) = root.get_mut("message") else {
            return Ok(raw);
        };
        message
    } else {
        &mut root
    };
    let error = match message.get("type").and_then(Value::as_str) {
        Some("output") => message.pointer_mut("/output/error"),
        Some("error" | "host_result" | "tool_result" | "channel_result") => {
            message.get_mut("error")
        }
        _ => None,
    };
    let Some(Value::Object(error)) = error else {
        return Ok(raw);
    };
    if !qualify(error) {
        return Ok(raw);
    }
    serde_json::to_string(&root).map_err(|e| e.to_string())
}

fn qualify(error: &mut Map<String, Value>) -> bool {
    let mut changed = false;
    if let Some(short) = error.get("kind").and_then(Value::as_str)
        && let Some((package, name)) = native_class(
            short,
            error
                .get("attributes")
                .and_then(|attrs| attrs.get("variant"))
                .and_then(Value::as_str),
        )
    {
        let bases = ancestors(package, name);
        let kind = format!("{package}:{name}");
        error.insert("kind".into(), Value::String(kind));
        if error
            .get("bases")
            .is_none_or(|values| values.as_array().is_some_and(Vec::is_empty))
        {
            error.insert("bases".into(), Value::from(bases));
        }
        changed = true;
    }
    if let Some(Value::Array(causes)) = error.get_mut("causes") {
        for cause in causes.iter_mut().filter_map(Value::as_object_mut) {
            changed |= qualify(cause);
        }
    }
    changed
}

const CORE: &str = "roomkit.core.exceptions";
const BUILTINS: &str = "builtins";

/// Map only known RoomKit and standard-library classes. An already-qualified
/// type or an unrecognized host error keeps the name supplied by its host.
pub(crate) fn native_class<'a>(
    short: &'a str,
    variant: Option<&str>,
) -> Option<(&'static str, &'a str)> {
    let name = if short == "RoomKitError" {
        match variant {
            Some("Type") => "TypeError",
            Some("Value") => "ValueError",
            Some("Runtime") => "RuntimeError",
            Some("Internal") => "AssertionError",
            Some("Cancelled") => "CancelledError",
            Some("Timeout") => "TimeoutError",
            Some("NoRecipient") => "NoRecipientError",
            Some("RoomAlreadyExists") => "RoomAlreadyExistsError",
            Some("RoomClosed") => "RoomClosedError",
            Some("TransitionRefused") => "TransitionRefusedError",
            Some("ProcessTimeout") => "ProcessTimeoutError",
            Some("ChangeRefused") => "ChangeRefusedError",
            Some("ReentrantCall") => "ReentrantCallError",
            Some("ReentryLoopCap") => "ReentryLoopCapError",
            Some("ChannelNotFound") => "ChannelNotFoundError",
            Some("ChannelAlreadyRegistered") => "ChannelAlreadyRegisteredError",
            Some("ParticipantNotFound") => "ParticipantNotFoundError",
            Some("IdentityNotFound") => "IdentityNotFoundError",
            Some("AddressLinked") => "AddressLinkedError",
            Some("EventNotFound") => "EventNotFoundError",
            Some("IdempotencyKeyTaken") => "IdempotencyKeyTakenError",
            Some("ProviderDelivery") => "ProviderDeliveryError",
            Some("HumanInputRejected") => "HumanInputRejectedError",
            Some("ToolNameCollision") => "ToolNameCollisionError",
            Some("VerificationRefused") => "VerificationRefusedError",
            Some("TurnLink") => "TurnLinkError",
            Some("RoomNotFound") => "RoomNotFoundError",
            Some("ChannelNotRegistered") => "ChannelNotRegisteredError",
            Some("KitClosed") => "KitClosedError",
            Some("ShutdownCancelled") => "ShutdownCancelledError",
            _ => short,
        }
    } else {
        short
    };
    let package = match name {
        "ValueError" | "TypeError" | "RuntimeError" | "PermissionError" | "AssertionError"
        | "TimeoutError" | "OSError" | "ConnectionError" | "KeyError" | "LookupError"
        | "FileNotFoundError" => BUILTINS,
        "CancelledError" => "asyncio.exceptions",
        "ProviderError" => "roomkit.providers.ai.base",
        "RoomKitError"
        | "ProviderDeliveryError"
        | "NoRecipientError"
        | "VerificationRefusedError"
        | "RoomNotFoundError"
        | "RoomAlreadyExistsError"
        | "EventNotFoundError"
        | "TaskNotFoundError"
        | "IdempotencyKeyTakenError"
        | "ChangeRefusedError"
        | "RoomClosedError"
        | "TransitionRefusedError"
        | "ProcessTimeoutError"
        | "KitClosedError"
        | "ShutdownCancelledError"
        | "ReentrantCallError"
        | "ReentryLoopCapError"
        | "ChannelNotFoundError"
        | "ChannelNotRegisteredError"
        | "ChannelAlreadyRegisteredError"
        | "ParticipantNotFoundError"
        | "ParticipantExistsError"
        | "IdentityNotFoundError"
        | "AddressLinkedError"
        | "ToolRefusedError"
        | "ToolFailedError"
        | "HumanInputRejectedError"
        | "ChannelRefusalError"
        | "UnservedToolCallError"
        | "ToolTimeoutError"
        | "ToolNameCollisionError"
        | "TurnCutShortError"
        | "TaskCutShortError"
        | "TaskTurnFailedError"
        | "RemoteTurnError"
        | "TurnLinkError" => CORE,
        _ => return None,
    };
    Some((package, name))
}

fn ancestors(package: &str, name: &str) -> Vec<&'static str> {
    match (package, name) {
        (
            BUILTINS,
            "PermissionError" | "TimeoutError" | "ConnectionError" | "FileNotFoundError",
        ) => vec!["builtins:OSError"],
        (BUILTINS, "KeyError") => vec!["builtins:LookupError"],
        (CORE, "RoomKitError") => Vec::new(),
        (CORE, "TaskCutShortError") => vec![
            "roomkit.core.exceptions:TurnCutShortError",
            "roomkit.core.exceptions:RoomKitError",
        ],
        (CORE, "NoRecipientError") => vec![
            "roomkit.core.exceptions:ProviderDeliveryError",
            "roomkit.core.exceptions:RoomKitError",
        ],
        (CORE, "ChannelRefusalError") => vec![
            "roomkit.core.exceptions:ToolRefusedError",
            "roomkit.core.exceptions:RoomKitError",
        ],
        (CORE, "HumanInputRejectedError") => vec![
            "roomkit.core.exceptions:RoomKitError",
            "builtins:RuntimeError",
        ],
        (CORE, "ToolNameCollisionError") => vec![
            "roomkit.core.exceptions:RoomKitError",
            "builtins:ValueError",
        ],
        (CORE, _) => vec!["roomkit.core.exceptions:RoomKitError"],
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::turns::error::{channel_error, kit_error, rebuild_kit_error};
    use crate::turns::wire::WireError;
    use crate::{ClassedError, RoomKitError, TaskTurnFailedError};
    use serde_json::json;
    use std::sync::Arc;

    fn record(kind: &str) -> Value {
        json!({"kind":kind,"message":"offline","args":["offline"],"attributes":{"source_id":1},"bases":[],"causes":[]})
    }

    #[test]
    fn only_protocol_errors_are_qualified_and_application_homonyms_survive() {
        let failure = TaskTurnFailedError::new(
            Arc::new(ClassedError::new(
                "company.gateway:RuntimeError",
                std::io::Error::other("offline"),
            )),
            "interrupted",
            None,
        );
        let error = channel_error(&failure);
        for mut raw in [
            json!({"turn_id":"t","seq":1,"message":{"type":"output","output":{"error":error,"response_metadata":{"error":record("TypeError")}}}}),
            json!({"turn_id":"t","seq":1,"message":{"type":"error","error":error}}),
            json!({"turn_id":"t","seq":1,"message":{"type":"tool_result","error":error}}),
            json!({"type":"host_result","error":error}),
            json!({"type":"channel_result","error":error}),
        ] {
            let rewritten = peer_error_payload(raw.to_string()).unwrap();
            let peer: Value = serde_json::from_str(&rewritten).unwrap();
            let path = if raw.pointer("/message/output/error").is_some() {
                "/message/output/error"
            } else if raw.pointer("/message/error").is_some() {
                "/message/error"
            } else {
                "/error"
            };
            let found = peer.pointer(path).unwrap();
            assert_eq!(found["kind"], "roomkit.core.exceptions:TaskTurnFailedError");
            assert_eq!(found["causes"][0]["kind"], "company.gateway:RuntimeError");
            assert_eq!(
                found["bases"],
                json!(["roomkit.core.exceptions:RoomKitError"])
            );
            *raw.pointer_mut(path).unwrap() = found.clone();
            assert_eq!(peer, raw);
            assert_eq!(peer_error_payload(rewritten.clone()).unwrap(), rewritten);
        }
        let meta = json!({"turn_id":"t","seq":1,"message":{"type":"record","record":{"error":record("RuntimeError")}}}).to_string();
        assert_eq!(peer_error_payload(meta.clone()).unwrap(), meta);
    }

    #[test]
    fn python_reads_the_actual_errors_issued_by_the_rust_sdk() {
        let wrapper = TaskTurnFailedError::new(
            Arc::new(crate::ToolTimeoutError::new("lookup", 0.5)),
            "interrupted",
            Some("working".to_owned()),
        );
        let native = [
            kit_error(&RoomKitError::Type("bad type".into())),
            kit_error(&RoomKitError::Value("bad value".into())),
            kit_error(&RoomKitError::Runtime("unavailable".into())),
            channel_error(&wrapper),
        ];
        let mut lines = String::new();
        for (index, error) in native.into_iter().enumerate() {
            let result = json!({"type":"host_result","call_id":index.to_string(),"result":null,"error":error,"record_changes":null});
            lines += &peer_error_payload(result.to_string()).unwrap();
            lines.push('\n');
        }
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/turns/rust-rpc-errors.jsonl");
        if std::env::var("ROOMKIT_WRITE_TURNS_FIXTURE").as_deref() == Ok("1") {
            std::fs::write(&path, &lines).unwrap();
        }
        assert_eq!(std::fs::read_to_string(path).unwrap(), lines);
    }

    #[test]
    fn python_and_rust_rebuild_errors_only_for_their_known_class() {
        for (kind, expected) in [
            (
                "builtins:RuntimeError",
                RoomKitError::Runtime("offline".into()),
            ),
            ("builtins:ValueError", RoomKitError::Value("offline".into())),
            ("builtins:TypeError", RoomKitError::Type("offline".into())),
        ] {
            let received: WireError = serde_json::from_value(record(kind)).unwrap();
            assert_eq!(rebuild_kit_error(&received), expected);
            let response =
                json!({"type":"host_result","call_id":"id","error":kit_error(&expected)})
                    .to_string();
            let formatted: Value =
                serde_json::from_str(&peer_error_payload(response).unwrap()).unwrap();
            assert_eq!(formatted["error"]["kind"], kind);
        }
        let foreign: WireError =
            serde_json::from_value(record("company.gateway:RuntimeError")).unwrap();
        let rebuilt = rebuild_kit_error(&foreign);
        assert!(
            matches!(&rebuilt,RoomKitError::RemoteTurn{kind,..} if kind=="company.gateway:RuntimeError")
        );
        assert_eq!(kit_error(&rebuilt).kind, foreign.kind);
    }
}
