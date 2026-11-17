//! Fixture controls are separate from public skill validation failures.
use serde_json::Value;
pub(crate) fn validate(spec: &Value, path: &str, found: &mut Vec<String>) {
    let Some(servers) = spec["servers"].as_object() else {
        found.push(format!("{path}: expected servers"));
        return;
    };
    for server in servers.values() {
        validate_server(server, found);
    }
    let Some(operations) = spec["operations"].as_array().filter(|ops| !ops.is_empty()) else {
        found.push("expected nonempty registry operations".to_owned());
        return;
    };
    for op in operations {
        let (fields, required): (&[&str], &[&str]) = match op["method"].as_str() {
            Some("add_mcp_server") => (
                &["server", "label", "allow_dynamic", "execution_tools"],
                &["server", "label"],
            ),
            Some("get_skill" | "load_skill" | "is_served" | "origin" | "mark_unlisted") => {
                (&["name"], &["name"])
            }
            Some("read_file") => (&["name", "filename"], &["name", "filename"]),
            Some("copy") => (&["target", "names", "marks"], &["target"]),
            Some("mark_unavailable") => (&["name", "reason"], &["name", "reason"]),
            Some("snapshot") => (&[], &[]),
            Some("install_channel") => {
                if !op["channel"].is_object()
                    || op["channel"].get("model").is_none()
                    || op["channel"].get("skills").is_some()
                {
                    found.push("install requires a model and selected registry".to_owned());
                }
                (&["channel"], &[])
            }
            Some("room_call") => {
                if !op["action"].is_object() || op["action"]["call"] == "skill_registry_session" {
                    found.push("invalid nested registry action".to_owned());
                }
                (&["action"], &[])
            }
            _ => {
                found.push("unknown registry operation".to_owned());
                continue;
            }
        };
        let fields: Vec<_> = fields
            .iter()
            .copied()
            .chain(["method", "registry"])
            .collect();
        object(op, &fields, found);
        if required.iter().any(|key| !op[key].is_string())
            || op.get("registry").is_some_and(|v| !v.is_string())
        {
            found.push("registry operation requires strings".to_owned());
        }
        for key in ["marks", "allow_dynamic"] {
            if op.get(key).is_some_and(|v| !v.is_boolean()) {
                found.push(format!("{key}: expected boolean"));
            }
        }
        for key in ["names", "execution_tools"] {
            if op
                .get(key)
                .is_some_and(|v| !v.as_array().is_some_and(|v| v.iter().all(Value::is_string)))
            {
                found.push(format!("{key}: expected strings"));
            }
        }
    }
}
fn object(value: &Value, allowed: &[&str], found: &mut Vec<String>) {
    match value.as_object() {
        Some(map) => {
            for key in map.keys() {
                if !allowed.contains(&key.as_str()) {
                    found.push(format!("unknown registry fixture member {key}"));
                }
            }
        }
        None => found.push("registry fixture expected an object".to_owned()),
    }
}

fn validate_server(server: &Value, found: &mut Vec<String>) {
    object(server, &["extensions", "replies", "approvals"], found);
    if server.get("extensions").is_some_and(|extensions| {
        !extensions
            .as_object()
            .is_some_and(|m| m.values().all(Value::is_object))
    }) {
        found.push("invalid skill extensions".to_owned());
    }
    if server.get("approvals").is_some_and(|answers| {
        !answers.as_array().is_some_and(|answers| {
            answers.iter().all(|answer| {
                answer.is_boolean()
                    || answer
                        .as_object()
                        .is_some_and(|map| map.len() == 1 && answer["error"].is_string())
            })
        })
    }) {
        found.push("invalid skill approvals".to_owned());
    }
    let Some(replies) = server["replies"].as_array() else {
        found.push("invalid skill replies".to_owned());
        return;
    };
    for reply in replies {
        object(reply, &["method", "params", "result", "error"], found);
        if !reply["method"].is_string()
            || !reply["params"].is_object()
            || reply.get("result").is_some() == reply.get("error").is_some()
            || reply.get("error").is_some_and(|e| !e.is_string())
        {
            found.push("invalid skill reply".to_owned());
        }
    }
}
