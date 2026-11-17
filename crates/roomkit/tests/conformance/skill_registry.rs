//! Public served-registry operations against exact scripted MCP server replies.

use crate::calls::Unreadable;
use roomkit_ai::{
    AddMcpServerOptions, CopyOptions, LoadedSkill, McpSkillServer, SkillError, SkillFileError,
    SkillMetadata, SkillRegistry, SkillToolNames,
};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[path = "skill_registry/server.rs"]
mod server;
#[path = "skill_registry/shape.rs"]
mod shape;
use server::Server;
pub(crate) use shape::validate;

pub(crate) async fn session(run: &Arc<crate::run::Run>, spec: &Value) -> Result<Value, Unreadable> {
    let mut problems = Vec::new();
    validate(spec, "skill_registry_session", &mut problems);
    if !problems.is_empty() {
        return Err(problems.join("; "));
    }
    let servers: HashMap<String, Arc<Server>> = spec["servers"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(name, spec)| (name.clone(), Arc::new(Server::new(spec))))
        .collect();
    let mut registries = HashMap::from([("main".to_owned(), Arc::new(SkillRegistry::default()))]);
    let mut results = Vec::new();
    for op in spec["operations"].as_array().unwrap() {
        let registry = registries
            .get(op["registry"].as_str().unwrap_or("main"))
            .cloned()
            .ok_or("unknown skill registry")?;
        if op["method"] == "install_channel" {
            let installed = crate::calls::register_channel_with_skills(
                run,
                &op["channel"],
                Some(registry.as_ref().clone()),
            )?;
            if let Err(error) = installed {
                return Err(format!("install_channel failed: {error:?}"));
            }
            results.push(ok(Value::Null));
        } else if op["method"] == "room_call" {
            let (result, _) = Box::pin(crate::calls::checked_call(run, &op["action"])).await?;
            run.idle().await;
            let outcome = match result {
                Ok(value) => json!({"result":value}),
                Err(error) => json!({"error":error}),
            };
            results.push(ok(outcome));
        } else if op["method"] == "copy" {
            let target = op["target"].as_str().unwrap();
            if registries.contains_key(target) {
                return Err("duplicate skill registry".to_owned());
            }
            let names = op
                .get("names")
                .map(|v| serde_json::from_value::<Vec<String>>(v.clone()).unwrap());
            match registry.copy(
                names.as_deref(),
                CopyOptions {
                    marks: op["marks"].as_bool().unwrap_or(true),
                },
            ) {
                Ok(copy) => {
                    registries.insert(target.to_owned(), Arc::new(copy));
                    results.push(ok(Value::Null));
                }
                Err(error) => results.push(failed(&error)),
            }
        } else {
            results.push(observe(&registry, &servers, op).await?);
        }
    }
    let problems: Vec<_> = servers
        .values()
        .flat_map(|server| server.finish())
        .collect();
    if !problems.is_empty() {
        return Err(problems.join("; "));
    }
    let requests: serde_json::Map<String, Value> = servers
        .iter()
        .map(|(name, server)| (name.clone(), json!(server.requests())))
        .collect();
    let approvals: serde_json::Map<String, Value> = servers
        .iter()
        .map(|(name, server)| (name.clone(), json!(server.approvals())))
        .collect();
    Ok(json!({"results":results,"requests":requests,"approvals":approvals}))
}
async fn observe(
    registry: &SkillRegistry,
    servers: &HashMap<String, Arc<Server>>,
    op: &Value,
) -> Result<Value, Unreadable> {
    let name = op["name"].as_str().unwrap_or_default();
    let result: Result<Value, SkillError> = match op["method"].as_str().unwrap() {
        "add_mcp_server" => {
            let server = servers.get(op["server"].as_str().unwrap()).cloned().ok_or("unknown skill server")?;
            let options = AddMcpServerOptions {
                approve_execution: server.callback(),
                allow_dynamic: op["allow_dynamic"].as_bool().unwrap_or(false),
                execution_tools: serde_json::from_value(op.get("execution_tools").cloned().unwrap_or_else(|| json!([]))).unwrap(),
            };
            registry.add_mcp_server(server, op["label"].as_str().unwrap(), options).await.map(|count| json!(count))
        }
        "get_skill" => registry.get_skill(name).map(skill),
        "load_skill" => registry.load_skill(name).await.map(skill),
        "read_file" => registry.read_file(name, op["filename"].as_str().unwrap()).await.map(Value::String),
        "is_served" => Ok(json!(registry.is_served(name))),
        "origin" => Ok(registry.origin(name).map_or(Value::Null, |origin| {
            let server = servers.iter().find(|(_, server)| {
                let erased: Arc<dyn McpSkillServer> = Arc::<Server>::clone(server);
                Arc::ptr_eq(&origin.server, &erased)
            }).map(|(name, _)| name);
            json!({"label":origin.label,"server":server,"allow_dynamic":origin.allow_dynamic,"approve_execution":origin.approve_execution.is_some(),"execution_tools":origin.execution_tools})
        })),
        "mark_unavailable" => registry.mark_unavailable(name, op["reason"].as_str().unwrap()).map(|()| Value::Null),
        "mark_unlisted" => { registry.mark_unlisted(name); Ok(Value::Null) }
        "snapshot" => Ok(snapshot(registry)),
        _ => return Err("unknown registry operation".to_owned()),
    };
    Ok(match result {
        Ok(value) => ok(value),
        Err(error) => failed(&error),
    })
}
fn ok(value: Value) -> Value {
    Value::Object(serde_json::Map::from_iter([
        ("status".to_owned(), json!("ok")),
        ("result".to_owned(), value),
    ]))
}
fn failed(error: &SkillError) -> Value {
    let category = match error {
        SkillError::EntryValidation { .. } | SkillError::Validation { .. } => {
            "SkillValidationError"
        }
        SkillError::ServerDiscovery { .. }
        | SkillError::ServerUnavailable { .. }
        | SkillError::Discovery { .. } => "SkillDiscoveryError",
        SkillError::Parse { .. } => "SkillParseError",
        SkillError::ManifestTooLarge { .. } => "SkillManifestTooLargeError",
        SkillError::Verification { .. } | SkillError::VerificationCaused { .. } => {
            "SkillVerificationError"
        }
        SkillError::Changed { .. } => "SkillChangedError",
        SkillError::NotFound { .. } => "KeyError",
        SkillError::File(source) => match source.as_ref() {
            SkillFileError::Path(_) => "SkillPathError",
            SkillFileError::Io(_) => "OSError",
            SkillFileError::Encoding(_) => "UnicodeDecodeError",
            _ => "unexpected",
        },
        _ => "unexpected",
    };
    let message = if let SkillError::NotFound { name } = error {
        name.clone()
    } else {
        error.to_string()
    };
    json!({"status":"error","error":category,"message":message})
}
fn metadata(meta: &SkillMetadata) -> Value {
    let allowed = match &meta.allowed_tools {
        Some(SkillToolNames::Text(s)) => json!(s),
        Some(SkillToolNames::List(v)) => json!(v),
        None => Value::Null,
    };
    json!({"name":meta.name,"description":meta.description,"license":meta.license,"compatibility":meta.compatibility,"allowed_tools":allowed,"extra_metadata":meta.extra_metadata})
}
fn skill(skill: Option<LoadedSkill>) -> Value {
    let Some(skill) = skill else {
        return Value::Null;
    };
    let mut value = json!({"metadata":metadata(skill.metadata()),"instructions":skill.instructions(),"has_scripts":skill.has_scripts().unwrap(),"references":skill.list_references().unwrap().into_iter().map(|name|name.to_string_lossy().into_owned()).collect::<Vec<_>>()});
    if let LoadedSkill::Served(skill) = skill {
        let files: serde_json::Map<String, Value> = skill
            .files
            .iter()
            .map(|(uri, bytes)| (uri.clone(), json!(hex(bytes))))
            .collect();
        value.as_object_mut().unwrap().extend(json!({"origin":skill.origin,"uri":skill.held().unwrap().uri(),"read_total":skill.read_total,"files":files,"retirements":skill.retirements}).as_object().unwrap().clone());
    }
    value
}
fn snapshot(registry: &SkillRegistry) -> Value {
    let sorted = |items: HashSet<String>| {
        let mut items: Vec<_> = items.into_iter().collect();
        items.sort();
        items
    };
    json!({"metadata":registry.all_metadata().iter().map(metadata).collect::<Vec<_>>(),"skill_names":registry.skill_names(),"listed_names":registry.listed_names(),"skill_count":registry.skill_count(),"has_entries":registry.has_entries(),"has_served":registry.has_served(),"unavailable_skills":registry.unavailable_skills(),"execution_tool_patterns":sorted(registry.execution_tool_patterns()),"gated_tool_names":sorted(registry.gated_tool_names(&HashSet::new())),"manifest":registry.to_prompt_xml()})
}

#[cfg(test)]
#[path = "skill_registry/tests.rs"]
mod tests;

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut text, byte| {
        let _infallible = write!(text, "{byte:02x}");
        text
    })
}
