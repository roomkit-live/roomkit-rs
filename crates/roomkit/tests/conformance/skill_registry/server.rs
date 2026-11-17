//! Exact MCP request and host approval scripts, independent of SDK errors.
use crate::run::lock;
use roomkit::model::Metadata;
use roomkit_ai::{
    ApproveExecution, McpSkillServer, McpSkillServerFuture, SKILLS_EXTENSION, SkillExecutionRequest,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct State {
    position: usize,
    requests: Vec<Value>,
    approvals: Vec<Value>,
    problems: Vec<String>,
}
pub(super) struct Server {
    extensions: BTreeMap<String, Metadata>,
    replies: Vec<Value>,
    approvals: Option<Vec<Value>>,
    state: Mutex<State>,
}
impl Server {
    pub(super) fn requests(&self) -> Vec<Value> {
        lock(&self.state).requests.clone()
    }
    pub(super) fn approvals(&self) -> Vec<Value> {
        lock(&self.state).approvals.clone()
    }
    pub(super) fn new(spec: &Value) -> Self {
        Self {
            extensions: serde_json::from_value(
                spec.get("extensions")
                    .cloned()
                    .unwrap_or_else(|| json!({SKILLS_EXTENSION:{}})),
            )
            .unwrap(),
            replies: spec["replies"].as_array().unwrap().clone(),
            approvals: spec
                .get("approvals")
                .map(|value| value.as_array().unwrap().clone()),
            state: Mutex::default(),
        }
    }
    fn reply(
        &self,
        method: &str,
        params: &Value,
    ) -> Result<Value, roomkit_ai::McpSkillServerError> {
        let mut state = lock(&self.state);
        state
            .requests
            .push(json!({"method":method,"params":params}));
        let Some(reply) = self.replies.get(state.position) else {
            state
                .problems
                .push(format!("unexpected {method}: no scripted reply remains"));
            return Err("skill server fixture exhausted".into());
        };
        state.position += 1;
        if reply["method"] != method || reply["params"] != *params {
            state
                .problems
                .push(format!("wrong request {method} {params}"));
            return Err("skill server fixture request mismatch".into());
        }
        if let Some(error) = reply["error"].as_str() {
            return Err(error.to_owned().into());
        }
        Ok(reply["result"].clone())
    }
    fn approve(
        &self,
        request: &SkillExecutionRequest,
    ) -> Result<bool, roomkit_ai::McpSkillServerError> {
        let mut state = lock(&self.state);
        state.approvals.push(json!({"name":request.name,"label":request.label,"uri":request.uri,"fingerprint":request.fingerprint,"room_id":request.room_id,"tool":request.tool}));
        let position = state.approvals.len() - 1;
        let Some(answer) = self
            .approvals
            .as_ref()
            .and_then(|answers| answers.get(position))
        else {
            state.problems.push("approval fixture exhausted".to_owned());
            return Err("approval fixture exhausted".into());
        };
        if let Some(error) = answer["error"].as_str() {
            return Err(error.to_owned().into());
        }
        Ok(answer.as_bool().unwrap())
    }
    pub(super) fn callback(self: &Arc<Self>) -> Option<ApproveExecution> {
        self.approvals.as_ref()?;
        let server = Arc::clone(self);
        Some(ApproveExecution::new(move |request| {
            let server = Arc::clone(&server);
            async move { server.approve(&request) }
        }))
    }
    pub(super) fn finish(&self) -> Vec<String> {
        let state = lock(&self.state);
        let mut problems = state.problems.clone();
        if self
            .approvals
            .as_ref()
            .is_some_and(|answers| answers.len() != state.approvals.len())
        {
            problems.push("scripted approvals were not consumed".to_owned());
        }
        if state.position != self.replies.len() {
            problems.push("scripted replies were not read".to_owned());
        }
        problems
    }
}
impl McpSkillServer for Server {
    fn server_extensions(
        &self,
    ) -> Result<BTreeMap<String, Metadata>, roomkit_ai::McpSkillServerError> {
        Ok(self.extensions.clone())
    }
    fn request<'a>(
        &'a self,
        method: &'a str,
        params: Option<&'a Metadata>,
    ) -> McpSkillServerFuture<'a, Metadata> {
        Box::pin(async move {
            let value = self.reply(method, &json!(params))?;
            value
                .as_object()
                .cloned()
                .ok_or_else(|| "fixture request result is not an object".into())
        })
    }
    fn read_resource<'a>(&'a self, uri: &'a str) -> McpSkillServerFuture<'a, Value> {
        Box::pin(async move { self.reply("resources/read", &json!({"uri":uri})) })
    }
}
