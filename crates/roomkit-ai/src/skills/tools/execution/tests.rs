//! The approval cache binds exact origins, held files and conversation scopes.

use super::*;
use crate::{
    AddMcpServerOptions, ApproveExecution, McpSkillServer, McpSkillServerError,
    McpSkillServerFuture, SKILLS_EXTENSION, SkillRegistry,
};
use roomkit::model::{ChannelId, Metadata, RoomId, ToolCall, ToolCallOutcome};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const NAME: &str = "docs:guide";
const URI: &str = "skill://guide/SKILL.md";
const BODY: &str = "---\nname: guide\ndescription: Read the guide\n---\nBody";
struct Server {
    dynamic: bool,
}
impl McpSkillServer for Server {
    fn server_extensions(&self) -> Result<BTreeMap<String, Metadata>, McpSkillServerError> {
        Ok(BTreeMap::from([(
            SKILLS_EXTENSION.to_owned(),
            Metadata::new(),
        )]))
    }
    fn request<'a>(
        &'a self,
        _method: &'a str,
        _params: Option<&'a Metadata>,
    ) -> McpSkillServerFuture<'a, Metadata> {
        Box::pin(async move {
            let resources = if self.dynamic {
                json!("dynamic")
            } else {
                json!([{"uri":URI,"size":BODY.len(),"digest":format!("sha256:{}",hex(&Sha256::digest(BODY.as_bytes())))}])
            };
            Ok(json!({"skills":[{"uri":URI,"frontmatter":{"name":"guide","description":"Read the guide"},"resources":resources}]}).as_object().unwrap().clone())
        })
    }
    fn read_resource<'a>(&'a self, uri: &'a str) -> McpSkillServerFuture<'a, Value> {
        Box::pin(async move { Ok(json!({"contents":[{"uri":uri,"text":BODY}]})) })
    }
}

async fn registry(
    dynamic: bool,
    requests: &Arc<Mutex<Vec<SkillExecutionRequest>>>,
) -> SkillRegistry {
    let registry = SkillRegistry::default();
    let recorded = Arc::clone(requests);
    registry
        .add_mcp_server(
            Arc::new(Server { dynamic }),
            "docs",
            AddMcpServerOptions {
                allow_dynamic: dynamic,
                execution_tools: vec!["host_*".to_owned()],
                approve_execution: Some(ApproveExecution::new(move |request| {
                    lock(&recorded).push(request);
                    async { Ok(true) }
                })),
            },
        )
        .await
        .unwrap();
    registry.load_skill(NAME).await.unwrap().unwrap();
    registry
}
fn context(room: &str) -> ToolContext {
    ToolContext::new(RoomId::new(room).unwrap(), ChannelId::new("agent").unwrap())
}
fn active() -> HashSet<String> {
    HashSet::from([NAME.to_owned()])
}

#[tokio::test]
async fn approval_is_shared_between_turns_but_bound_to_room_and_exact_origin() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let tools = SkillTools::new(registry(false, &requests).await, true, None);
    for turn in [tools.for_turn(), tools.for_turn()] {
        assert!(
            turn.execution_refusal("host_run", &context("one"), &active())
                .await
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(lock(&requests).len(), 1);
    let replacement = tools.of_turn(registry(false, &requests).await);
    assert!(
        replacement
            .execution_refusal("host_run", &context("one"), &active())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(lock(&requests).len(), 2);
    assert!(
        tools
            .execution_refusal("host_run", &context("two"), &active())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(lock(&requests).len(), 3);
    tools.forget(&RoomId::new("one").unwrap());
    assert!(
        tools
            .execution_refusal("host_run", &context("one"), &active())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(lock(&requests).len(), 4);
    let request = lock(&requests).first().unwrap().clone();
    assert_eq!(request.name, NAME);
    assert_eq!(request.label, "docs");
    assert_eq!(request.uri, URI);
    assert_eq!(request.room_id.as_deref(), Some("one"));
    assert_eq!(request.tool, "host_run");
    assert!(request.fingerprint.is_some());
}

#[tokio::test]
async fn dynamic_entries_are_approved_every_time() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let tools = SkillTools::new(registry(true, &requests).await, true, None);
    for _ in 0..2 {
        assert!(
            tools
                .execution_refusal("host_run", &context("one"), &active())
                .await
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(lock(&requests).len(), 2);
    assert!(lock(&requests).first().unwrap().fingerprint.is_none());
}

#[tokio::test]
async fn a_standalone_turn_borrows_no_room_activation_and_unknown_names_do_not_execute() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let tools = SkillTools::new(registry(false, &requests).await, true, None);
    tools.activated(&RoomId::new("one").unwrap(), NAME);
    let mut standalone = context("one");
    standalone.stands_alone = true;
    assert!(
        tools
            .execution_refusal("host_run", &standalone, &HashSet::new())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        tools
            .execution_refusal("read", &context("one"), &active())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(lock(&requests).len(), 0);
    // A served name retained by a conversation after losing its loaded body
    // cannot be approved by accidentally skipping the unavailable skill.
    let missing = HashSet::from(["docs:unloaded".to_owned()]);
    assert!(
        tools
            .execution_refusal("host_run", &standalone, &missing)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn retirement_is_immediate_and_pending_activations_belong_to_one_turn() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let tools = SkillTools::new(registry(false, &requests).await, true, None);
    let first = tools.for_turn();
    let second = tools.for_turn();
    let call = ToolCall::new(
        "same-id",
        "activate_skill",
        json!({"name":NAME}).as_object().unwrap().clone(),
    );
    first.activate(&call, &context("one")).await.unwrap();
    assert!(
        second
            .record_activation(&call, ToolCallOutcome::Served, &context("one"))
            .is_none()
    );
    assert_eq!(
        second.active_names(&RoomId::new("one").unwrap()),
        Vec::<String>::new()
    );
    assert_eq!(
        first.record_activation(&call, ToolCallOutcome::Served, &context("one")),
        Some(NAME.to_owned())
    );
    first.retire(&RoomId::new("one").unwrap(), NAME);
    assert!(
        first
            .execution_refusal("host_run", &context("one"), &active())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(lock(&requests).len(), 0);
    assert_eq!(
        second.active_names(&RoomId::new("one").unwrap()),
        Vec::<String>::new()
    );
}

#[test]
fn least_recent_approvals_are_forgotten_and_the_origin_is_held() {
    let gate = ExecutionGate::default();
    let origin = Arc::new(ServedOrigin {
        label: "docs".to_owned(),
        server: Arc::new(Server { dynamic: false }),
        allow_dynamic: false,
        approve_execution: None,
        execution_tools: vec![],
    });
    for name in ["oldest", "newest"] {
        lock(&gate.0).push_back(Approval {
            room: "one".to_owned(),
            origin: Arc::clone(&origin),
            name: name.to_owned(),
            fingerprint: "held".to_owned(),
        });
    }
    assert!(gate.cached("one", &origin, "oldest", "held"));
    assert_eq!(lock(&gate.0).front().unwrap().name, "newest");
    assert_eq!(Arc::strong_count(&origin), 3);
    gate.forget("one");
    assert_eq!(Arc::strong_count(&origin), 1);
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut text, byte| {
        let _infallible = write!(text, "{byte:02x}");
        text
    })
}
