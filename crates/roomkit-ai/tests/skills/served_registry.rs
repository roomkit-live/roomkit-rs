//! Public counterparts of Python's MCP registry/cache/manifest witnesses.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use roomkit::model::Metadata;
use roomkit_ai::{
    AddMcpServerOptions, CopyOptions, LoadedSkill, McpSkillServer, McpSkillServerError,
    McpSkillServerFuture, SKILLS_EXTENSION, SkillError, SkillRegistry,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Notify;

const NAME: &str = "docs:guide";
const URI: &str = "skill://guide/SKILL.md";
const BODY: &str = "---\nname: guide\ndescription: Read the guide\n---\nBody";

fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value.lock().unwrap()
}

fn entry() -> Value {
    let resource = |uri: &str, content: &str| {
        json!({"uri":uri,"size":content.len(),
        "digest":format!("sha256:{}",hex(&Sha256::digest(content.as_bytes())))})
    };
    json!({"uri":URI,"frontmatter":{"name":"guide","description":"Read the guide"},
        "resources":[resource(URI,BODY),resource("skill://guide/notes.txt","Notes")]})
}

#[derive(Default)]
struct Gate {
    started: Notify,
    release: Notify,
}

pub(super) struct Server {
    listing: Mutex<Vec<Value>>,
    current: Mutex<Value>,
    files: Mutex<BTreeMap<String, String>>,
    requests: Mutex<Vec<String>>,
    waiting: Option<(String, Arc<Gate>)>,
    read_gates: Mutex<BTreeMap<String, Arc<Gate>>>,
    read_errors: Mutex<BTreeMap<String, std::io::ErrorKind>>,
}
impl Server {
    pub(super) fn new() -> Self {
        Self {
            listing: Mutex::new(vec![entry()]),
            current: Mutex::new(entry()),
            files: Mutex::new(BTreeMap::from([
                (URI.to_owned(), BODY.to_owned()),
                ("skill://guide/notes.txt".to_owned(), "Notes".to_owned()),
            ])),
            requests: Mutex::default(),
            waiting: None,
            read_gates: Mutex::default(),
            read_errors: Mutex::default(),
        }
    }
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
        method: &'a str,
        _params: Option<&'a Metadata>,
    ) -> McpSkillServerFuture<'a, Metadata> {
        Box::pin(async move {
            lock(&self.requests).push(method.to_owned());
            if let Some((_, gate)) = self
                .waiting
                .as_ref()
                .filter(|(wait_for, _)| wait_for == method)
            {
                gate.started.notify_one();
                gate.release.notified().await;
            }
            let value = match method {
                "skills/list" => json!({"skills":*lock(&self.listing)}),
                "skills/get" => json!({"skill":*lock(&self.current)}),
                _ => return Err("unexpected test method".into()),
            };
            Ok(value.as_object().unwrap().clone())
        })
    }
    fn read_resource<'a>(&'a self, uri: &'a str) -> McpSkillServerFuture<'a, Value> {
        Box::pin(async move {
            lock(&self.requests).push(uri.to_owned());
            let gate = lock(&self.read_gates).get(uri).cloned();
            if let Some(gate) = gate {
                gate.started.notify_one();
                gate.release.notified().await;
            }
            if let Some(kind) = lock(&self.read_errors).get(uri).copied() {
                return Err(std::io::Error::new(kind, "remote read failed").into());
            }
            let text = lock(&self.files)
                .get(uri)
                .cloned()
                .ok_or("test file missing")?;
            Ok(json!({"contents":[{"uri":uri,"text":text}]}))
        })
    }
}

fn fill(registry: &SkillRegistry) {
    registry.mark_unavailable("padding", "").unwrap();
    let left = 65536 - registry.to_prompt_xml().chars().count();
    registry
        .mark_unavailable("padding", &"x".repeat(left))
        .unwrap();
}

#[tokio::test]
async fn owned_reads_do_not_alias_the_registry_but_copies_share_verified_files() {
    let server = Arc::new(Server::new());
    let registry = SkillRegistry::default();
    registry
        .add_mcp_server(
            Arc::<Server>::clone(&server),
            "docs",
            AddMcpServerOptions::default(),
        )
        .await
        .unwrap();
    let LoadedSkill::Served(mut first) = registry.load_skill(NAME).await.unwrap().unwrap() else {
        panic!("served snapshot required")
    };
    let mut metadata = registry.get_metadata(NAME).unwrap();
    metadata.description = "host changed its snapshot".to_owned();
    first.metadata.description = "changed".to_owned();
    first.instructions = "changed".to_owned();
    first.files.clear();
    let copied = registry.copy(None, CopyOptions::default()).unwrap();
    assert_eq!(copied.read_file(NAME, "notes.txt").await.unwrap(), "Notes");
    let LoadedSkill::Served(now) = registry.get_skill(NAME).unwrap().unwrap() else {
        panic!("served snapshot required")
    };
    assert_eq!(now.instructions, "Body");
    assert_eq!(now.description(), "Read the guide");
    assert_eq!(now.files.len(), 2);
    assert!(first.files.is_empty());
    assert_eq!(now.list_scripts(), Vec::<std::ffi::OsString>::new());
    assert!(!now.has_scripts());
    assert!(now.has_references().unwrap());
    assert!(now.read_reference("notes.txt").is_err());
    assert!(now.resolve_script("run.py").is_err());
    assert_eq!(registry, copied);
    let origin = registry.origin(NAME).unwrap();
    assert!(Arc::ptr_eq(&origin, &copied.origin(NAME).unwrap()));
    assert_eq!(
        lock(&server.requests)
            .iter()
            .filter(|r| *r == "skill://guide/notes.txt")
            .count(),
        1
    );
}

// Python: test_following_a_shared_refusal_fails_closed_without_partial_local_changes.
#[tokio::test]
async fn following_a_shared_refusal_never_returns_a_cached_body_past_the_manifest_bound() {
    let server = Arc::new(Server::new());
    let registry = SkillRegistry::default();
    registry
        .add_mcp_server(
            Arc::<Server>::clone(&server),
            "docs",
            AddMcpServerOptions::default(),
        )
        .await
        .unwrap();
    registry.load_skill(NAME).await.unwrap();
    let copied = registry.copy(None, CopyOptions::default()).unwrap();
    copied.mark_unlisted(NAME);
    fill(&copied);
    let before = copied.to_prompt_xml();
    let mut dynamic = entry();
    dynamic["resources"] = json!("dynamic");
    *lock(&server.listing) = vec![dynamic];
    registry
        .add_mcp_server(
            Arc::<Server>::clone(&server),
            "docs",
            AddMcpServerOptions::default(),
        )
        .await
        .unwrap();
    let reads = lock(&server.requests).clone();
    for result in [copied.get_skill(NAME), copied.load_skill(NAME).await] {
        assert!(matches!(result, Err(SkillError::ManifestTooLarge { .. })));
        assert_eq!(copied.to_prompt_xml(), before);
    }
    assert!(matches!(
        copied.read_file(NAME, "SKILL.md").await,
        Err(SkillError::ManifestTooLarge { .. })
    ));
    assert_eq!(*lock(&server.requests), reads);
    copied.mark_unavailable("padding", "short").unwrap();
    assert!(copied.load_skill(NAME).await.unwrap().is_none());
    assert!(copied.get_unavailable_reason(NAME).is_some());
}

// Python: test_remote_reads_check_current_state_after_io, both operations.
#[tokio::test]
async fn a_registry_can_change_during_network_io_and_current_state_is_checked_on_return() {
    for operation in ["skills/list", "skills/get"] {
        let gate = Arc::new(Gate::default());
        let mut server = Server::new();
        server.waiting = Some((operation.to_owned(), Arc::<Gate>::clone(&gate)));
        if operation == "skills/get" {
            lock(&server.listing).clear();
            lock(&server.current)["resources"] = json!("dynamic");
        }
        let server = Arc::new(server);
        let registry = Arc::new(SkillRegistry::default());
        if operation == "skills/get" {
            registry
                .add_mcp_server(
                    Arc::<Server>::clone(&server),
                    "docs",
                    AddMcpServerOptions::default(),
                )
                .await
                .unwrap();
        }
        let pending = {
            let registry = Arc::clone(&registry);
            let server = Arc::<Server>::clone(&server);
            tokio::spawn(async move {
                if operation == "skills/list" {
                    registry
                        .add_mcp_server(server, "docs", AddMcpServerOptions::default())
                        .await
                        .map(|_| ())
                } else {
                    registry.load_skill(NAME).await.map(|_| ())
                }
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(1), gate.started.notified())
            .await
            .unwrap();
        fill(&registry);
        let before = registry.to_prompt_xml();
        gate.release.notify_one();
        assert!(matches!(
            pending.await.unwrap(),
            Err(SkillError::ManifestTooLarge { .. })
        ));
        assert_eq!(registry.to_prompt_xml(), before);
        assert_eq!(registry.has_served(), operation == "skills/get");
        assert!(registry.get_metadata(NAME).is_none());
    }
}

// Python: test_rejected_relisting_keeps_old_entries_marks_and_shared_origin.
#[tokio::test]
async fn rejected_relisting_preserves_origin_identity_options_and_cached_content() {
    let registry = SkillRegistry::default();
    let changed = Arc::new(Server::new());
    registry
        .add_mcp_server(
            Arc::<Server>::clone(&changed),
            "docs",
            AddMcpServerOptions::default(),
        )
        .await
        .unwrap();
    let loaded = registry.load_skill(NAME).await.unwrap();
    fill(&registry);
    let before = registry.to_prompt_xml();
    let origin = registry.origin(NAME).unwrap();
    let mut another = entry();
    another["uri"] = json!("skill://extra/SKILL.md");
    another["resources"] = json!("dynamic");
    lock(&changed.listing).push(another);
    let result = registry
        .add_mcp_server(
            changed,
            "docs",
            AddMcpServerOptions {
                execution_tools: vec!["shell_*".to_owned()],
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(result, Err(SkillError::ManifestTooLarge { .. })));
    assert_eq!(registry.to_prompt_xml(), before);
    assert!(Arc::ptr_eq(&origin, &registry.origin(NAME).unwrap()));
    assert_eq!(registry.get_skill(NAME).unwrap(), loaded);
}

// Python: tests/skills/test_mcp_concurrent_reads.py, live accounting after I/O.
#[tokio::test]
async fn concurrent_dynamic_reads_charge_the_live_shared_byte_budget() {
    let server = Arc::new(Server::new());
    lock(&server.listing)[0]["resources"] = json!("dynamic");
    lock(&server.current)["resources"] = json!("dynamic");
    let first = Arc::new(Gate::default());
    let second = Arc::new(Gate::default());
    let size = 8 * 1024 * 1024;
    for (name, gate) in [
        ("first.txt", Arc::<Gate>::clone(&first)),
        ("second.txt", Arc::<Gate>::clone(&second)),
    ] {
        lock(&server.files).insert(format!("skill://guide/{name}"), "x".repeat(size));
        lock(&server.read_gates).insert(format!("skill://guide/{name}"), gate);
    }
    let registry = Arc::new(SkillRegistry::default());
    registry
        .add_mcp_server(
            server,
            "docs",
            AddMcpServerOptions {
                allow_dynamic: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    registry.load_skill(NAME).await.unwrap();
    let read_a = {
        let registry = Arc::clone(&registry);
        tokio::spawn(async move { registry.read_file(NAME, "first.txt").await })
    };
    let read_b = {
        let registry = Arc::clone(&registry);
        tokio::spawn(async move { registry.read_file(NAME, "second.txt").await })
    };
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        first.started.notified().await;
        second.started.notified().await;
    })
    .await
    .unwrap();
    first.release.notify_one();
    assert_eq!(read_a.await.unwrap().unwrap().len(), size);
    second.release.notify_one();
    assert!(matches!(
        read_b.await.unwrap(),
        Err(SkillError::Changed { .. })
    ));
    assert!(registry.get_skill(NAME).unwrap().is_none());
}

#[tokio::test]
async fn sanitized_load_and_changed_file_errors_keep_the_transport_cause() {
    let server = Arc::new(Server::new());
    let registry = SkillRegistry::default();
    registry
        .add_mcp_server(
            Arc::<Server>::clone(&server),
            "docs",
            AddMcpServerOptions::default(),
        )
        .await
        .unwrap();
    lock(&server.read_errors).insert(URI.to_owned(), std::io::ErrorKind::PermissionDenied);
    let error = registry.load_skill(NAME).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "Skill 'docs:guide' changed on its server or could not be verified"
    );
    assert_transport_cause(&error);
    lock(&server.read_errors).clear();
    registry.load_skill(NAME).await.unwrap().unwrap();
    lock(&server.read_errors).insert(
        "skill://guide/notes.txt".to_owned(),
        std::io::ErrorKind::PermissionDenied,
    );
    let error = registry.read_file(NAME, "notes.txt").await.unwrap_err();
    assert!(matches!(error, SkillError::Changed { .. }));
    assert_transport_cause(&error);
    assert!(registry.get_skill(NAME).unwrap().is_none());
}
fn assert_transport_cause(error: &SkillError) {
    let mut cause: &dyn std::error::Error = error;
    loop {
        if let Some(error) = cause.downcast_ref::<std::io::Error>() {
            assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
            return;
        }
        assert!(
            cause.source().is_some(),
            "transport cause remains in the chain: {cause:?}"
        );
        cause = cause.source().unwrap();
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut text, byte| {
        let _infallible = write!(text, "{byte:02x}");
        text
    })
}
