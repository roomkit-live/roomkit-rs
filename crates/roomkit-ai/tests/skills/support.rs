//! Skill directories written in a temporary directory, and a room whose
//! agent serves them on a scripted model.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{env, fs, process};

use roomkit::RoomKit;
use roomkit::model::{
    ChannelId, ChannelType, EventContent, EventType, InboundMessage, RoomEvent, RoomId,
    ToolCallContent,
};
use roomkit::testing::RecordingTransport;
use roomkit::{ConversationStore, InboundResult};
use roomkit_ai::testing::ScriptedProvider;
use roomkit_ai::{AiAgent, SkillRegistry};

/// A directory removed with what it holds when dropped.
pub(crate) struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub(crate) fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let name = format!(
            "roomkit-skills-{}-{}",
            process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        );
        let path = env::temp_dir().join(name);
        let _absent = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self {
            path: path.canonicalize().unwrap(),
        }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// The directory `relative` inside it, created.
    pub(crate) fn dir(&self, relative: &str) -> PathBuf {
        let path = self.path.join(relative);
        fs::create_dir_all(&path).unwrap();
        path
    }

    /// The file `relative` inside it, written with `text`.
    pub(crate) fn file(&self, relative: &str, text: &str) -> PathBuf {
        let path = self.path.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, text).unwrap();
        path
    }

    /// The skill `name` in the directory `parent`, its `SKILL.md` `text`.
    pub(crate) fn skill_with(&self, parent: &str, name: &str, text: &str) -> PathBuf {
        self.file(&format!("{parent}/{name}/SKILL.md"), text);
        self.path.join(parent).join(name)
    }

    /// The skill `name` in `parent`, described as `description`.
    pub(crate) fn skill(&self, parent: &str, name: &str, description: &str) -> PathBuf {
        self.skill_with(parent, name, &skill_md(name, description, ""))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _gone = fs::remove_dir_all(&self.path);
    }
}

/// A `SKILL.md` of `name`, `description` and the frontmatter lines
/// `extra`, its body following its name's rules.
pub(crate) fn skill_md(name: &str, description: &str, extra: &str) -> String {
    format!(
        "---\nname: {name}\ndescription: {description}\n{extra}---\n## Instructions\nFollow the {name} rules.\n"
    )
}

/// A link at `link` to `target`.
#[cfg(unix)]
pub(crate) fn link(target: &Path, link: &Path) {
    std::os::unix::fs::symlink(target, link).unwrap();
}

pub(crate) fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

pub(crate) fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// A room where Alice writes by SMS to an agent serving skills.
pub(crate) struct Room {
    pub(crate) kit: RoomKit,
}

impl Room {
    /// The room, its agent `adjust`ed from one on `model` serving `skills`
    /// after the system prompt `Base.`.
    pub(crate) async fn new(
        model: ScriptedProvider,
        skills: SkillRegistry,
        adjust: impl FnOnce(AiAgent<ScriptedProvider>) -> AiAgent<ScriptedProvider>,
    ) -> Self {
        let kit = RoomKit::new();
        let sms = RecordingTransport::new(channel("sms"), ChannelType::SMS);
        kit.register_channel(sms).unwrap();
        kit.register_channel(agent(model, skills, adjust)).unwrap();
        kit.create_room(&room_id()).await.unwrap();
        for id in ["sms", "assistant"] {
            kit.attach(&room_id(), &channel(id)).await.unwrap();
        }
        Self { kit }
    }

    pub(crate) async fn says(&self, text: &str) -> InboundResult {
        let message = InboundMessage::new(channel("sms"), "+15550001", EventContent::text(text));
        self.kit
            .process_inbound_in(&room_id(), message)
            .await
            .unwrap()
    }

    /// The agent replaced by a new one on `model`, serving `skills`: what
    /// it kept in memory is gone, as after a restart.
    pub(crate) fn replace_agent(&self, model: ScriptedProvider, skills: SkillRegistry) {
        self.kit.unregister_channel(&channel("assistant")).unwrap();
        self.kit
            .register_channel(agent(model, skills, |agent| agent))
            .unwrap();
    }

    /// The end rows of the agent's calls, in the room's order.
    pub(crate) async fn ends(&self) -> Vec<ToolCallContent> {
        let timeline: Vec<RoomEvent> = self
            .kit
            .store()
            .list_events(&room_id(), &every_row())
            .await
            .unwrap();
        timeline
            .into_iter()
            .filter(|event| event.event_type == EventType::ToolCallEnd)
            .filter_map(|event| {
                let EventContent::ToolCall(row) = event.content else {
                    return None;
                };
                Some(row)
            })
            .collect()
    }
}

fn agent(
    model: ScriptedProvider,
    skills: SkillRegistry,
    adjust: impl FnOnce(AiAgent<ScriptedProvider>) -> AiAgent<ScriptedProvider>,
) -> AiAgent<ScriptedProvider> {
    adjust(
        AiAgent::new(channel("assistant"), model)
            .with_system_prompt("Base.")
            .with_skills(skills)
            .unwrap(),
    )
}

/// What each request of `model` carried as tool results, by request.
pub(crate) fn tool_results(model: &ScriptedProvider) -> Vec<Vec<String>> {
    model
        .requests()
        .iter()
        .map(|request| {
            request
                .messages
                .iter()
                .filter(|message| message.tool_call_id.is_some())
                .map(|message| message.text.clone())
                .collect()
        })
        .collect()
}

/// The system prompt of each request of `model`.
pub(crate) fn prompts(model: &ScriptedProvider) -> Vec<String> {
    model
        .requests()
        .iter()
        .map(|request| request.system_prompt.clone().unwrap_or_default())
        .collect()
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> roomkit::EventQuery {
    roomkit::EventQuery {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::EventQuery::default()
    }
}
