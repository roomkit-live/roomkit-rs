//! The skills as the agent's own toolset (step 4, decision 7):
//! `activate_skill` and `read_skill_reference`, which only read or unlock
//! and so escape the tool policy (RFC §21.1), and what each turn's system
//! prompt carries of the skills (RFC §6.4, §24.3, §24.4).

use std::sync::{Mutex, MutexGuard, PoisonError};

use roomkit::model::{ChannelId, RoomEvent, RoomId, ToolCall, ToolCallOutcome};
use serde_json::{Value, json};

use super::activation::{self, Activations};
use super::files;
use super::registry::SkillRegistry;
use super::texts::{
    self, ACTIVATE_DESCRIPTION, ACTIVATE_SKILL, ACTIVE_HEADING, ALREADY_ACTIVE, PREAMBLE,
    READ_DESCRIPTION, READ_SKILL_REFERENCE, quoted_name,
};
use crate::tools::{AiTool, BoxFuture, DynToolset, ToolContext, ToolError, ToolResult};

/// The skills an agent serves: the registry, read only, and which skills
/// are active in each room.
#[derive(Debug)]
pub(crate) struct SkillTools {
    registry: SkillRegistry,
    manifest: String,
    /// Whether the system prompt lists the skills (Python's
    /// `skills_in_prompt`).
    listed: bool,
    activations: Mutex<Activations>,
}

/// What a turn's system prompt reads of its turn.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SkillTurn<'a> {
    /// The turn's room.
    pub(crate) room: &'a RoomId,
    /// The agent.
    pub(crate) agent: &'a ChannelId,
    /// The room's latest events, the agent's stored rows among them.
    pub(crate) history: &'a [RoomEvent],
    /// Whether the turn is an instruction that stands alone, which reads
    /// none of the room's working state (RFC §10.1.1 rule 7).
    pub(crate) stands_alone: bool,
}

impl SkillTools {
    /// The tools of `registry`, the system prompt listing its skills when
    /// `listed`.
    pub(crate) fn new(registry: SkillRegistry, listed: bool) -> Self {
        Self {
            manifest: registry.to_prompt_xml(),
            registry,
            listed,
            activations: Mutex::default(),
        }
    }

    /// The skills active in `room`, least recently used first.
    pub(crate) fn active_names(&self, room: &RoomId) -> Vec<String> {
        self.lock().active(room)
    }

    /// The two tools, as the reference implementation declares them.
    pub(crate) fn definitions() -> Vec<AiTool> {
        vec![
            AiTool::own(
                ACTIVATE_SKILL,
                ACTIVATE_DESCRIPTION,
                json!({
                    "type": "object",
                    "properties": {
                        "name": { "type": "string", "description": "Name of the skill to activate." },
                    },
                    "required": ["name"],
                }),
            ),
            AiTool::own(
                READ_SKILL_REFERENCE,
                READ_DESCRIPTION,
                json!({
                    "type": "object",
                    "properties": {
                        "skill_name": { "type": "string", "description": "Name of the skill." },
                        "filename": { "type": "string", "description": "Reference filename to read." },
                    },
                    "required": ["skill_name", "filename"],
                }),
            ),
        ]
    }

    /// The system prompt of `turn`: `base`, then the skills' preamble and
    /// manifest when they are listed, then the instructions of the skills
    /// active in its room
    /// (RFC §24.4), each part apart from the next by a blank line. A room
    /// served for the first time in this process is read again first from
    /// its stored rows; a turn that stands alone carries no active skill.
    pub(crate) fn system_prompt(&self, base: Option<&str>, turn: SkillTurn<'_>) -> String {
        let mut parts: Vec<&str> = base.into_iter().collect();
        if self.listed {
            parts.extend([PREAMBLE, self.manifest.as_str()]);
        }
        let active = if turn.stands_alone {
            None
        } else {
            self.instructions(&self.active_in(turn))
        };
        parts.extend(active.as_deref());
        parts.join("\n\n")
    }

    /// The skills active in `turn`'s room, its stored rows read first when
    /// this process has not read them yet.
    fn active_in(&self, turn: SkillTurn<'_>) -> Vec<String> {
        let mut activations = self.lock();
        if activations.needs_hydration(turn.room) {
            let names: Vec<String> = activation::activated_in(turn.history, turn.agent)
                .into_iter()
                .filter(|name| self.registry.get_skill(name).is_some())
                .collect();
            activations.hydrate(turn.room, &names);
        }
        activations.active(turn.room)
    }

    /// The instructions of the skills `active`, under their heading; none
    /// when no active skill has any.
    fn instructions(&self, active: &[String]) -> Option<String> {
        let sections: Vec<String> = active
            .iter()
            .filter_map(|name| self.registry.get_skill(name))
            .filter(|skill| !skill.instructions.trim().is_empty())
            .map(|skill| format!("## Skill: {}\n{}", skill.name(), skill.instructions.trim()))
            .collect();
        (!sections.is_empty()).then(|| format!("{ACTIVE_HEADING}\n\n{}", sections.join("\n\n")))
    }

    /// `activate_skill`: the skill's instructions and references the first
    /// time in the room, then an acknowledgement saying they are in force
    /// (RFC §24.4). The activation is recorded once the call is served
    /// ([`DynToolset::settled`]); a skill the registry does not hold is
    /// refused.
    async fn activate(
        &self,
        call: &ToolCall,
        context: &ToolContext,
    ) -> Result<ToolResult, ToolError> {
        let name = argument(call, "name");
        let Some(skill) = self.registry.get_skill(name) else {
            let names = self.registry.skill_names();
            return Err(ToolError::Refused(texts::object(&[
                (
                    "error",
                    json!(format!("Skill {} not found", quoted_name(name))),
                ),
                ("available_skills", json!(names)),
            ])));
        };
        let active = !context.stands_alone && self.lock().is_active(&context.room_id, name);
        let path = skill.path.clone();
        let references = blocking(move || files::list_references(&path)).await;
        let mut answer = if active {
            vec![
                ("ok", json!(true)),
                ("name", json!(skill.name())),
                ("_note", json!(ALREADY_ACTIVE)),
                ("already_active", json!(true)),
            ]
        } else {
            vec![
                ("name", json!(skill.name())),
                ("description", json!(skill.description())),
                ("instructions", json!(skill.instructions)),
            ]
        };
        if let Some(references) = references.filter(|names| !names.is_empty()) {
            answer.push(("references", json!(references)));
        }
        Ok(ToolResult::Text(texts::object(&answer)))
    }

    /// `read_skill_reference`: a reference of a skill the registry holds, or
    /// what the model reads of why it cannot be read (RFC §24.6).
    async fn read(&self, call: &ToolCall) -> Result<ToolResult, ToolError> {
        let (skill_name, filename) = (argument(call, "skill_name"), argument(call, "filename"));
        let Some(skill) = self.registry.get_skill(skill_name) else {
            let error = format!("Skill {} not found", quoted_name(skill_name));
            return Err(ToolError::Refused(texts::object(&[(
                "error",
                json!(error),
            )])));
        };
        let (path, name) = (skill.path.clone(), filename.to_owned());
        let read = blocking(move || files::read_reference(&path, &name))
            .await
            .unwrap_or_else(|| Err(format!("Reference cannot be read: {filename}")));
        let answer = match read {
            Ok(content) => {
                texts::object(&[("filename", json!(filename)), ("content", json!(content))])
            }
            Err(words) => texts::object(&[("error", json!(words))]),
        };
        Ok(ToolResult::Text(answer))
    }

    fn lock(&self) -> MutexGuard<'_, Activations> {
        self.activations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl DynToolset for SkillTools {
    fn call(
        &self,
        call: ToolCall,
        context: ToolContext,
    ) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
        Box::pin(async move {
            if call.name == ACTIVATE_SKILL {
                self.activate(&call, &context).await
            } else {
                self.read(&call).await
            }
        })
    }

    /// An activation served is recorded for its room: from the room's next
    /// turn, the skill's instructions ride the system prompt.
    fn settled(&self, call: &ToolCall, outcome: ToolCallOutcome, context: &ToolContext) {
        if call.name != ACTIVATE_SKILL || outcome != ToolCallOutcome::Served || context.stands_alone
        {
            return;
        }
        let name = argument(call, "name");
        if self.registry.get_skill(name).is_some() {
            self.lock().activate(&context.room_id, name);
        }
    }
}

/// The text argument `key` of `call`, empty when it gives none.
fn argument<'a>(call: &'a ToolCall, key: &str) -> &'a str {
    call.arguments
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
}

/// What `read`, a blocking read of the disk, returns, run off the turn's
/// task; `None` when it panicked.
async fn blocking<T: Send + 'static>(read: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    tokio::task::spawn_blocking(read).await.ok()
}
