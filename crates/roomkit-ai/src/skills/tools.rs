//! The skills as the agent's own toolset (step 4, decision 7):
//! `activate_skill` and `read_skill_reference`, which only read or unlock
//! and so escape the tool policy (RFC §21.1), and what each turn's system
//! prompt carries of the skills (RFC §6.4, §24.3, §24.4).

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit::model::{ChannelId, RoomEvent, RoomId, ToolCall, ToolCallOutcome};
use serde_json::{Value, json};

use super::activation::{self, Activations};
use super::files;
use super::registry::SkillRegistry;
use super::texts::{
    self, ACTIVATE_DESCRIPTION, ACTIVATE_SKILL, ACTIVE_HEADING, ALREADY_ACTIVE, PREAMBLE,
    READ_DESCRIPTION, READ_SKILL_REFERENCE, RUN_SKILL_SCRIPT, quoted_name,
};
use crate::tools::{AiTool, BoxFuture, DynToolset, ToolContext, ToolError, ToolResult};

/// The skills an agent serves: the registry, read only, and which skills
/// are active in each room, a record its turns share whichever registry each
/// reads (RFC §24.3, §24.4).
#[derive(Debug)]
pub(crate) struct SkillTools {
    registry: SkillRegistry,
    manifest: String,
    /// Whether the system prompt lists the skills (Python's
    /// `skills_in_prompt`).
    listed: bool,
    activations: Arc<Mutex<Activations>>,
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
            activations: Arc::default(),
        }
    }

    /// The tools a turn whose configuration gave `registry` reads in place of
    /// these (RFC §24.3), the record of each room's active skills the same.
    pub(crate) fn of_turn(&self, registry: SkillRegistry) -> Self {
        Self {
            manifest: registry.to_prompt_xml(),
            registry,
            listed: self.listed,
            activations: Arc::clone(&self.activations),
        }
    }

    /// The tools these skills declare: none for a registry with no skill
    /// (RFC §24.3).
    pub(crate) fn declared(&self) -> Vec<AiTool> {
        self.registry.tool_definitions()
    }

    /// The skills active in `room`, least recently used first.
    pub(crate) fn active_names(&self, room: &RoomId) -> Vec<String> {
        self.lock().active(room)
    }

    /// The names a channel given skills keeps for its own tools, as
    /// Python's does: the two it serves and `run_skill_script`, which
    /// Python serves with a script executor.
    pub(crate) const RESERVED_NAMES: [&str; 3] =
        [ACTIVATE_SKILL, READ_SKILL_REFERENCE, RUN_SKILL_SCRIPT];

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
    /// active in its room (RFC §24.4), each part after a blank line, whether a
    /// `base` stands before it or not. A room served for the first time in
    /// this process is read again first from its stored rows, whatever the
    /// turn reads; a registry with no skill adds nothing and leaves the room's
    /// record as it is, and a turn that stands alone carries no active skill.
    /// Python's `_hydrate_room_memories` and `_add_skills`.
    pub(crate) fn system_prompt(&self, base: Option<&str>, turn: SkillTurn<'_>) -> Option<String> {
        self.read_stored(turn);
        let mut prompt = base.map(str::to_owned);
        if !self.registry.has_entries() {
            return prompt;
        }
        if self.listed {
            let before = prompt.unwrap_or_default();
            prompt = Some(format!("{before}\n\n{PREAMBLE}\n\n{}", self.manifest));
        }
        if turn.stands_alone {
            return prompt;
        }
        match self.instructions(&self.active_in(turn.room)) {
            Some(active) => Some(format!("{}\n\n{active}", prompt.unwrap_or_default())),
            None => prompt,
        }
    }

    /// Reads `turn`'s room's stored rows, when this process has not yet.
    fn read_stored(&self, turn: SkillTurn<'_>) {
        let mut activations = self.lock();
        if activations.needs_hydration(turn.room) {
            let names = activation::activated_in(turn.history, turn.agent);
            activations.hydrate(turn.room, &names);
        }
    }

    /// The skills active in `room`. A skill this registry does not hold leaves
    /// them: its instructions are not in this prompt, so its next activation
    /// delivers them (RFC §24.3, §24.4).
    fn active_in(&self, room: &RoomId) -> Vec<String> {
        let mut activations = self.lock();
        activations.retain(room, |name| self.registry.get_skill(name).is_some());
        activations.active(room)
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
    /// time in the room, and always on a turn standing alone, whose prompt
    /// carries none; then an acknowledgement saying they are in force (RFC
    /// §24.4). The activation is recorded once the call is served
    /// ([`DynToolset::settled`]); a skill the registry does not hold is
    /// refused.
    async fn activate(
        &self,
        call: &ToolCall,
        context: &ToolContext,
    ) -> Result<ToolResult, ToolError> {
        let name = argument(call, "name");
        let Some(skill) = self.registry.get_skill(name) else {
            return not_a_skill(call, name, &self.registry.skill_names(), context);
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

    /// Records `name` as active in `room`, as a served activation does.
    #[cfg(test)]
    pub(crate) fn activated(&self, room: &RoomId, name: &str) {
        self.lock().activate(room, name);
    }

    /// Forgets the skills active in `room`: its next turn reads them again
    /// from its stored rows (RFC §10.6.3).
    pub(crate) fn forget(&self, room: &RoomId) {
        self.lock().forget(room);
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
        if call.name != ACTIVATE_SKILL || outcome != ToolCallOutcome::Served {
            return;
        }
        let name = argument(call, "name");
        if self.registry.get_skill(name).is_some() {
            self.lock().activate(&context.room_id, name);
        }
    }
}

/// The most tools a hint names; the call reveals them all. Python's
/// `tools_hint`.
const HINT_MAX_NAMES: usize = 8;

/// What an activation of `name`, no skill among `skills`, answers: its refusal,
/// or, when tools the turn may call match the name, the same words served with a
/// hint naming them, which the call reveals as `find_tools` reveals its matches
/// once it is served. A small model confuses a skill with a tool ("activate the
/// Spotify skill" when `spotify_play` is a tool), and the dead end becomes the
/// right outcome (RFC §24.4). Python's `tools_hint`.
fn not_a_skill(
    call: &ToolCall,
    name: &str,
    skills: &[&str],
    context: &ToolContext,
) -> Result<ToolResult, ToolError> {
    let mut answer = vec![
        (
            "error",
            json!(format!("Skill {} not found", quoted_name(name))),
        ),
        ("available_skills", json!(skills)),
    ];
    let wanted = name.to_lowercase();
    let mut matching: Vec<String> = context
        .hintable
        .iter()
        .filter(|tool| !wanted.is_empty() && tool.to_lowercase().contains(&wanted))
        .cloned()
        .collect();
    if matching.is_empty() {
        return Err(ToolError::Refused(texts::object(&answer)));
    }
    matching.sort();
    let named = matching
        .iter()
        .take(HINT_MAX_NAMES)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    let hint = format!(
        "{} is not a skill, but these TOOLS match and are now in your tool list — call one \
         directly instead: {named}.",
        quoted_name(name)
    );
    answer.push(("tools_hint", json!(hint)));
    if let Some(search) = &context.search {
        search.reveal_once_served(&call.id, matching);
    }
    Ok(ToolResult::Text(texts::object(&answer)))
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
