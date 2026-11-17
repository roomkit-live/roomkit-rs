//! Host approval for execution while server-provided instructions are active.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use super::{SkillTools, ToolContext, quoted_name};
use crate::skills::registry::lock;
use crate::{LoadedSkill, ServedOrigin, SkillError, SkillExecutionRequest};

#[derive(Default)]
pub(super) struct ExecutionGate(Mutex<VecDeque<Approval>>);

struct Approval {
    room: String,
    origin: Arc<ServedOrigin>,
    name: String,
    fingerprint: String,
}

impl ExecutionGate {
    pub(super) fn forget(&self, room: &str) {
        lock(&self.0).retain(|entry| entry.room != room);
    }

    fn cached(
        &self,
        room: &str,
        origin: &Arc<ServedOrigin>,
        name: &str,
        fingerprint: &str,
    ) -> bool {
        let mut approved = lock(&self.0);
        let position = approved.iter().position(|entry| {
            entry.room == room
                && Arc::ptr_eq(&entry.origin, origin)
                && entry.name == name
                && entry.fingerprint == fingerprint
        });
        let Some(entry) = position.and_then(|position| approved.remove(position)) else {
            return false;
        };
        approved.push_back(entry);
        true
    }

    async fn approves(
        &self,
        skills: &SkillTools,
        name: &str,
        context: &ToolContext,
        tool: &str,
    ) -> Result<bool, SkillError> {
        let loaded = skills.registry.get_skill(name)?;
        let origin = skills.registry.origin(name);
        let (Some(LoadedSkill::Served(skill)), Some(origin)) = (loaded, origin) else {
            return Ok(false);
        };
        let held = skill.held()?;
        let fingerprint = held.fingerprint();
        let room = context.room_id.as_str();
        let cacheable = !room.is_empty() && fingerprint.is_some();
        if cacheable
            && fingerprint
                .as_deref()
                .is_some_and(|fingerprint| self.cached(room, &origin, name, fingerprint))
        {
            return Ok(true);
        }
        let Some(callback) = &origin.approve_execution else {
            return Ok(false);
        };
        let request = SkillExecutionRequest {
            name: name.to_owned(),
            label: origin.label.clone(),
            uri: held.uri().to_owned(),
            fingerprint: fingerprint.clone(),
            room_id: Some(room.to_owned()),
            tool: tool.to_owned(),
        };
        let approved = match callback.call(request).await {
            Ok(approved) => approved,
            Err(error) => {
                tracing::warn!(target: "roomkit::ai", skill = name, %error, "approve_execution failed; the call is refused");
                false
            }
        };
        if let (true, Some(fingerprint)) = (approved && cacheable, fingerprint) {
            self.remember(room, origin, name, fingerprint);
        }
        Ok(approved)
    }
    fn remember(&self, room: &str, origin: Arc<ServedOrigin>, name: &str, fingerprint: String) {
        let mut approvals = lock(&self.0);
        // Concurrent approvals of the same origin still occupy one cache entry.
        approvals.retain(|entry| {
            !(entry.room == room
                && Arc::ptr_eq(&entry.origin, &origin)
                && entry.name == name
                && entry.fingerprint == fingerprint)
        });
        approvals.push_back(Approval {
            room: room.to_owned(),
            origin,
            name: name.to_owned(),
            fingerprint,
        });
        while approvals.len() > 1000 {
            approvals.pop_front();
        }
    }
}

impl SkillTools {
    pub(crate) async fn execution_refusal(
        &self,
        tool: &str,
        context: &ToolContext,
        local: &HashSet<String>,
        sandbox: bool,
    ) -> Result<Option<String>, SkillError> {
        if !self.registry.has_served() {
            return Ok(None);
        }
        let executes = sandbox
            || tool == super::RUN_SKILL_SCRIPT
            || self
                .registry
                .execution_tool_patterns()
                .iter()
                .any(|pattern| crate::tools::glob_matches(pattern, tool));
        if !executes {
            return Ok(None);
        }
        let mut active = local.clone();
        if !context.stands_alone {
            active.extend(self.active_names(&context.room_id));
        }
        let mut served: Vec<_> = active
            .into_iter()
            .filter(|name| self.registry.is_served(name) && !self.is_retired(name))
            .collect();
        served.sort();
        for name in served {
            if !self.execution.approves(self, &name, context, tool).await? {
                return Ok(Some(format!(
                    "Tool {} runs code, and skill {}, served by an MCP server, is active: the host has not approved code run on that skill's behalf.",
                    quoted_name(tool),
                    quoted_name(&name)
                )));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests;
