//! Public registry transactions over shared MCP entries (RFC §24.7).

use super::catalog::{self, Listed};
use super::{AddMcpServerOptions, McpSkillServer, SKILLS_EXTENSION, ServedOrigin, wire};
use crate::skills::registry::lock;
use crate::skills::texts::quoted_name;
use crate::{SkillError, SkillRegistry};
use std::collections::HashSet;
use std::sync::Arc;

impl SkillRegistry {
    /// Register a connected server's listing, without reading any skill file.
    ///
    /// # Errors
    /// Invalid labels, a label held by another server, absent extension, transport
    /// errors and an oversized prospective manifest leave all registries unchanged.
    pub async fn add_mcp_server(
        &self,
        server: Arc<dyn McpSkillServer>,
        label: &str,
        options: AddMcpServerOptions,
    ) -> Result<usize, SkillError> {
        self.check_server(&server, label)?;
        let extensions = server.server_extensions().map_err(|source| {
            wire::discovery(
                format!(
                    "MCP server {} could not declare extensions",
                    quoted_name(label)
                ),
                source,
            )
        })?;
        if !extensions.contains_key(SKILLS_EXTENSION) {
            return Err(SkillError::ServerUnavailable {
                message: format!(
                    "MCP server {} does not declare the skills extension",
                    quoted_name(label)
                ),
            });
        }
        let origin = Arc::new(ServedOrigin {
            label: label.to_owned(),
            server,
            allow_dynamic: options.allow_dynamic,
            approve_execution: options.approve_execution,
            execution_tools: options.execution_tools,
        });
        let entries = wire::list(origin.server.as_ref(), label).await?;
        let listed: Vec<_> = entries
            .iter()
            .enumerate()
            .map(|(index, entry)| catalog::listed(&origin, index, entry))
            .collect();
        self.commit_listing(origin, &listed)
    }

    fn check_server(
        &self,
        server: &Arc<dyn McpSkillServer>,
        label: &str,
    ) -> Result<(), SkillError> {
        let reason = if !catalog::valid_label(label) {
            Some(format!("Label {} is not an identifier", quoted_name(label)))
        } else if lock(&self.catalog)
            .origins
            .get(label)
            .is_some_and(|origin| !Arc::ptr_eq(&origin.server, server))
        {
            Some(format!(
                "Label {} already names another MCP server",
                quoted_name(label)
            ))
        } else {
            None
        };
        match reason {
            Some(message) => Err(SkillError::EntryValidation { message }),
            None => Ok(()),
        }
    }
    fn commit_listing(
        &self,
        origin: Arc<ServedOrigin>,
        listed: &[Listed],
    ) -> Result<usize, SkillError> {
        let mut state = lock(&self.state);
        let mut catalog = lock(&self.catalog);
        let mut next = state.clone();
        let offered: Vec<_> = listed
            .iter()
            .filter(|item| item.metadata.is_some())
            .map(|item| item.name.clone())
            .collect();
        next.remove_served(&catalog.names_of(&origin.label), &offered);
        for item in listed {
            next.record_served(item);
        }
        next.marks.check_manifest(&next.skills)?;
        next.owned_labels.insert(origin.label.clone());
        catalog.record(origin, listed);
        *state = next;
        Ok(offered.len())
    }

    /// Whether a registered server serves, or could serve, this qualified name.
    pub fn is_served(&self, name: &str) -> bool {
        lock(&self.catalog).label_of(name).is_some()
    }
    /// The shared immutable origin, including for an unlisted qualified name.
    pub fn origin(&self, name: &str) -> Option<Arc<ServedOrigin>> {
        let catalog = lock(&self.catalog);
        catalog
            .label_of(name)
            .and_then(|label| catalog.origins.get(label))
            .cloned()
    }
    /// Whether this registry shares any registered server, even an empty listing.
    pub fn has_served(&self) -> bool {
        lock(&self.catalog).has_servers()
    }
    /// The execution patterns declared by hosts of every shared server.
    pub fn execution_tool_patterns(&self) -> HashSet<String> {
        lock(&self.catalog)
            .origins
            .values()
            .flat_map(|origin| origin.execution_tools.iter().cloned())
            .collect()
    }

    pub(crate) fn follow_catalog(&self, name: &str) -> Result<(), SkillError> {
        let mut state = lock(&self.state);
        if !state.skills.iter().any(|skill| skill.name() == name) {
            return Ok(());
        }
        let catalog = lock(&self.catalog);
        if !catalog.entries.contains_key(name) {
            state.skills.retain(|skill| skill.name() != name);
            state.loaded.remove(name);
            state.marks.unlisted.remove(name);
            return Ok(());
        }
        if let Some(reason) = catalog.refusal(name) {
            let mut next = state.clone();
            next.record_served(&Listed {
                name: name.to_owned(),
                entry: None,
                metadata: None,
                reason: Some(reason),
            });
            next.marks.check_manifest(&next.skills)?;
            *state = next;
            return Ok(());
        }
        let retired = state.loaded.get(name).is_some_and(|skill| {
            lock(skill).retirements < catalog.retirements.get(name).copied().unwrap_or_default()
        });
        if retired {
            state.loaded.remove(name);
        }
        Ok(())
    }

    pub(super) async fn fetch_unlisted(&self, name: &str) -> Result<bool, SkillError> {
        let origin = self.origin(name).ok_or_else(|| not_found(name))?;
        {
            let state = lock(&self.state);
            if !state.owned_labels.contains(&origin.label) || state.withheld.contains(name) {
                return Err(not_found(name));
            }
        }
        let (_, path) = name.split_once(':').ok_or_else(|| not_found(name))?;
        if !catalog::valid_path(path) {
            return Err(not_found(name));
        }
        let uri = if path.contains("://") {
            format!("{path}/SKILL.md")
        } else {
            format!("skill://{path}/SKILL.md")
        };
        let entry = wire::get(origin.server.as_ref(), name, &uri).await?;
        if catalog::served_name(&origin.label, &entry).as_deref() != Some(name) {
            return Err(not_found(name));
        }
        let item = catalog::from_entry(&origin, name.to_owned(), entry);
        let mut state = lock(&self.state);
        let mut next = state.clone();
        next.record_served(&item);
        if item.metadata.is_some() {
            next.marks.unlisted.insert(name.to_owned());
        }
        next.marks.check_manifest(&next.skills)?;
        lock(&self.catalog).record_unlisted(&item, &origin.label);
        *state = next;
        Ok(item.metadata.is_some())
    }
}
fn not_found(name: &str) -> SkillError {
    SkillError::EntryValidation {
        message: format!("Skill {} not found", quoted_name(name)),
    }
}
