//! Verified content and shared file caches, never local executable files (RFC §24.7).

use super::{
    LoadedSkill, MAX_TOTAL_SIZE, McpSkill, McpSkillOptions, ServedOrigin, SkillEntry,
    verify_frontmatter, verify_skill_resource, wire,
};
use crate::skills::registry::lock;
use crate::skills::texts::quoted_name;
use crate::{SkillError, SkillPathError, SkillRegistry};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

impl SkillRegistry {
    /// Load a local skill snapshot or verify the served SKILL.md on demand.
    ///
    /// # Errors
    /// Preserves manifest and discovery failures. Invalid content refreshes the
    /// entry and returns a sanitized verification error without any instructions.
    pub async fn load_skill(&self, name: &str) -> Result<Option<LoadedSkill>, SkillError> {
        if !self.is_served(name) {
            return self.get_skill(name);
        }
        if let Some(skill) = self.get_skill(name)? {
            return Ok(Some(skill));
        }
        if self.get_unavailable_reason(name).is_some() {
            return Ok(None);
        }
        if self.get_metadata(name).is_none() && !self.fetch_unlisted(name).await? {
            return Ok(None);
        }
        let metadata = self.get_metadata(name).ok_or_else(|| missing(name))?;
        let (origin, entry, retirements) = self.held_entry(name)?;
        let (content, instructions) = match read_instructions(&origin, &entry).await {
            Ok(loaded) => loaded,
            Err(error) => {
                tracing::warn!(target:"roomkit::skills",%error,"served SKILL.md failed verification");
                self.refresh(name).await;
                return Err(SkillError::VerificationCaused {
                    message: format!(
                        "Skill {} changed on its server or could not be verified",
                        quoted_name(name)
                    ),
                    source: Arc::new(error),
                });
            }
        };
        let read_total = content.len();
        let skill = McpSkill::new(
            metadata,
            instructions,
            std::path::PathBuf::from("/dev/null"),
            McpSkillOptions {
                origin: origin.label.clone(),
                files: BTreeMap::from([(entry.uri().to_owned(), content)]),
                entry: Some(entry),
                read_total,
                retirements,
            },
        );
        lock(&self.state)
            .loaded
            .insert(name.to_owned(), Arc::new(Mutex::new(skill.clone())));
        Ok(Some(LoadedSkill::Served(skill)))
    }

    /// Read a local reference or a file in the loaded served entry, verified first.
    ///
    /// # Errors
    /// Returns missing-skill, path, local I/O/encoding, manifest or changed-skill
    /// errors. A failed served-file check retires the skill in every shared copy.
    pub async fn read_file(&self, name: &str, filename: &str) -> Result<String, SkillError> {
        if !self.is_served(name) {
            let skill = self.get_skill(name)?.ok_or_else(|| missing(name))?;
            let filename = filename.to_owned();
            return tokio::task::spawn_blocking(move || skill.read_reference(&filename))
                .await
                .map_err(|error| {
                    wire::discovery(
                        "local reference reader did not finish".to_owned(),
                        Box::new(error),
                    )
                })?
                .map_err(SkillError::from);
        }
        let loaded = lock(&self.state)
            .loaded
            .get(name)
            .cloned()
            .ok_or_else(|| missing(name))?;
        self.follow_catalog(name)?;
        if !lock(&self.state).loaded.contains_key(name) {
            return Err(changed(name));
        }
        let snapshot = lock(&loaded).clone();
        let entry = snapshot.held()?;
        let uri = entry.resolve(filename)?;
        if entry.resource(&uri).is_none() && !entry.dynamic() {
            return Err(SkillPathError(format!(
                "{} is not a file of skill {}",
                quoted_name(filename),
                quoted_name(name)
            ))
            .into());
        }
        if let Some(content) = snapshot.files.get(&uri) {
            return Ok(display(content));
        }
        let origin = self.origin(name).ok_or_else(|| missing(name))?;
        let read = read_file(&origin, &snapshot, &uri)
            .await
            .and_then(|content| cache_file(&loaded, uri, content));
        match read {
            Ok(result) => Ok(result),
            Err(error) => {
                tracing::warn!(target:"roomkit::skills",%error,"served file failed verification");
                *lock(&self.catalog)
                    .retirements
                    .entry(name.to_owned())
                    .or_default() += 1;
                self.refresh(name).await;
                lock(&self.state).loaded.remove(name);
                Err(SkillError::Changed {
                    name: name.to_owned(),
                    source: Some(Arc::new(error)),
                })
            }
        }
    }

    fn held_entry(&self, name: &str) -> Result<(Arc<ServedOrigin>, SkillEntry, u64), SkillError> {
        let catalog = lock(&self.catalog);
        let label = catalog.label_of(name).ok_or_else(|| missing(name))?;
        let origin = catalog
            .origins
            .get(label)
            .cloned()
            .ok_or_else(|| missing(name))?;
        let entry = catalog
            .entries
            .get(name)
            .cloned()
            .ok_or_else(|| missing(name))?;
        Ok((
            origin,
            entry,
            catalog.retirements.get(name).copied().unwrap_or_default(),
        ))
    }
    async fn refresh(&self, name: &str) {
        let Ok((origin, held, _)) = self.held_entry(name) else {
            return;
        };
        if let Ok(entry) = wire::get(origin.server.as_ref(), name, held.uri()).await {
            lock(&self.catalog).entries.insert(name.to_owned(), entry);
        }
    }
}
async fn read_instructions(
    origin: &ServedOrigin,
    entry: &SkillEntry,
) -> Result<(Vec<u8>, String), SkillError> {
    let content = wire::read(origin.server.as_ref(), entry.uri()).await?;
    if let Some(resource) = entry.resource(entry.uri()) {
        verify_skill_resource(resource, &content)?;
    } else {
        check_total(content.len())?;
    }
    let text = std::str::from_utf8(&content).map_err(|_| SkillError::Verification {
        message: format!("{}: not UTF-8 text", entry.uri()),
    })?;
    let body = verify_frontmatter(entry, text)?;
    Ok((content, body))
}
async fn read_file(
    origin: &ServedOrigin,
    skill: &McpSkill,
    uri: &str,
) -> Result<Vec<u8>, SkillError> {
    let content = wire::read(origin.server.as_ref(), uri).await?;
    if let Some(resource) = skill.held()?.resource(uri) {
        verify_skill_resource(resource, &content)?;
    }
    Ok(content)
}

/// Charge against bytes committed while the network read was pending. Check and
/// update share one short lock; no read or refresh awaits with that lock held.
fn cache_file(
    loaded: &Mutex<McpSkill>,
    uri: String,
    content: Vec<u8>,
) -> Result<String, SkillError> {
    let mut loaded = lock(loaded);
    if loaded.held()?.dynamic() {
        check_total(loaded.read_total.saturating_add(content.len()))?;
    }
    let result = display(&content);
    loaded.read_total += content.len();
    loaded.files.insert(uri, content);
    Ok(result)
}
fn check_total(total: usize) -> Result<(), SkillError> {
    if u64::try_from(total).map_or(true, |total| total > MAX_TOTAL_SIZE) {
        return Err(SkillError::Verification {
            message: format!("past {MAX_TOTAL_SIZE} bytes in all"),
        });
    }
    Ok(())
}
fn display(content: &[u8]) -> String {
    std::str::from_utf8(content).map_or_else(
        |_| format!("[binary file of {} bytes, not shown]", content.len()),
        str::to_owned,
    )
}
fn missing(name: &str) -> SkillError {
    SkillError::NotFound {
        name: name.to_owned(),
    }
}
fn changed(name: &str) -> SkillError {
    SkillError::Changed {
        name: name.to_owned(),
        source: None,
    }
}
