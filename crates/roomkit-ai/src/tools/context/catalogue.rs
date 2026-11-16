//! The resolved base belongs to one loop context; round declarations are
//! shared with its preparation (Python `_ToolLoopContext.for_loop`).

use std::collections::{BTreeSet, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

#[derive(Clone)]
struct Resolved {
    names: HashSet<String>,
    withdrawn: BTreeSet<String>,
}

/// What a turn's tool policy says of the server an MCP alias names (RFC
/// §21.2).
pub(crate) type Admits = Arc<dyn Fn(&str) -> bool + Send + Sync>;

#[derive(Clone, Default)]
pub(super) struct Catalogue {
    resolved: Arc<Mutex<Option<Resolved>>>,
    declared: Arc<Mutex<HashSet<String>>>,
    alias_admits: Arc<Mutex<Option<Admits>>>,
}

impl Catalogue {
    pub(super) fn judge_aliases(&self, alias_admits: Admits) {
        *self
            .alias_admits
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(alias_admits);
    }

    /// Whether the turn's policy lets a bare name run on the server `alias`
    /// names; true under no policy.
    pub(super) fn admits_alias(&self, alias: &str) -> bool {
        let admits = self
            .alias_admits
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        admits.is_none_or(|admits| admits(alias))
    }

    pub(super) fn resolve(&self, names: HashSet<String>, withdrawn: &BTreeSet<String>) {
        *self.resolved.lock().unwrap_or_else(PoisonError::into_inner) = Some(Resolved {
            names,
            withdrawn: withdrawn.clone(),
        });
    }

    pub(super) fn record(&self, name: &str) {
        self.declared
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name.to_owned());
    }

    pub(super) fn names(&self) -> Option<HashSet<String>> {
        let resolved = self.resolved.lock().unwrap_or_else(PoisonError::into_inner);
        let resolved = resolved.as_ref()?;
        let mut names = resolved.names.clone();
        names.extend(
            self.declared
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .cloned(),
        );
        names.retain(|name| !resolved.withdrawn.contains(name));
        Some(names)
    }

    /// Consumption keeps preparation's resolved base; later withdrawals
    /// affect only consumption and its children. Both see new declarations.
    pub(super) fn for_loop(&self) -> Self {
        Self {
            resolved: Arc::new(Mutex::new(
                self.resolved
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .clone(),
            )),
            declared: Arc::clone(&self.declared),
            alias_admits: Arc::new(Mutex::new(
                self.alias_admits
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .clone(),
            )),
        }
    }
}
