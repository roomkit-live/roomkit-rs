//! The two values of `SkillMetadata.allowed_tools` (RFC §24.2).

/// Python's text-or-list value, retained without changing its public shape.
/// `None` on [`super::SkillMetadata::allowed_tools`] means no value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SkillToolNames {
    /// A comma-separated text, optionally enclosed in one pair of brackets.
    Text(String),
    /// Individual names or glob patterns, in their original order.
    List(Vec<String>),
}

impl SkillToolNames {
    pub(crate) fn names(&self) -> Vec<String> {
        let entries: Vec<&str> = match self {
            Self::List(entries) => entries.iter().map(String::as_str).collect(),
            Self::Text(text) => {
                let text = text.trim();
                let inner = text
                    .strip_prefix('[')
                    .and_then(|text| text.strip_suffix(']'));
                let text = inner
                    .filter(|inner| !inner.contains(['[', ']']))
                    .unwrap_or(text);
                text.split(',').collect()
            }
        };
        entries
            .into_iter()
            .map(|entry| entry.trim().trim_matches(['\'', '"']).trim())
            .filter(|entry| !entry.is_empty())
            .map(str::to_owned)
            .collect()
    }

    /// Repeated keys cannot erase an earlier gate (RFC §24.1).
    pub(crate) fn merge(&mut self, next: &Self) {
        let mut names = self.names();
        for name in next.names() {
            if !names.contains(&name) {
                names.push(name);
            }
        }
        *self = Self::List(names);
    }
}
