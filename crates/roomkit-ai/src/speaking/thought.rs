//! The thought a silent agent carries to its next turn (RFC §6.4).

use roomkit::model::Metadata;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What the agent thinks, in the first person. This carrier does not truncate
/// its text or pending sentences; a thinker bounds the answer it generates.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Thought {
    /// What it makes of what is said.
    pub text: String,
    /// What it would say if given the turn, most important first.
    pub want_to_say: Vec<String>,
    /// Whether what it wants to say cannot wait.
    pub urgent: bool,
}

impl Thought {
    /// The thought after speaking: its reading remains, nothing is pending.
    #[must_use]
    pub fn said(&self) -> Self {
        Self {
            text: self.text.clone(),
            ..Self::default()
        }
    }

    /// The thought as a classifier reads it, with Python's `thinking` key.
    pub fn as_state(&self) -> Metadata {
        Metadata::from_iter([
            ("thinking".into(), Value::String(self.text.clone())),
            ("want_to_say".into(), Value::from(self.want_to_say.clone())),
            ("urgent".into(), Value::Bool(self.urgent)),
        ])
    }
}
