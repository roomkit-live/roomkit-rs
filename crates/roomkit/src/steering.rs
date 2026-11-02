//! Steering directives (RFC §21.3): what a host hands an AI turn's running
//! tool loop, Python's `roomkit.models.steering`. Each crosses the turn
//! backend in the serialized form, tagged by its `type` (RFC §10.6.3
//! `steer`).

use serde::{Deserialize, Serialize};

/// Adds a message to the turn's conversation, read at its next stop
/// (RFC §21.3): Python's `InjectMessage(*, content, role="user")`.
///
/// ```
/// use roomkit::model::{InjectMessage, SteeringDirective};
///
/// let directive = SteeringDirective::from(InjectMessage::new("Only the .py files."));
/// assert_eq!(
///     serde_json::to_value(&directive)?,
///     serde_json::json!({"type": "inject_message", "content": "Only the .py files.", "role": "user"})
/// );
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InjectMessage {
    /// What the message says.
    pub content: String,
    /// Who the conversation reads it from: `user` by default, or
    /// `assistant` or `system`.
    #[serde(default = "user")]
    pub role: String,
}

impl InjectMessage {
    /// A message saying `content`, from the user.
    pub fn new(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            role: user(),
        }
    }
}

fn user() -> String {
    "user".to_owned()
}

/// Ends the turn's loop at the first stop it reaches (RFC §21.3): Python's
/// `Cancel(*, reason="cancelled")`. It also sets the loop's cancel at
/// once, which a generation reads at each of its events.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cancel {
    /// Why: `cancelled` by default.
    #[serde(default = "cancelled")]
    pub reason: String,
}

impl Default for Cancel {
    fn default() -> Self {
        Self {
            reason: cancelled(),
        }
    }
}

fn cancelled() -> String {
    "cancelled".to_owned()
}

/// Appends to the system prompt of the turn's next generations (RFC
/// §21.3): Python's `UpdateSystemPrompt(*, append)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateSystemPrompt {
    /// What is appended, as written.
    pub append: String,
}

impl UpdateSystemPrompt {
    /// The directive appending `append`.
    pub fn new(append: impl Into<String>) -> Self {
        Self {
            append: append.into(),
        }
    }
}

/// A steering directive (RFC §21.3): Python's union `SteeringDirective`,
/// each variant tagged by its `type`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SteeringDirective {
    /// Adds a message to the conversation.
    InjectMessage(InjectMessage),
    /// Ends the loop.
    Cancel(Cancel),
    /// Appends to the system prompt.
    UpdateSystemPrompt(UpdateSystemPrompt),
}

impl SteeringDirective {
    /// Whether it is a [`Cancel`], which reaches every loop it addresses
    /// (RFC §21.3).
    pub(crate) fn is_cancel(&self) -> bool {
        matches!(self, Self::Cancel(_))
    }
}

impl From<InjectMessage> for SteeringDirective {
    fn from(directive: InjectMessage) -> Self {
        Self::InjectMessage(directive)
    }
}

impl From<Cancel> for SteeringDirective {
    fn from(directive: Cancel) -> Self {
        Self::Cancel(directive)
    }
}

impl From<UpdateSystemPrompt> for SteeringDirective {
    fn from(directive: UpdateSystemPrompt) -> Self {
        Self::UpdateSystemPrompt(directive)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    /// The wire form of each directive is Python's: tagged by `type`, its
    /// defaults filled when they are left out (RFC §10.6.3 `steer`).
    #[test]
    fn each_directive_reads_and_writes_python_s_form() {
        let written = [
            SteeringDirective::from(InjectMessage::new("x")),
            Cancel::default().into(),
            UpdateSystemPrompt::new(" more").into(),
        ]
        .map(|directive| serde_json::to_value(directive).unwrap());
        assert_eq!(
            written,
            [
                json!({"type": "inject_message", "content": "x", "role": "user"}),
                json!({"type": "cancel", "reason": "cancelled"}),
                json!({"type": "update_system_prompt", "append": " more"}),
            ]
        );
        let read: Vec<SteeringDirective> = serde_json::from_value(json!([
            {"type": "inject_message", "content": "x"},
            {"type": "cancel"},
        ]))
        .unwrap();
        assert_eq!(
            read,
            [InjectMessage::new("x").into(), Cancel::default().into()]
        );
    }
}
