//! What building an agent fails with.

/// What building an AI agent failed with.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AiError {
    /// A toolset declares a tool under a name the agent serves already, or
    /// twice: a name is served by one tool (RFC §21.1 "One tool per name").
    #[error("a tool named {name:?} is served already")]
    DuplicateTool {
        /// The name.
        name: String,
    },
}
