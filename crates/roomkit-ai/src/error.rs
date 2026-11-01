//! What building an AI channel fails with.

/// What building an AI channel failed with: what Python's raises as
/// `ValueError`.
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
    /// A label the host gives an MCP server is no identifier: letters,
    /// digits, `_`, `.` and `-`, 64 at most (RFC §21.2).
    #[error("the MCP server label {label:?} is not an identifier")]
    ServerLabel {
        /// The label.
        label: String,
    },
    /// The host names an MCP server's instructions twice under one label.
    #[error("the MCP server {label:?} is given twice")]
    DuplicateServer {
        /// The label.
        label: String,
    },
    /// A tool's name is one no vendor accepts (RFC §6.7): an empty one, or
    /// one with a character other than a letter, a digit, `_`, `.`, `:` or
    /// `-`. Python's `ValueError`, in its words.
    #[error(
        "tool name '{name}' is accepted by no provider: use letters, digits, '_', '.', ':' or '-'"
    )]
    ToolName {
        /// The name.
        name: String,
    },
    /// A tool policy's pattern holds a bracket, which a pattern reads as
    /// itself (RFC §21.1): a rule written as a character class would cover
    /// none of the names it was written for. Python's `ValueError`.
    #[error("tool pattern '{pattern}' holds a bracket: a pattern reads * and ? alone")]
    ToolPattern {
        /// The pattern.
        pattern: String,
    },
    /// A tool's call is bounded by zero (RFC §21.6): Python's
    /// `ValueError`, a bound being positive or none.
    #[error("tool timeout for 'tool_timeout' must be positive or None, got 0")]
    ToolTimeout,
    /// Fewer than two handlers were given to compose. Python's
    /// `ValueError`.
    #[error("compose_tool_handlers requires at least 2 handlers, {given} given")]
    TooFewHandlers {
        /// How many were given.
        given: usize,
    },
}
