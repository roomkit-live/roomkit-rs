//! What building or steering an AI channel fails with.

/// What building or steering an AI channel failed with: what Python's
/// raises as `ValueError`.
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
    /// A steering directive addressed to more than one of a loop, a room
    /// and a turn (RFC §21.3). Python's `ValueError`.
    #[error("steer() addresses a loop_id, a room_id or a turn_id, not several")]
    SteerAddress,
    /// A human-input tool handler given no tool name to serve. Python's
    /// `ValueError`.
    #[error("tool_names must not be empty")]
    HumanInputNames,
    /// A human-input tool handler given a definition under a name it does
    /// not serve. Python's `ValueError`.
    #[error(
        "Tool definition '{name}' not in tool_names {{{}}}",
        quoted_names(tool_names)
    )]
    HumanInputDefinition {
        /// The definition's name.
        name: String,
        /// The names served.
        tool_names: Vec<String>,
    },
    /// A tool's own bound (`tool_timeouts`) is zero (RFC §21.6): Python's
    /// `ValueError`, a bound being positive or none.
    #[error("tool timeout for '{name}' must be positive or None, got 0")]
    ToolTimeoutOf {
        /// The tool's name.
        name: String,
    },
    /// A turn's token budget is zero (RFC §6.4): Python's `ValueError`, a
    /// budget being a positive number.
    #[error("turn_budget_tokens must be a positive number, got 0")]
    TurnBudgetTokens,
    /// A retry policy's delay or multiplier is not greater than 0, as
    /// Python's `RetryPolicy` refuses it (`gt=0`): pydantic's words.
    #[error("{reason}")]
    RetryPolicy {
        /// What the policy's check refused.
        reason: String,
    },
}

/// `names` as Python writes a set of strings' items.
fn quoted_names(names: &[String]) -> String {
    names
        .iter()
        .map(|name| format!("'{name}'"))
        .collect::<Vec<_>>()
        .join(", ")
}
