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
    /// A router named no provider for a turn's start, where it must name
    /// one (RFC §6.4): Python's `ValueError`, in its words. The turn fails.
    #[error("router '{router}' named no provider for a turn")]
    RouterNamedNone {
        /// The router's name.
        router: String,
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
    /// A steering directive addressed to none or more than one of a loop,
    /// a room and a turn (RFC §21.3). Python's `ValueError`.
    #[error("steer() addresses exactly one of a loop_id, a room_id or a turn_id")]
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
    /// A tool under a name Tool Search serves, `find_tools` or `list_tools`
    /// (RFC §21.1): Python's `ValueError`, which turning Tool Search off frees.
    #[error(
        "Tool '{name}' is a tool channel '{channel}' serves itself: rename it or set tool_search \
         to Some(false) (RFC §21.1)"
    )]
    ToolSearchName {
        /// The name.
        name: String,
        /// The channel.
        channel: String,
    },
    /// A token cap of zero (RFC §6.4): Python's `ValueError`, a cap being a
    /// positive number or none.
    #[error("tool_search_threshold_tokens must be a positive integer or None, got 0")]
    ToolSearchThresholdTokens,
    /// A retry policy's delay or multiplier is not greater than 0, as
    /// Python's `RetryPolicy` refuses it (`gt=0`): pydantic's words.
    #[error("{reason}")]
    RetryPolicy {
        /// What the policy's check refused.
        reason: String,
    },
    /// A model's price holds a field Python's `ModelPricing` refuses: a
    /// rate below 0 or not finite, a multiplier or a threshold not above
    /// 0, an empty currency (pydantic's words).
    #[error("ModelPricing.{field}: {reason}")]
    Pricing {
        /// The field refused.
        field: &'static str,
        /// Why, in pydantic's words.
        reason: String,
    },
    /// A response's billed usage counter is no non-negative integer, as
    /// Python's `ModelPricing.cost_for` raises `ValueError`.
    #[error("usage counter '{name}' must be a non-negative integer")]
    UsageCounter {
        /// The counter.
        name: String,
    },
    /// A calendar day that does not exist, or a text that writes none, as
    /// Python's `date` and `date.fromisoformat` raise `ValueError`.
    #[error("{reason}")]
    Date {
        /// Python's words.
        reason: String,
    },
    /// A response schema outside the portable subset (RFC §6.7), as
    /// Python's `check_portable_schema` raises `ValueError`, in its words:
    /// the path of the first subschema that is not portable, and why.
    #[error("{reason}")]
    ResponseSchema {
        /// Python's words.
        reason: String,
    },
    /// A turn's cost budget is not above 0 (RFC §6.4): Python's
    /// `ValueError`, a budget being a positive number.
    #[error("turn_budget_usd must be a positive number, got {shown}")]
    TurnBudgetUsd {
        /// The budget, as Python's `repr` writes it.
        shown: String,
    },
    /// A cost budget set for a model with no catalogue price, whose cost
    /// cannot be counted (RFC §6.4): Python's `ValueError`.
    #[error(
        "turn_budget_usd needs a model with a known price; '{model}' has none in the catalogue. \
         Set turn_budget_tokens instead."
    )]
    UnpricedModel {
        /// The model's name.
        model: String,
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
