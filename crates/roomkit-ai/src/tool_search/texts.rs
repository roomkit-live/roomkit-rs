//! What Tool Search says to the model, word for word as the reference
//! implementation says it: its two tools' definitions and the preamble of a
//! prompt whose catalogue it hides. Python's `_tool_search_constants.py`.

use serde_json::{Value, json};

use crate::tools::AiTool;

/// The Tool Search tool that searches the catalogue by query and reveals its
/// matches. Python's `TOOL_FIND_TOOLS`.
pub const TOOL_FIND_TOOLS: &str = "find_tools";

/// The Tool Search tool that lists the catalogue. Python's `TOOL_LIST_TOOLS`.
pub const TOOL_LIST_TOOLS: &str = "list_tools";

/// The names of the discovery tools a channel serves itself under Tool Search,
/// beside the host's tools. Python's `TOOL_SEARCH_INFRA_TOOL_NAMES`.
pub const TOOL_SEARCH_INFRA_TOOL_NAMES: [&str; 2] = [TOOL_FIND_TOOLS, TOOL_LIST_TOOLS];

/// What a prompt whose catalogue is hidden ends with. Python's
/// `TOOL_SEARCH_PREAMBLE`.
pub(crate) const TOOL_SEARCH_PREAMBLE: &str = "IMPORTANT — your visible tools are only a SMALL \
SUBSET of what you can do. Many more tools (sending messages, playing music, web search, looking \
up data, calendars, files, …) exist but are hidden until you search for them. Two discovery tools \
are always available:\n- `find_tools(query)` — search your FULL tool catalogue by natural-language \
task (e.g. \"play music on spotify\", \"look up a contact by phone\"). It returns the best matches \
WITH their parameter schemas, ready to call.\n- `list_tools(category=None)` — list every tool you \
can access (overview only; prefer `find_tools` to act).\nHARD RULE: never tell the user you cannot \
do something, that you lack a tool, or that a task is outside your capabilities — UNTIL you have \
called `find_tools` for that task and it returned no usable match. If a request needs an action \
you don't see a tool for, your FIRST step is `find_tools(query=<the task>)`, then call the matched \
tool directly. Do NOT narrate that you are searching — just call the tool and continue.\nALWAYS \
phrase your `find_tools` query in ENGLISH, even when the conversation is in another language — the \
catalogue is indexed by English keywords, so an English query matches reliably (e.g. for « liste \
mes fichiers » search \"list files\", for « envoyer un message » search \"send a message\"). \
Translate the user's intent to English keywords first, then search.";

/// `find_tools`'s definition. Python's `FIND_TOOLS_SCHEMA`.
fn find_tools_parameters() -> Value {
    json!({
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Natural-language description of what you need to do (e.g. \"look \
    up a contact by phone number\", \"send an SMS\").",
            },
            "max_results": {
                "type": "integer",
                "description": "Maximum number of tools to return. Defaults to 5. Keep small — \
    the goal is a focused result, not a list.",
            },
        },
        "required": ["query"],
    })
}

/// `list_tools`'s definition. Python's `LIST_TOOLS_SCHEMA`.
fn list_tools_parameters() -> Value {
    json!({
        "type": "object",
        "properties": {
            "category": {
                "type": "string",
                "description": "Optional prefix to filter tools by name (e.g. \"crm_\" or \
    \"admin_\"). Empty/omitted returns all.",
            },
        },
    })
}

/// The two discovery tools as a turn declares them. Python's
/// `search_tool_defs()`.
pub(crate) fn search_tool_defs() -> Vec<AiTool> {
    vec![
        AiTool::own(
            TOOL_FIND_TOOLS,
            "Search the agent's full tool catalogue by natural-language query. Returns up to N \
best matches with their schemas, which become directly invocable for the rest of the session.",
            find_tools_parameters(),
        ),
        AiTool::own(
            TOOL_LIST_TOOLS,
            "List every tool the agent has access to (name + short description), optionally \
filtered by a name-prefix category. Use sparingly — prefer find_tools for action.",
            list_tools_parameters(),
        ),
    ]
}
