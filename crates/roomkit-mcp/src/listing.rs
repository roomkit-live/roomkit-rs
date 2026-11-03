//! A server's tools, listed once when it connects (RFC §21.2): every page
//! its cursor leads to, within bounds, each tool a definition the model
//! can be handed.

use std::collections::HashSet;

use rmcp::model::{PaginatedRequestParams, Tool};
use rmcp::service::{Peer, RoleClient};
use roomkit_ai::AiTool;
use serde_json::Value;

use crate::error::McpError;
use crate::texts::{bounded_message, bounded_text};

/// The most pages of `tools/list` read: a bound on a server whose cursor
/// never ends, far above any real catalogue, as the reference
/// implementation reads.
pub(crate) const PAGE_LIMIT: usize = 1000;

/// The most tools a server's listing keeps.
pub(crate) const TOOL_LIMIT: usize = 512;

/// The longest tool name kept, in characters: the longest a vendor accepts.
pub(crate) const NAME_LIMIT: usize = 128;

/// The longest tool description kept, in characters: a longer one is cut,
/// at a word.
pub(crate) const DESCRIPTION_LIMIT: usize = 4096;

/// The largest parameter schema kept, serialized, in bytes: a tool with a
/// larger one is left out.
pub(crate) const SCHEMA_LIMIT: usize = 32 * 1024;

/// Which tools of the server a provider keeps, by name.
pub(crate) type ToolFilter = std::sync::Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// Every tool the server lists that `filter` admits, following its cursor
/// (RFC §21.2), each one a definition, in the order listed: the filter
/// chooses before the bound counts, as Python filters the listing it reads.
/// The reading stops where it stands, with a warning naming the server, at
/// a page that brings nothing new (an empty page that still carries a
/// cursor, a cursor handed out twice), past [`PAGE_LIMIT`] pages, or once
/// [`TOOL_LIMIT`] tools are kept.
///
/// # Errors
///
/// Returns [`McpError::Listing`] when a page's request fails.
pub(crate) async fn tools(
    peer: &Peer<RoleClient>,
    program: &str,
    filter: Option<&ToolFilter>,
) -> Result<Vec<AiTool>, McpError> {
    let mut kept = Catalogue::new(program);
    let mut seen: HashSet<String> = HashSet::new();
    let mut params: Option<PaginatedRequestParams> = None;
    for _ in 0..PAGE_LIMIT {
        let page = peer
            .list_tools(params.take())
            .await
            .map_err(|error| McpError::Listing {
                program: program.to_owned(),
                message: bounded_message(&error.to_string()),
            })?;
        let brought = !page.tools.is_empty();
        let admitted = page
            .tools
            .iter()
            .filter(|tool| filter.is_none_or(|admits| admits(tool.name.as_ref())));
        for tool in admitted {
            if !kept.take(tool) {
                return Ok(kept.tools);
            }
        }
        let Some(cursor) = page.next_cursor.filter(|cursor| !cursor.is_empty()) else {
            return Ok(kept.tools);
        };
        if !brought || !seen.insert(cursor.clone()) {
            break;
        }
        params = Some(PaginatedRequestParams::default().with_cursor(Some(cursor)));
    }
    tracing::warn!(
        target: "roomkit::mcp",
        server = program,
        tools = kept.tools.len(),
        "an MCP server pages tools/list without end; the tools read are kept"
    );
    Ok(kept.tools)
}

/// The tools kept so far, and their names.
struct Catalogue<'a> {
    program: &'a str,
    tools: Vec<AiTool>,
    names: HashSet<String>,
}

impl<'a> Catalogue<'a> {
    fn new(program: &'a str) -> Self {
        Self {
            program,
            tools: Vec::new(),
            names: HashSet::new(),
        }
    }

    /// Keeps `tool` if it can be handed to a model; whether the catalogue
    /// takes more.
    fn take(&mut self, tool: &Tool) -> bool {
        if self.tools.len() == TOOL_LIMIT {
            tracing::warn!(
                target: "roomkit::mcp",
                server = self.program,
                kept = TOOL_LIMIT,
                "an MCP server lists more tools than are kept"
            );
            return false;
        }
        if let Some(definition) = self.definition(tool) {
            self.names.insert(definition.name.clone());
            self.tools.push(definition);
        }
        true
    }

    /// `tool` as the model is handed it, or none, with a warning naming the
    /// server, for a tool no model can be handed: a name no vendor accepts
    /// (RFC §6.7: a loader of tools defined elsewhere skips it and keeps
    /// the others) or longer than [`NAME_LIMIT`], a name listed already
    /// (RFC §21.1 "One tool per name"), or a schema larger than
    /// [`SCHEMA_LIMIT`]. Its description is cut at [`DESCRIPTION_LIMIT`]
    /// characters.
    fn definition(&self, tool: &Tool) -> Option<AiTool> {
        let name = tool.name.as_ref();
        let skip = |why: &str| {
            tracing::warn!(
                target: "roomkit::mcp",
                server = self.program,
                tool = %bounded_message(name),
                why,
                "an MCP tool is left out"
            );
            None
        };
        if !is_tool_name(name) {
            return skip("no vendor accepts its name");
        }
        if self.names.contains(name) {
            return skip("its name is listed already");
        }
        let parameters = Value::Object(tool.input_schema.as_ref().clone());
        if serde_json::to_string(&parameters).map_or(true, |schema| schema.len() > SCHEMA_LIMIT) {
            return skip("its parameters' schema is too large");
        }
        let description = tool.description.as_deref().unwrap_or_default();
        if description.chars().count() > DESCRIPTION_LIMIT {
            tracing::warn!(
                target: "roomkit::mcp",
                server = self.program,
                tool = name,
                kept = DESCRIPTION_LIMIT,
                "an MCP tool's description is cut"
            );
        }
        let tags = published_tags(tool);
        AiTool::new(name, bounded_text(description, DESCRIPTION_LIMIT))
            .ok()
            .map(|tool| AiTool {
                parameters,
                tags,
                ..tool
            })
    }
}

/// The tags a server published for `tool`, which `FastMCP` writes in its
/// `_meta.fastmcp.tags`: what lets `find_tools` meet a tool across languages
/// (RFC §6.4). Python's `_definition`.
fn published_tags(tool: &Tool) -> Vec<String> {
    tool.meta
        .as_ref()
        .and_then(|meta| meta.0.get("fastmcp"))
        .and_then(|fastmcp| fastmcp.get("tags"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

/// Whether some vendor accepts `name` for a tool (RFC §6.7): letters,
/// digits, `_`, `.`, `:` and `-`, and at most [`NAME_LIMIT`] of them.
fn is_tool_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= NAME_LIMIT
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'))
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    /// A tool as a server lists it, its `_meta` as written.
    fn tool(meta: &Value) -> Tool {
        serde_json::from_value(json!({
            "name": "search",
            "inputSchema": { "type": "object" },
            "_meta": meta,
        }))
        .unwrap()
    }

    // Python: tools/mcp.py's _definition(): FastMCP's _meta.fastmcp.tags
    #[test]
    fn a_tool_s_tags_are_what_fastmcp_published() {
        let tagged = tool(&json!({ "fastmcp": { "tags": ["send", "message"] } }));
        assert_eq!(published_tags(&tagged), ["send", "message"]);
        let other = tool(&json!({ "other": { "tags": ["send"] } }));
        assert_eq!(published_tags(&other), Vec::<String>::new());
    }

    // Python: tools/mcp.py's _definition(), which hands the tags to the AITool
    #[test]
    fn a_listed_tool_keeps_its_tags() {
        let catalogue = Catalogue::new("server");
        let tagged = tool(&json!({ "fastmcp": { "tags": ["send"] } }));
        let definition = catalogue.definition(&tagged).unwrap();
        assert_eq!(definition.tags, ["send"]);
    }
}
