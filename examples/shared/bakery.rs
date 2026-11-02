//! The bakery's agent, which the server examples serve: its prompt and its
//! tool, `lookup_order`.

use std::sync::Arc;

use roomkit::model::Metadata;
use roomkit_ai::{AiError, AiTool, Tool, ToolEntry, ToolError, ToolFuture, ToolResult};
use serde::Deserialize;
use serde_json::{Value, json};

/// What the agent is told before each conversation.
pub const SYSTEM_PROMPT: &str = "You answer the customers of a bakery, in one short sentence.";

/// The one order the lookup finds, as its result says it: the model reads
/// it as Python's `json.dumps` writes it.
pub const ORDER_42: &str =
    r#"{"items": "two loaves", "order": "42", "pickup": "5 pm", "status": "ready"}"#;

/// The bakery's tool: `lookup_order`, finding an order by its number (RFC
/// §6.4, §21.4), a tool that serves its own calls. A number it does not
/// know is refused with words the model reads.
///
/// # Errors
///
/// Returns the [`AiError`] the tool's definition is refused with.
pub fn orders() -> Result<ToolEntry, AiError> {
    let definition = AiTool {
        parameters: json!({
            "type": "object",
            "properties": { "order": { "type": "string", "description": "The order's number" } },
            "required": ["order"],
        }),
        ..AiTool::new(
            "lookup_order",
            "Find a customer's order by its number: what it holds, its status and its pickup time.",
        )?
    };
    Ok(ToolEntry::Tool(Arc::new(LookupOrder { definition })))
}

/// `lookup_order`, its definition and its handler.
struct LookupOrder {
    definition: AiTool,
}

impl Tool for LookupOrder {
    fn definition(&self) -> AiTool {
        self.definition.clone()
    }

    fn handler(&self, name: String, arguments: Metadata) -> ToolFuture {
        #[derive(Deserialize)]
        struct Lookup {
            order: String,
        }
        Box::pin(async move {
            if name != "lookup_order" {
                return Err(ToolError::unserved(format!("not this tool's: {name}")));
            }
            let Lookup { order } = serde_json::from_value(Value::Object(arguments))
                .map_err(|_| ToolError::refused("The order's number is missing."))?;
            if order != "42" {
                return Err(ToolError::refused(format!("There is no order {order}.")));
            }
            let found: Value = serde_json::from_str(ORDER_42).map_err(|e| ToolError::raised(&e))?;
            Ok(ToolResult::Json(found))
        })
    }
}
