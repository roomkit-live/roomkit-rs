//! The bakery's agent, which the server examples serve: its prompt and its
//! tool, `lookup_order`.

use roomkit_ai::{AiTool, ToolError, ToolOutput, Tools};
use serde::Deserialize;
use serde_json::{Value, json};

/// What the agent is told before each conversation.
pub const SYSTEM_PROMPT: &str = "You answer the customers of a bakery, in one short sentence.";

/// The one order the lookup finds, as its result says it.
pub const ORDER_42: &str =
    r#"{"items":"two loaves","order":"42","pickup":"5 pm","status":"ready"}"#;

/// The bakery's tools: `lookup_order`, finding an order by its number (RFC
/// §6.4, §21.4). A number it does not know is refused with words the model
/// reads.
pub fn orders() -> Tools {
    #[derive(Deserialize)]
    struct Lookup {
        order: String,
    }
    let lookup = AiTool::new(
        "lookup_order",
        "Find a customer's order by its number: what it holds, its status and its pickup time.",
        json!({
            "type": "object",
            "properties": { "order": { "type": "string", "description": "The order's number" } },
            "required": ["order"],
        }),
    );
    Tools::new().tool(lookup, |call, _context| async move {
        let Lookup { order } = roomkit_ai::arguments(&call)?;
        if order != "42" {
            return Err(ToolError::refused(format!("There is no order {order}.")));
        }
        let found: Value = serde_json::from_str(ORDER_42).map_err(|e| ToolError::raised(&e))?;
        Ok(ToolOutput::Json(found))
    })
}
