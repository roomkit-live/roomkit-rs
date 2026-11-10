//! Anthropic reasoning switches and effort levels (RFC §6.7).
use crate::AnthropicConfig;
use roomkit_ai::{AiContext, ModelInfo};
use serde_json::{Map, Value, json};

fn tagged(model: Option<&ModelInfo>, tag: &str) -> bool {
    model.is_some_and(|model| model.capabilities.iter().any(|value| value == tag))
}

fn switch(context: &AiContext) -> Option<bool> {
    context
        .thinking_budget
        .map(|budget| budget > 0)
        .or(context.enable_thinking)
        .or_else(|| (context.reasoning_effort.as_deref() == Some("none")).then_some(false))
}

pub(crate) fn sampling(
    config: &AnthropicConfig,
    context: &AiContext,
    model: Option<&ModelInfo>,
) -> Map<String, Value> {
    let mut fields = Map::new();
    if switch(context) == Some(true) {
        if config.adaptive() {
            fields.insert(
                "thinking".into(),
                json!({"type":"adaptive", "display":"summarized"}),
            );
            return fields;
        }
        if let Some(budget) = context.thinking_budget.filter(|budget| *budget > 0) {
            fields.insert(
                "thinking".into(),
                json!({"type":"enabled", "budget_tokens":budget}),
            );
            return fields;
        }
    }
    if switch(context) == Some(false) {
        for (tag, kind) in [
            ("thinking_off_disabled", "disabled"),
            ("thinking_off_between_tools", "between_tools"),
        ] {
            if tagged(model, tag) {
                fields.insert("thinking".into(), json!({"type":kind}));
                break;
            }
        }
    }
    if config.temperature() {
        fields.insert("temperature".into(), json!(context.temperature));
    }
    fields
}

pub(crate) fn effort<'a>(
    context: &'a AiContext,
    model: Option<&'a ModelInfo>,
) -> Option<&'static str> {
    if tagged(model, "effort_refused") {
        return None;
    }
    let floor = model.and_then(|model| {
        model
            .capabilities
            .iter()
            .find_map(|tag| tag.strip_prefix("reasoning_floor_"))
    });
    let off = switch(context) == Some(false);
    let effort = if off {
        floor.or(context.reasoning_effort.as_deref())
    } else {
        context.reasoning_effort.as_deref()
    }?;
    let scale = ["minimal", "low", "medium", "high", "xhigh", "max"];
    let position = scale.iter().position(|level| *level == effort)?;
    let high_only =
        tagged(model, "effort_up_to_high") || (off && tagged(model, "thinking_off_effort_high"));
    scale
        .iter()
        .enumerate()
        .skip(1)
        .filter(|(index, level)| {
            (!high_only || *index <= 3) && !(tagged(model, "effort_no_xhigh") && **level == "xhigh")
        })
        .min_by_key(|(index, _)| index.abs_diff(position))
        .map(|(_, level)| *level)
}
