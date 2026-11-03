//! The members a scenario may hold, so the runner never ignores one it does
//! not know. roomkit-specs `conformance/scenario.schema.json` is the
//! scenario's grammar; this is its skeleton, checked before a run: a member
//! the runner would not read (a misspelled expectation, a step of no known
//! kind) fails the scenario rather than pass unread. Records in the RFC's
//! serialized form (messages, contents, templates, capabilities) are the
//! implementation's to read, and open here.

use serde_json::Value;

const TOP: &[&str] = &[
    "$schema",
    "id",
    "title",
    "rules",
    "reentrancy",
    "notes",
    "setup",
    "steps",
    "expect",
];
const SETUP: &[&str] = &[
    "kit",
    "channels",
    "rooms",
    "hooks",
    "identity_hooks",
    "identities",
    "strategies",
    "session_auditor",
];
const KIT: &[&str] = &[
    "max_chain_depth",
    "process_timeout_ms",
    "router",
    "identity_resolver",
    "identity_channel_types",
    "identity_timeout_ms",
    "delivery_backend",
    "verification_policy",
    "media_resolver",
    "attached_media",
    "orchestration",
    "delivery_gap_timeout_ms",
    "lock_manager",
];
const MEDIA_RESOLVER: &[&str] = &["media"];
const ATTACHED_MEDIA: &[&str] = &["key"];
const REFUSES: &[&str] = &[
    "status",
    "message",
    "context_overflow",
    "provider",
    "retryable",
];
const RETRY_POLICY: &[&str] = &[
    "max_retries",
    "base_delay_ms",
    "max_delay_ms",
    "exponential_base",
];
const RESOLVER: &[&str] = &["answers"];
const RESOLVER_ANSWER: &[&str] = &["when", "repeat", "gate", "fail", "result"];
const RESOLVER_WHEN: &[&str] = &["sender_id", "channel_id", "room_id", "text"];
const IDENTITY_HOOK: &[&str] = &[
    "name",
    "trigger",
    "channel_types",
    "channel_ids",
    "directions",
    "reactions",
];
const IDENTITY_REACTION: &[&str] = &["when", "repeat", "gate", "fail", "result"];
const DELIVERY_BACKEND: &[&str] = &[
    "retry_delay_ms",
    "max_retry_delay_ms",
    "max_queue_size",
    "max_dead_letter_size",
];
const CHANNEL: &[&str] = &[
    "id",
    "type",
    "category",
    "direction",
    "capabilities",
    "recipient_key",
    "parse",
    "on_event",
    "deliver",
    "streams",
    "deliver_stream",
    "on_room_attached",
    "on_room_detached",
    "model",
    "tools",
    "tool_policy",
    "max_tool_rounds",
    "max_context_events",
    "tool_timeout_ms",
    "tool_timeouts_ms",
    "turn_budget_tokens",
    "reasoning_effort",
    "turn_budget_usd",
    "system_prompt",
    "temperature",
    "max_tokens",
    "config_provider",
    "thinking_budget",
    "enable_thinking",
    "thinking_coalesce_ms",
    "thinking_coalesce_chars",
    "publish_thinking_text",
    "retry_policy",
    "fallback_model",
    "max_author_images",
    "describe_empty_event",
    "serialize_room_turns",
    "steer_room_turns",
    "human_input",
    "kit_tools",
    "audit",
    "addresses",
    "streams_in",
    "recent_events_window",
    "sender_is_participant",
    "memory",
    "agent",
];
const ADDRESSES: &[&str] = &["normalizes", "conversation_key"];
const AGENT: &[&str] = &[
    "name",
    "role",
    "description",
    "purpose",
    "scope",
    "language",
    "identity_in_prompt",
    "greeting",
    "auto_greet",
];
const HUMAN_INPUT: &[&str] = &["tool_names", "timeout_ms", "tools"];
const TURN_CONFIG: &[&str] = &[
    "system_prompt",
    "thinking_budget",
    "enable_thinking",
    "tools",
    "temperature",
    "max_tokens",
    "reasoning_effort",
    "turn_budget_tokens",
    "turn_budget_usd",
    "tool_handler",
];
const TURN_TOOL: &[&str] = &["name", "description", "parameters"];
const HUMAN_TOOL: &[&str] = &["name", "description", "parameters"];
const MODEL: &[&str] = &["rounds", "vision", "pricing"];
const MODEL_ROUND: &[&str] = &[
    "text",
    "deltas",
    "thinking",
    "calls",
    "finish_reason",
    "usage",
    "fails",
    "done",
    "gate",
    "refuses",
    "failure_after_calls",
];
const MODEL_CALL: &[&str] = &["id", "name", "arguments", "cut"];
const TOOL: &[&str] = &[
    "name",
    "description",
    "parameters",
    "returns",
    "raises",
    "fails",
    "refuses",
    "declines",
    "hangs",
    "gate",
    "declared",
    "returns_json",
    "returns_parts",
];
const RAISES: &[&str] = &["kind", "message"];
const POLICY: &[&str] = &["allow", "deny"];
const MEMORY: &[&str] = &[
    "max_events",
    "messages",
    "notes",
    "speakers",
    "budget_aware",
    "compacting",
    "summarizing",
    "summarizer",
    "retrieval",
];
const MEMORY_MESSAGE: &[&str] = &["role", "text"];
const BUDGET_AWARE: &[&str] = &[
    "max_context_tokens",
    "safety_margin_ratio",
    "min_events",
    "reserved_tokens",
];
const COMPACTING: &[&str] = &[
    "max_context_tokens",
    "summary_ratio",
    "safety_margin_ratio",
    "min_events",
];
const RETRIEVAL: &[&str] = &["sources", "max_results", "min_query_length"];
const SOURCE: &[&str] = &["name", "results", "fails"];
const SOURCE_RESULT: &[&str] = &["content", "score", "source"];
const SUMMARIZING: &[&str] = &[
    "max_context_tokens",
    "tier1_ratio",
    "tier2_ratio",
    "truncate_chars",
    "summary_max_tokens",
    "min_events",
];
const ROOM: &[&str] = &[
    "id",
    "organization_id",
    "metadata",
    "timers",
    "bindings",
    "participants",
    "status",
];
const BINDING: &[&str] = &[
    "channel_id",
    "category",
    "access",
    "muted",
    "visibility",
    "metadata",
    "participant_id",
];
const HOOK: &[&str] = &[
    "name",
    "trigger",
    "execution",
    "priority",
    "timeout_ms",
    "channel_types",
    "channel_ids",
    "directions",
    "fail_closed",
    "needs_lock",
    "reactions",
    "router",
];
const REACTION: &[&str] = &[
    "when",
    "repeat",
    "gate",
    "do",
    "fail",
    "respond",
    "output",
    "result",
    "stop_after",
    "parsed",
    "refuse",
    "after_turn",
];
const REFUSE: &[&str] = &["error", "code", "retryable", "provider_message_id"];
const WHEN: &[&str] = &[
    "room_id",
    "text",
    "type",
    "source_channel_id",
    "chain_depth",
];
const OUTPUT: &[&str] = &[
    "tasks",
    "observations",
    "metadata_updates",
    "response_metadata",
    "error",
];
const RESULT: &[&str] = &[
    "action",
    "event",
    "reason",
    "injected_events",
    "tasks",
    "observations",
    "metadata",
];
const INJECTED: &[&str] = &["event", "target_channel_ids"];
const OUTCOME: &[&str] = &["result", "error"];
const ERROR: &[&str] = &["name", "reason"];
const EXPECT: &[&str] = &[
    "timeline",
    "rooms",
    "bindings",
    "framework_events",
    "ai_events",
    "hooks",
    "hook_order",
    "deliveries",
    "contexts",
    "turns",
    "observations",
    "thinking_events",
    "tasks",
    "actions",
    "lifecycle",
    "models",
    "streamed",
    "room_callbacks",
    "participants",
    "resolver",
    "media_resolver",
    "tool_audit",
    "session_audit",
];
const NEW_ROOM: &[&str] = &["id", "organization_id", "metadata", "timers"];
const STEPS: &[(&str, &[&str])] = &[
    ("await", &["await", "expect"]),
    ("concurrent", &["concurrent"]),
    ("wait_for", &["wait_for"]),
    ("open", &["open"]),
    ("idle", &["idle"]),
    ("wait_for_lock", &["wait_for_lock", "waiters"]),
    ("lock_releases", &["lock_releases"]),
];

/// Each call's arguments, beside `call` and `organization_id`.
fn arguments(call: &str) -> Option<&'static [&'static str]> {
    Some(match call {
        "process_inbound" => &["message", "room_id", "defer_delivery"],
        "delivery.cancel" => &["of", "reason", "timeout_ms"],
        "delivery.wait" => &["of"],
        "send_event" => &[
            "room_id",
            "channel_id",
            "content",
            "event_type",
            "participant_id",
            "visibility",
            "response_visibility",
            "addressed_to",
            "parent_event_id",
            "idempotency_key",
            "metadata",
            "chain_depth",
            "standalone",
        ],
        "post" => &[
            "room_id",
            "content",
            "participant_id",
            "addressed_to",
            "metadata",
            "channel_id",
            "idempotency_key",
            "chain_depth",
        ],
        "commit_event" => &["room_id", "event"],
        "update_event" => &["room_id", "event_id", "content", "source", "metadata"],
        "delete_event" => &["room_id", "event_id", "cascade_replies"],
        "create_room" => &["room", "orchestration"],
        "attach_channel" => &[
            "room_id",
            "channel_id",
            "category",
            "access",
            "visibility",
            "muted",
            "metadata",
            "participant_id",
        ],
        "detach_channel" | "mute" | "unmute" | "mute_output" | "unmute_output" => {
            &["room_id", "channel_id"]
        }
        "set_access" => &["room_id", "channel_id", "access"],
        "set_visibility" => &["room_id", "channel_id", "visibility"],
        "close_room" | "archive_room" | "check_timers" => &["room_id"],
        "send_greeting" => &["room_id", "channel_id", "agent_id", "greeting"],
        "register_channel" => &["channel"],
        "unregister_channel" => &["channel_id"],
        "register_hook" => &["hook"],
        "add_room_hook" => &["room_id", "hook"],
        "remove_room_hook" => &["room_id", "name"],
        "process_delivery_status" => &["status"],
        "process_webhook" => &["meta", "channel_id"],
        "deliver" => &[
            "room_id",
            "content",
            "channel_id",
            "strategy",
            "metadata",
            "addressed_to",
            "idempotency_key",
            "session_id",
            "chain_depth",
            "instruction",
        ],
        "cancel_delivery" => &["key", "reason"],
        "steer" => &["channel_id", "directive", "room_id"],
        other => {
            return crate::task_calls::arguments(other)
                .or_else(|| crate::store_calls::arguments(other))
                .or_else(|| crate::member_calls::arguments(other))
                .or_else(|| crate::input_calls::arguments(other))
                .or_else(|| crate::verification_calls::arguments(other))
                .or_else(|| crate::strategy_calls::arguments(other))
                .or_else(|| crate::routing_calls::arguments(other))
                .or_else(|| crate::handoff_calls::arguments(other));
        }
    })
}

/// Every member of `scenario` the runner does not know, as `path: member`.
pub(crate) fn unknown_members(scenario: &Value) -> Vec<String> {
    let mut found = Vec::new();
    members(scenario, TOP, "$", &mut found);
    let setup = &scenario["setup"];
    optional(setup, SETUP, "setup", &mut found);
    optional(&setup["kit"], KIT, "setup.kit", &mut found);
    identity(setup, &mut found);
    strategies(setup, &mut found);
    optional(
        &setup["kit"]["delivery_backend"],
        DELIVERY_BACKEND,
        "setup.kit.delivery_backend",
        &mut found,
    );
    let kit = &setup["kit"];
    optional(
        &kit["media_resolver"],
        MEDIA_RESOLVER,
        "setup.kit.media_resolver",
        &mut found,
    );
    optional(
        &kit["attached_media"],
        ATTACHED_MEDIA,
        "setup.kit.attached_media",
        &mut found,
    );
    for (position, channel) in items(&setup["channels"]) {
        self::channel(channel, &format!("setup.channels[{position}]"), &mut found);
    }
    for (position, room) in items(&setup["rooms"]) {
        let path = format!("setup.rooms[{position}]");
        members(room, ROOM, &path, &mut found);
        for (index, binding) in items(&room["bindings"]) {
            members(
                binding,
                BINDING,
                &format!("{path}.bindings[{index}]"),
                &mut found,
            );
        }
    }
    for (position, hook) in items(&setup["hooks"]) {
        self::hook(hook, &format!("setup.hooks[{position}]"), &mut found);
    }
    for (position, step) in items(&scenario["steps"]) {
        self::step(step, &format!("steps[{position}]"), &mut found);
    }
    let expect = &scenario["expect"];
    optional(expect, EXPECT, "expect", &mut found);
    for (label, outcomes) in expect["actions"].as_object().into_iter().flatten() {
        let listed = outcomes.get("$unordered").unwrap_or(outcomes);
        for (index, outcome) in items(listed) {
            self::outcome(
                outcome,
                &format!("expect.actions.{label}[{index}]"),
                &mut found,
            );
        }
    }
    found
}

/// The members of the scripted strategies, and of their install and
/// uninstall (RFC §19.7).
fn strategies(setup: &Value, found: &mut Vec<String>) {
    use crate::strategy_calls::{
        LOOP, STRATEGY, STRATEGY_STEP, STRATEGY_UNINSTALL, SUPERVISOR, SWARM,
    };
    for (position, strategy) in items(&setup["strategies"]) {
        let path = format!("setup.strategies[{position}]");
        members(strategy, STRATEGY, &path, found);
        optional(&strategy["loop"], LOOP, &format!("{path}.loop"), found);
        let team = format!("{path}.supervisor");
        optional(&strategy["supervisor"], SUPERVISOR, &team, found);
        optional(&strategy["swarm"], SWARM, &format!("{path}.swarm"), found);
        for (member, known) in [
            ("install", STRATEGY_STEP),
            ("uninstall", STRATEGY_UNINSTALL),
        ] {
            let at = format!("{path}.{member}");
            optional(&strategy[member], known, &at, found);
            for (index, action) in items(&strategy[member]["do"]) {
                call(
                    action,
                    &["as", "expect"],
                    &format!("{at}.do[{index}]"),
                    found,
                );
            }
        }
    }
}

/// The members of the scripted resolver and the identity hooks (RFC §11).
fn identity(setup: &Value, found: &mut Vec<String>) {
    let resolver = &setup["kit"]["identity_resolver"];
    if !resolver.is_null() {
        let path = "setup.kit.identity_resolver";
        members(resolver, RESOLVER, path, found);
        for (index, answer) in items(&resolver["answers"]) {
            let at = format!("{path}.answers[{index}]");
            members(answer, RESOLVER_ANSWER, &at, found);
            optional(&answer["when"], RESOLVER_WHEN, &format!("{at}.when"), found);
        }
    }
    for (position, hook) in items(&setup["identity_hooks"]) {
        let path = format!("setup.identity_hooks[{position}]");
        members(hook, IDENTITY_HOOK, &path, found);
        for (index, reaction) in items(&hook["reactions"]) {
            let at = format!("{path}.reactions[{index}]");
            members(reaction, IDENTITY_REACTION, &at, found);
            optional(&reaction["when"], WHEN, &format!("{at}.when"), found);
        }
    }
}

fn items(value: &Value) -> impl Iterator<Item = (usize, &Value)> {
    value.as_array().into_iter().flatten().enumerate()
}

/// The members of `value`, an object, that `known` does not list.
fn members(value: &Value, known: &[&str], path: &str, found: &mut Vec<String>) {
    let Some(object) = value.as_object() else {
        found.push(format!("{path}: not an object"));
        return;
    };
    found.extend(
        object
            .keys()
            .filter(|member| !known.contains(&member.as_str()))
            .map(|member| format!("{path}: {member}")),
    );
}

/// As [`members`], for a member the scenario may leave out.
fn optional(value: &Value, known: &[&str], path: &str, found: &mut Vec<String>) {
    if !value.is_null() {
        members(value, known, path, found);
    }
}

fn channel(channel: &Value, path: &str, found: &mut Vec<String>) {
    members(channel, CHANNEL, path, found);
    let human = &channel["human_input"];
    optional(human, HUMAN_INPUT, &format!("{path}.human_input"), found);
    for (index, tool) in items(&human["tools"]) {
        members(
            tool,
            HUMAN_TOOL,
            &format!("{path}.human_input.tools[{index}]"),
            found,
        );
    }
    for kind in [
        "parse",
        "on_event",
        "deliver",
        "deliver_stream",
        "on_room_attached",
        "on_room_detached",
    ] {
        for (index, reaction) in items(&channel[kind]) {
            self::reaction(reaction, &format!("{path}.{kind}[{index}]"), found);
        }
    }
    for kind in ["model", "fallback_model"] {
        scripted_model(&channel[kind], &format!("{path}.{kind}"), found);
    }
    optional(
        &channel["retry_policy"],
        RETRY_POLICY,
        &format!("{path}.retry_policy"),
        found,
    );
    for (index, tool) in items(&channel["tools"]) {
        let tool_path = format!("{path}.tools[{index}]");
        members(tool, TOOL, &tool_path, found);
        optional(
            &tool["raises"],
            RAISES,
            &format!("{tool_path}.raises"),
            found,
        );
    }
    optional(
        &channel["tool_policy"],
        POLICY,
        &format!("{path}.tool_policy"),
        found,
    );
    optional(
        &channel["addresses"],
        ADDRESSES,
        &format!("{path}.addresses"),
        found,
    );
    optional(&channel["agent"], AGENT, &format!("{path}.agent"), found);
    memory(&channel["memory"], &format!("{path}.memory"), found);
    for (room, config) in channel["config_provider"].as_object().into_iter().flatten() {
        turn_config(config, &format!("{path}.config_provider.{room}"), found);
    }
}

fn turn_config(config: &Value, path: &str, found: &mut Vec<String>) {
    optional(config, TURN_CONFIG, path, found);
    for (index, tool) in items(&config["tools"]) {
        members(tool, TURN_TOOL, &format!("{path}.tools[{index}]"), found);
    }
    for (index, tool) in items(&config["tool_handler"]) {
        members(tool, TOOL, &format!("{path}.tool_handler[{index}]"), found);
    }
}

fn memory(memory: &Value, path: &str, found: &mut Vec<String>) {
    optional(memory, MEMORY, path, found);
    for (index, message) in items(&memory["messages"]) {
        members(
            message,
            MEMORY_MESSAGE,
            &format!("{path}.messages[{index}]"),
            found,
        );
    }
    optional(
        &memory["budget_aware"],
        BUDGET_AWARE,
        &format!("{path}.budget_aware"),
        found,
    );
    optional(
        &memory["compacting"],
        COMPACTING,
        &format!("{path}.compacting"),
        found,
    );
    optional(
        &memory["summarizing"],
        SUMMARIZING,
        &format!("{path}.summarizing"),
        found,
    );
    let retrieval = &memory["retrieval"];
    optional(retrieval, RETRIEVAL, &format!("{path}.retrieval"), found);
    for (index, source) in items(&retrieval["sources"]) {
        let source_path = format!("{path}.retrieval.sources[{index}]");
        members(source, SOURCE, &source_path, found);
        for (position, result) in items(&source["results"]) {
            members(
                result,
                SOURCE_RESULT,
                &format!("{source_path}.results[{position}]"),
                found,
            );
        }
    }
    let summarizer = &memory["summarizer"];
    optional(summarizer, MODEL, &format!("{path}.summarizer"), found);
    for (index, round) in items(&summarizer["rounds"]) {
        members(
            round,
            MODEL_ROUND,
            &format!("{path}.summarizer.rounds[{index}]"),
            found,
        );
    }
}

fn hook(hook: &Value, path: &str, found: &mut Vec<String>) {
    hook_of(hook, HOOK, path, found);
}

/// A hook added for one room: it declares no filter and no timeout.
fn room_hook(hook: &Value, path: &str, found: &mut Vec<String>) {
    let known: Vec<&str> = HOOK
        .iter()
        .copied()
        .filter(|member| {
            !["timeout_ms", "channel_types", "channel_ids", "directions"].contains(member)
        })
        .collect();
    hook_of(hook, &known, path, found);
}

fn hook_of(hook: &Value, known: &[&str], path: &str, found: &mut Vec<String>) {
    members(hook, known, path, found);
    for (index, reaction) in items(&hook["reactions"]) {
        self::reaction(reaction, &format!("{path}.reactions[{index}]"), found);
    }
    if let Some(router) = hook.get("router") {
        self::router(router, &format!("{path}.router"), found);
    }
}

/// A conversation router's members, its rules' and their conditions'.
fn router(router: &Value, path: &str, found: &mut Vec<String>) {
    use crate::routing_calls::{CONDITIONS, ROUTER, ROUTING_RULE};
    members(router, ROUTER, path, found);
    for (index, rule) in items(&router["rules"]) {
        let path = format!("{path}.rules[{index}]");
        members(rule, ROUTING_RULE, &path, found);
        optional(
            &rule["conditions"],
            CONDITIONS,
            &format!("{path}.conditions"),
            found,
        );
    }
}

fn reaction(reaction: &Value, path: &str, found: &mut Vec<String>) {
    members(reaction, REACTION, path, found);
    optional(&reaction["when"], WHEN, &format!("{path}.when"), found);
    optional(
        &reaction["refuse"],
        REFUSE,
        &format!("{path}.refuse"),
        found,
    );
    optional(
        &reaction["output"],
        OUTPUT,
        &format!("{path}.output"),
        found,
    );
    let result = &reaction["result"];
    optional(result, RESULT, &format!("{path}.result"), found);
    for (index, item) in items(&result["injected_events"]) {
        members(
            item,
            INJECTED,
            &format!("{path}.result.injected_events[{index}]"),
            found,
        );
    }
    for member in ["do", "after_turn"] {
        for (index, action) in items(&reaction[member]) {
            call(
                action,
                &["as", "expect"],
                &format!("{path}.{member}[{index}]"),
                found,
            );
        }
    }
}

fn step(step: &Value, path: &str, found: &mut Vec<String>) {
    if step.get("call").is_some() {
        call(step, &["as", "background", "expect"], path, found);
        return;
    }
    let kinds: Vec<&(&str, &[&str])> = STEPS
        .iter()
        .filter(|(kind, _)| step.get(*kind).is_some())
        .collect();
    let [(kind, known)] = kinds.as_slice() else {
        let keys: Vec<&String> = step
            .as_object()
            .into_iter()
            .flatten()
            .map(|(key, _)| key)
            .collect();
        found.push(format!("{path}: a step of no known kind: {keys:?}"));
        return;
    };
    members(step, known, path, found);
    if *kind == "await" && step.get("expect").is_some() {
        outcome(&step["expect"], &format!("{path}.expect"), found);
    }
    for (index, concurrent) in items(&step["concurrent"]) {
        call(
            concurrent,
            &["as", "expect"],
            &format!("{path}.concurrent[{index}]"),
            found,
        );
    }
}

fn call(call: &Value, extra: &[&str], path: &str, found: &mut Vec<String>) {
    let name = call["call"].as_str().unwrap_or_default();
    let Some(arguments) = arguments(name) else {
        found.push(format!("{path}: no call {name:?}"));
        return;
    };
    let known: Vec<&str> = arguments
        .iter()
        .chain(extra)
        .chain(&["call", "organization_id"])
        .copied()
        .collect();
    members(call, &known, path, found);
    if call.get("expect").is_some() {
        outcome(&call["expect"], &format!("{path}.expect"), found);
    }
    match name {
        "create_room" => members(&call["room"], NEW_ROOM, &format!("{path}.room"), found),
        "register_channel" => channel(&call["channel"], &format!("{path}.channel"), found),
        "register_hook" => hook(&call["hook"], &format!("{path}.hook"), found),
        "add_room_hook" => room_hook(&call["hook"], &format!("{path}.hook"), found),
        "save_conversation_state" => {
            for (index, transition) in items(&call["transitions"]) {
                let path = format!("{path}.transitions[{index}]");
                members(transition, crate::routing_calls::TRANSITION, &path, found);
            }
        }
        _ => {}
    }
}

fn outcome(outcome: &Value, path: &str, found: &mut Vec<String>) {
    members(outcome, OUTCOME, path, found);
    if outcome["error"].is_object() {
        members(&outcome["error"], ERROR, &format!("{path}.error"), found);
    }
}

/// A scripted model's members, at `path`, and its rounds'.
fn scripted_model(model: &Value, path: &str, found: &mut Vec<String>) {
    optional(model, MODEL, path, found);
    for (index, round) in items(&model["rounds"]) {
        let round_path = format!("{path}.rounds[{index}]");
        members(round, MODEL_ROUND, &round_path, found);
        optional(
            &round["refuses"],
            REFUSES,
            &format!("{round_path}.refuses"),
            found,
        );
        for (position, call) in items(&round["calls"]) {
            members(
                call,
                MODEL_CALL,
                &format!("{round_path}.calls[{position}]"),
                found,
            );
        }
    }
}
