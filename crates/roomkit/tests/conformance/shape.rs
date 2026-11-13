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
    "processes",
];
const PROCESS: &[&str] = &["id"];
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
    "inbound_rate_limit",
    "persistence_policy",
];
const RATE_LIMIT: &[&str] = &["max_per_second", "max_per_minute", "max_per_hour"];
const PERSISTENCE_POLICY: &[&str] = &["persist_types", "exclude_types"];
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
    "reads_usage",
    "reads_loop_end",
    "deliver_stream",
    "on_room_attached",
    "on_room_detached",
    "model",
    "acp",
    "tools",
    "tool_policy",
    "max_tool_rounds",
    "max_empty_retries",
    "max_context_events",
    "tool_timeout_ms",
    "tool_timeouts_ms",
    "turn_budget_tokens",
    "reasoning_effort",
    "response_schema",
    "turn_budget_usd",
    "system_prompt",
    "temperature",
    "max_tokens",
    "config_provider",
    "skills",
    "skills_in_prompt",
    "thinking_budget",
    "enable_thinking",
    "thinking_coalesce_ms",
    "thinking_coalesce_chars",
    "publish_thinking_text",
    "text_coalesce_ms",
    "text_coalesce_chars",
    "publish_text_deltas",
    "tool_call_intents",
    "retry_policy",
    "fallback_model",
    "router",
    "max_author_images",
    "evict_threshold_tokens",
    "describe_empty_event",
    "serialize_room_turns",
    "steer_room_turns",
    "human_input",
    "kit_tools",
    "tool_search",
    "tool_search_pinned",
    "tool_search_threshold",
    "tool_search_threshold_pct",
    "tool_search_miss_hint",
    "tool_search_threshold_tokens",
    "audit",
    "addresses",
    "streams_in",
    "recent_events_window",
    "sender_is_participant",
    "memory",
    "agent",
    "verbs",
];
const ADDRESSES: &[&str] = &["normalizes", "conversation_key", "recipient_at"];
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
    "response_schema",
    "turn_budget_tokens",
    "turn_budget_usd",
    "tool_handler",
    "skills",
];
const TURN_TOOL: &[&str] = &["name", "description", "parameters"];
const SKILL: &[&str] = &["name", "description", "instructions", "references"];
const HUMAN_TOOL: &[&str] = &["name", "description", "parameters"];
const MODEL: &[&str] = &[
    "rounds",
    "streaming",
    "vision",
    "pricing",
    "context_window",
    "name",
    "provider",
    "chat_dialect",
    "deferred_tools",
    "response_schema",
    "response_schema_with_tools",
];
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
    "failure_after_done",
    "stream",
    "whole_response",
];
const MODEL_CALL: &[&str] = &["id", "name", "arguments", "cut", "metadata"];
const TOOL: &[&str] = &[
    "name",
    "description",
    "parameters",
    "tags",
    "changes_state",
    "returns",
    "raises",
    "fails",
    "refuses",
    "declines",
    "hangs",
    "gate",
    "declared",
    "returns_json",
    "writes_record",
    "returns_parts",
];
const RAISES: &[&str] = &["kind", "message"];
const POLICY: &[&str] = &["allow", "deny"];
const MEMORY: &[&str] = &[
    "max_events",
    "returns_current_event",
    "messages",
    "notes",
    "speakers",
    "compacts",
    "writes_record",
    "budget_aware",
    "compacting",
    "summarizing",
    "summarizer",
    "retrieval",
];
const MEMORY_MESSAGE: &[&str] = &["role", "text", "parts"];
const MEMORY_COMPACTION: &[&str] = &["messages", "keep", "fails"];
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
    "store_metadata",
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
    "delay_ms",
    "do",
    "fail",
    "respond",
    "output",
    "result",
    "invalid_result",
    "stop_after",
    "stop_after_rows",
    "hold_after",
    "hold_gate",
    "writes_record",
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
    "provider_result",
];
const PROVIDER_RESULT: &[&str] = &["success", "provider_message_id", "error", "metadata"];
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
    "ingested",
    "compacted",
    "dispatch_turns",
    "observations",
    "thinking_events",
    "tool_events",
    "text_events",
    "tasks",
    "actions",
    "lifecycle",
    "models",
    "acp_wire",
    "acp_permissions",
    "acp_handlers",
    "acp_prompts",
    "acp_mcp_requests",
    "streamed",
    "room_callbacks",
    "participants",
    "resolver",
    "router_calls",
    "ai_routes",
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
    ("acp_disconnect", &["acp_disconnect"]),
];

/// Each call's arguments, beside `call` and `organization_id`.
fn arguments(call: &str) -> Option<&'static [&'static str]> {
    Some(match call {
        "close" | "kit_executor.close" | "kit_executor.closed" => &[],
        "process_inbound" => &["message", "room_id", "defer_delivery"],
        "delivery.cancel" => &["of", "reason", "timeout_ms"],
        "delivery.wait" => &["of"],
        "regenerate_response" => &["room_id", "trigger_id"],
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
        "update_binding_metadata" => &["room_id", "channel_id", "metadata"],
        "set_visibility" => &["room_id", "channel_id", "visibility"],
        "pause_room" | "resume_room" | "reopen_room" | "close_room" | "archive_room"
        | "check_timers" | "regenerate_target" => &["room_id"],
        "set_room_timers" => &["room_id", "timers"],
        "set_agent_response_policy" => &["room_id", "policy"],
        "send_greeting" => &["room_id", "channel_id", "agent_id", "greeting"],
        "register_channel" => &["channel"],
        "unregister_channel" | "acp.close" => &["channel_id"],
        "register_hook" | "kit_executor.hook" | "kit_executor.room_hook" => &["hook"],
        "kit_executor.remove_room_hook" => &["name"],
        "enable_room_hook" | "disable_room_hook" | "remove_room_hook" => &["room_id", "name"],
        "add_room_hook" => &["room_id", "hook"],
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
        "channel_call" => &["channel_id", "room_id", "verb", "arguments"],
        "cancel_delivery" => &["key", "reason"],
        "steer" => &["channel_id", "directive", "room_id"],
        "acp.cancel" => &["channel_id", "room_id"],
        other => {
            return crate::room_queries::arguments(other)
                .or_else(|| crate::task_calls::arguments(other))
                .or_else(|| crate::store_calls::arguments(other))
                .or_else(|| crate::member_calls::arguments(other))
                .or_else(|| crate::input_calls::arguments(other))
                .or_else(|| crate::verification_calls::arguments(other))
                .or_else(|| crate::strategy_calls::arguments(other))
                .or_else(|| crate::routing_calls::arguments(other))
                .or_else(|| crate::handoff_calls::arguments(other))
                .or_else(|| crate::delegate_calls::arguments(other))
                .or_else(|| crate::discussion_calls::arguments(other));
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
    for (position, process) in items(&setup["processes"]) {
        members(
            process,
            PROCESS,
            &format!("setup.processes[{position}]"),
            &mut found,
        );
    }
    optional(
        &setup["kit"]["delivery_backend"],
        DELIVERY_BACKEND,
        "setup.kit.delivery_backend",
        &mut found,
    );
    let kit = &setup["kit"];
    optional(
        &kit["inbound_rate_limit"],
        RATE_LIMIT,
        "setup.kit.inbound_rate_limit",
        &mut found,
    );
    optional(
        &kit["persistence_policy"],
        PERSISTENCE_POLICY,
        "setup.kit.persistence_policy",
        &mut found,
    );
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
    use crate::discussion_calls::DISCUSSION;
    use crate::strategy_calls::{
        LOOP, PIPELINE, STRATEGY, STRATEGY_STEP, STRATEGY_UNINSTALL, SUPERVISOR, SWARM,
    };
    for (position, strategy) in items(&setup["strategies"]) {
        let path = format!("setup.strategies[{position}]");
        members(strategy, STRATEGY, &path, found);
        optional(&strategy["loop"], LOOP, &format!("{path}.loop"), found);
        let team = format!("{path}.supervisor");
        optional(&strategy["supervisor"], SUPERVISOR, &team, found);
        optional(&strategy["swarm"], SWARM, &format!("{path}.swarm"), found);
        optional(
            &strategy["discussion"],
            DISCUSSION,
            &format!("{path}.discussion"),
            found,
        );
        optional(
            &strategy["discussion"]["dispatch"],
            crate::discussion_calls::DISPATCH,
            &format!("{path}.discussion.dispatch"),
            found,
        );
        optional(
            &strategy["pipeline"],
            PIPELINE,
            &format!("{path}.pipeline"),
            found,
        );
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

/// A scripted ACP agent's members (README, "Scripted ACP agents").
fn scripted_acp(acp: &Value, path: &str, found: &mut Vec<String>) {
    use crate::acp_agents::{ACP, AGENT, INITIALIZE, PROMPT, SESSION_REPLY};
    optional(acp, ACP, path, found);
    acp_directories(acp, path, found);
    crate::acp_agents::mcp::validate(acp, path, found);
    crate::acp_agents::handler::validate(acp, path, found);
    crate::acp_agents::thinking::validate(acp, path, found);
    let transport = &acp["transport"];
    optional(
        transport,
        &["refuse_prompt"],
        &format!("{path}.transport"),
        found,
    );
    for (index, refusal) in items(&transport["refuse_prompt"]) {
        optional(
            refusal,
            &["reason", "recovery_authorized", "after_prompt"],
            &format!("{path}.transport.refuse_prompt[{index}]"),
            found,
        );
    }
    let agent = &acp["agent"];
    optional(agent, AGENT, &format!("{path}.agent"), found);
    optional(
        &agent["initialize"],
        INITIALIZE,
        &format!("{path}.agent.initialize"),
        found,
    );
    for kind in ["new_sessions", "set_config_options", "close_sessions"] {
        for (index, reply) in items(&agent[kind]) {
            optional(
                reply,
                SESSION_REPLY,
                &format!("{path}.agent.{kind}[{index}]"),
                found,
            );
        }
    }
    for (index, prompt) in items(&agent["prompts"]) {
        crate::acp_agents::permissions::validate(
            prompt,
            &format!("{path}.agent.prompts[{index}]"),
            found,
        );
        optional(
            prompt,
            PROMPT,
            &format!("{path}.agent.prompts[{index}]"),
            found,
        );
    }
}

fn acp_directories(acp: &Value, path: &str, found: &mut Vec<String>) {
    if let Some(value) = acp.get("additional_directories")
        && !value
            .as_array()
            .is_some_and(|values| values.iter().all(Value::is_string))
    {
        found.push(format!(
            "{path}.additional_directories: expected an array of strings"
        ));
    }
}

fn channel(channel: &Value, path: &str, found: &mut Vec<String>) {
    if let Some(verbs) = channel["verbs"].as_object() {
        for (name, verb) in verbs {
            members(
                verb,
                &["keeps", "reads", "result", "forgets", "gate", "fail", "do"],
                &format!("{path}.verbs.{name}"),
                found,
            );
            for (index, action) in items(&verb["do"]) {
                call(
                    action,
                    &["as", "expect"],
                    &format!("{path}.verbs.{name}.do[{index}]"),
                    found,
                );
            }
        }
    }

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
    ai_router(&channel["router"], &format!("{path}.router"), found);
    scripted_acp(&channel["acp"], &format!("{path}.acp"), found);
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
        let at = format!("{path}.config_provider.{room}");
        match config.as_array() {
            Some(turns) => {
                for (turn, given) in turns.iter().enumerate() {
                    turn_config(given, &format!("{at}[{turn}]"), found);
                }
            }
            None => turn_config(config, &at, found),
        }
    }
    skills(&channel["skills"], &format!("{path}.skills"), found);
}

fn skills(skills: &Value, path: &str, found: &mut Vec<String>) {
    for (index, skill) in items(skills) {
        members(skill, SKILL, &format!("{path}[{index}]"), found);
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
    skills(&config["skills"], &format!("{path}.skills"), found);
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
    for (index, answer) in items(&memory["compacts"]) {
        let at = format!("{path}.compacts[{index}]");
        optional(answer, MEMORY_COMPACTION, &at, found);
        for (message_at, message) in items(&answer["messages"]) {
            members(
                message,
                MEMORY_MESSAGE,
                &format!("{at}.messages[{message_at}]"),
                found,
            );
        }
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
    scripted_model(&memory["summarizer"], &format!("{path}.summarizer"), found);
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
        let permitted = hook["trigger"] == "before_broadcast"
            && hook.get("execution").is_none_or(|mode| mode == "sync")
            && hook.get("router").is_none();
        reaction_with_return(
            reaction,
            &format!("{path}.reactions[{index}]"),
            found,
            permitted,
        );
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
    reaction_with_return(reaction, path, found, false);
}

fn reaction_with_return(reaction: &Value, path: &str, found: &mut Vec<String>, permitted: bool) {
    if let Some(invalid) = reaction.get("invalid_result") {
        invalid_return(reaction, invalid, path, found, permitted);
    }
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
    provider_result(&reaction["output"], &format!("{path}.output"), found);
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
        call(step, &["as", "background", "expect", "kit"], path, found);
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
            &["as", "expect", "kit"],
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
        "kit_executor.hook" | "kit_executor.room_hook" => {
            let known: Vec<&str> = HOOK
                .iter()
                .copied()
                .filter(|field| {
                    !["router", "needs_lock"].contains(field)
                        && (name != "kit_executor.room_hook"
                            || !["channel_ids", "channel_types", "directions"].contains(field))
                })
                .collect();
            hook_of(&call["hook"], &known, &format!("{path}.hook"), found);
        }
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

/// A scripted agent's router (README, `router`): the models it adds, and
/// its picks.
fn ai_router(router: &Value, path: &str, found: &mut Vec<String>) {
    optional(router, &["models", "turns", "retries"], path, found);
    for (name, model) in router["models"].as_object().into_iter().flatten() {
        scripted_model(model, &format!("{path}.models.{name}"), found);
    }
}

/// A scripted model's members, at `path`, and its rounds'.
fn scripted_model(model: &Value, path: &str, found: &mut Vec<String>) {
    optional(model, MODEL, path, found);
    if model
        .get("streaming")
        .is_some_and(|flag| !flag.is_boolean())
    {
        found.push(format!("{path}.streaming: expected a boolean"));
    }
    for (index, round) in items(&model["rounds"]) {
        let round_path = format!("{path}.rounds[{index}]");
        members(round, MODEL_ROUND, &round_path, found);
        if model["streaming"] == false {
            for field in [
                "deltas",
                "thinking",
                "stream",
                "done",
                "failure_after_calls",
                "failure_after_done",
            ] {
                if round.get(field).is_some() {
                    found.push(format!("{round_path}.{field}: requires streaming"));
                }
            }
            if round.get("whole_response").is_some()
                && (round.get("fails").is_some() || round.get("refuses").is_some())
            {
                found.push(format!("{round_path}: whole_response cannot also fail"));
            }
        }
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

/// A deliberate bad return is not a malformed or silently ignored fixture.
fn invalid_return(
    reaction: &Value,
    value: &Value,
    path: &str,
    found: &mut Vec<String>,
    permitted: bool,
) {
    let at = format!("{path}.invalid_result");
    members(
        value,
        &["kind", "tasks", "observations", "injected_events"],
        &at,
        found,
    );
    if !permitted || reaction.get("result").is_some() || reaction.get("fail").is_some() {
        found.push(format!(
            "{at}: requires a SYNC BEFORE_BROADCAST hook without result or fail"
        ));
    }
    if !matches!(
        value["kind"].as_str(),
        Some("no_result" | "missing_payload")
    ) {
        found.push(format!("{at}: unknown or missing kind"));
    }
    if value["kind"] == "no_result" && value.as_object().is_none_or(|object| object.len() != 1) {
        found.push(format!("{at}: no_result cannot carry side effects"));
    }
    for member in ["tasks", "observations", "injected_events"] {
        if value.get(member).is_some_and(|value| !value.is_array()) {
            found.push(format!("{at}.{member}: not an array"));
        }
    }
    for (index, item) in items(&value["injected_events"]) {
        members(
            item,
            INJECTED,
            &format!("{at}.injected_events[{index}]"),
            found,
        );
    }
}

/// Refuse malformed receipts before delivery can turn a bad fixture into an outcome.
fn provider_result(output: &Value, path: &str, found: &mut Vec<String>) {
    let Some(value) = output.get("provider_result").filter(|v| !v.is_null()) else {
        return;
    };
    let at = format!("{path}.provider_result");
    members(value, PROVIDER_RESULT, &at, found);
    let Some(receipt) = value.as_object() else {
        return;
    };
    if !receipt.get("success").is_some_and(Value::is_boolean) {
        found.push(format!("{at}.success: a boolean is required"));
    }
    for key in ["provider_message_id", "error"] {
        if receipt
            .get(key)
            .is_some_and(|v| !v.is_null() && !v.is_string())
        {
            found.push(format!("{at}.{key}: not a string or null"));
        }
    }
    if receipt.get("metadata").is_some_and(|v| !v.is_object()) {
        found.push(format!("{at}.metadata: not an object"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Python: tests/conformance/test_acp_fixture.py::test_directory_fixture_rejects_malformed_lists
    #[test]
    fn test_directory_fixture_rejects_malformed_lists() {
        for directories in [
            json!(null),
            json!("/srv/shared"),
            json!({}),
            json!([1]),
            json!(["/srv/shared", false]),
        ] {
            let scenario = json!({"setup":{"channels":[{"id":"agent","type":"ai","acp":{
                "cwd":"/srv/project", "agent":{}, "additional_directories":directories
            }}]}});
            assert!(
                unknown_members(&scenario)
                    .iter()
                    .any(|problem| problem.contains("additional_directories"))
            );
        }
    }

    #[test]
    fn malformed_provider_receipts_are_refused_before_execution() {
        for receipt in [
            json!({}),
            json!({"success": "yes"}),
            json!({"success": 1}),
            json!({"success": true, "provider_id": "typo"}),
            json!({"success": true, "metadata": []}),
            json!({"success": true, "error": 5}),
            json!({"success": true, "provider_message_id": false}),
            json!([]),
            json!("bad"),
        ] {
            let input = json!({"setup": {"channels": [{"id": "out", "type": "sms", "deliver": [{"output": {"provider_result": receipt}}]}]}});
            assert!(!unknown_members(&input).is_empty(), "{receipt}");
        }
    }

    #[test]
    fn provider_receipts_are_supported_outputs() {
        for receipt in [
            Value::Null,
            json!({"success": true}),
            json!({"success": true, "provider_message_id": "m1", "error": null, "metadata": {"accepted": true}}),
        ] {
            let input = json!({"setup": {"channels": [{"id": "out", "type": "sms", "deliver": [{"output": {"provider_result": receipt}}]}]}});
            assert!(unknown_members(&input).is_empty(), "{receipt}");
        }
    }

    fn scenario(reaction: &Value) -> Value {
        json!({"setup": {"hooks": [{"name": "check", "trigger": "before_broadcast", "reactions": [reaction]}]}})
    }

    #[test]
    fn malformed_invalid_returns_are_not_silently_accepted() {
        for reaction in [
            json!({"invalid_result": {"kind": "typo"}}),
            json!({"invalid_result": {"kind": "no_result", "tasks": []}}),
            json!({"invalid_result": {"kind": "missing_payload", "task": []}}),
            json!({"invalid_result": {"kind": "no_result"}, "result": {"action": "allow"}}),
            json!({"invalid_result": {"kind": "no_result"}, "fail": "error"}),
        ] {
            assert_ne!(unknown_members(&scenario(&reaction)), Vec::<String>::new());
        }
        for member in ["tasks", "observations", "injected_events"] {
            for value in [json!({}), json!(""), json!(5)] {
                let reaction =
                    json!({"invalid_result": {"kind": "missing_payload", member: value}});
                assert_ne!(unknown_members(&scenario(&reaction)), Vec::<String>::new());
            }
        }
    }

    #[test]
    fn invalid_returns_on_unhandled_callbacks_are_refused() {
        let reaction = json!({"invalid_result": {"kind": "no_result"}});
        let mut after = scenario(&reaction);
        after["setup"]["hooks"][0]["trigger"] = json!("after_broadcast");
        let mut observer = scenario(&reaction);
        observer["setup"]["hooks"][0]["execution"] = json!("async");
        let channel =
            json!({"setup": {"channels": [{"id": "sms", "type": "sms", "deliver": [reaction]}]}});
        for invalid in [after, observer, channel] {
            assert_ne!(unknown_members(&invalid), Vec::<String>::new());
        }
    }
}
