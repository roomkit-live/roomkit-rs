# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `RoomKitOptions::close_grace`, Python's `close_grace=5.0`: the one grace
  the kit's close gives its running work before it cuts it, where it was a
  fixed 5 s (RFC §12.10.4).

### Changed

- **BREAKING — a sender whose name reads like an earlier sender's is labelled `Alice (2, not Alice)`**, where the label was `Alice (2)`, as in Python (RFC §6.4, RMK-1835): under the rank alone a model took the later sender for the first more than one time in two. The name after `not` is the label's own, cut within 24 characters. The second nameless sender of a channel keeps `@sms1 (2)`, and the speaker note is unchanged.
- **BREAKING — with `steer_room_turns`, a running turn reads only its own author's next messages**, where it read anyone's, as in Python (RFC §6.4, RMK-1840): someone else's message, read inside a turn that runs in its author's name and role, had its request served under them. It now waits a turn of its own. Someone else is another sender, or the same sender id under another name.
- A tool call whose arguments do not fit its schema is refused naming every
  one of them, where it named the first, as in Python: the required
  arguments missing, the ones a closed schema does not declare, then each
  argument of another type, joined by `; `, several of a kind in one reason
  (`missing required arguments 'title', 'steps'`). A call with one argument
  that does not fit reads as before (RFC §21.1, RMK-1848).

### Fixed

- A `deliver` is no longer put to the identity resolver, as in Python (RFC §11.6, RMK-1874). The framework's own message was resolved like a stranger's: with an `ON_IDENTITY_UNKNOWN` or `ON_IDENTITY_AMBIGUOUS` hook that challenges such senders, the host's own delivery was challenged and blocked, and the agent it addressed never ran; under a resolver answering PENDING it was stored under a `pending-…` id, with a participant record for the framework's sender. The framework knows its own message by where it comes from, not by the sender `system`: a message a caller writes under that name is resolved as before.
- A phone channel writes a number dialled with `00` and the trunk prefix
  `(0)` after its country code, `0033 (0)6 12 34 56 78`, as `+33612345678`,
  and an international number of six digits or fewer, `+12 34 56`, as
  `+123456`, as in Python: the first kept its `0`, the second was kept as
  written (RFC §5.7).

- A file outside a channel's `supported_media_types`, or over its
  `max_media_size_bytes`, reaches it as its text form, alone, in a composite
  or as a native edit's new content, as in Python (RFC §5.8).
- A first post made from the room's work past its cascade's reentry budget (from a delivery, a turn or an ASYNC hook's body, under the room lock, or in an off-lock hook's body) is stored `BLOCKED` with `reentry_loop_cap` and binds nothing: its pass is drawn before the host channel's attach, as in Python (RFC §10.5). A replay of its key binds nothing either.
- A post into a closed room the host channel was detached from raises `ChannelNotFound`, the revocation read before the room's status, as in Python.
- A post into a closed room that never bound the host channel emits `room_refused_event`, as the refusal of a later post does (RFC §5.1, §10.5).
- A room a first message opens takes the kit's default strategy before its `ON_ROOM_CREATED` hooks, whose context holds the agents it attached, as in Python (RFC §9.2, §10.1 step 2, §19.7). A set-up that fails fails the message, with a store error where Python raises the set-up's own (RMK-1811).
- The rows of a streamed answer take the thread parent of the message they answer, rooted as each row commits, as in Python: they were posted at the top level (RFC §10.1 step 8).
- An edit or a delete whose target the store no longer holds at its commit fails whole with `RoomKitError::EventNotFound`, as in Python: it failed with `RoomKitError::Store` (RFC §10.3, §14.3).

## [0.2.0] — 2026-11-18

The second release of roomkit-rs brings most of the Python implementation's
text side to Rust: a PostgreSQL store, the native Anthropic provider and three
more OpenAI-compatible vendors, the Agent Client Protocol channel,
orchestration strategies, delegation and background tasks, identity
verification, human input, memories, Tool Search, and turns run by separate
workers over Redis. Its public API still mirrors Python's, now checked against
roomkit 2eff9ec3 (after 0.115.0). Read the **BREAKING** entries under Changed,
Removed and Security before upgrading from 0.1.0. Applications that use
`roomkit-postgres` must patch the PostgreSQL driver in their workspace's root
`Cargo.toml`, as the README's [PostgreSQL driver](README.md#postgresql-driver)
section shows. A SQLite file opened by 0.2.0 moves to schema version 14, which
0.1.0 refuses to open: back it up before upgrading.

### Added

- **`roomkit-postgres`: a PostgreSQL store, a lock manager across processes
  and a knowledge source**, on Python's tables and advisory-lock keys, so
  processes of both implementations share one database: `PostgresStore`
  (`init`, `migrate`, `POSTGRES_SCHEMA`, `POSTGRES_SCHEMA_VERSION`),
  `PostgresAdvisoryLockManager` and `PostgresKnowledgeSource` for
  `RetrievalMemory`. A call its caller drops cancels its statement on the
  server, and `ConversationStore::connection(work)` keeps one connection for a
  sequence of store calls; a pool you supply must provide
  `PostgresStoreOptions::cancel_query`. **Applications must [pin the patched
  driver in their root manifest](README.md#postgresql-driver)** until the
  cancellation fix ships upstream. `init` refuses a database still on Python's
  legacy v1 (JSONB-blob) schema; `migrate` moves it to the current schema only
  with `MigrateOptions::confirm` set, and drops every v1 table: back that
  database up first.
- **PostgreSQL connection strings read as Python's asyncpg reads them**: Unix
  sockets before `localhost`, host lists, `PGHOST`/`PGPORT`, `~/.pgpass` and
  `PGPASSFILE`, and a 60-second bound per connection. Parameters asyncpg does
  not read (`hostaddr`, `connect_timeout`, `keepalives*`, `channel_binding`,
  `load_balance_hosts`) go to the server as settings, which refuses them, and
  `tcp_user_timeout` is the server's setting, in milliseconds.
- **`roomkit-anthropic`: the native Anthropic Messages provider**, with
  streamed text, images, signed reasoning, tools, structured answers, prompt
  caching, model discovery, per-turn credentials and deferred tools that keep
  their declarations stable across rounds.
- **DeepSeek, MiniMax and Kimi providers** in `roomkit-openai`
  (`DeepSeekAiProvider`, `MiniMaxAiProvider`, `KimiAiProvider`), on the
  vendor's API or behind a base URL, with their catalogues, reasoning replay
  and usage counters; a counter the server leaves out stays unknown instead of
  reading 0.
- **`OpenAiConfig` options**: `max_retries` (0 by default) for retries on
  connection failures, timeouts and retryable statuses; `thinking_field` for
  servers that read earlier reasoning from a named field
  (`reasoning_content`); `supports_reasoning_effort_with_tools` for servers
  behind a `base_url`. `AiProvider::list_models` lists the models an endpoint
  reports.
- **`roomkit-acp`: an Agent Client Protocol agent as a room's intelligence
  channel.** `AcpChannel` spawns an external coding agent
  (`StdioAcpTransport`) or reaches it through your `AcpTransport`, with one
  session per room; it streams the agent's text, thoughts, plans, tool updates
  and usage to the room, prompts it with host context and the conversation it
  missed, and records each turn. It takes the agent's name, role and
  description, used by discussions.
- **ACP session lifecycle, permissions and detach**: initial and host
  configuration through `channel_call`, fixed or per-room MCP servers and
  additional directories on every session, steering and cancellation by turn,
  external tool handlers with approval hooks, rejected permissions kept in
  tool outcomes, one rebuild after an authorized transport refusal, and
  sessions closed on detach without waiting for the agent. Tool arguments,
  results and progress reach room subscribers only with
  `publish_tool_call_text`.
- **Outbound URL validation in `roomkit-provider-http`**, with configurable
  schemes and DNS checks: one unambiguous authority, an ASCII host name
  (Punycode for internationalized domains) and no IPv6 zone identifier.
- **Memories for AI channels**: `AiChannelOptions::memory` takes a
  `MemoryProvider` (`SlidingWindowMemory` by default); `BudgetAwareMemory`,
  `SummarizingMemory` and `CompactingMemory` fit the history to the model's
  window, summarizing what they drop and counting the turn's footprint,
  replayed tool rounds included; `RetrievalMemory` searches
  `KnowledgeSource`s. `MemoryProvider::compact` lets a memory shorten history
  after each tool round; `MockMemoryProvider` and `MockKnowledgeSource` serve
  tests.
- **Model reasoning**: `thinking_budget`, `enable_thinking` and
  `reasoning_effort` per channel, per turn or per binding; reasoning in the
  stream (`StreamItem::Thinking`), in `ON_AI_THINKING` once per closed block
  with its text, and as `THINKING_*` ephemeral events (text only with
  `publish_thinking_text`). A round's reasoning goes back to the model within
  the turn on providers that replay it, but never in a Chat Completions
  request, and on Chat Completions a `<think>` tag is reasoning only when it
  opens a generation.
- **Tool Search**: past `AiChannelOptions::tool_search` thresholds, a turn
  declares its pinned tools plus `find_tools` and `list_tools`, and a tool
  found or used stays declared for the room's next turns. `AiTool::tags` feeds
  the search.
- **Response schemas**: `response_schema` on a channel, a turn or a binding
  holds the answer until it checks against the schema, as a strict
  `json_schema` on Chat Completions or `text.format` on Responses; a provider
  without support refuses before the request (`ResponseSchemaError`).
- **Model catalogue and turn budgets**: `ModelInfo`, `ModelPricing`,
  `AiProvider::available_models`, `catalog_entry` and `context_window`;
  `AiChannelOptions::turn_budget_tokens` and `turn_budget_usd` end a turn past
  its budget, and a priced budget refuses a model with no price
  (`AiError::UnpricedModel`).
- **Provider routing, retries and fallback**: `AiChannel::routed` and
  `Agent::routed` take an `AiRouter`, such as `FallbackRouter`, asked at each
  turn's start and again once a generation's retries under `retry_policy` are
  spent; a turn's later rounds stay on the provider that served it.
- **Per-turn and per-binding settings**: `AiChannelOptions::config_provider`
  returns an `AiChannelTurnConfig` for each turn (system prompt, temperature,
  max tokens, reasoning, tools and their handler, skills, `tool_timeouts`); a
  binding's metadata sets `system_prompt`, `temperature`, `max_tokens`,
  `turn_budget_tokens` and `tools` over the channel's.
  `AiChannelOptions::tool_timeouts` bounds each tool by name.
- **Tool results with images and long results kept out of the window**: a tool
  returns `ToolResult::Parts` (text and images, also from MCP), which a seeing
  model reads as images; an outcome past `evict_threshold_tokens` reaches the
  model as a preview, stored whole for `read_stored_result`. A turn refused as
  too long for the model compacts once and retries.
- **Media authors send**: `RoomKitOptions::media_resolver` and
  `attached_media` give AI channels the bytes of images authors send, placed
  after their captions; `max_author_images` bounds how many a turn sends,
  `describe_empty_event` says what an empty event reads as, and images a
  provider refused are remembered per room.
- **AI loop hooks**: `BEFORE_AI_GENERATION` (can change the turn's context and
  tools), `AFTER_TOOL_ROUND` and `ON_AI_RESPONSE` with `AiGenerationEvent`,
  `ToolRoundEvent` and `AiResponseEvent`, which reports the turn's segments,
  calls, rounds, end reason as text, usage and declared tools.
- **Live tool calls and text for room subscribers**: each round publishes
  `TOOL_CALL_START` and `TOOL_CALL_END` with ids, tools, intents, status and
  duration, and with arguments and a 500-character result preview only when
  `AiChannelOptions::publish_tool_call_text` is set. `publish_text_deltas`
  publishes coalesced `TEXT_DELTA` events for a delegated task's trace.
- **Steering and serialized turns**: `RoomKit::steer(room_id, channel_id,
  directive, SteerOptions)` and `AiChannel::steer` send `InjectMessage`,
  `Cancel` or `UpdateSystemPrompt` into a running turn; `serialize_room_turns`
  runs an agent's turns in a room one after the other, and `steer_room_turns`
  steers a new message into the running turn instead of starting another.
- **Anti-loop guard**: a call repeated with identical arguments is refused at
  its third time, a model that insists ends `force_stopped`, and a tool
  declared with `AiTool::changes_state` resets the counts once it runs.
- **`Agent`, an AI channel with an identity** (`AgentIdentity`: role,
  description, scope, language) written into its system prompt, with
  `greeting` and `auto_greet`; `RoomKit::send_greeting` lets an agent speak
  first. `extra_tools` updates an agent's or channel's host tools for later
  turns.
- **Skills**: served MCP skills (`McpSkillServer`) with verified loading and
  host approvals, skill scripts through a host `ScriptExecutor`
  (`RunSkillScriptTool`), tool gating until a skill is activated
  (`allowed_tools`), per-turn skill registries, in-memory registration with
  `SkillRegistry::add`, and file helpers.
- **MCP**: host requests, resource reads, server metadata and optional
  discovery over stdio and HTTP; the legacy SSE transport
  (`FromUrlOptions::transport`); `FromUrlOptions::connect_timeout` (60 s by
  default); headers and environment as shared handles a host can change
  between connections; `get_tools_as_dicts`. A text-and-image result reaches
  the model as parts.
- **More for AI channels**: opt-in task planning (`plan_tasks`), host-supplied
  sandbox executors, speaking policies, classifiers and thinkers, turn notes
  (`add_turn_note`), host continuation policies, external tool handlers,
  `tool_call_intents_only`, `set_tool_handler`, and `AiChannel::active_turns`.
- **What a turn asks its room's owner**: `TurnHost` posts statuses, updates
  room metadata, adds observations, reads a fresh room context, delegates and
  waits for tasks, and calls host verbs declared with
  `RoomKit::declare_host_verb`. `current_turn_id()`, `tool_turn_context` and
  `turn_report_claim` serve tool code.
- **Participants and identity resolution**:
  `RoomKitOptions::identity_resolver`, the `IdentityResolver` trait and
  identity hooks resolve each sender at step 5 of the inbound pipeline;
  `ensure_participant`, `resolve_participant`, `add_member`, `remove_member`,
  `rename_member` and `is_member` manage members, with the `ON_PARTICIPANT_*`
  triggers.
- **Identity verification**: `request_verification`, `complete_verification`,
  `revoke_verification` and the other verification calls, with
  `VerificationPolicy`, verification records in the stores (secrets hashed
  with scrypt as Python's), `ON_VERIFICATION_REQUIRED` in the inbound
  pipeline, and the release of messages held while a request was open
  (`release_on_verify`).
- **Human input**: `HumanInputHandler` and `HumanInputToolHandler` let an
  agent ask a person mid-turn; `answer_input`, `reject_input` and
  `input_request` answer and read requests, and `ON_USER_INPUT_REQUIRED` can
  refuse them.
- **Content from outside a room**: `RoomKit::deliver` with `BEFORE_DELIVER`
  and `AFTER_DELIVER` hooks, delivery strategies (`Immediate`, `WaitForIdle`,
  `Queued`) and a delivery queue with retries and dead letters;
  `process_delivery_status`, `on_delivery_status` and `process_webhook` route
  a provider's delivery statuses to their room.
- **Cancelling deliveries**: `RoomKit::cancel_delivery` cuts the turns of the
  messages sent under an `InboundMessage::delivery_key`, and
  `DeliveryHandle::cancel(CancelOptions)` cuts a deferred call's turns and
  returns once they are over, or `RoomKitError::Timeout` past its timeout. A
  response cancelled with the reason `SUPERSEDED` stays out of later turns'
  history.
- **Room and kit controls**: `RoomKitOptions::persistence_policy` (event types
  never stored), `inbound_rate_limit`, `set_room_timers`,
  `set_agent_response_policy`, `regenerate_response` and `regenerate_target`,
  `update_room_metadata`, `list_observations`, `submit_feedback`,
  `list_tasks`, `release_channel`, and `report_cascade` on an inbound result.
- **Channel callbacks**: `Channel::on_room_attached`, `on_room_detached`,
  `on_room_event`, `update_binding` (called after mute and access updates; an
  error keeps the stored binding) and `deliver_cut`, which hands a stream cut
  from outside its stored row; a stream ends with `StreamItem::LoopEnd` and
  carries `StreamItem::Usage`. A channel can refuse a missing recipient before
  sending (`RoomKitError::no_recipient`), and `framework_aware(channel)` gives
  a channel a weak handle to its kit.
- **Local channel calls**: `Channel::channel_call` with per-room state
  (`room_state`, `restore_room_state`) that persists across restarts and is
  cleared on detach.
- **Orchestration strategies**: `install_strategy`, `uninstall_strategy` and
  `CreateRoomOptions::orchestration`, with the conversation router and room
  conversation state, handoff and `Swarm`, `Pipeline`, a hold for a human
  (`human_hold`), and greetings on handoff. A room holds one strategy at a
  time; closing or archiving the room uninstalls it.
- **`Supervisor` and `Loop`**: per-worker tools or a `delegate_workers` team,
  sequential or parallel, framework-driven delegation (`auto_delegate`),
  `wait_for_result` and `async_delivery` for background teams; a loop reviewer
  approves only through its `submit_review` verdict.
- **`Discussion`**: several agents taking turns in one room, one at a time,
  with handles, `max_turns`, a dispatch policy (`DispatchPolicy`),
  `listen_only` and `talk_again`, `speak_queue`, regeneration, and processes
  that share one discussion through the store.
- **Delegation to a child room**: `RoomKit::delegate` runs a task inline or on
  the kit's `TaskRunner`, with structured results (`submit_result`,
  `ResultTool`), the task set apart in a `<task>` block and the parent's
  conversation quoted as data, and `ON_TASK_DELEGATED`/`ON_TASK_COMPLETED`;
  agents get the `delegate_task` tool, which can wait for its task from a
  task's room (`wait_in_task_rooms`).
- **Background tasks**: `setup_background` and `setup_task_tools` give an
  agent `work_in_background`, `task_status` and `cancel_task`; `start_task`
  and `cancel_task` act from the host, and a turn's notes list the room's
  recent tasks with their progress (`post_task_progress`).
- **Status bus**: `StatusBus` with its in-memory backend,
  `RoomKit::status_bus()`, `recent_text` for an agent's context, and
  `ON_STATUS_POSTED`.
- **Several processes over one store**: `RoomKitOptions::lock_manager` takes
  any `RoomLockManager` (`InMemoryLockManager` by default, with a warning when
  the store may be shared), and delivery lanes wait for other processes'
  indexes, skipping a gap after `delivery_gap_timeout`.
- **Turn backends and executors**: `TurnBackendHandle` shares a turn transport
  between a kit and standalone workers, with `ExecutorInfo`, executor
  organizations and `TurnExecutor::close()`; workers report application errors
  with their cause chain.
- **`RedisTurnBackend`** (cargo feature `redis`): text turns run on separately
  hosted Python or Rust workers, the room's owner keeping the history and
  resuming a later turn when a worker is lost.
- **Telemetry**: `roomkit::telemetry` with Noop, Mock and Console providers,
  `OpenTelemetryProvider` behind the `opentelemetry` feature, and spans under
  Python's names for inbound, routing, hooks, broadcasts, delegation,
  generations, tool calls and PostgreSQL store operations. Addresses and error
  text stay out unless `record_sensitive_data` is set.
- **Auditing and logging**: tool and session auditors (`JsonlToolAuditor`,
  `JsonlSessionAuditor`) writing Python's JSONL; process-wide content logging
  controls with opt-in DEBUG tool payloads; a failed turn logs one line naming
  its provider and status.
- **Python helpers now public**: the tool-schema, tool-name and Chat
  Completions rendering helpers in `roomkit_ai` (`ToolNameRule`,
  `ChatDialect`, `call_cut`, `stream_call_of`, `tool_call_of`,
  `extract_tools`, `dumps`, `current_tool_actor_id`), `parse_frontmatter`,
  `ToolCallVerdict`, `named_blocks` and `FENCED_TAGS`, `received_events`,
  `RESPONSE_SEGMENT_SEPARATOR`, and the task and orchestration names
  (`BackgroundSetup`, `room_tasks`, `SUBMIT_RESULT`, `BACKGROUND_TOOL`,
  `MissingResult`, `StatusCallback`). So are the text error types
  (`ChannelRefusalError`, `ToolTimeoutError`, `TurnCutShortError`,
  `TaskCutShortError`, `TaskTurnFailedError`) and `RoomKitError::Runtime` and
  `ToolNameCollision`.

### Changed

- **BREAKING — closing an AI channel leaves its provider open.** `AiChannel`'s
  close closed its provider, so a provider shared by several agents was lost
  at the first close; a provider is now its owner's, and neither the channel's
  nor the kit's close touches it. Migration: close your providers
  (`provider.close().await`) once nothing uses them.
- **BREAKING — `AiChannel::provider()` returns an `Option`**, `None` for a
  channel built with `AiChannel::routed`. Migration: unwrap it for a channel
  built with `AiChannel::new`.
- **BREAKING — `AiChannelOptions::temperature` and `AiContext::temperature`
  are an `f64`, 0.7 by default**, where they were an `Option<f64>`; whether a
  model takes a temperature is the provider's to know
  (`OpenAiConfig::supports_custom_temperature`). Migration: write
  `temperature: 0.3` where `temperature: Some(0.3)` was; for a model that
  takes no temperature, where `None` was, set
  `OpenAiConfig::supports_custom_temperature` to `Some(false)`.
- **BREAKING — `OpenAiConfig::max_tokens` is an `Option<u32>`, `Some(1024)` by
  default**; `None` sends no output bound and lets the endpoint decide.
  Migration: write `max_tokens: Some(n)` where `max_tokens: n` was.
- **BREAKING — `AiTool` has three new public fields: `tags` (for Tool Search),
  `changes_state`** (a call that changes what later calls read resets the
  repeat counts) **and `defer_loading`** (Anthropic's deferred tools). A host
  tool may no longer be named `find_tools` or `list_tools` unless
  `tool_search` is `Some(false)`. Migration: name the three fields in an
  `AiTool` literal, or end it with `..AiTool::new(..)?`.
- **BREAKING — a tool can answer text and images**: `ToolResult` has a third
  variant, `Parts`, and `AiToolResultPart::result` is a `ToolResult`.
  Migration: name `ToolResult::Parts` in an exhaustive match; read a result
  part's text with `AiToolResultPart::as_text()`.
- **BREAKING — two enums gained a variant**: `ToolEntry::Schema`, a raw schema
  `extract_tools` reads beside definitions and tool objects, and
  `AiContentPart::Thinking`, a round's reasoning that a provider replays.
  Migration: name the new variants in an exhaustive match; a custom provider
  that renders message parts skips or replays `Thinking`.
- **BREAKING — a `ToolPolicy` is built with
  `ToolPolicy::new(ToolPolicyOptions)`**, which validates it and resolves role
  overrides (`role_overrides`); a policy now applies the waking participant's
  role across tools, search, MCP aliases and skill scripts. Migration: replace
  a `ToolPolicy { allow, deny }` literal with
  `ToolPolicy::new(ToolPolicyOptions { allow, deny, ..Default::default() })?`.
- **BREAKING — a generation cut before its end counts the input its provider
  reported as the stream started.** `StreamEvent::Usage(StreamUsage)` carries
  that report (roomkit-anthropic sends it), handed on as a partial
  `UsageMarker`; a cut generation's record reads `ai_usage_complete: false`.
  Migration: an exhaustive match on `StreamEvent` names its three new
  variants, `Usage`, `ThinkingDelta` and `ToolCallDelta`; code that sums
  `UsageMarker`s skips partial ones.
- **BREAKING — `ErrorRecord` has two new public fields: `attributes`** (a
  provider error's `retryable`, `provider`, `status_code` and
  `context_overflow`, a cut task's `reason`) **and `causes`**, a worker's
  ordered error chain; a `ProviderError` such as a memory's overflow is no
  longer read as retryable. Migration: build one with `ErrorRecord::new`, or
  add `attributes: Box::default()` and `causes: Vec::new()` to a struct
  literal; match it with `..`.
- **BREAKING — `ProviderErrorOptions` has a `before_request` field**
  (`ProviderError::before_request`): a refusal raised before any request, such
  as an unsupported response schema or a tool name a vendor refuses, marks no
  author image refused. Migration: add `before_request: false` to a struct
  literal.
- **BREAKING — `RoomKitError::ChannelNotRegistered` carries its message**:
  `ChannelNotRegistered { channel_id, message }`, worded per call site.
  Migration: match `ChannelNotRegistered { channel_id, .. }` where
  `ChannelNotRegistered(channel_id)` was written.
- **BREAKING — three fields became `Option`s:**
  `RoomKitError::ParticipantNotFound`'s `channel_id` (`None` for a member
  call), `Observation::channel_id` (written `""`, as Python writes an
  observation with no channel), and `FrameworkEvent::RoomRefusedEvent`'s
  `event_id`, `event_type` and `status`. Migration: match `Some(..)` where the
  value was read directly; `Observation::new` takes an id or `None`.
- **BREAKING — more public fields on types built field by field**:
  `AiResponse` (tasks, observations, reasoning), `AiToolCall` and
  `StreamToolCall` (`served`), `AiToolResultPart` (`structured_content`,
  `outcome`, `references`), `ChannelDeclaration` (identity, owner tools, turn
  serialization), `RoomContext` (`history_start_index`), `EphemeralEvent`
  (`room_created_at`), `ToolCallReport` (`refused_but_ran`), and
  `RoomKitError::RemoteTurn` (`detail`). `AiChannel::tool_handler()` returns
  an owned handler. Migration: name the new fields in a literal or start it
  from an existing value, match these types with `..`, and drop a `.cloned()`
  after `tool_handler()`.
- **BREAKING — a turn's response metadata is a `ResponseMetadata`**, a live
  record that memories, hooks and tool handlers write during the turn:
  `InboundResult::response_metadata` and `ChannelOutput::response_metadata`
  both, and a deferred call's `DeliveryHandle::wait` fills the former. An
  agent's `loop_end_reason` and `ai_usage` now sit under `turns.<channel id>`,
  no longer at the record's top level. Migration: read the record with
  `get()`, a copy, and an agent's usage under `turns` (`{"turns": {"bot":
  {"ai_usage": …}}}`); a channel builds the field with `metadata.into()`.
- **BREAKING — `CreateRoomOptions` takes the kit's store type**, as
  `RoomKitOptions` does, to carry a room's orchestration strategy, and
  implements `PartialEq` and `Debug` by hand instead of deriving them.
  Migration: write `CreateRoomOptions<S>` for a kit over a store other than
  `InMemoryStore`.
- **BREAKING — `RealtimeBackend::publish` and `publish_to_room` return a
  `Result`** (`RealtimeFuture<Result<(), ChannelError>>`): an explicit
  publication that fails returns `RoomKitError::Realtime`, text notifications
  log and go on, and local presence retries until close. Migration: a backend
  returns `Ok(())` on success and its error on failure.
- **BREAKING — MCP connection options**: `FromUrlOptions::headers` and
  `FromCommandOptions::env` take optional shared handles (`McpHeaders`,
  `McpEnvironment`) that a host may change between connections, and
  `FromUrlOptions` gains `transport` (legacy SSE) and `connect_timeout`.
  Migration: write `headers: Some(map.into())` and `env: Some(map.into())`,
  and end literals with `..Default::default()`.
- **BREAKING — `SkillRegistry` is shared and fallible**:
  `SkillRegistry::new(SkillRegistryOptions)` (or `default()`) replaces
  `new()`, `copy(names, CopyOptions)` returns a `Result`, `get_skill` returns
  `Result<Option<LoadedSkill>, SkillError>`, reads (`get_metadata`,
  `all_metadata`, `skill_names`) return owned values, and mutations take
  `&self`. Migration: match `LoadedSkill::Local` or `Served`, or use its
  common getters; `api/PYTHON_API.md` lists the rest.
- **BREAKING — `SkillMetadata` has an `allowed_tools` field**: a local skill
  that declares it keeps those tools out of a turn until the skill is
  activated. Migration: add `allowed_tools: None` to a complete
  `SkillMetadata` literal.
- **BREAKING — `default_inbound_event` takes the channel:
  `default_inbound_event(self, &message, &context)`**, and sources the event
  from the channel's id, type and provider; the pipeline keeps the type,
  visibility, address, thread, idempotency key and depth a parse wrote.
  Migration: pass the channel as the first argument.
- **BREAKING — tasks and observations a transport returns from `deliver()` or
  `deliver_stream()` are no longer stored.** The room keeps `on_event()`'s
  tasks, observations and metadata updates, `deliver()`'s metadata updates,
  and a streaming target's failure. Migration: return tasks and observations
  from `on_event()`.
- **BREAKING — a sender left pending gets a record of its own, `pending-`
  followed by 16 hex digits** of the SHA-256 of its channel and address, so a
  sender naming itself after an identity no longer takes that identity's
  record. Records an earlier release left pending keep their ids. Migration:
  read a pending sender's record id from the stored event's source, not from
  the id its channel gave.
- **BREAKING — routing reads a binding's recipient as its channel answers it**
  (`recipient_address`, else the value under `recipient_key`), at routing, on
  attach and on a metadata update, where a channel keeping its recipient under
  an undeclared key let a stranger into its room. The hidden
  `Channel::address_config` is gone and `AddressForm` gains
  `answered_recipient`. Migration: a channel that keeps its recipient under
  its own key declares it through `recipient_key` or answers
  `recipient_address`.
- **BREAKING — a routed message never commits into a room whose binding, made
  as the room was created for it, names someone else.** Once `ON_ROOM_CREATED`
  hooks are over, the room is checked as any routed room: the message is
  routed again, or refused `room_taken` with nothing stored. Migration: name
  the room (`ProcessInboundOptions::room_id`) to deliver into it regardless.
- **BREAKING — the sender `system` is reserved, and a message with no address
  enters a room only through a `group` binding.** A message routed under
  `system` is blocked `reserved_sender`, nothing written; a room opened for an
  address-less message is bound to `system` and admits no other sender until
  the integrator gives it a recipient. Migration: a host writing as `system`
  names its room.
- **BREAKING — an agent keeps the application's instruction its turn read**:
  the kit stores it as the agent's own `EventType::InstructionRead` row,
  visible to that agent alone, and its later turns replay it. `list_events()`
  returns the row and `event_count` counts it. Migration: code that counts or
  lists a room's messages skips `instruction_read` rows.
- **BREAKING — a later turn replays each earlier round of tool calls whole, as
  its turn sent it**, where it replayed the last 8 calls with older results
  cut to a preview, so a server's prompt cache now keeps the conversation's
  prefix. Migration: a caller of
  `roomkit_core::rules::transcript::conversation` drops its `replayed`
  argument.
- **BREAKING — work a hook sends into its own room runs once the hook's body
  is over**: a message, injection, post or greeting sent under the room's lock
  waits in the room's lane until the hook returns, times out or is given up,
  so a hook that waits by its own means (an event, a queue, a callback) for
  the answer to a message it just sent now waits until its timeout. Migration:
  wait for that answer outside the hook body.
- **BREAKING — JSON objects keep their members in the order they were
  written**, as Python's dicts do: a tool's JSON result reaches the model in
  its own order, and stored metadata keeps its keys' order. The workspace
  turns on serde_json's `preserve_order`, and Cargo's feature unification
  turns it on for the whole application: every `serde_json::Map` iterates in
  insertion order, where it iterated sorted. Migration: sort keys explicitly
  where your code relied on sorted maps.
- **BREAKING — failures name their class and provider as Python's**: a
  provider failure reads `ProviderError`, not `ChannelError`, in a task's
  exception, `ON_ERROR`'s `error_type` and a delivery's code, also across the
  turn backend; its text names the provider and status (`openai (401): …`);
  `ON_ERROR`'s payload is an inbound message from the failed channel in the
  trigger's thread, with a failed stream's `correlation_id`, where it was an
  outbound `system` event. Migration: code that matched `ChannelError` for a
  provider's failure matches `ProviderError`, and a hook reads `ON_ERROR`'s
  source as the failed channel.
- **A cut stores what its turn left at once and frees the room's line**: a
  streaming target reads its stream to the end, with no 50 ms grace nor
  5-second bound; `CancelOptions::timeout` bounds only the waiter, and
  `DeliveryStream::cut` tells a cut's end from the response's.
- **A call whose model writes the intent property as `roomkit_intent`**,
  without its leading underscore, has it taken as the call's intent instead of
  refused as an unknown argument, unless the tool declares that name
  (`tool_intent::take_intent` now takes whether it does).
- **An AI channel's `max_context_events` window holds the turn's own event**,
  as Python's: a turn reads one message fewer before its input.
- **JSON the model reads is written as Python's `json.dumps` writes it**: `",
  "` and `": "` separators, `\uXXXX` escapes in a call's arguments, floats as
  Python's `repr` (`1e-05`, ties to even); names quoted in errors and records
  escape what Python's `repr` escapes.

### Removed

- **BREAKING — `DeliveryHandle::request_cancel` is removed**; `cancel` is the
  handle's only cancel. Migration: write
  `handle.cancel(CancelOptions::default()).await?` for
  `handle.request_cancel()` then `handle.wait().await?`; it gives up after its
  `timeout` (5 s by default) with `RoomKitError::Timeout`, and from the room's
  own work it fails with `RoomKitError::Runtime`: cancel there with
  `cancel_delivery`.

### Fixed

- **Inbound messages keep what their channel parsed**: an SMS with media is
  stored `MMS`, an idempotency key read off the wire dedupes a redelivery, a
  channel's echo of its own output stays outbound, and the kit calls a
  channel's `normalize_address`, `conversation_address`, `reply_metadata` and
  `recipient_address` where Python's does. An instruction with an idempotency
  key is refused `instruction_not_idempotent`.
- **Routing and streaming as Python's**: the default router reads a room's
  history on the channel up to the first other sender, over the whole timeline
  (none for a `group` binding); `update_binding_metadata` names its
  correspondent; an answer streams to the first streaming transport the room
  lists, asking each whether it streams in that room.
- **Delivery records and side effects**: a set's failure is recorded on its
  event under the room's lock (`broadcast_partial_failure`, then each success
  and failure); the tasks, observations and metadata updates of each delivery
  set are written once it ends, before its `AFTER_BROADCAST` hooks, and a
  write the store fails is logged while the call goes on.
- **Deferred calls and calls a hook makes into its own room**: such a call
  runs its whole cascade ahead of the event, has its own `process_timeout`,
  and ends with the work that made it when not committed by the event's
  deadline; a handle waits for the streams a cut left unread and reads
  `RoomKitError::Timeout` when a turn never ends; a room lock's failed release
  is the call's error; a cancelled delivery reaches its turn before the caller
  resumes.
- **Hooks**: a SYNC hook outside `BEFORE_BROADCAST` that returns injected
  events has them rejected with a warning; a MODIFY with no payload keeps its
  tasks and observations; observer hooks and framework events start in the
  order they were fired on a multi-thread runtime; a streaming target that
  fails after a cut fires `ON_ERROR`; `ON_TASK_CREATED` receives a
  `task_created` event carrying the source and content of the event its task
  was made for.
- **Chain depth**: a framework-event handler (`RoomKit::on`) that posts at
  each answer keeps the turn's depth and stops at `max_chain_depth` instead of
  looping; a read-only agent's answers are audited `source_read_only`, and a
  muted agent's stream is closed unread.
- **AI turns**: a call's arguments are repaired before their schema check, a
  call with no id gets one, a result says whether its call failed
  (`is_error`), and a stream that ends without its done event completes its
  round; each round's text is its own segment; a second call under an id
  already held is refused; a call that ended before its turn was cut keeps its
  outcome instead of reading `cancelled`; a shared location or a template
  stays in later turns' history.
- **`roomkit-openai`**: `generate` sends one non-streamed completion; an error
  inside a stream or a 200 answer carries its status; a message of parts sends
  its text and images in order; a message holding a call and its result
  renders as the assistant's call; an answer with no choice is an empty answer
  with its usage; usage counts reasoning and cache-write tokens; text read
  before a stream's failure is kept; an answer names its model on
  `metadata["model"]`; a streamed answer may reach 256 MiB.
- **MCP**: `tool_filter` chooses among every tool a server lists before the
  512-tool bound counts; a part with no media type is noted as `image/png`,
  `audio` or `binary`; a successful result with no content reads `[]`.
- **Closing**: closing an AI channel cancels its running tool calls and asks
  the model no more; the kit's close returns within its bound, as a
  `SqliteStore` write holding the file gets one second, where it could hold
  the close for SQLite's 5-second busy timeout; a streaming target that
  stopped reading is cut; a cut streaming-capability query is no longer
  reported as a provider failure.
- **Turn hosts**: a verb after the turn acts in the room's organization, and
  one for another room raises `RoomKitError::Value`, no longer
  `PermissionError`; `current_turn_host()` is `None` once the kit is gone;
  `TurnTimeouts::silent_intervals` rounds a half to even.
- **Smaller fixes**: a circuit breaker in recovery gives its probe to the next
  delivery; `visible_events` reads the deleted marks of the received events
  only; the in-memory realtime backend delivers an event published twice once;
  a store without private state warns once; the kit's clock reads
  microseconds; an address held by an identity record of another organization,
  which `SqliteStore::create_identity` of 0.1.0 could leave, is free again;
  closing or archiving a room clears its pause time, and archiving an open
  room records its close time; `post()` speaks as the record a speaker's id
  names on the host channel; a `SKILL.md` with the Agent Skills
  `allowed-tools` key loads, and the skills preamble ends with Python's note.

### Security

- **BREAKING — a media reference is refused in every spelling a reader decodes
  or trims, in any scheme a reader fetches or opens itself, and when it is
  rooted.** A reference is decoded three times at most and refused when it
  holds a backslash, a control character, a `..` segment or an escape left
  over, when it starts with `~`, a drive letter or a decoded slash, or when
  its scheme names one of the 84 the RFC lists; `file%2Ename.png`,
  `app:///photos/1`, `s3://` and `gs://` stay valid. Migration: a stored event
  whose media URL is now refused fails every read of its room's history;
  before reading those rooms, read the rows directly (PostgreSQL: the events'
  `content` column; SQLite: the `content` key of the events' `data` column),
  check each media URL with `MediaContent::new`, and rewrite or delete the
  events it refuses.
- **More copies of the runtime's marks are replaced in what participants
  write**: a mark split over two text parts or two messages, written with
  punctuation or invisible characters between its words, a closing tag with a
  combining mark or a separator in another form (`</project‐instructions>`),
  the headers over a background hand-back or replayed images. A sender's
  `runtime_record` is dropped, the hand-back carries the new
  `DeliveryItem::runtime_record`, and information separators no longer start a
  line of their own.
- **A room created again under a deleted room's id takes nothing from it**, in
  any process: not its room hooks, enabled hook names, subscriptions or turns,
  nor its running work's answers, tasks and metadata. Publications carry the
  room's creation mark in the new `EphemeralEvent::room_created_at`.
- **Stores no longer tell another organization's caller that an identity
  exists**: `SqliteStore` quotes no unreadable identity record and answers
  `IdentityNotFound` for one of another organization; `create_identity` never
  takes over another organization's id; `None` and an empty organization are
  one organization on every store. A SQLite file migrates to version 14 on
  open, moving unscoped address rows to their identity's organization; 0.1.0
  refuses to open a version 14 file, so back it up before upgrading.
- **MCP: a bare tool name obeys the servers a tool policy names**: in a
  `compose_tool_handlers` chain it runs on the first server the policy admits,
  or on none, where it ran on the first server that had it.
- **`rmcp` 3.9** fixes RUSTSEC-2026-0462, unbounded buffering of a Streamable
  HTTP response; the kit's own client already bounded a response at 4 MiB.

### Known differences

What Rust still lacks against Python, listed as `[[missing]]` in
`api/mapping.toml`; the forms only the language forces are in
`api/PYTHON_API.md`.

- **Media**: voice, video, realtime speech-to-speech and recording, and what
  serves them (voice greetings, a supervisor team serving a realtime voice
  session, a worker's media resolvers), 84 Python names.
- **Tier-1b subsystems** (RMK-1544): protocol traces, event sources and the
  kit's WebSocket helpers (`attach_source`, `connect_websocket`), the provider
  contract behind `TransportChannel`, and an HTTP transport injected into an
  AI provider, 18 names.
- **MCP result validation** against a tool's `outputSchema`, and
  `MCPToolProvider.call_tool_result` (RMK-1732): Rust MCP calls do not check
  structured results against the server's declared schema.
- **Edge cases still open**: when a deferred or adopted call answers
  (RMK-1519, RMK-1523), a delivery record's grant under a lease (RMK-1526),
  and the order of a supervisor's failure reports after its first pass
  (RMK-1569).

## [0.1.0] — 2026-11-01

The first release of roomkit-rs, the Rust implementation of RoomKit: Level 0
of the RFC with nothing left out, on a tokio runtime, an AI channel with
tools, streamed answers, skills and MCP servers, a provider for
OpenAI-compatible endpoints, and a SQLite store on the Python
implementation's schema. Its public API is the Python implementation's
for every feature both have: the same names, parameters, defaults and
behaviour, checked by `make api-parity` against Python's API snapshot (roomkit
b938a855, after 0.111.0). What only the language forces is listed in `docs/architecture.md`,
every behaviour still different in `docs/rfc-gaps.md`, each with its card.

Nothing is published to crates.io: a service takes the crates from the
repository at the tag `v0.1.0`, as git dependencies, with Rust 1.92 or later
(README, "In a service"). The API will change before 1.0.

### Added

- **`roomkit`, the runtime.** `RoomKit` and its `RoomKitOptions`: channels
  registered and attached, rooms through their lifecycle and their timers,
  the inbound and broadcast pipelines (RFC §10) with hooks (`kit.hook`, on
  events, tool calls and session starts; hooks per room), permissions,
  transcoding, the chain-depth limit, routing, edits and deletes,
  instructions, direct injection, framework events (`kit.on`), read markers
  and ephemeral events (typing, presence, reactions), the turn
  backend and the kit's own turn executor, one `close()`. The `Channel`,
  `ConversationStore` and `InboundRoomRouter` traits, `InMemoryStore`,
  `DefaultInboundRoomRouter`, streamed delivery, a transport's retry
  policy, rate limit and circuit breaker, and `roomkit::testing`'s
  `RecordingTransport` and `EchoAgent`. The core's model is re-exported as
  `roomkit::model`.
- **`roomkit-ai`, the AI channel.** `AiChannel` over any `AiProvider`: the
  tool loop with its bounds, `AiTool`, `ToolHandler`, `Tool`,
  `compose_tool_handlers` and `ToolPolicy`, tool-call intents, streamed
  answers, skills from the host's directories, an MCP server's
  instructions, and `MockAiProvider` for tests.
- **`roomkit-openai`.** `OpenAiAiProvider` over an OpenAI-compatible Chat
  Completions endpoint, or stateless Responses (`OpenAiApi::Responses`),
  with streamed text and function tools, Python's model profile, and
  renewable bearer credentials (`TokenSource`).
- **`roomkit-sqlite`.** `SqliteStore`, one SQLite file on the Python
  implementation's schema (version 13; a version 12 file is migrated on
  open), with Python's full-text `search_events`; one writer and four
  read-only connections, for one process at a time.
- **`roomkit-mcp`.** `McpToolProvider`: an MCP server's tools over stdio,
  or Streamable HTTP with `features = ["http"]`, its connection bounded at
  60 seconds.
- **`roomkit-core`**, the sans-IO core the crates run on: the RFC's model,
  rules and pipeline operations, with no I/O and no async runtime. It is no
  service's API; what `roomkit` does not re-export of it may change in any
  0.x release.

### Known differences

`docs/rfc-gaps.md` lists what still differs from the Python
implementation, each with its card (RMK-1070 to RMK-1077, and the review's
RMK-1082 to RMK-1106 under RMK-1081), to close after this release.

[Unreleased]: https://github.com/roomkit-live/roomkit-rs/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/roomkit-live/roomkit-rs/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/roomkit-live/roomkit-rs/releases/tag/v0.1.0
