# Python API mapping

This reference lists the public names whose Rust form needs an explicit mapping.
The machine-readable rules, rationales and Python snapshot fingerprints live in
[mapping.toml](mapping.toml). The pinned Python surface is in [python/](python/).

`make api-parity` compares the Rust documentation with that snapshot and checks
coverage, obsolete mappings, parameter defaults and the names documented here.
When an exception or mapped type changes, review this reference together with
the mapping and its tests. The check does not generate or update this document.

## Language-specific forms

| Rust name | Python name | Members or parameters |
| --- | --- | --- |
| `roomkit::TurnHost::room_context` | `roomkit.TurnHost.room_context` | `Result<Metadata, RoomKitError>` reads fresh owned JSON, including history despite a zero channel window; failures keep the existing kit mapping and store causes. |
| `roomkit::turns::ExecutorInfo`, `roomkit::turns::ExecutorInfoOptions` | `roomkit.turns.ExecutorInfo` | Required string id plus Python defaults; owned optional `BTreeSet<String>` declarations, `Timestamp` heartbeat and cloned snapshots (HITL eb3c6f0b). |
| `roomkit::turns::ExecutorInfo::covers` | `roomkit.turns.ExecutorInfo.covers` | `room_hooks: &[String]` and `organization_id: Option<&str>` use `[]` and `None` for Python's defaults; `Result<bool, RoomKitError>` preserves gated invalid-prefix errors (HITL eb3c6f0b). |
| `roomkit::turns::AgentPresence` | `roomkit.turns.AgentPresence` | Three required fields; owned executor-id list, independent across returned snapshots (HITL eb3c6f0b). |
| `roomkit::RoomKit::agent_presence`, `roomkit::AgentPresenceOptions` | `roomkit.RoomKit.agent_presence` | Owned sorted presence list and optional organization scope; a local kit's `executors()` excludes its own executor (HITL eb3c6f0b). |
| `roomkit::turns::DeclaredVerb` | `roomkit.turns.DeclaredVerb` | Cloneable `new(callback)`/`call` wrapper over owned `Metadata` arguments and `Result<Value, ChannelError>`. Direct invocation preserves errors; a host call rebuilds known kit errors and preserves other recorded classes/messages through `RemoteTurn` (HITL 50d017c9). |
| `roomkit::RoomKit::declare_host_verb`, `roomkit::turns::DeclareHostVerbOptions` | `roomkit.RoomKit.declare_host_verb` | `between_turns: false` by default; synchronous registration refuses closing kits, empty names, built-ins and duplicates. |
| `roomkit::TurnHost::call` | `roomkit.TurnHost.call` | `arguments: Option<Metadata>` uses an empty object for `None`; results cross as `serde_json::Value` and failures use the existing kit error mapping (HITL 50d017c9). |
| `roomkit::CascadeReport` | `roomkit.InboundResult.report_cascade` argument | Borrowed view of `delivery_results`, `unavailable_targets`, `response_metadata`, `response_events`, `error` and `cancelled`; no waiting or cancellation. `report_cascade(&mut self, cascade)` copies collections and their nested values, so later source mutations require another report. Python aliases its delivery mapping and list elements. The target `ResponseMetadata` stays shared; duplicate availability (including `None`), per-channel `turns`, write counts, first error and cancellation replacement follow Python (approved 2026-11-17). |
| `roomkit_ai::SandboxExecutor` | `roomkit.SandboxExecutor` | Shared `Arc<dyn SandboxExecutor>` in `AiChannelOptions::sandbox`; `execute` returns a boxed fallible future and takes explicit optional arguments. Synchronous `tool_definitions` returns `Result<Vec<Metadata>, ToolError>`; `close` follows `ScriptExecutor`'s infallible future convention, with panics aggregated by kit close (approved 2026-11-17). |
| `roomkit_ai::SandboxResult` | `roomkit.SandboxResult` | `new(exit_code)` requires the code; output, error and metadata default empty. `success()` is computed and serialized, never stored independently. (approved 2026-11-17). |
| `roomkit_ai::sandbox::SANDBOX_TOOL_SCHEMAS` | `roomkit.sandbox.SANDBOX_TOOL_SCHEMAS` | Cloneable `LazyLock<Vec<Metadata>>`, read at first use because strings and JSON cannot be constructed by a Rust const (approved 2026-11-17). |
| `roomkit_ai::AiError::SandboxDefinition` | Sandbox catalogue exceptions | Carries the original `ToolError` as its source, preserving the host's class and diagnostic instead of treating it as a value error (approved 2026-11-17). |
| `roomkit_ai::PlanUpdatedEvent`, `roomkit_ai::PlanTaskHandle` | `roomkit.PlanUpdatedEvent` and its task objects | Room and channel ids are unvalidated strings, as in the Python dataclass. The event's `Vec<PlanTaskHandle>` is an independent list of shared, open task objects. Handle `Clone` shares the object; `new` owns its fields, `get` copies all nested JSON, `replace` replaces the object whole, and `with_mut` edits it atomically. Edits through a handle affect later turns; edits to the list do not. The synchronous `with_mut` callback must not reenter that handle. An old handle does not affect a replacement or forgotten plan (approved 2026-11-17). |
| `roomkit_ai::ContinuationPolicy` | `roomkit.ContinuationPolicy` | Shared `Arc<dyn Fn(&str) -> Result<Option<String>, ProviderError> + Send + Sync>`; cloned channels share the callback, returned instructions are owned, and errors preserve their source without provider retry. |
| `roomkit_ai::extract_tools` | `roomkit.tools.extract_tools` | `Vec<ToolEntry>` represents the mixed input list; `Result<(Vec<AiTool>, Option<ToolHandler>), AiError>` represents the definitions, optional handler and schema errors. Raw dictionaries use the added `ToolEntry::Schema(Metadata)` variant (approved 2026-11-17). |
| `roomkit::tools::named_blocks` | `roomkit.tools.named_blocks` | `tags: Option<&[&str]>`: `None` uses the public `FENCED_TAGS`; `Some(&[])` preserves the text; supplied tags keep their order (approved 2026-11-17). |
| `roomkit_ai::RoleOverrideMode` | `roomkit.RoleOverride.mode` | `Restrict` / `Replace` represent the Python literal values (approved 2026-11-16, RMK-1709). |
| `roomkit_ai::RoleOverrideOptions`, `roomkit_ai::ToolPolicyOptions` | `roomkit.RoleOverride`, `roomkit.ToolPolicy` | Constructor keywords with the same defaults, passed to fallible `new`; role maps own their overrides (approved 2026-11-16, RMK-1709). |
| `roomkit_ai::ToolPolicy::resolve` | `roomkit.ToolPolicy.resolve` | `role: Option<&str>`, default `None`; `Result<Cow<'_, Self>, AiError>` borrows for absent/empty/unknown roles and owns a resolved policy otherwise (approved 2026-11-16, RMK-1709). |
| `roomkit_ai::ToolPolicy::as_filter` | `roomkit.ToolPolicy.as_filter` | A predicate borrowing the policy; mutation waits until its borrow ends. A static host callback can own an `Arc<ToolPolicy>` and call the predicate inside its move closure (approved 2026-11-16, RMK-1709). |
| `roomkit_ai::parse_tool_names` | `roomkit.skills.parse_tool_names` | `value: &serde_json::Value`: text/arrays read as Python; other values give no names. Tuples use arrays; non-JSON Python objects are supplied as their Python text (HITL 63fe8e98). |
| `roomkit_ai::serves_exactly` | `roomkit.skills.serves_exactly` | `offered`: an iterable of borrowed or owned names; exact membership (HITL 63fe8e98). |
| `roomkit_ai::missing_required_tools` | `roomkit.skills.missing_required_tools` | `closed: &[String]`, empty by default; explicit `r#match: Option<&RequiresMatch>`, `None` for exact matching; fallible result preserves callback errors (HITL 63fe8e98). |
| `roomkit_ai::safe_join_filename` | `roomkit.skills.safe_join_filename` | `kind: Option<&str>`, `None` for `"file"`; `Result<PathBuf, SkillPathError>` preserves the path error (HITL 63fe8e98). |
| `roomkit_ai::tool_turn_context` | `roomkit.tools.tool_turn_context` | `work`: scoped future replacing the enclosed `with` block (HITL 47e0cee0) |
| `roomkit_ai::turn_report_claim` | `roomkit.tools.turn_report_claim` | `Option<Arc<dyn Fn() -> bool + Send + Sync>>` owns the captured turn's report book. Clones share its claim; invocation resolves the call id in that turn, including a later round. Unreported calls remain claimable after completion (approved 2026-11-17). |
| `roomkit_ai::ToolStructuredContent` | — | Whole type |
| `roomkit_ai::AiToolHandle` | `AIChannel.extra_tools`, `Agent.extra_tools` elements | Shared `get`/`replace` handles; reads and prepared provider contexts are snapshots (approved 2026-11-17). |
| `roomkit_ai::ExternalToolHandler` | — | `state` |
| `roomkit_ai::ExternalToolHandlerState` | — | Whole type |
| `roomkit_ai::ExternalToolFuture` | — | Whole type |
| `roomkit_ai::FireBeforeHookOptions` | — | Whole type |
| `roomkit_ai::FireOnToolHookOptions` | — | Whole type |
| `roomkit::ConversationStore` | — | `is_process_local` |
| `roomkit::ConversationStore` | — | `set_telemetry` |
| `roomkit::store::ConversationStore` | — | `set_telemetry` |
| `roomkit::store::ConversationStore` | — | `is_process_local` |
| `roomkit::StoreError` | — | `Unavailable`, `unavailable` |
| `roomkit::model::ResponseMetadata` | — | `get`, `contains_key`, `len`, `is_empty`, `insert`, `pop`, `setdefault`, `update`, `clear`, `with_value`, `to_metadata` |
| `roomkit_openai::TokenSource` | — | Whole type |
| `roomkit_openai::TokenFuture` | — | Whole type |
| `roomkit_openai::TokenError` | — | Whole type |
| `roomkit_openai::OpenAiApi` | — | Whole type |
| `roomkit_openai::OpenAiConfig` | — | `api`, `token_source`, `tool_namespace` |
| — | `roomkit.ChannelOutput.__init__` | `response_stream` |
| — | `roomkit.TurnHost` | `__init__` |
| — | `roomkit.DeliveryHandle` | `__init__` |
| `roomkit::HookError` | — | Whole type |
| `roomkit::HookFuture` | — | Whole type |
| `roomkit::HookHandler` | — | Whole type |
| `roomkit::HookReturn` | — | Whole type |
| `roomkit::HookPayload` | — | Whole type |
| `roomkit::IntoHookReturn` | — | Whole type |
| `roomkit::RoomKit::hook` | — | `handler` |
| `roomkit::RoomKit::on` | — | `handler` |
| `roomkit::RoomKit` | — | `hook_handler` |
| `roomkit::RoomKit` | — | `downgrade` |
| `roomkit::StoreFailure` | — | Whole type |
| `roomkit::realtime::RealtimeFailure` (also `roomkit::RealtimeFailure`) | — | Owns a publication's original error, message and cause; `Deref`, `AsRef`, `into_inner`; identity equality |
| `roomkit::RoomKitError::Realtime` | Backend exception | A recoverable explicit publication failure; preserves the backend error |
| `roomkit::TurnHost` | — | `respond_with_stream` |
| `roomkit::WeakRoomKit` | — | Whole type |
| `roomkit::default_inbound_event` | — | Whole type |
| `roomkit::keep_context` | — | Whole type |
| `roomkit::RoomKitError` | — | `Store`, `Lock`, `Channel`, `Router`, `Identity` |
| `roomkit::RoomKitError` | — | `Internal` |
| `roomkit::model::ErrorRecord` | — | Whole type |
| `roomkit_ai::AiContentPart` | — | Whole type |
| `roomkit_ai::ToolResultPart` | — | Whole type |
| `roomkit_ai::ProviderError` | — | `with_source` |
| `roomkit_ai::ProviderStream` | — | Whole type |
| `roomkit_mcp::McpError` | — | Whole type |
| `roomkit::RoomKitError` | — | `code`, `retryable` |
| `roomkit::model::ChannelType` | — | `new`, `as_str` |
| `roomkit::RoomKitOptions` | — | `with_store` |
| `roomkit::realtime::RealtimeFuture` | — | Whole type |
| `roomkit::orchestration::StrategyFuture` | — | Whole type |
| `roomkit_ai::Agent::new` | — | `provider` |
| `roomkit_ai::AiChannel::new` | — | `provider` |
| — | `roomkit.AIChannel.__init__` | `router` |
| — | `roomkit.Agent.__init__` | `router` |
| `roomkit_ai::AiChannel` | — | `routed` |
| `roomkit_ai::Agent` | — | `routed` |
| `roomkit_ai::Routed` | — | Whole type |
| `roomkit_ai::AiRouterFuture` | — | Whole type |
| `roomkit_ai::Agent::channel` | — | Whole type |
| `roomkit::orchestration::StrategyAgent` | — | Whole type |
| `roomkit::RouterFuture` | — | Whole type |
| `roomkit::MockDispatchPolicy` | — | `turns` |
| `roomkit::orchestration::MockDispatchPolicy` | — | `turns` |
| `roomkit::orchestration::strategies::discussion::MockDispatchPolicy` | — | `turns` |
| `roomkit::orchestration::DispatchFuture` | — | Whole type |
| `roomkit::identity::IdentityFuture` | — | Whole type |
| `roomkit::RoomKit::identity_hook` | — | `handler` |
| `roomkit::Channel` | — | `sender_is_participant` |
| `roomkit::Channel` | Optional local attributes read by Python Discussion | `name`, `role`, `description`: `Option<String>` across channel erasure; ACP's declaration remains without an Agent identity (HITL 132f374b) |
| `roomkit::ChannelOf` | — | Whole type |
| `roomkit::RegisteredChannel` | — | Whole type |
| `roomkit::testing::Environment` | — | Whole type |
| `roomkit::testing::SequentialEnvironment` | — | Whole type |
| `roomkit::testing::kit_with_environment` | — | Whole type |
| `roomkit::Channel` | — | `channel_type`, `category`, `direction` |
| `roomkit::HandoffHandler` | — | `set_greeting_map`, `set_agents`, `set_on_handoff_complete` |
| `roomkit::orchestration::HandoffHandler` | — | `set_greeting_map`, `set_agents`, `set_on_handoff_complete` |
| `roomkit::Swarm::new` | — | `entry` |
| `roomkit::orchestration::Swarm::new` | — | `entry` |
| `roomkit::orchestration::strategies::Swarm::new` | — | `entry` |
| `roomkit_ai::orchestration::HANDOFF_TOOL` | — | Whole type |
| `roomkit_ai::tasks::DELEGATE_TOOL` | — | Whole type |
| `roomkit::orchestration::OnHandoffComplete` | — | Whole type |
| `roomkit::orchestration::DoneFn` | — | Whole type |
| — | `roomkit.HookEngine` | `FAIL_CLOSED_TRIGGERS`, `__init__`, `drain_observers`, `fail_closed_hook`, `has_hooks_for`, `has_off_lock_hooks`, `has_sync_hooks`, `holds`, `remove_hook`, `run_async_hooks`, `run_observers`, `run_sync_hooks`, `running_observers` |
| `roomkit::TurnExecutor::hook` | — | `fn` |
| `roomkit::TurnExecutor::room_hook` | — | `fn` |
| — | `roomkit.PendingInput.__init__` | `_event`, `_withdrawn` |
| — | `roomkit.DeliveryContext.__init__` | `_voice_sessions`, `_voice_channel`, `_wait_for_turn` |
| — | `roomkit.ResponseMetadata.__init__` | `initial` |
| — | `roomkit.providers.ai.AIContext` | `model_copy` |
| — | `roomkit.providers.ai.AITextPart.__init__` | `type` |
| — | `roomkit.providers.ai.AIThinkingPart.__init__` | `type` |
| — | `roomkit.providers.ai.AIImagePart.__init__` | `type` |
| — | `roomkit.providers.ai.AIToolCallPart.__init__` | `type` |
| — | `roomkit.providers.ai.AIToolResultPart.__init__` | `type` |
| `roomkit_ai::SkillRegistry` | — | `tool_definitions` |
| `roomkit_ai::SkillRegistry::copy` | — | `names` |
| `roomkit_ai::RequiresMatch` | `roomkit.skills.RequiresMatch` | Callable wrapper: synchronous `new`/`call`, borrowed names, fallible result; clones share callback identity (HITL 73da84cd) |
| `roomkit_ai::SkillRegistry.requires_match` | `SkillRegistry.requires_match` | Public instance attribute, not recorded by the signature snapshot (HITL 73da84cd) |
| `roomkit_ai::SkillRegistry::{gated_tool_names,closed_tool_names}` | Same names | Explicit `activated` set; empty means Python's default `()` (HITL 73da84cd) |
| `roomkit_ai::SkillToolNames` | `SkillMetadata.allowed_tools: str \| list[str] \| None` | Whole type: `Text(String)` or `List(Vec<String>)`, wrapped in `Option` (HITL ed472959) |
| `roomkit_ai::ImageResolver` | — | Whole type |
| `roomkit_openai::OpenAiConfig` | — | `vision` |
| `roomkit_ai::ToolEntry` | — | Whole type |
| `roomkit_ai::ToolResult` | — | Whole type |
| `roomkit_ai::DynAiProvider` | — | Whole type |
| `roomkit::RoomLock` | — | Whole type |
| `roomkit_ai::ToolFuture` | — | Whole type |
| — | `roomkit.Tool.__init__` | `args`, `kwargs` |
| `roomkit_ai::AiChannel` | — | `set_tool_handler` |
| `roomkit_ai::AiChannelOptions` | — | `image_resolver` |
| `roomkit_acp::AcpConnection` | — | Whole type |
| `roomkit_acp::ClientSideConnection` | — | Whole type |
| `roomkit_acp::AcpClient` | — | Whole type |
| `roomkit_acp::AcpError` | — | Whole type |
| `roomkit_acp::McpServers` | `ACPChannel(mcp_servers=)` | Fixed SDK list or per-room callback enum; `From<Vec<McpServer>>` (HITL 5ce911d3) |
| `roomkit_acp::McpServersForRoom` | `MCPServersForRoom` | Named callback returning a future; sync suppliers use `future::ready` (HITL 5ce911d3) |
| `roomkit_acp::AcpContextContributor` | `ACPContextContributor` | Cloneable `new(callback)`, owned context/event and `Result<Vec<String>, AcpError>`; one string becomes `vec![text]`, invalid dynamic returns are unrepresentable (HITL 52031019) |
| — | `roomkit.ACPTransport.open` | `queue` |
| `roomkit_mcp::McpToolProvider::new` | `MCPToolProvider(url=None)` | Explicit `Option<&str>` uses `None` for Python's omitted URL (HITL e4ef09db). |
| `roomkit_mcp::McpHeaders` | `MCPToolProvider(headers=)` | Named shared handle with atomic updates, owned snapshots and ordered case-sensitive keys (HITL e4ef09db). |
| `roomkit_mcp::McpEnvironment` | `MCPToolProvider(env=)` | Named shared handle with atomic updates and per-connection owned snapshots (HITL e4ef09db). |
| `roomkit_mcp::McpToolProvider` | — | `connect`, `close` |
| `roomkit_mcp::McpToolProvider` | `roomkit.tools.MCPToolProvider` | `request(method, params: Option<&Metadata>, RequestOptions)`, `read_resource(uri, ReadResourceOptions)`; timeout defaults to 30 s. Host snapshots are owned values returned by methods (HITL 70a4bdc7). |
| `roomkit_mcp::McpToolProvider::read_resource` | `MCPToolProvider.read_resource` | `Metadata` keeps the SDK model's complete resource JSON, including extensions and explicit nulls; URI validation/normalization is preserved (HITL 3de23e53). |
| `roomkit_ai::MockAiProvider` | — | `calls` |
| `roomkit_ai::memory::MockMemoryProvider` | — | `retrieve_calls`, `ingest_calls`, `clear_calls`, `closed` |
| `roomkit_ai::knowledge::MockKnowledgeSource` | — | `search_calls`, `index_calls`, `closed` |
| `roomkit_ai::ThinkingBlocks` | — | `last_signature` |
| `roomkit_ai::RoundTranscript` | — | `thinking` |
| `roomkit_ai::ToolError` | — | Whole type |
| `roomkit_ai::SkillFileError` | Exceptions from `Skill` file helpers | Whole type: `Path(SkillPathError)`, `Io(std::io::Error)`, `Encoding(FromUtf8Error)` (HITL 45d6fce8) |
| `roomkit_ai::ScriptExecutor` | `ScriptExecutor` | Borrowed skill/name and `Option<&Metadata>` (`None` for omitted arguments), boxed futures, `ToolError` for execute failures; `close` follows `Channel::close` (HITL 213974fa) |
| `roomkit_ai::ScriptExecutor::execute` | `roomkit.ScriptExecutor.execute` | Explicit `None` for Python's omitted `arguments`; `Some` borrows its mapping (HITL 213974fa) |
| `roomkit_ai::ScriptResult` | `roomkit.skills.ScriptResult` | `new(exit_code: i64)` requires the code; remaining public fields use Python's defaults, `success=true` independent of that code; serde JSON (HITL 213974fa) |
| `roomkit_ai::Skill::{list_scripts,list_references}` | Same names | `Vec<OsString>` preserves native filenames, including non-UTF-8 bytes; sorted as Python's decoded names (HITL 45d6fce8) |
| `roomkit_ai::SkillEntry`, `SkillResource` | Same names | Non-validating constructors and frozen outer fields exposed by borrowed accessors. Frontmatter is owned, editable through `frontmatter_mut`; copying a retained map does not preserve Python's external alias (HITL 450fa510). |
| `roomkit_ai::SkillResources`, `SkillResourceSize` | Entry resources and resource size | `Listed(Vec<SkillResource>)` / `Dynamic`; integer inputs from i64 through u64's range, including smaller primitives, with exact i128 size/total accessors. No arbitrary-precision JSON expansion (HITL 450fa510). |
| `roomkit_ai::SkillEntryKeyError` | `KeyError` | Fallible `name`/`description` return Python's string value, or the missing `key`; no validation error or panic for missing keys (HITL 450fa510). |
| `roomkit_ai::McpSkillServer` | `MCPSkillServer` | Three-method trait with borrowed boxed futures and owned JSON snapshots. Errors retain their concrete cause; a resource result may be any JSON value. The MCP adapter uses the provider's existing 30-second defaults (HITL 766277b2). |
| `roomkit_ai::LoadedSkill` | `Skill` or `MCPSkill` | Owned `Local`/`Served` snapshots, explicit common getters and six dispatched file helpers; no `Deref` (approved 2026-11-17). |
| `roomkit_ai::ApproveExecution` | `ApproveExecution` | Cloneable asynchronous callback; clones retain identity and errors retain their concrete cause. |
| `roomkit_ai::SkillError::{ServerDiscovery,ServerUnavailable,VerificationCaused,NotFound,File}` | Discovery/verification errors, `KeyError`, file errors | Sanitized public messages with concrete causes, missing name and existing typed file error (approved 2026-11-17). |
| `roomkit_ai::SkillError::Changed::source` | `SkillChangedError.__cause__` | Optional retained cause alongside the public name (approved 2026-11-17). |

| `roomkit_ai::McpSkillServerFuture`, `McpSkillServerError` | Protocol awaitables and exceptions | Boxed `Send` futures borrow one call's inputs; boxed errors retain concrete transport or host errors for downcasting (HITL 766277b2). |
| `roomkit::RoomKit::start` | — | Whole type |
| `roomkit::telemetry::SpanGuard` | — | Whole type |
| `roomkit::telemetry::MockTelemetryProvider` | — | `completed_spans`, `metrics` |
| `roomkit::telemetry::Attr` | — | `PROVIDER`, `MODEL`, `ROOM_ID`, `SESSION_ID`, `CHANNEL_ID`, `TTFB_MS`, `DURATION_MS`, `STT_TEXT`, `STT_TEXT_LENGTH`, `STT_CONFIDENCE`, `STT_IS_FINAL`, `STT_MODE`, `TTS_VOICE`, `TTS_CHAR_COUNT`, `TTS_TEXT_LENGTH`, `LLM_INPUT_TOKENS`, `LLM_OUTPUT_TOKENS`, `LLM_TOOL_COUNT`, `LLM_STREAMING`, `HOOK_NAME`, `HOOK_TRIGGER`, `HOOK_RESULT`, `PIPELINE_FRAME_COUNT`, `PIPELINE_BYTES_PROCESSED`, `PIPELINE_STAGES`, `PIPELINE_FRAMES`, `REALTIME_PROVIDER`, `REALTIME_TOOL_NAME`, `REALTIME_TOOL_DENIED`, `DELIVERY_CHANNEL_TYPE`, `DELIVERY_RECIPIENT`, `DELIVERY_SUCCESS`, `DELIVERY_ERROR`, `DELIVERY_MESSAGE_ID`, `STORE_OPERATION`, `STORE_TABLE`, `BACKEND_TYPE`, `DELEGATION_TASK_ID`, `DELEGATION_WORKER_ID`, `DELEGATION_CHILD_ROOM_ID`, `DELEGATION_PARENT_ROOM_ID`, `DELEGATION_STATUS`, `DELEGATION_MODE` |
| `roomkit_ai::CallIds` | — | `call` |
| — | `roomkit.PendingInput.__init__` | `status`, `result`, `reject_reason` |
| — | `roomkit.ACPSessionInvalidatedError.__init__` | `reason` |

## Types represented by language rules

`ToolTurnContextOptions` carries the seven Python keywords with the same defaults.
Its depth is `u32` (`0..=4294967295`); Python's helper also accepts negative and
arbitrarily large integers. Its room is an immutable `Arc<Room>`: an explicit
scope retains that same allocation, but this does not promise object identity
with Rust callbacks' by-value `RoomContext.room` copies. These representation
limits were approved in HITL 47e0cee0.

The explicit installer is covered by `tests/tool_turn_context.rs`. Local AI turns
install their room snapshot, actor and response depth before configuration and
memory retrieval, after ingestion. Their streams inherit the snapshot and record
with a separate live depth; preparation's children are never revived. Tool calls
and generation, response, tool-call and human-input observers retain those readers. Native
tests in `tests/ai_turn_context.rs` cover suspension, interleaving and cancellation.
Local AI turns also expose their policy-filtered catalogue before memory retrieval,
including Tool Search-hidden tools. Generation-hook adoption and
round withdrawals refresh it; new round declarations are shared with preparation,
while consumption's withdrawals stay local to its context. The shared
`tool-allowed-names/` scenarios and native catalogue lifetime tests cover this.
Requester attribution, role-specific policy overrides and the reasoning-backend
driven-turn path remain separate integration work. Skill-gated tools stay in the
allowed catalogue while their declarations, discovery and execution are closed
until activation succeeds. The shared `skill-gating/` scenarios cover local text
turns, including deferred providers; prerequisites and availability remain work
under RMK-1709. `SkillMetadata::new(name, description)` defaults `allowed_tools`
to `None`. Existing complete struct literals must add `allowed_tools: None`, or
use `..SkillMetadata::new(name, description)` (approved in HITL ed472959).

| Rust type | Python representation |
| --- | --- |
| `roomkit::ChannelError` | `Exception`; members: none |
| `roomkit::RouterError` | `Exception`; members: none |
| `roomkit::identity::IdentityError` | `Exception`; members: none |
| `roomkit::orchestration::DispatchError` | `Exception`; members: none |
| `roomkit::DeliveryBackendError` | `Exception`; members: none |
| `roomkit::model::RoomId` | `str`; members: `new`, `as_str` |
| `roomkit::model::EventId` | `str`; members: `new`, `as_str` |
| `roomkit::model::ChannelId` | `str`; members: `new`, `as_str` |
| `roomkit::model::ParticipantId` | `str`; members: `new`, `as_str` |
| `roomkit::model::IdentityId` | `str`; members: `new`, `as_str` |
| `roomkit::model::TaskId` | `str`; members: `new`, `as_str` |
| `roomkit::model::ObservationId` | `str`; members: `new`, `as_str` |
| `roomkit::CountryCode` | `str`; members: `new`, `as_str` |
| `roomkit::model::ChannelSet` | `str`; members: `new`, `contains`, `iter` |
| `roomkit::model::Timestamp` | `datetime`; members: `from_unix_millis`, `as_unix_millis`, `checked_add`, `duration_since`, `UNIX_EPOCH` |
| `roomkit_ai::Date` | `date`; members: `new`, `year`, `month`, `day` |
| `roomkit::model::Metadata` | `dict`; members: none |
| `roomkit::model::Confidence` | `float`; members: `new`, `value`, `FULL` |
| `roomkit::model::Latitude` | `float`; members: `new`, `degrees` |
| `roomkit::model::Longitude` | `float`; members: `new`, `degrees` |
| `roomkit::model::ModelError` | `ValueError`; members: none |
| `roomkit::InvalidCountryCode` | `ValueError`; members: none |
| `roomkit_ai::AiError` | `ValueError`; members: none |
| `roomkit::DeliveryStream` | `AsyncIterator`; members: `next`, `cut` |
| `roomkit::ResponseStream` | `AsyncIterator`; members: none |
| `roomkit::StreamItem` | `str`; members: none |
| `roomkit::ResponseItem` | `str`; members: none |
| `roomkit_ai::TextStream` | `AsyncIterator`; members: none |
| `roomkit_ai::AiMessageContent` | `str`; members: none |
| `roomkit::model::HookAction` | Values of `roomkit.HookResult.__init__(action)`: `allow`, `block`, `modify` |
| `roomkit::model::DeliveryResultStatus` | Values of `roomkit.DeliveryResult.__init__(status)`: `sent`, `queued`, `failed` |
| `roomkit::model::DeliveryOutcomeStatus` | Values of `roomkit.DeliveryOutcome.__init__(status)`: `queued`, `sent`, `blocked`, `unavailable`, `failed`, `unknown` |
| `roomkit::model::RichFormat` | Values of `roomkit.models.RichContent.__init__(format)`: `html`, `markdown` |
| `roomkit::model::ToolCallStatus` | Values of `roomkit.ToolCallContent.__init__(status)`: `pending`, `completed`, `failed` |
| `roomkit_ai::AiRole` | Values of `roomkit.providers.ai.AIMessage.__init__(role)`: `system`, `user`, `assistant`, `tool` |
| `roomkit::model::LoopEndReason` | Values of `roomkit.models.LoopEndMarker.__init__(reason)`: `completed`, `max_rounds`, `timeout`, `budget_exceeded`, `truncated`, `empty_response`, `unfinished`, `force_stopped`, `cancelled`, `error` |
| `roomkit_ai::GenerationPurpose` | Values of `roomkit.AIGenerationEvent.__init__(purpose)`: `answer`, `thought` |
| `roomkit_ai::ToolDeclarationOrigin` | Values of `roomkit.DeclaredTool.__init__(origin)`: `always`, `pinned`, `sticky`, `revealed` |
| `roomkit_ai::ResponseSchemaFailure` | Values of `roomkit.ResponseSchemaError.__init__(reason)`: `unsupported`, `refusal`, `truncated`, `invalid_json` |
| `roomkit::RoomKitError` | `roomkit.RoomNotFoundError`, `roomkit.RoomAlreadyExistsError`, `roomkit.RoomClosedError`, `roomkit.TransitionRefusedError`, `roomkit.ProcessTimeoutError`, `roomkit.ChangeRefusedError`, `roomkit.ReentrantCallError`, `roomkit.ReentryLoopCapError`, `roomkit.ChannelNotFoundError`, `roomkit.ChannelNotRegisteredError`, `roomkit.ChannelAlreadyRegisteredError`, `roomkit.ParticipantNotFoundError`, `roomkit.IdentityNotFoundError`, `roomkit.AddressLinkedError`, `roomkit.EventNotFoundError`, `roomkit.HumanInputRejectedError`, `roomkit.ToolNameCollisionError`, `roomkit.VerificationRefusedError`, `roomkit.IdempotencyKeyTakenError`, `roomkit.ProviderDeliveryError`, `roomkit.KitClosedError`, `roomkit.ShutdownCancelledError`, `roomkit.RemoteTurnError`, `roomkit.TurnLinkError` |
| `roomkit::StoreError` | `roomkit.RoomAlreadyExistsError`, `roomkit.RoomNotFoundError`, `roomkit.ParticipantExistsError`, `roomkit.ParticipantNotFoundError`, `roomkit.IdentityNotFoundError`, `roomkit.AddressLinkedError`, `roomkit.EventNotFoundError`, `roomkit.TaskNotFoundError`, `roomkit.IdempotencyKeyTakenError` |
| `roomkit_ai::SkillError` | `roomkit.skills.SkillDiscoveryError`, `roomkit.skills.SkillParseError`, `roomkit.skills.SkillValidationError`, `roomkit.skills.SkillManifestTooLargeError`, `roomkit.skills.SkillVerificationError` |

## External tool handlers

`roomkit_ai::ExternalToolHandler` is the object-safe form of Python's ABC. A custom
handler embeds `ExternalToolHandlerState` and returns it from `state()`: this holds
the inherited channel identity and the `fire_before_hook` / `fire_on_tool_hook`
helpers. The trait's default cancellation and refusal reports delegate to that
state. `ExternalToolFuture` boxes each asynchronous callback; method-specific
options structs carry Python's keyword parameters with the same defaults.

The handler's state belongs to the handler, not to a global registry or a task.
Registration sets its identity and callbacks together; sharing an instance makes
the last registration authoritative, with a warning when its channel changes.
This language form was approved in HITL 915d07a5-8f40-4b74-8f9f-16087e3b9b92.


MCP skills register with `SkillRegistry::add_mcp_server(server, label, options)`.
`AddMcpServerOptions::default()` keeps dynamic content disabled, no execution
approval callback and no execution-tool patterns. Listing reads metadata only;
`load_skill` verifies the held resource digest and bounded safe YAML before
returning any instructions, and `read_file` verifies supplementary bytes against
that same entry. A rejected read retires cached content across registry copies.
The server remains host-owned. Served instructions stay attributed data in text
turns, never binding rules or activations restored from conversation history.
Host tools declared as executing code require approval of every active served
skill; callbacks fail closed, and approvals bind room, exact origin identity,
name and fingerprint. Dynamic entries are approved for every call.

**Registry migration:** mutation methods take `&self` so other tasks can read or
modify the registry while network I/O awaits. No registry lock spans an await.
Metadata, name and reason reads now return owned snapshots.
`get_skill(name)?` and `load_skill(name).await?` return `Option<LoadedSkill>`:
match `Local(Skill)` or `Served(McpSkill)`, or use the explicit common getters
and file helpers. `LoadedSkill` has no `Deref`; served file helpers cannot reach
local paths. Mutating a returned snapshot changes that value only. Read the
registry again to see new verified files. `Clone` and `copy` duplicate local
state while sharing the served catalogue and existing loaded-file caches.
`origin` returns `Arc<ServedOrigin>`; clones retain identity and a rejected
relisting keeps the original source. Equality preserves local values and
matcher identity, and compares nonempty served catalogues by shared identity.
`SkillError::Changed` carries the name and optional source; discovery and
verification errors preserve both their sanitized message and concrete cause.
These ownership forms were approved on 2026-11-17.

Served YAML uses `saphyr-parser` 0.3.3 events with the reference's 65,536-character,
64-level and alias bounds, without a new node limit. YAML sets normalize to
arrays sorted by each member's canonical JSON after YAML membership equality.
Servers publishing old Python set display strings must publish these arrays;
file bytes, digests and approval fingerprints are unchanged. Existing loaded
content remains cached until reloaded. The local frontmatter parser keeps its
existing behavior. Large JSON integers retain the existing explicit deferral.

Skill registries use `SkillRegistryOptions` for Python's constructor keyword
`requires_match`; `new(SkillRegistryOptions::default())` or `default()`
selects exact matching. `CopyOptions { marks: true }` is the default for
`copy(names, options)`. Disabling marks never restores unavailable skills.
This replaces `new()` and `copy(names)` calls (HITL 73da84cd, RMK-1709).
Registry equality includes matcher identity: default equals default; custom
callbacks equal only their clones, not independently constructed closures.

Registry `mark_unavailable` and `copy` return `Result<_, SkillError>` for
Python's `SkillManifestTooLargeError`: the final escaped manifest is bounded
at 65,536 Unicode characters, and rejection leaves the source unchanged
(HITL 7196c294, RMK-1709). Handle the result before using the copied registry.
Rust exposes no mutable reference to stored metadata, so every reachable
registry state already fits and `to_prompt_xml` remains an infallible `String`.
Python's shared mutable metadata instead requires a final rendering guard.

All six direct `Skill` file helpers return `Result<_, SkillFileError>`.
`has_scripts` and `has_references` count any directory entry; the lists include
files only. A listed symlink still has to pass containment when read or resolved.
`SkillPathError` maps Python's `ValueError` subclass to a typed error; filesystem
failures retain `io::ErrorKind`, and invalid UTF-8 retains the decoding error.
Read/resolve names are `&str`; non-UTF-8 names can be listed as `OsString` but
cannot be passed as Rust strings (HITL 45d6fce8). `read_reference` uses UTF-8 and
universal newlines without the model tool's byte bound. `resolve_script` resolves
a contained file and never executes it. Filename NFKC uses `unicode-normalization`
with default features disabled, approved in HITL 19458017.

The direct helpers retain stat permission failures, as Python 3.12 does, instead
of treating inaccessible directories or targets as absent. Python 3.14's pathlib
masks some stat errors; the Rust form keeps the explicit filesystem error agreed
in HITL 45d6fce8. For `Skill.read_reference` and `Skill.resolve_script`, both
runtimes refuse symlink loops; Rust reports a path error as Python 3.12 does,
while newer pathlib can fail at the later file-existence check.

The standalone `safe_join_filename` does not check file existence: Python 3.13+
can return an unresolved cyclic path successfully. Rust retains Python 3.12's
explicit `SkillPathError` for that case (HITL ef1a3d62).


Local script execution uses `AiChannelOptions.script_executor`, default `None`.
The host implements `ScriptExecutor`; the channel resolves containment before
calling it, and tool policy governs `run_skill_script`. The executor receives
`None` for omitted arguments and `Some` for an object, including an empty one.
Script calls are neither deferred nor replayed as tool memory, matching Python's
channel tool traits. The channel owns and closes its executor even without a
skill registry, after ending running calls. A panic from executor close reaches
a direct caller and is aggregated by kit close, following the existing channel
convention (HITL 213974fa, 2026-11-16). RoomKit supplies no OS process runner.
The shared `skill-scripts/` corpus and native `skills::script_execution` tests
cover this local contract; MCP-served scripts remain separate work.


## MCP host access

`McpToolProvider::new(url: Option<&str>, McpToolProviderOptions)` mirrors
Python's common constructor. `transport` defaults to `streamable_http`; `sse`
selects legacy GET/POST SSE and `stdio` selects a subprocess. The stdio form
works without the `http` feature; HTTP forms then return a configuration error.
Label validation precedes all other validation. Construction performs no I/O;
process options are validated when the connection starts.

`McpHeaders` and `McpEnvironment` are cloned handles to Python's retained
mappings, with atomic `insert`, `remove` and `replace`; `get` returns an owned
snapshot. Headers preserve case-sensitive keys and insertion order. Replacing
an existing key keeps its position; removing then reinserting it appends it.
Nonempty headers and every supplied environment are retained, while empty
headers are replaced at construction. Every connection copies the then-current
values; an open connection keeps its snapshot. `FromUrlOptions.headers` and
`FromCommandOptions.env` are now `Option` handles (default `None`): replace a
previous map value with `Some(map.into())`. `FromUrlOptions.transport` selects
the HTTP transport. These Rust forms are approved in HITL e4ef09db.

Legacy SSE reads the POST address from the GET stream's endpoint event and
keeps its configured raw authority check. Redirects have the SDK's separate
same-origin and method-preservation rules. Failed POST opening and rejected
endpoint announcements retain the caller's connection deadline, and failed
or cancelled opening closes the GET stream and stops the POST writer before
returning. It does not create a Streamable HTTP session or send DELETE.


`FromCommandOptions.discover` and `FromUrlOptions.discover` default to true;
false opens a resource-only connection with no tool listing. The host can
still call tools directly. `tool_meta` reads the admitted listing once,
while `server_info` and `server_extensions` read the handshake snapshots.
Every accessor refuses outside a live connection.

`request` sends caller parameters unchanged, including `_meta`; `None`
sends an explicit empty object. Raw request results preserve extension fields,
omitting null model fields as Python does; its known `_meta` field remains
validated. Handshake extras remain intact even where a newer Rust SDK gives
them a more restrictive type. Resource results retain explicitly
sent nulls, validate their known model fields and normalize URIs. A malformed
resource input/result returns `McpError::Validation`, the representation of
Python's Pydantic validation error.

The connection advertises MCP 2025-11-25 and accepts the same initialize-based
protocol versions as Python's pinned SDK. Protocol 2026 is deferred for joint
support (HITL 9496ae52). The existing serde and url versions are direct
dependencies for lossless serialization and URI parsing (HITL 04bbf03c).
No arbitrary request is replayed after a timeout or session loss.

Served-skill entry parsing preserves the reference URI/path spelling, frontmatter
JSON character bound, resource-list checks and error order. Direct construction
remains non-validating; a parsed entry owns its frontmatter copy. File verification
checks length before SHA-256. Entry fingerprints sort complete URI/digest lines,
without sizes or a trailing newline. The listed count and total-byte limits are
reported by `limit_exceeded`, not enforced as parse failures.

`SkillError::EntryValidation` carries the served entry's exact
`SkillValidationError` message without the local skill variant's prefix;
`Verification` carries `SkillVerificationError` (HITL 450fa510).
JSON formatting, copying, equality and disposal traverse containers iteratively.
Only the existing 65,536-character JSON bound limits `parse_entry`: a deeply nested
value within that bound can be accepted as on Python 3.14 even when Python
3.12/3.13 rejects it at an interpreter recursion limit (HITL 1566ac3f).
The existing large-JSON-integer and YAML deferrals remain unchanged; these helpers
do not yet register, refresh, activate or execute a server's skills.

## Role policy ownership

`ToolPolicy::new(ToolPolicyOptions { ..Default::default() })` replaces the old
two-field struct literal: private intersection state now accompanies the public
`allow`, `deny` and `role_overrides` fields. A cloned policy owns independent
lists and overrides. Edit a retained override through `policy.role_overrides`;
Rust does not retain Python's external mutable alias to a nested model.

A resolved restriction survives cloning and equality comparisons but is absent
from serialized data, matching Python's `model_dump`. Deserialization validates
fresh overrides; the typed constructor accepts an already constructed override
without revalidating its edits. Resolution validates only the lists Python
reconstructs. These forms and the source migration were approved on 2026-11-16 (RMK-1709).

## Host tool definitions

`AIChannel.extra_tools` and `Agent.extra_tools` are `extra_tools() ->
Vec<AiToolHandle>` in Rust. The vector is independent; cloned handles and
channel clones share each host definition. `get() -> AiTool` reads an owned
snapshot and `replace(AiTool)` replaces the definition for subsequent turns.
Neither operation changes the handler or installs a tool. Owner, human and
infrastructure definitions remain outside the getter.

The Rust form was approved on 2026-11-17. Its precise ownership
boundary was approved separately: a snapshot's nested parameters
and tags do not write through, and a provider already generating keeps the
definitions its turn prepared. Python's mutable objects can still be observed
through those retained aliases; this boundary is an accepted difference, not a
shared aliasing guarantee.

`RealtimeBackend::publish` and `publish_to_room` return
`RealtimeFuture<Result<(), ChannelError>>`. Existing backend implementations must
return `Ok(())` on success and `Err(cause.into())` on failure. Explicit kit
publications propagate `RoomKitError::Realtime(RealtimeFailure)`, whose owned
backend error retains its display and source chain. Local presence retries
recoverable failures; closing the kit cancels an in-flight retry. Turn-owner
notifications log at ERROR and continue, as Python's owner publisher does;
notifications published directly by an idle channel log at DEBUG and continue.
This Rust error form was approved on 2026-11-17.

## Discussion queue changes

`SpeakQueueChange::TurnSkipped` is Python's `TURN_SKIPPED`, serialized as
`"turn_skipped"`. It can be constructed and read as public queue-change data.
Detecting an offline remote executor and skipping its queued discussion turn
remain unsupported; adding this value does not enable those runtime behaviors.
