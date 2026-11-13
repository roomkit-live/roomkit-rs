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
| `roomkit_mcp::McpToolProvider` | — | `connect`, `close` |
| `roomkit_ai::MockAiProvider` | — | `calls` |
| `roomkit_ai::memory::MockMemoryProvider` | — | `retrieve_calls`, `ingest_calls`, `clear_calls`, `closed` |
| `roomkit_ai::knowledge::MockKnowledgeSource` | — | `search_calls`, `index_calls`, `closed` |
| `roomkit_ai::ThinkingBlocks` | — | `last_signature` |
| `roomkit_ai::RoundTranscript` | — | `thinking` |
| `roomkit_ai::ToolError` | — | Whole type |
| `roomkit::RoomKit::start` | — | Whole type |
| `roomkit::telemetry::SpanGuard` | — | Whole type |
| `roomkit::telemetry::MockTelemetryProvider` | — | `completed_spans`, `metrics` |
| `roomkit::telemetry::Attr` | — | `PROVIDER`, `MODEL`, `ROOM_ID`, `SESSION_ID`, `CHANNEL_ID`, `TTFB_MS`, `DURATION_MS`, `STT_TEXT`, `STT_TEXT_LENGTH`, `STT_CONFIDENCE`, `STT_IS_FINAL`, `STT_MODE`, `TTS_VOICE`, `TTS_CHAR_COUNT`, `TTS_TEXT_LENGTH`, `LLM_INPUT_TOKENS`, `LLM_OUTPUT_TOKENS`, `LLM_TOOL_COUNT`, `LLM_STREAMING`, `HOOK_NAME`, `HOOK_TRIGGER`, `HOOK_RESULT`, `PIPELINE_FRAME_COUNT`, `PIPELINE_BYTES_PROCESSED`, `PIPELINE_STAGES`, `PIPELINE_FRAMES`, `REALTIME_PROVIDER`, `REALTIME_TOOL_NAME`, `REALTIME_TOOL_DENIED`, `DELIVERY_CHANNEL_TYPE`, `DELIVERY_RECIPIENT`, `DELIVERY_SUCCESS`, `DELIVERY_ERROR`, `DELIVERY_MESSAGE_ID`, `STORE_OPERATION`, `STORE_TABLE`, `BACKEND_TYPE`, `DELEGATION_TASK_ID`, `DELEGATION_WORKER_ID`, `DELEGATION_CHILD_ROOM_ID`, `DELEGATION_PARENT_ROOM_ID`, `DELEGATION_STATUS`, `DELEGATION_MODE` |
| `roomkit_ai::CallIds` | — | `call` |
| — | `roomkit.PendingInput.__init__` | `status`, `result`, `reject_reason` |
| — | `roomkit.ACPSessionInvalidatedError.__init__` | `reason` |

## Types represented by language rules

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
| `roomkit_ai::SkillError` | `roomkit.skills.SkillDiscoveryError`, `roomkit.skills.SkillParseError`, `roomkit.skills.SkillValidationError`, `roomkit.skills.SkillManifestTooLargeError` |

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
