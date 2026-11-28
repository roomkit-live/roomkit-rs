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
| `roomkit::RoomKitError::no_recipient` | `roomkit.NoRecipientError` | Builds the pre-send refusal with both supplied strings. The `NoRecipient` variant retains both strings and its structured `provider_result()`. Its `code()` is `no_recipient` and `retryable()` returns false. |
| `roomkit_ai::call_cut`, `call_partial`, `call_garbled` | `roomkit.providers.ai.call_cut`, `call_partial`, `call_garbled` | Pass the raw `serde_json::Value`, finish reason and the function’s options. Their default selects the last call, including a sole call. An object already supplied by a provider differs from JSON text under a cut. |
| `roomkit_ai::ToolNameRule` | `roomkit.providers.ai.ToolNameRule` | `new(vendor, pattern)` compiles the expression. Read-only accessors expose the supplied values. `check(names)` tests whole names in order. Rust uses fancy-regex 0.20.2: scoped ASCII flags, named conditional groups, and Unicode character-name escapes differ from Python. |
| `roomkit_ai::SkillError` | `roomkit.skills.SkillParseError` | `Frontmatter` carries the standalone parse message. `parse_frontmatter` returns ordered text or list values and the body. It reads the bounded local subset, including fallback globs. Flow lists split on commas. Nested metadata, quoted commas in flow entries and full YAML 1.1 scalar coercion are outside this subset. |
| `roomkit::ToolCallVerdict`, `roomkit::model::ToolCallVerdict` | `roomkit.ToolCallVerdict` | Immutable record. `Default` supplies the five Python defaults. `new` takes all five values explicitly; the accessors borrow the stored strings and handles. |
| `roomkit::model::tool_verdict::VerdictResult`, `roomkit::model::tool_verdict::VerdictValue`, `roomkit::model::tool_verdict::VerdictList`, `roomkit::model::tool_verdict::VerdictObject` | The verdict's text, parts and structured data | Clones share each list and ordered dictionary. Snapshots own only the outer collection and preserve nested handles. A host may retain its own native part by putting its `Arc` in `VerdictValue::Native`. Use a shared list or object for separately retained nested containers and `Json` for owned JSON. The owner must break cycles of strong handles before dropping its last reference. A synchronous `update` callback must not reacquire its list's lock. A native application's own equality semantics are opaque; the model provides no universal dynamic equality operator. |
| `roomkit_ai::ChatDialect`, `roomkit_ai::ChatDialectOptions` | `roomkit.providers.ai.ChatDialect` | The options carry the five Python defaults. `ChatDialect::new(options)` validates their dependencies. Read-only accessors preserve the frozen result (decision 8d00f04e). |
| `roomkit::tasks::BackgroundSetup` | `roomkit.tasks.BackgroundSetup` | `Default` selects the agent and relay. The two-argument `new(worker, hand_back)` validates immediately. Borrowed accessors expose immutable fields, including arbitrary worker text. |
| `roomkit_ai::RunSkillScriptTool` | `roomkit.RunSkillScriptTool` | `NAME` is the Python class variable `name`. The `Tool` trait supplies `definition` and `handler`. |
| `roomkit_ai::Answers` | Python speaking/classifier contract | `new`, `insert`, `get`, `remove`, `len`, `is_empty`, `snapshot`, `iter` |
| `roomkit_ai::ClassifierError` | Python speaking/classifier contract | `new`, `message`, `with_source` |
| `roomkit_ai::MockClassifier` | Python speaking/classifier contract | `calls` |
| `roomkit_ai::classifiers::Answers` | Python speaking/classifier contract | `new`, `insert`, `get`, `remove`, `len`, `is_empty`, `snapshot`, `iter` |
| `roomkit_ai::classifiers::ClassifierError` | Python speaking/classifier contract | `new`, `message`, `with_source` |
| `roomkit_ai::classifiers::MockClassifier` | Python speaking/classifier contract | `calls` |
| `roomkit_ai::MockSpeakPolicy` | Python speaking/classifier contract | `turns` |
| `roomkit_ai::MockThinker` | Python speaking/classifier contract | `calls` |
| `roomkit_ai::speaking::MockSpeakPolicy` | Python speaking/classifier contract | `turns` |
| `roomkit_ai::speaking::MockThinker` | Python speaking/classifier contract | `calls` |
| `roomkit_ai::ClassifierSpeakPolicyOptions` | Python speaking/classifier contract | `overrides` |
| `roomkit_ai::SharedAiContext` | Python speaking/classifier contract | Local shared object handle: new transfers ownership, Clone shares, snapshot copies, update edits synchronously, same_object compares identity (approved speaking forms). |
| `roomkit_ai::classifiers::SharedState` | Python speaking/classifier contract | Local shared object handle: new transfers ownership, Clone shares, snapshot copies, update edits synchronously, same_object compares identity (approved speaking forms). |
| `roomkit_ai::speaking::SharedRoomEvent` | Python speaking/classifier contract | Local shared object handle: new transfers ownership, Clone shares, snapshot copies, update edits synchronously, same_object compares identity (approved speaking forms). |
| `roomkit_ai::classifiers::ChoiceOptions` | Python speaking/classifier contract | Ordered mutable mapping handle with atomic insert/remove, owned reads and explicit snapshots; Clone preserves aliases (approved speaking and mock script forms). |
| `roomkit_ai::classifiers::Probabilities` | Python speaking/classifier contract | Ordered mutable mapping handle with atomic insert/remove, owned reads and explicit snapshots; Clone preserves aliases (approved speaking and mock script forms). |
| `roomkit_ai::classifiers::Questions` | Python speaking/classifier contract | Ordered mutable mapping handle with atomic insert/remove, owned reads and explicit snapshots; Clone preserves aliases (approved speaking and mock script forms). |
| `roomkit_ai::classifiers::Script` | Python speaking/classifier contract | Ordered mutable mapping handle with atomic insert/remove, owned reads and explicit snapshots; Clone preserves aliases (approved speaking and mock script forms). |
| `roomkit_ai::speaking::Judgments` | Python speaking/classifier contract | Ordered mutable mapping handle with atomic insert/remove, owned reads and explicit snapshots; Clone preserves aliases (approved speaking forms). |
| `roomkit_ai::speaking::Speakers` | Python speaking/classifier contract | Ordered mutable mapping handle with atomic insert/remove, owned reads and explicit snapshots; Clone preserves aliases (approved speaking forms). |
| `roomkit_ai::classifiers::MockClassifierOptions` | Python speaking/classifier contract | Module re-export of the keyword options also checked at roomkit_ai root; identical fields and defaults, not a separate constructor. |
| `roomkit_ai::speaking::ClassifierSpeakPolicyOptions` | Python speaking/classifier contract | Module re-export of the keyword options also checked at roomkit_ai root; identical fields and defaults, not a separate constructor. |
| `roomkit_ai::speaking::LlmThinkerOptions` | Python speaking/classifier contract | Module re-export of the keyword options also checked at roomkit_ai root; identical fields and defaults, not a separate constructor. |
| `roomkit_ai::speaking::MockSpeakPolicyOptions` | Python speaking/classifier contract | Module re-export of the keyword options also checked at roomkit_ai root; identical fields and defaults, not a separate constructor. |
| `roomkit_ai::speaking::MockThinkerOptions` | Python speaking/classifier contract | Module re-export of the keyword options also checked at roomkit_ai root; identical fields and defaults, not a separate constructor. |
| `roomkit_ai::speaking::SpeakDecisionOptions` | Python speaking/classifier contract | Module re-export of the keyword options also checked at roomkit_ai root; identical fields and defaults, not a separate constructor. |
| `roomkit_ai::classifiers::ScriptedAnswer` | Python speaking/classifier contract | Typed answer, number or text in a live mock script, interpreted only when its question is asked (approved mock script form). |
| `roomkit_ai::speaking::ClassifierSpeakOverrides` | Python speaking/classifier contract | Three optional callbacks are the explicit composition of Python state/questions/decision overrides, all absent by default (approved speaking forms). |
| `roomkit_ai::classifiers::ClassifierFuture` | Python speaking/classifier contract | Named borrowed fallible future or synchronous callback type for the approved classifier/speaking protocols; no extra operation. |
| `roomkit_ai::speaking::SpeakingFuture` | Python speaking/classifier contract | Named borrowed fallible future or synchronous callback type for the approved classifier/speaking protocols; no extra operation. |
| `roomkit_ai::speaking::SpeakDecisionRule` | Python speaking/classifier contract | Named borrowed fallible future or synchronous callback type for the approved classifier/speaking protocols; no extra operation. |
| `roomkit_ai::speaking::SpeakQuestionsRule` | Python speaking/classifier contract | Named borrowed fallible future or synchronous callback type for the approved classifier/speaking protocols; no extra operation. |
| `roomkit_ai::speaking::SpeakStateRule` | Python speaking/classifier contract | Named borrowed fallible future or synchronous callback type for the approved classifier/speaking protocols; no extra operation. |
| `roomkit_ai::speaking::SpeakMode` | `SpeakDecision.mode` | Literal `speak`, `offer`, `silent`; no implicit mode. |
| `roomkit::TurnHost::room_vision` | `roomkit.TurnHost.room_vision` | Reads the owner's optional camera note while the turn is open. A Rust text-only owner returns `None`; video analysis is outside the text SDK. |
| `roomkit::TurnHost::room_context` | `roomkit.TurnHost.room_context` | `Result<Metadata, RoomKitError>` reads fresh owned JSON, including history despite a zero channel window; failures keep the existing kit mapping and store causes. |
| `roomkit::turns::TurnBackendHandle` | `roomkit.turns.TurnBackend` instance | Clone the handle for each kit and client to share the transport and its default presence claims. Distributed transports may supply their own claims (HITL a12b3f92). |
| `roomkit::turns::wire::Envelope`, `FrameEnvelope`, `ControlEnvelope` | `turns.wire.FrameEnvelope`, `turns.wire.ControlEnvelope` | Each specialization carries the turn ID, sequence number and one message in its direction. |
| `roomkit::turns::wire::Frame`, `Control`, `OwnerMessage`, `PublicationFrame` | `turns.wire.Frame`, `Control`, `OwnerMessage`, `RoomPublication.frame` | JSON uses the source union’s discriminator. A publication accepts the two frame types allowed outside a turn. |
| `roomkit::turns::wire::InputOutcome`, `ToolOutcome` | `turns.wire.InputAnswer.outcome`, `ToolResult.outcome` | Variants serialize to the literals declared on the corresponding Python field. |
| `roomkit::turns::wire::Item`, `ToolEnd`, `WireLoopEnd`, `WireResult` | `turns._codec` stream items, tool ends, loop ends and results | The item discriminator carries the marker kind. Tool results preserve the choice of a JSON value or typed content parts. |
| `roomkit::turns::ToolHooks::decide_before_tool_use`, `roomkit::turns::ToolHooks::judge_tool_call`, `roomkit::turns::ToolHooks::observe_failed_tool_call`, `roomkit::turns::ToolHooks::report_tool_call`, `roomkit::turns::ToolHooks::run_tool_call_chain` | `executor.wiring.tools` methods of the same names | Rust accepts explicit `None` for the omitted `carrying` context and `claim` callback. |
| — in this crate; `roomkit_providers_opentelemetry::OpenTelemetryProvider`, `roomkit_providers_opentelemetry::OpenTelemetryProviderOptions` in the roomkit-providers repository | `roomkit.telemetry.OpenTelemetryProvider` | The OpenTelemetry provider left this crate with every vendor's code (RMK-2011), with the options and the behaviour it had here; it starts its spans under `telemetry::get_current_telemetry_ctx`, the current span's context the core gives. |
| — in this crate; `roomkit_providers_redis::RedisTurnBackend::new`, `roomkit_providers_redis::RedisTurnBackendOptions` in the roomkit-providers repository | `roomkit.RedisTurnBackend(url=None)` | The backend on Redis left this crate with every service's code (RMK-2012): it is `DistributedTurnBackend` assembled on the Redis implementation of the two ports. There, pass `None` for an omitted URL; both omission and an empty string select `redis://localhost:6379`. Pass the application's `redis::aio::ConnectionManager` as `client` to retain its data connection and timeouts; supplying a client requires a URL for the independent heartbeat and inbox connections. Clones share the same backend identity. |
| `roomkit::TurnExecutor::new`, `roomkit::TurnExecutorOptions` | `roomkit.TurnExecutor.__init__` | The factory asynchronously returns an optional channel. The listed hosts, organizations and executor ID are supplied through options. A worker starts serving when its backend's `serve` completes. |
| `roomkit::turns::TurnFuture` | An awaitable returned by a link or a backend | A boxed future allows a trait object to call asynchronous application code. |
| `roomkit::turns::TurnContext` | `contextvars.Context` from `ExecutorLink.turn_context` and `stream_context` | A local link can capture the current execution context. A distributed worker receives a fresh context. |
| `roomkit::turns::TurnBackend::executors_now` | `TurnBackend.executors_now` | Refresh a cached catalogue before skipping a turn. The default delegates to the transport's ordinary catalogue read. |
| `roomkit::turns::TurnBackend::watch_executors` | `TurnBackend.watch_executors` | Have a listener run when this process serves an executor or one it serves closes. The default keeps none, and the owner learns at its next look; a backend that wraps another hands the listener on. |
| `roomkit::turns::ExecutorWatchers`, `ExecutorWatchers::changed` | The listeners `TurnBackend` keeps, `TurnBackend.executors_changed` | A trait keeps no state: a backend holds an `ExecutorWatchers`, gives it each listener (`watch`) and runs them (`changed`) once it serves an executor and when one it serves closes. |
| `roomkit::turns::DistributedTurnBackend`, `roomkit::turns::DistributedTurnBackendOptions` | `roomkit.DistributedTurnBackend(broker, registry, *, timeouts=None)` | The turn backend across processes, over a broker and a registry: `new(broker, registry, options)`, the options holding Python's keywords. The `roomkit-providers-redis` crate assembles one on Redis as `RedisTurnBackend`, as Python's `RedisTurnBackend` is one (RMK-2012). |
| `roomkit::turns::TurnBroker`, `roomkit::turns::TurnInbox`, `roomkit::turns::InboxEntry`, `roomkit::turns::BrokerMessage` | `roomkit.TurnBroker`, `TurnInbox`, `InboxEntry`, `BrokerMessage` | The port of the messages between the processes of a distributed backend: an inbox a node, posted to (one or several messages a post), read in order after a cursor, acknowledged. A `block` is a `Duration` where Python's is in seconds; Python's `BrokerMessage.ref` is `reference`, `ref` being a Rust keyword; `BrokerMessage::new(kind, sender, body)` leaves `target` and `reference` empty, set by struct update as Python's defaults; a reader is let go of as it drops, where Python's `TurnInbox.close()` closes it (RMK-2012). |
| `roomkit::turns::TurnRegistry`, `roomkit::turns::BeatWriter`, `roomkit::turns::Beat`, `roomkit::turns::RegistryView`, `roomkit::turns::Beats` | `roomkit.TurnRegistry`, `BeatWriter`, `Beat`, `RegistryView`, `Beats` | The port of what those processes share: their beats and the view of them at the registry's time, the time of day in milliseconds, a room's holder, the presence last published. `Beat::new(node, generation)` is Python's `Beat(node, generation)`, the rest set by struct update; a beat writer is let go of as it drops, where Python's `BeatWriter.close()` closes it (RMK-2012). |
| `roomkit::turns::InMemoryTurnBroker`, `roomkit::turns::InMemoryTurnRegistry` | `roomkit.InMemoryTurnBroker`, `InMemoryTurnRegistry` | The two ports kept in one process, for tests and for nodes that share a process (RMK-2012). |
| `roomkit::testing::check_turn_broker`, `roomkit::testing::check_turn_registry` | `roomkit.testing.check_turn_broker`, `check_turn_registry` | The contract of the two ports, as checks a service that implements them calls from its own tests: each panics, saying what, where the contract is not kept, where Python's raise `AssertionError` (RMK-2012). |
| `roomkit::turns::ExecutorInfo::of`, `roomkit::turns::ExecutorInfoOfOptions` | `ExecutorInfo.of` | The entry of an executor served in this process, as a backend lists it: it covers what the executor hosts, a predicate included. `ExecutorInfoOfOptions` holds `node`. |
| `roomkit::turns::ServedExecutor::same_as` | `executor is other` | Two handles of one executor are the same: Python compares the objects, a Rust backend holds several handles of one executor. |
| `roomkit::TurnExecutor::watch_closed`, `roomkit::turns::ServedExecutor::watch_closed` | `TurnExecutor.watch_closed` | Have a listener run once this executor closed: the backend serving it then tells its room owner. A listener given again is kept once, by the allocation it shares, where Python compares the callables. |
| `roomkit::turns::ExecutorsListener` | `Callable[[], Awaitable[None]]` | A shared closure answering the work to await, or `None` once its owner is gone: the backend then forgets it. |
| `roomkit::turns::TurnClientOutput` | `TurnClient.run` returning `ChannelOutput` | The result exposes the channel output fields and an optional response stream. Reading the stream advances the worker. |
| `roomkit::turns::TurnClientInputs`, `roomkit::turns::TurnClientInputTurn` | `TurnClient.inputs.request(request_id)` | Read the original turn request and the worker's raw input request together. Use the client's `input_request` to read the resolved, organization-filtered event. |
| `roomkit::turns::ServedExecutor` | `roomkit.TurnBackend.serve(executor)` | A backend accepts an executor backed by any conversation store. The application keeps the typed executor to register hooks. |
| `roomkit::TurnExecutor::new_with_host_predicate` | `roomkit.TurnExecutor(..., hosts=predicate)` | A closure selects channels on its own node. Cloned local snapshots retain it. |
| `roomkit::turns::ErrorClasses` | `RoomKit(..., error_classes=...)`, `TurnExecutor(..., error_classes=...)` | Register the native type's encoder and decoder. An exact class match takes precedence over its nearest registered base. |
| `roomkit::RoomKitError::RemoteTurn::detail`, `roomkit::turns::RemoteTurnDetails` | The exception received by a turn owner or worker | Inspect the original record, the decoded error and each cause. Forwarding preserves the received record. |
| `roomkit::turns::ExecutorInfo`, `roomkit::turns::ExecutorInfoOptions` | `roomkit.turns.ExecutorInfo` | Use `ExecutorInfo::new(id, options)` for the seven data fields. Local snapshots and their clones retain the worker's host selector. Serializing and reading the JSON restores the data fields alone (HITL eb3c6f0b, 32eff891). |
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
| `roomkit::content_logging_enabled`, `roomkit::set_content_logging` | `roomkit.content_logging_enabled`, `roomkit.set_content_logging` | Same process-wide boolean and setter. Rust initializes `ROOMKIT_LOG_CONTENT` once on first use, the counterpart of Python module import; subsequent environment changes do not refresh it. DEBUG filtering and span privacy configuration remain independent (approved 2026-11-17). |
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
| `roomkit::ConversationStore::connection`, `roomkit::store::ConversationStore::connection` | `roomkit.ConversationStore.connection` | `work`: a borrowed async body passed as a future; acquisition precedes its first poll. Default backends run the body directly. PostgreSQL retains one connection through nested calls and cancellation cleanup (approved 2026-11-17). |
| — in this crate; `roomkit_providers_postgres::PostgresStore`, `PostgresAdvisoryLockManager`, `PostgresKnowledgeSource` and their options (`PostgresCancelQuery` among them) in the roomkit-providers repository | `roomkit.store.PostgresStore`, `roomkit.store.PostgresAdvisoryLockManager`, `roomkit.knowledge.PostgresKnowledgeSource` | The Postgres store, lock and knowledge source left this workspace with every vendor's code (RMK-2011), with the options and the behaviour they had here; the store passes `roomkit::testing::check_conversation_store`. |
| `roomkit::ConversationStore` | — | `is_process_local` |
| `roomkit::ConversationStore` | — | `set_telemetry` |
| `roomkit::store::ConversationStore` | — | `set_telemetry` |
| `roomkit::store::ConversationStore` | — | `is_process_local` |
| `roomkit::StoreError` | — | `Unavailable`, `unavailable` |
| `roomkit::model::ResponseMetadata` | — | `get`, `contains_key`, `len`, `is_empty`, `insert`, `pop`, `setdefault`, `update`, `clear`, `with_value`, `to_metadata` |
| — | `roomkit.ChannelOutput.__init__` | `response_stream` |
| — | `roomkit.TurnHost` | `__init__` |
| — | `roomkit.DeliveryHandle` | `__init__` |
| `roomkit::TaskCutShortError::new` | `roomkit.TaskCutShortError.__init__` | The `reason` belongs to its borrowed base. |
| `roomkit::TaskTurnFailedError::new` | `roomkit.TaskTurnFailedError.__init__` | The `error` is accessible through `Error::source`. |
| `roomkit::TurnCutShortError::new` | `roomkit.TurnCutShortError.__init__` | `Display` returns the original `message`. |
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
| `roomkit::FrameworkAwareChannel::set_framework` | `roomkit.FrameworkAwareChannel.set_framework` | Receives `WeakRoomKit<S>`; upgrading reaches the registering kit and its actual store. |
| `roomkit::framework_aware` | — | Explicit opt-in at `register_channel` (approved 2026-11-17). |
| `roomkit::FrameworkAware` | — | Whole type |
| `roomkit::ChannelRegistration` | — | Whole type |
| `roomkit::default_inbound_event` | — | Whole type |
| `roomkit::keep_context` | — | Whole type |
| `roomkit::store::written::event`, `binding`, `now` | — | A store's checks on what it writes and its clock: Python's models refuse such a value when they are built |
| `roomkit::testing::check_conversation_store` | — (RMK-2155) | The store contract as one check; `open` opens an empty store at each call |
| `roomkit::track_host_room_lock` | `roomkit.core.locks._has_room_lock`, `_mark_room_locked` (private; RMK-2156) | A lock manager wraps its acquisition in it, so that a host holding its guard can call the kit for that room |
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
| `roomkit::SUBMIT_RESULT`, `roomkit::orchestration::SUBMIT_RESULT` | `roomkit.SUBMIT_RESULT`, `roomkit.orchestration.SUBMIT_RESULT` | Both paths share one lazily initialized `ResultTool`. Its tool definition contains owned JSON and its fields contain callbacks. |
| `roomkit_ai::tasks::DELEGATE_TOOL` | — | Whole type |
| `roomkit::orchestration::MissingResult` | `roomkit.orchestration.MissingResult` | The callable protocol for `ResultTool.on_missing` is an owned Rust callback taking the role, last output and attempt count, and returning metadata. |
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
| `roomkit_ai::image::ImageGenerationError` | — | `results`, `with_source` |
| `roomkit_ai::image::MockImageProvider` | — | `calls` |
| `roomkit_ai::image::ImageSize` | `ImageOptions.image_size: Literal["512", "1K", "2K", "4K"]` | Whole type: variants `Tier512`, `Tier1K`, `Tier2K`, `Tier4K`, serialized as Python's values |
| `roomkit_ai::image::ImageCatalogEntry` | `list[ModelInfo]` of `ImageProvider.available_models()` | Whole type: `Model(ModelInfo)` or `Image(ImageModelInfo)`, what `available_models` and `catalog_entry` return (HITL 4071eaf1) |
| `roomkit_ai::image::ImageError` | `ValueError`, `ProviderError` of `ImageProvider.generate` | Whole type: `Value(ImageRequestError)` or `Provider(ProviderError)` (HITL 0e571af4) |
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
| `roomkit_ai::image::ImageResultError` | `ValueError`; members: none |
| `roomkit_ai::image::ImageRequestError` | `ValueError`; members: `new` |
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
| `roomkit_ai::image::ImageAttemptStatus` | Values of `roomkit.ImageAttempt.__init__(status)`: `started`, `preview`, `succeeded`, `failed`, `unknown` |
| `roomkit_ai::image::ImageQuality` | Values of `roomkit.ImageOptions.__init__(quality)`: `auto`, `low`, `medium`, `high`, `xhigh`, `max` |
| `roomkit_ai::image::ImageBackground` | Values of `roomkit.ImageOptions.__init__(background)`: `auto`, `opaque`, `transparent` |
| `roomkit_ai::image::ImageOutputFormat` | Values of `roomkit.ImageOptions.__init__(output_format)`: `png`, `jpeg`, `webp` |
| `roomkit_ai::image::ImageModeration` | Values of `roomkit.ImageOptions.__init__(moderation)`: `auto`, `low` |
| `roomkit_ai::image::ImageInputFidelity` | Values of `roomkit.ImageOptions.__init__(input_fidelity)`: `low`, `high` |
| `roomkit_ai::image::ImageThinkingLevel` | Values of `roomkit.ImageOptions.__init__(thinking_level)`: `minimal`, `medium`, `high` |
| `roomkit_ai::image::ImageSearchType` | Values of `roomkit.ImageOptions.__init__(search_types)`: `web_search`, `image_search` |
| `roomkit::RoomKitError` | `roomkit.RoomNotFoundError`, `roomkit.RoomAlreadyExistsError`, `roomkit.RoomClosedError`, `roomkit.TransitionRefusedError`, `roomkit.ProcessTimeoutError`, `roomkit.ChangeRefusedError`, `roomkit.ReentrantCallError`, `roomkit.ReentryLoopCapError`, `roomkit.ChannelNotFoundError`, `roomkit.ChannelNotRegisteredError`, `roomkit.ChannelAlreadyRegisteredError`, `roomkit.ParticipantNotFoundError`, `roomkit.IdentityNotFoundError`, `roomkit.AddressLinkedError`, `roomkit.EventNotFoundError`, `roomkit.HumanInputRejectedError`, `roomkit.ToolNameCollisionError`, `roomkit.VerificationRefusedError`, `roomkit.IdempotencyKeyTakenError`, `roomkit.ProviderDeliveryError`, `roomkit.NoRecipientError`, `roomkit.KitClosedError`, `roomkit.ShutdownCancelledError`, `roomkit.RemoteTurnError`, `roomkit.TurnLinkError` |
| `roomkit::StoreError` | `roomkit.RoomAlreadyExistsError`, `roomkit.RoomNotFoundError`, `roomkit.ParticipantExistsError`, `roomkit.ParticipantNotFoundError`, `roomkit.IdentityNotFoundError`, `roomkit.AddressLinkedError`, `roomkit.EventNotFoundError`, `roomkit.TaskNotFoundError`, `roomkit.IdempotencyKeyTakenError`, `roomkit.EventIndexTakenError` |
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
same-origin and method-preservation rules. Rejected endpoint announcements
retain the caller's connection deadline. A failed POST stops the writer;
a response already awaited keeps its own deadline. Failed or cancelled
opening closes the GET stream and stops the POST writer before returning. It does not create a Streamable HTTP session or send DELETE.
Its TCP connect and TLS handshake each have the Python SDK's five-second
budget. Each logical header/body write has five seconds, including partial
writes and TLS flushing; reads have 300 seconds between progress. These are
separate from the caller's opening/call bounds: a POST response may take more
than five seconds. Legacy SSE uses owned Hyper/rustls I/O for those deadlines
(approved in HITL 057d2d9a); Streamable HTTP retains its existing client.
As on Python's SDK 2.3 (RMK-2203), the GET's `Content-Type` is read as a media
type, an SSE event is bounded at 1 MiB, past which the connection ends, and a
request given up once rmcp holds it, posted or not, has the writer post
`notifications/cancelled` (reason `caller cancelled`): the cancellation joins
the writer's queue behind what waits there already, and is not sent once it
waited past five seconds. Python's `DeprecationWarning` for this transport is a
`tracing::warn!` (target `roomkit::mcp`) with the same text, at each
construction on `sse` (HITL 5fd1217f).


`FromCommandOptions.discover` and `FromUrlOptions.discover` default to true;
false opens a resource-only connection with no tool listing. The host can
still call tools directly. `tool_meta` reads the admitted listing once,
while `server_info` and `server_extensions` read the handshake snapshots.
Every accessor refuses outside a live connection.

`request` sends caller parameters unchanged, including a non-empty `_meta`;
`None`, an empty set or a `null` or empty `_meta` go out as no `params` member,
and a `_meta` that is neither an object nor `null` returns `McpError::Validation`
before anything is sent (RK-21.2-046). Raw request results preserve extension fields,
omitting null model fields as Python does; its known `_meta` field remains
validated. Handshake extras remain intact even where a newer Rust SDK gives
them a more restrictive type. Resource results retain explicitly
sent nulls, validate their known model fields and normalize URIs. A malformed
resource input/result returns `McpError::Validation`, the representation of
Python's Pydantic validation error.

`protocol` is Python's string, as `transport` is: `"auto"` (the default)
probes MCP 2026-07-28's `server/discover` over stdio and Streamable HTTP and
falls back to the `initialize` handshake, `"legacy"` goes straight to it, and
any other value is a `McpError::Configuration` at construction. The handshake
advertises MCP 2025-11-25 and accepts the same initialize-based revisions as
Python's pinned SDK; a discovery agrees on 2026-07-28. rmcp's `Auto`
lifecycle runs the probe and its 10-second bound; where Python's SDK falls
back and rmcp would fail (a discovery result it cannot read or that names no
revision both speak, a `-32022` naming a handshake revision, `-32020` or
`-32021`), the capture shows rmcp a refusal it falls back on. A handshake
answered with `-32022` after a probe that timed out fails the connection,
where Python's SDK probes again: rmcp offers no such step. On 2026-07-28 the
`Mcp-Name` and `Mcp-Param-*` headers and the `x-mcp-header` checks are
Python's SDK's (`mcp.shared.inbound`), written by roomkit-mcp: rmcp keeps its
own crate-private and they differ at the edges. `propagate_trace_context`
reads the kit's current span through its telemetry provider's
`inject_context`, roomkit-rs having no OpenTelemetry context of its own
(HITL 8dff5e28). The existing serde and url versions are direct
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

## Turn executors and transports

An application can give either a standalone worker or the kit's executor an
`Arc<dyn roomkit::turns::ExecutorLink>` through `TurnExecutor::take`. The call
starts the turn on a task. The link supplies the request, sends numbered
frames, receives controls and learns when the turn ends through `turn_ended`.
A write waiting on the transport can run concurrently with a control read.
`TurnExecutor::control` accepts a typed `ChannelControl` outside a turn.
A channel verb returns a `ChannelResult` with its value or error and any
changed room state. Attach, detach, release and committed-event notifications
return `None`. These worker APIs are available with the default features.

`roomkit::turns::TurnLink` describes the owner's end of a turn. The typed
request, frames, controls and envelopes are in `roomkit::turns::wire`. The
request and channel call track handover locally. Their serialized copies
start with a fresh handover marker.

Create a `roomkit::turns::TurnBackendHandle::new(Arc::new(transport))` and set
`RoomKitOptions::turn_backend` to `Some(backend.clone())` to run the
kit's turns and channel controls through an application implementation of
`roomkit::turns::TurnBackend`. On first use the kit calls `attend(owner)`,
then `serve(executor)`. The `ServedExecutor` view is independent of the
conversation store. Application code still registers hooks on its typed
`TurnExecutor<S>`. A worker can retain a `RoomOwner` for calls and publications
after the turn by calling `reach_owner`.

`TurnExecutor::organizations()` returns `Option<BTreeSet<String>>`. An ordinary
kit returns a set containing `"*"`. A standalone worker can serve rooms without
an organization by leaving its declaration at `None`.

A backend implements `submit`, `serve`, `control` and `close`. `submit_to` hands a turn to the one executor a host names; its default refuses with `no_executor`, nothing handed, and the in-memory and the distributed backends override it. Clone the same
handle for the kit, the standalone `TurnClient` and direct calls. Its default
`claim_presence` remembers a change once per room and channel. Detach forgets
the pair. Independent handles each own their default claims. A distributed
transport can return `Some(future)` from its trait override to share claims
across processes, as the distributed backend does. A transport that lists workers supplies a future from `executors`. An empty list confirms no worker is available. The default, `None`, declares no catalogue. In either case, the handle returns a list to the caller. A transport can override `executors_now` for a fresh read when a decision would skip a turn.
`DistributedTurnBackend` provides this interface over a broker and a registry; the `roomkit-providers-redis` crate gives both on Redis.

Each standalone factory returns a
future yielding `Result<Option<C>, RoomKitError>` for one concrete
`C: Channel`. `TurnExecutorOptions` supplies the executor ID, host
declarations, organization scope, timeouts, channel ownership and cache
bound. On the serving node, placement can consult a host predicate supplied
by `new_with_host_predicate` or a worker with an omitted host list. That
node lists its own active workers in registration order. Its executor snapshots apply the local host selector in `covers`. Other nodes
place work on advertised channel IDs and trailing-wildcard prefixes.

An application can construct `InMemoryTurnBackend::new()` and share it with a
room owner and standalone workers. `TurnExecutorOptions::telemetry` installs
the worker's telemetry provider and preserves an incoming trace parent.

`RoomKitOptions::error_classes` configures the owner and its local executor.
A standalone executor uses its own `TurnExecutorOptions::error_classes`.
Register a native error type with `ErrorClasses::register`, its wire class
name and two functions for reading and writing the record. The decoder tries
the exact class and then its nearest registered base. For application errors
`RoomKitError::RemoteTurn` exposes the complete wire record and the native
error in its details. Its `causes()` exposes each cause with its registered
type. Forwarding a remote error carries its original record.

`TurnClient::new(backend, options)` creates a standalone owner. Supply the
executor, host verbs, tools and event sink in `TurnClientOptions` as needed.
`run` returns the channel output and an optional response stream. Reading the
stream advances the worker. A client can also apply channel controls, steer
its active turns and handle human input.

`TurnExecutor::wiring()` returns a handle to the environment used to wire channels. Supply an `Arc<dyn WiringEnv>` to `use_wiring` to change
the environment used for subsequent channels. Call `wire(&channel)?` to bind
a channel before registration.

An application using the AI crate imports `roomkit_ai::TurnWiringExt` to
obtain the executor's AI event callbacks. The event types live in that crate.
The calls keep their Python names, for example
`executor.wiring().build_before_generation_hook(&channel)`. On
`executor.wiring().tools()`, pass `None` for Python's omitted `carrying`
or `claim`. A supplied context can reuse room history. A supplied claim
decides whether to announce an outcome already judged.

The signature snapshot lists methods and properties. The parity check also
reads turn attributes such as `link.request` and `TurnBackend.timeouts`,
and the constructor fields of wire records used by public links, from the
same pinned Python source. Rust exposes the two attributes as accessors. `ChannelCall::new` and `RoomCall::new` generate a fresh correlation ID. Applications can set an explicit ID. Reading either call from JSON uses the same defaults.

The wiring returned by the executor exposes the current environment, its
framework emitter, the realtime outlet and the text tool callbacks. These
are instance APIs. Python keeps their class constructors in private modules.
The conformance inventory reads their instance methods at the pinned Python
commit and checks their public Rust counterparts.

| Python call on the returned wiring | Rust call | Observable contract |
|---|---|---|
| `wiring.env.emitter()`, `await wiring.emit(event_type, **fields)` | `wiring.env.emitter()`, `wiring.emit(event_type, EmitOptions { .. }).await` | The host supplies the callback at each call. It receives the four optional fields, including the difference between omitted and empty data, and can return an error. |
| `wiring.realtime.publish(topic, event)`, `publish_to_room(room_id, event)` | `wiring.realtime().publish(topic, event)`, `publish_to_room(&room_id, event)` | Direct topics and room-specific backend overrides are preserved. Direct channel invocations check their room audience on both operations. The room publication carries its creation mark. |
| `wiring.realtime.subscribe`, `subscribe_to_room`, `unsubscribe`, `close` | The corresponding methods on `wiring.realtime()` | Subscriptions require a host backend. Closing the outlet leaves the host's backend available. Treating the outlet as a generic realtime backend remains in the realtime work. |
| `wiring.tools.build_before_tool_call_hook`, `build_tool_call_hook`, `build_tool_report_hook`, `build_tool_observer_hook` | The corresponding builders on `wiring.tools()` | A channel can gate its call, obtain the model's verdict, report an external result or report a refusal. The external report keeps the provider's actual result, and a claim prevents duplicate reports. |
| `executor.wire(channel)` | `executor.wire(&channel)` | The channel retains the environment selected at registration. A custom intelligence channel can report its answer to the executor's observers. The Rust channel bridge carries the callbacks between crates. |
| `wiring.env.context(room_id, carrying=None)`, `wiring.hook_context(room_id, trigger, carrying=None)` | `roomkit::turns::WiringEnv::context`, `roomkit::turns::TurnWiring::hook_context` | Pass `None` for the omitted context, or `Some(&context)` to borrow one the channel already holds. |

| Python call on the returned wiring | Rust import or output | Observable contract |
|---|---|---|
| Thinking, speaking, thought, tool-round, response and generation builders | Import `roomkit_ai::TurnWiringExt` and call its methods on `roomkit::turns::TurnWiring`. | The event types belong to the AI crate. The inventory reads the trait's concrete receiver in rustdoc. |
| The result returned by `wiring.tools.run_tool_call_chain` and the before-generation hook | `roomkit::turns::SyncPipelineResult<P>` | The result carries the final event, decision, blocking hook, side effects, diagnostics, metadata and fail-closed marker. Mutable collections default to an empty collection for each result. |

The Python worker constructor also takes `media_resolver` and `attached_media`.
These belong to the media API.

## Discussion queue changes

`SpeakQueueChange::TurnSkipped` is Python's `TURN_SKIPPED`, serialized as
`"turn_skipped"`. Before giving a queued turn, the discussion checks whether
an executor currently serves the declared channel, the room's organization
and its enabled turn hooks. A confirmed absence skips the turn and drops any
instruction it held. A committed event also skips the memory of a confirmed
offline agent. A registry read failure leaves the agent eligible.

## Speaking and thinking

The speaking forms were approved on 2026-11-17. `SpeakPolicy`, `Thinker` and
`Classifier` return borrowed boxed futures with `ChannelError`; optional `close`
returns success by default. `SpeakPolicy::forget_room` is synchronous and
fallible. `ClassifierError` retains its diagnostic and optional original cause.
The channel cancels its own running thoughts on close, without closing the
policy, thinker, classifier or model supplied by the application.

`Questions`, `Answers`, `ChoiceOptions`, `Probabilities`, `Judgments`, `Speakers`
and `Script` keep insertion order and share their mapping when cloned.
`insert` replaces an existing value in place; `remove` preserves other entries'
order. `get` clones a value, retaining any nested handles; `snapshot` and `iter`
read an owned outer list without retaining a lock. Each default is independent.
`ScriptedAnswer` holds a typed answer, number or text. `MockClassifierOptions`
uses an owned list of script handles: editing a retained script is observable,
reordering an external list is not, and the last script repeats.

`SharedState`, `SharedRoomEvent` and `SharedAiContext` preserve the complete
objects retained by mocks. `Clone` shares the cell, `snapshot()` requests a copy,
`same_object` tests identity, and `update` runs a short synchronous edit. An edit
must not access that same handle or its clones recursively. No lock is kept
while calling a hook, classifier, thinker or other asynchronous user operation.
The models themselves and provider signatures stay unchanged.

`AiGenerationEvent.ai_context` is `SharedAiContext`. Its constructor accepts an
owned `AiContext` by conversion. A hook writes with
`event.ai_context.update(|context| context.system_prompt = Some("New".into()))`.
Retaining `event.ai_context.clone()` retains the same object; replacing the
handle selects a different object. A thinker receives that selected handle.
The ordinary provider still receives its owned turn context.

`ClassifierSpeakOverrides` supplies optional `state`, `questions`, `decision`
callbacks. Each receives the actual policy and a borrowed standard operation;
calling that operation represents `super()` without recursively dispatching to
the override. No standard work is forced before the callback. Direct method
calls, error boundaries and per-room listening state remain the same.

Carrier durations are `f64`, without sign or integer validation, and carrier
room/channel ids are strings that may be empty. `SpeakDecision::new` validates
that only silence is final. `AnswerOnly::new` accepts an iterable of names, not
one string. Channel `speak_timeout` uses `Duration` (default two seconds, zero
refused); `think_wait` uses `Duration` (default 1500 ms, zero allowed).

`LlmThinkerOptions.timeout` is `Option<f64>` seconds, default `Some(10.0)`:
`None` and positive infinity are unbounded, and a nonpositive value expires
before calling the provider. NaN has the reference zero-wait scheduling bound:
an immediate result can succeed, while a pending call expires. Large finite
values which exceed the runtime clock's range do not panic. `max_tokens` uses
the existing provider context's `Option<u32>`, default `Some(600)`; negative
integers and values above `u32::MAX` accepted by Python are outside this
explicitly approved Rust constructor domain. Instructions default to the
reference prompt when absent or empty, and `reasoning_effort` defaults to None.
`CutReply` is a text carrier; this does not implement audio interruption,
realtime admission or vendor classifiers.

### Public tool and delegated-turn errors

A `ChannelRefusalError` carries the channel's words. It converts into the
existing `roomkit_ai::ToolError::Refused` family at the handler boundary.
`ToolTimeoutError` accepts a tool name and a bound in seconds, and formats its
diagnostic the same way as an expired call. The standalone constructor accepts
the full floating-point domain, including signed zero, infinities and NaN.

`TurnCutShortError` accepts the original message and an optional reason.
`TaskCutShortError` accepts an open-ended reason and an optional narration,
exposes the base by reference and provides the task's `message_for` formatter.
`TaskTurnFailedError` retains the original worker error in an
`Arc<dyn Error + Send + Sync>`. Its display captures the original message at
construction. The standard `Error::source` gives access to the same error.

The local channel driver copies a typed error into an `ErrorRecord`. Both
`ChannelOutput.error` and `DelegatedTaskResult.exception` hold this record
with its class, message, JSON attributes and `causes`. The causes form one
list, nearest first, matching the turn protocol's bound of sixteen. The
turn backend preserves qualified class names supplied in a record. Task
projection recognizes each built-in worker failure by its Rust name or
its full Python codec name. A native error whose class is unavailable
retains its message. A non-finite
timeout becomes JSON null in the record.

For a buffered failure, assign `Some(ErrorRecord::from(&failure))` to the
output's error, where `failure` is a `TaskTurnFailedError`. A stream can
return the same error through its usual error channel. With no independent
failure marker the task exposes the worker's original cause, its last
narration and the wrapper's reason. With an explicit failure marker it
exposes the wrapper record, including the original worker error at
`exception.causes[0]`, alongside the turn's own output and reason.

Records serialized without `causes` read with an empty list. Empty lists
are omitted when writing. `ErrorRecord::new` also starts with an empty
list. Exhaustive literals must set `causes` or update a record created by
`new`. Exhaustive destructuring patterns must include `causes` or `..`
(approved before the first public release, 2026-11-17).

### Framework injection for application channels

An application channel implementing `FrameworkAwareChannel<S>` opts in with
`kit.register_channel(framework_aware(channel))` (approved 2026-11-17).
Ordinary registration accepts the same channels as before. The callback can
look up the inserted channel and upgrade its `WeakRoomKit<S>` to use the
original store. It runs synchronously before the registration event. An error
reaches the caller unchanged and leaves the channel registered without that
event. A duplicate registration fails before injection. Retaining the weak
reference allows the kit and its registered channels to be released together.
