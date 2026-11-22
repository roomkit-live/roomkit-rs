# Architecture

RoomKit separates conversation decisions from the I/O that carries them out.
Applications compose the runtime with channels, providers and stores; external
services are explicit dependencies owned by the host.

## Core and runtime

`roomkit-core` contains the models, validation rules and operation state machines.
Operations perform no I/O: no clock reads, sockets, files, threads or locks. They
receive their environment and describe effects for a driver to execute.
The dependency allowlist is enforced by `scripts/check-core-deps.sh`.

The optional `system-environment` feature supplies model convenience constructors
with system timestamps and random identifiers. It is off by default in the core
and enabled by the runtime; operations still use their supplied environment.

`roomkit` drives those operations on Tokio, manages rooms and registered channels,
and supplies hooks, permissions, routing, lifecycle and orchestration. Its store
and channel traits are the application's integration boundaries. The core's
effect protocol is an implementation interface, not a replacement for this API.

## Adapters

`roomkit-ai` implements the provider-independent AI channel and tool loop.
The model providers implement its provider contract outside this repository, in
[roomkit-providers](https://github.com/roomkit-live/roomkit-providers), which
depends on these crates. `roomkit-mcp` exposes MCP tools and
`roomkit-acp` connects external agents. SQLite and PostgreSQL adapters implement
the storage boundaries.

Hosts supply credentials and decide which providers, tools, transports and stores
to expose. The framework does not read an application's secret files implicitly.
Closing the kit and adapters is part of the host's resource lifecycle.

## Speaking and thinking

AI channels and agents can ask a `SpeakPolicy` before ingesting an ordinary
message. A silent decision still feeds memory and can schedule a `Thinker` for
that room. The worker retains its previous thought, coalesces waiting inputs to
the latest one, and reports completions through the turn's tracked hooks. A
thought completed within `think_wait` can ask the policy again; a later thought
does not start a response by itself. Instructions and discussion strategy turns
keep their existing admission path. Closing the channel cancels its room workers;
the host continues to own the supplied policy, thinker and provider.

Classifier mappings and mock inputs use local shared handles to preserve Python's
observable order and aliases. `AiGenerationEvent::ai_context` is a
`SharedAiContext`: hooks use `snapshot()` to read a value and `update()` to edit the
context retained by a thinker. Provider requests still receive an owned snapshot.

## External tool handlers

`AiChannelOptions::external_tool_handler` and
`AcpChannelOptions::external_tool_handler` accept an `Arc<dyn ExternalToolHandler>`.
`PolicyExternalToolHandler` checks the tool policy before the registered
`BEFORE_TOOL_USE` hooks. Applications can implement a handler to wait for approval
and delegate reporting through its embedded `ExternalToolHandlerState`.
AI routes undeclared provider calls to this handler and keeps local or withdrawn
tools on its own execution path. A call already served by the provider keeps its
original outcome. Pending calls can be denied, rewritten, or given a replacement
result; reports retain the full result before the model's result is bounded.
Cancellation reports each occurrence once, preserving an already known outcome.
Kit-owned recovery is tracked observer work; standalone recovery is joined by the
channel's existing close grace and does not become an addressable AI turn.
AI neither starts nor stops the handler's service.

For ACP, the service starts after the connection handshake and stops once after
transport closure. A reconnect retains the running service.

ACP only accepts approval or refusal: replacement arguments or a replacement
result are refused because the protocol cannot apply them. A result the agent
produces despite refusal remains observable as `refused_but_ran`. Reports preserve
their turn's context and reach observers once even when the handler raises after
delegation. The runtime supplies the hook bridge; it does not depend on the AI or
ACP crates.

## Host sandbox executors

`AiChannelOptions::sandbox` accepts a shared `SandboxExecutor`. The host chooses
its execution environment; RoomKit provides no process runner and promises no
isolation. The channel re-reads the declared catalogue each turn and at call
boundaries.
A removed command can reach the host handler; a catalogue failure at a call
gate ends the turn and cuts its pending sibling calls. Exact prefixed names
retain ordinary policy, schema checks and Tool Search. Active served skills
require their host's execution approval before a
sandbox command runs. Only `sandbox_bash` carries its own default bound; an
explicit timeout for that name still wins. Channel close ends running calls
before closing the sandbox, including one with an empty catalogue. Providers
and routers remain owned by their callers.

## Contracts and verification

The [RoomKit specification](https://github.com/roomkit-live/roomkit-specs) defines
shared behavior. The Python implementation is the API reference; names and
language-specific forms are tracked in [api/PYTHON_API.md](api/PYTHON_API.md) and
`api/mapping.toml`, against a pinned Python API snapshot.

Tests exercise deterministic operation traces, runtime integration and shared
conformance scenarios. Native Rust API documentation and examples show the
current supported interfaces. See [CONTRIBUTING.md](CONTRIBUTING.md) for the
required gates. Internal planning documents are not required to build or test
a public checkout.
