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
`roomkit-openai` and `roomkit-anthropic` implement provider protocols, sharing HTTP
support through `roomkit-provider-http`. `roomkit-mcp` exposes MCP tools and
`roomkit-acp` connects external agents. SQLite and PostgreSQL adapters implement
the storage boundaries.

Hosts supply credentials and decide which providers, tools, transports and stores
to expose. The framework does not read an application's secret files implicitly.
Closing the kit and adapters is part of the host's resource lifecycle.

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
