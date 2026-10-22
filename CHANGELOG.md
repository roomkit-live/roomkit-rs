# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) once it
publishes a release.

## [Unreleased]

### Added

- Repository foundation: the `roomkit-core` and `roomkit` crates, workspace
  lints, the `make all` gate, CI, and the architecture, roadmap, step 1 and
  RFC gap documents.
- `roomkit_core::model`: the data model of RFC §5 (rooms, events and every
  content type, bindings and permissions, capabilities, participants, tasks,
  observations, inbound messages, channel outputs, hook results), with its
  invariants checked at construction and its JSON form.
- `roomkit_core::rules`: the RFC's rules as pure functions: permissions and
  delivery planning with solicitation and addressing (§7.5, §10.2, §19.3),
  the visibility of what a source writes (§7.3), transcoding (§6.6), the
  chain-depth limit and reentry budget (§8.3), hook registration checks and
  the SYNC hook chain (§9.1–9.4).
- `roomkit_core::ops`: the effect protocol (`Operation`, `Effect`, `Outcome`,
  `Notice`) and the operations run through it: `Inbound` (RFC §10.1 and
  §10.2: the locked commit pass, hooks, permissions, broadcast, reentry,
  chain depth, injected events, framework events), `CreateRoom`,
  `AttachChannel` and `CloseRoom`.
- `roomkit`: the tokio runtime. `RoomKit` registers channels and hooks,
  creates, attaches and closes rooms, and runs inbound messages; the
  `Channel` and `ConversationStore` traits, `InMemoryStore`, framework
  event subscription, and `roomkit::testing` (`RecordingTransport`,
  `EchoAgent`).
- Examples `echo_room`, `block_and_inject` and `agent_chain`, after the RFC's
  Appendix B.1, B.2 and B.4.
