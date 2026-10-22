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
