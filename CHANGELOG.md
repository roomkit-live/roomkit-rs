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
