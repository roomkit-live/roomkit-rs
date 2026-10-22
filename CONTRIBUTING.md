# Contributing

## Before writing code

- Read [docs/architecture.md](docs/architecture.md). A change that departs
  from it updates it first.
- The RoomKit RFC (`roomkit-specs/roomkit-rfc.md`) is normative. Code is
  written from its text. When the text is silent or ambiguous, record the gap
  in [docs/rfc-gaps.md](docs/rfc-gaps.md) with the decision taken; never settle
  it by reading another implementation.
- Work follows the current step's plan in [docs/steps/](docs/steps/).

## Setup

The toolchain is pinned in `rust-toolchain.toml`; rustup installs it on first
use. Install the commit-message hook once:

```sh
make hooks
```

## Gates

`make all` passes before every commit: formatting, clippy with warnings
denied, tests, documentation with warnings denied, `cargo deny` when it is
installed, and the check that `roomkit-core` stays free of I/O dependencies.
Run `make msrv` when a change adds a dependency or uses a recent standard
library API.

## Code

- English everywhere: code, comments, documentation, examples, test data.
- Public names are plain words, taken from the RFC where it names the concept.
- Library code never panics on input: no `unwrap`, `expect`, `panic!`,
  indexing or string slicing outside tests. Errors are `thiserror` types.
- A lint is silenced with `#[expect(lint, reason = "...")]`, never `allow`.
- Every public item is documented; an item implementing an RFC rule cites it
  (`RFC §7.5 rule 2`).
- One responsibility per function: a function either orchestrates named steps
  or performs one.
- `roomkit-core` performs no I/O: no clock reads, files, sockets, threads or
  locks (`crates/roomkit-core/clippy.toml` refuses them). Time and identifiers
  come from the operation's `Environment`.

## Tests

- One behaviour per test, named after it:
  `muted_source_commits_the_event_blocked`.
- A test that checks an RFC rule names the section in its module or its name.
- Operations are tested by their trace: scripted outcomes in, expected effects
  and result out.

## Dependencies

Ask before adding a production dependency. `roomkit-core` may depend only on
what [docs/architecture.md](docs/architecture.md#crates) lists for it.
`deny.toml` lists the licenses and sources the graph may contain.

## Commits

- Conventional prefixes with a scope: `feat(core): ...`, `fix(runtime): ...`,
  `docs: ...`, `chore: ...`.
- Reference RoomKit's own tickets (`RMK-...`). The repository is public: never
  name a downstream application or its tickets. The commit-msg hook and CI
  refuse it.
- Add a line to `CHANGELOG.md` under `Unreleased` for every change a user of
  the crates would notice.
