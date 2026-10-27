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
denied, tests, documentation with warnings denied, `cargo deny` (it fails
when cargo-deny is not installed: `cargo install cargo-deny --locked`), the
check that `roomkit-core` depends, on every target, only on the crates its
allow list names (`scripts/check-core-deps.sh`), every example, each of
which checks what it shows (`make examples`), the pin of the Python
implementation's public API snapshot (`make api-check-pin`) and the tests of
the parity report's scripts (`make api-test`), which need Python 3.11 or
later (`tomllib`) and git. Run `make msrv`
when a change adds a dependency or uses a recent standard library API: the
minimum supported Rust version is the workspace's `rust-version`, written
once in `Cargo.toml` (`make msrv-toolchain` installs it). CI checks the
dependency graph against the security advisories every week.

## Public API

The public API is the Python implementation's, in Rust's terms (step 5,
[docs/steps/step-05.md](docs/steps/step-05.md)). `make api-parity` reports,
from rustdoc's HTML, what differs from Python's snapshot pinned in
`api/python/` (`make api-sync` pins another roomkit commit), and
`api/mapping.toml` holds what its rules do not give. A change to the public
API keeps the report free of findings no entry covers, and of dead entries.

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

## Examples

- An example is one file, `examples/<name>.rs`, declared in
  `examples/Cargo.toml`. It takes the name of the Python example that
  covers the same, when one does.
- It shows the roomkit calls first: what is not roomkit, the kit's clock,
  stand-in agents and transports, waiting and printing, lives in
  `examples/shared/`.
- It checks what it shows with `shared::Checks` and returns its exit code,
  so `make examples` fails when it no longer holds. It needs no network and
  no key.
- Its header comment says what it shows, the RFC sections, and how to run
  it.

## Dependencies

Ask before adding a production dependency. `roomkit-core` may depend only on
what [docs/architecture.md](docs/architecture.md#crates) lists for it.
`deny.toml` lists the licenses and sources the graph may contain.

## Commits

- A subject of at most 72 characters, `type(scope): summary`, the scope
  always given: `feat(core): ...`, `fix(runtime): ...`, `docs(rfc-gaps): ...`,
  `chore(ci): ...`. The body says what changed and why, and names the ticket.
- Reference RoomKit's own tickets (`RMK-...`). The repository is public: never
  name a downstream application or its tickets. The commit-msg hook and CI
  check both rules (`scripts/check-commit-message.sh`; `make hooks` installs
  the hook).
- Add a line to `CHANGELOG.md` under `Unreleased` for every change a user of
  the crates would notice.
