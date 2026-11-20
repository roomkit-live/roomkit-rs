# Contributing

## Before writing code

Read [ARCHITECTURE.md](ARCHITECTURE.md) for crate boundaries and the
[RoomKit specification](https://github.com/roomkit-live/roomkit-specs) for shared
behavior. The Python implementation is the public API reference. Demonstrate
behavioral changes with deterministic tests and update the specification when
its contract changes.

Use [api/PYTHON_API.md](api/PYTHON_API.md) and `api/mapping.toml` for the
language-specific API forms. Internal plans under `docs/` are local, ignored
files; they are not prerequisites for contributing or running the checks.

## Setup

The toolchain is pinned in `rust-toolchain.toml`; rustup installs it on first
use. Install the commit-message hook once:

```sh
make hooks
```

## Gates

`make all` passes before every commit: formatting, clippy with warnings
denied, tests, the core's tests without its features (`make test-core`: the
core builds and passes without `system-environment`, which reads the clock),
documentation with warnings denied, `cargo deny` (it fails
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

The public API is the Python implementation's, in Rust's terms;
[api/PYTHON_API.md](api/PYTHON_API.md) records the language-specific forms. `make api-parity` reports,
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

## Conformance results

`make test` runs the shared conformance scenarios (`conformance/`) on the
in-memory store, on one thread and on four workers, and on SQLite on one
thread; `make postgres-test` runs them on PostgreSQL. When
`ROOMKIT_CONFORMANCE_JUNIT` names a directory, a relative one read from the
repository's root, each pass also writes `<backend>-<runtime>.xml` there: a
JUnit XML report with one case per scenario, named by its id, under the class
`conformance.<backend>.<runtime>` (runtime `current_thread` or
`multi_thread_4`), passed, failed or skipped, and the commit the run was at,
with whether tracked files differed from it. The scenarios no runner here
runs are skipped with their reason. Without the variable nothing is written.

```sh
ROOMKIT_CONFORMANCE_JUNIT=target/conformance cargo test -p roomkit --test conformance
ROOMKIT_CONFORMANCE_JUNIT=target/conformance cargo test -p roomkit-sqlite --test conformance
```

The scenarios with `setup.executors` run only across the Redis turn executor:
a runner built with the `redis` feature (the SQLite and PostgreSQL runners
always are) runs them when `REDIS_URL` names a Redis server, and skips them
otherwise. Every other scenario runs in process either way. A pass that runs
them names its backend `<backend>+redis-executors`, in its file and its class
(`memory+redis-executors-current_thread.xml`, class
`conformance.memory+redis-executors.current_thread`), so that it is never read
as the pass that skips them.

```sh
REDIS_URL=redis://127.0.0.1:6379 ROOMKIT_CONFORMANCE_JUNIT=target/conformance \
  cargo test -p roomkit --features redis --test conformance
```

roomkit-specs records these files as execution receipts, from a clean commit
(`conformance/TRACEABILITY.md`, "Execution receipts").

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
the dependency allowlist enforced by `scripts/check-core-deps.sh`.
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
