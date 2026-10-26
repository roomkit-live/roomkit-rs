# The gates every change passes before it is committed (CONTRIBUTING.md).

CARGO ?= cargo
# The minimum supported Rust version is written once, as the workspace's
# `rust-version`; its toolchain is that version's first release.
MSRV := $(shell sed -n 's/^rust-version = "\(.*\)"$$/\1/p' Cargo.toml).0
# The examples: every file of examples/, each declared in examples/Cargo.toml.
EXAMPLES := $(sort $(basename $(notdir $(wildcard examples/*.rs))))

.PHONY: all fmt fmt-check clippy test doc deny check-core-deps examples msrv msrv-toolchain hooks conformance-sync conformance-check sqlite-fixture turns-fixture

all: conformance-check fmt-check clippy test doc deny check-core-deps examples

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

clippy:
	$(CARGO) clippy --workspace --all-targets --all-features --locked -- -D warnings

test:
	$(CARGO) test --workspace --all-features --locked

doc:
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --workspace --no-deps --all-features --locked

# A gate, never skipped: without cargo-deny the check fails and says how to
# install it.
deny:
	@command -v cargo-deny >/dev/null 2>&1 || { \
		echo "deny: cargo-deny is not installed (cargo install cargo-deny --locked)" >&2; \
		exit 1; \
	}
	$(CARGO) deny check

check-core-deps:
	scripts/check-core-deps.sh

# Each example checks what it shows and exits non-zero when a check fails.
examples:
	$(CARGO) build --examples --locked --package roomkit-examples
	@for example in $(EXAMPLES); do \
		echo "== $$example"; \
		$(CARGO) run --quiet --locked --example "$$example" || exit 1; \
	done

msrv:
	$(CARGO) +$(MSRV) check --workspace --all-targets --all-features --locked

msrv-toolchain:
	rustup toolchain install $(MSRV) --profile minimal

hooks:
	git config core.hooksPath .githooks

# The shared conformance suite (roomkit-specs conformance/): its scenarios at
# one commit of roomkit-specs' origin/main, copied into conformance/scenarios
# with that commit recorded in conformance/SPECS_COMMIT and the copy's
# fingerprint in conformance/SCENARIOS_SHA256, which the suite checks; `make
# test` runs them (crates/roomkit/tests/conformance).
# The copy is never edited here: change the scenarios in roomkit-specs, push
# them, fetch, and sync again. SPECS_DIR is the roomkit-specs checkout beside
# this repository's main one, a worktree's included; SPECS_REF names the
# commit. ALLOW_UNPUSHED=1 takes a commit origin/main does not hold yet, for a
# copy synced again before it is pushed.
SPECS_DIR ?= $(abspath $(dir $(shell git rev-parse --path-format=absolute --git-common-dir))../roomkit-specs)
SPECS_REF ?= origin/main

conformance-sync:
	@commit=$$(git -C "$(SPECS_DIR)" rev-parse --verify "$(SPECS_REF)^{commit}") && \
	if ! git -C "$(SPECS_DIR)" merge-base --is-ancestor "$$commit" origin/main; then \
		if [ "$(ALLOW_UNPUSHED)" = 1 ]; then \
			echo "warning: roomkit-specs $$commit is not on origin/main: sync again once it is"; \
		else \
			echo "roomkit-specs $$commit is not on origin/main: push it and fetch first"; exit 1; \
		fi; \
	fi && \
	rm -rf conformance/scenarios && mkdir -p conformance && \
	git -C "$(SPECS_DIR)" archive --format=tar "$$commit" conformance/scenarios | tar -x -C . && \
	echo "$$commit" > conformance/SPECS_COMMIT && \
	(cd conformance && find scenarios -type f | LC_ALL=C sort | xargs sha256sum | sha256sum | cut -c1-64) \
		> conformance/SCENARIOS_SHA256 && \
	echo "conformance synced from roomkit-specs $$commit"

# A local gate, which `make all` runs first: the copy's roomkit-specs commit is
# on roomkit-specs' origin/main, so a copy synced from a commit not pushed yet
# is never pushed itself. Skipped without a roomkit-specs checkout, as in CI;
# ALLOW_UNPUSHED=1 only warns, for local work.
conformance-check:
	@commit=$$(cat conformance/SPECS_COMMIT) && \
	if ! git -C "$(SPECS_DIR)" rev-parse --git-dir >/dev/null 2>&1; then \
		echo "conformance-check: no roomkit-specs checkout at $(SPECS_DIR), skipped"; \
	elif git -C "$(SPECS_DIR)" merge-base --is-ancestor "$$commit" origin/main 2>/dev/null; then \
		echo "conformance-check: roomkit-specs $$commit is on origin/main"; \
	elif [ "$(ALLOW_UNPUSHED)" = 1 ]; then \
		echo "warning: roomkit-specs $$commit is not on origin/main: sync again once it is"; \
	else \
		echo "conformance-check: roomkit-specs $$commit is not on origin/main: push it, fetch, and run make conformance-sync"; \
		exit 1; \
	fi

# The cross-read fixture of roomkit-sqlite (step 4, decision 2): a SQLite
# file the Python implementation's SQLiteStore writes, which
# crates/roomkit-sqlite/tests/cross_read.rs reads and rewrites with nothing
# lost. It is written by tests/fixtures/make_fixture.py with the interpreter
# of a roomkit checkout (ROOMKIT_DIR, beside this repository's main one),
# whose commit, on roomkit's origin/main, is recorded in
# tests/fixtures/ROOMKIT_COMMIT, and the file's fingerprint in
# tests/fixtures/FIXTURE_SHA256, which the test checks. The file is never
# edited here: change the generator, and write it again.
ROOMKIT_DIR ?= $(abspath $(dir $(shell git rev-parse --path-format=absolute --git-common-dir))../roomkit)
SQLITE_FIXTURES := crates/roomkit-sqlite/tests/fixtures

sqlite-fixture:
	@commit=$$(git -C "$(ROOMKIT_DIR)" rev-parse --verify HEAD) && \
	if ! git -C "$(ROOMKIT_DIR)" diff --quiet HEAD -- src; then \
		echo "roomkit at $(ROOMKIT_DIR) has changes under src/: write the fixture from a commit"; exit 1; \
	fi && \
	if ! git -C "$(ROOMKIT_DIR)" merge-base --is-ancestor "$$commit" origin/main; then \
		echo "roomkit $$commit is not on origin/main: push it and fetch first"; exit 1; \
	fi && \
	PYTHONDONTWRITEBYTECODE=1 "$(ROOMKIT_DIR)/.venv/bin/python" -I \
		$(SQLITE_FIXTURES)/make_fixture.py $(SQLITE_FIXTURES)/python-v12.db && \
	echo "$$commit" > $(SQLITE_FIXTURES)/ROOMKIT_COMMIT && \
	sha256sum $(SQLITE_FIXTURES)/python-v12.db | cut -c1-64 > $(SQLITE_FIXTURES)/FIXTURE_SHA256 && \
	echo "sqlite fixture written by roomkit $$commit"

# The turn backend's cross-read fixture (step 4, milestone 7): the messages
# the Python implementation's in-memory backend carries for one turn, a
# streamed answer whose tool sends an event through its turn, which
# crates/roomkit/src/turns/tests/cross_read.rs parses and rewrites with
# nothing lost; and the other way, Python parsing the messages this crate
# carries for the same turn (rust-case1.jsonl, which
# crates/roomkit/tests/turns/tap.rs writes with ROOMKIT_WRITE_TURNS_FIXTURE=1
# and otherwise checks). It is written by tests/fixtures/turns/make_fixture.py
# with the interpreter of a roomkit checkout (ROOMKIT_DIR), whose commit, on
# roomkit's origin/main, is recorded in tests/fixtures/turns/ROOMKIT_COMMIT,
# and the file's fingerprint in tests/fixtures/turns/FIXTURE_SHA256, which
# the test checks; tests/fixtures/turns/read_rust.py then reads
# rust-case1.jsonl with Python's own models and fails on any message they
# refuse.
TURNS_FIXTURES := crates/roomkit/tests/fixtures/turns

turns-fixture:
	@commit=$$(git -C "$(ROOMKIT_DIR)" rev-parse --verify HEAD) && \
	if ! git -C "$(ROOMKIT_DIR)" diff --quiet HEAD -- src; then \
		echo "roomkit at $(ROOMKIT_DIR) has changes under src/: write the fixture from a commit"; exit 1; \
	fi && \
	if ! git -C "$(ROOMKIT_DIR)" merge-base --is-ancestor "$$commit" origin/main; then \
		echo "roomkit $$commit is not on origin/main: push it and fetch first"; exit 1; \
	fi && \
	PYTHONDONTWRITEBYTECODE=1 "$(ROOMKIT_DIR)/.venv/bin/python" -I \
		$(TURNS_FIXTURES)/make_fixture.py $(TURNS_FIXTURES)/python-case1.jsonl && \
	echo "$$commit" > $(TURNS_FIXTURES)/ROOMKIT_COMMIT && \
	sha256sum $(TURNS_FIXTURES)/python-case1.jsonl | cut -c1-64 > $(TURNS_FIXTURES)/FIXTURE_SHA256 && \
	echo "turns fixture written by roomkit $$commit" && \
	PYTHONDONTWRITEBYTECODE=1 "$(ROOMKIT_DIR)/.venv/bin/python" -I \
		$(TURNS_FIXTURES)/read_rust.py $(TURNS_FIXTURES)/rust-case1.jsonl
