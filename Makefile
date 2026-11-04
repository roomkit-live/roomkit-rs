# The gates every change passes before it is committed (CONTRIBUTING.md).

CARGO ?= cargo
# The minimum supported Rust version is written once, as the workspace's
# `rust-version`; its toolchain is that version's first release.
MSRV := $(shell sed -n 's/^rust-version = "\(.*\)"$$/\1/p' Cargo.toml).0
# The examples: every file of examples/, each declared in examples/Cargo.toml.
EXAMPLES := $(sort $(basename $(notdir $(wildcard examples/*.rs))))

.PHONY: all fmt fmt-check clippy test test-core doc deny check-core-deps examples msrv postgres-test msrv-toolchain hooks conformance-sync conformance-check sqlite-fixture postgres-fixture turns-fixture api-sync api-check-pin api-test api-parity models-sync models-check

all: conformance-check api-check-pin models-check api-test fmt-check clippy test test-core doc api-parity deny check-core-deps examples

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

clippy:
	$(CARGO) clippy --workspace --all-targets --all-features --locked -- -D warnings

test:
	$(CARGO) test --workspace --all-features --locked

# The core's suite on its default features: without `system-environment`,
# so that no operation and no core test builds a record with the model's
# filling constructors, which read the system clock (docs/steps/step-05.md,
# lot 2). `test` runs it with every feature, as the workspace builds it.
test-core:
	$(CARGO) test --package roomkit-core --locked

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
# Recipes read the refs and paths from their environment, quoted, so that no
# value is ever run as a command.
export SPECS_DIR SPECS_REF ALLOW_UNPUSHED

conformance-sync:
	@commit=$$(git -C "$$SPECS_DIR" rev-parse --verify "$${SPECS_REF}^{commit}") && \
	if ! git -C "$$SPECS_DIR" merge-base --is-ancestor "$$commit" origin/main; then \
		if [ "$${ALLOW_UNPUSHED:-}" = 1 ]; then \
			echo "warning: roomkit-specs $$commit is not on origin/main: sync again once it is"; \
		else \
			echo "roomkit-specs $$commit is not on origin/main: push it and fetch first"; exit 1; \
		fi; \
	fi && \
	rm -rf conformance/scenarios && mkdir -p conformance && \
	git -C "$$SPECS_DIR" archive --format=tar "$$commit" conformance/scenarios | tar -x -C . && \
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
	if ! git -C "$$SPECS_DIR" rev-parse --git-dir >/dev/null 2>&1; then \
		echo "conformance-check: no roomkit-specs checkout at $$SPECS_DIR, skipped"; \
	elif git -C "$$SPECS_DIR" merge-base --is-ancestor "$$commit" origin/main 2>/dev/null; then \
		echo "conformance-check: roomkit-specs $$commit is on origin/main"; \
	elif [ "$${ALLOW_UNPUSHED:-}" = 1 ]; then \
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
export ROOMKIT_DIR
SQLITE_FIXTURES := crates/roomkit-sqlite/tests/fixtures

sqlite-fixture:
	@commit=$$(git -C "$$ROOMKIT_DIR" rev-parse --verify HEAD) && \
	if ! git -C "$$ROOMKIT_DIR" diff --quiet HEAD -- src; then \
		echo "roomkit at $$ROOMKIT_DIR has changes under src/: write the fixture from a commit"; exit 1; \
	fi && \
	if ! git -C "$$ROOMKIT_DIR" merge-base --is-ancestor "$$commit" origin/main; then \
		echo "roomkit $$commit is not on origin/main: push it and fetch first"; exit 1; \
	fi && \
	PYTHONDONTWRITEBYTECODE=1 "$$ROOMKIT_DIR/.venv/bin/python" -I \
		$(SQLITE_FIXTURES)/make_fixture.py $(SQLITE_FIXTURES)/python-v14.db && \
	echo "$$commit" > $(SQLITE_FIXTURES)/ROOMKIT_COMMIT && \
	sha256sum $(SQLITE_FIXTURES)/python-v14.db | cut -c1-64 > $(SQLITE_FIXTURES)/FIXTURE_SHA256 && \
	echo "sqlite fixture written by roomkit $$commit"

# roomkit-postgres against a real server. `make test` runs its tests too, and
# without POSTGRES_DSN they skip themselves and prove only that they compile:
# this target fails then. With ROOMKIT_PYTHON naming a roomkit checkout's
# interpreter, Python reads the rows the crate wrote, and its schema is compared
# with Python's. One test binary runs at a time on one database.
postgres-test:
	@test -n "$$POSTGRES_DSN" || { echo "postgres-test: POSTGRES_DSN is not set"; exit 1; }
	$(CARGO) test --locked --package roomkit-postgres -- --test-threads=4

# The cross-read fixture of roomkit-postgres (RMK-989): the rows the Python
# implementation's PostgresStore writes, as JSON, which
# crates/roomkit-postgres/tests/cross_read.rs loads, reads and rewrites with
# nothing lost; its schema.sql is Python's SCHEMA, byte for byte. Both are written
# with the interpreter of a roomkit checkout (ROOMKIT_DIR) at a commit on its
# origin/main, recorded in tests/fixtures/ROOMKIT_COMMIT, the rows against the
# server POSTGRES_DSN names.
POSTGRES_CRATE := crates/roomkit-postgres

postgres-fixture:
	@commit=$$(git -C "$$ROOMKIT_DIR" rev-parse --verify HEAD) && \
	if ! git -C "$$ROOMKIT_DIR" diff --quiet HEAD -- src; then \
		echo "roomkit at $$ROOMKIT_DIR has changes under src/: write the fixture from a commit"; exit 1; \
	fi && \
	if ! git -C "$$ROOMKIT_DIR" merge-base --is-ancestor "$$commit" origin/main; then \
		echo "roomkit $$commit is not on origin/main: push it and fetch first"; exit 1; \
	fi && \
	PYTHONDONTWRITEBYTECODE=1 "$$ROOMKIT_DIR/.venv/bin/python" -c \
		'from roomkit.store.postgres_schema import SCHEMA; import sys; sys.stdout.write(SCHEMA)' \
		> $(POSTGRES_CRATE)/src/schema.sql && \
	PYTHONDONTWRITEBYTECODE=1 "$$ROOMKIT_DIR/.venv/bin/python" -I \
		$(POSTGRES_CRATE)/tests/fixtures/make_fixture.py $(POSTGRES_CRATE)/tests/fixtures/python-rows.json && \
	echo "$$commit" > $(POSTGRES_CRATE)/tests/fixtures/ROOMKIT_COMMIT && \
	sha256sum $(POSTGRES_CRATE)/tests/fixtures/python-rows.json | cut -c1-64 > $(POSTGRES_CRATE)/tests/fixtures/FIXTURE_SHA256 && \
	echo "postgres fixture written by roomkit $$commit"

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
# the test checks; tap.rs then checks rust-case1.jsonl is what the turn
# carries, and tests/fixtures/turns/read_rust.py reads it with Python's own
# models and fails on any message they refuse. That read runs here only,
# not in `make all`.
TURNS_FIXTURES := crates/roomkit/tests/fixtures/turns

turns-fixture:
	@commit=$$(git -C "$$ROOMKIT_DIR" rev-parse --verify HEAD) && \
	if ! git -C "$$ROOMKIT_DIR" diff --quiet HEAD -- src; then \
		echo "roomkit at $$ROOMKIT_DIR has changes under src/: write the fixture from a commit"; exit 1; \
	fi && \
	if ! git -C "$$ROOMKIT_DIR" merge-base --is-ancestor "$$commit" origin/main; then \
		echo "roomkit $$commit is not on origin/main: push it and fetch first"; exit 1; \
	fi && \
	PYTHONDONTWRITEBYTECODE=1 "$$ROOMKIT_DIR/.venv/bin/python" -I \
		$(TURNS_FIXTURES)/make_fixture.py $(TURNS_FIXTURES)/python-case1.jsonl && \
	echo "$$commit" > $(TURNS_FIXTURES)/ROOMKIT_COMMIT && \
	sha256sum $(TURNS_FIXTURES)/python-case1.jsonl | cut -c1-64 > $(TURNS_FIXTURES)/FIXTURE_SHA256 && \
	echo "turns fixture written by roomkit $$commit" && \
	$(CARGO) test -p roomkit --test turns --locked the_probe_s_turn_carries_python_s_messages && \
	PYTHONDONTWRITEBYTECODE=1 "$$ROOMKIT_DIR/.venv/bin/python" -I \
		$(TURNS_FIXTURES)/read_rust.py $(TURNS_FIXTURES)/rust-case1.jsonl

# The Python implementation's public API, which this one mirrors (step 5,
# docs/steps/step-05.md lot 0): roomkit's tests/public_api_signatures.json and
# tests/public_api_names.json at one commit of roomkit's origin/main, copied
# into api/python with that commit recorded in api/python/ROOMKIT_COMMIT and
# the copies' fingerprint in api/python/SNAPSHOT_SHA256, which api-check-pin
# and the parity report check. The copies are never edited here: change the
# snapshot in roomkit, push it, fetch, and sync again. ROOMKIT_REF names the
# commit in the roomkit checkout (ROOMKIT_DIR); ALLOW_UNPUSHED=1 takes one
# origin/main does not hold yet, for a copy synced again before it is pushed.
API_PYTHON := api/python
API_REPORT := target/api-parity
API_SNAPSHOT := public_api_names.json public_api_signatures.json
ROOMKIT_REF ?= origin/main
# The first roomkit commit whose snapshot is the exact public surface
# (RMK-1010): an older one leaves out what was added without breaking.
API_SNAPSHOT_SINCE ?= 6e420487c204b6642b927a2323122c0ec84af8e9
export ROOMKIT_REF API_PYTHON API_SNAPSHOT_SINCE
PYTHON ?= python3

api-sync:
	@commit=$$(git -C "$$ROOMKIT_DIR" rev-parse --verify "$${ROOMKIT_REF}^{commit}") && \
	if ! git -C "$$ROOMKIT_DIR" merge-base --is-ancestor "$$commit" origin/main; then \
		if [ "$${ALLOW_UNPUSHED:-}" = 1 ]; then \
			echo "warning: roomkit $$commit is not on origin/main: sync again once it is"; \
		else \
			echo "roomkit $$commit is not on origin/main: push it and fetch first"; exit 1; \
		fi; \
	fi && \
	if ! git -C "$$ROOMKIT_DIR" merge-base --is-ancestor "$$API_SNAPSHOT_SINCE" "$$commit"; then \
		echo "roomkit $$commit does not hold $$API_SNAPSHOT_SINCE, the first commit whose snapshot is the exact public surface"; \
		exit 1; \
	fi && \
	mkdir -p "$$API_PYTHON" && \
	for file in $(API_SNAPSHOT); do \
		git -C "$$ROOMKIT_DIR" show "$$commit:tests/$$file" > "$$API_PYTHON/$$file.new" || \
			{ rm -f "$$API_PYTHON"/*.new; exit 1; }; \
	done && \
	for file in $(API_SNAPSHOT); do mv "$$API_PYTHON/$$file.new" "$$API_PYTHON/$$file"; done && \
	echo "$$commit" > "$$API_PYTHON/ROOMKIT_COMMIT" && \
	(cd "$$API_PYTHON" && sha256sum $(API_SNAPSHOT) | sha256sum | cut -c1-64) > "$$API_PYTHON/SNAPSHOT_SHA256" && \
	echo "api synced from roomkit $$commit"

# A local gate, which `make all` runs and CI too: the copies are the ones
# their fingerprint names; with a roomkit checkout, their commit is on
# roomkit's origin/main, so a copy synced from a commit not pushed yet is never
# pushed itself, and each copy is that commit's file. Without a checkout, as
# in CI, only the fingerprint is checked; ALLOW_UNPUSHED=1 only warns about the
# commit, for local work.
api-check-pin:
	@fingerprint=$$(cd "$$API_PYTHON" && sha256sum $(API_SNAPSHOT) | sha256sum | cut -c1-64) && \
	if [ "$$fingerprint" != "$$(cat "$$API_PYTHON/SNAPSHOT_SHA256")" ]; then \
		echo "api-check-pin: the copies in $$API_PYTHON differ from SNAPSHOT_SHA256: they are never edited, run make api-sync"; \
		exit 1; \
	fi
	@commit=$$(cat "$$API_PYTHON/ROOMKIT_COMMIT") && \
	if ! git -C "$$ROOMKIT_DIR" rev-parse --git-dir >/dev/null 2>&1; then \
		echo "api-check-pin: no roomkit checkout at $$ROOMKIT_DIR, its commit's check skipped"; exit 0; \
	fi && \
	if git -C "$$ROOMKIT_DIR" merge-base --is-ancestor "$$commit" origin/main 2>/dev/null; then \
		echo "api-check-pin: roomkit $$commit is on origin/main"; \
	elif [ "$${ALLOW_UNPUSHED:-}" = 1 ]; then \
		echo "warning: roomkit $$commit is not on origin/main: sync again once it is"; \
	else \
		echo "api-check-pin: roomkit $$commit is not on origin/main: push it, fetch, and run make api-sync"; \
		exit 1; \
	fi && \
	for file in $(API_SNAPSHOT); do \
		if ! git -C "$$ROOMKIT_DIR" show "$$commit:tests/$$file" | cmp -s - "$$API_PYTHON/$$file"; then \
			echo "api-check-pin: $$API_PYTHON/$$file is not roomkit $$commit's tests/$$file: the copies are never edited, run make api-sync"; \
			exit 1; \
		fi; \
	done

# The OpenAI model catalogue roomkit-openai embeds (HITL e7768b18): the
# Python implementation's `OpenAIAIProvider.available_models()` at one commit
# of roomkit's origin/main, written as JSON by
# crates/roomkit-openai/catalog/export.py, with the interpreter of a roomkit
# checkout (ROOMKIT_DIR), on that commit's source extracted apart, into
# catalog/models.json; the commit recorded in catalog/ROOMKIT_COMMIT and the
# file's fingerprint in catalog/MODELS_SHA256. The copy is never edited here:
# change the catalogue in roomkit, push it, fetch, and sync again. ROOMKIT_REF
# names the commit; ALLOW_UNPUSHED=1 takes one origin/main does not hold yet,
# for a copy synced again before it is pushed.
MODELS := crates/roomkit-openai/catalog
# Writes roomkit's catalogue at the recipe's $$commit to its $$out.
MODELS_EXPORT = tmp=$$(mktemp -d "$${TMPDIR:-/tmp}/models.XXXXXX") && \
	{ git -C "$$ROOMKIT_DIR" archive --format=tar "$$commit" src/roomkit | tar -x -C "$$tmp" && \
	PYTHONDONTWRITEBYTECODE=1 "$$ROOMKIT_DIR/.venv/bin/python" -I $(MODELS)/export.py "$$tmp/src" > "$$out"; \
	status=$$?; rm -rf "$$tmp"; [ $$status -eq 0 ] || { rm -f "$$out"; false; }; }

models-sync:
	@commit=$$(git -C "$$ROOMKIT_DIR" rev-parse --verify "$${ROOMKIT_REF}^{commit}") && \
	if ! git -C "$$ROOMKIT_DIR" merge-base --is-ancestor "$$commit" origin/main; then \
		if [ "$${ALLOW_UNPUSHED:-}" = 1 ]; then \
			echo "warning: roomkit $$commit is not on origin/main: sync again once it is"; \
		else \
			echo "roomkit $$commit is not on origin/main: push it and fetch first"; exit 1; \
		fi; \
	fi && \
	out=$(MODELS)/models.json.new && $(MODELS_EXPORT) && \
	mv "$$out" $(MODELS)/models.json && \
	echo "$$commit" > $(MODELS)/ROOMKIT_COMMIT && \
	sha256sum $(MODELS)/models.json | cut -c1-64 > $(MODELS)/MODELS_SHA256 && \
	echo "models synced from roomkit $$commit"

# A local gate, which `make all` runs: the copy is the one its fingerprint
# names; with a roomkit checkout and its venv, its commit is on roomkit's
# origin/main and the copy is what that commit's catalogue exports, so a copy
# that drifted from Python's fails here. Without them, as in CI, only the
# fingerprint is checked; a catalogue roomkit's origin/main exports otherwise
# than the commit only warns: sync it.
models-check:
	@fingerprint=$$(sha256sum $(MODELS)/models.json | cut -c1-64) && \
	if [ "$$fingerprint" != "$$(cat $(MODELS)/MODELS_SHA256)" ]; then \
		echo "models-check: $(MODELS)/models.json differs from MODELS_SHA256: it is never edited, run make models-sync"; \
		exit 1; \
	fi
	@commit=$$(cat $(MODELS)/ROOMKIT_COMMIT) && \
	if ! git -C "$$ROOMKIT_DIR" rev-parse --git-dir >/dev/null 2>&1 || [ ! -x "$$ROOMKIT_DIR/.venv/bin/python" ]; then \
		echo "models-check: no roomkit checkout with its venv at $$ROOMKIT_DIR, the export's check skipped"; exit 0; \
	fi && \
	if git -C "$$ROOMKIT_DIR" merge-base --is-ancestor "$$commit" origin/main 2>/dev/null; then \
		:; \
	elif [ "$${ALLOW_UNPUSHED:-}" = 1 ]; then \
		echo "warning: roomkit $$commit is not on origin/main: sync again once it is"; \
	else \
		echo "models-check: roomkit $$commit is not on origin/main: push it, fetch, and run make models-sync"; \
		exit 1; \
	fi && \
	out=$$(mktemp "$${TMPDIR:-/tmp}/models.XXXXXX") && $(MODELS_EXPORT) && \
	if ! cmp -s "$$out" $(MODELS)/models.json; then \
		rm -f "$$out"; \
		echo "models-check: $(MODELS)/models.json is not roomkit $$commit's catalogue: it is never edited, run make models-sync"; \
		exit 1; \
	fi && \
	pinned=$$commit && commit=$$(git -C "$$ROOMKIT_DIR" rev-parse --verify origin/main) && \
	if ! { $(MODELS_EXPORT); }; then \
		echo "warning: roomkit's origin/main catalogue could not be exported"; \
	elif ! cmp -s "$$out" $(MODELS)/models.json; then \
		echo "warning: roomkit's origin/main exports another catalogue than $$pinned's: run make models-sync"; \
	fi && \
	rm -f "$$out" && \
	echo "models-check: the catalogue is roomkit $$pinned's"

# The tests of the reader and of the report, on fixtures: no build needed.
api-test:
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) -m unittest discover -s scripts/tests

# The parity report (docs/steps/step-05.md, lot 0): the public surface read
# from rustdoc's HTML, against the pinned snapshot and api/mapping.toml, and
# the gate before the tag (lot 6): it fails on a finding no entry covers, a
# dead entry or a pending entry, the report printed first, and on an input
# it cannot read whole. The report, the surface and the findings as JSON
# stay in target/api-parity.
api-parity: doc
	@mkdir -p $(API_REPORT)
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/rs-surface.py --doc target/doc > $(API_REPORT)/rs-surface.json
	@status=0; PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/api-parity.py $(API_REPORT)/rs-surface.json \
		--json $(API_REPORT)/report.json --gate --defaults crates/*/tests/api_defaults.rs \
		--architecture docs/architecture.md \
		> $(API_REPORT)/report.txt || status=$$?; \
	cat $(API_REPORT)/report.txt; exit $$status
