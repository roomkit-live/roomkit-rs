# The gates every change passes before it is committed (CONTRIBUTING.md).

CARGO ?= cargo
# The minimum supported Rust version is written once, as the workspace's
# `rust-version`; its toolchain is that version's first release.
MSRV := $(shell sed -n 's/^rust-version = "\(.*\)"$$/\1/p' Cargo.toml).0
# The examples: every file of examples/, each declared in examples/Cargo.toml.
EXAMPLES := $(sort $(basename $(notdir $(wildcard examples/*.rs))))

.PHONY: all fmt fmt-check clippy test doc deny check-core-deps examples msrv msrv-toolchain hooks conformance-sync

all: fmt-check clippy test doc deny check-core-deps examples

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
