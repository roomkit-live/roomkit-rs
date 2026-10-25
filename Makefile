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
# one roomkit-specs commit, copied into conformance/scenarios with that commit
# recorded in conformance/SPECS_COMMIT, so CI needs no sibling checkout; `make
# test` runs them (crates/roomkit/tests/conformance). The copy is never edited
# here: change the scenarios in roomkit-specs, commit them there, and sync
# again. SPECS_REF names the commit.
SPECS_DIR ?= ../roomkit-specs
SPECS_REF ?= HEAD

conformance-sync:
	@commit=$$(git -C $(SPECS_DIR) rev-parse --verify "$(SPECS_REF)^{commit}") && \
	rm -rf conformance/scenarios && mkdir -p conformance && \
	git -C $(SPECS_DIR) archive --format=tar "$$commit" conformance/scenarios | tar -x -C . && \
	echo "$$commit" > conformance/SPECS_COMMIT && \
	echo "conformance/ synced from roomkit-specs $$commit"
