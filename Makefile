# The gates every change passes before it is committed (CONTRIBUTING.md).

CARGO ?= cargo
# The minimum supported Rust version is written once, as the workspace's
# `rust-version`; its toolchain is that version's first release.
MSRV := $(shell sed -n 's/^rust-version = "\(.*\)"$$/\1/p' Cargo.toml).0
# The examples: every file of examples/, each declared in examples/Cargo.toml.
EXAMPLES := $(sort $(basename $(notdir $(wildcard examples/*.rs))))

.PHONY: all fmt fmt-check clippy test doc deny check-core-deps examples msrv msrv-toolchain hooks

all: fmt-check clippy test doc deny check-core-deps

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

deny:
	@if command -v cargo-deny >/dev/null 2>&1; then \
		$(CARGO) deny check; \
	else \
		echo "deny: skipped, cargo-deny is not installed (cargo install cargo-deny --locked)"; \
	fi

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
