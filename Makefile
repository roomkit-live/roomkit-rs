# The gates every change passes before it is committed (CONTRIBUTING.md).

CARGO ?= cargo
MSRV := 1.92.0

.PHONY: all fmt fmt-check clippy test doc deny check-core-deps msrv hooks

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

msrv:
	$(CARGO) +$(MSRV) check --workspace --all-targets --all-features --locked

hooks:
	git config core.hooksPath .githooks
