#!/usr/bin/env bash
# roomkit-core performs no I/O and runs on no async runtime
# (docs/architecture.md, "Crates"). Fail when its normal dependency tree pulls
# in a runtime or an I/O stack, directly or transitively.
set -euo pipefail

forbidden='^(tokio|async-std|smol|mio|futures-executor|hyper|reqwest|ureq|pyo3)$'

dependencies=$(
    cargo tree --package roomkit-core --edges normal --prefix none --format '{p}' --locked \
        | awk '{print $1}' \
        | sort -u
)

if offending=$(grep -E "$forbidden" <<<"$dependencies"); then
    echo "roomkit-core must not depend on an async runtime or an I/O stack, found:" >&2
    echo "$offending" >&2
    exit 1
fi

echo "roomkit-core dependencies: no async runtime, no I/O stack"
