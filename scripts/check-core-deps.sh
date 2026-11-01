#!/usr/bin/env bash
# roomkit-core performs no I/O, runs on no async runtime and reads no clock
# but its environment's (docs/architecture.md, "Crates"). Its normal
# dependency tree may hold only the crates allowed below, each there for a
# dependency the architecture lists for the core: a crate joins the list in
# the change that brings it, with its reason. The tree is read for every
# target, since the core is meant for mobile and WebAssembly hosts as much
# as for this one. jiff is built without its `std` feature, so its
# `Timestamp::now` is not compiled into the core.
set -euo pipefail

allowed=(
    roomkit-core
    # The date library behind `Timestamp`.
    jiff jiff-core
    # jiff's atomics on a target without pointer-sized ones.
    portable-atomic portable-atomic-util
    # jiff's static time zones, named under a target no platform matches
    # (`cfg(any())`), so never built.
    jiff-static
    # serde and serde_json.
    serde serde_core serde_derive serde_json itoa memchr zmij
    # The derive macros of serde and thiserror.
    proc-macro2 quote syn unicode-ident
    # thiserror.
    thiserror thiserror-impl
    # tracing.
    tracing tracing-core pin-project-lite once_cell
    # sha2: an instruction's fingerprint (RFC §10.1.1 rule 6).
    sha2 digest block-buffer crypto-common hybrid-array typenum cfg-if cpufeatures
    # cpufeatures' CPU detection on aarch64 (Linux, Android, Apple) and
    # loongarch64 Linux, which reads the system's feature flags.
    libc
)

dependencies=$(
    cargo tree --package roomkit-core --edges normal --prefix none --format '{p}' --locked \
        --target all \
        | awk '{print $1}' \
        | sort -u
)

unexpected=$(comm -23 <(echo "$dependencies") <(printf '%s\n' "${allowed[@]}" | sort -u))
if [[ -n "$unexpected" ]]; then
    echo "roomkit-core depends on crates its allow list does not name:" >&2
    echo "$unexpected" >&2
    exit 1
fi

jiff_features=$(
    cargo tree --package roomkit-core --edges normal,features --prefix none --locked \
        --target all --invert jiff \
        | grep -o 'jiff feature "[^"]*"' \
        | sort -u
)
if grep -q 'jiff feature "std"' <<<"$jiff_features"; then
    echo "roomkit-core builds jiff with std, which reads the system clock:" >&2
    echo "$jiff_features" >&2
    exit 1
fi

echo "roomkit-core dependencies: the allow list only, jiff without std"
