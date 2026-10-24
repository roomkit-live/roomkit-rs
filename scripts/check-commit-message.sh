#!/bin/sh
# Checks one commit message, given as a file, against CONTRIBUTING.md:
# - a subject of at most 72 characters, `type(scope): summary`;
# - no downstream application or its tickets named anywhere: the repository
#   will be public, and messages name RoomKit's own tickets (RMK-...).
# The commit-msg hook (`make hooks`) and CI run it, so the rule lives once.
set -eu

message=$(grep -v '^#' "$1")
subject=$(printf '%s\n' "$message" | sed -n '1p')
status=0

if ! printf '%s' "$subject" |
    grep -qE '^(feat|fix|docs|chore|refactor|perf|test|build|ci|style|revert)\([a-z0-9][a-z0-9_./-]*\)!?: [^ ]'; then
    echo "commit message: the subject is not \`type(scope): summary\` (CONTRIBUTING.md)" >&2
    status=1
fi

length=$(printf '%s' "$subject" | wc -m)
if [ "$length" -gt 72 ]; then
    echo "commit message: the subject has $length characters, 72 at most (CONTRIBUTING.md)" >&2
    status=1
fi

if printf '%s\n' "$message" | grep -iqE '\bluge\b|\bLUG-[0-9]+'; then
    echo "commit message: it names a downstream application (CONTRIBUTING.md)" >&2
    status=1
fi

exit "$status"
