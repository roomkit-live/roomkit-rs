"""Read the messages roomkit-rs's turn backend carries for the probe's turn
with the Python implementation's own models, and fail on any it refuses.

The file is `rust-case1.jsonl`, which roomkit-rs's test
`crates/roomkit/tests/turns/tap.rs` checks and writes again with
`ROOMKIT_WRITE_TURNS_FIXTURE=1`, one message per line:

    {"direction": "request", "message": <a TurnRequest>}
    {"direction": "frame", "message": <a FrameEnvelope>}
    {"direction": "control", "message": <a ControlEnvelope>}

Each message is validated as Python's in-memory backend parses what it
carries (`roomkit.turns.wire`), its stream items decoded as its owner
decodes them (`roomkit.turns._codec.decode_item`, the stream's end read
before it), and the arguments of a
`send_event` host call as its owner reads them (`roomkit.turns.host`).
`make turns-fixture` runs it with a roomkit checkout's interpreter:

    <roomkit>/.venv/bin/python -I read_rust.py rust-case1.jsonl

It prints the members a Python model drops, those of the RFC it does not
model, and exits non-zero on the first message it cannot read.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from roomkit.turns._codec import decode_item
from roomkit.turns.host import VERBS
from roomkit.turns.wire import ControlEnvelope, FrameEnvelope, TurnRequest

MODELS = {"request": TurnRequest, "frame": FrameEnvelope, "control": ControlEnvelope}


def dropped(path: str, before: Any, after: Any, out: set[str]) -> None:
    """The members of *before* that *after*, its parse written back, lacks."""
    if isinstance(before, dict) and isinstance(after, dict):
        for member, value in before.items():
            if member not in after:
                out.add(f"{path}.{member}")
            else:
                dropped(f"{path}.{member}", value, after[member], out)
    elif isinstance(before, list) and isinstance(after, list):
        for value, rewritten in zip(before, after, strict=False):
            dropped(f"{path}[]", value, rewritten, out)


def read(path: Path) -> int:
    lost: set[str] = set()
    lines = path.read_text().splitlines()
    for number, line in enumerate(lines, start=1):
        entry = json.loads(line)
        family, message = entry["direction"], entry["message"]
        parsed = MODELS[family].model_validate(message)
        written = json.loads(parsed.model_dump_json())
        dropped(family, message, written, lost)
        inner = message.get("message", {})
        # The stream's end is the owner's to read before any decoding, as
        # its reader reads it (`_owner_turn.OwnerTurn.read_item`).
        if inner.get("type") == "item" and inner["item"].get("kind") != "stream_end":
            decode_item(inner["item"])
        if inner.get("type") == "host_call":
            VERBS[inner["verb"]].model_validate(inner["arguments"])
        print(f"{number:2} {family:8} {inner.get('type', 'request')}: read")
    for member in sorted(lost):
        print(f"   not modelled by Python: {member}")
    print(f"{len(lines)} messages read")
    return 0


if __name__ == "__main__":
    sys.exit(read(Path(sys.argv[1])))
