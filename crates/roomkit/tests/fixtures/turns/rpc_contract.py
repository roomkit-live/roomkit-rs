"""Generate and verify the RFC 10.6.3 text turn messages with Python's own codec.

The historical probe supplies the event, binding and room. This corpus exercises
the additional fields of a distributed request, RPCs, channel controls and calls
after a turn. The reverse fixture must carry the Rust peer's own ids.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from roomkit.turns import wire
from roomkit.turns._codec import decode_end, decode_item, request_json
from roomkit.turns.host import VERBS

HERE = Path(__file__).parent
MODELS = {
    "request": wire.TurnRequest,
    "frame": wire.FrameEnvelope,
    "control": wire.ControlEnvelope,
    "channel_control": wire.ChannelControlEnvelope,
    "channel_result": wire.ChannelResult,
    "owner": wire.OwnerMessageEnvelope,
    "host_result": wire.HostResult,
}


def corpus(peer: str) -> list[dict[str, Any]]:
    case = next(
        item["message"]
        for line in (HERE / "python-case1.jsonl").read_text().splitlines()
        if (item := json.loads(line))["direction"] == "request"
    )
    turn = f"{peer}-turn"
    call = f"{peer}-call"
    case.update(
        turn_id=turn,
        channel_rows=[case["event"]],
        previous_turn={"turn_id": "prior", "executor_id": "executor"},
    )
    case["context"]["room"]["organization_id"] = "org"
    case["context"]["room"]["updated_at"] = "2026-11-04T07:08:09.399123Z"
    case["context"]["history_start_index"] = 0
    room = case["room_id"]
    channel = case["channel_id"]
    route = {"channel_id": channel, "room_id": room}
    context = dict(
        route,
        tool_call_id="lookup",
        chain_depth=2,
        actor_id="alice",
        allowed_names=["lookup"],
        requester="agent",
        response_metadata={"removed": 1},
    )
    change = {"written": {"answer": [1, None]}, "removed": ["removed"]}
    error = wire.ErrorRecord(
        kind="builtins:RuntimeError",
        message="unavailable",
        args=["unavailable"],
        attributes={"retryable": True},
        causes=[
            wire.ErrorRecord(
                kind="example:Cause", message="original", attributes={"id": 2}
            )
        ],
        traceback="prior call",
        bases=["builtins:Exception"],
    )
    framework_event = {
        "type": "framework_event",
        "event_type": "room_created",
        "room_id": room,
        "channel_id": channel,
        "event_id": "ev",
        "data": {"nested": [None]},
    }
    ephemeral = {
        "type": "ephemeral",
        "channel": f"room:{room}",
        "event": {"type": "thinking"},
    }
    messages: list[tuple[str, Any]] = [
        ("request", wire.TurnRequest.model_validate(case))
    ]
    end = {
        "tool_name": "lookup",
        "tool_id": "id",
        "arguments": {"key": "42"},
        "result": {"value": "answer"},
        "status": "completed",
        "duration_ms": 3,
        "error": None,
        "structured_content": {"found": True},
        "outcome": "served",
        "refused_but_ran": False,
        "intent": "search",
    }
    frames = [
        {"type": "accepted", "executor_id": "executor"},
        {"type": "heartbeat"},
        {
            "type": "output",
            "output": wire.WireOutput(
                responded=True, response_metadata={"round": 1}, error=error
            ),
            "streams": True,
        },
        {"type": "record", "record": {"step": "tool"}},
        {"type": "item", "item": {"kind": "text", "text": "three pieces", "deltas": 3}},
        {
            "type": "item",
            "item": {
                "kind": "tool_start",
                "tool_name": "lookup",
                "tool_id": "id",
                "ran_with": {"key": "42"},
                "arguments": {"key": "x"},
                "intent": "search",
            },
        },
        {"type": "tool_args", "start": 6, "arguments": {"key": "42"}},
        {"type": "tool_ended", "start": 6, "end": end},
        {"type": "item", "item": {"kind": "tool_end", "start": 6, **end}},
        {"type": "steer_result", "steer_id": "s", "loops": 0},
        {
            "type": "input_request",
            "request_id": "input",
            "tool_name": "ask_user",
            "arguments": {"question": "Ready?"},
            **route,
            "tool_call_id": "lookup",
            "actor_id": "alice",
            "created_at": "2026-11-17T00:00:00Z",
        },
        {"type": "input_closed", "request_id": "input", "reason": "answered"},
        {"type": "input_taken", "request_id": "input", "taken": True},
        ephemeral,
        {
            "type": "tool_call",
            "call_id": call,
            "name": "lookup",
            "arguments": {"key": "42"},
            "context": context,
        },
        {
            "type": "host_call",
            "call_id": call,
            "verb": "room_context",
            "arguments": {},
            "context": context,
        },
        {"type": "call_cancel", "call_id": call},
        framework_event,
        {"type": "error", "error": error},
        {"type": "end"},
    ]
    controls = [
        {"type": "pull", "count": 3, "read": 1},
        {"type": "heartbeat"},
        {
            "type": "steer",
            "steer_id": "s",
            "directive": {
                "type": "inject_message",
                "role": "user",
                "content": "Please continue",
            },
        },
        {
            "type": "input_answer",
            "request_id": "input",
            "outcome": "answered",
            "value": "yes",
        },
        {"type": "close"},
        {
            "type": "tool_result",
            "call_id": call,
            "outcome": "served",
            "result": {"parts": [{"type": "text", "text": "42"}]},
            "structured_content": {"found": True},
            "record_changes": change,
        },
        {
            "type": "tool_result",
            "call_id": call,
            "outcome": "refused",
            "error": error,
            "record_changes": change,
        },
        {
            "type": "host_result",
            "call_id": call,
            "result": {"items": [1]},
            "record_changes": change,
        },
        {"type": "cancel", "reason": "done"},
    ]
    for family, entries in [("frame", frames), ("control", controls)]:
        messages.extend(
            (
                family,
                MODELS[family].model_validate(
                    {"turn_id": turn, "seq": i, "message": message}
                ),
            )
            for i, message in enumerate(entries, 1)
        )
    channel_controls = [
        {
            "type": "room_attached",
            **route,
            "organization_id": "org",
            "binding": case["binding"],
        },
        {"type": "room_detached", **route, "organization_id": "org"},
        {
            "type": "committed_event",
            **route,
            "organization_id": "org",
            "event": case["event"],
            "room_hooks": ["decide"],
        },
        {
            "type": "channel_call",
            **route,
            "organization_id": "org",
            "verb": "close_session",
            "arguments": {"a": 1},
            "call_id": call,
            "room_state": {"cursor": 3},
            "previous_turn": {"turn_id": "prior", "executor_id": "executor"},
        },
        {"type": "channel_released", "channel_id": channel},
    ]
    messages.extend(
        ("channel_control", wire.ChannelControlEnvelope.model_validate({"message": m}))
        for m in channel_controls
    )
    messages.extend(
        [
            (
                "channel_result",
                wire.ChannelResult(
                    result={"answer": 42}, executor_id="executor", room_state={}
                ),
            ),
            ("channel_result", wire.ChannelResult(error=error)),
        ]
    )
    route = dict(route, executor_id="executor", turn_id=turn)
    owner = [
        {
            "type": "room_call",
            **route,
            "call_id": call,
            "verb": "room_context",
            "arguments": {},
        },
        {"type": "room_publication", **route, "frame": framework_event},
        {"type": "room_publication", **route, "frame": ephemeral},
    ]
    messages.extend(
        ("owner", wire.OwnerMessageEnvelope.model_validate({"message": m}))
        for m in owner
    )
    messages.append(
        (
            "host_result",
            wire.HostResult(
                call_id=call,
                result={"ok": 1},
                record_changes=wire.RecordChanges.model_validate(change),
            ),
        )
    )
    return [
        {
            "direction": family,
            "message": json.loads(
                request_json(message)
                if family == "request"
                else message.model_dump_json()
            ),
        }
        for family, message in messages
    ]


def dump(items: list[dict[str, Any]]) -> str:
    return "".join(
        json.dumps(item, separators=(",", ":"), sort_keys=True) + "\n" for item in items
    )


if __name__ == "__main__":
    if sys.argv[1:] == ["write-python"]:
        (HERE / "python-rpc.jsonl").write_text(dump(corpus("python")))
    elif sys.argv[1:] == ["read-rust"]:
        output = [
            json.loads(line)
            for line in (HERE / "rust-rpc.jsonl").read_text().splitlines()
        ]
        expected = corpus("rust")
        assert len(output) == len(expected)
        for index, (item, wanted) in enumerate(zip(output, expected, strict=True)):
            family = item["direction"]
            model = MODELS[family].model_validate(item["message"])
            message = item["message"].get("message", {})
            if message.get("type") == "host_call":
                VERBS[message["verb"]].model_validate(message["arguments"])
            elif message.get("type") == "item":
                decode_item(message["item"])
            elif message.get("type") == "tool_ended":
                decode_end(message["end"])
            assert (family, model.model_dump(mode="json")) == (
                wanted["direction"],
                wanted["message"],
            ), index
        print(f"Python read {len(output)} Rust text turn messages")
    else:
        raise SystemExit("expected write-python or read-rust")
