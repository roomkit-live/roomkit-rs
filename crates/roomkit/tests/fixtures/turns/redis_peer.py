"""Python peer for the cross-language Redis turn test (§10.6)."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import roomkit
from roomkit import EventType


async def owner(prefix: str) -> None:
    from tests.test_turns_redis import _owner, _said

    kit = await _owner(prefix, "worker")
    try:
        result = await kit.process_inbound(_said("Hi."), room_id="r")
        if result.error:
            raise AssertionError(result.error)
        rows = [
            event
            for event in await kit.store.list_events("r")
            if event.type == EventType.MESSAGE
        ]
        if len(rows) != 2:
            raise AssertionError(f"expected two messages, got {len(rows)}")
        question, answer = rows
        print(
            json.dumps(
                {
                    "input": [question.source.channel_id, question.content.body],
                    "answer": [answer.source.channel_id, answer.content.body],
                    "responds_to_input": answer.responds_to == question.id,
                    "executor_id": answer.metadata.get("executor_id"),
                    "loop_end_reason": result.response_metadata["turns"]["worker"][
                        "loop_end_reason"
                    ],
                }
            ),
            flush=True,
        )
    finally:
        await kit.close()


async def main(role: str) -> None:
    # ROOMKIT_PYTHON names the reference checkout's interpreter, as in cross_read.
    checkout = Path(roomkit.__file__).resolve().parents[2]
    if not (checkout / "tests/test_turns_redis.py").is_file():
        raise RuntimeError(
            "ROOMKIT_PYTHON must point to an editable roomkit checkout with its tests"
        )
    sys.path.insert(0, str(checkout))
    prefix = os.environ["ROOMKIT_INTEROP_PREFIX"]
    if role == "owner":
        await owner(prefix)
    elif role == "executor":
        from tests.redis_turn_executor import main as serve

        # The reference executor receives context from its owner's turn request.
        await serve(
            os.environ["REDIS_URL"],
            prefix,
            os.environ["ROOMKIT_INTEROP_EXECUTOR_ID"],
            "",
        )
    else:
        raise ValueError(f"unknown peer role {role}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
