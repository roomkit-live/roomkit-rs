"""A replacement worker sees its predecessor's private tool result via the owner (§10.6.3)."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from uuid import uuid4

import roomkit
from redis_process import line, namespace, output, process, serving, signal
from roomkit import Agent, EventType, TurnExecutor
from roomkit.providers.ai.base import AIContext, AIResponse, AIToolResultPart
from roomkit.providers.ai.mock import MockAIProvider
from roomkit.turns.redis import RedisTurnBackend


async def check_history(owner: list[str]) -> None:
    peer = [sys.executable, str(Path(__file__).resolve())]
    secret = "lookup-" + uuid4().hex
    first, second = "first-" + uuid4().hex[:12], "second-" + uuid4().hex[:12]
    async with namespace() as env, process([*owner, "history-owner"], env) as room:
        async with serving(
            [*peer, "write"], {**env, "ROOMKIT_TOOL_RESULT": secret}, first
        ):
            await signal(room, b"start")
            await line(room, b"stored")
        async with serving([*peer, "read"], env, second):
            await signal(room, b"next")
            actual = json.loads(await output(room))
    expected = [["stored", first], [secret, second]]
    if actual != expected:
        raise AssertionError(
            f"tool history after replacing the executor: {actual!r} != {expected!r}"
        )


class Reader(MockAIProvider):
    """Answer from the previous result actually present in the model's tool message."""

    async def generate(self, context: AIContext) -> AIResponse:
        results = [
            part
            for message in context.messages
            if message.role == "tool" and isinstance(message.content, list)
            for part in message.content
            if isinstance(part, AIToolResultPart)
            and part.tool_call_id == "c1"
            and part.name == "lookup"
            and not part.is_error
        ]
        if len(results) != 1 or not isinstance(results[0].result, str):
            return AIResponse(content=f"found {len(results)} matching lookup results")
        return AIResponse(content=results[0].result)


async def serve(role: str) -> None:
    from tests.redis_turn_executor import TIMEOUTS
    from tests.test_executor_without_store import CALL, LOOKUP, _agent

    async def lookup(name: str, arguments: dict[str, object]) -> str:
        return os.environ["ROOMKIT_TOOL_RESULT"]

    if role == "write":
        agent = _agent("worker", CALL, AIResponse(content="stored"), handler=lookup)
    else:
        agent = Agent("worker", provider=Reader(), tools=[LOOKUP])
    backend = RedisTurnBackend(
        os.environ["REDIS_URL"],
        key_prefix=os.environ["ROOMKIT_INTEROP_PREFIX"],
        timeouts=TIMEOUTS,
    )
    worker = TurnExecutor(
        lambda _: agent,
        hosts={"worker"},
        executor_id=os.environ["ROOMKIT_INTEROP_EXECUTOR_ID"],
        timeouts=TIMEOUTS,
    )
    try:
        await backend.serve(worker)
        print("ready", flush=True)
        await asyncio.to_thread(sys.stdin.readline)
    finally:
        await backend.close()
        await worker.close()


async def owner() -> None:
    from tests.test_turns_redis import _owner, _said

    await asyncio.to_thread(sys.stdin.readline)
    kit = await _owner(os.environ["ROOMKIT_INTEROP_PREFIX"], "worker")
    try:
        await kit.process_inbound(_said("Look it up."), room_id="r")
        print("stored", flush=True)
        await asyncio.to_thread(sys.stdin.readline)
        await kit.process_inbound(_said("What did it return?"), room_id="r")
        rows = await kit.store.list_events("r")
        answers = [
            [r.content.body, r.metadata.get("executor_id")]
            for r in rows
            if r.type == EventType.MESSAGE and r.source.channel_id == "worker"
        ]
        print(json.dumps(answers), flush=True)
    finally:
        await kit.close()


if __name__ == "__main__":
    sys.path.insert(0, str(Path(roomkit.__file__).resolve().parents[2]))
    asyncio.run(serve(sys.argv[1]))
