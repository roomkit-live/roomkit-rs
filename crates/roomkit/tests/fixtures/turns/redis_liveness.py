"""A slow executor, a dead process, and a live process that dropped a turn (§10.6.8)."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

import redis.asyncio as redis
import roomkit
from redis_process import line, namespace, output, process, signal
from roomkit import Agent, TurnExecutor
from roomkit.turns._redis_presence import Keys
from roomkit.turns.redis import RedisTurnBackend

PEER = Path(__file__).with_name("redis_peer.py")


async def beating(env: dict[str, str], executor_id: str, *, running: bool) -> None:
    """See the same process refresh its Redis beat with the same turn membership."""
    keys = Keys(env["ROOMKIT_INTEROP_PREFIX"])
    previous: tuple[float, set[bytes]] | None = None
    async with redis.from_url(env["REDIS_URL"]) as client, asyncio.timeout(5):
        while True:
            for raw, score in await client.zrange(keys.alive, 0, -1, withscores=True):
                node, _, name = raw.decode().rpartition(":")
                if name != executor_id:
                    continue
                turns = await client.smembers(keys.running(node))
                if len(turns) != int(running):
                    continue
                if previous and score > previous[0] and turns == previous[1]:
                    return
                previous = score, turns
            await asyncio.sleep(0.025)


async def check_liveness(rust: Path, case: str) -> None:
    executor_id = "worker-" + uuid4().hex[:12]
    peer = (
        [sys.executable, str(Path(__file__).resolve()), "refuse-output"]
        if case == "absent"
        else [sys.executable, str(PEER), "executor"]
    )
    prompt = {"stall": "Please stall.", "dead": "Please hold.", "absent": "Hi."}[case]
    loop = asyncio.get_running_loop()
    async with (
        namespace() as env,
        process(peer, {**env, "ROOMKIT_INTEROP_EXECUTOR_ID": executor_id}) as worker,
    ):
        await line(worker, b"ready")
        started = loop.time()
        async with process(
            [str(rust), "liveness-owner"], {**env, "ROOMKIT_INTEROP_PROMPT": prompt}
        ) as owner:
            if case == "absent":
                await line(worker, b"output refused")
                started = loop.time()
                await beating(env, executor_id, running=False)
            else:
                await line(worker, b"streaming")
                if case == "dead":
                    worker.kill()
                    await worker.wait()
                    started = loop.time()
                else:
                    await beating(env, executor_id, running=True)
            observed = json.loads(await output(owner))
            elapsed = loop.time() - started
        if case == "stall":
            expected = {
                "answers": [f"Hello from {executor_id}."],
                "end": "completed",
                "error": None,
            }
            assert elapsed >= 3 and observed == expected, (case, elapsed, observed)
        else:
            error = observed["error"] or {}
            assert (
                error.get("error_type") == "TurnLinkError"
                and error.get("attributes", {}).get("reason") == "executor_lost"
            ), (case, observed)
            assert elapsed < 6, (case, elapsed)
        if case != "dead":
            assert worker.returncode is None
            await signal(worker, b"stop")
            assert await asyncio.wait_for(worker.wait(), 10) == 0


async def absent_worker() -> None:
    from tests.redis_turn_executor import TIMEOUTS, _Scripted

    backend = RedisTurnBackend(
        os.environ["REDIS_URL"],
        key_prefix=os.environ["ROOMKIT_INTEROP_PREFIX"],
        timeouts=TIMEOUTS,
    )
    post = backend._outbox.post
    refused = False

    def refusing(node: str, envelope: Any, failed: Any = None, **kwargs: Any) -> None:
        nonlocal refused
        if (
            envelope.kind == "frame"
            and not refused
            and json.loads(envelope.body)["message"]["type"] == "output"
        ):
            refused = True
            failed(ConnectionError("the test peer refused this output"))
            print("output refused", flush=True)
            return
        post(node, envelope, failed, **kwargs)

    backend._outbox.post = refusing
    name = os.environ["ROOMKIT_INTEROP_EXECUTOR_ID"]
    worker = TurnExecutor(
        lambda channel: Agent(channel, provider=_Scripted(name)),
        hosts={"worker"},
        executor_id=name,
        timeouts=TIMEOUTS,
    )
    try:
        await backend.serve(worker)
        print("ready", flush=True)
        await asyncio.to_thread(sys.stdin.readline)
    finally:
        await backend.close()
        await worker.close()


if __name__ == "__main__":
    if sys.argv[1:] != ["refuse-output"]:
        raise SystemExit("run redis_interop.py --liveness-only --rust-peer BINARY")
    sys.path.insert(0, str(Path(roomkit.__file__).resolve().parents[2]))
    asyncio.run(absent_worker())
