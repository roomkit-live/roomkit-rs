"""A Python owner receives a Rust worker's host calls and publications across turn end."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from uuid import uuid4

import redis.asyncio as redis
import roomkit
from roomkit.models.framework_event import FrameworkEvent
from roomkit.realtime import EphemeralEvent


async def check_after_turn(rust: Path) -> None:
    checkout = Path(roomkit.__file__).resolve().parents[2]
    sys.path.insert(0, str(checkout))
    from tests.test_turns_redis import _owner, _said

    prefix = "roomkit:test:after-turn:" + uuid4().hex
    worker = await asyncio.create_subprocess_exec(
        str(rust),
        "after-turn-worker",
        env={**os.environ, "ROOMKIT_INTEROP_PREFIX": prefix},
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
    )
    assert worker.stdin is not None and worker.stdout is not None

    async def read() -> str:
        line = await asyncio.wait_for(worker.stdout.readline(), 10)
        assert line, "the Rust executor exited before answering"
        return line.decode().strip()

    try:
        assert await read() == "ready"
        kit = await _owner(prefix, "worker")
        try:
            frameworks: list[str] = []
            ephemerals: list[str] = []

            @kit.on("ai_response")
            async def framework(event: FrameworkEvent) -> None:
                if "phase" in event.data:
                    frameworks.append(event.data["phase"])

            async def ephemeral(event: EphemeralEvent) -> None:
                if "phase" in event.data:
                    ephemerals.append(event.data["phase"])

            await kit.subscribe_room("r", ephemeral)
            result = await kit.process_inbound(_said("first"), room_id="r")
            assert result.error is None, result.error
            worker.stdin.write(b"late\n")
            await worker.stdin.drain()
            event_id = await read()
            rows = await kit.store.list_events("r")
            assert [
                (e.content.body, e.chain_depth) for e in rows if e.id == event_id
            ] == [("after the turn", 0)]
            async with asyncio.timeout(5):
                while len(frameworks) < 2 or len(ephemerals) < 2:
                    await asyncio.sleep(0.01)
            assert frameworks == ephemerals == ["running", "after"]
            result = await kit.process_inbound(_said("next"), room_id="r")
            assert result.error is None, result.error
            worker.stdin.write(b"stale\n")
            await worker.stdin.drain()
            refusal = await read()
            assert "is no longer the room's last" in refusal, refusal
            assert all(
                getattr(row.content, "body", None) != "stale work"
                for row in await kit.store.list_events("r")
            )
        finally:
            await kit.close()
        worker.stdin.write(b"stop\n")
        await worker.stdin.drain()
        assert await asyncio.wait_for(worker.wait(), 10) == 0
    finally:
        if worker.returncode is None:
            worker.kill()
            await worker.wait()
        async with redis.from_url(os.environ["REDIS_URL"]) as client:
            keys = [k async for k in client.scan_iter(match=prefix + ":*")]
            if keys:
                await client.delete(*keys)


if __name__ == "__main__":
    asyncio.run(check_after_turn(Path(sys.argv[1]).resolve(strict=True)))
