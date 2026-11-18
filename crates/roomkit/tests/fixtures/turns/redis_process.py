"""Process and Redis namespace lifetimes for the real text-turn peers."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from uuid import uuid4

import redis.asyncio as redis

DEADLINE = 30


@asynccontextmanager
async def namespace() -> AsyncIterator[dict[str, str]]:
    prefix = "roomkit:test:interop:" + uuid4().hex
    try:
        yield {**os.environ, "ROOMKIT_INTEROP_PREFIX": prefix}
    finally:
        async with redis.from_url(os.environ["REDIS_URL"]) as client:
            keys = [key async for key in client.scan_iter(f"{prefix}:*")]
            if keys:
                await client.delete(*keys)


@asynccontextmanager
async def process(
    command: list[str], env: dict[str, str]
) -> AsyncIterator[asyncio.subprocess.Process]:
    child = await asyncio.create_subprocess_exec(
        *command, env=env, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE
    )
    try:
        yield child
    finally:
        if child.returncode is None:
            with suppress(ProcessLookupError):
                child.kill()
        await child.wait()


async def line(child: asyncio.subprocess.Process, expected: bytes) -> None:
    assert child.stdout is not None
    got = await asyncio.wait_for(child.stdout.readline(), DEADLINE)
    if got.rstrip() != expected:
        raise AssertionError(f"expected {expected!r}, got {got!r}")


async def signal(child: asyncio.subprocess.Process, word: bytes) -> None:
    assert child.stdin is not None
    child.stdin.write(word + b"\n")
    await child.stdin.drain()


@asynccontextmanager
async def serving(
    command: list[str], env: dict[str, str], executor_id: str
) -> AsyncIterator[None]:
    async with process(
        command, {**env, "ROOMKIT_INTEROP_EXECUTOR_ID": executor_id}
    ) as worker:
        await line(worker, b"ready")
        yield
        await signal(worker, b"stop")
        if await asyncio.wait_for(worker.wait(), DEADLINE) != 0:
            raise AssertionError(f"executor exited with status {worker.returncode}")


async def output(child: asyncio.subprocess.Process) -> bytes:
    raw, _ = await asyncio.wait_for(child.communicate(), DEADLINE)
    if child.returncode:
        raise AssertionError(f"owner exited with status {child.returncode}")
    return raw
