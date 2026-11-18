"""Run a real Redis turn between independent owner and executor processes.

REDIS_URL must name a test Redis. This interpreter belongs to the reference
Python checkout. Without a Rust peer executable, only Python's control runs.
Each peer accepts an owner or executor argument and shares one fresh prefix.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

import redis.asyncio as redis

PYTHON_PEER = Path(__file__).with_name("redis_peer.py")
DEADLINE = 30


async def stop(process: asyncio.subprocess.Process) -> None:
    if process.returncode is None:
        with suppress(ProcessLookupError):
            process.kill()
    await process.wait()


async def check_turn(owner: list[str], executor: list[str]) -> None:
    url = os.environ["REDIS_URL"]
    prefix = "roomkit:test:interop:" + uuid4().hex
    env = {**os.environ, "ROOMKIT_INTEROP_PREFIX": prefix}
    executor_id = "worker-" + uuid4().hex[:12]
    worker = await asyncio.create_subprocess_exec(
        *executor,
        "executor",
        env={**env, "ROOMKIT_INTEROP_EXECUTOR_ID": executor_id},
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
    )
    try:
        assert worker.stdout is not None and worker.stdin is not None
        ready = await asyncio.wait_for(worker.stdout.readline(), DEADLINE)
        if ready.rstrip() != b"ready":
            raise AssertionError(f"executor did not become ready: {ready!r}")
        room = await asyncio.create_subprocess_exec(
            *owner,
            "owner",
            env=env,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
        )
        try:
            output, _ = await asyncio.wait_for(room.communicate(), DEADLINE)
            if room.returncode != 0:
                raise AssertionError(f"owner exited with status {room.returncode}")
        finally:
            await stop(room)
        actual = json.loads(output)
        expected = {
            "input": ["tx", "Hi."],
            "answer": ["worker", f"Hello from {executor_id}."],
            "responds_to_input": True,
            "executor_id": executor_id,
            "loop_end_reason": "completed",
        }
        if actual != expected:
            raise AssertionError(
                f"owner observations differ: {actual!r} != {expected!r}"
            )
        worker.stdin.write(b"stop\n")
        await worker.stdin.drain()
        await asyncio.wait_for(worker.wait(), DEADLINE)
        if worker.returncode != 0:
            raise AssertionError(f"executor exited with status {worker.returncode}")
    finally:
        await stop(worker)
        async with redis.from_url(url) as client:
            keys = [key async for key in client.scan_iter(f"{prefix}:*")]
            if keys:
                await client.delete(*keys)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rust-peer", type=Path)
    parser.add_argument(
        "--pair",
        choices=("python-python", "rust-python", "python-rust", "rust-rust"),
        help="select one owner-executor pair for a targeted run",
    )
    args = parser.parse_args()
    peers = {"python": [sys.executable, str(PYTHON_PEER)]}
    if args.rust_peer:
        peers["rust"] = [str(args.rust_peer.resolve(strict=True))]
    if args.pair and "rust" in args.pair and "rust" not in peers:
        parser.error("a Rust pair requires --rust-peer")
    for owner, owner_command in peers.items():
        for executor, executor_command in peers.items():
            if args.pair and args.pair != f"{owner}-{executor}":
                continue
            await check_turn(owner_command, executor_command)
            print(f"passed: {owner} owner, {executor} executor", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
