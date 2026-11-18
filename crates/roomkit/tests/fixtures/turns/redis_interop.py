"""Run a real Redis turn between independent owner and executor processes.

REDIS_URL must name a test Redis. This interpreter belongs to the reference
Python checkout. The placement checks run with either language in either role.
The tool-history checks replace the Python worker between two turns of the same
room. Without a Rust peer executable, the Python controls run on their own.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from uuid import uuid4

from redis_process import namespace, output, process, serving

PYTHON_PEER = Path(__file__).with_name("redis_peer.py")


async def check_turn(owner: list[str], executor: list[str]) -> None:
    executor_id = "worker-" + uuid4().hex[:12]
    async with namespace() as env, serving([*executor, "executor"], env, executor_id):
        async with process([*owner, "owner"], env) as room:
            actual = json.loads(await output(room))
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


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rust-peer", type=Path)
    selector = parser.add_mutually_exclusive_group()
    selector.add_argument(
        "--pair",
        choices=("python-python", "rust-python", "python-rust", "rust-rust"),
        help="select one owner-executor pair for a targeted run",
    )
    selector.add_argument(
        "--history-only",
        action="store_true",
        help="run the two-turn replay on a new Python executor",
    )
    selector.add_argument(
        "--liveness-only",
        action="store_true",
        help="run the three Redis process-liveness cases",
    )
    args = parser.parse_args()
    if args.liveness_only and not args.rust_peer:
        parser.error("--liveness-only requires --rust-peer")
    peers = {"python": [sys.executable, str(PYTHON_PEER)]}
    if args.rust_peer:
        peers["rust"] = [str(args.rust_peer.resolve(strict=True))]
    if args.pair and "rust" in args.pair and "rust" not in peers:
        parser.error("a Rust pair requires --rust-peer")
    if not args.history_only and not args.liveness_only:
        for owner, owner_command in peers.items():
            for executor, executor_command in peers.items():
                if args.pair and args.pair != f"{owner}-{executor}":
                    continue
                await check_turn(owner_command, executor_command)
                print(f"passed: {owner} owner, {executor} executor", flush=True)
    if not args.pair and not args.liveness_only:
        from redis_history import check_history

        for owner, owner_command in peers.items():
            await check_history(owner_command)
            print(
                f"passed: {owner} owner, tool result replayed by a new Python executor",
                flush=True,
            )
    if args.rust_peer and not args.pair and not args.history_only:
        from redis_liveness import check_liveness

        for case in ("stall", "dead", "absent"):
            await check_liveness(args.rust_peer.resolve(strict=True), case)
            print(f"passed: Rust owner with Python worker ({case})", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
