"""Write the cross-read fixture: the rows the Python implementation's
PostgresStore writes, holding every kind of record it stores, as JSON.

roomkit-postgres loads them, reads them back and rewrites them with nothing
lost (tests/cross_read.rs). The records are those of roomkit-sqlite's generator,
through the kit and through the store. `make postgres-fixture` runs this script
with a roomkit checkout's interpreter, against POSTGRES_DSN, and records which
roomkit commit wrote the rows:

    POSTGRES_DSN=... <roomkit>/.venv/bin/python -I make_fixture.py OUT.json
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
import uuid
from pathlib import Path

import asyncpg
from roomkit import RoomKit, WebSocketChannel
from roomkit.store.postgres import PostgresStore

SQLITE_GENERATOR = (
    Path(__file__).parents[3] / "roomkit-sqlite" / "tests" / "fixtures" / "make_fixture.py"
)

TABLES = {
    "rooms": "id",
    "events": "id",
    "event_sequences": "room_id",
    "bindings": "room_id, channel_id",
    "participants": "room_id, id",
    "identities": "id",
    "identity_addresses": "channel_type, address, organization_id",
    "tasks": "id",
    "observations": "id",
    "read_markers": "room_id, channel_id",
    "private_state": "room_id, key",
}
"""Each table the records fill, children after parents, by its key."""


def records_module():
    spec = importlib.util.spec_from_file_location("sqlite_fixture", SQLITE_GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def main(out: str) -> None:
    dsn = os.environ["POSTGRES_DSN"]
    schema = f"fixture_{uuid.uuid4().hex}"
    admin = await asyncpg.connect(dsn)
    await admin.execute(f"CREATE SCHEMA {schema}")
    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=6,
        init=PostgresStore._init_connection,
        server_settings={"search_path": schema},
    )
    try:
        store = PostgresStore(pool=pool)
        await store.init()
        kit = RoomKit(store=store)
        kit.register_channel(WebSocketChannel("ws-user"))
        kit.register_channel(WebSocketChannel("ws-gone"))
        records = records_module()
        await records.through_the_kit(kit, store)
        await records.through_the_store(store)
        await kit.close()
        rows = {}
        for table, key in TABLES.items():
            found = await admin.fetch(
                f"SELECT to_jsonb(t)::text AS row FROM {schema}.{table} t ORDER BY {key}"
            )
            rows[table] = [json.loads(row["row"]) for row in found]
    finally:
        await pool.close()
        await admin.execute(f"DROP SCHEMA {schema} CASCADE")
        await admin.close()
    Path(out).write_text(json.dumps(rows, indent=1, sort_keys=True) + "\n")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
