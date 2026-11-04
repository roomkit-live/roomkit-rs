"""Read a schema's rows with the Python implementation's PostgresStore and
print what it reads, as JSON: tests/cross_read.rs compares what Python reads of
the rows roomkit-postgres wrote with what it reads of the rows Python wrote.

    <roomkit>/.venv/bin/python -I read_rows.py DSN SCHEMA
"""

from __future__ import annotations

import asyncio
import json
import sys

import asyncpg
from roomkit.models.store_filter import EventFilter
from roomkit.store.postgres import PostgresStore


async def main(dsn: str, schema: str) -> None:
    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=2,
        init=PostgresStore._init_connection,
        server_settings={"search_path": schema},
    )
    store = PostgresStore(pool=pool)
    rooms = await store.list_rooms(limit=1000)
    read = {"rooms": [room.model_dump(mode="json") for room in rooms]}
    for name, reader in (
        ("bindings", store.list_bindings),
        ("participants", store.list_participants),
        ("tasks", store.list_tasks),
        ("observations", store.list_observations),
    ):
        read[name] = {
            room.id: sorted(
                (record.model_dump(mode="json") for record in await reader(room.id)),
                key=lambda record: json.dumps(record, sort_keys=True),
            )
            for room in rooms
        }
    read["events"] = {
        room.id: [
            event.model_dump(mode="json")
            for event in await store.list_events(
                room.id, limit=1000, event_filter=EventFilter(include_blocked=True)
            )
        ]
        for room in rooms
    }
    identities = await pool.fetch("SELECT id FROM identities ORDER BY id")
    read["identities"] = [
        (await store.get_identity(row["id"])).model_dump(mode="json") for row in identities
    ]
    await pool.close()
    print(json.dumps(read, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2]))
