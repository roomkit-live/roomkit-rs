"""Write the cross-read fixture: the rows the Python implementation's
PostgresStore writes, holding every kind of record it stores, as JSON.

roomkit-postgres loads them, reads them back and rewrites them with nothing
lost (tests/cross_read.rs). The records are those of roomkit-sqlite's generator,
through the kit and through the store, with what only this store's columns hold
and verifications. `make postgres-fixture` runs this script
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
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
from roomkit import RoomKit, WebSocketChannel
from roomkit.models.enums import ChannelType, EventStatus, VerificationStatus
from roomkit.models.event import EventSource, RoomEvent, TextContent
from roomkit.models.verification import Verification, VerificationRequest
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
    "verifications": "identity_id, organization_id",
    "verification_requests": "id",
}
"""Each table the records fill, children after parents, by its key."""


AT = datetime(2026, 10, 22, 14, 0, 0, 250000, tzinfo=UTC)


async def what_only_this_store_holds(store: PostgresStore, room: str) -> None:
    """Every member of an event's source, each in its own column or in
    `source_extra`, a read marker, and verifications."""
    event = await store.commit_event(
        room,
        RoomEvent(
            id="evt-provider",
            room_id=room,
            source=EventSource(
                channel_id="sms-main",
                channel_type=ChannelType.SMS,
                participant_id="+15551234567",
                provider="twilio",
                external_id="ext-9",
                provider_message_id="pm-9",
                raw_payload={"n": 9},
                verification=VerificationStatus.VERIFIED,
            ),
            content=TextContent(body="from a provider"),
            status=EventStatus.DELIVERED,
            created_at=AT,
        ),
        activity=False,
    )
    await store.mark_read(room, "ws-user", event.id)
    await store.put_verification(
        Verification(
            identity_id="id-jean",
            organization_id="org-1",
            status=VerificationStatus.VERIFIED,
            method="sms_otp",
            verified_via="sms-main",
            verified_at=AT,
            expires_at=AT,
        )
    )
    await store.create_verification_request(
        VerificationRequest(
            id="req-1",
            identity_id="id-jean",
            organization_id="org-1",
            room_id=room,
            channel_id="sms-main",
            method="sms_otp",
            secret_hash="hash",
            window_start=AT,
            created_at=AT,
            expires_at=AT,
            max_attempts=3,
        )
    )


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
        await what_only_this_store_holds(store, records.ROOM)
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
