"""Write the cross-read fixture: a SQLite file the Python implementation's
SQLiteStore writes, holding every kind of record it stores.

roomkit-sqlite reads it back and rewrites it with nothing lost
(tests/cross_read.rs). `make sqlite-fixture` runs this script with a roomkit
checkout's interpreter and records which roomkit commit wrote the file:

    <roomkit>/.venv/bin/python -I make_fixture.py OUT.db

The kit writes what an integrator's calls write (rooms, bindings, messages
of every content type, an edit, a delete, a hard delete, a revoked binding,
the register of authors); the store writes what only providers and agents
write (a full SMS binding, a participant, an identity, tasks, an
observation, records of the framework and of tool calls).
"""

from __future__ import annotations

import asyncio
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path

from roomkit import InboundMessage, RoomKit, SQLiteStore, WebSocketChannel
from roomkit.models.channel import ChannelBinding, ChannelCapabilities, RateLimit, RetryPolicy
from roomkit.models.enums import (
    ChannelCategory,
    ChannelType,
    EventStatus,
    EventType,
    IdentificationStatus,
    ParticipantRole,
    RoomStatus,
    TaskStatus,
)
from roomkit.models.event import (
    AudioContent,
    ChannelData,
    CompositeContent,
    DeleteContent,
    EditContent,
    EventSource,
    LocationContent,
    MediaContent,
    RichContent,
    RoomEvent,
    SystemContent,
    TemplateContent,
    TextContent,
    ToolCallContent,
    VideoContent,
)
from roomkit.models.identity import Identity
from roomkit.models.participant import Participant
from roomkit.models.room import RoomTimers
from roomkit.models.task import Observation, Task

ROOM = "room-full"
AT = datetime(2026, 10, 22, 14, 0, 0, 250000, tzinfo=UTC)
"""An instant with a fraction, which Python writes with six digits."""

CONTENTS = [
    TextContent(body="Hello, my invoice is wrong", language="en"),
    RichContent(
        body="**Bold**",
        plain_text="Bold",
        buttons=[{"id": "b1", "label": "Yes"}],
        cards=[{"title": "Card"}],
        quick_replies=["yes", "no"],
    ),
    MediaContent(
        url="https://example.com/a.pdf",
        mime_type="application/pdf",
        filename="a.pdf",
        size_bytes=1024,
        caption="invoice",
    ),
    LocationContent(latitude=45.5, longitude=-73.6, label="Office", address="1 Main St"),
    AudioContent(url="https://example.com/a.ogg", duration_seconds=3.5, transcript="hi"),
    VideoContent(
        url="https://example.com/v.mp4",
        duration_seconds=10.0,
        thumbnail_url="https://example.com/t.jpg",
    ),
    CompositeContent(
        parts=[
            TextContent(body="part one"),
            CompositeContent(
                parts=[MediaContent(url="https://example.com/i.png", mime_type="image/png")]
            ),
        ]
    ),
    TemplateContent(
        template_id="tpl_1", language="fr", parameters={"name": "Jean"}, body="Bonjour Jean"
    ),
]


def inbound(content, **kwargs) -> InboundMessage:
    return InboundMessage(channel_id="ws-user", sender_id="user-1", content=content, **kwargs)


async def through_the_kit(kit: RoomKit, store: SQLiteStore) -> None:
    await kit.create_room(
        room_id=ROOM,
        metadata={"topic": "Billing", "nested": {"a": [1, 2.5, None, True]}},
        organization_id="org-1",
        timers=RoomTimers(inactive_after_seconds=1800, closed_after_seconds=86400),
    )
    await kit.attach_channel(ROOM, "ws-user", metadata={"persona": "formal"})
    await kit.attach_channel(ROOM, "ws-gone")
    # The detach writes the revocation, roomkit:detached_channels.
    await kit.detach_channel(ROOM, "ws-gone")
    events = []
    for number, content in enumerate(CONTENTS):
        await kit.process_inbound(
            inbound(
                content,
                idempotency_key=f"key-{number}",
                external_id=f"ext-{number}",
                provider_message_id=f"pm-{number}",
                raw_payload={"n": number},
                metadata={"sender_name": "Alice"},
            )
        )
        events.append(
            await store.get_event_by_idempotency_key(ROOM, f"key-{number}", channel_id="ws-user")
        )
    first, media, template = events[0], events[2], events[7]
    await kit.process_inbound(inbound(TextContent(body="a reply"), parent_event_id=first.id))
    await kit.process_inbound(
        inbound(
            EditContent(
                target_event_id=first.id,
                new_content=TextContent(body="Hello, my invoice for March is wrong"),
            ),
            event_type=EventType.EDIT,
        )
    )
    await kit.process_inbound(
        inbound(
            DeleteContent(target_event_id=media.id, reason="oops"), event_type=EventType.DELETE
        )
    )
    await kit.process_inbound(
        inbound(
            TextContent(body="for the agent"),
            addressed_to=["ai"],
            response_visibility="transport",
            visibility="ws-user",
        )
    )
    # A hard delete leaves a gap in the timeline.
    await kit.delete_event(ROOM, template.id)


def binding(channel_id: str, channel_type: ChannelType, **kwargs) -> ChannelBinding:
    return ChannelBinding(
        channel_id=channel_id, room_id=ROOM, channel_type=channel_type, attached_at=AT, **kwargs
    )


async def bindings_and_people(store: SQLiteStore) -> None:
    await store.add_binding(
        binding(
            "sms-main",
            ChannelType.SMS,
            participant_id="+15551234567",
            last_read_index=2,
            capabilities=ChannelCapabilities(max_length=160, supports_media=True),
            rate_limit=RateLimit(max_per_second=1.0),
            retry_policy=RetryPolicy(max_retries=2, retryable_errors=["timeout"]),
            metadata={"phone_number": "+15551234567"},
        )
    )
    await store.add_binding(
        binding("ai", ChannelType.AI, category=ChannelCategory.INTELLIGENCE, visibility="transport")
    )
    # Types the RFC does not list.
    await store.add_binding(binding("discord-main", ChannelType.DISCORD))
    await store.add_binding(binding("buzz-main", ChannelType.BUZZ))
    await store.add_participant(
        Participant(
            id="p-jean",
            room_id=ROOM,
            channel_id="sms-main",
            display_name="Jean Tremblay",
            role=ParticipantRole.OWNER,
            identification=IdentificationStatus.IDENTIFIED,
            identity_id="id-jean",
            candidates=["id-jean", "id-other"],
            connected_via=["sms-main", "ws-user"],
            external_id="crm-1",
            resolved_at=AT,
            resolved_by="auto",
            joined_at=AT,
            metadata={"vip": True},
        )
    )
    await store.create_identity(
        Identity(
            id="id-jean",
            organization_id="org-1",
            display_name="Jean Tremblay",
            email="jean@example.com",
            phone="+15551234567",
            channel_addresses={"sms": ["+15551234567"]},
            external_id="crm-42",
            external_ids={"crm": "42", "erp": "77"},
            metadata={"tier": "gold"},
        )
    )
    await store.link_address("id-jean", "email", "jean@example.com", organization_id="org-1")
    # A name no implementation knows.
    await store.link_address("id-jean", "slack", "U024BE7LH", organization_id="org-1")


async def side_effects(store: SQLiteStore) -> None:
    await store.add_task(
        Task(
            id="task-1",
            room_id=ROOM,
            title="Call back",
            description="About March",
            assigned_to="advisor-2",
            status=TaskStatus.IN_PROGRESS,
            created_at=AT,
            metadata={"priority": 1},
        )
    )
    await store.add_observation(
        Observation(
            id="obs-1",
            room_id=ROOM,
            channel_id="ai",
            content="Customer is upset",
            category="sentiment",
            confidence=0.75,
            created_at=AT,
            metadata={"model": "x"},
        )
    )


def records() -> list[RoomEvent]:
    """Events the pipeline writes through agents and providers."""
    agent = EventSource(channel_id="ai", channel_type=ChannelType.AI, direction="outbound")
    return [
        RoomEvent(
            id="evt-system",
            room_id=ROOM,
            type=EventType.SYSTEM,
            source=EventSource(
                channel_id="system", channel_type=ChannelType.SYSTEM, direction="outbound"
            ),
            content=SystemContent(body="Room paused", code="room_paused", data={"why": "timer"}),
            visibility="internal",
            created_at=AT,
            status=EventStatus.DELIVERED,
        ),
        RoomEvent(
            id="evt-tool-start",
            room_id=ROOM,
            type=EventType.TOOL_CALL_START,
            source=agent,
            content=ToolCallContent(tool_name="lookup", tool_id="call-1", arguments={"q": "march"}),
            chain_depth=1,
            responds_to="evt-system",
            correlation_id="turn-1",
            created_at=AT,
        ),
        RoomEvent(
            id="evt-tool-end",
            room_id=ROOM,
            type=EventType.TOOL_CALL_END,
            source=agent,
            content=ToolCallContent(
                tool_name="lookup",
                tool_id="call-1",
                result={"rows": [1, 2]},
                status="failed",
                duration_ms=12,
                error="boom",
                structured_content={"k": "v"},
                outcome="failed",
                refused_but_ran=True,
            ),
            chain_depth=1,
            responds_to="evt-system",
            created_at=AT,
        ),
        RoomEvent(
            id="evt-blocked",
            room_id=ROOM,
            source=agent,
            content=TextContent(body="blocked text"),
            status=EventStatus.BLOCKED,
            blocked_by="pii_hook",
            created_at=AT,
            channel_data=ChannelData(
                provider="openai", external_id="x1", thread_id="t1", extra={"a": 1}
            ),
            delivery_results={"sms-main": {"status": "failed"}},
        ),
        RoomEvent(
            id="evt-discord",
            room_id=ROOM,
            source=EventSource(
                channel_id="discord-main",
                channel_type=ChannelType.DISCORD,
                participant_id="discord-user",
            ),
            content=TextContent(body="from discord"),
            idempotency_key="discord-1",
            created_at=AT,
            status=EventStatus.DELIVERED,
        ),
    ]


async def through_the_store(store: SQLiteStore) -> None:
    await bindings_and_people(store)
    await side_effects(store)
    for event in records():
        await store.commit_event(ROOM, event, activity=False)
    room = await store.get_room(ROOM)
    await store.create_room(
        room.model_copy(
            update={
                "id": "room-closed",
                "organization_id": None,
                "status": RoomStatus.CLOSED,
                "paused_at": AT,
                "closed_at": AT,
                "metadata": {},
                "event_count": 0,
                "latest_index": None,
                "delivered_index": -1,
            }
        )
    )


def compact(path: str) -> None:
    """The file whole, without its write-ahead log, and as small as it
    goes."""
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("VACUUM")
    conn.close()


async def main(path: str) -> None:
    for suffix in ("", "-wal", "-shm"):
        Path(path + suffix).unlink(missing_ok=True)
    store = SQLiteStore(path)
    kit = RoomKit(store=store)
    kit.register_channel(WebSocketChannel("ws-user"))
    kit.register_channel(WebSocketChannel("ws-gone"))
    await through_the_kit(kit, store)
    await through_the_store(store)
    await kit.close()
    compact(path)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
