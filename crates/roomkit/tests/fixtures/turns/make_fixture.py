"""Write the turn backend's cross-read fixture: every message the Python
implementation's in-memory turn backend carries for one turn.

The turn is case 1 of the step's probe (step 4, milestone 7, `turns_probe`):
a streamed answer whose first round writes a text and calls a tool, the
tool's handler sending an event into the room through its turn's host
(`current_turn_host().send_event`), and whose second round answers. Each
line of the output is one message, as the backend serialized it:

    {"direction": "request", "message": <the TurnRequest>}
    {"direction": "frame", "message": <a FrameEnvelope>}
    {"direction": "control", "message": <a ControlEnvelope>}

roomkit-rs parses every line and writes it back with nothing lost
(src/turns/tests/cross_read.rs). `make turns-fixture` runs this script
with a roomkit checkout's interpreter and records which roomkit commit
wrote the file:

    <roomkit>/.venv/bin/python -I make_fixture.py OUT.jsonl

The backend's timeouts are its defaults, so no heartbeat crosses during the
turn. The in-memory backend hands a request's events over as they are; the
request is written here whole, as a distributed backend carries it
(`request_json`). The fixture reads the backend's private pipe to see each
message: Python has no public tap.
"""

from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from roomkit import (
    AIChannel,
    Channel,
    ChannelBinding,
    ChannelCategory,
    ChannelOutput,
    ChannelType,
    EventSource,
    InboundMessage,
    RoomContext,
    RoomEvent,
    RoomKit,
    TextContent,
)
from roomkit.providers.ai import (
    AIContext,
    AIProvider,
    AIResponse,
    AITool,
    StreamDone,
    StreamEvent,
    StreamTextDelta,
    StreamToolCall,
)
from roomkit.turns import InMemoryTurnBackend, TurnRequest, current_turn_host
from roomkit.turns import memory as memory_module
from roomkit.turns._codec import request_json

LINES: list[dict[str, Any]] = []

_put = memory_module._Pipe.put


def _logged_put(self: Any, message: Any) -> int:
    """Each message a pipe carries, as its envelope serializes it."""
    seq = _put(self, message)
    direction = "frame" if self._envelope is memory_module.FrameEnvelope else "control"
    envelope = self._envelope.model_validate(
        {"turn_id": self._turn_id, "seq": seq, "message": message}
    )
    LINES.append({"direction": direction, "message": json.loads(envelope.model_dump_json())})
    return seq


memory_module._Pipe.put = _logged_put


class RecordedBackend(InMemoryTurnBackend):
    """The in-memory backend, the requests it carries written whole."""

    async def submit(self, request: TurnRequest) -> Any:
        LINES.append({"direction": "request", "message": json.loads(request_json(request))})
        return await super().submit(request)


class ScriptedAI(AIProvider):
    """Round 1: a text delta, then a call to ``lookup``; round 2: the answer."""

    def __init__(self) -> None:
        self.rounds = 0

    @property
    def model_name(self) -> str:
        return "fixture-ai"

    @property
    def supports_streaming(self) -> bool:
        return True

    @property
    def supports_structured_streaming(self) -> bool:
        return True

    async def generate(self, context: AIContext) -> AIResponse:  # pragma: no cover
        return AIResponse(content="unused")

    async def generate_structured_stream(self, context: AIContext) -> AsyncIterator[StreamEvent]:
        self.rounds += 1
        if self.rounds == 1:
            yield StreamTextDelta(text="Let me check.")
            yield StreamToolCall(id="call-1", name="lookup", arguments={"q": "x"})
            yield StreamDone(finish_reason="tool_calls", usage={"input_tokens": 10})
            return
        yield StreamTextDelta(text="It is 42.")
        yield StreamDone(finish_reason="stop", usage={"input_tokens": 20})


class Sms(Channel):
    """A transport that delivers nowhere."""

    channel_type = ChannelType.SMS

    async def handle_inbound(self, message: InboundMessage, context: RoomContext) -> RoomEvent:
        return RoomEvent(
            room_id=context.room.id,
            source=EventSource(channel_id=self.channel_id, channel_type=self.channel_type),
            content=message.content,
        )

    async def deliver(
        self, event: RoomEvent, binding: ChannelBinding, context: RoomContext
    ) -> ChannelOutput:
        return ChannelOutput.empty()


LOOKUP = AITool(
    name="lookup",
    description="Look it up.",
    parameters={"type": "object", "properties": {"q": {"type": "string"}}},
)


async def lookup(name: str, arguments: dict[str, Any]) -> str:
    """The tool: a note into the room through its turn, then the answer."""
    host = current_turn_host()
    assert host is not None
    await host.send_event(
        room_id="r1", channel_id="ai", content=TextContent(body="(looking it up)")
    )
    return "42"


async def write(out: Path) -> None:
    kit = RoomKit(turn_backend=RecordedBackend())
    kit.register_channel(Sms("sms"))
    kit.register_channel(
        AIChannel("ai", provider=ScriptedAI(), tools=[LOOKUP], tool_handler=lookup)
    )
    await kit.create_room(room_id="r1")
    await kit.attach_channel("r1", "sms")
    await kit.attach_channel("r1", "ai", category=ChannelCategory.INTELLIGENCE)
    message = InboundMessage(
        channel_id="sms",
        sender_id="+15550000001",
        content=TextContent(body="What is it?"),
        delivery_key="k1",
    )
    try:
        result = await kit.process_inbound(message, room_id="r1")
        assert result.error is None, result.error
    finally:
        await kit.close()
    out.write_text("".join(json.dumps(line, sort_keys=True) + "\n" for line in LINES))


if __name__ == "__main__":
    asyncio.run(write(Path(sys.argv[1])))
