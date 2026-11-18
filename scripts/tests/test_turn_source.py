"""The verified additions to the pinned public-API signature snapshot."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from api_parity.turn_source import extract


class TurnSource(unittest.TestCase):
    def test_only_declared_and_directly_assigned_public_attributes_enter_the_protocol(self) -> None:
        source = """
class TurnBackend:
    timeouts: TurnTimeouts | None = None
    _private: int
    def __init__(self):
        self.node_id = identifier()
        self._link = None
        another.unrelated = 5
    def work(self):
        self.transient = True
class SomeOtherClass:
    external: bool
"""
        result = extract({"base": source})
        self.assertEqual(result["attributes"], {"roomkit.TurnBackend": ["node_id", "timeouts"]})
        self.assertEqual(result["protocol_records"], {})

    def test_required_values_fixed_defaults_and_factories_keep_the_python_signature(self) -> None:
        source = """
class HostResult(BaseModel):
    type: Literal["host_result"] = "host_result"
    call_id: str
    result: Any = None
    changes: dict = Field(default_factory=dict)
"""
        key = "roomkit.turns.wire.HostResult"
        spec = extract({"wire": source})["protocol_records"][key]
        self.assertEqual(spec, {"__init__": ["def", "*", "type='host_result'", "call_id", "result=None", "changes=<factory>"]})
        with self.assertRaisesRegex(ValueError, "unsupported wire default"):
            extract({"wire": source.replace("Field(default_factory=dict)", "build_default()")})
        with self.assertRaisesRegex(ValueError, "public method"):
            extract({"wire": source + "    def apply(self): pass"})
