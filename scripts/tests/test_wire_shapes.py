"""The wire rules reject stale Rust variants, fields, aliases and Python tags."""
from __future__ import annotations

import ast
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from api_parity.sides import Surface
from api_parity.wire_compare import check_wire
from api_parity.wire_source import union


class WireShapes(unittest.TestCase):
    def test_python_members_are_selected_by_the_discriminator_literal(self):
        source = """
class First:
    marker: Literal["old_tag"] = "old_tag"
    amount: int
class Added:
    marker: Literal["second"] = "second"
    text: str
"""
        classes = {node.name: node for node in ast.parse(source).body}
        def describe(members):
            return union(ast.parse(f'Annotated[{members}, Field(discriminator="marker")]', mode="eval").body, classes)
        self.assertEqual(describe("First"), {"tag": "marker", "variants": {"old_tag": ["amount"]}})
        self.assertEqual(describe("First | Added")["variants"]["second"], ["text"])
        with self.assertRaisesRegex(ValueError, "unsupported wire union member"):
            describe("First | None")
        with self.assertRaisesRegex(ValueError, "duplicate marker"):
            describe("First | First")

    def test_variant_name_and_public_field_drift_cannot_disappear_behind_the_mapping(self):
        item = {"path": "sample::Frame", "kind": "enum",
                "variants": [{"name": "Reported", "decl": "Reported", "fields": [{"name": "old", "type": "String"}]}]}
        surface = Surface({"crates": {"sample": [item]}})
        self.assertEqual(check_wire(item, {"variants": {"reported": ["old"]}}, surface), [])
        issues = check_wire(item, {"variants": {"reported": ["new"], "second": []}}, surface)
        for change in ["missing wire variants: second", "missing reported fields: new", "extra reported fields: old"]:
            self.assertIn(change, issues)

    def test_new_public_operations_and_non_buildable_records_are_reported(self):
        item = {"path": "sample::Record", "kind": "struct", "fields": [{"name": "value"}],
                "methods": [{"name": "install"}], "non_exhaustive": True}
        issues = check_wire(item, {"fields": ["value"]}, Surface({"crates": {}}))
        self.assertIn("extra public wire members: install", issues)
        self.assertIn("an external caller cannot build this wire record by struct update", issues)

    def test_numbered_alias_checks_the_message_and_the_generic_records_fields(self):
        record = {"path": "sample::Envelope", "kind": "struct", "fields": [{"name": n} for n in ("seq", "message")]}
        alias = {"path": "sample::ControlEnvelope", "kind": "type", "decl": "pub type ControlEnvelope = Envelope<Control>;"}
        surface = Surface({"crates": {"sample": [record, alias]}})
        fields = ["message", "seq", "turn_id"]
        self.assertEqual(check_wire(alias, {"message": "Control", "fields": fields}, surface), ["missing envelope fields: turn_id"])
        self.assertEqual(check_wire(alias, {"message": "Frame", "fields": fields}, surface), ["alias must carry Frame in its envelope"])

    def test_literal_and_untagged_result_forms_check_their_exact_values_and_keys(self):
        surface = Surface({"crates": {}})
        item = {"path": "sample::Result", "kind": "enum", "variants": [
            {"name": "Value", "decl": "Value", "fields": [{"name": "content", "type": "Value"}]}]}
        self.assertIn("missing wire variants: parts", check_wire(item, {"keys": ["value", "parts"]}, surface))
        self.assertIn("missing value fields: value", check_wire(item, {"keys": ["value"]}, surface))
        self.assertIn("extra wire variants: value", check_wire(item, {"values": ["served", "failed"]}, surface))
