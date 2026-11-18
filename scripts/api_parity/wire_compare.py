"""Compare rustdoc's public wire representations with Python's pinned JSON shapes."""
from __future__ import annotations

import re

from api_parity.mapping import snake_name


def field_names(item: dict) -> set[str]:
    return {field["name"] for field in item.get("fields", [])}


def variant_fields(variant: dict, surface, module: str, expected: set[str]) -> set[str]:
    own = field_names(variant)
    declaration = variant["decl"]
    wrapped = re.fullmatch(r"\w+\((\w+)\)", declaration)
    # The Frame variant for a flattened record has a single boxed member.
    # The Serde round-trip test independently checks this flattening on the wire.
    if not wrapped and len(own) == 1 and own != expected:
        wrapped = re.fullmatch(r"Box<(\w+)>", variant["fields"][0]["type"])
    if wrapped:
        record = surface.items.get(f"{module}::{wrapped[1]}")
        if record:
            return field_names(record)
    return own


def difference(expected, actual, label: str) -> list[str]:
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    return ([f"missing {label}: {', '.join(missing)}"] if missing else []) + (
        [f"extra {label}: {', '.join(extra)}"] if extra else [])


def check_wire(item: dict, shape: dict, surface) -> list[str]:
    """Wire records have data fields, variants and serializers. A new public
    method or associated constant needs its own Python counterpart."""
    extra = [member["name"] for key in ("methods", "constants") for member in item.get(key, [])]
    issues = difference([], extra, "public wire members")
    if "fields" in shape and item.get("non_exhaustive"):
        issues.append("an external caller cannot build this wire record by struct update")
    return issues + check_shape(item, shape, surface)


def check_shape(item: dict, shape: dict, surface) -> list[str]:
    path, kind = item["path"], item["kind"]
    module = path.rpartition("::")[0]
    if "message" in shape:
        record = item
        if kind == "type":
            if not item["decl"].endswith(f"= Envelope<{shape['message']}>;"):
                return [f"alias must carry {shape['message']} in its envelope"]
            record = surface.items.get(f"{module}::Envelope", {})
        if record.get("kind") != "struct":
            return ["missing generic envelope struct"]
        return difference(shape["fields"], field_names(record), "envelope fields")
    if "fields" in shape:
        if kind != "struct":
            return ["expected a wire record"]
        return difference(shape["fields"], field_names(item), "record fields")
    expected = shape.get("variants", {})
    subset = kind == "struct" and "variants" in shape
    if subset:
        if "TryFrom<Frame>" not in item.get("impls", []) or "Serialize" not in item.get("impls", []):
            return ["expected a checked, serializable subset of Frame"]
        item = surface.items.get(f"{module}::Frame", {})
    if item.get("kind") != "enum":
        return ["expected a wire enum"]
    variants = {snake_name(v["name"]): v for v in item["variants"]}
    if "values" in shape:
        expected = dict.fromkeys(shape["values"], ())
    elif "keys" in shape:
        expected = {name: [name] for name in shape["keys"]}
    actual = set(variants) & set(expected) if subset else set(variants)
    errors = difference(expected, actual, "wire variants")
    for name in sorted(set(expected) & actual):
        fields = variant_fields(variants[name], surface, module, set(expected[name]))
        errors += difference(expected[name], fields, f"{name} fields")
    return errors
