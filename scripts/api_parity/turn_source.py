"""Recover the typed turn attributes and protocol records absent from the signature snapshot.

The ordinary Python snapshot records methods, properties and exported classes.
These definitions are read statically from the very same commit. Protocol
records below occur in public link signatures or their wire payloads; they
are never added to a Python package's list of exports.
"""
from __future__ import annotations

import argparse
import ast
import json
import subprocess
from pathlib import Path

ATTRIBUTE_CLASSES = {
    "base": {"ExecutorLink", "TurnLink", "RoomOwner", "TurnBackend"},
    "redis": {"RedisTurnBackend"},
}
WIRE_RECORDS = {
    "CallContext", "ChannelControlEnvelope", "ErrorRecord", "HostResult",
    "InputRequest", "OwnerMessageEnvelope", "RecordChanges", "WireOutput",
}


def attributes(cls: ast.ClassDef) -> list[str]:
    """Class annotations and instance attributes assigned directly by __init__."""
    names = {
        node.target.id for node in cls.body
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
    }
    for method in cls.body:
        if isinstance(method, ast.FunctionDef) and method.name == "__init__":
            for statement in method.body:
                targets = statement.targets if isinstance(statement, ast.Assign) else (
                    [statement.target] if isinstance(statement, ast.AnnAssign) else []
                )
                names.update(
                    target.attr for target in targets if isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name) and target.value.id == "self"
                )
    return sorted(name for name in names if not name.startswith("_"))


def model_fields(cls: ast.ClassDef) -> list[str]:
    """Constructor fields in Python snapshot notation, from a plain wire BaseModel."""
    if [ast.unparse(base) for base in cls.bases] != ["BaseModel"]:
        raise ValueError(f"{cls.name}: the wire model's bases changed")
    fields = []
    for node in cls.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and not node.name.startswith("_"):
            raise ValueError(f"{cls.name}.{node.name}: public method on a protocol record")
        if not isinstance(node, ast.AnnAssign) or not isinstance(node.target, ast.Name):
            continue
        name = node.target.id
        if name.startswith("_"):
            continue
        if node.value is None:
            fields.append(name)
        elif isinstance(node.value, ast.Constant):
            fields.append(f"{name}={node.value.value!r}")
        elif (isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == "Field" and not node.value.args
              and [arg.arg for arg in node.value.keywords] == ["default_factory"]):
            fields.append(f"{name}=<factory>")
        else:
            raise ValueError(f"{cls.name}.{name}: unsupported wire default {ast.unparse(node.value)}")
    return fields


def extract(sources: dict[str, str]) -> dict:
    members, records = {}, {}
    for module, source in sources.items():
        for cls in ast.parse(source).body:
            if not isinstance(cls, ast.ClassDef):
                continue
            if cls.name in ATTRIBUTE_CLASSES.get(module, ()):
                members[f"roomkit.{cls.name}"] = attributes(cls)
            elif module == "wire" and cls.name in WIRE_RECORDS:
                records[f"roomkit.turns.wire.{cls.name}"] = {"__init__": ["def", "*", *model_fields(cls)]}
    return {"attributes": members, "protocol_records": records}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("checkout", type=Path)
    parser.add_argument("commit")
    args = parser.parse_args()
    sources = {
        module: subprocess.check_output(
            ["git", "-C", str(args.checkout), "show", f"{args.commit}:src/roomkit/turns/{module}.py"],
            text=True, encoding="utf-8",
        )
        for module in (*ATTRIBUTE_CLASSES, "wire")
    }
    print(json.dumps({"commit": args.commit, **extract(sources)}, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
