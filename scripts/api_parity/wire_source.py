"""Shapes of the JSON records exchanged by Python turn links, read from the pinned source."""
from __future__ import annotations

import ast

WIRE = "roomkit.turns.wire."
CODEC = "roomkit.turns._codec."
EXTRA_MODULES = {
    "codec": "turns/_codec", "sender": "turns/_frame_sender", "worker": "turns/_executor_turn",
    "markers": "models/streaming", "intent": "providers/ai/tool_intent",
}


def fields(cls: ast.ClassDef) -> dict[str, ast.expr]:
    return {
        node.target.id: node.annotation for node in cls.body if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name) and not node.target.id.startswith("_")
    }


def strings(node: ast.expr, generic: str) -> list[str]:
    if not isinstance(node, ast.Subscript) or ast.unparse(node.value) != generic:
        raise ValueError(f"expected {generic} in {ast.unparse(node)}")
    values = node.slice.elts if isinstance(node.slice, ast.Tuple) else [node.slice]
    return [ast.literal_eval(value) for value in values]


def member_names(node: ast.expr) -> list[str]:
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        return member_names(node.left) + member_names(node.right)
    raise ValueError(f"unsupported wire union member {ast.unparse(node)}")


def union(annotation: ast.expr, classes: dict) -> dict:
    if not isinstance(annotation, ast.Subscript) or ast.unparse(annotation.value) != "Annotated":
        raise ValueError(f"expected a tagged union in {ast.unparse(annotation)}")
    members, hint = annotation.slice.elts
    if not isinstance(hint, ast.Call) or ast.unparse(hint.func) != "Field":
        raise ValueError(f"missing wire discriminator in {ast.unparse(annotation)}")
    discriminator = next(ast.literal_eval(k.value) for k in hint.keywords if k.arg == "discriminator")
    variants = {}
    for name in member_names(members):
        record = fields(classes[name])
        tag, = strings(record.pop(discriminator), "Literal")
        if tag in variants:
            raise ValueError(f"duplicate {discriminator} {tag}")
        variants[tag] = sorted(record)
    return {"tag": discriminator, "variants": variants}


def dict_keys(node: ast.AST) -> set[str]:
    return {
        key.value for child in ast.walk(node) if isinstance(child, ast.Dict)
        for key in child.keys if isinstance(key, ast.Constant) and isinstance(key.value, str)
    }


def assigned_keys(node: ast.AST) -> set[str]:
    return {
        child.slice.value for child in ast.walk(node) if isinstance(child, ast.Subscript)
        and isinstance(child.ctx, ast.Store) and isinstance(child.slice, ast.Constant)
        and isinstance(child.slice.value, str)
    }


def functions(tree: ast.AST) -> dict:
    return {node.name: node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}


def model_shapes(tree: ast.Module) -> dict:
    classes = {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}
    aliases = {node.targets[0].id: node.value for node in tree.body if isinstance(node, ast.Assign)
               and isinstance(node.targets[0], ast.Name)}
    forms = {WIRE + name: union(aliases[name], classes) for name in ("Frame", "Control", "OwnerMessage")}
    forms[WIRE + "RoomPublication.frame"] = union(fields(classes["RoomPublication"])["frame"], classes)
    for name in ("FrameEnvelope", "ControlEnvelope"):
        members = fields(classes[name])
        forms[WIRE + name] = {"fields": sorted(members), "message": ast.unparse(members["message"])}
    for name in ("ToolResult", "InputAnswer"):
        forms[WIRE + name + ".outcome"] = {"values": strings(fields(classes[name])["outcome"], "Literal")}
    return forms


def codec_shapes(trees: dict) -> dict:
    codec = functions(trees["codec"])
    markers = {node.name: sorted(fields(node)) for node in trees["markers"].body if isinstance(node, ast.ClassDef)}
    intent = functions(trees["intent"])["with_intent_field"]
    optional = dict_keys(intent) | assigned_keys(intent)
    end = dict_keys(codec["encode_end"]) | optional
    kinds = next(node.value for node in trees["codec"].body if isinstance(node, ast.Assign)
                 and ast.unparse(node.targets[0]) == "_ITEM_KINDS")
    variants = {}
    for kind in kinds.elts:
        tag, marker, encoder, _decoder = kind.args
        name = ast.literal_eval(tag)
        if marker.id == "str":
            sender = functions(trees["sender"])["_send_text"]
            names = (dict_keys(sender) | assigned_keys(sender)) - {"kind"}
        elif isinstance(encoder, ast.Name) and encoder.id in codec:
            names = dict_keys(codec[encoder.id]) | optional
        else:
            names = set(markers[marker.id]) | dict_keys(encoder)
        variants[name] = sorted(names)
    # The executor itself sends the transport-only stream terminator.
    for call in ast.walk(trees["worker"]):
        if not isinstance(call, ast.Call) or ast.unparse(call.func) != "Item":
            continue
        for arg in call.keywords:
            if arg.arg == "item" and isinstance(arg.value, ast.Dict):
                record = ast.literal_eval(arg.value)
                terminator = record.pop("kind")
                variants[terminator] = sorted(record)
    tag, = dict_keys(codec["encode_item"])
    return {
        CODEC + "encode_item": {"tag": tag, "variants": variants},
        CODEC + "encode_end": {"fields": sorted(end)},
        CODEC + "encode_result": {"keys": sorted(dict_keys(codec["encode_result"]))},
        "roomkit.models.streaming.LoopEndMarker": {"fields": markers["LoopEndMarker"]},
    }


def wire_shapes(sources: dict[str, str]) -> dict:
    trees = {name: ast.parse(source) for name, source in sources.items()}
    forms = model_shapes(trees["wire"]) if any(
        isinstance(node, ast.ClassDef) and node.name == "RoomPublication" for node in trees["wire"].body
    ) else {}
    if "codec" in trees and trees["codec"].body:
        forms.update(codec_shapes(trees))
    return forms
