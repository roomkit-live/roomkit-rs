"""Instance APIs reachable through the public executor's wiring, at the pinned Python commit."""
from __future__ import annotations

import ast

PORTS = {
    "_wiring": "TurnWiring",
    "_wiring_env": "WiringEnv",
    "_outlet": "TurnRealtime",
    "_tool_hooks": "ToolHooks",
}
METHODS = (ast.FunctionDef, ast.AsyncFunctionDef)
EMITTER = "roomkit.turns._outlet.FrameworkEmitter"


def method(member: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    """Use the same names, binding and default tokens as the ordinary signature snapshot."""
    if any(isinstance(decorator, ast.Name) and decorator.id == "property" for decorator in member.decorator_list):
        return ["property"]
    args = member.args
    if args.vararg or args.kwarg or args.posonlyargs:
        raise ValueError(f"{member.name}: unsupported variadic or positional-only wiring signature")

    def argument(arg: ast.arg, default: ast.expr | None) -> str:
        return arg.arg if default is None else f"{arg.arg}={ast.unparse(default)}"

    defaults = [None] * (len(args.args) - len(args.defaults)) + list(args.defaults)
    signature = ["async def" if isinstance(member, ast.AsyncFunctionDef) else "def"]
    signature.extend(argument(arg, default) for arg, default in zip(args.args, defaults, strict=True) if arg.arg != "self")
    if args.kwonlyargs:
        signature.append("*")
        signature.extend(argument(arg, val) for arg, val in zip(args.kwonlyargs, args.kw_defaults, strict=True))
    return signature


def emitter_signature(outlet: ast.Module) -> list[str]:
    """The alias is variadic. Its caller supplies a named event and keyword fields."""
    assignments = (node for node in outlet.body if isinstance(node, ast.Assign))
    alias = next(node for node in assignments if any(
        isinstance(target, ast.Name) and target.id == "FrameworkEmitter" for target in node.targets
    ))
    if ast.unparse(alias.value) != "Callable[..., Awaitable[None]]":
        raise ValueError("the wiring emitter is no longer an asynchronous notification callback")
    emit = next(node for node in outlet.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "emit_framework_event")
    signature = method(emit)
    if signature[1:2] != ["fallback"]:
        raise ValueError("the wiring outlet no longer receives an explicit fallback")
    return [signature[0], *signature[2:]]


def class_named(tree: ast.Module, name: str) -> ast.ClassDef:
    return next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == name)


def instance_methods(cls: ast.ClassDef) -> dict[str, list[str]]:
    # The package exports an instance, whose implementation is in a private
    # module. Constructing this class by a private import is outside this API.
    return {node.name: method(node) for node in cls.body if isinstance(node, METHODS) and not node.name.startswith("_")}


def wiring_records(sources: dict[str, str]) -> dict:
    trees = {module: ast.parse(sources[module]) for module in (*PORTS, "realtime_base")}
    records = {EMITTER: {"__call__": emitter_signature(trees["_outlet"])}}
    for module, name in PORTS.items():
        path = f"roomkit.turns.{module}.{name}"
        cls = class_named(trees[module], name)
        members = instance_methods(cls)
        if module == "_outlet":
            bases = [ast.unparse(base) for base in cls.bases]
            if bases != ["RealtimeBackend"]:
                raise ValueError(f"{path}: expected the RealtimeBackend base, got {bases}")
            inherited = instance_methods(class_named(trees["realtime_base"], "RealtimeBackend"))
            members = inherited | members
        if module == "_wiring":
            ctor = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == "__init__")
            for stmt in ctor.body:
                if isinstance(stmt, ast.Assign):
                    for target in stmt.targets:
                        if isinstance(target, ast.Attribute) and ast.unparse(target.value) == "self" and not target.attr.startswith("_"):
                            members[target.attr] = ["property"]
        records[path] = members
    return records


def emitter_matches(alias: dict, expected: list[str], items: dict) -> bool:
    """The host supplies a callback on demand; the outlet must carry all its arguments."""
    env = items.get("roomkit::turns::WiringEnv", {})
    getter = next((m for m in env.get("provided", []) if m["name"] == "emitter"), {})
    options = items.get("roomkit::turns::EmitOptions", {})
    fields = {field["name"]: field["type"] for field in options.get("fields", [])}
    arguments = ["async def", "event_type", "*", *(name + "=None" for name in fields)]
    decl = "pub type FrameworkEmitter = Arc<dyn Fn(String, EmitOptions) -> TurnFuture<'static, Result<(), RoomKitError>> + Send + Sync>;"
    return (
        alias.get("decl") == decl and getter.get("returns") == "Option<FrameworkEmitter>" and
        getter.get("receiver") == "&self" and getter.get("params") == [] and arguments == expected and
        fields == {"room_id": "Option<RoomId>", "channel_id": "Option<ChannelId>",
                   "event_id": "Option<EventId>", "data": "Option<Metadata>"}
    )
