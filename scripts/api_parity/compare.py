"""Each Rust item against its Python counterpart.

The rules are written in `scripts/api-parity.py`'s docstring; each is one
method below.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from api_parity.mapping import Entry, Mapping, variant_name
from api_parity.sides import Python, Surface, python_signature

# What a struct a caller builds that keeps `#[non_exhaustive]` is found with.
NON_EXHAUSTIVE = "#[non_exhaustive]: a caller outside the crate cannot build it by struct update"

# The Python name a Rust item standing for a builtin reads as.
BUILTIN = "builtins."

@dataclass(frozen=True)
class Finding:
    side: str  # "rust" or "python": the side `path` is on
    path: str
    detail: str
    rust_item: str
    python_item: str | None


@dataclass
class ItemResult:
    path: str
    kind: str
    python: str | None
    status: str = ""  # matches, differs, options, exception or unmapped
    findings: list[Finding] = field(default_factory=list)


@dataclass
class _RustParam:
    name: str  # as Python names it: an unused parameter's `_` dropped
    type: str
    positional: bool
    path: str  # where a finding on it points


@dataclass
class _Class:
    """One Rust item against one Python class: its members still unmatched."""

    path: str
    python: str
    methods: dict[str, dict]
    fields: dict[str, dict]
    members: dict[str, str]  # a Python enum member: the Rust variant or constant
    owners: dict[str, str] = field(default_factory=dict)  # a method from a trait: the trait's path
    used: set[str] = field(default_factory=set)

    def method_path(self, name: str) -> str:
        return f"{self.owners.get(name, self.path)}::{name}"


class Comparison:
    def __init__(self, surface: Surface, python: Python, mapping: Mapping) -> None:
        self.surface, self.python, self.rules = surface, python, mapping.rules
        self.item_maps: dict[str, Entry] = {}
        self.member_maps: dict[str, dict[tuple[str, str], Entry]] = {}
        for entry in mapping.of("map"):
            item, member, _ = surface.split(entry.rust)
            if member is None:
                self.item_maps[item] = entry
            else:
                owner, python_member, _ = python.split(entry.python)
                self.member_maps.setdefault(item, {})[(owner, python_member or "")] = entry
        self.applied: set[int] = set()
        self.options_used: set[str] = set()
        self.findings: list[Finding] = []

    def run(self) -> dict[str, ItemResult]:
        results = {}
        for path, item in self.surface.items.items():
            python = self.python_of(item)
            before = len(self.findings)
            if python is None:
                self._find("rust", path, "no Python name", path, None)
            else:
                self._item(item, python)
            results[path] = ItemResult(path, item["kind"], python, findings=self.findings[before:])
        return results

    def python_of(self, item: dict) -> str | None:
        """The item's Python name: its `[[map]]` entry's, or the rules':
        the builtin a strong type, a validator's error or a raised
        exception's alias stands for, a `Literal`'s field, or the name by
        its casing."""
        entry = self.item_maps.get(item["path"])
        if entry is not None:
            self.applied.add(id(entry))
            return entry.python
        module, _, name = item["path"].rpartition("::")
        if name in self.rules.builtins:
            return BUILTIN + self.rules.builtins[name]
        if item["kind"] == "enum" and name in self.rules.literals:
            return self.rules.literals[name]
        wanted = name if item["kind"] in ("fn", "constant") else self.rules.python_type_name(name)
        return self.python.find(self.rules.packages_for(module), wanted)

    def own_members(self, path: str) -> list[Finding]:
        """An options struct's own methods and constants: its fields stand
        for Python's parameters, and nothing in Python stands for these. Nor
        does `#[non_exhaustive]`, which refuses the struct update a caller
        writes Python's keywords with."""
        item, before = self.surface.items[path], len(self.findings)
        for member in item.get("methods", []) + item.get("constants", []):
            self._find("rust", f"{path}::{member['name']}", "an options struct's own member: no Python member", path, None)
        self._buildable(item, None)
        return self.findings[before:]

    def excepted(self, path: str) -> list[Finding]:
        """A struct an `[[exception]]` covers whole, the options of a method
        only Rust has or a Rust-only record, is built by struct update all
        the same: `#[non_exhaustive]` is a finding no exception covers."""
        item, before = self.surface.items[path], len(self.findings)
        if item["kind"] == "struct":
            self._buildable(item, None)
        return self.findings[before:]

    def _buildable(self, item: dict, python: str | None) -> None:
        """A struct a caller builds, an options struct or a model, is not
        `#[non_exhaustive]`: outside its crate that refuses the struct update
        that stands for Python's keywords (E0639, convention 9)."""
        if item.get("non_exhaustive"):
            self._find("rust", item["path"], NON_EXHAUSTIVE, item["path"], python)

    def _find(self, side: str, path: str, detail: str, rust_item: str, python_item: str | None) -> None:
        self.findings.append(Finding(side, path, detail, rust_item, python_item))

    def _same(self, python: str, rust: str, rust_type: str) -> bool:
        """The names match, or the Rust one is a `Duration` that dropped
        Python's `_seconds`."""
        suffix = self.rules.duration_suffix
        return python == rust or (python == rust + suffix and "Duration" in rust_type)

    def _item(self, item: dict, python: str) -> None:
        if python.startswith(BUILTIN):
            return  # a strong type or an error a builtin stands for: no member to compare
        if item["path"].rpartition("::")[2] in self.rules.literals:
            # A `Literal`'s values are not in the snapshot: its field is.
            if not self.python.exists(python):
                self._find("rust", item["path"], f"a Literal of {python}: no such Python field", item["path"], python)
            return
        spec = self.python.specs.get(python)
        if spec is None:
            return  # a Python constant or alias: a name, and no member to compare
        if item["kind"] in ("type", "constant"):
            if spec:
                self._find("rust", item["path"], f"a {item['kind']}: {python} is a class with members", item["path"], python)
            return
        if item["kind"] != "fn":
            self._class(item, python, spec)
        elif "__call__" in spec:
            self._params(item["path"], item["signature"], python, spec["__call__"], (item["path"], python))
        else:
            self._find("rust", item["path"], f"a function: {python} is a class", item["path"], python)

    def _class(self, item: dict, python: str, spec: dict) -> None:
        cls = _Class(
            path=item["path"],
            python=python,
            methods={m["name"]: m for kind in ("methods", "required", "provided") for m in item.get(kind, [])},
            fields={f["name"]: f for f in item.get("fields", []) if not f["name"].isdigit()},
            members={variant_name(v["name"]): v["name"] for v in item.get("variants", [])}
            | {c["name"]: c["name"] for c in item.get("constants", [])},
        )
        if item["kind"] == "enum" and "Error" in item.get("impls", []):
            self._error_family(item, cls)
        bases, inherited = self._inherited(item)
        cls.owners = {name: owner for name, (owner, _) in inherited.items() if name not in cls.methods}
        cls.methods = {name: method for name, (_, method) in inherited.items()} | cls.methods
        maps = self.member_maps.get(cls.path, {})
        for member, member_spec in spec.items():
            if any(base.get(member) == member_spec for base in bases):
                continue  # the base's, compared on the trait it maps to
            entry = maps.get((python, member))
            if entry is not None:
                self.applied.add(id(entry))
            rust_name = self.surface.split(entry.rust)[1] if entry else None
            renames = entry.params if entry else {}
            if member_spec[0] == "member":
                self._enum_member(cls, member)
            elif member == "__init__":
                item_map = self.item_maps.get(cls.path)
                self._constructor(item, cls, member_spec, rust_name, renames or (item_map.params if item_map else {}))
            else:
                self._member(cls, member, member_spec, rust_name or member, renames)
        self._unmatched(cls, set(cls.owners))

    def _inherited(self, item: dict) -> tuple[list[dict], dict[str, tuple[str, dict]]]:
        """What the item takes from the traits of the surface it implements:
        the specs of their Python classes, and their methods with the trait
        each comes from. A Python subclass lists its base's members as its
        own; a Rust type provides them through the trait."""
        bases, methods = [], {}
        for implemented in item.get("impls", []):
            trait = self.surface.named("trait", re.split(r"[<\s]", implemented, maxsplit=1)[0], item["path"])
            if trait is None:
                continue
            python = self.python_of(trait)
            if python in self.python.specs:
                bases.append(self.python.specs[python])
            methods |= {m["name"]: (trait["path"], m) for kind in ("required", "provided") for m in trait.get(kind, [])}
        return bases, methods

    def _error_family(self, item: dict, cls: _Class) -> None:
        """An error enum is its Python base class, each variant one of its
        exception classes: `X` the class `XError`, or the family's
        `<Stem>XError` (`SkillError::Parse`, `SkillParseError`), or a
        builtin exception. A variant's named fields are the class's
        constructor's parameters, when the class has one; a class without
        takes a message, whatever the variant carries."""
        stem = cls.path.rpartition("::")[2].removesuffix("Error")
        packages = self.rules.packages_for(cls.path.rpartition("::")[0])
        for variant in item.get("variants", []):
            name = variant["name"]
            cls.used.add(name)
            if name in self.rules.builtin_errors:
                continue
            python = self.python.find(packages, f"{name}Error") or self.python.find(packages, f"{stem}{name}Error")
            path = f"{cls.path}::{name}"
            if python is None:
                self._find("rust", path, "an error variant: no Python exception class", cls.path, cls.python)
                continue
            init = self.python.specs.get(python, {}).get("__init__")
            if init is None:
                continue
            params = [param.name for param in python_signature(init)]
            fields = [field_["name"] for field_ in variant.get("fields", [])]
            for missing in (param for param in params if param not in fields):
                self._find("python", f"{python}.__init__({missing})", f"no field of {path}", cls.path, cls.python)
            for extra in (field_ for field_ in fields if field_ not in params):
                self._find("rust", f"{path}::{extra}", f"no parameter of {python}", cls.path, cls.python)

    def _enum_member(self, cls: _Class, member: str) -> None:
        rust = cls.members.get(member)
        if rust is None:
            self._find("python", f"{cls.python}.{member}", "no Rust variant or constant", cls.path, cls.python)
        else:
            cls.used.add(rust)

    def _member(self, cls: _Class, member: str, member_spec: list, rust_name: str, renames: dict) -> None:
        if member_spec[0] == "property":
            self._property(cls, member, rust_name)
            return
        method = cls.methods.get(rust_name)
        if method is None:
            self._find("python", f"{cls.python}.{member}", "no Rust method", cls.path, cls.python)
            return
        cls.used.add(rust_name)
        self._params(cls.method_path(rust_name), method, f"{cls.python}.{member}", member_spec, (cls.path, cls.python), renames)

    def _property(self, cls: _Class, member: str, rust_name: str) -> None:
        """A property is a field, or a method taking `&self` alone."""
        if rust_name in cls.fields:
            cls.used.add(rust_name)
            return
        method = cls.methods.get(rust_name)
        if method is None:
            self._find("python", f"{cls.python}.{member}", "a property: no Rust method or field", cls.path, cls.python)
            return
        cls.used.add(rust_name)
        if method["params"] or method["receiver"] is None:
            detail = f"{cls.python}.{member} is a property: a method of &self alone"
            self._find("rust", f"{cls.path}::{rust_name}", detail, cls.path, cls.python)

    def _constructor(self, item: dict, cls: _Class, init: list, rust_name: str | None, renames: dict) -> None:
        """A model's fields are Python's parameters, and its `new` takes the
        required ones; another type's `new` is `__init__`."""
        self._attributes(cls, init, item["kind"] == "trait")
        if item["kind"] == "trait":
            return
        new = cls.methods.get(rust_name or "new")
        if new is not None:
            cls.used.add(new["name"])
        if cls.fields and item["kind"] == "struct":
            cls.used.update(cls.fields)
            self._buildable(item, cls.python)
            self._model_fields(cls, init, renames)
            if new is not None:
                self._model_new(cls, init, new)
            else:
                self._model_without_new(cls, init, "Default" in item.get("impls", []))
        elif new is not None:
            path = f"{cls.path}::{new['name']}"
            self._params(path, new, f"{cls.python}.__init__", init, (cls.path, cls.python), renames)
        elif init[1:] or not ("Default" in item.get("impls", []) or re.fullmatch(r"pub struct \w+;", item.get("decl", ""))):
            self._find("python", f"{cls.python}.__init__", "no Rust constructor (new)", cls.path, cls.python)

    def _attributes(self, cls: _Class, init: list, trait: bool) -> None:
        """A constructor's parameter is also the attribute it sets: a Rust
        method of that name taking `&self` alone. A trait has no
        constructor: its Python class's parameters are its methods."""
        for param in python_signature(init):
            method = cls.methods.get(param.name)
            if method is not None and method["receiver"] is not None and not method["params"]:
                cls.used.add(param.name)
            elif trait:
                detail = "an attribute: no Rust method of &self alone"
                self._find("python", f"{cls.python}.__init__({param.name})", detail, cls.path, cls.python)

    def _model_fields(self, cls: _Class, init: list, renames: dict) -> None:
        params = [param for param in python_signature(init) if not self._union_tag(cls.path, param)]
        for param in params:
            if not any(self._same(param.name, renames.get(n, n), f["type"]) for n, f in cls.fields.items()):
                self._find("python", f"{cls.python}.__init__({param.name})", "no Rust field", cls.path, cls.python)
        for name, field_ in cls.fields.items():
            if not any(self._same(param.name, renames.get(name, name), field_["type"]) for param in params):
                self._find("rust", f"{cls.path}::{name}", "no Python parameter", cls.path, cls.python)

    def _union_tag(self, path: str, param) -> bool:
        """Python's `type='text'` of a content: the tag of the tagged union
        the Rust type is a variant of (`EventContent::Text(TextContent)`),
        no field or parameter of its own."""
        if param.name != "type" or not (param.default or "").startswith("'"):
            return False
        name = path.rpartition("::")[2]
        return any(
            variant["decl"].endswith(f"({name})")
            for item in self.surface.items.values()
            if item["kind"] == "enum"
            for variant in item.get("variants", [])
        )

    def _model_without_new(self, cls: _Class, init: list, default: bool) -> None:
        """A model Rust builds by struct literal alone cannot take Python's
        defaults: `new(<required fields>)` sets them, or `Default` when
        Python requires nothing (convention 4)."""
        params = python_signature(init)
        defaulted = [param.name for param in params if not param.required]
        required = [param.name for param in params if param.required]
        if not defaulted or (default and not required):
            return
        by = f"new({', '.join(required)})" if required else "new or Default"
        detail = f"Python defaults {', '.join(defaulted)}: no Rust {by} sets them"
        self._find("python", f"{cls.python}.__init__", detail, cls.path, cls.python)

    def _model_new(self, cls: _Class, init: list, new: dict) -> None:
        params = python_signature(init)
        positional: set[str] = set()
        for rust in self._rust_params(f"{cls.path}::{new['name']}", new, cls.path):
            param = next((p for p in params if self._same(p.name, rust.name, rust.type)), None)
            if param is None:
                self._find("rust", rust.path, "no Python parameter", cls.path, cls.python)
            elif rust.positional and not param.required:
                detail = f"Python defaults it ({param.name}={param.default}): a field set by struct update"
                self._find("rust", rust.path, detail, cls.path, cls.python)
            elif not rust.positional and param.required:
                self._find("rust", rust.path, "Python requires it: a parameter of new", cls.path, cls.python)
            if rust.positional and param is not None:
                positional.add(param.name)
        for param in params:
            known = any(self._same(param.name, name, field_["type"]) for name, field_ in cls.fields.items())
            if param.required and param.name not in positional and known:
                detail = f"required: a parameter of Rust's {new['name']}"
                self._find("python", f"{cls.python}.__init__({param.name})", detail, cls.path, cls.python)

    def _unmatched(self, cls: _Class, inherited: set[str]) -> None:
        for name in cls.methods:
            if name not in cls.used and name not in inherited:
                self._find("rust", f"{cls.path}::{name}", "no Python member", cls.path, cls.python)
        for name in cls.fields:
            if name not in cls.used:
                self._find("rust", f"{cls.path}::{name}", "no Python parameter or property", cls.path, cls.python)
        for name in cls.members.values():
            if name not in cls.used and not self._builtin_payload(cls, name):
                self._find("rust", f"{cls.path}::{name}", "no Python member", cls.path, cls.python)

    def _builtin_payload(self, cls: _Class, variant: str) -> bool:
        """A variant carrying a strong type stands for Python's value of
        that builtin, no member: `Visibility::Channels(ChannelSet)`, Python's
        list of channels."""
        item = self.surface.items[cls.path]
        decls = [found["decl"] for found in item.get("variants", []) if found["name"] == variant]
        payload = re.fullmatch(r"\w+\((\w+)\)", decls[0]) if decls else None
        return payload is not None and payload.group(1) in self.rules.builtins

    def _params(self, rust_path: str, method: dict, python_path: str, member_spec: list, owner: tuple, renames=None) -> None:
        """A method's parameters against Python's: a positional one for each
        required, an option for each defaulted."""
        rust = self._rust_params(rust_path, method, owner[0])
        renames = renames or {}
        taken: set[str] = set()
        for param in python_signature(member_spec):
            if self._union_tag(owner[0], param):
                continue
            named = [r for r in rust if renames.get(r.name) == param.name]
            if param.variadic and named:
                # `**kwargs` stands for every parameter a [[map]] entry names
                # it for: the fields the keywords reach.
                taken.update(r.path for r in named)
                continue
            pair = next((r for r in rust if r.path not in taken and self._stands_for(param.name, r, renames)), None)
            if pair is None:
                self._find("python", f"{python_path}({param.name})", "no Rust parameter", *owner)
                continue
            taken.add(pair.path)
            if param.required and not pair.positional:
                self._find("rust", pair.path, "Python requires it: a positional parameter", *owner)
            elif not param.required and not param.variadic and pair.positional:
                self._find("rust", pair.path, f"Python defaults it ({param.name}={param.default}): an option", *owner)
        for param in rust:
            if param.path not in taken:
                self._find("rust", param.path, "no Python parameter", *owner)

    def _stands_for(self, python: str, rust: _RustParam, renames: dict) -> bool:
        renamed = renames.get(rust.name)
        return renamed == python if renamed else self._same(python, rust.name, rust.type)

    def _rust_params(self, rust_path: str, method: dict, item: str) -> list[_RustParam]:
        """A method's parameters, an options struct's fields standing for
        the defaulted ones: `<Method>Options`, or `<Type>Options` for `new`."""
        wanted = {f"{_camel(method['name'])}Options"}
        if method["name"] == "new":
            wanted.add(f"{item.rpartition('::')[2]}Options")
        found: list[_RustParam] = []
        for param in method["params"]:
            base = _base_type(param["type"])
            options = self.surface.named("struct", base, rust_path) if base in wanted else None
            if options is None:
                found.append(_RustParam(param["name"].lstrip("_"), param["type"], True, f"{rust_path}({param['name']})"))
                continue
            self.options_used.add(options["path"])
            found += [
                _RustParam(f["name"], f["type"], False, f"{options['path']}::{f['name']}") for f in options.get("fields", [])
            ]
        return found


def _camel(snake: str) -> str:
    return "".join(part[:1].upper() + part[1:] for part in snake.split("_"))


def _base_type(rust_type: str) -> str:
    """`&'a mut path::Name<T>` as `Name`."""
    stripped = re.sub(r"^&(?:'\w+\s+)?(?:mut\s+)?", "", rust_type.strip())
    return re.split(r"[<\s]", stripped, maxsplit=1)[0].rpartition("::")[2]
