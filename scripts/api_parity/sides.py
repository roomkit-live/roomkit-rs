"""The two sides: the Rust surface and Python's pinned snapshot."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from api_parity import ParityError

SNAPSHOT = ("public_api_names.json", "public_api_signatures.json")


def spec_hash(spec: object) -> str:
    """The fingerprint a `[[map]]` entry keeps of the Python spec it was
    written against."""
    text = json.dumps(spec, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(text.encode()).hexdigest()


def fingerprint(directory: Path) -> str:
    """The copies' fingerprint, as `make api-sync` writes it: the sha256 of
    what `sha256sum public_api_names.json public_api_signatures.json`
    prints."""
    listing = "".join(
        f"{hashlib.sha256((directory / name).read_bytes()).hexdigest()}  {name}\n" for name in SNAPSHOT
    )
    return hashlib.sha256(listing.encode()).hexdigest()


def strip_param(path: str) -> tuple[str, str | None]:
    """`a.b(param)` as (`a.b`, `param`)."""
    if path.endswith(")") and "(" in path:
        head, _, param = path[:-1].partition("(")
        return head, param
    return path, None


def names(entries: list[dict]) -> list[str]:
    return [entry["name"] for entry in entries]


class Python:
    """The snapshot: every public name's spec, and every `__all__`."""

    def __init__(self, signatures: dict, exports: dict, commit: str = "") -> None:
        self.specs = signatures
        self.exports = {package: set(listed) for package, listed in exports.items()}
        self.commit = commit
        self._exported = {f"{package}.{name}" for package, listed in exports.items() for name in listed}
        self._described: dict[str, list[str]] = {}
        for path in signatures:
            self._described.setdefault(path.rpartition(".")[2], []).append(path)

    def find(self, packages: list[str], name: str) -> str | None:
        """The path of `name` exported by one of `packages`: the snapshot's,
        which is the shortest a name is exported under, or the export
        itself for a constant or an alias the snapshot does not describe."""
        for package in packages:
            path = f"{package}.{name}"
            if path in self.specs:
                return path
            if name in self.exports.get(package, ()):
                described = self._described.get(name, [])
                if len(described) > 1:
                    raise ParityError(f"{path}: the snapshot describes {len(described)} objects of that name")
                return described[0] if described else path
        return None

    def split(self, path: str) -> tuple[str, str | None, str | None]:
        """`pkg.Class.member(param)` as (name, member, param); a function's
        parameter is its `__call__`'s."""
        path, param = strip_param(path)
        if path in self.specs or path in self._exported:
            return path, ("__call__" if param is not None else None), param
        head, _, last = path.rpartition(".")
        return (head, last, param) if head in self.specs else (path, None, param)

    def exists(self, path: str) -> bool:
        base, member, param = self.split(path)
        if member is None:
            return param is None and (base in self.specs or base in self._exported)
        spec = self.specs.get(base, {})
        if member not in spec:
            return False
        return param is None or param in [p.name for p in python_signature(spec[member])]

    def spec_of(self, path: str) -> object | None:
        """The spec a `[[map]]` entry's hash is of: a class's, or a member's."""
        base, member, _ = self.split(path)
        spec = self.specs.get(base)
        if spec is None or member is None:
            return spec
        return spec.get(member)


class PythonParam:
    def __init__(self, raw: str) -> None:
        name, has_default, default = raw.partition("=")
        self.variadic = name.startswith("*")
        self.name = name.lstrip("*")
        self.required = not has_default and not self.variadic
        self.default = default if has_default else None


def python_signature(spec: list[str]) -> list[PythonParam]:
    """A member's parameters, without the `*` and `/` markers."""
    return [PythonParam(raw) for raw in spec[1:] if raw not in ("*", "/")]


class Surface:
    """The Rust surface `scripts/rs-surface.py` wrote."""

    def __init__(self, written: dict) -> None:
        self.rustdoc = written.get("rustdoc", "")
        self.counts = written.get("counts", {})
        self.items = {item["path"]: item for found in written["crates"].values() for item in found}
        self._named: dict[tuple[str, str], list[str]] = {}
        for path, item in self.items.items():
            self._named.setdefault((item["kind"], path.rpartition("::")[2]), []).append(path)

    def named(self, kind: str, name: str, near: str) -> dict | None:
        """The item of that kind and name, the one in `near`'s crate first."""
        found = self._named.get((kind, name), [])
        crate = near.split("::")[0]
        chosen = [path for path in found if path.split("::")[0] == crate] or found
        return self.items[chosen[0]] if chosen else None

    def split(self, path: str) -> tuple[str, str | None, str | None]:
        """`crate::Item::member(param)` as (item, member, param); a
        function's parameter has no member."""
        path, param = strip_param(path)
        if path in self.items:
            return path, None, param
        head, _, last = path.rpartition("::")
        return (head, last, param) if head in self.items else (path, None, param)

    def exists(self, path: str) -> bool:
        base, member, param = self.split(path)
        item = self.items.get(base)
        if item is None:
            return self._variant_field(path)
        if member is None:
            return param is None or param in names(item.get("signature", {}).get("params", []))
        found = member_of(item, member)
        return found is not None and (param is None or param in names(found.get("params", [])))


    def _variant_field(self, path: str) -> bool:
        """`crate::Enum::Variant::field`, a named field of an enum's variant."""
        rest, _, field_ = path.rpartition("::")
        enum, _, variant = rest.rpartition("::")
        found = member_of(self.items.get(enum, {}), variant)
        return found is not None and field_ in [named["name"] for named in found.get("fields", [])]


def member_of(item: dict, name: str) -> dict | None:
    """A Rust item's field, method, trait method, constant or variant."""
    for kind in ("fields", "methods", "required", "provided", "constants", "variants"):
        for member in item.get(kind, []):
            if member["name"] == name:
                return member
    return None


def load_python(directory: Path) -> Python:
    """The pinned snapshot, refused when its copies differ from their
    fingerprint."""
    try:
        expected = (directory / "SNAPSHOT_SHA256").read_text(encoding="utf-8").strip()
        commit = (directory / "ROOMKIT_COMMIT").read_text(encoding="utf-8").strip()
        actual = fingerprint(directory)
        exports = json.loads((directory / "public_api_names.json").read_text(encoding="utf-8"))
        signatures = json.loads((directory / "public_api_signatures.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ParityError(f"{directory}: the pinned snapshot cannot be read: {error}") from error
    if actual != expected:
        raise ParityError(
            f"{directory}: the copies' fingerprint is {actual}, SNAPSHOT_SHA256 holds {expected}: "
            "the copies are never edited, run make api-sync"
        )
    return Python(signatures, exports, commit)


def load_surface(path: Path) -> Surface:
    try:
        surface = Surface(json.loads(path.read_text(encoding="utf-8")))
    except (OSError, ValueError, KeyError) as error:
        raise ParityError(f"{path}: the surface cannot be read: {error}") from error
    if not surface.items:
        raise ParityError(f"{path}: the surface holds no item")
    return surface
