"""`api/mapping.toml`: the rules, and the entries a rule does not give."""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from api_parity import ParityError

LOTS = ("1", "2", "3", "4", "5", "6", "?")
# A [[missing]] entry's scope: the text side, which the API work drives to
# zero; voice, video, realtime, recording, vision and conference; and tier 1b
# (RMK-1544: transport providers, other AI providers' extras, sources, the
# WebSocket and webhook helpers, injected HTTP transports, protocol traces).
SCOPES = ("text", "media", "tier1b")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_PYTHON_NAME = re.compile(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+(?:\([A-Za-z_]\w*\))?")
_TOKEN = re.compile(r"[A-Z][a-z0-9]*|[a-z0-9]+")
# A Python private name: one leading underscore, not a dunder. A mangled
# `__x` (no trailing `__`) is not caught: the snapshot holds none.
_PRIVATE = re.compile(r"_(?!_)\w*")
# Each table's keys: those it requires, and those it may hold.
_KEYS = {
    "map": ({"rust", "python", "py_spec"}, {"params", "reason"}),
    "exception": ({"approved", "reason"}, {"rust", "python", "members", "params", "until"}),
    "missing": ({"python", "reason", "scope"}, {"members", "params"}),
    "pending": ({"lot", "reason"}, {"rust", "python", "members", "params"}),
}


def tokens(name: str) -> list[str]:
    return _TOKEN.findall(name)


def variant_name(variant: str) -> str:
    """A variant `CamelCase` as Python's member `UPPER_SNAKE`."""
    return "_".join(token.upper() for token in tokens(variant))


def snake_name(variant: str) -> str:
    """A variant `CamelCase` as the `snake_case` value it serializes to."""
    return "_".join(token.lower() for token in tokens(variant))


# The tables of [rules], and those of them that lot 2 of step 5 added, which
# a mapping may leave out.
_RULES = {
    "acronyms",
    "duration_suffix",
    "packages",
    "builtins",
    "builtin_payloads",
    "literals",
    "builtin_errors",
    "error_families",
}
_RULES_REQUIRED = {"acronyms", "duration_suffix", "packages"}


@dataclass(frozen=True)
class Builtin:
    """A Rust type that stands for a Python builtin (decision 4): its name,
    and the Rust members the architecture table maps to the builtin's
    operations."""

    python: str
    members: tuple[str, ...] = ()


@dataclass(frozen=True)
class Literal:
    """A Rust enum that stands for a Python `Literal`: the field it types,
    `pkg.Class.__init__(field)`, and the Literal's values, read from the
    Python source, which the snapshot does not hold."""

    field: str
    values: tuple[str, ...]


@dataclass
class Rules:
    acronyms: list[tuple[list[str], str]]
    duration_suffix: str
    packages: dict[str, list[str]]
    # By the Rust type's path.
    builtins: dict[str, Builtin] = field(default_factory=dict)
    # The variants carrying a builtin that stand for Python's value of it,
    # by path (`roomkit::model::Visibility::Channels`).
    builtin_payloads: tuple[str, ...] = ()
    # By the Rust enum's path.
    literals: dict[str, Literal] = field(default_factory=dict)
    # An error variant that stands for a Python builtin exception, by the
    # variant's path (`roomkit::RoomKitError::Value`).
    builtin_errors: dict[str, str] = field(default_factory=dict)
    # By the Rust error enum's path: the Python exception classes its
    # variants stand for, each with the fields its variant may carry when
    # the class takes a message alone (no `__init__` of its own). The
    # snapshot holds no class's bases, so a family is written here.
    error_families: dict[str, dict[str, tuple[str, ...]]] = field(default_factory=dict)

    @classmethod
    def from_table(cls, table: dict) -> Rules:
        if not _RULES_REQUIRED <= set(table) <= _RULES:
            raise ParityError(
                f"[rules] holds acronyms, duration_suffix and packages, and may hold {', '.join(sorted(_RULES - _RULES_REQUIRED))}, "
                f"not {sorted(table)}"
            )
        acronyms = sorted(
            ((tokens(rust), python) for rust, python in table["acronyms"].items()), key=lambda pair: -len(pair[0])
        )
        builtin_errors = dict(table.get("builtin_errors", {}))
        _rust_paths("builtin_errors", list(builtin_errors))
        return cls(
            acronyms,
            table["duration_suffix"],
            dict(table["packages"]),
            {path: _builtin(path, raw) for path, raw in table.get("builtins", {}).items()},
            tuple(_rust_paths("builtin_payloads", table.get("builtin_payloads", []))),
            {path: _literal(path, raw) for path, raw in table.get("literals", {}).items()},
            builtin_errors,
            {path: _family(path, raw) for path, raw in table.get("error_families", {}).items()},
        )

    def builtin(self, path: str) -> Builtin | None:
        return self.builtins.get(path)

    def ruled_types(self) -> list[str]:
        """The Rust types a rule maps, by path: its builtins, its literals and
        its error families."""
        return [*self.builtins, *self.literals, *self.error_families]

    def python_type_name(self, rust: str) -> str:
        """A Rust type's name with its acronyms in Python's casing."""
        left, out = tokens(rust), []
        while left:
            for words, python in self.acronyms:
                if left[: len(words)] == words:
                    out.append(python)
                    left = left[len(words) :]
                    break
            else:
                out.append(left.pop(0))
        return "".join(out)

    def packages_for(self, module: str) -> list[str]:
        """The Python packages a Rust module's items are looked for in: the
        module's own, or its closest listed parent's."""
        parts = module.split("::")
        for end in range(len(parts), 0, -1):
            listed = self.packages.get("::".join(parts[:end]))
            if listed is not None:
                return listed
        raise ParityError(f"[rules.packages] lists no package for {module}")


def _rust_paths(table: str, paths: list) -> list[str]:
    for path in paths:
        if not isinstance(path, str) or "::" not in path:
            raise ParityError(f"[rules] {table}: {path!r} is no Rust path")
    return list(paths)


def _builtin(path: str, raw: dict) -> Builtin:
    _rust_paths("builtins", [path])
    if not isinstance(raw, dict) or set(raw) - {"python", "members"} or "python" not in raw:
        raise ParityError(f"[rules] builtins: {path} is {{ python = name, members = [...] }}, not {raw!r}")
    return Builtin(raw["python"], tuple(raw.get("members", [])))


def _literal(path: str, raw: dict) -> Literal:
    _rust_paths("literals", [path])
    if not isinstance(raw, dict) or set(raw) != {"field", "values"}:
        raise ParityError(f"[rules] literals: {path} is {{ field = pkg.Class.__init__(field), values = [...] }}")
    if not _PYTHON_NAME.fullmatch(raw["field"]) or "(" not in raw["field"]:
        raise ParityError(f"[rules] literals: {path} names pkg.Class.__init__(field), not {raw['field']}")
    return Literal(raw["field"], tuple(raw["values"]))


def _family(path: str, raw: dict) -> dict[str, tuple[str, ...]]:
    _rust_paths("error_families", [path])
    if not isinstance(raw, dict) or not all(_PYTHON_NAME.fullmatch(name) for name in raw):
        raise ParityError(f"[rules] error_families: {path} maps each Python class to the fields its variant carries")
    return {name: tuple(fields) for name, fields in raw.items()}


@dataclass
class Entry:
    table: str
    index: int
    rust: str | None = None
    python: str | None = None
    members: list[str] = field(default_factory=list)
    params: list[str] | dict = field(default_factory=list)
    lot: str | None = None
    reason: str = ""
    approved: str | None = None
    until: str | None = None
    py_spec: str | None = None
    scope: str | None = None
    covered: list = field(default_factory=list)

    @property
    def name(self) -> str:
        return f"[[{self.table}]] #{self.index + 1} ({self.rust or self.python})"

    @property
    def whole(self) -> bool:
        """The entry names a whole item, not some of its members."""
        return not self.members and not (self.params and isinstance(self.params, list))

    def paths(self) -> list[tuple[str, str]]:
        """Each (side, path) the entry names."""
        if self.table == "map":
            return [("rust", self.rust), ("python", self.python)]
        side, base = ("rust", self.rust) if self.rust else ("python", self.python)
        joint = "::" if side == "rust" else "."
        if self.members:
            return [(side, f"{base}{joint}{member}") for member in self.members]
        if self.params:
            return [(side, f"{base}({param})") for param in self.params]
        return [(side, base)]


class Mapping:
    def __init__(self, rules: Rules, entries: list[Entry]) -> None:
        self.rules = rules
        self.entries = entries

    @classmethod
    def from_table(cls, table: dict, rules_only: bool = False) -> Mapping:
        unknown = set(table) - {"rules", *_KEYS}
        if unknown or "rules" not in table:
            raise ParityError(f"the mapping holds [rules] and {sorted(_KEYS)}, not {sorted(unknown) or 'no [rules]'}")
        entries = [entry(kind, index, raw) for kind in _KEYS for index, raw in enumerate(table.get(kind, []))]
        return cls(Rules.from_table(table["rules"]), [] if rules_only else entries)

    def of(self, table: str) -> list[Entry]:
        return [entry for entry in self.entries if entry.table == table]


def entry(table: str, index: int, raw: dict) -> Entry:
    """One entry, checked against its table's keys."""
    required, optional = _KEYS[table]
    where = f"[[{table}]] #{index + 1}"
    if missing := required - set(raw):
        raise ParityError(f"{where}: {', '.join(sorted(missing))} missing")
    if unknown := set(raw) - required - optional:
        raise ParityError(f"{where}: unknown {', '.join(sorted(unknown))}")
    if table in ("exception", "pending") and ("rust" in raw) == ("python" in raw):
        raise ParityError(f"{where}: names either rust or python")
    if "members" in raw and "params" in raw:
        raise ParityError(f"{where}: lists either members or params")
    if table != "map" and "python" in raw and not raw.get("members") and not raw.get("params"):
        raise ParityError(f"{where}: names a whole Python class: an entry names its members or params")
    if "until" in raw and not _PYTHON_NAME.fullmatch(str(raw["until"])):
        raise ParityError(f"{where}: until is the Python name whose arrival ends the exception")
    if table == "exception" and not _DATE.fullmatch(str(raw["approved"])):
        raise ParityError(f"{where}: approved is a date, YYYY-MM-DD")
    if table == "pending" and str(raw["lot"]) not in LOTS:
        raise ParityError(f"{where}: lot is one of {', '.join(LOTS)}")
    if table == "missing":
        _check_missing(where, raw)
    if table == "map" and not str(raw["py_spec"]).startswith("sha256:"):
        raise ParityError(f"{where}: py_spec is sha256:<hex>, as api-parity.py --spec NAME prints it")
    params = raw.get("params", {} if table == "map" else [])
    if isinstance(params, dict) != (table == "map"):
        raise ParityError(f"{where}: params is " + ("a table of Rust names to Python names" if table == "map" else "a list"))
    return Entry(
        table=table,
        index=index,
        rust=raw.get("rust"),
        python=raw.get("python"),
        members=list(raw.get("members", [])),
        params=params,
        lot=str(raw["lot"]) if "lot" in raw else None,
        reason=raw.get("reason", ""),
        approved=str(raw["approved"]) if "approved" in raw else None,
        until=raw.get("until"),
        py_spec=raw.get("py_spec"),
        scope=raw.get("scope"),
    )


def _check_missing(where: str, raw: dict) -> None:
    """A [[missing]] entry is a gap: its scope one of SCOPES, and neither a
    Python private name nor a form the language forces, which an
    [[exception]] holds with its row in the names table."""
    if raw["scope"] not in SCOPES:
        raise ParityError(f"{where}: scope is one of {', '.join(SCOPES)}")
    private = [name for name in [*raw.get("members", []), *raw.get("params", [])] if _PRIVATE.fullmatch(name)]
    if private:
        raise ParityError(f"{where}: {', '.join(private)}: a Python private name is no gap, an [[exception]] holds it")
    if str(raw["reason"]).startswith("language:"):
        raise ParityError(f"{where}: a form the language forces is no gap, an [[exception]] holds it")


def load_mapping(path: Path, rules_only: bool) -> Mapping:
    try:
        table = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ParityError(f"{path}: {error}") from error
    return Mapping.from_table(table, rules_only)
