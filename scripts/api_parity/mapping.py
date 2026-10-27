"""`api/mapping.toml`: the rules, and the entries a rule does not give."""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from api_parity import ParityError

LOTS = ("1", "2", "3", "4", "5", "6", "?")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_PYTHON_NAME = re.compile(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+(?:\([A-Za-z_]\w*\))?")
_TOKEN = re.compile(r"[A-Z][a-z0-9]*|[a-z0-9]+")
# Each table's keys: those it requires, and those it may hold.
_KEYS = {
    "map": ({"rust", "python", "py_spec"}, {"params", "reason"}),
    "exception": ({"approved", "reason"}, {"rust", "python", "members", "params", "until"}),
    "missing": ({"python", "reason"}, {"members", "params"}),
    "pending": ({"lot", "reason"}, {"rust", "python", "members", "params"}),
}


def tokens(name: str) -> list[str]:
    return _TOKEN.findall(name)


def variant_name(variant: str) -> str:
    """A variant `CamelCase` as Python's member `UPPER_SNAKE`."""
    return "_".join(token.upper() for token in tokens(variant))


@dataclass
class Rules:
    acronyms: list[tuple[list[str], str]]
    duration_suffix: str
    packages: dict[str, list[str]]

    @classmethod
    def from_table(cls, table: dict) -> Rules:
        if set(table) != {"acronyms", "duration_suffix", "packages"}:
            raise ParityError(f"[rules] holds acronyms, duration_suffix and packages, not {sorted(table)}")
        acronyms = sorted(
            ((tokens(rust), python) for rust, python in table["acronyms"].items()), key=lambda pair: -len(pair[0])
        )
        return cls(acronyms, table["duration_suffix"], dict(table["packages"]))

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
    )


def load_mapping(path: Path, rules_only: bool) -> Mapping:
    try:
        table = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ParityError(f"{path}: {error}") from error
    return Mapping.from_table(table, rules_only)
