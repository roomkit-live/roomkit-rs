"""What the entries cover: each finding's entry, the dead entries, and the
moved Python specs."""

from __future__ import annotations

from dataclasses import dataclass

from api_parity.compare import Comparison, Finding, ItemResult
from api_parity.mapping import Entry, Mapping
from api_parity.sides import Python, Surface, spec_hash


@dataclass(frozen=True)
class Dead:
    entry: str
    path: str
    why: str


@dataclass
class Report:
    surface: Surface
    python: Python
    mapping: Mapping
    items: dict[str, ItemResult]
    findings: list[Finding]
    uncovered: list[Finding]
    dead: list[Dead]
    moved: list[Entry]
    pending: dict[str, list[Entry]]


def compare(surface: Surface, python: Python, mapping: Mapping) -> Report:
    comparison = Comparison(surface, python, mapping)
    items = comparison.run()
    _settle(items, comparison, mapping)
    findings = comparison.findings
    uncovered = _cover(findings, mapping)
    dead = _dead(surface, python, mapping, comparison.applied)
    moved = [
        entry
        for entry in mapping.of("map")
        if python.exists(entry.python) and entry.py_spec != spec_hash(python.spec_of(entry.python))
    ]
    pending: dict[str, list[Entry]] = {}
    for entry in mapping.of("pending"):
        pending.setdefault(entry.lot, []).append(entry)
    return Report(surface, python, mapping, items, findings, uncovered, dead, moved, pending)


def _settle(items: dict[str, ItemResult], comparison: Comparison, mapping: Mapping) -> None:
    """Each item's status. An options struct a compared method takes stands
    for Python's parameters: it is no unmapped item."""
    exceptions = {entry.rust for entry in mapping.of("exception") if entry.rust and entry.whole}
    for path, result in items.items():
        if path in comparison.options_used and result.python is None:
            comparison.findings = [f for f in comparison.findings if not (f.path == path and f.detail == "no Python name")]
            result.findings, result.status = comparison.own_members(path), "options"
        elif path in exceptions:
            result.status = "exception"
        elif result.python is None:
            result.status = "unmapped"
        else:
            result.status = "differs" if result.findings else "matches"


def _cover(findings: list[Finding], mapping: Mapping) -> list[Finding]:
    """Gives each finding to the first entry covering it, and returns those
    no entry covers."""
    entries = [entry for entry in mapping.entries if entry.table != "map"]
    uncovered = []
    for finding in findings:
        entry = next((entry for entry in entries if _covers(entry, finding)), None)
        if entry is None:
            uncovered.append(finding)
        else:
            entry.covered.append(finding)
    return uncovered


def _covers(entry: Entry, finding: Finding) -> bool:
    """An entry covers what it names; one naming a whole item, everything of
    that item on its side, and an exception, on both sides."""
    for side, path in entry.paths():
        if entry.whole:
            if entry.table == "exception" and path in (finding.rust_item, finding.python_item):
                return True
            if side == finding.side and _within(path, finding.path, side):
                return True
        elif side == finding.side and path == finding.path:
            return True
    return False


def _within(base: str, path: str, side: str) -> bool:
    joint = "::" if side == "rust" else "."
    return path == base or path.startswith(base + joint) or path.startswith(base + "(")


def _dead(surface: Surface, python: Python, mapping: Mapping, applied: set[int]) -> list[Dead]:
    """The entries naming what is gone, a `[[map]]` that applies nowhere, and
    an entry, or a name of it, covering no finding."""
    dead = []
    for entry in mapping.entries:
        gone = [(side, path) for side, path in entry.paths() if not (surface if side == "rust" else python).exists(path)]
        dead += [Dead(entry.name, path, f"names nothing of the {side} side") for side, path in gone]
        if gone:
            continue
        if entry.table == "map":
            if id(entry) not in applied:
                dead.append(Dead(entry.name, entry.rust, f"applies nowhere: {entry.python} is no member of the item's Python class"))
        elif entry.whole:
            if not entry.covered:
                dead.append(Dead(entry.name, entry.paths()[0][1], "covers nothing: no finding there"))
        else:
            covered = {finding.path for finding in entry.covered}
            dead += [Dead(entry.name, path, "covers nothing: no finding there") for _, path in entry.paths() if path not in covered]
    return dead
