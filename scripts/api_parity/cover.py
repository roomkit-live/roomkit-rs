"""What the entries cover: each finding's entry, the dead entries, and the
moved Python specs."""

from __future__ import annotations

from dataclasses import dataclass

from api_parity.compare import NON_EXHAUSTIVE, RULES_FINDING, Comparison, Finding, ItemResult
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
    due: list[Entry]


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
    due = [entry for entry in mapping.of("exception") if entry.until and python.exists(entry.until)]
    return Report(surface, python, mapping, items, findings, uncovered, dead, moved, pending, due)


def blocking(report: Report) -> list[str]:
    """What fails the gate before the tag (step 5, lot 6): a finding no
    entry covers (a Rust item, member or parameter without a mapping, a
    Python one without its Rust side), a dead entry, a pending entry. None
    for a report the gate passes."""
    pending = sum(len(entries) for entries in report.pending.values())
    counts = [
        (len(report.uncovered), "finding no entry covers", "findings no entry covers"),
        (len(report.dead), "dead entry", "dead entries"),
        (pending, "pending entry", "pending entries"),
    ]
    return [f"{count} {one if count == 1 else many}" for count, one, many in counts if count]


def _settle(items: dict[str, ItemResult], comparison: Comparison, mapping: Mapping) -> None:
    """Each item's status. An options struct a compared method takes stands
    for Python's parameters: it is no unmapped item."""
    exceptions = {entry.rust for entry in mapping.of("exception") if entry.rust and entry.whole}
    for path, result in items.items():
        if path in comparison.options_used and result.python is None:
            comparison.findings = [f for f in comparison.findings if not (f.path == path and f.detail == "no Python name")]
            result.findings, result.status = comparison.own_members(path), "options"
        elif path in exceptions and any(finding.path == path for finding in result.findings):
            result.status = "exception"
            result.findings += comparison.excepted(path)
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
    """An entry covers what it names. One naming a whole Rust item covers
    the item's own finding alone ("no Python name", or the alias named as a
    class), never its members: it goes dead once the item maps. An
    `[[exception]]` approves the item's form, never its `#[non_exhaustive]`,
    which refuses the struct update a caller builds it with. No entry covers
    a finding about the rules themselves (`listed in [rules] ...`): the
    rules are fixed, not excused."""
    if finding.detail.startswith(RULES_FINDING):
        return False
    if entry.whole:
        side, path = entry.paths()[0]
        if entry.table == "exception" and finding.detail == NON_EXHAUSTIVE:
            return False
        return side == finding.side == "rust" and finding.path == path
    return any(side == finding.side and path == finding.path for side, path in entry.paths())


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
