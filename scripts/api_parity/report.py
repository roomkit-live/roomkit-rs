"""The report, as text and as JSON."""

from __future__ import annotations

from api_parity.compare import Finding
from api_parity.cover import Report
from api_parity.mapping import LOTS, SCOPES

_ORDER = ("matches", "differs", "options", "exception", "unmapped")


def render(report: Report, rules_only: bool) -> str:
    lines: list[str] = []
    _header(lines.append, report, rules_only)
    unmapped = [f for f in report.uncovered if f.detail == "no Python name"]
    _section(lines.append, "Unmapped Rust items, without an entry", [f"{f.path} ({report.items[f.path].kind})" for f in unmapped])
    _one_sided(lines.append, [f for f in report.uncovered if f.detail != "no Python name"])
    _section(lines.append, "Python specs moved since their [[map]] entry", [f"{e.name}: {e.python}" for e in report.moved])
    _section(lines.append, "Dead entries", [f"{d.entry}: {d.path}: {d.why}" for d in report.dead])
    _section(lines.append, "Exceptions due, their Python name now in the snapshot", [f"{e.name}: {e.until}" for e in report.due])
    if not rules_only:
        _pending(lines.append, report)
        for table in ("exception", "missing"):
            entries = report.mapping.of(table)
            lines.append(f"== [[{table}]]: {len(entries)} entries, {sum(len(e.covered) for e in entries)} findings")
        lines.append("")
    _section(
        lines.append,
        "Items",
        [f"{r.status:<9} {r.path} ({r.kind})" + (f" -> {r.python}" if r.python else "") for r in report.items.values()],
    )
    if not rules_only:
        _scopes(lines.append, report)
    return "\n".join(lines)


def _header(out, report: Report, rules_only: bool) -> None:
    python, surface, items = report.python, report.surface, report.items
    statuses = {status: sum(1 for r in items.values() if r.status == status) for status in _ORDER}
    in_scope = {r.python for r in items.values() if r.python in python.specs}
    counts = surface.counts
    out(f"API parity: roomkit-rs against roomkit {python.commit[:12] or '(no commit)'}, rustdoc {surface.rustdoc}")
    out(
        f"Rust: {len(items)} items, {counts.get('methods', 0)} methods, {counts.get('associated_constants', 0)} "
        f"associated constants, {counts.get('trait_methods', 0)} trait methods, {counts.get('fields', 0)} fields, "
        f"{counts.get('variants', 0)} variants. Python: {len(python.specs)} names described, {len(in_scope)} in scope."
    )
    tables = ("map", "exception", "missing", "pending")
    out("Mapping: rules only." if rules_only else "Mapping: " + ", ".join(f"{len(report.mapping.of(t))} [[{t}]]" for t in tables) + ".")
    out("Items: " + ", ".join(f"{count} {status}" for status, count in statuses.items()) + ".")
    covered = len(report.findings) - len(report.uncovered)
    out(f"Findings: {len(report.findings)}, {covered} covered by an entry, {len(report.uncovered)} not.")
    out("")


def _section(out, title: str, lines: list[str]) -> None:
    out(f"== {title}: {len(lines)}")
    for line in lines:
        out(f"  {line}")
    out("")


def _one_sided(out, findings: list[Finding]) -> None:
    out(f"== One side only, without an entry: {len(findings)}")
    by_item: dict[tuple[str, str | None], list[Finding]] = {}
    for finding in findings:
        by_item.setdefault((finding.rust_item, finding.python_item), []).append(finding)
    for (rust_item, python_item), found in by_item.items():
        out(f"  {rust_item} / {python_item}")
        width = max(len(f.path) for f in found)
        for finding in found:
            out(f"    {finding.side:<6} {finding.path:<{width}}  {finding.detail}")
    out("")


def _scopes(out, report: Report) -> None:
    """The [[missing]] entries by scope, the text side first, and their
    total: the entries, and the Python names they cover, a dead one not
    counted. The last lines of the report."""
    out("== [[missing]] by scope: the Python names Rust lacks")
    missing = report.mapping.of("missing")
    for scope in (*SCOPES, None):
        entries = missing if scope is None else [entry for entry in missing if entry.scope == scope]
        names = sum(len({finding.path for finding in entry.covered}) for entry in entries)
        out(f"  {scope or 'total'}: {len(entries)} entries, {names} names")
    out("")


def _pending(out, report: Report) -> None:
    out(f"== Pending, by lot: {sum(len(entries) for entries in report.pending.values())} entries")
    for lot in LOTS:
        entries = report.pending.get(lot, [])
        if not entries:
            continue
        out(f"  lot {lot}: {len(entries)} entries, {sum(len(e.covered) for e in entries)} findings")
        for entry in entries:
            named = entry.members or entry.params or []
            out(f"    {entry.rust or entry.python}" + (f": {', '.join(named)}" if named else ""))
            out(f"      {entry.reason}")
    out("")


def as_json(report: Report) -> dict:
    def finding(f: Finding) -> dict:
        return {"side": f.side, "path": f.path, "detail": f.detail, "rust_item": f.rust_item, "python_item": f.python_item}

    return {
        "commit": report.python.commit,
        "items": [
            {"path": r.path, "kind": r.kind, "python": r.python, "status": r.status, "findings": [finding(f) for f in r.findings]}
            for r in report.items.values()
        ],
        "uncovered": [finding(f) for f in report.uncovered],
        "dead": [{"entry": d.entry, "path": d.path, "why": d.why} for d in report.dead],
        "moved": [entry.name for entry in report.moved],
        "due": [entry.name for entry in report.due],
        "pending": {
            lot: [{"entry": e.name, "reason": e.reason, "covered": [finding(f) for f in e.covered]} for e in entries]
            for lot, entries in report.pending.items()
        },
    }
