#!/usr/bin/env python3
"""Reads roomkit-rs's public surface from rustdoc's HTML, as JSON.

`make doc` builds the HTML on the pinned stable toolchain. For each crate in
scope this reads `target/doc/<crate>/all.html`, then each item's page: its
fields, variants, inherent methods and associated constants, a trait's
required and provided methods, with their signatures, and the traits an item
implements. `scripts/api-parity.py` compares the result with the Python
implementation's snapshot (docs/steps/step-05.md, lot 0).

The scope is `roomkit`, `roomkit-ai`, `roomkit-openai`, `roomkit-sqlite` and
`roomkit-mcp`. `roomkit-core` enters only through what `roomkit` re-exports,
which rustdoc inlines into `roomkit`'s pages. An item or member hidden by
`#[doc(hidden)]` is on no page, and so not in the surface.

The reader fails closed. A missing page, a kind of item or a page section it
does not know, a declaration whose members disagree with the page's entries,
a header it cannot parse, a re-export rustdoc did not inline, or a crate with
no item is an error, never a smaller surface. rustdoc's templates change
between releases: a toolchain bump that breaks the reader fails here, and its
fix lands with the bump.

Usage: python3 scripts/rs-surface.py [--doc target/doc] [--crate NAME]... > surface.json
"""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
from pathlib import Path
from typing import TextIO

CRATES = ("roomkit", "roomkit_ai", "roomkit_openai", "roomkit_sqlite", "roomkit_mcp")

# The sections of all.html, and the heading each kind's page opens with.
KINDS = {
    "structs": ("struct", "Struct"),
    "enums": ("enum", "Enum"),
    "traits": ("trait", "Trait"),
    "functions": ("fn", "Function"),
    "types": ("type", "Type Alias"),
    "constants": ("constant", "Constant"),
}

# The structural sections an item's page may hold. Another one (methods
# reached through `Deref`, for one) would carry members this reader does not
# read, so it is refused.
SECTIONS = {
    "fields",
    "variants",
    "implementations",
    "trait-implementations",
    "synthetic-implementations",
    "blanket-implementations",
    "required-associated-types",
    "provided-associated-types",
    "required-associated-consts",
    "provided-associated-consts",
    "required-methods",
    "provided-methods",
    "dyn-compatibility",
    "implementors",
    "synthetic-implementors",
    "foreign-impls",
    "aliased-type",
}

_TAG = re.compile(r"<[^>]+>")
_NOTABLE = re.compile(r'<span class="notable-traits".*?</span>', re.S)
_SUMMARY = re.compile(r"<summary[^>]*>.*?</summary>", re.S)
_ATTRIBUTE = re.compile(r'<div class="code-attribute">(.*?)</div>', re.S)
_COMMENT = re.compile(r'<span class="comment">.*?</span>', re.S)
_SECTION_HEADER = re.compile(r'<h2 id="([^"]+)" class="[^"]*\bsection-header\b[^"]*">')
# Any tag holding a structural header's class, whatever its attributes' order.
_ANY_SECTION_HEADER = re.compile(r"<h2\b[^>]*\bclass\s*=\s*[\"'][^\"']*\bsection-header\b")
_SELF = re.compile(r"^(?:&(?:'\w+\s+)?(?:mut\s+)?|mut\s+)?self(?:\s*:.*)?$", re.S)
_FN_HEAD = re.compile(
    r"^(?P<quals>(?:(?:pub(?:\([^)]*\))?|const|async|unsafe|extern\s+\"[^\"]*\")\s+)*)"
    r"fn\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)"
)


class SurfaceError(Exception):
    """A page the reader cannot read whole."""


def text(fragment: str) -> str:
    """The text of an HTML fragment on one line, as rustdoc renders it."""
    fragment = _NOTABLE.sub("", fragment).replace("<wbr>", "")
    fragment = re.sub(r"<(?:div|/div|br)\b[^>]*>", " ", fragment)
    return re.sub(r"\s+", " ", html.unescape(_TAG.sub("", fragment))).strip()


def code_text(fragment: str) -> str:
    """The text of a declaration, its lines kept, with neither the toggle's
    summary nor attributes nor comments."""
    fragment = _SUMMARY.sub("", _ATTRIBUTE.sub("", fragment))
    fragment = _COMMENT.sub("", fragment).replace("<wbr>", "")
    return html.unescape(_TAG.sub("", fragment))


def _closing(source: str, start: int) -> int:
    """The index just past the bracket matching the one at `start`; `->` is
    an arrow, not a bracket."""
    depth = 0
    for index in range(start, len(source)):
        char = source[index]
        if char in "<([{":
            depth += 1
        elif char in ">)]}" and not (char == ">" and index > 0 and source[index - 1] == "-"):
            depth -= 1
            if depth == 0:
                return index + 1
    raise SurfaceError(f"unbalanced brackets in {source!r}")


def split_top(source: str, separator: str) -> list[str]:
    """`source` cut at each `separator` outside brackets, the pieces stripped
    and the empty ones dropped."""
    pieces, depth, start = [], 0, 0
    for index, char in enumerate(source):
        if char in "<([{":
            depth += 1
        elif char in ">)]}" and not (char == ">" and index > 0 and source[index - 1] == "-"):
            depth -= 1
        elif char == separator and depth == 0:
            pieces.append(source[start:index])
            start = index + 1
    pieces.append(source[start:])
    return [piece.strip() for piece in pieces if piece.strip()]


def parse_fn(header: str) -> dict:
    """A function's header, `pub async fn name<G>(params) -> R where …`."""
    header = re.sub(r"\s+", " ", header).strip()
    head = _FN_HEAD.match(header)
    if head is None:
        raise SurfaceError(f"not a function header: {header!r}")
    rest = header[head.end() :]
    if rest.startswith("<"):
        rest = rest[_closing(rest, 0) :]
    rest = rest.lstrip()
    if not rest.startswith("("):
        raise SurfaceError(f"no parameter list in {header!r}")
    end = _closing(rest, 0)
    receiver, params = _parameters(rest[1 : end - 1], header)
    tail = rest[end:].strip()
    where = None
    if " where " in f" {tail} ":
        tail, _, where = f" {tail} ".partition(" where ")
        tail, where = tail.strip(), where.strip()
    returns = tail[2:].strip() if tail.startswith("->") else None
    if tail and returns is None:
        raise SurfaceError(f"what follows the parameters is not read: {header!r}")
    quals = head.group("quals")
    is_async = "async" in quals.split() or (returns or "").startswith("impl Future<")
    return {
        "name": head.group("name"),
        "async": is_async,
        "receiver": receiver,
        "params": params,
        "returns": returns,
        "where": where,
    }


def _parameters(inside: str, header: str) -> tuple[str | None, list[dict]]:
    """The receiver and the named parameters between a header's parentheses."""
    receiver, params = None, []
    for piece in split_top(inside, ","):
        if _SELF.match(piece):
            receiver = piece
            continue
        parts = split_top(piece, ":")
        if len(parts) < 2:
            raise SurfaceError(f"a parameter without a type in {header!r}: {piece!r}")
        name = re.sub(r"^mut\s+", "", parts[0])
        params.append({"name": name, "type": piece[piece.index(":") + 1 :].strip()})
    return receiver, params


def anchors(fragment: str, prefix: str) -> int:
    """How many tags carry an id matching the pattern `prefix`, whatever
    their attributes' order or quotes: what a block of the page holds,
    against which what the reader read is counted."""
    return len(re.findall(rf"<\w+\b[^>]*?\bid\s*=\s*[\"']{prefix}", fragment))


def _page(doc: Path, crate: str, href: str) -> str:
    path = doc / crate / href
    try:
        return path.read_text(encoding="utf-8")
    except OSError as error:
        raise SurfaceError(f"{crate}/{href}: cannot read the page ({error.strerror})") from error


def _main_content(page: str, where: str) -> str:
    start = page.find('<section id="main-content"')
    if start < 0:
        raise SurfaceError(f"{where}: no main content")
    return page[start:]


def read_items(doc: Path, crate: str) -> list[tuple[str, str, str]]:
    """The (kind, name, href) of each item all.html lists."""
    where = f"{crate}/all.html"
    page = _main_content(_page(doc, crate, "all.html"), where)
    found: list[tuple[str, str, str]] = []
    sections = list(re.finditer(r'<h3 id="([^"]+)">[^<]*</h3>(?:<ul class="all-items">(.*?)</ul>)?', page, re.S))
    headings = len(re.findall(r"<h3\b", page))
    if headings != len(sections):
        raise SurfaceError(f"{where}: {headings} headings, of which {len(sections)} read")
    for section in sections:
        if section.group(1) not in KINDS:
            raise SurfaceError(f"{where}: a section of items it does not know: {section.group(1)}")
        entries = re.findall(r"<li>(.*?)</li>", section.group(2) or "", re.S)
        links = [re.fullmatch(r'<a href="([^"]+)">([^<]+)</a>', entry) for entry in entries]
        if not entries or None in links:
            raise SurfaceError(f"{where}: an entry of {section.group(1)} it cannot read")
        kind = KINDS[section.group(1)][0]
        found += [(kind, html.unescape(link.group(2)), link.group(1)) for link in links]
    if not found:
        raise SurfaceError(f"{where}: no item")
    return found


def _sections(body: str, where: str) -> dict[str, str]:
    """Each structural section of an item's page, by its id."""
    heads = list(_SECTION_HEADER.finditer(body))
    written = len(_ANY_SECTION_HEADER.findall(body))
    if written != len(heads):
        raise SurfaceError(f"{where}: {written} section headers, of which {len(heads)} read")
    found = {}
    for index, head in enumerate(heads):
        name = head.group(1)
        if name not in SECTIONS:
            raise SurfaceError(f"{where}: a section it does not know: {name}")
        end = heads[index + 1].start() if index + 1 < len(heads) else len(body)
        found[name] = body[head.end() : end]
    return found


def _declaration(body: str, where: str) -> tuple[str, str]:
    """The declaration's HTML and its text."""
    first = _SECTION_HEADER.search(body)
    head = body[: first.start()] if first else body
    decl = re.search(r'<pre class="rust item-decl"><code>(.*?)</code></pre>', head, re.S)
    if decl is None:
        raise SurfaceError(f"{where}: no declaration")
    return decl.group(1), code_text(decl.group(1))


def _body(decl: str, opening: str) -> str:
    """What the declaration holds between its outer `opening` bracket and the
    one closing it, or "" when it has none."""
    start = decl.find(opening)
    if start < 0:
        return ""
    return decl[start + 1 : _closing(decl, start) - 1]


def _code_entries(fragment: str, prefix: str, classes: str) -> list[dict]:
    """The `name: type` entries of the spans whose id starts with `prefix`
    and whose class is `classes`."""
    pattern = rf'<span id="{re.escape(prefix)}([^"]+)" class="{classes}">(?:(?!</span>).)*?<code>(.*?)</code></span>'
    return [
        {"name": name.removeprefix("r#"), "type": re.sub(r"^(?:r#)?\w+:\s*", "", text(code))}
        for name, code in re.findall(pattern, fragment, re.S)
    ]


def _fields(sections: dict[str, str], decl: str, where: str) -> list[dict]:
    fields = _code_entries(sections.get("fields", ""), "structfield.", "structfield section-header")
    spans = anchors(sections.get("fields", ""), r"structfield\.")
    named = sum(1 for entry in split_top(_body(decl, "{"), ",") if entry.startswith("pub "))
    tuple_ = sum(1 for entry in split_top(_body(decl, "("), ",") if entry.startswith("pub ")) if "{" not in decl else 0
    if not (len(fields) == spans == named + tuple_):
        raise SurfaceError(
            f"{where}: the declaration lists {named + tuple_} public fields, the page {spans} entries, "
            f"of which {len(fields)} read"
        )
    return fields


def _variants(sections: dict[str, str], decl: str, where: str) -> list[dict]:
    block = sections.get("variants", "")
    variants = []
    for match in re.finditer(r'<section id="variant\.([^"]+)" class="variant">.*?<h3 class="code-header">(.*?)</h3>', block, re.S):
        name = match.group(1)
        fields = _code_entries(block, f"variant.{name}.field.", "section-header")
        written = anchors(block, rf"variant\.{re.escape(name)}\.field\.")
        if written != len(fields):
            raise SurfaceError(f"{where}: variant {name}: {written} fields, of which {len(fields)} read")
        variants.append({"name": name, "decl": text(match.group(2)), "fields": fields})
    sections_count = anchors(block, r"variant\.[^.\"']+[\"']")
    listed = len(split_top(_body(decl, "{"), ","))
    if not (len(variants) == sections_count == listed):
        raise SurfaceError(
            f"{where}: the declaration lists {listed} variants, the page {sections_count} entries, "
            f"of which {len(variants)} read"
        )
    return variants


def _members(block: str, where: str) -> tuple[list[dict], list[dict], list[dict]]:
    """The methods, associated constants and associated types a block of the
    page lists, a trait's implementations left out."""
    methods, constants, types = [], [], []
    entries = re.findall(r'<section id="((?:method|tymethod|associatedconstant|associatedtype)\.[^"]+)" class="([^"]*)"', block)
    for anchor, classes in entries:
        if "trait-impl" in classes.split():
            continue
        section = re.search(rf'<section id="{re.escape(anchor)}" class="[^"]*">(.*?)</section>', block, re.S)
        found = re.search(r'<h4 class="code-header">(.*?)</h4>', section.group(1), re.S) if section else None
        if found is None:
            raise SurfaceError(f"{where}: no header for {anchor}")
        header = text(found.group(1))
        kind, _, name = anchor.partition(".")
        if kind in ("method", "tymethod"):
            parsed = _parse_member(header, where, anchor)
            parsed["links"] = linked_types(found.group(1))
            methods.append(parsed)
        elif kind == "associatedconstant":
            const = re.fullmatch(r"(?:pub\s+)?const\s+(\w+)\s*:\s*(.+?)(?:\s*=\s*.*)?", header)
            if const is None:
                raise SurfaceError(f"{where}: {anchor}: not a constant: {header!r}")
            constants.append({"name": const.group(1), "type": const.group(2)})
        else:
            if not re.fullmatch(rf"type\s+{re.escape(name)}\b.*", header):
                raise SurfaceError(f"{where}: {anchor}: not a type: {header!r}")
            types.append({"name": name})
    written = anchors(block, r"(?:method|tymethod|associatedconstant|associatedtype)\.")
    if written != len(entries):
        raise SurfaceError(f"{where}: {written} member entries, of which {len(entries)} read")
    return methods, constants, types


def linked_types(header_html: str) -> dict[str, str]:
    """The path of each type a header links to, by the name it shows:
    rustdoc's link title (`struct roomkit::store::CommitEventOptions`).
    Two items of one name in two modules are told apart by it, which the
    header's text cannot do."""
    links = re.findall(
        r'<a class="(?:struct|enum|trait|type|union)" href="[^"]*" title="\w+ ([\w:]+)">(\w+)</a>',
        header_html,
    )
    return {name: path for path, name in links}


def _parse_member(header: str, where: str, anchor: str) -> dict:
    try:
        parsed = parse_fn(header)
    except SurfaceError as error:
        raise SurfaceError(f"{where}: {anchor}: {error}") from error
    if parsed["name"] != anchor.partition(".")[2].rsplit("-", 1)[0]:
        raise SurfaceError(f"{where}: {anchor} names {parsed['name']}")
    return parsed


def _trait_members(sections: dict[str, str], decl: str, where: str) -> dict:
    required, _, required_types = _members(sections.get("required-methods", ""), where)
    provided, _, provided_types = _members(sections.get("provided-methods", ""), where)
    _, _, types = _members(sections.get("required-associated-types", "") + sections.get("provided-associated-types", ""), where)
    _, constants, _ = _members(sections.get("required-associated-consts", "") + sections.get("provided-associated-consts", ""), where)
    body = _body(decl, "{")
    listed = len(re.findall(r"(?m)^\s*(?:(?:async|unsafe|const)\s+)*fn\s+\w", body))
    if listed != len(required) + len(provided):
        raise SurfaceError(
            f"{where}: the declaration lists {listed} trait methods, the page {len(required) + len(provided)}"
        )
    listed_types = len(re.findall(r"(?m)^\s*type\s+\w", body))
    if listed_types != len(types) or required_types or provided_types:
        raise SurfaceError(f"{where}: the declaration lists {listed_types} associated types, the page {len(types)}")
    return {"required": required, "provided": provided, "types": types, "constants": constants}


def _impls(sections: dict[str, str], where: str) -> list[str]:
    """The traits an item implements itself, auto and blanket ones left out."""
    block = sections.get("trait-implementations", "")
    headers = re.findall(r'<section id="impl-[^"]+" class="impl">.*?<h3 class="code-header">(.*?)</h3>', block, re.S)
    written = anchors(block, r"impl-")
    if written != len(headers):
        raise SurfaceError(f"{where}: {written} implementation entries, of which {len(headers)} read")
    found = []
    for header in headers:
        line = text(header).removeprefix("unsafe ").removeprefix("impl")
        if line.startswith("<"):
            line = line[_closing(line, 0) :]
        found.append(line.strip().partition(" for ")[0].removeprefix("!"))
    return found


def read_item(doc: Path, crate: str, kind: str, name: str, href: str) -> dict:
    where = f"{crate}/{href}"
    body = _main_content(_page(doc, crate, href), where)
    heading = next(title for key, (item_kind, title) in KINDS.items() if item_kind == kind)
    shown = re.search(r"<h1>([A-Za-z ]+?) <span", body)
    if shown is None or shown.group(1) != heading:
        raise SurfaceError(f"{where}: the page is a {shown.group(1) if shown else 'page'}, not a {heading}")
    sections = _sections(body, where)
    decl_html, decl = _declaration(body, where)
    item: dict = {
        "path": f"{crate}::{name}",
        "kind": kind,
        "decl": re.sub(r"\s+", " ", decl).strip(),
        "non_exhaustive": "#[non_exhaustive]" in html.unescape(decl_html),
    }
    if kind in ("fn", "type", "constant"):
        item.update(_declared(kind, decl, item["decl"], where))
        return item
    methods, constants, _ = _members(sections.get("implementations", ""), where)
    item.update(methods=methods, constants=constants, impls=_impls(sections, where))
    if kind == "struct":
        item["fields"] = _fields(sections, decl, where)
    elif kind == "enum":
        item["variants"] = _variants(sections, decl, where)
    else:
        item.update(_trait_members(sections, decl, where))
    return item


def _declared(kind: str, decl: str, line: str, where: str) -> dict:
    """What a function's, a type alias's or a constant's declaration says."""
    if kind == "fn":
        return {"signature": parse_fn(decl)}
    if kind == "type":
        aliased = re.fullmatch(r"pub type \w+(?:<.*?>)?\s*=\s*(.+);", line)
        if aliased is None:
            raise SurfaceError(f"{where}: not a type alias: {line!r}")
        return {"aliased": aliased.group(1)}
    constant = re.fullmatch(r"pub const \w+\s*:\s*(.+?)(?:\s*=\s*.+?)?;(?:\s*//.*)?", line)
    if constant is None:
        raise SurfaceError(f"{where}: not a constant: {line!r}")
    return {"type": constant.group(1)}


def _refuse_reexports(doc: Path, crate: str) -> None:
    """A re-export rustdoc did not inline lists its item on no page of the
    crate: its members would be missing from the surface."""
    for index in sorted((doc / crate).rglob("index.html")):
        if 'id="reexports"' in index.read_text(encoding="utf-8"):
            raise SurfaceError(f"{index.relative_to(doc)}: a re-export rustdoc did not inline")


def read_crate(doc: Path, crate: str) -> list[dict]:
    """Every public item of `crate`, with its members."""
    items = read_items(doc, crate)
    _refuse_reexports(doc, crate)
    return [read_item(doc, crate, kind, name, href) for kind, name, href in items]


def rustdoc_version(doc: Path, crate: str) -> str:
    found = re.search(r'data-rustdoc-version="([^"]+)"', _page(doc, crate, "all.html"))
    if found is None:
        raise SurfaceError(f"{crate}/all.html: no rustdoc version")
    return found.group(1)


def counts(crates: dict[str, list[dict]]) -> dict[str, int]:
    """The audit's counts: items, inherent methods, associated constants,
    trait methods, fields and variants."""
    items = [item for found in crates.values() for item in found]
    return {
        "items": len(items),
        "methods": sum(len(item.get("methods", [])) for item in items),
        "associated_constants": sum(len(item.get("constants", [])) for item in items),
        "trait_methods": sum(len(item.get("required", [])) + len(item.get("provided", [])) for item in items),
        "fields": sum(len(item.get("fields", [])) for item in items),
        "variants": sum(len(item.get("variants", [])) for item in items),
    }


def main(argv: list[str] | None = None, out: TextIO = sys.stdout, log: TextIO = sys.stderr) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--doc", type=Path, default=Path("target/doc"), help="rustdoc's output directory")
    parser.add_argument("--crate", action="append", dest="crates", help="a crate to read, instead of the five in scope")
    args = parser.parse_args(argv)
    names = args.crates or list(CRATES)
    try:
        crates = {crate: read_crate(args.doc, crate) for crate in names}
        version = rustdoc_version(args.doc, names[0])
    except SurfaceError as error:
        print(f"rs-surface: {error}", file=log)
        return 1
    summary = counts(crates)
    json.dump({"rustdoc": version, "counts": summary, "crates": crates}, out, indent=1, ensure_ascii=False)
    out.write("\n")
    by_crate = ", ".join(f"{crate} {len(found)}" for crate, found in crates.items())
    print(f"rs-surface: rustdoc {version}: {summary['items']} items ({by_crate}); "
          + ", ".join(f"{count} {name.replace('_', ' ')}" for name, count in summary.items() if name != "items"),
          file=log)
    return 0


if __name__ == "__main__":
    sys.exit(main())
