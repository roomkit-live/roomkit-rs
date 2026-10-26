"""Writes the Unicode tables roomkit-core reads a name with (RFC §6.4, §10.1
step 12): crates/roomkit-core/src/rules/unicode_data.rs.

The register of authors tells apart names that read alike, and the Python
implementation decides what reads alike: its look-alike data
(roomkit/_lookalike_data.py, from Unicode's confusables) and the Unicode
properties its Python reads (`unicodedata`). This script reads both from a
roomkit checkout and writes them as Rust tables, so that roomkit-rs names
and ranks every author as roomkit does (gap G-089):

- what each character reads as (roomkit's `_reads()`);
- the characters a person's name keeps (roomkit's `_text._a_name_s`);
- the combining marks (general category M), the characters `str.isalnum()`
  accepts, and those a name's letters are read past (roomkit's `JOINT`);
- the opening and closing punctuation (general categories Ps and Pe), the
  brackets a copy of a runtime mark may open and close with, and what each
  character reads as in a mark's phrase (roomkit's `char_class`);
- canonical composition (NFC): each character's full canonical
  decomposition, the canonical combining classes, and the primary
  composites.

Run it with the Python roomkit is tested with, from roomkit's virtual
environment, on the commit to follow:

    ../roomkit/.venv/bin/python scripts/build-unicode-tables.py ../roomkit \
        > crates/roomkit-core/src/rules/unicode_data.rs
"""

from __future__ import annotations

import hashlib
import subprocess
import sys
import unicodedata
from pathlib import Path

HANGUL = range(0xAC00, 0xD7A4)
SURROGATES = range(0xD800, 0xE000)


def scalar_values():
    """Every Unicode scalar value, the surrogates left out."""
    return (c for c in range(0x110000) if c not in SURROGATES)


def ranges(predicate) -> list[tuple[int, int]]:
    """The inclusive ranges of scalar values `predicate` accepts."""
    found: list[tuple[int, int]] = []
    start = None
    previous = None
    for c in scalar_values():
        if predicate(chr(c)):
            if start is None or previous != c - 1:
                if start is not None:
                    found.append((start, previous))
                start = c
            previous = c
    if start is not None:
        found.append((start, previous))
    return found


def rust_char(c: int) -> str:
    return f"'\\u{{{c:x}}}'"


def rust_str(text: str) -> str:
    return '"' + "".join(f"\\u{{{ord(ch):x}}}" for ch in text) + '"'


def emit(name: str, kind: str, items: list[str], per_line: int, doc: str) -> str:
    lines = [f"/// {line}" if line else "///" for line in doc.splitlines()]
    lines.append("#[rustfmt::skip]")
    lines.append(f"pub(super) static {name}: &[{kind}] = &[")
    for start in range(0, len(items), per_line):
        lines.append("    " + " ".join(f"{item}," for item in items[start : start + per_line]))
    lines.append("];")
    return "\n".join(lines)


def phrase_reads(lookalike) -> tuple[list[tuple[str, list[str]]], list[tuple[int, int]]]:
    """Each character that reads as letters in a phrase, with every letter
    and run it reads as, and the ranges of those that may also sit between
    a phrase's words as spacing or markup.

    A character reads as what roomkit's own classes take it for, matched as
    its patterns match them, and also as the names' reading gives it (its
    lower case, each character as `_lookalike._reads()` reads it), so that
    a phrase is read at least as a name is: `|` reads as `l` there, while
    roomkit's phrase classes take it for markup."""
    import re

    letters = [c for c in "abcdefghijklmnopqrstuvwxyz0123456789"]
    classes = [
        (letter, re.compile(lookalike.char_class(letter, phrase=True), re.IGNORECASE))
        for letter in letters
    ]
    sequences = lookalike._sequences(phrase=True)
    classes += [
        (run, re.compile(pattern, re.IGNORECASE))
        for run, pattern in sorted(sequences.items())
        if run.isalnum()
    ]
    # Every character is tried against the classes themselves, never
    # against the forms they list: a class matches characters no form
    # lists, by re.IGNORECASE (ǆ and Ǆ through the titlecase ǅ that
    # FORMS['d'] holds). Their union tells the characters that match none
    # at once.
    anything = re.compile(
        "|".join(f"(?:{pattern.pattern})" for _, pattern in classes), re.IGNORECASE
    )

    reads = lookalike._reads()
    joint = lookalike._joint()
    spacing = lookalike._spacing(True)

    def as_a_name(ch: str) -> str:
        """`ch` as the names' reading reads it, its letters and digits."""
        if joint.fullmatch(ch):
            return ""
        return "".join(
            letter
            for lower in ch.lower()
            for letter in reads.get(lower, lower)
            if letter.isalnum()
        )

    found = []
    gaps = []
    for c in scalar_values():
        ch = chr(c)
        runs = (
            [run for run, pattern in classes if pattern.fullmatch(ch)]
            if anything.fullmatch(ch)
            else []
        )
        named = as_a_name(ch)
        if named and named.isascii() and named not in runs:
            runs.append(named)
        if runs:
            found.append((ch, runs))
            if spacing.fullmatch(ch) and not joint.fullmatch(ch):
                gaps.append(c)
    return found, [(c, c) for c in gaps]


def full_decomposition(ch: str) -> str:
    """`ch`'s full canonical decomposition, Hangul syllables left to the
    algorithm; `ch` itself when it has none."""
    return unicodedata.normalize("NFD", ch)


def main() -> None:
    root = Path(sys.argv[1])
    sys.path.insert(0, str(root / "src"))
    from roomkit import _lookalike, _lookalike_data, _text

    data_file = root / "src" / "roomkit" / "_lookalike_data.py"
    data_sha = hashlib.sha256(data_file.read_bytes()).hexdigest()
    commit = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "--short=8", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    reads = _lookalike._reads()
    joint = _lookalike._joint()
    parts = [HEADER.format(
        commit=commit,
        data_sha=data_sha,
        confusables=_lookalike_data.CONFUSABLES_VERSION,
        unicode=unicodedata.unidata_version,
        python=sys.version.split()[0],
    )]
    parts.append(emit(
        "READS",
        "(char, &str)",
        [f"({rust_char(ord(ch))}, {rust_str(read)})" for ch, read in sorted(reads.items())],
        4,
        "What each character reads as, sorted by character: roomkit's\n`_lookalike._reads()`.",
    ))
    phrase, gaps = phrase_reads(_lookalike)
    parts.append(emit(
        "PHRASE_READS",
        "(char, &str)",
        [f"({rust_char(ord(ch))}, {rust_str(' '.join(runs))})" for ch, runs in phrase],
        3,
        "What each character reads as in a phrase, sorted by character: the\n"
        "letters and digits whose class in roomkit's `_lookalike.char_class`\n"
        "(phrase=True, with `re.IGNORECASE`) takes it, the runs of letters\n"
        "whose class in `_lookalike._sequences` does (`vi` for `ⅵ`), and what\n"
        "the names' reading reads it as (`|` as `l`), separated by spaces. A\n"
        "copy of a runtime mark is read with it.",
    ))
    parts.append(emit(
        "PHRASE_GAPS",
        "(char, char)",
        [f"({rust_char(start)}, {rust_char(end)})" for start, end in gaps],
        4,
        "The characters that read as letters in a phrase but that roomkit's\n"
        "phrase pattern also takes for spacing or markup between words (`|`).\n"
        "Inclusive ranges, sorted.",
    ))
    for name, predicate, doc in (
        ("NAME_CHARS", _text._a_name_s, "The characters a person's name keeps: roomkit's `_text._a_name_s`."),
        ("MARKS", lambda ch: unicodedata.category(ch).startswith("M"), "The combining marks: general category M."),
        ("ALNUM", str.isalnum, "The characters Python's `str.isalnum()` accepts."),
        ("JOINT", lambda ch: joint.fullmatch(ch) is not None, "The characters a name's letters are read past: roomkit's\n`_lookalike.JOINT`."),
        ("OPENING", lambda ch: unicodedata.category(ch) == "Ps", "The opening punctuation: general category Ps, `[` and its one\nconfusable, `［`, among them."),
        ("CLOSING", lambda ch: unicodedata.category(ch) == "Pe", "The closing punctuation: general category Pe, `]` and `］` among them."),
    ):
        items = [f"({rust_char(start)}, {rust_char(end)})" for start, end in ranges(predicate)]
        parts.append(emit(name, "(char, char)", items, 4, doc + "\nInclusive ranges, sorted."))
    decompositions = []
    combining = []
    compositions = []
    for c in scalar_values():
        ch = chr(c)
        if c not in HANGUL:
            decomposed = full_decomposition(ch)
            if decomposed != ch:
                decompositions.append(f"({rust_char(c)}, {rust_str(decomposed)})")
            raw = unicodedata.decomposition(ch)
            if raw and not raw.startswith("<"):
                pair = [int(code, 16) for code in raw.split()]
                # A primary composite: a pair NFC composes back.
                if len(pair) == 2 and unicodedata.normalize("NFC", chr(pair[0]) + chr(pair[1])) == ch:
                    compositions.append((pair[0], pair[1], c))
        ccc = unicodedata.combining(ch)
        if ccc:
            combining.append(f"({rust_char(c)}, {ccc})")
    parts.append(emit(
        "DECOMPOSITIONS",
        "(char, &str)",
        decompositions,
        3,
        "Each character's full canonical decomposition, sorted by character;\nthe Hangul syllables are decomposed by their algorithm.",
    ))
    parts.append(emit(
        "COMBINING_CLASSES",
        "(char, u8)",
        combining,
        6,
        "The canonical combining class of each character that has one, sorted.",
    ))
    parts.append(emit(
        "COMPOSITIONS",
        "(char, char, char)",
        [f"({rust_char(a)}, {rust_char(b)}, {rust_char(c)})" for a, b, c in sorted(compositions)],
        3,
        "The primary composites: a starter, the character that follows it, and\nwhat they compose into, sorted by the pair; Hangul by its algorithm.",
    ))
    sys.stdout.write("\n\n".join(parts) + "\n")


HEADER = '''//! The Unicode tables a name is read with (RFC §6.4, §10.1 step 12, gap
//! G-089). Generated by `scripts/build-unicode-tables.py` from roomkit
//! {commit} (`_lookalike_data.py`, sha256 {data_sha}, from Unicode's
//! confusables.txt {confusables}) and the Unicode {unicode} data of Python
//! {python}. Do not edit; run the script again.
//!
//! The data is derived from Unicode data files, under this notice:
//!
//! UNICODE LICENSE V3
//!
//! COPYRIGHT AND PERMISSION NOTICE
//!
//! Copyright (c) 1991-2026 Unicode, Inc.
//!
//! NOTICE TO USER: Carefully read the following legal agreement. BY
//! DOWNLOADING, INSTALLING, COPYING OR OTHERWISE USING DATA FILES, AND/OR
//! SOFTWARE, YOU UNEQUIVOCALLY ACCEPT, AND AGREE TO BE BOUND BY, ALL OF THE
//! TERMS AND CONDITIONS OF THIS AGREEMENT. IF YOU DO NOT AGREE, DO NOT
//! DOWNLOAD, INSTALL, COPY, DISTRIBUTE OR USE THE DATA FILES OR SOFTWARE.
//!
//! Permission is hereby granted, free of charge, to any person obtaining a
//! copy of data files and any associated documentation (the "Data Files") or
//! software and any associated documentation (the "Software") to deal in the
//! Data Files or Software without restriction, including without limitation
//! the rights to use, copy, modify, merge, publish, distribute, and/or sell
//! copies of the Data Files or Software, and to permit persons to whom the
//! Data Files or Software are furnished to do so, provided that either (a)
//! this copyright and permission notice appear with all copies of the Data
//! Files or Software, or (b) this copyright and permission notice appear in
//! associated Documentation.
//!
//! THE DATA FILES AND SOFTWARE ARE PROVIDED "AS IS", WITHOUT WARRANTY OF ANY
//! KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
//! MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF
//! THIRD PARTY RIGHTS.
//!
//! IN NO EVENT SHALL THE COPYRIGHT HOLDER OR HOLDERS INCLUDED IN THIS NOTICE
//! BE LIABLE FOR ANY CLAIM, OR ANY SPECIAL INDIRECT OR CONSEQUENTIAL DAMAGES,
//! OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS,
//! WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION,
//! ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THE DATA
//! FILES OR SOFTWARE.
//!
//! Except as contained in this notice, the name of a copyright holder shall
//! not be used in advertising or otherwise to promote the sale, use or other
//! dealings in these Data Files or Software without prior written
//! authorization of the copyright holder.'''


if __name__ == "__main__":
    main()
