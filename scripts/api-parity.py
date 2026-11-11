#!/usr/bin/env python3
"""The public API parity report: roomkit-rs against the Python implementation.

The workspace rule: a public API that differs between the two
implementations is forbidden, except where the language forces it, and each
such case is a row of the public reference in api/PYTHON_API.md. This script reads the Rust surface
`scripts/rs-surface.py` wrote, Python's snapshot pinned in `api/python/`
(`make api-sync`), and `api/mapping.toml`, and prints:

- the Rust items that no rule and no entry give a Python name;
- the members and parameters on one side only, and the parameters one side
  requires and the other defaults;
- the `[[map]]` entries whose Python spec changed since they were written;
- the dead entries: one naming what is gone, or covering nothing;
- the `[[pending]]` entries, by the lot of step 5 that removes them;
- last, the `[[missing]]` entries by scope, `text` first, and their total:
  the entries, and the Python names they cover.

The rules, which apply with no entry:
- a Rust type's name is Python's with an acronym in Rust's casing (the
  acronyms of `[rules]`); a function or a constant keeps its name; a variant
  `CamelCase` is a member `UPPER_SNAKE`, and an associated constant a member
  of its name;
- a Rust item is looked for in the Python packages its module lists in
  `[rules.packages]`, its closest listed parent's for a module not listed;
- `&self` and a return type are not compared: a `Result` stands for a raised
  error; neither is a type, which the snapshot does not record;
- a Python property is a Rust method taking only `&self`, or a field; a
  constructor's parameter is also the attribute it sets, a Rust method of
  that name taking only `&self`; a trait has no constructor, so its Python
  class's constructor parameters are such methods;
- `r#type` is `type`, and an unused parameter's leading `_` is dropped;
- a `Duration` drops the `_seconds` of a Python name;
- a model's public fields are its Python constructor's parameters, and its
  `new` takes Python's required ones; another type's `new` is Python's
  `__init__`, or `Default` for an `__init__` of no parameter;
- a parameter typed `<Method>Options` (`<Type>Options` for `new`) stands for
  the method's defaulted Python parameters, its fields for theirs; a Python
  parameter without a default is a positional Rust one, and one with a
  default is an option; a `**kwargs` stands for every field a `[[map]]`
  entry gives its name;
- a struct a caller builds, an options struct or a model, is not
  `#[non_exhaustive]`, which refuses the struct update standing for Python's
  keywords outside its crate (convention 9); an enum may be. A struct an
  `[[exception]]` covers whole, the options of a method only Rust has or a
  Rust-only record, is checked too: the exception approves its form, never
  its `#[non_exhaustive]`;
- a type that implements a trait of the surface has the trait's methods: a
  member a Python subclass repeats from the trait's Python class, unchanged,
  is compared on the trait alone;
- a type alias or a constant named as a Python class with members is a
  finding: nothing of the class is there to compare;
- a Rust type `[rules] builtins` names by its path stands for that Python
  builtin (a strong id `str`, `Timestamp` `datetime`, `Metadata` `dict`,
  a validator's error `ValueError`, a raised exception's alias
  `Exception`): its methods, associated constants and public fields are
  the `members` the rule lists, each a row of the architecture table
  mapping an operation of the builtin, any other a finding; an error's
  variants are the exception's messages. A variant `[rules] builtin_payloads` lists
  is Python's value of the builtin it carries
  (`Visibility::Channels(ChannelSet)`), no member;
- a Rust enum `[rules] literals` names by its path stands for the Python
  `Literal` of the field it types (`HookAction`, `HookResult.action`):
  the field must exist, and the variants' `snake_case` names are the
  Literal's values the rule lists, read from the Python source;
- a content's `type='text'` is the tag of the tagged union its Rust type
  is a variant of (`EventContent::Text(TextContent)`), no field, when that
  enum stands for a Python union, a name the snapshot exports and
  describes as no class, and the tag is the variant's `snake_case` name;
- an error enum (one implementing `Error`) is its Python base class, each
  variant one of its exception classes: `X` the family's `<Stem>XError`
  (`SkillError::Parse`, `SkillParseError`), or else the class `XError`,
  each in Python's acronym casing, or a builtin exception `[rules]
  builtin_errors` names by the variant's path: a Python builtin exception
  (or `asyncio`'s), on a variant of an error enum whose family is read. `[rules] error_families`
  lists, by the enum's path, the classes its variants stand for (the
  snapshot holds no bases): a variant's class must be listed, each class
  listed must have its variant, an error enum with no family is a
  finding, and so is a family no Python class's comparison reads (an enum
  with no Python base, or one a builtin stands for). A variant's fields,
  a tuple variant's payload by position (`0`), are compared with the
  class's `__init__` when the class has one, no field listed for it; a
  class that takes a message alone lists the fields its variant carries;
- a rule naming a Rust item, variant or member that is gone, or what no
  comparison reads, is a finding (`listed in [rules] ...`) no entry covers,
  a whole item's included.

Known limits: an error enum with no Python base (`StoreError`) and no
family listed has its variants read by no rule; a `Literal`'s values and a
family's classes are read from the Python source by hand, the snapshot
holding neither; a union is any name the snapshot exports and does not
describe, its variants' tags telling it from another such name.

What a rule does not give, an entry of `api/mapping.toml` does: `[[map]]`
names the Python counterpart, `[[exception]]` an approved Rust form,
`[[missing]]` a Python name of a class in scope that Rust lacks (its
`scope` the text side, media or tier 1b; a private name or a form the
language forces is refused there, an `[[exception]]`), and
`[[pending]]` a known difference a lot of step 5 removes. A Python class is
in scope once a Rust item maps to it. An entry naming a whole Rust item
covers that item's own finding alone, so it goes dead once the item maps; an
exception's `until` names the Python name whose arrival ends it, and the
report lists it as due once the snapshot holds that name.

The report exits 0 whatever it finds (step 5, lot 0), but with `--gate`,
the gate before the tag (lot 6): it exits 1 on a finding no entry covers, a
dead entry or a pending entry, and, given the defaults tests
(`--defaults FILE...`), on a Python default a Rust option stands for that
no line of them names; given the names table (`--architecture FILE`), on an
exception or a rule no row of it names. An input it cannot read whole (the surface,
the snapshot, its fingerprint, the mapping) exits 2.

Usage: python3 scripts/api-parity.py SURFACE [--python DIR] [--mapping FILE]
                                     [--rules-only] [--json OUT] [--gate]
                                     [--defaults FILE...] [--architecture FILE]
       python3 scripts/api-parity.py --spec NAME [--python DIR]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import TextIO

# The package beside this script holds the check's parts.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from api_parity import ParityError  # noqa: E402
from api_parity.cover import Report, blocking, compare, missing_defaults, unwritten  # noqa: E402
from api_parity.mapping import Mapping, Rules, load_mapping, variant_name  # noqa: E402
from api_parity.report import as_json, render  # noqa: E402
from api_parity.sides import Python, Surface, fingerprint, load_python, load_surface, spec_hash  # noqa: E402

__all__ = [
    "Mapping",
    "ParityError",
    "Python",
    "Report",
    "Rules",
    "Surface",
    "blocking",
    "compare",
    "fingerprint",
    "load_python",
    "missing_defaults",
    "unwritten",
    "spec_hash",
    "variant_name",
]


def main(argv: list[str] | None = None, out: TextIO = sys.stdout, log: TextIO = sys.stderr) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("surface", type=Path, nargs="?", help="the JSON rs-surface.py wrote")
    parser.add_argument("--python", type=Path, default=Path("api/python"), help="the pinned snapshot")
    parser.add_argument("--mapping", type=Path, default=Path("api/mapping.toml"))
    parser.add_argument("--rules-only", action="store_true", help="apply the rules, none of the entries")
    parser.add_argument("--json", type=Path, help="also write the findings as JSON there")
    parser.add_argument("--spec", metavar="NAME", help="print the py_spec of a Python name, for a [[map]] entry")
    parser.add_argument("--gate", action="store_true", help="exit 1 on what fails the gate before the tag")
    parser.add_argument(
        "--defaults", type=Path, nargs="*", default=[], help="the defaults tests, which the gate checks name each default"
    )
    parser.add_argument(
        "--architecture", type=Path, help="the names table, which the gate checks names each exception and rule"
    )
    args = parser.parse_args(argv)
    try:
        python = load_python(args.python)
        if args.spec:
            if not python.exists(args.spec):
                raise ParityError(f"{args.spec}: not in the snapshot")
            print(spec_hash(python.spec_of(args.spec)), file=out)
            return 0
        if args.surface is None:
            parser.error("the surface is required")
        report = compare(load_surface(args.surface), python, load_mapping(args.mapping, args.rules_only))
    except ParityError as error:
        print(f"api-parity: {error}", file=log)
        return 2
    out.write(render(report, args.rules_only))
    if args.json:
        args.json.write_text(json.dumps(as_json(report), indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    failing = blocking(report) if args.gate else []
    if args.gate and args.defaults:
        tests = "\n".join(path.read_text(encoding="utf-8") for path in args.defaults)
        unasserted = missing_defaults(report, tests)
        for python in unasserted:
            print(f"api-parity: no defaults line names {python}", file=log)
        if unasserted:
            failing.append(f"{len(unasserted)} default{'' if len(unasserted) == 1 else 's'} no test asserts")
    if args.gate and args.architecture:
        rowless = unwritten(report.mapping, args.architecture.read_text(encoding="utf-8"))
        for name in rowless:
            print(f"api-parity: no row of the names table names {name}", file=log)
        if rowless:
            failing.append(f"{len(rowless)} entr{'y' if len(rowless) == 1 else 'ies'} or rules with no row")
    if failing:
        print(f"api-parity: the gate fails: {'; '.join(failing)}", file=log)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
