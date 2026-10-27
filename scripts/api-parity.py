#!/usr/bin/env python3
"""The public API parity report: roomkit-rs against the Python implementation.

The workspace rule: a public API that differs between the two
implementations is forbidden, except where the language forces it, and each
such case is a row of the table in docs/architecture.md (docs/steps/
step-05.md, "The architecture table"). This script reads the Rust surface
`scripts/rs-surface.py` wrote, Python's snapshot pinned in `api/python/`
(`make api-sync`), and `api/mapping.toml`, and prints:

- the Rust items that no rule and no entry give a Python name;
- the members and parameters on one side only, and the parameters one side
  requires and the other defaults;
- the `[[map]]` entries whose Python spec changed since they were written;
- the dead entries: one naming what is gone, or covering nothing;
- the `[[pending]]` entries, by the lot of step 5 that removes them.

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
  default is an option;
- a struct a caller builds, an options struct or a model, is not
  `#[non_exhaustive]`, which refuses the struct update standing for Python's
  keywords outside its crate (convention 9); an enum may be;
- a type that implements a trait of the surface has the trait's methods: a
  member a Python subclass repeats from the trait's Python class, unchanged,
  is compared on the trait alone;
- a type alias or a constant named as a Python class with members is a
  finding: nothing of the class is there to compare.

What a rule does not give, an entry of `api/mapping.toml` does: `[[map]]`
names the Python counterpart, `[[exception]]` an approved Rust form,
`[[missing]]` a Python name of a class in scope that Rust lacks, and
`[[pending]]` a known difference a lot of step 5 removes. A Python class is
in scope once a Rust item maps to it. An entry naming a whole Rust item
covers that item's own finding alone, so it goes dead once the item maps; an
exception's `until` names the Python name whose arrival ends it, and the
report lists it as due once the snapshot holds that name.

The report exits 0 whatever it finds (step 5, lot 0); an input it cannot
read whole (the surface, the snapshot, its fingerprint, the mapping) exits 2.

Usage: python3 scripts/api-parity.py SURFACE [--python DIR] [--mapping FILE]
                                     [--rules-only] [--json OUT]
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
from api_parity.cover import Report, compare  # noqa: E402
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
    "compare",
    "fingerprint",
    "load_python",
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
    return 0


if __name__ == "__main__":
    sys.exit(main())
