"""Freeze Python's generated YAML gating oracle for the Rust line reader.

Run with the reference checkout's Python (with dev dependencies installed):
  PYTHONPATH=<roomkit> <roomkit>/.venv/bin/python scripts/build-skill-gating-cases.py <roomkit>

Select allowed_tools (the default) or pass requires as the second argument. The source
generator and its mutation seed are unchanged. Quoted and unquoted YAML scalar
cases supplement it, since a scalar's YAML spelling can differ from its text.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import random
import subprocess
import sys

source = Path(sys.argv[1]).resolve()
spec = importlib.util.spec_from_file_location(
    "reference", source / "tests/test_skill_frontmatter_differential.py"
)
assert spec is not None and spec.loader is not None
reference = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reference)
key = sys.argv[2] if len(sys.argv) > 2 else "allowed_tools"
assert key in ("allowed_tools", "requires")
reference.KEYS = (key,)
rng = random.Random(693)
texts = [reference._document(rng) for _ in range(3000)]
texts += [reference._mutated(rng, text) for text in list(texts)]
for scalar in ["yes", "NO", "True", "off", "012", "0x34", "1:20", "1.2", ".inf", ".NaN", "-.inf", "+.Inf", "-.5", "2026-11-15", "3d_render", "123abc*"]:
    for value in [scalar, json.dumps(scalar), f"[{scalar}]", f"[{json.dumps(scalar)}]", f"\n  - {scalar}"]:
        texts.append(f"---\nname: research\ndescription: d\n{key}: {value}\n---\n")
cases = {}
for text in texts:
    if not reference._read_as_yaml(text):
        continue
    gating = reference._gating_of(text, by_lines=False)
    if gating is not None and gating[key]:
        frontmatter = reference.split_frontmatter(text)[0]
        cases[frontmatter] = sorted(gating[key])
filename = "frontmatter_cases.json" if key == "allowed_tools" else "requirements_cases.json"
output = Path(__file__).resolve().parents[1] / "crates/roomkit-ai/tests/skills" / filename
data = {
    "source": "tests/test_skill_frontmatter_differential.py::test_generated_frontmatters_never_gate_less_on_the_line_reader",
    "commit": subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip(),
    "scope": f"{key} only; 3000 generated + 3000 mutated (seed 693), plus YAML scalar spellings; nonempty YAML-readable gates only",
    "cases": [{"frontmatter": text, "gates": gates} for text, gates in sorted(cases.items())],
}
output.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
print(f"{len(cases)} YAML gating cases written to {output}")
