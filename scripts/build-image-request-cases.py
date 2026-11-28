"""Freeze Python's refusals of image requests for roomkit_ai::image.

Python tests `ImageCapabilities.validate_request` only through its vendors, so
the cases are drawn here: every model of the OpenAI, Gemini and Meta image
catalogs, its capabilities as Python's catalog states them, and a model of no catalog
that takes every control, each asked a seeded sweep of requests (sizes, counts, references, a mask, options, some of
them out of their field's bounds or of their Literal). Each case keeps what
Python answers: accepted, refused with its message, or refused when its
options are built (pydantic's `ValidationError`, which Rust's options cannot
raise when they are written: its check refuses them instead).

Run with the reference checkout's Python, on origin/main:
  PYTHONPATH=<roomkit>/src <roomkit>/.venv/bin/python scripts/build-image-request-cases.py <roomkit>
"""

from __future__ import annotations

import json
import random
import subprocess
import sys
from datetime import date
from pathlib import Path

from pydantic import ValidationError

from roomkit.providers.gemini import image_models as gemini
from roomkit.providers.image import ImageCapabilities, ImageModelInfo, ImageOptions
from roomkit.providers.meta import image_models as meta
from roomkit.providers.openai import image_models as openai

SEED = 2218
DRAWN = 40

SIZES = [None, "", "auto", "1024x1024", "1536x1024", "1024X1536", "2048x2048", "3840x2160",
         "3856x2176", "4096x1024", "3072x1024", "3584x1024", "1032x1024", "3840x2176", "1024x640",
         "1024x624", "1000x1000", "16x16", "512x512", "10x20x30", "abcx10", " 1024x1024"]
COUNTS = [0, 1, 2, 4, 10, 11]
REFERENCES = [0, 1, 3, 16, 17]
VALUES = {
    "quality": ["auto", "low", "medium", "high", "xhigh", "max", "ultra"],
    "background": ["auto", "opaque", "transparent"],
    "output_format": ["png", "jpeg", "webp", "gif"],
    "output_compression": [0, 50, 100, 101],
    "moderation": ["auto", "low"],
    "input_fidelity": ["low", "high"],
    "aspect_ratio": ["1:1", "16:9", "21:9", "5:4", ""],
    "image_size": ["512", "1K", "2K", "4K", "8K"],
    "thinking_level": ["minimal", "medium", "high"],
    "previous_interaction_id": ["int_1", ""],
    "store": [True, False],
    "search_types": [[], ["web_search"], ["image_search"], ["web_search", "image_search"]],
    "partial_images": [0, 3, 4],
}


# A model of no catalog that takes every control, so that the refusals the
# catalogs' models stop before (a search, a continuity, a value of a control
# they take) are reached; no bound on the count.
SYNTHETIC = ImageModelInfo(
    id="synthetic-every-control",
    image=ImageCapabilities(
        options=sorted(VALUES),
        qualities=["low", "high"],
        formats=["png", "webp"],
        backgrounds=["auto", "opaque"],
        aspect_ratios=["1:1"],
        image_sizes=["1K"],
        thinking_levels=["minimal"],
        flexible_size=True,
        max_images=None,
        max_references=2,
        mask=True,
        search_types=["web_search"],
        streaming=True,
        retirement_date=date(2027, 1, 31),
        verified=date(2026, 11, 28),
    ),
)


def models() -> list[ImageModelInfo]:
    found = [*openai.MODELS, *gemini.MODELS, *meta.MODELS]
    return [model for model in found if isinstance(model, ImageModelInfo)] + [SYNTHETIC]


def options_drawn(rng: random.Random) -> dict:
    fields = rng.sample(sorted(VALUES), rng.choice([0, 0, 1, 1, 2, 3, 4]))
    return {field: rng.choice(VALUES[field]) for field in fields}


def edges(model: ImageModelInfo) -> list[dict]:
    """The cases each refusal is first for, on this model."""
    image = model.image
    taken = list(image.options)
    one = {field: VALUES[field][0] for field in taken if field in VALUES}
    return [
        {"options": {}, "size": None, "n": 1, "references": 0, "mask": False},
        {"options": {}, "size": None, "n": 0, "references": 0, "mask": False},
        {"options": {}, "size": None, "n": (image.max_images or 10) + 1, "references": 0, "mask": False},
        {"options": {}, "size": None, "n": 1, "references": image.max_references + 1, "mask": False},
        {"options": {}, "size": None, "n": 1, "references": 0, "mask": True},
        {"options": {}, "size": None, "n": 1, "references": 1, "mask": True},
        {"options": one, "size": None, "n": 1, "references": 0, "mask": False},
        {"options": {"background": "transparent", "output_format": "jpeg"}, "size": None, "n": 1, "references": 0, "mask": False},
        {"options": {"output_compression": 50}, "size": None, "n": 1, "references": 0, "mask": False},
        {"options": {"output_compression": 50, "output_format": "webp"}, "size": None, "n": 1, "references": 0, "mask": False},
        {"options": {"aspect_ratio": "16:9"}, "size": "1024x1024", "n": 1, "references": 0, "mask": False},
        {"options": {"previous_interaction_id": "int_1"}, "size": None, "n": 1, "references": 0, "mask": False},
        {"options": {"search_types": ["web_search"]}, "size": None, "n": 1, "references": 0, "mask": False},
        *({"options": {}, "size": size, "n": 1, "references": 0, "mask": False} for size in SIZES),
    ]


def answered(model: ImageModelInfo, case: dict) -> dict:
    try:
        options = ImageOptions(**case["options"])
    except ValidationError:
        return {"refused": "built"}
    try:
        model.image.validate_request(
            options, size=case["size"], n=case["n"], references=case["references"], mask=case["mask"]
        )
    except ValueError as refused:
        return {"refused": str(refused)}
    return {"accepted": True}


def main() -> None:
    source = Path(sys.argv[1]).resolve()
    rng = random.Random(SEED)
    profiles, cases = [], []
    for model in models():
        profiles.append({"model": model.id, "image": model.image.model_dump(mode="json")})
        drawn = [
            {
                "options": options_drawn(rng),
                "size": rng.choice(SIZES),
                "n": rng.choice(COUNTS),
                "references": rng.choice(REFERENCES),
                "mask": rng.random() < 0.15,
            }
            for _ in range(DRAWN * (8 if model is SYNTHETIC else 1))
        ]
        for case in [*edges(model), *drawn]:
            cases.append({"profile": len(profiles) - 1, **case, **answered(model, case)})
    output = Path(__file__).resolve().parents[1] / "crates/roomkit-ai/tests/image/request_cases.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "source": "roomkit.providers.image.ImageCapabilities.validate_request, on the OpenAI, Gemini and Meta image catalogs and a synthetic model",
        "commit": subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip(),
        "scope": f"each model: the edge cases of each refusal, then {DRAWN} drawn requests, {DRAWN * 8} for the synthetic one (seed {SEED})",
        "profiles": profiles,
        "cases": cases,
    }
    # One profile, one case per line: a regenerated file's diff names the
    # cases that moved.
    lines = [json.dumps(item, ensure_ascii=False) for item in data.pop("profiles")]
    rows = [json.dumps(item, ensure_ascii=False) for item in data.pop("cases")]
    head = json.dumps(data, ensure_ascii=False, indent=1)[:-2]
    output.write_text(
        head + ',\n "profiles": [\n  ' + ",\n  ".join(lines) + '\n ],\n "cases": [\n  '
        + ",\n  ".join(rows) + "\n ]\n}\n"
    )
    refused = sum("refused" in case for case in cases)
    print(f"{len(cases)} image requests on {len(profiles)} models written to {output}: {refused} refused")


if __name__ == "__main__":
    main()
