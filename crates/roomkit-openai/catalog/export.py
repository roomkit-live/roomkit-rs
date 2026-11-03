"""Print the Python implementation's OpenAI model catalogue as JSON.

Run by `make models-sync` and `make models-check` with the interpreter of a
roomkit checkout (`ROOMKIT_DIR/.venv/bin/python -I`), on the `src` directory
of one roomkit commit, extracted apart: that commit's
`OpenAIAIProvider.available_models()`, each entry as `ModelInfo.model_dump`
writes it in JSON mode, is what roomkit-openai embeds (catalog/models.json).

Usage: export.py <roomkit src directory>
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: export.py <roomkit src directory>", file=sys.stderr)
        return 2
    source = Path(argv[1]).resolve()
    # The commit's own source, ahead of the checkout's editable install.
    sys.path.insert(0, str(source))
    import roomkit
    from roomkit.providers.openai.ai import OpenAIAIProvider

    if not Path(roomkit.__file__).resolve().is_relative_to(source):
        print(f"roomkit was imported from {roomkit.__file__}, not {source}", file=sys.stderr)
        return 1
    models = [model.model_dump(mode="json") for model in OpenAIAIProvider.available_models()]
    sys.stdout.write(json.dumps(models, indent=2, ensure_ascii=False) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
