"""Export Anthropic's catalogue from the supplied, pinned Python source tree."""
from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> None:
    source = Path(sys.argv[1]).resolve()
    sys.path.insert(0, str(source))
    import roomkit
    from roomkit.providers.anthropic import AnthropicAIProvider

    if not Path(roomkit.__file__).resolve().is_relative_to(source):
        raise RuntimeError("the imported roomkit is not the pinned source")
    models = [model.model_dump(mode="json") for model in AnthropicAIProvider.available_models()]
    sys.stdout.write(json.dumps(models, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
