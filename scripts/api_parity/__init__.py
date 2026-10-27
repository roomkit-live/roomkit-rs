"""The public API parity check between roomkit-rs and roomkit (step 5).

`scripts/api-parity.py` is the command; this package holds its parts:
- `sides`: the Rust surface `scripts/rs-surface.py` writes, and Python's
  pinned snapshot;
- `mapping`: `api/mapping.toml`, its rules and its entries;
- `compare`: each Rust item against its Python counterpart, and what the
  entries cover;
- `report`: the report, as text and as JSON.
"""


class ParityError(Exception):
    """An input the check cannot read whole."""
