"""Verify the documented PostgreSQL pin from an independent Cargo workspace."""

from __future__ import annotations

import json
import re
import subprocess
import tempfile
import tomllib
from pathlib import Path


def main() -> None:
    sdk = Path(__file__).resolve().parents[1]
    readme = (sdk / "README.md").read_text()
    snippets = re.findall(r"```toml\n(.*?)\n```", readme, re.DOTALL)
    (example,) = [block for block in snippets if "[patch.crates-io]" in block]
    workspace_manifest = tomllib.loads((sdk / "Cargo.toml").read_text())
    pin = workspace_manifest["workspace"]["dependencies"]["tokio-postgres"]
    expected = f"git+{pin['git']}?rev={pin['rev']}#{pin['rev']}"
    with tempfile.TemporaryDirectory(prefix="roomkit-postgres-consumer-") as workspace:
        root = Path(workspace)
        (root / "src").mkdir()
        (root / "src/lib.rs").touch()
        postgres_store = json.dumps(str(sdk / "crates/roomkit-postgres"))
        (root / "Cargo.toml").write_text(f"""[workspace]
[package]
name = "store-consumer"
version = "0.1.0"
edition = "2024"

{example}

[dependencies.roomkit-postgres]
path = {postgres_store}
[dependencies.postgres-types]
version = "0.2"
[dependencies.postgres-protocol]
version = "0.6"
""")
        output = subprocess.check_output(
            [
                "cargo",
                "metadata",
                "--format-version=1",
                "--manifest-path",
                str(root / "Cargo.toml"),
            ],
            cwd=root,
        )
    packages = json.loads(output)["packages"]
    for name in ("tokio-postgres", "postgres-types", "postgres-protocol"):
        sources = [pkg["source"] for pkg in packages if pkg["name"] == name]
        if sources != [expected]:
            raise SystemExit(f"{name} must resolve only to {expected}, got {sources}")
    print(f"PostgreSQL consumer resolves the driver and SQL types at {pin['rev']}")


if __name__ == "__main__":
    main()
