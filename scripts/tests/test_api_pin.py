"""Tests of the Makefile's api-sync and api-check-pin.

Each test drives the targets with `make`, on a roomkit repository it builds
(four commits: one older than the exact snapshot, the exact snapshot's,
origin/main's, and one not pushed) and a copy directory of its own, so it
needs git and make but no roomkit checkout.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
SNAPSHOT = ("public_api_names.json", "public_api_signatures.json")


def _load(name: str, file: str):
    spec = importlib.util.spec_from_file_location(name, HERE.parent / file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


parity = _load("api_parity_cli", "api-parity.py")


class PinTargets(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.roomkit = self.tmp / "roomkit"
        self.api = self.tmp / "api"
        self._git("init", "-q", "-b", "main", str(self.roomkit), cwd=self.tmp)
        self.before = self._commit("before the exact snapshot", 1)
        self.since = self._commit("the exact snapshot", 2)
        self.head = self._commit("origin/main", 3)
        self._git("update-ref", "refs/remotes/origin/main", self.head)
        self._git("checkout", "-q", "-b", "local")
        self.unpushed = self._commit("not pushed", 4)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _git(self, *args: str, cwd: Path | None = None) -> str:
        done = subprocess.run(
            ["git", "-c", "user.name=test", "-c", "user.email=test@example.org", "-c", "commit.gpgsign=false", *args],
            cwd=cwd or self.roomkit,
            capture_output=True,
            text=True,
            check=True,
        )
        return done.stdout.strip()

    def _commit(self, message: str, version: int) -> str:
        tests = self.roomkit / "tests"
        tests.mkdir(exist_ok=True)
        (tests / "public_api_names.json").write_text(json.dumps({"roomkit": [f"Name{version}"]}), encoding="utf-8")
        (tests / "public_api_signatures.json").write_text(json.dumps({f"roomkit.Name{version}": {}}), encoding="utf-8")
        self._git("add", "tests")
        self._git("commit", "-q", "-m", message)
        return self._git("rev-parse", "HEAD")

    def _make(self, target: str, **variables: str) -> subprocess.CompletedProcess:
        settings = {"ROOMKIT_DIR": str(self.roomkit), "API_PYTHON": str(self.api), "API_SNAPSHOT_SINCE": self.since}
        settings.update(variables)
        env = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MFLAGS", "MAKELEVEL", "ALLOW_UNPUSHED")}
        return subprocess.run(
            ["make", "--no-print-directory", "-C", str(ROOT), target, *(f"{k}={v}" for k, v in settings.items())],
            capture_output=True,
            text=True,
            env=env,
        )

    def _out(self, done: subprocess.CompletedProcess) -> str:
        return done.stdout + done.stderr

    def test_sync_copies_a_commit_of_origin_main(self) -> None:
        done = self._make("api-sync", ROOMKIT_REF=self.head)
        self.assertEqual(done.returncode, 0, self._out(done))
        for name in SNAPSHOT:
            self.assertEqual((self.api / name).read_text(encoding="utf-8"), self._git("show", f"{self.head}:tests/{name}"))
        self.assertEqual((self.api / "ROOMKIT_COMMIT").read_text(encoding="utf-8").strip(), self.head)
        self.assertEqual((self.api / "SNAPSHOT_SHA256").read_text(encoding="utf-8").strip(), parity.fingerprint(self.api))
        checked = self._make("api-check-pin")
        self.assertEqual(checked.returncode, 0, self._out(checked))

    def test_sync_refuses_a_commit_not_on_origin_main(self) -> None:
        done = self._make("api-sync", ROOMKIT_REF=self.unpushed)
        self.assertNotEqual(done.returncode, 0)
        self.assertIn("is not on origin/main", self._out(done))
        self.assertFalse(self.api.exists())

    def test_sync_refuses_a_commit_before_the_exact_snapshot(self) -> None:
        done = self._make("api-sync", ROOMKIT_REF=self.before)
        self.assertNotEqual(done.returncode, 0, self._out(done))
        self.assertIn(self.since, self._out(done))
        self.assertFalse(self.api.exists())

    def test_check_pin_refuses_an_edited_copy_whose_fingerprint_was_rewritten(self) -> None:
        self.assertEqual(self._make("api-sync", ROOMKIT_REF=self.head).returncode, 0)
        (self.api / "public_api_names.json").write_text(json.dumps({"roomkit": ["Edited"]}), encoding="utf-8")
        (self.api / "SNAPSHOT_SHA256").write_text(parity.fingerprint(self.api) + "\n", encoding="utf-8")
        done = self._make("api-check-pin")
        self.assertNotEqual(done.returncode, 0, self._out(done))
        self.assertIn("public_api_names.json", self._out(done))

    def test_check_pin_without_a_checkout_checks_the_fingerprint_alone(self) -> None:
        self.assertEqual(self._make("api-sync", ROOMKIT_REF=self.head).returncode, 0)
        missing = str(self.tmp / "no-roomkit")
        done = self._make("api-check-pin", ROOMKIT_DIR=missing)
        self.assertEqual(done.returncode, 0, self._out(done))
        self.assertIn("skipped", self._out(done))
        (self.api / "public_api_names.json").write_text("{}", encoding="utf-8")
        self.assertNotEqual(self._make("api-check-pin", ROOMKIT_DIR=missing).returncode, 0)

    def test_a_ref_or_a_directory_is_never_run_as_a_command(self) -> None:
        ran = self.tmp / "ran"
        self._make("api-sync", ROOMKIT_REF=f"$$(touch {ran})")
        self.assertFalse(ran.exists(), "api-sync ran ROOMKIT_REF")
        self.assertEqual(self._make("api-sync", ROOMKIT_REF=self.head).returncode, 0)
        self._make("api-check-pin", ROOMKIT_DIR=f"$$(touch {ran})")
        self.assertFalse(ran.exists(), "api-check-pin ran ROOMKIT_DIR")


if __name__ == "__main__":
    unittest.main()
