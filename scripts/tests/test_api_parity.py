"""Tests of scripts/api-parity.py: the rules, the entries and the pin.

Each test builds a small Rust surface (in rs-surface.py's form, its
signatures parsed by rs-surface.py itself), a Python snapshot in roomkit's
form, and a mapping, and reads what the report finds.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _load(name: str, file: str):
    spec = importlib.util.spec_from_file_location(name, HERE.parent / file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


surface_reader = _load("rs_surface", "rs-surface.py")
parity = _load("api_parity", "api-parity.py")

RULES = {
    "acronyms": {"Ai": "AI", "OpenAi": "OpenAI", "Mcp": "MCP", "Sqlite": "SQLite"},
    "duration_suffix": "_seconds",
    "packages": {
        "roomkit": ["roomkit"],
        "roomkit::model": ["roomkit", "roomkit.models"],
        "roomkit_ai": ["roomkit.providers.ai", "roomkit"],
    },
}


def struct(path, fields=(), methods=(), constants=(), impls=()):
    return {
        "path": path,
        "kind": "struct",
        "fields": [{"name": name, "type": type_} for name, type_ in fields],
        "methods": [surface_reader.parse_fn(header) for header in methods],
        "constants": [{"name": name, "type": "X"} for name in constants],
        "impls": list(impls),
    }


def enum(path, variants=(), methods=()):
    return {
        "path": path,
        "kind": "enum",
        "variants": [{"name": name, "decl": name, "fields": []} for name in variants],
        "methods": [surface_reader.parse_fn(header) for header in methods],
        "constants": [],
        "impls": [],
    }


def function(path, header):
    return {"path": path, "kind": "fn", "signature": surface_reader.parse_fn(header)}


def run(items, signatures, names=None, entries=None, rules_only=False):
    crates: dict[str, list] = {}
    for item in items:
        crates.setdefault(item["path"].split("::")[0], []).append(item)
    if names is None:
        names = {}
        for path in signatures:
            package, _, name = path.rpartition(".")
            names.setdefault(package, []).append(name)
    table = {"rules": RULES, **(entries or {})}
    return parity.compare(
        parity.Surface({"rustdoc": "test", "counts": {}, "crates": crates}),
        parity.Python(signatures, names),
        parity.Mapping.from_table(table, rules_only=rules_only),
    )


def found(report, side=None):
    return sorted(
        (finding.side, finding.path) for finding in report.findings if side is None or finding.side == side
    )


class Names(unittest.TestCase):
    def setUp(self) -> None:
        self.rules = parity.Rules.from_table(RULES)

    def test_acronyms_take_python_s_casing(self) -> None:
        cases = {
            "OpenAiAiProvider": "OpenAIAIProvider",
            "AiChannel": "AIChannel",
            "McpToolProvider": "MCPToolProvider",
            "SqliteStore": "SQLiteStore",
            "OpenAiConfig": "OpenAIConfig",
            "RoomKit": "RoomKit",
        }
        for rust, python in cases.items():
            with self.subTest(rust=rust):
                self.assertEqual(self.rules.python_type_name(rust), python)

    def test_a_variant_is_python_s_upper_snake_member(self) -> None:
        self.assertEqual(parity.variant_name("BeforeBroadcast"), "BEFORE_BROADCAST")
        self.assertEqual(parity.variant_name("OnAiResponse"), "ON_AI_RESPONSE")
        self.assertEqual(parity.variant_name("Sync"), "SYNC")

    def test_a_module_takes_its_closest_listed_parent_s_packages(self) -> None:
        self.assertEqual(self.rules.packages_for("roomkit::model"), ["roomkit", "roomkit.models"])
        self.assertEqual(self.rules.packages_for("roomkit::transcript"), ["roomkit"])
        self.assertEqual(self.rules.packages_for("roomkit_ai::testing"), ["roomkit.providers.ai", "roomkit"])
        with self.assertRaises(parity.ParityError):
            self.rules.packages_for("roomkit_other")


class Mapping(unittest.TestCase):
    def test_an_item_is_found_through_its_crate_s_packages(self) -> None:
        report = run(
            [struct("roomkit_ai::AiChannel"), struct("roomkit::model::RichContent")],
            {"roomkit.AIChannel": {}, "roomkit.models.RichContent": {}},
        )
        self.assertEqual(report.items["roomkit_ai::AiChannel"].python, "roomkit.AIChannel")
        self.assertEqual(report.items["roomkit::model::RichContent"].python, "roomkit.models.RichContent")

    def test_a_name_only_an_all_exports_maps_without_members(self) -> None:
        report = run(
            [enum("roomkit::model::EventContent", ["Text"])],
            {},
            names={"roomkit.models": ["EventContent"]},
        )
        self.assertEqual(report.items["roomkit::model::EventContent"].python, "roomkit.models.EventContent")
        self.assertEqual(found(report), [])

    def test_an_item_without_a_python_name_is_unmapped(self) -> None:
        report = run([struct("roomkit::OrganizationKit", methods=["pub fn organization_id(&self) -> &str"])], {})
        self.assertEqual(report.items["roomkit::OrganizationKit"].status, "unmapped")
        self.assertEqual(found(report), [("rust", "roomkit::OrganizationKit")])


class Members(unittest.TestCase):
    def test_an_options_struct_stands_for_the_defaulted_parameters(self) -> None:
        report = run(
            [
                struct(
                    "roomkit::RoomKit",
                    methods=[
                        "pub async fn send_event(&self, room_id: &RoomId, options: SendEventOptions)"
                        " -> Result<RoomEvent, KitError>"
                    ],
                ),
                struct("roomkit::SendEventOptions", fields=[("chain_depth", "u32"), ("metadata", "Metadata")]),
            ],
            {"roomkit.RoomKit": {"send_event": ["async def", "room_id", "*", "chain_depth=0", "metadata=None"]}},
        )
        self.assertEqual(found(report), [])
        self.assertEqual(report.items["roomkit::RoomKit"].status, "matches")
        self.assertEqual(report.items["roomkit::SendEventOptions"].status, "options")

    def test_parameters_on_one_side_only(self) -> None:
        report = run(
            [
                struct(
                    "roomkit::RoomKit",
                    methods=["pub async fn send_event(&self, room_id: &RoomId, extra: u8, options: SendEventOptions)"],
                ),
                struct("roomkit::SendEventOptions", fields=[("chain_depth", "u32"), ("only_rust", "bool")]),
            ],
            {"roomkit.RoomKit": {"send_event": ["async def", "room_id", "*", "chain_depth=0", "provider=None"]}},
        )
        self.assertEqual(
            found(report),
            [
                ("python", "roomkit.RoomKit.send_event(provider)"),
                ("rust", "roomkit::RoomKit::send_event(extra)"),
                ("rust", "roomkit::SendEventOptions::only_rust"),
            ],
        )
        self.assertEqual(report.items["roomkit::RoomKit"].status, "differs")

    def test_a_defaulted_python_parameter_rust_requires(self) -> None:
        report = run(
            [
                struct(
                    "roomkit::RoomKit",
                    methods=["pub async fn delete_event(&self, room_id: &RoomId, cascade_replies: bool)"],
                )
            ],
            {"roomkit.RoomKit": {"delete_event": ["async def", "room_id", "*", "cascade_replies=True"]}},
        )
        self.assertEqual(found(report), [("rust", "roomkit::RoomKit::delete_event(cascade_replies)")])
        self.assertIn("cascade_replies=True", report.findings[0].detail)

    def test_a_required_python_parameter_rust_takes_as_an_option(self) -> None:
        report = run(
            [
                struct("roomkit::RoomKit", methods=["pub fn hook(&self, options: HookOptions)"]),
                struct("roomkit::HookOptions", fields=[("trigger", "HookTrigger")]),
            ],
            {"roomkit.RoomKit": {"hook": ["def", "trigger"]}},
        )
        self.assertEqual(found(report), [("rust", "roomkit::HookOptions::trigger")])

    def test_an_options_struct_named_after_another_method_is_a_parameter(self) -> None:
        report = run(
            [
                struct("roomkit::RoomKit", methods=["pub fn post(&self, query: TimelineQuery)"]),
                struct("roomkit::TimelineQuery", fields=[("limit", "u64")]),
            ],
            {"roomkit.RoomKit": {"post": ["async def", "limit=50"]}},
        )
        self.assertIn(("rust", "roomkit::RoomKit::post(query)"), found(report))
        self.assertIn(("rust", "roomkit::TimelineQuery"), found(report))

    def test_a_duration_drops_the_seconds_suffix(self) -> None:
        report = run(
            [
                struct(
                    "roomkit_ai::AiChannel",
                    methods=["pub fn new(channel_id: ChannelId, options: AiChannelOptions) -> Self"],
                ),
                struct(
                    "roomkit_ai::AiChannelOptions",
                    fields=[("tool_timeout", "Option<Duration>"), ("loop_timeout", "u64")],
                ),
            ],
            {
                "roomkit.AIChannel": {
                    "__init__": ["def", "channel_id", "tool_timeout_seconds=30.0", "loop_timeout_seconds=300.0"]
                }
            },
        )
        self.assertEqual(
            found(report),
            [
                ("python", "roomkit.AIChannel.__init__(loop_timeout_seconds)"),
                ("rust", "roomkit_ai::AiChannelOptions::loop_timeout"),
            ],
        )

    def test_a_property_is_a_method_of_no_parameter_or_a_field(self) -> None:
        report = run(
            [
                struct(
                    "roomkit::RoomKit",
                    methods=["pub fn store(&self) -> &S", "pub fn open(&self, now: u8) -> bool"],
                ),
                struct("roomkit::model::ChannelBinding", fields=[("muted", "bool")]),
            ],
            {
                "roomkit.RoomKit": {"store": ["property"], "open": ["property"]},
                "roomkit.ChannelBinding": {"muted": ["property"]},
            },
        )
        self.assertEqual(found(report), [("rust", "roomkit::RoomKit::open")])

    def test_a_model_s_fields_are_its_constructor_s_parameters(self) -> None:
        report = run(
            [
                struct(
                    "roomkit::model::RoomEvent",
                    fields=[("id", "EventId"), ("room_id", "RoomId"), ("type", "EventType"), ("consumption", "Map")],
                    methods=["pub fn new(id: EventId, room_id: RoomId) -> Self"],
                )
            ],
            {"roomkit.RoomEvent": {"__init__": ["def", "*", "id=<factory>", "room_id", "type='message'", "status='pending'"]}},
        )
        self.assertEqual(
            found(report),
            [
                ("python", "roomkit.RoomEvent.__init__(status)"),
                ("rust", "roomkit::model::RoomEvent::consumption"),
                ("rust", "roomkit::model::RoomEvent::new(id)"),
            ],
        )

    def test_a_constructor_without_parameters_is_new_or_default(self) -> None:
        report = run(
            [struct("roomkit::InMemoryStore", methods=["pub fn new() -> Self"]), struct("roomkit::Quiet", impls=["Default"])],
            {"roomkit.InMemoryStore": {"__init__": ["def"]}, "roomkit.Quiet": {"__init__": ["def"]}},
        )
        self.assertEqual(found(report), [])

    def test_variants_and_associated_constants_are_enum_members(self) -> None:
        report = run(
            [
                enum("roomkit::HookExecution", ["Sync", "Async", "Later"]),
                struct("roomkit::model::ChannelType", constants=["SMS"], methods=["pub fn as_str(&self) -> &str"]),
            ],
            {
                "roomkit.HookExecution": {"SYNC": ["member", "'sync'"], "ASYNC": ["member", "'async'"]},
                "roomkit.ChannelType": {"SMS": ["member", "'sms'"], "VOICE": ["member", "'voice'"]},
            },
        )
        self.assertEqual(
            found(report),
            [
                ("python", "roomkit.ChannelType.VOICE"),
                ("rust", "roomkit::HookExecution::Later"),
                ("rust", "roomkit::model::ChannelType::as_str"),
            ],
        )

    def test_a_function_s_parameters(self) -> None:
        report = run(
            [function("roomkit::visibility_allows", "pub fn visibility_allows(visibility: &Visibility, binding: &ChannelBinding) -> bool")],
            {"roomkit.visibility_allows": {"__call__": ["def", "visibility", "target_binding"]}},
        )
        self.assertEqual(
            found(report),
            [("python", "roomkit.visibility_allows(target_binding)"), ("rust", "roomkit::visibility_allows(binding)")],
        )

    def test_a_subclass_s_inherited_members_are_compared_on_the_trait(self) -> None:
        trait = {
            "path": "roomkit::ConversationStore",
            "kind": "trait",
            "methods": [],
            "constants": [],
            "impls": [],
            "required": [surface_reader.parse_fn("fn get_room(&self, room_id: &RoomId) -> impl Future<Output = Option<Room>>")],
            "provided": [surface_reader.parse_fn("fn close(&self) -> impl Future<Output = ()>")],
            "types": [],
        }
        get_room = ["async def", "room_id"]
        report = run(
            [trait, struct("roomkit::InMemoryStore", methods=["pub fn new() -> Self"], impls=["ConversationStore"])],
            {
                "roomkit.ConversationStore": {"get_room": get_room, "close": ["async def"]},
                "roomkit.InMemoryStore": {
                    "__init__": ["def"],
                    "close": ["async def", "timeout=5.0"],
                    "get_room": ["async def", "id"],
                    "dump": ["def"],
                },
            },
        )
        self.assertEqual(
            found(report),
            [
                ("python", "roomkit.InMemoryStore.close(timeout)"),
                ("python", "roomkit.InMemoryStore.dump"),
                ("python", "roomkit.InMemoryStore.get_room(id)"),
                ("rust", "roomkit::ConversationStore::get_room(room_id)"),
            ],
        )

    def test_an_alias_for_a_python_class_with_members(self) -> None:
        report = run(
            [{"path": "roomkit_ai::ProviderError", "kind": "type", "aliased": "Box<dyn Error>"}],
            {"roomkit.providers.ai.ProviderError": {"__init__": ["def", "message", "*", "retryable=False"]}},
        )
        self.assertEqual(found(report), [("rust", "roomkit_ai::ProviderError")])

    def test_a_constructor_s_parameter_is_the_attribute_it_sets(self) -> None:
        trait = {
            "path": "roomkit::Channel",
            "kind": "trait",
            "methods": [],
            "constants": [],
            "impls": [],
            "required": [surface_reader.parse_fn("fn channel_id(&self) -> &ChannelId")],
            "provided": [],
            "types": [],
        }
        report = run(
            [
                trait,
                struct(
                    "roomkit_ai::Skill",
                    methods=["pub fn new(instructions: String, path: PathBuf) -> Self", "pub fn path(&self) -> &Path"],
                ),
            ],
            {
                "roomkit.Channel": {"__init__": ["def", "channel_id", "name=None"]},
                "roomkit.Skill": {"__init__": ["def", "instructions", "path"]},
            },
        )
        self.assertEqual(found(report), [("python", "roomkit.Channel.__init__(name)")])


class Entries(unittest.TestCase):
    ITEMS = [
        struct(
            "roomkit::RoomKit",
            methods=[
                "pub fn new(options: RoomKitOptions) -> Self",
                "pub fn attach(&self)",
                "pub fn downgrade(&self) -> WeakRoomKit",
            ],
        ),
        struct("roomkit::RoomKitOptions", fields=[("store", "S")]),
        struct("roomkit::WeakRoomKit", methods=["pub fn upgrade(&self) -> Option<RoomKit>"]),
    ]
    SIGNATURES = {
        "roomkit.RoomKit": {
            "attach_channel": ["async def", "room_id"],
            "connect_voice": ["async def", "room_id"],
            "__init__": ["def", "store=None", "stt=None"],
        }
    }

    def test_each_finding_is_covered_by_its_entry(self) -> None:
        report = run(
            self.ITEMS,
            self.SIGNATURES,
            entries={
                "exception": [
                    {"rust": "roomkit::WeakRoomKit", "approved": "2026-10-27", "reason": "an Arc cycle"},
                    {"rust": "roomkit::RoomKit", "members": ["downgrade"], "approved": "2026-10-27", "reason": "idem"},
                ],
                "missing": [
                    {"python": "roomkit.RoomKit", "members": ["connect_voice"], "reason": "voice"},
                    {"python": "roomkit.RoomKit.__init__", "params": ["stt"], "reason": "voice"},
                ],
                "pending": [
                    {"rust": "roomkit::RoomKit", "members": ["attach"], "lot": 1, "reason": "decision 1"},
                    {"python": "roomkit.RoomKit", "members": ["attach_channel"], "lot": 1, "reason": "decision 1"},
                ],
            },
        )
        self.assertEqual(report.uncovered, [])
        self.assertEqual(report.dead, [])
        self.assertEqual(report.items["roomkit::WeakRoomKit"].status, "exception")
        self.assertEqual(sorted(report.pending), ["1"])
        self.assertEqual(sum(len(entry.covered) for entry in report.pending["1"]), 2)
        self.assertEqual(report.items["roomkit::RoomKitOptions"].status, "options")

    def test_rules_only_ignores_the_entries(self) -> None:
        report = run(
            self.ITEMS,
            self.SIGNATURES,
            entries={"pending": [{"rust": "roomkit::RoomKit", "members": ["attach"], "lot": 1, "reason": "x"}]},
            rules_only=True,
        )
        self.assertIn(("rust", "roomkit::RoomKit::attach"), [(f.side, f.path) for f in report.uncovered])
        self.assertEqual(report.pending, {})

    def test_an_entry_naming_what_is_gone_is_dead(self) -> None:
        report = run(
            self.ITEMS,
            self.SIGNATURES,
            entries={
                "pending": [{"rust": "roomkit::RoomKit", "members": ["shutdown"], "lot": 1, "reason": "x"}],
                "missing": [{"python": "roomkit.RoomKit", "members": ["gone"], "reason": "x"}],
            },
        )
        self.assertEqual(
            sorted(dead.path for dead in report.dead), ["roomkit.RoomKit.gone", "roomkit::RoomKit::shutdown"]
        )

    def test_an_entry_covering_nothing_is_dead(self) -> None:
        report = run(
            [struct("roomkit::RoomKit", methods=["pub async fn attach_channel(&self, room_id: &RoomId)"])],
            {"roomkit.RoomKit": {"attach_channel": ["async def", "room_id"]}},
            entries={"pending": [{"python": "roomkit.RoomKit", "members": ["attach_channel"], "lot": 1, "reason": "x"}]},
        )
        self.assertEqual([dead.path for dead in report.dead], ["roomkit.RoomKit.attach_channel"])
        self.assertIn("covers nothing", report.dead[0].why)

    def test_a_map_entry_names_the_python_counterpart(self) -> None:
        spec = {"close": ["async def"], "__init__": ["def"]}
        report = run(
            [struct("roomkit::KitError", methods=["pub fn new() -> Self"]), struct("roomkit::Kit", methods=["pub async fn shutdown(&self)", "pub fn new() -> Self"])],
            {"roomkit.RoomKitError": {"__init__": ["def"]}, "roomkit.Kit": spec},
            entries={
                "map": [
                    {"rust": "roomkit::KitError", "python": "roomkit.RoomKitError", "py_spec": parity.spec_hash({"__init__": ["def"]})},
                    {"rust": "roomkit::Kit::shutdown", "python": "roomkit.Kit.close", "py_spec": parity.spec_hash(["async def"])},
                ]
            },
        )
        self.assertEqual(report.items["roomkit::KitError"].python, "roomkit.RoomKitError")
        self.assertEqual(found(report), [])
        self.assertEqual(report.moved, [])

    def test_a_map_entry_whose_python_spec_moved(self) -> None:
        report = run(
            [struct("roomkit::KitError", methods=["pub fn new() -> Self"])],
            {"roomkit.RoomKitError": {"__init__": ["def"]}},
            entries={"map": [{"rust": "roomkit::KitError", "python": "roomkit.RoomKitError", "py_spec": "sha256:" + "0" * 64}]},
        )
        self.assertEqual([entry.rust for entry in report.moved], ["roomkit::KitError"])

    def test_entries_are_checked_against_their_schema(self) -> None:
        cases = [
            {"pending": [{"rust": "roomkit::RoomKit", "reason": "no lot"}]},
            {"pending": [{"rust": "roomkit::RoomKit", "lot": 9, "reason": "no such lot"}]},
            {"exception": [{"rust": "roomkit::RoomKit", "reason": "no date"}]},
            {"exception": [{"rust": "roomkit::RoomKit", "approved": "yesterday", "reason": "a bad date"}]},
            {"missing": [{"python": "roomkit.RoomKit", "members": ["a"], "reason": "x", "status": "an unknown key"}]},
            {"missing": [{"python": "roomkit.RoomKit", "members": ["a"], "params": ["b"], "reason": "both"}]},
            {"map": [{"rust": "roomkit::A", "python": "roomkit.A"}]},
            {"unknown": []},
        ]
        for entries in cases:
            with self.subTest(entries=entries):
                with self.assertRaises(parity.ParityError):
                    parity.Mapping.from_table({"rules": RULES, **entries})


class ThePin(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        (self.dir / "public_api_signatures.json").write_text(json.dumps({"roomkit.RoomKit": {}}), encoding="utf-8")
        (self.dir / "public_api_names.json").write_text(json.dumps({"roomkit": ["RoomKit"]}), encoding="utf-8")
        (self.dir / "ROOMKIT_COMMIT").write_text("6e420487\n", encoding="utf-8")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _sha256sum_line(self, name: str) -> str:
        return f"{hashlib.sha256((self.dir / name).read_bytes()).hexdigest()}  {name}\n"

    def test_the_fingerprint_is_the_makefile_s(self) -> None:
        listing = self._sha256sum_line("public_api_names.json") + self._sha256sum_line("public_api_signatures.json")
        self.assertEqual(parity.fingerprint(self.dir), hashlib.sha256(listing.encode()).hexdigest())

    def test_a_copy_edited_by_hand_is_refused(self) -> None:
        (self.dir / "SNAPSHOT_SHA256").write_text(parity.fingerprint(self.dir) + "\n", encoding="utf-8")
        python = parity.load_python(self.dir)
        self.assertEqual(python.commit, "6e420487")
        (self.dir / "public_api_names.json").write_text(json.dumps({"roomkit": []}), encoding="utf-8")
        with self.assertRaises(parity.ParityError):
            parity.load_python(self.dir)


if __name__ == "__main__":
    unittest.main()
