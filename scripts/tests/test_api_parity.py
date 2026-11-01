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
        "roomkit::testing": ["roomkit.testing"],
        "roomkit_ai": ["roomkit.providers.ai", "roomkit"],
    },
}


def struct(path, fields=(), methods=(), constants=(), impls=(), non_exhaustive=False):
    return {
        "path": path,
        "kind": "struct",
        "non_exhaustive": non_exhaustive,
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


def run(items, signatures, names=None, entries=None, rules_only=False, rules=None):
    crates: dict[str, list] = {}
    for item in items:
        crates.setdefault(item["path"].split("::")[0], []).append(item)
    if names is None:
        names = {}
        for path in signatures:
            package, _, name = path.rpartition(".")
            names.setdefault(package, []).append(name)
    table = {"rules": {**RULES, **(rules or {})}, **(entries or {})}
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

    def test_an_options_struct_is_the_one_its_signature_links_to(self) -> None:
        # Two `CommitEventOptions`, the kit's and the store's: the link
        # rustdoc writes on the parameter's type tells them apart.
        store = struct(
            "roomkit::store::Store",
            methods=["pub async fn commit_event(&self, room_id: &RoomId, options: CommitEventOptions)"],
        )
        store["methods"][0]["links"] = {"CommitEventOptions": "roomkit::store::CommitEventOptions"}
        report = run(
            [
                struct("roomkit::CommitEventOptions", fields=[("organization_id", "Option<String>")]),
                store,
                struct("roomkit::store::CommitEventOptions", fields=[("activity", "bool")]),
            ],
            {"roomkit.Store": {"commit_event": ["async def", "room_id", "*", "activity=True"]}},
        )
        self.assertEqual(found(report), [("rust", "roomkit::CommitEventOptions")])
        self.assertEqual(report.items["roomkit::store::CommitEventOptions"].status, "options")

    def test_an_options_struct_s_own_members_have_no_python_counterpart(self) -> None:
        report = run(
            [
                struct(
                    "roomkit::RoomKit",
                    methods=["pub async fn attach_channel(&self, room_id: &RoomId, options: AttachChannelOptions)"],
                ),
                struct(
                    "roomkit::AttachChannelOptions",
                    fields=[("access", "Access")],
                    methods=["pub fn with_magic(self, magic: u8) -> Self"],
                    constants=["MAGIC"],
                ),
            ],
            {"roomkit.RoomKit": {"attach_channel": ["async def", "room_id", "access=<Access.READ_WRITE>"]}},
        )
        self.assertEqual(
            found(report),
            [("rust", "roomkit::AttachChannelOptions::MAGIC"), ("rust", "roomkit::AttachChannelOptions::with_magic")],
        )
        self.assertEqual(report.items["roomkit::AttachChannelOptions"].status, "options")

    def test_an_options_struct_or_a_model_keeping_non_exhaustive(self) -> None:
        report = run(
            [
                struct(
                    "roomkit::RoomKit",
                    methods=["pub async fn mute(&self, room_id: &RoomId, options: MuteOptions)"],
                ),
                struct("roomkit::MuteOptions", fields=[("organization_id", "Option<String>")], non_exhaustive=True),
                struct("roomkit::model::RateLimit", fields=[("max_per_second", "Option<f64>")], non_exhaustive=True),
                struct("roomkit::model::RetryPolicy", fields=[("max_retries", "u32")], impls=["Default"]),
                {**enum("roomkit::HookExecution", ["Sync"]), "non_exhaustive": True},
            ],
            {
                "roomkit.RoomKit": {"mute": ["async def", "room_id", "*", "organization_id=None"]},
                "roomkit.models.RateLimit": {"__init__": ["def", "*", "max_per_second=None"]},
                "roomkit.models.RetryPolicy": {"__init__": ["def", "*", "max_retries=3"]},
                "roomkit.HookExecution": {"SYNC": ["member", "'sync'"]},
            },
        )
        found_here = [(f.path, f.detail) for f in report.findings if "non_exhaustive" in f.detail]
        self.assertEqual(
            sorted(path for path, _ in found_here), ["roomkit::MuteOptions", "roomkit::model::RateLimit"]
        )
        self.assertEqual(report.items["roomkit::MuteOptions"].status, "options")

    def test_an_excepted_struct_keeping_non_exhaustive(self) -> None:
        """A struct an `[[exception]]` covers whole, the options of a method
        only Rust has (PauseRoomOptions) or a Rust-only record
        (testing::Delivery), is built by struct update all the same: it is a
        finding when it keeps `#[non_exhaustive]`, which no exception
        covers."""
        dated = {"approved": "2026-10-27", "reason": "Python adds it", "until": "roomkit.RoomKit.pause_room"}
        report = run(
            [
                struct("roomkit::PauseRoomOptions", fields=[("organization_id", "Option<String>")], non_exhaustive=True),
                struct("roomkit::testing::Delivery", fields=[("text", "String")], non_exhaustive=True),
                struct("roomkit::testing::Tapped", fields=[("text", "String")]),
            ],
            {},
            entries={
                "exception": [
                    {"rust": "roomkit::PauseRoomOptions", **dated},
                    {"rust": "roomkit::testing::Delivery", **dated},
                    {"rust": "roomkit::testing::Tapped", **dated},
                ]
            },
        )
        self.assertEqual(
            [f.path for f in report.uncovered if "non_exhaustive" in f.detail],
            ["roomkit::PauseRoomOptions", "roomkit::testing::Delivery"],
        )
        self.assertEqual(len(report.uncovered), 2)
        self.assertEqual(report.items["roomkit::PauseRoomOptions"].status, "exception")

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

    def test_a_model_python_fills_and_rust_builds_by_literal_alone(self) -> None:
        report = run(
            [
                struct("roomkit::model::ProviderResult", fields=[("success", "bool"), ("error", "Option<String>")]),
                struct("roomkit::model::RoomTimers", fields=[("inactive_after_seconds", "Option<u64>")], impls=["Default"]),
                struct("roomkit::model::RateLimit", fields=[("max_per_second", "Option<f64>")]),
                struct("roomkit::model::Pair", fields=[("left", "u8"), ("right", "u8")]),
            ],
            {
                "roomkit.ProviderResult": {"__init__": ["def", "*", "success", "error=None"]},
                "roomkit.RoomTimers": {"__init__": ["def", "*", "inactive_after_seconds=None"]},
                "roomkit.models.RateLimit": {"__init__": ["def", "*", "max_per_second=None"]},
                "roomkit.Pair": {"__init__": ["def", "*", "left", "right"]},
            },
        )
        self.assertEqual(
            found(report),
            [("python", "roomkit.ProviderResult.__init__"), ("python", "roomkit.models.RateLimit.__init__")],
        )

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


def variant(name, fields=(), decl=None):
    return {"name": name, "decl": decl or name, "fields": [{"name": field, "type": "X"} for field in fields]}


def error_enum(path, variants):
    return {**enum(path), "impls": ["Debug", "Display", "Error"], "variants": variants}


class LotTwoRules(unittest.TestCase):
    """The rules lot 2 of step 5 adds: strong types, `Literal`s, the
    contents' tag, and the errors."""

    BUILTINS = {
        "builtins": {
            "roomkit::model::RoomId": {"python": "str", "members": ["new"]},
            "roomkit::model::ModelError": {"python": "ValueError"},
        }
    }

    def test_a_strong_type_and_a_validator_s_error_read_as_their_builtin(self) -> None:
        report = run(
            [
                struct("roomkit::model::RoomId", methods=["pub fn new(value: impl Into<String>) -> Result<RoomId, ModelError>"]),
                enum("roomkit::model::ModelError", ["EmptyIdentifier"]),
            ],
            {},
            rules=self.BUILTINS,
        )
        self.assertEqual(
            (
                report.items["roomkit::model::RoomId"].python,
                report.items["roomkit::model::ModelError"].python,
                found(report),
            ),
            ("builtins.str", "builtins.ValueError", []),
        )

    def test_a_variant_carrying_a_strong_type_is_python_s_value_of_it(self) -> None:
        variants = [variant("All"), variant("Channels", decl="Channels(ChannelSet)"), variant("Other", decl="Other(String)")]
        visibility = {**enum("roomkit::model::Visibility"), "variants": variants}
        report = run(
            [visibility],
            {"roomkit.Visibility": {"ALL": ["member", "'all'"]}},
            rules={"builtin_payloads": ["roomkit::model::Visibility::Channels"]},
        )
        self.assertEqual(found(report), [("rust", "roomkit::model::Visibility::Other")])

    def test_a_literal_is_read_by_the_field_it_types(self) -> None:
        literals = {
            "roomkit::model::HookAction": {"field": "roomkit.HookResult.__init__(action)", "values": ["allow"]},
            "roomkit::model::RichFormat": {"field": "roomkit.RichContent.__init__(form)", "values": ["html"]},
        }
        report = run(
            [enum("roomkit::model::HookAction", ["Allow"]), enum("roomkit::model::RichFormat", ["Html"])],
            {
                "roomkit.HookResult": {"__init__": ["def", "*", "action", "event=None"]},
                "roomkit.RichContent": {"__init__": ["def", "*", "body", "format='markdown'"]},
            },
            rules={"literals": literals},
        )
        self.assertEqual(
            (report.items["roomkit::model::HookAction"].python, found(report)),
            ("roomkit.HookResult.__init__(action)", [("rust", "roomkit::model::RichFormat")]),
        )

    def test_a_content_s_type_is_its_union_s_tag(self) -> None:
        content = {**enum("roomkit::model::EventContent"), "variants": [variant("Text", decl="Text(TextContent)")]}
        report = run(
            [
                content,
                struct(
                    "roomkit::model::TextContent",
                    fields=[("body", "String")],
                    methods=["pub fn new(body: impl Into<String>) -> TextContent"],
                ),
                struct("roomkit::model::Other", fields=[("body", "String")], methods=["pub fn new(body: String) -> Other"]),
            ],
            {
                "roomkit.TextContent": {"__init__": ["def", "*", "type='text'", "body"]},
                "roomkit.Other": {"__init__": ["def", "*", "type='other'", "body"]},
            },
            names={"roomkit": ["TextContent", "Other"], "roomkit.models": ["EventContent"]},
        )
        self.assertEqual(found(report), [("python", "roomkit.Other.__init__(type)")])

    def test_an_error_variant_is_its_python_class(self) -> None:
        error = error_enum(
            "roomkit::RoomKitError",
            [
                variant("RoomNotFound"),
                variant("ChangeRefused", ["reason"]),
                variant("TransitionRefused", ["room_id", "status"]),
                variant("Value"),
                variant("Store"),
            ],
        )
        family = {**error_enum("roomkit::SkillError", [variant("Parse")]), "impls": ["Error"]}
        report = run(
            [error, family],
            {
                "roomkit.RoomKitError": {},
                "roomkit.RoomNotFoundError": {},
                "roomkit.ChangeRefusedError": {"__init__": ["def", "reason"]},
                "roomkit.TransitionRefusedError": {"__init__": ["def", "room_id", "status", "transition"]},
                "roomkit.SkillError": {},
                "roomkit.SkillParseError": {},
            },
            rules={
                "builtin_errors": {"roomkit::RoomKitError::Value": "ValueError"},
                "error_families": {
                    "roomkit::RoomKitError": {
                        "roomkit.RoomNotFoundError": [],
                        "roomkit.ChangeRefusedError": [],
                        "roomkit.TransitionRefusedError": [],
                    },
                    "roomkit::SkillError": {"roomkit.SkillParseError": []},
                },
            },
        )
        self.assertEqual(
            found(report),
            [
                ("python", "roomkit.TransitionRefusedError.__init__(transition)"),
                ("rust", "roomkit::RoomKitError::Store"),
            ],
        )

    def test_a_literal_naming_no_field_is_refused(self) -> None:
        with self.assertRaises(parity.ParityError):
            parity.Rules.from_table(
                {**RULES, "literals": {"roomkit::model::HookAction": {"field": "roomkit.HookResult", "values": []}}}
            )


class StrictRules(unittest.TestCase):
    """Each divergence lot 2's rules let through, found (review lane B,
    tasks/probes/step5-lot2-review-B/synthetic_rules.py)."""

    FAMILY = {"roomkit.RoomNotFoundError": []}

    def families(self, **families) -> dict:
        return {"error_families": families}

    def test_a_variant_s_class_outside_its_family_is_found(self) -> None:
        # Synthetic cases 1b and 2: a model whose fields match, and an
        # exception of another hierarchy.
        report = run(
            [error_enum("roomkit::RoomKitError", [variant("Delivery", ["code", "message"]), variant("Classifier")])],
            {
                "roomkit.RoomKitError": {},
                "roomkit.DeliveryError": {"__init__": ["def", "*", "code", "message"]},
                "roomkit.ClassifierError": {},
            },
            rules=self.families(**{"roomkit::RoomKitError": {}}),
        )
        self.assertEqual(
            found(report),
            [("rust", "roomkit::RoomKitError::Classifier"), ("rust", "roomkit::RoomKitError::Delivery")],
        )

    def test_a_message_only_class_takes_the_fields_its_family_lists(self) -> None:
        # Synthetic case 3.
        report = run(
            [error_enum("roomkit::RoomKitError", [variant("RoomNotFound", ["room_id", "bogus"])])],
            {"roomkit.RoomKitError": {}, "roomkit.RoomNotFoundError": {}},
            rules=self.families(**{"roomkit::RoomKitError": {"roomkit.RoomNotFoundError": ["room_id"]}}),
        )
        self.assertEqual(found(report), [("rust", "roomkit::RoomKitError::RoomNotFound::bogus")])

    def test_a_family_s_class_is_found_before_a_bare_one(self) -> None:
        # Synthetic case 4.
        report = run(
            [error_enum("roomkit_ai::SkillError", [variant("Path", ["path"])])],
            {
                "roomkit.skills.SkillError": {},
                "roomkit.PathError": {"__init__": ["def", "path"]},
                "roomkit.skills.SkillPathError": {"__init__": ["def", "skill", "reason"]},
            },
            names={"roomkit": ["PathError", "SkillError"], "roomkit.skills": ["SkillError", "SkillPathError"]},
            rules={
                "packages": {**RULES["packages"], "roomkit_ai": ["roomkit", "roomkit.skills"]},
                **self.families(**{"roomkit_ai::SkillError": {"roomkit.skills.SkillPathError": []}}),
            },
        )
        self.assertEqual(
            found(report),
            [
                ("python", "roomkit.skills.SkillPathError.__init__(reason)"),
                ("python", "roomkit.skills.SkillPathError.__init__(skill)"),
                ("rust", "roomkit_ai::SkillError::Path::path"),
            ],
        )

    def test_a_family_s_class_without_its_variant_is_found(self) -> None:
        # Synthetic case 5.
        report = run(
            [error_enum("roomkit::SkillError", [variant("Parse")])],
            {"roomkit.SkillError": {}, "roomkit.SkillParseError": {}, "roomkit.SkillPathError": {}},
            rules=self.families(**{"roomkit::SkillError": {"roomkit.SkillParseError": [], "roomkit.SkillPathError": []}}),
        )
        self.assertEqual(found(report), [("python", "roomkit.SkillPathError")])

    def test_an_error_enum_without_a_family_is_found(self) -> None:
        report = run(
            [error_enum("roomkit::RoomKitError", [variant("RoomNotFound")])],
            {"roomkit.RoomKitError": {}, "roomkit.RoomNotFoundError": {}},
        )
        self.assertEqual(found(report), [("rust", "roomkit::RoomKitError")])

    def test_a_variant_s_class_takes_python_s_acronym_casing(self) -> None:
        # Synthetic case 11, a false finding before.
        report = run(
            [error_enum("roomkit::RoomKitError", [variant("SqliteSchema")])],
            {"roomkit.RoomKitError": {}, "roomkit.SQLiteSchemaError": {}},
            rules=self.families(**{"roomkit::RoomKitError": {"roomkit.SQLiteSchemaError": []}}),
        )
        self.assertEqual(found(report), [])

    def test_a_builtin_is_its_type_s_path_not_its_name(self) -> None:
        # Synthetic case 6: another crate's type of the same name.
        report = run(
            [struct("roomkit_ai::RoomId", fields=[("x", "u8")], methods=["pub fn frobnicate(&self) -> u8"])],
            {},
            rules={"builtins": {"roomkit::model::RoomId": {"python": "str"}}},
        )
        self.assertEqual(
            found(report), [("rust", "roomkit::model::RoomId"), ("rust", "roomkit_ai::RoomId")]
        )

    def test_a_builtin_s_member_the_table_does_not_list_is_found(self) -> None:
        # Synthetic case 7.
        report = run(
            [struct("roomkit::model::RoomId", methods=["pub fn new(v: String) -> RoomId", "pub fn system() -> RoomId"])],
            {},
            rules={"builtins": {"roomkit::model::RoomId": {"python": "str", "members": ["new", "as_str"]}}},
        )
        self.assertEqual(
            found(report),
            [("rust", "roomkit::model::RoomId::as_str"), ("rust", "roomkit::model::RoomId::system")],
        )

    def test_only_a_listed_variant_carries_a_builtin_as_python_s_value(self) -> None:
        # Synthetic case 8.
        status = {**enum("roomkit::model::EventStatus"), "variants": [variant("Pending"), variant("Custom", decl="Custom(RoomId)")]}
        room_id = struct("roomkit::model::RoomId", methods=["pub fn new(v: String) -> RoomId"])
        report = run(
            [status, room_id],
            {"roomkit.EventStatus": {"PENDING": ["member", "'pending'"]}},
            rules={
                "builtin_payloads": [],
                "builtins": {"roomkit::model::RoomId": {"python": "str", "members": ["new"]}},
            },
        )
        self.assertEqual(found(report), [("rust", "roomkit::model::EventStatus::Custom")])

    def test_a_literal_s_values_are_compared(self) -> None:
        # Synthetic case 9.
        report = run(
            [enum("roomkit::model::HookAction", ["Allow", "Block", "Defer"])],
            {"roomkit.HookResult": {"__init__": ["def", "*", "action", "event=None"]}},
            rules={
                "literals": {
                    "roomkit::model::HookAction": {
                        "field": "roomkit.HookResult.__init__(action)",
                        "values": ["allow", "block", "modify"],
                    }
                }
            },
        )
        self.assertEqual(
            found(report),
            [("python", "roomkit.HookResult.__init__(action)"), ("rust", "roomkit::model::HookAction::Defer")],
        )

    def test_a_type_tag_is_skipped_for_a_union_s_variant_only(self) -> None:
        # Synthetic case 10: an enum that stands for no Python union.
        report = run(
            [
                {**enum("roomkit::Other"), "variants": [variant("Wrap", decl="Wrap(Thing)")]},
                struct("roomkit::Thing", fields=[("body", "String")], methods=["pub fn new(body: String) -> Thing"]),
            ],
            # The tag is the variant's name, so that only the union check
            # tells the enum from a union (light review B, M11).
            {"roomkit.Thing": {"__init__": ["def", "*", "type='wrap'", "body"]}},
            names={"roomkit": ["Thing"]},
        )
        self.assertEqual(found(report), [("python", "roomkit.Thing.__init__(type)"), ("rust", "roomkit::Other")])

    def test_a_rule_naming_what_is_gone_is_found(self) -> None:
        report = run(
            [],
            {},
            rules={
                "builtins": {"roomkit::model::Gone": {"python": "str"}},
                "builtin_payloads": ["roomkit::model::Visibility::Gone"],
                "builtin_errors": {"roomkit::RoomKitError::Gone": "KeyError"},
                "error_families": {"roomkit::GoneError": {}},
                "literals": {"roomkit::model::GoneLiteral": {"field": "roomkit.Gone.__init__(gone)", "values": []}},
            },
        )
        self.assertEqual(
            sorted((finding.path, finding.detail) for finding in report.findings),
            [
                ("roomkit::GoneError", "listed in [rules] error_families: no such error enum"),
                ("roomkit::RoomKitError::Gone", "listed in [rules] builtin_errors: no such Rust variant"),
                ("roomkit::model::Gone", "listed in [rules] builtins: no such Rust item"),
                ("roomkit::model::GoneLiteral", "listed in [rules] literals: no such Rust item"),
                ("roomkit::model::Visibility::Gone", "listed in [rules] builtin_payloads: no such Rust variant"),
            ],
        )

    def test_a_builtin_error_is_read_on_an_error_enum_s_family_only(self) -> None:
        # Light review B, E23, E24 and E24b: a variant of an enum that is no
        # error, of an error enum no family reads, of one a builtin stands
        # for; and E25, a name that is no Python builtin exception.
        visibility = {**enum("roomkit::model::Visibility"), "variants": [variant("Channels")]}
        report = run(
            [
                visibility,
                error_enum("roomkit::StoreError", [variant("Value")]),
                error_enum("roomkit::model::ModelError", [variant("EmptyComposite")]),
                error_enum("roomkit::RoomKitError", [variant("Value"), variant("Odd")]),
            ],
            {"roomkit.RoomKitError": {}, "roomkit.Visibility": {"CHANNELS": ["member", "'channels'"]}},
            rules={
                "builtins": {"roomkit::model::ModelError": {"python": "ValueError"}},
                "builtin_errors": {
                    "roomkit::model::Visibility::Channels": "ValueError",
                    "roomkit::StoreError::Value": "ValueError",
                    "roomkit::model::ModelError::EmptyComposite": "ValueError",
                    "roomkit::RoomKitError::Value": "ValueError",
                    "roomkit::RoomKitError::Odd": "NoSuchError",
                },
                "error_families": {"roomkit::RoomKitError": {}},
            },
        )
        unread = "listed in [rules] builtin_errors: no error enum's family reads the variant"
        self.assertEqual(
            sorted((finding.path, finding.detail) for finding in report.findings if "[rules]" in finding.detail),
            [
                ("roomkit::RoomKitError::Odd", "listed in [rules] builtin_errors: NoSuchError is no Python builtin exception"),
                ("roomkit::StoreError::Value", unread),
                ("roomkit::model::ModelError::EmptyComposite", unread),
                ("roomkit::model::Visibility::Channels", unread),
            ],
        )

    def test_a_tuple_variant_s_payload_counts_a_generic_s_comma_once(self) -> None:
        # Light review B, N5c: `HashMap<K, V>` is one payload.
        generic = variant("RoomNotFound", decl="RoomNotFound(HashMap<RoomId, String>)")
        report = run(
            [error_enum("roomkit::RoomKitError", [generic])],
            {"roomkit.RoomKitError": {}, "roomkit.RoomNotFoundError": {}},
            rules=self.families(**{"roomkit::RoomKitError": {"roomkit.RoomNotFoundError": ["0"]}}),
        )
        self.assertEqual(found(report), [])

    def test_a_builtin_error_named_by_its_variant_alone_is_refused(self) -> None:
        # Light review B, N7.
        with self.assertRaises(parity.ParityError):
            parity.Rules.from_table({**RULES, "builtin_errors": {"Value": "ValueError"}})

    def test_a_builtin_error_is_its_variant_s_path_not_its_name(self) -> None:
        # Light review B, E19: another enum's `Value` is no ValueError.
        report = run(
            [error_enum("roomkit::SkillError", [variant("Value")])],
            {"roomkit.SkillError": {}},
            rules={
                "builtin_errors": {"roomkit::RoomKitError::Value": "ValueError"},
                "error_families": {"roomkit::SkillError": {}},
            },
        )
        self.assertEqual(found(report), [("rust", "roomkit::RoomKitError::Value"), ("rust", "roomkit::SkillError::Value")])

    def test_a_family_whose_enum_is_never_read_is_found(self) -> None:
        # Light review B, E16 and E17: an error enum with no Python base, and
        # one a builtin stands for.
        report = run(
            [
                error_enum("roomkit::StoreError", [variant("Bogus")]),
                error_enum("roomkit::model::ModelError", [variant("Empty")]),
            ],
            {},
            rules={
                "builtins": {"roomkit::model::ModelError": {"python": "ValueError"}},
                "error_families": {
                    "roomkit::StoreError": {"roomkit.RoomNotFoundError": ["bogus"]},
                    "roomkit::model::ModelError": {"roomkit.RoomNotFoundError": []},
                },
            },
        )
        self.assertEqual(
            found(report),
            [("rust", "roomkit::StoreError"), ("rust", "roomkit::StoreError"), ("rust", "roomkit::model::ModelError")],
        )

    def test_a_listed_field_a_variant_lacks_is_found(self) -> None:
        # Light review B, M13.
        report = run(
            [error_enum("roomkit::RoomKitError", [variant("RoomClosed", ["room_id"])])],
            {"roomkit.RoomKitError": {}, "roomkit.RoomClosedError": {}},
            rules=self.families(**{"roomkit::RoomKitError": {"roomkit.RoomClosedError": ["room_id", "gone"]}}),
        )
        self.assertEqual(found(report), [("rust", "roomkit::RoomKitError::RoomClosed::gone")])

    def test_fields_listed_for_a_class_with_a_constructor_are_found(self) -> None:
        # Light review B, M14: a constructor's parameters are compared, no
        # listed field stands in for them.
        report = run(
            [error_enum("roomkit::RoomKitError", [variant("ChangeRefused", ["reason"])])],
            {"roomkit.RoomKitError": {}, "roomkit.ChangeRefusedError": {"__init__": ["def", "reason"]}},
            rules=self.families(**{"roomkit::RoomKitError": {"roomkit.ChangeRefusedError": ["reason"]}}),
        )
        self.assertEqual(found(report), [("rust", "roomkit::RoomKitError::ChangeRefused")])

    def test_a_builtin_s_public_field_is_found(self) -> None:
        # Light review B, E3.
        report = run(
            [struct("roomkit::model::RoomId", fields=[("raw", "String")], methods=["pub fn new(v: String) -> RoomId"])],
            {},
            rules={"builtins": {"roomkit::model::RoomId": {"python": "str", "members": ["new"]}}},
        )
        self.assertEqual(found(report), [("rust", "roomkit::model::RoomId::raw")])

    def test_a_tuple_variant_s_payload_is_read_on_a_message_only_class(self) -> None:
        # Light review B, E22: the payload is named by position.
        tuple_variant = variant("RoomNotFound", decl="RoomNotFound(RoomId, String)")
        report = run(
            [error_enum("roomkit::RoomKitError", [tuple_variant])],
            {"roomkit.RoomKitError": {}, "roomkit.RoomNotFoundError": {}},
            rules=self.families(**{"roomkit::RoomKitError": {"roomkit.RoomNotFoundError": ["0"]}}),
        )
        self.assertEqual(found(report), [("rust", "roomkit::RoomKitError::RoomNotFound::1")])

    def test_a_type_tag_is_skipped_only_when_it_is_its_variant_s_name(self) -> None:
        # Light review B's nit: a union's variant whose struct's tag is not
        # the variant's name keeps its tag a parameter.
        content = {**enum("roomkit::model::EventContent"), "variants": [variant("Text", decl="Text(TextContent)")]}
        report = run(
            [
                content,
                struct("roomkit::model::TextContent", fields=[("body", "String")], methods=["pub fn new(body: String) -> TextContent"]),
            ],
            {"roomkit.TextContent": {"__init__": ["def", "*", "type='plain'", "body"]}},
            names={"roomkit": ["TextContent"], "roomkit.models": ["EventContent"]},
        )
        self.assertEqual(found(report), [("python", "roomkit.TextContent.__init__(type)")])


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

    def test_an_entry_never_covers_a_finding_about_the_rules(self) -> None:
        # Light review B, T1: a whole [[pending]] on an enum covers its "no
        # Python name", never a family the rules list on it that nothing
        # reads (coverprobe.py).
        report = run(
            [error_enum("roomkit::StoreError", [variant("Bogus")])],
            {},
            rules={"error_families": {"roomkit::StoreError": {"roomkit.RoomNotFoundError": []}}},
            entries={"pending": [{"lot": "4", "rust": "roomkit::StoreError", "reason": "lot 4"}]},
        )
        self.assertEqual(
            [(finding.path, finding.detail) for finding in report.uncovered],
            [
                (
                    "roomkit::StoreError",
                    "listed in [rules] error_families: the enum stands for no Python class whose family is read",
                )
            ],
        )

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

    def test_a_whole_entry_covers_its_item_s_own_finding_alone(self) -> None:
        report = run(
            [struct("roomkit::testing::RecordingTransport", methods=["pub fn deliveries(&self) -> Vec<Delivery>"])],
            {"roomkit.testing.RecordingTransport": {"sent": ["property"]}},
            entries={
                "exception": [
                    {"rust": "roomkit::testing::RecordingTransport", "approved": "2026-10-27", "reason": "Python first"}
                ]
            },
        )
        self.assertEqual(
            sorted((f.side, f.path) for f in report.uncovered),
            [("python", "roomkit.testing.RecordingTransport.sent"), ("rust", "roomkit::testing::RecordingTransport::deliveries")],
        )
        self.assertEqual([dead.path for dead in report.dead], ["roomkit::testing::RecordingTransport"])
        self.assertEqual(report.items["roomkit::testing::RecordingTransport"].status, "differs")

    def test_a_whole_entry_covers_an_alias_named_as_a_class(self) -> None:
        report = run(
            [{"path": "roomkit_ai::ProviderError", "kind": "type", "aliased": "Box<dyn Error>"}],
            {"roomkit.providers.ai.ProviderError": {"__init__": ["def", "message"]}},
            entries={"pending": [{"rust": "roomkit_ai::ProviderError", "lot": 5, "reason": "decision 11"}]},
        )
        self.assertEqual((report.uncovered, report.dead), ([], []))

    def test_an_exception_whose_python_name_arrived_is_due(self) -> None:
        entries = {
            "exception": [
                {
                    "rust": "roomkit::testing::EchoAgent",
                    "approved": "2026-10-27",
                    "reason": "Python first",
                    "until": "roomkit.testing.EchoAgent",
                },
                {
                    "rust": "roomkit::testing::Delivery",
                    "approved": "2026-10-27",
                    "reason": "Python first",
                    "until": "roomkit.testing.RecordingTransport",
                },
            ]
        }
        report = run(
            [struct("roomkit::testing::EchoAgent"), struct("roomkit::testing::Delivery")],
            {"roomkit.testing.EchoAgent": {}},
            entries=entries,
        )
        self.assertEqual([entry.rust for entry in report.due], ["roomkit::testing::EchoAgent"])

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

    def test_kwargs_stand_for_the_fields_a_map_entry_names(self) -> None:
        spec = ["async def", "room_id", "access=<Access.READ_WRITE>", "**kwargs"]
        report = run(
            [
                struct(
                    "roomkit::RoomKit",
                    methods=["pub async fn attach_channel(&self, room_id: &RoomId, options: AttachChannelOptions)"],
                ),
                struct(
                    "roomkit::AttachChannelOptions",
                    fields=[("access", "Access"), ("muted", "bool"), ("group", "bool"), ("extra", "u8")],
                ),
            ],
            {"roomkit.RoomKit": {"attach_channel": spec}},
            entries={
                "map": [
                    {
                        "rust": "roomkit::RoomKit::attach_channel",
                        "python": "roomkit.RoomKit.attach_channel",
                        "params": {"muted": "kwargs", "group": "kwargs"},
                        "py_spec": parity.spec_hash(spec),
                    }
                ]
            },
        )
        self.assertEqual(found(report), [("rust", "roomkit::AttachChannelOptions::extra")])

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
        whole_python = [
            {"missing": [{"python": "roomkit.RoomKit", "reason": "a whole class"}]},
            {"pending": [{"python": "roomkit.RoomKit", "lot": 1, "reason": "a whole class"}]},
            {"exception": [{"python": "roomkit.RoomKit", "approved": "2026-10-27", "reason": "a whole class"}]},
        ]
        until = [{"exception": [{"rust": "roomkit::A", "approved": "2026-10-27", "reason": "x", "until": "the next release"}]}]
        cases += whole_python + until
        for entries in cases:
            with self.subTest(entries=entries):
                with self.assertRaises(parity.ParityError):
                    parity.Mapping.from_table({"rules": RULES, **entries})


class TheGate(unittest.TestCase):
    """The gate before the tag (step 5, lot 6): each blocking rule fails a
    report that breaks it, and a clean report passes."""

    ITEMS = [struct("roomkit::RoomKit", methods=["pub async fn attach_channel(&self, room_id: &RoomId)"])]
    SIGNATURES = {"roomkit.RoomKit": {"attach_channel": ["async def", "room_id"]}}

    def test_a_clean_report_passes(self) -> None:
        self.assertEqual(parity.blocking(run(self.ITEMS, self.SIGNATURES)), [])

    def test_each_blocking_rule_fails_the_gate(self) -> None:
        unmapped = struct("roomkit::Extra", methods=["pub fn new() -> Self"])
        one_sided = {"roomkit.RoomKit": {"attach_channel": ["async def", "room_id", "category=None"]}}
        dead = {"missing": [{"python": "roomkit.RoomKit", "members": ["gone"], "reason": "x"}]}
        pending = {"pending": [{"python": "roomkit.RoomKit.attach_channel", "params": ["category"], "lot": 6, "reason": "x"}]}
        cases = {
            "an unmapped item": (run([*self.ITEMS, unmapped], self.SIGNATURES), "1 finding no entry covers"),
            "a one-sided parameter": (run(self.ITEMS, one_sided), "1 finding no entry covers"),
            "a dead entry": (run(self.ITEMS, self.SIGNATURES, entries=dead), "1 dead entry"),
            "a pending entry": (run(self.ITEMS, one_sided, entries=pending), "1 pending entry"),
        }
        for label, (report, reason) in cases.items():
            with self.subTest(label):
                self.assertEqual(parity.blocking(report), [reason])


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
