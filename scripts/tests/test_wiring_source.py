"""The executor inventory follows the instances available through its Python wiring."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from api_parity.wiring_source import EMITTER, emitter_matches, wiring_records


class WiringInventory(unittest.TestCase):
    def sources(self) -> dict[str, str]:
        return {
            "_outlet": """
FrameworkEmitter = Callable[..., Awaitable[None]]
async def emit_framework_event(fallback, event_type, *, channel_id=None, data=None): pass
class TurnRealtime(RealtimeBackend):
    async def publish(self, channel, event): pass
""",
            "core_hooks": "@dataclass\nclass SyncPipelineResult:\n    event: object = None\n    errors: list = field(default_factory=list)",
            "realtime_base": "class RealtimeBackend:\n    async def close(self): pass",
            "_wiring": """
class TurnWiring:
    def __init__(self, env):
        self.env = env
        self._private = env
    async def hook_context(self, room_id, *, carrying=None): pass
""",
            "_wiring_env": "class WiringEnv:\n    def emitter(self): pass",
            "_tool_hooks": "class ToolHooks:\n    def build_tool_call_hook(self, channel_id): pass",
        }

    def test_inherited_methods_attributes_and_variadic_emitters_retain_their_signatures(self):
        source = self.sources()
        records = wiring_records(source)
        self.assertEqual(records["roomkit.turns._wiring.TurnWiring"], {
            "hook_context": ["async def", "room_id", "*", "carrying=None"], "env": ["property"],
        })
        self.assertEqual(records["roomkit.core.hooks.SyncPipelineResult"]["__init__"], ["def", "event=None", "errors=<factory>"])
        outlet = records["roomkit.turns._outlet.TurnRealtime"]
        self.assertEqual(outlet, {"close": ["async def"], "publish": ["async def", "channel", "event"]})
        self.assertEqual(records[EMITTER]["__call__"], ["async def", "event_type", "*", "channel_id=None", "data=None"])
        source["_outlet"] = source["_outlet"].replace("Awaitable[None]", "None")
        with self.assertRaisesRegex(ValueError, "asynchronous"):
            wiring_records(source)

    def test_changing_a_rust_callback_to_a_direct_call_or_losing_an_event_field_is_detected(self):
        alias = {"decl": "pub type FrameworkEmitter = Arc<dyn Fn(String, EmitOptions) -> TurnFuture<'static, Result<(), RoomKitError>> + Send + Sync>;"}
        fields = [
            {"name": name, "type": f"Option<{kind}>"}
            for name, kind in (("room_id","RoomId"), ("channel_id","ChannelId"), ("event_id","EventId"), ("data","Metadata"))
        ]
        getter = {"name": "emitter", "returns": "Option<FrameworkEmitter>", "receiver": "&self", "params": []}
        items = {"roomkit::turns::EmitOptions": {"fields": fields}, "roomkit::turns::WiringEnv": {"provided": [getter]}}
        expected = ["async def", "event_type", "*", "room_id=None", "channel_id=None", "event_id=None", "data=None"]
        self.assertTrue(emitter_matches(alias, expected, items))
        getter["params"] = [{"name": "event", "type": "FrameworkEvent"}]
        self.assertFalse(emitter_matches(alias, expected, items))
        getter["params"] = []
        fields.pop()
        self.assertFalse(emitter_matches(alias, expected, items))
