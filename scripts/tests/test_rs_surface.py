"""Tests of scripts/rs-surface.py, on the rustdoc pages of a fixture crate.

The fixture pages under fixtures/rustdoc/fixture/ keep the markup rustdoc
1.99.1 writes, trimmed to what the reader reads. The reader must read them
whole, and fail on a page it cannot read rather than return less.
"""

from __future__ import annotations

import importlib.util
import io
import json
import shutil
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "fixtures" / "rustdoc"


def _load(name: str, file: str):
    spec = importlib.util.spec_from_file_location(name, HERE.parent / file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


surface = _load("rs_surface", "rs-surface.py")


def _by_path(items: list[dict]) -> dict[str, dict]:
    return {item["path"]: item for item in items}


def _names(members: list[dict]) -> list[str]:
    return [member["name"] for member in members]


class ReadsTheFixtureCrate(unittest.TestCase):
    def setUp(self) -> None:
        self.items = _by_path(surface.read_crate(FIXTURES, "fixture"))

    def test_lists_every_item_with_its_kind(self) -> None:
        kinds = {path: item["kind"] for path, item in self.items.items()}
        self.assertEqual(
            kinds,
            {
                "fixture::Kit": "struct",
                "fixture::KitOptions": "struct",
                "fixture::model::Code": "struct",
                "fixture::Shape": "enum",
                "fixture::Store": "trait",
                "fixture::helper": "fn",
                "fixture::Alias": "type",
                "fixture::LIMIT": "constant",
            },
        )

    def test_reads_named_and_tuple_fields_with_their_types(self) -> None:
        options = self.items["fixture::KitOptions"]
        self.assertEqual(
            [(field["name"], field["type"]) for field in options["fields"]],
            [("max_depth", "u32"), ("timeout", "Option<Duration>"), ("type", "Shape")],
        )
        self.assertTrue(options["non_exhaustive"])
        self.assertEqual(self.items["fixture::model::Code"]["fields"], [{"name": "0", "type": "String"}])
        self.assertEqual(self.items["fixture::Kit"]["fields"], [])

    def test_reads_inherent_methods_and_leaves_trait_impls_out(self) -> None:
        methods = {method["name"]: method for method in self.items["fixture::Kit"]["methods"]}
        self.assertEqual(list(methods), ["new", "attach", "hook", "store"])
        attach = methods["attach"]
        self.assertTrue(attach["async"])
        self.assertEqual(attach["receiver"], "&self")
        self.assertEqual(
            attach["params"],
            [{"name": "room_id", "type": "&Code"}, {"name": "options", "type": "KitOptions"}],
        )
        self.assertEqual(attach["returns"], "Result<(), Shape>")
        self.assertEqual(attach["links"].get("KitOptions"), "fixture::KitOptions")
        self.assertIsNone(methods["new"]["receiver"])
        hook = methods["hook"]
        self.assertEqual(_names(hook["params"]), ["name", "handler"])
        self.assertEqual(hook["params"][0]["type"], "impl Into<String>")
        self.assertEqual(hook["where"], "F: Fn(Shape) -> Fut + Send,")
        self.assertEqual(methods["store"]["returns"], "&S")

    def test_reads_the_traits_an_item_implements(self) -> None:
        self.assertEqual(self.items["fixture::Kit"]["impls"], ["Default", "From<KitOptions>"])

    def test_reads_variants_and_the_fields_of_a_struct_variant(self) -> None:
        shape = self.items["fixture::Shape"]
        self.assertEqual(
            [(variant["name"], variant["decl"]) for variant in shape["variants"]],
            [("BeforeBroadcast", "BeforeBroadcast"), ("Wrapped", "Wrapped(Code)"), ("Refused", "Refused")],
        )
        self.assertEqual(
            shape["variants"][2]["fields"],
            [{"name": "room_id", "type": "Code"}, {"name": "reason", "type": "&'static str"}],
        )
        self.assertEqual(_names(shape["methods"]), ["is_refused"])
        self.assertEqual(shape["methods"][0]["receiver"], "self")

    def test_reads_associated_constants(self) -> None:
        code = self.items["fixture::model::Code"]
        self.assertEqual(code["constants"], [{"name": "SMS", "type": "Code"}])
        self.assertEqual(_names(code["methods"]), ["as_str"])

    def test_reads_required_and_provided_trait_methods(self) -> None:
        store = self.items["fixture::Store"]
        self.assertEqual(_names(store["required"]), ["get_room"])
        self.assertEqual(_names(store["provided"]), ["close"])
        self.assertEqual(store["types"], [{"name": "Error"}])
        get_room = store["required"][0]
        self.assertTrue(get_room["async"])
        self.assertEqual(get_room["params"], [{"name": "room_id", "type": "&Code"}])

    def test_reads_functions_aliases_and_constants(self) -> None:
        helper = self.items["fixture::helper"]["signature"]
        self.assertEqual(
            helper["params"],
            [{"name": "tag", "type": "&str"}, {"name": "directories", "type": "&[P]"}],
        )
        self.assertEqual(self.items["fixture::Alias"]["aliased"], "Box<dyn Error + Send>")
        self.assertEqual(self.items["fixture::LIMIT"]["type"], "usize")


class WritesTheSurfaceAsJson(unittest.TestCase):
    def test_counts_and_items_by_crate(self) -> None:
        out = io.StringIO()
        surface.main(["--doc", str(FIXTURES), "--crate", "fixture"], out=out, log=io.StringIO())
        written = json.loads(out.getvalue())
        self.assertEqual(written["rustdoc"], "1.99.1 (58cf8f9c3 2026-10-13)")
        self.assertEqual(len(written["crates"]["fixture"]), 8)
        self.assertEqual(
            written["counts"],
            {
                "items": 8,
                "methods": 6,
                "associated_constants": 1,
                "trait_methods": 2,
                "fields": 4,
                "variants": 3,
            },
        )


class FailsClosed(unittest.TestCase):
    """A page the reader cannot read is an error, never a smaller surface."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.doc = Path(self._tmp.name)
        shutil.copytree(FIXTURES / "fixture", self.doc / "fixture")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _edit(self, page: str, old: str, new: str) -> None:
        path = self.doc / "fixture" / page
        text = path.read_text(encoding="utf-8")
        self.assertIn(old, text, f"the fixture {page} no longer holds the text to edit")
        path.write_text(text.replace(old, new, 1), encoding="utf-8")

    def _refused(self, *fragments: str) -> None:
        with self.assertRaises(surface.SurfaceError) as raised:
            surface.read_crate(self.doc, "fixture")
        for fragment in fragments:
            self.assertIn(fragment, str(raised.exception))

    def test_a_crate_without_its_list_of_items(self) -> None:
        (self.doc / "fixture" / "all.html").unlink()
        self._refused("all.html")

    def test_a_crate_with_no_item(self) -> None:
        path = self.doc / "fixture" / "all.html"
        text = path.read_text(encoding="utf-8")
        start = text.index('<h3 id="structs">')
        end = text.index("</section>", start)
        path.write_text(text[:start] + text[end:], encoding="utf-8")
        self._refused("no item")

    def test_a_kind_of_item_it_does_not_know(self) -> None:
        self._edit(
            "all.html",
            '<h3 id="constants">',
            '<h3 id="unions">Unions</h3><ul class="all-items"><li><a href="union.U.html">U</a></li></ul>'
            '<h3 id="constants">',
        )
        self._refused("unions")

    def test_a_list_entry_it_cannot_read(self) -> None:
        self._edit("all.html", '<li><a href="fn.helper.html">helper</a></li>', "<li>helper</li>")
        self._refused("all.html")

    def test_a_missing_item_page(self) -> None:
        (self.doc / "fixture" / "enum.Shape.html").unlink()
        self._refused("enum.Shape.html")

    def test_a_page_of_another_kind(self) -> None:
        self._edit("enum.Shape.html", "<h1>Enum <span", "<h1>Struct <span")
        self._refused("enum.Shape.html", "Struct")

    def test_a_page_without_its_declaration(self) -> None:
        self._edit("fn.helper.html", '<pre class="rust item-decl">', '<pre class="rust">')
        self._refused("fn.helper.html", "declaration")

    def test_a_section_it_does_not_know(self) -> None:
        self._edit(
            "struct.Kit.html",
            '<h2 id="trait-implementations" class="section-header">',
            '<h2 id="deref-methods-Memory" class="section-header">Methods from Deref</h2>'
            '<h2 id="trait-implementations" class="section-header">',
        )
        self._refused("struct.Kit.html", "deref-methods-Memory")

    def test_fields_the_declaration_does_not_list(self) -> None:
        self._edit("struct.KitOptions.html", "    pub max_depth:", "    max_depth:")
        self._refused("struct.KitOptions.html", "fields")

    def test_a_field_whose_entry_it_cannot_read(self) -> None:
        self._edit(
            "struct.KitOptions.html",
            '<span id="structfield.timeout" class="structfield section-header">',
            '<span id="structfield.timeout" class="field section-header">',
        )
        self._refused("struct.KitOptions.html", "fields")

    def test_variants_the_declaration_does_not_list(self) -> None:
        self._edit("enum.Shape.html", "    BeforeBroadcast,\n", "")
        self._refused("enum.Shape.html", "variants")

    def test_trait_methods_the_page_does_not_list(self) -> None:
        path = self.doc / "fixture" / "trait.Store.html"
        page = path.read_text(encoding="utf-8")
        start = page.index('<h2 id="provided-methods"')
        end = page.index('<h2 id="dyn-compatibility"')
        path.write_text(page[:start] + page[end:], encoding="utf-8")
        self._refused("trait.Store.html", "2 trait methods", "the page 1")

    def test_a_method_entry_it_cannot_read(self) -> None:
        self._edit("trait.Store.html", '<section id="method.close" class="method">', '<section id="method.close">')
        self._refused("trait.Store.html", "member entries")

    def test_a_method_header_it_cannot_parse(self) -> None:
        self._edit("struct.Kit.html", 'pub fn <a href="#method.store"', 'pub fun <a href="#method.store"')
        self._refused("struct.Kit.html", "store")

    def test_a_method_without_its_header(self) -> None:
        self._edit(
            "struct.Kit.html",
            '<h4 class="code-header">pub fn <a href="#method.new"',
            '<h5 class="code-header">pub fn <a href="#method.new"',
        )
        self._refused("struct.Kit.html", "method.new")

    def _refused_after(self, cases: dict, *fragments: str) -> None:
        for label, (page, old, new) in cases.items():
            with self.subTest(label):
                shutil.rmtree(self.doc / "fixture")
                shutil.copytree(FIXTURES / "fixture", self.doc / "fixture")
                self._edit(page, old, new)
                self._refused(page, *fragments)

    def test_a_member_entry_written_another_way(self) -> None:
        method = '<section id="method.store" class="method">'
        constant = '<section id="associatedconstant.SMS" class="associatedconstant">'
        self._refused_after(
            {
                "its attributes swapped": ("struct.Kit.html", method, '<section class="method" id="method.store">'),
                "another attribute": ("struct.Kit.html", method, '<section id="method.store" data-x="1" class="method">'),
                "single quotes": ("struct.Kit.html", method, "<section id='method.store' class='method'>"),
                "a constant's attributes swapped": (
                    "model/struct.Code.html",
                    constant,
                    '<section class="associatedconstant" id="associatedconstant.SMS">',
                ),
            },
            "member entries",
        )

    def test_a_heading_of_the_list_it_cannot_read(self) -> None:
        self._refused_after(
            {"a heading with a class": ("all.html", '<h3 id="enums">', '<h3 id="enums" class="section-header">')},
            "headings",
        )

    def test_a_variant_field_it_cannot_read(self) -> None:
        self._refused_after(
            {
                "a field re-classed": (
                    "enum.Shape.html",
                    'id="variant.Refused.field.reason" class="section-header"',
                    'id="variant.Refused.field.reason" class="section-header field"',
                )
            },
            "Refused",
            "fields",
        )

    def test_an_implementation_entry_written_another_way(self) -> None:
        self._refused_after(
            {
                "its attributes swapped": (
                    "struct.Kit.html",
                    '<section id="impl-Default-for-Kit" class="impl">',
                    '<section class="impl" id="impl-Default-for-Kit">',
                )
            },
            "implementation entries",
        )

    def test_a_section_header_written_another_way(self) -> None:
        self._refused_after(
            {
                "its attributes swapped": (
                    "struct.Kit.html",
                    '<h2 id="implementations" class="section-header">',
                    '<h2 class="section-header" id="implementations">',
                )
            },
            "section headers",
        )

    def test_a_reexport_rustdoc_did_not_inline(self) -> None:
        self._edit(
            "index.html",
            '<h2 id="modules" class="section-header">',
            '<h2 id="reexports" class="section-header">Re-exports</h2><h2 id="modules" class="section-header">',
        )
        self._refused("index.html", "re-export")


class ParsesSignatures(unittest.TestCase):
    def test_a_closure_type_among_the_parameters(self) -> None:
        parsed = surface.parse_fn(
            "pub fn on<F>(&self, event_type: impl Into<String>, handler: F) where F: Fn(FrameworkEvent) -> Fut"
        )
        self.assertEqual(parsed["name"], "on")
        self.assertEqual(_names(parsed["params"]), ["event_type", "handler"])
        self.assertIsNone(parsed["returns"])

    def test_receivers(self) -> None:
        for receiver in ("&self", "&'a self", "&mut self", "mut self", "self", "self: Arc<Self>"):
            with self.subTest(receiver=receiver):
                parsed = surface.parse_fn(f"fn call({receiver}, x: u8) -> u8")
                self.assertEqual(parsed["receiver"], receiver)
                self.assertEqual(_names(parsed["params"]), ["x"])

    def test_qualifiers_and_generics(self) -> None:
        parsed = surface.parse_fn("pub const unsafe fn get<'a, T: Into<Vec<u8>>>(value: T) -> Result<(), E>")
        self.assertEqual(parsed["name"], "get")
        self.assertFalse(parsed["async"])
        self.assertEqual(parsed["params"], [{"name": "value", "type": "T"}])
        self.assertEqual(parsed["returns"], "Result<(), E>")

    def test_an_unused_parameter_keeps_its_name(self) -> None:
        parsed = surface.parse_fn("fn handle_inbound(&self, _context: RoomContext)")
        self.assertEqual(_names(parsed["params"]), ["_context"])

    def test_a_header_that_is_no_function(self) -> None:
        with self.assertRaises(surface.SurfaceError):
            surface.parse_fn("pub struct Kit")


if __name__ == "__main__":
    unittest.main()
