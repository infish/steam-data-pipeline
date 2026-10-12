import importlib.util
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]
spec = importlib.util.spec_from_file_location(
    "generate_data_dictionary",
    ROOT / "scripts" / "generate_data_dictionary.py"
)
generator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generator)


class DataDictionaryTests(unittest.TestCase):
    def descriptions(self):
        return json.loads((ROOT / "docs" / "data_dictionary.json").read_text())

    def test_missing_and_stale_descriptions_are_reported(self):
        descriptions = {"layers": [{"objects": {"a": {}, "old": {}}}]}

        self.assertEqual(
            generator.problems_with({"a": "BASE TABLE", "new": "VIEW"},
                                    descriptions),
            [
                "No description in docs/data_dictionary.json for: new",
                "Described but not in the database: old",
            ]
        )

    def test_every_table_created_by_a_migration_is_described(self):
        import re

        described = {
            name
            for layer in self.descriptions()["layers"]
            for name in layer["objects"]
        }
        created = set()

        for path in (ROOT / "db" / "migrations").glob("*.sql"):
            created |= set(re.findall(
                r"CREATE (?:TABLE|OR REPLACE VIEW) (\w+)", path.read_text()
            ))

        self.assertEqual(sorted(created - described), [])

    def test_render_marks_keys_and_rejects_unknown_columns(self):
        descriptions = {
            "introduction": "Intro.",
            "layers": [{
                "title": "Layer",
                "description": "About.",
                "objects": {"t": {
                    "description": "A table.",
                    "columns": {"id": "The key."},
                }},
            }],
        }
        columns = {"t": [{"name": "id", "type": "int", "nullable": False,
                          "key": "PRI", "generated": False}]}

        text = generator.render({"t": "BASE TABLE"}, columns, descriptions)

        self.assertIn("### `t` (table)", text)
        self.assertIn("| `id` | int | no | PK | The key. |", text)

        descriptions["layers"][0]["objects"]["t"]["columns"]["nope"] = "x"

        with self.assertRaisesRegex(ValueError, "nope"):
            generator.render({"t": "BASE TABLE"}, columns, descriptions)


if __name__ == "__main__":
    unittest.main()
