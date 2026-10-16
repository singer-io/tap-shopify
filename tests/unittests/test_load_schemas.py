import json
import os
import tempfile
import unittest
from unittest.mock import patch

import tap_shopify


class TestLoadSchemas(unittest.TestCase):
    def setUp(self):
        schema_dir = tempfile.TemporaryDirectory()
        self.addCleanup(schema_dir.cleanup)
        self.schema_dir = schema_dir.name
        schema_path = patch('tap_shopify.get_abs_path', return_value=self.schema_dir)
        schema_path.start()
        self.addCleanup(schema_path.stop)
        with open(os.path.join(self.schema_dir, 'orders.json'), 'w', encoding='UTF-8') as file:
            json.dump({'type': 'object', 'properties': {'id': {'type': 'integer'}}}, file)

    def test_loads_json_schema(self):
        self.assertEqual(tap_shopify.load_schemas(), {
            'orders': {'type': 'object', 'properties': {'id': {'type': 'integer'}}}
        })

    def test_ignores_non_json_files(self):
        for filename, contents in [
            ('.DS_Store', '\x00\x00\x00\x01Bud1'),
            ('notes.txt', 'not a JSON schema'),
            ('orders.json.bak', '{}'),
        ]:
            with self.subTest(filename=filename):
                path = os.path.join(self.schema_dir, filename)
                with open(path, 'w', encoding='UTF-8') as file:
                    file.write(contents)
                try:
                    self.assertEqual(tap_shopify.load_schemas(), {
                        'orders': {'type': 'object', 'properties': {'id': {'type': 'integer'}}}
                    })
                finally:
                    os.remove(path)

    def test_ignores_subdirectories(self):
        for dirname in ['nested', 'nested.json']:
            with self.subTest(dirname=dirname):
                path = os.path.join(self.schema_dir, dirname)
                os.mkdir(path)
                try:
                    self.assertEqual(tap_shopify.load_schemas(), {
                        'orders': {'type': 'object', 'properties': {'id': {'type': 'integer'}}}
                    })
                finally:
                    os.rmdir(path)

    def test_invalid_json_raises(self):
        with open(os.path.join(self.schema_dir, 'broken.json'), 'w', encoding='UTF-8') as file:
            file.write('{invalid JSON')

        with self.assertRaises(json.JSONDecodeError):
            tap_shopify.load_schemas()
