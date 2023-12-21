import json
from pathlib import (
    Path,
)
import sys

import fastavro

from azul_test_case import (
    AzulUnitTestCase,
)


class PFBTestCase(AzulUnitTestCase):

    def _assert_pfb_schema(self, schema):
        fastavro.parse_schema(schema)
        # Parsing successfully proves our schema is valid
        with self.assertRaises(KeyError):
            fastavro.parse_schema({'this': 'is not', 'an': 'avro schema'})

        def to_json(records):
            return json.dumps(records, indent=4, sort_keys=True)

        cls = type(self)
        module = sys.modules[cls.__module__]
        results_file = Path(module.__file__).parent / 'data' / 'pfb_manifest.schema.json'
        if results_file.exists():
            with open(results_file, 'r') as f:
                expected_records = json.load(f)
            self.assertEqual(expected_records, json.loads(to_json(schema)))
        else:
            with open(results_file, 'w') as f:
                f.write(to_json(schema))
