import json

import fastavro

from indexer import (
    CannedFileTestCase,
)


class PFBTestCase(CannedFileTestCase):

    def _assert_pfb_schema(self, schema):
        fastavro.parse_schema(schema)
        # Parsing successfully proves our schema is valid
        with self.assertRaises(KeyError):
            fastavro.parse_schema({'this': 'is not', 'an': 'avro schema'})

        def to_json(records):
            return json.dumps(records, indent=4, sort_keys=True)

        results_file = self._data_path('service') / 'pfb_manifest.schema.json'
        if results_file.exists():
            with open(results_file, 'r') as f:
                expected_records = json.load(f)
            self.assertEqual(expected_records, json.loads(to_json(schema)))
        else:
            with open(results_file, 'w') as f:
                f.write(to_json(schema))
