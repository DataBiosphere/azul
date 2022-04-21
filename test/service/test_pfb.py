import json
from pathlib import (
    Path,
)
from typing import (
    cast,
)

import fastavro

import azul
from azul.plugins.metadata.hca import (
    FileTransformer,
)
from azul.service import (
    avro_pfb,
)
from azul_test_case import (
    AzulUnitTestCase,
)


class TestPFB(AzulUnitTestCase):

    def test_pfb_schema(self):
        field_types = FileTransformer.field_types()
        schema = avro_pfb.pfb_schema_from_field_types(field_types)
        fastavro.parse_schema(schema)
        # Parsing successfully proves our schema is valid
        with self.assertRaises(KeyError):
            fastavro.parse_schema({'this': 'is not', 'an': 'avro schema'})

        def to_json(records):
            return json.dumps(records, indent=4, sort_keys=True)

        results_file = Path(__file__).parent / 'data' / 'pfb_manifest.schema.json'
        if results_file.exists():
            with open(results_file, 'r') as f:
                expected_records = json.load(f)
            self.assertEqual(expected_records, json.loads(to_json(schema)))
        else:
            with open(results_file, 'w') as f:
                f.write(to_json(schema))

    def test_pfb_metadata_object(self):
        metadata_entity = avro_pfb.pfb_metadata_entity(FileTransformer.field_types())
        field_types = FileTransformer.field_types()
        schema = avro_pfb.pfb_schema_from_field_types(field_types)
        parsed_schema = fastavro.parse_schema(cast(dict, schema))
        fastavro.validate(metadata_entity, parsed_schema)

    def test_pfb_entity_id(self):
        # Terra limits ID's 254 chars
        avro_pfb.PFBEntity(id='a' * 254, name='foo', object={})
        with self.assertRaises(azul.RequirementError):
            avro_pfb.PFBEntity(id='a' * 255, name='foo', object={})
