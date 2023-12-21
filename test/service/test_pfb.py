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
from pfb_test_case import (
    PFBTestCase,
)


class TestPFB(PFBTestCase):

    def test_pfb_schema(self):
        self.maxDiff = None
        field_types = FileTransformer.field_types()
        schema = avro_pfb.pfb_schema_from_field_types(field_types)
        self._assert_pfb_schema(schema)

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
