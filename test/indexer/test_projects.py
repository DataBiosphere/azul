import unittest

from more_itertools import (
    one,
)

from azul import (
    config,
)
from azul.es import (
    ESClientFactory,
)
from azul.indexer.document import (
    AggregateCoordinates,
    CataloguedEntityReference,
    ContributionCoordinates,
)
from azul.logging import (
    configure_test_logging,
)
from indexer.test_hca_indexer import (
    IndexerTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class TestDataExtractorTestCase(IndexerTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.es_client = ESClientFactory.get()

    def setUp(self) -> None:
        super().setUp()
        self.index_service.create_indices(self.catalog)

    def tearDown(self) -> None:
        self.index_service.delete_indices(self.catalog)
        super().tearDown()

    def test_hca_extraction(self):
        bundle_fqids = [
            self.bundle_fqid(uuid='17a3d288-01a0-464a-9599-7375fda3353d',
                             version='2018-03-28T151023.074974Z'),
            self.bundle_fqid(uuid='2a87dc5c-0c3c-4d91-a348-5d784ab48b92',
                             version='2018-03-29T104041.822717Z'),
            self.bundle_fqid(uuid='4afbb0ea-81ad-49dc-9b12-9f77f4f50be8',
                             version='2018-03-29T090403.442059Z'),
            self.bundle_fqid(uuid='aaa96233-bf27-44c7-82df-b4dc15ad4d9d',
                             version='2018-11-04T113344.698028Z'),
            self.bundle_fqid(uuid='b0850e79-5544-49fe-b54d-e29b9fc3f61f',
                             version='2018-03-29T090340.934358Z'),
            self.bundle_fqid(uuid='c94a43f9-257f-4cd0-b2fe-eaf6d5d37d18',
                             version='2018-03-29T090343.782253Z')
        ]
        for bundle_fqid in bundle_fqids:
            self._index_canned_bundle(bundle_fqid)
        for aggregate in True, False:
            with self.subTest(aggregate=aggregate):
                def index_name(entity_type):
                    return config.es_index_name(catalog=self.catalog,
                                                entity_type=entity_type,
                                                aggregate=aggregate)

                total_projects = self.es_client.count(index=index_name('projects'))
                # Three unique projects, six project contributions
                self.assertEqual(3 if aggregate else 6, total_projects["count"])
                total_files = self.es_client.count(index=index_name('files'))
                self.assertEqual(776, total_files["count"])
                total_samples = self.es_client.count(index=index_name('samples'))
                self.assertEqual(129, total_samples["count"])

    # When two processes point at a file (this is the case for most files in production)
    # there was a bug where the files index contains duplicate dictionaries for the file.
    #
    def test_no_duplicate_files_in_specimen(self):
        bundle_fqid = self.bundle_fqid(uuid='8543d32f-4c01-48d5-a79f-1c5439659da3',
                                       version='2018-03-29T143828.884167Z')
        self._index_canned_bundle(bundle_fqid)
        for aggregate in True, False:
            with self.subTest(aggregate=aggregate):
                entity = CataloguedEntityReference(catalog=self.catalog,
                                                   entity_type='samples',
                                                   entity_id='b3623b88-c369-46c9-a2e9-a16042d2c589')
                if aggregate:
                    coordinates = AggregateCoordinates(entity=entity)
                else:
                    coordinates = ContributionCoordinates(entity=entity,
                                                          bundle=bundle_fqid.upcast(),
                                                          deleted=False)
                result = self.es_client.get(index=coordinates.index_name,
                                            id=coordinates.document_id)
                files = result['_source']['contents']['files']
                num_files = 2  # fastqs
                if aggregate:
                    self.assertEqual(num_files, one(files)['count'])
                else:
                    file_ids = [f['uuid'] for f in files]
                    self.assertEqual(num_files, len(file_ids))
                    self.assertEqual(num_files, len(set(file_ids)))


if __name__ == "__main__":
    unittest.main()
