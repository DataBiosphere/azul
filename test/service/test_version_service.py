import unittest

from azul.logging import configure_test_logging
from azul.version_service import (
    VersionService,
    VersionConflict,
)
from version_table_test_case import VersionTableTestCase


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class TestVersionService(VersionTableTestCase):

    def setUp(self):
        super().setUp()
        self.version_service = VersionService()
        self.url = 's3:/some_bucket/some_object'

    def test_versioning(self):
        with self.subTest('init'):
            self.assertIsNone(self.version_service.get(self.url))
            self.version_service.put(self.url, None, 'v1')
            self.assertEqual(self.version_service.get(self.url), 'v1')

        with self.subTest('update'):
            self.version_service.put(self.url, 'v1', 'v2')
            self.assertEqual(self.version_service.get(self.url), 'v2')

        with self.subTest('conflict'):
            tests = (
                [self.url, 'not current version', 'v3'],
                [self.url, None, 'v4']
            )
            for args in tests:
                self.assertRaises(VersionConflict, self.version_service.put, *args)


if __name__ == '__main__':
    unittest.main()
