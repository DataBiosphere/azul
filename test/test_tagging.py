import json
from typing import (
    Mapping,
    Optional,
    Tuple,
)
import unittest
from unittest.mock import (
    MagicMock,
    patch,
)

from azul.deployment import (
    populate_tags,
)
from azul.json import (
    AnyJSON,
)
from azul.logging import (
    configure_test_logging,
)
from azul_test_case import (
    AzulUnitTestCase,
)


def setupModule():
    configure_test_logging()


class TestTerraformResourceTags(AzulUnitTestCase):

    def assertDictEqualPermissive(self, expected: Mapping, actual: Mapping):
        path = self.permissive_compare(expected, actual)
        self.assertIsNone(path, f'Discrepancy in path: {path}')

    def permissive_compare(self,
                           expected: AnyJSON,
                           actual: AnyJSON,
                           *path: str) -> Optional[Tuple[str]]:
        """
        Recursive dictionary comparison. Skips comparison for value in
        `actual` when the corresponding value in `expected` is set to
        None.

        Returns None if dictionaries match, or path of discrepancy as a
        tuple otherwise.

        >>> t = TestTerraformResourceTags()
        >>> t.permissive_compare(
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, 456: 789}]},
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, 456: 789}]}
        ... )

        >>> t.permissive_compare(
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, 456: None}]},
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, 456: 'abc'}]}
        ... )

        >>> t.permissive_compare(
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, 456: 'def'}]},
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, 456: 'abc'}]}
        ... )
        ...
        ('qaz', '[0]', '456')

        >>> t.permissive_compare(
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, 456: 'def'}]},
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, 456: None}]}
        ... )
        ('qaz', '[0]', '456')

        >>> t.permissive_compare(
        ...     {'foo': 'bar', 'qaz': None, 456: 'def'},
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, 456: 'abc'}]}
        ... )
        ('456',)

        >>> t.permissive_compare(
        ...     {'foo': 'bar', 'qaz': [{'qux': {123: 890}}]},
        ...     {'foo': 'bar', 'qaz': [{'qux': {123: 456}}]}
        ... )
        ('qaz', '[0]', 'qux', '123')
        """
        if isinstance(actual, list) and isinstance(expected, list):
            for i, v in enumerate(zip(expected, actual)):
                diff = self.permissive_compare(*v, *path, f'[{i}]')
                if diff is not None:
                    return diff
        elif isinstance(actual, dict) and isinstance(expected, dict):
            for k, v in expected.items():
                try:
                    diff = self.permissive_compare(v, actual[k], *path, str(k))
                except KeyError:
                    return path + (str(k),)
                else:
                    if diff is not None:
                        return diff
        elif expected is None:
            pass
        elif expected != actual:
            return path
        return None

    @patch('subprocess.run', new_callable=MagicMock)
    def test(self, terraform_mock):
        terraform_mock.return_value.stdout = json.dumps({
            'format_version': '0.1',
            'provider_schemas': {
                'aws': {
                    'resource_schemas': {
                        'aws_vpc': {
                            'block': {
                                'attributes': {
                                    'tags': {}
                                }
                            }
                        }
                    }
                },
                'gcp': {
                    'resource_schemas': {
                        'google_compute_instance': {
                            'block': {
                                'attributes': {
                                    'tags': {}
                                }
                            }
                        }
                    }
                }
            }
        }).encode()

        tagged_aws_resource = {
            'resource': {
                'aws_vpc': {
                    'name': {}
                }
            }
        }
        expected = {
            'resource': [{
                'aws_vpc': [{
                    'name': {
                        'tags': {
                            'project': None,
                            'service': None,
                            'deployment': None,
                            'owner': None,
                            'Name': None,
                            'component': None
                        }
                    }
                }]
            }]
        }
        tagged = populate_tags(tagged_aws_resource)
        self.assertDictEqualPermissive(expected, tagged)

        tagged_gcp_resource = {
            'resource': {
                'google_compute_instance': {
                    'name': {}
                }
            }
        }
        expected = {
            'resource': [{
                'google_compute_instance': [{
                    'name': {
                        'tags': {
                            'project': None,
                            'service': None,
                            'deployment': None,
                            'owner': None,
                            'name': None,
                            'component': None
                        }
                    }
                }]
            }]
        }
        tagged = populate_tags(tagged_gcp_resource)
        self.assertDictEqualPermissive(expected, tagged)

        untaggable_aws_resource = {
            'resource': {
                'aws_untaggable_resource': {'name': {}}
            }
        }
        expected = {
            'resource': [
                {'aws_untaggable_resource': [{'name': {}}]}
            ]
        }
        tagged = populate_tags(untaggable_aws_resource)
        self.assertDictEqualPermissive(expected, tagged)

        manually_tagged_resource = {
            'resource': {
                'aws_vpc': {
                    'name': {
                        'tags': {
                            'component': 'foo'
                        }
                    }
                }
            }
        }
        expected = {
            'resource': [{
                'aws_vpc': [{
                    'name': {
                        'tags': {
                            'project': None,
                            'service': None,
                            'deployment': None,
                            'owner': None,
                            'Name': None,
                            'component': 'foo'
                        }
                    }
                }]
            }]
        }
        tagged = populate_tags(manually_tagged_resource)
        self.assertDictEqualPermissive(expected, tagged)


if __name__ == '__main__':
    unittest.main()
