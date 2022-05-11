from collections.abc import (
    Mapping,
    Sequence,
)
import json
from typing import (
    Optional,
    Union,
)
import unittest
from unittest.mock import (
    MagicMock,
    patch,
)

from more_itertools import (
    first,
)

from azul.json import (
    AnyJSON,
)
from azul.logging import (
    configure_test_logging,
)
from azul.terraform import (
    populate_tags,
)
from azul_test_case import (
    AzulUnitTestCase,
)


# noinspection PyPep8Naming
def setupModule():
    configure_test_logging()


class TestTerraformResourceTags(AzulUnitTestCase):

    def assertDictEqualPermissive(self,
                                  expected: AnyJSON,
                                  actual: AnyJSON
                                  ) -> None:
        path = self.permissive_compare(expected, actual)
        self.assertIsNone(path, f'Discrepancy at path: {path}')

    def permissive_compare(self,
                           expected: AnyJSON,
                           actual: AnyJSON,
                           *path: Union[str, int]
                           ) -> Optional[tuple[Union[int, str], ...]]:
        """
        Recursive JSON comparison. A None value in `expected` matches any value
        at the same position in `actual`.

        :return: None, if the two arguments, the path of the discrepancy as a
                 tuple of strings otherwise.

        >>> t = TestTerraformResourceTags()
        >>> t.permissive_compare(
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, '456': 789}]},
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, '456': 789}]}
        ... )

        >>> t.permissive_compare(
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, '456': None}]},
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, '456': 'abc'}]}
        ... )

        >>> t.permissive_compare(
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, '456': 'def'}]},
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, '456': 'abc'}]}
        ... )
        ...
        ('qaz', 0, '456')

        >>> t.permissive_compare(
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, '456': 'def'}]},
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, '456': None}]}
        ... )
        ('qaz', 0, '456')

        >>> t.permissive_compare(
        ...     {'foo': 'bar', 'qaz': None, '456': 'def'},
        ...     {'foo': 'bar', 'qaz': [{'qux': 123, '456': 'abc'}]}
        ... )
        ('456',)

        >>> t.permissive_compare(
        ...     {'foo': 'bar', 'qaz': [{'qux': {'123': 890}}]},
        ...     {'foo': 'bar', 'qaz': [{'qux': {'123': 456}}]}
        ... )
        ('qaz', 0, 'qux', '123')

        >>> t.permissive_compare(None, 123)

        >>> t.permissive_compare({}, [])
        ()

        >>> t.permissive_compare([], [1])
        (0,)

        >>> t.permissive_compare([1], [])
        (0,)

        >>> t.permissive_compare({}, {'0':1})
        ('0',)

        >>> t.permissive_compare({'0':1}, {})
        ('0',)
        """
        primitive_json = (str, int, float, bool)
        if isinstance(actual, primitive_json) and isinstance(expected, primitive_json):
            if expected != actual:
                return path
        elif expected is None:
            pass
        elif isinstance(actual, Sequence) and isinstance(expected, Sequence):
            if len(actual) > len(expected):
                return *path, len(expected)
            else:
                for i, expected_v in enumerate(expected):
                    try:
                        actual_v = actual[i]
                    except IndexError:
                        return *path, i
                    else:
                        diff = self.permissive_compare(expected_v, actual_v, *path, i)
                        if diff is not None:
                            return diff
        elif isinstance(actual, Mapping) and isinstance(expected, Mapping):
            if len(actual) > len(expected):
                return *path, first(actual.keys() - expected.keys())
            else:
                for k, expected_v in expected.items():
                    assert isinstance(k, str)
                    try:
                        actual_v = actual[k]
                    except KeyError:
                        return *path, k
                    else:
                        diff = self.permissive_compare(expected_v, actual_v, *path, k)
                        if diff is not None:
                            return diff
        else:
            return path

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
