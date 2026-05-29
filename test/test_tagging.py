from azul.infra.terraform import (
    _transform_tf,
)
from azul.lib.types import (
    JSON,
)
from azul.logging import (
    configure_test_logging,
)
from azul_test_case import (
    AzulUnitTestCase,
    patch_config,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class TestTerraformResourceTags(AzulUnitTestCase):

    @patch_config('deployment_stage', 'spam')
    @patch_config('owner', 'spam@alot.tld')
    @patch_config('billing', 'donald')
    @patch_config('terraform_component', 'blimp')
    def test(self):
        tagged_aws_resource: JSON = {
            'resource': {
                'aws_vpc': {
                    'foo': {}
                }
            }
        }
        expected: JSON = {
            'resource': [{
                'aws_vpc': [{
                    'foo': {
                        'tags': {
                            'billing': 'donald',
                            'service': 'azul',
                            'deployment': 'spam',
                            'owner': 'spam@alot.tld',
                            'Name': 'azul-foo',
                            'component': 'azul-foo',
                            'terraform_component': 'blimp'
                        }
                    }
                }]
            }]
        }
        tagged = _transform_tf(tagged_aws_resource)
        self.assertDictEqual(expected, tagged)

        tagged_gcp_resource: JSON = {
            'resource': {
                'google_compute_instance': {
                    'foo': {}
                }
            }
        }
        expected = {
            'resource': [{
                'google_compute_instance': [{
                    'foo': {
                        'tags': {
                            'billing': 'donald',
                            'service': 'azul',
                            'deployment': 'spam',
                            'owner': 'spam@alot.tld',
                            'name': 'azul-foo',
                            'component': 'azul-foo',
                            'terraform_component': 'blimp'
                        }
                    }
                }]
            }]
        }
        tagged = _transform_tf(tagged_gcp_resource)
        self.assertDictEqual(expected, tagged)

        untaggable_aws_resource: JSON = {
            'resource': {
                'aws_untaggable_resource': {'foo': {}}
            }
        }
        expected = {
            'resource': [
                {'aws_untaggable_resource': [{'foo': {}}]}
            ]
        }
        tagged = _transform_tf(untaggable_aws_resource)
        self.assertDictEqual(expected, tagged)

        manually_tagged_resource = {
            'resource': {
                'aws_vpc': {
                    'foo': {
                        'tags': {
                            'component': 'bar'
                        }
                    }
                }
            }
        }
        expected = {
            'resource': [{
                'aws_vpc': [{
                    'foo': {
                        'tags': {
                            'billing': 'donald',
                            'service': 'azul',
                            'deployment': 'spam',
                            'owner': 'spam@alot.tld',
                            'Name': 'azul-foo',
                            'component': 'bar',
                            'terraform_component': 'blimp'
                        }
                    }
                }]
            }]
        }
        tagged = _transform_tf(manually_tagged_resource)
        self.assertDictEqual(expected, tagged)
