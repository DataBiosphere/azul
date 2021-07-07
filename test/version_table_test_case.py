from moto import (
    mock_dynamodb2,
)

from azul import (
    config,
)
from azul.version_service import (
    VersionService,
)
from dynamodb_test_case import (
    DynamoDBTestCase,
)


@mock_dynamodb2
class VersionTableTestCase(DynamoDBTestCase):
    ddb_table_name = config.dynamo_object_version_table_name
    ddb_hash_key = VersionService.key_name
    ddb_attrs = {VersionService.key_name: 'S'}
