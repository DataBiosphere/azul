from typing import (
    Optional,
)

from azul import (
    config,
)
from azul.deployment import (
    aws,
)


class VersionService:
    key_name = 'object_url'
    value_name = 'current_version'

    def __init__(self):
        self.client = aws.client('dynamodb')

    def get(self, object_url: str) -> Optional[str]:
        """
        Strongly consistent read of object's current version, or None if the url
        is not tracked in the version table.
        """
        response = self.client.get_item(TableName=config.dynamo_object_version_table_name,
                                        Key={self.key_name: {'S': object_url}},
                                        ProjectionExpression=self.value_name,
                                        ConsistentRead=True)
        try:
            item = response['Item']
        except KeyError:
            return None
        else:
            return item[self.value_name]['S']

    def put(self, object_url: str, version: Optional[str], new_version: str) -> None:
        """
        Update object's current version from `version` to `new_version`, or fail
        if `version` does not match the stored current version.
        Provide `None` for the current version to begin tracking a previously
        untracked object.
        """
        condition_params = {
            # Check no entry exists when setting initial version
            'ConditionExpression': f'attribute_not_exists({self.key_name})'
        } if version is None else {
            # Require provided version to match current DB entry
            'ConditionExpression': f'{self.value_name} = :v',
            'ExpressionAttributeValues': {':v': {'S': version}}
        }
        item = {
            self.key_name: {'S': object_url},
            self.value_name: {'S': new_version},
        }
        try:
            # Conditional updates SHOULD be strongly consistent.
            # They are designed with concurrency in mind and the docs for the
            # legacy parameters that `ConditionExpression` replaces explicitly
            # describe their behavior as "strongly consistent."
            # See:
            # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ConditionalUpdate
            # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LegacyConditionalParameters.Expected.html
            self.client.put_item(TableName=config.dynamo_object_version_table_name,
                                 Item=item,
                                 **condition_params)
        except self.client.exceptions.ConditionalCheckFailedException:
            raise VersionConflict(version) from None


class VersionConflict(RuntimeError):

    def __init__(self, version):
        super().__init__(f'Version {version} is not current')


class NoSuchObjectVersion(RuntimeError):

    def __init__(self, version):
        super().__init__(f'Version {version} does not exist in S3')
