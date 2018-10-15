from azul.deployment import aws


class DynamoClientFactory:
    @classmethod
    def get(cls):
        return aws.dynamo
