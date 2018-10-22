from azul.deployment import aws


class DynamoDataAccessor:
    """
    Data access abstraction layer between the webservice and DynamoDB
    """

    def __init__(self):
        self.dynamo_client = aws.dynamo

    def _flatten_item(self, item):
        """
        Boto3 DynamoDB results values are a dict with a type as the key.
        This function replaces each dict with just the value.

        TODO: Add support for lists and maps

        :param item: DynamoDB Item
        """
        flattened = dict()
        for k, v in item.items():
            value = list(v.items())[0]
            if value[0] == 'NULL':
                result = None
            else:
                result = value[1]
            flattened[k] = result
        return flattened

    def _add_type_to_item_values(self, item):
        """
        Convert each value of the given dict into a dict where the key is a type and the value
        is the existing value

        TODO: Add support for sets, maps, and lists
        """
        with_types = dict()
        for k, v in item.items():
            if isinstance(v, str):
                value_type = 'S'
            elif isinstance(v, int) or isinstance(v, float):
                value_type = 'N'
            elif isinstance(v, bytes):
                value_type = 'B'
            elif isinstance(v, bool):
                value_type = 'BOOL'
            elif v is None:
                value_type = 'NULL'
            else:
                raise ValueError('Type must be one of str, byte, number, None, or bool')
            with_types[k] = {value_type: v}
        return with_types

    # TODO: Do we need this method? There are ,more complex args that need to be added like secondary indexes
    def create_table(self, table_name, keys, attributes, read_capacity=1, write_capacity=1):
        """
        Create table in DynamoDB

        :param table_name: Name of table to create
        :param keys: Attributes that make up the primary key;
            Is a dict where key is attribute name and value is key type (HASH or RANGE)
            e.g. {'user_id': 'HASH', 'name': 'RANGE'}
        :param attributes: Attributes of the table (must include attributes in the primary key)
            Is a dict where the key is the attribute name and value is the attribute type
            e.g. {'user_id': 'S', 'name': 'S'}
        :param read_capacity: DynamoDB read capacity units to allocate
        :param write_capacity: DynamoDB write capacity units to allocate
        :return:
        """
        return self.dynamo_client.create_table(
            TableName=table_name,
            KeySchema=[{'AttributeName': attribute_name, 'KeyType': key_type}
                       for attribute_name, key_type in keys.items()],
            AttributeDefinitions=[{'AttributeName': attribute_name, 'AttributeType': attribute_type}
                                  for attribute_name, attribute_type in attributes.items()],
            ProvisionedThroughput={
                'ReadCapacityUnits': read_capacity,
                'WriteCapacityUnits': write_capacity
            }
        )['TableDescription']

    def query(self, table_name, key_conditions, filters=None, index_name=None):
        """
        Make query and return a formatted list of items

        :param table_name: DynamoDB table to query
        :param key_conditions: Primary key conditions to query for
            This is a dict with format {key1: value1, key2: value2} where the resulting query will be
            'key1 = value1 AND key2 = value2'
        :param filters: Optional conditions on non-primary key attributes
            Follow the same format as key conditions
        :param index_name: Name of secondary index to use; Use None to use primary index
        :return: List of the query results
        """
        expression_values = dict()
        query_params = dict()

        key_condition_expression = []

        for i, (attribute, value) in enumerate(key_conditions.items()):
            expression_name = f':k{i}'
            expression_values[expression_name] = {'S': value}
            key_condition_expression.append(f'{attribute} = {expression_name}')
        query_params['KeyConditionExpression'] = ' AND '.join(key_condition_expression)

        if filters:
            filter_expression = []
            for i, (attribute, value) in enumerate(filters.items()):
                expression_name = f':v{i}'
                expression_values[expression_name] = {'S': value}
                filter_expression.append(f'{attribute} = {expression_name}')
            query_params['FilterExpression'] = ' AND '.join(filter_expression)

        query_params['ExpressionAttributeValues'] = expression_values

        if index_name is not None:
            query_params['IndexName'] = index_name

        query_result = self.dynamo_client.query(
            TableName=table_name,
            **query_params
        ).get('Items')

        return [self._flatten_item(item) for item in query_result]

    def get_item(self, table_name, keys):
        """
        :param table_name: Table to get from
        :param keys: Primary key conditions to find
            This is a dict with format {key1: value1, key2: value2}
        :return: Item if found, otherwise None
        """
        item = self.dynamo_client.get_item(
                TableName=table_name,
                Key=self._add_type_to_item_values(keys)).get('Item')
        if item is None:
            return None
        return self._flatten_item(item)

    def insert_item(self, table_name, item):
        """
        Insert item into the given table.  If there was previously an item with the same primary key,
        it is replaced and the previous value is returned.

        :param table_name: DynamoDB table to put item
        :param item: Dict where a key is an attribute and a value is the attribute value
        :return: Previous item with the given primary key if it exists, otherwise None
        """
        return self.dynamo_client.put_item(
            TableName=table_name,
            Item=self._add_type_to_item_values(item),
            ReturnValues='ALL_OLD')

    def delete_item(self, table_name, keys):
        """
        :param table_name: Table to delete from
        :param keys: Primary key conditions to find and delete
            This is a dict with format {key1: value1, key2: value2}
        :return: Deleted item
        """
        return self.dynamo_client.delete_item(
            TableName=table_name,
            Key=self._add_type_to_item_values(keys),
            ReturnValues='ALL_OLD')
