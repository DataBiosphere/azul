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
        for property_name, value_with_type in item.items():
            value_type, value = list(value_with_type.items())[0]
            if value_type == 'NULL':
                result = None
            elif value_type == 'N':
                if value.isnumeric():
                    result = int(value)
                else:
                    result = float(value)
            else:
                result = value
            flattened[property_name] = result
        return flattened

    def _add_type_to_item_values(self, item):
        """
        Convert each value of the given dict into a dict where the key is a type and the value
        is the existing value

        TODO: Add support for sets, maps, and lists
        """
        with_types = dict()
        for property_name, value in item.items():
            if isinstance(value, str):
                value_type = 'S'
            elif isinstance(value, bool):
                value_type = 'BOOL'
            elif isinstance(value, int) or isinstance(value, float):
                value_type = 'N'
                value = str(value)
            elif isinstance(value, bytes):
                value_type = 'B'
            elif value is None:
                value_type = 'NULL'
                value = True
            else:
                raise ValueError('Type must be one of str, byte, number, None, or bool')
            with_types[property_name] = {value_type: value}
        return with_types

    def _build_condition_expression(self, values, name_suffix):
        expression_values = dict()
        expression_terms = []

        values = self._add_type_to_item_values(values)

        for i, (attribute, value) in enumerate(values.items()):
            expression_name = f':{name_suffix}{i}'
            expression_values[expression_name] = value
            expression_terms.append(f'{attribute} = {expression_name}')

        return expression_values, expression_terms

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
        query_params = dict()

        expression_values, key_expression_terms = self._build_condition_expression(key_conditions, 'k')
        query_params['KeyConditionExpression'] = ' AND '.join(key_expression_terms)

        if filters:
            filter_expression_values, filter_expression_terms = self._build_condition_expression(filters, 'f')
            query_params['FilterExpression'] = ' AND '.join(filter_expression_terms)
            expression_values.update(filter_expression_values)

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
                Key=self._add_type_to_item_values(keys)
        ).get('Item')
        if item is None:
            return None
        return self._flatten_item(item)

    def insert_item(self, table_name, item):
        """
        Insert item into the given table.  If there was previously an item with the same primary key,
        it is replaced and the previous value is returned.

        :param table_name: DynamoDB table to put item
        :param item: Dict where a key is an attribute and a value is the attribute value
        :return: Previous item with the given key; if no previous item, return none
        """
        previous_item = self.dynamo_client.put_item(
            TableName=table_name,
            Item=self._add_type_to_item_values(item),
            ReturnValues='ALL_OLD'
        ).get('Attributes')
        if previous_item is None:
            return None
        return previous_item or self._flatten_item(previous_item)

    def delete_item(self, table_name, keys):
        """
        :param table_name: Table to delete from
        :param keys: Primary key conditions to find and delete
            This is a dict with format {key1: value1, key2: value2}
        :return: Deleted item, or None if the item was not found
        """
        deleted = self.dynamo_client.delete_item(
            TableName=table_name,
            Key=self._add_type_to_item_values(keys),
            ReturnValues='ALL_OLD'
        ).get('Attributes')
        if deleted is None:
            return None
        return self._flatten_item(deleted)

    def update_item(self, table_name, keys, update_values):
        """
        :param table_name: Table to update
        :param keys: Primary key of the item to update
            This is a dict with format {key1: value1, key2: value2}
        :param update_values: Attributes in the item to update.  Attributes can be existing or new.
            This is a dict where key is an attribute name and value is the value to assign
        :return: Updated item, or None if item was not found
        """
        expression_params = dict()
        if len(update_values) > 0:  # allow update even if no values are given
            expression_values, expression_terms = self._build_condition_expression(update_values, 'v')
            expression_params['ExpressionAttributeValues'] = expression_values
            expression_params['UpdateExpression'] = 'SET ' + ', '.join(expression_terms)
        updated = self.dynamo_client.update_item(
            TableName=table_name,
            Key=self._add_type_to_item_values(keys),
            ReturnValues='ALL_NEW',
            **expression_params).get('Attributes')
        if updated is None:
            return None
        return self._flatten_item(updated)
