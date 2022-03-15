from typing import (
    Any,
    List,
    NamedTuple,
    Optional,
)

from boto3.dynamodb.conditions import (
    And,
    Attr,
    Key,
)

from azul.deployment import (
    aws,
)


class DynamoDataAccessor:
    """
    Data access abstraction layer between the webservice and DynamoDB
    """

    def __init__(self, endpoint_url=None, region_name=None):
        self.dynamo_client = aws.dynamodb_resource(endpoint_url, region_name)

    def get_table(self, table_name):
        return self.dynamo_client.Table(table_name)

    def make_query(self, table_name, key_conditions,
                   filters=None,
                   index_name=None,
                   select=None,
                   limit=None,
                   consistent_read: bool = False,
                   exclusive_start_key=None):
        """
        Make a query and get results one page at a time.  This method handles the pagination logic so the caller can
        process each page at a time without having to re-query

        :param table_name: DynamoDB table to query
        :param key_conditions: Primary key conditions to query for
            This is a dict with format {key1: value1, key2: value2} where the resulting query will be
            'key1 = value1 AND key2 = value2'
        :param filters: Optional conditions on non-primary key attributes
            Follow the same format as key conditions
        :param index_name: Name of secondary index to use; Use None to use primary index
        :param select: The list of selected attributes
        :param limit: Maximum number of items per query (used for testing pagination)
        :param consistent_read: Flag for consistent read
        :param exclusive_start_key: DynamoDB exclusive start key
        :yield: results of a single query call
        """
        query_params = dict(ConsistentRead=consistent_read)

        key_expression = [Key(attr).eq(value) for attr, value in key_conditions.items()]
        if len(key_conditions) == 0:
            raise ValueError('At least one key condition must be given')
        elif len(key_conditions) > 1:
            query_params['KeyConditionExpression'] = And(*key_expression)
        else:
            query_params['KeyConditionExpression'] = key_expression[0]

        if filters is not None:
            filter_expression = [Attr(attr).eq(value) for attr, value in filters.items()]
            if len(filter_expression) > 1:
                query_params['FilterExpression'] = And(*filter_expression)
            else:
                query_params['FilterExpression'] = filter_expression[0]

        if index_name is not None:
            query_params['IndexName'] = index_name

        if select:
            query_params['Select'] = 'SPECIFIC_ATTRIBUTES'
            query_params['ProjectionExpression'] = ', '.join(select)

        if limit is not None:
            query_params['Limit'] = limit

        if index_name is not None:
            query_params['IndexName'] = index_name

        if exclusive_start_key is not None:
            query_params['ExclusiveStartKey'] = exclusive_start_key

        while True:
            query_result = self.get_table(table_name).query(**query_params)
            last_evaluated_key = query_result.get('LastEvaluatedKey')
            yield Page(query_result.get('Items'), query_result.get('Count'), last_evaluated_key)
            if last_evaluated_key is None:
                break
            query_params['ExclusiveStartKey'] = last_evaluated_key

    def query(self, **kwargs):
        """
        Query the table based on the key conditions (can be hash and range key or just hash) and return
        results filtered by the given filters
        Parameters match those in _make_query()

        Arguments documented in _make_query

        :yields: Results of the query
        """
        for page in self.make_query(**kwargs):
            for item in page.items:
                yield item

    def count(self, **kwargs):
        """
        Return the number of items in the given index that match the key conditions and filter
        Parameters match those in _make_query()

        Arguments documented in _make_query

        :return number of matching items in the index
        """
        return sum([page.count for page in self.make_query(**kwargs)])

    def insert_item(self, table_name, item):
        """
        Insert item into the given table.  If there was previously an item with the same primary key,
        it is replaced and the previous value is returned.

        :param table_name: DynamoDB table to put item
        :param item: Dict where a key is an attribute and a value is the attribute value
        :return: Previous item with the given key; if no previous item, return none
        """
        return self.get_table(table_name).put_item(
            Item=item,
            ReturnValues='ALL_OLD'
        ).get('Attributes')

    def get_item(self, table_name, keys):
        """
        :param table_name: Table to get from
        :param keys: Primary key conditions to find
            This is a dict with format {key1: value1, key2: value2}
        :return: Item if found, otherwise None
        """
        return self.get_table(table_name).get_item(Key=keys).get('Item')

    def delete_item(self, table_name, keys):
        """
        :param table_name: Table to delete from
        :param keys: Primary key conditions to find and delete
            This is a dict with format {key1: value1, key2: value2}
        :return: Deleted item, or None if the item was not found
        """
        return self.get_table(table_name).delete_item(
            TableName=table_name,
            Key=keys,
            ReturnValues='ALL_OLD'
        ).get('Attributes')

    def update_item(self, table_name, keys, update_values, conditions=None):
        """
        :param table_name: Table to update
        :param keys: Primary key of the item to update
            This is a dict with format {key1: value1, key2: value2}
        :param update_values: Attributes in the item to update.  Attributes can be existing or new.
            This is a dict where key is an attribute name and value is the value to assign
        :param conditions: Values that must match in order to perform the update
            e.g. condition={'a': 1} means that attribute 'a' must be equal to 1 to perform update
        :return: Updated item, or None if item was not found
        """
        expression_params = dict()
        if len(update_values) > 0:  # allow update even if no values are given
            expression_attribute_values = {}
            expression_terms = []
            for attr, value in update_values.items():
                expression_attribute_values[f':{attr}'] = value
                expression_terms.append(f'{attr} = :{attr}')
            expression_params['UpdateExpression'] = 'SET ' + ', '.join(expression_terms)
            expression_params['ExpressionAttributeValues'] = expression_attribute_values
        if conditions:
            condition_terms = [Attr(key).eq(value) for key, value in conditions.items()]
            if len(condition_terms) > 1:
                expression_params['ConditionExpression'] = And(*condition_terms)
            else:
                expression_params['ConditionExpression'] = condition_terms[0]
        try:
            return self.get_table(table_name).update_item(
                TableName=table_name,
                Key=keys,
                ReturnValues='ALL_NEW',
                **expression_params
            ).get('Attributes')
        except self.dynamo_client.meta.client.exceptions.ConditionalCheckFailedException:
            raise ConditionalUpdateItemError(table_name, keys, update_values)

    def batch_write(self, table_name, items):
        """
        Write a batch of items to dynamo.  batch_writer will handle retries and batching

        :param table_name: Table to write to
        :param items: List of dicts where each dict is an item
        """
        with self.get_table(table_name).batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)

    def delete_by_key(self, table_name, key_conditions):
        """
        Delete all items matching the key conditions.  If table has a composite key, it is possible to delete
        by just partition key.

        :param table_name: Table to delete from
        :param key_conditions: Primary key conditions to find and delete
            This is a dict with format {key1: value1, key2: value2}
        :return: the number of items deleted
        """
        table = self.get_table(table_name)
        select = [schema['AttributeName'] for schema in table.key_schema]
        items_to_delete = self.query(table_name=table_name, key_conditions=key_conditions, select=select)
        delete_count = 0
        with table.batch_writer() as batch:
            for item in items_to_delete:
                delete_count += 1
                batch.delete_item(Key=item)
        return delete_count


class Page(NamedTuple):
    items: List[Any]
    count: int
    last_evaluated_key: Optional[str]


class ConditionalUpdateItemError(RuntimeError):
    pass
