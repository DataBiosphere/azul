import ast
import logging

logger = logging.getLogger(__name__)


class AbstractService:

    def parse_filters(self, filters: str):
        """
        Parses filters. Handles default cases where filters are None (not set) or {}
        :param filters: string of python interpretable data
        :raises ValueError: Will raise a ValueError if token is misformatted or invalid
        :return: python literal
        """
        default_value = {'file': {}}
        if filters is None:
            return default_value
        try:
            filters = ast.literal_eval(filters)
            # FIXME: when if ever is filters == {}
            return default_value if filters == {} else filters
        except ValueError as e:
            logger.error('Malformed filters parameter: {}'.format(e))
            raise ValueError('Malformed filters parameter')
