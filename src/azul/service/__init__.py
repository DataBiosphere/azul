import ast
import logging

from typing import Optional

from azul.service.responseobjects.elastic_request_builder import BadArgumentException

logger = logging.getLogger(__name__)


class AbstractService:

    def parse_filters(self, filters: Optional[str]):
        """
        Parses filters. Handles default cases where filters are None (not set) or {}
        :param filters: string of python interpretable data
        :raises ValueError: Will raise a ValueError if token is misformatted or invalid
        :return: python literal
        """
        default_filters = {'file': {}}
        if filters is None:
            return default_filters
        try:
            filters = ast.literal_eval(filters)
        except ValueError as e:
            logger.error('Malformed filters parameter: {}'.format(e))
            # FIXME: should this be a new kind of exception??
            raise BadArgumentException('Malformed filters parameter')
        else:
            return default_filters if filters == {} else filters
