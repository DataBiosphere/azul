import json
import logging

from typing import Optional

logger = logging.getLogger(__name__)


class BadArgumentException(Exception):

    def __init__(self, message):
        super().__init__(message)


class AbstractService:

    def parse_filters(self, filters: Optional[str]):
        """
        Parses filters. Handles default cases where filters are None (not set) or {}
        :param filters: string of python interpretable data
        :raises ValueError: Will raise a ValueError if token is misformatted or invalid
        :return: python literal
        """
        default_filters = {}
        if filters is None:
            return default_filters
        try:
            filters = json.loads(filters or '{}')
        except ValueError as e:
            logger.error('Malformed filters parameter: {}'.format(e))
            raise BadArgumentException('Malformed filters parameter')
        else:
            return default_filters if filters == {} else filters
