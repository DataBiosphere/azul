from datetime import (
    datetime,
)
import logging

log = logging.getLogger(__name__)

version_format = '%Y-%m-%dT%H%M%S.%fZ'


def new_version():
    # FIXME: DeprecationWarning for datetime methods in Python 3.12
    #        https://github.com/DataBiosphere/azul/issues/5953
    return datetime.utcnow().strftime(version_format)


def validate_version(version: str):
    """
    >>> validate_version('2018-10-18T150431.370880Z')
    '2018-10-18T150431.370880Z'

    >>> validate_version('2018-10-18T150431.0Z')
    Traceback (most recent call last):
    ...
    ValueError: ('2018-10-18T150431.0Z', '2018-10-18T150431.000000Z')

    >>> validate_version(' 2018-10-18T150431.370880Z')
    Traceback (most recent call last):
    ...
    ValueError: time data ' 2018-10-18T150431.370880Z' does not match format '%Y-%m-%dT%H%M%S.%fZ'

    >>> validate_version('2018-10-18T150431.370880')
    Traceback (most recent call last):
    ...
    ValueError: time data '2018-10-18T150431.370880' does not match format '%Y-%m-%dT%H%M%S.%fZ'

    >>> validate_version('2018-10-187150431.370880Z')
    Traceback (most recent call last):
    ...
    ValueError: time data '2018-10-187150431.370880Z' does not match format '%Y-%m-%dT%H%M%S.%fZ'
    """
    reparsed_version = datetime.strptime(version, version_format).strftime(version_format)
    if version != reparsed_version:
        raise ValueError(version, reparsed_version)
    return version
