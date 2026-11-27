from typing import (
    Protocol,
)

from furl import (
    furl,
)

from azul import (
    CatalogName,
)
from azul.lib import (
    mutable_furl,
)


class BadArgumentException(Exception):

    def __init__(self, message):
        super().__init__(message)


class FileUrlFunc(Protocol):

    def __call__(self,
                 *,
                 catalog: CatalogName,
                 file_uuid: str,
                 fetch: bool = True,
                 base_url: furl | None = None,
                 **params: str
                 ) -> mutable_furl:
        """
        Return the URL at which the service serves the file with the given
        UUID.

        :param catalog: the name of the catalog containing the file

        :param file_uuid: the UUID of the file

        :param fetch: True if the URL should point at the fetch variant of the
                      file endpoint, which is better suited for XHR

        :param base_url: the base URL of the returned URL. If None, the base
                         URL of the app is used

        :param params: additional query parameters to append to the URL
        """
        ...
