#! usr/bin/env python3


def dos_object_url(file_id: str) -> str:
    """
    Return a DOS URL for an input file ID
    """
    dos_endpoint = '/indexer.dev.explore.data.humancellatlas.org'
    return f'{dos_endpoint}/{file_id}'
