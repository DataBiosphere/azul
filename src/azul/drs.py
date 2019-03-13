#! usr/bin/env python3


def dos_object_url(file_id: str) -> str:
    """
    Return a DOS URL for an input file ID
    """
    dos_endpoint = '/ga4gh/dos/v1/dataobjects'
    return f'{dos_endpoint}/{file_id}'
