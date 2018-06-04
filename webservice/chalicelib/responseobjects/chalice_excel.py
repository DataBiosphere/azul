from urllib import quote
import pyexcel_webio as webio
from chalice import Response

"""
    chalice_excel
    ~~~~~~~~~~~~~~~~~~~
    A chalice extension that provides one application programming interface
    to write data in different excel file formats.  Based on 
    https://github.com/pyexcel-webwares/Flask-Excel/blob/master/flask_excel/__init__.py, 
    originally with license header:
    :copyright: (c) 2015-2017 by Onni Software Ltd and its contributors
    :license: New BSD License
"""


def _make_response(content, content_type, status, file_name=None):
    response = Response(content, content_type=content_type, status=status)
    if file_name:
        if isinstance(file_name, unicode):
            file_name = file_name.encode('utf-8')
        url_encoded_file_name = quote(file_name)
        response.headers["Content-Disposition"] = (
                "attachment; filename=%s;filename*=utf-8''%s"
                % (url_encoded_file_name, url_encoded_file_name)
        )
    return response


from pyexcel_webio import (  # noqa
    make_response,
    make_response_from_array,
    make_response_from_dict,
    make_response_from_records,
    make_response_from_book_dict,
    make_response_from_a_table,
    make_response_from_query_sets,
    make_response_from_tables
)


#webio.init_webio(_make_response)

