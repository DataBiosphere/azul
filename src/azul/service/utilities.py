#!/usr/bin/python
import json


def json_pp(json_object):
    """
    Helper method to convert objects into json formatted pretty string
    :param json_object: The object to be converted into pretty string
    :return: A pretty formatted string
    """
    formatted_json = json.dumps(json_object,
                                sort_keys=True,
                                indent=4,
                                separators=(',', ': '))
    return formatted_json
