import argparse
import json
import sys

from azul.portal_service import (
    PortalService,
)


def main(argv):
    parser = argparse.ArgumentParser(
        description='Upload/download portal integrations database with safe concurrency.',
    )
    actions = parser.add_mutually_exclusive_group(required=True)
    action_args = dict(action='store_const', dest='action')
    actions.add_argument('--get', **action_args, const=get, help='Download database')
    actions.add_argument('--put', **action_args, const=put, help='Upload database')
    parser.add_argument('FILE', nargs='?', default='-', help='File to upload download to/upload from. '
                                                             'Omit or use "-" for standard output/input.')
    args = parser.parse_args(argv)
    try:
        args.action(args.FILE)
    except argparse.ArgumentTypeError:
        parser.error(str(sys.exc_info()[1]))


def open_stream(name, mode):
    return argparse.FileType(mode)(name)


def get(filename):
    db = PortalService().read()
    json.dump(db, open_stream(filename, 'w'))


def put(filename):
    db = json.load(open_stream(filename, 'r'))
    PortalService().overwrite(db)


if __name__ == '__main__':
    main(sys.argv[1:])
