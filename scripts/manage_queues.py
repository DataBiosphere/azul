import argparse
import logging
import sys

from azul.logging import (
    configure_script_logging,
)
from azul.queues import (
    Queues,
)

logger = logging.getLogger(__name__)


def main(argv):
    configure_script_logging(logger)
    p = argparse.ArgumentParser(description='Manage the SQS queues in the current deployment')
    sps = p.add_subparsers(help='sub-command help', dest='command')

    sps.add_parser('list',
                   help='List SQS queues in current deployment')

    sp = sps.add_parser('dump',
                        help='Dump contents of queue into designated file')
    sp.add_argument('queue', metavar='QUEUE_NAME',
                    help='Name of the queue to obtain messages from')
    sp.add_argument('path', metavar='FILE_PATH',
                    help='Path of file to write messages to')
    sp.add_argument('--delete', '-D', action='store_true',
                    help='Remove messages from the queue after writing them to the specified file. By default the '
                         'messages will be returned to the queue')
    sp.add_argument('--no-json-body', '-J', dest='json_body', action='store_false',
                    help='Do not deserialize JSON in queue message body.')

    sp = sps.add_parser('feed', help='Feed messages from file back into queue')
    sp.add_argument('path', metavar='FILE_PATH',
                    help='Path of file to read messages from')
    sp.add_argument('queue', metavar='QUEUE_NAME',
                    help='Name of the queue to feed messages into')
    sp.add_argument('--force', '-F', action='store_true',
                    help='Force feeding messages to a queue they did not originate from.')
    sp.add_argument('--delete', '-D', action='store_true',
                    help='Remove messages from the file after submitting them to the specified queue. By default '
                         'the messages will remain in the file')

    sp = sps.add_parser('purge',
                        help='Purge all messages in a queue')
    sp.add_argument('queue', metavar='QUEUE_NAME',
                    help='Name of the queue to purge.')

    sps.add_parser('purge_all',
                   help='Purge all messages in all queues in the current deployment. Use with caution. The '
                        'messages will be lost forever.')

    sp = sps.add_parser('dump_all',
                        help='Dump all messages in all queues in the current deployment. Each queue will be '
                             'dumped into a separate JSON file. The name of the JSON file is the name of '
                             'the queue followed by ".json"')
    sp.add_argument('--delete', '-D', action='store_true',
                    help='Remove messages from each queue after writing them to the its file. By default the '
                         'messages will be returned to the queue')
    sp.add_argument('--no-json-body', '-J', dest='json_body', action='store_false',
                    help='Do not deserialize JSON in queue message body.')

    args = p.parse_args(argv)

    if args.command in ('list', 'purge', 'purge_all'):
        queues = Queues()
        if args.command == 'list':
            queues.list()
        elif args.command == 'purge':
            queues.purge(args.queue)
        elif args.command == 'purge_all':
            queues.purge_all()
        else:
            assert False, args.command
    elif args.command in ('dump', 'dump_all'):
        queues = Queues(delete=args.delete, json_body=args.json_body)
        if args.command == 'dump':
            queues.dump(args.queue, args.path)
        elif args.command == 'dump_all':
            queues.dump_all()
        else:
            assert False, args.command
    elif args.command == 'feed':
        queues = Queues(delete=args.delete)
        queues.feed(args.path, args.queue, force=args.force)
    else:
        p.print_usage()


if __name__ == '__main__':
    main(sys.argv[1:])
