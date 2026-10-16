import argparse
import logging
import sys

from azul.logging import (
    configure_script_logging,
)
from azul.queues import (
    Queues,
)

log = logging.getLogger(__name__)


def main(argv):
    configure_script_logging(log)
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
                    help='Permanently remove queued messages after writing them to the file. '
                         'By default the messages will be returned to the queue')

    sp = sps.add_parser('feed',
                        help='Feed messages from file back into queue')
    sp.add_argument('path', metavar='FILE_PATH',
                    help='Path of file to read messages from')
    sp.add_argument('queue', metavar='QUEUE_NAME',
                    help='Name of the queue to feed messages into')
    sp.add_argument('--delete', '-D', action='store_true',
                    help='Remove messages from the file after adding them to the specified queue. '
                         'By default the messages will remain in the file')

    sp = sps.add_parser('redrive',
                        help='Move all messages from one queue to another. '
                             'The source queue must be configured as the dead-letter queue of '
                             'another queue, and both queues must be either FIFO or standard.')
    sp.add_argument('source_queue', metavar='SOURCE_QUEUE_NAME',
                    help='Name of the queue to move messages from')
    sp.add_argument('destination_queue', metavar='DESTINATION_QUEUE_NAME',
                    help='Name of the queue to move messages to')

    sp = sps.add_parser('purge',
                        help='Purge all messages in a queue')
    sp.add_argument('queue', metavar='QUEUE_NAME',
                    help='Name of the queue to purge.')

    sps.add_parser('purge_indexer',
                   help='Purge the queues related to indexing in the current deployment. '
                        'Use with caution. '
                        'The messages will be lost forever.')
    sps.add_parser('purge_mirror',
                   help='Purge the queues related to indexing in the current deployment. '
                        'Use with caution. '
                        'The messages will be lost forever.')
    sps.add_parser('purge_all',
                   help='Purge all messages in all queues in the current deployment. '
                        'Use with caution. '
                        'The messages will be lost forever.')

    sp = sps.add_parser('dump_all',
                        help='Dump all messages in all queues in the current deployment. '
                             'Each queue will be dumped into a separate JSONL file. '
                             'The name of that file is the name of the queue followed by ".jsonl"')
    sp.add_argument('--delete', '-D', action='store_true',
                    help='Permanently remove queued messages after writing them to the file. '
                         'By default the messages will be returned to the queue')

    args = p.parse_args(argv)

    commands = (
        'list',
        'purge',
        'purge_all',
        'purge_indexer',
        'purge_mirror',
        'redrive'
    )
    if args.command in commands:
        queues = Queues()
        if args.command == 'list':
            queues.list()
        elif args.command == 'purge':
            queues.purge(args.queue)
        elif args.command == 'purge_all':
            queues.purge_all()
        elif args.command == 'purge_indexer':
            queues.purge_indexer()
        elif args.command == 'purge_mirror':
            queues.purge_mirror()
        elif args.command == 'redrive':
            queues.redrive(args.source_queue, args.destination_queue)
        else:
            assert False, args.command
    elif args.command in ('dump', 'dump_all'):
        queues = Queues(delete=args.delete)
        if args.command == 'dump':
            queues.dump(args.queue, args.path)
        elif args.command == 'dump_all':
            queues.dump_all()
        else:
            assert False, args.command
    elif args.command == 'feed':
        queues = Queues(delete=args.delete)
        queues.feed(args.path, args.queue)
    else:
        p.print_usage()


if __name__ == '__main__':
    main(sys.argv[1:])
