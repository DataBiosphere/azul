"""
This is a command line utility for managing announcement of maintenance events.
Reads the JSON from designated bucket, deserializes the model from it, validates
the model, applies an action to it, serializes the model back to JSON and
finally uploads it back to the bucket where the service exposes it. The service
must also validate the model before returning it.
"""
import argparse
from datetime import (
    timedelta,
)
import json
import re
import sys

from azul import (
    require,
)
from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.maintenance import (
    MaintenanceService,
)


def parse_duration(duration: str) -> timedelta:
    """
    >>> parse_duration('1d')
    datetime.timedelta(days=1)
    >>> parse_duration('24 hours')
    datetime.timedelta(days=1)
    >>> parse_duration('.5 Days 12 hours')
    datetime.timedelta(days=1)

    >>> parse_duration('2h20Min')
    datetime.timedelta(seconds=8400)
    >>> parse_duration('1 H 80m')
    datetime.timedelta(seconds=8400)
    >>> parse_duration('140 Minutes')
    datetime.timedelta(seconds=8400)

    >>> parse_duration('2 Days 3hours 4min 5 secs')
    datetime.timedelta(days=2, seconds=11045)
    >>> parse_duration('1d 25h')
    datetime.timedelta(days=2, seconds=3600)
    >>> parse_duration('1m30s')
    datetime.timedelta(seconds=90)

    >>> parse_duration('Bad foo')
    Traceback (most recent call last):
    ...
    azul.RequirementError: Try a duration such as "2d 6hrs", "1.5 Days", or "15m"
    """

    pattern = r'(\d*\.?\d+)\s*(d|h|m|s)'
    matches = re.findall(pattern, duration.lower())
    require(bool(matches), 'Try a duration such as "2d 6hrs", "1.5 Days", or "15m"')
    time_delta = {'days': 0, 'hours': 0, 'minutes': 0, 'seconds': 0}
    for value, unit in matches:
        value = float(value)
        match unit:
            case 'd':
                time_delta['days'] += value
            case 'h':
                time_delta['hours'] += value
            case 'm':
                time_delta['minutes'] += value
            case 's':
                time_delta['seconds'] += value
    return timedelta(**time_delta)


def main(args: list[str]):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=AzulArgumentHelpFormatter)
    subparsers = parser.add_subparsers(dest="command")
    list_parser = subparsers.add_parser("list", help="List events in JSON form")
    list_parser.add_argument("--all", action="store_true",
                             help="Include completed events")
    add_parser = subparsers.add_parser("add", help="Schedule an event")
    add_parser.add_argument("--start", required=True,
                            help="Event start time (ISO format)")
    add_parser.add_argument("--duration", required=True,
                            help="Event duration (e.g., '1h30m', '2d')")
    add_parser.add_argument("--description", required=True,
                            help="Event description")
    add_parser.add_argument("--partial-responses", nargs="+",
                            help="Catalog names for partial responses")
    add_parser.add_argument("--degraded-performance", nargs="+",
                            help="Catalog names for degraded performance")
    add_parser.add_argument("--service-unavailable", nargs="+",
                            help="Catalog names for service unavailability")
    cancel_parser = subparsers.add_parser("cancel",
                                          help="Cancel a pending event")
    cancel_parser.add_argument("--index", type=int, required=True,
                               help="Index of the event to cancel")
    subparsers.add_parser("start", help="Activate a pending event")
    subparsers.add_parser("end", help="Complete the active event")
    adjust_parser = subparsers.add_parser("adjust",
                                          help="Modify the active event")
    adjust_parser.add_argument("--duration", required=True,
                               help="New event duration (e.g., '1h30m', '2d')")

    args = parser.parse_args(args)

    service = MaintenanceService()

    if args.command == "list":
        events = service.get_schedule
        if args.all:
            events = events.to_json()
        else:
            active = events.active_event()
            active = {} if active is None else {'active': active.to_json()}
            pending = events.pending_events()
            pending = {'pending': list(pe.to_json() for pe in pending)}
            events = active | pending
    elif args.command == "add":
        duration = int(parse_duration(args.duration).total_seconds())
        events = service.provision_event(planned_start=args.start,
                                         planned_duration=duration,
                                         description=args.description,
                                         partial=args.partial_responses,
                                         degraded=args.degraded_performance,
                                         unavailable=args.service_unavailable)
        events = service.add_event(events)
    elif args.command == "cancel":
        events = service.cancel_event(args.index)
    elif args.command == "start":
        events = service.start_event()
    elif args.command == "end":
        events = service.end_event()
    elif args.command == "adjust":
        events = service.adjust_event(parse_duration(args.duration))
    else:
        assert False, 'Invalid command'
    print(json.dumps(events, indent=4))


if __name__ == "__main__":
    main(sys.argv[1:])
