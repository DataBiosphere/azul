import argparse
import json

from azul.maintenance import (
    MaintenanceService,
    MaintenanceEvent
)

from datetime import datetime, timedelta


def parse_duration(duration_str):
    # TODO, Code to parse the duration string (e.g., "1h30m", "2d")
    #  and return a timedelta object
    return 'TODO'


def main():
    parser = argparse.ArgumentParser(description="Maintenance Schedule CLI")
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

    start_parser = subparsers.add_parser("start",
                                         help="Activate a pending event")
    start_parser.add_argument("--index", type=int, required=True,
                              help="Index of the event to start")

    end_parser = subparsers.add_parser("end", help="Complete the active event")

    adjust_parser = subparsers.add_parser("adjust",
                                          help="Modify the active event")
    adjust_parser.add_argument("--duration", required=True,
                               help="New event duration (e.g., '1h30m', '2d')")

    args = parser.parse_args()

    event_service = MaintenanceService()

    if args.command == "list":
        events = event_service.list_upcoming_events(args.all)
        print(json.dumps(events, indent=4))

    elif args.command == "add":
        event = MaintenanceEvent.from_args(args)
        event_service.add_event(event)

    elif args.command == "cancel":
        event_service.cancel_event(args.index)

    elif args.command == "start":
        event_service.start_event(args.index)

    elif args.command == "end":
        event_service.end_event()

    elif args.command == "adjust":
        new_duration = parse_duration(args.duration)
        event_service.adjust_event(new_duration)


if __name__ == "__main__":
    main()
