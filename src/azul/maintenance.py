from datetime import (
    UTC,
    datetime,
    timedelta,
)
from enum import (
    Enum,
    auto,
)
import json
from operator import (
    attrgetter,
)
from typing import (
    Iterator,
    Self,
    Sequence,
)

import attrs
from more_itertools import (
    flatten,
    only,
)

from azul import (
    CatalogName,
    JSON,
    cached_property,
    config,
    reject,
    require,
)
from azul.collections import (
    adict,
)
from azul.deployment import (
    aws,
)
from azul.service.storage_service import (
    StorageObjectNotFound,
)
from azul.time import (
    format_dcp2_datetime,
    parse_dcp2_datetime,
)


class MaintenanceImpactKind(Enum):
    partial_responses = auto()
    degraded_performance = auto()
    service_unavailable = auto()


@attrs.define
class MaintenanceImpact:
    kind: MaintenanceImpactKind
    affected_catalogs: list[CatalogName]

    @classmethod
    def from_json(cls, impact: JSON) -> Self:
        return cls(kind=MaintenanceImpactKind[impact['kind']],
                   affected_catalogs=impact['affected_catalogs'])

    def to_json(self) -> JSON:
        return dict(kind=self.kind.name,
                    affected_catalogs=self.affected_catalogs)

    def validate(self):
        require(all(
            isinstance(c, CatalogName) and c for c in self.affected_catalogs),
            'Invalid catalog name/pattern')
        require(all({0: True, 1: c[-1] == '*'}.get(c.count('*'), False)
                    for c in self.affected_catalogs),
                'Invalid catalog pattern')


@attrs.define
class MaintenanceEvent:
    planned_start: datetime
    planned_duration: timedelta
    description: str
    impacts: list[MaintenanceImpact]
    actual_start: datetime | None
    actual_end: datetime | None

    @classmethod
    def from_json(cls, event: JSON) -> Self:
        return cls(planned_start=cls._parse_datetime(event['planned_start']),
                   planned_duration=timedelta(seconds=event['planned_duration']),
                   description=event['description'],
                   impacts=list(map(MaintenanceImpact.from_json, event['impacts'])),
                   actual_start=cls._parse_datetime(event.get('actual_start')),
                   actual_end=cls._parse_datetime(event.get('actual_end')))

    def to_json(self) -> JSON:
        result = adict(planned_start=self._format_datetime(self.planned_start),
                       planned_duration=int(self.planned_duration.total_seconds()),
                       description=self.description,
                       impacts=[i.to_json() for i in self.impacts],
                       actual_start=self._format_datetime(self.actual_start),
                       actual_end=self._format_datetime(self.actual_end))
        return result

    @classmethod
    def _parse_datetime(cls, value: str | None) -> datetime | None:
        return None if value is None else parse_dcp2_datetime(value)

    @classmethod
    def _format_datetime(cls, value: datetime | None) -> str | None:
        return None if value is None else format_dcp2_datetime(value)

    @property
    def start(self):
        return self.actual_start or self.planned_start

    @property
    def end(self):
        return self.actual_end or self.start + self.planned_duration

    def validate(self):
        require(isinstance(self.planned_start, datetime),
                'No planned start', self.planned_start)
        require(self.planned_start.tzinfo == UTC)
        require(isinstance(self.description, str) and self.description,
                'Invalid description', self.description)
        for impact in self.impacts:
            impact.validate()
        reject(self.actual_end is not None and self.actual_start is None,
               'Event has end but no start')
        require(self.start < self.end,
                'Event start is not before end')


@attrs.define
class MaintenanceSchedule:
    events: list[MaintenanceEvent]

    @classmethod
    def from_json(cls, schedule: JSON) -> Self:
        return cls(events=list(map(MaintenanceEvent.from_json, schedule['events'])))

    def to_json(self) -> JSON:
        return dict(events=[e.to_json() for e in self.events])

    def validate(self):
        for e in self.events:
            e.validate()
        starts = set(e.start for e in self.events)
        require(len(starts) == len(self.events),
                'Start times are not distinct')
        # Since starts are distinct, we'll never need the end as a tie-breaker
        intervals = [(e.start, e.end) for e in self.events]
        require(intervals == sorted(intervals),
                'Events are not sorted by start time')
        values = list(flatten(intervals))
        require(values == sorted(values),
                'Events overlap', values)
        reject(len(self._active_events()) > 1,
               'More than one active event')
        require(all(e.actual_start is None for e in self.pending_events()),
                'Active event mixed in with pending ones')

    def pending_events(self) -> list[MaintenanceEvent]:
        """
        Returns a list of pending events in this schedule. The elements in the
        returned list can be modified until another method is invoked on this
        schedule. The modifications will be reflected in ``self.events`` but the
        caller is responsible for ensuring they don't invalidate this schedule.
        """
        for i, e in enumerate(self.events):
            if e.actual_start is None:
                return self.events[i:]
        return []

    def past_events(self) -> list[MaintenanceEvent]:
        return [
            e
            for e in self.events
            if e.actual_end is not None and e.actual_start is not None
        ]

    def active_event(self) -> MaintenanceEvent | None:
        return only(self._active_events())

    def _active_events(self) -> Sequence[MaintenanceEvent]:
        return [
            e
            for e in self.events
            if e.actual_start is not None and e.actual_end is None
        ]

    def _now(self):
        return datetime.now(UTC)

    def add_event(self, event: MaintenanceEvent):
        """
        Add the given event to this schedule unless doing so would invalidate
        this schedule.
        """
        events = self.events
        try:
            self.events = events.copy()
            self.events.append(event)
            self.events.sort(key=attrgetter('start'))
            self.validate()
        except BaseException:
            self.events = events
            raise

    def adjust_event(self, additional_duration: timedelta) -> MaintenanceEvent:
        event = self.active_event()
        reject(event is None, 'No active event')
        event.planned_duration += additional_duration
        self._heal(event, iter(self.pending_events()))
        assert self.active_event() is not None
        return event

    def cancel_event(self, index: int) -> MaintenanceEvent:
        event = self.pending_events()[index]
        self.events.remove(event)
        self.validate()
        return event

    def start_event(self) -> MaintenanceEvent:
        pending = iter(self.pending_events())
        event = next(pending, None)
        reject(event is None, 'No events pending to be started')
        event.actual_start = self._now()
        self._heal(event, pending)
        assert event == self.active_event()
        return event

    def end_event(self) -> MaintenanceEvent:
        event = self.active_event()
        reject(event is None, 'No active event')
        event.actual_end = self._now()
        self._heal(event, iter(self.pending_events()))
        assert self.active_event() is None
        return event

    def _heal(self,
              event: MaintenanceEvent,
              pending: Iterator[MaintenanceEvent]):
        for next_event in pending:
            if next_event.planned_start < event.end:
                next_event.planned_start = event.end
            event = next_event
        self.validate()


class MaintenanceService:

    @property
    def bucket(self):
        return aws.shared_bucket

    @property
    def object_key(self):
        return f'azul/{config.deployment_stage}/azul.json'

    @cached_property
    def client(self):
        return aws.s3

    @property
    def _get_schedule(self) -> JSON:
        try:
            response = self.client.get_object(Bucket=self.bucket,
                                              Key=self.object_key)
        except self.client.exceptions.NoSuchKey:
            raise StorageObjectNotFound
        else:
            return json.loads(response['Body'].read())

    @property
    def get_schedule(self) -> MaintenanceSchedule:
        schedule = self._get_schedule
        schedule = MaintenanceSchedule.from_json(schedule['maintenance']['schedule'])
        schedule.validate()
        return schedule

    def put_schedule(self, schedule: MaintenanceSchedule):
        schedule = schedule.to_json()
        self.client.put_object(Bucket=self.bucket,
                               Key=self.object_key,
                               Body=json.dumps({
                                   "maintenance": {
                                       "schedule": schedule
                                   }
                               }).encode())

    def provision_event(self,
                        planned_start: str,
                        planned_duration: int,
                        description: str,
                        partial: list[str] | None = None,
                        degraded: list[str] | None = None,
                        unavailable: list[str] | None = None) -> MaintenanceEvent:
        """
        Uses the given input parameters to provision a new MaintenanceEvent.
        This new MaintenanceEvent object can then be added as an event to an
        existing schedule. It is primarily used by `add_event` to create and add
        events to the maintenance schedule.
        """
        partial = [{
            'kind': 'partial_responses',
            'affected_catalogs': partial
        }] if partial is not None else []
        degraded = [{
            'kind': 'degraded_performance',
            'affected_catalogs': degraded
        }] if degraded is not None else []
        unavailable = [{
            'kind': 'service_unavailable',
            'affected_catalogs': unavailable
        }] if unavailable is not None else []
        impacts = [*partial, *degraded, *unavailable]
        return MaintenanceEvent.from_json({
            'planned_start': planned_start,
            'planned_duration': planned_duration,
            'description': description,
            'impacts': impacts
        })

    def add_event(self, event: MaintenanceEvent) -> JSON:
        schedule = self.get_schedule
        schedule.add_event(event)
        self.put_schedule(schedule)
        return schedule.to_json()

    def cancel_event(self, index: int) -> JSON:
        schedule = self.get_schedule
        event = schedule.cancel_event(index)
        self.put_schedule(schedule)
        return event.to_json()

    def start_event(self) -> JSON:
        schedule = self.get_schedule
        event = schedule.start_event()
        self.put_schedule(schedule)
        return event.to_json()

    def end_event(self) -> JSON:
        schedule = self.get_schedule
        event = schedule.end_event()
        self.put_schedule(schedule)
        return event.to_json()

    def adjust_event(self, additional_duration: timedelta) -> JSON:
        schedule = self.get_schedule
        event = schedule.adjust_event(additional_duration)
        self.put_schedule(schedule)
        return event.to_json()
