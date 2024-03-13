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
import sys
from typing import (
    Iterator,
    Sequence,
)

import attrs
from more_itertools import (
    flatten,
    only,
)

from azul import (
    JSON,
    reject,
    require,
)
from azul.collections import (
    adict,
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
    affected_catalogs: list[str]

    @classmethod
    def from_json(cls, impact: JSON):
        return cls(kind=MaintenanceImpactKind[impact['kind']],
                   affected_catalogs=impact['affected_catalogs'])

    def to_json(self) -> JSON:
        return dict(kind=self.kind.name,
                    affected_catalogs=self.affected_catalogs)

    def validate(self):
        require(all(isinstance(c, str) and c for c in self.affected_catalogs),
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
    def from_json(cls, event: JSON):
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
    def from_json(cls, schedule: JSON):
        return cls(events=list(map(MaintenanceEvent.from_json, schedule['events'])))

    def to_json(self) -> JSON:
        return dict(events=[e.to_json() for e in self.events])

    def validate(self):
        for e in self.events:
            e.validate()
        starts = set(e.start for e in self.events)
        require(len(starts) == len(self.events),
                'Start times are not distinct')
        # Since starts are distinct, we'll never need the end as a tie breaker
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
        returned list can be modified until another method is invoked on this schedule. The
        modifications will be reflected in ``self.events`` but the caller is
        responsible for ensuring they don't invalidate this schedule.
        """
        events = enumerate(self.events)
        for i, e in events:
            if e.actual_start is None:
                return self.events[i:]
        return []

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
