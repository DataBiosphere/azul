import logging
from typing import (
    Optional,
    Union,
)

from google.cloud.bigquery_reservation_v1 import (
    Assignment,
    CapacityCommitment,
    Reservation,
    ReservationServiceClient,
)
from google.cloud.bigquery_reservation_v1.services.reservation_service.pagers import (
    ListAssignmentsPager,
    ListCapacityCommitmentsPager,
    ListReservationsPager,
)
from google.oauth2.service_account import (
    Credentials,
)
from more_itertools import (
    one,
)

from azul import (
    cached_property,
    config,
    require,
)
from azul.deployment import (
    aws,
)

log = logging.getLogger(__name__)


class BigQueryReservation:
    _reservation_id = 'default'

    _rest_api_url = 'https://content-bigqueryreservation.googleapis.com/v1/'

    _http_scopes = ['https://www.googleapis.com/auth/bigquery']

    _path_suffixes = {
        'capacity_commitment': '',
        'reservation': '',
        'assignment': '/reservations/-'
    }

    reservation: Optional[Reservation]
    assignment: Optional[Assignment]
    location: str

    def __init__(self,
                 *,
                 location: str = config.tdr_source_location,
                 slots: int = config.bigquery_reserved_slots,
                 dry_run: bool = False):
        """
        :param dry_run: If true, methods will not create/update/destroy any
                        cloud resources.
        """
        self.location = location
        self.slots = slots
        self.dry_run = dry_run
        self.refresh()

    def refresh(self):
        for resource_type, path_suffix in self._path_suffixes.items():
            self._refresh(resource_type)

    def _refresh(self, resource_type):
        pager_method = getattr(self._client, f'list_{resource_type}s')
        path_suffix = self._path_suffixes[resource_type]
        pager = pager_method(parent=self._reservation_parent_path + path_suffix)
        setattr(self, f'{resource_type}', self._single_resource(pager))

    @cached_property
    def credentials(self) -> Credentials:
        with aws.service_account_credentials(config.ServiceAccount.indexer) as file_name:
            credentials = Credentials.from_service_account_file(file_name)
        return credentials.with_scopes(self._http_scopes)

    @cached_property
    def _client(self) -> ReservationServiceClient:
        return ReservationServiceClient(credentials=self.credentials)

    @property
    def _project(self) -> str:
        return self.credentials.project_id

    @property
    def _reservation_parent_path(self) -> str:
        return self._client.common_location_path(project=self._project,
                                                 location=self.location)

    @property
    def is_active(self) -> Optional[bool]:
        resource_statuses = {
            self.reservation is not None,
            self.assignment is not None
        }
        try:
            return one(resource_statuses)
        except ValueError:
            return None

    @property
    def update_time(self) -> Optional[float]:
        """
        The time at which the current Reservation was updated as a Unix
        timestamp, or None if is there is no Reservation.
        """
        if self.reservation is None:
            return None
        else:
            return self.reservation.update_time.timestamp()

    def activate(self) -> None:
        self._create_reservation()
        self._assign_slots()
        self.refresh()
        if not self.dry_run:
            if not self.is_active:
                raise RuntimeError('Failed to activate slots')
            if self.reservation.slot_capacity < self.slots:
                raise RuntimeError('Failed to acquire enough slots',
                                   self.reservation.slot_capacity,
                                   self.slots)

    def _create_reservation(self) -> None:
        """
        Idempotently create reservation.
        """
        self._refresh('reservation')
        if self.reservation is None:
            reservation = Reservation(dict(slot_capacity=self.slots,
                                           ignore_idle_slots=False))
            if self.dry_run:
                log.info('Would reserve %d BigQuery slots in location %r, reservation ID: %r',
                         reservation.slot_capacity, self.location, self._reservation_id)
            else:
                log.info('Reserving %d BigQuery slots in location %r, reservation ID: %r',
                         reservation.slot_capacity, self.location, self._reservation_id)
                reservation = self._client.create_reservation(reservation=reservation,
                                                              reservation_id=self._reservation_id,
                                                              parent=self._reservation_parent_path)
                log.info('Reserved %d BigQuery slots in location %r, reservation name: %r',
                         reservation.slot_capacity, self.location, reservation.name)
                self.reservation = reservation
        else:
            current_capacity = self.reservation.slot_capacity
            log.info('Reservation with capacity %d already created in location %r',
                     current_capacity, self.location)
            if current_capacity < self.slots:
                log.info('Capacity deficit is %d', self.slots - current_capacity)
                if self.dry_run:
                    log.info('Would increase reservation capacity to %d', self.slots)
                else:
                    log.info('Increasing reservation capacity to %d', self.slots)
                    self.reservation.slot_capacity = self.slots
                    self.reservation = self._client.update_reservation(
                        reservation=self.reservation,
                        update_mask='slotCapacity'
                    )
                    log.info('Reservation now has capacity %d', self.reservation.slot_capacity)

    def _assign_slots(self) -> None:
        """
        Idempotently assign capacity commitment to a reservation.
        """
        self._refresh('assignment')
        if self.assignment is not None:
            log.info('Slots already assigned in location %r',
                     self.location)
        else:
            assignment = Assignment(dict(assignee=f'projects/{self._project}',
                                         job_type=Assignment.JobType.QUERY))
            if self.dry_run:
                reservation_name = None if self.reservation is None else self.reservation.name
                log.info('Would assign slots to reservation %r in location %r',
                         reservation_name, self.location)
            else:
                require(self.reservation is not None)
                log.info('Assigning slots to reservation %r in location %r',
                         self.reservation.name, self.location)
                assignment = self._client.create_assignment(parent=self.reservation.name,
                                                            assignment=assignment)
                log.info('Assigned slots in location %r, assignment name: %r',
                         self.location, assignment.name)
                self.assignment = assignment

    def deactivate(self) -> None:
        """
        Idempotently delete all resources.
        """
        for resource_type in ('assignment', 'reservation', 'capacity_commitment'):
            resource = getattr(self, resource_type)
            if resource is None:
                log.info('%r does not exist in location %r',
                         resource_type, self.location)
            else:
                resource_str = f'{resource_type}:{resource.name}'
                if self.dry_run:
                    log.info('Would delete resource %r in location %r',
                             resource_str, self.location)
                else:
                    delete_method = getattr(self._client, 'delete_' + resource_type)
                    delete_method(name=resource.name)
                    log.info('Deleted resource %r in location %r',
                             resource_str, self.location)
        self.refresh()
        # self.is_active is None when some, but not all resources are present
        if not self.dry_run and self.is_active is not False:
            raise RuntimeError(f'Failed to delete slots in location {self.location!r}')

    ResourcePager = Union[
        ListCapacityCommitmentsPager,
        ListReservationsPager,
        ListAssignmentsPager
    ]

    Resource = Union[
        CapacityCommitment,
        Reservation,
        Assignment
    ]

    def _single_resource(self, resources: ResourcePager) -> Optional[Resource]:
        resources = list(resources)
        try:
            resource, *extras = resources
        except ValueError:
            return None
        else:
            require(not extras,
                    'Too many resources in path (should be 0 or 1)',
                    self._reservation_parent_path, resources)
            return resource
