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

from azul import (
    cached_property,
    logging,
    require,
)
from azul.deployment import (
    aws,
)

log = logging.getLogger(__name__)


class BigQueryReservation:
    slots = 100

    _reservation_id = 'default'

    _path_suffixes = {
        'capacity_commitment': '',
        'reservation': '',
        'assignment': '/reservations/-'
    }

    capacity_commitment_name: Optional[str]
    reservation_name: Optional[str]
    assignment_name: Optional[str]

    def __init__(self, *, dry_run: bool = False):
        """
        :param dry_run: If true, methods will not create/update/destroy any
                        cloud resources.
        """
        self.dry_run = dry_run
        self.refresh()

    def refresh(self):
        for resource_type, path_suffix in self._path_suffixes.items():
            self._refresh(resource_type)

    def _refresh(self, resource_type):
        pager_method = getattr(self._client, f'list_{resource_type}s')
        path_suffix = self._path_suffixes[resource_type]
        pager = pager_method(parent=self._reservation_parent_path + path_suffix)
        setattr(self, f'{resource_type}_name', self._single_resource_name(pager))

    @cached_property
    def credentials(self) -> Credentials:
        with aws.service_account_credentials() as file_name:
            return Credentials.from_service_account_file(file_name)

    @cached_property
    def _client(self) -> ReservationServiceClient:
        return ReservationServiceClient(credentials=self.credentials)

    @property
    def _project(self) -> str:
        return self.credentials.project_id

    @property
    def _reservation_parent_path(self) -> str:
        return self._client.common_location_path(project=self._project,
                                                 location='US')

    @property
    def is_active(self) -> Optional[bool]:
        resources = {
            self.capacity_commitment_name,
            self.reservation_name,
            self.assignment_name
        }
        if resources == {None}:
            return False
        elif None not in resources:
            return True
        else:
            return None

    def activate(self) -> None:
        self._purchase_capacity_commitment()
        self._create_reservation()
        self._assign_slots()
        self.refresh()
        if not self.dry_run and not self.is_active:
            raise RuntimeError('Failed to activate slots')

    def _purchase_capacity_commitment(self) -> None:
        """
        Idempotently purchase capacity commitment.
        """
        self._refresh('capacity_commitment')
        if self.capacity_commitment_name is not None:
            log.info('Slot commitment already purchased')
        else:
            commitment = CapacityCommitment(dict(slot_count=self.slots,
                                                 plan=CapacityCommitment.CommitmentPlan.FLEX))
            if self.dry_run:
                log.info('Would purchase %d BigQuery slots', commitment.slot_count)
            else:
                log.info('Purchasing %d BigQuery slots', commitment.slot_count)
                commitment = self._client.create_capacity_commitment(capacity_commitment=commitment,
                                                                     parent=self._reservation_parent_path)
                log.info('Purchased %d BigQuery slots, commitment name: %r',
                         commitment.slot_count, commitment.name)
                self.capacity_commitment_name = commitment.name

    def _create_reservation(self) -> None:
        """
        Idempotently create reservation.
        """
        self._refresh('reservation')
        if self.reservation_name is not None:
            log.info('Reservation already created')
        else:
            reservation = Reservation(dict(slot_capacity=self.slots,
                                           ignore_idle_slots=False))
            if self.dry_run:
                log.info('Would reserve %d BigQuery slots, reservation ID: %r',
                         reservation.slot_capacity, self._reservation_id)
            else:
                log.info('Reserving %d BigQuery slots, reservation ID: %r',
                         reservation.slot_capacity, self._reservation_id)
                reservation = self._client.create_reservation(reservation=reservation,
                                                              reservation_id=self._reservation_id,
                                                              parent=self._reservation_parent_path)
                log.info('Reserved %d BigQuery slots, reservation name: %r',
                         reservation.slot_capacity, reservation.name)
                self.reservation_name = reservation.name

    def _assign_slots(self) -> None:
        """
        Idempotently assign capacity commitment to a reservation.
        """
        self._refresh('assignment')
        if self.assignment_name is not None:
            log.info('Slots already assigned')
        else:
            assignment = Assignment(dict(assignee=f'projects/{self._project}',
                                         job_type=Assignment.JobType.QUERY))
            if self.dry_run:
                log.info('Would assign slots to reservation %r', self.reservation_name)
            else:
                require(self.reservation_name is not None)
                log.info('Assigning slots to reservation %r', self.reservation_name)
                assignment = self._client.create_assignment(parent=self.reservation_name,
                                                            assignment=assignment)
                log.info('Assigned slots, assignment name: %r', assignment.name)
                self.assignment_name = assignment.name

    def deactivate(self) -> None:
        """
        Idempotently delete all resources.
        """
        for resource_type in ('assignment', 'reservation', 'capacity_commitment'):
            attr_name = resource_type + '_name'
            resource_name = getattr(self, attr_name)
            if resource_name is None:
                log.info('%r does not exist', resource_type)
            else:
                resource_str = f'{resource_type}:{resource_name}'
                if self.dry_run:
                    log.info('Would delete resource %r', resource_str)
                else:
                    delete_method = getattr(self._client, 'delete_' + resource_type)
                    delete_method(name=resource_name)
                    log.info('Deleted resource %r', resource_str)
        self.refresh()
        if not self.dry_run and self.is_active is not False:
            raise RuntimeError('Failed to delete slots')

    ResourcePager = Union[ListCapacityCommitmentsPager,
                          ListReservationsPager,
                          ListAssignmentsPager]

    def _single_resource_name(self, resources: ResourcePager) -> Optional[str]:
        resources = [resource.name for resource in resources]
        try:
            resource_name, *extras = resources
        except ValueError:
            return None
        else:
            require(not extras,
                    'Too many resources in path (should be 0 or 1)',
                    self._reservation_parent_path, resources)
            return resource_name
