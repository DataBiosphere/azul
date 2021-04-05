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
    RequirementError,
    cached_property,
    logging,
    require,
)
from azul.deployment import (
    aws,
)

log = logging.getLogger(__name__)


class SlotManager:
    slots = 100

    _reservation_id = 'azul-reindex'

    capacity_commitment_name: Optional[str]
    reservation_name: Optional[str]
    assignment_name: Optional[str]

    def __init__(self):
        self.refresh()

    def refresh(self):
        for resource_type, path_suffix in [
            ('capacity_commitment', ''),
            ('reservation', ''),
            ('assignment', '/reservations/-')
        ]:
            pager_method = getattr(self._client, f'list_{resource_type}s')
            pager = pager_method(parent=self._reservation_parent_path + path_suffix)
            setattr(self, f'{resource_type}_name', self._single_resource_name(pager))
        # Verify state
        self.has_active_slots()

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

    def has_active_slots(self) -> bool:
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
            raise RequirementError('BigQuery slot commitment state is '
                                   'inconsistent: some, but not all resources '
                                   'are missing',
                                   resources)

    def ensure_slots_active(self) -> None:
        """
        Idempotently purchase flex slots.
        """
        if self.has_active_slots():
            log.info('Slot commitment already active')
        else:
            self.capacity_commitment_name = self._purchase_commitment().name
            self.reservation_name = self._create_reservation().name
            self.assignment_name = self._create_assignment(self.reservation_name).name

    def ensure_slots_deleted(self) -> None:
        """
        Idempotently delete flex slots.
        """
        if self.has_active_slots():
            for resource_type in ('assignment',
                                  'reservation',
                                  'capacity_commitment'):
                attr_name = resource_type + '_name'
                resource_name = getattr(self, attr_name)
                delete_method = getattr(self._client, 'delete_' + resource_type)
                delete_method(name=resource_name)
                log.info('Deleted resource %r', f'{resource_type}:{resource_name}')
                setattr(self, attr_name, None)
        else:
            log.info('No slot commitment active')

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

    def _purchase_commitment(self) -> CapacityCommitment:
        commitment = CapacityCommitment(dict(slot_count=self.slots,
                                             plan=CapacityCommitment.CommitmentPlan.FLEX))
        commitment = self._client.create_capacity_commitment(capacity_commitment=commitment,
                                                             parent=self._reservation_parent_path)
        log.info('Purchased %d BigQuery slots, commitment name: %r',
                 commitment.slot_count, commitment.name)
        return commitment

    def _create_reservation(self) -> Reservation:
        reservation = Reservation(dict(slot_capacity=self.slots,
                                       ignore_idle_slots=False))
        reservation = self._client.create_reservation(reservation=reservation,
                                                      reservation_id=self._reservation_id,
                                                      parent=self._reservation_parent_path)
        log.info('Reserved %d BigQuery slots, reservation name: %r',
                 reservation.slot_capacity, reservation.name)
        return reservation

    def _create_assignment(self, reservation_name: str) -> Assignment:
        assignment = Assignment(dict(assignee=f'projects/{self._project}',
                                     job_type=Assignment.JobType.QUERY))
        assignment = self._client.create_assignment(parent=reservation_name,
                                                    assignment=assignment)
        log.info('Assigned slots, assignment name: %r', assignment.name)
        return assignment
