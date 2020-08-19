from contextlib import (
    contextmanager,
)
import json
import logging
import random
from urllib.parse import (
    urlsplit,
    urlunsplit,
)

from furl import (
    furl,
)
from gevent.pool import (
    Group,
)
from locust import (
    HttpUser,
    SequentialTaskSet,
    between,
    task,
)
from locust.clients import (
    HttpSession,
)
from more_itertools import one

from azul import (
    config,
)
from azul.logging import (
    configure_script_logging,
)
from azul.types import (
    JSON,
)

# This script simluates a user triggering Azul endpoints via the data browser GUI.
# To run:
#  - `locust -f scripts/locust/service.py`
#  - in browser go to localhost:8089
#
# For more info see https://docs.locust.io/en/stable/

log = logging.getLogger(__name__)
configure_script_logging(log)


def trim_url(url: str):
    url = list(urlsplit(url))
    url[0] = ''
    url[1] = ''
    return urlunsplit(url)


def endpoint(path, **query):
    return furl(path=path, query=query).url


@contextmanager
def parallel_requests():
    group = Group()
    yield group
    group.join()


def browse_page(client: HttpSession, index_name: str, filters: JSON, **extra_index_params):
    filters = json.dumps(filters)
    with parallel_requests() as group:
        group.spawn(lambda: client.get(endpoint('/repository/summary',
                                                filters=filters),
                                       name='/repository/summary'))
        group.spawn(lambda: client.get(endpoint(f'/repository/{index_name}',
                                                filters=filters,
                                                **extra_index_params),
                                       name=f'/repository/{index_name}'))


browser_search_params = dict(size=15, sort='entryId', order='desc')


class MatrixTaskSet(SequentialTaskSet):

    @task
    def projects_start_page(self):
        # By default data browser only shows human data
        browse_page(self.client,
                    'projects',
                    {"genusSpecies": {"is": ["Homo sapiens"]}},
                    size=15,
                    sort='projectTitle',
                    order='asc')

    @task
    def filter_mtx_files(self):
        browse_page(self.client, 'projects', {"fileFormat": {"is": ["mtx"]}}, **browser_search_params)


class ManifestTaskSet(SequentialTaskSet):
    """
    Filter files by organ part and download a BDBag-format manifest.
    """

    # Select islet of Langerhans since it's present in the develop deployment.
    organ_part_filter = {"organPart": {"is": ["islet of Langerhans"]}}
    manifest_file_format_filter = {"fileFormat": {"is": ["fastq.gz", "bai", "bam", "csv", "results", "txt"]}}

    @task
    def start_page(self):
        browse_page(self.client, 'files', {}, size=15)

    @task
    def filter_organ_part(self):
        browse_page(self.client,
                    'files',
                    self.organ_part_filter,
                    **browser_search_params)

    @task
    def download_manifest(self):
        self.client.get(endpoint('/repository/summary', filters=json.dumps(self.organ_part_filter)),
                        name='/repository/summary')
        export_url = endpoint('/manifest/files',
                              filters=json.dumps({**self.organ_part_filter,
                                                  **self.manifest_file_format_filter}),
                              format='bdbag')
        with self.client.get(export_url,
                             name='/manifest/files',
                             catch_response=True,
                             allow_redirects=False) as response:
            # This is necessary because non 2xx response are counted as failures unless specified like this
            if response.status_code == 301 or 200 <= response.status_code < 300:
                response.success()
        while response.status_code == 301:
            refresh_url = trim_url(response.headers['Location'])
            retry_after = int(response.headers['Retry-After'])
            self._sleep(retry_after)
            with self.client.get(refresh_url,
                                 name='/manifest/files',
                                 catch_response=True,
                                 allow_redirects=False) as response:
                if response.status_code in (301, 302):
                    response.success()


class IndexTaskSet(SequentialTaskSet):
    """
    Browse multiple pages of the samples index
    """

    organ_filter = {"organ": {"is": ["brain"]}}

    @task
    def start_page(self):
        browse_page(self.client, 'samples', {}, size=15)

    @task
    def select_brain(self):
        browse_page(self.client, 'samples', self.organ_filter, **browser_search_params)

    @task
    def next_pages(self):
        pagination_params = {}
        for _ in range(2):
            with self.client.get(endpoint('/repository/samples',
                                          filters=json.dumps(self.organ_filter),
                                          **pagination_params,
                                          **browser_search_params),
                                 name='/repository/samples') as response:
                pagination = response.json()['pagination']
                pagination_params = {
                    'search_after': pagination['search_after'],
                    'search_after_uid': pagination['search_after_uid']
                }
                self.wait()


class GA4GHTaskSet(SequentialTaskSet):
    """
    Test DOS endpoints after selecting a file
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dos_uuid = None

    @task
    def choose_file(self):
        with self.client.get(endpoint('/repository/files',
                                      filters=json.dumps({"fileFormat": {"is": ["fastq", "fastq.gz"]}}),
                                      size=15),
                             name='/repository/files') as response:
            hit = random.choice(response.json()['hits'])
            self.dos_uuid = one(hit['files'])['uuid']

    @task
    def dos(self):
        dos_path = '/ga4gh/dos/v1/dataobjects/{file_uuid}'
        self.client.get(dos_path.format(file_uuid=self.dos_uuid), name=dos_path)


class ServiceLocust(HttpUser):
    host = 'https://service.integration.explore.data.humancellatlas.org'
    tasks = {
        MatrixTaskSet: 1,
        GA4GHTaskSet: 1,
        IndexTaskSet: 5,
        ManifestTaskSet: 3,
    }
    wait_time = between(1, 5)  # seconds
