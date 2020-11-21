from contextlib import (
    contextmanager,
)
import json
import logging
import os
import random

import attr
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
from more_itertools import (
    one,
)

from azul import (
    Config,
    cached_property,
    require,
)
from azul.logging import (
    configure_script_logging,
)
from azul.types import (
    JSON,
)

# This script simulates a user triggering Azul endpoints via the Data Browser
# GUI.
#
# Usage:
#
#  - Set $azul_locust_catalog to the desired catalog, or leave unset to test the
#    default catalog.
#
#  - Run `locust -f scripts/locust/service.py`
#
#  - In browser go to localhost:8089
#
# For more info see https://docs.locust.io/en/stable/

log = logging.getLogger(__name__)
configure_script_logging(log)


class LocustConfig(Config):

    @cached_property
    def catalog(self) -> str:
        # Locust does not support passing command-line arguments to the script
        catalog = os.environ.get('azul_locust_catalog', self.default_catalog)
        require(catalog in self.catalogs)
        return catalog


config = LocustConfig()


@contextmanager
def parallel_requests():
    group = Group()
    yield group
    group.join()


class UnexpectedResponseError(ValueError):
    pass


class AzulTaskSet(SequentialTaskSet):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def endpoint(self, path: str, **query) -> str:
        return furl(path=path, query=dict(query, catalog=config.catalog)).url


class BrowserTaskSet(AzulTaskSet):
    browser_search_params = dict(size=15, sort='entryId', order='desc')

    def browse_page(self,
                    index_name: str,
                    filters: JSON,
                    **extra_index_params):
        filters = json.dumps(filters)
        with parallel_requests() as group:
            group.spawn(lambda: self.client.get(self.endpoint('/index/summary',
                                                              filters=filters),
                                                name='/index/summary'))
            group.spawn(lambda: self.client.get(self.endpoint(f'/index/{index_name}',
                                                              filters=filters,
                                                              **extra_index_params),
                                                name=f'/index/{index_name}'))


class FileSelectionTaskSet(AzulTaskSet):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metadata_uuid = None
        self.file_uuid = None

    def _choose_file(self, filters: JSON):
        with self.client.get(self.endpoint('/index/files',
                                           filters=json.dumps(filters),
                                           size=15),
                             name='/index/files') as response:
            hit = random.choice(response.json()['hits'])
            self.metadata_uuid = hit['entryId']
            self.file_uuid = one(hit['files'])['uuid']


class FileFetchTaskSet(AzulTaskSet):
    @attr.s(auto_attribs=True, frozen=True, kw_only=True)
    class DownloadResponse:
        """
        Abstraction of /*/files and /fetch/*/files response structures
        """
        status: int
        location: str
        retry_after: int

        @classmethod
        def from_response(cls, response):
            if response.status_code in (301, 302):
                result = cls(status=response.status_code,
                             location=response.headers['Location'],
                             retry_after=int(response.headers.get('Retry-After', 0)))
            elif response.status_code == 200:
                response_json = response.json()
                result = cls(status=response_json['Status'],
                             location=response_json['Location'],
                             retry_after=response_json.get('Retry-After', 0))
            else:
                raise UnexpectedResponseError(response.status_code)
            # This is necessary because non 2xx are counted as failures unless
            # specified like this
            response.success()
            return result

    def download_redirects(self, url: str, name: str):
        while True:
            with self.client.get(url,
                                 name=name,
                                 catch_response=True,
                                 allow_redirects=False) as response:
                try:
                    download_response = self.DownloadResponse.from_response(response)
                except UnexpectedResponseError:
                    # Count as failure, not exception
                    break
            self._sleep(download_response.retry_after)
            url = download_response.location
            if download_response.status == 302:
                break


class IndexTaskSet(BrowserTaskSet):
    """
    Browse multiple pages of the samples index
    """

    organ_filter = {"organ": {"is": ["brain"]}}

    @task
    def start_page(self):
        self.browse_page('samples', {}, size=15)

    @task
    def select_brain(self):
        self.browse_page('samples', self.organ_filter, **self.browser_search_params)

    @task
    def next_pages(self):
        url = self.endpoint('/index/samples',
                            filters=json.dumps(self.organ_filter),
                            **self.browser_search_params)
        for _ in range(2):
            with self.client.get(url, name='/index/samples') as response:
                url = response.json()['pagination']['next']
            if url is None:
                break
            else:
                self.wait()


class MatrixTaskSet(BrowserTaskSet):
    """
    Filter for matrix files using the Data Browser
    """

    @task
    def projects_start_page(self):
        # By default, the Data Browser only shows human data
        self.browse_page('projects',
                         {"genusSpecies": {"is": ["Homo sapiens"]}},
                         size=15,
                         sort='projectTitle',
                         order='asc')

    @task
    def filter_mtx_files(self):
        self.browse_page('projects',
                         {"fileFormat": {"is": ["mtx"]}},
                         **self.browser_search_params)


class ManifestTaskSet(BrowserTaskSet, FileFetchTaskSet):
    """
    Filter files by organ part and download a BDBag-format manifest.
    """

    # Select islet of Langerhans since it's present in the develop deployment.
    organ_part_filter = {"organPart": {"is": ["islet of Langerhans"]}}
    manifest_file_format_filter = {
        "fileFormat": {
            "is": ["fastq.gz", "bai", "bam", "csv", "results", "txt"]
        }
    }

    @task
    def start_page(self):
        self.browse_page('files', {}, size=15)

    @task
    def filter_organ_part(self):
        self.browse_page('files',
                         self.organ_part_filter,
                         **self.browser_search_params)

    @task
    def download_manifest(self):
        self.client.get(self.endpoint('/index/summary',
                                      filters=json.dumps(self.organ_part_filter)),
                        name='/index/summary')
        export_url = self.endpoint('/manifest/files',
                                   filters=json.dumps({**self.organ_part_filter,
                                                       **self.manifest_file_format_filter}),
                                   format='terra.bdbag')
        self.download_redirects(export_url, '/manifest/files')


class RepositoryTaskSet(FileSelectionTaskSet, FileFetchTaskSet):
    """
    Select and download a file using both fetch and non-fetch approaches.
    """

    @task
    def choose_file(self):
        self._choose_file({"fileFormat": {"is": ["fastq", "fastq.gz"]}})

    def _download(self, path: str):
        url = self.endpoint(path.format(file_uuid=self.file_uuid))
        self.download_redirects(url, path)

    @task
    def file(self):
        self._download('/repository/files/{file_uuid}')

    @task
    def fetch_file(self):
        self._download('/fetch/repository/files/{file_uuid}')


class DRSTaskSet(FileSelectionTaskSet):
    """
    Test DRS and DOS endpoints after selecting a file.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert config.is_dss_enabled(config.catalog), config.catalog
        self.drs_access_ids = set()

    @task
    def choose_file(self):
        self._choose_file({"fileFormat": {"is": ["fastq", "fastq.gz"]}})

    @task
    def drs(self):
        drs_path = '/ga4gh/drs/v1/objects/{file_uuid}'
        with self.client.get(drs_path.format(file_uuid=self.metadata_uuid),
                             name=drs_path) as response:
            access_methods = response.json()['access_methods']
        self.drs_access_ids = [
            access_id
            for access_method in access_methods
            if (access_id := access_method.get('access_id')) is not None
        ]

    @task
    def drs_access_ids(self):
        drs_access_path = '/ga4gh/drs/v1/objects/{file_uuid}/access/{access_id}'
        for access_id in self.drs_access_ids:
            self.client.get(drs_access_path.format(file_uuid=self.metadata_uuid,
                                                   access_id=access_id),
                            name=drs_access_path)

    @task
    def dos(self):
        dos_path = '/ga4gh/dos/v1/dataobjects/{file_uuid}'
        self.client.get(dos_path.format(file_uuid=self.file_uuid), name=dos_path)


class ServiceLocust(HttpUser):
    host = config.service_endpoint()
    tasks = {
        MatrixTaskSet: 1,
        RepositoryTaskSet: 1,
        # Our DRS implementation only works for files in the DSS
        DRSTaskSet: int(config.is_dss_enabled(config.catalog)),
        IndexTaskSet: 5,
        ManifestTaskSet: 3,
    }
    wait_time = between(1, 5)  # seconds
