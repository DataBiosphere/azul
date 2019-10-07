from contextlib import contextmanager
from urllib.parse import (
    urlsplit,
    urlunsplit,
)

from locust import (
    HttpLocust,
    TaskSet,
    TaskSequence,
    seq_task,
    task,
)
from gevent.pool import Group


# To run:
#  - make sure locust is installed
#  - `locust -f scripts/locust/service.py`
#  - in browser go to localhost:8089
#
# For more info see https://docs.locust.io/en/stable/


def trim_url(url: str):
    url = list(urlsplit(url))
    url[0] = ''
    url[1] = ''
    return urlunsplit(url)


@contextmanager
def parallel_requests():
    group = Group()
    yield group
    group.join()


class ServiceTaskSet(TaskSet):
    """This is the default page so start here"""

    def on_start(self):
        self.projects_page()

    def projects_page(self):
        with parallel_requests() as group:
            group.spawn(lambda: self.client.get('/repository/summary?filters=%7B%7D'))
            group.spawn(lambda: self.client.get('/repository/projects?filters=%7B%7D&size=15'))

    @task
    def filter_mtx_files(self):
        with parallel_requests() as group:
            group.spawn(lambda: self.client.get('/repository/summary?filters=%7B%22file%22%3A%7B%22fileFormat'
                                                '%22%3A%7B%22is%22%3A%5B%22mtx%22%5D%7D%7D%7D'))
            group.spawn(lambda: self.client.get('/repository/projects?filters=%7B%22file%22%3A%7B%22fileFormat'
                                                '%22%3A%7B%22is%22%3A%5B%22mtx%22%5D%7D%7D%7D'
                                                '&size=15&sort=sampleId&order=desc'))

    @task(3)
    class FilesTaskSet(TaskSequence):
        '''
        Because this subclass of TaskSequence, it represents the sequence of a user type,
        the `@task()` decorator gives a weight to the frequency of a users request.
        Read: https://docs.locust.io/en/stable/writing-a-locustfile.html#tasks-attribute
        '''

        def on_start(self):
            self.files_page()

        def files_page(self):
            with parallel_requests() as group:
                group.spawn(lambda: self.client.get('/repository/summary?filters=%7B%7D'))
                group.spawn(lambda: self.client.get('/repository/files?filters=%7B%7D&size=15'))

        @seq_task(1)
        @task(15)
        def filter_organ_part(self):
            """Select temporal lobe since it's shared between most deployments"""
            with parallel_requests() as group:
                group.spawn(lambda: self.client.get('/repository/summary?filters=%7B%22file%22%3A%7B%22organPart'
                                                    '%22%3A%7B%22is%22%3A%5B%22temporal%20lobe%22%5D%7D%7D%7D'))
                group.spawn(lambda: self.client.get('/repository/files?filters=%7B%22file%22%3A%7B%22organPart'
                                                    '%22%3A%7B%22is%22%3A%5B%22temporal%20lobe%22%5D%7D%7D%7D'
                                                    '&size=15&sort=sampleId&order=desc'))

        @seq_task(2)
        @task(1)
        def download_manifest(self):
            self.client.get('/repository/summary?filters=%7B%22file%22%3A%7B%22organPart%22%3A%7B%22'
                            'is%22%3A%5B%22temporal%20lobe%22%5D%7D%7D%7D')
            export_url = ('/manifest/files?filters=%7B%22file%22%3A%7B%22organPart%22%3A%7B%22is%22%3A%5B%22'
                          'temporal%20lobe%22%5D%7D%2C%22fileFormat%22%3A%7B%22is%22%3A%5B%22fastq.gz%22%2C%22'
                          'bai%22%2C%22bam%22%2C%22csv%22%2C%22results%22%2C%22txt%22%5D%7D%7D%7D&format=bdbag')
            with self.client.get(export_url, catch_response=True, allow_redirects=False) as response:
                # this is necessary because non 2xx response are counted as failures unless specified like this
                if response.status_code == 301 or (200 <= response.status_code < 300):
                    response.success()
            while response.status_code == 301:
                refresh_url = trim_url(response.headers['Location'])
                retry_after = int(response.headers['Retry-After'])
                self._sleep(retry_after)
                with self.client.get(refresh_url, catch_response=True, allow_redirects=False) as response:
                    if response.status_code in [301, 302]:
                        response.success()

        @seq_task(3)
        def stop(self):
            self.interrupt()

    @task
    class SamplesTaskSet(TaskSet):

        def on_start(self):
            self.samples_page()

        def samples_page(self):
            with parallel_requests() as group:
                group.spawn(lambda: self.client.get('/repository/summary?filters=%7B%7D'))
                group.spawn(lambda: self.client.get('/repository/samples?filters=%7B%7D&size=15'))

        @seq_task(1)
        def select_brain(self):
            with parallel_requests() as group:
                group.spawn(lambda: self.client.get('/repository/summary?filters=%7B%22file%22%3A%7B%22organ%22'
                                                    '%3A%7B%22is%22%3A%5B%22brain%22%5D%7D%7D%7D'))
                group.spawn(lambda: self.client.get('/repository/samples?filters=%7B%22file%22%3A%7B%22organ%22'
                                                    '%3A%7B%22is%22%3A%5B%22brain%22%5D%7D%7D%7D'
                                                    '&size=15&sort=sampleId&order=desc'))

        @seq_task(2)
        def next_page_1(self):
            self.client.get('/repository/samples?filters=%7B%22file%22%3A%7B%22organ%22'
                            '%3A%7B%22is%22%3A%5B%22brain%22%5D%7D%7D%7D&size=15'
                            '&sort=sampleId&order=desc&search_after=Q4_DEMO-'
                            'sample_SAMN02797092&search_after_uid=doc'
                            '%23e8dcd716-03d2-4244-a196-b7269b5e5e6f')

        @seq_task(3)
        def next_page_2(self):
            self.client.get('/repository/samples?filters=%7B%22file%22%3A%7B%22organ%22%'
                            '3A%7B%22is%22%3A%5B%22brain%22%5D%7D%7D%7D&size=15'
                            '&sort=sampleId&order=desc&search_after=Q4_DEMO-'
                            'sample_SAMN02797092&search_after_uid=doc'
                            '%23da9bd051-9ce7-4a38-99c1-284112f0f483')

        @seq_task(4)
        def stop(self):
            self.interrupt()


class ServiceLocust(HttpLocust):
    host = 'https://service.integration.explore.data.humancellatlas.org'
    task_set = ServiceTaskSet
    min_wait = 1000
    max_wait = 5000
