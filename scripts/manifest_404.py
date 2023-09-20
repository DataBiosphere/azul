from concurrent.futures.thread import (
    ThreadPoolExecutor,
)
import json
import logging
import random
import time

from furl import (
    furl,
)
from more_itertools import (
    one,
)
import requests

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.json_freeze import (
    freeze,
    thaw,
)
from azul.logging import (
    configure_script_logging,
)

logger = logging.getLogger(__name__)


def clear_cached_manifests():
    s3 = aws.resource('s3')
    bucket = s3.Bucket(config.s3_bucket)
    logger.debug(f'deleting bucket contents under {config.s3_bucket}/manifests/')
    bucket.objects.filter(Prefix='manifests/').delete()
    logger.debug('deletion complete')


organTypes = [
    'brain',
    'heart',
    'adipose tissue',
    'pancreas',
    'bone tissue',
    'lymph node',
    'tumor',
    'blood',
    'embryo',
    'large intestine',
    'skin of body',
    'lung',
    'thymus',
    'bladder organ',
    'kidney',
    'spleen',
    'decidua',
    'liver',
    'muscle organ',
    'tongue',
    'trachea',
    'diaphragm',
    'mammary gland',
    'colon',
    'immune system'
]


def filter_projects():
    # Tabula Muris causes 500 errors due to memory constraints
    # https://github.com/DataBiosphere/azul/issues/2442
    url = config.service_endpoint.set(path='/index/projects')
    resp = requests.get(url=str(url), params=dict(size=1000))
    projects = resp.json()['termFacets']['project']['terms']
    return [one(project['projectId']) for project in projects if project['term'] != 'Tabula Muris']


project_ids = filter_projects()


def generate_filters():
    num_organs = random.randint(1, len(organTypes) - 1)
    return {
        'organ': {
            'is': sorted(random.sample(organTypes, k=num_organs))},
        'projectId': {'is': project_ids}
    }


def request_manifest(filters):
    url = config.service_endpoint.set(path='fetch/manifest/files')
    params = {
        'catalog': 'dcp2',
        'filters': json.dumps(filters),
        'format': 'terra.bdbag'
    }

    logger.debug(params)
    resp = requests.get(url=str(url), params=params)
    body = resp.json()
    assert body['Status'] == 301, body['Status']

    while True:
        url = furl(body['Location'])
        logger.debug(f'location is {url}')
        time.sleep(0.5)
        resp = requests.get(url=str(url))
        body = resp.json()
        if body['Status'] != 301:
            break

    assert body['Status'] == 302, body['Status']
    url = body['Location']
    logger.debug(f'location is {url}')
    return url


def threaded_manifests():
    def full_run(filters):
        url = request_manifest(thaw(filters))
        resp = requests.get(url)
        return resp.status_code

    filterss = set(freeze(generate_filters()) for _ in range(140))
    with ThreadPoolExecutor(10) as main_tpe:
        return set(main_tpe.map(full_run, filterss))


if __name__ == '__main__':
    # prefixes 41, 42, 51 were indexed before running
    configure_script_logging(logger)
    clear_cached_manifests()
    manifests = threaded_manifests()
    assert {200} == manifests, manifests
