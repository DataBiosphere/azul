from collections import (
    defaultdict,
)
from functools import (
    lru_cache,
)
import gzip
import json
import os
import posixpath
import sys
import tempfile
from urllib.parse import (
    urlparse,
)

import requests

from azul import (
    config,
)
from azul.aws_service_model import (
    ServiceActionType,
)

program_name, _ = os.path.splitext(os.path.basename(__file__))

output_file_path = os.path.join(config.project_root, 'terraform', 'gitlab', 'aws_service_model.json.gz')


@lru_cache(maxsize=1000)
def get(url_path):
    assert url_path.startswith('/')
    cache_file_path = os.path.join(config.project_root, '.cache', program_name, url_path[1:], 'cache.json')
    if os.path.exists(cache_file_path):
        with open(cache_file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        url = 'https://console.aws.amazon.com/iam/api' + url_path
        print(url, file=sys.stderr)
        response = requests.get(url, headers=headers())
        response.raise_for_status()
        response_json = response.json()
        cache_dir_path = os.path.dirname(cache_file_path)
        os.makedirs(cache_dir_path, exist_ok=True)
        f = tempfile.NamedTemporaryFile(mode='w+', dir=cache_dir_path, encoding='utf-8', delete=False)
        try:
            json.dump(response_json, f, indent=4)
        except BaseException:
            os.unlink(f.name)
            raise
        else:
            os.rename(f.name, cache_file_path)
        finally:
            f.close()
        return response_json


@lru_cache(maxsize=1)
def services():
    return {
        service['serviceDisplayName']: {
            'description': service['description'],
            'serviceName': service['serviceName']
        } for service in get('/services')['_embedded'].values()
    }


@lru_cache(maxsize=1)
def actions():
    actions = defaultdict(dict)
    for service_name, service in services().items():
        escaped_service_name = service_name.replace('/', '\u00b6')  # Don't ask me
        service_actions = get(f'/services/{escaped_service_name}/actions')
        for link, action in service_actions['_embedded'].items():
            action_name = service['serviceName'] + ":" + posixpath.basename(urlparse(link).path)
            actions[service_name][action_name] = {
                'resources': action['requiredResources'] + action['optionalResources'],
                'type': ServiceActionType.for_action_groups(set(action['actionGroups']))
            }
    return actions


@lru_cache(maxsize=1)
def resources():
    resources = defaultdict(dict)
    for resource in get('/resources')['_embedded'].values():
        service_name = resource['serviceDisplayName']
        resource_name = resource['resourceName']
        resources[service_name][resource_name] = resource['arn']
    return resources


def model():
    return {
        'resources': resources(),
        'actions': actions(),
        'services': services()
    }


@lru_cache(maxsize=1)
def headers():
    header_file_path = os.path.expanduser(os.path.join('~', '.' + program_name))
    try:
        with open(header_file_path) as f:
            headers = f.read()
    except FileNotFoundError:
        print(f"""
            Open the AWS console in your browser, open the IAM console, open your browser's
            inspect UI and pretend to edit a policy in the IAM console. In the inspect UI,
            select any XHR request to https://console.aws.amazon.com/iam/api and copy the
            request headers. Paste into {header_file_path}. Then run program again. When
            the authentication token expires, you'll get authentication errors and need to
            redo these steps. The headers contain secrets which is why we place this file
            outside the project root.
            """, file=sys.stderr)
        raise
    return dict((k, v)
                for k, _, v in (
                    line.partition(': ')
                    for line in headers.splitlines()
                    if line)
                if v)


def scrape_model():
    with gzip.open(output_file_path, 'wt') as f:
        f.write(json.dumps(model(), indent=4, default=str))


if __name__ == '__main__':
    scrape_model()
