import logging
import requests
from more_itertools import one
import sys
import json
from json.decoder import JSONDecodeError
import argparse

from azul.logging import configure_script_logging

logger = logging.getLogger(__name__)


def get_project_name(document_id):
    url = service_base_url + f'/repository/projects/{document_id}'
    response = requests.get(url)
    project_list = response.json()['projects']

    one(project_list)
    return project_list[0]['projectShortname']


def get_project_bundle_count(project_name):
    url = f'{service_base_url}/repository/files'
    params = {
        'filters': json.dumps({'project': {'is': [project_name]}}),
        'order': 'desc',
        'sort': 'entryId',
        'size': 1000
    }
    found_analysis_bundle_ids = set()
    all_bundle_ids = set()
    page = 0
    while True:
        page += 1
        response = requests.get(url, params=params)
        response_json = response.json()
        hit_list = response_json.get('hits', [])

        for content in hit_list:
            one(content['files'])
            file_type = content['files'][0]['format']

            for bundle in content['bundles']:
                if file_type == 'bam':
                    found_analysis_bundle_ids.add(f"{bundle['bundleUuid']}.{bundle['bundleVersion']}")

                all_bundle_ids.add(f"{bundle['bundleUuid']}.{bundle['bundleVersion']}")

        search_after = response_json['pagination']['search_after']
        search_after_uid = response_json['pagination']['search_after_uid']
        total_entities = response_json['pagination']['total']
        total_pages = response_json['pagination']['pages']

        logger.info(f'All Bundles: {len(all_bundle_ids)}, Analysis Bundles: {len(found_analysis_bundle_ids)}'
                    f' Size: {len(hit_list)} Page: {page}/{total_pages}'
                    f' Total: {total_entities} URL: {response.url}')

        if search_after is None and search_after_uid is None:
            break
        else:
            params['search_after'] = search_after
            params['search_after_uid'] = search_after_uid
    return len(all_bundle_ids), len(found_analysis_bundle_ids)


if __name__ == '__main__':
    configure_script_logging(logger)

    dataset_progress = {
        'prod': {
            'service_url': 'https://service.explore.data.humancellatlas.org',
            'datasets': [
                {
                    'accountable': '5',
                    'dataset_name': 'treutlein',
                    'project_uuid': '2a0faf83-e342-4b1c-bb9b-cf1d1147f3bb',
                    'expected_count': 6
                },
                {
                    'accountable': '9-v2',
                    'dataset_name': 'pancreas6decades',
                    'project_uuid': 'e8642221-4c2c-4fd7-b926-a68bce363c88',
                    'expected_count': 2544
                },
                {
                    'accountable': '3',
                    'dataset_name': 'meyer',
                    'project_uuid': 'cf8439db-fcc9-44a8-b66f-8ffbf729bffa',
                    'expected_count': 7
                },
                {
                    'accountable': '12',
                    'dataset_name': 'peer',
                    'project_uuid': 'fd1d163d-d6a7-41cd-b3bc-9d77ba9a36fe',
                    'expected_count': 14
                },
                {
                    'accountable': '8',
                    'dataset_name': 'neuron_diff',
                    'project_uuid': 'f8880be0-210c-4aa3-9348-f5a423e07421',
                    'expected_count': 1733
                },
                {
                    'accountable': '2',
                    'dataset_name': 'Teichmann-mouse-melanoma',
                    'project_uuid': 'f396fa53-2a2d-4b8a-ad18-03bf4bd46833',
                    'expected_count': 6639
                },
                {
                    'accountable': '10',
                    'dataset_name': 'ido_amit',
                    'project_uuid': '0c7bbbce-3c70-4d6b-a443-1b92c1f205c8',
                    'expected_count': 25
                },
                {
                    'accountable': '11',
                    'dataset_name': 'humphreys',
                    'project_uuid': '1630e3dc-5501-4faf-9726-2e2c0b4da6d7',
                    'expected_count': 7
                },
                {
                    'accountable': '1',
                    'dataset_name': 'Regev-ICA',
                    'project_uuid': '179bf9e6-5b33-4c5b-ae26-96c7270976b8',
                    'expected_count': 254
                },
                {
                    'accountable': '7',
                    'dataset_name': 'EMTAB5061',
                    'project_uuid': '1a0f98b8-746a-489d-8af9-d5c657482aab',
                    'expected_count': 3514
                },
                {
                    'accountable': '6',
                    'dataset_name': 'EGEOD106540',
                    'project_uuid': '0ec2b05f-ddbe-4e5a-b30f-e81f4b1e330c',
                    'expected_count': 2244
                }
            ]
        },
    }

    parser = argparse.ArgumentParser()

    parser.add_argument('--stage', '-s')

    miscounted_bundle_projects = []
    complete_projects = []
    missing_projects = []

    args = parser.parse_args(sys.argv[1:])
    stage_datasets = dataset_progress[args.stage]
    service_base_url = stage_datasets['service_url']
    for dataset in stage_datasets['datasets']:
        accountable_id = dataset['accountable']
        dataset_name = dataset['dataset_name']
        beta_project_id = dataset['project_uuid']
        expected_count = dataset['expected_count']
        logger.info(f'#{accountable_id}')

        try:
            beta_project_name = get_project_name(beta_project_id)
        except JSONDecodeError:
            logger.info('Bundle Not Found')
            missing_projects.append(f'#{accountable_id} {beta_project_id}')
            continue

        total_bundle_count, analysis_bundle_count = get_project_bundle_count(beta_project_name) \
            if beta_project_id else None

        primary_bundle_count = total_bundle_count - analysis_bundle_count

        bundle_log = f'#{accountable_id: >4}:{dataset_name[:10]: <10} {beta_project_id} Primary Bundles:' \
                     f' {total_bundle_count - analysis_bundle_count:<5}/{expected_count:<5}' \
                     f' Analysis Bundles: {analysis_bundle_count}'

        if primary_bundle_count == expected_count:
            complete_projects.append(bundle_log)
        else:
            miscounted_bundle_projects.append(bundle_log)

    miscounted_projects_str = "\n".join(missing_projects)
    complete_projects = "\n".join(complete_projects)
    miscounted_bundle_projects = "\n".join(miscounted_bundle_projects)
    logger.info(f'\n\nComplete Projects:\n{complete_projects}'
                f'\n\nMissing Bundle Projects:\n{miscounted_bundle_projects}'
                f'\n\nMissing Projects:\n{miscounted_projects_str}')
