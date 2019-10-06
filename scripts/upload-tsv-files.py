import argparse
import boto3
from datetime import datetime
from more_itertools import one
import os
import requests
import re
import unicodedata
import sys


class ProjectTSVUploader:

    def __init__(self, deployment):
        deployments = {
            'dev':
                {
                    'service_url': 'https://service.dev.explore.data.humancellatlas.org'
                },
            'prod':
                {
                    'service_url': 'https://service.explore.data.humancellatlas.org'
                }
        }

        self.s3 = boto3.resource('s3')
        self.service_url = deployments[deployment]['service_url']
        self.deployment = deployment

    # Copied function from
    # https://github.com/DataBiosphere/azul/blob/5523064bbe2ffa7e6003d9d26105106229f0f93d/scripts/
    # count-bundles.py#L12-L18
    def _get_project_name(self, document_id):
        base_url = f'{self.service_url}/repository/projects/{document_id}'
        response = requests.get(base_url)
        response.raise_for_status()
        project_list = response.json()['projects']
        one(project_list)
        return project_list[0]['projectShortname']

    def upload_files_to_bucket(self, bucket_name, project_tsv_directory):
        key_prefix = 'projects/'
        for filename in os.listdir(project_tsv_directory):
            filepath = os.path.join(project_tsv_directory, filename)

            if os.path.isfile(filepath):
                key = f'{key_prefix}{filename}'
                obj = self.s3.Object(bucket_name, key)
                project_uuid = os.path.splitext(filename)[0]
                project_name = self._get_project_name(project_uuid)
                file_name = unicodedata.normalize('NFKD', project_name)
                file_name = re.sub(r'[^\w ,.@%&-_()\\[\]/{}]', '_', file_name).strip()
                timestamp = datetime.now().strftime("%Y-%m-%d %H.%M")
                content_disposition = f'attachment;filename="{file_name} {timestamp}.tsv"'
                assert '\\' not in file_name

                with open(filepath, 'rb') as tsv_file:
                    obj.put(Body=tsv_file,
                            ContentDisposition=content_disposition,
                            ContentType='text/tab-separated-values; charset=UTF-8')
                obj.Acl().put(AccessControlPolicy={
                    'Owner': {
                        'DisplayName': 'czi-aws-admins+humancellatlas',
                        'ID': '76fe35006be54bbb55cf619bf94684704f14362141f2422a19c3af23e080a148'
                    },
                    'Grants': [
                        *([{
                            'Grantee': {
                                'DisplayName': 'czi-aws-admins+hca-prod',
                                'ID': 'c4fcdac74aefb7a038077a810202b6318b8057e20b16e432724345a0a13335f4',
                                'Type': 'CanonicalUser'
                            },
                            'Permission': 'FULL_CONTROL'
                        }] if self.deployment == 'prod' else []),
                        {
                            'Grantee': {
                                'URI': 'http://acs.amazonaws.com/groups/global/AllUsers',
                                'Type': 'Group'
                            },
                            'Permission': 'READ'
                        }

                    ]
                })


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Uploads all Metadata TSV files from a single directory to a S3 bucket.')
    parser.add_argument('deployment')
    parser.add_argument('bucket_name')
    parser.add_argument('local_metadata_tsv_directory')
    options = parser.parse_args(sys.argv[1:])
    uploader = ProjectTSVUploader(options.deployment)
    uploader.upload_files_to_bucket(options.bucket_name, options.local_metadata_tsv_directory)
