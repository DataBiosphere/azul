"""
A command-line tool to download project files from the HCA Data Portal.

Please note that the purpose of this script is to serve as a proof of concept
and example how the Azul service (https://service.azul.data.humancellatlas.org/)
might be used to programmatically list and download project data matrices.
"""
import argparse
import json
import os
import sys
from time import (
    sleep,
)
from typing import (
    Any,
    List,
    Mapping,
    MutableMapping,
    Optional,
)
import uuid

import requests
from tqdm import (
    tqdm,
)


class HCAFileDownloader:

    def main(self):
        self._run()
        return 0

    @classmethod
    def _parse_args(cls, argv):
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument('--catalog', '-c',
                            help='A catalog name.')
        parser.add_argument('--project-id', '-p',
                            metavar='ID',
                            help='A Project UUID.')
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('--list-catalogs', '-C',
                           action='store_true', default=False,
                           help='List all catalogs. ')
        group.add_argument('--list-projects', '-P',
                           action='store_true', default=False,
                           help='List all projects in the specified catalog.')
        group.add_argument('--list-matrices', '-M',
                           action='store_true', default=False,
                           help='List the project level matrices in the specified project.')
        group.add_argument('--download-matrices', '-m',
                           action='store_true', default=False,
                           help='Download all project level matrices in the specified project.')
        group.add_argument('--list-file-summary', '-F',
                           action='store_true', default=False,
                           help='List the counts of files by format in the specified project.')
        group.add_argument('--download-file-format', '-f',
                           action='append', metavar='FORMAT',
                           help='Compose a curl command for downloading all project '
                                'files of the given format(s)s. Include this option more '
                                'than once to specify multiple formats. The '
                                'keyword \'ALL\' can be specified to request all files.')
        parser.add_argument('--destination', '-d',
                            default=os.curdir, metavar='PATH',
                            help='(Optional) Local path where downloaded files will be saved. '
                                 f'Default: {os.curdir!r} (current folder)')
        args = parser.parse_args(argv)
        return args

    def __init__(self, argv: List[str]) -> None:
        super().__init__()
        self.args = self._parse_args(argv)
        self.api_base = 'https://service.azul.data.humancellatlas.org'

    def _run(self):
        if self.args.list_catalogs:
            self.list_catalogs()
        elif self.args.list_projects:
            self.require_catalog(self.args.catalog)
            self.list_projects(catalog=self.args.catalog)
        elif self.args.list_matrices:
            self.require_catalog(self.args.catalog)
            self.require_project(self.args.catalog, self.args.project_id)
            self.list_project_matrices(catalog=self.args.catalog,
                                       project_id=self.args.project_id)
        elif self.args.list_file_summary:
            self.require_catalog(self.args.catalog)
            self.require_project(self.args.catalog, self.args.project_id)
            self.list_file_summary(catalog=self.args.catalog,
                                   project_id=self.args.project_id)
        elif self.args.download_matrices:
            self.require_catalog(self.args.catalog)
            self.require_project(self.args.catalog, self.args.project_id)
            self.download_project_matrices(catalog=self.args.catalog,
                                           project_id=self.args.project_id,
                                           destination=self.args.destination)
        elif self.args.download_file_format:
            self.require_catalog(self.args.catalog)
            self.require_project(self.args.catalog, self.args.project_id)
            self.download_files_by_format(catalog=self.args.catalog,
                                          project_id=self.args.project_id,
                                          formats=self.args.download_file_format)
        else:
            assert False, 'Unknown action specified.'

    def get_terminal_width(self):
        """
        Return the character width of the current terminal window
        """
        width = 60  # Use this as a minimum
        try:
            size = os.get_terminal_size()
        except OSError:
            size = None
        if size and size[0] > width:
            width = size[0]
        if os.name == 'nt':
            width -= 1  # Windows needs 1 empty space for newline
        return width

    def get_json_response(self,
                          url: str,
                          params: Optional[MutableMapping[str, str]] = None
                          ) -> MutableMapping[str, Any]:
        """
        Return the JSON decoded response from a URL request.
        """
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def require_catalog(self, catalog: str) -> None:
        if not catalog:
            print('ERROR: A catalog value is required for this action.')
            exit(1)
        self.validate_catalog(catalog)

    def validate_catalog(self, catalog: str) -> None:
        """
        Verify the given catalog is a valid value.
        """
        url = self.api_base + '/index/catalogs'
        response = self.get_json_response(url)
        if catalog not in response['catalogs']:
            print(f'Invalid catalog: {catalog}')
            exit(1)

    def require_project(self, catalog: str, project_id: str) -> None:
        if not project_id:
            print('ERROR: A project ID value is required for this action.')
            exit(1)
        self.validate_project(catalog, project_id)

    def validate_project(self, catalog: str, project_id: str) -> None:
        """
        Verify the given project ID is valid for the given catalog.
        """
        try:
            uuid.UUID(project_id)
        except (ValueError, TypeError):
            print('Project ID must be a valid a UUID')
            exit(1)
        url = self.api_base + f'/index/projects/{project_id}'
        try:
            self.get_json_response(url, params={'catalog': catalog})
        except requests.exceptions.HTTPError:
            print(f'Invalid project ID: {project_id}')
            exit(1)

    def list_catalogs(self) -> None:
        """
        Print a list of all available catalogs.
        """
        url = self.api_base + '/index/catalogs'
        response = self.get_json_response(url)
        print()
        for catalog, details in response['catalogs'].items():
            if not details['internal']:
                print(catalog)
        print()

    def list_projects(self, catalog: str) -> None:
        """
        Print a list of all available projects in the given catalog.
        """
        url = self.api_base + '/index/projects'
        params = {
            'catalog': catalog,
            'size': 100,
            'sort': 'projectTitle',
            'order': 'asc'
        }
        print()
        screen_width = self.get_terminal_width()
        while True:
            response = self.get_json_response(url, params=params)
            for hit in response['hits']:
                line = hit['entryId'] + ' | '
                shortname = hit['projects'][0]['projectShortname']
                width = int(0.25 * (screen_width - len(line)))
                line += shortname[:width] + (shortname[width:] and '..') + ' | '
                title = hit['projects'][0]['projectTitle']
                width = (screen_width - len(line))
                width -= 2 if len(title) > width else 0
                line += title[:width] + (title[width:] and '..')
                print(line)
            if next_url := response['pagination']['next']:
                url = next_url
                params = None
            else:
                break
        print()

    def get_file_summary(self,
                         catalog: str,
                         project_id: str
                         ) -> List[Mapping[str, Any]]:
        """
        Return a list of file type summaries for the given project.
        """
        url = self.api_base + '/index/summary'
        params = {
            'catalog': catalog,
            'filters': json.dumps({'projectId': {'is': [project_id]}})
        }
        response = self.get_json_response(url, params=params)
        return response['fileTypeSummaries']

    def list_file_summary(self, catalog: str, project_id: str) -> None:
        """
        Print a list of file type summaries for the given project.
        """
        summaries = self.get_file_summary(catalog, project_id)
        print()
        if summaries:
            width1 = max([len(s['format']) for s in summaries] + [6])
            width2 = max([len(str(s['count'])) for s in summaries] + [5])
            print('FORMAT'.ljust(width1, ' '), end='  ')
            print('COUNT'.ljust(width2, ' '), end='  ')
            print('TOTAL SIZE')
            for summary in summaries:
                print(summary['format'].ljust(width1, ' '), end='  ')
                print(str(summary['count']).rjust(width2, ' '), end='  ')
                print('{:.2f} MiB'.format(summary['totalSize'] / 1024 / 1024))
        else:
            print('Project has no files.')
        print()

    def download_file(self, url: str, output_path: str) -> None:
        """
        Download a file from the given URL.
        """
        url = url.replace('/fetch', '')  # Work around https://github.com/DataBiosphere/azul/issues/2908
        response = requests.get(url, stream=True)
        response.raise_for_status()
        try:
            existing_file_size = os.path.getsize(output_path)
        except FileNotFoundError:
            existing_file_size = None
        content_size = int(response.headers.get('content-length', 0))
        if existing_file_size is not None:
            if existing_file_size == content_size:
                print(f'Skipping completed download: {output_path}')
                return
            elif existing_file_size < content_size:
                headers = {'Range': f'bytes={existing_file_size}-'}
                response = requests.get(url, stream=True, headers=headers)
                response.raise_for_status()
                content_size = int(response.headers.get('content-length', 0))
                print(f'Resuming download to: {output_path}', flush=True)
            else:
                print(f'ERROR: Local file {output_path!r} larger than remote.')
                exit(1)
        else:
            print(f'Downloading to: {output_path}', flush=True)
        with open(output_path, 'ab') as f:
            with tqdm(total=content_size,
                      unit='iB',
                      unit_scale=True,
                      unit_divisor=1024
                      ) as bar:
                for chunk in response.iter_content(chunk_size=1024):
                    size = f.write(chunk)
                    bar.update(size)

    def iterate_matrices_tree(self, tree, keys=()):
        """
        Yield the leaf nodes from a project matrices tree.
        """
        if isinstance(tree, dict):
            for k, v in tree.items():
                yield from self.iterate_matrices_tree(v, keys=(*keys, k))
        elif isinstance(tree, list):
            for file in tree:
                yield keys, file
        else:
            assert False, 'Error parsing project matrices tree'

    def get_project_json(self,
                         catalog: str,
                         project_id: str) -> MutableMapping[str, Any]:
        """
        Return the project entity details from the projects endpoint response.
        """
        url = self.api_base + f'/index/projects/{project_id}'
        response = self.get_json_response(url, params={'catalog': catalog})
        return response['projects'][0]

    def list_project_matrices(self,
                              catalog: str,
                              project_id: str) -> None:
        """
        Print all the project matrices in the given project.
        """
        project = self.get_project_json(catalog, project_id)
        files = {}
        max_size_length = 0
        for key in ('matrices', 'contributorMatrices'):
            for path, file_info in self.iterate_matrices_tree(project[key]):
                size = '{:.2f} MiB'.format(file_info['size'] / 1024 / 1024)
                files[file_info['name']] = size
                if len(size) + 1 > max_size_length:
                    max_size_length = len(size) + 1
        print()
        if files:
            padding = max_size_length - len('SIZE') + 1
            print('SIZE', end=' ' * padding)
            print('FILE NAME')
            for file_name, file_size in files.items():
                padding = max_size_length - len(file_size) + 1
                print(file_size, end=' ' * padding)
                print(file_name)
        else:
            print('Project has no Matrices')
        print()

    def create_destination_dir(self, destination: str) -> None:
        if os.path.isfile(destination):
            print(f'ERROR: Destination path {destination!r} is not a folder.')
            exit(1)
        elif not os.path.isdir(destination):
            try:
                os.mkdir(destination)
            except OSError:
                print(f'ERROR: Unable to create destination {destination!r}')
                exit(1)

    def download_project_matrices(self,
                                  catalog: str,
                                  project_id: str,
                                  destination: str) -> None:
        """
        Download all the project matrices data files in the given project.
        """
        self.create_destination_dir(destination)
        project = self.get_project_json(catalog, project_id)
        file_urls = set()
        print()
        for key in ('matrices', 'contributorMatrices'):
            for path, file_info in self.iterate_matrices_tree(project[key]):
                url = file_info['url']
                if url not in file_urls:
                    dest_path = os.path.join(destination, file_info['name'])
                    self.download_file(url, dest_path)
                    file_urls.add(url)
        print('Downloads Complete.')
        print()

    def download_files_by_format(self,
                                 catalog: str,
                                 project_id: str,
                                 formats: List[str],
                                 ) -> None:
        """
        Prints instructions including a curl command to download all files of
        the given format(s) from a project.
        """
        if len(formats) == 1 and formats[0] == 'ALL':
            summaries = self.get_file_summary(catalog, project_id)
            formats = [s['format'] for s in summaries]
        print(f'Requesting a curl manifest for the file format(s) {formats!r}')
        url = self.api_base + '/fetch/manifest/files'
        params = {
            'catalog': catalog,
            'filters': json.dumps({
                'projectId': {'is': [project_id]},
                'fileFormat': {'is': formats}
            }),
            'format': 'curl'
        }
        response = self.get_json_response(url, params=params)
        while response['Status'] == 301:
            wait_time = response['Retry-After']
            units = 'second' if wait_time == 1 else 'seconds'
            print(f'Manifest is being built. Waiting {wait_time} {units}...')
            sleep(wait_time)
            response = self.get_json_response(url=response['Location'])
        if os.name == 'nt':
            print('\nTo complete this download, please run the following command:\n')
            print(response['CommandLine']['cmd.exe'], '\n')
        else:  # if os.name == 'posix':
            print('\nTo complete this download, please run the following command:\n')
            print(response['CommandLine']['bash'], '\n')


if __name__ == '__main__':
    adapter = HCAFileDownloader(sys.argv[1:])
    sys.exit(adapter.main())
