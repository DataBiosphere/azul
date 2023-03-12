import argparse
import logging
import os
import pathlib
import sys

import gitlab

from azul.compliance.fedramp_inventory_service import (
    FedRAMPInventoryService,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)
configure_script_logging(log)


def gitlab_auth() -> gitlab.Gitlab:
    # FIXME: the job token probably lacks the necessary permissions to update
    #        wiki pages. This is intended as a placeholder; authentication
    #        testing will need to wait until these changes are merged.
    try:
        url = os.environ['CI_SERVER_URL']
    except KeyError:
        url = input('Enter Gitlab url: ')
        authentication = dict(private_token=input('Enter Gitlab token: '))
    else:
        authentication = dict(job_token=os.environ['CI_JOB_TOKEN'])
    return gitlab.Gitlab(url=url, **authentication)


def main(output_path: pathlib.Path, update_wiki: bool) -> None:
    service = FedRAMPInventoryService()
    resources = list(service.get_resources())
    inventory = service.get_inventory(resources)
    template_path = pathlib.Path(__file__).parent / 'fedramp_inventory_template.xlsx'
    service.write_report(inventory, template_path, output_path)
    if update_wiki:
        gl = gitlab_auth()
        project = gl.projects.get('ucsc/azul')
        service.update_wiki(project, 'fedramp-inventory', resources)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('output_path', type=pathlib.Path)
    parser.add_argument('--wiki', action='store_true')
    args = parser.parse_args(sys.argv[1:])

    main(args.output_path, args.wiki)
