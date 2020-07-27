import argparse
from itertools import (
    chain,
)
import json
import logging
import sys
from typing import (
    Mapping,
)

import boto3
import more_itertools

from azul import (
    config,
)
from azul.logging import (
    configure_script_logging,
)
from azul.types import (
    JSON,
)

log = logging.getLogger(__name__)

HealthCheck = JSON


class HealthCheckManager:

    def __init__(self):
        self.route53 = boto3.client('route53')

    def get_health_checks(self) -> Mapping[str, HealthCheck]:
        paginator = self.route53.get_paginator('list_health_checks')
        hcs = chain.from_iterable(page['HealthChecks'] for page in paginator.paginate())
        batch_size = 10  # Route53 lets us get tags for at most ten resources at a time
        hc_batches = more_itertools.chunked(hcs, batch_size)
        dcp_hcs = {}
        for hc_batch in hc_batches:
            hc_batch = {hc['Id']: hc for hc in hc_batch}
            response = self.route53.list_tags_for_resources(ResourceType='healthcheck',
                                                            ResourceIds=list(hc_batch.keys()))
            for tag_set in response['ResourceTagSets']:
                assert tag_set['ResourceType'] == 'healthcheck'
                for tag in tag_set['Tags']:
                    if tag['Key'] == 'Name':
                        hc_name = tag['Value']
                        hc_id = tag_set['ResourceId']
                        dcp_hcs[hc_name] = hc_batch[hc_id]
        return dcp_hcs

    def provision_health_check(self, all_hcs: Mapping[str, HealthCheck], dcp_hc: HealthCheck, link: bool):
        dcp_child_hcs = set(dcp_hc['HealthCheckConfig']['ChildHealthChecks'])
        azul_child_hcs = {all_hcs[hc_name]['Id'] for hc_name in (config.service_name,
                                                                 config.indexer_name,
                                                                 config.data_browser_name,
                                                                 config.data_portal_name)}

        new_dcp_child_hcs = dcp_child_hcs - azul_child_hcs if not link else dcp_child_hcs | azul_child_hcs
        if new_dcp_child_hcs == dcp_child_hcs:
            log.info('DCP-wide composite health check up to date, no changes required!')
        else:
            response = self.route53.update_health_check(HealthCheckId=dcp_hc['Id'],
                                                        HealthCheckVersion=dcp_hc['HealthCheckVersion'],
                                                        ChildHealthChecks=list(new_dcp_child_hcs),
                                                        Inverted=dcp_hc['HealthCheckConfig']['Inverted'],
                                                        HealthThreshold=len(dcp_child_hcs))
            log.info("Update response: " + json.dumps(response))


def main(argv):
    manager = HealthCheckManager()
    parser = argparse.ArgumentParser(description='Dynamically reference and dereference health checks '
                                                 'not managed by Azul.')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--link', '-L', dest='link', action='store_true', default=False,
                       help='Link Azul-managed health check resources to DCP-wide composite health check.')
    group.add_argument('--unlink', '-U', dest='link', action='store_false', default=False,
                       help='Unlink Azul-managed health check resources to DCP-wide composite health check.')
    options = parser.parse_args(argv)
    hcs = manager.get_health_checks()
    dcp_health_deployment_name = f'dcp-health-check-{config.deployment_stage}'
    if dcp_health_deployment_name in hcs:
        manager.provision_health_check(hcs, hcs[dcp_health_deployment_name], options.link)
    else:
        log.info('DCP wide health check does not exist for ' + config.deployment_stage)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
