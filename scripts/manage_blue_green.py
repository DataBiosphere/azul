"""
Change the state of the currently selected instance of a deployment in
blue-green mode. See docs/blue-green-mode.md.

The `go_live` subcommand makes the instance live, by recording it as live and
pointing the public custom domain names of the deployment at the instance's
API. The `rest` subcommand destroys the instance's OpenSearch domain. Each of
these subcommands checks its precondition before making any change.

The `detach` and `attach` subcommands are run by Terraform, before and after it
replaces a stage of the instance's API. If the instance is live, they remove
the public base path mappings that point at the stage, and recreate them.
"""
import argparse
from collections.abc import (
    Iterable,
)
import logging
from pathlib import (
    Path,
)
import subprocess
import sys
import time
from typing import (
    TYPE_CHECKING,
)

from azul import (
    config,
)
from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.deployment import (
    aws,
)
from azul.http import (
    HasCachedHttpClient,
)
from azul.indexer.index_service import (
    IndexService,
)
from azul.lib import (
    R,
)
from azul.logging import (
    configure_script_logging,
)
from azul.opensearch import (
    OpenSearchClientFactory,
)
from azul.queues import (
    Queues,
)

if TYPE_CHECKING:
    from mypy_boto3_apigateway import (
        APIGatewayClient,
    )
    from mypy_boto3_ssm import (
        SSMClient,
    )

log = logging.getLogger(__name__)

#: The names of the apps whose APIs the public custom domain names lead to
app_names = ['indexer', 'service']

#: API Gateway's name for the empty base path of a base path mapping
root_base_path = '(none)'


class PreconditionFailed(Exception):
    pass


class BlueGreen(HasCachedHttpClient):

    def __init__(self):
        deployment = config.deployment
        alias = deployment.alias
        assert alias is not None, R(
            'The selected deployment is not an instance in blue-green mode',
            deployment.name)
        self.stage = config.deployment_stage
        self.number = int(self.stage.removeprefix(alias.name))
        self.alias_config = alias.config

    @property
    def _apigw(self) -> APIGatewayClient:
        return aws.apigateway

    @property
    def _ssm(self) -> SSMClient:
        return aws.ssm

    def go_live(self, timeout: float):
        api_ids = {app_name: self._rest_api_id(app_name) for app_name in app_names}
        self._check_go_live(api_ids)
        log.info('Recording %r as the live instance', self.stage)
        self._ssm.put_parameter(Name=config.live_instance_parameter,
                                Value=str(self.number),
                                Type='String',
                                Overwrite=True)
        for app_name, api_id in api_ids.items():
            for domain in self._public_domains(app_name):
                self._map(domain, api_id)
        self._await_switch(timeout)

    def _check_go_live(self, api_ids: dict[str, str]):
        problems = []
        for app_name in app_names:
            for domain in self._public_domains(app_name):
                try:
                    self._apigw.get_domain_name(domainName=domain)
                except self._apigw.exceptions.NotFoundException:
                    problems.append(f'The custom domain name {domain!r} does not exist')
        problems.extend(self._index_problems())
        for app_name in app_names:
            url = config.lambda_endpoint(app_name).set(path='/health/basic')
            response = self._http_client.request('GET', str(url))
            if response.status != 200:
                problems.append(f'{str(url)!r} responded with status {response.status}')
        if problems:
            raise PreconditionFailed('Refusing to go live', problems)

    def _index_problems(self) -> Iterable[str]:
        opensearch = OpenSearchClientFactory.get()
        index_service = IndexService()
        for catalog in config.catalogs:
            for index_name in index_service.index_names(catalog):
                if not opensearch.indices.exists(index=str(index_name)):
                    yield f'Index {str(index_name)!r} does not exist'
        queues = Queues()
        total, lengths = queues.get_queue_lengths(queues.get_queues(config.indexer_queue_names))
        if total:
            yield f'The indexer queues are not empty: {lengths!r}'

    def _map(self, domain: str, api_id: str):
        current = self._mapping(domain)
        if current == (api_id, self.stage):
            log.info('%r already points at %r', domain, current)
        elif current is None:
            log.info('Creating mapping of %r to %r', domain, (api_id, self.stage))
            self._apigw.create_base_path_mapping(domainName=domain,
                                                 restApiId=api_id,
                                                 stage=self.stage)
        else:
            log.info('Changing mapping of %r from %r to %r',
                     domain, current, (api_id, self.stage))
            self._apigw.update_base_path_mapping(
                domainName=domain,
                basePath=root_base_path,
                patchOperations=[
                    {'op': 'replace', 'path': '/restapiId', 'value': api_id},
                    {'op': 'replace', 'path': '/stage', 'value': self.stage}
                ]
            )

    #: The number of seconds between two rounds of requests to the public
    #: names. With two apps, this keeps the requests well below the WAF's rate
    #: limit of 1,000 requests per IP in five minutes.
    #:
    poll_interval = 2

    #: The number of consecutive rounds in which every response must come from
    #: the instance before the switch is considered complete. While the switch
    #: propagates, either API may answer a given request.
    #:
    required_rounds = 15

    def _await_switch(self, timeout: float):
        """
        Wait until the aliases point at the instance.
        """
        urls = [
            f'https://{domain}/health/basic'
            for app_name in app_names
            for domain in self._public_domains(app_name)
        ]
        deadline = time.time() + timeout
        rounds = 0
        while rounds < self.required_rounds:
            assert time.time() < deadline, R(
                'Timed out waiting for the public names to lead to the instance',
                self.stage)
            answers = {url: self._answer(url) for url in urls}
            if all(answer == self.stage for answer in answers.values()):
                rounds += 1
            else:
                if rounds:
                    log.info('Answers reverted after %i rounds: %r', rounds, answers)
                rounds = 0
            time.sleep(self.poll_interval)
        log.info('The public names lead to %r', self.stage)

    def _answer(self, url: str) -> str | None:
        """
        The name of the deployment that answered a request to the given URL, or
        `None` if the request failed.
        """
        response = self._http_client.request('GET', url)
        if response.status == 200:
            return response.headers.get('X-Azul-Deployment')
        else:
            return None

    def detach(self, app_name: str, api_id: str, stage: str):
        """
        If the instance is live, remove the public base path mappings of the
        given app that point at the given stage, so that the stage can be
        deleted.
        """
        if self._is_live():
            for domain in self._public_domains(app_name):
                mapping = self._mapping(domain)
                if mapping == (api_id, stage):
                    log.info('Removing mapping of %r to %r', domain, mapping)
                    self._apigw.delete_base_path_mapping(domainName=domain,
                                                         basePath=root_base_path)
                else:
                    log.info('Leaving mapping of %r to %r', domain, mapping)
        else:
            log.info('Instance %r is not live, leaving the public mappings alone',
                     self.stage)

    def attach(self, app_name: str, api_id: str, stage: str):
        """
        If the instance is live, create the missing public base path mappings
        of the given app, pointing at the given stage. Existing mappings are
        left alone.
        """
        if self._is_live():
            for domain in self._public_domains(app_name):
                mapping = self._mapping(domain)
                if mapping is None:
                    log.info('Creating mapping of %r to %r', domain, (api_id, stage))
                    self._apigw.create_base_path_mapping(domainName=domain,
                                                         restApiId=api_id,
                                                         stage=stage)
                else:
                    log.info('Leaving mapping of %r to %r', domain, mapping)
        else:
            log.info('Instance %r is not live, leaving the public mappings alone',
                     self.stage)

    def _is_live(self) -> bool:
        try:
            parameter = self._ssm.get_parameter(Name=config.live_instance_parameter)
        except self._ssm.exceptions.ParameterNotFound:
            return False
        else:
            return int(parameter['Parameter']['Value']) == self.number

    def rest(self):
        assert not config.share_opensearch_domain, R(
            'The instance does not have an OpenSearch domain of its own')
        self._check_rest()
        cwd = Path(config.project_root) / 'terraform'
        subprocess.run(['make', 'validate'], cwd=cwd, check=True)
        subprocess.run(
            ['terraform', 'destroy', '-auto-approve'] + [
                arg
                for target in [
                    'aws_cloudwatch_metric_alarm.indexercachehealth',
                    'aws_cloudwatch_metric_alarm.servicecachehealth',
                    'aws_opensearch_domain.index'
                ]
                for arg in ['-target', target]
            ],
            cwd=cwd,
            check=True
        )

    def _check_rest(self):
        if self._is_live():
            raise PreconditionFailed('Refusing to rest the live instance',
                                     config.live_instance_parameter)
        api_ids = {self._rest_api_id(app_name) for app_name in app_names}
        live_domains = []
        for app_name in app_names:
            for domain in self._public_domains(app_name):
                try:
                    mapping = self._mapping(domain)
                except self._apigw.exceptions.NotFoundException:
                    mapping = None
                if mapping is not None and mapping[0] in api_ids:
                    live_domains.append(domain)
        if live_domains:
            raise PreconditionFailed('Refusing to rest a live instance', live_domains)

    def _public_domains(self, app_name: str) -> list[str]:
        """
        The public custom domain names of the given app, as configured for the
        alias. During the migration of a deployment into blue-green mode, these
        are the domain names of the legacy instance.
        """
        alias_config = self.alias_config
        return [
            alias_config.api_lambda_domain(app_name),
            *alias_config.api_lambda_domain_aliases(app_name)
        ]

    def _rest_api_id(self, app_name: str) -> str:
        name = config.qualified_resource_name(app_name)
        paginator = self._apigw.get_paginator('get_rest_apis')
        ids = [
            api['id']
            for page in paginator.paginate()
            for api in page['items']
            if api['name'] == name
        ]
        assert len(ids) == 1, R('Expected exactly one REST API', name, ids)
        return ids[0]

    def _mapping(self, domain: str) -> tuple[str, str] | None:
        """
        The ID of the REST API and the name of the stage that the root base
        path mapping of the given custom domain name points at, or `None` if
        there is no such mapping.
        """
        mappings = self._apigw.get_base_path_mappings(domainName=domain)
        matches = [
            (mapping['restApiId'], mapping['stage'])
            for mapping in mappings.get('items', [])
            if mapping['basePath'] == root_base_path
        ]
        assert len(matches) <= 1, R('Ambiguous base path mappings', domain, matches)
        return matches[0] if matches else None


def main(argv: list[str]):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=AzulArgumentHelpFormatter)
    subparsers = parser.add_subparsers(dest='command', required=True)
    go_live = subparsers.add_parser('go_live',
                                    help='Make the selected instance live')

    # When the switch was tested, it took up to about 80 seconds to propagate
    # (see docs/blue-green-mode.md). The default leaves ample margin for that.
    #
    go_live.add_argument('--timeout',
                         type=float,
                         default=5 * 60,
                         help='The number of seconds to wait for the public '
                              'names to lead to the instance')
    subparsers.add_parser('rest',
                          help="Destroy the selected instance's OpenSearch domain")
    for command, description in [
        ('detach', 'Remove the public mappings to a stage of the live instance'),
        ('attach', 'Create the missing public mappings to a stage of the live instance')
    ]:
        subparser = subparsers.add_parser(command, help=description)
        subparser.add_argument('--app', required=True, choices=app_names)
        subparser.add_argument('--rest-api-id', required=True)
        subparser.add_argument('--stage', required=True)
    args = parser.parse_args(argv)
    blue_green = BlueGreen()
    if args.command == 'go_live':
        blue_green.go_live(args.timeout)
    elif args.command == 'rest':
        blue_green.rest()
    elif args.command == 'detach':
        blue_green.detach(args.app, args.rest_api_id, args.stage)
    elif args.command == 'attach':
        blue_green.attach(args.app, args.rest_api_id, args.stage)
    else:
        assert False, args.command


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
