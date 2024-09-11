import ast
import logging
import time
from typing import (
    Optional,
)

import attr
from more_itertools import (
    one,
)

from azul import (
    JSON,
    RequirementError,
    cache,
    config,
)
from azul.deployment import (
    aws,
)
from azul.modules import (
    load_app_module,
)

log = logging.getLogger(__name__)


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class Lambda:
    name: str
    role: str
    slot_location: Optional[str]
    version: str

    @property
    def is_contribution_lambda(self) -> bool:
        for lambda_name in self._contribution_lambda_names():
            try:
                # FIXME: Eliminate hardcoded separator
                #        https://github.com/databiosphere/azul/issues/2964
                resource_name, _ = config.unqualified_resource_name(self.name,
                                                                    suffix='-' + lambda_name)
            except RequirementError:
                pass
            else:
                if resource_name == 'indexer':
                    return True
        return False

    @classmethod
    @cache
    def _contribution_lambda_names(cls) -> frozenset[str]:
        indexer = load_app_module('indexer')
        notification_queue_names = {
            config.unqual_notifications_queue_name(retry=retry) for retry in (False, True)
        }

        def has_notification_queue(handler) -> bool:
            try:
                queue = handler.queue
            except AttributeError:
                return False
            else:
                resource_name, _, _ = config.unqualified_resource_name_and_suffix(queue)
                return resource_name in notification_queue_names

        return frozenset((
            handler.name
            for handler in vars(indexer).values()
            if has_notification_queue(handler)
        ))

    @classmethod
    def from_response(cls, response: JSON) -> 'Lambda':
        name = response['FunctionName']
        role = response['Role']
        version = response['Version']
        try:
            slot_location = response['Environment']['Variables']['AZUL_TDR_SOURCE_LOCATION']
        except KeyError:
            slot_location = None
        return cls(name=name,
                   role=role,
                   slot_location=slot_location,
                   version=version)

    def __attrs_post_init__(self):
        if self.slot_location is None:
            assert not self.is_contribution_lambda, self
        else:
            allowed_locations = config.tdr_allowed_source_locations
            assert self.slot_location in allowed_locations, self.slot_location


class Lambdas:
    tag_name = 'azul-original-concurrency-limit'

    @property
    def _lambda(self):
        return aws.lambda_

    def list_lambdas(self,
                     deployment: str | None = None,
                     all_versions: bool = False
                     ) -> list[Lambda]:
        """
        Return a list of all AWS Lambda functions (or function versions) in the
        current account, or the given deployment.

        :param deployment: Limit output to the specified deployment stage. If
                           `None`, functions from all deployments will be
                           returned.

        :param all_versions: If `True`, return all versions of each AWS Lambda
                             function (including '$LATEST'). If `False`, return
                             only the latest version of each function.
        """
        paginator = self._lambda.get_paginator('list_functions')
        lambda_prefixes = None if deployment is None else [
            config.qualified_resource_name(lambda_name, stage=deployment)
            for lambda_name in config.lambda_names()
        ]
        params = {'FunctionVersion': 'ALL'} if all_versions else {}
        return [
            Lambda.from_response(function)
            for response in paginator.paginate(**params)
            for function in response['Functions']
            if lambda_prefixes is None or any(
                function['FunctionName'].startswith(prefix)
                for prefix in lambda_prefixes
            )
        ]

    def manage_lambdas(self, enabled: bool):
        for function in self.list_lambdas(deployment=config.deployment_stage):
            self.manage_lambda(function.name, enabled)

    def manage_lambda(self, lambda_name: str, enable: bool):
        lambda_settings = self._lambda.get_function(FunctionName=lambda_name)
        lambda_arn = lambda_settings['Configuration']['FunctionArn']
        lambda_tags = self._lambda.list_tags(Resource=lambda_arn)['Tags']
        lambda_name = lambda_settings['Configuration']['FunctionName']
        if enable:
            if self.tag_name in lambda_tags.keys():
                original_concurrency_limit = ast.literal_eval(lambda_tags[self.tag_name])

                if original_concurrency_limit is not None:
                    log.info(f'Setting concurrency limit for {lambda_name} back to {original_concurrency_limit}.')
                    self._lambda.put_function_concurrency(FunctionName=lambda_name,
                                                          ReservedConcurrentExecutions=original_concurrency_limit)
                else:
                    log.info(f'Removed concurrency limit for {lambda_name}.')
                    self._lambda.delete_function_concurrency(FunctionName=lambda_name)

                lambda_arn = lambda_settings['Configuration']['FunctionArn']
                self._lambda.untag_resource(Resource=lambda_arn, TagKeys=[self.tag_name])
            else:
                log.warning(f'{lambda_name} is already enabled.')
        else:
            if self.tag_name not in lambda_tags.keys():
                try:
                    concurrency = lambda_settings['Concurrency']
                except KeyError:
                    # If a lambda doesn't have a limit for concurrency
                    # executions, Lambda.Client.get_function()
                    # doesn't return a response with the key, `Concurrency`.
                    concurrency_limit = None
                else:
                    concurrency_limit = concurrency['ReservedConcurrentExecutions']

                log.info(f'Setting concurrency limit for {lambda_name} to zero.')
                new_tag = {self.tag_name: repr(concurrency_limit)}
                self._lambda.tag_resource(Resource=lambda_settings['Configuration']['FunctionArn'], Tags=new_tag)
                self._lambda.put_function_concurrency(FunctionName=lambda_name, ReservedConcurrentExecutions=0)
            else:
                log.warning(f'{lambda_name} is already disabled.')

    def reset_lambda_roles(self):
        client = self._lambda
        lambda_names = set(config.lambda_names())

        for lambda_ in self.list_lambdas():
            for lambda_name in lambda_names:
                if lambda_.name.startswith(config.qualified_resource_name(lambda_name)):
                    other_lambda_name = one(lambda_names - {lambda_name})
                    temporary_role = lambda_.role.replace(
                        config.qualified_resource_name(lambda_name),
                        config.qualified_resource_name(other_lambda_name)
                    )
                    log.info('Temporarily updating %r to role %r', lambda_.name, temporary_role)
                    client.update_function_configuration(FunctionName=lambda_.name,
                                                         Role=temporary_role)
                    log.info('Updating %r to role %r', lambda_.name, lambda_.role)
                    while True:
                        try:
                            client.update_function_configuration(FunctionName=lambda_.name,
                                                                 Role=lambda_.role)
                        except client.exceptions.ResourceConflictException:
                            log.info('Function %r is being updated. Retrying ...', lambda_.name)
                            time.sleep(1)
                        else:
                            break

    def delete_published_function_versions(self):
        """
        Delete the published versions of every AWS Lambda function in the
        current deployment.
        """
        log.info('Deleting stale versions of AWS Lambda functions')
        for function in self.list_lambdas(deployment=config.deployment_stage,
                                          all_versions=True):
            if function.version == '$LATEST':
                log.info('Skipping the unpublished version of %r', function.name)
            else:
                log.info('Deleting published version %r of %r', function.version, function.name)
                self._lambda.delete_function(FunctionName=function.name,
                                             Qualifier=function.version)
