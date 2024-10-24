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
        try:
            slot_location = response['Environment']['Variables']['AZUL_TDR_SOURCE_LOCATION']
        except KeyError:
            slot_location = None
        return cls(name=name,
                   role=role,
                   slot_location=slot_location)

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

    def list_lambdas(self) -> list[Lambda]:
        return [
            Lambda.from_response(function)
            for response in self._lambda.get_paginator('list_functions').paginate()
            for function in response['Functions']
        ]

    def manage_lambdas(self, enabled: bool):
        paginator = self._lambda.get_paginator('list_functions')
        lambda_prefixes = [config.qualified_resource_name(lambda_infix) for lambda_infix in config.lambda_names()]
        assert all(lambda_prefixes)
        for lambda_page in paginator.paginate(FunctionVersion='ALL', MaxItems=500):
            for lambda_name in [metadata['FunctionName'] for metadata in lambda_page['Functions']]:
                if any(lambda_name.startswith(prefix) for prefix in lambda_prefixes):
                    self.manage_lambda(lambda_name, enabled)

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
