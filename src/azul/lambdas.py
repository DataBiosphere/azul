import ast
import logging
from typing import (
    FrozenSet,
    Optional,
)

import attr

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

logger = logging.getLogger(__name__)


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class Lambda:
    name: str
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
    def _contribution_lambda_names(cls) -> FrozenSet[str]:
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
                resource_name, _ = config.unqualified_resource_name(queue)
                return resource_name in notification_queue_names

        return frozenset((
            handler.name
            for handler in vars(indexer).values()
            if has_notification_queue(handler)
        ))

    @classmethod
    def from_response(cls, response: JSON) -> 'Lambda':
        name = response['FunctionName']
        try:
            env = response['Environment']['Variables']
        except KeyError:
            assert name.startswith('custodian-mandatory'), response
            slot_location = None
        else:
            slot_location = env['AZUL_TDR_SOURCE_LOCATION']
        return cls(name=name,
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
                    logger.info(f'Setting concurrency limit for {lambda_name} back to {original_concurrency_limit}.')
                    self._lambda.put_function_concurrency(FunctionName=lambda_name,
                                                          ReservedConcurrentExecutions=original_concurrency_limit)
                else:
                    logger.info(f'Removed concurrency limit for {lambda_name}.')
                    self._lambda.delete_function_concurrency(FunctionName=lambda_name)

                lambda_arn = lambda_settings['Configuration']['FunctionArn']
                self._lambda.untag_resource(Resource=lambda_arn, TagKeys=[self.tag_name])
            else:
                logger.warning(f'{lambda_name} is already enabled.')
        else:
            if self.tag_name not in lambda_tags.keys():
                try:
                    concurrency = lambda_settings['Concurrency']
                except KeyError:
                    # If a lambda doesn't have a limit for concurrency executions, Lambda.Client.get_function()
                    # doesn't return a response with the key, `Concurrency`.
                    concurrency_limit = None
                else:
                    concurrency_limit = concurrency['ReservedConcurrentExecutions']

                logger.info(f'Setting concurrency limit for {lambda_name} to zero.')
                new_tag = {self.tag_name: repr(concurrency_limit)}
                self._lambda.tag_resource(Resource=lambda_settings['Configuration']['FunctionArn'], Tags=new_tag)
                self._lambda.put_function_concurrency(FunctionName=lambda_name, ReservedConcurrentExecutions=0)
            else:
                logger.warning(f'{lambda_name} is already disabled.')
