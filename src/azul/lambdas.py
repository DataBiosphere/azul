import ast
import logging
import time
from typing import (
    Optional,
    Self,
    TYPE_CHECKING,
)

import attr
from more_itertools import (
    one,
)

from azul import (
    R,
    cache,
    config,
)
from azul.deployment import (
    aws,
)
from azul.modules import (
    load_app_module,
)

if TYPE_CHECKING:
    from mypy_boto3_lambda.type_defs import (
        FunctionConfigurationTypeDef,
    )

log = logging.getLogger(__name__)


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class LambdaFunction:
    name: str
    role: str
    slot_location: Optional[str]

    @property
    def contributes(self) -> bool:
        unqualify = config.unqualified_resource_name
        for handler_name in self._contribution_handler_names():
            try:
                # FIXME: Eliminate hardcoded separator
                #        https://github.com/databiosphere/azul/issues/2964
                app_name, _ = unqualify(self.name, suffix='-' + handler_name)
            except AssertionError as e:
                if not R.caused(e):
                    raise
            else:
                if app_name == 'indexer':
                    return True
        return False

    @classmethod
    @cache
    def _contribution_handler_names(cls) -> frozenset[str]:
        notification_queue_names = {
            config.notifications_queue.derive(retry=retry).unqual_name
            for retry in (False, True)
        }

        def has_notification_queue(handler) -> bool:
            try:
                queue = handler.queue
            except AttributeError:
                return False
            else:
                resource_name, _, _ = config.unqualified_resource_name_and_suffix(queue)
                return resource_name in notification_queue_names

        indexer = load_app_module('indexer')
        return frozenset(
            handler.name
            for handler in vars(indexer).values()
            if has_notification_queue(handler)
        )

    @classmethod
    def from_response(cls, response: 'FunctionConfigurationTypeDef') -> Self:
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
            assert not self.contributes, self
        else:
            allowed_locations = config.tdr_allowed_source_locations
            assert self.slot_location in allowed_locations, self.slot_location


class LambdaFunctions:
    tag_name = 'azul-original-concurrency-limit'

    @property
    def _lambda(self):
        return aws.lambda_

    def list_functions(self) -> list[LambdaFunction]:
        # Note that this method returns the $LATEST version, which is what
        # Amazon also refers to as the "unpublished" version.
        return [
            LambdaFunction.from_response(function)
            for response in self._lambda.get_paginator('list_functions').paginate()
            for function in response['Functions']
        ]

    def delete_older_versions(self, function_name: str, keep_version: int) -> None:
        """
        Delete all versions of a Lambda function prior to the specified one.

        :param function_name: The fully qualified name of the function
                              e.g. 'azul-service-dev'

        :param keep_version: The version of the function to not delete.
        """
        paginator = self._lambda.get_paginator('list_versions_by_function')
        versions = [
            function['Version']
            for page in paginator.paginate(FunctionName=function_name)
            for function in page['Versions']
            if (
                function['Version'] != '$LATEST'  # The so-called "unpublished" version
                and int(function['Version']) < keep_version
            )
        ]
        for version in versions:
            log.info('Deleting version %r of %r', version, function_name)
            self._lambda.delete_function(FunctionName=function_name,
                                         Qualifier=version)

    def manage_lambdas(self, enabled: bool):
        paginator = self._lambda.get_paginator('list_functions')
        prefixes = [
            config.qualified_resource_name(app_name)
            for app_name in config.app_names()
        ]
        assert all(prefixes)
        for response in paginator.paginate(MaxItems=500):
            for function in response['Functions']:
                function_name = function['FunctionName']
                if any(function_name.startswith(prefix) for prefix in prefixes):
                    self.manage_function(function_name, enabled)

    def manage_function(self, function_name: str, enable: bool):
        function = self._lambda.get_function(FunctionName=function_name)
        assert function_name == function['Configuration']['FunctionName']
        function_arn = function['Configuration']['FunctionArn']
        tags = self._lambda.list_tags(Resource=function_arn)['Tags']
        if enable:
            if self.tag_name in tags.keys():
                original_concurrency_limit = ast.literal_eval(tags[self.tag_name])
                if original_concurrency_limit is not None:
                    log.info('Setting concurrency limit on %r back to %r.',
                             function_name, original_concurrency_limit)
                    self._lambda.put_function_concurrency(FunctionName=function_name,
                                                          ReservedConcurrentExecutions=original_concurrency_limit)
                else:
                    log.info('Removed concurrency limit on %r.', function_name)
                    self._lambda.delete_function_concurrency(FunctionName=function_name)

                self._lambda.untag_resource(Resource=function_arn, TagKeys=[self.tag_name])
            else:
                log.warning('Function %r is already enabled.', function_name)
        else:
            if self.tag_name in tags.keys():
                log.warning('Function %r is already disabled.', function_name)
            else:
                try:
                    concurrency = function['Concurrency']
                except KeyError:
                    # Function doesn't have a concurrency limit
                    concurrency_limit = None
                else:
                    concurrency_limit = concurrency['ReservedConcurrentExecutions']
                log.info('Setting concurrency limit on %r to zero.', function_name)
                new_tag = {self.tag_name: repr(concurrency_limit)}
                self._lambda.tag_resource(Resource=function_arn, Tags=new_tag)
                self._lambda.put_function_concurrency(FunctionName=function_name, ReservedConcurrentExecutions=0)

    def reset_lambda_roles(self):
        """
        Attempt to fix KMSAccessDeniedException when invoking a function.

        See Troubleshooting section in README.md for details.
        """
        client = self._lambda
        app_names = set(config.app_names())

        for function in self.list_functions():
            for app_name in app_names:
                if function.name.startswith(config.qualified_resource_name(app_name)):
                    other_app_name = one(app_names - {app_name})
                    temporary_role = function.role.replace(
                        config.qualified_resource_name(app_name),
                        config.qualified_resource_name(other_app_name)
                    )
                    log.info('Temporarily updating %r to role %r', function.name, temporary_role)
                    client.update_function_configuration(FunctionName=function.name,
                                                         Role=temporary_role)
                    log.info('Updating %r to role %r', function.name, function.role)
                    while True:
                        try:
                            client.update_function_configuration(FunctionName=function.name,
                                                                 Role=function.role)
                        except client.exceptions.ResourceConflictException:
                            log.info('Function %r is being updated. Retrying ...', function.name)
                            time.sleep(1)
                        else:
                            break
