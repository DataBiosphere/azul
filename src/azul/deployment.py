from collections.abc import (
    Mapping,
)
from contextlib import (
    contextmanager,
)
import inspect
import json
import logging
import os
from pathlib import (
    Path,
)
import re
import tempfile
import threading
from typing import (
    Any,
    Callable,
    Optional,
    TYPE_CHECKING,
    Tuple,
    TypeVar,
    cast,
)
from unittest.mock import (
    patch,
)

import boto3
from botocore.awsrequest import (
    AWSPreparedRequest,
    AWSResponse,
)
import botocore.credentials
from botocore.exceptions import (
    NoCredentialsError,
)
import botocore.session
import botocore.utils
from more_itertools import (
    one,
)

from azul import (
    Netloc,
    cache,
    cached_property,
    config,
    reject,
)
from azul.logging import (
    azul_boto3_log as boto3_log,
    http_body_log_message,
)
from azul.types import (
    JSON,
    JSONs,
)

if TYPE_CHECKING:
    from mypy_boto3_ecr import (
        ECRClient,
    )
    from mypy_boto3_iam import (
        IAMClient,
    )
    from mypy_boto3_kms import (
        KMSClient,
    )
    from mypy_boto3_s3 import (
        S3Client,
    )
    from mypy_boto3_sesv2 import (
        SESV2Client,
    )
    from mypy_boto3_stepfunctions import (
        SFNClient,
    )

log = logging.getLogger(__name__)

R = TypeVar('R')


def _cache(func: Callable[..., R]) -> Callable[..., R]:
    """
    Methods and properties whose return values depend on the currently active
    AWS credentials must be cached under the currently active Boto3 session.
    This session is local to the current thread and, within a thread, may
    temporarily change to a session that uses the credentials of another role
    (see self.assumed_role_credentials()). To cache such methods and properties,
    use this @_cache instead of @cached_property, @lru_cache or @cache.
    """

    @cache
    def cached_func(_session, self, *args, **kwargs):
        return func(self, *args, **kwargs)

    def wrapper(self, *args, **kwargs):
        return cached_func(self.boto3_session, self, *args, **kwargs)

    wrapper.cache_clear = cached_func.cache_clear

    return wrapper


class AWS:
    class _PerThread(threading.local):
        session: Optional[boto3.session.Session] = None

    def __init__(self) -> None:
        super().__init__()
        self._per_thread = self._PerThread()

    def discard_all_sessions(self):
        self._per_thread = self._PerThread()

    def discard_current_session(self):
        self._per_thread.session = None

    def clear_caches(self):
        # Find all methods and properties wrapped with lru_cache and reset
        # their cache.
        for attribute in inspect.classify_class_attrs(type(self)):
            if attribute.kind == 'method':
                method = attribute.object
            elif attribute.kind == 'property':
                method = cast(property, attribute.object).fget
            else:
                continue
            try:
                # cache_clear is a documented method of the lru_cache wrapper
                cache_clear = getattr(method, 'cache_clear')
            except AttributeError:
                pass
            else:
                log.debug('Clearing cache of %r', attribute.name)
                cache_clear()

    @cached_property
    def profile(self):
        session = botocore.session.Session()
        profile_name = session.get_config_variable('profile')
        return {} if profile_name is None else session.full_config['profiles'][profile_name]

    @property
    @_cache
    def region_name(self):
        return self.sts.meta.region_name

    @property
    def s3(self) -> 'S3Client':
        return self.client('s3')

    @property
    def securityhub(self):
        return self.client('securityhub')

    @property
    def sns(self):
        return self.client('sns')

    @property
    def ses(self) -> 'SESV2Client':
        return self.client('sesv2')

    @property
    def sts(self):
        return self.client('sts')

    @property
    def lambda_(self):
        return self.client('lambda')

    @property
    def cloudwatch(self):
        return self.client('cloudwatch')

    @property
    def apigateway(self):
        return self.client('apigateway')

    @property
    def ecr(self) -> 'ECRClient':
        return self.client('ecr')

    @property
    def account(self):
        # See also `make check_aws`
        return config.aws_account_id

    @property
    def account_name(self):
        return one(self.iam.list_account_aliases()['AccountAliases'])

    @property
    def es(self):
        return self.client('es')

    @property
    def stepfunctions(self) -> 'SFNClient':
        return self.client('stepfunctions')

    @property
    def iam(self) -> 'IAMClient':
        return self.client('iam')

    @property
    def kms(self) -> 'KMSClient':
        return self.client('kms')

    @property
    def secretsmanager(self):
        return self.client('secretsmanager')

    @property
    def ec2(self):
        return self.client('ec2')

    @property
    def dynamodb(self):
        return self.client('dynamodb', azul_logging=True)

    @property
    def es_endpoint(self) -> Optional[Netloc]:
        if config.es_endpoint:
            return config.es_endpoint
        else:
            return self._es_domain_status['Endpoints']['vpc'], 443

    @property
    def es_instance_count(self) -> Optional[int]:
        if config.es_endpoint:
            return config.es_instance_count
        else:
            return self._es_domain_status['ElasticsearchClusterConfig']['InstanceCount']

    @property
    @_cache
    def _es_domain_status(self) -> Optional[JSON]:
        """
        Return the status of the current deployment's Elasticsearch domain
        """
        es_domain = self.es.describe_elasticsearch_domain(DomainName=config.es_domain)
        return es_domain['DomainStatus']

    def get_lambda_arn(self, function_name, suffix):
        return f'arn:aws:lambda:{self.region_name}:{self.account}:function:{function_name}-{suffix}'

    @property
    @_cache
    def permissions_boundary_arn(self) -> str:
        return f'arn:aws:iam::{self.account}:policy/{config.permissions_boundary_name}'

    @property
    @_cache
    def permissions_boundary(self):
        try:
            return self.iam.get_policy(PolicyArn=self.permissions_boundary_arn)['Policy']
        except self.iam.exceptions.NoSuchEntityException:
            return None

    @property
    @_cache
    def permissions_boundary_tf(self) -> Mapping[str, str]:
        return {} if self.permissions_boundary is None else {
            'permissions_boundary': self.permissions_boundary['Arn']
        }

    @_cache
    def get_hmac_key_and_id(self) -> Tuple[bytes, str]:
        # Note: dict contains 'key' and 'key_id' as keys and is provisioned in
        # scripts/provision_credentials.py
        secret_id = config.secrets_manager_secret_name('indexer', 'hmac')
        response = self.secretsmanager.get_secret_value(SecretId=secret_id)
        secret_dict = json.loads(response['SecretString'])
        return secret_dict['key'].encode(), secret_dict['key_id']

    def dss_main_bucket(self, dss_endpoint: str) -> str:
        return self._dss_bucket(dss_endpoint, lambda_name='indexer')

    # Remove once https://github.com/HumanCellAtlas/data-store/issues/1837 is resolved

    def dss_checkout_bucket(self, dss_endpoint: str) -> str:
        return self._dss_bucket(dss_endpoint, 'checkout', lambda_name='service')

    @_cache
    def _dss_bucket(self,
                    dss_endpoint: str,
                    *qualifiers: str,
                    lambda_name: str
                    ) -> str:
        with self.direct_access_credentials(dss_endpoint, lambda_name):
            stage = config.dss_deployment_stage(dss_endpoint)
            name = f'/dcp/dss/{stage}/environment'
            # FIXME: make region configurable (https://github.com/DataBiosphere/azul/issues/1560)
            ssm = aws.client('ssm', region_name='us-east-1')
            dss_parameter = ssm.get_parameter(Name=name)
        dss_config = json.loads(dss_parameter['Parameter']['Value'])
        bucket_key = '_'.join(['dss', 's3', *qualifiers, 'bucket']).upper()
        return dss_config[bucket_key]

    @_cache
    def _service_account_creds(self, secret_name: str) -> JSON:
        sm = self.secretsmanager
        creds = sm.get_secret_value(SecretId=secret_name)
        return creds

    @contextmanager
    def service_account_credentials(self, service_account: config.ServiceAccount):
        """
        A context manager that provides a temporary file containing the
        credentials of the Google service account that represents the Azul
        deployment. The returned context is the path to the file.

        While the context manager is active, accidental usage of the default
        credentials is prevented by patching the environment variable
        GOOGLE_APPLICATION_CREDENTIALS to the empty string.
        """
        secret_name = config.secrets_manager_secret_name(service_account.secret_name)
        secret = self._service_account_creds(secret_name)['SecretString']
        with tempfile.NamedTemporaryFile(mode='w+') as f:
            f.write(secret)
            f.flush()
            with patch.dict(os.environ, GOOGLE_APPLICATION_CREDENTIALS=''):
                yield f.name

    def direct_access_credentials(self, dss_endpoint: str, lambda_name: str):
        """
        A context manager that causes the client() method to return boto3
        clients that use credentials suitable for accessing the DSS bucket
        directly.

        :param dss_endpoint: The URL of the REST API endpoint of the DSS
                             instance whose bucket is to be accessed directly

        :param lambda_name: The name of the lambda wishing to access the bucket
                            directly. If direct access is gained by assuming a
                            role and if the role name is parameterized with the
                            lambda name, the specified value will be
                            interpolated into the role name. See
                            AZUL_DSS_DIRECT_ACCESS_ROLE for details
        """
        if dss_endpoint == config.dss_endpoint:
            role_arn = config.dss_direct_access_role(lambda_name)
        else:
            role_arn = None
        return self.assumed_role_credentials(role_arn)

    @contextmanager
    def assumed_role_credentials(self, role_arn: Optional[str]):
        """
        A context manager that causes the client() method to return boto3
        clients that use credentials obtained by assuming the given role as long
        as that method is invoked in context i.e., the body of the `with`
        statement.

        This context manager is thread-safe in that it doesn't affect clients
        obtained by other threads, even when the context manager is active in
        one thread.

        It can be nested as long as the outer context's role has permission to
        assume the inner context's role. It is not reentrant in that two nested
        contexts cannot use the same role, since a role cannot assume itself.

        The given role is assumed using currently active credentials, either the
        default ones or those from another assumed_role_credentials context.

        :param role_arn: the ARN of the role to assume. If None, the context
                         manager does nothing and calls to the .client() method
                         in context will use the same credentials as calls out
                         of context
        """
        if role_arn is None:
            # FIXME: make this CM reentrant by taking this branch if the given
            #        role is already assumed
            yield
        else:
            sts = self.client('sts')
            identity = sts.get_caller_identity()
            # If we used the current identity's ARN to derive the session name,
            # we'd quickly risk exceeding the maximum 64 character limit on the
            # session name, especially when nesting this context manager.
            # Instead we use the user ID, something like AKIAIOSFODNN7EXAMPLE),
            # as the session name and log it along with the ARN. That way we
            # can at least string things back together forensically.
            #
            # FIXME: Eliminate .sub() and only parse out the AROA
            #        https://github.com/DataBiosphere/azul/issues/3890
            session_name = self.invalid_session_name_re.sub('.', identity['UserId'])
            log.info('Identity %s with ARN %s is about to assume role %s using session name %s.',
                     identity['UserId'], identity['Arn'], role_arn, session_name)
            response = sts.assume_role(RoleArn=role_arn,
                                       RoleSessionName=session_name)
            credentials = response['Credentials']
            new_session = boto3.session.Session(aws_access_key_id=credentials['AccessKeyId'],
                                                aws_secret_access_key=credentials['SecretAccessKey'],
                                                aws_session_token=credentials['SessionToken'])
            old_session = self._per_thread.session
            self._per_thread.session = new_session
            try:
                yield
            finally:
                self._per_thread.session = old_session

    invalid_session_name_re = re.compile(r'[^\w+=,.@-]')

    @property
    def boto3_session(self) -> boto3.session.Session:
        """
        The Boto3 session for the current thread.
        """
        session = self._per_thread.session
        if session is None:
            session = self._create_boto3_session()
            self._per_thread.session = session
        return session

    def _create_boto3_session(self) -> boto3.session.Session:
        session = botocore.session.get_session()
        cli_cache = Path('~', '.aws', 'cli', 'cache').expanduser()
        if cli_cache.exists():
            # Get the AssumeRole credential provider
            resolver = session.get_component('credential_provider')
            provider = resolver.get_provider('assume-role')
            # Make the provider use the same cache as the AWS CLI
            provider.cache = botocore.utils.JSONFileCache(cli_cache)
            # Remove the provider that reads from environment variables. It
            # typically precedes the assume-role provider so its presence would
            # defeat the CLI cache sharing if credentials are present in the
            # environment, which is typically the case on developer machines
            # (see envhook.py and _login_aws in `environment`).
            resolver.remove('env')
        return boto3.session.Session(botocore_session=session)

    @_cache
    def client(self, *args, azul_logging: bool = False, **kwargs):
        """
        Outside of a context established by `.assumed_role_credentials()` this
        method returns a Boto3 client object of the same type as that of
        `boto3.client()` except that the client object only shares its session
        with clients created in the same thread.

        Within such a context, the returned Boto3 client uses a session with
        temporary credentials for the assumed role. That session is only shared
        with clients created in the same thread and context. When the context is
        exited, the thread reverts to a session that uses default credentials
        but is still only shared with clients created in the current thread.

        Note that direct_access_credentials() uses assumed_role_credentials()
        and therefore affects the return value in the same way.

        Caching the result of this function is not necessary and will be harmful
        if the cached value is used by a thread other than the one that called
        this function.

        :param azul_logging: Whether to log the client's requests and
                             responses. Note that using DEBUG level will
                             enable logging of request bodies, which could
                             contain sensitive or secret information.
        """
        client = self.boto3_session.client(*args, **kwargs)
        if azul_logging:
            events = client.meta.events
            events.register_last(self._request_event_name, self._log_client_request)
            events.register_first(self._response_event_name, self._log_client_response)
        return client

    _request_event_name = 'before-send'
    _response_event_name = 'response-received'

    def _log_client_request(self,
                            event_name: str,
                            request: AWSPreparedRequest,
                            **_kwargs: Any
                            ) -> Optional[AWSResponse]:
        event_name = self._shorten_event_name(event_name, self._request_event_name)
        boto3_log.info('%s:\tMaking %s request to %s',
                       event_name,
                       request.method,
                       request.url)
        message = http_body_log_message(boto3_log, 'request', request.body)
        boto3_log.info('%s:\t%s', event_name, message)
        return None

    def _log_client_response(self,
                             *,
                             event_name: str,
                             **kwargs: Any
                             ) -> Optional[AWSResponse]:
        event_name = self._shorten_event_name(event_name, self._response_event_name)
        response = kwargs['response_dict']
        boto3_log.info('%s:\tGot %s response', event_name, response['status_code'])
        message = http_body_log_message(boto3_log, 'response', response.get('body'))
        boto3_log.info('%s:\t%s', event_name, message)
        return None

    def _shorten_event_name(self, event_name: str, rm_prefix: str) -> str:
        prefix, _, suffix = event_name.partition('.')
        assert prefix == rm_prefix, event_name
        return suffix

    @_cache
    def resource(self, *args, **kwargs):
        """
        Same as `self.client()` but for `boto3.resource()` instead of
        `boto3.client()`.
        """
        return self.boto3_session.resource(*args, **kwargs)

    def qualified_bucket_name(self, bucket_name: str) -> str:
        return config.qualified_bucket_name(account_name=config.aws_account_name,
                                            region_name=self.region_name,
                                            bucket_name=bucket_name)

    @property
    def shared_bucket(self):
        return self.qualified_bucket_name(config.shared_term)

    @property
    def logs_bucket(self):
        return self.qualified_bucket_name(config.logs_term)

    # An ELB account ID, which varies depending on region, is needed to specify
    # the principal in bucket policies for buckets storing LB access logs.
    #
    # https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-logging-bucket-permissions
    #
    elb_region_account_id = {
        'us-east-1': '127311923021',
        'us-east-2': '033677994240',
        'us-west-1': '027434742980',
        'us-west-2': '797873946194',
        'af-south-1': '098369216593',
        'ca-central-1': '985666609251',
        'eu-central-1': '054676820928',
        'eu-west-1': '156460612806',
        'eu-west-2': '652711504416',
        'eu-south-1': '635631232127',
        'eu-west-3': '009996457667',
        'eu-north-1': '897822967062',
        'ap-east-1': '754344448648',
        'ap-northeast-1': '582318560864',
        'ap-northeast-2': '600734575887',
        'ap-northeast-3': '383597477331',
        'ap-southeast-1': '114774131450',
        'ap-southeast-2': '783225319266',
        'ap-south-1': '718504428378',
        'me-south-1': '076674570225',
        'sa-east-1': '507241528517'
    }

    def elb_access_log_bucket_policy(self,
                                     *,
                                     bucket_arn: str,
                                     path_prefix: str
                                     ) -> JSONs:
        """
        The S3 bucket policy statements needed for ELB load balancers to write
        access logs to a bucket. Note that this method returns only policy
        statements to allow for easier merging of policies.

        :param bucket_arn: the ARN of the bucket or a Terraform (TF) expression
                           yielding it. If a TF expression is passed the
                           return value of this method must be used inside TF
                           config and won't work as a plain policy.

        :param path_prefix: the path prefix of log objects, relative to the
                            bucket root. ELB appends additional prefix elements
                            at the end of the given prefix. Must not begin or
                            end in a slash.
        """
        self._validate_bucket_path_prefix(path_prefix)
        path = f'{path_prefix}/AWSLogs/{aws.account}'
        return [
            {
                'Effect': 'Allow',
                'Principal': {
                    'AWS': f'arn:aws:iam::{self.elb_region_account_id[self.region_name]}:root'
                },
                'Action': 's3:PutObject',
                'Resource': f'{bucket_arn}/{path}/*'
            },
        ]

    def s3_access_log_bucket_policy(self,
                                    *,
                                    source_bucket_arn: str,
                                    target_bucket_arn: str,
                                    path_prefix: str
                                    ) -> JSONs:
        """
        The S3 bucket policy statements needed for S3 to write server access
        logs to a bucket. Note that this method returns only policy
        statements to allow for easier merging of policies.

        :param source_bucket_arn: the ARN of the bucket that is generating logs
                                  or a Terraform (TF) expression yielding it.
                                  If a TF expression is passed, the return value
                                  of this method must be used inside TF config
                                  and won't work as a plain policy. The ARN may
                                  contain wildcards but only buckets owned by
                                  the current AWS account will match.

        :param target_bucket_arn: the ARN of the bucket to write the logs to
                                  or a Terraform (TF) expression yielding it.
                                  If a TF expression is passed, the return value
                                  of this method must be used inside TF config
                                  and won't work as a plain policy.

        :param path_prefix: the path prefix of log objects, relative to the
                            target bucket root. ELB appends additional prefix
                            elements at the end of the given prefix. Must not
                            begin or end in a slash.
        """
        self._validate_bucket_path_prefix(path_prefix)
        return [
            {
                'Effect': 'Allow',
                'Principal': {
                    'Service': 'logging.s3.amazonaws.com'
                },
                'Action': [
                    's3:PutObject'
                ],
                'Resource': f'{target_bucket_arn}/{path_prefix}/*',
                'Condition': {
                    'ArnLike': {
                        'aws:SourceArn': source_bucket_arn
                    },
                }
            }
        ]

    def _validate_bucket_path_prefix(self, path_prefix):
        reject(path_prefix.startswith('/') or path_prefix.endswith('/'), path_prefix)

    @property
    def monitoring_topic_name(self):
        try:
            stage = config.main_deployment_stage
        except NoCredentialsError:
            # Running `make openapi` in GitHub fails since retrieving the main
            # deployment stage requires making a roundtrip to IAM. The
            # monitoring topic is used by the monitoring Lambda.
            stage = config.deployment_stage
        return config.qualified_resource_name('monitoring',
                                              stage=stage)


aws = AWS()
del AWS
del _cache
