from contextlib import (
    contextmanager,
)
import inspect
from itertools import (
    chain,
)
import json
import logging
import os
from pathlib import (
    Path,
)
import re
import subprocess
import tempfile
import threading
from typing import (
    Dict,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)
from unittest.mock import (
    patch,
)

import attr
import boto3
import botocore.credentials
import botocore.session

from azul import (
    Netloc,
    cache,
    cached_property,
    config,
    require,
)
from azul.template import (
    emit,
)
from azul.types import (
    AnyJSON,
    JSON,
    JSONs,
)

log = logging.getLogger(__name__)


def _cache(func):
    """
    Methods and properties whose return values depend on the current AWS
    credentials must be cached in association with the current Boto3 session.
    This session is local to the current thread and, within a thread, may
    temporarily change to a session that uses the credentials of another role
    (see self.assumed_role_credentials()). To cache such methods and properties,
    use @_cache instead of @cached_property, @lru_cache or @cache.
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
    def sts(self):
        return self.client('sts')

    @property
    def lambda_(self):
        return self.client('lambda')

    @property
    def apigateway(self):
        return self.client('apigateway')

    @property
    def account(self):
        # See also `make check_aws`
        return config.aws_account_id

    @property
    def es(self):
        return self.client('es')

    @property
    def stepfunctions(self):
        return self.client('stepfunctions')

    @property
    def iam(self):
        return self.client('iam')

    @property
    def secretsmanager(self):
        return self.client('secretsmanager')

    def dynamo(self, endpoint_url, region_name):
        return aws.resource('dynamodb',
                            endpoint_url=endpoint_url,
                            region_name=region_name)

    @property
    def es_endpoint(self) -> Optional[Netloc]:
        if config.es_endpoint:
            return config.es_endpoint
        else:
            return self._es_domain_status['Endpoint'], 443

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
        return f"arn:aws:lambda:{self.region_name}:{self.account}:function:{function_name}-{suffix}"

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

    def get_hmac_key_and_id(self):
        # Note: dict contains 'key' and 'key_id' as keys and is provisioned in scripts/provision_credentials.py
        response = self.secretsmanager.get_secret_value(SecretId=config.secrets_manager_secret_name('indexer', 'hmac'))
        secret_dict = json.loads(response['SecretString'])
        return secret_dict['key'], secret_dict['key_id']

    @_cache
    def get_hmac_key_and_id_cached(self, cache_key_id):
        key, key_id = self.get_hmac_key_and_id()
        assert cache_key_id == key_id
        return key, key_id

    def dss_main_bucket(self, dss_endpoint: str) -> str:
        return self._dss_bucket(dss_endpoint, lambda_name='indexer')

    # Remove once https://github.com/HumanCellAtlas/data-store/issues/1837 is resolved

    def dss_checkout_bucket(self, dss_endpoint: str) -> str:
        return self._dss_bucket(dss_endpoint, 'checkout', lambda_name='service')

    @_cache
    def _dss_bucket(self, dss_endpoint: str, *qualifiers: str, lambda_name: str) -> str:
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
    def service_account_credentials(self):
        """
        A context manager that patches the GOOGLE_APPLICATION_CREDENTIALS
        environment variable to point to a file containing the credentials of
        the Google service account that represents the Azul deployment. The
        returned context is the name of a temporary file containing the
        credentials.
        """
        secret_name = config.secrets_manager_secret_name('google_service_account')
        secret = self._service_account_creds(secret_name)['SecretString']
        with tempfile.NamedTemporaryFile(mode='w+') as f:
            f.write(secret)
            f.flush()
            with patch.dict(os.environ, GOOGLE_APPLICATION_CREDENTIALS=f.name):
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
        the default ones or those from another assumed_role_credentials context.

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
        # Get the AssumeRole credential provider
        session = botocore.session.get_session()
        resolver = session.get_component('credential_provider')
        assume_role_provider = resolver.get_provider('assume-role')

        # Make the provider use the same cache as the AWS CLI
        cli_cache = Path('~', '.aws', 'cli', 'cache').expanduser()
        assume_role_provider.cache = botocore.credentials.JSONFileCache(cli_cache)

        return boto3.session.Session(botocore_session=session)

    @_cache
    def client(self, *args, **kwargs):
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
        """
        return self.boto3_session.client(*args, **kwargs)

    @_cache
    def resource(self, *args, **kwargs):
        """
        Same as `self.client()` but for `boto3.resource()` instead of
        `boto3.client()`.
        """
        return self.boto3_session.resource(*args, **kwargs)


aws = AWS()
del AWS
del _cache


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class TerraformSchema:
    versions: Sequence[str]
    document: JSON
    path: Path

    @classmethod
    def load(cls, path: Path):
        with path.open() as f:
            doc = json.load(f)
        return cls(versions=doc['versions'],
                   document=doc['schema'],
                   path=path)

    def store(self):
        with self.path.open('w') as f:
            json.dump(dict(versions=self.versions,
                           schema=self.document), f, indent=4)


class Terraform:

    def taggable_resource_types(self) -> Sequence[str]:
        schema = self.schema.document
        require(schema['format_version'] == '0.1')
        resources = chain.from_iterable(
            schema['provider_schemas'][provider]['resource_schemas'].items()
            for provider in schema['provider_schemas']
        )
        return [
            resource_type
            for resource_type, resource in resources
            if 'tags' in resource['block']['attributes']
        ]

    def run(self, *args: str) -> str:
        terraform_dir = Path(config.project_root) / 'terraform'
        cmd = subprocess.run(['terraform', *args],
                             cwd=terraform_dir,
                             check=True,
                             stdout=subprocess.PIPE,
                             text=True,
                             shell=False)
        return cmd.stdout

    schema_path = Path(config.project_root) / 'terraform' / '_schema.json'

    @cached_property
    def schema(self):
        return TerraformSchema.load(self.schema_path)

    def update_schema(self):
        schema = self.run('providers', 'schema', '-json')
        schema = TerraformSchema(versions=self.versions,
                                 document=json.loads(schema),
                                 path=self.schema_path)
        schema.store()
        # Reset the cache
        try:
            # noinspection PyPropertyAccess
            del self.schema
        except AttributeError:
            pass

    @cached_property
    def versions(self) -> Sequence[str]:
        # `terraform -version` prints a warning if you are not running the latest
        # release of Terraform; we discard it, otherwise, we would need to update
        # the tracked schema every time a new version of Terraform is released
        versions, footer = self.run('-version').split('\n\n')
        return sorted(versions.splitlines())


terraform = Terraform()
del Terraform


def _sanitize_tf(tf_config: JSON) -> JSON:
    """
    Avoid errors like

        Error: Missing block label

          on api_gateway.tf.json line 12:
          12:     "resource": []

        At least one object property is required, whose name represents the resource
        block's type.
    """
    return {k: v for k, v in tf_config.items() if v}


def _normalize_tf(tf_config: Union[JSON, JSONs]) -> Iterable[Tuple[str, AnyJSON]]:
    """
    Certain levels of a Terraform JSON structure can either be a single
    dictionary or a list of dictionaries. For example, these are equivalent:

        {"resource": {"resource_type": {"resource_id": {"foo": ...}}}}
        {"resource": [{"resource_type": {"resource_id": {"foo": ...}}}]}

    So are these:

        {"resource": {"type": {"id": {"foo": ...}, "id2": {"bar": ...}}}}
        {"resource": {"type": [{"id": {"foo": ...}}, {"id2": {"bar": ...}}]}}

    This function normalizes input to prefer the second form of both cases to
    make parsing Terraform configuration simpler. It returns an iterator of the
    dictionary entries in the argument, regardless which form is used.

    >>> list(_normalize_tf({'foo': 'bar'}))
    [('foo', 'bar')]

    >>> list(_normalize_tf([{'foo': 'bar'}]))
    [('foo', 'bar')]

    >>> list(_normalize_tf({"foo": "bar", "baz": "qux"}))
    [('foo', 'bar'), ('baz', 'qux')]

    >>> list(_normalize_tf([{"foo": "bar"}, {"baz": "qux"}]))
    [('foo', 'bar'), ('baz', 'qux')]

    >>> list(_normalize_tf([{"foo": "bar", "baz": "qux"}]))
    [('foo', 'bar'), ('baz', 'qux')]
    """
    if isinstance(tf_config, dict):
        return tf_config.items()
    elif isinstance(tf_config, list):
        return chain.from_iterable(d.items() for d in tf_config)
    else:
        assert False, type(tf_config)


def populate_tags(tf_config: JSON) -> JSON:
    """
    Add tags to all taggable resources and change the `name` tag to `Name`
    for tagged AWS resources.
    """
    taggable_resource_types = terraform.taggable_resource_types()
    try:
        resources = tf_config['resource']
    except KeyError:
        return tf_config
    else:
        return {
            k: v if k != 'resource' else [
                {
                    resource_type: [
                        {
                            resource_name: {
                                **arguments,
                                'tags': _adjust_name_tag(resource_type,
                                                         _tags(resource_name, **arguments.get('tags', {})))
                            } if resource_type in taggable_resource_types else arguments
                        }
                        for resource_name, arguments in _normalize_tf(resource)
                    ]
                }
                for resource_type, resource in _normalize_tf(resources)
            ]
            for k, v in tf_config.items()
        }


def emit_tf(tf_config: Optional[JSON]):
    if tf_config is None:
        return emit(tf_config)
    else:
        return emit(_sanitize_tf(populate_tags(tf_config)))


def _tags(resource_name: str, **overrides: str) -> Dict[str, str]:
    """
    Return tags named for cloud resources based on :class:`azul.Config`.

    :param resource_name: The Terraform name of the resource.

    :param overrides: Additional tags that override the defaults.

    >>> from azul.doctests import assert_json
    >>> assert_json(_tags('service'))  #doctest: +ELLIPSIS
    {
        "project": "dcp",
        "service": "azul",
        "deployment": "...",
        "owner": ...,
        "name": "azul-service-...",
        "component": "azul-service"
    }

    >>> from azul.doctests import assert_json
    >>> assert_json(_tags('service', project='foo'))  #doctest: +ELLIPSIS
    {
        "project": "foo",
        "service": "azul",
        "deployment": "...",
        "owner": ...,
        "name": "azul-service-...",
        "component": "azul-service"
    }
    """
    return {
        'project': 'dcp',
        'service': config.resource_prefix,
        'deployment': config.deployment_stage,
        'owner': config.owner,
        'name': config.qualified_resource_name(resource_name),
        'component': f'{config.resource_prefix}-{resource_name}',
        **overrides
    }


def _adjust_name_tag(resource_type: str, tags: Dict[str, str]) -> Dict[str, str]:
    return {
        'Name' if k == 'name' and resource_type.startswith('aws_') else k: v
        for k, v in tags.items()
    }
