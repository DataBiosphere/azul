import json
import os
from typing import (
    Mapping,
    Optional,
)


def env() -> Mapping[str, Optional[str]]:
    """
    Returns a dictionary that maps environment variable names to values. The
    values are either None or strings. String values can contain references to
    other environment variables in the form `{FOO}` where FOO is the name of an
    environment variable. See

    https://docs.python.org/3.8/library/string.html#format-string-syntax

    for the concrete syntax. These references will be resolved *after* the
    overall environment has been compiled by merging all relevant
    `environment.py` and `environment.local.py` files.

    Entries with a `None` value will be excluded from the environment. They
    can be used to document a variable without a default value in which case
    other, more specific `environment.py` or `environment.local.py` files must
    provide the value.
    """
    xdg_data_home = os.environ.get('XDG_DATA_HOME',
                                   os.path.expanduser('~/.local/share'))
    return {

        # Configure the catalogs to be managed by this Azul deployment. A
        # catalog is a group of indices populated from a particular source.
        #
        # The AZUL_CATALOGS variable must be a string that represents a JSON
        # array of JSON objects with the following fields:
        #
        # {
        #   'name': {
        #       'atlas': 'bar',
        #       'internal': True,
        #       'plugins': {
        #           plugin_type: {'name'=plugin_package},
        #           plugin_type: {'name'=plugin_package},
        #           ...
        #       }
        #       'sources': [
        #                   source,
        #                   ...
        #       ]
        #   }
        # }
        #
        # The `atlas` and `name` properties follow the same, fairly restrictive
        # syntax defined by azul.Config.Catalog.validate_name.
        # `plugin_type` is the name of a child package of `azul.plugins` and
        # `plugin_package` is the name of a child package of that package. The
        # `plugin_type` denotes the purpose (like accessing a repository or
        # transforming metadata) and `plugin_package` denotes the concrete
        # implementation of how to fulfill that purpose.
        #
        # The first catalog listed is the default catalog.
        #
        # A source represents a TDR dataset, TDR snapshot, or canned staging
        # area to index. Each source is a string matching the following EBNF grammar:
        #
        # source = TDR source | canned source ;
        #
        # TDR source = 'tdr:', Google Cloud project name,
        #              ':', ( 'dataset' | 'snapshot' ),
        #              '/', TDR dataset or snapshot name,
        #              ':', [ prefix ],
        #              '/', partition prefix length ;
        #
        # canned source = 'https://github.com',
        #                 '/', owner,
        #                 '/', repo,
        #                 '/tree/', ref,
        #                 ['/', path],
        #                 ':', [ prefix ],
        #                 '/', partition prefix length ;
        #
        # The `prefix` is an optional string of hexadecimal digits
        # constraining the set of indexed subgraphs from the source. A
        # subgraph will be indexed if its UUID begins with the `prefix`. The
        # default `prefix` is the empty string.
        #
        # The partition prefix length is an integer that is used to further
        # partition the set of indexed subgraphs. Each partition is
        # assigned a prefix of `partition prefix length` hexadecimal digits.
        # A subgraph belongs to a partition if its UUID starts with the
        # overall `prefix` followed by the partition's prefix. The number of
        # partitions of a source is therefore `16 ** partition prefix length`.
        # Partition prefixes that are too long result in many small or even
        # empty partitions and waste some amount of resources. Partition
        # prefixes that are too short result in few large partitions that could
        # exceed the memory and running time limitations of the AWS Lambda
        # function that processes them. If in doubt err on the side of too many
        # small partitions.
        #
        # The `partition prefix length` plus the length of `prefix` must not
        # exceed 8.
        #
        # `ref` can be a branch, tag, or commit SHA. If `ref` contains special
        # characters like `/`, '?` or `#` they must be URL-encoded.
        #
        # Examples:
        #
        # tdr:broad-jade-dev-data:snapshot/hca_mvp:/1
        # tdr:broad-jade-dev-data:dataset/hca_mvp:2/1
        # https://github.com/HumanCellAtlas/schema-test-data/tree/de355ca/tests:2
        #
        'AZUL_CATALOGS': None,

        # The Account ID number for AWS
        'AZUL_AWS_ACCOUNT_ID': None,

        # The region of the Azul deployment. This variable is primarily used by
        # the AWS CLI, by TerraForm, botocore and boto3 but Azul references it
        # too. This variable is typically set in deployment-specific
        # environments.
        #
        'AWS_DEFAULT_REGION': None,

        # Only variables whose names start in `AZUL_` will be published to a deployed
        # Lambda. Note that by implication, `azul_` variables will not be published,
        # even though they are considered part of Azul. For secret values that should
        # not be printed or logged, use a variable name containing any of the strings
        # `secret`, `password` or `token`, either upper or lower case. Think twice
        # before publishing a variable containing a secret.

        # The email address of a user that owns the cloud resources in the current
        # deployment. This will become the value of the Owner tag on all resources.
        'AZUL_OWNER': None,

        # Controls the verbosity of application logs. Use 0 for no debug logging
        # 1 for debug logging by application code and info logging by other code
        # and 2 for debug logging by all code. This also controls app.debug, a
        # Chalice setting that causes an app to return a traceback in the body of
        # error responses: Setting AZUL_DEBUG to 0 disables the traceback
        # (app.debug = False), 1 or higher enable it (app.debug = True).
        # See https://github.com/aws/chalice#tutorial-error-messages for more.
        'AZUL_DEBUG': '0',

        # The name of the current deployment. This variable controls the name of all
        # cloud resources and is the main vehicle for isolating cloud resources
        # between deployments.
        'AZUL_DEPLOYMENT_STAGE': None,

        # Whether to enable direct access to objects in the DSS main bucket. If 0,
        # bundles and files are retrieved from the DSS using the GET /bundles/{uuid}
        # and GET /files/{UUID} endpoints. If 1, S3 GetObject requests are made
        # directly to the underlying bucket. This requires intimate knowledge of DSS
        # implementation details but was ten times faster. Recent optimizations to
        # the DSS (mainly, the delayed eviction of metadata files from the checkout
        # bucket made the performance gains less dramatic but the first hit to a
        # metadata file is still going to be slow because the objects needs to be
        # checked out. Aside from the latency improvements on a single request,
        # direct access also bypasses DSS lambda concurrency limits, thereby
        # increasing in the throughput of the Azul indexer, which is especially
        # noticeable during reindexing and scale testing.
        #
        # More importantly, direct access needs to be enabled for deletions to work
        # properly as the Azul indexer needs to know the metadata of the deleted
        # bundle in order to place the correct tombstone contributions into its
        # index. Direct access is also required for the Azul service's DSS files
        # proxy and DOS/DRS endpoints. Disabling DSS direct access will break these
        # endpoints.
        #
        'AZUL_DSS_DIRECT_ACCESS': '0',

        # An optional ARN of a role to assume when directly accessing a DSS bucket.
        # This can be useful when the DSS buckets are not owned by the same AWS
        # account owning the current Azul deployment. If there is another Azul
        # deployment in the account owning the DSS bucket, the role assumed by the
        # other Azul indexer would be an obvious candidate for the current
        # deployment's indexer to assume for direct access. Presumably that other
        # indexer has sufficient privileges to directly access the DSS buckets.
        #
        # The character '*' will be replaced with the name of the lambda
        # wishing to gain access. This parameterization can be used to have the
        # indexer lambda in the native deployment assume the role of the indexer
        # lambda in the foreign deployment, while the native service lambda assumes
        # the role of the foreign service lambda.
        #
        # If specified, this ARN must be of the following form:
        #
        # arn:aws:iam::ACCOUNT_ID:role/azul-*-DEPLOYMENT
        #
        # The only free variables are ACCOUNT_ID and DEPLOYMENT which are the
        # AWS account ID and the deployment stage of the Azul deployment that's
        # providing the role to be assumed.
        #
        'AZUL_DSS_DIRECT_ACCESS_ROLE': None,

        # The name of the hosted zone in Route 53 in which to create user friendly
        # domain names for various API gateways. This hosted zone will have to be
        # created manually prior to running `make deploy`. The value is typically
        # not deployment specific. A subdomain will automatically be created for
        # each deployment.
        'AZUL_DOMAIN_NAME': '{AZUL_DEPLOYMENT_STAGE}.singlecell.gi.ucsc.edu',

        # An optional list of roles in other AWS accounts that can assume the IAM
        # role normally assumed by lambda functions in the active Azul deployment.
        #
        # The syntax is <account>[,<role>...][:<account>[,<role>...]...] where
        # <account> is the numeric AWS account ID and role is a role name with
        # optional * or ? wildcards for the StringLike operator in IAM conditions.
        # Whitespace around separators and at the beginning or end of the value
        # are ignored.
        #
        # This parameter has profound security implications: the external role can
        # do anything an Azul lambda can do. The external account and any principal
        # with IAM access in that account, not just the specified roles, must be
        # fully trusted.
        #
        # This configuration is typically used to enable an external Azul deployment
        # to directly access the same DSS buckets the active deployment has direct
        # access to.
        #
        'AZUL_EXTERNAL_LAMBDA_ROLE_ASSUMPTORS': None,

        # The domain name of the HCA DRS endpoint. The service lambda serves
        # requests under both its canonical domain name as well as the domain name
        # given here. It is assumed that the parent domain of the given domain is
        # a hosted zone in Route 53 that we can create additional certificate
        # validation records in. If unset or set to empty string, the service lambda
        # will only serve requests under its canonical domain name and no validation
        # records will be created in hosted zones other than the zone defined by
        # AZUL_DOMAIN_NAME.
        'AZUL_DRS_DOMAIN_NAME': '',

        # A template for the name of the Route 53 record set in the hosted zone
        # specified by AZUL_DOMAIN_NAME. The character '*' in the template
        # will be substituted with the name of the Lambda function, e.g. `indexer`
        # or `service`. May contain periods.
        'AZUL_SUBDOMAIN_TEMPLATE': '*',

        # A prefix to be prepended to the names of AWS Lambda functions and
        # associated resources. Must not contain periods.
        'AZUL_RESOURCE_PREFIX': 'azul',

        # The host and port of the Elasticsearch instance to use. This takes
        # precedence over AZUL_ES_DOMAIN.
        'AZUL_ES_ENDPOINT': None,

        # The name of the AWS-hosted Elasticsearch instance (not a domain name) to
        # use. The given ES domain's endpoint will be looked up dynamically.
        'AZUL_ES_DOMAIN': 'azul-index-{AZUL_DEPLOYMENT_STAGE}',

        # Boolean value, 1 to share `dev` ES domain, 0 to create your own
        'AZUL_SHARE_ES_DOMAIN': '0',

        # Prefix to describe ES indices
        'AZUL_INDEX_PREFIX': 'azul',

        # The number of nodes in the AWS-hosted Elasticsearch cluster
        'AZUL_ES_INSTANCE_COUNT': None,

        # The EC2 instance type to use for a cluster node.
        #
        # Indexing performance benefits from the increased memory offered
        # by the `r` family, especially now that the number of shards is
        # tied to the indexer Lambda concurrency.
        #
        'AZUL_ES_INSTANCE_TYPE': None,

        # The size of the EBS volume backing each cluster node. Set to 0 when
        # using an instance type with SSD volumes.
        #
        'AZUL_ES_VOLUME_SIZE': '0',

        # Elasticsearch operation timeout in seconds
        # matches AWS' own timeout on the ELB sitting in front of ES:
        # https://forums.aws.amazon.com/thread.jspa?threadID=233378
        'AZUL_ES_TIMEOUT': '60',

        # The name of the bucket where Terraform and Chalice maintain their
        # state, allowing multiple developers to collaboratively use those
        # frameworks on a single Azul deployment.
        #
        # If your developers assume a role via Amazon STS, the bucket should
        # reside in the same region as the Azul deployment. This is because
        # temporary STS AssumeRole credentials are specific to a region and
        # won't be recognized by an S3 region that's different from the one
        # the temporary credentials were issued in:
        #
        # AuthorizationHeaderMalformed: The authorization header is malformed;
        # the region 'us-east-1' is wrong; expecting 'us-west-2' status code:
        # 400.
        #
        # To account for the region specificity of the bucket, you may want to
        # include the region name at then end of the bucket name. That way you
        # can have consistent bucket names across regions.
        #
        'AZUL_VERSIONED_BUCKET': 'edu-ucsc-gi-singlecell-azul-config-dev.{AWS_DEFAULT_REGION}',

        # The number of workers pulling files from the DSS repository.
        # There is one such set of repository workers per index worker.
        'AZUL_DSS_WORKERS': '8',

        # The number of workers pulling metadata from the TDR repository.
        # There is one such set of repository workers per index worker.
        # Using one worker as opposed to 8 (or 4) improved the indexing time
        # noticeably because it reduced retries due to exceeding BigQuery's
        # limit on the # of concurrent queries. Using two workers wasn't
        # significantly faster.
        'AZUL_TDR_WORKERS': '1',

        # The number of times a deployment has been destroyed and rebuilt. Some
        # services used by Azul do not support the case of a resource being
        # recreated under the same name as a previous incarnation. The name of
        # such resources will include this value, therefore making the names
        # distinct. If a deployment is being rebuilt, increment this value in
        # the deployment's `environment.py` file.
        'AZUL_DEPLOYMENT_INCARNATION': '0',

        # The name of the Google Cloud service account to represent the
        # deployment. It is used to access all (meta)data in Google-based
        # repositories. If unset, a canonical resource name will be used. That
        # default allows one such account per Azul deployment and Google Cloud
        # project.
        'AZUL_GOOGLE_SERVICE_ACCOUNT': 'azul-ucsc-{AZUL_DEPLOYMENT_INCARNATION}-{AZUL_DEPLOYMENT_STAGE}',

        # The name of the Google Cloud service account to be created and used
        # for accessing public (not access-controlled) (meta)data in Google-
        # based repositories anonymously i.e., without authentication. Used for
        # determining the limits of public access to TDR.
        #
        'AZUL_GOOGLE_SERVICE_ACCOUNT_PUBLIC': 'azul-ucsc-{AZUL_DEPLOYMENT_INCARNATION}-public-{AZUL_DEPLOYMENT_STAGE}',

        # The name of the Google Cloud service account to be created and used
        # to simulate access from users who are logged in but not registered
        # with SAM.
        'AZUL_GOOGLE_SERVICE_ACCOUNT_UNREGISTERED': 'azul-ucsc-{AZUL_DEPLOYMENT_INCARNATION}-unreg-{AZUL_DEPLOYMENT_STAGE}',

        # The number of concurrently running lambda executions for the
        # contribution and aggregation stages of indexing, respectively.
        # Concurrency for the retry lambdas of each stage can be configured
        # separately via a '/' separator, e.g. '{normal concurrency}/{retry concurrency}'.
        # Chalice creates one Lambda function for handling HTTP requests from
        # API Gateway and one additional Lambda function per event handler. The
        # concurrency limit applies to each such function independently. See
        # https://docs.aws.amazon.com/lambda/latest/dg/concurrent-executions.html
        # for details. These settings may also be used to drive other scaling
        # choices. For example, the non-retry contribution concurrency
        # determines the number of shards in Elasticsearch.
        #
        'AZUL_CONTRIBUTION_CONCURRENCY': '64',
        'AZUL_AGGREGATION_CONCURRENCY': '64',

        # The name of the S3 bucket where the manifest API stores the downloadable
        # content requested by client.
        'AZUL_S3_BUCKET': 'edu-ucsc-gi-singlecell-azul-storage-{AZUL_DEPLOYMENT_STAGE}',

        # Name of the Route 53 zone used for shortened URLs.
        # This hosted zone will have to be created manually prior to running
        # `make deploy`. Personal deployments typically share a zone with the
        # `dev` deployment.
        # If this variable is empty, a route 53 record will not be created and it
        # is assumed that the record and zone have been created manually.  This is
        # the case for staging, integration, and prod environments.
        'AZUL_URL_REDIRECT_BASE_DOMAIN_NAME': 'url.singlecell.gi.ucsc.edu',

        # Full domain name to be used in the URL redirection URLs
        # This is also used as the name of the S3 bucket used to store URL
        # redirection objects
        'AZUL_URL_REDIRECT_FULL_DOMAIN_NAME': '{AZUL_DEPLOYMENT_STAGE}.{AZUL_URL_REDIRECT_BASE_DOMAIN_NAME}',

        # Enables deployment of monitoring resources and enables logging of all API
        # Gateway traffic through CloudWatch (1 yes, 0 no).
        # Typically only set for main deployments.
        'AZUL_ENABLE_MONITORING': '0',

        # Identifies the DSS repository endpoint and prefix to index.
        # The syntax in EBNF is:
        #
        # source = endpoint,
        #          ':', [ prefix ],
        #          '/', partition prefix length ;
        #
        # `endpoint` is the URL of the DSS instance. For `prefix` and
        # `partition prefix length` see `AZUL_CATALOGS` above.
        #
        # Examples:
        #
        # https://dss.data.humancellatlas.org/v1:/1
        # https://dss.data.humancellatlas.org/v1:aa/1
        'AZUL_DSS_SOURCE': None,

        # A short string (no punctuation allowed) that identifies a Terraform
        # component i.e., a distinct set of Terraform resources to be deployed
        # together but separately from resources in other components. They are
        # typically defined in a subdirectory of the `terraform` directory and have
        # their own directory under `deployments`. The main component is identified
        # by the empty string and its resources are defined in the `terraform`
        # directory.
        'azul_terraform_component': '',

        # The slug of a the Github repository hosting this fork of Azul
        'azul_github_project': 'DataBiosphere/azul',

        # An Github REST API access token with permission to post status checks to
        # the repository defined in `azul_github_project`.
        'azul_github_access_token': '',

        'PYTHONPATH': '{project_root}/src:{project_root}/test',
        'MYPYPATH': '{project_root}/stubs',

        # The path of the directory where the public key infrastructure files
        # are managed on developer, operator and administrator machines. The
        # directory contains secrets so it Must reside outside of the project
        # root so as to prevent accidentally committing those secrets to source
        # control.
        #
        'azul_easyrsa_pki': xdg_data_home + '/azul/easyrsa',

        # Set the Terraform state directory. Since we reuse deployment names across
        # different AWS accounts, we need a discriminator for the state directory and
        # the best I could come up with is the profile name.
        #
        'TF_DATA_DIR': '{project_root}/deployments/.active/.terraform.{AWS_PROFILE}',

        # BigQuery dataset location of the TDR snapshots the deployment is
        # configured to index. All configured snapshots must reside in the same
        # location.
        #
        # https://cloud.google.com/bigquery/docs/locations
        'AZUL_TDR_SOURCE_LOCATION': None,

        # The full set of BigQuery dataset locations of the TDR snapshots
        # indexed across all deployments. The value of
        # ``AZUL_TDR_SOURCE_LOCATION`` must always be an element of this set.
        #
        'AZUL_TDR_ALLOWED_SOURCE_LOCATIONS': json.dumps([
            'US',
            'us-central1'
        ]),

        # BigQuery offers two modes for queries: interactive queries, which are
        # started immediately and limited to 100 concurrent queries, and batch
        # queries, which are not started until resources are available and do
        # not count towards the concurrency limit. Set this variable to 1 to
        # enable batch mode.
        #
        # https://cloud.google.com/bigquery/docs/running-queries
        'AZUL_BIGQUERY_BATCH_MODE': '0',

        # Timeout in seconds for requests to Terra. Two different values are
        # configured, separated by a colon. The first is for time-sensitive
        # contexts such as API Gateway. The second is for contexts in which we
        # can afford to be more patient.
        'AZUL_TERRA_TIMEOUT': '5:20',

        # The URL of the Terra Data Repository instance to index metadata from.
        'AZUL_TDR_SERVICE_URL': None,

        # The URL of an instance of Broad Institute's SAM.
        # This needs to point to the SAM instance that's used by the TDR
        # instance configured in `AZUL_TDR_SERVICE_URL`.
        'AZUL_SAM_SERVICE_URL': None,

        # OAuth2 Client ID to be used for authenticating users. See section
        # 3.2 of the README
        'AZUL_GOOGLE_OAUTH2_CLIENT_ID': None
    }
