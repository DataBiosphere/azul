from collections.abc import (
    Mapping,
)
import json
import os
from typing import (
    Optional,
)


def env() -> Mapping[str, Optional[str]]:
    """
    Returns a dictionary that maps environment variable names to values. The
    values are either None or strings. String values can contain references to
    other environment variables in the form `{FOO}` where FOO is the name of an
    environment variable. See

    https://docs.python.org/3.11/library/string.html#format-string-syntax

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
        # Only variables whose names start in `AZUL_` will be published to a
        # deployed Lambda. Note that by implication, `azul_` variables will not
        # be published, even though they are considered part of Azul. For secret
        # values that should not be printed or logged, use a variable name
        # containing any of the strings `secret`, `password` or `token`, either
        # upper or lower case. Think twice before publishing a variable
        # containing a secret.

        # Configure the catalogs to be managed by this Azul deployment. A
        # catalog is a group of indices populated from a particular source.
        #
        # The AZUL_CATALOGS variable must be a string containing a JSON object
        # of the following shape:
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
        #   },
        #   ...
        # }
        #
        # The `atlas` and `name` properties follow the same, fairly restrictive
        # syntax defined by azul.Config.Catalog.validate_name. `plugin_type` is
        # the name of a child package of `azul.plugins` and `plugin_package` is
        # the name of a child package of that package. The `plugin_type` denotes
        # the purpose (like accessing a repository or transforming metadata) and
        # `plugin_package` denotes the concrete implementation of how to fulfill
        # that purpose.
        #
        # The first catalog listed is the default catalog.
        #
        # A source represents a TDR dataset, TDR snapshot, or canned staging
        # area to index. Each source is a string matching the following EBNF
        # grammar:
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
        # The `prefix` is an optional string of hexadecimal digits constraining
        # the set of indexed subgraphs from the source. A subgraph will be
        # indexed if its UUID begins with the `prefix`. The default `prefix` is
        # the empty string.
        #
        # The partition prefix length is an integer that is used to further
        # partition the set of indexed subgraphs. Each partition is assigned a
        # prefix of `partition prefix length` hexadecimal digits. A subgraph
        # belongs to a partition if its UUID starts with the overall `prefix`
        # followed by the partition's prefix. The number of partitions of a
        # source is therefore `16 ** partition prefix length`. Partition
        # prefixes that are too long result in many small or even empty
        # partitions and waste some amount of resources. Partition prefixes that
        # are too short result in few large partitions that could exceed the
        # memory and running time limitations of the AWS Lambda function that
        # processes them. If in doubt err on the side of too many small
        # partitions.
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
        # This variable tends to be large. If you get `Argument list too long`
        # after sourcing the environment, a last-resort option is to compress
        # the variable. The application automatically detects a compressed value
        # and decompresses it on the fly. If the uncompressed definition of this
        # variable is
        #
        # 'AZUL_CATALOGS': json.dumps({
        #   ...
        # }),
        #
        # the compressed version of that definition would be
        #
        # 'AZUL_CATALOGS': base64.b64encode(bz2.compress(json.dumps({
        #   ...
        # }).encode())).decode('ascii'),
        #
        'AZUL_CATALOGS': None,

        # The name of a catalog to perform reindex or other operational tasks on.
        #
        'azul_current_catalog': None,

        # The Account ID number for AWS
        #
        'AZUL_AWS_ACCOUNT_ID': None,

        # The region of the Azul deployment. This variable is primarily used by
        # the AWS CLI, by TerraForm, botocore and boto3 but Azul references it
        # too. This variable is typically set in deployment-specific
        # environments.
        #
        'AWS_DEFAULT_REGION': None,

        # The name of the billing account that pays for this deployment.
        #
        'AZUL_BILLING': None,

        # The email address of a user that owns the cloud resources in the
        # current deployment. This will become the value of the Owner tag on all
        # resources.
        #
        'AZUL_OWNER': None,

        # An email address to subscribe to the SNS topic for monitoring
        # notifications in the current deployment. Warn all members of the group
        # to ignore any "Subscription Confirmation" emails and not to click the
        # "Confirm subscription" link contained within, since doing so would
        # confirm the subscription in a way that allows anyone with an
        # unsubscribe link to unsubscribe the entire group from the topic.
        # Instead, confirmation of the subscription should be done when prompted
        # to do so during the `make deploy` process.
        #
        'AZUL_MONITORING_EMAIL': None,

        # Controls the verbosity of application logs. Use 0 for no debug logging
        # 1 for debug logging by application code and info logging by other code
        # and 2 for debug logging by all code. This also controls app.debug, a
        # Chalice setting that causes an app to return a traceback in the body
        # of error responses: Setting AZUL_DEBUG to 0 disables the traceback
        # (app.debug = False), 1 or higher enable it (app.debug = True). See
        # https://github.com/aws/chalice#tutorial-error-messages for more.
        #
        'AZUL_DEBUG': '0',

        # Whether to create and populate an index for replica documents.
        'AZUL_ENABLE_REPLICAS': '1',

        # Maximum number of conflicts to allow before giving when writing
        # replica documents.
        'AZUL_REPLICA_CONFLICT_LIMIT': '10',

        # The name of the current deployment. This variable controls the name of
        # all cloud resources and is the main vehicle for isolating cloud
        # resources between deployments.
        #
        'AZUL_DEPLOYMENT_STAGE': None,

        # The Docker registry containing all 3rd party images used by this
        # project, including images used locally, in FROM clauses, for CI/CD or
        # GitLab. Must be empty or end in a slash. All references to 3rd party
        # images must point at the registry defined here, ideally by prefixing
        # the image reference with a reference to this variable. The registry
        # and the images therein are managed by the `shared` TF component, which
        # copies images from the upstream registry into the Azul registry. A
        # 3rd-party image at `<registry>/<username>/<repository>:tag`, is stored
        # as `${azul_docker_registry>}<registry>/<username>/<repository>:tag` in
        # the Azul registry. To disable the use of the Azul registry, set this
        # variable to the empty string.
        #
        'azul_docker_registry': '{AZUL_AWS_ACCOUNT_ID}.dkr.ecr.'
                                '{AWS_DEFAULT_REGION}.amazonaws.com/',

        # The version of Docker used throughout the system.
        #
        # This variable is duplicated in a file called `environment.boot`
        # because it is referenced in the early stages of the GitLab build. In
        # order to update that file, you must run `_refresh && make
        # environment.boot` after changing the definition below.
        #
        # This variable is not intended to be overridden per deployment or
        # locally.
        #
        # Modifying this variable requires running `make image_manifests.json`,
        # redeploying the `shared` and `gitlab` components, as well as building
        # and pushing the executor image (see terraform/gitlab/runner/Dockerfile
        # for how).
        #
        'azul_docker_version': '25.0.5',

        # The version of Python used throughout the system.
        #
        # This variable is duplicated in a file called `environment.boot`
        # because it is referenced in the early stages of the GitLab and GitHub
        # Actions build. In order to update that file, you must run `_refresh &&
        # make environment.boot` after changing the definition below.
        #
        # This variable is not intended to be overridden per deployment or
        # locally.
        #
        # Modifying this variable requires running `make image_manifests.json`
        # and redeploying the `shared` component.
        #
        'azul_python_version': '3.11.9',

        # The version of Terraform used throughout the system.
        #
        # This variable is duplicated in a file called `environment.boot`
        # because it is referenced in the early stages of the GitLab build. In
        # order to update that file, you must run `_refresh && make
        # environment.boot` after changing the definition below.
        #
        # This variable is not intended to be overridden per deployment or
        # locally.
        #
        'azul_terraform_version': '1.6.6',

        # A dictionary mapping the short name of each Docker image used in Azul
        # to its fully qualified name. Note that a change to any of the image
        # references below requires running `make image_manifests.json` and
        # redeploying the `shared` TF component.

        'azul_docker_images': json.dumps({
            # Updating the Docker image also requires building and pushing the
            # executor image (see terraform/gitlab/runner/Dockerfile for how).
            'docker': {
                'ref': 'docker.io/library/docker:{azul_docker_version}',
                'url': 'https://hub.docker.com/_/docker'
            },
            # Run `_refresh && make environment.boot` after modifying the Python
            # image reference.
            'python': {
                'ref': 'docker.io/library/python:{azul_python_version}-bullseye',
                'url': 'https://hub.docker.com/_/python',
            },
            'pycharm': {
                'ref': 'docker.io/ucscgi/azul-pycharm:2023.3.5-21',
                'url': 'https://hub.docker.com/repository/docker/ucscgi/azul-pycharm',
                'is_custom': True
            },
            'elasticsearch': {
                'ref': 'docker.io/ucscgi/azul-elasticsearch:7.17.20-16',
                'url': 'https://hub.docker.com/repository/docker/ucscgi/azul-elasticsearch',
                'is_custom': True
            },
            'bigquery_emulator': {
                'ref': 'ghcr.io/hannes-ucsc/bigquery-emulator:azul'
            },
            # Updating any of the four images below additionally requires
            # redeploying the `gitlab` TF component.
            'clamav': {
                # FIXME: https://github.com/DataBiosphere/azul/issues/6022
                #        Keep ClamAV at 1.2.1 until 1.3.x failure is resolved
                'ref': 'docker.io/clamav/clamav:1.2.1-27',
                'url': 'https://hub.docker.com/r/clamav/clamav'
            },
            'gitlab': {
                'ref': 'docker.io/gitlab/gitlab-ce:16.11.1-ce.0',
                'url': 'https://hub.docker.com/r/gitlab/gitlab-ce'
            },
            'gitlab_runner': {
                'ref': 'docker.io/gitlab/gitlab-runner:ubuntu-v16.11.0',
                'url': 'https://hub.docker.com/r/gitlab/gitlab-runner'
            },
            'dind': {
                'ref': 'docker.io/library/docker:{azul_docker_version}-dind',
                'url': 'https://hub.docker.com/_/docker'
            },
            # The images below are not used within the security boundary:
            '_signing_proxy': {
                'ref': 'docker.io/cllunsford/aws-signing-proxy:0.2.2',
                'url': 'https://hub.docker.com/r/cllunsford/aws-signing-proxy'
            },
            '_cerebro': {
                'ref': 'docker.io/lmenezes/cerebro:0.9.4',
                'url': 'https://hub.docker.com/r/lmenezes/cerebro'
            },
            '_kibana': {
                'ref': 'docker.io/bitnami/kibana:7.10.2',
                'url': 'https://hub.docker.com/r/bitnami/kibana'
            }
        }),

        # Whether to enable direct access to objects in the DSS main bucket. If
        # 0, bundles and files are retrieved from the DSS using the GET
        # /bundles/{uuid} and GET /files/{UUID} endpoints. If 1, S3 GetObject
        # requests are made directly to the underlying bucket. This requires
        # intimate knowledge of DSS implementation details but was ten times
        # faster. Recent optimizations to the DSS (mainly, the delayed eviction
        # of metadata files from the checkout bucket made the performance gains
        # less dramatic but the first hit to a metadata file is still going to
        # be slow because the objects needs to be checked out. Aside from the
        # latency improvements on a single request, direct access also bypasses
        # DSS lambda concurrency limits, thereby increasing in the throughput of
        # the Azul indexer, which is especially noticeable during reindexing and
        # scale testing.
        #
        # More importantly, direct access needs to be enabled for deletions to
        # work properly as the Azul indexer needs to know the metadata of the
        # deleted bundle in order to place the correct tombstone contributions
        # into its index. Direct access is also required for the Azul service's
        # DSS files proxy and DOS/DRS endpoints. Disabling DSS direct access
        # will break these endpoints.
        #
        'AZUL_DSS_DIRECT_ACCESS': '0',

        # An optional ARN of a role to assume when directly accessing a DSS
        # bucket. This can be useful when the DSS buckets are not owned by the
        # same AWS account owning the current Azul deployment. If there is
        # another Azul deployment in the account owning the DSS bucket, the role
        # assumed by the other Azul indexer would be an obvious candidate for
        # the current deployment's indexer to assume for direct access.
        # Presumably that other indexer has sufficient privileges to directly
        # access the DSS buckets.
        #
        # The character '*' will be replaced with the name of the lambda wishing
        # to gain access. This parameterization can be used to have the indexer
        # lambda in the native deployment assume the role of the indexer lambda
        # in the foreign deployment, while the native service lambda assumes the
        # role of the foreign service lambda.
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

        # The name of the hosted zone in Route 53 in which to create user
        # friendly domain names for various API gateways. This hosted zone will
        # have to be created manually prior to running `make deploy`. The value
        # is typically not deployment specific. A subdomain will automatically
        # be created for each deployment.
        'AZUL_DOMAIN_NAME': None,

        # An optional list of roles in other AWS accounts that can assume the
        # IAM role normally assumed by lambda functions in the active Azul
        # deployment.
        #
        # The syntax is <account>[,<role>...][:<account>[,<role>...]...] where
        # <account> is the numeric AWS account ID and role is a role name with
        # optional * or ? wildcards for the StringLike operator in IAM
        # conditions. Whitespace around separators and at the beginning or end
        # of the value are ignored.
        #
        # This parameter has profound security implications: the external role
        # can do anything an Azul lambda can do. The external account and any
        # principal with IAM access in that account, not just the specified
        # roles, must be fully trusted.
        #
        # This configuration is typically used to enable an external Azul
        # deployment to directly access the same DSS buckets the active
        # deployment has direct access to.
        #
        'AZUL_EXTERNAL_LAMBDA_ROLE_ASSUMPTORS': None,

        # The domain name of the HCA DRS endpoint. The service lambda serves
        # requests under both its canonical domain name as well as the domain
        # name given here. It is assumed that the parent domain of the given
        # domain is a hosted zone in Route 53 that we can create additional
        # certificate validation records in. If unset or set to empty string,
        # the service lambda will only serve requests under its canonical domain
        # name and no validation records will be created in hosted zones other
        # than the zone defined by AZUL_DOMAIN_NAME.
        #
        'AZUL_DRS_DOMAIN_NAME': '',

        # A template for the name of the Route 53 record set in the hosted zone
        # specified by AZUL_DOMAIN_NAME. The character '*' in the template will
        # be substituted with the name of the Lambda function, e.g. `indexer` or
        # `service`. May contain periods.
        #
        'AZUL_SUBDOMAIN_TEMPLATE': '*',

        # Boolean value, 0 to create public APIs, 1 to create private APIs that
        # can only be accessed from within the VPC or through the VPN tunnel
        # into the VPC.
        #
        'AZUL_PRIVATE_API': '0',

        # A prefix to be prepended to the names of AWS Lambda functions and
        # associated resources. Must not contain periods.
        #
        'AZUL_RESOURCE_PREFIX': 'azul',

        # The host and port of the Elasticsearch instance to use. This takes
        # precedence over AZUL_ES_DOMAIN.
        #
        'AZUL_ES_ENDPOINT': None,

        # The name of the AWS-hosted Elasticsearch instance (not a domain name)
        # to use. The given ES domain's endpoint will be looked up dynamically.
        #
        'AZUL_ES_DOMAIN': 'azul-index-{AZUL_DEPLOYMENT_STAGE}',

        # Boolean value, 1 to share `dev` ES domain, 0 to create your own
        #
        'AZUL_SHARE_ES_DOMAIN': '0',

        # The number of nodes in the AWS-hosted Elasticsearch cluster
        #
        'AZUL_ES_INSTANCE_COUNT': None,

        # The EC2 instance type to use for a cluster node.
        #
        # Indexing performance benefits from the increased memory offered by the
        # `r` family, especially now that the number of shards is tied to the
        # indexer Lambda concurrency.
        #
        'AZUL_ES_INSTANCE_TYPE': None,

        # The size of the EBS volume backing each cluster node. Set to 0 when
        # using an instance type with SSD volumes.
        #
        'AZUL_ES_VOLUME_SIZE': '0',

        # Elasticsearch operation timeout in seconds. Matches AWS' own timeout
        # on the ELB sitting in front of ES:
        #
        # https://forums.aws.amazon.com/thread.jspa?threadID=233378
        #
        'AZUL_ES_TIMEOUT': '60',

        # The number of workers pulling files from the DSS repository. There is
        # one such set of repository workers per index worker.
        #
        'AZUL_DSS_WORKERS': '8',

        # The number of workers pulling metadata from the TDR repository. There
        # is one such set of repository workers per index worker. Using one
        # worker as opposed to 8 (or 4) improved the indexing time noticeably
        # because it reduced retries due to exceeding BigQuery's limit on the #
        # of concurrent queries. Using two workers wasn't significantly faster.
        #
        'AZUL_TDR_WORKERS': '1',

        # The number of times a deployment has been destroyed and rebuilt. Some
        # services used by Azul do not support the case of a resource being
        # recreated under the same name as a previous incarnation. The name of
        # such resources will include this value, therefore making the names
        # distinct. If a deployment is being rebuilt, increment this value in
        # the deployment's `environment.py` file.
        #
        'AZUL_DEPLOYMENT_INCARNATION': '0',

        # The name of the Google Cloud project to host the Azul deployment.
        # There are two methods of authenticating with Google Cloud: setting the
        # GOOGLE_APPLICATION_CREDENTIALS environment variable to point to the
        # key file of a Google service account, or setting the application
        # default credentials using the `gcloud` CLI interactive login.
        #
        'GOOGLE_PROJECT': None,

        # The path of the directory where the Google Cloud Python libraries and
        # the Google Cloud CLI (gcloud) put their state. If this variable is not
        # set, the state is placed in ~/.config/gcloud by default. Since we want
        # to segregate this state per working copy and deployment, we set this
        # variable to the path of a deployment-specific directory in the working
        # copy. Note that this variable does not affect the Google Cloud
        # libraries for Go, or the Google Cloud provider for Terraform which
        # uses these Go libraries. Luckily, the Go libraries don't write any
        # state, they only read credentials from the location configured via
        # GOOGLE_APPLICATION_CREDENTIALS below.
        #
        'CLOUDSDK_CONFIG': '{project_root}/deployments/.active/.gcloud',

        # The path of a JSON file with credentials for an authorized user or a
        # service account. The Google Cloud libraries for Python and Go will
        # load the so-called ADC (Application-default credentials) from this
        # file and use them for any Google Cloud API requests. According to
        # Google documentation, if this variable is not set,
        # ~/.config/gcloud/application_default_credentials.json is used.
        # However, the Google Cloud SDK and Python libraries will only default
        # to that if CLOUDSDK_CONFIG is not set. If it is,
        # $CLOUDSDK_CONFIG/application_default_credentials.json is used. Since
        # the Go libraries are unaffected by CLOUDSDK_CONFIG, the officially
        # documented default applies. We'll work around the inconsistent
        # defaults by setting both variables explicitly.
        #
        # If the azul_google_user variable is set, the _login helper defined
        # in `environment` will populate this file with credentials (a
        # long-lived refresh token) for that user. It does so by logging that
        # user into Google Cloud, which requires a web browser. As a
        # convenience, and to avoid confusion, it will, at the same time,
        # provision credentials for the Google Cloud CLI, in a different file,
        # but also in the directory configured via CLOUDSDK_CONFIG above.
        #
        # To have a service account (as opposed to a user account) manage Google
        # Cloud resources, leave azul_google_user unset and change this variable
        # to point to a file with the private key of that service account. Note
        # that the service account would have to be different from the one whose
        # name is set in AZUL_GOOGLE_SERVICE_ACCOUNT. In fact, the service
        # account from this variable is used to manage those other service
        # accounts, so generally, it needs elevated permissions to the project.
        # We used to call this type of account "personal service account" but we
        # don't use that type anymore. GitLab is nowadays the only place where
        # this variable is set to service account credentials.
        #
        'GOOGLE_APPLICATION_CREDENTIALS': '{CLOUDSDK_CONFIG}/application_default_credentials.json',

        # The name of a Google user account with authorization to manage the
        # Google Cloud resources in the project referred to by GOOGLE_PROJECT.
        # If this variable is not set, GOOGLE_APPLICATION_CREDENTIALS must be
        # changed to the path of a file containing service account credentials.
        #
        'azul_google_user': None,

        # The name of the Google Cloud service account to represent the
        # deployment. This service account will be created automatically during
        # deployment and will then be used to access (meta)data in Google-based
        # repositories, like the Terra Data Repository (TDR). If unset, a
        # canonical resource name will be used. That default allows one such
        # account per Azul deployment and Google Cloud project.
        #
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
        #
        'AZUL_GOOGLE_SERVICE_ACCOUNT_UNREGISTERED':
            'azul-ucsc-{AZUL_DEPLOYMENT_INCARNATION}-unreg-{AZUL_DEPLOYMENT_STAGE}',

        # The number of concurrently running lambda executions for the
        # contribution and aggregation stages of indexing, respectively.
        # Concurrency for the retry lambdas of each stage can be configured
        # separately via a '/' separator, e.g. '{normal concurrency}/{retry
        # concurrency}'. Chalice creates one Lambda function for handling HTTP
        # requests from API Gateway and one additional Lambda function per event
        # handler. The concurrency limit applies to each such function
        # independently. See
        #
        # https://docs.aws.amazon.com/lambda/latest/dg/concurrent-executions.html
        #
        # for details. These settings may also be used to drive other scaling
        # choices. For example, the non-retry contribution concurrency
        # determines the number of shards in Elasticsearch.
        #
        'AZUL_CONTRIBUTION_CONCURRENCY': '64',
        'AZUL_AGGREGATION_CONCURRENCY': '64',

        # The name of the S3 bucket where the manifest API stores the downloadable
        # content requested by client.
        #
        'AZUL_S3_BUCKET': None,

        # Collect and monitor important health metrics of the deployment (1 yes, 0 no).
        # Typically only enabled on main deployments.
        #
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
        #
        'AZUL_DSS_SOURCE': None,

        # A short string (no punctuation allowed) that identifies a Terraform
        # component i.e., a distinct set of Terraform resources to be deployed
        # together but separately from resources in other components. They are
        # typically defined in a subdirectory of the `terraform` directory and have
        # their own directory under `deployments`. The main component is identified
        # by the empty string and its resources are defined in the `terraform`
        # directory.
        #
        'azul_terraform_component': '',

        # Set this to '1' in order to skip deleting certain types of unused
        # resources during `terraform apply`. The unused resources can be
        # deleted later by running `terraform apply` again with this variable
        # reset to its default. The resource types affected by this flag are:
        #
        # - Unused Docker images in ECR
        #
        # This variable should be not be overridden in other `environment.py*`
        # files. It should be left at its default, except temporarily, when
        # running `terraform apply`.
        #
        'azul_terraform_keep_unused': '0',

        # The slug of the Github repository hosting this fork of Azul
        #
        'azul_github_project': 'DataBiosphere/azul',

        # A Github REST API access token with permission to post status checks to
        # the repository defined in `azul_github_project`.
        #
        'azul_github_access_token': '',

        # A GitLab private access token with scopes `read_api`, `read_registry`
        # and `write_registry`. This variable is typically only set on developer
        # machines. In GitLab CI/CD pipelines, this variable should NOT be set
        # because a different type of token is automatically provided via the
        # CI_JOB_TOKEN variable.
        #
        'azul_gitlab_access_token': None,

        # The name of the user owning the token in `azul_gitlab_access_token`.
        #
        'azul_gitlab_user': None,

        'PYTHONPATH': '{project_root}/src:{project_root}/test',
        'MYPYPATH': '{project_root}/stubs',

        # The path of a directory containing a wheel for each runtime
        # dependency. Settng this variable causes our fork of Chalice to skip
        # the downloading and building of wheels and instead install the wheels
        # from that directory. The wheels must be compatible with the AWS
        # Lambda platform.
        #
        'azul_chalice_bin': '{project_root}/bin/wheels/runtime',

        # Stop `pip` from nagging us about updates. We update pip regularly like
        # any other dependency. There is nothing special about `pip` that would
        # warrant the distraction.
        #
        'PIP_DISABLE_PIP_VERSION_CHECK': '1',

        # FIXME: Remove once we upgrade to botocore 1.28.x
        #        https://github.com/DataBiosphere/azul/issues/4560
        #
        'BOTO_DISABLE_COMMONNAME': 'true',

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

        # Make Terraform's deprecation warnings more compact
        #
        **{
            'TF_CLI_ARGS_' + command: '-compact-warnings'
            for command in ['validate', 'plan', 'apply']
        },

        # BigQuery dataset location of the TDR snapshots the deployment is
        # configured to index. All configured snapshots must reside in the same
        # location.
        #
        # https://cloud.google.com/bigquery/docs/locations
        #
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
        #
        'AZUL_BIGQUERY_BATCH_MODE': '0',

        # The URL of the Terra Data Repository instance to index metadata from.
        #
        'AZUL_TDR_SERVICE_URL': None,

        # The URL of an instance of Broad Institute's SAM.
        # This needs to point to the SAM instance that's used by the TDR
        # instance configured in `AZUL_TDR_SERVICE_URL`.
        #
        'AZUL_SAM_SERVICE_URL': None,

        # The URL of Terra's DUOS service from which to index descriptions of
        # AnVIL datasets. If left unset, this step is skipped during indexing.
        #
        'AZUL_DUOS_SERVICE_URL': None,

        # OAuth2 Client ID to be used for authenticating users. See section
        # 3.2 of the README
        #
        'AZUL_GOOGLE_OAUTH2_CLIENT_ID': None,

        # Maps a branch name to a list of names of deployments the branch may be
        # deployed to. When building a given branch, a GitLab instance uses this
        # variable to automatically determine the target deployment by using the
        # first item of the value for that branch. An empty key signifies any
        # other branch not mentioned explicitly, or a detached HEAD. Note that
        # this variable is likely being overridden on a GitLab instance so that
        # feature branches are deployed to the sandbox deployment.
        #
        # Only shared deployments are mentioned here. A shared deployment is one
        # that is not personal. A personal deployment is owned and maintained by
        # a single person. Shared deployments can be either main or sandbox
        # deployments. A sandbox deployment is used to test feature branches. A
        # main deployment is a shared deployment that is not a sandbox. Main
        # deployments can be either stable or lower. A stable (aka production)
        # deployment is one that must be kept operational at all times because
        # it is exposed to the public *and* serves external users for production
        # purposes. A lower (aka unstable) deployment is a main deployment that
        # is not stable.
        #
        # ╔════════════╗ ╔══════════════════════════════════════════════════╗
        # ║  Personal  ║ ║                      Shared                      ║
        # ║            ║ ║ ╔═════════════╗ ╔══════════════════════════════╗ ║
        # ║            ║ ║ ║   Sandbox   ║ ║             Main             ║ ║
        # ║            ║ ║ ║             ║ ║ ╔═════════════╗ ╔══════════╗ ║ ║
        # ║            ║ ║ ║             ║ ║ ║    Lower    ║ ║  Stable  ║ ║ ║
        # ║ ┌────────┐ ║ ║ ║ ┌─────────┐ ║ ║ ║ ┌─────────┐ ║ ║ ┌──────┐ ║ ║ ║
        # ║ │ hannes │ ║ ║ ║ │ sandbox │ ║ ║ ║ │   dev   │ ║ ║ │ prod │ ║ ║ ║
        # ║ └────────┘ ║ ║ ║ └─────────┘ ║ ║ ║ └─────────┘ ║ ║ └──────┘ ║ ║ ║
        # ║            ║ ║ ║ ┌─────────┐ ║ ║ ║ ┌─────────┐ ║ ║          ║ ║ ║
        # ║            ║ ║ ║ │anvilbox │ ║ ║ ║ │anvildev │ ║ ║          ║ ║ ║
        # ║            ║ ║ ║ └─────────┘ ║ ║ ║ └─────────┘ ║ ║          ║ ║ ║
        # ║            ║ ║ ║ ┌─────────┐ ║ ║ ║ ┌─────────┐ ║ ║          ║ ║ ║
        # ║            ║ ║ ║ │hammerbox│ ║ ║ ║ │anvilprod│ ║ ║          ║ ║ ║
        # ║            ║ ║ ║ └─────────┘ ║ ║ ║ └─────────┘ ║ ║          ║ ║ ║
        # ║            ║ ║ ║             ║ ║ ╚═════════════╝ ╚══════════╝ ║ ║
        # ║            ║ ║ ╚═════════════╝ ╚══════════════════════════════╝ ║
        # ╚════════════╝ ╚══════════════════════════════════════════════════╝
        #
        'azul_shared_deployments': json.dumps({
            'develop': ['dev', 'sandbox', 'anvildev', 'anvilbox'],
            'prod': ['prod'],
            'anvilprod': ['anvilprod', 'hammerbox']
        }),

        # A dictionary with one entry per browser or portal site that is to be
        # managed by the `browser` TF component of the current Azul deployment.
        #
        # {
        #     'ucsc/data-browser': {  // The path of the GitLab project hosting
        #                             // the source code for the site. The
        #                             // project must exist on the GitLab
        #                             // instance managing the current Azul
        #                             // deployment.
        #
        #         'main': {  // The name of the branch (in that project) from
        #                    // which the site's content tarball was built
        #
        #             'anvil': {  // The site name. Typically corresponds to an
        #                         // Azul atlas as defined in the AZUL_CATALOGS.
        #
        #                 'domain': '{AZUL_DOMAIN_NAME}',  // The domain name of
        #                                                  // the site
        #
        #                 'bucket': 'browser',  // The TF resource name (in the
        #                                       // `browser` component) of the
        #                                       // S3 bucket hosting the site
        #                                       // ('portal' or 'browser')
        #
        #                 'tarball_path': 'explore',  // The path to the site's
        #                                             // content in the tarball
        #
        #                 'real_path': 'explore/anvil-cmg'  // The path of that
        #                                                   // same content in
        #                                                   // the bucket
        #             }
        #         }
        #     }
        # }
        #
        # The real_path and tarball_path properties define a path mapping from
        # files in the tarball to objects in the S3 bucket. In the above
        # example, the tarball entry `explore/foo.html` will be stored in the S3
        # bucket under the key `explore/anvil-cmg/foo.html`. The tarball entry
        # `fubaz/bar.html` will be store in the S3 bucket under the same key
        # (`fubaz/bar.html`). It is uncommon to have such tarball entries. Site
        # tarballs should always contain a single root directory.
        #
        'azul_browser_sites': json.dumps({}),

        # 1 if current deployment is a main deployment with the sole purpose of
        # testing feature branches in GitLab before they are merged to the
        # develop branch, 0 otherwise. Personal deployments have this set to 0.
        #
        'AZUL_IS_SANDBOX': '0',

        # A list of names of AWS IAM roles that should be given permission to
        # manage incidents with AWS support as defined in CIS rule 1.20:
        #
        # https://docs.aws.amazon.com/securityhub/latest/userguide/securityhub-cis-controls.html#securityhub-cis-controls-1.20
        #
        'azul_aws_support_roles': json.dumps([]),

        # A dict containing the contact details of the AWS account alternate
        # contact for security communications. The keys must include those
        # required by the aws_account_alternate_contact Terraform resource,
        # however should exclude the key alternate_contact_type.
        #
        # https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/account_alternate_contact
        #
        'azul_security_contact': None,

        # To enable Slack integration with AWS Chatbot, set this variable to
        # a JSON object containing the IDs of the Slack workspace and Slack
        # channel for Chatbot to post notifications to. For example:
        #
        # 'azul_slack_integration': json.dumps({
        #     'workspace_id': 'your-workspace-id',
        #     'channel_id': 'your-channel-id'
        # })
        #
        'azul_slack_integration': None,

        # The CIDR of the Azul VPC. The VPC is shared by all deployments. Keep
        # that in mind when deciding on the width of the netmask. Also note that
        # the CIDR is split into four subnets. In other words, a /8 CIDR is
        # probably not a suitable choice. In the platform-hca-dev account, for
        # example, there are currently sixty network interfaces and most of them
        # are in two of the four subnets. Dividing a /8 CIDR into four subnets
        # would only allow for 64 IP addresses in each of the subnets, which is
        # dangerously close to that current number of network interfaces.
        #
        'azul_vpc_cidr': None,

        # The CIDR of the subnet used for the IP addresses on each end of a VPN
        # tunnel. AWS uses NAT between the tunnel IPs and the two IPs of an ENI
        # in the Azul VPC. This subnet can't overlap the VPC CIDR and the subnet
        # mask must be less than 22 bits.
        #
        'azul_vpn_subnet': None
    }
