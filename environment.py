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
    return {

        # Configure the catalogs to be managed by this Azul deployment. A
        # catalog is a group of indices populated from a particular source. The
        # syntax is
        #
        # catalog, (',', catalog)*
        #
        # The syntax of each catalog is
        #
        # atlas, ':', catalog_name, (':', plugin_type, '/', plugin_package)+
        #
        # where `atlas` is the name of the atlas the catalog belongs to
        # and `catalog_name` is the name of the catalog. The atlas name and 
        # catalog names follow the same syntax, which is fairly
        # restrictive (see azul.IndexName.validate_catalog_name for details).
        # `plugin_type` is the name of a child package of `azul.plugins` and
        # `plugin_package` is the name of a child package of that package. The
        # `plugin_type` denotes the purpose (like accessing a repository or
        # transforming metadata) and `plugin_package` denotes the concrete
        # implementation of how to fulfill that purpose.
        #
        # The first catalog listed is the default catalog.
        #
        'AZUL_CATALOGS': ','.join([
            'hca:dcp2:repository/tdr:metadata/hca',
            'hca:it2:repository/tdr:metadata/hca'
        ]),

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

        # The URL to the DSS (aka Blue box) REST API
        'AZUL_DSS_ENDPOINT': None,

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

        # The EC2 instance type to use for a cluster node
        # Indexing performance benefits from the increased memory offered
        # by the `r` family, especially now that the number of shards is
        # tied to the indexer Lambda concurrency. 2xlarge was chosen
        # heuristically to accommodate scale tests.
        'AZUL_ES_INSTANCE_TYPE': 'r4.2xlarge.elasticsearch',

        # The size of the EBS volume backing each cluster node
        'AZUL_ES_VOLUME_SIZE': '70',

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

        # Whether to create a subscription to DSS during deployment. Set this
        # variable to 1 to enable `make subscribe` to subscribe the indexer in
        # the active deployment to DSS bundle events. Making a subscription
        # requires authenticating against DSS using a Google service account
        # specific to the indexer in this deployment.
        #
        # `make deploy` will automatically set up that Google service account
        # for the indexer and deposit its credentials into AWS secrets
        # manager. For this to work, you must to configure your *personal*
        # service account credentials in `environment.local` enabling
        # Terraform to create the shared *indexer* service account. The two
        # variables that need to be set are GOOGLE_APPLICATION_CREDENTIALS
        # and GOOGLE_PROJECT. These are well documented. You need to use
        # service account credentials, `gcloud auth login` apparently does
        # not work for this.
        #
        # Set this variable to 0 to prevent the registration of a
        # subscription. Note that disabling the subscription registration
        # won't remove any existing subscriptions. Use `scripts/subscribe.py
        # -U` for that.
        #
        'AZUL_SUBSCRIBE_TO_DSS': '0',

        # The number of times a deployment has been destroyed and rebuilt. Some
        # services used by Azul do not support the case of a resource being
        # recreated under the same name as a previous incarnation. The name of
        # such resources will include this value, therefore making the names
        # distinct. If a deployment is being rebuilt, increment this value in
        # the deployment's `environment.py` file.
        'AZUL_DEPLOYMENT_INCARNATION': '0',

        # The name of the Google Cloud service account to be created and used
        # in conjunction with DSS subscriptions. If unset, a canonical resource
        # name will be used. That default allows one such account per Azul
        # deployment and Google Cloud project.
        'AZUL_GOOGLE_SERVICE_ACCOUNT': 'azul-ucsc-{AZUL_DEPLOYMENT_INCARNATION}-{AZUL_DEPLOYMENT_STAGE}',

        # The number of concurrently running indexer lambda executions. Chalice
        # creates one Lambda function for handling HTTP requests from API Gateway
        # and one additional Lambda function per event handler. The concurrency
        # limit applies to each such function independently. See
        # https://docs.aws.amazon.com/lambda/latest/dg/concurrent-executions.html
        # for details. This setting may also be used to drive other scaling choices,
        # like the number of shards in Elasticsearch.
        #
        'AZUL_INDEXER_CONCURRENCY': '64',

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

        # Boolean value, 1 to upload a manifest in a single request to S3, 0 to
        # upload the manifest in multiple concurrent requests for equal parts of
        # a smaller size. This allows the manifest generation code to start
        # uploading the manifest to S3 while the manifest data is still being
        # fetched from Elasticsearch, shortening the overall time needed to
        # generate and upload the manifest.
        'AZUL_DISABLE_MULTIPART_MANIFESTS': '0',

        # The default bundle UUID prefix to use for reindexing bundles in the DSS
        # and for subscriptions to the DSS. If this variable is set to a non-empty
        # string, only bundles whose UUID starts with the specified string will be
        # indexed.
        'azul_dss_query_prefix': '',

        # The URL Prefix of Fusillade (Authentication Broker Service)
        'AZUL_FUSILLADE_ENDPOINT': 'https://auth.dev.data.humancellatlas.org',

        # A URL pointing at the REST API of the Grafana instance that should host
        # the Azul dashboard. Typically only set for main deployments.
        'azul_grafana_endpoint': None,

        # The user name with which to authenticate against the Grafana instance.
        # Typically only set for main deployments.
        'azul_grafana_user': None,

        # The password with which to authenticate against the Grafana instance.
        # Typically only set for main deployments.
        'azul_grafana_password': None,

        # Maximum batch size for data export to DSS Collection API
        'AZUL_CART_EXPORT_MAX_BATCH_SIZE': '100',

        # The minimum remaining lifespan of the access token (JWT) for cart export
        # in seconds
        'AZUL_CART_EXPORT_MIN_ACCESS_TOKEN_TTL': '3600',

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

        # Set the Terraform state directory. Since we reuse deployment names across
        # different AWS accounts, we need a discriminator for the state directory and
        # the best I could come up with is the profile name.
        #
        'TF_DATA_DIR': '{project_root}/deployments/.active/.terraform.{AWS_PROFILE}',

        # HCA client caches Swagger specs downloaded from the DSS endpoint here
        'XDG_CONFIG_HOME': '{project_root}/.config',

        # Identifies the Terra Data Repository dataset or snapshot to index.
        #
        # The syntax in EBNF is:
        #
        # 'tdr:', Google Cloud project name, ':', ( 'dataset' | 'snapshot' ), '/', 'TDR dataset or snapshot name'
        #
        # Examples:
        #
        # tdr:broad-jade-dev-data:dataset/hca_mvp
        # tdr:broad-jade-dev-data:snapshot/hca_mvp
        #
        # To configure a source specific to a particular catalog, include the
        # catalog name in all upper case in the variable name. For example, to
        # configure a source specific to a catalog named `bar0`, use
        #
        # AZUL_TDR_BAR0_SOURCE
        #
        # If defined, the catalog-specific variable will be used, otherwise this
        # variable will be used.
        #
        'AZUL_TDR_SOURCE': None,

        # The URL of the Terra Data Repository instance to index metadata from.
        'AZUL_TDR_SERVICE_URL': None,

        # The URL of an instance of Broad Institute's SAM.
        # This needs to point to the SAM instance that's used by the TDR
        # instance configured in `AZUL_TDR_SERVICE_URL`.
        'AZUL_SAM_SERVICE_URL': None,

        # To retrieve issue data, you need a GitHub personal access token with
        # the `public_repo` scope at minimum, or the entire `repo` scope to
        # access private repository data. You can generate a token at
        # https://github.com/settings/tokens
        'azul_velocity_github_token': None,

        # GitHub username of the user associated with azul_velocity_github_token
        'azul_velocity_github_user': None,

        # To retrieve issue estimates, you need a ZenHub API key. You can
        # generate one at https://app.zenhub.com/dashboard/tokens
        'azul_velocity_zenhub_key': None,

        # To retrieve relationships between PRs and issues, you need a key for
        # ZenHub's private API. Open a ZenHub workspace in your browser with
        # the network requests pane open, and look for a request to `events`.
        # The key we want is in the X-Authentication-Token header.
        'azul_velocity_zenhub_pkey': None,

        # URL of the ZenHub workspace with all repos selected
        'azul_velocity_zenhub_url': None
    }
