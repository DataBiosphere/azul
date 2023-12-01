The Azul project contains the components that together serve as the backend to
Boardwalk, a web application for browsing genomic data sets.


# 1. Architecture Overview

## 1.1 Components

[Data Store]: https://github.com/HumanCellAtlas/data-store

Azul consists of two components: an indexer and a web service. The Azul indexer
is an AWS Lambda function that responds to web-hook notifications about bundle
addition and deletion events occurring in a [Data Store] instance. The indexer
responds to those notifications by retrieving the bundle's metadata from said
data store, transforming it and writing the transformed metadata into an
Elasticsearch index. The transformation extracts selected entities and
denormalizes the relations between them into a document shape that facilitates
efficient queries on a number of customizable metadata facets.

The Azul web service, another AWS Lambda function fronted by API Gateway, serves
as a thin translation layer between Elasticsearch and the Boardwalk UI,
providing features like pluggable authentication, field name translation and
introspective capabilities such as facet and entity type discovery.

Both the indexer and the web service allow for project-specific customizations
via a plug-in mechanism, allowing the Boardwalk UI codebase to be functionally
generic with minimal need for project-specific behavior.


## 1.2 Architecture Diagram

![Azul architecture diagram](docs/azul-arch.svg)


# 2. Getting Started


## 2.1 Development Prerequisites

- Python, see [environment.boot](environment.boot) for exact version

- The `bash` shell

- GNU make 3.81 or newer

- git 2.36.0 or newer

- [Docker], for running the tests (the community edition is sufficient).
  The minimal required version is uncertain, but 19.03, 18.09, and 17.09 are
  known to work.

- Terraform, to manage deployments. Azul requires a specific version of
  Terraform, which is defined in a variable called `azul_terraform_version` in
  [environment.py](environment.py). If you have a working Azul checkout, you can
  run `python -m azul config.terraform_version` to print the value of that
  variable. Refer to the official documentation on how to [install terraform].
  Terraform comes as a single, statically linked binary, so the easiest method
  of installation is to download the binary and put it in a directory mentioned
  in the `PATH` environment variable.

- AWS credentials configured in `~/.aws/credentials` and/or `~/.aws/config`

- [git-secrets](#211-git-secrets)

- [jq](https://stedolan.github.io/jq/)

- The build process relies on numerous utilities that are pretty much standard 
  on any modern Unix. Things like `perl`, `sort`, `comm`, `uniq`, `sed`, `cp`, 
  `mv` and `rm`.

- For VPN support: OpenSSL (version 1.1.10 and 3.0.5 are known to work but other 
  versions should work, too). LibreSSL, which became the default on macOS at 
  some point, is an acceptible replacement. Version 2.8.3 is known to work.  

- Users of macOS 12 (Monterey) should follow additional steps outlined in 
  [Troubleshooting](#setting-up-the-azul-build-prerequisites-on-macos-12-monterey)

- Users of macOS 11 (Big Sur) should follow additional steps outlined in 
  [Troubleshooting](#installing-python-3812-on-macos-11-big-sur)

[install terraform]: https://developer.hashicorp.com/terraform/downloads
[Docker]: https://docs.docker.com/install/overview/


### 2.1.1 git-secrets

[git-secrets] helps prevent secrets (passwords, credentials, etc.) from being
committed to a Git repository. See the *Installing git-secrets* section of the
project's README for instructions how to install [git-secrets] on your OS.

Once installed, [git-secrets] will need to be configured individually in each 
one of your existing clones, be they clones of this repository or any of the 
team's other repositories. Run

```
cd /path/to/clone
git secrets --install  # install the hooks
```
    
To register the provider that adds AWS-specific secret patterns, run

```
git secrets --global --register-aws
```

Optionally, to configure [git-secrets] in all repository clones created 
subsequently, run:

```
git secrets --install ~/.git-templates/git-secrets
git config --global init.templateDir ~/.git-templates/git-secrets
```

You must now verify the proper function of [git-secrets] in each one of your 
existing clones, be they clones of this repository or any of the team's other 
repositories:

1) Run `cd /path/to/clone`

2) Make sure there is no `foo.txt` in the current directory

3) Run `(echo -e 'AWS_ACCOUNT_ID=00000000000\x30' > foo.txt && git add foo.txt && git hook run pre-commit); git rm -fq foo.txt`

**This must produce output containing `[ERROR] Matched one or more prohibited 
patterns`. If it doesn't, proper function of [git-secrets] has not been 
verified!**

If you get `git: 'hook' is not a git command. See 'git --help'.`, you are using 
an outdated version of `git`.

If you get `error: cannot find a hook named pre-commit`, [git-secrets] has not 
been configured for the clone.

If you get no output, the AWS provider has not been registered.

[git-secrets]: https://github.com/awslabs/git-secrets


## 2.2 Runtime Prerequisites (Infrastructure)

An instance of the HCA [Data Store] aka DSS. The URL of that instance can be
configured in `environment.py` or `deployments/*/environment.py`.

The remaining infrastructure is managed internally using TerraForm.


## 2.3 Project configuration

Getting started without attempting to make contributions does not require AWS
credentials. A subset of the test suite passes without configured AWS
credentials. To validate your setup, we'll be running one of those tests at the
end.

1. Load the environment defaults

   ```
   source environment
   ```

2. Activate the `dev` deployment:

   ```
   _select dev
   ```

3. Load the environment:

   ```
   source environment
   ```

   The output should indicate that the environment is being loaded from the
   selected deployment (in this case, `dev`).

4. Create a Python virtual environment and activate it:

   ```
   make virtualenv
   source .venv/bin/activate
   ```

5. Install the development prerequisites:

   ```
   make requirements
   ```

   Linux users whose distribution does not offer the required Python version
   should consider installing [pyenv] first, then Python using `pyenv install
   x.y.z` and setting `PYENV_VERSION` to `x.y.z`, where `x.y.z` is the value of
   `azul_python_version` in [environment.boot](environment.boot). You may need
   to update [pyenv] itself before it recognizes the given Python version. Even
   if a distribution provides the required minor version of Python natively,
   using [pyenv] is generally preferred because it offers every patch-level
   release of Python, supports an arbitrary number of different Python versions
   to be installed concurrently and allows for easily switching between them.

   Ubuntu users using their system's default Python installation must
   install `python3-dev` before any wheel requirements can be built.

   ```
   sudo apt install python3-dev
   ```

   [pyenv]: https://github.com/pyenv/pyenv

6. Run `make`. It should say `Looking good!` If one of the check target fails,
   address the failure and repeat. Most check targets are defined in `common.mk`.

7. Make sure Docker works without explicit root access. Run the following
   command *without `sudo`*:

   ```
   docker ps
   ```

   If that fails, you're on your own.

8. Finally, confirm that everything is configured properly on your machine by
   running the unit tests:

   ```
   make test
   ```

### 2.3.1 GitHub credentials

Integration tests require a GitHub personal access token to be configured.

1. Log into your account on https://github.com/. Click your user icon and 
navigate to *Settings* -> *Developer settings* -> *Personal access tokens*

2. Click *Generate new token*

3. Enter an appropriate description such as "Integration tests for Azul"

4. Select *No expiration*

5. Do not select any scopes

6. Click *Generate token* and copy the resulting token

7. Edit the `deployments/.active/environment.local.py` file and modify the
   `GITHUB_TOKEN` variable: 

   ```
   'GITHUB_TOKEN': '<the token you just copied>'
   ```
   
   Do not add the token to any `environment.py` files.

8. Repeat the previous step for any deployments you intend to use for running 
   the integration tests.

### 2.3.2 AWS credentials

You should have been issued AWS credentials. Typically, those credentials
require assuming a role in an account other than the one defining your IAM
user.  Just set that up normally in `~/.aws/config` and `~/.aws/credentials`.
If the  assumed role additionally requires an MFA token, you should run
`_login`  immediately after running `source environment` or switching
deployments with  `_select`.


### 2.3.3 Google Cloud credentials

When it comes to Azul and Google Cloud, we distinguish between two types of
accounts: an Azul deployment uses a *service account* to authenticate against
Google Cloud and Azul developers use their *individual Google account* in a web
browser. For the remainder of this section we'll refer to the individual Google
account simply as "your account". For developers at UCSC this is their
`…@ucsc.edu` account.

On Slack, ask for your account to be added as an owner of the Google Cloud
project that hosts—or will host—the Azul deployment you intend to work with.
For the lower HCA DCP/2 deployments (`dev`, `sandbox` and personal deployments),
this is `platform-hca-dev`. The project name is configured via the
`GOOGLE_PROJECT` variable in `environment.py` for each deployment.


### 2.3.4 Google Cloud, TDR and SAM


The Terra ecosystem is tightly integrated with Google's authentication
infrastructure, and the same two types of accounts mentioned in the previous
section are used to authenticate against SAM and [Terra Data Repository]
(TDR). Meaning that there are now at least two Google accounts at play:

1) your individual Google account ("your account"),

2) a service account for each shared or personal Azul deployment.

You use your account to interact with Google Cloud in general, along with both
production and non-production instances of Terra, SAM, and TDR, provided you
have access. You also use your account for programmatic interactions with the
above systems and the Google Cloud resources they host, like the BiqQuery
datasets and GCS buckets that TDR manages. For programmatic access to the
latter, you can either `gcloud auth login` with your account or use the
`service_account_credentials` context manager from `aws.deployment`.

[Terra Data Repository]: https://jade.datarepo-dev.broadinstitute.org/

In order for an Azul deployment to index metadata stored in a TDR instance,
the Google service account for that deployment must be registered with SAM and
authorized for repository read access to datasets and snapshots. Additionally,
in order for the deployment to accept unauthenticated servce requests, a second
Google service account called the *public* account must likewise be registered
and authorzied.

The SAM registration of the service accounts is handled automatically during
`make deploy`. To register without deploying, run `make sam`. Mere
registration with SAM only provides authentication. Authorization to access
TDR datasets and snapshots is granted by adding the registered service accounts
to dedicated SAM groups (an extension of a Google group). This must be
performed manually by someone with administrator access to that SAM group. For
non-production instances of TDR, the indexer service account needs to be added
to the group `azul-dev`.

A member of the `azul-dev` group has read access to TDR. An *administrator* of
this group can add other accounts to it, and optionally make them
administrators, too. Before any account can be added to a group, it needs to be
registered with SAM. While `make deploy` does this automatically for the
deployment's service account, for your account, you must follow the steps below:


1. Log into Google Cloud by running

    ```
    gcloud auth login
    ```

    A browser window opens to complete the authentication flow interactively.
    When being prompted, select your account.

    For more information refer to the Google authorization
    [documentation](https://cloud.google.com/sdk/docs/authorizing).

2. Register your account with SAM. Run

    ```
    (account="$(gcloud config get-value account)"
    token="$(gcloud auth --account $account print-access-token)"
    curl $AZUL_SAM_SERVICE_URL/register/user/v1  -d "" -H "Authorization: Bearer $token")
    ```

3. Ask an administrator of the `azul-dev` group to add your account to the
   group. The best way to reach an administrator is via the `#team-boardwalk`
   channel on Slack. Also, ask for a link to the group and note it in your
   records.

4. If you've already attempted to create your deployment via `make deploy`,
   visit the link, sign in as your account and add your deployment's service
   account to the group. Run `make deploy` again.

For production, use the same procedure, but substitute `azul-dev` with
`azul-prod`.


### 2.3.5 Creating a personal deployment

Creating a personal deployment of Azul allows you test changes on a live system
in complete isolation from other users. If you intend to make contributions,
this is preferred. You will need IAM user credentials to the AWS account you are
deploying to.

1. Choose a name for your personal deployment. The name should be a short handle
   that is unique within the AWS account you are deploying to. It should also be
   informative enough to let others know whose deployment this is. We'll be
   using `foo` as an example here. The handle must only consist of digits or
   lowercase alphabetic characters, must not start with a digit and must be
   between 2 and 16 characters long.

2. Create a new directory for the configuration of your personal deployment:

   ```
   cd deployments
   cp -r sandbox yourname.local
   ln -snf yourname.local .active
   mv .active/.example.environment.local.py .active/environment.local.py 
   cd ..
   ```

3. Read all comments in `deployments/.active/environment.py` and
   `deployments/.active/environment.local.py` and make the appropriate edits.


## 2.4 PyCharm

Running tests from PyCharm requires `environment` to be sourced. The easiest way
to do this automatically is by installing `envhook.py`, a helper script that
injects the environment variables from `environment` into the Python interpreter
process started from the project's virtual environment in `.venv`.

To install `envhook.py` run

```
make envhook
```

The script works by adding a `sitecustomize.py` file to your virtual
environment. If a different `sitecustomize` module is already present in your
Python path, its `sitecustomize.py` file must be renamed or removed before the
installation can proceed. The current install location can be found by importing
`sitecustomize` and inspecting the module's `__file__` attribute.

Whether you installed `envook.py` or not, a couple more steps are necessary to
configure PyCharm for Azul:

1. Under *Settings* -> *Project—Interpreter* select the virtual environment
   created above.

2. Set the `src` and `test` folders as source roots by right-clicking each
   folder name and selecting *Mark Directory as* → *Sources Root*.

3. Exclude the `.venv`, `lambdas/indexer/vendor`, and  `lambdas/service/vendor`
   folders by right-clicking each folder name and selecting *Mark Directory as*
   → *Excluded*.

Newer versions of PyCharm install another `sitecustomize` module which attempts
to wrap the user-provided one, in our case `envhook.py`. This usually works
unless `envhook.py` tries to report an error. PyCharm's `sitecustomize` swallows
the exception and, due to a bug, raises different one. The original exception
is lost, making diagnosing the problem harder. Luckily, the `sitecustomize`
module is part of a rarely used feature that can be disabled by unchecking
*Show plots in tool window* under *Settings* — *Tools* — *Python Scientific*.


# 3. Deployment


## 3.1 One-time provisioning of shared cloud resources

Most of the cloud resources used by a particular deployment (personal or main
ones alike) are provisioned automatically by `make deploy`. A handful of
resources must be created manually before invoking this Makefile target for
the first time in a particular AWS account. This only needs to be done once
per AWS account, before the first Azul deployment is created in that account.
Additional deployments do not require this step.

### 3.1.1 Versioned bucket for shared state

Create an S3 bucket for shared Terraform and Chalice state. The bucket must
not be publicly accessible since Terraform state may include secrets. If your
developers assume a role via Amazon STS, the bucket should reside in the same
region as the Azul deployment. This is because temporary STS AssumeRole
credentials are specific to a region and won't be recognized by an S3 region
that's different from the one the temporary credentials were issued in. The 
name of the bucket is not configurable but instead dictated by Azul's internal 
convention for bucket names. Use the commands below to create that bucket.

```
_select dev.shared  # or prod.shared, anvildev.shared, anvilprod.shared …
bucket="$(python -c 'from azul.deployment import aws; print(aws.shared_bucket)')"
aws s3api create-bucket --bucket "$bucket"
aws s3api put-bucket-tagging \
          --bucket "$bucket" \
          --tagging TagSet="[{Key=owner,Value=$AZUL_OWNER}]"
```

### 3.1.2 Route 53 hosted zones

Azul uses Route 53 to provide user-friendly domain names for its services. The 
DNS setup for Azul deployments has historically been varied and rather 
protracted. Azul's infrastrcture code will typically manage Route 53 records 
but the zones have to be created manually.  

Create a Route 53 hosted zone for the Azul service and indexer. Multiple
deployments can share a hosted zone, but they don't have to. The name of the
hosted zone is configured with `AZUL_DOMAIN_NAME`. `make deploy` will
automatically provision record sets in the configured zone, but it will not
create the zone itself or register the  domain name it is associated with.

Optionally, create a hosted zone for the DRS domain alias of the Azul service. 
The corresponding environment variable is `AZUL_DRS_DOMAIN_NAME`. This feature 
has not been used since 2020 when Azul stopped offering DRS for HCA.

The hosted zone(s) should be configured with tags for cost tracking. A list of
tags that should be provisioned is noted in
[src/azul/deployment.py:tags](src/azul/deployment.py).

### 3.1.3 AWS Chatbot integration with Slack

Azul deployments can make use of an AWS Chatbot instance to forward messages
from the SNS monitoring topic to a channel in a Slack workspace. Both the topic
and the Chatbot instance are shared by all deployments that are collocated in
one AWS account and that have monitoring enabled via the
`AZUL_ENABLE_MONITORING` environment variable. Most of the AWS Chatbot
integration is [managed by Terraform](#314-shared-resources-managed-by-terraform)
but the following manual steps must be performed once per AWS account containing
such deployments, before Terraform can take care of the rest. The AWS Chatbot
integration can be enabled or disabled separately for each AWS account by
setting the `azul_slack_integration` environment variable in the configuration
for the main deployment in that account. If it is disabled in an account, these
steps can be skipped in that account.

1. In the AWS Chatbot console, under *Configure a chat client*, select the
   *Slack* chat client option, then click the *Configure client* button.
   
2. Once redirected to Slack's authorization page, you may be prompted to sign
   in using your UCSC account, in order to provide permission for Chatbot to
   access the Slack workspace. When this step is completed, you should see the
   workspace name and ID listed in the console.

3. Use the ID displayed in the console to set the `workspace_id` attribute of
   the `azul_slack_integration` variable in the main deployment's environment
   file for that account.

4. Set the `channel_id` attribute to the ID of the appropriate channel. Get the
   channel ID by right-clicking the channel in Slack and selecting *View channel
   details*. The ID is listed at the bottom of the *About* tab.

### 3.1.4 Shared resources managed by Terraform

The remaining resources for each of the AWS accounts hosting Azul deployments
are provisioned through Terraform. The corresponding resource definitions reside
in a separate *Terraform component*.

A Terraform component is a set of related resources. It is our own bastardized
form of Terraform's *module* concept, aimed at facilitating encapsulation and
reuse. Each deployment has at least a main component and zero or more child
components. The main component is identified by the empty string for a name;
child components have a non-empty name. The `dev` component has a child
component `dev.shared`. To deploy the main component of the `dev` deployment, 
one selects the `dev` deployment and runs `make apply` from 
`${project_root}/terraform` (or `make deploy` from the project root). To deploy 
the `shared` child component of the `dev` deployment, one selects `dev.shared` 
and runs `make apply` from `${project_root}/terraform/shared`. In other words, 
there is one generic set of resource definitions for a child component, but 
multiple concrete deployment directories.

There are currently two Terraform components: `shared` and `gitlab`. 
Interestingly, not every deployment uses these components. Typically, only the 
`dev` and `prod` deployments use them. The other deployment share them with 
`dev` or `prod`, depending on which of those deployments they are colocated 
with. Two deployments are colocated if they use the same AWS account. The 
`shared` component contains the resources shared by all deployments in an AWS 
account.

To deploy the remaining shared resources, run: 

```
_select dev.shared  # or prod.shared, anvildev.shared, anvilprod.shared …
cd terraform/shared
make validate
bucket="$(python -c 'from azul.deployment import aws; print(aws.shared_bucket)')"
terraform import aws_s3_bucket.shared "$bucket"
make
```

The invocation of `terraform import` puts the bucket we created 
[earlier](#311-versioned-bucket-for-shared-state) under management by Terraform.

### 3.1.5 GitLab

A self-hosted GitLab instance is provided by the `gitlab` TerraForm component. 
It provides the necessary CI/CD infrastructure for one or more Azul deployments 
and protects access to that infrastructure through a VPN. That same VPN is also
used to access to Azul deployments with private APIs (see AZUL_PRIVATE_API in 
[environment.py]). Like the `shared` component, the `gitlab` component belongs 
to one main deployment in an AWS account (typically `dev` or `prod`) and is 
shared by the other deployments colocated with that deployment. Unlike the 
`shared` component, the `gitlab` component is optional.    

[environment.py]: /environment.py

The following resources must be created manually before deploying the `gitlab` 
component:

- An EBS volume needs to be created. See [gitlab.tf.json.template.py] and the
  [section on CI/CD](#95-storage) for details.

- A certificate authority must be set up for VPN access. For details refer to
  [section on GitLab CA](#912-setting-up-the-certificate-authority).


## 3.2 One-time manual configuration of deployments

In order for users to authenticate using OAuth 2.0, an OAuth 2.0 consent screen
must be configured once per Google project, and an OAuth 2.0 client ID must
be created for each deployment.

### 3.2.1 Google OAuth 2.0 consent screen

These steps are performed once per Google project.

1. Log into the Google Cloud console and select the desired project, e.g. `dev` 
   or `prod`

2. Navigate to *APIs & Services* -> *OAuth Consent Screen*

3. Click *CONFIGURE CONSENT SCREEN*

4. For *User Type*, select *External*

5. Click *CREATE*

6. For *App name*, enter `Azul {stage}`, where `{stage}` is the last component
   of the Google project name, e.g. `dev` or `prod`

7. Provide appropriate email addresses for *App information* -> 
   *User support email* and *Developer contact information* -> 
   *Email addresses*, e.g. `azul-group@ucsc.edu`

8. Click *SAVE AND CONTINUE*

9. For scopes, select:
   ```
   https://www.googleapis.com/auth/userinfo.email
   https://www.googleapis.com/auth/userinfo.profile
   openid
   ```

10. Click *SAVE AND CONTINUE* twice

11. Click *PUBLISH APP* and *CONFIRM*

### 3.2.2 Google Oauth 2.0 Client ID

These steps are performed once per deployment (multiple times per project).

1. Log into the Google Cloud console and select the desired project, e.g. `dev`
   or `prod`
   
2. Navigate to *APIs & Services* -> *Credentials*; click *+ CREATE CREDENTIALS*
   -> *OAuth Client ID*

3. For *Application Type*, select *Web application*

4. For *Name*, enter `azul-{stage}` where stage is the name of the deployment

5. Add an entry to *Authorized JavaScript origins* and enter the output from
   `python3 -c 'from azul import config; print(config.service_endpoint)'`

6. Add an entry to *Authorized redirect URIs*. Append `/oauth2_redirect` to the
    value of the previous field and enter the resulting value.
   
7. Click *Create*

8. Copy the OAuth Client ID (_not_ the client secret) and insert it into the
    deployment's `environment.py` file:

    ```
    'AZUL_GOOGLE_OAUTH2_CLIENT_ID': 'the-client-id'
    ```

9. `_refresh`

### 3.2.3 Transition Amazon SES resource out of sandbox

Perform these steps once the cloud infrastructure has been provisioned for the
shared deployment, section #3.3. Before continuing, make sure that the SES
identity provisioned by Terraform is listed in the Verified identities tab,
from the AWS SES console. The identity listed should be the deployments' domain
name.

Run the following AWS CLI command to request for the AWS SES Identity to be
granted production access::

    aws sesv2 put-account-details \
    --contact-language EN \
    --mail-type TRANSACTIONAL \
    --production-access-enabled \
    --website-url $(python -c 'from azul import config; print(api_lambda_domain("notify"))') \
    --use-case-description \
        'Use a lambda function invoked by an SNS topic to forward the SNS notification via email to a single recipient.'

## 3.3 Provisioning cloud infrastructure

Once you've configured the project and your personal deployment or a shared
deployment you intend to create, and once you manually provisioned
the shared cloud resources, it is time to provision the cloud infrastructure
for your deployment. Run

```
make deploy
```

to prepare the Lambda functions defined in the `lambdas` directory for
deployment via Terraform. It will display a plan and ask you to confirm it.
Please consult the Terraform documentation for details.

Any time you wish to change the code running in the lambdas you will need to
run `make deploy`.

Some Terraform configuration is generated by `make -C lambdas`, but the rest is
defined in `….tf.json` files which in turn are generated from
`….tf.json.template.py` templates which are simple Python scripts containing the
desired JSON as Python dictionary and list literals and comprehensions.
Running `make deploy` will run `make -C lambda` and also expand the
template files. Changes to either the templates or anything in the `lambdas`
directory requires running `make deploy` again in order to update cloud
infrastructure for the selected deployment.


## 3.4 Creating the Elasticsearch indices

While `make deploy` takes care of creating the Elasticsearch domain, the actual
Elasticsearch indices for the selected deployment must be created by running

```
make create
```

In a newly created deployment, the indices will be empty and requests to the
deployment's service REST API may return errors. To fill the indices,
initiate a [reindexing](#37-reindexing). In an existing deployment
`make create` only creates indices that maybe missing. To force the recreation
of indices run `make delete create`.

## 3.5 Locating REST API endpoints via DNS

The HTTP endpoint offered by API Gateway have somewhat cryptic and hard to
remember domain names:

```
https://klm8yi31z7.execute-api.us-east-1.amazonaws.com/hannes/
```

Furthermore, the API ID at the beginning of the above URL is likely to change
any time the REST API is re-provisioned.  To provide stable and user-friendly
URLs for the API lambdas, we provision a *custom domain name* object in API
Gateway along with an ACM certificate and a CNAME record in Route 53. the
user-friendly domain names depend on project configuration. The default for HCA
is currently

```
http://indexer.${AZUL_DEPLOYMENT_STAGE}.singlecell.gi.ucsc.edu/
http://service.${AZUL_DEPLOYMENT_STAGE}.singlecell.gi.ucsc.edu/
```

Personal deployments are subdomains of the domain for the `dev` deployment:

```
http://indexer.${AZUL_DEPLOYMENT_STAGE}.dev.singlecell.gi.ucsc.edu/
http://service.${AZUL_DEPLOYMENT_STAGE}.dev.singlecell.gi.ucsc.edu/
```

## 3.6 Private API

Follow these steps to put a deployment's API Gateway in the GitLab VPC so that a
VPN connection is required to access the deployment. See [9.1 VPN access to
GitLab](#91-vpn-access-to-gitlab) for details. Read this entire section before
following these steps.

1. Destroy the current deployment (`make -C terraform destroy`).

2. Increment `AZUL_DEPLOYMENT_INCARNATION`.
 
3. Set `AZUL_PRIVATE_API` to `1`.
 
4. Redeploy (`make deploy`).

Going in the opposite direction i.e., attempting to change `AZUL_PRIVATE_API`
from `1` to `0` will result in `Cannot update endpoint from PRIVATE to EDGE`
during `make deploy`. The error message will be shown for every REST API
separately. It should be sufficient to simply `terraform taint` the REST API
resources mentioned in the error messages and then to run `make deploy` again.
It is possible that this also works when changing `AZUL_PRIVATE_API` from `0` to
`1`. Try that first, before destroying the entire deployment.

### Troubleshooting

Transient errors might be encountered during the deploy such as `SQS Error Code:
AWS.SimpleQueueService.NonExistentQueue. SQS Error Message: The specified queue
does not exist for this wsdl version` In such cases rerunning `make deploy`
should resolve the issue.

[aws_cloudwatch_log_group]: https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/cloudwatch_log_group

If the error `ResourceAlreadyExistsException: The specified log group already
exists` is encountered, follow the steps below to import the
[aws_cloudwatch_log_group] resources into terraform and retry the deploy.

1. `cd terraform`

2. `terraform import aws_cloudwatch_log_group.indexer /aws/apigateway/azul-indexer-foo`
 
3. `terraform import aws_cloudwatch_log_group.service /aws/apigateway/azul-service-foo`
 
4. `cd ..`
 
5. `make deploy`

If the error `azul.RequirementError: The service account (SA) '...' is not
authorized to access ... or that resource does not exist. Make sure that it 
exists, that the SA is registered with SAM and has been granted read access to 
the resource` is encountered, ask an administrator of the Terra group `azul-dev` 
to add the service account as specified in the error messaged to that group. See
[2.3.4 Google Cloud, TDR, and SAM](#234-google-cloud-tdr-and-sam) for details.

[KMSAccessDeniedException]: https://aws.amazon.com/premiumsupport/knowledge-center/lambda-kmsaccessdeniedexception-errors/

After a successful invocation of `make deploy`, if the deployment is unresponsive
and CloudWatch shows logs entries in the `/aws/apigateway/…` log group but not in
`/aws/lambda/…`, first confirm whether the issue is the known
[KMSAccessDeniedException] error. In the AWS Console, go to the Lambda function
details page, click on the `Test` tab, and click on the `Test` buttton. 

Note that it is normal for some Lambda functions to fail the test due to the
parameters of the test event. Examine the error message to determine if the
failure is due to a `KMSAccessDeniedException` which would be explicitly
specified.

To resolve a `KMSAccessDeniedException` run the `reset_lambda_role.py` script to
reset all the Lambda functions in the selected deployment.

## 3.7 Reindexing

The DSS instance used by a deployment is likely to contain existing bundles. To
index them run:

```
make reindex
```

When reindexing, artificial notifications are generated by Azul.

The `reindex` make target will purchase a BigQuery slot commitment if:

1. No slot commitment is currently active, and
2. At least one catalog being indexed uses the TDR repository plugin.

To avoid cost-ineffective slot purchases, the `reindex_no_slots` target should be
used instead of `reindex` if the reindexing is expected to complete in 15
minutes or less.

## 3.8 Cancelling an ongoing (re)indexing operation

```
python scripts/manage_queues.py purge_all
```

After that it is advisable to delete the indices and reindex at some later time.

## 3.9 Deleting all indices

To delete all Elasticsearch indices run

```
make delete
```

The indices can be created again using

```
make create
```

but they will be empty.

## 3.10 Deleting a deployment


1. `cd` to the project root, then

   ```
   source environment
   ```

2. Select the deployment to deleted

   ```
   _select foo.local
   ```

3. Delete all Elasticsearch indices in the selected deployment

   ```
   make delete
   ```

4. Delete the API Gateway base path mappings

   ```
   cd terraform
   make init
   terraform destroy $(terraform state list | grep aws_api_gateway_base_path_mapping | sed 's/^/-target /')
   cd ..
   ```

5. Destroy cloud infrastructure

   ```
   make -C terraform destroy
   ```

   The destruction of `aws_acm_certificate` resources may time out. Simply
   repeat this step until it succeeds.

6. From the shared bucket (run `python -c 'from azul.deployment import aws; 
   print(aws.shared_bucket)'` to reveal its name), delete all keys relating to 
   your deployment.

7. Delete the local Terraform state file at
   `deployments/.active/.terraform.{$AWS_PROFILE}/terraform.tfstate`.


# 4. Running indexer or service locally

While this method *does* run the service or indexer locally on your machine, it
still requires that the cloud resources used by them are already deployed.
See sections [2](#2-getting-started) and [3](#3-deployment) on how to do that.

1. As usual, activate the virtual environment and `source environment` if you
   haven't done so already

2. `cd lambdas/service`

3. Run

   ```
   make local
   ```

4. You can now hit the app under `http://127.0.0.1:8000/`

PyCharm recently added a feature that allows you to attach a debugger: From the
main menu choose *Run*, *Attach to local process* and select the `chalice`
process.


# 5. Troubleshooting


## `Error: Invalid index` during `make deploy`

```
aws_route53_record.service_0: Refreshing state... [id=XXXXXXXXXXXXX_service.dev.singlecell.gi.ucsc.edu_A]
 Error: Invalid index
   on modules.tf.json line 8, in module.chalice_indexer.es_endpoint:
    8:                 "${aws_elasticsearch_domain.index.endpoint}",
     |----------------
     | aws_elasticsearch_domain.index is empty tuple
 The given key does not identify an element in this collection value.
```

This may be an [issue](https://github.com/hashicorp/terraform/issues/25784) with
Terraform. To work around this, run …

```
terraform state rm aws_elasticsearch_domain.index
```

… to update the Terraform state so that it reflects the deletion of the
Elasticsearch domain. Now running `make deploy` should succeed.


## `NoCredentialProviders` while running `make deploy`

If you get …

```
Failed to save state: failed to upload state: NoCredentialProviders: no valid providers in chain.
…
The error shown above has prevented Terraform from writing the updated state
to the configured backend. To allow for recovery, the state has been written
to the file "errored.tfstate" in the current working directory.

Running "terraform apply" again at this point will create a forked state,
making it harder to recover.
```

… during `make deploy`, your temporary STS credentials might have expired while
`terraform apply` was running. To fix, run …

```
_login
(cd terraform && terraform state push errored.tfstate)
```

… to refresh the credentials and upload the most recent Terraform state to the
configuration bucket.


## `AccessDeniedException` in indexer lambda

If you get the following exception:
```
An error occurred (AccessDeniedException) when calling the GetParameter operation: User: arn:aws:sts::{account_id}:assumed-role/azul-indexer-{deployment_stage}/azul-indexer-{deployment_stage}-index is not authorized to perform: ssm:GetParameter on resource: arn:aws:ssm:{aws_region}:{account_id}:parameter/dcp/dss/{deployment_stage}/environment: ClientError
Traceback (most recent call last):
    ...
botocore.exceptions.ClientError: An error occurred (AccessDeniedException) when calling the GetParameter operation: User: arn:aws:sts::{account_id}:assumed-role/azul-indexer-{deployment_stage}/azul-indexer-{deployment_stage}-index is not authorized to perform: ssm:GetParameter on resource: arn:aws:ssm:{aws_region}:{account_id}:parameter/dcp/dss/integration/environment
```

Check whether the DSS switched buckets. If so, the lambda policy may need to be
updated to reflect that change. To fix this, redeploy the lambdas (`make
package`) in the affected deployment.


## `make requirements_update` does not update transitive requirements

In some cases, `make requirements_update` might not produce any updates to
transitive requirements, even if you expect them. For example, a sandbox build
on Gitlab might identify updated transitive requirements even though doing `make
requirements_update` locally doesn't.

This is a side effect of the Docker build cache on two different machines
diverging to reflect different states on PyPI. This can be fixed by incrementing
`azul_image_version` in the Dockerfile.


##  Unable to re-register service account with SAM

If you have destroyed your deployment and are rebuilding it, it's possible that
SAM will not allow the Google service account to be registered again because
the service account's email is the same in the current and previous incarnation
of the deployment, while the service account's `uniqueID` is different. SAM
does not support this.

A warning message stating that `SAM does not allow re-registration of service
account emails` will be visible during the `make sam` step of the deployment
process. To get around this, increment the current value of
`AZUL_DEPLOYMENT_INCARNATION` in the deployment's `environment.py` file, then
redeploy.


## Unexpected warnings cause tests to fail in `tearDownClass`

Unexpected warnings that occur during testing will cause failures in 
`AzulTestCase.tearDownClass`. There is a context manager in `AzulTestCase` that
keeps record of emitted warnings during test execution. Due to the unit test 
discovery process loading modules as it traverses directories, it’s possible 
that a warning is emitted outside the scope of the context manager.

In the two commands below, the unit test discovery process occurs within a 
different directory.

```
$ (cd test && python -m unittest service.test_app_logging.TestServiceAppLogging)
```

In the first case, it's possible that an unpermitted warning is emitted outside 
the `AzulTestCase` context manager, due to modules being loaded recursively from
the directory `test/`. If a warning is emitted outside the context manager no 
test failure will occur.

```
$ (cd test/service && python -m unittest test_app_logging.TestServiceAppLogging)
```

In the second case, the test discovery process loads fewer modules due to the  
narrowed working directory. This may emit a warning during test execution, 
enabling the context manager to catch the unpermitted warning, and fail 
appropriately.

Similarly, when running tests in PyCharm, its own proprietary test discovery 
process may also increase the chance of the `AzulTestCase` context manager
causing a failure.

If these failures occur, add the warning to the list of permitted warnings
found in [`AzulTestCase`](test/azul_test_case.py) and commit the modifications. 


## Setting up the Azul build prerequisites on macOS 12 (Monterey)

The steps below are examplary for Python 3.11.5. Replace `3.5.11` with the 
version listed in [environment.boot](environment.boot).   

Make `bash` the default shell. Google it.

Install Homebrew. Google it. 

Install pyenv:

```
brew install zlib pyenv
```

Install python

```
pyenv install 3.11.5
```

Set `PYENV_VERSION` to `3.11.5` in `environment.local.py` at the project root.
Do not set `SYSTEM_VERSION_COMPAT`. For a more maintainable configuration use 
`os.environ['azul_python_version']` as the value and `import os` at the top.

Install Docker Desktop. Google it.

Install Terraform by downloading and unziping the binary to a directory on the 
`PATH`. Be sure to download the file for the architecture of your Mac. For Apple 
Silicon the file name contains `arm64`, for older Intel Macs it's `amd64`.


## Installing Python 3.8.12 on macOS 11 (Big Sur)

[pyenv macOS 11 GitHub issue](https://github.com/pyenv/pyenv/issues/1740)

Users of macOS 11 or later may encounter a `build failed` error when installing
Python through pyenv. A patch was made available to remedy this:

First, ensure that bzip2 and any other requirements for the Python build
environment are met. See [pyenv wiki] for details:

[pyenv wiki]:https://github.com/pyenv/pyenv/wiki#suggested-build-environment

```
brew install openssl readline sqlite3 xz zlib bzip2
```

Follow any additional steps that `brew` prompts for at the end of the
installation. These should include modifying path variables `LDFLAGS` and
`CPPFLAGS`. The commands from the `brew` output to modify the aforementioned
path variables can be placed in `~/.bash_profile` to make the change persistent.

Then install Python 3.8.12 using `pyenv` by running:

```
pyenv install 3.8.12
```

Users of macOS 11 or later may encounter `pip` installation errors due to `pip`
not being able to locate the appropriate wheels. The information below will
help remedy this:

[Resolution source](https://stackoverflow.com/a/63972598)

[macOS 11 Release Notes](https://developer.apple.com/documentation/macos-release-notes/macos-big-sur-11_0_1-release-notes#Third-Party-Apps)

`pip` will not be able to locate the appropriate wheels due to the major release
version of macOS being incremented from `10.x` to `11.x`, instead pip will
attempt to compile wheels manually for wheels that it cannot locate.

In order to be able to run `make requirements` successfully, a backwards
compatibility flag needs to be added to the `environment.local.py` file in the
project root. The flag is `SYSTEM_VERSION_COMPAT=1` and it needs to be inserted
into the file (starting from line 25) as a key/value pair:
`'SYSTEM_VERSION_COMPAT': 1`.


# 6. Branch flow & development process

**This section should be considered a draft. It describes a future extension to the current branching flow.**

The section below describes the flow we want to get to eventually, not the one
we are currently using while this repository recovers from the aftermath of its
inception.

The declared goal here is a process that prevents diverging forks yet allows
each project to operate independently as far as release schedule, deployment
cadence, project management and issue tracking is concerned. The main challenges
are 1) preventing contention on a single `develop` or `master` branch, 2)
isolating project-specific changes from generic ones, 3) maintaining a
reasonably linear and clean history and 4) ensuring code reuse.

The [original repository](https://github.com/DataBiosphere/azul), also known as
*upstream*, should only contain generic functionality and infrastructure code.
Project-specific functionality should be maintained in separate project-specific
forks of that repository. The upstream repository will only contain a `master`
branch and the occasional PR branch.

Azul dynamically imports project-specific plugin modules from a special location
in the Python package hierarchy: `azul.projects`. The package structure in
upstream is

```
root
├── ...
├── src
│   └── azul
│       ├── index
│       │   └── ...
│       ├── projects (empty)
│       ├── service
│       │   └── ...
│       └── util
│       │   └── ...
└── ...
```

Note that the `projects` directory is empty.

The directory structure in forked repositories is generally the same with one
important difference. While a fork's `master` branch is an approximate mirror of
upstream's `master` and therefore also lacks content in `projects`, that
directory *does* contain modules in the fork's `develop` branch. In
`HumanCellAtlas/azul-hca`, the fork of Azul for the HumanCellAtlas project, the
`develop` branch would look like this:


```
root
├── ...
├── src
│   └── azul
│       ├── index
│       │   └── ...
│       ├── projects
│       │   └── hca
│       │       └── ...
│       ├── service
│       │   └── ...
│       └── util
│       │   └── ...
└── ...
```

The `develop` branch would only contain changes to the `azul.projects.hca`
package. All other changes would have to be considered generic—they would occur
on the fork's `master` branch and eventually be merged into upstream's `master`
branch. The `master` branches in each fork should not be divergent for sustained
periods of time while the project-specific branches can and will be.

The reason why each fork maintains a copy of the `master` branch is that forks
generally need to have a place to test and evaluate generic features before they
are promoted upstream. If there wasn't a `master` branch in a fork, the
project-specific `develop` branch in that fork would inevitably conflate
project-specific changes with generic ones. It would be very hard to selectively
promote generic changes upstream, even if the generic changes were separate
commits.

The flow presented here establishes an easy-to-follow rule: If you're modifying
`azul.projects.hca`, you need to do so in a PR against `develop`. If you're
modifying anything else, you need to do so in a PR against `master`. The figure
below illustrates that.

```
                                                      ●────● feature/generic-foo
                                                     ╱
                                              4     ╱
    ─────●────────────────────────────────────●────●──────────────        master
          ╲                                  ╱
 azul      ╲                                ╱
 ─ ─ ─ ─ ─ ─╲─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ╱ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
 azul-hca    ╲                            ╱
              ╲                          ╱
    ──────●────●────●────●────●────●────●──────────────────────────       master
           ╲   1     ╲    ╲   A'   B'
            ╲         ╲    ╲
             ╲         ╲    ●────● feature/master/generic-stuff
              ╲         ╲   A    B
               ╲         ╲
                ●─────────●─────────────●────●────●─────────────────     develop
                2         3              ╲   C'   D'
                                          ╲
                                           ●────● feature/develop/specific-stuff
                                                C    D
```

Merge commit 1 from the upstream `master` branch integrates upstream changes
into the fork. These may be generic changes merged upstream from other forks or
changes that were directly PR-ed against `master` in upstream. Commit 2 marks
the beginning of the `develop` branch, adding the `azul.projects.hca` package.
Merge commit 3 brings the changes from commit 1 into the `develop` branch.

Another important rule is that collaborative branches like `develop` and
`master` are never rebased. Changes are exchanged between them using merge
commits instead. Individual branches however, like feature branches, are always
rebased onto the base branch. In the above example,
`feature/master/generic-stuff` is first rebased onto `master`, creating commits
A' and B'. Later those changes are merged upstream via commit 4. Both the rebase
and the merge happen via a pull request, but the landing action will be "Rebase
and merge" for the first PR and "Create a merge commit" for the second.

The reason for this distinction is that rebasing usually triggers more rebasing
of branches that were based on the rebased branch. It also rewrites the commit
timestamps, thereby obfuscating the history to some extent. For these two
reasons, rebasing is not a sustainable practice for collaborative branches. For
individual branches however, rebasing is possible because feature branches are
typically not used as a base for other branches. Rebasing is also desirable
because it produces a cleaner, linear history and we should use it whenever
possible. The back and forth merging between collaborative branches produces a
history that's somewhat convoluted so it is important to keep the history as
clean as possible in between merges.

Generic changes don't have to be conceived in a fork. We can also PR them
directly against the upstream repository as illustrated by branch
`feature/generic-foo`.

The most common type of pull request in a fork is one against that fork's
`develop` branch, `feature/develop/specific-stuff` for example. Note that
changes occurring on `develop` are never merged upstream.

As mentioned before, merge commit 4 is done via a pull request against the
upstream repository. It is possible and perfectly acceptable that such upstream
PRs combine multiple unrelated changes. They should be requested by the team
lead for the forking project and reviewed by an upstream lead. Shortly after the
PR lands, the requesting lead should perform a fast-forward merge of the
upstream `master` branch into the fork's `master` branch. This will propagate
the merge commit downstream before any subsequent commits occurring on fork's
`master` have a chance to complicate the history by introducing the infamous
merge of merge commits.

```
$ git branch
* master
  develop
$ git merge --ff-only upstream/master
Updating 450b0c0..212003c
Fast-forward
```

This procedure requires that the lead's local clone of the fork be set up with
two remotes: `origin` (the forked repository) and `upstream` (the upstream
repository). Other team members can usually get by with just one remote,
`origin`.


## 6.1 Deployment branches

The code in the upstream repository should never be deployed anywhere because it
does not contain any concrete modules to be loaded at runtime. The code in a
fork, however, is typically active in a number of deployments. The specifics
should be left to each project but the rule of thumb should be that each
deployment corresponds to a separate branch in the fork. The `azul-hca` fork has
four deployments: development, integration, staging and production. The
development deployment, or `dev`, is done from the `develop` branch. Whenever a
commit is pushed to that branch, a continuous deployment script deploys the code
to AWS. The other deployment branches are named accordingly. Changes are
promoted between deployments via a merge. The merge is likely going to be a
fast-forward. A push to any of the deployment branches will trigger a CI/CD
build that performs the deployment. The promotion could be automatic and/or
gated on a condition, like tests passing.


# 7. Operational Procedures


## 7.1 Main deployments and promotions

We will refer to the branch of the stage to which you are deploying as the
**`TARGET`** branch. The branch of the stage just below will be referred to as
the **`SOURCE`** branch.

This cheat sheet may differ from branch to branch. Be sure to follow the cheat
sheet in the README on the branch currently checked out.


### 7.1.1 Initial setup

[Gitlab instance]: https://gitlab.dev.singlecell.gi.ucsc.edu/

_Note: You can skip this step if you've deployed or promoted with Gitlab at
least once already._

[SSH keys]: https://gitlab.dev.singlecell.gi.ucsc.edu/profile/keys

1. For promotion, we recommend keeping a separate clone of Azul that is never in
   a dirty state. To create this if it doesn't yet exist run

   ```
   git clone git@github.com:DataBiosphere/azul.git azul.stable
   ```

   Then follow the setup instructions in [2.3 Project configuration](#23-project-configuration).

2. Next you will need to login to our [Gitlab instance] in order to be able to
   push to Gitlab which automatically takes care of most of the deployment
   process. If you haven't signed on yet, sign on with Github. You will need at
   least `developer` permissions in order to be able to `push` to Gitlab.
   Contact the team lead if you have problems signing on or have insufficient
   permissions.

3. Deposit you public SSH key into the [SSH keys] section of your profile so
   that you can push to Git repositories hosted on that Gitlab instance.

4. Now that your SSH key is set up, you will need to add Gitlab as a remote. Run

   ```
   git remote add gitlab.dev git@ssh.gitlab.dev.singlecell.gi.ucsc.edu:ucsc/azul.git
   ```

   Run

   ```
   git fetch gitlab.dev
   ```

   to ensure that your connection is working.

If you have been given write access to our production Gitlab instance, you need
to repeat these steps for that instance as well. For the name of the `git`
remote use `gitlab.prod` instead of `gitlab.dev` in step 4 above. The hostname
of that instance is the same as that of the Gitlab instance for the lesser
deployments, without `.dev`.

Note that access to the production instance of Gitlab does not necessarily
imply access to production AWS account which that Gitlab instance deploys to.
So while you may be able to run certain `make` targets like `make reindex` or
`make deploy` against the development AWS account (with `dev`, `integration`
or `staging` selected), you may not be able to do the same for the production
AWS account (with `prod` selected).


### 7.1.2 Prepare for promotion

_NOTE: Skip these steps if you are deploying without promoting changes._

[DCP release SOP]: https://allspark.dev.data.humancellatlas.org/dcp-ops/docs/wikis/SOP:%20Releasing%20new%20Versions%20of%20DCP%20Software

_NOTE: If promoting to `staging` or `prod` you will need to do these steps **at least
24 hours in advance** so that the release notes are ready in time._

1. From the `azul.stable` clone make sure all of the relevant branches are up to
   date

   ```
   cd azul.stable
   git checkout SOURCE
   git pull
   git checkout TARGET
   git pull
   ```

2. You should be on the `TARGET` branch. Run

   ```
   git merge --no-ff SOURCE
   ```

   and resolve conflicts in necessary. Conflict resolution should only be
   necessary if cherry-picks occurred on the target branch.

3. The merge may have affected `README.md`, the file you are looking at right
   now. Reopen the file now to ensure you are following the updated version.

4. Now you need to create the release notes. (Skip this step if no link to the release
   notes document can be found either in the #dcp-ops channel on HCA Slack or in
   the Google Drive folder mentioned in the [DCP release SOP].

   To produce the list of changes for the DCP release notes, first find the
   previous release tag for the TARGET branch. Then run:

   ```
   git log LAST_RELEASE_TAG..HEAD --format="%C(auto) %h %s" --graph
   ```
   Edit this output so that the commits within merged branches are removed, along with
   merge commits between deployments. For example
   ```
   *  C  <-- merge commit
   |\
   | *  B
   |/
   *  A
   *  Merge branch 'develop' into integration
   ```
   should be changed to look like
   ```
   *  C  <-- merge commit
   *  A
   ```

   For the version, use the full hash of the latest commit:

   ```
   git log -1 --format="%H"
   ```

5. At this point you should determine whether or not you will need to reindex.
   The `CHANGELOG.yml` _should_ contain this information but is notoriously
   unreliable. Try running

   ```
   git diff LAST_RELEASE_TAG..HEAD src/azul/project/ src/azul/indexer.py src/azul/plugin.py src/azul/transformer.py
   ```

   where `LAST_RELEASE_TAG` is the previous release of the target branch. If the diff
   contains non-trivial changes reindexing is probably necessary. When in doubt
   assume yes.


### 7.1.3 Finishing up deployment / promotion

If promoting to staging or production this part of the process must be
coordinated on the
[#dcp-ops](https://humancellatlas.slack.com/messages/G9XD6L0AD) Slack channel.
While any component can technically promote to integration at any time, you
should consider that promoting to integration while the DCP-wide test is red
for that deployment could interfere with other teams' efforts to fix the test.
If in doubt ask on #dcp-ops.

None of these steps can be performed ahead of time. Only perform them once you
are ready to actually deploy.

1. Activate your virtual environment and run
   ```
   source environment
   ```
   and then select the target deployment stage with
   ```
   _select STAGE
   ```
   where stage is one of `dev`, `integration`, `staging`, or `prod`

2. Now you need to push the current branch to Github. This is needed because
   the Gitlab build performs a status check update on Github. This would fail
   if Github didn't know the commit.

   ```
   git push origin
   ```

3. Finally, push to Gitlab.

   ```
   git push gitlab.dev   # for a dev, integration or staging deployment
   git push gitlab.prod  # for a prod deployment
   ```

   The build should start immediately. You can monitor its progress from the
   [Gitlab Pipelines page](https://gitlab.gitlab.dev.singlecell.gi.ucsc.edu/ucsc/azul/pipelines).

   If reindexing and promoting to staging or production, send a second
   warning about reindexing to the #data-wrangling channel at this point.

   Wait until the pipeline on Gitlab succeeds or fails. If the build fails before
   the `deploy` stage, no permanent changes were made to the deployment but you
   need to investigate the failure. If the pipeline fails at or after the `deploy`
   stage, you need triage the failure. If it can't be resolved manually, you need
   to reset the branch back to the LAST_RELEASE_TAG and repeat step 2 in this section.

4. Invoke the health and version endpoints.

   * For the `develop` branch and the corresponding `dev` deployment use

     ```
     http https://indexer.dev.singlecell.gi.ucsc.edu/version
     http https://service.dev.singlecell.gi.ucsc.edu/version
     http https://indexer.dev.singlecell.gi.ucsc.edu/health
     http https://service.dev.singlecell.gi.ucsc.edu/health
     ```

   * For the `integration` branch/deployment use

     ```
     http https://indexer.integration.singlecell.gi.ucsc.edu/version
     http https://service.integration.singlecell.gi.ucsc.edu/version
     http https://indexer.integration.singlecell.gi.ucsc.edu/health
     http https://service.integration.singlecell.gi.ucsc.edu/health
     ```

   * For the `staging` branch/deployment use

     ```
     http https://indexer.staging.singlecell.gi.ucsc.edu/version
     http https://service.staging.singlecell.gi.ucsc.edu/version
     http https://indexer.staging.singlecell.gi.ucsc.edu/health
     http https://service.staging.singlecell.gi.ucsc.edu/health
     ```

   * For the `prod` branch/deployment use

     ```
     http https://indexer.singlecell.gi.ucsc.edu/version
     http https://service.singlecell.gi.ucsc.edu/version
     http https://indexer.singlecell.gi.ucsc.edu/health
     http https://service.singlecell.gi.ucsc.edu/health
     ```

5. Assuming everything is successful, run

   ```
   make tag
   ```

   and the

   ```
   git push ...
   ```

   invocation that it echoes.

6. In Zenhub, move all tickets from the pipeline representing the source
   deployment of the promotion to the pipeline representing the target
   deployment.

7. In the case that you need to reindex run the manual `reindex` job on the
   Gitlab pipeline representing the most recent build on the current branch.


## 7.2 Big red button

In the event of an emergency, Azul can be shut down immediately using the
`enable_lambdas.py` script. Before using this script, make sure that the desired
deployment is selected and your Python virtual environment is activated.

Shut down Azul by running

```
python scripts/enable_lambdas.py --disable
```

Once your issue has been resolved, you can resume Azul's services by running

```
python scripts/enable_lambdas.py --enable
```


## 7.3 Copying bundles

In order to copy bundles from one DSS instance to another, you can use
`scripts/copy_bundles.py`. The script copies specific bundles or all bundles
listed in a given manifest. It iterates over all source bundles, and all files
in each source bundle. It copies the files by determining the native URL
(`s3://…` ) of the DSS blob object for each file and passing that native URL to
the destination DSS' `PUT /files` endpoint as the source URL parameter for that
request. This means that it is actually the destination DSS that physically
copies the files. Once all files in a bundle were copied, the script requests
the `PUT /bundles` endpoint to create a copy of the source bundle.

The script is idempotent, meaning you can run it repeatedly without harm,
mostly thanks to the fact that the DSS' `PUT /files` and `PUT /bundles`
endpoints are idempotent. If a script invocation resulted in a transient error,
running the script again will retry all DSS requests, both successful requests
and requests that failed in the previous invocation.

In order to determine the native URL of the source blob, the script needs
direct read access to the source DSS bucket. This is because blobs are an
implementation detail of the DSS and obtaining their native URL is not
supported by the DSS.

Furthermore, The destination DSS requires the source object to carry tags
containing the four checksums of the blob. Some blobs in some DSS instances
have those tags, some don't. It is unclear the tags are supposed to be present
on all blob objects or if their presence is incidental. To work around this,
the script can optionally create those tags when the destination DSS complains
that they are missing. To enable the creation of checksum tags on source blob
objects, use the `---fix-tags` option. Please be aware that `--fix-tags`
entails modifying object tags in the source (!) bucket.

The destination DSS instance requires read access to the blobs in the source
DSS bucket. The `integration` and `staging` instances can read each other's
buckets so copies can be made between those two instances. To copy bundles from
a DSS instance that is in a different AWS account compared to the destination
instance, from prod to integration, for example, you will likely need to modify
the source DSS bucket's bucket policy.

You should never copy **to** the HCA `prod` instance of the DSS.

Here is a complete example for copying bundles from `prod` to `integration`.

1) Ask someone with admin access to the DSS `prod` bucket (`org-hca-dss-prod`)
   to add the following statements to the bucket policy of said bucket. The
   first statement gives the destination DSS read access to the source DSS
   instance. The second statement gives you read access to that bucket (needed
   for direct access) and permission to set tags on objects (needed for
   `--fix-tags`).

   ```json
   [
       {
           "Sid": "copy-bundles",
           "Effect": "Allow",
           "Principal": {
               "AWS": [
                   "arn:aws:iam::861229788715:role/dss-integration",
                   "arn:aws:iam::861229788715:role/dss-s3-copy-sfn-integration",
                   "arn:aws:iam::861229788715:role/dss-s3-copy-write-metadata-sfn-integration"
               ]
           },
           "Action": [
               "s3:GetObject",
               "s3:GetObjectTagging"
           ],
           "Resource": "arn:aws:s3:::org-hca-dss-prod/*"
       },
       {
           "Sid": "direct-read-access-and-retag-blobs",
           "Effect": "Allow",
           "Principal": {
               "AWS": [
                   "arn:aws:iam::861229788715:role/dcp-admin",
                   "arn:aws:iam::861229788715:role/dcp-developer"
               ]
           },
           "Action": [
               "s3:GetObject",
               "s3:GetObjectTagging",
               "s3:PutObjectTagging"
           ],
           "Resource": [
               "arn:aws:s3:::org-hca-dss-prod/*"
           ]
       }
   ]
   ```

2) Select the `integration` deployment:

   ```
   _select integration
   ```

3) Run

   ```
   python scripts/copy_bundles.py --map-version 1.374856 \
                                  --fix-tags \
                                  --source https://dss.data.humancellatlas.org/v1 \
                                  --destination https://dss.integration.data.humancellatlas.org/v1 \
                                  --manifest /path/to/manifest.tsv
   ```

   The `--map-version` option adds a specific duration to the version of each
   copied file and bundle. Run `python scripts/copy_bundles --help` for details.

# 8. Scale testing

[Locust]: https://locust.io/

Scale testing can be done with [Locust]. Locust is a development requirement so
running it is straight-forward with your development environment set up.

1. Make sure Locust is installed by running

   ```
   locust --version
   ```

   If it is not installed, do step 1.3 in this README.

2. To scale test the Azul web service on integration run

   ```
   locust -f scripts/locust/service.py
   ```

   If you want to test against a different stage use the `--host` option:

   ```
   locust -f scripts/locust/service.py --host https://service.dev.singlecell.gi.ucsc.edu
   ```

3. Navigate to `http://localhost:8090` in your browser to start a test run.

[Locust documentation]: https://docs.locust.io/en/stable/

For more advanced usage refer to the official [Locust documentation].


# 9. Continuous deployment and integration

For the purposes of continually testing and deploying the Azul application, we 
run the community edition of GitLab on a project-specific EC2 instance. There is 
currently one such instance for the `sandbox` and `dev` deployments and another 
one for `prod`.

The GitLab instances are provisioned through the `gitlab` *Terraform component*.
For more information about *Terraform components*, refer the [section on shared 
resources managed by Terraform](#314-shared-resources-managed-by-terraform). 
Within the `gitlab` component, the `dev.gitlab` child component provides a 
single Gitlab EC2 instance that serves our CI/CD needs not only for `dev` but 
for `integration` and `staging` as well. The `prod.gitlab` child component 
provides the Gitlab EC2 instance for `prod`.

To access the web UI of the Gitlab instance for `dev`, visit
`https://gitlab.dev.explore.…/`, authenticating yourself with your GitHub
account. After attempting to log in for the first time, one of the
administrators will need to approve your access. For `prod` use
`https://gitlab.explore.…/`.

[gitlab.tf.json.template.py]: /terraform/gitlab/gitlab.tf.json.template.py

To have the Gitlab instance build a branch, one pushes that branch to the Azul
fork hosted on the Gitlab instance. The URL of the fork can be viewed by
visiting the GitLab web UI. One can only push via SSH, using a public SSH key 
that must be deposited in each user's profile on the GitLab web UI. 

An Azul build on Gitlab runs the `test`, `package`, `deploy`,  and
`integration_test` Makefile targets, in that order. The target deployment for
feature branches is `sandbox`, the protected branches (`develop` and `prod` use 
their respective deployments.


## 9.1 VPN access to GitLab

The GitLab EC2 instance resides in a VPC that can only be accessed through a
VPN. The VPN uses AWS Client VPN. It is Amazon's flavor of OpenVPN. The AWS
Client VPN endpoint is set up by Terraform as part of the `dev.gitlab` and
`prod.gitlab` components. VPN clients authenticate via certificates signed by
a certificate authority (CA) that is self-signed. A system administrator
(currently the technical lead) manages the CA on their local disk. That is
the only place where the private key for signing the CA certificate is kept.
If the CA private key is lost, the CA must be reinitialized, the VPN must be
redeployed and new client certificates must be issued. Each deployment of
GitLab uses a separate CA and therefore a separate set of client
certificates.

Each client certificate is backed by a private key as well. That private key
resides solely on the developer's local disk. If the developer's private key
is lost, a new one must be issued. 

<!--
FIXME: Automate the revocation of VPN client certificates
       https://github.com/DataBiosphere/azul/issues/3929
-->

When a developer with VPN access departs the team, either the entire CA must be
reinitialized and all remaining client certificates reissued or the departing
developer's certificates must be revoked by adding it to the list of revoked
client certificates on the AWS Client VPN instance. The VPN's server's
certificate and private key is stored in ACM so that AWS Client VPN can
authenticate itself to clients and check validity of the certificates that
clients present to the server. Both client and server keys must be signed by
the same CA.

### 9.1.1 Setting up a VPN client

Install an OpenVPN client. On Ubuntu, the respective package is called
`network-manager-openvpn-gnome`. Popular clients for macOS are [Tunnelblick]
(free) and [Viscosity] (for pay, with 30 day trial). For Windows, only 
[Viscosity] was tested but the [official Windows client] may also work there.

[Tunnelblick]: https://tunnelblick.net/index.html
[Viscosity]: https://www.sparklabs.com/viscosity/
[official Windows client]: https://openvpn.net/client-connect-vpn-for-windows/

<!--
FIXME: Figure out why Tunnelblick doesn't work
       https://github.com/DataBiosphere/azul/issues/3930
-->

Generate a certificate request, import the certificate and generate the `.ovpn` 
file containing the configuration for the VPN connection:

```
_select dev.gitlab  # or prod.gitlab, anvildev.gitlab
cd terraform/gitlab/vpn
git submodule update --init easy-rsa
make init  # (do this only once per GitLab deployment)
make request  # then send request to administrator
make import  # paste the certificate
make config > ~/azul-gitlab-dev.ovpn  # or azul-gitlab-prod.ovpn
```

The `make init` step creates a PKI directory in `~/.local/share` outside of the 
Azul source tree. It should only be done once per GitLab deployment. On a second 
attempt it will ask for confirmation to overwrite the existing directory. If 
confirmed, existing OpenVPN client connections will remain functional (as they 
keep a copy of the private key) but you will lose the ability to regenerate the 
`.ovpn` file.

Now import the generated `.ovpn` file into your client. `make config` prints
instructions on how to do so on Ubuntu. For other VPN clients the process is
pretty much self-explanatory. Delete the file after importing it. It contains
the private key and can always be regenerated again later using `make config`. 

### 9.1.2 Ensuring split tunnel on client

It is important that you configure the client to only route VPC traffic
through the VPN. The VPN server will not forward any other traffic, in what's
commonly referred to as a *split tunnel*. The key indicator of a split tunnel
is that it doesn't set up a default route on the client system. There will
only be a route to the private 172.… subnet of the GitLab VPC but the default
route remains in place. If you configure the VPN connection to set up a
default route, your Internet access will be severed as soon as you establish
the VPN connection. 

The `make config` step prints instruction on how to configure a split tunnel
on Ubuntu. 

For Viscosity, the steps are as follows:

1) Click the Viscosity menu bar icon (or the task bar icon on windows)

2) Click *Preferences*

3) Right-click `azul-gitlab-dev` or `azul-gitlab-prod` -> click *Edit*

4) Click the *Networking* tab

5) Under *All traffic*, select *Automatic (Set by server)*

6) Click *Save*

For Tunnelblick, the steps are as follows:

1) Right-click the Tunnelblick menu bar icon

2) Click *VPN Details …*

3) Click on the left-hand side bar entry for the connection you just imported

4) On the *Settings* tab of the right-hand side of the window, make sure that
   the *Route all IPv4 traffic through the VPN* option is unchecked

### 9.1.2 Setting up the certificate authority

This must be done by a system administrator before a GitLab instance is first 
deployed:

```
_select dev.gitlab  # or prod.gitlab
cd terraform/gitlab/vpn
git submodule update --init easy-rsa
make ca  # initialize the CA (do this only once)
make server  # build the server certificate
make publish  # upload the server certificate to ACM
cd ..
make apply  # (re)deploy GitLab
```

### 9.1.3 Issuing a certificate

To issue a client certificate for a developer so that they can access the VPN,
ask the developer to send you a certificate request as described in the previous 
section . The request must be made under the developer's email address as the 
common name (CN). Sign the request:

```
_select dev.gitlab  # or prod.gitlab
cd terraform/gitlab/vpn
git submodule update --init easy-rsa
make import/joe@foo.org
make sign/joe@foo.org
```

Send the resulting certificate back to the requesting developer.

The communication channel through which requests and certificates are messaged
does not need to be private but it needs to ensure the integrity of the
messages.

### 9.1.4 Revoking a certificate

```
_select dev.gitlab  # or prod.gitlab
cd terraform/gitlab/vpn
git submodule update --init easy-rsa
make revoke/joe@foo.org
make publish_revocations
```

To list all previously issued certificates, use `make list`. 

There are now precautions in place to prevent this situation but I'll mention it 
anyways. If this list contains more than one active certificate for the same CN, 
all but the most recent one needs to be revoked by serial. Since `easyrsa` does
not support this out of the box, we need to jump through some extra hoops:  

```
eval "`make _admin _env`"
mv $EASYRSA_PKI/issued/joe@foo.org.crt $EASYRSA_PKI/issued/joe@foo.org.crt.orig
cp $EASYRSA_PKI/certs_by_serial/<SERIAL_OF_CERT_TO_BE_REVOKED>.pem $EASYRSA_PKI/issued/joe@foo.org.crt
make revoke/joe@foo.org
make publish_revocations
mv $EASYRSA_PKI/issued/joe@foo.org.crt.orig $EASYRSA_PKI/issued/joe@foo.org.crt
```

### 9.1.5 Issuing a certificate on a person's behalf

A private key and OpenVPN configuration can be generated by a system
administrator on behalf of any person that doesn't have a configured working
copy of this repository. Doing so has the disadvantage of making that
person's private key known to the system administrator and anyone that
eavesdrops on the channel through which the OpenVPN configuration
(which includes the private key) is communicated to the person.

To generate the key and OpenVPN configuration file on another person's behalf, 
invoke the `make` steps as outlined in [9.1.1](#911-setting-up-a-vpn-client) and 
[9.1.3](#913-issuing-a-certificate) but use `make client_cn=joe@foo.org` instead 
of `make`.


## 9.2 The Sandbox Deployment

There is only one such deployment and it should be used to validate feature
branches (one at a time) or to run experiments. This implies that access to the
sandbox must be coordinated externally e.g., via Slack. The project lead owns
the sandbox deployment and coordinates access to it.


## 9.3 Security

Gitlab has AWS write permissions for the AWS services used by Azul and the
principle of least privilege is applied as much as IAM allows it. Some AWS
services support restricting the creation and deletion of resource by matching
on the name. For these services, Gitlab can only create, modify or write
resources whose name begins with `azul-*`. Other services, such as API Gateway
only support matching on resource IDs. This is unfortunate because API Gateway
allocates the ID. Since it is therefore impossible to know the ID of an API before
creating it, Gitlab must be given write access to **all** API IDs. For details
refer to the `azul-gitlab` role and the policy of the same name, both defined in
[gitlab.tf.json.template.py].

[permissions boundary]: https://aws.amazon.com/blogs/security/delegate-permission-management-to-developers-using-iam-permissions-boundaries/

Gitlab does not have general write permissions to IAM, its write access is
limited to creating roles and attaching policies to them as long as the roles
and policies specify the `azul-gitlab` policy as a [permissions boundary]. This
means that code running on the Gitlab instance can never escalate privileges
beyond the boundary. This mechanism is defined in the `azul-gitlab-iam` policy.

Code running on the Gitlab instance has access to credentials of a Google Cloud
service account that has write privileges to Google Cloud. This service account 
for Gitlab is created automatically by TF but its private key is not. They need 
to created manually and copied to `/mnt/gitlab/runner/config/etc` on the 
instance. See [section 9.9](#910-the-gitlab-build-environment) for details.


## 9.4 Networking

The networking details are documented in [gitlab.tf.json.template.py]. The
Gitlab EC2 instance uses a VPC and is fronted by an Application Load Balancer
(ALB) and a Network Load Balancer (NLB). The ALB proxies HTTPS access to the
Gitlab web UI, the NLB provides SSH shell access and `git+ssh` access for
pushing to the project forks on the instance.


## 9.5 Storage

The Gitlab EC2 instance is attached to an EBS volume that contains all of
Gitlab's data and configuration. That volume is not controlled by Terraform and
must be created manually before terraforming the `gitlab` component for the
first time. Details about creating and formatting the volume can be found in
[gitlab.tf.json.template.py]. The volume is mounted at `/mnt/gitlab`. The
configuration changes are tracked in a local Git repository on the system 
administrator's computer. The system administrator keeps the configuration files 
consistent between GitLab instances.

When an instance boots and finds the EBS volume empty, Gitlab will initialize it
with default configuration. That configuration is very vulnerable because the
first user to visit the instance will be given the opportunity to chose the root
password. It is therefore important that you visit the Gitlab UI immediately
after the instance boots for the first time on an empty EBS volume.

Other than that, the default configuration is functional but lacks features like
sign-in with Github and a Docker image repository. To enable those you could
follow the respective Gitlab documentation but a faster approach is to compare
`/mnt/gitlab/config/gitlab.rb` between an existing Gitlab instance and the new
one. Just keep in mind that the new instance might have a newer version of
Gitlab which may have added new settings. You may see commented-out default
settings in the new gitlab.rb file that may be missing in the old one.

## 9.5.1 Freeing up storage space

There are three docker daemons running on the instance: the RancherOS system 
daemon, the RancherOS user daemon and the Docker-in-Docker (DIND) daemon. For 
reasons unknown at this time, the DIND keeps caching images, continually 
consuming disk space until the `/mnt/gitlab` volume fills up. In the past, this 
occurred once every six months or so. One of the symptoms might be a failing unit
test job with message like 

> `2021-03-11 19:38:05,133 WARNING MainThread: There was a general error with document ContributionCoordinates(entity=EntityReference(entity_type='files', entity_id='5ceb5dc3-9194-494a-b1df-42bb75ab1a04'), aggregate=False, bundle=BundleFQID(uuid='94f2ba52-30c8-4de0-a78e-f95a3f8deb9c', version='2019-04-03T103426.471000Z'), deleted=False): {'_index': 'azul_v2_dev_test_files', '_type': 'doc', '_id': '5ceb5dc3-9194-494a-b1df-42bb75ab1a04_94f2ba52-30c8-4de0-a78e-f95a3f8deb9c_2019-04-03T103426.471000Z_exists', 'status': 403, 'error': {'type': 'cluster_block_exception', 'reason': 'blocked by: [FORBIDDEN/12/index read-only / allow delete (api)];'}}. Total # of errors: 1, giving up.`

A cron job running on the instance should prevent this by periodically pruning
unused images. If the above error occurs despite that, there might be a problem
with that cron job. To manually clean up unused images run:

```
sudo docker exec -it gitlab-dind docker image prune -a --filter "until=720h"
```

on the instance.

## 9.6 The Gitlab web application

The instance runs Gitlab CE running inside a rather elaborate concoction of
Docker containers. See [gitlab.tf.json.template.py] for details. Administrative
tasks within a container should be performed with `docker exec`. To reconfigure
Gitlab, for example, one would run `docker exec -it gitlab gitlab-ctl
reconfigure`.


## 9.7 Registering the Gitlab runner

The runner is the container that performs the builds. The instance is configured
to automatically start that container. The primary configuration for the runner
is in `/mnt/gitlab/runner/config/config.toml`. There is one catch, on a fresh
EBS volume that just been initialized, this file is missing, so the container
starts but doesn't advertise any runners to Gitlab.

The easiest way to create the file is to kill the `gitlab-runner` container and
the run it manually using the `docker run` command from the `systemd` unit file, 
but adding `-it` after `run` and `register` at the end of the command. 
You will be prompted to supply a URL and a registration token as
[documented here](https://docs.gitlab.com/runner/register/).

Note that since version 15.0.0 of GitLab, there is no way to convert a runner
from shared to project-specific or vice versa. If you want to register a runner
reserved to a specific group, you must get the registration token from
the *CI/CD* — *Runners* page of the respective group. Runners reserved to a
project must be registered from the project's *Settings* — *CI/CD* — *Runners*
page. Shared runners are registered via *Admin* — *Overview* — *Runners*. 

Specify `docker` as the runner type and 

`docker.gitlab.anvil.gi.ucsc.edu/ucsc/azul/runner:latest` 

as the image for Azul runners. For generic runners you could use the 
`docker:20.10.18-ce` image instead, but you'd need to match the tag (aka 
version) of the image currently used for the `gitlab-dind` container. 

Here's an example terminal transcript:

```
$ systemctl stop gitlab-runner.service

$ systemctl show gitlab-runner.service | grep ExecStart=
ExecStart={ path=/usr/bin/docker ; argv[]=/usr/bin/docker run --name gitlab-runner …

$ /usr/bin/docker run -it --name gitlab-runner --rm --volume /mnt/gitlab/runner/config:/etc/gitlab-runner --network gitlab-runner-net --env DOCKER_HOST=tcp://gitlab-dind:2375 gitlab/gitlab-runner:v15.9.1 register
Runtime platform                                    arch=amd64 os=linux pid=7 revision=d540b510 version=15.9.1
Running in system-mode.

Enter the GitLab instance URL (for example, https://gitlab.com/):
https://gitlab.prod.anvil.gi.ucsc.edu/
Enter the registration token:
REDACTED
Enter a description for the runner:
[cd20ca0ec956]:
Enter tags for the runner (comma-separated):

Enter optional maintenance note for the runner:

WARNING: Support for registration tokens and runner parameters in the 'register' command has been deprecated in GitLab Runner 15.6 and will be replaced with support for authentication tokens. For more information, see https://gitlab.com/gitlab-org/gitlab/-/issues/380872
Registering runner... succeeded                     runner=GR1348941eDiqsoCC
Enter an executor: docker, shell, ssh, docker-ssh+machine, instance, custom, docker-ssh, parallels, virtualbox, docker+machine, kubernetes:
docker
Enter the default Docker image (for example, ruby:2.7):
docker.gitlab.anvil.gi.ucsc.edu/ucsc/ azul/runner:latest
Runner registered successfully. Feel free to start it, but if it's running already the config should be automatically reloaded!

Configuration (with the authentication token) was saved in "/etc/gitlab-runner/config.toml"
```

Once the container exits, `config.toml` should have been created. Edit it and 
adjust the `volumes` setting to read

```
volumes = ["/var/run/docker.sock:/var/run/docker.sock", "/cache", "/etc/gitlab-runner/etc:/etc/gitlab"]
```

If you already have a GitLab instance to copy `config.toml` from, do that and
register the runners as described above. Copy the runner tokens from the newly
added runners at the end of config.toml to the preexisting runners. Then
discard the newly added runners from the file. For another instance's
`config.toml` to work on a new instance, the only piece of information that
needs to be updated is the runner token. That's because the runner token is
derived from the registration token which is different between the two
instances.

Finally, start the runner unit using `systemctl start gitlab-runner.service` or
simply reboot the instance. Either way, the Gitlab UI should now show the newly 
registered runners.


## 9.8 The Gitlab runner image for Azul

Because the first stage of the Azul pipeline on Gitlab creates a dedicated image
containing the dependencies of the subsequent stages, that first stage only
requires the `docker` client binary, `make` and `bash` to be in the runner.
These are provided by yet another custom Docker image for the Gitlab runner that
executes Azul builds. This image must be created when the EBS volume attached to
the Gitlab instance is first provisioned, or when the corresponding Dockerfile
is modified. See `terraform/gitlab/runner/Dockerfile` for details on how to
build the image and register it with the runner.


## 9.9 Updating Gitlab

Modify the Docker image tags in [gitlab.tf.json.template.py] and run `make
apply` in `terraform/gitlab`. The instance will be terminated (the EBS volume
will survive) and a new instance will be launched, with fresh containers from
updated images. This should be done regularly.


## 9.10 The Gitlab Build Environment

The `/mnt/gitlab/runner/config/etc` directory on the Gitlab EC2 instance is
mounted into the build container as `/etc/gitlab`. The Gitlab build for Azul
copies the files from the `azul` subdirectory of that directory into the Azul
project root. Secrets and other Gitlab-specific settings should be specified in
`/mnt/gitlab/runner/config/etc/azul/environment.local` which will end up in
`${project_root}/environment.local` where `source environment` will find and load
them. For secrets, we prefer this mechanism over specifying them as environment
variables under project settings on the Gitlab web UI. Only people with push
access can push code to intentionally or accidentally expose those variables,
push access is tied to shell access which is what one would normally need to
modify those files.


## 9.11. Cleaning up hung test containers

When cancelling the `make test` job on Gitlab, test containers will be left
running. To clean those up, ssh into the instance as described in
[gitlab.tf.json.template.py] and run `docker exec gitlab-dind docker ps -qa |
xargs docker exec gitlab-dind docker kill` and again but with `rm` instead
of `kill`.


# 10. Kibana and Cerebro

Kibana is a web UI for interactively querying and managing an Elasticsearch
instance. To use Kibana with Azul's AWS Elasticsearch instance, you have two
options:

* For one, you can add your local IP to the policy of Azul's AWS Elasticsearch
  instance and access its Kibana directly. This can take 10 minutes and you
  might have to do it repeatedly because the policy is reset periodically,
  potentially multiple times a day.

* Alternatively, you can use `scripts/kibana_proxy.py` to run Kibana locally
  and have it point at Azul's AWS Elasticsearch instance. The script also
  starts a signing proxy which eliminates the need to add your local IP to the
  Elasticsearch policy, using your local AWS credentials instead for
  authentication.

  For the script to work, you need to

  * have Docker installed,

  * a deployment selected, and

  * `environment` sourced.

[Cerebro] is a cluster management web UI for Elasticsearch. It is very useful
for determining the status of individual nodes and shards. In addition to the
Kibana container, `scripts/kibana_proxy.py` also starts one for Cerebro.

Look for this line in the script output:

```
Now open Kibana at http://127.0.0.1:5601/ and open Cerebro at
http://127.0.0.1:5602/#/overview?host=http://localhost:5603 (or paste in
http://localhost:5603)
```

and open the specified URLs in your browser.

[Cerebro]: https://github.com/lmenezes/cerebro

## 10.1 Connecting Kibana to a local Elasticsearch instance

Certain unit tests use a locally running Elasticsearch container. It's possible 
to connect a Kibana instance to such a container, in order to aid debugging.

While the unit test is running (paused at a breakpoint), open a terminal window.

Download the Kibana container:

```
kibana_image=$azul_docker_registry$(python -m azul "config.docker_images['kibana']")
docker pull $kibana_image
```

Copy the container name for the Elasticsearch instance you want to examine. This
is likely the most recent entry in

```
docker ps
```

Run

```
docker run --link ES_CONTAINER_NAME:elasticsearch -p 5601:5601 $kibana_image
```

where `ES_CONTAINER_NAME` is what you copied from above.

Kibana should now be available at `http://0.0.0.0:5601`.

Some of these steps were taken or modified from the official [Elasticsearch 
documentation](https://www.elastic.co/guide/en/kibana/7.10/docker.html#_run_kibana_on_docker_for_development).

# 11. Managing dependencies

We pin all dependencies, direct and transitive ones alike. That's the only way
to get a somewhat reproducible build. It's possible that the build still
fails if a dependency version is deleted from pypi.org or if a dependency
maintainer re-releases a version, but aside from caching all dependencies,
pinning them is next best thing for reproducibility of the build.

Now, while pinning direct dependencies should be routine, chasing down
transitive dependencies and pinning those is difficult, tedious and prone to
errors. That's why we automate that step: When a developer updates, adds or
removes a direct dependency, running `make requirements_update` will reevaluate
all transitive dependencies and update their pins. If the added direct
dependency has transitive dependencies, those will be picked up. It's likely
that the reevaluation picks up updates to transitive dependencies unrelated to
the modified direct dependency, but that's unavoidable. It's even possible that
a direct dependency update causes a downgrade of a transitive dependency if the
updated direct dependency further restricts the allowed version range of the
transitive dependency.

We distinguish between run-time and build-time — or _development_ —
dependencies. A run-time dependency is a one that is needed by deployed code.
A build-time dependency is one that is **not** needed by deployed code, but by
some other code, like unit tests, for example. A developer's virtualenv will
have both run-time and build-time dependencies installed. Combined with the
distinction between direct and transitive dependencies this yields four
categories of dependencies. Let's refer to them as DR (direct run-time), TR
(transitive run-time), DB (direct build-time) and TB (transitive build-time).
The intersections DR ∩ TR, DB ∩ TB, DR ∩ DB, TR ∩ TB and DR ∩ TB should all be
empty but the intersection TR ∩ DB may not be.

![Azul architecture diagram](docs/dependencies.svg)

Ambiguities can arise as to which version of a requirement should be used when
multiple requirements have overlapping transitive dependencies. We can't
resolve these ambiguities automatically because different versions of a package
may have different dependencies in and of themselves, so pinning just the
dependency in question might omit some of its dependencies. By pinning it
explicitly the normal dependency resolution kicks in, including all transitive
dependencies of the pinned version.

`make requirements_update` will raise an exception when ambiguous requirements
are found.

```
ERROR   MainThread: Ambiguous version of transitive runtime requirement jsonschema==2.6.0,==3.2.0. Consider pinning it to the version used at build time (==3.2.0).
```

With this example case the solution would be to add `jsonschema` as a
direct run-time requirement in the file `reqirements.txt` along with a comment
`# resolve ambiguity with build-time dependency`, and then to run `make
requirements_update` to remove the package as a transitive run-time requirement.

There is a separate category for requirements that need to be installed before
any other dependency is installed, either run-time or build-time, in order to
ensure that the remaining dependencies are resolved and installed correctly.
We call that category  _pip requirements_ and don't distinguish between direct
or transitive requirements in that category.


# 12. Making wheels

_Note: Support for custom wheels is currently disabled. We don't currently have 
any dependencies for which a binary wheel is unavailable. We'll leave this 
section in place until support is needed and enabled again_  

Some of Azul's dependencies contain native code that needs to be compiled into
a binary executable which is then dynamically loaded into the Python
interpreter process when the package is imported. These dependencies are
commonly distributed in the form of wheels. A wheel is a Python package
distribution that contains the pre-compiled binary code for a particular
operating system and processor architecture combination, aka platform. Many such
packages lack a wheel for the `linux_x86_64` platform that Lambda functions
execute on. Chalice will attempt to build the wheel on the fly during `chalice
package` (`make -C lambdas`) but only if invoked on a system with `linux_x86_64`.
On macOS, Chalice will fail to build a wheel for the `linux_x86_64` platform but
only prints a warning that's easily missed. The deployed Lambda will likely
fail with an import error.

If you add a dependency on a package with native code, you need to build the
wheel manually:

```
(.venv) ~/workspace/hca/azul$ docker run -it -v ${project_root}/:/root/azul python:3.11.5-bullseye bash

root@97804cb60d95:/# pip --version
pip 22.0.4 from /usr/local/lib/python3.11/site-packages/pip (python 3.11)

root@97804cb60d95:/# cd /root/azul/lambdas/.wheels

root@97804cb60d95:~/azul/lambdas/.wheels# pip wheel jsonobject==2.0.0
Collecting jsonobject==2.0.0
  Downloading jsonobject-2.0.0.tar.gz (402 kB)
     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 403.0/403.0 KB 9.0 MB/s eta 0:00:00
  Preparing metadata (setup.py) ... done
Collecting six
  Downloading six-1.16.0-py2.py3-none-any.whl (11 kB)
Saved ./six-1.16.0-py2.py3-none-any.whl
Building wheels for collected packages: jsonobject
  Building wheel for jsonobject (setup.py) ... done
  Created wheel for jsonobject: filename=jsonobject-2.0.0-cp39-cp39-linux_x86_64.whl size=1606493 sha256=7f69b1ef612e13265ea95817e24b7d33ec63f07c0924f8c8692ee689679e1a18
  Stored in directory: /root/.cache/pip/wheels/c1/1b/00/8958e64a98b73db2ca8d997a7034c93b545cdcf30054aa7e43
Successfully built jsonobject

root@97804cb60d95:~/azul/lambdas/.wheels# ls -l
total 1584
-rw-r--r-- 1 root root 1606493 May 10 00:35 jsonobject-2.0.0-cp39-cp39-linux_x86_64.whl
-rw-r--r-- 1 root root   11053 May 10 00:35 six-1.16.0-py2.py3-none-any.whl

root@97804cb60d95:~/azul/lambdas/.wheels# exit
exit

(.venv) ~/workspace/hca/azul$ ls -l lambdas/.wheels
total 1584
-rw-r--r-- 1 root root 1606493 May  9 17:35 jsonobject-2.0.0-cp39-cp39-linux_x86_64.whl
-rw-r--r-- 1 root root   11053 May  9 17:35 six-1.16.0-py2.py3-none-any.whl

(.venv) ~/workspace/hca/azul$ sudo chown -R `id -u`:`id -g` lambdas/.wheels

(.venv) ~/workspace/hca/azul$ ls -l lambdas/.wheels
total 1584
-rw-r--r-- 1 hannes hannes 1606493 May  9 17:35 jsonobject-2.0.0-cp39-cp39-linux_x86_64.whl
-rw-r--r-- 1 hannes hannes   11053 May  9 17:35 six-1.16.0-py2.py3-none-any.whl
(.venv) ~/workspace/hca/azul$ 
```

Then modify the `wheels` target in `lambdas/*/Makefile` to unzip the wheel into
the corresponding vendor directory.

Also see https://chalice.readthedocs.io/en/latest/topics/packaging.html


# 13. Development tools


## 13.1 OpenAPI development

[Azul Service OpenAPI page]: https://service.dev.singlecell.gi.ucsc.edu/

To assist with adding documentation to the [Azul Service OpenAPI page] we can
run the service app locally:

```
make -C lambdas/service local
```

The script serves the Swagger editor locally at a URL where your current version
of the API documentation is visible. Change the docs in `azul/service/app.py`,
save, refresh the page, and your changes will appear immediately.


## 13.2 Tracking changes to the OpenAPI definition

Changes to the OpenAPI definition are tracked in the source tree. When making 
changes that affect the definition, run:

```
make -C lambdas openapi
```

and commit any modifications to the `openapi.json` file. Failure to do so will 
break continuous integration during `make check_clean`.
