

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

- Python 3.8 (no other Python 3.x is supported) with `pip`.

- The `bash` shell

- [Docker] for running the tests (the community edition is sufficient).
  The minimal required version is uncertain, but 19.03, 18.09, and 17.09 are
  known to work.

- Terraform (optional, to create new deployments). Refer the official
  documentation on how to [install terraform]. On macOS with Homebrew installed,
  'brew install terraform' works, too.

- AWS credentials configured in `~/.aws/credentials` and/or `~/.aws/config`

[install terraform]: https://www.terraform.io/intro/getting-started/install.html
[Docker]: https://docs.docker.com/install/overview/


## 2.2 Runtime Prerequisites (Infrastructure)

An instance of the HCA [Data Store] aka DSS. The URL of that instance can be
configured in `environment.py` or `deployments/*/environment.py`.

The remaining infrastructure is managed internally using TerraForm.


## 2.3 Project configuration

Getting started without attempting to make contributions does not require AWS
credentials. A subset of the test suite passes without configured AWS
credentials. To validate your setup, we'll be running one of those tests at the
end.

1. Activate the `dev` deployment:

   ```
   (cd azul/deployments && ln -snf dev .active)
   ```

2. Load the environment:

   ```
   source environment
   ```

   The output should indicate that the environment is being loaded from the
   selected deployment (in this case, `dev`).

3. Create a Python 3.8 virtual environment and activate it:

   ```
   make virtualenv
   source .venv/bin/activate
   ```

4. Install the development prerequisites:

   ```
   make requirements
   ```

   Linux users whose distribution does not offer Python 3.8 should consider
   installing [pyenv] and then Python 3.8 using `pyenv install 3.8.3` and
   setting `PYENV_VERSION` to `3.8.3`. You may need to update pyenv itself
   before it recognizes the given Python version. Even if a distribution
   provides the  required minor version of Python natively, using pyenv is
   generally preferred because it offers every patch-level release of Python,
   supports an arbitrary number of different Python versions to be installed
   concurrently and allows for easily switching between them.

   Ubuntu users using their system's default Python 3.8 installation must
   install `python3-dev` before the wheel requirements can be built.

   ```
   sudo apt install python3-dev
   ```

   [pyenv]: https://github.com/pyenv/pyenv


5. Run `make`. It should say `Looking good!` If one of the check target fails,
   address the failure and repeat. Most check targets are defined in `common.mk`.

6. Make sure Docker works without explicit root access. Run the following
   command *without `sudo`*:

   ```
   docker ps
   ```

   If that fails, you're on your own.

7. Finally, confirm that everything is configured properly on your machine by
   running the unit tests:

   ```
   make test
   ```


### 2.3.1 AWS credentials

You should have been issued AWS credentials. Typically, those credentials
require assuming a role in an account other than the one defining your IAM
user.  Just set that up normally in `~/.aws/config` and `~/.aws/credentials`.
If the  assumed role additionally requires an MFA token, you should run
`_preauth`  immediately after running `source environment` or switching
deployments with  `_select`.


### 2.3.2 Google Cloud credentials

When it comes to Azul and Google Cloud, we distinguish between three types of 
accounts: an Azul deployment uses a *service account* to authenticate against
Google Cloud and Azul developers use their *individual Google account* in a web
browser and a *personal service account* for programmatic interactions. For
the remainder of this section we'll refer to the individual Google account
simply as "your account". For developers at UCSC this is their `…@ucsc.edu`
account.

1.  On Slack, ask for your account to be added as an owner of the Google Cloud
    project that hosts—or will host—the Azul deployment you intend to work with. 
    For the lower HCA DCP/2 deployments (`dev`, `sandbox` and personal
    deployments), this is `platform-hca-dev`. The project name is configured
    via the `GOOGLE_PROJECT` variable in `environment.py` for each deployment.

2.  Log into your account on https://console.cloud.google.com and select that
    project.

3.  Navigate to *IAM & Admin*, locate your account in the list, take note of
    the email address found in the *Member* column (e.g. `alice@example.com`)

4.  Create a service account for yourself in that project. Under *IAM & admin*,
    *Service Accounts* click *CREATE SERVICE ACCOUNT* and 

5.  For *Service account name* use the local part of the email address noted in 
    step 3 above e.g. `alice`

6.  For *Service account ID* use the provided default

7.  Click *CREATE* to progress to *Grant this service account access to project*

8.  Under *Select a role* click `Project`, then click `Owner`.

9.  Click *CONTINUE* to progress to *Grant users access to this service account 
    (optional)*

10. Click *DONE*, to return to the list of service accounts

11. Click on the newly created account to edit it

12. Under *Keys* click *ADD KEY* and *Create new key* 

13. Select *JSON* and click *CREATE* to download the private key file

14. When prompted, store the private key file in a safe location

    ```
    mkdir /Users/alice/.gcp
    mv /Users/alice/Downloads/example-project-name_key.json /Users/alice/.gcp/
    ```

15. Edit the `environment.local.py` file for your personal deployment:

    ```
    vim /Users/alice/azul/deployments/alice.local/environment.local.py
    ```

    and modify the `GOOGLE_APPLICATION_CREDENTIALS` variable:

    ```
    'GOOGLE_APPLICATION_CREDENTIALS': '/Users/alice/.gcp/example-project-name_key.json'
    ```

16. Repeat the previous step for other deployments as needed or alternatively
    create a symlink to your deployment's `environment.local.py` file:

    ```
    cd /Users/alice/azul/deployments/dev
    vim environment.local.py
    ```

    or, alternatively:

    ```
    cd /Users/alice/azul/deployments/dev
    ln -snf ../alice.local/environment.local.py environment.local.py
    ```

Alternatively, create an `environment.local.py` file in the project root
directory and specify a global default for `GOOGLE_APPLICATION_CREDENTIALS`
there.


### 2.3.3 Google Cloud, TDR and SAM


The Terra ecosystem is tightly integrated with Google's authentication
infrastructure, and the same three types of accounts mentioned in the previous
section are used to authenticate against SAM and [Terra Data Repository]
(TDR). However, because the production instances of Terra, SAM and TDR must
not share user accounts with other instances of those services, we were asked
to create dedicated burner Google accounts for non-production use. 

If you intend to work with an Azul deployment that uses non-production
instances of SAM or TDR, you need to create such a burner account for
yourself. Developers at UCSC, by convention, have been creating Google
accounts called `….ucsc.edu@gmail.com`. This means that there are now at least
four Google accounts at play: 

1) your individual Google account ("your account"),

2) your personal Google service account, 

3) your individual burner account ("your burner") and

4) a service account for each shared or personal Azul deployment.

You use your account to interact with Google Cloud in general and the
production instance of Terra, SAM and TDR, assuming you have access. You use
your personal service account for programmatic interactions with the above.
You use your burner to interact with non-production instances of Terra, SAM
and TDR and the Google Cloud resources they own, like the BiqQuery datasets
and GCS buckets that TDR manages. For programmatic access to the latter, you
can either `gcloud auth login` with your burner or use the
`service_account_credentials` context manager from `aws.deployment`.

[Terra Data Repository]: https://jade.datarepo-dev.broadinstitute.org/

In order for an Azul deployment to index metadata stored in a TDR instance,
the Google service account for that deployment must be registered with SAM and
authorized for repository read access to datasets and snapshots.

The SAM registration of the service account is handled automatically during
`make deploy`. To register without deploying, run `make sam`. Mere
registration with SAM only provides authentication. Authorization to access
TDR datasets and snapshots is granted by adding the registered service account
to a dedicated SAM group (an extension of a Google group). This must be
performed manually by someone with administrator access to that SAM group. For
non-production instances of TDR the group is `azul-dev`. The only members in
that group should be burner accounts and service accounts belonging to
non-production deployments of Azul.

A member of the `azul-dev` group has read access to TDR, and an
*administrator*  of that group can add other accounts to it, and optionally
make them  administrators, too.  Before any account can be added to the group,
it needs to be registered with SAM. While `make deploy` does this
automatically for the deployment's service account, for your burner you must
follow the steps below:


1. Log into Google Cloud by running

    ```
    gcloud auth login
    ```

    A browser window opens to complete the authentication flow interactively.
    When being prompted, select your burner.

    For more information refer to the Google authorization
    [documentation](https://cloud.google.com/sdk/docs/authorizing).

2. Register your burner with SAM. Run

    ```
    (account="$(gcloud config get-value account)"
    token="$(gcloud auth --account $account print-access-token)"
    curl $AZUL_SAM_SERVICE_URL/register/user/v1  -d "" -H "Authorization: Bearer $token")
    ```

3. Ask an administrator of the `azul-dev` group to add your burner to the
   group. The best way to reach an administrator is via the `#team-boardwalk`
   channel on Slack. Also, ask for a link to the group and note it in your
   records.

4. If you've already attempted to create your deployment via `make deploy`,
   visit the link, sign in as your burner and add your deployment's service
   account to the group. Run `make deploy` again.

For production, use the same procedure, but substitute `azul-dev` with
`azul-prod` and "burner" with "account".


### 2.3.4 Creating a personal deployment

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

Most of the cloud resources used by a particular deployment (personal or shared)
are provisioned automatically by `make deploy`. A handful of  resources must be
created manually before invoking these Makefile targets for the first time in a
particular AWS account. This only needs to be done once per AWS account, before
the first Azul deployment is created in that account. Additional deployments do 
not require this step.

Create an S3 bucket for shared Terraform and Chalice state. That bucket should
have object versioning enabled and must not be publicly accessible since
Terraform state may include secrets. If your developers assume a role via
Amazon STS, the bucket should reside in the same region as the Azul deployment.
This is because temporary STS AssumeRole credentials are specific to a region
and won't be recognized by an S3 region that's different from the one the
temporary credentials were issued in. To account for the region specificity of
the bucket, you may want to include the region name at then end of the bucket
name. That way you can have consistent bucket names across regions.

### 3.1.1 Route 53 hosted zones

Create a Route 53 hosted zone for the Azul service and indexer. Multiple
deployments can share a hosted zone, but they don't have to. The name of the
hosted zone is configured with `AZUL_DOMAIN_NAME`. `make deploy` will
automatically provision record sets in the configured zone, but it will not
create the zone itself or register the  domain name it is associated with.

Optionally create another hosted zone for the URL shortener. The URLs produced
by the Azul service's URL shortening endpoint will refer to this zone. The name
of this zone is configured in `AZUL_URL_REDIRECT_BASE_DOMAIN_NAME`. It should be
supported to use the same zone for both `AZUL_URL_REDIRECT_BASE_DOMAIN_NAME` and
`AZUL_DOMAIN_NAME` but this was not tested. The shortener zone can be a
subdomain of the main Azul zone, but it doesn't have to be.

The hosted zone(s) should be configured with tags for cost tracking. A list of
tags that should be provisioned is noted in
[src/azul/deployment.py:tags](src/azul/deployment.py).

### 3.1.2 EBS volume for Gitlab

If you intend to set up a Gitlab instance for CI/CD of your Azul deployments, an
EBS volume needs to be created as well. See [gitlab.tf.json.template.py] and the
[section on CI/CD](#9-continuous-deployment-and-integration) and for details.

## 3.2 One-time manual configuration of deployments

### 3.2.1 Google OAuth 2.0 client for Terra

In order for users to authenticate using OAuth 2.0, an OAuth 2.0 client ID must
be created. This step should only be done once per Google project, e.g., for 
`dev` and `prod`.

1. Log into the Google Cloud console and select the desired project.

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
  
11. Navigate to *APIs & Services* -> *Credentials*; click *+ CREATE CREDENTIALS*
   -> *OAuth Client ID*

12. For *Application Type*, select *Web application*

13. For *Name*, enter `azul-{stage}` where stage is the same as in step 6

14. Click *Create*

15. Click *PUBLISH APP* and *CONFIRM*

###3.2.2 Configuring the OAuth 2.0 client per-deployment

Personal deployments share an OAuth 2.0 client ID with the `dev` deployment.
Provisioning the client ID is covered in [One-time provisioning of shared cloud resources](#31-one-time-provisioning-of-shared-cloud-resources).
The shared credentials must be manually configured to accept requests from each
deployment.

1. Log into the Google Cloud console and select the Google project used for the 
   `dev` deployment.

2. Navigate to *APIs & Services* -> *Credentials*

3. Under *OAuth 2.0 Client IDs*, select `azul-dev` and click the pencil icon to
   edit

4. Add an entry to *Authorized JavaScript origins* and enter the output from
   `python3 -c 'from azul import config; print(config.service_endpoint()'`

5. Add an entry to *Authorized redirect URIs*. Append `/oauth2_redirect` to the
    value of the previous field and enter the resulting value.

6. Click *SAVE*

7. Copy the OAuth Client ID (_not_ the client secret) and insert it into the
    deployment's `environment.py` file:

    ```
    'AZUL_GOOGLE_OAUTH2_CLIENT_ID': 'the-client-id'
    ```

8. `_refresh`

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
[subscribe](#36-subscribing-to-dss) to notifications by a DSS instance or
initiate a [reindexing](#37-reindexing), or both. In an existing deployment
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

## 3.6 Subscribing to DSS

Once deployed, the indexer can be registered to receive notifications about new
bundles from the configured DSS instance.

```
make subscribe
```

By default, the creation of that subscription is enabled (see
`AZUL_SUBSCRIBE_TO_DSS` in `environment.py`). All shared deployments in
`deployments/` inherit that default.

Personal deployments should not be permanently subscribed to any DSS instance
because they are more likely to be broken, causing unnecessary load on the DSS
instance when it retries sending notifications to a broken personal Azul
deployment. To temporarily subscribe a personal deployment, set
`AZUL_SUBSCRIBE_TO_DSS` to 1 and run `make subscribe`. When you are done, run
`make unsubscribe` and set `AZUL_SUBSCRIBE_TO_DSS` back to 0.

Subscription requires credentials to a Google service account with permission
to create another service account under which the subscription is then made.
This indirection exists to facilitate shared deployments without having to
share any one person's Google credentials. The indexer service account must
belong to a GCP project that is allow-listed in the DSS instance to which the
indexer is subscribed to. The credentials of the indexer service account are
stored in Amazon Secrets Manager.

See [Google Cloud credentials](#232-google-cloud-credentials) for details.

## 3.7 Reindexing

The DSS instance used by a deployment is likely to contain existing bundles. To
index them run:

```
make reindex
```

When reindexing, artificial notifications are generated by Azul. To distinguish
from legitimate notifications made by the DSS, the `subscription_id` field is
hardcoded to be `cafebabe-feed-4bad-dead-beaf8badf00d`.

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

4. Unsubscribe

   ```
   make unsubscribe
   ```

5. Delete the API Gateway base path mappings

   ```
   cd terraform
   make init
   terraform destroy $(terraform state list | grep aws_api_gateway_base_path_mapping | sed 's/^/-target /')
   cd ..
   ```

6. Destroy cloud infrastructure

   ```
   make -C terraform destroy
   ```

   The destruction of `aws_acm_certificate` resources may time out. Simply
   repeat this step until it succeeds.

7. From the config bucket (see environment var AZUL_VERSIONED_BUCKET),
   delete all keys relating to your deployment.

8. Delete the local Terraform state file at
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


## `NoSuchBucket` during `make deploy`

```
Initializing the backend...
Backend configuration changed!

Terraform has detected that the configuration specified for the backend
has changed. Terraform will now check for existing state in the backends.


Error inspecting states in the "s3" backend:
    NoSuchBucket: The specified bucket does not exist
```

… but the bucket does exist. Make sure
`deployments/.active/.terraform/terraform.tfstate` refers to the correct bucket,
the one configured in `AZUL_VERSIONED_BUCKET`. If it doesn't, you may
have to remove that file or modify it to fix the bucket name.


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
_preauth
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
diverging to reflect different states on PyPI. This can be fixed by running
`make requirements_update_force` instead.


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

## 7.4 Debugging running lambdas via Pycharm

It's possible to connect a remote debugger to a running lambda function.

Instructions in [`remote_debug.py`](https://github.com/DataBiosphere/azul/blob/develop/src/azul/remote_debug.py#L25)
explain how to do this.

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

We use two automated deployments performed on a project-specific EC2 instance
running Gitlab. There is currently one such instance for the `dev`,
`integration` and `staging` deployments and another one for `prod`.

The Gitlab instances are provisioned through Terraform but its resource
definitions reside in a separate *Terraform component*. A *Terraform component*
is a set of related resources. Each deployment has at least a main component
and zero or more subcomponents. The main component is identified by the empty
string for a name, child components have a non-empty name. The `dev` component
has a subcomponent `dev.gitlab`. To terraform the main component of the `dev`
deployment, one selects the `dev` deployment and runs `make apply` from
`${project_root}/terraform`. To deploy the `gitlab` subcomponent of the `dev`
deployment, one selects `dev.gitlab` and runs `make apply` from
`${project_root}/terraform/gitlab`. The `dev.gitlab` subcomponent provides a
single Gitlab EC2 instance that serves our CI/CD needs not only for `dev` but
for `integration` and `staging` as well. The `prod.gitlab` subcomponent
provides the Gitlab EC2 instance for `prod`.

To access the web UI of the Gitlab instance for `dev`, visit
`https://gitlab.dev.explore.…/`, authenticating yourself with your GitHub
account. After attempting to log in for the first time, one of the
administrators will need to approve your access. For `prod` use
`https://gitlab.explore.…/`.

[gitlab.tf.json.template.py]: /terraform/gitlab/gitlab.tf.json.template.py

To have the Gitlab instance build a branch, one pushes that branch to the Azul
fork hosted on the Gitlab instance. The URL of the fork can be viewed by
visiting the GitLab web UI. One can only push via SSH and only a specific set of
public keys are allowed to push. These keys are configured in
[gitlab.tf.json.template.py]. A change to that file—and this should be obvious
by now—requires running `make apply` in `${project_root}/terraform/gitlab` while
having `dev.gitlab` selected.

An Azul build on Gitlab runs the `test`, `package`, `deploy`, `subscribe` and
`integration_test` Makefile targets, in that order. The target deployment for
feature branches is `sandbox`, the protected branches use their respective
deployments.


## 9.1 The Sandbox Deployment

There is only one such deployment and it should be used to validate feature
branches (one at a time) or to run experiments. This implies that access to the
sandbox must be coordinated externally e.g., via Slack. The project lead owns
the sandbox deployment and coordinates access to it.


## 9.2 Security

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
instance. See [section 9.9](#99-the-gitlab-build-environment) for details.


## 9.3 Networking

The networking details are documented in [gitlab.tf.json.template.py]. The
Gitlab EC2 instance uses a VPC and is fronted by an Application Load Balancer
(ALB) and a Network Load Balancer (NLB). The ALB proxies HTTPS access to the
Gitlab web UI, the NLB provides SSH shell access and `git+ssh` access for
pushing to the project forks on the instance.


## 9.4 Storage

The Gitlab EC2 instance is attached to an EBS volume that contains all of
Gitlab's data and configuration. That volume is not controlled by Terraform and
must be created manually before terraforming the `gitlab` component for the
first time. Details about creating and formatting the volume can be found in
[gitlab.tf.json.template.py]. The volume is mounted at `/mnt/gitlab`. The
configuration changes are tracked in a Git repository under `/mnt/gitlab/.git`
which is tracked in an AWS CodeCommit repo. Since Git isn't installed natively
on RancherOS, you must use a Docker image for it. An alias for this is defined
in the `environment` file of that repository.

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

## 9.4.1 Freeing up storage space

There are three docker daemons running on the instance: the RancherOS system 
daemon, the RancherOS user daemon and the Docker-in-Docker (DIND) daemon. For 
reasons unknown at this time, the DIND keeps caching images, continually 
consuming disk space until the `/mnt/gitlab` volume fills up. In the past, this 
occurred once every six months or so. One of the symptoms might be a failing unit
test job with message like 

> `2021-03-11 19:38:05,133 WARNING MainThread: There was a general error with document ContributionCoordinates(entity=EntityReference(entity_type='files', entity_id='5ceb5dc3-9194-494a-b1df-42bb75ab1a04'), aggregate=False, bundle=BundleFQID(uuid='94f2ba52-30c8-4de0-a78e-f95a3f8deb9c', version='2019-04-03T103426.471000Z'), deleted=False): {'_index': 'azul_v2_dev_test_files', '_type': 'doc', '_id': '5ceb5dc3-9194-494a-b1df-42bb75ab1a04_94f2ba52-30c8-4de0-a78e-f95a3f8deb9c_2019-04-03T103426.471000Z_exists', 'status': 403, 'error': {'type': 'cluster_block_exception', 'reason': 'blocked by: [FORBIDDEN/12/index read-only / allow delete (api)];'}}. Total # of errors: 1, giving up.`

The remedy is to periodically clean up unused images by running:

```
sudo docker exec -it gitlab-dind docker image prune -a
```

on the instance.

## 9.5 Gitlab

The instance runs Gitlab CE running inside a rather elaborate concoction of
Docker containers. See [gitlab.tf.json.template.py] for details. Administrative
tasks within a container should be performed with `docker exec`. To reconfigure
Gitlab, for example, one would run `docker exec -it gitlab gitlab-ctl
reconfigure`.


## 9.6 Registering the Gitlab runner

The runner is the container that performs the builds. The instance is configured
to automatically start that container. The primary configuration for the runner
is in `/mnt/gitlab/runner/config/config.toml`. There is one catch, on a fresh
EBS volume that just been initialized, this file is missing, so the container
starts but doesn't advertise itself to Gitlab. The easiest way to create the
file is to kill the `gitlab-runner` container and the run it manually using
the `docker run` command from the instance user data in
[gitlab.tf.json.template.py], but replacing `--detach` with `-it` and adding
`register` at the end of the command. You will be prompted to supply a URL and
a token as [documented here](https://docs.gitlab.com/runner/register/). Specify
`docker` as the runner type and `docker:18.03.1-ce` as the image. Once the
container exits `config.toml` should have been created. Edit it and adjust the
`volumes` setting to read

```
volumes = ["/var/run/docker.sock:/var/run/docker.sock", "/cache", "/etc/gitlab-runner/etc:/etc/gitlab"]
```

Comparing `config.toml` between an existing instance and the new one doesn't
hurt either. Finally, reboot the instance or manually start the container using
the command from [gitlab.tf.json.template.py] verbatim. The Gitlab UI should
now show the runner.


## 9.7 The Gitlab runner image for Azul

Because the first stage of the Azul pipeline on Gitlab creates a dedicated
image containing the dependencies of the subsequent stages, that first stage
only requires the `docker` client binary, `make` and `bash` to be in the
runner. These are provided by yet another custom Docker image for the Gitlab
runner that executes Azul builds. This image must be created when the EBS
volume attached to the Gitlab instance is first provisioned, or when the
corresponding Dockerfile is modified. See `terraform/gitlab/Dockerfile` for
details on how to build the image and register it with the runner.


## 9.8 Updating Gitlab

Modify the Docker image tags in [gitlab.tf.json.template.py] and run `make
apply` in `terraform/gitlab`. The instance will be terminated (the EBS volume
will survive) and a new instance will be launched, with fresh containers from
updated images. This should be done periodically.


## 9.9 The Gitlab Build Environment

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


## 9.10. Cleaning up hung test containers

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
docker pull docker.elastic.co/kibana/kibana-oss:6.8.0
```

Copy the container name for the Elasticsearch instance you want to examine. This
is likely the most recent entry in

```
docker ps
```

Run

```
docker run --link ES_CONTAINER_NAME:elasticsearch -p 5601:5601 docker.elastic.co/kibana/kibana:6.8.0
```

where `ES_CONTAINER_NAME` is what you copied from above.

Kibana should now be available at `http://0.0.0.0:5601`.

Some of these steps were taken or modified from the official [Elasticsearch 
documentation](https://www.elastic.co/guide/en/kibana/6.8/docker.html#_running_kibana_on_docker_for_development).

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
$ docker run -it -v ${project_root}/:/root/azul python:3.8.3-buster bash

root:/# pip --version
pip 20.1 from /usr/local/lib/python3.8/site-packages/pip (python 3.8)

root:/# cd /root/azul/lambdas/.wheels/

root:~/azul/lambdas/.wheels# pip wheel jsonobject==0.9.9
Collecting jsonobject==0.9.9
  Downloading jsonobject-0.9.9.tar.gz (389 kB)
     |████████████████████████████████| 389 kB 658 kB/s
Collecting six
  File was already downloaded /root/azul/lambdas/.wheels/six-1.14.0-py2.py3-none-any.whl
Skipping six, due to already being wheel.
Building wheels for collected packages: jsonobject
  Building wheel for jsonobject (setup.py) ... done
  Created wheel for jsonobject: filename=jsonobject-0.9.9-cp38-cp38-linux_x86_64.whl size=1767625 sha256=efcbbecbaed194d2b78e6c7b4eb512745636b2bffea6dbdbbdd81e7055c527fe
  Stored in directory: /root/.cache/pip/wheels/b4/41/ea/4aa46d992e8256de18b3c923a792c07b32c2e5d348ca2be376
Successfully built jsonobject

root:~/azul/lambdas/.wheels# ls -l
total 1740
-rw-r--r-- 1 root root 1767621 May 13 16:38 jsonobject-0.9.9-cp38-cp38-linux_x86_64.whl
-rw-r--r-- 1 root root   10938 May 13 16:33 six-1.14.0-py2.py3-none-any.whl

root:~/azul/lambdas/.wheels# rm six-1.14.0-py2.py3-none-any.whl

root:~/azul/lambdas/.wheels# exit

$
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
