[![travis-ci.org](https://travis-ci.org/DataBiosphere/azul.svg?branch=develop)](https://travis-ci.org/DataBiosphere/azul)
[![coveralls.io](https://coveralls.io/repos/github/DataBiosphere/azul/badge.svg?branch=develop)](https://coveralls.io/github/DataBiosphere/azul?branch=develop)
[![codecov.io](https://codecov.io/gh/DataBiosphere/azul/branch/develop/graph/badge.svg)](https://codecov.io/gh/DataBiosphere/azul)

The Azul project contains the components that together serve as the backend to
Boardwalk, a web application for browsing genomic data sets. 

Azul consists of two components: an indexer and a web service. The Azul indexer
is an AWS Lambda function that responds to web-hook notifications about bundle
addition and deletion events occurring in a [data
store](https://github.com/HumanCellAtlas/data-store) instance. The indexer
responds to those notifications by retrieving the bundle's metadata from said
data store, transforming it and writing the transformed metadata into an
Elasticsearch index. The transformation extracts selected entities and
denormalizes the relations between them into a document shape that facilitates
efficient queries on a number of customizable metadata facets.

The Azul web service, another AWS Lambda function fronted by API Gateway,
serves as a thin translation layer between Elasticsearch and the Boardwalk UI,
providing features like pluggable authentication, field name translation and
introspective capabilities such as facet and entity type discovery.

Both the indexer and the web service allow for project-specific customizations
via a plug-in mechanism, allowing the Boardwalk UI codebase to be functionally
generic with minimal need for project-specific behavior.

## 1. Getting Started

### 1.1. Development Prerequisites

- Python 3.6 (3.7 does not work) with `pip`

- The `bash` shell

- Docker for running the tests. The minimal required version is uncertain but 
  18.09 and 17.09 are known to work

- Terraform (optional, to create new deployments):
  https://www.terraform.io/intro/getting-started/install.html On macOS with
  Homebrew installed, 'brew install terraform' works, too.

- AWS credentials configured in `~/.aws/credentials` and/or `~/.aws/config`

### 1.2. Runtime Prerequisites (Infrastructure)

- HCA DSS (aka Blue Box): It is required to know the URL of the HumanCellAtlas
  Data Store webservice endpoint. See here for instructions:
  https://github.com/HumanCellAtlas/data-store/tree/master

The remaining infrastructure is managed internally with TerraForm.

### 1.3. Project configuration

Getting started without attempting to make contributions does not require AWS 
credentials. A subset of the test suite passes without configured AWS 
credentials. To validate your setup, we'll be running one of those tests at the
end.

1) Create a Python 3.6 virtual environment and activate it:
   
   ```
   cd azul
   python3.6 -m venv .venv
   source .venv/bin/activate
   ```
   
2) Install the development prerequisites:

   ```
   pip install -U setuptools==40.1.0 wheel==0.32.3
   pip install -r requirements.dev.txt
   ```

3) Activate the `dev` deployment: 
   
   ```
   cd deployments
   ln -snf dev .active
   cd ..
   ```

4) Load the environment:

   ```
   source environment
   ```
   
   Examine the output.

5) Run `make`. It should say `Looking good!` If one of the sanity checks fails,
   address the complaint and repeat. The various sanity checks are defined in
   `common.mk`.
   
6) Confirm proper configuration, run the following:
   
   ```
   make test
   ``` 

#### 1.3.1 For personal deployment (AWS credentials available)

Creating a personal deployment of Azul allows you test changes on a live system
in complete isolation from other users. If you intend to make contributions,
this is preferred. You will need IAM user credentials to the AWS account you
are deploying to.


1) Choose a name for your personal deployment. The name should be a short
   handle that is unique within the AWS account you are deploying to. It should
   also be informative enough to let others know whose deployment this is. We'll
   be using `foo` as an example here. The handle must only consist of alphabetic 
   characters.

2) Create a new directory for the configuration of your personal deployment: 

   ```
   cd deployments
   cp -r .example.local yourname.local
   ln -snf yourname.local .active
   cd ..
   ```  

3) Edit `deployments/.active/environment` and
   `deployments/.active/environment.local` according to the comments in there.


### 1.4. PyCharm configuration specifics

Running tests from PyCharm requires `environment` to be sourced. The easiest way
to do this is to install `envhook.py`, a helper script that injects the
environment variables from `environment` into the Python interpreter process
started from the project's virtual environment in `.venv`:   

   ```
   python scripts/envhook.py install
   ```

- Under *Settings* -> *Project—Interpreter* select the virtual environment created
above.
   * Under show all, select `.venv/bin/python` if not already selected.


- Set `src` & `test` folder as Source Root.
   * Right click the folder name and select `Mark Directory as` `->` `Source Root`
   
## 2. Deployment

### 2.1. Provisioning cloud infrastructure

Once you've successfully configured the project and your personal deployment,
it is time to provision the cloud infrastructure for your deployment. Running

```
make terraform
```

will display a plan and ask you to confirm it. Please consult the Terraform
documentation for details. You will need to run `make terraform` once to set up
your deployment and every time code changes define new cloud resources. The
resources are defined in `….tf.json` files which in turn are generated from
`….tf.json.template.py` files which are simple Python scripts containing the
desired JSON as Python dictionary and list literals and comprehensions.

### 2.2. Deploying lambda functions

Once the cloud infrastructure for your deployment has been provisioned, you can
deploy the project code into AWS Lambda. Running

```
make deploy
```

Will create or update AWS Lambda functions for each lambda defined in the
`lambdas` directory. It will also create or update an AWS API Gateway to proxy
the functions that act as web services. We call those functions *API lambdas*.

### 2.3. Provisioning stable API domain names

The HTTP endpoint offered by API Gateway have somewhat cryptic and hard to
remember domain names:

https://klm8yi31z7.execute-api.us-east-1.amazonaws.com/hannes/

Furthermore, the API ID at the beginning of the above URL is likely to change
when you accidentally delete the REST API and then recreate it. To provide
stable and user-friendly URLs for the API lambdas, we provision a *custom
domain name* object in API Gateway along with an ACM certificate and a CNAME
record in Route 53. Running `make terraform` again after `make deploy` will
detect the newly deployed API lambdas and create those resources for you. What
the user-friendly domain names look like depends on project configuration. The
default for HCA is currently

http://indexer.${AZUL_DEPLOYMENT_STAGE}.azul.data.humancellatlas.org/
http://service.${AZUL_DEPLOYMENT_STAGE}.azul.data.humancellatlas.org/

Note that while the native API Gateway URL refers to the stage in the URL path,
the stable URL mentions it in the domain name.

### 2.4. Subscribing to DSS

Once the Lambda functions have been deployed, and the custom domain names
provisioned, the indexer can be registered to receive notifications about new
bundles from the configured DSS instance. 

```
make subscribe
```

By default, the creation of that subscription is enabled (see
`AZUL_SUBSCRIBE_TO_DSS` in `environment`). All shared deployments in
`deployments/` inherit that default. If you don't want a personal deployment to
subscribe to the configured DSS instance you should set `AZUL_SUBSCRIBE_TO_DSS`
to 0. Subscription requires credentials to a service account that has the
required privileges to create another service account under which the
subscription is then made. This indirection exists to faciliate shared
deployments without having to share any one person's Google credentials. The
indexer service account must belong to a GCP project that is whitelisted in the
DSS instance to which the indexer is subscribed to. The credentials of the
indexer service account are stored in Amazon Secrets Manager.

### 2.5. Reindexing

The DSS instance used by a deployment is likely to contain existing bundles. To
index them run:

```
make reindex
```

### 2.6 Cancelling all ongoing (re)indexing operations

1) Go to the Simple Queue Service dashboard in the AWS Console. Then, find your
   target notify SQS queue (should be named azul-notify-…) and purge the queue.

2) Go to the Lambda dashboard in the AWS Console. Find and open the
   `azul-indexer-…-write` lambda. Then, disable the event binding to the
   document queue (usually named `azul-documents-…`). This is done by clicking
   on the `SQS` trigger in the *Designer* box, clicking on the *Enabled* switch
   of `azul-documents-…` in the newly appeared *SQS* box, then finally saving
   your settings.

3) Purge the remaining queues.

4) If azul-documents-… and azul-documents-…fifo isn't empty after 5 minutes,
   repeat steps 3.

5) Renable the event binding from step 2.

## 3. Running indexer or service locally

1) As usual, activate the virtual environment and `source environment` if you haven't
   done so already

2) `cd lambdas/indexer` or `cd lambdas/service` 

3) Run

   ```
   make local
   ````

4) You can now hit the app under `http://127.0.0.1:8000/`

   To hit the indexer (not the service) with multiple notification requests, run

   ```
   python scripts/reindex.py --workers=1 --sync --indexer-url http://127.0.0.1:8000/
   ```

   in a separate shell. The `--sync` argument causes the Chalice app to invoke
   the indexing code directly instead of queuing an SQS message to be consumed
   by the indexer worker Lambda function in AWS.

   Consider passing `--es-query` to restrict the set of bundles for which
   notifications are sent, especially if you are using a debugger.

   Instead of using `reindex.py`, you can speed things up by using `curl` to
   POST directly to the indexer endpoint. But you'd have to know the
   notification payload format (hint: see reindex.py). Note that the query
   member of the notification is currently not used by the indexer.

PyCharm recently added a feature that allows you to attach a debugger: From the
main menu choose *Run*, *Attach to local process* and select the `chalice`
process.

## 4. Troubleshooting

`make terraform` complains 

```
Initializing the backend...
Backend configuration changed!

Terraform has detected that the configuration specified for the backend
has changed. Terraform will now check for existing state in the backends.


Error inspecting states in the "s3" backend:
    NoSuchBucket: The specified bucket does not exist
```

… but the bucket does exist. Make sure
`deployments/.active/.terraform/terraform.tfstate` refers to the correct
bucket, the one configured in `AZUL_TERRAFORM_BACKEND_BUCKET`. If it
doesn't, you may have to remove that file or modify it to fix the bucket name.

## 5. Branch flow & development process

The section below describes the flow we want to get to eventually, not the one
we are currently using while this repository recovers from the aftermath of its
inception.

The declared goal here is a process that prevents diverging forks yet allows
each project to operate independently as far as release schedule, deployment
cadence, project management and issue tracking is concerned. The main
challenges are 1) preventing contention on a single `develop` or `master`
branch, 2) isolating project-specific changes from generic ones, 3) maintaining
a reasonably linear and clean history and 4) ensuring code reuse.

The [original repository](https://github.com/DataBiosphere/azul), also known as
*upstream*, should only contain generic functionality and infrastructure code.
Project-specific functionality should be maintained in separate
project-specific forks of that repository. The upstream repository will only
contain a `master` branch and the occasional PR branch.

Azul dynamically imports project-specific plugin modules from a special
location in the Python package hierarchy: `azul.projects`. The package
structure in upstream is

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
important difference. While a fork's `master` branch is an approximate mirror
of upstream's `master` and therefore also lacks content in `projects`, that
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
branch. The `master` branches in each fork should not be divergent for
sustained periods of time while the project-specific branches can and will be.

The reason why each fork maintains a copy of the `master` branch is that forks
generally need to have a place to test and evaluate generic features before
they are promoted upstream. If there wasn't a `master` branch in a fork, the
project-specific `develop` branch in that fork would inevitably conflate
project-specific changes with generic ones. It would be very hard to
selectively promote generic changes upstream, even if the generic changes were
separate commits. 

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
A' and B'. Later those changes are merged upstream via commit 4. Both the
rebase and the merge happen via a pull request, but the landing action will be
"Rebase and merge" for the first PR and "Create a merge commit" for the second.

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
lead for the forking project and reviewed by an upstream lead. Shortly after
the PR lands, the requesting lead should perform a fast-forward merge of the
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

### 5.1. Deployment branches

The code in the upstream repository should never be deployed anywhere because
it does not contain any concrete modules to be loaded at runtime. The code in a
fork, however, is typically active in a number of deployments. The specifics
should be left to each project but the rule of thumb should be that each
deployment corresponds to a separate branch in the fork. The `azul-hca` fork
has four deployments: development, integration, staging and production. The
development deployment, or `dev`, is done from the `develop` branch. Whenever a
commit is pushed to that branch, a continuous deployment script deploys the
code to AWS. The other deployment branches are named accordingly. Changes are
promoted between deployments via a merge. The merge is likely going to be a
fast-forward. A push to any of the deployment branches will trigger a CI/CD
build that performs the deployment. The promotion could be automatic and/or
gated on a condition, like tests passing.

## 6. Cheat sheets

### 6.1. Deploying to `dev`, `integration`, `staging` or `prod`

1) Change into the Azul project root directory: `cd azul`

2) Run `git checkout develop` (alternatively `integration`, `staging` or `prod`)

3) This cheat sheet may differ from branch to branch. Be sure to follow the
   cheat sheet in the README on the branch currently checked out.

4) Run `git status` and make sure that your working copy is clean and the
   branch is up-to-date.

5) Run `source environment`

6) Run `_select dev` (alternatively `integration`, `staging` or `prod`). Except
   for `dev` and `develop`, the branch name matches the name of the deployment.

7) To ensure a consistent and up-to-date set of dependencies, run

   ```
   deactivate; rm -rf .venv 
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -U setuptools==40.1.0 wheel==0.32.3
   pip install -r requirements.dev.txt
   ```
8) Run `python scripts/envhook.py install` if you use envhook.py

9) Run `make clean`

10) Run `make terraform`

11) Run `make deploy`

12) Invoke the health and version endpoints. Be sure to use the correct 
    deployment name:

    ```
    http https://indexer.dev.explore.data.humancellatlas.org/version
    http https://service.dev.explore.data.humancellatlas.org/version
    http https://indexer.dev.explore.data.humancellatlas.org/health
    http https://service.dev.explore.data.humancellatlas.org/health
    ```

13) Run `make tag` and the `git push …` invocation that it echoes 

14) Run `make subscribe`

15) Run `make reindex`

### 6.2 Promoting changes

For promoting `dev` to `integration` use the steps outlined below. For higher
promotions (`integration` to `staging`, or `staging` to `prod`) change the
source and target branches accordingly. You can and should perform steps 1
through 7 ahead of the actual promotion time.

1) Change into the Azul project root directory: `cd azul`

2) Checkout the target branch: `git checkout integration`

3) This cheat sheet may differ from branch to branch. Be sure to follow the
   cheat sheet in the README on the branch currently checked out.

4) Run `git status` and make sure that your working copy is clean and the
   branch is up-to-date.

5) Merge the source branch: `git merge develop` and resolve any conflicts.
   Conflict resolution should only be necessary if cherry-picks occured on the
   target branch.

6) The merge may have affected the README. Make sure you're looking at the
   right version.
   
7) To produce the list of changes for the DCP release notes, run 
   `git log --pretty=oneline --topo-order --abbrev-commit`. All non-merge
   commits from the top down to the commit labeled with the most recent
   `deployed/…` tag represent changes to be deployed and should be mentioned in
   the release notes.

8) Deploy, see section 6.1, starting at step 5). In step 6) use the deployment
   that matches the target branch

9) Run `git push origin`

## 7. Scale testing

Scale testing can be done with [Locust](https://locust.io/). Locust is a
development requirement so running it is straight-forward with your development
environment set up.

1. Make sure Locust is installed by running
   ```
   locust --version
   ```
   If it is not installed, do step 1.3 in this README.

1. To scale test the Azul web service on integration run
   ```
   locust -f scripts/locust/service.py
   ```

   If you want to test against a different stage use the `--host` option:
   ```
   locust -f scripts/locust/service.py --host https://service.dev.explore.data.humancellatlas.org
   ```

1. Navigate to `http://localhost:8090` in your browser to start a test run.

For more advanced usage see [the Locust documentation](https://docs.locust.io/en/stable/).


## 8. Continuous deployment and integration

We are currently in the process of migrating from manual deployments to
automated deployments performed on a project-specific Gitlab EC2 instance.
There is currently one such Gitlab instance for the `dev`, `integration` and
`staging` deployments. The `prod` instance is soon to follow.

The Gitlab instance is provisioned through Terraform but its resource
definitions reside in a separate *Terraform component*. A *Terraform component*
is a set of related resources. Each deployment has at least a main component
and zero or more subcomponents. The main component is identified by the empty
string, child components have a non-empty name. The `dev` component has a
subcomponent `dev.gitlab`. To terraform the main component of the `dev`
deployment, one selects the `dev` deployment and runs `make apply` from
`${azul_home}/terraform`. To deploy the `gitlab` component of the `dev`
deployment, one selects `dev.gitlab` and runs `make apply` from
`${azul_home}/terraform/gitlab`.

To access the web UI of the Gitlab instance for `dev`, visit
`https://gitlab.dev.explore.…/`, authenticating yourself with your GitHub
account. After attempting to log in for the first time, one of the
administrators will need to approve your access.

To have the Gitlab instance build a branch, one pushes that branch to the Azul
fork hosted on the Gitlab instance. The URL of the fork can be viewed by
visiting the GitLab web UI. One can only push via SSH and only a specific set
of public keys are allowed to push. These keys are configured in
[gitlab.tf.json.template.py](terraform/gitlab/gitlab.tf.json.template.py). A
change to that file—and this should be obvious by now—requires running `make
apply` in `${azul_home}/terraform/gitlab` while having `dev.gitlab` selected.

An Azul build on Gitlab runs the `test`, `terraform`, `deploy` and
`integration_test` Makefile targets, in that order. The target deployment for
feature branches is `sandbox`, the protected branches use their respective
deployments.

8.1. The Sandbox Deployment

There is only one such deployment and it should be used to validate feature
branches (one at a time) or to run experiments. This implies that access to the
sandbox must be coordinated externally e.g., via Slack. The build master owns
the sandbox deployment by default.

8.2. Security

Gitlab has AWS write permissions for the AWS services used by Azul and the
principle of least privilege is applied as much as IAM allows it. Some AWS
services support restricting the creation and deletion of resource by matching
on the name. For these services, Gitlab can only create, modify or write
resources whose name begins with `azul-*`. Other services, such as API Gateway
only support matching on resource IDs. This is unfortunate because API Gateway
allocates the ID. Since it therefore impossible to know the ID of an API before
creating it, Gitlab must be given write access to **all** API IDs. For details
refer to the `azul-gitlab` role and the policy of the same name, both defined
in [gitlab.tf.json.template.py](terraform/gitlab/gitlab.tf.json.template.py).

Gitlab does not have general write permissions to IAM, its write access is
limited to creating roles and attaching policies to them as long as the roles
and policies specify the `azul-gitlab` policy as a [permisions
boundary][1]. This means that code running
on the Gitlab instance can never escalate privileges beyond the boundary. This
mechanism is defined in the `azul-gitlab-iam` policy.

[1]: https://aws.amazon.com/blogs/security/delegate-permission-management-to-developers-using-iam-permissions-boundaries/

Code running on the Gitlab instance has access to credentials of a Google Cloud
service account that has read-only privileges to Google Cloud. This implies
that Gitlab cannot terraform Google Cloud resources. Fortunately, there are
only two such resources: 1) the service account that is used to subscribe Azul
to the DSS and 2) its credentials. That resource must be deployed manually once
before pushing a branch that would create a deployment for the first time or to
recreate it after it was destroyed:

```
cd terraform
make config 
terraform apply -target google_service_account.indexer \
                -target google_service_account_key.indexer
```

8.3. Networking

The networking details are documented in
[gitlab.tf.json.template.py](terraform/gitlab/gitlab.tf.json.template.py). The
Gitlab EC2 instance uses a VPC and is fronted by an Application Load Balancer
(ALB) and a Network Load Balancer (NLB). The ALB proxies HTTPS access to the
Gitlab web UI, the NLB provides SSH shell access and `git+ssh` access for
pushing to the project forks on the instance.

8.4. Storage

The Gitab EC2 instance is attached to an EBS volume that contains all of
Gitlab's data and configuration. That volume is not controlled by Terraform and
must be created manually once before terraforming the `gitlab` component.
The details can be found in
[gitlab.tf.json.template.py](terraform/gitlab/gitlab.tf.json.template.py).

8.5. Gitlab

The instance runs Gitlab CE running inside a rather elaborate concoction of
Docker containers. See
[gitlab.tf.json.template.py](terraform/gitlab/gitlab.tf.json.template.py) for
details.

8.6. Updating Gitlab

Modify the Docker image tags in
[gitlab.tf.json.template.py](terraform/gitlab/gitlab.tf.json.template.py) and
apply. The instance will be terminated (the EBS volume will survive) and a new
instance will be launched, with fresh containers from updated images. This
should be done periodically.

8.7. The Gitlab Build Environment

The `/mnt/gitlab/runner/config/etc` directory on the Gitlab EC2 instance is
mounted into the build container as `/etc/gitlab`. The Gitlab build for Azul
copies the files from the `azul` subdirectory of that directory into the Azul
project root. Secrets and other Gitab-specific settings should be specified in
`/mnt/gitlab/runner/config/etc/azul/environment.local` which will end up in
`${azul_home}/environment.local` where `source environment` will find and load
them. For secrets, we prefer this mechanism over specifying them as environment
variables under project settings on the Gitlab web UI. Only people with push
access can push code to intentionally or accidentally expose those variables,
push access is tied to shell access which is what one would normally need to
modify those files.
