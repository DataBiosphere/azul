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

### 1.1. Development Preequisites

- Python 3.6 with virtualenv and pip

- Terraform (optional, to create new deployments):
  https://www.terraform.io/intro/getting-started/install.html On macOS with
  Homebrew installed, 'brew install terraform' works, too.

- AWS credentials configured in `~/.aws/credentials` and/or `~/.aws/config`

### 1.2. Runtime Preequisites (Infrastructure)

- HCA DSS (aka Blue Box): It is required to know the URL of the HumanCellAtlas
  Data Store webservice endpoint. See here for instructions:
  https://github.com/HumanCellAtlas/data-store/tree/master

The remaining infrastructure is managed internally with TerraForm.

### 1.3. Project configuration

1) Create a Python 3.6 virtualenv and activate it, for example 
   
   ```
   virtualenv .venv
   source .venv/bin/activate
   ```

2) Choose a name for your personal deployment. The name should be a short
   handle that is unique within the AWS account you are deploying to. It should
   also be informative enough to let others know whose deployment this is. We'll
   be using `foo` as an example here.

3) Create a new directory for the configuration of your personal deployment, as 
   a copy of the `dev` deployment: 

   ```
   cd deployments
   cp -r dev foo.local
   ln -snf foo.local .active
   cd ..
   ```  

4) In `deployments/.active/environment` change `AZUL_DEPLOYMENT_STAGE` to the
   name of your deployment. In this example, we'd be setting it to `foo`. If you
   don't have a Google service account in a GCP project that's white-listed for
   subscriptions in DSS, set AZUL_SUBSCRIBE_TO_DSS to 0.

5) In the project root, create `deployments/.active/environment.local`
   containing

   ```
   export AWS_PROFILE=...
   export AWS_DEFAULT_REGION=...
   ```

6) Load the environment:

   ```
   source environment
   ```
   
   Scrutinize the output. We copied the `dev` deployment configuration but then
   changed `AZUL_DEPLOYMENT_STAGE` to ensure that we're not actually
   touching anything in `dev`.

7) Install the development prerequisites
   
   ```
   pip install -r requirements.dev.txt
   ```

8) Run `make`. It should say `Looking good!` If one of the sanity checks fails,
   address the complaint and repeat. The various sanity checks are defined in
   `common.mk`.

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

By default, the provisioning of that subscription is disabled (see
`AZUL_SUBSCRIBE_TO_DSS` in `environment`) but the shared deployments in
`deployments/` have it enabled. If you want to subscribe your personal
deployment to DSS you can set `AZUL_SUBSCRIBE_TO_DSS` or run
`scripts/subscribe.py` manually.

### 2.5. Reindexing

The DSS instance used by a deployment is likely to contain existing bundles. To
index them run:

`make reindex`

## 3. Running indexer locally

1) As usual, activate the virtualenv and `source environment` if you haven't
   done so already

2) `cd lambdas/indexer`

3) Run

   ```
   AWS_CONFIG_FILE='~/.aws/config' AWS_SHARED_CREDENTIALS_FILE='~/.aws/credentials' make local`
   ````

4) In another shell, run

   ```
   python scripts/reindex.py --workers=1 --sync --indexer-url http://127.0.0.1:8000/`
   ```

The `--sync` argument causes the Chalice app to invoke the indexing code
directly instead of queuing an SQS message to be consumed by the indexer worker
Lambda function in AWS.

PyCharm recently added a feature that allows you to attach a debugger: From the
main menu choose *Run*, *Attach to local process* and select the `chalice`
process.

The `make install` step needs to be repeated after every code change. This
requirement should go away soon.

Consider passing `--es-query` to restrict the set of bundles for which
notifications are sent, especially if you are using a debugger.

Instead of using `reindex.py`, you can speed things up by using `curl` to POST
directly to the indexer endpoint. But you'd have to know the notification
payload format (hint: see reindex.py). Note that the query member of the
notification is currently not used by the indexer.

Overriding `AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE` for the `chalice
local` step is necessary because `config.json` sets `HOME` to `/tmp`.

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
bucket, the one configured in `AZUL_TERRAFORM_BACKEND_BUCKET_TEMPLATE`. If it
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

## 6. Indexer

### 6.1. Config File

[This section is out of date.]

`chalicelib/config.json` should contain the keys that you wish to add to the
index documents. The structure of the config.json should mimic the metadata
json file being looked at.

For example, the following metadata for assay.json:

```
{
  "rna": {
    "primer": "random"
  },
  "seq": {
    "machine": "Illumina HiSeq 2000",
    "molecule": "total RNA",
    "paired_ends": "no",
    "prep": "TruSeq"
  },
  "single_cell": {
    "method": "mouth pipette"
  },
  "sra_experiment": "SRX129997",
  "sra_run": [
    "SRR445718"
  ],
  "files": [
    {
      "name": "SRR445718_1.fastq.gz",
      "format": ".fastq.gz",
      "type": "reads",
      "lane": 1
    }
  ]
}
```

and this cell.json


```
{
  "type": "oocyte",
  "ontology": "CL_0000023",
  "id": "oocyte #1"
}
```

Could have a config like such:

```
{
  "assay.json": [
    {
      "rna": [
        "primer"
      ]
    },
    {
      "single_cell": [
        "method"
      ]
    },
    "sra_experiment",
    {
      "files":[
        "format"
      ]
    }
  ],
  "cell.json":[
    "type",
    "ontology",
    "id"
  ]
 }
```

***NOTE***: The config should be rooted under a version of the metadata being received.

In Elasticsearch, the fields for the File Indexer will be

```
assay,json|rna|primer
assay,json|single_cell|method
assay,json|sra_experiment
assay,json|files|format
cell,json|type
cell,json|ontology
cell,json|id
```

Notice the commas(,) where there were previously periods(.). Also, the pipe (|) is used as the separator between levels in the config.

#### Adding Mappings

Given a config:

```
{
  "cell.json":[
    "type",
    "ontology",
    "id"
  ]
 }
```

### 6.2. Manual Loading

Download and expand import.tgz from Data-Bundle-Examples:
https://github.com/HumanCellAtlas/data-bundle-examples/blob/master/import/import
.tgz Download the test/indexer/local-import.py file from this repo. Create an
environmental variable `BUNDLE_PATH` that points to the import.tgz files.
(Note: There are thousands of files in import.tgz, can specify parts of bundles
to download: `import/geo/GSE67835` or `import/geo` or `import`) Add
environmental variable `ES_ENDPOINT` which points to your ES box or have a
localhost running. Optionally, create the name of the ES index to add the files
to with the environmental variable `AZUL_ES_INDEX` (default index is
`test-import`) Required to have a config.json (like the one in
`chalicelib/config.json`)

Run `local-import.py`. Open Kibana to see your files appear. The

Note: Manual loading creates mappings for ES, has some list parsing capability,
and if `key` in config.json does not exist, returns a value of "no `key`".
(This functionality is not present in the Chalice function yet)

### 6.3. Stress test

The test data can be populated under `test/data_generator` directory to an
ElasticSearch instance by updating the ES URL and directory name in
`make_fake_data`.

To run the stress test, first update the `host` variable in
`test_stress_indexer.py`, or pass it as a flag when running the test. The query
by default matches all of the elements in elasticsearch to stress the system to
the maximum, but that can be optionally changed `json` parameter in the
`query_indexer` method.

To run the test, use `locust -f test_stress_indexer.py --no-web -c 10 -r 2 -n
10` , where `-c` represents the number of concurrent users to simulate, `-r`
the number of new users generated per second and `-n` the number of times this
is run. You can optionally specify the total run time instead of the number of
times by passing in `-t HHh:MMm:SSs` in place of `-n`. If you want to use a
different host, you can pass the Elasticsearch URL by passing it in using the
`-host <HOST_URL>` option

If `--no-web` is not generated, locust will create an UI on port `8089` where
you can configure the parameters.


### 6.4. Todo List

[This section needs to be converted to tickets and then removed]

* how to setup Kibana for security group reasons
* how to run find-golden-tickets.py
* improve mappings to Chalice
* list handling in json files
* cron deduplication
* capibility to download files that are not json
* multiple version handling (per file version or per file?)
* Unit testing: Flask mock up of the Blue Box endpoints
    * We need something that will generate POSTS to the lambda, such as a shell script.
    * Flask has endpoints for looking up bundles, and get a particular manifest.
    * Assume  bundles uuid always exist. generate a request to download anything indexable ? 
* Improve debugging (config for turning on/off debug)


## 7. Service

### 7.1. General Overview
The web app is subdivided into different flask blueprints, each filling out a particular function. The current blueprints are:

* action
  * Responsible for returning the status of jobs running on Consonance.
* billing
  * Responsible for returning the prices of storage and computing cost. It reads from a billing index in ElasticSearch.
* webservice
  * The backend that powers the Boardwalk portal. Queries ElasticSearch to serve an API that allows to apply filters and do faceting on entries within ElasticSearch.
  
### 7.2. On the Webservice

The responseobjects module is responsible for handling the faceting and API response creation. Within this module, `elastic_request_builder` is responsible for taking in the parameters passed in through the `HTTP` request and creating a query to ElasticSearch. Then, `api_response` is responsible for parsing the data from ElasticSearch and creating the API response.

There are currently five working endpoints:
<ul>
<li>"<code>/repository/files/</code>" returns the index search results along with a count of the terms available for the facets.</li>
<li>"<code>/repository/files/summary</code>" returns a summary of the current data stored.</li>
<li>"<code>/repository/files/export</code>" returns a manifest file with the filters provided.</li>
<li>"<code>/repository/files/order</code>" returns the desired order for the facets.</li>
<li>"<code>/keywords</code>" returns a list of search results for some search query.</li>
</ul>

<h4>Webservice Endpoints</h4>

***/repository/files***<br>
Currently there are 6 parameters supported. They are as follows:<br>

|Parameter|Description|Data Type|Example|
|--- |--- |--- |--- |
|filters|Specifies which filters to use to return only the files with the matching criteria. Supplied as a string with the format: {"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}|String|http://ucsc-cgp.org/api/v1/repository/files/?filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D This will return only those files who have a file format of type "bam"|
|from|Specifies the start index. Defaults to 1 if not specified:|Integer|http://ucsc-cgp.org/api/v1/repository/files/?from=26  This will return the next 25 results starting at index 26|
|size|Specifies how many hits to return. Defaults to 10|Integer|http://ucsc-cgp.org/api/v1/repository/files/?size=50  This will return 50 hits starting from 1|
|sort|Specifies the field under which the list of hits should be sorted. Defaults to "center_name"|String|http://ucsc-cgp.org/api/v1/repository/files/?sort=donor  This will return the hits sorted by the "donor" field.|
|order|Specifies the order in which the hits should be sorted; two options, "asc" and "desc". Defaults to "desc".|String|http://ucsc-cgp.org/api/v1/repository/files/?order=desc  This will return the hits in descending order.|

<br>


***/repository/files/export***<br>

|Parameter|Description|Data Type|Example|
|--- |--- |--- |--- |
|filters|Specifies which filters to use to return only the manifest with of the files that matching the criteria. Supplied as a string with the format:`{"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}`|String|http://ucsc-cgp.org/api/v1/repository/files/export?filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D This will return a manifest with only those files who have a file format of type "bam"|


<br>

***/repository/files/summary***<br>

|Parameter|Description|Data Type|Example|
|--- |--- |--- |--- |
|filters|Specifies which filters to use to return only the summary with the matching criteria. Supplied as a string with the format:`{"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}`|String|http://ucsc-cgp.org/api/v1/repository/files/summary?filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D This will return a manifest with only those files who have a file format of type "bam"|

<br>

***/keywords***<br>

|Parameter|Description|Data Type|Example|
|--- |--- |--- |--- |
|type|Specifies which type of format to return (file or file-donor). Supplied as a string. Defaults to 'file'.|String|http://ucsc-cgp.org/api/v1/keywords?type=file&q=8f1 This will return files based on the search query 8f1.|
|field|Specifies which field to perform the search on. Defaults to 'fileId'.|String|http://ucsc-cgp.org/api/v1/keywords?type=file&q=UCSC&field=centerName would search for files with center name `UCSC` |
|filters|Specifies which filters to use to return only the search results with the matching criteria. Supplied as a string with the format:`{"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}`|String|http://ucsc-cgp.org/api/v1/keywords?type=file&q=8f1&filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D This will return only those files who have a file format of type "bam" for the query 8f1 .|
|from|Specifies the start index. Defaults to 1 if not specified:|Integer|http://ucsc-cgp.org/api/v1/keywords?type=file&q=8f1&from=26 This will return the search results from result 26 onwards|
|size|Specifies how many hits to return. Defaults to 5|Integer|http://ucsc-cgp.org/api/v1/keywords?type=file&q=8f1&size=5 This will return at most 5 hits.|
|q|Specifies the query for search|String|http://ucsc-cgp.org/api/v1/keywords?type=file&q=8f1&size=5 This will return at most 5 hits for the query 8f1.|
