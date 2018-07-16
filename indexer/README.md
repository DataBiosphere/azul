## Getting Started

### Development Preequisites

- Python 3.6 with virtualenv and pip

- Terraform (optional, to create new deployments):
  https://www.terraform.io/intro/getting-started/install.html On macOS with
  Homebrew installed, 'brew install terraform' works, too.

- AWS credentials configured in `~/.aws/credentials` and/or `~/.aws/config`

### Runtime Preequisites (Infrastructure)

- HCA DSS (aka Blue Box): It is required to know the URL of the HumanCellAtlas
  Data Store webservice endpoint. See here for instructions:
  https://github.com/HumanCellAtlas/data-store/tree/master

The remaining infrastructure is managed internally with TerraForm.

### Project configuration

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
   name of your deployment. In this example, we'd be setting it to `foo`.

5) In the project root, create `environment.local` containing 

   ```
   export AWS_PROFILE=...
   export AWS_DEFAULT_REGION=...
   ```
   
   Alternatively, you could use `deployments/.active/environment.local` but if 
   these two variables are the same in all deployments it makes sense to 
   set them globally, for all deployments.

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


### Running indexer locally

1) As usual, activate the virtualenv and `source environment` if you haven't
   done so already

2) `cd lambdas/indexer`

3) `make install`

4) Run
   
   ```
   AWS_CONFIG_FILE='~/.aws/config' AWS_SHARED_CREDENTIALS_FILE='~/.aws/config' chalice local`
   ````

5) In another shell, run

   ```
   PYTHONPATH=. python scripts/reindex.py --workers=1 --sync --indexer-url http://127.0.0.1:8000/`
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


### Config File

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

### Manual Loading

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

### Stress test

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


### Todo List

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
