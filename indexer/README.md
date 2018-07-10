# Azul - The HumanCellAtlas Portal Backend

This is the indexer that consumes events from the blue box and indexes them into Elasticsearch.

## Getting Started

### Development Preequisites

- Python 3.6 with virtualenv and pip
- Terraform (optional, to create new deployments): https://www.terraform.io/intro/getting-started/install.html
  On macOS with Homebrew installed, 'brew install terraform' works, too.
- AWS credentials configured in `~/.aws/credentials` and/or `~/.aws/config`

### Runtime Preequisites (Infrastructure)

- HCA DSS (aka Blue Box): It is required to know the URL of the HumanCellAtlas Data Store webservice endpoint. See
  here for instructions: https://github.com/HumanCellAtlas/data-store/tree/master

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

4) `AWS_CONFIG_FILE='~/.aws/config' AWS_SHARED_CREDENTIALS_FILE='~/.aws/config' chalice local`

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

### Deprecated: Elasticsearch (ES)

Create an Elasticsearch box on AWS. 
On the AWS console click "Services," then click "Elasticsearch Service." Then click "Create a new domain." Assign a domain name to the ES instance (eg: "dss-sapphire"). Choose your configuration for your requirements.
For the Access Policy, copy the contents of policies/elasticsearch-policy.json and edit the places with `<>`
To find your `<AWS-account-ID>`, click "Select a Template" and click "Allow or deny access to one or more AWS accounts or IAM users". Then a pop-up will appear with your "AWS account ID".
Take note of the Elasticsearch endpoint.

### Configure AWS and create a Virtual Environment
Install python3.

Create a virtual environment with `virtualenv -p python3 <envname>` and activate with `source <envname>/bin/activate`.

Install and configure the AWS CLI with your credentials
```
pip install awscli --upgrade
aws configure
```

### Chalice

Chalice is similar to Flask but is serverless and uses AWS Lambda.
```
pip install chalice
chalice new-project
```
When prompted for project name, input `<your-indexer-lambda-application-name>`, (e.g., dss-indigo).

Change the working directory to the newly created folder `<your-indexer-lambda-application-name>` (e.g., dss-indigo) and execute `chalice deploy`. Record the URL returned in the last line of stdout returned by this command - henceforth referred to as `<callback_url>`. This will create an AWS Lambda function called `dss-indigo` which will be updated using `chalice deploy`. Chalice automatically generated a folder `chalicelib/` and the files `rm app.py` and `rm requirements.txt`. Overwrite those by copying `app.py`, `requirements.txt` and `chalicelib/` from this repo and to the dss-indigo folder. Then execute

`pip install -r requirements.txt`

### Config File

`chalicelib/config.json` should contain the keys that you wish to add to the index documents. The structure of the config.json should mimic the metadata json file being looked at.

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

### Environment Variables
In order to add environmental variables to Chalice, the variables must be added to three locations.
Do not add protocols to any of the Endpoints. Make sure the ES_ENDPOINT does not have any trailing slashes.
BLUE_BOX_ENDPOINT looks like `dss.data.humancellatlas.org/v1`.

1) Edit Chalice  
open `.chalice/config.json`  
Replace the current file with the following, making sure to replace the <> with your information.  
```
{
  "version": "2.0",
  "app_name": "<your-indexer-lambda-application-name>",
  "stages": {
    "dev": {
      "api_gateway_stage": "api",
      "manage_iam_role":false,
      "iam_role_arn":"arn:aws:iam::<AWS-account-ID>:role/<your-indexer-lambda-application-name>-dev",
      "environment_variables": {
         "ES_ENDPOINT":"<your elasticsearch endpoint>",
         "BLUE_BOX_ENDPOINT":"<your blue box>",
         "AZUL_ES_INDEX":"<elasticsearch index to use>",
         "AZUL_INDEXER_NAME":"<your-indexer-lambda-application-name>",
         "HOME":"/tmp"
      }
    }
  }
}
```   

2) Edit .profile
open `~/.profile` and add the following items.

```
export ES_ENDPOINT=<your elasticsearch endpoint>
export BLUE_BOX_ENDPOINT=<your blue box>
export AZUL_ES_INDEX=<elasticsearch index to use>
export AZUL_INDEXER_NAME=<your-indexer-lambda-application-name>
export HOME=/tmp
```

run `. ~/.profile` to load the variables

3) Edit Lambda

Go to the AWS console, and then to your Lambda function and add the following environment variables:

```
ES_ENDPOINT  -->   <your elasticsearch endpoint>
BLUE_BOX_ENDPOINT   -->   <your blue box>
AZUL_ES_INDEX  -->  <elasticsearch index to use>
AZUL_INDEXER_NAME  -->  <your-indexer-lambda-application-name>
HOME --> /tmp
```

### Elasticsearch & Lambda

Given the current configuration, a deployment will result in errors when attempting to reach Elasticsearch. This is because Lambda is not configured to allow ES actions.

Open the AWS console and go to IAM. On the side menu bar, chose roles, then choose your lambda function, `<your-indexer-lambda-application-name>` and under "Policy name" click the drop down, then click on "Edit Policy". Add the policy found in lambda-policy.json under the `policies` folder, making sure to change the `Resource` value to the ARN of your elasticsearch box.

### Deploy Chalice

Enter your directory of your chalice function and `chalice deploy --no-autogen-policy`. Since we have created a policy in AWS we do not want chalice to automatically create a policy for us.

Your `<callback_url>` should be able to take post requests from the Blue Box and index the resulting files 
This is untested, but can take in a simulated curl request of the following format.
```
curl -H "Content-Type: application/json" -X POST -d '{ "query": { "query": { "bool": { "must": [ { "match": { "files.sample_json.donor.species": "Homo sapiens" } }, { "match": { "files.assay_json.single_cell.method": "Fluidigm C1" } }, { "match": { "files.sample_json.ncbi_biosample": "SAMN04303778" } } ] } } }, "subscription_id": "ba50df7b-5a97-4e87-b9ce-c0935a817f0b", "transaction_id": "ff6b7fa3-dc79-4a79-a313-296801de76b9", "match": { "bundle_version": "2017-08-03T041453.170646Z", "bundle_uuid": "4ce8a36d-51d6-4a3c-bae7-41a63699f877" } }' <callback_url> 
```

### Methods and Endpoints

|  Methods/Endpoints | Notes |
| ------------- | ------------- |
| `<callback_url>`/  | takes in a post request and indexes the bundle found in the request   |
| es_check() |  returns the ES info, good check to make sure Chalice can talk to ES  |

### Manual Loading

Download and expand import.tgz from Data-Bundle-Examples: https://github.com/HumanCellAtlas/data-bundle-examples/blob/master/import/import.tgz
Download the test/local-import.py file from this repo. Create an environmental variable `BUNDLE_PATH` that points to the import.tgz files. (Note: There are thousands of files in import.tgz, can specify parts of bundles to download: `import/geo/GSE67835` or `import/geo` or `import`)
Add environmental variable `ES_ENDPOINT` which points to your ES box or have a localhost running. Optionally, create the name of the ES index to add the files to with the environmental variable `AZUL_ES_INDEX` (default index is `test-import`)
Required to have a config.json (like the one in `chalicelib/config.json`)

Run `local-import.py`. Open Kibana to see your files appear. The

Note: Manual loading creates mappings for ES, has some list parsing capability, and if `key` in config.json does not exist, returns a value of "no `key`". (This functionality is not present in the Chalice function yet)

### Stress test
The test data can be populated under `test/data_generator` directory to an ElasticSearch instance by updating the ES URL and directory name in `make_fake_data`. 

To run the stress test, first update the `host` variable in `test_stress_indexer.py`, or pass it as a flag when running the test. 
The query by default matches all of the elements in elasticsearch to stress the system to the maximum, but that can be optionally changed `json` parameter in the `query_indexer` method. 

To run the test, use `locust -f test_stress_indexer.py --no-web -c 10 -r 2 -n 10` , where `-c` represents the number of concurrent users to simulate, `-r` the number of new users generated per second and `-n` the number of times this is run. You can optionally specify the total run time instead of the number of times by passing in `-t HHh:MMm:SSs` in place of `-n`. If you want to use a different host, you can pass the Elasticsearch URL by passing it in using the `-host <HOST_URL>` option

If `--no-web` is not generated, locust will create an UI on port `8089` where you can configure the parameters.


### Todo List

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

## Automated Deployment
You can use `make` to create a ElasticSearch service, chalice lambda and set everything up. The `Makefile` can take in `STAGE=<STAGE_VALUE>` as an input for different stages when deploying `chalice`. 

Before you run `make`, you will need to setup the prerequisites. 

### Prerequisities to run `make`
You will need to update the `config/elasticsearch-policy.json` file to enter your desired domain name, lambda name and the IP address. 
Next, you will need to update the `config/config.env` file and apply all the values. The ```
BB_ENDPOINT=dss.staging.data.humancellatlas.org/v1
AZUL_ES_INDEX=test-import
```
values are pre-filled by default. 

If you've an existing ElasticSearch service instance, you should fill in the following values:
```
ES_DOMAIN_NAME=
ES_ENDPOINT=
ES_ARN=
```

If you don't supply the `ES_DOMAIN_NAME` value, the `make` file will automatically create a new ElasticSearch instance. 

The `config/elasticsearch-config.json` and `config/ebs-config.json` files can be modified to change the shape of the ElasticSearch service instance and ElasticSearch Beanstalk. 

Once `make` finishes successfully, you can find all the required environment variables' values in `values_generated.txt`

You will need to export these values to your `~/.profile`
