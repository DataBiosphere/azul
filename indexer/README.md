# dss-azul-indexer

This is the indexer that consumes events from the blue box and indexes them into Elasticsearch.

## Getting Started

### Blue Box 

It is required to know the blue box endpoint.

### Elasticsearch (ES)

Create an Elasticsearch box on AWS. Take note of the endpoint.

### Create a Virtualenv and configure AWS
It is recommended to have python3 and create a virtual environment with `virtualenv -p python3 envname` and activate with `source envname/bin/activate`.

Install and configure the AWS CLI with your information
```
pip install awscli --upgrade --user
aws configure
```

### Chalice
```
pip install chalice
chalice new-project
```
When prompted for project name, input `azul_indexer`.

`chalice deploy`. Record the url returned in the last line of stdout returned by this command - henceforth referred to as `<callback_url>`. 

This will create an AWS Lambda function called `azul_indexer` which will be updated using `chalice deploy`.

`cd azul_indexer` and then `rm app.py` and `rm requirements.txt`
add `app.py`, `requirements.txt` and `chalicelib/config.json` to the azul_indexer folder.


### Environmental Variables
In order to add environmental variables to Chalice, the variable must be added to three locations.

1) Edit Chalice  
open `.chalice/config.json`  
Replace the current file with the following, making sure to replace the <> with your information.  
```
{
  "version": "2.0",
  "app_name": "azul_indexer",
  "stages": {
    "dev": {
      "api_gateway_stage": "api",
      "manage_iam_role":false,
      "iam_role_arn":"arn:aws:iam::<your arn>:role/azul_indexer-dev",
      "environment_variables": {
         "ES_ENDPOINT":<your elasticsearch endpoint>,
         "BLUE_BOXENDPOINT":<your blue box>,
         "INDEXER_ENDPOINT":<your <callback_url>>
      }
    }
  }
}
```   
ARN: your ARN is found on the AWS console, under Lambda. Click on the `functions` on the left menu bar. Click on `azul_indexer-dev`. Your ARN is found on the top right corner of the console.

2) Edit .profile
open `~/.profile` and add the following items.

```
export ES_ENDPOINT=<your elasticsearch endpoint>
export BLUE_BOX_ENDPOINT=<your blue box>
export INDEXER_ENDPOINT=<your <callback_url>>
```

run `. ~/.profile` to load the variables

3) Edit Lambda

Go to the AWS console, and then to your Lambda function and add the following environmental variables:

```
ES_ENDPOINT  -->   your elasticsearch endpoint
BLUE_BOX_ENDPOINT   -->   your blue box
INDEXER_ENDPOINT    -->   your <callback_url>
```

### Elasticsearch & Lambda

Given the current configuration, a deployment will result in errors when attempting to reach Elasticsearch. This is because Lambda is not configured to allow ES actions.

Open the AWS console and go to IAM. On the side menu bar, chose roles, then choose your lambda function, `azul_indexer` and click on `attach policy` add the policy found in policy-template.json, making sure to change the `Resource` value to the ARN of your elasticsearch box.

### Deploy Chalice

Enter your directory of your chalice function and `deploy chalice --no-autogen-policy`. Since we have created a policy in AWS we do not want chalice to automatically create a policy for us.

Your `<callback_url>` should be able to take post requests from the Blue Box and index the resulting files 
This is untested, but can take in a simulated curl request of the following format.
```
curl -H "Content-Type: application/json" -X POST -d '{ "query": { "query": { "bool": { "must": [ { "match": { "files.sample_json.donor.species": "Homo sapiens" } }, { "match": { "files.assay_json.single_cell.method": "Fluidigm C1" } }, { "match": { "files.sample_json.ncbi_biosample": "SAMN04303778" } } ] } } }, "subscription_id": "ba50df7b-5a97-4e87-b9ce-c0935a817f0b", "transaction_id": "ff6b7fa3-dc79-4a79-a313-296801de76b9", "match": { "bundle_version": "2017-08-03T041453.170646Z", "bundle_uuid": "4ce8a36d-51d6-4a3c-bae7-41a63699f877" } }' <callback_url> 
```

### Endpoints

|  Endpoints | Notes |
| ------------- | ------------- |
| `<callback_url>`/  | takes in a post request and indexes the bundle found in the request   |
| `<callback_url>`/escheck  |  returns the ES info, good check to make sure Chalice can talk to ES  |
| `<callback_url>`/bundle/{bundle_uuid}  |  returns the uuids of the contents of the bundle (given by the uuid), separated by json and not json files  |
| `<callback_url>`/file/{file_uuid}  |  returns the contents of the file specified by the uuid   |
| `<callback_url>`/write/{bundle_uuid}  |  does the bulk of the work, takes a bundle_uuid and indexes the entire bundle and adds to ES   |
| `<callback_url>`/cron  |  this function is called daily. Sends a match_all request to the Blue Box and then indexes all bundles  |

### Manual Loading

Download and expand import.tgz from Data-Bundle-Examples: https://github.com/HumanCellAtlas/data-bundle-examples/blob/master/import/import.tgz
Download the test/local-import.py file from this repo. Create an environmental variable `BUNDLE_PATH` that points to the import.tgz files. (Note: There are thousands of files in import.tgz, can specify parts of bundles to download: `import/geo/GSE67835` or `import/geo` or `import`)
Add environmental variable `ES_ENDPOINT` which points to your ES box or have a localhost running. Optionally, create the name of the ES index to add the files to with the environmental variable `ES_INDEX` (default index is `test-import`)
Required to have a config.json (like the one in `chalicelib/config.json`)

Run `local-import.py`. Open Kibana to see your files appear. The

Note: Manual loading creates mappings for ES, has some list parsing capability, and if `key` in config.json does not exist, returns a value of "no `key`". (This functionality is not present in the Chalice function yet)

