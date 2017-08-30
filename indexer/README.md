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

Go to the AWS Lambda function and add the following environmental variables:

```
ES_ENDPOINT  -->   your elasticsearch endpoint
BLUE_BOX_ENDPOINT   -->   your blue box
INDEXER_ENDPOINT    -->   your <callback_url>
```

### Elasticsearch & Lambda

Given the current configuration, a deployment will result in errors when attempting to reach Elasticsearch. This is because Lambda is not configured to allow ES actions.
