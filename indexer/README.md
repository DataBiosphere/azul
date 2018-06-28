# dss-azul-indexer

This is the indexer that consumes events from the blue box and indexes them into Elasticsearch.

## Installation

### Before Getting Started

Make sure to write down: 
- Your blue box endpoint. See here for instructions: https://github.com/HumanCellAtlas/data-store/tree/master
- Your AWS Access Keys and AWS Secret Keys

### 1. Creating an Elasticsearch Domain
 
1. First, create an Elasticsearch Domain on AWS. You can do this in the AWS Console by clicking "Services" in the top toolbar, 
and then clicking "Elasticsearch Service".
2. Once you are in the Amazon Elasticsearch Service dashboard. Click "Create a new domain".
3. Under the Define Domain step, give a domain name to new the ES Domain
and use Elasticsearch version 5.5 and click next.
4. Under *Configure Cluster* step, unless you need other configurations for your domain (*Storage, Node, Snapshot, etc*), skip to the next step
5. In the *Set Up Access* step, set network configuration to *Public Access*.
6. Copy the contents of `policies/elasticsearch-policy.json` file in the cgp-dss-azul-indexer repository
7. Then, paste those contents into the textbox under *Access policy* 
8. Edit the following values in the policy with your own values:
    - `<your-es-domain-name>` is the name of the elasticsearch domain
    - `<your-indexer-lambda-domain-name>` is the name that you will give the lambda project.
    - `<AWS-account-ID>` is not your IAM User name, but a numeric id representing your aws account. You can find your AWS account ID by clicking "Select a Template" dropdown box. 
    Then, click "Allow or deny access to one or more AWS accounts or IAM users". Then, a pop-up will appear with your "AWS account ID".
    - `<your-ip-address>` is your ip address.
9. Record the values above for later use. They will be need to setup later components.
10. Confirm your policy settings, review what your configured, and finalize setting up the Elasticsearch domain. 
11. You should be in the Amazon Elasticsearch Service dashboard. Click on the name of the newly created Elasticsearch domain
    - record your ARN Domain. Knowing the ARN Domain will help you configure with configuration later.

### 2. Configure AWS and create a Virtual Environment

1. Install python 3.6
2. Create a virtual environment with `virtualenv -p python3.6 <envname>` 
3. Activate with `source <envname>/bin/activate`.
4. Install and configure the AWS CLI with your credentials (AWS Access Keys and AWS Secret Keys)

```
pip install awscli --upgrade
aws configure
```

### 3. Setup Chalice

1. Install Chalice with the following command:
    ```
    pip install chalice
    chalice new-project
    ```
2. When prompted for project name, input `<your-indexer-lambda-application-name>`.
3. Change the working directory to the newly created folder `<your-indexer-lambda-application-name>`.
4. Execute `chalice deploy`. **Note:** If you are planning to deploy a different staging instance add the `--staging <stage name>` option.
5. Record the URL returned in the last line of stdout returned by this command - henceforth referred to as `<azul-indexer-url>`.
This will create an AWS Lambda function with the name of your application name which will be updated using `chalice deploy`.
6. Chalice will automatically generate a folder with the name of your application name. The folder will contain the files `app.py` and `requirements.txt`
and a `.chalice` folder containing a `config.json` file. Overwrite those by copying `app.py`, `requirements.txt` and `chalicelib/` from this repo and the generated folder.
7. Then, execute `pip install -r requirements.txt`.
8. Open `.chalice/config.json`, remove any existing text in the file and copy the text below onto `config.json`.
    ```json
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
             "ES_INDEX":"<elasticsearch index to use>",
             "INDEXER_NAME":"<your-indexer-lambda-application-name>",
             "HOME":"/tmp"
          }
        }
      }
    }
    ```
9. Then, replace the following values in the config.json with your own values. 
    - `<your-indexer-lambda-application-name>` is the name that you will give the lambda project.
    - `<AWS-account-ID>` is one of the values in the access policy of your Elasticsearch. An numerical id of your AWS Account. 
    - `<your blue box>` is the endpoint of the DSS Blue Box.
    - `<your elasticsearch endpoint>` is url you will use to access Azul's Elasticsearch Domain. This url can be found by returning to your Elasticsearch Service Dashboard,
    clicking the link of your Elasticsearch Domain. The url should be labled as **Endpoint** in the Overview Tab.
10. Next, like the config.json. Open `~/.profile`. If that file doesn't exist, create one and append the text below. 
    ```
    export ES_ENDPOINT=<your elasticsearch endpoint>
    export BLUE_BOX_ENDPOINT=<your blue box>
    export ES_INDEX=<elasticsearch index to use>
    export INDEXER_NAME=<your-indexer-lambda-application-name>
    export HOME=/tmp
    ```
11. Then, replace the `<>` values in the `.profile`. They are the same values used in the `config.json` from the previous steps.
11. Run `. ~/.profile` to load the variables

### 4. Modifying IAM Access Policies

1. In the AWS Console, click Services in the top toolbar, and click IAM in the Security, Identity & Compliance subsection.
2. On the side menu bar, click **Roles**, then search for your lambda function, `<your-indexer-lambda-application-name>`. Once you find it, click on it.
3. Find a table in the Permissions tab under the blue Attack Policy button. You should see for lambda name in the table. Click on the caret on the right side of the lambda name. Then, click on "Edit Policy".
4. Add the policy found in `policies/elasticsearch-policy.json` file in the cgp-dss-azul-indexer repository (Make sure to change the `Resource` value to the ARN of your elasticsearch box)

### 5. Deploying Indexer Lambda

1. In the directory of your chalice function, run `chalice deploy --no-autogen-policy`. Since we have created a policy in AWS we do not want chalice to automatically create a policy for us.

## Methods and Endpoints

|  Methods/Endpoints | Notes |
| ------------- | ------------- |
| `<callback_url>`/  | takes in a post request and indexes the bundle found in the request   |
| es_check() |  returns the ES info, good check to make sure Chalice can talk to ES  |


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
