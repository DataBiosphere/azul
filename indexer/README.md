# dss-azul-indexer

This is the indexer that consumes events from the HCA DSS and indexes them into Elasticsearch.

## Installation

### Before Getting Started

Make sure to write down: 
- Your HCA DSS endpoint. See here for instructions: https://github.com/HumanCellAtlas/data-store/tree/master
- The AWS Access Key and AWS Secret Key to be used

### 1. Creating an Elasticsearch Domain
 
1. First, create an Elasticsearch Domain on AWS. You can do this in the AWS Console by clicking "Services" in the top toolbar,
and then clicking "Elasticsearch Service".
1. Once you are in the Amazon Elasticsearch Service dashboard. Click "Create a new domain".
1. Under the Define Domain step, give a domain name to new the ES Domain
and use Elasticsearch version 5.5 and click next.
1. Under *Configure Cluster* step, unless you need other configurations for your domain (*Storage, Node, Snapshot, etc*), skip to the next step
1. In the *Set Up Access* step, set network configuration to *Public Access*.
1. Copy the contents of `policies/elasticsearch-policy.json` file in the cgp-dss-azul-indexer repository
1. Then, paste those contents into the textbox under *Access policy*
1. Edit the following values in the policy with your own values:
    - `<your-es-domain-name>` is the name of the elasticsearch domain
    - `<your-indexer-lambda-domain-name>` is the name that you will give the Lambda project.
    - `<AWS-account-ID>` is not your IAM User name, but a numeric id representing your AWS account. You can find your AWS account ID by clicking "Select a Template" dropdown box. 
    Then, click "Allow or deny access to one or more AWS accounts or IAM users". Then, a pop-up will appear with your "AWS account ID".
    - `<your-ip-address>` is your ip address.
1. Record the values above for later use. They will be need to setup later components.
1. Confirm your policy settings, review what you configured, and finalize setting up the Elasticsearch domain.
1. You should be in the Amazon Elasticsearch Service dashboard. Click on the name of the newly created Elasticsearch domain
    - record your ARN Domain. Knowing the ARN Domain will help you configure with configuration later.

### 2. Configure AWS and create a Virtual Environment

1. Install python 3.6.
1. Create a virtual environment with
   ```
   virtualenv -p python3.6 <envname>
   ```
1. Activate with
   ```
   source <envname>/bin/activate
   ```
1. Install and configure the AWS CLI with credentials to be used (AWS Access Keys and AWS Secret Keys)

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
1. When prompted for project name, input `<your-indexer-lambda-application-name>`.
1. Change the working directory to the newly created folder `<your-indexer-Lambda-application-name>`.
1. Execute `chalice deploy`. **Note:** If you are deploying an AWS API Gateway stage other than the default (dev)
add the `--stage <stage name>` option.
1. Record the URL returned in the last line of stdout returned by this command - henceforth referred to as `<azul-indexer-url>`.
This will create an AWS Lambda function with the name of your application name which will be updated using `chalice deploy`.
1. Chalice will automatically generate a folder with the name of your application name. The folder will contain the files `app.py` and `requirements.txt`
and a `.chalice` folder containing a `config.json` file. Overwrite those by copying `app.py`, `requirements.txt` and `chalicelib/` from this repo and the generated folder.
1. Then, execute `pip install -r requirements.txt`.
1. Open `.chalice/config.json`, remove any existing text in the file and copy the text below into `config.json`.
    ```json
    {
      "version": "2.0",
      "app_name": "<your-indexer-lambda-application-name>",
      "stages": {
        "<stage name>": {
          "api_gateway_stage": "<stage_name>",
          "manage_iam_role":false,
          "iam_role_arn":"arn:aws:iam::<AWS-account-ID>:role/<your-indexer-lambda-application-name>-dev",
          "environment_variables": {
             "ES_ENDPOINT":"<your elasticsearch endpoint>",
             "BLUE_BOX_ENDPOINT":"<your DSS URL>",
             "ES_INDEX":"<elasticsearch domain endpoint>",
             "INDEXER_NAME":"<your-indexer-lambda-application-name>",
             "HOME":"/tmp"
          }
        }
      }
    }
    ```
1. Then, replace the following values in the config.json with your own values.
    - `<stage_name>` is the intended stage name of the Lambda
    - `<your-indexer-lambda-application-name>` is the name that you will give the Lambda project.
    - `<AWS-account-ID>` is one of the values in the access policy of your Elasticsearch. An numerical id of your AWS Account. 
    - `<your blue box>` is the endpoint of the HCA DSS.
    - `<your elasticsearch endpoint>` is url you will use to access Azul's Elasticsearch Domain. This url can be found by returning to your Elasticsearch Service Dashboard,
    clicking the link of your Elasticsearch Domain. The url should be labled as **Endpoint** in the Overview Tab.

### 4. Modifying IAM Access Policies

1. In the AWS Console, click Services in the top toolbar, and click IAM in the Security, Identity & Compliance subsection.
1. On the side menu bar, click **Roles**, then search for your Lambda function, `<your-indexer-lambda-application-name>`. Once you find it, click on it.
1. Find a table in the Permissions tab under the blue Attach Policy button. You should see for Lambda name in the table. Click on the caret on the right side of the Lambda name. Then, click on "Edit Policy".
1. Add the policy found in `policies/elasticsearch-policy.json` file in the cgp-dss-azul-indexer repository (Make sure to change the `Resource` value to the ARN of your elasticsearch box).

### 5. Deploying Indexer Lambda

1. In the directory of your chalice function, run
   ```
   chalice deploy --no-autogen-policy
   ```
   **Note:** If you are deploying an AWS API Gateway stage other than the default (dev)
add the `--stage <stage name>` option. Since we have created a policy in AWS we do not want chalice to automatically create a policy for us.

### 6. Registering a Subscription for Notification with the DSS

1. Make sure your computer has a working web browser.
1. If you are currently in a virtual environment, run the command `deactivate` to get out of the environment.
1. Create a new Python 3.6 virtual environment and activate it.
1. Install HCA using the command
   ```
   pip install --upgrade hca
   ```
1. Setting up config file using the instructions located [here](https://github.com/HumanCellAtlas/dcp-cli#development).
1. Login to HCA using the command
   ```
   hca dss login
   ```
1. Check if your HCA DSS deployment has been configured correctly by running
   ```
   hca dss post-search --replica aws --es-query '{}'
   ```
You should receive a response json containing one or more urls. Check if the returned URLs match the intended DSS Server.
1. You should have received the response `Please visit this URL to authorize this application...` and the corresponding URL.
Click on link. Your web browser should appear requesting you to login. Login with your Google Account.
1. Registering the subscription
   ```
   hca dss put-subscription --replica <aws or gcp> --callback-url <azul-indexer-url> --es-query '{"query": {"match_all": {}}}'
   ```

    - `<aws or gcp>`: your preferred storage bucket service
    - `<azul-indexer-url>`: your azul indexer's url. If you need to know what is your url, following the directions 
    in the below section *Finding the URL of your Azul Indexer Lambda*.
1. Copy down the uuid for the subscription.

## Reindexing

Run the command below.
```
python test/find-golden-tickets.py --dss-url https://<your DSS URL> --indexer-url https://<your elasticsearch endpoint> --es-query {}`
```
where you replace the `<>` value with your own values.
- `<your DSS URL>` is the endpoint of the HCA DSS and the /v1 url path. (e.g. example-dss.ucsc-cgp.org/v1)
- `<your elasticsearch endpoint>` is url of Azul's Elasticsearch Domain. (e.g. search-example-lambda-abcdefghijklmnopqrstuvwxyz.us-west-2.es.amazonaws.com)
       

## Finding the URL of your Azul Indexer Lambda

1. Click **Services** in the Top Toolbar.
1. Click **Lambda** under the **Compute** subsection.
1. Search for the name of your lambda and click name of your lambda.
1. Click on **API Gateway** in the **Designer** Table. Should bear the very top.
1. Then, the only table below the **Designer** Table is the **API Gateway** Table. In **API Gateway** Table, the
click on the caret next to under the gateway name.

After expanding the details, you should see the **Invoke URL**. This is the URL of your Azul Indexer Lambda.

## Methods and Endpoints

|  Methods/Endpoints | Notes |
| ------------- | ------------- |
| `<callback_url>`/  | takes in a post request and indexes the bundle found in the request   |
| es_check() |  returns the ES info, good check to make sure Chalice can talk to ES  |


### Todo List

* how to setup Kibana for security group reasons
* improve mappings to Chalice
* list handling in json files
* cron deduplication
* capibility to download files that are not json
* multiple version handling (per file version or per file?)
* Unit testing: Flask mock up of the HCA DSS endpoints
    * We need something that will generate POSTS to the Lambda, such as a shell script.
    * Flask has endpoints for looking up bundles, and get a particular manifest.
    * Assume  bundles uuid always exist. generate a request to download anything indexable ? 
* Improve debugging (config for turning on/off debug)
