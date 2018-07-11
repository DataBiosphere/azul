# dss-azul-indexer

This is the indexer that consumes events from the HCA DSS and indexes them into Elasticsearch.

## Installation

### Before Getting Started

Make sure to write down: 
- Your HCA DSS endpoint. See here for instructions: https://github.com/HumanCellAtlas/data-store/tree/master
- The AWS Access Key and AWS Secret Key to be used

### 1. Creating an Elasticsearch Domain
 
9. First, create an Elasticsearch Domain on AWS. You can do this in the AWS Console by clicking "Services" in the top toolbar, 
and then clicking "Elasticsearch Service".
9. Once you are in the Amazon Elasticsearch Service dashboard. Click "Create a new domain".
9. Under the Define Domain step, give a domain name to new the ES Domain
and use Elasticsearch version 5.5 and click next.
9. Under *Configure Cluster* step, unless you need other configurations for your domain (*Storage, Node, Snapshot, etc*), skip to the next step
9. In the *Set Up Access* step, set network configuration to *Public Access*.
9. Copy the contents of `policies/elasticsearch-policy.json` file in the cgp-dss-azul-indexer repository
9. Then, paste those contents into the textbox under *Access policy* 
9. Edit the following values in the policy with your own values:
    - `<your-es-domain-name>` is the name of the elasticsearch domain
    - `<your-indexer-lambda-domain-name>` is the name that you will give the Lambda project.
    - `<AWS-account-ID>` is not your IAM User name, but a numeric id representing your aws account. You can find your AWS account ID by clicking "Select a Template" dropdown box. 
    Then, click "Allow or deny access to one or more AWS accounts or IAM users". Then, a pop-up will appear with your "AWS account ID".
    - `<your-ip-address>` is your ip address.
9. Record the values above for later use. They will be need to setup later components.
9. Confirm your policy settings, review what your configured, and finalize setting up the Elasticsearch domain. 
9. You should be in the Amazon Elasticsearch Service dashboard. Click on the name of the newly created Elasticsearch domain
    - record your ARN Domain. Knowing the ARN Domain will help you configure with configuration later.

### 2. Configure AWS and create a Virtual Environment

9. Install python 3.6
9. Create a virtual environment with `virtualenv -p python3.6 <envname>` 
9. Activate with `source <envname>/bin/activate`.
9. Install and configure the AWS CLI with credentials to be used (AWS Access Keys and AWS Secret Keys)

```
pip install awscli --upgrade
aws configure
```

### 3. Setup Chalice

9. Install Chalice with the following command:
    ```
    pip install chalice
    chalice new-project
    ```
9. When prompted for project name, input `<your-indexer-lambda-application-name>`.
9. Change the working directory to the newly created folder `<your-indexer-Lambda-application-name>`.
9. Execute `chalice deploy`. **Note:** If you are deploying an AWS API Gateway stage other than the default (dev)
add the `--stage <stage name>` option.
9. Record the URL returned in the last line of stdout returned by this command - henceforth referred to as `<azul-indexer-url>`.
This will create an AWS Lambda function with the name of your application name which will be updated using `chalice deploy`.
9. Chalice will automatically generate a folder with the name of your application name. The folder will contain the files `app.py` and `requirements.txt`
and a `.chalice` folder containing a `config.json` file. Overwrite those by copying `app.py`, `requirements.txt` and `chalicelib/` from this repo and the generated folder.
9. Then, execute `pip install -r requirements.txt`.
9. Open `.chalice/config.json`, remove any existing text in the file and copy the text below onto `config.json`.
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
9. Then, replace the following values in the config.json with your own values. 
    - `<stage_name>` is the intended stage name of the Lambda
    - `<your-indexer-lambda-application-name>` is the name that you will give the Lambda project.
    - `<AWS-account-ID>` is one of the values in the access policy of your Elasticsearch. An numerical id of your AWS Account. 
    - `<your blue box>` is the endpoint of the HCA DSS.
    - `<your elasticsearch endpoint>` is url you will use to access Azul's Elasticsearch Domain. This url can be found by returning to your Elasticsearch Service Dashboard,
    clicking the link of your Elasticsearch Domain. The url should be labled as **Endpoint** in the Overview Tab.

### 4. Modifying IAM Access Policies

9. In the AWS Console, click Services in the top toolbar, and click IAM in the Security, Identity & Compliance subsection.
9. On the side menu bar, click **Roles**, then search for your Lambda function, `<your-indexer-lambda-application-name>`. Once you find it, click on it.
9. Find a table in the Permissions tab under the blue Attach Policy button. You should see for Lambda name in the table. Click on the caret on the right side of the Lambda name. Then, click on "Edit Policy".
9. Add the policy found in `policies/elasticsearch-policy.json` file in the cgp-dss-azul-indexer repository (Make sure to change the `Resource` value to the ARN of your elasticsearch box)

### 5. Deploying Indexer Lambda

9. In the directory of your chalice function, run `chalice deploy --no-autogen-policy.`  **Note:** If you are deploying an AWS API Gateway stage other than the default (dev)
add the `--stage <stage name>` option. Since we have created a policy in AWS we do not want chalice to automatically create a policy for us.

### 6. Registering a Subscription for Notification with the DSS

9. Make sure your computer has a working web browser.
9. If you are currently in a virtual environment, run the command `deactivate` to get out of the environment.
9. Create (another) virtual enviroment and activate it.
9. Install HCA using the command `pip install --upgrade hca`
9. Setting up config file using the instructions located [here](https://github.com/HumanCellAtlas/dcp-cli#development).
9. Check if your HCA DSS deployment has been configured correctly by running `hca dss post-search --replica aws --es-query '{}'`.
You should receive a response json containing one or more urls. Check if the returned URLs match the intended DSS Server.
9. Login to HCA using the command `hca dss login`.
9. You should have received the response `Please visit this URL to authorize this application...` and the corresponding URL.
Click on link. Your web browser should appear requesting you to login. Login with your UCSC Gmail Account.
9. Registering the subscription `hca dss put-subscription --replica <aws or gcp> --callback-url <azul-indexer-url> --es-query '{"query": {"match_all": {}}}'`
    - `<aws or gcp>`: your preferred storage bucket service
    - `<azul-indexer-url>`: your azul indexer's url. If you need to know what is your url, following the directions 
    in the below section *Finding the URL of your Azul Indexer Lambda*.
9. Copy down the uuid for the subscription

## Reindexing

Run the command below.
```
python test/find-golden-tickets.py --dss-url https://<your DSS URL>c--indexer-url https://<your elasticsearch endpoint> --es-query {}`
```
where you replace the `<>` value with your own values.
- <your DSS URL> is the endpoint of the HCA DSS.
- <your elasticsearch endpoint> is url of Azul's Elasticsearch Domain.
       

## Finding the URL of your Azul Indexer Lambda

9. Click **Services** in the Top Toolbar.
9. Click **Lambda** under the **Compute** subsection.
9. Search for the name of your lambda and click name of your lambda.
9. Click on **API Gateway** in the **Designer** Table. Should bear the very top.
9. Then, the only table below the **Designer** Table is the **API Gateway** Table. In **API Gateway** Table, the
click on the caret next to under the gateway name.

After expanding the details, you should see the **Invoke URL**. This is the URL of your Azul Indexer Lambda.

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
* Unit testing: Flask mock up of the HCA DSS endpoints
    * We need something that will generate POSTS to the Lambda, such as a shell script.
    * Flask has endpoints for looking up bundles, and get a particular manifest.
    * Assume  bundles uuid always exist. generate a request to download anything indexable ? 
* Improve debugging (config for turning on/off debug)
