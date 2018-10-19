# DCC-DASHBOARD-SERVICE
[![Build Status](https://travis-ci.org/BD2KGenomics/dcc-dashboard-service.svg?branch=develop)](https://travis-ci.org/BD2KGenomics/dcc-dashboard-service)

Simple Dockerized Flask web service to communicate with ElasticSearch and output the list of results in JSON format. It also supports monitoring of consonance jobs and tracking of billing as part of the UCSC platform.<br>

<h2>General Overview</h2>
The web app is subdivided into different flask blueprints, each filling out a particular function. The current blueprints are:

* action
  * Responsible for returning the status of jobs running on Consonance.
* billing
  * Responsible for returning the prices of storage and computing cost. It reads from a billing index in ElasticSearch.
* webservice
  * The backend that powers the Boardwalk portal. Queries ElasticSearch to serve an API that allows to apply filters and do faceting on entries within ElasticSearch.
  
<h3>On the Webservice</h3>

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
|filters|Specifies which filters to use to return only the files with the matching criteria. Supplied as a string with the format: {"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}|String|https://commons.ucsc-cgp-dev.org/api/v1/repository/files/?filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D This will return only those files who have a file format of type "bam"|
|from|Specifies the start index. Defaults to 1 if not specified:|Integer|https://commons.ucsc-cgp-dev.org/api/v1/repository/files/?from=26  This will return the next 25 results starting at index 26|
|size|Specifies how many hits to return. Defaults to 10|Integer|https://commons.ucsc-cgp-dev.org/api/v1/repository/files/?size=50  This will return 50 hits starting from 1|
|sort|Specifies the field under which the list of hits should be sorted. Defaults to "center_name"|String|https://commons.ucsc-cgp-dev.org/api/v1/repository/files/?sort=donor  This will return the hits sorted by the "donor" field.|
|order|Specifies the order in which the hits should be sorted; two options, "asc" and "desc". Defaults to "desc".|String|https://commons.ucsc-cgp-dev.org/api/v1/repository/files/?order=desc  This will return the hits in descending order.|

<br>


***/repository/files/export***<br>

|Parameter|Description|Data Type|Example|
|--- |--- |--- |--- |
|filters|Specifies which filters to use to return only the manifest with of the files that matching the criteria. Supplied as a string with the format:`{"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}`|String|https://commons.ucsc-cgp-dev.org/api/v1/repository/files/export?filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D This will return a manifest with only those files who have a file format of type "bam"|
|format|Specifies the output format of the manifest and works in conjunction with filters. Possible values are `tsv` (default) and `bdbag`. If `bdbag` is chosen, the metadata manifest will be uploaded to an AWS S3 bucket as configured during deployment, and a presigned URL pointing to the metadata manifest will be returned in the response.|String|https://commons.ucsc-cgp-dev.org/api/v1/repository/files/export?filters=%7B%22file%22:%7B%22fileFormat%22:%7B%22is%22:%5B%22crai%22%5D%7D%7D%7D&format=bdbag This uploads a BDBag containing a manifest with all "CRAI" files to AWS S3, and returns a presigned URL pointing to the metadata manifest.|


<br>

***/repository/files/summary***<br>

|Parameter|Description|Data Type|Example|
|--- |--- |--- |--- |
|filters|Specifies which filters to use to return only the summary with the matching criteria. Supplied as a string with the format:`{"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}`|String|https://commons.ucsc-cgp-dev.org/api/v1/repository/files/summary?filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D This will return a manifest with only those files who have a file format of type "bam"|

<br>

***/keywords***<br>

|Parameter|Description|Data Type|Example|
|--- |--- |--- |--- |
|type|Specifies which type of format to return (file or file-donor). Supplied as a string. Defaults to 'file'.|String|https://commons.ucsc-cgp-dev.org/api/v1/keywords?type=file&q=8f1 This will return files based on the search query 8f1.|
|field|Specifies which field to perform the search on. Defaults to 'fileId'.|String|https://commons.ucsc-cgp-dev.org/api/v1/keywords?type=file&q=UCSC&field=centerName would search for files with center name `UCSC` |
|filters|Specifies which filters to use to return only the search results with the matching criteria. Supplied as a string with the format:`{"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}`|String|https://commons.ucsc-cgp-dev.org/api/v1/keywords?type=file&q=8f1&filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D This will return only those files who have a file format of type "bam" for the query 8f1 .|
|from|Specifies the start index. Defaults to 1 if not specified:|Integer|https://commons.ucsc-cgp-dev.org/api/v1/keywords?type=file&q=8f1&from=26 This will return the search results from result 26 onwards|
|size|Specifies how many hits to return. Defaults to 5|Integer|https://commons.ucsc-cgp-dev.org/api/v1/keywords?type=file&q=8f1&size=5 This will return at most 5 hits.|
|q|Specifies the query for search|String|https://commons.ucsc-cgp-dev.org/api/v1/keywords?type=file&q=8f1&size=5 This will return at most 5 hits for the query 8f1.|



<h2>Installation Instructions</h2>

Please refer to [dcc-ops](https://github.com/BD2KGenomics/dcc-ops) for installing the the dashboard-service and all its required components. This will automatically set up the dashboard service behind an NGINX server.

If you want to locally set up the webservice for quick prototyping, you can type `make play` in your terminal. In order for this to work, you need to have `Docker` and `docker-compose` installed in your machine. Aditionally, you need to allow Docker to use 5GB of RAM. `make play` will setup the webservice, ElasticSearch, and Kibana on your machine as a series of Docker containers bound to `localhost` as follows:

* Web Service: port 9000 (access via `localhost:9000`)
* ElasticSearch: port 9200 (access via `localhost:9200`)
* Kibana: port 5601 (access via `localhost:5601`)

ElasticSearch will be loaded with a set of dummy test indexes. You can modify or change the indexes as you wish. You can do for example do

```
curl -XPUT http://localhost:9200/myindex/_bulk?pretty --data-binary @test/my_index_entries.jsonl
```

to create an index called myindex and loading the entries contained in the `my_index_entries.jsonl` file. Please take a look at the `tests/populator.sh` script for more details on how the test suite creates and loads indexes (the 'aliases' for the file and donor oriented indexes are named `fb_index` and `analysis_index` respectively). 




