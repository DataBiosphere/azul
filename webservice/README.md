# DCC-DASHBOARD-SERVICE

[![Build Status](https://travis-ci.org/BD2KGenomics/dcc-dashboard-service.svg?branch=develop)](https://travis-ci.org/BD2KGenomics/dcc-dashboard-service)

Simple Dockerized Flask web service to communicate with ElasticSearch and
output the list of results in JSON format. It also supports monitoring of
consonance jobs and tracking of billing as part of the UCSC platform.<br>

### General Overview
The web app is subdivided into different flask blueprints, each filling out a particular function. The current blueprints are:

* action
  * Responsible for returning the status of jobs running on Consonance.
* billing
  * Responsible for returning the prices of storage and computing cost. It reads from a billing index in ElasticSearch.
* webservice
  * The backend that powers the Boardwalk portal. Queries ElasticSearch to serve an API that allows to apply filters and do faceting on entries within ElasticSearch.
  
### On the Webservice

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
