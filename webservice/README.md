# dcc-dashboard-service
Simple Flask web service to communicate with ElasticSearch and output the list of results in JSON format. <br>
There are currently five working endpoints:
<ul>
<li>"<code>/repository/files/</code>" returns the index search results along with a count of the terms available for the facets.</li>
<li>"<code>/repository/files/summary</code>" returns a summary of the current data stored.</li>
<li>"<code>/repository/files/export</code>" returns a manifest file with the filters provided.</li>
<li>"<code>/repository/files/order</code>" returns the desired order for the facets.</li>
<li>"<code>/keywords</code>" returns a list of search results for some search query.</li>
</ul>

<h2>Installation Instructions</h2>

Please refer to [dcc-ops](https://github.com/BD2KGenomics/dcc-ops) for installing the the dashboard-service and all its required components. This will automatically set up the dashboard service behind an NGINX server.

***API Instructions; /repository/files***<br>
Currently there are 6 parameters supported. They are as follows:<br>

|Parameter|Description|Data Type|Example|
|--- |--- |--- |--- |
|filters|Specifies which filters to use to return only the files with the matching criteria. Supplied as a string with the format: {"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}|String|http://ucsc-cgl.org/api/v1/repository/files/?filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D This will return only those files who have a file format of type "bam"|
|from|Specifies the start index. Defaults to 1 if not specified:|Integer|http://ucsc-cgl.org/api/v1/repository/files/?from=26  This will return the next 25 results starting at index 26|
|size|Specifies how many hits to return. Defaults to 10|Integer|http://ucsc-cgl.org/api/v1/repository/files/?size=50  This will return 50 hits starting from 1|
|sort|Specifies the field under which the list of hits should be sorted. Defaults to "center_name"|String|http://ucsc-cgl.org/api/v1/repository/files/?sort=donor  This will return the hits sorted by the "donor" field.|
|order|Specifies the order in which the hits should be sorted; two options, "asc" and "desc". Defaults to "desc".|String|http://ucsc-cgl.org/api/v1/repository/files/?order=desc  This will return the hits in descending order.|

<br>


***API Instructions; /repository/files/export***<br>

|Parameter|Description|Data Type|Example|
|--- |--- |--- |--- |
|filters|Specifies which filters to use to return only the manifest with of the files that matching the criteria. Supplied as a string with the format:`{"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}`|String|http://ucsc-cgl.org/api/v1/repository/files/export?filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D This will return a manifest with only those files who have a file format of type "bam"|


<br>

***API Instructions; /repository/files/summary***<br>

|Parameter|Description|Data Type|Example|
|--- |--- |--- |--- |
|filters|Specifies which filters to use to return only the summary with the matching criteria. Supplied as a string with the format:`{"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}`|String|http://ucsc-cgl.org/api/v1/repository/files/summary?filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D This will return a manifest with only those files who have a file format of type "bam"|

<br>

***API Instructions; /keywords***<br>

Currently there are 5 parameters supported. They are as follows:<br>

|Parameter|Description|Data Type|Example|
|--- |--- |--- |--- |
|type|Specifies which type to return (file or file-donor). Supplied as a string. Defaults to 'file'.|String|http://ucsc-cgl.org/api/v1/keywords?type=file&q=84f153 This will return files based on the search query 84f153.|
|filters|Specifies which filters to use to return only the search results with the matching criteria. Supplied as a string with the format:`{"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}`|String|http://ucsc-cgl.org/api/v1/keywords?type=file&q=84f153&filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D This will return only those files who have a file format of type "bam" for the query 84f153 .|
|from|Specifies the start index. Defaults to 1 if not specified:|Integer|http://ucsc-cgl.org/api/v1/keywords?type=file&q=84f153&from=26 This will return the search results from result 26 onwards|
|size|Specifies how many hits to return. Defaults to 5|Integer|http://ucsc-cgl.org/api/v1/keywords?type=file&q=84f153&size=5 This will return at most 5 hits.|
|q|Specifies the query for search|String|http://ucsc-cgl.org/api/v1/keywords?type=file&q=84f153&size=5 This will return at most 5 hits for the query 84f153.|


