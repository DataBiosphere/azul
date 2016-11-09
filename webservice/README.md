# dcc-dashboard-service
Simple Flask web service to communicate with ElasticSearch and output the list of results in JSON format. <br>
There are currently two working endpoints; one (/files/) returns the index search results along with a count of the terms available for the facets. <br>
The other endpoint (/files/export) returns a manifest file with the filters provided. <br>

<h2>Instructions</h2>
***Assumptions***<br>
-You have ElasticSearch 2.4.1 installed<br>
-You have virtualenv installed<br>
<br>In case you don't have virtualenv installed, you can do so by:<br>
```
sudo easy_install virtualenv
```
<br>
1.-Download this repo by using `git clone https://github.com/caaespin/APITest.git`. Make sure you are on the "forBrowserTesting" branch by using `git checkout forBrowserTesting`<br>
2.-The repo includes a sample jsonl file called elasticsearch.jsonl. It is a sample search index for testing purposes. Use `curl -XPOST "http://localhost:9200/mfiles/analysis_index/_bulk?pretty" --data-binary  @elasticsearch.jsonl` to index the data in elasticsearch.jsonl. <br>
3.-Use `virtualenv venv` to set up your virtualenv. Activate it using `. venv/bin/activate`<br>
4.-In case you don't have Flask and the Flask ElasticSearch client, use:
```
pip install Flask
pip install Flask-Elasticsearch
```
<br>
5.-You need to install flask_cors and flask_excel. You can do so by running the following:
```
pip install -U flask-cors
pip install flask_excel
```
<br>
6.-Once you have that, start your Elasticsearch copy in another terminal window. Back where you have your virtual environment, do:
```
export FLASK_APP=mapi.py
flask run
```
<br>
This will start the app. <br>
7.-Open your browser and go to `http://127.0.0.1:5000/files/` to see the API response.  
<br>
***API Instructions; /files/***<br>
Currently there are 6 parameters supported. They are as follows:<br>
<table width="100%">
  <tbody>
    <tr>
      <th>Parameter</th>

      <th>Description</th>

      <th>Data Type</th>

      <th>Example</th>
    </tr>

    <tr>
      <td>field</td>

      <td>Specifies which fields to return. Supplied as a series of strings separated by a comma</td>

      <td>Array[String]</td>

      <td>http://127.0.0.1:5000/files?field=analysis_type%2Cdownload_id <br>
      This will return only the "analysis_type" and the "download_id" field from each hit. 
</td>
    </tr>

    <tr>
      <td>filters</td>

      <td>Specifies which filters to use to return only the files with the matching criteria. Supplied as a string with the format:
      {"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}
      </td>

      <td>String</td>

      <td>http://127.0.0.1:5000/files/?filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D<br>
      This will return only those files who have a file format of type "bam"</td>
    </tr>

    <tr>
      <td>from</td>

      <td>Specifies the start index. Defaults to 1 if not specified:</td>

      <td>Integer</td>

      <td>http://127.0.0.1:5000/files/?from=26 <br>
      This will return the next 25 results starting at index 26</td>
    </tr>

    <tr>
      <td>size</td>

      <td>Specifies how many hits to return. Defaults to 25</td>

      <td>Integer</td>

      <td>http://127.0.0.1:5000/files/?size=50 <br>
      This will return 50 hits starting from 1</td>
    </tr>

    <tr>
      <td>sort</td>

      <td>Specifies the field under which the list of hits should be sorted. Defaults to "center_name"</td>

      <td>String</td>

      <td>http://127.0.0.1:5000/files/?sort=donor <br>
      This will return the hits sorted by the "donor" field.
      </td>
    </tr>

    <tr>
      <td>order</td>

      <td>Specifies the order in which the hits should be sorted; two options, "asc" and "desc". Defaults to "desc".</td>

      <td>String</td>

      <td>http://127.0.0.1:5000/files/?order=desc <br>
      This will return the hits in descending order.</td>
    </tr>
  </tbody>
</table>
<br>

***API Instructions; /files/export***<br>

<table width="100%">
  <tbody>
    <tr>
      <th>Parameter</th>

      <th>Description</th>

      <th>Data Type</th>

      <th>Example</th>
    </tr>

    <tr>
      <td>filters</td>

      <td>Specifies which filters to use to return only the files with the matching criteria. Supplied as a string with the format:
      {"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}
      </td>

      <td>String</td>

      <td>http://127.0.0.1:5000/files/?filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D<br>
      This will return a manifest with only those files who have a file format of type "bam"</td>
    </tr>


  </tbody>
</table>
