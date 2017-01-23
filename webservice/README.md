# dcc-dashboard-service
Simple Flask web service to communicate with ElasticSearch and output the list of results in JSON format. <br>
There are currently five working endpoints:
<ul>
<li>"<code>/repository/files/</code>" returns the index search results along with a count of the terms available for the facets.</li>
<li>"<code>/repository/files/summary</code>" returns a summary of the current data stored.</li>
<li>"<code>/repository/files/export</code>" returns a manifest file with the filters provided.</li>
<li>"<code>/repository/files/meta</code>" returns the desired order for the facets.</li>
<li>"<code>/keywords</code>" returns a list of search results for some search query.</li>
</ul>

<h2>Developer Installation Instructions</h2>
##Assumptions 
<ul>
<li>You have Elasticsearch 5.0.0 installed in your machine.</li>
<li>You have virtualenv installed in your machine. </li>
</ul>

If you don’t have virtualenv installed, you can do so by running:
```
sudo easy_install virtualenv
```
##Setting up the Directory Structure
The following directory structure should be set up:

```
|
|__dcc-dashboard-service
|
|__dcc-metadata-indexer
|
|__redwood-client
	|
	|__ucsc-storage-client
		|
		|__...etc
```
To  get the dcc-dashboard-service, do:
```
git clone https://github.com/BD2KGenomics/dcc-dashboard-service.git
```
It is assumed that you’ve setup the other folders (the dcc-metadata-indexer, and the redwood-client). If you haven’t, see https://github.com/BD2KGenomics/dcc-metadata-indexer/tree/release/1.0.0 to set up the dcc-metadata-indexer. The link for the redwood-client is also in the same page. You can set the redwood-client directory and download the client by running the following commands:
```
#Make the redwood-client directory and cd into the directory
mkdir redwood-client
cd redwood-client
#And then just download the client
wget https://s3-us-west-2.amazonaws.com/beni-dcc-storage-dev/20161216_ucsc-storage-client.tar.gz && tar zxf 20161216_ucsc-storage-client.tar.gz && rm -f 20161216_ucsc-storage-client.tar.gz
```
To set up the dashboard, follow the instructions https://github.com/BD2KGenomics/dcc-dashboard

##Installing Apache
To deploy the Flask application, we will use Apache. To install Apache and the required components in your machine, run the following commands:
```
# Install the apache webserver and mod_wsgi.
sudo apt-get update
sudo apt-get install apache2
sudo apt-get install libapache2-mod-wsgi
sudo a2enmod headers
sudo service apache2 restart
```
##Make the Soft Link
We will set the soft link for Apache to access the web service
```
#Make the soft-links for apache to use
sudo ln -sT ~/dcc-dashboard-service /var/www/html/dcc-dashboard-service
```
##Setting Up the Apache Configuration
Edit the configuration file /etc/apache2/sites-enabled/000-default.conf as follows:
```
<VirtualHost *:80>
	#Change as appropriate
        ServerName your-server-name.com 
        Header set Access-Control-Allow-Origin "*"
	#Change as appropriate
        ServerAdmin webmaster@localhost

        WSGIDaemonProcess dcc-dashboard-service threads=5
        WSGIScriptAlias /api/v1 /var/www/html/dcc-dashboard-service/dcc-dashboard-service.wsgi
        <Directory dcc-dashboard-service>
                 WSGIProcessGroup dcc-dashboard-service
                 WSGIApplicationGroup %{GLOBAL}
                 Order deny,allow
                 Allow from all
        </Directory>
        ErrorLog ${APACHE_LOG_DIR}/error.log
        CustomLog ${APACHE_LOG_DIR}/access.log combined
</VirtualHost>
```

This will set the web service endpoint under ‘your-server-name.com/api/v1’ 

Save it and run 
```
sudo service apache2 restart
```
##Setting Up HTTPS Using 'LetsEncrypt'
If you want to set up HTTPS for the web service, you can do so by using LetsEncrypt. 
Before starting however, you must comment out the `WSGIDaemonProcess` line. Not doing so will cause an error when trying to install the SSL certificates via Certbot.  
Go to https://certbot.eff.org/ and for software, choose Apache. For the system, choose as appropriate. Follow their installation instructions. 
Once the whole installation process has concluded, go back to `/etc/apache2/sites-enabled/000-default.conf` and uncomment the `WSGIDaemonProcess` line, so it is active once again. The daemon will serve both port 80 and 443 (HTTP and HTTPS respectively). 
Finally, run:
```
sudo service apache2 restart
```
##Installing Pip and Flask (If you don't have them)
You want to have pip installed so you can install various python packages:
```
#Install pip
sudo apt-get install python-pip
```
Then, to install Flask, do:
```
#Install flask
sudo pip install flask
```
##Setting up the virtualenv for the Flask App
The Web Service Flask app requires various python packages. To avoid any conflicts with other web applications within the same machine, we will create a virtual environment with the required packages to run. Assuming you are in the root directory of your machine, you can set up the virtual environment as follows:
```
#Make the virtual environment and install requirements
cd dcc-dashboard-service
virtualenv env
. env/bin/activate 
pip install -r requirements
```
##Enabling the 'instructions.sh' Script
To enable the instructions.sh script, which contains the commands to run and update the Elasticsearch indicies that the web service uses, do:
```
chmod +x dcc-dashboard-service/instructions.sh
```
##Setting the 'instructions.sh' as a cron job
To start the cron job, simply do:
```
crontab dcc-dashboard-service/crontab.txt
```
<b>NOTE:</b> crontab.txt assumes you have an environmental variable called `$REDWOOD_ACCESS_TOKEN`. To set up that variable, edit your bash file (in my case it was .bashrc). At the end of it, add
```
export REDWOOD_ACCESS_TOKEN=<your_token>
```
Save the file, and then start the cron job. It also assumes that you have the dcc-dashboard setup. In case you don’t, simply edit your cron job so it only executes the first command from the crontab.txt file. 


***API Instructions; /repository/files/***<br>
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

      <td>http://ucsc-cgl.org/api/v1/repository/files?field=analysis_type%2Cdownload_id <br>
      This will return only the "analysis_type" and the "download_id" field from each hit. 
</td>
    </tr>

    <tr>
      <td>filters</td>

      <td>Specifies which filters to use to return only the files with the matching criteria. Supplied as a string with the format:
      {"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}
      </td>

      <td>String</td>

      <td>http://ucsc-cgl.org/api/v1/repository/files/?filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D<br>
      This will return only those files who have a file format of type "bam"</td>
    </tr>

    <tr>
      <td>from</td>

      <td>Specifies the start index. Defaults to 1 if not specified:</td>

      <td>Integer</td>

      <td>http://ucsc-cgl.org/api/v1/repository/files/?from=26 <br>
      This will return the next 25 results starting at index 26</td>
    </tr>

    <tr>
      <td>size</td>

      <td>Specifies how many hits to return. Defaults to 10</td>

      <td>Integer</td>

      <td>http://ucsc-cgl.org/api/v1/repository/files/?size=50 <br>
      This will return 50 hits starting from 1</td>
    </tr>

    <tr>
      <td>sort</td>

      <td>Specifies the field under which the list of hits should be sorted. Defaults to "center_name"</td>

      <td>String</td>

      <td>http://ucsc-cgl.org/api/v1/repository/files/?sort=donor <br>
      This will return the hits sorted by the "donor" field.
      </td>
    </tr>

    <tr>
      <td>order</td>

      <td>Specifies the order in which the hits should be sorted; two options, "asc" and "desc". Defaults to "desc".</td>

      <td>String</td>

      <td>http://ucsc-cgl.org/api/v1/repository/files/?order=desc <br>
      This will return the hits in descending order.</td>
    </tr>
  </tbody>
</table>
<br>

***API Instructions; /repository/files/export***<br>

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

      <td>Specifies which filters to use to return only the manifest with of the files that matching the criteria. Supplied as a string with the format:
      {"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}
      </td>

      <td>String</td>

      <td>http://ucsc-cgl.org/api/v1/repository/files/?filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D<br>
      This will return a manifest with only those files who have a file format of type "bam"</td>
    </tr>


  </tbody>
</table>
<br>

***API Instructions; /repository/files/summary***<br>

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

      <td>Specifies which filters to use to return only the summary with the matching criteria. Supplied as a string with the format:
      {"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}
      </td>

      <td>String</td>

      <td>http://ucsc-cgl.org/api/v1/repository/files/?filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D<br>
      This will return a manifest with only those files who have a file format of type "bam"</td>
    </tr>


  </tbody>
</table>
<br>

***API Instructions; /keywords***<br>
Currently there are 5 parameters supported. They are as follows:<br>
<table width="100%">
  <tbody>
    <tr>
      <th>Parameter</th>

      <th>Description</th>

      <th>Data Type</th>

      <th>Example</th>
    </tr>

    <tr>
      <td>type</td>

      <td>Specifies which type to return (file or file-donor). Supplied as a string. Defaults to 'file'.</td>

      <td>String</td>

      <td>http://ucsc-cgl.org/api/v1/keywords?type=file&q=84f153 <br>
      This will return files based on the search query 84f153. 
</td>
    </tr>

    <tr>
      <td>filters</td>

      <td>Specifies which filters to use to return only the search results with the matching criteria. Supplied as a string with the format:
      {"file":{"fieldA":{"is":["VALUE_A", "VALUE_B"]}, "fieldB":{"is":["VALUE_C", "VALUE_D"]}, ...}}
      </td>

      <td>String</td>

      <td>http://ucsc-cgl.org/api/v1/keywords?type=file&q=84f153&filters=%7B%22file%22%3A%7B%22file_type%22%3A%7B%22is%22%3A%5B%22bam%22%5D%7D%7D%7D<br>
      This will return only those files who have a file format of type "bam" for the query 84f153 .</td>
    </tr>

    <tr>
      <td>from</td>

      <td>Specifies the start index. Defaults to 1 if not specified:</td>

      <td>Integer</td>

      <td>http://ucsc-cgl.org/api/v1/keywords?type=file&q=84f153&from=26 <br>
      This will return the search results from result 26 onwards</td>
    </tr>

    <tr>
      <td>size</td>

      <td>Specifies how many hits to return. Defaults to 5</td>

      <td>Integer</td>

      <td>http://ucsc-cgl.org/api/v1/keywords?type=file&q=84f153&size=5 <br>
      This will return at most 5 hits.</td>
    </tr>

    <tr>
      <td>q</td>

      <td>Specifies the query for search</td>

      <td>String</td>

      <td>http://ucsc-cgl.org/api/v1/keywords?type=file&q=84f153&size=5 <br>
      This will return at most 5 hits for the query 84f153.
      </td>
    </tr>
  </tbody>
</table>

