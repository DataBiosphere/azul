# Luigi Monitor Backend Page

The contents of this directory feed the Luigi Monitor page on the CGL website. 
* spawned.py and run.sh are scripts used to provide test jobs for Luigi. 
* elasticsearch.jsonl supplies the local elasticsearch index. In the final implementation, I think it's better to use a python library than appending to the file..
* request.py scrapes all jobs from my local Luigi server and appends them to elasticsearch.json.
