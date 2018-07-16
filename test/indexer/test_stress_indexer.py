from locust import HttpLocust, TaskSet, task
from pprint import pprint
import random
import os

#Note that this test assumes there is a pre-existing server on which the stress test is done

#Priorities control the relative weighting ascribed to each test.
#If a test has priority X, and another has 2X, the latter will be
#entered twice as often

#QUERY_GROUP priority
INDEXER_QUERY_PRIORITY = 1

TEST_INDEXER_URL = "/test_data/_search"

class IndexerTester(TaskSet):

	@task(INDEXER_QUERY_PRIORITY)
	def query_indexer(self):

		with self.client.post(TEST_INDEXER_URL, catch_response=True, name=TEST_INDEXER_URL, json={'query': {'match_all': {}}}) as response:
			if 'error' in response.json():
				response.error(INDEXER_QUERY_PRIORITY + " is not responding properly to a valid query")
			else:
				try:
					print (response.json())
					#return response.json()['key']
				except ValueError:
					response.failure(TEST_INDEXER_URL + " did not return a json")


class IndexerStress(HttpLocust):
	host = "<YOUR_ES_HOST_URL_HERE>"
	task_set = IndexerTester
	#This sets the minimum time in ms between requests made by different sets of users
	min_wait = 1000
	#This sets the maximum time in ms between requests made by different sets of users
	max_wait = 1000
