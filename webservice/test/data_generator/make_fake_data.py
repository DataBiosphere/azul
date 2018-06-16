#!/usr/bin/python
import json
import sys
import subprocess
from faker_schema.schema_loader import load_json_from_file
from faker import Faker


# Modified from https://github.com/ueg1990/faker-schema/blob/master/faker_schema/faker_schema.py
# (Adjusted code to allow lists of strings in a template)
class FakerSchema(object):

    def __init__(self, faker=None, locale=None, providers=None, includes=None):
        self._faker = faker or Faker(locale=locale, providers=providers, includes=includes)

    def generate_fake(self, schema, iterations=1):
        result = [self._generate_one_fake(schema) for _ in range(iterations)]
        return result[0] if len(result) == 1 else result

    def _generate_one_fake(self, schema):
        """
        Recursively traverse schema dictionary and for each "leaf node", evaluate the fake
        value
        Implementation:
        For each key-value pair:
        1) If value is not an iterable (i.e. dict or list), evaluate the fake data (base case)
        2) If value is a dictionary, recurse
        3) If value is a list, iteratively recurse over each item
        """
        data = {}
        for k, v in schema.items():
            if isinstance(v, dict):
                data[k] = self._generate_one_fake(v)
            elif isinstance(v, list):
                if (isinstance(v[0], dict)):
                    data[k] = [self._generate_one_fake(item) for item in v]
                else:
                    data[k] = [getattr(self._faker, a)() for a in v]
            else:
                data[k] = getattr(self._faker, v)()
        return data


numIts = 1
page_size = 100000
if len(sys.argv) < 2:
    print("USAGE: " + sys.argv[
        0] + " <template file> {number of entries per call (default=100000)} {number of iterations (default=1)}")
    sys.exit(1)
json_file = sys.argv[1]

if len(sys.argv) >= 3:
    page_size = int(sys.argv[2])
    print("Setting number of entries per call to " + str(page_size))
if len(sys.argv) == 4:
    numIts = int(sys.argv[3])
    print("Setting number of iterations to " + str(numIts))

schema = load_json_from_file(json_file)
faker = FakerSchema()
subprocess.run("echo Deleting test_data index at $ES_SERVICE", shell=True)
subprocess.run("curl -XDELETE $ES_PROTOCOL://$ES_SERVICE:$ES_PORT/test_data/", shell=True)
print("\n")
subprocess.run("echo Configuring settings at $ES_SERVICE", shell=True)
subprocess.run("curl -XPUT $ES_PROTOCOL://$ES_SERVICE:$ES_PORT/test_data/ -d @td_settings.json", shell=True)
print("\n")
subprocess.run("echo Configuring mapping at $ES_SERVICE", shell=True)
subprocess.run(
    "curl -XPUT $ES_PROTOCOL://$ES_SERVICE:$ES_PORT/test_data/_mapping/meta?update_all_types  -d @td_mapping.json",
    shell=True)
print("\n\nLoading data")
index = 1
for j in range(0, numIts):
    pct = round(j / numIts * 100, 1)
    print("Iteration: " + str(j) + ", total loaded: " + str(index - 1) + " complete: " + str(pct) + "%")
    file = open('td_index_page.jsonl', 'w')
    for i in range(0, page_size):
        file.write("""{"index": {"_type": "meta", "_id": """ + str(index) + """}}""" + "\n")
        file.write(json.dumps(faker.generate_fake(schema)) + "\n")
        index = index + 1
    file.close()
    print("Wrote td_index_page.jsonl")
    subprocess.run(
        "curl -s -XPUT $ES_PROTOCOL://$ES_SERVICE:$ES_PORT/test_data/_bulk?pretty --data-binary @td_index_page.jsonl >/dev/null",
        shell=True)
