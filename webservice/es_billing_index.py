import jsonlines
import json
with jsonlines.open("validated.jsonl") as reader:
    with open("billing_idx.jsonl", "w") as billing_idx:
        counter = 0
        for obj in reader:
            adict = json.dumps(obj)
            adict = str(adict).replace("'",'"')
            indexing = {"index":{"_id": counter, "_type":"meta"}}
            indexing = str(indexing).replace("'",'"')
            counter += 1
            billing_idx.write(indexing + "\n" + adict + "\n")
