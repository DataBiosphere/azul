import lxml.html
from IPython import embed
import json
data = lxml.html.parse('http://www.ec2instances.info')

region_name_mapping = {
    "us-west-1": "n. california",
    "us-west-2": "oregon",
    "us-east-1": "n. virginia",
    "us-east-2": "ohio"
}
api_price_mapping = {}
rows = data.xpath("//tr[contains(@class, 'instance')]")
costs = {}
for x in rows:
    apiname = x.xpath("td[contains(@class, 'apiname')]")[0].text
    vals = x.xpath("td[contains(@class, 'cost-ondemand-linux')]")[0].attrib.get("data-pricing").replace("&#34;","\"")
    costlinux = None
    print(apiname)
    print(vals)
    if vals:
        costlinux = dict(json.loads(vals))
    if costlinux:
        for region, cost in costlinux.items():
            costs[str(region)+str(apiname)] = cost
            if region_name_mapping.get(str(region)):
                costs[region_name_mapping[str(region)]+str(apiname)] = cost

with open('region_instance_prices.json','w') as fout:
    json.dump(costs, fout, sort_keys=True)




