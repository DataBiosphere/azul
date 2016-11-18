import json
from decimal import Decimal
import calendar
pricing = json.load(open("region_instance_prices.json"))
EXTRA_MONEY = 1.2
SECONDS_IN_HR = 3600

BYTES_IN_GB = 1000000000
STORAGE_PRICE_GB_MONTH = 0.03


def make_bills(comp_aggregations, storage_past_aggregations, portion_of_month):
    x=comp_aggregations
    print(x)
    instances = x["aggregations"]["filtered_nested_timestamps"]["filtered_range"]["vmtype"]["buckets"]
    total_pricing = Decimal()
    for instance in instances:
        instanceType = instance["key"]
        regions = instance["regions"]["buckets"]
        for region in regions:
            regionName = region["key"]
            totalTime = region["totaltime"]["value"]
            print(regionName, instanceType, totalTime, pricing[regionName+instanceType])
            total_pricing += Decimal(pricing[regionName+instanceType]) * Decimal(EXTRA_MONEY) * \
                             Decimal(totalTime)/Decimal(SECONDS_IN_HR)

    # need to get the storage size
    storage_size_bytes = storage_past_aggregations['aggregations']['filtered_nested_timestamps']['sum_sizes']['value']
    storage_size_gb = Decimal(storage_size_bytes)/Decimal(BYTES_IN_GB)

    total_pricing += Decimal(STORAGE_PRICE_GB_MONTH)*storage_size_gb*portion_of_month*Decimal(EXTRA_MONEY)

    return total_pricing
