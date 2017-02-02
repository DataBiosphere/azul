import json
from decimal import Decimal
import calendar
import datetime
pricing = json.load(open("region_instance_prices.json"))
EXTRA_MONEY = 1.2
SECONDS_IN_HR = 3600

BYTES_IN_GB = 1000000000
STORAGE_PRICE_GB_MONTH = 0.03


def make_bills(comp_aggregations, previous_month_bytes, portion_of_month, this_month_timestamps_sizes, curr_time,
               seconds_in_month):
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

    # need to get the storage size for files completed before start of this month
    storage_size_bytes = previous_month_bytes['aggregations']['filtered_nested_timestamps']['sum_sizes']['value']
    storage_size_gb = Decimal(storage_size_bytes)/Decimal(BYTES_IN_GB)
    total_pricing += Decimal(STORAGE_PRICE_GB_MONTH)*storage_size_gb*portion_of_month*Decimal(EXTRA_MONEY)


    # calculate the money spent on storing workflow outputs which were uploaded during this month
    this_month_timestamps = this_month_timestamps_sizes['aggregations']['filtered_nested_timestamps']['times'][
        'buckets']
    for ts_sum in this_month_timestamps:
        time_string = ts_sum['key_as_string']
        time = datetime.datetime.strptime(time_string, "%Y-%m-%dT%H:%M:%S.%fZ")

        timediff = (curr_time - time).total_seconds()
        month_portion = Decimal(timediff)/Decimal(seconds_in_month)

        storage_size_bytes = ts_sum['sum_sizes']['value']
        storage_size_gb = Decimal(storage_size_bytes)/Decimal(BYTES_IN_GB)

        cost_here = storage_size_gb * month_portion
        total_pricing += cost_here


    return total_pricing

def get_compute_costs(comp_aggregations):
    instances = comp_aggregations["aggregations"]["filtered_nested_timestamps"]["filtered_range"]["vmtype"]["buckets"]
    compute_costs = Decimal(0)
    for instance in instances:
        instanceType = instance["key"]
        regions = instance["regions"]["buckets"]
        for region in regions:
            regionName = region["key"]
            totalTime = region["totaltime"]["value"]
            print(regionName, instanceType, totalTime, pricing[regionName+instanceType])
            compute_costs += Decimal(pricing[regionName+instanceType]) * Decimal(EXTRA_MONEY) * \
                             Decimal(totalTime)/Decimal(SECONDS_IN_HR)
    return compute_costs

def get_storage_costs(previous_month_bytes, portion_of_month, this_month_timestamps_sizes, curr_time, seconds_in_month):
    storage_costs = Decimal(0)
    storage_size_bytes = previous_month_bytes['aggregations']['filtered_nested_timestamps']['sum_sizes']['value']
    storage_size_gb = Decimal(storage_size_bytes)/Decimal(BYTES_IN_GB)
    storage_costs += Decimal(STORAGE_PRICE_GB_MONTH)*storage_size_gb*portion_of_month*Decimal(EXTRA_MONEY)


    # calculate the money spent on storing workflow outputs which were uploaded during this month
    this_month_timestamps = this_month_timestamps_sizes['aggregations']['filtered_nested_timestamps']['times'][
        'buckets']
    for ts_sum in this_month_timestamps:
        time_string = ts_sum['key_as_string']
        time = datetime.datetime.strptime(time_string, "%Y-%m-%dT%H:%M:%S.%fZ")

        timediff = (curr_time - time).total_seconds()
        month_portion = Decimal(timediff)/Decimal(seconds_in_month)

        storage_size_bytes = ts_sum['sum_sizes']['value']
        storage_size_gb = Decimal(storage_size_bytes)/Decimal(BYTES_IN_GB)

        analysis_cost = storage_size_gb * month_portion
        storage_costs += analysis_cost

    return storage_costs

