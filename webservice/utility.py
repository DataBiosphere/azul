import json
from decimal import Decimal
import datetime
import os
apache_path = os.environ.get("APACHE_PATH")
pricing = json.load(open(apache_path+"region_instance_prices.json"))
EXTRA_MONEY = 1.2  # if you want to tune billings, this is the dial. 1.2 means add 20% on top of what is calculated
# that we pay to AWS or whichever host
SECONDS_IN_HR = 3600

BYTES_IN_GB = 1000000000
STORAGE_PRICE_GB_MONTH = 0.03

def get_datetime_from_es(timestr):
    return datetime.datetime.strptime(timestr, "%Y-%m-%dT%H:%M:%S.%f")

def calculate_compute_cost(total_seconds, vm_cost_hr):
    return Decimal(Decimal(total_seconds)/Decimal(SECONDS_IN_HR)*Decimal(vm_cost_hr)*Decimal(EXTRA_MONEY))

def calculate_storage_cost(portion_month_stored, file_size_gb):
    return Decimal(portion_month_stored)*Decimal(file_size_gb)*Decimal(STORAGE_PRICE_GB_MONTH)*Decimal(EXTRA_MONEY)



def get_vm_string(host_metrics):
    return str(host_metrics.get("vm_region")) + str(host_metrics.get("vm_instance_type"))


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
            total_pricing += calculate_compute_cost(totalTime, pricing[regionName + instanceType])

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
    #create total compute cost for an entire project for a month
    instances = comp_aggregations["aggregations"]["filtered_nested_timestamps"]["filtered_range"]["vmtype"]["buckets"]
    compute_costs = Decimal(0)
    for instance in instances:
        instanceType = instance["key"]
        regions = instance["regions"]["buckets"]
        for region in regions:
            regionName = region["key"]
            totalTime = region["totaltime"]["value"]
            print(regionName, instanceType, totalTime, pricing[regionName+instanceType])
            compute_costs += calculate_compute_cost(totalTime, pricing[regionName + instanceType])
    return compute_costs


def create_analysis_costs_json(this_month_comp_hits, bill_time_start, bill_time_end):
    analysis_costs = []
    analysis_cost_actual = 0
    for donor_doc in this_month_comp_hits:
        donor = donor_doc.get("_source")
        for specimen in donor.get("specimen"):
            for sample in specimen.get("samples"):
                for analysis in sample.get("analysis"):
                    timing_stats = analysis.get("timing_metrics")
                    if timing_stats:
                        time = timing_stats["overall_walltime_seconds"]
                        analysis_end_time = get_datetime_from_es(timing_stats["overall_stop_time_utc"])
                        analysis_start_time = get_datetime_from_es(timing_stats["overall_start_time_utc"])
                        if analysis_end_time < bill_time_end and analysis_start_time >= bill_time_start:
                            host_metrics = analysis.get("host_metrics")
                            if host_metrics:
                                cost = calculate_compute_cost(time, pricing.get(get_vm_string(host_metrics)))
                                analysis_costs.append(
                                    {
                                        "donor": donor.get("submitter_donor_id"),
                                        "specimen": specimen.get("submitter_specimen_id"),
                                        "sample": sample.get("submitter_sample_id"),
                                        "workflow": analysis.get("analysis_type"),
                                        "version": analysis.get("workflow_version"),
                                        "cost": str(cost)
                                    }
                                )
                                analysis_cost_actual += cost

    return analysis_costs


def workflow_output_total_size(workflow_outputs_array):
    size = 0
    if workflow_outputs_array:
        for output in workflow_outputs_array:
            this_size = output.get("file_size")
            if this_size:
                size+=this_size
    return size

def get_gb_size(byte_size):
    return Decimal(byte_size)/Decimal(BYTES_IN_GB)

def create_storage_costs_json(project_files_hits, bill_time_start, bill_time_end, month_total_seconds):

    storage_costs = []
    storage_cost_actual = 0
    for donor_doc in project_files_hits:
        donor = donor_doc.get("_source")
        for specimen in donor.get("specimen"):
            for sample in specimen.get("samples"):
                for analysis in sample.get("analysis"):
                    timing_stats = analysis.get("timing_metrics")
                    if timing_stats:
                        analysis_end_time = get_datetime_from_es(timing_stats["overall_stop_time_utc"])
                        if analysis_end_time < bill_time_end:
                            this_size = get_gb_size(workflow_output_total_size(analysis.get("workflow_outputs")))
                            if analysis_end_time >= bill_time_start: #means it's from this month
                                seconds = (bill_time_end - analysis_end_time).total_seconds()
                            else:#it's from previous month, charge it portion of month
                                seconds = (bill_time_end - bill_time_start).total_seconds()
                            cost = calculate_storage_cost(Decimal(seconds)/Decimal(month_total_seconds), this_size)
                            storage_costs.append(
                                {
                                    "donor": donor.get("submitter_donor_id"),
                                    "specimen": specimen.get("submitter_specimen_id"),
                                    "sample": sample.get("submitter_sample_id"),
                                    "workflow": analysis.get("analysis_type"),
                                    "version": analysis.get("workflow_version"),
                                    "cost": str(cost)
                                }
                            )
                            storage_cost_actual += cost

    return storage_costs

def get_storage_costs(previous_month_bytes, portion_of_month, this_month_timestamps_sizes, curr_time, seconds_in_month):
    storage_costs = Decimal(0)
    storage_size_bytes = previous_month_bytes['aggregations']['filtered_nested_timestamps']['sum_sizes']['value']
    storage_size_gb = Decimal(storage_size_bytes)/Decimal(BYTES_IN_GB)
    storage_costs += calculate_storage_cost(portion_of_month, storage_size_gb)

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

        storage_costs += calculate_storage_cost(month_portion, storage_size_gb)

    return storage_costs

