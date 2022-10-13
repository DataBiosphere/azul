"""
This script is referenced by our TerraForm config as an external data source. It
prints the current deployment's Elasticsearch node IDs as JSON to STDOUT. If the
list of node IDs contains fewer items than the number of placeholders in the
Cloudwatch dashboard template, the output is padded with dummy entries.

Note that Terraform requires the object given to an external data source is a
map of string values, so the list of node IDs is JSON-encoded and later
converted back into a list when processed in the Terraform config.
"""
import json
import logging
from time import (
    sleep,
)
from typing import (
    Sequence,
)

from botocore.exceptions import (
    ClientError,
)
import jq
from more_itertools import (
    padded,
)

from azul import (
    config,
    require,
)
from azul.deployment import (
    aws,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)

# How many times to try getting the node IDs from AWS before giving up.
max_attempts = 10


def main() -> None:
    node_ids = get_node_ids()
    data = prepare_node_ids(node_ids)
    print(json.dumps(data))


def get_node_ids() -> Sequence[str]:
    """
    Return a list of ES node IDs used by the current deployment.
    """
    for i in range(max_attempts):
        log.debug('Attempt %d/%d', i + 1, max_attempts)
        try:
            response = aws.cloudwatch.list_metrics(Namespace='AWS/ES',
                                                   MetricName='CPUUtilization',
                                                   # To exclude old and unused ES nodes we filter by 'PT3H' (the only
                                                   # choice for this parameter) so that only metrics that have had data
                                                   # points published in the past three hours will be included.
                                                   RecentlyActive='PT3H',
                                                   Dimensions=[
                                                       {
                                                           'Name': 'DomainName',
                                                           'Value': config.es_domain
                                                       }
                                                   ])
        except ClientError as e:
            log.warning("Failed to list Cloudwatch metrics due to '%s'. Retrying …",
                        e.response['Error']['Code'])
        else:
            nodes = [
                d['Value']
                for m in response['Metrics']
                for d in m['Dimensions']
                if d['Name'] == 'NodeId'
            ]
            if nodes:
                return nodes
            else:
                log.warning('No node IDs found in response. Retrying …')
        sleep(5)
    raise RuntimeError('Failed to obtain node IDs')


def prepare_node_ids(node_ids: Sequence[str]) -> dict[str, str]:
    """
    Return the given node IDs in a data structure that TerraForm can use as an
    external data source.
    """
    # The list of nodes IDs is appended with dummy entries to ensure the length
    # matches the number of placeholders in the template. If our list had fewer
    # items than there are matches in the template, Terraform would throw an
    # "Invalid index" error.
    num_placeholders = node_placeholder_count()
    require(num_placeholders > 0,
            'Error: No node ID placeholders found in template file')
    # The dashboard will not allow an empty string as a node ID value, causing
    # the error `Invalid metric field type, only "String" type is allowed`
    # during deploy. For this reason we pad the list with a dummy value.
    node_ids = list(padded(node_ids, 'none', num_placeholders))
    return {
        'nodes': json.dumps(node_ids)  # TerraForm only accepts string values
    }


def node_placeholder_count() -> int:
    """
    Return the number of unique ES node ID placeholders found in the Cloudwatch
    dashboard template file.
    """
    with open(config.cloudwatch_dashboard_template) as f:
        body = json.load(f)
    filters = [
        # Indexing from end so we don't have to deal with the `...` placeholder.
        # The last three elements are the `Client ID` dimension name, its value
        # and the options object.
        # https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/CloudWatch-Metric-Widget-Structure.html#CloudWatch-Metric-Widget-Metrics-Array-Format
        '.widgets[].properties.metrics[]?[-4]',
        'values',
        'capture(' + json.dumps(r'local\.nodes\[(?<x>[0-9]+)\]') + ').x',
        'tonumber',
    ]
    max_index = max(jq.all('|'.join(filters), body))
    return max_index + 1


if __name__ == '__main__':
    configure_script_logging(log)
    main()
