"""
Fetch AWS Inspector findings and generate a CSV file with the results grouped by
vulnerability.
"""
from collections import (
    defaultdict,
)
import csv
import datetime
import json
import logging
import sys

from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.deployment import (
    aws,
)
from azul.logging import (
    configure_script_logging,
)
from azul.types import (
    JSON,
    JSONs,
)

log = logging.getLogger(__name__)

SummaryType = dict[str, str | set[str]]


class ParseInspectorFindings:
    all_severities = [
        'CRITICAL',
        'HIGH',
        'MEDIUM',
        'LOW',
        'INFORMATIONAL',
        'UNTRIAGED'
    ]
    default_severities = [
        'CRITICAL',
        'HIGH'
    ]

    @classmethod
    def _parse_args(cls, argv):
        import argparse
        parser = argparse.ArgumentParser(description=__doc__,
                                         formatter_class=AzulArgumentHelpFormatter)
        parser.add_argument('--severity', '-s',
                            default=cls.default_severities,
                            help='Only fetch findings with the specified'
                                 ' severity. '
                                 f'(choices: {cls.all_severities})',
                            nargs='+',
                            metavar='S',
                            choices=cls.all_severities)
        parser.add_argument('--all-images', '-a',
                            default=False, action='store_true',
                            help='Fetch findings for all images, including those'
                                 ' outside the security boundary.')
        parser.add_argument('--json', '-j',
                            default=False, action='store_true',
                            help='Dump findings to a JSON file.')
        args = parser.parse_args(argv)
        return args

    def __init__(self, argv: list[str]) -> None:
        super().__init__()
        self.args = self._parse_args(argv)
        self.date = datetime.datetime.now().strftime('%Y-%m-%d')
        self.images = set()
        self.instances = set()

    def main(self) -> None:
        log.info('Fetching findings from AWS Inspector')
        criteria = {
            'findingStatus': [
                {
                    'comparison': 'EQUALS',
                    'value': 'ACTIVE'
                }
            ],
            'severity': [
                {
                    'comparison': 'EQUALS',
                    'value': severity
                }
                for severity in self.args.severity
            ],
            **({} if self.args.all_images else {
                'ecrImageRepositoryName': [
                    {
                        'comparison': 'NOT_EQUALS',
                        'value': 'docker.elastic.co/kibana/kibana-oss'
                    },
                    {
                        'comparison': 'NOT_EQUALS',
                        'value': 'docker.io/lmenezes/cerebro'
                    }
                ]
            })
        }
        client = aws.client('inspector2')
        paginator = client.get_paginator('list_findings')
        findings = [
            finding
            for page in paginator.paginate(filterCriteria=criteria)
            for finding in page['findings']
        ]
        log.info('Fetched %i findings from AWS Inspector with severity %s',
                 len(findings), self.args.severity)
        if self.args.json:
            self.dump_to_json(findings)
        parsed_findings = defaultdict(list)
        for finding in findings:
            vulnerability, summary = self.parse_finding(finding)
            parsed_findings[vulnerability].append(summary)
        log.info('Found %i unique vulnerabilities', len(parsed_findings))
        self.write_to_csv(parsed_findings)
        log.info('Done.')

    def dump_to_json(self, findings: JSONs) -> None:
        output_file_name = f'inspector-findings_{self.date}.json'
        log.info(f'Writing file {output_file_name!r}')
        with open(output_file_name, 'w') as f:
            json.dump({'findings': findings}, f, default=str, indent=4)

    def parse_finding(self, finding: JSON) -> tuple[str, SummaryType]:
        severity = finding['severity']
        # The vulnerabilityId is usually a substring of the finding title (e.g.
        # "CVE-2023-44487" vs"CVE-2023-44487 - google.golang.org/grpc,
        # google.golang.org/grpc"), however this is not always the case,
        # specifically wih the "SNYK-" prefixed vulnerabilityIds, so instead of
        # using the vulnerabilityId we just use the first part of the title.
        vulnerability, _, _ = finding['title'].partition(' ')
        assert len(finding['resources']) == 1, finding
        resource = finding['resources'][0]
        resource_type = resource['type']
        summary = {
            'severity': severity,
            'resource_type': resource_type,
            'resources': set(),
        }
        if resource_type == 'AWS_ECR_CONTAINER_IMAGE':
            for tag in resource['details']['awsEcrContainerImage']['imageTags']:
                repo = resource['details']['awsEcrContainerImage']['repositoryName']
                image = f'{repo}/{tag}'
                summary['resources'].add(image)
                self.images.add(image)
        elif resource_type == 'AWS_EC2_INSTANCE':
            instance_name = resource['details']['awsEc2Instance']['keyName']
            instance_id = resource['id']
            instance = f'{instance_name} {instance_id}'
            summary['resources'].add(instance)
            self.instances.add(instance)
        else:
            assert False, resource
        return vulnerability, summary

    def column_alpha(self, col: int) -> str:
        assert col > 0, col
        chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        cols = list(chars) + [a + b for a in chars for b in chars]
        return cols[col - 1]

    def findings_sort(self, item: tuple[str, list[SummaryType]]) -> tuple[int, str]:
        score = 0
        for summary in item[1]:
            score += 10 if summary['severity'] == 'CRITICAL' else 0
            score += 1 if summary['severity'] == 'HIGH' else 0
        return score, item[0]

    def write_to_csv(self, findings: dict[str, list[SummaryType]]) -> None:
        titles = [
            'Vulnerability',
            'Severity',
            *sorted(self.images),
            *sorted(self.instances)
        ]
        last_col = self.column_alpha(len(titles))
        # A mapping of column titles to column index (0-based)
        lookup = dict(zip(titles, range(len(titles))))

        rows = [titles]
        for vulnerability, summaries in sorted(findings.items(),
                                               key=self.findings_sort,
                                               reverse=True):
            # A mapping of column index to abbreviated severity value
            column_values = {
                lookup[key]: summary['severity'][0:1]
                for summary in summaries
                for key in summary['resources']
            }
            row_num = len(rows) + 1
            col_range = f'C{row_num}:{last_col}{row_num}'
            severity_formula = f'=(COUNTIF({col_range},"C")*10)+(COUNTIF({col_range},"H"))'
            row = [vulnerability, severity_formula]
            for column_index in range(len(row), len(titles) + 1):
                row.append(column_values.get(column_index, ''))
            rows.append(row)

        output_file_name = f'inspector-findings_{self.date}.csv'
        log.info('Writing file: %s', output_file_name)
        with open(output_file_name, mode='w') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerows(rows)


if __name__ == '__main__':
    configure_script_logging(log)
    parser = ParseInspectorFindings(sys.argv[1:])
    sys.exit(parser.main())
