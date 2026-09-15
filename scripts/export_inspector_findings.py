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
from azul.http import (
    http_client,
)
from azul.lib import (
    cached_property,
)
from azul.lib.types import (
    JSON,
    JSONs,
)
from azul.logging import (
    configure_script_logging,
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
    weights = {
        'CRITICAL': 10,
        'HIGH': 1
    }
    debian_url_prefix = 'https://security-tracker.debian.org/tracker/'

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

    @cached_property
    def http(self):
        return http_client()

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
        log.info('Generating CSV output')
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
        packages = {
            p['name'].rpartition(':')[2]
            for p in finding['packageVulnerabilityDetails']['vulnerablePackages']
        }
        assert len(finding['resources']) == 1, finding
        resource = finding['resources'][0]
        resource_type = resource['type']
        summary = {
            'severity': severity,
            'source_url': finding['packageVulnerabilityDetails']['sourceUrl'],
            'packages': packages,
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
            instance_name = resource['tags']['Name']
            instance_image_id = resource['details']['awsEc2Instance']['imageId']
            instance_id = resource['id']
            instance = f'{instance_name} {instance_image_id} {instance_id}'
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
        vulnerability, summaries = item
        for summary in summaries:
            count = len(summary['resources'])
            score += count * self.weights.get(summary['severity'], 0)
        packages = ', '.join(sorted(set(
            package
            for summary in summaries
            for package in summary['packages']
        )))
        if vulnerability.startswith('CVE-'):
            # Best effort on sorting CVEs by descending year and sequence
            # number. Other types of findings are sorted strictly
            # alphanumerically.
            sequence = vulnerability.rsplit('-', 1)[1]
            # The sequence number portion of CVE IDs is at most seven digits
            # long. We pad it to that length so that, for example, a CVE with
            # sequence number 11 precedes one with number 2.
            # See https://cve.mitre.org/cve/identifiers/syntaxchange.html#new.
            vulnerability = vulnerability.removesuffix(sequence) + f'{sequence:0>7}'
        return score, packages, vulnerability

    def write_to_csv(self, findings: dict[str, list[SummaryType]]) -> None:
        titles = [
            'Packages',
            'Vulnerability',
            'Link',
            'Since',
            'Severity'
        ]
        img_first_col = self.column_alpha(len(titles) + 1)
        titles.extend(sorted(self.images))
        titles.extend(sorted(self.instances))
        img_last_col = self.column_alpha(len(titles))
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
            packages = ', '.join(sorted(set(
                package
                for summary in summaries
                for package in summary['packages']
            )))
            row_num = len(rows) + 1
            col_range = f'{img_first_col}{row_num}:{img_last_col}{row_num}'
            severity_formula = (f'=(COUNTIF({col_range},"C")*{self.weights['CRITICAL']})'
                                f'+(COUNTIF({col_range},"H")*{self.weights['HIGH']})')
            # If there are multiple URLs for a vulnerability, this grabs the one
            # from nist.gov if available, else the first after sorting the list
            urls = sorted({summary['source_url'] for summary in summaries},
                          key=lambda s: ('nist.gov' not in s, s))
            source_url = urls[0]
            debian_url = self.debian_url_prefix + vulnerability
            response = self.http.request('HEAD', debian_url)
            if response.status != 200:
                log.debug('Got %s for %s', response.status, debian_url)
            if response.status == 200 and source_url != debian_url:
                link1 = f'=HYPERLINK("{debian_url}","{vulnerability}")'
                link2 = f'=HYPERLINK("{source_url}","...")'
            else:
                link1 = f'=HYPERLINK("{source_url}","{vulnerability}")'
                link2 = ''
            row = [packages, link1, link2, '', severity_formula]
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
