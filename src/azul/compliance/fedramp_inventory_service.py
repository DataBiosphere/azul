import abc
from collections import (
    Counter,
    defaultdict,
)
import inspect
import json
import logging
from operator import (
    attrgetter,
)
import pathlib
import sys
from typing import (
    AbstractSet,
    Iterable,
    Iterator,
    Optional,
    Sequence,
)

import attr
import gitlab.v4.objects.projects
from more_itertools import (
    flatten,
)
import openpyxl
from openpyxl.utils import (
    get_column_letter,
)
from openpyxl.worksheet.worksheet import (
    Worksheet,
)

from azul import (
    cached_property,
)
from azul.deployment import (
    aws,
)
from azul.types import (
    AnyJSON,
    JSON,
    JSONs,
)

log = logging.getLogger(__name__)

null_str = Optional[str]


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class InventoryRow:
    unique_id: null_str = attr.ib(default=None)
    ip_address: null_str = attr.ib(default=None)
    is_virtual: null_str = attr.ib(default=None)
    is_public: null_str = attr.ib(default=None)
    dns_name: null_str = attr.ib(default=None)
    netbios_name: null_str = attr.ib(default=None)
    mac_address: null_str = attr.ib(default=None)
    authenticated_scan_planned: null_str = attr.ib(default=None)
    baseline_config: null_str = attr.ib(default=None)
    os: null_str = attr.ib(default=None)
    location: null_str = attr.ib(default=None)
    asset_type: null_str = attr.ib(default=None)
    hardware_model: null_str = attr.ib(default=None)
    in_latest_scan: null_str = attr.ib(default=None)
    software_vendor: null_str = attr.ib(default=None)
    software_product_name: null_str = attr.ib(default=None)
    patch_level: null_str = attr.ib(default=None)
    purpose: null_str = attr.ib(default=None)
    comments: null_str = attr.ib(default=None)
    asset_tag: null_str = attr.ib(default=None)
    network_id: null_str = attr.ib(default=None)
    system_owner: null_str = attr.ib(default=None)
    application_owner: null_str = attr.ib(default=None)


class Mapper(abc.ABC):

    @abc.abstractmethod
    def map(self, resource: JSON) -> Iterable[InventoryRow]:
        raise NotImplementedError

    def _supported_resource_types(self) -> AbstractSet[str]:
        return frozenset()

    def can_map(self, resource: JSON) -> bool:
        return resource['resourceType'] in self._supported_resource_types()

    def _get_polymorphic_key(self, json: JSON, *keys: str) -> AnyJSON:
        for key in keys:
            try:
                return json[key]
            except KeyError:
                pass
        raise KeyError(*keys)

    def _get_asset_tag(self, resource: JSON) -> str:
        return self._get_polymorphic_key(resource, 'resourceName', 'resourceId')

    def _get_tag_value(self, tags: JSONs, tag_name: str) -> str:
        try:
            return next(
                tag['value']
                for tag in tags
                if tag.get('key', '').casefold() == tag_name.casefold()
            )
        except StopIteration:
            return ''

    def _get_owner(self, resource: JSON) -> str:
        return self._get_tag_value(resource['tags'], 'owner')


class EC2Mapper(Mapper):

    def _supported_resource_types(self) -> set[str]:
        return {'AWS::EC2::Instance'}

    def map(self, resource: JSON) -> Iterable[InventoryRow]:
        configuration = resource['configuration']
        try:
            public_dns_name = configuration['publicDnsName']
        except KeyError:
            dns_name = configuration['privateDnsName']
            is_public = 'No'
        else:
            dns_name = public_dns_name
            is_public = 'Yes'
        for nic in configuration['networkInterfaces']:
            for ip_addresses in nic['privateIpAddresses']:
                for ip_address_path in [('privateIpAddress',), ('association', 'publicIp')]:
                    try:
                        ip_address = self._get_ip_address(ip_addresses, ip_address_path)
                    except KeyError:
                        continue
                    else:
                        yield InventoryRow(
                            asset_tag=self._get_asset_tag(resource),
                            asset_type='AWS EC2 Instance',
                            authenticated_scan_planned='Yes',
                            baseline_config=resource['configuration']['imageId'],
                            dns_name=dns_name,
                            hardware_model=resource['configuration']['instanceType'],
                            ip_address=ip_address,
                            is_public=is_public,
                            is_virtual='Yes',
                            mac_address=nic['macAddress'],
                            network_id=resource['configuration']['vpcId'],
                            software_vendor='AWS',
                            system_owner=self._get_owner(resource),
                            unique_id=resource['configuration']['instanceId'],
                        )

    def _get_ip_address(self, ip_addresses: JSON, *keys) -> str:
        for key in keys:
            ip_addresses = ip_addresses[key]
        return ip_addresses


class ELBMapper(Mapper):

    def _supported_resource_types(self) -> set[str]:
        return {
            'AWS::ElasticLoadBalancing::LoadBalancer',
            'AWS::ElasticLoadBalancingV2::LoadBalancer'
        }

    def map(self, resource: JSON) -> Iterator[InventoryRow]:
        configuration = resource['configuration']
        ip_addresses = self._get_ip_addresses(configuration['availabilityZones'])
        if not ip_addresses:
            ip_addresses = [None]
        for ip_address in ip_addresses:
            yield InventoryRow(
                asset_tag=self._get_asset_tag(resource),
                asset_type=self._get_asset_type_name(resource),
                dns_name=configuration['dNSName'],
                ip_address=ip_address,
                is_public='Yes' if configuration['scheme'] == 'internet-facing' else 'No',
                is_virtual='Yes',
                # Classic ELBs have key of 'vpcid' while V2 ELBs have key of 'vpcId'
                network_id=self._get_polymorphic_key(configuration, 'vpcId', 'vpcid'),
                software_vendor='AWS',
                system_owner=self._get_owner(resource),
                unique_id=resource['arn'],
            )

    def _get_asset_type_name(self, resource: JSON) -> str:
        prefix = 'AWS Elastic Load Balancer-'
        if resource['resourceType'] == 'AWS::ElasticLoadBalancing::LoadBalancer':
            return prefix + 'Classic'
        else:
            return prefix + resource['configuration']['type']

    def _get_ip_addresses(self, availability_zones: JSONs) -> set[Optional[str]]:
        return {
            load_balancer_address.get('ipAddress')
            for availability_zone in availability_zones
            for load_balancer_addresses in availability_zone.get('loadBalancerAddresses', ())
            for load_balancer_address in load_balancer_addresses
        }


class RDSMapper(Mapper):

    def _supported_resource_types(self) -> set[str]:
        return {'AWS::RDS::DBInstance'}

    def map(self, resource: JSON) -> Iterator[InventoryRow]:
        configuration = resource['configuration']
        yield InventoryRow(
            asset_tag=self._get_asset_tag(resource),
            asset_type='AWS RDS Instance',
            hardware_model=configuration['dBInstanceClass'],
            is_public='Yes' if configuration['publiclyAccessible'] else 'No',
            is_virtual='Yes',
            location=resource['awsRegion'],
            network_id=configuration.get('dBSubnetGroup', {}).get('vpcId'),
            software_product_name=f"{configuration['engine']}-{configuration['engineVersion']}",
            software_vendor='AWS',
            system_owner=self._get_owner(resource),
            unique_id=resource['arn'],
        )


class DynamoDbTableMapper(Mapper):

    def _supported_resource_types(self) -> set[str]:
        return {'AWS::DynamoDB::Table'}

    def map(self, resource: JSON) -> Iterator[InventoryRow]:
        yield InventoryRow(
            asset_tag=self._get_asset_tag(resource),
            asset_type='AWS DynamoDB Table',
            is_public='No',
            is_virtual='Yes',
            software_product_name='AWS DynamoDB',
            software_vendor='AWS',
            system_owner=self._get_owner(resource),
            unique_id=resource['arn'],
        )


class S3Mapper(Mapper):

    def _supported_resource_types(self) -> set[str]:
        return {'AWS::S3::Bucket'}

    def map(self, resource: JSON) -> Iterator[InventoryRow]:
        yield InventoryRow(
            asset_tag=self._get_asset_tag(resource),
            asset_type='AWS S3 Bucket',
            comments=self._get_encryption_status(resource),
            is_public='Yes' if self._get_is_public(resource) else 'No',
            is_virtual='Yes',
            location=resource['awsRegion'],
            software_vendor='AWS',
            system_owner=self._get_owner(resource),
            unique_id=resource['arn'],
        )

    def _get_is_public(self, resource: JSON) -> bool:
        try:
            public_access_config = resource['supplementaryConfiguration']['PublicAccessBlockConfiguration']
        except KeyError:
            # If there is no PublicAccessBlockConfiguration then this bucket is public
            return True
        else:
            # The bucket is public if any access blocks are false
            return not all(public_access_config.values())

    def _get_encryption_status(self, resource: JSON) -> str:
        if 'ServerSideEncryptionConfiguration' in resource['supplementaryConfiguration']:
            return 'Encrypted'
        else:
            return 'Not encrypted'


class VPCMapper(Mapper):

    def _supported_resource_types(self) -> set[str]:
        return {'AWS::EC2::VPC'}

    def map(self, resource: JSON) -> Iterator[InventoryRow]:
        yield InventoryRow(
            asset_tag=self._get_asset_tag(resource),
            asset_type='AWS VPC',
            baseline_config=resource['configurationStateId'],
            ip_address=resource['configuration']['cidrBlock'],
            is_public='Yes',
            is_virtual='Yes',
            location=resource['awsRegion'],
            network_id=resource['configuration']['vpcId'],
            software_vendor='AWS',
            system_owner=self._get_owner(resource),
            unique_id=resource['arn'],
        )


class LambdaMapper(Mapper):

    def _supported_resource_types(self) -> set[str]:
        return {'AWS::Lambda::Function'}

    def map(self, resource: JSON) -> Iterator[InventoryRow]:
        configuration = resource['configuration']
        yield InventoryRow(
            asset_tag=self._get_asset_tag(resource),
            asset_type='AWS Lambda Function',
            baseline_config=configuration['runtime'],
            is_public='No',
            is_virtual='Yes',
            location=resource['awsRegion'],
            purpose=configuration.get('description'),
            software_product_name='AWS Lambda',
            software_vendor='AWS',
            system_owner=self._get_owner(resource),
            unique_id=resource['arn'],
        )


class ElasticSearchMapper(Mapper):

    def _supported_resource_types(self) -> set[str]:
        return {'AWS::Elasticsearch::Domain'}

    def map(self, resource: JSON) -> Iterator[InventoryRow]:
        configuration = resource['configuration']
        yield InventoryRow(
            asset_tag=self._get_asset_tag(resource),
            asset_type='AWS OpenSearch Domain',
            baseline_config=configuration['elasticsearchVersion'],
            is_public='No',
            is_virtual='Yes',
            location=resource['awsRegion'],
            network_id=configuration['endpoints'].get('vpc'),
            patch_level=resource.get('serviceSoftwareOptions', {}).get('currentVersion'),
            software_product_name='AWS OpenSearch',
            software_vendor='AWS',
            system_owner=self._get_owner(resource),
            unique_id=resource['arn'],
        )


class NetworkInterfaceMapper(Mapper):

    def _supported_resource_types(self) -> AbstractSet[str]:
        return {'AWS::EC2::NetworkInterface'}

    def map(self, resource: JSON) -> Iterable[InventoryRow]:
        configuration = resource['configuration']
        association = configuration.get('association', {})
        try:
            ip_addresses = [('Yes', association['publicIp'])]
            public_dns_name = association['public_dns_name']
        except KeyError:
            ip_addresses = []
            public_dns_name = None
        ip_addresses.extend(
            ('No', private_ip['privateIpAddress'])
            for private_ip in configuration['privateIpAddresses']
        )
        for is_public, ip_address in ip_addresses:
            yield InventoryRow(
                asset_tag=self._get_asset_tag(resource),
                asset_type='AWS EC2 Network Interface',
                dns_name=public_dns_name,
                ip_address=ip_address,
                is_public=is_public,
                location=resource['awsRegion'],
                mac_address=resource.get('macAddress'),
                network_id=configuration['networkInterfaceId'],
                purpose=configuration.get('description'),
                system_owner=self._get_owner(resource),
                unique_id=resource['arn']
            )


class ElasticIPMapper(Mapper):

    def _supported_resource_types(self) -> AbstractSet[str]:
        return {'AWS::EC2::EIP'}

    def map(self, resource: JSON) -> Iterable[InventoryRow]:
        configuration = resource['configuration']
        for ip, is_public in [
            (configuration['publicIp'], True),
            (configuration['privateIpAddress'], False)
        ]:
            yield InventoryRow(
                asset_tag=self._get_asset_tag(resource),
                asset_type='AWS EC2 Elastic IP',
                ip_address=ip,
                is_public='Yes' if is_public else 'No',
                location=resource['awsRegion'],
                network_id=configuration['networkInterfaceId'],
                system_owner=self._get_owner(resource),
                unique_id=resource['arn']
            )


class DefaultMapper(Mapper):

    def can_map(self, resource: JSON) -> bool:
        return True

    def map(self, resource: JSON) -> Iterable[InventoryRow]:
        yield InventoryRow(
            asset_tag=self._get_asset_tag(resource),
            asset_type=repr(resource['resourceType']),
            location=resource['awsRegion'],
            system_owner=self._get_owner(resource),
            unique_id=resource.get('arn')
        )


class FedRAMPInventoryService:
    default_column_width = 10
    first_writable_row = 6
    report_worksheet_name = 'Inventory'

    @property
    def config(self):
        return aws.client('config')

    @cached_property
    def _mappers(self) -> Sequence[Mapper]:
        current_module = sys.modules[__name__]

        def is_mapper_cls(o: object) -> bool:
            return (
                inspect.isclass(o)
                and not inspect.isabstract(o)
                and issubclass(o, Mapper)
            )

        mapper_clss = [
            mapper_cls
            for name, mapper_cls in inspect.getmembers(current_module, is_mapper_cls)
        ]

        def get_linenno(o: type) -> int:
            src, lineno = inspect.findsource(o)
            return lineno

        mapper_clss.sort(key=get_linenno)
        return [mapper_cls() for mapper_cls in mapper_clss]

    def get_resources(self) -> Iterator[JSON]:
        fields = [
            'arn',
            'resourceName',
            'resourceId',
            'resourceType',
            'configuration',
            'supplementaryConfiguration',
            'configurationStateId',
            'tags',
            'awsRegion'
        ]
        order_fields = [
            'resourceType',
            'resourceName',
            'resourceId'
        ]

        def join(fields):
            return ', '.join(map(repr, fields))

        query = f"SELECT {join(fields)} ORDER BY {join(order_fields)}"
        next_token = ''
        while next_token is not None:
            # FIXME: FedRAMP resource inventory does not cover all regions
            #        https://github.com/DataBiosphere/azul/issues/5025
            response = self.config.select_resource_config(Expression=query,
                                                          NextToken=next_token)
            resources = response.get('Results', [])
            log.debug('Got page of %d resources', len(resources))
            for resource in resources:
                yield json.loads(resource)
            next_token = response.get('NextToken')

    def get_inventory(self, resources: Iterable[JSON]):
        rows_by_mapper: defaultdict[Mapper, list[InventoryRow]] = defaultdict(list)
        resource_counts = Counter()
        row_counts = Counter()
        for resource in resources:
            resource_type = resource['resourceType']
            mapper = self._get_mapper(resource)
            log.debug('Mapping %r resource using %r',
                      resource_type, type(mapper).__name__)
            rows = sorted(mapper.map(resource), key=attrgetter('asset_tag', 'ip_address'))
            log.debug('Mapped to %d rows', len(rows))
            resource_counts[resource_type] += 1
            row_counts[resource_type] += len(rows)
            rows_by_mapper[mapper].extend(rows)

        log.info('Inventory contents:')
        print(f'\n{"Resource type":<42s}'
              f'{"# resources":<20s}'
              f'{"# rows":<20s}\n')
        for resource_type in resource_counts.keys():
            print(f'{resource_type:<42s}'
                  f'{resource_counts[resource_type]:>15d}'
                  f'{row_counts[resource_type]:>10d}')

        return flatten(rows_by_mapper[mapper] for mapper in self._mappers)

    def write_report(self,
                     inventory: Iterable[InventoryRow],
                     template_path: pathlib.Path,
                     output_path: pathlib.Path
                     ) -> None:
        workbook = openpyxl.load_workbook(template_path)
        worksheet = workbook[self.report_worksheet_name]
        for row_number, row in enumerate(inventory, start=self.first_writable_row):
            row = attr.astuple(row)
            for column_number, value in enumerate(row, start=1):
                self._write_cell_if_value_provided(worksheet,
                                                   column=column_number,
                                                   row=row_number,
                                                   value=value)
        workbook.save(output_path)
        log.info('Wrote report to %s', output_path)

    def update_wiki(self,
                    project: gitlab.v4.objects.projects.Project,
                    page_name: str,
                    resources: Iterable[JSON],
                    ) -> None:
        content = self._wiki_content(resources)
        try:
            page = project.wikis.get(page_name)
        except gitlab.exceptions.GitlabError as e:
            if e.response_code == 404:
                log.info('Wiki page %r not found', page_name)
                project.wikis.create({
                    'title': page_name,
                    'content': content
                })
                log.info('Created wiki page %r (character count: %d)',
                         page_name, len(content))
            else:
                raise
        else:
            old_length = len(page.content)
            page.content = content
            page.save()
            log.info('Updated wiki page %r (character count: %d -> %d)',
                     page_name, old_length, len(content))

    def _get_mapper(self, resource: JSON) -> Mapper:
        return next(
            mapper
            for mapper in self._mappers
            if mapper.can_map(resource)
        )

    def _write_cell_if_value_provided(self,
                                      worksheet: Worksheet,
                                      column: int,
                                      row: int,
                                      value: Optional[str]
                                      ) -> None:
        if value:
            # Scale the size of the column with the input value if necessary.
            # By default, width is None.
            dimensions = worksheet.column_dimensions[get_column_letter(column)]
            if dimensions.width is None:
                dimensions.width = self.default_column_width
            else:
                dimensions.width = max(dimensions.width, len(value))
            worksheet.cell(column=column, row=row, value=value)

    def _wiki_content(self, resources: Iterable[JSON]) -> str:
        return '\n\n'.join(
            f'```\n{json.dumps(resource, indent=4)}\n```'
            for resource in resources
        )
