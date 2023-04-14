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
from furl import (
    furl,
)
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
    config,
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


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class ResourceConfig:
    arn: str
    region: str
    id: str
    type: str
    config: JSON
    supplementary_config: JSON

    @classmethod
    def from_response(cls, response: dict) -> 'ResourceConfig':
        print(response.keys())
        return cls(
            arn=response['arn'],
            region=response['awsRegion'],
            type=response['resourceType'],
            id=response['resourceId'],
            config=json.loads(response['configuration']),
            supplementary_config=response['supplementaryConfiguration']
        )


null_str = Optional[str]


class YesNo:
    yes = 'Yes'
    no = 'No'

    @classmethod
    def from_bool(cls, b: bool) -> str:
        return cls.yes if b else cls.no


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
    def map(self, resource: ResourceConfig) -> Iterable[InventoryRow]:
        raise NotImplementedError

    def _common_fields(self, resource: ResourceConfig, *, id_suffix: Optional[str] = None) -> dict:
        return dict(
            asset_tag=resource.id,
            location=resource.region,
            software_vendor='AWS',
            system_owner=config.owner,
            unique_id=resource.arn + ('' if id_suffix is None else f'/{id_suffix}')
        )

    def _supported_resource_types(self) -> AbstractSet[str]:
        return frozenset()

    def can_map(self, resource: ResourceConfig) -> bool:
        return resource.type in self._supported_resource_types()

    def _get_polymorphic_key(self, json: JSON, *keys: str) -> AnyJSON:
        for key in keys:
            try:
                return json[key]
            except KeyError:
                pass
        raise KeyError(*keys)

    def _get_asset_tag(self, resource: JSON) -> str:
        return self._get_polymorphic_key(resource, 'resourceName', 'resourceId')


class LambdaMapper(Mapper):

    def _supported_resource_types(self) -> set[str]:
        return {'AWS::Lambda::Function'}

    def map(self, resource: ResourceConfig) -> Iterator[InventoryRow]:
        yield InventoryRow(
            asset_type='AWS Lambda Function',
            baseline_config=resource.config['runtime'],
            is_public=YesNo.no,
            is_virtual=YesNo.yes,
            purpose=resource.config.get('description'),
            software_product_name='AWS Lambda',
            **self._common_fields(resource)
        )


class ElasticSearchMapper(Mapper):

    def _supported_resource_types(self) -> set[str]:
        return {'AWS::Elasticsearch::Domain'}

    def map(self, resource: ResourceConfig) -> Iterator[InventoryRow]:
        yield InventoryRow(
            asset_type='AWS OpenSearch Domain',
            baseline_config=resource.config['elasticsearchVersion'],
            is_public=YesNo.no,
            is_virtual=YesNo.yes,
            network_id=resource.config['endpoints'].get('vpc'),
            patch_level=resource.get('serviceSoftwareOptions', {}).get('currentVersion'),
            software_product_name='AWS OpenSearch',
            **self._common_fields(resource)
        )


class EC2Mapper(Mapper):

    def _supported_resource_types(self) -> set[str]:
        return {'AWS::EC2::Instance'}

    def map(self, resource: ResourceConfig) -> Iterable[InventoryRow]:
        dns_name = resource.config.get('publicDnsName')
        if dns_name:
            is_public = YesNo.yes
        else:
            is_public = YesNo.no
        for nic in resource.config['networkInterfaces']:
            for ip_addresses in nic['privateIpAddresses']:
                for ip_address_path in [('privateIpAddress',), ('association', 'publicIp')]:
                    try:
                        ip_address = self._get_ip_address(ip_addresses, ip_address_path)
                    except KeyError:
                        continue
                    else:
                        yield InventoryRow(
                            asset_type='AWS EC2 Instance',
                            authenticated_scan_planned=YesNo.yes,
                            baseline_config=resource.config['imageId'],
                            dns_name=dns_name,
                            hardware_model=resource.config['instanceType'],
                            ip_address=ip_address,
                            is_public=is_public,
                            is_virtual=YesNo.yes,
                            mac_address=nic['macAddress'],
                            network_id=resource['configuration']['vpcId'],
                            **self._common_fields(resource, id_suffix=ip_address)
                        )

    def _get_ip_address(self, ip_addresses: JSON, keys) -> str:
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
                dns_name=configuration['dNSName'],
                ip_address=ip_address,
                is_public=YesNo.from_bool(configuration['scheme'] == 'internet-facing'),
                is_virtual=YesNo.yes,
                **self._polymorphic_fields(resource),
                **self._common_fields(resource, id_suffix=ip_address)
            )

    def _polymorphic_fields(self, resource: JSON) -> dict[str, str]:
        # Classic ELBs have key of 'vpcid' while V2 ELBs have key of 'vpcId'
        prefix = 'AWS Elastic Load Balancer-'
        if resource['resourceType'] == 'AWS::ElasticLoadBalancing::LoadBalancer':
            asset_type = prefix + 'Classic'
            network_id = resource['configuration']['vpcid']
        else:
            asset_type = prefix + resource['configuration']['type']
            network_id = resource['configuration']['vpcId']
        return dict(asset_type=asset_type, network_id=network_id)

    def _get_ip_addresses(self, availability_zones: JSONs) -> set[Optional[str]]:
        return {
            load_balancer_address.get('ipAddress')
            for availability_zone in availability_zones
            for load_balancer_addresses in availability_zone.get('loadBalancerAddresses', ())
            for load_balancer_address in load_balancer_addresses
        }


class NetworkInterfaceMapper(Mapper):

    def _supported_resource_types(self) -> AbstractSet[str]:
        return {'AWS::EC2::NetworkInterface'}

    def map(self, resource: ResourceConfig) -> Iterable[InventoryRow]:
        association = resource.config.get('association', {})
        try:
            ip_addresses = [(YesNo.yes, association['publicIp'])]
            public_dns_name = association['publicDnsName']
        except KeyError:
            ip_addresses = []
            public_dns_name = None
        ip_addresses.extend(
            (YesNo.no, private_ip['privateIpAddress'])
            for private_ip in resource.config['privateIpAddresses']
        )
        for is_public, ip_address in ip_addresses:
            yield InventoryRow(
                asset_type='AWS EC2 Network Interface',
                dns_name=public_dns_name,
                ip_address=ip_address,
                is_public=is_public,
                mac_address=resource.config.get('macAddress'),
                network_id=resource.config['subnetId'],
                purpose=resource.config.get('description'),
                **self._common_fields(resource, id_suffix=ip_address)
            )


class S3Mapper(Mapper):

    def _supported_resource_types(self) -> set[str]:
        return {'AWS::S3::Bucket'}

    def map(self, resource: ResourceConfig) -> Iterator[InventoryRow]:
        yield InventoryRow(
            asset_type='AWS S3 Bucket',
            comments=self._get_encryption_status(resource),
            is_public=YesNo.from_bool(self._get_is_public(resource)),
            is_virtual=YesNo.yes,
            **self._common_fields(resource)
        )

    def _get_is_public(self, resource: ResourceConfig) -> bool:
        try:
            public_access_config = resource.supplementary_config['PublicAccessBlockConfiguration']
        except KeyError:
            # If there is no PublicAccessBlockConfiguration then this bucket is public
            return True
        else:
            # The bucket is public if any access blocks are false
            return not all(public_access_config.values())

    def _get_encryption_status(self, resource: ResourceConfig) -> str:
        if 'ServerSideEncryptionConfiguration' in resource.supplementary_config:
            return 'Encrypted'
        else:
            return 'Not encrypted'


class DynamoDbTableMapper(Mapper):

    def _supported_resource_types(self) -> set[str]:
        return {'AWS::DynamoDB::Table'}

    def map(self, resource: ResourceConfig) -> Iterator[InventoryRow]:
        yield InventoryRow(
            asset_type='AWS DynamoDB Table',
            is_public=YesNo.no,
            is_virtual=YesNo.yes,
            software_product_name='AWS DynamoDB',
            **self._common_fields(resource)
        )


class ElasticIPMapper(Mapper):

    def _supported_resource_types(self) -> AbstractSet[str]:
        return {'AWS::EC2::EIP'}

    def map(self, resource: ResourceConfig) -> Iterable[InventoryRow]:
        for ip, is_public in [
            (resource.config['publicIp'], YesNo.yes),
            (resource.config['privateIpAddress'], YesNo.no)
        ]:
            yield InventoryRow(
                asset_type='AWS EC2 Elastic IP',
                ip_address=ip,
                is_public=is_public,
                network_id=resource.config['networkInterfaceId'],
                **self._common_fields(resource, id_suffix=ip)
            )


class RDSMapper(Mapper):

    def _supported_resource_types(self) -> set[str]:
        return {'AWS::RDS::DBInstance'}

    def map(self, resource: ResourceConfig) -> Iterator[InventoryRow]:
        yield InventoryRow(
            asset_type='AWS RDS Instance',
            hardware_model=resource.config['dBInstanceClass'],
            is_public=YesNo.from_bool(resource.config['publiclyAccessible']),
            is_virtual=YesNo.yes,
            network_id=resource.config.get('dBSubnetGroup', {}).get('vpcId'),
            software_product_name=f"{resource.config['engine']}-{resource.config['engineVersion']}",
            **self._common_fields(resource)
        )


class VPCMapper(Mapper):

    def _supported_resource_types(self) -> set[str]:
        return {'AWS::EC2::VPC'}

    def map(self, resource: ResourceConfig) -> Iterator[InventoryRow]:
        yield InventoryRow(
            asset_type='AWS VPC',
            baseline_config=resource['configurationStateId'],
            ip_address=resource.config['cidrBlock'],
            is_public=YesNo.yes,
            is_virtual=YesNo.yes,
            network_id=resource.config['vpcId'],
            **self._common_fields(resource)
        )


class ACMCertificateMapper(Mapper):

    def _supported_resource_types(self) -> AbstractSet[str]:
        return {'AWS::ACM::Certificate'}

    def map(self, resource: ResourceConfig) -> Iterable[InventoryRow]:
        yield InventoryRow(
            asset_type='AWS ACM Certificate',
            **self._common_fields(resource)
        )
        for user in resource.config['inUseBy']:
            parts, id = user.split('/', 1)
            parts = parts.split(':')
            if parts[:2] == ['aws', 'clientvpn']:
                _, resource_type, region, stage = parts
                url = '.'.join([id, stage, resource_type, region, 'amazonaws.com'])
                yield InventoryRow(
                    asset_tag=user,
                    asset_type='AWS Client VPN',
                    dns_name=url,
                    location=region,
                    software_vendor='AWS',
                    unique_id=url + ':443',
                )


class ResourceComplianceMapper(Mapper):

    def _supported_resource_types(self) -> AbstractSet[str]:
        return {'AWS::Config::ResourceCompliance'}

    def map(self, resource: ResourceConfig) -> Iterable[InventoryRow]:
        # Intentionally omit rows for this resource type
        return ()


class DefaultMapper(Mapper):

    def can_map(self, resource: ResourceConfig) -> bool:
        return True

    def map(self, resource: ResourceConfig) -> Iterable[InventoryRow]:
        yield InventoryRow(
            asset_type=resource.type,
            **self._common_fields(resource)
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

    def get_inventory(self, resources: Iterable[JSON]) -> Iterable[InventoryRow]:
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

    def get_synthetic_inventory(self) -> Iterable[InventoryRow]:
        data_browser_url = furl(scheme='https', netloc=config.data_browser_domain)
        yield InventoryRow(
            asset_type='Application endpoint',
            dns_name=str(data_browser_url),
            is_public=YesNo.yes,
            purpose='UI for external users',
            software_vendor='UCSC',
            system_owner=config.owner,
            unique_id='Data Browser UI',
        )
        yield InventoryRow(
            asset_type='Service endpoint',
            dns_name=str(config.service_endpoint),
            is_public=YesNo.from_bool(not config.private_api),
            purpose='Service API (backend for Data Browser UI, programmatic use by external users)',
            software_vendor='UCSC',
            system_owner=config.owner,
            unique_id='Service REST API',
        )
        yield InventoryRow(
            asset_type='Application endpoint',
            dns_name=str(config.indexer_endpoint),
            is_public=YesNo.from_bool(not config.private_api),
            purpose='Indexer API (primarily for internal users)',
            software_vendor='UCSC',
            system_owner=config.owner,
            unique_id='Indexer API',
        )

        for unique_id, purpose, port, scheme in [
            ('GitLab UI', 'CI/CD (internal users only)', None, 'https'),
            ('GitLab SSH', 'CI/CD (system administrators only)', 2222, 'ssh'),
            ('GitLab Git', 'Source repository for CI/CD (internal users only)', 22, 'git+ssh')
        ]:
            gitlab_url = furl(scheme=scheme,
                              host=f'gitlab.{config.domain_name}',
                              port=port)
            yield InventoryRow(
                asset_type='Service endpoint',
                dns_name=str(gitlab_url),
                is_public=YesNo.no,
                software_vendor='GitLab',
                system_owner=config.owner,
                purpose=purpose,
                unique_id=unique_id,
            )

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

    def _get_mapper(self, resource: ResourceConfig) -> Mapper:
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
