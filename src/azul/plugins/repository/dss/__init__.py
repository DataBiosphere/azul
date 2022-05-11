from collections.abc import (
    Set,
)
import logging
import time
from typing import (
    Optional,
    Sequence,
    Type,
    cast,
)
import urllib
from urllib.parse import (
    quote,
)
from uuid import (
    UUID,
    uuid5,
)

from deprecated import (
    deprecated,
)
from furl import (
    furl,
)
from humancellatlas.data.metadata.helpers.dss import (
    download_bundle_metadata,
)
from more_itertools import (
    one,
)
import requests

from azul import (
    CatalogName,
    cached_property,
    config,
)
from azul.auth import (
    Authentication,
)
from azul.collections import (
    adict,
)
from azul.deployment import (
    aws,
)
from azul.dss import (
    client,
    direct_access_client,
)
from azul.indexer import (
    Bundle,
    SimpleSourceSpec,
    SourceRef,
    SourcedBundleFQID,
)
from azul.plugins import (
    RepositoryFileDownload,
    RepositoryPlugin,
)
from azul.types import (
    JSON,
    MutableJSON,
    MutableJSONs,
)
from azul.uuids import (
    validate_uuid_prefix,
)

log = logging.getLogger(__name__)


class DSSSourceRef(SourceRef[SimpleSourceSpec, 'DSSSourceRef']):
    """
    Subclass of `Source` to create new namespace for source IDs.
    """
    namespace: UUID = UUID('6925391e-6519-41d9-879f-c6307eb83c1c')

    @classmethod
    def for_dss_source(cls, source: str):
        # We hash the endpoint instead of using it verbatim to distinguish them
        # within a document, which is helpful for testing.
        spec = SimpleSourceSpec.parse(source)
        return cls(id=cls.id_from_spec(spec), spec=spec)

    @classmethod
    def id_from_spec(cls, spec: SimpleSourceSpec) -> str:
        return str(uuid5(cls.namespace, spec.name))


DSSBundleFQID = SourcedBundleFQID[DSSSourceRef]


class Plugin(RepositoryPlugin[SimpleSourceSpec, DSSSourceRef]):

    @classmethod
    def create(cls, catalog: CatalogName) -> RepositoryPlugin:
        return cls()

    @property
    def sources(self) -> Set[SimpleSourceSpec]:
        assert config.dss_source is not None
        return {
            SimpleSourceSpec.parse(config.dss_source)
        }

    def lookup_source_id(self, spec: SimpleSourceSpec) -> str:
        return DSSSourceRef.id_from_spec(spec)

    def list_sources(self,
                     authentication: Optional[Authentication]
                     ) -> list[DSSSourceRef]:
        return [
            DSSSourceRef(id=self.lookup_source_id(spec), spec=spec)
            for spec in self.sources
        ]

    @cached_property
    def dss_client(self):
        return client(dss_endpoint=config.dss_endpoint)

    def list_bundles(self, source: DSSSourceRef, prefix: str) -> list[DSSBundleFQID]:
        self._assert_source(source)
        prefix = source.spec.prefix.common + prefix
        validate_uuid_prefix(prefix)
        log.info('Listing bundles with prefix %r in source %r.', prefix, source)
        bundle_fqids = []
        response = self.dss_client.get_bundles_all.iterate(prefix=prefix,
                                                           replica='aws',
                                                           per_page=500)
        for bundle in response:
            bundle_fqids.append(SourcedBundleFQID(source=source,
                                                  uuid=bundle['uuid'],
                                                  version=bundle['version']))
        log.info('There are %i bundle(s) with prefix %r in source %r.',
                 len(bundle_fqids), prefix, source)
        return bundle_fqids

    @deprecated
    def fetch_bundle_manifest(self, bundle_fqid: DSSBundleFQID) -> MutableJSONs:
        """
        Only used by integration test to filter out bad bundles.

        https://github.com/DataBiosphere/azul/issues/1784 should make this
        unnecessary in DCP/2.

        See Bundle.manifest for the shape of the return value.
        """
        # noinspection PyProtectedMember
        response = self.dss_client.get_bundle._auto_page(uuid=bundle_fqid.uuid,
                                                         version=bundle_fqid.version,
                                                         replica='aws')
        return response['bundle']['files']

    def fetch_bundle(self, bundle_fqid: DSSBundleFQID) -> Bundle:
        self._assert_source(bundle_fqid.source)
        now = time.time()
        # One client per invocation. That's OK because the client will be used
        # for many requests and a typical lambda invocation calls this only once.
        dss_client = direct_access_client(num_workers=config.num_dss_workers)
        version, manifest, metadata_files = download_bundle_metadata(
            client=dss_client,
            replica='aws',
            uuid=bundle_fqid.uuid,
            version=bundle_fqid.version,
            num_workers=config.num_dss_workers
        )
        bundle = DSSBundle(fqid=bundle_fqid,
                           # FIXME: remove need for cast by fixing declaration in metadata API
                           #        https://github.com/DataBiosphere/hca-metadata-api/issues/13
                           manifest=cast(MutableJSONs, manifest),
                           metadata_files=cast(MutableJSON, metadata_files))
        assert version == bundle.version
        log.info("It took %.003fs to download bundle %s.%s",
                 time.time() - now, bundle.uuid, bundle.version)
        return bundle

    def dss_subscription_query(self, prefix: str) -> JSON:
        return {
            "query": {
                "bool": {
                    "must_not": [
                        {
                            "term": {
                                "admin_deleted": True
                            }
                        }
                    ],
                    "must": [
                        {
                            "exists": {
                                "field": "files.project_json"
                            }
                        },
                        *self._prefix_clause(prefix)
                    ]
                }
            }
        }

    def dss_deletion_subscription_query(self, prefix: str) -> JSON:
        return {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "admin_deleted": True
                            }
                        },
                        *self._prefix_clause(prefix)
                    ]
                }
            }
        }

    def _prefix_clause(self, prefix):
        return [
            {
                'prefix': {
                    'uuid': prefix
                }
            }
        ] if prefix else []

    def portal_db(self) -> Sequence[JSON]:
        """
        A hardcoded example database for use during development of the integrations API implementation
        """
        return [
            {
                "portal_id": "7bc432a6-0fcf-4c19-b6e6-4cb6231279b3",
                "portal_name": "Terra Portal",
                "portal_icon": "https://app.terra.bio/favicon.png",
                "contact_email": "",
                "organization_name": "Broad Institute",
                "portal_description": "Terra is a cloud-native platform for biomedical researchers to access data, "
                                      "run analysis tools, and collaborate.",
                "integrations": [
                    {
                        "integration_id": "b87b7f30-2e60-4ca5-9a6f-00ebfcd35f35",
                        "integration_type": "get_manifest",
                        "entity_type": "file",
                        "title": "Populate a Terra workspace with data files matching the current filter selection",
                        "manifest_type": "full",
                        "portal_url_template": "https://app.terra.bio/#import-data?url={manifest_url}"
                    }
                ]
            },
            {
                "portal_id": "9852dece-443d-42e8-869c-17b9a86d447e",
                "portal_name": "Single Cell Portal",
                "portal_icon": "https://singlecell.broadinstitute.org/single_cell/assets/"
                               "scp_favicon-1e5be59fdd577f7e7e275109b800364728b01b4ffc54a41e9e32117f3d5d9aa6.ico",
                "contact_email": "",
                "organization_name": "Broad Institute",
                "portal_description": "Reducing barriers and accelerating single-cell research.",
                "integrations": [
                    {
                        "integration_id": "977854a0-2eea-4fec-9459-d4807fe79f0c",
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": "Visualize in SCP",
                        "entity_ids": {
                            "dev": ["bc2229e7-e330-435a-8c2a-4275741f2c2d"],
                            "staging": ["bc2229e7-e330-435a-8c2a-4275741f2c2d", "259f9041-b72f-45ce-894d-b645add2e620"],
                            "integration": ["bc2229e7-e330-435a-8c2a-4275741f2c2d"],
                            "prod": ["c4077b3c-5c98-4d26-a614-246d12c2e5d7"]
                        },
                        "portal_url": "https://singlecell.broadinstitute.org/single_cell/study/SCP495"
                    },
                    # https://docs.google.com/document/d/1HBOPe6h_RjxltfbPenKsNmoN3MVAtkhVKJ_LGG1DBkA/edit#
                    # https://github.com/HumanCellAtlas/data-browser/issues/545#issuecomment-528092658
                    # {
                    #     "integration_id": "f62f5202-55c3-4dfa-bedd-ba4d2c4fb6c9",
                    #     "integration_type": "get_entity",
                    #     "entity_type": "project",
                    #     "title": "",
                    #     "allow_head": False,
                    #     "portal_url_template": "https://singlecell.broadinstitute.org/hca-project/{entity_id}"
                    # }
                ]
            },
            {
                "portal_id": "f58bdc5e-98cd-4df4-80a4-7372dc035e87",
                "portal_name": "Single Cell Expression Atlas",
                "portal_icon": "https://ebi.emblstatic.net/web_guidelines/EBI-Framework/v1.3/images/logos/EMBL-EBI/"
                               "favicons/favicon.ico",
                "contact_email": "Irene Papatheodorou irenep@ebi.ac.uk",
                "organization_name": "European Bioinformatics Institute",
                "portal_description": "Single Cell Expression Atlas annotates publicly available single cell "
                                      "RNA-Seq experiments with ontology identifiers and re-analyses them using "
                                      "standardised pipelines available through SCXA-Workflows, our collection of "
                                      "RNA-Seq analysis pipelines, which is available at "
                                      "https://github.com/ebi-gene-expression-group/scxa-workflows . The browser "
                                      "enables visualisation of clusters of cells, their annotations and supports "
                                      "searches for gene expression within and across studies.",
                "integrations": [
                    {
                        "integration_id": "dbfe9394-a326-4574-9632-fbadb51a7b1a",
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": "Single-cell transcriptome analysis of precursors of human CD4+ "
                                 "cytotoxic T lymphocytes",
                        "entity_ids": {
                            "staging": ["519b58ef-6462-4ed3-8c0d-375b54f53c31"],
                            "integration": ["90bd6933-40c0-48d4-8d76-778c103bf545"],
                            "prod": ["90bd6933-40c0-48d4-8d76-778c103bf545"]
                        },
                        "portal_url": "https://www.ebi.ac.uk/gxa/sc/experiments/E-GEOD-106540/results/tsne"
                    },
                    {
                        "integration_id": "081a6a90-29b6-4100-9c42-17a50014ea03",
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": "Reconstructing the human first trimester fetal-maternal interface using single cell "
                                 "transcriptomics - 10x data",
                        "entity_ids": {
                            "staging": [],
                            "integration": ["f83165c5-e2ea-4d15-a5cf-33f3550bffde"],
                            "prod": ["f83165c5-e2ea-4d15-a5cf-33f3550bffde"]
                        },
                        "portal_url": "https://www.ebi.ac.uk/gxa/sc/experiments/E-MTAB-6701/results/tsne"
                    },
                    {
                        "integration_id": "f0886c45-e339-4f22-8f6b-a715db1943e3",
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": "Reconstructing the human first trimester fetal-maternal interface using single cell "
                                 "transcriptomics - Smartseq 2 data",
                        "entity_ids": {
                            "staging": [],
                            "integration": ["f83165c5-e2ea-4d15-a5cf-33f3550bffde"],
                            "prod": ["f83165c5-e2ea-4d15-a5cf-33f3550bffde"]
                        },
                        "portal_url": "https://www.ebi.ac.uk/gxa/sc/experiments/E-MTAB-6678/results/tsne"
                    },
                    {
                        "integration_id": "f13ddf2d-d913-492b-9ea8-2de4b1881c26",
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": "Single cell transcriptome analysis of human pancreas",
                        "entity_ids": {
                            "staging": ["b1f3afcb-f061-4862-b6c2-ace971595d22", "08e7b6ba-5825-47e9-be2d-7978533c5f8c"],
                            "integration": ["cddab57b-6868-4be4-806f-395ed9dd635a"],
                            "prod": ["cddab57b-6868-4be4-806f-395ed9dd635a"]
                        },
                        "portal_url": "https://www.ebi.ac.uk/gxa/sc/experiments/E-GEOD-81547/results/tsne"
                    },
                    {
                        "integration_id": "5ef44133-e71f-4f52-893b-3b200d5fb99b",
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": "Single-cell RNA-seq analysis of 1,732 cells throughout a 125-day differentiation "
                                 "protocol that converted H1 human embryonic stem cells to a variety of "
                                 "ventrally-derived cell types",
                        "entity_ids": {
                            "staging": ["019a935b-ea35-4d83-be75-e1a688179328"],
                            "integration": ["2043c65a-1cf8-4828-a656-9e247d4e64f1"],
                            "prod": ["2043c65a-1cf8-4828-a656-9e247d4e64f1"]
                        },
                        "portal_url": "https://www.ebi.ac.uk/gxa/sc/experiments/E-GEOD-93593/results/tsne"
                    },
                    {
                        "integration_id": "d43464c0-38c6-402d-bdec-8972d71005c5 ",
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": "Single-cell RNA-seq analysis of human pancreas from healthy individuals and type 2 "
                                 "diabetes patients",
                        "entity_ids": {
                            "staging": ["a5ae0428-476c-46d2-a9f2-aad955b149aa"],
                            "integration": ["ae71be1d-ddd8-4feb-9bed-24c3ddb6e1ad"],
                            "prod": ["ae71be1d-ddd8-4feb-9bed-24c3ddb6e1ad"]
                        },
                        "portal_url": "https://www.ebi.ac.uk/gxa/sc/experiments/E-MTAB-5061/results/tsne"
                    },
                    {
                        "integration_id": "60912ae7-e88f-48bf-8b33-27daccade2b6",
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": "Single-cell RNA-seq analysis of 20 organs and tissues from individual mice creates "
                                 "a Tabula muris",
                        "entity_ids": {
                            "staging": ["2cd14cf5-f8e0-4c97-91a2-9e8957f41ea8"],
                            "integration": ["e0009214-c0a0-4a7b-96e2-d6a83e966ce0"],
                            "prod": ["e0009214-c0a0-4a7b-96e2-d6a83e966ce0"]
                        },
                        "portal_url": "https://www.ebi.ac.uk/gxa/sc/experiments/E-ENAD-15/results/tsne"
                    },
                ],
            },
            {
                "portal_id": "2e05f611-16fb-4bf3-b860-aa500f0256de",
                "portal_name": "Xena",
                "portal_icon": "https://xena.ucsc.edu/icons-9ac0cb8372f662ad72d747b981120f73/favicon.ico",
                "contact_email": "",
                "organization_name": "UCSC",
                "portal_description": "",
                "integrations": [
                    {
                        "integration_id": integration_id,
                        "integration_type": "get",
                        "entity_type": "project",
                        "title": title,
                        "entity_ids": {
                            "staging": [],
                            "integration": [project_uuid],
                            "prod": [project_uuid]
                        },
                        "portal_url": "https://singlecell.xenabrowser.net/datapages/?cohort=" + quote(title)
                    } for integration_id, project_uuid, title in (
                        # @formatter:off
                        ("73aa70fe-e40a-48da-9fa4-bea4c4d2ae74", "4a95101c-9ffc-4f30-a809-f04518a23803", "HCA Human Tissue T cell Activation"),  # noqa E501
                        ("c36e46c2-34d6-4129-853b-60256bc0af8d", "8185730f-4113-40d3-9cc3-929271784c2b", "HCA Adult Retina (Wong)"),  # noqa E501
                        ("dedb2f00-b92f-4f81-8633-6f58edcbf3f7", "005d611a-14d5-4fbf-846e-571a1f874f70", "HCA HPSI human cerebral organoids"),  # noqa E501
                        ("ced58994-05c2-4a2d-87b1-fff4faf2ca93", "cc95ff89-2e68-4a08-a234-480eca21ce79", "HCA Census of Immune Cells"),  # noqa E501
                        ("65e1465e-0641-4770-abbb-fde8bc0582aa", "4d6f6c96-2a83-43d8-8fe1-0f53bffd4674", "HCA Single Cell Liver Landscape"),  # noqa E501
                        ("39c69c7d-a245-4460-a045-6bd055564cca", "c4077b3c-5c98-4d26-a614-246d12c2e5d7", "HCA Tissue stability"),  # noqa E501
                        ("9cd98133-1a86-4937-94dc-4e9a68a36192", "091cf39b-01bc-42e5-9437-f419a66c8a45", "HCA Human Hematopoietic Profiling"),  # noqa E501
                        ("0f760ab0-2f81-40c8-a838-6a6c508bdd59", "f83165c5-e2ea-4d15-a5cf-33f3550bffde", "HCA Fetal Maternal Interface"),  # noqa E501
                        ("23ce54c0-58b5-4617-9b61-53ac020c1087", "cddab57b-6868-4be4-806f-395ed9dd635a", "HCA Human Pancreas"),  # noqa E501
                        ("980577ba-02d5-4edb-9662-24f2d1dc351a", "2043c65a-1cf8-4828-a656-9e247d4e64f1", "HCA Human Interneuron Development"),  # noqa E501
                        ("1dae24a2-998e-47d3-a570-26c36f2b073e", "abe1a013-af7a-45ed-8c26-f3793c24a1f4", "HCA Kidney Single Cell Atlas"),  # noqa E501
                        ("66cff7c4-8e79-4e84-a269-2c0970b49392", "f8aa201c-4ff1-45a4-890e-840d63459ca2", "HCA Human Colonic Mesenchyme IBD")  # noqa E501
                        # @formatter:on
                    )
                ]
            }
        ]

    def drs_uri(self, drs_path: Optional[str]) -> Optional[str]:
        if drs_path is None:
            return None
        else:
            netloc = config.drs_domain or config.api_lambda_domain('service')
            return f'drs://{netloc}/{drs_path}'

    def _direct_file_url(self,
                         file_uuid: str,
                         *,
                         file_version: Optional[str] = None,
                         replica: Optional[str] = None,
                         token: Optional[str] = None,
                         ) -> Optional[str]:
        dss_endpoint = one(self.sources).name
        url = furl(dss_endpoint)
        url.path.add(['files', file_uuid])
        url.query.add(adict(version=file_version, replica=replica, token=token))
        return str(url)

    def file_download_class(self) -> Type[RepositoryFileDownload]:
        return DSSFileDownload


class DSSFileDownload(RepositoryFileDownload):
    _location: Optional[str] = None
    _retry_after: Optional[int] = None

    def update(self,
               plugin: RepositoryPlugin,
               authentication: Optional[Authentication]
               ) -> None:
        self.drs_path = None  # to shorten the retry URLs
        if self.replica is None:
            self.replica = 'aws'
        assert isinstance(plugin, Plugin)
        dss_url = plugin._direct_file_url(file_uuid=self.file_uuid,
                                          file_version=self.file_version,
                                          replica=self.replica,
                                          token=self.token)
        dss_response = requests.get(dss_url, allow_redirects=False)
        if dss_response.status_code == 301:
            retry_after = int(dss_response.headers.get('Retry-After'))
            location = dss_response.headers['Location']

            location = urllib.parse.urlparse(location)
            query = urllib.parse.parse_qs(location.query, strict_parsing=True)
            self.token = one(query['token'])
            self.replica = one(query['replica'])
            self.file_version = one(query['version'])
            self._retry_after = retry_after
        elif dss_response.status_code == 302:
            location = dss_response.headers['Location']
            # Remove once https://github.com/HumanCellAtlas/data-store/issues/1837 is resolved
            if True:
                location = urllib.parse.urlparse(location)
                query = urllib.parse.parse_qs(location.query, strict_parsing=True)
                expires = int(one(query['Expires']))
                bucket = location.netloc.partition('.')[0]
                dss_endpoint = one(plugin.sources).name
                assert bucket == aws.dss_checkout_bucket(dss_endpoint), bucket
                with aws.direct_access_credentials(dss_endpoint, lambda_name='service'):
                    # FIXME: make region configurable (https://github.com/DataBiosphere/azul/issues/1560)
                    s3 = aws.client('s3', region_name='us-east-1')
                    params = {
                        'Bucket': bucket,
                        'Key': location.path[1:],
                        'ResponseContentDisposition': 'attachment;filename=' + self.file_name,
                    }
                    location = s3.generate_presigned_url(ClientMethod=s3.get_object.__name__,
                                                         ExpiresIn=round(expires - time.time()),
                                                         Params=params)
            self._location = location
        else:
            dss_response.raise_for_status()
            assert False

    @property
    def location(self) -> Optional[str]:
        return self._location

    @property
    def retry_after(self) -> Optional[int]:
        return self._retry_after


class DSSBundle(Bundle[DSSSourceRef]):

    def drs_path(self, manifest_entry: JSON) -> str:
        file_uuid = manifest_entry['uuid']
        file_version = manifest_entry['version']
        return str(furl(path=(file_uuid,), args={'version': file_version}))
