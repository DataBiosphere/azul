from collections.abc import (
    Mapping,
)
import json
from typing import (
    Optional,
)


def partition_prefix_length(n: int) -> int:
    """
    For a given number of subgraphs, return a partition prefix length that is
    expected to rarely exceed 512 subgraphs per partition.

    >>> [partition_prefix_length(n) for n in (0, 1, 512, 513, 16 * 512, 16 * 513 )]
    [0, 0, 0, 1, 1, 2]
    """
    return 1 + partition_prefix_length(n // 16) if n > 512 else 0


ma = 1  # managed access
pop = 2  # remove snapshot


def mksrc(google_project, snapshot, subgraphs, flags: int = 0) -> tuple[str, str]:
    project = '_'.join(snapshot.split('_')[1:-3])
    assert flags <= ma | pop
    source = None if flags & pop else ':'.join([
        'tdr',
        google_project,
        'snapshot/' + snapshot,
        '/' + str(partition_prefix_length(subgraphs))
    ])
    return project, source


def mkdelta(items: list[tuple[str, str]]) -> dict[str, str]:
    result = dict(items)
    assert len(items) == len(result), 'collisions detected'
    assert list(result.keys()) == sorted(result.keys()), 'input not sorted'
    return result


def mklist(catalog: dict[str, str]) -> list[str]:
    return list(filter(None, catalog.values()))


def mkdict(previous_catalog: dict[str, str],
           num_expected: int,
           delta: dict[str, str]
           ) -> dict[str, str]:
    catalog = previous_catalog | delta
    num_actual = len(mklist(catalog))
    assert num_expected == num_actual, (num_expected, num_actual)
    return catalog


anvil_sources = mkdict({}, 11, mkdelta([
    mksrc('datarepo-3edb7fb1', 'ANVIL_1000G_high_coverage_2019_20230517_ANV5_202305181946', 6804),
    mksrc('datarepo-db7353fb', 'ANVIL_CMG_UWASH_DS_BAV_IRB_PUB_RD_20230419_ANV5_202304201858', 181),
    mksrc('datarepo-3b8ef67a', 'ANVIL_CMG_UWASH_DS_BDIS_20230418_ANV5_202304201958', 10),
    mksrc('datarepo-5d27ebfe', 'ANVIL_CMG_UWASH_DS_HFA_20230418_ANV5_202304201932', 198),
    mksrc('datarepo-9d1a6e0a', 'ANVIL_CMG_UWASH_DS_NBIA_20230418_ANV5_202304201949', 110),
    mksrc('datarepo-3243df15', 'ANVIL_CMG_UWASH_HMB_20230418_ANV5_202304201923', 423),
    mksrc('datarepo-50484f86', 'ANVIL_CMG_UWASH_HMB_IRB_20230418_ANV5_202304201915', 45),
    mksrc('datarepo-74bd0964', 'ANVIL_CMG_UWash_DS_EP_20230419_ANV5_202304201906', 53),
    mksrc('datarepo-e5914f89', 'ANVIL_CMG_UWash_GRU_20230418_ANV5_202304201848', 5861),
    mksrc('datarepo-97ec5366', 'ANVIL_CMG_UWash_GRU_IRB_20230418_ANV5_202304201940', 563),
    mksrc('datarepo-4150bd87', 'ANVIL_GTEx_V8_hg38_20230419_ANV5_202304202007', 100367)
]))

anvil1_sources = mkdict(anvil_sources, 65, mkdelta([
    mksrc('datarepo-d53aa186', 'ANVIL_CMG_BROAD_BRAIN_ENGLE_WES_20221102_ANV5_202304241525', 473),
    mksrc('datarepo-69b2535a', 'ANVIL_CMG_BROAD_BRAIN_SHERR_WGS_20221102_ANV5_202304241530', 3),
    mksrc('datarepo-490be510', 'ANVIL_CMG_BROAD_ORPHAN_SCOTT_WGS_20221102_ANV5_202304241538', 15),
    mksrc('datarepo-6ddb7e8d', 'ANVIL_CMG_BaylorHopkins_HMB_IRB_NPU_WES_20221020_ANV5_202304211516', 2223),
    mksrc('datarepo-d09f6f5a', 'ANVIL_CMG_BaylorHopkins_HMB_NPU_WES_20230525_ANV5_202306211834', 4804),
    mksrc('datarepo-3b33c41b', 'ANVIL_CMG_Broad_Blood_Gazda_WES_20221117_ANV5_202304241459', 612),
    mksrc('datarepo-96df3cea', 'ANVIL_CMG_Broad_Blood_Sankaran_WES_20221117_ANV5_202304241501', 1141),
    mksrc('datarepo-179ee079', 'ANVIL_CMG_Broad_Blood_Sankaran_WGS_20221117_ANV5_202304241503', 96),
    mksrc('datarepo-3dd4d039', 'ANVIL_CMG_Broad_Brain_Gleeson_WES_20221117_ANV5_202304241517', 1180),
    mksrc('datarepo-c361373f', 'ANVIL_CMG_Broad_Brain_Muntoni_WES_20221102_ANV5_202304241527', 39),
    mksrc('datarepo-12ac342c', 'ANVIL_CMG_Broad_Brain_NeuroDev_WES_20221102_ANV5_202304241529', 292),
    mksrc('datarepo-d7bfafc6', 'ANVIL_CMG_Broad_Brain_Thaker_WES_20221102_ANV5_202304241531', 46),
    mksrc('datarepo-3c031cc3', 'ANVIL_CMG_Broad_Brain_Walsh_WES_20230605_ANV5_202306131445', 2777),
    mksrc('datarepo-29812b42', 'ANVIL_CMG_Broad_Eye_Pierce_WES_20221205_ANV5_202304242250', 2150),
    mksrc('datarepo-48134558', 'ANVIL_CMG_Broad_Eye_Pierce_WGS_20221117_ANV5_202304241507', 35),
    mksrc('datarepo-36ebaa12', 'ANVIL_CMG_Broad_Heart_PCGC_Tristani_WGS_20221025_ANV5_202304211840', 214),
    mksrc('datarepo-f9826139', 'ANVIL_CMG_Broad_Heart_Seidman_WES_20221117_ANV5_202304241504', 133),
    mksrc('datarepo-85952af8', 'ANVIL_CMG_Broad_Kidney_Hildebrandt_WES_20230525_ANV5_202305251733', 3544),
    mksrc('datarepo-ee4ae9a1', 'ANVIL_CMG_Broad_Kidney_Hildebrandt_WGS_20221025_ANV5_202304211844', 27),
    mksrc('datarepo-cf168274', 'ANVIL_CMG_Broad_Kidney_Pollak_WES_20221025_ANV5_202304211846', 147),
    mksrc('datarepo-937b5d92', 'ANVIL_CMG_Broad_Muscle_Beggs_WES_20221102_ANV5_202304241506', 934),
    mksrc('datarepo-4d47ba2c', 'ANVIL_CMG_Broad_Muscle_Beggs_WGS_20221102_ANV5_202304241533', 141),
    mksrc('datarepo-82d1271a', 'ANVIL_CMG_Broad_Muscle_Bonnemann_WES_20221117_ANV5_202304241509', 305),
    mksrc('datarepo-6be3fb25', 'ANVIL_CMG_Broad_Muscle_Bonnemann_WGS_20221117_ANV5_202304241510', 152),
    mksrc('datarepo-b168eb10', 'ANVIL_CMG_Broad_Muscle_KNC_WES_20221116_ANV5_202304242219', 169),
    mksrc('datarepo-372244aa', 'ANVIL_CMG_Broad_Muscle_KNC_WGS_20221117_ANV5_202304242221', 16),
    mksrc('datarepo-72d751e6', 'ANVIL_CMG_Broad_Muscle_Kang_WES_20230525_ANV5_202305251745', 118),
    mksrc('datarepo-77a6c0aa', 'ANVIL_CMG_Broad_Muscle_Kang_WGS_20221025_ANV5_202304211849', 8),
    mksrc('datarepo-736a5f1f', 'ANVIL_CMG_Broad_Muscle_Laing_WES_20221208_ANV5_202304271308', 31),
    mksrc('datarepo-5019143b', 'ANVIL_CMG_Broad_Muscle_Myoseq_WES_20230621_ANV5_202306211852', 1382),
    mksrc('datarepo-27eb651a', 'ANVIL_CMG_Broad_Muscle_Myoseq_WGS_20221208_ANV5_202304271310', 10),
    mksrc('datarepo-c087af7a', 'ANVIL_CMG_Broad_Muscle_OGrady_WES_20221205_ANV5_202304242252', 226),
    mksrc('datarepo-db987a2e', 'ANVIL_CMG_Broad_Muscle_Ravenscroft_WES_20221208_ANV5_202304271311', 140),
    mksrc('datarepo-05df566c', 'ANVIL_CMG_Broad_Muscle_Topf_WES_20221208_ANV5_202304271313', 2408),
    mksrc('datarepo-87d91f06', 'ANVIL_CMG_Broad_Orphan_Chung_WES_20221102_ANV5_202304241534', 71),
    mksrc('datarepo-25f6b696', 'ANVIL_CMG_Broad_Orphan_Estonia_Ounap_WES_20221117_ANV5_202304241512', 107),
    mksrc('datarepo-c3b16b41', 'ANVIL_CMG_Broad_Orphan_Estonia_Ounap_WGS_20221205_ANV5_202304242255', 427),
    mksrc('datarepo-e2976b05', 'ANVIL_CMG_Broad_Orphan_Jueppner_WES_20221102_ANV5_202304241535', 11),
    mksrc('datarepo-32fe2260', 'ANVIL_CMG_Broad_Orphan_Lerner_Ellis_WES_20221102_ANV5_202304241536', 11),
    mksrc('datarepo-6f9e574e', 'ANVIL_CMG_Broad_Orphan_Manton_WES_20221117_ANV5_202304241513', 1254),
    mksrc('datarepo-53cd689b', 'ANVIL_CMG_Broad_Orphan_Manton_WGS_20221117_ANV5_202304241515', 64),
    mksrc('datarepo-e7c5babf', 'ANVIL_CMG_Broad_Orphan_Scott_WES_20221025_ANV5_202304241458', 237),
    mksrc('datarepo-051877f4', 'ANVIL_CMG_Broad_Orphan_Sweetser_WES_20221102_ANV5_202304241539', 3),
    mksrc('datarepo-555c7706', 'ANVIL_CMG_Broad_Orphan_VCGS_White_WES_20221018_ANV5_202304241522', 1526),
    mksrc('datarepo-3a8f7952', 'ANVIL_CMG_Broad_Orphan_VCGS_White_WGS_20221117_ANV5_202304241523', 156),
    mksrc('datarepo-b699c5e3', 'ANVIL_CMG_Broad_Rare_RGP_WES_20221102_ANV5_202304241540', 6),
    mksrc('datarepo-2d5bd095', 'ANVIL_CMG_Broad_Stillbirth_Wilkins_Haug_WES_20221102_ANV5_202304241542', 60),
    mksrc('datarepo-f3d0eda6', 'ANVIL_CMG_UWash_GRU_20230418_ANV5_202306211828', 5861),
    mksrc('datarepo-19b85efe', 'ANVIL_CMG_YALE_DS_MC_20221026_ANV5_202304211522', 748),
    mksrc('datarepo-ab5c3fa5', 'ANVIL_CMG_YALE_DS_RARED_20221020_ANV5_202304211812', 173),
    mksrc('datarepo-d51578f4', 'ANVIL_CMG_Yale_GRU_20221020_ANV5_202304211517', 2196),
    mksrc('datarepo-bcedc554', 'ANVIL_CMG_Yale_HMB_20221020_ANV5_202304211813', 125),
    mksrc('datarepo-f485fa3e', 'ANVIL_CMG_Yale_HMB_GSO_20221020_ANV5_202304211519', 4264),
    mksrc('datarepo-d948d21a', 'ANVIL_cmg_broad_brain_engle_wgs_20221202_ANV5_202304271345', 95),
    mksrc('datarepo-1cb73890', 'ANVIL_cmg_broad_heart_ware_wes_20221215_ANV5_202304242145', 40),
]))


def env() -> Mapping[str, Optional[str]]:
    """
    Returns a dictionary that maps environment variable names to values. The
    values are either None or strings. String values can contain references to
    other environment variables in the form `{FOO}` where FOO is the name of an
    environment variable. See

    https://docs.python.org/3.9/library/string.html#format-string-syntax

    for the concrete syntax. These references will be resolved *after* the
    overall environment has been compiled by merging all relevant
    `environment.py` and `environment.local.py` files.

    Entries with a `None` value will be excluded from the environment. They
    can be used to document a variable without a default value in which case
    other, more specific `environment.py` or `environment.local.py` files must
    provide the value.
    """
    return {
        # Set variables for the `anvilprod` (short for AnVIL production)
        # deployment here.
        #
        # Only modify this file if you intend to commit those changes. To change the
        # environment with a setting that's specific to you AND the deployment, create
        # a environment.local.py right next to this file and make your changes there.
        # Settings applicable to all environments but specific to you go into
        # environment.local.py at the project root.

        'AZUL_DEPLOYMENT_STAGE': 'anvilprod',

        'AZUL_DOMAIN_NAME': 'prod.anvil.gi.ucsc.edu',
        'AZUL_PRIVATE_API': '0',

        'AZUL_S3_BUCKET': 'edu-ucsc-gi-platform-anvil-prod-storage-{AZUL_DEPLOYMENT_STAGE}.{AWS_DEFAULT_REGION}',

        'AZUL_CATALOGS': json.dumps({
            f'{catalog}{suffix}': dict(atlas=atlas,
                                       internal=internal,
                                       plugins=dict(metadata=dict(name='anvil'),
                                                    repository=dict(name='tdr_anvil')),
                                       sources=list(filter(None, sources.values())))
            for atlas, catalog, sources in [
                ('anvil', 'anvil1', anvil1_sources),
                ('anvil', 'anvil', anvil_sources),
            ]
            for suffix, internal in [
                ('', False),
                ('-it', True)
            ]
        }),

        'AZUL_TDR_SOURCE_LOCATION': 'us-central1',
        'AZUL_TDR_SERVICE_URL': 'https://data.terra.bio',
        'AZUL_SAM_SERVICE_URL': 'https://sam.dsde-prod.broadinstitute.org',

        'AZUL_ENABLE_MONITORING': '1',

        # $0.382/h × 3 × 24h/d × 30d/mo = $825.12/mo
        'AZUL_ES_INSTANCE_TYPE': 'r6gd.xlarge.elasticsearch',
        'AZUL_ES_INSTANCE_COUNT': '4',

        'AZUL_DEBUG': '1',

        'AZUL_BILLING': 'anvil',

        'AZUL_OWNER': 'hannes@ucsc.edu',

        'AZUL_MONITORING_EMAIL': 'azul-group@ucsc.edu',

        'AZUL_AWS_ACCOUNT_ID': '465330168186',
        'AWS_DEFAULT_REGION': 'us-east-1',

        'GOOGLE_PROJECT': 'platform-anvil-prod',

        'AZUL_DEPLOYMENT_INCARNATION': '1',

        'AZUL_GOOGLE_OAUTH2_CLIENT_ID': '1055427471534-8ee4mhig5j40n6n366j7uul26bbbhp2p.apps.googleusercontent.com',

        'azul_slack_integration': json.dumps({
            'workspace_id': 'T09P9H91S',  # ucsc-gi.slack.com
            'channel_id': 'C04TKUL49FA'  # #team-boardwalk-anvilprod
        }),
    }
