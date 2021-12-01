import json
from typing import (
    Mapping,
    Optional,
)

is_sandbox = '/sandbox/' in __file__


def prefix(n):
    """
    For a given number of subgraphs, return a partition prefix length that
    yields at most 512 subgraphs per partition.

    >>> [prefix(n) for n in (0, 1, 512, 513, 16 * 512, 16 * 513 )]
    [0, 0, 0, 1, 1, 2]
    """
    return 1 + prefix(n // 16) if n > 512 else 0


def mksrc(project, snapshot, subgraphs, ma: int = 0):
    """
    :param ma: 1 for managed access
    """
    return f'tdr:{project}:snapshot/{snapshot}:/{prefix(subgraphs)}'


dcp2_sources = [
    mksrc('datarepo-dev-a9252919', 'hca_dev_005d611a14d54fbf846e571a1f874f70__20210827_20210903', 7),
    mksrc('datarepo-dev-c148d39c', 'hca_dev_027c51c60719469fa7f5640fe57cbece__20210827_20210902', 8),
    mksrc('datarepo-dev-e2ab8487', 'hca_dev_03c6fce7789e4e78a27a664d562bb738__20210902_20210907', 1530),
    mksrc('datarepo-dev-37639c56', 'hca_dev_05657a599f9d4bb9b77b24be13aa5cea__20210827_20210928', 185),
    mksrc('datarepo-dev-9f4012c9', 'hca_dev_05be4f374506429bb112506444507d62__20210827_20210902', 1544),
    mksrc('datarepo-dev-baa2812f', 'hca_dev_0792db3480474e62802c9177c9cd8e28__20210827_20210903', 1450),
    mksrc('datarepo-dev-38e08b5c', 'hca_dev_08b794a0519c4516b184c583746254c5__20210901_20210903', 2),
    mksrc('datarepo-dev-2749da57', 'hca_dev_091cf39b01bc42e59437f419a66c8a45__20210830_20210903', 20),
    mksrc('datarepo-dev-eab7fa76', 'hca_dev_0c3b7785f74d40918616a68757e4c2a8__20210827_20210903', 177),
    mksrc('datarepo-dev-fef02a92', 'hca_dev_0d4b87ea6e9e456982e41343e0e3259f__20210827_20210903', 8),
    mksrc('datarepo-dev-78bae095', 'hca_dev_0fd8f91862d64b8bac354c53dd601f71__20210830_20210903', 10),
    mksrc('datarepo-dev-ff0a2fe7', 'hca_dev_116965f3f09447699d28ae675c1b569c__20210827_20210903', 8),
    mksrc('datarepo-dev-4de1b9fd', 'hca_dev_16ed4ad8731946b288596fe1c1d73a82__20210830_20210903', 28),
    mksrc('datarepo-dev-135f340c', 'hca_dev_1c6a960d52ac44eab728a59c7ab9dc8e__20210827_20210928', 10),
    mksrc('datarepo-dev-86c60513', 'hca_dev_1cd1f41ff81a486ba05b66ec60f81dcf__20210901_20210903', 18),
    mksrc('datarepo-dev-f5321179', 'hca_dev_1ce3b3dc02f244a896dad6d107b27a76__20210827_20210903', 421),
    mksrc('datarepo-dev-76de829d', 'hca_dev_2043c65a1cf84828a6569e247d4e64f1__20210831_20210907', 1734),
    mksrc('datarepo-dev-c808badb', 'hca_dev_2086eb0510b9432bb7f0169ccc49d270__20210827_20210903', 10),
    mksrc('datarepo-dev-23782220', 'hca_dev_23587fb31a4a4f58ad74cc9a4cb4c254__20210827_20210909', 1476),
    mksrc('datarepo-dev-4c3e6011', 'hca_dev_248fcf0316c64a41b6ccaad4d894ca42__20210907_20210907', 2958),
    mksrc('datarepo-dev-1c2c69d9', 'hca_dev_24c654a5caa5440a8f02582921f2db4a__20210830_20210903', 3),
    mksrc('datarepo-dev-38f08cd8', 'hca_dev_2a64db431b554639aabb8dba0145689d__20210830_20210903', 10),
    mksrc('datarepo-dev-4cf05ce2', 'hca_dev_2a72a4e566b2405abb7c1e463e8febb0__20210901_20210903', 2290),
    mksrc('datarepo-dev-3041c2cf', 'hca_dev_2af52a1365cb4973b51339be38f2df3f__20210830_20210903', 10),
    mksrc('datarepo-dev-311340f6', 'hca_dev_2d8460958a334f3c97d4585bafac13b4__20210902_20210907', 3589),
    mksrc('datarepo-dev-766bfb76', 'hca_dev_2ef3655a973d4d699b4121fa4041eed7__20210827_20210903', 8),
    mksrc('datarepo-dev-1720b3c5', 'hca_dev_379ed69ebe0548bcaf5ea7fc589709bf__20210827_20210902', 4),
    mksrc('datarepo-dev-ac6efd3f', 'hca_dev_38449aea70b540db84b31e08f32efe34__20210830_20210903', 42),
    mksrc('datarepo-dev-40283c27', 'hca_dev_3a69470330844ece9abed935fd5f6748__20210901_20210903', 125),
    mksrc('datarepo-dev-b08233fa', 'hca_dev_3cfcdff5dee14a7ba591c09c6e850b11__20210827_20210903', 8),
    mksrc('datarepo-dev-bdc9f342', 'hca_dev_3e329187a9c448ec90e3cc45f7c2311c__20210901_20210903', 1001),
    mksrc('datarepo-dev-ec07c8d8', 'hca_dev_4037007b0eff4e6db7bd8dd8eec80143__20210831_20210903', 39),
    mksrc('datarepo-dev-c0ec174a', 'hca_dev_403c3e7668144a2da5805dd5de38c7ff__20210827_20210903', 63),
    mksrc('datarepo-dev-31b3553a', 'hca_dev_414accedeba0440fb721befbc5642bef__20210827_20210903', 4),
    mksrc('datarepo-dev-b4789901', 'hca_dev_41fb1734a121461695c73b732c9433c7__20210830_20210903', 12),
    mksrc('datarepo-dev-4e5ffd52', 'hca_dev_42d4f8d454224b78adaee7c3c2ef511c__20210830_20210903', 9),
    mksrc('datarepo-dev-5ef7f2e2', 'hca_dev_455b46e6d8ea4611861ede720a562ada__20210901_20210903', 74),
    mksrc('datarepo-dev-a6c6b953', 'hca_dev_4bec484dca7a47b48d488830e06ad6db__20210830_20210903', 14),
    mksrc('datarepo-dev-f31edbc2', 'hca_dev_4d6f6c962a8343d88fe10f53bffd4674__20210901_20210903', 12),
    mksrc('datarepo-dev-bb8fbae4', 'hca_dev_51f02950ee254f4b8d0759aa99bb3498__20210827_20210928', 6),
    mksrc('datarepo-dev-71de019e', 'hca_dev_520afa10f9d24e93ab7a26c4c863ce18__20210827_20210928', 649),
    mksrc('datarepo-dev-ffcf8b00', 'hca_dev_52b29aa4c8d642b4807ab35be94469ca__20210830_20210903', 467),
    mksrc('datarepo-dev-f76414c8', 'hca_dev_52d10a60c8d14d068a5eaf0d5c0d5034__20210827_20210902', 176),
    mksrc('datarepo-dev-319b80f7', 'hca_dev_577c946d6de54b55a854cd3fde40bff2__20210827_20210903', 7),
    mksrc('datarepo-dev-279f1986', 'hca_dev_5ee710d7e2d54fe2818d15f5e31dae32__20210901_20210903', 41),
    mksrc('datarepo-dev-0abea017', 'hca_dev_6072616c87944b208f52fb15992ea5a4__20210827_20210902', 603),
    mksrc('datarepo-dev-da221b1a', 'hca_dev_60ea42e1af4942f58164d641fdb696bc__20210827_20210903', 1145),
    mksrc('datarepo-dev-c3d623dc', 'hca_dev_63b5b6c1bbcd487d8c2e0095150c1ecd__20210830_20210903', 11),
    mksrc('datarepo-dev-ecb9c129', 'hca_dev_67a3de0945b949c3a068ff4665daa50e__20210827_20210903', 732),
    mksrc('datarepo-dev-3545971c', 'hca_dev_71436067ac414acebe1b2fbcc2cb02fa__20210827_20210928', 4),
    mksrc('datarepo-dev-12b7a9e1', 'hca_dev_7880637a35a14047b422b5eac2a2a358__20210901_20210903', 366),
    mksrc('datarepo-dev-7913b094', 'hca_dev_78b2406dbff246fc8b6120690e602227__20210827_20210902', 216),
    mksrc('datarepo-dev-4747d8fe', 'hca_dev_7adede6a0ab745e69b67ffe7466bec1f__20210830_20210903', 1601),
    mksrc('datarepo-dev-aba01389', 'hca_dev_7b947aa243a74082afff222a3e3a4635__20210831_20210907', 7),
    mksrc('datarepo-dev-bf3a4c8a', 'hca_dev_8185730f411340d39cc3929271784c2b__20210830_20210903', 12),
    mksrc('datarepo-dev-560ee3d1', 'hca_dev_83f5188e3bf749569544cea4f8997756__20210929_20211007', 1612),
    mksrc('datarepo-dev-bd995e95', 'hca_dev_842605c7375a47c59e2ca71c2c00fcad__20210830_20210903', 8),
    mksrc('datarepo-dev-d0772077', 'hca_dev_8787c23889ef4636a57d3167e8b54a80__20210827_20210903', 3),
    mksrc('datarepo-dev-8eb2ffd1', 'hca_dev_87d52a86bdc7440cb84d170f7dc346d9__20210830_20210903', 16),
    mksrc('datarepo-dev-0c5c20b5', 'hca_dev_8c3c290ddfff4553886854ce45f4ba7f__20210902_20210907', 6640),
    mksrc('datarepo-dev-29509483', 'hca_dev_90bd693340c048d48d76778c103bf545__20210827_20211110', 2245),
    mksrc('datarepo-dev-59d37b9a', 'hca_dev_946c5add47d1402a97bba5af97e8bce7__20210831_20210903', 149),
    mksrc('datarepo-dev-788c3b52', 'hca_dev_955dfc2ca8c64d04aa4d907610545d11__20210831_20210903', 13),
    mksrc('datarepo-dev-4b88b45b', 'hca_dev_962bd805eb894c54bad2008e497d1307__20210830_20210903', 28),
    mksrc('datarepo-dev-02c59b72', 'hca_dev_99101928d9b14aafb759e97958ac7403__20210830_20210903', 1190, ma=1),
    mksrc('datarepo-dev-a6312a94', 'hca_dev_992aad5e7fab46d9a47ddf715e8cfd24__20210830_20210903', 41),
    mksrc('datarepo-dev-75589244', 'hca_dev_996120f9e84f409fa01e732ab58ca8b9__20210827_20210903', 26),
    mksrc('datarepo-dev-d4b988d6', 'hca_dev_a004b1501c364af69bbd070c06dbc17d__20210830_20210903', 16, ma=1),
    mksrc('datarepo-dev-9ec7beb6', 'hca_dev_a29952d9925e40f48a1c274f118f1f51__20210827_20210902', 26),
    mksrc('datarepo-dev-d3d5bbfa', 'hca_dev_a39728aa70a04201b0a281b7badf3e71__20210830_20210903', 33),
    mksrc('datarepo-dev-7b7daff7', 'hca_dev_a96b71c078a742d188ce83c78925cfeb__20210827_20210902', 6),
    mksrc('datarepo-dev-58610528', 'hca_dev_a9c022b4c7714468b769cabcf9738de3__20210827_20210903', 23),
    mksrc('datarepo-dev-1dce87e5', 'hca_dev_ae71be1dddd84feb9bed24c3ddb6e1ad__20210916_20210916', 3515),
    mksrc('datarepo-dev-b2004d1c', 'hca_dev_b4a7d12f6c2f40a39e359756997857e3__20210831_20210903', 24),
    mksrc('datarepo-dev-0b465564', 'hca_dev_b51f49b40d2e4cbdbbd504cd171fc2fa__20210830_20210903', 193),
    mksrc('datarepo-dev-cd97e83a', 'hca_dev_b963bd4b4bc14404842569d74bc636b8__20210827_20210928', 2),
    mksrc('datarepo-dev-376d3f4a', 'hca_dev_bd40033154b94fccbff66bb8b079ee1f__20210901_20210903', 18),
    mksrc('datarepo-dev-aa783adb', 'hca_dev_c5f4661568de4cf4bbc2a0ae10f08243__20210827_20210928', 1),
    mksrc('datarepo-dev-990234a8', 'hca_dev_c6ad8f9bd26a4811b2ba93d487978446__20210827_20210903', 639),
    mksrc('datarepo-dev-61b8d081', 'hca_dev_c715cd2fdc7c44a69cd5b6a6d9f075ae__20210827_20210902', 9),
    mksrc('datarepo-dev-71926fdc', 'hca_dev_c893cb575c9f4f26931221b85be84313__20210901_20210903', 20),
    mksrc('datarepo-dev-2f4bfe5d', 'hca_dev_ccd1f1ba74ce469b9fc9f6faea623358__20210827_20210902', 222),
    mksrc('datarepo-dev-aa8357fb', 'hca_dev_ccef38d7aa9240109621c4c7b1182647__20210831_20210903', 1314),
    mksrc('datarepo-dev-24e672db', 'hca_dev_cddab57b68684be4806f395ed9dd635a__20210831_20210907', 2545),
    mksrc('datarepo-dev-f4cb2365', 'hca_dev_ce33dde2382d448cb6acbfb424644f23__20210827_20210928', 189),
    mksrc('datarepo-dev-0d6f73ac', 'hca_dev_d012d4768f8c4ff389d6ebbe22c1b5c1__20210827_20210903', 8),
    mksrc('datarepo-dev-5674b4eb', 'hca_dev_d2111fac3fc44f429b6d32cd6a828267__20210830_20210903', 735),
    mksrc('datarepo-dev-b3632667', 'hca_dev_d3446f0c30f34a12b7c36af877c7bb2d__20210901_20210903', 40),
    mksrc('datarepo-dev-92c3a1de', 'hca_dev_d3a4ceac4d66498497042570c0647a56__20210831_20210903', 14),
    mksrc('datarepo-dev-e5bc6d76', 'hca_dev_d3ac7c1b53024804b611dad9f89c049d__20210827_20211015', 11),
    mksrc('datarepo-dev-dbc582d9', 'hca_dev_dbcd4b1d31bd4eb594e150e8706fa192__20210827_20210902', 84),
    mksrc('datarepo-dev-848e2d4f', 'hca_dev_dbd836cfbfc241f0983441cc6c0b235a__20210827_20210902', 1),
    mksrc('datarepo-dev-d7517bce', 'hca_dev_dc1a41f69e0942a6959e3be23db6da56__20210827_20210902', 5),
    mksrc('datarepo-dev-27ad01e5', 'hca_dev_df88f39f01a84b5b92f43177d6c0f242__20210827_20210928', 1),
    mksrc('datarepo-dev-b839d6c7', 'hca_dev_e526d91dcf3a44cb80c5fd7676b55a1d__20210902_20210907', 606),
    mksrc('datarepo-dev-3faef568', 'hca_dev_e5d455791f5b48c3b568320d93e7ca72__20210827_20210903', 8),
    mksrc('datarepo-dev-e304a8fe', 'hca_dev_e77fed30959d4fadbc15a0a5a85c21d2__20210830_20210903', 333),
    mksrc('datarepo-dev-6fdac3db', 'hca_dev_e8808cc84ca0409680f2bba73600cba6__20210902_20210907', 898),
    mksrc('datarepo-dev-dbc3e131', 'hca_dev_eaefa1b6dae14414953b17b0427d061e__20210827_20210903', 385),
    mksrc('datarepo-dev-b51e6694', 'hca_dev_f48e7c39cc6740559d79bc437892840c__20210830_20211007', 14),
    mksrc('datarepo-dev-10f0610a', 'hca_dev_f81efc039f564354aabb6ce819c3d414__20210827_20210903', 4),
    mksrc('datarepo-dev-24e9529e', 'hca_dev_f83165c5e2ea4d15a5cf33f3550bffde__20210901_20210908', 7663),
    mksrc('datarepo-dev-67240cf2', 'hca_dev_f86f1ab41fbb4510ae353ffd752d4dfc__20210901_20210903', 20),
    mksrc('datarepo-dev-e8e0a59a', 'hca_dev_f8aa201c4ff145a4890e840d63459ca2__20210901_20210903', 384),
    mksrc('datarepo-dev-96d8e08c', 'hca_dev_faeedcb0e0464be7b1ad80a3eeabb066__20210831_20210903', 62),
]


def env() -> Mapping[str, Optional[str]]:
    """
    Returns a dictionary that maps environment variable names to values. The
    values are either None or strings. String values can contain references to
    other environment variables in the form `{FOO}` where FOO is the name of an
    environment variable. See

    https://docs.python.org/3.8/library/string.html#format-string-syntax

    for the concrete syntax. These references will be resolved *after* the
    overall environment has been compiled by merging all relevant
    `environment.py` and `environment.local.py` files.

    Entries with a `None` value will be excluded from the environment. They
    can be used to document a variable without a default value in which case
    other, more specific `environment.py` or `environment.local.py` files must
    provide the value.
    """
    return {
        # Set variables for the `dev` (short for development) deployment here.
        #
        # Only modify this file if you intend to commit those changes. To change the
        # environment with a setting that's specific to you AND the deployment, create
        # a environment.local.py right next to this file and make your changes there.
        # Settings applicable to all environments but specific to you go into
        # environment.local.py at the project root.

        'AZUL_DEPLOYMENT_STAGE': 'dev',

        'AZUL_CATALOGS': json.dumps({
            f'dcp2{suffix}': dict(atlas='hca',
                                  internal=internal,
                                  plugins=dict(metadata=dict(name='hca'),
                                               repository=dict(name='tdr')),
                                  sources=dcp2_sources)
            for suffix, internal in [
                ('', False),
                ('-it', True)
            ]
        }),

        'AZUL_PARTITION_PREFIX_LENGTH': '0',

        'AZUL_TDR_SOURCE_LOCATION': 'us-central1',
        'AZUL_TDR_SERVICE_URL': 'https://jade.datarepo-dev.broadinstitute.org',
        'AZUL_SAM_SERVICE_URL': 'https://sam.dsde-dev.broadinstitute.org',

        'AZUL_DRS_DOMAIN_NAME': 'drs.dev.singlecell.gi.ucsc.edu',

        'AZUL_URL_REDIRECT_BASE_DOMAIN_NAME': 'dev.url.singlecell.gi.ucsc.edu',
        'AZUL_URL_REDIRECT_FULL_DOMAIN_NAME': '{AZUL_DEPLOYMENT_STAGE}.{AZUL_URL_REDIRECT_BASE_DOMAIN_NAME}',

        # $0.372/h × 3 × 24h/d × 30d/mo = $803.52/mo
        'AZUL_ES_INSTANCE_TYPE': 'r5.xlarge.elasticsearch',
        'AZUL_ES_INSTANCE_COUNT': '3',

        'AZUL_DEBUG': '1',

        'AZUL_OWNER': 'hannes@ucsc.edu',

        'AZUL_AWS_ACCOUNT_ID': '122796619775',
        'AWS_DEFAULT_REGION': 'us-east-1',

        'GOOGLE_PROJECT': 'platform-hca-dev',

        'AZUL_GOOGLE_OAUTH2_CLIENT_ID': '713613812354-aelk662bncv14d319dk8juce9p11um00.apps.googleusercontent.com',
    }
