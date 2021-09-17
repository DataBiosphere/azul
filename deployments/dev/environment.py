import json
from typing import (
    Mapping,
    Optional,
)


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
            name: dict(atlas=atlas,
                       internal=internal,
                       plugins=dict(metadata=dict(name='hca'),
                                    repository=dict(name='tdr')))
            for name, atlas, internal in [
                ('dcp2', 'hca', False),
                ('lungmap', 'lungmap', False),
                ('it2', 'hca', True),
                ('it3lungmap', 'lungmap', True)
            ]
        }),

        'AZUL_PARTITION_PREFIX_LENGTH': '2',

        'AZUL_TDR_SOURCES': ','.join([
            'tdr:datarepo-dev-a9252919:snapshot/hca_dev_005d611a14d54fbf846e571a1f874f70__20210827_20210903:',
            'tdr:datarepo-dev-c148d39c:snapshot/hca_dev_027c51c60719469fa7f5640fe57cbece__20210827_20210902:',
            'tdr:datarepo-dev-e2ab8487:snapshot/hca_dev_03c6fce7789e4e78a27a664d562bb738__20210902_20210907:',
            'tdr:datarepo-dev-37639c56:snapshot/hca_dev_05657a599f9d4bb9b77b24be13aa5cea__20210827_20210928:',
            'tdr:datarepo-dev-9f4012c9:snapshot/hca_dev_05be4f374506429bb112506444507d62__20210827_20210902:',
            'tdr:datarepo-dev-baa2812f:snapshot/hca_dev_0792db3480474e62802c9177c9cd8e28__20210827_20210903:',
            'tdr:datarepo-dev-38e08b5c:snapshot/hca_dev_08b794a0519c4516b184c583746254c5__20210901_20210903:',
            'tdr:datarepo-dev-2749da57:snapshot/hca_dev_091cf39b01bc42e59437f419a66c8a45__20210830_20210903:',
            'tdr:datarepo-dev-eab7fa76:snapshot/hca_dev_0c3b7785f74d40918616a68757e4c2a8__20210827_20210903:',
            'tdr:datarepo-dev-fef02a92:snapshot/hca_dev_0d4b87ea6e9e456982e41343e0e3259f__20210827_20210903:',
            'tdr:datarepo-dev-78bae095:snapshot/hca_dev_0fd8f91862d64b8bac354c53dd601f71__20210830_20210903:',
            'tdr:datarepo-dev-ff0a2fe7:snapshot/hca_dev_116965f3f09447699d28ae675c1b569c__20210827_20210903:',
            'tdr:datarepo-dev-4de1b9fd:snapshot/hca_dev_16ed4ad8731946b288596fe1c1d73a82__20210830_20210903:',
            'tdr:datarepo-dev-135f340c:snapshot/hca_dev_1c6a960d52ac44eab728a59c7ab9dc8e__20210827_20210928:',
            'tdr:datarepo-dev-86c60513:snapshot/hca_dev_1cd1f41ff81a486ba05b66ec60f81dcf__20210901_20210903:',
            'tdr:datarepo-dev-f5321179:snapshot/hca_dev_1ce3b3dc02f244a896dad6d107b27a76__20210827_20210903:',
            'tdr:datarepo-dev-76de829d:snapshot/hca_dev_2043c65a1cf84828a6569e247d4e64f1__20210831_20210907:',
            'tdr:datarepo-dev-c808badb:snapshot/hca_dev_2086eb0510b9432bb7f0169ccc49d270__20210827_20210903:',
            'tdr:datarepo-dev-23782220:snapshot/hca_dev_23587fb31a4a4f58ad74cc9a4cb4c254__20210827_20210909:',
            'tdr:datarepo-dev-4c3e6011:snapshot/hca_dev_248fcf0316c64a41b6ccaad4d894ca42__20210907_20210907:',
            'tdr:datarepo-dev-1c2c69d9:snapshot/hca_dev_24c654a5caa5440a8f02582921f2db4a__20210830_20210903:',
            'tdr:datarepo-dev-38f08cd8:snapshot/hca_dev_2a64db431b554639aabb8dba0145689d__20210830_20210903:',
            'tdr:datarepo-dev-4cf05ce2:snapshot/hca_dev_2a72a4e566b2405abb7c1e463e8febb0__20210901_20210903:',
            'tdr:datarepo-dev-3041c2cf:snapshot/hca_dev_2af52a1365cb4973b51339be38f2df3f__20210830_20210903:',
            'tdr:datarepo-dev-311340f6:snapshot/hca_dev_2d8460958a334f3c97d4585bafac13b4__20210902_20210907:',
            'tdr:datarepo-dev-766bfb76:snapshot/hca_dev_2ef3655a973d4d699b4121fa4041eed7__20210827_20210903:',
            'tdr:datarepo-dev-1720b3c5:snapshot/hca_dev_379ed69ebe0548bcaf5ea7fc589709bf__20210827_20210902:',
            'tdr:datarepo-dev-ac6efd3f:snapshot/hca_dev_38449aea70b540db84b31e08f32efe34__20210830_20210903:',
            'tdr:datarepo-dev-40283c27:snapshot/hca_dev_3a69470330844ece9abed935fd5f6748__20210901_20210903:',
            'tdr:datarepo-dev-b08233fa:snapshot/hca_dev_3cfcdff5dee14a7ba591c09c6e850b11__20210827_20210903:',
            'tdr:datarepo-dev-bdc9f342:snapshot/hca_dev_3e329187a9c448ec90e3cc45f7c2311c__20210901_20210903:',
            'tdr:datarepo-dev-ec07c8d8:snapshot/hca_dev_4037007b0eff4e6db7bd8dd8eec80143__20210831_20210903:',
            'tdr:datarepo-dev-c0ec174a:snapshot/hca_dev_403c3e7668144a2da5805dd5de38c7ff__20210827_20210903:',
            'tdr:datarepo-dev-31b3553a:snapshot/hca_dev_414accedeba0440fb721befbc5642bef__20210827_20210903:',
            'tdr:datarepo-dev-b4789901:snapshot/hca_dev_41fb1734a121461695c73b732c9433c7__20210830_20210903:',
            'tdr:datarepo-dev-4e5ffd52:snapshot/hca_dev_42d4f8d454224b78adaee7c3c2ef511c__20210830_20210903:',
            'tdr:datarepo-dev-5ef7f2e2:snapshot/hca_dev_455b46e6d8ea4611861ede720a562ada__20210901_20210903:',
            'tdr:datarepo-dev-a6c6b953:snapshot/hca_dev_4bec484dca7a47b48d488830e06ad6db__20210830_20210903:',
            'tdr:datarepo-dev-f31edbc2:snapshot/hca_dev_4d6f6c962a8343d88fe10f53bffd4674__20210901_20210903:',
            'tdr:datarepo-dev-bb8fbae4:snapshot/hca_dev_51f02950ee254f4b8d0759aa99bb3498__20210827_20210928:',
            'tdr:datarepo-dev-71de019e:snapshot/hca_dev_520afa10f9d24e93ab7a26c4c863ce18__20210827_20210928:',
            'tdr:datarepo-dev-ffcf8b00:snapshot/hca_dev_52b29aa4c8d642b4807ab35be94469ca__20210830_20210903:',
            'tdr:datarepo-dev-f76414c8:snapshot/hca_dev_52d10a60c8d14d068a5eaf0d5c0d5034__20210827_20210902:',
            'tdr:datarepo-dev-319b80f7:snapshot/hca_dev_577c946d6de54b55a854cd3fde40bff2__20210827_20210903:',
            'tdr:datarepo-dev-279f1986:snapshot/hca_dev_5ee710d7e2d54fe2818d15f5e31dae32__20210901_20210903:',
            'tdr:datarepo-dev-0abea017:snapshot/hca_dev_6072616c87944b208f52fb15992ea5a4__20210827_20210902:',
            'tdr:datarepo-dev-da221b1a:snapshot/hca_dev_60ea42e1af4942f58164d641fdb696bc__20210827_20210903:',
            'tdr:datarepo-dev-c3d623dc:snapshot/hca_dev_63b5b6c1bbcd487d8c2e0095150c1ecd__20210830_20210903:',
            'tdr:datarepo-dev-ecb9c129:snapshot/hca_dev_67a3de0945b949c3a068ff4665daa50e__20210827_20210903:',
            'tdr:datarepo-dev-3545971c:snapshot/hca_dev_71436067ac414acebe1b2fbcc2cb02fa__20210827_20210928:',
            'tdr:datarepo-dev-12b7a9e1:snapshot/hca_dev_7880637a35a14047b422b5eac2a2a358__20210901_20210903:',
            'tdr:datarepo-dev-7913b094:snapshot/hca_dev_78b2406dbff246fc8b6120690e602227__20210827_20210902:',
            'tdr:datarepo-dev-4747d8fe:snapshot/hca_dev_7adede6a0ab745e69b67ffe7466bec1f__20210830_20210903:',
            'tdr:datarepo-dev-aba01389:snapshot/hca_dev_7b947aa243a74082afff222a3e3a4635__20210831_20210907:',
            'tdr:datarepo-dev-bf3a4c8a:snapshot/hca_dev_8185730f411340d39cc3929271784c2b__20210830_20210903:',
            'tdr:datarepo-dev-a86993e6:snapshot/hca_dev_83f5188e3bf749569544cea4f8997756__20210929_20210929:',
            'tdr:datarepo-dev-bd995e95:snapshot/hca_dev_842605c7375a47c59e2ca71c2c00fcad__20210830_20210903:',
            'tdr:datarepo-dev-d0772077:snapshot/hca_dev_8787c23889ef4636a57d3167e8b54a80__20210827_20210903:',
            'tdr:datarepo-dev-8eb2ffd1:snapshot/hca_dev_87d52a86bdc7440cb84d170f7dc346d9__20210830_20210903:',
            'tdr:datarepo-dev-0c5c20b5:snapshot/hca_dev_8c3c290ddfff4553886854ce45f4ba7f__20210902_20210907:',
            'tdr:datarepo-dev-a198b032:snapshot/hca_dev_90bd693340c048d48d76778c103bf545__20210827_20210903:',
            'tdr:datarepo-dev-59d37b9a:snapshot/hca_dev_946c5add47d1402a97bba5af97e8bce7__20210831_20210903:',
            'tdr:datarepo-dev-788c3b52:snapshot/hca_dev_955dfc2ca8c64d04aa4d907610545d11__20210831_20210903:',
            'tdr:datarepo-dev-4b88b45b:snapshot/hca_dev_962bd805eb894c54bad2008e497d1307__20210830_20210903:',
            # Managed access:
            'tdr:datarepo-dev-02c59b72:snapshot/hca_dev_99101928d9b14aafb759e97958ac7403__20210830_20210903:',
            'tdr:datarepo-dev-a6312a94:snapshot/hca_dev_992aad5e7fab46d9a47ddf715e8cfd24__20210830_20210903:',
            'tdr:datarepo-dev-75589244:snapshot/hca_dev_996120f9e84f409fa01e732ab58ca8b9__20210827_20210903:',
            # Managed access:
            'tdr:datarepo-dev-d4b988d6:snapshot/hca_dev_a004b1501c364af69bbd070c06dbc17d__20210830_20210903:',
            'tdr:datarepo-dev-9ec7beb6:snapshot/hca_dev_a29952d9925e40f48a1c274f118f1f51__20210827_20210902:',
            'tdr:datarepo-dev-d3d5bbfa:snapshot/hca_dev_a39728aa70a04201b0a281b7badf3e71__20210830_20210903:',
            'tdr:datarepo-dev-7b7daff7:snapshot/hca_dev_a96b71c078a742d188ce83c78925cfeb__20210827_20210902:',
            'tdr:datarepo-dev-58610528:snapshot/hca_dev_a9c022b4c7714468b769cabcf9738de3__20210827_20210903:',
            'tdr:datarepo-dev-1dce87e5:snapshot/hca_dev_ae71be1dddd84feb9bed24c3ddb6e1ad__20210916_20210916:',
            'tdr:datarepo-dev-b2004d1c:snapshot/hca_dev_b4a7d12f6c2f40a39e359756997857e3__20210831_20210903:',
            'tdr:datarepo-dev-0b465564:snapshot/hca_dev_b51f49b40d2e4cbdbbd504cd171fc2fa__20210830_20210903:',
            'tdr:datarepo-dev-cd97e83a:snapshot/hca_dev_b963bd4b4bc14404842569d74bc636b8__20210827_20210928:',
            'tdr:datarepo-dev-376d3f4a:snapshot/hca_dev_bd40033154b94fccbff66bb8b079ee1f__20210901_20210903:',
            'tdr:datarepo-dev-aa783adb:snapshot/hca_dev_c5f4661568de4cf4bbc2a0ae10f08243__20210827_20210928:',
            'tdr:datarepo-dev-990234a8:snapshot/hca_dev_c6ad8f9bd26a4811b2ba93d487978446__20210827_20210903:',
            'tdr:datarepo-dev-61b8d081:snapshot/hca_dev_c715cd2fdc7c44a69cd5b6a6d9f075ae__20210827_20210902:',
            'tdr:datarepo-dev-71926fdc:snapshot/hca_dev_c893cb575c9f4f26931221b85be84313__20210901_20210903:',
            'tdr:datarepo-dev-2f4bfe5d:snapshot/hca_dev_ccd1f1ba74ce469b9fc9f6faea623358__20210827_20210902:',
            'tdr:datarepo-dev-aa8357fb:snapshot/hca_dev_ccef38d7aa9240109621c4c7b1182647__20210831_20210903:',
            'tdr:datarepo-dev-24e672db:snapshot/hca_dev_cddab57b68684be4806f395ed9dd635a__20210831_20210907:',
            'tdr:datarepo-dev-f4cb2365:snapshot/hca_dev_ce33dde2382d448cb6acbfb424644f23__20210827_20210928:',
            'tdr:datarepo-dev-0d6f73ac:snapshot/hca_dev_d012d4768f8c4ff389d6ebbe22c1b5c1__20210827_20210903:',
            'tdr:datarepo-dev-5674b4eb:snapshot/hca_dev_d2111fac3fc44f429b6d32cd6a828267__20210830_20210903:',
            'tdr:datarepo-dev-b3632667:snapshot/hca_dev_d3446f0c30f34a12b7c36af877c7bb2d__20210901_20210903:',
            'tdr:datarepo-dev-92c3a1de:snapshot/hca_dev_d3a4ceac4d66498497042570c0647a56__20210831_20210903:',
            'tdr:datarepo-dev-4e88d60b:snapshot/hca_dev_d3ac7c1b53024804b611dad9f89c049d__20210827_20210928:',
            'tdr:datarepo-dev-dbc582d9:snapshot/hca_dev_dbcd4b1d31bd4eb594e150e8706fa192__20210827_20210902:',
            'tdr:datarepo-dev-848e2d4f:snapshot/hca_dev_dbd836cfbfc241f0983441cc6c0b235a__20210827_20210902:',
            'tdr:datarepo-dev-d7517bce:snapshot/hca_dev_dc1a41f69e0942a6959e3be23db6da56__20210827_20210902:',
            'tdr:datarepo-dev-27ad01e5:snapshot/hca_dev_df88f39f01a84b5b92f43177d6c0f242__20210827_20210928:',
            'tdr:datarepo-dev-b839d6c7:snapshot/hca_dev_e526d91dcf3a44cb80c5fd7676b55a1d__20210902_20210907:',
            'tdr:datarepo-dev-3faef568:snapshot/hca_dev_e5d455791f5b48c3b568320d93e7ca72__20210827_20210903:',
            'tdr:datarepo-dev-e304a8fe:snapshot/hca_dev_e77fed30959d4fadbc15a0a5a85c21d2__20210830_20210903:',
            'tdr:datarepo-dev-6fdac3db:snapshot/hca_dev_e8808cc84ca0409680f2bba73600cba6__20210902_20210907:',
            'tdr:datarepo-dev-dbc3e131:snapshot/hca_dev_eaefa1b6dae14414953b17b0427d061e__20210827_20210903:',
            'tdr:datarepo-dev-6883f2a5:snapshot/hca_dev_f48e7c39cc6740559d79bc437892840c__20210830_20210929:',
            'tdr:datarepo-dev-10f0610a:snapshot/hca_dev_f81efc039f564354aabb6ce819c3d414__20210827_20210903:',
            'tdr:datarepo-dev-24e9529e:snapshot/hca_dev_f83165c5e2ea4d15a5cf33f3550bffde__20210901_20210908:',
            'tdr:datarepo-dev-67240cf2:snapshot/hca_dev_f86f1ab41fbb4510ae353ffd752d4dfc__20210901_20210903:',
            'tdr:datarepo-dev-e8e0a59a:snapshot/hca_dev_f8aa201c4ff145a4890e840d63459ca2__20210901_20210903:',
            'tdr:datarepo-dev-96d8e08c:snapshot/hca_dev_faeedcb0e0464be7b1ad80a3eeabb066__20210831_20210903:'
        ]),
        **{
            f'AZUL_TDR_{catalog.upper()}_SOURCES': ','.join([
                'tdr:broad-jade-dev-data:snapshot/lungmap_dev_20210412__20210414:',
            ])
            for catalog in ('lungmap', 'it3lungmap')
        },
        'AZUL_TDR_SOURCE_LOCATION': 'US',
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
