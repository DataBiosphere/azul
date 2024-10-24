import base64
import bz2
from collections.abc import (
    Mapping,
)
import json
from typing import (
    Literal,
    Optional,
)

ma = 1  # managed access
pop = 2  # remove snapshot


def mksrc(source_type: Literal['bigquery', 'parquet'],
          google_project,
          snapshot,
          flags: int = 0,
          /,
          prefix: str = ''
          ) -> tuple[str, str | None]:
    _, env, project, _ = snapshot.split('_', 3)
    assert flags <= ma | pop
    source = None if flags & pop else ':'.join([
        'tdr',
        source_type,
        'gcp',
        google_project,
        snapshot,
        prefix
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


dcp12_sources = mkdict({}, 195, mkdelta([
    mksrc('bigquery', 'datarepo-a1c89fba', 'hca_prod_005d611a14d54fbf846e571a1f874f70__20220111_dcp2_20220113_dcp12'),
    mksrc('bigquery', 'datarepo-a9316414', 'hca_prod_027c51c60719469fa7f5640fe57cbece__20220110_dcp2_20220113_dcp12'),
    mksrc('bigquery', 'datarepo-d111fe96', 'hca_prod_03c6fce7789e4e78a27a664d562bb738__20220110_dcp2_20220113_dcp12'),
    mksrc('bigquery', 'datarepo-a2d29140', 'hca_prod_04ad400c58cb40a5bc2b2279e13a910b__20220114_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-d8ad6862', 'hca_prod_05657a599f9d4bb9b77b24be13aa5cea__20220110_dcp2_20220113_dcp12'),
    mksrc('bigquery', 'datarepo-c9b9a2e8', 'hca_prod_05be4f374506429bb112506444507d62__20220107_dcp2_20220113_dcp12'),
    mksrc('bigquery', 'datarepo-4e087937', 'hca_prod_07073c1280064710a00b23abdb814904__20220107_dcp2_20220113_dcp12'),
    mksrc('bigquery', 'datarepo-9226064c', 'hca_prod_074a9f88729a455dbca50ce80edf0cea__20220107_dcp2_20220113_dcp12'),
    mksrc('bigquery', 'datarepo-5bd98333', 'hca_prod_0792db3480474e62802c9177c9cd8e28__20220107_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-580db83c', 'hca_prod_08b794a0519c4516b184c583746254c5__20220107_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-0b49ea1e', 'hca_prod_091cf39b01bc42e59437f419a66c8a45__20220107_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-109db6e4', 'hca_prod_0c09fadee0794fde8e606725b8c1d84b__20220107_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-26de5247', 'hca_prod_0c3b7785f74d40918616a68757e4c2a8__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-ae49a863', 'hca_prod_0d4b87ea6e9e456982e41343e0e3259f__20220110_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-76169feb', 'hca_prod_0fd8f91862d64b8bac354c53dd601f71__20220110_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-4b42c4ef', 'hca_prod_116965f3f09447699d28ae675c1b569c__20220107_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-eb39c36f', 'hca_prod_16ed4ad8731946b288596fe1c1d73a82__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-982c56ad', 'hca_prod_1c6a960d52ac44eab728a59c7ab9dc8e__20220110_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-f24e8394', 'hca_prod_1cd1f41ff81a486ba05b66ec60f81dcf__20220107_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-b8ffd379', 'hca_prod_1ce3b3dc02f244a896dad6d107b27a76__20220107_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-b1ac3907', 'hca_prod_1defdadaa36544ad9b29443b06bd11d6__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-4e5e9f9b', 'hca_prod_2043c65a1cf84828a6569e247d4e64f1__20220111_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-156c78f4', 'hca_prod_2084526ba66f4c40bb896fd162f2eb38__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-228ac7b7', 'hca_prod_2086eb0510b9432bb7f0169ccc49d270__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-7defc353', 'hca_prod_20f37aafcaa140e69123be6ce8feb2d6__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-783bc6c3', 'hca_prod_21ea8ddb525f4f1fa82031f0360399a2__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-d8b00524', 'hca_prod_23587fb31a4a4f58ad74cc9a4cb4c254__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-8390f5e3', 'hca_prod_248fcf0316c64a41b6ccaad4d894ca42__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-45f08380', 'hca_prod_24c654a5caa5440a8f02582921f2db4a__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-ab44f4d8', 'hca_prod_2a64db431b554639aabb8dba0145689d__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-bfdde7e3', 'hca_prod_2a72a4e566b2405abb7c1e463e8febb0__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-f4d7c97e', 'hca_prod_2ad191cdbd7a409b9bd1e72b5e4cce81__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-e4d77c97', 'hca_prod_2af52a1365cb4973b51339be38f2df3f__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-aebdd74a', 'hca_prod_2b38025da5ea4c0fb22e367824bcaf4c__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-e67b97d4', 'hca_prod_2d8460958a334f3c97d4585bafac13b4__20220111_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-b123707e', 'hca_prod_2ef3655a973d4d699b4121fa4041eed7__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-3b845979', 'hca_prod_2f67614380c24bc6b7b42613fe0fadf0__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-40cecf86', 'hca_prod_3089d311f9ed44ddbb10397059bad4dc__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-e6d0e6ab', 'hca_prod_31887183a72c43089eacc6140313f39c__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-059455a6', 'hca_prod_34cba5e9ecb14d81bf0848987cd63073__20220111_dcp2_20220114_dcp12'),
    mksrc('bigquery', 'datarepo-18838720', 'hca_prod_376a7f55b8764f609cf3ed7bc83d5415__20220111_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-002f293a', 'hca_prod_379ed69ebe0548bcaf5ea7fc589709bf__20220111_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-94ea8d84', 'hca_prod_38449aea70b540db84b31e08f32efe34__20220111_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-597059bb', 'hca_prod_38e44dd0c3df418e9256d0824748901f__20220112_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-9b80ca5d', 'hca_prod_3a69470330844ece9abed935fd5f6748__20220112_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-caef7414', 'hca_prod_3c27d2ddb1804b2bbf05e2e418393fd1__20220112_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-d091ac22', 'hca_prod_3cfcdff5dee14a7ba591c09c6e850b11__20220112_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-ab983bdd', 'hca_prod_3e329187a9c448ec90e3cc45f7c2311c__20220112_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-5e5bce33', 'hca_prod_4037007b0eff4e6db7bd8dd8eec80143__20220112_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-c6ce3ced', 'hca_prod_403c3e7668144a2da5805dd5de38c7ff__20220113_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-d2fa6418', 'hca_prod_414accedeba0440fb721befbc5642bef__20220113_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-3ae19ddb', 'hca_prod_41fb1734a121461695c73b732c9433c7__20220113_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-50081b3c', 'hca_prod_42d4f8d454224b78adaee7c3c2ef511c__20220113_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-a7e55305', 'hca_prod_455b46e6d8ea4611861ede720a562ada__20220113_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-99250e4a', 'hca_prod_4a95101c9ffc4f30a809f04518a23803__20220113_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-d1983cfc', 'hca_prod_4af795f73e1d4341b8674ac0982b9efd__20220113_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-21212245', 'hca_prod_4bec484dca7a47b48d488830e06ad6db__20220113_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-001a2f34', 'hca_prod_4d6f6c962a8343d88fe10f53bffd4674__20220113_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-26396466', 'hca_prod_4e6f083b5b9a439398902a83da8188f1__20220113_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-3ef66093', 'hca_prod_50151324f3ed435898afec352a940a61__20220113_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-bd1c5759', 'hca_prod_504e0cee168840fab936361c4a831f87__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-6ab76705', 'hca_prod_5116c0818be749c58ce073b887328aa9__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-458232e4', 'hca_prod_51f02950ee254f4b8d0759aa99bb3498__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-3e19670d', 'hca_prod_520afa10f9d24e93ab7a26c4c863ce18__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-3eec204e', 'hca_prod_52b29aa4c8d642b4807ab35be94469ca__20220117_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-e7c01a93', 'hca_prod_52d10a60c8d14d068a5eaf0d5c0d5034__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-3b0847fe', 'hca_prod_53c53cd481274e12bc7f8fe1610a715c__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-d4f43fb3', 'hca_prod_54aaa409dc2848c5be26d368b4a5d5c6__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-6ed675f9', 'hca_prod_559bb888782941f2ace52c05c7eb81e9__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-5bdba230', 'hca_prod_56e73ccb7ae94faea738acfb69936d7a__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-6b1109e5', 'hca_prod_577c946d6de54b55a854cd3fde40bff2__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-d6e79c46', 'hca_prod_58028aa80ed249cab60f15e2ed5989d5__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-8494da48', 'hca_prod_591af954cdcd483996d3a0d1b1e885ac__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-17088287', 'hca_prod_5b3285614a9740acb7ad6a90fc59d374__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-4977894a', 'hca_prod_5b5f05b72482468db76d8f68c04a7a47__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-99725d7d', 'hca_prod_5bb1f67e2ff04848bbcf17d133f0fd2d__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-83783d1c', 'hca_prod_5eafb94b02d8423e81b83673da319ca0__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-f25df8f2', 'hca_prod_5ee710d7e2d54fe2818d15f5e31dae32__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-99348797', 'hca_prod_602628d7c03848a8aa97ffbb2cb44c9d__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-e8e29a46', 'hca_prod_6072616c87944b208f52fb15992ea5a4__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-bd224cce', 'hca_prod_60ea42e1af4942f58164d641fdb696bc__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-a4f706c9', 'hca_prod_63b5b6c1bbcd487d8c2e0095150c1ecd__20220117_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-0e494119', 'hca_prod_65858543530d48a6a670f972b34dfe10__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-c8ed0e98', 'hca_prod_67a3de0945b949c3a068ff4665daa50e__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-b1223d0f', 'hca_prod_68df3629d2d24eedb0aba10e0f019b88__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-b7734519', 'hca_prod_6c040a938cf84fd598de2297eb07e9f6__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-489f5a00', 'hca_prod_7027adc6c9c946f384ee9badc3a4f53b__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-465f2c7c', 'hca_prod_71436067ac414acebe1b2fbcc2cb02fa__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-574f6410', 'hca_prod_71eb5f6dcee04297b503b1125909b8c7__20220117_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-61e3e2d1', 'hca_prod_74493e9844fc48b0a58fcc7e77268b59__20220117_dcp2_20220120_dcp12'),
    mksrc('bigquery', 'datarepo-699bbe9b', 'hca_prod_74b6d5693b1142efb6b1a0454522b4a0__20220117_dcp2_20220124_dcp12'),
    mksrc('bigquery', 'datarepo-674de9c8', 'hca_prod_75dbbce90cde489c88a793e8f92914a3__20220117_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-d043e30f', 'hca_prod_769a08d1b8a44f1e95f76071a9827555__20220117_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-5c757273', 'hca_prod_783c9952a4ae4106a6ce56f20ce27f88__20220117_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-bc095feb', 'hca_prod_7880637a35a14047b422b5eac2a2a358__20220117_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-333e09de', 'hca_prod_78b2406dbff246fc8b6120690e602227__20220117_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-9268e5a3', 'hca_prod_79b13a2a9ca142a497bd70208a11bea6__20220117_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-93812eed', 'hca_prod_7ac8822c4ef04194adf074290611b1c6__20220117_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-db3813a8', 'hca_prod_7adede6a0ab745e69b67ffe7466bec1f__20220117_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-33a60e82', 'hca_prod_7b947aa243a74082afff222a3e3a4635__20220117_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-ccf60635', 'hca_prod_7c75f07c608d4c4aa1b7b13d11c0ad31__20220117_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-30e31b57', 'hca_prod_8185730f411340d39cc3929271784c2b__20220117_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-9d5ab6f0', 'hca_prod_83f5188e3bf749569544cea4f8997756__20220117_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-57af0017', 'hca_prod_842605c7375a47c59e2ca71c2c00fcad__20220117_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-c3aea89c', 'hca_prod_8559a8ed5d8c4fb6bde8ab639cebf03c__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-a054435f', 'hca_prod_8787c23889ef4636a57d3167e8b54a80__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-4d40e3cb', 'hca_prod_87d52a86bdc7440cb84d170f7dc346d9__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-486fab06', 'hca_prod_88ec040b87054f778f41f81e57632f7d__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-32fc3ac7', 'hca_prod_8999b4566fa6438bab17b62b1d8ec0c3__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-19e9b807', 'hca_prod_8a40ff19e6144c50b23b5c9e1d546bab__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-a71cbef5', 'hca_prod_8ab8726d81b94bd2acc24d50bee786b4__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-6cf8837e', 'hca_prod_8bd2e5f694534b9b9c5659e3a40dc87e__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-8383e25b', 'hca_prod_8c3c290ddfff4553886854ce45f4ba7f__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-d425ceae', 'hca_prod_8d566d35d8d34975a351be5e25e9b2ea__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-c15b7397', 'hca_prod_8dacb243e9184bd2bb9aaac6dc424161__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-8ed2742a', 'hca_prod_90bd693340c048d48d76778c103bf545__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-05d8344b', 'hca_prod_94023a08611d4f22a8c990956e091b2e__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-87faf2bd', 'hca_prod_946c5add47d1402a97bba5af97e8bce7__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-8238f8f6', 'hca_prod_955dfc2ca8c64d04aa4d907610545d11__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-0f11337c', 'hca_prod_95f07e6e6a734e1ba880c83996b3aa5c__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-f680e590', 'hca_prod_962bd805eb894c54bad2008e497d1307__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-9b7aa7dd', 'hca_prod_99101928d9b14aafb759e97958ac7403__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-8a2c2dfd', 'hca_prod_996120f9e84f409fa01e732ab58ca8b9__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-9385cdd8', 'hca_prod_9d97f01f9313416e9b07560f048b2350__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-ddcd2940', 'hca_prod_a004b1501c364af69bbd070c06dbc17d__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-16e78655', 'hca_prod_a29952d9925e40f48a1c274f118f1f51__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-9aa62158', 'hca_prod_a39728aa70a04201b0a281b7badf3e71__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-7180120b', 'hca_prod_a60803bbf7db45cfb52995436152a801__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-b4669bfd', 'hca_prod_a80a63f2e223489081b0415855b89abc__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-e899aaaa', 'hca_prod_a9301bebe9fa42feb75c84e8a460c733__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-373a5866', 'hca_prod_a96b71c078a742d188ce83c78925cfeb__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-92bd008d', 'hca_prod_a991ef154d4a4b80a93ec538b4b54127__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-6652ddcb', 'hca_prod_a9c022b4c7714468b769cabcf9738de3__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-4975e16f', 'hca_prod_abe1a013af7a45ed8c26f3793c24a1f4__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-be8901be', 'hca_prod_ad04c8e79b7d4cceb8e901e31da10b94__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-4a3b719a', 'hca_prod_ad98d3cd26fb4ee399c98a2ab085e737__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-d7f6d5fa', 'hca_prod_ae71be1dddd84feb9bed24c3ddb6e1ad__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-58678d36', 'hca_prod_b32a9915c81b4cbcaf533a66b5da3c9a__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-ff628beb', 'hca_prod_b4a7d12f6c2f40a39e359756997857e3__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-2a48ce64', 'hca_prod_b51f49b40d2e4cbdbbd504cd171fc2fa__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-ffc43998', 'hca_prod_b7259878436c4274bfffca76f4cb7892__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-41c56298', 'hca_prod_b9484e4edc404e389b854cecf5b8c068__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-0cbe9f7b', 'hca_prod_b963bd4b4bc14404842569d74bc636b8__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-d4fa6f7e', 'hca_prod_bd40033154b94fccbff66bb8b079ee1f__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-5cfa0843', 'hca_prod_bd7104c9a950490e94727d41c6b11c62__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-53d134a5', 'hca_prod_c1810dbc16d245c3b45e3e675f88d87b__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-8d82eeff', 'hca_prod_c1a9a93dd9de4e659619a9cec1052eaa__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-7357dda4', 'hca_prod_c31fa434c9ed4263a9b6d9ffb9d44005__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-4322539b', 'hca_prod_c4077b3c5c984d26a614246d12c2e5d7__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-c746ef64', 'hca_prod_c41dffbfad83447ca0e113e689d9b258__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-072807b7', 'hca_prod_c5ca43aa3b2b42168eb3f57adcbc99a1__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-62e87fe3', 'hca_prod_c5f4661568de4cf4bbc2a0ae10f08243__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-c59a45c5', 'hca_prod_c6ad8f9bd26a4811b2ba93d487978446__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-d0540cc6', 'hca_prod_c715cd2fdc7c44a69cd5b6a6d9f075ae__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-2b95c100', 'hca_prod_c893cb575c9f4f26931221b85be84313__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-dd3d8e06', 'hca_prod_cc95ff892e684a08a234480eca21ce79__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-2a5a6085', 'hca_prod_ccd1f1ba74ce469b9fc9f6faea623358__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-2b0f8836', 'hca_prod_ccef38d7aa9240109621c4c7b1182647__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-8d6a8dd5', 'hca_prod_cddab57b68684be4806f395ed9dd635a__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-202827af', 'hca_prod_ce33dde2382d448cb6acbfb424644f23__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-cde2c4a4', 'hca_prod_ce7b12ba664f4f798fc73de6b1892183__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-4f711011', 'hca_prod_d012d4768f8c4ff389d6ebbe22c1b5c1__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-a718a79a', 'hca_prod_d2111fac3fc44f429b6d32cd6a828267__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-12801bd1', 'hca_prod_d3446f0c30f34a12b7c36af877c7bb2d__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-dac6d601', 'hca_prod_d3a4ceac4d66498497042570c0647a56__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-1e99243c', 'hca_prod_d3ac7c1b53024804b611dad9f89c049d__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-e73ca25f', 'hca_prod_d7845650f6b14b1cb2fec0795416ba7b__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-7796a030', 'hca_prod_d7b7beae652b4fc09bf2bcda7c7115af__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-438137ee', 'hca_prod_da2747fa292142e0afd439ef57b2b88b__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-7f7fb2ac', 'hca_prod_daf9d9827ce643f6ab51272577290606__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-404d888e', 'hca_prod_dbcd4b1d31bd4eb594e150e8706fa192__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-ee4df1a4', 'hca_prod_dc1a41f69e0942a6959e3be23db6da56__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-89f05580', 'hca_prod_dd7f24360c564709bd17e526bba4cc15__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-d6e13093', 'hca_prod_df88f39f01a84b5b92f43177d6c0f242__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-319b223a', 'hca_prod_e0009214c0a04a7b96e2d6a83e966ce0__20220119_dcp2_20220126_dcp12'),
    mksrc('bigquery', 'datarepo-cd37664c', 'hca_prod_e0c74c7a20a445059cf138dcdd23011b__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-921c7df9', 'hca_prod_e526d91dcf3a44cb80c5fd7676b55a1d__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-8d441277', 'hca_prod_e57dc176ab98446b90c289e0842152fd__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-a2bba34f', 'hca_prod_e5d455791f5b48c3b568320d93e7ca72__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-32d08de8', 'hca_prod_e77fed30959d4fadbc15a0a5a85c21d2__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-de2f2f56', 'hca_prod_e8808cc84ca0409680f2bba73600cba6__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-b49ee748', 'hca_prod_eaefa1b6dae14414953b17b0427d061e__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-192f44d3', 'hca_prod_ede2e0b46652464fabbc0b2d964a25a0__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-79a515aa', 'hca_prod_ef1d9888fa8647a4bb720ab0f20f7004__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-8ddfa027', 'hca_prod_ef1e3497515e4bbe8d4c10161854b699__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-902930ad', 'hca_prod_efea6426510a4b609a19277e52bfa815__20220118_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-708835eb', 'hca_prod_f0f89c1474604bab9d4222228a91f185__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-cf6bd64d', 'hca_prod_f2fe82f044544d84b416a885f3121e59__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-44df5b5a', 'hca_prod_f48e7c39cc6740559d79bc437892840c__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-eb93ad96', 'hca_prod_f6133d2a9f3d4ef99c19c23d6c7e6cc0__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-e3c29b0f', 'hca_prod_f81efc039f564354aabb6ce819c3d414__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-11942c76', 'hca_prod_f83165c5e2ea4d15a5cf33f3550bffde__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-c64a357d', 'hca_prod_f86f1ab41fbb4510ae353ffd752d4dfc__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-4167b729', 'hca_prod_f8aa201c4ff145a4890e840d63459ca2__20220119_dcp2_20220121_dcp12'),
    mksrc('bigquery', 'datarepo-590e9f21', 'hca_prod_faeedcb0e0464be7b1ad80a3eeabb066__20220119_dcp2_20220121_dcp12'),
]))

dcp13_sources = mkdict(dcp12_sources, 208, mkdelta([
    # @formatter:off
    mksrc('bigquery', 'datarepo-c8f9ec5d', 'hca_prod_03c6fce7789e4e78a27a664d562bb738__20220110_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-991fac12', 'hca_prod_05657a599f9d4bb9b77b24be13aa5cea__20220110_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-b185fd85', 'hca_prod_05be4f374506429bb112506444507d62__20220107_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-30dc00be', 'hca_prod_065e6c13ad6b46a38075c3137eb03068__20220213_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-0285bfe0', 'hca_prod_06c7dd8d6cc64b79b7958805c47d36e1__20220213_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-bde655c7', 'hca_prod_102018327c7340339b653ef13d81656a__20220213_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-003ece01', 'hca_prod_1ce3b3dc02f244a896dad6d107b27a76__20220107_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-5a090360', 'hca_prod_1dddae6e375348afb20efa22abad125d__20220213_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-a0659f9b', 'hca_prod_1eb69a39b5b241ecafae5fe37f272756__20220213_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-cbdabbb4', 'hca_prod_23587fb31a4a4f58ad74cc9a4cb4c254__20220111_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-2ad5c040', 'hca_prod_2a72a4e566b2405abb7c1e463e8febb0__20220111_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-ca52c87a', 'hca_prod_2d8460958a334f3c97d4585bafac13b4__20220111_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-3da21f85', 'hca_prod_3a69470330844ece9abed935fd5f6748__20220112_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-f84c69b4', 'hca_prod_520afa10f9d24e93ab7a26c4c863ce18__20220117_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-fd9c289b', 'hca_prod_58028aa80ed249cab60f15e2ed5989d5__20220117_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-afff5936', 'hca_prod_67a3de0945b949c3a068ff4665daa50e__20220117_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-7e70b0df', 'hca_prod_6f89a7f38d4a4344aa4feccfe7e91076__20220213_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-cafbc244', 'hca_prod_78b2406dbff246fc8b6120690e602227__20220117_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-0558746b', 'hca_prod_78d7805bfdc8472b8058d92cf886f7a4__20220213_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-fb7a9fe5', 'hca_prod_8559a8ed5d8c4fb6bde8ab639cebf03c__20220118_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-5ee4d674', 'hca_prod_85a9263b088748edab1addfa773727b6__20220224_dcp2_20220224_dcp13'),
    mksrc('bigquery', 'datarepo-604c0800', 'hca_prod_88ec040b87054f778f41f81e57632f7d__20220118_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-651b3c64', 'hca_prod_8c3c290ddfff4553886854ce45f4ba7f__20220118_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-9029753d', 'hca_prod_99101928d9b14aafb759e97958ac7403__20220118_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-0a0a2225', 'hca_prod_9c20a245f2c043ae82c92232ec6b594f__20220212_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-9385cdd8', 'hca_prod_9d97f01f9313416e9b07560f048b2350__20220118_dcp2_20220121_dcp12', pop), # noqa E501
    mksrc('bigquery', 'datarepo-3dda61fd', 'hca_prod_ccd1f1ba74ce469b9fc9f6faea623358__20220118_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-021d07c6', 'hca_prod_ccef38d7aa9240109621c4c7b1182647__20220118_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-8c5ae0d1', 'hca_prod_cd61771b661a4e19b2696e5d95350de6__20220213_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-e69f2dd7', 'hca_prod_d6225aee8f0e4b20a20c682509a9ea14__20220213_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-b11dcc58', 'hca_prod_d71c76d336704774a9cf034249d37c60__20220213_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-e251e383', 'hca_prod_dbd836cfbfc241f0983441cc6c0b235a__20220212_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-ce17ac99', 'hca_prod_dd7ada843f144765b7ce9b64642bb3dc__20220212_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-8e3d7fce', 'hca_prod_e8808cc84ca0409680f2bba73600cba6__20220118_dcp2_20220214_dcp13'),
    mksrc('bigquery', 'datarepo-43f772c9', 'hca_prod_f6133d2a9f3d4ef99c19c23d6c7e6cc0__20220119_dcp2_20220214_dcp13'),
    # @formatter:on
]))

dcp14_sources = mkdict(dcp13_sources, 218, mkdelta([
    mksrc('bigquery', 'datarepo-ef305f42', 'hca_prod_005d611a14d54fbf846e571a1f874f70__20220111_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-4fb4619a', 'hca_prod_074a9f88729a455dbca50ce80edf0cea__20220107_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-1dbff5cd', 'hca_prod_091cf39b01bc42e59437f419a66c8a45__20220107_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-73b30762', 'hca_prod_116965f3f09447699d28ae675c1b569c__20220107_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-ecd9f488', 'hca_prod_165dea71a95a44e188cdb2d9ad68bb1e__20220303_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-c3ca85db', 'hca_prod_24d0dbbc54eb49048141934d26f1c936__20220303_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-6eecb96e', 'hca_prod_2c041c26f75a495fab36a076f89d422a__20220303_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-99fdfa87', 'hca_prod_3cdaf942f8ad42e8a77b4efedb9ea7b6__20220303_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-cf90c331', 'hca_prod_403c3e7668144a2da5805dd5de38c7ff__20220113_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-b9918259', 'hca_prod_4a95101c9ffc4f30a809f04518a23803__20220113_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-77f534b9', 'hca_prod_4bec484dca7a47b48d488830e06ad6db__20220113_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-b230b42b', 'hca_prod_4d6f6c962a8343d88fe10f53bffd4674__20220113_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-b83d5d98', 'hca_prod_4e6f083b5b9a439398902a83da8188f1__20220113_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-d7e92ae1', 'hca_prod_5116c0818be749c58ce073b887328aa9__20220117_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-9e63ca34', 'hca_prod_53c53cd481274e12bc7f8fe1610a715c__20220117_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-6b360d3f', 'hca_prod_5b5f05b72482468db76d8f68c04a7a47__20220117_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-47534f24', 'hca_prod_6ac8e777f9a04288b5b0446e8eba3078__20220303_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-aa6a9210', 'hca_prod_74b6d5693b1142efb6b1a0454522b4a0__20220117_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-7274c749', 'hca_prod_7b947aa243a74082afff222a3e3a4635__20220117_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-06d0218d', 'hca_prod_8185730f411340d39cc3929271784c2b__20220117_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-958f743f', 'hca_prod_91af6e2f65f244ec98e0ba4e98db22c8__20220303_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-8ef24363', 'hca_prod_95f07e6e6a734e1ba880c83996b3aa5c__20220118_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-bc66239d', 'hca_prod_abe1a013af7a45ed8c26f3793c24a1f4__20220118_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-ccddf7b7', 'hca_prod_b963bd4b4bc14404842569d74bc636b8__20220118_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-145862d0', 'hca_prod_c05184453b3b49c6b8fcc41daa4eacba__20220213_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-1d4ac83f', 'hca_prod_c211fd49d9804ba18c6ac24254a3cb52__20220303_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-a7ff96eb', 'hca_prod_c4077b3c5c984d26a614246d12c2e5d7__20220118_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-ff4ee826', 'hca_prod_c6a50b2a3dfd4ca89b483e682f568a25__20220303_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-15efafd9', 'hca_prod_cc95ff892e684a08a234480eca21ce79__20220118_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-264555df', 'hca_prod_e5d455791f5b48c3b568320d93e7ca72__20220119_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-9cbb67c6', 'hca_prod_f29b124a85974862ae98ff3a0fd9033e__20220303_dcp2_20220307_dcp14'),
    mksrc('bigquery', 'datarepo-09a8dd1a', 'hca_prod_f83165c5e2ea4d15a5cf33f3550bffde__20220119_dcp2_20220307_dcp14'),
]))

dcp15_sources = mkdict(dcp14_sources, 237, mkdelta([
    mksrc('bigquery', 'datarepo-bb0322f9', 'hca_prod_04ad400c58cb40a5bc2b2279e13a910b__20220114_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-4c006992', 'hca_prod_0562d2ae0b8a459ebbc06357108e5da9__20220330_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-625580ba', 'hca_prod_0777b9ef91f3468b9deadb477437aa1a__20220330_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-c6460226', 'hca_prod_18d4aae283634e008eebb9e568402cf8__20220330_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-9e1d30cd', 'hca_prod_1ce3b3dc02f244a896dad6d107b27a76__20220107_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-426125f5', 'hca_prod_2b38025da5ea4c0fb22e367824bcaf4c__20220111_dcp2_20220331_dcp15'),
    mksrc('bigquery', 'datarepo-67ebf8c0', 'hca_prod_40272c3b46974bd4ba3f82fa96b9bf71__20220303_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-7e581d49', 'hca_prod_40ca2a03ec0f471fa834948199495fe7__20220330_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-4b461192', 'hca_prod_45c2c853d06f4879957ef1366fb5d423__20220303_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-b5a6fdd9', 'hca_prod_5116c0818be749c58ce073b887328aa9__20220117_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-abf80711', 'hca_prod_65d7a1684d624bc083244e742aa62de6__20220330_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-4a1d1031', 'hca_prod_6621c827b57a4268bc80df4049140193__20220330_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-ecd5ed43', 'hca_prod_6ac8e777f9a04288b5b0446e8eba3078__20220303_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-993d35db', 'hca_prod_6f89a7f38d4a4344aa4feccfe7e91076__20220213_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-fb756d63', 'hca_prod_73769e0a5fcd41f4908341ae08bfa4c1__20220330_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-b174a30e', 'hca_prod_77780d5603c0481faade2038490cef9f__20220330_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-54af5ab6', 'hca_prod_91af6e2f65f244ec98e0ba4e98db22c8__20220303_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-89b77174', 'hca_prod_957261f72bd64358a6ed24ee080d5cfc__20220330_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-c95907eb', 'hca_prod_99101928d9b14aafb759e97958ac7403__20220118_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-a186fcb1', 'hca_prod_a2a2f324cf24409ea859deaee871269c__20220330_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-b44b5550', 'hca_prod_a815c84b8999433f958e422c0720e00d__20220330_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-89acf5db', 'hca_prod_aefb919243fc46d7a4c129597f7ef61b__20220330_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-06565264', 'hca_prod_aff9c3cd6b844fc2abf2b9c0b3038277__20220330_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-0bb76e5c', 'hca_prod_c1810dbc16d245c3b45e3e675f88d87b__20220118_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-5d6926b7', 'hca_prod_c7c54245548b4d4fb15e0d7e238ae6c8__20220330_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-46a00828', 'hca_prod_dc1a41f69e0942a6959e3be23db6da56__20220119_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-e9f2b830', 'hca_prod_e255b1c611434fa683a8528f15b41038__20220330_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-c93c8ea6', 'hca_prod_f2fe82f044544d84b416a885f3121e59__20220119_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-d5d5cacf', 'hca_prod_fa3f460f4fb94cedb5488ba6a8ecae3f__20220330_dcp2_20220330_dcp15'),
    mksrc('bigquery', 'datarepo-b60aabf3', 'hca_prod_fde199d2a8414ed1aa65b9e0af8969b1__20220330_dcp2_20220330_dcp15'),
]))

dcp16_sources = mkdict(dcp15_sources, 250, mkdelta([
    mksrc('bigquery', 'datarepo-c531f177', 'hca_prod_0562d2ae0b8a459ebbc06357108e5da9__20220330_dcp2_20220519_dcp16'),
    mksrc('bigquery', 'datarepo-589be0ea', 'hca_prod_0b29914025b54861a69f7651ff3f46cf__20220519_dcp2_20220519_dcp16'),
    mksrc('bigquery', 'datarepo-a584f228', 'hca_prod_16cd67912adb4d0f82220184dada6456__20220519_dcp2_20220519_dcp16'),
    mksrc('bigquery', 'datarepo-84b4312d', 'hca_prod_18e5843776b740218ede3f0b443fa915__20220519_dcp2_20220519_dcp16'),
    mksrc('bigquery', 'datarepo-5ba935e0', 'hca_prod_2253ae594cc54bd2b44eecb6d3fd7646__20220519_dcp2_20220519_dcp16'),
    mksrc('bigquery', 'datarepo-bc2fe57d', 'hca_prod_24d0dbbc54eb49048141934d26f1c936__20220303_dcp2_20220519_dcp16'),
    mksrc('bigquery', 'datarepo-e227ee12', 'hca_prod_3cdaf942f8ad42e8a77b4efedb9ea7b6__20220303_dcp2_20220519_dcp16'),
    mksrc('bigquery', 'datarepo-3b6cd966', 'hca_prod_425c2759db664c93a358a562c069b1f1__20220519_dcp2_20220519_dcp16'),
    mksrc('bigquery', 'datarepo-26738b05', 'hca_prod_6663070ffd8b41a9a4792d1e07afa201__20220519_dcp2_20220519_dcp16'),
    mksrc('bigquery', 'datarepo-3dc96215', 'hca_prod_7b393e4d65bc4c03b402aae769299329__20220519_dcp2_20220519_dcp16'),
    mksrc('bigquery', 'datarepo-c6955be8', 'hca_prod_94e4ee099b4b410a84dca751ad36d0df__20220519_dcp2_20220519_dcp16'),
    mksrc('bigquery', 'datarepo-721e0608', 'hca_prod_b51f49b40d2e4cbdbbd504cd171fc2fa__20220118_dcp2_20220519_dcp16'),
    mksrc('bigquery', 'datarepo-9f724133', 'hca_prod_b733dc1b1d5545e380367eab0821742c__20220519_dcp2_20220519_dcp16'),
    mksrc('bigquery', 'datarepo-3403d8d8', 'hca_prod_c16a754f5da346ed8c1e6426af2ef625__20220519_dcp2_20220519_dcp16'),
    mksrc('bigquery', 'datarepo-54c4ef0b', 'hca_prod_daa371e81ec343ef924f896d901eab6f__20220519_dcp2_20220519_dcp16'),
    mksrc('bigquery', 'datarepo-190ddba0', 'hca_prod_e9f36305d85744a393f0df4e6007dc97__20220519_dcp2_20220519_dcp16'),
    mksrc('bigquery', 'datarepo-b3a12f99', 'hca_prod_f4d011ced1f548a4ab61ae14176e3a6e__20220519_dcp2_20220519_dcp16'),
]))

dcp17_sources = mkdict(dcp16_sources, 261, mkdelta([
    # @formatter:off
    mksrc('bigquery', 'datarepo-0c908bdf', 'hca_prod_005d611a14d54fbf846e571a1f874f70__20220111_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-91af9f1b', 'hca_prod_04ad400c58cb40a5bc2b2279e13a910b__20220114_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-adcbf4c3', 'hca_prod_18d4aae283634e008eebb9e568402cf8__20220330_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-4cc7b9fb', 'hca_prod_20f37aafcaa140e69123be6ce8feb2d6__20220111_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-b4210c33', 'hca_prod_2eb4f5f842a54368aa2d337bacb96197__20220606_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-ba2650b6', 'hca_prod_2fe3c60bac1a4c619b59f6556c0fce63__20220606_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-eb73a0f3', 'hca_prod_34da2c5f801148afa7fdad2f56ec10f4__20220606_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-0d3feb7c', 'hca_prod_376a7f55b8764f609cf3ed7bc83d5415__20220111_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-7cfb2129', 'hca_prod_3c27d2ddb1804b2bbf05e2e418393fd1__20220112_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-cbdb9b65', 'hca_prod_3cfcdff5dee14a7ba591c09c6e850b11__20220112_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-c1f0a228', 'hca_prod_425c2759db664c93a358a562c069b1f1__20220519_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-261ff5ff', 'hca_prod_4f17edf6e9f042afa54af02fdca76ade__20220606_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-c9a47174', 'hca_prod_5b5f05b72482468db76d8f68c04a7a47__20220117_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-7dd487af', 'hca_prod_5bb1f67e2ff04848bbcf17d133f0fd2d__20220117_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-c6191eb9', 'hca_prod_6f89a7f38d4a4344aa4feccfe7e91076__20220213_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-8b966ec9', 'hca_prod_71436067ac414acebe1b2fbcc2cb02fa__20220117_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-3a9d88c9', 'hca_prod_7880637a35a14047b422b5eac2a2a358__20220117_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-63ab653d', 'hca_prod_78b2406dbff246fc8b6120690e602227__20220117_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-b79b6e00', 'hca_prod_7be050259972493a856f3342a8d1b183__20220606_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-7d012d70', 'hca_prod_8999b4566fa6438bab17b62b1d8ec0c3__20220118_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-385ef7e4', 'hca_prod_8a40ff19e6144c50b23b5c9e1d546bab__20220118_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-0339714f', 'hca_prod_8bd2e5f694534b9b9c5659e3a40dc87e__20220118_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-492bd104', 'hca_prod_a27dd61925ad46a0ae0c5c4940a1139b__20220606_dcp2_20220607_dcp17', pop),  # noqa E501
    mksrc('bigquery', 'datarepo-bc83ab27', 'hca_prod_a2a2f324cf24409ea859deaee871269c__20220330_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-10a33a05', 'hca_prod_a62dae2ecd694d5cb5f84f7e8abdbafa__20220606_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-e3d0317e', 'hca_prod_a9f5323ace71471c9caf04cc118fd1d7__20220606_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-cd2ab73f', 'hca_prod_ad04c8e79b7d4cceb8e901e31da10b94__20220118_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-dcd2f9cf', 'hca_prod_aff9c3cd6b844fc2abf2b9c0b3038277__20220330_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-c9b6cc1c', 'hca_prod_b9484e4edc404e389b854cecf5b8c068__20220118_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-49083689', 'hca_prod_bd7104c9a950490e94727d41c6b11c62__20220118_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-c29ee607', 'hca_prod_c302fe54d22d451fa130e24df3d6afca__20220606_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-50fa4c1b', 'hca_prod_d138a1147df54f7d9ff1f79dfd2d428f__20220606_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-9810d23f', 'hca_prod_d3446f0c30f34a12b7c36af877c7bb2d__20220119_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-3171dab6', 'hca_prod_da2747fa292142e0afd439ef57b2b88b__20220119_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-693c392c', 'hca_prod_dbcd4b1d31bd4eb594e150e8706fa192__20220119_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-b9410272', 'hca_prod_e993adcdd4ba4f889a05d1c05bdf0c45__20220606_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-4647591c', 'hca_prod_e9f36305d85744a393f0df4e6007dc97__20220519_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-d20af009', 'hca_prod_f29b124a85974862ae98ff3a0fd9033e__20220303_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-959ea334', 'hca_prod_f6133d2a9f3d4ef99c19c23d6c7e6cc0__20220119_dcp2_20220607_dcp17'),
    mksrc('bigquery', 'datarepo-a09e8946', 'hca_prod_fccd3f50cde247bf8972a293b5928aea__20220606_dcp2_20220607_dcp17'),
    # @formatter:on
]))

dcp18_sources = mkdict(dcp17_sources, 267, mkdelta([
    mksrc('bigquery', 'datarepo-3fa8ab06', 'hca_prod_1538d572bcb7426b8d2c84f3a7f87bb0__20220630_dcp2_20220630_dcp18'),
    mksrc('bigquery', 'datarepo-7a96c98c', 'hca_prod_8a666b76daaf4b1f9414e4807a1d1e8b__20220630_dcp2_20220630_dcp18'),
    mksrc('bigquery', 'datarepo-65814a19', 'hca_prod_9833669bd6944b93a3d06b6f9dbcfc10__20220630_dcp2_20220630_dcp18'),
    mksrc('bigquery', 'datarepo-935adc8a', 'hca_prod_9fc0064b84ce40a5a768e6eb3d364ee0__20220630_dcp2_20220630_dcp18'),
    mksrc('bigquery', 'datarepo-38233156', 'hca_prod_a27dd61925ad46a0ae0c5c4940a1139b__20220606_dcp2_20220630_dcp18'),
    mksrc('bigquery', 'datarepo-57050405', 'hca_prod_a62dae2ecd694d5cb5f84f7e8abdbafa__20220606_dcp2_20220630_dcp18'),
    mksrc('bigquery', 'datarepo-99178745', 'hca_prod_b4a7d12f6c2f40a39e359756997857e3__20220118_dcp2_20220630_dcp18'),
    mksrc('bigquery', 'datarepo-38191a90', 'hca_prod_b51f49b40d2e4cbdbbd504cd171fc2fa__20220118_dcp2_20220630_dcp18'),
    mksrc('bigquery', 'datarepo-0e7c311b', 'hca_prod_c4e1136978d44d29ba8eb67907c4c65c__20220630_dcp2_20220630_dcp18'),
    mksrc('bigquery', 'datarepo-21969ae7', 'hca_prod_e9f36305d85744a393f0df4e6007dc97__20220519_dcp2_20220630_dcp18'),
]))

dcp19_sources = mkdict(dcp18_sources, 276, mkdelta([
    mksrc('bigquery', 'datarepo-f15e3b59', 'hca_prod_005d611a14d54fbf846e571a1f874f70__20220111_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-995a6952', 'hca_prod_04ad400c58cb40a5bc2b2279e13a910b__20220114_dcp2_20220805_dcp19'),
    mksrc('bigquery', 'datarepo-d48e7a0f', 'hca_prod_0562d2ae0b8a459ebbc06357108e5da9__20220330_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-ca2968d6', 'hca_prod_05657a599f9d4bb9b77b24be13aa5cea__20220110_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-9c3b164c', 'hca_prod_074a9f88729a455dbca50ce80edf0cea__20220107_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-413da832', 'hca_prod_0777b9ef91f3468b9deadb477437aa1a__20220330_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-5b9d4163', 'hca_prod_0b29914025b54861a69f7651ff3f46cf__20220519_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-4e2997d7', 'hca_prod_135f7f5c4a854bcf9f7c4f035ff1e428__20220729_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-44258246', 'hca_prod_20f37aafcaa140e69123be6ce8feb2d6__20220111_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-7302c74c', 'hca_prod_24d0dbbc54eb49048141934d26f1c936__20220303_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-4ff95533', 'hca_prod_2c041c26f75a495fab36a076f89d422a__20220303_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-b839ef2e', 'hca_prod_2f67614380c24bc6b7b42613fe0fadf0__20220111_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-67845d12', 'hca_prod_34da2c5f801148afa7fdad2f56ec10f4__20220606_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-00aa7019', 'hca_prod_3c9d586ebd264b4686903faaa18ccf38__20220729_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-16ec881b', 'hca_prod_3e92c74d256c40cd927316f155da8342__20220729_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-4d2761c3', 'hca_prod_40272c3b46974bd4ba3f82fa96b9bf71__20220303_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-bc41fa3c', 'hca_prod_425c2759db664c93a358a562c069b1f1__20220519_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-6aadacef', 'hca_prod_45c2c853d06f4879957ef1366fb5d423__20220303_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-2ea93600', 'hca_prod_4d9d56e4610d4748b57df8315e3f53a3__20220729_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-a59dcc04', 'hca_prod_51f02950ee254f4b8d0759aa99bb3498__20220117_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-d07fd1c7', 'hca_prod_5b3285614a9740acb7ad6a90fc59d374__20220117_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-a0b7c8ae', 'hca_prod_5b5f05b72482468db76d8f68c04a7a47__20220117_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-465c9e6a', 'hca_prod_5b910a437fb54ea7b9d643dbd1bf2776__20220729_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-75be85e4', 'hca_prod_6ac8e777f9a04288b5b0446e8eba3078__20220303_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-8336318c', 'hca_prod_7be050259972493a856f3342a8d1b183__20220606_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-57425cd8', 'hca_prod_88ec040b87054f778f41f81e57632f7d__20220118_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-cc58cf4d', 'hca_prod_8f630e0f6bf94a04975402533152a954__20220729_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-c20ed7ec', 'hca_prod_91af6e2f65f244ec98e0ba4e98db22c8__20220303_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-071fb08c', 'hca_prod_94e4ee099b4b410a84dca751ad36d0df__20220519_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-2f190159', 'hca_prod_957261f72bd64358a6ed24ee080d5cfc__20220330_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-8cdacdcd', 'hca_prod_a1312f9a01ef40a789bf9091ca76a03a__20220729_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-36f14100', 'hca_prod_a62dae2ecd694d5cb5f84f7e8abdbafa__20220606_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-f0901ade', 'hca_prod_a9f5323ace71471c9caf04cc118fd1d7__20220606_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-b8655ece', 'hca_prod_c05184453b3b49c6b8fcc41daa4eacba__20220213_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-dd7e0cf4', 'hca_prod_c16a754f5da346ed8c1e6426af2ef625__20220519_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-3b8b21f1', 'hca_prod_c1a9a93dd9de4e659619a9cec1052eaa__20220118_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-9e1a0138', 'hca_prod_c5f4661568de4cf4bbc2a0ae10f08243__20220118_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-4af209c5', 'hca_prod_c7c54245548b4d4fb15e0d7e238ae6c8__20220330_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-4888a055', 'hca_prod_d2111fac3fc44f429b6d32cd6a828267__20220119_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-5af7a6fe', 'hca_prod_d3ac7c1b53024804b611dad9f89c049d__20220119_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-3c496a8c', 'hca_prod_d71c76d336704774a9cf034249d37c60__20220213_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-9b9df5aa', 'hca_prod_d7b7beae652b4fc09bf2bcda7c7115af__20220119_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-7769445f', 'hca_prod_da9d6f243bdf4eaa9e3ff47ce2a65b36__20220729_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-68a6d3c9', 'hca_prod_daf9d9827ce643f6ab51272577290606__20220119_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-ddce2a24', 'hca_prod_df88f39f01a84b5b92f43177d6c0f242__20220119_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-4f8a62b5', 'hca_prod_e0009214c0a04a7b96e2d6a83e966ce0__20220119_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-391955ef', 'hca_prod_e526d91dcf3a44cb80c5fd7676b55a1d__20220119_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-26d7f723', 'hca_prod_e57dc176ab98446b90c289e0842152fd__20220119_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-234ad9b8', 'hca_prod_e9f36305d85744a393f0df4e6007dc97__20220519_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-125471a9', 'hca_prod_ec6476ee294941f3947b8eef41d6d3ac__20220729_dcp2_20220804_dcp19'),
    mksrc('bigquery', 'datarepo-f91198ce', 'hca_prod_f86f1ab41fbb4510ae353ffd752d4dfc__20220119_dcp2_20220804_dcp19'),
]))

dcp20_sources = mkdict(dcp19_sources, 288, mkdelta([
    mksrc('bigquery', 'datarepo-7a619b7f', 'hca_prod_34da2c5f801148afa7fdad2f56ec10f4__20220606_dcp2_20220907_dcp20'),
    mksrc('bigquery', 'datarepo-ab0fbc7f', 'hca_prod_425c2759db664c93a358a562c069b1f1__20220519_dcp2_20220907_dcp20'),
    mksrc('bigquery', 'datarepo-86ffb30f', 'hca_prod_4c73d1e4bad24a22a0ba55abbdbdcc3d__20220906_dcp2_20220907_dcp20'),
    mksrc('bigquery', 'datarepo-68f0a599', 'hca_prod_66d7d92ad6c5492c815bf81c7c93c984__20220906_dcp2_20220907_dcp20'),
    mksrc('bigquery', 'datarepo-999b7ef7', 'hca_prod_74e2ef9d7c9f418cb2817fb38f3b1571__20220906_dcp2_20220907_dcp20'),
    mksrc('bigquery', 'datarepo-d9e203e6', 'hca_prod_7be050259972493a856f3342a8d1b183__20220606_dcp2_20220907_dcp20'),
    mksrc('bigquery', 'datarepo-60b7f35f', 'hca_prod_7f351a4cd24c4fcd9040f79071b097d0__20220906_dcp2_20220909_dcp20'),
    mksrc('bigquery', 'datarepo-3ef6c389', 'hca_prod_8b9cb6ae6a434e47b9fb3df7aeec941f__20220906_dcp2_20220907_dcp20'),
    mksrc('bigquery', 'datarepo-b6764413', 'hca_prod_923d323172954184b3f6c3082766a8c7__20220906_dcp2_20220907_dcp20'),
    mksrc('bigquery', 'datarepo-c341e9ae', 'hca_prod_9a23ac2d93dd4bac9bb8040e6426db9d__20220906_dcp2_20220907_dcp20'),
    mksrc('bigquery', 'datarepo-a07e8478', 'hca_prod_9ac53858606a4b89af49804ccedaa660__20220906_dcp2_20220907_dcp20'),
    mksrc('bigquery', 'datarepo-6d6c8d6e', 'hca_prod_9b876d3107394e969846f76e6a427279__20220906_dcp2_20220907_dcp20'),
    mksrc('bigquery', 'datarepo-4bad599c', 'hca_prod_a7c66eb14a4e4f6c9e30ad2a485f8301__20220906_dcp2_20220907_dcp20'),
    mksrc('bigquery', 'datarepo-b4a08996', 'hca_prod_a815c84b8999433f958e422c0720e00d__20220330_dcp2_20220907_dcp20'),
    mksrc('bigquery', 'datarepo-90b9fc26', 'hca_prod_ac289b77fb124a6bad43c0721c698e70__20220906_dcp2_20220907_dcp20'),
    mksrc('bigquery', 'datarepo-e2dfd11f', 'hca_prod_daf9d9827ce643f6ab51272577290606__20220119_dcp2_20220907_dcp20'),
    mksrc('bigquery', 'datarepo-bfc87e2c', 'hca_prod_e9f36305d85744a393f0df4e6007dc97__20220519_dcp2_20220907_dcp20'),
    mksrc('bigquery', 'datarepo-17b90eb5', 'hca_prod_fcaa53cdba574bfeaf9ceaa958f95c1a__20220906_dcp2_20220907_dcp20'),
]))

dcp21_sources = mkdict(dcp20_sources, 293, mkdelta([
    mksrc('bigquery', 'datarepo-f8cc03ae', 'hca_prod_03c6fce7789e4e78a27a664d562bb738__20220110_dcp2_20221011_dcp21'),
    mksrc('bigquery', 'datarepo-1f11d8e0', 'hca_prod_0b29914025b54861a69f7651ff3f46cf__20220519_dcp2_20221011_dcp21'),
    mksrc('bigquery', 'datarepo-eeee85f1', 'hca_prod_1fa8b11f56fa45a6a7776af70e17a6b3__20220928_dcp2_20221011_dcp21'),
    mksrc('bigquery', 'datarepo-e2f09b06', 'hca_prod_34ec845bcd7a4c4399e4d2932d5d85bb__20220928_dcp2_20221011_dcp21'),
    mksrc('bigquery', 'datarepo-abf25e53', 'hca_prod_575c0ad9c78e469b9fdf9a68dd881137__20220928_dcp2_20221011_dcp21'),
    mksrc('bigquery', 'datarepo-41b246f0', 'hca_prod_615158205bb845d08d12f0850222ecf0__20221007_dcp2_20221011_dcp21'),
    mksrc('bigquery', 'datarepo-a5548d96', 'hca_prod_7f351a4cd24c4fcd9040f79071b097d0__20220906_dcp2_20221011_dcp21'),
    mksrc('bigquery', 'datarepo-ee2f9607', 'hca_prod_a62dae2ecd694d5cb5f84f7e8abdbafa__20220606_dcp2_20221011_dcp21'),
    mksrc('bigquery', 'datarepo-ccacece4', 'hca_prod_c0d82ef215044ef09e5ed8a13e45fdec__20220928_dcp2_20221011_dcp21'),
]))

dcp22_sources = mkdict(dcp21_sources, 303, mkdelta([
    mksrc('bigquery', 'datarepo-89e53cfa', 'hca_prod_0d4aaaac02c344c48ae04465f97f83ed__20221101_dcp2_20221101_dcp22'),
    mksrc('bigquery', 'datarepo-8d629004', 'hca_prod_16e9915978bc44aab47955a5e903bf50__20221101_dcp2_20221101_dcp22'),
    mksrc('bigquery', 'datarepo-69c9824a', 'hca_prod_21ea8ddb525f4f1fa82031f0360399a2__20220111_dcp2_20221101_dcp22'),
    mksrc('bigquery', 'datarepo-fe5ea9a7', 'hca_prod_2837165560ba449ea3035859b29ead65__20221101_dcp2_20221101_dcp22'),
    mksrc('bigquery', 'datarepo-96dc6466', 'hca_prod_2b81ecc46ee0438f8c5bc10b2464069e__20221101_dcp2_20221101_dcp22'),
    mksrc('bigquery', 'datarepo-330124fc', 'hca_prod_34c9a62ca6104e31b3438fb7be676f8c__20221101_dcp2_20221101_dcp22'),
    mksrc('bigquery', 'datarepo-31ee9b01', 'hca_prod_4a95101c9ffc4f30a809f04518a23803__20220113_dcp2_20221101_dcp22'),
    mksrc('bigquery', 'datarepo-a03f3f9a', 'hca_prod_94023a08611d4f22a8c990956e091b2e__20220118_dcp2_20221101_dcp22'),
    mksrc('bigquery', 'datarepo-9c08dc57', 'hca_prod_957261f72bd64358a6ed24ee080d5cfc__20220330_dcp2_20221102_dcp22'),
    mksrc('bigquery', 'datarepo-93c53553', 'hca_prod_990d251f6dab4a98a2b66cfe7e4708b9__20221101_dcp2_20221101_dcp22'),
    mksrc('bigquery', 'datarepo-93db2e27', 'hca_prod_9e3370a0144a49a99e926f6a9290125a__20221101_dcp2_20221101_dcp22'),
    mksrc('bigquery', 'datarepo-d0ed9366', 'hca_prod_c8e6c5d9fcde4845beadff96999e3051__20221101_dcp2_20221101_dcp22'),
    mksrc('bigquery', 'datarepo-a5bd2972', 'hca_prod_dcbb50d19acf4f709fdab1f63a948c49__20221101_dcp2_20221101_dcp22'),
    mksrc('bigquery', 'datarepo-7e89d1a8', 'hca_prod_e4b18cd28f15490db9f1d118aa067dc3__20221101_dcp2_20221101_dcp22'),
]))

dcp23_sources = mkdict(dcp22_sources, 313, mkdelta([
    mksrc('bigquery', 'datarepo-1a3040e1', 'hca_prod_0751843070314bdfa3ce1bf0917a1923__20221208_dcp2_20221208_dcp23'),
    mksrc('bigquery', 'datarepo-79dd7eb4', 'hca_prod_1ebe8c34454e4c28bd713a3e8b127be4__20221208_dcp2_20221208_dcp23'),
    mksrc('bigquery', 'datarepo-e0ff550c', 'hca_prod_258c5e15d1254f2d8b4ce3122548ec9b__20221208_dcp2_20221209_dcp23'),
    mksrc('bigquery', 'datarepo-2a9467bc', 'hca_prod_29ed827bc5394f4cbb6bce8f9173dfb7__20221208_dcp2_20221208_dcp23'),
    mksrc('bigquery', 'datarepo-023be474', 'hca_prod_34ec845bcd7a4c4399e4d2932d5d85bb__20220928_dcp2_20221208_dcp23'),
    mksrc('bigquery', 'datarepo-5d5e636b', 'hca_prod_48b198ef3d594e57900fdf54c2435669__20221208_dcp2_20221208_dcp23'),
    mksrc('bigquery', 'datarepo-354e1286', 'hca_prod_504e0cee168840fab936361c4a831f87__20220117_dcp2_20221208_dcp23'),
    mksrc('bigquery', 'datarepo-fa6adf44', 'hca_prod_79351583b21244bab473731bdcddb407__20221208_dcp2_20221208_dcp23'),
    mksrc('bigquery', 'datarepo-678f9dd2', 'hca_prod_b7259878436c4274bfffca76f4cb7892__20220118_dcp2_20221208_dcp23'),
    mksrc('bigquery', 'datarepo-cde3a08e', 'hca_prod_cc95ff892e684a08a234480eca21ce79__20220118_dcp2_20221209_dcp23'),
    mksrc('bigquery', 'datarepo-0fd803ec', 'hca_prod_cdc2d2706c99414288839bd95c041d05__20221208_dcp2_20221208_dcp23'),
    mksrc('bigquery', 'datarepo-c008323e', 'hca_prod_e0c74c7a20a445059cf138dcdd23011b__20220119_dcp2_20221209_dcp23'),
    mksrc('bigquery', 'datarepo-e3e51223', 'hca_prod_e6773550c1a6494986431a3154cf2670__20221208_dcp2_20221208_dcp23'),
    mksrc('bigquery', 'datarepo-8d65fbe5', 'hca_prod_ea9eec5a4fc24c5894d02fcb598732bc__20221208_dcp2_20221208_dcp23'),
    mksrc('bigquery', 'datarepo-6a118cd6', 'hca_prod_f4d011ced1f548a4ab61ae14176e3a6e__20220519_dcp2_20221208_dcp23'),
    mksrc('bigquery', 'datarepo-f7f5893b', 'hca_prod_fc381e70df1b407d813152ab523270bd__20221208_dcp2_20221208_dcp23')
]))

dcp24_sources = mkdict(dcp23_sources, 324, mkdelta([
    mksrc('bigquery', 'datarepo-b7dfe3eb', 'hca_prod_12f320548f184dae8959bfce7e3108e7__20230201_dcp2_20230210_dcp24'),
    mksrc('bigquery', 'datarepo-27434eaf', 'hca_prod_2d4d89f2ebeb467cae60a3efc5e8d4ba__20230206_dcp2_20230210_dcp24'),
    mksrc('bigquery', 'datarepo-215ae20a', 'hca_prod_3ce9ae94c469419a96375d138a4e642f__20230201_dcp2_20230210_dcp24'),
    mksrc('bigquery', 'datarepo-7230d8d4', 'hca_prod_6e60a555fd954aa28e293ec2ef01a580__20230206_dcp2_20230210_dcp24'),
    mksrc('bigquery', 'datarepo-c5864eb0', 'hca_prod_77dedd59137648879bcadc42b56d5b7a__20230201_dcp2_20230210_dcp24'),
    mksrc('bigquery', 'datarepo-f3272b0a', 'hca_prod_8787c23889ef4636a57d3167e8b54a80__20220118_dcp2_20230210_dcp24'),
    mksrc('bigquery', 'datarepo-b1aa4336', 'hca_prod_957261f72bd64358a6ed24ee080d5cfc__20220330_dcp2_20230210_dcp24'),
    mksrc('bigquery', 'datarepo-83e0bc68', 'hca_prod_95d058bc9cec4c888d2c05b4a45bf24f__20230201_dcp2_20230210_dcp24'),
    mksrc('bigquery', 'datarepo-6a865365', 'hca_prod_cbd2911f252b4428abde69e270aefdfc__20230201_dcp2_20230210_dcp24'),
    mksrc('bigquery', 'datarepo-da0b7e39', 'hca_prod_cd9d6360ce38432197dff13c79e3cb84__20230206_dcp2_20230210_dcp24'),
    mksrc('bigquery', 'datarepo-19358e1b', 'hca_prod_cdabcf0b76024abf9afb3b410e545703__20230201_dcp2_20230210_dcp24'),
    mksrc('bigquery', 'datarepo-4582b46e', 'hca_prod_e57dc176ab98446b90c289e0842152fd__20220119_dcp2_20230210_dcp24'),
    mksrc('bigquery', 'datarepo-06c4cdf8', 'hca_prod_e88714c22e7849da81465a60b50628b4__20230206_dcp2_20230210_dcp24'),
    mksrc('bigquery', 'datarepo-7e506a9c', 'hca_prod_f2078d5f2e7d48448552f7c41a231e52__20230201_dcp2_20230210_dcp24')
]))

dcp25_sources = mkdict(dcp24_sources, 333, mkdelta([
    # @formatter:off
    mksrc('bigquery', 'datarepo-3b7ecb2b', 'hca_prod_0562d2ae0b8a459ebbc06357108e5da9__20220330_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-23177a5e', 'hca_prod_065e6c13ad6b46a38075c3137eb03068__20220213_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-28f02436', 'hca_prod_0751843070314bdfa3ce1bf0917a1923__20221208_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-2be1e691', 'hca_prod_0777b9ef91f3468b9deadb477437aa1a__20220330_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-2ace314a', 'hca_prod_0b29914025b54861a69f7651ff3f46cf__20220519_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-620d39a2', 'hca_prod_0d4aaaac02c344c48ae04465f97f83ed__20221101_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-70e4eabb', 'hca_prod_0d4b87ea6e9e456982e41343e0e3259f__20220110_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-7888c2ce', 'hca_prod_102018327c7340339b653ef13d81656a__20220213_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-d023038c', 'hca_prod_135f7f5c4a854bcf9f7c4f035ff1e428__20220729_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-c5065d3a', 'hca_prod_1538d572bcb7426b8d2c84f3a7f87bb0__20220630_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-7767d47a', 'hca_prod_165dea71a95a44e188cdb2d9ad68bb1e__20220303_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-673f4c49', 'hca_prod_1688d7cc6f5c49efb353e308b61d4e4c__20230313_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-9abcd921', 'hca_prod_16cd67912adb4d0f82220184dada6456__20220519_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-9b0d8d86', 'hca_prod_16e9915978bc44aab47955a5e903bf50__20221101_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-f59c20aa', 'hca_prod_16ed4ad8731946b288596fe1c1d73a82__20220111_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-2b6bbe36', 'hca_prod_18d4aae283634e008eebb9e568402cf8__20220330_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-a85d395a', 'hca_prod_18e5843776b740218ede3f0b443fa915__20220519_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-7ec15bbc', 'hca_prod_1dddae6e375348afb20efa22abad125d__20220213_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-8e5453fd', 'hca_prod_1eb69a39b5b241ecafae5fe37f272756__20220213_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-e55df4ed', 'hca_prod_1ebe8c34454e4c28bd713a3e8b127be4__20221208_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-8e161efb', 'hca_prod_1fa8b11f56fa45a6a7776af70e17a6b3__20220928_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-76ddb6a2', 'hca_prod_2084526ba66f4c40bb896fd162f2eb38__20220111_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-857c8f17', 'hca_prod_2253ae594cc54bd2b44eecb6d3fd7646__20220519_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-8e9e865c', 'hca_prod_235092021e3c49598a459c5b642a1066__20230313_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-634a63c7', 'hca_prod_258c5e15d1254f2d8b4ce3122548ec9b__20221208_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-c0287284', 'hca_prod_2837165560ba449ea3035859b29ead65__20221101_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-6a89dcba', 'hca_prod_2a64db431b554639aabb8dba0145689d__20220111_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-c215e7c1', 'hca_prod_2ad191cdbd7a409b9bd1e72b5e4cce81__20220111_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-d4e011f2', 'hca_prod_2b38025da5ea4c0fb22e367824bcaf4c__20220111_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-529a323b', 'hca_prod_2b81ecc46ee0438f8c5bc10b2464069e__20221101_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-deaef690', 'hca_prod_2eb4f5f842a54368aa2d337bacb96197__20220606_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-49314a87', 'hca_prod_2f67614380c24bc6b7b42613fe0fadf0__20220111_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-573f4ced', 'hca_prod_2fe3c60bac1a4c619b59f6556c0fce63__20220606_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-d8951cb5', 'hca_prod_3089d311f9ed44ddbb10397059bad4dc__20220111_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-b1d73b5d', 'hca_prod_34c9a62ca6104e31b3438fb7be676f8c__20221101_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-ede741aa', 'hca_prod_34ec845bcd7a4c4399e4d2932d5d85bb__20220928_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-d4b1487a', 'hca_prod_379ed69ebe0548bcaf5ea7fc589709bf__20220111_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-06d8d7f5', 'hca_prod_38e44dd0c3df418e9256d0824748901f__20220112_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-497bd309', 'hca_prod_3c9d586ebd264b4686903faaa18ccf38__20220729_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-cf03259b', 'hca_prod_3cdaf942f8ad42e8a77b4efedb9ea7b6__20220303_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-b2aefc7f', 'hca_prod_3cfcdff5dee14a7ba591c09c6e850b11__20220112_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-cbd0d764', 'hca_prod_3e92c74d256c40cd927316f155da8342__20220729_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-5934cc66', 'hca_prod_403c3e7668144a2da5805dd5de38c7ff__20220113_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-8a262357', 'hca_prod_414accedeba0440fb721befbc5642bef__20220113_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-84e2745e', 'hca_prod_425c2759db664c93a358a562c069b1f1__20220519_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-0baf4047', 'hca_prod_45c2c853d06f4879957ef1366fb5d423__20220303_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-ff143887', 'hca_prod_48b198ef3d594e57900fdf54c2435669__20221208_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-7bf7a66d', 'hca_prod_4a95101c9ffc4f30a809f04518a23803__20220113_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-641f98b4', 'hca_prod_4bec484dca7a47b48d488830e06ad6db__20220113_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-61ddb23f', 'hca_prod_4c73d1e4bad24a22a0ba55abbdbdcc3d__20220906_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-616afd75', 'hca_prod_4d9d56e4610d4748b57df8315e3f53a3__20220729_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-fc5c976e', 'hca_prod_4ef86852aca04a9185229968e0e54dbe__20230313_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-10eb0685', 'hca_prod_4f17edf6e9f042afa54af02fdca76ade__20220606_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-7964fe37', 'hca_prod_50151324f3ed435898afec352a940a61__20220113_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-82d6ee00', 'hca_prod_504e0cee168840fab936361c4a831f87__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-5fefa259', 'hca_prod_5116c0818be749c58ce073b887328aa9__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-8bb6f657', 'hca_prod_54aaa409dc2848c5be26d368b4a5d5c6__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-f194b1da', 'hca_prod_575c0ad9c78e469b9fdf9a68dd881137__20220928_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-102399d2', 'hca_prod_58028aa80ed249cab60f15e2ed5989d5__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-cbb1feac', 'hca_prod_591af954cdcd483996d3a0d1b1e885ac__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-2917ceb6', 'hca_prod_5b3285614a9740acb7ad6a90fc59d374__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-46332822', 'hca_prod_5b910a437fb54ea7b9d643dbd1bf2776__20220729_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-04558655', 'hca_prod_5bb1f67e2ff04848bbcf17d133f0fd2d__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-6a083ad7', 'hca_prod_602628d7c03848a8aa97ffbb2cb44c9d__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-41cca7ce', 'hca_prod_60ea42e1af4942f58164d641fdb696bc__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-96f28c63', 'hca_prod_615158205bb845d08d12f0850222ecf0__20221007_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-4f72c082', 'hca_prod_65d7a1684d624bc083244e742aa62de6__20220330_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-6e86e38d', 'hca_prod_6621c827b57a4268bc80df4049140193__20220330_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-1a46eca7', 'hca_prod_6663070ffd8b41a9a4792d1e07afa201__20220519_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-d9380f70', 'hca_prod_66d7d92ad6c5492c815bf81c7c93c984__20220906_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-9586eb0b', 'hca_prod_68df3629d2d24eedb0aba10e0f019b88__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-31de31e5', 'hca_prod_6c040a938cf84fd598de2297eb07e9f6__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-fe215496', 'hca_prod_6e522b939b704f0c9990b9cff721251b__20230313_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-bf5922e5', 'hca_prod_6f03e4ad93054bfaa5b6929ffb1d94bd__20230313_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-259b6fce', 'hca_prod_6f89a7f38d4a4344aa4feccfe7e91076__20220213_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-e3ce8d32', 'hca_prod_71eb5f6dcee04297b503b1125909b8c7__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-63751bc2', 'hca_prod_73769e0a5fcd41f4908341ae08bfa4c1__20220330_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-a7e215db', 'hca_prod_74e2ef9d7c9f418cb2817fb38f3b1571__20220906_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-0059cce8', 'hca_prod_769a08d1b8a44f1e95f76071a9827555__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-e62a1f78', 'hca_prod_78b2406dbff246fc8b6120690e602227__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-aad6282b', 'hca_prod_79351583b21244bab473731bdcddb407__20221208_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-6ffd3fa5', 'hca_prod_79b13a2a9ca142a497bd70208a11bea6__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-12a154cb', 'hca_prod_7ac8822c4ef04194adf074290611b1c6__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-ff62a88c', 'hca_prod_7b393e4d65bc4c03b402aae769299329__20220519_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-d4e5780e', 'hca_prod_7b947aa243a74082afff222a3e3a4635__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-664a24cb', 'hca_prod_7c75f07c608d4c4aa1b7b13d11c0ad31__20220117_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-9caf17c8', 'hca_prod_8559a8ed5d8c4fb6bde8ab639cebf03c__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-3e85541c', 'hca_prod_8999b4566fa6438bab17b62b1d8ec0c3__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-a4f6e9ac', 'hca_prod_8b9cb6ae6a434e47b9fb3df7aeec941f__20220906_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-d8991cff', 'hca_prod_8d566d35d8d34975a351be5e25e9b2ea__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-65954398', 'hca_prod_8f630e0f6bf94a04975402533152a954__20220729_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-3539412c', 'hca_prod_91af6e2f65f244ec98e0ba4e98db22c8__20220303_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-64e86c6c', 'hca_prod_923d323172954184b3f6c3082766a8c7__20220906_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-bd3072b5', 'hca_prod_955dfc2ca8c64d04aa4d907610545d11__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-1fc717b1', 'hca_prod_957261f72bd64358a6ed24ee080d5cfc__20220330_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-b7b5c053', 'hca_prod_990d251f6dab4a98a2b66cfe7e4708b9__20221101_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-2736a43e', 'hca_prod_9ac53858606a4b89af49804ccedaa660__20220906_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-9f5be9ac', 'hca_prod_9b876d3107394e969846f76e6a427279__20220906_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-34fd3cd0', 'hca_prod_9e3370a0144a49a99e926f6a9290125a__20221101_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-78a0de43', 'hca_prod_a1312f9a01ef40a789bf9091ca76a03a__20220729_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-be58fa69', 'hca_prod_a39728aa70a04201b0a281b7badf3e71__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-11157f54', 'hca_prod_a60803bbf7db45cfb52995436152a801__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-278d9dd8', 'hca_prod_a62dae2ecd694d5cb5f84f7e8abdbafa__20220606_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-7a531766', 'hca_prod_a7c66eb14a4e4f6c9e30ad2a485f8301__20220906_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-39471648', 'hca_prod_a80a63f2e223489081b0415855b89abc__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-1553d994', 'hca_prod_a9301bebe9fa42feb75c84e8a460c733__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-d962a1b4', 'hca_prod_a991ef154d4a4b80a93ec538b4b54127__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-60416b5f', 'hca_prod_a9f5323ace71471c9caf04cc118fd1d7__20220606_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-f2f57a7c', 'hca_prod_ac289b77fb124a6bad43c0721c698e70__20220906_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-bbe8303d', 'hca_prod_ad04c8e79b7d4cceb8e901e31da10b94__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-d67d5486', 'hca_prod_ae62bb3155ca4127b0fbb1771a604645__20230313_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-2a19065b', 'hca_prod_aefb919243fc46d7a4c129597f7ef61b__20220330_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-ed809ac3', 'hca_prod_b7259878436c4274bfffca76f4cb7892__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-4c6b830c', 'hca_prod_b733dc1b1d5545e380367eab0821742c__20220519_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-2285af8d', 'hca_prod_b9484e4edc404e389b854cecf5b8c068__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-ffeff93b', 'hca_prod_bd40033154b94fccbff66bb8b079ee1f__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-208fbdb6', 'hca_prod_bd7104c9a950490e94727d41c6b11c62__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-3daf9fbc', 'hca_prod_be010abcfb684581b61f7dd7c3d7b044__20230314_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-03ab12e5', 'hca_prod_c05184453b3b49c6b8fcc41daa4eacba__20220213_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-e798f7a3', 'hca_prod_c0d82ef215044ef09e5ed8a13e45fdec__20220928_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-3be98d08', 'hca_prod_c16a754f5da346ed8c1e6426af2ef625__20220519_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-097f9535', 'hca_prod_c1a9a93dd9de4e659619a9cec1052eaa__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-c674ec37', 'hca_prod_c211fd49d9804ba18c6ac24254a3cb52__20220303_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-7978f7d7', 'hca_prod_c31fa434c9ed4263a9b6d9ffb9d44005__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-06d2f7a5', 'hca_prod_c4e1136978d44d29ba8eb67907c4c65c__20220630_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-cba6aa82', 'hca_prod_c5ca43aa3b2b42168eb3f57adcbc99a1__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-e7beed27', 'hca_prod_c6a50b2a3dfd4ca89b483e682f568a25__20220303_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-4c1109c3', 'hca_prod_c715cd2fdc7c44a69cd5b6a6d9f075ae__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-b9ed6937', 'hca_prod_c7c54245548b4d4fb15e0d7e238ae6c8__20220330_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-8b443018', 'hca_prod_c8e6c5d9fcde4845beadff96999e3051__20221101_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-cf75399d', 'hca_prod_cd61771b661a4e19b2696e5d95350de6__20220213_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-6e91dfce', 'hca_prod_cdc2d2706c99414288839bd95c041d05__20221208_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-ebcfd951', 'hca_prod_ce7b12ba664f4f798fc73de6b1892183__20220119_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-2bf47b4d', 'hca_prod_d138a1147df54f7d9ff1f79dfd2d428f__20220606_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-fdb31a8e', 'hca_prod_d6225aee8f0e4b20a20c682509a9ea14__20220213_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-9f65205b', 'hca_prod_d71c76d336704774a9cf034249d37c60__20220213_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-94746fdf', 'hca_prod_d7845650f6b14b1cb2fec0795416ba7b__20220119_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-29fbe41f', 'hca_prod_d8ae869c39c24cddb3fc2d0d8f60e7b8__20230313_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-225f8649', 'hca_prod_da9d6f243bdf4eaa9e3ff47ce2a65b36__20220729_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-fc1febb7', 'hca_prod_daa371e81ec343ef924f896d901eab6f__20220519_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-6d15b6d0', 'hca_prod_daf9d9827ce643f6ab51272577290606__20220119_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-cf5ce794', 'hca_prod_dbcd4b1d31bd4eb594e150e8706fa192__20220119_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-a44da434', 'hca_prod_dc1a41f69e0942a6959e3be23db6da56__20220119_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-c3eee222', 'hca_prod_dcbb50d19acf4f709fdab1f63a948c49__20221101_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-5ba86b6e', 'hca_prod_dd7ada843f144765b7ce9b64642bb3dc__20220212_dcp2_20230314_dcp25', pop), # noqa E501
    mksrc('bigquery', 'datarepo-a4d35f23', 'hca_prod_dd7f24360c564709bd17e526bba4cc15__20220119_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-7882d448', 'hca_prod_e0c74c7a20a445059cf138dcdd23011b__20220119_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-cf662b44', 'hca_prod_e255b1c611434fa683a8528f15b41038__20220330_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-fc908765', 'hca_prod_e4b18cd28f15490db9f1d118aa067dc3__20221101_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-410d6eb1', 'hca_prod_e57dc176ab98446b90c289e0842152fd__20220119_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-d0d87d10', 'hca_prod_e6773550c1a6494986431a3154cf2670__20221208_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-4a7bcd86', 'hca_prod_e8808cc84ca0409680f2bba73600cba6__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-a8d7a228', 'hca_prod_e956e66aac8e483a963a0f92c7e5abfb__20230313_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-03f5da17', 'hca_prod_e993adcdd4ba4f889a05d1c05bdf0c45__20220606_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-aa5721d7', 'hca_prod_e9f36305d85744a393f0df4e6007dc97__20220519_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-1c07a6e4', 'hca_prod_ea9eec5a4fc24c5894d02fcb598732bc__20221208_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-f01e417c', 'hca_prod_ec6476ee294941f3947b8eef41d6d3ac__20220729_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-b1b215b9', 'hca_prod_ede2e0b46652464fabbc0b2d964a25a0__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-7bf9ffaf', 'hca_prod_ee166275f63a486481554df86c9de679__20230313_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-d9d4043e', 'hca_prod_ef1d9888fa8647a4bb720ab0f20f7004__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-4cca88b5', 'hca_prod_ef1e3497515e4bbe8d4c10161854b699__20220118_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-d7d0cebe', 'hca_prod_f29b124a85974862ae98ff3a0fd9033e__20220303_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-7949d5c1', 'hca_prod_f2fe82f044544d84b416a885f3121e59__20220119_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-10db03cf', 'hca_prod_f4d011ced1f548a4ab61ae14176e3a6e__20220519_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-2df3359d', 'hca_prod_fa3f460f4fb94cedb5488ba6a8ecae3f__20220330_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-7da03753', 'hca_prod_fc381e70df1b407d813152ab523270bd__20221208_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-c4cba2d7', 'hca_prod_fcaa53cdba574bfeaf9ceaa958f95c1a__20220906_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-fa96c6bd', 'hca_prod_fccd3f50cde247bf8972a293b5928aea__20220606_dcp2_20230314_dcp25'),
    mksrc('bigquery', 'datarepo-289f5713', 'hca_prod_fde199d2a8414ed1aa65b9e0af8969b1__20220330_dcp2_20230314_dcp25'),
    # @formatter:on
]))

dcp26_sources = mkdict(dcp25_sources, 334, mkdelta([
    # @formatter:off
    mksrc('bigquery', 'datarepo-37928b8f', 'hca_prod_2d4d89f2ebeb467cae60a3efc5e8d4ba__20230206_dcp2_20230331_dcp26'),
    mksrc('bigquery', 'datarepo-96f28c63', 'hca_prod_615158205bb845d08d12f0850222ecf0__20221007_dcp2_20230314_dcp25', pop), # noqa E501
    mksrc('bigquery', 'datarepo-0d0c66d4', 'hca_prod_c281ab637b7d4bdfb7619b1baaa18f82__20230331_dcp2_20230331_dcp26'),
    mksrc('bigquery', 'datarepo-b3b1e92f', 'hca_prod_c5b475f276b34a8e8465f3b69828fec3__20230331_dcp2_20230331_dcp26'),
    mksrc('bigquery', 'datarepo-baa51c1d', 'hca_prod_cd9d6360ce38432197dff13c79e3cb84__20230206_dcp2_20230331_dcp26'),
    mksrc('bigquery', 'datarepo-d0d87d10', 'hca_prod_e6773550c1a6494986431a3154cf2670__20221208_dcp2_20230314_dcp25', pop), # noqa E501
    mksrc('bigquery', 'datarepo-636717a5', 'hca_prod_f3825dfe990a431fb9719c26d39840db__20230331_dcp2_20230331_dcp26'),
    # @formatter:on
]))

dcp27_sources = mkdict(dcp26_sources, 350, mkdelta([
    mksrc('bigquery', 'datarepo-75b50ae7', 'hca_prod_04e4292cf62f4098ae9bfd69ae002a90__20230427_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-d2f36342', 'hca_prod_0751843070314bdfa3ce1bf0917a1923__20221208_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-26882f0f', 'hca_prod_07d5987e7f9e4f34b0fba185a35504f5__20230427_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-c89e2c59', 'hca_prod_1fac187b1c3f41c4b6b66a9a8c0489d1__20230427_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-87be288c', 'hca_prod_30dc396411354b56b393ce2dcbc6e379__20230427_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-c4141b50', 'hca_prod_3ce9ae94c469419a96375d138a4e642f__20230201_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-4064f470', 'hca_prod_40272c3b46974bd4ba3f82fa96b9bf71__20220303_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-94eb2b77', 'hca_prod_4627f43ea43f44dd8c4b7efddb3f296d__20230501_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-e3915cae', 'hca_prod_4f4f0193ede84a828cb07a0a22f06e63__20230427_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-a498b8c7', 'hca_prod_50154d1e230844bf960810c7afaa560b__20230427_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-f800a6a6', 'hca_prod_566d00b0e1f84b929cbd57de9fad0050__20230427_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-bf159952', 'hca_prod_5f44a860d96e4a99b67e24e1b8ccfd26__20230427_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-319b1c54', 'hca_prod_6e1771950ac0468b99a287de96dc9db4__20230503_dcp2_20230503_dcp27'),
    mksrc('bigquery', 'datarepo-7eec55b7', 'hca_prod_77c13c40a5984036807fbe09209ec2dd__20230427_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-daba93c4', 'hca_prod_7c5990297a3c4b5c8e79e72c9a9a65fe__20230427_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-2431c03c', 'hca_prod_aa55000c016848d890262d3a76ec8af3__20230427_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-e2b50763', 'hca_prod_c844538b88544a95bd01aacbaf86d97f__20230427_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-37a0e19e', 'hca_prod_dc0b65b0771346f0a3390b03ea786046__20230427_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-cc43a54a', 'hca_prod_e090445c69714212bc5fae4ec3914102__20230427_dcp2_20230501_dcp27'),
    mksrc('bigquery', 'datarepo-054ea5fa', 'hca_prod_f6133d2a9f3d4ef99c19c23d6c7e6cc0__20220119_dcp2_20230501_dcp27'),
]))

dcp28_sources = mkdict(dcp27_sources, 364, mkdelta([
    mksrc('bigquery', 'datarepo-60acbcdf', 'hca_prod_111d272bc25a49ac9b25e062b70d66e0__20230530_dcp2_20230530_dcp28'),
    mksrc('bigquery', 'datarepo-7ef68d1b', 'hca_prod_272b760266cd4b02a86b2b7c9c51a9ea__20230526_dcp2_20230530_dcp28'),
    mksrc('bigquery', 'datarepo-db922a93', 'hca_prod_29b5416534ee4da5b257b4c1f7343656__20230530_dcp2_20230530_dcp28'),
    mksrc('bigquery', 'datarepo-6985b629', 'hca_prod_57a2c2deb0d4465abe53a41e59e75fab__20230526_dcp2_20230530_dcp28'),
    mksrc('bigquery', 'datarepo-131b24da', 'hca_prod_77423e580fbb495a9ec2bd9a8010f21d__20230526_dcp2_20230530_dcp28'),
    mksrc('bigquery', 'datarepo-6a3a84b5', 'hca_prod_842605c7375a47c59e2ca71c2c00fcad__20220117_dcp2_20230530_dcp28'),
    mksrc('bigquery', 'datarepo-e3af5c43', 'hca_prod_8b954fb2bccb44c584e39f91e9189c40__20230526_dcp2_20230530_dcp28'),
    mksrc('bigquery', 'datarepo-bb339f80', 'hca_prod_92afaa56d501481ea027dddd72212ba8__20230526_dcp2_20230530_dcp28'),
    mksrc('bigquery', 'datarepo-f4518d09', 'hca_prod_9746f4e0d3b2454389b310288162851b__20230526_dcp2_20230530_dcp28'),
    mksrc('bigquery', 'datarepo-0ea3a03c', 'hca_prod_a4f154f85cc940b5b8d7af90afce8a8f__20230526_dcp2_20230530_dcp28'),
    mksrc('bigquery', 'datarepo-a5f69eaf', 'hca_prod_ae62bb3155ca4127b0fbb1771a604645__20230313_dcp2_20230530_dcp28'),
    mksrc('bigquery', 'datarepo-72f08c60', 'hca_prod_c3354786c17c4e53b4d7c7afbed5b208__20230526_dcp2_20230530_dcp28'),
    mksrc('bigquery', 'datarepo-d2e866b8', 'hca_prod_cae461deecbd482fa5d411d607fc12ba__20230526_dcp2_20230530_dcp28'),
    mksrc('bigquery', 'datarepo-30f2f8b0', 'hca_prod_e6773550c1a6494986431a3154cf2670__20221208_dcp2_20230530_dcp28'),
    mksrc('bigquery', 'datarepo-c3fad823', 'hca_prod_e925633fabd9486a81c61a6a66891d23__20230526_dcp2_20230530_dcp28'),
    mksrc('bigquery', 'datarepo-9176f1e6', 'hca_prod_fae72d894ac44aab9b93574775e168d4__20230530_dcp2_20230530_dcp28')
]))

dcp29_sources = mkdict(dcp28_sources, 386, mkdelta([
    mksrc('bigquery', 'datarepo-a066b1d5', 'hca_prod_01aacb6840764fd99eb9aba0f48c1b5a__20230616_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-36015bab', 'hca_prod_0d737cce1c1c493a8e2eb00143bccc12__20230616_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-34ba456a', 'hca_prod_0efecd202b524e4f96c59b4b94158713__20230614_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-7ab77437', 'hca_prod_16e9915978bc44aab47955a5e903bf50__20221101_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-cac974a7', 'hca_prod_1c4cbdd433e34dedab435958de817123__20230614_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-e13ec32c', 'hca_prod_2973a42cf81048129a235bbc9644588d__20230614_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-780b7376', 'hca_prod_2caedc30c8164b99a237b9f3b458c8e5__20230614_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-27e5c5cf', 'hca_prod_2d559a6e7cd9432f9f6e0e4df03b0888__20230614_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-340d69da', 'hca_prod_3d49e5e5976f44cbb6b9079016c31c56__20230614_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-6c9af5df', 'hca_prod_457d0bfe79e443f1be5d83bf080d809e__20230616_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-8d3a1856', 'hca_prod_5a54c6170eed486e8c1a8a8041fc1729__20230616_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-3b876136', 'hca_prod_5f607e50ba224598b1e9f3d9d7a35dcc__20230201_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-4c3d24fa', 'hca_prod_615158205bb845d08d12f0850222ecf0__20221007_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-2eac2e30', 'hca_prod_65cbfea55c544255a1d014549a86a5c1__20230616_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-2987dba5', 'hca_prod_73011a86475548ac9f70a28903b4ad77__20230616_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-a0198d42', 'hca_prod_92892ab213344b1c976114f5a73548ea__20230616_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-3525d30e', 'hca_prod_9c20a245f2c043ae82c92232ec6b594f__20220212_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-a03549fc', 'hca_prod_9f17ed7d93254723a120b00e48db20c0__20230614_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-76e3e0fa', 'hca_prod_b208466a6fb043858cfb8e03ff6b939e__20230616_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-32c91a7f', 'hca_prod_b91c623b19454727b1670a93027b0d3f__20230616_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-41efd06c', 'hca_prod_bc5512cc95444aa48b758af445ee2257__20230614_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-3ed34ae5', 'hca_prod_cea413af79b34f118b48383fe9a65fbe__20230614_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-4d68a94d', 'hca_prod_da74b50760ee4dd1bd02807bb051a337__20230614_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-72424b3d', 'hca_prod_e5fe827437694d7daa356d33c226ab43__20230616_dcp2_20230616_dcp29'),
    mksrc('bigquery', 'datarepo-0e1a9ef4', 'hca_prod_f0f89c1474604bab9d4222228a91f185__20220119_dcp2_20230616_dcp29'),
]))

dcp30_sources = mkdict(dcp29_sources, 391, mkdelta([
    mksrc('bigquery', 'datarepo-664081d7', 'hca_prod_07073c1280064710a00b23abdb814904__20220107_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-b58dd635', 'hca_prod_1c6a960d52ac44eab728a59c7ab9dc8e__20220110_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-db2290d3', 'hca_prod_1cd1f41ff81a486ba05b66ec60f81dcf__20220107_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-bb040c00', 'hca_prod_1eba4d0b2d154ba7bb3cd4654dd94519__20230815_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-3c37eadf', 'hca_prod_23587fb31a4a4f58ad74cc9a4cb4c254__20220111_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-f259cc76', 'hca_prod_279f176633194e3c9f996fb59ba9b3e5__20230815_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-b5f40aa1', 'hca_prod_31887183a72c43089eacc6140313f39c__20220111_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-f1f04670', 'hca_prod_41fb1734a121461695c73b732c9433c7__20220113_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-1c267fa5', 'hca_prod_4a95101c9ffc4f30a809f04518a23803__20220113_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-9ec63340', 'hca_prod_51f02950ee254f4b8d0759aa99bb3498__20220117_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-1188524b', 'hca_prod_520afa10f9d24e93ab7a26c4c863ce18__20220117_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-c35a61eb', 'hca_prod_559bb888782941f2ace52c05c7eb81e9__20220117_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-1840929b', 'hca_prod_7027adc6c9c946f384ee9badc3a4f53b__20220117_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-019a64bb', 'hca_prod_739ef78aba5d4487a0139982db66d222__20230815_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-e0eccf2a', 'hca_prod_74493e9844fc48b0a58fcc7e77268b59__20220117_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-ccee34ca', 'hca_prod_783c9952a4ae4106a6ce56f20ce27f88__20220117_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-5dd80f6c', 'hca_prod_8f1f653d3ea14d8eb4a7b97dc852c2b1__20230815_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-6f3c6cc2', 'hca_prod_92afaa56d501481ea027dddd72212ba8__20230526_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-41c736b4', 'hca_prod_996120f9e84f409fa01e732ab58ca8b9__20220118_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-bdbe74eb', 'hca_prod_b208466a6fb043858cfb8e03ff6b939e__20230616_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-20f3401b', 'hca_prod_b4a7d12f6c2f40a39e359756997857e3__20220118_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-47aed999', 'hca_prod_c893cb575c9f4f26931221b85be84313__20220118_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-e7b395be', 'hca_prod_ccef38d7aa9240109621c4c7b1182647__20220118_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-b11d40e9', 'hca_prod_d3a4ceac4d66498497042570c0647a56__20220119_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-c47b01c5', 'hca_prod_d8ae869c39c24cddb3fc2d0d8f60e7b8__20230313_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-a2b3ca2a', 'hca_prod_efea6426510a4b609a19277e52bfa815__20220118_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-6f4f0e4f', 'hca_prod_f7b464770f2a4bffa9b7719e000499a3__20230815_dcp2_20230815_dcp30'),
    mksrc('bigquery', 'datarepo-80208d02', 'hca_prod_f86f1ab41fbb4510ae353ffd752d4dfc__20220119_dcp2_20230815_dcp30'),
]))

dcp31_sources = mkdict(dcp30_sources, 399, mkdelta([
    mksrc('bigquery', 'datarepo-36295e0b', 'hca_prod_0911cc0406d64ffc8318b90b0039e8ad__20230905_dcp2_20230905_dcp31'),
    mksrc('bigquery', 'datarepo-83dafa1a', 'hca_prod_279f176633194e3c9f996fb59ba9b3e5__20230815_dcp2_20230905_dcp31'),
    mksrc('bigquery', 'datarepo-789ec382', 'hca_prod_326b36bd0975475f983b56ddb8f73a4d__20230905_dcp2_20230905_dcp31'),
    mksrc('bigquery', 'datarepo-b68fee1b', 'hca_prod_3e92c74d256c40cd927316f155da8342__20220729_dcp2_20230905_dcp31'),
    mksrc('bigquery', 'datarepo-d8916247', 'hca_prod_453d7ee2319f496c986299d397870b63__20230905_dcp2_20230906_dcp31'),
    mksrc('bigquery', 'datarepo-7ad0a304', 'hca_prod_4ef86852aca04a9185229968e0e54dbe__20230313_dcp2_20230905_dcp31'),
    mksrc('bigquery', 'datarepo-18691416', 'hca_prod_51f02950ee254f4b8d0759aa99bb3498__20220117_dcp2_20230905_dcp31'),
    mksrc('bigquery', 'datarepo-d6c0be70', 'hca_prod_577c946d6de54b55a854cd3fde40bff2__20220117_dcp2_20230905_dcp31'),
    mksrc('bigquery', 'datarepo-3352a319', 'hca_prod_6936da41369246bbbca1cd0f507991e9__20230905_dcp2_20230905_dcp31'),
    mksrc('bigquery', 'datarepo-ff7365d6', 'hca_prod_739ef78aba5d4487a0139982db66d222__20230815_dcp2_20230905_dcp31'),
    mksrc('bigquery', 'datarepo-517878d7', 'hca_prod_7dcffc327c8243969a4f88b5579bfe8a__20230905_dcp2_20230905_dcp31'),
    mksrc('bigquery', 'datarepo-088b9165', 'hca_prod_7f9766ffbb124279b34078d140bdd7ba__20230905_dcp2_20230905_dcp31'),
    mksrc('bigquery', 'datarepo-4450b12b', 'hca_prod_92892ab213344b1c976114f5a73548ea__20230616_dcp2_20230905_dcp31'),
    mksrc('bigquery', 'datarepo-f305a966', 'hca_prod_9746f4e0d3b2454389b310288162851b__20230526_dcp2_20230905_dcp31'),
    mksrc('bigquery', 'datarepo-6a51c34a', 'hca_prod_e374c1cf73fd4a7a866979dc41714984__20230905_dcp2_20230905_dcp31'),
    mksrc('bigquery', 'datarepo-2abbf49d', 'hca_prod_e456c042f6b64ceca3381a8ef80bd779__20230905_dcp2_20230905_dcp31'),
]))

dcp32_sources = mkdict(dcp31_sources, 405, mkdelta([
    # @formatter:off
    mksrc('bigquery', 'datarepo-6885133e', 'hca_prod_0792db3480474e62802c9177c9cd8e28__20220107_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-03b554f8', 'hca_prod_0911cc0406d64ffc8318b90b0039e8ad__20230905_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-a5249352', 'hca_prod_279f176633194e3c9f996fb59ba9b3e5__20230815_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-e676a270', 'hca_prod_2a72a4e566b2405abb7c1e463e8febb0__20220111_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-720eb4d9', 'hca_prod_31887183a72c43089eacc6140313f39c__20220111_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-8c4f04c0', 'hca_prod_326b36bd0975475f983b56ddb8f73a4d__20230905_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-9f296da3', 'hca_prod_376a7f55b8764f609cf3ed7bc83d5415__20220111_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-223a067e', 'hca_prod_3e92c74d256c40cd927316f155da8342__20220729_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-f053f0b1', 'hca_prod_421bc6cdbbb44398ac60a32ea94f02ae__20230929_dcp2_20231003_dcp32'),
    mksrc('bigquery', 'datarepo-7221e50b', 'hca_prod_453d7ee2319f496c986299d397870b63__20230905_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-262093aa', 'hca_prod_48f60534ba4e45bcaa5b6d3a6c45962e__20230929_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-129b6bcc', 'hca_prod_4a95101c9ffc4f30a809f04518a23803__20220113_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-85e28021', 'hca_prod_4ef86852aca04a9185229968e0e54dbe__20230313_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-ad300086', 'hca_prod_50154d1e230844bf960810c7afaa560b__20230427_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-6d4f90e5', 'hca_prod_51f02950ee254f4b8d0759aa99bb3498__20220117_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-d13e36e7', 'hca_prod_53c53cd481274e12bc7f8fe1610a715c__20220117_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-0287a0ba', 'hca_prod_577c946d6de54b55a854cd3fde40bff2__20220117_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-4d2eebce', 'hca_prod_5a54c6170eed486e8c1a8a8041fc1729__20230616_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-2917ceb6', 'hca_prod_5b3285614a9740acb7ad6a90fc59d374__20220117_dcp2_20230314_dcp25', pop),  # noqa E501
    mksrc('bigquery', 'datarepo-069ac8d2', 'hca_prod_67a3de0945b949c3a068ff4665daa50e__20220117_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-7be1db07', 'hca_prod_6936da41369246bbbca1cd0f507991e9__20230905_dcp2_20231003_dcp32'),
    mksrc('bigquery', 'datarepo-b59acd40', 'hca_prod_72ff481856924bbc8886e47763531023__20230929_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-a2f56dc5', 'hca_prod_739ef78aba5d4487a0139982db66d222__20230815_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-114ecc76', 'hca_prod_74e2ef9d7c9f418cb2817fb38f3b1571__20220906_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-87452786', 'hca_prod_7dcffc327c8243969a4f88b5579bfe8a__20230905_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-c844e919', 'hca_prod_bfaedc29fe844e72a46175dc8aabbd1b__20230929_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-46a39a89', 'hca_prod_c412be53cf9547c7980cc0a0caa2d3a0__20230929_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-89205d73', 'hca_prod_cfece4d2f18d44ada46a42bbcb5cb3b7__20230929_dcp2_20231002_dcp32'),
    mksrc('bigquery', 'datarepo-48284a59', 'hca_prod_dcc28fb37bab48cebc4b684c00e133ce__20230905_dcp2_20231002_dcp32'),
    # @formatter:on
]))

dcp33_sources = mkdict(dcp32_sources, 412, mkdelta([
    mksrc('bigquery', 'datarepo-fe1f8660', 'hca_prod_0d737cce1c1c493a8e2eb00143bccc12__20230616_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-46bbfa8a', 'hca_prod_16e9915978bc44aab47955a5e903bf50__20221101_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-d5d4057a', 'hca_prod_1c5eaabf075b4b7aa9e607792c2034b3__20231101_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-b64e953d', 'hca_prod_1ffa222328a64133a5a4badd00faf4bc__20231101_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-ca76b840', 'hca_prod_21ea8ddb525f4f1fa82031f0360399a2__20220111_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-a0ffb40f', 'hca_prod_2af52a1365cb4973b51339be38f2df3f__20220111_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-526f3da5', 'hca_prod_3d49e5e5976f44cbb6b9079016c31c56__20230614_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-766c30b1', 'hca_prod_4ef86852aca04a9185229968e0e54dbe__20230313_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-08e00b61', 'hca_prod_5bd01deb01ee46118efdcf0ec5f56ac4__20231101_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-70ee98ab', 'hca_prod_645b20c95ed0450086b57aef770d010a__20230929_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-2e06a188', 'hca_prod_73011a86475548ac9f70a28903b4ad77__20230616_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-76a818d4', 'hca_prod_7f9766ffbb124279b34078d140bdd7ba__20230905_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-2bb1dd84', 'hca_prod_849ed38c591743c4a8f90782241cf10c__20231101_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-11a44864', 'hca_prod_8a666b76daaf4b1f9414e4807a1d1e8b__20220630_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-4bc03f16', 'hca_prod_91674dcf864140e6978dc1706feffba8__20231101_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-0b11f34c', 'hca_prod_94023a08611d4f22a8c990956e091b2e__20220118_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-bc7bde81', 'hca_prod_95f07e6e6a734e1ba880c83996b3aa5c__20220118_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-69bbc337', 'hca_prod_9c20a245f2c043ae82c92232ec6b594f__20220212_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-55e4f61e', 'hca_prod_9f17ed7d93254723a120b00e48db20c0__20230614_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-7f36ef82', 'hca_prod_cc35f94ee93b4dbda08c702978d9046f__20231101_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-3ad8245f', 'hca_prod_da74b50760ee4dd1bd02807bb051a337__20230614_dcp2_20231102_dcp33'),
    mksrc('bigquery', 'datarepo-5f867d37', 'hca_prod_dbd836cfbfc241f0983441cc6c0b235a__20220212_dcp2_20231102_dcp33')
]))

dcp34_sources = mkdict(dcp33_sources, 427, mkdelta([
    mksrc('bigquery', 'datarepo-a2f2ced7', 'hca_prod_08fb10df32e5456c9882e33fcd49077a__20231212_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-cd738b8d', 'hca_prod_10a845f7036146fa92a32a36483136b1__20231212_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-6fec2801', 'hca_prod_1538d572bcb7426b8d2c84f3a7f87bb0__20220630_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-05ece841', 'hca_prod_1dd552a5eb4f4b9280887224bcbd0629__20231212_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-1d8f9fa4', 'hca_prod_2184e63d82d84ab2839ee93f8395f568__20231212_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-64f7ba3d', 'hca_prod_222a92d5277b489caad8a680d1fd2b12__20231212_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-5f2ddddc', 'hca_prod_272b760266cd4b02a86b2b7c9c51a9ea__20230526_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-3dbccc52', 'hca_prod_2f67614380c24bc6b7b42613fe0fadf0__20220111_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-078dbc55', 'hca_prod_34c9a62ca6104e31b3438fb7be676f8c__20221101_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-a0115d6e', 'hca_prod_3cfcdff5dee14a7ba591c09c6e850b11__20220112_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-66dbe882', 'hca_prod_415eb773cadb43d1ab897d160d5cfc7d__20231212_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-37f63790', 'hca_prod_58028aa80ed249cab60f15e2ed5989d5__20220117_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-9f64fc88', 'hca_prod_581de139461f4875b40856453a9082c7__20231212_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-78a292c4', 'hca_prod_5b910a437fb54ea7b9d643dbd1bf2776__20220729_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-84d96baf', 'hca_prod_65cbfea55c544255a1d014549a86a5c1__20230616_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-96cc1349', 'hca_prod_6735ff731a04422eb500730202e46f8a__20231212_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-e8198e31', 'hca_prod_6874b7eb344547ec877375141430e169__20231213_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-43f096b1', 'hca_prod_77dedd59137648879bcadc42b56d5b7a__20230201_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-594e57c3', 'hca_prod_78b2406dbff246fc8b6120690e602227__20220117_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-13c1e76b', 'hca_prod_8185730f411340d39cc3929271784c2b__20220117_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-10cff382', 'hca_prod_849ed38c591743c4a8f90782241cf10c__20231101_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-77cc3acc', 'hca_prod_8559a8ed5d8c4fb6bde8ab639cebf03c__20220118_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-af50b124', 'hca_prod_85c0d6faf1174d76b01a5d5e8f5f9188__20231212_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-40731b27', 'hca_prod_894ae6ac5b4841a8a72f315a9b60a62e__20231212_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-e7931a4c', 'hca_prod_925f9a4ccac0444aad2c612656ab3a85__20231212_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-96344c2b', 'hca_prod_957261f72bd64358a6ed24ee080d5cfc__20220330_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-7f53a7f2', 'hca_prod_abe1a013af7a45ed8c26f3793c24a1f4__20220118_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-1e2e46c4', 'hca_prod_bfaedc29fe844e72a46175dc8aabbd1b__20230929_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-2901e79b', 'hca_prod_c05184453b3b49c6b8fcc41daa4eacba__20220213_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-5b1e55df', 'hca_prod_c16a754f5da346ed8c1e6426af2ef625__20220519_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-94d17e05', 'hca_prod_c1a9a93dd9de4e659619a9cec1052eaa__20220118_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-d12842d9', 'hca_prod_c4077b3c5c984d26a614246d12c2e5d7__20220118_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-abaa9315', 'hca_prod_c844538b88544a95bd01aacbaf86d97f__20230427_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-02709814', 'hca_prod_cbd3d2769f244af98381b11f6cdbdc4b__20231212_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-7da203ad', 'hca_prod_cfece4d2f18d44ada46a42bbcb5cb3b7__20230929_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-45fc3b21', 'hca_prod_da77bd0643ae4012a774e4d62797df51__20231212_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-0bc51bfc', 'hca_prod_daf9d9827ce643f6ab51272577290606__20220119_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-8ecfd261', 'hca_prod_e49e556ada5a442ab45c8691b457623e__20231212_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-99284b34', 'hca_prod_e526d91dcf3a44cb80c5fd7676b55a1d__20220119_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-88582dc4', 'hca_prod_ede2e0b46652464fabbc0b2d964a25a0__20220118_dcp2_20231213_dcp34'),
    mksrc('bigquery', 'datarepo-145a904d', 'hca_prod_ef1e3497515e4bbe8d4c10161854b699__20220118_dcp2_20231213_dcp34')
]))

dcp35_sources = mkdict(dcp34_sources, 438, mkdelta([
    mksrc('bigquery', 'datarepo-3b981d26', 'hca_prod_17cf943be247454f908bda58665fcc56__20240201_dcp2_20240206_dcp35'),
    mksrc('bigquery', 'datarepo-2a225323', 'hca_prod_1dddae6e375348afb20efa22abad125d__20220213_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-a318416f', 'hca_prod_27e2e0ae59714927aac119e81804097b__20240201_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-3403e1a6', 'hca_prod_41fb1734a121461695c73b732c9433c7__20220113_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-f2e5bb83', 'hca_prod_4bec484dca7a47b48d488830e06ad6db__20220113_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-d9f05210', 'hca_prod_4f4f0193ede84a828cb07a0a22f06e63__20230427_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-ea22560e', 'hca_prod_6735ff731a04422eb500730202e46f8a__20231212_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-33c2177f', 'hca_prod_77780d5603c0481faade2038490cef9f__20220330_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-a8a3410a', 'hca_prod_7a8d45f1353b45088e8965a96785b167__20240201_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-d9783a5a', 'hca_prod_7bc1f14b5e644c7f86b023596b97e2aa__20240201_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-011b06f4', 'hca_prod_894ae6ac5b4841a8a72f315a9b60a62e__20231212_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-9a5c3a4a', 'hca_prod_896f377c8e88463e82b0b2a5409d6fe4__20240201_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-aef76795', 'hca_prod_902dc0437091445c9442d72e163b9879__20240201_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-dea71195', 'hca_prod_95f07e6e6a734e1ba880c83996b3aa5c__20220118_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-33ff6c5e', 'hca_prod_9a23ac2d93dd4bac9bb8040e6426db9d__20220906_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-060b0c69', 'hca_prod_a2a2f324cf24409ea859deaee871269c__20220330_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-5e526b40', 'hca_prod_aebc99a33151482a9709da6802617763__20240201_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-84148b68', 'hca_prod_aecfd908674c4d4eb36e0c1ceab02245__20231101_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-a31095ba', 'hca_prod_aff9c3cd6b844fc2abf2b9c0b3038277__20220330_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-9a93e11b', 'hca_prod_c302fe54d22d451fa130e24df3d6afca__20220606_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-6db90b39', 'hca_prod_c4077b3c5c984d26a614246d12c2e5d7__20220118_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-cc96a15f', 'hca_prod_c6ef0270eafc43bd8097c10020a03cfc__20240201_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-258d3043', 'hca_prod_c9e83418a9f04ed1ab4f56d9513417bf__20240201_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-faa23f13', 'hca_prod_e1fda2177ee14c1aadfa648279dafac6__20240201_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-87eefe3c', 'hca_prod_e255b1c611434fa683a8528f15b41038__20220330_dcp2_20240202_dcp35'),
    mksrc('bigquery', 'datarepo-36bcfc7a', 'hca_prod_e9f36305d85744a393f0df4e6007dc97__20220519_dcp2_20240202_dcp35')
]))

dcp36_sources = mkdict(dcp35_sources, 441, mkdelta([
    # @formatter:off
    mksrc('bigquery', 'datarepo-e650c603', 'hca_prod_07d5987e7f9e4f34b0fba185a35504f5__20230427_dcp2_20240301_dcp36'),
    mksrc('bigquery', 'datarepo-bac72cd7', 'hca_prod_116965f3f09447699d28ae675c1b569c__20220107_dcp2_20240301_dcp36'),
    mksrc('bigquery', 'datarepo-4c496b01', 'hca_prod_28dd14388f4040d08e53ee3301b66218__20240301_dcp2_20240306_dcp36'),
    mksrc('bigquery', 'datarepo-314aac18', 'hca_prod_377c35d193bf470c806708f954b269bd__20240301_dcp2_20240301_dcp36'),
    mksrc('bigquery', 'datarepo-9f97463d', 'hca_prod_87f519b4886241f9acff75e823e0e430__20240301_dcp2_20240301_dcp36'),
    mksrc('bigquery', 'datarepo-383230bf', 'hca_prod_9483c664d5464b309ba3efbdbf9290b4__20240301_dcp2_20240301_dcp36'),
    mksrc('bigquery', 'datarepo-f0643a05', 'hca_prod_957261f72bd64358a6ed24ee080d5cfc__20220330_dcp2_20240301_dcp36'),
    mksrc('bigquery', 'datarepo-72f08c60', 'hca_prod_c3354786c17c4e53b4d7c7afbed5b208__20230526_dcp2_20230530_dcp28', pop), # noqa E501
    mksrc('bigquery', 'datarepo-91076846', 'hca_prod_e090445c69714212bc5fae4ec3914102__20230427_dcp2_20240301_dcp36')
    # @formatter:on
]))

dcp37_sources = mkdict(dcp36_sources, 450, mkdelta([
    mksrc('bigquery', 'datarepo-e57afe2a', 'hca_prod_2079bb2e676e4bbf8c68f9c6459edcbb__20240327_dcp2_20240328_dcp37'),
    mksrc('bigquery', 'datarepo-a37f1015', 'hca_prod_46a7e4bf04744a8f8d1843afcde90491__20240327_dcp2_20240328_dcp37'),
    mksrc('bigquery', 'datarepo-3bb4aecc', 'hca_prod_4bcc16b57a4745bbb9c0be9d5336df2d__20240327_dcp2_20240328_dcp37'),
    mksrc('bigquery', 'datarepo-cad8e8e7', 'hca_prod_581de139461f4875b40856453a9082c7__20231212_dcp2_20240328_dcp37'),
    mksrc('bigquery', 'datarepo-27a37706', 'hca_prod_60109425a6e64be1a3bc15de680317d4__20240327_dcp2_20240328_dcp37'),
    mksrc('bigquery', 'datarepo-03e157f1', 'hca_prod_6836c1e4906b4c34a11ccb025167896d__20240327_dcp2_20240328_dcp37'),
    mksrc('bigquery', 'datarepo-a1e5fe66', 'hca_prod_69324a96a68a4514bbb4f8f3ea4bd0f1__20240327_dcp2_20240328_dcp37'),
    mksrc('bigquery', 'datarepo-f9215b2b', 'hca_prod_750b455ae3cf472195818609a6c9d561__20240327_dcp2_20240328_dcp37'),
    mksrc('bigquery', 'datarepo-d8e57f88', 'hca_prod_86fe0a0c88b34a3e94a16f9feadc401e__20240327_dcp2_20240328_dcp37'),
    mksrc('bigquery', 'datarepo-f0498b78', 'hca_prod_902dc0437091445c9442d72e163b9879__20240201_dcp2_20240328_dcp37'),
    mksrc('bigquery', 'datarepo-28635bac', 'hca_prod_aebc99a33151482a9709da6802617763__20240201_dcp2_20240328_dcp37'),
    mksrc('bigquery', 'datarepo-6ac05956', 'hca_prod_c05184453b3b49c6b8fcc41daa4eacba__20220213_dcp2_20240328_dcp37'),
    mksrc('bigquery', 'datarepo-86633e77', 'hca_prod_c0fecf0baf8641b8ba82d5fd81b7542a__20240301_dcp2_20240328_dcp37')
]))

dcp38_sources = mkdict(dcp37_sources, 455, mkdelta([
    mksrc('bigquery', 'datarepo-316d4b45', 'hca_prod_1662accf0e0c48c493145aba063f2220__20240503_dcp2_20240508_dcp38'),
    mksrc('bigquery', 'datarepo-126c9c22', 'hca_prod_bcdf233f92464c0c98430514120b7e3a__20240503_dcp2_20240508_dcp38'),
    mksrc('bigquery', 'datarepo-cc6b2b4f', 'hca_prod_c05184453b3b49c6b8fcc41daa4eacba__20220213_dcp2_20240508_dcp38'),
    mksrc('bigquery', 'datarepo-5292bdb6', 'hca_prod_ccc3b7861da0427fa45f76306d6143b6__20240503_dcp2_20240508_dcp38'),
    mksrc('bigquery', 'datarepo-37460143', 'hca_prod_d5c91e922e7f473d8cf3ab03bbae21c2__20240503_dcp2_20240508_dcp38'),
    mksrc('bigquery', 'datarepo-39884574', 'hca_prod_daef3fda262045aea3f71613814a35bf__20240503_dcp2_20240508_dcp38')
]))

dcp39_sources = mkdict(dcp38_sources, 455, mkdelta([
    # @formatter:off
    mksrc('bigquery', 'datarepo-31abbcbe', 'hca_prod_4a95101c9ffc4f30a809f04518a23803__20220113_dcp2_20240603_dcp39'),
    mksrc('bigquery', 'datarepo-664a24cb', 'hca_prod_7c75f07c608d4c4aa1b7b13d11c0ad31__20220117_dcp2_20230314_dcp25', pop), # noqa E501
    mksrc('bigquery', 'datarepo-cd6f5afa', 'hca_prod_838d46603d624b08b32ddc5cbd93919d__20240531_dcp2_20240603_dcp39'),
    mksrc('bigquery', 'datarepo-f6c258a6', 'hca_prod_9483c664d5464b309ba3efbdbf9290b4__20240301_dcp2_20240604_dcp39'),
    mksrc('bigquery', 'datarepo-cf29bb39', 'hca_prod_f2078d5f2e7d48448552f7c41a231e52__20230201_dcp2_20240603_dcp39')
    # @formatter:on
]))

dcp40_sources = mkdict(dcp39_sources, 458, mkdelta([
    mksrc('bigquery', 'datarepo-7ff6ae27', 'hca_prod_005d611a14d54fbf846e571a1f874f70__20220111_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-083a593d', 'hca_prod_027c51c60719469fa7f5640fe57cbece__20220110_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-6e878a15', 'hca_prod_065e6c13ad6b46a38075c3137eb03068__20220213_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-d001eadd', 'hca_prod_102018327c7340339b653ef13d81656a__20220213_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-56a4f662', 'hca_prod_135f7f5c4a854bcf9f7c4f035ff1e428__20220729_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-b081c1a1', 'hca_prod_1538d572bcb7426b8d2c84f3a7f87bb0__20220630_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-0c56d5cc', 'hca_prod_16dc40f92c1342e38cdf251e95bfc043__20240708_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-2f17d9dd', 'hca_prod_16ed4ad8731946b288596fe1c1d73a82__20220111_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-98b77527', 'hca_prod_1c6a960d52ac44eab728a59c7ab9dc8e__20220110_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-8c31fd19', 'hca_prod_2d4d89f2ebeb467cae60a3efc5e8d4ba__20230206_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-5feaa5ad', 'hca_prod_31887183a72c43089eacc6140313f39c__20220111_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-c094bcbc', 'hca_prod_40272c3b46974bd4ba3f82fa96b9bf71__20220303_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-d72f8298', 'hca_prod_425c2759db664c93a358a562c069b1f1__20220519_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-496892e7', 'hca_prod_4a95101c9ffc4f30a809f04518a23803__20220113_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-1f66dc6c', 'hca_prod_4bec484dca7a47b48d488830e06ad6db__20220113_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-3b468668', 'hca_prod_4d6f6c962a8343d88fe10f53bffd4674__20220113_dcp2_20240712_dcp40'),
    mksrc('bigquery', 'datarepo-03fca13b', 'hca_prod_50151324f3ed435898afec352a940a61__20220113_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-bfba7263', 'hca_prod_51f02950ee254f4b8d0759aa99bb3498__20220117_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-af6e91dc', 'hca_prod_577c946d6de54b55a854cd3fde40bff2__20220117_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-1a5200cb', 'hca_prod_86fd2521c5014e41841c06d79277bb7c__20240708_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-436c5a47', 'hca_prod_99101928d9b14aafb759e97958ac7403__20220118_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-e10ecf5f', 'hca_prod_a83b7f45bfb14c6a97e98e3370065cc1__20240708_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-028b06ac', 'hca_prod_ad04c8e79b7d4cceb8e901e31da10b94__20220118_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-7c60076d', 'hca_prod_ae71be1dddd84feb9bed24c3ddb6e1ad__20220118_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-27cbfba4', 'hca_prod_b963bd4b4bc14404842569d74bc636b8__20220118_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-7345f02d', 'hca_prod_c16a754f5da346ed8c1e6426af2ef625__20220519_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-ed0b32a2', 'hca_prod_c1a9a93dd9de4e659619a9cec1052eaa__20220118_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-9e3eace2', 'hca_prod_c211fd49d9804ba18c6ac24254a3cb52__20220303_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-4db5785d', 'hca_prod_c4077b3c5c984d26a614246d12c2e5d7__20220118_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-325d0681', 'hca_prod_c5ca43aa3b2b42168eb3f57adcbc99a1__20220118_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-2e8307b5', 'hca_prod_c6ad8f9bd26a4811b2ba93d487978446__20220118_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-812cbdeb', 'hca_prod_cddab57b68684be4806f395ed9dd635a__20220118_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-d8cb1e24', 'hca_prod_d3446f0c30f34a12b7c36af877c7bb2d__20220119_dcp2_20240711_dcp40'),
    mksrc('bigquery', 'datarepo-bde87024', 'hca_prod_dc0b65b0771346f0a3390b03ea786046__20230427_dcp2_20240711_dcp40')
]))

dcp41_sources = mkdict(dcp40_sources, 462, mkdelta([
    mksrc('bigquery', 'datarepo-ed01025c', 'hca_prod_0cc58d0b17344e1d9113b32e52f75e36__20240531_dcp2_20240604_dcp39'),
    mksrc('bigquery', 'datarepo-50b00aaf', 'hca_prod_2079bb2e676e4bbf8c68f9c6459edcbb__20240327_dcp2_20240807_dcp41'),
    mksrc('bigquery', 'datarepo-32cb91ae', 'hca_prod_4bcc16b57a4745bbb9c0be9d5336df2d__20240327_dcp2_20240807_dcp41'),
    mksrc('bigquery', 'datarepo-17cfd151', 'hca_prod_660fc8b58fb840508c57e6313195bc81__20240806_dcp2_20240807_dcp41'),
    mksrc('bigquery', 'datarepo-cc9e8ac9', 'hca_prod_815c5ef50fb14eb798821d160362468e__20240806_dcp2_20240807_dcp41'),
    mksrc('bigquery', 'datarepo-c2886bdd', 'hca_prod_838d46603d624b08b32ddc5cbd93919d__20240531_dcp2_20240807_dcp41'),
    mksrc('bigquery', 'datarepo-832dbfa1', 'hca_prod_c16a754f5da346ed8c1e6426af2ef625__20220519_dcp2_20240807_dcp41'),
    mksrc('bigquery', 'datarepo-65bb12f3', 'hca_prod_e870ab5635374b6da66f534fbf8cc57f__20240806_dcp2_20240807_dcp41')
]))

dcp42_sources = mkdict(dcp41_sources, 470, mkdelta([
    # @formatter:off
    mksrc('bigquery', 'datarepo-db22b6c5', 'hca_prod_19037ec943a74823b93f9e59c694d17e__20240903_dcp2_20240904_dcp42'),
    mksrc('bigquery', 'datarepo-8e43554a', 'hca_prod_35d5b0573daf4ccd8112196194598893__20240903_dcp2_20240905_dcp42', ma),  # noqa E501
    mksrc('bigquery', 'datarepo-5b6ac433', 'hca_prod_5f1a1aee6c484dd4a2c4eb4ca6aadf74__20240903_dcp2_20240904_dcp42',),
    mksrc('bigquery', 'datarepo-d5e4c41e', 'hca_prod_7c75f07c608d4c4aa1b7b13d11c0ad31__20220117_dcp2_20240904_dcp42',),
    mksrc('bigquery', 'datarepo-eb6182b7', 'hca_prod_888f17664c8443bb8717b5f9d2046097__20240903_dcp2_20240904_dcp42',),
    mksrc('bigquery', 'datarepo-b9e1d9ec', 'hca_prod_9dd91b6e7c6249d3a3d474f603deffdb__20240903_dcp2_20240904_dcp42',),
    mksrc('bigquery', 'datarepo-582bf509', 'hca_prod_b176d75662d8493383a48b026380262f__20240903_dcp2_20240904_dcp42',),
    mksrc('bigquery', 'datarepo-c85d293d', 'hca_prod_f598aee0d269403690e9d6d5b1c84429__20240903_dcp2_20240904_dcp42',)
    # @formatter:on
]))

dcp43_sources = mkdict(dcp42_sources, 476, mkdelta([
    # @formatter:off
    mksrc('bigquery', 'datarepo-ac7cee91', 'hca_prod_087efc3c26014de6bbe90114593050d1__20241004_dcp2_20241007_dcp43'),
    mksrc('bigquery', 'datarepo-e9df1043', 'hca_prod_248c5dc36b754fb4ad8acc771968483f__20240806_dcp2_20241007_dcp43'),
    mksrc('bigquery', 'datarepo-65c49269', 'hca_prod_2ef3655a973d4d699b4121fa4041eed7__20220111_dcp2_20241004_dcp43'),
    mksrc('bigquery', 'datarepo-456691e5', 'hca_prod_3627473eb6d645c987b5b9f12ce57a10__20241004_dcp2_20241007_dcp43'),
    mksrc('bigquery', 'datarepo-c577eed5', 'hca_prod_7f351a4cd24c4fcd9040f79071b097d0__20220906_dcp2_20241004_dcp43'),
    mksrc('bigquery', 'datarepo-1dbd3c50', 'hca_prod_ae9f439bbd474d6ebd7232dc70b35d97__20241004_dcp2_20241004_dcp43', ma),  # noqa E501
    mksrc('bigquery', 'datarepo-21d1f89b', 'hca_prod_b39381584e8d4fdb9e139e94270dde16__20241004_dcp2_20241004_dcp43'),
    mksrc('bigquery', 'datarepo-550c8f98', 'hca_prod_c3dd819dabab4957b20988f1e0900368__20241004_dcp2_20241004_dcp43'),
    mksrc('bigquery', 'datarepo-06a00830', 'hca_prod_c5ca43aa3b2b42168eb3f57adcbc99a1__20220118_dcp2_20241004_dcp43'),
    mksrc('bigquery', 'datarepo-55151ed4', 'hca_prod_cdabcf0b76024abf9afb3b410e545703__20230201_dcp2_20241008_dcp43')
    # @formatter:on
]))

lungmap_sources = mkdict({}, 3, mkdelta([
    mksrc('bigquery', 'datarepo-32f75497', 'lungmap_prod_00f056f273ff43ac97ff69ca10e38c89__20220308_20220308'),
    mksrc('bigquery', 'datarepo-7066459d', 'lungmap_prod_1bdcecde16be420888f478cd2133d11d__20220308_20220308'),
    mksrc('bigquery', 'datarepo-cfaedae8', 'lungmap_prod_2620497955a349b28d2b53e0bdfcb176__20220308_20220308'),
]))

lm2_sources = mkdict(lungmap_sources, 5, mkdelta([
    mksrc('bigquery', 'datarepo-5eee9956', 'lungmap_prod_00f056f273ff43ac97ff69ca10e38c89__20220308_20220314_lm2'),
    mksrc('bigquery', 'datarepo-73453de6', 'lungmap_prod_20037472ea1d4ddb9cd356a11a6f0f76__20220307_20220310_lm2'),
    mksrc('bigquery', 'datarepo-360d3b54', 'lungmap_prod_f899709cae2c4bb988f0131142e6c7ec__20220310_20220608_lm2'),
]))

lm3_sources = mkdict(lm2_sources, 6, mkdelta([
    mksrc('bigquery', 'datarepo-d139f96d', 'lungmap_prod_1bdcecde16be420888f478cd2133d11d__20220308_20230207_lm3'),
    mksrc('bigquery', 'datarepo-0fdfdb69', 'lungmap_prod_6135382f487d4adb9cf84d6634125b68__20230207_20230314_lm3'),
]))

lm4_sources = mkdict(lm3_sources, 7, mkdelta([
    mksrc('bigquery', 'datarepo-3d684ccd', 'lungmap_prod_00f056f273ff43ac97ff69ca10e38c89__20220308_20231207_lm4'),
    mksrc('bigquery', 'datarepo-a65c8237', 'lungmap_prod_1bdcecde16be420888f478cd2133d11d__20220308_20231207_lm4'),
    mksrc('bigquery', 'datarepo-43d1f2cb', 'lungmap_prod_20037472ea1d4ddb9cd356a11a6f0f76__20220307_20231207_lm4'),
    mksrc('bigquery', 'datarepo-91587240', 'lungmap_prod_2620497955a349b28d2b53e0bdfcb176__20220308_20231207_lm4'),
    mksrc('bigquery', 'datarepo-252f2a7d', 'lungmap_prod_4ae8c5c91520437198276935661f6c84__20231004_20231207_lm4'),
    mksrc('bigquery', 'datarepo-e70d4665', 'lungmap_prod_6135382f487d4adb9cf84d6634125b68__20230207_20231207_lm4'),
    mksrc('bigquery', 'datarepo-3f332829', 'lungmap_prod_f899709cae2c4bb988f0131142e6c7ec__20220310_20231207_lm4'),
]))

lm6_sources = mkdict(lm4_sources, 8, mkdelta([
    mksrc('bigquery', 'datarepo-c3ad47d2', 'lungmap_prod_6511b041b11e4ccf85932b40148c437e__20240326_20240326_lm6'),
]))

lm7_sources = mkdict(lm6_sources, 10, mkdelta([
    mksrc('bigquery', 'datarepo-2555a3ee', 'lungmap_prod_1977dc4784144263a8706b0f207d8ab3__20240206_20240626_lm7'),
    mksrc('bigquery', 'datarepo-43814140', 'lungmap_prod_fdadee7e209745d5bf81cc280bd8348e__20240206_20240626_lm7')
]))

lm8_sources = mkdict(lm7_sources, 12, mkdelta([
    mksrc('bigquery', 'datarepo-2b15227b', 'lungmap_prod_1977dc4784144263a8706b0f207d8ab3__20240206_20241002_lm8'),
    mksrc('bigquery', 'datarepo-c9158593', 'lungmap_prod_20037472ea1d4ddb9cd356a11a6f0f76__20220307_20241002_lm8'),
    mksrc('bigquery', 'datarepo-35a6d7ca', 'lungmap_prod_3a02d15f9c6a4ef7852b4ddec733b70b__20241001_20241002_lm8'),
    mksrc('bigquery', 'datarepo-131a1234', 'lungmap_prod_4ae8c5c91520437198276935661f6c84__20231004_20241002_lm8'),
    mksrc('bigquery', 'datarepo-3377446f', 'lungmap_prod_6135382f487d4adb9cf84d6634125b68__20230207_20241002_lm8'),
    mksrc('bigquery', 'datarepo-3c4905d2', 'lungmap_prod_834e0d1671b64425a8ab022b5000961c__20241001_20241002_lm8'),
    mksrc('bigquery', 'datarepo-d7447983', 'lungmap_prod_f899709cae2c4bb988f0131142e6c7ec__20220310_20241002_lm8'),
    mksrc('bigquery', 'datarepo-c11ef363', 'lungmap_prod_fdadee7e209745d5bf81cc280bd8348e__20240206_20241002_lm8'),
]))


def env() -> Mapping[str, Optional[str]]:
    """
    Returns a dictionary that maps environment variable names to values. The
    values are either None or strings. String values can contain references to
    other environment variables in the form `{FOO}` where FOO is the name of an
    environment variable. See

    https://docs.python.org/3.11/library/string.html#format-string-syntax

    for the concrete syntax. These references will be resolved *after* the
    overall environment has been compiled by merging all relevant
    `environment.py` and `environment.local.py` files.

    Entries with a `None` value will be excluded from the environment. They
    can be used to document a variable without a default value in which case
    other, more specific `environment.py` or `environment.local.py` files must
    provide the value.
    """
    return {
        # Set variables for the `prod` (short for production) deployment here.
        #
        # Only modify this file if you intend to commit those changes. To change the
        # environment with a setting that's specific to you AND the deployment, create
        # a environment.local.py right next to this file and make your changes there.
        # Settings applicable to all environments but specific to you go into
        # environment.local.py at the project root.

        'AZUL_DEPLOYMENT_STAGE': 'prod',

        'AZUL_DOMAIN_NAME': 'azul.data.humancellatlas.org',

        'AZUL_CATALOGS': base64.b64encode(bz2.compress(json.dumps({
            f'{catalog}{suffix}': dict(atlas=atlas,
                                       internal=internal,
                                       plugins=dict(metadata=dict(name='hca'),
                                                    repository=dict(name='tdr_hca')),
                                       sources=mklist(sources))
            for atlas, catalog, sources in [
                ('hca', 'dcp42', dcp42_sources),
                ('hca', 'dcp43', dcp43_sources),
                ('lungmap', 'lm7', lm7_sources),
                ('lungmap', 'lm8', lm8_sources)
            ] for suffix, internal in [
                ('', False),
                ('-it', True)
            ]
        }).encode())).decode('ascii'),

        'AZUL_TDR_SOURCE_LOCATION': 'US',
        'AZUL_TDR_SERVICE_URL': 'https://data.terra.bio',
        'AZUL_SAM_SERVICE_URL': 'https://sam.dsde-prod.broadinstitute.org',
        'AZUL_TERRA_SERVICE_URL': 'https://firecloud-orchestration.dsde-prod.broadinstitute.org',

        'AZUL_ENABLE_MONITORING': '1',

        # $0.382/h × 4 × 24h/d × 30d/mo = $1100.16/mo
        'AZUL_ES_INSTANCE_TYPE': 'r6gd.xlarge.elasticsearch',
        'AZUL_ES_INSTANCE_COUNT': '4',

        'AZUL_CONTRIBUTION_CONCURRENCY': '300/64',

        'AZUL_DEBUG': '1',

        'AZUL_BILLING': 'hca',

        'AZUL_OWNER': 'hannes@ucsc.edu',

        'AZUL_MONITORING_EMAIL': 'azul-group@ucsc.edu',

        'AZUL_AWS_ACCOUNT_ID': '542754589326',
        'AWS_DEFAULT_REGION': 'us-east-1',

        'GOOGLE_PROJECT': 'platform-hca-prod',

        'AZUL_GOOGLE_OAUTH2_CLIENT_ID': '473200283737-h5e1l7neunbuesrtgjf8b12lb7o3jf1m.apps.googleusercontent.com',

        'azul_slack_integration': json.dumps({
            'workspace_id': 'T09P9H91S',  # ucsc-gi.slack.com
            'channel_id': 'C04JWDFCPFZ'  # #team-boardwalk-prod
        }),

        'AZUL_ENABLE_REPLICAS': '1',
    }
