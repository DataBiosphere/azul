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


def mksrc(google_project, snapshot, subgraphs, flags: int = 0):
    _, env, project, _ = snapshot.split('_', 3)
    assert flags <= ma | pop
    source = None if flags & pop else ':'.join([
        'tdr',
        google_project,
        'snapshot/' + snapshot,
        '/' + str(partition_prefix_length(subgraphs))
    ])
    return project, source


def mkdict(items):
    result = dict(items)
    assert len(items) == len(result), 'collisions detected'
    assert list(result.keys()) == sorted(result.keys()), 'input not sorted'
    return result


dcp1_sources = mkdict([
    mksrc('datarepo-673cd580', 'hca_prod_005d611a14d54fbf846e571a1f874f70__20211129_dcp1_20211129_dcp1', 13),
    mksrc('datarepo-daebba9b', 'hca_prod_027c51c60719469fa7f5640fe57cbece__20211129_dcp1_20211129_dcp1', 7),
    mksrc('datarepo-41546537', 'hca_prod_091cf39b01bc42e59437f419a66c8a45__20211129_dcp1_20211129_dcp1', 19),
    mksrc('datarepo-23f37ee4', 'hca_prod_116965f3f09447699d28ae675c1b569c__20211129_dcp1_20211129_dcp1', 7),
    mksrc('datarepo-dde43a62', 'hca_prod_1defdadaa36544ad9b29443b06bd11d6__20211129_dcp1_20211129_dcp1', 2560),
    mksrc('datarepo-4112f077', 'hca_prod_2043c65a1cf84828a6569e247d4e64f1__20211129_dcp1_20220126_dcp1', 3467),
    mksrc('datarepo-3fb60b9c', 'hca_prod_4a95101c9ffc4f30a809f04518a23803__20211129_dcp1_20211129_dcp1', 33),
    mksrc('datarepo-70f89602', 'hca_prod_4d6f6c962a8343d88fe10f53bffd4674__20211129_dcp1_20211129_dcp1', 11),
    mksrc('datarepo-db7a8a68', 'hca_prod_4e6f083b5b9a439398902a83da8188f1__20211129_dcp1_20211129_dcp1', 29),
    mksrc('datarepo-b0a40e19', 'hca_prod_577c946d6de54b55a854cd3fde40bff2__20211129_dcp1_20211129_dcp1', 6),
    mksrc('datarepo-5df70008', 'hca_prod_74b6d5693b1142efb6b1a0454522b4a0__20211129_dcp1_20211129_dcp1', 5459),
    mksrc('datarepo-8d469885', 'hca_prod_8185730f411340d39cc3929271784c2b__20211213_dcp1_20220104_dcp1', 11),
    mksrc('datarepo-fa418c19', 'hca_prod_88ec040b87054f778f41f81e57632f7d__20220111_dcp1_20220126_dcp1', 4927),
    mksrc('datarepo-a91ec558', 'hca_prod_8c3c290ddfff4553886854ce45f4ba7f__20211213_dcp1_20220104_dcp1', 6639),
    mksrc('datarepo-ba05c608', 'hca_prod_90bd693340c048d48d76778c103bf545__20211220_dcp1_20220104_dcp1', 2244),
    mksrc('datarepo-c3a18afe', 'hca_prod_9c20a245f2c043ae82c92232ec6b594f__20211221_dcp1_20220104_dcp1', 11),
    mksrc('datarepo-8bdfb63a', 'hca_prod_a29952d9925e40f48a1c274f118f1f51__20211213_dcp1_20220104_dcp1', 25),
    mksrc('datarepo-2fa410c8', 'hca_prod_a9c022b4c7714468b769cabcf9738de3__20211213_dcp1_20220104_dcp1', 22),
    mksrc('datarepo-f646ffa6', 'hca_prod_abe1a013af7a45ed8c26f3793c24a1f4__20211213_dcp1_20220104_dcp1', 45),
    mksrc('datarepo-0e566a81', 'hca_prod_ae71be1dddd84feb9bed24c3ddb6e1ad__20211213_dcp1_20220104_dcp1', 3514),
    mksrc('datarepo-798bacbd', 'hca_prod_c4077b3c5c984d26a614246d12c2e5d7__20211220_dcp1_20220106_dcp1', 215),
    mksrc('datarepo-6d2bb0f1', 'hca_prod_cc95ff892e684a08a234480eca21ce79__20211220_dcp1_20220106_dcp1', 255),
    mksrc('datarepo-ee76186b', 'hca_prod_cddab57b68684be4806f395ed9dd635a__20220113_dcp1_20220126_dcp1', 5089),
    mksrc('datarepo-224af5ae', 'hca_prod_e0009214c0a04a7b96e2d6a83e966ce0__20220113_dcp1_20220126_dcp1', 99840),
    mksrc('datarepo-dc119c81', 'hca_prod_f81efc039f564354aabb6ce819c3d414__20211220_dcp1_20220106_dcp1', 3),
    mksrc('datarepo-f144a7cd', 'hca_prod_f83165c5e2ea4d15a5cf33f3550bffde__20220111_dcp1_20220126_dcp1', 15257),
    mksrc('datarepo-62ca7774', 'hca_prod_f86f1ab41fbb4510ae353ffd752d4dfc__20211220_dcp1_20220106_dcp1', 19),
    mksrc('datarepo-057faf2b', 'hca_prod_f8aa201c4ff145a4890e840d63459ca2__20211220_dcp1_20220106_dcp1', 382),
])

dcp12_sources = mkdict([
    mksrc('datarepo-a1c89fba', 'hca_prod_005d611a14d54fbf846e571a1f874f70__20220111_dcp2_20220113_dcp12', 7),
    mksrc('datarepo-a9316414', 'hca_prod_027c51c60719469fa7f5640fe57cbece__20220110_dcp2_20220113_dcp12', 8),
    mksrc('datarepo-d111fe96', 'hca_prod_03c6fce7789e4e78a27a664d562bb738__20220110_dcp2_20220113_dcp12', 1530),
    mksrc('datarepo-a2d29140', 'hca_prod_04ad400c58cb40a5bc2b2279e13a910b__20220114_dcp2_20220114_dcp12', 355),
    mksrc('datarepo-d8ad6862', 'hca_prod_05657a599f9d4bb9b77b24be13aa5cea__20220110_dcp2_20220113_dcp12', 185),
    mksrc('datarepo-c9b9a2e8', 'hca_prod_05be4f374506429bb112506444507d62__20220107_dcp2_20220113_dcp12', 1544),
    mksrc('datarepo-4e087937', 'hca_prod_07073c1280064710a00b23abdb814904__20220107_dcp2_20220113_dcp12', 25),
    mksrc('datarepo-9226064c', 'hca_prod_074a9f88729a455dbca50ce80edf0cea__20220107_dcp2_20220113_dcp12', 2),
    mksrc('datarepo-5bd98333', 'hca_prod_0792db3480474e62802c9177c9cd8e28__20220107_dcp2_20220114_dcp12', 1450),
    mksrc('datarepo-580db83c', 'hca_prod_08b794a0519c4516b184c583746254c5__20220107_dcp2_20220114_dcp12', 2),
    mksrc('datarepo-0b49ea1e', 'hca_prod_091cf39b01bc42e59437f419a66c8a45__20220107_dcp2_20220114_dcp12', 20),
    mksrc('datarepo-109db6e4', 'hca_prod_0c09fadee0794fde8e606725b8c1d84b__20220107_dcp2_20220114_dcp12', 31),
    mksrc('datarepo-26de5247', 'hca_prod_0c3b7785f74d40918616a68757e4c2a8__20220111_dcp2_20220114_dcp12', 178),
    mksrc('datarepo-ae49a863', 'hca_prod_0d4b87ea6e9e456982e41343e0e3259f__20220110_dcp2_20220114_dcp12', 8),
    mksrc('datarepo-76169feb', 'hca_prod_0fd8f91862d64b8bac354c53dd601f71__20220110_dcp2_20220114_dcp12', 10),
    mksrc('datarepo-4b42c4ef', 'hca_prod_116965f3f09447699d28ae675c1b569c__20220107_dcp2_20220114_dcp12', 8),
    mksrc('datarepo-eb39c36f', 'hca_prod_16ed4ad8731946b288596fe1c1d73a82__20220111_dcp2_20220114_dcp12', 28),
    mksrc('datarepo-982c56ad', 'hca_prod_1c6a960d52ac44eab728a59c7ab9dc8e__20220110_dcp2_20220114_dcp12', 10),
    mksrc('datarepo-f24e8394', 'hca_prod_1cd1f41ff81a486ba05b66ec60f81dcf__20220107_dcp2_20220114_dcp12', 18),
    mksrc('datarepo-b8ffd379', 'hca_prod_1ce3b3dc02f244a896dad6d107b27a76__20220107_dcp2_20220114_dcp12', 421),
    mksrc('datarepo-b1ac3907', 'hca_prod_1defdadaa36544ad9b29443b06bd11d6__20220111_dcp2_20220114_dcp12', 2561),
    mksrc('datarepo-4e5e9f9b', 'hca_prod_2043c65a1cf84828a6569e247d4e64f1__20220111_dcp2_20220120_dcp12', 1735),
    mksrc('datarepo-156c78f4', 'hca_prod_2084526ba66f4c40bb896fd162f2eb38__20220111_dcp2_20220114_dcp12', 23),
    mksrc('datarepo-228ac7b7', 'hca_prod_2086eb0510b9432bb7f0169ccc49d270__20220111_dcp2_20220114_dcp12', 10),
    mksrc('datarepo-7defc353', 'hca_prod_20f37aafcaa140e69123be6ce8feb2d6__20220111_dcp2_20220114_dcp12', 484),
    mksrc('datarepo-783bc6c3', 'hca_prod_21ea8ddb525f4f1fa82031f0360399a2__20220111_dcp2_20220114_dcp12', 35),
    mksrc('datarepo-d8b00524', 'hca_prod_23587fb31a4a4f58ad74cc9a4cb4c254__20220111_dcp2_20220114_dcp12', 1476),
    mksrc('datarepo-8390f5e3', 'hca_prod_248fcf0316c64a41b6ccaad4d894ca42__20220111_dcp2_20220114_dcp12', 2958),
    mksrc('datarepo-45f08380', 'hca_prod_24c654a5caa5440a8f02582921f2db4a__20220111_dcp2_20220114_dcp12', 6),
    mksrc('datarepo-ab44f4d8', 'hca_prod_2a64db431b554639aabb8dba0145689d__20220111_dcp2_20220114_dcp12', 10),
    mksrc('datarepo-bfdde7e3', 'hca_prod_2a72a4e566b2405abb7c1e463e8febb0__20220111_dcp2_20220114_dcp12', 2290),
    mksrc('datarepo-f4d7c97e', 'hca_prod_2ad191cdbd7a409b9bd1e72b5e4cce81__20220111_dcp2_20220114_dcp12', 40),
    mksrc('datarepo-e4d77c97', 'hca_prod_2af52a1365cb4973b51339be38f2df3f__20220111_dcp2_20220114_dcp12', 10),
    mksrc('datarepo-aebdd74a', 'hca_prod_2b38025da5ea4c0fb22e367824bcaf4c__20220111_dcp2_20220114_dcp12', 44),
    mksrc('datarepo-e67b97d4', 'hca_prod_2d8460958a334f3c97d4585bafac13b4__20220111_dcp2_20220120_dcp12', 3589),
    mksrc('datarepo-b123707e', 'hca_prod_2ef3655a973d4d699b4121fa4041eed7__20220111_dcp2_20220114_dcp12', 8),
    mksrc('datarepo-3b845979', 'hca_prod_2f67614380c24bc6b7b42613fe0fadf0__20220111_dcp2_20220114_dcp12', 1),
    mksrc('datarepo-40cecf86', 'hca_prod_3089d311f9ed44ddbb10397059bad4dc__20220111_dcp2_20220114_dcp12', 129),
    mksrc('datarepo-e6d0e6ab', 'hca_prod_31887183a72c43089eacc6140313f39c__20220111_dcp2_20220114_dcp12', 6),
    mksrc('datarepo-059455a6', 'hca_prod_34cba5e9ecb14d81bf0848987cd63073__20220111_dcp2_20220114_dcp12', 11),
    mksrc('datarepo-18838720', 'hca_prod_376a7f55b8764f609cf3ed7bc83d5415__20220111_dcp2_20220120_dcp12', 45),
    mksrc('datarepo-002f293a', 'hca_prod_379ed69ebe0548bcaf5ea7fc589709bf__20220111_dcp2_20220120_dcp12', 4),
    mksrc('datarepo-94ea8d84', 'hca_prod_38449aea70b540db84b31e08f32efe34__20220111_dcp2_20220120_dcp12', 42),
    mksrc('datarepo-597059bb', 'hca_prod_38e44dd0c3df418e9256d0824748901f__20220112_dcp2_20220120_dcp12', 10),
    mksrc('datarepo-9b80ca5d', 'hca_prod_3a69470330844ece9abed935fd5f6748__20220112_dcp2_20220120_dcp12', 125),
    mksrc('datarepo-caef7414', 'hca_prod_3c27d2ddb1804b2bbf05e2e418393fd1__20220112_dcp2_20220120_dcp12', 6),
    mksrc('datarepo-d091ac22', 'hca_prod_3cfcdff5dee14a7ba591c09c6e850b11__20220112_dcp2_20220120_dcp12', 8),
    mksrc('datarepo-ab983bdd', 'hca_prod_3e329187a9c448ec90e3cc45f7c2311c__20220112_dcp2_20220120_dcp12', 1001),
    mksrc('datarepo-5e5bce33', 'hca_prod_4037007b0eff4e6db7bd8dd8eec80143__20220112_dcp2_20220120_dcp12', 39),
    mksrc('datarepo-c6ce3ced', 'hca_prod_403c3e7668144a2da5805dd5de38c7ff__20220113_dcp2_20220120_dcp12', 63),
    mksrc('datarepo-d2fa6418', 'hca_prod_414accedeba0440fb721befbc5642bef__20220113_dcp2_20220120_dcp12', 4),
    mksrc('datarepo-3ae19ddb', 'hca_prod_41fb1734a121461695c73b732c9433c7__20220113_dcp2_20220120_dcp12', 12),
    mksrc('datarepo-50081b3c', 'hca_prod_42d4f8d454224b78adaee7c3c2ef511c__20220113_dcp2_20220120_dcp12', 9),
    mksrc('datarepo-a7e55305', 'hca_prod_455b46e6d8ea4611861ede720a562ada__20220113_dcp2_20220120_dcp12', 74),
    mksrc('datarepo-99250e4a', 'hca_prod_4a95101c9ffc4f30a809f04518a23803__20220113_dcp2_20220120_dcp12', 37),
    mksrc('datarepo-d1983cfc', 'hca_prod_4af795f73e1d4341b8674ac0982b9efd__20220113_dcp2_20220120_dcp12', 4),
    mksrc('datarepo-21212245', 'hca_prod_4bec484dca7a47b48d488830e06ad6db__20220113_dcp2_20220120_dcp12', 14),
    mksrc('datarepo-001a2f34', 'hca_prod_4d6f6c962a8343d88fe10f53bffd4674__20220113_dcp2_20220120_dcp12', 12),
    mksrc('datarepo-26396466', 'hca_prod_4e6f083b5b9a439398902a83da8188f1__20220113_dcp2_20220120_dcp12', 53),
    mksrc('datarepo-3ef66093', 'hca_prod_50151324f3ed435898afec352a940a61__20220113_dcp2_20220120_dcp12', 116),
    mksrc('datarepo-bd1c5759', 'hca_prod_504e0cee168840fab936361c4a831f87__20220117_dcp2_20220120_dcp12', 252),
    mksrc('datarepo-6ab76705', 'hca_prod_5116c0818be749c58ce073b887328aa9__20220117_dcp2_20220120_dcp12', 26),
    mksrc('datarepo-458232e4', 'hca_prod_51f02950ee254f4b8d0759aa99bb3498__20220117_dcp2_20220120_dcp12', 6),
    mksrc('datarepo-3e19670d', 'hca_prod_520afa10f9d24e93ab7a26c4c863ce18__20220117_dcp2_20220120_dcp12', 649),
    mksrc('datarepo-3eec204e', 'hca_prod_52b29aa4c8d642b4807ab35be94469ca__20220117_dcp2_20220121_dcp12', 467),
    mksrc('datarepo-e7c01a93', 'hca_prod_52d10a60c8d14d068a5eaf0d5c0d5034__20220117_dcp2_20220120_dcp12', 176),
    mksrc('datarepo-3b0847fe', 'hca_prod_53c53cd481274e12bc7f8fe1610a715c__20220117_dcp2_20220120_dcp12', 34),
    mksrc('datarepo-d4f43fb3', 'hca_prod_54aaa409dc2848c5be26d368b4a5d5c6__20220117_dcp2_20220120_dcp12', 57),
    mksrc('datarepo-6ed675f9', 'hca_prod_559bb888782941f2ace52c05c7eb81e9__20220117_dcp2_20220120_dcp12', 22),
    mksrc('datarepo-5bdba230', 'hca_prod_56e73ccb7ae94faea738acfb69936d7a__20220117_dcp2_20220120_dcp12', 10),
    mksrc('datarepo-6b1109e5', 'hca_prod_577c946d6de54b55a854cd3fde40bff2__20220117_dcp2_20220120_dcp12', 7),
    mksrc('datarepo-d6e79c46', 'hca_prod_58028aa80ed249cab60f15e2ed5989d5__20220117_dcp2_20220120_dcp12', 4),
    mksrc('datarepo-8494da48', 'hca_prod_591af954cdcd483996d3a0d1b1e885ac__20220117_dcp2_20220120_dcp12', 33),
    mksrc('datarepo-17088287', 'hca_prod_5b3285614a9740acb7ad6a90fc59d374__20220117_dcp2_20220120_dcp12', 408),
    mksrc('datarepo-4977894a', 'hca_prod_5b5f05b72482468db76d8f68c04a7a47__20220117_dcp2_20220120_dcp12', 87),
    mksrc('datarepo-99725d7d', 'hca_prod_5bb1f67e2ff04848bbcf17d133f0fd2d__20220117_dcp2_20220120_dcp12', 6),
    mksrc('datarepo-83783d1c', 'hca_prod_5eafb94b02d8423e81b83673da319ca0__20220117_dcp2_20220120_dcp12', 39),
    mksrc('datarepo-f25df8f2', 'hca_prod_5ee710d7e2d54fe2818d15f5e31dae32__20220117_dcp2_20220120_dcp12', 41),
    mksrc('datarepo-99348797', 'hca_prod_602628d7c03848a8aa97ffbb2cb44c9d__20220117_dcp2_20220120_dcp12', 14),
    mksrc('datarepo-e8e29a46', 'hca_prod_6072616c87944b208f52fb15992ea5a4__20220117_dcp2_20220120_dcp12', 603),
    mksrc('datarepo-bd224cce', 'hca_prod_60ea42e1af4942f58164d641fdb696bc__20220117_dcp2_20220120_dcp12', 1145),
    mksrc('datarepo-a4f706c9', 'hca_prod_63b5b6c1bbcd487d8c2e0095150c1ecd__20220117_dcp2_20220121_dcp12', 11),
    mksrc('datarepo-0e494119', 'hca_prod_65858543530d48a6a670f972b34dfe10__20220117_dcp2_20220120_dcp12', 48),
    mksrc('datarepo-c8ed0e98', 'hca_prod_67a3de0945b949c3a068ff4665daa50e__20220117_dcp2_20220120_dcp12', 732),
    mksrc('datarepo-b1223d0f', 'hca_prod_68df3629d2d24eedb0aba10e0f019b88__20220117_dcp2_20220120_dcp12', 5),
    mksrc('datarepo-b7734519', 'hca_prod_6c040a938cf84fd598de2297eb07e9f6__20220117_dcp2_20220120_dcp12', 21),
    mksrc('datarepo-489f5a00', 'hca_prod_7027adc6c9c946f384ee9badc3a4f53b__20220117_dcp2_20220120_dcp12', 26),
    mksrc('datarepo-465f2c7c', 'hca_prod_71436067ac414acebe1b2fbcc2cb02fa__20220117_dcp2_20220120_dcp12', 4),
    mksrc('datarepo-574f6410', 'hca_prod_71eb5f6dcee04297b503b1125909b8c7__20220117_dcp2_20220121_dcp12', 36),
    mksrc('datarepo-61e3e2d1', 'hca_prod_74493e9844fc48b0a58fcc7e77268b59__20220117_dcp2_20220120_dcp12', 8),
    mksrc('datarepo-699bbe9b', 'hca_prod_74b6d5693b1142efb6b1a0454522b4a0__20220117_dcp2_20220124_dcp12', 5460),
    mksrc('datarepo-674de9c8', 'hca_prod_75dbbce90cde489c88a793e8f92914a3__20220117_dcp2_20220121_dcp12', 72),
    mksrc('datarepo-d043e30f', 'hca_prod_769a08d1b8a44f1e95f76071a9827555__20220117_dcp2_20220121_dcp12', 1),
    mksrc('datarepo-5c757273', 'hca_prod_783c9952a4ae4106a6ce56f20ce27f88__20220117_dcp2_20220121_dcp12', 8),
    mksrc('datarepo-bc095feb', 'hca_prod_7880637a35a14047b422b5eac2a2a358__20220117_dcp2_20220121_dcp12', 366),
    mksrc('datarepo-333e09de', 'hca_prod_78b2406dbff246fc8b6120690e602227__20220117_dcp2_20220121_dcp12', 216),
    mksrc('datarepo-9268e5a3', 'hca_prod_79b13a2a9ca142a497bd70208a11bea6__20220117_dcp2_20220121_dcp12', 4),
    mksrc('datarepo-93812eed', 'hca_prod_7ac8822c4ef04194adf074290611b1c6__20220117_dcp2_20220121_dcp12', 4),
    mksrc('datarepo-db3813a8', 'hca_prod_7adede6a0ab745e69b67ffe7466bec1f__20220117_dcp2_20220121_dcp12', 1601),
    mksrc('datarepo-33a60e82', 'hca_prod_7b947aa243a74082afff222a3e3a4635__20220117_dcp2_20220121_dcp12', 16),
    mksrc('datarepo-ccf60635', 'hca_prod_7c75f07c608d4c4aa1b7b13d11c0ad31__20220117_dcp2_20220121_dcp12', 66),
    mksrc('datarepo-30e31b57', 'hca_prod_8185730f411340d39cc3929271784c2b__20220117_dcp2_20220121_dcp12', 12),
    mksrc('datarepo-9d5ab6f0', 'hca_prod_83f5188e3bf749569544cea4f8997756__20220117_dcp2_20220121_dcp12', 1613),
    mksrc('datarepo-57af0017', 'hca_prod_842605c7375a47c59e2ca71c2c00fcad__20220117_dcp2_20220121_dcp12', 8),
    mksrc('datarepo-c3aea89c', 'hca_prod_8559a8ed5d8c4fb6bde8ab639cebf03c__20220118_dcp2_20220121_dcp12', 379),
    mksrc('datarepo-a054435f', 'hca_prod_8787c23889ef4636a57d3167e8b54a80__20220118_dcp2_20220121_dcp12', 3),
    mksrc('datarepo-4d40e3cb', 'hca_prod_87d52a86bdc7440cb84d170f7dc346d9__20220118_dcp2_20220121_dcp12', 27),
    mksrc('datarepo-486fab06', 'hca_prod_88ec040b87054f778f41f81e57632f7d__20220118_dcp2_20220121_dcp12', 2628),
    mksrc('datarepo-32fc3ac7', 'hca_prod_8999b4566fa6438bab17b62b1d8ec0c3__20220118_dcp2_20220121_dcp12', 19),
    mksrc('datarepo-19e9b807', 'hca_prod_8a40ff19e6144c50b23b5c9e1d546bab__20220118_dcp2_20220121_dcp12', 140),
    mksrc('datarepo-a71cbef5', 'hca_prod_8ab8726d81b94bd2acc24d50bee786b4__20220118_dcp2_20220121_dcp12', 141),
    mksrc('datarepo-6cf8837e', 'hca_prod_8bd2e5f694534b9b9c5659e3a40dc87e__20220118_dcp2_20220121_dcp12', 80),
    mksrc('datarepo-8383e25b', 'hca_prod_8c3c290ddfff4553886854ce45f4ba7f__20220118_dcp2_20220121_dcp12', 6640),
    mksrc('datarepo-d425ceae', 'hca_prod_8d566d35d8d34975a351be5e25e9b2ea__20220118_dcp2_20220121_dcp12', 23),
    mksrc('datarepo-c15b7397', 'hca_prod_8dacb243e9184bd2bb9aaac6dc424161__20220118_dcp2_20220121_dcp12', 12),
    mksrc('datarepo-8ed2742a', 'hca_prod_90bd693340c048d48d76778c103bf545__20220118_dcp2_20220121_dcp12', 2246),
    mksrc('datarepo-05d8344b', 'hca_prod_94023a08611d4f22a8c990956e091b2e__20220118_dcp2_20220121_dcp12', 93),
    mksrc('datarepo-87faf2bd', 'hca_prod_946c5add47d1402a97bba5af97e8bce7__20220118_dcp2_20220121_dcp12', 149),
    mksrc('datarepo-8238f8f6', 'hca_prod_955dfc2ca8c64d04aa4d907610545d11__20220118_dcp2_20220121_dcp12', 13),
    mksrc('datarepo-0f11337c', 'hca_prod_95f07e6e6a734e1ba880c83996b3aa5c__20220118_dcp2_20220121_dcp12', 40),
    mksrc('datarepo-f680e590', 'hca_prod_962bd805eb894c54bad2008e497d1307__20220118_dcp2_20220121_dcp12', 28),
    mksrc('datarepo-9b7aa7dd', 'hca_prod_99101928d9b14aafb759e97958ac7403__20220118_dcp2_20220121_dcp12', 1190),
    mksrc('datarepo-8a2c2dfd', 'hca_prod_996120f9e84f409fa01e732ab58ca8b9__20220118_dcp2_20220121_dcp12', 26),
    mksrc('datarepo-9385cdd8', 'hca_prod_9d97f01f9313416e9b07560f048b2350__20220118_dcp2_20220121_dcp12', 43),
    mksrc('datarepo-ddcd2940', 'hca_prod_a004b1501c364af69bbd070c06dbc17d__20220118_dcp2_20220121_dcp12', 34),
    mksrc('datarepo-16e78655', 'hca_prod_a29952d9925e40f48a1c274f118f1f51__20220118_dcp2_20220121_dcp12', 26),
    mksrc('datarepo-9aa62158', 'hca_prod_a39728aa70a04201b0a281b7badf3e71__20220118_dcp2_20220121_dcp12', 33),
    mksrc('datarepo-7180120b', 'hca_prod_a60803bbf7db45cfb52995436152a801__20220118_dcp2_20220121_dcp12', 306),
    mksrc('datarepo-b4669bfd', 'hca_prod_a80a63f2e223489081b0415855b89abc__20220118_dcp2_20220121_dcp12', 9),
    mksrc('datarepo-e899aaaa', 'hca_prod_a9301bebe9fa42feb75c84e8a460c733__20220118_dcp2_20220121_dcp12', 18),
    mksrc('datarepo-373a5866', 'hca_prod_a96b71c078a742d188ce83c78925cfeb__20220118_dcp2_20220121_dcp12', 6),
    mksrc('datarepo-92bd008d', 'hca_prod_a991ef154d4a4b80a93ec538b4b54127__20220118_dcp2_20220121_dcp12', 5),
    mksrc('datarepo-6652ddcb', 'hca_prod_a9c022b4c7714468b769cabcf9738de3__20220118_dcp2_20220121_dcp12', 23),
    mksrc('datarepo-4975e16f', 'hca_prod_abe1a013af7a45ed8c26f3793c24a1f4__20220118_dcp2_20220121_dcp12', 46),
    mksrc('datarepo-be8901be', 'hca_prod_ad04c8e79b7d4cceb8e901e31da10b94__20220118_dcp2_20220121_dcp12', 49),
    mksrc('datarepo-4a3b719a', 'hca_prod_ad98d3cd26fb4ee399c98a2ab085e737__20220118_dcp2_20220121_dcp12', 299),
    mksrc('datarepo-d7f6d5fa', 'hca_prod_ae71be1dddd84feb9bed24c3ddb6e1ad__20220118_dcp2_20220121_dcp12', 3516),
    mksrc('datarepo-58678d36', 'hca_prod_b32a9915c81b4cbcaf533a66b5da3c9a__20220118_dcp2_20220121_dcp12', 62),
    mksrc('datarepo-ff628beb', 'hca_prod_b4a7d12f6c2f40a39e359756997857e3__20220118_dcp2_20220121_dcp12', 24),
    mksrc('datarepo-2a48ce64', 'hca_prod_b51f49b40d2e4cbdbbd504cd171fc2fa__20220118_dcp2_20220121_dcp12', 193),
    mksrc('datarepo-ffc43998', 'hca_prod_b7259878436c4274bfffca76f4cb7892__20220118_dcp2_20220121_dcp12', 11),
    mksrc('datarepo-41c56298', 'hca_prod_b9484e4edc404e389b854cecf5b8c068__20220118_dcp2_20220121_dcp12', 62),
    mksrc('datarepo-0cbe9f7b', 'hca_prod_b963bd4b4bc14404842569d74bc636b8__20220118_dcp2_20220121_dcp12', 2),
    mksrc('datarepo-d4fa6f7e', 'hca_prod_bd40033154b94fccbff66bb8b079ee1f__20220118_dcp2_20220121_dcp12', 18),
    mksrc('datarepo-5cfa0843', 'hca_prod_bd7104c9a950490e94727d41c6b11c62__20220118_dcp2_20220121_dcp12', 17),
    mksrc('datarepo-53d134a5', 'hca_prod_c1810dbc16d245c3b45e3e675f88d87b__20220118_dcp2_20220121_dcp12', 134),
    mksrc('datarepo-8d82eeff', 'hca_prod_c1a9a93dd9de4e659619a9cec1052eaa__20220118_dcp2_20220121_dcp12', 46),
    mksrc('datarepo-7357dda4', 'hca_prod_c31fa434c9ed4263a9b6d9ffb9d44005__20220118_dcp2_20220121_dcp12', 141),
    mksrc('datarepo-4322539b', 'hca_prod_c4077b3c5c984d26a614246d12c2e5d7__20220118_dcp2_20220121_dcp12', 218),
    mksrc('datarepo-c746ef64', 'hca_prod_c41dffbfad83447ca0e113e689d9b258__20220118_dcp2_20220121_dcp12', 56),
    mksrc('datarepo-072807b7', 'hca_prod_c5ca43aa3b2b42168eb3f57adcbc99a1__20220118_dcp2_20220121_dcp12', 194),
    mksrc('datarepo-62e87fe3', 'hca_prod_c5f4661568de4cf4bbc2a0ae10f08243__20220118_dcp2_20220121_dcp12', 1),
    mksrc('datarepo-c59a45c5', 'hca_prod_c6ad8f9bd26a4811b2ba93d487978446__20220118_dcp2_20220121_dcp12', 639),
    mksrc('datarepo-d0540cc6', 'hca_prod_c715cd2fdc7c44a69cd5b6a6d9f075ae__20220118_dcp2_20220121_dcp12', 9),
    mksrc('datarepo-2b95c100', 'hca_prod_c893cb575c9f4f26931221b85be84313__20220118_dcp2_20220121_dcp12', 20),
    mksrc('datarepo-dd3d8e06', 'hca_prod_cc95ff892e684a08a234480eca21ce79__20220118_dcp2_20220121_dcp12', 257),
    mksrc('datarepo-2a5a6085', 'hca_prod_ccd1f1ba74ce469b9fc9f6faea623358__20220118_dcp2_20220121_dcp12', 222),
    mksrc('datarepo-2b0f8836', 'hca_prod_ccef38d7aa9240109621c4c7b1182647__20220118_dcp2_20220121_dcp12', 1314),
    mksrc('datarepo-8d6a8dd5', 'hca_prod_cddab57b68684be4806f395ed9dd635a__20220118_dcp2_20220121_dcp12', 2546),
    mksrc('datarepo-202827af', 'hca_prod_ce33dde2382d448cb6acbfb424644f23__20220118_dcp2_20220121_dcp12', 189),
    mksrc('datarepo-cde2c4a4', 'hca_prod_ce7b12ba664f4f798fc73de6b1892183__20220119_dcp2_20220121_dcp12', 44),
    mksrc('datarepo-4f711011', 'hca_prod_d012d4768f8c4ff389d6ebbe22c1b5c1__20220119_dcp2_20220121_dcp12', 8),
    mksrc('datarepo-a718a79a', 'hca_prod_d2111fac3fc44f429b6d32cd6a828267__20220119_dcp2_20220121_dcp12', 735),
    mksrc('datarepo-12801bd1', 'hca_prod_d3446f0c30f34a12b7c36af877c7bb2d__20220119_dcp2_20220121_dcp12', 40),
    mksrc('datarepo-dac6d601', 'hca_prod_d3a4ceac4d66498497042570c0647a56__20220119_dcp2_20220121_dcp12', 14),
    mksrc('datarepo-1e99243c', 'hca_prod_d3ac7c1b53024804b611dad9f89c049d__20220119_dcp2_20220121_dcp12', 11),
    mksrc('datarepo-e73ca25f', 'hca_prod_d7845650f6b14b1cb2fec0795416ba7b__20220119_dcp2_20220121_dcp12', 28),
    mksrc('datarepo-7796a030', 'hca_prod_d7b7beae652b4fc09bf2bcda7c7115af__20220119_dcp2_20220121_dcp12', 9),
    mksrc('datarepo-438137ee', 'hca_prod_da2747fa292142e0afd439ef57b2b88b__20220119_dcp2_20220121_dcp12', 8),
    mksrc('datarepo-7f7fb2ac', 'hca_prod_daf9d9827ce643f6ab51272577290606__20220119_dcp2_20220121_dcp12', 42),
    mksrc('datarepo-404d888e', 'hca_prod_dbcd4b1d31bd4eb594e150e8706fa192__20220119_dcp2_20220121_dcp12', 84),
    mksrc('datarepo-ee4df1a4', 'hca_prod_dc1a41f69e0942a6959e3be23db6da56__20220119_dcp2_20220121_dcp12', 5),
    mksrc('datarepo-89f05580', 'hca_prod_dd7f24360c564709bd17e526bba4cc15__20220119_dcp2_20220121_dcp12', 37),
    mksrc('datarepo-d6e13093', 'hca_prod_df88f39f01a84b5b92f43177d6c0f242__20220119_dcp2_20220121_dcp12', 1),
    mksrc('datarepo-319b223a', 'hca_prod_e0009214c0a04a7b96e2d6a83e966ce0__20220119_dcp2_20220126_dcp12', 99841),
    mksrc('datarepo-cd37664c', 'hca_prod_e0c74c7a20a445059cf138dcdd23011b__20220119_dcp2_20220121_dcp12', 2),
    mksrc('datarepo-921c7df9', 'hca_prod_e526d91dcf3a44cb80c5fd7676b55a1d__20220119_dcp2_20220121_dcp12', 606),
    mksrc('datarepo-8d441277', 'hca_prod_e57dc176ab98446b90c289e0842152fd__20220119_dcp2_20220121_dcp12', 94),
    mksrc('datarepo-a2bba34f', 'hca_prod_e5d455791f5b48c3b568320d93e7ca72__20220119_dcp2_20220121_dcp12', 8),
    mksrc('datarepo-32d08de8', 'hca_prod_e77fed30959d4fadbc15a0a5a85c21d2__20220119_dcp2_20220121_dcp12', 333),
    mksrc('datarepo-de2f2f56', 'hca_prod_e8808cc84ca0409680f2bba73600cba6__20220118_dcp2_20220121_dcp12', 898),
    mksrc('datarepo-b49ee748', 'hca_prod_eaefa1b6dae14414953b17b0427d061e__20220118_dcp2_20220121_dcp12', 385),
    mksrc('datarepo-192f44d3', 'hca_prod_ede2e0b46652464fabbc0b2d964a25a0__20220118_dcp2_20220121_dcp12', 12),
    mksrc('datarepo-79a515aa', 'hca_prod_ef1d9888fa8647a4bb720ab0f20f7004__20220118_dcp2_20220121_dcp12', 57),
    mksrc('datarepo-8ddfa027', 'hca_prod_ef1e3497515e4bbe8d4c10161854b699__20220118_dcp2_20220121_dcp12', 6),
    mksrc('datarepo-902930ad', 'hca_prod_efea6426510a4b609a19277e52bfa815__20220118_dcp2_20220121_dcp12', 31),
    mksrc('datarepo-708835eb', 'hca_prod_f0f89c1474604bab9d4222228a91f185__20220119_dcp2_20220121_dcp12', 690),
    mksrc('datarepo-cf6bd64d', 'hca_prod_f2fe82f044544d84b416a885f3121e59__20220119_dcp2_20220121_dcp12', 156),
    mksrc('datarepo-44df5b5a', 'hca_prod_f48e7c39cc6740559d79bc437892840c__20220119_dcp2_20220121_dcp12', 14),
    mksrc('datarepo-eb93ad96', 'hca_prod_f6133d2a9f3d4ef99c19c23d6c7e6cc0__20220119_dcp2_20220121_dcp12', 384),
    mksrc('datarepo-e3c29b0f', 'hca_prod_f81efc039f564354aabb6ce819c3d414__20220119_dcp2_20220121_dcp12', 4),
    mksrc('datarepo-11942c76', 'hca_prod_f83165c5e2ea4d15a5cf33f3550bffde__20220119_dcp2_20220121_dcp12', 7665),
    mksrc('datarepo-c64a357d', 'hca_prod_f86f1ab41fbb4510ae353ffd752d4dfc__20220119_dcp2_20220121_dcp12', 20),
    mksrc('datarepo-4167b729', 'hca_prod_f8aa201c4ff145a4890e840d63459ca2__20220119_dcp2_20220121_dcp12', 384),
    mksrc('datarepo-590e9f21', 'hca_prod_faeedcb0e0464be7b1ad80a3eeabb066__20220119_dcp2_20220121_dcp12', 62),
])

dcp13_sources = dict(dcp12_sources, **mkdict([
    mksrc('datarepo-c8f9ec5d', 'hca_prod_03c6fce7789e4e78a27a664d562bb738__20220110_dcp2_20220214_dcp13', 1531),
    mksrc('datarepo-991fac12', 'hca_prod_05657a599f9d4bb9b77b24be13aa5cea__20220110_dcp2_20220214_dcp13', 186),
    mksrc('datarepo-b185fd85', 'hca_prod_05be4f374506429bb112506444507d62__20220107_dcp2_20220214_dcp13', 1545),
    mksrc('datarepo-30dc00be', 'hca_prod_065e6c13ad6b46a38075c3137eb03068__20220213_dcp2_20220214_dcp13', 120),
    mksrc('datarepo-0285bfe0', 'hca_prod_06c7dd8d6cc64b79b7958805c47d36e1__20220213_dcp2_20220214_dcp13', 24),
    mksrc('datarepo-bde655c7', 'hca_prod_102018327c7340339b653ef13d81656a__20220213_dcp2_20220214_dcp13', 1),
    mksrc('datarepo-003ece01', 'hca_prod_1ce3b3dc02f244a896dad6d107b27a76__20220107_dcp2_20220214_dcp13', 422),
    mksrc('datarepo-5a090360', 'hca_prod_1dddae6e375348afb20efa22abad125d__20220213_dcp2_20220214_dcp13', 39),
    mksrc('datarepo-a0659f9b', 'hca_prod_1eb69a39b5b241ecafae5fe37f272756__20220213_dcp2_20220214_dcp13', 386),
    mksrc('datarepo-cbdabbb4', 'hca_prod_23587fb31a4a4f58ad74cc9a4cb4c254__20220111_dcp2_20220214_dcp13', 1477),
    mksrc('datarepo-2ad5c040', 'hca_prod_2a72a4e566b2405abb7c1e463e8febb0__20220111_dcp2_20220214_dcp13', 2291),
    mksrc('datarepo-ca52c87a', 'hca_prod_2d8460958a334f3c97d4585bafac13b4__20220111_dcp2_20220214_dcp13', 3590),
    mksrc('datarepo-3da21f85', 'hca_prod_3a69470330844ece9abed935fd5f6748__20220112_dcp2_20220214_dcp13', 127),
    mksrc('datarepo-f84c69b4', 'hca_prod_520afa10f9d24e93ab7a26c4c863ce18__20220117_dcp2_20220214_dcp13', 650),
    mksrc('datarepo-fd9c289b', 'hca_prod_58028aa80ed249cab60f15e2ed5989d5__20220117_dcp2_20220214_dcp13', 4),
    mksrc('datarepo-afff5936', 'hca_prod_67a3de0945b949c3a068ff4665daa50e__20220117_dcp2_20220214_dcp13', 734),
    mksrc('datarepo-7e70b0df', 'hca_prod_6f89a7f38d4a4344aa4feccfe7e91076__20220213_dcp2_20220214_dcp13', 8),
    mksrc('datarepo-cafbc244', 'hca_prod_78b2406dbff246fc8b6120690e602227__20220117_dcp2_20220214_dcp13', 217),
    mksrc('datarepo-0558746b', 'hca_prod_78d7805bfdc8472b8058d92cf886f7a4__20220213_dcp2_20220214_dcp13', 36),
    mksrc('datarepo-fb7a9fe5', 'hca_prod_8559a8ed5d8c4fb6bde8ab639cebf03c__20220118_dcp2_20220214_dcp13', 380),
    mksrc('datarepo-5ee4d674', 'hca_prod_85a9263b088748edab1addfa773727b6__20220224_dcp2_20220224_dcp13', 8),
    mksrc('datarepo-604c0800', 'hca_prod_88ec040b87054f778f41f81e57632f7d__20220118_dcp2_20220214_dcp13', 2630),
    mksrc('datarepo-651b3c64', 'hca_prod_8c3c290ddfff4553886854ce45f4ba7f__20220118_dcp2_20220214_dcp13', 6643),
    mksrc('datarepo-9029753d', 'hca_prod_99101928d9b14aafb759e97958ac7403__20220118_dcp2_20220214_dcp13', 1191),
    mksrc('datarepo-0a0a2225', 'hca_prod_9c20a245f2c043ae82c92232ec6b594f__20220212_dcp2_20220214_dcp13', 101),
    mksrc('datarepo-9385cdd8', 'hca_prod_9d97f01f9313416e9b07560f048b2350__20220118_dcp2_20220121_dcp12', 43, pop),
    mksrc('datarepo-3dda61fd', 'hca_prod_ccd1f1ba74ce469b9fc9f6faea623358__20220118_dcp2_20220214_dcp13', 223),
    mksrc('datarepo-021d07c6', 'hca_prod_ccef38d7aa9240109621c4c7b1182647__20220118_dcp2_20220214_dcp13', 1319),
    mksrc('datarepo-8c5ae0d1', 'hca_prod_cd61771b661a4e19b2696e5d95350de6__20220213_dcp2_20220214_dcp13', 13),
    mksrc('datarepo-e69f2dd7', 'hca_prod_d6225aee8f0e4b20a20c682509a9ea14__20220213_dcp2_20220214_dcp13', 26),
    mksrc('datarepo-b11dcc58', 'hca_prod_d71c76d336704774a9cf034249d37c60__20220213_dcp2_20220214_dcp13', 1589),
    mksrc('datarepo-e251e383', 'hca_prod_dbd836cfbfc241f0983441cc6c0b235a__20220212_dcp2_20220214_dcp13', 9),
    mksrc('datarepo-ce17ac99', 'hca_prod_dd7ada843f144765b7ce9b64642bb3dc__20220212_dcp2_20220214_dcp13', 133),
    mksrc('datarepo-8e3d7fce', 'hca_prod_e8808cc84ca0409680f2bba73600cba6__20220118_dcp2_20220214_dcp13', 901),
    mksrc('datarepo-43f772c9', 'hca_prod_f6133d2a9f3d4ef99c19c23d6c7e6cc0__20220119_dcp2_20220214_dcp13', 385),
]))

dcp14_sources = dict(dcp13_sources, **mkdict([
    mksrc('datarepo-ef305f42', 'hca_prod_005d611a14d54fbf846e571a1f874f70__20220111_dcp2_20220307_dcp14', 7),
    mksrc('datarepo-4fb4619a', 'hca_prod_074a9f88729a455dbca50ce80edf0cea__20220107_dcp2_20220307_dcp14', 2),
    mksrc('datarepo-1dbff5cd', 'hca_prod_091cf39b01bc42e59437f419a66c8a45__20220107_dcp2_20220307_dcp14', 20),
    mksrc('datarepo-73b30762', 'hca_prod_116965f3f09447699d28ae675c1b569c__20220107_dcp2_20220307_dcp14', 8),
    mksrc('datarepo-ecd9f488', 'hca_prod_165dea71a95a44e188cdb2d9ad68bb1e__20220303_dcp2_20220307_dcp14', 1),
    mksrc('datarepo-c3ca85db', 'hca_prod_24d0dbbc54eb49048141934d26f1c936__20220303_dcp2_20220307_dcp14', 246),
    mksrc('datarepo-6eecb96e', 'hca_prod_2c041c26f75a495fab36a076f89d422a__20220303_dcp2_20220307_dcp14', 97),
    mksrc('datarepo-99fdfa87', 'hca_prod_3cdaf942f8ad42e8a77b4efedb9ea7b6__20220303_dcp2_20220307_dcp14', 10),
    mksrc('datarepo-cf90c331', 'hca_prod_403c3e7668144a2da5805dd5de38c7ff__20220113_dcp2_20220307_dcp14', 63),
    mksrc('datarepo-b9918259', 'hca_prod_4a95101c9ffc4f30a809f04518a23803__20220113_dcp2_20220307_dcp14', 37),
    mksrc('datarepo-77f534b9', 'hca_prod_4bec484dca7a47b48d488830e06ad6db__20220113_dcp2_20220307_dcp14', 14),
    mksrc('datarepo-b230b42b', 'hca_prod_4d6f6c962a8343d88fe10f53bffd4674__20220113_dcp2_20220307_dcp14', 12),
    mksrc('datarepo-b83d5d98', 'hca_prod_4e6f083b5b9a439398902a83da8188f1__20220113_dcp2_20220307_dcp14', 53),
    mksrc('datarepo-d7e92ae1', 'hca_prod_5116c0818be749c58ce073b887328aa9__20220117_dcp2_20220307_dcp14', 26),
    mksrc('datarepo-9e63ca34', 'hca_prod_53c53cd481274e12bc7f8fe1610a715c__20220117_dcp2_20220307_dcp14', 34),
    mksrc('datarepo-6b360d3f', 'hca_prod_5b5f05b72482468db76d8f68c04a7a47__20220117_dcp2_20220307_dcp14', 87),
    mksrc('datarepo-47534f24', 'hca_prod_6ac8e777f9a04288b5b0446e8eba3078__20220303_dcp2_20220307_dcp14', 51),
    mksrc('datarepo-aa6a9210', 'hca_prod_74b6d5693b1142efb6b1a0454522b4a0__20220117_dcp2_20220307_dcp14', 5460),
    mksrc('datarepo-7274c749', 'hca_prod_7b947aa243a74082afff222a3e3a4635__20220117_dcp2_20220307_dcp14', 16),
    mksrc('datarepo-06d0218d', 'hca_prod_8185730f411340d39cc3929271784c2b__20220117_dcp2_20220307_dcp14', 12),
    mksrc('datarepo-958f743f', 'hca_prod_91af6e2f65f244ec98e0ba4e98db22c8__20220303_dcp2_20220307_dcp14', 10),
    mksrc('datarepo-8ef24363', 'hca_prod_95f07e6e6a734e1ba880c83996b3aa5c__20220118_dcp2_20220307_dcp14', 40),
    mksrc('datarepo-bc66239d', 'hca_prod_abe1a013af7a45ed8c26f3793c24a1f4__20220118_dcp2_20220307_dcp14', 46),
    mksrc('datarepo-ccddf7b7', 'hca_prod_b963bd4b4bc14404842569d74bc636b8__20220118_dcp2_20220307_dcp14', 2),
    mksrc('datarepo-145862d0', 'hca_prod_c05184453b3b49c6b8fcc41daa4eacba__20220213_dcp2_20220307_dcp14', 6),
    mksrc('datarepo-1d4ac83f', 'hca_prod_c211fd49d9804ba18c6ac24254a3cb52__20220303_dcp2_20220307_dcp14', 120),
    mksrc('datarepo-a7ff96eb', 'hca_prod_c4077b3c5c984d26a614246d12c2e5d7__20220118_dcp2_20220307_dcp14', 218),
    mksrc('datarepo-ff4ee826', 'hca_prod_c6a50b2a3dfd4ca89b483e682f568a25__20220303_dcp2_20220307_dcp14', 13),
    mksrc('datarepo-15efafd9', 'hca_prod_cc95ff892e684a08a234480eca21ce79__20220118_dcp2_20220307_dcp14', 257),
    mksrc('datarepo-264555df', 'hca_prod_e5d455791f5b48c3b568320d93e7ca72__20220119_dcp2_20220307_dcp14', 15),
    mksrc('datarepo-9cbb67c6', 'hca_prod_f29b124a85974862ae98ff3a0fd9033e__20220303_dcp2_20220307_dcp14', 170),
    mksrc('datarepo-09a8dd1a', 'hca_prod_f83165c5e2ea4d15a5cf33f3550bffde__20220119_dcp2_20220307_dcp14', 7665),
]))

dcp15_sources = dict(dcp14_sources, **mkdict([
    mksrc('datarepo-bb0322f9', 'hca_prod_04ad400c58cb40a5bc2b2279e13a910b__20220114_dcp2_20220330_dcp15', 955),
    mksrc('datarepo-4c006992', 'hca_prod_0562d2ae0b8a459ebbc06357108e5da9__20220330_dcp2_20220330_dcp15', 17),
    mksrc('datarepo-625580ba', 'hca_prod_0777b9ef91f3468b9deadb477437aa1a__20220330_dcp2_20220330_dcp15', 62),
    mksrc('datarepo-c6460226', 'hca_prod_18d4aae283634e008eebb9e568402cf8__20220330_dcp2_20220330_dcp15', 9),
    mksrc('datarepo-9e1d30cd', 'hca_prod_1ce3b3dc02f244a896dad6d107b27a76__20220107_dcp2_20220330_dcp15', 422),
    mksrc('datarepo-426125f5', 'hca_prod_2b38025da5ea4c0fb22e367824bcaf4c__20220111_dcp2_20220331_dcp15', 44),
    mksrc('datarepo-67ebf8c0', 'hca_prod_40272c3b46974bd4ba3f82fa96b9bf71__20220303_dcp2_20220330_dcp15', 6),
    mksrc('datarepo-7e581d49', 'hca_prod_40ca2a03ec0f471fa834948199495fe7__20220330_dcp2_20220330_dcp15', 12),
    mksrc('datarepo-4b461192', 'hca_prod_45c2c853d06f4879957ef1366fb5d423__20220303_dcp2_20220330_dcp15', 90),
    mksrc('datarepo-b5a6fdd9', 'hca_prod_5116c0818be749c58ce073b887328aa9__20220117_dcp2_20220330_dcp15', 26),
    mksrc('datarepo-abf80711', 'hca_prod_65d7a1684d624bc083244e742aa62de6__20220330_dcp2_20220330_dcp15', 32),
    mksrc('datarepo-4a1d1031', 'hca_prod_6621c827b57a4268bc80df4049140193__20220330_dcp2_20220330_dcp15', 103),
    mksrc('datarepo-ecd5ed43', 'hca_prod_6ac8e777f9a04288b5b0446e8eba3078__20220303_dcp2_20220330_dcp15', 51),
    mksrc('datarepo-993d35db', 'hca_prod_6f89a7f38d4a4344aa4feccfe7e91076__20220213_dcp2_20220330_dcp15', 8),
    mksrc('datarepo-fb756d63', 'hca_prod_73769e0a5fcd41f4908341ae08bfa4c1__20220330_dcp2_20220330_dcp15', 29),
    mksrc('datarepo-b174a30e', 'hca_prod_77780d5603c0481faade2038490cef9f__20220330_dcp2_20220330_dcp15', 94),
    mksrc('datarepo-54af5ab6', 'hca_prod_91af6e2f65f244ec98e0ba4e98db22c8__20220303_dcp2_20220330_dcp15', 10),
    mksrc('datarepo-89b77174', 'hca_prod_957261f72bd64358a6ed24ee080d5cfc__20220330_dcp2_20220330_dcp15', 599),
    mksrc('datarepo-c95907eb', 'hca_prod_99101928d9b14aafb759e97958ac7403__20220118_dcp2_20220330_dcp15', 1191),
    mksrc('datarepo-a186fcb1', 'hca_prod_a2a2f324cf24409ea859deaee871269c__20220330_dcp2_20220330_dcp15', 17),
    mksrc('datarepo-b44b5550', 'hca_prod_a815c84b8999433f958e422c0720e00d__20220330_dcp2_20220330_dcp15', 39),
    mksrc('datarepo-89acf5db', 'hca_prod_aefb919243fc46d7a4c129597f7ef61b__20220330_dcp2_20220330_dcp15', 34),
    mksrc('datarepo-06565264', 'hca_prod_aff9c3cd6b844fc2abf2b9c0b3038277__20220330_dcp2_20220330_dcp15', 17),
    mksrc('datarepo-0bb76e5c', 'hca_prod_c1810dbc16d245c3b45e3e675f88d87b__20220118_dcp2_20220330_dcp15', 134),
    mksrc('datarepo-5d6926b7', 'hca_prod_c7c54245548b4d4fb15e0d7e238ae6c8__20220330_dcp2_20220330_dcp15', 35),
    mksrc('datarepo-46a00828', 'hca_prod_dc1a41f69e0942a6959e3be23db6da56__20220119_dcp2_20220330_dcp15', 10),
    mksrc('datarepo-e9f2b830', 'hca_prod_e255b1c611434fa683a8528f15b41038__20220330_dcp2_20220330_dcp15', 106),
    mksrc('datarepo-c93c8ea6', 'hca_prod_f2fe82f044544d84b416a885f3121e59__20220119_dcp2_20220330_dcp15', 156),
    mksrc('datarepo-d5d5cacf', 'hca_prod_fa3f460f4fb94cedb5488ba6a8ecae3f__20220330_dcp2_20220330_dcp15', 247),
    mksrc('datarepo-b60aabf3', 'hca_prod_fde199d2a8414ed1aa65b9e0af8969b1__20220330_dcp2_20220330_dcp15', 185),
]))

lungmap_sources = mkdict([
    mksrc('datarepo-32f75497', 'lungmap_prod_00f056f273ff43ac97ff69ca10e38c89__20220308_20220308', 1),
    mksrc('datarepo-7066459d', 'lungmap_prod_1bdcecde16be420888f478cd2133d11d__20220308_20220308', 1),
    mksrc('datarepo-cfaedae8', 'lungmap_prod_2620497955a349b28d2b53e0bdfcb176__20220308_20220308', 1),
])

lm2_sources = dict(lungmap_sources, **mkdict([
    mksrc('datarepo-5eee9956', 'lungmap_prod_00f056f273ff43ac97ff69ca10e38c89__20220308_20220314_lm2', 1),
    mksrc('datarepo-73453de6', 'lungmap_prod_20037472ea1d4ddb9cd356a11a6f0f76__20220307_20220310_lm2', 1),
    mksrc('datarepo-df6004c2', 'lungmap_prod_f899709cae2c4bb988f0131142e6c7ec__20220310_20220311_lm2', 1),
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
        # Set variables for the `prod` (short for production) deployment here.
        #
        # Only modify this file if you intend to commit those changes. To change the
        # environment with a setting that's specific to you AND the deployment, create
        # a environment.local.py right next to this file and make your changes there.
        # Settings applicable to all environments but specific to you go into
        # environment.local.py at the project root.

        'AZUL_VERSIONED_BUCKET': 'edu-ucsc-gi-azul-dcp2-prod-config.{AWS_DEFAULT_REGION}',
        'AZUL_DOMAIN_NAME': 'azul.data.humancellatlas.org',

        'AZUL_DEPLOYMENT_STAGE': 'prod',

        'AZUL_S3_BUCKET': 'edu-ucsc-gi-azul-dcp2-prod-storage-{AZUL_DEPLOYMENT_STAGE}',

        'AZUL_CATALOGS': json.dumps({
            f'{catalog}{suffix}': dict(atlas=atlas,
                                       internal=internal,
                                       plugins=dict(metadata=dict(name='hca'),
                                                    repository=dict(name='tdr')),
                                       sources=list(filter(None, sources.values())))
            for atlas, catalog, sources in [
                ('hca', 'dcp15', dcp15_sources),
                ('hca', 'dcp1', dcp1_sources),
                ('lungmap', 'lm2', lm2_sources)
            ] for suffix, internal in [
                ('', False),
                ('-it', True)
            ]
        }),

        'AZUL_TDR_SOURCE_LOCATION': 'US',
        'AZUL_TDR_SERVICE_URL': 'https://data.terra.bio',
        'AZUL_SAM_SERVICE_URL': 'https://sam.dsde-prod.broadinstitute.org',

        'AZUL_URL_REDIRECT_BASE_DOMAIN_NAME': 'azul.data.humancellatlas.org',
        'AZUL_URL_REDIRECT_FULL_DOMAIN_NAME': 'url.{AZUL_URL_REDIRECT_BASE_DOMAIN_NAME}',

        'AZUL_ENABLE_MONITORING': '1',

        # $0.382/h × 4 × 24h/d × 30d/mo = $1100.16/mo
        'AZUL_ES_INSTANCE_TYPE': 'r6gd.xlarge.elasticsearch',
        'AZUL_ES_INSTANCE_COUNT': '4',

        'AZUL_DEBUG': '1',

        'AZUL_OWNER': 'hannes@ucsc.edu',

        'AZUL_AWS_ACCOUNT_ID': '542754589326',
        'AWS_DEFAULT_REGION': 'us-east-1',

        'GOOGLE_PROJECT': 'platform-hca-prod',

        'AZUL_CONTRIBUTION_CONCURRENCY': '300/64',
    }
