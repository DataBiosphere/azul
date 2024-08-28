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


def mksrc(google_project,
          snapshot,
          subgraphs,
          flags: int = 0
          ) -> tuple[str, str | None]:
    project = '_'.join(snapshot.split('_')[1:-3])
    assert flags <= ma | pop
    source = None if flags & pop else ':'.join([
        'tdr',
        'bigquery',
        'gcp',
        google_project,
        snapshot,
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

anvil1_sources = mkdict(anvil_sources, 63, mkdelta([
    mksrc('datarepo-d53aa186', 'ANVIL_CMG_BROAD_BRAIN_ENGLE_WES_20221102_ANV5_202304241525', 473),
    mksrc('datarepo-69b2535a', 'ANVIL_CMG_BROAD_BRAIN_SHERR_WGS_20221102_ANV5_202304241530', 3),
    mksrc('datarepo-490be510', 'ANVIL_CMG_BROAD_ORPHAN_SCOTT_WGS_20221102_ANV5_202304241538', 15),
    mksrc('datarepo-3b33c41b', 'ANVIL_CMG_Broad_Blood_Gazda_WES_20221117_ANV5_202304241459', 612),
    mksrc('datarepo-96df3cea', 'ANVIL_CMG_Broad_Blood_Sankaran_WES_20221117_ANV5_202304241501', 1141),
    mksrc('datarepo-179ee079', 'ANVIL_CMG_Broad_Blood_Sankaran_WGS_20221117_ANV5_202304241503', 96),
    mksrc('datarepo-3dd4d039', 'ANVIL_CMG_Broad_Brain_Gleeson_WES_20221117_ANV5_202304241517', 1180),
    mksrc('datarepo-c361373f', 'ANVIL_CMG_Broad_Brain_Muntoni_WES_20221102_ANV5_202304241527', 39),
    mksrc('datarepo-12ac342c', 'ANVIL_CMG_Broad_Brain_NeuroDev_WES_20221102_ANV5_202304241529', 292),
    mksrc('datarepo-d7bfafc6', 'ANVIL_CMG_Broad_Brain_Thaker_WES_20221102_ANV5_202304241531', 46),
    mksrc('datarepo-29812b42', 'ANVIL_CMG_Broad_Eye_Pierce_WES_20221205_ANV5_202304242250', 2150),
    mksrc('datarepo-48134558', 'ANVIL_CMG_Broad_Eye_Pierce_WGS_20221117_ANV5_202304241507', 35),
    mksrc('datarepo-36ebaa12', 'ANVIL_CMG_Broad_Heart_PCGC_Tristani_WGS_20221025_ANV5_202304211840', 214),
    mksrc('datarepo-f9826139', 'ANVIL_CMG_Broad_Heart_Seidman_WES_20221117_ANV5_202304241504', 133),
    mksrc('datarepo-85952af8', 'ANVIL_CMG_Broad_Kidney_Hildebrandt_WES_20230525_ANV5_202305251733', 3544),
    mksrc('datarepo-ee4ae9a1', 'ANVIL_CMG_Broad_Kidney_Hildebrandt_WGS_20221025_ANV5_202304211844', 27),
    mksrc('datarepo-cf168274', 'ANVIL_CMG_Broad_Kidney_Pollak_WES_20221025_ANV5_202304211846', 147),
    mksrc('datarepo-4d47ba2c', 'ANVIL_CMG_Broad_Muscle_Beggs_WGS_20221102_ANV5_202304241533', 141),
    mksrc('datarepo-82d1271a', 'ANVIL_CMG_Broad_Muscle_Bonnemann_WES_20221117_ANV5_202304241509', 305),
    mksrc('datarepo-6be3fb25', 'ANVIL_CMG_Broad_Muscle_Bonnemann_WGS_20221117_ANV5_202304241510', 152),
    mksrc('datarepo-b168eb10', 'ANVIL_CMG_Broad_Muscle_KNC_WES_20221116_ANV5_202304242219', 169),
    mksrc('datarepo-372244aa', 'ANVIL_CMG_Broad_Muscle_KNC_WGS_20221117_ANV5_202304242221', 16),
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
    mksrc('datarepo-ab5c3fa5', 'ANVIL_CMG_YALE_DS_RARED_20221020_ANV5_202304211812', 173),
    mksrc('datarepo-d51578f4', 'ANVIL_CMG_Yale_GRU_20221020_ANV5_202304211517', 2196),
    mksrc('datarepo-bcedc554', 'ANVIL_CMG_Yale_HMB_20221020_ANV5_202304211813', 125),
    mksrc('datarepo-f485fa3e', 'ANVIL_CMG_Yale_HMB_GSO_20221020_ANV5_202304211519', 4264),
    mksrc('datarepo-45487b69', 'ANVIL_GTEx_Somatic_WGS_20230331_ANV5_202304211636', 707),
    mksrc('datarepo-5ebc368c', 'ANVIL_GTEx_V7_hg19_20221128_ANV5_202304211804', 15974),
    mksrc('datarepo-864913f2', 'ANVIL_GTEx_V9_hg38_20221128_ANV5_202304211853', 8298),
    mksrc('datarepo-b093b69d', 'ANVIL_GTEx_public_data_20221115_ANV5_202304211659', 81),
    mksrc('datarepo-d948d21a', 'ANVIL_cmg_broad_brain_engle_wgs_20221202_ANV5_202304271345', 95),
    mksrc('datarepo-1cb73890', 'ANVIL_cmg_broad_heart_ware_wes_20221215_ANV5_202304242145', 40),
]))

anvil2_sources = mkdict(anvil1_sources, 104, mkdelta([
    # @formatter:off
    mksrc('datarepo-36124817', 'ANVIL_African_American_Seq_HGV_20230727_ANV5_202308291753', 4656),
    mksrc('datarepo-d795027d', 'ANVIL_CCDG_Broad_CVD_AF_VAFAR_Arrays_20221020_ANV5_202304211823', 1390),
    mksrc('datarepo-642829f3', 'ANVIL_CCDG_Broad_CVD_AF_VAFAR_WES_20221024_ANV5_202304211826', 1548),
    mksrc('datarepo-08216a2c', 'ANVIL_CCDG_Broad_CVD_AFib_Vanderbilt_Ablation_WGS_20221020_ANV5_202304211819', 2),
    mksrc('datarepo-74975e89', 'ANVIL_CCDG_Broad_NP_Epilepsy_JPNFKA_GRU_WES_20221220_ANV5_202304271548', 372),
    mksrc('datarepo-ad61c47e', 'ANVIL_CCDG_NHGRI_Broad_ASD_Daly_phs000298_WES_vcf_20230403_ANV5_202304271610', 3922),
    mksrc('datarepo-5e719362', 'ANVIL_CCDG_NYGC_AI_Asthma_Gala2_WGS_20230605_ANV5_202306131248', 1310),
    mksrc('datarepo-2734a0e4', 'ANVIL_CCDG_NYGC_NP_Alz_EFIGA_WGS_20230605_ANV5_202306141705', 3750),
    mksrc('datarepo-710fc60d', 'ANVIL_CCDG_NYGC_NP_Alz_LOAD_WGS_20230605_ANV5_202306131256', 1049),
    mksrc('datarepo-9626b3eb', 'ANVIL_CCDG_NYGC_NP_Alz_WHICAP_WGS_20230605_ANV5_202306131303', 148),
    mksrc('datarepo-25ec7b57', 'ANVIL_CCDG_WASHU_PAGE_20221220_ANV5_202304271544', 690),
    mksrc('datarepo-6d8536f4', 'ANVIL_CMH_GAFK_GS_linked_read_20221107_ANV5_202304211527', 626),
    mksrc('datarepo-482ab960', 'ANVIL_CMH_GAFK_GS_long_read_20221109_ANV5_202304211529', 777),
    mksrc('datarepo-8745e97d', 'ANVIL_CMH_GAFK_scRNA_20221107_ANV5_202304211533', 198),
    mksrc('datarepo-1c89dcac', 'ANVIL_CSER_CHARM_GRU_20221208_ANV5_202304271348', 2392),
    mksrc('datarepo-12d56848', 'ANVIL_CSER_NCGENES2_GRU_20221208_ANV5_202304271349', 104),
    mksrc('datarepo-8a4d67ef', 'ANVIL_CSER_SouthSeq_GRU_20221208_ANV5_202304271351', 800),
    mksrc('datarepo-f622180d', 'ANVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_CIRM_GRU_VillageData_20230109_ANV5_202304242045', 71),  # noqa E501
    mksrc('datarepo-732d1a55', 'ANVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_CIRM_GRU_WGS_20230109_ANV5_202304242048', 445),  # noqa E501
    mksrc('datarepo-90bab913', 'ANVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_Finkel_SMA_DS_WGS_20230109_ANV5_202304242043', 3),  # noqa E501
    mksrc('datarepo-e4eb7641', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Braff_DS_WGS_20221115_ANV5_202304242052', 864),
    mksrc('datarepo-f9aef3dc', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Escamilla_DS_WGS_20221103_ANV5_202304242049', 85),
    mksrc('datarepo-aca6a582', 'ANVIL_NIMH_CIRM_FCDI_ConvergentNeuro_McCarroll_Eggan_GRU_Arrays_20230109_ANV5_202304242046', 6510),  # noqa E501
    mksrc('datarepo-06abb598', 'ANVIL_PAGE_BioMe_GRU_WGS_20221128_ANV5_202304211817', 308),
    mksrc('datarepo-7c4410ed', 'ANVIL_PAGE_MEC_GRU_WGS_20230131_ANV5_202304211721', 70),
    mksrc('datarepo-84d2e3b1', 'ANVIL_PAGE_Stanford_Global_Reference_Panel_GRU_WGS_20221128_ANV5_202304211827', 78),
    mksrc('datarepo-ffbc38fd', 'ANVIL_PAGE_WHI_HMB_IRB_WGS_20221019_ANV5_202304211722', 235),
    mksrc('datarepo-b1f3e0d1', 'ANVIL_ccdg_asc_ndd_daly_talkowski_cdcseed_asd_gsa_md_20221024_ANV5_202304211749', 2888),
    mksrc('datarepo-11330a21', 'ANVIL_ccdg_asc_ndd_daly_talkowski_schloesser_asd_gsa_md_20221025_ANV5_202304211759', 158),  # noqa E501
    mksrc('datarepo-86a1dbf3', 'ANVIL_ccdg_broad_ai_ibd_daly_bernstein_gsa_20221025_ANV5_202304241921', 543),
    mksrc('datarepo-833ff0a3', 'ANVIL_eMERGE_GRU_IRB_NPU_eMERGEseq_20230130_ANV5_202304271614', 2569),
    mksrc('datarepo-baf040af', 'ANVIL_eMERGE_GRU_IRB_PUB_NPU_eMERGEseq_20230130_ANV5_202304271616', 3017),
    mksrc('datarepo-270b3b62', 'ANVIL_eMERGE_GRU_IRB_eMERGEseq_20230130_ANV5_202304271613', 2432),
    mksrc('datarepo-c13efbe9', 'ANVIL_eMERGE_GRU_NPU_eMERGEseq_20230130_ANV5_202304271617', 5907),
    mksrc('datarepo-34f8138d', 'ANVIL_eMERGE_GRU_eMERGEseq_20230130_ANV5_202304271612', 2971),
    mksrc('datarepo-90b7b6e8', 'ANVIL_eMERGE_HMB_GSO_eMERGEseq_20230130_ANV5_202304271621', 2491),
    mksrc('datarepo-6e6dca92', 'ANVIL_eMERGE_HMB_IRB_PUB_eMERGEseq_20230130_ANV5_202304271622', 478),
    mksrc('datarepo-1ddf2a8e', 'ANVIL_eMERGE_HMB_NPU_eMERGEseq_20230130_ANV5_202304271624', 2488),
    mksrc('datarepo-dba97a65', 'ANVIL_eMERGE_HMB_eMERGEseq_20230130_ANV5_202304271619', 2486),
    mksrc('datarepo-51aa9a22', 'ANVIL_eMERGE_PGRNseq_20230118_ANV5_202304241853', 9027),
    mksrc('datarepo-ce8c469f', 'ANVIL_eMERGE_PRS_Arrays_20221220_ANV5_202304271346', 7296)
    # @formatter:on
]))

anvil3_sources = mkdict(anvil2_sources, 151, mkdelta([
    # @formatter:off
    mksrc('datarepo-9a74aed3', 'ANVIL_CCDG_Baylor_CVD_ARIC_20231008_ANV5_202310091900', 10012),
    mksrc('datarepo-0768a322', 'ANVIL_CCDG_Broad_CVD_AF_Ellinor_MGH_Arrays_20221024_ANV5_202304211831', 387),
    mksrc('datarepo-2b135baf', 'ANVIL_CCDG_Broad_CVD_AFib_MGH_WGS_20221024_ANV5_202304211829', 105),
    mksrc('datarepo-96b594f9', 'ANVIL_CCDG_Broad_CVD_EOCAD_TaiChi_WGS_20221026_ANV5_202310101655', 912),
    mksrc('datarepo-318ae48e', 'ANVIL_CCDG_Broad_CVD_Stroke_BRAVE_WGS_20221107_ANV5_202304241543', 500),
    mksrc('datarepo-7ea7a6e9', 'ANVIL_CCDG_Broad_MI_BRAVE_GRU_WES_20221107_ANV5_202304241545', 1500),
    mksrc('datarepo-2339e241', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EPIL_BA_MDS_WES_20221101_ANV5_202304241613', 47),
    mksrc('datarepo-cd6cee03', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EPI_BA_ID_MDS_WES_20221101_ANV5_202304241612', 136),
    mksrc('datarepo-da88c3ce', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EP_BA_CN_ID_MDS_WES_20221101_ANV5_202304241657', 5399),  # noqa E501
    mksrc('datarepo-2b361bda', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSRMB_DS_EAED_MDS_WES_20221026_ANV5_202304241549', 393),
    mksrc('datarepo-6eeff3fc', 'ANVIL_CCDG_Broad_NP_Epilepsy_BELATW_GRU_WES_20221108_ANV5_202304241701', 113),
    mksrc('datarepo-21923ed0', 'ANVIL_CCDG_Broad_NP_Epilepsy_BELULB_DS_EP_NPU_WES_20221027_ANV5_202304241556', 419),
    mksrc('datarepo-5b10132b', 'ANVIL_CCDG_Broad_NP_Epilepsy_CANUTN_DS_EP_WES_20230328_ANV5_202304241552', 149),
    mksrc('datarepo-d2d5ba15', 'ANVIL_CCDG_Broad_NP_Epilepsy_CZEMTH_GRU_WES_20221108_ANV5_202304241702', 18),
    mksrc('datarepo-fc0a35a8', 'ANVIL_CCDG_Broad_NP_Epilepsy_DEUULG_GRU_WES_20221108_ANV5_202304241704', 94),
    mksrc('datarepo-f14cd6d7', 'ANVIL_CCDG_Broad_NP_Epilepsy_FINKPH_EPIL_CO_MORBIDI_MDS_WES_20230328_ANV5_202304241659', 914),  # noqa E501
    mksrc('datarepo-3832cf81', 'ANVIL_CCDG_Broad_NP_Epilepsy_GBRSWU_CARDI_NEURO_WES_20221026_ANV5_202304241548', 319),
    mksrc('datarepo-098aadb0', 'ANVIL_CCDG_Broad_NP_Epilepsy_GBRUCL_DS_EARET_MDS_WES_20221026_ANV5_202304241551', 686),
    mksrc('datarepo-d9ea4f23', 'ANVIL_CCDG_Broad_NP_Epilepsy_GBRUNL_EP_ETIOLOGY_MDS_WES_20221027_ANV5_202304241554', 460),  # noqa E501
    mksrc('datarepo-0c9ab563', 'ANVIL_CCDG_Broad_NP_Epilepsy_GBRUNL_GRU_WES_20221108_ANV5_202304241705', 57),
    mksrc('datarepo-a383d752', 'ANVIL_CCDG_Broad_NP_Epilepsy_ITAIGI_GRU_WES_20221108_ANV5_202304241707', 1163),
    mksrc('datarepo-03b52641', 'ANVIL_CCDG_Broad_NP_Epilepsy_ITAUBG_DS_EPI_NPU_MDS_WES_20221027_ANV5_202304241601', 619),  # noqa E501
    mksrc('datarepo-2e9ab296', 'ANVIL_CCDG_Broad_NP_Epilepsy_ITAUMC_DS_NEURO_MDS_WES_20221108_ANV5_202304241605', 418),
    mksrc('datarepo-89162c54', 'ANVIL_CCDG_Broad_NP_Epilepsy_JPNRKI_DS_NPD_IRB_NPU_WES_20221027_ANV5_202304241609', 100),  # noqa E501
    mksrc('datarepo-fd5cd738', 'ANVIL_CCDG_Broad_NP_Epilepsy_NZLUTO_EPIL_BC_ID_MDS_WES_20230328_ANV5_202304241602', 275),  # noqa E501
    mksrc('datarepo-d987821a', 'ANVIL_CCDG_Broad_NP_Epilepsy_TURBZU_GRU_WES_20221108_ANV5_202304241709', 214),
    mksrc('datarepo-b93e1cfa', 'ANVIL_CCDG_Broad_NP_Epilepsy_TURIBU_DS_NEURO_AD_NPU_WES_20221027_ANV5_202304241604', 169),  # noqa E501
    mksrc('datarepo-2e9630dd', 'ANVIL_CCDG_Broad_NP_Epilepsy_USABCH_EPI_MUL_CON_MDS_WES_20221027_ANV5_202304241559', 330),  # noqa E501
    mksrc('datarepo-ee58a7a9', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACHP_GRU_WES_20230612_ANV5_202306131343', 3754),
    mksrc('datarepo-ff5356bb', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACRW_DS_EP_MDS_WES_20221027_ANV5_202304241555', 328),
    mksrc('datarepo-2262daa7', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACRW_DS_SEIZD_WES_20221027_ANV5_202304241610', 154),
    mksrc('datarepo-2a947c33', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACRW_EPI_ASZ_MED_MDS_WES_20221027_ANV5_202304241558', 39),  # noqa E501
    mksrc('datarepo-5b3c42e1', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAEGP_GRU_WES_20221110_ANV5_202304241713', 129),
    mksrc('datarepo-91b4b33c', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAHEP_GRU_WES_20230328_ANV5_202306211900', 328),
    mksrc('datarepo-e4fe111a', 'ANVIL_CCDG_Broad_NP_Epilepsy_USANCH_DS_NEURO_MDS_WES_20221108_ANV5_202304241607', 313),
    mksrc('datarepo-8b120833', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAUPN_Marsh_GRU_WES_20230328_ANV5_202304241716', 355),
    mksrc('datarepo-f051499d', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAUPN_Rader_GRU_WES_20230328_ANV5_202304241720', 832),
    mksrc('datarepo-fd49a493', 'ANVIL_CCDG_WashU_CVD_EOCAD_WashU_CAD_DS_WGS_20230525_ANV5_202306211841', 80),
    mksrc('datarepo-076da44b', 'ANVIL_CCDG_WashU_CVD_EOCAD_WashU_CAD_GRU_IRB_WGS_20230525_ANV5_202306211847', 265),
    mksrc('datarepo-7e03b5fd', 'ANVIL_CMG_Broad_Brain_Walsh_WES_20230605_ANV5_202310101734', 2778),
    mksrc('datarepo-c43e7400', 'ANVIL_CMG_Broad_Muscle_Kang_WES_20230525_ANV5_202310101649', 121),
    mksrc('datarepo-14f5afa3', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Braff_DS_10XLRGenomes_20221115_ANV5_202310101713', 561),  # noqa E501
    mksrc('datarepo-94091a22', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Pato_GRU_10XLRGenomes_20230331_ANV5_202310101715', 368),  # noqa E501
    mksrc('datarepo-55b75002', 'ANVIL_PAGE_SoL_HMB_WGS_20221220_ANV5_202310061302', 234),
    mksrc('datarepo-02ad84ea', 'ANVIL_T2T_20230714_ANV5_202310101616', 261317),
    mksrc('datarepo-08cd15a2', 'ANVIL_ccdg_washu_ai_t1d_t1dgc_wgs_20221031_ANV5_202304211552', 3397),
    mksrc('datarepo-e3065356', 'ANVIL_ccdg_washu_cvd_eocad_biome_wgs_20221024_ANV5_202304211601', 648),
    # @formatter:on
]))

anvil4_sources = mkdict(anvil3_sources, 200, mkdelta([
    # @formatter:off
    mksrc('datarepo-1a86e7ca', 'ANVIL_CCDG_Baylor_CVD_AFib_Groningen_WGS_20221122_ANV5_202304242224', 639),
    mksrc('datarepo-92716a90', 'ANVIL_CCDG_Baylor_CVD_AFib_VAFAR_HMB_IRB_WGS_20221020_ANV5_202304211525', 253),
    mksrc('datarepo-77445496', 'ANVIL_CCDG_Baylor_CVD_EOCAD_BioMe_WGS_20221122_ANV5_202304242226', 1201),
    mksrc('datarepo-1b0d6b90', 'ANVIL_CCDG_Baylor_CVD_HHRC_Brownsville_GRU_WGS_20221122_ANV5_202304242228', 276),
    mksrc('datarepo-373b7918', 'ANVIL_CCDG_Baylor_CVD_HemStroke_BNI_HMB_WGS_20221215_ANV5_202304242306', 160),
    mksrc('datarepo-efc3e806', 'ANVIL_CCDG_Baylor_CVD_HemStroke_Duke_DS_WGS_20221117_ANV5_202304242122', 60),
    mksrc('datarepo-1044f96d', 'ANVIL_CCDG_Baylor_CVD_HemStroke_ERICH_WGS_20221207_ANV5_202304271256', 2558),
    mksrc('datarepo-f23a6ec8', 'ANVIL_CCDG_Baylor_CVD_HemStroke_GERFHS_HMB_WGS_20221215_ANV5_202304242307', 412),
    mksrc('datarepo-de34ca6e', 'ANVIL_CCDG_Baylor_CVD_HemStroke_Regards_DS_WGS_20221117_ANV5_202304242123', 121),
    mksrc('datarepo-d9c6f406', 'ANVIL_CCDG_Baylor_CVD_HemStroke_Yale_HMB_WGS_20221215_ANV5_202304242309', 185),
    mksrc('datarepo-56883e56', 'ANVIL_CCDG_Baylor_CVD_Oregon_SUDS_GRU_WGS_20221215_ANV5_202304242302', 2216),
    mksrc('datarepo-7f3ba7ec', 'ANVIL_CCDG_Baylor_CVD_TexGen_DS_WGS_20221117_ANV5_202304242125', 6461),
    mksrc('datarepo-da965e26', 'ANVIL_CCDG_Baylor_CVD_Ventura_Presto_GRU_IRB_WGS_20221117_ANV5_202304242127', 584),
    mksrc('datarepo-906bf803', 'ANVIL_CCDG_Broad_AI_IBD_Brant_DS_IBD_WGS_20221110_ANV5_202304241911', 575),
    mksrc('datarepo-343ca1c3', 'ANVIL_CCDG_Broad_AI_IBD_Brant_HMB_WGS_20221110_ANV5_202304241912', 907),
    mksrc('datarepo-80a63603', 'ANVIL_CCDG_Broad_AI_IBD_Cho_WGS_20230313_ANV5_202304241903', 352),
    mksrc('datarepo-a98e7a43', 'ANVIL_CCDG_Broad_AI_IBD_Kugathasan_WGS_20221110_ANV5_202304241906', 1406),
    mksrc('datarepo-381bc957', 'ANVIL_CCDG_Broad_AI_IBD_McCauley_WGS_20221110_ANV5_202304241914', 1256),
    mksrc('datarepo-6a10165d', 'ANVIL_CCDG_Broad_AI_IBD_McGovern_WGS_20221110_ANV5_202304241907', 11273),
    mksrc('datarepo-a2743c82', 'ANVIL_CCDG_Broad_AI_IBD_Newberry_WGS_20221025_ANV5_202304241901', 135),
    mksrc('datarepo-ed109b2f', 'ANVIL_CCDG_Broad_CVD_AF_BioVU_HMB_GSO_Arrays_20230612_ANV5_202306131350', 9),
    mksrc('datarepo-3d8b62d7', 'ANVIL_CCDG_Broad_CVD_AF_BioVU_HMB_GSO_WES_20221025_ANV5_202304241856', 5039),
    mksrc('datarepo-450ba911', 'ANVIL_CCDG_Broad_CVD_AF_ENGAGE_DS_WES_20230418_ANV5_202304210808', 13621),
    mksrc('datarepo-dfabf632', 'ANVIL_CCDG_Broad_CVD_AF_Ellinor_MGH_WES_20221117_ANV5_202304271354', 499),
    mksrc('datarepo-485eb707', 'ANVIL_CCDG_Broad_CVD_AF_Figtree_BioHeart_Arrays_20230128_ANV5_202304271554', 935),
    mksrc('datarepo-58dffe5a', 'ANVIL_CCDG_Broad_CVD_AF_GAPP_DS_MDS_Arrays_20221103_ANV5_202304242105', 2087),
    mksrc('datarepo-cf7f2c0c', 'ANVIL_CCDG_Broad_CVD_AF_GAPP_DS_MDS_WES_20221103_ANV5_202304242107', 2154),
    mksrc('datarepo-f896734e', 'ANVIL_CCDG_Broad_CVD_AF_Marcus_UCSF_Arrays_20221102_ANV5_202304242039', 160),
    mksrc('datarepo-40c2f4f4', 'ANVIL_CCDG_Broad_CVD_AF_Marcus_UCSF_WES_20221222_ANV5_202304242040', 599),
    mksrc('datarepo-67117555', 'ANVIL_CCDG_Broad_CVD_AF_Rienstra_WES_20221222_ANV5_202304242035', 2097),
    mksrc('datarepo-c45dd622', 'ANVIL_CCDG_Broad_CVD_AF_Swiss_Cases_DS_MDS_Arrays_20221103_ANV5_202304242110', 4467),
    mksrc('datarepo-b12d2e52', 'ANVIL_CCDG_Broad_CVD_AF_Swiss_Cases_DS_MDS_WES_20230118_ANV5_202304242112', 4550),
    mksrc('datarepo-43f6230a', 'ANVIL_CCDG_Broad_CVD_AFib_AFLMU_WGS_20231008_ANV5_202310091911', 386),
    mksrc('datarepo-de64d25a', 'ANVIL_CCDG_Broad_CVD_AFib_UCSF_WGS_20221222_ANV5_202304242037', 112),
    mksrc('datarepo-e25350dd', 'ANVIL_CCDG_Broad_CVD_EOCAD_PartnersBiobank_HMB_Arrays_20230517_ANV5_202310101704', 46570),  # noqa E501
    mksrc('datarepo-9921a6fa', 'ANVIL_CCDG_Broad_CVD_EOCAD_PartnersBiobank_HMB_WES_20230621_ANV5_202306211933', 17479),
    mksrc('datarepo-383d9d9b', 'ANVIL_CCDG_Broad_CVD_PROMIS_GRU_WES_20230418_ANV5_202306211912', 20557),
    mksrc('datarepo-5df71da4', 'ANVIL_CCDG_Broad_MI_InStem_WES_20221122_ANV5_202304242236', 1452),
    mksrc('datarepo-1793828c', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSALF_HMB_IRB_GSRS_WES_20230324_ANV5_202304241752', 18),
    mksrc('datarepo-d44547dc', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSALF_HMB_IRB_WES_20230128_ANV5_202304271556', 19),
    mksrc('datarepo-70c803d7', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EPIL_BA_MDS_GSA_MD_20221117_ANV5_202304271400', 22),
    mksrc('datarepo-f5a4a895', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EPI_BA_ID_MDS_GSA_MD_20221117_ANV5_202304271358', 37),  # noqa E501
    mksrc('datarepo-b8b8ba44', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EP_BA_CN_ID_MDS_GSA_MD_20221117_ANV5_202304271356', 1530),  # noqa E501
    mksrc('datarepo-0b0ca621', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSRMB_DS_EAED_IRB_WES_20230621_ANV5_202306211945', 216),
    mksrc('datarepo-f85048a3', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSRMB_DS_EAED_MDS_GSA_MD_20221117_ANV5_202304271401', 398),  # noqa E501
    mksrc('datarepo-68037179', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUTMUV_DS_NS_ADLT_WES_20230128_ANV5_202304271559', 14),
    mksrc('datarepo-025215fc', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUTMUV_DS_NS_WES_20230314_ANV5_202304271601', 107),
    mksrc('datarepo-92905a2b', 'ANVIL_CCDG_Broad_NP_Epilepsy_BELATW_GRU_GSA_MD_20221117_ANV5_202304271403', 239),
    mksrc('datarepo-3f3ad5c7', 'ANVIL_CCDG_Broad_NP_Epilepsy_BELULB_DS_EP_NPU_GSA_MD_20230118_ANV5_202304271404', 430)
    # @formatter:on
]))

anvil5_sources = mkdict(anvil4_sources, 262, mkdelta([
    # @formatter:off
    mksrc('datarepo-3c30a9a2', 'ANVIL_1000G_high_coverage_2019_20230517_ANV5_202403030329', 6804),
    mksrc('datarepo-adf70694', 'ANVIL_ALS_FTD_ALS_AssociatedGenes_GRU_v1_20231221_ANV5_202401112025', 11853),
    mksrc('datarepo-815ad21b', 'ANVIL_ALS_FTD_DEMENTIA_SEQ_GRU_v1_20231221_ANV5_202401112033', 1351),
    mksrc('datarepo-ab46a8e4', 'ANVIL_CCDG_NYGC_NP_Autism_ACE2_DS_MDS_WGS_20230605_ANV5_202403032021', 158),
    mksrc('datarepo-df058a48', 'ANVIL_CCDG_NYGC_NP_Autism_AGRE_WGS_20230605_ANV5_202403032044', 4601),
    mksrc('datarepo-61910b61', 'ANVIL_CCDG_NYGC_NP_Autism_CAG_DS_WGS_20230605_ANV5_202403032053', 738),
    mksrc('datarepo-8d6472a1', 'ANVIL_CCDG_NYGC_NP_Autism_HFA_DS_WGS_20230605_ANV5_202403032108', 510),
    mksrc('datarepo-f0a12498', 'ANVIL_CCDG_NYGC_NP_Autism_PELPHREY_ACE_DS_WGS_20221103_ANV5_202403032124', 48),
    mksrc('datarepo-f06dc5dd', 'ANVIL_CCDG_NYGC_NP_Autism_PELPHREY_ACE_GRU_WGS_20221103_ANV5_202403032131', 198),
    mksrc('datarepo-b791f5c1', 'ANVIL_CCDG_NYGC_NP_Autism_SAGE_WGS_20230605_ANV5_202403032137', 1627),
    mksrc('datarepo-b9222139', 'ANVIL_CMG_BROAD_BRAIN_ENGLE_WES_20240205_ANV5_202402051624', 473),
    mksrc('datarepo-7e094253', 'ANVIL_CMG_BROAD_BRAIN_SHERR_WGS_20221102_ANV5_202402281543', 3),
    mksrc('datarepo-c797490f', 'ANVIL_CMG_BROAD_ORPHAN_SCOTT_WGS_20221102_ANV5_202402281552', 15),
    mksrc('datarepo-0a1360b1', 'ANVIL_CMG_Broad_Blood_Gazda_WES_20221117_ANV5_202402290547', 612),
    mksrc('datarepo-faa71b49', 'ANVIL_CMG_Broad_Blood_Sankaran_WES_20221117_ANV5_202402290555', 1141),
    mksrc('datarepo-abce6387', 'ANVIL_CMG_Broad_Blood_Sankaran_WGS_20221117_ANV5_202402290606', 96),
    mksrc('datarepo-4153ad1f', 'ANVIL_CMG_Broad_Muscle_Laing_WES_20221208_ANV5_202402291926', 31),
    mksrc('datarepo-5bbb5a28', 'ANVIL_CMG_Broad_Orphan_Jueppner_WES_20240205_ANV5_202402051640', 11),
    mksrc('datarepo-18bd3df4', 'ANVIL_CMG_UWASH_HMB_20230418_ANV5_202402070029', 610),
    mksrc('datarepo-6f4155f2', 'ANVIL_CMG_UWash_GRU_20240301_ANV5_202403040330', 7415),
    mksrc('datarepo-6486ae96', 'ANVIL_CMG_UWash_GRU_1_20240113_ANV5_202401141440', 3005),
    mksrc('datarepo-0fad0f77', 'ANVIL_CMG_YALE_DS_RARED_20221020_ANV5_202402281620', 173),
    mksrc('datarepo-ad307392', 'ANVIL_CMG_Yale_GRU_20221020_ANV5_202402281628', 2182),
    mksrc('datarepo-fecab5bc', 'ANVIL_CMG_Yale_HMB_20221020_ANV5_202402290926', 125),
    mksrc('datarepo-f9699204', 'ANVIL_CMG_Yale_HMB_GSO_20221020_ANV5_202402290935', 4264),
    mksrc('datarepo-c5bd892a', 'ANVIL_CMH_GAFK_GS_linked_read_20221107_ANV5_202402290945', 626),
    mksrc('datarepo-5e64223a', 'ANVIL_CMH_GAFK_GS_long_read_20240301_ANV5_202403040349', 2817),
    mksrc('datarepo-ba97c05c', 'ANVIL_CMH_GAFK_scRNA_20221107_ANV5_202402291004', 198),
    mksrc('datarepo-2659c380', 'ANVIL_CSER_CHARM_GRU_20240301_ANV5_202403040357', 3338),
    mksrc('datarepo-0f2e95ad', 'ANVIL_CSER_KidsCanSeq_GRU_20221208_ANV5_202402292138', 2594),
    mksrc('datarepo-62a0bd6d', 'ANVIL_CSER_NCGENES2_GRU_20221208_ANV5_202402292147', 142),
    mksrc('datarepo-df02801a', 'ANVIL_CSER_NYCKIDSEQ_GRU_20240113_ANV5_202401141520', 513),
    mksrc('datarepo-4b9c138d', 'ANVIL_CSER_NYCKIDSEQ_HMB_20240113_ANV5_202401141527', 333),
    mksrc('datarepo-f4d60c69', 'ANVIL_CSER_P3EGS_GRU_20230727_ANV5_202402070059', 2544),
    mksrc('datarepo-fc5ed559', 'ANVIL_CSER_SouthSeq_GRU_20221208_ANV5_202402292154', 808),
    mksrc('datarepo-74121c99', 'ANVIL_GTEx_BCM_GRU_CoRSIVs_20240116_ANV5_202401170141', 3232),
    mksrc('datarepo-1a706b0c', 'ANVIL_GTEx_Somatic_WGS_20240116_ANV5_202401170147', 708),
    mksrc('datarepo-e063cf6d', 'ANVIL_GTEx_V7_hg19_20221128_ANV5_202402291034', 15974),
    mksrc('datarepo-383c097a', 'ANVIL_GTEx_V8_hg38_20240116_ANV5_202401170154', 74688),
    mksrc('datarepo-701eea84', 'ANVIL_GTEx_V9_hg38_20221128_ANV5_202402070108', 8619),
    mksrc('datarepo-ff9d78a5', 'ANVIL_GTEx_public_data_20240117_ANV5_202401180400', 25792),
    mksrc('datarepo-37c3d458', 'ANVIL_NIA_CARD_Coriell_Cell_Lines_Open_20230727_ANV5_202401111624', 12531),
    mksrc('datarepo-06c78117', 'ANVIL_NIA_CARD_LR_WGS_NABEC_GRU_20230727_ANV5_202401111634', 27672),
    mksrc('datarepo-e4eb7641', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Braff_DS_WGS_20221115_ANV5_202304242052', 864, pop),
    mksrc('datarepo-a3880121', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Pato_GRU_WGS_20240112_ANV5_202402062129', 8084),
    mksrc('datarepo-25790186', 'ANVIL_PAGE_BioMe_GRU_WGS_20221128_ANV5_202403040429', 308),
    mksrc('datarepo-b371989b', 'ANVIL_PAGE_MEC_GRU_WGS_20230131_ANV5_202403040437', 70),
    mksrc('datarepo-4a4eec27', 'ANVIL_PAGE_SoL_HMB_WGS_20221220_ANV5_202403040445', 234),
    mksrc('datarepo-a1f917db', 'ANVIL_PAGE_Stanford_Global_Reference_Panel_GRU_WGS_20221128_ANV5_202403040453', 78),
    mksrc('datarepo-6264931f', 'ANVIL_PAGE_WHI_HMB_IRB_WGS_20221019_ANV5_202403040500', 235),
    mksrc('datarepo-8d62ec8f', 'ANVIL_T2T_20230714_ANV5_202312122150', 261317),
    mksrc('datarepo-bfabc906', 'ANVIL_ccdg_asc_ndd_daly_talkowski_ac_boston_asd_exome_20221117_ANV5_202403040552', 759),
    mksrc('datarepo-825399a4', 'ANVIL_ccdg_asc_ndd_daly_talkowski_barbosa_asd_exome_20221108_ANV5_202403040608', 215),
    mksrc('datarepo-e3b070a7', 'ANVIL_ccdg_asc_ndd_daly_talkowski_brusco_asd_exome_20230327_ANV5_202403040615', 2246),
    mksrc('datarepo-2354d65a', 'ANVIL_ccdg_asc_ndd_daly_talkowski_cdcseed_asd_gsa_md_20221024_ANV5_202402291144', 2888),
    mksrc('datarepo-0ad3f21a', 'ANVIL_ccdg_asc_ndd_daly_talkowski_chung_asd_exome_20221107_ANV5_202403040623', 527),
    mksrc('datarepo-c148a340', 'ANVIL_ccdg_asc_ndd_daly_talkowski_control_NIMH_asd_exome_20221201_ANV5_202403040630', 372),  # noqa E501
    mksrc('datarepo-bc613fa9', 'ANVIL_ccdg_asc_ndd_daly_talkowski_domenici_asd_exome_20221117_ANV5_202403040637', 713),
    mksrc('datarepo-97e22445', 'ANVIL_ccdg_asc_ndd_daly_talkowski_goethe_asd_exome_20221107_ANV5_202403040652', 2573),
    mksrc('datarepo-72efc816', 'ANVIL_ccdg_asc_ndd_daly_talkowski_herman_asd_exome_20221117_ANV5_202403040701', 51),
    mksrc('datarepo-e25caee8', 'ANVIL_ccdg_asc_ndd_daly_talkowski_hertz_picciotto_asd_exome_20221107_ANV5_202403040708', 1539),  # noqa E501
    mksrc('datarepo-22af2470', 'ANVIL_ccdg_asc_ndd_daly_talkowski_hertz_picciotto_asd_wgs_20221107_ANV5_202403040716', 69),  # noqa E501
    mksrc('datarepo-a81009d9', 'ANVIL_ccdg_asc_ndd_daly_talkowski_hultman_asd_exome_20231013_ANV5_202403040723', 1556),
    mksrc('datarepo-bc078d98', 'ANVIL_ccdg_asc_ndd_daly_talkowski_kolevzon_asd_exome_20221108_ANV5_202403040731', 1166),
    mksrc('datarepo-0949186c', 'ANVIL_ccdg_asc_ndd_daly_talkowski_kolevzon_asd_wgs_20221109_ANV5_202403040739', 31),
    mksrc('datarepo-4dc4f939', 'ANVIL_ccdg_asc_ndd_daly_talkowski_lattig_asd_exome_20221122_ANV5_202403040746', 496),
    mksrc('datarepo-5ed988f8', 'ANVIL_ccdg_asc_ndd_daly_talkowski_menashe_asd_exome_20221108_ANV5_202403040800', 716),
    mksrc('datarepo-c6a938e4', 'ANVIL_ccdg_asc_ndd_daly_talkowski_minshew_asd_exome_20221117_ANV5_202403040807', 241),
    mksrc('datarepo-a245d786', 'ANVIL_ccdg_asc_ndd_daly_talkowski_palotie_asd_exome_20221019_ANV5_202403040815', 155),
    mksrc('datarepo-7ddd7425', 'ANVIL_ccdg_asc_ndd_daly_talkowski_parellada_asd_exome_20221108_ANV5_202403040822', 2831),  # noqa E501
    mksrc('datarepo-aa9f0b28', 'ANVIL_ccdg_asc_ndd_daly_talkowski_pericak_vance_asd_wgs_20221027_ANV5_202403040846', 299),  # noqa E501
    mksrc('datarepo-0b4c3cfb', 'ANVIL_ccdg_asc_ndd_daly_talkowski_schloesser_asd_gsa_md_20221025_ANV5_202402291202', 158),  # noqa E501
    mksrc('datarepo-8023858b', 'ANVIL_ccdg_asc_ndd_daly_talkowski_weiss_asd_exome_20221108_ANV5_202403040925', 43),
    mksrc('datarepo-381b5d80', 'ANVIL_ccdg_broad_ai_ibd_alm_gmc_wes_20230328_ANV5_202403040932', 919),
    mksrc('datarepo-714d60b9', 'ANVIL_ccdg_broad_ai_ibd_daly_alm_gmc_gsa_20221025_ANV5_202402291210', 948),
    mksrc('datarepo-86a1dbf3', 'ANVIL_ccdg_broad_ai_ibd_daly_bernstein_gsa_20221025_ANV5_202304241921', 543, pop),
    mksrc('datarepo-dc7a9acd', 'ANVIL_ccdg_broad_ai_ibd_daly_brant_niddk_gsa_20240103_ANV5_202401112147', 6470),
    mksrc('datarepo-916fc0b6', 'ANVIL_ccdg_broad_ai_ibd_daly_duerr_niddk_gsa_20240113_ANV5_202402062134', 9647),
    mksrc('datarepo-48d85607', 'ANVIL_ccdg_broad_ai_ibd_daly_hyams_protect_wes_20240104_ANV5_202403041011', 1672),
    mksrc('datarepo-21d3c731', 'ANVIL_ccdg_broad_ai_ibd_daly_kupcinskas_wes_20240104_ANV5_202403041018', 9827),
    mksrc('datarepo-614a8519', 'ANVIL_ccdg_broad_ai_ibd_daly_lewis_ccfa_wes_20240113_ANV5_202403041026', 2868),
    mksrc('datarepo-6799d240', 'ANVIL_ccdg_broad_ai_ibd_daly_lewis_sparc_gsa_20240104_ANV5_202401121517', 17188),
    mksrc('datarepo-d7ae08a2', 'ANVIL_ccdg_broad_ai_ibd_daly_louis_wes_20240104_ANV5_202403041042', 8937),
    mksrc('datarepo-9b04a16e', 'ANVIL_ccdg_broad_ai_ibd_daly_mccauley_gsa_20240113_ANV5_202402062137', 8217),
    mksrc('datarepo-b6a95447', 'ANVIL_ccdg_broad_ai_ibd_daly_mccauley_wes_20240104_ANV5_202403041049', 6998),
    mksrc('datarepo-df7a6188', 'ANVIL_ccdg_broad_ai_ibd_daly_mcgovern_gsa_20240118_ANV5_202402062140', 30450),
    mksrc('datarepo-5cd83e88', 'ANVIL_ccdg_broad_ai_ibd_daly_mcgovern_niddk_wes_20240104_ANV5_202403041057', 35211),
    mksrc('datarepo-fa7e066f', 'ANVIL_ccdg_broad_ai_ibd_daly_mcgovern_share_wes_20240104_ANV5_202401121556', 2422),
    mksrc('datarepo-2def0ed8', 'ANVIL_ccdg_broad_ai_ibd_daly_moayyedi_imagine_gsa_20240105_ANV5_202401121603', 15302),
    mksrc('datarepo-6e9fe586', 'ANVIL_ccdg_broad_ai_ibd_daly_moayyedi_imagine_wes_20240105_ANV5_202403041109', 2553),
    mksrc('datarepo-1f3dab2b', 'ANVIL_ccdg_broad_ai_ibd_daly_pekow_share_gsa_20240105_ANV5_202401121646', 4170),
    mksrc('datarepo-74869ac4', 'ANVIL_ccdg_broad_ai_ibd_daly_pekow_share_wes_20240105_ANV5_202403041133', 2572),
    mksrc('datarepo-d95b9a73', 'ANVIL_ccdg_broad_ai_ibd_niddk_daly_brant_wes_20240112_ANV5_202403041232', 1084),
    mksrc('datarepo-7a0883a4', 'ANVIL_ccdg_broad_cvd_af_pegasus_hmb_20221025_ANV5_202403030736', 7483),
    mksrc('datarepo-f62c5ebd', 'ANVIL_ccdg_broad_cvd_eocad_promis_wgs_20221213_ANV5_202403030935', 1136),
    mksrc('datarepo-9d116a5c', 'ANVIL_ccdg_broad_mi_atvb_ds_cvd_wes_20221025_ANV5_202403031035', 60),
    mksrc('datarepo-bb315b29', 'ANVIL_ccdg_nygc_np_autism_tasc_wgs_20221024_ANV5_202403032216', 905),
    mksrc('datarepo-33e3428b', 'ANVIL_ccdg_washu_cvd_np_ai_controls_vccontrols_wgs_20221024_ANV5_202403032319', 112),
    mksrc('datarepo-17c5f983', 'ANVIL_cmg_broad_brain_engle_wgs_20221202_ANV5_202402290614', 95),
    mksrc('datarepo-a46c0244', 'ANVIL_nhgri_broad_ibd_daly_kugathasan_wes_20240112_ANV5_202403041258', 548),
    mksrc('datarepo-4b4f2325', 'ANVIL_nhgri_broad_ibd_daly_turner_wes_20240112_ANV5_202403041307', 157),
    # @formatter:on
]))

anvil6_sources = mkdict(anvil5_sources, 250, mkdelta([
    # @formatter:off
    mksrc('datarepo-38af6304', 'ANVIL_1000G_PRIMED_data_model_20240410_ANV5_202404101419', 14695),
    mksrc('datarepo-1a86e7ca', 'ANVIL_CCDG_Baylor_CVD_AFib_Groningen_WGS_20221122_ANV5_202304242224', 639, pop),
    mksrc('datarepo-92716a90', 'ANVIL_CCDG_Baylor_CVD_AFib_VAFAR_HMB_IRB_WGS_20221020_ANV5_202304211525', 253, pop),
    mksrc('datarepo-e8fc4258', 'ANVIL_CCDG_Baylor_CVD_ARIC_20231008_ANV5_202403030358', 10012),
    mksrc('datarepo-77445496', 'ANVIL_CCDG_Baylor_CVD_EOCAD_BioMe_WGS_20221122_ANV5_202304242226', 1201, pop),
    mksrc('datarepo-1b0d6b90', 'ANVIL_CCDG_Baylor_CVD_HHRC_Brownsville_GRU_WGS_20221122_ANV5_202304242228', 276, pop),
    mksrc('datarepo-373b7918', 'ANVIL_CCDG_Baylor_CVD_HemStroke_BNI_HMB_WGS_20221215_ANV5_202304242306', 160, pop),
    mksrc('datarepo-efc3e806', 'ANVIL_CCDG_Baylor_CVD_HemStroke_Duke_DS_WGS_20221117_ANV5_202304242122', 60, pop),
    mksrc('datarepo-1044f96d', 'ANVIL_CCDG_Baylor_CVD_HemStroke_ERICH_WGS_20221207_ANV5_202304271256', 2558, pop),
    mksrc('datarepo-f23a6ec8', 'ANVIL_CCDG_Baylor_CVD_HemStroke_GERFHS_HMB_WGS_20221215_ANV5_202304242307', 412, pop),
    mksrc('datarepo-de34ca6e', 'ANVIL_CCDG_Baylor_CVD_HemStroke_Regards_DS_WGS_20221117_ANV5_202304242123', 121, pop),
    mksrc('datarepo-d9c6f406', 'ANVIL_CCDG_Baylor_CVD_HemStroke_Yale_HMB_WGS_20221215_ANV5_202304242309', 185, pop),
    mksrc('datarepo-56883e56', 'ANVIL_CCDG_Baylor_CVD_Oregon_SUDS_GRU_WGS_20221215_ANV5_202304242302', 2216, pop),
    mksrc('datarepo-7f3ba7ec', 'ANVIL_CCDG_Baylor_CVD_TexGen_DS_WGS_20221117_ANV5_202304242125', 6461, pop),
    mksrc('datarepo-da965e26', 'ANVIL_CCDG_Baylor_CVD_Ventura_Presto_GRU_IRB_WGS_20221117_ANV5_202304242127', 584, pop),
    mksrc('datarepo-40647d03', 'ANVIL_CCDG_Broad_AI_IBD_Brant_DS_IBD_WGS_20240113_ANV5_202401141252', 575),
    mksrc('datarepo-83339911', 'ANVIL_CCDG_Broad_AI_IBD_Brant_HMB_WGS_20240113_ANV5_202401141259', 907),
    mksrc('datarepo-3f36066b', 'ANVIL_CCDG_Broad_AI_IBD_Cho_WGS_20240113_ANV5_202403030543', 352),
    mksrc('datarepo-65e890b6', 'ANVIL_CCDG_Broad_AI_IBD_Kugathasan_WGS_20240113_ANV5_202403030551', 1406),
    mksrc('datarepo-cec499cd', 'ANVIL_CCDG_Broad_AI_IBD_McCauley_WGS_20240114_ANV5_202403030559', 1256),
    mksrc('datarepo-8043de16', 'ANVIL_CCDG_Broad_AI_IBD_McGovern_WGS_20240113_ANV5_202403030608', 11273),
    mksrc('datarepo-de3bfd4e', 'ANVIL_CCDG_Broad_AI_IBD_Newberry_WGS_20240113_ANV5_202403030616', 135),
    mksrc('datarepo-ed109b2f', 'ANVIL_CCDG_Broad_CVD_AF_BioVU_HMB_GSO_Arrays_20230612_ANV5_202306131350', 9, pop),
    mksrc('datarepo-3d8b62d7', 'ANVIL_CCDG_Broad_CVD_AF_BioVU_HMB_GSO_WES_20221025_ANV5_202304241856', 5039, pop),
    mksrc('datarepo-450ba911', 'ANVIL_CCDG_Broad_CVD_AF_ENGAGE_DS_WES_20230418_ANV5_202304210808', 13621, pop),
    mksrc('datarepo-0768a322', 'ANVIL_CCDG_Broad_CVD_AF_Ellinor_MGH_Arrays_20221024_ANV5_202304211831', 387, pop),
    mksrc('datarepo-dfabf632', 'ANVIL_CCDG_Broad_CVD_AF_Ellinor_MGH_WES_20221117_ANV5_202304271354', 499, pop),
    mksrc('datarepo-485eb707', 'ANVIL_CCDG_Broad_CVD_AF_Figtree_BioHeart_Arrays_20230128_ANV5_202304271554', 935, pop),
    mksrc('datarepo-58dffe5a', 'ANVIL_CCDG_Broad_CVD_AF_GAPP_DS_MDS_Arrays_20221103_ANV5_202304242105', 2087, pop),
    mksrc('datarepo-cf7f2c0c', 'ANVIL_CCDG_Broad_CVD_AF_GAPP_DS_MDS_WES_20221103_ANV5_202304242107', 2154, pop),
    mksrc('datarepo-f896734e', 'ANVIL_CCDG_Broad_CVD_AF_Marcus_UCSF_Arrays_20221102_ANV5_202304242039', 160, pop),
    mksrc('datarepo-40c2f4f4', 'ANVIL_CCDG_Broad_CVD_AF_Marcus_UCSF_WES_20221222_ANV5_202304242040', 599, pop),
    mksrc('datarepo-67117555', 'ANVIL_CCDG_Broad_CVD_AF_Rienstra_WES_20221222_ANV5_202304242035', 2097, pop),
    mksrc('datarepo-c45dd622', 'ANVIL_CCDG_Broad_CVD_AF_Swiss_Cases_DS_MDS_Arrays_20221103_ANV5_202304242110', 4467, pop), # noqa E501
    mksrc('datarepo-b12d2e52', 'ANVIL_CCDG_Broad_CVD_AF_Swiss_Cases_DS_MDS_WES_20230118_ANV5_202304242112', 4550, pop),
    mksrc('datarepo-d795027d', 'ANVIL_CCDG_Broad_CVD_AF_VAFAR_Arrays_20221020_ANV5_202304211823', 1390, pop),
    mksrc('datarepo-642829f3', 'ANVIL_CCDG_Broad_CVD_AF_VAFAR_WES_20221024_ANV5_202304211826', 1548, pop),
    mksrc('datarepo-43f6230a', 'ANVIL_CCDG_Broad_CVD_AFib_AFLMU_WGS_20231008_ANV5_202310091911', 386, pop),
    mksrc('datarepo-2b135baf', 'ANVIL_CCDG_Broad_CVD_AFib_MGH_WGS_20221024_ANV5_202304211829', 105, pop),
    mksrc('datarepo-de64d25a', 'ANVIL_CCDG_Broad_CVD_AFib_UCSF_WGS_20221222_ANV5_202304242037', 112, pop),
    mksrc('datarepo-08216a2c', 'ANVIL_CCDG_Broad_CVD_AFib_Vanderbilt_Ablation_WGS_20221020_ANV5_202304211819', 2, pop),
    mksrc('datarepo-342c77f2', 'ANVIL_CCDG_Broad_CVD_EOCAD_PartnersBiobank_HMB_Arrays_20230517_ANV5_202312122054', 46570), # noqa E501
    mksrc('datarepo-a16f8bac', 'ANVIL_CCDG_Broad_CVD_EOCAD_PartnersBiobank_HMB_WES_20230621_ANV5_202403030943', 17479),
    mksrc('datarepo-f2179275', 'ANVIL_CCDG_Broad_CVD_EOCAD_TaiChi_WGS_20221026_ANV5_202403030955', 912),
    mksrc('datarepo-e8ee6358', 'ANVIL_CCDG_Broad_CVD_EOCAD_VIRGO_WGS_20221024_ANV5_202403031003', 2182),
    mksrc('datarepo-383d9d9b', 'ANVIL_CCDG_Broad_CVD_PROMIS_GRU_WES_20230418_ANV5_202306211912', 20557, pop),
    mksrc('datarepo-318ae48e', 'ANVIL_CCDG_Broad_CVD_Stroke_BRAVE_WGS_20221107_ANV5_202304241543', 500, pop),
    mksrc('datarepo-7ea7a6e9', 'ANVIL_CCDG_Broad_MI_BRAVE_GRU_WES_20221107_ANV5_202304241545', 1500, pop),
    mksrc('datarepo-5df71da4', 'ANVIL_CCDG_Broad_MI_InStem_WES_20221122_ANV5_202304242236', 1452, pop),
    mksrc('datarepo-1793828c', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSALF_HMB_IRB_GSRS_WES_20230324_ANV5_202304241752', 18, pop), # noqa E501
    mksrc('datarepo-0db6105c', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSALF_HMB_IRB_WES_20230128_ANV5_202402020211', 19),
    mksrc('datarepo-70c803d7', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EPIL_BA_MDS_GSA_MD_20221117_ANV5_202304271400', 22, pop), # noqa E501
    mksrc('datarepo-1b92691d', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EPIL_BA_MDS_WES_20221101_ANV5_202403031115', 47),
    mksrc('datarepo-f5a4a895', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EPI_BA_ID_MDS_GSA_MD_20221117_ANV5_202304271358', 37, pop), # noqa E501
    mksrc('datarepo-3da39a32', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EPI_BA_ID_MDS_WES_20221101_ANV5_202403031123', 136),
    mksrc('datarepo-b8b8ba44', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EP_BA_CN_ID_MDS_GSA_MD_20221117_ANV5_202304271356', 1530, pop), # noqa E501
    mksrc('datarepo-b3e42c63', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EP_BA_CN_ID_MDS_WES_20221101_ANV5_202403031131', 5399), # noqa E501
    mksrc('datarepo-a2b20d71', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSRMB_DS_EAED_IRB_WES_20230621_ANV5_202402020256', 216),
    mksrc('datarepo-f85048a3', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSRMB_DS_EAED_MDS_GSA_MD_20221117_ANV5_202304271401', 398, pop), # noqa E501
    mksrc('datarepo-b3ef2bd3', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSRMB_DS_EAED_MDS_WES_20221026_ANV5_202403031140', 429),
    mksrc('datarepo-1cafba94', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUTMUV_DS_NS_ADLT_WES_20230128_ANV5_202402020305', 14),
    mksrc('datarepo-006c9286', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUTMUV_DS_NS_WES_20230314_ANV5_202402020314', 107),
    mksrc('datarepo-92905a2b', 'ANVIL_CCDG_Broad_NP_Epilepsy_BELATW_GRU_GSA_MD_20221117_ANV5_202304271403', 239, pop),
    mksrc('datarepo-33e1bed9', 'ANVIL_CCDG_Broad_NP_Epilepsy_BELATW_GRU_WES_20221108_ANV5_202402020322', 113),
    mksrc('datarepo-3f3ad5c7', 'ANVIL_CCDG_Broad_NP_Epilepsy_BELULB_DS_EP_NPU_GSA_MD_20230118_ANV5_202304271404', 430, pop), # noqa E501
    mksrc('datarepo-b2a5eccc', 'ANVIL_CCDG_Broad_NP_Epilepsy_BELULB_DS_EP_NPU_WES_20221027_ANV5_202403031148', 419),
    mksrc('datarepo-7a7b911a', 'ANVIL_CCDG_Broad_NP_Epilepsy_BRAUSP_DS_WES_20240201_ANV5_202402020339', 7),
    mksrc('datarepo-33634ed0', 'ANVIL_CCDG_Broad_NP_Epilepsy_CANCAL_GRU_v2_WES_20240201_ANV5_202402020347', 272),
    mksrc('datarepo-47f93bbb', 'ANVIL_CCDG_Broad_NP_Epilepsy_CANUTN_DS_EP_WES_20230328_ANV5_202403031156', 149),
    mksrc('datarepo-389af3b3', 'ANVIL_CCDG_Broad_NP_Epilepsy_CHEUBB_HMB_IRB_MDS_WES_20221102_ANV5_202403031205', 49),
    mksrc('datarepo-ac8e01aa', 'ANVIL_CCDG_Broad_NP_Epilepsy_CYPCYP_HMB_NPU_MDS_WES_20230328_ANV5_202403031213', 185),
    mksrc('datarepo-5d4aa202', 'ANVIL_CCDG_Broad_NP_Epilepsy_CZEMTH_GRU_WES_20221108_ANV5_202403031222', 18),
    mksrc('datarepo-bd066b5a', 'ANVIL_CCDG_Broad_NP_Epilepsy_DEUPUM_HMB_MDS_WES_20230328_ANV5_202403031231', 341),
    mksrc('datarepo-17de3c3b', 'ANVIL_CCDG_Broad_NP_Epilepsy_DEUUGS_DS_EP_MDS_WES_20240201_ANV5_202403031239', 393),
    mksrc('datarepo-46e7e2ab', 'ANVIL_CCDG_Broad_NP_Epilepsy_DEUUKB_HMB_NPU_MDS_WES_20230328_ANV5_202403031247', 2512),
    mksrc('datarepo-ba863f29', 'ANVIL_CCDG_Broad_NP_Epilepsy_DEUUKL_HMB_WES_20221102_ANV5_202403031256', 217),
    mksrc('datarepo-113d9969', 'ANVIL_CCDG_Broad_NP_Epilepsy_DEUULG_GRU_WES_20221108_ANV5_202403031305', 94),
    mksrc('datarepo-fd6d20c8', 'ANVIL_CCDG_Broad_NP_Epilepsy_DEUUTB_HMB_NPU_MDS_WES_20230328_ANV5_202403031313', 1977),
    mksrc('datarepo-55d32c1b', 'ANVIL_CCDG_Broad_NP_Epilepsy_FINKPH_EPIL_CO_MORBIDI_MDS_WES_20230328_ANV5_202403031322', 914), # noqa E501
    mksrc('datarepo-844a1ecf', 'ANVIL_CCDG_Broad_NP_Epilepsy_FINUVH_HMB_NPU_MDS_WES_20221114_ANV5_202403031331', 102),
    mksrc('datarepo-1cbd28a5', 'ANVIL_CCDG_Broad_NP_Epilepsy_FRALYU_HMB_WES_20230621_ANV5_202403031340', 1042),
    mksrc('datarepo-b8b0b663', 'ANVIL_CCDG_Broad_NP_Epilepsy_GBRSWU_CARDI_NEURO_WES_20221026_ANV5_202403031348', 319),
    mksrc('datarepo-2686a76a', 'ANVIL_CCDG_Broad_NP_Epilepsy_GBRUNL_EP_ETIOLOGY_MDS_WES_20221027_ANV5_202403031405', 460), # noqa E501
    mksrc('datarepo-05e028a4', 'ANVIL_CCDG_Broad_NP_Epilepsy_GBRUNL_GRU_WES_20221108_ANV5_202403031413', 57),
    mksrc('datarepo-4a6228be', 'ANVIL_CCDG_Broad_NP_Epilepsy_GHAKNT_GRU_WES_20221122_ANV5_202403031421', 646),
    mksrc('datarepo-98dddf8f', 'ANVIL_CCDG_Broad_NP_Epilepsy_HKGHKK_HMB_MDS_WES_20230328_ANV5_202403031430', 675),
    mksrc('datarepo-9ed2a64a', 'ANVIL_CCDG_Broad_NP_Epilepsy_HKOSB_GRU_WES_20230110_ANV5_202403031439', 118),
    mksrc('datarepo-22a9e8bd', 'ANVIL_CCDG_Broad_NP_Epilepsy_HRVUZG_HMB_MDS_WES_20221114_ANV5_202403031446', 42),
    mksrc('datarepo-517eda47', 'ANVIL_CCDG_Broad_NP_Epilepsy_IRLRCI_GRU_IRB_WES_20230328_ANV5_202403031454', 943),
    mksrc('datarepo-b6e444c4', 'ANVIL_CCDG_Broad_NP_Epilepsy_ITAICB_HMB_NPU_MDS_WES_20230223_ANV5_202403031503', 434),
    mksrc('datarepo-d8145bea', 'ANVIL_CCDG_Broad_NP_Epilepsy_ITAIGI_GRU_WES_20221108_ANV5_202403031512', 1163),
    mksrc('datarepo-67c3b200', 'ANVIL_CCDG_Broad_NP_Epilepsy_ITAUBG_DS_EPI_NPU_MDS_WES_20221027_ANV5_202403031520', 619), # noqa E501
    mksrc('datarepo-4476c338', 'ANVIL_CCDG_Broad_NP_Epilepsy_ITAUMC_DS_NEURO_MDS_WES_20221108_ANV5_202403031529', 418),
    mksrc('datarepo-5cd83a64', 'ANVIL_CCDG_Broad_NP_Epilepsy_ITAUMR_GRU_NPU_WES_20221114_ANV5_202403031537', 1098),
    mksrc('datarepo-5115b904', 'ANVIL_CCDG_Broad_NP_Epilepsy_JPNFKA_GRU_WES_20221220_ANV5_202403031547', 372),
    mksrc('datarepo-f7fb0742', 'ANVIL_CCDG_Broad_NP_Epilepsy_JPNRKI_DS_NPD_IRB_NPU_WES_20221027_ANV5_202402062057', 100), # noqa E501
    mksrc('datarepo-b979e83a', 'ANVIL_CCDG_Broad_NP_Epilepsy_KENKIL_GRU_WES_20230110_ANV5_202403031555', 452),
    mksrc('datarepo-54571a90', 'ANVIL_CCDG_Broad_NP_Epilepsy_LEBABM_DS_Epilepsy_WES_20230328_ANV5_202403031603', 398),
    mksrc('datarepo-5495da63', 'ANVIL_CCDG_Broad_NP_Epilepsy_LEBABM_GRU_WES_20230110_ANV5_202403031612', 856),
    mksrc('datarepo-7275a9bd', 'ANVIL_CCDG_Broad_NP_Epilepsy_LTUUHK_HMB_NPU_MDS_WES_20221114_ANV5_202403031621', 302),
    mksrc('datarepo-2c2a7d19', 'ANVIL_CCDG_Broad_NP_Epilepsy_NZLUTO_EPIL_BC_ID_MDS_WES_20230328_ANV5_202403031629', 275), # noqa E501
    mksrc('datarepo-edbd02ca', 'ANVIL_CCDG_Broad_NP_Epilepsy_TURBZU_GRU_WES_20221108_ANV5_202403031637', 214),
    mksrc('datarepo-225a7340', 'ANVIL_CCDG_Broad_NP_Epilepsy_TURIBU_DS_NEURO_AD_NPU_WES_20221027_ANV5_202403031645', 169), # noqa E501
    mksrc('datarepo-97dadba8', 'ANVIL_CCDG_Broad_NP_Epilepsy_TWNCGM_HMB_NPU_AdultsONLY_WES_20240201_ANV5_202402020902', 897), # noqa E501
    mksrc('datarepo-6dcb5d39', 'ANVIL_CCDG_Broad_NP_Epilepsy_USABCH_EPI_MUL_CON_MDS_WES_20221027_ANV5_202403031701', 330), # noqa E501
    mksrc('datarepo-fb4ac7d8', 'ANVIL_CCDG_Broad_NP_Epilepsy_USABLC_GRU_NPU_WES_20221215_ANV5_202402062059', 227),
    mksrc('datarepo-5de241b3', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACCF_HMB_MDS_WES_20221207_ANV5_202403031709', 390),
    mksrc('datarepo-62a84074', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACCH_DS_NEURO_MDS_WES_20221116_ANV5_202403031719', 362),
    mksrc('datarepo-7c06247a', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACHP_GRU_WES_20230612_ANV5_202402062101', 3754),
    mksrc('datarepo-9042eb4a', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACRW_DS_EP_MDS_WES_20221027_ANV5_202403031727', 328),
    mksrc('datarepo-cb75258b', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACRW_DS_SEIZD_WES_20221027_ANV5_202403031735', 154),
    mksrc('datarepo-744bc858', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACRW_EPI_ASZ_MED_MDS_WES_20221027_ANV5_202403031744', 39), # noqa E501
    mksrc('datarepo-faff5b2b', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAEGP_GRU_WES_20221110_ANV5_202403031752', 129),
    mksrc('datarepo-275b2a46', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAFEB_GRU_WES_20221205_ANV5_202403031800', 31),
    mksrc('datarepo-5a548fd8', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAHEP_GRU_WES_20230328_ANV5_202403031809', 328),
    mksrc('datarepo-999301d3', 'ANVIL_CCDG_Broad_NP_Epilepsy_USALCH_HMB_WES_20230126_ANV5_202402021048', 10),
    mksrc('datarepo-eda3f720', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAMGH_HMB_MDS_WES_20221207_ANV5_202403031817', 22),
    mksrc('datarepo-d9e55ea0', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAMGH_MGBB_HMB_MDS_WES_20221207_ANV5_202403031826', 46),
    mksrc('datarepo-6a627e94', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAMON_GRU_NPU_WES_20221215_ANV5_202403031834', 8),
    mksrc('datarepo-bfa59a11', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAMON_GRU_WES_20240201_ANV5_202403031842', 251),
    mksrc('datarepo-f8d5318a', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAMON_HMB_WES_20230131_ANV5_202402021131', 7),
    mksrc('datarepo-4ef1d979', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAMSS_DS_EP_NEURO_MDS_WES_20230612_ANV5_202402021139', 1275), # noqa E501
    mksrc('datarepo-5e00a0df', 'ANVIL_CCDG_Broad_NP_Epilepsy_USANCH_DS_NEURO_MDS_WES_20221108_ANV5_202402062105', 313),
    mksrc('datarepo-10948836', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAUPN_Marsh_GRU_NPU_WES_20221114_ANV5_202403031858', 49),
    mksrc('datarepo-0a247e9e', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAUPN_Marsh_GRU_WES_20230328_ANV5_202403031906', 355),
    mksrc('datarepo-154b4ef8', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAUPN_Rader_GRU_WES_20230328_ANV5_202403031915', 832),
    mksrc('datarepo-07b8d88c', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAVAN_HMB_GSO_WES_20221207_ANV5_202402021226', 2454),
    mksrc('datarepo-1985a01d', 'ANVIL_CCDG_Broad_Spalletta_HMB_NPU_MDS_WES_20221102_ANV5_202403031942', 104),
    mksrc('datarepo-ad61c47e', 'ANVIL_CCDG_NHGRI_Broad_ASD_Daly_phs000298_WES_vcf_20230403_ANV5_202304271610', 3922, pop), # noqa E501
    mksrc('datarepo-5e719362', 'ANVIL_CCDG_NYGC_AI_Asthma_Gala2_WGS_20230605_ANV5_202306131248', 1310, pop),
    mksrc('datarepo-2734a0e4', 'ANVIL_CCDG_NYGC_NP_Alz_EFIGA_WGS_20230605_ANV5_202306141705', 3750, pop),
    mksrc('datarepo-710fc60d', 'ANVIL_CCDG_NYGC_NP_Alz_LOAD_WGS_20230605_ANV5_202306131256', 1049, pop),
    mksrc('datarepo-9626b3eb', 'ANVIL_CCDG_NYGC_NP_Alz_WHICAP_WGS_20230605_ANV5_202306131303', 148, pop),
    mksrc('datarepo-86bb81c0', 'ANVIL_CCDG_NYGC_NP_Autism_ACE2_GRU_MDS_WGS_20230605_ANV5_202403032029', 839),
    mksrc('datarepo-85674dce', 'ANVIL_CCDG_NYGC_NP_Autism_AGRE_WGS_20230605_ANV5_202403081651', 4601),
    mksrc('datarepo-7d1461b2', 'ANVIL_CCDG_NYGC_NP_Autism_SSC_WGS_20230605_ANV5_202403032206', 9340),
    mksrc('datarepo-25ec7b57', 'ANVIL_CCDG_WASHU_PAGE_20221220_ANV5_202304271544', 690, pop),
    mksrc('datarepo-15645b8d', 'ANVIL_CCDG_WashU_CVD_EOCAD_WashU_CAD_DS_WGS_20230525_ANV5_202403040118', 611),
    mksrc('datarepo-4a0769c7', 'ANVIL_CCDG_WashU_CVD_EOCAD_WashU_CAD_GRU_IRB_WGS_20230525_ANV5_202403040126', 381),
    mksrc('datarepo-b9222139', 'ANVIL_CMG_BROAD_BRAIN_ENGLE_WES_20240205_ANV5_202402051624', 473, pop),
    mksrc('datarepo-7e094253', 'ANVIL_CMG_BROAD_BRAIN_SHERR_WGS_20221102_ANV5_202402281543', 3, pop),
    mksrc('datarepo-c797490f', 'ANVIL_CMG_BROAD_ORPHAN_SCOTT_WGS_20221102_ANV5_202402281552', 15, pop),
    mksrc('datarepo-0a21cbfd', 'ANVIL_CMG_BaylorHopkins_HMB_IRB_NPU_WES_20221020_ANV5_202402290528', 2223),
    mksrc('datarepo-d321333c', 'ANVIL_CMG_BaylorHopkins_HMB_NPU_WES_20230525_ANV5_202402290537', 4804),
    mksrc('datarepo-0a1360b1', 'ANVIL_CMG_Broad_Blood_Gazda_WES_20221117_ANV5_202402290547', 612, pop),
    mksrc('datarepo-faa71b49', 'ANVIL_CMG_Broad_Blood_Sankaran_WES_20221117_ANV5_202402290555', 1141, pop),
    mksrc('datarepo-abce6387', 'ANVIL_CMG_Broad_Blood_Sankaran_WGS_20221117_ANV5_202402290606', 96, pop),
    mksrc('datarepo-3dd4d039', 'ANVIL_CMG_Broad_Brain_Gleeson_WES_20221117_ANV5_202304241517', 1180, pop),
    mksrc('datarepo-c361373f', 'ANVIL_CMG_Broad_Brain_Muntoni_WES_20221102_ANV5_202304241527', 39, pop),
    mksrc('datarepo-fc6ce406', 'ANVIL_CMG_Broad_Brain_NeuroDev_WES_20240112_ANV5_202401152208', 296),
    mksrc('datarepo-d7bfafc6', 'ANVIL_CMG_Broad_Brain_Thaker_WES_20221102_ANV5_202304241531', 46, pop),
    mksrc('datarepo-7e03b5fd', 'ANVIL_CMG_Broad_Brain_Walsh_WES_20230605_ANV5_202310101734', 2778, pop),
    mksrc('datarepo-29812b42', 'ANVIL_CMG_Broad_Eye_Pierce_WES_20221205_ANV5_202304242250', 2150, pop),
    mksrc('datarepo-48134558', 'ANVIL_CMG_Broad_Eye_Pierce_WGS_20221117_ANV5_202304241507', 35, pop),
    mksrc('datarepo-36ebaa12', 'ANVIL_CMG_Broad_Heart_PCGC_Tristani_WGS_20221025_ANV5_202304211840', 214, pop),
    mksrc('datarepo-f9826139', 'ANVIL_CMG_Broad_Heart_Seidman_WES_20221117_ANV5_202304241504', 133, pop),
    mksrc('datarepo-85952af8', 'ANVIL_CMG_Broad_Kidney_Hildebrandt_WES_20230525_ANV5_202305251733', 3544, pop),
    mksrc('datarepo-ee4ae9a1', 'ANVIL_CMG_Broad_Kidney_Hildebrandt_WGS_20221025_ANV5_202304211844', 27, pop),
    mksrc('datarepo-cf168274', 'ANVIL_CMG_Broad_Kidney_Pollak_WES_20221025_ANV5_202304211846', 147, pop),
    mksrc('datarepo-4d47ba2c', 'ANVIL_CMG_Broad_Muscle_Beggs_WGS_20221102_ANV5_202304241533', 141, pop),
    mksrc('datarepo-82d1271a', 'ANVIL_CMG_Broad_Muscle_Bonnemann_WES_20221117_ANV5_202304241509', 305, pop),
    mksrc('datarepo-6be3fb25', 'ANVIL_CMG_Broad_Muscle_Bonnemann_WGS_20221117_ANV5_202304241510', 152, pop),
    mksrc('datarepo-b168eb10', 'ANVIL_CMG_Broad_Muscle_KNC_WES_20221116_ANV5_202304242219', 169, pop),
    mksrc('datarepo-372244aa', 'ANVIL_CMG_Broad_Muscle_KNC_WGS_20221117_ANV5_202304242221', 16, pop),
    mksrc('datarepo-c43e7400', 'ANVIL_CMG_Broad_Muscle_Kang_WES_20230525_ANV5_202310101649', 121, pop),
    mksrc('datarepo-77a6c0aa', 'ANVIL_CMG_Broad_Muscle_Kang_WGS_20221025_ANV5_202304211849', 8, pop),
    mksrc('datarepo-4153ad1f', 'ANVIL_CMG_Broad_Muscle_Laing_WES_20221208_ANV5_202402291926', 31, pop),
    mksrc('datarepo-5019143b', 'ANVIL_CMG_Broad_Muscle_Myoseq_WES_20230621_ANV5_202306211852', 1382, pop),
    mksrc('datarepo-27eb651a', 'ANVIL_CMG_Broad_Muscle_Myoseq_WGS_20221208_ANV5_202304271310', 10, pop),
    mksrc('datarepo-c087af7a', 'ANVIL_CMG_Broad_Muscle_OGrady_WES_20221205_ANV5_202304242252', 226, pop),
    mksrc('datarepo-db987a2e', 'ANVIL_CMG_Broad_Muscle_Ravenscroft_WES_20221208_ANV5_202304271311', 140, pop),
    mksrc('datarepo-05df566c', 'ANVIL_CMG_Broad_Muscle_Topf_WES_20221208_ANV5_202304271313', 2408, pop),
    mksrc('datarepo-87d91f06', 'ANVIL_CMG_Broad_Orphan_Chung_WES_20221102_ANV5_202304241534', 71, pop),
    mksrc('datarepo-25f6b696', 'ANVIL_CMG_Broad_Orphan_Estonia_Ounap_WES_20221117_ANV5_202304241512', 107, pop),
    mksrc('datarepo-c3b16b41', 'ANVIL_CMG_Broad_Orphan_Estonia_Ounap_WGS_20221205_ANV5_202304242255', 427, pop),
    mksrc('datarepo-5bbb5a28', 'ANVIL_CMG_Broad_Orphan_Jueppner_WES_20240205_ANV5_202402051640', 11, pop),
    mksrc('datarepo-32fe2260', 'ANVIL_CMG_Broad_Orphan_Lerner_Ellis_WES_20221102_ANV5_202304241536', 11, pop),
    mksrc('datarepo-6f9e574e', 'ANVIL_CMG_Broad_Orphan_Manton_WES_20221117_ANV5_202304241513', 1254, pop),
    mksrc('datarepo-53cd689b', 'ANVIL_CMG_Broad_Orphan_Manton_WGS_20221117_ANV5_202304241515', 64, pop),
    mksrc('datarepo-e7c5babf', 'ANVIL_CMG_Broad_Orphan_Scott_WES_20221025_ANV5_202304241458', 237, pop),
    mksrc('datarepo-051877f4', 'ANVIL_CMG_Broad_Orphan_Sweetser_WES_20221102_ANV5_202304241539', 3, pop),
    mksrc('datarepo-555c7706', 'ANVIL_CMG_Broad_Orphan_VCGS_White_WES_20221018_ANV5_202304241522', 1526, pop),
    mksrc('datarepo-3a8f7952', 'ANVIL_CMG_Broad_Orphan_VCGS_White_WGS_20221117_ANV5_202304241523', 156, pop),
    mksrc('datarepo-b699c5e3', 'ANVIL_CMG_Broad_Rare_RGP_WES_20221102_ANV5_202304241540', 6, pop),
    mksrc('datarepo-2d5bd095', 'ANVIL_CMG_Broad_Stillbirth_Wilkins_Haug_WES_20221102_ANV5_202304241542', 60, pop),
    mksrc('datarepo-db7353fb', 'ANVIL_CMG_UWASH_DS_BAV_IRB_PUB_RD_20230419_ANV5_202304201858', 181, pop),
    mksrc('datarepo-3b8ef67a', 'ANVIL_CMG_UWASH_DS_BDIS_20230418_ANV5_202304201958', 10, pop),
    mksrc('datarepo-5d27ebfe', 'ANVIL_CMG_UWASH_DS_HFA_20230418_ANV5_202304201932', 198, pop),
    mksrc('datarepo-9d1a6e0a', 'ANVIL_CMG_UWASH_DS_NBIA_20230418_ANV5_202304201949', 110, pop),
    mksrc('datarepo-18bd3df4', 'ANVIL_CMG_UWASH_HMB_20230418_ANV5_202402070029', 610, pop),
    mksrc('datarepo-50484f86', 'ANVIL_CMG_UWASH_HMB_IRB_20230418_ANV5_202304201915', 45, pop),
    mksrc('datarepo-74bd0964', 'ANVIL_CMG_UWash_DS_EP_20230419_ANV5_202304201906', 53, pop),
    mksrc('datarepo-6f4155f2', 'ANVIL_CMG_UWash_GRU_20240301_ANV5_202403040330', 7415, pop),
    mksrc('datarepo-6486ae96', 'ANVIL_CMG_UWash_GRU_1_20240113_ANV5_202401141440', 3005, pop),
    mksrc('datarepo-97ec5366', 'ANVIL_CMG_UWash_GRU_IRB_20230418_ANV5_202304201940', 563, pop),
    mksrc('datarepo-cb305c8e', 'ANVIL_CMG_YALE_DS_MC_20221026_ANV5_202402281611', 748),
    mksrc('datarepo-c2897355', 'ANVIL_CMG_Yale_DS_BPEAKD_20240113_ANV5_202401141447', 101),
    mksrc('datarepo-4b5667f8', 'ANVIL_CMG_Yale_DS_RD_20240113_ANV5_202401141453', 100),
    mksrc('datarepo-9e86cb23', 'ANVIL_CMG_Yale_DS_THAL_IRB_20240113_ANV5_202401141500', 359),
    mksrc('datarepo-278252c3', 'ANVIL_CMG_Yale_HMB_IRB_20240113_ANV5_202401141507', 48),
    mksrc('datarepo-eea2a20c', 'ANVIL_CMH_GAFK_10X_Genomics_20240304_ANV5_202403071539', 312),
    mksrc('datarepo-0e0bf0f8', 'ANVIL_CMH_GAFK_ES_20240301_ANV5_202403040338', 26829),
    mksrc('datarepo-9935aa3f', 'ANVIL_CMH_GAFK_IlluminaGSA_20240311_ANV5_202403121355', 13940),
    mksrc('datarepo-d391ce5f', 'ANVIL_CMH_GAFK_IsoSeq_20240113_ANV5_202402062116', 415),
    mksrc('datarepo-beef6734', 'ANVIL_CMH_GAFK_MGI_20240304_ANV5_202403071559', 1609),
    mksrc('datarepo-8599b1fb', 'ANVIL_CMH_GAFK_PacBio_methyl_tagged_20240311_ANV5_202403121402', 793),
    mksrc('datarepo-94f58e6c', 'ANVIL_CMH_GAFK_SCATAC_20221107_ANV5_202402290954', 322),
    mksrc('datarepo-5447de30', 'ANVIL_CMH_GAFK_WGBS_20230327_ANV5_202402062120', 1475),
    mksrc('datarepo-db73a316', 'ANVIL_CMH_GAFK_WGS_20240113_ANV5_202402062123', 23848),
    mksrc('datarepo-5227851b', 'ANVIL_CSER_ClinSeq_GRU_20240401_ANV5_202404081541', 23),
    mksrc('datarepo-1a706b0c', 'ANVIL_GTEx_Somatic_WGS_20240116_ANV5_202401170147', 708, pop),
    mksrc('datarepo-8a98bcb4', 'ANVIL_NIMH_Broad_ConvNeuro_McCarroll_Nehme_Levy_CIRM_DS_Village_20240405_ANV5_202404081511', 495), # noqa E501
    mksrc('datarepo-c02a5efb', 'ANVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_CIRM_GRU_VillageData_20230109_ANV5_202402292203', 1357), # noqa E501
    mksrc('datarepo-817f27aa', 'ANVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_CIRM_GRU_WGS_20240206_ANV5_202402081755', 678), # noqa E501
    mksrc('datarepo-ddc1d72b', 'ANVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_Finkel_SMA_DS_WGS_20230109_ANV5_202402292209', 3), # noqa E501
    mksrc('datarepo-14f5afa3', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Braff_DS_10XLRGenomes_20221115_ANV5_202310101713', 561, pop), # noqa E501
    mksrc('datarepo-69e4bc19', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_COGS_DS_WGS_20240113_ANV5_202401152215', 611),
    mksrc('datarepo-da595e23', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Escamilla_DS_WGS_20240112_ANV5_202401141541', 340),
    mksrc('datarepo-94091a22', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Pato_GRU_10XLRGenomes_20230331_ANV5_202310101715', 368, pop), # noqa E501
    mksrc('datarepo-df20901c', 'ANVIL_NIMH_Broad_WGSPD_1_McCarroll_Braff_DS_WGS_20240304_ANV5_202403071610', 213),
    mksrc('datarepo-75e17b99', 'ANVIL_NIMH_CIRM_FCDI_ConvergentNeuro_McCarroll_Eggan_GRU_Arrays_20230109_ANV5_202402292215', 6510), # noqa E501
    mksrc('datarepo-25790186', 'ANVIL_PAGE_BioMe_GRU_WGS_20221128_ANV5_202403040429', 308, pop),
    mksrc('datarepo-b371989b', 'ANVIL_PAGE_MEC_GRU_WGS_20230131_ANV5_202403040437', 70, pop),
    mksrc('datarepo-4a4eec27', 'ANVIL_PAGE_SoL_HMB_WGS_20221220_ANV5_202403040445', 234, pop),
    mksrc('datarepo-a1f917db', 'ANVIL_PAGE_Stanford_Global_Reference_Panel_GRU_WGS_20221128_ANV5_202403040453', 78, pop), # noqa E501
    mksrc('datarepo-6264931f', 'ANVIL_PAGE_WHI_HMB_IRB_WGS_20221019_ANV5_202403040500', 235, pop),
    mksrc('datarepo-f3817357', 'ANVIL_ccdg_asc_ndd_daly_talkowski_AGRE_asd_exome_20221102_ANV5_202403040528', 850),
    mksrc('datarepo-23635d1c', 'ANVIL_ccdg_asc_ndd_daly_talkowski_IBIS_asd_exome_20221024_ANV5_202403040537', 241),
    mksrc('datarepo-ecf311e7', 'ANVIL_ccdg_asc_ndd_daly_talkowski_TASC_asd_exome_20221117_ANV5_202403040544', 3175),
    mksrc('datarepo-90923a9d', 'ANVIL_ccdg_asc_ndd_daly_talkowski_aleksic_asd_exome_20231013_ANV5_202403040600', 701),
    mksrc('datarepo-2354d65a', 'ANVIL_ccdg_asc_ndd_daly_talkowski_cdcseed_asd_gsa_md_20221024_ANV5_202402291144', 2888, pop), # noqa E501
    mksrc('datarepo-efc0eb70', 'ANVIL_ccdg_asc_ndd_daly_talkowski_gargus_asd_exome_20231013_ANV5_202403040645', 131),
    mksrc('datarepo-d1f95953', 'ANVIL_ccdg_asc_ndd_daly_talkowski_gurrieri_asd_exome_20221024_ANV5_202402291153', 61),
    mksrc('datarepo-5590427b', 'ANVIL_ccdg_asc_ndd_daly_talkowski_mayo_asd_exome_20221024_ANV5_202402291115', 337),
    mksrc('datarepo-3cbe3dd3', 'ANVIL_ccdg_asc_ndd_daly_talkowski_mcpartland_asd_exome_20221116_ANV5_202403040753', 6214), # noqa E501
    mksrc('datarepo-a245d786', 'ANVIL_ccdg_asc_ndd_daly_talkowski_palotie_asd_exome_20221019_ANV5_202403040815', 155, pop), # noqa E501
    mksrc('datarepo-104705f5', 'ANVIL_ccdg_asc_ndd_daly_talkowski_passos_bueno_asd_exome_20221108_ANV5_202403040831', 948), # noqa E501
    mksrc('datarepo-a07262c0', 'ANVIL_ccdg_asc_ndd_daly_talkowski_pericak_vance_asd_exome__20221025_ANV5_202403040839', 110), # noqa E501
    mksrc('datarepo-418e64c1', 'ANVIL_ccdg_asc_ndd_daly_talkowski_persico_asd_exome_20221027_ANV5_202403040854', 533),
    mksrc('datarepo-cfe20662', 'ANVIL_ccdg_asc_ndd_daly_talkowski_renieri_asd_exome_20230327_ANV5_202403040909', 777),
    mksrc('datarepo-7c668a5c', 'ANVIL_ccdg_asc_ndd_daly_talkowski_schloesser_asd_exome_20230324_ANV5_202403040917', 156), # noqa E501
    mksrc('datarepo-0b4c3cfb', 'ANVIL_ccdg_asc_ndd_daly_talkowski_schloesser_asd_gsa_md_20221025_ANV5_202402291202', 158, pop), # noqa E501
    mksrc('datarepo-2571477f', 'ANVIL_ccdg_broad_ai_ibd_daly_burnstein_gsa_20240103_ANV5_202401112154', 396),
    mksrc('datarepo-c0abacf6', 'ANVIL_ccdg_broad_ai_ibd_daly_chen_gsa_20240103_ANV5_202401112202', 96),
    mksrc('datarepo-c7473b33', 'ANVIL_ccdg_broad_ai_ibd_daly_chen_wes_20240103_ANV5_202403040940', 84),
    mksrc('datarepo-ac30439c', 'ANVIL_ccdg_broad_ai_ibd_daly_cho_niddk_gsa_20240103_ANV5_202401112215', 10725),
    mksrc('datarepo-267ea46f', 'ANVIL_ccdg_broad_ai_ibd_daly_chung_gider_gsa_20240103_ANV5_202401121413', 3773),
    mksrc('datarepo-c481c20f', 'ANVIL_ccdg_broad_ai_ibd_daly_chung_gider_wes_20240103_ANV5_202403040947', 2512),
    mksrc('datarepo-938f9e89', 'ANVIL_ccdg_broad_ai_ibd_daly_faubion_share_gsa_20240104_ANV5_202401121427', 5442),
    mksrc('datarepo-d4b1264d', 'ANVIL_ccdg_broad_ai_ibd_daly_faubion_share_wes_20240104_ANV5_202403040954', 3616),
    mksrc('datarepo-4d149951', 'ANVIL_ccdg_broad_ai_ibd_daly_franchimont_gsa_20240104_ANV5_202401121441', 16964),
    mksrc('datarepo-e12ce5bd', 'ANVIL_ccdg_broad_ai_ibd_daly_franchimont_wes_20240104_ANV5_202403041001', 12167),
    mksrc('datarepo-2c7e5905', 'ANVIL_ccdg_broad_ai_ibd_daly_hyams_protect_gsa_20240311_ANV5_202403121623', 2096),
    mksrc('datarepo-f5463526', 'ANVIL_ccdg_broad_ai_ibd_daly_kastner_fmf_gsa_20240104_ANV5_202401121503', 168),
    mksrc('datarepo-51367192', 'ANVIL_ccdg_broad_ai_ibd_daly_kastner_fmf_nhgri_wes_20240104_ANV5_202401152230', 28),
    mksrc('datarepo-7268c3a0', 'ANVIL_ccdg_broad_ai_ibd_daly_kupcinskas_gsa_20240311_ANV5_202403121627', 11472),
    mksrc('datarepo-51449a60', 'ANVIL_ccdg_broad_ai_ibd_daly_lira_share_wes_20240104_ANV5_202403041035', 220),
    mksrc('datarepo-ee1b3121', 'ANVIL_ccdg_broad_ai_ibd_daly_louis_gsa_20240311_ANV5_202403121633', 8298),
    mksrc('datarepo-083044ec', 'ANVIL_ccdg_broad_ai_ibd_daly_newberry_share_gsa_20240105_ANV5_202401121611', 5229),
    mksrc('datarepo-10ae29e5', 'ANVIL_ccdg_broad_ai_ibd_daly_newberry_share_wes_20240105_ANV5_202403041117', 3668),
    mksrc('datarepo-a240ffda', 'ANVIL_ccdg_broad_ai_ibd_daly_niddk_cho_wes_20240105_ANV5_202403041125', 10907),
    mksrc('datarepo-929acb2a', 'ANVIL_ccdg_broad_ai_ibd_daly_rioux_bitton_igenomed_wes_20240105_ANV5_202401121701', 183), # noqa E501
    mksrc('datarepo-fa70ba86', 'ANVIL_ccdg_broad_ai_ibd_daly_rioux_genizon_wes_20240311_ANV5_202403121426', 1923),
    mksrc('datarepo-6e9030de', 'ANVIL_ccdg_broad_ai_ibd_daly_rioux_igenomed_gsa_20240105_ANV5_202401121709', 1118),
    mksrc('datarepo-c9265cf7', 'ANVIL_ccdg_broad_ai_ibd_daly_rioux_niddk_gsa_20240108_ANV5_202401121716', 5520),
    mksrc('datarepo-fe283248', 'ANVIL_ccdg_broad_ai_ibd_daly_rioux_niddk_wes_20240108_ANV5_202403041140', 6829),
    mksrc('datarepo-3ca098f3', 'ANVIL_ccdg_broad_ai_ibd_daly_sands_msccr_gsa_20240108_ANV5_202401121730', 8790),
    mksrc('datarepo-fd47ae7f', 'ANVIL_ccdg_broad_ai_ibd_daly_sands_msccr_wes_20240108_ANV5_202403041148', 5745),
    mksrc('datarepo-4300fbc6', 'ANVIL_ccdg_broad_ai_ibd_daly_silverberg_niddk_gsa_20240108_ANV5_202401121745', 14654),
    mksrc('datarepo-14285871', 'ANVIL_ccdg_broad_ai_ibd_daly_stampfer_nhs_gsa_20240311_ANV5_202403121637', 9597),
    mksrc('datarepo-d69ac752', 'ANVIL_ccdg_broad_ai_ibd_daly_stampfer_wes_20240108_ANV5_202403041155', 6506),
    mksrc('datarepo-268dabf8', 'ANVIL_ccdg_broad_ai_ibd_daly_vermeire_gsa_20240113_ANV5_202402062145', 24162),
    mksrc('datarepo-636bc565', 'ANVIL_ccdg_broad_ai_ibd_daly_vermeire_wes_20240108_ANV5_202403041203', 18885),
    mksrc('datarepo-7cc92556', 'ANVIL_ccdg_broad_ai_ibd_daly_xavier_prism_gsa_20240108_ANV5_202402062149', 3850),
    mksrc('datarepo-6b12cac1', 'ANVIL_ccdg_broad_ai_ibd_daly_xavier_prism_wes_20240108_ANV5_202403041214', 35516),
    mksrc('datarepo-5d4e150c', 'ANVIL_ccdg_broad_ai_ibd_daly_xavier_share_gsa_20240108_ANV5_202401121819', 4255),
    mksrc('datarepo-e30e7797', 'ANVIL_ccdg_broad_ai_ibd_daly_xavier_share_wes_20240108_ANV5_202403041224', 2934),
    mksrc('datarepo-597e5f25', 'ANVIL_ccdg_broad_ai_ibd_niddk_daly_duerr_wes_20240112_ANV5_202403041241', 1891),
    mksrc('datarepo-2f8b185b', 'ANVIL_ccdg_broad_ai_ibd_niddk_daly_silverberg_wes_20240112_ANV5_202403041250', 2465),
    mksrc('datarepo-7a0883a4', 'ANVIL_ccdg_broad_cvd_af_pegasus_hmb_20221025_ANV5_202403030736', 7483, pop),
    mksrc('datarepo-f62c5ebd', 'ANVIL_ccdg_broad_cvd_eocad_promis_wgs_20221213_ANV5_202403030935', 1136, pop),
    mksrc('datarepo-9d116a5c', 'ANVIL_ccdg_broad_mi_atvb_ds_cvd_wes_20221025_ANV5_202403031035', 60, pop),
    mksrc('datarepo-6c0a5f0d', 'ANVIL_ccdg_broad_mi_univutah_ds_cvd_wes_20221026_ANV5_202403031059', 1913),
    mksrc('datarepo-235663ab', 'ANVIL_ccdg_broad_np_epilepsy_usavancontrols_hmb_gso_wes_20221101_ANV5_202403031924', 1975), # noqa E501
    mksrc('datarepo-81cf50b1', 'ANVIL_ccdg_broad_np_epilepsy_zafagn_ds_epi_como_mds_wes_20221026_ANV5_202403031933', 507), # noqa E501
    mksrc('datarepo-e6801146', 'ANVIL_ccdg_nygc_np_autism_hmca_wgs_20221024_ANV5_202403032115', 724),
    mksrc('datarepo-64b26798', 'ANVIL_ccdg_washu_ai_t1d_t1dgc_wgs_20221031_ANV5_202403032311', 3397),
    mksrc('datarepo-e3065356', 'ANVIL_ccdg_washu_cvd_eocad_biome_wgs_20221024_ANV5_202304211601', 648, pop),
    mksrc('datarepo-01e3396c', 'ANVIL_ccdg_washu_cvd_eocad_cleveland_wgs_20221024_ANV5_202403040008', 348),
    mksrc('datarepo-5e62ca4f', 'ANVIL_ccdg_washu_cvd_eocad_emerge_wgs_20221024_ANV5_202403040026', 277),
    mksrc('datarepo-a0d77559', 'ANVIL_ccdg_washu_cvd_eocad_emory_wgs_20221024_ANV5_202403040034', 430),
    mksrc('datarepo-33e3428b', 'ANVIL_ccdg_washu_cvd_np_ai_controls_vccontrols_wgs_20221024_ANV5_202403032319', 112, pop), # noqa E501
    mksrc('datarepo-17c5f983', 'ANVIL_cmg_broad_brain_engle_wgs_20221202_ANV5_202402290614', 95, pop),
    mksrc('datarepo-1cb73890', 'ANVIL_cmg_broad_heart_ware_wes_20221215_ANV5_202304242145', 40, pop),
    mksrc('datarepo-833ff0a3', 'ANVIL_eMERGE_GRU_IRB_NPU_eMERGEseq_20230130_ANV5_202304271614', 2569, pop),
    mksrc('datarepo-baf040af', 'ANVIL_eMERGE_GRU_IRB_PUB_NPU_eMERGEseq_20230130_ANV5_202304271616', 3017, pop),
    mksrc('datarepo-270b3b62', 'ANVIL_eMERGE_GRU_IRB_eMERGEseq_20230130_ANV5_202304271613', 2432, pop),
    mksrc('datarepo-c13efbe9', 'ANVIL_eMERGE_GRU_NPU_eMERGEseq_20230130_ANV5_202304271617', 5907, pop),
    mksrc('datarepo-34f8138d', 'ANVIL_eMERGE_GRU_eMERGEseq_20230130_ANV5_202304271612', 2971, pop),
    mksrc('datarepo-90b7b6e8', 'ANVIL_eMERGE_HMB_GSO_eMERGEseq_20230130_ANV5_202304271621', 2491, pop),
    mksrc('datarepo-6e6dca92', 'ANVIL_eMERGE_HMB_IRB_PUB_eMERGEseq_20230130_ANV5_202304271622', 478, pop),
    mksrc('datarepo-1ddf2a8e', 'ANVIL_eMERGE_HMB_NPU_eMERGEseq_20230130_ANV5_202304271624', 2488, pop),
    mksrc('datarepo-dba97a65', 'ANVIL_eMERGE_HMB_eMERGEseq_20230130_ANV5_202304271619', 2486, pop),
    mksrc('datarepo-51aa9a22', 'ANVIL_eMERGE_PGRNseq_20230118_ANV5_202304241853', 9027, pop),
    mksrc('datarepo-ce8c469f', 'ANVIL_eMERGE_PRS_Arrays_20221220_ANV5_202304271346', 7296, pop),
    mksrc('datarepo-bf91a039', 'ANVIL_nhgri_broad_ibd_daly_winter_wes_20240112_ANV5_202403041315', 414),
    # @formatter:on
]))

anvil7_sources = mkdict(anvil6_sources, 257, mkdelta([
    mksrc('datarepo-c9e438dc', 'ANVIL_CCDG_Broad_NP_Epilepsy_GBRUCL_DS_EARET_MDS_WES_20221026_ANV5_202406261957', 686),
    mksrc('datarepo-90a1d452', 'ANVIL_GREGoR_R01_GRU_20240208_ANV5_202407011515', 2473),
    mksrc('datarepo-c27c13db', 'ANVIL_GREGoR_R01_HMB_20240208_ANV5_202407011529', 222),
    mksrc('datarepo-3594cc06', 'ANVIL_HPRC_20240401_ANV5_202406261913', 63201),
    mksrc('datarepo-49f55ff6', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Light_DS_WGS_20240625_ANV5_202406262032', 60),
    mksrc('datarepo-54040f7f', 'ANVIL_T2T_CHRY_20240301_ANV5_202406271432', 309979),
    mksrc('datarepo-5048eadd', 'ANVIL_ccdg_broad_ai_ibd_daly_brant_burnstein_utsw_wes_20240627_ANV5_202406271535', 66),
    mksrc('datarepo-5d003f44', 'ANVIL_ccdg_broad_daly_igsr_1kg_twist_wes_20240625_ANV5_202406261904', 670)
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
        # Set variables for the `anvilprod` (short for AnVIL production)
        # deployment here.
        #
        # Only modify this file if you intend to commit those changes. To change the
        # environment with a setting that's specific to you AND the deployment, create
        # a environment.local.py right next to this file and make your changes there.
        # Settings applicable to all environments but specific to you go into
        # environment.local.py at the project root.

        'AZUL_DEPLOYMENT_STAGE': 'anvilprod',

        'AZUL_DOMAIN_NAME': 'explore.anvilproject.org',
        'AZUL_PRIVATE_API': '0',

        'AZUL_CATALOGS': json.dumps({
            f'{catalog}{suffix}': dict(atlas=atlas,
                                       internal=internal,
                                       plugins=dict(metadata=dict(name='anvil'),
                                                    repository=dict(name='tdr_anvil')),
                                       sources=list(filter(None, sources.values())))
            for atlas, catalog, sources in [
                ('anvil', 'anvil7', anvil7_sources),
            ]
            for suffix, internal in [
                ('', False),
                ('-it', True)
            ]
        }),

        'AZUL_TDR_SOURCE_LOCATION': 'us-central1',
        'AZUL_TDR_SERVICE_URL': 'https://data.terra.bio',
        'AZUL_SAM_SERVICE_URL': 'https://sam.dsde-prod.broadinstitute.org',
        'AZUL_DUOS_SERVICE_URL': 'https://consent.dsde-prod.broadinstitute.org',
        'AZUL_TERRA_SERVICE_URL': 'https://firecloud-orchestration.dsde-prod.broadinstitute.org',

        'AZUL_ENABLE_MONITORING': '1',

        # $0.382/h × 6 × 24h/d × 30d/mo = 1,650.24/mo
        'AZUL_ES_INSTANCE_TYPE': 'r6gd.xlarge.elasticsearch',
        'AZUL_ES_INSTANCE_COUNT': '6',

        'AZUL_CONTRIBUTION_CONCURRENCY': '300/64',

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
