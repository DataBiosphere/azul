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

anvil1_sources = mkdict(anvil_sources, 67, mkdelta([
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
    mksrc('datarepo-45487b69', 'ANVIL_GTEx_Somatic_WGS_20230331_ANV5_202304211636', 707),
    mksrc('datarepo-5ebc368c', 'ANVIL_GTEx_V7_hg19_20221128_ANV5_202304211804', 15974),
    mksrc('datarepo-864913f2', 'ANVIL_GTEx_V9_hg38_20221128_ANV5_202304211853', 8298),
    mksrc('datarepo-b093b69d', 'ANVIL_GTEx_public_data_20221115_ANV5_202304211659', 81),
    mksrc('datarepo-d948d21a', 'ANVIL_cmg_broad_brain_engle_wgs_20221202_ANV5_202304271345', 95),
    mksrc('datarepo-1cb73890', 'ANVIL_cmg_broad_heart_ware_wes_20221215_ANV5_202304242145', 40),
]))

anvil2_sources = mkdict(anvil1_sources, 112, mkdelta([
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
    mksrc('datarepo-f2af4233', 'ANVIL_CCDG_WashU_CVD_PAGE_HMB_NPU_WGS_20221025_ANV5_202304211801', 38),
    mksrc('datarepo-6d8536f4', 'ANVIL_CMH_GAFK_GS_linked_read_20221107_ANV5_202304211527', 626),
    mksrc('datarepo-482ab960', 'ANVIL_CMH_GAFK_GS_long_read_20221109_ANV5_202304211529', 777),
    mksrc('datarepo-3b296131', 'ANVIL_CMH_GAFK_SCATAC_20221107_ANV5_202304211531', 322),
    mksrc('datarepo-8acbf07f', 'ANVIL_CMH_GAFK_WGBS_20230327_ANV5_202304211534', 1475),
    mksrc('datarepo-8745e97d', 'ANVIL_CMH_GAFK_scRNA_20221107_ANV5_202304211533', 198),
    mksrc('datarepo-1c89dcac', 'ANVIL_CSER_CHARM_GRU_20221208_ANV5_202304271348', 2392),
    mksrc('datarepo-12d56848', 'ANVIL_CSER_NCGENES2_GRU_20221208_ANV5_202304271349', 104),
    mksrc('datarepo-8a4d67ef', 'ANVIL_CSER_SouthSeq_GRU_20221208_ANV5_202304271351', 800),
    mksrc('datarepo-f622180d', 'ANVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_CIRM_GRU_VillageData_20230109_ANV5_202304242045', 71),  # noqa E501
    mksrc('datarepo-732d1a55', 'ANVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_CIRM_GRU_WGS_20230109_ANV5_202304242048', 445),  # noqa E501
    mksrc('datarepo-90bab913', 'ANVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_Finkel_SMA_DS_WGS_20230109_ANV5_202304242043', 3),  # noqa E501
    mksrc('datarepo-e4eb7641', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Braff_DS_WGS_20221115_ANV5_202304242052', 864),
    mksrc('datarepo-f9aef3dc', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Escamilla_DS_WGS_20221103_ANV5_202304242049', 85),
    mksrc('datarepo-7c00e8e5', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Pato_GRU_WGS_20221115_ANV5_202304242056', 8084),
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

anvil3_sources = mkdict(anvil2_sources, 160, mkdelta([
    # @formatter:off
    mksrc('datarepo-9a74aed3', 'ANVIL_CCDG_Baylor_CVD_ARIC_20231008_ANV5_202310091900', 10012),
    mksrc('datarepo-a749913a', 'ANVIL_CCDG_Baylor_CVD_EOCAD_SoL_WGS_20230418_ANV5_202310101651', 9225),
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
                ('anvil', 'anvil3', anvil3_sources),
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

        # $0.382/h × 4 × 24h/d × 30d/mo = $1100.16/mo
        'AZUL_ES_INSTANCE_TYPE': 'r6gd.xlarge.elasticsearch',
        'AZUL_ES_INSTANCE_COUNT': '4',

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

        'AZUL_ENABLE_REPLICAS': '0',
    }
