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
    project = '_'.join(snapshot.split('_')[1:-3])
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


anvil_sources = mkdict({}, 11, mkdelta([
    mksrc('bigquery', 'datarepo-3edb7fb1', 'ANVIL_1000G_high_coverage_2019_20230517_ANV5_202305181946'),
    mksrc('bigquery', 'datarepo-db7353fb', 'ANVIL_CMG_UWASH_DS_BAV_IRB_PUB_RD_20230419_ANV5_202304201858'),
    mksrc('bigquery', 'datarepo-3b8ef67a', 'ANVIL_CMG_UWASH_DS_BDIS_20230418_ANV5_202304201958'),
    mksrc('bigquery', 'datarepo-5d27ebfe', 'ANVIL_CMG_UWASH_DS_HFA_20230418_ANV5_202304201932'),
    mksrc('bigquery', 'datarepo-9d1a6e0a', 'ANVIL_CMG_UWASH_DS_NBIA_20230418_ANV5_202304201949'),
    mksrc('bigquery', 'datarepo-3243df15', 'ANVIL_CMG_UWASH_HMB_20230418_ANV5_202304201923'),
    mksrc('bigquery', 'datarepo-50484f86', 'ANVIL_CMG_UWASH_HMB_IRB_20230418_ANV5_202304201915'),
    mksrc('bigquery', 'datarepo-74bd0964', 'ANVIL_CMG_UWash_DS_EP_20230419_ANV5_202304201906'),
    mksrc('bigquery', 'datarepo-e5914f89', 'ANVIL_CMG_UWash_GRU_20230418_ANV5_202304201848'),
    mksrc('bigquery', 'datarepo-97ec5366', 'ANVIL_CMG_UWash_GRU_IRB_20230418_ANV5_202304201940'),
    mksrc('bigquery', 'datarepo-4150bd87', 'ANVIL_GTEx_V8_hg38_20230419_ANV5_202304202007')
]))

anvil1_sources = mkdict(anvil_sources, 63, mkdelta([
    mksrc('bigquery', 'datarepo-d53aa186', 'ANVIL_CMG_BROAD_BRAIN_ENGLE_WES_20221102_ANV5_202304241525'),
    mksrc('bigquery', 'datarepo-69b2535a', 'ANVIL_CMG_BROAD_BRAIN_SHERR_WGS_20221102_ANV5_202304241530'),
    mksrc('bigquery', 'datarepo-490be510', 'ANVIL_CMG_BROAD_ORPHAN_SCOTT_WGS_20221102_ANV5_202304241538'),
    mksrc('bigquery', 'datarepo-3b33c41b', 'ANVIL_CMG_Broad_Blood_Gazda_WES_20221117_ANV5_202304241459'),
    mksrc('bigquery', 'datarepo-96df3cea', 'ANVIL_CMG_Broad_Blood_Sankaran_WES_20221117_ANV5_202304241501'),
    mksrc('bigquery', 'datarepo-179ee079', 'ANVIL_CMG_Broad_Blood_Sankaran_WGS_20221117_ANV5_202304241503'),
    mksrc('bigquery', 'datarepo-3dd4d039', 'ANVIL_CMG_Broad_Brain_Gleeson_WES_20221117_ANV5_202304241517'),
    mksrc('bigquery', 'datarepo-c361373f', 'ANVIL_CMG_Broad_Brain_Muntoni_WES_20221102_ANV5_202304241527'),
    mksrc('bigquery', 'datarepo-12ac342c', 'ANVIL_CMG_Broad_Brain_NeuroDev_WES_20221102_ANV5_202304241529'),
    mksrc('bigquery', 'datarepo-d7bfafc6', 'ANVIL_CMG_Broad_Brain_Thaker_WES_20221102_ANV5_202304241531'),
    mksrc('bigquery', 'datarepo-29812b42', 'ANVIL_CMG_Broad_Eye_Pierce_WES_20221205_ANV5_202304242250'),
    mksrc('bigquery', 'datarepo-48134558', 'ANVIL_CMG_Broad_Eye_Pierce_WGS_20221117_ANV5_202304241507'),
    mksrc('bigquery', 'datarepo-36ebaa12', 'ANVIL_CMG_Broad_Heart_PCGC_Tristani_WGS_20221025_ANV5_202304211840'),
    mksrc('bigquery', 'datarepo-f9826139', 'ANVIL_CMG_Broad_Heart_Seidman_WES_20221117_ANV5_202304241504'),
    mksrc('bigquery', 'datarepo-85952af8', 'ANVIL_CMG_Broad_Kidney_Hildebrandt_WES_20230525_ANV5_202305251733'),
    mksrc('bigquery', 'datarepo-ee4ae9a1', 'ANVIL_CMG_Broad_Kidney_Hildebrandt_WGS_20221025_ANV5_202304211844'),
    mksrc('bigquery', 'datarepo-cf168274', 'ANVIL_CMG_Broad_Kidney_Pollak_WES_20221025_ANV5_202304211846'),
    mksrc('bigquery', 'datarepo-4d47ba2c', 'ANVIL_CMG_Broad_Muscle_Beggs_WGS_20221102_ANV5_202304241533'),
    mksrc('bigquery', 'datarepo-82d1271a', 'ANVIL_CMG_Broad_Muscle_Bonnemann_WES_20221117_ANV5_202304241509'),
    mksrc('bigquery', 'datarepo-6be3fb25', 'ANVIL_CMG_Broad_Muscle_Bonnemann_WGS_20221117_ANV5_202304241510'),
    mksrc('bigquery', 'datarepo-b168eb10', 'ANVIL_CMG_Broad_Muscle_KNC_WES_20221116_ANV5_202304242219'),
    mksrc('bigquery', 'datarepo-372244aa', 'ANVIL_CMG_Broad_Muscle_KNC_WGS_20221117_ANV5_202304242221'),
    mksrc('bigquery', 'datarepo-77a6c0aa', 'ANVIL_CMG_Broad_Muscle_Kang_WGS_20221025_ANV5_202304211849'),
    mksrc('bigquery', 'datarepo-736a5f1f', 'ANVIL_CMG_Broad_Muscle_Laing_WES_20221208_ANV5_202304271308'),
    mksrc('bigquery', 'datarepo-5019143b', 'ANVIL_CMG_Broad_Muscle_Myoseq_WES_20230621_ANV5_202306211852'),
    mksrc('bigquery', 'datarepo-27eb651a', 'ANVIL_CMG_Broad_Muscle_Myoseq_WGS_20221208_ANV5_202304271310'),
    mksrc('bigquery', 'datarepo-c087af7a', 'ANVIL_CMG_Broad_Muscle_OGrady_WES_20221205_ANV5_202304242252'),
    mksrc('bigquery', 'datarepo-db987a2e', 'ANVIL_CMG_Broad_Muscle_Ravenscroft_WES_20221208_ANV5_202304271311'),
    mksrc('bigquery', 'datarepo-05df566c', 'ANVIL_CMG_Broad_Muscle_Topf_WES_20221208_ANV5_202304271313'),
    mksrc('bigquery', 'datarepo-87d91f06', 'ANVIL_CMG_Broad_Orphan_Chung_WES_20221102_ANV5_202304241534'),
    mksrc('bigquery', 'datarepo-25f6b696', 'ANVIL_CMG_Broad_Orphan_Estonia_Ounap_WES_20221117_ANV5_202304241512'),
    mksrc('bigquery', 'datarepo-c3b16b41', 'ANVIL_CMG_Broad_Orphan_Estonia_Ounap_WGS_20221205_ANV5_202304242255'),
    mksrc('bigquery', 'datarepo-e2976b05', 'ANVIL_CMG_Broad_Orphan_Jueppner_WES_20221102_ANV5_202304241535'),
    mksrc('bigquery', 'datarepo-32fe2260', 'ANVIL_CMG_Broad_Orphan_Lerner_Ellis_WES_20221102_ANV5_202304241536'),
    mksrc('bigquery', 'datarepo-6f9e574e', 'ANVIL_CMG_Broad_Orphan_Manton_WES_20221117_ANV5_202304241513'),
    mksrc('bigquery', 'datarepo-53cd689b', 'ANVIL_CMG_Broad_Orphan_Manton_WGS_20221117_ANV5_202304241515'),
    mksrc('bigquery', 'datarepo-e7c5babf', 'ANVIL_CMG_Broad_Orphan_Scott_WES_20221025_ANV5_202304241458'),
    mksrc('bigquery', 'datarepo-051877f4', 'ANVIL_CMG_Broad_Orphan_Sweetser_WES_20221102_ANV5_202304241539'),
    mksrc('bigquery', 'datarepo-555c7706', 'ANVIL_CMG_Broad_Orphan_VCGS_White_WES_20221018_ANV5_202304241522'),
    mksrc('bigquery', 'datarepo-3a8f7952', 'ANVIL_CMG_Broad_Orphan_VCGS_White_WGS_20221117_ANV5_202304241523'),
    mksrc('bigquery', 'datarepo-b699c5e3', 'ANVIL_CMG_Broad_Rare_RGP_WES_20221102_ANV5_202304241540'),
    mksrc('bigquery', 'datarepo-2d5bd095', 'ANVIL_CMG_Broad_Stillbirth_Wilkins_Haug_WES_20221102_ANV5_202304241542'),
    mksrc('bigquery', 'datarepo-f3d0eda6', 'ANVIL_CMG_UWash_GRU_20230418_ANV5_202306211828'),
    mksrc('bigquery', 'datarepo-ab5c3fa5', 'ANVIL_CMG_YALE_DS_RARED_20221020_ANV5_202304211812'),
    mksrc('bigquery', 'datarepo-d51578f4', 'ANVIL_CMG_Yale_GRU_20221020_ANV5_202304211517'),
    mksrc('bigquery', 'datarepo-bcedc554', 'ANVIL_CMG_Yale_HMB_20221020_ANV5_202304211813'),
    mksrc('bigquery', 'datarepo-f485fa3e', 'ANVIL_CMG_Yale_HMB_GSO_20221020_ANV5_202304211519'),
    mksrc('bigquery', 'datarepo-45487b69', 'ANVIL_GTEx_Somatic_WGS_20230331_ANV5_202304211636'),
    mksrc('bigquery', 'datarepo-5ebc368c', 'ANVIL_GTEx_V7_hg19_20221128_ANV5_202304211804'),
    mksrc('bigquery', 'datarepo-864913f2', 'ANVIL_GTEx_V9_hg38_20221128_ANV5_202304211853'),
    mksrc('bigquery', 'datarepo-b093b69d', 'ANVIL_GTEx_public_data_20221115_ANV5_202304211659'),
    mksrc('bigquery', 'datarepo-d948d21a', 'ANVIL_cmg_broad_brain_engle_wgs_20221202_ANV5_202304271345'),
    mksrc('bigquery', 'datarepo-1cb73890', 'ANVIL_cmg_broad_heart_ware_wes_20221215_ANV5_202304242145'),
]))

anvil2_sources = mkdict(anvil1_sources, 104, mkdelta([
    # @formatter:off
    mksrc('bigquery', 'datarepo-36124817', 'ANVIL_African_American_Seq_HGV_20230727_ANV5_202308291753'),
    mksrc('bigquery', 'datarepo-d795027d', 'ANVIL_CCDG_Broad_CVD_AF_VAFAR_Arrays_20221020_ANV5_202304211823'),
    mksrc('bigquery', 'datarepo-642829f3', 'ANVIL_CCDG_Broad_CVD_AF_VAFAR_WES_20221024_ANV5_202304211826'),
    mksrc('bigquery', 'datarepo-08216a2c', 'ANVIL_CCDG_Broad_CVD_AFib_Vanderbilt_Ablation_WGS_20221020_ANV5_202304211819'), # noqa E501
    mksrc('bigquery', 'datarepo-74975e89', 'ANVIL_CCDG_Broad_NP_Epilepsy_JPNFKA_GRU_WES_20221220_ANV5_202304271548'),
    mksrc('bigquery', 'datarepo-ad61c47e', 'ANVIL_CCDG_NHGRI_Broad_ASD_Daly_phs000298_WES_vcf_20230403_ANV5_202304271610'), # noqa E501
    mksrc('bigquery', 'datarepo-5e719362', 'ANVIL_CCDG_NYGC_AI_Asthma_Gala2_WGS_20230605_ANV5_202306131248'),
    mksrc('bigquery', 'datarepo-2734a0e4', 'ANVIL_CCDG_NYGC_NP_Alz_EFIGA_WGS_20230605_ANV5_202306141705'),
    mksrc('bigquery', 'datarepo-710fc60d', 'ANVIL_CCDG_NYGC_NP_Alz_LOAD_WGS_20230605_ANV5_202306131256'),
    mksrc('bigquery', 'datarepo-9626b3eb', 'ANVIL_CCDG_NYGC_NP_Alz_WHICAP_WGS_20230605_ANV5_202306131303'),
    mksrc('bigquery', 'datarepo-25ec7b57', 'ANVIL_CCDG_WASHU_PAGE_20221220_ANV5_202304271544'),
    mksrc('bigquery', 'datarepo-6d8536f4', 'ANVIL_CMH_GAFK_GS_linked_read_20221107_ANV5_202304211527'),
    mksrc('bigquery', 'datarepo-482ab960', 'ANVIL_CMH_GAFK_GS_long_read_20221109_ANV5_202304211529'),
    mksrc('bigquery', 'datarepo-8745e97d', 'ANVIL_CMH_GAFK_scRNA_20221107_ANV5_202304211533'),
    mksrc('bigquery', 'datarepo-1c89dcac', 'ANVIL_CSER_CHARM_GRU_20221208_ANV5_202304271348'),
    mksrc('bigquery', 'datarepo-12d56848', 'ANVIL_CSER_NCGENES2_GRU_20221208_ANV5_202304271349'),
    mksrc('bigquery', 'datarepo-8a4d67ef', 'ANVIL_CSER_SouthSeq_GRU_20221208_ANV5_202304271351'),
    mksrc('bigquery', 'datarepo-f622180d', 'ANVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_CIRM_GRU_VillageData_20230109_ANV5_202304242045'),  # noqa E501
    mksrc('bigquery', 'datarepo-732d1a55', 'ANVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_CIRM_GRU_WGS_20230109_ANV5_202304242048'),  # noqa E501
    mksrc('bigquery', 'datarepo-90bab913', 'ANVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_Finkel_SMA_DS_WGS_20230109_ANV5_202304242043'),  # noqa E501
    mksrc('bigquery', 'datarepo-e4eb7641', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Braff_DS_WGS_20221115_ANV5_202304242052'),
    mksrc('bigquery', 'datarepo-f9aef3dc', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Escamilla_DS_WGS_20221103_ANV5_202304242049'), # noqa E501
    mksrc('bigquery', 'datarepo-aca6a582', 'ANVIL_NIMH_CIRM_FCDI_ConvergentNeuro_McCarroll_Eggan_GRU_Arrays_20230109_ANV5_202304242046'),  # noqa E501
    mksrc('bigquery', 'datarepo-06abb598', 'ANVIL_PAGE_BioMe_GRU_WGS_20221128_ANV5_202304211817'),
    mksrc('bigquery', 'datarepo-7c4410ed', 'ANVIL_PAGE_MEC_GRU_WGS_20230131_ANV5_202304211721'),
    mksrc('bigquery', 'datarepo-84d2e3b1', 'ANVIL_PAGE_Stanford_Global_Reference_Panel_GRU_WGS_20221128_ANV5_202304211827'), # noqa E501
    mksrc('bigquery', 'datarepo-ffbc38fd', 'ANVIL_PAGE_WHI_HMB_IRB_WGS_20221019_ANV5_202304211722'),
    mksrc('bigquery', 'datarepo-b1f3e0d1', 'ANVIL_ccdg_asc_ndd_daly_talkowski_cdcseed_asd_gsa_md_20221024_ANV5_202304211749'), # noqa E501
    mksrc('bigquery', 'datarepo-11330a21', 'ANVIL_ccdg_asc_ndd_daly_talkowski_schloesser_asd_gsa_md_20221025_ANV5_202304211759'),  # noqa E501
    mksrc('bigquery', 'datarepo-86a1dbf3', 'ANVIL_ccdg_broad_ai_ibd_daly_bernstein_gsa_20221025_ANV5_202304241921'),
    mksrc('bigquery', 'datarepo-833ff0a3', 'ANVIL_eMERGE_GRU_IRB_NPU_eMERGEseq_20230130_ANV5_202304271614'),
    mksrc('bigquery', 'datarepo-baf040af', 'ANVIL_eMERGE_GRU_IRB_PUB_NPU_eMERGEseq_20230130_ANV5_202304271616'),
    mksrc('bigquery', 'datarepo-270b3b62', 'ANVIL_eMERGE_GRU_IRB_eMERGEseq_20230130_ANV5_202304271613'),
    mksrc('bigquery', 'datarepo-c13efbe9', 'ANVIL_eMERGE_GRU_NPU_eMERGEseq_20230130_ANV5_202304271617'),
    mksrc('bigquery', 'datarepo-34f8138d', 'ANVIL_eMERGE_GRU_eMERGEseq_20230130_ANV5_202304271612'),
    mksrc('bigquery', 'datarepo-90b7b6e8', 'ANVIL_eMERGE_HMB_GSO_eMERGEseq_20230130_ANV5_202304271621'),
    mksrc('bigquery', 'datarepo-6e6dca92', 'ANVIL_eMERGE_HMB_IRB_PUB_eMERGEseq_20230130_ANV5_202304271622'),
    mksrc('bigquery', 'datarepo-1ddf2a8e', 'ANVIL_eMERGE_HMB_NPU_eMERGEseq_20230130_ANV5_202304271624'),
    mksrc('bigquery', 'datarepo-dba97a65', 'ANVIL_eMERGE_HMB_eMERGEseq_20230130_ANV5_202304271619'),
    mksrc('bigquery', 'datarepo-51aa9a22', 'ANVIL_eMERGE_PGRNseq_20230118_ANV5_202304241853'),
    mksrc('bigquery', 'datarepo-ce8c469f', 'ANVIL_eMERGE_PRS_Arrays_20221220_ANV5_202304271346')
    # @formatter:on
]))

anvil3_sources = mkdict(anvil2_sources, 151, mkdelta([
    # @formatter:off
    mksrc('bigquery', 'datarepo-9a74aed3', 'ANVIL_CCDG_Baylor_CVD_ARIC_20231008_ANV5_202310091900'),
    mksrc('bigquery', 'datarepo-0768a322', 'ANVIL_CCDG_Broad_CVD_AF_Ellinor_MGH_Arrays_20221024_ANV5_202304211831'),
    mksrc('bigquery', 'datarepo-2b135baf', 'ANVIL_CCDG_Broad_CVD_AFib_MGH_WGS_20221024_ANV5_202304211829'),
    mksrc('bigquery', 'datarepo-96b594f9', 'ANVIL_CCDG_Broad_CVD_EOCAD_TaiChi_WGS_20221026_ANV5_202310101655'),
    mksrc('bigquery', 'datarepo-318ae48e', 'ANVIL_CCDG_Broad_CVD_Stroke_BRAVE_WGS_20221107_ANV5_202304241543'),
    mksrc('bigquery', 'datarepo-7ea7a6e9', 'ANVIL_CCDG_Broad_MI_BRAVE_GRU_WES_20221107_ANV5_202304241545'),
    mksrc('bigquery', 'datarepo-2339e241', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EPIL_BA_MDS_WES_20221101_ANV5_202304241613'), # noqa E501
    mksrc('bigquery', 'datarepo-cd6cee03', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EPI_BA_ID_MDS_WES_20221101_ANV5_202304241612'), # noqa E501
    mksrc('bigquery', 'datarepo-da88c3ce', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EP_BA_CN_ID_MDS_WES_20221101_ANV5_202304241657'),  # noqa E501
    mksrc('bigquery', 'datarepo-2b361bda', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSRMB_DS_EAED_MDS_WES_20221026_ANV5_202304241549'), # noqa E501
    mksrc('bigquery', 'datarepo-6eeff3fc', 'ANVIL_CCDG_Broad_NP_Epilepsy_BELATW_GRU_WES_20221108_ANV5_202304241701'),
    mksrc('bigquery', 'datarepo-21923ed0', 'ANVIL_CCDG_Broad_NP_Epilepsy_BELULB_DS_EP_NPU_WES_20221027_ANV5_202304241556'), # noqa E501
    mksrc('bigquery', 'datarepo-5b10132b', 'ANVIL_CCDG_Broad_NP_Epilepsy_CANUTN_DS_EP_WES_20230328_ANV5_202304241552'),
    mksrc('bigquery', 'datarepo-d2d5ba15', 'ANVIL_CCDG_Broad_NP_Epilepsy_CZEMTH_GRU_WES_20221108_ANV5_202304241702'),
    mksrc('bigquery', 'datarepo-fc0a35a8', 'ANVIL_CCDG_Broad_NP_Epilepsy_DEUULG_GRU_WES_20221108_ANV5_202304241704'),
    mksrc('bigquery', 'datarepo-f14cd6d7', 'ANVIL_CCDG_Broad_NP_Epilepsy_FINKPH_EPIL_CO_MORBIDI_MDS_WES_20230328_ANV5_202304241659'),  # noqa E501
    mksrc('bigquery', 'datarepo-3832cf81', 'ANVIL_CCDG_Broad_NP_Epilepsy_GBRSWU_CARDI_NEURO_WES_20221026_ANV5_202304241548'), # noqa E501
    mksrc('bigquery', 'datarepo-098aadb0', 'ANVIL_CCDG_Broad_NP_Epilepsy_GBRUCL_DS_EARET_MDS_WES_20221026_ANV5_202304241551'), # noqa E501
    mksrc('bigquery', 'datarepo-d9ea4f23', 'ANVIL_CCDG_Broad_NP_Epilepsy_GBRUNL_EP_ETIOLOGY_MDS_WES_20221027_ANV5_202304241554'),  # noqa E501
    mksrc('bigquery', 'datarepo-0c9ab563', 'ANVIL_CCDG_Broad_NP_Epilepsy_GBRUNL_GRU_WES_20221108_ANV5_202304241705'),
    mksrc('bigquery', 'datarepo-a383d752', 'ANVIL_CCDG_Broad_NP_Epilepsy_ITAIGI_GRU_WES_20221108_ANV5_202304241707'),
    mksrc('bigquery', 'datarepo-03b52641', 'ANVIL_CCDG_Broad_NP_Epilepsy_ITAUBG_DS_EPI_NPU_MDS_WES_20221027_ANV5_202304241601'),  # noqa E501
    mksrc('bigquery', 'datarepo-2e9ab296', 'ANVIL_CCDG_Broad_NP_Epilepsy_ITAUMC_DS_NEURO_MDS_WES_20221108_ANV5_202304241605'), # noqa E501
    mksrc('bigquery', 'datarepo-89162c54', 'ANVIL_CCDG_Broad_NP_Epilepsy_JPNRKI_DS_NPD_IRB_NPU_WES_20221027_ANV5_202304241609'),  # noqa E501
    mksrc('bigquery', 'datarepo-fd5cd738', 'ANVIL_CCDG_Broad_NP_Epilepsy_NZLUTO_EPIL_BC_ID_MDS_WES_20230328_ANV5_202304241602'),  # noqa E501
    mksrc('bigquery', 'datarepo-d987821a', 'ANVIL_CCDG_Broad_NP_Epilepsy_TURBZU_GRU_WES_20221108_ANV5_202304241709'),
    mksrc('bigquery', 'datarepo-b93e1cfa', 'ANVIL_CCDG_Broad_NP_Epilepsy_TURIBU_DS_NEURO_AD_NPU_WES_20221027_ANV5_202304241604'),  # noqa E501
    mksrc('bigquery', 'datarepo-2e9630dd', 'ANVIL_CCDG_Broad_NP_Epilepsy_USABCH_EPI_MUL_CON_MDS_WES_20221027_ANV5_202304241559'),  # noqa E501
    mksrc('bigquery', 'datarepo-ee58a7a9', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACHP_GRU_WES_20230612_ANV5_202306131343'),
    mksrc('bigquery', 'datarepo-ff5356bb', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACRW_DS_EP_MDS_WES_20221027_ANV5_202304241555'), # noqa E501
    mksrc('bigquery', 'datarepo-2262daa7', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACRW_DS_SEIZD_WES_20221027_ANV5_202304241610'), # noqa E501
    mksrc('bigquery', 'datarepo-2a947c33', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACRW_EPI_ASZ_MED_MDS_WES_20221027_ANV5_202304241558'),  # noqa E501
    mksrc('bigquery', 'datarepo-5b3c42e1', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAEGP_GRU_WES_20221110_ANV5_202304241713'),
    mksrc('bigquery', 'datarepo-91b4b33c', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAHEP_GRU_WES_20230328_ANV5_202306211900'),
    mksrc('bigquery', 'datarepo-e4fe111a', 'ANVIL_CCDG_Broad_NP_Epilepsy_USANCH_DS_NEURO_MDS_WES_20221108_ANV5_202304241607'), # noqa E501
    mksrc('bigquery', 'datarepo-8b120833', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAUPN_Marsh_GRU_WES_20230328_ANV5_202304241716'), # noqa E501
    mksrc('bigquery', 'datarepo-f051499d', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAUPN_Rader_GRU_WES_20230328_ANV5_202304241720'), # noqa E501
    mksrc('bigquery', 'datarepo-fd49a493', 'ANVIL_CCDG_WashU_CVD_EOCAD_WashU_CAD_DS_WGS_20230525_ANV5_202306211841'),
    mksrc('bigquery', 'datarepo-076da44b', 'ANVIL_CCDG_WashU_CVD_EOCAD_WashU_CAD_GRU_IRB_WGS_20230525_ANV5_202306211847'), # noqa E501
    mksrc('bigquery', 'datarepo-7e03b5fd', 'ANVIL_CMG_Broad_Brain_Walsh_WES_20230605_ANV5_202310101734'),
    mksrc('bigquery', 'datarepo-c43e7400', 'ANVIL_CMG_Broad_Muscle_Kang_WES_20230525_ANV5_202310101649'),
    mksrc('bigquery', 'datarepo-14f5afa3', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Braff_DS_10XLRGenomes_20221115_ANV5_202310101713'),  # noqa E501
    mksrc('bigquery', 'datarepo-94091a22', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Pato_GRU_10XLRGenomes_20230331_ANV5_202310101715'),  # noqa E501
    mksrc('bigquery', 'datarepo-55b75002', 'ANVIL_PAGE_SoL_HMB_WGS_20221220_ANV5_202310061302'),
    mksrc('bigquery', 'datarepo-02ad84ea', 'ANVIL_T2T_20230714_ANV5_202310101616'),
    mksrc('bigquery', 'datarepo-08cd15a2', 'ANVIL_ccdg_washu_ai_t1d_t1dgc_wgs_20221031_ANV5_202304211552'),
    mksrc('bigquery', 'datarepo-e3065356', 'ANVIL_ccdg_washu_cvd_eocad_biome_wgs_20221024_ANV5_202304211601'),
    # @formatter:on
]))

anvil4_sources = mkdict(anvil3_sources, 200, mkdelta([
    # @formatter:off
    mksrc('bigquery', 'datarepo-1a86e7ca', 'ANVIL_CCDG_Baylor_CVD_AFib_Groningen_WGS_20221122_ANV5_202304242224'),
    mksrc('bigquery', 'datarepo-92716a90', 'ANVIL_CCDG_Baylor_CVD_AFib_VAFAR_HMB_IRB_WGS_20221020_ANV5_202304211525'),
    mksrc('bigquery', 'datarepo-77445496', 'ANVIL_CCDG_Baylor_CVD_EOCAD_BioMe_WGS_20221122_ANV5_202304242226'),
    mksrc('bigquery', 'datarepo-1b0d6b90', 'ANVIL_CCDG_Baylor_CVD_HHRC_Brownsville_GRU_WGS_20221122_ANV5_202304242228'),
    mksrc('bigquery', 'datarepo-373b7918', 'ANVIL_CCDG_Baylor_CVD_HemStroke_BNI_HMB_WGS_20221215_ANV5_202304242306'),
    mksrc('bigquery', 'datarepo-efc3e806', 'ANVIL_CCDG_Baylor_CVD_HemStroke_Duke_DS_WGS_20221117_ANV5_202304242122'),
    mksrc('bigquery', 'datarepo-1044f96d', 'ANVIL_CCDG_Baylor_CVD_HemStroke_ERICH_WGS_20221207_ANV5_202304271256'),
    mksrc('bigquery', 'datarepo-f23a6ec8', 'ANVIL_CCDG_Baylor_CVD_HemStroke_GERFHS_HMB_WGS_20221215_ANV5_202304242307'),
    mksrc('bigquery', 'datarepo-de34ca6e', 'ANVIL_CCDG_Baylor_CVD_HemStroke_Regards_DS_WGS_20221117_ANV5_202304242123'),
    mksrc('bigquery', 'datarepo-d9c6f406', 'ANVIL_CCDG_Baylor_CVD_HemStroke_Yale_HMB_WGS_20221215_ANV5_202304242309'),
    mksrc('bigquery', 'datarepo-56883e56', 'ANVIL_CCDG_Baylor_CVD_Oregon_SUDS_GRU_WGS_20221215_ANV5_202304242302'),
    mksrc('bigquery', 'datarepo-7f3ba7ec', 'ANVIL_CCDG_Baylor_CVD_TexGen_DS_WGS_20221117_ANV5_202304242125'),
    mksrc('bigquery', 'datarepo-da965e26', 'ANVIL_CCDG_Baylor_CVD_Ventura_Presto_GRU_IRB_WGS_20221117_ANV5_202304242127'), # noqa E501
    mksrc('bigquery', 'datarepo-906bf803', 'ANVIL_CCDG_Broad_AI_IBD_Brant_DS_IBD_WGS_20221110_ANV5_202304241911'),
    mksrc('bigquery', 'datarepo-343ca1c3', 'ANVIL_CCDG_Broad_AI_IBD_Brant_HMB_WGS_20221110_ANV5_202304241912'),
    mksrc('bigquery', 'datarepo-80a63603', 'ANVIL_CCDG_Broad_AI_IBD_Cho_WGS_20230313_ANV5_202304241903'),
    mksrc('bigquery', 'datarepo-a98e7a43', 'ANVIL_CCDG_Broad_AI_IBD_Kugathasan_WGS_20221110_ANV5_202304241906'),
    mksrc('bigquery', 'datarepo-381bc957', 'ANVIL_CCDG_Broad_AI_IBD_McCauley_WGS_20221110_ANV5_202304241914'),
    mksrc('bigquery', 'datarepo-6a10165d', 'ANVIL_CCDG_Broad_AI_IBD_McGovern_WGS_20221110_ANV5_202304241907'),
    mksrc('bigquery', 'datarepo-a2743c82', 'ANVIL_CCDG_Broad_AI_IBD_Newberry_WGS_20221025_ANV5_202304241901'),
    mksrc('bigquery', 'datarepo-ed109b2f', 'ANVIL_CCDG_Broad_CVD_AF_BioVU_HMB_GSO_Arrays_20230612_ANV5_202306131350'),
    mksrc('bigquery', 'datarepo-3d8b62d7', 'ANVIL_CCDG_Broad_CVD_AF_BioVU_HMB_GSO_WES_20221025_ANV5_202304241856'),
    mksrc('bigquery', 'datarepo-450ba911', 'ANVIL_CCDG_Broad_CVD_AF_ENGAGE_DS_WES_20230418_ANV5_202304210808'),
    mksrc('bigquery', 'datarepo-dfabf632', 'ANVIL_CCDG_Broad_CVD_AF_Ellinor_MGH_WES_20221117_ANV5_202304271354'),
    mksrc('bigquery', 'datarepo-485eb707', 'ANVIL_CCDG_Broad_CVD_AF_Figtree_BioHeart_Arrays_20230128_ANV5_202304271554'), # noqa E501
    mksrc('bigquery', 'datarepo-58dffe5a', 'ANVIL_CCDG_Broad_CVD_AF_GAPP_DS_MDS_Arrays_20221103_ANV5_202304242105'),
    mksrc('bigquery', 'datarepo-cf7f2c0c', 'ANVIL_CCDG_Broad_CVD_AF_GAPP_DS_MDS_WES_20221103_ANV5_202304242107'),
    mksrc('bigquery', 'datarepo-f896734e', 'ANVIL_CCDG_Broad_CVD_AF_Marcus_UCSF_Arrays_20221102_ANV5_202304242039'),
    mksrc('bigquery', 'datarepo-40c2f4f4', 'ANVIL_CCDG_Broad_CVD_AF_Marcus_UCSF_WES_20221222_ANV5_202304242040'),
    mksrc('bigquery', 'datarepo-67117555', 'ANVIL_CCDG_Broad_CVD_AF_Rienstra_WES_20221222_ANV5_202304242035'),
    mksrc('bigquery', 'datarepo-c45dd622', 'ANVIL_CCDG_Broad_CVD_AF_Swiss_Cases_DS_MDS_Arrays_20221103_ANV5_202304242110'), # noqa E501
    mksrc('bigquery', 'datarepo-b12d2e52', 'ANVIL_CCDG_Broad_CVD_AF_Swiss_Cases_DS_MDS_WES_20230118_ANV5_202304242112'),
    mksrc('bigquery', 'datarepo-43f6230a', 'ANVIL_CCDG_Broad_CVD_AFib_AFLMU_WGS_20231008_ANV5_202310091911'),
    mksrc('bigquery', 'datarepo-de64d25a', 'ANVIL_CCDG_Broad_CVD_AFib_UCSF_WGS_20221222_ANV5_202304242037'),
    mksrc('bigquery', 'datarepo-e25350dd', 'ANVIL_CCDG_Broad_CVD_EOCAD_PartnersBiobank_HMB_Arrays_20230517_ANV5_202310101704'),  # noqa E501
    mksrc('bigquery', 'datarepo-9921a6fa', 'ANVIL_CCDG_Broad_CVD_EOCAD_PartnersBiobank_HMB_WES_20230621_ANV5_202306211933'), # noqa E501
    mksrc('bigquery', 'datarepo-383d9d9b', 'ANVIL_CCDG_Broad_CVD_PROMIS_GRU_WES_20230418_ANV5_202306211912'),
    mksrc('bigquery', 'datarepo-5df71da4', 'ANVIL_CCDG_Broad_MI_InStem_WES_20221122_ANV5_202304242236'),
    mksrc('bigquery', 'datarepo-1793828c', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSALF_HMB_IRB_GSRS_WES_20230324_ANV5_202304241752'), # noqa E501
    mksrc('bigquery', 'datarepo-d44547dc', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSALF_HMB_IRB_WES_20230128_ANV5_202304271556'), # noqa E501
    mksrc('bigquery', 'datarepo-70c803d7', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EPIL_BA_MDS_GSA_MD_20221117_ANV5_202304271400'), # noqa E501
    mksrc('bigquery', 'datarepo-f5a4a895', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EPI_BA_ID_MDS_GSA_MD_20221117_ANV5_202304271358'),  # noqa E501
    mksrc('bigquery', 'datarepo-b8b8ba44', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EP_BA_CN_ID_MDS_GSA_MD_20221117_ANV5_202304271356'),  # noqa E501
    mksrc('bigquery', 'datarepo-0b0ca621', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSRMB_DS_EAED_IRB_WES_20230621_ANV5_202306211945'), # noqa E501
    mksrc('bigquery', 'datarepo-f85048a3', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSRMB_DS_EAED_MDS_GSA_MD_20221117_ANV5_202304271401'),  # noqa E501
    mksrc('bigquery', 'datarepo-68037179', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUTMUV_DS_NS_ADLT_WES_20230128_ANV5_202304271559'), # noqa E501
    mksrc('bigquery', 'datarepo-025215fc', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUTMUV_DS_NS_WES_20230314_ANV5_202304271601'),
    mksrc('bigquery', 'datarepo-92905a2b', 'ANVIL_CCDG_Broad_NP_Epilepsy_BELATW_GRU_GSA_MD_20221117_ANV5_202304271403'),
    mksrc('bigquery', 'datarepo-3f3ad5c7', 'ANVIL_CCDG_Broad_NP_Epilepsy_BELULB_DS_EP_NPU_GSA_MD_20230118_ANV5_202304271404') # noqa E501
    # @formatter:on
]))

anvil5_sources = mkdict(anvil4_sources, 262, mkdelta([
    # @formatter:off
    mksrc('bigquery', 'datarepo-3c30a9a2', 'ANVIL_1000G_high_coverage_2019_20230517_ANV5_202403030329'),
    mksrc('bigquery', 'datarepo-adf70694', 'ANVIL_ALS_FTD_ALS_AssociatedGenes_GRU_v1_20231221_ANV5_202401112025'),
    mksrc('bigquery', 'datarepo-815ad21b', 'ANVIL_ALS_FTD_DEMENTIA_SEQ_GRU_v1_20231221_ANV5_202401112033'),
    mksrc('bigquery', 'datarepo-ab46a8e4', 'ANVIL_CCDG_NYGC_NP_Autism_ACE2_DS_MDS_WGS_20230605_ANV5_202403032021'),
    mksrc('bigquery', 'datarepo-df058a48', 'ANVIL_CCDG_NYGC_NP_Autism_AGRE_WGS_20230605_ANV5_202403032044'),
    mksrc('bigquery', 'datarepo-61910b61', 'ANVIL_CCDG_NYGC_NP_Autism_CAG_DS_WGS_20230605_ANV5_202403032053'),
    mksrc('bigquery', 'datarepo-8d6472a1', 'ANVIL_CCDG_NYGC_NP_Autism_HFA_DS_WGS_20230605_ANV5_202403032108'),
    mksrc('bigquery', 'datarepo-f0a12498', 'ANVIL_CCDG_NYGC_NP_Autism_PELPHREY_ACE_DS_WGS_20221103_ANV5_202403032124'),
    mksrc('bigquery', 'datarepo-f06dc5dd', 'ANVIL_CCDG_NYGC_NP_Autism_PELPHREY_ACE_GRU_WGS_20221103_ANV5_202403032131'),
    mksrc('bigquery', 'datarepo-b791f5c1', 'ANVIL_CCDG_NYGC_NP_Autism_SAGE_WGS_20230605_ANV5_202403032137'),
    mksrc('bigquery', 'datarepo-b9222139', 'ANVIL_CMG_BROAD_BRAIN_ENGLE_WES_20240205_ANV5_202402051624'),
    mksrc('bigquery', 'datarepo-7e094253', 'ANVIL_CMG_BROAD_BRAIN_SHERR_WGS_20221102_ANV5_202402281543'),
    mksrc('bigquery', 'datarepo-c797490f', 'ANVIL_CMG_BROAD_ORPHAN_SCOTT_WGS_20221102_ANV5_202402281552'),
    mksrc('bigquery', 'datarepo-0a1360b1', 'ANVIL_CMG_Broad_Blood_Gazda_WES_20221117_ANV5_202402290547'),
    mksrc('bigquery', 'datarepo-faa71b49', 'ANVIL_CMG_Broad_Blood_Sankaran_WES_20221117_ANV5_202402290555'),
    mksrc('bigquery', 'datarepo-abce6387', 'ANVIL_CMG_Broad_Blood_Sankaran_WGS_20221117_ANV5_202402290606'),
    mksrc('bigquery', 'datarepo-4153ad1f', 'ANVIL_CMG_Broad_Muscle_Laing_WES_20221208_ANV5_202402291926'),
    mksrc('bigquery', 'datarepo-5bbb5a28', 'ANVIL_CMG_Broad_Orphan_Jueppner_WES_20240205_ANV5_202402051640'),
    mksrc('bigquery', 'datarepo-18bd3df4', 'ANVIL_CMG_UWASH_HMB_20230418_ANV5_202402070029'),
    mksrc('bigquery', 'datarepo-6f4155f2', 'ANVIL_CMG_UWash_GRU_20240301_ANV5_202403040330'),
    mksrc('bigquery', 'datarepo-6486ae96', 'ANVIL_CMG_UWash_GRU_1_20240113_ANV5_202401141440'),
    mksrc('bigquery', 'datarepo-0fad0f77', 'ANVIL_CMG_YALE_DS_RARED_20221020_ANV5_202402281620'),
    mksrc('bigquery', 'datarepo-ad307392', 'ANVIL_CMG_Yale_GRU_20221020_ANV5_202402281628'),
    mksrc('bigquery', 'datarepo-fecab5bc', 'ANVIL_CMG_Yale_HMB_20221020_ANV5_202402290926'),
    mksrc('bigquery', 'datarepo-f9699204', 'ANVIL_CMG_Yale_HMB_GSO_20221020_ANV5_202402290935'),
    mksrc('bigquery', 'datarepo-c5bd892a', 'ANVIL_CMH_GAFK_GS_linked_read_20221107_ANV5_202402290945'),
    mksrc('bigquery', 'datarepo-5e64223a', 'ANVIL_CMH_GAFK_GS_long_read_20240301_ANV5_202403040349'),
    mksrc('bigquery', 'datarepo-ba97c05c', 'ANVIL_CMH_GAFK_scRNA_20221107_ANV5_202402291004'),
    mksrc('bigquery', 'datarepo-2659c380', 'ANVIL_CSER_CHARM_GRU_20240301_ANV5_202403040357'),
    mksrc('bigquery', 'datarepo-0f2e95ad', 'ANVIL_CSER_KidsCanSeq_GRU_20221208_ANV5_202402292138'),
    mksrc('bigquery', 'datarepo-62a0bd6d', 'ANVIL_CSER_NCGENES2_GRU_20221208_ANV5_202402292147'),
    mksrc('bigquery', 'datarepo-df02801a', 'ANVIL_CSER_NYCKIDSEQ_GRU_20240113_ANV5_202401141520'),
    mksrc('bigquery', 'datarepo-4b9c138d', 'ANVIL_CSER_NYCKIDSEQ_HMB_20240113_ANV5_202401141527'),
    mksrc('bigquery', 'datarepo-f4d60c69', 'ANVIL_CSER_P3EGS_GRU_20230727_ANV5_202402070059'),
    mksrc('bigquery', 'datarepo-fc5ed559', 'ANVIL_CSER_SouthSeq_GRU_20221208_ANV5_202402292154'),
    mksrc('bigquery', 'datarepo-74121c99', 'ANVIL_GTEx_BCM_GRU_CoRSIVs_20240116_ANV5_202401170141'),
    mksrc('bigquery', 'datarepo-1a706b0c', 'ANVIL_GTEx_Somatic_WGS_20240116_ANV5_202401170147'),
    mksrc('bigquery', 'datarepo-e063cf6d', 'ANVIL_GTEx_V7_hg19_20221128_ANV5_202402291034'),
    mksrc('bigquery', 'datarepo-383c097a', 'ANVIL_GTEx_V8_hg38_20240116_ANV5_202401170154'),
    mksrc('bigquery', 'datarepo-701eea84', 'ANVIL_GTEx_V9_hg38_20221128_ANV5_202402070108'),
    mksrc('bigquery', 'datarepo-ff9d78a5', 'ANVIL_GTEx_public_data_20240117_ANV5_202401180400'),
    mksrc('bigquery', 'datarepo-37c3d458', 'ANVIL_NIA_CARD_Coriell_Cell_Lines_Open_20230727_ANV5_202401111624'),
    mksrc('bigquery', 'datarepo-06c78117', 'ANVIL_NIA_CARD_LR_WGS_NABEC_GRU_20230727_ANV5_202401111634'),
    mksrc('bigquery', 'datarepo-e4eb7641', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Braff_DS_WGS_20221115_ANV5_202304242052', pop), # noqa E501
    mksrc('bigquery', 'datarepo-a3880121', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Pato_GRU_WGS_20240112_ANV5_202402062129'),
    mksrc('bigquery', 'datarepo-25790186', 'ANVIL_PAGE_BioMe_GRU_WGS_20221128_ANV5_202403040429'),
    mksrc('bigquery', 'datarepo-b371989b', 'ANVIL_PAGE_MEC_GRU_WGS_20230131_ANV5_202403040437'),
    mksrc('bigquery', 'datarepo-4a4eec27', 'ANVIL_PAGE_SoL_HMB_WGS_20221220_ANV5_202403040445'),
    mksrc('bigquery', 'datarepo-a1f917db', 'ANVIL_PAGE_Stanford_Global_Reference_Panel_GRU_WGS_20221128_ANV5_202403040453'), # noqa E501
    mksrc('bigquery', 'datarepo-6264931f', 'ANVIL_PAGE_WHI_HMB_IRB_WGS_20221019_ANV5_202403040500'),
    mksrc('bigquery', 'datarepo-8d62ec8f', 'ANVIL_T2T_20230714_ANV5_202312122150'),
    mksrc('bigquery', 'datarepo-bfabc906', 'ANVIL_ccdg_asc_ndd_daly_talkowski_ac_boston_asd_exome_20221117_ANV5_202403040552'), # noqa E501
    mksrc('bigquery', 'datarepo-825399a4', 'ANVIL_ccdg_asc_ndd_daly_talkowski_barbosa_asd_exome_20221108_ANV5_202403040608'), # noqa E501
    mksrc('bigquery', 'datarepo-e3b070a7', 'ANVIL_ccdg_asc_ndd_daly_talkowski_brusco_asd_exome_20230327_ANV5_202403040615'), # noqa E501
    mksrc('bigquery', 'datarepo-2354d65a', 'ANVIL_ccdg_asc_ndd_daly_talkowski_cdcseed_asd_gsa_md_20221024_ANV5_202402291144'), # noqa E501
    mksrc('bigquery', 'datarepo-0ad3f21a', 'ANVIL_ccdg_asc_ndd_daly_talkowski_chung_asd_exome_20221107_ANV5_202403040623'), # noqa E501
    mksrc('bigquery', 'datarepo-c148a340', 'ANVIL_ccdg_asc_ndd_daly_talkowski_control_NIMH_asd_exome_20221201_ANV5_202403040630'),  # noqa E501
    mksrc('bigquery', 'datarepo-bc613fa9', 'ANVIL_ccdg_asc_ndd_daly_talkowski_domenici_asd_exome_20221117_ANV5_202403040637'), # noqa E501
    mksrc('bigquery', 'datarepo-97e22445', 'ANVIL_ccdg_asc_ndd_daly_talkowski_goethe_asd_exome_20221107_ANV5_202403040652'), # noqa E501
    mksrc('bigquery', 'datarepo-72efc816', 'ANVIL_ccdg_asc_ndd_daly_talkowski_herman_asd_exome_20221117_ANV5_202403040701'), # noqa E501
    mksrc('bigquery', 'datarepo-e25caee8', 'ANVIL_ccdg_asc_ndd_daly_talkowski_hertz_picciotto_asd_exome_20221107_ANV5_202403040708'),  # noqa E501
    mksrc('bigquery', 'datarepo-22af2470', 'ANVIL_ccdg_asc_ndd_daly_talkowski_hertz_picciotto_asd_wgs_20221107_ANV5_202403040716'),  # noqa E501
    mksrc('bigquery', 'datarepo-a81009d9', 'ANVIL_ccdg_asc_ndd_daly_talkowski_hultman_asd_exome_20231013_ANV5_202403040723'), # noqa E501
    mksrc('bigquery', 'datarepo-bc078d98', 'ANVIL_ccdg_asc_ndd_daly_talkowski_kolevzon_asd_exome_20221108_ANV5_202403040731'), # noqa E501
    mksrc('bigquery', 'datarepo-0949186c', 'ANVIL_ccdg_asc_ndd_daly_talkowski_kolevzon_asd_wgs_20221109_ANV5_202403040739'), # noqa E501
    mksrc('bigquery', 'datarepo-4dc4f939', 'ANVIL_ccdg_asc_ndd_daly_talkowski_lattig_asd_exome_20221122_ANV5_202403040746'), # noqa E501
    mksrc('bigquery', 'datarepo-5ed988f8', 'ANVIL_ccdg_asc_ndd_daly_talkowski_menashe_asd_exome_20221108_ANV5_202403040800'), # noqa E501
    mksrc('bigquery', 'datarepo-c6a938e4', 'ANVIL_ccdg_asc_ndd_daly_talkowski_minshew_asd_exome_20221117_ANV5_202403040807'), # noqa E501
    mksrc('bigquery', 'datarepo-a245d786', 'ANVIL_ccdg_asc_ndd_daly_talkowski_palotie_asd_exome_20221019_ANV5_202403040815'), # noqa E501
    mksrc('bigquery', 'datarepo-7ddd7425', 'ANVIL_ccdg_asc_ndd_daly_talkowski_parellada_asd_exome_20221108_ANV5_202403040822'),  # noqa E501
    mksrc('bigquery', 'datarepo-aa9f0b28', 'ANVIL_ccdg_asc_ndd_daly_talkowski_pericak_vance_asd_wgs_20221027_ANV5_202403040846'),  # noqa E501
    mksrc('bigquery', 'datarepo-0b4c3cfb', 'ANVIL_ccdg_asc_ndd_daly_talkowski_schloesser_asd_gsa_md_20221025_ANV5_202402291202'),  # noqa E501
    mksrc('bigquery', 'datarepo-8023858b', 'ANVIL_ccdg_asc_ndd_daly_talkowski_weiss_asd_exome_20221108_ANV5_202403040925'), # noqa E501
    mksrc('bigquery', 'datarepo-381b5d80', 'ANVIL_ccdg_broad_ai_ibd_alm_gmc_wes_20230328_ANV5_202403040932'),
    mksrc('bigquery', 'datarepo-714d60b9', 'ANVIL_ccdg_broad_ai_ibd_daly_alm_gmc_gsa_20221025_ANV5_202402291210'),
    mksrc('bigquery', 'datarepo-86a1dbf3', 'ANVIL_ccdg_broad_ai_ibd_daly_bernstein_gsa_20221025_ANV5_202304241921', pop), # noqa E501
    mksrc('bigquery', 'datarepo-dc7a9acd', 'ANVIL_ccdg_broad_ai_ibd_daly_brant_niddk_gsa_20240103_ANV5_202401112147'),
    mksrc('bigquery', 'datarepo-916fc0b6', 'ANVIL_ccdg_broad_ai_ibd_daly_duerr_niddk_gsa_20240113_ANV5_202402062134'),
    mksrc('bigquery', 'datarepo-48d85607', 'ANVIL_ccdg_broad_ai_ibd_daly_hyams_protect_wes_20240104_ANV5_202403041011'),
    mksrc('bigquery', 'datarepo-21d3c731', 'ANVIL_ccdg_broad_ai_ibd_daly_kupcinskas_wes_20240104_ANV5_202403041018'),
    mksrc('bigquery', 'datarepo-614a8519', 'ANVIL_ccdg_broad_ai_ibd_daly_lewis_ccfa_wes_20240113_ANV5_202403041026'),
    mksrc('bigquery', 'datarepo-6799d240', 'ANVIL_ccdg_broad_ai_ibd_daly_lewis_sparc_gsa_20240104_ANV5_202401121517'),
    mksrc('bigquery', 'datarepo-d7ae08a2', 'ANVIL_ccdg_broad_ai_ibd_daly_louis_wes_20240104_ANV5_202403041042'),
    mksrc('bigquery', 'datarepo-9b04a16e', 'ANVIL_ccdg_broad_ai_ibd_daly_mccauley_gsa_20240113_ANV5_202402062137'),
    mksrc('bigquery', 'datarepo-b6a95447', 'ANVIL_ccdg_broad_ai_ibd_daly_mccauley_wes_20240104_ANV5_202403041049'),
    mksrc('bigquery', 'datarepo-df7a6188', 'ANVIL_ccdg_broad_ai_ibd_daly_mcgovern_gsa_20240118_ANV5_202402062140'),
    mksrc('bigquery', 'datarepo-5cd83e88', 'ANVIL_ccdg_broad_ai_ibd_daly_mcgovern_niddk_wes_20240104_ANV5_202403041057'), # noqa E501
    mksrc('bigquery', 'datarepo-fa7e066f', 'ANVIL_ccdg_broad_ai_ibd_daly_mcgovern_share_wes_20240104_ANV5_202401121556'), # noqa E501
    mksrc('bigquery', 'datarepo-2def0ed8', 'ANVIL_ccdg_broad_ai_ibd_daly_moayyedi_imagine_gsa_20240105_ANV5_202401121603'), # noqa E501
    mksrc('bigquery', 'datarepo-6e9fe586', 'ANVIL_ccdg_broad_ai_ibd_daly_moayyedi_imagine_wes_20240105_ANV5_202403041109'), # noqa E501
    mksrc('bigquery', 'datarepo-1f3dab2b', 'ANVIL_ccdg_broad_ai_ibd_daly_pekow_share_gsa_20240105_ANV5_202401121646'),
    mksrc('bigquery', 'datarepo-74869ac4', 'ANVIL_ccdg_broad_ai_ibd_daly_pekow_share_wes_20240105_ANV5_202403041133'),
    mksrc('bigquery', 'datarepo-d95b9a73', 'ANVIL_ccdg_broad_ai_ibd_niddk_daly_brant_wes_20240112_ANV5_202403041232'),
    mksrc('bigquery', 'datarepo-7a0883a4', 'ANVIL_ccdg_broad_cvd_af_pegasus_hmb_20221025_ANV5_202403030736'),
    mksrc('bigquery', 'datarepo-f62c5ebd', 'ANVIL_ccdg_broad_cvd_eocad_promis_wgs_20221213_ANV5_202403030935'),
    mksrc('bigquery', 'datarepo-9d116a5c', 'ANVIL_ccdg_broad_mi_atvb_ds_cvd_wes_20221025_ANV5_202403031035'),
    mksrc('bigquery', 'datarepo-bb315b29', 'ANVIL_ccdg_nygc_np_autism_tasc_wgs_20221024_ANV5_202403032216'),
    mksrc('bigquery', 'datarepo-33e3428b', 'ANVIL_ccdg_washu_cvd_np_ai_controls_vccontrols_wgs_20221024_ANV5_202403032319'), # noqa E501
    mksrc('bigquery', 'datarepo-17c5f983', 'ANVIL_cmg_broad_brain_engle_wgs_20221202_ANV5_202402290614'),
    mksrc('bigquery', 'datarepo-a46c0244', 'ANVIL_nhgri_broad_ibd_daly_kugathasan_wes_20240112_ANV5_202403041258'),
    mksrc('bigquery', 'datarepo-4b4f2325', 'ANVIL_nhgri_broad_ibd_daly_turner_wes_20240112_ANV5_202403041307'),
    # @formatter:on
]))

anvil6_sources = mkdict(anvil5_sources, 250, mkdelta([
    # @formatter:off
    mksrc('bigquery', 'datarepo-38af6304', 'ANVIL_1000G_PRIMED_data_model_20240410_ANV5_202404101419'),
    mksrc('bigquery', 'datarepo-1a86e7ca', 'ANVIL_CCDG_Baylor_CVD_AFib_Groningen_WGS_20221122_ANV5_202304242224', pop),
    mksrc('bigquery', 'datarepo-92716a90', 'ANVIL_CCDG_Baylor_CVD_AFib_VAFAR_HMB_IRB_WGS_20221020_ANV5_202304211525', pop), # noqa E501
    mksrc('bigquery', 'datarepo-e8fc4258', 'ANVIL_CCDG_Baylor_CVD_ARIC_20231008_ANV5_202403030358'),
    mksrc('bigquery', 'datarepo-77445496', 'ANVIL_CCDG_Baylor_CVD_EOCAD_BioMe_WGS_20221122_ANV5_202304242226', pop),
    mksrc('bigquery', 'datarepo-1b0d6b90', 'ANVIL_CCDG_Baylor_CVD_HHRC_Brownsville_GRU_WGS_20221122_ANV5_202304242228', pop), # noqa E501
    mksrc('bigquery', 'datarepo-373b7918', 'ANVIL_CCDG_Baylor_CVD_HemStroke_BNI_HMB_WGS_20221215_ANV5_202304242306', pop), # noqa E501
    mksrc('bigquery', 'datarepo-efc3e806', 'ANVIL_CCDG_Baylor_CVD_HemStroke_Duke_DS_WGS_20221117_ANV5_202304242122', pop), # noqa E501
    mksrc('bigquery', 'datarepo-1044f96d', 'ANVIL_CCDG_Baylor_CVD_HemStroke_ERICH_WGS_20221207_ANV5_202304271256', pop),
    mksrc('bigquery', 'datarepo-f23a6ec8', 'ANVIL_CCDG_Baylor_CVD_HemStroke_GERFHS_HMB_WGS_20221215_ANV5_202304242307', pop), # noqa E501
    mksrc('bigquery', 'datarepo-de34ca6e', 'ANVIL_CCDG_Baylor_CVD_HemStroke_Regards_DS_WGS_20221117_ANV5_202304242123', pop), # noqa E501
    mksrc('bigquery', 'datarepo-d9c6f406', 'ANVIL_CCDG_Baylor_CVD_HemStroke_Yale_HMB_WGS_20221215_ANV5_202304242309', pop), # noqa E501
    mksrc('bigquery', 'datarepo-56883e56', 'ANVIL_CCDG_Baylor_CVD_Oregon_SUDS_GRU_WGS_20221215_ANV5_202304242302', pop),
    mksrc('bigquery', 'datarepo-7f3ba7ec', 'ANVIL_CCDG_Baylor_CVD_TexGen_DS_WGS_20221117_ANV5_202304242125', pop),
    mksrc('bigquery', 'datarepo-da965e26', 'ANVIL_CCDG_Baylor_CVD_Ventura_Presto_GRU_IRB_WGS_20221117_ANV5_202304242127', pop), # noqa E501
    mksrc('bigquery', 'datarepo-40647d03', 'ANVIL_CCDG_Broad_AI_IBD_Brant_DS_IBD_WGS_20240113_ANV5_202401141252'),
    mksrc('bigquery', 'datarepo-83339911', 'ANVIL_CCDG_Broad_AI_IBD_Brant_HMB_WGS_20240113_ANV5_202401141259'),
    mksrc('bigquery', 'datarepo-3f36066b', 'ANVIL_CCDG_Broad_AI_IBD_Cho_WGS_20240113_ANV5_202403030543'),
    mksrc('bigquery', 'datarepo-65e890b6', 'ANVIL_CCDG_Broad_AI_IBD_Kugathasan_WGS_20240113_ANV5_202403030551'),
    mksrc('bigquery', 'datarepo-cec499cd', 'ANVIL_CCDG_Broad_AI_IBD_McCauley_WGS_20240114_ANV5_202403030559'),
    mksrc('bigquery', 'datarepo-8043de16', 'ANVIL_CCDG_Broad_AI_IBD_McGovern_WGS_20240113_ANV5_202403030608'),
    mksrc('bigquery', 'datarepo-de3bfd4e', 'ANVIL_CCDG_Broad_AI_IBD_Newberry_WGS_20240113_ANV5_202403030616'),
    mksrc('bigquery', 'datarepo-ed109b2f', 'ANVIL_CCDG_Broad_CVD_AF_BioVU_HMB_GSO_Arrays_20230612_ANV5_202306131350', pop), # noqa E501
    mksrc('bigquery', 'datarepo-3d8b62d7', 'ANVIL_CCDG_Broad_CVD_AF_BioVU_HMB_GSO_WES_20221025_ANV5_202304241856', pop),
    mksrc('bigquery', 'datarepo-450ba911', 'ANVIL_CCDG_Broad_CVD_AF_ENGAGE_DS_WES_20230418_ANV5_202304210808', pop),
    mksrc('bigquery', 'datarepo-0768a322', 'ANVIL_CCDG_Broad_CVD_AF_Ellinor_MGH_Arrays_20221024_ANV5_202304211831', pop), # noqa E501
    mksrc('bigquery', 'datarepo-dfabf632', 'ANVIL_CCDG_Broad_CVD_AF_Ellinor_MGH_WES_20221117_ANV5_202304271354', pop),
    mksrc('bigquery', 'datarepo-485eb707', 'ANVIL_CCDG_Broad_CVD_AF_Figtree_BioHeart_Arrays_20230128_ANV5_202304271554', pop), # noqa E501
    mksrc('bigquery', 'datarepo-58dffe5a', 'ANVIL_CCDG_Broad_CVD_AF_GAPP_DS_MDS_Arrays_20221103_ANV5_202304242105', pop), # noqa E501
    mksrc('bigquery', 'datarepo-cf7f2c0c', 'ANVIL_CCDG_Broad_CVD_AF_GAPP_DS_MDS_WES_20221103_ANV5_202304242107', pop),
    mksrc('bigquery', 'datarepo-f896734e', 'ANVIL_CCDG_Broad_CVD_AF_Marcus_UCSF_Arrays_20221102_ANV5_202304242039', pop), # noqa E501
    mksrc('bigquery', 'datarepo-40c2f4f4', 'ANVIL_CCDG_Broad_CVD_AF_Marcus_UCSF_WES_20221222_ANV5_202304242040', pop),
    mksrc('bigquery', 'datarepo-67117555', 'ANVIL_CCDG_Broad_CVD_AF_Rienstra_WES_20221222_ANV5_202304242035', pop),
    mksrc('bigquery', 'datarepo-c45dd622', 'ANVIL_CCDG_Broad_CVD_AF_Swiss_Cases_DS_MDS_Arrays_20221103_ANV5_202304242110', pop), # noqa E501
    mksrc('bigquery', 'datarepo-b12d2e52', 'ANVIL_CCDG_Broad_CVD_AF_Swiss_Cases_DS_MDS_WES_20230118_ANV5_202304242112', pop), # noqa E501
    mksrc('bigquery', 'datarepo-d795027d', 'ANVIL_CCDG_Broad_CVD_AF_VAFAR_Arrays_20221020_ANV5_202304211823', pop),
    mksrc('bigquery', 'datarepo-642829f3', 'ANVIL_CCDG_Broad_CVD_AF_VAFAR_WES_20221024_ANV5_202304211826', pop),
    mksrc('bigquery', 'datarepo-43f6230a', 'ANVIL_CCDG_Broad_CVD_AFib_AFLMU_WGS_20231008_ANV5_202310091911', pop),
    mksrc('bigquery', 'datarepo-2b135baf', 'ANVIL_CCDG_Broad_CVD_AFib_MGH_WGS_20221024_ANV5_202304211829', pop),
    mksrc('bigquery', 'datarepo-de64d25a', 'ANVIL_CCDG_Broad_CVD_AFib_UCSF_WGS_20221222_ANV5_202304242037', pop),
    mksrc('bigquery', 'datarepo-08216a2c', 'ANVIL_CCDG_Broad_CVD_AFib_Vanderbilt_Ablation_WGS_20221020_ANV5_202304211819', pop), # noqa E501
    mksrc('bigquery', 'datarepo-342c77f2', 'ANVIL_CCDG_Broad_CVD_EOCAD_PartnersBiobank_HMB_Arrays_20230517_ANV5_202312122054'), # noqa E501
    mksrc('bigquery', 'datarepo-a16f8bac', 'ANVIL_CCDG_Broad_CVD_EOCAD_PartnersBiobank_HMB_WES_20230621_ANV5_202403030943'), # noqa E501
    mksrc('bigquery', 'datarepo-f2179275', 'ANVIL_CCDG_Broad_CVD_EOCAD_TaiChi_WGS_20221026_ANV5_202403030955'),
    mksrc('bigquery', 'datarepo-e8ee6358', 'ANVIL_CCDG_Broad_CVD_EOCAD_VIRGO_WGS_20221024_ANV5_202403031003'),
    mksrc('bigquery', 'datarepo-383d9d9b', 'ANVIL_CCDG_Broad_CVD_PROMIS_GRU_WES_20230418_ANV5_202306211912', pop),
    mksrc('bigquery', 'datarepo-318ae48e', 'ANVIL_CCDG_Broad_CVD_Stroke_BRAVE_WGS_20221107_ANV5_202304241543', pop),
    mksrc('bigquery', 'datarepo-7ea7a6e9', 'ANVIL_CCDG_Broad_MI_BRAVE_GRU_WES_20221107_ANV5_202304241545', pop),
    mksrc('bigquery', 'datarepo-5df71da4', 'ANVIL_CCDG_Broad_MI_InStem_WES_20221122_ANV5_202304242236', pop),
    mksrc('bigquery', 'datarepo-1793828c', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSALF_HMB_IRB_GSRS_WES_20230324_ANV5_202304241752', pop), # noqa E501
    mksrc('bigquery', 'datarepo-0db6105c', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSALF_HMB_IRB_WES_20230128_ANV5_202402020211'), # noqa E501
    mksrc('bigquery', 'datarepo-70c803d7', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EPIL_BA_MDS_GSA_MD_20221117_ANV5_202304271400', pop), # noqa E501
    mksrc('bigquery', 'datarepo-1b92691d', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EPIL_BA_MDS_WES_20221101_ANV5_202403031115'), # noqa E501
    mksrc('bigquery', 'datarepo-f5a4a895', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EPI_BA_ID_MDS_GSA_MD_20221117_ANV5_202304271358', pop), # noqa E501
    mksrc('bigquery', 'datarepo-3da39a32', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EPI_BA_ID_MDS_WES_20221101_ANV5_202403031123'), # noqa E501
    mksrc('bigquery', 'datarepo-b8b8ba44', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EP_BA_CN_ID_MDS_GSA_MD_20221117_ANV5_202304271356', pop), # noqa E501
    mksrc('bigquery', 'datarepo-b3e42c63', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSAUS_EP_BA_CN_ID_MDS_WES_20221101_ANV5_202403031131'), # noqa E501
    mksrc('bigquery', 'datarepo-a2b20d71', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSRMB_DS_EAED_IRB_WES_20230621_ANV5_202402020256'), # noqa E501
    mksrc('bigquery', 'datarepo-f85048a3', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSRMB_DS_EAED_MDS_GSA_MD_20221117_ANV5_202304271401', pop), # noqa E501
    mksrc('bigquery', 'datarepo-b3ef2bd3', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUSRMB_DS_EAED_MDS_WES_20221026_ANV5_202403031140'), # noqa E501
    mksrc('bigquery', 'datarepo-1cafba94', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUTMUV_DS_NS_ADLT_WES_20230128_ANV5_202402020305'), # noqa E501
    mksrc('bigquery', 'datarepo-006c9286', 'ANVIL_CCDG_Broad_NP_Epilepsy_AUTMUV_DS_NS_WES_20230314_ANV5_202402020314'),
    mksrc('bigquery', 'datarepo-92905a2b', 'ANVIL_CCDG_Broad_NP_Epilepsy_BELATW_GRU_GSA_MD_20221117_ANV5_202304271403', pop), # noqa E501
    mksrc('bigquery', 'datarepo-33e1bed9', 'ANVIL_CCDG_Broad_NP_Epilepsy_BELATW_GRU_WES_20221108_ANV5_202402020322'),
    mksrc('bigquery', 'datarepo-3f3ad5c7', 'ANVIL_CCDG_Broad_NP_Epilepsy_BELULB_DS_EP_NPU_GSA_MD_20230118_ANV5_202304271404', pop), # noqa E501
    mksrc('bigquery', 'datarepo-b2a5eccc', 'ANVIL_CCDG_Broad_NP_Epilepsy_BELULB_DS_EP_NPU_WES_20221027_ANV5_202403031148'), # noqa E501
    mksrc('bigquery', 'datarepo-7a7b911a', 'ANVIL_CCDG_Broad_NP_Epilepsy_BRAUSP_DS_WES_20240201_ANV5_202402020339'),
    mksrc('bigquery', 'datarepo-33634ed0', 'ANVIL_CCDG_Broad_NP_Epilepsy_CANCAL_GRU_v2_WES_20240201_ANV5_202402020347'),
    mksrc('bigquery', 'datarepo-47f93bbb', 'ANVIL_CCDG_Broad_NP_Epilepsy_CANUTN_DS_EP_WES_20230328_ANV5_202403031156'),
    mksrc('bigquery', 'datarepo-389af3b3', 'ANVIL_CCDG_Broad_NP_Epilepsy_CHEUBB_HMB_IRB_MDS_WES_20221102_ANV5_202403031205'), # noqa E501
    mksrc('bigquery', 'datarepo-ac8e01aa', 'ANVIL_CCDG_Broad_NP_Epilepsy_CYPCYP_HMB_NPU_MDS_WES_20230328_ANV5_202403031213'), # noqa E501
    mksrc('bigquery', 'datarepo-5d4aa202', 'ANVIL_CCDG_Broad_NP_Epilepsy_CZEMTH_GRU_WES_20221108_ANV5_202403031222'),
    mksrc('bigquery', 'datarepo-bd066b5a', 'ANVIL_CCDG_Broad_NP_Epilepsy_DEUPUM_HMB_MDS_WES_20230328_ANV5_202403031231'), # noqa E501
    mksrc('bigquery', 'datarepo-17de3c3b', 'ANVIL_CCDG_Broad_NP_Epilepsy_DEUUGS_DS_EP_MDS_WES_20240201_ANV5_202403031239'), # noqa E501
    mksrc('bigquery', 'datarepo-46e7e2ab', 'ANVIL_CCDG_Broad_NP_Epilepsy_DEUUKB_HMB_NPU_MDS_WES_20230328_ANV5_202403031247'), # noqa E501
    mksrc('bigquery', 'datarepo-ba863f29', 'ANVIL_CCDG_Broad_NP_Epilepsy_DEUUKL_HMB_WES_20221102_ANV5_202403031256'),
    mksrc('bigquery', 'datarepo-113d9969', 'ANVIL_CCDG_Broad_NP_Epilepsy_DEUULG_GRU_WES_20221108_ANV5_202403031305'),
    mksrc('bigquery', 'datarepo-fd6d20c8', 'ANVIL_CCDG_Broad_NP_Epilepsy_DEUUTB_HMB_NPU_MDS_WES_20230328_ANV5_202403031313'), # noqa E501
    mksrc('bigquery', 'datarepo-55d32c1b', 'ANVIL_CCDG_Broad_NP_Epilepsy_FINKPH_EPIL_CO_MORBIDI_MDS_WES_20230328_ANV5_202403031322'), # noqa E501
    mksrc('bigquery', 'datarepo-844a1ecf', 'ANVIL_CCDG_Broad_NP_Epilepsy_FINUVH_HMB_NPU_MDS_WES_20221114_ANV5_202403031331'), # noqa E501
    mksrc('bigquery', 'datarepo-1cbd28a5', 'ANVIL_CCDG_Broad_NP_Epilepsy_FRALYU_HMB_WES_20230621_ANV5_202403031340'),
    mksrc('bigquery', 'datarepo-b8b0b663', 'ANVIL_CCDG_Broad_NP_Epilepsy_GBRSWU_CARDI_NEURO_WES_20221026_ANV5_202403031348'), # noqa E501
    mksrc('bigquery', 'datarepo-2686a76a', 'ANVIL_CCDG_Broad_NP_Epilepsy_GBRUNL_EP_ETIOLOGY_MDS_WES_20221027_ANV5_202403031405'), # noqa E501
    mksrc('bigquery', 'datarepo-05e028a4', 'ANVIL_CCDG_Broad_NP_Epilepsy_GBRUNL_GRU_WES_20221108_ANV5_202403031413'),
    mksrc('bigquery', 'datarepo-4a6228be', 'ANVIL_CCDG_Broad_NP_Epilepsy_GHAKNT_GRU_WES_20221122_ANV5_202403031421'),
    mksrc('bigquery', 'datarepo-98dddf8f', 'ANVIL_CCDG_Broad_NP_Epilepsy_HKGHKK_HMB_MDS_WES_20230328_ANV5_202403031430'), # noqa E501
    mksrc('bigquery', 'datarepo-9ed2a64a', 'ANVIL_CCDG_Broad_NP_Epilepsy_HKOSB_GRU_WES_20230110_ANV5_202403031439'),
    mksrc('bigquery', 'datarepo-22a9e8bd', 'ANVIL_CCDG_Broad_NP_Epilepsy_HRVUZG_HMB_MDS_WES_20221114_ANV5_202403031446'), # noqa E501
    mksrc('bigquery', 'datarepo-517eda47', 'ANVIL_CCDG_Broad_NP_Epilepsy_IRLRCI_GRU_IRB_WES_20230328_ANV5_202403031454'), # noqa E501
    mksrc('bigquery', 'datarepo-b6e444c4', 'ANVIL_CCDG_Broad_NP_Epilepsy_ITAICB_HMB_NPU_MDS_WES_20230223_ANV5_202403031503'), # noqa E501
    mksrc('bigquery', 'datarepo-d8145bea', 'ANVIL_CCDG_Broad_NP_Epilepsy_ITAIGI_GRU_WES_20221108_ANV5_202403031512'),
    mksrc('bigquery', 'datarepo-67c3b200', 'ANVIL_CCDG_Broad_NP_Epilepsy_ITAUBG_DS_EPI_NPU_MDS_WES_20221027_ANV5_202403031520'), # noqa E501
    mksrc('bigquery', 'datarepo-4476c338', 'ANVIL_CCDG_Broad_NP_Epilepsy_ITAUMC_DS_NEURO_MDS_WES_20221108_ANV5_202403031529'), # noqa E501
    mksrc('bigquery', 'datarepo-5cd83a64', 'ANVIL_CCDG_Broad_NP_Epilepsy_ITAUMR_GRU_NPU_WES_20221114_ANV5_202403031537'), # noqa E501
    mksrc('bigquery', 'datarepo-5115b904', 'ANVIL_CCDG_Broad_NP_Epilepsy_JPNFKA_GRU_WES_20221220_ANV5_202403031547'),
    mksrc('bigquery', 'datarepo-f7fb0742', 'ANVIL_CCDG_Broad_NP_Epilepsy_JPNRKI_DS_NPD_IRB_NPU_WES_20221027_ANV5_202402062057'), # noqa E501
    mksrc('bigquery', 'datarepo-b979e83a', 'ANVIL_CCDG_Broad_NP_Epilepsy_KENKIL_GRU_WES_20230110_ANV5_202403031555'),
    mksrc('bigquery', 'datarepo-54571a90', 'ANVIL_CCDG_Broad_NP_Epilepsy_LEBABM_DS_Epilepsy_WES_20230328_ANV5_202403031603'), # noqa E501
    mksrc('bigquery', 'datarepo-5495da63', 'ANVIL_CCDG_Broad_NP_Epilepsy_LEBABM_GRU_WES_20230110_ANV5_202403031612'),
    mksrc('bigquery', 'datarepo-7275a9bd', 'ANVIL_CCDG_Broad_NP_Epilepsy_LTUUHK_HMB_NPU_MDS_WES_20221114_ANV5_202403031621'), # noqa E501
    mksrc('bigquery', 'datarepo-2c2a7d19', 'ANVIL_CCDG_Broad_NP_Epilepsy_NZLUTO_EPIL_BC_ID_MDS_WES_20230328_ANV5_202403031629'), # noqa E501
    mksrc('bigquery', 'datarepo-edbd02ca', 'ANVIL_CCDG_Broad_NP_Epilepsy_TURBZU_GRU_WES_20221108_ANV5_202403031637'),
    mksrc('bigquery', 'datarepo-225a7340', 'ANVIL_CCDG_Broad_NP_Epilepsy_TURIBU_DS_NEURO_AD_NPU_WES_20221027_ANV5_202403031645'), # noqa E501
    mksrc('bigquery', 'datarepo-97dadba8', 'ANVIL_CCDG_Broad_NP_Epilepsy_TWNCGM_HMB_NPU_AdultsONLY_WES_20240201_ANV5_202402020902'), # noqa E501
    mksrc('bigquery', 'datarepo-6dcb5d39', 'ANVIL_CCDG_Broad_NP_Epilepsy_USABCH_EPI_MUL_CON_MDS_WES_20221027_ANV5_202403031701'), # noqa E501
    mksrc('bigquery', 'datarepo-fb4ac7d8', 'ANVIL_CCDG_Broad_NP_Epilepsy_USABLC_GRU_NPU_WES_20221215_ANV5_202402062059'), # noqa E501
    mksrc('bigquery', 'datarepo-5de241b3', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACCF_HMB_MDS_WES_20221207_ANV5_202403031709'), # noqa E501
    mksrc('bigquery', 'datarepo-62a84074', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACCH_DS_NEURO_MDS_WES_20221116_ANV5_202403031719'), # noqa E501
    mksrc('bigquery', 'datarepo-7c06247a', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACHP_GRU_WES_20230612_ANV5_202402062101'),
    mksrc('bigquery', 'datarepo-9042eb4a', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACRW_DS_EP_MDS_WES_20221027_ANV5_202403031727'), # noqa E501
    mksrc('bigquery', 'datarepo-cb75258b', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACRW_DS_SEIZD_WES_20221027_ANV5_202403031735'), # noqa E501
    mksrc('bigquery', 'datarepo-744bc858', 'ANVIL_CCDG_Broad_NP_Epilepsy_USACRW_EPI_ASZ_MED_MDS_WES_20221027_ANV5_202403031744'), # noqa E501
    mksrc('bigquery', 'datarepo-faff5b2b', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAEGP_GRU_WES_20221110_ANV5_202403031752'),
    mksrc('bigquery', 'datarepo-275b2a46', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAFEB_GRU_WES_20221205_ANV5_202403031800'),
    mksrc('bigquery', 'datarepo-5a548fd8', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAHEP_GRU_WES_20230328_ANV5_202403031809'),
    mksrc('bigquery', 'datarepo-999301d3', 'ANVIL_CCDG_Broad_NP_Epilepsy_USALCH_HMB_WES_20230126_ANV5_202402021048'),
    mksrc('bigquery', 'datarepo-eda3f720', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAMGH_HMB_MDS_WES_20221207_ANV5_202403031817'), # noqa E501
    mksrc('bigquery', 'datarepo-d9e55ea0', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAMGH_MGBB_HMB_MDS_WES_20221207_ANV5_202403031826'), # noqa E501
    mksrc('bigquery', 'datarepo-6a627e94', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAMON_GRU_NPU_WES_20221215_ANV5_202403031834'), # noqa E501
    mksrc('bigquery', 'datarepo-bfa59a11', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAMON_GRU_WES_20240201_ANV5_202403031842'),
    mksrc('bigquery', 'datarepo-f8d5318a', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAMON_HMB_WES_20230131_ANV5_202402021131'),
    mksrc('bigquery', 'datarepo-4ef1d979', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAMSS_DS_EP_NEURO_MDS_WES_20230612_ANV5_202402021139'), # noqa E501
    mksrc('bigquery', 'datarepo-5e00a0df', 'ANVIL_CCDG_Broad_NP_Epilepsy_USANCH_DS_NEURO_MDS_WES_20221108_ANV5_202402062105'), # noqa E501
    mksrc('bigquery', 'datarepo-10948836', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAUPN_Marsh_GRU_NPU_WES_20221114_ANV5_202403031858'), # noqa E501
    mksrc('bigquery', 'datarepo-0a247e9e', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAUPN_Marsh_GRU_WES_20230328_ANV5_202403031906'), # noqa E501
    mksrc('bigquery', 'datarepo-154b4ef8', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAUPN_Rader_GRU_WES_20230328_ANV5_202403031915'), # noqa E501
    mksrc('bigquery', 'datarepo-07b8d88c', 'ANVIL_CCDG_Broad_NP_Epilepsy_USAVAN_HMB_GSO_WES_20221207_ANV5_202402021226'), # noqa E501
    mksrc('bigquery', 'datarepo-1985a01d', 'ANVIL_CCDG_Broad_Spalletta_HMB_NPU_MDS_WES_20221102_ANV5_202403031942'),
    mksrc('bigquery', 'datarepo-ad61c47e', 'ANVIL_CCDG_NHGRI_Broad_ASD_Daly_phs000298_WES_vcf_20230403_ANV5_202304271610', pop), # noqa E501
    mksrc('bigquery', 'datarepo-5e719362', 'ANVIL_CCDG_NYGC_AI_Asthma_Gala2_WGS_20230605_ANV5_202306131248', pop),
    mksrc('bigquery', 'datarepo-2734a0e4', 'ANVIL_CCDG_NYGC_NP_Alz_EFIGA_WGS_20230605_ANV5_202306141705', pop),
    mksrc('bigquery', 'datarepo-710fc60d', 'ANVIL_CCDG_NYGC_NP_Alz_LOAD_WGS_20230605_ANV5_202306131256', pop),
    mksrc('bigquery', 'datarepo-9626b3eb', 'ANVIL_CCDG_NYGC_NP_Alz_WHICAP_WGS_20230605_ANV5_202306131303', pop),
    mksrc('bigquery', 'datarepo-86bb81c0', 'ANVIL_CCDG_NYGC_NP_Autism_ACE2_GRU_MDS_WGS_20230605_ANV5_202403032029'),
    mksrc('bigquery', 'datarepo-85674dce', 'ANVIL_CCDG_NYGC_NP_Autism_AGRE_WGS_20230605_ANV5_202403081651'),
    mksrc('bigquery', 'datarepo-7d1461b2', 'ANVIL_CCDG_NYGC_NP_Autism_SSC_WGS_20230605_ANV5_202403032206'),
    mksrc('bigquery', 'datarepo-25ec7b57', 'ANVIL_CCDG_WASHU_PAGE_20221220_ANV5_202304271544', pop),
    mksrc('bigquery', 'datarepo-15645b8d', 'ANVIL_CCDG_WashU_CVD_EOCAD_WashU_CAD_DS_WGS_20230525_ANV5_202403040118'),
    mksrc('bigquery', 'datarepo-4a0769c7', 'ANVIL_CCDG_WashU_CVD_EOCAD_WashU_CAD_GRU_IRB_WGS_20230525_ANV5_202403040126'), # noqa E501
    mksrc('bigquery', 'datarepo-b9222139', 'ANVIL_CMG_BROAD_BRAIN_ENGLE_WES_20240205_ANV5_202402051624', pop),
    mksrc('bigquery', 'datarepo-7e094253', 'ANVIL_CMG_BROAD_BRAIN_SHERR_WGS_20221102_ANV5_202402281543', pop),
    mksrc('bigquery', 'datarepo-c797490f', 'ANVIL_CMG_BROAD_ORPHAN_SCOTT_WGS_20221102_ANV5_202402281552', pop),
    mksrc('bigquery', 'datarepo-0a21cbfd', 'ANVIL_CMG_BaylorHopkins_HMB_IRB_NPU_WES_20221020_ANV5_202402290528'),
    mksrc('bigquery', 'datarepo-d321333c', 'ANVIL_CMG_BaylorHopkins_HMB_NPU_WES_20230525_ANV5_202402290537'),
    mksrc('bigquery', 'datarepo-0a1360b1', 'ANVIL_CMG_Broad_Blood_Gazda_WES_20221117_ANV5_202402290547', pop),
    mksrc('bigquery', 'datarepo-faa71b49', 'ANVIL_CMG_Broad_Blood_Sankaran_WES_20221117_ANV5_202402290555', pop),
    mksrc('bigquery', 'datarepo-abce6387', 'ANVIL_CMG_Broad_Blood_Sankaran_WGS_20221117_ANV5_202402290606', pop),
    mksrc('bigquery', 'datarepo-3dd4d039', 'ANVIL_CMG_Broad_Brain_Gleeson_WES_20221117_ANV5_202304241517', pop),
    mksrc('bigquery', 'datarepo-c361373f', 'ANVIL_CMG_Broad_Brain_Muntoni_WES_20221102_ANV5_202304241527', pop),
    mksrc('bigquery', 'datarepo-fc6ce406', 'ANVIL_CMG_Broad_Brain_NeuroDev_WES_20240112_ANV5_202401152208'),
    mksrc('bigquery', 'datarepo-d7bfafc6', 'ANVIL_CMG_Broad_Brain_Thaker_WES_20221102_ANV5_202304241531', pop),
    mksrc('bigquery', 'datarepo-7e03b5fd', 'ANVIL_CMG_Broad_Brain_Walsh_WES_20230605_ANV5_202310101734', pop),
    mksrc('bigquery', 'datarepo-29812b42', 'ANVIL_CMG_Broad_Eye_Pierce_WES_20221205_ANV5_202304242250', pop),
    mksrc('bigquery', 'datarepo-48134558', 'ANVIL_CMG_Broad_Eye_Pierce_WGS_20221117_ANV5_202304241507', pop),
    mksrc('bigquery', 'datarepo-36ebaa12', 'ANVIL_CMG_Broad_Heart_PCGC_Tristani_WGS_20221025_ANV5_202304211840', pop),
    mksrc('bigquery', 'datarepo-f9826139', 'ANVIL_CMG_Broad_Heart_Seidman_WES_20221117_ANV5_202304241504', pop),
    mksrc('bigquery', 'datarepo-85952af8', 'ANVIL_CMG_Broad_Kidney_Hildebrandt_WES_20230525_ANV5_202305251733', pop),
    mksrc('bigquery', 'datarepo-ee4ae9a1', 'ANVIL_CMG_Broad_Kidney_Hildebrandt_WGS_20221025_ANV5_202304211844', pop),
    mksrc('bigquery', 'datarepo-cf168274', 'ANVIL_CMG_Broad_Kidney_Pollak_WES_20221025_ANV5_202304211846', pop),
    mksrc('bigquery', 'datarepo-4d47ba2c', 'ANVIL_CMG_Broad_Muscle_Beggs_WGS_20221102_ANV5_202304241533', pop),
    mksrc('bigquery', 'datarepo-82d1271a', 'ANVIL_CMG_Broad_Muscle_Bonnemann_WES_20221117_ANV5_202304241509', pop),
    mksrc('bigquery', 'datarepo-6be3fb25', 'ANVIL_CMG_Broad_Muscle_Bonnemann_WGS_20221117_ANV5_202304241510', pop),
    mksrc('bigquery', 'datarepo-b168eb10', 'ANVIL_CMG_Broad_Muscle_KNC_WES_20221116_ANV5_202304242219', pop),
    mksrc('bigquery', 'datarepo-372244aa', 'ANVIL_CMG_Broad_Muscle_KNC_WGS_20221117_ANV5_202304242221', pop),
    mksrc('bigquery', 'datarepo-c43e7400', 'ANVIL_CMG_Broad_Muscle_Kang_WES_20230525_ANV5_202310101649', pop),
    mksrc('bigquery', 'datarepo-77a6c0aa', 'ANVIL_CMG_Broad_Muscle_Kang_WGS_20221025_ANV5_202304211849', pop),
    mksrc('bigquery', 'datarepo-4153ad1f', 'ANVIL_CMG_Broad_Muscle_Laing_WES_20221208_ANV5_202402291926', pop),
    mksrc('bigquery', 'datarepo-5019143b', 'ANVIL_CMG_Broad_Muscle_Myoseq_WES_20230621_ANV5_202306211852', pop),
    mksrc('bigquery', 'datarepo-27eb651a', 'ANVIL_CMG_Broad_Muscle_Myoseq_WGS_20221208_ANV5_202304271310', pop),
    mksrc('bigquery', 'datarepo-c087af7a', 'ANVIL_CMG_Broad_Muscle_OGrady_WES_20221205_ANV5_202304242252', pop),
    mksrc('bigquery', 'datarepo-db987a2e', 'ANVIL_CMG_Broad_Muscle_Ravenscroft_WES_20221208_ANV5_202304271311', pop),
    mksrc('bigquery', 'datarepo-05df566c', 'ANVIL_CMG_Broad_Muscle_Topf_WES_20221208_ANV5_202304271313', pop),
    mksrc('bigquery', 'datarepo-87d91f06', 'ANVIL_CMG_Broad_Orphan_Chung_WES_20221102_ANV5_202304241534', pop),
    mksrc('bigquery', 'datarepo-25f6b696', 'ANVIL_CMG_Broad_Orphan_Estonia_Ounap_WES_20221117_ANV5_202304241512', pop),
    mksrc('bigquery', 'datarepo-c3b16b41', 'ANVIL_CMG_Broad_Orphan_Estonia_Ounap_WGS_20221205_ANV5_202304242255', pop),
    mksrc('bigquery', 'datarepo-5bbb5a28', 'ANVIL_CMG_Broad_Orphan_Jueppner_WES_20240205_ANV5_202402051640', pop),
    mksrc('bigquery', 'datarepo-32fe2260', 'ANVIL_CMG_Broad_Orphan_Lerner_Ellis_WES_20221102_ANV5_202304241536', pop),
    mksrc('bigquery', 'datarepo-6f9e574e', 'ANVIL_CMG_Broad_Orphan_Manton_WES_20221117_ANV5_202304241513', pop),
    mksrc('bigquery', 'datarepo-53cd689b', 'ANVIL_CMG_Broad_Orphan_Manton_WGS_20221117_ANV5_202304241515', pop),
    mksrc('bigquery', 'datarepo-e7c5babf', 'ANVIL_CMG_Broad_Orphan_Scott_WES_20221025_ANV5_202304241458', pop),
    mksrc('bigquery', 'datarepo-051877f4', 'ANVIL_CMG_Broad_Orphan_Sweetser_WES_20221102_ANV5_202304241539', pop),
    mksrc('bigquery', 'datarepo-555c7706', 'ANVIL_CMG_Broad_Orphan_VCGS_White_WES_20221018_ANV5_202304241522', pop),
    mksrc('bigquery', 'datarepo-3a8f7952', 'ANVIL_CMG_Broad_Orphan_VCGS_White_WGS_20221117_ANV5_202304241523', pop),
    mksrc('bigquery', 'datarepo-b699c5e3', 'ANVIL_CMG_Broad_Rare_RGP_WES_20221102_ANV5_202304241540', pop),
    mksrc('bigquery', 'datarepo-2d5bd095', 'ANVIL_CMG_Broad_Stillbirth_Wilkins_Haug_WES_20221102_ANV5_202304241542', pop), # noqa E501
    mksrc('bigquery', 'datarepo-db7353fb', 'ANVIL_CMG_UWASH_DS_BAV_IRB_PUB_RD_20230419_ANV5_202304201858', pop),
    mksrc('bigquery', 'datarepo-3b8ef67a', 'ANVIL_CMG_UWASH_DS_BDIS_20230418_ANV5_202304201958', pop),
    mksrc('bigquery', 'datarepo-5d27ebfe', 'ANVIL_CMG_UWASH_DS_HFA_20230418_ANV5_202304201932', pop),
    mksrc('bigquery', 'datarepo-9d1a6e0a', 'ANVIL_CMG_UWASH_DS_NBIA_20230418_ANV5_202304201949', pop),
    mksrc('bigquery', 'datarepo-18bd3df4', 'ANVIL_CMG_UWASH_HMB_20230418_ANV5_202402070029', pop),
    mksrc('bigquery', 'datarepo-50484f86', 'ANVIL_CMG_UWASH_HMB_IRB_20230418_ANV5_202304201915', pop),
    mksrc('bigquery', 'datarepo-74bd0964', 'ANVIL_CMG_UWash_DS_EP_20230419_ANV5_202304201906', pop),
    mksrc('bigquery', 'datarepo-6f4155f2', 'ANVIL_CMG_UWash_GRU_20240301_ANV5_202403040330', pop),
    mksrc('bigquery', 'datarepo-6486ae96', 'ANVIL_CMG_UWash_GRU_1_20240113_ANV5_202401141440', pop),
    mksrc('bigquery', 'datarepo-97ec5366', 'ANVIL_CMG_UWash_GRU_IRB_20230418_ANV5_202304201940', pop),
    mksrc('bigquery', 'datarepo-cb305c8e', 'ANVIL_CMG_YALE_DS_MC_20221026_ANV5_202402281611'),
    mksrc('bigquery', 'datarepo-c2897355', 'ANVIL_CMG_Yale_DS_BPEAKD_20240113_ANV5_202401141447'),
    mksrc('bigquery', 'datarepo-4b5667f8', 'ANVIL_CMG_Yale_DS_RD_20240113_ANV5_202401141453'),
    mksrc('bigquery', 'datarepo-9e86cb23', 'ANVIL_CMG_Yale_DS_THAL_IRB_20240113_ANV5_202401141500'),
    mksrc('bigquery', 'datarepo-278252c3', 'ANVIL_CMG_Yale_HMB_IRB_20240113_ANV5_202401141507'),
    mksrc('bigquery', 'datarepo-eea2a20c', 'ANVIL_CMH_GAFK_10X_Genomics_20240304_ANV5_202403071539'),
    mksrc('bigquery', 'datarepo-0e0bf0f8', 'ANVIL_CMH_GAFK_ES_20240301_ANV5_202403040338'),
    mksrc('bigquery', 'datarepo-9935aa3f', 'ANVIL_CMH_GAFK_IlluminaGSA_20240311_ANV5_202403121355'),
    mksrc('bigquery', 'datarepo-d391ce5f', 'ANVIL_CMH_GAFK_IsoSeq_20240113_ANV5_202402062116'),
    mksrc('bigquery', 'datarepo-beef6734', 'ANVIL_CMH_GAFK_MGI_20240304_ANV5_202403071559'),
    mksrc('bigquery', 'datarepo-8599b1fb', 'ANVIL_CMH_GAFK_PacBio_methyl_tagged_20240311_ANV5_202403121402'),
    mksrc('bigquery', 'datarepo-94f58e6c', 'ANVIL_CMH_GAFK_SCATAC_20221107_ANV5_202402290954'),
    mksrc('bigquery', 'datarepo-5447de30', 'ANVIL_CMH_GAFK_WGBS_20230327_ANV5_202402062120'),
    mksrc('bigquery', 'datarepo-db73a316', 'ANVIL_CMH_GAFK_WGS_20240113_ANV5_202402062123'),
    mksrc('bigquery', 'datarepo-5227851b', 'ANVIL_CSER_ClinSeq_GRU_20240401_ANV5_202404081541'),
    mksrc('bigquery', 'datarepo-1a706b0c', 'ANVIL_GTEx_Somatic_WGS_20240116_ANV5_202401170147', pop),
    mksrc('bigquery', 'datarepo-8a98bcb4', 'ANVIL_NIMH_Broad_ConvNeuro_McCarroll_Nehme_Levy_CIRM_DS_Village_20240405_ANV5_202404081511'), # noqa E501
    mksrc('bigquery', 'datarepo-c02a5efb', 'ANVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_CIRM_GRU_VillageData_20230109_ANV5_202402292203'), # noqa E501
    mksrc('bigquery', 'datarepo-817f27aa', 'ANVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_CIRM_GRU_WGS_20240206_ANV5_202402081755'), # noqa E501
    mksrc('bigquery', 'datarepo-ddc1d72b', 'ANVIL_NIMH_Broad_ConvergentNeuro_McCarroll_Eggan_Finkel_SMA_DS_WGS_20230109_ANV5_202402292209'), # noqa E501
    mksrc('bigquery', 'datarepo-14f5afa3', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Braff_DS_10XLRGenomes_20221115_ANV5_202310101713', pop), # noqa E501
    mksrc('bigquery', 'datarepo-69e4bc19', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_COGS_DS_WGS_20240113_ANV5_202401152215'),
    mksrc('bigquery', 'datarepo-da595e23', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Escamilla_DS_WGS_20240112_ANV5_202401141541'), # noqa E501
    mksrc('bigquery', 'datarepo-94091a22', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Pato_GRU_10XLRGenomes_20230331_ANV5_202310101715', pop), # noqa E501
    mksrc('bigquery', 'datarepo-df20901c', 'ANVIL_NIMH_Broad_WGSPD_1_McCarroll_Braff_DS_WGS_20240304_ANV5_202403071610'), # noqa E501
    mksrc('bigquery', 'datarepo-75e17b99', 'ANVIL_NIMH_CIRM_FCDI_ConvergentNeuro_McCarroll_Eggan_GRU_Arrays_20230109_ANV5_202402292215'), # noqa E501
    mksrc('bigquery', 'datarepo-25790186', 'ANVIL_PAGE_BioMe_GRU_WGS_20221128_ANV5_202403040429', pop),
    mksrc('bigquery', 'datarepo-b371989b', 'ANVIL_PAGE_MEC_GRU_WGS_20230131_ANV5_202403040437', pop),
    mksrc('bigquery', 'datarepo-4a4eec27', 'ANVIL_PAGE_SoL_HMB_WGS_20221220_ANV5_202403040445', pop),
    mksrc('bigquery', 'datarepo-a1f917db', 'ANVIL_PAGE_Stanford_Global_Reference_Panel_GRU_WGS_20221128_ANV5_202403040453', pop), # noqa E501
    mksrc('bigquery', 'datarepo-6264931f', 'ANVIL_PAGE_WHI_HMB_IRB_WGS_20221019_ANV5_202403040500', pop),
    mksrc('bigquery', 'datarepo-f3817357', 'ANVIL_ccdg_asc_ndd_daly_talkowski_AGRE_asd_exome_20221102_ANV5_202403040528'), # noqa E501
    mksrc('bigquery', 'datarepo-23635d1c', 'ANVIL_ccdg_asc_ndd_daly_talkowski_IBIS_asd_exome_20221024_ANV5_202403040537'), # noqa E501
    mksrc('bigquery', 'datarepo-ecf311e7', 'ANVIL_ccdg_asc_ndd_daly_talkowski_TASC_asd_exome_20221117_ANV5_202403040544'), # noqa E501
    mksrc('bigquery', 'datarepo-90923a9d', 'ANVIL_ccdg_asc_ndd_daly_talkowski_aleksic_asd_exome_20231013_ANV5_202403040600'), # noqa E501
    mksrc('bigquery', 'datarepo-2354d65a', 'ANVIL_ccdg_asc_ndd_daly_talkowski_cdcseed_asd_gsa_md_20221024_ANV5_202402291144', pop), # noqa E501
    mksrc('bigquery', 'datarepo-efc0eb70', 'ANVIL_ccdg_asc_ndd_daly_talkowski_gargus_asd_exome_20231013_ANV5_202403040645'), # noqa E501
    mksrc('bigquery', 'datarepo-d1f95953', 'ANVIL_ccdg_asc_ndd_daly_talkowski_gurrieri_asd_exome_20221024_ANV5_202402291153'), # noqa E501
    mksrc('bigquery', 'datarepo-5590427b', 'ANVIL_ccdg_asc_ndd_daly_talkowski_mayo_asd_exome_20221024_ANV5_202402291115'), # noqa E501
    mksrc('bigquery', 'datarepo-3cbe3dd3', 'ANVIL_ccdg_asc_ndd_daly_talkowski_mcpartland_asd_exome_20221116_ANV5_202403040753'), # noqa E501
    mksrc('bigquery', 'datarepo-a245d786', 'ANVIL_ccdg_asc_ndd_daly_talkowski_palotie_asd_exome_20221019_ANV5_202403040815', pop), # noqa E501
    mksrc('bigquery', 'datarepo-104705f5', 'ANVIL_ccdg_asc_ndd_daly_talkowski_passos_bueno_asd_exome_20221108_ANV5_202403040831'), # noqa E501
    mksrc('bigquery', 'datarepo-a07262c0', 'ANVIL_ccdg_asc_ndd_daly_talkowski_pericak_vance_asd_exome__20221025_ANV5_202403040839'), # noqa E501
    mksrc('bigquery', 'datarepo-418e64c1', 'ANVIL_ccdg_asc_ndd_daly_talkowski_persico_asd_exome_20221027_ANV5_202403040854'), # noqa E501
    mksrc('bigquery', 'datarepo-cfe20662', 'ANVIL_ccdg_asc_ndd_daly_talkowski_renieri_asd_exome_20230327_ANV5_202403040909'), # noqa E501
    mksrc('bigquery', 'datarepo-7c668a5c', 'ANVIL_ccdg_asc_ndd_daly_talkowski_schloesser_asd_exome_20230324_ANV5_202403040917'), # noqa E501
    mksrc('bigquery', 'datarepo-0b4c3cfb', 'ANVIL_ccdg_asc_ndd_daly_talkowski_schloesser_asd_gsa_md_20221025_ANV5_202402291202', pop), # noqa E501
    mksrc('bigquery', 'datarepo-2571477f', 'ANVIL_ccdg_broad_ai_ibd_daly_burnstein_gsa_20240103_ANV5_202401112154'),
    mksrc('bigquery', 'datarepo-c0abacf6', 'ANVIL_ccdg_broad_ai_ibd_daly_chen_gsa_20240103_ANV5_202401112202'),
    mksrc('bigquery', 'datarepo-c7473b33', 'ANVIL_ccdg_broad_ai_ibd_daly_chen_wes_20240103_ANV5_202403040940'),
    mksrc('bigquery', 'datarepo-ac30439c', 'ANVIL_ccdg_broad_ai_ibd_daly_cho_niddk_gsa_20240103_ANV5_202401112215'),
    mksrc('bigquery', 'datarepo-267ea46f', 'ANVIL_ccdg_broad_ai_ibd_daly_chung_gider_gsa_20240103_ANV5_202401121413'),
    mksrc('bigquery', 'datarepo-c481c20f', 'ANVIL_ccdg_broad_ai_ibd_daly_chung_gider_wes_20240103_ANV5_202403040947'),
    mksrc('bigquery', 'datarepo-938f9e89', 'ANVIL_ccdg_broad_ai_ibd_daly_faubion_share_gsa_20240104_ANV5_202401121427'),
    mksrc('bigquery', 'datarepo-d4b1264d', 'ANVIL_ccdg_broad_ai_ibd_daly_faubion_share_wes_20240104_ANV5_202403040954'),
    mksrc('bigquery', 'datarepo-4d149951', 'ANVIL_ccdg_broad_ai_ibd_daly_franchimont_gsa_20240104_ANV5_202401121441'),
    mksrc('bigquery', 'datarepo-e12ce5bd', 'ANVIL_ccdg_broad_ai_ibd_daly_franchimont_wes_20240104_ANV5_202403041001'),
    mksrc('bigquery', 'datarepo-2c7e5905', 'ANVIL_ccdg_broad_ai_ibd_daly_hyams_protect_gsa_20240311_ANV5_202403121623'),
    mksrc('bigquery', 'datarepo-f5463526', 'ANVIL_ccdg_broad_ai_ibd_daly_kastner_fmf_gsa_20240104_ANV5_202401121503'),
    mksrc('bigquery', 'datarepo-51367192', 'ANVIL_ccdg_broad_ai_ibd_daly_kastner_fmf_nhgri_wes_20240104_ANV5_202401152230'), # noqa E501
    mksrc('bigquery', 'datarepo-7268c3a0', 'ANVIL_ccdg_broad_ai_ibd_daly_kupcinskas_gsa_20240311_ANV5_202403121627'),
    mksrc('bigquery', 'datarepo-51449a60', 'ANVIL_ccdg_broad_ai_ibd_daly_lira_share_wes_20240104_ANV5_202403041035'),
    mksrc('bigquery', 'datarepo-ee1b3121', 'ANVIL_ccdg_broad_ai_ibd_daly_louis_gsa_20240311_ANV5_202403121633'),
    mksrc('bigquery', 'datarepo-083044ec', 'ANVIL_ccdg_broad_ai_ibd_daly_newberry_share_gsa_20240105_ANV5_202401121611'), # noqa E501
    mksrc('bigquery', 'datarepo-10ae29e5', 'ANVIL_ccdg_broad_ai_ibd_daly_newberry_share_wes_20240105_ANV5_202403041117'), # noqa E501
    mksrc('bigquery', 'datarepo-a240ffda', 'ANVIL_ccdg_broad_ai_ibd_daly_niddk_cho_wes_20240105_ANV5_202403041125'),
    mksrc('bigquery', 'datarepo-929acb2a', 'ANVIL_ccdg_broad_ai_ibd_daly_rioux_bitton_igenomed_wes_20240105_ANV5_202401121701'), # noqa E501
    mksrc('bigquery', 'datarepo-fa70ba86', 'ANVIL_ccdg_broad_ai_ibd_daly_rioux_genizon_wes_20240311_ANV5_202403121426'),
    mksrc('bigquery', 'datarepo-6e9030de', 'ANVIL_ccdg_broad_ai_ibd_daly_rioux_igenomed_gsa_20240105_ANV5_202401121709'), # noqa E501
    mksrc('bigquery', 'datarepo-c9265cf7', 'ANVIL_ccdg_broad_ai_ibd_daly_rioux_niddk_gsa_20240108_ANV5_202401121716'),
    mksrc('bigquery', 'datarepo-fe283248', 'ANVIL_ccdg_broad_ai_ibd_daly_rioux_niddk_wes_20240108_ANV5_202403041140'),
    mksrc('bigquery', 'datarepo-3ca098f3', 'ANVIL_ccdg_broad_ai_ibd_daly_sands_msccr_gsa_20240108_ANV5_202401121730'),
    mksrc('bigquery', 'datarepo-fd47ae7f', 'ANVIL_ccdg_broad_ai_ibd_daly_sands_msccr_wes_20240108_ANV5_202403041148'),
    mksrc('bigquery', 'datarepo-4300fbc6', 'ANVIL_ccdg_broad_ai_ibd_daly_silverberg_niddk_gsa_20240108_ANV5_202401121745'), # noqa E501
    mksrc('bigquery', 'datarepo-14285871', 'ANVIL_ccdg_broad_ai_ibd_daly_stampfer_nhs_gsa_20240311_ANV5_202403121637'),
    mksrc('bigquery', 'datarepo-d69ac752', 'ANVIL_ccdg_broad_ai_ibd_daly_stampfer_wes_20240108_ANV5_202403041155'),
    mksrc('bigquery', 'datarepo-268dabf8', 'ANVIL_ccdg_broad_ai_ibd_daly_vermeire_gsa_20240113_ANV5_202402062145'),
    mksrc('bigquery', 'datarepo-636bc565', 'ANVIL_ccdg_broad_ai_ibd_daly_vermeire_wes_20240108_ANV5_202403041203'),
    mksrc('bigquery', 'datarepo-7cc92556', 'ANVIL_ccdg_broad_ai_ibd_daly_xavier_prism_gsa_20240108_ANV5_202402062149'),
    mksrc('bigquery', 'datarepo-6b12cac1', 'ANVIL_ccdg_broad_ai_ibd_daly_xavier_prism_wes_20240108_ANV5_202403041214'),
    mksrc('bigquery', 'datarepo-5d4e150c', 'ANVIL_ccdg_broad_ai_ibd_daly_xavier_share_gsa_20240108_ANV5_202401121819'),
    mksrc('bigquery', 'datarepo-e30e7797', 'ANVIL_ccdg_broad_ai_ibd_daly_xavier_share_wes_20240108_ANV5_202403041224'),
    mksrc('bigquery', 'datarepo-597e5f25', 'ANVIL_ccdg_broad_ai_ibd_niddk_daly_duerr_wes_20240112_ANV5_202403041241'),
    mksrc('bigquery', 'datarepo-2f8b185b', 'ANVIL_ccdg_broad_ai_ibd_niddk_daly_silverberg_wes_20240112_ANV5_202403041250'), # noqa E501
    mksrc('bigquery', 'datarepo-7a0883a4', 'ANVIL_ccdg_broad_cvd_af_pegasus_hmb_20221025_ANV5_202403030736', pop),
    mksrc('bigquery', 'datarepo-f62c5ebd', 'ANVIL_ccdg_broad_cvd_eocad_promis_wgs_20221213_ANV5_202403030935', pop),
    mksrc('bigquery', 'datarepo-9d116a5c', 'ANVIL_ccdg_broad_mi_atvb_ds_cvd_wes_20221025_ANV5_202403031035', pop),
    mksrc('bigquery', 'datarepo-6c0a5f0d', 'ANVIL_ccdg_broad_mi_univutah_ds_cvd_wes_20221026_ANV5_202403031059'),
    mksrc('bigquery', 'datarepo-235663ab', 'ANVIL_ccdg_broad_np_epilepsy_usavancontrols_hmb_gso_wes_20221101_ANV5_202403031924'), # noqa E501
    mksrc('bigquery', 'datarepo-81cf50b1', 'ANVIL_ccdg_broad_np_epilepsy_zafagn_ds_epi_como_mds_wes_20221026_ANV5_202403031933'), # noqa E501
    mksrc('bigquery', 'datarepo-e6801146', 'ANVIL_ccdg_nygc_np_autism_hmca_wgs_20221024_ANV5_202403032115'),
    mksrc('bigquery', 'datarepo-64b26798', 'ANVIL_ccdg_washu_ai_t1d_t1dgc_wgs_20221031_ANV5_202403032311'),
    mksrc('bigquery', 'datarepo-e3065356', 'ANVIL_ccdg_washu_cvd_eocad_biome_wgs_20221024_ANV5_202304211601', pop),
    mksrc('bigquery', 'datarepo-01e3396c', 'ANVIL_ccdg_washu_cvd_eocad_cleveland_wgs_20221024_ANV5_202403040008'),
    mksrc('bigquery', 'datarepo-5e62ca4f', 'ANVIL_ccdg_washu_cvd_eocad_emerge_wgs_20221024_ANV5_202403040026'),
    mksrc('bigquery', 'datarepo-a0d77559', 'ANVIL_ccdg_washu_cvd_eocad_emory_wgs_20221024_ANV5_202403040034'),
    mksrc('bigquery', 'datarepo-33e3428b', 'ANVIL_ccdg_washu_cvd_np_ai_controls_vccontrols_wgs_20221024_ANV5_202403032319', pop), # noqa E501
    mksrc('bigquery', 'datarepo-17c5f983', 'ANVIL_cmg_broad_brain_engle_wgs_20221202_ANV5_202402290614', pop),
    mksrc('bigquery', 'datarepo-1cb73890', 'ANVIL_cmg_broad_heart_ware_wes_20221215_ANV5_202304242145', pop),
    mksrc('bigquery', 'datarepo-833ff0a3', 'ANVIL_eMERGE_GRU_IRB_NPU_eMERGEseq_20230130_ANV5_202304271614', pop),
    mksrc('bigquery', 'datarepo-baf040af', 'ANVIL_eMERGE_GRU_IRB_PUB_NPU_eMERGEseq_20230130_ANV5_202304271616', pop),
    mksrc('bigquery', 'datarepo-270b3b62', 'ANVIL_eMERGE_GRU_IRB_eMERGEseq_20230130_ANV5_202304271613', pop),
    mksrc('bigquery', 'datarepo-c13efbe9', 'ANVIL_eMERGE_GRU_NPU_eMERGEseq_20230130_ANV5_202304271617', pop),
    mksrc('bigquery', 'datarepo-34f8138d', 'ANVIL_eMERGE_GRU_eMERGEseq_20230130_ANV5_202304271612', pop),
    mksrc('bigquery', 'datarepo-90b7b6e8', 'ANVIL_eMERGE_HMB_GSO_eMERGEseq_20230130_ANV5_202304271621', pop),
    mksrc('bigquery', 'datarepo-6e6dca92', 'ANVIL_eMERGE_HMB_IRB_PUB_eMERGEseq_20230130_ANV5_202304271622', pop),
    mksrc('bigquery', 'datarepo-1ddf2a8e', 'ANVIL_eMERGE_HMB_NPU_eMERGEseq_20230130_ANV5_202304271624', pop),
    mksrc('bigquery', 'datarepo-dba97a65', 'ANVIL_eMERGE_HMB_eMERGEseq_20230130_ANV5_202304271619', pop),
    mksrc('bigquery', 'datarepo-51aa9a22', 'ANVIL_eMERGE_PGRNseq_20230118_ANV5_202304241853', pop),
    mksrc('bigquery', 'datarepo-ce8c469f', 'ANVIL_eMERGE_PRS_Arrays_20221220_ANV5_202304271346', pop),
    mksrc('bigquery', 'datarepo-bf91a039', 'ANVIL_nhgri_broad_ibd_daly_winter_wes_20240112_ANV5_202403041315'),
    # @formatter:on
]))

anvil7_sources = mkdict(anvil6_sources, 257, mkdelta([
    # @formatter:off
    mksrc('bigquery', 'datarepo-c9e438dc', 'ANVIL_CCDG_Broad_NP_Epilepsy_GBRUCL_DS_EARET_MDS_WES_20221026_ANV5_202406261957'), # noqa E501
    mksrc('bigquery', 'datarepo-90a1d452', 'ANVIL_GREGoR_R01_GRU_20240208_ANV5_202407011515'),
    mksrc('bigquery', 'datarepo-c27c13db', 'ANVIL_GREGoR_R01_HMB_20240208_ANV5_202407011529'),
    mksrc('bigquery', 'datarepo-3594cc06', 'ANVIL_HPRC_20240401_ANV5_202406261913'),
    mksrc('bigquery', 'datarepo-49f55ff6', 'ANVIL_NIMH_Broad_WGSPD1_McCarroll_Light_DS_WGS_20240625_ANV5_202406262032'),
    mksrc('bigquery', 'datarepo-54040f7f', 'ANVIL_T2T_CHRY_20240301_ANV5_202406271432'),
    mksrc('bigquery', 'datarepo-5048eadd', 'ANVIL_ccdg_broad_ai_ibd_daly_brant_burnstein_utsw_wes_20240627_ANV5_202406271535'), # noqa E501
    mksrc('bigquery', 'datarepo-5d003f44', 'ANVIL_ccdg_broad_daly_igsr_1kg_twist_wes_20240625_ANV5_202406261904')
    # @formatter:on
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
