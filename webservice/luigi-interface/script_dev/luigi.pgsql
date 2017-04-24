--
-- PostgreSQL database cluster dump
--

SET default_transaction_read_only = off;

SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

--
-- Drop databases
--

DROP DATABASE monitor;




--
-- Drop roles
--

DROP ROLE monitor;
DROP ROLE postgres;


--
-- Roles
--

CREATE ROLE monitor;
ALTER ROLE monitor WITH SUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION NOBYPASSRLS PASSWORD 'md55cdbdc5f6bb245976cac73a4afc92ed4';
CREATE ROLE postgres;
ALTER ROLE postgres WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN REPLICATION BYPASSRLS;






--
-- Database creation
--

CREATE DATABASE monitor WITH TEMPLATE = template0 OWNER = postgres;
REVOKE CONNECT,TEMPORARY ON DATABASE template1 FROM PUBLIC;
GRANT CONNECT ON DATABASE template1 TO PUBLIC;


\connect monitor

SET default_transaction_read_only = off;

--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.2
-- Dumped by pg_dump version 9.6.2

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: luigi; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE luigi (
    luigi_job character varying(100) NOT NULL,
    status character varying(20),
    submitter_specimen_id character varying(100),
    specimen_uuid character varying(100),
    workflow_name character varying(100),
    center_name character varying(100),
    submitter_donor_id character varying(100),
    consonance_job_uuid character varying(100),
    submitter_donor_primary_site character varying(100),
    project character varying(100),
    analysis_type character varying(100),
    program character varying(100),
    donor_uuid character varying(100),
    submitter_sample_id character varying(100),
    submitter_experimental_design character varying(100),
    submitter_specimen_type character varying(80),
    workflow_version character varying(100),
    sample_uuid character varying(100),
    last_updated character varying,
    start_time character varying
);


ALTER TABLE luigi OWNER TO postgres;

--
-- Data for Name: luigi; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY luigi (luigi_job, status, submitter_specimen_id, specimen_uuid, workflow_name, center_name, submitter_donor_id, consonance_job_uuid, submitter_donor_primary_site, project, analysis_type, program, donor_uuid, submitter_sample_id, submitter_experimental_design, submitter_specimen_type, workflow_version, sample_uuid, last_updated, start_time) FROM stdin;
ConsonanceTask_true_36_false_59c4c8500e	FAILED	THR12_0281_S02	b27745a6-acfd-58ac-8ae3-38885e61ac33	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR12	THR12_0281	04da29d1-a256-4bf0-a128-b0856d2c7aa6	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	d4005e1c-b002-55b6-b3ab-2d114c84512d	THR12_0281_S02	RNA-Seq	Normal - other	3.2.1-1	1702258d-effc-5daf-aede-f146c96fa20b	2017-03-16T05:29:51.225+0000	2017-03-16T00:53:04.794+0000
ConsonanceTask_true_36_false_6c46fadbc1	SUCCESS	LNCAP_batch8_Baseline	2ec9d744-273f-509d-9898-cdd993c63387	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	LNCAP_batch8	670e0daa-6c6c-4499-8de7-7294cd174885	not provided	WCDT	rna_seq_quantification	SU2C	ce3524f6-f106-5a2b-af0d-5208eafa0be6	LNCAP_batch8_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	ecdf47d1-fac3-53c9-a369-83a0c68a74fc	2017-03-19T00:00:27.379+0000	2017-03-16T04:01:53.831+0000
ConsonanceTask_true_36_false_e7e63bcc9f	FAILED	DTB-073_Progression	ead1e6cf-887d-5577-bec5-eeae9ce698aa	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-073	17c1afa9-f777-4728-b642-e35a76fbabcb	not provided	WCDT	rna_seq_quantification	SU2C	45ce02ba-9452-5c99-af85-5f26fde83505	DTB-073_Progression_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	78d2e5f4-a95c-5696-a040-deeb0c6b4d77	2017-03-16T20:35:32.327+0000	2017-03-16T04:02:04.947+0000
ConsonanceTask_true_36_false_265ef36e81	SUCCESS	DTB-154_Baseline	d91d359d-688f-5e2a-9c41-d7cd8c33a4c4	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-154	0605f472-2788-44a0-9f88-05eb0b196390	not provided	WCDT	rna_seq_quantification	SU2C	993149d6-281a-5a09-ad4e-c7d16365c438	DTB-154_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	dfdb5073-81a5-5930-8d82-de698b6c0ee1	2017-03-17T05:07:53.478+0000	2017-03-16T04:01:58.319+0000
ConsonanceTask_true_36_false_6fecba3d53	SUCCESS	DTB-074_Baseline	1180e8cd-a236-55fd-a802-387011a60447	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-074	c9e92efc-c8c5-4aa1-8eec-66b7a03b20a4	not provided	WCDT	rna_seq_quantification	SU2C	a4a941ec-7cc5-53a9-8110-ddb9ad0ca55e	DTB-074_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	3c6bca6d-c0b8-5250-87c4-27fb05238a34	2017-03-17T07:55:29.797+0000	2017-03-16T04:02:11.608+0000
ConsonanceTask_true_36_false_2130c5f338	FAILED	THR10_0229_S01	e34feea8-a545-58df-a9e7-b7343df57853	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0229	d1fc6ddf-b847-4b27-b196-18dbf11a8a0c	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	0e688abe-b915-50c8-a629-195dd1ac6c99	THR10_0229_S01	RNA-Seq	Primary tumour - other	3.2.1-1	9d175071-820c-5cf7-ad17-f66fceb5b55e	2017-03-11T08:41:19.356+0000	2017-03-10T19:10:37.856+0000
ConsonanceTask_true_36_false_4d27775797	FAILED	THR12_0282_S01	3dbbec5d-4f1c-5862-a803-16cf4a5cd336	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR12	THR12_0282	f51d15ed-f544-40d8-a5b3-69383d0de649	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	f6fd54da-e98c-5016-bf16-8f2a8b3dff09	THR12_0282_S01	RNA-Seq	Primary tumour - other	3.2.1-1	78702b37-e4c3-59c8-9b78-6e58ee1bbef4	2017-03-16T05:47:00.540+0000	2017-03-16T00:52:58.251+0000
ConsonanceTask_true_36_false_c1f8786be9	SUCCESS	DTB-061_Baseline	01b09ab8-8bc1-5487-9faf-456b8ec438fc	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-061	924dacd1-de66-4a1a-a358-c7f66841fc07	not provided	WCDT	rna_seq_quantification	SU2C	4a629150-0c7e-5e04-b4cb-08fa504140a4	DTB-061_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	405d4a18-9748-5b32-bc46-5881ef99f1e1	2017-03-18T20:10:53.306+0000	2017-03-16T04:02:20.078+0000
ConsonanceTask_true_36_false_5088ed3c6d	SUCCESS	DTB-194-Progression	a1f39759-773a-5f87-98d1-13e6b76f564a	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-194	900b744a-9fb9-4fbb-8531-57b5ead4e832	not provided	WCDT	rna_seq_quantification	SU2C	621a6f29-78e0-54b2-8b44-3b59bc9bce20	DTB-194-Progression_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	307b91f4-fd10-5a65-ab66-3606e8c6d259	2017-03-18T22:26:17.368+0000	2017-03-16T04:01:25.763+0000
ConsonanceTask_true_36_false_697c2b04f6	FAILED	THR11_0259_S01	5de579c6-c617-5604-9907-915393c8913d	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR11	THR11_0259	86505096-7a2e-4007-bdc8-96f7190c2c4e	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	9fe2ebd8-30db-503b-ae38-28dd3cd40cab	THR11_0259_S01	RNA-Seq	Primary tumour - other	3.2.1-1	11667e46-b9d5-5225-a544-6ac9319efda4	2017-03-14T04:34:49.837+0000	2017-03-13T22:47:45.967+0000
ConsonanceTask_true_36_false_149b3caa3b	FAILED	THR12_0281_S01	d7b83218-6075-5628-8a8c-06e4f8d58cc4	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR12	THR12_0281	dff56258-c96b-47a2-b6c1-f64d830e71d9	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	d4005e1c-b002-55b6-b3ab-2d114c84512d	THR12_0281_S01	RNA-Seq	Primary tumour - other	3.2.1-1	af0293d0-b63d-5485-933a-83d1eb98239a	2017-03-16T08:39:26.950+0000	2017-03-16T00:53:05.819+0000
ConsonanceTask_true_36_false_37db888cea	FAILED	THR10_0243_S01	f6fcd829-645e-5c4a-b15c-2dd2a0ce6113	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0243	5689c5d0-5f00-4956-8f2d-48c085e7606e	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	d7a76fdb-6fbc-5f07-903b-b1416eb657cd	THR10_0243_S01	RNA-Seq	Primary tumour - other	3.2.1-1	ae23f47b-6710-55d0-b41e-7595f3d4a82b	2017-03-11T06:28:55.827+0000	2017-03-10T18:37:03.074+0000
ConsonanceTask_true_36_false_e3f610f14d	FAILED	THR12_0280_S01	632e3c57-2e9d-5b73-bbe2-858963b44787	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR12	THR12_0280	30b18572-6271-451c-90dc-77c2c8863846	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	6005cb08-cd12-5dc5-b05c-2eb79c37ae1d	THR12_0280_S01	RNA-Seq	Primary tumour - other	3.2.1-1	2cafa00f-768e-549a-b7fa-89f4ad08172e	2017-03-16T05:49:03.785+0000	2017-03-16T00:53:02.505+0000
ConsonanceTask_true_36_false_89387f40c2	FAILED	LNCAP_batch5_Baseline	43bc02a8-0871-5224-955d-67c8a4313fbe	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	LNCAP_batch5	c58748eb-76d6-4844-b977-4b34fd76e00f	not provided	WCDT	rna_seq_quantification	SU2C	6680b9bc-2538-5ee1-8757-a59fc47317f4	LNCAP_batch5_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	7e5c95f4-5f53-5487-8514-aeb34a8e7ba5	2017-03-16T05:35:45.843+0000	2017-03-16T04:01:24.729+0000
ConsonanceTask_true_36_false_545b9cfe29	FAILED	THR10_0234_S01	f7865dbd-d88d-5087-a648-522edb1f1301	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0234	77ff4fbd-6544-46ef-b0cd-2628fd85b07e	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	0c2eeb09-c344-508e-a164-5cea29d6f16d	THR10_0234_S01	RNA-Seq	Primary tumour - other	3.2.1-1	c4fbd6d9-a093-57da-8396-e999e64cb98b	2017-03-11T12:00:26.694+0000	2017-03-10T19:10:33.994+0000
ConsonanceTask_true_36_false_7e43b05ff6	SUCCESS	DTB-176-Progression	ebd42658-bbcc-5c41-8ecb-31e5541d87e7	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-176	c50f46b5-8d6b-4ac0-b397-67e25263256d	not provided	WCDT	rna_seq_quantification	SU2C	1c724c2a-4a4a-5df6-abca-5cdfa4d690a0	DTB-176-Progression_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	1c1a767c-f226-5c91-912e-6f35f6c90dc4	2017-03-17T03:30:33.668+0000	2017-03-16T04:02:08.185+0000
ConsonanceTask_true_36_false_ce5af203a1	FAILED	THR10_0227_S01	cee683a3-2f8b-5edf-ab01-0c322418c4f3	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0227	fd153f9a-6434-4f5a-ac6a-8f61fa43b4db	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	56a505e5-b459-57e9-a2b7-ccc5b630679e	THR10_0227_S01	RNA-Seq	Primary tumour - other	3.2.1-1	efb2be65-cca8-5f3b-83c4-0daaae045684	2017-03-11T08:30:57.634+0000	2017-03-10T18:37:07.560+0000
ConsonanceTask_true_36_false_ea093c0332	FAILED	THR10_0233_S01	8805eab3-6a91-527b-84e1-f1a5c4e0ed30	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0233	9d794a1a-0fd6-4d06-b69c-1cb78463755f	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	16a2a63f-3c15-5bc2-bc54-89c9299e1e91	THR10_0233_S01	RNA-Seq	Primary tumour - other	3.2.1-1	0aaea5f4-f1f3-5cab-8fc5-71711659d72c	2017-03-12T16:03:51.093+0000	2017-03-12T08:56:13.011+0000
ConsonanceTask_true_36_false_4373b086bd	SUCCESS	DTB-069-Baseline2	2fcecd56-3824-5f67-978e-30a26549dc2c	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-069	0c4ec467-939b-4bdc-aca7-e47b4f239499	not provided	WCDT	rna_seq_quantification	SU2C	1aa50db8-be90-5c86-9208-4e329e2030c0	DTB-069-Baseline2_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	79378cf1-e780-5e44-9767-bd2ac80450b3	2017-03-17T08:12:43.297+0000	2017-03-16T04:02:21.109+0000
ConsonanceTask_true_36_false_a458813f41	SUCCESS	LNCAP_batch7_Baseline	4385ba3b-f2a3-5964-91bd-4530a67cc23b	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	LNCAP_batch7	33abbbd0-55c7-4cdd-b30e-bebbdf299f23	not provided	WCDT	rna_seq_quantification	SU2C	04f05e80-5d36-5042-805d-a34385f8ed10	LNCAP_batch7_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	24af1b6a-07c2-53a3-a1b5-ce8629ee3721	2017-03-17T04:13:11.767+0000	2017-03-16T04:02:18.773+0000
ConsonanceTask_true_36_false_9659943aab	SUCCESS	DTB-090_Progression	fef670d2-b2d6-52a7-aa38-84630722b281	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-090	de8ddfc9-1727-4096-81df-daa620815ced	not provided	WCDT	rna_seq_quantification	SU2C	c42aa629-2594-566e-a886-7773fa502e43	DTB-090_Progression_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	a4c53901-d056-5ecf-b1f8-3b4d75326eb9	2017-03-17T05:17:10.765+0000	2017-03-16T04:02:07.103+0000
ConsonanceTask_true_36_false_17ed36d51f	FAILED	THR11_0260_S01	7cffcbc1-8c05-5f60-a7b5-dad660a2ff07	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR11	THR11_0260	27e6dc8b-ed0f-4619-83df-9c6f6a5954d2	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	04cb4a45-627c-5e87-a789-94be2c96527e	THR11_0260_S01	RNA-Seq	Primary tumour - other	3.2.1-1	5db5452e-59d4-5272-abbd-ebfac9f1257b	2017-03-14T05:31:28.482+0000	2017-03-13T22:47:54.501+0000
ConsonanceTask_true_36_false_745d03b414	SUCCESS	DTB-031_Baseline	9f8ae892-8cfd-5031-b916-92a2c52bd29c	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-031	abf2a2c0-b9be-4596-9914-75ea015cfc46	not provided	WCDT	rna_seq_quantification	SU2C	b0f4d969-f238-5b1f-be07-8abe5eac4783	DTB-031_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	11cc78d4-58f3-589a-80e5-f1276dcde0dd	2017-03-18T21:43:28.851+0000	2017-03-16T04:02:06.013+0000
ConsonanceTask_true_36_false_f9155c64fc	SUCCESS	DTB-060_Baseline	73e69dc8-af5c-5644-a646-63affb7d57bf	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-060	2e2b18c0-cf84-462d-8985-40f335151fff	not provided	WCDT	rna_seq_quantification	SU2C	56d18524-1258-51a0-b4ea-d68f6777085f	DTB-060_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	48d70635-c642-5a65-9140-64c927f24151	2017-03-17T05:54:00.329+0000	2017-03-16T04:02:15.559+0000
ConsonanceTask_true_36_false_fdb64cc010	SUCCESS	DTB-022_Progression2	18202207-584c-53f8-a3ed-fcf1623b626e	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-022	52daa7c7-2c26-464a-ac01-bd72ac905d64	not provided	WCDT	rna_seq_quantification	SU2C	ae5bee60-10f5-5576-89d0-6128c28ae855	DTB-022_Progression2_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	7f70ca56-4230-5c13-9a1b-022101470014	2017-03-17T07:55:55.696+0000	2017-03-16T04:02:22.198+0000
ConsonanceTask_true_36_false_9c1b3b7462	SUCCESS	DTB-059_Baseline	7f97c641-cecd-5c01-a298-d454d4b82816	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-059	95789f06-0713-4b75-ab0d-08d228f08c6c	not provided	WCDT	rna_seq_quantification	SU2C	a42f9b08-51ec-5b8c-97f5-560d8cd5cb8e	DTB-059_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	49bc6e94-72b2-558d-8c47-0cc9dc9459e9	2017-03-17T06:17:28.157+0000	2017-03-16T04:02:10.447+0000
ConsonanceTask_true_36_false_8ee7380b8d	SUCCESS	DTB-009_Baseline	24cf0fb6-8721-5e18-8e8d-e021a5888aaa	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-009	43c75bca-3b49-41e9-b99f-ae6028e64b51	not provided	WCDT	rna_seq_quantification	SU2C	1e4a0bb5-04ff-5ed9-b9a3-c26d6ba5a671	DTB-009_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	666c149a-d176-5817-af09-50eba4c2d71e	2017-03-17T04:22:46.350+0000	2017-03-16T04:02:14.473+0000
ConsonanceTask_true_36_false_b5d5457d87	SUCCESS	DTB-091_Baseline	70c68d3c-acee-5358-b219-610500b75456	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-091	5f3a0227-33a7-4128-8dd7-c17e7cdf91cc	not provided	WCDT	rna_seq_quantification	SU2C	aa642a6d-1458-56f7-8017-3f0f4770bbde	DTB-091_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	ff761631-cea2-554b-9545-a49ae0dacd04	2017-03-17T06:02:40.545+0000	2017-03-16T04:02:09.286+0000
ConsonanceTask_true_36_false_e9636a137f	FAILED	TH07_0151_S01	be1eec23-4026-51af-8568-20296099580f	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	TH07	TH07_0151	0015cc92-d566-4277-9725-95ee9491557e	BTO:0000214	Expression Analysis	rna_seq_quantification	Treehouse	f0953cc2-552f-58d3-a17e-068592d735e0	TH07_0151_S01	RNA-Seq	Cell line - derived from tumour	3.2.1-1	a5c53c1d-4048-51e2-956e-65815371ed3a	2017-03-16T04:45:27.350+0000	2017-03-16T00:53:09.285+0000
ConsonanceTask_true_36_false_95ff270ea7	SUCCESS	DTB-143_Baseline	74adc5d9-b021-5f60-b302-54096fa72b69	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-143	42f52029-f304-42ce-8eac-df4314df3edb	not provided	WCDT	rna_seq_quantification	SU2C	d012804d-4e6f-5c7a-a3ca-56fc68ef10ef	DTB-143_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	f5f1de7c-a445-5a26-8c0f-e7d4d71cddb3	2017-03-17T04:30:53.234+0000	2017-03-16T04:02:03.859+0000
ConsonanceTask_true_36_false_cf2d909dec	FAILED	THR10_0230_S01	65591443-98bf-5b6a-ba85-10d1cc726ae2	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0230	5f9424fc-e451-486f-bf53-2c9aaa319f04	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	b53cb872-e60a-5cec-8706-275b56fd37fb	THR10_0230_S01	RNA-Seq	Primary tumour - other	3.2.1-1	d3a28b92-6aa5-58ec-b937-5fa844061849	2017-03-11T07:00:56.903+0000	2017-03-10T18:37:01.771+0000
ConsonanceTask_true_36_false_9a8b47ce11	FAILED	THR10_0240_S01	b5e3343e-1ecb-5de7-8f18-1cd8d95806bc	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0240	a3f2d90b-b9a1-4088-af13-22cd9ee81241	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	79cfa963-4462-5659-8107-4d2fc73e960a	THR10_0240_S01	RNA-Seq	Primary tumour - other	3.2.1-1	21013705-1931-5cac-94b5-946638ba4738	2017-03-12T16:25:26.018+0000	2017-03-12T08:56:19.351+0000
ConsonanceTask_true_36_false_2be35233e8	SUCCESS	DTB-141-Baseline	78e34235-6040-58cb-8d96-edc3d4c2837a	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-141	98dd90e7-daa7-46e8-bc48-b9017a064d0a	not provided	WCDT	rna_seq_quantification	SU2C	87907d34-ae93-5e48-99e3-f9f1f6e1e85b	DTB-141-Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	321dec63-a11e-5fdd-be6e-ec0980e58ef5	2017-03-17T23:42:42.040+0000	2017-03-16T04:02:53.587+0000
ConsonanceTask_true_36_false_797e022b7f	FAILED	THR11_0253_S01	907fe907-0e04-5297-8c0e-9236cbf1f6a4	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR11	THR11_0253	b537eae4-7f44-4855-8cd9-6cdf65fc1a3f	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	fbec7fba-a441-56b3-a24b-89e0b40530cc	THR11_0253_S01	RNA-Seq	Primary tumour - other	3.2.1-1	266fc785-f685-5411-84e5-cbe94cd5eaf9	2017-03-14T04:52:29.608+0000	2017-03-13T22:47:51.445+0000
ConsonanceTask_true_36_false_3f59d566ef	SUCCESS	DTB-112_Baseline	c4c79b03-0a07-54f1-b26c-25e9a0f9d743	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-112	4c7ac7a2-61af-4a7f-ad48-14d611276b19	not provided	WCDT	rna_seq_quantification	SU2C	093062a5-a20a-5729-a5b0-48b3b9993867	DTB-112_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	9cce7f59-766d-5f87-981e-f2a536d6437f	2017-03-17T10:59:25.176+0000	2017-03-16T04:02:35.199+0000
ConsonanceTask_true_36_false_2625cbd25a	SUCCESS	DTB-034_Baseline	30208e5f-df7d-53a9-aed9-88c6bd7ff8cb	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-034	9da7fbea-1ce1-42c9-b911-ec2bea6de199	not provided	WCDT	rna_seq_quantification	SU2C	7d674736-0717-53bd-8651-ed026ee5b862	DTB-034_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	1ba31a26-d422-5a7d-b5bd-8c627b8d38b9	2017-03-18T21:55:12.051+0000	2017-03-16T04:02:36.251+0000
ConsonanceTask_true_36_false_7e62a2712d	SUCCESS	DTB-118_Baseline	7562adb7-c63a-5cb2-81c2-6e414204e05f	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-118	a6c64070-c932-4df6-aba7-7888791d546c	not provided	WCDT	rna_seq_quantification	SU2C	8ba77452-4838-58e2-916e-50b37823ba84	DTB-118_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	046b9284-2d8b-50a0-9aa6-fcb5155ece49	2017-03-17T08:47:28.117+0000	2017-03-16T04:02:30.993+0000
ConsonanceTask_true_36_false_4a19ab5828	FAILED	THR10_0237_S01	6c903a98-daff-5427-99b1-cbb3b406d83b	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0237	9e1626cb-94b9-4938-95da-d1e9dc9cc42c	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	caa7feaa-e029-58ef-b33f-eee6f79a3b13	THR10_0237_S01	RNA-Seq	Primary tumour - other	3.2.1-1	799e52b1-6bcd-5ba8-b385-54f7f25c68f9	2017-03-11T06:41:56.760+0000	2017-03-10T18:37:08.924+0000
ConsonanceTask_true_36_false_a2461f458f	SUCCESS	DTB-104_Baseline	01c994a1-bf52-5d7b-ade3-dc2d5a90be7b	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-104	d605749c-0d69-4af3-878e-ff0c8785f6d5	not provided	WCDT	rna_seq_quantification	SU2C	004aac48-e75e-538e-aeef-ba69918d88d9	DTB-104_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	15ae15e2-9fc5-5895-b7ef-bdc364c8636f	2017-03-17T13:40:32.155+0000	2017-03-16T04:02:41.316+0000
ConsonanceTask_true_36_false_3aaddd1b0e	SUCCESS	DTB-035_Baseline	7f43c30c-025c-514c-bffa-6cb29f6e1019	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-035	7563bfc8-f174-4606-b9c6-a471a175d144	not provided	WCDT	rna_seq_quantification	SU2C	a3d328a8-7345-534a-b4e8-534756628930	DTB-035_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	8a884ad1-6801-507d-8582-d4dabde04674	2017-03-17T06:14:11.079+0000	2017-03-16T04:02:28.873+0000
ConsonanceTask_true_36_false_027fd37390	SUCCESS	DTB-131_Baseline	458cabb9-1105-5a3b-8c92-ab8068573da0	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-131	2d4643e4-e063-495a-a1a0-2227b49811d4	not provided	WCDT	rna_seq_quantification	SU2C	aa4d7504-d17a-580d-a45e-6a65b446b4ad	DTB-131_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	df3bf0d4-c7d8-5576-935a-a9f54819f5e5	2017-03-17T08:32:32.161+0000	2017-03-16T04:02:24.537+0000
ConsonanceTask_true_36_false_8519d54931	SUCCESS	DTB-176_Baseline	21ef86d3-cd9f-59a3-936e-8e031b4c5f83	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-176	2356d8e4-0c4f-4bb0-bc91-40aa1037bd64	not provided	WCDT	rna_seq_quantification	SU2C	1c724c2a-4a4a-5df6-abca-5cdfa4d690a0	DTB-176_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	f0ddb190-e41d-5225-bcd2-51844d737777	2017-03-17T09:20:41.151+0000	2017-03-16T04:02:26.726+0000
ConsonanceTask_true_36_false_bae76efcae	SUCCESS	DTB-098_Baseline	1877dd5b-bdfa-5397-8e5a-2784fe1afe16	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-098	11f156cc-978c-4c28-a407-91eae298ae7e	not provided	WCDT	rna_seq_quantification	SU2C	2587df0d-9825-521e-8a95-3fd2a9a3200c	DTB-098_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	587757d7-3a7e-5ded-81bf-28bb3bb59d96	2017-03-17T11:36:37.352+0000	2017-03-16T04:02:38.549+0000
ConsonanceTask_true_36_false_9edaf41eed	FAILED	THR10_0225_S01	b6fdb669-977b-5bf0-9d4b-bf6fe3009581	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0225	b6e6d33d-42fd-41ad-afce-f8a5b34854aa	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	ec7e0639-dde2-5904-b62f-e865456ad2be	THR10_0225_S01	RNA-Seq	Primary tumour - other	3.2.1-1	c6aaa7e6-5e9e-5204-8856-846d30225a08	2017-03-11T13:37:58.446+0000	2017-03-10T19:04:27.806+0000
ConsonanceTask_true_36_false_d0736cc5a1	SUCCESS	DTB-023_Baseline	87bd5262-92b9-5312-921d-1d31d90326d1	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-023	a52a903f-2d61-4ac8-838c-f8d633335b9e	not provided	WCDT	rna_seq_quantification	SU2C	4fb7563c-c404-57b4-87cf-241cc8e5446c	DTB-023_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	35932dc0-67c0-5c06-b41e-dce58faffccf	2017-03-17T06:53:19.748+0000	2017-03-16T04:02:27.829+0000
ConsonanceTask_true_36_false_96681d1da9	SUCCESS	DTB-038_Baseline	a710387a-beca-575d-91ca-66d36b47b31f	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-038	9ce64eb7-5a5a-4777-a498-9ad291418181	not provided	WCDT	rna_seq_quantification	SU2C	266a780e-4c38-570e-a22d-f6b804a15a9f	DTB-038_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	e80d3bfc-6834-5aca-ad35-e7de3f9cc55a	2017-03-17T11:53:52.373+0000	2017-03-16T04:02:42.536+0000
ConsonanceTask_true_36_false_adacb5211f	FAILED	THR12_0279_S01	63df6919-f00a-54dd-8625-33b0a9a12b9d	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR12	THR12_0279	d479ebd8-2886-4aac-b276-befaf42ec31f	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	1ea8a870-d6b5-52f1-b8a9-205343455828	THR12_0279_S01	RNA-Seq	Recurrent tumour - solid tissue	3.2.1-1	167f2b4a-7189-51dd-8323-40b37ced33f2	2017-03-16T06:19:56.686+0000	2017-03-16T00:53:06.887+0000
ConsonanceTask_true_36_false_92137e6c94	FAILED	DTB-151_Baseline	48301f09-3c39-570c-9ee6-c243f8d14583	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-151	3bc2182e-b25e-45a6-a84d-6ed6324eb9b6	not provided	WCDT	rna_seq_quantification	SU2C	193f1bb1-e30c-5e42-b3e1-89565c80392e	DTB-151_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	48d7e438-617d-584e-afe2-958419c2a7b2	2017-03-17T19:17:59.791+0000	2017-03-16T04:02:56.749+0000
ConsonanceTask_true_36_false_1b1ee1dc2d	SUCCESS	DTB-003_Baseline	13031659-4f5f-5081-ac30-7e348267522f	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-003	7e5b5a75-6042-41de-bb20-69f7a443b311	not provided	WCDT	rna_seq_quantification	SU2C	34a66b99-fa80-52e4-952d-ec6b19958b40	DTB-003_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	33459fa8-d02f-5bef-9857-d4e12badcb0d	2017-03-17T14:10:14.336+0000	2017-03-16T04:02:44.894+0000
ConsonanceTask_true_36_false_ea0b11ee91	SUCCESS	DTB-137_Baseline	f71f4c08-e9a1-54e2-945c-150ecd4c1104	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-137	ae1908f4-82b4-42c5-9bbc-585a8ad2b3d8	not provided	WCDT	rna_seq_quantification	SU2C	060407f6-0fe8-5303-8ed4-60773ff8752a	DTB-137_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	4f9d6e19-3b76-509b-b673-03da7d8a2493	2017-03-18T01:07:02.083+0000	2017-03-16T04:03:05.516+0000
ConsonanceTask_true_36_false_c029401c49	SUCCESS	DTB-063_Baseline	41488b25-1989-5be8-afef-48325e950980	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-063	4e3b39a3-3144-4540-9753-8c373cf5b847	not provided	WCDT	rna_seq_quantification	SU2C	512bd898-78b1-5f7f-a07c-0125708c3ff0	DTB-063_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	6bb6221e-a9b6-5749-880e-026218a830ab	2017-03-18T19:46:43.640+0000	2017-03-16T04:01:21.393+0000
ConsonanceTask_true_36_false_e1f1834f42	SUCCESS	DTB-032_Baseline	dbe66b32-e562-5d34-bda3-5c2cd10ecb1a	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-032	119c74b1-15ae-4e9f-89e0-367d221a8d90	not provided	WCDT	rna_seq_quantification	SU2C	798318b7-e97e-5fa6-aba7-9d96e0c5dd7c	DTB-032_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	eb8a3d2c-8cbc-52bc-a4c4-7a836fdde6f4	2017-03-17T20:25:12.792+0000	2017-03-16T04:02:57.910+0000
ConsonanceTask_true_36_false_1a6cc48b12	FAILED	DTB-089_Progression	d7cf49b3-9731-5c86-b1eb-b2aa52db1c1b	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-089	f40cb6bf-f2b3-4c04-a3cb-a207d690d62c	not provided	WCDT	rna_seq_quantification	SU2C	61a15a69-5e89-52a2-a4c2-0b6405968b2f	DTB-089_Progression_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	b1c3f496-c41e-50a4-95aa-34c6a0bd3043	2017-03-17T18:44:46.824+0000	2017-03-16T04:02:50.596+0000
ConsonanceTask_true_36_false_7d16334d31	FAILED	THR12_0277_S01	cbdcbf7f-cfe7-5d9d-a717-b47d1012f42f	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR12	THR12_0277	ad7ff6fa-816c-44e6-a3dc-b190e9e19d72	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	7c0117b7-ac8b-5fb9-b997-b16e430560cc	THR12_0277_S01	RNA-Seq	Metastatic tumour - other	3.2.1-1	559d4320-f33e-5d5f-807e-8e9f3ecd3fa6	2017-03-16T00:14:49.032+0000	2017-03-16T00:14:49.032+0000
ConsonanceTask_true_36_false_d18eab4794	SUCCESS	DTB-124_Baseline	08a8ae17-8f0b-53ce-85ff-2b57f3a8e47b	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-124	ec2b05f8-4428-4461-a515-1a9eca479983	not provided	WCDT	rna_seq_quantification	SU2C	0fd77f4e-5985-5dcf-95b2-b2aa89276b48	DTB-124_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	e4dfee6c-97a0-5ac6-8e61-daf7ad54310c	2017-03-17T21:48:43.967+0000	2017-03-16T04:02:54.635+0000
ConsonanceTask_true_36_false_8ac806c91d	FAILED	DTB-108_Baseline	8bba9867-1eba-5a51-a642-ae375db0ef09	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-108	0c3e081f-2e4e-467b-be14-f0a9ca1ec278	not provided	WCDT	rna_seq_quantification	SU2C	9cd44f97-fc73-58e1-ae6d-6a16c592b204	DTB-108_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	1e9c4a61-3f4e-5d48-8255-e490765a1cfc	2017-03-17T23:14:55.429+0000	2017-03-16T04:03:04.382+0000
ConsonanceTask_true_36_false_ed77d2602a	SUCCESS	DTB-187_Baseline	85aef307-a33b-5a42-90a3-3997be924524	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-187	33c50c95-6bc3-4819-993b-4189dd372d99	not provided	WCDT	rna_seq_quantification	SU2C	8ddd811a-c5d8-5a1c-b595-c2375231fae0	DTB-187_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	2429a0f4-2c8f-5379-a7d1-fcf9c8d48110	2017-03-17T20:45:45.957+0000	2017-03-16T04:02:48.536+0000
ConsonanceTask_true_36_false_b7739c2cc4	SUCCESS	DTB-073_Baseline	a5823a30-6566-5d2c-8d34-c54eaa4c33f5	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-073	f88101a4-87a0-4482-acc6-2487bc30a830	not provided	WCDT	rna_seq_quantification	SU2C	45ce02ba-9452-5c99-af85-5f26fde83505	DTB-073_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	727cb696-394b-5b7b-81d9-9db9270373cc	2017-03-17T22:44:57.651+0000	2017-03-16T04:02:58.964+0000
ConsonanceTask_true_36_false_f6f50d0840	FAILED	THR12_0278_S02	e11b633c-d4e7-53f3-8c35-82e47083a302	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR12	THR12_0278	85c7ce2c-4c1f-4222-80e2-332a91d52130	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	9b54e1c1-6a5b-57ee-93b2-923848244151	THR12_0278_S02	RNA-Seq	Normal - solid tissue	3.2.1-1	5d523fe6-a658-59ab-a740-6416e0973030	2017-03-16T00:14:51.078+0000	2017-03-16T00:14:51.078+0000
ConsonanceTask_true_36_false_cf34be2b70	SUCCESS	DTB-077_Progression	23725d91-bb6b-5f73-993f-5d0b2f0a14d7	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-077	13287cb2-cb75-4bf8-ba69-15793fa07ac2	not provided	WCDT	rna_seq_quantification	SU2C	14e653eb-94cb-5248-aa02-bdf1aca8523c	DTB-077_Progression_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	8c233c90-7492-5ef9-b040-b8e70f8c9e4a	2017-03-18T01:58:58.191+0000	2017-03-16T04:03:01.108+0000
ConsonanceTask_true_36_false_d403df1b2c	SUCCESS	DTB-036_Baseline	5c4d697f-8e18-5e3c-b914-e1a228754687	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-036	90abd072-64da-4cdd-bddc-089797a2cced	not provided	WCDT	rna_seq_quantification	SU2C	bbe88bcd-f815-55ff-a367-028906c42c62	DTB-036_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	55a26d40-5bc7-5230-9855-290d23a04076	2017-03-17T19:42:54.341+0000	2017-03-16T04:03:00.032+0000
ConsonanceTask_true_36_false_64fb12459e	SUCCESS	DTB-063_Progression	35e1bf36-b554-5407-a063-37ee07bc9625	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-063	eb544475-5f4c-4867-8ffc-9eab91351e20	not provided	WCDT	rna_seq_quantification	SU2C	512bd898-78b1-5f7f-a07c-0125708c3ff0	DTB-063_Progression_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	9ae477c9-c48a-5858-b59f-5cef362940e4	2017-03-17T23:43:51.948+0000	2017-03-16T04:03:03.297+0000
ConsonanceTask_true_36_false_548b9fe8a5	FAILED	DTB-146_Baseline	104ff116-dd3d-5f67-9f4c-b6f87f6e5d9a	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-146	550db4d9-06b5-490e-ac73-b21126c6acea	not provided	WCDT	rna_seq_quantification	SU2C	ead76160-042f-5c19-9cf2-b3a634a5c12c	DTB-146_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	2a2bfd49-1245-5971-af7a-0f59391907e3	2017-03-17T20:55:53.935+0000	2017-03-16T04:03:02.236+0000
ConsonanceTask_true_36_false_022d249f96	SUCCESS	DTB-022_Baseline	676d7d4f-f58f-5000-ad0b-e209051a9c75	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-022	c7433ccd-702d-4fae-a888-1658e90e437e	not provided	WCDT	rna_seq_quantification	SU2C	ae5bee60-10f5-5576-89d0-6128c28ae855	DTB-022_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	dce71516-81db-55d5-a098-0d77bd6b6e7d	2017-03-18T01:12:50.723+0000	2017-03-16T04:03:16.417+0000
ConsonanceTask_true_36_false_8a6c0bad39	FAILED	DTB-001_Baseline	8cb6536c-1f3e-594f-bec2-2ebf41b7295d	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-001	e539dd56-fc88-4e1f-b218-391761255644	not provided	WCDT	rna_seq_quantification	SU2C	0ed22656-2726-552c-b4d9-b0648de1d374	DTB-001_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	39949fa0-8359-5f53-b9ba-2fc5b57790c2	2017-03-18T00:38:12.408+0000	2017-03-16T04:03:19.738+0000
ConsonanceTask_true_36_false_6edf6634ce	SUCCESS	DTB-008_Baseline	4762b9bb-9459-5cdf-af6e-40859c9a63c0	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-008	55b9be78-ffd4-4ac0-bb3c-debef0516c00	not provided	WCDT	rna_seq_quantification	SU2C	9efc2151-d4f0-5ac1-aeb1-3240628a0db4	DTB-008_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	a79f05a6-f66a-5a25-9b81-8d39124ee56c	2017-03-18T03:12:19.945+0000	2017-03-16T04:03:17.519+0000
ConsonanceTask_true_36_false_0554dc0d4b	FAILED	THR10_0226_S01	1b9eb085-0ce6-529a-9ada-502c051264d5	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0226	3acf3ca8-6a75-4467-981e-9b319fa96621	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	97f76fec-e3c3-504c-8511-f724512d1ddb	THR10_0226_S01	RNA-Seq	Primary tumour - other	3.2.1-1	854df652-ca3e-552b-9d4a-e03ea6dc85f8	2017-03-12T02:58:26.177+0000	2017-03-10T19:11:06.244+0000
ConsonanceTask_true_36_false_2d01af4628	SUCCESS	DTB-126_Baseline	50d15a6d-56d7-5702-bf9e-860aed5a67bf	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-126	ace36aea-3db8-4769-a7fc-4f9396a580ba	not provided	WCDT	rna_seq_quantification	SU2C	b094c658-9fff-5439-8a16-78169a3918d4	DTB-126_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	2c6a91d8-6733-58c0-95be-98c284963b4a	2017-03-18T04:53:02.283+0000	2017-03-16T04:03:07.715+0000
ConsonanceTask_true_36_false_aa095758d5	FAILED	THR10_0232_S01	c898b259-b8a6-5c04-a343-a4368ada6a51	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0232	2fd2cb9d-e434-47df-89e9-2f0bd2138ca4	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	436b2bc5-172f-5464-8688-c33825451cf4	THR10_0232_S01	RNA-Seq	Primary tumour - other	3.2.1-1	3c3b108c-f1d4-51aa-9b85-53dd009f90c5	2017-03-11T04:21:14.533+0000	2017-03-10T18:36:57.741+0000
ConsonanceTask_true_36_false_c7b9931826	FAILED	DTB-141_Progression	8dc19793-4cd3-5d39-9a08-2b283ee2968b	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-141	a59bd85a-0724-484a-af14-c75eb3077c38	not provided	WCDT	rna_seq_quantification	SU2C	87907d34-ae93-5e48-99e3-f9f1f6e1e85b	DTB-141_Progression_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	5ddcf040-e0e4-5995-8ed7-c52955e66102	2017-03-18T21:31:05.345+0000	2017-03-16T04:03:32.539+0000
ConsonanceTask_true_36_false_1b98ffd6f1	SUCCESS	DTB-018_Progression	e95e0fd2-be41-5c03-8cda-c13f2b998d27	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-018	cb4fb962-9c12-4f6f-a09a-28b80c554ebf	not provided	WCDT	rna_seq_quantification	SU2C	d5e27201-9aab-57af-adf1-38ceaf841e42	DTB-018_Progression_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	e4e6489a-1a3a-5700-b1ba-f106400f33a9	2017-03-18T01:35:39.485+0000	2017-03-16T04:03:11.946+0000
ConsonanceTask_true_36_false_9c300462c7	FAILED	THR11_0254_S01	f1faae34-db2f-5073-b9c5-f2ce69363d04	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR11	THR11_0254	a61de3b0-9e6f-4d51-8588-bcf9bedb2ef0	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	66df9362-3738-5f2d-823b-e31348474958	THR11_0254_S01	RNA-Seq	Primary tumour - other	3.2.1-1	5f471661-cf07-5fe1-8fc4-47fd04972e82	2017-03-14T05:16:20.785+0000	2017-03-13T22:47:57.549+0000
ConsonanceTask_true_36_false_c41e87836f	SUCCESS	DTB-018_Baseline	40a03544-ae28-56ed-b172-94589d314117	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-018	984c8061-c13f-41ef-8b25-3b057ffc48a0	not provided	WCDT	rna_seq_quantification	SU2C	d5e27201-9aab-57af-adf1-38ceaf841e42	DTB-018_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	3fcc7b54-0c8f-5981-a3b1-d48f4fa5db09	2017-03-18T00:42:20.770+0000	2017-03-16T04:03:21.802+0000
ConsonanceTask_true_36_false_ac6d1d7373	FAILED	DTB-083_Baseline	45b11be3-cefc-564b-9549-b9a6773dfb37	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-083	c81ce175-7def-47c2-a9a6-16a35b8463ce	not provided	WCDT	rna_seq_quantification	SU2C	54986228-7dd7-5368-a6b5-657095bd267f	DTB-083_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	23b3c96f-e58b-5c1a-a663-51897345e285	2017-03-17T21:25:41.880+0000	2017-03-16T04:03:09.798+0000
ConsonanceTask_true_36_false_e7c8914fd1	FAILED	THR10_0236_S01	128104dc-3f15-51a8-b2b3-9752e9d1b935	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0236	6bc3a627-bda4-4c3a-b228-b8630c9712a2	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	0702cbf3-5f91-5553-8f55-10061fd3ec29	THR10_0236_S01	RNA-Seq	Primary tumour - other	3.2.1-1	8a7f3c95-78ff-5dad-ab7f-2ae57756c2fa	2017-03-11T05:52:58.307+0000	2017-03-10T18:36:59.104+0000
ConsonanceTask_true_36_false_a03639f279	SUCCESS	DTB-132_Baseline	3fe2da01-4e9c-58a2-97f2-0c3cfdf0a591	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-132	00930b3d-0448-4a31-a426-655f49f6b99a	not provided	WCDT	rna_seq_quantification	SU2C	b8ba575e-1dd3-5a8e-bc89-1eb68394df6d	DTB-132_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	17d0df15-f647-5552-a1de-f2dec8f6d3b0	2017-03-18T02:59:28.951+0000	2017-03-16T04:03:20.776+0000
ConsonanceTask_true_36_false_ef7be868bf	FAILED	TH07_0152_S01	4fd77e23-4b7a-53a2-b103-c93349676462	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	TH07	TH07_0152	dfca07e7-ed05-49e2-8661-da9bb6bdee0d	BTO:0000214	Expression Analysis	rna_seq_quantification	Treehouse	7628fe98-047c-5ad1-bf33-139035146db3	TH07_0152_S01	RNA-Seq	Cell line - derived from tumour	3.2.1-1	4c035f39-897a-5695-b241-48b809b9889e	2017-03-16T03:14:34.020+0000	2017-03-16T00:53:00.385+0000
ConsonanceTask_true_36_false_3faeb74da2	FAILED	DTB-005_Baseline	8b9cc591-6e92-5a29-bcb4-999650bc64be	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-005	98055895-4cee-4125-a98f-ea7cde9f266b	not provided	WCDT	rna_seq_quantification	SU2C	6ccaad72-1608-5a1b-b5a1-f5f53c19198a	DTB-005_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	d3ef6c71-ccbc-5b5f-a499-bc1a17e047d5	2017-03-18T18:35:42.620+0000	2017-03-16T04:03:33.581+0000
ConsonanceTask_true_36_false_c863766548	FAILED	THR10_0239_S01	241fb6a1-e3cd-5ad7-9625-2ff0ff1084c0	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0239	0438c1f0-d91c-48cc-a446-e1bb5d0f6fe2	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	bf3b65aa-580a-5306-b109-7adef98e8d80	THR10_0239_S01	RNA-Seq	Primary tumour - other	3.2.1-1	e2103ded-a5f6-52db-9367-3ec63e98823b	2017-03-12T00:02:50.762+0000	2017-03-10T19:11:07.554+0000
ConsonanceTask_true_36_false_1be8fdbf6c	FAILED	THR10_0242_S01	3c48be7e-84e6-5a3c-91d5-55b81d1bb0ed	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0242	92d230f0-3cfc-4398-b2dc-40c437eecde6	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	cffe6db3-ec6d-5049-ab65-0f8091eafe6f	THR10_0242_S01	RNA-Seq	Primary tumour - other	3.2.1-1	07a4a630-a706-5732-a2b5-b1a9db19e2da	2017-03-11T05:01:28.682+0000	2017-03-10T18:37:06.230+0000
ConsonanceTask_true_36_false_e5fe9caf37	FAILED	TH07_0154_S01	cab7de46-b0df-55a3-92c0-09ba8d95ec5b	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	TH07	TH07_0154	c5fb210b-39eb-4f20-972a-d91ea33e8e94	BTO:0000214	Expression Analysis	rna_seq_quantification	Treehouse	f184d6fa-2d8f-5366-a13e-809c8cb6b177	TH07_0154_S01	RNA-Seq	Cell line - derived from tumour	3.2.1-1	4386359f-bedd-5426-acb4-fb52f7c41399	2017-03-16T03:27:57.118+0000	2017-03-16T00:53:03.662+0000
ConsonanceTask_true_36_false_f0d2b90d10	FAILED	TH07_0155_S01	f39141ab-9779-53e4-8dce-3d8e4b9138c7	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	TH07	TH07_0155	5ca1c2ce-15f3-4ea8-847e-29ea863057bd	BTO:0000214	Expression Analysis	rna_seq_quantification	Treehouse	b2cdd713-d800-5cd9-9f38-0903bcae6cbf	TH07_0155_S01	RNA-Seq	Cell line - derived from tumour	3.2.1-1	188f14f2-7b31-5893-a9fc-dc0d4b652912	2017-03-16T03:06:19.317+0000	2017-03-16T00:52:59.317+0000
ConsonanceTask_true_36_false_665a3306c1	FAILED	DTB-049_Baseline	e189076f-2c73-5823-a940-9fea858577ad	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-049	a42be1ed-d5c5-437c-98c2-5c9f4c2e5fcd	not provided	WCDT	rna_seq_quantification	SU2C	e8484186-f810-5bf0-ac70-026bb1189867	DTB-049_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	66247b4a-3758-5114-afc0-b80e442fa8b3	2017-03-16T04:03:28.329+0000	2017-03-16T04:03:28.279+0000
ConsonanceTask_true_36_false_158b3d9961	SUCCESS	DTB-022Pro	c223a9f0-8c14-50be-ba69-c6a10914c49c	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-022	75606474-8c04-4ab7-b3c6-aa7cd867ca6b	not provided	WCDT	rna_seq_quantification	SU2C	ae5bee60-10f5-5576-89d0-6128c28ae855	DTB-022Pro_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	465f23d9-7f33-5c01-84f2-28cb5bb7d96e	2017-03-18T06:41:30.591+0000	2017-03-16T04:03:30.367+0000
ConsonanceTask_true_36_false_15e321b01c	FAILED	THR10_0241_S01	d9bc7257-090b-5824-8c1f-b4c9f8a3adfc	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0241	3274a76d-2f76-429e-b734-d461162c725a	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	cf6d07e9-08d6-5c87-bcde-34f6d585722c	THR10_0241_S01	RNA-Seq	Primary tumour - other	3.2.1-1	73a5911f-fbc3-5dd5-9c22-0d8768da4469	2017-03-12T00:19:55.051+0000	2017-03-10T19:11:10.847+0000
ConsonanceTask_true_36_false_0a8a1dadb6	FAILED	THR12_0280_S03	03ae0f27-c088-51bb-a637-63aefdfef3e8	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR12	THR12_0280	4029693a-0238-490a-8ed0-00760d34d407	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	6005cb08-cd12-5dc5-b05c-2eb79c37ae1d	THR12_0280_S03	RNA-Seq	Metastatic tumour - other	3.2.1-1	4c6b4d8b-6a6d-53e0-bdfc-1160c7d00697	2017-03-16T06:16:17.991+0000	2017-03-16T00:53:08.083+0000
ConsonanceTask_true_36_false_b9127eaa19	SUCCESS	DTB-067_Progression	a9022ea6-39da-580c-a779-29a9740fcc08	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-067	6b199086-fc0b-42ab-8ca5-82ffbe6c1f4e	not provided	WCDT	rna_seq_quantification	SU2C	84521a1d-d084-50d6-b544-45b6e486d677	DTB-067_Progression_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	ef4cc985-0b49-568d-b70b-1ba19b912e67	2017-03-18T21:12:47.023+0000	2017-03-16T04:03:37.870+0000
ConsonanceTask_true_36_false_b66c015c7e	SUCCESS	DTB-181-Baseline	820c557d-09b1-5ed9-87cf-0435140f5e7a	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-181	0a2082bf-9aec-4a8a-8efc-79bc085d842f	not provided	WCDT	rna_seq_quantification	SU2C	02f6fc3b-8e64-53ae-9cf7-7d366b07491b	DTB-181-Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	1841e2cc-be99-5fae-be8e-7623a890c3eb	2017-03-18T05:11:01.092+0000	2017-03-16T04:03:25.137+0000
ConsonanceTask_true_36_false_c9ac250aa5	SUCCESS	DTB-194-Baseline	1457fe70-8b0f-50b6-ae1e-d888462dba31	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-194	013a3284-e0be-4d6a-ace7-0742d2234306	not provided	WCDT	rna_seq_quantification	SU2C	621a6f29-78e0-54b2-8b44-3b59bc9bce20	DTB-194-Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	31453ca1-5620-53d4-a46a-75c6e6ba6c56	2017-03-18T07:48:36.684+0000	2017-03-16T04:03:26.177+0000
ConsonanceTask_true_36_false_42e3f6615b	FAILED	DTB-170_Baseline	75d3fbcb-8cc4-54fe-861b-be20f896ce78	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-170	cc2c4e96-a72a-4336-a05d-00b90155e6c5	not provided	WCDT	rna_seq_quantification	SU2C	1ccc76ea-55ec-5139-a4ef-9b9a1e8cead4	DTB-170_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	4190d297-f54a-5db0-aa30-9ae17f78529d	2017-03-16T04:03:31.511+0000	2017-03-16T04:03:31.465+0000
ConsonanceTask_true_36_false_7c6ffccc19	SUCCESS	DTB-085_Baseline	075fbef5-a57e-5ed0-9de4-76c52d583ede	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-085	8096bbaf-b147-49a1-84c4-5db2e7d7f8ce	not provided	WCDT	rna_seq_quantification	SU2C	47c9f268-f28b-50fe-a61d-5cec2ef62c9b	DTB-085_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	cb39d636-fc9e-5204-8d8b-959c732be13f	2017-03-18T16:08:38.059+0000	2017-03-16T04:03:35.681+0000
ConsonanceTask_true_36_false_e687671052	SUCCESS	DTB-127_Baseline	febe0f4a-80ff-592c-8ef4-088130994dce	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-127	ba4b3a15-65a7-4808-b9ac-6382b120a5f5	not provided	WCDT	rna_seq_quantification	SU2C	20bf2d3a-5596-5513-83a3-d324af1785ae	DTB-127_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	a4ebb45e-251a-53b7-8ba7-66db20ae8c80	2017-03-18T02:21:13.880+0000	2017-03-16T04:03:18.672+0000
ConsonanceTask_true_36_false_38d345e91f	FAILED	THR12_0278_S03	abfcd1e4-7926-501b-a676-0f205eac3ab2	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR12	THR12_0278	1b5bba26-de2d-4c98-ad4d-2e40b57c2915	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	9b54e1c1-6a5b-57ee-93b2-923848244151	THR12_0278_S03	RNA-Seq	Recurrent tumour - solid tissue	3.2.1-1	476ac472-d41e-5d97-bbbc-654bee072e9b	2017-03-16T07:08:40.245+0000	2017-03-16T00:53:12.235+0000
ConsonanceTask_true_36_false_9ea04fccb0	SUCCESS	DTB-140_Baseline	58b681c4-4a96-5e3d-a77c-b9e0dcb8d4d9	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-140	bb2b3cdf-2a7f-412e-9d7e-38d02a67e71c	not provided	WCDT	rna_seq_quantification	SU2C	7a8f1906-c72e-52f2-844d-cf996bf480fe	DTB-140_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	884cbb34-f0ed-5597-94d7-74d3ca9cd8a0	2017-03-18T07:01:22.906+0000	2017-03-16T04:03:27.219+0000
ConsonanceTask_true_36_false_71baca30b4	FAILED	THR12_0278_S01	8760892c-35cc-5ba3-9630-02465e3c55c0	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR12	THR12_0278	207faba0-9a97-4e3c-b3c2-8e73be72dd39	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	9b54e1c1-6a5b-57ee-93b2-923848244151	THR12_0278_S01	RNA-Seq	Primary tumour - solid tissue	3.2.1-1	a9a09c02-9892-5f97-b625-a15723527bc8	2017-03-16T00:14:50.056+0000	2017-03-16T00:14:50.056+0000
ConsonanceTask_true_36_false_3656417119	SUCCESS	LNCAP_batch9_Baseline	f456e193-1dd0-5f3a-813d-0064243ee963	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	LNCAP_batch9	4e6784e7-653f-45b3-852c-940266c01798	not provided	WCDT	rna_seq_quantification	SU2C	6a41b954-3721-5c1b-9889-a802f24d212e	LNCAP_batch9_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	4a6b7a88-da40-50d0-b99a-a8c60117cc81	2017-03-17T12:35:17.958+0000	2017-03-16T04:02:34.197+0000
ConsonanceTask_true_36_false_8d2e9d01a3	SUCCESS	DTB-116_Baseline	9bcf42e4-d3f7-5009-be60-770ccdaf83df	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-116	24e96a7f-4c47-4303-b3ac-05e66b0689a9	not provided	WCDT	rna_seq_quantification	SU2C	32a63fe9-7f8d-5738-9df7-45e97c17ea5a	DTB-116_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	31eb8451-d228-5eb7-be1a-c76b434b7052	2017-03-18T01:00:01.316+0000	2017-03-16T04:03:14.305+0000
ConsonanceTask_true_36_false_bbd480cd70	SUCCESS	DTB-042_Baseline	da611da8-70bb-572d-8f32-5a862b5b5f6b	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-042	6c6a2fd0-f278-4a02-a5d1-ea57f1569d26	not provided	WCDT	rna_seq_quantification	SU2C	87164058-a3ee-5716-b568-4d9f5d735415	DTB-042_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	32da4ca0-d8dc-5804-8ffd-e055a284b01c	2017-03-17T08:17:34.254+0000	2017-03-16T04:02:16.596+0000
ConsonanceTask_true_36_false_278a8c9e13	SUCCESS	DTB-060Pro	ce6ab3a9-608e-5845-aedc-b79bb6268a49	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-060	b1e97515-d151-45ba-ab4d-644023f0fad6	not provided	WCDT	rna_seq_quantification	SU2C	56d18524-1258-51a0-b4ea-d68f6777085f	DTB-060Pro_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	b364159d-8174-5b3f-ac19-f8a07fdf655a	2017-03-17T07:08:19.257+0000	2017-03-16T04:02:17.649+0000
ConsonanceTask_true_36_false_9a2fd1f76b	FAILED	THR12_0279_S02	939b71b8-3c02-5c1a-93cd-be38e3a25727	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR12	THR12_0279	16fb2ebc-9afd-4b6a-bb84-dd618de027bf	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	1ea8a870-d6b5-52f1-b8a9-205343455828	THR12_0279_S02	RNA-Seq	Normal - other	3.2.1-1	023b657d-1138-5d7d-8e89-d4871c4f9b32	2017-03-16T05:13:59.631+0000	2017-03-16T00:53:01.429+0000
ConsonanceTask_true_36_false_fe05bcf1e6	FAILED	HCC1395.griffith	ed697e01-413a-5fcf-869e-4c82a1d78f21	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	HCB_precision_analysis	HCC1395.griffith	d3452826-0b09-4c67-9e70-1866404d6a93	BTO:0000214	Expression Analysis	rna_seq_quantification	Treehouse	9041afe5-b49a-5a20-a08e-cb792eeec2ee	HCC1395.griffith	RNA-Seq	Cell line - derived from tumour	3.2.1-1	09183880-e89d-5b49-9a3a-2126499c7dce	2017-03-20T15:51:12.528+0000	2017-03-16T20:01:13.480+0000
spawnFlop_SRR1988343_demo__consonance_jobs__0992701f4f	JOB NOT FOUND	DTB-060Pro	ce6ab3a9-608e-5845-aedc-b79bb6268a49	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-060	0c42e39c-7b6c-436f-8ec5-e268d9972fd3	not provided	WCDT	rna_seq_quantification	SU2C	56d18524-1258-51a0-b4ea-d68f6777085f	DTB-060Pro_1	RNA-Seq	Metastatic tumour - other	3.1.3	b364159d-8174-5b3f-ac19-f8a07fdf655a	\N	\N
ConsonanceTask_true_36_false_58d5121f7b	FAILED	THR11_0256_S01	5608a9e1-4e97-5b00-963c-8f9f416ac0a3	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR11	THR11_0256	5a1702f7-7363-408e-b1dd-e2a91ab79e7a	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	21843548-881c-53f6-bbb3-ba905aa39ad7	THR11_0256_S01	RNA-Seq	Primary tumour - other	3.2.1-1	4f788263-d31d-5d35-92bc-de0359acaf61	2017-03-14T05:43:15.392+0000	2017-03-13T22:47:56.184+0000
ConsonanceTask_true_36_false_04403d571a	FAILED	DTB-156_Baseline	a9f1d09a-ddf8-5ba1-9852-b517b165d4ce	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-156	9d3becd0-840d-4718-afaf-09ec6d4728de	not provided	WCDT	rna_seq_quantification	SU2C	13e9e3a0-98a8-53ce-b13d-a2c55c7e45cd	DTB-156_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	81bc59fc-367c-5c62-9031-5a132d5b2d28	2017-03-16T21:16:42.907+0000	2017-03-16T04:01:52.789+0000
ConsonanceTask_true_36_false_7cdaf833c1	FAILED	THR10_0238_S01	2f2e510f-4bd5-5efc-b7ec-2a7f3f1f83a9	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0238	bcd440c6-850e-4942-884b-19af11109920	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	f3702629-6636-51aa-b97b-5d50b862f287	THR10_0238_S01	RNA-Seq	Primary tumour - other	3.2.1-1	611e72c7-1090-58cc-9dda-8ddc9f5d899a	2017-03-12T16:46:48.982+0000	2017-03-12T08:56:14.268+0000
ConsonanceTask_true_36_false_9465493a29	FAILED	DTB-115_Progression	6c15a406-2053-5683-8718-f11155f67f85	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-115	61a01371-35f5-49de-81f1-aa2760724bef	not provided	WCDT	rna_seq_quantification	SU2C	7d0c329f-113d-5dd1-a435-dff265074b4f	DTB-115_Progression_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	d025d204-46e6-5d86-9ac0-9a6f370af9b4	2017-03-17T18:11:47.414+0000	2017-03-16T04:02:45.944+0000
ConsonanceTask_true_36_false_7a6cba7249	FAILED	THR10_0228_S01	711b689f-2244-55fa-a9e6-4fdcb7351e75	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0228	2581cda3-f7b6-49b7-8ca8-30b0c5bd5f22	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	39fe411d-1827-50c1-8e9d-9489513ba87a	THR10_0228_S01	RNA-Seq	Primary tumour - other	3.2.1-1	9dfa3eb6-dc0b-5e80-b446-85be7524722f	2017-03-12T19:13:33.130+0000	2017-03-12T08:56:16.793+0000
ConsonanceTask_true_36_false_c086a31019	FAILED	THR10_0231_S01	be07d607-d8b2-583d-b45a-81c1e235a2d6	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0231	4da5a8c2-c0b5-4dfe-8fb2-a7760e46f81e	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	d90b7008-5866-5693-a09f-4c2b232e2442	THR10_0231_S01	RNA-Seq	Primary tumour - other	3.2.1-1	aaa22461-a6e9-5149-8301-91c331e954f1	2017-03-11T06:59:42.860+0000	2017-03-10T18:37:00.479+0000
ConsonanceTask_true_36_false_5464356429	SUCCESS	DTB-090_Baseline	fef65902-442d-5c32-a489-add8f8e85e6d	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-090	0ed60274-b024-43c6-83ae-7cbc0f4cc5a8	not provided	WCDT	rna_seq_quantification	SU2C	c42aa629-2594-566e-a886-7773fa502e43	DTB-090_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	e6a4484d-a282-5ae3-935e-6c0a47b299af	2017-03-18T00:58:49.195+0000	2017-03-16T04:02:55.704+0000
ConsonanceTask_true_36_false_c8559202f1	FAILED	TH07_0150_S01	314ddbbd-8f88-5bd3-81db-75c6316c0ba8	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	TH07	TH07_0150	9ce749b6-6911-43d0-b996-f7676a6d286e	BTO:0000214	Expression Analysis	rna_seq_quantification	Treehouse	358b1232-5681-5bae-b904-a8765b17cb2a	TH07_0150_S01	RNA-Seq	Cell line - derived from tumour	3.2.1-1	81090b39-ae12-5823-884b-dd5d6acc4bd7	2017-03-16T05:37:18.682+0000	2017-03-16T00:53:13.395+0000
ConsonanceTask_true_36_false_d419de9143	SUCCESS	DTB-011_Baseline	2fc21c1d-4d68-5baf-b656-d685fae9bc8d	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-011	c1cda1a7-4916-4d0a-bba2-afb1de5396c9	not provided	WCDT	rna_seq_quantification	SU2C	24f5a29b-9be3-5732-9a3a-841c3ee236b4	DTB-011_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	dc910dba-0934-5c02-b8cb-26d071d29694	2017-03-17T13:41:32.827+0000	2017-03-16T04:02:37.514+0000
ConsonanceTask_true_36_false_1a4f1f71e8	FAILED	TH07_0153_S01	60883a8d-edd7-5a18-8854-3fab59574112	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	TH07	TH07_0153	4b6be479-77ac-4641-a110-c6fa0a342894	BTO:0000214	Expression Analysis	rna_seq_quantification	Treehouse	a05ec5b5-10df-5224-9158-6bf92bf909a3	TH07_0153_S01	RNA-Seq	Cell line - derived from tumour	3.2.1-1	280a0bb3-42a9-5507-9490-c8f65786f65c	2017-04-11T18:36:07.394+0000	2017-04-10T23:52:57.565+0000
ConsonanceTask_true_36_false_e3f998edf1	SUCCESS	DTB-137-Progression	792d98dd-595a-5b55-aec2-09b6ac84426a	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-137	037d74fc-9377-4e0f-974b-f272afab9acb	not provided	WCDT	rna_seq_quantification	SU2C	060407f6-0fe8-5303-8ed4-60773ff8752a	DTB-137-Progression_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	4b814aef-a142-54d6-a77f-3d2c1b7330a5	2017-03-18T01:52:30.652+0000	2017-03-16T04:03:15.321+0000
ConsonanceTask_true_36_false_43ba9e5683	FAILED	THR20_0519_S01	0d69dfa6-940d-5c39-a89f-958ad29fd0de	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR20	THR20_0519	178e8dcf-0fae-48bd-be6f-b43ceae53213	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	2a3a2026-cc71-50d7-b1ec-8a8f3f8a14fa	THR20_0519_S01	RNA-Seq	Primary tumour - other	3.2.1-1	ed9597e8-12ce-540d-b1ce-a6af33c6e056	2017-04-14T20:57:46.193+0000	2017-04-14T00:02:00.149+0000
ConsonanceTask_true_36_false_86d7f18ded	FAILED	THR20_0502_S01	82c0769d-1851-572c-9e07-28cf39ac0e7a	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR20	THR20_0502	5967c6f3-c7fd-4c68-8dcf-ebf12791584f	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	817bee35-9c68-5e88-bd60-15b6f8b7d3d5	THR20_0502_S01	RNA-Seq	Primary tumour - other	3.2.1-1	9c5c8bf2-fb53-53e2-9af7-217326c7801f	2017-04-15T18:04:22.223+0000	2017-04-14T00:02:03.538+0000
ConsonanceTask_true_36_false_6cdda1795c	SUCCESS	DTB-040_Baseline	67dc3514-36ed-5dbf-8bbb-a45f52f7a66e	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-040	c9ca097b-138f-4154-aa5a-8d11c38a04ad	not provided	WCDT	rna_seq_quantification	SU2C	be6fe15e-d45a-5bdf-b6be-b0308280f0ca	DTB-040_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	00bc96ee-1af6-5362-ac40-94d67e1459cc	2017-03-17T19:07:56.463+0000	2017-03-16T04:02:49.602+0000
ConsonanceTask_true_36_false_5e5f9addcd	FAILED	THR12_0280_S02	00abcf93-5fb5-5a86-adeb-736fec351188	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR12	THR12_0280	033f1921-a896-4652-8b58-674e4e6c5d14	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	6005cb08-cd12-5dc5-b05c-2eb79c37ae1d	THR12_0280_S02	RNA-Seq	Normal - other	3.2.1-1	137c02a4-0797-5e20-8433-b4a85681db55	2017-03-16T05:26:16.252+0000	2017-03-16T00:52:57.182+0000
spawnFlop_SRR1988343_demo__consonance_jobs__0992701f6f	JOB NOT FOUND	DTB-116_Baseline	9bcf42e4-d3f7-5009-be60-770ccdaf83df	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-116	79d7936d-4630-432e-8ba0-2645fbbb3a5e	not provided	WCDT	rna_seq_quantification	SU2C	32a63fe9-7f8d-5738-9df7-45e97c17ea5a	DTB-116_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.1.3	31eb8451-d228-5eb7-be1a-c76b434b7052	\N	\N
ConsonanceTask_true_36_false_7d5655a4d7	SUCCESS	DTB-109_Baseline	13b04d7f-270e-5da5-97bf-a8bbdf066fff	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-109	fae520f2-6d56-4966-a4bf-36bdd63c547a	not provided	WCDT	rna_seq_quantification	SU2C	34b1cbc6-13f8-58da-8f7b-77d8149b45ff	DTB-109_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	3ff570e8-4821-583a-967e-f3a39822cc0d	2017-03-18T23:47:30.867+0000	2017-03-16T04:02:25.618+0000
ConsonanceTask_true_36_false_7208da1ec3	FAILED	TH07_0153_S01	60883a8d-edd7-5a18-8854-3fab59574112	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	TH07	TH07_0153	5c4248e9-010f-4cea-a5d4-ece482331a7a	BTO:0000214	Expression Analysis	rna_seq_quantification	Treehouse	a05ec5b5-10df-5224-9158-6bf92bf909a3	TH07_0153_S01	RNA-Seq	Cell line - derived from tumour	3.2.1-1	280a0bb3-42a9-5507-9490-c8f65786f65c	2017-03-16T06:15:06.206+0000	2017-03-16T00:53:14.487+0000
ConsonanceTask_true_36_false_554b090cba	FAILED	THR21_0552_S01	36adb3f1-ad2c-5a00-bcb7-8cb18d2b2cef	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR21	THR21_0552	0efc67a1-8bf2-499d-b47a-77da600330bd	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	e2965017-cf3a-5fb0-a837-e6305c330e26	THR21_0552_S01	RNA-Seq	Primary tumour - other	3.2.1-1	f5283d41-728a-506c-b70d-13af7aea511a	2017-04-13T16:15:58.349+0000	2017-04-12T22:02:04.047+0000
ConsonanceTask_true_36_false_7dc112be7d	SUCCESS	DTB-069_Baseline	b6a5a215-4ebd-5dda-ab1c-3ac9ea984470	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-069	ed42d65b-72ec-469b-9788-fd4a3d01cf3b	not provided	WCDT	rna_seq_quantification	SU2C	1aa50db8-be90-5c86-9208-4e329e2030c0	DTB-069_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	3956ac97-a6c0-521e-9494-5fc94821f422	2017-03-17T08:37:36.281+0000	2017-03-16T04:02:23.475+0000
ConsonanceTask_true_36_false_5b74d36d41	SUCCESS	DTB-055_Progression2	10ac050c-453f-5a3f-9169-f4a4077de935	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-055	b9aa4a2e-7efe-4e50-b7f7-78582a84bbbc	not provided	WCDT	rna_seq_quantification	SU2C	549265f7-f165-5f84-a3e8-a7b6614a0b08	DTB-055_Progression2_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	e7390f0d-19e4-57c4-885a-fc358c492180	2017-03-18T07:45:12.756+0000	2017-03-16T04:03:29.360+0000
ConsonanceTask_true_36_false_1993399f1e	FAILED	THR10_0235_S01	b44165ed-acd3-5ddf-90c1-65da7f39342d	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR10	THR10_0235	0b5f531e-ef2d-4a56-ba11-c8ab871ce2e6	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	214e6f73-3be2-5fc0-99f8-d1d83027e32d	THR10_0235_S01	RNA-Seq	Primary tumour - other	3.2.1-1	f1b049b4-c512-5b51-9a1c-6de42999eae1	2017-03-11T06:19:32.400+0000	2017-03-10T19:06:53.594+0000
ConsonanceTask_true_36_false_34c714f028	FAILED	THR23_0609_S01	367db59c-b33d-5e57-9cd9-2a0fc1edee43	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR23	THR23_0609	a7da6da1-838f-46d3-af1b-d625878cc85f	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	dcb516d2-c71b-5165-ba12-03e97548d956	THR23_0609_S01	RNA-Seq	Primary tumour - other	3.2.1-1	a83f976d-0541-52ab-986f-4a002ea88915	2017-04-11T06:00:38.759+0000	2017-04-10T23:52:15.488+0000
ConsonanceTask_true_36_false_997f63810f	FAILED	TH01_0051_S01	3594be8e-4b88-5a9a-a17e-624ea183536e	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	TH01	TH01_0051	adc7c30c-ed34-4ecc-9264-b29738958349	BTO:0000042	Expression Analysis P	rna_seq_quantification	Treehouse	19cdd827-6d1f-5979-b86c-09474cfee751	TH01_0051_S01	RNA-Seq	Primary tumour - other	3.2.1-1	a6c425af-e047-5c14-ab98-4b5f076e24c8	2017-04-14T11:13:35.340+0000	2017-04-14T00:02:05.213+0000
ConsonanceTask_true_36_false_bf1796ff79	FAILED	TH03_0108_S01	617c3af6-0ae7-5ef8-890e-e3110eb0bcf0	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	TH03	TH03_0108	a489f7b2-5f7d-4942-87b4-85ebc3bb126a	BTO:0000042	Expression Analysis P	rna_seq_quantification	Treehouse	6ea82e3f-1e5e-5b44-b74b-8aac71e75aa8	TH03_0108_S01	RNA-Seq	Primary tumour - other	3.2.1-1	6d7678e4-01e5-50c8-a545-26f3ebb83e64	2017-04-13T06:12:16.298+0000	2017-04-12T22:02:05.507+0000
ConsonanceTask_true_36_false_2922925544	FAILED	TH07_0155_S01	f39141ab-9779-53e4-8dce-3d8e4b9138c7	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	TH07	TH07_0155	c65b375c-6554-49f8-9765-70a5a064fb41	BTO:0000214	Expression Analysis	rna_seq_quantification	Treehouse	b2cdd713-d800-5cd9-9f38-0903bcae6cbf	TH07_0155_S01	RNA-Seq	Cell line - derived from tumour	3.2.1-1	188f14f2-7b31-5893-a9fc-dc0d4b652912	2017-04-11T08:09:31.743+0000	2017-04-10T23:52:43.269+0000
ConsonanceTask_true_36_false_5189afb810	FAILED	THR13_0385_S01	efb525d7-30c2-515a-8086-09c2b480e580	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR13	THR13_0385	3febfe63-e0f2-44ec-9646-b61e73095304	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	735a343d-81f0-530d-bf71-327b99be195a	THR13_0385_S01	RNA-Seq	Primary tumour - other	3.2.1-1	688bf6b7-1016-523e-9bb0-027af265bde3	2017-04-14T05:27:48.763+0000	2017-04-14T00:02:07.361+0000
ConsonanceTask_true_36_false_d458f0f1aa	FAILED	TH07_0152_S01	4fd77e23-4b7a-53a2-b103-c93349676462	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	TH07	TH07_0152	4afc0c77-9ec7-4725-9c17-e0a6a5bd0ba8	BTO:0000214	Expression Analysis	rna_seq_quantification	Treehouse	7628fe98-047c-5ad1-bf33-139035146db3	TH07_0152_S01	RNA-Seq	Cell line - derived from tumour	3.2.1-1	4c035f39-897a-5695-b241-48b809b9889e	2017-04-11T07:44:05.507+0000	2017-04-10T23:52:34.140+0000
ConsonanceTask_true_36_false_bd32ecfffa	FAILED	THR15_0355_S01	291a03ed-f1fb-56a0-a96a-e4a383847789	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR15	THR15_0355	20f0989b-eb78-4517-a938-11fd2c106b20	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	6ec4deff-fbf9-56d5-a76c-9e41afedd2f0	THR15_0355_S01	RNA-Seq	Primary tumour - other	3.2.1-1	5767cd5d-b03c-51bd-8198-8f7037d96d5f	2017-04-14T11:05:25.952+0000	2017-04-14T00:02:12.165+0000
ConsonanceTask_true_36_false_daa4d1b108	FAILED	THR21_0548_S01	0dcbbd9d-695b-56cf-8d3c-1ede82a34300	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR21	THR21_0548	24abada3-d35d-4df5-906f-67401a0da1e1	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	1a3f3efc-5334-59f3-a64d-5fb1cc6c5eed	THR21_0548_S01	RNA-Seq	Primary tumour - other	3.2.1-1	e8888226-77bc-5143-b3dc-167742cfba2d	2017-04-13T12:14:12.872+0000	2017-04-12T22:02:21.309+0000
ConsonanceTask_true_36_false_c215d944f7	FAILED	THR23_0608_S01	a5c8cf6e-c7bd-5dfe-89f6-fe99e651bfe1	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR23	THR23_0608	a0f0a138-2065-443b-b02f-7e060e12873f	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	2873275b-4992-54e0-bc17-2758fee7b098	THR23_0608_S01	RNA-Seq	Primary tumour - other	3.2.1-1	48b49fda-3380-5c92-94dc-8c19ff3c5ddb	2017-04-11T05:47:54.766+0000	2017-04-10T23:52:36.778+0000
ConsonanceTask_true_36_false_4319a9e6cb	FAILED	TH01_0135_S01	798d7f98-6831-5c4d-b11d-76a43420c006	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	TH01	TH01_0135	17bfea09-952d-409e-abc5-5a9de77553a8	BTO:0000042	Expression Analysis P	rna_seq_quantification	Treehouse	2d23ff9f-b3d3-5906-9d68-c367c00fbf44	TH01_0135_S01	RNA-Seq	Primary tumour - other	3.2.1-1	004223e8-796c-5fce-961f-37fabc66499e	2017-04-13T17:00:56.707+0000	2017-04-12T22:02:01.063+0000
ConsonanceTask_true_36_false_875da57308	FAILED	THR20_0517_S01	61a88e6c-ac44-507c-9ae6-2213c98f7d6a	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR20	THR20_0517	a9e065e3-94c3-429c-bc02-8ab116c3ca3e	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	9f51bece-a015-513c-adb6-84b21e0084c9	THR20_0517_S01	RNA-Seq	Primary tumour - other	3.2.1-1	438ae8c4-2288-56b0-8ec5-b2ba9c72884b	2017-04-14T12:47:15.462+0000	2017-04-12T22:02:22.843+0000
ConsonanceTask_true_36_false_af0fc5df1a	FAILED	THR21_0554_S01	51bc798f-568f-5591-b8da-dcd7388f0e3a	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR21	THR21_0554	1ac5a155-83a8-42e1-8357-78150d0065f5	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	17db167f-f957-583b-8eb3-590268aa5b4a	THR21_0554_S01	RNA-Seq	Primary tumour - other	3.2.1-1	af18cdb0-36f0-506b-a45c-288fbe9ecd14	2017-04-14T11:38:36.714+0000	2017-04-12T22:02:08.452+0000
ConsonanceTask_true_36_false_a0945f2d55	FAILED	THR21_0553_S01	4e522f78-e71c-5a11-8799-74c192c60b08	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR21	THR21_0553	5c099903-c030-44ce-87f3-3fa2214a0233	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	6818ef20-ae45-5e2a-a4b8-c0cf563a6e95	THR21_0553_S01	RNA-Seq	Primary tumour - other	3.2.1-1	ad638997-2e28-5d67-8aeb-7b727e3d8d4d	2017-04-14T05:22:08.775+0000	2017-04-12T22:02:40.366+0000
ConsonanceTask_true_36_false_c61adff7b8	FAILED	THR15_0343_S01	4458cd96-207a-50e2-b888-6eaa9d05d451	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR15	THR15_0343	e5c6df53-e96e-47a9-8a2e-3c493cb4782b	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	7f2823b5-6b36-57be-a5ab-f829ed19d9d3	THR15_0343_S01	RNA-Seq	Primary tumour - other	3.2.1-1	dc1f3d7a-e143-5baa-b910-f98f7b6741df	2017-04-13T08:18:35.778+0000	2017-04-12T22:02:16.820+0000
ConsonanceTask_true_36_false_3e34c725d2	FAILED	TH07_0154_S01	cab7de46-b0df-55a3-92c0-09ba8d95ec5b	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	TH07	TH07_0154	2b6b2b59-492a-4725-a9b7-3adf83b553ab	BTO:0000214	Expression Analysis	rna_seq_quantification	Treehouse	f184d6fa-2d8f-5366-a13e-809c8cb6b177	TH07_0154_S01	RNA-Seq	Cell line - derived from tumour	3.2.1-1	4386359f-bedd-5426-acb4-fb52f7c41399	2017-04-11T07:31:14.036+0000	2017-04-10T23:52:42.008+0000
ConsonanceTask_true_36_false_0869eb28fc	FAILED	THR11_0253_S01	907fe907-0e04-5297-8c0e-9236cbf1f6a4	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR11	THR11_0253	c13d297d-f060-4211-977e-59c6b2b44d97	BTO:0000042	Expression Analysis	rna_seq_quantification	Treehouse	fbec7fba-a441-56b3-a24b-89e0b40530cc	THR11_0253_S01	RNA-Seq	Primary tumour - other	3.2.1-1	266fc785-f685-5411-84e5-cbe94cd5eaf9	2017-04-16T20:31:46.249+0000	2017-04-16T20:17:04.943+0000
ConsonanceTask_true_36_false_77276b8295	FAILED	THR21_0555_S01	3476ccd0-fa7d-5a00-80d9-ff4906f57fdb	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR21	THR21_0555	6ba0f8f3-e1ca-4ea8-ac11-7ef3e9276f7b	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	d775320b-e22f-5fbe-9cf1-43e5287424f3	THR21_0555_S01	RNA-Seq	Primary tumour - other	3.2.1-1	7f9c78e5-c16d-51bf-b635-65ff97fccfc7	2017-04-14T19:23:31.478+0000	2017-04-14T00:02:10.666+0000
ConsonanceTask_true_36_false_487cec431e	FAILED	THR15_0358_S01	1f69dfe5-a0c2-50b2-9512-58e83b67fd24	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR15	THR15_0358	f1b49b47-d9ae-4e89-a57d-44af5fa08230	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	db02771c-a860-5b82-929f-8d0216e7f418	THR15_0358_S01	RNA-Seq	Primary tumour - other	3.2.1-1	97181911-7fde-572d-bba5-8c429aae51ad	2017-04-13T11:32:53.355+0000	2017-04-12T22:02:30.768+0000
ConsonanceTask_true_36_false_535c3c1fa5	FAILED	THR13_0386_S01	68f280a6-0ded-5c83-ab5f-2a4ee01035eb	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR13	THR13_0386	7ce00d61-94b7-40c3-af00-b3e8f5554a27	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	3d4360fb-705b-5783-b610-f4eaf8f76efc	THR13_0386_S01	RNA-Seq	Primary tumour - other	3.2.1-1	7077b8d8-ab41-54c4-8d43-e35c252db77e	2017-04-14T04:34:12.070+0000	2017-04-14T00:02:09.064+0000
ConsonanceTask_true_36_false_23e65d019d	SUCCESS	DTB-089_Baseline	4313f9f0-5db2-5dc4-911f-ab2f3e928afd	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-089	be10e4be-d7ec-4229-9789-27db899066f7	not provided	WCDT	rna_seq_quantification	SU2C	61a15a69-5e89-52a2-a4c2-0b6405968b2f	DTB-089_Baseline_1	RNA-Seq	Metastatic tumour - other	3.2.1-1	db55dbee-264c-55af-862d-6f13e5a14332	2017-03-18T23:37:02.688+0000	2017-03-16T04:02:43.720+0000
ConsonanceTask_true_36_false_8f795cf989	FAILED	THR23_0607_S01	768ff79e-c050-5caf-a345-f117457b34dd	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR23	THR23_0607	404589c6-9dd2-4132-9dfa-39403e12d3b1	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	eefa926f-1530-5922-a049-4f1aade5eeaf	THR23_0607_S01	RNA-Seq	Primary tumour - other	3.2.1-1	ab37d01b-6fdb-5578-a7f6-fe2733520d95	2017-04-11T06:23:02.111+0000	2017-04-10T23:52:30.135+0000
ConsonanceTask_true_36_false_8c38f7f394	SUCCESS	DTB-174_Baseline	fe20723a-e1bf-572c-aa13-ff1f8368c647	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-174	36e3d13d-9426-4976-a4e5-8d574b7156c8	not provided	WCDT	rna_seq_quantification	SU2C	5ef4d072-2f0a-5684-8f3a-45140778d224	DTB-174_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	c4fd671f-403c-5e20-8ca5-d6f55bd746d9	2017-03-18T07:30:31.951+0000	2017-03-16T04:03:24.055+0000
spawnFlop_SRR1988343_demo__consonance_jobs__0992701f5f	JOB NOT FOUND	DTB-060_Baseline	73e69dc8-af5c-5644-a646-63affb7d57bf	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-060	dcd9b733-0ed0-4b9b-a36e-8e377dd21f6b	not provided	WCDT	rna_seq_quantification	SU2C	56d18524-1258-51a0-b4ea-d68f6777085f	DTB-060_Baseline_1	RNA-Seq	Metastatic tumour - other	3.1.3	48d70635-c642-5a65-9140-64c927f24151	\N	\N
ConsonanceTask_true_36_false_863e841cc8	SUCCESS	DTB-128_Baseline	eb42a24b-f6ce-5f46-a257-ddc4eeb7a2f2	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-128	2db716ed-8f74-4a47-bd90-c064310a8412	not provided	WCDT	rna_seq_quantification	SU2C	450f8359-cb75-5512-9bd1-b079726f0efc	DTB-128_Baseline_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	197f1cf8-0958-52d6-9c48-43c699d38875	2017-03-17T23:01:03.719+0000	2017-03-16T04:03:06.561+0000
ConsonanceTask_true_36_false_2deb825f1f	SUCCESS	DTB-127_Progression	6147d704-456e-53dd-b32d-68c35488aef8	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	UCSF	DTB-127	d238f7b5-e30d-40ee-ac93-46055bc90364	not provided	WCDT	rna_seq_quantification	SU2C	20bf2d3a-5596-5513-83a3-d324af1785ae	DTB-127_Progression_1	RNA-Seq	Metastatic tumour - metastasis to distant location	3.2.1-1	c00a3daf-20c9-51a8-8d5c-976e167d8fcb	2017-03-18T20:41:08.856+0000	2017-03-16T04:03:38.969+0000
ConsonanceTask_true_36_false_b889577cea	FAILED	THR20_0510_S01	b029ff20-095d-5446-999b-5635df35eba3	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR20	THR20_0510	848c8768-c84b-4a2d-98c7-cff405a48a00	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	8d466b3b-fcae-51a9-95f3-ea3e6aad6ed0	THR20_0510_S01	RNA-Seq	Primary tumour - other	3.2.1-1	d1d2f248-482c-5315-b318-62ce626559c2	2017-04-13T14:12:10.638+0000	2017-04-12T22:02:24.348+0000
ConsonanceTask_true_36_false_9593c43ddd	FAILED	TH01_0119_S01	8a551373-fe50-52d8-86f8-f2d3ed8709ca	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	TH01	TH01_0119	4d8988b9-2f4f-4fc6-b50a-391bcdd529b3	BTO:0000042	Expression Analysis P	rna_seq_quantification	Treehouse	f71d5f5f-e5f3-5200-964e-dea935d3f6a4	TH01_0119_S01	RNA-Seq	Primary tumour - other	3.2.1-1	9b3c909e-693a-5fa8-bc39-147d3877f049	2017-04-14T06:20:23.556+0000	2017-04-12T22:02:02.599+0000
ConsonanceTask_true_36_false_0d72fc42c3	FAILED	THR21_0558_S01	83e0f1a2-9d5f-5399-a838-fb0ffbcd60cf	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR21	THR21_0558	2be3d04a-f47e-4d1b-bcc6-859f997258d5	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	c30a6a29-6b3e-5f2f-b2b8-b316d5bf4212	THR21_0558_S01	RNA-Seq	Primary tumour - other	3.2.1-1	7083240a-ec43-5bef-939d-38d5c6fd203d	2017-04-14T11:24:58.722+0000	2017-04-12T22:02:19.813+0000
ConsonanceTask_true_36_false_0685af9b32	FAILED	TH07_0151_S01	be1eec23-4026-51af-8568-20296099580f	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	TH07	TH07_0151	b7254617-0ebc-4aa9-9ee8-33aa57a3dc68	BTO:0000214	Expression Analysis	rna_seq_quantification	Treehouse	f0953cc2-552f-58d3-a17e-068592d735e0	TH07_0151_S01	RNA-Seq	Cell line - derived from tumour	3.2.1-1	a5c53c1d-4048-51e2-956e-65815371ed3a	2017-04-11T08:29:57.714+0000	2017-04-10T23:52:51.980+0000
ConsonanceTask_true_36_false_afffd9e32a	FAILED	THR23_0606_S01	a502ea65-4dae-535d-b4ff-a0064d084d97	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR23	THR23_0606	6e8770bc-1497-4a18-a70f-f05f52077450	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	5194424e-893c-5ef4-9245-01118fd18dae	THR23_0606_S01	RNA-Seq	Primary tumour - other	3.2.1-1	d1f50c38-6f5c-542a-a488-ffe73c72fcf4	2017-04-11T16:42:24.048+0000	2017-04-10T23:52:53.683+0000
ConsonanceTask_true_36_false_82079963ef	FAILED	THR13_0381_S01	b312ec3e-1381-5492-a20e-26d3afd1997e	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR13	THR13_0381	b6790f4e-48b3-4bcd-99a2-80c42104c095	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	f410b616-8dda-57ea-ac05-fb70ede7abfb	THR13_0381_S01	RNA-Seq	Primary tumour - other	3.2.1-1	e42461d1-e784-583a-b90a-ccf44a75c5ab	2017-04-18T21:36:40.328+0000	2017-04-18T15:17:02.363+0000
ConsonanceTask_true_36_false_443ce5d9fa	FAILED	THR13_0381_S03	53354368-1546-5431-8f55-3e255777d551	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	THR13	THR13_0381	21c96440-0cbf-4a2d-ad6a-c0169b565000	BTO:0000042	Expression Analysis R	rna_seq_quantification	Treehouse	f410b616-8dda-57ea-ac05-fb70ede7abfb	THR13_0381_S03	RNA-Seq	Primary tumour - other	3.2.1-1	9f221c5b-52ed-5504-a620-cd38ee052d79	2017-04-19T00:32:57.224+0000	2017-04-18T15:17:03.874+0000
ConsonanceTask_true_36_false_8fc4f1ccb7	FAILED	TH07_0150_S01	314ddbbd-8f88-5bd3-81db-75c6316c0ba8	quay.io/ucsc_cgl/rnaseq-cgl-pipeline	TH07	TH07_0150	7636cfd6-33bb-4146-b883-1d844977f293	BTO:0000214	Expression Analysis	rna_seq_quantification	Treehouse	358b1232-5681-5bae-b904-a8765b17cb2a	TH07_0150_S01	RNA-Seq	Cell line - derived from tumour	3.2.1-1	81090b39-ae12-5823-884b-dd5d6acc4bd7	2017-04-11T07:04:31.811+0000	2017-04-10T23:52:26.211+0000
\.


--
-- Name: luigi luigi_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY luigi
    ADD CONSTRAINT luigi_pkey PRIMARY KEY (luigi_job);


--
-- PostgreSQL database dump complete
--

\connect postgres

SET default_transaction_read_only = off;

--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.2
-- Dumped by pg_dump version 9.6.2

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: postgres; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON DATABASE postgres IS 'default administrative connection database';


--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- PostgreSQL database dump complete
--

\connect template1

SET default_transaction_read_only = off;

--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.2
-- Dumped by pg_dump version 9.6.2

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: template1; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON DATABASE template1 IS 'default template for new databases';


--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- PostgreSQL database dump complete
--

--
-- PostgreSQL database cluster dump complete
--

