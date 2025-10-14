-- PERSON_SUMMARY
SELECT
 'CREATE TABLE dw.PERSON_SUMMARY'
FROM
 dual;

DROP TABLE dw.PERSON_SUMMARY;

CREATE TABLE dw.PERSON_SUMMARY (
 EMPLID VARCHAR2(11 CHAR),
 HOUSEHOLD_ID VARCHAR2(22 CHAR),
 STATE VARCHAR2(6 CHAR),
 POSTAL VARCHAR2(5 CHAR),
 COUNTRY VARCHAR2(3 CHAR),
 TERRITORY NUMBER,
 BIRTHDATE DATE,
 AGE NUMBER,
 GENDER VARCHAR2(1 CHAR),
 MARITAL_STATUS VARCHAR2(1 CHAR),
 SPOUSE_EMPLID VARCHAR2(11 CHAR),
 DT_OF_DEATH DATE,
 CONSTITUENT_TYPES VARCHAR2(10 CHAR),
 SERVICE_INDICATORS VARCHAR2(200 CHAR),
 INVOLVEMENT_CODES VARCHAR2(200 CHAR),
 MBI_CR_SRC_CD VARCHAR2(12 CHAR)
)
TABLESPACE USERS;

INSERT INTO dw.PERSON_SUMMARY
 WITH personal_data_cte AS (
 SELECT
 pd.emplid,
 decode(pd.address1,
 ' ',
 decode(pd.state, ' ', pd.state_other, pd.state),
 pd.state) AS state,
 substr(decode(pd.address1,
 ' ',
 decode(pd.postal, ' ', pd.postal_other, pd.postal),
 pd.postal),
 1,
 5) AS postal,
 decode(pd.address1,
 ' ',
 decode(pd.country, ' ', pd.country_other, pd.country),
 pd.country) AS country,
 (
 SELECT
 z.mbi_territory_code
 FROM
 src.zip_territories z
 WHERE
 z.zip = substr(decode(pd.address1,
 ' ',
 decode(pd.postal, ' ', pd.postal_other, pd.postal),
 pd.postal),
 1,
 5)
 ) AS territory,
 pd.birthdate,
 round(months_between(sysdate, pd.birthdate) / 12,
 0) AS age,
 pd.sex AS gender,
 pd.mar_status AS marital_status,
 (
 SELECT
 rel.emplid_related
 FROM
 src.crm_relationships rel
 WHERE
 rel.eff_status = 'A'
 AND rel.people_relation IN ('SP', 'WI', 'LP')
 AND pd.emplid = rel.emplid
 AND ROWNUM = 1
 ) AS spouse_emplid,
 pd.dt_of_death,
 (
 SELECT
 LISTAGG(cnst_type, ',' ON OVERFLOW TRUNCATE) WITHIN GROUP(
 ORDER BY
 1
 )
 FROM
 src.crm_constituent_types
 WHERE
 emplid = pd.emplid
 AND cnst_type_past_flg <> 'N'
 AND ROWNUM < 1000
 ) AS constituent_types,
 (
 SELECT
 LISTAGG(srvc_ind_cd, ',' ON OVERFLOW TRUNCATE) WITHIN GROUP(
 ORDER BY
 srvc_ind_cd
 )
 FROM
 (
 SELECT
 srvc_ind_cd
 FROM
 src.crm_service_indicators
 WHERE
 emplid = pd.emplid
 AND ( scc_si_end_dt IS NULL
 OR scc_si_end_dt < srvc_ind_active_dt )
 )
 ) AS service_indicators,
 (
 SELECT
 LISTAGG(invlv_cd, ',' ON OVERFLOW TRUNCATE) WITHIN GROUP(
 ORDER BY
 invlv_cd
 )
 FROM
 (
 SELECT
 invlv_cd
 FROM
 src.crm_involvements
 WHERE
 emplid = pd.emplid
 AND invlv_ctgy_cd = 'STEW'
 AND ( end_dt IS NULL
 OR end_dt < start_dt )
 )
 ) AS involvement_codes,
 (
 SELECT
 mbi_cr_src_cd
 FROM
 src.crm_source_codes
 WHERE
 emplid = pd.emplid
 ) AS mbi_cr_src_cd
 FROM
 src.crm_personal_data pd
 )
 SELECT
 emplid,
 ltrim(
 CASE
 WHEN spouse_emplid IS NULL THEN
 to_char(emplid)
 WHEN emplid < spouse_emplid THEN
 to_char(emplid)
 || '-'
 || to_char(spouse_emplid)
 ELSE
 to_char(spouse_emplid)
 || '-'
 || to_char(emplid)
 END,
 ' -') AS household_id,
 state,
 postal,
 country,
 territory,
 birthdate,
 age,
 gender,
 marital_status,
 spouse_emplid,
 dt_of_death,
 constituent_types,
 service_indicators,
 involvement_codes,
 mbi_cr_src_cd
 FROM
 personal_data_cte;
