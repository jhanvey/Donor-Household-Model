-- HOUSEHOLD_INFO

SELECT 'CREATE TABLE dw.HOUSEHOLD_INFO' FROM Dual;

DROP TABLE dw.HOUSEHOLD_INFO;

CREATE TABLE dw.HOUSEHOLD_INFO AS

WITH
 -- Ranking the recognition rows
 rankedgifts AS (
 SELECT
 session_no,
 gift_no,
 emplid,
 ext_org_id,
 sa_id_type,
 ROW_NUMBER() OVER (
 PARTITION BY session_no, gift_no
 ORDER BY
 CASE WHEN recognition_pct = 100 THEN 0 ELSE 1 END,
 sa_id_type DESC,
 recognition_type ASC,
 emplid ASC
 ) AS rn
 FROM src.gift_recognition
 ),

 -- Designation buckets used later for donor category logic
 tw_designations AS (
 SELECT DISTINCT designation
 FROM src.account_master
 WHERE av_inst_type = 'TW'
 ),
 radio_designations AS (
 SELECT DISTINCT designation
 FROM src.account_master
 WHERE av_inst_type IN ('RU','MR','SH')
 ),

 -- Consolidated gift data with only needed columns
 gift_data AS (
 SELECT
 g.gift_dt,
 g.gift_amt_entry,
 r.sa_id_type,
 COALESCE(psm.household_id, CASE WHEN r.emplid <> ' ' THEN r.emplid ELSE r.ext_org_id END) AS household_id,
 TO_NUMBER(TO_CHAR(g.gift_dt, 'MM')) AS gift_month,
 TO_NUMBER(TO_CHAR(g.gift_dt, 'YYYY')) AS gift_year,
 d.designation,
 d.av_inst_type AS designation_type,
 d.des_pct_amt AS designation_amt,
 d.intv_cd,
 acc.av_fasb_type
 FROM src.gift_details g
 INNER JOIN rankedgifts r
 ON g.session_no = r.session_no
 AND g.gift_no = r.gift_no
 LEFT JOIN dw.PERSON_SUMMARY psm
 ON r.emplid = psm.emplid
 LEFT JOIN src.gift_designations d
 ON g.session_no = d.session_no
 AND g.gift_no = d.gift_no
 LEFT JOIN src.account_master acc
 ON d.designation = acc.designation
 WHERE r.rn = 1
 AND g.adjustment_flg = 'N'
 AND g.business_unit = 'ORG'
 AND g.institution = 'ORG'
 AND g.gift_amt_entry > 0
 AND (acc.av_fasb_type IS NULL OR acc.av_fasb_type <> '4')
 ),

 -- Section 1: Donor Category per Household
 aggregated AS (
 SELECT
 household_id,
 SUM(designation_amt) AS total_amt,
 SUM(CASE WHEN designation IN (SELECT designation FROM tw_designations) THEN designation_amt ELSE 0 END) AS tw_amt,
 SUM(CASE WHEN designation IN (SELECT designation FROM radio_designations) THEN designation_amt ELSE 0 END) AS radio_amt,
 SUM(CASE WHEN designation NOT IN (SELECT designation FROM tw_designations)
 AND designation NOT IN (SELECT designation FROM radio_designations)
 THEN designation_amt ELSE 0 END) AS institute_amt
 FROM gift_data
 GROUP BY household_id
 ),
 donor_category_cte AS (
 SELECT
 household_id,
 CASE
 WHEN total_amt = tw_amt THEN 'Today in the Word Only'
 WHEN total_amt = radio_amt THEN 'Radio Only'
 WHEN total_amt = institute_amt THEN 'Institute Only'
 WHEN tw_amt > radio_amt AND tw_amt > institute_amt THEN 'Combo Today in the Word'
 WHEN radio_amt > tw_amt AND radio_amt > institute_amt THEN 'Combo Radio'
 ELSE 'Combo Institute'
 END AS donor_category
 FROM aggregated
 ),

 -- Section 2: Giving Level per Household Based on Fiscal Year Aggregation
 gift_fiscal_totals AS (
 SELECT
 household_id,
 CASE WHEN gift_month >= 7 THEN gift_year + 1 ELSE gift_year END AS fiscal_year,
 SUM(designation_amt) AS total_gift_amount
 FROM gift_data
 WHERE gift_year >= 1996
 GROUP BY household_id,
 CASE WHEN gift_month >= 7 THEN gift_year + 1 ELSE gift_year END
 ),
 household_giving AS (
 SELECT
 household_id,
 MAX(CASE WHEN total_gift_amount >= 10000 THEN 1 ELSE 0 END) AS is_major,
 MAX(CASE WHEN total_gift_amount >= 1000 THEN 1 ELSE 0 END) AS is_mid
 FROM gift_fiscal_totals
 GROUP BY household_id
 ),
 giving_info AS (
 SELECT
 household_id,
 CASE
 WHEN is_major = 1 THEN 'Major'
 WHEN is_mid = 1 THEN 'Mid-level'
 ELSE 'General'
 END AS giving_level
 FROM household_giving
 ),
 giving_level_cte AS (
 SELECT household_id, COALESCE(giving_level, 'General') AS giving_level
 FROM giving_info
 ),

 -- Section 3: Additional Household Attributes
 agg AS (
 SELECT
 household_id,
 MAX(CASE WHEN sa_id_type = 'P' THEN 1 ELSE 0 END) AS has_person,
 MAX(CASE WHEN sa_id_type = 'O' AND designation_type = 'RU' THEN 1 ELSE 0 END) AS has_ru
 FROM gift_data
 GROUP BY household_id
 ),
 household_involvement AS (
 SELECT
 psm.household_id,
 MAX(CASE WHEN inv.invlv_cd = 'AGD' AND inv.end_dt IS NULL THEN 1 ELSE 0 END) AS is_autogiver,
 MAX(CASE WHEN inv.invlv_cd = 'MPP' AND inv.end_dt IS NULL THEN 1 ELSE 0 END) AS is_monthly_partner
 FROM dw.PERSON_SUMMARY psm
 LEFT JOIN src.crm_involvements inv
 ON psm.emplid = inv.emplid
 GROUP BY psm.household_id
 ),
 household_constituent_info AS (
 SELECT
 a.household_id,
 CASE
 WHEN a.has_person = 1 THEN 'Person'
 WHEN a.has_ru = 1 THEN 'Radio Underwriter'
 ELSE 'Organization'
 END AS constituent_type,
 CASE WHEN hi.is_monthly_partner = 1 THEN 'Yes' ELSE 'No' END AS monthly_partner,
 CASE WHEN hi.is_autogiver = 1 THEN 'Yes' ELSE 'No' END AS autogiver
 FROM agg a
 LEFT JOIN household_involvement hi
 ON a.household_id = hi.household_id
 ),

 -- Section 4: Postal and Source Code from donor data
 household_postal AS (
 SELECT
 household_id,
 postal
 FROM (
 SELECT
 psm.household_id,
 psm.postal,
 ROW_NUMBER() OVER (
 PARTITION BY psm.household_id
 ORDER BY CASE WHEN psm.postal IS NOT NULL THEN 0 ELSE 1 END, psm.emplid
 ) AS rn
 FROM dw.PERSON_SUMMARY psm
 )
 WHERE rn = 1
 ),
 household_source AS (
 SELECT
 household_id,
 mbi_cr_src_cd AS source_code
 FROM (
 SELECT
 psm.household_id,
 psm.mbi_cr_src_cd,
 ROW_NUMBER() OVER (
 PARTITION BY psm.household_id
 ORDER BY CASE WHEN psm.mbi_cr_src_cd IS NOT NULL THEN 0 ELSE 1 END, psm.emplid
 ) AS rn
 FROM dw.PERSON_SUMMARY psm
 )
 WHERE rn = 1
 )
 
-- FINAL SELECT: Combine All Information by Household
SELECT
 hc.household_id,
 hc.constituent_type,
 dc.donor_category,
 COALESCE(gl.giving_level, 'General') AS giving_level,
 hc.monthly_partner,
 hc.autogiver,
 (
 hp.postal 
 || (
 SELECT SUBSTR(org.postal, 1, 5) 
 FROM src.org_addresses org
 WHERE hc.household_id = org.ext_org_id 
 AND ROWNUM = 1)
 ) AS postal,
 hs.source_code
FROM household_constituent_info hc
LEFT JOIN donor_category_cte dc ON hc.household_id = dc.household_id
LEFT JOIN giving_level_cte gl ON hc.household_id = gl.household_id
LEFT JOIN household_postal hp ON hc.household_id = hp.household_id
LEFT JOIN household_source hs ON hc.household_id = hs.household_id
ORDER BY hc.household_id;
COMMIT;
