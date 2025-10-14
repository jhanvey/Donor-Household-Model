-- LIFECYCLE_STATUS

SELECT 'CREATE TABLE dw.LIFECYCLE_STATUS' FROM Dual;

DROP TABLE dw.LIFECYCLE_STATUS;

CREATE TABLE dw.LIFECYCLE_STATUS AS

WITH
-- CTE to rank recognition records
 rankedgifts AS (
 SELECT
 session_no,
 gift_no,
 emplid,
 ext_org_id,
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

-- CTE to filter out future gift dates
 valid_gift_dates AS (
 SELECT
 g.gift_dt
 FROM src.gift_details g
 JOIN src.gift_sessions s
 ON g.session_no = s.session_no
 AND s.av_post_dt IS NOT NULL
 WHERE g.gift_dt <= CURRENT_DATE
 ),

-- CTE to determine anchor fiscal years
max_gift AS (
 SELECT
 MAX(gd.gift_dt) AS max_gift_dt,
 EXTRACT(YEAR FROM (MAX(gd.gift_dt) + INTERVAL '6' MONTH)) AS current_fy,
 EXTRACT(YEAR FROM (MAX(gd.gift_dt) + INTERVAL '6' MONTH)) - 1 AS prev_fy,
 EXTRACT(YEAR FROM (MAX(gd.gift_dt) + INTERVAL '6' MONTH)) - 2 AS two_fy_ago,
 EXTRACT(YEAR FROM (MAX(gd.gift_dt) + INTERVAL '6' MONTH)) - 3 AS three_fy_ago,
 EXTRACT(YEAR FROM (MAX(gd.gift_dt) + INTERVAL '6' MONTH)) - 4 AS four_fy_ago
 FROM valid_gift_dates gd
),

-- CTE to build gift_history using only the top-ranked recognition record per gift
 gift_history AS (
 SELECT
 COALESCE(psm.household_id,
 CASE
 WHEN rg.emplid <> ' ' THEN rg.emplid
 ELSE rg.ext_org_id
 END
 ) AS household_id,
 g.gift_dt,
 CASE
 WHEN EXTRACT(MONTH FROM g.gift_dt) >= 7
 THEN EXTRACT(YEAR FROM g.gift_dt) + 1
 ELSE EXTRACT(YEAR FROM g.gift_dt)
 END AS fiscal_year
 FROM src.gift_details g
 JOIN rankedgifts rg
 ON g.session_no = rg.session_no
 AND g.gift_no = rg.gift_no
 JOIN src.gift_designations des
 ON g.session_no = des.session_no
 AND g.gift_no = des.gift_no
 JOIN src.account_master acc
 ON acc.designation = des.designation
 LEFT JOIN dw.PERSON_SUMMARY psm
 ON rg.emplid = psm.emplid
 WHERE rg.rn = 1
 AND g.adjustment_flg = 'N'
 AND g.business_unit = 'ORG'
 AND g.institution = 'ORG'
 AND g.gift_amt_entry > 0
 AND g.gift_dt <= CURRENT_DATE
 AND acc.av_fasb_type <> '4'
 ),

-- Lifecycle bucket for current fiscal year (Target = mg.current_fy)
 current_status AS (
 SELECT
 ds.household_id,
 TO_CHAR(mg.current_fy - 1) || '-' ||
 LPAD(TO_CHAR(MOD(mg.current_fy,100)),2,'0') AS fiscal_year,
 CASE
 WHEN ds.first_gift_fy = mg.current_fy THEN 'New Donor'
 WHEN ds.first_gift_fy = mg.current_fy - 1 THEN 'New Last Year'
 WHEN ds.gave_target_minus_1 = 1
 AND ds.gave_target_minus_2 = 0 THEN 'Reactivated Last Year'
 WHEN ds.gave_target_minus_1 = 1
 AND ds.gave_target_minus_2 = 1 THEN 'Key Multi-Year'
 WHEN ds.gave_target_minus_1 = 0
 AND ds.gave_target_minus_2 = 1 THEN 'Recently Lapsed'
 WHEN ds.gave_target_minus_1 = 0
 AND ds.gave_target_minus_2 = 0
 AND ds.gave_within_last_five_excl = 1 THEN 'Lapsed'
 WHEN ds.gave_target_minus_1 = 0
 AND ds.gave_target_minus_2 = 0
 AND ds.gave_within_last_five_excl = 0
 AND ds.first_gift_fy < mg.current_fy - 5 THEN 'Long Lapsed'
 ELSE 'No Giving History'
 END AS lifecycle_status,
 COALESCE(hs.household_deceased,'No') AS household_deceased,
 ds.household_id ||
 SUBSTR(
 TO_CHAR(mg.current_fy - 1) || '-' ||
 LPAD(TO_CHAR(MOD(mg.current_fy,100)),2,'0'),
 LENGTH(TO_CHAR(mg.current_fy - 1) || '-' ||
 LPAD(TO_CHAR(MOD(mg.current_fy,100)),2,'0'))
 - 2,
 3
 ) AS lifecycle_id
 FROM (
 SELECT
 gh.household_id,
 MIN(gh.fiscal_year) AS first_gift_fy,
 MAX(CASE WHEN gh.fiscal_year = mg.current_fy - 1 THEN 1 ELSE 0 END) AS gave_target_minus_1,
 MAX(CASE WHEN gh.fiscal_year = mg.current_fy - 2 THEN 1 ELSE 0 END) AS gave_target_minus_2,
 MAX(CASE WHEN gh.fiscal_year BETWEEN mg.current_fy - 5 AND mg.current_fy - 3 THEN 1 ELSE 0 END) AS gave_within_last_five_excl
 FROM gift_history gh
 CROSS JOIN max_gift mg
 GROUP BY gh.household_id
 ) ds
 CROSS JOIN max_gift mg
 LEFT JOIN (
 SELECT
 psm.household_id,
 CASE
 WHEN COUNT(*) = COUNT(
 CASE
 WHEN psm.dt_of_death IS NOT NULL
 AND (
 CASE
 WHEN EXTRACT(MONTH FROM psm.dt_of_death) >= 7
 THEN EXTRACT(YEAR FROM psm.dt_of_death) + 1
 ELSE EXTRACT(YEAR FROM psm.dt_of_death)
 END
 ) < mg.current_fy
 THEN 1
 END
 ) THEN 'Yes'
 ELSE 'No'
 END AS household_deceased
 FROM dw.PERSON_SUMMARY psm
 CROSS JOIN max_gift mg
 GROUP BY psm.household_id, mg.current_fy
 ) hs
 ON ds.household_id = hs.household_id
 ),

-- Lifecycle bucket for previous fiscal year (Target = mg.prev_fy)
 prev_status AS (
 SELECT
 ds.household_id,
 TO_CHAR(mg.prev_fy - 1) || '-' ||
 LPAD(TO_CHAR(MOD(mg.prev_fy,100)),2,'0') AS fiscal_year,
 CASE
 WHEN ds.first_gift_fy = mg.prev_fy THEN 'New Donor'
 WHEN ds.first_gift_fy = mg.prev_fy - 1 THEN 'New Last Year'
 WHEN ds.gave_target_minus_1 = 1
 AND ds.gave_target_minus_2 = 0 THEN 'Reactivated Last Year'
 WHEN ds.gave_target_minus_1 = 1
 AND ds.gave_target_minus_2 = 1 THEN 'Key Multi-Year'
 WHEN ds.gave_target_minus_1 = 0
 AND ds.gave_target_minus_2 = 1 THEN 'Recently Lapsed'
 WHEN ds.gave_target_minus_1 = 0
 AND ds.gave_target_minus_2 = 0
 AND ds.gave_within_last_five_excl = 1 THEN 'Lapsed'
 WHEN ds.gave_target_minus_1 = 0
 AND ds.gave_target_minus_2 = 0
 AND ds.gave_within_last_five_excl = 0
 AND ds.first_gift_fy < mg.prev_fy - 5 THEN 'Long Lapsed'
 ELSE 'No Giving History'
 END AS lifecycle_status,
 COALESCE(hs.household_deceased,'No') AS household_deceased,
 ds.household_id ||
 SUBSTR(
 TO_CHAR(mg.prev_fy - 1) || '-' ||
 LPAD(TO_CHAR(MOD(mg.prev_fy,100)),2,'0'),
 LENGTH(TO_CHAR(mg.prev_fy - 1) || '-' ||
 LPAD(TO_CHAR(MOD(mg.prev_fy,100)),2,'0'))
 - 2,
 3
 ) AS lifecycle_id
 FROM (
 SELECT
 gh.household_id,
 MIN(gh.fiscal_year) AS first_gift_fy,
 MAX(CASE WHEN gh.fiscal_year = mg.prev_fy - 1 THEN 1 ELSE 0 END) AS gave_target_minus_1,
 MAX(CASE WHEN gh.fiscal_year = mg.prev_fy - 2 THEN 1 ELSE 0 END) AS gave_target_minus_2,
 MAX(CASE WHEN gh.fiscal_year BETWEEN mg.prev_fy - 5 AND mg.prev_fy - 3 THEN 1 ELSE 0 END) AS gave_within_last_five_excl
 FROM gift_history gh
 CROSS JOIN max_gift mg
 GROUP BY gh.household_id
 ) ds
 CROSS JOIN max_gift mg
 LEFT JOIN (
 SELECT
 psm.household_id,
 CASE
 WHEN COUNT(*) = COUNT(
 CASE
 WHEN psm.dt_of_death IS NOT NULL
 AND (
 CASE
 WHEN EXTRACT(MONTH FROM psm.dt_of_death) >= 7
 THEN EXTRACT(YEAR FROM psm.dt_of_death) + 1
 ELSE EXTRACT(YEAR FROM psm.dt_of_death)
 END
 ) < mg.prev_fy
 THEN 1
 END
 ) THEN 'Yes'
 ELSE 'No'
 END AS household_deceased
 FROM dw.PERSON_SUMMARY psm
 CROSS JOIN max_gift mg
 GROUP BY psm.household_id, mg.prev_fy
 ) hs
 ON ds.household_id = hs.household_id
 ),

-- Lifecycle bucket for two fiscal years ago (Target = mg.two_fy_ago)
 two_status AS (
 SELECT
 ds.household_id,
 TO_CHAR(mg.two_fy_ago - 1) || '-' ||
 LPAD(TO_CHAR(MOD(mg.two_fy_ago,100)),2,'0') AS fiscal_year,
 CASE
 WHEN ds.first_gift_fy = mg.two_fy_ago THEN 'New Donor'
 WHEN ds.first_gift_fy = mg.two_fy_ago - 1 THEN 'New Last Year'
 WHEN ds.gave_target_minus_1 = 1
 AND ds.gave_target_minus_2 = 0 THEN 'Reactivated Last Year'
 WHEN ds.gave_target_minus_1 = 1
 AND ds.gave_target_minus_2 = 1 THEN 'Key Multi-Year'
 WHEN ds.gave_target_minus_1 = 0
 AND ds.gave_target_minus_2 = 1 THEN 'Recently Lapsed'
 WHEN ds.gave_target_minus_1 = 0
 AND ds.gave_target_minus_2 = 0
 AND ds.gave_within_last_five_excl = 1 THEN 'Lapsed'
 WHEN ds.gave_target_minus_1 = 0
 AND ds.gave_target_minus_2 = 0
 AND ds.gave_within_last_five_excl = 0
 AND ds.first_gift_fy < mg.two_fy_ago - 5 THEN 'Long Lapsed'
 ELSE 'No Giving History'
 END AS lifecycle_status,
 COALESCE(hs.household_deceased,'No') AS household_deceased,
 ds.household_id ||
 SUBSTR(
 TO_CHAR(mg.two_fy_ago - 1) || '-' ||
 LPAD(TO_CHAR(MOD(mg.two_fy_ago,100)),2,'0'),
 LENGTH(TO_CHAR(mg.two_fy_ago - 1) || '-' ||
 LPAD(TO_CHAR(MOD(mg.two_fy_ago,100)),2,'0'))
 - 2,
 3
 ) AS lifecycle_id
 FROM (
 SELECT
 gh.household_id,
 MIN(gh.fiscal_year) AS first_gift_fy,
 MAX(CASE WHEN gh.fiscal_year = mg.two_fy_ago - 1 THEN 1 ELSE 0 END) AS gave_target_minus_1,
 MAX(CASE WHEN gh.fiscal_year = mg.two_fy_ago - 2 THEN 1 ELSE 0 END) AS gave_target_minus_2,
 MAX(CASE WHEN gh.fiscal_year BETWEEN mg.two_fy_ago - 5 AND mg.two_fy_ago - 3 THEN 1 ELSE 0 END) AS gave_within_last_five_excl
 FROM gift_history gh
 CROSS JOIN max_gift mg
 GROUP BY gh.household_id
 ) ds
 CROSS JOIN max_gift mg
 LEFT JOIN (
 SELECT
 psm.household_id,
 CASE
 WHEN COUNT(*) = COUNT(
 CASE
 WHEN psm.dt_of_death IS NOT NULL
 AND (
 CASE
 WHEN EXTRACT(MONTH FROM psm.dt_of_death) >= 7
 THEN EXTRACT(YEAR FROM psm.dt_of_death) + 1
 ELSE EXTRACT(YEAR FROM psm.dt_of_death)
 END
 ) < mg.two_fy_ago
 THEN 1
 END
 ) THEN 'Yes'
 ELSE 'No'
 END AS household_deceased
 FROM dw.PERSON_SUMMARY psm
 CROSS JOIN max_gift mg
 GROUP BY psm.household_id, mg.two_fy_ago
 ) hs
 ON ds.household_id = hs.household_id
 ),

-- Lifecycle bucket for three fiscal years ago (Target = mg.three_fy_ago)
 three_status AS (
 SELECT
 ds.household_id,
 TO_CHAR(mg.three_fy_ago - 1) || '-' ||
 LPAD(TO_CHAR(MOD(mg.three_fy_ago,100)),2,'0') AS fiscal_year,
 CASE
 WHEN ds.first_gift_fy = mg.three_fy_ago THEN 'New Donor'
 WHEN ds.first_gift_fy = mg.three_fy_ago - 1 THEN 'New Last Year'
 WHEN ds.gave_target_minus_1 = 1
 AND ds.gave_target_minus_2 = 0 THEN 'Reactivated Last Year'
 WHEN ds.gave_target_minus_1 = 1
 AND ds.gave_target_minus_2 = 1 THEN 'Key Multi-Year'
 WHEN ds.gave_target_minus_1 = 0
 AND ds.gave_target_minus_2 = 1 THEN 'Recently Lapsed'
 WHEN ds.gave_target_minus_1 = 0
 AND ds.gave_target_minus_2 = 0
 AND ds.gave_within_last_five_excl = 1 THEN 'Lapsed'
 WHEN ds.gave_target_minus_1 = 0
 AND ds.gave_target_minus_2 = 0
 AND ds.gave_within_last_five_excl = 0
 AND ds.first_gift_fy < mg.three_fy_ago - 5 THEN 'Long Lapsed'
 ELSE 'No Giving History'
 END AS lifecycle_status,
 COALESCE(hs.household_deceased,'No') AS household_deceased,
 ds.household_id ||
 SUBSTR(
 TO_CHAR(mg.three_fy_ago - 1) || '-' ||
 LPAD(TO_CHAR(MOD(mg.three_fy_ago,100)),2,'0'),
 LENGTH(TO_CHAR(mg.three_fy_ago - 1) || '-' ||
 LPAD(TO_CHAR(MOD(mg.three_fy_ago,100)),2,'0'))
 - 2,
 3
 ) AS lifecycle_id
 FROM (
 SELECT
 gh.household_id,
 MIN(gh.fiscal_year) AS first_gift_fy,
 MAX(CASE WHEN gh.fiscal_year = mg.three_fy_ago - 1 THEN 1 ELSE 0 END) AS gave_target_minus_1,
 MAX(CASE WHEN gh.fiscal_year = mg.three_fy_ago - 2 THEN 1 ELSE 0 END) AS gave_target_minus_2,
 MAX(CASE WHEN gh.fiscal_year BETWEEN mg.three_fy_ago - 5 AND mg.three_fy_ago - 3 THEN 1 ELSE 0 END) AS gave_within_last_five_excl
 FROM gift_history gh
 CROSS JOIN max_gift mg
 GROUP BY gh.household_id
 ) ds
 CROSS JOIN max_gift mg
 LEFT JOIN (
 SELECT
 psm.household_id,
 CASE
 WHEN COUNT(*) = COUNT(
 CASE
 WHEN psm.dt_of_death IS NOT NULL
 AND (
 CASE
 WHEN EXTRACT(MONTH FROM psm.dt_of_death) >= 7
 THEN EXTRACT(YEAR FROM psm.dt_of_death) + 1
 ELSE EXTRACT(YEAR FROM psm.dt_of_death)
 END
 ) < mg.three_fy_ago
 THEN 1
 END
 ) THEN 'Yes'
 ELSE 'No'
 END AS household_deceased
 FROM dw.PERSON_SUMMARY psm
 CROSS JOIN max_gift mg
 GROUP BY psm.household_id, mg.three_fy_ago
 ) hs
 ON ds.household_id = hs.household_id
 ),

-- Lifecycle bucket for four fiscal years ago (Target = mg.four_fy_ago)
 four_status AS (
 SELECT
 ds.household_id,
 TO_CHAR(mg.four_fy_ago - 1) || '-' ||
 LPAD(TO_CHAR(MOD(mg.four_fy_ago,100)),2,'0') AS fiscal_year,
 CASE
 WHEN ds.first_gift_fy = mg.four_fy_ago THEN 'New Donor'
 WHEN ds.first_gift_fy = mg.four_fy_ago - 1 THEN 'New Last Year'
 WHEN ds.gave_target_minus_1 = 1
 AND ds.gave_target_minus_2 = 0 THEN 'Reactivated Last Year'
 WHEN ds.gave_target_minus_1 = 1
 AND ds.gave_target_minus_2 = 1 THEN 'Key Multi-Year'
 WHEN ds.gave_target_minus_1 = 0
 AND ds.gave_target_minus_2 = 1 THEN 'Recently Lapsed'
 WHEN ds.gave_target_minus_1 = 0
 AND ds.gave_target_minus_2 = 0
 AND ds.gave_within_last_five_excl = 1 THEN 'Lapsed'
 WHEN ds.gave_target_minus_1 = 0
 AND ds.gave_target_minus_2 = 0
 AND ds.gave_within_last_five_excl = 0
 AND ds.first_gift_fy < mg.four_fy_ago - 5 THEN 'Long Lapsed'
 ELSE 'No Giving History'
 END AS lifecycle_status,
 COALESCE(hs.household_deceased,'No') AS household_deceased,
 ds.household_id ||
 SUBSTR(
 TO_CHAR(mg.four_fy_ago - 1) || '-' ||
 LPAD(TO_CHAR(MOD(mg.four_fy_ago,100)),2,'0'),
 LENGTH(TO_CHAR(mg.four_fy_ago - 1) || '-' ||
 LPAD(TO_CHAR(MOD(mg.four_fy_ago,100)),2,'0'))
 - 2,
 3
 ) AS lifecycle_id
 FROM (
 SELECT
 gh.household_id,
 MIN(gh.fiscal_year) AS first_gift_fy,
 MAX(CASE WHEN gh.fiscal_year = mg.four_fy_ago - 1 THEN 1 ELSE 0 END) AS gave_target_minus_1,
 MAX(CASE WHEN gh.fiscal_year = mg.four_fy_ago - 2 THEN 1 ELSE 0 END) AS gave_target_minus_2,
 MAX(CASE WHEN gh.fiscal_year BETWEEN mg.four_fy_ago - 5 AND mg.four_fy_ago - 3 THEN 1 ELSE 0 END) AS gave_within_last_five_excl
 FROM gift_history gh
 CROSS JOIN max_gift mg
 GROUP BY gh.household_id
 ) ds
 CROSS JOIN max_gift mg
 LEFT JOIN (
 SELECT
 psm.household_id,
 CASE
 WHEN COUNT(*) = COUNT(
 CASE
 WHEN psm.dt_of_death IS NOT NULL
 AND (
 CASE
 WHEN EXTRACT(MONTH FROM psm.dt_of_death) >= 7
 THEN EXTRACT(YEAR FROM psm.dt_of_death) + 1
 ELSE EXTRACT(YEAR FROM psm.dt_of_death)
 END
 ) < mg.four_fy_ago
 THEN 1
 END
 ) THEN 'Yes'
 ELSE 'No'
 END AS household_deceased
 FROM dw.PERSON_SUMMARY psm
 CROSS JOIN max_gift mg
 GROUP BY psm.household_id, mg.four_fy_ago
 ) hs
 ON ds.household_id = hs.household_id
 )

-- Outer query: Add sort_order based on lifecycle_status.
SELECT
 household_id,
 fiscal_year,
 lifecycle_status,
 household_deceased,
 lifecycle_id,
 CASE lifecycle_status
 WHEN 'Key Multi-Year' THEN 1
 WHEN 'New Last Year' THEN 2
 WHEN 'Reactivated Last Year' THEN 3
 WHEN 'New Donor' THEN 4
 WHEN 'Recently Lapsed' THEN 5
 WHEN 'Lapsed' THEN 6
 WHEN 'Long Lapsed' THEN 7
 WHEN 'No Giving History' THEN 8
 ELSE 9
 END AS sort_order
FROM (
 SELECT * FROM current_status
 UNION ALL
 SELECT * FROM prev_status
 UNION ALL
 SELECT * FROM two_status
 UNION ALL
 SELECT * FROM three_status
 UNION ALL
 SELECT * FROM four_status
) combined
ORDER BY household_id, fiscal_year DESC;
COMMIT;
