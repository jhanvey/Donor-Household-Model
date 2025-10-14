-- GIFT_HOUSEHOLD_LIFECYLCE

SELECT 'CREATE TABLE dw.GIFT_HOUSEHOLD_LIFECYCLE' from Dual;

DROP TABLE dw.GIFT_HOUSEHOLD_LIFECYCLE;

CREATE TABLE dw.GIFT_HOUSEHOLD_LIFECYCLE AS

WITH rankedgifts AS (
 -- Rank recognition records per gift based on recognition percentage, sa_id_type, and recognition type
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

-- Determine the first gift per household based on the earliest gift date and the lowest gift number on that date
firstgifts AS (
 SELECT
 household_id,
 gift_dt AS first_gift_dt,
 gift_no AS first_gift_no
 FROM (
 SELECT
 COALESCE(psm.household_id,
 CASE WHEN r.emplid != ' ' THEN r.emplid ELSE r.ext_org_id END
 ) AS household_id,
 g.gift_dt,
 g.gift_no,
 ROW_NUMBER() OVER (
 PARTITION BY COALESCE(psm.household_id,
 CASE WHEN r.emplid != ' ' THEN r.emplid ELSE r.ext_org_id END
 )
 ORDER BY g.gift_dt, g.gift_no
 ) AS rn
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
 ) sub
 WHERE rn = 1
)

-- Final Query
SELECT DISTINCT
 g.session_no || '-' || g.gift_no AS gift_id,
 CASE WHEN r.emplid != ' ' THEN r.emplid ELSE r.ext_org_id END AS donor_id,
 COALESCE(psm.household_id,
 CASE WHEN r.emplid != ' ' THEN r.emplid ELSE r.ext_org_id END
 ) AS household_id,
 COALESCE(psm.household_id,
 CASE WHEN r.emplid != ' ' THEN r.emplid ELSE r.ext_org_id END
 ) || '-' ||
 TO_CHAR(
 MOD(
 CASE
 WHEN TO_NUMBER(TO_CHAR(g.gift_dt,'MM')) >= 7 THEN TO_NUMBER(TO_CHAR(g.gift_dt,'YYYY')) + 1
 ELSE TO_NUMBER(TO_CHAR(g.gift_dt,'YYYY'))
 END,
 100
 ),
 'FM00'
 ) AS lifecycle_id,
 r.sa_id_type,
 g.gift_amt AS total_gift_amount,
 g.gift_type,
 g.pledge_gift_no,
 g.gift_dt,
 s.av_post_dt,
 g.tender_type,
 tx.mbi_giftsource AS gift_source,
 d.designation,
 d.intv_cd,
 d.designation_pct,
 d.des_pct_amt AS designation_amt,
 d.motivation_cd,
 d.av_inst_type AS designation_type,
 acc.av_fasb_type,
 tx.mbi_stationgrp,
 CASE
 WHEN tx.mbi_pri_station = ' ' THEN NULL
 ELSE tx.mbi_pri_station
 END AS mbi_pri_station,
 CASE
 WHEN g.gift_dt = f.first_gift_dt
 AND g.gift_no = f.first_gift_no THEN 'Yes'
 ELSE 'No'
 END AS first_time_gift
FROM src.gift_details g
INNER JOIN rankedgifts r
 ON g.session_no = r.session_no
 AND g.gift_no = r.gift_no
LEFT JOIN dw.PERSON_SUMMARY psm
 ON r.emplid = psm.emplid
LEFT JOIN src.gift_sessions s
 ON g.session_no = s.session_no
 AND s.sess_status = 'P'
LEFT JOIN src.tx_data tx
 ON g.session_no = tx.session_no
 AND g.gift_no = tx.gift_no
LEFT JOIN src.gift_designations d
 ON g.session_no = d.session_no
 AND g.gift_no = d.gift_no
LEFT JOIN src.account_master acc
 ON d.designation = acc.designation
LEFT JOIN src.tx_data tx_station
 ON g.session_no = tx_station.session_no
 AND g.gift_no = tx_station.gift_no
LEFT JOIN firstgifts f
 ON COALESCE(psm.household_id,
 CASE WHEN r.emplid != ' ' THEN r.emplid ELSE r.ext_org_id END
 ) = f.household_id
WHERE
 r.rn = 1 -- Select the top-ranked recognition record per gift
 AND g.adjustment_flg = 'N' -- Exclude adjusted gifts
 AND g.business_unit = 'ORG' -- Filter by business unit
 AND g.institution = 'ORG' -- Filter by institution
 AND g.gift_dt >= (
 SELECT
 ADD_MONTHS(
 CASE
 -- if the max gift_dt is on/after July 20 of its year, start new FY July 1 of that year
 WHEN MAX(g2.gift_dt) 
 >= TRUNC(MAX(g2.gift_dt), 'YEAR') 
 + INTERVAL '6' MONTH 
 + INTERVAL '19' DAY
 THEN TRUNC(MAX(g2.gift_dt), 'YEAR') + INTERVAL '6' MONTH
 -- otherwise keep last FY (i.e. start July 1 of prior year)
 ELSE TRUNC(MAX(g2.gift_dt), 'YEAR') - INTERVAL '6' MONTH
 END,
 -48 -- back up 48 months = previous 4 FY start points
 )
 FROM src.gift_details g2
 JOIN src.gift_sessions s2
 ON g2.session_no = s2.session_no
 AND s2.av_post_dt IS NOT NULL -- only consider gift_dts with a post date
 WHERE g2.gift_dt <= CURRENT_DATE
 )
 AND g.gift_amt_entry > 0 -- Exclude gifts with zero or negative amounts
 AND (acc.av_fasb_type IS NULL OR acc.av_fasb_type <> '4') -- Exclude AV_FASB_TYPE = '4'
 AND s.av_post_dt IS NOT NULL
ORDER BY g.gift_dt DESC;
COMMIT;
