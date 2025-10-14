-- HOUSEHOLD_ASSIGNMENTS

SELECT 'CREATE TABLE dw.HOUSEHOLD_ASSIGNMENTS' FROM Dual;

DROP TABLE dw.HOUSEHOLD_ASSIGNMENTS;

CREATE TABLE dw.HOUSEHOLD_ASSIGNMENTS AS

WITH
 -- Retrieve raw staff assignment records filtered by end date and specific purpose codes.
 assigned_staff_raw AS (
 SELECT
 CASE
 WHEN psm.household_id IS NOT NULL THEN psm.household_id
 WHEN ps.emplid <> '' THEN ps.emplid
 ELSE ps.ext_org_id
 END AS household_id,
 ps.staff_id,
 ps.purpose_cd
 FROM src.crm_staff_assignments ps
 LEFT JOIN dw.PERSON_SUMMARY psm
 ON ps.emplid = psm.emplid
 WHERE ps.end_dt IS NULL
 AND ps.purpose_cd IN ('MAJ','MGP','MID','PG', 'GEN')
 ),

 -- Deduplicate the staff assignments: select one record per household.
 assigned_staff AS (
 SELECT household_id, staff_id, purpose_cd
 FROM (
 SELECT
 household_id,
 staff_id,
 purpose_cd,
 ROW_NUMBER() OVER (PARTITION BY household_id ORDER BY purpose_cd ASC) AS rn
 FROM assigned_staff_raw
 ) t
 WHERE rn = 1
 )
 
-- Select the household identifier along with its corresponding staff details.
SELECT
 household_id,
 staff_id,
 purpose_cd
FROM assigned_staff
ORDER BY household_id;
COMMIT;
