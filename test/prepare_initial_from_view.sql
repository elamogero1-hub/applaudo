-- Optional sample: same stage and history, single configuration row in control_panel.
DECLARE v_limit INT64 DEFAULT 100;
ASSERT v_limit BETWEEN 1 AND 10000 AS 'Invalid sample size';
ASSERT (SELECT COUNT(*) FROM `gsw-analytics-sandbox.test_zuora.control_panel`
 WHERE job_id = 'zuora_tickets' AND enabled) = 1
 AS 'Install control configuration first';
BEGIN TRANSACTION;
DELETE FROM `gsw-analytics-sandbox.test_zuora.control_panel`
WHERE job_id IN ('zuora_tickets_initial_simulation', 'zuora_tickets_initial_simulation_delivery');

INSERT INTO `gsw-analytics-sandbox.test_zuora.control_panel`
SELECT c.* REPLACE(
 'zuora_tickets_initial_simulation' AS job_id,
 v_limit AS sample_rows_per_type,
 JSON '{"bucket":"gsw-composer-gh","prefix":"data/process/test/initial_extract","date_path_format":"%Y/%m/%d","file_name_template":"QA_INITIAL_{file_type}_{execution_id}.csv"}' AS destination_config
)
FROM `gsw-analytics-sandbox.test_zuora.control_panel` c
WHERE job_id = 'zuora_tickets' AND enabled;
COMMIT TRANSACTION;
-- Publishing this sample writes shared history. Do not reset SENT for testing.
