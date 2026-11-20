-- Run after triggering the DAG with job_id=zuora_tickets_initial_simulation.
SELECT run_id, line_type, line_status, sent_run_id, COUNT(*) AS rows_count
FROM `gsw-analytics-sandbox.test_zuora.tickets_extract_hist`
GROUP BY run_id, line_type, line_status, sent_run_id;

SELECT execution_id, record_type, file_type, status, rows_extracted, rows_held,
 source_snapshot_at, source_rows, rows_exported, error_step, error_message, destination_uri
FROM `gsw-analytics-sandbox.test_zuora.control_panel_detail`
WHERE job_id = 'zuora_tickets_initial_simulation'
ORDER BY started_at DESC;
