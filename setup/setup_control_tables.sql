-- Run manually in the US location after confirming dataset location.
CREATE TABLE IF NOT EXISTS `gsw-analytics-sandbox.test_zuora.control_panel` (
  job_id STRING NOT NULL, config_version INT64 NOT NULL, enabled BOOL NOT NULL,
  source_project STRING NOT NULL, source_dataset STRING NOT NULL,
  source_object STRING NOT NULL, source_type STRING NOT NULL, source_location STRING NOT NULL,
  target_table STRING NOT NULL,
  schema_source JSON,
  dedup_keys STRING, file_types STRING,
  destination_type STRING NOT NULL, destination_config JSON,
  business_timezone STRING NOT NULL, send_empty BOOL NOT NULL,
  schema_policy STRING NOT NULL,
  gcp_connection_id STRING NOT NULL, page_size INT64 NOT NULL,
  file_config JSON,
  created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP,
  sample_rows_per_type INT64
);

CREATE TABLE IF NOT EXISTS `gsw-analytics-sandbox.test_zuora.control_panel_detail` (
  job_id STRING NOT NULL, execution_id STRING NOT NULL,
  record_type STRING NOT NULL, file_type STRING,
  airflow_dag_id STRING NOT NULL, airflow_run_id STRING NOT NULL,
  config_version INT64 NOT NULL, config_snapshot JSON,
  status STRING NOT NULL, business_date DATE NOT NULL,
  file_name STRING, destination_uri STRING,
  rows_extracted INT64, rows_held INT64, rows_exported INT64,
  checksum STRING, schema_validation_result JSON,
  started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP, updated_at TIMESTAMP,
  error_step STRING, error_message STRING,
  quality_validation_result JSON, source_snapshot_at TIMESTAMP, source_rows INT64
)
PARTITION BY business_date
CLUSTER BY job_id, execution_id;

-- Generic per-execution staging.
CREATE TABLE IF NOT EXISTS `gsw-analytics-sandbox.test_zuora.tickets_extract_stg` (
 job_id STRING NOT NULL, execution_id STRING NOT NULL,
 captured_at TIMESTAMP NOT NULL, capture_date DATE NOT NULL, source_record JSON,
 prepared_record JSON, line_status STRING, sent_run_id INT64, sent_at TIMESTAMP
)
PARTITION BY capture_date
CLUSTER BY job_id, execution_id
OPTIONS(require_partition_filter = TRUE);

-- Whole-batch quarantine: every captured row is retained, not only invalid rows.
CREATE TABLE IF NOT EXISTS `gsw-analytics-sandbox.test_zuora.tickets_extract_reject` (
 job_id STRING NOT NULL, execution_id STRING NOT NULL,
 captured_at TIMESTAMP NOT NULL, capture_date DATE NOT NULL,
 rejected_at TIMESTAMP NOT NULL, reject_scope STRING NOT NULL,
 rejection_code STRING NOT NULL, validation_result JSON, source_record JSON
)
PARTITION BY capture_date
CLUSTER BY job_id, execution_id
OPTIONS(require_partition_filter = TRUE);

-- Resume-safe schema alignment for existing installations.
ALTER TABLE `gsw-analytics-sandbox.test_zuora.control_panel`
 ADD COLUMN IF NOT EXISTS sample_rows_per_type INT64;
ALTER TABLE `gsw-analytics-sandbox.test_zuora.control_panel_detail`
 ADD COLUMN IF NOT EXISTS quality_validation_result JSON,
 ADD COLUMN IF NOT EXISTS source_snapshot_at TIMESTAMP,
 ADD COLUMN IF NOT EXISTS source_rows INT64;
ALTER TABLE `gsw-analytics-sandbox.test_zuora.tickets_extract_stg`
 ADD COLUMN IF NOT EXISTS prepared_record JSON,
 ADD COLUMN IF NOT EXISTS line_status STRING,
 ADD COLUMN IF NOT EXISTS sent_run_id INT64,
 ADD COLUMN IF NOT EXISTS sent_at TIMESTAMP;

-- Remove obsolete columns and monitoring view if present from prior versions.
ALTER TABLE `gsw-analytics-sandbox.test_zuora.control_panel`
 DROP COLUMN IF EXISTS schema_error_action,
 DROP COLUMN IF EXISTS data_error_action,
 DROP COLUMN IF EXISTS process_type,
 DROP COLUMN IF EXISTS delivery_job_id;

DROP VIEW IF EXISTS `gsw-analytics-sandbox.test_zuora.vw_zuora_job_status`;
