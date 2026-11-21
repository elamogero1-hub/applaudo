-- Deploy after 01_setup_control_tables.sql.
-- Decoupled into 3 single-responsibility procedures (5 parameters each):
--   1. sp_prepare_extraction: freezes config, validates schema/quality, captures to stage, deduplicates, prepares file manifests and returns export plans.
--   2. sp_finalize_delivery: commits delivered CSV rows to history, marks stage/manifest SENT, and closes RUN as DONE when all files complete.
--   3. sp_fail_execution: records FAILED status and error details on RUN/FILE in control_panel_detail.

-- ============================================================================
-- 1. sp_prepare_extraction (5 parameters)
-- ============================================================================
CREATE OR REPLACE PROCEDURE `gsw-analytics-sandbox.app_integration_zuora.sp_prepare_extraction`(
 p_job_id STRING,
 p_execution_id STRING,
 p_dag_id STRING,
 p_airflow_run_id STRING,
 p_logical_date STRING)
OPTIONS(strict_mode = FALSE)
BEGIN
DECLARE v_cfg JSON;
DECLARE v_date DATE;
DECLARE v_status STRING;
DECLARE v_schema JSON;
DECLARE v_quality JSON;
DECLARE v_snapshot_at TIMESTAMP;
DECLARE v_source_rows INT64;
DECLARE v_sample_rows INT64;
DECLARE v_source_dataset STRING;
DECLARE v_target_dataset STRING;
DECLARE v_source STRING;
DECLARE v_target STRING;
DECLARE v_stage STRING DEFAULT 'gsw-analytics-sandbox.app_integration_zuora.tickets_extract_stg';
DECLARE v_batch INT64 DEFAULT SAFE_CAST(p_execution_id AS INT64);
DECLARE v_keys ARRAY<STRING>;
DECLARE v_types ARRAY<STRING>;
DECLARE v_line_col STRING;
DECLARE v_has_line_type BOOL DEFAULT FALSE;
DECLARE v_has_hold BOOL DEFAULT FALSE;
DECLARE v_type_expr STRING;
DECLARE v_hold_expr STRING;
DECLARE v_key_order STRING;
DECLARE v_scope STRING;
DECLARE v_parse STRING;
DECLARE v_work STRING;
DECLARE v_sql STRING;
DECLARE v_fields STRING;
DECLARE v_invalid STRING;
DECLARE v_nulls STRING;
DECLARE v_key_list STRING;
DECLARE v_match STRING;
DECLARE v_capture_fields STRING;
DECLARE v_count INT64;
DECLARE v_hold INT64;
DECLARE v_invalid_count INT64;
DECLARE v_null_count INT64;
DECLARE v_type_count INT64;
DECLARE v_duplicate_count INT64;

-- Normalize placeholder job_id ('{}', 'ALL', '*', '') to the active job_id in control_panel.
IF UPPER(TRIM(COALESCE(p_job_id, ''))) IN ('', 'ALL', '{}', '*') THEN
 SET p_job_id = (
  SELECT job_id FROM `gsw-analytics-sandbox.app_integration_zuora.control_panel`
  WHERE enabled ORDER BY config_version DESC, updated_at DESC LIMIT 1
 );
END IF;

-- Load or initialize the frozen RUN snapshot in control_panel_detail.
SET (v_cfg, v_date, v_status, v_schema, v_snapshot_at, v_source_rows) = (
 SELECT AS STRUCT config_snapshot, business_date, status, schema_validation_result, source_snapshot_at, source_rows
 FROM `gsw-analytics-sandbox.app_integration_zuora.control_panel_detail`
 WHERE job_id = p_job_id AND execution_id = p_execution_id AND record_type = 'RUN'
 LIMIT 1
);

IF v_cfg IS NULL THEN
 SET v_cfg = (
  SELECT TO_JSON(c) FROM `gsw-analytics-sandbox.app_integration_zuora.control_panel` c
  WHERE enabled AND LOWER(TRIM(job_id)) = LOWER(TRIM(p_job_id))
  ORDER BY config_version DESC, updated_at DESC LIMIT 1
 );
 IF v_cfg IS NULL THEN
  RAISE USING MESSAGE = FORMAT(
   'No enabled configuration in app_integration_zuora.control_panel for job_id="%s" (enabled job_ids: %s).',
   COALESCE(p_job_id, 'NULL'),
   COALESCE((SELECT STRING_AGG(job_id, ', ') FROM `gsw-analytics-sandbox.app_integration_zuora.control_panel` WHERE enabled), 'NONE')
  );
 END IF;
 SET p_job_id = COALESCE(JSON_VALUE(v_cfg, '$.job_id'), p_job_id);
 SET v_date = DATE(TIMESTAMP(p_logical_date), JSON_VALUE(v_cfg, '$.business_timezone'));
 INSERT INTO `gsw-analytics-sandbox.app_integration_zuora.control_panel_detail`
  (job_id, execution_id, record_type, airflow_dag_id, airflow_run_id,
   config_version, config_snapshot, status, business_date, started_at, updated_at)
 VALUES (p_job_id, p_execution_id, 'RUN', p_dag_id, p_airflow_run_id,
  CAST(JSON_VALUE(v_cfg, '$.config_version') AS INT64), v_cfg, 'RUNNING', v_date,
  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
 SET v_status = 'RUNNING';
END IF;

SET v_sample_rows = SAFE_CAST(JSON_VALUE(v_cfg, '$.sample_rows_per_type') AS INT64);
SET v_source_dataset = CONCAT(JSON_VALUE(v_cfg, '$.source_project'), '.', JSON_VALUE(v_cfg, '$.source_dataset'));
SET v_source = CONCAT(v_source_dataset, '.', JSON_VALUE(v_cfg, '$.source_object'));
SET v_target = JSON_VALUE(v_cfg, '$.target_table');
SET v_target_dataset = CONCAT(SPLIT(v_target, '.')[SAFE_OFFSET(0)], '.', SPLIT(v_target, '.')[SAFE_OFFSET(1)]);
SET v_keys = ARRAY(SELECT TRIM(k) FROM UNNEST(SPLIT(JSON_VALUE(v_cfg, '$.dedup_keys'), ',')) k WITH OFFSET n WHERE TRIM(k) != '' ORDER BY n);
SET v_types = ARRAY(SELECT TRIM(k) FROM UNNEST(SPLIT(JSON_VALUE(v_cfg, '$.file_types'), ',')) k WITH OFFSET n WHERE TRIM(k) != '' ORDER BY n);

CREATE OR REPLACE TEMP TABLE cols AS
SELECT JSON_VALUE(c, '$.source_column') AS source_column,
 JSON_VALUE(c, '$.output_column') AS output_column,
 UPPER(JSON_VALUE(c, '$.source_data_type')) AS source_data_type,
 UPPER(JSON_VALUE(c, '$.target_type')) AS target_type,
 SAFE_CAST(JSON_VALUE(c, '$.position') AS INT64) AS position,
 SAFE_CAST(JSON_VALUE(c, '$.required') AS BOOL) AS required,
 SAFE_CAST(JSON_VALUE(c, '$.nullable') AS BOOL) AS nullable
FROM UNNEST(IFNULL(JSON_QUERY_ARRAY(v_cfg, '$.schema_source'), ARRAY<JSON>[])) c;

SET v_line_col = (SELECT source_column FROM cols WHERE LOWER(source_column) = 'line_type' LIMIT 1);
SET v_has_line_type = (v_line_col IS NOT NULL);
SET v_has_hold = ((SELECT COUNT(DISTINCT LOWER(source_column)) FROM cols WHERE LOWER(source_column) IN ('season_start_date', 'subsidiary')) = 2);
SET v_type_expr = IF(v_has_line_type, 'line_type', FORMAT('%T', v_types[SAFE_OFFSET(0)]));

-- 1a. Validate source and target schemas against INFORMATION_SCHEMA.
IF v_status NOT IN ('REJECTED', 'DONE') AND v_schema IS NULL THEN
 IF v_snapshot_at IS NULL THEN
  EXECUTE IMMEDIATE FORMAT('''
   CREATE OR REPLACE TEMP TABLE source_columns AS
   SELECT c.column_name, c.data_type
   FROM `%s.INFORMATION_SCHEMA.TABLES` t
   JOIN `%s.INFORMATION_SCHEMA.COLUMNS` c USING (table_name)
   WHERE t.table_name = @name AND t.table_type = @kind
  ''', v_source_dataset, v_source_dataset)
  USING JSON_VALUE(v_cfg, '$.source_object') AS name,
   IF(JSON_VALUE(v_cfg, '$.source_type') = 'TABLE', 'BASE TABLE', 'VIEW') AS kind;
 ELSE
  CREATE OR REPLACE TEMP TABLE source_columns AS
  SELECT source_column AS column_name, source_data_type AS data_type FROM cols;
 END IF;

 EXECUTE IMMEDIATE FORMAT('CREATE OR REPLACE TEMP TABLE target_columns AS SELECT column_name, data_type, is_nullable FROM `%s.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = @name', v_target_dataset)
 USING SPLIT(v_target, '.')[OFFSET(2)] AS name;

 CREATE OR REPLACE TEMP TABLE validation_errors AS
 SELECT message FROM (
  SELECT 'Invalid configuration contract' AS message FROM UNNEST([1]) AS one_row
  WHERE (SELECT COUNT(*) FROM cols) = 0 OR ARRAY_LENGTH(v_keys) = 0 OR ARRAY_LENGTH(v_types) = 0
     OR JSON_VALUE(v_cfg, '$.destination_type') NOT IN ('GCS', 'SFTP')
  UNION ALL
  SELECT 'Source object missing or source_type mismatch' FROM UNNEST([1]) AS one_row WHERE (SELECT COUNT(*) FROM source_columns) = 0
  UNION ALL
  SELECT 'Target object missing or not a table' FROM UNNEST([1]) AS one_row WHERE (SELECT COUNT(*) FROM target_columns) = 0
  UNION ALL
  SELECT CONCAT('Missing or incompatible source field: ', c.source_column)
  FROM cols c LEFT JOIN source_columns s ON LOWER(c.source_column) = LOWER(s.column_name)
  WHERE (s.column_name IS NULL AND c.required)
     OR (s.column_name IS NOT NULL AND s.data_type != c.source_data_type)
  UNION ALL
  SELECT CONCAT('Missing or incompatible target field: ', c.source_column)
  FROM cols c LEFT JOIN target_columns t ON LOWER(c.source_column) = LOWER(t.column_name)
  WHERE t.column_name IS NULL OR t.data_type != c.target_type
  UNION ALL
  SELECT CONCAT('Unexpected source field under STRICT: ', s.column_name)
  FROM source_columns s LEFT JOIN cols c ON LOWER(c.source_column) = LOWER(s.column_name)
  WHERE c.source_column IS NULL AND JSON_VALUE(v_cfg, '$.schema_policy') = 'STRICT'
  UNION ALL
  SELECT CONCAT('Incompatible operational metadata: ', r.name)
  FROM UNNEST([STRUCT('run_id' AS name,'INT64' AS kind),('line_status','STRING'),('sent_run_id','INT64'),('sent_at','TIMESTAMP'),('job_id','STRING'),('execution_id','STRING'),('file_type','STRING'),('file_name','STRING'),('destination_uri','STRING'),('checksum','STRING')]) r
  LEFT JOIN target_columns t ON LOWER(t.column_name) = r.name
  WHERE t.column_name IS NULL OR t.data_type != r.kind
 );

 SET v_schema = JSON_OBJECT(
  'errors', (SELECT IFNULL(ARRAY_AGG(message), ARRAY<STRING>[]) FROM validation_errors),
  'missing_columns', (SELECT IFNULL(ARRAY_AGG(c.source_column), ARRAY<STRING>[]) FROM cols c
   LEFT JOIN source_columns s ON LOWER(c.source_column) = LOWER(s.column_name) WHERE s.column_name IS NULL),
  'additional_columns', (SELECT IFNULL(ARRAY_AGG(s.column_name), ARRAY<STRING>[]) FROM source_columns s
   LEFT JOIN cols c ON LOWER(c.source_column) = LOWER(s.column_name) WHERE c.source_column IS NULL)
 );

 IF EXISTS (SELECT 1 FROM validation_errors) THEN
  UPDATE `gsw-analytics-sandbox.app_integration_zuora.control_panel_detail`
  SET schema_validation_result = v_schema, status = 'REJECTED', error_step = 'schema_validation',
   error_message = (SELECT STRING_AGG(message, '; ') FROM validation_errors),
   finished_at = CURRENT_TIMESTAMP(), updated_at = CURRENT_TIMESTAMP()
  WHERE job_id = p_job_id AND execution_id = p_execution_id AND record_type = 'RUN';
  SET v_status = 'REJECTED';
 ELSE
  UPDATE `gsw-analytics-sandbox.app_integration_zuora.control_panel_detail`
  SET schema_validation_result = v_schema, updated_at = CURRENT_TIMESTAMP()
  WHERE job_id = p_job_id AND execution_id = p_execution_id AND record_type = 'RUN';
 END IF;
END IF;

-- 1b. Capture source snapshot into tickets_extract_stg once per execution.
IF v_status NOT IN ('REJECTED', 'DONE') AND v_snapshot_at IS NULL THEN
 SET v_snapshot_at = CURRENT_TIMESTAMP();
 SET v_capture_fields = (SELECT STRING_AGG(FORMAT('%s AS `%s`',
  IF(c.source_column IN UNNEST(IFNULL(JSON_VALUE_ARRAY(v_schema, '$.missing_columns'), ARRAY<STRING>[])),
   'NULL', FORMAT('f.`%s`', c.source_column)), LOWER(c.source_column)), ', ' ORDER BY position) FROM cols c);
 SET v_key_order = (SELECT STRING_AGG(FORMAT('f.`%s`', c.source_column), ', ' ORDER BY n)
  FROM UNNEST(v_keys) k WITH OFFSET n JOIN cols c ON LOWER(c.source_column) = LOWER(k));
 SET v_sql = FORMAT('CREATE OR REPLACE TEMP TABLE capture_rows AS SELECT TO_JSON(STRUCT(%s), stringify_wide_numbers => TRUE) AS source_record FROM `%s` f%s',
  v_capture_fields, v_source,
  IF(v_sample_rows IS NULL, '',
   IF(v_has_line_type,
    FORMAT(' WHERE f.`%s` IN UNNEST(@types) QUALIFY ROW_NUMBER() OVER (PARTITION BY f.`%s` ORDER BY %s) <= @sample', v_line_col, v_line_col, v_key_order),
    FORMAT(' WHERE ARRAY_LENGTH(@types) > 0 QUALIFY ROW_NUMBER() OVER (ORDER BY %s) <= @sample', v_key_order))));
 IF v_sample_rows IS NULL THEN
  EXECUTE IMMEDIATE v_sql;
 ELSE
  EXECUTE IMMEDIATE v_sql USING v_types AS types, v_sample_rows AS sample;
 END IF;
 SET v_source_rows = (SELECT COUNT(*) FROM capture_rows);
 BEGIN TRANSACTION;
 INSERT INTO `gsw-analytics-sandbox.app_integration_zuora.tickets_extract_stg`
  (job_id, execution_id, captured_at, capture_date, source_record)
 SELECT p_job_id, p_execution_id, v_snapshot_at, DATE(v_snapshot_at), source_record FROM capture_rows;
 UPDATE `gsw-analytics-sandbox.app_integration_zuora.control_panel_detail`
 SET source_snapshot_at = v_snapshot_at, source_rows = v_source_rows, updated_at = CURRENT_TIMESTAMP()
 WHERE job_id = p_job_id AND execution_id = p_execution_id AND record_type = 'RUN';
 COMMIT TRANSACTION;
END IF;

-- 1c. Validate quality, deduplicate against history, update stage and create FILE manifests.
IF v_status NOT IN ('REJECTED', 'DONE')
   AND NOT EXISTS (SELECT 1 FROM `gsw-analytics-sandbox.app_integration_zuora.control_panel_detail`
                   WHERE job_id = p_job_id AND execution_id = p_execution_id AND record_type = 'FILE') THEN
 CREATE OR REPLACE TEMP TABLE cast_expressions AS
 SELECT c.*,
  IF(source_data_type = 'BYTES',
   FORMAT("FROM_BASE64(JSON_VALUE(f.source_record, '$.%s'))", LOWER(source_column)),
   FORMAT("SAFE_CAST(JSON_VALUE(f.source_record, '$.%s') AS %s)", LOWER(source_column), source_data_type)
  ) AS source_expression
 FROM cols c;

 SET v_fields = (SELECT STRING_AGG(FORMAT('SAFE_CAST(%s AS %s) AS `%s`',
  source_expression, target_type, LOWER(source_column)), ', ' ORDER BY position) FROM cast_expressions);
 SET v_invalid = (SELECT STRING_AGG(FORMAT("(JSON_VALUE(f.source_record, '$.%s') IS NOT NULL AND SAFE_CAST(%s AS %s) IS NULL)",
  LOWER(source_column), source_expression, target_type), ' OR ') FROM cast_expressions);

 EXECUTE IMMEDIATE FORMAT('CREATE OR REPLACE TEMP TABLE typed_source AS SELECT DISTINCT %s, (%s) AS __invalid_cast, TO_JSON_STRING(f.source_record) AS __raw FROM `gsw-analytics-sandbox.app_integration_zuora.tickets_extract_stg` f WHERE capture_date = @capture_date AND job_id = @job AND execution_id = @execution',
  v_fields, COALESCE(v_invalid, 'FALSE'))
 USING DATE(v_snapshot_at) AS capture_date, p_job_id AS job, p_execution_id AS execution;

 SET v_nulls = (SELECT STRING_AGG(FORMAT('`%s` IS NULL', LOWER(source_column)), ' OR ') FROM cols WHERE NOT nullable);
 SET v_key_list = (SELECT STRING_AGG(FORMAT('`%s`', LOWER(k)), ', ' ORDER BY n) FROM UNNEST(v_keys) k WITH OFFSET n);
 EXECUTE IMMEDIATE FORMAT('''
  SELECT COUNTIF(__invalid_cast), COUNTIF(%s), COUNTIF(%s IS NULL OR %s NOT IN UNNEST(@types)),
   (SELECT COUNT(*) FROM (SELECT 1 FROM typed_source GROUP BY %s HAVING COUNT(*) > 1))
  FROM typed_source
 ''', COALESCE(v_nulls, 'FALSE'), v_type_expr, v_type_expr, v_key_list)
 INTO v_invalid_count, v_null_count, v_type_count, v_duplicate_count USING v_types AS types;

 SET v_quality = JSON_OBJECT('invalid_cast_rows', v_invalid_count, 'required_null_rows', v_null_count,
  'invalid_file_type_rows', v_type_count, 'conflicting_keys', v_duplicate_count);

 IF v_invalid_count + v_null_count + v_type_count + v_duplicate_count > 0 THEN
  BEGIN TRANSACTION;
  DELETE FROM `gsw-analytics-sandbox.app_integration_zuora.tickets_extract_reject`
  WHERE capture_date = DATE(v_snapshot_at) AND job_id = p_job_id AND execution_id = p_execution_id;
  INSERT INTO `gsw-analytics-sandbox.app_integration_zuora.tickets_extract_reject`
   (job_id, execution_id, captured_at, capture_date, rejected_at,
    reject_scope, rejection_code, validation_result, source_record)
  SELECT job_id, execution_id, captured_at, capture_date, CURRENT_TIMESTAMP(),
   'BATCH', 'QUALITY_VALIDATION_FAILED', v_quality, source_record
  FROM `gsw-analytics-sandbox.app_integration_zuora.tickets_extract_stg`
  WHERE capture_date = DATE(v_snapshot_at) AND job_id = p_job_id AND execution_id = p_execution_id;
  UPDATE `gsw-analytics-sandbox.app_integration_zuora.control_panel_detail`
  SET status = 'REJECTED', error_step = 'quality_validation', error_message = TO_JSON_STRING(v_quality),
   quality_validation_result = v_quality, finished_at = CURRENT_TIMESTAMP(), updated_at = CURRENT_TIMESTAMP()
  WHERE job_id = p_job_id AND execution_id = p_execution_id AND record_type = 'RUN';
  COMMIT TRANSACTION;
  SET v_status = 'REJECTED';
 ELSE
  ASSERT NOT EXISTS (SELECT 1 FROM `gsw-analytics-sandbox.app_integration_zuora.tickets_extract_stg`
   WHERE capture_date >= DATE '1970-01-01' AND line_status = 'PROCESSING'
    AND job_id = p_job_id AND execution_id != p_execution_id)
  AS 'Recover the prior PROCESSING delivery for this job_id before claiming another extraction';

  CREATE OR REPLACE TEMP TABLE names AS
  SELECT kind, n AS sequence,
   REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(JSON_VALUE(v_cfg, '$.destination_config.file_name_template'),
    '{file_type}', kind), '{execution_id}', p_execution_id), '{job_id}', p_job_id),
    '{run_id}', p_job_id), '{business_date}', CAST(v_date AS STRING)) AS file_name
  FROM UNNEST(v_types) kind WITH OFFSET n;

  SET v_match = (SELECT STRING_AGG(FORMAT('h.`%s` IS NOT DISTINCT FROM s.`%s`', LOWER(k), LOWER(k)), ' AND ') FROM UNNEST(v_keys) k);
  SET v_fields = (SELECT STRING_AGG(FORMAT('s.`%s` AS `%s`', LOWER(source_column), LOWER(source_column)), ', ' ORDER BY position) FROM cols);
  SET v_hold_expr = IF(v_has_hold, '''
    s.season_start_date > CASE
     WHEN s.subsidiary IN ('Golden State Warriors, LLC', 'SC Warriors LLC')
      THEN DATE(EXTRACT(YEAR FROM @day) - IF(EXTRACT(MONTH FROM @day) >= 10, 0, 1), 10, 1)
     WHEN s.subsidiary = 'Golden State Valkyries LLC'
      THEN DATE(EXTRACT(YEAR FROM @day) - IF(EXTRACT(MONTH FROM @day) >= 4, 0, 1), 4, 1)
    END''', 'FALSE');

  EXECUTE IMMEDIATE FORMAT('''
   CREATE OR REPLACE TEMP TABLE prepared_rows AS
   SELECT s.__raw, %s AS file_kind,
    TO_JSON(STRUCT(%s), stringify_wide_numbers => TRUE) AS prepared_record,
    CASE
     WHEN %s THEN 'HOLD'
     WHEN d.__raw IS NOT NULL THEN 'ALREADY_SENT'
     ELSE 'PROCESSING'
    END AS line_status
   FROM typed_source s
   LEFT JOIN (SELECT DISTINCT s.__raw FROM typed_source s JOIN `%s` h ON %s) d
    ON s.__raw = d.__raw
  ''', v_type_expr, v_fields, v_hold_expr, v_target, v_match)
  USING v_date AS day;

  SET (v_count, v_hold) = (SELECT AS STRUCT COUNT(*), COUNTIF(line_status = 'HOLD') FROM prepared_rows);

  BEGIN TRANSACTION;
  UPDATE `gsw-analytics-sandbox.app_integration_zuora.tickets_extract_stg` f
  SET prepared_record = p.prepared_record,
   line_status = p.line_status,
   sent_run_id = IF(p.line_status = 'PROCESSING', v_batch, NULL)
  FROM prepared_rows p
  WHERE f.capture_date = DATE(v_snapshot_at) AND f.job_id = p_job_id AND f.execution_id = p_execution_id
   AND TO_JSON_STRING(f.source_record) = p.__raw
   AND (f.line_status IS NULL OR f.line_status = 'READY');

  UPDATE `gsw-analytics-sandbox.app_integration_zuora.control_panel_detail`
  SET rows_extracted = v_count, rows_held = v_hold, quality_validation_result = v_quality,
   updated_at = CURRENT_TIMESTAMP()
  WHERE job_id = p_job_id AND execution_id = p_execution_id AND record_type = 'RUN';

  INSERT INTO `gsw-analytics-sandbox.app_integration_zuora.control_panel_detail`
   (job_id, execution_id, record_type, file_type, airflow_dag_id, airflow_run_id, config_version,
    config_snapshot, status, business_date, file_name, destination_uri, rows_exported, started_at, updated_at)
  SELECT p_job_id, p_execution_id, 'FILE', f.kind, p_dag_id, p_airflow_run_id,
   CAST(JSON_VALUE(v_cfg, '$.config_version') AS INT64), v_cfg, 'PREPARED', v_date, f.file_name,
   IF(JSON_VALUE(v_cfg, '$.destination_type') = 'SFTP',
    CONCAT('sftp://', COALESCE(NULLIF(JSON_VALUE(v_cfg, '$.destination_config.sftp_connection_id'), ''), JSON_VALUE(v_cfg, '$.gcp_connection_id')), '/',
     TRIM(JSON_VALUE(v_cfg, '$.destination_config.remote_path'), '/'), '/',
     IF(NULLIF(JSON_VALUE(v_cfg, '$.destination_config.date_path_format'), '') IS NULL, '',
      CONCAT(FORMAT_DATE(JSON_VALUE(v_cfg, '$.destination_config.date_path_format'), v_date), '/')),
     f.file_name),
    CONCAT('gs://', JSON_VALUE(v_cfg, '$.destination_config.bucket'), '/',
     IF(TRIM(COALESCE(JSON_VALUE(v_cfg, '$.destination_config.prefix'), ''), '/') = '', '',
      CONCAT(TRIM(JSON_VALUE(v_cfg, '$.destination_config.prefix'), '/'), '/')),
     FORMAT_DATE(JSON_VALUE(v_cfg, '$.destination_config.date_path_format'), v_date), '/', f.file_name)),
   COALESCE(w.cnt, 0), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
  FROM names f
  LEFT JOIN (
   SELECT file_kind, COUNT(*) AS cnt FROM prepared_rows
   WHERE line_status = 'PROCESSING'
   GROUP BY file_kind
  ) w ON w.file_kind = f.kind
  ORDER BY f.sequence;
  COMMIT TRANSACTION;
 END IF;
END IF;

-- 1d. Return the prepared file manifests and export queries directly to Python.
SET v_parse = (SELECT STRING_AGG(FORMAT('%s AS `%s`',
 IF(target_type = 'BYTES', FORMAT("FROM_BASE64(JSON_VALUE(prepared_record, '$.%s'))", LOWER(source_column)),
 FORMAT("SAFE_CAST(JSON_VALUE(prepared_record, '$.%s') AS %s)", LOWER(source_column), target_type)),
 LOWER(source_column)), ', ' ORDER BY position) FROM cols);
SET v_scope = FORMAT('capture_date = DATE(%T) AND job_id = %T AND execution_id = %T',
 v_snapshot_at, p_job_id, p_execution_id);
SET v_work = FORMAT('(SELECT DISTINCT %s, sent_run_id, line_status FROM `%s` WHERE %s)', v_parse, v_stage, v_scope);
SET v_fields = (SELECT STRING_AGG(FORMAT('`%s` AS `%s`', LOWER(source_column), output_column), ', ' ORDER BY position) FROM cols);
SET v_key_list = (SELECT STRING_AGG(FORMAT('`%s`', LOWER(k)), ', ' ORDER BY n) FROM UNNEST(v_keys) k WITH OFFSET n);

SELECT p_job_id AS job_id, p_execution_id AS execution_id, v_status AS run_status,
 d.file_type, REGEXP_EXTRACT(d.file_name, r'[^/]+$') AS file_name, d.destination_uri, d.rows_exported, d.status,
 COALESCE(JSON_VALUE(v_cfg, '$.destination_type'), 'GCS') AS destination_type,
 SAFE_CAST(JSON_VALUE(v_cfg, '$.send_empty') AS BOOL) AS send_empty,
 CAST(JSON_VALUE(v_cfg, '$.page_size') AS INT64) AS page_size,
 JSON_VALUE(v_cfg, '$.file_config.encoding') AS encoding,
 JSON_VALUE(v_cfg, '$.file_config.delimiter') AS delimiter,
 JSON_VALUE(v_cfg, '$.file_config.line_terminator') AS line_terminator,
 COALESCE(JSON_VALUE(v_cfg, '$.gcp_connection_id'), 'google_cloud_default') AS connection_id,
 COALESCE(NULLIF(JSON_VALUE(v_cfg, '$.destination_config.sftp_connection_id'), ''), JSON_VALUE(v_cfg, '$.gcp_connection_id')) AS sftp_connection_id,
 COALESCE(JSON_VALUE(v_cfg, '$.source_location'), 'US') AS location,
 FORMAT('SELECT %s FROM %s WHERE line_status = "PROCESSING" AND sent_run_id = %d AND %s = %T ORDER BY %s',
  v_fields, v_work, v_batch, v_type_expr, d.file_type, v_key_list) AS export_sql
FROM `gsw-analytics-sandbox.app_integration_zuora.control_panel_detail` d
JOIN UNNEST(v_types) kind WITH OFFSET n ON kind = d.file_type
WHERE d.job_id = p_job_id AND d.execution_id = p_execution_id AND d.record_type = 'FILE'
 AND v_status NOT IN ('REJECTED', 'DONE')
ORDER BY n;
END;


-- ============================================================================
-- 2. sp_finalize_delivery (5 parameters)
-- ============================================================================
CREATE OR REPLACE PROCEDURE `gsw-analytics-sandbox.app_integration_zuora.sp_finalize_delivery`(
 p_job_id STRING,
 p_execution_id STRING,
 p_file_type STRING,
 p_checksum STRING,
 p_row_count INT64)
OPTIONS(strict_mode = FALSE)
BEGIN
DECLARE v_cfg JSON;
DECLARE v_status STRING;
DECLARE v_snapshot_at TIMESTAMP;
DECLARE v_target STRING;
DECLARE v_stage STRING DEFAULT 'gsw-analytics-sandbox.app_integration_zuora.tickets_extract_stg';
DECLARE v_batch INT64 DEFAULT SAFE_CAST(p_execution_id AS INT64);
DECLARE v_types ARRAY<STRING>;
DECLARE v_has_line_type BOOL DEFAULT FALSE;
DECLARE v_type_expr STRING;
DECLARE v_stage_type_expr STRING;
DECLARE v_scope STRING;
DECLARE v_parse STRING;
DECLARE v_work STRING;
DECLARE v_names STRING;
DECLARE v_file_status STRING;
DECLARE v_filename STRING;
DECLARE v_uri STRING;
DECLARE v_count INT64;

SET (v_cfg, v_status, v_snapshot_at) = (
 SELECT AS STRUCT config_snapshot, status, source_snapshot_at
 FROM `gsw-analytics-sandbox.app_integration_zuora.control_panel_detail`
 WHERE job_id = p_job_id AND execution_id = p_execution_id AND record_type = 'RUN'
 LIMIT 1
);

IF v_status NOT IN ('REJECTED', 'DONE') THEN
 SET v_target = JSON_VALUE(v_cfg, '$.target_table');
 SET v_types = ARRAY(SELECT TRIM(k) FROM UNNEST(SPLIT(JSON_VALUE(v_cfg, '$.file_types'), ',')) k WITH OFFSET n WHERE TRIM(k) != '' ORDER BY n);

 CREATE OR REPLACE TEMP TABLE cols AS
 SELECT JSON_VALUE(c, '$.source_column') AS source_column,
  UPPER(JSON_VALUE(c, '$.target_type')) AS target_type,
  SAFE_CAST(JSON_VALUE(c, '$.position') AS INT64) AS position
 FROM UNNEST(IFNULL(JSON_QUERY_ARRAY(v_cfg, '$.schema_source'), ARRAY<JSON>[])) c;

 SET v_has_line_type = EXISTS (SELECT 1 FROM cols WHERE LOWER(source_column) = 'line_type');
 SET v_type_expr = IF(v_has_line_type, 'line_type', FORMAT('%T', v_types[SAFE_OFFSET(0)]));
 SET v_stage_type_expr = IF(v_has_line_type, "JSON_VALUE(prepared_record, '$.line_type')", FORMAT('%T', v_types[SAFE_OFFSET(0)]));
 SET v_parse = (SELECT STRING_AGG(FORMAT('%s AS `%s`',
  IF(target_type = 'BYTES', FORMAT("FROM_BASE64(JSON_VALUE(prepared_record, '$.%s'))", LOWER(source_column)),
  FORMAT("SAFE_CAST(JSON_VALUE(prepared_record, '$.%s') AS %s)", LOWER(source_column), target_type)),
  LOWER(source_column)), ', ' ORDER BY position) FROM cols);
 SET v_scope = FORMAT('capture_date = DATE(%T) AND job_id = %T AND execution_id = %T',
  v_snapshot_at, p_job_id, p_execution_id);
 SET v_work = FORMAT('(SELECT DISTINCT %s, sent_run_id, line_status FROM `%s` WHERE %s)', v_parse, v_stage, v_scope);

 SET (v_file_status, v_filename, v_uri, v_count) = (
  SELECT AS STRUCT status, file_name, destination_uri, rows_exported
  FROM `gsw-analytics-sandbox.app_integration_zuora.control_panel_detail`
  WHERE job_id = p_job_id AND execution_id = p_execution_id AND record_type = 'FILE' AND file_type = p_file_type
  LIMIT 1
 );
 ASSERT v_file_status IS NOT NULL AS 'Expected one file manifest';

 IF v_file_status NOT IN ('SENT', 'SKIPPED') THEN
  ASSERT v_count = p_row_count AS 'Manifest row count differs from delivered file';
  SET v_names = (SELECT STRING_AGG(FORMAT('`%s`', LOWER(source_column)), ', ' ORDER BY position) FROM cols);
  BEGIN TRANSACTION;
  EXECUTE IMMEDIATE FORMAT('INSERT INTO `%s` (run_id, %s, line_status, sent_run_id, sent_at, job_id, execution_id, file_type, file_name, destination_uri, checksum) SELECT @batch, %s, "SENT", @batch, CURRENT_TIMESTAMP(), @job, @execution, @kind, @filename, @uri, @checksum FROM %s WHERE line_status = "PROCESSING" AND sent_run_id = @batch AND %s = @kind',
   v_target, v_names, v_names, v_work, v_type_expr) USING v_batch AS batch, p_file_type AS kind,
   p_job_id AS job, p_execution_id AS execution, v_filename AS filename, v_uri AS uri, p_checksum AS checksum;

  EXECUTE IMMEDIATE FORMAT("UPDATE `%s` SET line_status = 'SENT', sent_at = CURRENT_TIMESTAMP() WHERE %s AND line_status = 'PROCESSING' AND sent_run_id = @batch AND %s = @kind",
   v_stage, v_scope, v_stage_type_expr)
  USING v_batch AS batch, p_file_type AS kind;

  UPDATE `gsw-analytics-sandbox.app_integration_zuora.control_panel_detail`
  SET status = IF(p_row_count = 0 AND NOT CAST(JSON_VALUE(v_cfg, '$.send_empty') AS BOOL), 'SKIPPED', 'SENT'),
   checksum = p_checksum, rows_exported = p_row_count, finished_at = CURRENT_TIMESTAMP(),
   updated_at = CURRENT_TIMESTAMP(), error_step = NULL, error_message = NULL
  WHERE job_id = p_job_id AND execution_id = p_execution_id AND record_type = 'FILE' AND file_type = p_file_type;

  IF (SELECT COUNT(*) FROM `gsw-analytics-sandbox.app_integration_zuora.control_panel_detail`
      WHERE job_id = p_job_id AND execution_id = p_execution_id AND record_type = 'FILE'
       AND file_type IN UNNEST(v_types) AND status IN ('SENT', 'SKIPPED')) = ARRAY_LENGTH(v_types) THEN
   UPDATE `gsw-analytics-sandbox.app_integration_zuora.control_panel_detail`
   SET status = 'DONE', finished_at = CURRENT_TIMESTAMP(), updated_at = CURRENT_TIMESTAMP(),
    rows_exported = (SELECT COALESCE(SUM(rows_exported), 0) FROM `gsw-analytics-sandbox.app_integration_zuora.control_panel_detail`
     WHERE job_id = p_job_id AND execution_id = p_execution_id AND record_type = 'FILE' AND status = 'SENT')
   WHERE job_id = p_job_id AND execution_id = p_execution_id AND record_type = 'RUN';
  END IF;
  COMMIT TRANSACTION;
 END IF;
END IF;
END;


-- ============================================================================
-- 3. sp_fail_execution (5 parameters)
-- ============================================================================
CREATE OR REPLACE PROCEDURE `gsw-analytics-sandbox.app_integration_zuora.sp_fail_execution`(
 p_job_id STRING,
 p_execution_id STRING,
 p_file_type STRING,
 p_error_step STRING,
 p_error_message STRING)
OPTIONS(strict_mode = FALSE)
BEGIN
UPDATE `gsw-analytics-sandbox.app_integration_zuora.control_panel_detail`
SET status = 'FAILED', error_step = p_error_step, error_message = p_error_message,
 finished_at = CURRENT_TIMESTAMP(), updated_at = CURRENT_TIMESTAMP()
WHERE job_id = p_job_id AND execution_id = p_execution_id
 AND status NOT IN ('REJECTED', 'DONE', 'SENT', 'SKIPPED')
 AND (record_type = 'RUN' OR (record_type = 'FILE' AND file_type = p_file_type));
END;
