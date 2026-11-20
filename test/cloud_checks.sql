-- name: audit
SELECT status, error_step, error_message, schema_validation_result, quality_validation_result
FROM `{{ project }}.{{ dataset }}.control_panel_detail`
WHERE job_id = @job_id AND execution_id = @execution_id AND record_type = 'RUN';

-- name: verify
DECLARE v_capture_at TIMESTAMP;
DECLARE v_source_rows INT64;
SET (v_capture_at, v_source_rows) = (SELECT AS STRUCT source_snapshot_at, source_rows
 FROM `{{ project }}.{{ dataset }}.control_panel_detail`
 WHERE job_id = @job_id AND execution_id = @execution_id AND record_type = 'RUN');
ASSERT (SELECT COUNT(*) FROM `{{ project }}.{{ dataset }}.control_panel_detail`
 WHERE job_id = @job_id AND execution_id = @execution_id AND record_type = 'RUN'
 AND status = @expected_status) = 1 AS 'Unexpected execution status';
IF @expected_status = 'REJECTED' THEN
 ASSERT (SELECT COUNT(*) FROM `{{ project }}.{{ dataset }}.{{ target }}`
 WHERE run_id = CAST(@execution_id AS INT64)) = 0
 AS 'Rejected execution inserted operational rows';
 ASSERT (SELECT COUNT(*) FROM `{{ project }}.{{ dataset }}.control_panel_detail`
 WHERE job_id = @job_id AND execution_id = @execution_id AND record_type = 'FILE') = 0
 AS 'Rejected execution generated a file manifest';
ELSE
 ASSERT (SELECT COUNT(*) FROM `{{ project }}.{{ dataset }}.control_panel_detail`
 WHERE job_id = @job_id AND execution_id = @execution_id AND record_type = 'FILE'
 AND status IN ('SENT','SKIPPED')) = 2 AS 'Expected two completed manifests';
 ASSERT NOT EXISTS (SELECT 1 FROM `{{ project }}.{{ dataset }}.{{ target }}`
 WHERE line_status != 'SENT') AS 'Unexpected operational status after delivery';
 ASSERT (SELECT COUNT(*) FROM `{{ project }}.{{ dataset }}.{{ target }}`)
 = @expected_rows AS 'Unexpected operational row count';
END IF;

-- Every retained snapshot, including a rejected data batch, must match its audit count.
IF v_capture_at IS NOT NULL THEN
 ASSERT (SELECT COUNT(*) FROM `{{ project }}.{{ dataset }}.tickets_extract_stg`
  WHERE capture_date = DATE(v_capture_at) AND job_id = @job_id AND execution_id = @execution_id) = v_source_rows
 AS 'Staging audit count differs from retained source rows';
END IF;

-- Quality failures quarantine the complete snapshot, including otherwise valid rows.
IF @expected_status = 'REJECTED' AND v_capture_at IS NOT NULL THEN
 ASSERT (SELECT COUNT(*) FROM `{{ project }}.{{ dataset }}.tickets_extract_reject`
  WHERE capture_date = DATE(v_capture_at) AND job_id = @job_id AND execution_id = @execution_id) = v_source_rows
 AS 'Rejected batch is not fully retained in quarantine';
END IF;

IF @expected_status != 'REJECTED' AND v_capture_at IS NOT NULL THEN
 ASSERT (SELECT COUNT(*) FROM `{{ project }}.{{ dataset }}.tickets_extract_reject`
  WHERE capture_date = DATE(v_capture_at) AND job_id = @job_id AND execution_id = @execution_id) = 0
 AS 'Valid execution unexpectedly quarantined rows';
END IF;

-- name: before_delivery
ASSERT (SELECT COUNT(*) FROM `{{ project }}.{{ dataset }}.{{ target }}`
 WHERE run_id = CAST(@execution_id AS INT64)) = 0
 AS 'Extraction wrote history before publication';
