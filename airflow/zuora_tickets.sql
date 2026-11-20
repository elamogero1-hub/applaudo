-- Airflow and the transfer adapter call this routine with bound parameters.
CALL `{{ project }}.{{ dataset }}.sp_zuora_tickets`(
 @p_step, @p_job_id, @p_execution_id, @p_dag_id, @p_airflow_run_id,
 @p_logical_date, @p_location, @p_file_type, @p_checksum, @p_row_count,
 @p_error_step, @p_error_message
);
