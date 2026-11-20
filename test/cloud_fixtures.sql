-- Synthetic fixtures only. Each test session uses new, uniquely named objects.
CREATE TABLE `{{ project }}.{{ dataset }}.{{ base }}` AS
SELECT 'INV' AS Line_Type, 'qa_inv_1' AS Invoice_Line_ID, 10.0 AS Ext_Sell_Price,
 'Golden State Warriors, LLC' AS Subsidiary, DATE '2020-10-01' AS Season_start_date,
 '100' AS Customer_Number
UNION ALL SELECT 'CM', 'qa_cm_1', -5.0, 'Golden State Warriors, LLC', DATE '2020-10-01', '101';

{% if scenario == 'sample_limit' %}
INSERT INTO `{{ project }}.{{ dataset }}.{{ base }}`
SELECT 'INV', 'qa_inv_2', 20.0, 'Golden State Warriors, LLC', DATE '2020-10-01', '102'
UNION ALL SELECT 'INV', 'qa_inv_3', 30.0, 'Golden State Warriors, LLC', DATE '2020-10-01', '103'
UNION ALL SELECT 'CM', 'qa_cm_2', -10.0, 'Golden State Warriors, LLC', DATE '2020-10-01', '104'
UNION ALL SELECT 'CM', 'qa_cm_3', -15.0, 'Golden State Warriors, LLC', DATE '2020-10-01', '105';
{% endif %}

{% if scenario == 'empty' %}
DELETE FROM `{{ project }}.{{ dataset }}.{{ base }}` WHERE TRUE;
{% elif scenario == 'bad_value' %}
UPDATE `{{ project }}.{{ dataset }}.{{ base }}` SET Customer_Number = 'not_a_number' WHERE Line_Type = 'INV';
{% elif scenario == 'required_null' %}
UPDATE `{{ project }}.{{ dataset }}.{{ base }}` SET Invoice_Line_ID = NULL WHERE Line_Type = 'INV';
{% elif scenario == 'duplicate_key' %}
INSERT INTO `{{ project }}.{{ dataset }}.{{ base }}`
SELECT Line_Type, Invoice_Line_ID, Ext_Sell_Price, Subsidiary, Season_start_date, '999'
FROM `{{ project }}.{{ dataset }}.{{ base }}` WHERE Line_Type = 'INV';
{% endif %}

CREATE VIEW `{{ project }}.{{ dataset }}.{{ source }}` AS
SELECT
{% if scenario == 'missing_column' %}
 * EXCEPT(Invoice_Line_ID)
{% elif scenario == 'changed_type' %}
 * REPLACE(CAST(Customer_Number AS INT64) AS Customer_Number)
{% elif scenario in ['extra_column','strict_extra'] %}
 *, 'new value' AS Additional_Column
{% else %}
 *
{% endif %}
FROM `{{ project }}.{{ dataset }}.{{ base }}`;

CREATE TABLE `{{ project }}.{{ dataset }}.{{ target }}` (
 run_id INT64, line_type STRING, invoice_line_id STRING, ext_sell_price FLOAT64,
 subsidiary STRING, season_start_date DATE, customer_number INT64,
 line_status STRING, sent_run_id INT64, sent_at TIMESTAMP,
 job_id STRING, execution_id STRING, file_type STRING, file_name STRING, destination_uri STRING, checksum STRING
);

INSERT INTO `{{ project }}.{{ dataset }}.control_panel`
(job_id, config_version, enabled, source_project, source_dataset, source_object,
 source_type, source_location, target_table, schema_source, dedup_keys, file_types,
 destination_type, destination_config, business_timezone, send_empty, schema_policy,
 gcp_connection_id, page_size, file_config, created_at, updated_at,
 sample_rows_per_type)
VALUES (@job_id, 1, TRUE, @project, @dataset, @source, 'VIEW', @location, @target_table,
 TO_JSON([STRUCT('Line_Type' AS source_column,'Line_Type' AS output_column,'STRING' AS source_data_type,
    'STRING' AS target_type,1 AS position,TRUE AS required,FALSE AS nullable),
  ('Invoice_Line_ID','Invoice_Line_ID','STRING','STRING',2,TRUE,FALSE),
  ('Ext_Sell_Price','Ext_Sell_Price','FLOAT64','FLOAT64',3,TRUE,FALSE),
  ('Subsidiary','Subsidiary','STRING','STRING',4,TRUE,FALSE),
  ('Season_start_date','Season_start_date','DATE','DATE',5,TRUE,FALSE),
  ('Customer_Number','Customer_Number','STRING','INT64',6,TRUE,FALSE)]),
 'Invoice_Line_ID,Ext_Sell_Price', 'INV,CM', 'GCS', PARSE_JSON(@destination_json),
 'America/Los_Angeles', FALSE, @schema_policy, 'google_cloud_default', 100,
 JSON r'{"delimiter":",","encoding":"utf-8","line_terminator":"\n"}',
 CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
 {% if scenario == 'sample_limit' %}1{% else %}CAST(NULL AS INT64){% endif %});
