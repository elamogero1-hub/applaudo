-- Register or update the zuora_tickets configuration in control_panel (single row per job_id).
MERGE `gsw-analytics-sandbox.app_integration_zuora.control_panel` t
USING (
 WITH mappings AS (
  SELECT * FROM UNNEST([
   STRUCT('Line_Type' AS source_column, 'Line_Type' AS output_column, 'STRING' AS target_type, 1 AS position),
   STRUCT('Source_System' AS source_column, 'Source_System' AS output_column, 'STRING' AS target_type, 2 AS position),
   STRUCT('Customer_Name' AS source_column, 'Customer_Name' AS output_column, 'STRING' AS target_type, 3 AS position),
   STRUCT('Customer_Number' AS source_column, 'Customer_Number' AS output_column, 'INT64' AS target_type, 4 AS position),
   STRUCT('EventID' AS source_column, 'EventID' AS output_column, 'INT64' AS target_type, 5 AS position),
   STRUCT('Price_Code_Group' AS source_column, 'Price_Code_Group' AS output_column, 'STRING' AS target_type, 6 AS position),
   STRUCT('Parent_Price_Code' AS source_column, 'Parent_Price_Code' AS output_column, 'STRING' AS target_type, 7 AS position),
   STRUCT('EventDate' AS source_column, 'EventDate' AS output_column, 'DATE' AS target_type, 8 AS position),
   STRUCT('Full_Price_code' AS source_column, 'Full_Price_code' AS output_column, 'STRING' AS target_type, 9 AS position),
   STRUCT('Event_Name_Long' AS source_column, 'Event_Name_Long' AS output_column, 'STRING' AS target_type, 10 AS position),
   STRUCT('Subsidiary' AS source_column, 'Subsidiary' AS output_column, 'STRING' AS target_type, 11 AS position),
   STRUCT('SubsidiaryInternalID' AS source_column, 'SubsidiaryInternalID' AS output_column, 'INT64' AS target_type, 12 AS position),
   STRUCT('Sales_Order_Num' AS source_column, 'Sales_Order_Num' AS output_column, 'STRING' AS target_type, 13 AS position),
   STRUCT('Sales_Order_Line_Num' AS source_column, 'Sales_Order_Line_Num' AS output_column, 'STRING' AS target_type, 14 AS position),
   STRUCT('Sales_Order_Date' AS source_column, 'Sales_Order_Date' AS output_column, 'DATE' AS target_type, 15 AS position),
   STRUCT('SalesOrderLineID' AS source_column, 'SalesOrderLineID' AS output_column, 'STRING' AS target_type, 16 AS position),
   STRUCT('INV_NUM' AS source_column, 'INV_NUM' AS output_column, 'STRING' AS target_type, 17 AS position),
   STRUCT('INV_LINE_NUM' AS source_column, 'INV_LINE_NUM' AS output_column, 'STRING' AS target_type, 18 AS position),
   STRUCT('Invoice_Line_ID' AS source_column, 'Invoice_Line_ID' AS output_column, 'STRING' AS target_type, 19 AS position),
   STRUCT('Invoice_Date' AS source_column, 'Invoice_Date' AS output_column, 'DATE' AS target_type, 20 AS position),
   STRUCT('ORIG_INV_LINE_ID' AS source_column, 'ORIG_INV_LINE_ID' AS output_column, 'STRING' AS target_type, 21 AS position),
   STRUCT('OrderedQty' AS source_column, 'OrderedQty' AS output_column, 'INT64' AS target_type, 22 AS position),
   STRUCT('InvoiceQty' AS source_column, 'InvoiceQty' AS output_column, 'INT64' AS target_type, 23 AS position),
   STRUCT('Revenue_Start_Date' AS source_column, 'Revenue_Start_Date' AS output_column, 'DATE' AS target_type, 24 AS position),
   STRUCT('Revenue_End_Date' AS source_column, 'Revenue_End_Date' AS output_column, 'DATE' AS target_type, 25 AS position),
   STRUCT('Line_Item_Num' AS source_column, 'Line_Item_Num' AS output_column, 'STRING' AS target_type, 26 AS position),
   STRUCT('POBIdentifier' AS source_column, 'POBIdentifier' AS output_column, 'STRING' AS target_type, 27 AS position),
   STRUCT('EventName' AS source_column, 'EventName' AS output_column, 'STRING' AS target_type, 28 AS position),
   STRUCT('Ext_List_Price' AS source_column, 'Ext_List_Price' AS output_column, 'FLOAT64' AS target_type, 29 AS position),
   STRUCT('Ext_Sell_Price' AS source_column, 'Ext_Sell_Price' AS output_column, 'FLOAT64' AS target_type, 30 AS position),
   STRUCT('Transaction_Currency' AS source_column, 'Transaction_Currency' AS output_column, 'STRING' AS target_type, 31 AS position),
   STRUCT('Functional_Currency' AS source_column, 'Functional_Currency' AS output_column, 'STRING' AS target_type, 32 AS position),
   STRUCT('Functional_Ex_Rate' AS source_column, 'Functional_Ex_Rate' AS output_column, 'FLOAT64' AS target_type, 33 AS position),
   STRUCT('Global_Ex_Rate' AS source_column, 'Global_Ex_Rate' AS output_column, 'FLOAT64' AS target_type, 34 AS position),
   STRUCT('DeferredSegments' AS source_column, 'DeferredSegments' AS output_column, 'STRING' AS target_type, 35 AS position),
   STRUCT('RevenueSegments' AS source_column, 'RevenueSegments' AS output_column, 'STRING' AS target_type, 36 AS position),
   STRUCT('CV_ELIGIBLE_FLAG' AS source_column, 'CV_ELIGIBLE_FLAG' AS output_column, 'STRING' AS target_type, 37 AS position),
   STRUCT('FV_ELIGIBLE_FLAG' AS source_column, 'FV_ELIGIBLE_FLAG' AS output_column, 'STRING' AS target_type, 38 AS position),
   STRUCT('Food_and_Beverages' AS source_column, 'Food_and_Beverages' AS output_column, 'STRING' AS target_type, 39 AS position),
   STRUCT('Parking' AS source_column, 'Parking' AS output_column, 'STRING' AS target_type, 40 AS position),
   STRUCT('Facilities_fee' AS source_column, 'Facilities_fee' AS output_column, 'FLOAT64' AS target_type, 41 AS position),
   STRUCT('RightToBillFlag' AS source_column, 'RightToBillFlag' AS output_column, 'STRING' AS target_type, 42 AS position),
   STRUCT('Subscription_Number' AS source_column, 'Subscription_Number' AS output_column, 'STRING' AS target_type, 43 AS position),
   STRUCT('Term' AS source_column, 'Term' AS output_column, 'STRING' AS target_type, 44 AS position),
   STRUCT('Product_Class' AS source_column, 'Product_Class' AS output_column, 'STRING' AS target_type, 45 AS position),
   STRUCT('Product_Category' AS source_column, 'Product_Category' AS output_column, 'STRING' AS target_type, 46 AS position),
   STRUCT('Product_line' AS source_column, 'Product_line' AS output_column, 'STRING' AS target_type, 47 AS position),
   STRUCT('SF_Transportation_Fee' AS source_column, 'SF_Transportation_Fee' AS output_column, 'STRING' AS target_type, 48 AS position),
   STRUCT('Admission_Fee' AS source_column, 'Admission_Fee' AS output_column, 'FLOAT64' AS target_type, 49 AS position),
   STRUCT('Amenities' AS source_column, 'Amenities' AS output_column, 'STRING' AS target_type, 50 AS position),
   STRUCT('Season_start_date' AS source_column, 'Season_start_date' AS output_column, 'DATE' AS target_type, 51 AS position),
   STRUCT('authorization_code' AS source_column, 'authorization_code' AS output_column, 'STRING' AS target_type, 52 AS position)
  ])
 ), source_columns AS (
  SELECT column_name, data_type
  FROM `gsw-analytics-warehouse-prod.dev_zuora.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'ticketmaster_tickets'
 ), schema_metadata AS (
  SELECT
   COUNTIF(c.column_name IS NULL AND LOWER(m.source_column) != 'authorization_code') AS missing_count,
   STRING_AGG(IF(c.column_name IS NULL AND LOWER(m.source_column) != 'authorization_code', m.source_column, NULL), ', ') AS missing_names,
   ARRAY_AGG(STRUCT(
    COALESCE(c.column_name, m.source_column) AS source_column,
    m.output_column AS output_column,
    COALESCE(c.data_type, m.target_type) AS source_data_type,
    m.target_type AS target_type,
    m.position AS position,
    IF(LOWER(m.source_column) = 'authorization_code', c.column_name IS NOT NULL, TRUE) AS required,
    m.source_column != 'Invoice_Line_ID' AS nullable
   ) ORDER BY m.position) AS schema_fields
  FROM mappings m
  LEFT JOIN source_columns c ON LOWER(c.column_name) = LOWER(m.source_column)
 )
 SELECT
  'zuora_tickets' AS job_id,
  1 AS config_version,
  TRUE AS enabled,
  'gsw-analytics-warehouse-prod' AS source_project,
  'dev_zuora' AS source_dataset,
  'ticketmaster_tickets' AS source_object,
  'VIEW' AS source_type,
  'US' AS source_location,
  'gsw-analytics-sandbox.app_integration_zuora.tickets_extract_hist' AS target_table,
  IF(sm.missing_count = 0, TO_JSON(sm.schema_fields),
     ERROR(CONCAT('Source columns missing: ', sm.missing_names))) AS schema_source,
  'Invoice_Line_ID,Ext_Sell_Price' AS dedup_keys,
  'INV,CM' AS file_types,
  'GCS' AS destination_type,
  JSON '{"bucket":"gsw-composer-gh","prefix":"data/output","date_path_format":"%Y/%m/%d","file_name_template":"GSW_TICKETS_{file_type}_{execution_id}.csv"}' AS destination_config,
  'America/Los_Angeles' AS business_timezone,
  FALSE AS send_empty,
  'ALLOW_ADDITIONAL_COLUMNS' AS schema_policy,
  'google_cloud_default' AS gcp_connection_id,
  5000 AS page_size,
  JSON r'{"delimiter":",","encoding":"utf-8","line_terminator":"\n"}' AS file_config
 FROM schema_metadata sm
) s
ON t.job_id = s.job_id AND t.config_version = s.config_version
WHEN MATCHED THEN
 UPDATE SET
  target_table = s.target_table,
  schema_source = s.schema_source,
  dedup_keys = s.dedup_keys,
  file_types = s.file_types,
  destination_config = s.destination_config,
  file_config = s.file_config,
  updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
 INSERT (
  job_id, config_version, enabled, source_project, source_dataset, source_object,
  source_type, source_location, target_table, schema_source, dedup_keys, file_types,
  destination_type, destination_config, business_timezone, send_empty, schema_policy,
  gcp_connection_id, page_size, file_config, created_at, updated_at
 )
 VALUES (
  s.job_id, s.config_version, s.enabled, s.source_project, s.source_dataset, s.source_object,
  s.source_type, s.source_location, s.target_table, s.schema_source, s.dedup_keys, s.file_types,
  s.destination_type, s.destination_config, s.business_timezone, s.send_empty, s.schema_policy,
  s.gcp_connection_id, s.page_size, s.file_config, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
 );
