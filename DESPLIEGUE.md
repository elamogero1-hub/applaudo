# Despliegue y Configuración Multi-Fuente

## 1. Archivos SQL a ejecutar en BigQuery

En el dataset `gsw-analytics-sandbox.test_zuora` (ubicación `US`), ejecutar en orden:

1. `setup/setup_control_tables.sql`: crea o alinea `control_panel`, `control_panel_detail`, `tickets_extract_stg` y `tickets_extract_reject`, y elimina columnas obsoletas y la vista redundante `vw_zuora_job_status`.
2. `setup/seed_control_panel.sql`: registra o actualiza la configuración de `zuora_tickets` en `control_panel` y limpia filas `_delivery` duplicadas de versiones previas.
3. `setup/create_tickets_extract.sql`: crea la tabla histórica `tickets_extract_hist` con columnas de trazabilidad de publicación.
4. `setup/deploy_zuora_procedure.sql`: despliega `sp_zuora_tickets` optimizado y preparado para múltiples fuentes.

## 2. Verificar fuentes configuradas en `control_panel`

```sql
SELECT job_id, config_version, enabled, source_dataset, source_object,
       target_table, dedup_keys, file_types, destination_config
FROM `gsw-analytics-sandbox.test_zuora.control_panel`
ORDER BY job_id, config_version;
```

## 3. Cloud Composer (Airflow)

1. Confirmar en **Admin → Variables** la variable JSON `tickets_bootstrap` (ver `setup/tickets_bootstrap.json`):
   ```json
   {
     "project": "gsw-analytics-sandbox",
     "dataset": "test_zuora",
     "table": "control_panel",
     "location": "US",
     "connection_id": "google_cloud_default"
   }
   ```
2. Subir a `gs://gsw-composer-gh/dags/`:
   - `airflow/zuora_tickets.sql`
   - `airflow/dags_tickets_test.py`

## 4. Modos de Ejecución en Airflow

El DAG `dags_tickets_test` (`prepare_batches >> publish_files`) soporta tres modos al hacer **Trigger DAG w/ config**:

1. **Ejecutar todas las fuentes habilitadas (`enabled = TRUE`) en `control_panel`** (por defecto):
   ```json
   {}
   ```
2. **Ejecutar una sola fuente específica**:
   ```json
   {"job_id": "zuora_tickets"}
   ```
3. **Ejecutar una lista de fuentes específicas**:
   ```json
   {"job_ids": ["zuora_tickets", "otra_fuente"]}
   ```

## 5. Cómo agregar una nueva vista fuente

Para incorporar una nueva vista sin modificar el DAG ni el Stored Procedure:

1. Crear su tabla histórica destino (`target_table`) en BigQuery con las columnas de la vista en minúsculas más las columnas operativas (`run_id`, `line_status`, `sent_run_id`, `sent_at`, `job_id`, `execution_id`, `file_type`, `file_name`, `destination_uri`, `checksum`), tomando como plantilla `setup/create_tickets_extract.sql`.
2. Insertar una fila en `control_panel` con el nuevo `job_id`, `enabled = TRUE`, `source_object`, `target_table`, `schema_source`, `dedup_keys`, `file_types` y `destination_config`, tomando como plantilla `setup/seed_control_panel.sql`.

## 6. Monitoreo en `control_panel_detail`

```sql
SELECT job_id, execution_id, record_type, file_type, status,
       source_rows, rows_extracted, rows_held, rows_exported,
       destination_uri, error_step, error_message, started_at, finished_at
FROM `gsw-analytics-sandbox.test_zuora.control_panel_detail`
ORDER BY started_at DESC, job_id, record_type DESC;
```
