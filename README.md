# Extracción y entrega multi-fuente controlada por BigQuery

Las vistas origen no se modifican. BigQuery controla todo el flujo mediante `control_panel` y `control_panel_detail`, ejecutando validaciones, captura en stage, control de calidad y preparación de lotes en una sola invocación del procedimiento almacenado (`sp_zuora_tickets`). Python únicamente resuelve la identidad de ejecución, lee el bootstrap en tiempo de ejecución y publica los archivos CSV en GCS.

## Arquitectura Multi-Fuente en `control_panel`

| job_id | Origen (`source_object`) | Stage compartido | Histórico propio (`target_table`) |
|---|---|---|---|
| zuora_tickets | `gsw-analytics-warehouse-prod.dev_zuora.ticketmaster_tickets` | `gsw-analytics-sandbox.test_zuora.tickets_extract_stg` | `gsw-analytics-sandbox.test_zuora.tickets_extract_hist` |
| `<nueva_fuente>` | `<proyecto>.<dataset>.<vista_origen>` | `gsw-analytics-sandbox.test_zuora.tickets_extract_stg` | `<proyecto>.<dataset>.<tabla_hist>` |

Cada fuente se define con **una sola fila habilitada** (`enabled = TRUE`) en `control_panel` por `job_id`:
- **Esquema independiente (`schema_source`)**: cada vista puede tener sus propias columnas, tipos de datos y obligatoriedad.
- **Claves de deduplicación propias (`dedup_keys`)**: p. ej. `Invoice_Line_ID,Ext_Sell_Price` u otras columnas clave según la fuente.
- **Tipos de archivo (`file_types`)**: múltiples archivos particionados por `Line_Type` (p. ej. `INV,CM`) o un único archivo por ejecución si la vista no usa `Line_Type`.
- **Stage y Reject compartidos sin conflicto de esquema**: `tickets_extract_stg` y `tickets_extract_reject` almacenan los registros en JSON (`source_record`, `prepared_record`) particionados por `capture_date` y aislados por `job_id, execution_id`.
- **Histórico independiente (`target_table`)**: cada fuente apunta a su propia tabla histórica tipada.

## Flujo optimizado (2 tareas en Airflow)

```text
prepare_batches -> publish_files
```

1. **`prepare_batches`**:
   - Lee la Airflow Variable `tickets_bootstrap` en ejecución (`Variable.get(..., deserialize_json=True)`).
   - **Resolución de fuentes (`resolve_requests`)**:
     - Si se dispara con `{"job_id": "zuora_tickets"}` (o `{"job_ids": ["fuente_1", "fuente_2"]}`), ejecuta únicamente la(s) fuente(s) indicada(s).
     - Si se dispara sin parámetros (`{}` o `{"job_id": "ALL"}`), consulta `CALL sp_zuora_tickets('list_jobs', ...)` y ejecuta **todas las fuentes con `enabled = TRUE`** en `control_panel`.
   - Para cada fuente activa, invoca `CALL sp_zuora_tickets('prepare', ...)` en una sola sesión de BigQuery:
     - Congela la configuración de `control_panel` en `control_panel_detail` (`record_type = 'RUN'`).
     - Valida configuración y esquema origen/destino en una única consulta consolidada contra `INFORMATION_SCHEMA`.
     - Captura el lote de la vista fuente en `tickets_extract_stg` (`source_record` JSON) una sola vez por ejecución.
     - Evalúa conversiones, nulos, tipos de archivo y claves duplicadas. Si hay error de calidad, guarda el lote en `tickets_extract_reject` y marca el `RUN` como `REJECTED`.
     - Si la calidad es válida, deduplica en memoria contra el `target_table` de esa fuente, aplica la regla de `HOLD` (si la fuente incluye `Season_start_date` y `Subsidiary`), actualiza `tickets_extract_stg` en un único `UPDATE` e inserta los manifiestos `FILE` de forma vectorial.
2. **`publish_files`**:
   - Para cada fuente preparada, consulta sus manifiestos `FILE`, exporta cada lote `PROCESSING` a CSV y lo sube a GCS con creación exclusiva (`if_generation_match=0`) y verificación de `sha256` y `row_count`.
   - Llama a `finalize_file`, que en una sola transacción inserta las filas confirmadas en el `target_table` correspondiente (`SENT`), actualiza `tickets_extract_stg` a `SENT`, marca el manifiesto `FILE` como `SENT`/`SKIPPED` y cierra automáticamente el `RUN` en `DONE`.

## Tablas en BigQuery

- `control_panel`: catálogo de fuentes e integraciones (una fila habilitada por `job_id`).
- `control_panel_detail`: auditoría y monitoreo centralizado (`RUN` y `FILE`) por `job_id` y `execution_id`.
- `tickets_extract_stg`: stage multi-fuente en JSON particionado por `capture_date` UTC y agrupado por `job_id, execution_id`.
- `tickets_extract_reject`: cuarentena de calidad multi-fuente por lote completo.
- `tickets_extract_hist` (y tablas históricas de nuevas fuentes): registros publicados con trazabilidad de archivo.
