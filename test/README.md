# Pruebas de integración y adaptador

## Pruebas locales (sin credenciales de Cloud)

```powershell
python test/test_offline.py -v
```

Valida el adaptador Airflow (`prepare_batches >> publish_files`), la resolución multi-fuente, la parametrización SQL, la creación idempotente de CSV en GCS con mocks y la consistencia de los scripts SQL.

## Pruebas en Google Cloud aislado (`*_test`)

Con Application Default Credentials:

```powershell
python -m pip install -r test/requirements.txt
python test/run_cloud_tests.py --project gsw-analytics-sandbox --dataset app_integration_zuora_test --bucket gsw-composer-gh --scenario all --report cloud_report.json
```

El dataset debe terminar en `_test`. Crea fixtures sintéticos para los 12 escenarios (`normal`, `extra_column`, `empty`, `sample_limit`, `snapshot_recovery`, `publish_recovery`, `missing_column`, `changed_type`, `strict_extra`, `bad_value`, `required_null` y `duplicate_key`).
