"""Named offline scenarios for the thin Airflow adapter. Data validation runs in BigQuery."""
import argparse
import json
import sys
import unittest
from pathlib import Path
from test_offline import Checks

SCENARIOS = {
 'variable_bootstrap': 'test_bootstrap_variable_is_read_only_when_called',
 'variable_bootstrap_invalida': 'test_invalid_bootstrap_variable_stops_before_cloud_access',
 'configuracion_en_ejecucion': 'test_runtime_configuration_is_loaded_and_used_by_stage',
 'configuracion_ausente': 'test_missing_runtime_configuration_stops_execution',
 'multiples_fuentes': 'test_multi_source_resolution_and_batch_execution',
 'staging_particionado': 'test_partitioned_staging_and_monitoring_are_explicit',
 'vista_directa_columnas_minusculas': 'test_direct_view_simulation_and_lowercase_destination_schema',
 'job_id_compatible': 'test_job_id_rename_preserves_existing_execution_identity',
 'identidad': 'test_identity_stable_and_configuration_specific',
 'sql_parametrizado': 'test_all_sql_stages_render_with_bound_parameters',
 'identificadores_invalidos': 'test_bootstrap_identifier_injection_is_rejected',
 'transformaciones_en_bigquery': 'test_no_business_transformations_remain_in_python',
 'sin_reenvios': 'test_completed_files_are_not_republished',
 'rejected_sin_archivos': 'test_rejected_plan_with_no_files_has_no_io',
 'archivo_vacio_omitido': 'test_empty_files_are_skipped_without_querying_data',
 'archivo_vacio_generado': 'test_header_only_file_respects_send_empty',
 'csv_y_paginacion': 'test_csv_serialization_and_paging',
 'conteo_incorrecto': 'test_count_mismatch_never_uploads_or_finalizes',
 'falla_entrega': 'test_failed_transfer_stops_following_files',
 'reintento_publicacion': 'test_existing_identical_object_recovers_and_collision_fails',
 'configuracion_inicial': 'test_initial_setup_contract_is_consistent',
 'orquestamiento': 'test_dag_import_and_explicit_dependencies_need_no_cloud',
}
PENDING = ['Esquema, tipos, deduplicacion, calidad y REJECTED deben validarse con run_cloud_tests.py: ahora se ejecutan en SQL.',
           'SFTP, ZIP y MERGE/versionado siguen pendientes.']


def main():
    parser = argparse.ArgumentParser(description='Pruebas offline del adaptador Airflow, sin credenciales.')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--list', action='store_true')
    group.add_argument('--all', action='store_true')
    group.add_argument('--scenario', choices=list(SCENARIOS))
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    if args.list:
        for name in SCENARIOS: print(name)
        for text in PENDING: print(text)
        return 0
    report = {'mode': 'offline_adapter', 'scenarios': [], 'pending': PENDING}
    for name in [args.scenario] if args.scenario else SCENARIOS:
        result = unittest.TextTestRunner(stream=sys.stdout, verbosity=1).run(unittest.TestSuite([Checks(SCENARIOS[name])]))
        report['scenarios'].append({'name': name, 'status': 'PASS' if result.wasSuccessful() else 'FAIL',
            'details': [detail for _, detail in result.failures + result.errors]})
    failed = sum(s['status'] == 'FAIL' for s in report['scenarios'])
    report['summary'] = {'total': len(report['scenarios']), 'failed': failed,
                         'passed': len(report['scenarios']) - failed}
    if args.report: args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(report['summary'])
    return int(failed > 0)


if __name__ == '__main__':
    raise SystemExit(main())
