"""Run synthetic integration tests in BigQuery + GCS, or prepare fixtures for Composer.
Uses Application Default Credentials. All SQL is in .sql files; no Airflow installation needed.
"""
import argparse
import ast
import json
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from google.cloud import bigquery, storage
from jinja2 import Environment, StrictUndefined

ROOT = Path(__file__).resolve().parents[1]
SCENARIOS = {
    'normal': 'DONE', 'extra_column': 'DONE', 'empty': 'DONE', 'sample_limit': 'DONE', 'snapshot_recovery': 'DONE', 'publish_recovery': 'DONE',
    'missing_column': 'REJECTED', 'changed_type': 'REJECTED', 'strict_extra': 'REJECTED',
    'bad_value': 'REJECTED', 'required_null': 'REJECTED', 'duplicate_key': 'REJECTED',
}


def runtime(project, dataset, location, bq, gcs):
    class BigQueryHook:
        def __init__(self, **kwargs): pass
        def get_client(self, **kwargs): return bq
        def insert_job(self, configuration, **kwargs):
            return bq.query(configuration['query']['query'],
                            job_config=bigquery.QueryJobConfig.from_api_repr(configuration), location=location)
    class GCSHook:
        def __init__(self, **kwargs): pass
        def get_conn(self): return gcs
    path = ROOT / 'airflow/dags_tickets_v2.py'
    tree = ast.parse(path.read_text(encoding='utf-8-sig'))
    nodes = [n for n in tree.body if not isinstance(n, ast.With)
        and not (isinstance(n, ast.ImportFrom) and (n.module or '').startswith('airflow'))]
    ns = {'__file__': str(path), 'BigQueryHook': BigQueryHook, 'GCSHook': GCSHook,
          'Variable': SimpleNamespace(get=lambda key, **kwargs: dict(project=project, dataset=dataset,
              table='control_panel', location=location, connection_id='google_cloud_default'))}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), ns)
    return ns


def query_file(client, path, identifiers, parameters=None, section=None):
    sql = path.read_text(encoding='utf-8-sig')
    if section:
        sql = {part.split('\n', 1)[0].strip(): part.split('\n', 1)[1]
               for part in sql.split('-- name: ')[1:]}[section]
    sql = Environment(undefined=StrictUndefined).from_string(sql).render(**identifiers)
    params = []
    for name, value in (parameters or {}).items():
        kind = 'INT64' if isinstance(value, int) else 'STRING'
        params.append(bigquery.ScalarQueryParameter(name, kind, value))
    return list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())


def main():
    parser = argparse.ArgumentParser(description='Real cloud tests: creates synthetic fixtures and publishes to GCS test/zuora/.')
    parser.add_argument('--project', required=True)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--dataset', default='app_integration_zuora_test')
    parser.add_argument('--location', default='US')
    parser.add_argument('--scenario', choices=['all'] + list(SCENARIOS), default='all')
    parser.add_argument('--prepare-only', action='store_true', help='Create fixtures/configs, then trigger the DAG manually in Composer')
    parser.add_argument('--report', type=Path, default=Path('cloud_report.json'))
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9_-]+', args.project): parser.error('Invalid project')
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*_test', args.dataset):
        parser.error('Dataset must end in _test; operational datasets are not allowed')
    if not re.fullmatch(r'[a-z0-9][a-z0-9._-]+[a-z0-9]', args.bucket): parser.error('Invalid bucket')
    bq = bigquery.Client(project=args.project, location=args.location)
    gcs = storage.Client(project=args.project)
    dataset = bigquery.Dataset(f'{args.project}.{args.dataset}')
    dataset.location = args.location
    existing = bq.create_dataset(dataset, exists_ok=True)
    if existing.location.upper() != args.location.upper():
        raise ValueError(f'Dataset location is {existing.location}, expected {args.location}')
    setup = (ROOT / 'setup/01_setup_control_tables.sql').read_text(encoding='utf-8-sig')
    setup = setup.replace('gsw-analytics-sandbox.app_integration_zuora', f'{args.project}.{args.dataset}')
    bq.query(setup).result()
    procedure = (ROOT / 'setup/04_deploy_zuora_procedure.sql').read_text(encoding='utf-8-sig')
    procedure = procedure.replace('gsw-analytics-sandbox.app_integration_zuora', f'{args.project}.{args.dataset}')
    bq.query(procedure).result()
    ns = runtime(args.project, args.dataset, args.location, bq, gcs)
    session = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S') + '_' + uuid.uuid4().hex[:8]
    report = {'session': session, 'project': args.project, 'dataset': args.dataset,
              'mode': 'prepare_only' if args.prepare_only else 'cloud_helpers', 'results': []}
    failed = 0
    for scenario in (list(SCENARIOS) if args.scenario == 'all' else [args.scenario]):
        job_id = f'qa_{session}_{scenario}'
        prefix = f'test/zuora/{session}/{scenario}'
        identifiers = {'project': args.project, 'dataset': args.dataset, 'scenario': scenario,
                       'base': job_id + '_base', 'source': job_id + '_source', 'target': job_id + '_target'}
        entry = {'scenario': scenario, 'job_id': job_id, 'expected_status': SCENARIOS[scenario],
                 'gcs_prefix': f'gs://{args.bucket}/{prefix}', 'trigger_conf': {'job_id': job_id}}
        try:
            query_file(bq, ROOT / 'test/cloud_fixtures.sql', identifiers, {
                'job_id': job_id, 'project': args.project, 'dataset': args.dataset,
                'source': identifiers['source'], 'location': args.location,
                'target_table': f"{args.project}.{args.dataset}.{identifiers['target']}",
                'schema_policy': 'STRICT' if scenario == 'strict_extra' else 'ALLOW_ADDITIONAL_COLUMNS',
                'destination_json': json.dumps({'bucket': args.bucket, 'prefix': prefix,
                    'date_path_format': '%Y/%m/%d', 'file_name_template': 'QA_{file_type}_{execution_id}.csv'}),
            })
            if args.prepare_only:
                entry['result'] = 'PREPARED'
            else:
                ctx = {'dag': SimpleNamespace(dag_id='zuora_cloud_helpers_test'), 'run_id': session + '_' + scenario,
                       'params': {'job_id': job_id}, 'dag_run': SimpleNamespace(conf={}),
                       'logical_date': datetime.now(timezone.utc)}
                if scenario == 'snapshot_recovery':
                    base_run = {**ns['request'](ctx), **ns['read_bootstrap']()}
                    ns['sql']('capture_source', base_run)
                    query_file(bq, ROOT / 'test/cloud_change_source.sql', identifiers)
                run = ns['prepare_batches'](ns['request'](ctx))
                entry['execution_id'] = run['execution_id']
                query_file(bq, ROOT / 'test/cloud_checks.sql', identifiers,
                           {'execution_id': run['execution_id']}, section='before_delivery')
                if scenario == 'publish_recovery':
                    original_sql = ns['sql']
                    def fail_after_upload(step, request, **values):
                        if step == 'finalize_file':
                            raise RuntimeError('Injected failure after upload, before history commit')
                        return original_sql(step, request, **values)
                    ns['sql'] = fail_after_upload
                    try:
                        try:
                            ns['publish_files'](run)
                        except RuntimeError as exc:
                            if 'Injected failure' not in str(exc): raise
                        else:
                            raise AssertionError('Publication failure was not injected')
                        query_file(bq, ROOT / 'test/cloud_checks.sql', identifiers,
                                   {'execution_id': run['execution_id']}, section='before_delivery')
                        uploaded = {o.name: o.generation for o in gcs.list_blobs(args.bucket, prefix=prefix + '/')}
                        if not uploaded: raise AssertionError('Expected object before history commit')
                    finally:
                        ns['sql'] = original_sql
                ns['publish_files'](run)
                if scenario == 'publish_recovery':
                    recovered = {o.name: o.generation for o in gcs.list_blobs(args.bucket, prefix=prefix + '/')}
                    if any(recovered.get(name) != generation for name, generation in uploaded.items()):
                        raise AssertionError('Recovery recreated an already published object')
                ns['sql']('close_run', run)
                query_file(bq, ROOT / 'test/cloud_checks.sql', identifiers, {
                    'job_id': job_id, 'execution_id': run['execution_id'],
                    'expected_status': SCENARIOS[scenario], 'expected_rows': 0 if scenario == 'empty' else 2,
                }, section='verify')
                objects = list(gcs.list_blobs(args.bucket, prefix=prefix + '/'))
                expected_files = 0 if scenario == 'empty' or SCENARIOS[scenario] == 'REJECTED' else 2
                if len(objects) != expected_files:
                    raise AssertionError(f'Expected {expected_files} objects, found {len(objects)}')
                if SCENARIOS[scenario] != 'REJECTED':
                    generations = {obj.name: obj.generation for obj in objects}
                    ns['publish_files'](run)
                    after = {obj.name: obj.generation for obj in gcs.list_blobs(args.bucket, prefix=prefix + '/')}
                    if generations != after: raise AssertionError('Retry changed published objects')
                    ctx['run_id'] += '_repeat'
                    repeated = ns['prepare_batches'](ns['request'](ctx))
                    ns['publish_files'](repeated)
                    ns['sql']('close_run', repeated)
                    entry['repeat_execution_id'] = repeated['execution_id']
                    query_file(bq, ROOT / 'test/cloud_checks.sql', identifiers, {
                        'job_id': job_id, 'execution_id': repeated['execution_id'],
                        'expected_status': 'REJECTED' if scenario == 'snapshot_recovery' else 'DONE',
                        'expected_rows': 0 if scenario == 'empty' else 2,
                    }, section='verify')
                    if generations != {obj.name: obj.generation for obj in gcs.list_blobs(args.bucket, prefix=prefix + '/')}:
                        raise AssertionError('Repeated extraction republished objects')

                entry['result'] = 'PASS'
        except Exception as exc:
            failed += 1
            entry.update(result='FAIL', error=f'{type(exc).__name__}: {exc}')
        report['results'].append(entry)
        args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        print(json.dumps(entry, ensure_ascii=False), flush=True)
    print(f'Report: {args.report.resolve()}. Fixtures are retained for audit; nothing is automatically deleted.')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
