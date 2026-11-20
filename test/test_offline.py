"""Offline tests of the thin Airflow adapter. SQL behavior is tested by run_cloud_tests.py."""
import ast
import hashlib
import re
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = ROOT / 'airflow/dags_tickets_test.py'
TREE = ast.parse(DAG_FILE.read_text(encoding='utf-8-sig'))
NODES = [n for n in TREE.body if not isinstance(n, ast.With)
    and not (isinstance(n, ast.ImportFrom) and (n.module or '').startswith('airflow'))]


def runtime():
    ns = {'__file__': str(DAG_FILE), 'BigQueryHook': Mock(), 'GCSHook': Mock(), 'Variable': Mock()}
    exec(compile(ast.Module(body=NODES, type_ignores=[]), str(DAG_FILE), 'exec'), ns)
    return ns


def request():
    return dict(job_id='zuora_tickets', execution_id='12', dag_id='test',
                airflow_run_id='manual_test', logical_date='2026-11-10T00:00:00+00:00',
                bootstrap_project='sandbox', bootstrap_dataset='test_zuora', bootstrap_table='control_panel',
                bootstrap_location='US', bootstrap_connection_id='google_cloud_default')


def file(**changes):
    values = dict(file_type='INV', file_name='file.csv', destination_uri='gs://test-bucket/data/output/file.csv',
        rows_exported=1, status='PREPARED', send_empty=False, connection_id='google_cloud_default',
        encoding='utf-8', delimiter=',', line_terminator='\n', export_sql='SELECT 1 AS id', page_size=100)
    values.update(changes)
    return SimpleNamespace(**values)


class Rows(list):
    schema = [SimpleNamespace(name='id')]


class Checks(unittest.TestCase):
    def setUp(self): self.ns = runtime()

    def test_identity_stable_and_configuration_specific(self):
        ctx = {'dag': SimpleNamespace(dag_id='test'), 'run_id': 'manual_a',
               'params': {'job_id': 'zuora_tickets'}, 'dag_run': SimpleNamespace(conf={}),
               'logical_date': datetime(2026, 11, 10, tzinfo=timezone.utc)}
        a = self.ns['request'](ctx)
        self.assertEqual(a, self.ns['request'](ctx))
        self.assertLess(int(a['execution_id']), 2**63)
        ctx['dag_run'].conf = {'job_id': 'another_source'}
        self.assertNotEqual(a['execution_id'], self.ns['request'](ctx)['execution_id'])

    def test_job_id_rename_preserves_existing_execution_identity(self):
        ctx = {'dag': SimpleNamespace(dag_id='test'), 'run_id': 'manual_a',
               'params': {'job_id': 'zuora_tickets'}, 'dag_run': SimpleNamespace(conf={}),
               'logical_date': datetime(2026, 11, 10, tzinfo=timezone.utc)}
        expected = str(int(hashlib.sha256(b'test:manual_a:zuora_tickets').hexdigest()[:15], 16))
        self.assertEqual(self.ns['request'](ctx)['execution_id'], expected)
        ctx['dag_run'].conf = {'run_id': 'zuora_tickets'}
        self.assertEqual(self.ns['request'](ctx)['execution_id'], expected)
        ctx['dag_run'].conf = {'job_id': 'zuora_tickets'}
        self.assertEqual(self.ns['request'](ctx)['execution_id'], expected)
        ctx['dag_run'].conf = {'job_id': 'zuora_tickets', 'run_id': 'different'}
        with self.assertRaisesRegex(ValueError, 'different configurations'):
            self.ns['request'](ctx)

    def test_all_sql_stages_render_with_bound_parameters(self):
        hook = Mock(); hook.insert_job.return_value.result.return_value = []
        self.ns['BigQueryHook'] = Mock(return_value=hook)
        stages = ('configure','validate_source','capture_source','extract','prepare_batches',
                  'files','finalize_file','close_run','fail_run')
        for stage in stages:
            self.ns['sql'](stage, request(), row_count=0, file_type="untrusted_'",
                           checksum='sha', error_step='step', error_message='message')
        self.assertEqual(hook.insert_job.call_count, 9)
        for call in hook.insert_job.call_args_list:
            query = call.kwargs['configuration']['query']
            self.assertNotIn('{{', query['query'])
            self.assertNotIn("untrusted_'", query['query'])
            self.assertEqual(set(re.findall(r'@p_(\w+)', query['query'])),
                {p['name'][2:] for p in query['queryParameters']})
        self.assertIn('`sandbox.test_zuora.sp_zuora_tickets`', hook.insert_job.call_args.kwargs['configuration']['query']['query'])

    def test_bootstrap_variable_is_read_only_when_called(self):
        value = dict(project='sandbox', dataset='test_zuora', table='control_panel',
                     location='US', connection_id='bootstrap_conn')
        self.ns['Variable'].assert_not_called()
        self.ns['Variable'].get.return_value = value
        loaded = self.ns['read_bootstrap']()
        self.ns['Variable'].get.assert_called_once_with('tickets_bootstrap', deserialize_json=True)
        self.assertEqual(loaded['bootstrap_connection_id'], 'bootstrap_conn')
        self.ns['sql'] = Mock(return_value=[dict(control_connection='from_table', control_location='US',
                                                delivery_connection='delivery_conn', delivery_location='US')])
        run = {**request(), **loaded}
        frozen = self.ns['load_configuration'](run)
        self.assertEqual(self.ns['Variable'].get.call_count, 1)
        self.assertEqual(frozen['control_connection'], 'from_table')
        value['project'] = 'later_change'
        self.assertEqual(frozen['bootstrap_project'], 'sandbox')

    def test_invalid_bootstrap_variable_stops_before_cloud_access(self):
        for value in [[], {}, dict(project='sandbox', dataset='x`; DROP TABLE y', table='control_panel',
                                   location='US', connection_id='conn')]:
            self.ns['Variable'].get.return_value = value
            with self.assertRaises(ValueError): self.ns['read_bootstrap']()
        self.ns['BigQueryHook'].assert_not_called()

    def test_runtime_configuration_is_loaded_and_used_by_stage(self):
        settings = dict(status='RUNNING', control_connection='source_conn', control_location='US',
                        delivery_connection='delivery_conn', delivery_location='US')
        self.ns['sql'] = Mock(return_value=[settings])
        frozen = self.ns['load_configuration'](request())
        self.ns['sql'].assert_called_once_with('prepare', request())
        settings['control_connection'] = 'changed_after_load'
        self.assertEqual(frozen['control_connection'], 'source_conn')
        self.assertEqual(self.ns['runtime_settings']('extract', frozen), ('source_conn', 'US'))
        self.assertEqual(self.ns['runtime_settings']('files', frozen), ('delivery_conn', 'US'))
        hook = Mock(); hook.insert_job.return_value.result.return_value = []
        self.ns['BigQueryHook'] = Mock(return_value=hook)
        # Restore the real adapter SQL entry point after the configuration read mock.
        real = runtime()
        real['BigQueryHook'] = self.ns['BigQueryHook']
        real['sql']('prepare_batches', frozen)
        self.assertEqual(real['BigQueryHook'].call_args.kwargs['gcp_conn_id'], 'delivery_conn')
        self.assertEqual(hook.insert_job.call_args.kwargs['location'], 'US')

    def test_missing_runtime_configuration_stops_execution(self):
        self.ns['sql'] = Mock(return_value=[])
        with self.assertRaisesRegex(RuntimeError, 'one frozen'):
            self.ns['load_configuration'](request())

    def test_multi_source_resolution_and_batch_execution(self):
        ctx = {'dag': SimpleNamespace(dag_id='test'), 'run_id': 'manual_multi',
               'params': {'job_id': 'ALL'}, 'dag_run': SimpleNamespace(conf={}),
               'logical_date': datetime(2026, 11, 10, tzinfo=timezone.utc)}
        bootstrap = dict(bootstrap_project='sandbox', bootstrap_dataset='test_zuora',
                         bootstrap_table='control_panel', bootstrap_location='US',
                         bootstrap_connection_id='google_cloud_default')
        # 1. No job_id in conf -> discovers all enabled sources in control_panel via list_jobs.
        self.ns['sql'] = Mock(return_value=[SimpleNamespace(job_id='zuora_tickets'),
                                            SimpleNamespace(job_id='zuora_suites')])
        resolved = self.ns['resolve_requests'](ctx, bootstrap)
        self.assertEqual([r['job_id'] for r in resolved], ['zuora_tickets', 'zuora_suites'])
        self.assertNotEqual(resolved[0]['execution_id'], resolved[1]['execution_id'])
        # 2. Explicit job_id in conf -> runs only that source without calling list_jobs.
        self.ns['sql'].reset_mock()
        ctx['dag_run'].conf = {'job_id': 'zuora_suites'}
        single = self.ns['resolve_requests'](ctx, bootstrap)
        self.assertEqual([r['job_id'] for r in single], ['zuora_suites'])
        self.ns['sql'].assert_not_called()
        # 3. prepare_batches and publish_files support a list of sources.
        self.ns['sql'] = Mock(side_effect=lambda step, run, **kw: (
            [dict(status='RUNNING', control_connection='c', control_location='US',
                  delivery_connection='c', delivery_location='US')]
            if step == 'prepare' else []))
        prepared = self.ns['prepare_batches'](resolved)
        self.assertEqual(len(prepared), 2)
        self.ns['publish_files'](prepared)

    def test_bootstrap_identifier_injection_is_rejected(self):
        run = {**request(), 'bootstrap_dataset': 'test`; DROP TABLE x; --'}
        with self.assertRaises(ValueError): self.ns['sql']('files', run)
        self.ns['BigQueryHook'].assert_not_called()

    def test_no_business_transformations_remain_in_python(self):
        names = {n.name for n in TREE.body if isinstance(n, ast.FunctionDef)}
        self.assertNotIn('validate_config', names)
        self.assertNotIn('check_schema', names)
        self.assertNotIn('parse_config_list', names)
        sql = (ROOT / 'setup/deploy_zuora_procedure.sql').read_text(encoding='utf-8-sig')
        self.assertNotRegex(sql, r"SELECT\s+'[^'\n]*'\s+WHERE")
        for fragment in ('CREATE OR REPLACE PROCEDURE', 'tickets_extract_stg', 'capture_date =', 'SAFE_CAST', 'IS NOT DISTINCT FROM', 'configuration_validation',
                         'schema_validation', 'quality_validation', 'REJECTED', 'BEGIN TRANSACTION', 'v_types'):
            self.assertIn(fragment, sql)

    def setup_transfer(self, files, rows=None):
        calls = []
        def sql(step, run, **kwargs):
            calls.append((step, kwargs))
            return files if step == 'files' else []
        client = Mock(); client.query.return_value.result.return_value = rows if rows is not None else Rows([{'id': 1}])
        hook = Mock(); hook.get_client.return_value = client
        self.ns['sql'] = sql
        self.ns['BigQueryHook'] = Mock(return_value=hook)
        self.ns['upload_csv'] = Mock(return_value='sha256')
        return calls, client

    def test_completed_files_are_not_republished(self):
        calls, client = self.setup_transfer([file(status='SENT'), file(status='SKIPPED')])
        self.ns['publish_files'](request())
        client.query.assert_not_called()
        self.ns['upload_csv'].assert_not_called()
        self.assertEqual(len(calls), 1)

    def test_rejected_plan_with_no_files_has_no_io(self):
        calls, client = self.setup_transfer([])
        self.ns['publish_files'](request())
        client.query.assert_not_called()
        self.ns['upload_csv'].assert_not_called()

    def test_empty_files_are_skipped_without_querying_data(self):
        calls, client = self.setup_transfer([file(rows_exported=0)])
        self.ns['publish_files'](request())
        client.query.assert_not_called()
        self.assertEqual(calls[-1], ('finalize_file', {'file_type': 'INV', 'checksum': None, 'row_count': 0}))

    def test_header_only_file_respects_send_empty(self):
        calls, client = self.setup_transfer([file(rows_exported=0, send_empty=True)], Rows())
        def upload(path, plan):
            self.assertEqual(Path(path).read_bytes(), b'id\n')
            return 'sha'
        self.ns['upload_csv'] = upload
        self.ns['publish_files'](request())
        self.assertEqual(calls[-1][1]['row_count'], 0)
        self.assertEqual(calls[-1][1]['checksum'], 'sha')

    def test_csv_serialization_and_paging(self):
        calls, client = self.setup_transfer([file()], Rows([{'id': 'comma,value'}]))
        def upload(path, plan):
            self.assertEqual(Path(path).read_bytes(), b'id\n"comma,value"\n')
            return 'sha'
        self.ns['upload_csv'] = upload
        self.ns['publish_files'](request())
        client.query.return_value.result.assert_called_once_with(page_size=100)
        self.assertEqual(calls[-1][1]['row_count'], 1)

    def test_count_mismatch_never_uploads_or_finalizes(self):
        calls, client = self.setup_transfer([file(rows_exported=2)])
        with self.assertRaisesRegex(RuntimeError, 'CSV count'):
            self.ns['publish_files'](request())
        self.ns['upload_csv'].assert_not_called()
        self.assertEqual(calls[-1][0], 'fail_run')
        self.assertNotIn('finalize_file', [c[0] for c in calls])

    def test_failed_transfer_stops_following_files(self):
        calls, client = self.setup_transfer([file(), file(file_type='CM')])
        self.ns['upload_csv'].side_effect = ConnectionError('upload interrupted')
        with self.assertRaises(ConnectionError): self.ns['publish_files'](request())
        self.assertEqual(self.ns['upload_csv'].call_count, 1)
        self.assertEqual(calls[-1][0], 'fail_run')

    def test_existing_identical_object_recovers_and_collision_fails(self):
        blob = Mock(); client = SimpleNamespace(bucket=lambda _: SimpleNamespace(blob=lambda _: blob))
        self.ns['GCSHook'] = Mock(return_value=SimpleNamespace(get_conn=lambda: client))
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'file.csv'; path.write_bytes(b'id\n1\n')
            expected = self.ns['upload_csv'](path, file())
            self.assertEqual(expected, hashlib.sha256(b'id\n1\n').hexdigest())
            self.assertEqual(blob.upload_from_filename.call_args.kwargs['if_generation_match'], 0)
            blob.upload_from_filename.side_effect = self.ns['PreconditionFailed']('exists')
            self.assertEqual(expected, self.ns['upload_csv'](path, file()))
            blob.reload.side_effect = lambda: setattr(blob, 'metadata', {})
            with self.assertRaisesRegex(RuntimeError, 'collision'):
                self.ns['upload_csv'](path, file())

    def test_initial_setup_contract_is_consistent(self):
        seed = (ROOT / 'setup/seed_control_panel.sql').read_text(encoding='utf-8-sig')
        initial = (ROOT / 'setup/create_tickets_extract.sql').read_text(encoding='utf-8-sig')
        cols = re.findall(r"STRUCT\('([^']+)' AS source_column", seed)
        self.assertEqual(len(cols), 51)
        for col in cols: self.assertIn(f'AS `{col.lower()}`', initial)
        self.assertIn('WHERE FALSE', initial)
        self.assertIn('TO_JSON(sm.schema_fields)', seed)
        self.assertIn("'Invoice_Line_ID,Ext_Sell_Price', 'INV,CM'", seed)
        setup = (ROOT / 'setup/setup_control_tables.sql').read_text(encoding='utf-8-sig')
        self.assertEqual(setup.count('job_id STRING NOT NULL'), 4)
        self.assertIn('CLUSTER BY job_id, execution_id', setup)
        self.assertIn('(job_id, config_version', seed)
        self.assertIn(' AS run_id,', initial)
        self.assertIn("STRUCT('run_id' AS name,'INT64' AS kind)", (ROOT / 'setup/deploy_zuora_procedure.sql').read_text(encoding='utf-8-sig'))

    def test_partitioned_staging_and_monitoring_are_explicit(self):
        setup = (ROOT / 'setup/setup_control_tables.sql').read_text(encoding='utf-8-sig')
        procedure = (ROOT / 'setup/deploy_zuora_procedure.sql').read_text(encoding='utf-8-sig')
        self.assertFalse((ROOT / 'setup/create_monitoring_view.sql').exists())
        self.assertIn('DROP VIEW IF EXISTS `gsw-analytics-sandbox.test_zuora.vw_zuora_job_status`', setup)
        self.assertIn('PARTITION BY capture_date', setup)
        self.assertIn('CLUSTER BY job_id, execution_id', setup)
        self.assertIn('require_partition_filter = TRUE', setup)
        self.assertIn('IF v_snapshot_at IS NULL THEN', procedure)
        self.assertIn('source_snapshot_at = v_snapshot_at', procedure)
        self.assertIn('WHERE capture_date =', procedure)
        self.assertIn('stringify_wide_numbers => TRUE', procedure)
        self.assertIn('FROM_BASE64', procedure)

    def test_direct_view_simulation_and_lowercase_destination_schema(self):
        initial = (ROOT / 'setup/create_tickets_extract.sql').read_text(encoding='utf-8-sig')
        aliases = re.findall(r' AS `([^`]+)`', initial)
        self.assertEqual(len(aliases), 51)
        self.assertTrue(all(name == name.lower() for name in aliases))
        simulation = (ROOT / 'test/prepare_initial_from_view.sql').read_text(encoding='utf-8-sig')
        self.assertNotIn('CREATE TABLE IF NOT EXISTS `gsw-analytics-sandbox.test_zuora.ticketmaster_initial_sample`', simulation)
        self.assertIn('sample_rows_per_type', simulation)
        self.assertIn("job_id = 'zuora_tickets'", simulation)
        self.assertIn("'zuora_tickets_initial_simulation' AS job_id", simulation)
        self.assertNotIn('CREATE TABLE', simulation)
        self.assertIn('BEGIN TRANSACTION', simulation)
        sql = (ROOT / 'setup/deploy_zuora_procedure.sql').read_text(encoding='utf-8-sig')
        self.assertIn('ROW_NUMBER() OVER', sql)
        self.assertIn('v_sample_rows', sql)
        self.assertIn('LOWER(source_column), output_column', sql)

    def test_dag_import_and_explicit_dependencies_need_no_cloud(self):
        edges = []
        class Node:
            def __init__(self, task_id): self.task_id = task_id
            def __rshift__(self, other):
                edges.append((self.task_id, other.task_id)); return other
        class FakeDAG:
            def __init__(self, **kwargs): pass
            def __enter__(self): return self
            def __exit__(self, *args): pass
        class Task:
            def __init__(self, task_id): self.task_id = task_id
            def __call__(self, *args, **kwargs): return Node(self.task_id)
            def override(self, **kwargs): return Task(kwargs['task_id'])
        def task(fn=None, **kwargs):
            if fn: return Task(fn.__name__)
            return lambda fn: Task(kwargs.get('task_id', fn.__name__))
        ns = {'__file__': str(DAG_FILE), 'DAG': FakeDAG, 'task': task,
              'BigQueryHook': Mock(), 'GCSHook': Mock(), 'Variable': Mock()}
        nodes = [n for n in TREE.body if not (isinstance(n, ast.ImportFrom) and (n.module or '').startswith('airflow'))]
        exec(compile(ast.Module(body=nodes, type_ignores=[]), '<DAG import>', 'exec'), ns)
        expected = ['prepare_batches', 'publish_files']
        self.assertEqual(edges, list(zip(expected, expected[1:])))
        ns['BigQueryHook'].assert_not_called(); ns['GCSHook'].assert_not_called()
        ns['Variable'].get.assert_not_called()


if __name__ == '__main__':
    unittest.main()
