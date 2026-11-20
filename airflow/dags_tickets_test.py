"""Airflow 2: BigQuery owns multi-source control, validation and preparation; Python only orchestrates and publishes CSV."""
import csv
import hashlib
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.api_core.exceptions import PreconditionFailed
from google.cloud import bigquery
from jinja2 import Environment, StrictUndefined

BOOTSTRAP_VARIABLE = 'tickets_bootstrap'
SQL_FILE = Path(__file__).with_name('zuora_tickets.sql')
DELIVERY_STEPS = frozenset(('prepare_batches', 'files', 'finalize_file', 'close_run'))


def request(context, job_id=None):
    conf = context['dag_run'].conf or {}
    if job_id is None:
        if 'job_id' in conf and 'run_id' in conf and conf['job_id'] != conf['run_id']:
            raise ValueError('job_id and legacy run_id cannot identify different configurations')
        job_id = conf.get('job_id', conf.get('run_id', context.get('params', {}).get('job_id', context.get('params', {}).get('run_id'))))
    if not isinstance(job_id, str) or not job_id.strip():
        raise ValueError('job_id must identify a configuration')
    job_id = job_id.strip()
    key = f"{context['dag'].dag_id}:{context['run_id']}:{job_id}"
    return dict(job_id=job_id, execution_id=str(int(hashlib.sha256(key.encode()).hexdigest()[:15], 16)),
                dag_id=context['dag'].dag_id, airflow_run_id=context['run_id'],
                logical_date=(context.get('logical_date') or context['dag_run'].start_date).isoformat())


def read_bootstrap():
    """Read only while a task runs, never while the scheduler parses this file."""
    values = Variable.get(BOOTSTRAP_VARIABLE, deserialize_json=True)
    if not isinstance(values, dict):
        raise ValueError('tickets_bootstrap must be a JSON object')
    required = ('project', 'dataset', 'table', 'location', 'connection_id')
    if any(not isinstance(values.get(k), str) or not values[k].strip() for k in required):
        raise ValueError('tickets_bootstrap requires project, dataset, table, location and connection_id')
    if (not re.fullmatch(r'[A-Za-z0-9_-]+', values['project'])
        or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', values['dataset'])
        or values['table'] != 'control_panel'):
        raise ValueError('Invalid control identifier')
    return {'bootstrap_' + key: values[key] for key in required}


def runtime_settings(step, run):
    prefix = 'delivery' if step in DELIVERY_STEPS else 'control'
    return (run.get(prefix + '_connection') or run['bootstrap_connection_id'],
            run.get(prefix + '_location') or run['bootstrap_location'])


def resolve_requests(context, bootstrap=None):
    """Resolve one explicit job_id, a list of job_ids, or all enabled sources in control_panel."""
    bootstrap = bootstrap or read_bootstrap()
    conf = context['dag_run'].conf or {}
    if isinstance(conf.get('job_ids'), list) and conf['job_ids']:
        return [{**request(context, job_id=jid), **bootstrap} for jid in conf['job_ids']]
    explicit = conf.get('job_id', conf.get('run_id'))
    if isinstance(explicit, str) and explicit.strip() and explicit.strip().upper() != 'ALL':
        return [{**request(context, job_id=explicit), **bootstrap}]
    probe = {**request(context, job_id='ALL'), **bootstrap}
    jobs = [row['job_id'] if isinstance(row, dict) else row.job_id for row in sql('list_jobs', probe)]
    if not jobs:
        raise RuntimeError('No enabled configurations found in control_panel')
    return [{**request(context, job_id=jid), **bootstrap} for jid in jobs]


def prepare_batches(run):
    """Run BigQuery configuration freeze, validation, capture, extraction and batch preparation."""
    if isinstance(run, list):
        return [prepare_batches(item) for item in run]
    if 'bootstrap_project' not in run:
        run = {**run, **read_bootstrap()}
    rows = list(sql('prepare', run))
    if len(rows) != 1:
        raise RuntimeError('Expected one frozen runtime configuration')
    return {**run, **dict(rows[0].items())}


load_configuration = prepare_batches


def query_config(step, run, **values):
    """Build a parameterized CALL. No data rules or transformations execute in Python."""
    project, dataset = run['bootstrap_project'], run['bootstrap_dataset']
    if (not re.fullmatch(r'[A-Za-z0-9_-]+', project)
        or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', dataset)
        or run['bootstrap_table'] != 'control_panel'):
        raise ValueError('Invalid control identifier')
    query = Environment(undefined=StrictUndefined).from_string(SQL_FILE.read_text(encoding='utf-8-sig')).render(
        project=project, dataset=dataset)
    params = {**run, 'step': step, 'location': runtime_settings(step, run)[1], 'file_type': None, 'checksum': None,
              'row_count': None, 'error_step': None, 'error_message': None, **values}
    bindings = [bigquery.ScalarQueryParameter('p_' + name, 'INT64' if name == 'row_count' else 'STRING', params[name]).to_api_repr()
                for name in sorted(set(re.findall(r'@p_(\w+)', query)))]
    return {'query': {'query': query, 'useLegacySql': False, 'parameterMode': 'NAMED', 'queryParameters': bindings}}


def sql(step, run, **values):
    configuration = query_config(step, run, **values)
    connection, location = runtime_settings(step, run)
    hook = BigQueryHook(gcp_conn_id=connection, location=location, use_legacy_sql=False)
    return hook.insert_job(project_id=run['bootstrap_project'], location=location,
                           configuration=configuration).result()


def upload_csv(path, file):
    """Create-only publication; a retry accepts an existing object with the same content."""
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    checksum = digest.hexdigest()
    expected = {'sha256': checksum, 'row_count': str(file.rows_exported)}
    bucket, object_name = file.destination_uri[5:].split('/', 1)
    blob = GCSHook(gcp_conn_id=file.connection_id).get_conn().bucket(bucket).blob(object_name)
    blob.metadata = expected
    try:
        blob.upload_from_filename(str(path), content_type='text/csv', if_generation_match=0, checksum='auto')
    except PreconditionFailed:
        blob.reload()
        if not all((blob.metadata or {}).get(k) == v for k, v in expected.items()):
            raise RuntimeError(f'Object collision: {file.destination_uri}')
    return checksum


def publish_files(run):
    if isinstance(run, list):
        for item in run:
            publish_files(item)
        return
    if run.get('status') in ('REJECTED', 'DONE'):
        return
    for file in list(sql('files', run)):
        if file.status in ('SENT', 'SKIPPED'):
            continue
        if file.status not in ('PREPARED', 'FAILED'):
            raise RuntimeError(f'Unexpected manifest state: {file.status}')
        try:
            checksum, count = None, 0
            if file.rows_exported or file.send_empty:
                client = BigQueryHook(gcp_conn_id=file.connection_id).get_client(project_id=run['bootstrap_project'])
                rows = client.query(file.export_sql, location=runtime_settings('files', run)[1]).result(page_size=file.page_size)
                with TemporaryDirectory(prefix='zuora_') as tmp:
                    path = Path(tmp) / file.file_name
                    with path.open('w', encoding=file.encoding, newline='') as stream:
                        writer = csv.writer(stream, delimiter=file.delimiter, lineterminator=file.line_terminator)
                        writer.writerow(field.name for field in rows.schema)
                        for row in rows:
                            writer.writerow('' if value is None else value for value in row.values())
                            count += 1
                    if count != file.rows_exported:
                        raise RuntimeError('CSV count differs from the prepared batch')
                    checksum = upload_csv(path, file)
            sql('finalize_file', run, file_type=file.file_type, checksum=checksum, row_count=count)
        except Exception as exc:
            sql('fail_run', run, file_type=file.file_type, error_step='publish_files', error_message=str(exc)[:8000])
            raise


def mark_failed(context):
    runs = context['task_instance'].xcom_pull(task_ids='prepare_batches')
    if not runs:
        try:
            runs = [request(context)]
            runs = [{**r, **read_bootstrap()} for r in runs]
        except Exception:
            return
    if isinstance(runs, dict):
        runs = [runs]
    for run in runs:
        sql('fail_run', run, error_step=context['task_instance'].task_id,
            error_message=str(context.get('exception', 'Task failed'))[:8000])


with DAG(
    dag_id='dags_tickets_test', description='BigQuery multi-source control-table driven extraction and CSV delivery',
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), schedule=None, catchup=False,
    max_active_runs=1, is_paused_upon_creation=True, params={'job_id': 'ALL'},
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=2),
                  'retry_exponential_backoff': True, 'on_failure_callback': mark_failed},
    tags=['zuora'],
) as dag:
    @task(task_id='prepare_batches')
    def prepare_task():
        return prepare_batches(resolve_requests(get_current_context()))

    @task(task_id='publish_files')
    def publication_task(runs):
        publish_files(runs)

    preparacion = prepare_task()
    publicacion = publication_task(preparacion)
    preparacion >> publicacion
