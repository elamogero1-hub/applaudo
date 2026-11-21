"""Airflow 2: BigQuery owns multi-source control, validation and preparation via 3 decoupled stored procedures."""
import csv
import hashlib
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.api_core.exceptions import PreconditionFailed
from google.cloud import bigquery

BOOTSTRAP_VARIABLE = 'tickets_bootstrap'
DELIVERY_STEPS = frozenset(('prepare_batches', 'files', 'finalize_file', 'close_run'))
PROCEDURES = {
    'prepare': ('sp_prepare_extraction', ('job_id', 'execution_id', 'dag_id', 'airflow_run_id', 'logical_date')),
    'files': ('sp_prepare_extraction', ('job_id', 'execution_id', 'dag_id', 'airflow_run_id', 'logical_date')),
    'finalize_file': ('sp_finalize_delivery', ('job_id', 'execution_id', 'file_type', 'checksum', 'row_count')),
    'fail_run': ('sp_fail_execution', ('job_id', 'execution_id', 'file_type', 'error_step', 'error_message')),
}


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
    if isinstance(explicit, str) and explicit.strip() and explicit.strip().upper() not in ('ALL', '{}', '*'):
        return [{**request(context, job_id=explicit), **bootstrap}]
    probe = {**request(context, job_id='ALL'), **bootstrap}
    jobs = [row['job_id'] if isinstance(row, dict) else row.job_id for row in sql('list_jobs', probe)]
    if not jobs:
        raise RuntimeError('No enabled configurations found in control_panel')
    return [{**request(context, job_id=jid), **bootstrap} for jid in jobs]


def prepare_batches(run):
    """Invoke sp_prepare_extraction (5 params) and return the prepared file manifests."""
    if isinstance(run, list):
        return [prepare_batches(item) for item in run]
    if 'bootstrap_project' not in run:
        run = {**run, **read_bootstrap()}
    rows = [dict(r.items()) if hasattr(r, 'items') else dict(vars(r)) for r in sql('prepare', run)]
    if not rows:
        return {**run, 'status': 'REJECTED', 'files': []}
    first = rows[0]
    conn = first.get('connection_id') or first.get('control_connection') or run['bootstrap_connection_id']
    loc = first.get('location') or first.get('control_location') or run['bootstrap_location']
    return {
        **run,
        'job_id': first.get('job_id', run['job_id']),
        'status': first.get('run_status', first.get('status', 'RUNNING')),
        'control_connection': first.get('control_connection', conn),
        'control_location': first.get('control_location', loc),
        'delivery_connection': first.get('delivery_connection', conn),
        'delivery_location': first.get('delivery_location', loc),
        'files': rows if 'file_type' in first else [],
    }


load_configuration = prepare_batches


def query_config(step, run, **values):
    """Build a parameterized query for the target decoupled stored procedure."""
    project, dataset = run['bootstrap_project'], run['bootstrap_dataset']
    if (not re.fullmatch(r'[A-Za-z0-9_-]+', project)
        or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', dataset)
        or run['bootstrap_table'] != 'control_panel'):
        raise ValueError('Invalid control identifier')
    if step == 'list_jobs':
        query = f'SELECT job_id FROM `{project}.{dataset}.control_panel` WHERE enabled GROUP BY job_id ORDER BY job_id;'
        return {'query': {'query': query, 'useLegacySql': False}}
    proc_name, param_names = PROCEDURES[step]
    args = ', '.join(f'@p_{name}' for name in param_names)
    query = f'CALL `{project}.{dataset}.{proc_name}`({args});'
    params = {**run, 'file_type': None, 'checksum': None, 'row_count': None,
              'error_step': None, 'error_message': None, **values}
    bindings = [bigquery.ScalarQueryParameter('p_' + name, 'INT64' if name == 'row_count' else 'STRING', params[name]).to_api_repr()
                for name in param_names]
    return {'query': {'query': query, 'useLegacySql': False, 'parameterMode': 'NAMED', 'queryParameters': bindings}}


def sql(step, run, **values):
    configuration = query_config(step, run, **values)
    connection, location = runtime_settings(step, run)
    hook = BigQueryHook(gcp_conn_id=connection, location=location, use_legacy_sql=False)
    return hook.insert_job(project_id=run['bootstrap_project'], location=location,
                           configuration=configuration).result()


def upload_sftp(path, file, checksum):
    """Atomic SFTP publication (.tmp + rename); retry-safe if matching file already exists."""
    from airflow.providers.sftp.hooks.sftp import SFTPHook
    conn_id, remote_rel = file.destination_uri[7:].split('/', 1)
    conn_id = getattr(file, 'sftp_connection_id', None) or conn_id
    remote_path = '/' + remote_rel.lstrip('/')
    remote_dir = str(Path(remote_path).parent).replace('\\', '/')
    tmp_path = remote_path + '.tmp'
    size = Path(path).stat().st_size
    hook = SFTPHook(ssh_conn_id=conn_id)
    with hook.get_conn() as sftp:
        try:
            existing = sftp.stat(remote_path)
            if existing.st_size == size:
                return checksum
            raise RuntimeError(f'Object collision on SFTP: {file.destination_uri}')
        except FileNotFoundError:
            pass
        current = ''
        for part in [p for p in remote_dir.split('/') if p]:
            current += '/' + part
            try:
                sftp.stat(current)
            except FileNotFoundError:
                sftp.mkdir(current)
        sftp.put(str(path), tmp_path)
        try:
            sftp.posix_rename(tmp_path, remote_path)
        except AttributeError:
            sftp.rename(tmp_path, remote_path)
    return checksum


def upload_csv(path, file):
    """Create-only publication to GCS or SFTP driven by control_panel.destination_type."""
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    checksum = digest.hexdigest()
    if file.destination_uri.startswith('sftp://') or getattr(file, 'destination_type', 'GCS') == 'SFTP':
        return upload_sftp(path, file, checksum)
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
    raw_files = run['files'] if 'files' in run else list(sql('files', run))
    manifests = [SimpleNamespace(**f) if isinstance(f, dict) else f for f in raw_files]
    for file in manifests:
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
    dag_id='dags_tickets_v2', description='BigQuery multi-source control-table driven extraction and CSV delivery',
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), schedule=None, catchup=False,
    max_active_runs=3, is_paused_upon_creation=True, params={'job_id': 'zuora_tickets'},
    default_args={'retries': 1, 'retry_delay': timedelta(seconds=30),
                  'retry_exponential_backoff': False, 'on_failure_callback': mark_failed},
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
