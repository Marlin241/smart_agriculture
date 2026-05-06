import os
from datetime import datetime, timedelta, timezone

from minio import Minio
from minio.commonconfig import CopySource

from airflow import DAG
from airflow.operators.python import PythonOperator


def find_objects_older_than(objects: list, days: int, reference: datetime) -> list:
    cutoff = reference - timedelta(days=days)
    return [obj for obj in objects if obj.last_modified < cutoff]


def archive_and_cleanup(**context):
    reference = datetime.now(timezone.utc)

    client = Minio(
        os.environ['MINIO_ENDPOINT'].replace('http://', ''),
        access_key=os.environ['MINIO_ACCESS_KEY'],
        secret_key=os.environ['MINIO_SECRET_KEY'],
        secure=False,
    )

    raw_objects = list(client.list_objects('smart-farm-raw', recursive=True))
    to_archive = find_objects_older_than(raw_objects, 7, reference)

    archived_size = 0
    for obj in to_archive:
        client.copy_object(
            'smart-farm-archive', obj.object_name,
            CopySource('smart-farm-raw', obj.object_name),
        )
        client.remove_object('smart-farm-raw', obj.object_name)
        archived_size += obj.size
    print(f"[Archive] {len(to_archive)} objets archivés ({archived_size // 1024} KiB)")

    archive_objects = list(client.list_objects('smart-farm-archive', recursive=True))
    to_delete = find_objects_older_than(archive_objects, 30, reference)

    deleted_size = 0
    for obj in to_delete:
        client.remove_object('smart-farm-archive', obj.object_name)
        deleted_size += obj.size
    print(f"[Archive] {len(to_delete)} objets supprimés de smart-farm-archive ({deleted_size // 1024} KiB)")


with DAG(
    dag_id='data_archiving',
    schedule_interval='0 2 * * 1',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['archiving', 'maintenance'],
) as dag:
    PythonOperator(
        task_id='archive_and_cleanup',
        python_callable=archive_and_cleanup,
    )
