import os
from datetime import datetime, timedelta, timezone

from minio import Minio

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator


def find_recent_objects(objects: list, cutoff: datetime) -> list:
    return [obj for obj in objects if obj.last_modified >= cutoff]


def check_sensors(**context):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=2)

    client = Minio(
        os.environ['MINIO_ENDPOINT'].replace('http://', ''),
        access_key=os.environ['MINIO_ACCESS_KEY'],
        secret_key=os.environ['MINIO_SECRET_KEY'],
        secure=False,
    )

    all_objects = list(client.list_objects('smart-farm-raw', recursive=True))
    recent = find_recent_objects(all_objects, cutoff)

    if not recent:
        raise AirflowException(
            "Aucune donnée reçue dans smart-farm-raw depuis 2h — vérifier Webots et kafka-consumer"
        )

    total_size = sum(obj.size for obj in recent)
    print(f"[HealthCheck] {len(recent)} fichiers reçus ({total_size // 1024} KiB)")


with DAG(
    dag_id='sensor_health_check',
    schedule_interval='0 */2 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['monitoring', 'health'],
) as dag:
    PythonOperator(
        task_id='check_sensor_data',
        python_callable=check_sensors,
    )
