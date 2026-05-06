import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id='spark_cleaning_job',
    schedule_interval='@hourly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['spark', 'cleaning'],
) as dag:
    DockerOperator(
        task_id='run_cleaning_job',
        image='smart_agriculture-spark:latest',
        command='bash -c "/opt/spark/bin/spark-submit /opt/spark-jobs/cleaning_job.py"',
        environment={
            'MINIO_ENDPOINT': os.environ.get('MINIO_ENDPOINT', 'http://minio:9000'),
            'MINIO_ACCESS_KEY': os.environ.get('MINIO_ACCESS_KEY', 'minioadmin'),
            'MINIO_SECRET_KEY': os.environ.get('MINIO_SECRET_KEY', 'minioadmin'),
        },
        network_mode='smart-farm-net',
        auto_remove=True,
        docker_url='unix:///var/run/docker.sock',
    )
